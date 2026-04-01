#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Dock + Supply + Charge manager.

This node encapsulates the real-robot workflow:
  1) Fine docking via my_docking_controller (docking_action),
     unless station_status[11] already reports AGV in-place
  2) Station IR in-place check
  3) Optional mechanical connect (legacy cylinder/rod flow)
  4) Drain sewage if needed
  5) Refill clean water if needed
  6) Enable charge (robot relay + station charger)
  7) Wait until target SOC
  8) Disable charge
  9) Optional mechanical disconnect (legacy cylinder/rod flow)

Temporary fallback:
  - When `~direct_charge_after_precise_docking` is enabled, the workflow will
    start charging immediately after precise docking succeeds, skipping station
    IR in-place / drain / refill. This is meant for现场临时兜底验证，不是正式长期流程。

Interfaces:
  - Service  : /dock_supply/start          (std_srvs/Trigger)  -> start workflow (non-blocking)
  - Service  : /dock_supply/cancel         (std_srvs/Trigger)  -> cancel workflow
  - Service  : /dock_supply/set_defer_exit (std_srvs/SetBool)  -> defer exit until /dock_supply/exit
  - Service  : /dock_supply/exit           (std_srvs/Trigger)  -> execute configured exit workflow
  - Topic    : /dock_supply/state          (std_msgs/String)   -> IDLE/LOCK_DOCK_POSE/SEARCH_DOCK_POSE/PRECISE_DOCKING/WAIT_STATION_IN_PLACE/
                                                           SEARCH_STATION_IN_PLACE/
                                                           DRAINING/REFILLING/CHARGE_CMD_SENT/CHARGE_CONFIRMED/READY_TO_EXIT/EXIT_BACKING/
                                                           DONE/FAILED/CANCELED

Dependencies:
  - /battery_state     (sensor_msgs/BatteryState) published by mcore_tcp_bridge
  - /combined_status   (my_msg_srv/CombinedStatus) for water levels
  - /station_status    (my_msg_srv/StationStatus) for IR/(legacy rod) states
  - /station/control   (my_msg_srv/ControlStation) command to station_tcp_bridge
  - /mcore/control_water_tap (my_msg_srv/ControlWaterTap) command to mcore_tcp_bridge
  - /mcore/charge_enable (std_msgs/Bool) command to mcore_tcp_bridge

NOTE:
  Task layer (coverage_task_manager) is expected to:
    - navigate to pre-dock pose using MBF
    - on DOCK_OK, call /dock_supply/start
    - wait /dock_supply/state==READY_TO_EXIT if task layer wants to hold the robot docked
    - call /dock_supply/exit when task layer decides it is safe to back out
"""

import threading
import time
import math
from collections import deque
from typing import Optional

import rospy
import actionlib
from std_msgs.msg import String, Bool
from std_srvs.srv import Trigger, TriggerResponse, SetBool, SetBoolResponse
from sensor_msgs.msg import BatteryState
from geometry_msgs.msg import Twist, PoseStamped

from my_msg_srv.msg import CombinedStatus, StationStatus, ControlStation, ControlWaterTap
from my_docking_controller.msg import AutoDockingAction, AutoDockingGoal


def soc_from_battery(msg: Optional[BatteryState]) -> Optional[float]:
    if msg is None:
        return None
    try:
        soc = float(msg.percentage)
    except Exception:
        return None
    if soc > 1.0 + 1e-6:
        soc = soc / 100.0
    soc = max(0.0, min(1.0, soc))
    return soc


def soc_reached_target(soc: Optional[float], target: float, tol: float = 1e-4) -> bool:
    if soc is None:
        return False
    try:
        return float(soc) >= (float(target) - float(tol))
    except Exception:
        return False


def voltage_from_battery(msg: Optional[BatteryState]) -> Optional[float]:
    if msg is None:
        return None
    try:
        v = float(msg.voltage)
    except Exception:
        return None
    if v <= 0.0:
        return None
    return v


class DockSupplyError(RuntimeError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = str(code or "FAILED").strip().upper() or "FAILED"


class DockSupplyManager:
    def __init__(self):
        # thresholds
        self.target_soc = float(rospy.get_param('~target_soc', 0.80))
        self.target_clean_level = int(rospy.get_param('~target_clean_level', 35))
        self.enable_drain = bool(rospy.get_param('~enable_drain', True))
        self.enable_refill = bool(rospy.get_param('~enable_refill', True))
        self.test_continue_on_charge_timeout = bool(rospy.get_param('~test_continue_on_charge_timeout', False))

        # docking
        self.docking_action_name = str(rospy.get_param('~docking_action_name', 'docking_action')).strip() or 'docking_action'
        self.docking_action_wait_s = float(rospy.get_param('~docking_action_wait_s', 5.0))
        self.docking_target_dist = float(rospy.get_param('~docking_target_dist', 0.0))
        self.docking_timeout_s = float(rospy.get_param('~docking_timeout_s', 120.0))
        self.station_in_place_timeout_s = float(rospy.get_param('~station_in_place_timeout_s', 5.0))
        self.pre_dock_settle_s = float(rospy.get_param('~pre_dock_settle_s', 0.5))
        self.mechanical_connect_enable = bool(rospy.get_param('~mechanical_connect_enable', False))
        self.skip_precise_docking_if_station_in_place = bool(
            rospy.get_param('~skip_precise_docking_if_station_in_place', False)
        )
        self.direct_charge_after_precise_docking = bool(
            rospy.get_param('~direct_charge_after_precise_docking', False)
        )
        self.dock_pose_topic = str(rospy.get_param('~dock_pose_topic', '/dock_pose')).strip() or '/dock_pose'
        self.dock_pose_lock_wait_s = float(rospy.get_param('~dock_pose_lock_wait_s', 3.0))
        self.dock_pose_lock_min_frames = max(1, int(rospy.get_param('~dock_pose_lock_min_frames', 3)))
        self.dock_pose_lock_hold_s = max(0.0, float(rospy.get_param('~dock_pose_lock_hold_s', 0.6)))
        self.dock_pose_lock_frame_timeout_s = max(
            0.05, float(rospy.get_param('~dock_pose_lock_frame_timeout_s', 0.5))
        )
        self.dock_pose_search_cycles = max(0, int(rospy.get_param('~dock_pose_search_cycles', 1)))
        self.dock_pose_search_yaw_deg = max(0.0, float(rospy.get_param('~dock_pose_search_yaw_deg', 6.0)))
        self.dock_pose_search_ang_speed = max(0.05, float(rospy.get_param('~dock_pose_search_ang_speed', 0.12)))
        self.dock_pose_search_forward_m = max(0.0, float(rospy.get_param('~dock_pose_search_forward_m', 0.05)))
        self.dock_pose_search_lin_speed = max(0.01, float(rospy.get_param('~dock_pose_search_lin_speed', 0.05)))
        self.dock_pose_search_step_wait_s = max(
            0.2, float(rospy.get_param('~dock_pose_search_step_wait_s', 1.2))
        )
        self.station_in_place_search_cycles = max(
            0, int(rospy.get_param('~station_in_place_search_cycles', 1))
        )
        self.station_in_place_search_yaw_deg = max(
            0.0, float(rospy.get_param('~station_in_place_search_yaw_deg', 4.0))
        )
        self.station_in_place_search_ang_speed = max(
            0.05, float(rospy.get_param('~station_in_place_search_ang_speed', 0.10))
        )
        self.station_in_place_search_step_wait_s = max(
            0.2, float(rospy.get_param('~station_in_place_search_step_wait_s', 0.8))
        )
        self.station_failure_backoff_m = max(
            0.0, float(rospy.get_param('~station_failure_backoff_m', 1.0))
        )
        self.station_failure_backoff_speed = max(
            0.02, float(rospy.get_param('~station_failure_backoff_speed', 0.10))
        )

        # timeouts
        self.rod_timeout_s = float(rospy.get_param('~rod_timeout_s', 25.0))
        self.drain_timeout_s = float(rospy.get_param('~drain_timeout_s', 600.0))
        self.refill_timeout_s = float(rospy.get_param('~refill_timeout_s', 600.0))
        self.charge_timeout_s = float(rospy.get_param('~charge_timeout_s', 10800.0))
        self.charge_check_period_s = float(rospy.get_param('~charge_check_period_s', 5.0))
        self.charge_cmd_repeat = max(1, int(rospy.get_param('~charge_cmd_repeat', 2)))
        self.charge_cmd_interval_s = max(0.0, float(rospy.get_param('~charge_cmd_interval_s', 0.08)))
        self.station_charge_enable_repeat = max(1, int(rospy.get_param('~station_charge_enable_repeat', 3)))
        self.station_charge_enable_interval_s = max(
            0.0, float(rospy.get_param('~station_charge_enable_interval_s', 3.0))
        )
        self.charge_confirm_voltage_delta_v = max(
            0.01, float(rospy.get_param('~charge_confirm_voltage_delta_v', 0.20))
        )
        self.charge_confirm_timeout_s = max(1.0, float(rospy.get_param('~charge_confirm_timeout_s', 18.0)))
        self.charge_confirm_min_samples = max(1, int(rospy.get_param('~charge_confirm_min_samples', 2)))
        self.charge_confirm_retry_limit = max(0, int(rospy.get_param('~charge_confirm_retry_limit', 2)))
        self.charge_confirm_check_period_s = max(
            0.2, float(rospy.get_param('~charge_confirm_check_period_s', 1.0))
        )

        # exit dock
        self.exit_mode = str(rospy.get_param('~exit_mode', 'none')).strip().lower()  # none/back
        self.back_distance = float(rospy.get_param('~back_distance', 2.2))
        self.back_speed = float(rospy.get_param('~back_speed', 0.10))
        self._defer_exit = bool(rospy.get_param('~defer_exit', False))

        # IO
        self._state_pub = rospy.Publisher('/dock_supply/state', String, queue_size=1, latch=True)
        self._cmd_vel_pub = rospy.Publisher('/cmd_vel', Twist, queue_size=10)
        self._station_pub = rospy.Publisher('/station/control', ControlStation, queue_size=10)
        self._tap_pub = rospy.Publisher('/mcore/control_water_tap', ControlWaterTap, queue_size=10)
        self._charge_pub = rospy.Publisher('/mcore/charge_enable', Bool, queue_size=10)

        self._bat: Optional[BatteryState] = None
        self._comb: Optional[CombinedStatus] = None
        self._station: Optional[StationStatus] = None
        self._dock_pose_ts: float = 0.0
        self._dock_pose_times = deque(maxlen=128)
        rospy.Subscriber('/battery_state', BatteryState, self._on_battery, queue_size=10)
        rospy.Subscriber('/combined_status', CombinedStatus, self._on_combined, queue_size=10)
        rospy.Subscriber('/station_status', StationStatus, self._on_station, queue_size=10)
        rospy.Subscriber(self.dock_pose_topic, PoseStamped, self._on_dock_pose, queue_size=20)

        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._cancel = False
        self._ready_to_exit = False
        self._state = 'IDLE'
        self._set_state('IDLE')

        self._srv_start = rospy.Service('/dock_supply/start', Trigger, self._srv_start_cb)
        self._srv_cancel = rospy.Service('/dock_supply/cancel', Trigger, self._srv_cancel_cb)
        self._srv_set_defer_exit = rospy.Service('/dock_supply/set_defer_exit', SetBool, self._srv_set_defer_exit_cb)
        self._srv_exit = rospy.Service('/dock_supply/exit', Trigger, self._srv_exit_cb)

        self._dock_client = actionlib.SimpleActionClient(self.docking_action_name, AutoDockingAction)
        rospy.loginfo(
            '[SUPPLY] config: mechanical_connect_enable=%s skip_precise_docking_if_station_in_place=%s direct_charge_after_precise_docking=%s drain_timeout=%.1fs refill_timeout=%.1fs charge_timeout=%.1fs continue_on_charge_timeout=%s',
            str(self.mechanical_connect_enable),
            str(self.skip_precise_docking_if_station_in_place),
            str(self.direct_charge_after_precise_docking),
            self.drain_timeout_s,
            self.refill_timeout_s,
            self.charge_timeout_s,
            str(self.test_continue_on_charge_timeout),
        )

    # ---------------- callbacks ----------------
    def _on_battery(self, msg: BatteryState):
        self._bat = msg

    def _on_combined(self, msg: CombinedStatus):
        self._comb = msg

    def _on_station(self, msg: StationStatus):
        self._station = msg

    def _on_dock_pose(self, _msg: PoseStamped):
        now = time.time()
        self._dock_pose_ts = now
        self._dock_pose_times.append(now)

    # ---------------- public services ----------------
    def _srv_start_cb(self, _req):
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return TriggerResponse(success=False, message='already running')
            if self._ready_to_exit:
                return TriggerResponse(success=False, message='workflow finished; call /dock_supply/exit before restarting')
            self._cancel = False
            self._thread = threading.Thread(target=self._run, daemon=True)
            self._thread.start()
            return TriggerResponse(success=True, message='started')

    def _srv_cancel_cb(self, _req):
        with self._lock:
            self._cancel = True
        return TriggerResponse(success=True, message='cancel requested')

    def _srv_set_defer_exit_cb(self, req):
        enabled = bool(getattr(req, 'data', False))
        with self._lock:
            self._defer_exit = enabled
            ready = bool(self._ready_to_exit)
        return SetBoolResponse(
            success=True,
            message='defer_exit=%s ready_to_exit=%s' % (str(enabled).lower(), str(ready).lower()),
        )

    def _srv_exit_cb(self, _req):
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return TriggerResponse(success=False, message='workflow already running')
            if not self._ready_to_exit:
                return TriggerResponse(success=False, message='dock_supply is not ready to exit')
            self._cancel = False
            self._thread = threading.Thread(target=self._run_exit_workflow, daemon=True)
            self._thread.start()
        return TriggerResponse(success=True, message='exit started')

    # ---------------- helpers ----------------
    def _set_state(self, s: str):
        self._state = s
        self._ready_to_exit = (str(s or '').strip().upper() == 'READY_TO_EXIT')
        self._state_pub.publish(String(data=s))
        rospy.loginfo('[SUPPLY] state=%s', s)

    def _is_canceled(self) -> bool:
        with self._lock:
            return bool(self._cancel)

    def _defer_exit_enabled(self) -> bool:
        with self._lock:
            return bool(self._defer_exit)

    def _stop_move(self):
        t = Twist()
        t.linear.x = 0.0
        t.angular.z = 0.0
        for _ in range(10):
            self._cmd_vel_pub.publish(t)
            rospy.sleep(0.05)

    def _drive_for(self, vx: float, wz: float, duration_s: float):
        duration_s = max(0.0, float(duration_s))
        if duration_s <= 1e-6:
            self._stop_move()
            return
        t = Twist()
        t.linear.x = float(vx)
        t.angular.z = float(wz)
        end_ts = time.time() + duration_s
        while time.time() < end_ts and not rospy.is_shutdown():
            if self._is_canceled():
                raise DockSupplyError('CANCELED', 'canceled')
            self._cmd_vel_pub.publish(t)
            rospy.sleep(0.05)
        self._stop_move()

    def _station_cmd(self, op: int, on: bool):
        msg = ControlStation()
        msg.operation = int(op)
        msg.status = bool(on)
        self._station_pub.publish(msg)

    def _tap_cmd(self, tap_id: int, op: int):
        msg = ControlWaterTap()
        msg.tap_id = int(tap_id)
        msg.operation = int(op)
        self._tap_pub.publish(msg)

    def _charge_enable(self, on: bool):
        desired = bool(on)
        station_repeats = self.station_charge_enable_repeat if desired else 1
        station_interval_s = self.station_charge_enable_interval_s if desired else 0.0
        rospy.loginfo(
            '[SUPPLY] charge_enable on=%s dispatch: mcore_topic=1 station_topic=%d interval=%.2fs',
            str(desired),
            station_repeats,
            station_interval_s,
        )
        self._charge_pub.publish(Bool(data=desired))
        for idx in range(station_repeats):
            self._station_cmd(1, desired)
            rospy.loginfo(
                '[SUPPLY] charge_enable station tx %d/%d on=%s',
                idx + 1,
                station_repeats,
                str(desired),
            )
            if idx + 1 < station_repeats and station_interval_s > 0.0:
                rospy.sleep(station_interval_s)
        rospy.loginfo('[SUPPLY] charge_enable dispatched on=%s', str(desired))

    def _safe_abort(self):
        # best-effort safety shutdown
        try:
            self._dock_client.cancel_all_goals()
        except Exception:
            pass
        try:
            self._charge_enable(False)
        except Exception:
            pass
        try:
            # close valves
            self._tap_cmd(2, 0)
            self._tap_cmd(3, 0)
        except Exception:
            pass
        try:
            # stop station pumps
            self._station_cmd(3, False)
            self._station_cmd(11, False)
        except Exception:
            pass
        if self.mechanical_connect_enable:
            try:
                # retract rod
                self._station_cmd(8, True)
            except Exception:
                pass
        try:
            self._stop_move()
        except Exception:
            pass

    def _latest_soc(self) -> Optional[float]:
        return soc_from_battery(self._bat)

    def _latest_voltage(self) -> Optional[float]:
        return voltage_from_battery(self._bat)

    def _station_in_place(self) -> bool:
        st = self._station
        return st is not None and len(st.status) >= 12 and bool(st.status[11])

    def _dock_pose_locked(self) -> bool:
        now = time.time()
        times = [ts for ts in self._dock_pose_times if (now - ts) <= self.dock_pose_lock_hold_s]
        if len(times) < self.dock_pose_lock_min_frames:
            return False
        if (now - times[-1]) > self.dock_pose_lock_frame_timeout_s:
            return False
        for t0, t1 in zip(times, times[1:]):
            if (t1 - t0) > self.dock_pose_lock_frame_timeout_s:
                return False
        return True

    def _wait_for_dock_pose_lock(self, timeout_s: float) -> bool:
        t0 = time.time()
        while (time.time() - t0) < timeout_s and (not rospy.is_shutdown()):
            if self._is_canceled():
                raise DockSupplyError('CANCELED', 'canceled')
            if self._dock_pose_locked():
                return True
            rospy.sleep(0.1)
        return False

    def _ensure_dock_pose_locked(self):
        self._set_state('LOCK_DOCK_POSE')
        if self._wait_for_dock_pose_lock(self.dock_pose_lock_wait_s):
            rospy.loginfo('[SUPPLY] dock pose locked without search')
            return
        yaw_rad = math.radians(self.dock_pose_search_yaw_deg)
        if self.dock_pose_search_cycles > 0 and yaw_rad > 1e-6:
            left_dur = yaw_rad / max(1e-6, self.dock_pose_search_ang_speed)
            right_dur = (2.0 * yaw_rad) / max(1e-6, self.dock_pose_search_ang_speed)
            center_dur = left_dur
            forward_dur = self.dock_pose_search_forward_m / max(1e-6, self.dock_pose_search_lin_speed)
            for idx in range(self.dock_pose_search_cycles):
                cycle = idx + 1
                self._set_state('SEARCH_DOCK_POSE')
                rospy.loginfo(
                    '[SUPPLY] dock pose search cycle %d/%d: left %.1fdeg',
                    cycle,
                    self.dock_pose_search_cycles,
                    self.dock_pose_search_yaw_deg,
                )
                self._drive_for(0.0, self.dock_pose_search_ang_speed, left_dur)
                if self._wait_for_dock_pose_lock(self.dock_pose_search_step_wait_s):
                    rospy.loginfo('[SUPPLY] dock pose locked after left search')
                    return

                rospy.loginfo(
                    '[SUPPLY] dock pose search cycle %d/%d: right %.1fdeg',
                    cycle,
                    self.dock_pose_search_cycles,
                    2.0 * self.dock_pose_search_yaw_deg,
                )
                self._drive_for(0.0, -self.dock_pose_search_ang_speed, right_dur)
                if self._wait_for_dock_pose_lock(self.dock_pose_search_step_wait_s):
                    rospy.loginfo('[SUPPLY] dock pose locked after right search')
                    return

                rospy.loginfo(
                    '[SUPPLY] dock pose search cycle %d/%d: recenter %.1fdeg',
                    cycle,
                    self.dock_pose_search_cycles,
                    self.dock_pose_search_yaw_deg,
                )
                self._drive_for(0.0, self.dock_pose_search_ang_speed, center_dur)
                if self._wait_for_dock_pose_lock(self.dock_pose_search_step_wait_s):
                    rospy.loginfo('[SUPPLY] dock pose locked after recenter search')
                    return

                if self.dock_pose_search_forward_m > 1e-6:
                    rospy.loginfo(
                        '[SUPPLY] dock pose search cycle %d/%d: forward %.2fm',
                        cycle,
                        self.dock_pose_search_cycles,
                        self.dock_pose_search_forward_m,
                    )
                    self._drive_for(self.dock_pose_search_lin_speed, 0.0, forward_dur)
                    if self._wait_for_dock_pose_lock(self.dock_pose_search_step_wait_s):
                        rospy.loginfo('[SUPPLY] dock pose locked after forward search')
                        return
                self._set_state('LOCK_DOCK_POSE')
        raise DockSupplyError('FAILED_NO_DOCK_POSE_LOCK', 'dock pose not locked before precise docking')

    def _run_precise_docking(self):
        if self.skip_precise_docking_if_station_in_place and self._station_in_place():
            rospy.logwarn('[SUPPLY] station status[11]=True before fine docking, skip precise docking')
            return
        if not self._dock_client.wait_for_server(rospy.Duration(self.docking_action_wait_s)):
            raise DockSupplyError('FAILED_PRECISE_DOCK', f'{self.docking_action_name} server not available')
        self._ensure_dock_pose_locked()
        self._set_state('PRECISE_DOCKING')
        goal = AutoDockingGoal()
        goal.target_dist = float(self.docking_target_dist)
        rospy.loginfo('[SUPPLY] start fine docking target_dist=%.3f', goal.target_dist)
        self._dock_client.send_goal(goal)

        t0 = time.time()
        while not rospy.is_shutdown():
            if self._is_canceled():
                self._dock_client.cancel_goal()
                raise DockSupplyError('CANCELED', 'canceled')
            if self._dock_client.wait_for_result(rospy.Duration(0.2)):
                break
            if time.time() - t0 >= self.docking_timeout_s:
                self._dock_client.cancel_goal()
                raise DockSupplyError('FAILED_PRECISE_DOCK', 'fine docking timeout')

        res = self._dock_client.get_result()
        if res is not None and bool(res.success):
            return

        msg = str(getattr(res, 'message', '') or '').strip()
        if 'No dock pose received' in msg:
            raise DockSupplyError('FAILED_NO_DOCK_POSE', f'fine docking failed: {msg}')
        if 'Dock pose lost during docking' in msg:
            raise DockSupplyError('FAILED_PRECISE_DOCK_POSE_LOST', f'fine docking failed: {msg}')
        raise DockSupplyError('FAILED_PRECISE_DOCK', f'fine docking failed: {msg}')

    def _run_exit_sequence(self):
        if self.exit_mode == 'back':
            self._set_state('EXIT_BACKING')
            dur = max(0.0, self.back_distance / max(1e-3, self.back_speed))
            rospy.loginfo('[SUPPLY] exit back %.2fm v=%.2f (%.1fs)', self.back_distance, self.back_speed, dur)
            t = Twist()
            t.linear.x = -abs(self.back_speed)
            t.angular.z = 0.0
            t0 = time.time()
            while time.time() - t0 < dur and not rospy.is_shutdown():
                if self._is_canceled():
                    raise DockSupplyError('CANCELED', 'canceled')
                self._cmd_vel_pub.publish(t)
                rospy.sleep(0.1)
            self._stop_move()
        else:
            rospy.loginfo('[SUPPLY] exit skipped (exit_mode=%s)', self.exit_mode)
        self._set_state('DONE')

    def _run_exit_workflow(self):
        try:
            self._run_exit_sequence()
        except Exception as e:
            if isinstance(e, DockSupplyError):
                code = e.code
            else:
                code = ""
            if code == 'CANCELED' or 'canceled' in str(e).lower() or self._is_canceled():
                self._set_state('CANCELED')
            else:
                rospy.logerr('[SUPPLY] exit failed: %s', str(e))
                self._set_state(code or 'FAILED')
            self._safe_abort()

    def _confirm_charge_started(self, base_v: Optional[float]) -> bool:
        elevated_samples = 0
        t0 = time.time()
        while (time.time() - t0) < self.charge_confirm_timeout_s and (not rospy.is_shutdown()):
            if self._is_canceled():
                raise DockSupplyError('CANCELED', 'canceled')
            voltage = self._latest_voltage()
            if voltage is not None and base_v is not None and voltage >= (base_v + self.charge_confirm_voltage_delta_v):
                elevated_samples += 1
                if elevated_samples >= self.charge_confirm_min_samples:
                    return True
            else:
                elevated_samples = 0
            rospy.sleep(self.charge_confirm_check_period_s)
        return False

    def _wait_for_station_in_place(self) -> bool:
        t0 = time.time()
        while time.time() - t0 < self.station_in_place_timeout_s and not rospy.is_shutdown():
            if self._is_canceled():
                raise DockSupplyError('CANCELED', 'canceled')
            if self._station_in_place():
                return True
            rospy.sleep(0.2)
        return False

    def _wait_for_station_in_place_step(self, timeout_s: float) -> bool:
        t0 = time.time()
        timeout_s = max(0.0, float(timeout_s))
        while time.time() - t0 < timeout_s and not rospy.is_shutdown():
            if self._is_canceled():
                raise DockSupplyError('CANCELED', 'canceled')
            if self._station_in_place():
                return True
            rospy.sleep(0.1)
        return False

    def _retreat_after_station_failure(self):
        if self.station_failure_backoff_m <= 1e-6:
            return
        dur = self.station_failure_backoff_m / max(1e-6, self.station_failure_backoff_speed)
        rospy.logwarn(
            '[SUPPLY] station in-place not confirmed, retreat %.2fm at %.2fm/s before retry/fail',
            self.station_failure_backoff_m,
            self.station_failure_backoff_speed,
        )
        self._drive_for(-abs(self.station_failure_backoff_speed), 0.0, dur)

    def _ensure_station_in_place(self):
        self._set_state('WAIT_STATION_IN_PLACE')
        if self._wait_for_station_in_place():
            return
        yaw_rad = math.radians(self.station_in_place_search_yaw_deg)
        if self.station_in_place_search_cycles > 0 and yaw_rad > 1e-6:
            left_dur = yaw_rad / max(1e-6, self.station_in_place_search_ang_speed)
            right_dur = (2.0 * yaw_rad) / max(1e-6, self.station_in_place_search_ang_speed)
            center_dur = left_dur
            for idx in range(self.station_in_place_search_cycles):
                cycle = idx + 1
                self._set_state('SEARCH_STATION_IN_PLACE')
                rospy.loginfo(
                    '[SUPPLY] station IR search cycle %d/%d: left %.1fdeg',
                    cycle,
                    self.station_in_place_search_cycles,
                    self.station_in_place_search_yaw_deg,
                )
                self._drive_for(0.0, self.station_in_place_search_ang_speed, left_dur)
                if self._wait_for_station_in_place_step(self.station_in_place_search_step_wait_s):
                    rospy.loginfo('[SUPPLY] station IR in-place after left search')
                    return

                rospy.loginfo(
                    '[SUPPLY] station IR search cycle %d/%d: right %.1fdeg',
                    cycle,
                    self.station_in_place_search_cycles,
                    2.0 * self.station_in_place_search_yaw_deg,
                )
                self._drive_for(0.0, -self.station_in_place_search_ang_speed, right_dur)
                if self._wait_for_station_in_place_step(self.station_in_place_search_step_wait_s):
                    rospy.loginfo('[SUPPLY] station IR in-place after right search')
                    return

                rospy.loginfo(
                    '[SUPPLY] station IR search cycle %d/%d: recenter %.1fdeg',
                    cycle,
                    self.station_in_place_search_cycles,
                    self.station_in_place_search_yaw_deg,
                )
                self._drive_for(0.0, self.station_in_place_search_ang_speed, center_dur)
                if self._wait_for_station_in_place_step(self.station_in_place_search_step_wait_s):
                    rospy.loginfo('[SUPPLY] station IR in-place after recenter search')
                    return
                self._set_state('WAIT_STATION_IN_PLACE')
        self._retreat_after_station_failure()
        raise DockSupplyError('FAILED_STATION_IN_PLACE', 'station IR not in-place (status[11] false)')

    def _wait_for_rod_connected(self) -> None:
        rospy.loginfo('[SUPPLY] extend rod...')
        self._station_cmd(9, True)  # cylinder retract == rod extend
        t0 = time.time()
        while time.time() - t0 < self.rod_timeout_s and not rospy.is_shutdown():
            if self._is_canceled():
                raise DockSupplyError('CANCELED', 'canceled')
            st = self._station
            if st is not None and len(st.status) >= 9 and bool(st.status[8]):
                rospy.loginfo('[SUPPLY] rod connected (status[8])')
                return
            rospy.sleep(0.2)
        raise DockSupplyError('FAILED_MECHANICAL_CONNECT', 'rod extend timeout')

    def _wait_for_rod_reset(self) -> None:
        rospy.loginfo('[SUPPLY] retract rod...')
        self._station_cmd(8, True)  # cylinder extend == rod retract
        t0 = time.time()
        while time.time() - t0 < self.rod_timeout_s and not rospy.is_shutdown():
            if self._is_canceled():
                raise DockSupplyError('CANCELED', 'canceled')
            st = self._station
            if st is not None and len(st.status) >= 8 and bool(st.status[7]):
                rospy.loginfo('[SUPPLY] rod reset (status[7])')
                return
            rospy.sleep(0.2)
        raise DockSupplyError('FAILED_MECHANICAL_DISCONNECT', 'rod retract timeout')

    def _run_charge_phase(self) -> None:
        self._set_state('CHARGE_CMD_SENT')
        charge_base_v = self._latest_voltage()
        rospy.loginfo(
            '[SUPPLY] enable charging... target_soc=%.2f base_v=%s',
            self.target_soc,
            '%.3f' % charge_base_v if charge_base_v is not None else 'n/a',
        )
        self._charge_enable(True)
        rospy.sleep(1.0)

        confirmed = False
        for attempt in range(self.charge_confirm_retry_limit + 1):
            if self._confirm_charge_started(charge_base_v):
                confirmed = True
                break
            if attempt >= self.charge_confirm_retry_limit:
                break
            rospy.logwarn(
                '[SUPPLY] charge not confirmed, resend command attempt=%d/%d',
                attempt + 1,
                self.charge_confirm_retry_limit,
            )
            self._charge_enable(True)
        if not confirmed:
            raise DockSupplyError('FAILED_CHARGE_NOT_CONFIRMED', 'charge not confirmed after retries')

        self._set_state('CHARGE_CONFIRMED')

        t0 = time.time()
        while not rospy.is_shutdown():
            if self._is_canceled():
                raise DockSupplyError('CANCELED', 'canceled')
            soc = soc_from_battery(self._bat)
            if soc_reached_target(soc, self.target_soc):
                rospy.loginfo('[SUPPLY] charged soc=%.3f', soc)
                break
            if self.charge_timeout_s > 1e-6 and (time.time() - t0) >= self.charge_timeout_s:
                if self.test_continue_on_charge_timeout:
                    rospy.logwarn('[SUPPLY] charge timeout after %.1fs, continue workflow', self.charge_timeout_s)
                    break
                raise DockSupplyError('FAILED_CHARGE_TIMEOUT', 'charge timeout')
            rospy.sleep(self.charge_check_period_s)

    # ---------------- main workflow ----------------
    def _run(self):
        self._set_state('RUNNING')
        try:
            # 1) fine docking (or skip if already in station IR in-place)
            self._run_precise_docking()

            self._stop_move()
            rospy.sleep(self.pre_dock_settle_s)

            if self._is_canceled():
                self._set_state('CANCELED')
                self._safe_abort()
                return

            if self.direct_charge_after_precise_docking:
                rospy.logwarn(
                    '[SUPPLY] temporary override enabled: start charging immediately after precise docking; skip station in-place / supply steps'
                )
            else:
                # 2) IR in-place
                self._ensure_station_in_place()

                # 3) optional rod/cylinder connect for legacy dock hardware
                if self.mechanical_connect_enable:
                    self._set_state('MECHANICAL_CONNECT')
                    self._wait_for_rod_connected()
                else:
                    rospy.loginfo('[SUPPLY] skip mechanical connect (mechanical_connect_enable=false)')

                # 4) drain sewage
                comb = self._comb
                sewage = int(getattr(comb, 'sewage_level', 0)) if comb is not None else 0
                if self.enable_drain and sewage > 0:
                    self._set_state('DRAINING')
                    rospy.loginfo('[SUPPLY] drain sewage level=%d', sewage)
                    self._tap_cmd(3, 1)  # sewage valve open
                    rospy.sleep(0.5)
                    self._station_cmd(3, True)  # start drain
                    t0 = time.time()
                    while time.time() - t0 < self.drain_timeout_s and not rospy.is_shutdown():
                        if self._is_canceled():
                            raise DockSupplyError('CANCELED', 'canceled')
                        comb = self._comb
                        sewage = int(getattr(comb, 'sewage_level', 0)) if comb is not None else 0
                        if sewage == 0:
                            rospy.loginfo('[SUPPLY] sewage drained')
                            break
                        rospy.sleep(1.0)
                    else:
                        rospy.logwarn('[SUPPLY] drain timeout after %.1fs, continue workflow', self.drain_timeout_s)
                    self._station_cmd(3, False)
                    self._tap_cmd(3, 0)
                else:
                    rospy.loginfo('[SUPPLY] skip drain (enable=%s sewage=%d)', str(self.enable_drain), sewage)

                # 5) refill clean water
                comb = self._comb
                clean = int(getattr(comb, 'clean_level', 0)) if comb is not None else 0
                if self.enable_refill and clean < self.target_clean_level:
                    self._set_state('REFILLING')
                    rospy.loginfo('[SUPPLY] refill clean water level=%d -> target=%d', clean, self.target_clean_level)
                    self._tap_cmd(2, 1)  # clean valve open
                    rospy.sleep(0.5)
                    self._station_cmd(11, True)  # start refill
                    t0 = time.time()
                    while time.time() - t0 < self.refill_timeout_s and not rospy.is_shutdown():
                        if self._is_canceled():
                            raise DockSupplyError('CANCELED', 'canceled')
                        comb = self._comb
                        clean = int(getattr(comb, 'clean_level', 0)) if comb is not None else 0
                        if clean >= self.target_clean_level:
                            rospy.loginfo('[SUPPLY] clean water refilled')
                            break
                        rospy.sleep(1.0)
                    else:
                        rospy.logwarn('[SUPPLY] refill timeout after %.1fs, continue workflow', self.refill_timeout_s)
                    self._station_cmd(11, False)
                    self._tap_cmd(2, 0)
                else:
                    rospy.loginfo('[SUPPLY] skip refill (enable=%s clean=%d)', str(self.enable_refill), clean)

            # 6) charging loop
            self._run_charge_phase()

            # 7) disable charge + optional retract rod
            self._set_state('DISABLE_CHARGING')
            rospy.loginfo('[SUPPLY] disable charging...')
            self._charge_enable(False)
            rospy.sleep(0.5)

            if self.mechanical_connect_enable:
                self._set_state('MECHANICAL_DISCONNECT')
                self._wait_for_rod_reset()
            else:
                rospy.loginfo('[SUPPLY] skip mechanical disconnect (mechanical_connect_enable=false)')

            # 8) optional exit
            if self._defer_exit_enabled():
                self._set_state('READY_TO_EXIT')
                return

            self._run_exit_sequence()

        except Exception as e:
            if isinstance(e, DockSupplyError):
                code = e.code
            else:
                code = ""
            if code == 'CANCELED' or 'canceled' in str(e).lower() or self._is_canceled():
                self._set_state('CANCELED')
            else:
                rospy.logerr('[SUPPLY] failed: %s', str(e))
                self._set_state(code or 'FAILED')
            self._safe_abort()


def main():
    rospy.init_node('dock_supply_manager', anonymous=False)
    _ = DockSupplyManager()
    rospy.loginfo('[SUPPLY] manager started')
    rospy.spin()


if __name__ == '__main__':
    main()

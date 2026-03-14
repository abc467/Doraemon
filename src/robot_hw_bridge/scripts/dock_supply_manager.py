#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Dock + Supply + Charge manager.

This node encapsulates the *verified* real-robot workflow from clean_robot.py:
  1) Fine docking via my_docking_controller (docking_action)
  2) Station IR in-place check
  3) Rod extend (mechanical connect)
  4) Drain sewage if needed
  5) Refill clean water if needed
  6) Enable charge (robot relay + station charger)
  7) Wait until target SOC
  8) Disable charge, retract rod

Interfaces:
  - Service  : /dock_supply/start   (std_srvs/Trigger)  -> start workflow (non-blocking)
  - Service  : /dock_supply/cancel  (std_srvs/Trigger)  -> cancel workflow
  - Topic    : /dock_supply/state   (std_msgs/String)   -> IDLE/RUNNING/DONE/FAILED/CANCELED

Dependencies:
  - /battery_state     (sensor_msgs/BatteryState) published by mcore_tcp_bridge
  - /combined_status   (my_msg_srv/CombinedStatus) for water levels
  - /station_status    (my_msg_srv/StationStatus) for IR/rod states
  - /station/control   (my_msg_srv/ControlStation) command to station_tcp_bridge
  - /mcore/control_water_tap (my_msg_srv/ControlWaterTap) command to mcore_tcp_bridge
  - /mcore/charge_enable (std_msgs/Bool) command to mcore_tcp_bridge

NOTE:
  Task layer (coverage_task_manager) is expected to:
    - navigate to pre-dock pose using MBF
    - on DOCK_OK, call /dock_supply/start and wait /dock_supply/state==DONE
    - then undock/resume mission
"""

import threading
import time
from typing import Optional

import rospy
import actionlib
from std_msgs.msg import String, Bool
from std_srvs.srv import Trigger, TriggerResponse
from sensor_msgs.msg import BatteryState
from geometry_msgs.msg import Twist

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


class DockSupplyManager:
    def __init__(self):
        # thresholds
        self.target_soc = float(rospy.get_param('~target_soc', 0.80))
        self.target_clean_level = int(rospy.get_param('~target_clean_level', 35))
        self.enable_drain = bool(rospy.get_param('~enable_drain', True))
        self.enable_refill = bool(rospy.get_param('~enable_refill', True))

        # docking
        self.docking_action_name = str(rospy.get_param('~docking_action_name', 'docking_action')).strip() or 'docking_action'
        self.docking_action_wait_s = float(rospy.get_param('~docking_action_wait_s', 5.0))
        self.docking_target_dist = float(rospy.get_param('~docking_target_dist', 0.0))
        self.docking_timeout_s = float(rospy.get_param('~docking_timeout_s', 120.0))
        self.station_in_place_timeout_s = float(rospy.get_param('~station_in_place_timeout_s', 5.0))
        self.pre_dock_settle_s = float(rospy.get_param('~pre_dock_settle_s', 0.5))

        # timeouts
        self.rod_timeout_s = float(rospy.get_param('~rod_timeout_s', 25.0))
        self.drain_timeout_s = float(rospy.get_param('~drain_timeout_s', 600.0))
        self.refill_timeout_s = float(rospy.get_param('~refill_timeout_s', 600.0))
        self.charge_timeout_s = float(rospy.get_param('~charge_timeout_s', 10800.0))
        self.charge_check_period_s = float(rospy.get_param('~charge_check_period_s', 5.0))

        # exit dock
        self.exit_mode = str(rospy.get_param('~exit_mode', 'none')).strip().lower()  # none/back
        self.back_distance = float(rospy.get_param('~back_distance', 2.2))
        self.back_speed = float(rospy.get_param('~back_speed', 0.10))

        # IO
        self._state_pub = rospy.Publisher('/dock_supply/state', String, queue_size=1, latch=True)
        self._cmd_vel_pub = rospy.Publisher('/cmd_vel', Twist, queue_size=10)
        self._station_pub = rospy.Publisher('/station/control', ControlStation, queue_size=10)
        self._tap_pub = rospy.Publisher('/mcore/control_water_tap', ControlWaterTap, queue_size=10)
        self._charge_pub = rospy.Publisher('/mcore/charge_enable', Bool, queue_size=10)

        self._bat: Optional[BatteryState] = None
        self._comb: Optional[CombinedStatus] = None
        self._station: Optional[StationStatus] = None
        rospy.Subscriber('/battery_state', BatteryState, self._on_battery, queue_size=10)
        rospy.Subscriber('/combined_status', CombinedStatus, self._on_combined, queue_size=10)
        rospy.Subscriber('/station_status', StationStatus, self._on_station, queue_size=10)

        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._cancel = False
        self._state = 'IDLE'
        self._set_state('IDLE')

        self._srv_start = rospy.Service('/dock_supply/start', Trigger, self._srv_start_cb)
        self._srv_cancel = rospy.Service('/dock_supply/cancel', Trigger, self._srv_cancel_cb)

        self._dock_client = actionlib.SimpleActionClient(self.docking_action_name, AutoDockingAction)

    # ---------------- callbacks ----------------
    def _on_battery(self, msg: BatteryState):
        self._bat = msg

    def _on_combined(self, msg: CombinedStatus):
        self._comb = msg

    def _on_station(self, msg: StationStatus):
        self._station = msg

    # ---------------- public services ----------------
    def _srv_start_cb(self, _req):
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return TriggerResponse(success=False, message='already running')
            self._cancel = False
            self._thread = threading.Thread(target=self._run, daemon=True)
            self._thread.start()
            return TriggerResponse(success=True, message='started')

    def _srv_cancel_cb(self, _req):
        with self._lock:
            self._cancel = True
        return TriggerResponse(success=True, message='cancel requested')

    # ---------------- helpers ----------------
    def _set_state(self, s: str):
        self._state = s
        self._state_pub.publish(String(data=s))
        rospy.loginfo('[SUPPLY] state=%s', s)

    def _is_canceled(self) -> bool:
        with self._lock:
            return bool(self._cancel)

    def _stop_move(self):
        t = Twist()
        t.linear.x = 0.0
        t.angular.z = 0.0
        for _ in range(10):
            self._cmd_vel_pub.publish(t)
            rospy.sleep(0.05)

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
        self._charge_pub.publish(Bool(data=bool(on)))
        # station charger relay
        self._station_cmd(1, on)

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
        try:
            # retract rod
            self._station_cmd(8, True)
        except Exception:
            pass
        try:
            self._stop_move()
        except Exception:
            pass

    def _wait_for_station_in_place(self) -> bool:
        t0 = time.time()
        while time.time() - t0 < self.station_in_place_timeout_s and not rospy.is_shutdown():
            if self._is_canceled():
                raise RuntimeError('canceled')
            st = self._station
            if st is not None and len(st.status) >= 12 and bool(st.status[11]):
                return True
            rospy.sleep(0.2)
        return False

    # ---------------- main workflow ----------------
    def _run(self):
        self._set_state('RUNNING')
        try:
            # 1) fine docking
            if not self._dock_client.wait_for_server(rospy.Duration(self.docking_action_wait_s)):
                raise RuntimeError(f'{self.docking_action_name} server not available')
            goal = AutoDockingGoal()
            goal.target_dist = float(self.docking_target_dist)
            rospy.loginfo('[SUPPLY] start fine docking target_dist=%.3f', goal.target_dist)
            self._dock_client.send_goal(goal)

            t0 = time.time()
            while not rospy.is_shutdown():
                if self._is_canceled():
                    self._dock_client.cancel_goal()
                    raise RuntimeError('canceled')
                if self._dock_client.wait_for_result(rospy.Duration(0.2)):
                    break
                if time.time() - t0 >= self.docking_timeout_s:
                    self._dock_client.cancel_goal()
                    raise RuntimeError('fine docking timeout')
            res = self._dock_client.get_result()
            if res is None or (not bool(res.success)):
                raise RuntimeError(f'fine docking failed: {getattr(res, "message", "")}')

            self._stop_move()
            rospy.sleep(self.pre_dock_settle_s)

            if self._is_canceled():
                self._set_state('CANCELED')
                self._safe_abort()
                return

            # 2) IR in-place
            if not self._wait_for_station_in_place():
                raise RuntimeError('station IR not in-place (status[11] false)')

            # 3) rod extend (mechanical connect)
            rospy.loginfo('[SUPPLY] extend rod...')
            self._station_cmd(9, True)  # cylinder retract == rod extend
            t0 = time.time()
            while time.time() - t0 < self.rod_timeout_s and not rospy.is_shutdown():
                if self._is_canceled():
                    raise RuntimeError('canceled')
                st = self._station
                if st is not None and len(st.status) >= 9 and bool(st.status[8]):
                    rospy.loginfo('[SUPPLY] rod connected (status[8])')
                    break
                rospy.sleep(0.2)
            else:
                raise RuntimeError('rod extend timeout')

            # 4) drain sewage
            comb = self._comb
            sewage = int(getattr(comb, 'sewage_level', 0)) if comb is not None else 0
            if self.enable_drain and sewage > 0:
                rospy.loginfo('[SUPPLY] drain sewage level=%d', sewage)
                self._tap_cmd(3, 1)  # sewage valve open
                rospy.sleep(0.5)
                self._station_cmd(3, True)  # start drain
                t0 = time.time()
                while time.time() - t0 < self.drain_timeout_s and not rospy.is_shutdown():
                    if self._is_canceled():
                        raise RuntimeError('canceled')
                    comb = self._comb
                    sewage = int(getattr(comb, 'sewage_level', 0)) if comb is not None else 0
                    if sewage == 0:
                        rospy.loginfo('[SUPPLY] sewage drained')
                        break
                    rospy.sleep(1.0)
                self._station_cmd(3, False)
                self._tap_cmd(3, 0)
            else:
                rospy.loginfo('[SUPPLY] skip drain (enable=%s sewage=%d)', str(self.enable_drain), sewage)

            # 5) refill clean water
            comb = self._comb
            clean = int(getattr(comb, 'clean_level', 0)) if comb is not None else 0
            if self.enable_refill and clean < self.target_clean_level:
                rospy.loginfo('[SUPPLY] refill clean water level=%d -> target=%d', clean, self.target_clean_level)
                self._tap_cmd(2, 1)  # clean valve open
                rospy.sleep(0.5)
                self._station_cmd(11, True)  # start refill
                t0 = time.time()
                while time.time() - t0 < self.refill_timeout_s and not rospy.is_shutdown():
                    if self._is_canceled():
                        raise RuntimeError('canceled')
                    comb = self._comb
                    clean = int(getattr(comb, 'clean_level', 0)) if comb is not None else 0
                    if clean >= self.target_clean_level:
                        rospy.loginfo('[SUPPLY] clean water refilled')
                        break
                    rospy.sleep(1.0)
                self._station_cmd(11, False)
                self._tap_cmd(2, 0)
            else:
                rospy.loginfo('[SUPPLY] skip refill (enable=%s clean=%d)', str(self.enable_refill), clean)

            # 6) charging loop
            rospy.loginfo('[SUPPLY] enable charging... target_soc=%.2f', self.target_soc)
            self._charge_enable(True)
            rospy.sleep(1.0)

            t0 = time.time()
            while not rospy.is_shutdown():
                if self._is_canceled():
                    raise RuntimeError('canceled')
                soc = soc_from_battery(self._bat)
                if soc is not None and soc >= self.target_soc:
                    rospy.loginfo('[SUPPLY] charged soc=%.3f', soc)
                    break
                if self.charge_timeout_s > 1e-6 and (time.time() - t0) >= self.charge_timeout_s:
                    raise RuntimeError('charge timeout')
                rospy.sleep(self.charge_check_period_s)

            # 7) disable charge + retract rod
            rospy.loginfo('[SUPPLY] disable charging...')
            self._charge_enable(False)
            rospy.sleep(0.5)

            rospy.loginfo('[SUPPLY] retract rod...')
            self._station_cmd(8, True)  # cylinder extend == rod retract
            t0 = time.time()
            while time.time() - t0 < self.rod_timeout_s and not rospy.is_shutdown():
                if self._is_canceled():
                    raise RuntimeError('canceled')
                st = self._station
                if st is not None and len(st.status) >= 8 and bool(st.status[7]):
                    rospy.loginfo('[SUPPLY] rod reset (status[7])')
                    break
                rospy.sleep(0.2)
            else:
                raise RuntimeError('rod retract timeout')

            # 8) optional exit
            if self.exit_mode == 'back':
                dur = max(0.0, self.back_distance / max(1e-3, self.back_speed))
                rospy.loginfo('[SUPPLY] exit back %.2fm v=%.2f (%.1fs)', self.back_distance, self.back_speed, dur)
                t = Twist()
                t.linear.x = -abs(self.back_speed)
                t.angular.z = 0.0
                t0 = time.time()
                while time.time() - t0 < dur and not rospy.is_shutdown():
                    if self._is_canceled():
                        raise RuntimeError('canceled')
                    self._cmd_vel_pub.publish(t)
                    rospy.sleep(0.1)
                self._stop_move()

            self._set_state('DONE')

        except Exception as e:
            if 'canceled' in str(e).lower() or self._is_canceled():
                self._set_state('CANCELED')
            else:
                rospy.logerr('[SUPPLY] failed: %s', str(e))
                self._set_state('FAILED')
            self._safe_abort()


def main():
    rospy.init_node('dock_supply_manager', anonymous=False)
    _ = DockSupplyManager()
    rospy.loginfo('[SUPPLY] manager started')
    rospy.spin()


if __name__ == '__main__':
    main()

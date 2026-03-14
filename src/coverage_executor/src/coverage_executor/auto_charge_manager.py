# -*- coding: utf-8 -*-

import math
import time
import threading
from typing import Optional, Tuple

import rospy
from sensor_msgs.msg import BatteryState

from .ros_utils import make_pose


def _soc_from_battery(msg: BatteryState) -> Optional[float]:
    # 兼容：percentage 可能是 0~1，也可能是 0~100（部分系统会这么发）
    if msg is None:
        return None
    soc = float(msg.percentage) if msg.percentage is not None else float("nan")
    if not math.isfinite(soc):
        return None
    if soc > 1.0 + 1e-6:
        soc = soc / 100.0
    soc = max(0.0, min(1.0, soc))
    return soc


class AutoChargeManager:
    """
    低电量自动回充：
      FOLLOW/CONNECT 中检测到 soc<=low -> pause -> docking -> charging -> undock -> resume

    关键工程点：
      - docking/undock/return-to-resume 都属于“通行段”，强制关清洁
      - undock 后，强制下一次 resume 的 CONNECT 走 transit（即便距离很近也先 CONNECT 再 FOLLOW）
    """

    def __init__(
        self,
        *,
        fsm,
        mbf,
        frame_id: str,
        zone_id: str,
        enabled: bool,
        check_hz: float,
        battery_topic: str,
        battery_stale_timeout_s: float,
        low_soc: float,
        resume_soc: float,
        rearm_soc: float,
        dock_xyyaw: Tuple[float, float, float],
        dock_timeout_s: float,
        charge_timeout_s: float,
        wait_paused_timeout_s: float,
        trigger_when_idle: bool,
        undock_forward_m: float = 0.6,
    ):
        self.fsm = fsm
        self.mbf = mbf
        self.frame_id = str(frame_id)
        self.zone_id = str(zone_id)

        self.enabled = bool(enabled)
        self.check_hz = max(0.2, float(check_hz))
        self.battery_topic = str(battery_topic)
        self.battery_stale_timeout_s = float(battery_stale_timeout_s)

        self.low_soc = float(low_soc)
        self.resume_soc = float(resume_soc)
        self.rearm_soc = float(rearm_soc)

        self.dock_xyyaw = (float(dock_xyyaw[0]), float(dock_xyyaw[1]), float(dock_xyyaw[2]))
        self.undock_forward_m = float(undock_forward_m)

        self.dock_timeout_s = float(dock_timeout_s)
        self.charge_timeout_s = float(charge_timeout_s)
        self.wait_paused_timeout_s = float(wait_paused_timeout_s)
        self.trigger_when_idle = bool(trigger_when_idle)

        self._lock = threading.Lock()
        self._last_soc: Optional[float] = None
        self._last_ts: float = 0.0

        self._armed = True
        self._phase = "IDLE"  # IDLE/DOCKING/CHARGING/UNDOCKING/RESUMING
        self._stop = False

        rospy.Subscriber(self.battery_topic, BatteryState, self._on_battery, queue_size=10)

        t = threading.Thread(target=self._run, daemon=True)
        t.start()

        rospy.logwarn(
            "[PWR] AutoChargeManager started. enabled=%s low=%.2f resume=%.2f rearm=%.2f topic=%s dock=(%.2f,%.2f,%.2f)",
            str(self.enabled), self.low_soc, self.resume_soc, self.rearm_soc,
            self.battery_topic, self.dock_xyyaw[0], self.dock_xyyaw[1], self.dock_xyyaw[2],
        )

    def _on_battery(self, msg: BatteryState):
        soc = _soc_from_battery(msg)
        if soc is None:
            return
        with self._lock:
            self._last_soc = soc
            self._last_ts = time.time()

    def _battery_ok(self) -> Tuple[Optional[float], bool]:
        with self._lock:
            soc = self._last_soc
            ts = self._last_ts
        if soc is None:
            return None, False
        age = time.time() - ts
        if self.battery_stale_timeout_s > 1e-3 and age > self.battery_stale_timeout_s:
            return soc, False
        return soc, True

    def _should_trigger(self, soc: float) -> bool:
        if not self.enabled:
            return False
        if not self._armed:
            return False
        if soc > self.low_soc:
            return False
        if self.trigger_when_idle:
            return True
        # 默认：仅在“正在执行任务”时触发
        st = self.fsm.get_state()
        if st.startswith("FOLLOW") or st.startswith("CONNECT") or self.fsm.is_running():
            return True
        return False

    def _dock_goal(self):
        x, y, yaw = self.dock_xyyaw
        return make_pose(self.frame_id, x, y, yaw)

    def _undock_goal(self):
        x, y, yaw = self.dock_xyyaw
        x2 = x + self.undock_forward_m * math.cos(yaw)
        y2 = y + self.undock_forward_m * math.sin(yaw)
        return make_pose(self.frame_id, x2, y2, yaw), (x2, y2, yaw)

    def _wait_action(self, timeout_s: float, check_fn, sleep_s: float = 0.05) -> bool:
        t0 = time.time()
        while not rospy.is_shutdown():
            if time.time() - t0 > timeout_s:
                return False
            if check_fn():
                return True
            time.sleep(sleep_s)
        return False

    def _run(self):
        rate = rospy.Rate(self.check_hz)
        while not rospy.is_shutdown():
            if self._stop:
                return

            soc, fresh = self._battery_ok()

            # stale
            if soc is not None and (not fresh):
                rospy.logwarn_throttle(2.0, "[PWR] battery stale: soc=%.3f topic=%s", soc, self.battery_topic)

            # re-arm
            if soc is not None and (not self._armed) and soc >= self.rearm_soc:
                self._armed = True
                rospy.loginfo("[PWR] re-armed: soc=%.3f >= rearm=%.3f", soc, self.rearm_soc)

            # IDLE phase trigger
            if self._phase == "IDLE":
                if soc is not None and fresh and self._should_trigger(soc):
                    rospy.logwarn("[PWR] low battery soc=%.3f <= %.3f -> auto charge sequence", soc, self.low_soc)
                    self._armed = False
                    self._phase = "DOCKING"

                    # 1) pause job
                    self.fsm.apply_cmd("pause")
                    # pause hold 会硬停 cmd_vel，回桩前必须关掉
                    self.fsm.set_pause_hold(False)

                    # 等待进入 PAUSED（最多 wait_paused_timeout_s）
                    t0 = time.time()
                    while (time.time() - t0) < self.wait_paused_timeout_s and (not rospy.is_shutdown()):
                        if self.fsm.get_state() == "PAUSED":
                            break
                        time.sleep(0.05)

                    # 2) 通行段强制关清洁（回桩）
                    self.fsm.request_transit_cleaning_off()

                    rospy.logwarn("[PWR] docking to fixed pose... goal=(%.2f,%.2f,%.2f) frame=%s",
                                  self.dock_xyyaw[0], self.dock_xyyaw[1], self.dock_xyyaw[2], self.frame_id)

                    self.fsm.publish_state_external("PWR_DOCKING")
                    self.mbf.send_connect(self._dock_goal())

                    ok = self._wait_action(self.dock_timeout_s, self.mbf.connect_done)
                    if (not ok) or (not self.mbf.connect_succeeded()):
                        rospy.logerr("[PWR] docking failed -> back to PAUSED (manual intervene)")
                        self.fsm.publish_state_external("PWR_DOCK_FAILED")
                        self._phase = "IDLE"
                    else:
                        rospy.logwarn("[PWR] docking success -> CHARGING")
                        self.fsm.publish_state_external("PWR_CHARGING")
                        self._phase = "CHARGING"

            elif self._phase == "CHARGING":
                if soc is None or (not fresh):
                    rate.sleep()
                    continue

                if soc >= self.resume_soc:
                    rospy.logwarn("[PWR] charge complete soc=%.3f >= %.3f -> undock", soc, self.resume_soc)

                    # 通行段：离桩前也必须关清洁
                    self.fsm.request_transit_cleaning_off()
                    self.fsm.publish_state_external("PWR_UNDOCKING")

                    undock_pose, undock_xyyaw = self._undock_goal()
                    rospy.logwarn("[PWR] undocking... goal=(%.2f,%.2f,%.2f) frame=%s",
                                  undock_xyyaw[0], undock_xyyaw[1], undock_xyyaw[2], self.frame_id)

                    self.mbf.send_connect(undock_pose)
                    ok = self._wait_action(self.dock_timeout_s, self.mbf.connect_done)
                    if (not ok) or (not self.mbf.connect_succeeded()):
                        rospy.logerr("[PWR] undock failed -> keep PAUSED (manual intervene)")
                        self.fsm.publish_state_external("PWR_UNDOCK_FAILED")
                        self._phase = "IDLE"
                    else:
                        rospy.logwarn("[PWR] undock success -> resume job")
                        # 强制下一次 resume 的 CONNECT 走 transit（不清洁），到 FOLLOW 再开
                        self.fsm.set_force_resume_transit_once(True)
                        self.fsm.apply_cmd(f"resume {self.zone_id}")
                        self.fsm.publish_state_external("PWR_RESUMING")
                        self._phase = "RESUMING"

            elif self._phase == "RESUMING":
                # 等待进入 FOLLOW（或 CONNECT 后进入 FOLLOW）
                st = self.fsm.get_state()
                if st.startswith("FOLLOW"):
                    rospy.logwarn("[PWR] resumed into %s -> back to IDLE monitor", st)
                    self._phase = "IDLE"

            rate.sleep()

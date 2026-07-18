#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Realtime M-core cleaning actuator test.

This is a temporary field-test node. It uses persistent ROS publishers and the
same actuator ordering/interval defaults as coverage_executor, unlike shell
`rostopic pub -1` tests which add several seconds per command.
"""

import threading
import time
from dataclasses import dataclass
from typing import Optional

import rospy
from geometry_msgs.msg import Twist
from sensor_msgs.msg import BatteryState
from std_msgs.msg import Bool, Float32, UInt16

from robot_platform_msgs.msg import (
    CleaningParams,
    CombinedStatus,
    ControlCleanTools,
    ControlMotor,
    ControlWaterTap,
)


def clamp_int(value, lo, hi):
    return max(int(lo), min(int(hi), int(value)))


def water_level_percent(raw):
    raw = int(raw) & 0x0F
    if raw <= 0:
        return 0
    # M-core uses cumulative bit masks: 1, 3, 7, 15.
    bits = 0
    for idx in range(4):
        if raw & (1 << idx):
            bits += 1
    return clamp_int(bits * 25, 0, 100)


@dataclass
class TelemetryValue:
    count: int = 0
    value: Optional[float] = None
    stamp: float = 0.0
    extra: str = ""


class RealtimeCleaningTestNode:
    def __init__(self):
        self.cmd_vel_topic = rospy.get_param("~cmd_vel_topic", "/cmd_vel")
        self.clean_tools_topic = rospy.get_param("~clean_tools_topic", "/mcore/control_clean_tools")
        self.water_tap_topic = rospy.get_param("~water_tap_topic", "/mcore/control_water_tap")
        self.vacuum_motor_topic = rospy.get_param("~vacuum_motor_topic", "/mcore/control_motor")
        self.cleaning_params_topic = rospy.get_param("~cleaning_params_topic", "/mcore/cleaning_params/set")

        self.connected_topic = rospy.get_param("~mcore_connected_topic", "/mcore_velocity_sender/connected")
        self.battery_remaining_topic = rospy.get_param("~battery_remaining_topic", "/mcore/battery_remaining")
        self.battery_state_topic = rospy.get_param("~battery_state_topic", "/battery_state")
        self.clean_water_topic = rospy.get_param("~clean_water_level_topic", "/mcore/clean_water_level")
        self.sewage_topic = rospy.get_param("~sewage_level_topic", "/mcore/sewage_level")
        self.combined_status_topic = rospy.get_param("~combined_status_topic", "/combined_status")

        self.cmd_vel_rate_hz = max(0.1, float(rospy.get_param("~cmd_vel_rate_hz", 10.0)))
        self.cmd_vel_linear_x = float(rospy.get_param("~cmd_vel_linear_x", 0.0))
        self.cmd_vel_angular_z = float(rospy.get_param("~cmd_vel_angular_z", 0.0))
        self.active_duration_sec = max(0.0, float(rospy.get_param("~active_duration_sec", 20.0)))
        self.post_stop_hold_sec = max(0.0, float(rospy.get_param("~post_stop_hold_sec", 3.0)))
        self.start_delay_sec = max(0.0, float(rospy.get_param("~start_delay_sec", 2.0)))
        self.wait_for_subscribers_sec = max(0.0, float(rospy.get_param("~wait_for_subscribers_sec", 10.0)))
        self.wait_for_mcore_connected_sec = max(0.0, float(rospy.get_param("~wait_for_mcore_connected_sec", 10.0)))
        self.require_mcore_connected = bool(rospy.get_param("~require_mcore_connected", True))

        self.profile_name = str(rospy.get_param("~profile_name", "standard_test"))
        self.main_brush_speed = clamp_int(rospy.get_param("~main_brush_speed", 40), 0, 100)
        self.side_brush_speed = clamp_int(rospy.get_param("~side_brush_speed", 10), 0, 100)
        self.side_brush_enable = bool(rospy.get_param("~side_brush_enable", True))
        self.brush_down_distance = clamp_int(rospy.get_param("~brush_down_distance", 1000), 0, 1800)
        self.water_pump = clamp_int(rospy.get_param("~vel_water_pump", 5), 0, 100)
        self.suction_machine_pwm = clamp_int(rospy.get_param("~suction_machine_pwm", 70), 0, 100)
        self.vacuum_motor_pwm = clamp_int(
            rospy.get_param("~vacuum_motor_pwm", self.suction_machine_pwm), 0, 100
        )
        self.height_scrub = clamp_int(rospy.get_param("~height_scrub", 0), 0, 255)

        self.clean_tool_cmd_interval_s = max(0.0, float(rospy.get_param("~clean_tool_cmd_interval_s", 0.05)))
        self.water_tap_cmd_interval_s = max(0.0, float(rospy.get_param("~water_tap_cmd_interval_s", 0.20)))
        self.vacuum_off_repeat = max(1, int(rospy.get_param("~vacuum_off_repeat", 3)))
        self.transition_retry_window_s = max(
            0.0, float(rospy.get_param("~actuator_transition_retry_window_s", 1.0))
        )
        self.transition_retry_interval_s = max(
            0.05, float(rospy.get_param("~actuator_retry_interval_s", 0.35))
        )
        self.active_refresh_s = max(0.0, float(rospy.get_param("~actuator_active_refresh_s", 1.0)))
        self.enable_active_refresh = bool(rospy.get_param("~enable_active_refresh", True))
        self.enable_transition_retry = bool(rospy.get_param("~enable_transition_retry", True))
        self.enable_on_transition_retry = bool(rospy.get_param("~enable_on_transition_retry", True))
        self.enable_stop_transition_retry = bool(rospy.get_param("~enable_stop_transition_retry", True))

        self.brush_tool_id = int(rospy.get_param("~brush_tool_id", 0x01))
        self.scraper_tool_id = int(rospy.get_param("~scraper_tool_id", 0x02))
        self.side_brush_tool_id = int(rospy.get_param("~side_brush_tool_id", 0x03))
        self.water_pump_tap_id = int(rospy.get_param("~water_pump_tap_id", 0x01))
        self.clean_water_valve_tap_id = int(rospy.get_param("~clean_water_valve_tap_id", 0x02))
        self.suction_tap_id = int(rospy.get_param("~suction_tap_id", 0x05))

        self._lock = threading.RLock()
        self._mcore_connected = False
        self._mcore_connected_seen = False
        self._cmd_vel_timer = None
        self._publish_count = {
            "cmd_vel": 0,
            "params": 0,
            "clean_tool": 0,
            "water_tap": 0,
            "vacuum_motor": 0,
        }
        self._telemetry = {
            "battery_remaining": TelemetryValue(),
            "battery_state": TelemetryValue(),
            "clean_water": TelemetryValue(),
            "sewage": TelemetryValue(),
            "combined": TelemetryValue(),
        }

        self.cmd_pub = rospy.Publisher(self.cmd_vel_topic, Twist, queue_size=1)
        self.clean_tools_pub = rospy.Publisher(self.clean_tools_topic, ControlCleanTools, queue_size=10)
        self.water_tap_pub = rospy.Publisher(self.water_tap_topic, ControlWaterTap, queue_size=10)
        self.vacuum_motor_pub = rospy.Publisher(self.vacuum_motor_topic, ControlMotor, queue_size=10)
        self.cleaning_params_pub = rospy.Publisher(
            self.cleaning_params_topic, CleaningParams, queue_size=1, latch=True
        )

        rospy.Subscriber(self.connected_topic, Bool, self._on_connected, queue_size=10)
        rospy.Subscriber(self.battery_remaining_topic, Float32, self._on_battery_remaining, queue_size=10)
        rospy.Subscriber(self.battery_state_topic, BatteryState, self._on_battery_state, queue_size=10)
        rospy.Subscriber(self.clean_water_topic, UInt16, self._on_clean_water, queue_size=10)
        rospy.Subscriber(self.sewage_topic, UInt16, self._on_sewage, queue_size=10)
        rospy.Subscriber(self.combined_status_topic, CombinedStatus, self._on_combined_status, queue_size=10)

    def _on_connected(self, msg):
        with self._lock:
            self._mcore_connected = bool(msg.data)
            self._mcore_connected_seen = True

    def _note_telemetry(self, key, value, extra=""):
        with self._lock:
            item = self._telemetry[key]
            item.count += 1
            item.value = value
            item.stamp = time.time()
            item.extra = str(extra or "")

    def _on_battery_remaining(self, msg):
        self._note_telemetry("battery_remaining", float(msg.data), "remaining")

    def _on_battery_state(self, msg):
        pct = float(msg.percentage) * 100.0 if msg.percentage >= 0.0 else -1.0
        self._note_telemetry("battery_state", pct, f"charge={msg.charge:.2f} capacity={msg.capacity:.2f}")

    def _on_clean_water(self, msg):
        raw = int(msg.data)
        self._note_telemetry("clean_water", raw, f"percent={water_level_percent(raw)}")

    def _on_sewage(self, msg):
        raw = int(msg.data)
        self._note_telemetry("sewage", raw, f"percent={water_level_percent(raw)}")

    def _on_combined_status(self, msg):
        self._note_telemetry(
            "combined",
            float(msg.battery_percentage),
            f"clean={int(msg.clean_level)} sewage={int(msg.sewage_level)}",
        )

    def _sleep(self, duration):
        if duration > 1e-6:
            rospy.sleep(duration)

    def _wait_for_subscribers(self):
        deadline = time.time() + self.wait_for_subscribers_sec
        pubs = [
            ("cmd_vel", self.cmd_pub),
            ("clean_tools", self.clean_tools_pub),
            ("water_tap", self.water_tap_pub),
            ("vacuum_motor", self.vacuum_motor_pub),
            ("cleaning_params", self.cleaning_params_pub),
        ]
        while not rospy.is_shutdown() and time.time() < deadline:
            missing = [name for name, pub in pubs if pub.get_num_connections() <= 0]
            if not missing:
                return True
            rospy.sleep(0.05)
        missing = [name for name, pub in pubs if pub.get_num_connections() <= 0]
        if missing:
            rospy.logwarn("[TEST] missing subscribers after %.1fs: %s", self.wait_for_subscribers_sec, ",".join(missing))
            return False
        return True

    def _wait_for_mcore_connected(self):
        if self.wait_for_mcore_connected_sec <= 0.0:
            return True
        deadline = time.time() + self.wait_for_mcore_connected_sec
        while not rospy.is_shutdown() and time.time() < deadline:
            with self._lock:
                connected = bool(self._mcore_connected)
            if connected:
                return True
            rospy.sleep(0.05)
        with self._lock:
            connected = bool(self._mcore_connected)
            seen = bool(self._mcore_connected_seen)
        if not connected:
            rospy.logwarn(
                "[TEST] mcore connected not true after %.1fs seen=%s",
                self.wait_for_mcore_connected_sec,
                str(seen),
            )
        return connected

    def _start_cmd_vel_timer(self):
        if self._cmd_vel_timer is not None:
            return
        self._cmd_vel_timer = rospy.Timer(rospy.Duration(1.0 / self.cmd_vel_rate_hz), self._publish_cmd_vel)

    def _stop_cmd_vel_timer(self):
        if self._cmd_vel_timer is not None:
            self._cmd_vel_timer.shutdown()
            self._cmd_vel_timer = None
        self._publish_cmd_vel(None)

    def _publish_cmd_vel(self, _event):
        msg = Twist()
        msg.linear.x = self.cmd_vel_linear_x
        msg.angular.z = self.cmd_vel_angular_z
        self.cmd_pub.publish(msg)
        self._publish_count["cmd_vel"] += 1

    def _publish_params(self):
        msg = CleaningParams()
        msg.profile_name = self.profile_name
        msg.vel_water_pump = self.water_pump
        msg.vel_water_suction = self.vacuum_motor_pwm
        msg.height_scrub = self.height_scrub
        msg.main_brush_speed = self.main_brush_speed
        msg.side_brush_speed = self.side_brush_speed
        msg.brush_down_distance = self.brush_down_distance
        msg.side_brush_enable = self.side_brush_enable
        self.cleaning_params_pub.publish(msg)
        self._publish_count["params"] += 1
        rospy.loginfo(
            "[TEST] params profile=%s main=%d side=%d side_enable=%s brush_down=%d pump=%d suction=%d",
            msg.profile_name,
            msg.main_brush_speed,
            msg.side_brush_speed,
            str(bool(msg.side_brush_enable)),
            msg.brush_down_distance,
            msg.vel_water_pump,
            msg.vel_water_suction,
        )

    def _send_tool(self, tool_id, operation, label):
        msg = ControlCleanTools()
        msg.tool_id = int(tool_id)
        msg.operation = int(operation)
        self.clean_tools_pub.publish(msg)
        self._publish_count["clean_tool"] += 1
        rospy.loginfo("[TEST] tool %-18s tool_id=%d operation=%d", label, msg.tool_id, msg.operation)

    def _send_tap(self, tap_id, operation, label):
        msg = ControlWaterTap()
        msg.tap_id = int(tap_id)
        msg.operation = int(operation)
        self.water_tap_pub.publish(msg)
        self._publish_count["water_tap"] += 1
        rospy.loginfo("[TEST] tap  %-18s tap_id=%d operation=%d", label, msg.tap_id, msg.operation)

    def _send_motor(self, vel, label):
        msg = ControlMotor()
        msg.vel = clamp_int(vel, 0, 100)
        self.vacuum_motor_pub.publish(msg)
        self._publish_count["vacuum_motor"] += 1
        rospy.loginfo("[TEST] motor %-18s vel=%d", label, msg.vel)

    def _brush_on(self):
        self._send_tool(self.brush_tool_id, 0x02, "brush_lift_down")
        self._sleep(self.clean_tool_cmd_interval_s)
        self._send_tool(self.brush_tool_id, 0x03, "main_brush_on")
        if self.side_brush_enable and self.side_brush_speed > 0:
            self._sleep(self.clean_tool_cmd_interval_s)
            self._send_tool(self.side_brush_tool_id, 0x03, "side_brush_on")

    def _brush_off(self):
        if self.side_brush_enable:
            self._send_tool(self.side_brush_tool_id, 0x04, "side_brush_off")
            self._sleep(self.clean_tool_cmd_interval_s)
        self._send_tool(self.brush_tool_id, 0x00, "brush_stop")
        self._sleep(self.clean_tool_cmd_interval_s)
        self._send_tool(self.brush_tool_id, 0x04, "main_brush_off")
        self._sleep(self.clean_tool_cmd_interval_s)
        self._send_tool(self.brush_tool_id, 0x01, "brush_lift_home")

    def _scraper_on(self):
        self._send_tool(self.scraper_tool_id, 0x02, "scraper_on")

    def _scraper_off(self):
        self._send_tool(self.scraper_tool_id, 0x01, "scraper_off")

    def _vacuum_on(self):
        self._send_tap(self.suction_tap_id, self.suction_machine_pwm, "suction_tap_on")
        self._send_motor(self.vacuum_motor_pwm, "vacuum_motor_on")

    def _vacuum_off(self):
        for idx in range(self.vacuum_off_repeat):
            self._send_tap(self.suction_tap_id, 0, f"suction_tap_off_{idx + 1}")
            self._send_motor(0, f"vacuum_motor_off_{idx + 1}")
            if idx + 1 < self.vacuum_off_repeat:
                self._sleep(self.water_tap_cmd_interval_s)

    def _water_on(self):
        self._send_tap(self.clean_water_valve_tap_id, 1, "clean_valve_on")
        self._sleep(self.water_tap_cmd_interval_s)
        self._send_tap(self.water_pump_tap_id, self.water_pump, "water_pump_on")

    def _water_off(self):
        self._send_tap(self.water_pump_tap_id, 0, "water_pump_off")
        self._sleep(self.water_tap_cmd_interval_s)
        self._send_tap(self.clean_water_valve_tap_id, 0, "clean_valve_off")

    def _transition_retry(self, phase, enabled):
        if not self.enable_transition_retry:
            return
        if enabled and not self.enable_on_transition_retry:
            return
        if (not enabled) and not self.enable_stop_transition_retry:
            return
        end_time = time.time() + self.transition_retry_window_s
        while not rospy.is_shutdown() and time.time() < end_time:
            self._sleep(self.transition_retry_interval_s)
            if enabled:
                rospy.loginfo("[TEST] transition retry ON")
                self._scraper_on()
                self._sleep(self.clean_tool_cmd_interval_s)
                self._brush_on()
                self._sleep(self.clean_tool_cmd_interval_s)
                self._vacuum_on()
                self._sleep(self.clean_tool_cmd_interval_s)
                self._water_on()
            else:
                rospy.loginfo("[TEST] transition retry OFF")
                self._water_off()
                self._sleep(self.clean_tool_cmd_interval_s)
                self._brush_off()
                self._sleep(self.clean_tool_cmd_interval_s)
                self._scraper_off()
                self._sleep(self.clean_tool_cmd_interval_s)
                self._vacuum_off()

    def _active_refresh_loop(self, stop_event):
        if not self.enable_active_refresh or self.active_refresh_s <= 0.0:
            return
        while not rospy.is_shutdown() and not stop_event.wait(self.active_refresh_s):
            rospy.loginfo("[TEST] active refresh")
            self._vacuum_on()
            self._sleep(self.clean_tool_cmd_interval_s)
            self._water_on()

    def _turn_on_cleaning(self):
        rospy.loginfo("[TEST] ON sequence: scraper -> brush -> vacuum -> water")
        self._scraper_on()
        self._sleep(self.clean_tool_cmd_interval_s)
        self._brush_on()
        self._sleep(self.clean_tool_cmd_interval_s)
        self._vacuum_on()
        self._sleep(self.clean_tool_cmd_interval_s)
        self._water_on()
        self._transition_retry("on", True)

    def _turn_off_cleaning(self):
        rospy.loginfo("[TEST] OFF sequence: water -> brush -> scraper -> vacuum")
        self._water_off()
        self._sleep(self.clean_tool_cmd_interval_s)
        self._brush_off()
        self._sleep(self.clean_tool_cmd_interval_s)
        self._scraper_off()
        self._sleep(self.clean_tool_cmd_interval_s)
        self._vacuum_off()
        self._transition_retry("off", False)

    def _summary(self):
        with self._lock:
            telemetry = {key: TelemetryValue(v.count, v.value, v.stamp, v.extra) for key, v in self._telemetry.items()}
            counts = dict(self._publish_count)
        rospy.loginfo("[TEST] publish counts %s", counts)
        now = time.time()
        for key in ["battery_remaining", "battery_state", "clean_water", "sewage", "combined"]:
            item = telemetry[key]
            age = now - item.stamp if item.stamp > 0 else -1.0
            rospy.loginfo(
                "[TEST] telemetry %-17s count=%d last=%s extra=%s age=%.2fs",
                key,
                item.count,
                str(item.value),
                item.extra,
                age,
            )

    def run(self):
        rospy.loginfo(
            "[TEST] realtime cleaning test start cmd_vel=%.2fHz vx=%.3f wz=%.3f active=%.1fs tool_interval=%.3fs water_interval=%.3fs",
            self.cmd_vel_rate_hz,
            self.cmd_vel_linear_x,
            self.cmd_vel_angular_z,
            self.active_duration_sec,
            self.clean_tool_cmd_interval_s,
            self.water_tap_cmd_interval_s,
        )
        self._wait_for_subscribers()
        connected = self._wait_for_mcore_connected()
        if self.require_mcore_connected and not connected:
            rospy.logerr("[TEST] abort: mcore is not connected")
            return

        self._start_cmd_vel_timer()
        self._sleep(self.start_delay_sec)
        self._publish_params()
        self._sleep(0.05)

        refresh_stop = threading.Event()
        refresh_thread = threading.Thread(target=self._active_refresh_loop, args=(refresh_stop,), daemon=True)

        try:
            self._turn_on_cleaning()
            refresh_thread.start()
            rospy.loginfo("[TEST] active hold %.1fs", self.active_duration_sec)
            self._sleep(self.active_duration_sec)
            refresh_stop.set()
            refresh_thread.join(timeout=max(1.0, self.active_refresh_s + 1.0))
            self._turn_off_cleaning()
            rospy.loginfo("[TEST] post stop hold %.1fs", self.post_stop_hold_sec)
            self._sleep(self.post_stop_hold_sec)
        finally:
            refresh_stop.set()
            self._stop_cmd_vel_timer()
            self._summary()


def main():
    rospy.init_node("mcore_realtime_cleaning_test")
    node = RealtimeCleaningTestNode()
    node.run()


if __name__ == "__main__":
    main()

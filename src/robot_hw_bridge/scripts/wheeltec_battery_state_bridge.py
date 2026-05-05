#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import math
import threading

import rospy
from sensor_msgs.msg import BatteryState
from std_msgs.msg import Bool, Float32


def _clamp(value, lo, hi):
    return max(lo, min(hi, value))


def _finite_or_nan(value):
    try:
        value = float(value)
    except Exception:
        return float("nan")
    return value if math.isfinite(value) else float("nan")


def _battery_technology(value):
    text = str(value or "lion").strip().lower()
    mapping = {
        "unknown": BatteryState.POWER_SUPPLY_TECHNOLOGY_UNKNOWN,
        "nimh": BatteryState.POWER_SUPPLY_TECHNOLOGY_NIMH,
        "lion": BatteryState.POWER_SUPPLY_TECHNOLOGY_LION,
        "li-ion": BatteryState.POWER_SUPPLY_TECHNOLOGY_LION,
        "lipo": BatteryState.POWER_SUPPLY_TECHNOLOGY_LIPO,
        "lifepo": BatteryState.POWER_SUPPLY_TECHNOLOGY_LIFE,
        "lifepo4": BatteryState.POWER_SUPPLY_TECHNOLOGY_LIFE,
        "nicd": BatteryState.POWER_SUPPLY_TECHNOLOGY_NICD,
    }
    return mapping.get(text, BatteryState.POWER_SUPPLY_TECHNOLOGY_LION)


class WheeltecBatteryStateBridge:
    def __init__(self):
        self.voltage_topic = rospy.get_param("~voltage_topic", "/PowerVoltage")
        self.charging_flag_topic = rospy.get_param("~charging_flag_topic", "/robot_charging_flag")
        self.charging_current_topic = rospy.get_param("~charging_current_topic", "/robot_charging_current")
        self.battery_topic = rospy.get_param("~battery_topic", "/battery_state")

        self.empty_voltage = float(rospy.get_param("~empty_voltage", 20.0))
        self.full_voltage = float(rospy.get_param("~full_voltage", 25.2))
        self.publish_rate_hz = max(0.2, float(rospy.get_param("~publish_rate_hz", 2.0)))
        self.input_stale_timeout_s = max(0.1, float(rospy.get_param("~input_stale_timeout_s", 5.0)))
        self.serial_number = str(rospy.get_param("~serial_number", "wheeltec"))
        self.power_supply_technology = _battery_technology(rospy.get_param("~power_supply_technology", "lion"))

        self._lock = threading.Lock()
        self._voltage = None
        self._voltage_stamp = rospy.Time(0)
        self._charging = None
        self._charging_current = None

        self._pub = rospy.Publisher(self.battery_topic, BatteryState, queue_size=10)
        self._voltage_sub = rospy.Subscriber(self.voltage_topic, Float32, self._on_voltage, queue_size=10)
        self._charging_sub = rospy.Subscriber(self.charging_flag_topic, Bool, self._on_charging, queue_size=10)
        self._current_sub = rospy.Subscriber(self.charging_current_topic, Float32, self._on_charging_current, queue_size=10)

        self._timer = rospy.Timer(rospy.Duration(1.0 / self.publish_rate_hz), self._publish)
        rospy.loginfo(
            "[WHEELTEC_BAT] bridge started voltage=%s battery=%s empty=%.2fV full=%.2fV",
            self.voltage_topic,
            self.battery_topic,
            self.empty_voltage,
            self.full_voltage,
        )

    def _on_voltage(self, msg):
        with self._lock:
            self._voltage = _finite_or_nan(msg.data)
            self._voltage_stamp = rospy.Time.now()

    def _on_charging(self, msg):
        with self._lock:
            self._charging = bool(msg.data)

    def _on_charging_current(self, msg):
        with self._lock:
            self._charging_current = _finite_or_nan(msg.data)

    def _percentage_from_voltage(self, voltage):
        if not math.isfinite(voltage):
            return float("nan")
        span = self.full_voltage - self.empty_voltage
        if span <= 1e-6:
            return float("nan")
        return _clamp((voltage - self.empty_voltage) / span, 0.0, 1.0)

    def _publish(self, _event):
        with self._lock:
            voltage = self._voltage
            voltage_stamp = self._voltage_stamp
            charging = self._charging
            charging_current = self._charging_current

        if voltage is None or voltage_stamp == rospy.Time(0):
            rospy.logwarn_throttle(5.0, "[WHEELTEC_BAT] waiting for %s", self.voltage_topic)
            return

        age = (rospy.Time.now() - voltage_stamp).to_sec()
        if age > self.input_stale_timeout_s:
            rospy.logwarn_throttle(
                5.0,
                "[WHEELTEC_BAT] stale %s age=%.1fs; skip /battery_state",
                self.voltage_topic,
                age,
            )
            return

        msg = BatteryState()
        msg.header.stamp = rospy.Time.now()
        msg.voltage = float(voltage)
        msg.current = float("nan")
        if charging is True and charging_current is not None and math.isfinite(charging_current):
            msg.current = max(0.0, float(charging_current))
        elif charging is False:
            msg.current = 0.0

        msg.charge = float("nan")
        msg.capacity = float("nan")
        msg.design_capacity = float("nan")
        msg.percentage = self._percentage_from_voltage(float(voltage))
        msg.present = True
        msg.power_supply_technology = self.power_supply_technology
        msg.serial_number = self.serial_number

        if charging is True:
            msg.power_supply_status = BatteryState.POWER_SUPPLY_STATUS_CHARGING
        elif charging is False:
            msg.power_supply_status = BatteryState.POWER_SUPPLY_STATUS_DISCHARGING
        else:
            msg.power_supply_status = BatteryState.POWER_SUPPLY_STATUS_UNKNOWN
        msg.power_supply_health = BatteryState.POWER_SUPPLY_HEALTH_UNKNOWN

        self._pub.publish(msg)


def main():
    rospy.init_node("wheeltec_battery_state_bridge")
    WheeltecBatteryStateBridge()
    rospy.spin()


if __name__ == "__main__":
    main()

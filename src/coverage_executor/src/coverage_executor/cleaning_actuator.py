# -*- coding: utf-8 -*-
import threading
from dataclasses import dataclass

import rospy
from geometry_msgs.msg import Twist

from my_msg_srv.msg import CleaningParams, ControlCleanTools, ControlMotor, ControlWaterTap
from coverage_planner.ops_store.store import OperationsStore


def _clamp_u8(val: int, lo: int = 0, hi: int = 255) -> int:
    return max(int(lo), min(int(hi), int(val)))


@dataclass
class CleaningProfile:
    profile_name: str = ""
    vel_water_pump: int = 0
    suction_machine_pwm: int = 0
    vacuum_motor_pwm: int = 0
    height_scrub: int = 38


class CleaningActuator:
    """ROS-backed device capability layer for cleaning hardware.

    Device semantics are fixed by the current M-core protocol:
      - brush      -> 5002 tool=0x01
      - scraper    -> 5002 tool=0x02
      - water      -> 5003 tap=0x01/0x02
      - suction    -> 5003 tap=0x05 + 5004

    `height_scrub` is stored and published as profile metadata only; it has no
    direct mapping in the current protocol and is intentionally not sent.
    """

    def __init__(self):
        self._cmd_vel_topic = rospy.get_param("~cmd_vel_topic", "/cmd_vel")
        self._clean_tools_topic = rospy.get_param("~clean_tools_topic", "/mcore/control_clean_tools")
        self._water_tap_topic = rospy.get_param("~water_tap_topic", "/mcore/control_water_tap")
        self._vacuum_motor_topic = rospy.get_param("~vacuum_motor_topic", "/mcore/control_motor")
        self._cleaning_params_topic = rospy.get_param("~cleaning_params_topic", "/mcore/cleaning_params/set")

        self._brush_tool_id = int(rospy.get_param("~brush_tool_id", 0x01))
        self._scraper_tool_id = int(rospy.get_param("~scraper_tool_id", 0x02))
        self._water_pump_tap_id = int(rospy.get_param("~water_pump_tap_id", 0x01))
        self._clean_water_valve_tap_id = int(rospy.get_param("~clean_water_valve_tap_id", 0x02))
        self._suction_tap_id = int(rospy.get_param("~suction_tap_id", 0x05))

        self._clean_tool_cmd_interval_s = float(rospy.get_param("~clean_tool_cmd_interval_s", 0.05))
        self._water_tap_cmd_interval_s = float(rospy.get_param("~water_tap_cmd_interval_s", 0.20))
        self._lower_brush_on_enable = bool(rospy.get_param("~lower_brush_on_enable", True))
        self._lower_scraper_on_enable = bool(rospy.get_param("~lower_scraper_on_enable", True))
        self._raise_brush_on_disable = bool(rospy.get_param("~raise_brush_on_disable", True))
        self._raise_scraper_on_disable = bool(rospy.get_param("~raise_scraper_on_disable", True))
        self._ops_db_path = str(rospy.get_param("~ops_db_path", "") or "").strip()
        self._ops_store = None
        if self._ops_db_path:
            try:
                self._ops_store = OperationsStore(self._ops_db_path)
            except Exception as exc:
                rospy.logwarn("[ACT] operations.db disabled: path=%s err=%s", self._ops_db_path, str(exc))
                self._ops_store = None

        self._cmd_pub = rospy.Publisher(self._cmd_vel_topic, Twist, queue_size=1)
        self._clean_tools_pub = rospy.Publisher(self._clean_tools_topic, ControlCleanTools, queue_size=10)
        self._water_tap_pub = rospy.Publisher(self._water_tap_topic, ControlWaterTap, queue_size=10)
        self._vacuum_motor_pub = rospy.Publisher(self._vacuum_motor_topic, ControlMotor, queue_size=10)
        self._cleaning_params_pub = rospy.Publisher(self._cleaning_params_topic, CleaningParams, queue_size=1, latch=True)

        self._lock = threading.RLock()
        self._current_profile = self._resolve_profile("")
        self._publish_current_params(reason="startup")

    def _resolve_profile(self, profile_name: str) -> CleaningProfile:
        raw_profiles = rospy.get_param("~actuator_profiles", {})
        defaults = CleaningProfile(
            profile_name=str(profile_name or rospy.get_param("~profile_name", "standard") or "standard"),
            vel_water_pump=_clamp_u8(rospy.get_param("~vel_water_pump", 28), 0, 64),
            suction_machine_pwm=_clamp_u8(rospy.get_param("~vel_water_suction", 36), 0, 64),
            vacuum_motor_pwm=_clamp_u8(rospy.get_param("~vel_water_suction", 36), 0, 64),
            height_scrub=_clamp_u8(rospy.get_param("~height_scrub", 38)),
        )

        if self._ops_store is not None:
            try:
                row = self._ops_store.get_actuator_profile(str(profile_name or defaults.profile_name))
            except Exception:
                row = None
            if row is not None:
                return CleaningProfile(
                    profile_name=str(row.actuator_profile_name or defaults.profile_name),
                    vel_water_pump=_clamp_u8(row.water_pump_pwm, 0, 64),
                    suction_machine_pwm=_clamp_u8(row.suction_machine_pwm, 0, 64),
                    vacuum_motor_pwm=_clamp_u8(row.vacuum_motor_pwm, 0, 64),
                    height_scrub=_clamp_u8(row.height_scrub),
                )

        if not isinstance(raw_profiles, dict):
            return defaults

        cfg = raw_profiles.get(str(profile_name or "").strip())
        if not isinstance(cfg, dict):
            return defaults

        return CleaningProfile(
            profile_name=str(profile_name or defaults.profile_name),
            vel_water_pump=_clamp_u8(cfg.get("vel_water_pump", defaults.vel_water_pump), 0, 64),
            suction_machine_pwm=_clamp_u8(
                cfg.get("suction_machine_pwm", cfg.get("vel_water_suction", defaults.suction_machine_pwm)),
                0,
                64,
            ),
            vacuum_motor_pwm=_clamp_u8(
                cfg.get("vacuum_motor_pwm", cfg.get("vel_water_suction", defaults.vacuum_motor_pwm)),
                0,
                64,
            ),
            height_scrub=_clamp_u8(cfg.get("height_scrub", defaults.height_scrub)),
        )

    def _build_params_msg(self, profile: CleaningProfile) -> CleaningParams:
        msg = CleaningParams()
        msg.profile_name = str(profile.profile_name or "")
        msg.vel_water_pump = int(profile.vel_water_pump)
        msg.vel_water_suction = int(profile.vacuum_motor_pwm)
        msg.height_scrub = int(profile.height_scrub)
        return msg

    def _publish_current_params(self, *, reason: str):
        with self._lock:
            profile = CleaningProfile(
                profile_name=self._current_profile.profile_name,
                vel_water_pump=self._current_profile.vel_water_pump,
                suction_machine_pwm=self._current_profile.suction_machine_pwm,
                vacuum_motor_pwm=self._current_profile.vacuum_motor_pwm,
                height_scrub=self._current_profile.height_scrub,
            )
        self._cleaning_params_pub.publish(self._build_params_msg(profile))
        rospy.loginfo(
            "[ACT] %s params profile=%s pump=%d suction_machine=%d vacuum_motor=%d height_scrub=%d (reserved/inactive in current protocol)",
            reason,
            profile.profile_name,
            profile.vel_water_pump,
            profile.suction_machine_pwm,
            profile.vacuum_motor_pwm,
            profile.height_scrub,
        )

    def _send_clean_tool(self, tool_id: int, operation: int):
        msg = ControlCleanTools()
        msg.tool_id = int(tool_id)
        msg.operation = int(operation)
        self._clean_tools_pub.publish(msg)

    def _send_tap(self, tap_id: int, operation: int):
        msg = ControlWaterTap()
        msg.tap_id = int(tap_id)
        msg.operation = int(operation)
        self._water_tap_pub.publish(msg)

    def _send_vacuum_motor(self, vel: int):
        msg = ControlMotor()
        msg.vel = int(_clamp_u8(vel, 0, 64))
        self._vacuum_motor_pub.publish(msg)

    def _pause_between_tool_cmds(self):
        if self._clean_tool_cmd_interval_s > 1e-3:
            rospy.sleep(self._clean_tool_cmd_interval_s)

    def _pause_between_water_cmds(self):
        if self._water_tap_cmd_interval_s > 1e-3:
            rospy.sleep(self._water_tap_cmd_interval_s)

    def apply_profile(self, profile_name: str):
        with self._lock:
            self._current_profile = self._resolve_profile(profile_name)
        self._publish_current_params(reason="apply_profile")

    # ---------- device capability layer ----------
    def brush_on(self):
        if self._lower_brush_on_enable:
            self._send_clean_tool(self._brush_tool_id, 0x02)
            self._pause_between_tool_cmds()
        self._send_clean_tool(self._brush_tool_id, 0x03)

    def brush_off(self):
        # Some chassis require an explicit stop opcode before close/retract.
        self._send_clean_tool(self._brush_tool_id, 0x00)
        self._pause_between_tool_cmds()
        self._send_clean_tool(self._brush_tool_id, 0x04)
        if self._raise_brush_on_disable:
            self._pause_between_tool_cmds()
            self._send_clean_tool(self._brush_tool_id, 0x01)

    def scraper_on(self):
        if self._lower_scraper_on_enable:
            self._send_clean_tool(self._scraper_tool_id, 0x02)

    def scraper_off(self):
        if self._raise_scraper_on_disable:
            self._send_clean_tool(self._scraper_tool_id, 0x01)

    def vacuum_on(self):
        with self._lock:
            suction_machine_pwm = int(self._current_profile.suction_machine_pwm)
            vacuum_motor_pwm = int(self._current_profile.vacuum_motor_pwm)
        self._send_tap(self._suction_tap_id, suction_machine_pwm)
        self._send_vacuum_motor(vacuum_motor_pwm)

    def vacuum_off(self):
        self._send_tap(self._suction_tap_id, 0)
        self._send_vacuum_motor(0)

    def water_on(self):
        with self._lock:
            pump_vel = int(self._current_profile.vel_water_pump)
        self._send_tap(self._clean_water_valve_tap_id, 0x01)
        self._pause_between_water_cmds()
        self._send_tap(self._water_pump_tap_id, pump_vel)
        rospy.loginfo(
            "[ACT] water_on valve_tap=%d pump_tap=%d pump_pwm=%d",
            self._clean_water_valve_tap_id,
            self._water_pump_tap_id,
            pump_vel,
        )

    def water_off(self):
        self._send_tap(self._water_pump_tap_id, 0x00)
        self._pause_between_water_cmds()
        self._send_tap(self._clean_water_valve_tap_id, 0x00)
        rospy.loginfo(
            "[ACT] water_off pump_tap=%d valve_tap=%d",
            self._water_pump_tap_id,
            self._clean_water_valve_tap_id,
        )

    # ---------- compatibility wrappers ----------
    def cleaning_on(self):
        self.scraper_on()
        self._pause_between_tool_cmds()
        self.brush_on()

    def cleaning_off(self):
        self.brush_off()
        self._pause_between_tool_cmds()
        self.scraper_off()

    def hard_stop_once(self):
        t = Twist()
        self._cmd_pub.publish(t)

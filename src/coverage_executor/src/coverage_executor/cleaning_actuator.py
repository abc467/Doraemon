# -*- coding: utf-8 -*-
import threading
from dataclasses import dataclass

import rospy
from geometry_msgs.msg import Twist

from robot_platform_msgs.msg import CleaningParams, ControlCleanTools, ControlMotor, ControlWaterTap
from coverage_planner.ops_store.store import OperationsStore

MCORE_ACTUATOR_MAX = 100


def _clamp_u8(val: int, lo: int = 0, hi: int = 255) -> int:
    return max(int(lo), min(int(hi), int(val)))


@dataclass
class CleaningProfile:
    profile_name: str = ""
    main_brush_speed: int = 0
    side_brush_speed: int = 0
    brush_down_distance: int = 0
    vel_water_pump: int = 0
    suction_machine_pwm: int = 0
    vacuum_motor_pwm: int = 0
    height_scrub: int = 0
    side_brush_enable: bool = False


class CleaningActuator:
    """ROS-backed device capability layer for cleaning hardware.

    Device semantics are fixed by the current M-core protocol:
      - brush      -> 5002 tool=0x01
      - scraper    -> 5002 tool=0x02
      - side brush -> 5002 tool=0x03
      - water      -> 5003 tap=0x01/0x02
      - sewage     -> 5003 tap=0x03 (OFF safety path)
      - suction    -> 5003 tap=0x05 + 5004

    `height_scrub` is kept for compatibility with older profile data. The new
    M-core serial protocol uses `brush_down_distance` for the brush lift command.
    """

    def __init__(self):
        self._cmd_vel_topic = rospy.get_param("~cmd_vel_topic", "/cmd_vel")
        self._clean_tools_topic = rospy.get_param("~clean_tools_topic", "/mcore/control_clean_tools")
        self._water_tap_topic = rospy.get_param("~water_tap_topic", "/mcore/control_water_tap")
        self._vacuum_motor_topic = rospy.get_param("~vacuum_motor_topic", "/mcore/control_motor")
        self._cleaning_params_topic = rospy.get_param("~cleaning_params_topic", "/mcore/cleaning_params/set")

        self._brush_tool_id = int(rospy.get_param("~brush_tool_id", 0x01))
        self._scraper_tool_id = int(rospy.get_param("~scraper_tool_id", 0x02))
        self._side_brush_tool_id = int(rospy.get_param("~side_brush_tool_id", 0x03))
        self._water_pump_tap_id = int(rospy.get_param("~water_pump_tap_id", 0x01))
        self._clean_water_valve_tap_id = int(rospy.get_param("~clean_water_valve_tap_id", 0x02))
        self._sewage_valve_tap_id = int(rospy.get_param("~sewage_valve_tap_id", 0x03))
        self._suction_tap_id = int(rospy.get_param("~suction_tap_id", 0x05))

        self._clean_tool_cmd_interval_s = float(rospy.get_param("~clean_tool_cmd_interval_s", 0.05))
        self._water_tap_cmd_interval_s = float(rospy.get_param("~water_tap_cmd_interval_s", 0.20))
        self._vacuum_off_repeat = max(1, int(rospy.get_param("~vacuum_off_repeat", 3)))
        self._lower_brush_on_enable = bool(rospy.get_param("~lower_brush_on_enable", True))
        self._lower_scraper_on_enable = bool(rospy.get_param("~lower_scraper_on_enable", True))
        self._raise_brush_on_disable = bool(rospy.get_param("~raise_brush_on_disable", True))
        self._raise_scraper_on_disable = bool(rospy.get_param("~raise_scraper_on_disable", True))
        self._side_brush_follows_brush = bool(rospy.get_param("~side_brush_follows_brush", True))
        self._side_brush_default_enable = bool(rospy.get_param("~side_brush_enable", False))
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
            main_brush_speed=_clamp_u8(rospy.get_param("~main_brush_speed", 0), 0, MCORE_ACTUATOR_MAX),
            side_brush_speed=_clamp_u8(rospy.get_param("~side_brush_speed", 0), 0, MCORE_ACTUATOR_MAX),
            brush_down_distance=_clamp_u8(rospy.get_param("~brush_down_distance", 0), 0, 1800),
            vel_water_pump=_clamp_u8(rospy.get_param("~vel_water_pump", 0), 0, MCORE_ACTUATOR_MAX),
            suction_machine_pwm=_clamp_u8(rospy.get_param("~vel_water_suction", 0), 0, MCORE_ACTUATOR_MAX),
            vacuum_motor_pwm=_clamp_u8(rospy.get_param("~vacuum_motor_pwm", rospy.get_param("~vel_water_suction", 0)), 0, MCORE_ACTUATOR_MAX),
            height_scrub=_clamp_u8(rospy.get_param("~height_scrub", 0)),
            side_brush_enable=bool(self._side_brush_default_enable),
        )

        if self._ops_store is not None:
            try:
                row = self._ops_store.get_actuator_profile(str(profile_name or defaults.profile_name))
            except Exception:
                row = None
            if row is not None:
                return CleaningProfile(
                    profile_name=str(row.actuator_profile_name or defaults.profile_name),
                    main_brush_speed=_clamp_u8(getattr(row, "main_brush_speed", defaults.main_brush_speed), 0, MCORE_ACTUATOR_MAX),
                    side_brush_speed=_clamp_u8(getattr(row, "side_brush_speed", defaults.side_brush_speed), 0, MCORE_ACTUATOR_MAX),
                    brush_down_distance=_clamp_u8(getattr(row, "brush_down_distance", defaults.brush_down_distance), 0, 1800),
                    vel_water_pump=_clamp_u8(row.water_pump_pwm, 0, MCORE_ACTUATOR_MAX),
                    suction_machine_pwm=_clamp_u8(row.suction_machine_pwm, 0, MCORE_ACTUATOR_MAX),
                    vacuum_motor_pwm=_clamp_u8(row.vacuum_motor_pwm, 0, MCORE_ACTUATOR_MAX),
                    height_scrub=_clamp_u8(row.height_scrub),
                    side_brush_enable=bool(getattr(row, "side_brush_enable", self._side_brush_default_enable)),
                )

        if not isinstance(raw_profiles, dict):
            return defaults

        cfg = raw_profiles.get(str(profile_name or "").strip())
        if not isinstance(cfg, dict):
            return defaults

        return CleaningProfile(
            profile_name=str(profile_name or defaults.profile_name),
            main_brush_speed=_clamp_u8(
                cfg.get("main_brush_speed", cfg.get("main_brush_pwm", defaults.main_brush_speed)),
                0,
                MCORE_ACTUATOR_MAX,
            ),
            side_brush_speed=_clamp_u8(
                cfg.get("side_brush_speed", cfg.get("side_brush_on_value", defaults.side_brush_speed)),
                0,
                MCORE_ACTUATOR_MAX,
            ),
            brush_down_distance=_clamp_u8(
                cfg.get("brush_down_distance", defaults.brush_down_distance),
                0,
                1800,
            ),
            vel_water_pump=_clamp_u8(cfg.get("vel_water_pump", defaults.vel_water_pump), 0, MCORE_ACTUATOR_MAX),
            suction_machine_pwm=_clamp_u8(
                cfg.get("suction_machine_pwm", cfg.get("vel_water_suction", defaults.suction_machine_pwm)),
                0,
                MCORE_ACTUATOR_MAX,
            ),
            vacuum_motor_pwm=_clamp_u8(
                cfg.get("vacuum_motor_pwm", cfg.get("vel_water_suction", defaults.vacuum_motor_pwm)),
                0,
                MCORE_ACTUATOR_MAX,
            ),
            height_scrub=_clamp_u8(cfg.get("height_scrub", defaults.height_scrub)),
            side_brush_enable=bool(cfg.get("side_brush_enable", defaults.side_brush_enable)),
        )

    def _build_params_msg(self, profile: CleaningProfile) -> CleaningParams:
        msg = CleaningParams()
        msg.profile_name = str(profile.profile_name or "")
        msg.vel_water_pump = int(profile.vel_water_pump)
        msg.vel_water_suction = int(profile.vacuum_motor_pwm)
        msg.height_scrub = int(profile.height_scrub)
        msg.main_brush_speed = int(profile.main_brush_speed)
        msg.side_brush_speed = int(profile.side_brush_speed)
        msg.brush_down_distance = int(profile.brush_down_distance)
        msg.side_brush_enable = bool(profile.side_brush_enable)
        return msg

    def _publish_current_params(self, *, reason: str):
        with self._lock:
            profile = CleaningProfile(
                profile_name=self._current_profile.profile_name,
                main_brush_speed=self._current_profile.main_brush_speed,
                side_brush_speed=self._current_profile.side_brush_speed,
                brush_down_distance=self._current_profile.brush_down_distance,
                vel_water_pump=self._current_profile.vel_water_pump,
                suction_machine_pwm=self._current_profile.suction_machine_pwm,
                vacuum_motor_pwm=self._current_profile.vacuum_motor_pwm,
                height_scrub=self._current_profile.height_scrub,
                side_brush_enable=bool(self._current_profile.side_brush_enable),
            )
        self._cleaning_params_pub.publish(self._build_params_msg(profile))
        rospy.loginfo(
            "[ACT] %s params profile=%s main_brush=%d side_brush=%d brush_down=%d pump=%d suction_machine=%d vacuum_motor=%d height_scrub=%d side_brush_enable=%s",
            reason,
            profile.profile_name,
            profile.main_brush_speed,
            profile.side_brush_speed,
            profile.brush_down_distance,
            profile.vel_water_pump,
            profile.suction_machine_pwm,
            profile.vacuum_motor_pwm,
            profile.height_scrub,
            str(bool(profile.side_brush_enable)),
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
        msg.vel = int(_clamp_u8(vel, 0, MCORE_ACTUATOR_MAX))
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
        if self._side_brush_should_follow():
            self._pause_between_tool_cmds()
            self.side_brush_on()

    def brush_off(self):
        if self._side_brush_follows_brush:
            self.side_brush_off()
            self._pause_between_tool_cmds()
        # Some chassis require an explicit stop opcode before close/retract.
        self._send_clean_tool(self._brush_tool_id, 0x00)
        self._pause_between_tool_cmds()
        self._send_clean_tool(self._brush_tool_id, 0x04)
        if self._raise_brush_on_disable:
            self._pause_between_tool_cmds()
            self._send_clean_tool(self._brush_tool_id, 0x01)

    def _side_brush_should_follow(self) -> bool:
        with self._lock:
            profile_enabled = bool(self._current_profile.side_brush_enable)
            side_brush_speed = int(self._current_profile.side_brush_speed)
        return bool(self._side_brush_follows_brush and profile_enabled and side_brush_speed > 0)

    def side_brush_on(self):
        self._send_clean_tool(self._side_brush_tool_id, 0x03)

    def side_brush_off(self):
        self._send_clean_tool(self._side_brush_tool_id, 0x04)

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
        repeats = max(1, int(self._vacuum_off_repeat))
        for idx in range(repeats):
            self._send_tap(self._suction_tap_id, 0)
            self._send_vacuum_motor(0)
            if idx + 1 < repeats:
                self._pause_between_water_cmds()
        rospy.loginfo(
            "[ACT] vacuum_off suction_tap=%d vacuum_motor=0 repeat=%d",
            self._suction_tap_id,
            repeats,
        )

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

    def sewage_valve_off(self):
        """Close the vehicle sewage valve used by station drain debugging."""
        self._send_tap(self._sewage_valve_tap_id, 0x00)
        rospy.loginfo("[ACT] sewage_valve_off tap=%d", self._sewage_valve_tap_id)

    def hard_stop_once(self):
        t = Twist()
        self._cmd_pub.publish(t)

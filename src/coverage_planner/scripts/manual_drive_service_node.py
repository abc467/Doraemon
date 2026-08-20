#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import threading
import time

import rospy
from cleanrobot_app_msgs.msg import OdometryState, SlamState
from cleanrobot_app_msgs.srv import (
    GetManualDriveStatus,
    GetManualDriveStatusResponse,
    ManualDriveCommand,
    ManualDriveCommandResponse,
)
from coverage_msgs.msg import TaskState as TaskStateMsg
from geometry_msgs.msg import Twist
from robot_platform_msgs.msg import CombinedStatus

from coverage_planner.manual_drive_controller import ManualDriveConfig, ManualDriveSafetyController
from coverage_planner.ros_contract import build_contract_report
from coverage_planner.service_mode import publish_contract_param


def _csv_param(name: str, default):
    raw = rospy.get_param(name, ",".join(default))
    if isinstance(raw, (list, tuple)):
        return tuple(str(item).strip() for item in raw if str(item).strip())
    return tuple(item.strip() for item in str(raw or "").split(",") if item.strip())


def _now_ms() -> int:
    return int(round(time.time() * 1000.0))


def _now_ns() -> int:
    return time.monotonic_ns()


def _positive_float_param(name: str, default: float, minimum: float) -> float:
    try:
        value = float(rospy.get_param(name, default))
    except Exception:
        value = float(default)
    return max(float(minimum), value)


def _strict_bool_param(name: str, default: bool) -> bool:
    value = rospy.get_param(name, default)
    if isinstance(value, bool):
        return value
    normalized = str(value or "").strip().lower()
    if normalized in ("1", "true", "yes", "on"):
        return True
    if normalized in ("0", "false", "no", "off"):
        return False
    raise RuntimeError("%s must be an explicit boolean" % name)


def _contract_param_for_service_name(service_name: str) -> str:
    name = str(service_name or "").strip()
    if "/app/" in name:
        prefix, leaf = name.split("/app/", 1)
        base = prefix.rstrip("/") or "/clean_robot_server"
        if not base.startswith("/"):
            base = "/%s" % base
        return "%s/contracts/app/%s" % (base, leaf.strip("/") or "manual_drive")
    leaf = name.strip("/").rsplit("/", 1)[-1] if name.strip("/") else "manual_drive"
    return "/clean_robot_server/contracts/app/%s" % leaf


class ManualDriveServiceNode:
    def __init__(self):
        self.robot_id = str(rospy.get_param("~robot_id", "local_robot")).strip() or "local_robot"
        self.command_service_name = (
            str(rospy.get_param("~command_service_name", "/clean_robot_server/app/manual_drive_command")).strip()
            or "/clean_robot_server/app/manual_drive_command"
        )
        self.status_service_name = (
            str(rospy.get_param("~status_service_name", "/clean_robot_server/app/get_manual_drive_status")).strip()
            or "/clean_robot_server/app/get_manual_drive_status"
        )
        self.contract_param_ns = (
            str(rospy.get_param("~contract_param_ns", "/clean_robot_server/contracts/app/manual_drive")).strip()
            or "/clean_robot_server/contracts/app/manual_drive"
        )
        self.command_contract_param_ns = (
            str(rospy.get_param("~command_contract_param_ns", _contract_param_for_service_name(self.command_service_name))).strip()
            or _contract_param_for_service_name(self.command_service_name)
        )
        self.status_contract_param_ns = (
            str(rospy.get_param("~status_contract_param_ns", _contract_param_for_service_name(self.status_service_name))).strip()
            or _contract_param_for_service_name(self.status_service_name)
        )
        self.slam_state_topic = str(rospy.get_param("~slam_state_topic", "/clean_robot_server/slam_state")).strip()
        self.task_state_topic = str(rospy.get_param("~task_state_topic", "/task_state")).strip()
        self.odometry_state_topic = str(rospy.get_param("~odometry_state_topic", "/clean_robot_server/odometry_state")).strip()
        self.combined_status_topic = str(rospy.get_param("~combined_status_topic", "/combined_status")).strip()

        enabled = _strict_bool_param("~enabled", True)
        no_action_acceptance = _strict_bool_param("~commercial_no_action_acceptance", True)
        action_test_approved = _strict_bool_param("~commercial_action_test_approved", False)
        require_role = _strict_bool_param("~require_role", False)
        require_slam_state = _strict_bool_param("~require_slam_state", False)
        require_task_state = _strict_bool_param("~require_task_state", False)
        require_odometry_state = _strict_bool_param("~require_odometry_state", False)
        require_combined_status = _strict_bool_param("~require_combined_status", True)
        if not enabled:
            raise RuntimeError("disabled manual-drive node must not be started")
        if no_action_acceptance or not action_test_approved:
            raise RuntimeError(
                "manual drive requires action-capable mode and explicit action-test approval"
            )
        if not require_combined_status:
            raise RuntimeError(
                "manual drive requires the physical platform safety gate: "
                "require_combined_status=true"
            )

        config = ManualDriveConfig(
            enabled=enabled,
            cmd_vel_topic=str(rospy.get_param("~cmd_vel_topic", "/cmd_vel")).strip() or "/cmd_vel",
            linear_mps_limit=max(0.01, float(rospy.get_param("~linear_mps_limit", 0.3))),
            angular_radps_limit=max(0.05, float(rospy.get_param("~angular_radps_limit", 0.5))),
            default_linear_mps=max(0.01, float(rospy.get_param("~default_linear_mps", 0.12))),
            default_angular_radps=max(0.05, float(rospy.get_param("~default_angular_radps", 0.35))),
            watchdog_timeout_ms=max(100, int(rospy.get_param("~watchdog_timeout_ms", 1000))),
            min_duration_ms=max(50, int(rospy.get_param("~min_duration_ms", 100))),
            publish_hz=_positive_float_param("~publish_hz", 20.0, 1.0),
            require_role=require_role,
            allowed_roles=_csv_param("~allowed_roles", ("operator", "service", "engineer", "admin")),
            allowed_capabilities=_csv_param(
                "~allowed_capabilities",
                ("manual_drive", "manual-drive", "robot:manual_drive"),
            ),
            require_slam_state=require_slam_state,
            require_task_state=require_task_state,
            require_odometry_state=require_odometry_state,
            require_combined_status=require_combined_status,
            slam_state_stale_timeout_s=max(0.2, float(rospy.get_param("~slam_state_stale_timeout_s", 2.0))),
            task_state_stale_timeout_s=max(0.2, float(rospy.get_param("~task_state_stale_timeout_s", 2.0))),
            odometry_state_stale_timeout_s=max(0.2, float(rospy.get_param("~odometry_state_stale_timeout_s", 2.0))),
            combined_status_stale_timeout_s=max(0.2, float(rospy.get_param("~combined_status_stale_timeout_s", 2.0))),
            supports_strafe=bool(rospy.get_param("~supports_strafe", False)),
        )
        self.controller = ManualDriveSafetyController(config)
        self._lock = threading.RLock()
        self._slam_state = None
        self._slam_state_ts = 0.0
        self._task_state = None
        self._task_state_ts = 0.0
        self._odometry_state = None
        self._odometry_state_ts = 0.0
        self._combined_status = None
        self._combined_status_ts = 0.0
        self._active = False
        self._last_direction = ""
        self._last_command_at_ms = 0
        self._last_move_at_ms = 0
        self._last_stop_at_ms = 0
        self._last_stop_token_ns = 0
        self._last_action = ""
        self._last_duration_ms = 0
        self._watchdog_deadline_ms = 0
        self._target_twist = None
        self._command_seq = 0
        self._request_seq = 0

        self._cmd_pub = rospy.Publisher(config.cmd_vel_topic, Twist, queue_size=1)
        rospy.Subscriber(self.slam_state_topic, SlamState, self._on_slam_state, queue_size=10)
        rospy.Subscriber(self.task_state_topic, TaskStateMsg, self._on_task_state, queue_size=10)
        rospy.Subscriber(self.odometry_state_topic, OdometryState, self._on_odometry_state, queue_size=10)
        rospy.Subscriber(self.combined_status_topic, CombinedStatus, self._on_combined_status, queue_size=10)
        self._command_srv = rospy.Service(self.command_service_name, ManualDriveCommand, self._handle_command)
        self._status_srv = rospy.Service(self.status_service_name, GetManualDriveStatus, self._handle_status)
        self._cmd_vel_timer = rospy.Timer(
            rospy.Duration(1.0 / float(config.publish_hz)),
            self._on_cmd_vel_timer,
        )
        rospy.on_shutdown(self._on_shutdown)
        publish_contract_param(rospy, self.command_contract_param_ns, self._build_command_contract_report(), enabled=True)
        publish_contract_param(rospy, self.status_contract_param_ns, self._build_status_contract_report(), enabled=True)
        publish_contract_param(rospy, self.contract_param_ns, self._build_contract_report(), enabled=True)
        rospy.loginfo(
            "[manual_drive] ready command=%s status=%s cmd_vel=%s linear<=%.3f angular<=%.3f watchdog=%dms publish=%.1fHz",
            self.command_service_name,
            self.status_service_name,
            config.cmd_vel_topic,
            config.linear_mps_limit,
            config.angular_radps_limit,
            config.watchdog_timeout_ms,
            config.publish_hz,
        )

    def _build_command_contract_report(self):
        return build_contract_report(
            service_name=self.command_service_name,
            contract_name="manual_drive_command_app",
            service_cls=ManualDriveCommand,
            request_cls=ManualDriveCommand._request_class,
            response_cls=ManualDriveCommandResponse,
            dependencies={},
            features=[
                "frontend_manual_drive",
                "continuous_cmd_vel_publish",
                "watchdog_stop",
                "cmd_vel_twist",
                "cleanrobot_app_msgs_parallel",
            ],
        )

    def _build_status_contract_report(self):
        return build_contract_report(
            service_name=self.status_service_name,
            contract_name="get_manual_drive_status_app",
            service_cls=GetManualDriveStatus,
            request_cls=GetManualDriveStatus._request_class,
            response_cls=GetManualDriveStatusResponse,
            dependencies={},
            features=[
                "frontend_manual_drive_status",
                "manual_drive_status",
                "watchdog_state",
                "cleanrobot_app_msgs_parallel",
            ],
        )

    def _build_contract_report(self):
        return {
            "contract_name": "manual_drive_app",
            "features": [
                "frontend_manual_drive",
                "watchdog_stop",
                "continuous_cmd_vel_publish",
                "optional_safety_gates",
                "cmd_vel_twist",
            ],
            "services": {
                "command": {
                    "name": self.command_service_name,
                    "type": "cleanrobot_app_msgs/ManualDriveCommand",
                    "http_gateway": "POST /api/manual-drive/command",
                },
                "status": {
                    "name": self.status_service_name,
                    "type": "cleanrobot_app_msgs/GetManualDriveStatus",
                    "http_gateway": "GET /api/manual-drive/status",
                },
            },
            "cmd_vel": {
                "topic": self.controller.config.cmd_vel_topic,
                "type": "geometry_msgs/Twist",
                "forward_backward": "linear.x, meters per second",
                "turn_left_right": "angular.z, radians per second; positive is left/counter-clockwise",
                "supports_strafe": bool(self.controller.config.supports_strafe),
                "publish_hz": float(self.controller.config.publish_hz),
            },
            "safety": {
                "allowed_roles": list(self.controller.config.allowed_roles),
                "allowed_capabilities": list(self.controller.config.allowed_capabilities),
                "require_role": bool(self.controller.config.require_role),
                "require_slam_state": bool(self.controller.config.require_slam_state),
                "require_task_state": bool(self.controller.config.require_task_state),
                "require_odometry_state": bool(self.controller.config.require_odometry_state),
                "require_combined_status": bool(self.controller.config.require_combined_status),
                "watchdog_timeout_ms": int(self.controller.config.watchdog_timeout_ms),
                "publish_hz": float(self.controller.config.publish_hz),
                "linear_mps_limit": float(self.controller.config.linear_mps_limit),
                "angular_radps_limit": float(self.controller.config.angular_radps_limit),
            },
        }

    def _on_slam_state(self, msg):
        with self._lock:
            self._slam_state = msg
            self._slam_state_ts = time.time()

    def _on_task_state(self, msg):
        with self._lock:
            self._task_state = msg
            self._task_state_ts = time.time()

    def _on_odometry_state(self, msg):
        with self._lock:
            self._odometry_state = msg
            self._odometry_state_ts = time.time()

    def _on_combined_status(self, msg):
        with self._lock:
            self._combined_status = msg
            self._combined_status_ts = time.time()

    def _make_twist(self, linear_x: float = 0.0, linear_y: float = 0.0, angular_z: float = 0.0) -> Twist:
        msg = Twist()
        msg.linear.x = float(linear_x)
        msg.linear.y = float(linear_y)
        msg.linear.z = 0.0
        msg.angular.x = 0.0
        msg.angular.y = 0.0
        msg.angular.z = float(angular_z)
        return msg

    def _log_zero_publish_locked(
        self,
        *,
        reason: str,
        now_ms: int,
        expire_at_ms: int,
        duration_ms=None,
        request_seq: int = 0,
    ):
        rospy.logwarn(
            (
                "[manual_drive] publish_zero reason=%s now_ms=%d now=%.3f "
                "expire_at_ms=%d expire_at=%.3f last_move_at_ms=%d last_move_at=%.3f "
                "last_action=%s duration_ms=%d request_seq=%d command_seq=%d active=%s"
            ),
            str(reason or "unknown"),
            int(now_ms or 0),
            float(now_ms or 0) / 1000.0,
            int(expire_at_ms or 0),
            float(expire_at_ms or 0) / 1000.0,
            int(self._last_move_at_ms or 0),
            float(self._last_move_at_ms or 0) / 1000.0,
            str(self._last_action or ""),
            int(self._last_duration_ms if duration_ms is None else duration_ms),
            int(request_seq or 0),
            int(self._command_seq or 0),
            str(bool(self._active)).lower(),
        )

    def _publish_zero_locked(
        self,
        *,
        reason: str,
        request_seq: int = 0,
        duration_ms=None,
        stop_token_ns=None,
        update_stop_token: bool = True,
    ):
        now_ms = _now_ms()
        expire_at_ms = int(self._watchdog_deadline_ms or 0)
        self._target_twist = None
        self._cmd_pub.publish(self._make_twist())
        self._active = False
        self._watchdog_deadline_ms = 0
        self._last_command_at_ms = now_ms
        self._last_stop_at_ms = now_ms
        if update_stop_token:
            stop_token = int(stop_token_ns if stop_token_ns is not None else _now_ns())
            self._last_stop_token_ns = max(int(self._last_stop_token_ns or 0), stop_token)
        self._command_seq += 1
        self._log_zero_publish_locked(
            reason=reason,
            now_ms=now_ms,
            expire_at_ms=expire_at_ms,
            duration_ms=duration_ms,
            request_seq=request_seq,
        )

    def _on_cmd_vel_timer(self, _event):
        with self._lock:
            if not self._active:
                return
            now_ms = _now_ms()
            if self._watchdog_deadline_ms > 0 and now_ms >= self._watchdog_deadline_ms:
                self._publish_zero_locked(
                    reason="expired",
                    request_seq=self._request_seq,
                    duration_ms=self._last_duration_ms,
                    update_stop_token=False,
                )
            elif self._target_twist is None:
                self._last_action = "no_target"
                self._publish_zero_locked(
                    reason="no_target",
                    request_seq=self._request_seq,
                    duration_ms=self._last_duration_ms,
                    update_stop_token=False,
                )
            else:
                linear_x, linear_y, angular_z = self._target_twist
                self._cmd_pub.publish(self._make_twist(linear_x=linear_x, linear_y=linear_y, angular_z=angular_z))

    def _on_shutdown(self):
        try:
            with self._lock:
                for _ in range(3):
                    self._last_action = "shutdown"
                    self._publish_zero_locked(
                        reason="shutdown",
                        request_seq=self._request_seq,
                        duration_ms=self._last_duration_ms,
                    )
                    rospy.sleep(0.02)
                self._active = False
                self._target_twist = None
                self._watchdog_deadline_ms = 0
        except Exception:
            pass

    def _current_blockers_locked(self, req) -> list:
        return self.controller.safety_blockers(
            now=time.time(),
            slam_state=self._slam_state,
            slam_state_ts=self._slam_state_ts,
            task_state=self._task_state,
            task_state_ts=self._task_state_ts,
            odometry_state=self._odometry_state,
            odometry_state_ts=self._odometry_state_ts,
            combined_status=self._combined_status,
            combined_status_ts=self._combined_status_ts,
            caller_role=str(getattr(req, "caller_role", "") or ""),
            caller_capabilities=list(getattr(req, "caller_capabilities", []) or []),
        )

    def _status_payload_locked(self, req):
        blockers = self._current_blockers_locked(req)
        allowed = bool(not blockers)
        return blockers, allowed

    def _fill_command_response(self, resp, *, req=None, blockers=None, allowed=False):
        config = self.controller.config
        resp.blocked_reasons = list(blockers or [])
        resp.enabled = bool(config.enabled)
        resp.active = bool(self._active)
        resp.allowed = bool(allowed)
        resp.last_direction = str(self._last_direction or "")
        resp.last_command_at = int(self._last_command_at_ms or 0)
        resp.watchdog_timeout_ms = int(config.watchdog_timeout_ms)
        resp.linear_mps_limit = float(config.linear_mps_limit)
        resp.angular_radps_limit = float(config.angular_radps_limit)
        resp.cmd_vel_topic = str(config.cmd_vel_topic)
        resp.cmd_vel_type = "geometry_msgs/Twist"
        resp.supports_strafe = bool(config.supports_strafe)
        resp.supported_directions = list(self.controller.supported_directions)
        return resp

    def _fill_status_response(self, resp, *, req=None, blockers=None, allowed=False):
        config = self.controller.config
        resp.blocked_reasons = list(blockers or [])
        resp.enabled = bool(config.enabled)
        resp.active = bool(self._active)
        resp.allowed = bool(allowed)
        resp.last_direction = str(self._last_direction or "")
        resp.last_command_at = int(self._last_command_at_ms or 0)
        resp.watchdog_timeout_ms = int(config.watchdog_timeout_ms)
        resp.linear_mps_limit = float(config.linear_mps_limit)
        resp.angular_radps_limit = float(config.angular_radps_limit)
        resp.cmd_vel_topic = str(config.cmd_vel_topic)
        resp.cmd_vel_type = "geometry_msgs/Twist"
        resp.supports_strafe = bool(config.supports_strafe)
        resp.supported_directions = list(self.controller.supported_directions)
        return resp

    def _handle_status(self, req):
        with self._lock:
            blockers, allowed = self._status_payload_locked(req)
            resp = GetManualDriveStatusResponse(success=True, message="ok")
            return self._fill_status_response(resp, req=req, blockers=blockers, allowed=allowed)

    def _handle_command(self, req):
        action = str(getattr(req, "action", "") or "").strip().lower()
        direction = str(getattr(req, "direction", "") or "").strip().lower()
        requested_duration_ms = int(getattr(req, "duration_ms", 0) or 0)
        received_ms = _now_ms()
        received_token_ns = _now_ns()
        resp = None
        with self._lock:
            self._request_seq += 1
            request_seq = int(self._request_seq or 0)
            rospy.loginfo(
                (
                    "[manual_drive] request_received request_seq=%d action=%s direction=%s "
                    "duration_ms=%d arrival_ms=%d arrival=%.3f"
                ),
                request_seq,
                action or "-",
                direction or "-",
                requested_duration_ms,
                received_ms,
                float(received_ms) / 1000.0,
            )
        try:
            with self._lock:
                resp = self._handle_command_locked(
                    req,
                    action=action,
                    direction=direction,
                    requested_duration_ms=requested_duration_ms,
                    received_ms=received_ms,
                    received_token_ns=received_token_ns,
                    request_seq=request_seq,
                )
            return resp
        finally:
            processing_ms = float(_now_ns() - received_token_ns) / 1000000.0
            with self._lock:
                expire_at_ms = int(self._watchdog_deadline_ms or 0)
                last_move_at_ms = int(self._last_move_at_ms or 0)
                last_action = str(self._last_action or "")
                active = bool(self._active)
            rospy.loginfo(
                (
                    "[manual_drive] request_done request_seq=%d action=%s direction=%s duration_ms=%d "
                    "arrival_ms=%d processing_ms=%.3f success=%s accepted=%s error_code=%s "
                    "active=%s expire_at_ms=%d last_move_at_ms=%d last_action=%s"
                ),
                int(request_seq or 0),
                action or "-",
                direction or "-",
                requested_duration_ms,
                received_ms,
                processing_ms,
                str(bool(getattr(resp, "success", False))).lower(),
                str(bool(getattr(resp, "accepted", False))).lower(),
                str(getattr(resp, "error_code", "") or ""),
                str(active).lower(),
                expire_at_ms,
                last_move_at_ms,
                last_action or "-",
            )

    def _handle_command_locked(
        self,
        req,
        *,
        action: str,
        direction: str,
        requested_duration_ms: int,
        received_ms: int,
        received_token_ns: int,
        request_seq: int,
    ):
        if action == "stop":
            self._last_action = "stop_request"
            self._last_duration_ms = requested_duration_ms
            self._publish_zero_locked(
                reason="stop_request",
                request_seq=request_seq,
                duration_ms=requested_duration_ms,
                stop_token_ns=received_token_ns,
            )
            blockers, allowed = self._status_payload_locked(req)
            resp = ManualDriveCommandResponse(
                success=True,
                accepted=True,
                message="stopped",
                error_code="",
            )
            return self._fill_command_response(resp, req=req, blockers=blockers, allowed=allowed)

        if action != "move":
            self._last_action = action or "unsupported_action"
            self._last_duration_ms = requested_duration_ms
            self._publish_zero_locked(
                reason="blocked",
                request_seq=request_seq,
                duration_ms=requested_duration_ms,
                stop_token_ns=received_token_ns,
            )
            resp = ManualDriveCommandResponse(
                success=False,
                accepted=False,
                message="unsupported manual drive action",
                error_code="unsupported_action",
            )
            return self._fill_command_response(
                resp,
                req=req,
                blockers=["unsupported manual drive action: %s" % (action or "-")],
                allowed=False,
            )

        blockers, allowed = self._status_payload_locked(req)
        if blockers:
            self._last_action = "blocked"
            self._last_duration_ms = requested_duration_ms
            self._publish_zero_locked(
                reason="blocked",
                request_seq=request_seq,
                duration_ms=requested_duration_ms,
                stop_token_ns=received_token_ns,
            )
            rospy.logwarn(
                "[manual_drive] request_blocked request_seq=%d blocked_reasons=%s",
                int(request_seq or 0),
                "; ".join(blockers),
            )
            resp = ManualDriveCommandResponse(
                success=False,
                accepted=False,
                message="manual drive blocked",
                error_code="manual_drive_blocked",
            )
            return self._fill_command_response(resp, req=req, blockers=blockers, allowed=False)

        try:
            linear_x, linear_y, angular_z = self.controller.velocity_for_request(
                direction=direction,
                linear_mps=float(getattr(req, "linear_mps", 0.0) or 0.0),
                angular_radps=float(getattr(req, "angular_radps", 0.0) or 0.0),
            )
        except Exception as exc:
            self._last_action = "blocked"
            self._last_duration_ms = requested_duration_ms
            self._publish_zero_locked(
                reason="blocked",
                request_seq=request_seq,
                duration_ms=requested_duration_ms,
                stop_token_ns=received_token_ns,
            )
            resp = ManualDriveCommandResponse(
                success=False,
                accepted=False,
                message=str(exc),
                error_code="invalid_direction",
            )
            return self._fill_command_response(resp, req=req, blockers=[str(exc)], allowed=False)

        if received_token_ns <= self._last_stop_token_ns:
            self._last_action = "stale_move"
            self._last_duration_ms = requested_duration_ms
            self._publish_zero_locked(
                reason="stale_move",
                request_seq=request_seq,
                duration_ms=requested_duration_ms,
            )
            resp = ManualDriveCommandResponse(
                success=False,
                accepted=False,
                message="stale move ignored after stop",
                error_code="stale_move",
            )
            return self._fill_command_response(
                resp,
                req=req,
                blockers=["stale move ignored after stop"],
                allowed=False,
            )

        duration_ms = self.controller.requested_duration_ms(requested_duration_ms)
        self._target_twist = (linear_x, linear_y, angular_z)
        self._cmd_pub.publish(self._make_twist(linear_x=linear_x, linear_y=linear_y, angular_z=angular_z))
        self._active = True
        self._last_direction = direction
        self._last_action = "move"
        self._last_duration_ms = duration_ms
        self._last_move_at_ms = received_ms
        self._last_command_at_ms = received_ms
        self._watchdog_deadline_ms = int(self._last_command_at_ms + duration_ms)
        self._command_seq += 1
        rospy.loginfo(
            (
                "[manual_drive] move_refresh request_seq=%d direction=%s linear_x=%.3f linear_y=%.3f "
                "angular_z=%.3f duration_ms=%d expire_at_ms=%d last_move_at_ms=%d"
            ),
            int(request_seq or 0),
            direction or "-",
            float(linear_x),
            float(linear_y),
            float(angular_z),
            int(duration_ms),
            int(self._watchdog_deadline_ms or 0),
            int(self._last_move_at_ms or 0),
        )
        resp = ManualDriveCommandResponse(
            success=True,
            accepted=True,
            message="move accepted",
            error_code="",
        )
        return self._fill_command_response(resp, req=req, blockers=[], allowed=allowed)


def main():
    rospy.init_node("manual_drive_service", anonymous=False)
    ManualDriveServiceNode()
    rospy.spin()


if __name__ == "__main__":
    main()

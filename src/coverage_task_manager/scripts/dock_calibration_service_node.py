#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import math
import os
import tempfile
import threading
import time
from typing import Optional, Tuple

import rospy
from geometry_msgs.msg import PoseStamped
from std_msgs.msg import Float32

from cleanrobot_app_msgs.msg import DockCalibrationState, SlamState
from cleanrobot_app_msgs.srv import (
    GetDockCalibrationStatus,
    GetDockCalibrationStatusResponse,
    OperateDockCalibration,
    OperateDockCalibrationRequest,
    OperateDockCalibrationResponse,
)
from coverage_planner.ros_contract import build_contract_report
from coverage_planner.service_mode import publish_contract_param

try:
    import yaml
except Exception:  # pragma: no cover - yaml is expected in ROS envs, json fallback is enough.
    yaml = None


def _now_ms() -> int:
    return int(round(time.time() * 1000.0))


def _text(value) -> str:
    return str(value or "").strip()


def _as_float(value, default: float = 0.0) -> float:
    try:
        f = float(value)
    except Exception:
        return float(default)
    if not math.isfinite(f):
        return float(default)
    return f


def _parse_xyyaw(value, default: Optional[Tuple[float, float, float]] = None):
    if default is None:
        default = (0.0, 0.0, 0.0)
    if isinstance(value, (list, tuple)) and len(value) >= 3:
        return (_as_float(value[0]), _as_float(value[1]), _as_float(value[2]))
    if isinstance(value, str):
        s = value.strip().strip("[]")
        parts = [p.strip() for p in s.replace(";", ",").split(",") if p.strip()]
        if len(parts) >= 3:
            return (_as_float(parts[0]), _as_float(parts[1]), _as_float(parts[2]))
    return tuple(default)


def _parse_xyyaw_strict(value):
    """Return a finite three-element pose, or None for an unset/invalid value."""
    values = None
    if isinstance(value, (list, tuple)) and len(value) == 3:
        values = value
    elif isinstance(value, str):
        s = value.strip().strip("[]")
        parts = [p.strip() for p in s.replace(";", ",").split(",") if p.strip()]
        if len(parts) == 3:
            values = parts
    if values is None:
        return None
    try:
        pose = tuple(float(item) for item in values)
    except (TypeError, ValueError):
        return None
    if not all(math.isfinite(item) for item in pose):
        return None
    return pose


def _validate_stage_pose(value):
    """Return a legal map-frame calibration pose without coercing bad input."""
    pose = _parse_xyyaw_strict(value)
    if pose is None:
        raise ValueError("dock calibration pose must contain three finite numbers")
    x, y, yaw = pose
    # A million metres is deliberately far outside any supported occupancy
    # map, while still avoiding an arbitrary site-size limit.
    if abs(x) > 1.0e6 or abs(y) > 1.0e6:
        raise ValueError("dock calibration x/y coordinate is outside the supported map range")
    if yaw < -math.pi or yaw > math.pi:
        raise ValueError("dock calibration yaw must be within [-pi, pi]")
    return pose


def _map_identity_match_issue(*, saved, active, runtime) -> str:
    saved_values = {key: _text((saved or {}).get(key, "")) for key in ("name", "id", "md5")}
    active_values = {key: _text((active or {}).get(key, "")) for key in ("name", "id", "md5")}
    runtime_values = {key: _text((runtime or {}).get(key, "")) for key in ("name", "id", "md5")}
    active_revision_id = _text((active or {}).get("revision_id", ""))
    runtime_revision_id = _text((runtime or {}).get("revision_id", ""))
    missing_saved = [key for key, value in saved_values.items() if not value]
    if missing_saved:
        return "saved dock points have incomplete map identity: missing %s" % ",".join(missing_saved)
    for key, expected in saved_values.items():
        if not expected:
            continue
        if active_values.get(key) != expected:
            return "saved dock map %s does not match active map" % key
    # The active identity describes the verified map asset, while the runtime
    # id/md5 may describe a live OccupancyGrid encoding of that same revision.
    # Prefer the canonical revision scope and retain exact legacy identity
    # checks only when either side does not publish a revision id.
    if active_revision_id and runtime_revision_id:
        if active_revision_id != runtime_revision_id:
            return "active and runtime map revision ids do not match"
        if runtime_values.get("name") != saved_values.get("name"):
            return "saved dock map name does not match runtime map"
        return ""
    for key, expected in saved_values.items():
        if runtime_values.get(key) != expected:
            return "saved dock map %s does not match runtime map" % key
    return ""


def _yaw_from_quat(q) -> float:
    x = _as_float(getattr(q, "x", 0.0))
    y = _as_float(getattr(q, "y", 0.0))
    z = _as_float(getattr(q, "z", 0.0))
    w = _as_float(getattr(q, "w", 1.0), 1.0)
    siny_cosp = 2.0 * (w * z + x * y)
    cosy_cosp = 1.0 - 2.0 * (y * y + z * z)
    return math.atan2(siny_cosp, cosy_cosp)


def _angle_abs(a: float) -> float:
    while a > math.pi:
        a -= 2.0 * math.pi
    while a < -math.pi:
        a += 2.0 * math.pi
    return abs(a)


class DockCalibrationServiceNode:
    def __init__(self):
        self.robot_id = _text(rospy.get_param("~robot_id", "local_robot")) or "local_robot"
        self.frame_id = _text(rospy.get_param("~frame_id", "map")) or "map"

        self.slam_state_topic = _text(rospy.get_param("~slam_state_topic", "/clean_robot_server/slam_state"))
        self.dock_pose_topic = _text(rospy.get_param("~dock_pose_topic", "/dock_pose"))
        self.dock_score_topic = _text(rospy.get_param("~dock_score_topic", "/dock_pose_score"))
        self.state_topic = _text(rospy.get_param("~state_topic", "/clean_robot_server/dock_calibration_state"))

        self.stage1_param_name = _text(
            rospy.get_param("~stage1_param_name", "/coverage_task_manager/dock_stage1_xyyaw")
        )
        self.stage2_param_name = _text(
            rospy.get_param("~stage2_param_name", "/coverage_task_manager/dock_xyyaw")
        )
        self.docking_target_dist_param_name = _text(
            rospy.get_param("~docking_target_dist_param_name", "/dock_supply_manager/docking_target_dist")
        )
        self.docking_controller_dist_param_name = _text(
            rospy.get_param("~docking_controller_dist_param_name", "/docking_controller/docking_distance")
        )
        self.docking_xy_tolerance_param_name = _text(
            rospy.get_param("~docking_xy_tolerance_param_name", "/docking_controller/xy_tolerance")
        )

        self.storage_path = _text(
            rospy.get_param("~storage_path", "/data/coverage/dock_calibration.yaml")
        )
        self.load_persisted_on_start = bool(rospy.get_param("~load_persisted_on_start", True))
        self.require_persisted_calibration = bool(
            rospy.get_param("~require_persisted_calibration", True)
        )
        self._runtime_config_map_name = _text(rospy.get_param("~runtime_config_map_name", ""))
        self._runtime_config_map_id = _text(rospy.get_param("~runtime_config_map_id", ""))
        self._runtime_config_map_md5 = _text(rospy.get_param("~runtime_config_map_md5", ""))

        self.slam_state_stale_timeout_s = max(0.2, float(rospy.get_param("~slam_state_stale_timeout_s", 2.0)))
        self.dock_pose_stale_timeout_s = max(0.1, float(rospy.get_param("~dock_pose_stale_timeout_s", 1.0)))
        self.dock_score_stale_timeout_s = max(0.1, float(rospy.get_param("~dock_score_stale_timeout_s", 1.0)))
        self.dock_score_threshold = float(rospy.get_param("~dock_score_threshold", 0.00012))
        self.dock_target_dist = float(rospy.get_param("~dock_target_dist", 0.780))
        self.dock_xy_tolerance = float(rospy.get_param("~dock_xy_tolerance", 0.005))
        self.stage2_min_extra_dist_m = max(0.0, float(rospy.get_param("~stage2_min_extra_dist_m", 0.20)))
        self.stage2_abs_y_max = max(0.0, float(rospy.get_param("~stage2_abs_y_max", 0.15)))
        self.stage2_abs_yaw_max_rad = max(0.0, float(rospy.get_param("~stage2_abs_yaw_max_rad", math.radians(8.0))))
        self.publish_hz = max(0.2, float(rospy.get_param("~publish_hz", 2.0)))

        self.status_service_name = _text(
            rospy.get_param("~status_service_name", "/clean_robot_server/app/get_dock_calibration_status")
        )
        self.command_service_name = _text(
            rospy.get_param("~command_service_name", "/clean_robot_server/app/dock_calibration_command")
        )
        self.contract_param_ns = _text(
            rospy.get_param("~contract_param_ns", "/clean_robot_server/contracts/app/dock_calibration")
        )
        self.status_contract_param_ns = _text(
            rospy.get_param(
                "~status_contract_param_ns",
                "/clean_robot_server/contracts/app/get_dock_calibration_status",
            )
        )
        self.command_contract_param_ns = _text(
            rospy.get_param(
                "~command_contract_param_ns",
                "/clean_robot_server/contracts/app/dock_calibration_command",
            )
        )

        self._lock = threading.RLock()
        self._slam_state = None
        self._slam_state_ts = 0.0
        self._dock_pose = None
        self._dock_pose_ts = 0.0
        self._dock_score = float("nan")
        self._dock_score_ts = 0.0
        self._saved_map_name = self._runtime_config_map_name
        self._saved_map_id = self._runtime_config_map_id
        self._saved_map_md5 = self._runtime_config_map_md5
        runtime_stage1 = self._stage_param(self.stage1_param_name)[1]
        runtime_stage2 = self._stage_param(self.stage2_param_name)[1]
        self._runtime_config_valid = bool(
            runtime_stage1 is not None
            and runtime_stage2 is not None
            and self._runtime_config_map_name
            and self._runtime_config_map_id
            and self._runtime_config_map_md5
        )
        self._calibration_source_valid = bool(
            self._runtime_config_valid and not self.require_persisted_calibration
        )
        self._persisted_calibration_loaded = False
        self._trusted_stage1 = runtime_stage1 if self._calibration_source_valid else None
        self._trusted_stage2 = runtime_stage2 if self._calibration_source_valid else None
        self._publish_persisted_loaded_marker()
        self._startup_reapply_remaining = 0
        self._startup_reapply_timer = None

        if self.load_persisted_on_start:
            loaded, _ = self._load_storage(apply_params=True, quiet=True)
            if loaded:
                self._startup_reapply_remaining = 4

        self._state_pub = rospy.Publisher(self.state_topic, DockCalibrationState, queue_size=1, latch=True)
        rospy.Subscriber(self.slam_state_topic, SlamState, self._on_slam_state, queue_size=10)
        rospy.Subscriber(self.dock_pose_topic, PoseStamped, self._on_dock_pose, queue_size=20)
        rospy.Subscriber(self.dock_score_topic, Float32, self._on_dock_score, queue_size=20)

        self._status_srv = rospy.Service(self.status_service_name, GetDockCalibrationStatus, self._handle_status)
        self._command_srv = rospy.Service(self.command_service_name, OperateDockCalibration, self._handle_command)
        self._timer = rospy.Timer(rospy.Duration(1.0 / self.publish_hz), self._on_timer)
        if self._startup_reapply_remaining > 0:
            self._startup_reapply_timer = rospy.Timer(rospy.Duration(3.0), self._on_startup_reapply_timer)

        publish_contract_param(rospy, self.status_contract_param_ns, self._build_status_contract_report(), enabled=True)
        publish_contract_param(rospy, self.command_contract_param_ns, self._build_command_contract_report(), enabled=True)
        publish_contract_param(rospy, self.contract_param_ns, self._build_contract_report(), enabled=True)

        rospy.loginfo(
            "[dock_calib] ready status=%s command=%s state=%s stage1_param=%s stage2_param=%s score=%s storage=%s",
            self.status_service_name,
            self.command_service_name,
            self.state_topic,
            self.stage1_param_name,
            self.stage2_param_name,
            self.dock_score_topic,
            self.storage_path,
        )

    def _build_status_contract_report(self):
        return build_contract_report(
            service_name=self.status_service_name,
            contract_name="get_dock_calibration_status_app",
            service_cls=GetDockCalibrationStatus,
            request_cls=GetDockCalibrationStatus._request_class,
            response_cls=GetDockCalibrationStatusResponse,
            dependencies={"state": DockCalibrationState},
            features=[
                "dock_calibration_status",
                "tracked_pose_snapshot",
                "dock_tracker_score",
                "stage1_stage2_pose_readback",
            ],
        )

    def _build_command_contract_report(self):
        return build_contract_report(
            service_name=self.command_service_name,
            contract_name="dock_calibration_command_app",
            service_cls=OperateDockCalibration,
            request_cls=OperateDockCalibration._request_class,
            response_cls=OperateDockCalibrationResponse,
            dependencies={"state": DockCalibrationState},
            features=[
                "save_stage1_from_current_pose",
                "save_stage2_from_current_pose",
                "manual_stage_pose_set",
                "precise_docking_parameter_set",
                "persistent_dock_points",
                "hot_rosparam_apply",
            ],
        )

    def _build_contract_report(self):
        return {
            "contract_name": "dock_calibration_app",
            "features": [
                "dock_stage1_stage2_calibration",
                "frontend_operator_quality_judgement",
                "dock_pose_score_display",
                "precise_docking_parameter_tuning",
                "persistent_dock_points",
            ],
            "services": {
                "status": {
                    "name": self.status_service_name,
                    "type": "cleanrobot_app_msgs/GetDockCalibrationStatus",
                    "http_gateway": "GET /api/dock-calibration/status",
                },
                "command": {
                    "name": self.command_service_name,
                    "type": "cleanrobot_app_msgs/OperateDockCalibration",
                    "http_gateway": "POST /api/dock-calibration/command",
                },
            },
            "topics": {
                "state": {
                    "name": self.state_topic,
                    "type": "cleanrobot_app_msgs/DockCalibrationState",
                },
                "dock_score": {
                    "name": self.dock_score_topic,
                    "type": "std_msgs/Float32",
                    "semantics": "ICP fitness score, lower is better",
                },
            },
        }

    def _on_slam_state(self, msg: SlamState):
        incoming_robot_id = _text(getattr(msg, "robot_id", ""))
        if incoming_robot_id != self.robot_id:
            rospy.logwarn_throttle(
                5.0,
                "[dock_calib] ignored slam state for robot_id=%s local=%s",
                incoming_robot_id or "-",
                self.robot_id,
            )
            return
        with self._lock:
            self._slam_state = msg
            self._slam_state_ts = time.time()

    def _on_dock_pose(self, msg: PoseStamped):
        with self._lock:
            self._dock_pose = msg
            self._dock_pose_ts = time.time()

    def _on_dock_score(self, msg: Float32):
        with self._lock:
            self._dock_score = _as_float(getattr(msg, "data", float("nan")), float("nan"))
            self._dock_score_ts = time.time()

    def _on_timer(self, _event):
        self._state_pub.publish(self._build_state())

    def _on_startup_reapply_timer(self, _event):
        with self._lock:
            if self._startup_reapply_remaining <= 0:
                if self._startup_reapply_timer is not None:
                    self._startup_reapply_timer.shutdown()
                    self._startup_reapply_timer = None
                return
            target = float(self.dock_target_dist)
            tolerance = float(self.dock_xy_tolerance)
            self._startup_reapply_remaining -= 1
        self._apply_dock_params(target=target, tolerance=tolerance)
        rospy.loginfo(
            "[dock_calib] reapplied persisted dock params target=%.3f tolerance=%.3f remaining=%d",
            target,
            tolerance,
            self._startup_reapply_remaining,
        )
        if self._startup_reapply_remaining <= 0 and self._startup_reapply_timer is not None:
            self._startup_reapply_timer.shutdown()
            self._startup_reapply_timer = None

    def _handle_status(self, req):
        state = self._build_state()
        robot_id = _text(getattr(req, "robot_id", ""))
        if robot_id != self.robot_id:
            return GetDockCalibrationStatusResponse(
                success=False,
                message="robot_id mismatch: requested=%s local=%s" % (robot_id, self.robot_id),
                state=state,
            )
        return GetDockCalibrationStatusResponse(success=True, message="ok", state=state)

    def _handle_command(self, req):
        robot_id = _text(getattr(req, "robot_id", ""))
        if robot_id != self.robot_id:
            return self._command_response(False, "ROBOT_ID_MISMATCH", "robot_id mismatch", int(req.operation))

        op = int(getattr(req, "operation", 0))
        try:
            if op == int(OperateDockCalibrationRequest.GET):
                return self._command_response(True, "", "ok", op)
            if op == int(OperateDockCalibrationRequest.SAVE_STAGE1):
                return self._save_current_pose(stage=1, require_quality=False, operation=op)
            if op == int(OperateDockCalibrationRequest.SAVE_STAGE2):
                return self._save_current_pose(
                    stage=2,
                    require_quality=bool(getattr(req, "require_stage2_quality", False)),
                    operation=op,
                )
            if op == int(OperateDockCalibrationRequest.SET_STAGE1):
                return self._set_stage(1, req.x, req.y, req.yaw, operation=op)
            if op == int(OperateDockCalibrationRequest.SET_STAGE2):
                return self._set_stage(2, req.x, req.y, req.yaw, operation=op)
            if op == int(OperateDockCalibrationRequest.RELOAD):
                ok, msg = self._load_storage(apply_params=True, quiet=False)
                return self._command_response(ok, "" if ok else "LOAD_FAILED", msg, op)
            if op == int(OperateDockCalibrationRequest.SET_DOCK_PARAMS):
                return self._set_dock_params(
                    getattr(req, "dock_target_dist", 0.0),
                    getattr(req, "dock_xy_tolerance", 0.0),
                    operation=op,
                )
        except Exception as e:
            rospy.logerr("[dock_calib] command failed op=%s: %s", str(op), str(e))
            return self._command_response(False, "COMMAND_FAILED", str(e), op)
        return self._command_response(False, "UNKNOWN_OPERATION", "unknown operation", op)

    def _command_response(self, success: bool, error_code: str, message: str, operation: int):
        return OperateDockCalibrationResponse(
            success=bool(success),
            message=str(message or ""),
            error_code=str(error_code or ""),
            operation=int(operation),
            state=self._build_state(),
        )

    def _save_current_pose(self, *, stage: int, require_quality: bool, operation: int):
        state = self._build_state()
        if not state.tracked_pose_fresh or state.tracked_pose_frame != self.frame_id:
            return self._command_response(
                False,
                "TRACKED_POSE_NOT_READY",
                "tracked pose is not fresh in %s frame" % self.frame_id,
                operation,
            )
        if stage == 2 and require_quality and not state.stage2_save_recommended:
            return self._command_response(
                False,
                "STAGE2_QUALITY_NOT_RECOMMENDED",
                "stage2 quality is not recommended: %s" % "; ".join(state.warnings),
                operation,
            )
        return self._set_stage(stage, state.current_x, state.current_y, state.current_yaw, operation=operation)

    def _set_stage(self, stage: int, x, y, yaw, *, operation: int):
        pose = list(_validate_stage_pose((x, y, yaw)))
        stage_number = int(stage)
        if stage_number not in (1, 2):
            raise ValueError("dock calibration stage must be 1 or 2")
        param_name = self.stage1_param_name if stage_number == 1 else self.stage2_param_name
        other_param_name = self.stage2_param_name if stage_number == 1 else self.stage1_param_name
        with self._lock:
            current_identity = self._current_calibration_map_identity()
            snapshots = {}
            for name in (self.stage1_param_name, self.stage2_param_name):
                exists = bool(name and rospy.has_param(name))
                snapshots[name] = (exists, rospy.get_param(name) if exists else None)
            metadata_snapshot = (
                self._saved_map_name,
                self._saved_map_id,
                self._saved_map_md5,
                self._persisted_calibration_loaded,
                self._calibration_source_valid,
                self._trusted_stage1,
                self._trusted_stage2,
            )
            saved_identity = self._saved_map_identity()
            same_round = bool(
                self._calibration_source_valid
                and self._map_identities_equal(saved_identity, current_identity)
            )
            if not same_round:
                # A map switch starts a new calibration round. Never carry the
                # other stage from the old map into the new map binding.
                self._clear_stage_param(other_param_name)
            stage1 = tuple(pose) if stage_number == 1 else (self._trusted_stage1 if same_round else None)
            stage2 = tuple(pose) if stage_number == 2 else (self._trusted_stage2 if same_round else None)
            if stage1 is None:
                self._clear_stage_param(self.stage1_param_name)
            else:
                rospy.set_param(self.stage1_param_name, list(stage1))
            if stage2 is None:
                self._clear_stage_param(self.stage2_param_name)
            else:
                rospy.set_param(self.stage2_param_name, list(stage2))
            try:
                self._persist_storage(
                    expected_map_identity=current_identity,
                    stage1=stage1,
                    stage2=stage2,
                )
            except Exception:
                for name, (existed, previous_value) in snapshots.items():
                    if existed:
                        rospy.set_param(name, previous_value)
                    else:
                        self._clear_stage_param(name)
                (
                    self._saved_map_name,
                    self._saved_map_id,
                    self._saved_map_md5,
                    self._persisted_calibration_loaded,
                    self._calibration_source_valid,
                    self._trusted_stage1,
                    self._trusted_stage2,
                ) = metadata_snapshot
                self._publish_persisted_loaded_marker()
                raise
        self._state_pub.publish(self._build_state())
        return self._command_response(
            True,
            "",
            "saved stage%d xyyaw=[%.6f, %.6f, %.6f]" % (stage_number, pose[0], pose[1], pose[2]),
            operation,
        )

    def _set_dock_params(self, dock_target_dist, dock_xy_tolerance, *, operation: int):
        target = _as_float(dock_target_dist, float("nan"))
        tolerance = _as_float(dock_xy_tolerance, float("nan"))
        if not math.isfinite(target) or target < 0.20 or target > 2.00:
            return self._command_response(
                False,
                "INVALID_DOCK_TARGET_DIST",
                "dock_target_dist must be within [0.20, 2.00] meters",
                operation,
            )
        if not math.isfinite(tolerance) or tolerance < 0.0 or tolerance > 0.05:
            return self._command_response(
                False,
                "INVALID_DOCK_XY_TOLERANCE",
                "dock_xy_tolerance must be within [0.000, 0.050] meters",
                operation,
            )

        with self._lock:
            current_identity = self._current_calibration_map_identity()
            if not self._calibration_source_valid or not self._map_identities_equal(
                self._saved_map_identity(), current_identity
            ):
                raise ValueError(
                    "dock parameters may only be changed on the map already bound to the calibration"
                )
            previous_target = self.dock_target_dist
            previous_tolerance = self.dock_xy_tolerance
            param_snapshots = {}
            for name in (
                self.docking_target_dist_param_name,
                self.docking_controller_dist_param_name,
                self.docking_xy_tolerance_param_name,
            ):
                if not name or name in param_snapshots:
                    continue
                exists = rospy.has_param(name)
                param_snapshots[name] = (exists, rospy.get_param(name) if exists else None)
            self.dock_target_dist = float(target)
            self.dock_xy_tolerance = float(tolerance)
            try:
                self._apply_dock_params(
                    target=self.dock_target_dist,
                    tolerance=self.dock_xy_tolerance,
                )
                self._persist_storage(
                    expected_map_identity=current_identity,
                    require_existing_binding=True,
                    stage1=self._trusted_stage1,
                    stage2=self._trusted_stage2,
                )
            except Exception:
                self.dock_target_dist = previous_target
                self.dock_xy_tolerance = previous_tolerance
                for name, (existed, previous_value) in param_snapshots.items():
                    if existed:
                        rospy.set_param(name, previous_value)
                    else:
                        self._clear_stage_param(name)
                raise
        self._state_pub.publish(self._build_state())
        return self._command_response(
            True,
            "",
            "updated dock params target=%.3f tolerance=%.3f threshold=%.3f"
            % (self.dock_target_dist, self.dock_xy_tolerance, self.dock_target_dist + self.dock_xy_tolerance),
            operation,
        )

    def _stage_param(self, param_name: str):
        if not param_name or not rospy.has_param(param_name):
            return False, None
        try:
            pose = _validate_stage_pose(rospy.get_param(param_name))
        except ValueError:
            pose = None
        return pose is not None, pose

    @staticmethod
    def _pose_or_zero(pose):
        return pose if pose is not None else (0.0, 0.0, 0.0)

    def _clear_stage_param(self, param_name: str):
        if not param_name:
            return
        try:
            if rospy.has_param(param_name):
                rospy.delete_param(param_name)
        except Exception:
            pass

    def _publish_persisted_loaded_marker(self):
        try:
            rospy.set_param(
                "~persisted_calibration_loaded",
                bool(self._persisted_calibration_loaded),
            )
        except Exception:
            pass

    def _invalidate_loaded_calibration(self):
        self._persisted_calibration_loaded = False
        self._calibration_source_valid = bool(
            self._runtime_config_valid and not self.require_persisted_calibration
        )
        self._saved_map_name = self._runtime_config_map_name
        self._saved_map_id = self._runtime_config_map_id
        self._saved_map_md5 = self._runtime_config_map_md5
        self._trusted_stage1 = None
        self._trusted_stage2 = None
        if self._calibration_source_valid:
            self._trusted_stage1 = self._stage_param(self.stage1_param_name)[1]
            self._trusted_stage2 = self._stage_param(self.stage2_param_name)[1]
        if not self._calibration_source_valid:
            self._clear_stage_param(self.stage1_param_name)
            self._clear_stage_param(self.stage2_param_name)
        self._publish_persisted_loaded_marker()

    def _target_dist(self) -> float:
        if self.docking_target_dist_param_name and rospy.has_param(self.docking_target_dist_param_name):
            return _as_float(rospy.get_param(self.docking_target_dist_param_name), self.dock_target_dist)
        return float(self.dock_target_dist)

    def _xy_tolerance(self) -> float:
        if self.docking_xy_tolerance_param_name and rospy.has_param(self.docking_xy_tolerance_param_name):
            return _as_float(rospy.get_param(self.docking_xy_tolerance_param_name), self.dock_xy_tolerance)
        return float(self.dock_xy_tolerance)

    def _apply_dock_params(self, *, target: float, tolerance: float):
        if self.docking_target_dist_param_name:
            rospy.set_param(self.docking_target_dist_param_name, float(target))
        if self.docking_controller_dist_param_name:
            rospy.set_param(self.docking_controller_dist_param_name, float(target))
        if self.docking_xy_tolerance_param_name:
            rospy.set_param(self.docking_xy_tolerance_param_name, float(tolerance))

    def _current_pose_from_slam(self, now: float):
        msg = self._slam_state
        age = (now - self._slam_state_ts) if self._slam_state_ts > 0.0 else float("inf")
        if msg is None:
            return False, age, "", 0.0, 0.0, 0.0
        pose_age = max(_as_float(getattr(msg, "tracked_pose_age_s", age), age), age)
        frame = _text(getattr(msg, "tracked_pose_frame", ""))
        pose = _parse_xyyaw_strict(
            (
                getattr(msg, "tracked_pose_x", None),
                getattr(msg, "tracked_pose_y", None),
                getattr(msg, "tracked_pose_theta", None),
            )
        )
        robot_matches = _text(getattr(msg, "robot_id", "")) == self.robot_id
        fresh = bool(
            robot_matches
            and pose is not None
            and getattr(msg, "tracked_pose_fresh", False)
            and pose_age <= self.slam_state_stale_timeout_s
        )
        if pose is None:
            pose = (0.0, 0.0, 0.0)
        return (
            fresh,
            pose_age,
            frame,
            pose[0],
            pose[1],
            pose[2],
        )

    def _build_state(self):
        with self._lock:
            now = time.time()
            msg = self._slam_state
            stage1_present, stage1 = self._stage_param(self.stage1_param_name)
            stage2_present, stage2 = self._stage_param(self.stage2_param_name)
            stage1_matches_trusted = bool(
                stage1_present
                and self._trusted_stage1 is not None
                and tuple(stage1) == tuple(self._trusted_stage1)
            )
            stage2_matches_trusted = bool(
                stage2_present
                and self._trusted_stage2 is not None
                and tuple(stage2) == tuple(self._trusted_stage2)
            )
            stage1_set = bool(stage1_matches_trusted and self._calibration_source_valid)
            stage2_set = bool(stage2_matches_trusted and self._calibration_source_valid)
            stage1 = self._pose_or_zero(self._trusted_stage1)
            stage2 = self._pose_or_zero(self._trusted_stage2)
            pose_fresh, pose_age, pose_frame, current_x, current_y, current_yaw = self._current_pose_from_slam(now)
            dock_pose_age = (now - self._dock_pose_ts) if self._dock_pose_ts > 0.0 else float("inf")
            dock_pose_fresh = bool(self._dock_pose is not None and dock_pose_age <= self.dock_pose_stale_timeout_s)
            dock_score_age = (now - self._dock_score_ts) if self._dock_score_ts > 0.0 else float("inf")
            dock_score_fresh = bool(math.isfinite(self._dock_score) and dock_score_age <= self.dock_score_stale_timeout_s)

            dock_pose_frame = ""
            dock_x = 0.0
            dock_y = 0.0
            dock_yaw = 0.0
            if self._dock_pose is not None:
                dock_pose_frame = _text(getattr(self._dock_pose.header, "frame_id", ""))
                dock_x = _as_float(getattr(self._dock_pose.pose.position, "x", 0.0))
                dock_y = _as_float(getattr(self._dock_pose.pose.position, "y", 0.0))
                dock_yaw = _yaw_from_quat(self._dock_pose.pose.orientation)

            target_dist = self._target_dist()
            xy_tolerance = self._xy_tolerance()
            success_threshold = target_dist + xy_tolerance
            min_dock_x = target_dist + self.stage2_min_extra_dist_m
            quality_ok = bool(
                dock_pose_fresh
                and dock_score_fresh
                and self._dock_score <= self.dock_score_threshold
                and dock_x >= min_dock_x
                and abs(dock_y) <= self.stage2_abs_y_max
                and _angle_abs(dock_yaw) <= self.stage2_abs_yaw_max_rad
            )
            stage2_save_recommended = bool(pose_fresh and pose_frame == self.frame_id and quality_ok)

            warnings = []
            if self._calibration_source_valid and stage1_present and not stage1_matches_trusted:
                warnings.append("dock stage1 ROS parameter differs from trusted calibration")
            if self._calibration_source_valid and stage2_present and not stage2_matches_trusted:
                warnings.append("dock stage2 ROS parameter differs from trusted calibration")
            if not pose_fresh:
                warnings.append("tracked_pose is stale or missing")
            elif pose_frame != self.frame_id:
                warnings.append("tracked_pose frame is %s, expected %s" % (pose_frame or "-", self.frame_id))
            if not dock_score_fresh:
                warnings.append("dock score is stale or missing")
            elif self._dock_score > self.dock_score_threshold:
                warnings.append(
                    "dock score %.6f is above threshold %.6f" % (self._dock_score, self.dock_score_threshold)
                )
            if not dock_pose_fresh:
                warnings.append("dock_pose is stale or missing")
            else:
                if dock_x < min_dock_x:
                    warnings.append("dock_pose.x %.3f is below recommended %.3f" % (dock_x, min_dock_x))
                if abs(dock_y) > self.stage2_abs_y_max:
                    warnings.append("abs(dock_pose.y) %.3f is above %.3f" % (abs(dock_y), self.stage2_abs_y_max))
                if _angle_abs(dock_yaw) > self.stage2_abs_yaw_max_rad:
                    warnings.append(
                        "abs(dock_pose.yaw) %.3f is above %.3f"
                        % (_angle_abs(dock_yaw), self.stage2_abs_yaw_max_rad)
                    )
            if stage1_present or stage2_present:
                if not self._calibration_source_valid:
                    warnings.append("dock points are not backed by vehicle-bound calibration")
                elif msg is None:
                    warnings.append("saved dock map identity cannot be checked without slam state")
                else:
                    map_issue = _map_identity_match_issue(
                        saved={
                            "name": self._saved_map_name,
                            "id": self._saved_map_id,
                            "md5": self._saved_map_md5,
                        },
                        active={
                            "name": getattr(msg, "active_map_name", ""),
                            "revision_id": getattr(msg, "active_map_revision_id", ""),
                            "id": getattr(msg, "active_map_id", ""),
                            "md5": getattr(msg, "active_map_md5", ""),
                        },
                        runtime={
                            "name": getattr(msg, "runtime_map_name", ""),
                            "revision_id": getattr(msg, "runtime_map_revision_id", ""),
                            "id": getattr(msg, "runtime_map_id", ""),
                            "md5": getattr(msg, "runtime_map_md5", ""),
                        },
                    )
                    if map_issue:
                        warnings.append(map_issue)

            state = DockCalibrationState()
            state.robot_id = self.robot_id
            state.stamp = rospy.Time.now()
            state.frame_id = self.frame_id

            if msg is not None:
                state.active_map_name = _text(getattr(msg, "active_map_name", ""))
                state.active_map_id = _text(getattr(msg, "active_map_id", ""))
                state.active_map_md5 = _text(getattr(msg, "active_map_md5", ""))
                state.runtime_map_name = _text(getattr(msg, "runtime_map_name", ""))
                state.runtime_map_id = _text(getattr(msg, "runtime_map_id", ""))
                state.runtime_map_md5 = _text(getattr(msg, "runtime_map_md5", ""))
                state.runtime_map_ready = bool(getattr(msg, "runtime_map_ready", False))
                state.active_map_match = bool(getattr(msg, "active_map_match", False))
                state.localization_state = _text(getattr(msg, "localization_state", ""))
                state.localization_valid = bool(getattr(msg, "localization_valid", False))

            state.tracked_pose_fresh = bool(pose_fresh)
            state.tracked_pose_age_s = float(pose_age if math.isfinite(pose_age) else -1.0)
            state.tracked_pose_frame = pose_frame
            state.current_x = current_x
            state.current_y = current_y
            state.current_yaw = current_yaw

            state.stage1_set = bool(stage1_set)
            state.stage1_x = stage1[0]
            state.stage1_y = stage1[1]
            state.stage1_yaw = stage1[2]
            state.stage2_set = bool(stage2_set)
            state.stage2_x = stage2[0]
            state.stage2_y = stage2[1]
            state.stage2_yaw = stage2[2]

            state.dock_pose_fresh = bool(dock_pose_fresh)
            state.dock_pose_age_s = float(dock_pose_age if math.isfinite(dock_pose_age) else -1.0)
            state.dock_pose_frame = dock_pose_frame
            state.dock_pose_x = dock_x
            state.dock_pose_y = dock_y
            state.dock_pose_yaw = dock_yaw

            state.dock_score_fresh = bool(dock_score_fresh)
            state.dock_score_age_s = float(dock_score_age if math.isfinite(dock_score_age) else -1.0)
            state.dock_score = float(self._dock_score if math.isfinite(self._dock_score) else 0.0)
            state.dock_score_threshold = float(self.dock_score_threshold)
            state.dock_score_lower_is_better = True

            state.dock_target_dist = float(target_dist)
            state.dock_xy_tolerance = float(xy_tolerance)
            state.dock_success_threshold = float(success_threshold)
            state.stage2_min_extra_dist_m = float(self.stage2_min_extra_dist_m)
            state.stage2_min_dock_pose_x = float(min_dock_x)
            state.stage2_abs_y_max = float(self.stage2_abs_y_max)
            state.stage2_abs_yaw_max_rad = float(self.stage2_abs_yaw_max_rad)
            state.dock_pose_quality_ok = bool(quality_ok)
            state.stage2_save_recommended = bool(stage2_save_recommended)

            state.saved_map_name = self._saved_map_name
            state.saved_map_id = self._saved_map_id
            state.saved_map_md5 = self._saved_map_md5
            state.storage_path = self.storage_path
            state.warnings = warnings
            return state

    def _saved_map_identity(self):
        return {
            "name": _text(self._saved_map_name),
            "id": _text(self._saved_map_id),
            "md5": _text(self._saved_map_md5),
        }

    @staticmethod
    def _map_identities_equal(left, right) -> bool:
        return all(
            _text((left or {}).get(key, "")) == _text((right or {}).get(key, ""))
            for key in ("name", "id", "md5")
        )

    def _current_calibration_map_identity(self):
        """Return the fresh, exact live map binding or fail closed."""
        msg = self._slam_state
        now = time.time()
        age = (now - self._slam_state_ts) if self._slam_state_ts > 0.0 else float("inf")
        if msg is None or age < 0.0 or age > self.slam_state_stale_timeout_s:
            raise ValueError("cannot persist dock calibration without a fresh slam state")
        if _text(getattr(msg, "robot_id", "")) != self.robot_id:
            raise ValueError("slam state robot_id does not match local robot_id")
        if not bool(getattr(msg, "runtime_map_ready", False)):
            raise ValueError("runtime map is not ready")
        if not bool(getattr(msg, "active_map_match", False)):
            raise ValueError("active/runtime map match is not valid")
        if not bool(getattr(msg, "localization_valid", False)):
            raise ValueError("localization is not valid")
        active = {
            "name": _text(getattr(msg, "active_map_name", "")),
            "revision_id": _text(getattr(msg, "active_map_revision_id", "")),
            "id": _text(getattr(msg, "active_map_id", "")),
            "md5": _text(getattr(msg, "active_map_md5", "")),
        }
        runtime = {
            "name": _text(getattr(msg, "runtime_map_name", "")),
            "revision_id": _text(getattr(msg, "runtime_map_revision_id", "")),
            "id": _text(getattr(msg, "runtime_map_id", "")),
            "md5": _text(getattr(msg, "runtime_map_md5", "")),
        }
        missing = [
            "%s.%s" % (source, key)
            for source, values, keys in (
                ("active", active, ("name", "id", "md5")),
                (
                    "runtime",
                    runtime,
                    ("name", "revision_id")
                    if active["revision_id"] and runtime["revision_id"]
                    else ("name", "id", "md5"),
                ),
            )
            for key in keys
            if not values[key]
        ]
        if missing:
            raise ValueError(
                "cannot persist dock calibration without complete map identity: missing %s"
                % ",".join(missing)
            )
        if active["revision_id"] and runtime["revision_id"]:
            if (
                active["name"] != runtime["name"]
                or active["revision_id"] != runtime["revision_id"]
            ):
                raise ValueError("active and runtime map revision scopes do not match")
        elif not self._map_identities_equal(active, runtime):
            raise ValueError("active and runtime legacy map identities do not match exactly")
        return active

    def _storage_payload(self, map_identity, *, stage1, stage2):
        return {
            "robot_id": self.robot_id,
            "saved_at_ms": _now_ms(),
            "frame_id": self.frame_id,
            "map": {
                "name": _text((map_identity or {}).get("name", "")),
                "id": _text((map_identity or {}).get("id", "")),
                "md5": _text((map_identity or {}).get("md5", "")),
            },
            "dock_stage1_xyyaw": list(stage1) if stage1 is not None else None,
            "dock_xyyaw": list(stage2) if stage2 is not None else None,
            # Persist only values validated through this service. Public ROS
            # parameters are downstream outputs and are never an authority.
            "dock_target_dist": float(self.dock_target_dist),
            "dock_xy_tolerance": float(self.dock_xy_tolerance),
        }

    def _persist_storage(
        self,
        *,
        expected_map_identity=None,
        require_existing_binding=False,
        stage1=None,
        stage2=None,
    ):
        with self._lock:
            if not self.storage_path:
                raise ValueError("dock calibration storage_path is required")
            current_identity = self._current_calibration_map_identity()
            if expected_map_identity is not None and not self._map_identities_equal(
                current_identity, expected_map_identity
            ):
                raise ValueError("live map identity changed during dock calibration update")
            if require_existing_binding and (
                not self._calibration_source_valid
                or not self._map_identities_equal(self._saved_map_identity(), current_identity)
            ):
                raise ValueError("live map identity does not match saved dock calibration")
            stage1_validated = None if stage1 is None else _validate_stage_pose(stage1)
            stage2_validated = None if stage2 is None else _validate_stage_pose(stage2)
            payload = self._storage_payload(
                current_identity,
                stage1=stage1_validated,
                stage2=stage2_validated,
            )
            directory = os.path.dirname(self.storage_path) or "."
            os.makedirs(directory, exist_ok=True)
            fd, tmp_path = tempfile.mkstemp(
                prefix=".%s." % (os.path.basename(self.storage_path) or "dock_calibration"),
                suffix=".tmp",
                dir=directory,
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as fh:
                    if yaml is not None:
                        yaml.safe_dump(payload, fh, default_flow_style=False, sort_keys=True)
                    else:
                        json.dump(payload, fh, indent=2, sort_keys=True)
                    fh.flush()
                    os.fsync(fh.fileno())
                os.replace(tmp_path, self.storage_path)
            except Exception:
                try:
                    os.unlink(tmp_path)
                except FileNotFoundError:
                    pass
                raise
            self._saved_map_name = current_identity["name"]
            self._saved_map_id = current_identity["id"]
            self._saved_map_md5 = current_identity["md5"]
            self._trusted_stage1 = stage1_validated
            self._trusted_stage2 = stage2_validated
            if self._trusted_stage1 is None:
                self._clear_stage_param(self.stage1_param_name)
            else:
                try:
                    rospy.set_param(self.stage1_param_name, list(self._trusted_stage1))
                except Exception as e:
                    rospy.logwarn("[dock_calib] failed to reapply trusted stage1 output: %s", str(e))
            if self._trusted_stage2 is None:
                self._clear_stage_param(self.stage2_param_name)
            else:
                try:
                    rospy.set_param(self.stage2_param_name, list(self._trusted_stage2))
                except Exception as e:
                    rospy.logwarn("[dock_calib] failed to reapply trusted stage2 output: %s", str(e))
            self._persisted_calibration_loaded = True
            self._calibration_source_valid = True
            self._publish_persisted_loaded_marker()

    def _load_storage(self, *, apply_params: bool, quiet: bool):
        with self._lock:
            return self._load_storage_locked(apply_params=apply_params, quiet=quiet)

    def _load_storage_locked(self, *, apply_params: bool, quiet: bool):
        if not self.storage_path or not os.path.exists(self.storage_path):
            self._invalidate_loaded_calibration()
            return False, "storage file does not exist"
        try:
            with open(self.storage_path, "r", encoding="utf-8") as fh:
                if yaml is not None:
                    payload = yaml.safe_load(fh) or {}
                else:
                    payload = json.load(fh)
        except Exception as e:
            self._invalidate_loaded_calibration()
            if not quiet:
                rospy.logerr("[dock_calib] load storage failed: %s", str(e))
            return False, str(e)

        if not isinstance(payload, dict):
            self._invalidate_loaded_calibration()
            return False, "storage payload must be a mapping"
        stored_robot_id = _text(payload.get("robot_id", ""))
        if stored_robot_id != self.robot_id:
            self._invalidate_loaded_calibration()
            return False, "storage robot_id mismatch: stored=%s local=%s" % (
                stored_robot_id or "-",
                self.robot_id,
            )
        stored_frame_id = _text(payload.get("frame_id", ""))
        if stored_frame_id != self.frame_id:
            self._invalidate_loaded_calibration()
            return False, "storage frame_id mismatch: stored=%s local=%s" % (
                stored_frame_id or "-",
                self.frame_id,
            )
        map_info = payload.get("map") or {}
        if not isinstance(map_info, dict):
            self._invalidate_loaded_calibration()
            return False, "storage map identity must be a mapping"
        stage1_raw = payload.get("dock_stage1_xyyaw")
        stage2_raw = payload.get("dock_xyyaw")
        try:
            stage1 = None if stage1_raw is None else _validate_stage_pose(stage1_raw)
        except ValueError:
            self._invalidate_loaded_calibration()
            return False, "invalid dock_stage1_xyyaw"
        try:
            stage2 = None if stage2_raw is None else _validate_stage_pose(stage2_raw)
        except ValueError:
            self._invalidate_loaded_calibration()
            return False, "invalid dock_xyyaw"
        try:
            target = float(payload.get("dock_target_dist", self.dock_target_dist))
            tolerance = float(payload.get("dock_xy_tolerance", self.dock_xy_tolerance))
        except (TypeError, ValueError):
            self._invalidate_loaded_calibration()
            return False, "invalid dock distance parameters"
        if not math.isfinite(target) or target < 0.20 or target > 2.00:
            self._invalidate_loaded_calibration()
            return False, "dock_target_dist must be within [0.20, 2.00] meters"
        if not math.isfinite(tolerance) or tolerance < 0.0 or tolerance > 0.05:
            self._invalidate_loaded_calibration()
            return False, "dock_xy_tolerance must be within [0.000, 0.050] meters"

        saved_map_name = _text(map_info.get("name", ""))
        saved_map_id = _text(map_info.get("id", ""))
        saved_map_md5 = _text(map_info.get("md5", ""))
        missing_map_fields = [
            field
            for field, value in (
                ("name", saved_map_name),
                ("id", saved_map_id),
                ("md5", saved_map_md5),
            )
            if not value
        ]
        if missing_map_fields:
            self._invalidate_loaded_calibration()
            return False, "storage map identity is incomplete: missing %s" % ",".join(
                missing_map_fields
            )
        if apply_params:
            output_names = (
                self.stage1_param_name,
                self.stage2_param_name,
                self.docking_target_dist_param_name,
                self.docking_controller_dist_param_name,
                self.docking_xy_tolerance_param_name,
            )
            param_snapshots = {}
            for name in output_names:
                if not name or name in param_snapshots:
                    continue
                exists = rospy.has_param(name)
                param_snapshots[name] = (exists, rospy.get_param(name) if exists else None)
            memory_snapshot = (
                self.dock_target_dist,
                self.dock_xy_tolerance,
                self._saved_map_name,
                self._saved_map_id,
                self._saved_map_md5,
                self._persisted_calibration_loaded,
                self._calibration_source_valid,
                self._trusted_stage1,
                self._trusted_stage2,
            )
            try:
                if stage1 is None:
                    self._clear_stage_param(self.stage1_param_name)
                else:
                    rospy.set_param(self.stage1_param_name, list(stage1))
                if stage2 is None:
                    self._clear_stage_param(self.stage2_param_name)
                else:
                    rospy.set_param(self.stage2_param_name, list(stage2))
                self.dock_target_dist = float(target)
                self.dock_xy_tolerance = float(tolerance)
                self._apply_dock_params(
                    target=self.dock_target_dist,
                    tolerance=self.dock_xy_tolerance,
                )
            except Exception as e:
                restore_ok = True
                for name, (existed, previous_value) in param_snapshots.items():
                    try:
                        if existed:
                            rospy.set_param(name, previous_value)
                        elif rospy.has_param(name):
                            rospy.delete_param(name)
                    except Exception:
                        restore_ok = False
                (
                    self.dock_target_dist,
                    self.dock_xy_tolerance,
                    self._saved_map_name,
                    self._saved_map_id,
                    self._saved_map_md5,
                    self._persisted_calibration_loaded,
                    self._calibration_source_valid,
                    self._trusted_stage1,
                    self._trusted_stage2,
                ) = memory_snapshot
                if restore_ok:
                    self._publish_persisted_loaded_marker()
                else:
                    self._persisted_calibration_loaded = False
                    self._calibration_source_valid = False
                    self._trusted_stage1 = None
                    self._trusted_stage2 = None
                    self._clear_stage_param(self.stage1_param_name)
                    self._clear_stage_param(self.stage2_param_name)
                    self._publish_persisted_loaded_marker()
                return False, "failed to apply dock calibration atomically: %s" % str(e)
        self._saved_map_name = saved_map_name
        self._saved_map_id = saved_map_id
        self._saved_map_md5 = saved_map_md5
        self._trusted_stage1 = stage1
        self._trusted_stage2 = stage2
        self._persisted_calibration_loaded = True
        self._calibration_source_valid = True
        self._publish_persisted_loaded_marker()
        if not quiet:
            rospy.loginfo("[dock_calib] loaded storage: %s", self.storage_path)
        return True, "loaded"


def main():
    rospy.init_node("dock_calibration_service", anonymous=False)
    _ = DockCalibrationServiceNode()
    rospy.spin()


if __name__ == "__main__":
    main()

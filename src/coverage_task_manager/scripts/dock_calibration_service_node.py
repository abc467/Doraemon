#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import math
import os
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

        self.storage_path = _text(
            rospy.get_param("~storage_path", "/data/coverage/dock_calibration.yaml")
        )
        self.load_persisted_on_start = bool(rospy.get_param("~load_persisted_on_start", True))

        self.slam_state_stale_timeout_s = max(0.2, float(rospy.get_param("~slam_state_stale_timeout_s", 2.0)))
        self.dock_pose_stale_timeout_s = max(0.1, float(rospy.get_param("~dock_pose_stale_timeout_s", 1.0)))
        self.dock_score_stale_timeout_s = max(0.1, float(rospy.get_param("~dock_score_stale_timeout_s", 1.0)))
        self.dock_score_threshold = float(rospy.get_param("~dock_score_threshold", 0.00012))
        self.dock_target_dist = float(rospy.get_param("~dock_target_dist", 0.607))
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
        self._saved_map_name = ""
        self._saved_map_id = ""
        self._saved_map_md5 = ""

        if self.load_persisted_on_start:
            self._load_storage(apply_params=True, quiet=True)

        self._state_pub = rospy.Publisher(self.state_topic, DockCalibrationState, queue_size=1, latch=True)
        rospy.Subscriber(self.slam_state_topic, SlamState, self._on_slam_state, queue_size=10)
        rospy.Subscriber(self.dock_pose_topic, PoseStamped, self._on_dock_pose, queue_size=20)
        rospy.Subscriber(self.dock_score_topic, Float32, self._on_dock_score, queue_size=20)

        self._status_srv = rospy.Service(self.status_service_name, GetDockCalibrationStatus, self._handle_status)
        self._command_srv = rospy.Service(self.command_service_name, OperateDockCalibration, self._handle_command)
        self._timer = rospy.Timer(rospy.Duration(1.0 / self.publish_hz), self._on_timer)

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

    def _handle_status(self, req):
        state = self._build_state()
        robot_id = _text(getattr(req, "robot_id", ""))
        if robot_id and robot_id != self.robot_id:
            return GetDockCalibrationStatusResponse(
                success=False,
                message="robot_id mismatch: requested=%s local=%s" % (robot_id, self.robot_id),
                state=state,
            )
        return GetDockCalibrationStatusResponse(success=True, message="ok", state=state)

    def _handle_command(self, req):
        robot_id = _text(getattr(req, "robot_id", ""))
        if robot_id and robot_id != self.robot_id:
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
        pose = [_as_float(x), _as_float(y), _as_float(yaw)]
        param_name = self.stage1_param_name if int(stage) == 1 else self.stage2_param_name
        rospy.set_param(param_name, pose)
        self._persist_storage()
        self._state_pub.publish(self._build_state())
        return self._command_response(
            True,
            "",
            "saved stage%d xyyaw=[%.6f, %.6f, %.6f]" % (int(stage), pose[0], pose[1], pose[2]),
            operation,
        )

    def _stage_param(self, param_name: str):
        if not param_name or not rospy.has_param(param_name):
            return False, (0.0, 0.0, 0.0)
        return True, _parse_xyyaw(rospy.get_param(param_name), default=(0.0, 0.0, 0.0))

    def _target_dist(self) -> float:
        if self.docking_target_dist_param_name and rospy.has_param(self.docking_target_dist_param_name):
            return _as_float(rospy.get_param(self.docking_target_dist_param_name), self.dock_target_dist)
        return float(self.dock_target_dist)

    def _current_pose_from_slam(self, now: float):
        msg = self._slam_state
        age = (now - self._slam_state_ts) if self._slam_state_ts > 0.0 else float("inf")
        if msg is None:
            return False, age, "", 0.0, 0.0, 0.0
        pose_age = max(_as_float(getattr(msg, "tracked_pose_age_s", age), age), age)
        frame = _text(getattr(msg, "tracked_pose_frame", ""))
        fresh = bool(getattr(msg, "tracked_pose_fresh", False)) and pose_age <= self.slam_state_stale_timeout_s
        return (
            fresh,
            pose_age,
            frame,
            _as_float(getattr(msg, "tracked_pose_x", 0.0)),
            _as_float(getattr(msg, "tracked_pose_y", 0.0)),
            _as_float(getattr(msg, "tracked_pose_theta", 0.0)),
        )

    def _build_state(self):
        with self._lock:
            now = time.time()
            msg = self._slam_state
            stage1_set, stage1 = self._stage_param(self.stage1_param_name)
            stage2_set, stage2 = self._stage_param(self.stage2_param_name)
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
            if msg is not None and self._saved_map_md5:
                current_md5 = _text(getattr(msg, "active_map_md5", "")) or _text(getattr(msg, "runtime_map_md5", ""))
                if current_md5 and current_md5 != self._saved_map_md5:
                    warnings.append("saved dock points belong to a different map md5")

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

    def _storage_payload(self):
        stage1_set, stage1 = self._stage_param(self.stage1_param_name)
        stage2_set, stage2 = self._stage_param(self.stage2_param_name)
        msg = self._slam_state
        map_name = ""
        map_id = ""
        map_md5 = ""
        if msg is not None:
            map_name = _text(getattr(msg, "active_map_name", "")) or _text(getattr(msg, "runtime_map_name", ""))
            map_id = _text(getattr(msg, "active_map_id", "")) or _text(getattr(msg, "runtime_map_id", ""))
            map_md5 = _text(getattr(msg, "active_map_md5", "")) or _text(getattr(msg, "runtime_map_md5", ""))
        if map_name or map_id or map_md5:
            self._saved_map_name = map_name
            self._saved_map_id = map_id
            self._saved_map_md5 = map_md5
        return {
            "robot_id": self.robot_id,
            "saved_at_ms": _now_ms(),
            "frame_id": self.frame_id,
            "map": {
                "name": self._saved_map_name,
                "id": self._saved_map_id,
                "md5": self._saved_map_md5,
            },
            "dock_stage1_xyyaw": list(stage1) if stage1_set else None,
            "dock_xyyaw": list(stage2) if stage2_set else None,
        }

    def _persist_storage(self):
        if not self.storage_path:
            return
        payload = self._storage_payload()
        directory = os.path.dirname(self.storage_path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        with open(self.storage_path, "w", encoding="utf-8") as fh:
            if yaml is not None:
                yaml.safe_dump(payload, fh, default_flow_style=False, sort_keys=True)
            else:
                json.dump(payload, fh, indent=2, sort_keys=True)

    def _load_storage(self, *, apply_params: bool, quiet: bool):
        if not self.storage_path or not os.path.exists(self.storage_path):
            return False, "storage file does not exist"
        try:
            with open(self.storage_path, "r", encoding="utf-8") as fh:
                if yaml is not None:
                    payload = yaml.safe_load(fh) or {}
                else:
                    payload = json.load(fh)
        except Exception as e:
            if not quiet:
                rospy.logerr("[dock_calib] load storage failed: %s", str(e))
            return False, str(e)
        map_info = payload.get("map") or {}
        self._saved_map_name = _text(map_info.get("name", ""))
        self._saved_map_id = _text(map_info.get("id", ""))
        self._saved_map_md5 = _text(map_info.get("md5", ""))
        if apply_params:
            stage1 = payload.get("dock_stage1_xyyaw")
            stage2 = payload.get("dock_xyyaw")
            if isinstance(stage1, (list, tuple)) and len(stage1) >= 3:
                rospy.set_param(self.stage1_param_name, list(_parse_xyyaw(stage1)))
            if isinstance(stage2, (list, tuple)) and len(stage2) >= 3:
                rospy.set_param(self.stage2_param_name, list(_parse_xyyaw(stage2)))
        if not quiet:
            rospy.loginfo("[dock_calib] loaded storage: %s", self.storage_path)
        return True, "loaded"


def main():
    rospy.init_node("dock_calibration_service", anonymous=False)
    _ = DockCalibrationServiceNode()
    rospy.spin()


if __name__ == "__main__":
    main()

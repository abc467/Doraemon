#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import sqlite3
import sys
import time
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


TERMINAL_JOB_STATES = {"succeeded", "failed", "manual_assist_required", "canceled"}
TASK_TERMINAL_STATES = {"DONE", "FAILED", "CANCELED"}
SUPPORTED_ACTIONS = (
    "prepare_for_task",
    "relocalize",
    "switch_map_and_localize",
    "verify_map_revision",
    "activate_map_revision",
    "start_mapping",
    "save_mapping",
    "stop_mapping",
)


def parse_actions(raw: str) -> List[str]:
    if not str(raw or "").strip():
        return []
    actions = [item.strip() for item in str(raw).split(",") if str(item).strip()]
    unsupported = [item for item in actions if item not in SUPPORTED_ACTIONS]
    if unsupported:
        raise ValueError("unsupported actions: %s" % ", ".join(unsupported))
    return actions


def filter_ignored_warnings(messages: Iterable[str], ignored: Iterable[str]) -> List[str]:
    ignored_set = {str(item) for item in list(ignored or []) if str(item)}
    result = []
    for message in list(messages or []):
        text = str(message or "")
        if text and text not in ignored_set:
            result.append(text)
    return result


def job_terminal_snapshot(job) -> Tuple[bool, str]:
    job_state = str(getattr(job, "job_state", "") or getattr(job, "status", "") or "").strip().lower()
    if bool(getattr(job, "done", False)):
        return True, job_state or ("succeeded" if bool(getattr(job, "success", False)) else "failed")
    if job_state in TERMINAL_JOB_STATES:
        return True, job_state
    return False, job_state


def job_succeeded(job) -> bool:
    terminal, state = job_terminal_snapshot(job)
    if not terminal:
        return False
    if state == "succeeded":
        return True
    if state == "manual_assist_required":
        return False
    if bool(getattr(job, "result_success", False)):
        return True
    return bool(getattr(job, "success", False))


def _ros_time_to_dict(stamp) -> Dict[str, int]:
    return {
        "secs": int(getattr(stamp, "secs", 0) or 0),
        "nsecs": int(getattr(stamp, "nsecs", 0) or 0),
    }


def _job_to_dict(job) -> Dict[str, object]:
    if job is None:
        return {}
    requested_revision_id = str(getattr(job, "requested_map_revision_id", "") or "")
    resolved_revision_id = str(getattr(job, "resolved_map_revision_id", "") or "")
    return {
        "job_id": str(getattr(job, "job_id", "") or ""),
        "operation_name": str(getattr(job, "operation_name", "") or ""),
        "status": str(getattr(job, "status", "") or ""),
        "phase": str(getattr(job, "phase", "") or ""),
        "job_state": str(getattr(job, "job_state", "") or ""),
        "workflow_phase": str(getattr(job, "workflow_phase", "") or ""),
        "progress_0_1": float(getattr(job, "progress_0_1", 0.0) or 0.0),
        "progress_percent": float(getattr(job, "progress_percent", 0.0) or 0.0),
        "done": bool(getattr(job, "done", False)),
        "success": bool(getattr(job, "success", False)),
        "result_success": bool(getattr(job, "result_success", False)),
        "error_code": str(getattr(job, "error_code", "") or ""),
        "message": str(getattr(job, "message", "") or ""),
        "result_code": str(getattr(job, "result_code", "") or ""),
        "result_message": str(getattr(job, "result_message", "") or ""),
        "manual_assist_required": bool(getattr(job, "manual_assist_required", False)),
        "manual_assist_map_name": str(getattr(job, "manual_assist_map_name", "") or ""),
        "manual_assist_map_revision_id": str(getattr(job, "manual_assist_map_revision_id", "") or ""),
        "manual_assist_retry_action": str(getattr(job, "manual_assist_retry_action", "") or ""),
        "manual_assist_guidance": str(getattr(job, "manual_assist_guidance", "") or ""),
        "runtime_map_match": bool(getattr(job, "runtime_map_match", False)),
        "localization_valid": bool(getattr(job, "localization_valid", False)),
        "requested_map_revision_id": requested_revision_id,
        "resolved_map_revision_id": resolved_revision_id,
        "revision_scope": {
            "requested": _revision_scope_slot(
                revision_id=requested_revision_id,
                source="slam_job.requested_map_revision_id",
            ),
            "resolved": _revision_scope_slot(
                revision_id=resolved_revision_id,
                source="slam_job.resolved_map_revision_id",
            ),
        },
        "created_at": _ros_time_to_dict(getattr(job, "created_at", None) or object()),
        "started_at": _ros_time_to_dict(getattr(job, "started_at", None) or object()),
        "finished_at": _ros_time_to_dict(getattr(job, "finished_at", None) or object()),
        "updated_at": _ros_time_to_dict(getattr(job, "updated_at", None) or object()),
    }


def _revision_scope_slot(
    *,
    map_name: str = "",
    revision_id: str = "",
    status: str = "",
    lifecycle_status: str = "",
    verification_status: str = "",
    source: str = "",
) -> Dict[str, object]:
    map_name = str(map_name or "")
    revision_id = str(revision_id or "")
    status = str(status or "")
    lifecycle_status = str(lifecycle_status or "")
    verification_status = str(verification_status or "")
    source = str(source or "")
    return {
        "map_name": map_name,
        "revision_id": revision_id,
        "status": status,
        "lifecycle_status": lifecycle_status,
        "verification_status": verification_status,
        "source": source,
        "present": bool(map_name or revision_id or status or lifecycle_status or verification_status),
    }


def build_revision_scope(
    *,
    slam_state: Optional[Dict[str, object]] = None,
    readiness_state: Optional[Dict[str, object]] = None,
    latest_head: Optional[Dict[str, object]] = None,
) -> Dict[str, object]:
    slam_state = dict(slam_state or {})
    readiness_state = dict(readiness_state or {})
    latest_head = dict(latest_head or {})
    latest_head_source = str(latest_head.get("source") or "not_reported")
    return {
        "task_binding": _revision_scope_slot(
            revision_id=str(readiness_state.get("task_map_revision_id") or ""),
            source="system_readiness.task_map_revision_id",
        ),
        "active": _revision_scope_slot(
            map_name=str(slam_state.get("active_map_name") or readiness_state.get("active_map_name") or ""),
            revision_id=str(slam_state.get("active_map_revision_id") or readiness_state.get("active_map_revision_id") or ""),
            source="slam_status.active_map_revision_id",
        ),
        "runtime": _revision_scope_slot(
            map_name=str(slam_state.get("runtime_map_name") or readiness_state.get("runtime_map_name") or ""),
            revision_id=str(slam_state.get("runtime_map_revision_id") or readiness_state.get("runtime_map_revision_id") or ""),
            source="slam_status.runtime_map_revision_id",
        ),
        "pending_target": _revision_scope_slot(
            map_name=str(slam_state.get("pending_map_name") or ""),
            revision_id=str(slam_state.get("pending_map_revision_id") or ""),
            status=str(slam_state.get("pending_map_switch_status") or ""),
            source="slam_status.pending_map_revision_id",
        ),
        "latest_head": _revision_scope_slot(
            map_name=str(latest_head.get("map_name") or ""),
            revision_id=str(latest_head.get("revision_id") or ""),
            lifecycle_status=str(latest_head.get("lifecycle_status") or ""),
            verification_status=str(latest_head.get("verification_status") or ""),
            source=latest_head_source,
        ),
    }


def _latest_head_scope_from_map_msg(map_msg) -> Dict[str, object]:
    if map_msg is None:
        return _revision_scope_slot(source="map_server.get")
    map_name = str(getattr(map_msg, "map_name", "") or "")
    map_revision_id = str(getattr(map_msg, "map_revision_id", "") or "")
    latest_head_revision_id = str(getattr(map_msg, "latest_head_revision_id", "") or "")
    latest_head_lifecycle_status = str(getattr(map_msg, "latest_head_lifecycle_status", "") or "")
    latest_head_verification_status = str(getattr(map_msg, "latest_head_verification_status", "") or "")
    if not latest_head_revision_id and bool(getattr(map_msg, "is_latest_head", False)):
        latest_head_revision_id = map_revision_id
    if not latest_head_lifecycle_status and latest_head_revision_id == map_revision_id:
        latest_head_lifecycle_status = str(getattr(map_msg, "lifecycle_status", "") or "")
    if not latest_head_verification_status and latest_head_revision_id == map_revision_id:
        latest_head_verification_status = str(getattr(map_msg, "verification_status", "") or "")
    return _revision_scope_slot(
        map_name=map_name,
        revision_id=latest_head_revision_id,
        lifecycle_status=latest_head_lifecycle_status,
        verification_status=latest_head_verification_status,
        source="map_server.get",
    )


def build_runtime_revision_scope(
    checks: Sequence[Dict[str, object]],
    *,
    latest_head: Optional[Dict[str, object]] = None,
) -> Dict[str, object]:
    slam_state: Dict[str, object] = {}
    readiness_state: Dict[str, object] = {}
    for item in list(checks or []):
        name = str(item.get("name") or "")
        response = dict(item.get("response") or {})
        if name == "slam_status":
            slam_state = dict(response.get("state") or {})
        elif name == "system_readiness":
            readiness_state = dict(response.get("readiness") or {})
    return build_revision_scope(slam_state=slam_state, readiness_state=readiness_state, latest_head=latest_head)


def _format_revision_scope(scope: Dict[str, object], *, include_task_binding: bool = True, include_latest_head: bool = True) -> str:
    scope = dict(scope or {})
    active = dict(scope.get("active") or {})
    runtime = dict(scope.get("runtime") or {})
    pending_target = dict(scope.get("pending_target") or {})
    task_binding = dict(scope.get("task_binding") or {})
    latest_head = dict(scope.get("latest_head") or {})
    parts = []
    if include_task_binding:
        parts.append("task_revision=%s" % (str(task_binding.get("revision_id") or "") or "-"))
    parts.append(
        "active=%s/%s"
        % (
            str(active.get("map_name") or "") or "-",
            str(active.get("revision_id") or "") or "-",
        )
    )
    parts.append(
        "runtime=%s/%s"
        % (
            str(runtime.get("map_name") or "") or "-",
            str(runtime.get("revision_id") or "") or "-",
        )
    )
    parts.append(
        "pending_target=%s/%s status=%s"
        % (
            str(pending_target.get("map_name") or "") or "-",
            str(pending_target.get("revision_id") or "") or "-",
            str(pending_target.get("status") or "") or "-",
        )
    )
    if include_latest_head:
        parts.append(
            "latest_head=%s/%s lifecycle=%s verification=%s"
            % (
                str(latest_head.get("map_name") or "") or "-",
                str(latest_head.get("revision_id") or "") or "-",
                str(latest_head.get("lifecycle_status") or "") or "-",
                str(latest_head.get("verification_status") or "") or "-",
            )
        )
    return " ".join(parts)


class BackendRuntimeSmokeClient:
    def __init__(self):
        import rospy

        from cleanrobot_app_msgs.msg import PgmData
        from cleanrobot_app_msgs.srv import (
            ExeTask,
            GetOdometryStatus,
            GetSlamJob,
            GetSlamStatus,
            GetSystemReadiness,
            OperateMap,
            SubmitSlamCommand,
        )
        from coverage_msgs.msg import TaskState as TaskStateMsg

        self.rospy = rospy
        rospy.init_node("backend_runtime_smoke", anonymous=True, disable_signals=True)
        self._submit_request_cls = SubmitSlamCommand._request_class
        self._operate_map_request_cls = OperateMap._request_class
        self._pgm_data_cls = PgmData
        self._get_slam_status = rospy.ServiceProxy("/clean_robot_server/app/get_slam_status", GetSlamStatus)
        self._get_odometry_status = rospy.ServiceProxy("/clean_robot_server/app/get_odometry_status", GetOdometryStatus)
        self._get_system_readiness = rospy.ServiceProxy(
            "/coverage_task_manager/app/get_system_readiness",
            GetSystemReadiness,
        )
        self._exe_task = rospy.ServiceProxy("/coverage_task_manager/app/exe_task_server", ExeTask)
        self._submit_slam_command = rospy.ServiceProxy("/clean_robot_server/app/submit_slam_command", SubmitSlamCommand)
        self._get_slam_job = rospy.ServiceProxy("/clean_robot_server/app/get_slam_job", GetSlamJob)
        self._operate_map = rospy.ServiceProxy("/clean_robot_server/app/map_server", OperateMap)
        self._task_state_cls = TaskStateMsg

    def wait_for_services(self, timeout_s: float) -> None:
        deadline = time.time() + float(timeout_s)
        services = (
            "/clean_robot_server/app/get_slam_status",
            "/clean_robot_server/app/get_odometry_status",
            "/coverage_task_manager/app/get_system_readiness",
            "/coverage_task_manager/app/exe_task_server",
            "/clean_robot_server/app/submit_slam_command",
            "/clean_robot_server/app/get_slam_job",
            "/clean_robot_server/app/map_server",
        )
        for service_name in services:
            remain = max(0.1, deadline - time.time())
            self.rospy.wait_for_service(service_name, timeout=remain)

    def get_slam_status(self, robot_id: str = "local_robot", refresh_map_identity: bool = True):
        return self._get_slam_status(robot_id=str(robot_id or "local_robot"), refresh_map_identity=refresh_map_identity)

    def get_odometry_status(self):
        return self._get_odometry_status()

    def get_system_readiness(self, task_id: int, refresh_map_identity: bool = True):
        return self._get_system_readiness(task_id=int(task_id), refresh_map_identity=bool(refresh_map_identity))

    def get_slam_job(self, job_id: str, robot_id: str = "local_robot"):
        return self._get_slam_job(job_id=str(job_id or ""), robot_id=str(robot_id or "local_robot"))

    def start_task(self, task_id: int):
        return self._exe_task(command=0, task_id=int(task_id))

    def wait_for_task_state(self, timeout_s: float):
        return self.rospy.wait_for_message("/task_state", self._task_state_cls, timeout=float(timeout_s))

    def get_map_view(self, map_name: str, map_revision_id: str = ""):
        request_cls = self._operate_map_request_cls
        return self._operate_map(
            operation=int(getattr(request_cls, "get")),
            map_name=str(map_name or ""),
            map=self._pgm_data_cls(
                map_name=str(map_name or ""),
                map_revision_id=str(map_revision_id or ""),
            ),
            set_active=False,
            enabled_state=int(getattr(request_cls, "ENABLE_KEEP")),
        )

    def submit_action(
        self,
        operation_name: str,
        robot_id: str,
        map_name: str,
        map_revision_id: str,
        frame_id: str,
        save_map_name: str,
        description: str,
        set_active: bool,
        has_initial_pose: bool,
        initial_pose_x: float,
        initial_pose_y: float,
        initial_pose_yaw: float,
        include_unfinished_submaps: bool,
        set_active_on_save: bool,
        switch_to_localization_after_save: bool,
        relocalize_after_switch: bool,
    ):
        operation_value = int(getattr(self._submit_request_cls, str(operation_name)))
        return self._submit_slam_command(
            operation=operation_value,
            robot_id=str(robot_id or "local_robot"),
            map_name=str(map_name or ""),
            map_revision_id=str(map_revision_id or ""),
            set_active=bool(set_active),
            description=str(description or ""),
            frame_id=str(frame_id or "map"),
            has_initial_pose=bool(has_initial_pose),
            initial_pose_x=float(initial_pose_x),
            initial_pose_y=float(initial_pose_y),
            initial_pose_yaw=float(initial_pose_yaw),
            save_map_name=str(save_map_name or ""),
            include_unfinished_submaps=bool(include_unfinished_submaps),
            set_active_on_save=bool(set_active_on_save),
            switch_to_localization_after_save=bool(switch_to_localization_after_save),
            relocalize_after_switch=bool(relocalize_after_switch),
        )


def _check_readiness(resp, ignored_warnings: Sequence[str]) -> Dict[str, object]:
    readiness = getattr(resp, "readiness", None)
    issues = []
    if not bool(getattr(resp, "success", False)):
        issues.append("service success=false message=%s" % str(getattr(resp, "message", "") or ""))
    if readiness is None:
        issues.append("missing readiness payload")
        readiness_dict = {}
    else:
        readiness_dict = {
            "overall_ready": bool(getattr(readiness, "overall_ready", False)),
            "can_start_task": bool(getattr(readiness, "can_start_task", False)),
            "mission_state": str(getattr(readiness, "mission_state", "") or ""),
            "phase": str(getattr(readiness, "phase", "") or ""),
            "public_state": str(getattr(readiness, "public_state", "") or ""),
            "executor_state": str(getattr(readiness, "executor_state", "") or ""),
            "task_map_revision_id": str(getattr(readiness, "task_map_revision_id", "") or ""),
            "active_map_revision_id": str(getattr(readiness, "active_map_revision_id", "") or ""),
            "runtime_map_revision_id": str(getattr(readiness, "runtime_map_revision_id", "") or ""),
            "active_map_name": str(getattr(readiness, "active_map_name", "") or ""),
            "runtime_map_name": str(getattr(readiness, "runtime_map_name", "") or ""),
            "manual_assist_required": bool(getattr(readiness, "manual_assist_required", False)),
            "manual_assist_map_name": str(getattr(readiness, "manual_assist_map_name", "") or ""),
            "manual_assist_map_revision_id": str(getattr(readiness, "manual_assist_map_revision_id", "") or ""),
            "manual_assist_retry_action": str(getattr(readiness, "manual_assist_retry_action", "") or ""),
            "manual_assist_guidance": str(getattr(readiness, "manual_assist_guidance", "") or ""),
            "blocking_reasons": [str(item) for item in list(getattr(readiness, "blocking_reasons", []) or [])],
            "warnings": [str(item) for item in list(getattr(readiness, "warnings", []) or [])],
            "stamp": _ros_time_to_dict(getattr(readiness, "stamp", None) or object()),
        }
        readiness_dict["revision_scope"] = build_revision_scope(readiness_state=readiness_dict)
        if not readiness_dict["overall_ready"]:
            issues.append("overall_ready=false")
        if not readiness_dict["can_start_task"]:
            issues.append("can_start_task=false")
        if readiness_dict["blocking_reasons"]:
            issues.append("blocking_reasons=%s" % ", ".join(readiness_dict["blocking_reasons"]))
        extra_warnings = filter_ignored_warnings(readiness_dict["warnings"], ignored_warnings)
        if extra_warnings:
            issues.append("unexpected warnings=%s" % ", ".join(extra_warnings))
    return {
        "name": "system_readiness",
        "ok": not issues,
        "issues": issues,
        "response": {
            "success": bool(getattr(resp, "success", False)),
            "message": str(getattr(resp, "message", "") or ""),
            "readiness": readiness_dict,
        },
    }


def _check_odometry(resp, ignored_warnings: Sequence[str]) -> Dict[str, object]:
    state = getattr(resp, "state", None)
    issues = []
    if not bool(getattr(resp, "success", False)):
        issues.append("service success=false message=%s" % str(getattr(resp, "message", "") or ""))
    if state is None:
        issues.append("missing odometry state")
        state_dict = {}
    else:
        state_dict = {
            "odom_source": str(getattr(state, "odom_source", "") or ""),
            "odom_topic": str(getattr(state, "odom_topic", "") or ""),
            "validation_mode": str(getattr(state, "validation_mode", "") or ""),
            "connected": bool(getattr(state, "connected", False)),
            "odom_stream_ready": bool(getattr(state, "odom_stream_ready", False)),
            "frame_id_valid": bool(getattr(state, "frame_id_valid", False)),
            "child_frame_id_valid": bool(getattr(state, "child_frame_id_valid", False)),
            "odom_valid": bool(getattr(state, "odom_valid", False)),
            "error_code": str(getattr(state, "error_code", "") or ""),
            "message": str(getattr(state, "message", "") or ""),
            "warnings": [str(item) for item in list(getattr(state, "warnings", []) or [])],
            "stamp": _ros_time_to_dict(getattr(state, "stamp", None) or object()),
        }
        if not state_dict["odom_valid"]:
            issues.append("odom_valid=false")
        if not state_dict["odom_stream_ready"]:
            issues.append("odom_stream_ready=false")
        if not state_dict["frame_id_valid"]:
            issues.append("frame_id_valid=false")
        if not state_dict["child_frame_id_valid"]:
            issues.append("child_frame_id_valid=false")
        extra_warnings = filter_ignored_warnings(state_dict["warnings"], ignored_warnings)
        if extra_warnings:
            issues.append("unexpected warnings=%s" % ", ".join(extra_warnings))
    return {
        "name": "odometry_status",
        "ok": not issues,
        "issues": issues,
        "response": {
            "success": bool(getattr(resp, "success", False)),
            "message": str(getattr(resp, "message", "") or ""),
            "state": state_dict,
        },
    }


def _check_slam(resp, ignored_warnings: Sequence[str]) -> Dict[str, object]:
    state = getattr(resp, "state", None)
    issues = []
    if not bool(getattr(resp, "success", False)):
        issues.append("service success=false message=%s" % str(getattr(resp, "message", "") or ""))
    if state is None:
        issues.append("missing slam state")
        state_dict = {}
    else:
        state_dict = {
            "current_mode": str(getattr(state, "current_mode", "") or ""),
            "runtime_mode": str(getattr(state, "runtime_mode", "") or ""),
            "workflow_state": str(getattr(state, "workflow_state", "") or ""),
            "workflow_phase": str(getattr(state, "workflow_phase", "") or ""),
            "active_map_revision_id": str(getattr(state, "active_map_revision_id", "") or ""),
            "runtime_map_revision_id": str(getattr(state, "runtime_map_revision_id", "") or ""),
            "active_map_name": str(getattr(state, "active_map_name", "") or ""),
            "runtime_map_name": str(getattr(state, "runtime_map_name", "") or ""),
            "pending_map_name": str(getattr(state, "pending_map_name", "") or ""),
            "pending_map_revision_id": str(getattr(state, "pending_map_revision_id", "") or ""),
            "pending_map_switch_status": str(getattr(state, "pending_map_switch_status", "") or ""),
            "localization_state": str(getattr(state, "localization_state", "") or ""),
            "localization_valid": bool(getattr(state, "localization_valid", False)),
            "runtime_map_ready": bool(getattr(state, "runtime_map_ready", False)),
            "runtime_map_match": bool(getattr(state, "runtime_map_match", False)),
            "busy": bool(getattr(state, "busy", False)),
            "task_ready": bool(getattr(state, "task_ready", False)),
            "manual_assist_required": bool(getattr(state, "manual_assist_required", False)),
            "manual_assist_map_name": str(getattr(state, "manual_assist_map_name", "") or ""),
            "manual_assist_map_revision_id": str(getattr(state, "manual_assist_map_revision_id", "") or ""),
            "manual_assist_retry_action": str(getattr(state, "manual_assist_retry_action", "") or ""),
            "manual_assist_guidance": str(getattr(state, "manual_assist_guidance", "") or ""),
            "can_verify_map_revision": bool(getattr(state, "can_verify_map_revision", False)),
            "can_activate_map_revision": bool(getattr(state, "can_activate_map_revision", False)),
            "can_start_mapping": bool(getattr(state, "can_start_mapping", False)),
            "can_save_mapping": bool(getattr(state, "can_save_mapping", False)),
            "can_stop_mapping": bool(getattr(state, "can_stop_mapping", False)),
            "blocking_reasons": [str(item) for item in list(getattr(state, "blocking_reasons", []) or [])],
            "warnings": [str(item) for item in list(getattr(state, "warnings", []) or [])],
            "stamp": _ros_time_to_dict(getattr(state, "stamp", None) or object()),
        }
        state_dict["revision_scope"] = build_revision_scope(slam_state=state_dict)
        if not state_dict["localization_valid"]:
            issues.append("localization_valid=false")
        if not state_dict["runtime_map_ready"]:
            issues.append("runtime_map_ready=false")
        if not state_dict["runtime_map_match"]:
            issues.append("runtime_map_match=false")
        if state_dict["manual_assist_required"]:
            issues.append("manual_assist_required=true")
        if state_dict["blocking_reasons"]:
            issues.append("blocking_reasons=%s" % ", ".join(state_dict["blocking_reasons"]))
        extra_warnings = filter_ignored_warnings(state_dict["warnings"], ignored_warnings)
        if extra_warnings:
            issues.append("unexpected warnings=%s" % ", ".join(extra_warnings))
    return {
        "name": "slam_status",
        "ok": not issues,
        "issues": issues,
        "response": {
            "success": bool(getattr(resp, "success", False)),
            "message": str(getattr(resp, "message", "") or ""),
            "state": state_dict,
        },
    }


def run_read_checks(client: BackendRuntimeSmokeClient, task_id: int, ignored_warnings: Sequence[str]) -> List[Dict[str, object]]:
    checks = []
    checks.append(_check_slam(client.get_slam_status(), ignored_warnings))
    checks.append(_check_odometry(client.get_odometry_status(), ignored_warnings))
    checks.append(_check_readiness(client.get_system_readiness(task_id=task_id), ignored_warnings))
    return checks


def _task_state_to_dict(msg) -> Dict[str, object]:
    if msg is None:
        return {}
    return {
        "mission_state": str(getattr(msg, "mission_state", "") or ""),
        "phase": str(getattr(msg, "phase", "") or ""),
        "public_state": str(getattr(msg, "public_state", "") or ""),
        "executor_state": str(getattr(msg, "executor_state", "") or ""),
        "run_id": str(getattr(msg, "run_id", "") or ""),
        "progress_pct": float(getattr(msg, "progress_pct", 0.0) or 0.0),
        "plan_id": str(getattr(msg, "plan_id", "") or ""),
        "map_id": str(getattr(msg, "map_id", "") or ""),
        "map_md5": str(getattr(msg, "map_md5", "") or ""),
        "zone_id": str(getattr(msg, "zone_id", "") or ""),
        "stamp": _ros_time_to_dict(getattr(msg, "stamp", None) or object()),
    }


def _load_task_cycle_db_state(ops_db_path: str, run_id: str) -> Dict[str, object]:
    state = {
        "run": {},
        "runtime": {},
    }
    if not str(ops_db_path or "").strip():
        return state
    conn = sqlite3.connect(str(ops_db_path))
    try:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        if str(run_id or "").strip():
            row = cur.execute(
                """
                SELECT run_id, job_id, state, reason, plan_id, map_revision_id, map_id, map_md5,
                       start_ts, end_ts, updated_ts
                FROM mission_runs
                WHERE run_id = ?
                """,
                (str(run_id),),
            ).fetchone()
            if row is not None:
                state["run"] = {str(key): row[key] for key in row.keys()}
        runtime_row = cur.execute(
            """
            SELECT active_run_id, active_job_id, mission_state, phase, public_state, executor_state, updated_ts
            FROM robot_runtime_state
            LIMIT 1
            """
        ).fetchone()
        if runtime_row is not None:
            state["runtime"] = {str(key): runtime_row[key] for key in runtime_row.keys()}
    finally:
        conn.close()
    return state


def _runtime_row_is_idle(runtime_row: Dict[str, object]) -> bool:
    runtime_row = dict(runtime_row or {})
    return (
        not str(runtime_row.get("active_run_id") or "").strip()
        and str(runtime_row.get("mission_state") or "IDLE").strip().upper() == "IDLE"
        and str(runtime_row.get("phase") or "IDLE").strip().upper() == "IDLE"
        and str(runtime_row.get("public_state") or "IDLE").strip().upper() == "IDLE"
        and str(runtime_row.get("executor_state") or "IDLE").strip().upper() == "IDLE"
    )


def _task_cycle_issues(
    *,
    task_id: int,
    running_seen: bool,
    run_id: str,
    final_run_row: Dict[str, object],
    final_runtime_row: Dict[str, object],
    post_readiness: Dict[str, object],
) -> List[str]:
    issues: List[str] = []
    if int(task_id or 0) <= 0:
        issues.append("task_id must be > 0 for task cycle")
    if not running_seen:
        issues.append("task never reached running state")
    if not str(run_id or "").strip():
        issues.append("task run_id was never observed")
    run_state = str((final_run_row or {}).get("state") or "").strip().upper()
    if final_run_row:
        if run_state != "DONE":
            issues.append("mission_run terminal state=%s" % (run_state or "UNKNOWN"))
    else:
        issues.append("mission_run row missing")
    if final_runtime_row:
        if not _runtime_row_is_idle(final_runtime_row):
            issues.append(
                "runtime not idle after task: mission=%s phase=%s public=%s executor=%s active_run_id=%s"
                % (
                    str(final_runtime_row.get("mission_state") or ""),
                    str(final_runtime_row.get("phase") or ""),
                    str(final_runtime_row.get("public_state") or ""),
                    str(final_runtime_row.get("executor_state") or ""),
                    str(final_runtime_row.get("active_run_id") or ""),
                )
            )
    else:
        issues.append("robot_runtime_state row missing")
    if not bool(post_readiness.get("ok", False)):
        issues.append("post task readiness failed")
    return issues


def run_task_cycle(client: BackendRuntimeSmokeClient, args) -> Dict[str, object]:
    result = {
        "name": "task_cycle",
        "ok": False,
        "issues": [],
        "start": {},
        "running_seen": False,
        "run_id": "",
        "task_state_observations": [],
        "db_observations": [],
        "post_readiness": {},
    }
    pre_readiness = _check_readiness(client.get_system_readiness(task_id=args.task_id), args.ignore_warning)
    result["pre_readiness"] = pre_readiness
    if not bool(pre_readiness.get("ok", False)):
        result["issues"].append("pre task readiness failed")
        return result

    start_resp = client.start_task(args.task_id)
    result["start"] = {
        "success": bool(getattr(start_resp, "success", False)),
        "message": str(getattr(start_resp, "message", "") or ""),
    }
    if not bool(getattr(start_resp, "success", False)):
        result["issues"].append("start task rejected: %s" % str(getattr(start_resp, "message", "") or ""))
        return result

    deadline = time.time() + float(args.task_timeout)
    run_id = ""
    running_seen = False
    final_db_state = {"run": {}, "runtime": {}}
    last_task_state = None
    while time.time() < deadline:
        remain = max(0.1, min(float(args.poll_interval), deadline - time.time()))
        try:
            msg = client.wait_for_task_state(timeout_s=remain)
        except Exception:
            msg = None
        task_state = _task_state_to_dict(msg)
        if task_state and task_state != last_task_state:
            result["task_state_observations"].append(task_state)
            last_task_state = dict(task_state)
        if str(task_state.get("run_id") or "").strip():
            run_id = str(task_state.get("run_id") or "").strip()
        mission_state = str(task_state.get("mission_state") or "IDLE").strip().upper()
        public_state = str(task_state.get("public_state") or "IDLE").strip().upper()
        if run_id or mission_state not in ("", "IDLE") or public_state not in ("", "IDLE"):
            running_seen = True
        if run_id and str(args.ops_db_path or "").strip():
            final_db_state = _load_task_cycle_db_state(args.ops_db_path, run_id)
            run_row = dict(final_db_state.get("run") or {})
            runtime_row = dict(final_db_state.get("runtime") or {})
            db_snapshot = {
                "run": run_row,
                "runtime": runtime_row,
            }
            if (not result["db_observations"]) or (db_snapshot != result["db_observations"][-1]):
                result["db_observations"].append(db_snapshot)
            if str(run_row.get("state") or "").strip().upper() in TASK_TERMINAL_STATES and _runtime_row_is_idle(runtime_row):
                break

    post_readiness = _check_readiness(client.get_system_readiness(task_id=args.task_id), args.ignore_warning)
    if str(args.ops_db_path or "").strip() and run_id:
        final_db_state = _load_task_cycle_db_state(args.ops_db_path, run_id)

    result["running_seen"] = bool(running_seen)
    result["run_id"] = str(run_id or "")
    result["post_readiness"] = post_readiness
    result["final_db_state"] = final_db_state
    result["issues"] = _task_cycle_issues(
        task_id=int(args.task_id),
        running_seen=bool(running_seen),
        run_id=str(run_id or ""),
        final_run_row=dict(final_db_state.get("run") or {}),
        final_runtime_row=dict(final_db_state.get("runtime") or {}),
        post_readiness=post_readiness,
    )
    result["ok"] = not result["issues"]
    return result


def wait_for_job(
    client: BackendRuntimeSmokeClient,
    job_id: str,
    robot_id: str,
    timeout_s: float,
    poll_interval_s: float,
) -> Dict[str, object]:
    deadline = time.time() + float(timeout_s)
    observations = []
    while time.time() < deadline:
        resp = client.get_slam_job(job_id=job_id, robot_id=robot_id)
        entry = {
            "found": bool(getattr(resp, "found", False)),
            "message": str(getattr(resp, "message", "") or ""),
            "error_code": str(getattr(resp, "error_code", "") or ""),
            "job": _job_to_dict(getattr(resp, "job", None)),
        }
        observations.append(entry)
        if not bool(getattr(resp, "found", False)):
            time.sleep(float(poll_interval_s))
            continue
        terminal, state = job_terminal_snapshot(getattr(resp, "job", None))
        if terminal:
            return {
                "ok": job_succeeded(getattr(resp, "job", None)),
                "terminal_state": state,
                "response": entry,
                "observations": observations,
            }
        time.sleep(float(poll_interval_s))
    return {
        "ok": False,
        "terminal_state": "timeout",
        "response": observations[-1] if observations else {},
        "observations": observations,
    }


def run_actions(client: BackendRuntimeSmokeClient, args) -> List[Dict[str, object]]:
    results = []
    description_prefix = str(args.description_prefix or "backend_runtime_smoke").strip() or "backend_runtime_smoke"
    for action_name in list(args.actions or []):
        description = "%s:%s" % (description_prefix, action_name)
        submit = client.submit_action(
            operation_name=action_name,
            robot_id=args.robot_id,
            map_name=args.map_name,
            map_revision_id=args.map_revision_id,
            frame_id=args.frame_id,
            save_map_name=args.save_map_name,
            description=description,
            set_active=args.set_active,
            has_initial_pose=args.has_initial_pose,
            initial_pose_x=args.initial_pose_x,
            initial_pose_y=args.initial_pose_y,
            initial_pose_yaw=args.initial_pose_yaw,
            include_unfinished_submaps=args.include_unfinished_submaps,
            set_active_on_save=args.set_active_on_save,
            switch_to_localization_after_save=args.switch_to_localization_after_save,
            relocalize_after_switch=args.relocalize_after_switch,
        )
        action_result = {
            "name": action_name,
            "submit": {
                "accepted": bool(getattr(submit, "accepted", False)),
                "message": str(getattr(submit, "message", "") or ""),
                "error_code": str(getattr(submit, "error_code", "") or ""),
                "job_id": str(getattr(submit, "job_id", "") or ""),
                "map_name": str(getattr(submit, "map_name", "") or ""),
                "map_revision_id": str(getattr(submit, "map_revision_id", "") or ""),
                "operation": int(getattr(submit, "operation", 0) or 0),
                "job": _job_to_dict(getattr(submit, "job", None)),
            },
            "ok": False,
            "issues": [],
        }
        if not action_result["submit"]["accepted"]:
            action_result["issues"].append(
                "submit rejected error_code=%s message=%s"
                % (action_result["submit"]["error_code"], action_result["submit"]["message"])
            )
            results.append(action_result)
            continue
        job_id = action_result["submit"]["job_id"]
        if not job_id:
            action_result["issues"].append("missing job_id on accepted submit")
            results.append(action_result)
            continue
        waited = wait_for_job(
            client=client,
            job_id=job_id,
            robot_id=args.robot_id,
            timeout_s=args.job_timeout,
            poll_interval_s=args.poll_interval,
        )
        action_result["job"] = waited
        if not waited["ok"]:
            action_result["issues"].append("job terminal_state=%s" % str(waited.get("terminal_state") or ""))
        action_result["ok"] = not action_result["issues"]
        results.append(action_result)
    return results


def _lookup_latest_head_scope(client: BackendRuntimeSmokeClient, revision_scope: Dict[str, object]) -> Dict[str, object]:
    active = dict((revision_scope or {}).get("active") or {})
    active_map_name = str(active.get("map_name") or "")
    if not active_map_name:
        return _revision_scope_slot(source="map_server.get")
    try:
        resp = client.get_map_view(active_map_name)
    except Exception:
        return _revision_scope_slot(source="map_server.get_failed")
    if not bool(getattr(resp, "success", False)):
        return _revision_scope_slot(source="map_server.get_failed")
    return _latest_head_scope_from_map_msg(getattr(resp, "map", None))


def build_report(args) -> Dict[str, object]:
    client = BackendRuntimeSmokeClient()
    client.wait_for_services(timeout_s=args.service_timeout)
    checks = run_read_checks(client, task_id=args.task_id, ignored_warnings=args.ignore_warning)
    actions = run_actions(client, args) if args.actions else []
    task_cycle = run_task_cycle(client, args) if bool(getattr(args, "run_task_cycle", False)) else None
    issues = []
    for item in checks:
        if not bool(item.get("ok", False)):
            issues.append("%s: %s" % (str(item.get("name") or ""), "; ".join(item.get("issues") or [])))
    for item in actions:
        if not bool(item.get("ok", False)):
            issues.append("%s: %s" % (str(item.get("name") or ""), "; ".join(item.get("issues") or [])))
    if task_cycle is not None and not bool(task_cycle.get("ok", False)):
        issues.append("%s: %s" % (str(task_cycle.get("name") or ""), "; ".join(task_cycle.get("issues") or [])))
    revision_scope = build_runtime_revision_scope(checks)
    latest_head = _lookup_latest_head_scope(client, revision_scope)
    revision_scope = build_runtime_revision_scope(checks, latest_head=latest_head)
    for item in checks:
        name = str(item.get("name") or "")
        if name not in {"slam_status", "system_readiness"}:
            continue
        response = dict(item.get("response") or {})
        payload_key = "state" if name == "slam_status" else "readiness"
        payload = dict(response.get(payload_key) or {})
        if not payload:
            continue
        payload["revision_scope"] = build_revision_scope(
            slam_state=payload if name == "slam_status" else {},
            readiness_state=payload if name == "system_readiness" else {},
            latest_head=latest_head,
        )
        response[payload_key] = payload
        item["response"] = response
    return {
        "task_id": int(args.task_id),
        "actions": actions,
        "task_cycle": task_cycle or {},
        "checks": checks,
        "revision_scope": revision_scope,
        "summary": {
            "ok": not issues,
            "issues": issues,
        },
    }


def _print_text(report: Dict[str, object]) -> None:
    print("Backend runtime smoke")
    print("Task baseline: %s" % int(report.get("task_id", 0) or 0))
    revision_scope = dict(report.get("revision_scope") or {})
    if revision_scope:
        print("Revision scope: %s" % _format_revision_scope(revision_scope))
    for item in list(report.get("checks") or []):
        status = "OK" if bool(item.get("ok", False)) else "FAIL"
        print("- check %s: %s" % (str(item.get("name") or ""), status))
        payload = dict((item.get("response") or {}).get("state") or (item.get("response") or {}).get("readiness") or {})
        if payload:
            scope = dict(payload.get("revision_scope") or {})
            if scope:
                print("  revision_scope: %s" % _format_revision_scope(scope))
        for issue in list(item.get("issues") or []):
            print("  issue: %s" % str(issue))
    for item in list(report.get("actions") or []):
        status = "OK" if bool(item.get("ok", False)) else "FAIL"
        submit = dict(item.get("submit") or {})
        print("- action %s: %s" % (str(item.get("name") or ""), status))
        print("  submit: accepted=%s job_id=%s error_code=%s message=%s" % (
            bool(submit.get("accepted", False)),
            str(submit.get("job_id") or ""),
            str(submit.get("error_code") or ""),
            str(submit.get("message") or ""),
        ))
        job = dict((item.get("job") or {}).get("response") or {})
        if job:
            job_payload = dict(job.get("job") or {})
            print(
                "  job: terminal_state=%s job_state=%s result_code=%s result_message=%s"
                % (
                    str((item.get("job") or {}).get("terminal_state") or ""),
                    str(job_payload.get("job_state") or ""),
                    str(job_payload.get("result_code") or ""),
                    str(job_payload.get("result_message") or ""),
                )
            )
            requested_revision_id = str(job_payload.get("requested_map_revision_id") or "")
            resolved_revision_id = str(job_payload.get("resolved_map_revision_id") or "")
            if requested_revision_id or resolved_revision_id:
                print(
                    "  job_revision_scope: requested=%s resolved=%s"
                    % (requested_revision_id or "-", resolved_revision_id or "-")
                )
        for issue in list(item.get("issues") or []):
            print("  issue: %s" % str(issue))
    task_cycle = dict(report.get("task_cycle") or {})
    if task_cycle:
        print("- task_cycle: %s" % ("OK" if bool(task_cycle.get("ok", False)) else "FAIL"))
        start = dict(task_cycle.get("start") or {})
        print(
            "  start: success=%s message=%s run_id=%s running_seen=%s"
            % (
                bool(start.get("success", False)),
                str(start.get("message") or ""),
                str(task_cycle.get("run_id") or ""),
                bool(task_cycle.get("running_seen", False)),
            )
        )
        final_db_state = dict(task_cycle.get("final_db_state") or {})
        final_run = dict(final_db_state.get("run") or {})
        final_runtime = dict(final_db_state.get("runtime") or {})
        if final_run or final_runtime:
            print(
                "  final_db: run_state=%s plan_id=%s map_revision_id=%s runtime=%s/%s/%s/%s active_run_id=%s"
                % (
                    str(final_run.get("state") or ""),
                    str(final_run.get("plan_id") or ""),
                    str(final_run.get("map_revision_id") or ""),
                    str(final_runtime.get("mission_state") or ""),
                    str(final_runtime.get("phase") or ""),
                    str(final_runtime.get("public_state") or ""),
                    str(final_runtime.get("executor_state") or ""),
                    str(final_runtime.get("active_run_id") or ""),
                )
            )
        for issue in list(task_cycle.get("issues") or []):
            print("  issue: %s" % str(issue))
    summary = dict(report.get("summary") or {})
    print("Summary: %s" % ("OK" if bool(summary.get("ok", False)) else "FAIL"))
    for issue in list(summary.get("issues") or []):
        print("- %s" % str(issue))


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run backend runtime smoke checks for SLAM / odom / readiness, with optional minimal action jobs."
    )
    parser.add_argument("--task-id", type=int, default=0, help="task id for readiness baseline")
    parser.add_argument("--service-timeout", type=float, default=10.0, help="wait time for core services")
    parser.add_argument("--job-timeout", type=float, default=30.0, help="wait time for async slam jobs")
    parser.add_argument("--poll-interval", type=float, default=1.0, help="poll interval for get_slam_job")
    parser.add_argument("--robot-id", default="local_robot", help="robot_id for slam actions")
    parser.add_argument("--map-name", default="", help="optional map_name for actions")
    parser.add_argument("--map-revision-id", default="", help="optional map_revision_id for revision-scoped actions")
    parser.add_argument("--frame-id", default="map", help="frame_id for initial pose actions")
    parser.add_argument("--save-map-name", default="", help="save_map_name for save_mapping")
    parser.add_argument("--description-prefix", default="backend_runtime_smoke", help="action description prefix")
    parser.add_argument(
        "--actions",
        default="",
        help="comma-separated action list from: %s" % ", ".join(SUPPORTED_ACTIONS),
    )
    parser.add_argument("--set-active", action="store_true", help="set_active on submit")
    parser.add_argument("--has-initial-pose", action="store_true", help="send initial pose with relocalize actions")
    parser.add_argument("--initial-pose-x", type=float, default=0.0)
    parser.add_argument("--initial-pose-y", type=float, default=0.0)
    parser.add_argument("--initial-pose-yaw", type=float, default=0.0)
    parser.add_argument("--include-unfinished-submaps", action="store_true")
    parser.add_argument("--set-active-on-save", action="store_true")
    parser.add_argument("--switch-to-localization-after-save", action="store_true")
    parser.add_argument("--relocalize-after-switch", action="store_true")
    parser.add_argument(
        "--ignore-warning",
        action="append",
        default=["station_status stale or missing"],
        help="warning text to ignore; may be passed multiple times",
    )
    parser.add_argument("--run-task-cycle", action="store_true", help="start the given task_id once and wait for DONE/idle")
    parser.add_argument("--task-timeout", type=float, default=300.0, help="timeout for task cycle acceptance")
    parser.add_argument("--ops-db-path", default="", help="operations.db path for task cycle persistence checks")
    parser.add_argument("--json", action="store_true", help="print json report")
    parser.add_argument("--text", action="store_true", help="print text report")
    return parser


def validate_args(args) -> None:
    args.actions = parse_actions(args.actions)
    if "save_mapping" in args.actions and not str(args.save_map_name or "").strip():
        raise ValueError("--save-map-name is required when actions include save_mapping")
    if args.relocalize_after_switch and not args.switch_to_localization_after_save:
        raise ValueError("--relocalize-after-switch requires --switch-to-localization-after-save")
    revision_scoped_actions = {"switch_map_and_localize", "relocalize", "verify_map_revision", "activate_map_revision"}
    if revision_scoped_actions.intersection(set(args.actions or [])):
        if not (str(args.map_name or "").strip() or str(args.map_revision_id or "").strip()):
            raise ValueError("--map-name or --map-revision-id is required for revision-scoped actions")
    if bool(args.run_task_cycle):
        if int(args.task_id or 0) <= 0:
            raise ValueError("--task-id must be > 0 when --run-task-cycle is enabled")
        if not str(args.ops_db_path or "").strip():
            raise ValueError("--ops-db-path is required when --run-task-cycle is enabled")


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()
    try:
        validate_args(args)
        report = build_report(args)
    except Exception as exc:
        if args.json:
            json.dump({"summary": {"ok": False, "issues": [str(exc)]}}, sys.stdout, ensure_ascii=False, indent=2)
            sys.stdout.write("\n")
        else:
            print("Summary: FAIL")
            print("- %s" % str(exc))
        return 1

    if args.json:
        json.dump(report, sys.stdout, ensure_ascii=False, indent=2, sort_keys=True)
        sys.stdout.write("\n")
    if args.text or not args.json:
        _print_text(report)
    return 0 if bool(dict(report.get("summary") or {}).get("ok", False)) else 1


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import os
import sqlite3
import sys
import time
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from coverage_planner.new_vehicle_commissioning_state import (
    CommissioningStateError,
    build_new_vehicle_commissioning_snapshot,
)


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
SLAM_ACTION_OPERATION_CODES = {
    "start_mapping": 3,
    "save_mapping": 4,
    "stop_mapping": 5,
    "prepare_for_task": 6,
    "switch_map_and_localize": 7,
    "relocalize": 8,
    "verify_map_revision": 9,
    "activate_map_revision": 10,
}

TASK_READY_PROFILE = "task_ready"
STAGE_K_NEW_VEHICLE_PROFILE = "stage_k_new_vehicle_no_map"
SUPPORTED_PROFILES = (TASK_READY_PROFILE, STAGE_K_NEW_VEHICLE_PROFILE)
DEFAULT_TASK_READY_IGNORED_WARNINGS = ("station_status stale or missing",)
STAGE_K_REQUIRED_READINESS_WARNINGS = {
    "battery_state missing",
    "combined_status missing",
    "station bridge offline",
}
STAGE_K_OPTIONAL_READINESS_WARNINGS = {
    'health warning latched: TF_LOOKUP_FAIL "map" passed to lookupTransform argument target_frame does not exist.',
}
STAGE_K_SLAM_WARNINGS = {
    "no current active map selected",
    "tracked_pose stale or missing",
}
STAGE_K_COMMERCIAL_STORAGE_PATHS = {
    "plan_db_path": "/data/coverage/planning.db",
    "ops_db_path": "/data/coverage/operations.db",
    "maps_root": "/data/maps",
    "dock_calibration_path": "/data/coverage/dock_calibration.yaml",
    "auto_charge_state_path": "/data/coverage/auto_charge_monitor_state.json",
    "auto_charge_event_log_path": "/data/coverage/auto_charge_monitor_events.jsonl",
}
STAGE_K_EXPECTED_SERVICE_PROVIDERS = (
    ("/clean_robot_server/app/get_slam_status", "/slam_api_service"),
    ("/clean_robot_server/app/get_odometry_status", "/odometry_health"),
    ("/coverage_task_manager/app/get_system_readiness", "/coverage_task_manager"),
    ("/coverage_task_manager/app/exe_task_server", "/coverage_task_manager"),
    ("/clean_robot_server/app/submit_slam_command", "/slam_api_service"),
    ("/clean_robot_server/app/get_slam_job", "/slam_api_service"),
    ("/clean_robot_server/app/map_server", "/map_asset_service"),
    ("/cartographer/runtime/app/operate", "/slam_runtime_manager"),
    ("/cartographer/runtime/app/submit_job", "/slam_runtime_manager"),
    ("/cartographer/runtime/app/get_job", "/slam_runtime_manager"),
    (
        "/cartographer/runtime/app/restart_localization",
        "/localization_lifecycle_manager",
    ),
    (
        "/clean_robot_server/app/dock_calibration_command",
        "/dock_calibration_service",
    ),
    (
        "/clean_robot_server/app/get_dock_calibration_status",
        "/dock_calibration_service",
    ),
)
STAGE_K_EXPECTED_NODE_PRIVATE_PARAMS = {
    "/slam_api_service": {
        "plan_db_path": "/data/coverage/planning.db",
        "ops_db_path": "/data/coverage/operations.db",
        "maps_root": "/data/maps",
    },
    "/slam_runtime_manager": {
        "plan_db_path": "/data/coverage/planning.db",
        "ops_db_path": "/data/coverage/operations.db",
        "maps_root": "/data/maps",
        "repo_map_root": "/data/maps",
    },
    "/map_asset_service": {
        "plan_db_path": "/data/coverage/planning.db",
        "ops_db_path": "/data/coverage/operations.db",
        "maps_root": "/data/maps",
        "external_maps_root": "/data/maps/imports",
    },
    "/coverage_task_manager": {
        "plan_db_path": "/data/coverage/planning.db",
        "ops_db_path": "/data/coverage/operations.db",
    },
    "/localization_lifecycle_manager": {
        "plan_db_path": "/data/coverage/planning.db",
        "ops_db_path": "/data/coverage/operations.db",
    },
    "/dock_calibration_service": {
        "storage_path": "/data/coverage/dock_calibration.yaml",
    },
}
MAPPING_SESSION_ID_PARAM = "/cartographer/runtime/mapping_session_id"
LOCAL_ROS_MASTER_URIS = {
    "http://127.0.0.1:11311",
    "http://localhost:11311",
}
STAGE_K_READINESS_CHECKS = {
    "active_map": ("WARN", False),
    "runtime_map": ("WARN", False),
    "odometry": ("OK", True),
    "localization": ("WARN", False),
    "slam_runtime": ("ERROR", False),
    "move_base_flex": ("OK", True),
    "mcore_bridge": ("WARN", False),
    "task_manager": ("OK", True),
    "executor": ("OK", True),
    "health": (("OK", "WARN"), True),
    "battery": ("WARN", False),
    "combined_status": ("WARN", False),
    "dock_supply": ("OK", True),
    "station_status": ("WARN", False),
}


def require_explicit_commercial_robot_id(robot_id: str) -> str:
    raw_value = str(robot_id or "")
    value = raw_value.strip()
    if (
        not value
        or value.lower() == "local_robot"
        or "REPLACE" in value.upper()
        or value != raw_value
        or any(character.isspace() for character in value)
    ):
        raise ValueError("an explicit commercial --robot-id is required")
    return value


def require_local_ros_master_uri() -> str:
    value = str(os.environ.get("ROS_MASTER_URI", "") or "").strip()
    if value not in LOCAL_ROS_MASTER_URIS:
        raise ValueError(
            "ROS_MASTER_URI must be local http://127.0.0.1:11311 or http://localhost:11311"
        )
    return value


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


def _job_field(job, field_name: str, default=None):
    if isinstance(job, dict):
        return job.get(field_name, default)
    return getattr(job, field_name, default)


def job_terminal_snapshot(job) -> Tuple[bool, str]:
    job_state = str(
        _job_field(job, "job_state", "") or _job_field(job, "status", "") or ""
    ).strip().lower()
    if bool(_job_field(job, "done", False)):
        return True, job_state or (
            "succeeded" if bool(_job_field(job, "success", False)) else "failed"
        )
    if job_state in TERMINAL_JOB_STATES:
        return True, job_state
    return False, job_state


def job_succeeded(job) -> bool:
    terminal, state = job_terminal_snapshot(job)
    if not terminal or state != "succeeded":
        return False
    if not bool(_job_field(job, "done", False)):
        return False
    if bool(_job_field(job, "manual_assist_required", False)):
        return False
    return bool(
        bool(_job_field(job, "success", False))
        and bool(_job_field(job, "result_success", False))
        and not job_terminal_consistency_issues(job)
    )


def job_terminal_consistency_issues(job) -> List[str]:
    terminal, state = job_terminal_snapshot(job)
    if not terminal:
        return []
    issues = []
    job_state = str(_job_field(job, "job_state", "") or "").strip().lower()
    status = str(_job_field(job, "status", "") or "").strip().lower()
    phase = str(_job_field(job, "phase", "") or "")
    workflow_phase = str(_job_field(job, "workflow_phase", "") or "")
    success = bool(_job_field(job, "success", False))
    result_success = bool(_job_field(job, "result_success", False))
    manual_assist_required = bool(_job_field(job, "manual_assist_required", False))
    error_code = str(_job_field(job, "error_code", "") or "")
    message = str(_job_field(job, "message", "") or "")
    result_code = str(_job_field(job, "result_code", "") or "")
    result_message = str(_job_field(job, "result_message", "") or "")
    if job_state != state or status != state:
        issues.append(
            "terminal job state mismatch expected=%s job_state=%s status=%s"
            % (state or "-", job_state or "-", status or "-")
        )
    if state == "succeeded":
        if not bool(_job_field(job, "done", False)):
            issues.append("succeeded job requires done=true")
        if not success or not result_success:
            issues.append(
                "succeeded job requires success=true and result_success=true "
                "observed=%s/%s" % (success, result_success)
            )
        if manual_assist_required:
            issues.append("succeeded job reports manual_assist_required=true")
        if error_code:
            issues.append(
                "succeeded job requires empty error_code observed=%s" % error_code
            )
        if result_code != "ok":
            issues.append(
                "succeeded job requires result_code=ok observed=%s"
                % (result_code or "-")
            )
        if phase != "done" or workflow_phase != "done":
            issues.append(
                "succeeded job requires phase/workflow_phase=done observed=%s/%s"
                % (phase or "-", workflow_phase or "-")
            )
    elif state in {"failed", "canceled", "manual_assist_required"}:
        if not bool(_job_field(job, "done", False)):
            issues.append("terminal_state=%s requires done=true" % state)
        if success or result_success:
            issues.append(
                "terminal_state=%s conflicts with success/result_success=%s/%s"
                % (state, success, result_success)
            )
        expected_manual_assist = state == "manual_assist_required"
        if manual_assist_required != expected_manual_assist:
            issues.append(
                "terminal_state=%s conflicts with manual_assist_required=%s"
                % (state, manual_assist_required)
            )
        if not error_code.strip():
            issues.append("terminal_state=%s requires nonempty error_code" % state)
        if result_code != error_code or result_code.strip().lower() == "ok":
            issues.append(
                "terminal_state=%s requires result_code=error_code!=ok observed=%s/%s"
                % (state, result_code or "-", error_code or "-")
            )
        expected_phase = {
            "failed": "failed",
            "canceled": "canceled",
            "manual_assist_required": "manual_assist_required",
        }[state]
        if phase != expected_phase or workflow_phase != expected_phase:
            issues.append(
                "terminal_state=%s requires phase/workflow_phase=%s observed=%s/%s"
                % (state, expected_phase, phase or "-", workflow_phase or "-")
            )
    else:
        issues.append("unsupported terminal_state=%s" % (state or "-"))
    if result_message != message:
        issues.append(
            "terminal job result_message/message mismatch observed=%s/%s"
            % (result_message, message)
        )
    return issues


def _ros_time_to_dict(stamp) -> Dict[str, int]:
    return {
        "secs": int(getattr(stamp, "secs", 0) or 0),
        "nsecs": int(getattr(stamp, "nsecs", 0) or 0),
    }


def _job_to_dict(job) -> Dict[str, object]:
    if job is None:
        return {}
    requested_map_name = str(getattr(job, "requested_map_name", "") or "")
    requested_revision_id = str(getattr(job, "requested_map_revision_id", "") or "")
    resolved_map_name = str(getattr(job, "resolved_map_name", "") or "")
    resolved_revision_id = str(getattr(job, "resolved_map_revision_id", "") or "")
    return {
        "job_id": str(getattr(job, "job_id", "") or ""),
        "robot_id": str(getattr(job, "robot_id", "") or ""),
        "operation": int(getattr(job, "operation", 0) or 0),
        "operation_name": str(getattr(job, "operation_name", "") or ""),
        "requested_map_name": requested_map_name,
        "resolved_map_name": resolved_map_name,
        "description": str(getattr(job, "description", "") or ""),
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
                map_name=requested_map_name,
                revision_id=requested_revision_id,
                source="slam_job.requested_map_revision_id",
            ),
            "resolved": _revision_scope_slot(
                map_name=resolved_map_name,
                revision_id=resolved_revision_id,
                source="slam_job.resolved_map_revision_id",
            ),
        },
        "created_at": _ros_time_to_dict(getattr(job, "created_at", None) or object()),
        "started_at": _ros_time_to_dict(getattr(job, "started_at", None) or object()),
        "finished_at": _ros_time_to_dict(getattr(job, "finished_at", None) or object()),
        "updated_at": _ros_time_to_dict(getattr(job, "updated_at", None) or object()),
    }


def job_contract_issues(
    job,
    *,
    expected_job_id: str,
    expected_robot_id: str,
    expected_operation_name: str = "",
    expected_map_name: Optional[str] = None,
    expected_map_revision_id: Optional[str] = None,
    expected_description: Optional[str] = None,
    check_resolved_scope: bool = False,
    allow_resolved_revision_change: bool = False,
    require_resolved_revision_id: bool = False,
) -> List[str]:
    payload = dict(job or {}) if isinstance(job, dict) else _job_to_dict(job)
    expected_job_id = str(expected_job_id or "")
    expected_robot_id = str(expected_robot_id or "")
    observed_job_id = str(payload.get("job_id") or "")
    observed_robot_id = str(payload.get("robot_id") or "")
    issues = []
    if observed_job_id != expected_job_id or observed_robot_id != expected_robot_id:
        issues.append(
            "job identity mismatch expected=%s/%s observed=%s/%s"
            % (
                expected_robot_id,
                expected_job_id,
                observed_robot_id or "-",
                observed_job_id or "-",
            )
        )

    expected_operation_name = str(expected_operation_name or "")
    if expected_operation_name:
        expected_operation = int(SLAM_ACTION_OPERATION_CODES.get(expected_operation_name, 0) or 0)
        observed_operation_name = str(payload.get("operation_name") or "")
        observed_operation = int(payload.get("operation", 0) or 0)
        if (
            observed_operation_name != expected_operation_name
            or not expected_operation
            or observed_operation != expected_operation
        ):
            issues.append(
                "job operation mismatch expected=%s/%s observed=%s/%s"
                % (
                    expected_operation_name,
                    expected_operation or "-",
                    observed_operation_name or "-",
                    observed_operation or "-",
                )
            )

    if expected_description is not None:
        expected_description = str(expected_description)
        observed_description = str(payload.get("description") or "")
        if observed_description != expected_description:
            issues.append(
                "job description mismatch expected=%s observed=%s"
                % (expected_description or "-", observed_description or "-")
            )

    if expected_map_name is not None:
        expected_map_name = str(expected_map_name)
        requested_map_name = str(payload.get("requested_map_name") or "")
        if requested_map_name != expected_map_name:
            issues.append(
                "job requested map_name mismatch expected=%s observed=%s"
                % (expected_map_name or "-", requested_map_name or "-")
            )
        if check_resolved_scope:
            resolved_map_name = str(payload.get("resolved_map_name") or "")
            if resolved_map_name != expected_map_name:
                issues.append(
                    "job resolved map_name mismatch expected=%s observed=%s"
                    % (expected_map_name or "-", resolved_map_name or "-")
                )

    if expected_map_revision_id is not None:
        expected_map_revision_id = str(expected_map_revision_id)
        requested_revision_id = str(payload.get("requested_map_revision_id") or "")
        if requested_revision_id != expected_map_revision_id:
            issues.append(
                "job requested map_revision_id mismatch expected=%s observed=%s"
                % (expected_map_revision_id or "-", requested_revision_id or "-")
            )
        if check_resolved_scope and not allow_resolved_revision_change:
            resolved_revision_id = str(payload.get("resolved_map_revision_id") or "")
            if resolved_revision_id != expected_map_revision_id:
                issues.append(
                    "job resolved map_revision_id mismatch expected=%s observed=%s"
                    % (expected_map_revision_id or "-", resolved_revision_id or "-")
                )
    if check_resolved_scope and require_resolved_revision_id:
        resolved_revision_id = str(payload.get("resolved_map_revision_id") or "")
        if not resolved_revision_id:
            issues.append("job resolved map_revision_id is empty")
    return issues


def accepted_submit_consistency_issues(
    submit,
    *,
    expected_job_id: str,
    expected_robot_id: str,
    expected_operation_name: str,
    expected_map_name: Optional[str],
    expected_map_revision_id: Optional[str],
    expected_description: str,
) -> List[str]:
    """Validate the asynchronous job returned by an accepted submit response."""

    payload = dict(submit or {}) if isinstance(submit, dict) else {
        "accepted": bool(getattr(submit, "accepted", False)),
        "message": str(getattr(submit, "message", "") or ""),
        "error_code": str(getattr(submit, "error_code", "") or ""),
        "job_id": str(getattr(submit, "job_id", "") or ""),
        "job": _job_to_dict(getattr(submit, "job", None)),
    }
    issues = []
    if payload.get("accepted") is not True:
        issues.append("submit accepted must be true")
        return issues
    submit_error_code = str(payload.get("error_code") or "")
    if submit_error_code:
        issues.append(
            "accepted submit requires empty error_code observed=%s" % submit_error_code
        )
    submit_job_id = str(payload.get("job_id") or "")
    if submit_job_id != str(expected_job_id or ""):
        issues.append(
            "accepted submit job_id mismatch expected=%s observed=%s"
            % (str(expected_job_id or "-") or "-", submit_job_id or "-")
        )
    job = payload.get("job")
    job_payload = dict(job or {}) if isinstance(job, dict) else _job_to_dict(job)
    if not job_payload:
        issues.append("accepted submit job payload is missing")
        return issues
    issues.extend(
        job_contract_issues(
            job_payload,
            expected_job_id=expected_job_id,
            expected_robot_id=expected_robot_id,
            expected_operation_name=expected_operation_name,
            expected_map_name=expected_map_name,
            expected_map_revision_id=expected_map_revision_id,
            expected_description=expected_description,
            check_resolved_scope=False,
        )
    )
    status = str(job_payload.get("status") or "")
    job_state = str(job_payload.get("job_state") or "")
    phase = str(job_payload.get("phase") or "")
    workflow_phase = str(job_payload.get("workflow_phase") or "")
    if status != "queued" or job_state != "queued":
        issues.append(
            "accepted submit job requires status/job_state=queued observed=%s/%s"
            % (status or "-", job_state or "-")
        )
    if phase != "accepted" or workflow_phase != "accepted":
        issues.append(
            "accepted submit job requires phase/workflow_phase=accepted observed=%s/%s"
            % (phase or "-", workflow_phase or "-")
        )
    if bool(job_payload.get("done", False)):
        issues.append("accepted submit job requires done=false")
    if bool(job_payload.get("success", False)) or bool(
        job_payload.get("result_success", False)
    ):
        issues.append("accepted submit job requires success/result_success=false/false")
    if bool(job_payload.get("manual_assist_required", False)):
        issues.append("accepted submit job requires manual_assist_required=false")
    job_error_code = str(job_payload.get("error_code") or "")
    result_code = str(job_payload.get("result_code") or "")
    if job_error_code or result_code:
        issues.append(
            "accepted submit job requires empty error_code/result_code observed=%s/%s"
            % (job_error_code or "-", result_code or "-")
        )
    submit_message = str(payload.get("message") or "")
    job_message = str(job_payload.get("message") or "")
    result_message = str(job_payload.get("result_message") or "")
    if not submit_message or job_message != submit_message or result_message != job_message:
        issues.append(
            "accepted submit message contract mismatch submit/job/result=%s/%s/%s"
            % (submit_message or "-", job_message or "-", result_message or "-")
        )
    return issues


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
        require_local_ros_master_uri()
        import rospy

        from cleanrobot_app_msgs.msg import PgmData
        from cleanrobot_app_msgs.srv import (
            ExeTask,
            GetDockCalibrationStatus,
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
        self._get_dock_calibration_status = rospy.ServiceProxy(
            "/clean_robot_server/app/get_dock_calibration_status",
            GetDockCalibrationStatus,
        )
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
        services = tuple(
            service_name
            for service_name, _expected_node in STAGE_K_EXPECTED_SERVICE_PROVIDERS
        )
        for service_name in services:
            remain = max(0.1, deadline - time.time())
            self.rospy.wait_for_service(service_name, timeout=remain)

    def get_ros_topology_identity(self) -> Dict[str, object]:
        import rosservice

        service_providers = {}
        node_robot_ids = {}
        node_private_params = {}
        for service_name, expected_node in STAGE_K_EXPECTED_SERVICE_PROVIDERS:
            service_providers[service_name] = rosservice.get_service_node(service_name)
        expected_nodes = {
            expected_node
            for _service_name, expected_node in STAGE_K_EXPECTED_SERVICE_PROVIDERS
        }
        for expected_node in sorted(expected_nodes):
            node_robot_ids[expected_node] = self.rospy.get_param(
                "%s/robot_id" % expected_node,
                None,
            )
        for expected_node, expected_params in STAGE_K_EXPECTED_NODE_PRIVATE_PARAMS.items():
            node_private_params[expected_node] = {
                param_name: self.rospy.get_param(
                    "%s/%s" % (expected_node, param_name),
                    None,
                )
                for param_name in expected_params
            }
        return {
            "service_providers": service_providers,
            "node_robot_ids": node_robot_ids,
            "node_private_params": node_private_params,
            "mapping_session_id": str(
                self.rospy.get_param(MAPPING_SESSION_ID_PARAM, "") or ""
            ),
        }

    def get_mapping_session_id(self) -> str:
        return str(self.rospy.get_param(MAPPING_SESSION_ID_PARAM, "") or "")

    def get_slam_status(self, robot_id: str, refresh_map_identity: bool = True):
        return self._get_slam_status(robot_id=str(robot_id or ""), refresh_map_identity=refresh_map_identity)

    def get_odometry_status(self, robot_id: str):
        return self._get_odometry_status(robot_id=str(robot_id or ""))

    def get_dock_calibration_status(self, robot_id: str):
        return self._get_dock_calibration_status(robot_id=str(robot_id or ""))

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
            "task_id": int(getattr(readiness, "task_id", 0) or 0),
            "task_name": str(getattr(readiness, "task_name", "") or ""),
            "task_map_name": str(getattr(readiness, "task_map_name", "") or ""),
            "task_zone_id": str(getattr(readiness, "task_zone_id", "") or ""),
            "task_plan_profile": str(getattr(readiness, "task_plan_profile", "") or ""),
            "mission_state": str(getattr(readiness, "mission_state", "") or ""),
            "phase": str(getattr(readiness, "phase", "") or ""),
            "public_state": str(getattr(readiness, "public_state", "") or ""),
            "executor_state": str(getattr(readiness, "executor_state", "") or ""),
            "dock_supply_state": str(getattr(readiness, "dock_supply_state", "") or ""),
            "battery_valid": bool(getattr(readiness, "battery_valid", False)),
            "task_map_revision_id": str(getattr(readiness, "task_map_revision_id", "") or ""),
            "active_map_revision_id": str(getattr(readiness, "active_map_revision_id", "") or ""),
            "runtime_map_revision_id": str(getattr(readiness, "runtime_map_revision_id", "") or ""),
            "active_map_name": str(getattr(readiness, "active_map_name", "") or ""),
            "runtime_map_name": str(getattr(readiness, "runtime_map_name", "") or ""),
            "active_map_id": str(getattr(readiness, "active_map_id", "") or ""),
            "active_map_md5": str(getattr(readiness, "active_map_md5", "") or ""),
            "runtime_map_id": str(getattr(readiness, "runtime_map_id", "") or ""),
            "runtime_map_md5": str(getattr(readiness, "runtime_map_md5", "") or ""),
            "manual_assist_required": bool(getattr(readiness, "manual_assist_required", False)),
            "manual_assist_map_name": str(getattr(readiness, "manual_assist_map_name", "") or ""),
            "manual_assist_map_revision_id": str(getattr(readiness, "manual_assist_map_revision_id", "") or ""),
            "manual_assist_retry_action": str(getattr(readiness, "manual_assist_retry_action", "") or ""),
            "manual_assist_guidance": str(getattr(readiness, "manual_assist_guidance", "") or ""),
            "blocking_reasons": [str(item) for item in list(getattr(readiness, "blocking_reasons", []) or [])],
            "warnings": [str(item) for item in list(getattr(readiness, "warnings", []) or [])],
            "checks": [
                {
                    "key": str(getattr(item, "key", "") or ""),
                    "level": str(getattr(item, "level", "") or ""),
                    "ok": bool(getattr(item, "ok", False)),
                    "fresh": bool(getattr(item, "fresh", False)),
                    "stale": bool(getattr(item, "stale", False)),
                    "missing": bool(getattr(item, "missing", False)),
                    "age_s": float(getattr(item, "age_s", -1.0) or 0.0),
                    "summary": str(getattr(item, "summary", "") or ""),
                }
                for item in list(getattr(readiness, "checks", []) or [])
            ],
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


def _check_odometry(
    resp,
    ignored_warnings: Sequence[str],
    expected_robot_id: str = "",
) -> Dict[str, object]:
    state = getattr(resp, "state", None)
    issues = []
    if not bool(getattr(resp, "success", False)):
        issues.append("service success=false message=%s" % str(getattr(resp, "message", "") or ""))
    if state is None:
        issues.append("missing odometry state")
        state_dict = {}
    else:
        state_dict = {
            "robot_id": str(getattr(state, "robot_id", "") or ""),
            "odom_source": str(getattr(state, "odom_source", "") or ""),
            "odom_topic": str(getattr(state, "odom_topic", "") or ""),
            "raw_odom_topic": str(getattr(state, "raw_odom_topic", "") or ""),
            "imu_topic": str(getattr(state, "imu_topic", "") or ""),
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
        if expected_robot_id and state_dict["robot_id"] != str(expected_robot_id):
            issues.append(
                "robot_id mismatch expected=%s observed=%s"
                % (str(expected_robot_id), state_dict["robot_id"] or "-")
            )
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


def _check_slam(
    resp,
    ignored_warnings: Sequence[str],
    expected_robot_id: str = "",
) -> Dict[str, object]:
    state = getattr(resp, "state", None)
    issues = []
    if not bool(getattr(resp, "success", False)):
        issues.append("service success=false message=%s" % str(getattr(resp, "message", "") or ""))
    if state is None:
        issues.append("missing slam state")
        state_dict = {}
    else:
        state_dict = {
            "robot_id": str(getattr(state, "robot_id", "") or ""),
            "desired_mode": str(getattr(state, "desired_mode", "") or ""),
            "current_mode": str(getattr(state, "current_mode", "") or ""),
            "runtime_mode": str(getattr(state, "runtime_mode", "") or ""),
            "workflow_state": str(getattr(state, "workflow_state", "") or ""),
            "workflow_phase": str(getattr(state, "workflow_phase", "") or ""),
            "active_map_revision_id": str(getattr(state, "active_map_revision_id", "") or ""),
            "runtime_map_revision_id": str(getattr(state, "runtime_map_revision_id", "") or ""),
            "active_map_name": str(getattr(state, "active_map_name", "") or ""),
            "runtime_map_name": str(getattr(state, "runtime_map_name", "") or ""),
            "active_map_id": str(getattr(state, "active_map_id", "") or ""),
            "active_map_md5": str(getattr(state, "active_map_md5", "") or ""),
            "runtime_map_id": str(getattr(state, "runtime_map_id", "") or ""),
            "runtime_map_md5": str(getattr(state, "runtime_map_md5", "") or ""),
            "pending_map_name": str(getattr(state, "pending_map_name", "") or ""),
            "pending_map_revision_id": str(getattr(state, "pending_map_revision_id", "") or ""),
            "pending_map_switch_status": str(getattr(state, "pending_map_switch_status", "") or ""),
            "localization_state": str(getattr(state, "localization_state", "") or ""),
            "localization_valid": bool(getattr(state, "localization_valid", False)),
            "runtime_map_ready": bool(getattr(state, "runtime_map_ready", False)),
            "active_map_match": bool(getattr(state, "active_map_match", False)),
            "runtime_map_match": bool(getattr(state, "runtime_map_match", False)),
            "lifecycle_state": str(getattr(state, "lifecycle_state", "") or ""),
            "active_job_id": str(getattr(state, "active_job_id", "") or ""),
            "active_job_status": str(getattr(state, "active_job_status", "") or ""),
            "active_job_phase": str(getattr(state, "active_job_phase", "") or ""),
            "map_topic_fresh": bool(getattr(state, "map_topic_fresh", False)),
            "tracked_pose_fresh": bool(getattr(state, "tracked_pose_fresh", False)),
            "mission_state": str(getattr(state, "mission_state", "") or ""),
            "phase": str(getattr(state, "phase", "") or ""),
            "public_state": str(getattr(state, "public_state", "") or ""),
            "executor_state": str(getattr(state, "executor_state", "") or ""),
            "task_running": bool(getattr(state, "task_running", False)),
            "localization_backend_available": bool(
                getattr(state, "localization_backend_available", False)
            ),
            "runtime_reload_service_available": bool(
                getattr(state, "runtime_reload_service_available", False)
            ),
            "runtime_save_state_service_available": bool(
                getattr(state, "runtime_save_state_service_available", False)
            ),
            "can_switch_map_and_localize": bool(
                getattr(state, "can_switch_map_and_localize", False)
            ),
            "can_relocalize": bool(getattr(state, "can_relocalize", False)),
            "busy": bool(getattr(state, "busy", False)),
            "mapping_session_active": bool(getattr(state, "mapping_session_active", False)),
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
            "last_error_code": str(getattr(state, "last_error_code", "") or ""),
            "last_error_msg": str(getattr(state, "last_error_msg", "") or ""),
            "blocking_reasons": [str(item) for item in list(getattr(state, "blocking_reasons", []) or [])],
            "warnings": [str(item) for item in list(getattr(state, "warnings", []) or [])],
            "stamp": _ros_time_to_dict(getattr(state, "stamp", None) or object()),
        }
        if expected_robot_id and state_dict["robot_id"] != str(expected_robot_id):
            issues.append(
                "robot_id mismatch expected=%s observed=%s"
                % (str(expected_robot_id), state_dict["robot_id"] or "-")
            )
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


def runtime_identity_issues(
    checks: Sequence[Dict[str, object]],
    expected_robot_id: str,
) -> List[str]:
    expected = str(expected_robot_id or "")
    by_name = {str(item.get("name") or ""): item for item in list(checks or [])}
    issues = []
    for check_name in ("slam_status", "odometry_status"):
        check = dict(by_name.get(check_name) or {})
        if not check:
            issues.append("%s missing" % check_name)
            continue
        response = dict(check.get("response") or {})
        state = dict(response.get("state") or {})
        if not bool(response.get("success", False)):
            issues.append("%s service success=false" % check_name)
        observed = str(state.get("robot_id") or "")
        if observed != expected:
            issues.append(
                "%s robot_id mismatch expected=%s observed=%s"
                % (check_name, expected, observed or "-")
            )
    return issues


def _check_commercial_ros_topology_identity(
    snapshot: Dict[str, object],
    expected_robot_id: str,
    *,
    check_name: str,
    require_empty_mapping_session: bool,
) -> Dict[str, object]:
    expected_identity = str(expected_robot_id)
    payload = snapshot if isinstance(snapshot, dict) else {}
    issues = []
    if not isinstance(snapshot, dict):
        issues.append("ROS topology identity snapshot must be a dictionary")
    provider_payload = payload.get("service_providers", {})
    robot_id_payload = payload.get("node_robot_ids", {})
    private_param_payload = payload.get("node_private_params", {})
    if not isinstance(provider_payload, dict):
        issues.append("service_providers must be a dictionary")
        provider_payload = {}
    if not isinstance(robot_id_payload, dict):
        issues.append("node_robot_ids must be a dictionary")
        robot_id_payload = {}
    if not isinstance(private_param_payload, dict):
        issues.append("node_private_params must be a dictionary")
        private_param_payload = {}
    service_providers = dict(provider_payload)
    node_robot_ids = dict(robot_id_payload)
    node_private_params = dict(private_param_payload)
    expected_service_providers = dict(STAGE_K_EXPECTED_SERVICE_PROVIDERS)
    expected_nodes = {
        expected_node
        for _service_name, expected_node in STAGE_K_EXPECTED_SERVICE_PROVIDERS
    }
    observed_service_names = set(service_providers)
    expected_service_names = set(expected_service_providers)
    for service_name in sorted(observed_service_names - expected_service_names):
        issues.append("unexpected service provider ownership service=%s" % service_name)
    for node_name in sorted(set(node_robot_ids) - expected_nodes):
        issues.append("unexpected node robot_id ownership node=%s" % node_name)
    expected_private_nodes = set(STAGE_K_EXPECTED_NODE_PRIVATE_PARAMS)
    for node_name in sorted(set(node_private_params) - expected_private_nodes):
        issues.append("unexpected node private parameter ownership node=%s" % node_name)
    mapping_session_id = str(payload.get("mapping_session_id") or "")
    if require_empty_mapping_session and mapping_session_id:
        issues.append(
            "%s requires an empty mapping session token observed=%s"
            % (str(check_name), mapping_session_id)
        )
    for service_name, expected_node in STAGE_K_EXPECTED_SERVICE_PROVIDERS:
        observed_node = service_providers.get(service_name)
        if observed_node != expected_node:
            issues.append(
                "service provider mismatch service=%s expected=%s observed=%s"
                % (service_name, expected_node, repr(observed_node))
            )
        observed_robot_id = node_robot_ids.get(expected_node)
        if observed_robot_id != expected_identity:
            issues.append(
                "node robot_id mismatch node=%s expected=%s observed=%s"
                % (expected_node, expected_identity, repr(observed_robot_id))
            )
    for expected_node, expected_params in STAGE_K_EXPECTED_NODE_PRIVATE_PARAMS.items():
        observed_params_payload = node_private_params.get(expected_node)
        if not isinstance(observed_params_payload, dict):
            issues.append(
                "node private parameters missing or invalid node=%s observed=%s"
                % (expected_node, repr(observed_params_payload))
            )
            observed_params = {}
        else:
            observed_params = dict(observed_params_payload)
        unexpected_params = set(observed_params) - set(expected_params)
        for param_name in sorted(unexpected_params):
            issues.append(
                "unexpected node private parameter ownership node=%s param=%s"
                % (expected_node, param_name)
            )
        for param_name, expected_value in expected_params.items():
            observed_value = observed_params.get(param_name)
            if observed_value != expected_value:
                issues.append(
                    "node private parameter mismatch node=%s param=%s expected=%s observed=%s"
                    % (
                        expected_node,
                        param_name,
                        expected_value,
                        repr(observed_value),
                    )
                )
    return {
        "name": str(check_name),
        "ok": not issues,
        "issues": issues,
        "response": {
            "expected_robot_id": expected_identity,
            "expected_service_providers": expected_service_providers,
            "expected_node_private_params": STAGE_K_EXPECTED_NODE_PRIVATE_PARAMS,
            "service_providers": service_providers,
            "node_robot_ids": node_robot_ids,
            "node_private_params": node_private_params,
            "mapping_session_id": mapping_session_id,
        },
    }


def _check_stage_k_ros_topology_identity(
    snapshot: Dict[str, object],
    expected_robot_id: str,
) -> Dict[str, object]:
    return _check_commercial_ros_topology_identity(
        snapshot,
        expected_robot_id,
        check_name="stage_k_ros_topology_identity",
        require_empty_mapping_session=True,
    )


def _commercial_ros_topology_identity_check(
    client: BackendRuntimeSmokeClient,
    expected_robot_id: str,
    *,
    check_name: str,
    require_empty_mapping_session: bool,
) -> Dict[str, object]:
    try:
        snapshot = client.get_ros_topology_identity()
        return _check_commercial_ros_topology_identity(
            snapshot,
            expected_robot_id,
            check_name=check_name,
            require_empty_mapping_session=require_empty_mapping_session,
        )
    except Exception as exc:
        return {
            "name": str(check_name),
            "ok": False,
            "issues": ["ROS topology identity inspection failed: %s" % str(exc)],
            "response": {
                "expected_robot_id": str(expected_robot_id),
                "expected_service_providers": dict(STAGE_K_EXPECTED_SERVICE_PROVIDERS),
                "expected_node_private_params": STAGE_K_EXPECTED_NODE_PRIVATE_PARAMS,
                "service_providers": {},
                "node_robot_ids": {},
                "node_private_params": {},
            },
        }


def _stage_k_ros_topology_identity_check(
    client: BackendRuntimeSmokeClient,
    expected_robot_id: str,
) -> Dict[str, object]:
    return _commercial_ros_topology_identity_check(
        client,
        expected_robot_id,
        check_name="stage_k_ros_topology_identity",
        require_empty_mapping_session=True,
    )


def _exact_text_set_issues(
    *,
    label: str,
    observed: Sequence[str],
    expected: Sequence[str],
) -> List[str]:
    values = [str(item) for item in list(observed or [])]
    issues = []
    if len(values) != len(set(values)):
        issues.append("%s contains duplicate entries" % label)
    observed_set = set(values)
    expected_set = set(expected)
    if observed_set != expected_set:
        issues.append(
            "%s mismatch missing=%s extra=%s"
            % (label, sorted(expected_set - observed_set), sorted(observed_set - expected_set))
        )
    return issues


def _stage_k_localization_blocker(localization_state: str) -> str:
    state = str(localization_state or "").strip()
    return "runtime localization not ready: state=%s valid=false" % (state or "-")


def _check_stage_k_new_vehicle_slam(resp, expected_robot_id: str) -> Dict[str, object]:
    result = _check_slam(resp, ignored_warnings=[], expected_robot_id=expected_robot_id)
    state = dict((result.get("response") or {}).get("state") or {})
    issues = []
    if not bool((result.get("response") or {}).get("success", False)):
        issues.append(
            "service success=false message=%s"
            % str((result.get("response") or {}).get("message", "") or "")
        )
    if not state:
        issues.append("missing slam state")
    else:
        if state.get("robot_id") != str(expected_robot_id):
            issues.append(
                "robot_id mismatch expected=%s observed=%s"
                % (str(expected_robot_id), str(state.get("robot_id") or "-"))
            )
        for field in (
            "active_map_name",
            "active_map_revision_id",
            "active_map_id",
            "active_map_md5",
            "runtime_map_name",
            "runtime_map_revision_id",
            "runtime_map_id",
            "runtime_map_md5",
            "pending_map_name",
            "pending_map_revision_id",
            "pending_map_switch_status",
            "active_job_id",
            "active_job_status",
            "active_job_phase",
            "last_error_code",
            "last_error_msg",
            "manual_assist_map_name",
            "manual_assist_map_revision_id",
            "manual_assist_retry_action",
            "manual_assist_guidance",
        ):
            if str(state.get(field) or "").strip():
                issues.append("%s must be empty" % field)
        for field in (
            "localization_valid",
            "runtime_map_ready",
            "active_map_match",
            "runtime_map_match",
            "map_topic_fresh",
            "tracked_pose_fresh",
            "task_running",
            "busy",
            "mapping_session_active",
            "task_ready",
            "manual_assist_required",
            "can_switch_map_and_localize",
            "can_relocalize",
            "can_verify_map_revision",
            "can_activate_map_revision",
            "can_save_mapping",
            "can_stop_mapping",
        ):
            if bool(state.get(field, False)):
                issues.append("%s must be false" % field)
        if str(state.get("localization_state") or "").strip() not in ("", "not_localized"):
            issues.append("localization_state must be empty or not_localized")
        for field in (
            "localization_backend_available",
            "runtime_reload_service_available",
            "runtime_save_state_service_available",
            "can_start_mapping",
        ):
            if not bool(state.get(field, False)):
                issues.append("%s must be true" % field)
        for field in ("desired_mode", "current_mode", "runtime_mode"):
            if str(state.get(field) or "").strip().lower() != "localization":
                issues.append("%s must be localization" % field)
        if str(state.get("workflow_state") or "").strip().upper() != "IDLE":
            issues.append("workflow_state must be IDLE")
        if str(state.get("workflow_phase") or "").strip().lower() != "idle":
            issues.append("workflow_phase must be idle")
        for field in ("mission_state", "phase", "public_state"):
            if str(state.get(field) or "").strip().upper() != "IDLE":
                issues.append("%s must be IDLE" % field)
        if str(state.get("executor_state") or "").strip().upper() not in ("", "IDLE"):
            issues.append("executor_state must be empty or IDLE")
        if str(state.get("lifecycle_state") or "").strip().lower() != "steady":
            issues.append("lifecycle_state must be steady")
        localization_blocker = _stage_k_localization_blocker(str(state.get("localization_state") or ""))
        issues.extend(
            _exact_text_set_issues(
                label="slam blocking_reasons",
                observed=list(state.get("blocking_reasons") or []),
                expected=["runtime /map identity unavailable", localization_blocker],
            )
        )
        issues.extend(
            _exact_text_set_issues(
                label="slam warnings",
                observed=list(state.get("warnings") or []),
                expected=sorted(STAGE_K_SLAM_WARNINGS),
            )
        )
    result["ok"] = not issues
    result["issues"] = issues
    result["profile"] = STAGE_K_NEW_VEHICLE_PROFILE
    return result


def _check_stage_k_new_vehicle_dock_calibration(
    resp,
    expected_robot_id: str,
) -> Dict[str, object]:
    state = getattr(resp, "state", None)
    issues = []
    if not bool(getattr(resp, "success", False)):
        issues.append(
            "service success=false message=%s"
            % str(getattr(resp, "message", "") or "")
        )
    if state is None:
        issues.append("missing dock calibration state")
        state_dict = {}
    else:
        state_dict = {
            "robot_id": str(getattr(state, "robot_id", "") or ""),
            "frame_id": str(getattr(state, "frame_id", "") or ""),
            "active_map_name": str(getattr(state, "active_map_name", "") or ""),
            "active_map_id": str(getattr(state, "active_map_id", "") or ""),
            "active_map_md5": str(getattr(state, "active_map_md5", "") or ""),
            "runtime_map_name": str(getattr(state, "runtime_map_name", "") or ""),
            "runtime_map_id": str(getattr(state, "runtime_map_id", "") or ""),
            "runtime_map_md5": str(getattr(state, "runtime_map_md5", "") or ""),
            "runtime_map_ready": bool(getattr(state, "runtime_map_ready", False)),
            "active_map_match": bool(getattr(state, "active_map_match", False)),
            "localization_state": str(getattr(state, "localization_state", "") or ""),
            "localization_valid": bool(getattr(state, "localization_valid", False)),
            "stage1_set": bool(getattr(state, "stage1_set", False)),
            "stage1_x": float(getattr(state, "stage1_x", 0.0) or 0.0),
            "stage1_y": float(getattr(state, "stage1_y", 0.0) or 0.0),
            "stage1_yaw": float(getattr(state, "stage1_yaw", 0.0) or 0.0),
            "stage2_set": bool(getattr(state, "stage2_set", False)),
            "stage2_x": float(getattr(state, "stage2_x", 0.0) or 0.0),
            "stage2_y": float(getattr(state, "stage2_y", 0.0) or 0.0),
            "stage2_yaw": float(getattr(state, "stage2_yaw", 0.0) or 0.0),
            "saved_map_name": str(getattr(state, "saved_map_name", "") or ""),
            "saved_map_id": str(getattr(state, "saved_map_id", "") or ""),
            "saved_map_md5": str(getattr(state, "saved_map_md5", "") or ""),
            "storage_path": str(getattr(state, "storage_path", "") or ""),
            "warnings": [str(item) for item in list(getattr(state, "warnings", []) or [])],
            "stamp": _ros_time_to_dict(getattr(state, "stamp", None) or object()),
        }
        if state_dict["robot_id"] != str(expected_robot_id):
            issues.append(
                "robot_id mismatch expected=%s observed=%s"
                % (str(expected_robot_id), state_dict["robot_id"] or "-")
            )
        if state_dict["frame_id"] != "map":
            issues.append("frame_id must be map")
        if state_dict["storage_path"] != STAGE_K_COMMERCIAL_STORAGE_PATHS["dock_calibration_path"]:
            issues.append(
                "storage_path mismatch expected=%s observed=%s"
                % (
                    STAGE_K_COMMERCIAL_STORAGE_PATHS["dock_calibration_path"],
                    state_dict["storage_path"] or "-",
                )
            )
        for field in (
            "active_map_name",
            "active_map_id",
            "active_map_md5",
            "runtime_map_name",
            "runtime_map_id",
            "runtime_map_md5",
            "saved_map_name",
            "saved_map_id",
            "saved_map_md5",
        ):
            if state_dict[field].strip():
                issues.append("%s must be empty" % field)
        for field in (
            "runtime_map_ready",
            "active_map_match",
            "localization_valid",
            "stage1_set",
            "stage2_set",
        ):
            if bool(state_dict[field]):
                issues.append("%s must be false" % field)
        if state_dict["localization_state"].strip() not in ("", "not_localized"):
            issues.append("localization_state must be empty or not_localized")
        for field in (
            "stage1_x",
            "stage1_y",
            "stage1_yaw",
            "stage2_x",
            "stage2_y",
            "stage2_yaw",
        ):
            if state_dict[field] != 0.0:
                issues.append("%s must be zero while unset" % field)
    return {
        "name": "dock_calibration_status",
        "ok": not issues,
        "issues": issues,
        "profile": STAGE_K_NEW_VEHICLE_PROFILE,
        "response": {
            "success": bool(getattr(resp, "success", False)),
            "message": str(getattr(resp, "message", "") or ""),
            "state": state_dict,
        },
    }


def _check_stage_k_new_vehicle_readiness(resp, localization_state: str) -> Dict[str, object]:
    result = _check_readiness(resp, ignored_warnings=[])
    readiness = dict((result.get("response") or {}).get("readiness") or {})
    issues = []
    if not bool((result.get("response") or {}).get("success", False)):
        issues.append(
            "service success=false message=%s"
            % str((result.get("response") or {}).get("message", "") or "")
        )
    if not readiness:
        issues.append("missing readiness payload")
    else:
        if bool(readiness.get("overall_ready", False)):
            issues.append("overall_ready must remain false")
        if bool(readiness.get("can_start_task", False)):
            issues.append("can_start_task must remain false")
        if int(readiness.get("task_id", 0) or 0) != 0:
            issues.append("task_id must be 0")
        for field in (
            "task_name",
            "task_map_name",
            "task_map_revision_id",
            "task_zone_id",
            "task_plan_profile",
            "active_map_name",
            "active_map_revision_id",
            "active_map_id",
            "active_map_md5",
            "runtime_map_name",
            "runtime_map_revision_id",
            "runtime_map_id",
            "runtime_map_md5",
            "manual_assist_map_name",
            "manual_assist_map_revision_id",
            "manual_assist_retry_action",
            "manual_assist_guidance",
        ):
            if str(readiness.get(field) or "").strip():
                issues.append("%s must be empty" % field)
        if bool(readiness.get("manual_assist_required", False)):
            issues.append("manual_assist_required must be false")
        if bool(readiness.get("battery_valid", False)):
            issues.append("battery_valid must be false while no-action hardware bridges are disabled")
        for field in ("mission_state", "phase", "public_state", "dock_supply_state"):
            if str(readiness.get(field) or "").strip().upper() not in ("", "IDLE"):
                issues.append("%s must be empty or IDLE" % field)
        if str(readiness.get("executor_state") or "").strip().upper() not in ("", "IDLE"):
            issues.append("executor_state must be empty or IDLE")
        checks = [dict(item or {}) for item in list(readiness.get("checks") or [])]
        check_keys = [str(item.get("key") or "") for item in checks]
        if len(check_keys) != len(set(check_keys)):
            issues.append("readiness checks contains duplicate keys")
        observed_checks = {str(item.get("key") or ""): item for item in checks}
        expected_keys = set(STAGE_K_READINESS_CHECKS)
        observed_keys = set(observed_checks)
        if observed_keys != expected_keys:
            issues.append(
                "readiness checks mismatch missing=%s extra=%s"
                % (sorted(expected_keys - observed_keys), sorted(observed_keys - expected_keys))
            )
        for key in sorted(expected_keys & observed_keys):
            expected_level, expected_ok = STAGE_K_READINESS_CHECKS[key]
            observed_level = str(observed_checks[key].get("level") or "").upper()
            allowed_levels = (
                set(expected_level)
                if isinstance(expected_level, tuple)
                else {str(expected_level)}
            )
            if observed_level not in allowed_levels:
                issues.append(
                    "readiness check %s level=%s expected=%s"
                    % (key, observed_level or "-", sorted(allowed_levels))
                )
            if bool(observed_checks[key].get("ok", False)) != bool(expected_ok):
                issues.append(
                    "readiness check %s ok=%s expected=%s"
                    % (key, bool(observed_checks[key].get("ok", False)), bool(expected_ok))
                )
        expected_blockers = {
            "no current active map selected",
            "runtime /map identity unavailable",
            _stage_k_localization_blocker(localization_state),
        }
        issues.extend(
            _exact_text_set_issues(
                label="readiness blocking_reasons",
                observed=list(readiness.get("blocking_reasons") or []),
                expected=sorted(expected_blockers),
            )
        )
        warnings = [str(item) for item in list(readiness.get("warnings") or [])]
        warning_set = set(warnings)
        if len(warnings) != len(warning_set):
            issues.append("readiness warnings contains duplicate entries")
        missing = STAGE_K_REQUIRED_READINESS_WARNINGS - warning_set
        extra = warning_set - STAGE_K_REQUIRED_READINESS_WARNINGS - STAGE_K_OPTIONAL_READINESS_WARNINGS
        if missing or extra:
            issues.append(
                "readiness warnings mismatch missing=%s extra=%s"
                % (sorted(missing), sorted(extra))
            )
    result["ok"] = not issues
    result["issues"] = issues
    result["profile"] = STAGE_K_NEW_VEHICLE_PROFILE
    return result


def run_read_checks(
    client: BackendRuntimeSmokeClient,
    task_id: int,
    ignored_warnings: Sequence[str],
    robot_id: str,
    profile: str = TASK_READY_PROFILE,
) -> List[Dict[str, object]]:
    checks = []
    refresh_map_identity = profile != STAGE_K_NEW_VEHICLE_PROFILE
    slam_response = client.get_slam_status(
        robot_id=robot_id,
        refresh_map_identity=refresh_map_identity,
    )
    odometry_response = client.get_odometry_status(robot_id=robot_id)
    readiness_response = client.get_system_readiness(
        task_id=task_id,
        refresh_map_identity=refresh_map_identity,
    )
    if profile == STAGE_K_NEW_VEHICLE_PROFILE:
        dock_calibration_response = client.get_dock_calibration_status(robot_id=robot_id)
        slam = _check_stage_k_new_vehicle_slam(slam_response, expected_robot_id=robot_id)
        odometry = _check_odometry(odometry_response, ignored_warnings=[], expected_robot_id=robot_id)
        odometry_state = dict((odometry.get("response") or {}).get("state") or {})
        if odometry_state and not bool(odometry_state.get("connected", False)):
            odometry.setdefault("issues", []).append("connected=false")
            odometry["ok"] = False
        if odometry_state and str(odometry_state.get("error_code") or "").strip():
            odometry.setdefault("issues", []).append("error_code must be empty")
            odometry["ok"] = False
        expected_odometry_identity = {
            "odom_source": "odom_stream",
            "odom_topic": "/odom",
            "raw_odom_topic": "/odom_raw",
            "imu_topic": "/imu",
            "validation_mode": "odom_stream",
        }
        for field, expected in expected_odometry_identity.items():
            observed = str(odometry_state.get(field) or "")
            if observed != expected:
                odometry.setdefault("issues", []).append(
                    "%s mismatch expected=%s observed=%s"
                    % (field, expected, observed or "-")
                )
                odometry["ok"] = False
        slam_state = dict((slam.get("response") or {}).get("state") or {})
        readiness = _check_stage_k_new_vehicle_readiness(
            readiness_response,
            localization_state=str(slam_state.get("localization_state") or ""),
        )
        dock_calibration = _check_stage_k_new_vehicle_dock_calibration(
            dock_calibration_response,
            expected_robot_id=robot_id,
        )
        checks.extend([slam, odometry, readiness, dock_calibration])
    else:
        checks.append(_check_slam(slam_response, ignored_warnings, expected_robot_id=robot_id))
        checks.append(_check_odometry(odometry_response, ignored_warnings, expected_robot_id=robot_id))
        checks.append(_check_readiness(readiness_response, ignored_warnings))
    return checks


def _task_state_to_dict(msg) -> Dict[str, object]:
    if msg is None:
        return {}
    return {
        "mission_state": str(getattr(msg, "mission_state", "") or ""),
        "phase": str(getattr(msg, "phase", "") or ""),
        "public_state": str(getattr(msg, "public_state", "") or ""),
        "executor_state": str(getattr(msg, "executor_state", "") or ""),
        "active_job_id": str(getattr(msg, "active_job_id", "") or ""),
        "run_id": str(getattr(msg, "run_id", "") or ""),
        "progress_pct": float(getattr(msg, "progress_pct", 0.0) or 0.0),
        "plan_id": str(getattr(msg, "plan_id", "") or ""),
        "map_id": str(getattr(msg, "map_id", "") or ""),
        "map_md5": str(getattr(msg, "map_md5", "") or ""),
        "zone_id": str(getattr(msg, "zone_id", "") or ""),
        "stamp": _ros_time_to_dict(getattr(msg, "stamp", None) or object()),
    }


def _load_task_cycle_db_baseline(ops_db_path: str, robot_id: str) -> Dict[str, object]:
    baseline = {
        "run_ids": frozenset(),
        "runtime": {},
    }
    conn = sqlite3.connect(str(ops_db_path))
    try:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        baseline["run_ids"] = frozenset(
            str(row["run_id"] or "").strip()
            for row in cur.execute("SELECT run_id FROM mission_runs").fetchall()
            if str(row["run_id"] or "").strip()
        )
        runtime_row = cur.execute(
            """
            SELECT robot_id, active_run_id, active_job_id, mission_state, phase,
                   public_state, executor_state, updated_ts
            FROM robot_runtime_state
            WHERE robot_id = ?
            """,
            (str(robot_id or "").strip(),),
        ).fetchone()
        if runtime_row is not None:
            baseline["runtime"] = {str(key): runtime_row[key] for key in runtime_row.keys()}
    finally:
        conn.close()
    return baseline


def _load_task_cycle_db_state(ops_db_path: str, run_id: str, robot_id: str) -> Dict[str, object]:
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
                SELECT run_id, job_id, state, reason, map_name, map_revision_id,
                       plan_profile_name, plan_id, map_id, map_md5, created_ts,
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
            SELECT robot_id, active_run_id, active_job_id, mission_state, phase,
                   public_state, executor_state, updated_ts
            FROM robot_runtime_state
            WHERE robot_id = ?
            """,
            (str(robot_id or "").strip(),),
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
        and not str(runtime_row.get("active_job_id") or "").strip()
        and str(runtime_row.get("mission_state") or "IDLE").strip().upper() == "IDLE"
        and str(runtime_row.get("phase") or "IDLE").strip().upper() == "IDLE"
        and str(runtime_row.get("public_state") or "IDLE").strip().upper() == "IDLE"
        and str(runtime_row.get("executor_state") or "IDLE").strip().upper() == "IDLE"
    )


def _task_cycle_run_binding_issues(
    *,
    task_id: int,
    run_id: str,
    run_row: Dict[str, object],
    start_requested_ts: float,
    baseline_run_ids: Iterable[str],
    expected_scope: Optional[Dict[str, object]] = None,
    require_resolved_plan: bool = False,
) -> List[str]:
    issues: List[str] = []
    expected_run_id = str(run_id or "").strip()
    row = dict(run_row or {})
    baseline_ids = {str(item or "").strip() for item in baseline_run_ids if str(item or "").strip()}
    if not expected_run_id:
        issues.append("task run_id was never observed")
        return issues
    if expected_run_id in baseline_ids:
        issues.append("task run_id is not new: %s existed before START" % expected_run_id)
    if not row:
        issues.append("mission_run row missing for run_id=%s" % expected_run_id)
        return issues
    observed_run_id = str(row.get("run_id") or "").strip()
    if observed_run_id != expected_run_id:
        issues.append(
            "mission_run run_id mismatch expected=%s observed=%s"
            % (expected_run_id, observed_run_id or "-")
        )
    expected_job_id = str(int(task_id or 0))
    observed_job_id = str(row.get("job_id") or "").strip()
    if observed_job_id != expected_job_id:
        issues.append(
            "mission_run job_id mismatch expected=%s observed=%s"
            % (expected_job_id, observed_job_id or "-")
        )

    requested_at = float(start_requested_ts or 0.0)
    timestamp_values = {}
    for field in ("created_ts", "start_ts", "updated_ts"):
        try:
            value = float(row.get(field) or 0.0)
        except (TypeError, ValueError):
            value = 0.0
        timestamp_values[field] = value
        if requested_at > 0.0 and value < requested_at:
            issues.append(
                "mission_run %s predates START request: value=%.6f start_requested_ts=%.6f"
                % (field, value, requested_at)
            )
    if timestamp_values["start_ts"] < timestamp_values["created_ts"]:
        issues.append("mission_run start_ts predates created_ts")
    if timestamp_values["updated_ts"] < timestamp_values["start_ts"]:
        issues.append("mission_run updated_ts predates start_ts")
    try:
        end_ts = float(row.get("end_ts") or 0.0)
    except (TypeError, ValueError):
        end_ts = 0.0
    if end_ts > 0.0 and end_ts < timestamp_values["start_ts"]:
        issues.append("mission_run end_ts predates start_ts")

    scope = dict(expected_scope or {})
    for row_field, scope_field in (
        ("map_name", "map_name"),
        ("map_revision_id", "map_revision_id"),
        ("plan_profile_name", "plan_profile_name"),
    ):
        expected_value = str(scope.get(scope_field) or "").strip()
        if not expected_value:
            continue
        observed_value = str(row.get(row_field) or "").strip()
        if observed_value != expected_value:
            issues.append(
                "mission_run %s mismatch expected=%s observed=%s"
                % (row_field, expected_value, observed_value or "-")
            )
    if require_resolved_plan and not str(row.get("plan_id") or "").strip():
        issues.append("mission_run plan_id is empty")
    return issues


def _task_cycle_issues(
    *,
    task_id: int,
    running_seen: bool,
    run_id: str,
    final_run_row: Dict[str, object],
    final_runtime_row: Dict[str, object],
    post_readiness: Dict[str, object],
    expected_robot_id: str = "",
    start_requested_ts: float = 0.0,
    baseline_run_ids: Iterable[str] = (),
    expected_scope: Optional[Dict[str, object]] = None,
) -> List[str]:
    issues: List[str] = []
    if int(task_id or 0) <= 0:
        issues.append("task_id must be > 0 for task cycle")
    if not running_seen:
        issues.append("task never reached running state")
    issues.extend(
        _task_cycle_run_binding_issues(
            task_id=int(task_id or 0),
            run_id=str(run_id or ""),
            run_row=dict(final_run_row or {}),
            start_requested_ts=float(start_requested_ts or 0.0),
            baseline_run_ids=baseline_run_ids,
            expected_scope=expected_scope,
            require_resolved_plan=True,
        )
    )
    run_state = str((final_run_row or {}).get("state") or "").strip().upper()
    if final_run_row:
        if run_state != "DONE":
            issues.append("mission_run terminal state=%s" % (run_state or "UNKNOWN"))
    if final_runtime_row:
        expected_identity = str(expected_robot_id or "").strip()
        observed_identity = str(final_runtime_row.get("robot_id") or "").strip()
        if expected_identity and observed_identity != expected_identity:
            issues.append(
                "robot_runtime_state identity mismatch expected=%s observed=%s"
                % (expected_identity, observed_identity or "-")
            )
        if float(start_requested_ts or 0.0) > 0.0:
            try:
                runtime_updated_ts = float(final_runtime_row.get("updated_ts") or 0.0)
            except (TypeError, ValueError):
                runtime_updated_ts = 0.0
            if runtime_updated_ts < float(start_requested_ts):
                issues.append(
                    "robot_runtime_state updated_ts predates START request: "
                    "value=%.6f start_requested_ts=%.6f"
                    % (runtime_updated_ts, float(start_requested_ts))
                )
        if not _runtime_row_is_idle(final_runtime_row):
            issues.append(
                "runtime not idle after task: mission=%s phase=%s public=%s executor=%s "
                "active_run_id=%s active_job_id=%s"
                % (
                    str(final_runtime_row.get("mission_state") or ""),
                    str(final_runtime_row.get("phase") or ""),
                    str(final_runtime_row.get("public_state") or ""),
                    str(final_runtime_row.get("executor_state") or ""),
                    str(final_runtime_row.get("active_run_id") or ""),
                    str(final_runtime_row.get("active_job_id") or ""),
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
        "baseline": {},
        "candidate_rejections": [],
    }
    pre_readiness = _check_readiness(client.get_system_readiness(task_id=args.task_id), args.ignore_warning)
    result["pre_readiness"] = pre_readiness
    if not bool(pre_readiness.get("ok", False)):
        result["issues"].append("pre task readiness failed")
        return result

    readiness_state = dict((pre_readiness.get("response") or {}).get("readiness") or {})
    expected_scope = {
        "map_name": str(readiness_state.get("task_map_name") or "").strip(),
        "map_revision_id": str(readiness_state.get("task_map_revision_id") or "").strip(),
        "plan_profile_name": str(readiness_state.get("task_plan_profile") or "").strip(),
    }
    readiness_task_id = int(readiness_state.get("task_id") or 0)
    if readiness_task_id != int(args.task_id):
        result["issues"].append(
            "pre readiness task_id mismatch expected=%d observed=%d"
            % (int(args.task_id), readiness_task_id)
        )
        return result

    try:
        baseline_msg = client.wait_for_task_state(timeout_s=max(0.1, float(args.poll_interval)))
    except Exception:
        baseline_msg = None
    baseline_task_state = _task_state_to_dict(baseline_msg)
    db_baseline = _load_task_cycle_db_baseline(args.ops_db_path, args.robot_id)
    baseline_run_ids = set(db_baseline.get("run_ids") or ())
    baseline_task_run_id = str(baseline_task_state.get("run_id") or "").strip()
    if baseline_task_run_id:
        baseline_run_ids.add(baseline_task_run_id)
    baseline_runtime = dict(db_baseline.get("runtime") or {})
    result["baseline"] = {
        "task_state": baseline_task_state,
        "existing_run_count": len(baseline_run_ids),
        "runtime": baseline_runtime,
        "expected_scope": expected_scope,
    }
    if not baseline_runtime:
        result["issues"].append(
            "pre START robot_runtime_state row missing for robot_id=%s" % str(args.robot_id)
        )
        return result
    if str(baseline_runtime.get("robot_id") or "").strip() != str(args.robot_id):
        result["issues"].append(
            "pre START robot_runtime_state identity mismatch expected=%s observed=%s"
            % (str(args.robot_id), str(baseline_runtime.get("robot_id") or "-") or "-")
        )
        return result
    if not _runtime_row_is_idle(baseline_runtime):
        result["issues"].append("pre START robot_runtime_state is not idle")
        return result

    start_requested_ts = time.time()
    result["start_requested_ts"] = float(start_requested_ts)
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
    run_conflicts: List[str] = []
    candidate_rejections: Dict[str, List[str]] = {}
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
        observed_run_id = str(task_state.get("run_id") or "").strip()
        if observed_run_id:
            if observed_run_id in baseline_run_ids:
                candidate_rejections[observed_run_id] = [
                    "run_id existed before START"
                ]
            elif run_id and observed_run_id != run_id:
                conflict = "task_state run_id changed after binding expected=%s observed=%s" % (
                    run_id,
                    observed_run_id,
                )
                if conflict not in run_conflicts:
                    run_conflicts.append(conflict)
            elif not run_id:
                candidate_db_state = _load_task_cycle_db_state(
                    args.ops_db_path,
                    observed_run_id,
                    args.robot_id,
                )
                candidate_issues = _task_cycle_run_binding_issues(
                    task_id=int(args.task_id),
                    run_id=observed_run_id,
                    run_row=dict(candidate_db_state.get("run") or {}),
                    start_requested_ts=float(start_requested_ts),
                    baseline_run_ids=baseline_run_ids,
                    expected_scope=expected_scope,
                    require_resolved_plan=False,
                )
                if candidate_issues:
                    candidate_rejections[observed_run_id] = candidate_issues
                else:
                    run_id = observed_run_id
                    final_db_state = candidate_db_state
        mission_state = str(task_state.get("mission_state") or "IDLE").strip().upper()
        public_state = str(task_state.get("public_state") or "IDLE").strip().upper()
        if observed_run_id == run_id and (
            mission_state not in ("", "IDLE") or public_state not in ("", "IDLE")
        ):
            running_seen = True
        if run_id and str(args.ops_db_path or "").strip():
            final_db_state = _load_task_cycle_db_state(args.ops_db_path, run_id, args.robot_id)
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
        final_db_state = _load_task_cycle_db_state(args.ops_db_path, run_id, args.robot_id)

    result["running_seen"] = bool(running_seen)
    result["run_id"] = str(run_id or "")
    result["post_readiness"] = post_readiness
    result["final_db_state"] = final_db_state
    result["candidate_rejections"] = [
        {"run_id": candidate_id, "issues": list(candidate_rejections[candidate_id])}
        for candidate_id in sorted(candidate_rejections)
    ]
    result["issues"] = _task_cycle_issues(
        task_id=int(args.task_id),
        running_seen=bool(running_seen),
        run_id=str(run_id or ""),
        final_run_row=dict(final_db_state.get("run") or {}),
        final_runtime_row=dict(final_db_state.get("runtime") or {}),
        post_readiness=post_readiness,
        expected_robot_id=str(args.robot_id),
        start_requested_ts=float(start_requested_ts),
        baseline_run_ids=baseline_run_ids,
        expected_scope=expected_scope,
    )
    result["issues"].extend(run_conflicts)
    if not run_id:
        for rejection in result["candidate_rejections"]:
            for issue in list(rejection.get("issues") or []):
                result["issues"].append(
                    "rejected task_state run_id=%s: %s"
                    % (str(rejection.get("run_id") or "-"), str(issue))
                )
    result["ok"] = not result["issues"]
    return result


def _action_job_contract_options(action_name: str, args, description: str) -> Dict[str, object]:
    action_name = str(action_name or "")
    requested_map_name = str(getattr(args, "map_name", "") or "")
    requested_revision_id = str(getattr(args, "map_revision_id", "") or "")
    if action_name == "stop_mapping":
        expected_map_name: Optional[str] = ""
        expected_revision_id: Optional[str] = ""
    elif action_name == "save_mapping":
        expected_map_name = str(getattr(args, "save_map_name", "") or "")
        expected_revision_id = requested_revision_id or None
    elif requested_map_name:
        expected_map_name = requested_map_name
        expected_revision_id = requested_revision_id or None
    elif requested_revision_id:
        expected_map_name = None
        expected_revision_id = requested_revision_id
    elif action_name in {
        "prepare_for_task",
        "relocalize",
        "switch_map_and_localize",
        "verify_map_revision",
        "activate_map_revision",
    }:
        expected_map_name = None
        expected_revision_id = None
    else:
        expected_map_name = ""
        expected_revision_id = requested_revision_id or None
    return {
        "expected_operation_name": action_name,
        "expected_map_name": expected_map_name,
        "expected_map_revision_id": expected_revision_id,
        "expected_description": str(description),
        "allow_resolved_revision_change": action_name == "save_mapping",
        "require_resolved_revision_id": action_name == "save_mapping",
    }


def wait_for_job(
    client: BackendRuntimeSmokeClient,
    job_id: str,
    robot_id: str,
    timeout_s: float,
    poll_interval_s: float,
    expected_operation_name: str = "",
    expected_map_name: Optional[str] = None,
    expected_map_revision_id: Optional[str] = None,
    expected_description: Optional[str] = None,
    allow_resolved_revision_change: bool = False,
    require_resolved_revision_id: bool = False,
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
        if entry["error_code"]:
            return {
                "ok": False,
                "terminal_state": "contract_mismatch",
                "response": entry,
                "observations": observations,
                "issues": [
                    "found job response requires empty error_code observed=%s"
                    % entry["error_code"]
                ],
            }
        terminal, state = job_terminal_snapshot(getattr(resp, "job", None))
        contract_issues = job_contract_issues(
            entry["job"],
            expected_job_id=job_id,
            expected_robot_id=robot_id,
            expected_operation_name=expected_operation_name,
            expected_map_name=expected_map_name,
            expected_map_revision_id=expected_map_revision_id,
            expected_description=expected_description,
            check_resolved_scope=terminal,
            allow_resolved_revision_change=allow_resolved_revision_change,
            require_resolved_revision_id=bool(
                require_resolved_revision_id and state == "succeeded"
            ),
        )
        if contract_issues:
            mismatch_state = (
                "identity_mismatch"
                if any("job identity mismatch" in issue for issue in contract_issues)
                else "contract_mismatch"
            )
            return {
                "ok": False,
                "terminal_state": mismatch_state,
                "response": entry,
                "observations": observations,
                "issues": contract_issues,
            }
        if terminal:
            consistency_issues = job_terminal_consistency_issues(getattr(resp, "job", None))
            return {
                "ok": bool(
                    job_succeeded(getattr(resp, "job", None))
                    and not consistency_issues
                ),
                "terminal_state": state,
                "response": entry,
                "observations": observations,
                "issues": consistency_issues,
            }
        time.sleep(float(poll_interval_s))
    return {
        "ok": False,
        "terminal_state": "timeout",
        "response": observations[-1] if observations else {},
        "observations": observations,
    }


def run_actions(client: BackendRuntimeSmokeClient, args) -> List[Dict[str, object]]:
    action_names = list(args.actions or [])
    if action_names:
        raise ValueError(
            "write actions are forbidden in backend runtime smoke; use the explicit "
            "run_revision_workflow_acceptance.py commercial workflow"
        )
    results = []
    description_prefix = str(args.description_prefix or "backend_runtime_smoke").strip() or "backend_runtime_smoke"
    for action_name in action_names:
        description = "%s:%s" % (description_prefix, action_name)
        contract_options = _action_job_contract_options(action_name, args, description)
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
            break
        job_id = action_result["submit"]["job_id"]
        if not job_id:
            action_result["issues"].append("missing job_id on accepted submit")
            results.append(action_result)
            break
        submit_job = dict(action_result["submit"].get("job") or {})
        expected_operation = int(SLAM_ACTION_OPERATION_CODES.get(action_name, 0) or 0)
        if int(action_result["submit"].get("operation", 0) or 0) != expected_operation:
            action_result["issues"].append(
                "submit operation mismatch expected=%s/%s observed=%s"
                % (
                    action_name,
                    expected_operation or "-",
                    int(action_result["submit"].get("operation", 0) or 0) or "-",
                )
            )
        expected_submit_map_name = contract_options.get("expected_map_name")
        if (
            expected_submit_map_name is not None
            and str(action_result["submit"].get("map_name") or "")
            != str(expected_submit_map_name)
        ):
            action_result["issues"].append(
                "submit map_name mismatch expected=%s observed=%s"
                % (
                    str(expected_submit_map_name or "-"),
                    str(action_result["submit"].get("map_name") or "-") or "-",
                )
            )
        action_result["issues"].extend(
            accepted_submit_consistency_issues(
                action_result["submit"],
                expected_job_id=job_id,
                expected_robot_id=args.robot_id,
                expected_operation_name=str(contract_options["expected_operation_name"]),
                expected_map_name=contract_options["expected_map_name"],
                expected_map_revision_id=contract_options["expected_map_revision_id"],
                expected_description=str(contract_options["expected_description"]),
            )
        )
        if action_result["issues"]:
            results.append(action_result)
            break
        wait_contract_options = dict(contract_options)
        if wait_contract_options.get("expected_map_name") is None:
            submit_requested_map_name = str(submit_job.get("requested_map_name") or "")
            if submit_requested_map_name:
                wait_contract_options["expected_map_name"] = submit_requested_map_name
        if wait_contract_options.get("expected_map_revision_id") is None:
            wait_contract_options["expected_map_revision_id"] = str(
                submit_job.get("requested_map_revision_id") or ""
            )
        waited = wait_for_job(
            client=client,
            job_id=job_id,
            robot_id=args.robot_id,
            timeout_s=args.job_timeout,
            poll_interval_s=args.poll_interval,
            **wait_contract_options,
        )
        action_result["job"] = waited
        if not waited["ok"]:
            action_result["issues"].extend(
                str(issue) for issue in list(waited.get("issues") or []) if str(issue)
            )
            action_result["issues"].append("job terminal_state=%s" % str(waited.get("terminal_state") or ""))
        action_result["ok"] = not action_result["issues"]
        results.append(action_result)
        if not action_result["ok"]:
            break
    return results


def _check_active_map_persistent_asset(
    client: BackendRuntimeSmokeClient,
    revision_scope: Dict[str, object],
) -> Dict[str, object]:
    active = dict((revision_scope or {}).get("active") or {})
    expected_map_name = str(active.get("map_name") or "")
    expected_revision_id = str(active.get("revision_id") or "")
    issues = []
    response_payload = {
        "expected_map_name": expected_map_name,
        "expected_map_revision_id": expected_revision_id,
        "success": False,
        "message": "",
        "asset": {},
        "latest_head": _revision_scope_slot(source="map_server.get"),
    }
    if not expected_map_name:
        issues.append("active map_name is empty")
    if not expected_revision_id:
        issues.append("active map_revision_id is empty")
    if issues:
        return {
            "name": "active_map_persistent_asset",
            "ok": False,
            "issues": issues,
            "response": response_payload,
        }

    try:
        resp = client.get_map_view(expected_map_name, expected_revision_id)
    except Exception as exc:
        issues.append("map_server.get failed: %s" % str(exc))
        response_payload["latest_head"] = _revision_scope_slot(
            source="map_server.get_failed"
        )
        return {
            "name": "active_map_persistent_asset",
            "ok": False,
            "issues": issues,
            "response": response_payload,
        }

    success = bool(getattr(resp, "success", False))
    message = str(getattr(resp, "message", "") or "")
    response_payload["success"] = success
    response_payload["message"] = message
    if not success:
        issues.append("map_server.get success=false message=%s" % message)

    map_msg = getattr(resp, "map", None)
    if map_msg is None:
        issues.append("map_server.get missing map payload")
    else:
        asset = {
            "map_name": str(getattr(map_msg, "map_name", "") or ""),
            "map_revision_id": str(
                getattr(map_msg, "map_revision_id", "") or ""
            ),
            "lifecycle_status": str(
                getattr(map_msg, "lifecycle_status", "") or ""
            ),
            "verification_status": str(
                getattr(map_msg, "verification_status", "") or ""
            ),
            "is_active": bool(getattr(map_msg, "is_active", False)),
            "is_latest_head": bool(getattr(map_msg, "is_latest_head", False)),
            "has_newer_head_revision": bool(
                getattr(map_msg, "has_newer_head_revision", False)
            ),
            "active_revision_id": str(
                getattr(map_msg, "active_revision_id", "") or ""
            ),
            "latest_head_revision_id": str(
                getattr(map_msg, "latest_head_revision_id", "") or ""
            ),
            "latest_head_lifecycle_status": str(
                getattr(map_msg, "latest_head_lifecycle_status", "") or ""
            ),
            "latest_head_verification_status": str(
                getattr(map_msg, "latest_head_verification_status", "") or ""
            ),
        }
        response_payload["asset"] = asset
        response_payload["latest_head"] = _latest_head_scope_from_map_msg(map_msg)
        exact_values = (
            ("map_name", expected_map_name),
            ("map_revision_id", expected_revision_id),
            ("lifecycle_status", "available"),
            ("verification_status", "verified"),
            ("active_revision_id", expected_revision_id),
            ("latest_head_revision_id", expected_revision_id),
            ("latest_head_lifecycle_status", "available"),
            ("latest_head_verification_status", "verified"),
        )
        for field_name, expected_value in exact_values:
            observed_value = str(asset.get(field_name) or "")
            if observed_value != expected_value:
                issues.append(
                    "%s mismatch expected=%s observed=%s"
                    % (field_name, expected_value, observed_value or "-")
                )
        if not bool(asset.get("is_active", False)):
            issues.append("is_active=false")
        if not bool(asset.get("is_latest_head", False)):
            issues.append("is_latest_head=false")
        if bool(asset.get("has_newer_head_revision", False)):
            issues.append("has_newer_head_revision=true")

    return {
        "name": "active_map_persistent_asset",
        "ok": not issues,
        "issues": issues,
        "response": response_payload,
    }


def _lookup_latest_head_scope(client: BackendRuntimeSmokeClient, revision_scope: Dict[str, object]) -> Dict[str, object]:
    active = dict((revision_scope or {}).get("active") or {})
    active_map_name = str(active.get("map_name") or "")
    active_revision_id = str(active.get("revision_id") or "")
    if not active_map_name or not active_revision_id:
        return _revision_scope_slot(source="map_server.get")
    try:
        resp = client.get_map_view(active_map_name, active_revision_id)
    except Exception:
        return _revision_scope_slot(source="map_server.get_failed")
    if not bool(getattr(resp, "success", False)):
        return _revision_scope_slot(source="map_server.get_failed")
    return _latest_head_scope_from_map_msg(getattr(resp, "map", None))


def _validate_stage_k_new_vehicle_args(args) -> None:
    require_explicit_commercial_robot_id(getattr(args, "robot_id", ""))
    if int(getattr(args, "task_id", 0) or 0) != 0:
        raise ValueError("stage_k_new_vehicle_no_map requires --task-id 0")
    if list(getattr(args, "actions", []) or []):
        raise ValueError("stage_k_new_vehicle_no_map forbids --actions")
    if bool(getattr(args, "run_task_cycle", False)):
        raise ValueError("stage_k_new_vehicle_no_map forbids --run-task-cycle")
    if list(getattr(args, "ignore_warning", []) or []):
        raise ValueError("stage_k_new_vehicle_no_map forbids custom --ignore-warning")
    for field in ("map_name", "map_revision_id", "save_map_name"):
        if str(getattr(args, field, "") or "").strip():
            raise ValueError("stage_k_new_vehicle_no_map requires --%s to be empty" % field.replace("_", "-"))
    for field in (
        "set_active",
        "has_initial_pose",
        "include_unfinished_submaps",
        "set_active_on_save",
        "switch_to_localization_after_save",
        "relocalize_after_switch",
    ):
        if bool(getattr(args, field, False)):
            raise ValueError("stage_k_new_vehicle_no_map forbids --%s" % field.replace("_", "-"))
    for field, expected_path in STAGE_K_COMMERCIAL_STORAGE_PATHS.items():
        value = str(getattr(args, field, "") or "").strip()
        if not value or not value.startswith("/"):
            raise ValueError("stage_k_new_vehicle_no_map requires absolute --%s" % field.replace("_", "-"))
        if value != expected_path:
            raise ValueError(
                "stage_k_new_vehicle_no_map requires --%s %s"
                % (field.replace("_", "-"), expected_path)
            )


def _commissioning_state_check(args, name: str) -> Dict[str, object]:
    try:
        snapshot = build_new_vehicle_commissioning_snapshot(
            plan_db_path=str(args.plan_db_path),
            ops_db_path=str(args.ops_db_path),
            maps_root=str(args.maps_root),
            dock_calibration_path=str(args.dock_calibration_path),
            auto_charge_state_path=str(args.auto_charge_state_path),
            auto_charge_event_log_path=str(args.auto_charge_event_log_path),
            robot_id=str(args.robot_id),
        )
        return {
            "name": name,
            "ok": True,
            "issues": [],
            "response": {"snapshot": snapshot},
        }
    except (CommissioningStateError, OSError, sqlite3.Error, ValueError) as exc:
        return {
            "name": name,
            "ok": False,
            "issues": [str(exc)],
            "response": {"snapshot": {}},
        }


def build_report(args) -> Dict[str, object]:
    profile = str(getattr(args, "profile", TASK_READY_PROFILE) or TASK_READY_PROFILE)
    robot_id = require_explicit_commercial_robot_id(getattr(args, "robot_id", ""))
    if bool(getattr(args, "run_task_cycle", False)):
        raise ValueError(
            "--run-task-cycle is prohibited by the commercial backend runtime smoke entry; "
            "task motion acceptance requires a dedicated fail-safe harness"
        )
    checks = []
    commissioning_pre = None
    if profile == STAGE_K_NEW_VEHICLE_PROFILE:
        _validate_stage_k_new_vehicle_args(args)
        commissioning_pre = _commissioning_state_check(args, "new_vehicle_state_pre")
        checks.append(commissioning_pre)
        if not commissioning_pre["ok"]:
            issue = "%s: %s" % (
                commissioning_pre["name"],
                "; ".join(commissioning_pre.get("issues") or []),
            )
            return {
                "profile": profile,
                "robot_id": robot_id,
                "task_id": int(args.task_id),
                "actions": [],
                "task_cycle": {},
                "checks": checks,
                "revision_scope": {},
                "summary": {"ok": False, "issues": [issue]},
            }

    requested_actions = list(getattr(args, "actions", []) or [])
    requested_task_cycle = bool(getattr(args, "run_task_cycle", False))
    actions = []
    task_cycle = None
    client = None
    runtime_inspection_failed = False
    topology_blocked = False
    active_asset_check = None
    try:
        client = BackendRuntimeSmokeClient()
        client.wait_for_services(timeout_s=args.service_timeout)
        if profile == STAGE_K_NEW_VEHICLE_PROFILE:
            topology_identity = _stage_k_ros_topology_identity_check(client, robot_id)
            checks.append(topology_identity)
            topology_blocked = not bool(topology_identity.get("ok", False))
        else:
            topology_identity = _commercial_ros_topology_identity_check(
                client,
                robot_id,
                check_name=(
                    "commercial_write_ros_topology_identity"
                    if requested_actions or requested_task_cycle
                    else "commercial_read_ros_topology_identity"
                ),
                require_empty_mapping_session=True,
            )
            checks.append(topology_identity)
            topology_blocked = not bool(topology_identity.get("ok", False))
        if not topology_blocked:
            checks.extend(
                run_read_checks(
                    client,
                    task_id=args.task_id,
                    ignored_warnings=list(getattr(args, "ignore_warning", []) or []),
                    robot_id=robot_id,
                    profile=profile,
                )
            )
            identity_issues = runtime_identity_issues(checks, robot_id)
            if profile != STAGE_K_NEW_VEHICLE_PROFILE:
                active_asset_check = _check_active_map_persistent_asset(
                    client,
                    build_runtime_revision_scope(checks),
                )
                checks.append(active_asset_check)
            if requested_actions or requested_task_cycle:
                checks.append(
                    {
                        "name": "runtime_identity_gate",
                        "ok": not identity_issues,
                        "issues": identity_issues,
                        "response": {"expected_robot_id": robot_id},
                    }
                )
            runtime_gate_ok = all(bool(item.get("ok", False)) for item in checks)
            if requested_actions and not identity_issues and runtime_gate_ok:
                actions = run_actions(client, args)
            if (
                requested_task_cycle
                and not identity_issues
                and runtime_gate_ok
                and (not requested_actions or all(bool(item.get("ok", False)) for item in actions))
            ):
                task_cycle = run_task_cycle(client, args)
    except Exception as exc:
        if profile != STAGE_K_NEW_VEHICLE_PROFILE:
            raise
        runtime_inspection_failed = True
        checks.append(
            {
                "name": "stage_k_runtime_inspection",
                "ok": False,
                "issues": ["runtime inspection failed: %s" % str(exc)],
                "response": {},
            }
        )
    finally:
        if profile == STAGE_K_NEW_VEHICLE_PROFILE:
            commissioning_post = _commissioning_state_check(args, "new_vehicle_state_post")
            if commissioning_post["ok"] and commissioning_pre is not None:
                pre_snapshot = dict((commissioning_pre.get("response") or {}).get("snapshot") or {})
                post_snapshot = dict((commissioning_post.get("response") or {}).get("snapshot") or {})
                if pre_snapshot != post_snapshot:
                    commissioning_post["ok"] = False
                    commissioning_post["issues"] = ["new-vehicle semantic state changed during the gate"]
            checks.append(commissioning_post)
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
    if active_asset_check is not None:
        latest_head = dict(
            (active_asset_check.get("response") or {}).get("latest_head") or {}
        )
    elif profile == STAGE_K_NEW_VEHICLE_PROFILE and not runtime_inspection_failed:
        latest_head = _revision_scope_slot(source="stage_k_new_vehicle_no_map")
    else:
        latest_head = _revision_scope_slot(source="runtime_inspection_unavailable")
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
        "profile": profile,
        "robot_id": robot_id,
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
    print("Profile: %s" % str(report.get("profile") or TASK_READY_PROFILE))
    print("Robot: %s" % str(report.get("robot_id") or "-"))
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
        description="Run the strictly read-only commercial backend smoke for SLAM, odometry, readiness, topology, and storage identity."
    )
    parser.add_argument("--profile", choices=SUPPORTED_PROFILES, default=TASK_READY_PROFILE)
    parser.add_argument("--task-id", type=int, default=0, help="task id for readiness baseline")
    parser.add_argument("--service-timeout", type=float, default=10.0, help="wait time for core services")
    parser.add_argument("--job-timeout", type=float, default=30.0, help="wait time for async slam jobs")
    parser.add_argument("--poll-interval", type=float, default=1.0, help="poll interval for get_slam_job")
    parser.add_argument("--robot-id", default="", help="explicit commercial robot_id for all runtime reads and actions")
    parser.add_argument("--map-name", default="", help="optional map_name for actions")
    parser.add_argument("--map-revision-id", default="", help="optional map_revision_id for revision-scoped actions")
    parser.add_argument("--frame-id", default="map", help="frame_id for initial pose actions")
    parser.add_argument("--save-map-name", default="", help="save_map_name for save_mapping")
    parser.add_argument("--description-prefix", default="backend_runtime_smoke", help="action description prefix")
    parser.add_argument(
        "--actions",
        default="",
        help=(
            "prohibited; retained only to return an explicit fail-closed error "
            "(use run_revision_workflow_acceptance.py)"
        ),
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
        default=None,
        help="warning text to ignore; may be passed multiple times",
    )
    parser.add_argument(
        "--run-task-cycle",
        action="store_true",
        help="reserved unsafe task-cycle probe; rejected by the commercial smoke entry",
    )
    parser.add_argument("--task-timeout", type=float, default=300.0, help="timeout for task cycle acceptance")
    parser.add_argument("--plan-db-path", default="", help="planning.db path for the new-vehicle gate")
    parser.add_argument("--ops-db-path", default="", help="operations.db path for task cycle or new-vehicle checks")
    parser.add_argument("--maps-root", default="", help="map root for the new-vehicle gate")
    parser.add_argument("--dock-calibration-path", default="", help="dock calibration path for the new-vehicle gate")
    parser.add_argument("--auto-charge-state-path", default="", help="auto-charge state path for the new-vehicle gate")
    parser.add_argument(
        "--auto-charge-event-log-path",
        default="",
        help="auto-charge event log path for the new-vehicle gate",
    )
    parser.add_argument("--json", action="store_true", help="print json report")
    parser.add_argument("--text", action="store_true", help="print text report")
    return parser


def validate_args(args) -> None:
    args.actions = parse_actions(args.actions)
    args.robot_id = require_explicit_commercial_robot_id(args.robot_id)
    if args.ignore_warning is None:
        args.ignore_warning = (
            []
            if str(args.profile or TASK_READY_PROFILE) == STAGE_K_NEW_VEHICLE_PROFILE
            else list(DEFAULT_TASK_READY_IGNORED_WARNINGS)
        )
    if str(args.profile or TASK_READY_PROFILE) == STAGE_K_NEW_VEHICLE_PROFILE:
        _validate_stage_k_new_vehicle_args(args)
    if args.actions:
        raise ValueError(
            "--actions is forbidden in backend runtime smoke; use the explicit "
            "run_revision_workflow_acceptance.py commercial workflow"
        )
    action_only_values = {
        "map_name": str(args.map_name or ""),
        "map_revision_id": str(args.map_revision_id or ""),
        "save_map_name": str(args.save_map_name or ""),
        "set_active": bool(args.set_active),
        "has_initial_pose": bool(args.has_initial_pose),
        "initial_pose_x": float(args.initial_pose_x or 0.0),
        "initial_pose_y": float(args.initial_pose_y or 0.0),
        "initial_pose_yaw": float(args.initial_pose_yaw or 0.0),
        "include_unfinished_submaps": bool(args.include_unfinished_submaps),
        "set_active_on_save": bool(args.set_active_on_save),
        "switch_to_localization_after_save": bool(
            args.switch_to_localization_after_save
        ),
        "relocalize_after_switch": bool(args.relocalize_after_switch),
    }
    supplied_action_only = [
        name for name, value in action_only_values.items() if value not in ("", False, 0.0)
    ]
    if str(args.frame_id or "map") != "map":
        supplied_action_only.append("frame_id")
    if str(args.description_prefix or "backend_runtime_smoke") != "backend_runtime_smoke":
        supplied_action_only.append("description_prefix")
    if supplied_action_only:
        raise ValueError(
            "write-action-only options are forbidden in backend runtime smoke: %s"
            % ",".join(sorted(supplied_action_only))
        )
    if args.actions and bool(args.run_task_cycle):
        raise ValueError("--actions and --run-task-cycle cannot be used together")
    if "save_mapping" in args.actions and not str(args.save_map_name or "").strip():
        raise ValueError("--save-map-name is required when actions include save_mapping")
    if args.relocalize_after_switch and not args.switch_to_localization_after_save:
        raise ValueError("--relocalize-after-switch requires --switch-to-localization-after-save")
    revision_scoped_actions = {
        "prepare_for_task",
        "switch_map_and_localize",
        "relocalize",
        "verify_map_revision",
        "activate_map_revision",
    }
    if revision_scoped_actions.intersection(set(args.actions or [])):
        if not str(args.map_name or "").strip() or not str(args.map_revision_id or "").strip():
            raise ValueError(
                "--map-name and --map-revision-id are required for revision-scoped actions"
            )
    if bool(args.run_task_cycle):
        raise ValueError(
            "--run-task-cycle is prohibited by the commercial backend runtime smoke entry; "
            "task motion acceptance requires a dedicated fail-safe harness"
        )


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

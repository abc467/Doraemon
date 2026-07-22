#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import os
import stat
import sys
import tempfile
from types import SimpleNamespace
from typing import Dict, Iterable, List, Optional, Sequence

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
if THIS_DIR not in sys.path:
    sys.path.insert(0, THIS_DIR)

from run_backend_runtime_smoke import (
    BackendRuntimeSmokeClient,
    SLAM_ACTION_OPERATION_CODES,
    STAGE_K_EXPECTED_NODE_PRIVATE_PARAMS,
    STAGE_K_EXPECTED_SERVICE_PROVIDERS,
    _commercial_ros_topology_identity_check,
    _lookup_latest_head_scope,
    _check_odometry,
    _check_readiness,
    _check_slam,
    _format_revision_scope,
    _job_to_dict,
    accepted_submit_consistency_issues,
    build_revision_scope,
    job_contract_issues,
    job_succeeded,
    job_terminal_consistency_issues,
    require_explicit_commercial_robot_id,
    runtime_identity_issues,
    wait_for_job,
)


SUPPORTED_PROFILES = (
    "verify_revision",
    "activate_revision",
    "activate_revision_prepare_for_task",
    "mapping_save_candidate",
    "mapping_save_verify_activate",
)

WRITE_PROFILES = set(SUPPORTED_PROFILES)
CHECKPOINT_VERSION = 2
RESUMABLE_PHASE = "paused_after_start_mapping"


def _state_from_check(check_result: Dict[str, object], field_name: str) -> Dict[str, object]:
    return dict((check_result.get("response") or {}).get(field_name) or {})


def capture_snapshot(
    client: BackendRuntimeSmokeClient,
    *,
    robot_id: str,
    task_id: int,
    ignored_warnings: Sequence[str],
) -> Dict[str, object]:
    slam = _check_slam(
        client.get_slam_status(robot_id=robot_id),
        ignored_warnings,
        expected_robot_id=robot_id,
    )
    odometry = _check_odometry(
        client.get_odometry_status(robot_id=robot_id),
        ignored_warnings,
        expected_robot_id=robot_id,
    )
    readiness = _check_readiness(client.get_system_readiness(task_id=task_id), ignored_warnings)
    slam_state = _state_from_check(slam, "state")
    readiness_state = _state_from_check(readiness, "readiness")
    latest_head = _lookup_latest_head_scope(
        client,
        build_revision_scope(slam_state=slam_state, readiness_state=readiness_state),
    )
    return {
        "slam": slam_state,
        "odometry": _state_from_check(odometry, "state"),
        "readiness": readiness_state,
        "revision_scope": build_revision_scope(
            slam_state=slam_state,
            readiness_state=readiness_state,
            latest_head=latest_head,
        ),
        "checks": {
            "slam": slam,
            "odometry": odometry,
            "readiness": readiness,
        },
    }


def require_snapshot_identity(
    snapshot: Dict[str, object],
    *,
    robot_id: str,
    label: str,
) -> None:
    checks = dict(snapshot.get("checks") or {})
    issues = runtime_identity_issues(list(checks.values()), robot_id)
    if issues:
        raise ValueError("%s identity gate failed: %s" % (label, "; ".join(issues)))


def require_write_topology_identity(client: BackendRuntimeSmokeClient, args) -> Dict[str, object]:
    check = _commercial_ros_topology_identity_check(
        client,
        args.robot_id,
        check_name="revision_write_ros_topology_identity",
        require_empty_mapping_session=not bool(
            str(getattr(args, "resume_from_checkpoint", "") or "").strip()
        ),
    )
    if not bool(check.get("ok", False)):
        raise ValueError(
            "revision write topology gate failed: %s"
            % "; ".join(str(item) for item in list(check.get("issues") or []))
        )
    return check


def require_mapping_session_active(snapshot: Dict[str, object], *, label: str) -> None:
    slam = dict(snapshot.get("slam") or {})
    current_mode = str(slam.get("current_mode") or slam.get("runtime_mode") or "").lower()
    if current_mode != "mapping" or not bool(slam.get("mapping_session_active", False)):
        raise ValueError(
            "%s requires a live mapping session (mode=%s active=%s)"
            % (label, current_mode or "-", bool(slam.get("mapping_session_active", False)))
        )


def _submit_and_wait(
    client: BackendRuntimeSmokeClient,
    *,
    action_name: str,
    robot_id: str,
    map_name: str,
    map_revision_id: str,
    frame_id: str,
    save_map_name: str,
    description: str,
    job_timeout: float,
    poll_interval: float,
) -> Dict[str, object]:
    submit = client.submit_action(
        operation_name=action_name,
        robot_id=robot_id,
        map_name=map_name,
        map_revision_id=map_revision_id,
        frame_id=frame_id,
        save_map_name=save_map_name,
        description=description,
        set_active=False,
        has_initial_pose=False,
        initial_pose_x=0.0,
        initial_pose_y=0.0,
        initial_pose_yaw=0.0,
        include_unfinished_submaps=True,
        set_active_on_save=False,
        switch_to_localization_after_save=False,
        relocalize_after_switch=False,
    )
    result = {
        "name": str(action_name or ""),
        "submit": {
            "accepted": bool(getattr(submit, "accepted", False)),
            "message": str(getattr(submit, "message", "") or ""),
            "error_code": str(getattr(submit, "error_code", "") or ""),
            "job_id": str(getattr(submit, "job_id", "") or ""),
            "map_name": str(getattr(submit, "map_name", "") or ""),
            "operation": int(getattr(submit, "operation", 0) or 0),
            "job": _job_to_dict(getattr(submit, "job", None)),
        },
        "ok": False,
        "issues": [],
        "job": {},
    }
    if not result["submit"]["accepted"]:
        result["issues"].append(
            "submit rejected error_code=%s message=%s"
            % (result["submit"]["error_code"], result["submit"]["message"])
        )
        return result
    if not result["submit"]["job_id"]:
        result["issues"].append("accepted submit missing job_id")
        return result
    expected_operation = int(SLAM_ACTION_OPERATION_CODES.get(action_name, 0) or 0)
    if int(result["submit"].get("operation", 0) or 0) != expected_operation:
        result["issues"].append(
            "submit operation mismatch expected=%s/%s observed=%s"
            % (
                action_name,
                expected_operation or "-",
                int(result["submit"].get("operation", 0) or 0) or "-",
            )
        )
    if action_name == "stop_mapping":
        expected_map_name = ""
        expected_revision_id = ""
    elif action_name == "save_mapping":
        expected_map_name = str(save_map_name or "")
        expected_revision_id = str(map_revision_id or "") or None
    else:
        expected_map_name = str(map_name or "")
        expected_revision_id = str(map_revision_id or "")
    if str(result["submit"].get("map_name") or "") != str(expected_map_name or ""):
        result["issues"].append(
            "submit map_name mismatch expected=%s observed=%s"
            % (
                str(expected_map_name or "-") or "-",
                str(result["submit"].get("map_name") or "-") or "-",
            )
        )
    result["issues"].extend(
        accepted_submit_consistency_issues(
            result["submit"],
            expected_job_id=result["submit"]["job_id"],
            expected_robot_id=robot_id,
            expected_operation_name=action_name,
            expected_map_name=expected_map_name,
            expected_map_revision_id=expected_revision_id,
            expected_description=description,
        )
    )
    if result["issues"]:
        return result
    waited = wait_for_job(
        client=client,
        job_id=result["submit"]["job_id"],
        robot_id=robot_id,
        timeout_s=job_timeout,
        poll_interval_s=poll_interval,
        expected_operation_name=action_name,
        expected_map_name=expected_map_name,
        expected_map_revision_id=expected_revision_id,
        expected_description=description,
        allow_resolved_revision_change=action_name == "save_mapping",
        require_resolved_revision_id=action_name == "save_mapping",
    )
    result["job"] = waited
    if not bool(waited.get("ok", False)):
        result["issues"].extend(
            str(issue) for issue in list(waited.get("issues") or []) if str(issue)
        )
        result["issues"].append("job terminal_state=%s" % str(waited.get("terminal_state") or ""))
    result["ok"] = not result["issues"]
    return result


def action_target_revision_id(action_result: Dict[str, object], explicit_revision_id: str = "") -> str:
    explicit_revision_id = str(explicit_revision_id or "").strip()
    if explicit_revision_id:
        return explicit_revision_id
    job_payload = dict(((action_result.get("job") or {}).get("response") or {}).get("job") or {})
    resolved_revision_id = str(job_payload.get("resolved_map_revision_id") or "").strip()
    if resolved_revision_id:
        return resolved_revision_id
    requested_revision_id = str(job_payload.get("requested_map_revision_id") or "").strip()
    if requested_revision_id:
        return requested_revision_id
    return ""


def action_target_map_name(action_result: Dict[str, object], explicit_map_name: str = "") -> str:
    explicit_map_name = str(explicit_map_name or "").strip()
    if explicit_map_name:
        return explicit_map_name
    job_payload = dict(((action_result.get("job") or {}).get("response") or {}).get("job") or {})
    resolved_map_name = str(job_payload.get("resolved_map_name") or "").strip()
    if resolved_map_name:
        return resolved_map_name
    requested_map_name = str(job_payload.get("requested_map_name") or "").strip()
    if requested_map_name:
        return requested_map_name
    return str((action_result.get("submit") or {}).get("map_name") or "").strip()


def _pending_switch_clear(snapshot: Dict[str, object]) -> bool:
    slam = dict(snapshot.get("slam") or {})
    return not any(
        (
            str(slam.get("pending_map_name") or "").strip(),
            str(slam.get("pending_map_revision_id") or "").strip(),
            str(slam.get("pending_map_switch_status") or "").strip(),
        )
    )


def _append_pending_switch_issue(issues: List[str], snapshot: Dict[str, object], *, label: str) -> None:
    slam = dict(snapshot.get("slam") or {})
    if _pending_switch_clear(snapshot):
        return
    issues.append(
        "%s pending switch not cleared map=%s revision=%s status=%s"
        % (
            label,
            str(slam.get("pending_map_name") or "-"),
            str(slam.get("pending_map_revision_id") or "-"),
            str(slam.get("pending_map_switch_status") or "-"),
        )
    )


def _capture_map_revision_view(
    client: BackendRuntimeSmokeClient,
    *,
    map_name: str,
    map_revision_id: str,
) -> Dict[str, object]:
    try:
        response = client.get_map_view(map_name, map_revision_id)
    except Exception as exc:
        return {
            "success": False,
            "message": "map revision lookup failed: %s" % str(exc),
            "map": {},
        }
    map_msg = getattr(response, "map", None)
    return {
        "success": bool(getattr(response, "success", False)),
        "message": str(getattr(response, "message", "") or ""),
        "map": {
            "map_name": str(getattr(map_msg, "map_name", "") or ""),
            "map_revision_id": str(getattr(map_msg, "map_revision_id", "") or ""),
            "lifecycle_status": str(getattr(map_msg, "lifecycle_status", "") or ""),
            "verification_status": str(getattr(map_msg, "verification_status", "") or ""),
            "enabled": bool(getattr(map_msg, "enabled", False)),
            "is_active": bool(getattr(map_msg, "is_active", False)),
        },
    }


def _map_revision_view_issues(
    view: Dict[str, object],
    *,
    label: str,
    expected_map_name: str,
    expected_revision_id: str,
    expected_lifecycle_status: str,
    expected_verification_status: str,
    require_inactive: bool = False,
    expected_is_active: Optional[bool] = None,
) -> List[str]:
    issues = []
    response = dict(view or {})
    asset = dict(response.get("map") or {})
    if not bool(response.get("success", False)):
        issues.append(
            "%s map revision lookup failed: %s"
            % (label, str(response.get("message") or "unknown error"))
        )
        return issues
    expected_values = {
        "map_name": str(expected_map_name),
        "map_revision_id": str(expected_revision_id),
        "lifecycle_status": str(expected_lifecycle_status),
        "verification_status": str(expected_verification_status),
    }
    for field_name, expected_value in expected_values.items():
        observed_value = str(asset.get(field_name) or "")
        if observed_value != expected_value:
            issues.append(
                "%s %s mismatch expected=%s actual=%s"
                % (label, field_name, expected_value or "-", observed_value or "-")
            )
    if require_inactive and bool(asset.get("is_active", False)):
        issues.append("%s candidate is active unexpectedly" % label)
    if (
        expected_is_active is not None
        and bool(asset.get("is_active", False)) != bool(expected_is_active)
    ):
        issues.append(
            "%s is_active mismatch expected=%s actual=%s"
            % (
                label,
                str(bool(expected_is_active)).lower(),
                str(bool(asset.get("is_active", False))).lower(),
            )
        )
    return issues


def _append_exact_localization_mode_issues(
    issues: List[str],
    snapshot: Dict[str, object],
    *,
    label: str,
) -> None:
    slam = dict(snapshot.get("slam") or {})
    for field_name in ("desired_mode", "current_mode", "runtime_mode"):
        observed = str(slam.get(field_name) or "").strip().lower()
        if observed != "localization":
            issues.append(
                "%s %s mismatch expected=localization actual=%s"
                % (label, field_name, observed or "-")
            )


def verify_profile_issues(
    pre_snapshot: Dict[str, object],
    post_snapshot: Dict[str, object],
    action_result: Dict[str, object],
    *,
    target_map_name: str,
    target_revision_id: str,
) -> List[str]:
    issues = list(action_result.get("issues") or [])
    pre_slam = dict(pre_snapshot.get("slam") or {})
    post_slam = dict(post_snapshot.get("slam") or {})
    target_map_name = str(target_map_name or "")
    target_revision_id = str(target_revision_id or "").strip()
    if not target_map_name:
        issues.append("verify target map_name is empty")
    if not target_revision_id:
        issues.append("verify target revision_id is empty")
    _append_exact_localization_mode_issues(issues, post_snapshot, label="post verify")
    for field_name in (
        "active_map_name",
        "active_map_revision_id",
        "active_map_id",
        "active_map_md5",
    ):
        before = str(pre_slam.get(field_name) or "")
        after = str(post_slam.get(field_name) or "")
        if after != before:
            issues.append(
                "verify changed %s unexpectedly before=%s after=%s"
                % (field_name, before or "-", after or "-")
            )
    if not bool(post_slam.get("localization_valid", False)):
        issues.append("post verify localization_valid=false")
    pre_active_name = str(pre_slam.get("active_map_name") or "")
    pre_active_revision = str(pre_slam.get("active_map_revision_id") or "")
    runtime_name = str(post_slam.get("runtime_map_name") or "")
    runtime_revision = str(post_slam.get("runtime_map_revision_id") or "")
    if pre_active_name or pre_active_revision:
        if runtime_name != pre_active_name:
            issues.append(
                "post verify runtime map was not restored expected=%s actual=%s"
                % (pre_active_name or "-", runtime_name or "-")
            )
        if runtime_revision != pre_active_revision:
            issues.append(
                "post verify runtime revision was not restored expected=%s actual=%s"
                % (pre_active_revision or "-", runtime_revision or "-")
            )
        if not bool(post_slam.get("runtime_map_match", False)):
            issues.append("post verify runtime_map_match=false")
    else:
        if runtime_name != target_map_name:
            issues.append(
                "post verify runtime map mismatch expected=%s actual=%s"
                % (target_map_name or "-", runtime_name or "-")
            )
        if runtime_revision != target_revision_id:
            issues.append(
                "post verify runtime revision mismatch expected=%s actual=%s"
                % (target_revision_id or "-", runtime_revision or "-")
            )
        if bool(post_slam.get("runtime_map_match", False)):
            issues.append("post verify runtime_map_match=true without an active map")
    _append_pending_switch_issue(issues, post_snapshot, label="post verify")
    return issues


def activate_profile_issues(
    post_snapshot: Dict[str, object],
    action_result: Dict[str, object],
    *,
    target_map_name: str,
    target_revision_id: str,
) -> List[str]:
    issues = list(action_result.get("issues") or [])
    post_slam = dict(post_snapshot.get("slam") or {})
    post_readiness = dict(post_snapshot.get("readiness") or {})
    target_map_name = str(target_map_name or "")
    target_revision_id = str(target_revision_id or "").strip()
    if not target_map_name:
        issues.append("activate target map_name is empty")
    if not target_revision_id:
        issues.append("activate target revision_id is empty")
        return issues
    _append_exact_localization_mode_issues(issues, post_snapshot, label="post activate")
    active_name = str(post_slam.get("active_map_name") or "")
    active_revision = str(post_slam.get("active_map_revision_id") or "").strip()
    runtime_name = str(post_slam.get("runtime_map_name") or "")
    runtime_revision = str(post_slam.get("runtime_map_revision_id") or "").strip()
    readiness_active_name = str(post_readiness.get("active_map_name") or "")
    readiness_active_revision = str(post_readiness.get("active_map_revision_id") or "").strip()
    if active_name != target_map_name:
        issues.append(
            "post activate active map mismatch expected=%s actual=%s"
            % (target_map_name or "-", active_name or "-")
        )
    if active_revision != target_revision_id:
        issues.append(
            "post activate active revision mismatch expected=%s actual=%s"
            % (target_revision_id, active_revision or "-")
        )
    if runtime_name != target_map_name:
        issues.append(
            "post activate runtime map mismatch expected=%s actual=%s"
            % (target_map_name or "-", runtime_name or "-")
        )
    if runtime_revision != target_revision_id:
        issues.append(
            "post activate runtime revision mismatch expected=%s actual=%s"
            % (target_revision_id, runtime_revision or "-")
        )
    if readiness_active_name != target_map_name:
        issues.append(
            "post activate readiness active map mismatch expected=%s actual=%s"
            % (target_map_name or "-", readiness_active_name or "-")
        )
    if readiness_active_revision != target_revision_id:
        issues.append(
            "post activate readiness active revision mismatch expected=%s actual=%s"
            % (target_revision_id, readiness_active_revision or "-")
        )
    if not bool(post_slam.get("runtime_map_match", False)):
        issues.append("post activate runtime_map_match=false")
    if not bool(post_slam.get("localization_valid", False)):
        issues.append("post activate localization_valid=false")
    _append_pending_switch_issue(issues, post_snapshot, label="post activate")
    return issues


def prepare_for_task_profile_issues(
    post_snapshot: Dict[str, object],
    action_result: Dict[str, object],
    *,
    target_map_name: str,
    target_revision_id: str,
    target_task_id: int,
) -> List[str]:
    issues = list(action_result.get("issues") or [])
    post_slam = dict(post_snapshot.get("slam") or {})
    post_readiness = dict(post_snapshot.get("readiness") or {})
    target_map_name = str(target_map_name or "")
    target_revision_id = str(target_revision_id or "").strip()
    if not target_map_name:
        issues.append("prepare_for_task target map_name is empty")
    if not target_revision_id:
        issues.append("prepare_for_task target revision_id is empty")
        return issues
    if int(target_task_id) <= 0:
        issues.append("prepare_for_task target task_id must be greater than zero")
    _append_exact_localization_mode_issues(
        issues,
        post_snapshot,
        label="post prepare_for_task",
    )
    active_name = str(post_slam.get("active_map_name") or "")
    active_revision = str(post_slam.get("active_map_revision_id") or "").strip()
    runtime_name = str(post_slam.get("runtime_map_name") or "")
    runtime_revision = str(post_slam.get("runtime_map_revision_id") or "").strip()
    readiness_active_name = str(post_readiness.get("active_map_name") or "")
    readiness_active_revision = str(post_readiness.get("active_map_revision_id") or "").strip()
    if active_name != target_map_name:
        issues.append(
            "post prepare_for_task active map mismatch expected=%s actual=%s"
            % (target_map_name or "-", active_name or "-")
        )
    if active_revision != target_revision_id:
        issues.append(
            "post prepare_for_task active revision mismatch expected=%s actual=%s"
            % (target_revision_id, active_revision or "-")
        )
    if runtime_name != target_map_name:
        issues.append(
            "post prepare_for_task runtime map mismatch expected=%s actual=%s"
            % (target_map_name or "-", runtime_name or "-")
        )
    if runtime_revision != target_revision_id:
        issues.append(
            "post prepare_for_task runtime revision mismatch expected=%s actual=%s"
            % (target_revision_id, runtime_revision or "-")
        )
    if readiness_active_name != target_map_name:
        issues.append(
            "post prepare_for_task readiness active map mismatch expected=%s actual=%s"
            % (target_map_name or "-", readiness_active_name or "-")
        )
    if readiness_active_revision != target_revision_id:
        issues.append(
            "post prepare_for_task readiness active revision mismatch expected=%s actual=%s"
            % (target_revision_id, readiness_active_revision or "-")
        )
    readiness_task_id = int(post_readiness.get("task_id", 0) or 0)
    readiness_task_map_name = str(post_readiness.get("task_map_name") or "")
    readiness_task_revision = str(post_readiness.get("task_map_revision_id") or "")
    if readiness_task_id != int(target_task_id):
        issues.append(
            "post prepare_for_task readiness task_id mismatch expected=%s actual=%s"
            % (int(target_task_id), readiness_task_id)
        )
    if readiness_task_map_name != target_map_name:
        issues.append(
            "post prepare_for_task readiness task map mismatch expected=%s actual=%s"
            % (target_map_name or "-", readiness_task_map_name or "-")
        )
    if readiness_task_revision != target_revision_id:
        issues.append(
            "post prepare_for_task readiness task revision mismatch expected=%s actual=%s"
            % (target_revision_id, readiness_task_revision or "-")
        )
    if not bool(post_slam.get("runtime_map_match", False)):
        issues.append("post prepare_for_task runtime_map_match=false")
    if not bool(post_slam.get("localization_valid", False)):
        issues.append("post prepare_for_task localization_valid=false")
    if not bool(post_slam.get("task_ready", False)):
        issues.append("post prepare_for_task task_ready=false")
    if not bool(post_readiness.get("overall_ready", False)):
        issues.append("post prepare_for_task overall_ready=false")
    if not bool(post_readiness.get("can_start_task", False)):
        issues.append("post prepare_for_task can_start_task=false")
    for state_label, state, field_names in (
        ("slam", post_slam, ("blocking_reasons", "warnings")),
        (
            "odometry",
            dict(post_snapshot.get("odometry") or {}),
            ("blocking_reasons", "warnings"),
        ),
        ("readiness", post_readiness, ("blocking_reasons", "warnings")),
    ):
        for field_name in field_names:
            observed = [str(item) for item in list(state.get(field_name) or []) if str(item)]
            if observed:
                issues.append(
                    "post prepare_for_task %s %s must be empty actual=%s"
                    % (state_label, field_name, ", ".join(observed))
                )
    checks = dict(post_snapshot.get("checks") or {})
    for check_name in ("slam", "odometry", "readiness"):
        check = checks.get(check_name)
        if not isinstance(check, dict):
            issues.append("post prepare_for_task %s check is missing" % check_name)
            continue
        if check.get("ok") is not True:
            issues.append("post prepare_for_task %s check ok=false" % check_name)
        check_issues = [
            str(item) for item in list(check.get("issues") or []) if str(item)
        ]
        if check_issues:
            issues.append(
                "post prepare_for_task %s check contains issues=%s"
                % (check_name, "; ".join(check_issues))
            )
    _append_pending_switch_issue(issues, post_snapshot, label="post prepare_for_task")
    return issues


def mapping_save_candidate_issues(
    pre_snapshot: Dict[str, object],
    post_snapshot: Dict[str, object],
    *,
    candidate_revision_id: str,
) -> List[str]:
    issues: List[str] = []
    pre_slam = dict(pre_snapshot.get("slam") or {})
    post_slam = dict(post_snapshot.get("slam") or {})
    candidate_revision_id = str(candidate_revision_id or "").strip()
    if not candidate_revision_id:
        issues.append("save_mapping candidate revision_id is empty")
    for field_name in (
        "active_map_name",
        "active_map_revision_id",
        "active_map_id",
        "active_map_md5",
    ):
        before = str(pre_slam.get(field_name) or "")
        after = str(post_slam.get(field_name) or "")
        if after != before:
            issues.append(
                "mapping cycle changed %s unexpectedly before=%s after=%s"
                % (field_name, before or "-", after or "-")
            )
    _append_exact_localization_mode_issues(issues, post_snapshot, label="post stop")
    if bool(post_slam.get("mapping_session_active", False)):
        issues.append("post stop mapping_session_active=true")
    if bool(post_slam.get("localization_valid", False)):
        issues.append("post stop localization_valid=true")
    if bool(post_slam.get("runtime_map_match", False)):
        issues.append("post stop runtime_map_match=true")
    for field_name in (
        "runtime_map_name",
        "runtime_map_revision_id",
        "runtime_map_id",
        "runtime_map_md5",
    ):
        value = str(post_slam.get(field_name) or "")
        if value:
            issues.append("post stop %s must be empty actual=%s" % (field_name, value))
    for field_name in ("active_job_id", "active_job_status", "active_job_phase"):
        value = str(post_slam.get(field_name) or "")
        if value:
            issues.append("post stop %s must be empty actual=%s" % (field_name, value))
    _append_pending_switch_issue(issues, post_snapshot, label="post mapping cycle")
    return issues


def _build_summary_ok(issues: Iterable[str]) -> bool:
    return not [str(item) for item in list(issues or []) if str(item)]


def _mapping_profile_name(*, verify_after_save: bool, prepare_after_activate: bool) -> str:
    if prepare_after_activate:
        raise ValueError(
            "mapping prepare-in-one-chain is unsupported; run "
            "mapping_save_verify_activate, create the revision-bound zone/plan/task, "
            "then run activate_revision_prepare_for_task"
        )
    if verify_after_save:
        return "mapping_save_verify_activate"
    return "mapping_save_candidate"


def _mapping_profile_flags(profile_name: str) -> Dict[str, bool]:
    profile_name = str(profile_name or "")
    if profile_name == "mapping_save_candidate":
        return {"verify_after_save": False, "prepare_after_activate": False}
    if profile_name == "mapping_save_verify_activate":
        return {"verify_after_save": True, "prepare_after_activate": False}
    raise ValueError("profile does not support mapping checkpoint flow: %s" % profile_name)


def _require_checkpoint_parent(path: str) -> tuple:
    requested_path = str(path or "").strip()
    if not requested_path:
        raise ValueError("checkpoint path is empty")
    target_path = os.path.abspath(requested_path)
    parent_dir = os.path.dirname(target_path)
    current_path = os.path.sep
    for component in [item for item in parent_dir.split(os.path.sep) if item]:
        current_path = os.path.join(current_path, component)
        try:
            component_stat = os.lstat(current_path)
        except OSError as exc:
            raise ValueError(
                "checkpoint parent does not exist or is inaccessible: %s: %s"
                % (current_path, str(exc))
            )
        if stat.S_ISLNK(component_stat.st_mode):
            raise ValueError(
                "checkpoint parent path must not contain a symlink: %s" % current_path
            )
        if not stat.S_ISDIR(component_stat.st_mode):
            raise ValueError(
                "checkpoint parent path component is not a directory: %s" % current_path
            )
    return target_path, parent_dir


def _require_secure_checkpoint_file(path: str) -> tuple:
    target_path, parent_dir = _require_checkpoint_parent(path)
    try:
        target_stat = os.lstat(target_path)
    except OSError as exc:
        raise ValueError(
            "checkpoint file does not exist or is inaccessible: %s: %s"
            % (target_path, str(exc))
        )
    if stat.S_ISLNK(target_stat.st_mode):
        raise ValueError("checkpoint file must not be a symlink: %s" % target_path)
    if not stat.S_ISREG(target_stat.st_mode):
        raise ValueError("checkpoint file must be a regular file: %s" % target_path)
    if int(target_stat.st_uid) != int(os.geteuid()):
        raise ValueError(
            "checkpoint file owner mismatch expected_uid=%s actual_uid=%s: %s"
            % (int(os.geteuid()), int(target_stat.st_uid), target_path)
        )
    mode = stat.S_IMODE(target_stat.st_mode)
    if mode & 0o077:
        raise ValueError(
            "checkpoint file permissions are too broad expected no group/other access "
            "actual=%04o: %s" % (mode, target_path)
        )
    return target_path, parent_dir, target_stat


def _require_secure_checkpoint_write_target(path: str) -> tuple:
    target_path, parent_dir = _require_checkpoint_parent(path)
    if os.path.lexists(target_path):
        _require_secure_checkpoint_file(target_path)
    return target_path, parent_dir


def _write_checkpoint(path: str, payload: Dict[str, object]) -> None:
    target_path, parent_dir = _require_secure_checkpoint_write_target(path)
    serialized = json.dumps(
        payload,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    ) + "\n"
    temp_fd = -1
    temp_path = ""
    try:
        temp_fd, temp_path = tempfile.mkstemp(
            prefix=".%s." % (os.path.basename(target_path) or "checkpoint"),
            suffix=".tmp",
            dir=parent_dir,
        )
        os.fchmod(temp_fd, 0o600)
        with os.fdopen(temp_fd, "w", encoding="utf-8") as handle:
            temp_fd = -1
            handle.write(serialized)
            handle.flush()
            os.fsync(handle.fileno())
        if os.path.lexists(target_path):
            _require_secure_checkpoint_file(target_path)
        os.replace(temp_path, target_path)
        temp_path = ""
        directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        directory_flags |= getattr(os, "O_NOFOLLOW", 0)
        directory_fd = os.open(parent_dir, directory_flags)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if temp_fd >= 0:
            os.close(temp_fd)
        if temp_path:
            try:
                os.unlink(temp_path)
            except FileNotFoundError:
                pass


def _load_checkpoint(path: str) -> Dict[str, object]:
    target_path, _parent_dir, expected_stat = _require_secure_checkpoint_file(path)
    open_flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    file_fd = os.open(target_path, open_flags)
    with os.fdopen(file_fd, "r", encoding="utf-8") as handle:
        opened_stat = os.fstat(handle.fileno())
        if (
            int(opened_stat.st_dev) != int(expected_stat.st_dev)
            or int(opened_stat.st_ino) != int(expected_stat.st_ino)
        ):
            raise ValueError("checkpoint file changed while opening: %s" % target_path)
        if not stat.S_ISREG(opened_stat.st_mode):
            raise ValueError("checkpoint file must be a regular file: %s" % target_path)
        if int(opened_stat.st_uid) != int(os.geteuid()):
            raise ValueError(
                "checkpoint file owner mismatch expected_uid=%s actual_uid=%s: %s"
                % (int(os.geteuid()), int(opened_stat.st_uid), target_path)
            )
        if stat.S_IMODE(opened_stat.st_mode) & 0o077:
            raise ValueError(
                "checkpoint file permissions are too broad: %s" % target_path
            )
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError("checkpoint root must be a dictionary in %s" % target_path)
    if (
        type(payload.get("checkpoint_version")) is not int
        or payload.get("checkpoint_version") != CHECKPOINT_VERSION
    ):
        raise ValueError("unsupported checkpoint version in %s" % target_path)
    return dict(payload or {})


def build_checkpoint_report(checkpoint_path: str) -> Dict[str, object]:
    payload = _load_checkpoint(checkpoint_path)
    result = dict(payload.get("result") or {})
    final_revision_scope = dict(result.get("final_revision_scope") or {})
    return {
        "checkpoint_path": str(checkpoint_path or ""),
        "checkpoint_version": int(payload.get("checkpoint_version", 0) or 0),
        "profile": str(payload.get("profile") or ""),
        "phase": str(payload.get("phase") or ""),
        "resumable": str(payload.get("phase") or "") == RESUMABLE_PHASE,
        "save_map_name": str(payload.get("save_map_name") or ""),
        "mapping_session_id": str(payload.get("mapping_session_id") or ""),
        "robot_id": str(payload.get("robot_id") or ""),
        "task_id": int(payload.get("task_id", 0) or 0),
        "frame_id": str(payload.get("frame_id") or ""),
        "description_prefix": str(payload.get("description_prefix") or ""),
        "resume_request": dict(payload.get("resume_request") or {}),
        "result": {
            "ok": bool(result.get("ok", False)),
            "paused": bool(result.get("paused", False)),
            "issues": [str(item) for item in list(result.get("issues") or []) if str(item)],
            "target_revision_id": str(result.get("target_revision_id") or ""),
            "final_revision_scope": final_revision_scope,
        },
    }


def resolve_profile_from_checkpoint(path: str) -> str:
    payload = _load_checkpoint(path)
    profile_name = str(payload.get("profile") or "").strip()
    if profile_name not in SUPPORTED_PROFILES:
        raise ValueError("checkpoint profile is missing or unsupported: %s" % (profile_name or "-"))
    return profile_name


def resolve_effective_profile(args) -> str:
    requested_profile = str(getattr(args, "profile", "") or "").strip()
    if requested_profile:
        return requested_profile
    checkpoint_path = str(getattr(args, "resume_from_checkpoint", "") or "").strip()
    if checkpoint_path:
        return resolve_profile_from_checkpoint(checkpoint_path)
    raise ValueError("--profile is required unless --resume-from-checkpoint is provided")


def _checkpoint_string_field(payload: Dict[str, object], field_name: str) -> str:
    return str((payload or {}).get(field_name) or "").strip()


def _start_mapping_job_payload_type_issues(
    payload: Dict[str, object],
    *,
    label: str,
) -> List[str]:
    issues = []
    for field_name in (
        "job_id",
        "robot_id",
        "operation_name",
        "requested_map_name",
        "resolved_map_name",
        "requested_map_revision_id",
        "resolved_map_revision_id",
        "description",
        "job_state",
        "status",
        "phase",
        "workflow_phase",
        "error_code",
        "message",
        "result_code",
        "result_message",
    ):
        if not isinstance(payload.get(field_name), str):
            issues.append("%s %s must be a string" % (label, field_name))
    if type(payload.get("operation")) is not int:
        issues.append("%s operation must be an integer" % label)
    for field_name in (
        "done",
        "success",
        "result_success",
        "manual_assist_required",
    ):
        if type(payload.get(field_name)) is not bool:
            issues.append("%s %s must be a boolean" % (label, field_name))
    return issues


def _start_mapping_action_contract_issues(
    action: Dict[str, object],
    *,
    robot_id: str,
    description_prefix: str,
    expected_mapping_session_id: str = "",
) -> List[str]:
    issues = []
    if not isinstance(action, dict):
        return ["start_action must be a dictionary"]

    expected_description = "%s:start_mapping" % str(description_prefix or "")
    if not isinstance(action.get("name"), str) or action.get("name") != "start_mapping":
        issues.append(
            "start_action name mismatch expected=start_mapping observed=%s"
            % (str(action.get("name") or "-") or "-")
        )
    if action.get("ok") is not True:
        issues.append("start_action ok must be true")
    if not isinstance(action.get("issues"), list):
        issues.append("start_action issues must be a list")
        action_issues = []
    else:
        action_issues = [str(item) for item in action.get("issues") if str(item)]
    if action_issues:
        issues.append("start_action contains issues=%s" % "; ".join(action_issues))

    submit = action.get("submit")
    if not isinstance(submit, dict):
        issues.append("start_action submit must be a dictionary")
        return issues
    if submit.get("accepted") is not True:
        issues.append("start_action submit accepted must be true")
    if not isinstance(submit.get("job_id"), str):
        issues.append("start_action submit job_id must be a string")
    raw_job_id = str(submit.get("job_id") or "")
    job_id = raw_job_id.strip()
    if not job_id or raw_job_id != job_id:
        issues.append("start_action submit job_id must be nonempty without surrounding whitespace")
    expected_session_id = str(expected_mapping_session_id or "")
    if expected_session_id and job_id != expected_session_id:
        issues.append(
            "mapping_session_id/start job mismatch expected=%s observed=%s"
            % (expected_session_id, job_id or "-")
        )
    expected_operation = int(SLAM_ACTION_OPERATION_CODES["start_mapping"])
    if type(submit.get("operation")) is not int:
        issues.append("start_action submit operation must be an integer")
    elif submit.get("operation") != expected_operation:
        issues.append(
            "start_action submit operation mismatch expected=start_mapping/%s observed=%s"
            % (expected_operation, submit.get("operation") or "-")
        )
    if not isinstance(submit.get("map_name"), str):
        issues.append("start_action submit map_name must be a string")
    elif submit.get("map_name"):
        issues.append("start_action submit map_name must be empty")

    submit_job = submit.get("job")
    if not isinstance(submit_job, dict) or not submit_job:
        issues.append("start_action submit job payload is missing")
    else:
        issues.extend(
            _start_mapping_job_payload_type_issues(
                submit_job,
                label="start_action submit job",
            )
        )
        issues.extend(
            "start_action submit %s" % item
            for item in accepted_submit_consistency_issues(
                submit,
                expected_job_id=job_id,
                expected_robot_id=robot_id,
                expected_operation_name="start_mapping",
                expected_map_name="",
                expected_map_revision_id="",
                expected_description=expected_description,
            )
        )

    waited = action.get("job")
    if not isinstance(waited, dict):
        issues.append("start_action terminal job result must be a dictionary")
        return issues
    if waited.get("ok") is not True:
        issues.append("start_action terminal job ok must be true")
    if str(waited.get("terminal_state") or "").strip().lower() != "succeeded":
        issues.append(
            "start_action terminal_state mismatch expected=succeeded observed=%s"
            % (str(waited.get("terminal_state") or "-") or "-")
        )
    if not isinstance(waited.get("issues"), list):
        issues.append("start_action terminal job issues must be a list")
        waited_issues = []
    else:
        waited_issues = [str(item) for item in waited.get("issues") if str(item)]
    if waited_issues:
        issues.append("start_action terminal job contains issues=%s" % "; ".join(waited_issues))
    response = waited.get("response")
    if not isinstance(response, dict):
        issues.append("start_action terminal response must be a dictionary")
        return issues
    if response.get("found") is not True:
        issues.append("start_action terminal response found must be true")
    terminal_job = response.get("job")
    if not isinstance(terminal_job, dict) or not terminal_job:
        issues.append("start_action terminal job payload is missing")
        return issues
    issues.extend(
        _start_mapping_job_payload_type_issues(
            terminal_job,
            label="start_action terminal job",
        )
    )
    issues.extend(
        "start_action terminal %s" % item
        for item in job_contract_issues(
            terminal_job,
            expected_job_id=job_id,
            expected_robot_id=robot_id,
            expected_operation_name="start_mapping",
            expected_map_name="",
            expected_map_revision_id="",
            expected_description=expected_description,
            check_resolved_scope=True,
        )
    )
    terminal_job_object = SimpleNamespace(
        job_state=str(terminal_job.get("job_state") or ""),
        status=str(terminal_job.get("status") or ""),
        phase=str(terminal_job.get("phase") or ""),
        workflow_phase=str(terminal_job.get("workflow_phase") or ""),
        done=bool(terminal_job.get("done", False)),
        success=bool(terminal_job.get("success", False)),
        result_success=bool(terminal_job.get("result_success", False)),
        error_code=str(terminal_job.get("error_code") or ""),
        message=str(terminal_job.get("message") or ""),
        result_code=str(terminal_job.get("result_code") or ""),
        result_message=str(terminal_job.get("result_message") or ""),
        manual_assist_required=bool(
            terminal_job.get("manual_assist_required", False)
        ),
    )
    issues.extend(
        "start_action terminal %s" % item
        for item in job_terminal_consistency_issues(terminal_job_object)
    )
    if not job_succeeded(terminal_job_object):
        issues.append("start_action terminal job is not a consistent succeeded job")
    return issues


def _require_valid_start_mapping_action(
    action: Dict[str, object],
    *,
    robot_id: str,
    description_prefix: str,
    expected_mapping_session_id: str = "",
    label: str,
) -> str:
    issues = _start_mapping_action_contract_issues(
        action,
        robot_id=robot_id,
        description_prefix=description_prefix,
        expected_mapping_session_id=expected_mapping_session_id,
    )
    if issues:
        raise ValueError("%s failed: %s" % (label, "; ".join(issues)))
    return str((action.get("submit") or {}).get("job_id") or "").strip()


def _require_live_mapping_session_token(
    client: BackendRuntimeSmokeClient,
    *,
    expected_session_id: str,
    label: str,
) -> str:
    expected = str(expected_session_id or "")
    if not expected:
        raise ValueError("%s expected mapping_session_id is empty" % label)
    try:
        observed = str(client.get_mapping_session_id() or "")
    except Exception as exc:
        raise ValueError("%s cannot read live mapping_session_id: %s" % (label, str(exc)))
    if observed != expected:
        raise ValueError(
            "%s mapping_session_id mismatch expected=%s observed=%s"
            % (label, expected, observed or "-")
        )
    return observed


def _capture_cleared_mapping_session(
    client: BackendRuntimeSmokeClient,
    *,
    label: str,
) -> Dict[str, object]:
    try:
        observed = str(client.get_mapping_session_id() or "")
    except Exception as exc:
        return {
            "ok": False,
            "mapping_session_id": "",
            "issues": ["%s cannot read live mapping_session_id: %s" % (label, str(exc))],
        }
    issues = []
    if observed:
        issues.append(
            "%s mapping_session_id must be empty after successful stop_mapping actual=%s"
            % (label, observed)
        )
    return {
        "ok": not issues,
        "mapping_session_id": observed,
        "issues": issues,
    }


def _require_live_start_mapping_job(
    client: BackendRuntimeSmokeClient,
    *,
    args,
    mapping_session_id: str,
    label: str,
) -> Dict[str, object]:
    session_id = _require_live_mapping_session_token(
        client,
        expected_session_id=mapping_session_id,
        label=label,
    )
    try:
        response = client.get_slam_job(job_id=session_id, robot_id=args.robot_id)
    except Exception as exc:
        raise ValueError("%s cannot query start_mapping job: %s" % (label, str(exc)))
    if not bool(getattr(response, "found", False)):
        raise ValueError(
            "%s start_mapping job not found session_id=%s message=%s error_code=%s"
            % (
                label,
                session_id,
                str(getattr(response, "message", "") or "-") or "-",
                str(getattr(response, "error_code", "") or "-") or "-",
            )
        )
    live_job = getattr(response, "job", None)
    live_job_payload = _job_to_dict(live_job)
    issues = job_contract_issues(
        live_job_payload,
        expected_job_id=session_id,
        expected_robot_id=args.robot_id,
        expected_operation_name="start_mapping",
        expected_map_name="",
        expected_map_revision_id="",
        expected_description="%s:start_mapping" % str(args.description_prefix or ""),
        check_resolved_scope=True,
    )
    issues.extend(job_terminal_consistency_issues(live_job))
    if not job_succeeded(live_job):
        issues.append("live start_mapping job is not a consistent succeeded job")
    if issues:
        raise ValueError("%s failed: %s" % (label, "; ".join(issues)))
    return live_job_payload


def _validate_resume_identity(payload: Dict[str, object], args) -> None:
    for field_name in ("robot_id", "frame_id", "description_prefix"):
        if not isinstance((payload or {}).get(field_name), str):
            raise ValueError("checkpoint %s must be a string" % field_name)
    if type((payload or {}).get("task_id")) is not int:
        raise ValueError("checkpoint task_id must be an integer")
    checkpoint_robot_id = _checkpoint_string_field(payload, "robot_id")
    checkpoint_frame_id = _checkpoint_string_field(payload, "frame_id")
    checkpoint_description_prefix = _checkpoint_string_field(payload, "description_prefix")
    checkpoint_task_id = int((payload or {}).get("task_id", 0) or 0)

    requested_robot_id = str(args.robot_id or "").strip()
    requested_frame_id = str(args.frame_id or "").strip()
    requested_description_prefix = str(args.description_prefix or "").strip()
    requested_task_id = int(args.task_id)

    if requested_robot_id != checkpoint_robot_id:
        raise ValueError(
            "robot_id mismatch checkpoint=%s requested=%s"
            % (checkpoint_robot_id, requested_robot_id or "-")
        )
    if requested_frame_id != checkpoint_frame_id:
        raise ValueError(
            "frame_id mismatch checkpoint=%s requested=%s"
            % (checkpoint_frame_id, requested_frame_id or "-")
        )
    if requested_description_prefix != checkpoint_description_prefix:
        raise ValueError(
            "description_prefix mismatch checkpoint=%s requested=%s"
            % (checkpoint_description_prefix, requested_description_prefix or "-")
        )
    if requested_task_id != checkpoint_task_id:
        raise ValueError(
            "task_id mismatch checkpoint=%s requested=%s"
            % (checkpoint_task_id, requested_task_id)
        )


def _mark_checkpoint_resume_started(path: str, payload: Dict[str, object], args) -> Dict[str, object]:
    updated = dict(payload or {})
    updated["phase"] = "resume_in_progress"
    updated["resume_request"] = {
        "profile": str(args.profile or ""),
        "task_id": int(args.task_id),
        "robot_id": str(args.robot_id or ""),
        "frame_id": str(args.frame_id or ""),
        "description_prefix": str(args.description_prefix or ""),
    }
    _write_checkpoint(path, updated)
    return updated


def _finalize_consumed_checkpoint(path: str, payload: Dict[str, object], report: Dict[str, object]) -> None:
    updated = dict(payload or {})
    summary = dict(report.get("summary") or {})
    updated["phase"] = "completed" if bool(summary.get("ok", False)) else "completed_with_issues"
    updated["result"] = {
        "ok": bool(summary.get("ok", False)),
        "paused": bool(summary.get("paused", False)),
        "issues": [str(item) for item in list(summary.get("issues") or []) if str(item)],
        "target_revision_id": str(report.get("target_revision_id") or ""),
        "final_revision_scope": dict((report.get("revision_scope") or {}).get("post") or {}),
    }
    _write_checkpoint(path, updated)


def _paused_mapping_report(
    *,
    profile_name: str,
    checkpoint_path: str,
    pre_snapshot: Dict[str, object],
    mapping_snapshot: Dict[str, object],
    start_action: Dict[str, object],
) -> Dict[str, object]:
    return {
        "profile": str(profile_name or ""),
        "phase": RESUMABLE_PHASE,
        "paused": True,
        "checkpoint_path": str(checkpoint_path or ""),
        "resume_hint": "resume with --resume-from-checkpoint %s" % str(checkpoint_path or ""),
        "target_revision_id": "",
        "pre_snapshot": pre_snapshot,
        "mapping_snapshot": mapping_snapshot,
        "post_snapshot": mapping_snapshot,
        "revision_scope": {
            "pre": dict(pre_snapshot.get("revision_scope") or {}),
            "mapping": dict(mapping_snapshot.get("revision_scope") or {}),
            "post": dict(mapping_snapshot.get("revision_scope") or {}),
        },
        "actions": [start_action],
        "summary": {
            "ok": True,
            "paused": True,
            "issues": [],
        },
    }


def _mapping_checkpoint_payload(
    *,
    profile_name: str,
    args,
    pre_snapshot: Dict[str, object],
    start_action: Dict[str, object],
    mapping_snapshot: Dict[str, object],
    mapping_session_id: str,
) -> Dict[str, object]:
    return {
        "checkpoint_version": CHECKPOINT_VERSION,
        "profile": str(profile_name or ""),
        "phase": RESUMABLE_PHASE,
        "save_map_name": str(args.save_map_name or ""),
        "mapping_session_id": str(mapping_session_id or ""),
        "task_id": int(args.task_id),
        "service_timeout": float(args.service_timeout),
        "job_timeout": float(args.job_timeout),
        "poll_interval": float(args.poll_interval),
        "robot_id": str(args.robot_id or ""),
        "frame_id": str(args.frame_id or ""),
        "description_prefix": str(args.description_prefix or ""),
        "ignore_warning": [str(item) for item in list(args.ignore_warning or [])],
        "pre_snapshot": pre_snapshot,
        "start_action": start_action,
        "mapping_snapshot": mapping_snapshot,
    }


def _load_mapping_checkpoint_context(args) -> Dict[str, object]:
    payload = _load_checkpoint(args.resume_from_checkpoint)
    if not isinstance(payload.get("profile"), str):
        raise ValueError("checkpoint profile must be a string")
    if not isinstance(payload.get("phase"), str):
        raise ValueError("checkpoint phase must be a string")
    profile_name = str(payload.get("profile") or "").strip()
    if profile_name != str(args.profile or "").strip():
        raise ValueError(
            "checkpoint profile mismatch checkpoint=%s requested=%s"
            % (profile_name or "-", str(args.profile or "").strip() or "-")
        )
    if str(payload.get("phase") or "").strip() != RESUMABLE_PHASE:
        raise ValueError("checkpoint is not paused after start_mapping")
    if not isinstance(payload.get("save_map_name"), str):
        raise ValueError("checkpoint save_map_name must be a string")
    save_map_name = str(payload.get("save_map_name") or "").strip()
    if not save_map_name:
        raise ValueError("checkpoint save_map_name is empty")
    if str(args.save_map_name or "").strip() and str(args.save_map_name or "").strip() != save_map_name:
        raise ValueError(
            "save_map_name mismatch checkpoint=%s requested=%s"
            % (save_map_name or "-", str(args.save_map_name or "").strip())
        )
    _validate_resume_identity(payload, args)
    if not isinstance(payload.get("mapping_session_id"), str):
        raise ValueError("checkpoint mapping_session_id must be a string")
    raw_mapping_session_id = str(payload.get("mapping_session_id") or "")
    mapping_session_id = raw_mapping_session_id.strip()
    if not mapping_session_id or raw_mapping_session_id != mapping_session_id:
        raise ValueError(
            "checkpoint mapping_session_id must be nonempty without surrounding whitespace"
        )
    start_action_payload = payload.get("start_action")
    if not isinstance(start_action_payload, dict):
        raise ValueError("checkpoint start_action must be a dictionary")
    start_job_id = _require_valid_start_mapping_action(
        start_action_payload,
        robot_id=args.robot_id,
        description_prefix=args.description_prefix,
        expected_mapping_session_id=mapping_session_id,
        label="checkpoint start_action contract",
    )
    if start_job_id != mapping_session_id:
        raise ValueError(
            "checkpoint mapping_session_id/start job mismatch expected=%s observed=%s"
            % (mapping_session_id, start_job_id or "-")
        )
    pre_snapshot_payload = payload.get("pre_snapshot")
    mapping_snapshot_payload = payload.get("mapping_snapshot")
    if not isinstance(pre_snapshot_payload, dict) or not pre_snapshot_payload:
        raise ValueError("checkpoint pre_snapshot must be a nonempty dictionary")
    if not isinstance(mapping_snapshot_payload, dict) or not mapping_snapshot_payload:
        raise ValueError("checkpoint mapping_snapshot must be a nonempty dictionary")
    require_snapshot_identity(
        pre_snapshot_payload,
        robot_id=args.robot_id,
        label="checkpoint pre_snapshot",
    )
    require_snapshot_identity(
        mapping_snapshot_payload,
        robot_id=args.robot_id,
        label="checkpoint mapping_snapshot",
    )
    require_mapping_session_active(
        mapping_snapshot_payload,
        label="checkpoint mapping_snapshot",
    )
    flags = _mapping_profile_flags(profile_name)
    return {
        "payload": payload,
        "profile_name": profile_name,
        "save_map_name": save_map_name,
        "mapping_session_id": mapping_session_id,
        "verify_after_save": bool(flags["verify_after_save"]),
        "prepare_after_activate": bool(flags["prepare_after_activate"]),
        "pre_snapshot": dict(payload.get("pre_snapshot") or {}),
        "start_action": dict(start_action_payload),
        "mapping_snapshot": dict(mapping_snapshot_payload),
    }


def _run_verify_revision_profile(client: BackendRuntimeSmokeClient, args) -> Dict[str, object]:
    pre_snapshot = capture_snapshot(
        client,
        robot_id=args.robot_id,
        task_id=args.task_id,
        ignored_warnings=args.ignore_warning,
    )
    require_snapshot_identity(pre_snapshot, robot_id=args.robot_id, label="verify preflight")
    action = _submit_and_wait(
        client,
        action_name="verify_map_revision",
        robot_id=args.robot_id,
        map_name=args.map_name,
        map_revision_id=args.map_revision_id,
        frame_id=args.frame_id,
        save_map_name="",
        description="%s:verify_map_revision" % args.description_prefix,
        job_timeout=args.job_timeout,
        poll_interval=args.poll_interval,
    )
    post_snapshot = capture_snapshot(
        client,
        robot_id=args.robot_id,
        task_id=args.task_id,
        ignored_warnings=args.ignore_warning,
    )
    require_snapshot_identity(post_snapshot, robot_id=args.robot_id, label="verify result")
    target_map_name = action_target_map_name(action, args.map_name)
    target_revision_id = action_target_revision_id(action, args.map_revision_id)
    issues = verify_profile_issues(
        pre_snapshot,
        post_snapshot,
        action,
        target_map_name=target_map_name,
        target_revision_id=target_revision_id,
    )
    verified_map_view = _capture_map_revision_view(
        client,
        map_name=target_map_name,
        map_revision_id=target_revision_id,
    )
    issues.extend(
        _map_revision_view_issues(
            verified_map_view,
            label="post standalone verify target",
            expected_map_name=target_map_name,
            expected_revision_id=target_revision_id,
            expected_lifecycle_status="available",
            expected_verification_status="verified",
        )
    )
    return {
        "profile": "verify_revision",
        "target_revision_id": target_revision_id,
        "pre_snapshot": pre_snapshot,
        "post_snapshot": post_snapshot,
        "verified_map_view": verified_map_view,
        "revision_scope": {
            "pre": dict(pre_snapshot.get("revision_scope") or {}),
            "post": dict(post_snapshot.get("revision_scope") or {}),
        },
        "actions": [action],
        "summary": {
            "ok": _build_summary_ok(issues),
            "issues": issues,
        },
    }


def _run_activate_revision_profile(
    client: BackendRuntimeSmokeClient,
    args,
    *,
    prepare_after_activate: bool,
) -> Dict[str, object]:
    if prepare_after_activate and int(args.task_id) <= 0:
        raise ValueError(
            "--task-id must be greater than zero for profile "
            "activate_revision_prepare_for_task"
        )
    pre_snapshot = capture_snapshot(
        client,
        robot_id=args.robot_id,
        task_id=args.task_id,
        ignored_warnings=args.ignore_warning,
    )
    require_snapshot_identity(pre_snapshot, robot_id=args.robot_id, label="activate preflight")
    action = _submit_and_wait(
        client,
        action_name="activate_map_revision",
        robot_id=args.robot_id,
        map_name=args.map_name,
        map_revision_id=args.map_revision_id,
        frame_id=args.frame_id,
        save_map_name="",
        description="%s:activate_map_revision" % args.description_prefix,
        job_timeout=args.job_timeout,
        poll_interval=args.poll_interval,
    )
    target_revision_id = action_target_revision_id(action, args.map_revision_id)
    activate_snapshot = capture_snapshot(
        client,
        robot_id=args.robot_id,
        task_id=args.task_id,
        ignored_warnings=args.ignore_warning,
    )
    require_snapshot_identity(activate_snapshot, robot_id=args.robot_id, label="activate result")
    target_map_name = action_target_map_name(action, args.map_name)
    issues = activate_profile_issues(
        activate_snapshot,
        action,
        target_map_name=target_map_name,
        target_revision_id=target_revision_id,
    )
    activated_map_view = _capture_map_revision_view(
        client,
        map_name=target_map_name,
        map_revision_id=target_revision_id,
    )
    issues.extend(
        _map_revision_view_issues(
            activated_map_view,
            label="post standalone activate target",
            expected_map_name=target_map_name,
            expected_revision_id=target_revision_id,
            expected_lifecycle_status="available",
            expected_verification_status="verified",
            expected_is_active=True,
        )
    )
    actions = [action]
    prepare_snapshot = None
    if prepare_after_activate and target_revision_id and bool(action.get("ok", False)) and not issues:
        prepare_action = _submit_and_wait(
            client,
            action_name="prepare_for_task",
            robot_id=args.robot_id,
            map_name=target_map_name,
            map_revision_id=target_revision_id,
            frame_id=args.frame_id,
            save_map_name="",
            description="%s:prepare_for_task" % args.description_prefix,
            job_timeout=args.job_timeout,
            poll_interval=args.poll_interval,
        )
        actions.append(prepare_action)
        prepare_snapshot = capture_snapshot(
            client,
            robot_id=args.robot_id,
            task_id=args.task_id,
            ignored_warnings=args.ignore_warning,
        )
        require_snapshot_identity(prepare_snapshot, robot_id=args.robot_id, label="prepare result")
        issues.extend(
            prepare_for_task_profile_issues(
                prepare_snapshot,
                prepare_action,
                target_map_name=target_map_name,
                target_revision_id=target_revision_id,
                target_task_id=args.task_id,
            )
        )
    elif prepare_after_activate:
        issues.append("activate did not pass; skip prepare_for_task")
    return {
        "profile": "activate_revision_prepare_for_task" if prepare_after_activate else "activate_revision",
        "target_revision_id": target_revision_id,
        "pre_snapshot": pre_snapshot,
        "post_snapshot": prepare_snapshot or activate_snapshot,
        "activate_snapshot": activate_snapshot,
        "activated_map_view": activated_map_view,
        "prepare_snapshot": prepare_snapshot,
        "revision_scope": {
            "pre": dict(pre_snapshot.get("revision_scope") or {}),
            "activate": dict(activate_snapshot.get("revision_scope") or {}),
            "post": dict((prepare_snapshot or activate_snapshot or {}).get("revision_scope") or {}),
        },
        "actions": actions,
        "summary": {
            "ok": _build_summary_ok(issues),
            "issues": issues,
        },
    }


def _run_mapping_workflow_profile(
    client: BackendRuntimeSmokeClient,
    args,
    *,
    verify_after_save: bool,
    prepare_after_activate: bool,
) -> Dict[str, object]:
    profile_name = _mapping_profile_name(
        verify_after_save=verify_after_save,
        prepare_after_activate=prepare_after_activate,
    )
    actions: List[Dict[str, object]] = []
    consumed_checkpoint_payload = None
    resumed_mapping_session_id = ""
    if not str(args.resume_from_checkpoint or "").strip():
        if not bool(args.pause_after_start_mapping):
            raise ValueError(
                "initial mapping workflow invocation requires "
                "--pause-after-start-mapping"
            )
        if not str(args.checkpoint_path or "").strip():
            raise ValueError(
                "initial mapping workflow invocation requires --checkpoint-path"
            )
        _require_secure_checkpoint_write_target(args.checkpoint_path)
    if str(args.resume_from_checkpoint or "").strip():
        checkpoint_context = _load_mapping_checkpoint_context(args)
        pre_snapshot = dict(checkpoint_context.get("pre_snapshot") or {})
        start_action = dict(checkpoint_context.get("start_action") or {})
        resumed_mapping_session_id = str(
            checkpoint_context.get("mapping_session_id") or ""
        )
        _require_live_start_mapping_job(
            client,
            args=args,
            mapping_session_id=resumed_mapping_session_id,
            label="mapping resume live session/job gate",
        )
        mapping_snapshot = capture_snapshot(
            client,
            robot_id=args.robot_id,
            task_id=args.task_id,
            ignored_warnings=args.ignore_warning,
        )
        require_snapshot_identity(
            mapping_snapshot,
            robot_id=args.robot_id,
            label="mapping resume preflight",
        )
        require_mapping_session_active(mapping_snapshot, label="mapping resume preflight")
        _require_live_mapping_session_token(
            client,
            expected_session_id=resumed_mapping_session_id,
            label="mapping resume post-snapshot gate",
        )
        actions.append(start_action)
        save_map_name = str(checkpoint_context.get("save_map_name") or "")
        consumed_checkpoint_payload = _mark_checkpoint_resume_started(
            args.resume_from_checkpoint,
            dict(checkpoint_context.get("payload") or {}),
            args,
        )
    else:
        pre_snapshot = capture_snapshot(
            client,
            robot_id=args.robot_id,
            task_id=args.task_id,
            ignored_warnings=args.ignore_warning,
        )
        require_snapshot_identity(pre_snapshot, robot_id=args.robot_id, label="mapping preflight")
        start_action = _submit_and_wait(
            client,
            action_name="start_mapping",
            robot_id=args.robot_id,
            map_name="",
            map_revision_id="",
            frame_id=args.frame_id,
            save_map_name="",
            description="%s:start_mapping" % args.description_prefix,
            job_timeout=args.job_timeout,
            poll_interval=args.poll_interval,
        )
        actions.append(start_action)
        mapping_snapshot = capture_snapshot(
            client,
            robot_id=args.robot_id,
            task_id=args.task_id,
            ignored_warnings=args.ignore_warning,
        )
        require_snapshot_identity(mapping_snapshot, robot_id=args.robot_id, label="start_mapping result")
        save_map_name = str(args.save_map_name or "")
        start_issues = list(start_action.get("issues") or [])
        if start_issues or not bool(start_action.get("ok", False)):
            return {
                "profile": profile_name,
                "phase": "start_mapping_failed",
                "paused": False,
                "target_revision_id": "",
                "pre_snapshot": pre_snapshot,
                "mapping_snapshot": mapping_snapshot,
                "post_snapshot": mapping_snapshot,
                "revision_scope": {
                    "pre": dict(pre_snapshot.get("revision_scope") or {}),
                    "mapping": dict(mapping_snapshot.get("revision_scope") or {}),
                    "post": dict(mapping_snapshot.get("revision_scope") or {}),
                },
                "actions": [start_action],
                "summary": {
                    "ok": False,
                    "issues": start_issues or ["start_mapping failed"],
                },
            }
        require_mapping_session_active(mapping_snapshot, label="start_mapping result")
        if args.pause_after_start_mapping:
            mapping_session_id = _require_valid_start_mapping_action(
                start_action,
                robot_id=args.robot_id,
                description_prefix=args.description_prefix,
                label="pause start_mapping action contract",
            )
            _require_live_mapping_session_token(
                client,
                expected_session_id=mapping_session_id,
                label="pause mapping session gate",
            )
            checkpoint_payload = _mapping_checkpoint_payload(
                profile_name=profile_name,
                args=args,
                pre_snapshot=pre_snapshot,
                start_action=start_action,
                mapping_snapshot=mapping_snapshot,
                mapping_session_id=mapping_session_id,
            )
            _write_checkpoint(args.checkpoint_path, checkpoint_payload)
            return _paused_mapping_report(
                profile_name=profile_name,
                checkpoint_path=args.checkpoint_path,
                pre_snapshot=pre_snapshot,
                mapping_snapshot=mapping_snapshot,
                start_action=start_action,
            )
    if resumed_mapping_session_id:
        _require_live_mapping_session_token(
            client,
            expected_session_id=resumed_mapping_session_id,
            label="mapping resume before save_mapping",
        )
    save_action = _submit_and_wait(
        client,
        action_name="save_mapping",
        robot_id=args.robot_id,
        map_name="",
        map_revision_id="",
        frame_id=args.frame_id,
        save_map_name=save_map_name,
        description="%s:save_mapping" % args.description_prefix,
        job_timeout=args.job_timeout,
        poll_interval=args.poll_interval,
    )
    actions.append(save_action)
    candidate_revision_id = action_target_revision_id(save_action, "")
    pre_stop_snapshot = capture_snapshot(
        client,
        robot_id=args.robot_id,
        task_id=args.task_id,
        ignored_warnings=args.ignore_warning,
    )
    require_snapshot_identity(pre_stop_snapshot, robot_id=args.robot_id, label="stop_mapping preflight")
    stop_action = _submit_and_wait(
        client,
        action_name="stop_mapping",
        robot_id=args.robot_id,
        map_name=save_map_name,
        map_revision_id=candidate_revision_id,
        frame_id=args.frame_id,
        save_map_name="",
        description="%s:stop_mapping" % args.description_prefix,
        job_timeout=args.job_timeout,
        poll_interval=args.poll_interval,
    )
    actions.append(stop_action)
    post_stop_snapshot = capture_snapshot(
        client,
        robot_id=args.robot_id,
        task_id=args.task_id,
        ignored_warnings=args.ignore_warning,
    )
    require_snapshot_identity(post_stop_snapshot, robot_id=args.robot_id, label="stop_mapping result")
    post_stop_mapping_session = {
        "ok": False,
        "mapping_session_id": "",
        "issues": ["stop_mapping did not succeed; mapping session clearance not accepted"],
    }
    if resumed_mapping_session_id and bool(stop_action.get("ok", False)):
        post_stop_mapping_session = _capture_cleared_mapping_session(
            client,
            label="mapping resume post-stop gate",
        )
    candidate_map_view = (
        _capture_map_revision_view(
            client,
            map_name=save_map_name,
            map_revision_id=candidate_revision_id,
        )
        if candidate_revision_id
        else {
            "success": False,
            "message": "candidate revision_id is empty",
            "map": {},
        }
    )
    save_stop_issues = list(save_action.get("issues") or [])
    save_stop_issues.extend(stop_action.get("issues") or [])
    save_stop_issues.extend(post_stop_mapping_session.get("issues") or [])
    save_stop_issues.extend(
        mapping_save_candidate_issues(
            pre_snapshot,
            post_stop_snapshot,
            candidate_revision_id=candidate_revision_id,
        )
    )
    save_stop_issues.extend(
        _map_revision_view_issues(
            candidate_map_view,
            label="post save candidate",
            expected_map_name=save_map_name,
            expected_revision_id=candidate_revision_id,
            expected_lifecycle_status="saved_unverified",
            expected_verification_status="pending",
            require_inactive=True,
        )
    )
    issues = list(start_action.get("issues") or [])
    issues.extend(save_stop_issues)
    verify_snapshot = None
    verified_map_view = None
    activate_snapshot = None
    prepare_snapshot = None
    chain_ready = bool(
        candidate_revision_id
        and save_action.get("ok", False)
        and stop_action.get("ok", False)
        and not save_stop_issues
    )
    if verify_after_save and chain_ready:
        verify_action = _submit_and_wait(
            client,
            action_name="verify_map_revision",
            robot_id=args.robot_id,
            map_name=save_map_name,
            map_revision_id=candidate_revision_id,
            frame_id=args.frame_id,
            save_map_name="",
            description="%s:verify_map_revision" % args.description_prefix,
            job_timeout=args.job_timeout,
            poll_interval=args.poll_interval,
        )
        actions.append(verify_action)
        verify_snapshot = capture_snapshot(
            client,
            robot_id=args.robot_id,
            task_id=args.task_id,
            ignored_warnings=args.ignore_warning,
        )
        require_snapshot_identity(verify_snapshot, robot_id=args.robot_id, label="verify result")
        verify_issues = verify_profile_issues(
            pre_snapshot,
            verify_snapshot,
            verify_action,
            target_map_name=save_map_name,
            target_revision_id=candidate_revision_id,
        )
        verified_map_view = _capture_map_revision_view(
            client,
            map_name=save_map_name,
            map_revision_id=candidate_revision_id,
        )
        verify_issues.extend(
            _map_revision_view_issues(
                verified_map_view,
                label="post verify candidate",
                expected_map_name=save_map_name,
                expected_revision_id=candidate_revision_id,
                expected_lifecycle_status="available",
                expected_verification_status="verified",
            )
        )
        issues.extend(verify_issues)
        if bool(verify_action.get("ok", False)) and not verify_issues:
            activate_action = _submit_and_wait(
                client,
                action_name="activate_map_revision",
                robot_id=args.robot_id,
                map_name=save_map_name,
                map_revision_id=candidate_revision_id,
                frame_id=args.frame_id,
                save_map_name="",
                description="%s:activate_map_revision" % args.description_prefix,
                job_timeout=args.job_timeout,
                poll_interval=args.poll_interval,
            )
            actions.append(activate_action)
            activate_snapshot = capture_snapshot(
                client,
                robot_id=args.robot_id,
                task_id=args.task_id,
                ignored_warnings=args.ignore_warning,
            )
            require_snapshot_identity(activate_snapshot, robot_id=args.robot_id, label="activate result")
            activate_issues = activate_profile_issues(
                activate_snapshot,
                activate_action,
                target_map_name=save_map_name,
                target_revision_id=candidate_revision_id,
            )
            issues.extend(activate_issues)
            if prepare_after_activate and bool(activate_action.get("ok", False)) and not activate_issues:
                prepare_action = _submit_and_wait(
                    client,
                    action_name="prepare_for_task",
                    robot_id=args.robot_id,
                    map_name=save_map_name,
                    map_revision_id=candidate_revision_id,
                    frame_id=args.frame_id,
                    save_map_name="",
                    description="%s:prepare_for_task" % args.description_prefix,
                    job_timeout=args.job_timeout,
                    poll_interval=args.poll_interval,
                )
                actions.append(prepare_action)
                prepare_snapshot = capture_snapshot(
                    client,
                    robot_id=args.robot_id,
                    task_id=args.task_id,
                    ignored_warnings=args.ignore_warning,
                )
                require_snapshot_identity(prepare_snapshot, robot_id=args.robot_id, label="prepare result")
                issues.extend(
                    prepare_for_task_profile_issues(
                        prepare_snapshot,
                        prepare_action,
                        target_map_name=save_map_name,
                        target_revision_id=candidate_revision_id,
                        target_task_id=args.task_id,
                    )
                )
            elif prepare_after_activate:
                issues.append("activate did not pass; skip prepare_for_task")
        else:
            issues.append("verify did not pass; skip activate_map_revision")
    elif verify_after_save:
        issues.append("save/stop did not pass or candidate revision_id is empty; skip verify/activate")
    revision_scope = {
        "pre": dict(pre_snapshot.get("revision_scope") or {}),
        "mapping": dict(mapping_snapshot.get("revision_scope") or {}),
        "post_stop": dict(post_stop_snapshot.get("revision_scope") or {}),
        "post": dict((prepare_snapshot or activate_snapshot or verify_snapshot or post_stop_snapshot or {}).get("revision_scope") or {}),
    }
    if verify_snapshot:
        revision_scope["verify"] = dict(verify_snapshot.get("revision_scope") or {})
    if activate_snapshot:
        revision_scope["activate"] = dict(activate_snapshot.get("revision_scope") or {})
    if prepare_snapshot:
        revision_scope["prepare"] = dict(prepare_snapshot.get("revision_scope") or {})
    report = {
        "profile": profile_name,
        "phase": "completed",
        "paused": False,
        "resumed_from_checkpoint": str(args.resume_from_checkpoint or ""),
        "target_revision_id": candidate_revision_id,
        "pre_snapshot": pre_snapshot,
        "mapping_snapshot": mapping_snapshot,
        "post_snapshot": prepare_snapshot or activate_snapshot or verify_snapshot or post_stop_snapshot,
        "post_stop_snapshot": post_stop_snapshot,
        "post_stop_mapping_session": post_stop_mapping_session,
        "candidate_map_view": candidate_map_view,
        "verify_snapshot": verify_snapshot,
        "verified_map_view": verified_map_view,
        "activate_snapshot": activate_snapshot,
        "prepare_snapshot": prepare_snapshot,
        "revision_scope": revision_scope,
        "actions": actions,
        "summary": {
            "ok": _build_summary_ok(issues),
            "issues": issues,
        },
    }
    if consumed_checkpoint_payload is not None:
        _finalize_consumed_checkpoint(args.resume_from_checkpoint, consumed_checkpoint_payload, report)
    return report


def build_report(args) -> Dict[str, object]:
    validate_args(args)
    args.robot_id = require_explicit_commercial_robot_id(args.robot_id)
    client = BackendRuntimeSmokeClient()
    client.wait_for_services(timeout_s=args.service_timeout)
    require_write_topology_identity(client, args)
    if args.profile == "verify_revision":
        return _run_verify_revision_profile(client, args)
    if args.profile == "activate_revision":
        return _run_activate_revision_profile(client, args, prepare_after_activate=False)
    if args.profile == "activate_revision_prepare_for_task":
        return _run_activate_revision_profile(client, args, prepare_after_activate=True)
    if args.profile == "mapping_save_candidate":
        return _run_mapping_workflow_profile(client, args, verify_after_save=False, prepare_after_activate=False)
    if args.profile == "mapping_save_verify_activate":
        return _run_mapping_workflow_profile(client, args, verify_after_save=True, prepare_after_activate=False)
    raise ValueError("unsupported profile: %s" % str(args.profile or ""))


def _print_snapshot(label: str, snapshot: Dict[str, object]) -> None:
    slam = dict(snapshot.get("slam") or {})
    readiness = dict(snapshot.get("readiness") or {})
    print(
        "- %s: mode=%s localization=%s valid=%s active_map=%s active_revision=%s runtime_map=%s runtime_revision=%s pending_revision=%s pending_status=%s can_start_task=%s"
        % (
            label,
            str(slam.get("current_mode") or "-"),
            str(slam.get("localization_state") or "-"),
            bool(slam.get("localization_valid", False)),
            str(slam.get("active_map_name") or "-"),
            str(slam.get("active_map_revision_id") or "-"),
            str(slam.get("runtime_map_name") or "-"),
            str(slam.get("runtime_map_revision_id") or "-"),
            str(slam.get("pending_map_revision_id") or "-"),
            str(slam.get("pending_map_switch_status") or "-"),
            bool(readiness.get("can_start_task", False)),
        )
    )
    scope = dict(snapshot.get("revision_scope") or {})
    if scope:
        print("  revision_scope: %s" % _format_revision_scope(scope))


def _print_action(action: Dict[str, object]) -> None:
    submit = dict(action.get("submit") or {})
    job = dict(((action.get("job") or {}).get("response") or {}).get("job") or {})
    print(
        "- action %s: accepted=%s ok=%s job_id=%s terminal=%s result_code=%s requested_revision=%s resolved_revision=%s"
        % (
            str(action.get("name") or ""),
            bool(submit.get("accepted", False)),
            bool(action.get("ok", False)),
            str(submit.get("job_id") or "-"),
            str((action.get("job") or {}).get("terminal_state") or "-"),
            str(job.get("result_code") or "-"),
            str(job.get("requested_map_revision_id") or "-"),
            str(job.get("resolved_map_revision_id") or "-"),
        )
    )
    for issue in list(action.get("issues") or []):
        print("  issue: %s" % str(issue))


def _print_text(report: Dict[str, object]) -> None:
    print("Revision workflow acceptance")
    print("Profile: %s" % str(report.get("profile") or ""))
    if bool(report.get("paused", False)):
        print("Phase: %s" % str(report.get("phase") or "paused"))
        if str(report.get("checkpoint_path") or ""):
            print("Checkpoint: %s" % str(report.get("checkpoint_path") or ""))
        if str(report.get("resume_hint") or ""):
            print("Resume hint: %s" % str(report.get("resume_hint") or ""))
    elif str(report.get("resumed_from_checkpoint") or ""):
        print("Resumed from checkpoint: %s" % str(report.get("resumed_from_checkpoint") or ""))
    target_revision_id = str(report.get("target_revision_id") or "")
    if target_revision_id:
        print("Target revision: %s" % target_revision_id)
    report_scope = dict(report.get("revision_scope") or {})
    final_scope = dict(report_scope.get("post") or {})
    if final_scope:
        print("Final revision scope: %s" % _format_revision_scope(final_scope))
    for label in (
        "pre_snapshot",
        "mapping_snapshot",
        "post_stop_snapshot",
        "verify_snapshot",
        "activate_snapshot",
        "prepare_snapshot",
        "post_snapshot",
    ):
        snapshot = report.get(label)
        if snapshot:
            _print_snapshot(label, snapshot)
    for action in list(report.get("actions") or []):
        _print_action(action)
    summary = dict(report.get("summary") or {})
    if bool(summary.get("paused", False)):
        print("Summary: PAUSED")
    else:
        print("Summary: %s" % ("OK" if bool(summary.get("ok", False)) else "FAIL"))
    for issue in list(summary.get("issues") or []):
        print("- %s" % str(issue))


def _print_checkpoint_text(report: Dict[str, object]) -> None:
    print("Revision acceptance checkpoint")
    print("Checkpoint: %s" % str(report.get("checkpoint_path") or ""))
    print("Checkpoint version: %s" % int(report.get("checkpoint_version", 0) or 0))
    print("Profile: %s" % str(report.get("profile") or "-"))
    print("Phase: %s" % str(report.get("phase") or "-"))
    print("Resumable: %s" % ("yes" if bool(report.get("resumable", False)) else "no"))
    print("Robot: %s" % str(report.get("robot_id") or "-"))
    print("Task: %s" % int(report.get("task_id", 0) or 0))
    print("Frame: %s" % str(report.get("frame_id") or "-"))
    print("Save map: %s" % str(report.get("save_map_name") or "-"))
    print("Mapping session: %s" % str(report.get("mapping_session_id") or "-"))
    print("Description prefix: %s" % str(report.get("description_prefix") or "-"))

    resume_request = dict(report.get("resume_request") or {})
    if resume_request:
        print(
            "Resume request: profile=%s task_id=%s robot_id=%s frame_id=%s"
            % (
                str(resume_request.get("profile") or "-"),
                str(resume_request.get("task_id") or "-"),
                str(resume_request.get("robot_id") or "-"),
                str(resume_request.get("frame_id") or "-"),
            )
        )

    result = dict(report.get("result") or {})
    if result:
        print(
            "Result: ok=%s target_revision=%s"
            % (
                bool(result.get("ok", False)),
                str(result.get("target_revision_id") or "-"),
            )
        )
        final_scope = dict(result.get("final_revision_scope") or {})
        if final_scope:
            print("Final revision scope: %s" % _format_revision_scope(final_scope))
        for issue in list(result.get("issues") or []):
            print("- %s" % str(issue))


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run commercial-grade revision workflow acceptance checks for verify/activate/mapping candidate flows."
    )
    parser.add_argument("--inspect-checkpoint", action="store_true")
    parser.add_argument("--profile", choices=SUPPORTED_PROFILES, default="")
    parser.add_argument("--task-id", type=int, default=0)
    parser.add_argument("--service-timeout", type=float, default=10.0)
    parser.add_argument("--job-timeout", type=float, default=90.0)
    parser.add_argument("--poll-interval", type=float, default=1.0)
    parser.add_argument("--robot-id", default="")
    parser.add_argument("--map-name", default="")
    parser.add_argument("--map-revision-id", default="")
    parser.add_argument("--save-map-name", default="")
    parser.add_argument("--frame-id", default="map")
    parser.add_argument("--description-prefix", default="revision_acceptance")
    parser.add_argument("--allow-write-actions", action="store_true")
    parser.add_argument("--pause-after-start-mapping", action="store_true")
    parser.add_argument("--checkpoint-path", default="")
    parser.add_argument("--resume-from-checkpoint", default="")
    parser.add_argument(
        "--ignore-warning",
        action="append",
        default=["station_status stale or missing"],
    )
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--text", action="store_true")
    parser.add_argument("--require-resumable", action="store_true")
    return parser


def validate_args(args) -> None:
    args.profile = resolve_effective_profile(args)
    args.robot_id = require_explicit_commercial_robot_id(args.robot_id)
    if args.profile in WRITE_PROFILES and not bool(args.allow_write_actions):
        raise ValueError("--allow-write-actions is required for profile %s" % str(args.profile))
    if bool(args.pause_after_start_mapping) and str(args.resume_from_checkpoint or "").strip():
        raise ValueError("--pause-after-start-mapping and --resume-from-checkpoint cannot be used together")
    mapping_profiles = {
        "mapping_save_candidate",
        "mapping_save_verify_activate",
    }
    if (bool(args.pause_after_start_mapping) or str(args.resume_from_checkpoint or "").strip()) and args.profile not in mapping_profiles:
        raise ValueError("pause/resume options are only supported for mapping workflow profiles")
    if bool(args.pause_after_start_mapping) and not str(args.checkpoint_path or "").strip():
        raise ValueError("--checkpoint-path is required with --pause-after-start-mapping")
    if args.profile in {
        "verify_revision",
        "activate_revision",
        "activate_revision_prepare_for_task",
    }:
        if not str(args.map_name or "").strip() or not str(args.map_revision_id or "").strip():
            raise ValueError(
                "--map-name and --map-revision-id are required for profile %s"
                % str(args.profile)
            )
    prepare_profiles = {"activate_revision_prepare_for_task"}
    if args.profile in prepare_profiles and int(args.task_id) <= 0:
        raise ValueError(
            "--task-id must be greater than zero for profile "
            "%s" % str(args.profile)
        )
    if args.profile in mapping_profiles:
        if str(args.resume_from_checkpoint or "").strip():
            return
        if not bool(args.pause_after_start_mapping):
            raise ValueError(
                "initial mapping workflow invocation requires "
                "--pause-after-start-mapping"
            )
        if not str(args.checkpoint_path or "").strip():
            raise ValueError(
                "initial mapping workflow invocation requires --checkpoint-path"
            )
        _require_secure_checkpoint_write_target(args.checkpoint_path)
        if not str(args.save_map_name or "").strip():
            raise ValueError("--save-map-name is required for profile %s" % str(args.profile))


def validate_checkpoint_inspect_args(args) -> None:
    if not str(args.checkpoint_path or "").strip():
        raise ValueError("--checkpoint-path is required with --inspect-checkpoint")
    if bool(args.pause_after_start_mapping) or str(args.resume_from_checkpoint or "").strip():
        raise ValueError("--inspect-checkpoint cannot be combined with pause/resume workflow options")


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()
    try:
        if bool(args.inspect_checkpoint):
            validate_checkpoint_inspect_args(args)
            report = build_checkpoint_report(args.checkpoint_path)
        else:
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
        json.dump(report, sys.stdout, ensure_ascii=False, indent=2)
        sys.stdout.write("\n")
    if args.text or not args.json:
        if bool(args.inspect_checkpoint):
            _print_checkpoint_text(report)
        else:
            _print_text(report)
    if bool(args.inspect_checkpoint):
        if bool(args.require_resumable) and not bool(report.get("resumable", False)):
            return 2
        return 0
    return 0 if bool((report.get("summary") or {}).get("ok", False)) else 2


if __name__ == "__main__":
    raise SystemExit(main())

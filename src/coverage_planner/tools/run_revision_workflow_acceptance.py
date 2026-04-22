#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import os
import sys
from typing import Dict, Iterable, List, Sequence

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
if THIS_DIR not in sys.path:
    sys.path.insert(0, THIS_DIR)

from run_backend_runtime_smoke import (
    BackendRuntimeSmokeClient,
    _lookup_latest_head_scope,
    _check_odometry,
    _check_readiness,
    _check_slam,
    _format_revision_scope,
    build_revision_scope,
    wait_for_job,
)


SUPPORTED_PROFILES = (
    "verify_revision",
    "activate_revision",
    "activate_revision_prepare_for_task",
    "mapping_save_candidate",
    "mapping_save_verify_activate",
    "mapping_save_verify_activate_prepare_for_task",
)

WRITE_PROFILES = set(SUPPORTED_PROFILES)
CHECKPOINT_VERSION = 1
RESUMABLE_PHASE = "paused_after_start_mapping"


def _state_from_check(check_result: Dict[str, object], field_name: str) -> Dict[str, object]:
    return dict((check_result.get("response") or {}).get(field_name) or {})


def capture_snapshot(
    client: BackendRuntimeSmokeClient,
    *,
    task_id: int,
    ignored_warnings: Sequence[str],
) -> Dict[str, object]:
    slam = _check_slam(client.get_slam_status(), ignored_warnings)
    odometry = _check_odometry(client.get_odometry_status(), ignored_warnings)
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
    waited = wait_for_job(
        client=client,
        job_id=result["submit"]["job_id"],
        robot_id=robot_id,
        timeout_s=job_timeout,
        poll_interval_s=poll_interval,
    )
    result["job"] = waited
    if not bool(waited.get("ok", False)):
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


def verify_profile_issues(
    pre_snapshot: Dict[str, object],
    post_snapshot: Dict[str, object],
    action_result: Dict[str, object],
    *,
    target_revision_id: str,
) -> List[str]:
    issues = list(action_result.get("issues") or [])
    pre_slam = dict(pre_snapshot.get("slam") or {})
    post_slam = dict(post_snapshot.get("slam") or {})
    target_revision_id = str(target_revision_id or "").strip()
    if not target_revision_id:
        issues.append("verify target revision_id is empty")
    pre_active_revision = str(pre_slam.get("active_map_revision_id") or "").strip()
    post_active_revision = str(post_slam.get("active_map_revision_id") or "").strip()
    pre_active_name = str(pre_slam.get("active_map_name") or "").strip()
    post_active_name = str(post_slam.get("active_map_name") or "").strip()
    if pre_active_revision and post_active_revision != pre_active_revision:
        issues.append(
            "verify changed active revision unexpectedly before=%s after=%s"
            % (pre_active_revision, post_active_revision or "-")
        )
    if pre_active_name and post_active_name != pre_active_name:
        issues.append(
            "verify changed active map unexpectedly before=%s after=%s"
            % (pre_active_name, post_active_name or "-")
        )
    if target_revision_id and pre_active_revision and target_revision_id != pre_active_revision and post_active_revision == target_revision_id:
        issues.append("verify committed candidate revision as active unexpectedly")
    if not bool(post_slam.get("runtime_map_match", False)):
        issues.append("post verify runtime_map_match=false")
    if not bool(post_slam.get("localization_valid", False)):
        issues.append("post verify localization_valid=false")
    active_revision = str(post_slam.get("active_map_revision_id") or "").strip()
    runtime_revision = str(post_slam.get("runtime_map_revision_id") or "").strip()
    if active_revision and runtime_revision and active_revision != runtime_revision:
        issues.append(
            "post verify runtime revision mismatches active revision active=%s runtime=%s"
            % (active_revision, runtime_revision)
        )
    _append_pending_switch_issue(issues, post_snapshot, label="post verify")
    return issues


def activate_profile_issues(
    post_snapshot: Dict[str, object],
    action_result: Dict[str, object],
    *,
    target_revision_id: str,
) -> List[str]:
    issues = list(action_result.get("issues") or [])
    post_slam = dict(post_snapshot.get("slam") or {})
    post_readiness = dict(post_snapshot.get("readiness") or {})
    target_revision_id = str(target_revision_id or "").strip()
    if not target_revision_id:
        issues.append("activate target revision_id is empty")
        return issues
    active_revision = str(post_slam.get("active_map_revision_id") or "").strip()
    runtime_revision = str(post_slam.get("runtime_map_revision_id") or "").strip()
    readiness_active_revision = str(post_readiness.get("active_map_revision_id") or "").strip()
    if active_revision != target_revision_id:
        issues.append(
            "post activate active revision mismatch expected=%s actual=%s"
            % (target_revision_id, active_revision or "-")
        )
    if runtime_revision != target_revision_id:
        issues.append(
            "post activate runtime revision mismatch expected=%s actual=%s"
            % (target_revision_id, runtime_revision or "-")
        )
    if readiness_active_revision and readiness_active_revision != target_revision_id:
        issues.append(
            "post activate readiness active revision mismatch expected=%s actual=%s"
            % (target_revision_id, readiness_active_revision)
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
    target_revision_id: str,
) -> List[str]:
    issues = list(action_result.get("issues") or [])
    post_slam = dict(post_snapshot.get("slam") or {})
    post_readiness = dict(post_snapshot.get("readiness") or {})
    target_revision_id = str(target_revision_id or "").strip()
    if not target_revision_id:
        issues.append("prepare_for_task target revision_id is empty")
        return issues
    active_revision = str(post_slam.get("active_map_revision_id") or "").strip()
    runtime_revision = str(post_slam.get("runtime_map_revision_id") or "").strip()
    readiness_active_revision = str(post_readiness.get("active_map_revision_id") or "").strip()
    if active_revision != target_revision_id:
        issues.append(
            "post prepare_for_task active revision mismatch expected=%s actual=%s"
            % (target_revision_id, active_revision or "-")
        )
    if runtime_revision != target_revision_id:
        issues.append(
            "post prepare_for_task runtime revision mismatch expected=%s actual=%s"
            % (target_revision_id, runtime_revision or "-")
        )
    if readiness_active_revision and readiness_active_revision != target_revision_id:
        issues.append(
            "post prepare_for_task readiness active revision mismatch expected=%s actual=%s"
            % (target_revision_id, readiness_active_revision)
        )
    if not bool(post_slam.get("runtime_map_match", False)):
        issues.append("post prepare_for_task runtime_map_match=false")
    if not bool(post_slam.get("localization_valid", False)):
        issues.append("post prepare_for_task localization_valid=false")
    if not bool(post_slam.get("task_ready", False)):
        issues.append("post prepare_for_task task_ready=false")
    if not bool(post_readiness.get("can_start_task", False)):
        issues.append("post prepare_for_task can_start_task=false")
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
    pre_active_revision = str(pre_slam.get("active_map_revision_id") or "").strip()
    post_active_revision = str(post_slam.get("active_map_revision_id") or "").strip()
    pre_active_name = str(pre_slam.get("active_map_name") or "").strip()
    post_active_name = str(post_slam.get("active_map_name") or "").strip()
    if pre_active_revision and post_active_revision != pre_active_revision:
        issues.append(
            "mapping cycle changed active revision unexpectedly before=%s after=%s"
            % (pre_active_revision, post_active_revision or "-")
        )
    if pre_active_name and post_active_name != pre_active_name:
        issues.append(
            "mapping cycle changed active map unexpectedly before=%s after=%s"
            % (pre_active_name, post_active_name or "-")
        )
    if candidate_revision_id and pre_active_revision and candidate_revision_id == post_active_revision and candidate_revision_id != pre_active_revision:
        issues.append("saved candidate revision became active unexpectedly")
    if not bool(post_slam.get("runtime_map_match", False)):
        issues.append("post mapping cycle runtime_map_match=false")
    if not bool(post_slam.get("localization_valid", False)):
        issues.append("post mapping cycle localization_valid=false")
    _append_pending_switch_issue(issues, post_snapshot, label="post mapping cycle")
    return issues


def _build_summary_ok(issues: Iterable[str]) -> bool:
    return not [str(item) for item in list(issues or []) if str(item)]


def _mapping_profile_name(*, verify_after_save: bool, prepare_after_activate: bool) -> str:
    if verify_after_save and prepare_after_activate:
        return "mapping_save_verify_activate_prepare_for_task"
    if verify_after_save:
        return "mapping_save_verify_activate"
    return "mapping_save_candidate"


def _mapping_profile_flags(profile_name: str) -> Dict[str, bool]:
    profile_name = str(profile_name or "")
    if profile_name == "mapping_save_candidate":
        return {"verify_after_save": False, "prepare_after_activate": False}
    if profile_name == "mapping_save_verify_activate":
        return {"verify_after_save": True, "prepare_after_activate": False}
    if profile_name == "mapping_save_verify_activate_prepare_for_task":
        return {"verify_after_save": True, "prepare_after_activate": True}
    raise ValueError("profile does not support mapping checkpoint flow: %s" % profile_name)


def _write_checkpoint(path: str, payload: Dict[str, object]) -> None:
    target_path = str(path or "").strip()
    if not target_path:
        raise ValueError("checkpoint path is empty")
    parent_dir = os.path.dirname(target_path)
    if parent_dir:
        os.makedirs(parent_dir, exist_ok=True)
    with open(target_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")


def _load_checkpoint(path: str) -> Dict[str, object]:
    target_path = str(path or "").strip()
    if not target_path:
        raise ValueError("checkpoint path is empty")
    with open(target_path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if int(payload.get("checkpoint_version", 0) or 0) != CHECKPOINT_VERSION:
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


def _validate_resume_identity(payload: Dict[str, object], args) -> None:
    checkpoint_robot_id = _checkpoint_string_field(payload, "robot_id")
    checkpoint_frame_id = _checkpoint_string_field(payload, "frame_id")
    checkpoint_description_prefix = _checkpoint_string_field(payload, "description_prefix")
    checkpoint_task_id = int((payload or {}).get("task_id", 0) or 0)

    requested_robot_id = str(args.robot_id or "").strip()
    requested_frame_id = str(args.frame_id or "").strip()
    requested_description_prefix = str(args.description_prefix or "").strip()
    requested_task_id = int(args.task_id)

    if checkpoint_robot_id and requested_robot_id != checkpoint_robot_id:
        raise ValueError(
            "robot_id mismatch checkpoint=%s requested=%s"
            % (checkpoint_robot_id, requested_robot_id or "-")
        )
    if checkpoint_frame_id and requested_frame_id != checkpoint_frame_id:
        raise ValueError(
            "frame_id mismatch checkpoint=%s requested=%s"
            % (checkpoint_frame_id, requested_frame_id or "-")
        )
    if checkpoint_description_prefix and requested_description_prefix != checkpoint_description_prefix:
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
) -> Dict[str, object]:
    return {
        "checkpoint_version": CHECKPOINT_VERSION,
        "profile": str(profile_name or ""),
        "phase": RESUMABLE_PHASE,
        "save_map_name": str(args.save_map_name or ""),
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
    profile_name = str(payload.get("profile") or "").strip()
    if profile_name != str(args.profile or "").strip():
        raise ValueError(
            "checkpoint profile mismatch checkpoint=%s requested=%s"
            % (profile_name or "-", str(args.profile or "").strip() or "-")
        )
    if str(payload.get("phase") or "").strip() != RESUMABLE_PHASE:
        raise ValueError("checkpoint is not paused after start_mapping")
    save_map_name = str(payload.get("save_map_name") or "").strip()
    if str(args.save_map_name or "").strip() and str(args.save_map_name or "").strip() != save_map_name:
        raise ValueError(
            "save_map_name mismatch checkpoint=%s requested=%s"
            % (save_map_name or "-", str(args.save_map_name or "").strip())
        )
    _validate_resume_identity(payload, args)
    flags = _mapping_profile_flags(profile_name)
    return {
        "payload": payload,
        "profile_name": profile_name,
        "save_map_name": save_map_name,
        "verify_after_save": bool(flags["verify_after_save"]),
        "prepare_after_activate": bool(flags["prepare_after_activate"]),
        "pre_snapshot": dict(payload.get("pre_snapshot") or {}),
        "start_action": dict(payload.get("start_action") or {}),
        "mapping_snapshot": dict(payload.get("mapping_snapshot") or {}),
    }


def _run_verify_revision_profile(client: BackendRuntimeSmokeClient, args) -> Dict[str, object]:
    pre_snapshot = capture_snapshot(client, task_id=args.task_id, ignored_warnings=args.ignore_warning)
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
    post_snapshot = capture_snapshot(client, task_id=args.task_id, ignored_warnings=args.ignore_warning)
    target_revision_id = action_target_revision_id(action, args.map_revision_id)
    issues = verify_profile_issues(
        pre_snapshot,
        post_snapshot,
        action,
        target_revision_id=target_revision_id,
    )
    return {
        "profile": "verify_revision",
        "target_revision_id": target_revision_id,
        "pre_snapshot": pre_snapshot,
        "post_snapshot": post_snapshot,
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
    pre_snapshot = capture_snapshot(client, task_id=args.task_id, ignored_warnings=args.ignore_warning)
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
    activate_snapshot = capture_snapshot(client, task_id=args.task_id, ignored_warnings=args.ignore_warning)
    issues = activate_profile_issues(
        activate_snapshot,
        action,
        target_revision_id=target_revision_id,
    )
    actions = [action]
    prepare_snapshot = None
    if prepare_after_activate and target_revision_id:
        prepare_action = _submit_and_wait(
            client,
            action_name="prepare_for_task",
            robot_id=args.robot_id,
            map_name=args.map_name,
            map_revision_id=target_revision_id,
            frame_id=args.frame_id,
            save_map_name="",
            description="%s:prepare_for_task" % args.description_prefix,
            job_timeout=args.job_timeout,
            poll_interval=args.poll_interval,
        )
        actions.append(prepare_action)
        prepare_snapshot = capture_snapshot(client, task_id=args.task_id, ignored_warnings=args.ignore_warning)
        issues.extend(
            prepare_for_task_profile_issues(
                prepare_snapshot,
                prepare_action,
                target_revision_id=target_revision_id,
            )
        )
    return {
        "profile": "activate_revision_prepare_for_task" if prepare_after_activate else "activate_revision",
        "target_revision_id": target_revision_id,
        "pre_snapshot": pre_snapshot,
        "post_snapshot": prepare_snapshot or activate_snapshot,
        "activate_snapshot": activate_snapshot,
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
    if str(args.resume_from_checkpoint or "").strip():
        checkpoint_context = _load_mapping_checkpoint_context(args)
        pre_snapshot = dict(checkpoint_context.get("pre_snapshot") or {})
        start_action = dict(checkpoint_context.get("start_action") or {})
        mapping_snapshot = dict(checkpoint_context.get("mapping_snapshot") or {})
        actions.append(start_action)
        save_map_name = str(checkpoint_context.get("save_map_name") or "")
        consumed_checkpoint_payload = _mark_checkpoint_resume_started(
            args.resume_from_checkpoint,
            dict(checkpoint_context.get("payload") or {}),
            args,
        )
    else:
        pre_snapshot = capture_snapshot(client, task_id=args.task_id, ignored_warnings=args.ignore_warning)
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
        mapping_snapshot = capture_snapshot(client, task_id=args.task_id, ignored_warnings=args.ignore_warning)
        save_map_name = str(args.save_map_name or "")
        if args.pause_after_start_mapping:
            start_issues = list(start_action.get("issues") or [])
            if start_issues:
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
                        "issues": start_issues,
                    },
                }
            checkpoint_payload = _mapping_checkpoint_payload(
                profile_name=profile_name,
                args=args,
                pre_snapshot=pre_snapshot,
                start_action=start_action,
                mapping_snapshot=mapping_snapshot,
            )
            _write_checkpoint(args.checkpoint_path, checkpoint_payload)
            return _paused_mapping_report(
                profile_name=profile_name,
                checkpoint_path=args.checkpoint_path,
                pre_snapshot=pre_snapshot,
                mapping_snapshot=mapping_snapshot,
                start_action=start_action,
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
    post_stop_snapshot = capture_snapshot(client, task_id=args.task_id, ignored_warnings=args.ignore_warning)
    issues = list(start_action.get("issues") or [])
    issues.extend(save_action.get("issues") or [])
    issues.extend(stop_action.get("issues") or [])
    issues.extend(
        mapping_save_candidate_issues(
            pre_snapshot,
            post_stop_snapshot,
            candidate_revision_id=candidate_revision_id,
        )
    )
    verify_snapshot = None
    activate_snapshot = None
    prepare_snapshot = None
    if verify_after_save and candidate_revision_id:
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
        verify_snapshot = capture_snapshot(client, task_id=args.task_id, ignored_warnings=args.ignore_warning)
        issues.extend(
            verify_profile_issues(
                pre_snapshot,
                verify_snapshot,
                verify_action,
                target_revision_id=candidate_revision_id,
            )
        )
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
        activate_snapshot = capture_snapshot(client, task_id=args.task_id, ignored_warnings=args.ignore_warning)
        issues.extend(
            activate_profile_issues(
                activate_snapshot,
                activate_action,
                target_revision_id=candidate_revision_id,
            )
        )
        if prepare_after_activate:
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
            prepare_snapshot = capture_snapshot(client, task_id=args.task_id, ignored_warnings=args.ignore_warning)
            issues.extend(
                prepare_for_task_profile_issues(
                    prepare_snapshot,
                    prepare_action,
                    target_revision_id=candidate_revision_id,
                )
            )
    elif verify_after_save:
        issues.append("save_mapping did not yield candidate revision_id; skip verify/activate")
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
        "verify_snapshot": verify_snapshot,
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
    client = BackendRuntimeSmokeClient()
    client.wait_for_services(timeout_s=args.service_timeout)
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
    if args.profile == "mapping_save_verify_activate_prepare_for_task":
        return _run_mapping_workflow_profile(client, args, verify_after_save=True, prepare_after_activate=True)
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
    print("Profile: %s" % str(report.get("profile") or "-"))
    print("Phase: %s" % str(report.get("phase") or "-"))
    print("Resumable: %s" % ("yes" if bool(report.get("resumable", False)) else "no"))
    print("Robot: %s" % str(report.get("robot_id") or "-"))
    print("Task: %s" % int(report.get("task_id", 0) or 0))
    print("Frame: %s" % str(report.get("frame_id") or "-"))
    print("Save map: %s" % str(report.get("save_map_name") or "-"))
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
    parser.add_argument("--robot-id", default="local_robot")
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
    if args.profile in WRITE_PROFILES and not bool(args.allow_write_actions):
        raise ValueError("--allow-write-actions is required for profile %s" % str(args.profile))
    if bool(args.pause_after_start_mapping) and str(args.resume_from_checkpoint or "").strip():
        raise ValueError("--pause-after-start-mapping and --resume-from-checkpoint cannot be used together")
    mapping_profiles = {
        "mapping_save_candidate",
        "mapping_save_verify_activate",
        "mapping_save_verify_activate_prepare_for_task",
    }
    if (bool(args.pause_after_start_mapping) or str(args.resume_from_checkpoint or "").strip()) and args.profile not in mapping_profiles:
        raise ValueError("pause/resume options are only supported for mapping workflow profiles")
    if bool(args.pause_after_start_mapping) and not str(args.checkpoint_path or "").strip():
        raise ValueError("--checkpoint-path is required with --pause-after-start-mapping")
    if args.profile in {"verify_revision", "activate_revision"}:
        if not (str(args.map_name or "").strip() or str(args.map_revision_id or "").strip()):
            raise ValueError("--map-name or --map-revision-id is required for profile %s" % str(args.profile))
    if args.profile in {"activate_revision_prepare_for_task"}:
        if not (str(args.map_name or "").strip() or str(args.map_revision_id or "").strip()):
            raise ValueError("--map-name or --map-revision-id is required for profile %s" % str(args.profile))
    if args.profile in mapping_profiles:
        if str(args.resume_from_checkpoint or "").strip():
            return
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

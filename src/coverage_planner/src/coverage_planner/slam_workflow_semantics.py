# -*- coding: utf-8 -*-

"""Shared frontend-visible SLAM workflow semantics for the formal backend."""

from __future__ import annotations

from typing import Tuple


LOCALIZATION_READY_STATES = frozenset({"localized"})
LOCALIZATION_TRANSITION_STATES = frozenset({"localizing", "relocalizing", "stabilizing"})
LOCALIZATION_DEGRADED_STATES = frozenset({"degraded"})
LOCALIZATION_MANUAL_ASSIST_STATES = frozenset({"manual_assist_required"})


GLOBAL_RELOCATE_MANUAL_ASSIST_CODES = {-2, -3, -4, -6, -8, -9}
GLOBAL_RELOCATE_RESULT_CODE_MAP = {
    0: "ok",
    -2: "relocalize_need_more_trajectories",
    -3: "relocalize_no_interest_points",
    -4: "relocalize_submap_not_found",
    -5: "relocalize_busy",
    -6: "relocalize_wait_for_sensor_timeout",
    -7: "relocalize_unknown_failure",
    -8: "relocalize_no_candidate_pose",
    -9: "relocalize_low_constraint_score",
}

MANUAL_ASSIST_ERROR_CODES = {
    GLOBAL_RELOCATE_RESULT_CODE_MAP[code]
    for code in GLOBAL_RELOCATE_MANUAL_ASSIST_CODES
}


def map_global_relocate_result_code(code):
    try:
        normalized = int(code)
    except Exception:
        normalized = -7
    return GLOBAL_RELOCATE_RESULT_CODE_MAP.get(normalized, "relocalize_unknown_failure")


def is_global_relocate_manual_assist(code):
    try:
        normalized = int(code)
    except Exception:
        return False
    return normalized in GLOBAL_RELOCATE_MANUAL_ASSIST_CODES


def is_manual_assist_error_code(error_code: str) -> bool:
    value = str(error_code or "").strip()
    if not value:
        return False
    return value in MANUAL_ASSIST_ERROR_CODES


def normalize_localization_state(localization_state: str) -> str:
    return str(localization_state or "").strip().lower()


def localization_is_ready(localization_state: str, localization_valid: bool) -> bool:
    state = normalize_localization_state(localization_state)
    return bool(bool(localization_valid) and state in LOCALIZATION_READY_STATES)


def localization_is_transitioning(localization_state: str) -> bool:
    state = normalize_localization_state(localization_state)
    return state in LOCALIZATION_TRANSITION_STATES


def localization_is_degraded(localization_state: str) -> bool:
    state = normalize_localization_state(localization_state)
    return state in LOCALIZATION_DEGRADED_STATES


def localization_needs_manual_assist(localization_state: str) -> bool:
    state = normalize_localization_state(localization_state)
    return state in LOCALIZATION_MANUAL_ASSIST_STATES


def derive_localization_status(
    *,
    localization_state: str,
    localization_valid: bool,
    tracked_pose_age_s: float = -1.0,
    degraded_timeout_s: float = 0.0,
) -> Tuple[str, bool]:
    state = normalize_localization_state(localization_state)
    valid = bool(localization_valid)
    try:
        tracked_pose_age = float(tracked_pose_age_s)
    except Exception:
        tracked_pose_age = -1.0
    try:
        degraded_timeout = float(degraded_timeout_s)
    except Exception:
        degraded_timeout = 0.0

    if localization_is_ready(state, valid) and degraded_timeout > 0.0:
        if tracked_pose_age < 0.0 or tracked_pose_age > degraded_timeout:
            return "degraded", False
    return state, valid


def compute_task_ready(
    *,
    runtime_mode="",
    runtime_map_match=False,
    localization_state="",
    localization_valid=False,
    odometry_valid=True,
    task_running=False,
    mapping_session_active=False,
    manual_assist_required=False,
    busy=False,
):
    return bool(
        str(runtime_mode or "").strip() == "localization"
        and bool(runtime_map_match)
        and localization_is_ready(localization_state, localization_valid)
        and bool(odometry_valid)
        and (not bool(task_running))
        and (not bool(mapping_session_active))
        and (not bool(manual_assist_required))
        and (not bool(busy))
    )


def compute_idle_workflow_state(
    *,
    manual_assist_required=False,
    mapping_session_active=False,
    task_ready=False,
    localization_state="",
):
    normalized_state = normalize_localization_state(localization_state)
    if bool(manual_assist_required) or localization_needs_manual_assist(normalized_state):
        return "MANUAL_ASSIST_REQUIRED"
    if bool(mapping_session_active):
        return "MAPPING"
    if bool(task_ready):
        return "LOCALIZED"
    if localization_is_degraded(normalized_state):
        return "DEGRADED"
    if localization_is_transitioning(normalized_state):
        return "LOCALIZING"
    return "IDLE"


def compute_effective_workflow_state(
    *,
    current_workflow_state="",
    busy=False,
    manual_assist_required=False,
    mapping_session_active=False,
    task_ready=False,
    localization_state="",
):
    if bool(busy):
        return str(current_workflow_state or "IDLE").strip() or "IDLE"
    return compute_idle_workflow_state(
        manual_assist_required=manual_assist_required,
        mapping_session_active=mapping_session_active,
        task_ready=task_ready,
        localization_state=localization_state,
    )

# -*- coding: utf-8 -*-

"""Shared gate / readiness message builders used by SLAM state and system readiness."""

from __future__ import annotations


_MANUAL_ASSIST_RETRY_ACTION_BY_OPERATION = {
    "prepare_for_task": "prepare_for_task",
    "switch_map_and_localize": "switch_map_and_localize",
    "relocalize": "relocalize",
    "verify_map_revision": "verify_map_revision",
    "activate_map_revision": "activate_map_revision",
}


def no_current_active_map_selected_message() -> str:
    return "no current active map selected"


def runtime_map_identity_unavailable_message() -> str:
    return "runtime /map identity unavailable"


def runtime_map_mismatch_reason(
    *,
    expected_label: str = "active map",
    active_revision_id: str = "",
    active_map_name: str = "",
    active_map_id: str = "",
    active_map_md5: str = "",
    runtime_revision_id: str = "",
    runtime_map_name: str = "",
    runtime_map_id: str = "",
    runtime_map_md5: str = "",
) -> str:
    active_revision_id = str(active_revision_id or "").strip()
    active_map_name = str(active_map_name or "").strip()
    active_map_id = str(active_map_id or "").strip()
    active_map_md5 = str(active_map_md5 or "").strip()
    expected_label = str(expected_label or "").strip() or "active map"
    runtime_revision_id = str(runtime_revision_id or "").strip()
    runtime_map_name = str(runtime_map_name or "").strip()
    runtime_map_id = str(runtime_map_id or "").strip()
    runtime_map_md5 = str(runtime_map_md5 or "").strip()

    if active_revision_id and runtime_revision_id and runtime_revision_id != active_revision_id:
        return "runtime revision %s != %s revision %s" % (
            runtime_revision_id,
            expected_label,
            active_revision_id,
        )
    if active_map_name and runtime_map_name and runtime_map_name != active_map_name:
        return "runtime map_name %s != %s %s" % (runtime_map_name, expected_label, active_map_name)
    if active_map_id and runtime_map_id and runtime_map_id != active_map_id:
        return "runtime map_id %s != %s_id %s" % (runtime_map_id, expected_label, active_map_id)
    if active_map_md5 and runtime_map_md5 and runtime_map_md5 != active_map_md5:
        return "runtime map_md5 %s != %s_md5 %s" % (runtime_map_md5, expected_label, active_map_md5)
    return ""


def odometry_not_ready_message(error_code: str, message: str) -> str:
    code = str(error_code or "").strip() or "-"
    text = str(message or "").strip() or "odom invalid"
    return "odometry not ready: code=%s message=%s" % (code, text)


def manual_assist_scope_label(
    *,
    map_name: str = "",
    map_revision_id: str = "",
) -> str:
    normalized_map_name = str(map_name or "").strip()
    normalized_revision_id = str(map_revision_id or "").strip()
    if normalized_map_name and normalized_revision_id:
        return "map=%s revision=%s" % (normalized_map_name, normalized_revision_id)
    if normalized_revision_id:
        return "revision=%s" % normalized_revision_id
    if normalized_map_name:
        return "map=%s" % normalized_map_name
    return ""


def manual_assist_recovery_message(
    *,
    map_name: str = "",
    map_revision_id: str = "",
    retry_action: str = "prepare_for_task",
) -> str:
    scope = manual_assist_scope_label(map_name=map_name, map_revision_id=map_revision_id)
    action = str(retry_action or "").strip() or "prepare_for_task"
    scope_prefix = (" for %s" % scope) if scope else ""
    return (
        "manual assist required%s; place robot at a known anchor point, align heading, "
        "provide initial pose, then retry %s"
    ) % (scope_prefix, action)


def resolve_manual_assist_retry_action(
    *,
    retry_action: str = "",
    operation_name: str = "",
    default_action: str = "prepare_for_task",
) -> str:
    normalized_retry_action = str(retry_action or "").strip()
    if normalized_retry_action:
        return normalized_retry_action
    normalized_operation_name = str(operation_name or "").strip()
    if normalized_operation_name:
        return str(
            _MANUAL_ASSIST_RETRY_ACTION_BY_OPERATION.get(
                normalized_operation_name,
                normalized_operation_name,
            )
        ).strip()
    return str(default_action or "prepare_for_task").strip() or "prepare_for_task"


def manual_assist_metadata(
    *,
    required: bool,
    map_name: str = "",
    map_revision_id: str = "",
    retry_action: str = "",
    operation_name: str = "",
    default_action: str = "prepare_for_task",
):
    if not bool(required):
        return {
            "map_name": "",
            "map_revision_id": "",
            "retry_action": "",
            "guidance": "",
        }
    normalized_map_name = str(map_name or "").strip()
    normalized_revision_id = str(map_revision_id or "").strip()
    normalized_retry_action = resolve_manual_assist_retry_action(
        retry_action=retry_action,
        operation_name=operation_name,
        default_action=default_action,
    )
    return {
        "map_name": normalized_map_name,
        "map_revision_id": normalized_revision_id,
        "retry_action": normalized_retry_action,
        "guidance": manual_assist_recovery_message(
            map_name=normalized_map_name,
            map_revision_id=normalized_revision_id,
            retry_action=normalized_retry_action,
        ),
    }


def runtime_localization_not_ready_message(
    localization_state: str,
    localization_valid: bool,
    *,
    map_name: str = "",
    map_revision_id: str = "",
    retry_action: str = "prepare_for_task",
) -> str:
    state = str(localization_state or "").strip() or "-"
    valid = str(bool(localization_valid)).lower()
    base = "runtime localization not ready: state=%s valid=%s" % (state, valid)
    normalized_state = state.lower()
    if normalized_state == "stabilizing":
        return "%s; wait until localization stabilizes before start/resume" % base
    if normalized_state == "manual_assist_required":
        return "%s; %s" % (
            base,
            manual_assist_recovery_message(
                map_name=map_name,
                map_revision_id=map_revision_id,
                retry_action=retry_action,
            ),
        )
    if normalized_state == "degraded":
        return "%s; relocalize before start/resume" % base
    return base


def runtime_map_switch_required_before_task_message(
    *,
    selected_revision_id: str = "",
    selected_map_name: str = "",
    selected_map_id: str = "",
    selected_map_md5: str = "",
    runtime_revision_id: str = "",
    runtime_map_name: str = "",
    runtime_map_id: str = "",
    runtime_map_md5: str = "",
) -> str:
    mismatch = runtime_map_mismatch_reason(
        expected_label="selected map",
        active_revision_id=selected_revision_id,
        active_map_name=selected_map_name,
        active_map_id=selected_map_id,
        active_map_md5=selected_map_md5,
        runtime_revision_id=runtime_revision_id,
        runtime_map_name=runtime_map_name,
        runtime_map_id=runtime_map_id,
        runtime_map_md5=runtime_map_md5,
    )
    base = mismatch or runtime_map_identity_unavailable_message()
    return "%s; complete SLAM switch_map_and_localize or prepare_for_task before start/resume" % base


def runtime_localization_relocalize_required_before_task_message(
    localization_state: str,
    localization_valid: bool,
) -> str:
    return "%s; relocalize before start/resume" % runtime_localization_not_ready_message(
        localization_state,
        localization_valid,
    )


def selected_map_does_not_match_requested_map_message(
    selected_map_name: str,
    requested_map_name: str,
    *,
    selected_revision_id: str = "",
    requested_revision_id: str = "",
) -> str:
    selected_revision_id = str(selected_revision_id or "").strip()
    requested_revision_id = str(requested_revision_id or "").strip()
    if requested_revision_id and not selected_revision_id:
        return "active selected map revision unavailable for requested map revision %s" % (
            requested_revision_id,
        )
    if selected_revision_id and requested_revision_id and selected_revision_id != requested_revision_id:
        return "active selected map revision %s != requested map revision %s" % (
            selected_revision_id,
            requested_revision_id,
        )
    return "active selected map %s != requested map %s" % (
        str(selected_map_name or "").strip() or "-",
        str(requested_map_name or "").strip() or "-",
    )

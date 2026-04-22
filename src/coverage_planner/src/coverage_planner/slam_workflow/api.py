# -*- coding: utf-8 -*-

"""Pure workflow helpers shared by formal SLAM backend nodes."""

from __future__ import annotations

from dataclasses import dataclass

from coverage_planner.slam_workflow_semantics import (
    compute_effective_workflow_state,
    compute_task_ready,
    is_manual_assist_error_code,
    localization_is_degraded,
    localization_is_transitioning,
    localization_needs_manual_assist,
    normalize_localization_state,
)


START_MAPPING = 3
SAVE_MAPPING = 4
STOP_MAPPING = 5
PREPARE_FOR_TASK = 6
SWITCH_MAP_AND_LOCALIZE = 7
RELOCALIZE = 8
VERIFY_MAP_REVISION = 9
ACTIVATE_MAP_REVISION = 10

RUNTIME_RESTART_LOCALIZATION = 1
RUNTIME_START_MAPPING = 2
RUNTIME_SAVE_MAPPING = 3
RUNTIME_STOP_MAPPING = 4
RUNTIME_VERIFY_MAP_REVISION = 21
RUNTIME_ACTIVATE_MAP_REVISION = 22

SUPPORTED_SUBMIT_OPERATIONS = frozenset(
    {
        START_MAPPING,
        SAVE_MAPPING,
        STOP_MAPPING,
        PREPARE_FOR_TASK,
        SWITCH_MAP_AND_LOCALIZE,
        RELOCALIZE,
        VERIFY_MAP_REVISION,
        ACTIVATE_MAP_REVISION,
    }
)

ASSET_MUST_EXIST_OPERATIONS = frozenset(
    {
        STOP_MAPPING,
        PREPARE_FOR_TASK,
        SWITCH_MAP_AND_LOCALIZE,
        RELOCALIZE,
        VERIFY_MAP_REVISION,
        ACTIVATE_MAP_REVISION,
    }
)
ASSET_MUST_NOT_EXIST_OPERATIONS = frozenset()
PATH_CONFLICT_CHECK_OPERATIONS = frozenset()

_OPERATION_NAME_BY_ID = {
    START_MAPPING: "start_mapping",
    SAVE_MAPPING: "save_mapping",
    STOP_MAPPING: "stop_mapping",
    PREPARE_FOR_TASK: "prepare_for_task",
    SWITCH_MAP_AND_LOCALIZE: "switch_map_and_localize",
    RELOCALIZE: "relocalize",
    VERIFY_MAP_REVISION: "verify_map_revision",
    ACTIVATE_MAP_REVISION: "activate_map_revision",
}

_RUNNING_PHASE_BY_OPERATION = {
    START_MAPPING: "starting_mapping",
    SAVE_MAPPING: "saving_mapping",
    STOP_MAPPING: "stopping_mapping",
    PREPARE_FOR_TASK: "preparing_for_task",
    SWITCH_MAP_AND_LOCALIZE: "switching_map",
    RELOCALIZE: "localizing",
    VERIFY_MAP_REVISION: "verifying_map_revision",
    ACTIVATE_MAP_REVISION: "activating_map_revision",
}

_WORKFLOW_STATE_HINT_BY_OPERATION_NAME = {
    "prepare_for_task": "LOCALIZING",
    "relocalize": "LOCALIZING",
    "switch_map_and_localize": "SWITCHING_MAP",
    "verify_map_revision": "LOCALIZING",
    "activate_map_revision": "SWITCHING_MAP",
    "start_mapping": "MAPPING",
    "save_mapping": "MAPPING",
    "stop_mapping": "LOCALIZING",
}


@dataclass(frozen=True)
class WorkflowProjection:
    runtime_mode: str
    workflow_state: str
    workflow_phase: str
    busy: bool
    runtime_map_match: bool
    mapping_session_active: bool
    task_ready: bool
    manual_assist_required: bool
    progress_text: str
    blocking_reason: str


@dataclass(frozen=True)
class SubmitValidationContext:
    active_map_name: str = ""
    current_mode: str = ""
    task_running: bool = False
    can_switch_map_and_localize: bool = False
    can_relocalize: bool = False
    can_verify_map_revision: bool = False
    can_activate_map_revision: bool = False
    can_start_mapping: bool = False
    can_save_mapping: bool = False
    can_stop_mapping: bool = False
    localization_backend_available: bool = False
    odometry_valid: bool = False


@dataclass(frozen=True)
class SubmitValidationResult:
    effective_map_name: str
    error_code: str = ""
    message: str = ""

    @property
    def ok(self) -> bool:
        return not bool(self.error_code)


def operation_name(operation: int) -> str:
    try:
        normalized = int(operation)
    except Exception:
        normalized = 0
    return str(_OPERATION_NAME_BY_ID.get(normalized, "unknown")).strip()


def normalize_map_name(map_name: str) -> str:
    value = str(map_name or "").strip()
    if value.endswith(".pbstream"):
        value = value[:-len(".pbstream")]
    return str(value or "").strip()


def running_phase_for_operation(operation: int) -> str:
    try:
        normalized = int(operation)
    except Exception:
        normalized = 0
    return str(_RUNNING_PHASE_BY_OPERATION.get(normalized, "running")).strip()


def submit_to_runtime_operation(operation: int) -> int:
    mapping = {
        START_MAPPING: RUNTIME_START_MAPPING,
        SAVE_MAPPING: RUNTIME_SAVE_MAPPING,
        STOP_MAPPING: RUNTIME_STOP_MAPPING,
        PREPARE_FOR_TASK: PREPARE_FOR_TASK,
        SWITCH_MAP_AND_LOCALIZE: SWITCH_MAP_AND_LOCALIZE,
        RELOCALIZE: RELOCALIZE,
        VERIFY_MAP_REVISION: RUNTIME_VERIFY_MAP_REVISION,
        ACTIVATE_MAP_REVISION: RUNTIME_ACTIVATE_MAP_REVISION,
    }
    try:
        normalized = int(operation)
    except Exception:
        normalized = 0
    return int(mapping.get(normalized, 0))


def workflow_state_hint_for_operation(operation_name_text: str) -> str:
    return str(_WORKFLOW_STATE_HINT_BY_OPERATION_NAME.get(str(operation_name_text or "").strip(), "")).strip()


def compute_effective_submit_map_name(
    operation: int,
    *,
    map_name: str = "",
    save_map_name: str = "",
    active_map_name: str = "",
) -> str:
    normalized_map_name = str(map_name or "").strip()
    normalized_save_map_name = str(save_map_name or "").strip()
    normalized_active_map_name = str(active_map_name or "").strip()

    if int(operation) == SAVE_MAPPING:
        return normalized_save_map_name or normalized_map_name
    if int(operation) in (STOP_MAPPING, PREPARE_FOR_TASK, RELOCALIZE):
        return normalized_map_name or normalized_active_map_name
    return normalized_map_name


def project_workflow_state(
    *,
    current_mode: str,
    localization_state: str,
    localization_valid: bool,
    odometry_valid: bool,
    task_running: bool,
    active_map_match: bool,
    active_job_id: str,
    active_job_operation_name: str,
    active_job_phase: str,
    active_job_progress_text: str,
    active_job_manual_assist_required: bool,
    blockers,
    last_error_code: str,
    last_error_message: str,
) -> WorkflowProjection:
    runtime_mode = str(current_mode or "").strip()
    normalized_localization_state = normalize_localization_state(localization_state)
    runtime_map_match = bool(active_map_match)
    mapping_session_active = runtime_mode == "mapping"
    busy = bool(str(active_job_id or "").strip())
    manual_assist_required = bool(
        bool(active_job_manual_assist_required)
        or is_manual_assist_error_code(str(last_error_code or ""))
        or localization_needs_manual_assist(normalized_localization_state)
    )
    task_ready = compute_task_ready(
        runtime_mode=runtime_mode,
        runtime_map_match=runtime_map_match,
        localization_state=normalized_localization_state,
        localization_valid=localization_valid,
        odometry_valid=odometry_valid,
        task_running=task_running,
        mapping_session_active=mapping_session_active,
        manual_assist_required=manual_assist_required,
        busy=busy,
    )
    workflow_state_hint = workflow_state_hint_for_operation(active_job_operation_name) if busy else ""
    workflow_state = compute_effective_workflow_state(
        current_workflow_state=workflow_state_hint,
        busy=busy,
        manual_assist_required=manual_assist_required,
        mapping_session_active=mapping_session_active,
        task_ready=task_ready,
        localization_state=normalized_localization_state,
    )
    if busy:
        workflow_phase = str(active_job_phase or "running").strip() or "running"
    elif mapping_session_active:
        workflow_phase = "mapping"
    elif manual_assist_required:
        workflow_phase = "manual_assist_required"
    elif task_ready:
        workflow_phase = "ready"
    elif localization_is_degraded(normalized_localization_state):
        workflow_phase = "degraded"
    elif normalized_localization_state == "stabilizing":
        workflow_phase = "stabilizing"
    elif localization_is_transitioning(normalized_localization_state):
        workflow_phase = "localizing"
    else:
        workflow_phase = "idle"
    progress_text = str(active_job_progress_text or last_error_message or "").strip()
    blocking_reason = str((list(blockers or [""])[0]) if blockers else "").strip()
    return WorkflowProjection(
        runtime_mode=runtime_mode,
        workflow_state=workflow_state,
        workflow_phase=workflow_phase,
        busy=bool(busy),
        runtime_map_match=bool(runtime_map_match),
        mapping_session_active=bool(mapping_session_active),
        task_ready=bool(task_ready),
        manual_assist_required=bool(manual_assist_required),
        progress_text=progress_text,
        blocking_reason=blocking_reason,
    )


def validate_submit_request(
    *,
    operation: int,
    context: SubmitValidationContext,
    map_name: str = "",
    save_map_name: str = "",
    map_asset_exists: bool = False,
    map_asset_verified: bool = False,
    asset_path_conflict: str = "",
) -> SubmitValidationResult:
    effective_map_name = compute_effective_submit_map_name(
        operation,
        map_name=map_name,
        save_map_name=save_map_name,
        active_map_name=context.active_map_name,
    )

    if int(operation) not in SUPPORTED_SUBMIT_OPERATIONS:
        return SubmitValidationResult(
            effective_map_name=effective_map_name,
            error_code="unsupported_operation",
            message="unsupported operation=%s" % int(operation),
        )

    if int(operation) == START_MAPPING:
        if not context.can_start_mapping:
            return SubmitValidationResult(effective_map_name, "start_mapping_blocked", "start_mapping is blocked")

    if int(operation) == SAVE_MAPPING:
        if not effective_map_name:
            return SubmitValidationResult(effective_map_name, "map_name_required", "map_name or save_map_name is required for save_mapping")
        if not context.can_save_mapping:
            return SubmitValidationResult(effective_map_name, "save_mapping_blocked", "save_mapping is blocked")

    if int(operation) == STOP_MAPPING:
        if not context.can_stop_mapping:
            return SubmitValidationResult(effective_map_name, "stop_mapping_blocked", "stop_mapping is blocked")
        if effective_map_name and not map_asset_exists:
            return SubmitValidationResult(effective_map_name, "map_asset_not_found", "map asset not found")

    if int(operation) == PREPARE_FOR_TASK:
        if not effective_map_name:
            return SubmitValidationResult(effective_map_name, "active_map_required", "map_name or active map is required for prepare_for_task")
        if context.task_running:
            return SubmitValidationResult(effective_map_name, "task_busy", "prepare_for_task is blocked by running task")
        if str(context.current_mode or "").strip() == "mapping":
            return SubmitValidationResult(effective_map_name, "prepare_for_task_blocked", "prepare_for_task is blocked while runtime is in mapping mode")
        if not context.localization_backend_available:
            return SubmitValidationResult(effective_map_name, "localization_backend_unavailable", "localization backend unavailable")
        if not context.odometry_valid:
            return SubmitValidationResult(effective_map_name, "odometry_not_ready", "prepare_for_task requires odometry ready")
        if not map_asset_exists:
            return SubmitValidationResult(effective_map_name, "map_asset_not_found", "map asset not found")

    if int(operation) == SWITCH_MAP_AND_LOCALIZE:
        if not effective_map_name:
            return SubmitValidationResult(effective_map_name, "map_name_required", "map_name is required for switch_map_and_localize")
        if not context.can_switch_map_and_localize:
            return SubmitValidationResult(effective_map_name, "switch_map_and_localize_blocked", "switch_map_and_localize is blocked")
        if not context.localization_backend_available:
            return SubmitValidationResult(effective_map_name, "localization_backend_unavailable", "localization backend unavailable")
        if not context.odometry_valid:
            return SubmitValidationResult(effective_map_name, "odometry_not_ready", "switch_map_and_localize requires odometry ready")
        if not map_asset_exists:
            return SubmitValidationResult(effective_map_name, "map_asset_not_found", "map asset not found")

    if int(operation) == RELOCALIZE:
        if not effective_map_name:
            return SubmitValidationResult(effective_map_name, "active_map_required", "map_name or active map is required for relocalize")
        if not context.can_relocalize:
            return SubmitValidationResult(effective_map_name, "relocalize_blocked", "relocalize is blocked")
        if context.task_running:
            return SubmitValidationResult(effective_map_name, "task_busy", "relocalize is blocked by running task")
        if str(context.current_mode or "").strip() == "mapping":
            return SubmitValidationResult(effective_map_name, "relocalize_blocked", "relocalize is blocked while runtime is in mapping mode")
        if not context.localization_backend_available:
            return SubmitValidationResult(effective_map_name, "localization_backend_unavailable", "localization backend unavailable")
        if not context.odometry_valid:
            return SubmitValidationResult(effective_map_name, "odometry_not_ready", "relocalize requires odometry ready")
        if not map_asset_exists:
            return SubmitValidationResult(effective_map_name, "map_asset_not_found", "map asset not found")

    if int(operation) == VERIFY_MAP_REVISION:
        if not effective_map_name:
            return SubmitValidationResult(effective_map_name, "map_name_required", "map_name is required for verify_map_revision")
        if not context.can_verify_map_revision:
            return SubmitValidationResult(effective_map_name, "verify_map_revision_blocked", "verify_map_revision is blocked")
        if context.task_running:
            return SubmitValidationResult(effective_map_name, "task_busy", "verify_map_revision is blocked by running task")
        if str(context.current_mode or "").strip() == "mapping":
            return SubmitValidationResult(effective_map_name, "verify_map_revision_blocked", "verify_map_revision is blocked while runtime is in mapping mode")
        if not context.localization_backend_available:
            return SubmitValidationResult(effective_map_name, "localization_backend_unavailable", "localization backend unavailable")
        if not context.odometry_valid:
            return SubmitValidationResult(effective_map_name, "odometry_not_ready", "verify_map_revision requires odometry ready")
        if not map_asset_exists:
            return SubmitValidationResult(effective_map_name, "map_asset_not_found", "map asset not found")

    if int(operation) == ACTIVATE_MAP_REVISION:
        if not effective_map_name:
            return SubmitValidationResult(effective_map_name, "map_name_required", "map_name is required for activate_map_revision")
        if not context.can_activate_map_revision:
            return SubmitValidationResult(effective_map_name, "activate_map_revision_blocked", "activate_map_revision is blocked")
        if context.task_running:
            return SubmitValidationResult(effective_map_name, "task_busy", "activate_map_revision is blocked by running task")
        if str(context.current_mode or "").strip() == "mapping":
            return SubmitValidationResult(effective_map_name, "activate_map_revision_blocked", "activate_map_revision is blocked while runtime is in mapping mode")
        if not context.localization_backend_available:
            return SubmitValidationResult(effective_map_name, "localization_backend_unavailable", "localization backend unavailable")
        if not context.odometry_valid:
            return SubmitValidationResult(effective_map_name, "odometry_not_ready", "activate_map_revision requires odometry ready")
        if not map_asset_exists:
            return SubmitValidationResult(effective_map_name, "map_asset_not_found", "map asset not found")
        if not map_asset_verified:
            return SubmitValidationResult(effective_map_name, "map_revision_not_verified", "map revision must be verified before activation")

    return SubmitValidationResult(effective_map_name=effective_map_name)

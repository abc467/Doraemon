# -*- coding: utf-8 -*-

"""Job execution helpers for the formal SLAM runtime backend."""

from __future__ import annotations

import time
from typing import Any

from coverage_planner.slam_workflow.api import operation_name, running_phase_for_operation
from coverage_planner.slam_workflow_semantics import (
    is_manual_assist_error_code,
    localization_is_ready,
    localization_needs_manual_assist,
)


class CartographerSlamJobRunner:
    def __init__(self, backend: Any):
        self._backend = backend

    def run_job(self, job_id: str):
        backend = self._backend
        snapshot = backend._job_state.get_job_snapshot(job_id)
        if not snapshot:
            return

        robot_id = str(snapshot.get("robot_id") or backend.robot_id)
        operation = int(snapshot.get("operation") or 0)
        runtime_operation = int(snapshot.get("runtime_operation") or 0)
        op_name = str(snapshot.get("operation_name") or operation_name(operation))

        snapshot = backend._job_state.update_job_fields(
            snapshot,
            status="running",
            phase=str(running_phase_for_operation(operation) or "running"),
            progress_0_1=0.1,
            message="%s started" % op_name,
            progress_text="%s started" % op_name,
            started_ts=time.time(),
        )
        snapshot = backend._job_state.publish_job_snapshot(snapshot, sync_runtime=True, update_error=False)
        backend._job_events.job_started(snapshot)

        try:
            with backend._lock:
                resp = backend._service_api.execute_operation(
                    operation=runtime_operation,
                    robot_id=robot_id,
                    map_name=str(snapshot.get("requested_map_name") or ""),
                    map_revision_id=str(snapshot.get("requested_map_revision_id") or ""),
                    set_active=bool(snapshot.get("set_active", False)),
                    description=str(snapshot.get("description") or ""),
                    frame_id=str(snapshot.get("frame_id") or "map"),
                    has_initial_pose=bool(snapshot.get("has_initial_pose", False)),
                    initial_pose_x=float(snapshot.get("initial_pose_x") or 0.0),
                    initial_pose_y=float(snapshot.get("initial_pose_y") or 0.0),
                    initial_pose_yaw=float(snapshot.get("initial_pose_yaw") or 0.0),
                    include_unfinished_submaps=bool(snapshot.get("include_unfinished_submaps", True)),
                    switch_to_localization_after_save=bool(
                        snapshot.get("switch_to_localization_after_save", False)
                    ),
                    relocalize_after_switch=bool(snapshot.get("relocalize_after_switch", False)),
                )
        except Exception as exc:
            resp = backend._service_api.response(
                success=False,
                message=str(exc),
                error_code="unexpected_error",
                operation=runtime_operation,
                map_name=str(snapshot.get("requested_map_name") or ""),
                localization_state="",
                current_mode="",
            )

        result_error_code = str(getattr(resp, "error_code", "") or "")
        result_message = str(getattr(resp, "message", "") or "")
        result_success = bool(getattr(resp, "success", False))
        result_localization_state = str(getattr(resp, "localization_state", "") or "")
        manual_assist_required = bool(
            is_manual_assist_error_code(result_error_code)
            or localization_needs_manual_assist(result_localization_state)
        )
        finished_status = "manual_assist_required" if manual_assist_required else ("succeeded" if result_success else "failed")
        finished_phase = "manual_assist_required" if manual_assist_required else ("done" if result_success else "failed")
        finished_progress_text = result_message if manual_assist_required else ("completed" if result_success else result_message)
        resolved_map_name = str(getattr(resp, "map_name", "") or snapshot.get("requested_map_name") or "")
        response_map_revision_id = str(getattr(resp, "map_revision_id", "") or "").strip()
        runtime_revision_id = ""
        runtime_context = getattr(backend, "_runtime_context", None)
        runtime_revision_getter = getattr(runtime_context, "runtime_map_revision_id", None)
        if callable(runtime_revision_getter):
            try:
                runtime_revision_id = str(runtime_revision_getter() or "").strip()
            except Exception:
                runtime_revision_id = ""
        resolved_map_revision_id = response_map_revision_id or runtime_revision_id
        if not resolved_map_revision_id:
            resolver = getattr(backend._job_state, "resolve_map_revision_id", None)
            if callable(resolver):
                try:
                    resolved_map_revision_id = str(
                        resolver(
                            robot_id=robot_id,
                            map_name=resolved_map_name,
                            allow_active_fallback=not bool(resolved_map_name),
                        )
                        or ""
                    ).strip()
                except Exception:
                    resolved_map_revision_id = ""
        if not resolved_map_revision_id:
            resolved_map_revision_id = str(snapshot.get("requested_map_revision_id") or "").strip()
        runtime_map_match = bool(
            bool(getattr(resp, "success", False))
            and str(getattr(resp, "current_mode", "") or "") == "localization"
            and (
                (
                    resolved_map_revision_id
                    and runtime_revision_id
                    and resolved_map_revision_id == runtime_revision_id
                )
                or (
                    not resolved_map_revision_id
                    and bool(resolved_map_name)
                )
            )
        )

        finished = backend._job_state.update_job_fields(
            snapshot,
            resolved_map_name=resolved_map_name,
            resolved_map_revision_id=resolved_map_revision_id,
            status=finished_status,
            phase=finished_phase,
            progress_0_1=1.0,
            done=True,
            success=result_success,
            error_code=result_error_code,
            message=result_message,
            progress_text=finished_progress_text,
            current_mode=str(getattr(resp, "current_mode", "") or ""),
            localization_state=result_localization_state,
            runtime_map_match=runtime_map_match,
            localization_valid=localization_is_ready(
                result_localization_state,
                result_success,
            ),
            manual_assist_required=manual_assist_required,
            finished_ts=time.time(),
        )
        finished = backend._job_state.publish_job_snapshot(finished, sync_runtime=True, update_error=True)
        backend._job_events.job_finished(finished)

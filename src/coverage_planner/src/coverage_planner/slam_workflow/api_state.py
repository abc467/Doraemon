# -*- coding: utf-8 -*-

"""State projection helpers for the public SLAM API service."""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional, Tuple

import rospy
from cleanrobot_app_msgs.msg import (
    OdometryState as AppOdometryState,
    SlamJobState as AppSlamJobState,
    SlamState as AppSlamState,
)
from cleanrobot_app_msgs.srv import GetSlamStatusResponse as AppGetSlamStatusResponse
from coverage_planner.canonical_contract_types import APP_GET_ODOMETRY_STATUS_SERVICE_TYPE
from coverage_planner.map_identity import ensure_map_identity, get_runtime_map_revision_id, get_runtime_map_scope
from coverage_planner.runtime_gate_messages import (
    active_map_localization_not_ready_message,
    active_map_runtime_unavailable_message,
    manual_assist_metadata,
    no_current_active_map_selected_message,
    odometry_not_ready_message,
    runtime_localization_not_ready_message,
    runtime_map_identity_unavailable_message,
    runtime_map_mismatch_reason,
)
from coverage_planner.slam_workflow.api import project_workflow_state
from coverage_planner.slam_workflow_semantics import derive_localization_status, localization_is_ready


_TASK_IDLE_STATES = ("", "IDLE")
_TASK_NON_RUNNING_EXECUTOR_STATES = (
    "",
    "IDLE",
    "INIT",
    "DONE",
    "FAILED",
    "CANCELED",
    "CANCELLED",
    "ABORTED",
    "STOPPED",
)


def _active_map_matches_runtime(
    *,
    active_map: Dict[str, object],
    runtime_revision_id: str,
    runtime_map_name: str,
    runtime_map_id: str,
    runtime_map_md5: str,
) -> bool:
    active_revision_id = str((active_map or {}).get("revision_id") or "").strip()
    active_map_name = str((active_map or {}).get("map_name") or "").strip()
    active_map_id = str((active_map or {}).get("map_id") or "").strip()
    active_map_md5 = str((active_map or {}).get("map_md5") or "").strip()
    if not (active_map_name or active_revision_id):
        return False
    if active_revision_id and runtime_revision_id:
        return runtime_revision_id == active_revision_id
    if active_map_id and runtime_map_id:
        return active_map_id == runtime_map_id
    if active_map_md5 and runtime_map_md5:
        return active_map_md5 == runtime_map_md5
    if runtime_map_name:
        return runtime_map_name == active_map_name
    return False

class SlamApiStateController:
    def __init__(self, backend: Any):
        self._backend = backend

    def active_job_state(self) -> Optional[AppSlamJobState]:
        backend = self._backend
        msg = backend._job_state_msg
        if msg is None:
            return None
        if not bool(getattr(msg, "job_id", "") or ""):
            return None
        if bool(getattr(msg, "done", False)):
            return None
        return msg

    def get_modes(self) -> Tuple[str, str]:
        backend = self._backend
        runtime_state = backend._runtime_state
        desired = str(rospy.get_param(runtime_state.runtime_param("mode"), "localization")).strip().lower() or "localization"
        current = str(rospy.get_param(runtime_state.runtime_param("current_mode"), desired)).strip().lower() or desired
        if desired not in ("localization", "mapping"):
            desired = "localization"
        if current not in ("localization", "mapping"):
            current = desired
        return desired, current

    def runtime_map_snapshot(self, *, refresh: bool) -> Dict[str, object]:
        backend = self._backend
        map_name = ""
        revision_id = ""
        try:
            map_name, _scope = get_runtime_map_scope()
        except Exception:
            map_name = ""
        try:
            revision_id = get_runtime_map_revision_id(getattr(backend, "runtime_ns", "/cartographer/runtime"))
        except Exception:
            revision_id = ""
        try:
            map_id, map_md5, ok = ensure_map_identity(
                map_topic=backend.map_topic,
                timeout_s=backend.map_identity_timeout_s,
                set_global_params=True,
                set_private_params=False,
                refresh=bool(refresh),
            )
        except Exception:
            map_id, map_md5, ok = "", "", False
        if (not map_name) and revision_id:
            try:
                revision = backend._plan_store.resolve_map_revision(revision_id=revision_id) or {}
            except Exception:
                revision = {}
            if revision:
                map_name = str(revision.get("map_name") or "").strip()
        if (not map_name) and (map_id or map_md5):
            try:
                matches = backend._plan_store.find_map_assets_by_identity(map_id=str(map_id), map_md5=str(map_md5))
            except Exception:
                matches = []
            if len(matches or []) == 1:
                map_name = str((matches[0] or {}).get("map_name") or "").strip()
        return {
            "revision_id": str(revision_id or "").strip(),
            "map_name": str(map_name or "").strip(),
            "map_id": str(map_id or "").strip(),
            "map_md5": str(map_md5 or "").strip(),
            "ok": bool(ok),
        }

    def live_task_state(self) -> Optional[Dict[str, str]]:
        backend = self._backend
        now = time.time()
        if backend._task_state_msg is None:
            return None
        if backend._task_state_ts <= 0.0 or (now - backend._task_state_ts) > backend.task_state_fresh_timeout_s:
            return None
        msg = backend._task_state_msg
        return {
            "mission_state": str(getattr(msg, "mission_state", "") or "IDLE").strip(),
            "phase": str(getattr(msg, "phase", "") or "IDLE").strip(),
            "public_state": str(getattr(msg, "public_state", "") or "IDLE").strip(),
            "executor_state": str(getattr(msg, "executor_state", "") or "").strip(),
            "run_id": str(getattr(msg, "run_id", "") or "").strip(),
        }

    def live_odometry_state(self) -> Optional[AppOdometryState]:
        backend = self._backend
        now = time.time()
        if backend._odometry_state_msg is None:
            return None
        if backend._odometry_state_ts <= 0.0 or (now - backend._odometry_state_ts) > backend.odometry_state_fresh_timeout_s:
            return None
        return backend._odometry_state_msg

    def is_task_running(self, *, runtime, live_task: Optional[Dict[str, str]]) -> bool:
        if live_task:
            mission_state = str(live_task.get("mission_state") or "IDLE").strip().upper()
            public_state = str(live_task.get("public_state") or "IDLE").strip().upper()
            executor_state = str(live_task.get("executor_state") or "").strip().upper()
            if str(live_task.get("run_id") or "").strip():
                return True
            if mission_state not in _TASK_IDLE_STATES:
                return True
            if public_state not in _TASK_IDLE_STATES:
                return True
            if executor_state not in _TASK_NON_RUNNING_EXECUTOR_STATES:
                return True
            return False
        mission_state = str(runtime.mission_state or "IDLE").strip().upper()
        public_state = str(runtime.public_state or "IDLE").strip().upper()
        executor_state = str(runtime.executor_state or "").strip().upper()
        if str(runtime.active_run_id or "").strip():
            return True
        if mission_state not in _TASK_IDLE_STATES:
            return True
        if public_state not in _TASK_IDLE_STATES:
            return True
        if executor_state not in _TASK_NON_RUNNING_EXECUTOR_STATES:
            return True
        return False

    def build_state(self, *, robot_id: str, refresh_map_identity: bool) -> AppSlamState:
        backend = self._backend
        runtime_client = backend._runtime_client
        runtime_state = backend._runtime_state
        runtime = runtime_state.get_runtime_record(robot_id)
        live_task = self.live_task_state()
        active_job = self.active_job_state()
        active_map = backend._plan_store.get_active_map(robot_id=robot_id) or {}
        pending_switch = backend._plan_store.get_pending_map_switch(robot_id=robot_id) or {}
        runtime_map = self.runtime_map_snapshot(refresh=bool(refresh_map_identity))
        desired_mode, current_mode = self.get_modes()

        active_map_name = str(active_map.get("map_name") or "").strip()
        active_revision_id = str(active_map.get("revision_id") or "").strip()
        pending_map_name = str(pending_switch.get("target_map_name") or "").strip()
        pending_map_revision_id = str(pending_switch.get("target_revision_id") or "").strip()
        pending_map_switch_status = str(pending_switch.get("status") or "").strip()
        runtime_revision_id = str(runtime_map.get("revision_id") or "").strip()
        runtime_map_name = str(runtime_map.get("map_name") or "").strip()
        runtime_map_id = str(runtime_map.get("map_id") or "").strip()
        runtime_map_md5 = str(runtime_map.get("map_md5") or "").strip()

        raw_localization_state = str(
            rospy.get_param(runtime_state.runtime_param("localization_state"), str(runtime.localization_state or ""))
            or ""
        ).strip()
        raw_localization_valid = bool(
            rospy.get_param(
                runtime_state.runtime_param("localization_valid"),
                bool(runtime.localization_valid),
            )
        )
        runtime_map_ready = bool(runtime_revision_id or runtime_map_name or runtime_map_id or runtime_map_md5)
        active_map_match = False
        if (active_map_name or active_revision_id) and runtime_map_ready:
            active_map_match = _active_map_matches_runtime(
                active_map=active_map,
                runtime_revision_id=runtime_revision_id,
                runtime_map_name=runtime_map_name,
                runtime_map_id=runtime_map_id,
                runtime_map_md5=runtime_map_md5,
            )

        now = time.time()
        map_age_s = float(max(0.0, now - backend._map_ts)) if backend._map_ts > 0.0 else -1.0
        tracked_pose_age_s = float(max(0.0, now - backend._tracked_pose_ts)) if backend._tracked_pose_ts > 0.0 else -1.0
        map_topic_fresh = bool(backend._map_ts > 0.0 and map_age_s <= backend.map_fresh_timeout_s)
        tracked_pose_fresh = bool(
            backend._tracked_pose_ts > 0.0 and tracked_pose_age_s <= backend.tracked_pose_fresh_timeout_s
        )

        if current_mode == "mapping":
            localization_state = "mapping"
            localization_valid = False
            runtime_revision_id = ""
            runtime_map_name = ""
            runtime_map_id = ""
            runtime_map_md5 = ""
            runtime_map_ready = False
            active_map_match = False
        else:
            localization_state, localization_valid = derive_localization_status(
                localization_state=raw_localization_state,
                localization_valid=raw_localization_valid,
                tracked_pose_age_s=tracked_pose_age_s,
                degraded_timeout_s=float(getattr(backend, "localization_degraded_timeout_s", 0.0)),
            )

        mission_state = str((live_task or {}).get("mission_state") or runtime.mission_state or "IDLE")
        phase = str((live_task or {}).get("phase") or runtime.phase or "IDLE")
        public_state = str((live_task or {}).get("public_state") or runtime.public_state or "IDLE")
        executor_state = str((live_task or {}).get("executor_state") or runtime.executor_state or "")
        task_running = self.is_task_running(runtime=runtime, live_task=live_task)
        odometry_state = self.live_odometry_state()
        active_job_id = str(getattr(active_job, "job_id", "") or "").strip()
        active_job_status = str(getattr(active_job, "status", "") or "").strip()
        active_job_phase = str(getattr(active_job, "phase", "") or "").strip()
        active_job_progress = float(getattr(active_job, "progress_0_1", 0.0) or 0.0)
        active_job_operation_name = str(getattr(active_job, "operation_name", "") or "").strip()
        active_job_progress_text = str(
            getattr(active_job, "progress_text", "") or getattr(active_job, "message", "") or ""
        ).strip()
        manual_assist_map_name = str(
            getattr(active_job, "resolved_map_name", "")
            or getattr(active_job, "requested_map_name", "")
            or pending_map_name
            or active_map_name
            or ""
        ).strip()
        manual_assist_revision_id = str(
            getattr(active_job, "resolved_map_revision_id", "")
            or getattr(active_job, "requested_map_revision_id", "")
            or pending_map_revision_id
            or active_revision_id
            or ""
        ).strip()
        slam_job_running = bool(active_job_id)
        submit_available = runtime_client.runtime_submit_job_available()
        restart_available = runtime_client.restart_backend_available()
        reload_available = runtime_client.mapping_runtime_available()
        save_state_available = runtime_client.save_runtime_available()
        odometry_status_available = bool(
            (odometry_state is not None)
            or runtime_client.service_available(
                backend.odometry_status_service_name,
                APP_GET_ODOMETRY_STATUS_SERVICE_TYPE,
            )
        )
        odometry_valid = bool(getattr(odometry_state, "odom_valid", False)) if odometry_state is not None else False

        blockers: List[str] = []
        warnings: List[str] = []

        if task_running:
            blockers.append(
                "task manager busy: mission=%s phase=%s public=%s"
                % (
                    mission_state,
                    phase,
                    public_state,
                )
            )
        if slam_job_running:
            blockers.append(
                "slam job running: job_id=%s op=%s phase=%s"
                % (
                    active_job_id,
                    str(getattr(active_job, "operation_name", "") or "-"),
                    active_job_phase or "-",
                )
            )
        if not active_map_name:
            warnings.append(no_current_active_map_selected_message())
        if current_mode == "mapping":
            warnings.append("runtime is in mapping mode")
        if current_mode != "mapping" and not runtime_map_ready:
            if active_map_name:
                blockers.append(
                    active_map_runtime_unavailable_message(
                        map_name=active_map_name,
                        map_revision_id=active_revision_id,
                    )
                )
            blockers.append(runtime_map_identity_unavailable_message())
        if current_mode != "mapping" and active_map_name and runtime_map_ready and (not active_map_match):
            blockers.append(
                runtime_map_mismatch_reason(
                    active_revision_id=str(active_map.get("revision_id") or ""),
                    active_map_name=str(active_map.get("map_name") or ""),
                    active_map_id=str(active_map.get("map_id") or ""),
                    active_map_md5=str(active_map.get("map_md5") or ""),
                    runtime_revision_id=runtime_revision_id,
                    runtime_map_name=runtime_map_name,
                    runtime_map_id=runtime_map_id,
                    runtime_map_md5=runtime_map_md5,
                )
                or "runtime map does not match active map"
            )
        localization_ready = localization_is_ready(localization_state, localization_valid)
        if current_mode != "mapping" and (not localization_ready):
            if active_map_name:
                blockers.append(
                    active_map_localization_not_ready_message(
                        localization_state,
                        localization_valid,
                        map_name=manual_assist_map_name or active_map_name,
                        map_revision_id=manual_assist_revision_id or active_revision_id,
                    )
                )
            blockers.append(
                runtime_localization_not_ready_message(
                    localization_state,
                    localization_valid,
                    map_name=manual_assist_map_name,
                    map_revision_id=manual_assist_revision_id,
                    retry_action="prepare_for_task",
                )
            )
        if current_mode != "mapping" and not tracked_pose_fresh:
            warnings.append("tracked_pose stale or missing")
        if not odometry_status_available:
            warnings.append("odometry health state unavailable")
        elif not odometry_valid:
            blockers.append(
                odometry_not_ready_message(
                    str(getattr(odometry_state, "error_code", "") or ""),
                    str(getattr(odometry_state, "message", "") or ""),
                )
            )
            for item in list(getattr(odometry_state, "warnings", []) or []):
                text = str(item or "").strip()
                if text:
                    warnings.append(text)
        if not submit_available:
            warnings.append("slam submit backend unavailable")
        if not restart_available:
            warnings.append("restart localization backend unavailable")
        if not reload_available:
            warnings.append("mapping runtime backend unavailable")
        if not save_state_available:
            warnings.append("mapping save backend unavailable")

        can_switch_map_and_localize = bool(
            submit_available
            and (not task_running)
            and (not slam_job_running)
            and current_mode != "mapping"
            and restart_available
            and odometry_valid
        )
        can_relocalize = bool(
            submit_available
            and
            (not task_running)
            and (not slam_job_running)
            and current_mode != "mapping"
            and restart_available
            and bool(active_map_name)
            and odometry_valid
        )
        can_verify_map_revision = bool(can_switch_map_and_localize)
        can_activate_map_revision = bool(can_switch_map_and_localize)
        can_start_mapping = bool(
            submit_available
            and
            (not task_running)
            and (not slam_job_running)
            and current_mode != "mapping"
            and reload_available
            and odometry_valid
        )
        can_save_mapping = bool(
            submit_available
            and
            (not task_running)
            and (not slam_job_running)
            and current_mode == "mapping"
            and save_state_available
            and map_topic_fresh
        )
        can_stop_mapping = bool(
            submit_available
            and
            (not task_running)
            and (not slam_job_running)
            and current_mode == "mapping"
            and restart_available
            and odometry_valid
        )
        projection = project_workflow_state(
            current_mode=current_mode,
            localization_state=localization_state,
            localization_valid=localization_valid,
            odometry_valid=odometry_valid,
            task_running=task_running,
            active_map_match=active_map_match,
            active_job_id=active_job_id,
            active_job_operation_name=str(getattr(active_job, "operation_name", "") or "").strip(),
            active_job_phase=active_job_phase,
            active_job_progress_text=active_job_progress_text,
            active_job_manual_assist_required=bool(getattr(active_job, "manual_assist_required", False)),
            blockers=blockers,
            last_error_code=str(runtime.last_error_code or ""),
            last_error_message=str(runtime.last_error_msg or ""),
        )
        manual_assist_info = manual_assist_metadata(
            required=bool(projection.manual_assist_required),
            map_name=manual_assist_map_name,
            map_revision_id=manual_assist_revision_id,
            operation_name=active_job_operation_name,
            default_action="prepare_for_task",
        )

        msg = AppSlamState()
        msg.robot_id = str(robot_id or backend.robot_id)
        msg.desired_mode = str(desired_mode or "")
        msg.current_mode = str(current_mode or "")
        msg.active_map_name = active_map_name
        msg.active_map_revision_id = active_revision_id
        msg.active_map_id = str(active_map.get("map_id") or "")
        msg.active_map_md5 = str(active_map.get("map_md5") or "")
        msg.runtime_map_name = runtime_map_name
        msg.runtime_map_revision_id = runtime_revision_id
        msg.runtime_map_id = runtime_map_id
        msg.runtime_map_md5 = runtime_map_md5
        msg.pending_map_name = pending_map_name
        msg.pending_map_revision_id = pending_map_revision_id
        msg.pending_map_switch_status = pending_map_switch_status
        msg.localization_state = str(localization_state or "")
        msg.localization_valid = bool(localization_valid)
        msg.runtime_map_ready = bool(runtime_map_ready)
        msg.active_map_match = bool(active_map_match)
        msg.lifecycle_state = "running_job" if slam_job_running else "steady"
        msg.active_job_id = active_job_id
        msg.active_job_status = active_job_status
        msg.active_job_phase = active_job_phase
        msg.active_job_progress_0_1 = float(active_job_progress)
        msg.map_topic_fresh = bool(map_topic_fresh)
        msg.map_age_s = float(map_age_s)
        msg.tracked_pose_fresh = bool(tracked_pose_fresh)
        msg.tracked_pose_age_s = float(tracked_pose_age_s)
        msg.mission_state = mission_state
        msg.phase = phase
        msg.public_state = public_state
        msg.executor_state = executor_state
        msg.task_running = bool(task_running)
        msg.localization_backend_available = bool(restart_available)
        msg.runtime_reload_service_available = bool(reload_available)
        msg.runtime_save_state_service_available = bool(save_state_available)
        msg.can_switch_map_and_localize = bool(can_switch_map_and_localize)
        msg.can_relocalize = bool(can_relocalize)
        msg.can_verify_map_revision = bool(can_verify_map_revision)
        msg.can_activate_map_revision = bool(can_activate_map_revision)
        msg.can_start_mapping = bool(can_start_mapping)
        msg.can_save_mapping = bool(can_save_mapping)
        msg.can_stop_mapping = bool(can_stop_mapping)
        msg.last_error_code = str(runtime.last_error_code or "")
        msg.last_error_msg = str(runtime.last_error_msg or "")
        msg.blocking_reasons = list(blockers)
        msg.warnings = list(warnings)
        msg.stamp = rospy.Time.now()
        msg.runtime_mode = projection.runtime_mode
        msg.workflow_state = projection.workflow_state
        msg.workflow_phase = projection.workflow_phase
        msg.busy = bool(projection.busy)
        msg.runtime_map_match = bool(projection.runtime_map_match)
        msg.mapping_session_active = bool(projection.mapping_session_active)
        msg.task_ready = bool(projection.task_ready)
        msg.manual_assist_required = bool(projection.manual_assist_required)
        msg.progress_text = projection.progress_text
        msg.blocking_reason = projection.blocking_reason
        msg.last_error_message = str(runtime.last_error_msg or "")
        msg.manual_assist_map_name = str(manual_assist_info.get("map_name") or "")
        msg.manual_assist_map_revision_id = str(manual_assist_info.get("map_revision_id") or "")
        msg.manual_assist_retry_action = str(manual_assist_info.get("retry_action") or "")
        msg.manual_assist_guidance = str(manual_assist_info.get("guidance") or "")
        return msg

    def handle_get_status_app(self, req):
        backend = self._backend
        robot_id = str(req.robot_id or backend.robot_id).strip() or backend.robot_id
        return AppGetSlamStatusResponse(
            success=True,
            message="ok",
            state=self.build_state(robot_id=robot_id, refresh_map_identity=bool(req.refresh_map_identity)),
        )

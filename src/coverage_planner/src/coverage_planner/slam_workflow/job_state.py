# -*- coding: utf-8 -*-

"""Job snapshot, persistence, and projection helpers for the formal SLAM backend."""

from __future__ import annotations

import threading
import time
import uuid
from typing import Any, Dict, Optional

import rospy
from cleanrobot_app_msgs.msg import SlamJobState

from coverage_planner.ops_store.store import SlamJobRecord
from coverage_planner.runtime_gate_messages import manual_assist_metadata
from coverage_planner.slam_workflow.api import operation_name, submit_to_runtime_operation
from coverage_planner.slam_workflow_semantics import is_manual_assist_error_code, localization_is_ready


class CartographerSlamJobController:
    def __init__(self, backend: Any, *, events=None):
        self._backend = backend
        self._events = events
        self._job_lock = threading.Lock()
        self._jobs: Dict[str, Dict[str, object]] = {}
        self._job_order = []
        self._active_job_id = ""

    def job_to_msg(self, job: Optional[Dict[str, object]]) -> SlamJobState:
        backend = self._backend
        to_ros_time = backend._runtime_context.to_ros_time
        data = dict(job or {})
        msg = SlamJobState()
        status = str(data.get("status") or "")
        phase = str(data.get("phase") or "")
        progress_0_1 = float(data.get("progress_0_1") or 0.0)
        success = bool(data.get("success", False))
        error_code = str(data.get("error_code") or "")
        message = str(data.get("message") or "")
        progress_text = str(data.get("progress_text") or message or "")
        manual_assist_required = bool(data.get("manual_assist_required", False))
        manual_assist_info = manual_assist_metadata(
            required=manual_assist_required,
            map_name=str(data.get("resolved_map_name") or data.get("requested_map_name") or ""),
            map_revision_id=str(
                data.get("resolved_map_revision_id")
                or data.get("requested_map_revision_id")
                or ""
            ),
            operation_name=str(data.get("operation_name") or operation_name(int(data.get("operation") or 0))),
            default_action="prepare_for_task",
        )
        msg.job_id = str(data.get("job_id") or "")
        msg.robot_id = str(data.get("robot_id") or backend.robot_id)
        msg.operation = int(data.get("operation") or 0)
        msg.operation_name = str(data.get("operation_name") or operation_name(msg.operation))
        msg.requested_map_name = str(data.get("requested_map_name") or "")
        msg.requested_map_revision_id = str(data.get("requested_map_revision_id") or "")
        msg.resolved_map_name = str(data.get("resolved_map_name") or "")
        msg.resolved_map_revision_id = str(data.get("resolved_map_revision_id") or "")
        msg.set_active = bool(data.get("set_active", False))
        msg.description = str(data.get("description") or "")
        msg.status = status
        msg.phase = phase
        msg.progress_0_1 = progress_0_1
        msg.done = bool(data.get("done", False))
        msg.success = success
        msg.error_code = error_code
        msg.message = message
        msg.current_mode = str(data.get("current_mode") or "")
        msg.localization_state = str(data.get("localization_state") or "")
        msg.created_at = to_ros_time(data.get("created_ts"))
        msg.started_at = to_ros_time(data.get("started_ts"))
        msg.finished_at = to_ros_time(data.get("finished_ts"))
        msg.updated_at = to_ros_time(data.get("updated_ts"))
        msg.job_state = "manual_assist_required" if manual_assist_required else status
        msg.workflow_phase = phase
        msg.progress_percent = progress_0_1 * 100.0
        msg.progress_text = progress_text
        msg.result_success = success
        msg.result_code = "ok" if success else error_code
        msg.result_message = message
        msg.runtime_map_match = bool(data.get("runtime_map_match", False))
        msg.localization_valid = bool(data.get("localization_valid", False))
        msg.manual_assist_required = manual_assist_required
        msg.manual_assist_map_name = str(manual_assist_info.get("map_name") or "")
        msg.manual_assist_map_revision_id = str(manual_assist_info.get("map_revision_id") or "")
        msg.manual_assist_retry_action = str(manual_assist_info.get("retry_action") or "")
        msg.manual_assist_guidance = str(manual_assist_info.get("guidance") or "")
        return msg

    def snapshot_to_record(self, job: Dict[str, object]) -> SlamJobRecord:
        backend = self._backend
        data = dict(job or {})
        return SlamJobRecord(
            job_id=str(data.get("job_id") or "").strip(),
            robot_id=str(data.get("robot_id") or backend.robot_id).strip(),
            operation=int(data.get("operation") or 0),
            operation_name=str(data.get("operation_name") or "").strip(),
            requested_map_name=str(data.get("requested_map_name") or "").strip(),
            requested_map_revision_id=str(data.get("requested_map_revision_id") or "").strip(),
            resolved_map_name=str(data.get("resolved_map_name") or "").strip(),
            resolved_map_revision_id=str(data.get("resolved_map_revision_id") or "").strip(),
            set_active=bool(data.get("set_active", False)),
            description=str(data.get("description") or "").strip(),
            status=str(data.get("status") or "").strip(),
            phase=str(data.get("phase") or "").strip(),
            progress_0_1=float(data.get("progress_0_1") or 0.0),
            done=bool(data.get("done", False)),
            success=bool(data.get("success", False)),
            error_code=str(data.get("error_code") or "").strip(),
            message=str(data.get("message") or "").strip(),
            current_mode=str(data.get("current_mode") or "").strip(),
            localization_state=str(data.get("localization_state") or "").strip(),
            created_ts=float(data.get("created_ts") or 0.0),
            started_ts=float(data.get("started_ts") or 0.0),
            finished_ts=float(data.get("finished_ts") or 0.0),
            updated_ts=float(data.get("updated_ts") or 0.0),
        )

    def record_to_snapshot(self, record: Optional[SlamJobRecord]) -> Optional[Dict[str, object]]:
        if record is None:
            return None
        return {
            "job_id": str(record.job_id or ""),
            "robot_id": str(record.robot_id or self._backend.robot_id),
            "operation": int(record.operation or 0),
            "runtime_operation": int(submit_to_runtime_operation(record.operation)),
            "operation_name": str(record.operation_name or operation_name(record.operation)),
            "requested_map_name": str(record.requested_map_name or ""),
            "requested_map_revision_id": str(record.requested_map_revision_id or ""),
            "resolved_map_name": str(record.resolved_map_name or ""),
            "resolved_map_revision_id": str(record.resolved_map_revision_id or ""),
            "set_active": bool(record.set_active),
            "description": str(record.description or ""),
            "status": str(record.status or ""),
            "phase": str(record.phase or ""),
            "progress_0_1": float(record.progress_0_1 or 0.0),
            "done": bool(record.done),
            "success": bool(record.success),
            "error_code": str(record.error_code or ""),
            "message": str(record.message or ""),
            "current_mode": str(record.current_mode or ""),
            "localization_state": str(record.localization_state or ""),
            "created_ts": float(record.created_ts or 0.0),
            "started_ts": float(record.started_ts or 0.0),
            "finished_ts": float(record.finished_ts or 0.0),
            "updated_ts": float(record.updated_ts or 0.0),
            "progress_text": str(record.message or ""),
            "runtime_map_match": False,
            "localization_valid": localization_is_ready(
                str(record.localization_state or "").strip(),
                True,
            ),
            "manual_assist_required": is_manual_assist_error_code(str(record.error_code or "")),
        }

    def remember_job_locked(self, job: Dict[str, object]):
        backend = self._backend
        job_id = str(job.get("job_id") or "").strip()
        if not job_id:
            return
        self._jobs[job_id] = dict(job)
        if job_id in self._job_order:
            self._job_order.remove(job_id)
        self._job_order.append(job_id)
        while len(self._job_order) > backend.max_job_history:
            oldest = str(self._job_order[0] or "").strip()
            if oldest and oldest != self._active_job_id:
                self._job_order.pop(0)
                self._jobs.pop(oldest, None)
                continue
            break

    def store_job(self, job: Dict[str, object]) -> Dict[str, object]:
        snapshot = dict(job or {})
        with self._job_lock:
            self.remember_job_locked(snapshot)
            job_id = str(snapshot.get("job_id") or "").strip()
            if job_id:
                if bool(snapshot.get("done", False)):
                    if self._active_job_id == job_id:
                        self._active_job_id = ""
                else:
                    self._active_job_id = job_id
        return snapshot

    def get_job_snapshot(self, job_id: str = "") -> Optional[Dict[str, object]]:
        backend = self._backend
        with self._job_lock:
            target_id = str(job_id or "").strip()
            if not target_id:
                if self._active_job_id:
                    target_id = self._active_job_id
                elif self._job_order:
                    target_id = str(self._job_order[-1] or "").strip()
            if not target_id:
                latest = backend._ops.get_latest_slam_job(robot_id=backend.robot_id)
                return self.record_to_snapshot(latest)
            job = self._jobs.get(target_id)
            if job:
                return dict(job)
        record = backend._ops.get_slam_job(target_id)
        return self.record_to_snapshot(record)

    def job_running(self) -> bool:
        with self._job_lock:
            if not self._active_job_id:
                return False
            job = dict(self._jobs.get(self._active_job_id) or {})
            return bool(job) and (not bool(job.get("done", False)))

    def publish_job_snapshot(
        self,
        job: Dict[str, object],
        *,
        sync_runtime: bool = True,
        update_error: bool = False,
    ) -> Dict[str, object]:
        backend = self._backend
        runtime_state = backend._runtime_state
        snapshot = self.store_job(job)
        backend._ops.upsert_slam_job(self.snapshot_to_record(snapshot))
        robot_id = str(snapshot.get("robot_id") or backend.robot_id)
        active_job_id = "" if bool(snapshot.get("done", False)) else str(snapshot.get("job_id") or "")
        last_error_code = None
        last_error_msg = None
        runtime_map_revision_id = None
        if update_error:
            if bool(snapshot.get("success", False)):
                last_error_code = ""
                last_error_msg = ""
            else:
                last_error_code = str(snapshot.get("error_code") or "")
                last_error_msg = str(snapshot.get("message") or "")
        if (
            bool(snapshot.get("done", False))
            and bool(snapshot.get("success", False))
            and str(snapshot.get("current_mode") or "").strip() == "localization"
        ):
            runtime_map_revision_id = str(
                snapshot.get("resolved_map_revision_id")
                or snapshot.get("requested_map_revision_id")
                or ""
            ).strip()
        if sync_runtime:
            runtime_state.update_runtime_state(
                robot_id=robot_id,
                active_job_id=active_job_id,
                map_revision_id=runtime_map_revision_id,
                last_error_code=last_error_code,
                last_error_msg=last_error_msg,
            )
        backend._job_state_pub.publish(self.job_to_msg(snapshot))
        return snapshot

    def restore_jobs_from_store(self):
        backend = self._backend
        runtime_param = backend._runtime_context.runtime_param
        restored = list(backend._ops.list_recent_slam_jobs(robot_id=backend.robot_id, limit=backend.max_job_history))
        latest_snapshot = None
        if restored:
            for record in reversed(restored):
                snapshot = self.record_to_snapshot(record)
                if not snapshot:
                    continue
                if not bool(snapshot.get("done", False)):
                    now = time.time()
                    snapshot.update(
                        {
                            "status": "failed",
                            "phase": "interrupted",
                            "done": True,
                            "success": False,
                            "error_code": "runtime_manager_restarted",
                            "message": "slam runtime manager restarted before job completed",
                            "current_mode": str(rospy.get_param(runtime_param("current_mode"), "") or "").strip(),
                            "localization_state": str(
                                rospy.get_param(runtime_param("localization_state"), "") or ""
                            ).strip(),
                            "finished_ts": float(snapshot.get("finished_ts") or now),
                            "updated_ts": now,
                        }
                    )
                    backend._ops.upsert_slam_job(self.snapshot_to_record(snapshot))
                    if self._events is not None:
                        self._events.job_interrupted_on_restore(snapshot)
                self.store_job(snapshot)
                latest_snapshot = dict(snapshot)
            backend._runtime_state.update_runtime_state(robot_id=backend.robot_id, active_job_id="")
        if latest_snapshot:
            backend._job_state_pub.publish(self.job_to_msg(latest_snapshot))
        else:
            backend._job_state_pub.publish(SlamJobState())

    def make_job_record(
        self,
        *,
        operation: int,
        robot_id: str,
        map_name: str,
        map_revision_id: str = "",
        set_active: bool,
        description: str,
        frame_id: str = "map",
        has_initial_pose: bool = False,
        initial_pose_x: float = 0.0,
        initial_pose_y: float = 0.0,
        initial_pose_yaw: float = 0.0,
        include_unfinished_submaps: bool = True,
        switch_to_localization_after_save: bool = False,
        relocalize_after_switch: bool = False,
    ) -> Dict[str, object]:
        backend = self._backend
        now = time.time()
        runtime_param = backend._runtime_context.runtime_param
        job_id = "slam_job_%s_%s" % (
            time.strftime("%Y%m%d_%H%M%S", time.localtime(now)),
            uuid.uuid4().hex[:8],
        )
        requested_map_name = str(map_name or "")
        explicit_revision_id = str(map_revision_id or "").strip()
        if explicit_revision_id and not requested_map_name:
            try:
                revision = backend._plan_store.resolve_map_revision(
                    revision_id=explicit_revision_id,
                    robot_id=str(robot_id or backend.robot_id),
                ) or {}
            except Exception:
                revision = {}
            requested_map_name = str(revision.get("map_name") or "")
        current_mode = str(rospy.get_param(runtime_param("current_mode"), "") or "").strip()
        localization_state = str(rospy.get_param(runtime_param("localization_state"), "") or "").strip()
        requested_map_revision_id = explicit_revision_id or self.resolve_map_revision_id(
            robot_id=str(robot_id or backend.robot_id),
            map_name=str(requested_map_name or ""),
            allow_active_fallback=not bool(str(requested_map_name or "").strip()),
        )
        return {
            "job_id": job_id,
            "robot_id": str(robot_id or backend.robot_id),
            "operation": int(operation),
            "runtime_operation": int(submit_to_runtime_operation(operation)),
            "operation_name": operation_name(operation),
            "requested_map_name": requested_map_name,
            "requested_map_revision_id": requested_map_revision_id,
            "resolved_map_name": requested_map_name,
            "resolved_map_revision_id": requested_map_revision_id,
            "set_active": bool(set_active),
            "description": str(description or ""),
            "frame_id": str(frame_id or "map").strip() or "map",
            "has_initial_pose": bool(has_initial_pose),
            "initial_pose_x": float(initial_pose_x or 0.0),
            "initial_pose_y": float(initial_pose_y or 0.0),
            "initial_pose_yaw": float(initial_pose_yaw or 0.0),
            "include_unfinished_submaps": bool(include_unfinished_submaps),
            "switch_to_localization_after_save": bool(switch_to_localization_after_save),
            "relocalize_after_switch": bool(relocalize_after_switch),
            "status": "queued",
            "phase": "accepted",
            "progress_0_1": 0.0,
            "done": False,
            "success": False,
            "error_code": "",
            "message": "accepted",
            "progress_text": "accepted",
            "current_mode": current_mode,
            "localization_state": localization_state,
            "runtime_map_match": False,
            "localization_valid": localization_is_ready(
                str(localization_state or "").strip(),
                True,
            ),
            "manual_assist_required": False,
            "created_ts": now,
            "started_ts": 0.0,
            "finished_ts": 0.0,
            "updated_ts": now,
        }

    def resolve_map_revision_id(
        self,
        *,
        robot_id: str,
        map_name: str,
        allow_active_fallback: bool = True,
    ) -> str:
        backend = self._backend
        plan_store = getattr(backend, "_plan_store", None)
        if plan_store is None:
            return ""
        try:
            normalized_name = str(map_name or "").strip()
            normalized_robot_id = str(robot_id or backend.robot_id).strip() or backend.robot_id
            active_asset = None
            asset = None
            if normalized_name:
                resolve_revision = getattr(plan_store, "resolve_map_revision", None)
                if callable(resolve_revision):
                    asset = resolve_revision(
                        map_name=normalized_name,
                        robot_id=normalized_robot_id,
                    )
                else:
                    active_asset = plan_store.get_active_map(robot_id=normalized_robot_id)
                    active_map_name = str((active_asset or {}).get("map_name") or "").strip()
                    active_revision_id = str((active_asset or {}).get("revision_id") or "").strip()
                    if active_revision_id and active_map_name == normalized_name:
                        return active_revision_id
                    asset = plan_store.resolve_map_asset(
                        map_name=normalized_name,
                        robot_id=normalized_robot_id,
                    )
            elif allow_active_fallback:
                asset = active_asset or plan_store.get_active_map(robot_id=normalized_robot_id)
            return str((asset or {}).get("revision_id") or "").strip()
        except Exception:
            return ""

    def update_job_fields(self, job: Dict[str, object], **kwargs) -> Dict[str, object]:
        snapshot = dict(job or {})
        snapshot.update(kwargs)
        snapshot["updated_ts"] = time.time()
        return snapshot

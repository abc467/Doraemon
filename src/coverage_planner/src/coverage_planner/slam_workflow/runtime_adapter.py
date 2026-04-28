# -*- coding: utf-8 -*-

"""ROS-heavy runtime orchestration helpers for the formal SLAM backend."""

from __future__ import annotations

import os
import time
from typing import Any, Dict, Optional, Tuple

import rospy
from nav_msgs.msg import OccupancyGrid, Odometry

from cleanrobot_app_msgs.srv import GetOdometryStatus as AppGetOdometryStatus, OperateSlamRuntime as AppOperateSlamRuntime
from coverage_planner.canonical_contract_types import APP_GET_ODOMETRY_STATUS_SERVICE_TYPE

from coverage_planner.map_asset_status import map_asset_is_verified
from coverage_planner.map_io import compute_occupancy_grid_md5, origin_to_jsonable, write_occupancy_to_yaml_pgm
from coverage_planner.runtime_gate_messages import (
    manual_assist_recovery_message,
    runtime_map_identity_unavailable_message,
    runtime_map_mismatch_reason,
    selected_map_does_not_match_requested_map_message,
)
from coverage_planner.slam_workflow.executor import should_restart_localization_after_save
from coverage_planner.slam_workflow_semantics import (
    is_global_relocate_manual_assist,
    is_manual_assist_error_code,
    localization_needs_manual_assist,
    map_global_relocate_result_code,
)


def _normalize_runtime_map_name(map_name: str) -> str:
    value = str(map_name or "").strip()
    if value.endswith(".pbstream"):
        value = value[:-len(".pbstream")]
    return str(value or "").strip()


def _map_id_from_md5(map_md5: str) -> str:
    value = str(map_md5 or "").strip()
    if not value:
        return ""
    return "map_%s" % value[:8]


def _response_value(response: Any, field_name: str, default: Any = "") -> Any:
    if isinstance(response, dict):
        return response.get(field_name, default)
    return getattr(response, field_name, default)


class SlamWorkflowRuntimeError(RuntimeError):
    def __init__(self, code: str, message: str, *, manual_assist_required: bool = False):
        super().__init__(str(message or code or "slam workflow failed"))
        self.code = str(code or "").strip()
        self.manual_assist_required = bool(manual_assist_required)


class CartographerRuntimeAdapter:
    def __init__(self, backend: Any, *, transport):
        self._backend = backend
        self._transport = transport

    def _asset_revision_id(self, *, robot_id: str, asset: Optional[Dict[str, object]]) -> str:
        backend = self._backend
        resolved_asset = dict(asset or {})
        revision_id = str(resolved_asset.get("revision_id") or "").strip()
        if revision_id:
            return revision_id
        map_name = str(resolved_asset.get("map_name") or "").strip()
        if not map_name:
            return ""
        try:
            return str(backend._plan_store.ensure_map_asset_revision(map_name=map_name, robot_id=robot_id) or "").strip()
        except Exception:
            return ""

    def _update_pending_switch(
        self,
        *,
        robot_id: str,
        from_asset: Optional[Dict[str, object]],
        target_asset: Optional[Dict[str, object]],
        status: str,
        requested_activate: bool,
        last_error_code: str = "",
        last_error_msg: str = "",
    ):
        backend = self._backend
        from_map_name = str((from_asset or {}).get("map_name") or "").strip()
        target_map_name = str((target_asset or {}).get("map_name") or "").strip()
        if not target_map_name:
            return
        backend._plan_store.upsert_pending_map_switch(
            robot_id=robot_id,
            from_map_name=from_map_name,
            target_map_name=target_map_name,
            requested_activate=bool(requested_activate),
            status=str(status or "requested").strip() or "requested",
            last_error_code=str(last_error_code or "").strip(),
            last_error_msg=str(last_error_msg or "").strip(),
        )
        target_revision_id = self._asset_revision_id(robot_id=robot_id, asset=target_asset)
        if target_revision_id:
            backend._plan_store.upsert_pending_map_revision(
                robot_id=robot_id,
                from_revision_id=self._asset_revision_id(robot_id=robot_id, asset=from_asset),
                target_revision_id=target_revision_id,
                requested_activate=bool(requested_activate),
                status=str(status or "requested").strip() or "requested",
                last_error_code=str(last_error_code or "").strip(),
                last_error_msg=str(last_error_msg or "").strip(),
            )

    def _execute_localization_attempt(
        self,
        *,
        robot_id: str,
        asset: Dict[str, object],
        frame_id: str,
        has_initial_pose: bool,
        initial_pose_x: float,
        initial_pose_y: float,
        initial_pose_yaw: float,
    ) -> Tuple[bool, str, str, str]:
        backend = self._backend
        transport = self._transport
        runtime_state = backend._runtime_state
        resolved_name = str((asset or {}).get("map_name") or "").strip()
        pbstream_path = os.path.expanduser(str((asset or {}).get("pbstream_path") or "").strip())
        revision_id = self._asset_revision_id(robot_id=robot_id, asset=asset)
        self.clear_runtime_map_identity()
        runtime_state.set_runtime_mode(
            mode="localization",
            map_name=resolved_name,
            pbstream_path=pbstream_path,
            map_revision_id=revision_id,
        )
        runtime_state.publish_runtime_snapshot(
            current_mode="localization",
            map_name=resolved_name,
            pbstream_path=pbstream_path,
            map_revision_id=revision_id,
        )
        runtime_state.set_localization_state(
            robot_id=robot_id,
            map_name=resolved_name,
            state="localizing" if bool(has_initial_pose) else "relocalizing",
            valid=False,
            map_revision_id=revision_id,
        )
        restart_started_ts = time.time()
        transport.stop_runtime()
        start_pid, start_detail = transport.start_runtime_processes(
            include_unfrozen_submaps=backend.localization_include_unfrozen_submaps
        )
        localization_detail = self.run_localization_sequence(
            robot_id=robot_id,
            repo_map_file=backend._asset_helper.ensure_repo_map_link(asset),
            frame_id=frame_id,
            has_initial_pose=bool(has_initial_pose),
            initial_pose_x=float(initial_pose_x),
            initial_pose_y=float(initial_pose_y),
            initial_pose_yaw=float(initial_pose_yaw),
        )
        ok, msg = self.wait_until_ready(
            robot_id=robot_id,
            asset=asset,
            started_after_ts=restart_started_ts,
        )
        detail = ("%s start_pid=%s %s" % (msg or "-", start_pid, start_detail)).strip()
        if localization_detail:
            detail = ("%s %s" % (detail, localization_detail)).strip()
        return bool(ok), str(detail or ""), pbstream_path, revision_id

    def _rollback_to_previous_active(
        self,
        *,
        robot_id: str,
        previous_active: Optional[Dict[str, object]],
        frame_id: str,
        failed_target_name: str,
    ) -> Tuple[bool, str]:
        backend = self._backend
        runtime_state = backend._runtime_state
        stable_asset = dict(previous_active or {})
        stable_name = str(stable_asset.get("map_name") or "").strip()
        if not stable_name:
            runtime_state.set_localization_state(
                robot_id=robot_id,
                map_name=failed_target_name,
                state="not_localized",
                valid=False,
                map_revision_id="",
            )
            return False, ""
        try:
            self._update_pending_switch(
                robot_id=robot_id,
                from_asset=stable_asset,
                target_asset=stable_asset,
                status="rolling_back",
                requested_activate=False,
                last_error_code="restart_localization_failed",
                last_error_msg="restoring previous active map after candidate verification failure",
            )
            ok, msg, stable_pbstream_path, stable_revision_id = self._execute_localization_attempt(
                robot_id=robot_id,
                asset=stable_asset,
                frame_id=frame_id,
                has_initial_pose=False,
                initial_pose_x=0.0,
                initial_pose_y=0.0,
                initial_pose_yaw=0.0,
            )
            if not ok:
                runtime_state.set_localization_state(
                    robot_id=robot_id,
                    map_name=stable_name,
                    state="not_localized",
                    valid=False,
                    map_revision_id=stable_revision_id,
                )
                return False, str(msg or "")
            if stable_revision_id:
                backend._plan_store.set_active_map_revision(revision_id=stable_revision_id, robot_id=robot_id)
            else:
                backend._plan_store.set_active_map(map_name=stable_name, robot_id=robot_id)
            runtime_state.publish_runtime_snapshot(
                current_mode="localization",
                map_name=stable_name,
                pbstream_path=stable_pbstream_path,
                map_revision_id=stable_revision_id,
            )
            return True, str(msg or "")
        except Exception as exc:
            runtime_state.set_localization_state(
                robot_id=robot_id,
                map_name=stable_name,
                state="not_localized",
                valid=False,
                map_revision_id=str(stable_asset.get("revision_id") or ""),
            )
            return False, str(exc)

    @staticmethod
    def _runtime_map_matches(runtime_context, *, target_md5: str, target_revision_id: str) -> bool:
        try:
            return bool(
                runtime_context.runtime_map_matches(
                    str(target_md5 or ""),
                    target_revision_id=str(target_revision_id or ""),
                )
            )
        except TypeError:
            return bool(runtime_context.runtime_map_matches(str(target_md5 or "")))

    @staticmethod
    def _runtime_map_revision_id(runtime_context) -> str:
        getter = getattr(runtime_context, "runtime_map_revision_id", None)
        if not callable(getter):
            return ""
        try:
            return str(getter() or "").strip()
        except Exception:
            return ""

    def _same_map_scope(
        self,
        *,
        robot_id: str,
        left_asset: Optional[Dict[str, object]],
        right_asset: Optional[Dict[str, object]],
    ) -> bool:
        left = dict(left_asset or {})
        right = dict(right_asset or {})
        left_revision_id = self._asset_revision_id(robot_id=robot_id, asset=left)
        right_revision_id = self._asset_revision_id(robot_id=robot_id, asset=right)
        if left_revision_id and right_revision_id:
            return left_revision_id == right_revision_id
        return str(left.get("map_name") or "").strip() == str(right.get("map_name") or "").strip()

    @staticmethod
    def _manual_assist_message(
        message: str,
        *,
        map_name: str = "",
        map_revision_id: str = "",
        retry_action: str = "prepare_for_task",
    ) -> str:
        base = str(message or "").strip() or "localization requires manual assist"
        return "%s; %s" % (
            base,
            manual_assist_recovery_message(
                map_name=map_name,
                map_revision_id=map_revision_id,
                retry_action=retry_action,
            ),
        )

    @staticmethod
    def _retry_action_for_operation(operation: int) -> str:
        try:
            normalized = int(operation)
        except Exception:
            normalized = 0
        mapping = {
            6: "prepare_for_task",
            7: "switch_map_and_localize",
            8: "relocalize",
            9: "verify_map_revision",
            10: "activate_map_revision",
        }
        return str(mapping.get(normalized, "prepare_for_task"))

    def verify_map_revision(
        self,
        *,
        robot_id: str,
        map_name: str,
        map_revision_id: str = "",
        operation: int,
        frame_id: str = "map",
        has_initial_pose: bool = False,
        initial_pose_x: float = 0.0,
        initial_pose_y: float = 0.0,
        initial_pose_yaw: float = 0.0,
    ):
        backend = self._backend
        assets = backend._asset_helper
        runtime_state = backend._runtime_state
        service_api = backend._service_api
        try:
            asset = assets.resolve_asset(
                robot_id=robot_id,
                map_name=map_name,
                map_revision_id=str(map_revision_id or "").strip(),
                prefer_active_revision=not bool(str(map_revision_id or "").strip()),
            )
        except ValueError as exc:
            return service_api.response(
                success=False,
                message=str(exc),
                error_code="map_scope_mismatch",
                operation=operation,
                map_name=str(map_name or ""),
                map_revision_id=str(map_revision_id or "").strip(),
                localization_state="not_localized",
                current_mode="localization",
            )
        if not asset:
            return service_api.response(
                success=False,
                message="map asset not found",
                error_code="map_asset_not_found",
                operation=operation,
                map_name=map_name,
                map_revision_id=str(map_revision_id or "").strip(),
                localization_state="not_localized",
                current_mode="localization",
            )
        if not bool(asset.get("enabled", True)):
            return service_api.response(
                success=False,
                message="map asset is disabled",
                error_code="map_asset_disabled",
                operation=operation,
                map_name=str(asset.get("map_name") or map_name),
                map_revision_id=str(asset.get("revision_id") or map_revision_id or "").strip(),
                localization_state="not_localized",
                current_mode="localization",
            )

        resolved_name = str(asset.get("map_name") or map_name or "").strip()
        previous_active = backend._plan_store.get_active_map(robot_id=robot_id) or {}
        previous_revision_id = str(previous_active.get("revision_id") or "").strip()
        rollback_candidate = (not self._same_map_scope(robot_id=robot_id, left_asset=previous_active, right_asset=asset))
        try:
            self._update_pending_switch(
                robot_id=robot_id,
                from_asset=previous_active,
                target_asset=asset,
                status="verifying",
                requested_activate=False,
                last_error_code="",
                last_error_msg="",
            )
            ok, msg, pbstream_path, revision_id = self._execute_localization_attempt(
                robot_id=robot_id,
                asset=asset,
                frame_id=frame_id,
                has_initial_pose=bool(has_initial_pose),
                initial_pose_x=float(initial_pose_x),
                initial_pose_y=float(initial_pose_y),
                initial_pose_yaw=float(initial_pose_yaw),
            )
            if not ok:
                self.record_failed_map_verification(
                    map_name=resolved_name,
                    revision_id=revision_id,
                    error_code="verify_map_revision_failed",
                    error_msg=str(msg or ""),
                )
                rollback_ok = False
                rollback_msg = ""
                if rollback_candidate:
                    rollback_ok, rollback_msg = self._rollback_to_previous_active(
                        robot_id=robot_id,
                        previous_active=previous_active,
                        frame_id=frame_id,
                        failed_target_name=resolved_name,
                    )
                backend._plan_store.clear_pending_map_switch(robot_id=robot_id)
                localization_state = "localized" if rollback_ok else "not_localized"
                if rollback_ok:
                    msg = (
                        "map revision verification failed and runtime rolled back to active map: %s rollback=%s"
                    ) % (str(msg or "candidate verification failed"), str(rollback_msg or "ok"))
                return service_api.response(
                    success=False,
                    message=str(msg or ""),
                    error_code="verify_map_revision_failed",
                    operation=operation,
                    map_name=resolved_name,
                    map_revision_id=str(revision_id or asset.get("revision_id") or map_revision_id or "").strip(),
                    localization_state=localization_state,
                    current_mode="localization",
                )

            self.finalize_verified_map_asset(map_name=resolved_name, revision_id=revision_id)
            restore_required = bool(previous_active) and (previous_revision_id != str(revision_id or "").strip())
            restore_ok = True
            restore_msg = ""
            if restore_required:
                restore_ok, restore_msg = self._rollback_to_previous_active(
                    robot_id=robot_id,
                    previous_active=previous_active,
                    frame_id=frame_id,
                    failed_target_name=resolved_name,
                )
            else:
                runtime_state.publish_runtime_snapshot(
                    current_mode="localization",
                    map_name=resolved_name,
                    pbstream_path=pbstream_path,
                    map_revision_id=revision_id,
                )
            backend._plan_store.clear_pending_map_switch(robot_id=robot_id)
            if not restore_ok:
                return service_api.response(
                    success=False,
                    message="map revision verified but failed to restore previous active map: %s" % str(restore_msg or ""),
                    error_code="restore_previous_active_failed",
                    operation=operation,
                    map_name=resolved_name,
                    map_revision_id=str(revision_id or asset.get("revision_id") or map_revision_id or "").strip(),
                    localization_state="not_localized",
                    current_mode="localization",
                )
            if restore_required:
                msg = "map revision verified and runtime restored to previous active map"
            else:
                msg = "map revision verified"
            return service_api.response(
                success=True,
                message=str(msg or ""),
                error_code="",
                operation=operation,
                map_name=resolved_name,
                map_revision_id=str(revision_id or asset.get("revision_id") or map_revision_id or "").strip(),
                localization_state="localized",
                current_mode="localization",
            )
        except SlamWorkflowRuntimeError as exc:
            self.record_failed_map_verification(
                map_name=resolved_name,
                revision_id=str(asset.get("revision_id") or ""),
                error_code=str(exc.code or "verify_map_revision_failed"),
                error_msg=str(exc),
            )
            rollback_ok = False
            rollback_msg = ""
            if rollback_candidate:
                rollback_ok, rollback_msg = self._rollback_to_previous_active(
                    robot_id=robot_id,
                    previous_active=previous_active,
                    frame_id=frame_id,
                    failed_target_name=resolved_name,
                )
            backend._plan_store.clear_pending_map_switch(robot_id=robot_id)
            localization_state = "localized" if rollback_ok else (
                "manual_assist_required" if bool(getattr(exc, "manual_assist_required", False)) else "not_localized"
            )
            if (not rollback_ok) and localization_needs_manual_assist(localization_state):
                runtime_state.set_localization_state(
                    robot_id=robot_id,
                    map_name=resolved_name,
                    state=localization_state,
                    valid=False,
                    map_revision_id=str(asset.get("revision_id") or ""),
                )
            message = (
                self._manual_assist_message(
                    str(exc),
                    map_name=resolved_name,
                    map_revision_id=str(asset.get("revision_id") or map_revision_id or "").strip(),
                    retry_action="verify_map_revision",
                )
                if localization_needs_manual_assist(localization_state)
                else str(exc)
            )
            if rollback_ok:
                message = "%s rollback=%s" % (message, str(rollback_msg or "ok"))
            return service_api.response(
                success=False,
                message=message,
                error_code=str(exc.code or "verify_map_revision_failed"),
                operation=operation,
                map_name=resolved_name,
                map_revision_id=str(asset.get("revision_id") or map_revision_id or "").strip(),
                localization_state=localization_state,
                current_mode="localization",
            )
        except Exception as exc:
            self.record_failed_map_verification(
                map_name=resolved_name,
                revision_id=str(asset.get("revision_id") or ""),
                error_code="verify_map_revision_failed",
                error_msg=str(exc),
            )
            rollback_ok = False
            rollback_msg = ""
            if rollback_candidate:
                rollback_ok, rollback_msg = self._rollback_to_previous_active(
                    robot_id=robot_id,
                    previous_active=previous_active,
                    frame_id=frame_id,
                    failed_target_name=resolved_name,
                )
            backend._plan_store.clear_pending_map_switch(robot_id=robot_id)
            message = str(exc)
            if rollback_ok:
                message = "%s rollback=%s" % (message, str(rollback_msg or "ok"))
            return service_api.response(
                success=False,
                message=message,
                error_code="verify_map_revision_failed",
                operation=operation,
                map_name=resolved_name,
                map_revision_id=str(asset.get("revision_id") or map_revision_id or "").strip(),
                localization_state="localized" if rollback_ok else "not_localized",
                current_mode="localization",
            )

    def activate_map_revision(
        self,
        *,
        robot_id: str,
        map_name: str,
        map_revision_id: str = "",
        operation: int,
        frame_id: str = "map",
        has_initial_pose: bool = False,
        initial_pose_x: float = 0.0,
        initial_pose_y: float = 0.0,
        initial_pose_yaw: float = 0.0,
    ):
        backend = self._backend
        try:
            asset = backend._asset_helper.resolve_asset(
                robot_id=robot_id,
                map_name=map_name,
                map_revision_id=str(map_revision_id or "").strip(),
            )
        except ValueError as exc:
            return backend._service_api.response(
                success=False,
                message=str(exc),
                error_code="map_scope_mismatch",
                operation=operation,
                map_name=str(map_name or ""),
                map_revision_id=str(map_revision_id or "").strip(),
                localization_state="not_localized",
                current_mode="localization",
            )
        if not asset:
            return backend._service_api.response(
                success=False,
                message="map asset not found",
                error_code="map_asset_not_found",
                operation=operation,
                map_name=map_name,
                map_revision_id=str(map_revision_id or "").strip(),
                localization_state="not_localized",
                current_mode="localization",
            )
        if not map_asset_is_verified(asset):
            return backend._service_api.response(
                success=False,
                message="map revision must be verified before activation",
                error_code="map_revision_not_verified",
                operation=operation,
                map_name=str(asset.get("map_name") or map_name),
                map_revision_id=str(asset.get("revision_id") or map_revision_id or "").strip(),
                localization_state="not_localized",
                current_mode="localization",
            )
        return self.restart_localization(
            robot_id=robot_id,
            map_name=str(asset.get("map_name") or map_name),
            map_revision_id=str(asset.get("revision_id") or map_revision_id or ""),
            operation=operation,
            frame_id=frame_id,
            has_initial_pose=bool(has_initial_pose),
            initial_pose_x=float(initial_pose_x),
            initial_pose_y=float(initial_pose_y),
            initial_pose_yaw=float(initial_pose_yaw),
        )

    def restart_localization(
        self,
        *,
        robot_id: str,
        map_name: str,
        map_revision_id: str = "",
        operation: int,
        frame_id: str = "map",
        has_initial_pose: bool = False,
        initial_pose_x: float = 0.0,
        initial_pose_y: float = 0.0,
        initial_pose_yaw: float = 0.0,
    ):
        backend = self._backend
        transport = self._transport
        assets = backend._asset_helper
        runtime_state = backend._runtime_state
        service_api = backend._service_api
        try:
            asset = assets.resolve_asset(
                robot_id=robot_id,
                map_name=map_name,
                map_revision_id=str(map_revision_id or "").strip(),
            )
        except ValueError as exc:
            return service_api.response(
                success=False,
                message=str(exc),
                error_code="map_scope_mismatch",
                operation=operation,
                map_name=str(map_name or ""),
                map_revision_id=str(map_revision_id or "").strip(),
                localization_state="not_localized",
                current_mode="localization",
            )
        if not asset:
            return service_api.response(
                success=False,
                message="map asset not found",
                error_code="map_asset_not_found",
                operation=operation,
                map_name=map_name,
                map_revision_id=str(map_revision_id or "").strip(),
                localization_state="not_localized",
                current_mode="localization",
            )
        if not bool(asset.get("enabled", True)):
            return service_api.response(
                success=False,
                message="map asset is disabled",
                error_code="map_asset_disabled",
                operation=operation,
                map_name=str(asset.get("map_name") or map_name),
                map_revision_id=str(asset.get("revision_id") or map_revision_id or "").strip(),
                localization_state="not_localized",
                current_mode="localization",
            )

        resolved_name = str(asset.get("map_name") or map_name or "").strip()
        previous_active = backend._plan_store.get_active_map(robot_id=robot_id) or {}
        rollback_candidate = (not self._same_map_scope(robot_id=robot_id, left_asset=previous_active, right_asset=asset))
        try:
            self._update_pending_switch(
                robot_id=robot_id,
                from_asset=previous_active,
                target_asset=asset,
                status="verifying",
                requested_activate=True,
                last_error_code="",
                last_error_msg="",
            )
            ok, msg, pbstream_path, revision_id = self._execute_localization_attempt(
                robot_id=robot_id,
                asset=asset,
                frame_id=frame_id,
                has_initial_pose=bool(has_initial_pose),
                initial_pose_x=float(initial_pose_x),
                initial_pose_y=float(initial_pose_y),
                initial_pose_yaw=float(initial_pose_yaw),
            )
            if ok:
                canonical_asset = self.finalize_verified_map_asset(map_name=resolved_name, revision_id=revision_id)
                if revision_id:
                    backend._plan_store.set_active_map_revision(revision_id=revision_id, robot_id=robot_id)
                else:
                    backend._plan_store.set_active_map(map_name=resolved_name, robot_id=robot_id)
                backend._plan_store.clear_pending_map_switch(robot_id=robot_id)
                canonical_map_id = str((canonical_asset or {}).get("map_id") or "").strip()
                canonical_map_md5 = str((canonical_asset or {}).get("map_md5") or "").strip()
                if canonical_map_id:
                    rospy.set_param("/map_id", canonical_map_id)
                if canonical_map_md5:
                    rospy.set_param("/map_md5", canonical_map_md5)
                if revision_id:
                    rospy.set_param("/map_revision_id", revision_id)
                runtime_state.publish_runtime_snapshot(
                    current_mode="localization",
                    map_name=resolved_name,
                    pbstream_path=pbstream_path,
                    map_revision_id=revision_id,
                )
                localization_state = "localized"
            else:
                self.record_failed_map_verification(
                    map_name=resolved_name,
                    revision_id=revision_id,
                    error_code="restart_localization_failed",
                    error_msg=str(msg or ""),
                )
                rollback_ok = False
                rollback_msg = ""
                if rollback_candidate:
                    rollback_ok, rollback_msg = self._rollback_to_previous_active(
                        robot_id=robot_id,
                        previous_active=previous_active,
                        frame_id=frame_id,
                        failed_target_name=resolved_name,
                    )
                backend._plan_store.clear_pending_map_switch(robot_id=robot_id)
                localization_state = "localized" if rollback_ok else "not_localized"
                if rollback_ok:
                    msg = (
                        "target map verification failed and runtime rolled back to active map: "
                        "%s rollback=%s"
                    ) % (str(msg or "candidate verification failed"), str(rollback_msg or "ok"))
                elif rollback_msg:
                    msg = (
                        "target map verification failed and rollback to previous active map also failed: "
                        "%s rollback=%s"
                    ) % (str(msg or "candidate verification failed"), str(rollback_msg or ""))
            return service_api.response(
                success=bool(ok),
                message=str(msg or ""),
                error_code="" if ok else "restart_localization_failed",
                operation=operation,
                map_name=resolved_name,
                map_revision_id=str(revision_id or asset.get("revision_id") or map_revision_id or "").strip(),
                localization_state=localization_state,
                current_mode="localization",
            )
        except SlamWorkflowRuntimeError as exc:
            self.record_failed_map_verification(
                map_name=resolved_name,
                revision_id=str(asset.get("revision_id") or ""),
                error_code=str(exc.code or "restart_localization_failed"),
                error_msg=str(exc),
            )
            rollback_ok = False
            rollback_msg = ""
            if rollback_candidate:
                rollback_ok, rollback_msg = self._rollback_to_previous_active(
                    robot_id=robot_id,
                    previous_active=previous_active,
                    frame_id=frame_id,
                    failed_target_name=resolved_name,
                )
            backend._plan_store.clear_pending_map_switch(robot_id=robot_id)
            if rollback_ok:
                localization_state = "localized"
            elif bool(getattr(exc, "manual_assist_required", False)):
                localization_state = "manual_assist_required"
                runtime_state.set_localization_state(
                    robot_id=robot_id,
                    map_name=resolved_name,
                    state=localization_state,
                    valid=False,
                    map_revision_id=str(asset.get("revision_id") or ""),
                )
            else:
                localization_state = "not_localized"
                runtime_state.set_localization_state(
                    robot_id=robot_id,
                    map_name=resolved_name,
                    state=localization_state,
                    valid=False,
                    map_revision_id=str(asset.get("revision_id") or ""),
                )
            message = (
                self._manual_assist_message(
                    str(exc),
                    map_name=resolved_name,
                    map_revision_id=str(asset.get("revision_id") or map_revision_id or "").strip(),
                    retry_action=self._retry_action_for_operation(operation),
                )
                if localization_needs_manual_assist(localization_state)
                else str(exc)
            )
            if rollback_ok:
                message = "%s rollback=%s" % (message, str(rollback_msg or "ok"))
            return service_api.response(
                success=False,
                message=message,
                error_code=str(exc.code or "restart_localization_failed"),
                operation=operation,
                map_name=resolved_name,
                map_revision_id=str(asset.get("revision_id") or map_revision_id or "").strip(),
                localization_state=localization_state,
                current_mode="localization",
            )
        except Exception as exc:
            self.record_failed_map_verification(
                map_name=resolved_name,
                revision_id=str(asset.get("revision_id") or ""),
                error_code="restart_localization_failed",
                error_msg=str(exc),
            )
            rollback_ok = False
            rollback_msg = ""
            if rollback_candidate:
                rollback_ok, rollback_msg = self._rollback_to_previous_active(
                    robot_id=robot_id,
                    previous_active=previous_active,
                    frame_id=frame_id,
                    failed_target_name=resolved_name,
                )
            backend._plan_store.clear_pending_map_switch(robot_id=robot_id)
            if not rollback_ok:
                runtime_state.set_localization_state(
                    robot_id=robot_id,
                    map_name=resolved_name,
                    state="not_localized",
                    valid=False,
                    map_revision_id=str(asset.get("revision_id") or ""),
                )
            message = str(exc)
            if rollback_ok:
                message = "%s rollback=%s" % (message, str(rollback_msg or "ok"))
            return service_api.response(
                success=False,
                message=message,
                error_code="restart_localization_failed",
                operation=operation,
                map_name=resolved_name,
                map_revision_id=str(asset.get("revision_id") or map_revision_id or "").strip(),
                localization_state="localized" if rollback_ok else "not_localized",
                current_mode="localization",
            )

    def start_mapping(
        self,
        *,
        robot_id: str,
        operation: int,
    ):
        backend = self._backend
        transport = self._transport
        runtime_state = backend._runtime_state
        service_api = backend._service_api
        runtime_state.set_runtime_mode(mode="mapping", map_name="", pbstream_path="")
        self.clear_runtime_map_identity()
        backend._runtime_context.reset_runtime_observations()
        try:
            transport.stop_runtime()
            start_pid, start_detail = transport.start_runtime_processes(
                include_unfrozen_submaps=backend.mapping_include_unfrozen_submaps
            )
            code, msg, _data = transport.call_visual_command(
                "load_config",
                {"config_entry": backend.mapping_config_entry},
            )
            if code != 0:
                raise RuntimeError(
                    "load_config failed code=%s msg=%s start_pid=%s %s"
                    % (code, msg or "-", start_pid, start_detail)
                )
            code, msg, data_json = transport.call_visual_command("add_trajectory", "")
            if code != 0:
                raise RuntimeError(
                    "add_trajectory failed code=%s msg=%s start_pid=%s %s"
                    % (code, msg or "-", start_pid, start_detail)
                )
            save_backend_mode = transport.wait_for_save_backend_ready(
                timeout_s=min(float(backend.command_timeout_s), 15.0),
                watched_labels=("cartographer_node", "cartographer_occupancy_grid_node"),
            )
            rospy.wait_for_message(
                backend.map_topic,
                OccupancyGrid,
                timeout=min(float(backend.command_timeout_s), 15.0),
            )
            msg = "mapping runtime started trajectory=%s save_backend=%s start_pid=%s %s" % (
                data_json or "-",
                str(save_backend_mode or "unknown"),
                start_pid,
                start_detail,
            )
            runtime_state.publish_runtime_snapshot(current_mode="mapping", map_name="", pbstream_path="")
            runtime_state.set_localization_state(
                robot_id=robot_id,
                map_name=None,
                state="not_localized",
                valid=False,
                map_revision_id="",
            )
            return service_api.response(
                success=True,
                message=str(msg or "mapping mode started"),
                error_code="",
                operation=operation,
                map_name="",
                localization_state="mapping",
                current_mode="mapping",
            )
        except Exception as exc:
            return service_api.response(
                success=False,
                message=str(exc),
                error_code="runtime_reload_failed",
                operation=operation,
                map_name="",
                localization_state="not_localized",
                current_mode="localization",
            )

    def stop_mapping(
        self,
        *,
        robot_id: str,
        map_name: str,
        map_revision_id: str = "",
        operation: int,
    ):
        backend = self._backend
        transport = self._transport
        runtime_state = backend._runtime_state
        service_api = backend._service_api
        resolved_name = str(map_name or "").strip()
        resolved_revision_id = str(map_revision_id or "").strip()
        if not resolved_name and not resolved_revision_id:
            active_asset = backend._plan_store.get_active_map(robot_id=robot_id) or {}
            resolved_name = str(active_asset.get("map_name") or "").strip()
            resolved_revision_id = str(active_asset.get("revision_id") or "").strip()
        if resolved_name or resolved_revision_id:
            result = self.restart_localization(
                robot_id=robot_id,
                map_name=resolved_name,
                map_revision_id=resolved_revision_id,
                operation=operation,
            )
            if (
                not bool(_response_value(result, "success", False))
                and is_manual_assist_error_code(str(_response_value(result, "error_code") or ""))
                and localization_needs_manual_assist(str(_response_value(result, "localization_state") or ""))
            ):
                return service_api.response(
                    success=True,
                    message=(
                        "mapping stopped; localization requires manual assist: %s"
                        % str(_response_value(result, "message") or "").strip()
                    ).strip(),
                    error_code="",
                    operation=operation,
                    map_name=str(_response_value(result, "map_name") or resolved_name),
                    map_revision_id=str(_response_value(result, "map_revision_id") or resolved_revision_id),
                    localization_state=str(_response_value(result, "localization_state") or "manual_assist_required"),
                    current_mode=str(_response_value(result, "current_mode") or "localization"),
                )
            return result

        try:
            transport.stop_runtime()
            self.clear_runtime_map_identity()
            runtime_state.set_runtime_mode(mode="localization", map_name="", pbstream_path="", map_revision_id="")
            runtime_state.publish_runtime_snapshot(
                current_mode="localization",
                map_name="",
                pbstream_path="",
                map_revision_id="",
            )
            runtime_state.set_localization_state(
                robot_id=robot_id,
                map_name="",
                state="not_localized",
                valid=False,
                map_revision_id="",
            )
            return service_api.response(
                success=True,
                message="mapping stopped; no active map available for localization",
                error_code="",
                operation=operation,
                map_name="",
                map_revision_id="",
                localization_state="not_localized",
                current_mode="localization",
            )
        except Exception as exc:
            return service_api.response(
                success=False,
                message=str(exc),
                error_code="stop_mapping_failed",
                operation=operation,
                map_name="",
                map_revision_id="",
                localization_state="mapping",
                current_mode="mapping",
            )

    def save_mapping(
        self,
        *,
        robot_id: str,
        map_name: str,
        description: str,
        set_active: bool,
        operation: int,
        include_unfinished_submaps: bool = True,
        switch_to_localization_after_save: bool = False,
        relocalize_after_switch: bool = False,
    ):
        backend = self._backend
        transport = self._transport
        assets = backend._asset_helper
        service_api = backend._service_api
        normalized_map_name = _normalize_runtime_map_name(map_name)
        if not normalized_map_name:
            return service_api.response(
                success=False,
                message="map_name is required for save_mapping",
                error_code="map_name_required",
                operation=operation,
                map_name="",
                localization_state="mapping",
                current_mode="mapping",
            )
        candidate_revision_id = backend._plan_store.generate_map_revision_id(normalized_map_name)
        target_paths = assets.target_paths(
            normalized_map_name,
            revision_id=candidate_revision_id,
        )
        for path in target_paths.values():
            if os.path.exists(path):
                return service_api.response(
                    success=False,
                    message="target asset path already exists: %s" % path,
                    error_code="asset_path_exists",
                    operation=operation,
                    map_name=normalized_map_name,
                    localization_state="mapping",
                    current_mode="mapping",
                )

        pbstream_path = target_paths["pbstream_path"]
        yaml_path = ""
        pgm_path = ""
        saved_revision_id = ""
        try:
            artifact_dir = os.path.dirname(pbstream_path)
            if artifact_dir:
                os.makedirs(artifact_dir, exist_ok=True)
            occ = rospy.wait_for_message(
                backend.map_topic,
                OccupancyGrid,
                timeout=float(backend.command_timeout_s),
            )
            save_snapshot_md5 = str(compute_occupancy_grid_md5(occ) or "").strip()
            if not save_snapshot_md5:
                raise RuntimeError("failed to compute map_md5 from current /map")

            ok, msg = transport.save_pbstream(
                pbstream_path,
                include_unfinished_submaps=bool(include_unfinished_submaps),
            )
            if not ok:
                raise RuntimeError(str(msg or "save_state failed"))
            if not os.path.exists(pbstream_path):
                raise RuntimeError("pbstream save did not produce file: %s" % pbstream_path)

            pgm_path, yaml_path = write_occupancy_to_yaml_pgm(
                occ,
                artifact_dir or backend.maps_root,
                base_name=normalized_map_name,
            )
            backend._plan_store.register_map_asset(
                map_name=normalized_map_name,
                map_id="",
                map_md5="",
                yaml_path=yaml_path,
                pgm_path=pgm_path,
                pbstream_path=pbstream_path,
                frame_id=str(occ.header.frame_id or "map"),
                resolution=float(occ.info.resolution or 0.0),
                origin=origin_to_jsonable(occ),
                display_name=normalized_map_name,
                description=str(description or ""),
                enabled=True,
                lifecycle_status="saved_unverified",
                verification_status="pending",
                save_snapshot_md5=save_snapshot_md5,
                verified_runtime_map_id="",
                verified_runtime_map_md5="",
                last_error_code="",
                last_error_msg="",
                robot_id=robot_id,
                revision_id=candidate_revision_id,
                set_active=False,
            )
            saved_asset = assets.resolve_asset(
                robot_id=robot_id,
                map_name=normalized_map_name,
                map_revision_id=candidate_revision_id,
            ) or {}
            saved_revision_id = str(saved_asset.get("revision_id") or candidate_revision_id).strip()
            if should_restart_localization_after_save(
                switch_to_localization_after_save=bool(switch_to_localization_after_save),
                relocalize_after_switch=bool(relocalize_after_switch),
            ):
                verify_followup = self.verify_map_revision(
                    robot_id=robot_id,
                    map_name=normalized_map_name,
                    map_revision_id=saved_revision_id,
                    operation=int(AppOperateSlamRuntime._request_class.verify_map_revision),
                )
                if not bool(verify_followup.success):
                    return service_api.response(
                        success=False,
                        message="mapping asset saved but revision verification failed: %s"
                        % str(verify_followup.message or verify_followup.error_code or "verification failed"),
                        error_code=str(verify_followup.error_code or "verify_map_revision_failed"),
                        operation=operation,
                        map_name=normalized_map_name,
                        map_revision_id=saved_revision_id,
                        localization_state=str(verify_followup.localization_state or "not_localized"),
                        current_mode=str(verify_followup.current_mode or "localization"),
                    )
                followup = verify_followup
                if bool(set_active):
                    followup = self.activate_map_revision(
                        robot_id=robot_id,
                        map_name=normalized_map_name,
                        map_revision_id=saved_revision_id,
                        operation=int(AppOperateSlamRuntime._request_class.activate_map_revision),
                    )
                if bool(followup.success):
                    return service_api.response(
                        success=True,
                        message=(
                            "mapping asset saved, revision verified, and activated"
                            if bool(set_active)
                            else "mapping asset saved and revision verified"
                        ),
                        error_code="",
                        operation=operation,
                        map_name=normalized_map_name,
                        map_revision_id=saved_revision_id,
                        localization_state=str(followup.localization_state or "localized"),
                        current_mode=str(followup.current_mode or "localization"),
                    )
                return service_api.response(
                    success=False,
                    message="mapping asset saved and revision verified, but activation failed: %s"
                    % str(followup.message or followup.error_code or "restart failed"),
                    error_code=str(followup.error_code or "activate_map_revision_failed"),
                    operation=operation,
                    map_name=normalized_map_name,
                    map_revision_id=saved_revision_id,
                    localization_state=str(followup.localization_state or "not_localized"),
                    current_mode=str(followup.current_mode or "localization"),
                )
            activation_note = ""
            if bool(set_active):
                activation_note = " activation deferred until localization verification succeeds"
            return service_api.response(
                success=True,
                message=("mapping asset saved%s" % activation_note).strip(),
                error_code="",
                operation=operation,
                map_name=normalized_map_name,
                map_revision_id=saved_revision_id,
                localization_state="mapping",
                current_mode="mapping",
            )
        except Exception as exc:
            assets.cleanup_paths(pgm_path, yaml_path, pbstream_path)
            return service_api.response(
                success=False,
                message=str(exc),
                error_code="save_mapping_failed",
                operation=operation,
                map_name=normalized_map_name,
                map_revision_id=saved_revision_id,
                localization_state="mapping",
                current_mode="mapping",
            )

    def wait_for_odometry_ready(self, *, robot_id: str) -> str:
        backend = self._backend
        transport = self._transport
        odom_service_type = ""
        odom_service_cls = None
        if backend._runtime_context.service_available(
            backend.odometry_status_service,
            APP_GET_ODOMETRY_STATUS_SERVICE_TYPE,
        ):
            odom_service_type = APP_GET_ODOMETRY_STATUS_SERVICE_TYPE
            odom_service_cls = AppGetOdometryStatus
        if odom_service_cls is not None:
            try:
                rospy.wait_for_service(
                    backend.odometry_status_service,
                    timeout=float(backend.odometry_ready_timeout_s),
                )
                cli = rospy.ServiceProxy(backend.odometry_status_service, odom_service_cls)
                resp = cli(robot_id=robot_id)
                state = getattr(resp, "state", None)
                if bool(getattr(state, "odom_valid", False)):
                    return "odom_state=valid source=%s topic=%s" % (
                        str(getattr(state, "odom_source", "") or ""),
                        str(getattr(state, "odom_topic", backend.odom_topic) or backend.odom_topic),
                    )
                raise RuntimeError(
                    "odometry backend not ready code=%s message=%s"
                    % (
                        str(getattr(state, "error_code", "") or ""),
                        str(getattr(state, "message", "") or "odometry invalid"),
                    )
                )
            except Exception as exc:
                rospy.logwarn(
                    "[slam_runtime_manager] odometry status service check failed type=%s: %s",
                    odom_service_type,
                    str(exc),
                )

        try:
            odom = rospy.wait_for_message(
                backend.odom_topic,
                Odometry,
                timeout=float(backend.odometry_ready_timeout_s),
            )
            return "odom_topic=%s frame=%s child=%s" % (
                backend.odom_topic,
                str(getattr(getattr(odom, "header", None), "frame_id", "") or ""),
                str(getattr(odom, "child_frame_id", "") or ""),
            )
        except Exception as exc:
            raise RuntimeError("odometry backend unavailable topic=%s: %s" % (backend.odom_topic, str(exc)))

    def run_localization_sequence(
        self,
        *,
        robot_id: str,
        repo_map_file: str,
        frame_id: str = "map",
        has_initial_pose: bool = False,
        initial_pose_x: float = 0.0,
        initial_pose_y: float = 0.0,
        initial_pose_yaw: float = 0.0,
    ) -> str:
        backend = self._backend
        transport = self._transport
        code, msg, _data = transport.call_visual_command(
            "load_config",
            {"config_entry": backend.localization_config_entry},
        )
        if code != 0:
            raise RuntimeError("load_config failed code=%s msg=%s" % (code, msg or "-"))

        code, msg, _data = transport.call_visual_command(
            "load_state",
            {"filename": str(repo_map_file or ""), "frozen": True},
        )
        if code != 0:
            raise RuntimeError("load_state failed code=%s msg=%s map=%s" % (code, msg or "-", repo_map_file))

        odom_detail = self.wait_for_odometry_ready(robot_id=robot_id)

        code, msg, data_json = transport.call_visual_command("add_trajectory", "")
        if code != 0:
            raise RuntimeError(
                "add_trajectory failed code=%s msg=%s %s"
                % (code, msg or "-", odom_detail)
            )

        if bool(has_initial_pose):
            backend._runtime_context.publish_initial_pose(
                frame_id=frame_id,
                initial_pose_x=initial_pose_x,
                initial_pose_y=initial_pose_y,
                initial_pose_yaw=initial_pose_yaw,
            )
            return "%s trajectory=%s initial_pose=(%.3f,%.3f,%.3f)" % (
                odom_detail,
                data_json or "-",
                float(initial_pose_x),
                float(initial_pose_y),
                float(initial_pose_yaw),
            )

        code, msg, _data = transport.call_visual_command("try_global_relocate", "")
        if code != 0:
            mapped_code = map_global_relocate_result_code(code)
            raise SlamWorkflowRuntimeError(
                mapped_code,
                "try_global_relocate failed code=%s msg=%s %s"
                % (code, msg or "-", odom_detail),
                manual_assist_required=is_global_relocate_manual_assist(code),
            )

        ok, set_param_msg = transport.call_set_param("global_relocated", "true")
        if not ok:
            raise RuntimeError(
                "set_param global_relocated=true failed: %s %s"
                % (set_param_msg or "-", odom_detail)
            )

        return "%s trajectory=%s" % (
            odom_detail,
            data_json or "-",
        )

    def save_pbstream(self, filename: str, include_unfinished_submaps: bool = True) -> Tuple[bool, str]:
        return self._transport.save_pbstream(
            filename,
            include_unfinished_submaps=include_unfinished_submaps,
        )

    def clear_runtime_map_identity(self):
        backend = self._backend
        for key in (
            "/map_name",
            "/map_id",
            "/map_md5",
            "/map_revision_id",
            backend._runtime_context.runtime_param("map_revision_id"),
            backend._runtime_context.runtime_param("current_map_name"),
            backend._runtime_context.runtime_param("current_pbstream_path"),
            backend._runtime_context.runtime_param("current_map_revision_id"),
        ):
            try:
                rospy.set_param(key, "")
            except Exception:
                pass

    def finalize_verified_map_asset(self, *, map_name: str, revision_id: str = "") -> Dict[str, object]:
        backend = self._backend
        runtime_map_md5 = backend._runtime_context.runtime_map_md5()
        return backend._plan_store.mark_map_asset_verification_result(
            map_name=map_name,
            revision_id=str(revision_id or "").strip(),
            verification_status="verified",
            lifecycle_status="available",
            runtime_map_id=_map_id_from_md5(runtime_map_md5),
            runtime_map_md5=runtime_map_md5,
            error_code="",
            error_msg="",
            promote_canonical_identity=True,
        )

    def record_failed_map_verification(self, *, map_name: str, revision_id: str = "", error_code: str, error_msg: str):
        backend = self._backend
        asset = backend._plan_store.resolve_map_asset(
            map_name=map_name,
            revision_id=str(revision_id or "").strip(),
        ) or {}
        current_verification_status = str(asset.get("verification_status") or "").strip().lower()
        verification_status = "failed" if current_verification_status != "verified" else None
        lifecycle_status = "saved_unverified" if verification_status == "failed" else None
        backend._plan_store.mark_map_asset_verification_result(
            map_name=map_name,
            revision_id=str(asset.get("revision_id") or revision_id or "").strip(),
            verification_status=verification_status,
            lifecycle_status=lifecycle_status,
            error_code=str(error_code or "").strip(),
            error_msg=str(error_msg or "").strip(),
            promote_canonical_identity=False,
        )

    def rebind_map_identity_from_runtime(
        self,
        *,
        map_name: str,
        asset: Dict[str, object],
    ) -> Optional[Dict[str, object]]:
        backend = self._backend
        if not backend.allow_identity_rebind_on_localize:
            return None
        runtime_map_md5 = backend._runtime_context.runtime_map_md5()
        if not map_name or not runtime_map_md5:
            return None
        asset_map_id = str((asset or {}).get("map_id") or "").strip()
        asset_map_md5 = str((asset or {}).get("map_md5") or "").strip()
        if asset_map_id or asset_map_md5:
            return None
        rebound = backend._plan_store.rebind_map_identity(
            map_name=map_name,
            map_id=_map_id_from_md5(runtime_map_md5),
            map_md5=runtime_map_md5,
            old_map_id=asset_map_id,
            old_map_md5=asset_map_md5,
        )
        rospy.logwarn(
            "[slam_runtime_manager] rebound map identity map=%s old_id=%s old_md5=%s new_id=%s new_md5=%s",
            map_name,
            str((asset or {}).get("map_id") or "").strip() or "-",
            str((asset or {}).get("map_md5") or "").strip() or "-",
            str((rebound or {}).get("map_id") or "").strip() or "-",
            str((rebound or {}).get("map_md5") or "").strip() or "-",
        )
        return rebound

    def wait_until_ready(
        self,
        *,
        robot_id: str,
        asset: Dict[str, object],
        started_after_ts: float,
    ) -> Tuple[bool, str]:
        backend = self._backend
        runtime_context = backend._runtime_context
        runtime_state = backend._runtime_state
        deadline = time.time() + backend.ready_timeout_s
        target_name = str((asset or {}).get("map_name") or "").strip()
        target_revision_id = self._asset_revision_id(robot_id=robot_id, asset=asset)
        last_reason = "waiting_localization_ready"
        last_reported_state = ""
        while (time.time() < deadline) and (not rospy.is_shutdown()):
            now = time.time()
            tf_ok = False
            try:
                tf_ok = bool(
                    backend._tf_buffer.can_transform(
                        backend.tf_parent_frame,
                        backend.tf_child_frame,
                        rospy.Time(0),
                        rospy.Duration(backend.tf_poll_timeout_s),
                    )
                )
            except Exception:
                tf_ok = False

            tracked_ok = runtime_context.tracked_pose_fresh_since(
                started_after_ts=started_after_ts,
                now=now,
                freshness_timeout_s=backend.tracked_pose_fresh_timeout_s,
            )
            tracked_stable = runtime_context.tracked_pose_stable_since(
                started_after_ts=started_after_ts,
                now=now,
                freshness_timeout_s=backend.tracked_pose_fresh_timeout_s,
                stable_window_s=float(getattr(backend, "localization_stable_window_s", 0.0)),
            )
            map_ready = runtime_context.runtime_map_ready_since(started_after_ts=started_after_ts)
            selected = backend._plan_store.get_active_map(robot_id=robot_id) or {}
            selected_name = str(selected.get("map_name") or "").strip()
            selected_revision_id = str(selected.get("revision_id") or "").strip()
            pending_switch = backend._plan_store.get_pending_map_switch(robot_id=robot_id) or {}
            pending_target_name = str(pending_switch.get("target_map_name") or "").strip()
            selected_ok = (not target_name) or (selected_name == target_name) or (pending_target_name == target_name)
            canonical_asset = dict(asset or {})
            target_md5 = str(canonical_asset.get("map_md5") or "").strip()
            map_match = self._runtime_map_matches(
                runtime_context,
                target_md5=target_md5,
                target_revision_id=target_revision_id,
            )

            if tf_ok and tracked_ok and map_ready and selected_ok and (not map_match):
                try:
                    rebound = self.rebind_map_identity_from_runtime(
                        map_name=target_name,
                        asset=canonical_asset,
                    )
                except Exception as exc:
                    rospy.logwarn(
                        "[slam_runtime_manager] failed to rebind map identity map=%s err=%s",
                        target_name or "-",
                        exc,
                    )
                else:
                    if rebound:
                        canonical_asset = dict(rebound)
                        target_md5 = str(canonical_asset.get("map_md5") or "").strip()
                        map_match = self._runtime_map_matches(
                            runtime_context,
                            target_md5=target_md5,
                            target_revision_id=target_revision_id,
                        )

            ready_for_stabilization = tf_ok and tracked_ok and map_ready and map_match and selected_ok

            if ready_for_stabilization and (not tracked_stable):
                last_reason = "localization stabilizing"
                if last_reported_state != "stabilizing":
                    runtime_state.set_localization_state(
                        robot_id=robot_id,
                        map_name=target_name,
                        state="stabilizing",
                        valid=False,
                        map_revision_id=target_revision_id,
                    )
                    last_reported_state = "stabilizing"

            if ready_for_stabilization and tracked_stable:
                map_id = str((canonical_asset or {}).get("map_id") or "").strip()
                map_md5 = str(target_md5 or runtime_context.runtime_map_md5() or "").strip()
                if map_id:
                    rospy.set_param("/map_id", map_id)
                if map_md5:
                    rospy.set_param("/map_md5", map_md5)
                if target_revision_id:
                    rospy.set_param("/map_revision_id", target_revision_id)
                runtime_state.set_localization_state(
                    robot_id=robot_id,
                    map_name=target_name,
                    state="localized",
                    valid=True,
                    map_revision_id=target_revision_id,
                )
                return True, "localized"

            if not tf_ok:
                last_reason = "map->odom TF not ready"
            elif not tracked_ok:
                last_reason = "tracked_pose not fresh"
            elif ready_for_stabilization and (not tracked_stable):
                last_reason = "localization stabilizing"
            elif not map_ready:
                last_reason = runtime_map_identity_unavailable_message()
            elif not map_match:
                last_reason = (
                    runtime_map_mismatch_reason(
                        expected_label="requested map",
                        active_revision_id=target_revision_id,
                        active_map_name=target_name,
                        active_map_md5=target_md5,
                        runtime_revision_id=self._runtime_map_revision_id(runtime_context),
                        runtime_map_md5=runtime_context.runtime_map_md5(),
                    )
                    or "runtime map does not match requested map"
                )
            elif not selected_ok:
                last_reason = selected_map_does_not_match_requested_map_message(
                    selected_name,
                    target_name,
                    selected_revision_id=selected_revision_id,
                    requested_revision_id=target_revision_id,
                )

            rospy.sleep(0.2)

        runtime_state.set_localization_state(
            robot_id=robot_id,
            map_name=target_name,
            state="not_localized",
            valid=False,
            map_revision_id=target_revision_id,
        )
        return False, last_reason

    def reconcile_pending_map_switch(self, *, robot_id: str):
        backend = self._backend
        pending_switch = backend._plan_store.get_pending_map_switch(robot_id=robot_id) or {}
        if not pending_switch:
            return
        active_revision = backend._plan_store.get_active_map_revision(robot_id=robot_id) or {}
        runtime_revision_id = str(backend._runtime_context.runtime_map_revision_id() or "").strip()
        active_revision_id = str(active_revision.get("revision_id") or "").strip()
        if active_revision_id and runtime_revision_id and runtime_revision_id == active_revision_id:
            backend._plan_store.clear_pending_map_switch(robot_id=robot_id)
            return
        target_map_name = str(pending_switch.get("target_map_name") or "").strip()
        if target_map_name:
            self.record_failed_map_verification(
                map_name=target_map_name,
                revision_id=str(pending_switch.get("target_revision_id") or ""),
                error_code="stale_pending_map_switch",
                error_msg="cleared stale pending map switch during runtime manager startup recovery",
            )
        backend._plan_store.clear_pending_map_switch(robot_id=robot_id)

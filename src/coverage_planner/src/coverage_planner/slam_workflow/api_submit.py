# -*- coding: utf-8 -*-

"""Async submit/get_job handling for the public SLAM API service."""

from __future__ import annotations

import os
from typing import Any, Callable

from cleanrobot_app_msgs.msg import SlamJobState as AppSlamJobState
from cleanrobot_app_msgs.srv import (
    GetSlamJobResponse as AppGetSlamJobResponse,
    SubmitSlamCommandResponse as AppSubmitSlamCommandResponse,
)
from coverage_planner.app_msg_clone import clone_app_slam_job_state
from coverage_planner.map_asset_status import map_asset_is_verified
from coverage_planner.slam_workflow.api import (
    ASSET_MUST_EXIST_OPERATIONS,
    ASSET_MUST_NOT_EXIST_OPERATIONS,
    PATH_CONFLICT_CHECK_OPERATIONS,
    SUPPORTED_SUBMIT_OPERATIONS,
    SubmitValidationContext,
    normalize_map_name,
    validate_submit_request,
)


class SlamApiSubmitController:
    def __init__(self, backend: Any):
        self._backend = backend

    @staticmethod
    def _job_message(job_cls, converter: Callable[[Any], Any], job=None):
        if job is None:
            return job_cls()
        return converter(job)

    def _submit_response(
        self,
        response_cls,
        job_cls,
        job_converter: Callable[[Any], Any],
        *,
        accepted: bool,
        message: str,
        error_code: str,
        job_id: str,
        operation: int,
        map_name: str,
        job=None,
    ):
        return response_cls(
            accepted=bool(accepted),
            message=str(message or ""),
            error_code=str(error_code or ""),
            job_id=str(job_id or ""),
            operation=int(operation),
            map_name=str(map_name or ""),
            job=self._job_message(job_cls, job_converter, job),
        )

    def _handle_submit_command(
        self,
        req,
        *,
        response_cls,
        job_cls,
        job_converter: Callable[[Any], Any],
    ):
        backend = self._backend
        runtime_client = backend._runtime_client
        state_controller = backend._state_controller
        robot_id = str(req.robot_id or backend.robot_id).strip() or backend.robot_id
        operation = int(req.operation)
        map_name = normalize_map_name(req.map_name)
        map_revision_id = str(getattr(req, "map_revision_id", "") or "").strip()
        save_map_name = normalize_map_name(getattr(req, "save_map_name", ""))
        if map_revision_id:
            try:
                resolved_revision_asset = backend._runtime_assets.resolve_asset(
                    robot_id=robot_id,
                    map_name=map_name,
                    map_revision_id=map_revision_id,
                )
            except ValueError as exc:
                return self._submit_response(
                    response_cls,
                    job_cls,
                    job_converter,
                    accepted=False,
                    message=str(exc),
                    error_code="map_scope_mismatch",
                    job_id="",
                    operation=operation,
                    map_name=map_name,
                )
            if resolved_revision_asset is not None:
                resolved_map_name = normalize_map_name(str(resolved_revision_asset.get("map_name") or ""))
                if map_name and resolved_map_name and resolved_map_name != map_name:
                    return self._submit_response(
                        response_cls,
                        job_cls,
                        job_converter,
                        accepted=False,
                        message="map revision does not match selected map",
                        error_code="map_scope_mismatch",
                        job_id="",
                        operation=operation,
                        map_name=map_name,
                    )
                map_name = resolved_map_name or map_name
        state = state_controller.build_state(robot_id=robot_id, refresh_map_identity=False)
        odometry_state = state_controller.live_odometry_state()
        odometry_valid = bool(getattr(odometry_state, "odom_valid", False)) if odometry_state is not None else False

        if operation not in SUPPORTED_SUBMIT_OPERATIONS:
            return self._submit_response(
                response_cls,
                job_cls,
                job_converter,
                accepted=False,
                message="unsupported operation=%s" % operation,
                error_code="unsupported_operation",
                job_id="",
                operation=operation,
                map_name=map_name,
            )
        if not runtime_client.runtime_submit_job_available():
            return self._submit_response(
                response_cls,
                job_cls,
                job_converter,
                accepted=False,
                message="runtime submit job service unavailable",
                error_code="runtime_manager_unavailable",
                job_id="",
                operation=operation,
                map_name=map_name,
            )

        context = SubmitValidationContext(
            active_map_name=str(state.active_map_name or ""),
            current_mode=str(state.current_mode or ""),
            task_running=bool(state.task_running),
            can_switch_map_and_localize=bool(state.can_switch_map_and_localize),
            can_relocalize=bool(state.can_relocalize),
            can_verify_map_revision=bool(getattr(state, "can_verify_map_revision", False)),
            can_activate_map_revision=bool(getattr(state, "can_activate_map_revision", False)),
            can_start_mapping=bool(state.can_start_mapping),
            can_save_mapping=bool(state.can_save_mapping),
            can_stop_mapping=bool(state.can_stop_mapping),
            localization_backend_available=bool(state.localization_backend_available),
            odometry_valid=bool(odometry_valid),
        )
        validation = validate_submit_request(
            operation=operation,
            context=context,
            map_name=map_name,
            save_map_name=save_map_name,
        )
        effective_map_name = validation.effective_map_name
        map_asset_exists = False
        map_asset_verified = False
        asset_path_conflict = ""
        if effective_map_name and operation in (ASSET_MUST_EXIST_OPERATIONS | ASSET_MUST_NOT_EXIST_OPERATIONS):
            try:
                resolved_asset = backend._runtime_assets.resolve_asset(
                    robot_id=robot_id,
                    map_name=effective_map_name,
                    map_revision_id=map_revision_id,
                )
            except ValueError as exc:
                return self._submit_response(
                    response_cls,
                    job_cls,
                    job_converter,
                    accepted=False,
                    message=str(exc),
                    error_code="map_scope_mismatch",
                    job_id="",
                    operation=operation,
                    map_name=effective_map_name,
                )
            map_asset_exists = resolved_asset is not None
            map_asset_verified = map_asset_is_verified(resolved_asset or {})
        if effective_map_name and operation in PATH_CONFLICT_CHECK_OPERATIONS and not map_asset_exists:
            target_paths = backend._runtime_assets.target_paths(effective_map_name)
            for path in target_paths.values():
                if os.path.exists(path):
                    asset_path_conflict = path
                    break
        validation = validate_submit_request(
            operation=operation,
            context=context,
            map_name=map_name,
            save_map_name=save_map_name,
            map_asset_exists=bool(map_asset_exists),
            map_asset_verified=bool(map_asset_verified),
            asset_path_conflict=asset_path_conflict,
        )
        effective_map_name = validation.effective_map_name

        if not validation.ok:
            return self._submit_response(
                response_cls,
                job_cls,
                job_converter,
                accepted=False,
                message=validation.message,
                error_code=validation.error_code,
                job_id="",
                operation=operation,
                map_name=effective_map_name,
            )

        try:
            resp = runtime_client.call_runtime_submit_job(
                operation=operation,
                robot_id=robot_id,
                map_name=effective_map_name,
                map_revision_id=map_revision_id,
                set_active=bool(req.set_active),
                description=str(req.description or ""),
                frame_id=str(getattr(req, "frame_id", "map") or "map"),
                has_initial_pose=bool(getattr(req, "has_initial_pose", False)),
                initial_pose_x=float(getattr(req, "initial_pose_x", 0.0) or 0.0),
                initial_pose_y=float(getattr(req, "initial_pose_y", 0.0) or 0.0),
                initial_pose_yaw=float(getattr(req, "initial_pose_yaw", 0.0) or 0.0),
                save_map_name=save_map_name,
                include_unfinished_submaps=bool(getattr(req, "include_unfinished_submaps", True)),
                set_active_on_save=bool(getattr(req, "set_active_on_save", False) or req.set_active),
                switch_to_localization_after_save=bool(
                    getattr(req, "switch_to_localization_after_save", False)
                ),
                relocalize_after_switch=bool(getattr(req, "relocalize_after_switch", False)),
            )
            if bool(getattr(resp, "job", None)) and bool(getattr(resp.job, "job_id", "") or ""):
                backend._runtime_state.cache_job_state(resp.job)
            return self._submit_response(
                response_cls,
                job_cls,
                job_converter,
                accepted=bool(getattr(resp, "accepted", False)),
                message=str(getattr(resp, "message", "") or ""),
                error_code=str(getattr(resp, "error_code", "") or ""),
                job_id=str(getattr(resp, "job_id", "") or ""),
                operation=int(getattr(resp, "operation", operation)),
                map_name=str(getattr(resp, "map_name", effective_map_name) or ""),
                job=getattr(resp, "job", None),
            )
        except Exception as exc:
            return self._submit_response(
                response_cls,
                job_cls,
                job_converter,
                accepted=False,
                message=str(exc),
                error_code="runtime_submit_failed",
                job_id="",
                operation=operation,
                map_name=effective_map_name,
            )

    def handle_submit_command_app(self, req):
        return self._handle_submit_command(
            req,
            response_cls=AppSubmitSlamCommandResponse,
            job_cls=AppSlamJobState,
            job_converter=clone_app_slam_job_state,
        )

    def handle_get_job_app(self, req):
        backend = self._backend
        runtime_client = backend._runtime_client
        robot_id = str(req.robot_id or backend.robot_id).strip() or backend.robot_id
        job_id = str(req.job_id or "").strip()
        if runtime_client.runtime_get_job_available():
            try:
                resp = runtime_client.call_runtime_get_job(job_id=job_id, robot_id=robot_id)
                job = clone_app_slam_job_state(getattr(resp, "job", None))
                if bool(getattr(resp, "found", False)) and bool(getattr(job, "job_id", "") or ""):
                    backend._runtime_state.cache_job_state(job)
                return AppGetSlamJobResponse(
                    found=bool(getattr(resp, "found", False)),
                    message=str(getattr(resp, "message", "") or ""),
                    error_code=str(getattr(resp, "error_code", "") or ""),
                    job=job,
                )
            except Exception as exc:
                return AppGetSlamJobResponse(
                    found=False,
                    message=str(exc),
                    error_code="runtime_get_job_failed",
                    job=AppSlamJobState(),
                )

        # Runtime get_job is the canonical path. The cache is only a same-job snapshot
        # fallback for brief backend unavailability, never a "latest job" lookup.
        cached = backend._job_state_msg
        if cached is None:
            return AppGetSlamJobResponse(
                found=False,
                message="slam job not found",
                error_code="job_not_found",
                job=AppSlamJobState(),
            )
        if job_id and str(getattr(cached, "job_id", "") or "") != job_id:
            return AppGetSlamJobResponse(
                found=False,
                message="slam job not found",
                error_code="job_not_found",
                job=AppSlamJobState(),
            )
        return AppGetSlamJobResponse(
            found=True,
            message="ok",
            error_code="",
            job=clone_app_slam_job_state(cached),
        )

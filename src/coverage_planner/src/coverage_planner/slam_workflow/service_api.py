# -*- coding: utf-8 -*-

"""Service-layer helpers for the formal SLAM runtime backend."""

from __future__ import annotations

import threading
from typing import Any

from cleanrobot_app_msgs.msg import SlamJobState as AppSlamJobState
from cleanrobot_app_msgs.srv import (
    GetSlamJob as AppGetSlamJob,
    GetSlamJobResponse as AppGetSlamJobResponse,
    OperateSlamRuntime as AppOperateSlamRuntime,
    OperateSlamRuntimeResponse as AppOperateSlamRuntimeResponse,
    SubmitSlamCommand as AppSubmitSlamCommand,
    SubmitSlamCommandResponse as AppSubmitSlamCommandResponse,
)

from coverage_planner.app_msg_clone import clone_app_slam_job_state
from coverage_planner.ros_contract import build_contract_report, validate_ros_contract
from coverage_planner.slam_workflow.api import SUPPORTED_SUBMIT_OPERATIONS, normalize_map_name
from coverage_planner.slam_workflow.executor import LocalizationRequest


class SlamRuntimeServiceController:
    def __init__(self, backend: Any):
        self._backend = backend

    def _resolve_effective_map_name(self, *, robot_id: str, map_name: str, map_revision_id: str = "") -> str:
        normalized_name = normalize_map_name(map_name)
        normalized_revision_id = str(map_revision_id or "").strip()
        if not normalized_revision_id:
            return normalized_name
        asset_helper = getattr(self._backend, "_asset_helper", None)
        resolver = getattr(asset_helper, "resolve_asset", None)
        if not callable(resolver):
            return normalized_name
        try:
            asset = resolver(
                robot_id=str(robot_id or self._backend.robot_id).strip() or self._backend.robot_id,
                map_name=normalized_name,
                map_revision_id=normalized_revision_id,
            )
        except ValueError:
            raise
        except Exception:
            asset = None
        resolved_name = normalize_map_name(str((asset or {}).get("map_name") or ""))
        if normalized_name and resolved_name and resolved_name != normalized_name:
            raise ValueError("map revision does not match selected map")
        return resolved_name or normalized_name

    def response(
        self,
        *,
        success: bool,
        message: str,
        error_code: str,
        operation: int,
        map_name: str,
        map_revision_id: str = "",
        localization_state: str,
        current_mode: str,
    ) -> AppOperateSlamRuntimeResponse:
        return AppOperateSlamRuntimeResponse(
            success=bool(success),
            message=str(message or ""),
            error_code=str(error_code or ""),
            operation=int(operation),
            map_name=str(map_name or ""),
            map_revision_id=str(map_revision_id or ""),
            localization_state=str(localization_state or ""),
            current_mode=str(current_mode or ""),
        )

    def build_app_contract_reports(self):
        backend = self._backend
        validate_ros_contract(
            "AppOperateSlamRuntimeRequest",
            AppOperateSlamRuntime._request_class,
            required_fields=["operation", "robot_id", "map_name", "map_revision_id", "set_active", "description"],
            required_constants=[
                "restart_localization",
                "start_mapping",
                "save_mapping",
                "stop_mapping",
                "verify_map_revision",
                "activate_map_revision",
            ],
        )
        validate_ros_contract(
            "AppOperateSlamRuntimeResponse",
            AppOperateSlamRuntimeResponse,
            required_fields=[
                "success",
                "message",
                "error_code",
                "operation",
                "map_name",
                "map_revision_id",
                "localization_state",
                "current_mode",
            ],
        )
        validate_ros_contract(
            "AppSubmitSlamCommandRequest",
            AppSubmitSlamCommand._request_class,
            required_fields=["operation", "robot_id", "map_name", "set_active", "description"],
            required_constants=[
                "start_mapping",
                "save_mapping",
                "stop_mapping",
                "prepare_for_task",
                "switch_map_and_localize",
                "relocalize",
                "verify_map_revision",
                "activate_map_revision",
            ],
        )
        validate_ros_contract(
            "AppSubmitSlamCommandResponse",
            AppSubmitSlamCommandResponse,
            required_fields=["accepted", "message", "error_code", "job_id", "operation", "map_name", "job"],
        )
        validate_ros_contract(
            "AppGetSlamJobRequest",
            AppGetSlamJob._request_class,
            required_fields=["job_id", "robot_id"],
        )
        validate_ros_contract(
            "AppGetSlamJobResponse",
            AppGetSlamJobResponse,
            required_fields=["found", "message", "error_code", "job"],
        )
        operate_contract = build_contract_report(
            service_name=backend.app_service_name,
            contract_name="runtime_operate_app",
            service_cls=AppOperateSlamRuntime,
            request_cls=AppOperateSlamRuntime._request_class,
            response_cls=AppOperateSlamRuntimeResponse,
            dependencies={},
            features=[
                "runtime_operation_control",
                "restart_localization",
                "start_mapping",
                "save_mapping",
                "stop_mapping",
                "verify_map_revision",
                "activate_map_revision",
                "cleanrobot_app_msgs_parallel",
            ],
        )
        submit_contract = build_contract_report(
            service_name=backend.app_submit_job_service_name,
            contract_name="runtime_submit_job_app",
            service_cls=AppSubmitSlamCommand,
            request_cls=AppSubmitSlamCommand._request_class,
            response_cls=AppSubmitSlamCommandResponse,
            dependencies={"job": AppSlamJobState},
            features=["async_job_submission", "runtime_manager_backend", "cleanrobot_app_msgs_parallel"],
        )
        get_job_contract = build_contract_report(
            service_name=backend.app_get_job_service_name,
            contract_name="runtime_get_job_app",
            service_cls=AppGetSlamJob,
            request_cls=AppGetSlamJob._request_class,
            response_cls=AppGetSlamJobResponse,
            dependencies={"job": AppSlamJobState},
            features=["async_job_query", "runtime_manager_backend", "cleanrobot_app_msgs_parallel"],
        )
        return operate_contract, submit_contract, get_job_contract

    @staticmethod
    def make_localization_request(
        *,
        robot_id: str,
        map_name: str,
        map_revision_id: str,
        operation: int,
        frame_id: str,
        has_initial_pose: bool,
        initial_pose_x: float,
        initial_pose_y: float,
        initial_pose_yaw: float,
    ) -> LocalizationRequest:
        return LocalizationRequest(
            robot_id=robot_id,
            map_name=normalize_map_name(map_name),
            map_revision_id=str(map_revision_id or "").strip(),
            operation=operation,
            frame_id=frame_id,
            has_initial_pose=bool(has_initial_pose),
            initial_pose_x=float(initial_pose_x),
            initial_pose_y=float(initial_pose_y),
            initial_pose_yaw=float(initial_pose_yaw),
        )

    def execute_operation(
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
    ) -> AppOperateSlamRuntimeResponse:
        backend = self._backend
        runtime_adapter = backend._runtime_adapter
        workflow_executor = backend._workflow_executor
        try:
            effective_map_name = self._resolve_effective_map_name(
                robot_id=robot_id,
                map_name=map_name,
                map_revision_id=map_revision_id,
            )
        except ValueError as exc:
            return self.response(
                success=False,
                message=str(exc),
                error_code="map_scope_mismatch",
                operation=operation,
                map_name=normalize_map_name(map_name),
                map_revision_id=map_revision_id,
                localization_state="not_localized",
                current_mode="",
            )
        if operation == int(AppOperateSlamRuntime._request_class.restart_localization):
            return runtime_adapter.restart_localization(
                robot_id=robot_id,
                map_name=effective_map_name,
                map_revision_id=map_revision_id,
                operation=operation,
                frame_id=frame_id,
                has_initial_pose=has_initial_pose,
                initial_pose_x=initial_pose_x,
                initial_pose_y=initial_pose_y,
                initial_pose_yaw=initial_pose_yaw,
            )
        if operation == int(AppOperateSlamRuntime._request_class.start_mapping):
            return runtime_adapter.start_mapping(robot_id=robot_id, operation=operation)
        if operation == int(AppOperateSlamRuntime._request_class.save_mapping):
            return runtime_adapter.save_mapping(
                robot_id=robot_id,
                map_name=effective_map_name,
                description=description,
                set_active=bool(set_active),
                operation=operation,
                include_unfinished_submaps=bool(include_unfinished_submaps),
                switch_to_localization_after_save=bool(switch_to_localization_after_save),
                relocalize_after_switch=bool(relocalize_after_switch),
            )
        if operation == int(AppOperateSlamRuntime._request_class.stop_mapping):
            return runtime_adapter.stop_mapping(
                robot_id=robot_id,
                map_name=effective_map_name,
                map_revision_id=map_revision_id,
                operation=operation,
            )
        if operation == int(AppOperateSlamRuntime._request_class.verify_map_revision):
            return runtime_adapter.verify_map_revision(
                robot_id=robot_id,
                map_name=effective_map_name,
                map_revision_id=map_revision_id,
                operation=operation,
                frame_id=frame_id,
                has_initial_pose=has_initial_pose,
                initial_pose_x=initial_pose_x,
                initial_pose_y=initial_pose_y,
                initial_pose_yaw=initial_pose_yaw,
            )
        if operation == int(AppOperateSlamRuntime._request_class.activate_map_revision):
            return runtime_adapter.activate_map_revision(
                robot_id=robot_id,
                map_name=effective_map_name,
                map_revision_id=map_revision_id,
                operation=operation,
                frame_id=frame_id,
                has_initial_pose=has_initial_pose,
                initial_pose_x=initial_pose_x,
                initial_pose_y=initial_pose_y,
                initial_pose_yaw=initial_pose_yaw,
            )
        if operation == int(AppSubmitSlamCommand._request_class.prepare_for_task):
            return workflow_executor.prepare_for_task(
                self.make_localization_request(
                    robot_id=robot_id,
                    map_name=effective_map_name,
                    map_revision_id=map_revision_id,
                    operation=operation,
                    frame_id=frame_id,
                    has_initial_pose=has_initial_pose,
                    initial_pose_x=initial_pose_x,
                    initial_pose_y=initial_pose_y,
                    initial_pose_yaw=initial_pose_yaw,
                )
            )
        if operation == int(AppSubmitSlamCommand._request_class.switch_map_and_localize):
            return workflow_executor.switch_map_and_localize(
                self.make_localization_request(
                    robot_id=robot_id,
                    map_name=effective_map_name,
                    map_revision_id=map_revision_id,
                    operation=operation,
                    frame_id=frame_id,
                    has_initial_pose=has_initial_pose,
                    initial_pose_x=initial_pose_x,
                    initial_pose_y=initial_pose_y,
                    initial_pose_yaw=initial_pose_yaw,
                )
            )
        if operation == int(AppSubmitSlamCommand._request_class.relocalize):
            return workflow_executor.relocalize(
                self.make_localization_request(
                    robot_id=robot_id,
                    map_name=effective_map_name,
                    map_revision_id=map_revision_id,
                    operation=operation,
                    frame_id=frame_id,
                    has_initial_pose=has_initial_pose,
                    initial_pose_x=initial_pose_x,
                    initial_pose_y=initial_pose_y,
                    initial_pose_yaw=initial_pose_yaw,
                )
            )
        if operation == int(AppSubmitSlamCommand._request_class.verify_map_revision):
            return runtime_adapter.verify_map_revision(
                robot_id=robot_id,
                map_name=effective_map_name,
                map_revision_id=map_revision_id,
                operation=int(AppOperateSlamRuntime._request_class.verify_map_revision),
                frame_id=frame_id,
                has_initial_pose=has_initial_pose,
                initial_pose_x=initial_pose_x,
                initial_pose_y=initial_pose_y,
                initial_pose_yaw=initial_pose_yaw,
            )
        if operation == int(AppSubmitSlamCommand._request_class.activate_map_revision):
            return runtime_adapter.activate_map_revision(
                robot_id=robot_id,
                map_name=effective_map_name,
                map_revision_id=map_revision_id,
                operation=int(AppOperateSlamRuntime._request_class.activate_map_revision),
                frame_id=frame_id,
                has_initial_pose=has_initial_pose,
                initial_pose_x=initial_pose_x,
                initial_pose_y=initial_pose_y,
                initial_pose_yaw=initial_pose_yaw,
            )
        return self.response(
            success=False,
            message="unsupported operation=%s" % operation,
            error_code="unsupported_operation",
            operation=operation,
            map_name=effective_map_name,
            localization_state="",
            current_mode="",
        )

    def _job_state_msg(self, snapshot=None) -> AppSlamJobState:
        return clone_app_slam_job_state(self._backend._job_state.job_to_msg(snapshot))

    def handle_submit_job_app(self, req):
        backend = self._backend
        job_state = backend._job_state
        robot_id = str(req.robot_id or backend.robot_id).strip() or backend.robot_id
        raw_map_name = normalize_map_name(req.map_name)
        map_revision_id = str(getattr(req, "map_revision_id", "") or "").strip()
        try:
            map_name = self._resolve_effective_map_name(
                robot_id=robot_id,
                map_name=raw_map_name,
                map_revision_id=map_revision_id,
            )
        except ValueError as exc:
            job = self._job_state_msg(None)
            return AppSubmitSlamCommandResponse(
                accepted=False,
                message=str(exc),
                error_code="map_scope_mismatch",
                job_id="",
                operation=int(req.operation),
                map_name=raw_map_name,
                job=job,
            )
        save_map_name = normalize_map_name(getattr(req, "save_map_name", ""))
        operation = int(req.operation)
        if operation not in SUPPORTED_SUBMIT_OPERATIONS:
            job = self._job_state_msg(None)
            return AppSubmitSlamCommandResponse(
                accepted=False,
                message="unsupported operation=%s" % operation,
                error_code="unsupported_operation",
                job_id="",
                operation=operation,
                map_name=map_name,
                job=job,
            )
        if job_state.job_running():
            active = job_state.get_job_snapshot()
            active_msg = self._job_state_msg(active)
            return AppSubmitSlamCommandResponse(
                accepted=False,
                message="another slam job is already running: %s" % str(active_msg.job_id or ""),
                error_code="job_in_progress",
                job_id=str(active_msg.job_id or ""),
                operation=operation,
                map_name=map_name,
                job=active_msg,
            )

        requested_map_name = save_map_name if operation == int(req.save_mapping) and save_map_name else map_name
        job = job_state.make_job_record(
            operation=operation,
            robot_id=robot_id,
            map_name=requested_map_name,
            map_revision_id=map_revision_id,
            set_active=bool(getattr(req, "set_active_on_save", False) or req.set_active),
            description=str(req.description or ""),
            frame_id=str(getattr(req, "frame_id", "map") or "map"),
            has_initial_pose=bool(getattr(req, "has_initial_pose", False)),
            initial_pose_x=float(getattr(req, "initial_pose_x", 0.0) or 0.0),
            initial_pose_y=float(getattr(req, "initial_pose_y", 0.0) or 0.0),
            initial_pose_yaw=float(getattr(req, "initial_pose_yaw", 0.0) or 0.0),
            include_unfinished_submaps=bool(getattr(req, "include_unfinished_submaps", True)),
            switch_to_localization_after_save=bool(
                getattr(req, "switch_to_localization_after_save", False)
            ),
            relocalize_after_switch=bool(getattr(req, "relocalize_after_switch", False)),
        )
        snapshot = job_state.publish_job_snapshot(job, sync_runtime=True, update_error=False)
        worker = threading.Thread(
            target=backend._job_runner.run_job,
            args=(str(snapshot.get("job_id") or ""),),
            name="slam_job_%s" % str(snapshot.get("job_id") or ""),
            daemon=True,
        )
        worker.start()
        return AppSubmitSlamCommandResponse(
            accepted=True,
            message="accepted",
            error_code="",
            job_id=str(snapshot.get("job_id") or ""),
            operation=operation,
            map_name=str(snapshot.get("requested_map_name") or ""),
            job=self._job_state_msg(snapshot),
        )

    def handle_get_job_app(self, req):
        backend = self._backend
        job_state = backend._job_state
        robot_id = str(req.robot_id or backend.robot_id).strip() or backend.robot_id
        snapshot = job_state.get_job_snapshot(str(req.job_id or ""))
        if not snapshot:
            return AppGetSlamJobResponse(
                found=False,
                message="slam job not found",
                error_code="job_not_found",
                job=self._job_state_msg(None),
            )
        if str(snapshot.get("robot_id") or backend.robot_id) != robot_id:
            return AppGetSlamJobResponse(
                found=False,
                message="slam job not found for robot_id=%s" % robot_id,
                error_code="job_not_found",
                job=self._job_state_msg(None),
            )
        return AppGetSlamJobResponse(
            found=True,
            message="ok",
            error_code="",
            job=self._job_state_msg(snapshot),
        )

    def _handle_operate(self, req):
        backend = self._backend
        job_state = backend._job_state
        robot_id = str(req.robot_id or backend.robot_id).strip() or backend.robot_id
        raw_map_name = normalize_map_name(req.map_name)
        map_revision_id = str(getattr(req, "map_revision_id", "") or "").strip()
        try:
            map_name = self._resolve_effective_map_name(
                robot_id=robot_id,
                map_name=raw_map_name,
                map_revision_id=map_revision_id,
            )
        except ValueError as exc:
            return self.response(
                success=False,
                message=str(exc),
                error_code="map_scope_mismatch",
                operation=int(req.operation),
                map_name=raw_map_name,
                map_revision_id=map_revision_id,
                localization_state="not_localized",
                current_mode="",
            )
        operation = int(req.operation)
        if job_state.job_running():
            active = job_state.get_job_snapshot()
            active_job_id = str((active or {}).get("job_id") or "").strip()
            return self.response(
                success=False,
                message="slam job in progress: %s" % (active_job_id or "unknown"),
                error_code="job_in_progress",
                operation=operation,
                map_name=map_name,
                localization_state=str((active or {}).get("localization_state") or ""),
                current_mode=str((active or {}).get("current_mode") or ""),
            )
        with backend._lock:
            resp = self.execute_operation(
                operation=operation,
                robot_id=robot_id,
                map_name=map_name,
                map_revision_id=map_revision_id,
                set_active=bool(req.set_active),
                description=str(req.description or ""),
            )
        backend._runtime_state.update_runtime_state(
            robot_id=robot_id,
            active_job_id="",
            last_error_code="" if bool(resp.success) else str(resp.error_code or ""),
            last_error_msg="" if bool(resp.success) else str(resp.message or ""),
        )
        return resp

    def handle_operate_app(self, req):
        return self._handle_operate(req)

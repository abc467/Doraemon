# -*- coding: utf-8 -*-

"""ROS contract validation and report helpers for the public SLAM API service."""

from __future__ import annotations

from typing import Any, Dict, Tuple

from cleanrobot_app_msgs.msg import SlamJobState as AppSlamJobState, SlamState as AppSlamState
from cleanrobot_app_msgs.srv import (
    GetSlamJob as AppGetSlamJob,
    GetSlamJobResponse as AppGetSlamJobResponse,
    GetSlamStatus as AppGetSlamStatus,
    GetSlamStatusResponse as AppGetSlamStatusResponse,
    SubmitSlamCommand as AppSubmitSlamCommand,
    SubmitSlamCommandResponse as AppSubmitSlamCommandResponse,
)

from coverage_planner.ros_contract import build_contract_report, validate_ros_contract


class SlamApiContractController:
    def __init__(self, backend: Any):
        self._backend = backend

    def build_app_contract_reports(self) -> Tuple[Dict[str, object], Dict[str, object], Dict[str, object]]:
        backend = self._backend
        validate_ros_contract(
            "AppSlamState",
            AppSlamState,
            required_fields=[
                "robot_id",
                "desired_mode",
                "current_mode",
                "active_map_name",
                "active_map_revision_id",
                "runtime_map_name",
                "runtime_map_revision_id",
                "pending_map_name",
                "pending_map_revision_id",
                "pending_map_switch_status",
                "localization_state",
                "lifecycle_state",
                "active_job_id",
                "active_job_status",
                "active_job_phase",
                "active_job_progress_0_1",
                "workflow_state",
                "workflow_phase",
                "task_ready",
                "can_switch_map_and_localize",
                "can_relocalize",
                "can_verify_map_revision",
                "can_activate_map_revision",
                "can_start_mapping",
                "can_save_mapping",
                "can_stop_mapping",
                "blocking_reasons",
                "warnings",
                "stamp",
            ],
        )
        validate_ros_contract(
            "AppSlamJobState",
            AppSlamJobState,
            required_fields=[
                "job_id",
                "robot_id",
                "operation",
                "operation_name",
                "requested_map_name",
                "requested_map_revision_id",
                "resolved_map_revision_id",
                "status",
                "phase",
                "progress_0_1",
                "job_state",
                "workflow_phase",
                "progress_percent",
                "done",
                "success",
                "error_code",
                "message",
                "current_mode",
                "localization_state",
                "updated_at",
            ],
        )
        validate_ros_contract(
            "AppGetSlamStatusRequest",
            AppGetSlamStatus._request_class,
            required_fields=["robot_id", "refresh_map_identity"],
        )
        validate_ros_contract(
            "AppGetSlamStatusResponse",
            AppGetSlamStatusResponse,
            required_fields=["success", "message", "state"],
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
        validate_ros_contract(
            "AppSubmitSlamCommandRequest",
            AppSubmitSlamCommand._request_class,
            required_fields=[
                "operation",
                "robot_id",
                "map_name",
                "map_revision_id",
                "set_active",
                "description",
            ],
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
        status_contract = build_contract_report(
            service_name=backend.app_status_service_name,
            contract_name="get_slam_status_app",
            service_cls=AppGetSlamStatus,
            request_cls=AppGetSlamStatus._request_class,
            response_cls=AppGetSlamStatusResponse,
            dependencies={"state": AppSlamState},
            features=["structured_slam_state", "frontend_read_only", "workflow_state_projection", "cleanrobot_app_msgs_parallel"],
        )
        submit_contract = build_contract_report(
            service_name=backend.app_submit_command_service_name,
            contract_name="submit_slam_command_app",
            service_cls=AppSubmitSlamCommand,
            request_cls=AppSubmitSlamCommand._request_class,
            response_cls=AppSubmitSlamCommandResponse,
            dependencies={"job": AppSlamJobState},
            features=[
                "async_job_submission",
                "start_mapping",
                "save_mapping",
                "stop_mapping",
                "prepare_for_task",
                "switch_map_and_localize",
                "relocalize",
                "verify_map_revision",
                "activate_map_revision",
                "cleanrobot_app_msgs_parallel",
            ],
        )
        get_job_contract = build_contract_report(
            service_name=backend.app_get_job_service_name,
            contract_name="get_slam_job_app",
            service_cls=AppGetSlamJob,
            request_cls=AppGetSlamJob._request_class,
            response_cls=AppGetSlamJobResponse,
            dependencies={"job": AppSlamJobState},
            features=["async_job_query", "cached_job_snapshot_fallback", "cleanrobot_app_msgs_parallel"],
        )
        return status_contract, submit_contract, get_job_contract

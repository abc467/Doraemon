# -*- coding: utf-8 -*-

"""Runtime RPC and script helpers for the public SLAM API service."""

from __future__ import annotations

from typing import Any, Tuple

import rospy
import rosservice

from cleanrobot_app_msgs.srv import GetSlamJob as AppGetSlamJob, SubmitSlamCommand as AppSubmitSlamCommand
from coverage_planner.canonical_contract_types import (
    APP_GET_SLAM_JOB_SERVICE_TYPE,
    APP_SUBMIT_SLAM_COMMAND_SERVICE_TYPE,
)


class SlamApiRuntimeClient:
    def __init__(self, backend: Any):
        self._backend = backend

    def _runtime_submit_job_endpoint(self):
        backend = self._backend
        app_service_name = str(getattr(backend, "app_runtime_submit_job_service", "") or "").strip()
        return app_service_name, AppSubmitSlamCommand

    def _runtime_get_job_endpoint(self):
        backend = self._backend
        app_service_name = str(getattr(backend, "app_runtime_get_job_service", "") or "").strip()
        return app_service_name, AppGetSlamJob

    def service_available(self, service_name: str, expected_type: str) -> bool:
        try:
            runtime_type = str(rosservice.get_service_type(service_name) or "").strip()
            if not runtime_type:
                return False
            return (not expected_type) or (runtime_type == str(expected_type or "").strip())
        except Exception:
            return False

    def runtime_submit_job_available(self) -> bool:
        backend = self._backend
        app_service_name = str(getattr(backend, "app_runtime_submit_job_service", "") or "").strip()
        return bool(app_service_name) and self.service_available(app_service_name, APP_SUBMIT_SLAM_COMMAND_SERVICE_TYPE)

    def runtime_get_job_available(self) -> bool:
        backend = self._backend
        app_service_name = str(getattr(backend, "app_runtime_get_job_service", "") or "").strip()
        return bool(app_service_name) and self.service_available(app_service_name, APP_GET_SLAM_JOB_SERVICE_TYPE)

    def restart_backend_available(self) -> bool:
        return self.runtime_submit_job_available()

    def mapping_runtime_available(self) -> bool:
        # Public runtime capability is now defined by the app submit_job surface.
        return self.runtime_submit_job_available()

    def save_runtime_available(self) -> bool:
        # save_mapping is submitted as a runtime job on the canonical app surface.
        return self.runtime_submit_job_available()

    def call_runtime_submit_job(
        self,
        *,
        operation: int,
        robot_id: str,
        map_name: str = "",
        map_revision_id: str = "",
        set_active: bool = False,
        description: str = "",
        frame_id: str = "map",
        has_initial_pose: bool = False,
        initial_pose_x: float = 0.0,
        initial_pose_y: float = 0.0,
        initial_pose_yaw: float = 0.0,
        save_map_name: str = "",
        include_unfinished_submaps: bool = True,
        set_active_on_save: bool = False,
        switch_to_localization_after_save: bool = False,
        relocalize_after_switch: bool = False,
    ):
        backend = self._backend
        service_name, service_cls = self._runtime_submit_job_endpoint()
        rospy.wait_for_service(service_name, timeout=float(backend.command_timeout_s))
        cli = rospy.ServiceProxy(service_name, service_cls)
        return cli(
            operation=int(operation),
            robot_id=str(robot_id or backend.robot_id),
            map_name=str(map_name or ""),
            map_revision_id=str(map_revision_id or ""),
            set_active=bool(set_active),
            description=str(description or ""),
            frame_id=str(frame_id or "map"),
            has_initial_pose=bool(has_initial_pose),
            initial_pose_x=float(initial_pose_x or 0.0),
            initial_pose_y=float(initial_pose_y or 0.0),
            initial_pose_yaw=float(initial_pose_yaw or 0.0),
            save_map_name=str(save_map_name or ""),
            include_unfinished_submaps=bool(include_unfinished_submaps),
            set_active_on_save=bool(set_active_on_save),
            switch_to_localization_after_save=bool(switch_to_localization_after_save),
            relocalize_after_switch=bool(relocalize_after_switch),
        )

    def call_runtime_get_job(self, *, job_id: str, robot_id: str):
        backend = self._backend
        service_name, service_cls = self._runtime_get_job_endpoint()
        rospy.wait_for_service(service_name, timeout=float(backend.command_timeout_s))
        cli = rospy.ServiceProxy(service_name, service_cls)
        return cli(job_id=str(job_id or ""), robot_id=str(robot_id or backend.robot_id))

# -*- coding: utf-8 -*-

"""Runtime state and lightweight cache helpers for the public SLAM API service."""

from __future__ import annotations

import time
from typing import Any, Optional

import rospy

from coverage_planner.app_msg_clone import clone_app_slam_job_state

from coverage_planner.ops_store.store import RobotRuntimeStateRecord


class SlamApiRuntimeStateController:
    def __init__(self, backend: Any):
        self._backend = backend

    def runtime_param(self, key: str) -> str:
        backend = self._backend
        return backend.runtime_ns + "/" + str(key or "").strip()

    def get_runtime_record(self, robot_id: str) -> RobotRuntimeStateRecord:
        backend = self._backend
        return backend._ops.get_robot_runtime_state(robot_id=robot_id) or RobotRuntimeStateRecord(robot_id=robot_id)

    def update_runtime_state(
        self,
        *,
        robot_id: str,
        map_name: Optional[str] = None,
        map_revision_id: Optional[str] = None,
        localization_state: Optional[str] = None,
        localization_valid: Optional[bool] = None,
    ):
        backend = self._backend
        current = self.get_runtime_record(robot_id)
        backend._ops.upsert_robot_runtime_state(
            RobotRuntimeStateRecord(
                robot_id=robot_id,
                active_run_id=str(current.active_run_id or ""),
                active_job_id=str(current.active_job_id or ""),
                active_schedule_id=str(current.active_schedule_id or ""),
                map_name=str(map_name if map_name is not None else current.map_name or "").strip(),
                map_revision_id=str(
                    map_revision_id if map_revision_id is not None else current.map_revision_id or ""
                ).strip(),
                localization_state=(
                    str(localization_state)
                    if localization_state is not None
                    else str(current.localization_state or "")
                ),
                localization_valid=(
                    bool(localization_valid)
                    if localization_valid is not None
                    else bool(current.localization_valid)
                ),
                mission_state=str(current.mission_state or "IDLE"),
                phase=str(current.phase or "IDLE"),
                public_state=str(current.public_state or "IDLE"),
                return_to_dock_on_finish=current.return_to_dock_on_finish,
                repeat_after_full_charge=current.repeat_after_full_charge,
                armed=bool(current.armed),
                dock_state=str(current.dock_state or ""),
                battery_soc=float(current.battery_soc or 0.0),
                battery_valid=bool(current.battery_valid),
                executor_state=str(current.executor_state or ""),
                last_error_code=str(current.last_error_code or ""),
                last_error_msg=str(current.last_error_msg or ""),
                updated_ts=time.time(),
            )
        )

    def set_runtime_mode(self, *, mode: str, map_name: Optional[str], pbstream_path: Optional[str]):
        rospy.set_param(self.runtime_param("mode"), str(mode or "").strip())
        if map_name is not None:
            rospy.set_param(self.runtime_param("map_name"), str(map_name or "").strip())
        if pbstream_path is not None:
            rospy.set_param(self.runtime_param("pbstream_path"), str(pbstream_path or "").strip())

    def set_current_mode(self, mode: str):
        rospy.set_param(self.runtime_param("current_mode"), str(mode or "").strip())

    def set_localization_flags(
        self,
        *,
        robot_id: str,
        state: str,
        valid: bool,
        map_name: Optional[str] = None,
        map_revision_id: Optional[str] = None,
    ):
        rospy.set_param(self.runtime_param("localization_state"), str(state or "").strip())
        rospy.set_param(self.runtime_param("localization_valid"), bool(valid))
        rospy.set_param(self.runtime_param("localization_stamp"), float(rospy.Time.now().to_sec()))
        if map_revision_id is not None:
            rospy.set_param(self.runtime_param("current_map_revision_id"), str(map_revision_id or "").strip())
            try:
                rospy.set_param("/map_revision_id", str(map_revision_id or "").strip())
            except Exception:
                pass
        self.update_runtime_state(
            robot_id=robot_id,
            map_name=map_name,
            map_revision_id=map_revision_id,
            localization_state=str(state or "").strip(),
            localization_valid=bool(valid),
        )

    def cache_job_state(self, job):
        backend = self._backend
        if job is None:
            return
        backend._job_state_msg = clone_app_slam_job_state(job)
        backend._job_state_ts = time.time()
        try:
            backend._job_state_pub.publish(backend._job_state_msg)
        except Exception as exc:
            rospy.logwarn_throttle(5.0, "[slam_api_service] publish cached job state failed: %s", str(exc))

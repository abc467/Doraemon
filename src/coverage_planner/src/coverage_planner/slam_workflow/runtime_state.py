# -*- coding: utf-8 -*-

"""Runtime state read/write helpers for the formal SLAM backend."""

from __future__ import annotations

import time
from typing import Any, Optional

import rospy

from coverage_planner.ops_store.store import RobotRuntimeStateRecord


class CartographerRuntimeStateController:
    def __init__(self, backend: Any):
        self._backend = backend

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
        active_job_id: Optional[str] = None,
        last_error_code: Optional[str] = None,
        last_error_msg: Optional[str] = None,
    ):
        backend = self._backend
        current = self.get_runtime_record(robot_id)
        backend._ops.upsert_robot_runtime_state(
            RobotRuntimeStateRecord(
                robot_id=robot_id,
                active_run_id=str(current.active_run_id or ""),
                active_job_id=(
                    str(active_job_id if active_job_id is not None else current.active_job_id or "").strip()
                ),
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
                last_error_code=(
                    str(last_error_code if last_error_code is not None else current.last_error_code or "").strip()
                ),
                last_error_msg=(
                    str(last_error_msg if last_error_msg is not None else current.last_error_msg or "").strip()
                ),
                updated_ts=time.time(),
            )
        )

    def set_runtime_mode(
        self,
        *,
        mode: str,
        map_name: Optional[str],
        pbstream_path: Optional[str],
        map_revision_id: Optional[str] = None,
    ):
        backend = self._backend
        runtime_param = backend._runtime_context.runtime_param
        rospy.set_param(runtime_param("mode"), str(mode or "").strip())
        if map_name is not None:
            rospy.set_param(runtime_param("map_name"), str(map_name or "").strip())
        if pbstream_path is not None:
            rospy.set_param(runtime_param("pbstream_path"), str(pbstream_path or "").strip())
        if map_revision_id is not None:
            rospy.set_param(runtime_param("map_revision_id"), str(map_revision_id or "").strip())

    def publish_runtime_snapshot(
        self,
        *,
        current_mode: str,
        map_name: str,
        pbstream_path: str,
        map_revision_id: str = "",
    ):
        backend = self._backend
        runtime_param = backend._runtime_context.runtime_param
        rospy.set_param(runtime_param("current_mode"), str(current_mode or "").strip())
        rospy.set_param(runtime_param("current_map_name"), str(map_name or "").strip())
        rospy.set_param(runtime_param("current_pbstream_path"), str(pbstream_path or "").strip())
        rospy.set_param(runtime_param("current_map_revision_id"), str(map_revision_id or "").strip())
        try:
            rospy.set_param("/map_revision_id", str(map_revision_id or "").strip())
        except Exception:
            pass

    def set_localization_state(
        self,
        *,
        robot_id: str,
        map_name: Optional[str],
        state: str,
        valid: bool,
        map_revision_id: Optional[str] = None,
    ):
        backend = self._backend
        resolved_state = str(state or "not_localized").strip() or "not_localized"
        runtime_param = backend._runtime_context.runtime_param
        rospy.set_param(runtime_param("localization_state"), resolved_state)
        rospy.set_param(runtime_param("localization_valid"), bool(valid))
        rospy.set_param(runtime_param("localization_stamp"), float(rospy.Time.now().to_sec()))
        if map_name is not None:
            rospy.set_param("/map_name", str(map_name or "").strip())
        if map_revision_id is not None:
            rospy.set_param(runtime_param("current_map_revision_id"), str(map_revision_id or "").strip())
            try:
                rospy.set_param("/map_revision_id", str(map_revision_id or "").strip())
            except Exception:
                pass
        self.update_runtime_state(
            robot_id=robot_id,
            map_name=map_name,
            map_revision_id=map_revision_id,
            localization_state=resolved_state,
            localization_valid=bool(valid),
        )

    def sync_task_ready_state(
        self,
        *,
        robot_id: str,
        map_name: str,
        map_revision_id: str = "",
        localization_state: str = "localized",
        localization_valid: bool = True,
        current_mode: str = "localization",
    ):
        backend = self._backend
        runtime_param = backend._runtime_context.runtime_param
        current_pbstream_path = str(rospy.get_param(runtime_param("current_pbstream_path"), "") or "").strip()
        self.publish_runtime_snapshot(
            current_mode=str(current_mode or "localization").strip() or "localization",
            map_name=str(map_name or "").strip(),
            pbstream_path=current_pbstream_path,
            map_revision_id=str(map_revision_id or "").strip(),
        )
        self.set_localization_state(
            robot_id=robot_id,
            map_name=map_name,
            state=localization_state,
            valid=bool(localization_valid),
            map_revision_id=map_revision_id,
        )

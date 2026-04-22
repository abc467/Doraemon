# -*- coding: utf-8 -*-

"""Task runtime state wrapper on top of operations.db."""

import time
from dataclasses import dataclass
from typing import Optional

from coverage_planner.ops_store.store import OperationsStore, RobotRuntimeStateRecord


@dataclass
class TaskState:
    active_zone: str = ""
    active_run_id: str = ""
    active_run_loop_index: int = 0
    task_map_name: str = ""
    task_map_revision_id: str = ""
    plan_profile_name: str = ""
    sys_profile_name: str = ""
    clean_mode: str = ""
    return_to_dock_on_finish: bool = False
    repeat_after_full_charge: bool = False
    mission_state: str = "IDLE"
    phase: str = "IDLE"
    public_state: str = "IDLE"
    armed: bool = True
    active_job_id: str = ""
    active_schedule_id: str = ""
    job_loops_total: int = 0
    job_loops_done: int = 0
    dock_state: str = ""
    battery_soc: float = 0.0
    battery_valid: bool = False
    executor_state: str = ""
    last_error_code: str = ""
    last_error_msg: str = ""
    updated_ts: float = 0.0


class TaskStateStore:
    def __init__(self, db_path: str, robot_id: str = "local_robot"):
        self._ops = OperationsStore(str(db_path))
        self._robot_id = str(robot_id or "local_robot")

    def load(self, *, key: str = "active") -> Optional[TaskState]:
        del key
        rs = self._ops.get_robot_runtime_state(self._robot_id)
        if rs is None:
            return None

        run = self._ops.get_run(str(rs.active_run_id or "")) if rs.active_run_id else None
        return TaskState(
            active_zone=str(run.zone_id or "") if run is not None else "",
            active_run_id=str(rs.active_run_id or ""),
            active_run_loop_index=int(run.loop_index or 0) if run is not None else 0,
            task_map_name=str(rs.map_name or (run.map_name if run is not None else "")),
            task_map_revision_id=str(rs.map_revision_id or (run.map_revision_id if run is not None else "")),
            plan_profile_name=str(run.plan_profile_name or "") if run is not None else "",
            sys_profile_name=str(run.sys_profile_name or "") if run is not None else "",
            clean_mode=str(run.clean_mode or "") if run is not None else "",
            return_to_dock_on_finish=bool(rs.return_to_dock_on_finish),
            repeat_after_full_charge=bool(rs.repeat_after_full_charge),
            mission_state=str(rs.mission_state or "IDLE"),
            phase=str(rs.phase or "IDLE"),
            public_state=str(rs.public_state or "IDLE"),
            armed=bool(rs.armed),
            active_job_id=str(rs.active_job_id or (run.job_id if run is not None else "")),
            active_schedule_id=str(rs.active_schedule_id or ""),
            job_loops_total=int(run.loops_total or 0) if run is not None else 0,
            job_loops_done=max(0, int(run.loop_index or 1) - 1) if run is not None else 0,
            dock_state=str(rs.dock_state or ""),
            battery_soc=float(rs.battery_soc or 0.0),
            battery_valid=bool(rs.battery_valid),
            executor_state=str(rs.executor_state or ""),
            last_error_code=str(rs.last_error_code or ""),
            last_error_msg=str(rs.last_error_msg or ""),
            updated_ts=float(rs.updated_ts or 0.0),
        )

    def save(self, state: TaskState, *, key: str = "active"):
        del key
        ts = float(state.updated_ts) if state.updated_ts > 0 else time.time()
        self._ops.upsert_robot_runtime_state(
            RobotRuntimeStateRecord(
                robot_id=self._robot_id,
                active_run_id=str(state.active_run_id or ""),
                active_job_id=str(state.active_job_id or ""),
                active_schedule_id=str(state.active_schedule_id or ""),
                map_name=str(state.task_map_name or ""),
                map_revision_id=str(state.task_map_revision_id or ""),
                return_to_dock_on_finish=bool(state.return_to_dock_on_finish),
                repeat_after_full_charge=bool(state.repeat_after_full_charge),
                mission_state=str(state.mission_state or "IDLE"),
                phase=str(state.phase or "IDLE"),
                public_state=str(state.public_state or "IDLE"),
                armed=bool(state.armed),
                dock_state=str(state.dock_state or ""),
                battery_soc=float(state.battery_soc or 0.0),
                battery_valid=bool(state.battery_valid),
                executor_state=str(state.executor_state or ""),
                last_error_code=str(state.last_error_code or ""),
                last_error_msg=str(state.last_error_msg or ""),
                updated_ts=ts,
            )
        )

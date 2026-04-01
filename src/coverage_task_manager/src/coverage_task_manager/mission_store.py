# -*- coding: utf-8 -*-

"""Mission run history & robot events on top of operations.db."""

import json
from dataclasses import dataclass
from typing import List, Optional

from coverage_planner.ops_store.store import MissionRunRecord, OperationsStore


@dataclass
class MissionRun:
    run_id: str
    job_id: str
    map_name: str
    zone_id: str
    plan_profile_name: str
    constraint_version: str
    sys_profile_name: str
    clean_mode: str
    loop_index: int
    loops_total: int
    state: str
    reason: str
    start_ts: float
    end_ts: float
    updated_ts: float
    plan_id: str = ""
    zone_version: int = 0
    mbf_controller_name: str = ""
    actuator_profile_name: str = ""
    trigger_source: str = ""
    map_id: str = ""
    map_md5: str = ""
    created_ts: float = 0.0


def _convert_run(row: MissionRunRecord) -> MissionRun:
    return MissionRun(
        run_id=str(row.run_id or ""),
        job_id=str(row.job_id or ""),
        map_name=str(row.map_name or ""),
        zone_id=str(row.zone_id or ""),
        plan_profile_name=str(row.plan_profile_name or ""),
        constraint_version=str(row.constraint_version or ""),
        sys_profile_name=str(row.sys_profile_name or ""),
        clean_mode=str(row.clean_mode or ""),
        loop_index=int(row.loop_index or 1),
        loops_total=int(row.loops_total or 1),
        state=str(row.state or ""),
        reason=str(row.reason or ""),
        start_ts=float(row.start_ts or 0.0),
        end_ts=float(row.end_ts or 0.0),
        updated_ts=float(row.updated_ts or 0.0),
        plan_id=str(row.plan_id or ""),
        zone_version=int(row.zone_version or 0),
        mbf_controller_name=str(row.mbf_controller_name or ""),
        actuator_profile_name=str(row.actuator_profile_name or ""),
        trigger_source=str(row.trigger_source or ""),
        map_id=str(row.map_id or ""),
        map_md5=str(row.map_md5 or ""),
        created_ts=float(row.created_ts or 0.0),
    )


class MissionStore:
    def __init__(self, db_path: str):
        self._ops = OperationsStore(str(db_path))

    def close(self):
        return

    def create_run(
        self,
        *,
        run_id: str,
        job_id: str,
        zone_id: str,
        plan_profile_name: str,
        sys_profile_name: str,
        clean_mode: str,
        loop_index: int,
        loops_total: int,
        map_name: str = "",
        state: str = "RUNNING",
        reason: str = "",
        start_ts: Optional[float] = None,
        plan_id: str = "",
        zone_version: int = 0,
        constraint_version: str = "",
        mbf_controller_name: str = "",
        actuator_profile_name: str = "",
        trigger_source: str = "",
        map_id: str = "",
        map_md5: str = "",
    ):
        self._ops.create_run(
            run_id=str(run_id or ""),
            job_id=str(job_id or ""),
            map_name=str(map_name or ""),
            zone_id=str(zone_id or ""),
            plan_profile_name=str(plan_profile_name or ""),
            plan_id=str(plan_id or ""),
            zone_version=int(zone_version or 0),
            constraint_version=str(constraint_version or ""),
            sys_profile_name=str(sys_profile_name or ""),
            mbf_controller_name=str(mbf_controller_name or ""),
            actuator_profile_name=str(actuator_profile_name or ""),
            clean_mode=str(clean_mode or ""),
            loops_total=int(loops_total or 1),
            loop_index=int(loop_index or 1),
            trigger_source=str(trigger_source or ""),
            state=str(state or ""),
            reason=str(reason or ""),
            map_id=str(map_id or ""),
            map_md5=str(map_md5 or ""),
            start_ts=start_ts,
        )

    def update_state(self, run_id: str, state: str, *, reason: str = "", set_end: bool = False):
        self._ops.update_run_state(str(run_id or ""), str(state or ""), reason=str(reason or ""), set_end=bool(set_end))

    def finish(self, run_id: str, state: str, *, reason: str = ""):
        self._ops.finish_run(str(run_id or ""), str(state or ""), reason=str(reason or ""))

    def update_execution_context(self, run_id: str, **kwargs):
        self._ops.update_run_execution_context(str(run_id or ""), **kwargs)

    def get_run(self, run_id: str) -> Optional[MissionRun]:
        row = self._ops.get_run(str(run_id or ""))
        return _convert_run(row) if row is not None else None

    def find_latest_open_run(self, zone_id: str) -> Optional[str]:
        return self._ops.find_latest_open_run(str(zone_id or ""))

    def add_event(
        self,
        run_id: str,
        *,
        source: str,
        level: str,
        code: str,
        msg: str,
        data_json: str = "",
        ts: Optional[float] = None,
    ):
        scope = "RUN" if str(run_id or "").strip() else "SYSTEM"
        data = ""
        if data_json:
            try:
                data = json.loads(data_json)
            except Exception:
                data = str(data_json)
        self._ops.add_robot_event(
            scope=scope,
            component=str(source or ""),
            level=str(level or ""),
            code=str(code or ""),
            message=str(msg or ""),
            run_id=str(run_id or ""),
            data=data,
            ts=ts,
        )

    def list_recent_runs(self, *, limit: int = 20) -> List[MissionRun]:
        return [_convert_run(row) for row in self._ops.list_recent_runs(limit=max(1, int(limit)))]

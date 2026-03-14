# -*- coding: utf-8 -*-

"""Schedule trigger state on top of operations.db."""

from dataclasses import dataclass
from typing import Optional

from coverage_planner.ops_store.store import OperationsStore, ScheduleStateRecord


@dataclass
class ScheduleRun:
    schedule_id: str
    last_fire_ts: float = 0.0
    last_done_ts: float = 0.0
    last_status: str = ""
    updated_ts: float = 0.0


class ScheduleRunStore:
    def __init__(self, db_path: str):
        self._ops = OperationsStore(str(db_path))

    def get(self, schedule_id: str) -> Optional[ScheduleRun]:
        st: Optional[ScheduleStateRecord] = self._ops.get_schedule_state(str(schedule_id or ""))
        if st is None:
            return None
        return ScheduleRun(
            schedule_id=str(st.schedule_id or ""),
            last_fire_ts=float(st.last_fire_ts or 0.0),
            last_done_ts=float(st.last_done_ts or 0.0),
            last_status=str(st.last_status or ""),
            updated_ts=float(st.updated_ts or 0.0),
        )

    def was_fired(self, schedule_id: str, slot_ts: float) -> bool:
        return self._ops.was_schedule_fired(str(schedule_id or ""), float(slot_ts or 0.0))

    def mark_fired(self, schedule_id: str, slot_ts: Optional[float] = None, *, fire_ts: Optional[float] = None):
        self._ops.mark_schedule_fired(
            str(schedule_id or ""),
            float(slot_ts if slot_ts is not None else fire_ts if fire_ts is not None else 0.0),
            fire_ts=fire_ts,
        )

    def mark_done(self, schedule_id: str, *, status: str, done_ts: Optional[float] = None):
        self._ops.mark_schedule_done(str(schedule_id or ""), status=str(status or ""), done_ts=done_ts)

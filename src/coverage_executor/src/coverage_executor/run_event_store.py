# -*- coding: utf-8 -*-

"""Executor-side event sink on top of operations.db."""

from __future__ import annotations

from typing import Any, Dict, Optional

from coverage_planner.ops_store.store import OperationsStore


class RunEventStore:
    def __init__(self, db_path: str):
        self._ops = OperationsStore(str(db_path))

    def add_event(
        self,
        *,
        run_id: str,
        source: str,
        level: str,
        code: str,
        msg: str,
        data: Optional[Dict[str, Any]] = None,
    ):
        self._ops.add_robot_event(
            scope="RUN" if str(run_id or "").strip() else "SYSTEM",
            component=str(source or ""),
            level=str(level or ""),
            code=str(code or ""),
            message=str(msg or ""),
            run_id=str(run_id or ""),
            data=data,
        )

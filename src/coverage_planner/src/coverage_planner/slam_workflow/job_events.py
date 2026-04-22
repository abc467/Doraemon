# -*- coding: utf-8 -*-

"""Robot-event helpers for formal SLAM workflow jobs."""

from __future__ import annotations

from typing import Any, Dict

from coverage_planner.slam_workflow.api import operation_name


class CartographerSlamJobEventLogger:
    def __init__(self, backend: Any):
        self._backend = backend

    def job_started(self, snapshot: Dict[str, object]):
        operation = int(snapshot.get("operation") or 0)
        op_name = str(snapshot.get("operation_name") or operation_name(operation))
        self._backend._ops.add_robot_event(
            scope="slam",
            component="slam_runtime_manager",
            level="INFO",
            code="slam_job_started",
            message="%s started" % op_name,
            job_id=str(snapshot.get("job_id") or ""),
            data={
                "operation": operation,
                "operation_name": op_name,
                "map_name": str(snapshot.get("requested_map_name") or ""),
                "map_revision_id": str(snapshot.get("requested_map_revision_id") or ""),
            },
        )

    def job_finished(self, snapshot: Dict[str, object]):
        operation = int(snapshot.get("operation") or 0)
        op_name = str(snapshot.get("operation_name") or operation_name(operation))
        success = bool(snapshot.get("success", False))
        self._backend._ops.add_robot_event(
            scope="slam",
            component="slam_runtime_manager",
            level="INFO" if success else "WARN",
            code="slam_job_succeeded" if success else "slam_job_failed",
            message=str(snapshot.get("message") or op_name),
            job_id=str(snapshot.get("job_id") or ""),
            data={
                "operation": operation,
                "operation_name": op_name,
                "requested_map_name": str(snapshot.get("requested_map_name") or ""),
                "requested_map_revision_id": str(snapshot.get("requested_map_revision_id") or ""),
                "resolved_map_name": str(snapshot.get("resolved_map_name") or ""),
                "resolved_map_revision_id": str(snapshot.get("resolved_map_revision_id") or ""),
                "error_code": str(snapshot.get("error_code") or ""),
            },
        )

    def job_interrupted_on_restore(self, snapshot: Dict[str, object]):
        self._backend._ops.add_robot_event(
            scope="slam",
            component="slam_runtime_manager",
            level="WARN",
            code="slam_job_interrupted_on_restore",
            message=str(snapshot.get("message") or "slam runtime manager restarted before job completed"),
            job_id=str(snapshot.get("job_id") or ""),
            data={
                "operation": int(snapshot.get("operation") or 0),
                "operation_name": str(snapshot.get("operation_name") or ""),
                "requested_map_name": str(snapshot.get("requested_map_name") or ""),
                "requested_map_revision_id": str(snapshot.get("requested_map_revision_id") or ""),
            },
        )

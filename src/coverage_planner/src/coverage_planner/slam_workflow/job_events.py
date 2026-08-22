# -*- coding: utf-8 -*-

"""Robot-event helpers for formal SLAM workflow jobs."""

from __future__ import annotations

import json
import re
import time
from typing import Any, Dict

from coverage_planner.slam_workflow.api import operation_name


_AUDIT_SOURCE_PREFIX = "__doraemon_submit_source__="
_AUDIT_SOURCE_RE = re.compile(r"[^a-zA-Z0-9_.:-]+")


def _normalized_source(source: str, *, default: str = "unknown") -> str:
    value = _AUDIT_SOURCE_RE.sub("_", str(source or "").strip()).strip("_")
    return (value or str(default or "unknown"))[:96]


def encode_submit_audit_description(source: str, description: str) -> str:
    """Carry submit provenance across the unchanged ROS service contract."""

    return "%s%s\n%s" % (
        _AUDIT_SOURCE_PREFIX,
        _normalized_source(source),
        str(description or ""),
    )


def decode_submit_audit_description(description: str):
    """Return (source, user_description), stripping the private audit marker."""

    value = str(description or "")
    first_line, separator, remainder = value.partition("\n")
    if first_line.startswith(_AUDIT_SOURCE_PREFIX):
        source = _normalized_source(first_line[len(_AUDIT_SOURCE_PREFIX) :])
        return source, remainder if separator else ""
    return "", value


def infer_submit_source(description: str) -> str:
    text = str(description or "").strip().lower()
    if text.startswith("localization lifecycle"):
        return "localization_lifecycle_manager"
    if text.startswith("task manager"):
        return "coverage_task_manager"
    if text.startswith("startup"):
        return "startup_runtime"
    return "direct_runtime_client"


class CartographerSlamJobEventLogger:
    def __init__(self, backend: Any):
        self._backend = backend

    def _publish_audit(self, event: str, snapshot: Dict[str, object]):
        publisher = getattr(self._backend, "_audit_event_pub", None)
        if publisher is None:
            return
        data = dict(snapshot or {})
        payload = {
            "schema_version": 1,
            "event": str(event or "slam_job_event"),
            "wall_time_s": time.time(),
            "job_id": str(data.get("job_id") or ""),
            "robot_id": str(data.get("robot_id") or getattr(self._backend, "robot_id", "")),
            "operation": int(data.get("operation") or 0),
            "operation_name": str(
                data.get("operation_name")
                or operation_name(int(data.get("operation") or 0))
            ),
            "submit_source": str(data.get("submit_source") or "unknown"),
            "description": str(data.get("description") or ""),
            "status": str(data.get("status") or ""),
            "phase": str(data.get("phase") or ""),
            "success": bool(data.get("success", False)),
            "error_code": str(data.get("error_code") or ""),
            "message": str(data.get("message") or ""),
            "requested_map_name": str(data.get("requested_map_name") or ""),
            "requested_map_revision_id": str(data.get("requested_map_revision_id") or ""),
            "created_ts": float(data.get("created_ts") or 0.0),
            "started_ts": float(data.get("started_ts") or 0.0),
            "finished_ts": float(data.get("finished_ts") or 0.0),
        }
        try:
            publisher.publish(
                json.dumps(
                    payload,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
            )
        except Exception:
            # Observability must never change a SLAM job's outcome.
            pass

    def job_submitted(self, snapshot: Dict[str, object]):
        operation = int(snapshot.get("operation") or 0)
        op_name = str(snapshot.get("operation_name") or operation_name(operation))
        submit_source = str(snapshot.get("submit_source") or "unknown")
        try:
            self._backend._ops.add_robot_event(
                scope="slam",
                component="slam_runtime_manager",
                level="INFO",
                code="slam_job_submitted",
                message="%s submitted by %s" % (op_name, submit_source),
                job_id=str(snapshot.get("job_id") or ""),
                data={
                    "operation": operation,
                    "operation_name": op_name,
                    "submit_source": submit_source,
                    "description": str(snapshot.get("description") or ""),
                    "map_name": str(snapshot.get("requested_map_name") or ""),
                    "map_revision_id": str(snapshot.get("requested_map_revision_id") or ""),
                    "created_ts": float(snapshot.get("created_ts") or 0.0),
                },
            )
        except Exception:
            # A diagnostics database failure must not strand an accepted job
            # before its worker thread starts.
            pass
        self._publish_audit("slam_job_submitted", snapshot)

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
                "submit_source": str(snapshot.get("submit_source") or "unknown"),
                "map_name": str(snapshot.get("requested_map_name") or ""),
                "map_revision_id": str(snapshot.get("requested_map_revision_id") or ""),
            },
        )
        self._publish_audit("slam_job_started", snapshot)

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
                "submit_source": str(snapshot.get("submit_source") or "unknown"),
                "error_code": str(snapshot.get("error_code") or ""),
            },
        )
        self._publish_audit(
            "slam_job_succeeded" if success else "slam_job_failed",
            snapshot,
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
                "submit_source": str(snapshot.get("submit_source") or "unknown"),
                "requested_map_name": str(snapshot.get("requested_map_name") or ""),
                "requested_map_revision_id": str(snapshot.get("requested_map_revision_id") or ""),
            },
        )
        self._publish_audit("slam_job_interrupted_on_restore", snapshot)

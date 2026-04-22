#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import os
import sqlite3
import sys
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


TERMINAL_RUN_STATES = {
    "SUCCEEDED",
    "COMPLETED",
    "FAILED",
    "CANCELED",
    "CANCELLED",
    "ABORTED",
    "STOPPED",
    "FINISHED",
    "DONE",
}


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path or ""))
    conn.row_factory = sqlite3.Row
    return conn


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1;",
        (str(table_name or "").strip(),),
    ).fetchone()
    return bool(row)


def _fetch_rows(conn: sqlite3.Connection, query: str, args: Sequence[object] = ()) -> List[sqlite3.Row]:
    try:
        return list(conn.execute(query, tuple(args)).fetchall() or [])
    except Exception:
        return []


def _row_dicts(rows: Iterable[sqlite3.Row]) -> List[Dict[str, object]]:
    return [dict(row) for row in list(rows or [])]


def _nonempty(value: object) -> str:
    return str(value or "").strip()


def _bool_int(value: object) -> bool:
    try:
        return bool(int(value or 0))
    except Exception:
        return bool(value)


def _revision_is_verified(revision: Dict[str, object]) -> bool:
    if not revision:
        return False
    return _nonempty(revision.get("verification_status")).lower() == "verified"


def _revision_is_enabled(revision: Dict[str, object]) -> bool:
    return bool(revision) and _bool_int(revision.get("enabled", 0))


def _run_is_active(run_row: Dict[str, object]) -> bool:
    end_ts = float(run_row.get("end_ts") or 0.0)
    if end_ts > 0.0:
        return False
    return _nonempty(run_row.get("state")).upper() not in TERMINAL_RUN_STATES


def _finding(severity: str, code: str, scope: str, message: str, **details) -> Dict[str, object]:
    return {
        "severity": str(severity or "error"),
        "code": str(code or ""),
        "scope": str(scope or ""),
        "message": str(message or ""),
        "details": dict(details or {}),
    }


def _append(findings: List[Dict[str, object]], severity: str, code: str, scope: str, message: str, **details) -> None:
    findings.append(_finding(severity, code, scope, message, **details))


def _summary(findings: Sequence[Dict[str, object]], *, strict: bool = False) -> Dict[str, object]:
    errors = [item for item in list(findings or []) if str(item.get("severity") or "") == "error"]
    warnings = [item for item in list(findings or []) if str(item.get("severity") or "") == "warning"]
    return {
        "ok": not errors and (not strict or not warnings),
        "error_count": len(errors),
        "warning_count": len(warnings),
        "strict": bool(strict),
    }


def _revision_scope_slot(
    *,
    map_name: str = "",
    revision_id: str = "",
    status: str = "",
    lifecycle_status: str = "",
    verification_status: str = "",
    source: str = "",
) -> Dict[str, object]:
    map_name = _nonempty(map_name)
    revision_id = _nonempty(revision_id)
    status = _nonempty(status)
    lifecycle_status = _nonempty(lifecycle_status)
    verification_status = _nonempty(verification_status)
    source = _nonempty(source)
    return {
        "map_name": map_name,
        "revision_id": revision_id,
        "status": status,
        "lifecycle_status": lifecycle_status,
        "verification_status": verification_status,
        "source": source,
        "present": bool(map_name or revision_id or status or lifecycle_status or verification_status),
    }


def _head_scope_for_map(
    assets: Dict[str, Dict[str, object]],
    revisions: Dict[str, Dict[str, object]],
    map_name: str,
) -> Dict[str, object]:
    map_name = _nonempty(map_name)
    asset = dict(assets.get(map_name) or {})
    revision_id = _nonempty(asset.get("current_revision_id"))
    revision = dict(revisions.get(revision_id) or {})
    return _revision_scope_slot(
        map_name=map_name or _nonempty(revision.get("map_name")),
        revision_id=revision_id,
        lifecycle_status=_nonempty(revision.get("lifecycle_status")),
        verification_status=_nonempty(revision.get("verification_status")),
        source="map_assets.current_revision_id",
    )


def _build_revision_scope(plan_ctx: Dict[str, object], *, robot_id: str = "") -> Dict[str, object]:
    revisions = dict(plan_ctx.get("revisions") or {})
    assets = dict(plan_ctx.get("assets") or {})
    active_rows = list(plan_ctx.get("active_rows") or [])
    pending_revision_rows = list(plan_ctx.get("pending_revision_rows") or [])
    pending_switch_rows = list(plan_ctx.get("pending_switch_rows") or [])

    active_by_robot = {
        _nonempty(row.get("robot_id")): row
        for row in active_rows
        if _nonempty(row.get("robot_id"))
    }
    pending_by_robot = {
        _nonempty(row.get("robot_id")): row
        for row in pending_revision_rows
        if _nonempty(row.get("robot_id"))
    }
    name_only_pending_by_robot = {
        _nonempty(row.get("robot_id")): row
        for row in pending_switch_rows
        if _nonempty(row.get("robot_id"))
    }

    robot_ids = set(active_by_robot.keys()) | set(pending_by_robot.keys()) | set(name_only_pending_by_robot.keys())
    if robot_id:
        robot_ids.add(_nonempty(robot_id))

    robot_scopes: List[Dict[str, object]] = []
    for current_robot_id in sorted(robot_ids):
        active_row = dict(active_by_robot.get(current_robot_id) or {})
        active_revision_id = _nonempty(active_row.get("active_revision_id"))
        active_revision = dict(revisions.get(active_revision_id) or {})
        active_map_name = _nonempty(active_revision.get("map_name"))

        pending_row = dict(pending_by_robot.get(current_robot_id) or {})
        pending_target_revision_id = _nonempty(pending_row.get("target_revision_id"))
        pending_target_revision = dict(revisions.get(pending_target_revision_id) or {})
        name_only_pending_row = dict(name_only_pending_by_robot.get(current_robot_id) or {})
        pending_map_name = _nonempty(pending_target_revision.get("map_name")) or _nonempty(
            name_only_pending_row.get("target_map_name")
        )

        latest_head_map_name = active_map_name or pending_map_name
        latest_head = _head_scope_for_map(assets, revisions, latest_head_map_name)

        robot_scopes.append(
            {
                "robot_id": current_robot_id,
                "active": _revision_scope_slot(
                    map_name=active_map_name,
                    revision_id=active_revision_id,
                    lifecycle_status=_nonempty(active_revision.get("lifecycle_status")),
                    verification_status=_nonempty(active_revision.get("verification_status")),
                    source="robot_active_map_revision.active_revision_id",
                ),
                "latest_head": latest_head,
                "pending_target": _revision_scope_slot(
                    map_name=pending_map_name,
                    revision_id=pending_target_revision_id,
                    status=_nonempty(pending_row.get("status")) or _nonempty(name_only_pending_row.get("status")),
                    lifecycle_status=_nonempty(pending_target_revision.get("lifecycle_status")),
                    verification_status=_nonempty(pending_target_revision.get("verification_status")),
                    source=(
                        "robot_pending_map_revision.target_revision_id"
                        if pending_target_revision_id
                        else "robot_pending_map_switch.target_map_name_only_row"
                    ),
                ),
            }
        )

    selected_robot_id = _nonempty(robot_id)
    if not selected_robot_id and robot_scopes:
        selected_robot_id = _nonempty(robot_scopes[0].get("robot_id"))
    selected_scope = next(
        (item for item in robot_scopes if _nonempty(item.get("robot_id")) == selected_robot_id),
        {},
    )
    return {
        "selected_robot_id": selected_robot_id,
        "active": dict(selected_scope.get("active") or {}),
        "latest_head": dict(selected_scope.get("latest_head") or {}),
        "pending_target": dict(selected_scope.get("pending_target") or {}),
        "robots": robot_scopes,
    }


def _plan_context(plan_db_path: str) -> Dict[str, object]:
    conn = _connect(plan_db_path)
    try:
        revisions = {
            _nonempty(row["revision_id"]): dict(row)
            for row in _fetch_rows(conn, "SELECT * FROM map_revisions ORDER BY map_name ASC, created_ts ASC;")
            if _nonempty(row["revision_id"])
        }
        assets = {
            _nonempty(row["map_name"]): dict(row)
            for row in _fetch_rows(conn, "SELECT * FROM map_assets ORDER BY map_name ASC;")
            if _nonempty(row["map_name"])
        }
        zones = _row_dicts(_fetch_rows(conn, "SELECT * FROM zones ORDER BY map_revision_id ASC, zone_id ASC;"))
        zone_versions = _row_dicts(
            _fetch_rows(conn, "SELECT * FROM zone_versions ORDER BY map_revision_id ASC, zone_id ASC, zone_version ASC;")
        )
        plans = _row_dicts(_fetch_rows(conn, "SELECT * FROM plans ORDER BY map_revision_id ASC, zone_id ASC, plan_id ASC;"))
        zone_active_plans = _row_dicts(
            _fetch_rows(conn, "SELECT * FROM zone_active_plans ORDER BY map_revision_id ASC, zone_id ASC, plan_profile_name ASC;")
        )
        active_rows = _row_dicts(_fetch_rows(conn, "SELECT * FROM robot_active_map_revision ORDER BY robot_id ASC;"))
        pending_revision_rows = _row_dicts(
            _fetch_rows(conn, "SELECT * FROM robot_pending_map_revision ORDER BY robot_id ASC;")
        )
        pending_switch_rows = _row_dicts(
            _fetch_rows(conn, "SELECT * FROM robot_pending_map_switch ORDER BY robot_id ASC;")
        )
    finally:
        conn.close()

    zone_keys = {
        (
            _nonempty(row.get("map_revision_id")),
            _nonempty(row.get("zone_id")),
        ): row
        for row in zones
    }
    zone_version_keys = {
        (
            _nonempty(row.get("map_revision_id")),
            _nonempty(row.get("zone_id")),
            int(row.get("zone_version") or 0),
        ): row
        for row in zone_versions
    }
    plan_keys = {
        _nonempty(row.get("plan_id")): row
        for row in plans
        if _nonempty(row.get("plan_id"))
    }
    active_plan_keys = {
        (
            _nonempty(row.get("map_revision_id")),
            _nonempty(row.get("zone_id")),
            _nonempty(row.get("plan_profile_name")),
        ): row
        for row in zone_active_plans
    }
    return {
        "revisions": revisions,
        "assets": assets,
        "zones": zones,
        "zone_keys": zone_keys,
        "zone_versions": zone_versions,
        "zone_version_keys": zone_version_keys,
        "plans": plans,
        "plan_keys": plan_keys,
        "zone_active_plans": zone_active_plans,
        "active_plan_keys": active_plan_keys,
        "active_rows": active_rows,
        "pending_revision_rows": pending_revision_rows,
        "pending_switch_rows": pending_switch_rows,
    }


def _ops_context(ops_db_path: str) -> Dict[str, object]:
    if not ops_db_path or not os.path.exists(ops_db_path):
        return {
            "available": False,
            "jobs": [],
            "job_keys": {},
            "job_schedules": [],
            "schedule_keys": {},
            "mission_runs": [],
            "run_keys": {},
            "mission_checkpoints": [],
            "runtime_states": [],
            "slam_jobs": [],
        }
    conn = _connect(ops_db_path)
    try:
        jobs = _row_dicts(_fetch_rows(conn, "SELECT * FROM jobs ORDER BY job_id ASC;")) if _table_exists(conn, "jobs") else []
        job_schedules = (
            _row_dicts(_fetch_rows(conn, "SELECT * FROM job_schedules ORDER BY schedule_id ASC;"))
            if _table_exists(conn, "job_schedules")
            else []
        )
        mission_runs = (
            _row_dicts(_fetch_rows(conn, "SELECT * FROM mission_runs ORDER BY updated_ts DESC, run_id ASC;"))
            if _table_exists(conn, "mission_runs")
            else []
        )
        mission_checkpoints = (
            _row_dicts(_fetch_rows(conn, "SELECT * FROM mission_checkpoints ORDER BY run_id ASC;"))
            if _table_exists(conn, "mission_checkpoints")
            else []
        )
        runtime_states = (
            _row_dicts(_fetch_rows(conn, "SELECT * FROM robot_runtime_state ORDER BY robot_id ASC;"))
            if _table_exists(conn, "robot_runtime_state")
            else []
        )
        slam_jobs = (
            _row_dicts(_fetch_rows(conn, "SELECT * FROM slam_jobs ORDER BY updated_ts DESC, job_id ASC;"))
            if _table_exists(conn, "slam_jobs")
            else []
        )
    finally:
        conn.close()
    return {
        "available": True,
        "jobs": jobs,
        "job_keys": {_nonempty(row.get("job_id")): row for row in jobs if _nonempty(row.get("job_id"))},
        "job_schedules": job_schedules,
        "schedule_keys": {
            _nonempty(row.get("schedule_id")): row for row in job_schedules if _nonempty(row.get("schedule_id"))
        },
        "mission_runs": mission_runs,
        "run_keys": {_nonempty(row.get("run_id")): row for row in mission_runs if _nonempty(row.get("run_id"))},
        "mission_checkpoints": mission_checkpoints,
        "runtime_states": runtime_states,
        "slam_jobs": slam_jobs,
    }


def _check_plan_revision_pointers(findings: List[Dict[str, object]], plan_ctx: Dict[str, object], *, robot_id: str = "") -> None:
    revisions = dict(plan_ctx.get("revisions") or {})
    assets = dict(plan_ctx.get("assets") or {})
    active_rows = list(plan_ctx.get("active_rows") or [])
    pending_revision_rows = list(plan_ctx.get("pending_revision_rows") or [])
    pending_switch_rows = list(plan_ctx.get("pending_switch_rows") or [])

    for map_name, asset in assets.items():
        revision_id = _nonempty(asset.get("current_revision_id"))
        if not revision_id:
            _append(
                findings,
                "error",
                "asset_current_revision_missing",
                "map_asset:%s" % map_name,
                "map_assets.current_revision_id is empty",
                map_name=map_name,
            )
            continue
        revision = dict(revisions.get(revision_id) or {})
        if not revision:
            _append(
                findings,
                "error",
                "asset_current_revision_unknown",
                "map_asset:%s" % map_name,
                "map_assets.current_revision_id points to missing map_revisions row",
                map_name=map_name,
                revision_id=revision_id,
            )
            continue
        if _nonempty(revision.get("map_name")) != map_name:
            _append(
                findings,
                "error",
                "asset_current_revision_map_mismatch",
                "map_asset:%s" % map_name,
                "map_assets.current_revision_id map_name mismatches map_assets.map_name",
                map_name=map_name,
                revision_id=revision_id,
                revision_map_name=_nonempty(revision.get("map_name")),
            )

    for row in active_rows:
        if robot_id and _nonempty(row.get("robot_id")) != robot_id:
            continue
        revision_id = _nonempty(row.get("active_revision_id"))
        scope = "robot_active_map_revision:%s" % (_nonempty(row.get("robot_id")) or "-")
        if not revision_id:
            _append(findings, "error", "active_revision_missing", scope, "active revision pointer is empty")
            continue
        revision = dict(revisions.get(revision_id) or {})
        if not revision:
            _append(
                findings,
                "error",
                "active_revision_unknown",
                scope,
                "robot_active_map_revision points to missing map_revisions row",
                revision_id=revision_id,
            )
            continue
        if not _revision_is_verified(revision):
            _append(
                findings,
                "error",
                "active_revision_not_verified",
                scope,
                "active revision is not verified",
                revision_id=revision_id,
                verification_status=_nonempty(revision.get("verification_status")),
            )
        if not _revision_is_enabled(revision):
            _append(
                findings,
                "error",
                "active_revision_disabled",
                scope,
                "active revision is disabled",
                revision_id=revision_id,
            )

    for row in pending_revision_rows:
        if robot_id and _nonempty(row.get("robot_id")) != robot_id:
            continue
        target_revision_id = _nonempty(row.get("target_revision_id"))
        from_revision_id = _nonempty(row.get("from_revision_id"))
        scope = "robot_pending_map_revision:%s" % (_nonempty(row.get("robot_id")) or "-")
        if not target_revision_id:
            _append(findings, "error", "pending_target_revision_missing", scope, "pending target revision is empty")
        elif target_revision_id not in revisions:
            _append(
                findings,
                "error",
                "pending_target_revision_unknown",
                scope,
                "pending target revision points to missing map_revisions row",
                revision_id=target_revision_id,
            )
        if from_revision_id and from_revision_id not in revisions:
            _append(
                findings,
                "error",
                "pending_from_revision_unknown",
                scope,
                "pending from revision points to missing map_revisions row",
                revision_id=from_revision_id,
            )

    pending_by_robot = {
        _nonempty(row.get("robot_id")): row
        for row in pending_revision_rows
        if _nonempty(row.get("robot_id"))
    }
    for row in pending_switch_rows:
        robot = _nonempty(row.get("robot_id"))
        if robot_id and robot != robot_id:
            continue
        scope = "robot_pending_map_switch:%s" % (robot or "-")
        target_map_name = _nonempty(row.get("target_map_name"))
        if robot not in pending_by_robot:
            _append(
                findings,
                "warning",
                "name_only_pending_switch_without_revision_row",
                scope,
                "name-only pending switch row exists without robot_pending_map_revision companion row",
                target_map_name=target_map_name,
            )
        if target_map_name and target_map_name not in assets:
            _append(
                findings,
                "warning",
                "name_only_pending_switch_unknown_target_map",
                scope,
                "name-only pending switch target_map_name does not exist in map_assets",
                target_map_name=target_map_name,
            )


def _check_zone_plan_scope(findings: List[Dict[str, object]], plan_ctx: Dict[str, object]) -> None:
    revisions = dict(plan_ctx.get("revisions") or {})
    zone_version_keys = dict(plan_ctx.get("zone_version_keys") or {})
    zone_keys = dict(plan_ctx.get("zone_keys") or {})
    plan_keys = dict(plan_ctx.get("plan_keys") or {})
    active_plan_keys = dict(plan_ctx.get("active_plan_keys") or {})

    for row in list(plan_ctx.get("zones") or []):
        map_revision_id = _nonempty(row.get("map_revision_id"))
        zone_id = _nonempty(row.get("zone_id"))
        scope = "zone:%s" % zone_id
        if not map_revision_id:
            _append(findings, "error", "zone_missing_revision", scope, "zone is missing map_revision_id")
            continue
        revision = dict(revisions.get(map_revision_id) or {})
        if not revision:
            _append(
                findings,
                "error",
                "zone_unknown_revision",
                scope,
                "zone points to missing map_revisions row",
                zone_id=zone_id,
                revision_id=map_revision_id,
            )
            continue
        if _nonempty(row.get("map_name")) and _nonempty(row.get("map_name")) != _nonempty(revision.get("map_name")):
            _append(
                findings,
                "error",
                "zone_map_name_mismatch",
                scope,
                "zone.map_name mismatches map_revisions.map_name",
                zone_id=zone_id,
                revision_id=map_revision_id,
            )
        current_zone_version = int(row.get("current_zone_version") or 0)
        if current_zone_version <= 0 or (map_revision_id, zone_id, current_zone_version) not in zone_version_keys:
            _append(
                findings,
                "error",
                "zone_current_version_missing",
                scope,
                "zone current_zone_version does not resolve to a zone_versions row in the same revision",
                zone_id=zone_id,
                revision_id=map_revision_id,
                zone_version=current_zone_version,
            )
        if not _revision_is_verified(revision):
            _append(
                findings,
                "error",
                "zone_unverified_revision",
                scope,
                "zone is bound to a revision that is not verified",
                zone_id=zone_id,
                revision_id=map_revision_id,
                verification_status=_nonempty(revision.get("verification_status")),
            )

    for row in list(plan_ctx.get("zone_versions") or []):
        map_revision_id = _nonempty(row.get("map_revision_id"))
        zone_id = _nonempty(row.get("zone_id"))
        zone_version = int(row.get("zone_version") or 0)
        scope = "zone_version:%s:%s" % (zone_id, zone_version)
        if not map_revision_id:
            _append(findings, "error", "zone_version_missing_revision", scope, "zone_version is missing map_revision_id")
            continue
        revision = dict(revisions.get(map_revision_id) or {})
        if not revision:
            _append(
                findings,
                "error",
                "zone_version_unknown_revision",
                scope,
                "zone_version points to missing map_revisions row",
                zone_id=zone_id,
                zone_version=zone_version,
                revision_id=map_revision_id,
            )
            continue
        if _nonempty(row.get("map_name")) and _nonempty(row.get("map_name")) != _nonempty(revision.get("map_name")):
            _append(
                findings,
                "error",
                "zone_version_map_name_mismatch",
                scope,
                "zone_version.map_name mismatches map_revisions.map_name",
                zone_id=zone_id,
                zone_version=zone_version,
                revision_id=map_revision_id,
            )

    for row in list(plan_ctx.get("plans") or []):
        plan_id = _nonempty(row.get("plan_id"))
        zone_id = _nonempty(row.get("zone_id"))
        scope = "plan:%s" % (plan_id or "-")
        map_revision_id = _nonempty(row.get("map_revision_id"))
        if not map_revision_id:
            _append(findings, "error", "plan_missing_revision", scope, "plan is missing map_revision_id", plan_id=plan_id)
            continue
        revision = dict(revisions.get(map_revision_id) or {})
        if not revision:
            _append(
                findings,
                "error",
                "plan_unknown_revision",
                scope,
                "plan points to missing map_revisions row",
                plan_id=plan_id,
                revision_id=map_revision_id,
            )
            continue
        if not _revision_is_verified(revision):
            _append(
                findings,
                "error",
                "plan_unverified_revision",
                scope,
                "plan is bound to a revision that is not verified",
                plan_id=plan_id,
                revision_id=map_revision_id,
                verification_status=_nonempty(revision.get("verification_status")),
            )
        if _nonempty(row.get("map_name")) and _nonempty(row.get("map_name")) != _nonempty(revision.get("map_name")):
            _append(
                findings,
                "error",
                "plan_map_name_mismatch",
                scope,
                "plan.map_name mismatches map_revisions.map_name",
                plan_id=plan_id,
                revision_id=map_revision_id,
            )
        zone_key = (map_revision_id, zone_id, int(row.get("zone_version") or 0))
        if zone_key not in zone_version_keys:
            _append(
                findings,
                "error",
                "plan_zone_version_missing",
                scope,
                "plan zone_id/zone_version does not resolve inside the same map_revision_id",
                plan_id=plan_id,
                zone_id=zone_id,
                zone_version=int(row.get("zone_version") or 0),
                revision_id=map_revision_id,
            )

    for row in list(plan_ctx.get("zone_active_plans") or []):
        map_revision_id = _nonempty(row.get("map_revision_id"))
        zone_id = _nonempty(row.get("zone_id"))
        profile = _nonempty(row.get("plan_profile_name"))
        active_plan_id = _nonempty(row.get("active_plan_id"))
        scope = "zone_active_plan:%s:%s" % (zone_id or "-", profile or "-")
        if not map_revision_id:
            _append(findings, "error", "active_plan_missing_revision", scope, "zone_active_plans is missing map_revision_id")
            continue
        if (map_revision_id, zone_id) not in zone_keys:
            _append(
                findings,
                "error",
                "active_plan_missing_zone_scope",
                scope,
                "zone_active_plans points to a zone that does not exist in the same revision",
                zone_id=zone_id,
                revision_id=map_revision_id,
            )
        if not active_plan_id:
            _append(findings, "error", "active_plan_missing_plan_id", scope, "zone_active_plans.active_plan_id is empty")
            continue
        plan_row = dict(plan_keys.get(active_plan_id) or {})
        if not plan_row:
            _append(
                findings,
                "error",
                "active_plan_unknown_plan",
                scope,
                "zone_active_plans.active_plan_id points to missing plans row",
                active_plan_id=active_plan_id,
            )
            continue
        if _nonempty(plan_row.get("map_revision_id")) != map_revision_id or _nonempty(plan_row.get("zone_id")) != zone_id:
            _append(
                findings,
                "error",
                "active_plan_scope_mismatch",
                scope,
                "zone_active_plans.active_plan_id is not scoped to the same zone/revision",
                active_plan_id=active_plan_id,
                zone_id=zone_id,
                revision_id=map_revision_id,
            )
        if _nonempty(plan_row.get("plan_profile_name")) != profile:
            _append(
                findings,
                "error",
                "active_plan_profile_mismatch",
                scope,
                "zone_active_plans.plan_profile_name mismatches plans.plan_profile_name",
                active_plan_id=active_plan_id,
                zone_id=zone_id,
                revision_id=map_revision_id,
            )
        expected_row = dict(active_plan_keys.get((map_revision_id, zone_id, profile)) or {})
        if expected_row and _nonempty(expected_row.get("active_plan_id")) != active_plan_id:
            _append(
                findings,
                "error",
                "active_plan_lookup_mismatch",
                scope,
                "active plan lookup returned a different plan_id than zone_active_plans.active_plan_id",
                active_plan_id=active_plan_id,
                expected_plan_id=_nonempty(expected_row.get("active_plan_id")),
            )


def _check_ops_scope(
    findings: List[Dict[str, object]],
    plan_ctx: Dict[str, object],
    ops_ctx: Dict[str, object],
    *,
    robot_id: str = "",
) -> None:
    revisions = dict(plan_ctx.get("revisions") or {})
    zone_keys = dict(plan_ctx.get("zone_keys") or {})
    plan_keys = dict(plan_ctx.get("plan_keys") or {})
    job_keys = dict(ops_ctx.get("job_keys") or {})
    run_keys = dict(ops_ctx.get("run_keys") or {})
    schedule_keys = dict(ops_ctx.get("schedule_keys") or {})

    for row in list(ops_ctx.get("jobs") or []):
        job_id = _nonempty(row.get("job_id"))
        scope = "job:%s" % (job_id or "-")
        map_name = _nonempty(row.get("map_name"))
        map_revision_id = _nonempty(row.get("map_revision_id"))
        zone_id = _nonempty(row.get("zone_id"))
        if (map_name or zone_id) and not map_revision_id:
            _append(findings, "error", "job_missing_revision", scope, "job is missing map_revision_id", job_id=job_id)
            continue
        if not map_revision_id:
            continue
        revision = dict(revisions.get(map_revision_id) or {})
        if not revision:
            _append(
                findings,
                "error",
                "job_unknown_revision",
                scope,
                "job points to missing map_revisions row",
                job_id=job_id,
                revision_id=map_revision_id,
            )
            continue
        if not _revision_is_verified(revision):
            _append(
                findings,
                "error",
                "job_unverified_revision",
                scope,
                "job is bound to a revision that is not verified",
                job_id=job_id,
                revision_id=map_revision_id,
                verification_status=_nonempty(revision.get("verification_status")),
            )
        if map_name and map_name != _nonempty(revision.get("map_name")):
            _append(
                findings,
                "error",
                "job_map_name_mismatch",
                scope,
                "job.map_name mismatches map_revisions.map_name",
                job_id=job_id,
                revision_id=map_revision_id,
            )
        if zone_id and (map_revision_id, zone_id) not in zone_keys:
            _append(
                findings,
                "error",
                "job_zone_scope_missing",
                scope,
                "job zone_id does not exist in the same map_revision_id",
                job_id=job_id,
                revision_id=map_revision_id,
                zone_id=zone_id,
            )

    for row in list(ops_ctx.get("job_schedules") or []):
        schedule_id = _nonempty(row.get("schedule_id"))
        job_id = _nonempty(row.get("job_id"))
        scope = "schedule:%s" % (schedule_id or "-")
        if not job_id:
            _append(findings, "error", "schedule_missing_job_id", scope, "job_schedule.job_id is empty")
            continue
        if job_id not in job_keys:
            _append(
                findings,
                "error",
                "schedule_unknown_job",
                scope,
                "job_schedule points to missing jobs row",
                schedule_id=schedule_id,
                job_id=job_id,
            )

    for row in list(ops_ctx.get("mission_runs") or []):
        run_id = _nonempty(row.get("run_id"))
        scope = "mission_run:%s" % (run_id or "-")
        map_revision_id = _nonempty(row.get("map_revision_id"))
        zone_id = _nonempty(row.get("zone_id"))
        plan_id = _nonempty(row.get("plan_id"))
        severity = "error" if _run_is_active(row) else "warning"
        if (zone_id or plan_id or _nonempty(row.get("map_name"))) and not map_revision_id:
            _append(findings, severity, "run_missing_revision", scope, "mission_run is missing map_revision_id", run_id=run_id)
            continue
        if not map_revision_id:
            continue
        revision = dict(revisions.get(map_revision_id) or {})
        if not revision:
            _append(
                findings,
                severity,
                "run_unknown_revision",
                scope,
                "mission_run points to missing map_revisions row",
                run_id=run_id,
                revision_id=map_revision_id,
            )
            continue
        if zone_id and (map_revision_id, zone_id) not in zone_keys:
            _append(
                findings,
                severity,
                "run_zone_scope_missing",
                scope,
                "mission_run zone_id does not exist in the same map_revision_id",
                run_id=run_id,
                zone_id=zone_id,
                revision_id=map_revision_id,
            )
        if plan_id:
            plan_row = dict(plan_keys.get(plan_id) or {})
            if not plan_row:
                _append(
                    findings,
                    severity,
                    "run_unknown_plan",
                    scope,
                    "mission_run plan_id points to missing plans row",
                    run_id=run_id,
                    plan_id=plan_id,
                )
            elif _nonempty(plan_row.get("map_revision_id")) != map_revision_id:
                _append(
                    findings,
                    severity,
                    "run_plan_revision_mismatch",
                    scope,
                    "mission_run plan_id is not bound to the same map_revision_id",
                    run_id=run_id,
                    plan_id=plan_id,
                    revision_id=map_revision_id,
                    plan_revision_id=_nonempty(plan_row.get("map_revision_id")),
                )

    for row in list(ops_ctx.get("mission_checkpoints") or []):
        run_id = _nonempty(row.get("run_id"))
        scope = "mission_checkpoint:%s" % (run_id or "-")
        run_row = dict(run_keys.get(run_id) or {})
        if not run_row:
            _append(
                findings,
                "warning",
                "checkpoint_unknown_run",
                scope,
                "mission_checkpoint points to missing mission_runs row",
                run_id=run_id,
            )
            continue
        checkpoint_revision_id = _nonempty(row.get("map_revision_id"))
        run_revision_id = _nonempty(run_row.get("map_revision_id"))
        severity = "error" if _run_is_active(run_row) else "warning"
        if checkpoint_revision_id and run_revision_id and checkpoint_revision_id != run_revision_id:
            _append(
                findings,
                severity,
                "checkpoint_revision_mismatch",
                scope,
                "mission_checkpoint.map_revision_id mismatches mission_run.map_revision_id",
                run_id=run_id,
                checkpoint_revision_id=checkpoint_revision_id,
                run_revision_id=run_revision_id,
            )

    for row in list(ops_ctx.get("runtime_states") or []):
        robot = _nonempty(row.get("robot_id"))
        if robot_id and robot != robot_id:
            continue
        scope = "robot_runtime_state:%s" % (robot or "-")
        state_revision_id = _nonempty(row.get("map_revision_id"))
        if state_revision_id and state_revision_id not in revisions:
            _append(
                findings,
                "error",
                "runtime_state_unknown_revision",
                scope,
                "robot_runtime_state.map_revision_id points to missing map_revisions row",
                revision_id=state_revision_id,
            )
        active_run_id = _nonempty(row.get("active_run_id"))
        if active_run_id:
            run_row = dict(run_keys.get(active_run_id) or {})
            if not run_row:
                _append(
                    findings,
                    "error",
                    "runtime_state_unknown_run",
                    scope,
                    "robot_runtime_state.active_run_id points to missing mission_runs row",
                    active_run_id=active_run_id,
                )
            elif state_revision_id and _nonempty(run_row.get("map_revision_id")) and _nonempty(run_row.get("map_revision_id")) != state_revision_id:
                _append(
                    findings,
                    "error",
                    "runtime_state_run_revision_mismatch",
                    scope,
                    "robot_runtime_state.map_revision_id mismatches active mission_run.map_revision_id",
                    active_run_id=active_run_id,
                    state_revision_id=state_revision_id,
                    run_revision_id=_nonempty(run_row.get("map_revision_id")),
                )
        active_job_id = _nonempty(row.get("active_job_id"))
        if active_job_id:
            job_row = dict(job_keys.get(active_job_id) or {})
            if not job_row:
                _append(
                    findings,
                    "error",
                    "runtime_state_unknown_job",
                    scope,
                    "robot_runtime_state.active_job_id points to missing jobs row",
                    active_job_id=active_job_id,
                )
            elif state_revision_id and _nonempty(job_row.get("map_revision_id")) and _nonempty(job_row.get("map_revision_id")) != state_revision_id:
                _append(
                    findings,
                    "error",
                    "runtime_state_job_revision_mismatch",
                    scope,
                    "robot_runtime_state.map_revision_id mismatches active job.map_revision_id",
                    active_job_id=active_job_id,
                    state_revision_id=state_revision_id,
                    job_revision_id=_nonempty(job_row.get("map_revision_id")),
                )
        active_schedule_id = _nonempty(row.get("active_schedule_id"))
        if active_schedule_id and active_schedule_id not in schedule_keys:
            _append(
                findings,
                "error",
                "runtime_state_unknown_schedule",
                scope,
                "robot_runtime_state.active_schedule_id points to missing job_schedules row",
                active_schedule_id=active_schedule_id,
            )

    for row in list(ops_ctx.get("slam_jobs") or []):
        if _bool_int(row.get("done", 0)):
            continue
        job_id = _nonempty(row.get("job_id"))
        scope = "slam_job:%s" % (job_id or "-")
        requested_revision_id = _nonempty(row.get("requested_map_revision_id"))
        resolved_revision_id = _nonempty(row.get("resolved_map_revision_id"))
        if requested_revision_id and requested_revision_id not in revisions:
            _append(
                findings,
                "error",
                "slam_job_requested_revision_unknown",
                scope,
                "unfinished slam_job requested_map_revision_id points to missing map_revisions row",
                job_id=job_id,
                revision_id=requested_revision_id,
            )
        if resolved_revision_id and resolved_revision_id not in revisions:
            _append(
                findings,
                "error",
                "slam_job_resolved_revision_unknown",
                scope,
                "unfinished slam_job resolved_map_revision_id points to missing map_revisions row",
                job_id=job_id,
                revision_id=resolved_revision_id,
            )


def build_report(*, plan_db_path: str, ops_db_path: str = "", robot_id: str = "", strict: bool = False) -> Dict[str, object]:
    if not os.path.exists(plan_db_path):
        raise FileNotFoundError("plan_db not found: %s" % str(plan_db_path))
    findings: List[Dict[str, object]] = []
    plan_ctx = _plan_context(plan_db_path)
    ops_ctx = _ops_context(ops_db_path)
    _check_plan_revision_pointers(findings, plan_ctx, robot_id=str(robot_id or ""))
    _check_zone_plan_scope(findings, plan_ctx)
    if not bool(ops_ctx.get("available", False)):
        _append(
            findings,
            "warning",
            "ops_db_missing",
            "ops_db",
            "operations.db not found; skipped job/run/runtime revision scope checks",
            ops_db_path=str(ops_db_path or ""),
        )
    else:
        _check_ops_scope(findings, plan_ctx, ops_ctx, robot_id=str(robot_id or ""))
    revision_scope = _build_revision_scope(plan_ctx, robot_id=str(robot_id or ""))
    return {
        "plan_db_path": str(plan_db_path or ""),
        "ops_db_path": str(ops_db_path or ""),
        "robot_id": str(robot_id or ""),
        "revision_scope": revision_scope,
        "findings": findings,
        "summary": _summary(findings, strict=bool(strict)),
    }


def _format_scope_triplet(scope: Dict[str, object]) -> str:
    scope = dict(scope or {})
    return "%s/%s lifecycle=%s verification=%s status=%s" % (
        _nonempty(scope.get("map_name")) or "-",
        _nonempty(scope.get("revision_id")) or "-",
        _nonempty(scope.get("lifecycle_status")) or "-",
        _nonempty(scope.get("verification_status")) or "-",
        _nonempty(scope.get("status")) or "-",
    )


def _print_text(report: Dict[str, object]) -> None:
    summary = dict(report.get("summary") or {})
    print("Revision DB health")
    print("Plan DB: %s" % str(report.get("plan_db_path") or ""))
    if str(report.get("ops_db_path") or ""):
        print("Ops DB: %s" % str(report.get("ops_db_path") or ""))
    if str(report.get("robot_id") or ""):
        print("Robot: %s" % str(report.get("robot_id") or ""))
    print(
        "Summary: %s errors=%d warnings=%d strict=%s"
        % (
            "OK" if bool(summary.get("ok", False)) else "FAIL",
            int(summary.get("error_count", 0) or 0),
            int(summary.get("warning_count", 0) or 0),
            bool(summary.get("strict", False)),
        )
    )
    revision_scope = dict(report.get("revision_scope") or {})
    selected_robot_id = _nonempty(revision_scope.get("selected_robot_id"))
    if selected_robot_id or revision_scope.get("robots"):
        print("Revision scope:")
        if selected_robot_id:
            print("  selected_robot: %s" % selected_robot_id)
        print("  active: %s" % _format_scope_triplet(revision_scope.get("active") or {}))
        print("  latest_head: %s" % _format_scope_triplet(revision_scope.get("latest_head") or {}))
        print("  pending_target: %s" % _format_scope_triplet(revision_scope.get("pending_target") or {}))
        robot_scopes = list(revision_scope.get("robots") or [])
        if len(robot_scopes) > 1:
            for robot_scope in robot_scopes:
                print(
                    "  robot %s: active=%s latest_head=%s pending_target=%s"
                    % (
                        _nonempty(robot_scope.get("robot_id")) or "-",
                        _format_scope_triplet(robot_scope.get("active") or {}),
                        _format_scope_triplet(robot_scope.get("latest_head") or {}),
                        _format_scope_triplet(robot_scope.get("pending_target") or {}),
                    )
                )
    for item in list(report.get("findings") or []):
        print(
            "- %s [%s] %s: %s"
            % (
                str(item.get("severity") or "").upper(),
                str(item.get("code") or ""),
                str(item.get("scope") or ""),
                str(item.get("message") or ""),
            )
        )


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Check plan_db and operations.db for revision binding consistency across active/pending, zone/plan, and task/runtime state."
    )
    parser.add_argument("--plan-db-path", required=True, help="planning.db path")
    parser.add_argument("--ops-db-path", default="", help="operations.db path")
    parser.add_argument("--robot-id", default="", help="optional robot_id filter for active/pending/runtime rows")
    parser.add_argument("--strict", action="store_true", help="treat warnings as failure")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--text", action="store_true")
    return parser


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()
    try:
        report = build_report(
            plan_db_path=args.plan_db_path,
            ops_db_path=args.ops_db_path,
            robot_id=args.robot_id,
            strict=bool(args.strict),
        )
    except Exception as exc:
        if args.json:
            json.dump(
                {
                    "summary": {
                        "ok": False,
                        "error_count": 1,
                        "warning_count": 0,
                        "strict": bool(args.strict),
                    },
                    "findings": [_finding("error", "tool_failed", "tool", str(exc))],
                },
                sys.stdout,
                ensure_ascii=False,
                indent=2,
            )
            sys.stdout.write("\n")
        else:
            print("Revision DB health")
            print("Summary: FAIL errors=1 warnings=0 strict=%s" % bool(args.strict))
            print("- ERROR [tool_failed] tool: %s" % str(exc))
        return 1

    if args.json:
        json.dump(report, sys.stdout, ensure_ascii=False, indent=2)
        sys.stdout.write("\n")
    if args.text or not args.json:
        _print_text(report)
    return 0 if bool((report.get("summary") or {}).get("ok", False)) else 2


if __name__ == "__main__":
    raise SystemExit(main())

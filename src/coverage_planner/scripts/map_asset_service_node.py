#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import time

import rospy

from cleanrobot_app_msgs.msg import PgmData as AppPgmData
from cleanrobot_app_msgs.srv import (
    OperateMap as AppOperateMap,
    OperateMapResponse as AppOperateMapResponse,
)
from coverage_planner.map_asset_import import register_imported_map_asset
from coverage_planner.map_io import write_occupancy_to_yaml_pgm, yaml_pgm_to_occupancy
from coverage_planner.map_path_security import (
    MapPathSecurityError,
    copy_regular_file_exclusive,
    ensure_secure_parent_directory,
    map_asset_target_paths,
    validate_commercial_map_roots,
    validate_existing_regular_file,
    validate_map_name,
    validate_revision_id,
)
from coverage_planner.ops_store.store import OperationsStore
from coverage_planner.plan_store.store import PlanStore
from coverage_planner.ros_contract import build_contract_report, validate_ros_contract
from coverage_planner.service_mode import publish_contract_param

PgmData = AppPgmData
OperateMap = AppOperateMap
OperateMapResponse = AppOperateMapResponse

_PGM_DATA_FIELDS = [
    "map_name",
    "map_revision_id",
    "description",
    "enabled",
    "map_id",
    "map_md5",
    "lifecycle_status",
    "verification_status",
    "is_active",
    "is_latest_head",
    "has_newer_head_revision",
    "active_revision_id",
    "latest_head_revision_id",
    "latest_head_lifecycle_status",
    "latest_head_verification_status",
    "map_data",
]
_OPERATE_MAP_REQUEST_FIELDS = [
    "operation",
    "map_name",
    "map",
    "set_active",
    "enabled_state",
    "dry_run",
    "force",
    "cascade",
    "min_age_days",
    "max_reclaim_bytes",
    "confirm_token",
]
_OPERATE_MAP_REQUEST_CONSTANTS = [
    "ENABLE_KEEP",
    "ENABLE_DISABLE",
    "ENABLE_ENABLE",
    "get",
    "add",
    "modify",
    "Delete",
    "getAll",
    "hardDelete",
    "cleanupDisabled",
]
_OPERATE_MAP_RESPONSE_FIELDS = [
    "success",
    "message",
    "map",
    "maps",
    "dry_run",
    "cascade",
    "candidate_count",
    "deleted_count",
    "reclaimable_bytes",
    "reclaimed_bytes",
    "deleted_paths",
    "blocked_reasons",
    "affected_zones_count",
    "affected_plans_count",
    "affected_tasks_count",
    "affected_schedules_count",
    "affected_zone_versions_count",
    "confirm_token",
    "deleted_business_refs",
]


class MapAssetServiceNode:
    ACTIVE_SWITCH_REJECT_MESSAGE = (
        "direct map activation is disabled; use the SLAM submit workflow to verify and activate a map revision"
    )

    def __init__(self):
        self.plan_db_path = rospy.get_param("~plan_db_path", "/data/coverage/planning.db")
        self.ops_db_path = rospy.get_param("~ops_db_path", "/data/coverage/operations.db")
        self.robot_id = str(rospy.get_param("~robot_id", "local_robot"))
        self.app_service_name = str(
            rospy.get_param("~app_service_name", "/clean_robot_server/app/map_server")
        ).strip() or "/clean_robot_server/app/map_server"
        self.app_contract_param_ns = str(
            rospy.get_param("~app_contract_param_ns", "/clean_robot_server/contracts/app/map_server")
        ).strip() or "/clean_robot_server/contracts/app/map_server"
        self.maps_root = os.path.expanduser(str(rospy.get_param("~maps_root", "/data/maps")))
        self.external_maps_root = os.path.expanduser(
            str(rospy.get_param("~external_maps_root", "/data/maps/imports")).strip() or "/data/maps/imports"
        )
        validate_commercial_map_roots(
            self.maps_root,
            external_maps_root=self.external_maps_root,
        )
        self.map_topic = str(rospy.get_param("~map_topic", "/map")).strip() or "/map"
        self.map_timeout_s = max(0.5, float(rospy.get_param("~map_timeout_s", 5.0)))
        self.store = PlanStore(self.plan_db_path)
        self.ops = OperationsStore(self.ops_db_path)
        self._app_contract_report = self._prepare_app_contract_report()
        self.srv = None
        self.app_srv = rospy.Service(self.app_service_name, AppOperateMap, self._handle_app)
        publish_contract_param(rospy, self.app_contract_param_ns, self._app_contract_report, enabled=True)
        rospy.loginfo(
            "[map_asset_service] ready app_service=%s plan_db=%s ops_db=%s external_maps_root=%s app_contract=%s",
            self.app_service_name,
            self.plan_db_path,
            self.ops_db_path,
            self.external_maps_root,
            self.app_contract_param_ns or "-",
        )

    def _prepare_app_contract_report(self):
        validate_ros_contract("AppPgmData", AppPgmData, required_fields=_PGM_DATA_FIELDS)
        validate_ros_contract(
            "AppOperateMapRequest",
            AppOperateMap._request_class,
            required_fields=_OPERATE_MAP_REQUEST_FIELDS,
            required_constants=_OPERATE_MAP_REQUEST_CONSTANTS,
        )
        validate_ros_contract(
            "AppOperateMapResponse",
            AppOperateMapResponse,
            required_fields=_OPERATE_MAP_RESPONSE_FIELDS,
        )
        return build_contract_report(
            service_name=self.app_service_name,
            contract_name="map_server_app",
            service_cls=AppOperateMap,
            request_cls=AppOperateMap._request_class,
            response_cls=AppOperateMapResponse,
            dependencies={"map": AppPgmData},
            features=[
                "map_asset_revision_view",
                "import_as_candidate_revision",
                "verified_head_vs_active_revision_projection",
                "protected_map_asset_gc",
                "cascade_map_revision_gc",
                "cleanrobot_app_msgs_parallel",
            ],
        )

    def _empty_resp(self, *, success: bool, message: str, response_cls=AppOperateMapResponse, map_cls=AppPgmData):
        return response_cls(success=bool(success), message=str(message or ""), map=map_cls(), maps=[])

    def _resolve_req_name(self, req):
        map_name = str(req.map_name or "")
        if (not map_name.strip()) and getattr(req, "map", None):
            map_name = str(req.map.map_name or "")
        return validate_map_name(map_name, allow_empty=True)

    def _resolve_req_revision_id(self, req):
        if not getattr(req, "map", None):
            return ""
        return validate_revision_id(
            getattr(req.map, "map_revision_id", ""),
            allow_empty=True,
        )

    def _target_paths(self, map_name: str, revision_id: str = ""):
        return map_asset_target_paths(self.maps_root, map_name, revision_id)

    def _cleanup_paths(self, *paths: str):
        for path in paths:
            pp = str(path or "").strip()
            if not pp or not os.path.lexists(pp):
                continue
            try:
                os.remove(pp)
            except Exception:
                pass

    def _set_current_map_params(self, asset: dict):
        rospy.set_param("/map_name", str(asset.get("map_name") or ""))

    def _enabled_from_request(self, req, *, current_enabled: bool, default_enabled: bool) -> bool:
        enabled_state = int(getattr(req, "enabled_state", int(req.ENABLE_KEEP)))
        if enabled_state == int(req.ENABLE_ENABLE):
            return True
        if enabled_state == int(req.ENABLE_DISABLE):
            return False
        return bool(default_enabled if current_enabled is None else current_enabled)

    @staticmethod
    def _request_bool(req, field_name: str, default: bool = False) -> bool:
        return bool(getattr(req, field_name, default))

    @staticmethod
    def _request_uint(req, field_name: str, default: int = 0) -> int:
        try:
            return max(0, int(getattr(req, field_name, default) or 0))
        except Exception:
            return int(default or 0)

    @staticmethod
    def _request_text(req, field_name: str, default: str = "") -> str:
        return str(getattr(req, field_name, default) or "").strip()

    def _runtime_map_revision_id(self) -> str:
        for key in (
            "/map_revision_id",
            "/cartographer/runtime/current_map_revision_id",
            "/cartographer/runtime/map_revision_id",
        ):
            try:
                value = str(rospy.get_param(key, "") or "").strip()
                if value:
                    return value
            except Exception:
                pass
        ops = getattr(self, "ops", None)
        try:
            state = ops.get_robot_runtime_state(self.robot_id) if ops is not None else None
            value = str(getattr(state, "map_revision_id", "") or "").strip()
            if value:
                return value
        except Exception:
            pass
        return ""

    def _revision_file_paths(self, asset: dict):
        paths = []
        seen = set()
        for key in ("pbstream_path", "yaml_path", "pgm_path"):
            path = os.path.expanduser(str((asset or {}).get(key) or "").strip())
            if not path or path in seen:
                continue
            seen.add(path)
            paths.append(path)
        return paths

    def _path_in_maps_root(self, path: str) -> bool:
        candidate = os.path.realpath(os.path.expanduser(str(path or "").strip()))
        root = os.path.realpath(os.path.expanduser(str(self.maps_root or "")))
        if not candidate or not root:
            return False
        try:
            return os.path.commonpath([candidate, root]) == root
        except Exception:
            return False

    @staticmethod
    def _existing_file_size(path: str) -> int:
        try:
            return int(os.path.getsize(path)) if os.path.isfile(path) else 0
        except Exception:
            return 0

    @staticmethod
    def _count_from_conn(conn, query: str, args=()) -> int:
        try:
            row = conn.execute(query, tuple(args or ())).fetchone()
            return int(row["count"] or 0) if row else 0
        except Exception:
            return 0

    @staticmethod
    def _json_summary(data) -> str:
        try:
            return json.dumps(dict(data or {}), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        except Exception:
            return "{}"

    @staticmethod
    def _cascade_confirm_token(revision_id: str) -> str:
        return "CASCADE_DELETE:%s" % str(revision_id or "").strip()

    @staticmethod
    def _hard_delete_confirm_token(revision_id: str) -> str:
        return "DELETE:%s" % str(revision_id or "").strip()

    def _ops_business_refs_for_revision(self, revision_id: str):
        revision_id = str(revision_id or "").strip()
        refs = {
            "tasks": 0,
            "schedules": 0,
            "schedule_state": 0,
        }
        ops = getattr(self, "ops", None)
        if (not revision_id) or ops is None:
            return refs
        conn = None
        try:
            conn = ops._connect()
            if not ops._table_exists(conn, "jobs") or "map_revision_id" not in ops._table_columns(conn, "jobs"):
                return refs
            job_rows = conn.execute(
                "SELECT job_id FROM jobs WHERE map_revision_id=?;",
                (revision_id,),
            ).fetchall()
            job_ids = [str(row["job_id"] or "").strip() for row in job_rows or [] if str(row["job_id"] or "").strip()]
            refs["tasks"] = len(job_ids)
            if job_ids and ops._table_exists(conn, "job_schedules"):
                placeholders = ",".join("?" for _ in job_ids)
                schedule_rows = conn.execute(
                    "SELECT schedule_id FROM job_schedules WHERE job_id IN (%s);" % placeholders,
                    tuple(job_ids),
                ).fetchall()
                schedule_ids = [
                    str(row["schedule_id"] or "").strip()
                    for row in schedule_rows or []
                    if str(row["schedule_id"] or "").strip()
                ]
                refs["schedules"] = len(schedule_ids)
                if schedule_ids and ops._table_exists(conn, "job_schedule_state"):
                    refs["schedule_state"] = self._count_from_conn(
                        conn,
                        "SELECT COUNT(*) AS count FROM job_schedule_state WHERE schedule_id IN (%s);" % ",".join("?" for _ in schedule_ids),
                        tuple(schedule_ids),
                    )
        except Exception:
            return refs
        finally:
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass
        return refs

    def _business_refs_for_revision(self, revision_id: str):
        revision_id = str(revision_id or "").strip()
        refs = {
            "zones": 0,
            "zone_versions": 0,
            "zone_editor_metadata": 0,
            "plans": 0,
            "plan_blocks": 0,
            "zone_active_plans": 0,
            "map_constraint_revision_versions": 0,
            "map_revision_no_go_areas": 0,
            "map_revision_virtual_walls": 0,
            "map_active_constraint_revisions": 0,
            "map_alignment_revision_configs": 0,
            "map_active_alignment_revisions": 0,
            "tasks": 0,
            "schedules": 0,
            "schedule_state": 0,
        }
        if not revision_id:
            return refs
        for table in (
            "zones",
            "zone_versions",
            "zone_editor_metadata",
            "plans",
            "zone_active_plans",
            "map_constraint_revision_versions",
            "map_revision_no_go_areas",
            "map_revision_virtual_walls",
            "map_active_constraint_revisions",
            "map_alignment_revision_configs",
            "map_active_alignment_revisions",
        ):
            try:
                if not self.store._table_exists(table) or "map_revision_id" not in self.store._table_columns(table):
                    continue
                refs[table] = self._count_from_conn(
                    self.store.conn,
                    "SELECT COUNT(*) AS count FROM %s WHERE map_revision_id=?;" % table,
                    (revision_id,),
                )
            except Exception:
                continue
        try:
            if refs["plans"] and self.store._table_exists("plan_blocks"):
                plan_rows = self.store.conn.execute(
                    "SELECT plan_id FROM plans WHERE map_revision_id=?;",
                    (revision_id,),
                ).fetchall()
                plan_ids = [str(row["plan_id"] or "").strip() for row in plan_rows or [] if str(row["plan_id"] or "").strip()]
                if plan_ids:
                    refs["plan_blocks"] = self._count_from_conn(
                        self.store.conn,
                        "SELECT COUNT(*) AS count FROM plan_blocks WHERE plan_id IN (%s);" % ",".join("?" for _ in plan_ids),
                        tuple(plan_ids),
                    )
        except Exception:
            pass
        refs.update(self._ops_business_refs_for_revision(revision_id))
        return refs

    def _reference_counts_for_revision(self, revision_id: str):
        return {
            key: int(value or 0)
            for key, value in self._business_refs_for_revision(revision_id).items()
            if int(value or 0) > 0
        }

    def _gc_candidate_for_asset(self, asset: dict, *, runtime_revision_id: str = "", cascade: bool = False):
        asset = dict(asset or {})
        revision_id = str(asset.get("revision_id") or "").strip()
        map_name = str(asset.get("map_name") or "").strip()
        blockers = []
        if not revision_id:
            blockers.append("map_revision_id is required")
        if bool(asset.get("enabled", True)):
            blockers.append("map revision must be soft-deleted before hard delete")

        active_map = self.store.get_active_map(robot_id=self.robot_id) or {}
        active_revision_id = str(active_map.get("revision_id") or "").strip()
        if active_revision_id and revision_id and active_revision_id == revision_id:
            blockers.append("cannot hard-delete the active map revision")
        elif (not active_revision_id) and map_name and str(active_map.get("map_name") or "").strip() == map_name:
            blockers.append("cannot hard-delete the active map")

        runtime_revision_id = str(runtime_revision_id or self._runtime_map_revision_id() or "").strip()
        if runtime_revision_id and revision_id and runtime_revision_id == revision_id:
            blockers.append("cannot hard-delete the runtime map revision")

        pending_revision = self.store.get_pending_map_revision(robot_id=self.robot_id) or {}
        if revision_id and revision_id in {
            str(pending_revision.get("from_revision_id") or "").strip(),
            str(pending_revision.get("target_revision_id") or "").strip(),
        }:
            blockers.append("cannot hard-delete a pending map switch revision")

        business_refs = self._business_refs_for_revision(revision_id)
        if not bool(cascade):
            for table, count in sorted((k, v) for k, v in business_refs.items() if int(v or 0) > 0):
                blockers.append("referenced by %s: %d" % (table, count))

        paths = self._revision_file_paths(asset)
        for path in paths:
            if not self._path_in_maps_root(path):
                blockers.append("asset path is outside maps_root: %s" % path)
        reclaimable_bytes = sum(self._existing_file_size(path) for path in paths)
        return {
            "asset": asset,
            "paths": paths,
            "blockers": blockers,
            "cascade": bool(cascade),
            "business_refs": business_refs,
            "reclaimable_bytes": int(reclaimable_bytes),
        }

    def _make_gc_response(
        self,
        *,
        success: bool,
        message: str,
        dry_run: bool,
        candidates,
        deleted_paths=None,
        reclaimed_bytes: int = 0,
        confirm_token: str = "",
        deleted_business_refs=None,
        response_cls=AppOperateMapResponse,
        map_cls=AppPgmData,
    ):
        candidates = list(candidates or [])
        blocked_reasons = []
        for candidate in candidates:
            revision_id = str((candidate.get("asset") or {}).get("revision_id") or "").strip()
            for blocker in list(candidate.get("blockers") or []):
                blocked_reasons.append(("%s: %s" % (revision_id, blocker)).strip(": "))
        maps = [
            self._to_msg(
                candidate.get("asset") or {},
                include_map_data=False,
                active_map=self.store.get_active_map(robot_id=self.robot_id) or {},
                msg_cls=map_cls,
            )
            for candidate in candidates
        ]
        resp = response_cls(
            success=bool(success),
            message=str(message or ""),
            map=maps[0] if maps else map_cls(),
            maps=maps,
        )
        def _sum_ref(key: str) -> int:
            return int(sum(int((candidate.get("business_refs") or {}).get(key) or 0) for candidate in candidates))

        extra = {
            "dry_run": bool(dry_run),
            "cascade": any(bool(candidate.get("cascade", False)) for candidate in candidates),
            "candidate_count": len(candidates),
            "deleted_count": sum(1 for candidate in candidates if bool(candidate.get("deleted", False))),
            "reclaimable_bytes": int(sum(int(candidate.get("reclaimable_bytes") or 0) for candidate in candidates)),
            "reclaimed_bytes": int(reclaimed_bytes or 0),
            "deleted_paths": list(deleted_paths or []),
            "blocked_reasons": blocked_reasons,
            "affected_zones_count": _sum_ref("zones"),
            "affected_plans_count": _sum_ref("plans"),
            "affected_tasks_count": _sum_ref("tasks"),
            "affected_schedules_count": _sum_ref("schedules"),
            "affected_zone_versions_count": _sum_ref("zone_versions"),
            "confirm_token": str(confirm_token or ""),
            "deleted_business_refs": self._json_summary(deleted_business_refs or {}),
        }
        for key, value in extra.items():
            if hasattr(resp, key):
                setattr(resp, key, value)
        return resp

    @staticmethod
    def _sql_placeholders(values) -> str:
        return ",".join("?" for _ in list(values or []))

    def _validate_hard_delete_candidate_files(self, candidate):
        for path in list((candidate or {}).get("paths") or []):
            if not path:
                continue
            if not self._path_in_maps_root(path):
                raise ValueError("asset path is outside maps_root: %s" % path)
            if os.path.exists(path) and not os.path.isfile(path):
                raise ValueError("asset path is not a file: %s" % path)

    def _delete_plan_business_refs_for_revision(self, revision_id: str):
        revision_id = str(revision_id or "").strip()
        summary = {}
        if not revision_id:
            return summary
        plan_ids = []
        if self.store._table_exists("plans"):
            try:
                rows = self.store.conn.execute(
                    "SELECT plan_id FROM plans WHERE map_revision_id=?;",
                    (revision_id,),
                ).fetchall()
                plan_ids = [
                    str(row["plan_id"] or "").strip()
                    for row in rows or []
                    if str(row["plan_id"] or "").strip()
                ]
            except Exception:
                plan_ids = []
        with self.store.conn:
            if plan_ids and self.store._table_exists("plan_blocks"):
                cur = self.store.conn.execute(
                    "DELETE FROM plan_blocks WHERE plan_id IN (%s);" % self._sql_placeholders(plan_ids),
                    tuple(plan_ids),
                )
                summary["plan_blocks"] = max(0, int(cur.rowcount or 0))
            for table in (
                "zone_active_plans",
                "zone_editor_metadata",
                "plans",
                "zone_versions",
                "zones",
                "map_active_alignment_revisions",
                "map_alignment_revision_configs",
                "map_active_constraint_revisions",
                "map_revision_virtual_walls",
                "map_revision_no_go_areas",
                "map_constraint_revision_versions",
            ):
                if not self.store._table_exists(table) or "map_revision_id" not in self.store._table_columns(table):
                    continue
                cur = self.store.conn.execute(
                    "DELETE FROM %s WHERE map_revision_id=?;" % table,
                    (revision_id,),
                )
                summary[table] = max(0, int(cur.rowcount or 0))
        return summary

    def _delete_ops_business_refs_for_revision(self, revision_id: str):
        revision_id = str(revision_id or "").strip()
        summary = {}
        ops = getattr(self, "ops", None)
        if (not revision_id) or ops is None:
            return summary
        conn = None
        try:
            conn = ops._connect()
            if not ops._table_exists(conn, "jobs") or "map_revision_id" not in ops._table_columns(conn, "jobs"):
                return summary
            job_rows = conn.execute(
                "SELECT job_id FROM jobs WHERE map_revision_id=?;",
                (revision_id,),
            ).fetchall()
            job_ids = [str(row["job_id"] or "").strip() for row in job_rows or [] if str(row["job_id"] or "").strip()]
            schedule_ids = []
            if job_ids and ops._table_exists(conn, "job_schedules"):
                schedule_rows = conn.execute(
                    "SELECT schedule_id FROM job_schedules WHERE job_id IN (%s);" % self._sql_placeholders(job_ids),
                    tuple(job_ids),
                ).fetchall()
                schedule_ids = [
                    str(row["schedule_id"] or "").strip()
                    for row in schedule_rows or []
                    if str(row["schedule_id"] or "").strip()
                ]
            with conn:
                if schedule_ids and ops._table_exists(conn, "job_schedule_state"):
                    cur = conn.execute(
                        "DELETE FROM job_schedule_state WHERE schedule_id IN (%s);" % self._sql_placeholders(schedule_ids),
                        tuple(schedule_ids),
                    )
                    summary["schedule_state"] = max(0, int(cur.rowcount or 0))
                if schedule_ids and ops._table_exists(conn, "job_schedules"):
                    cur = conn.execute(
                        "DELETE FROM job_schedules WHERE schedule_id IN (%s);" % self._sql_placeholders(schedule_ids),
                        tuple(schedule_ids),
                    )
                    summary["schedules"] = max(0, int(cur.rowcount or 0))
                if job_ids:
                    cur = conn.execute(
                        "DELETE FROM jobs WHERE job_id IN (%s);" % self._sql_placeholders(job_ids),
                        tuple(job_ids),
                    )
                    summary["tasks"] = max(0, int(cur.rowcount or 0))
            return summary
        finally:
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass

    def _delete_cascade_business_refs(self, candidate):
        asset = dict((candidate or {}).get("asset") or {})
        revision_id = str(asset.get("revision_id") or "").strip()
        summary = {}
        for key, value in self._delete_ops_business_refs_for_revision(revision_id).items():
            summary[key] = int(summary.get(key, 0) or 0) + int(value or 0)
        for key, value in self._delete_plan_business_refs_for_revision(revision_id).items():
            summary[key] = int(summary.get(key, 0) or 0) + int(value or 0)
        candidate["deleted_business_refs"] = summary
        return summary

    def _promote_or_remove_head_after_revision_delete(self, *, map_name: str):
        map_name = str(map_name or "").strip()
        if not map_name:
            return
        rows = self.store._fetch_rows(
            "SELECT * FROM map_revisions WHERE map_name=? ORDER BY created_ts DESC, updated_ts DESC, revision_id DESC LIMIT 1;",
            (map_name,),
        )
        if not rows:
            self.store.conn.execute("DELETE FROM map_assets WHERE map_name=?;", (map_name,))
            self.store.conn.commit()
            return
        revision = self.store._map_revision_row_to_dict(rows[0])
        self.store.upsert_map_asset(
            map_name=map_name,
            display_name=str(revision.get("display_name") or map_name),
            enabled=bool(revision.get("enabled", True)),
            description=str(revision.get("description") or ""),
            map_id=str(revision.get("map_id") or ""),
            map_md5=str(revision.get("map_md5") or ""),
            yaml_path=str(revision.get("yaml_path") or ""),
            pgm_path=str(revision.get("pgm_path") or ""),
            pbstream_path=str(revision.get("pbstream_path") or ""),
            frame_id=str(revision.get("frame_id") or "map"),
            resolution=float(revision.get("resolution") or 0.0),
            origin=list(revision.get("origin") or [0.0, 0.0, 0.0]),
            lifecycle_status=str(revision.get("lifecycle_status") or "available"),
            verification_status=str(revision.get("verification_status") or "verified"),
            save_snapshot_md5=str(revision.get("live_snapshot_md5") or ""),
            verified_runtime_map_id=str(revision.get("verified_runtime_map_id") or ""),
            verified_runtime_map_md5=str(revision.get("verified_runtime_map_md5") or ""),
            last_error_code=str(revision.get("last_error_code") or ""),
            last_error_msg=str(revision.get("last_error_msg") or ""),
            source_job_id=str(revision.get("source_job_id") or ""),
            revision_id=str(revision.get("revision_id") or ""),
        )

    def _hard_delete_candidate(self, candidate):
        asset = dict(candidate.get("asset") or {})
        revision_id = str(asset.get("revision_id") or "").strip()
        map_name = str(asset.get("map_name") or "").strip()
        deleted_paths = []
        reclaimed_bytes = 0
        if not revision_id:
            raise ValueError("map_revision_id is required")

        self._validate_hard_delete_candidate_files(candidate)
        existing_paths = []
        for path in list(candidate.get("paths") or []):
            if not path:
                continue
            if os.path.exists(path):
                existing_paths.append(path)

        for path in existing_paths:
            size = self._existing_file_size(path)
            os.remove(path)
            deleted_paths.append(path)
            reclaimed_bytes += size
            parent = os.path.dirname(path)
            try:
                if parent and self._path_in_maps_root(parent) and not os.listdir(parent):
                    os.rmdir(parent)
            except Exception:
                pass

        self.store.conn.execute("DELETE FROM map_revisions WHERE revision_id=?;", (revision_id,))
        self.store.conn.commit()
        self._promote_or_remove_head_after_revision_delete(map_name=map_name)
        candidate["deleted"] = True
        return deleted_paths, int(reclaimed_bytes)

    def _latest_head_asset(self, map_name: str):
        normalized_map_name = str(map_name or "").strip()
        if not normalized_map_name:
            return {}
        return self.store.resolve_map_asset(
            map_name=normalized_map_name,
            robot_id=self.robot_id,
        ) or {}

    def _preferred_query_asset(self, *, map_name: str, map_revision_id: str = "", active_map=None):
        normalized_map_name = str(map_name or "").strip()
        normalized_revision_id = str(map_revision_id or "").strip()
        active_map = active_map or {}
        if normalized_revision_id:
            return self.store.resolve_map_asset(
                map_name=normalized_map_name,
                revision_id=normalized_revision_id,
                robot_id=self.robot_id,
            )
        if normalized_map_name:
            resolve_revision = getattr(self.store, "resolve_map_revision", None)
            if callable(resolve_revision):
                preferred_revision = resolve_revision(
                    map_name=normalized_map_name,
                    robot_id=self.robot_id,
                )
                if preferred_revision is not None:
                    return preferred_revision
            active_map_name = str(active_map.get("map_name") or "").strip()
            active_revision_id = str(active_map.get("revision_id") or "").strip()
            if active_revision_id and active_map_name == normalized_map_name:
                active_revision = self.store.resolve_map_asset(
                    revision_id=active_revision_id,
                    robot_id=self.robot_id,
                )
                if active_revision is not None:
                    return active_revision
                return active_map or None
            return self.store.resolve_map_asset(
                map_name=normalized_map_name,
                robot_id=self.robot_id,
            )
        return active_map or self.store.get_active_map(robot_id=self.robot_id)

    def _import_external_map(self, *, map_name: str, set_active: bool, description: str = ""):
        map_name = validate_map_name(map_name)
        description = str(description or "")

        source_pbstream = validate_existing_regular_file(
            self.external_maps_root,
            os.path.join(self.external_maps_root, map_name + ".pbstream"),
            suffix=".pbstream",
            label="external pbstream",
        )
        source_yaml = validate_existing_regular_file(
            self.external_maps_root,
            os.path.join(self.external_maps_root, map_name + ".yaml"),
            suffix=".yaml",
            label="external yaml",
        )

        revision_id = self.store.generate_map_revision_id(map_name)
        target_paths = self._target_paths(map_name, revision_id=revision_id)
        for path in target_paths.values():
            if os.path.lexists(path):
                raise ValueError("target asset path already exists: %s" % path)

        occ = yaml_pgm_to_occupancy(source_yaml, allowed_root=self.external_maps_root)
        pgm_path = ""
        yaml_path = ""
        pbstream_path = target_paths["pbstream_path"]
        try:
            artifact_dir = os.path.dirname(pbstream_path)
            if artifact_dir:
                ensure_secure_parent_directory(self.maps_root, pbstream_path)
            pgm_path, yaml_path = write_occupancy_to_yaml_pgm(
                occ,
                artifact_dir or self.maps_root,
                base_name=map_name,
                allowed_root=self.maps_root,
            )
            copy_regular_file_exclusive(
                self.external_maps_root,
                source_pbstream,
                self.maps_root,
                pbstream_path,
                suffix=".pbstream",
            )
            asset, _snapshot_md5 = register_imported_map_asset(
                self.store,
                map_name=map_name,
                occ=occ,
                yaml_path=yaml_path,
                pgm_path=pgm_path,
                pbstream_path=pbstream_path,
                display_name=map_name,
                description=description,
                enabled=True,
                robot_id=self.robot_id,
                verification_mode="candidate",
                set_active=False,
                revision_id=revision_id,
            )
        except Exception:
            self._cleanup_paths(pgm_path, yaml_path, pbstream_path)
            raise

        return asset or {
            "map_name": map_name,
            "map_id": "",
            "map_md5": "",
            "yaml_path": yaml_path,
            "pgm_path": pgm_path,
            "pbstream_path": pbstream_path,
            "enabled": True,
            "lifecycle_status": "saved_unverified",
            "verification_status": "pending",
        }

    def _to_msg(
        self,
        asset: dict,
        *,
        include_map_data: bool = False,
        active_map=None,
        latest_head=None,
        msg_cls=AppPgmData,
    ):
        msg = msg_cls()
        if not asset:
            return msg
        active_map = active_map or {}
        latest_head = latest_head or self._latest_head_asset(str(asset.get("map_name") or ""))
        msg.map_name = str(asset.get("map_name") or "")
        msg.map_revision_id = str(asset.get("revision_id") or "")
        msg.description = str(asset.get("description") or "")
        msg.enabled = bool(asset.get("enabled", True))
        msg.map_id = str(asset.get("map_id") or "")
        msg.map_md5 = str(asset.get("map_md5") or "")
        msg.lifecycle_status = str(asset.get("lifecycle_status") or "")
        msg.verification_status = str(asset.get("verification_status") or "")
        active_revision_id = str(active_map.get("revision_id") or "").strip()
        active_map_name = str(active_map.get("map_name") or "").strip()
        latest_head_revision_id = str(latest_head.get("revision_id") or asset.get("revision_id") or "").strip()
        if active_revision_id and msg.map_revision_id:
            msg.is_active = msg.map_revision_id == active_revision_id
        else:
            msg.is_active = msg.map_name == active_map_name
        if latest_head_revision_id and msg.map_revision_id:
            msg.is_latest_head = msg.map_revision_id == latest_head_revision_id
        else:
            msg.is_latest_head = True
        msg.active_revision_id = active_revision_id if active_map_name == msg.map_name else ""
        msg.latest_head_revision_id = latest_head_revision_id
        msg.latest_head_lifecycle_status = str(latest_head.get("lifecycle_status") or asset.get("lifecycle_status") or "")
        msg.latest_head_verification_status = str(
            latest_head.get("verification_status") or asset.get("verification_status") or ""
        )
        msg.has_newer_head_revision = bool(
            msg.active_revision_id and msg.latest_head_revision_id and msg.active_revision_id != msg.latest_head_revision_id
        )
        if include_map_data:
            yaml_path = str(asset.get("yaml_path") or "").strip()
            if yaml_path:
                msg.map_data = yaml_pgm_to_occupancy(
                    yaml_path,
                    allowed_root=self.maps_root,
                )
        return msg

    def _list_map_views(self, *, msg_cls=AppPgmData):
        active_map = self.store.get_active_map(robot_id=self.robot_id) or {}
        out = []
        seen = set()
        for head_asset in self.store.list_map_assets():
            name = str(head_asset.get("map_name") or "")
            if not name or name in seen:
                continue
            seen.add(name)
            view_asset = self._preferred_query_asset(
                map_name=name,
                map_revision_id="",
                active_map=active_map,
            ) or head_asset
            out.append(
                self._to_msg(
                    view_asset,
                    include_map_data=False,
                    active_map=active_map,
                    latest_head=head_asset,
                    msg_cls=msg_cls,
                )
            )
        active_map_name = str(active_map.get("map_name") or "").strip()
        if active_map_name and active_map_name not in seen:
            head_asset = self._latest_head_asset(active_map_name) or active_map
            out.append(
                self._to_msg(
                    active_map,
                    include_map_data=False,
                    active_map=active_map,
                    latest_head=head_asset,
                    msg_cls=msg_cls,
                )
            )
        return out

    def _handle(self, req, *, response_cls=AppOperateMapResponse, map_cls=AppPgmData):
        op = int(req.operation)
        try:
            map_name = self._resolve_req_name(req)
            map_revision_id = self._resolve_req_revision_id(req)
        except MapPathSecurityError as exc:
            return self._empty_resp(
                success=False,
                message="%s: %s" % (str(exc.code or "invalid_map_path"), str(exc)),
                response_cls=response_cls,
                map_cls=map_cls,
            )
        active_map = self.store.get_active_map(robot_id=self.robot_id) or {}

        if op == int(req.getAll):
            return response_cls(success=True, message="ok", map=map_cls(), maps=self._list_map_views(msg_cls=map_cls))

        if op == int(req.get):
            asset = self._preferred_query_asset(
                map_name=map_name,
                map_revision_id=map_revision_id,
                active_map=active_map,
            )
            if not asset:
                return self._empty_resp(success=False, message="map asset not found", response_cls=response_cls, map_cls=map_cls)
            latest_head = self._latest_head_asset(str(asset.get("map_name") or "")) or asset
            return response_cls(
                success=True,
                message="ok",
                map=self._to_msg(
                    asset,
                    include_map_data=True,
                    active_map=active_map,
                    latest_head=latest_head,
                    msg_cls=map_cls,
                ),
                maps=[],
            )

        if op == int(req.add):
            if not map_name:
                return self._empty_resp(success=False, message="map_name is required for add", response_cls=response_cls, map_cls=map_cls)
            try:
                desired_enabled = self._enabled_from_request(req, current_enabled=None, default_enabled=True)
                if bool(req.set_active):
                    return self._empty_resp(success=False, message=self.ACTIVE_SWITCH_REJECT_MESSAGE, response_cls=response_cls, map_cls=map_cls)
                if bool(req.set_active) and (not desired_enabled):
                    return self._empty_resp(success=False, message="cannot set_active on a disabled map", response_cls=response_cls, map_cls=map_cls)
                asset = self._import_external_map(
                    map_name=map_name,
                    set_active=False,
                    description=str(getattr(req.map, "description", "") or ""),
                )
                if not desired_enabled:
                    asset = self.store.update_map_revision_meta(
                        revision_id=str(asset.get("revision_id") or ""),
                        robot_id=self.robot_id,
                        enabled=False,
                    )
                active_map = self.store.get_active_map(robot_id=self.robot_id) or active_map
                return response_cls(
                    success=True,
                    message="imported as unverified candidate revision",
                    map=self._to_msg(asset, include_map_data=False, active_map=active_map, msg_cls=map_cls),
                    maps=[],
                )
            except Exception as e:
                return self._empty_resp(success=False, message=str(e), response_cls=response_cls, map_cls=map_cls)

        if op == int(req.modify):
            if (not map_name) and (not map_revision_id):
                return self._empty_resp(success=False, message="map_name or map_revision_id is required for modify", response_cls=response_cls, map_cls=map_cls)
            current_asset = self._preferred_query_asset(
                map_name=map_name,
                map_revision_id=map_revision_id,
                active_map=active_map,
            )
            if not current_asset:
                return self._empty_resp(success=False, message="map revision not found", response_cls=response_cls, map_cls=map_cls)
            desired_enabled = self._enabled_from_request(
                req,
                current_enabled=bool(current_asset.get("enabled", True)),
                default_enabled=True,
            )
            if bool(req.set_active) and (not desired_enabled):
                return self._empty_resp(success=False, message="cannot set_active on a disabled map", response_cls=response_cls, map_cls=map_cls)
            if bool(req.set_active):
                return self._empty_resp(success=False, message=self.ACTIVE_SWITCH_REJECT_MESSAGE, response_cls=response_cls, map_cls=map_cls)
            asset = self.store.update_map_revision_meta(
                revision_id=str(current_asset.get("revision_id") or map_revision_id or "").strip(),
                map_name=str(current_asset.get("map_name") or map_name),
                robot_id=self.robot_id,
                display_name=str(current_asset.get("map_name") or map_name),
                description=(req.map.description if str(req.map.description or "").strip() else None),
                enabled=desired_enabled,
            )
            active_map = self.store.get_active_map(robot_id=self.robot_id) or active_map
            latest_head = self._latest_head_asset(str(asset.get("map_name") or map_name)) or asset
            return response_cls(
                success=True,
                message="updated",
                map=self._to_msg(asset or {}, include_map_data=False, active_map=active_map, latest_head=latest_head, msg_cls=map_cls),
                maps=[],
            )

        if op == int(req.Delete):
            if (not map_name) and (not map_revision_id):
                return self._empty_resp(success=False, message="map_name or map_revision_id is required for Delete", response_cls=response_cls, map_cls=map_cls)
            target_asset = self._preferred_query_asset(
                map_name=map_name,
                map_revision_id=map_revision_id,
                active_map=active_map,
            )
            if not target_asset:
                return self._empty_resp(success=False, message="map revision not found", response_cls=response_cls, map_cls=map_cls)
            if (
                str(active_map.get("revision_id") or "").strip()
                and str(target_asset.get("revision_id") or "").strip() == str(active_map.get("revision_id") or "").strip()
            ) or (
                (not str(active_map.get("revision_id") or "").strip())
                and str(active_map.get("map_name") or "").strip() == str(target_asset.get("map_name") or "").strip()
            ):
                return self._empty_resp(success=False, message="cannot disable the active map", response_cls=response_cls, map_cls=map_cls)
            asset = self.store.disable_map_revision(
                revision_id=str(target_asset.get("revision_id") or map_revision_id or "").strip(),
                map_name=str(target_asset.get("map_name") or map_name),
                robot_id=self.robot_id,
            )
            latest_head = self._latest_head_asset(str(asset.get("map_name") or map_name)) or asset
            return response_cls(
                success=True,
                message="disabled",
                map=self._to_msg(asset or {}, include_map_data=False, active_map=active_map, latest_head=latest_head, msg_cls=map_cls),
                maps=[],
            )

        hard_delete_op = int(getattr(req, "hardDelete", 5))
        cleanup_disabled_op = int(getattr(req, "cleanupDisabled", 6))

        if op == hard_delete_op:
            if not map_revision_id:
                return self._empty_resp(success=False, message="map_revision_id is required for hardDelete", response_cls=response_cls, map_cls=map_cls)
            dry_run = self._request_bool(req, "dry_run", True)
            cascade = self._request_bool(req, "cascade", False)
            confirm_token = self._request_text(req, "confirm_token", "")
            target_asset = self.store.resolve_map_asset(
                map_name=map_name,
                revision_id=map_revision_id,
                robot_id=self.robot_id,
            )
            if not target_asset:
                return self._empty_resp(success=False, message="map revision not found", response_cls=response_cls, map_cls=map_cls)
            candidate = self._gc_candidate_for_asset(target_asset, cascade=cascade)
            expected_confirm_token = (
                self._cascade_confirm_token(map_revision_id)
                if cascade
                else self._hard_delete_confirm_token(map_revision_id)
            )
            if candidate["blockers"]:
                return self._make_gc_response(
                    success=False,
                    message="hard delete blocked",
                    dry_run=dry_run,
                    candidates=[candidate],
                    confirm_token=expected_confirm_token if dry_run else "",
                    response_cls=response_cls,
                    map_cls=map_cls,
                )
            expected_tokens = {expected_confirm_token} if cascade else {map_revision_id, expected_confirm_token}
            if (not dry_run) and confirm_token not in expected_tokens:
                if cascade:
                    candidate["blockers"].append("confirm_token must equal %s" % expected_confirm_token)
                else:
                    candidate["blockers"].append("confirm_token must equal map_revision_id or DELETE:<map_revision_id>")
                return self._make_gc_response(
                    success=False,
                    message="hard delete confirmation required",
                    dry_run=dry_run,
                    candidates=[candidate],
                    confirm_token=expected_confirm_token,
                    response_cls=response_cls,
                    map_cls=map_cls,
                )
            if dry_run:
                return self._make_gc_response(
                    success=True,
                    message="dry-run cascade hard delete candidate" if cascade else "dry-run hard delete candidate",
                    dry_run=True,
                    candidates=[candidate],
                    confirm_token=expected_confirm_token,
                    response_cls=response_cls,
                    map_cls=map_cls,
                )
            try:
                deleted_business_refs = {}
                if cascade:
                    self._validate_hard_delete_candidate_files(candidate)
                    deleted_business_refs = self._delete_cascade_business_refs(candidate)
                deleted_paths, reclaimed_bytes = self._hard_delete_candidate(candidate)
                return self._make_gc_response(
                    success=True,
                    message="cascade hard-deleted map revision" if cascade else "hard-deleted map revision",
                    dry_run=False,
                    candidates=[candidate],
                    deleted_paths=deleted_paths,
                    reclaimed_bytes=reclaimed_bytes,
                    deleted_business_refs=deleted_business_refs,
                    response_cls=response_cls,
                    map_cls=map_cls,
                )
            except Exception as exc:
                candidate["blockers"].append(str(exc))
                return self._make_gc_response(
                    success=False,
                    message="hard delete failed: %s" % str(exc),
                    dry_run=False,
                    candidates=[candidate],
                    deleted_business_refs=candidate.get("deleted_business_refs") or {},
                    response_cls=response_cls,
                    map_cls=map_cls,
                )

        if op == cleanup_disabled_op:
            dry_run = self._request_bool(req, "dry_run", True)
            confirm_token = self._request_text(req, "confirm_token", "")
            min_age_days = self._request_uint(req, "min_age_days", 0)
            max_reclaim_bytes = self._request_uint(req, "max_reclaim_bytes", 0)
            now = time.time()
            runtime_revision_id = self._runtime_map_revision_id()
            candidates = []
            selected_bytes = 0
            for asset in self.store.list_map_revisions(map_name=map_name):
                if bool(asset.get("enabled", True)):
                    continue
                updated_ts = float(asset.get("updated_ts") or asset.get("created_ts") or 0.0)
                if min_age_days and updated_ts and (now - updated_ts) < (float(min_age_days) * 86400.0):
                    continue
                candidate = self._gc_candidate_for_asset(asset, runtime_revision_id=runtime_revision_id)
                if max_reclaim_bytes and (not candidate["blockers"]):
                    candidate_size = int(candidate.get("reclaimable_bytes") or 0)
                    if selected_bytes + candidate_size > max_reclaim_bytes:
                        continue
                    selected_bytes += candidate_size
                candidates.append(candidate)
            if dry_run:
                return self._make_gc_response(
                    success=True,
                    message="dry-run cleanup disabled map revisions",
                    dry_run=True,
                    candidates=candidates,
                    response_cls=response_cls,
                    map_cls=map_cls,
                )
            if confirm_token != "CLEANUP_DISABLED":
                for candidate in candidates:
                    if not candidate["blockers"]:
                        candidate["blockers"].append("confirm_token must be CLEANUP_DISABLED")
                return self._make_gc_response(
                    success=False,
                    message="cleanup confirmation required",
                    dry_run=False,
                    candidates=candidates,
                    response_cls=response_cls,
                    map_cls=map_cls,
                )
            deleted_paths = []
            reclaimed_bytes = 0
            for candidate in candidates:
                if candidate["blockers"]:
                    continue
                try:
                    paths, size = self._hard_delete_candidate(candidate)
                    deleted_paths.extend(paths)
                    reclaimed_bytes += int(size or 0)
                except Exception as exc:
                    candidate["blockers"].append(str(exc))
            blocked = [candidate for candidate in candidates if candidate["blockers"]]
            return self._make_gc_response(
                success=not bool(blocked),
                message="cleanup disabled map revisions completed" if not blocked else "cleanup completed with blocked revisions",
                dry_run=False,
                candidates=candidates,
                deleted_paths=deleted_paths,
                reclaimed_bytes=reclaimed_bytes,
                response_cls=response_cls,
                map_cls=map_cls,
            )

        return self._empty_resp(success=False, message="unsupported operation=%s" % op, response_cls=response_cls, map_cls=map_cls)

    def _handle_app(self, req):
        return self._handle(req, response_cls=AppOperateMapResponse, map_cls=AppPgmData)


def main():
    rospy.init_node("map_asset_service", anonymous=False)
    MapAssetServiceNode()
    rospy.spin()


if __name__ == "__main__":
    main()

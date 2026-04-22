# -*- coding: utf-8 -*-
import json
import os
import sqlite3
import time
import uuid
import hashlib
from typing import Any, Dict, List, Optional, Tuple

from .codec import (
    decode_path_xyyaw_f32,
    encode_path_xyyaw_f32,
    polyline_length_xy,
    xy_to_xyyaw,
)
from coverage_planner.constraints import DEFAULT_VIRTUAL_WALL_BUFFER_M
from coverage_planner.map_asset_status import map_asset_verification_error


def _now_ts() -> float:
    return float(time.time())


def _json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def _json_loads(text: str, default: Any):
    try:
        if not text:
            return default
        return json.loads(text)
    except Exception:
        return default


def _normalize_map_name(map_name: str) -> str:
    value = str(map_name or "").strip()
    if value.endswith(".pbstream"):
        value = value[:-len(".pbstream")]
    return str(value or "").strip()


def _map_id_from_md5(map_md5: str, prefix: str = "map_") -> str:
    value = str(map_md5 or "").strip()
    if not value:
        return ""
    return str(prefix or "map_") + value[:8]


def _artifact_sha256_for_paths(*paths: str) -> str:
    digest = hashlib.sha256()
    has_content = False
    for raw_path in paths:
        path = os.path.expanduser(str(raw_path or "").strip())
        if not path or (not os.path.isfile(path)):
            continue
        has_content = True
        digest.update(path.encode("utf-8"))
        with open(path, "rb") as fh:
            while True:
                chunk = fh.read(1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
    return digest.hexdigest() if has_content else ""


def _quarantine_sqlite_wal_files(db_path: str) -> bool:
    moved = False
    stamp = time.strftime("%Y%m%d_%H%M%S", time.localtime())
    for suffix in ("-wal", "-shm"):
        src = str(db_path or "") + suffix
        if not src or (not os.path.exists(src)):
            continue
        dst = src + ".stale." + stamp
        idx = 1
        while os.path.exists(dst):
            idx += 1
            dst = src + ".stale.%s.%d" % (stamp, idx)
        os.replace(src, dst)
        moved = True
    return moved


def _connect_sqlite_robust(db_path: str) -> sqlite3.Connection:
    last_exc: Optional[Exception] = None
    for attempt in range(2):
        conn = sqlite3.connect(db_path, timeout=30.0, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("PRAGMA foreign_keys=ON;")
            return conn
        except sqlite3.OperationalError as e:
            last_exc = e
            try:
                conn.close()
            except Exception:
                pass
            if (attempt == 0) and ("disk i/o error" in str(e).lower()) and _quarantine_sqlite_wal_files(db_path):
                continue
            raise
    raise last_exc if last_exc is not None else RuntimeError("failed to open sqlite database")

class PlanStore:
    """SQLite store for managed map assets, zones, plans and map constraints."""

    _SCHEMA_UPGRADE_TABLES = (
        "zones",
        "zone_versions",
        "zone_active_plans",
        "plans",
        "plan_blocks",
        "plan_profiles",
        "map_assets",
        "map_asset_versions",
        "robot_active_map",
        "map_zones",
        "map_zone_versions",
        "map_zone_active_plans",
        "map_constraint_versions",
        "map_no_go_areas",
        "map_virtual_walls",
        "map_active_constraint_versions",
    )

    def __init__(self, db_path: str):
        self.db_path = os.path.expanduser(str(db_path))
        if os.path.dirname(self.db_path):
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self.conn = _connect_sqlite_robust(self.db_path)
        self._init_schema()

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass

    def _table_exists(self, table_name: str) -> bool:
        row = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=? LIMIT 1;",
            (str(table_name or "").strip(),),
        ).fetchone()
        return bool(row)

    def _table_columns(self, table_name: str) -> List[str]:
        if not self._table_exists(table_name):
            return []
        rows = self.conn.execute("PRAGMA table_info(%s);" % str(table_name)).fetchall()
        return [str(r["name"]) for r in (rows or []) if r and r["name"]]

    def _ensure_table_column(self, table_name: str, column_name: str, ddl_fragment: str):
        if not self._table_exists(table_name):
            return
        if column_name in self._table_columns(table_name):
            return
        self.conn.execute(
            "ALTER TABLE %s ADD COLUMN %s %s;" % (
                str(table_name),
                str(column_name),
                str(ddl_fragment),
            )
        )

    def _ensure_map_asset_runtime_columns(self):
        self._ensure_table_column(
            "map_assets",
            "lifecycle_status",
            "TEXT NOT NULL DEFAULT 'available'",
        )
        self._ensure_table_column(
            "map_assets",
            "verification_status",
            "TEXT NOT NULL DEFAULT 'verified'",
        )
        self._ensure_table_column(
            "map_assets",
            "save_snapshot_md5",
            "TEXT",
        )
        self._ensure_table_column(
            "map_assets",
            "verified_runtime_map_id",
            "TEXT",
        )
        self._ensure_table_column(
            "map_assets",
            "verified_runtime_map_md5",
            "TEXT",
        )
        self._ensure_table_column(
            "map_assets",
            "last_error_code",
            "TEXT",
        )
        self._ensure_table_column(
            "map_assets",
            "last_error_msg",
            "TEXT",
        )
        self._ensure_table_column(
            "map_assets",
            "source_job_id",
            "TEXT",
        )
        self._ensure_table_column(
            "map_assets",
            "current_revision_id",
            "TEXT",
        )
        self.conn.execute(
            """
            UPDATE map_assets
            SET lifecycle_status=COALESCE(NULLIF(lifecycle_status, ''), 'available'),
                verification_status=COALESCE(NULLIF(verification_status, ''), 'verified')
            WHERE 1=1;
            """
        )

    def _ensure_pending_map_switch_table(self):
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS robot_pending_map_switch (
              robot_id TEXT PRIMARY KEY,
              from_map_name TEXT,
              target_map_name TEXT NOT NULL,
              requested_activate INTEGER NOT NULL DEFAULT 1,
              status TEXT NOT NULL DEFAULT 'requested',
              last_error_code TEXT,
              last_error_msg TEXT,
              updated_ts REAL NOT NULL
            );
            """
        )

    def _ensure_revision_tables(self):
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS map_revisions (
              revision_id TEXT PRIMARY KEY,
              map_name TEXT NOT NULL,
              display_name TEXT,
              enabled INTEGER NOT NULL DEFAULT 1,
              description TEXT,
              map_id TEXT,
              map_md5 TEXT,
              yaml_path TEXT NOT NULL,
              pgm_path TEXT NOT NULL,
              pbstream_path TEXT,
              frame_id TEXT,
              resolution REAL,
              origin_json TEXT,
              lifecycle_status TEXT NOT NULL DEFAULT 'available',
              verification_status TEXT NOT NULL DEFAULT 'verified',
              artifact_sha256 TEXT,
              live_snapshot_md5 TEXT,
              verified_runtime_map_id TEXT,
              verified_runtime_map_md5 TEXT,
              last_error_code TEXT,
              last_error_msg TEXT,
              source_job_id TEXT,
              created_ts REAL NOT NULL,
              verified_ts REAL,
              activated_ts REAL,
              updated_ts REAL NOT NULL
            );
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS robot_active_map_revision (
              robot_id TEXT PRIMARY KEY,
              active_revision_id TEXT NOT NULL,
              updated_ts REAL NOT NULL
            );
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS robot_pending_map_revision (
              robot_id TEXT PRIMARY KEY,
              from_revision_id TEXT,
              target_revision_id TEXT NOT NULL,
              requested_activate INTEGER NOT NULL DEFAULT 1,
              status TEXT NOT NULL DEFAULT 'requested',
              last_error_code TEXT,
              last_error_msg TEXT,
              updated_ts REAL NOT NULL
            );
            """
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_map_revisions_scope ON map_revisions(map_name, created_ts DESC);"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_map_revisions_identity ON map_revisions(map_id, map_md5, updated_ts);"
        )

    def _generate_revision_id(self, map_name: str) -> str:
        normalized = _normalize_map_name(map_name) or "map"
        safe_name = "".join(ch if ch.isalnum() else "_" for ch in normalized).strip("_") or "map"
        return "rev_%s_%s" % (safe_name[:40], uuid.uuid4().hex[:12])

    def generate_map_revision_id(self, map_name: str) -> str:
        normalized = _normalize_map_name(map_name)
        if not normalized:
            raise ValueError("map_name is required")
        return self._generate_revision_id(normalized)

    def _backfill_map_revision_tables(self):
        if (not self._table_exists("map_assets")) or (not self._table_exists("map_revisions")):
            return
        rows = self._fetch_rows("SELECT * FROM map_assets ORDER BY map_name ASC;")
        for row in rows or []:
            map_name = _normalize_map_name(str(row["map_name"] or ""))
            if not map_name:
                continue
            revision_id = str(row["current_revision_id"] or "").strip() if "current_revision_id" in row.keys() else ""
            if not revision_id:
                revision_id = self._generate_revision_id(map_name)
                self.conn.execute(
                    "UPDATE map_assets SET current_revision_id=?, updated_ts=? WHERE map_name=?;",
                    (revision_id, _now_ts(), map_name),
                )
            self.upsert_map_revision(
                revision_id=revision_id,
                map_name=map_name,
                display_name=str(row["display_name"] or map_name),
                enabled=bool(int(row["enabled"] or 0)),
                description=str(row["description"] or ""),
                map_id=str(row["map_id"] or ""),
                map_md5=str(row["map_md5"] or ""),
                yaml_path=str(row["yaml_path"] or ""),
                pgm_path=str(row["pgm_path"] or ""),
                pbstream_path=str(row["pbstream_path"] or ""),
                frame_id=str(row["frame_id"] or "map"),
                resolution=float(row["resolution"] or 0.0),
                origin=_json_loads(str(row["origin_json"] or ""), [0.0, 0.0, 0.0]),
                lifecycle_status=str(row["lifecycle_status"] or "available"),
                verification_status=str(row["verification_status"] or "verified"),
                live_snapshot_md5=str(row["save_snapshot_md5"] or ""),
                verified_runtime_map_id=str(row["verified_runtime_map_id"] or ""),
                verified_runtime_map_md5=str(row["verified_runtime_map_md5"] or ""),
                last_error_code=str(row["last_error_code"] or ""),
                last_error_msg=str(row["last_error_msg"] or ""),
                source_job_id=str(row["source_job_id"] or ""),
                created_ts=float(row["created_ts"] or _now_ts()),
                updated_ts=float(row["updated_ts"] or _now_ts()),
            )
        for active in self._read_pre_upgrade_active_maps():
            asset = self.resolve_map_asset(
                map_name=str(active.get("map_name") or ""),
                robot_id=str(active.get("robot_id") or "local_robot"),
            ) or {}
            revision_id = str(asset.get("revision_id") or "").strip()
            if not revision_id:
                continue
            self.conn.execute(
                """
                INSERT INTO robot_active_map_revision(robot_id, active_revision_id, updated_ts)
                VALUES(?,?,?)
                ON CONFLICT(robot_id) DO UPDATE SET
                  active_revision_id=excluded.active_revision_id,
                  updated_ts=excluded.updated_ts;
                """,
                (
                    str(active.get("robot_id") or "local_robot").strip() or "local_robot",
                    revision_id,
                    float(active.get("updated_ts") or _now_ts()),
                ),
            )
        pending = self._fetch_rows("SELECT * FROM robot_pending_map_switch;") if self._table_exists("robot_pending_map_switch") else []
        for row in pending or []:
            robot_id = str(row["robot_id"] or "local_robot").strip() or "local_robot"
            if self.conn.execute(
                "SELECT 1 FROM robot_pending_map_revision WHERE robot_id=? LIMIT 1;",
                (robot_id,),
            ).fetchone():
                continue
            from_asset = self.resolve_map_asset(
                map_name=str(row["from_map_name"] or ""),
                robot_id=robot_id,
            ) or {}
            target_asset = self.resolve_map_asset(
                map_name=str(row["target_map_name"] or ""),
                robot_id=robot_id,
            ) or {}
            target_revision_id = str(target_asset.get("revision_id") or "").strip()
            if not target_revision_id:
                continue
            self.upsert_pending_map_revision(
                robot_id=robot_id,
                from_revision_id=str(from_asset.get("revision_id") or ""),
                target_revision_id=target_revision_id,
                requested_activate=bool(int(row["requested_activate"] or 0)),
                status=str(row["status"] or "requested"),
                last_error_code=str(row["last_error_code"] or ""),
                last_error_msg=str(row["last_error_msg"] or ""),
            )
        self.conn.commit()

    def _schema_upgrade_reasons(self) -> List[str]:
        reasons: List[str] = []
        map_asset_cols = set(self._table_columns("map_assets"))
        if "map_id" not in map_asset_cols and self._table_exists("map_assets"):
            reasons.append("map_assets missing map_id")
        if "map_version" in self._table_columns("plans"):
            reasons.append("plans still expose map_version")
        if "map_version" in self._table_columns("robot_active_map"):
            reasons.append("robot_active_map still exposes map_version")
        if self._table_exists("map_asset_versions"):
            reasons.append("map_asset_versions table still exists")
        if self._table_exists("map_zones"):
            reasons.append("map_zones table still exists")
        return reasons

    def _create_zone_plan_storage_tables(self, cur: sqlite3.Cursor) -> None:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS zones (
              map_revision_id TEXT NOT NULL,
              map_name TEXT NOT NULL,
              zone_id TEXT NOT NULL,
              display_name TEXT,
              enabled INTEGER NOT NULL DEFAULT 1,
              current_zone_version INTEGER NOT NULL DEFAULT 0,
              updated_ts REAL NOT NULL,
              PRIMARY KEY(map_revision_id, zone_id)
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS zone_versions (
              map_revision_id TEXT NOT NULL,
              map_name TEXT NOT NULL,
              zone_id TEXT NOT NULL,
              zone_version INTEGER NOT NULL,
              frame_id TEXT,
              outer_json TEXT,
              holes_json TEXT,
              map_id TEXT,
              map_md5 TEXT,
              created_ts REAL NOT NULL,
              PRIMARY KEY(map_revision_id, zone_id, zone_version)
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS plans (
              plan_id TEXT PRIMARY KEY,
              map_revision_id TEXT NOT NULL,
              map_name TEXT NOT NULL,
              zone_id TEXT NOT NULL,
              zone_version INTEGER NOT NULL,
              frame_id TEXT,
              plan_profile_name TEXT,
              params_json TEXT,
              robot_json TEXT,
              blocks INTEGER NOT NULL,
              total_length_m REAL NOT NULL,
              exec_order_json TEXT,
              map_id TEXT,
              map_md5 TEXT,
              constraint_version TEXT,
              planner_version TEXT,
              created_ts REAL NOT NULL
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS plan_blocks (
              plan_id TEXT NOT NULL,
              block_id INTEGER NOT NULL,
              entry_x REAL,
              entry_y REAL,
              entry_yaw REAL,
              exit_x REAL,
              exit_y REAL,
              exit_yaw REAL,
              point_count INTEGER NOT NULL,
              length_m REAL NOT NULL,
              path_step_m REAL,
              path_blob BLOB,
              PRIMARY KEY(plan_id, block_id),
              FOREIGN KEY(plan_id) REFERENCES plans(plan_id) ON DELETE CASCADE
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS zone_active_plans (
              map_revision_id TEXT NOT NULL,
              map_name TEXT NOT NULL,
              zone_id TEXT NOT NULL,
              plan_profile_name TEXT NOT NULL,
              active_plan_id TEXT NOT NULL,
              updated_ts REAL NOT NULL,
              PRIMARY KEY(map_revision_id, zone_id, plan_profile_name)
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS zone_editor_metadata (
              map_revision_id TEXT NOT NULL,
              map_name TEXT NOT NULL,
              zone_id TEXT NOT NULL,
              zone_version INTEGER NOT NULL,
              alignment_version TEXT,
              display_frame TEXT,
              display_outer_json TEXT,
              display_holes_json TEXT,
              profile_name TEXT,
              preview_plan_id TEXT,
              estimated_length_m REAL NOT NULL DEFAULT 0.0,
              estimated_duration_s REAL NOT NULL DEFAULT 0.0,
              warnings_json TEXT,
              updated_ts REAL NOT NULL,
              PRIMARY KEY(map_revision_id, zone_id, zone_version)
            );
            """
        )

    def _create_zone_plan_storage_indexes(self, cur: sqlite3.Cursor) -> None:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_zones_scope ON zones(map_revision_id, updated_ts);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_zones_scope_name ON zones(map_name, zone_id, updated_ts);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_zone_versions_scope ON zone_versions(map_revision_id, zone_id, zone_version);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_zone_versions_scope_name ON zone_versions(map_name, zone_id, zone_version);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_plans_scope ON plans(map_revision_id, zone_id, created_ts);")
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_plans_scope_profile ON plans(map_revision_id, zone_id, plan_profile_name, created_ts);"
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_plans_scope_name ON plans(map_name, zone_id, created_ts);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_plan_blocks_plan ON plan_blocks(plan_id);")
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_zone_active_plans_scope ON zone_active_plans(map_revision_id, zone_id, updated_ts);"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_zone_active_plans_scope_name ON zone_active_plans(map_name, zone_id, updated_ts);"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_zone_editor_meta_scope ON zone_editor_metadata(map_revision_id, zone_id, updated_ts);"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_zone_editor_meta_scope_name ON zone_editor_metadata(map_name, zone_id, updated_ts);"
        )

    def _map_revision_lookup_by_name(self) -> Dict[str, str]:
        lookup: Dict[str, str] = {}
        if not self._table_exists("map_assets"):
            return lookup
        rows = self._fetch_rows("SELECT map_name, current_revision_id FROM map_assets ORDER BY map_name ASC;")
        for row in rows or []:
            map_name = _normalize_map_name(str(row["map_name"] or ""))
            if not map_name:
                continue
            revision_id = str(row["current_revision_id"] or "").strip()
            if (not revision_id) and self._table_exists("map_revisions"):
                revision = self.resolve_map_revision(map_name=map_name) or {}
                revision_id = str(revision.get("revision_id") or "").strip()
            if revision_id:
                lookup[map_name] = revision_id
        return lookup

    def _resolve_map_scope(
        self,
        *,
        map_name: str = "",
        map_revision_id: str = "",
        robot_id: str = "local_robot",
    ) -> Tuple[str, str]:
        resolved_revision_id = str(map_revision_id or "").strip()
        resolved_map_name = _normalize_map_name(map_name)
        if resolved_revision_id:
            revision = self.resolve_map_revision(revision_id=resolved_revision_id, robot_id=robot_id) or {}
            revision_map_name = _normalize_map_name(str(revision.get("map_name") or ""))
            return revision_map_name or resolved_map_name, resolved_revision_id
        if resolved_map_name:
            asset = self._preferred_map_scope_asset(
                map_name=resolved_map_name,
                robot_id=robot_id,
            ) or {}
            revision_id = str(asset.get("revision_id") or "").strip()
            if (not revision_id) and asset:
                revision_id = self.ensure_map_asset_revision(map_name=resolved_map_name, robot_id=robot_id)
            return resolved_map_name, revision_id
        active_revision = self.get_active_map_revision(robot_id=robot_id) or {}
        if active_revision:
            return (
                _normalize_map_name(str(active_revision.get("map_name") or "")),
                str(active_revision.get("revision_id") or "").strip(),
            )
        active_asset = self.get_active_map(robot_id=robot_id) or {}
        return (
            _normalize_map_name(str(active_asset.get("map_name") or "")),
            str(active_asset.get("revision_id") or "").strip(),
        )

    def _ensure_zone_plan_revision_scope(self) -> None:
        required_columns = {
            "zones": {"map_revision_id"},
            "zone_versions": {"map_revision_id"},
            "plans": {"map_revision_id"},
            "zone_active_plans": {"map_revision_id"},
            "zone_editor_metadata": {"map_revision_id"},
        }
        needs_migration = False
        for table_name, required in required_columns.items():
            cols = set(self._table_columns(table_name))
            if not required.issubset(cols):
                needs_migration = True
                break
        if not needs_migration:
            return

        zones = [dict(r) for r in self._fetch_rows("SELECT * FROM zones ORDER BY map_name ASC, zone_id ASC;")] if self._table_exists("zones") else []
        zone_versions = [dict(r) for r in self._fetch_rows("SELECT * FROM zone_versions ORDER BY map_name ASC, zone_id ASC, zone_version ASC;")] if self._table_exists("zone_versions") else []
        plans = [dict(r) for r in self._fetch_rows("SELECT * FROM plans ORDER BY created_ts ASC, plan_id ASC;")] if self._table_exists("plans") else []
        plan_blocks = [dict(r) for r in self._fetch_rows("SELECT * FROM plan_blocks ORDER BY plan_id ASC, block_id ASC;")] if self._table_exists("plan_blocks") else []
        active_plans = [dict(r) for r in self._fetch_rows("SELECT * FROM zone_active_plans ORDER BY map_name ASC, zone_id ASC, plan_profile_name ASC;")] if self._table_exists("zone_active_plans") else []
        zone_editor_rows = [dict(r) for r in self._fetch_rows("SELECT * FROM zone_editor_metadata ORDER BY map_name ASC, zone_id ASC, zone_version ASC;")] if self._table_exists("zone_editor_metadata") else []
        revision_lookup = self._map_revision_lookup_by_name()

        def revision_for_map_name(name: str) -> str:
            normalized = _normalize_map_name(name)
            if not normalized:
                return ""
            revision_id = str(revision_lookup.get(normalized) or "").strip()
            if revision_id:
                return revision_id
            asset = self.resolve_map_asset(map_name=normalized) or {}
            revision_id = str(asset.get("revision_id") or "").strip()
            if (not revision_id) and asset:
                revision_id = self.ensure_map_asset_revision(map_name=normalized)
            if revision_id:
                revision_lookup[normalized] = revision_id
            return revision_id

        cur = self.conn.cursor()
        migrated_plan_ids = set()
        try:
            cur.execute("BEGIN IMMEDIATE;")
            for table_name in ("plan_blocks", "zone_active_plans", "zone_editor_metadata", "plans", "zone_versions", "zones"):
                cur.execute("DROP TABLE IF EXISTS %s;" % str(table_name))
            self._create_zone_plan_storage_tables(cur)
            self._create_zone_plan_storage_indexes(cur)

            for row in zones:
                map_name = _normalize_map_name(str(row.get("map_name") or ""))
                revision_id = str(row.get("map_revision_id") or "").strip() or revision_for_map_name(map_name)
                if (not map_name) or (not revision_id):
                    continue
                cur.execute(
                    """
                    INSERT OR REPLACE INTO zones(
                      map_revision_id, map_name, zone_id, display_name, enabled, current_zone_version, updated_ts
                    ) VALUES(?,?,?,?,?,?,?);
                    """,
                    (
                        revision_id,
                        map_name,
                        str(row.get("zone_id") or ""),
                        str(row.get("display_name") or row.get("zone_id") or ""),
                        int(row.get("enabled", 1) or 0),
                        int(row.get("current_zone_version") or 0),
                        float(row.get("updated_ts") or _now_ts()),
                    ),
                )

            for row in zone_versions:
                map_name = _normalize_map_name(str(row.get("map_name") or ""))
                revision_id = str(row.get("map_revision_id") or "").strip() or revision_for_map_name(map_name)
                if (not map_name) or (not revision_id):
                    continue
                cur.execute(
                    """
                    INSERT OR REPLACE INTO zone_versions(
                      map_revision_id, map_name, zone_id, zone_version, frame_id, outer_json, holes_json, map_id, map_md5, created_ts
                    ) VALUES(?,?,?,?,?,?,?,?,?,?);
                    """,
                    (
                        revision_id,
                        map_name,
                        str(row.get("zone_id") or ""),
                        int(row.get("zone_version") or 0),
                        str(row.get("frame_id") or ""),
                        str(row.get("outer_json") or "[]"),
                        str(row.get("holes_json") or "[]"),
                        str(row.get("map_id") or ""),
                        str(row.get("map_md5") or ""),
                        float(row.get("created_ts") or _now_ts()),
                    ),
                )

            for row in plans:
                map_name = _normalize_map_name(str(row.get("map_name") or ""))
                revision_id = str(row.get("map_revision_id") or "").strip() or revision_for_map_name(map_name)
                if (not map_name) or (not revision_id):
                    continue
                cur.execute(
                    """
                    INSERT OR REPLACE INTO plans(
                      plan_id, map_revision_id, map_name, zone_id, zone_version, frame_id, plan_profile_name,
                      params_json, robot_json, blocks, total_length_m, exec_order_json,
                      map_id, map_md5, constraint_version, planner_version, created_ts
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
                    """,
                    (
                        str(row.get("plan_id") or ""),
                        revision_id,
                        map_name,
                        str(row.get("zone_id") or ""),
                        int(row.get("zone_version") or 0),
                        str(row.get("frame_id") or ""),
                        str(row.get("plan_profile_name") or ""),
                        str(row.get("params_json") or "{}"),
                        str(row.get("robot_json") or "{}"),
                        int(row.get("blocks") or 0),
                        float(row.get("total_length_m") or 0.0),
                        str(row.get("exec_order_json") or "[]"),
                        str(row.get("map_id") or ""),
                        str(row.get("map_md5") or ""),
                        str(row.get("constraint_version") or ""),
                        str(row.get("planner_version") or ""),
                        float(row.get("created_ts") or _now_ts()),
                    ),
                )
                migrated_plan_ids.add(str(row.get("plan_id") or ""))

            for row in plan_blocks:
                plan_id = str(row.get("plan_id") or "")
                if plan_id not in migrated_plan_ids:
                    continue
                cur.execute(
                    """
                    INSERT OR REPLACE INTO plan_blocks(
                      plan_id, block_id, entry_x, entry_y, entry_yaw, exit_x, exit_y, exit_yaw,
                      point_count, length_m, path_step_m, path_blob
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?);
                    """,
                    (
                        plan_id,
                        int(row.get("block_id") or 0),
                        row.get("entry_x"),
                        row.get("entry_y"),
                        row.get("entry_yaw"),
                        row.get("exit_x"),
                        row.get("exit_y"),
                        row.get("exit_yaw"),
                        int(row.get("point_count") or 0),
                        float(row.get("length_m") or 0.0),
                        row.get("path_step_m"),
                        row.get("path_blob"),
                    ),
                )

            for row in active_plans:
                map_name = _normalize_map_name(str(row.get("map_name") or ""))
                revision_id = str(row.get("map_revision_id") or "").strip() or revision_for_map_name(map_name)
                active_plan_id = str(row.get("active_plan_id") or "").strip()
                if (not map_name) or (not revision_id):
                    continue
                if active_plan_id and migrated_plan_ids and active_plan_id not in migrated_plan_ids:
                    continue
                cur.execute(
                    """
                    INSERT OR REPLACE INTO zone_active_plans(
                      map_revision_id, map_name, zone_id, plan_profile_name, active_plan_id, updated_ts
                    ) VALUES(?,?,?,?,?,?);
                    """,
                    (
                        revision_id,
                        map_name,
                        str(row.get("zone_id") or ""),
                        str(row.get("plan_profile_name") or ""),
                        active_plan_id,
                        float(row.get("updated_ts") or _now_ts()),
                    ),
                )

            for row in zone_editor_rows:
                map_name = _normalize_map_name(str(row.get("map_name") or ""))
                revision_id = str(row.get("map_revision_id") or "").strip() or revision_for_map_name(map_name)
                if (not map_name) or (not revision_id):
                    continue
                cur.execute(
                    """
                    INSERT OR REPLACE INTO zone_editor_metadata(
                      map_revision_id, map_name, zone_id, zone_version, alignment_version, display_frame, display_outer_json,
                      display_holes_json, profile_name, preview_plan_id, estimated_length_m, estimated_duration_s,
                      warnings_json, updated_ts
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?);
                    """,
                    (
                        revision_id,
                        map_name,
                        str(row.get("zone_id") or ""),
                        int(row.get("zone_version") or 0),
                        str(row.get("alignment_version") or ""),
                        str(row.get("display_frame") or ""),
                        str(row.get("display_outer_json") or "[]"),
                        str(row.get("display_holes_json") or "[]"),
                        str(row.get("profile_name") or ""),
                        str(row.get("preview_plan_id") or ""),
                        float(row.get("estimated_length_m") or 0.0),
                        float(row.get("estimated_duration_s") or 0.0),
                        str(row.get("warnings_json") or "[]"),
                        float(row.get("updated_ts") or _now_ts()),
                    ),
                )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def _init_schema(self):
        upgrade_reasons = self._schema_upgrade_reasons()
        if upgrade_reasons:
            self._upgrade_pre_scoped_map_schema(upgrade_reasons)
        self._create_schema()
        self._seed_plan_profiles()
        self._record_schema_meta(upgrade_reasons=upgrade_reasons)

    def _create_schema(self):
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_meta (
              key TEXT PRIMARY KEY,
              value TEXT NOT NULL,
              updated_ts REAL NOT NULL
            );
            """
        )
        self._create_zone_plan_storage_tables(cur)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS plan_profiles (
              plan_profile_name TEXT PRIMARY KEY,
              description TEXT,
              enabled INTEGER NOT NULL DEFAULT 1,
              usage_type TEXT,
              updated_ts REAL NOT NULL
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS map_assets (
              map_name TEXT PRIMARY KEY,
              display_name TEXT,
              enabled INTEGER NOT NULL DEFAULT 1,
              description TEXT,
              map_id TEXT,
              map_md5 TEXT,
              yaml_path TEXT NOT NULL,
              pgm_path TEXT NOT NULL,
              pbstream_path TEXT,
              frame_id TEXT,
              resolution REAL,
              origin_json TEXT,
              lifecycle_status TEXT NOT NULL DEFAULT 'available',
              verification_status TEXT NOT NULL DEFAULT 'verified',
              save_snapshot_md5 TEXT,
              verified_runtime_map_id TEXT,
              verified_runtime_map_md5 TEXT,
              last_error_code TEXT,
              last_error_msg TEXT,
              source_job_id TEXT,
              created_ts REAL NOT NULL,
              updated_ts REAL NOT NULL
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS robot_active_map (
              robot_id TEXT PRIMARY KEY,
              map_name TEXT NOT NULL,
              updated_ts REAL NOT NULL
            );
            """
        )
        self._ensure_map_asset_runtime_columns()
        self._ensure_pending_map_switch_table()
        self._ensure_revision_tables()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS map_constraint_versions (
              map_id TEXT NOT NULL,
              constraint_version TEXT NOT NULL,
              map_md5 TEXT,
              created_ts REAL NOT NULL,
              updated_ts REAL NOT NULL,
              PRIMARY KEY(map_id, constraint_version)
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS map_no_go_areas (
              map_id TEXT NOT NULL,
              constraint_version TEXT NOT NULL,
              area_id TEXT NOT NULL,
              name TEXT,
              polygon_json TEXT NOT NULL,
              enabled INTEGER NOT NULL DEFAULT 1,
              updated_ts REAL NOT NULL,
              PRIMARY KEY(map_id, constraint_version, area_id)
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS map_virtual_walls (
              map_id TEXT NOT NULL,
              constraint_version TEXT NOT NULL,
              wall_id TEXT NOT NULL,
              name TEXT,
              polyline_json TEXT NOT NULL,
              buffer_m REAL NOT NULL,
              enabled INTEGER NOT NULL DEFAULT 1,
              updated_ts REAL NOT NULL,
              PRIMARY KEY(map_id, constraint_version, wall_id)
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS map_active_constraint_versions (
              map_id TEXT PRIMARY KEY,
              active_constraint_version TEXT NOT NULL,
              updated_ts REAL NOT NULL
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS map_alignment_configs (
              map_name TEXT NOT NULL,
              map_id TEXT,
              map_version TEXT,
              alignment_version TEXT NOT NULL,
              raw_frame TEXT NOT NULL,
              aligned_frame TEXT NOT NULL,
              yaw_offset_deg REAL NOT NULL,
              pivot_x REAL NOT NULL DEFAULT 0.0,
              pivot_y REAL NOT NULL DEFAULT 0.0,
              source TEXT,
              status TEXT NOT NULL DEFAULT 'draft',
              created_ts REAL NOT NULL,
              updated_ts REAL NOT NULL,
              PRIMARY KEY(map_name, alignment_version)
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS map_active_alignments (
              map_name TEXT PRIMARY KEY,
              active_alignment_version TEXT NOT NULL,
              updated_ts REAL NOT NULL
            );
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_map_assets_identity ON map_assets(map_id, map_md5, updated_ts);")
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_constraint_versions_map ON map_constraint_versions(map_id, updated_ts);"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_no_go_areas_map ON map_no_go_areas(map_id, constraint_version, updated_ts);"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_virtual_walls_map ON map_virtual_walls(map_id, constraint_version, updated_ts);"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_map_alignments_scope ON map_alignment_configs(map_name, map_version, updated_ts);"
        )
        self.conn.commit()
        self._backfill_map_revision_tables()
        self._ensure_zone_plan_revision_scope()
        # Legacy field deployments can still carry pre-revision zone/plan tables.
        # Build revision-scoped indexes only after the migration has normalized
        # those tables, otherwise sqlite will fail on missing map_revision_id.
        cur = self.conn.cursor()
        self._create_zone_plan_storage_indexes(cur)
        self.conn.commit()
        self._ensure_revision_scoped_site_editor_tables()

    def _ensure_revision_scoped_site_editor_tables(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS map_constraint_revision_versions (
              map_revision_id TEXT NOT NULL,
              map_id TEXT,
              constraint_version TEXT NOT NULL,
              map_md5 TEXT,
              created_ts REAL NOT NULL,
              updated_ts REAL NOT NULL,
              PRIMARY KEY(map_revision_id, constraint_version)
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS map_revision_no_go_areas (
              map_revision_id TEXT NOT NULL,
              constraint_version TEXT NOT NULL,
              area_id TEXT NOT NULL,
              name TEXT,
              polygon_json TEXT NOT NULL,
              enabled INTEGER NOT NULL DEFAULT 1,
              updated_ts REAL NOT NULL,
              PRIMARY KEY(map_revision_id, constraint_version, area_id)
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS map_revision_virtual_walls (
              map_revision_id TEXT NOT NULL,
              constraint_version TEXT NOT NULL,
              wall_id TEXT NOT NULL,
              name TEXT,
              polyline_json TEXT NOT NULL,
              buffer_m REAL NOT NULL,
              enabled INTEGER NOT NULL DEFAULT 1,
              updated_ts REAL NOT NULL,
              PRIMARY KEY(map_revision_id, constraint_version, wall_id)
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS map_active_constraint_revisions (
              map_revision_id TEXT PRIMARY KEY,
              active_constraint_version TEXT NOT NULL,
              updated_ts REAL NOT NULL
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS map_alignment_revision_configs (
              map_revision_id TEXT NOT NULL,
              map_name TEXT NOT NULL,
              map_id TEXT,
              map_version TEXT,
              alignment_version TEXT NOT NULL,
              raw_frame TEXT NOT NULL,
              aligned_frame TEXT NOT NULL,
              yaw_offset_deg REAL NOT NULL,
              pivot_x REAL NOT NULL DEFAULT 0.0,
              pivot_y REAL NOT NULL DEFAULT 0.0,
              source TEXT,
              status TEXT NOT NULL DEFAULT 'draft',
              created_ts REAL NOT NULL,
              updated_ts REAL NOT NULL,
              PRIMARY KEY(map_revision_id, alignment_version)
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS map_active_alignment_revisions (
              map_revision_id TEXT PRIMARY KEY,
              active_alignment_version TEXT NOT NULL,
              updated_ts REAL NOT NULL
            );
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_constraint_revision_versions_scope ON map_constraint_revision_versions(map_revision_id, updated_ts);"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_revision_no_go_areas_scope ON map_revision_no_go_areas(map_revision_id, constraint_version, updated_ts);"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_revision_virtual_walls_scope ON map_revision_virtual_walls(map_revision_id, constraint_version, updated_ts);"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_alignment_revision_configs_scope ON map_alignment_revision_configs(map_revision_id, map_version, updated_ts);"
        )
        self.conn.commit()
        self._backfill_revision_scoped_site_editor_tables()

    def _revision_ids_for_map_name(self, map_name: str) -> List[str]:
        normalized = _normalize_map_name(map_name)
        if not normalized:
            return []
        rows = self._fetch_rows(
            "SELECT revision_id FROM map_revisions WHERE map_name=? ORDER BY updated_ts DESC, created_ts DESC, revision_id DESC;",
            (normalized,),
        )
        return [str(row["revision_id"] or "").strip() for row in (rows or []) if str(row["revision_id"] or "").strip()]

    def _revision_ids_for_map_identity(self, *, map_id: str = "", map_md5: str = "") -> List[str]:
        normalized_map_id = str(map_id or "").strip()
        normalized_map_md5 = str(map_md5 or "").strip()
        if normalized_map_id:
            rows = self._fetch_rows(
                "SELECT revision_id FROM map_revisions WHERE map_id=? ORDER BY updated_ts DESC, created_ts DESC, revision_id DESC;",
                (normalized_map_id,),
            )
            values = [str(row["revision_id"] or "").strip() for row in (rows or []) if str(row["revision_id"] or "").strip()]
            if values:
                return values
        if normalized_map_md5:
            rows = self._fetch_rows(
                "SELECT revision_id FROM map_revisions WHERE map_md5=? ORDER BY updated_ts DESC, created_ts DESC, revision_id DESC;",
                (normalized_map_md5,),
            )
            return [str(row["revision_id"] or "").strip() for row in (rows or []) if str(row["revision_id"] or "").strip()]
        return []

    def _backfill_revision_scoped_site_editor_tables(self) -> None:
        if not self._table_exists("map_revisions"):
            return
        cur = self.conn.cursor()
        try:
            cur.execute("BEGIN IMMEDIATE;")
            if self._table_exists("map_alignment_configs"):
                alignment_rows = self._fetch_rows("SELECT * FROM map_alignment_configs ORDER BY map_name ASC, updated_ts ASC;")
                for row in alignment_rows or []:
                    revision_ids = self._revision_ids_for_map_name(str(row["map_name"] or ""))
                    for revision_id in revision_ids:
                        cur.execute(
                            """
                            INSERT OR IGNORE INTO map_alignment_revision_configs(
                              map_revision_id, map_name, map_id, map_version, alignment_version, raw_frame, aligned_frame,
                              yaw_offset_deg, pivot_x, pivot_y, source, status, created_ts, updated_ts
                            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?);
                            """,
                            (
                                revision_id,
                                _normalize_map_name(str(row["map_name"] or "")),
                                str(row["map_id"] or ""),
                                str(row["map_version"] or ""),
                                str(row["alignment_version"] or ""),
                                str(row["raw_frame"] or "map"),
                                str(row["aligned_frame"] or "site_map"),
                                float(row["yaw_offset_deg"] or 0.0),
                                float(row["pivot_x"] or 0.0),
                                float(row["pivot_y"] or 0.0),
                                str(row["source"] or ""),
                                str(row["status"] or "draft"),
                                float(row["created_ts"] or _now_ts()),
                                float(row["updated_ts"] or _now_ts()),
                            ),
                        )
            if self._table_exists("map_active_alignments"):
                active_rows = self._fetch_rows("SELECT * FROM map_active_alignments;")
                for row in active_rows or []:
                    revision_ids = self._revision_ids_for_map_name(str(row["map_name"] or ""))
                    for revision_id in revision_ids:
                        cur.execute(
                            """
                            INSERT OR IGNORE INTO map_active_alignment_revisions(map_revision_id, active_alignment_version, updated_ts)
                            VALUES(?,?,?);
                            """,
                            (
                                revision_id,
                                str(row["active_alignment_version"] or ""),
                                float(row["updated_ts"] or _now_ts()),
                            ),
                        )
            if self._table_exists("map_constraint_versions"):
                version_rows = self._fetch_rows("SELECT * FROM map_constraint_versions ORDER BY created_ts ASC, updated_ts ASC;")
                for row in version_rows or []:
                    revision_ids = self._revision_ids_for_map_identity(
                        map_id=str(row["map_id"] or ""),
                        map_md5=str(row["map_md5"] or ""),
                    )
                    for revision_id in revision_ids:
                        cur.execute(
                            """
                            INSERT OR IGNORE INTO map_constraint_revision_versions(
                              map_revision_id, map_id, constraint_version, map_md5, created_ts, updated_ts
                            ) VALUES(?,?,?,?,?,?);
                            """,
                            (
                                revision_id,
                                str(row["map_id"] or ""),
                                str(row["constraint_version"] or ""),
                                str(row["map_md5"] or ""),
                                float(row["created_ts"] or _now_ts()),
                                float(row["updated_ts"] or _now_ts()),
                            ),
                        )
            if self._table_exists("map_no_go_areas"):
                area_rows = self._fetch_rows("SELECT * FROM map_no_go_areas ORDER BY area_id ASC, updated_ts ASC;")
                for row in area_rows or []:
                    revision_ids = self._revision_ids_for_map_identity(map_id=str(row["map_id"] or ""))
                    for revision_id in revision_ids:
                        cur.execute(
                            """
                            INSERT OR IGNORE INTO map_revision_no_go_areas(
                              map_revision_id, constraint_version, area_id, name, polygon_json, enabled, updated_ts
                            ) VALUES(?,?,?,?,?,?,?);
                            """,
                            (
                                revision_id,
                                str(row["constraint_version"] or ""),
                                str(row["area_id"] or ""),
                                str(row["name"] or ""),
                                str(row["polygon_json"] or "[]"),
                                int(row["enabled"] or 0),
                                float(row["updated_ts"] or _now_ts()),
                            ),
                        )
            if self._table_exists("map_virtual_walls"):
                wall_rows = self._fetch_rows("SELECT * FROM map_virtual_walls ORDER BY wall_id ASC, updated_ts ASC;")
                for row in wall_rows or []:
                    revision_ids = self._revision_ids_for_map_identity(map_id=str(row["map_id"] or ""))
                    for revision_id in revision_ids:
                        cur.execute(
                            """
                            INSERT OR IGNORE INTO map_revision_virtual_walls(
                              map_revision_id, constraint_version, wall_id, name, polyline_json, buffer_m, enabled, updated_ts
                            ) VALUES(?,?,?,?,?,?,?,?);
                            """,
                            (
                                revision_id,
                                str(row["constraint_version"] or ""),
                                str(row["wall_id"] or ""),
                                str(row["name"] or ""),
                                str(row["polyline_json"] or "[]"),
                                float(row["buffer_m"] or DEFAULT_VIRTUAL_WALL_BUFFER_M),
                                int(row["enabled"] or 0),
                                float(row["updated_ts"] or _now_ts()),
                            ),
                        )
            if self._table_exists("map_active_constraint_versions"):
                active_rows = self._fetch_rows("SELECT * FROM map_active_constraint_versions;")
                for row in active_rows or []:
                    revision_ids = self._revision_ids_for_map_identity(map_id=str(row["map_id"] or ""))
                    for revision_id in revision_ids:
                        cur.execute(
                            """
                            INSERT OR IGNORE INTO map_active_constraint_revisions(map_revision_id, active_constraint_version, updated_ts)
                            VALUES(?,?,?);
                            """,
                            (
                                revision_id,
                                str(row["active_constraint_version"] or ""),
                                float(row["updated_ts"] or _now_ts()),
                            ),
                        )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def _set_meta_value(self, key: str, value: Any):
        self.conn.execute(
            """
            INSERT INTO schema_meta(key, value, updated_ts)
            VALUES(?,?,?)
            ON CONFLICT(key) DO UPDATE SET
              value=excluded.value,
              updated_ts=excluded.updated_ts;
            """,
            (
                str(key or "").strip(),
                str(value if value is not None else ""),
                _now_ts(),
            ),
        )

    def _record_schema_meta(self, *, upgrade_reasons: Optional[List[str]] = None):
        self._set_meta_value("schema_generation", "plan_store_v2_scoped_maps")
        if upgrade_reasons:
            self._set_meta_value("last_schema_upgrade_reasons", _json_dumps(list(upgrade_reasons)))
            self._set_meta_value("last_schema_upgrade_ts", str(_now_ts()))
        self.conn.commit()

    def _seed_plan_profiles(self):
        now = _now_ts()
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT OR IGNORE INTO plan_profiles(plan_profile_name, description, enabled, usage_type, updated_ts)
            VALUES(?,?,?,?,?);
            """,
            ("cover_standard", "default coverage execution profile", 1, "normal", now),
        )
        cur.execute(
            """
            INSERT OR IGNORE INTO plan_profiles(plan_profile_name, description, enabled, usage_type, updated_ts)
            VALUES(?,?,?,?,?);
            """,
            ("cover_eco", "reserved plan profile for inspection/AI-triggered cleaning", 1, "inspect_reserved", now),
        )
        self.conn.commit()

    def list_plan_profiles(self) -> List[Dict[str, Any]]:
        rows = self._fetch_rows(
            """
            SELECT plan_profile_name, description, enabled, usage_type, updated_ts
            FROM plan_profiles
            ORDER BY plan_profile_name ASC;
            """
        )
        out: List[Dict[str, Any]] = []
        for row in rows or []:
            out.append(
                {
                    "plan_profile_name": str(row["plan_profile_name"] or ""),
                    "description": str(row["description"] or ""),
                    "enabled": bool(int(row["enabled"] or 0)),
                    "usage_type": str(row["usage_type"] or ""),
                    "updated_ts": float(row["updated_ts"] or 0.0),
                }
            )
        return out

    def list_plan_profile_supported_maps(self) -> Dict[str, List[str]]:
        rows = self._fetch_rows(
            """
            SELECT DISTINCT plan_profile_name, map_name
            FROM plans
            WHERE TRIM(COALESCE(plan_profile_name, '')) != ''
            ORDER BY plan_profile_name ASC, map_name ASC;
            """
        )
        out: Dict[str, List[str]] = {}
        for row in rows or []:
            profile_name = str(row["plan_profile_name"] or "").strip()
            map_name = str(row["map_name"] or "").strip()
            if (not profile_name) or (not map_name):
                continue
            vals = out.setdefault(profile_name, [])
            if map_name not in vals:
                vals.append(map_name)
        return out

    def _fetch_rows(self, query: str, args: Tuple[Any, ...] = ()) -> List[sqlite3.Row]:
        try:
            return list(self.conn.execute(query, args).fetchall() or [])
        except Exception:
            return []

    def _table_not_empty(self, table_name: str) -> bool:
        if not self._table_exists(table_name):
            return False
        row = self.conn.execute("SELECT 1 FROM %s LIMIT 1;" % str(table_name)).fetchone()
        return bool(row)

    def _read_pre_upgrade_assets(self) -> List[Dict[str, Any]]:
        assets: Dict[str, Dict[str, Any]] = {}
        if self._table_exists("map_asset_versions"):
            version_rows = self._fetch_rows(
                """
                SELECT *
                FROM map_asset_versions
                ORDER BY
                  CASE WHEN map_version = map_name THEN 0 ELSE 1 END ASC,
                  created_ts DESC,
                  map_version DESC;
                """
            )
            for row in version_rows:
                map_name = _normalize_map_name(row["map_name"] or "")
                if not map_name or map_name in assets:
                    continue
                assets[map_name] = {
                    "map_name": map_name,
                    "map_id": str(row["map_id"] or ""),
                    "map_md5": str(row["map_md5"] or ""),
                    "yaml_path": str(row["yaml_path"] or ""),
                    "pgm_path": str(row["pgm_path"] or ""),
                    "pbstream_path": str(row["pbstream_path"] or ""),
                    "frame_id": str(row["frame_id"] or ""),
                    "resolution": float(row["resolution"] or 0.0),
                    "origin_json": str(row["origin_json"] or "[0,0,0]"),
                    "created_ts": float(row["created_ts"] or _now_ts()),
                    "updated_ts": float(row["created_ts"] or _now_ts()),
                }
        if self._table_exists("map_assets"):
            meta_rows = self._fetch_rows("SELECT * FROM map_assets;")
            for row in meta_rows:
                map_name = _normalize_map_name(row["map_name"] or "")
                if not map_name:
                    continue
                asset = assets.get(map_name, {})
                asset.update(
                    {
                        "map_name": map_name,
                        "display_name": map_name,
                        "enabled": int(row["enabled"] or 0),
                        "description": str(row["description"] or ""),
                        "created_ts": float(asset.get("created_ts", row["created_ts"] or _now_ts())),
                        "updated_ts": float(row["updated_ts"] or asset.get("updated_ts", _now_ts())),
                    }
                )
                if "map_id" in row.keys():
                    asset["map_id"] = str(row["map_id"] or asset.get("map_id") or "")
                    asset["map_md5"] = str(row["map_md5"] or asset.get("map_md5") or "")
                    asset["yaml_path"] = str(row["yaml_path"] or asset.get("yaml_path") or "")
                    asset["pgm_path"] = str(row["pgm_path"] or asset.get("pgm_path") or "")
                    asset["pbstream_path"] = str(row["pbstream_path"] or asset.get("pbstream_path") or "")
                    asset["frame_id"] = str(row["frame_id"] or asset.get("frame_id") or "")
                    asset["resolution"] = float(row["resolution"] or asset.get("resolution") or 0.0)
                    asset["origin_json"] = str(row["origin_json"] or asset.get("origin_json") or "[0,0,0]")
                assets[map_name] = asset
        out: List[Dict[str, Any]] = []
        for map_name, asset in sorted(assets.items()):
            if not asset.get("yaml_path") or not asset.get("pgm_path"):
                continue
            out.append(
                {
                    "map_name": map_name,
                    "display_name": map_name,
                    "enabled": int(asset.get("enabled", 1)),
                    "description": str(asset.get("description") or ""),
                    "map_id": str(asset.get("map_id") or ""),
                    "map_md5": str(asset.get("map_md5") or ""),
                    "yaml_path": str(asset.get("yaml_path") or ""),
                    "pgm_path": str(asset.get("pgm_path") or ""),
                    "pbstream_path": str(asset.get("pbstream_path") or ""),
                    "frame_id": str(asset.get("frame_id") or ""),
                    "resolution": float(asset.get("resolution") or 0.0),
                    "origin_json": str(asset.get("origin_json") or "[0,0,0]"),
                    "created_ts": float(asset.get("created_ts") or _now_ts()),
                    "updated_ts": float(asset.get("updated_ts") or asset.get("created_ts") or _now_ts()),
                }
            )
        return out

    def _read_pre_upgrade_active_maps(self) -> List[Dict[str, Any]]:
        if not self._table_exists("robot_active_map"):
            return []
        rows = self._fetch_rows("SELECT * FROM robot_active_map;")
        out: List[Dict[str, Any]] = []
        for row in rows:
            map_name = _normalize_map_name(row["map_name"] or "")
            if not map_name:
                continue
            out.append(
                {
                    "robot_id": str(row["robot_id"] or "local_robot"),
                    "map_name": map_name,
                    "updated_ts": float(row["updated_ts"] or _now_ts()),
                }
            )
        return out

    def _read_pre_upgrade_zone_heads(self) -> List[Dict[str, Any]]:
        if self._table_not_empty("map_zones"):
            rows = self._fetch_rows("SELECT * FROM map_zones ORDER BY map_name ASC, zone_id ASC;")
        else:
            rows = self._fetch_rows("SELECT * FROM zones ORDER BY zone_id ASC;")
        out: List[Dict[str, Any]] = []
        seen = set()
        for row in rows:
            map_name = _normalize_map_name((row["map_name"] if "map_name" in row.keys() else "") or "")
            if not map_name and "map_version" in row.keys():
                map_name = _normalize_map_name(row["map_version"] or "")
            zone_id = str(row["zone_id"] or "").strip()
            if not zone_id:
                continue
            key = (map_name, zone_id)
            if key in seen:
                continue
            seen.add(key)
            out.append(
                {
                    "map_name": map_name,
                    "zone_id": zone_id,
                    "display_name": str(row["display_name"] or zone_id),
                    "enabled": int(row["enabled"] or 0),
                    "current_zone_version": int(row["current_zone_version"] or 0),
                    "updated_ts": float(row["updated_ts"] or _now_ts()),
                }
            )
        return out

    def _read_pre_upgrade_zone_versions(self) -> List[Dict[str, Any]]:
        if self._table_not_empty("map_zone_versions"):
            rows = self._fetch_rows(
                "SELECT * FROM map_zone_versions ORDER BY map_name ASC, zone_id ASC, zone_version ASC;"
            )
        else:
            rows = self._fetch_rows("SELECT * FROM zone_versions ORDER BY zone_id ASC, zone_version ASC;")
        out: List[Dict[str, Any]] = []
        seen = set()
        for row in rows:
            map_name = _normalize_map_name((row["map_name"] if "map_name" in row.keys() else "") or "")
            if not map_name and "map_version" in row.keys():
                map_name = _normalize_map_name(row["map_version"] or "")
            zone_id = str(row["zone_id"] or "").strip()
            zone_version = int(row["zone_version"] or 0)
            if not zone_id:
                continue
            key = (map_name, zone_id, zone_version)
            if key in seen:
                continue
            seen.add(key)
            out.append(
                {
                    "map_name": map_name,
                    "zone_id": zone_id,
                    "zone_version": zone_version,
                    "frame_id": str(row["frame_id"] or ""),
                    "outer_json": str(row["outer_json"] or "[]"),
                    "holes_json": str(row["holes_json"] or "[]"),
                    "map_id": str(row["map_id"] or ""),
                    "map_md5": str(row["map_md5"] or ""),
                    "created_ts": float(row["created_ts"] or _now_ts()),
                }
            )
        return out

    def _read_pre_upgrade_active_plans(self) -> List[Dict[str, Any]]:
        if self._table_not_empty("map_zone_active_plans"):
            rows = self._fetch_rows(
                "SELECT * FROM map_zone_active_plans ORDER BY map_name ASC, zone_id ASC, plan_profile_name ASC;"
            )
        else:
            rows = self._fetch_rows("SELECT * FROM zone_active_plans ORDER BY zone_id ASC, plan_profile_name ASC;")
        out: List[Dict[str, Any]] = []
        seen = set()
        for row in rows:
            map_name = _normalize_map_name((row["map_name"] if "map_name" in row.keys() else "") or "")
            if not map_name and "active_plan_id" in row.keys():
                plan_row = self.conn.execute(
                    "SELECT map_name FROM plans WHERE plan_id=? LIMIT 1;",
                    (str(row["active_plan_id"] or "").strip(),),
                ).fetchone()
                map_name = _normalize_map_name((plan_row["map_name"] if plan_row else "") or "")
            if not map_name and "map_version" in row.keys():
                map_name = _normalize_map_name(row["map_version"] or "")
            zone_id = str(row["zone_id"] or "").strip()
            profile = str(row["plan_profile_name"] or "").strip()
            active_plan_id = str(row["active_plan_id"] or "").strip()
            if not zone_id or not profile or not active_plan_id:
                continue
            key = (map_name, zone_id, profile)
            if key in seen:
                continue
            seen.add(key)
            out.append(
                {
                    "map_name": map_name,
                    "zone_id": zone_id,
                    "plan_profile_name": profile,
                    "active_plan_id": active_plan_id,
                    "updated_ts": float(row["updated_ts"] or _now_ts()),
                }
            )
        return out

    def _read_pre_upgrade_plans(self) -> List[Dict[str, Any]]:
        if not self._table_exists("plans"):
            return []
        rows = self._fetch_rows("SELECT * FROM plans ORDER BY created_ts ASC;")
        out: List[Dict[str, Any]] = []
        for row in rows:
            map_name = _normalize_map_name((row["map_name"] if "map_name" in row.keys() else "") or "")
            if not map_name and "map_version" in row.keys():
                map_name = _normalize_map_name(row["map_version"] or "")
            out.append(
                {
                    "plan_id": str(row["plan_id"] or ""),
                    "map_name": map_name,
                    "zone_id": str(row["zone_id"] or ""),
                    "zone_version": int(row["zone_version"] or 0),
                    "frame_id": str(row["frame_id"] or ""),
                    "plan_profile_name": str(row["plan_profile_name"] or row["profile_name"] or ""),
                    "params_json": str(row["params_json"] or "{}"),
                    "robot_json": str(row["robot_json"] or "{}"),
                    "blocks": int(row["blocks"] or 0),
                    "total_length_m": float(row["total_length_m"] or 0.0),
                    "exec_order_json": str(row["exec_order_json"] or "[]"),
                    "map_id": str(row["map_id"] or ""),
                    "map_md5": str(row["map_md5"] or ""),
                    "constraint_version": str(row["constraint_version"] or ""),
                    "planner_version": str(row["planner_version"] or ""),
                    "created_ts": float(row["created_ts"] or _now_ts()),
                }
            )
        return out

    def _copy_simple_table(self, table_name: str) -> List[Dict[str, Any]]:
        if not self._table_exists(table_name):
            return []
        rows = self._fetch_rows("SELECT * FROM %s;" % str(table_name))
        return [dict(r) for r in rows]

    def _drop_table_if_exists(self, table_name: str):
        self.conn.execute("DROP TABLE IF EXISTS %s;" % str(table_name))

    def _upgrade_pre_scoped_map_schema(self, upgrade_reasons: Optional[List[str]] = None):
        plan_profiles = self._copy_simple_table("plan_profiles")
        plan_blocks = self._copy_simple_table("plan_blocks")
        constraint_versions = self._copy_simple_table("map_constraint_versions")
        no_go_areas = self._copy_simple_table("map_no_go_areas")
        virtual_walls = self._copy_simple_table("map_virtual_walls")
        active_constraint_versions = self._copy_simple_table("map_active_constraint_versions")

        assets = self._read_pre_upgrade_assets()
        active_maps = self._read_pre_upgrade_active_maps()
        zones = self._read_pre_upgrade_zone_heads()
        zone_versions = self._read_pre_upgrade_zone_versions()
        active_plans = self._read_pre_upgrade_active_plans()
        plans = self._read_pre_upgrade_plans()

        cur = self.conn.cursor()
        try:
            cur.execute("BEGIN IMMEDIATE;")
            for table_name in self._SCHEMA_UPGRADE_TABLES:
                self._drop_table_if_exists(table_name)
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

        self._create_schema()
        asset_revision_lookup: Dict[str, str] = {}
        for row in assets:
            map_name = _normalize_map_name(str(row.get("map_name") or ""))
            if not map_name:
                continue
            asset_revision_lookup[map_name] = self._generate_revision_id(map_name)

        cur = self.conn.cursor()
        try:
            cur.execute("BEGIN IMMEDIATE;")
            for row in plan_profiles:
                cur.execute(
                    """
                    INSERT OR REPLACE INTO plan_profiles(plan_profile_name, description, enabled, usage_type, updated_ts)
                    VALUES(?,?,?,?,?);
                    """,
                    (
                        str(row.get("plan_profile_name") or ""),
                        str(row.get("description") or ""),
                        int(row.get("enabled", 1) or 0),
                        str(row.get("usage_type") or ""),
                        float(row.get("updated_ts") or _now_ts()),
                    ),
                )
            for row in assets:
                map_name = _normalize_map_name(str(row.get("map_name") or ""))
                revision_id = str(asset_revision_lookup.get(map_name) or "").strip()
                if not map_name or not revision_id:
                    continue
                cur.execute(
                    """
                    INSERT OR REPLACE INTO map_assets(
                      map_name, display_name, enabled, description, map_id, map_md5,
                      yaml_path, pgm_path, pbstream_path, frame_id, resolution, origin_json,
                      created_ts, updated_ts, current_revision_id
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
                    """,
                    (
                        map_name,
                        str(row.get("display_name") or map_name),
                        int(row.get("enabled", 1) or 0),
                        str(row.get("description") or ""),
                        str(row.get("map_id") or ""),
                        str(row.get("map_md5") or ""),
                        os.path.expanduser(str(row.get("yaml_path") or "")),
                        os.path.expanduser(str(row.get("pgm_path") or "")),
                        os.path.expanduser(str(row.get("pbstream_path") or "")),
                        str(row.get("frame_id") or "map"),
                        float(row.get("resolution") or 0.0),
                        str(row.get("origin_json") or "[0,0,0]"),
                        float(row.get("created_ts") or _now_ts()),
                        float(row.get("updated_ts") or row.get("created_ts") or _now_ts()),
                        revision_id,
                    ),
                )
                cur.execute(
                    """
                    INSERT OR REPLACE INTO map_revisions(
                      revision_id, map_name, display_name, enabled, description,
                      map_id, map_md5, yaml_path, pgm_path, pbstream_path,
                      frame_id, resolution, origin_json, lifecycle_status, verification_status,
                      artifact_sha256, live_snapshot_md5, verified_runtime_map_id, verified_runtime_map_md5,
                      last_error_code, last_error_msg, source_job_id,
                      created_ts, verified_ts, activated_ts, updated_ts
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
                    """,
                    (
                        revision_id,
                        map_name,
                        str(row.get("display_name") or map_name),
                        int(row.get("enabled", 1) or 0),
                        str(row.get("description") or ""),
                        str(row.get("map_id") or ""),
                        str(row.get("map_md5") or ""),
                        os.path.expanduser(str(row.get("yaml_path") or "")),
                        os.path.expanduser(str(row.get("pgm_path") or "")),
                        os.path.expanduser(str(row.get("pbstream_path") or "")),
                        str(row.get("frame_id") or "map"),
                        float(row.get("resolution") or 0.0),
                        str(row.get("origin_json") or "[0,0,0]"),
                        "available",
                        "verified",
                        _artifact_sha256_for_paths(
                            str(row.get("pbstream_path") or ""),
                            str(row.get("yaml_path") or ""),
                            str(row.get("pgm_path") or ""),
                        ),
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        float(row.get("created_ts") or _now_ts()),
                        None,
                        None,
                        float(row.get("updated_ts") or row.get("created_ts") or _now_ts()),
                    ),
                )
            for row in active_maps:
                map_name = _normalize_map_name(str(row.get("map_name") or ""))
                cur.execute(
                    """
                    INSERT OR REPLACE INTO robot_active_map(robot_id, map_name, updated_ts)
                    VALUES(?,?,?);
                    """,
                    (
                        str(row.get("robot_id") or "local_robot"),
                        map_name,
                        float(row.get("updated_ts") or _now_ts()),
                    ),
                )
                revision_id = str(asset_revision_lookup.get(map_name) or "").strip()
                if revision_id:
                    cur.execute(
                        """
                        INSERT OR REPLACE INTO robot_active_map_revision(robot_id, active_revision_id, updated_ts)
                        VALUES(?,?,?);
                        """,
                        (
                            str(row.get("robot_id") or "local_robot"),
                            revision_id,
                            float(row.get("updated_ts") or _now_ts()),
                        ),
                    )
            for row in zones:
                map_name = _normalize_map_name(str(row.get("map_name") or ""))
                revision_id = str(asset_revision_lookup.get(map_name) or "").strip()
                if not map_name or not revision_id:
                    continue
                cur.execute(
                    """
                    INSERT OR REPLACE INTO zones(
                      map_revision_id, map_name, zone_id, display_name, enabled, current_zone_version, updated_ts
                    ) VALUES(?,?,?,?,?,?,?);
                    """,
                    (
                        revision_id,
                        map_name,
                        str(row.get("zone_id") or ""),
                        str(row.get("display_name") or row.get("zone_id") or ""),
                        int(row.get("enabled", 1) or 0),
                        int(row.get("current_zone_version") or 0),
                        float(row.get("updated_ts") or _now_ts()),
                    ),
                )
            for row in zone_versions:
                map_name = _normalize_map_name(str(row.get("map_name") or ""))
                revision_id = str(asset_revision_lookup.get(map_name) or "").strip()
                if not map_name or not revision_id:
                    continue
                cur.execute(
                    """
                    INSERT OR REPLACE INTO zone_versions(
                      map_revision_id, map_name, zone_id, zone_version, frame_id, outer_json, holes_json, map_id, map_md5, created_ts
                    ) VALUES(?,?,?,?,?,?,?,?,?,?);
                    """,
                    (
                        revision_id,
                        map_name,
                        str(row.get("zone_id") or ""),
                        int(row.get("zone_version") or 0),
                        str(row.get("frame_id") or ""),
                        str(row.get("outer_json") or "[]"),
                        str(row.get("holes_json") or "[]"),
                        str(row.get("map_id") or ""),
                        str(row.get("map_md5") or ""),
                        float(row.get("created_ts") or _now_ts()),
                    ),
                )
            for row in plans:
                map_name = _normalize_map_name(str(row.get("map_name") or ""))
                revision_id = str(asset_revision_lookup.get(map_name) or "").strip()
                if not map_name or not revision_id:
                    continue
                cur.execute(
                    """
                    INSERT OR REPLACE INTO plans(
                      plan_id, map_revision_id, map_name, zone_id, zone_version, frame_id, plan_profile_name,
                      params_json, robot_json, blocks, total_length_m, exec_order_json,
                      map_id, map_md5, constraint_version, planner_version, created_ts
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
                    """,
                    (
                        str(row.get("plan_id") or ""),
                        revision_id,
                        map_name,
                        str(row.get("zone_id") or ""),
                        int(row.get("zone_version") or 0),
                        str(row.get("frame_id") or ""),
                        str(row.get("plan_profile_name") or ""),
                        str(row.get("params_json") or "{}"),
                        str(row.get("robot_json") or "{}"),
                        int(row.get("blocks") or 0),
                        float(row.get("total_length_m") or 0.0),
                        str(row.get("exec_order_json") or "[]"),
                        str(row.get("map_id") or ""),
                        str(row.get("map_md5") or ""),
                        str(row.get("constraint_version") or ""),
                        str(row.get("planner_version") or ""),
                        float(row.get("created_ts") or _now_ts()),
                    ),
                )
            for row in plan_blocks:
                cur.execute(
                    """
                    INSERT OR REPLACE INTO plan_blocks(
                      plan_id, block_id, entry_x, entry_y, entry_yaw, exit_x, exit_y, exit_yaw,
                      point_count, length_m, path_step_m, path_blob
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?);
                    """,
                    (
                        str(row.get("plan_id") or ""),
                        int(row.get("block_id") or 0),
                        row.get("entry_x"),
                        row.get("entry_y"),
                        row.get("entry_yaw"),
                        row.get("exit_x"),
                        row.get("exit_y"),
                        row.get("exit_yaw"),
                        int(row.get("point_count") or 0),
                        float(row.get("length_m") or 0.0),
                        row.get("path_step_m"),
                        row.get("path_blob"),
                    ),
                )
            for row in active_plans:
                map_name = _normalize_map_name(str(row.get("map_name") or ""))
                revision_id = str(asset_revision_lookup.get(map_name) or "").strip()
                if not map_name or not revision_id:
                    continue
                cur.execute(
                    """
                    INSERT OR REPLACE INTO zone_active_plans(
                      map_revision_id, map_name, zone_id, plan_profile_name, active_plan_id, updated_ts
                    ) VALUES(?,?,?,?,?,?);
                    """,
                    (
                        revision_id,
                        map_name,
                        str(row.get("zone_id") or ""),
                        str(row.get("plan_profile_name") or ""),
                        str(row.get("active_plan_id") or ""),
                        float(row.get("updated_ts") or _now_ts()),
                    ),
                )
            for row in constraint_versions:
                cur.execute(
                    """
                    INSERT OR REPLACE INTO map_constraint_versions(map_id, constraint_version, map_md5, created_ts, updated_ts)
                    VALUES(?,?,?,?,?);
                    """,
                    (
                        str(row.get("map_id") or ""),
                        str(row.get("constraint_version") or ""),
                        str(row.get("map_md5") or ""),
                        float(row.get("created_ts") or _now_ts()),
                        float(row.get("updated_ts") or _now_ts()),
                    ),
                )
            for row in no_go_areas:
                cur.execute(
                    """
                    INSERT OR REPLACE INTO map_no_go_areas(
                      map_id, constraint_version, area_id, name, polygon_json, enabled, updated_ts
                    ) VALUES(?,?,?,?,?,?,?);
                    """,
                    (
                        str(row.get("map_id") or ""),
                        str(row.get("constraint_version") or ""),
                        str(row.get("area_id") or ""),
                        str(row.get("name") or ""),
                        str(row.get("polygon_json") or "[]"),
                        int(row.get("enabled", 1) or 0),
                        float(row.get("updated_ts") or _now_ts()),
                    ),
                )
            for row in virtual_walls:
                cur.execute(
                    """
                    INSERT OR REPLACE INTO map_virtual_walls(
                      map_id, constraint_version, wall_id, name, polyline_json, buffer_m, enabled, updated_ts
                    ) VALUES(?,?,?,?,?,?,?,?);
                    """,
                    (
                        str(row.get("map_id") or ""),
                        str(row.get("constraint_version") or ""),
                        str(row.get("wall_id") or ""),
                        str(row.get("name") or ""),
                        str(row.get("polyline_json") or "[]"),
                        float(row.get("buffer_m") or DEFAULT_VIRTUAL_WALL_BUFFER_M),
                        int(row.get("enabled", 1) or 0),
                        float(row.get("updated_ts") or _now_ts()),
                    ),
                )
            for row in active_constraint_versions:
                cur.execute(
                    """
                    INSERT OR REPLACE INTO map_active_constraint_versions(map_id, active_constraint_version, updated_ts)
                    VALUES(?,?,?);
                    """,
                    (
                        str(row.get("map_id") or ""),
                        str(row.get("active_constraint_version") or ""),
                        float(row.get("updated_ts") or _now_ts()),
                    ),
                )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        self._record_schema_meta(upgrade_reasons=list(upgrade_reasons or []))

    def upsert_plan_profile(
        self,
        *,
        plan_profile_name: str,
        description: str = "",
        enabled: bool = True,
        usage_type: str = "normal",
    ):
        cur = self.conn.cursor()
        self._upsert_plan_profile_cur(
            cur,
            plan_profile_name=plan_profile_name,
            description=description,
            enabled=enabled,
            usage_type=usage_type,
        )
        self.conn.commit()

    def _upsert_plan_profile_cur(
        self,
        cur: sqlite3.Cursor,
        *,
        plan_profile_name: str,
        description: str = "",
        enabled: bool = True,
        usage_type: str = "normal",
    ):
        cur.execute(
            """
            INSERT INTO plan_profiles(plan_profile_name, description, enabled, usage_type, updated_ts)
            VALUES(?,?,?,?,?)
            ON CONFLICT(plan_profile_name) DO UPDATE SET
              description=excluded.description,
              enabled=excluded.enabled,
              usage_type=excluded.usage_type,
              updated_ts=excluded.updated_ts;
            """,
            (
                str(plan_profile_name or "").strip(),
                str(description or "").strip(),
                1 if bool(enabled) else 0,
                str(usage_type or "normal").strip(),
                _now_ts(),
            ),
        )

    def upsert_map_asset(
        self,
        *,
        map_name: str,
        display_name: str = "",
        enabled: bool = True,
        description: str = "",
        map_id: str = "",
        map_md5: str = "",
        yaml_path: str = "",
        pgm_path: str = "",
        pbstream_path: str = "",
        frame_id: str = "map",
        resolution: float = 0.0,
        origin: Optional[List[float]] = None,
        created_ts: Optional[float] = None,
        lifecycle_status: Optional[str] = None,
        verification_status: Optional[str] = None,
        save_snapshot_md5: Optional[str] = None,
        verified_runtime_map_id: Optional[str] = None,
        verified_runtime_map_md5: Optional[str] = None,
        last_error_code: Optional[str] = None,
        last_error_msg: Optional[str] = None,
        source_job_id: Optional[str] = None,
        revision_id: Optional[str] = None,
    ):
        map_name = _normalize_map_name(map_name)
        if not map_name:
            raise ValueError("map_name is required")
        ts = _now_ts()
        current = self.conn.execute(
            "SELECT * FROM map_assets WHERE map_name=? LIMIT 1;",
            (map_name,),
        ).fetchone()
        cts = float(created_ts if created_ts is not None else ((current["created_ts"] if current else 0.0) or ts))
        resolved_display_name = str(display_name or (current["display_name"] if current else "") or map_name).strip() or map_name
        resolved_lifecycle_status = str(
            lifecycle_status
            if lifecycle_status is not None
            else ((current["lifecycle_status"] if current and "lifecycle_status" in current.keys() else "") or "available")
        ).strip() or "available"
        resolved_verification_status = str(
            verification_status
            if verification_status is not None
            else ((current["verification_status"] if current and "verification_status" in current.keys() else "") or "verified")
        ).strip() or "verified"
        resolved_save_snapshot_md5 = str(
            save_snapshot_md5
            if save_snapshot_md5 is not None
            else ((current["save_snapshot_md5"] if current and "save_snapshot_md5" in current.keys() else "") or "")
        ).strip()
        resolved_verified_runtime_map_id = str(
            verified_runtime_map_id
            if verified_runtime_map_id is not None
            else ((current["verified_runtime_map_id"] if current and "verified_runtime_map_id" in current.keys() else "") or "")
        ).strip()
        resolved_verified_runtime_map_md5 = str(
            verified_runtime_map_md5
            if verified_runtime_map_md5 is not None
            else ((current["verified_runtime_map_md5"] if current and "verified_runtime_map_md5" in current.keys() else "") or "")
        ).strip()
        resolved_last_error_code = str(
            last_error_code
            if last_error_code is not None
            else ((current["last_error_code"] if current and "last_error_code" in current.keys() else "") or "")
        ).strip()
        resolved_last_error_msg = str(
            last_error_msg
            if last_error_msg is not None
            else ((current["last_error_msg"] if current and "last_error_msg" in current.keys() else "") or "")
        ).strip()
        resolved_source_job_id = str(
            source_job_id
            if source_job_id is not None
            else ((current["source_job_id"] if current and "source_job_id" in current.keys() else "") or "")
        ).strip()
        resolved_revision_id = str(
            revision_id
            if revision_id is not None
            else ((current["current_revision_id"] if current and "current_revision_id" in current.keys() else "") or "")
        ).strip()
        if not resolved_revision_id:
            resolved_revision_id = self._generate_revision_id(map_name)
        self.conn.execute(
            """
            INSERT INTO map_assets(
              map_name, display_name, enabled, description, map_id, map_md5,
              yaml_path, pgm_path, pbstream_path, frame_id, resolution, origin_json,
              lifecycle_status, verification_status, save_snapshot_md5,
              verified_runtime_map_id, verified_runtime_map_md5,
              last_error_code, last_error_msg, source_job_id, current_revision_id,
              created_ts, updated_ts
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(map_name) DO UPDATE SET
              display_name=excluded.display_name,
              enabled=excluded.enabled,
              description=excluded.description,
              map_id=excluded.map_id,
              map_md5=excluded.map_md5,
              yaml_path=excluded.yaml_path,
              pgm_path=excluded.pgm_path,
              pbstream_path=excluded.pbstream_path,
              frame_id=excluded.frame_id,
              resolution=excluded.resolution,
              origin_json=excluded.origin_json,
              lifecycle_status=excluded.lifecycle_status,
              verification_status=excluded.verification_status,
              save_snapshot_md5=excluded.save_snapshot_md5,
              verified_runtime_map_id=excluded.verified_runtime_map_id,
              verified_runtime_map_md5=excluded.verified_runtime_map_md5,
              last_error_code=excluded.last_error_code,
              last_error_msg=excluded.last_error_msg,
              source_job_id=excluded.source_job_id,
              current_revision_id=excluded.current_revision_id,
              updated_ts=excluded.updated_ts;
            """,
            (
                map_name,
                resolved_display_name,
                1 if bool(enabled) else 0,
                str(description or "").strip(),
                str(map_id or "").strip(),
                str(map_md5 or "").strip(),
                os.path.expanduser(str(yaml_path or "").strip()),
                os.path.expanduser(str(pgm_path or "").strip()),
                os.path.expanduser(str(pbstream_path or "").strip()),
                str(frame_id or "map").strip(),
                float(resolution or 0.0),
                _json_dumps(list(origin or [0.0, 0.0, 0.0])),
                resolved_lifecycle_status,
                resolved_verification_status,
                resolved_save_snapshot_md5,
                resolved_verified_runtime_map_id,
                resolved_verified_runtime_map_md5,
                resolved_last_error_code,
                resolved_last_error_msg,
                resolved_source_job_id,
                resolved_revision_id,
                cts,
                ts,
            ),
        )
        self.conn.commit()
        self.upsert_map_revision(
            revision_id=resolved_revision_id,
            map_name=map_name,
            display_name=resolved_display_name,
            enabled=bool(enabled),
            description=str(description or "").strip(),
            map_id=str(map_id or "").strip(),
            map_md5=str(map_md5 or "").strip(),
            yaml_path=os.path.expanduser(str(yaml_path or "").strip()),
            pgm_path=os.path.expanduser(str(pgm_path or "").strip()),
            pbstream_path=os.path.expanduser(str(pbstream_path or "").strip()),
            frame_id=str(frame_id or "map").strip(),
            resolution=float(resolution or 0.0),
            origin=list(origin or [0.0, 0.0, 0.0]),
            lifecycle_status=resolved_lifecycle_status,
            verification_status=resolved_verification_status,
            live_snapshot_md5=resolved_save_snapshot_md5,
            verified_runtime_map_id=resolved_verified_runtime_map_id,
            verified_runtime_map_md5=resolved_verified_runtime_map_md5,
            last_error_code=resolved_last_error_code,
            last_error_msg=resolved_last_error_msg,
            source_job_id=resolved_source_job_id,
            created_ts=cts,
            updated_ts=ts,
        )
        return resolved_revision_id

    def upsert_map_revision(
        self,
        *,
        revision_id: str,
        map_name: str,
        yaml_path: str,
        pgm_path: str,
        display_name: str = "",
        map_id: str = "",
        map_md5: str = "",
        pbstream_path: str = "",
        frame_id: str = "map",
        resolution: float = 0.0,
        origin: Optional[List[float]] = None,
        description: str = "",
        enabled: bool = True,
        lifecycle_status: str = "available",
        verification_status: str = "verified",
        live_snapshot_md5: str = "",
        verified_runtime_map_id: str = "",
        verified_runtime_map_md5: str = "",
        last_error_code: str = "",
        last_error_msg: str = "",
        source_job_id: str = "",
        created_ts: Optional[float] = None,
        verified_ts: Optional[float] = None,
        activated_ts: Optional[float] = None,
        updated_ts: Optional[float] = None,
    ):
        revision_id = str(revision_id or "").strip()
        map_name = _normalize_map_name(map_name)
        if not revision_id:
            raise ValueError("revision_id is required")
        if not map_name:
            raise ValueError("map_name is required")
        ts = float(updated_ts if updated_ts is not None else _now_ts())
        current = self.conn.execute(
            "SELECT * FROM map_revisions WHERE revision_id=? LIMIT 1;",
            (revision_id,),
        ).fetchone()
        cts = float(created_ts if created_ts is not None else ((current["created_ts"] if current else 0.0) or ts))
        current_verified_ts = float(current["verified_ts"] or 0.0) if current else 0.0
        current_activated_ts = float(current["activated_ts"] or 0.0) if current else 0.0
        resolved_verified_ts = (
            float(verified_ts)
            if verified_ts is not None
            else (
                current_verified_ts
                or (ts if str(verification_status or "").strip().lower() == "verified" else 0.0)
            )
        )
        resolved_activated_ts = (
            float(activated_ts)
            if activated_ts is not None
            else current_activated_ts
        )
        artifact_sha256 = _artifact_sha256_for_paths(pbstream_path, yaml_path, pgm_path)
        self.conn.execute(
            """
            INSERT INTO map_revisions(
              revision_id, map_name, display_name, enabled, description,
              map_id, map_md5, yaml_path, pgm_path, pbstream_path,
              frame_id, resolution, origin_json, lifecycle_status, verification_status,
              artifact_sha256, live_snapshot_md5, verified_runtime_map_id, verified_runtime_map_md5,
              last_error_code, last_error_msg, source_job_id,
              created_ts, verified_ts, activated_ts, updated_ts
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(revision_id) DO UPDATE SET
              map_name=excluded.map_name,
              display_name=excluded.display_name,
              enabled=excluded.enabled,
              description=excluded.description,
              map_id=excluded.map_id,
              map_md5=excluded.map_md5,
              yaml_path=excluded.yaml_path,
              pgm_path=excluded.pgm_path,
              pbstream_path=excluded.pbstream_path,
              frame_id=excluded.frame_id,
              resolution=excluded.resolution,
              origin_json=excluded.origin_json,
              lifecycle_status=excluded.lifecycle_status,
              verification_status=excluded.verification_status,
              artifact_sha256=excluded.artifact_sha256,
              live_snapshot_md5=excluded.live_snapshot_md5,
              verified_runtime_map_id=excluded.verified_runtime_map_id,
              verified_runtime_map_md5=excluded.verified_runtime_map_md5,
              last_error_code=excluded.last_error_code,
              last_error_msg=excluded.last_error_msg,
              source_job_id=excluded.source_job_id,
              verified_ts=excluded.verified_ts,
              activated_ts=excluded.activated_ts,
              updated_ts=excluded.updated_ts;
            """,
            (
                revision_id,
                map_name,
                str(display_name or map_name).strip() or map_name,
                1 if bool(enabled) else 0,
                str(description or "").strip(),
                str(map_id or "").strip(),
                str(map_md5 or "").strip(),
                os.path.expanduser(str(yaml_path or "").strip()),
                os.path.expanduser(str(pgm_path or "").strip()),
                os.path.expanduser(str(pbstream_path or "").strip()),
                str(frame_id or "map").strip(),
                float(resolution or 0.0),
                _json_dumps(list(origin or [0.0, 0.0, 0.0])),
                str(lifecycle_status or "available").strip() or "available",
                str(verification_status or "verified").strip() or "verified",
                artifact_sha256,
                str(live_snapshot_md5 or "").strip(),
                str(verified_runtime_map_id or "").strip(),
                str(verified_runtime_map_md5 or "").strip(),
                str(last_error_code or "").strip(),
                str(last_error_msg or "").strip(),
                str(source_job_id or "").strip(),
                cts,
                resolved_verified_ts if resolved_verified_ts > 0.0 else None,
                resolved_activated_ts if resolved_activated_ts > 0.0 else None,
                ts,
            ),
        )
        self.conn.commit()

    def _map_revision_row_to_dict(self, row: sqlite3.Row, *, updated_ts: Optional[float] = None) -> Dict[str, Any]:
        revision_id = str(row["revision_id"] or "").strip()
        return {
            "revision_id": revision_id,
            "map_name": _normalize_map_name(str(row["map_name"] or "")),
            "display_name": str(row["display_name"] or ""),
            "enabled": bool(int(row["enabled"] or 0)),
            "description": str(row["description"] or ""),
            "map_id": str(row["map_id"] or ""),
            "map_md5": str(row["map_md5"] or ""),
            "yaml_path": str(row["yaml_path"] or ""),
            "pgm_path": str(row["pgm_path"] or ""),
            "pbstream_path": str(row["pbstream_path"] or ""),
            "frame_id": str(row["frame_id"] or ""),
            "resolution": float(row["resolution"] or 0.0),
            "origin": _json_loads(str(row["origin_json"] or ""), [0.0, 0.0, 0.0]),
            "lifecycle_status": str(row["lifecycle_status"] or "available"),
            "verification_status": str(row["verification_status"] or "verified"),
            "artifact_sha256": str(row["artifact_sha256"] or ""),
            "live_snapshot_md5": str(row["live_snapshot_md5"] or ""),
            "verified_runtime_map_id": str(row["verified_runtime_map_id"] or ""),
            "verified_runtime_map_md5": str(row["verified_runtime_map_md5"] or ""),
            "last_error_code": str(row["last_error_code"] or ""),
            "last_error_msg": str(row["last_error_msg"] or ""),
            "source_job_id": str(row["source_job_id"] or ""),
            "created_ts": float(row["created_ts"] or 0.0),
            "verified_ts": float(row["verified_ts"] or 0.0),
            "activated_ts": float(row["activated_ts"] or 0.0),
            "updated_ts": float(updated_ts if updated_ts is not None else row["updated_ts"] or 0.0),
        }

    def _active_revision_row_to_asset_dict(
        self,
        row: sqlite3.Row,
        *,
        updated_ts: Optional[float] = None,
    ) -> Dict[str, Any]:
        revision = self._map_revision_row_to_dict(row, updated_ts=updated_ts)
        return {
            "map_name": str(revision.get("map_name") or ""),
            "revision_id": str(revision.get("revision_id") or ""),
            "current_revision_id": str(revision.get("revision_id") or ""),
            "display_name": str(revision.get("display_name") or revision.get("map_name") or ""),
            "enabled": bool(revision.get("enabled", True)),
            "description": str(revision.get("description") or ""),
            "map_id": str(revision.get("map_id") or ""),
            "map_md5": str(revision.get("map_md5") or ""),
            "yaml_path": str(revision.get("yaml_path") or ""),
            "pgm_path": str(revision.get("pgm_path") or ""),
            "pbstream_path": str(revision.get("pbstream_path") or ""),
            "frame_id": str(revision.get("frame_id") or ""),
            "resolution": float(revision.get("resolution") or 0.0),
            "origin": list(revision.get("origin") or [0.0, 0.0, 0.0]),
            "lifecycle_status": str(revision.get("lifecycle_status") or "available"),
            "verification_status": str(revision.get("verification_status") or "verified"),
            "save_snapshot_md5": str(revision.get("live_snapshot_md5") or ""),
            "live_snapshot_md5": str(revision.get("live_snapshot_md5") or ""),
            "verified_runtime_map_id": str(revision.get("verified_runtime_map_id") or ""),
            "verified_runtime_map_md5": str(revision.get("verified_runtime_map_md5") or ""),
            "last_error_code": str(revision.get("last_error_code") or ""),
            "last_error_msg": str(revision.get("last_error_msg") or ""),
            "source_job_id": str(revision.get("source_job_id") or ""),
            "created_ts": float(revision.get("created_ts") or 0.0),
            "verified_ts": float(revision.get("verified_ts") or 0.0),
            "activated_ts": float(revision.get("activated_ts") or 0.0),
            "updated_ts": float(revision.get("updated_ts") or 0.0),
        }

    def _preferred_map_scope_asset(
        self,
        *,
        map_name: str,
        robot_id: str = "local_robot",
    ) -> Optional[Dict[str, Any]]:
        normalized_map_name = _normalize_map_name(map_name)
        if not normalized_map_name:
            return None

        active_revision = self.get_active_map_revision(robot_id=robot_id) or {}
        if _normalize_map_name(str(active_revision.get("map_name") or "")) == normalized_map_name:
            if not map_asset_verification_error(active_revision, label="map revision"):
                return active_revision

        try:
            preferred = self._select_activatable_map_revision(
                map_name=normalized_map_name,
                robot_id=robot_id,
            ) or {}
            if preferred:
                return preferred
        except Exception:
            pass

        return self.resolve_map_asset(map_name=normalized_map_name, robot_id=robot_id)

    def resolve_map_revision(
        self,
        *,
        revision_id: str = "",
        map_name: str = "",
        robot_id: str = "local_robot",
    ) -> Optional[Dict[str, Any]]:
        normalized_revision_id = str(revision_id or "").strip()
        if normalized_revision_id:
            row = self.conn.execute(
                "SELECT * FROM map_revisions WHERE revision_id=? LIMIT 1;",
                (normalized_revision_id,),
            ).fetchone()
            return self._map_revision_row_to_dict(row) if row else None
        asset = self._preferred_map_scope_asset(map_name=map_name, robot_id=robot_id)
        if not asset:
            return None
        asset_revision_id = str(asset.get("revision_id") or "").strip()
        if not asset_revision_id:
            return None
        return self.resolve_map_revision(revision_id=asset_revision_id)

    def list_map_revisions(self, *, map_name: str = "") -> List[Dict[str, Any]]:
        normalized_map_name = _normalize_map_name(map_name)
        if normalized_map_name:
            rows = self._fetch_rows(
                "SELECT * FROM map_revisions WHERE map_name=? ORDER BY created_ts DESC, revision_id DESC;",
                (normalized_map_name,),
            )
        else:
            rows = self._fetch_rows("SELECT * FROM map_revisions ORDER BY map_name ASC, created_ts DESC;")
        return [self._map_revision_row_to_dict(row) for row in rows or []]

    def ensure_map_asset_revision(self, *, map_name: str, robot_id: str = "local_robot") -> str:
        asset = self.resolve_map_asset(map_name=map_name, robot_id=robot_id) or {}
        revision_id = str(asset.get("revision_id") or "").strip()
        if revision_id:
            return revision_id
        if not asset:
            return ""
        revision_id = self._generate_revision_id(str(asset.get("map_name") or map_name))
        self.upsert_map_asset(
            map_name=str(asset.get("map_name") or map_name),
            display_name=str(asset.get("display_name") or ""),
            enabled=bool(asset.get("enabled", True)),
            description=str(asset.get("description") or ""),
            map_id=str(asset.get("map_id") or ""),
            map_md5=str(asset.get("map_md5") or ""),
            yaml_path=str(asset.get("yaml_path") or ""),
            pgm_path=str(asset.get("pgm_path") or ""),
            pbstream_path=str(asset.get("pbstream_path") or ""),
            frame_id=str(asset.get("frame_id") or "map"),
            resolution=float(asset.get("resolution") or 0.0),
            origin=list(asset.get("origin") or [0.0, 0.0, 0.0]),
            created_ts=float(asset.get("created_ts") or _now_ts()),
            lifecycle_status=str(asset.get("lifecycle_status") or "available"),
            verification_status=str(asset.get("verification_status") or "verified"),
            save_snapshot_md5=str(asset.get("save_snapshot_md5") or ""),
            verified_runtime_map_id=str(asset.get("verified_runtime_map_id") or ""),
            verified_runtime_map_md5=str(asset.get("verified_runtime_map_md5") or ""),
            last_error_code=str(asset.get("last_error_code") or ""),
            last_error_msg=str(asset.get("last_error_msg") or ""),
            source_job_id=str(asset.get("source_job_id") or ""),
            revision_id=revision_id,
        )
        return revision_id

    def register_map_asset(
        self,
        *,
        map_name: str,
        yaml_path: str,
        pgm_path: str,
        map_id: str = "",
        map_md5: str = "",
        pbstream_path: str = "",
        frame_id: str = "map",
        resolution: float = 0.0,
        origin: Optional[List[float]] = None,
        display_name: str = "",
        description: str = "",
        enabled: bool = True,
        lifecycle_status: Optional[str] = None,
        verification_status: Optional[str] = None,
        save_snapshot_md5: Optional[str] = None,
        verified_runtime_map_id: Optional[str] = None,
        verified_runtime_map_md5: Optional[str] = None,
        last_error_code: Optional[str] = None,
        last_error_msg: Optional[str] = None,
        source_job_id: Optional[str] = None,
        revision_id: Optional[str] = None,
        robot_id: str = "local_robot",
        set_active: bool = True,
    ):
        resolved_revision_id = str(
            self.upsert_map_asset(
                map_name=map_name,
                display_name=display_name,
                enabled=enabled,
                description=description,
                map_id=map_id,
                map_md5=map_md5,
                yaml_path=yaml_path,
                pgm_path=pgm_path,
                pbstream_path=pbstream_path,
                frame_id=frame_id,
                resolution=resolution,
                origin=origin,
                lifecycle_status=lifecycle_status,
                verification_status=verification_status,
                save_snapshot_md5=save_snapshot_md5,
                verified_runtime_map_id=verified_runtime_map_id,
                verified_runtime_map_md5=verified_runtime_map_md5,
                last_error_code=last_error_code,
                last_error_msg=last_error_msg,
                source_job_id=source_job_id,
                revision_id=revision_id,
            )
            or ""
        ).strip()
        if set_active:
            if resolved_revision_id:
                self.set_active_map_revision(revision_id=resolved_revision_id, robot_id=robot_id)
            else:
                self.set_active_map(map_name=map_name, robot_id=robot_id)
        return resolved_revision_id

    def set_active_map(self, *, map_name: str, robot_id: str = "local_robot"):
        map_name = _normalize_map_name(map_name)
        if not map_name:
            raise ValueError("map_name is required")
        selected_revision = self._select_activatable_map_revision(
            map_name=map_name,
            robot_id=robot_id,
        )
        revision_id = str((selected_revision or {}).get("revision_id") or "").strip()
        if not revision_id:
            raise ValueError("map revision is unavailable for activation: %s" % map_name)
        self.set_active_map_revision(revision_id=revision_id, robot_id=robot_id)

    def _select_activatable_map_revision(
        self,
        *,
        map_name: str,
        robot_id: str = "local_robot",
    ) -> Dict[str, Any]:
        normalized_map_name = _normalize_map_name(map_name)
        if not normalized_map_name:
            raise ValueError("map_name is required")

        candidates: List[Dict[str, Any]] = []
        seen_revision_ids = set()

        def _append_candidate(asset: Optional[Dict[str, Any]]) -> None:
            asset = dict(asset or {})
            revision_id = str(asset.get("revision_id") or "").strip()
            if not revision_id or revision_id in seen_revision_ids:
                return
            seen_revision_ids.add(revision_id)
            candidates.append(asset)

        head_asset = self.resolve_map_asset(map_name=normalized_map_name, robot_id=robot_id) or {}
        head_revision_id = str(head_asset.get("revision_id") or "").strip()
        if head_revision_id:
            _append_candidate(self.resolve_map_revision(revision_id=head_revision_id, robot_id=robot_id))

        active_revision = self.get_active_map_revision(robot_id=robot_id) or {}
        if str(active_revision.get("map_name") or "").strip() == normalized_map_name:
            _append_candidate(active_revision)

        for row in self._fetch_rows(
            """
            SELECT *
            FROM map_revisions
            WHERE map_name=?
            ORDER BY activated_ts DESC, verified_ts DESC, created_ts DESC, revision_id DESC;
            """,
            (normalized_map_name,),
        ):
            _append_candidate(self._map_revision_row_to_dict(row))

        first_error = ""
        for candidate in candidates:
            error = map_asset_verification_error(candidate, label="map revision")
            if not error:
                return candidate
            if not first_error:
                first_error = error

        if first_error:
            raise ValueError(first_error)
        raise KeyError("map revision not found: %s" % normalized_map_name)

    def set_active_map_revision(self, *, revision_id: str, robot_id: str = "local_robot"):
        resolved = self.resolve_map_revision(revision_id=revision_id, robot_id=robot_id) or {}
        map_name = str(resolved.get("map_name") or "").strip()
        if not map_name:
            raise KeyError("map revision not found: %s" % str(revision_id or "").strip())
        activation_error = map_asset_verification_error(resolved, label="map revision")
        if activation_error:
            raise ValueError(activation_error)
        ts = _now_ts()
        self.conn.execute(
            """
            INSERT INTO robot_active_map(robot_id, map_name, updated_ts)
            VALUES(?,?,?)
            ON CONFLICT(robot_id) DO UPDATE SET
              map_name=excluded.map_name,
              updated_ts=excluded.updated_ts;
            """,
            (str(robot_id or "local_robot").strip() or "local_robot", map_name, ts),
        )
        self.conn.execute(
            """
            INSERT INTO robot_active_map_revision(robot_id, active_revision_id, updated_ts)
            VALUES(?,?,?)
            ON CONFLICT(robot_id) DO UPDATE SET
              active_revision_id=excluded.active_revision_id,
              updated_ts=excluded.updated_ts;
            """,
            (str(robot_id or "local_robot").strip() or "local_robot", str(revision_id or "").strip(), ts),
        )
        self.conn.execute(
            """
            UPDATE map_revisions
            SET activated_ts=?,
                updated_ts=?
            WHERE revision_id=?;
            """,
            (ts, ts, str(revision_id or "").strip()),
        )
        self.conn.commit()

    def upsert_pending_map_switch(
        self,
        *,
        robot_id: str = "local_robot",
        from_map_name: str = "",
        target_map_name: str,
        requested_activate: bool = True,
        status: str = "requested",
        last_error_code: str = "",
        last_error_msg: str = "",
    ):
        target_map_name = _normalize_map_name(target_map_name)
        if not target_map_name:
            raise ValueError("target_map_name is required")
        self.conn.execute(
            """
            INSERT INTO robot_pending_map_switch(
              robot_id, from_map_name, target_map_name, requested_activate, status,
              last_error_code, last_error_msg, updated_ts
            ) VALUES(?,?,?,?,?,?,?,?)
            ON CONFLICT(robot_id) DO UPDATE SET
              from_map_name=excluded.from_map_name,
              target_map_name=excluded.target_map_name,
              requested_activate=excluded.requested_activate,
              status=excluded.status,
              last_error_code=excluded.last_error_code,
              last_error_msg=excluded.last_error_msg,
              updated_ts=excluded.updated_ts;
            """,
            (
                str(robot_id or "local_robot").strip(),
                _normalize_map_name(from_map_name),
                target_map_name,
                1 if bool(requested_activate) else 0,
                str(status or "requested").strip() or "requested",
                str(last_error_code or "").strip(),
                str(last_error_msg or "").strip(),
                _now_ts(),
            ),
        )
        self.conn.commit()

    def upsert_pending_map_revision(
        self,
        *,
        robot_id: str = "local_robot",
        from_revision_id: str = "",
        target_revision_id: str,
        requested_activate: bool = True,
        status: str = "requested",
        last_error_code: str = "",
        last_error_msg: str = "",
    ):
        normalized_target_revision_id = str(target_revision_id or "").strip()
        if not normalized_target_revision_id:
            raise ValueError("target_revision_id is required")
        self.conn.execute(
            """
            INSERT INTO robot_pending_map_revision(
              robot_id, from_revision_id, target_revision_id, requested_activate, status,
              last_error_code, last_error_msg, updated_ts
            ) VALUES(?,?,?,?,?,?,?,?)
            ON CONFLICT(robot_id) DO UPDATE SET
              from_revision_id=excluded.from_revision_id,
              target_revision_id=excluded.target_revision_id,
              requested_activate=excluded.requested_activate,
              status=excluded.status,
              last_error_code=excluded.last_error_code,
              last_error_msg=excluded.last_error_msg,
              updated_ts=excluded.updated_ts;
            """,
            (
                str(robot_id or "local_robot").strip() or "local_robot",
                str(from_revision_id or "").strip(),
                normalized_target_revision_id,
                1 if bool(requested_activate) else 0,
                str(status or "requested").strip() or "requested",
                str(last_error_code or "").strip(),
                str(last_error_msg or "").strip(),
                _now_ts(),
            ),
        )
        self.conn.commit()

    def get_pending_map_switch(self, *, robot_id: str = "local_robot") -> Optional[Dict[str, Any]]:
        row = self.conn.execute(
            "SELECT * FROM robot_pending_map_switch WHERE robot_id=? LIMIT 1;",
            (str(robot_id or "local_robot").strip(),),
        ).fetchone()
        if not row:
            return None
        result = {
            "robot_id": str(row["robot_id"] or ""),
            "from_map_name": _normalize_map_name(str(row["from_map_name"] or "")),
            "target_map_name": _normalize_map_name(str(row["target_map_name"] or "")),
            "requested_activate": bool(int(row["requested_activate"] or 0)),
            "status": str(row["status"] or ""),
            "last_error_code": str(row["last_error_code"] or ""),
            "last_error_msg": str(row["last_error_msg"] or ""),
            "updated_ts": float(row["updated_ts"] or 0.0),
        }
        pending_revision = self.get_pending_map_revision(robot_id=robot_id)
        if pending_revision:
            result["from_revision_id"] = str(pending_revision.get("from_revision_id") or "")
            result["target_revision_id"] = str(pending_revision.get("target_revision_id") or "")
        return result

    def get_pending_map_revision(self, *, robot_id: str = "local_robot") -> Optional[Dict[str, Any]]:
        row = self.conn.execute(
            "SELECT * FROM robot_pending_map_revision WHERE robot_id=? LIMIT 1;",
            (str(robot_id or "local_robot").strip(),),
        ).fetchone()
        if not row:
            return None
        return {
            "robot_id": str(row["robot_id"] or ""),
            "from_revision_id": str(row["from_revision_id"] or ""),
            "target_revision_id": str(row["target_revision_id"] or ""),
            "requested_activate": bool(int(row["requested_activate"] or 0)),
            "status": str(row["status"] or ""),
            "last_error_code": str(row["last_error_code"] or ""),
            "last_error_msg": str(row["last_error_msg"] or ""),
            "updated_ts": float(row["updated_ts"] or 0.0),
        }

    def clear_pending_map_switch(self, *, robot_id: str = "local_robot"):
        self.conn.execute(
            "DELETE FROM robot_pending_map_switch WHERE robot_id=?;",
            (str(robot_id or "local_robot").strip(),),
        )
        if self._table_exists("robot_pending_map_revision"):
            self.conn.execute(
                "DELETE FROM robot_pending_map_revision WHERE robot_id=?;",
                (str(robot_id or "local_robot").strip(),),
            )
        self.conn.commit()

    def mark_map_asset_verification_result(
        self,
        *,
        map_name: str,
        revision_id: str = "",
        verification_status: Optional[str] = None,
        lifecycle_status: Optional[str] = None,
        runtime_map_id: Optional[str] = None,
        runtime_map_md5: Optional[str] = None,
        error_code: Optional[str] = None,
        error_msg: Optional[str] = None,
        promote_canonical_identity: bool = False,
    ) -> Dict[str, Any]:
        asset = self.resolve_map_asset(map_name=map_name, revision_id=revision_id) or {}
        if not asset:
            raise KeyError("map asset not found: %s" % (str(revision_id or map_name or "").strip() or "-"))

        resolved_map_name = str(asset.get("map_name") or map_name).strip()
        resolved_revision_id = str(asset.get("revision_id") or revision_id).strip()
        head_asset = self.resolve_map_asset(map_name=resolved_map_name) or {}
        head_revision_id = str(head_asset.get("revision_id") or "").strip()
        target_is_head_revision = (not resolved_revision_id) or (not head_revision_id) or (resolved_revision_id == head_revision_id)

        current_map_id = str(asset.get("map_id") or "").strip()
        current_map_md5 = str(asset.get("map_md5") or "").strip()
        normalized_runtime_map_md5 = str(runtime_map_md5 or "").strip()
        normalized_runtime_map_id = str(
            runtime_map_id or _map_id_from_md5(normalized_runtime_map_md5) or ""
        ).strip()

        rebound_asset = dict(asset)
        if promote_canonical_identity and (normalized_runtime_map_id or normalized_runtime_map_md5):
            needs_canonical_update = (not current_map_id) or (not current_map_md5) or (
                str(asset.get("verification_status") or "").strip().lower() != "verified"
            )
            if needs_canonical_update:
                if target_is_head_revision:
                    rebound_asset = self.rebind_map_identity(
                        map_name=resolved_map_name,
                        map_id=normalized_runtime_map_id,
                        map_md5=normalized_runtime_map_md5,
                        old_map_id=current_map_id,
                        old_map_md5=current_map_md5,
                    )
                else:
                    rebound_asset = dict(asset)
                    rebound_asset["map_id"] = normalized_runtime_map_id
                    rebound_asset["map_md5"] = normalized_runtime_map_md5

        common_kwargs = dict(
            map_name=str(rebound_asset.get("map_name") or resolved_map_name),
            display_name=str(rebound_asset.get("display_name") or rebound_asset.get("map_name") or resolved_map_name),
            enabled=bool(rebound_asset.get("enabled", True)),
            description=str(rebound_asset.get("description") or ""),
            map_id=str(rebound_asset.get("map_id") or ""),
            map_md5=str(rebound_asset.get("map_md5") or ""),
            yaml_path=str(rebound_asset.get("yaml_path") or ""),
            pgm_path=str(rebound_asset.get("pgm_path") or ""),
            pbstream_path=str(rebound_asset.get("pbstream_path") or ""),
            frame_id=str(rebound_asset.get("frame_id") or "map"),
            resolution=float(rebound_asset.get("resolution") or 0.0),
            origin=list(rebound_asset.get("origin") or [0.0, 0.0, 0.0]),
            created_ts=float(rebound_asset.get("created_ts") or _now_ts()),
            lifecycle_status=lifecycle_status,
            verification_status=verification_status,
            save_snapshot_md5=str(rebound_asset.get("save_snapshot_md5") or ""),
            verified_runtime_map_id=(
                normalized_runtime_map_id
                if runtime_map_id is not None or runtime_map_md5 is not None
                else str(rebound_asset.get("verified_runtime_map_id") or "")
            ),
            verified_runtime_map_md5=(
                normalized_runtime_map_md5
                if runtime_map_md5 is not None or runtime_map_id is not None
                else str(rebound_asset.get("verified_runtime_map_md5") or "")
            ),
            last_error_code=(
                str(error_code or "").strip()
                if error_code is not None
                else str(rebound_asset.get("last_error_code") or "")
            ),
            last_error_msg=(
                str(error_msg or "").strip()
                if error_msg is not None
                else str(rebound_asset.get("last_error_msg") or "")
            ),
            source_job_id=str(rebound_asset.get("source_job_id") or ""),
            revision_id=resolved_revision_id or None,
        )
        if target_is_head_revision:
            self.upsert_map_asset(**common_kwargs)
        else:
            self.upsert_map_revision(
                revision_id=str(resolved_revision_id or rebound_asset.get("revision_id") or ""),
                map_name=str(common_kwargs["map_name"] or resolved_map_name),
                display_name=str(common_kwargs["display_name"] or resolved_map_name),
                enabled=bool(common_kwargs["enabled"]),
                description=str(common_kwargs["description"] or ""),
                map_id=str(common_kwargs["map_id"] or ""),
                map_md5=str(common_kwargs["map_md5"] or ""),
                yaml_path=str(common_kwargs["yaml_path"] or ""),
                pgm_path=str(common_kwargs["pgm_path"] or ""),
                pbstream_path=str(common_kwargs["pbstream_path"] or ""),
                frame_id=str(common_kwargs["frame_id"] or "map"),
                resolution=float(common_kwargs["resolution"] or 0.0),
                origin=list(common_kwargs["origin"] or [0.0, 0.0, 0.0]),
                lifecycle_status=str(
                    common_kwargs["lifecycle_status"]
                    if common_kwargs["lifecycle_status"] is not None
                    else rebound_asset.get("lifecycle_status") or "available"
                ),
                verification_status=str(
                    common_kwargs["verification_status"]
                    if common_kwargs["verification_status"] is not None
                    else rebound_asset.get("verification_status") or "verified"
                ),
                live_snapshot_md5=str(common_kwargs["save_snapshot_md5"] or ""),
                verified_runtime_map_id=str(common_kwargs["verified_runtime_map_id"] or ""),
                verified_runtime_map_md5=str(common_kwargs["verified_runtime_map_md5"] or ""),
                last_error_code=str(common_kwargs["last_error_code"] or ""),
                last_error_msg=str(common_kwargs["last_error_msg"] or ""),
                source_job_id=str(common_kwargs["source_job_id"] or ""),
                created_ts=float(common_kwargs["created_ts"] or _now_ts()),
            )
        return (
            self.resolve_map_asset(revision_id=resolved_revision_id, map_name=str(common_kwargs["map_name"] or resolved_map_name))
            or self.resolve_map_asset(map_name=str(common_kwargs["map_name"] or resolved_map_name))
            or rebound_asset
        )

    def _asset_row_to_dict(self, row: sqlite3.Row, *, updated_ts: Optional[float] = None) -> Dict[str, Any]:
        map_name = _normalize_map_name(str(row["map_name"] or ""))
        revision_id = ""
        if "active_revision_id" in row.keys():
            revision_id = str(row["active_revision_id"] or "").strip()
        if (not revision_id) and ("current_revision_id" in row.keys()):
            revision_id = str(row["current_revision_id"] or "").strip()
        return {
            "map_name": map_name,
            "revision_id": revision_id,
            "display_name": str(row["display_name"] or map_name),
            "enabled": bool(int(row["enabled"] or 0)),
            "description": str(row["description"] or ""),
            "map_id": str(row["map_id"] or ""),
            "map_md5": str(row["map_md5"] or ""),
            "yaml_path": str(row["yaml_path"] or ""),
            "pgm_path": str(row["pgm_path"] or ""),
            "pbstream_path": str(row["pbstream_path"] or ""),
            "frame_id": str(row["frame_id"] or ""),
            "resolution": float(row["resolution"] or 0.0),
            "origin": _json_loads(str(row["origin_json"] or ""), [0.0, 0.0, 0.0]),
            "lifecycle_status": str(row["lifecycle_status"] or "available"),
            "verification_status": str(row["verification_status"] or "verified"),
            "save_snapshot_md5": str(row["save_snapshot_md5"] or ""),
            "verified_runtime_map_id": str(row["verified_runtime_map_id"] or ""),
            "verified_runtime_map_md5": str(row["verified_runtime_map_md5"] or ""),
            "last_error_code": str(row["last_error_code"] or ""),
            "last_error_msg": str(row["last_error_msg"] or ""),
            "source_job_id": str(row["source_job_id"] or ""),
            "created_ts": float(row["created_ts"] or 0.0),
            "updated_ts": float(updated_ts if updated_ts is not None else row["updated_ts"] or 0.0),
        }

    def get_active_map(self, robot_id: str = "local_robot") -> Optional[Dict[str, Any]]:
        normalized_robot_id = str(robot_id or "local_robot").strip()
        if self._table_exists("robot_active_map_revision"):
            row = self.conn.execute(
                """
                SELECT rar.robot_id, rar.updated_ts AS active_updated_ts, mr.*
                FROM robot_active_map_revision rar
                JOIN map_revisions mr ON mr.revision_id=rar.active_revision_id
                WHERE rar.robot_id=?
                LIMIT 1;
                """,
                (normalized_robot_id,),
            ).fetchone()
            if row:
                result = self._active_revision_row_to_asset_dict(
                    row,
                    updated_ts=float(row["active_updated_ts"] or 0.0),
                )
                result["robot_id"] = str(row["robot_id"] or "")
                return result

        row = self.conn.execute(
            """
            SELECT ram.robot_id, ram.updated_ts AS active_updated_ts, rar.active_revision_id, ma.*
            FROM robot_active_map ram
            JOIN map_assets ma ON ma.map_name=ram.map_name
            LEFT JOIN robot_active_map_revision rar ON rar.robot_id=ram.robot_id
            WHERE ram.robot_id=?
            LIMIT 1;
            """,
            (normalized_robot_id,),
        ).fetchone()
        if not row:
            return None
        result = self._asset_row_to_dict(row, updated_ts=float(row["active_updated_ts"] or 0.0))
        result["robot_id"] = str(row["robot_id"] or "")
        return result

    def get_active_map_revision(self, robot_id: str = "local_robot") -> Optional[Dict[str, Any]]:
        row = self.conn.execute(
            """
            SELECT rar.robot_id, rar.updated_ts AS active_updated_ts, mr.*
            FROM robot_active_map_revision rar
            JOIN map_revisions mr ON mr.revision_id=rar.active_revision_id
            WHERE rar.robot_id=?
            LIMIT 1;
            """,
            (str(robot_id or "local_robot").strip(),),
        ).fetchone()
        if not row:
            active = self.get_active_map(robot_id=robot_id) or {}
            revision_id = str(active.get("revision_id") or "").strip()
            return self.resolve_map_revision(revision_id=revision_id, robot_id=robot_id) if revision_id else None
        result = self._map_revision_row_to_dict(row, updated_ts=float(row["active_updated_ts"] or 0.0))
        result["robot_id"] = str(row["robot_id"] or "")
        return result

    def resolve_map_asset(
        self,
        *,
        map_name: str = "",
        revision_id: str = "",
        robot_id: str = "local_robot",
    ) -> Optional[Dict[str, Any]]:
        normalized_revision_id = str(revision_id or "").strip()
        if normalized_revision_id:
            row = self.conn.execute(
                "SELECT * FROM map_revisions WHERE revision_id=? LIMIT 1;",
                (normalized_revision_id,),
            ).fetchone()
            if not row:
                return None
            return self._active_revision_row_to_asset_dict(row)
        map_name = _normalize_map_name(map_name)
        if not map_name:
            return self.get_active_map(robot_id=robot_id)
        row = self.conn.execute(
            "SELECT * FROM map_assets WHERE map_name=? LIMIT 1;",
            (map_name,),
        ).fetchone()
        if not row:
            return None
        return self._asset_row_to_dict(row)

    def list_map_assets(self) -> List[Dict[str, Any]]:
        rows = self._fetch_rows("SELECT * FROM map_assets ORDER BY map_name ASC;")
        return [self._asset_row_to_dict(row) for row in rows]

    def list_map_assets_by_name(self, map_name: str = "") -> List[Dict[str, Any]]:
        map_name = _normalize_map_name(map_name)
        if map_name:
            row = self.resolve_map_asset(map_name=map_name)
            return [row] if row else []
        return self.list_map_assets()

    def update_map_asset_meta(
        self,
        *,
        map_name: str,
        display_name: Optional[str] = None,
        description: Optional[str] = None,
        enabled: Optional[bool] = None,
    ):
        row = self.conn.execute("SELECT * FROM map_assets WHERE map_name=? LIMIT 1;", (_normalize_map_name(map_name),)).fetchone()
        if not row:
            raise KeyError("map asset not found: %s" % map_name)
        self.upsert_map_asset(
            map_name=_normalize_map_name(map_name),
            display_name=str(display_name if display_name is not None else row["display_name"] or ""),
            enabled=bool(enabled if enabled is not None else int(row["enabled"] or 0)),
            description=str(description if description is not None else row["description"] or ""),
            map_id=str(row["map_id"] or ""),
            map_md5=str(row["map_md5"] or ""),
            yaml_path=str(row["yaml_path"] or ""),
            pgm_path=str(row["pgm_path"] or ""),
            pbstream_path=str(row["pbstream_path"] or ""),
            frame_id=str(row["frame_id"] or ""),
            resolution=float(row["resolution"] or 0.0),
            origin=_json_loads(str(row["origin_json"] or ""), [0.0, 0.0, 0.0]),
            created_ts=float(row["created_ts"] or _now_ts()),
            lifecycle_status=str(row["lifecycle_status"] or "available"),
            verification_status=str(row["verification_status"] or "verified"),
            save_snapshot_md5=str(row["save_snapshot_md5"] or ""),
            verified_runtime_map_id=str(row["verified_runtime_map_id"] or ""),
            verified_runtime_map_md5=str(row["verified_runtime_map_md5"] or ""),
            last_error_code=str(row["last_error_code"] or ""),
            last_error_msg=str(row["last_error_msg"] or ""),
            source_job_id=str(row["source_job_id"] or ""),
        )

    def update_map_revision_meta(
        self,
        *,
        revision_id: str = "",
        map_name: str = "",
        robot_id: str = "local_robot",
        display_name: Optional[str] = None,
        description: Optional[str] = None,
        enabled: Optional[bool] = None,
    ) -> Dict[str, Any]:
        target = None
        normalized_revision_id = str(revision_id or "").strip()
        normalized_map_name = _normalize_map_name(map_name)
        if normalized_revision_id:
            target = self.resolve_map_revision(revision_id=normalized_revision_id, robot_id=robot_id)
        elif normalized_map_name:
            target = self._preferred_map_scope_asset(map_name=normalized_map_name, robot_id=robot_id)
        if not target:
            raise KeyError("map revision not found: %s" % (normalized_revision_id or normalized_map_name or ""))

        target_revision_id = str(target.get("revision_id") or normalized_revision_id).strip()
        target_map_name = _normalize_map_name(str(target.get("map_name") or normalized_map_name))
        current = self.resolve_map_revision(revision_id=target_revision_id, robot_id=robot_id) or {}
        if not current:
            raise KeyError("map revision not found: %s" % (target_revision_id or target_map_name or ""))

        resolved_display_name = str(
            display_name if display_name is not None else current.get("display_name") or target_map_name
        ).strip() or target_map_name
        resolved_description = str(
            description if description is not None else current.get("description") or ""
        ).strip()
        resolved_enabled = bool(enabled if enabled is not None else current.get("enabled", True))

        self.upsert_map_revision(
            revision_id=target_revision_id,
            map_name=target_map_name,
            display_name=resolved_display_name,
            enabled=resolved_enabled,
            description=resolved_description,
            map_id=str(current.get("map_id") or ""),
            map_md5=str(current.get("map_md5") or ""),
            yaml_path=str(current.get("yaml_path") or ""),
            pgm_path=str(current.get("pgm_path") or ""),
            pbstream_path=str(current.get("pbstream_path") or ""),
            frame_id=str(current.get("frame_id") or "map"),
            resolution=float(current.get("resolution") or 0.0),
            origin=list(current.get("origin") or [0.0, 0.0, 0.0]),
            lifecycle_status=str(current.get("lifecycle_status") or "available"),
            verification_status=str(current.get("verification_status") or "verified"),
            live_snapshot_md5=str(current.get("live_snapshot_md5") or current.get("save_snapshot_md5") or ""),
            verified_runtime_map_id=str(current.get("verified_runtime_map_id") or ""),
            verified_runtime_map_md5=str(current.get("verified_runtime_map_md5") or ""),
            last_error_code=str(current.get("last_error_code") or ""),
            last_error_msg=str(current.get("last_error_msg") or ""),
            source_job_id=str(current.get("source_job_id") or ""),
            created_ts=float(current.get("created_ts") or _now_ts()),
            verified_ts=float(current.get("verified_ts") or 0.0),
            activated_ts=float(current.get("activated_ts") or 0.0),
        )

        head_asset = self.resolve_map_asset(map_name=target_map_name, robot_id=robot_id) or {}
        head_revision_id = str(head_asset.get("revision_id") or "").strip()
        if head_revision_id and head_revision_id == target_revision_id:
            self.upsert_map_asset(
                map_name=target_map_name,
                display_name=resolved_display_name,
                enabled=resolved_enabled,
                description=resolved_description,
                map_id=str(current.get("map_id") or ""),
                map_md5=str(current.get("map_md5") or ""),
                yaml_path=str(current.get("yaml_path") or ""),
                pgm_path=str(current.get("pgm_path") or ""),
                pbstream_path=str(current.get("pbstream_path") or ""),
                frame_id=str(current.get("frame_id") or "map"),
                resolution=float(current.get("resolution") or 0.0),
                origin=list(current.get("origin") or [0.0, 0.0, 0.0]),
                created_ts=float(current.get("created_ts") or _now_ts()),
                lifecycle_status=str(current.get("lifecycle_status") or "available"),
                verification_status=str(current.get("verification_status") or "verified"),
                save_snapshot_md5=str(current.get("live_snapshot_md5") or current.get("save_snapshot_md5") or ""),
                verified_runtime_map_id=str(current.get("verified_runtime_map_id") or ""),
                verified_runtime_map_md5=str(current.get("verified_runtime_map_md5") or ""),
                last_error_code=str(current.get("last_error_code") or ""),
                last_error_msg=str(current.get("last_error_msg") or ""),
                source_job_id=str(current.get("source_job_id") or ""),
                revision_id=target_revision_id,
            )

        return self.resolve_map_asset(revision_id=target_revision_id, robot_id=robot_id) or current

    def disable_map_asset(self, *, map_name: str):
        self.update_map_asset_meta(map_name=map_name, enabled=False)

    def disable_map_revision(
        self,
        *,
        revision_id: str = "",
        map_name: str = "",
        robot_id: str = "local_robot",
    ) -> Dict[str, Any]:
        return self.update_map_revision_meta(
            revision_id=revision_id,
            map_name=map_name,
            robot_id=robot_id,
            enabled=False,
        )

    def find_map_assets_by_identity(self, *, map_id: str = "", map_md5: str = "") -> List[Dict[str, Any]]:
        where = []
        args: List[Any] = []
        if str(map_id or "").strip():
            where.append("map_id=?")
            args.append(str(map_id or "").strip())
        if str(map_md5 or "").strip():
            where.append("map_md5=?")
            args.append(str(map_md5 or "").strip())
        if not where:
            return []
        rows = self._fetch_rows(
            "SELECT * FROM map_assets WHERE %s ORDER BY updated_ts DESC;" % (" AND ".join(where)),
            tuple(args),
        )
        return [self._asset_row_to_dict(row) for row in rows]

    def backfill_unscoped_map_scope(self, *, map_name: str, map_id: str = "", map_md5: str = ""):
        map_name = _normalize_map_name(map_name)
        if not map_name:
            raise ValueError("map_name is required")
        if not map_id and not map_md5:
            return
        map_revision_id = self.ensure_map_asset_revision(map_name=map_name)
        where = []
        args: List[Any] = []
        if map_id:
            where.append("map_id=?")
            args.append(str(map_id))
        if map_md5:
            where.append("map_md5=?")
            args.append(str(map_md5))
        pred = " AND ".join(where)
        cur = self.conn.cursor()
        cur.execute(
            "UPDATE plans SET map_name=?, map_revision_id=? WHERE (%s) AND (map_name='' OR map_name IS NULL OR map_revision_id='' OR map_revision_id IS NULL);" % pred,
            (map_name, map_revision_id, *args),
        )
        cur.execute(
            "UPDATE zone_versions SET map_name=?, map_revision_id=? WHERE (%s) AND (map_name='' OR map_name IS NULL OR map_revision_id='' OR map_revision_id IS NULL);" % pred,
            (map_name, map_revision_id, *args),
        )
        cur.execute(
            """
            UPDATE zones
            SET map_name=?, map_revision_id=?
            WHERE (map_name='' OR map_name IS NULL OR map_revision_id='' OR map_revision_id IS NULL)
              AND EXISTS (
                SELECT 1 FROM zone_versions zv
                WHERE zv.zone_id=zones.zone_id
                  AND zv.zone_version=zones.current_zone_version
                  AND (%s)
              );
            """ % pred,
            (map_name, map_revision_id, *args),
        )
        self.conn.commit()

    def rebind_map_identity(
        self,
        *,
        map_name: str,
        map_id: str = "",
        map_md5: str = "",
        old_map_id: str = "",
        old_map_md5: str = "",
    ) -> Dict[str, Any]:
        map_name = _normalize_map_name(map_name)
        if not map_name:
            raise ValueError("map_name is required")

        row = self.conn.execute(
            "SELECT * FROM map_assets WHERE map_name=? LIMIT 1;",
            (map_name,),
        ).fetchone()
        if not row:
            raise KeyError("map asset not found: %s" % map_name)

        current = self._asset_row_to_dict(row)
        previous_map_id = str(old_map_id or current.get("map_id") or "").strip()
        previous_map_md5 = str(old_map_md5 or current.get("map_md5") or "").strip()
        target_map_md5 = str(map_md5 or previous_map_md5).strip()
        target_map_id = str(map_id or _map_id_from_md5(target_map_md5) or previous_map_id).strip()
        if not target_map_id and not target_map_md5:
            raise ValueError("map_id or map_md5 is required")

        if previous_map_id or previous_map_md5:
            self.backfill_unscoped_map_scope(
                map_name=map_name,
                map_id=previous_map_id,
                map_md5=previous_map_md5,
            )

        ts = _now_ts()
        cur = self.conn.cursor()
        cur.execute("BEGIN IMMEDIATE;")
        try:
            cur.execute(
                """
                UPDATE map_assets
                SET map_id=?, map_md5=?, updated_ts=?
                WHERE map_name=?;
                """,
                (target_map_id, target_map_md5, ts, map_name),
            )
            cur.execute(
                """
                UPDATE zone_versions
                SET map_id=?, map_md5=?
                WHERE map_name=?;
                """,
                (target_map_id, target_map_md5, map_name),
            )
            cur.execute(
                """
                UPDATE plans
                SET map_id=?, map_md5=?
                WHERE map_name=?;
                """,
                (target_map_id, target_map_md5, map_name),
            )
            cur.execute(
                """
                UPDATE map_revisions
                SET map_id=?, map_md5=?, updated_ts=?
                WHERE map_name=?;
                """,
                (target_map_id, target_map_md5, ts, map_name),
            )
            cur.execute(
                """
                UPDATE map_alignment_configs
                SET map_id=?, map_version=?, updated_ts=?
                WHERE map_name=?;
                """,
                (target_map_id, target_map_md5, ts, map_name),
            )
            cur.execute(
                """
                UPDATE map_alignment_revision_configs
                SET map_id=?, map_version=?, updated_ts=?
                WHERE map_revision_id IN (SELECT revision_id FROM map_revisions WHERE map_name=?);
                """,
                (target_map_id, target_map_md5, ts, map_name),
            )

            if previous_map_id and previous_map_id != target_map_id:
                cur.execute(
                    """
                    INSERT OR REPLACE INTO map_constraint_versions(
                      map_id, constraint_version, map_md5, created_ts, updated_ts
                    )
                    SELECT ?, constraint_version, ?, created_ts, ?
                    FROM map_constraint_versions
                    WHERE map_id=?;
                    """,
                    (target_map_id, target_map_md5, ts, previous_map_id),
                )
                cur.execute("DELETE FROM map_constraint_versions WHERE map_id=?;", (previous_map_id,))

                cur.execute(
                    """
                    INSERT OR REPLACE INTO map_no_go_areas(
                      map_id, constraint_version, area_id, name, polygon_json, enabled, updated_ts
                    )
                    SELECT ?, constraint_version, area_id, name, polygon_json, enabled, ?
                    FROM map_no_go_areas
                    WHERE map_id=?;
                    """,
                    (target_map_id, ts, previous_map_id),
                )
                cur.execute("DELETE FROM map_no_go_areas WHERE map_id=?;", (previous_map_id,))

                cur.execute(
                    """
                    INSERT OR REPLACE INTO map_virtual_walls(
                      map_id, constraint_version, wall_id, name, polyline_json, buffer_m, enabled, updated_ts
                    )
                    SELECT ?, constraint_version, wall_id, name, polyline_json, buffer_m, enabled, ?
                    FROM map_virtual_walls
                    WHERE map_id=?;
                    """,
                    (target_map_id, ts, previous_map_id),
                )
                cur.execute("DELETE FROM map_virtual_walls WHERE map_id=?;", (previous_map_id,))

                cur.execute(
                    """
                    INSERT OR REPLACE INTO map_active_constraint_versions(
                      map_id, active_constraint_version, updated_ts
                    )
                    SELECT ?, active_constraint_version, ?
                    FROM map_active_constraint_versions
                    WHERE map_id=?;
                    """,
                    (target_map_id, ts, previous_map_id),
                )
                cur.execute("DELETE FROM map_active_constraint_versions WHERE map_id=?;", (previous_map_id,))
            else:
                if target_map_id:
                    cur.execute(
                        """
                        UPDATE map_constraint_versions
                        SET map_md5=?, updated_ts=?
                        WHERE map_id=?;
                        """,
                        (target_map_md5, ts, target_map_id),
                    )
            cur.execute(
                """
                UPDATE map_constraint_revision_versions
                SET map_id=?, map_md5=?, updated_ts=?
                WHERE map_revision_id IN (SELECT revision_id FROM map_revisions WHERE map_name=?);
                """,
                (target_map_id, target_map_md5, ts, map_name),
            )

            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

        rebound = self.resolve_map_asset(map_name=map_name) or {}
        if rebound:
            return rebound
        return {
            **current,
            "map_name": map_name,
            "map_id": target_map_id,
            "map_md5": target_map_md5,
            "updated_ts": ts,
        }

    def _upsert_zone_version_cur(
        self,
        cur: sqlite3.Cursor,
        *,
        map_name: str,
        map_revision_id: str = "",
        zone_id: str,
        zone_version: int,
        frame_id: str,
        outer: List[Tuple[float, float]],
        holes: List[List[Tuple[float, float]]],
        map_id: str = "",
        map_md5: str = "",
    ):
        map_name, map_revision_id = self._resolve_map_scope(
            map_name=map_name,
            map_revision_id=map_revision_id,
        )
        if not map_name:
            raise ValueError("map_name is required for zone persistence")
        if not map_revision_id:
            raise ValueError("map_revision_id is required for zone persistence")
        ts = _now_ts()
        cur.execute(
            """
            INSERT INTO zone_versions(
              map_revision_id, map_name, zone_id, zone_version, frame_id, outer_json, holes_json, map_id, map_md5, created_ts
            )
            VALUES(?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(map_revision_id, zone_id, zone_version) DO UPDATE SET
              map_name=excluded.map_name,
              frame_id=excluded.frame_id,
              outer_json=excluded.outer_json,
              holes_json=excluded.holes_json,
              map_id=excluded.map_id,
              map_md5=excluded.map_md5;
            """,
            (
                map_revision_id,
                map_name,
                str(zone_id or "").strip(),
                int(zone_version),
                str(frame_id or "").strip(),
                _json_dumps([[float(x), float(y)] for x, y in (outer or [])]),
                _json_dumps([[[float(x), float(y)] for x, y in ring] for ring in (holes or [])]),
                str(map_id or "").strip(),
                str(map_md5 or "").strip(),
                ts,
            ),
        )

    def _upsert_zone_record_cur(
        self,
        cur: sqlite3.Cursor,
        *,
        map_name: str,
        map_revision_id: str = "",
        zone_id: str,
        zone_version: int,
        display_name: str = "",
        enabled: bool = True,
    ):
        map_name, map_revision_id = self._resolve_map_scope(
            map_name=map_name,
            map_revision_id=map_revision_id,
        )
        if not map_name:
            raise ValueError("map_name is required for zone persistence")
        if not map_revision_id:
            raise ValueError("map_revision_id is required for zone persistence")
        ts = _now_ts()
        cur.execute(
            """
            INSERT INTO zones(map_revision_id, map_name, zone_id, display_name, enabled, current_zone_version, updated_ts)
            VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(map_revision_id, zone_id) DO UPDATE SET
              map_name=excluded.map_name,
              display_name=excluded.display_name,
              enabled=excluded.enabled,
              current_zone_version=excluded.current_zone_version,
              updated_ts=excluded.updated_ts;
            """,
            (
                map_revision_id,
                map_name,
                str(zone_id or "").strip(),
                str(display_name or zone_id or "").strip(),
                1 if bool(enabled) else 0,
                int(zone_version),
                ts,
            ),
        )

    def _upsert_zone_version(
        self,
        *,
        map_name: str,
        map_revision_id: str = "",
        zone_id: str,
        zone_version: int,
        frame_id: str,
        outer: List[Tuple[float, float]],
        holes: List[List[Tuple[float, float]]],
        map_id: str = "",
        map_md5: str = "",
    ):
        cur = self.conn.cursor()
        self._upsert_zone_version_cur(
            cur,
            map_name=map_name,
            map_revision_id=map_revision_id,
            zone_id=zone_id,
            zone_version=zone_version,
            frame_id=frame_id,
            outer=outer,
            holes=holes,
            map_id=map_id,
            map_md5=map_md5,
        )
        self._upsert_zone_record_cur(
            cur,
            map_name=map_name,
            map_revision_id=map_revision_id,
            zone_id=zone_id,
            zone_version=zone_version,
            display_name=str(zone_id or "").strip(),
            enabled=True,
        )
        self.conn.commit()

    def _insert_plan_cur(
        self,
        cur: sqlite3.Cursor,
        *,
        plan_id: str,
        map_name: str,
        map_revision_id: str = "",
        zone_id: str,
        zone_version: int,
        frame_id: str,
        plan_profile_name: str,
        params: Dict[str, Any],
        robot_spec: Dict[str, Any],
        plan_result: Any,
        map_id: str = "",
        map_md5: str = "",
        constraint_version: str = "",
        planner_version: str = "",
        created_ts: Optional[float] = None,
    ) -> None:
        created_ts = float(created_ts or _now_ts())
        params_json = _json_dumps(params or {})
        robot_json = _json_dumps(robot_spec or {})
        blocks = getattr(plan_result, "blocks", []) or []
        exec_order = getattr(plan_result, "exec_order", []) or []
        exec_order_json = _json_dumps([int(i) for i in exec_order])
        total_len = getattr(plan_result, "total_length_m", None)
        if total_len is None:
            total_len = 0.0
        map_name, map_revision_id = self._resolve_map_scope(
            map_name=map_name,
            map_revision_id=map_revision_id,
        )
        if not map_name:
            raise ValueError("map_name is required for plan persistence")
        if not map_revision_id:
            raise ValueError("map_revision_id is required for plan persistence")

        cur.execute(
            """
            INSERT INTO plans(
              plan_id, map_revision_id, map_name, zone_id, zone_version, frame_id, plan_profile_name,
              params_json, robot_json, blocks, total_length_m, exec_order_json,
              map_id, map_md5, constraint_version, planner_version, created_ts
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
            """,
            (
                str(plan_id or "").strip(),
                map_revision_id,
                map_name,
                str(zone_id or "").strip(),
                int(zone_version),
                str(frame_id or "").strip(),
                str(plan_profile_name or "").strip() or "cover_standard",
                params_json,
                robot_json,
                int(len(blocks)),
                float(total_len),
                exec_order_json,
                str(map_id or "").strip(),
                str(map_md5 or "").strip(),
                str(constraint_version or "").strip(),
                str(planner_version or "").strip(),
                float(created_ts),
            ),
        )

        sum_len = 0.0
        for b in blocks:
            block_id = int(getattr(b, "block_id", 0))
            path_xyyaw = getattr(b, "path_xyyaw", None)
            if path_xyyaw is None:
                path_xy = getattr(b, "path_xy", None)
                if path_xy is None:
                    raise ValueError("block missing path_xy/path_xyyaw")
                path_xyyaw = xy_to_xyyaw(path_xy)

            blob = encode_path_xyyaw_f32(path_xyyaw)
            pts = list(path_xyyaw)
            point_count = int(len(pts))
            length_m = polyline_length_xy([(p[0], p[1]) for p in pts])
            sum_len += length_m

            entry = getattr(b, "entry_xyyaw", None)
            exitp = getattr(b, "exit_xyyaw", None)
            if entry is None and point_count > 0:
                entry = pts[0]
            if exitp is None and point_count > 0:
                exitp = pts[-1]

            cur.execute(
                """
                INSERT INTO plan_blocks(
                  plan_id, block_id, entry_x, entry_y, entry_yaw, exit_x, exit_y, exit_yaw,
                  point_count, length_m, path_step_m, path_blob
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?);
                """,
                (
                    str(plan_id or "").strip(),
                    block_id,
                    float(entry[0]) if entry else None,
                    float(entry[1]) if entry else None,
                    float(entry[2]) if entry else None,
                    float(exitp[0]) if exitp else None,
                    float(exitp[1]) if exitp else None,
                    float(exitp[2]) if exitp else None,
                    point_count,
                    float(length_m),
                    float(params.get("path_step_m", 0.0)) if isinstance(params, dict) else None,
                    sqlite3.Binary(blob),
                ),
            )

        if getattr(plan_result, "total_length_m", None) is None:
            cur.execute("UPDATE plans SET total_length_m=? WHERE plan_id=?;", (float(sum_len), str(plan_id or "").strip()))

    def save_plan(
        self,
        zone_id: str,
        zone_version: int,
        frame_id: str,
        plan_profile_name: str,
        params: Dict[str, Any],
        robot_spec: Dict[str, Any],
        outer: List[Tuple[float, float]],
        holes: List[List[Tuple[float, float]]],
        plan_result: Any,
        map_name: str = "",
        map_revision_id: str = "",
        map_id: str = "",
        map_md5: str = "",
        constraint_version: str = "",
        planner_version: str = "",
    ) -> str:
        map_name, map_revision_id = self._resolve_map_scope(
            map_name=map_name,
            map_revision_id=map_revision_id,
        )
        if not map_name:
            raise ValueError("map_name is required for save_plan")
        plan_id = str(uuid.uuid4())
        created_ts = _now_ts()

        cur = self.conn.cursor()
        try:
            self._upsert_plan_profile_cur(cur, plan_profile_name=str(plan_profile_name or "").strip() or "cover_standard")
            self._upsert_zone_version_cur(
                cur,
                map_name=map_name,
                map_revision_id=map_revision_id,
                zone_id=str(zone_id),
                zone_version=int(zone_version),
                frame_id=str(frame_id),
                outer=outer or [],
                holes=holes or [],
                map_id=str(map_id or "").strip(),
                map_md5=str(map_md5 or "").strip(),
            )
            self._upsert_zone_record_cur(
                cur,
                map_name=map_name,
                map_revision_id=map_revision_id,
                zone_id=str(zone_id),
                zone_version=int(zone_version),
                display_name=str(zone_id or "").strip(),
                enabled=True,
            )
            self._insert_plan_cur(
                cur,
                plan_id=plan_id,
                map_name=map_name,
                map_revision_id=map_revision_id,
                zone_id=str(zone_id),
                zone_version=int(zone_version),
                frame_id=str(frame_id),
                plan_profile_name=str(plan_profile_name or "").strip() or "cover_standard",
                params=params,
                robot_spec=robot_spec,
                plan_result=plan_result,
                map_id=str(map_id or "").strip(),
                map_md5=str(map_md5 or "").strip(),
                constraint_version=str(constraint_version or "").strip(),
                planner_version=str(planner_version or "").strip(),
                created_ts=created_ts,
            )
            self.conn.commit()
            return plan_id
        except Exception:
            self.conn.rollback()
            raise

    def set_active_plan(
        self,
        zone_id: str,
        zone_version: int,
        plan_id: str,
        *,
        map_name: str = "",
        map_revision_id: str = "",
        plan_profile_name: str,
        frame_id: Optional[str] = None,
        outer: Optional[list] = None,
        holes: Optional[list] = None,
        map_id: str = "",
        map_md5: str = "",
        display_name: Optional[str] = None,
    ):
        map_name, map_revision_id = self._resolve_map_scope(
            map_name=map_name,
            map_revision_id=map_revision_id,
        )
        if not map_name:
            raise ValueError("map_name is required for set_active_plan")
        plan_profile_name = str(plan_profile_name or "").strip() or "cover_standard"
        cur = self.conn.cursor()
        if frame_id is not None and outer is not None and holes is not None:
            self._upsert_zone_version_cur(
                cur,
                map_name=map_name,
                map_revision_id=map_revision_id,
                zone_id=str(zone_id),
                zone_version=int(zone_version),
                frame_id=str(frame_id or ""),
                outer=outer or [],
                holes=holes or [],
                map_id=str(map_id or "").strip(),
                map_md5=str(map_md5 or "").strip(),
            )
        self._upsert_zone_record_cur(
            cur,
            map_name=map_name,
            map_revision_id=map_revision_id,
            zone_id=str(zone_id),
            zone_version=int(zone_version),
            display_name=str(display_name or zone_id or "").strip(),
            enabled=True,
        )
        ts = _now_ts()
        cur.execute(
            """
            INSERT INTO zone_active_plans(map_revision_id, map_name, zone_id, plan_profile_name, active_plan_id, updated_ts)
            VALUES(?,?,?,?,?,?)
            ON CONFLICT(map_revision_id, zone_id, plan_profile_name) DO UPDATE SET
              map_name=excluded.map_name,
              active_plan_id=excluded.active_plan_id,
              updated_ts=excluded.updated_ts;
            """,
            (
                map_revision_id,
                map_name,
                str(zone_id or "").strip(),
                plan_profile_name,
                str(plan_id or "").strip(),
                ts,
            ),
        )
        self.conn.commit()

    def commit_zone_submission(
        self,
        *,
        map_name: str,
        map_revision_id: str = "",
        zone_id: str,
        zone_version: int,
        frame_id: str,
        display_name: str,
        plan_profile_name: str,
        params: Dict[str, Any],
        robot_spec: Dict[str, Any],
        outer: List[Tuple[float, float]],
        holes: List[List[Tuple[float, float]]],
        plan_result: Any,
        map_id: str = "",
        map_md5: str = "",
        constraint_version: str = "",
        planner_version: str = "",
        set_active_plan: bool = True,
        publish_zone_record: bool = True,
        alignment_version: str = "",
        display_frame: str = "",
        display_outer: Optional[List[Tuple[float, float]]] = None,
        display_holes: Optional[List[List[Tuple[float, float]]]] = None,
        estimated_length_m: float = 0.0,
        estimated_duration_s: float = 0.0,
        warnings: Optional[List[str]] = None,
    ) -> str:
        map_name, map_revision_id = self._resolve_map_scope(
            map_name=map_name,
            map_revision_id=map_revision_id,
        )
        if not map_name:
            raise ValueError("map_name is required for commit_zone_submission")
        zone_id = str(zone_id or "").strip()
        if not zone_id:
            raise ValueError("zone_id is required for commit_zone_submission")
        plan_profile_name = str(plan_profile_name or "").strip() or "cover_standard"
        plan_id = str(uuid.uuid4())
        created_ts = _now_ts()
        cur = self.conn.cursor()
        try:
            self._upsert_plan_profile_cur(cur, plan_profile_name=plan_profile_name)
            self._upsert_zone_version_cur(
                cur,
                map_name=map_name,
                map_revision_id=map_revision_id,
                zone_id=zone_id,
                zone_version=int(zone_version),
                frame_id=str(frame_id or "").strip(),
                outer=outer or [],
                holes=holes or [],
                map_id=str(map_id or "").strip(),
                map_md5=str(map_md5 or "").strip(),
            )
            if bool(publish_zone_record):
                self._upsert_zone_record_cur(
                    cur,
                    map_name=map_name,
                    map_revision_id=map_revision_id,
                    zone_id=zone_id,
                    zone_version=int(zone_version),
                    display_name=str(display_name or zone_id).strip(),
                    enabled=True,
                )
            self._insert_plan_cur(
                cur,
                plan_id=plan_id,
                map_name=map_name,
                map_revision_id=map_revision_id,
                zone_id=zone_id,
                zone_version=int(zone_version),
                frame_id=str(frame_id or "").strip(),
                plan_profile_name=plan_profile_name,
                params=params,
                robot_spec=robot_spec,
                plan_result=plan_result,
                map_id=str(map_id or "").strip(),
                map_md5=str(map_md5 or "").strip(),
                constraint_version=str(constraint_version or "").strip(),
                planner_version=str(planner_version or "").strip(),
                created_ts=created_ts,
            )
            if bool(set_active_plan):
                ts = _now_ts()
                cur.execute(
                    """
                    INSERT INTO zone_active_plans(map_revision_id, map_name, zone_id, plan_profile_name, active_plan_id, updated_ts)
                    VALUES(?,?,?,?,?,?)
                    ON CONFLICT(map_revision_id, zone_id, plan_profile_name) DO UPDATE SET
                      map_name=excluded.map_name,
                      active_plan_id=excluded.active_plan_id,
                      updated_ts=excluded.updated_ts;
                    """,
                    (
                        map_revision_id,
                        map_name,
                        zone_id,
                        plan_profile_name,
                        plan_id,
                        ts,
                    ),
                )
            cur.execute(
                """
                INSERT INTO zone_editor_metadata(
                  map_revision_id, map_name, zone_id, zone_version, alignment_version, display_frame, display_outer_json,
                  display_holes_json, profile_name, preview_plan_id, estimated_length_m, estimated_duration_s,
                  warnings_json, updated_ts
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(map_revision_id, zone_id, zone_version) DO UPDATE SET
                  map_name=excluded.map_name,
                  alignment_version=excluded.alignment_version,
                  display_frame=excluded.display_frame,
                  display_outer_json=excluded.display_outer_json,
                  display_holes_json=excluded.display_holes_json,
                  profile_name=excluded.profile_name,
                  preview_plan_id=excluded.preview_plan_id,
                  estimated_length_m=excluded.estimated_length_m,
                  estimated_duration_s=excluded.estimated_duration_s,
                  warnings_json=excluded.warnings_json,
                  updated_ts=excluded.updated_ts;
                """,
                (
                    map_revision_id,
                    map_name,
                    zone_id,
                    int(zone_version),
                    str(alignment_version or "").strip(),
                    str(display_frame or "").strip(),
                    _json_dumps([[float(x), float(y)] for x, y in (display_outer or [])]),
                    _json_dumps([[[float(x), float(y)] for x, y in ring] for ring in (display_holes or [])]),
                    plan_profile_name,
                    plan_id,
                    float(estimated_length_m or 0.0),
                    float(estimated_duration_s or 0.0),
                    _json_dumps([str(w) for w in (warnings or [])]),
                    _now_ts(),
                ),
            )
            self.conn.commit()
            return plan_id
        except Exception:
            self.conn.rollback()
            raise

    def get_active_constraint_version(self, map_id: str, map_revision_id: str = "") -> Optional[str]:
        normalized_revision_id = str(map_revision_id or "").strip()
        if normalized_revision_id:
            row = self.conn.execute(
                """
                SELECT active_constraint_version
                FROM map_active_constraint_revisions
                WHERE map_revision_id=?
                LIMIT 1;
                """,
                (normalized_revision_id,),
            ).fetchone()
            if row:
                return str(row["active_constraint_version"] or "") or None
        row = self.conn.execute(
            """
            SELECT active_constraint_version
            FROM map_active_constraint_versions
            WHERE map_id=?
            LIMIT 1;
            """,
            (str(map_id or "").strip(),),
        ).fetchone()
        if not row:
            return None
        return str(row["active_constraint_version"] or "") or None

    def set_active_constraint_version(self, map_id: str, constraint_version: str, map_revision_id: str = ""):
        ts = _now_ts()
        normalized_revision_id = str(map_revision_id or "").strip()
        if normalized_revision_id:
            self.conn.execute(
                """
                INSERT INTO map_active_constraint_revisions(map_revision_id, active_constraint_version, updated_ts)
                VALUES(?,?,?)
                ON CONFLICT(map_revision_id) DO UPDATE SET
                  active_constraint_version=excluded.active_constraint_version,
                  updated_ts=excluded.updated_ts;
                """,
                (normalized_revision_id, str(constraint_version or "").strip(), ts),
            )
        else:
            self.conn.execute(
                """
                INSERT INTO map_active_constraint_versions(map_id, active_constraint_version, updated_ts)
                VALUES(?,?,?)
                ON CONFLICT(map_id) DO UPDATE SET
                  active_constraint_version=excluded.active_constraint_version,
                  updated_ts=excluded.updated_ts;
                """,
                (str(map_id or "").strip(), str(constraint_version or "").strip(), ts),
            )
        self.conn.commit()

    def ensure_active_constraint_version(self, map_id: str, *, map_md5: str = "", map_revision_id: str = "") -> str:
        map_id = str(map_id or "").strip()
        if not map_id:
            raise ValueError("map_id is required for constraint version lookup")
        normalized_revision_id = str(map_revision_id or "").strip()
        existing = self.get_active_constraint_version(map_id, normalized_revision_id)
        if existing:
            return str(existing)
        version = uuid.uuid4().hex
        ts = _now_ts()
        cur = self.conn.cursor()
        if normalized_revision_id:
            cur.execute(
                """
                INSERT INTO map_constraint_revision_versions(map_revision_id, map_id, constraint_version, map_md5, created_ts, updated_ts)
                VALUES(?,?,?,?,?,?);
                """,
                (normalized_revision_id, map_id, version, str(map_md5 or "").strip(), ts, ts),
            )
            cur.execute(
                """
                INSERT INTO map_active_constraint_revisions(map_revision_id, active_constraint_version, updated_ts)
                VALUES(?,?,?);
                """,
                (normalized_revision_id, version, ts),
            )
        else:
            cur.execute(
                """
                INSERT INTO map_constraint_versions(map_id, constraint_version, map_md5, created_ts, updated_ts)
                VALUES(?,?,?,?,?);
                """,
                (map_id, version, str(map_md5 or "").strip(), ts, ts),
            )
            cur.execute(
                """
                INSERT INTO map_active_constraint_versions(map_id, active_constraint_version, updated_ts)
                VALUES(?,?,?);
                """,
                (map_id, version, ts),
            )
        self.conn.commit()
        return version

    def replace_map_constraints(
        self,
        *,
        map_id: str,
        map_revision_id: str = "",
        map_md5: str = "",
        no_go_areas: List[Dict[str, Any]],
        virtual_walls: List[Dict[str, Any]],
        constraint_version: str = "",
    ) -> str:
        map_id = str(map_id or "").strip()
        if not map_id:
            raise ValueError("map_id is required")

        normalized_revision_id = str(map_revision_id or "").strip()
        version = str(constraint_version or "").strip() or uuid.uuid4().hex
        ts = _now_ts()
        cur = self.conn.cursor()
        try:
            if normalized_revision_id:
                cur.execute(
                    """
                    INSERT INTO map_constraint_revision_versions(map_revision_id, map_id, constraint_version, map_md5, created_ts, updated_ts)
                    VALUES(?,?,?,?,?,?);
                    """,
                    (normalized_revision_id, map_id, version, str(map_md5 or "").strip(), ts, ts),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO map_constraint_versions(map_id, constraint_version, map_md5, created_ts, updated_ts)
                    VALUES(?,?,?,?,?);
                    """,
                    (map_id, version, str(map_md5 or "").strip(), ts, ts),
                )

            for idx, area in enumerate(no_go_areas or []):
                area_id = str(area.get("area_id") or area.get("id") or ("no_go_%d" % idx)).strip()
                polygon = area.get("polygon") or area.get("points") or area.get("outer") or []
                if normalized_revision_id:
                    cur.execute(
                        """
                        INSERT INTO map_revision_no_go_areas(
                          map_revision_id, constraint_version, area_id, name, polygon_json, enabled, updated_ts
                        ) VALUES(?,?,?,?,?,?,?);
                        """,
                        (
                            normalized_revision_id,
                            version,
                            area_id,
                            str(area.get("name") or "").strip(),
                            _json_dumps(polygon),
                            1 if bool(area.get("enabled", True)) else 0,
                            ts,
                        ),
                    )
                else:
                    cur.execute(
                        """
                        INSERT INTO map_no_go_areas(
                          map_id, constraint_version, area_id, name, polygon_json, enabled, updated_ts
                        ) VALUES(?,?,?,?,?,?,?);
                        """,
                        (
                            map_id,
                            version,
                            area_id,
                            str(area.get("name") or "").strip(),
                            _json_dumps(polygon),
                            1 if bool(area.get("enabled", True)) else 0,
                            ts,
                        ),
                    )

            for idx, wall in enumerate(virtual_walls or []):
                wall_id = str(wall.get("wall_id") or wall.get("id") or ("virtual_wall_%d" % idx)).strip()
                polyline = wall.get("polyline") or wall.get("points") or []
                if normalized_revision_id:
                    cur.execute(
                        """
                        INSERT INTO map_revision_virtual_walls(
                          map_revision_id, constraint_version, wall_id, name, polyline_json, buffer_m, enabled, updated_ts
                        ) VALUES(?,?,?,?,?,?,?,?);
                        """,
                        (
                            normalized_revision_id,
                            version,
                            wall_id,
                            str(wall.get("name") or "").strip(),
                            _json_dumps(polyline),
                            float(wall.get("buffer_m", DEFAULT_VIRTUAL_WALL_BUFFER_M) or DEFAULT_VIRTUAL_WALL_BUFFER_M),
                            1 if bool(wall.get("enabled", True)) else 0,
                            ts,
                        ),
                    )
                else:
                    cur.execute(
                        """
                        INSERT INTO map_virtual_walls(
                          map_id, constraint_version, wall_id, name, polyline_json, buffer_m, enabled, updated_ts
                        ) VALUES(?,?,?,?,?,?,?,?);
                        """,
                        (
                            map_id,
                            version,
                            wall_id,
                            str(wall.get("name") or "").strip(),
                            _json_dumps(polyline),
                            float(wall.get("buffer_m", DEFAULT_VIRTUAL_WALL_BUFFER_M) or DEFAULT_VIRTUAL_WALL_BUFFER_M),
                            1 if bool(wall.get("enabled", True)) else 0,
                            ts,
                        ),
                    )

            if normalized_revision_id:
                cur.execute(
                    """
                    INSERT INTO map_active_constraint_revisions(map_revision_id, active_constraint_version, updated_ts)
                    VALUES(?,?,?)
                    ON CONFLICT(map_revision_id) DO UPDATE SET
                      active_constraint_version=excluded.active_constraint_version,
                      updated_ts=excluded.updated_ts;
                    """,
                    (normalized_revision_id, version, ts),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO map_active_constraint_versions(map_id, active_constraint_version, updated_ts)
                    VALUES(?,?,?)
                    ON CONFLICT(map_id) DO UPDATE SET
                      active_constraint_version=excluded.active_constraint_version,
                      updated_ts=excluded.updated_ts;
                    """,
                    (map_id, version, ts),
                )
            self.conn.commit()
            return version
        except Exception:
            self.conn.rollback()
            raise

    def load_map_constraints(
        self,
        *,
        map_id: str,
        map_revision_id: str = "",
        constraint_version: Optional[str] = None,
        map_md5_hint: str = "",
        create_if_missing: bool = False,
    ) -> Dict[str, Any]:
        map_id = str(map_id or "").strip()
        if not map_id:
            raise ValueError("map_id is required")

        normalized_revision_id = str(map_revision_id or "").strip()
        version = str(constraint_version or "").strip()
        if not version:
            if create_if_missing:
                version = self.ensure_active_constraint_version(
                    map_id,
                    map_md5=str(map_md5_hint or "").strip(),
                    map_revision_id=normalized_revision_id,
                )
            else:
                version = str(self.get_active_constraint_version(map_id, normalized_revision_id) or "").strip()
        if not version:
            return {
                "map_revision_id": normalized_revision_id,
                "map_id": map_id,
                "map_md5": str(map_md5_hint or "").strip(),
                "constraint_version": "",
                "no_go_areas": [],
                "virtual_walls": [],
            }

        cur = self.conn.cursor()
        ver_row = None
        area_rows = []
        wall_rows = []
        if normalized_revision_id:
            ver_row = cur.execute(
                """
                SELECT map_md5
                FROM map_constraint_revision_versions
                WHERE map_revision_id=? AND constraint_version=?
                LIMIT 1;
                """,
                (normalized_revision_id, version),
            ).fetchone()
            area_rows = cur.execute(
                """
                SELECT area_id, name, polygon_json, enabled
                FROM map_revision_no_go_areas
                WHERE map_revision_id=? AND constraint_version=?
                ORDER BY area_id ASC;
                """,
                (normalized_revision_id, version),
            ).fetchall()
            wall_rows = cur.execute(
                """
                SELECT wall_id, name, polyline_json, buffer_m, enabled
                FROM map_revision_virtual_walls
                WHERE map_revision_id=? AND constraint_version=?
                ORDER BY wall_id ASC;
                """,
                (normalized_revision_id, version),
            ).fetchall()
        if (not normalized_revision_id) or ((not ver_row) and (not area_rows) and (not wall_rows)):
            ver_row = cur.execute(
                """
                SELECT map_md5
                FROM map_constraint_versions
                WHERE map_id=? AND constraint_version=?
                LIMIT 1;
                """,
                (map_id, version),
            ).fetchone()
            area_rows = cur.execute(
                """
                SELECT area_id, name, polygon_json, enabled
                FROM map_no_go_areas
                WHERE map_id=? AND constraint_version=?
                ORDER BY area_id ASC;
                """,
                (map_id, version),
            ).fetchall()
            wall_rows = cur.execute(
                """
                SELECT wall_id, name, polyline_json, buffer_m, enabled
                FROM map_virtual_walls
                WHERE map_id=? AND constraint_version=?
                ORDER BY wall_id ASC;
                """,
                (map_id, version),
            ).fetchall()

        no_go_areas = []
        for row in area_rows or []:
            no_go_areas.append(
                {
                    "area_id": str(row["area_id"] or ""),
                    "name": str(row["name"] or ""),
                    "polygon": _json_loads(str(row["polygon_json"] or ""), []),
                    "enabled": bool(int(row["enabled"] or 0)),
                }
            )

        virtual_walls = []
        for row in wall_rows or []:
            virtual_walls.append(
                {
                    "wall_id": str(row["wall_id"] or ""),
                    "name": str(row["name"] or ""),
                    "polyline": _json_loads(str(row["polyline_json"] or ""), []),
                    "buffer_m": float(row["buffer_m"] or DEFAULT_VIRTUAL_WALL_BUFFER_M),
                    "enabled": bool(int(row["enabled"] or 0)),
                }
            )

        return {
            "map_revision_id": normalized_revision_id,
            "map_id": map_id,
            "map_md5": str((ver_row["map_md5"] if ver_row else "") or map_md5_hint or "").strip(),
            "constraint_version": version,
            "no_go_areas": no_go_areas,
            "virtual_walls": virtual_walls,
        }

    def _alignment_row_to_dict(self, row: sqlite3.Row) -> Dict[str, Any]:
        active_alignment_version = ""
        try:
            active_alignment_version = str(row["active_alignment_version"] or "")
        except Exception:
            active_alignment_version = ""
        alignment_version = str(row["alignment_version"] or "")
        return {
            "map_revision_id": str((row["map_revision_id"] if "map_revision_id" in row.keys() else "") or ""),
            "map_name": _normalize_map_name(str(row["map_name"] or "")),
            "map_id": str(row["map_id"] or ""),
            "map_version": str(row["map_version"] or ""),
            "alignment_version": alignment_version,
            "raw_frame": str(row["raw_frame"] or ""),
            "aligned_frame": str(row["aligned_frame"] or ""),
            "yaw_offset_deg": float(row["yaw_offset_deg"] or 0.0),
            "pivot_x": float(row["pivot_x"] or 0.0),
            "pivot_y": float(row["pivot_y"] or 0.0),
            "source": str(row["source"] or ""),
            "status": str(row["status"] or "draft"),
            "active": bool(alignment_version and alignment_version == active_alignment_version),
            "created_ts": float(row["created_ts"] or 0.0),
            "updated_ts": float(row["updated_ts"] or 0.0),
        }

    def list_map_alignment_configs(self, *, map_name: str = "", map_revision_id: str = "") -> List[Dict[str, Any]]:
        map_name, map_revision_id = self._resolve_map_scope(
            map_name=map_name,
            map_revision_id=map_revision_id,
        )
        if map_revision_id:
            rows = self._fetch_rows(
                """
                SELECT c.*, a.active_alignment_version
                FROM map_alignment_revision_configs c
                LEFT JOIN map_active_alignment_revisions a ON a.map_revision_id=c.map_revision_id
                WHERE c.map_revision_id=?
                ORDER BY c.updated_ts DESC, c.alignment_version ASC;
                """,
                (map_revision_id,),
            )
            if rows:
                return [self._alignment_row_to_dict(r) for r in rows]
        where = ""
        args: Tuple[Any, ...] = ()
        if map_name:
            where = "WHERE c.map_name=?"
            args = (map_name,)
        rows = self._fetch_rows(
            """
            SELECT c.*, a.active_alignment_version
            FROM map_alignment_configs c
            LEFT JOIN map_active_alignments a ON a.map_name=c.map_name
            %s
            ORDER BY c.map_name ASC, c.updated_ts DESC;
            """ % where,
            args,
        )
        return [self._alignment_row_to_dict(r) for r in rows]

    def get_map_alignment_config(
        self,
        *,
        map_name: str,
        map_revision_id: str = "",
        alignment_version: str = "",
        active_only: bool = False,
    ) -> Optional[Dict[str, Any]]:
        map_name, map_revision_id = self._resolve_map_scope(
            map_name=map_name,
            map_revision_id=map_revision_id,
        )
        if not map_name:
            return None
        alignment_version = str(alignment_version or "").strip()
        row = None
        if map_revision_id:
            if alignment_version:
                row = self.conn.execute(
                    """
                    SELECT c.*, a.active_alignment_version
                    FROM map_alignment_revision_configs c
                    LEFT JOIN map_active_alignment_revisions a ON a.map_revision_id=c.map_revision_id
                    WHERE c.map_revision_id=? AND c.alignment_version=?
                    LIMIT 1;
                    """,
                    (map_revision_id, alignment_version),
                ).fetchone()
            elif active_only:
                row = self.conn.execute(
                    """
                    SELECT c.*, a.active_alignment_version
                    FROM map_alignment_revision_configs c
                    JOIN map_active_alignment_revisions a
                      ON a.map_revision_id=c.map_revision_id
                     AND a.active_alignment_version=c.alignment_version
                    WHERE c.map_revision_id=?
                    LIMIT 1;
                    """,
                    (map_revision_id,),
                ).fetchone()
            else:
                row = self.conn.execute(
                    """
                    SELECT c.*, a.active_alignment_version
                    FROM map_alignment_revision_configs c
                    LEFT JOIN map_active_alignment_revisions a ON a.map_revision_id=c.map_revision_id
                    WHERE c.map_revision_id=?
                    ORDER BY
                      CASE WHEN c.alignment_version=a.active_alignment_version THEN 0 ELSE 1 END ASC,
                      c.updated_ts DESC
                    LIMIT 1;
                    """,
                    (map_revision_id,),
                ).fetchone()
        if not row:
            if alignment_version:
                row = self.conn.execute(
                    """
                    SELECT c.*, a.active_alignment_version
                    FROM map_alignment_configs c
                    LEFT JOIN map_active_alignments a ON a.map_name=c.map_name
                    WHERE c.map_name=? AND c.alignment_version=?
                    LIMIT 1;
                    """,
                    (map_name, alignment_version),
                ).fetchone()
            elif active_only:
                row = self.conn.execute(
                    """
                    SELECT c.*, a.active_alignment_version
                    FROM map_alignment_configs c
                    JOIN map_active_alignments a
                      ON a.map_name=c.map_name
                     AND a.active_alignment_version=c.alignment_version
                    WHERE c.map_name=?
                    LIMIT 1;
                    """,
                    (map_name,),
                ).fetchone()
            else:
                row = self.conn.execute(
                    """
                    SELECT c.*, a.active_alignment_version
                    FROM map_alignment_configs c
                    LEFT JOIN map_active_alignments a ON a.map_name=c.map_name
                    WHERE c.map_name=?
                    ORDER BY
                      CASE WHEN c.alignment_version=a.active_alignment_version THEN 0 ELSE 1 END ASC,
                      c.updated_ts DESC
                    LIMIT 1;
                    """,
                    (map_name,),
                ).fetchone()
        return self._alignment_row_to_dict(row) if row else None

    def upsert_map_alignment_config(
        self,
        *,
        map_name: str,
        map_revision_id: str = "",
        alignment_version: str,
        map_id: str = "",
        map_version: str = "",
        raw_frame: str = "map",
        aligned_frame: str = "site_map",
        yaw_offset_deg: float = 0.0,
        pivot_x: float = 0.0,
        pivot_y: float = 0.0,
        source: str = "",
        status: str = "draft",
        created_ts: Optional[float] = None,
    ) -> Dict[str, Any]:
        map_name, map_revision_id = self._resolve_map_scope(
            map_name=map_name,
            map_revision_id=map_revision_id,
        )
        if not map_name:
            raise ValueError("map_name is required for alignment config")
        alignment_version = str(alignment_version or "").strip()
        if not alignment_version:
            raise ValueError("alignment_version is required")
        existing = self.get_map_alignment_config(
            map_name=map_name,
            map_revision_id=map_revision_id,
            alignment_version=alignment_version,
        )
        ts = _now_ts()
        created = float(created_ts if created_ts is not None else (existing or {}).get("created_ts") or ts)
        if map_revision_id:
            self.conn.execute(
                """
                INSERT INTO map_alignment_revision_configs(
                  map_revision_id, map_name, map_id, map_version, alignment_version, raw_frame, aligned_frame,
                  yaw_offset_deg, pivot_x, pivot_y, source, status, created_ts, updated_ts
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(map_revision_id, alignment_version) DO UPDATE SET
                  map_name=excluded.map_name,
                  map_id=excluded.map_id,
                  map_version=excluded.map_version,
                  raw_frame=excluded.raw_frame,
                  aligned_frame=excluded.aligned_frame,
                  yaw_offset_deg=excluded.yaw_offset_deg,
                  pivot_x=excluded.pivot_x,
                  pivot_y=excluded.pivot_y,
                  source=excluded.source,
                  status=excluded.status,
                  updated_ts=excluded.updated_ts;
                """,
                (
                    map_revision_id,
                    map_name,
                    str(map_id or "").strip(),
                    str(map_version or "").strip(),
                    alignment_version,
                    str(raw_frame or "map").strip() or "map",
                    str(aligned_frame or "site_map").strip() or "site_map",
                    float(yaw_offset_deg or 0.0),
                    float(pivot_x or 0.0),
                    float(pivot_y or 0.0),
                    str(source or "").strip(),
                    str(status or "draft").strip() or "draft",
                    created,
                    ts,
                ),
            )
        else:
            self.conn.execute(
                """
                INSERT INTO map_alignment_configs(
                  map_name, map_id, map_version, alignment_version, raw_frame, aligned_frame,
                  yaw_offset_deg, pivot_x, pivot_y, source, status, created_ts, updated_ts
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(map_name, alignment_version) DO UPDATE SET
                  map_id=excluded.map_id,
                  map_version=excluded.map_version,
                  raw_frame=excluded.raw_frame,
                  aligned_frame=excluded.aligned_frame,
                  yaw_offset_deg=excluded.yaw_offset_deg,
                  pivot_x=excluded.pivot_x,
                  pivot_y=excluded.pivot_y,
                  source=excluded.source,
                  status=excluded.status,
                  updated_ts=excluded.updated_ts;
                """,
                (
                    map_name,
                    str(map_id or "").strip(),
                    str(map_version or "").strip(),
                    alignment_version,
                    str(raw_frame or "map").strip() or "map",
                    str(aligned_frame or "site_map").strip() or "site_map",
                    float(yaw_offset_deg or 0.0),
                    float(pivot_x or 0.0),
                    float(pivot_y or 0.0),
                    str(source or "").strip(),
                    str(status or "draft").strip() or "draft",
                    created,
                    ts,
                ),
            )
        self.conn.commit()
        return self.get_map_alignment_config(
            map_name=map_name,
            map_revision_id=map_revision_id,
            alignment_version=alignment_version,
        ) or {}

    def set_active_map_alignment(self, *, map_name: str, alignment_version: str, map_revision_id: str = "") -> Optional[Dict[str, Any]]:
        map_name, map_revision_id = self._resolve_map_scope(
            map_name=map_name,
            map_revision_id=map_revision_id,
        )
        alignment_version = str(alignment_version or "").strip()
        if not map_name or not alignment_version:
            raise ValueError("map_name and alignment_version are required")
        row = self.get_map_alignment_config(
            map_name=map_name,
            map_revision_id=map_revision_id,
            alignment_version=alignment_version,
        )
        if not row:
            raise KeyError("alignment config not found")
        ts = _now_ts()
        cur = self.conn.cursor()
        if map_revision_id:
            cur.execute(
                "UPDATE map_alignment_revision_configs SET status='draft', updated_ts=? WHERE map_revision_id=? AND status='active' AND alignment_version<>?;",
                (ts, map_revision_id, alignment_version),
            )
            cur.execute(
                "UPDATE map_alignment_revision_configs SET status='active', updated_ts=? WHERE map_revision_id=? AND alignment_version=?;",
                (ts, map_revision_id, alignment_version),
            )
            cur.execute(
                """
                INSERT INTO map_active_alignment_revisions(map_revision_id, active_alignment_version, updated_ts)
                VALUES(?,?,?)
                ON CONFLICT(map_revision_id) DO UPDATE SET
                  active_alignment_version=excluded.active_alignment_version,
                  updated_ts=excluded.updated_ts;
                """,
                (map_revision_id, alignment_version, ts),
            )
        else:
            cur.execute(
                "UPDATE map_alignment_configs SET status='draft', updated_ts=? WHERE map_name=? AND status='active' AND alignment_version<>?;",
                (ts, map_name, alignment_version),
            )
            cur.execute(
                "UPDATE map_alignment_configs SET status='active', updated_ts=? WHERE map_name=? AND alignment_version=?;",
                (ts, map_name, alignment_version),
            )
            cur.execute(
                """
                INSERT INTO map_active_alignments(map_name, active_alignment_version, updated_ts)
                VALUES(?,?,?)
                ON CONFLICT(map_name) DO UPDATE SET
                  active_alignment_version=excluded.active_alignment_version,
                  updated_ts=excluded.updated_ts;
                """,
                (map_name, alignment_version, ts),
            )
        self.conn.commit()
        return self.get_map_alignment_config(
            map_name=map_name,
            map_revision_id=map_revision_id,
            alignment_version=alignment_version,
        )

    def archive_map_alignment_config(self, *, map_name: str, alignment_version: str, map_revision_id: str = "") -> None:
        map_name, map_revision_id = self._resolve_map_scope(
            map_name=map_name,
            map_revision_id=map_revision_id,
        )
        alignment_version = str(alignment_version or "").strip()
        if not map_name or not alignment_version:
            raise ValueError("map_name and alignment_version are required")
        ts = _now_ts()
        cur = self.conn.cursor()
        if map_revision_id:
            cur.execute(
                "UPDATE map_alignment_revision_configs SET status='archived', updated_ts=? WHERE map_revision_id=? AND alignment_version=?;",
                (ts, map_revision_id, alignment_version),
            )
            cur.execute(
                "DELETE FROM map_active_alignment_revisions WHERE map_revision_id=? AND active_alignment_version=?;",
                (map_revision_id, alignment_version),
            )
        else:
            cur.execute(
                "UPDATE map_alignment_configs SET status='archived', updated_ts=? WHERE map_name=? AND alignment_version=?;",
                (ts, map_name, alignment_version),
            )
            cur.execute(
                "DELETE FROM map_active_alignments WHERE map_name=? AND active_alignment_version=?;",
                (map_name, alignment_version),
            )
        self.conn.commit()

    def upsert_zone_editor_metadata(
        self,
        *,
        map_name: str,
        map_revision_id: str = "",
        zone_id: str,
        zone_version: int,
        alignment_version: str = "",
        display_frame: str = "",
        display_outer: Optional[List[Tuple[float, float]]] = None,
        display_holes: Optional[List[List[Tuple[float, float]]]] = None,
        profile_name: str = "",
        preview_plan_id: str = "",
        estimated_length_m: float = 0.0,
        estimated_duration_s: float = 0.0,
        warnings: Optional[List[str]] = None,
    ) -> None:
        map_name, map_revision_id = self._resolve_map_scope(
            map_name=map_name,
            map_revision_id=map_revision_id,
        )
        if not map_name:
            raise ValueError("map_name is required for zone editor metadata")
        if not map_revision_id:
            raise ValueError("map_revision_id is required for zone editor metadata")
        ts = _now_ts()
        self.conn.execute(
            """
            INSERT INTO zone_editor_metadata(
              map_revision_id, map_name, zone_id, zone_version, alignment_version, display_frame, display_outer_json,
              display_holes_json, profile_name, preview_plan_id, estimated_length_m, estimated_duration_s,
              warnings_json, updated_ts
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(map_revision_id, zone_id, zone_version) DO UPDATE SET
              map_name=excluded.map_name,
              alignment_version=excluded.alignment_version,
              display_frame=excluded.display_frame,
              display_outer_json=excluded.display_outer_json,
              display_holes_json=excluded.display_holes_json,
              profile_name=excluded.profile_name,
              preview_plan_id=excluded.preview_plan_id,
              estimated_length_m=excluded.estimated_length_m,
              estimated_duration_s=excluded.estimated_duration_s,
              warnings_json=excluded.warnings_json,
              updated_ts=excluded.updated_ts;
            """,
            (
                map_revision_id,
                map_name,
                str(zone_id or "").strip(),
                int(zone_version),
                str(alignment_version or "").strip(),
                str(display_frame or "").strip(),
                _json_dumps([[float(x), float(y)] for x, y in (display_outer or [])]),
                _json_dumps([[[float(x), float(y)] for x, y in ring] for ring in (display_holes or [])]),
                str(profile_name or "").strip(),
                str(preview_plan_id or "").strip(),
                float(estimated_length_m or 0.0),
                float(estimated_duration_s or 0.0),
                _json_dumps([str(w) for w in (warnings or [])]),
                ts,
            ),
        )
        self.conn.commit()

    def get_zone_editor_metadata(
        self,
        *,
        map_name: str,
        map_revision_id: str = "",
        zone_id: str,
        zone_version: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        map_name, map_revision_id = self._resolve_map_scope(
            map_name=map_name,
            map_revision_id=map_revision_id,
        )
        if not map_name:
            return None
        zone_id = str(zone_id or "").strip()
        if zone_version is None:
            zone = self.get_zone_meta(zone_id, map_name=map_name, map_revision_id=map_revision_id)
            if not zone:
                return None
            zone_version = int(zone.get("zone_version") or zone.get("current_zone_version") or 0)
        if map_revision_id:
            row = self.conn.execute(
                """
                SELECT *
                FROM zone_editor_metadata
                WHERE map_revision_id=? AND zone_id=? AND zone_version=?
                LIMIT 1;
                """,
                (map_revision_id, zone_id, int(zone_version)),
            ).fetchone()
        else:
            row = self.conn.execute(
                """
                SELECT *
                FROM zone_editor_metadata
                WHERE map_name=? AND zone_id=? AND zone_version=?
                LIMIT 1;
                """,
                (map_name, zone_id, int(zone_version)),
            ).fetchone()
        if not row:
            return None
        return {
            "map_revision_id": str(row["map_revision_id"] or "") if "map_revision_id" in row.keys() else map_revision_id,
            "map_name": str(row["map_name"] or ""),
            "zone_id": str(row["zone_id"] or ""),
            "zone_version": int(row["zone_version"] or 0),
            "alignment_version": str(row["alignment_version"] or ""),
            "display_frame": str(row["display_frame"] or ""),
            "display_outer": _json_loads(str(row["display_outer_json"] or ""), []),
            "display_holes": _json_loads(str(row["display_holes_json"] or ""), []),
            "profile_name": str(row["profile_name"] or ""),
            "preview_plan_id": str(row["preview_plan_id"] or ""),
            "estimated_length_m": float(row["estimated_length_m"] or 0.0),
            "estimated_duration_s": float(row["estimated_duration_s"] or 0.0),
            "warnings": _json_loads(str(row["warnings_json"] or ""), []),
            "updated_ts": float(row["updated_ts"] or 0.0),
        }

    def list_zone_metas(
        self,
        *,
        map_name: str = "",
        map_revision_id: str = "",
        include_disabled: bool = False,
        map_version: str = "",
    ) -> List[Dict[str, Any]]:
        map_name, map_revision_id = self._resolve_map_scope(
            map_name=map_name,
            map_revision_id=map_revision_id,
        )
        if not map_name:
            return []
        map_version = str(map_version or "").strip()
        params: List[Any] = []
        where_scope = "z.map_name=?"
        if map_revision_id:
            where_scope = "z.map_revision_id=?"
            params.append(map_revision_id)
        else:
            params.append(map_name)
        sql = """
            SELECT
              z.map_revision_id,
              z.map_name,
              z.zone_id,
              z.display_name,
              z.enabled,
              z.current_zone_version,
              z.updated_ts,
              zv.frame_id,
              zv.outer_json,
              zv.holes_json,
              zv.map_id,
              zv.map_md5
            FROM zones z
            LEFT JOIN zone_versions zv
              ON zv.map_revision_id=z.map_revision_id
             AND zv.zone_id=z.zone_id
             AND zv.zone_version=z.current_zone_version
            WHERE %s
        """ % where_scope
        if not bool(include_disabled):
            sql += " AND z.enabled=1"
        if map_version and (not map_revision_id):
            sql += " AND (zv.map_md5=? OR zv.map_md5='' OR zv.map_md5 IS NULL)"
            params.append(map_version)
        sql += " ORDER BY z.updated_ts DESC, z.zone_id ASC;"
        rows = self.conn.execute(sql, tuple(params)).fetchall()
        out: List[Dict[str, Any]] = []
        for row in rows or []:
            out.append(
                {
                    "map_revision_id": str(row["map_revision_id"] or "") if "map_revision_id" in row.keys() else map_revision_id,
                    "map_name": str(row["map_name"] or ""),
                    "zone_id": str(row["zone_id"] or ""),
                    "display_name": str(row["display_name"] or ""),
                    "enabled": bool(int(row["enabled"] or 0)),
                    "zone_version": int(row["current_zone_version"] or 0),
                    "current_zone_version": int(row["current_zone_version"] or 0),
                    "frame_id": str(row["frame_id"] or ""),
                    "outer_json": str(row["outer_json"] or ""),
                    "holes_json": str(row["holes_json"] or ""),
                    "map_id": str(row["map_id"] or ""),
                    "map_md5": str(row["map_md5"] or ""),
                    "updated_ts": float(row["updated_ts"] or 0.0),
                }
            )
        return out

    def get_zone_meta(
        self,
        zone_id: str,
        *,
        map_name: str = "",
        map_revision_id: str = "",
        map_version: str = "",
    ) -> Optional[Dict[str, Any]]:
        map_name, map_revision_id = self._resolve_map_scope(
            map_name=map_name,
            map_revision_id=map_revision_id,
        )
        if not map_name:
            return None
        map_version = str(map_version or "").strip()
        args: List[Any] = []
        where_scope = "z.map_name=?"
        if map_revision_id:
            where_scope = "z.map_revision_id=?"
            args.append(map_revision_id)
        else:
            args.append(map_name)
        sql = """
            SELECT
              z.map_revision_id,
              z.map_name,
              z.zone_id,
              z.display_name,
              z.enabled,
              z.current_zone_version,
              z.updated_ts,
              zv.frame_id,
              zv.outer_json,
              zv.holes_json,
              zv.map_id,
              zv.map_md5
            FROM zones z
            LEFT JOIN zone_versions zv
              ON zv.map_revision_id=z.map_revision_id
             AND zv.zone_id=z.zone_id
             AND zv.zone_version=z.current_zone_version
            WHERE %s AND z.zone_id=?
        """ % where_scope
        args.append(str(zone_id or "").strip())
        if map_version and (not map_revision_id):
            sql += " AND (zv.map_md5=? OR zv.map_md5='' OR zv.map_md5 IS NULL)"
            args.append(map_version)
        sql += " LIMIT 1;"
        row = self.conn.execute(sql, tuple(args)).fetchone()
        if not row:
            return None
        return {
            "map_revision_id": str(row["map_revision_id"] or "") if "map_revision_id" in row.keys() else map_revision_id,
            "map_name": str(row["map_name"] or ""),
            "zone_id": str(row["zone_id"] or ""),
            "display_name": str(row["display_name"] or ""),
            "enabled": bool(int(row["enabled"] or 0)),
            "zone_version": int(row["current_zone_version"] or 0),
            "current_zone_version": int(row["current_zone_version"] or 0),
            "frame_id": str(row["frame_id"] or ""),
            "outer_json": str(row["outer_json"] or ""),
            "holes_json": str(row["holes_json"] or ""),
            "map_id": str(row["map_id"] or ""),
            "map_md5": str(row["map_md5"] or ""),
            "updated_ts": float(row["updated_ts"] or 0.0),
        }

    def set_zone_enabled(
        self,
        *,
        zone_id: str,
        enabled: bool,
        map_name: str = "",
        map_revision_id: str = "",
        map_version: str = "",
    ) -> Optional[Dict[str, Any]]:
        map_name, map_revision_id = self._resolve_map_scope(
            map_name=map_name,
            map_revision_id=map_revision_id,
        )
        if not map_name:
            raise ValueError("map_name is required")
        zone_id = str(zone_id or "").strip()
        if not zone_id:
            raise ValueError("zone_id is required")
        current = self.get_zone_meta(zone_id, map_name=map_name, map_revision_id=map_revision_id, map_version=map_version)
        if not current:
            return None
        ts = _now_ts()
        if map_revision_id:
            self.conn.execute(
                """
                UPDATE zones
                SET enabled=?, updated_ts=?
                WHERE map_revision_id=? AND zone_id=?;
                """,
                (
                    1 if bool(enabled) else 0,
                    ts,
                    map_revision_id,
                    zone_id,
                ),
            )
        else:
            self.conn.execute(
                """
                UPDATE zones
                SET enabled=?, updated_ts=?
                WHERE map_name=? AND zone_id=?;
                """,
                (
                    1 if bool(enabled) else 0,
                    ts,
                    map_name,
                    zone_id,
                ),
            )
        self.conn.commit()
        return self.get_zone_meta(zone_id, map_name=map_name, map_revision_id=map_revision_id, map_version=map_version)

    def list_active_plan_refs(
        self,
        zone_id: str,
        *,
        map_name: str = "",
        map_revision_id: str = "",
    ) -> List[Dict[str, Any]]:
        map_name, map_revision_id = self._resolve_map_scope(
            map_name=map_name,
            map_revision_id=map_revision_id,
        )
        if not map_name:
            return []
        if map_revision_id:
            rows = self.conn.execute(
                """
                SELECT plan_profile_name, active_plan_id, updated_ts
                FROM zone_active_plans
                WHERE map_revision_id=? AND zone_id=?
                ORDER BY updated_ts DESC, plan_profile_name ASC;
                """,
                (map_revision_id, str(zone_id or "").strip()),
            ).fetchall()
        else:
            rows = self.conn.execute(
                """
                SELECT plan_profile_name, active_plan_id, updated_ts
                FROM zone_active_plans
                WHERE map_name=? AND zone_id=?
                ORDER BY updated_ts DESC, plan_profile_name ASC;
                """,
                (map_name, str(zone_id or "").strip()),
            ).fetchall()
        out: List[Dict[str, Any]] = []
        for row in rows or []:
            out.append(
                {
                    "map_revision_id": map_revision_id,
                    "plan_profile_name": str(row["plan_profile_name"] or ""),
                    "active_plan_id": str(row["active_plan_id"] or ""),
                    "updated_ts": float(row["updated_ts"] or 0.0),
                }
            )
        return out

    def get_active_plan_id(
        self,
        zone_id: str,
        plan_profile_name: Optional[str] = None,
        *,
        map_name: str = "",
        map_revision_id: str = "",
    ) -> Optional[str]:
        map_name, map_revision_id = self._resolve_map_scope(
            map_name=map_name,
            map_revision_id=map_revision_id,
        )
        if not map_name:
            return None
        zone_id = str(zone_id or "").strip()
        profile = str(plan_profile_name or "").strip()
        if profile:
            if map_revision_id:
                row = self.conn.execute(
                    """
                    SELECT active_plan_id FROM zone_active_plans
                    WHERE map_revision_id=? AND zone_id=? AND plan_profile_name=?
                    LIMIT 1;
                    """,
                    (map_revision_id, zone_id, profile),
                ).fetchone()
            else:
                row = self.conn.execute(
                    """
                    SELECT active_plan_id FROM zone_active_plans
                    WHERE map_name=? AND zone_id=? AND plan_profile_name=?
                    LIMIT 1;
                    """,
                    (map_name, zone_id, profile),
                ).fetchone()
            return str(row["active_plan_id"]) if row and row["active_plan_id"] else None
        if map_revision_id:
            row = self.conn.execute(
                """
                SELECT active_plan_id FROM zone_active_plans
                WHERE map_revision_id=? AND zone_id=?
                ORDER BY updated_ts DESC
                LIMIT 1;
                """,
                (map_revision_id, zone_id),
            ).fetchone()
        else:
            row = self.conn.execute(
                """
                SELECT active_plan_id FROM zone_active_plans
                WHERE map_name=? AND zone_id=?
                ORDER BY updated_ts DESC
                LIMIT 1;
                """,
                (map_name, zone_id),
            ).fetchone()
        return str(row["active_plan_id"]) if row and row["active_plan_id"] else None

    def get_latest_plan_id(
        self,
        zone_id: str,
        zone_version: Optional[int] = None,
        *,
        map_name: str = "",
        map_revision_id: str = "",
    ) -> Optional[str]:
        map_name, map_revision_id = self._resolve_map_scope(
            map_name=map_name,
            map_revision_id=map_revision_id,
        )
        if not map_name:
            return None
        zone_id = str(zone_id or "").strip()
        if zone_version is None:
            if map_revision_id:
                row = self.conn.execute(
                    """
                    SELECT plan_id FROM plans
                    WHERE map_revision_id=? AND zone_id=?
                    ORDER BY created_ts DESC LIMIT 1;
                    """,
                    (map_revision_id, zone_id),
                ).fetchone()
            else:
                row = self.conn.execute(
                    """
                    SELECT plan_id FROM plans
                    WHERE map_name=? AND zone_id=?
                    ORDER BY created_ts DESC LIMIT 1;
                    """,
                    (map_name, zone_id),
                ).fetchone()
        else:
            if map_revision_id:
                row = self.conn.execute(
                    """
                    SELECT plan_id FROM plans
                    WHERE map_revision_id=? AND zone_id=? AND zone_version=?
                    ORDER BY created_ts DESC LIMIT 1;
                    """,
                    (map_revision_id, zone_id, int(zone_version)),
                ).fetchone()
            else:
                row = self.conn.execute(
                    """
                    SELECT plan_id FROM plans
                    WHERE map_name=? AND zone_id=? AND zone_version=?
                    ORDER BY created_ts DESC LIMIT 1;
                    """,
                    (map_name, zone_id, int(zone_version)),
                ).fetchone()
        return str(row["plan_id"]) if row else None

    def get_latest_plan_id_by_profile(
        self,
        zone_id: str,
        plan_profile_name: str,
        zone_version: Optional[int] = None,
        *,
        map_name: str = "",
        map_revision_id: str = "",
    ) -> Optional[str]:
        plan_profile_name = str(plan_profile_name or "").strip()
        if not plan_profile_name:
            return self.get_latest_plan_id(
                zone_id,
                zone_version=zone_version,
                map_name=map_name,
                map_revision_id=map_revision_id,
            )
        map_name, map_revision_id = self._resolve_map_scope(
            map_name=map_name,
            map_revision_id=map_revision_id,
        )
        if not map_name:
            return None
        if zone_version is None:
            if map_revision_id:
                row = self.conn.execute(
                    """
                    SELECT plan_id FROM plans
                    WHERE map_revision_id=? AND zone_id=? AND plan_profile_name=?
                    ORDER BY created_ts DESC LIMIT 1;
                    """,
                    (map_revision_id, str(zone_id or "").strip(), plan_profile_name),
                ).fetchone()
            else:
                row = self.conn.execute(
                    """
                    SELECT plan_id FROM plans
                    WHERE map_name=? AND zone_id=? AND plan_profile_name=?
                    ORDER BY created_ts DESC LIMIT 1;
                    """,
                    (map_name, str(zone_id or "").strip(), plan_profile_name),
                ).fetchone()
        else:
            if map_revision_id:
                row = self.conn.execute(
                    """
                    SELECT plan_id FROM plans
                    WHERE map_revision_id=? AND zone_id=? AND zone_version=? AND plan_profile_name=?
                    ORDER BY created_ts DESC LIMIT 1;
                    """,
                    (map_revision_id, str(zone_id or "").strip(), int(zone_version), plan_profile_name),
                ).fetchone()
            else:
                row = self.conn.execute(
                    """
                    SELECT plan_id FROM plans
                    WHERE map_name=? AND zone_id=? AND zone_version=? AND plan_profile_name=?
                    ORDER BY created_ts DESC LIMIT 1;
                    """,
                    (map_name, str(zone_id or "").strip(), int(zone_version), plan_profile_name),
                ).fetchone()
        return str(row["plan_id"]) if row else None

    def load_plan_meta(self, plan_id: str) -> Dict[str, Any]:
        row = self.conn.execute(
            "SELECT * FROM plans WHERE plan_id=?;",
            (str(plan_id or "").strip(),),
        ).fetchone()
        if not row:
            raise KeyError(f"plan_id not found: {plan_id}")
        d = dict(row)
        for k in ("params_json", "robot_json", "exec_order_json"):
            if d.get(k):
                d[k] = _json_loads(d[k], {})
        d["map_name"] = str(d.get("map_name") or "")
        d["map_revision_id"] = str(d.get("map_revision_id") or "")
        return d

    def load_block(self, plan_id: str, block_id: int) -> Dict[str, Any]:
        row = self.conn.execute(
            "SELECT * FROM plan_blocks WHERE plan_id=? AND block_id=?;",
            (str(plan_id or "").strip(), int(block_id)),
        ).fetchone()
        if not row:
            raise KeyError(f"block not found: {plan_id} block {block_id}")
        d = dict(row)
        d["path_xyyaw"] = decode_path_xyyaw_f32(d["path_blob"])
        return d

# -*- coding: utf-8 -*-
import json
import os
import sqlite3
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

from .codec import (
    decode_path_xyyaw_f32,
    encode_path_xyyaw_f32,
    polyline_length_xy,
    xy_to_xyyaw,
)
from coverage_planner.constraints import DEFAULT_VIRTUAL_WALL_BUFFER_M


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

    _MIGRATED_TABLES = (
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

    def _needs_schema_migration(self) -> bool:
        map_asset_cols = set(self._table_columns("map_assets"))
        if "map_id" not in map_asset_cols and self._table_exists("map_assets"):
            return True
        if "map_version" in self._table_columns("plans"):
            return True
        if "map_version" in self._table_columns("robot_active_map"):
            return True
        if self._table_exists("map_asset_versions"):
            return True
        if self._table_exists("map_zones"):
            return True
        return False

    def _init_schema(self):
        if self._needs_schema_migration():
            self._migrate_legacy_schema()
        self._create_schema()
        self._seed_plan_profiles()

    def _create_schema(self):
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS zones (
              map_name TEXT NOT NULL,
              zone_id TEXT NOT NULL,
              display_name TEXT,
              enabled INTEGER NOT NULL DEFAULT 1,
              current_zone_version INTEGER NOT NULL DEFAULT 0,
              updated_ts REAL NOT NULL,
              PRIMARY KEY(map_name, zone_id)
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS zone_versions (
              map_name TEXT NOT NULL,
              zone_id TEXT NOT NULL,
              zone_version INTEGER NOT NULL,
              frame_id TEXT,
              outer_json TEXT,
              holes_json TEXT,
              map_id TEXT,
              map_md5 TEXT,
              created_ts REAL NOT NULL,
              PRIMARY KEY(map_name, zone_id, zone_version)
            );
            """
        )
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
            CREATE TABLE IF NOT EXISTS plans (
              plan_id TEXT PRIMARY KEY,
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
              map_name TEXT NOT NULL,
              zone_id TEXT NOT NULL,
              plan_profile_name TEXT NOT NULL,
              active_plan_id TEXT NOT NULL,
              updated_ts REAL NOT NULL,
              PRIMARY KEY(map_name, zone_id, plan_profile_name)
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
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS zone_editor_metadata (
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
              PRIMARY KEY(map_name, zone_id, zone_version)
            );
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_zone_versions_scope ON zone_versions(map_name, zone_id, zone_version);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_plans_scope ON plans(map_name, zone_id, created_ts);")
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_plans_scope_profile ON plans(map_name, zone_id, plan_profile_name, created_ts);"
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_plan_blocks_plan ON plan_blocks(plan_id);")
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_zone_active_plans_scope ON zone_active_plans(map_name, zone_id, updated_ts);"
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
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_zone_editor_meta_scope ON zone_editor_metadata(map_name, zone_id, updated_ts);"
        )
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

    def _legacy_table_not_empty(self, table_name: str) -> bool:
        if not self._table_exists(table_name):
            return False
        row = self.conn.execute("SELECT 1 FROM %s LIMIT 1;" % str(table_name)).fetchone()
        return bool(row)

    def _read_legacy_assets(self) -> List[Dict[str, Any]]:
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

    def _read_legacy_active_maps(self) -> List[Dict[str, Any]]:
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

    def _read_legacy_zone_heads(self) -> List[Dict[str, Any]]:
        if self._legacy_table_not_empty("map_zones"):
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

    def _read_legacy_zone_versions(self) -> List[Dict[str, Any]]:
        if self._legacy_table_not_empty("map_zone_versions"):
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

    def _read_legacy_active_plans(self) -> List[Dict[str, Any]]:
        if self._legacy_table_not_empty("map_zone_active_plans"):
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

    def _read_legacy_plans(self) -> List[Dict[str, Any]]:
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

    def _migrate_legacy_schema(self):
        plan_profiles = self._copy_simple_table("plan_profiles")
        plan_blocks = self._copy_simple_table("plan_blocks")
        constraint_versions = self._copy_simple_table("map_constraint_versions")
        no_go_areas = self._copy_simple_table("map_no_go_areas")
        virtual_walls = self._copy_simple_table("map_virtual_walls")
        active_constraint_versions = self._copy_simple_table("map_active_constraint_versions")

        assets = self._read_legacy_assets()
        active_maps = self._read_legacy_active_maps()
        zones = self._read_legacy_zone_heads()
        zone_versions = self._read_legacy_zone_versions()
        active_plans = self._read_legacy_active_plans()
        plans = self._read_legacy_plans()

        cur = self.conn.cursor()
        try:
            cur.execute("BEGIN IMMEDIATE;")
            for table_name in self._MIGRATED_TABLES:
                self._drop_table_if_exists(table_name)
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

        self._create_schema()

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
                cur.execute(
                    """
                    INSERT OR REPLACE INTO map_assets(
                      map_name, display_name, enabled, description, map_id, map_md5,
                      yaml_path, pgm_path, pbstream_path, frame_id, resolution, origin_json,
                      created_ts, updated_ts
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?);
                    """,
                    (
                        str(row["map_name"]),
                        str(row.get("display_name") or row["map_name"]),
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
                    ),
                )
            for row in active_maps:
                cur.execute(
                    """
                    INSERT OR REPLACE INTO robot_active_map(robot_id, map_name, updated_ts)
                    VALUES(?,?,?);
                    """,
                    (
                        str(row.get("robot_id") or "local_robot"),
                        str(row.get("map_name") or ""),
                        float(row.get("updated_ts") or _now_ts()),
                    ),
                )
            for row in zones:
                cur.execute(
                    """
                    INSERT OR REPLACE INTO zones(map_name, zone_id, display_name, enabled, current_zone_version, updated_ts)
                    VALUES(?,?,?,?,?,?);
                    """,
                    (
                        str(row.get("map_name") or ""),
                        str(row.get("zone_id") or ""),
                        str(row.get("display_name") or row.get("zone_id") or ""),
                        int(row.get("enabled", 1) or 0),
                        int(row.get("current_zone_version") or 0),
                        float(row.get("updated_ts") or _now_ts()),
                    ),
                )
            for row in zone_versions:
                cur.execute(
                    """
                    INSERT OR REPLACE INTO zone_versions(
                      map_name, zone_id, zone_version, frame_id, outer_json, holes_json, map_id, map_md5, created_ts
                    ) VALUES(?,?,?,?,?,?,?,?,?);
                    """,
                    (
                        str(row.get("map_name") or ""),
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
                cur.execute(
                    """
                    INSERT OR REPLACE INTO plans(
                      plan_id, map_name, zone_id, zone_version, frame_id, plan_profile_name,
                      params_json, robot_json, blocks, total_length_m, exec_order_json,
                      map_id, map_md5, constraint_version, planner_version, created_ts
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
                    """,
                    (
                        str(row.get("plan_id") or ""),
                        str(row.get("map_name") or ""),
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
                cur.execute(
                    """
                    INSERT OR REPLACE INTO zone_active_plans(map_name, zone_id, plan_profile_name, active_plan_id, updated_ts)
                    VALUES(?,?,?,?,?);
                    """,
                    (
                        str(row.get("map_name") or ""),
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
    ):
        map_name = _normalize_map_name(map_name)
        if not map_name:
            raise ValueError("map_name is required")
        ts = _now_ts()
        current = self.conn.execute(
            "SELECT created_ts FROM map_assets WHERE map_name=? LIMIT 1;",
            (map_name,),
        ).fetchone()
        cts = float(created_ts if created_ts is not None else ((current["created_ts"] if current else 0.0) or ts))
        self.conn.execute(
            """
            INSERT INTO map_assets(
              map_name, display_name, enabled, description, map_id, map_md5,
              yaml_path, pgm_path, pbstream_path, frame_id, resolution, origin_json,
              created_ts, updated_ts
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
              updated_ts=excluded.updated_ts;
            """,
            (
                map_name,
                map_name,
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
                cts,
                ts,
            ),
        )
        self.conn.commit()

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
        robot_id: str = "local_robot",
        set_active: bool = True,
    ):
        del display_name
        self.upsert_map_asset(
            map_name=map_name,
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
        )
        if set_active:
            self.set_active_map(map_name=map_name, robot_id=robot_id)

    def set_active_map(self, *, map_name: str, robot_id: str = "local_robot"):
        map_name = _normalize_map_name(map_name)
        if not map_name:
            raise ValueError("map_name is required")
        ts = _now_ts()
        self.conn.execute(
            """
            INSERT INTO robot_active_map(robot_id, map_name, updated_ts)
            VALUES(?,?,?)
            ON CONFLICT(robot_id) DO UPDATE SET
              map_name=excluded.map_name,
              updated_ts=excluded.updated_ts;
            """,
            (str(robot_id or "local_robot").strip(), map_name, ts),
        )
        self.conn.commit()

    def _asset_row_to_dict(self, row: sqlite3.Row, *, updated_ts: Optional[float] = None) -> Dict[str, Any]:
        map_name = _normalize_map_name(str(row["map_name"] or ""))
        return {
            "map_name": map_name,
            "display_name": map_name,
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
            "created_ts": float(row["created_ts"] or 0.0),
            "updated_ts": float(updated_ts if updated_ts is not None else row["updated_ts"] or 0.0),
        }

    def get_active_map(self, robot_id: str = "local_robot") -> Optional[Dict[str, Any]]:
        row = self.conn.execute(
            """
            SELECT ram.robot_id, ram.updated_ts AS active_updated_ts, ma.*
            FROM robot_active_map ram
            JOIN map_assets ma ON ma.map_name=ram.map_name
            WHERE ram.robot_id=?
            LIMIT 1;
            """,
            (str(robot_id or "local_robot").strip(),),
        ).fetchone()
        if not row:
            return None
        d = self._asset_row_to_dict(row, updated_ts=float(row["active_updated_ts"] or 0.0))
        d["robot_id"] = str(row["robot_id"] or "")
        return d

    def resolve_map_asset(
        self,
        *,
        map_name: str = "",
        robot_id: str = "local_robot",
    ) -> Optional[Dict[str, Any]]:
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
        del display_name
        row = self.conn.execute("SELECT * FROM map_assets WHERE map_name=? LIMIT 1;", (_normalize_map_name(map_name),)).fetchone()
        if not row:
            raise KeyError("map asset not found: %s" % map_name)
        self.upsert_map_asset(
            map_name=_normalize_map_name(map_name),
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
        )

    def disable_map_asset(self, *, map_name: str):
        self.update_map_asset_meta(map_name=map_name, enabled=False)

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

    def backfill_legacy_map_scope(self, *, map_name: str, map_id: str = "", map_md5: str = ""):
        map_name = _normalize_map_name(map_name)
        if not map_name:
            raise ValueError("map_name is required")
        if not map_id and not map_md5:
            return
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
        cur.execute("UPDATE plans SET map_name=? WHERE (%s) AND (map_name='' OR map_name IS NULL);" % pred, (map_name, *args))
        cur.execute(
            "UPDATE zone_versions SET map_name=? WHERE (%s) AND (map_name='' OR map_name IS NULL);" % pred,
            (map_name, *args),
        )
        cur.execute(
            """
            UPDATE zones
            SET map_name=?
            WHERE (map_name='' OR map_name IS NULL)
              AND EXISTS (
                SELECT 1 FROM zone_versions zv
                WHERE zv.zone_id=zones.zone_id
                  AND zv.zone_version=zones.current_zone_version
                  AND (%s)
              );
            """ % pred,
            (map_name, *args),
        )
        self.conn.commit()

    def _upsert_zone_version_cur(
        self,
        cur: sqlite3.Cursor,
        *,
        map_name: str,
        zone_id: str,
        zone_version: int,
        frame_id: str,
        outer: List[Tuple[float, float]],
        holes: List[List[Tuple[float, float]]],
        map_id: str = "",
        map_md5: str = "",
    ):
        map_name = _normalize_map_name(map_name)
        if not map_name:
            raise ValueError("map_name is required for zone persistence")
        ts = _now_ts()
        cur.execute(
            """
            INSERT INTO zone_versions(map_name, zone_id, zone_version, frame_id, outer_json, holes_json, map_id, map_md5, created_ts)
            VALUES(?,?,?,?,?,?,?,?,?)
            ON CONFLICT(map_name, zone_id, zone_version) DO UPDATE SET
              frame_id=excluded.frame_id,
              outer_json=excluded.outer_json,
              holes_json=excluded.holes_json,
              map_id=excluded.map_id,
              map_md5=excluded.map_md5;
            """,
            (
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
        zone_id: str,
        zone_version: int,
        display_name: str = "",
        enabled: bool = True,
    ):
        map_name = _normalize_map_name(map_name)
        if not map_name:
            raise ValueError("map_name is required for zone persistence")
        ts = _now_ts()
        cur.execute(
            """
            INSERT INTO zones(map_name, zone_id, display_name, enabled, current_zone_version, updated_ts)
            VALUES(?,?,?,?,?,?)
            ON CONFLICT(map_name, zone_id) DO UPDATE SET
              display_name=excluded.display_name,
              enabled=excluded.enabled,
              current_zone_version=excluded.current_zone_version,
              updated_ts=excluded.updated_ts;
            """,
            (
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

        cur.execute(
            """
            INSERT INTO plans(
              plan_id, map_name, zone_id, zone_version, frame_id, plan_profile_name,
              params_json, robot_json, blocks, total_length_m, exec_order_json,
              map_id, map_md5, constraint_version, planner_version, created_ts
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
            """,
            (
                str(plan_id or "").strip(),
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
        map_id: str = "",
        map_md5: str = "",
        constraint_version: str = "",
        planner_version: str = "",
    ) -> str:
        map_name = _normalize_map_name(map_name)
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
                zone_id=str(zone_id),
                zone_version=int(zone_version),
                display_name=str(zone_id or "").strip(),
                enabled=True,
            )
            self._insert_plan_cur(
                cur,
                plan_id=plan_id,
                map_name=map_name,
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
        plan_profile_name: str,
        frame_id: Optional[str] = None,
        outer: Optional[list] = None,
        holes: Optional[list] = None,
        map_id: str = "",
        map_md5: str = "",
        display_name: Optional[str] = None,
    ):
        map_name = _normalize_map_name(map_name)
        if not map_name:
            raise ValueError("map_name is required for set_active_plan")
        plan_profile_name = str(plan_profile_name or "").strip() or "cover_standard"
        cur = self.conn.cursor()
        if frame_id is not None and outer is not None and holes is not None:
            self._upsert_zone_version_cur(
                cur,
                map_name=map_name,
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
            zone_id=str(zone_id),
            zone_version=int(zone_version),
            display_name=str(display_name or zone_id or "").strip(),
            enabled=True,
        )
        ts = _now_ts()
        cur.execute(
            """
            INSERT INTO zone_active_plans(map_name, zone_id, plan_profile_name, active_plan_id, updated_ts)
            VALUES(?,?,?,?,?)
            ON CONFLICT(map_name, zone_id, plan_profile_name) DO UPDATE SET
              active_plan_id=excluded.active_plan_id,
              updated_ts=excluded.updated_ts;
            """,
            (
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
        map_name = _normalize_map_name(map_name)
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
                    zone_id=zone_id,
                    zone_version=int(zone_version),
                    display_name=str(display_name or zone_id).strip(),
                    enabled=True,
                )
            self._insert_plan_cur(
                cur,
                plan_id=plan_id,
                map_name=map_name,
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
                    INSERT INTO zone_active_plans(map_name, zone_id, plan_profile_name, active_plan_id, updated_ts)
                    VALUES(?,?,?,?,?)
                    ON CONFLICT(map_name, zone_id, plan_profile_name) DO UPDATE SET
                      active_plan_id=excluded.active_plan_id,
                      updated_ts=excluded.updated_ts;
                    """,
                    (
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
                  map_name, zone_id, zone_version, alignment_version, display_frame, display_outer_json,
                  display_holes_json, profile_name, preview_plan_id, estimated_length_m, estimated_duration_s,
                  warnings_json, updated_ts
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(map_name, zone_id, zone_version) DO UPDATE SET
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

    def get_active_constraint_version(self, map_id: str) -> Optional[str]:
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

    def set_active_constraint_version(self, map_id: str, constraint_version: str):
        ts = _now_ts()
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

    def ensure_active_constraint_version(self, map_id: str, *, map_md5: str = "") -> str:
        map_id = str(map_id or "").strip()
        if not map_id:
            raise ValueError("map_id is required for constraint version lookup")
        existing = self.get_active_constraint_version(map_id)
        if existing:
            return str(existing)
        version = uuid.uuid4().hex
        ts = _now_ts()
        cur = self.conn.cursor()
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
        map_md5: str = "",
        no_go_areas: List[Dict[str, Any]],
        virtual_walls: List[Dict[str, Any]],
        constraint_version: str = "",
    ) -> str:
        map_id = str(map_id or "").strip()
        if not map_id:
            raise ValueError("map_id is required")

        version = str(constraint_version or "").strip() or uuid.uuid4().hex
        ts = _now_ts()
        cur = self.conn.cursor()
        try:
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
        constraint_version: Optional[str] = None,
        map_md5_hint: str = "",
        create_if_missing: bool = False,
    ) -> Dict[str, Any]:
        map_id = str(map_id or "").strip()
        if not map_id:
            raise ValueError("map_id is required")

        version = str(constraint_version or "").strip()
        if not version:
            if create_if_missing:
                version = self.ensure_active_constraint_version(map_id, map_md5=str(map_md5_hint or "").strip())
            else:
                version = str(self.get_active_constraint_version(map_id) or "").strip()
        if not version:
            return {
                "map_id": map_id,
                "map_md5": str(map_md5_hint or "").strip(),
                "constraint_version": "",
                "no_go_areas": [],
                "virtual_walls": [],
            }

        cur = self.conn.cursor()
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

    def list_map_alignment_configs(self, *, map_name: str = "") -> List[Dict[str, Any]]:
        map_name = _normalize_map_name(map_name)
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
        alignment_version: str = "",
        active_only: bool = False,
    ) -> Optional[Dict[str, Any]]:
        map_name = _normalize_map_name(map_name)
        if not map_name:
            return None
        alignment_version = str(alignment_version or "").strip()
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
        map_name = _normalize_map_name(map_name)
        if not map_name:
            raise ValueError("map_name is required for alignment config")
        alignment_version = str(alignment_version or "").strip()
        if not alignment_version:
            raise ValueError("alignment_version is required")
        existing = self.get_map_alignment_config(map_name=map_name, alignment_version=alignment_version)
        ts = _now_ts()
        created = float(created_ts if created_ts is not None else (existing or {}).get("created_ts") or ts)
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
        return self.get_map_alignment_config(map_name=map_name, alignment_version=alignment_version) or {}

    def set_active_map_alignment(self, *, map_name: str, alignment_version: str) -> Optional[Dict[str, Any]]:
        map_name = _normalize_map_name(map_name)
        alignment_version = str(alignment_version or "").strip()
        if not map_name or not alignment_version:
            raise ValueError("map_name and alignment_version are required")
        row = self.get_map_alignment_config(map_name=map_name, alignment_version=alignment_version)
        if not row:
            raise KeyError("alignment config not found")
        ts = _now_ts()
        cur = self.conn.cursor()
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
        return self.get_map_alignment_config(map_name=map_name, alignment_version=alignment_version)

    def archive_map_alignment_config(self, *, map_name: str, alignment_version: str) -> None:
        map_name = _normalize_map_name(map_name)
        alignment_version = str(alignment_version or "").strip()
        if not map_name or not alignment_version:
            raise ValueError("map_name and alignment_version are required")
        ts = _now_ts()
        cur = self.conn.cursor()
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
        map_name = _normalize_map_name(map_name)
        if not map_name:
            raise ValueError("map_name is required for zone editor metadata")
        ts = _now_ts()
        self.conn.execute(
            """
            INSERT INTO zone_editor_metadata(
              map_name, zone_id, zone_version, alignment_version, display_frame, display_outer_json,
              display_holes_json, profile_name, preview_plan_id, estimated_length_m, estimated_duration_s,
              warnings_json, updated_ts
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(map_name, zone_id, zone_version) DO UPDATE SET
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
        zone_id: str,
        zone_version: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        map_name = _normalize_map_name(map_name)
        if not map_name:
            return None
        zone_id = str(zone_id or "").strip()
        if zone_version is None:
            zone = self.get_zone_meta(zone_id, map_name=map_name)
            if not zone:
                return None
            zone_version = int(zone.get("zone_version") or zone.get("current_zone_version") or 0)
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
        include_disabled: bool = False,
        map_version: str = "",
    ) -> List[Dict[str, Any]]:
        map_name = _normalize_map_name(map_name)
        if not map_name:
            active = self.get_active_map()
            map_name = _normalize_map_name((active or {}).get("map_name") or "")
        if not map_name:
            return []
        map_version = str(map_version or "").strip()
        params: List[Any] = [map_name]
        sql = """
            SELECT
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
              ON zv.map_name=z.map_name
             AND zv.zone_id=z.zone_id
             AND zv.zone_version=z.current_zone_version
            WHERE z.map_name=?
        """
        if not bool(include_disabled):
            sql += " AND z.enabled=1"
        if map_version:
            sql += " AND (zv.map_md5=? OR zv.map_md5='' OR zv.map_md5 IS NULL)"
            params.append(map_version)
        sql += " ORDER BY z.updated_ts DESC, z.zone_id ASC;"
        rows = self.conn.execute(sql, tuple(params)).fetchall()
        out: List[Dict[str, Any]] = []
        for row in rows or []:
            out.append(
                {
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

    def get_zone_meta(self, zone_id: str, *, map_name: str = "", map_version: str = "") -> Optional[Dict[str, Any]]:
        map_name = _normalize_map_name(map_name)
        if not map_name:
            active = self.get_active_map()
            map_name = _normalize_map_name((active or {}).get("map_name") or "")
        if not map_name:
            return None
        map_version = str(map_version or "").strip()
        sql = """
            SELECT
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
              ON zv.map_name=z.map_name
             AND zv.zone_id=z.zone_id
             AND zv.zone_version=z.current_zone_version
            WHERE z.map_name=? AND z.zone_id=?
        """
        args: List[Any] = [map_name, str(zone_id or "").strip()]
        if map_version:
            sql += " AND (zv.map_md5=? OR zv.map_md5='' OR zv.map_md5 IS NULL)"
            args.append(map_version)
        sql += " LIMIT 1;"
        row = self.conn.execute(sql, tuple(args)).fetchone()
        if not row:
            return None
        return {
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
        map_version: str = "",
    ) -> Optional[Dict[str, Any]]:
        map_name = _normalize_map_name(map_name)
        if not map_name:
            active = self.get_active_map()
            map_name = _normalize_map_name((active or {}).get("map_name") or "")
        if not map_name:
            raise ValueError("map_name is required")
        zone_id = str(zone_id or "").strip()
        if not zone_id:
            raise ValueError("zone_id is required")
        current = self.get_zone_meta(zone_id, map_name=map_name, map_version=map_version)
        if not current:
            return None
        ts = _now_ts()
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
        return self.get_zone_meta(zone_id, map_name=map_name, map_version=map_version)

    def list_active_plan_refs(
        self,
        zone_id: str,
        *,
        map_name: str = "",
    ) -> List[Dict[str, Any]]:
        map_name = _normalize_map_name(map_name)
        if not map_name:
            active = self.get_active_map()
            map_name = _normalize_map_name((active or {}).get("map_name") or "")
        if not map_name:
            return []
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
    ) -> Optional[str]:
        map_name = _normalize_map_name(map_name)
        if not map_name:
            active = self.get_active_map()
            map_name = _normalize_map_name((active or {}).get("map_name") or "")
        if not map_name:
            return None
        zone_id = str(zone_id or "").strip()
        profile = str(plan_profile_name or "").strip()
        if profile:
            row = self.conn.execute(
                """
                SELECT active_plan_id FROM zone_active_plans
                WHERE map_name=? AND zone_id=? AND plan_profile_name=?
                LIMIT 1;
                """,
                (map_name, zone_id, profile),
            ).fetchone()
            return str(row["active_plan_id"]) if row and row["active_plan_id"] else None
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
    ) -> Optional[str]:
        map_name = _normalize_map_name(map_name)
        if not map_name:
            active = self.get_active_map()
            map_name = _normalize_map_name((active or {}).get("map_name") or "")
        if not map_name:
            return None
        zone_id = str(zone_id or "").strip()
        if zone_version is None:
            row = self.conn.execute(
                """
                SELECT plan_id FROM plans
                WHERE map_name=? AND zone_id=?
                ORDER BY created_ts DESC LIMIT 1;
                """,
                (map_name, zone_id),
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
    ) -> Optional[str]:
        plan_profile_name = str(plan_profile_name or "").strip()
        if not plan_profile_name:
            return self.get_latest_plan_id(zone_id, zone_version=zone_version, map_name=map_name)
        map_name = _normalize_map_name(map_name)
        if not map_name:
            active = self.get_active_map()
            map_name = _normalize_map_name((active or {}).get("map_name") or "")
        if not map_name:
            return None
        if zone_version is None:
            row = self.conn.execute(
                """
                SELECT plan_id FROM plans
                WHERE map_name=? AND zone_id=? AND plan_profile_name=?
                ORDER BY created_ts DESC LIMIT 1;
                """,
                (map_name, str(zone_id or "").strip(), plan_profile_name),
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
        d["profile_name"] = d.get("plan_profile_name") or ""
        d["map_name"] = str(d.get("map_name") or "")
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

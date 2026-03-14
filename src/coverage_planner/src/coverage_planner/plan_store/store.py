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


class PlanStore:
    """SQLite store for planning assets.

    Canonical planning schema:
      - zones              : business zones (latest/current metadata only)
      - zone_versions      : immutable geometry/map snapshots
      - plan_profiles      : planning profile catalog (cover_standard, cover_eco, ...)
      - plans              : immutable planning results
      - plan_blocks        : one blob row per block
      - zone_active_plans  : active plan binding by (zone_id, plan_profile_name)
      - map_assets / map_asset_versions / robot_active_map : static map assets & active selection
      - map_zones / map_zone_versions / map_zone_active_plans : zone/plan bindings scoped by map asset
      - map_constraint_versions       : immutable map-level keepout snapshots
      - map_no_go_areas              : polygon keepout assets by snapshot
      - map_virtual_walls            : polyline keepout assets by snapshot
      - map_active_constraint_versions : active snapshot pointer by map_id
    """

    def __init__(self, db_path: str):
        self.db_path = os.path.expanduser(str(db_path))
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True) if os.path.dirname(self.db_path) else None
        self.conn = sqlite3.connect(self.db_path, timeout=30.0, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self.conn.execute("PRAGMA foreign_keys=ON;")
        self._init_schema()

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass

    def _table_columns(self, table_name: str) -> List[str]:
        cur = self.conn.cursor()
        rows = cur.execute("PRAGMA table_info(%s);" % str(table_name)).fetchall()
        return [str(r["name"]) for r in rows or [] if r and r["name"]]

    def _ensure_column(self, table_name: str, column_name: str, column_sql: str):
        if str(column_name or "") in self._table_columns(table_name):
            return
        self.conn.execute(
            "ALTER TABLE %s ADD COLUMN %s %s;" % (str(table_name), str(column_name), str(column_sql))
        )
        self.conn.commit()

    def _init_schema(self):
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS zones (
              zone_id TEXT PRIMARY KEY,
              display_name TEXT,
              enabled INTEGER NOT NULL DEFAULT 1,
              current_zone_version INTEGER NOT NULL DEFAULT 0,
              updated_ts REAL NOT NULL
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS zone_versions (
              zone_id TEXT NOT NULL,
              zone_version INTEGER NOT NULL,
              frame_id TEXT,
              outer_json TEXT,
              holes_json TEXT,
              map_id TEXT,
              map_md5 TEXT,
              created_ts REAL NOT NULL,
              PRIMARY KEY(zone_id, zone_version)
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
              zone_id TEXT,
              zone_version INTEGER,
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
              zone_id TEXT NOT NULL,
              plan_profile_name TEXT NOT NULL,
              active_plan_id TEXT NOT NULL,
              updated_ts REAL NOT NULL,
              PRIMARY KEY(zone_id, plan_profile_name)
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
              created_ts REAL NOT NULL,
              updated_ts REAL NOT NULL
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS map_asset_versions (
              map_name TEXT NOT NULL,
              map_version TEXT NOT NULL,
              map_id TEXT,
              map_md5 TEXT,
              yaml_path TEXT NOT NULL,
              pgm_path TEXT NOT NULL,
              pbstream_path TEXT,
              frame_id TEXT,
              resolution REAL,
              origin_json TEXT,
              created_ts REAL NOT NULL,
              PRIMARY KEY(map_name, map_version)
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS robot_active_map (
              robot_id TEXT PRIMARY KEY,
              map_name TEXT NOT NULL,
              map_version TEXT NOT NULL,
              updated_ts REAL NOT NULL
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS map_zones (
              map_name TEXT NOT NULL,
              map_version TEXT NOT NULL,
              zone_id TEXT NOT NULL,
              display_name TEXT,
              enabled INTEGER NOT NULL DEFAULT 1,
              current_zone_version INTEGER NOT NULL DEFAULT 0,
              updated_ts REAL NOT NULL,
              PRIMARY KEY(map_name, map_version, zone_id)
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS map_zone_versions (
              map_name TEXT NOT NULL,
              map_version TEXT NOT NULL,
              zone_id TEXT NOT NULL,
              zone_version INTEGER NOT NULL,
              frame_id TEXT,
              outer_json TEXT,
              holes_json TEXT,
              map_id TEXT,
              map_md5 TEXT,
              created_ts REAL NOT NULL,
              PRIMARY KEY(map_name, map_version, zone_id, zone_version)
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS map_zone_active_plans (
              map_name TEXT NOT NULL,
              map_version TEXT NOT NULL,
              zone_id TEXT NOT NULL,
              plan_profile_name TEXT NOT NULL,
              active_plan_id TEXT NOT NULL,
              updated_ts REAL NOT NULL,
              PRIMARY KEY(map_name, map_version, zone_id, plan_profile_name)
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

        cur.execute("CREATE INDEX IF NOT EXISTS idx_zone_versions_zone ON zone_versions(zone_id, zone_version);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_plans_zone ON plans(zone_id, zone_version, created_ts);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_plans_zone_profile ON plans(zone_id, plan_profile_name, created_ts);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_blocks_plan ON plan_blocks(plan_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_active_plans_zone ON zone_active_plans(zone_id, updated_ts);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_map_asset_versions_name ON map_asset_versions(map_name, created_ts);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_map_asset_versions_identity ON map_asset_versions(map_id, map_md5, created_ts);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_map_zones_scope ON map_zones(map_name, map_version, zone_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_map_zone_versions_scope ON map_zone_versions(map_name, map_version, zone_id, zone_version);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_map_zone_active_plans_scope ON map_zone_active_plans(map_name, map_version, zone_id, updated_ts);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_constraint_versions_map ON map_constraint_versions(map_id, updated_ts);")
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_no_go_areas_map ON map_no_go_areas(map_id, constraint_version, updated_ts);"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_virtual_walls_map ON map_virtual_walls(map_id, constraint_version, updated_ts);"
        )

        self._ensure_column("plans", "constraint_version", "TEXT")
        self._ensure_column("zones", "map_name", "TEXT")
        self._ensure_column("zones", "map_version", "TEXT")
        self._ensure_column("zone_versions", "map_name", "TEXT")
        self._ensure_column("zone_versions", "map_version", "TEXT")
        self._ensure_column("zone_active_plans", "map_name", "TEXT")
        self._ensure_column("zone_active_plans", "map_version", "TEXT")
        self._ensure_column("plans", "map_name", "TEXT")
        self._ensure_column("plans", "map_version", "TEXT")

        # Seed built-in plan profiles once.
        now = _now_ts()
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

    def upsert_plan_profile(
        self,
        *,
        plan_profile_name: str,
        description: str = "",
        enabled: bool = True,
        usage_type: str = "normal",
    ):
        cur = self.conn.cursor()
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
        self.conn.commit()

    def upsert_map_asset(
        self,
        *,
        map_name: str,
        display_name: str = "",
        enabled: bool = True,
        description: str = "",
    ):
        map_name = str(map_name or "").strip()
        if not map_name:
            raise ValueError("map_name is required")
        ts = _now_ts()
        self.conn.execute(
            """
            INSERT INTO map_assets(map_name, display_name, enabled, description, created_ts, updated_ts)
            VALUES(?,?,?,?,?,?)
            ON CONFLICT(map_name) DO UPDATE SET
              display_name=excluded.display_name,
              enabled=excluded.enabled,
              description=excluded.description,
              updated_ts=excluded.updated_ts;
            """,
            (
                map_name,
                str(display_name or map_name).strip(),
                1 if bool(enabled) else 0,
                str(description or "").strip(),
                ts,
                ts,
            ),
        )
        self.conn.commit()

    def register_map_asset_version(
        self,
        *,
        map_name: str,
        map_version: str,
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
        map_name = str(map_name or "").strip()
        map_version = str(map_version or "").strip()
        if not map_name or not map_version:
            raise ValueError("map_name and map_version are required")
        self.upsert_map_asset(
            map_name=map_name,
            display_name=(display_name or map_name),
            enabled=enabled,
            description=description,
        )
        ts = _now_ts()
        self.conn.execute(
            """
            INSERT INTO map_asset_versions(
              map_name, map_version, map_id, map_md5, yaml_path, pgm_path, pbstream_path,
              frame_id, resolution, origin_json, created_ts
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(map_name, map_version) DO UPDATE SET
              map_id=excluded.map_id,
              map_md5=excluded.map_md5,
              yaml_path=excluded.yaml_path,
              pgm_path=excluded.pgm_path,
              pbstream_path=excluded.pbstream_path,
              frame_id=excluded.frame_id,
              resolution=excluded.resolution,
              origin_json=excluded.origin_json;
            """,
            (
                map_name,
                map_version,
                str(map_id or "").strip(),
                str(map_md5 or "").strip(),
                os.path.expanduser(str(yaml_path or "").strip()),
                os.path.expanduser(str(pgm_path or "").strip()),
                os.path.expanduser(str(pbstream_path or "").strip()),
                str(frame_id or "map").strip(),
                float(resolution or 0.0),
                json.dumps(list(origin or [0.0, 0.0, 0.0]), ensure_ascii=False, separators=(",", ":")),
                ts,
            ),
        )
        if set_active:
            self.conn.execute(
                """
                INSERT INTO robot_active_map(robot_id, map_name, map_version, updated_ts)
                VALUES(?,?,?,?)
                ON CONFLICT(robot_id) DO UPDATE SET
                  map_name=excluded.map_name,
                  map_version=excluded.map_version,
                  updated_ts=excluded.updated_ts;
                """,
                (str(robot_id or "local_robot").strip(), map_name, map_version, ts),
            )
        self.conn.commit()

    def set_active_map(self, *, map_name: str, map_version: str, robot_id: str = "local_robot"):
        map_name = str(map_name or "").strip()
        map_version = str(map_version or "").strip()
        if not map_name or not map_version:
            raise ValueError("map_name and map_version are required")
        ts = _now_ts()
        self.conn.execute(
            """
            INSERT INTO robot_active_map(robot_id, map_name, map_version, updated_ts)
            VALUES(?,?,?,?)
            ON CONFLICT(robot_id) DO UPDATE SET
              map_name=excluded.map_name,
              map_version=excluded.map_version,
              updated_ts=excluded.updated_ts;
            """,
            (str(robot_id or "local_robot").strip(), map_name, map_version, ts),
        )
        self.conn.commit()

    def get_latest_map_version(self, map_name: str) -> Optional[str]:
        row = self.conn.cursor().execute(
            """
            SELECT map_version
            FROM map_asset_versions
            WHERE map_name=?
            ORDER BY created_ts DESC, map_version DESC
            LIMIT 1;
            """,
            (str(map_name or "").strip(),),
        ).fetchone()
        if not row:
            return None
        return str(row["map_version"] or "") or None

    def get_active_map(self, robot_id: str = "local_robot") -> Optional[Dict[str, Any]]:
        row = self.conn.cursor().execute(
            """
            SELECT
              ram.robot_id,
              ram.map_name,
              ram.map_version,
              ram.updated_ts,
              mav.map_id,
              mav.map_md5,
              mav.yaml_path,
              mav.pgm_path,
              mav.pbstream_path,
              mav.frame_id,
              mav.resolution,
              mav.origin_json
            FROM robot_active_map ram
            LEFT JOIN map_asset_versions mav
              ON mav.map_name=ram.map_name AND mav.map_version=ram.map_version
            WHERE ram.robot_id=?
            LIMIT 1;
            """,
            (str(robot_id or "local_robot").strip(),),
        ).fetchone()
        if not row:
            return None
        return {
            "robot_id": str(row["robot_id"] or ""),
            "map_name": str(row["map_name"] or ""),
            "map_version": str(row["map_version"] or ""),
            "map_id": str(row["map_id"] or ""),
            "map_md5": str(row["map_md5"] or ""),
            "yaml_path": str(row["yaml_path"] or ""),
            "pgm_path": str(row["pgm_path"] or ""),
            "pbstream_path": str(row["pbstream_path"] or ""),
            "frame_id": str(row["frame_id"] or ""),
            "resolution": float(row["resolution"] or 0.0),
            "origin": json.loads(row["origin_json"] or "[0,0,0]"),
            "updated_ts": float(row["updated_ts"] or 0.0),
        }

    def resolve_map_asset_version(
        self,
        *,
        map_name: str = "",
        map_version: str = "",
        robot_id: str = "local_robot",
    ) -> Optional[Dict[str, Any]]:
        map_name = str(map_name or "").strip()
        map_version = str(map_version or "").strip()
        if not map_name:
            return self.get_active_map(robot_id=robot_id)
        if not map_version:
            active = self.get_active_map(robot_id=robot_id)
            if active and str(active.get("map_name") or "").strip() == map_name:
                map_version = str(active.get("map_version") or "").strip()
            if not map_version:
                map_version = str(self.get_latest_map_version(map_name) or "").strip()
        if not map_version:
            return None
        row = self.conn.cursor().execute(
            """
            SELECT
              ma.map_name,
              ma.display_name,
              ma.enabled,
              ma.description,
              mav.map_version,
              mav.map_id,
              mav.map_md5,
              mav.yaml_path,
              mav.pgm_path,
              mav.pbstream_path,
              mav.frame_id,
              mav.resolution,
              mav.origin_json,
              mav.created_ts
            FROM map_assets ma
            JOIN map_asset_versions mav
              ON mav.map_name=ma.map_name
            WHERE ma.map_name=? AND mav.map_version=?
            LIMIT 1;
            """,
            (map_name, map_version),
        ).fetchone()
        if not row:
            return None
        return {
            "map_name": str(row["map_name"] or ""),
            "display_name": str(row["display_name"] or ""),
            "enabled": bool(int(row["enabled"] or 0)),
            "description": str(row["description"] or ""),
            "map_version": str(row["map_version"] or ""),
            "map_id": str(row["map_id"] or ""),
            "map_md5": str(row["map_md5"] or ""),
            "yaml_path": str(row["yaml_path"] or ""),
            "pgm_path": str(row["pgm_path"] or ""),
            "pbstream_path": str(row["pbstream_path"] or ""),
            "frame_id": str(row["frame_id"] or ""),
            "resolution": float(row["resolution"] or 0.0),
            "origin": json.loads(row["origin_json"] or "[0,0,0]"),
            "created_ts": float(row["created_ts"] or 0.0),
        }

    def list_map_assets(self) -> List[Dict[str, Any]]:
        rows = self.conn.cursor().execute(
            """
            SELECT ma.map_name, ma.display_name, ma.enabled, ma.description,
                   mav.map_version, mav.map_id, mav.map_md5, mav.yaml_path, mav.pgm_path, mav.pbstream_path, mav.created_ts
            FROM map_assets ma
            LEFT JOIN map_asset_versions mav
              ON mav.map_name=ma.map_name
            ORDER BY ma.map_name ASC, mav.created_ts DESC;
            """
        ).fetchall()
        out: List[Dict[str, Any]] = []
        for row in rows or []:
            out.append(
                {
                    "map_name": str(row["map_name"] or ""),
                    "display_name": str(row["display_name"] or ""),
                    "enabled": bool(int(row["enabled"] or 0)),
                    "description": str(row["description"] or ""),
                    "map_version": str(row["map_version"] or ""),
                    "map_id": str(row["map_id"] or ""),
                    "map_md5": str(row["map_md5"] or ""),
                    "yaml_path": str(row["yaml_path"] or ""),
                    "pgm_path": str(row["pgm_path"] or ""),
                    "pbstream_path": str(row["pbstream_path"] or ""),
                    "created_ts": float(row["created_ts"] or 0.0),
                }
            )
        return out

    def list_map_asset_versions(self, map_name: str = "") -> List[Dict[str, Any]]:
        map_name = str(map_name or "").strip()
        where = ""
        args: Tuple[Any, ...] = ()
        if map_name:
            where = "WHERE ma.map_name=?"
            args = (map_name,)
        rows = self.conn.cursor().execute(
            """
            SELECT ma.map_name, ma.display_name, ma.enabled, ma.description,
                   mav.map_version, mav.map_id, mav.map_md5, mav.yaml_path, mav.pgm_path, mav.pbstream_path,
                   mav.frame_id, mav.resolution, mav.origin_json, mav.created_ts
            FROM map_assets ma
            JOIN map_asset_versions mav
              ON mav.map_name=ma.map_name
            %s
            ORDER BY ma.map_name ASC, mav.created_ts DESC, mav.map_version DESC;
            """ % where,
            args,
        ).fetchall()
        out: List[Dict[str, Any]] = []
        for row in rows or []:
            out.append(
                {
                    "map_name": str(row["map_name"] or ""),
                    "display_name": str(row["display_name"] or ""),
                    "enabled": bool(int(row["enabled"] or 0)),
                    "description": str(row["description"] or ""),
                    "map_version": str(row["map_version"] or ""),
                    "map_id": str(row["map_id"] or ""),
                    "map_md5": str(row["map_md5"] or ""),
                    "yaml_path": str(row["yaml_path"] or ""),
                    "pgm_path": str(row["pgm_path"] or ""),
                    "pbstream_path": str(row["pbstream_path"] or ""),
                    "frame_id": str(row["frame_id"] or ""),
                    "resolution": float(row["resolution"] or 0.0),
                    "origin": json.loads(row["origin_json"] or "[0,0,0]"),
                    "created_ts": float(row["created_ts"] or 0.0),
                }
            )
        return out

    def update_map_asset_meta(
        self,
        *,
        map_name: str,
        display_name: Optional[str] = None,
        description: Optional[str] = None,
        enabled: Optional[bool] = None,
    ):
        map_name = str(map_name or "").strip()
        if not map_name:
            raise ValueError("map_name is required")
        row = self.conn.cursor().execute(
            "SELECT display_name, description, enabled FROM map_assets WHERE map_name=? LIMIT 1;",
            (map_name,),
        ).fetchone()
        if not row:
            raise KeyError("map asset not found: %s" % map_name)
        self.upsert_map_asset(
            map_name=map_name,
            display_name=str(display_name if display_name is not None else row["display_name"] or map_name),
            description=str(description if description is not None else row["description"] or ""),
            enabled=bool(enabled if enabled is not None else int(row["enabled"] or 0)),
        )

    def disable_map_asset(self, *, map_name: str):
        self.update_map_asset_meta(map_name=map_name, enabled=False)

    def find_map_asset_versions_by_identity(self, *, map_id: str = "", map_md5: str = "") -> List[Dict[str, Any]]:
        map_id = str(map_id or "").strip()
        map_md5 = str(map_md5 or "").strip()
        if not map_id and not map_md5:
            return []
        where = []
        args: List[Any] = []
        if map_id:
            where.append("map_id=?")
            args.append(map_id)
        if map_md5:
            where.append("map_md5=?")
            args.append(map_md5)
        rows = self.conn.cursor().execute(
            """
            SELECT map_name, map_version, map_id, map_md5, yaml_path, pgm_path, pbstream_path, frame_id, resolution, origin_json, created_ts
            FROM map_asset_versions
            WHERE %s
            ORDER BY created_ts DESC;
            """ % (" AND ".join(where)),
            tuple(args),
        ).fetchall()
        out: List[Dict[str, Any]] = []
        for row in rows or []:
            out.append(
                {
                    "map_name": str(row["map_name"] or ""),
                    "map_version": str(row["map_version"] or ""),
                    "map_id": str(row["map_id"] or ""),
                    "map_md5": str(row["map_md5"] or ""),
                    "yaml_path": str(row["yaml_path"] or ""),
                    "pgm_path": str(row["pgm_path"] or ""),
                    "pbstream_path": str(row["pbstream_path"] or ""),
                    "frame_id": str(row["frame_id"] or ""),
                    "resolution": float(row["resolution"] or 0.0),
                    "origin": json.loads(row["origin_json"] or "[0,0,0]"),
                    "created_ts": float(row["created_ts"] or 0.0),
                }
            )
        return out

    def backfill_legacy_map_scope(self, *, map_name: str, map_version: str, map_id: str = "", map_md5: str = ""):
        map_name = str(map_name or "").strip()
        map_version = str(map_version or "").strip()
        map_id = str(map_id or "").strip()
        map_md5 = str(map_md5 or "").strip()
        if not map_name or not map_version:
            raise ValueError("map_name and map_version are required")
        if not map_id and not map_md5:
            return

        where = []
        args: List[Any] = []
        if map_id:
            where.append("map_id=?")
            args.append(map_id)
        if map_md5:
            where.append("map_md5=?")
            args.append(map_md5)
        pred = " AND ".join(where)

        cur = self.conn.cursor()
        cur.execute(
            "UPDATE plans SET map_name=?, map_version=? WHERE %s;" % pred,
            (map_name, map_version, *args),
        )
        cur.execute(
            "UPDATE zone_versions SET map_name=?, map_version=? WHERE %s;" % pred,
            (map_name, map_version, *args),
        )
        cur.execute(
            "UPDATE zones SET map_name=?, map_version=? WHERE zone_id IN (SELECT zone_id FROM zone_versions WHERE %s);" % pred,
            (map_name, map_version, *args),
        )
        cur.execute(
            """
            INSERT INTO map_zone_versions(map_name, map_version, zone_id, zone_version, frame_id, outer_json, holes_json, map_id, map_md5, created_ts)
            SELECT ?, ?, zone_id, zone_version, frame_id, outer_json, holes_json, map_id, map_md5, created_ts
            FROM zone_versions
            WHERE %s
            ON CONFLICT(map_name, map_version, zone_id, zone_version) DO UPDATE SET
              frame_id=excluded.frame_id,
              outer_json=excluded.outer_json,
              holes_json=excluded.holes_json,
              map_id=excluded.map_id,
              map_md5=excluded.map_md5;
            """ % pred,
            (map_name, map_version, *args),
        )
        cur.execute(
            """
            INSERT INTO map_zones(map_name, map_version, zone_id, display_name, enabled, current_zone_version, updated_ts)
            SELECT ?, ?, z.zone_id, z.display_name, z.enabled, z.current_zone_version, z.updated_ts
            FROM zones z
            WHERE z.zone_id IN (SELECT zone_id FROM zone_versions WHERE %s)
            ON CONFLICT(map_name, map_version, zone_id) DO UPDATE SET
              display_name=excluded.display_name,
              enabled=excluded.enabled,
              current_zone_version=excluded.current_zone_version,
              updated_ts=excluded.updated_ts;
            """ % pred,
            (map_name, map_version, *args),
        )
        cur.execute(
            """
            INSERT INTO map_zone_active_plans(map_name, map_version, zone_id, plan_profile_name, active_plan_id, updated_ts)
            SELECT ?, ?, zap.zone_id, zap.plan_profile_name, zap.active_plan_id, zap.updated_ts
            FROM zone_active_plans zap
            JOIN plans p ON p.plan_id=zap.active_plan_id
            WHERE %s
            ON CONFLICT(map_name, map_version, zone_id, plan_profile_name) DO UPDATE SET
              active_plan_id=excluded.active_plan_id,
              updated_ts=excluded.updated_ts;
            """ % pred,
            (map_name, map_version, *args),
        )
        self.conn.commit()

    def _upsert_zone_version(
        self,
        *,
        map_name: str = "",
        map_version: str = "",
        zone_id: str,
        zone_version: int,
        frame_id: str,
        outer: List[Tuple[float, float]],
        holes: List[List[Tuple[float, float]]],
        map_id: str = "",
        map_md5: str = "",
    ):
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO zone_versions(zone_id, zone_version, frame_id, outer_json, holes_json, map_id, map_md5, created_ts, map_name, map_version)
            VALUES(?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(zone_id, zone_version) DO UPDATE SET
              frame_id=excluded.frame_id,
              outer_json=excluded.outer_json,
              holes_json=excluded.holes_json,
              map_id=excluded.map_id,
              map_md5=excluded.map_md5,
              map_name=excluded.map_name,
              map_version=excluded.map_version;
            """,
            (
                str(zone_id or "").strip(),
                int(zone_version),
                str(frame_id or "").strip(),
                json.dumps([[float(x), float(y)] for x, y in (outer or [])], separators=(",", ":")),
                json.dumps(
                    [[[float(x), float(y)] for x, y in ring] for ring in (holes or [])],
                    separators=(",", ":"),
                ),
                str(map_id or "").strip(),
                str(map_md5 or "").strip(),
                _now_ts(),
                str(map_name or "").strip(),
                str(map_version or "").strip(),
            ),
        )
        cur.execute(
            """
            INSERT INTO zones(zone_id, display_name, enabled, current_zone_version, updated_ts, map_name, map_version)
            VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(zone_id) DO UPDATE SET
              current_zone_version=excluded.current_zone_version,
              map_name=excluded.map_name,
              map_version=excluded.map_version,
              updated_ts=excluded.updated_ts;
            """,
            (
                str(zone_id or "").strip(),
                str(zone_id or "").strip(),
                1,
                int(zone_version),
                _now_ts(),
                str(map_name or "").strip(),
                str(map_version or "").strip(),
            ),
        )
        if str(map_name or "").strip() and str(map_version or "").strip():
            cur.execute(
                """
                INSERT INTO map_zone_versions(
                  map_name, map_version, zone_id, zone_version, frame_id, outer_json, holes_json, map_id, map_md5, created_ts
                ) VALUES(?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(map_name, map_version, zone_id, zone_version) DO UPDATE SET
                  frame_id=excluded.frame_id,
                  outer_json=excluded.outer_json,
                  holes_json=excluded.holes_json,
                  map_id=excluded.map_id,
                  map_md5=excluded.map_md5;
                """,
                (
                    str(map_name or "").strip(),
                    str(map_version or "").strip(),
                    str(zone_id or "").strip(),
                    int(zone_version),
                    str(frame_id or "").strip(),
                    json.dumps([[float(x), float(y)] for x, y in (outer or [])], separators=(",", ":")),
                    json.dumps(
                        [[[float(x), float(y)] for x, y in ring] for ring in (holes or [])],
                        separators=(",", ":"),
                    ),
                    str(map_id or "").strip(),
                    str(map_md5 or "").strip(),
                    _now_ts(),
                ),
            )
            cur.execute(
                """
                INSERT INTO map_zones(map_name, map_version, zone_id, display_name, enabled, current_zone_version, updated_ts)
                VALUES(?,?,?,?,?,?,?)
                ON CONFLICT(map_name, map_version, zone_id) DO UPDATE SET
                  display_name=excluded.display_name,
                  enabled=excluded.enabled,
                  current_zone_version=excluded.current_zone_version,
                  updated_ts=excluded.updated_ts;
                """,
                (
                    str(map_name or "").strip(),
                    str(map_version or "").strip(),
                    str(zone_id or "").strip(),
                    str(zone_id or "").strip(),
                    1,
                    int(zone_version),
                    _now_ts(),
                ),
            )

    # ---------- public API ----------
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
        map_version: str = "",
        map_id: str = "",
        map_md5: str = "",
        constraint_version: str = "",
        planner_version: str = "",
    ) -> str:
        plan_id = str(uuid.uuid4())
        created_ts = _now_ts()

        map_name = str(map_name or "").strip()
        map_version = str(map_version or "").strip()
        plan_profile_name = str(plan_profile_name or "").strip()
        map_id = str(map_id or "").strip()
        map_md5 = str(map_md5 or "").strip()
        constraint_version = str(constraint_version or "").strip()
        planner_version = str(planner_version or "").strip()

        self.upsert_plan_profile(plan_profile_name=plan_profile_name or "cover_standard")
        self._upsert_zone_version(
            map_name=map_name,
            map_version=map_version,
            zone_id=str(zone_id),
            zone_version=int(zone_version),
            frame_id=str(frame_id),
            outer=outer or [],
            holes=holes or [],
            map_id=map_id,
            map_md5=map_md5,
        )

        params_json = json.dumps(params, ensure_ascii=False, separators=(",", ":"))
        robot_json = json.dumps(robot_spec, ensure_ascii=False, separators=(",", ":"))

        blocks = getattr(plan_result, "blocks", []) or []
        exec_order = getattr(plan_result, "exec_order", []) or []
        exec_order_json = json.dumps([int(i) for i in exec_order], separators=(",", ":"))

        total_len = getattr(plan_result, "total_length_m", None)
        if total_len is None:
            total_len = 0.0

        cur = self.conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO plans(
                  plan_id, zone_id, zone_version, frame_id, plan_profile_name,
                  params_json, robot_json, blocks, total_length_m, exec_order_json,
                  map_id, map_md5, map_name, map_version, constraint_version, planner_version, created_ts
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
                """,
                (
                    plan_id,
                    str(zone_id),
                    int(zone_version),
                    str(frame_id),
                    str(plan_profile_name or ""),
                    params_json,
                    robot_json,
                    int(len(blocks)),
                    float(total_len),
                    exec_order_json,
                    map_id,
                    map_md5,
                    map_name,
                    map_version,
                    constraint_version,
                    planner_version,
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
                        plan_id,
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
                cur.execute("UPDATE plans SET total_length_m=? WHERE plan_id=?;", (float(sum_len), plan_id))

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
        map_version: str = "",
        plan_profile_name: str,
        frame_id: Optional[str] = None,
        outer: Optional[list] = None,
        holes: Optional[list] = None,
        map_id: str = "",
        map_md5: str = "",
        display_name: Optional[str] = None,
    ):
        plan_profile_name = str(plan_profile_name or "").strip() or "cover_standard"
        map_name = str(map_name or "").strip()
        map_version = str(map_version or "").strip()
        map_id = str(map_id or "").strip()
        map_md5 = str(map_md5 or "").strip()
        self.upsert_plan_profile(plan_profile_name=plan_profile_name)

        if frame_id is not None and outer is not None and holes is not None:
            self._upsert_zone_version(
                map_name=map_name,
                map_version=map_version,
                zone_id=str(zone_id),
                zone_version=int(zone_version),
                frame_id=str(frame_id or ""),
                outer=outer or [],
                holes=holes or [],
                map_id=map_id,
                map_md5=map_md5,
            )

        ts = _now_ts()
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO zones(zone_id, display_name, enabled, current_zone_version, updated_ts, map_name, map_version)
            VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(zone_id) DO UPDATE SET
              display_name=excluded.display_name,
              current_zone_version=excluded.current_zone_version,
              map_name=excluded.map_name,
              map_version=excluded.map_version,
              updated_ts=excluded.updated_ts;
            """,
            (
                str(zone_id or "").strip(),
                str(display_name or zone_id or "").strip(),
                1,
                int(zone_version),
                ts,
                map_name,
                map_version,
            ),
        )
        cur.execute(
            """
            INSERT INTO zone_active_plans(zone_id, plan_profile_name, active_plan_id, updated_ts, map_name, map_version)
            VALUES(?,?,?,?,?,?)
            ON CONFLICT(zone_id, plan_profile_name) DO UPDATE SET
              active_plan_id=excluded.active_plan_id,
              map_name=excluded.map_name,
              map_version=excluded.map_version,
              updated_ts=excluded.updated_ts;
            """,
            (
                str(zone_id or "").strip(),
                plan_profile_name,
                str(plan_id or "").strip(),
                ts,
                map_name,
                map_version,
            ),
        )
        if map_name and map_version:
            cur.execute(
                """
                INSERT INTO map_zone_active_plans(map_name, map_version, zone_id, plan_profile_name, active_plan_id, updated_ts)
                VALUES(?,?,?,?,?,?)
                ON CONFLICT(map_name, map_version, zone_id, plan_profile_name) DO UPDATE SET
                  active_plan_id=excluded.active_plan_id,
                  updated_ts=excluded.updated_ts;
                """,
                (
                    map_name,
                    map_version,
                    str(zone_id or "").strip(),
                    plan_profile_name,
                    str(plan_id or "").strip(),
                    ts,
                ),
            )
        self.conn.commit()

    def get_active_constraint_version(self, map_id: str) -> Optional[str]:
        row = self.conn.cursor().execute(
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
                        json.dumps(polygon, ensure_ascii=False, separators=(",", ":")),
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
                        json.dumps(polyline, ensure_ascii=False, separators=(",", ":")),
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
                    "polygon": json.loads(row["polygon_json"] or "[]"),
                    "enabled": bool(int(row["enabled"] or 0)),
                }
            )

        virtual_walls = []
        for row in wall_rows or []:
            virtual_walls.append(
                {
                    "wall_id": str(row["wall_id"] or ""),
                    "name": str(row["name"] or ""),
                    "polyline": json.loads(row["polyline_json"] or "[]"),
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

    def get_zone_meta(self, zone_id: str, *, map_name: str = "", map_version: str = "") -> Optional[Dict[str, Any]]:
        cur = self.conn.cursor()
        map_name = str(map_name or "").strip()
        map_version = str(map_version or "").strip()
        if map_name:
            if not map_version:
                active = self.resolve_map_asset_version(map_name=map_name)
                map_version = str((active or {}).get("map_version") or "").strip()
            if map_version:
                row = cur.execute(
                    """
                    SELECT
                      z.map_name,
                      z.map_version,
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
                    FROM map_zones z
                    LEFT JOIN map_zone_versions zv
                      ON zv.map_name=z.map_name
                     AND zv.map_version=z.map_version
                     AND zv.zone_id=z.zone_id
                     AND zv.zone_version=z.current_zone_version
                    WHERE z.map_name=? AND z.map_version=? AND z.zone_id=?
                    LIMIT 1;
                    """,
                    (map_name, map_version, str(zone_id or "").strip()),
                ).fetchone()
                if row:
                    return {
                        "map_name": str(row["map_name"] or ""),
                        "map_version": str(row["map_version"] or ""),
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
        row = cur.execute(
            """
            SELECT
              z.zone_id,
              z.display_name,
              z.enabled,
              z.current_zone_version,
              z.updated_ts,
              z.map_name,
              z.map_version,
              zv.frame_id,
              zv.outer_json,
              zv.holes_json,
              zv.map_id,
              zv.map_md5
            FROM zones z
            LEFT JOIN zone_versions zv
              ON zv.zone_id=z.zone_id AND zv.zone_version=z.current_zone_version
            WHERE z.zone_id=?;
            """,
            (str(zone_id or "").strip(),),
        ).fetchone()
        if not row:
            return None
        return {
            "map_name": str(row["map_name"] or ""),
            "map_version": str(row["map_version"] or ""),
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

    def get_active_plan_id(
        self,
        zone_id: str,
        plan_profile_name: Optional[str] = None,
        *,
        map_name: str = "",
        map_version: str = "",
    ) -> Optional[str]:
        cur = self.conn.cursor()
        zone_id = str(zone_id or "").strip()
        profile = str(plan_profile_name or "").strip()
        map_name = str(map_name or "").strip()
        map_version = str(map_version or "").strip()
        if map_name:
            if not map_version:
                active = self.resolve_map_asset_version(map_name=map_name)
                map_version = str((active or {}).get("map_version") or "").strip()
            if map_version:
                if profile:
                    row = cur.execute(
                        """
                        SELECT active_plan_id FROM map_zone_active_plans
                        WHERE map_name=? AND map_version=? AND zone_id=? AND plan_profile_name=?
                        LIMIT 1;
                        """,
                        (map_name, map_version, zone_id, profile),
                    ).fetchone()
                    return str(row["active_plan_id"]) if row and row["active_plan_id"] else None
                row = cur.execute(
                    """
                    SELECT active_plan_id FROM map_zone_active_plans
                    WHERE map_name=? AND map_version=? AND zone_id=?
                    ORDER BY updated_ts DESC
                    LIMIT 1;
                    """,
                    (map_name, map_version, zone_id),
                ).fetchone()
                return str(row["active_plan_id"]) if row and row["active_plan_id"] else None
        if profile:
            row = cur.execute(
                """
                SELECT active_plan_id FROM zone_active_plans
                WHERE zone_id=? AND plan_profile_name=?
                LIMIT 1;
                """,
                (zone_id, profile),
            ).fetchone()
            return str(row["active_plan_id"]) if row and row["active_plan_id"] else None

        row = cur.execute(
            """
            SELECT active_plan_id FROM zone_active_plans
            WHERE zone_id=?
            ORDER BY updated_ts DESC
            LIMIT 1;
            """,
            (zone_id,),
        ).fetchone()
        return str(row["active_plan_id"]) if row and row["active_plan_id"] else None

    def get_latest_plan_id(
        self,
        zone_id: str,
        zone_version: Optional[int] = None,
        *,
        map_name: str = "",
        map_version: str = "",
    ) -> Optional[str]:
        cur = self.conn.cursor()
        zone_id = str(zone_id or "").strip()
        map_name = str(map_name or "").strip()
        map_version = str(map_version or "").strip()
        if map_name:
            if not map_version:
                active = self.resolve_map_asset_version(map_name=map_name)
                map_version = str((active or {}).get("map_version") or "").strip()
            if zone_version is None:
                row = cur.execute(
                    """
                    SELECT plan_id FROM plans
                    WHERE zone_id=? AND map_name=? AND map_version=?
                    ORDER BY created_ts DESC LIMIT 1;
                    """,
                    (zone_id, map_name, map_version),
                ).fetchone()
            else:
                row = cur.execute(
                    """
                    SELECT plan_id FROM plans
                    WHERE zone_id=? AND zone_version=? AND map_name=? AND map_version=?
                    ORDER BY created_ts DESC LIMIT 1;
                    """,
                    (zone_id, int(zone_version), map_name, map_version),
                ).fetchone()
            return str(row["plan_id"]) if row else None
        if zone_version is None:
            row = cur.execute(
                "SELECT plan_id FROM plans WHERE zone_id=? ORDER BY created_ts DESC LIMIT 1;",
                (zone_id,),
            ).fetchone()
        else:
            row = cur.execute(
                "SELECT plan_id FROM plans WHERE zone_id=? AND zone_version=? ORDER BY created_ts DESC LIMIT 1;",
                (zone_id, int(zone_version)),
            ).fetchone()
        return str(row["plan_id"]) if row else None

    def get_latest_plan_id_by_profile(
        self,
        zone_id: str,
        plan_profile_name: str,
        zone_version: Optional[int] = None,
        *,
        map_name: str = "",
        map_version: str = "",
    ) -> Optional[str]:
        zone_id = str(zone_id or "").strip()
        plan_profile_name = str(plan_profile_name or "").strip()
        if not plan_profile_name:
            return self.get_latest_plan_id(zone_id, zone_version=zone_version, map_name=map_name, map_version=map_version)
        cur = self.conn.cursor()
        map_name = str(map_name or "").strip()
        map_version = str(map_version or "").strip()
        if map_name:
            if not map_version:
                active = self.resolve_map_asset_version(map_name=map_name)
                map_version = str((active or {}).get("map_version") or "").strip()
            if zone_version is None:
                row = cur.execute(
                    """
                    SELECT plan_id FROM plans
                    WHERE zone_id=? AND plan_profile_name=? AND map_name=? AND map_version=?
                    ORDER BY created_ts DESC LIMIT 1;
                    """,
                    (zone_id, plan_profile_name, map_name, map_version),
                ).fetchone()
            else:
                row = cur.execute(
                    """
                    SELECT plan_id FROM plans
                    WHERE zone_id=? AND zone_version=? AND plan_profile_name=? AND map_name=? AND map_version=?
                    ORDER BY created_ts DESC LIMIT 1;
                    """,
                    (zone_id, int(zone_version), plan_profile_name, map_name, map_version),
                ).fetchone()
            return str(row["plan_id"]) if row else None
        if zone_version is None:
            row = cur.execute(
                """
                SELECT plan_id FROM plans
                WHERE zone_id=? AND plan_profile_name=?
                ORDER BY created_ts DESC LIMIT 1;
                """,
                (zone_id, plan_profile_name),
            ).fetchone()
        else:
            row = cur.execute(
                """
                SELECT plan_id FROM plans
                WHERE zone_id=? AND zone_version=? AND plan_profile_name=?
                ORDER BY created_ts DESC LIMIT 1;
                """,
                (zone_id, int(zone_version), plan_profile_name),
            ).fetchone()
        return str(row["plan_id"]) if row else None

    def load_plan_meta(self, plan_id: str) -> Dict[str, Any]:
        cur = self.conn.cursor()
        row = cur.execute("SELECT * FROM plans WHERE plan_id=?;", (str(plan_id or "").strip(),)).fetchone()
        if not row:
            raise KeyError(f"plan_id not found: {plan_id}")
        d = dict(row)
        for k in ("params_json", "robot_json", "exec_order_json"):
            if d.get(k):
                d[k] = json.loads(d[k])
        # Temporary alias for call sites that still read profile_name.
        d["profile_name"] = d.get("plan_profile_name") or ""
        d["map_name"] = str(d.get("map_name") or "")
        d["map_version"] = str(d.get("map_version") or "")
        return d

    def load_block(self, plan_id: str, block_id: int) -> Dict[str, Any]:
        cur = self.conn.cursor()
        row = cur.execute(
            "SELECT * FROM plan_blocks WHERE plan_id=? AND block_id=?;",
            (str(plan_id or "").strip(), int(block_id)),
        ).fetchone()
        if not row:
            raise KeyError(f"block not found: {plan_id} block {block_id}")
        d = dict(row)
        d["path_xyyaw"] = decode_path_xyyaw_f32(d["path_blob"])
        return d

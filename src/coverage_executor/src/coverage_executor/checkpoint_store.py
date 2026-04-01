# -*- coding: utf-8 -*-
import sqlite3
import time
from dataclasses import dataclass
from typing import Optional


@dataclass
class ExecCheckpoint:
    zone_id: str
    plan_id: str
    zone_version: int
    exec_index: int          # index in exec_order list (0..)
    block_id: int
    path_index: int          # index in block path list (0..)
    path_s: float            # meters from block path start to path_index (approx)
    state: str               # RUNNING/PAUSED/CANCELED/DONE/FAILED
    water_off_latched: bool  # last-block water-off latch
    last_x: float
    last_y: float
    last_yaw: float
    updated_ts: float        # wall time seconds


class CheckpointStore:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA temp_store=MEMORY;")
        return conn

    def ensure_schema(self):
        conn = self._connect()
        try:
            conn.execute("""
            CREATE TABLE IF NOT EXISTS exec_checkpoints (
              zone_id TEXT PRIMARY KEY,
              plan_id TEXT NOT NULL,
              zone_version INTEGER NOT NULL DEFAULT 0,
              exec_index INTEGER NOT NULL,
              block_id INTEGER NOT NULL,
              path_index INTEGER NOT NULL,
              path_s REAL NOT NULL,
              state TEXT NOT NULL,
              water_off_latched INTEGER NOT NULL DEFAULT 0,
              last_x REAL,
              last_y REAL,
              last_yaw REAL,
              updated_ts REAL NOT NULL
            );
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_exec_cp_plan ON exec_checkpoints(plan_id);")
            conn.commit()
        finally:
            conn.close()

    def load(self, zone_id: str) -> Optional[ExecCheckpoint]:
        conn = self._connect()
        try:
            cur = conn.execute("""
              SELECT zone_id, plan_id, zone_version, exec_index, block_id, path_index, path_s,
                     state, water_off_latched, last_x, last_y, last_yaw, updated_ts
              FROM exec_checkpoints WHERE zone_id=?
            """, (zone_id,))
            row = cur.fetchone()
            if not row:
                return None
            return ExecCheckpoint(
                zone_id=row[0],
                plan_id=row[1],
                zone_version=int(row[2]),
                exec_index=int(row[3]),
                block_id=int(row[4]),
                path_index=int(row[5]),
                path_s=float(row[6]),
                state=str(row[7]),
                water_off_latched=bool(int(row[8])),
                last_x=float(row[9] if row[9] is not None else 0.0),
                last_y=float(row[10] if row[10] is not None else 0.0),
                last_yaw=float(row[11] if row[11] is not None else 0.0),
                updated_ts=float(row[12]),
            )
        finally:
            conn.close()

    def upsert(self, cp: ExecCheckpoint):
        cp.updated_ts = time.time()
        conn = self._connect()
        try:
            conn.execute("""
              INSERT INTO exec_checkpoints(
                zone_id, plan_id, zone_version, exec_index, block_id, path_index, path_s,
                state, water_off_latched, last_x, last_y, last_yaw, updated_ts
              )
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
              ON CONFLICT(zone_id) DO UPDATE SET
                plan_id=excluded.plan_id,
                zone_version=excluded.zone_version,
                exec_index=excluded.exec_index,
                block_id=excluded.block_id,
                path_index=excluded.path_index,
                path_s=excluded.path_s,
                state=excluded.state,
                water_off_latched=excluded.water_off_latched,
                last_x=excluded.last_x,
                last_y=excluded.last_y,
                last_yaw=excluded.last_yaw,
                updated_ts=excluded.updated_ts
            """, (
                cp.zone_id, cp.plan_id, int(cp.zone_version),
                int(cp.exec_index), int(cp.block_id), int(cp.path_index), float(cp.path_s),
                str(cp.state), 1 if cp.water_off_latched else 0,
                float(cp.last_x), float(cp.last_y), float(cp.last_yaw),
                float(cp.updated_ts)
            ))
            conn.commit()
        finally:
            conn.close()

    def set_state(self, zone_id: str, state: str):
        conn = self._connect()
        try:
            conn.execute("""
              UPDATE exec_checkpoints SET state=?, updated_ts=?
              WHERE zone_id=?
            """, (state, time.time(), zone_id))
            conn.commit()
        finally:
            conn.close()

    def clear(self, zone_id: str):
        conn = self._connect()
        try:
            conn.execute("DELETE FROM exec_checkpoints WHERE zone_id=?", (zone_id,))
            conn.commit()
        finally:
            conn.close()

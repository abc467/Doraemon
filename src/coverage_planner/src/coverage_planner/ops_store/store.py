# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import os
import sqlite3
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional


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


def _bool_int(v: Any) -> int:
    return 1 if bool(v) else 0


def _parse_dow_mask(text: str) -> List[int]:
    vals: List[int] = []
    for tok in str(text or "").split(","):
        tok = tok.strip()
        if not tok:
            continue
        try:
            d = int(tok, 10)
        except Exception:
            continue
        if 0 <= d <= 6 and d not in vals:
            vals.append(d)
    return vals


def _format_dow_mask(dows: Iterable[int]) -> str:
    vals: List[str] = []
    seen = set()
    for d in dows or []:
        try:
            di = int(d)
        except Exception:
            continue
        if di < 0 or di > 6 or di in seen:
            continue
        seen.add(di)
        vals.append(str(di))
    return ",".join(vals)


@dataclass
class SysProfileRecord:
    sys_profile_name: str
    mbf_controller_name: str = ""
    actuator_profile_name: str = ""
    default_clean_mode: str = ""
    enabled: bool = True
    updated_ts: float = 0.0


@dataclass
class ActuatorProfileRecord:
    actuator_profile_name: str
    water_pump_pwm: int = 0
    suction_machine_pwm: int = 0
    vacuum_motor_pwm: int = 0
    height_scrub: int = 38
    height_scrub_active: bool = False
    updated_ts: float = 0.0


@dataclass
class JobRecord:
    job_id: str
    job_name: str = ""
    map_name: str = ""
    map_version: str = ""
    zone_id: str = ""
    plan_profile_name: str = ""
    sys_profile_name: str = ""
    default_clean_mode: str = ""
    default_loops: int = 1
    enabled: bool = True
    priority: int = 0
    created_ts: float = 0.0
    updated_ts: float = 0.0


@dataclass
class ScheduleStateRecord:
    schedule_id: str
    last_fire_ts: float = 0.0
    last_done_ts: float = 0.0
    last_status: str = ""
    updated_ts: float = 0.0


@dataclass
class MissionRunRecord:
    run_id: str
    job_id: str = ""
    map_name: str = ""
    map_version: str = ""
    zone_id: str = ""
    plan_profile_name: str = ""
    plan_id: str = ""
    zone_version: int = 0
    constraint_version: str = ""
    sys_profile_name: str = ""
    mbf_controller_name: str = ""
    actuator_profile_name: str = ""
    clean_mode: str = ""
    loops_total: int = 1
    loop_index: int = 1
    trigger_source: str = ""
    state: str = ""
    reason: str = ""
    map_id: str = ""
    map_md5: str = ""
    created_ts: float = 0.0
    start_ts: float = 0.0
    end_ts: float = 0.0
    updated_ts: float = 0.0


@dataclass
class MissionCheckpointRecord:
    run_id: str
    zone_id: str = ""
    plan_id: str = ""
    zone_version: int = 0
    exec_index: int = 0
    block_id: int = -1
    path_index: int = 0
    path_s: float = 0.0
    state: str = ""
    water_off_latched: bool = False
    map_id: str = ""
    map_md5: str = ""
    updated_ts: float = 0.0


@dataclass
class RobotRuntimeStateRecord:
    robot_id: str = "local_robot"
    active_run_id: str = ""
    active_job_id: str = ""
    active_schedule_id: str = ""
    map_name: str = ""
    map_version: str = ""
    localization_state: Optional[str] = None
    localization_valid: Optional[bool] = None
    mission_state: str = "IDLE"
    phase: str = "IDLE"
    public_state: str = "IDLE"
    armed: bool = True
    dock_state: str = ""
    battery_soc: float = 0.0
    battery_valid: bool = False
    executor_state: str = ""
    last_error_code: str = ""
    last_error_msg: str = ""
    updated_ts: float = 0.0


class OperationsStore:
    def __init__(self, db_path: str):
        self.db_path = os.path.expanduser(str(db_path))
        self.ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        d = os.path.dirname(self.db_path)
        if d:
            os.makedirs(d, exist_ok=True)
        conn = sqlite3.connect(self.db_path, timeout=30.0, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        return conn

    def _table_columns(self, conn: sqlite3.Connection, table_name: str) -> List[str]:
        rows = conn.execute("PRAGMA table_info(%s);" % str(table_name)).fetchall()
        return [str(r["name"]) for r in rows or [] if r and r["name"]]

    def _ensure_column(self, conn: sqlite3.Connection, table_name: str, column_name: str, column_sql: str):
        if str(column_name or "") in self._table_columns(conn, table_name):
            return
        conn.execute("ALTER TABLE %s ADD COLUMN %s %s;" % (str(table_name), str(column_name), str(column_sql)))

    def ensure_schema(self):
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS sys_profiles(
                  sys_profile_name TEXT PRIMARY KEY,
                  mbf_controller_name TEXT,
                  actuator_profile_name TEXT,
                  default_clean_mode TEXT,
                  enabled INTEGER NOT NULL DEFAULT 1,
                  updated_ts REAL NOT NULL
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS actuator_profiles(
                  actuator_profile_name TEXT PRIMARY KEY,
                  water_pump_pwm INTEGER NOT NULL DEFAULT 0,
                  suction_machine_pwm INTEGER NOT NULL DEFAULT 0,
                  vacuum_motor_pwm INTEGER NOT NULL DEFAULT 0,
                  height_scrub INTEGER NOT NULL DEFAULT 38,
                  height_scrub_active INTEGER NOT NULL DEFAULT 0,
                  updated_ts REAL NOT NULL
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS jobs(
                  job_id TEXT PRIMARY KEY,
                  job_name TEXT,
                  map_name TEXT,
                  map_version TEXT,
                  zone_id TEXT,
                  plan_profile_name TEXT,
                  sys_profile_name TEXT,
                  default_clean_mode TEXT,
                  default_loops INTEGER NOT NULL DEFAULT 1,
                  enabled INTEGER NOT NULL DEFAULT 1,
                  priority INTEGER NOT NULL DEFAULT 0,
                  created_ts REAL NOT NULL,
                  updated_ts REAL NOT NULL
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS job_schedules(
                  schedule_id TEXT PRIMARY KEY,
                  job_id TEXT NOT NULL,
                  enabled INTEGER NOT NULL DEFAULT 1,
                  schedule_type TEXT NOT NULL,
                  dow_mask TEXT,
                  time_local TEXT,
                  timezone TEXT,
                  start_date TEXT,
                  end_date TEXT,
                  updated_ts REAL NOT NULL
                );
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_job_schedules_job ON job_schedules(job_id);")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS job_schedule_state(
                  schedule_id TEXT PRIMARY KEY,
                  last_fire_ts REAL,
                  last_done_ts REAL,
                  last_status TEXT,
                  updated_ts REAL NOT NULL
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS mission_runs(
                  run_id TEXT PRIMARY KEY,
                  job_id TEXT,
                  map_name TEXT,
                  map_version TEXT,
                  zone_id TEXT,
                  plan_profile_name TEXT,
                  plan_id TEXT,
                  zone_version INTEGER NOT NULL DEFAULT 0,
                  constraint_version TEXT,
                  sys_profile_name TEXT,
                  mbf_controller_name TEXT,
                  actuator_profile_name TEXT,
                  clean_mode TEXT,
                  loops_total INTEGER NOT NULL DEFAULT 1,
                  loop_index INTEGER NOT NULL DEFAULT 1,
                  trigger_source TEXT,
                  state TEXT,
                  reason TEXT,
                  map_id TEXT,
                  map_md5 TEXT,
                  created_ts REAL NOT NULL,
                  start_ts REAL NOT NULL,
                  end_ts REAL NOT NULL DEFAULT 0.0,
                  updated_ts REAL NOT NULL
                );
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_mission_runs_zone_ts ON mission_runs(zone_id, updated_ts);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_mission_runs_job_ts ON mission_runs(job_id, updated_ts);")
            self._ensure_column(conn, "mission_runs", "constraint_version", "TEXT")
            self._ensure_column(conn, "jobs", "map_name", "TEXT")
            self._ensure_column(conn, "jobs", "map_version", "TEXT")
            self._ensure_column(conn, "mission_runs", "map_name", "TEXT")
            self._ensure_column(conn, "mission_runs", "map_version", "TEXT")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS mission_checkpoints(
                  run_id TEXT PRIMARY KEY,
                  zone_id TEXT,
                  plan_id TEXT,
                  zone_version INTEGER NOT NULL DEFAULT 0,
                  exec_index INTEGER NOT NULL DEFAULT 0,
                  block_id INTEGER NOT NULL DEFAULT -1,
                  path_index INTEGER NOT NULL DEFAULT 0,
                  path_s REAL NOT NULL DEFAULT 0.0,
                  state TEXT,
                  water_off_latched INTEGER NOT NULL DEFAULT 0,
                  map_id TEXT,
                  map_md5 TEXT,
                  updated_ts REAL NOT NULL
                );
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_mission_ckpt_zone_ts ON mission_checkpoints(zone_id, updated_ts);")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS robot_runtime_state(
                  robot_id TEXT PRIMARY KEY,
                  active_run_id TEXT,
                  active_job_id TEXT,
                  active_schedule_id TEXT,
                  map_name TEXT,
                  map_version TEXT,
                  localization_state TEXT,
                  localization_valid INTEGER NOT NULL DEFAULT 0,
                  mission_state TEXT,
                  phase TEXT,
                  public_state TEXT,
                  armed INTEGER NOT NULL DEFAULT 1,
                  dock_state TEXT,
                  battery_soc REAL NOT NULL DEFAULT 0.0,
                  battery_valid INTEGER NOT NULL DEFAULT 0,
                  executor_state TEXT,
                  last_error_code TEXT,
                  last_error_msg TEXT,
                  updated_ts REAL NOT NULL
                );
                """
            )
            self._ensure_column(conn, "robot_runtime_state", "active_schedule_id", "TEXT")
            self._ensure_column(conn, "robot_runtime_state", "map_name", "TEXT")
            self._ensure_column(conn, "robot_runtime_state", "map_version", "TEXT")
            self._ensure_column(conn, "robot_runtime_state", "localization_state", "TEXT")
            self._ensure_column(conn, "robot_runtime_state", "localization_valid", "INTEGER NOT NULL DEFAULT 0")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS robot_events(
                  event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                  ts REAL NOT NULL,
                  scope TEXT,
                  component TEXT,
                  level TEXT,
                  code TEXT,
                  message TEXT,
                  run_id TEXT,
                  job_id TEXT,
                  zone_id TEXT,
                  data_json TEXT
                );
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_robot_events_run_ts ON robot_events(run_id, ts);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_robot_events_zone_ts ON robot_events(zone_id, ts);")
            conn.commit()
        finally:
            conn.close()

    # ---------- profile config ----------
    def upsert_sys_profile(
        self,
        *,
        sys_profile_name: str,
        mbf_controller_name: str,
        actuator_profile_name: str,
        default_clean_mode: str,
        enabled: bool = True,
    ):
        now = _now_ts()
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO sys_profiles(sys_profile_name, mbf_controller_name, actuator_profile_name, default_clean_mode, enabled, updated_ts)
                VALUES(?,?,?,?,?,?)
                ON CONFLICT(sys_profile_name) DO UPDATE SET
                  mbf_controller_name=excluded.mbf_controller_name,
                  actuator_profile_name=excluded.actuator_profile_name,
                  default_clean_mode=excluded.default_clean_mode,
                  enabled=excluded.enabled,
                  updated_ts=excluded.updated_ts;
                """,
                (
                    str(sys_profile_name or "").strip(),
                    str(mbf_controller_name or "").strip(),
                    str(actuator_profile_name or "").strip(),
                    str(default_clean_mode or "").strip(),
                    _bool_int(enabled),
                    now,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def get_sys_profile(self, sys_profile_name: str) -> Optional[SysProfileRecord]:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM sys_profiles WHERE sys_profile_name=?;",
                (str(sys_profile_name or "").strip(),),
            ).fetchone()
            if not row:
                return None
            return SysProfileRecord(
                sys_profile_name=str(row["sys_profile_name"] or ""),
                mbf_controller_name=str(row["mbf_controller_name"] or ""),
                actuator_profile_name=str(row["actuator_profile_name"] or ""),
                default_clean_mode=str(row["default_clean_mode"] or ""),
                enabled=bool(int(row["enabled"] or 0)),
                updated_ts=float(row["updated_ts"] or 0.0),
            )
        finally:
            conn.close()

    def list_sys_profiles(self) -> List[SysProfileRecord]:
        conn = self._connect()
        try:
            rows = conn.execute("SELECT * FROM sys_profiles ORDER BY sys_profile_name ASC;").fetchall()
            return [
                SysProfileRecord(
                    sys_profile_name=str(r["sys_profile_name"] or ""),
                    mbf_controller_name=str(r["mbf_controller_name"] or ""),
                    actuator_profile_name=str(r["actuator_profile_name"] or ""),
                    default_clean_mode=str(r["default_clean_mode"] or ""),
                    enabled=bool(int(r["enabled"] or 0)),
                    updated_ts=float(r["updated_ts"] or 0.0),
                )
                for r in rows
            ]
        finally:
            conn.close()

    def export_mode_profiles_dict(self) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        for rec in self.list_sys_profiles():
            if not rec.enabled:
                continue
            out[rec.sys_profile_name] = {
                "mbf_controller_name": str(rec.mbf_controller_name or ""),
                "actuator_profile_name": str(rec.actuator_profile_name or rec.sys_profile_name or ""),
                "default_clean_mode": str(rec.default_clean_mode or ""),
            }
        return out

    def upsert_actuator_profile(
        self,
        *,
        actuator_profile_name: str,
        water_pump_pwm: int,
        suction_machine_pwm: int,
        vacuum_motor_pwm: int,
        height_scrub: int,
        height_scrub_active: bool = False,
    ):
        now = _now_ts()
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO actuator_profiles(
                  actuator_profile_name, water_pump_pwm, suction_machine_pwm, vacuum_motor_pwm, height_scrub, height_scrub_active, updated_ts
                ) VALUES(?,?,?,?,?,?,?)
                ON CONFLICT(actuator_profile_name) DO UPDATE SET
                  water_pump_pwm=excluded.water_pump_pwm,
                  suction_machine_pwm=excluded.suction_machine_pwm,
                  vacuum_motor_pwm=excluded.vacuum_motor_pwm,
                  height_scrub=excluded.height_scrub,
                  height_scrub_active=excluded.height_scrub_active,
                  updated_ts=excluded.updated_ts;
                """,
                (
                    str(actuator_profile_name or "").strip(),
                    int(water_pump_pwm),
                    int(suction_machine_pwm),
                    int(vacuum_motor_pwm),
                    int(height_scrub),
                    _bool_int(height_scrub_active),
                    now,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def get_actuator_profile(self, actuator_profile_name: str) -> Optional[ActuatorProfileRecord]:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM actuator_profiles WHERE actuator_profile_name=?;",
                (str(actuator_profile_name or "").strip(),),
            ).fetchone()
            if not row:
                return None
            return ActuatorProfileRecord(
                actuator_profile_name=str(row["actuator_profile_name"] or ""),
                water_pump_pwm=int(row["water_pump_pwm"] or 0),
                suction_machine_pwm=int(row["suction_machine_pwm"] or 0),
                vacuum_motor_pwm=int(row["vacuum_motor_pwm"] or 0),
                height_scrub=int(row["height_scrub"] or 38),
                height_scrub_active=bool(int(row["height_scrub_active"] or 0)),
                updated_ts=float(row["updated_ts"] or 0.0),
            )
        finally:
            conn.close()

    # ---------- jobs / schedules ----------
    def upsert_job(
        self,
        *,
        job_id: str,
        job_name: str,
        map_name: str = "",
        map_version: str = "",
        zone_id: str,
        plan_profile_name: str,
        sys_profile_name: str,
        default_clean_mode: str,
        default_loops: int = 1,
        enabled: bool = True,
        priority: int = 0,
    ):
        now = _now_ts()
        created_ts = now
        conn = self._connect()
        try:
            row = conn.execute("SELECT created_ts FROM jobs WHERE job_id=?;", (str(job_id),)).fetchone()
            if row and row["created_ts"] is not None:
                created_ts = float(row["created_ts"])
            conn.execute(
                """
                INSERT INTO jobs(
                  job_id, job_name, map_name, map_version, zone_id, plan_profile_name, sys_profile_name, default_clean_mode,
                  default_loops, enabled, priority, created_ts, updated_ts
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(job_id) DO UPDATE SET
                  job_name=excluded.job_name,
                  map_name=excluded.map_name,
                  map_version=excluded.map_version,
                  zone_id=excluded.zone_id,
                  plan_profile_name=excluded.plan_profile_name,
                  sys_profile_name=excluded.sys_profile_name,
                  default_clean_mode=excluded.default_clean_mode,
                  default_loops=excluded.default_loops,
                  enabled=excluded.enabled,
                  priority=excluded.priority,
                  updated_ts=excluded.updated_ts;
                """,
                (
                    str(job_id or "").strip(),
                    str(job_name or "").strip(),
                    str(map_name or "").strip(),
                    str(map_version or "").strip(),
                    str(zone_id or "").strip(),
                    str(plan_profile_name or "").strip(),
                    str(sys_profile_name or "").strip(),
                    str(default_clean_mode or "").strip(),
                    max(1, int(default_loops or 1)),
                    _bool_int(enabled),
                    int(priority or 0),
                    created_ts,
                    now,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def upsert_job_schedule(
        self,
        *,
        schedule_id: str,
        job_id: str,
        enabled: bool,
        schedule_type: str,
        dow_mask: str = "",
        time_local: str = "",
        timezone: str = "Asia/Shanghai",
        start_date: str = "",
        end_date: str = "",
    ):
        now = _now_ts()
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO job_schedules(
                  schedule_id, job_id, enabled, schedule_type, dow_mask, time_local, timezone, start_date, end_date, updated_ts
                ) VALUES(?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(schedule_id) DO UPDATE SET
                  job_id=excluded.job_id,
                  enabled=excluded.enabled,
                  schedule_type=excluded.schedule_type,
                  dow_mask=excluded.dow_mask,
                  time_local=excluded.time_local,
                  timezone=excluded.timezone,
                  start_date=excluded.start_date,
                  end_date=excluded.end_date,
                  updated_ts=excluded.updated_ts;
                """,
                (
                    str(schedule_id or "").strip(),
                    str(job_id or "").strip(),
                    _bool_int(enabled),
                    str(schedule_type or "").strip().lower(),
                    str(dow_mask or "").strip(),
                    str(time_local or "").strip(),
                    str(timezone or "Asia/Shanghai").strip(),
                    str(start_date or "").strip(),
                    str(end_date or "").strip(),
                    now,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def list_schedule_specs(self) -> List[Dict[str, Any]]:
        conn = self._connect()
        try:
            rows = conn.execute(
                """
                SELECT
                  s.schedule_id, s.job_id, s.enabled AS schedule_enabled, s.schedule_type, s.dow_mask,
                  s.time_local, s.timezone, s.start_date, s.end_date,
                  j.job_name, j.map_name, j.map_version, j.zone_id, j.plan_profile_name, j.sys_profile_name,
                  j.default_clean_mode, j.default_loops, j.enabled AS job_enabled
                FROM job_schedules s
                JOIN jobs j ON j.job_id=s.job_id
                WHERE s.enabled=1 AND j.enabled=1
                ORDER BY j.priority DESC, s.schedule_id ASC;
                """
            ).fetchall()
            out: List[Dict[str, Any]] = []
            for row in rows:
                schedule_type = str(row["schedule_type"] or "").strip().lower()
                item: Dict[str, Any] = {
                    "id": str(row["schedule_id"] or ""),
                    "job_id": str(row["job_id"] or ""),
                    "job_name": str(row["job_name"] or ""),
                    "enabled": bool(int(row["schedule_enabled"] or 0)),
                    "type": schedule_type,
                    "task": {
                        "map_name": str(row["map_name"] or ""),
                        "map_version": str(row["map_version"] or ""),
                        "zone_id": str(row["zone_id"] or ""),
                        "loops": int(row["default_loops"] or 1),
                        "plan_profile_name": str(row["plan_profile_name"] or ""),
                        "sys_profile_name": str(row["sys_profile_name"] or ""),
                        "clean_mode": str(row["default_clean_mode"] or ""),
                    },
                }
                if schedule_type == "weekly":
                    item["dow"] = _parse_dow_mask(str(row["dow_mask"] or ""))
                    item["time"] = str(row["time_local"] or "")
                elif schedule_type == "daily":
                    item["time"] = str(row["time_local"] or "")
                elif schedule_type == "once":
                    date_s = str(row["start_date"] or "").strip()
                    time_s = str(row["time_local"] or "").strip()
                    item["at"] = f"{date_s} {time_s}".strip()
                    item["oneshot"] = True
                out.append(item)
            return out
        finally:
            conn.close()

    def get_job(self, job_id: str) -> Optional[JobRecord]:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM jobs WHERE job_id=?;",
                (str(job_id or "").strip(),),
            ).fetchone()
            if not row:
                return None
            return JobRecord(
                job_id=str(row["job_id"] or ""),
                job_name=str(row["job_name"] or ""),
                map_name=str(row["map_name"] or ""),
                map_version=str(row["map_version"] or ""),
                zone_id=str(row["zone_id"] or ""),
                plan_profile_name=str(row["plan_profile_name"] or ""),
                sys_profile_name=str(row["sys_profile_name"] or ""),
                default_clean_mode=str(row["default_clean_mode"] or ""),
                default_loops=int(row["default_loops"] or 1),
                enabled=bool(int(row["enabled"] or 0)),
                priority=int(row["priority"] or 0),
                created_ts=float(row["created_ts"] or 0.0),
                updated_ts=float(row["updated_ts"] or 0.0),
            )
        finally:
            conn.close()

    def list_jobs(self) -> List[JobRecord]:
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT * FROM jobs ORDER BY CAST(job_id AS INTEGER) ASC, job_id ASC;"
            ).fetchall()
            out: List[JobRecord] = []
            for row in rows or []:
                out.append(
                    JobRecord(
                        job_id=str(row["job_id"] or ""),
                        job_name=str(row["job_name"] or ""),
                        map_name=str(row["map_name"] or ""),
                        map_version=str(row["map_version"] or ""),
                        zone_id=str(row["zone_id"] or ""),
                        plan_profile_name=str(row["plan_profile_name"] or ""),
                        sys_profile_name=str(row["sys_profile_name"] or ""),
                        default_clean_mode=str(row["default_clean_mode"] or ""),
                        default_loops=int(row["default_loops"] or 1),
                        enabled=bool(int(row["enabled"] or 0)),
                        priority=int(row["priority"] or 0),
                        created_ts=float(row["created_ts"] or 0.0),
                        updated_ts=float(row["updated_ts"] or 0.0),
                    )
                )
            return out
        finally:
            conn.close()

    def delete_job(self, job_id: str):
        conn = self._connect()
        try:
            conn.execute(
                "DELETE FROM job_schedules WHERE job_id=?;",
                (str(job_id or "").strip(),),
            )
            conn.execute(
                "DELETE FROM jobs WHERE job_id=?;",
                (str(job_id or "").strip(),),
            )
            conn.commit()
        finally:
            conn.close()

    def get_schedule_state(self, schedule_id: str) -> Optional[ScheduleStateRecord]:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM job_schedule_state WHERE schedule_id=?;",
                (str(schedule_id or "").strip(),),
            ).fetchone()
            if not row:
                return None
            return ScheduleStateRecord(
                schedule_id=str(row["schedule_id"] or ""),
                last_fire_ts=float(row["last_fire_ts"] or 0.0),
                last_done_ts=float(row["last_done_ts"] or 0.0),
                last_status=str(row["last_status"] or ""),
                updated_ts=float(row["updated_ts"] or 0.0),
            )
        finally:
            conn.close()

    def was_schedule_fired(self, schedule_id: str, slot_ts: float) -> bool:
        st = self.get_schedule_state(schedule_id)
        if st is None:
            return False
        return abs(float(st.last_fire_ts or 0.0) - float(slot_ts or 0.0)) < 1.0

    def mark_schedule_fired(self, schedule_id: str, slot_ts: float, *, fire_ts: Optional[float] = None):
        now = _now_ts()
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO job_schedule_state(schedule_id, last_fire_ts, last_done_ts, last_status, updated_ts)
                VALUES(?,?,?,?,?)
                ON CONFLICT(schedule_id) DO UPDATE SET
                  last_fire_ts=excluded.last_fire_ts,
                  updated_ts=excluded.updated_ts;
                """,
                (
                    str(schedule_id or "").strip(),
                    float(slot_ts or 0.0),
                    0.0,
                    "",
                    float(fire_ts if fire_ts is not None else now),
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def mark_schedule_done(self, schedule_id: str, *, status: str, done_ts: Optional[float] = None):
        now = _now_ts()
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO job_schedule_state(schedule_id, last_fire_ts, last_done_ts, last_status, updated_ts)
                VALUES(?,?,?,?,?)
                ON CONFLICT(schedule_id) DO UPDATE SET
                  last_done_ts=excluded.last_done_ts,
                  last_status=excluded.last_status,
                  updated_ts=excluded.updated_ts;
                """,
                (
                    str(schedule_id or "").strip(),
                    0.0,
                    float(done_ts if done_ts is not None else now),
                    str(status or "").strip(),
                    now,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    # ---------- mission runs ----------
    def create_run(
        self,
        *,
        run_id: str,
        job_id: str = "",
        map_name: str = "",
        map_version: str = "",
        zone_id: str = "",
        plan_profile_name: str = "",
        plan_id: str = "",
        zone_version: int = 0,
        constraint_version: str = "",
        sys_profile_name: str = "",
        mbf_controller_name: str = "",
        actuator_profile_name: str = "",
        clean_mode: str = "",
        loops_total: int = 1,
        loop_index: int = 1,
        trigger_source: str = "",
        state: str = "RUNNING",
        reason: str = "",
        map_id: str = "",
        map_md5: str = "",
        created_ts: Optional[float] = None,
        start_ts: Optional[float] = None,
    ):
        now = _now_ts()
        cts = float(created_ts if created_ts is not None else now)
        sts = float(start_ts if start_ts is not None else cts)
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT OR REPLACE INTO mission_runs(
                  run_id, job_id, map_name, map_version, zone_id, plan_profile_name, plan_id, zone_version, constraint_version, sys_profile_name,
                  mbf_controller_name, actuator_profile_name, clean_mode, loops_total, loop_index,
                  trigger_source, state, reason, map_id, map_md5, created_ts, start_ts, end_ts, updated_ts
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
                """,
                (
                    str(run_id or "").strip(),
                    str(job_id or "").strip(),
                    str(map_name or "").strip(),
                    str(map_version or "").strip(),
                    str(zone_id or "").strip(),
                    str(plan_profile_name or "").strip(),
                    str(plan_id or "").strip(),
                    int(zone_version or 0),
                    str(constraint_version or "").strip(),
                    str(sys_profile_name or "").strip(),
                    str(mbf_controller_name or "").strip(),
                    str(actuator_profile_name or "").strip(),
                    str(clean_mode or "").strip(),
                    max(1, int(loops_total or 1)),
                    max(1, int(loop_index or 1)),
                    str(trigger_source or "").strip(),
                    str(state or "").strip(),
                    str(reason or "").strip(),
                    str(map_id or "").strip(),
                    str(map_md5 or "").strip(),
                    cts,
                    sts,
                    0.0,
                    now,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def ensure_run_exists(
        self,
        *,
        run_id: str,
        zone_id: str,
        map_name: str = "",
        map_version: str = "",
        plan_profile_name: str,
        sys_profile_name: str,
        clean_mode: str,
        trigger_source: str,
    ):
        if self.get_run(run_id) is not None:
            return
        self.create_run(
            run_id=str(run_id or "").strip(),
            job_id="",
            map_name=str(map_name or "").strip(),
            map_version=str(map_version or "").strip(),
            zone_id=str(zone_id or "").strip(),
            plan_profile_name=str(plan_profile_name or "").strip(),
            sys_profile_name=str(sys_profile_name or "").strip(),
            clean_mode=str(clean_mode or "").strip(),
            loops_total=1,
            loop_index=1,
            trigger_source=str(trigger_source or "").strip(),
            state="RUNNING",
        )

    def update_run_state(self, run_id: str, state: str, *, reason: str = "", set_end: bool = False):
        now = _now_ts()
        conn = self._connect()
        try:
            if set_end:
                conn.execute(
                    "UPDATE mission_runs SET state=?, reason=?, end_ts=?, updated_ts=? WHERE run_id=?;",
                    (str(state or "").strip(), str(reason or "").strip(), now, now, str(run_id or "").strip()),
                )
            else:
                conn.execute(
                    "UPDATE mission_runs SET state=?, reason=?, updated_ts=? WHERE run_id=?;",
                    (str(state or "").strip(), str(reason or "").strip(), now, str(run_id or "").strip()),
                )
            conn.commit()
        finally:
            conn.close()

    def update_run_execution_context(
        self,
        run_id: str,
        *,
        map_name: Optional[str] = None,
        map_version: Optional[str] = None,
        zone_id: Optional[str] = None,
        plan_profile_name: Optional[str] = None,
        plan_id: Optional[str] = None,
        zone_version: Optional[int] = None,
        constraint_version: Optional[str] = None,
        sys_profile_name: Optional[str] = None,
        mbf_controller_name: Optional[str] = None,
        actuator_profile_name: Optional[str] = None,
        clean_mode: Optional[str] = None,
        map_id: Optional[str] = None,
        map_md5: Optional[str] = None,
    ):
        run = self.get_run(run_id)
        if run is None:
            return
        conn = self._connect()
        now = _now_ts()
        try:
            conn.execute(
                """
                UPDATE mission_runs
                SET map_name=?, map_version=?, zone_id=?, plan_profile_name=?, plan_id=?, zone_version=?, constraint_version=?, sys_profile_name=?,
                    mbf_controller_name=?, actuator_profile_name=?, clean_mode=?,
                    map_id=?, map_md5=?, updated_ts=?
                WHERE run_id=?;
                """,
                (
                    str(map_name if map_name is not None else run.map_name),
                    str(map_version if map_version is not None else run.map_version),
                    str(zone_id if zone_id is not None else run.zone_id),
                    str(plan_profile_name if plan_profile_name is not None else run.plan_profile_name),
                    str(plan_id if plan_id is not None else run.plan_id),
                    int(zone_version if zone_version is not None else run.zone_version),
                    str(constraint_version if constraint_version is not None else run.constraint_version),
                    str(sys_profile_name if sys_profile_name is not None else run.sys_profile_name),
                    str(mbf_controller_name if mbf_controller_name is not None else run.mbf_controller_name),
                    str(actuator_profile_name if actuator_profile_name is not None else run.actuator_profile_name),
                    str(clean_mode if clean_mode is not None else run.clean_mode),
                    str(map_id if map_id is not None else run.map_id),
                    str(map_md5 if map_md5 is not None else run.map_md5),
                    now,
                    str(run_id or "").strip(),
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def finish_run(self, run_id: str, state: str, *, reason: str = ""):
        self.update_run_state(run_id, state, reason=reason, set_end=True)

    def get_run(self, run_id: str) -> Optional[MissionRunRecord]:
        conn = self._connect()
        try:
            row = conn.execute("SELECT * FROM mission_runs WHERE run_id=?;", (str(run_id or "").strip(),)).fetchone()
            if not row:
                return None
            return MissionRunRecord(
                run_id=str(row["run_id"] or ""),
                job_id=str(row["job_id"] or ""),
                map_name=str(row["map_name"] or ""),
                map_version=str(row["map_version"] or ""),
                zone_id=str(row["zone_id"] or ""),
                plan_profile_name=str(row["plan_profile_name"] or ""),
                plan_id=str(row["plan_id"] or ""),
                zone_version=int(row["zone_version"] or 0),
                constraint_version=str(row["constraint_version"] or ""),
                sys_profile_name=str(row["sys_profile_name"] or ""),
                mbf_controller_name=str(row["mbf_controller_name"] or ""),
                actuator_profile_name=str(row["actuator_profile_name"] or ""),
                clean_mode=str(row["clean_mode"] or ""),
                loops_total=int(row["loops_total"] or 1),
                loop_index=int(row["loop_index"] or 1),
                trigger_source=str(row["trigger_source"] or ""),
                state=str(row["state"] or ""),
                reason=str(row["reason"] or ""),
                map_id=str(row["map_id"] or ""),
                map_md5=str(row["map_md5"] or ""),
                created_ts=float(row["created_ts"] or 0.0),
                start_ts=float(row["start_ts"] or 0.0),
                end_ts=float(row["end_ts"] or 0.0),
                updated_ts=float(row["updated_ts"] or 0.0),
            )
        finally:
            conn.close()

    def list_recent_runs(self, *, limit: int = 20) -> List[MissionRunRecord]:
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT * FROM mission_runs ORDER BY updated_ts DESC LIMIT ?;",
                (max(1, int(limit)),),
            ).fetchall()
            out: List[MissionRunRecord] = []
            for row in rows:
                out.append(
                    MissionRunRecord(
                        run_id=str(row["run_id"] or ""),
                        job_id=str(row["job_id"] or ""),
                        map_name=str(row["map_name"] or ""),
                        map_version=str(row["map_version"] or ""),
                        zone_id=str(row["zone_id"] or ""),
                        plan_profile_name=str(row["plan_profile_name"] or ""),
                        plan_id=str(row["plan_id"] or ""),
                        zone_version=int(row["zone_version"] or 0),
                        constraint_version=str(row["constraint_version"] or ""),
                        sys_profile_name=str(row["sys_profile_name"] or ""),
                        mbf_controller_name=str(row["mbf_controller_name"] or ""),
                        actuator_profile_name=str(row["actuator_profile_name"] or ""),
                        clean_mode=str(row["clean_mode"] or ""),
                        loops_total=int(row["loops_total"] or 1),
                        loop_index=int(row["loop_index"] or 1),
                        trigger_source=str(row["trigger_source"] or ""),
                        state=str(row["state"] or ""),
                        reason=str(row["reason"] or ""),
                        map_id=str(row["map_id"] or ""),
                        map_md5=str(row["map_md5"] or ""),
                        created_ts=float(row["created_ts"] or 0.0),
                        start_ts=float(row["start_ts"] or 0.0),
                        end_ts=float(row["end_ts"] or 0.0),
                        updated_ts=float(row["updated_ts"] or 0.0),
                    )
                )
            return out
        finally:
            conn.close()

    def get_latest_run_for_job(self, job_id: str) -> Optional[MissionRunRecord]:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM mission_runs WHERE job_id=? ORDER BY updated_ts DESC LIMIT 1;",
                (str(job_id or "").strip(),),
            ).fetchone()
            if not row:
                return None
            return MissionRunRecord(
                run_id=str(row["run_id"] or ""),
                job_id=str(row["job_id"] or ""),
                map_name=str(row["map_name"] or ""),
                map_version=str(row["map_version"] or ""),
                zone_id=str(row["zone_id"] or ""),
                plan_profile_name=str(row["plan_profile_name"] or ""),
                plan_id=str(row["plan_id"] or ""),
                zone_version=int(row["zone_version"] or 0),
                constraint_version=str(row["constraint_version"] or ""),
                sys_profile_name=str(row["sys_profile_name"] or ""),
                mbf_controller_name=str(row["mbf_controller_name"] or ""),
                actuator_profile_name=str(row["actuator_profile_name"] or ""),
                clean_mode=str(row["clean_mode"] or ""),
                loops_total=int(row["loops_total"] or 1),
                loop_index=int(row["loop_index"] or 1),
                trigger_source=str(row["trigger_source"] or ""),
                state=str(row["state"] or ""),
                reason=str(row["reason"] or ""),
                map_id=str(row["map_id"] or ""),
                map_md5=str(row["map_md5"] or ""),
                created_ts=float(row["created_ts"] or 0.0),
                start_ts=float(row["start_ts"] or 0.0),
                end_ts=float(row["end_ts"] or 0.0),
                updated_ts=float(row["updated_ts"] or 0.0),
            )
        finally:
            conn.close()

    def find_latest_open_run(self, zone_id: str) -> Optional[str]:
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT run_id, state FROM mission_runs WHERE zone_id=? ORDER BY updated_ts DESC LIMIT 10;",
                (str(zone_id or "").strip(),),
            ).fetchall()
            for row in rows or []:
                state = str(row["state"] or "").upper()
                if state in ("DONE", "FAILED", "CANCELED", "ESTOP") or state.startswith("ERROR"):
                    continue
                return str(row["run_id"] or "")
            return None
        finally:
            conn.close()

    # ---------- checkpoints ----------
    def upsert_mission_checkpoint(self, cp: MissionCheckpointRecord):
        now = _now_ts()
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO mission_checkpoints(
                  run_id, zone_id, plan_id, zone_version, exec_index, block_id, path_index, path_s,
                  state, water_off_latched, map_id, map_md5, updated_ts
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(run_id) DO UPDATE SET
                  zone_id=excluded.zone_id,
                  plan_id=excluded.plan_id,
                  zone_version=excluded.zone_version,
                  exec_index=excluded.exec_index,
                  block_id=excluded.block_id,
                  path_index=excluded.path_index,
                  path_s=excluded.path_s,
                  state=excluded.state,
                  water_off_latched=excluded.water_off_latched,
                  map_id=excluded.map_id,
                  map_md5=excluded.map_md5,
                  updated_ts=excluded.updated_ts;
                """,
                (
                    str(cp.run_id or "").strip(),
                    str(cp.zone_id or "").strip(),
                    str(cp.plan_id or "").strip(),
                    int(cp.zone_version or 0),
                    int(cp.exec_index or 0),
                    int(cp.block_id if cp.block_id is not None else -1),
                    int(cp.path_index or 0),
                    float(cp.path_s or 0.0),
                    str(cp.state or "").strip(),
                    _bool_int(cp.water_off_latched),
                    str(cp.map_id or "").strip(),
                    str(cp.map_md5 or "").strip(),
                    now,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def get_mission_checkpoint(self, run_id: str) -> Optional[MissionCheckpointRecord]:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM mission_checkpoints WHERE run_id=?;",
                (str(run_id or "").strip(),),
            ).fetchone()
            if not row:
                return None
            return MissionCheckpointRecord(
                run_id=str(row["run_id"] or ""),
                zone_id=str(row["zone_id"] or ""),
                plan_id=str(row["plan_id"] or ""),
                zone_version=int(row["zone_version"] or 0),
                exec_index=int(row["exec_index"] or 0),
                block_id=int(row["block_id"] if row["block_id"] is not None else -1),
                path_index=int(row["path_index"] or 0),
                path_s=float(row["path_s"] or 0.0),
                state=str(row["state"] or ""),
                water_off_latched=bool(int(row["water_off_latched"] or 0)),
                map_id=str(row["map_id"] or ""),
                map_md5=str(row["map_md5"] or ""),
                updated_ts=float(row["updated_ts"] or 0.0),
            )
        finally:
            conn.close()

    def get_latest_checkpoint_run_id(self, zone_id: str) -> str:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT run_id FROM mission_checkpoints WHERE zone_id=? ORDER BY updated_ts DESC LIMIT 1;",
                (str(zone_id or "").strip(),),
            ).fetchone()
            return str(row["run_id"] or "") if row else ""
        finally:
            conn.close()

    def delete_mission_checkpoint(self, run_id: str):
        conn = self._connect()
        try:
            conn.execute("DELETE FROM mission_checkpoints WHERE run_id=?;", (str(run_id or "").strip(),))
            conn.commit()
        finally:
            conn.close()

    # ---------- runtime state ----------
    def upsert_robot_runtime_state(self, state: RobotRuntimeStateRecord):
        ts = float(state.updated_ts or _now_ts())
        conn = self._connect()
        try:
            current = conn.execute(
                "SELECT localization_state, localization_valid FROM robot_runtime_state WHERE robot_id=?;",
                (str(state.robot_id or "local_robot"),),
            ).fetchone()
            localization_state = (
                str(state.localization_state)
                if state.localization_state is not None
                else str((current["localization_state"] if current else "") or "")
            )
            localization_valid = (
                bool(state.localization_valid)
                if state.localization_valid is not None
                else bool(int((current["localization_valid"] if current else 0) or 0))
            )
            conn.execute(
                """
                INSERT INTO robot_runtime_state(
                  robot_id, active_run_id, active_job_id, active_schedule_id, map_name, map_version, localization_state, localization_valid, mission_state, phase, public_state,
                  armed, dock_state, battery_soc, battery_valid, executor_state,
                  last_error_code, last_error_msg, updated_ts
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(robot_id) DO UPDATE SET
                  active_run_id=excluded.active_run_id,
                  active_job_id=excluded.active_job_id,
                  active_schedule_id=excluded.active_schedule_id,
                  map_name=excluded.map_name,
                  map_version=excluded.map_version,
                  localization_state=excluded.localization_state,
                  localization_valid=excluded.localization_valid,
                  mission_state=excluded.mission_state,
                  phase=excluded.phase,
                  public_state=excluded.public_state,
                  armed=excluded.armed,
                  dock_state=excluded.dock_state,
                  battery_soc=excluded.battery_soc,
                  battery_valid=excluded.battery_valid,
                  executor_state=excluded.executor_state,
                  last_error_code=excluded.last_error_code,
                  last_error_msg=excluded.last_error_msg,
                  updated_ts=excluded.updated_ts;
                """,
                (
                    str(state.robot_id or "local_robot"),
                    str(state.active_run_id or ""),
                    str(state.active_job_id or ""),
                    str(state.active_schedule_id or ""),
                    str(state.map_name or ""),
                    str(state.map_version or ""),
                    str(localization_state or ""),
                    _bool_int(localization_valid),
                    str(state.mission_state or "IDLE"),
                    str(state.phase or "IDLE"),
                    str(state.public_state or "IDLE"),
                    _bool_int(state.armed),
                    str(state.dock_state or ""),
                    float(state.battery_soc or 0.0),
                    _bool_int(state.battery_valid),
                    str(state.executor_state or ""),
                    str(state.last_error_code or ""),
                    str(state.last_error_msg or ""),
                    ts,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def get_robot_runtime_state(self, robot_id: str = "local_robot") -> Optional[RobotRuntimeStateRecord]:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM robot_runtime_state WHERE robot_id=?;",
                (str(robot_id or "local_robot"),),
            ).fetchone()
            if not row:
                return None
            return RobotRuntimeStateRecord(
                robot_id=str(row["robot_id"] or "local_robot"),
                active_run_id=str(row["active_run_id"] or ""),
                active_job_id=str(row["active_job_id"] or ""),
                active_schedule_id=str(row["active_schedule_id"] or ""),
                map_name=str(row["map_name"] or ""),
                map_version=str(row["map_version"] or ""),
                localization_state=str(row["localization_state"] or ""),
                localization_valid=bool(int(row["localization_valid"] or 0)),
                mission_state=str(row["mission_state"] or "IDLE"),
                phase=str(row["phase"] or "IDLE"),
                public_state=str(row["public_state"] or "IDLE"),
                armed=bool(int(row["armed"] or 0)),
                dock_state=str(row["dock_state"] or ""),
                battery_soc=float(row["battery_soc"] or 0.0),
                battery_valid=bool(int(row["battery_valid"] or 0)),
                executor_state=str(row["executor_state"] or ""),
                last_error_code=str(row["last_error_code"] or ""),
                last_error_msg=str(row["last_error_msg"] or ""),
                updated_ts=float(row["updated_ts"] or 0.0),
            )
        finally:
            conn.close()

    # ---------- events ----------
    def add_robot_event(
        self,
        *,
        scope: str,
        component: str,
        level: str,
        code: str,
        message: str,
        run_id: str = "",
        job_id: str = "",
        zone_id: str = "",
        data: Optional[Any] = None,
        ts: Optional[float] = None,
    ):
        event_ts = float(ts if ts is not None else _now_ts())
        rid = str(run_id or "").strip()
        jid = str(job_id or "").strip()
        zid = str(zone_id or "").strip()
        if rid and (not jid or not zid):
            run = self.get_run(rid)
            if run is not None:
                if not jid:
                    jid = str(run.job_id or "")
                if not zid:
                    zid = str(run.zone_id or "")

        data_json = ""
        if data is not None and data != "":
            if isinstance(data, str):
                data_json = str(data)
            else:
                try:
                    data_json = _json_dumps(data)
                except Exception:
                    data_json = ""

        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO robot_events(ts, scope, component, level, code, message, run_id, job_id, zone_id, data_json)
                VALUES(?,?,?,?,?,?,?,?,?,?);
                """,
                (
                    event_ts,
                    str(scope or "").strip(),
                    str(component or "").strip(),
                    str(level or "").strip(),
                    str(code or "").strip(),
                    str(message or "").strip(),
                    rid,
                    jid,
                    zid,
                    data_json,
                ),
            )
            conn.commit()
        finally:
            conn.close()


def _derive_controller_name(name: str, cfg: Dict[str, Any]) -> str:
    val = str(cfg.get("mbf_controller_name") or "").strip()
    if val:
        return val
    base = str(name or "standard").strip() or "standard"
    return f"MPPI_{base.capitalize()}_Controller"


def seed_sys_profiles_from_param(store: OperationsStore, raw_profiles: Any):
    if not isinstance(raw_profiles, dict):
        return
    for key, val in raw_profiles.items():
        if not isinstance(val, dict):
            val = {}
        name = str(key or "").strip()
        if not name:
            continue
        controller_name = _derive_controller_name(name, val)
        actuator_profile_name = str(
            val.get("actuator_profile_name")
            or val.get("executor_sys_profile")
            or val.get("sys_profile")
            or name
        ).strip()
        default_clean_mode = str(val.get("default_clean_mode") or val.get("clean_mode") or "scrub").strip() or "scrub"
        enabled = bool(val.get("enabled", True))
        store.upsert_sys_profile(
            sys_profile_name=name,
            mbf_controller_name=controller_name,
            actuator_profile_name=actuator_profile_name,
            default_clean_mode=default_clean_mode,
            enabled=enabled,
        )


def _clamp_u8(val: Any, default: int = 0) -> int:
    try:
        return max(0, min(255, int(val)))
    except Exception:
        return int(default)


def seed_actuator_profiles_from_param(store: OperationsStore, raw_profiles: Any):
    if not isinstance(raw_profiles, dict):
        return
    for key, val in raw_profiles.items():
        if not isinstance(val, dict):
            val = {}
        name = str(key or "").strip()
        if not name:
            continue
        water_pump_pwm = _clamp_u8(val.get("water_pump_pwm", val.get("vel_water_pump", 0)))
        suction_machine_pwm = _clamp_u8(val.get("suction_machine_pwm", val.get("vel_water_suction", 0)))
        vacuum_motor_pwm = _clamp_u8(val.get("vacuum_motor_pwm", val.get("vel_water_suction", suction_machine_pwm)))
        height_scrub = _clamp_u8(val.get("height_scrub", 38), default=38)
        height_scrub_active = bool(val.get("height_scrub_active", False))
        store.upsert_actuator_profile(
            actuator_profile_name=name,
            water_pump_pwm=water_pump_pwm,
            suction_machine_pwm=suction_machine_pwm,
            vacuum_motor_pwm=vacuum_motor_pwm,
            height_scrub=height_scrub,
            height_scrub_active=height_scrub_active,
        )


def seed_schedule_jobs_from_specs(store: OperationsStore, schedules: Any):
    if not isinstance(schedules, list):
        return
    for item in schedules:
        if not isinstance(item, dict):
            continue
        schedule_id = str(item.get("id", "") or "").strip()
        if not schedule_id:
            continue
        job_id = str(item.get("job_id", "") or schedule_id).strip()
        if not job_id:
            continue
        task = item.get("task", {}) or {}
        if not isinstance(task, dict):
            task = {}
        map_name = str(task.get("map_name") or "").strip()
        map_version = str(task.get("map_version") or "").strip()
        zone_id = str(task.get("zone_id") or task.get("zone") or "").strip()
        plan_profile_name = str(task.get("plan_profile_name") or task.get("plan_profile") or "").strip()
        sys_profile_name = str(task.get("sys_profile_name") or task.get("sys_profile") or "").strip()
        clean_mode = str(task.get("clean_mode") or task.get("mode") or "").strip()
        loops = max(1, int(task.get("loops", 1) or 1))
        enabled = bool(item.get("enabled", True))
        schedule_type = str(item.get("type", "") or "").strip().lower()
        time_local = ""
        start_date = ""
        dow_mask = ""
        if schedule_type in ("weekly", "daily"):
            time_local = str(item.get("time", "") or "").strip()
        elif schedule_type == "once":
            at = str(item.get("at", "") or "").strip().replace("/", "-")
            if " " in at:
                start_date, time_local = at.split(" ", 1)
            else:
                start_date = at
                time_local = "00:00"
        if schedule_type == "weekly":
            dow_mask = _format_dow_mask(item.get("dow", []) or [])

        store.upsert_job(
            job_id=job_id,
            job_name=str(item.get("job_name") or job_id),
            map_name=map_name,
            map_version=map_version,
            zone_id=zone_id,
            plan_profile_name=plan_profile_name,
            sys_profile_name=sys_profile_name,
            default_clean_mode=clean_mode,
            default_loops=loops,
            enabled=enabled,
            priority=int(item.get("priority", 0) or 0),
        )
        store.upsert_job_schedule(
            schedule_id=schedule_id,
            job_id=job_id,
            enabled=enabled,
            schedule_type=schedule_type,
            dow_mask=dow_mask,
            time_local=time_local,
            timezone=str(item.get("timezone", "Asia/Shanghai") or "Asia/Shanghai"),
            start_date=start_date,
            end_date=str(item.get("end_date", "") or "").strip(),
        )

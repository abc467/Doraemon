# -*- coding: utf-8 -*-

"""Fail-closed, read-only state checks for a never-mapped production vehicle."""

from __future__ import annotations

import json
import os
import sqlite3
import stat
from typing import Dict, Iterable, List, Mapping, Sequence, Tuple
from urllib.parse import quote


PLANNING_TABLES = {
    "map_active_alignment_revisions",
    "map_active_alignments",
    "map_active_constraint_revisions",
    "map_active_constraint_versions",
    "map_alignment_configs",
    "map_alignment_revision_configs",
    "map_assets",
    "map_constraint_revision_versions",
    "map_constraint_versions",
    "map_no_go_areas",
    "map_revision_no_go_areas",
    "map_revision_virtual_walls",
    "map_revisions",
    "map_virtual_walls",
    "plan_blocks",
    "plan_profiles",
    "plans",
    "robot_active_map",
    "robot_active_map_revision",
    "robot_pending_map_revision",
    "robot_pending_map_switch",
    "schema_meta",
    "zone_active_plans",
    "zone_editor_metadata",
    "zone_versions",
    "zones",
}

OPERATIONS_TABLES = {
    "actuator_profiles",
    "job_schedule_state",
    "job_schedules",
    "jobs",
    "mission_checkpoints",
    "mission_runs",
    "robot_events",
    "robot_runtime_state",
    "schema_meta",
    "slam_jobs",
    "sys_profiles",
}

PLANNING_EMPTY_TABLES = PLANNING_TABLES - {"schema_meta", "plan_profiles"}
OPERATIONS_EMPTY_TABLES = OPERATIONS_TABLES - {
    "schema_meta",
    "actuator_profiles",
    "sys_profiles",
    "robot_events",
    "robot_runtime_state",
}

EXPECTED_PLAN_PROFILES = {"cover_standard", "cover_eco"}
EXPECTED_ACTUATOR_PROFILES = {"standard", "heavy", "eco"}
EXPECTED_SYS_PROFILES = {"standard", "heavy", "eco"}
EXPECTED_PLAN_PROFILE_ROWS = {
    "cover_standard": {
        "description": "default coverage execution profile",
        "enabled": 1,
        "usage_type": "normal",
    },
    "cover_eco": {
        "description": "reserved plan profile for inspection/AI-triggered cleaning",
        "enabled": 1,
        "usage_type": "inspect_reserved",
    },
}
EXPECTED_ACTUATOR_PROFILE_ROWS = {
    "standard": {
        "main_brush_speed": 40,
        "side_brush_speed": 10,
        "brush_down_distance": 1000,
        "water_pump_pwm": 12,
        "suction_machine_pwm": 70,
        "vacuum_motor_pwm": 70,
        "height_scrub": 10,
        "height_scrub_active": 0,
        "side_brush_enable": 1,
    },
    "heavy": {
        "main_brush_speed": 60,
        "side_brush_speed": 20,
        "brush_down_distance": 80,
        "water_pump_pwm": 40,
        "suction_machine_pwm": 50,
        "vacuum_motor_pwm": 50,
        "height_scrub": 42,
        "height_scrub_active": 0,
        "side_brush_enable": 1,
    },
    "eco": {
        "main_brush_speed": 0,
        "side_brush_speed": 0,
        "brush_down_distance": 50,
        "water_pump_pwm": 0,
        "suction_machine_pwm": 0,
        "vacuum_motor_pwm": 0,
        "height_scrub": 5,
        "height_scrub_active": 0,
        "side_brush_enable": 0,
    },
}
EXPECTED_SYS_PROFILE_ROWS = {
    "standard": {
        "mbf_controller_name": "MPPI_Standard_Controller",
        "actuator_profile_name": "standard",
        "default_clean_mode": "scrub",
        "enabled": 1,
    },
    "heavy": {
        "mbf_controller_name": "MPPI_Heavy_Controller",
        "actuator_profile_name": "heavy",
        "default_clean_mode": "scrub",
        "enabled": 1,
    },
    "eco": {
        "mbf_controller_name": "MPPI_Eco_Controller",
        "actuator_profile_name": "eco",
        "default_clean_mode": "scrub",
        "enabled": 1,
    },
}
EXPECTED_MAP_TF_EVENT = (
    'HEALTH_WARN:TF_LOOKUP_FAIL:"map" passed to lookupTransform argument target_frame does not exist.'
)

ZERO_AUTO_CHARGE_COUNTERS = (
    "attempt_count",
    "canceled_count",
    "completed_count",
    "failed_count",
    "recovery_attempt_count",
    "recovery_failed_count",
    "recovery_success_count",
)

AUTO_CHARGE_REQUIRED_FIELDS = {
    "schema_version",
    *ZERO_AUTO_CHARGE_COUNTERS,
    "current_cycle",
    "recent_cycles",
    "last_event",
}

COMMERCIAL_DATA_ROOT = "/data"


class CommissioningStateError(ValueError):
    """The host is not in the exact safe state required by the new-vehicle gate."""


_CANONICAL_SCHEMA_SIGNATURES = None


class _NoCloseConnection:
    """Delegate SQLite operations while letting a schema builder call close safely."""

    def __init__(self, connection: sqlite3.Connection):
        self._connection = connection

    def __getattr__(self, name):
        return getattr(self._connection, name)

    def close(self) -> None:
        return None


def _require_absolute_real_file(path: str, label: str) -> str:
    normalized = os.path.abspath(str(path or ""))
    if not str(path or "").strip() or not os.path.isabs(str(path)):
        raise CommissioningStateError("%s must be an explicit absolute path" % label)
    if os.path.realpath(normalized) != normalized:
        raise CommissioningStateError("%s must not traverse a symlink" % label)
    try:
        info = os.lstat(normalized)
    except OSError as exc:
        raise CommissioningStateError("%s is unavailable: %s" % (label, exc))
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise CommissioningStateError("%s must be a real regular file" % label)
    return normalized


def _require_absolute_path_with_real_parent(path: str, label: str) -> str:
    raw_path = str(path or "")
    if not raw_path.strip() or not os.path.isabs(raw_path):
        raise CommissioningStateError("%s path must be explicit and absolute" % label)
    normalized = os.path.abspath(raw_path)
    parent = os.path.dirname(normalized)
    if os.path.realpath(parent) != parent:
        raise CommissioningStateError("%s parent must not traverse a symlink" % label)
    try:
        parent_info = os.lstat(parent)
    except OSError as exc:
        raise CommissioningStateError("%s parent is unavailable: %s" % (label, exc))
    if stat.S_ISLNK(parent_info.st_mode) or not stat.S_ISDIR(parent_info.st_mode):
        raise CommissioningStateError("%s parent must be a real directory" % label)
    return normalized


def _connect_read_only(path: str, label: str) -> sqlite3.Connection:
    real_path = _require_absolute_real_file(path, label)
    uri = "file:%s?mode=ro" % quote(real_path, safe="/")
    try:
        conn = sqlite3.connect(uri, uri=True, timeout=2.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only = ON")
        conn.execute("PRAGMA busy_timeout = 2000")
        conn.execute("BEGIN")
        return conn
    except Exception as exc:
        raise CommissioningStateError("unable to open %s read-only: %s" % (label, exc))


def _quoted_identifier(name: str) -> str:
    return '"%s"' % str(name).replace('"', '""')


def _require_database_integrity(conn: sqlite3.Connection, label: str) -> None:
    rows = [str(row[0]) for row in conn.execute("PRAGMA integrity_check")]
    if rows != ["ok"]:
        raise CommissioningStateError("%s integrity_check failed: %s" % (label, rows))
    foreign_key_violations = [tuple(row) for row in conn.execute("PRAGMA foreign_key_check")]
    if foreign_key_violations:
        raise CommissioningStateError(
            "%s foreign_key_check failed: %s" % (label, foreign_key_violations)
        )


def _table_inventory(conn: sqlite3.Connection) -> set:
    return {
        str(row[0])
        for row in conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
    }


def _require_inventory(conn: sqlite3.Connection, expected: set, label: str) -> None:
    observed = _table_inventory(conn)
    if observed != expected:
        raise CommissioningStateError(
            "%s table inventory mismatch missing=%s extra=%s"
            % (label, sorted(expected - observed), sorted(observed - expected))
        )


def _schema_signature(conn: sqlite3.Connection) -> Tuple[Tuple[str, str, str, str], ...]:
    rows = conn.execute(
        "SELECT type, name, tbl_name, sql FROM sqlite_master "
        "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
    )
    return tuple(
        (
            str(row[0]),
            str(row[1]),
            str(row[2]),
            " ".join(str(row[3] or "").split()),
        )
        for row in rows
    )


def _canonical_schema_signatures() -> Tuple[Tuple[object, ...], Tuple[object, ...]]:
    global _CANONICAL_SCHEMA_SIGNATURES
    if _CANONICAL_SCHEMA_SIGNATURES is not None:
        return _CANONICAL_SCHEMA_SIGNATURES

    from coverage_planner.ops_store.store import OperationsStore
    from coverage_planner.plan_store.store import PlanStore

    plan_store = PlanStore(":memory:")
    operations_connection = sqlite3.connect(":memory:")
    operations_connection.row_factory = sqlite3.Row
    operations_store = object.__new__(OperationsStore)
    operations_store.db_path = ":memory:"
    operations_store._connect = lambda: _NoCloseConnection(operations_connection)
    try:
        operations_store.ensure_schema()
        _CANONICAL_SCHEMA_SIGNATURES = (
            _schema_signature(plan_store.conn),
            _schema_signature(operations_connection),
        )
    finally:
        plan_store.close()
        operations_connection.close()
    return _CANONICAL_SCHEMA_SIGNATURES


def _require_schema_signature(
    conn: sqlite3.Connection,
    expected: Tuple[Tuple[str, str, str, str], ...],
    label: str,
) -> None:
    observed = _schema_signature(conn)
    if observed == expected:
        return
    expected_by_name = {(item[0], item[1]): item for item in expected}
    observed_by_name = {(item[0], item[1]): item for item in observed}
    expected_keys = set(expected_by_name)
    observed_keys = set(observed_by_name)
    changed = sorted(
        "%s:%s" % key
        for key in expected_keys & observed_keys
        if expected_by_name[key] != observed_by_name[key]
    )
    raise CommissioningStateError(
        "%s schema mismatch missing=%s extra=%s changed=%s"
        % (
            label,
            sorted("%s:%s" % key for key in expected_keys - observed_keys),
            sorted("%s:%s" % key for key in observed_keys - expected_keys),
            changed,
        )
    )


def _table_count(conn: sqlite3.Connection, table: str) -> int:
    return int(conn.execute("SELECT COUNT(*) FROM %s" % _quoted_identifier(table)).fetchone()[0])


def _require_empty_tables(conn: sqlite3.Connection, tables: Iterable[str], label: str) -> Dict[str, int]:
    counts = {name: _table_count(conn, name) for name in sorted(tables)}
    nonempty = {name: count for name, count in counts.items() if count != 0}
    if nonempty:
        raise CommissioningStateError("%s contains vehicle data: %s" % (label, nonempty))
    return counts


def _require_schema_generation(conn: sqlite3.Connection, expected: str, label: str) -> None:
    rows = list(conn.execute("SELECT key, value FROM schema_meta ORDER BY key"))
    observed = [(str(row["key"]), str(row["value"])) for row in rows]
    if observed != [("schema_generation", expected)]:
        raise CommissioningStateError("%s schema generation mismatch: %s" % (label, observed))


def _require_profile_names(
    conn: sqlite3.Connection,
    table: str,
    column: str,
    expected: set,
    label: str,
) -> List[str]:
    rows = conn.execute(
        "SELECT %s FROM %s ORDER BY %s"
        % (_quoted_identifier(column), _quoted_identifier(table), _quoted_identifier(column))
    )
    observed = [str(row[0]) for row in rows]
    if set(observed) != expected or len(observed) != len(expected):
        raise CommissioningStateError("%s seed profiles mismatch: %s" % (label, observed))
    return observed


def _require_exact_profile_rows(
    conn: sqlite3.Connection,
    *,
    table: str,
    key_column: str,
    expected: Mapping[str, Mapping[str, object]],
    label: str,
) -> Dict[str, Dict[str, object]]:
    rows = [dict(row) for row in conn.execute("SELECT * FROM %s" % _quoted_identifier(table))]
    observed = {str(row.get(key_column) or ""): row for row in rows}
    if set(observed) != set(expected) or len(rows) != len(expected):
        raise CommissioningStateError("%s profile inventory mismatch" % label)
    result = {}
    for profile_name, expected_fields in expected.items():
        row = observed[profile_name]
        values = {field: row.get(field) for field in expected_fields}
        if values != dict(expected_fields):
            raise CommissioningStateError(
                "%s profile %s mismatch expected=%s observed=%s"
                % (label, profile_name, dict(expected_fields), values)
            )
        result[profile_name] = values
    return result


def _planning_snapshot(path: str) -> Dict[str, object]:
    conn = _connect_read_only(path, "planning.db")
    try:
        _require_database_integrity(conn, "planning.db")
        _require_inventory(conn, PLANNING_TABLES, "planning.db")
        _require_schema_signature(conn, _canonical_schema_signatures()[0], "planning.db")
        _require_schema_generation(conn, "plan_store_v2_scoped_maps", "planning.db")
        counts = _require_empty_tables(conn, PLANNING_EMPTY_TABLES, "planning.db")
        profiles = _require_profile_names(
            conn,
            "plan_profiles",
            "plan_profile_name",
            EXPECTED_PLAN_PROFILES,
            "planning.db",
        )
        profile_rows = _require_exact_profile_rows(
            conn,
            table="plan_profiles",
            key_column="plan_profile_name",
            expected=EXPECTED_PLAN_PROFILE_ROWS,
            label="planning.db",
        )
        return {
            "empty_counts": counts,
            "plan_profiles": profiles,
            "plan_profile_rows": profile_rows,
        }
    except CommissioningStateError:
        raise
    except Exception as exc:
        raise CommissioningStateError("planning.db validation failed: %s" % exc)
    finally:
        conn.close()


def _require_empty_runtime_value(row: Mapping[str, object], field: str) -> None:
    if str(row[field] or "").strip():
        raise CommissioningStateError("robot_runtime_state.%s must be empty" % field)


def _runtime_snapshot(conn: sqlite3.Connection, robot_id: str) -> Dict[str, object]:
    rows = [dict(row) for row in conn.execute("SELECT * FROM robot_runtime_state ORDER BY robot_id")]
    if len(rows) != 1:
        raise CommissioningStateError("robot_runtime_state must contain exactly one vehicle row")
    row = rows[0]
    if str(row.get("robot_id") or "") != robot_id:
        raise CommissioningStateError(
            "robot_runtime_state identity mismatch expected=%s observed=%s"
            % (robot_id, str(row.get("robot_id") or ""))
        )
    for field in (
        "active_run_id",
        "active_job_id",
        "active_schedule_id",
        "map_name",
        "map_revision_id",
        "last_error_code",
        "last_error_msg",
    ):
        _require_empty_runtime_value(row, field)
    if str(row.get("localization_state") or "").strip() not in ("", "not_localized"):
        raise CommissioningStateError("robot_runtime_state localization_state is not a new-vehicle value")
    if int(row.get("localization_valid") or 0) != 0:
        raise CommissioningStateError("robot_runtime_state localization_valid must be false")
    for field in ("mission_state", "phase", "public_state", "dock_state"):
        if str(row.get(field) or "").strip().upper() != "IDLE":
            raise CommissioningStateError("robot_runtime_state.%s must be IDLE" % field)
    if str(row.get("executor_state") or "").strip().upper() not in ("", "IDLE"):
        raise CommissioningStateError("robot_runtime_state.executor_state must be empty or IDLE")
    for field in ("return_to_dock_on_finish", "repeat_after_full_charge", "battery_valid"):
        if int(row.get(field) or 0) != 0:
            raise CommissioningStateError("robot_runtime_state.%s must be false" % field)
    return {
        "robot_id": robot_id,
        "localization_state": str(row.get("localization_state") or ""),
        "mission_state": "IDLE",
        "phase": "IDLE",
        "public_state": "IDLE",
        "dock_state": "IDLE",
        "executor_state": str(row.get("executor_state") or ""),
    }


def _events_snapshot(conn: sqlite3.Connection) -> Dict[str, object]:
    rows = [dict(row) for row in conn.execute("SELECT * FROM robot_events ORDER BY event_id")]
    for row in rows:
        expected = {
            "scope": "SYSTEM",
            "component": "TASK",
            "level": "INFO",
            "code": "HEALTH",
            "message": EXPECTED_MAP_TF_EVENT,
            "run_id": "",
            "job_id": "",
            "zone_id": "",
            "data_json": "",
        }
        for key, value in expected.items():
            if str(row.get(key) or "") != value:
                raise CommissioningStateError("robot_events contains non-commissioning event id=%s" % row.get("event_id"))
    # Event timestamps/counts may grow while the read-only health gate runs.
    return {"all_events_match_new_vehicle_map_tf_warning": True}


def _operations_snapshot(path: str, robot_id: str) -> Dict[str, object]:
    conn = _connect_read_only(path, "operations.db")
    try:
        _require_database_integrity(conn, "operations.db")
        _require_inventory(conn, OPERATIONS_TABLES, "operations.db")
        _require_schema_signature(conn, _canonical_schema_signatures()[1], "operations.db")
        _require_schema_generation(conn, "ops_store_v2_scoped_maps", "operations.db")
        counts = _require_empty_tables(conn, OPERATIONS_EMPTY_TABLES, "operations.db")
        actuator_profiles = _require_profile_names(
            conn,
            "actuator_profiles",
            "actuator_profile_name",
            EXPECTED_ACTUATOR_PROFILES,
            "operations.db actuator",
        )
        sys_profiles = _require_profile_names(
            conn,
            "sys_profiles",
            "sys_profile_name",
            EXPECTED_SYS_PROFILES,
            "operations.db sys",
        )
        actuator_profile_rows = _require_exact_profile_rows(
            conn,
            table="actuator_profiles",
            key_column="actuator_profile_name",
            expected=EXPECTED_ACTUATOR_PROFILE_ROWS,
            label="operations.db actuator",
        )
        sys_profile_rows = _require_exact_profile_rows(
            conn,
            table="sys_profiles",
            key_column="sys_profile_name",
            expected=EXPECTED_SYS_PROFILE_ROWS,
            label="operations.db sys",
        )
        runtime = _runtime_snapshot(conn, robot_id)
        events = _events_snapshot(conn)
        return {
            "empty_counts": counts,
            "actuator_profiles": actuator_profiles,
            "sys_profiles": sys_profiles,
            "actuator_profile_rows": actuator_profile_rows,
            "sys_profile_rows": sys_profile_rows,
            "runtime": runtime,
            "events": events,
        }
    except CommissioningStateError:
        raise
    except Exception as exc:
        raise CommissioningStateError("operations.db validation failed: %s" % exc)
    finally:
        conn.close()


def _maps_snapshot(path: str) -> Dict[str, object]:
    normalized = os.path.abspath(str(path or ""))
    if not str(path or "").strip() or not os.path.isabs(str(path)):
        raise CommissioningStateError("maps root must be an explicit absolute path")
    if os.path.realpath(normalized) != normalized:
        raise CommissioningStateError("maps root must not traverse a symlink")
    try:
        root_info = os.lstat(normalized)
    except OSError as exc:
        raise CommissioningStateError("maps root is unavailable: %s" % exc)
    if stat.S_ISLNK(root_info.st_mode) or not stat.S_ISDIR(root_info.st_mode):
        raise CommissioningStateError("maps root must be a real directory")
    entries = sorted(os.scandir(normalized), key=lambda item: item.name)
    if not entries:
        return {"entries": []}
    if len(entries) != 1 or entries[0].name != "imports":
        raise CommissioningStateError("maps root must be empty or contain only empty imports/")
    imports = entries[0]
    imports_info = imports.stat(follow_symlinks=False)
    if imports.is_symlink() or not stat.S_ISDIR(imports_info.st_mode) or os.path.ismount(imports.path):
        raise CommissioningStateError("maps imports must be a real, non-mounted directory")
    if list(os.scandir(imports.path)):
        raise CommissioningStateError("maps imports must be empty")
    return {"entries": ["imports/"]}


def _dock_calibration_snapshot(path: str) -> Dict[str, object]:
    normalized = _require_absolute_path_with_real_parent(path, "dock calibration")
    if os.path.lexists(normalized):
        raise CommissioningStateError("new vehicle must not contain dock calibration data or symlink")
    return {"present": False}


def _auto_charge_snapshot(path: str) -> Dict[str, object]:
    normalized = _require_absolute_path_with_real_parent(path, "auto-charge state")
    if not os.path.lexists(normalized):
        return {"present": False, "counters": {name: 0 for name in ZERO_AUTO_CHARGE_COUNTERS}}
    real_path = _require_absolute_real_file(normalized, "auto-charge state")
    try:
        with open(real_path, "r", encoding="utf-8") as stream:
            payload = json.load(stream)
    except Exception as exc:
        raise CommissioningStateError("auto-charge state is unreadable: %s" % exc)
    if not isinstance(payload, dict):
        raise CommissioningStateError("auto-charge state root must be an object")
    missing_fields = AUTO_CHARGE_REQUIRED_FIELDS - set(payload)
    if missing_fields:
        raise CommissioningStateError(
            "auto-charge state is missing required fields: %s" % sorted(missing_fields)
        )
    schema_version = payload["schema_version"]
    if isinstance(schema_version, bool) or not isinstance(schema_version, int) or schema_version != 1:
        raise CommissioningStateError("auto-charge state schema_version must be integer 1")
    counters = {}
    for name in ZERO_AUTO_CHARGE_COUNTERS:
        value = payload[name]
        if isinstance(value, bool) or not isinstance(value, int):
            raise CommissioningStateError("auto-charge state %s must be an integer" % name)
        counters[name] = value
    if any(counters.values()):
        raise CommissioningStateError("auto-charge state contains nonzero history: %s" % counters)
    if payload["current_cycle"] is not None:
        raise CommissioningStateError("auto-charge state contains a cycle")
    if not isinstance(payload["recent_cycles"], list):
        raise CommissioningStateError("auto-charge state recent_cycles must be a list")
    if payload["recent_cycles"]:
        raise CommissioningStateError("auto-charge state contains a cycle")
    if payload["last_event"] is not None:
        raise CommissioningStateError("auto-charge state contains a last_event")
    return {"present": True, "counters": counters, "schema_version": schema_version}


def _auto_charge_event_log_snapshot(path: str) -> Dict[str, object]:
    normalized = _require_absolute_path_with_real_parent(path, "auto-charge event log")
    if not os.path.lexists(normalized):
        return {"present": False, "size_bytes": 0}
    real_path = _require_absolute_real_file(normalized, "auto-charge event log")
    size_bytes = int(os.stat(real_path, follow_symlinks=False).st_size)
    if size_bytes != 0:
        raise CommissioningStateError(
            "new vehicle auto-charge event log must be absent or empty"
        )
    return {"present": True, "size_bytes": 0}


def _secure_path_metadata(
    path: str,
    *,
    label: str,
    expected_mode: int,
    expected_uid: int,
    expected_gid: int,
    expect_directory: bool,
) -> Dict[str, object]:
    normalized = os.path.abspath(str(path or ""))
    if not str(path or "").strip() or not os.path.isabs(str(path or "")):
        raise CommissioningStateError("%s must be an explicit absolute path" % label)
    if os.path.realpath(normalized) != normalized:
        raise CommissioningStateError("%s must not traverse a symlink" % label)
    try:
        info = os.lstat(normalized)
    except OSError as exc:
        raise CommissioningStateError("%s is unavailable: %s" % (label, exc))
    expected_type = stat.S_ISDIR if expect_directory else stat.S_ISREG
    if stat.S_ISLNK(info.st_mode) or not expected_type(info.st_mode):
        raise CommissioningStateError(
            "%s must be a real %s" % (label, "directory" if expect_directory else "regular file")
        )
    observed_mode = stat.S_IMODE(info.st_mode)
    if info.st_uid != expected_uid or info.st_gid != expected_gid:
        raise CommissioningStateError(
            "%s owner mismatch expected=%s:%s observed=%s:%s"
            % (label, expected_uid, expected_gid, info.st_uid, info.st_gid)
        )
    if observed_mode != expected_mode:
        raise CommissioningStateError(
            "%s mode mismatch expected=%04o observed=%04o"
            % (label, expected_mode, observed_mode)
        )
    return {
        "uid": int(info.st_uid),
        "gid": int(info.st_gid),
        "mode": "%04o" % observed_mode,
        "type": "directory" if expect_directory else "file",
    }


def _commercial_data_root_metadata_snapshot() -> Dict[str, object]:
    return _secure_path_metadata(
        COMMERCIAL_DATA_ROOT,
        label="/data root",
        expected_mode=0o755,
        expected_uid=0,
        expected_gid=0,
        expect_directory=True,
    )


def _commercial_storage_metadata_snapshot(
    *,
    plan_db_path: str,
    ops_db_path: str,
    maps_root: str,
    dock_calibration_path: str,
    auto_charge_state_path: str,
    auto_charge_event_log_path: str,
) -> Dict[str, object]:
    expected_uid = os.geteuid()
    expected_gid = os.getegid()
    if expected_uid == 0:
        raise CommissioningStateError("new-vehicle gate must run as the non-root runtime user")

    coverage_root = os.path.dirname(os.path.abspath(plan_db_path))
    result = {
        "data_root": _commercial_data_root_metadata_snapshot(),
        "coverage_root": _secure_path_metadata(
            coverage_root,
            label="coverage root",
            expected_mode=0o750,
            expected_uid=expected_uid,
            expected_gid=expected_gid,
            expect_directory=True,
        ),
        "maps_root": _secure_path_metadata(
            maps_root,
            label="maps root",
            expected_mode=0o750,
            expected_uid=expected_uid,
            expected_gid=expected_gid,
            expect_directory=True,
        ),
    }
    imports_root = os.path.join(os.path.abspath(maps_root), "imports")
    if os.path.lexists(imports_root):
        result["maps_imports"] = _secure_path_metadata(
            imports_root,
            label="maps imports",
            expected_mode=0o750,
            expected_uid=expected_uid,
            expected_gid=expected_gid,
            expect_directory=True,
        )

    for path, label in ((plan_db_path, "planning.db"), (ops_db_path, "operations.db")):
        result[label] = _secure_path_metadata(
            path,
            label=label,
            expected_mode=0o640,
            expected_uid=expected_uid,
            expected_gid=expected_gid,
            expect_directory=False,
        )
        for suffix in ("-wal", "-shm"):
            sidecar = str(path) + suffix
            if os.path.lexists(sidecar):
                result[label + suffix] = _secure_path_metadata(
                    sidecar,
                    label=label + suffix,
                    expected_mode=0o640,
                    expected_uid=expected_uid,
                    expected_gid=expected_gid,
                    expect_directory=False,
                )

    for path, label in (
        (auto_charge_state_path, "auto-charge state"),
        (auto_charge_event_log_path, "auto-charge event log"),
    ):
        if os.path.lexists(path):
            result[label] = _secure_path_metadata(
                path,
                label=label,
                expected_mode=0o640,
                expected_uid=expected_uid,
                expected_gid=expected_gid,
                expect_directory=False,
            )
    return result


def build_new_vehicle_commissioning_snapshot(
    *,
    plan_db_path: str,
    ops_db_path: str,
    maps_root: str,
    dock_calibration_path: str,
    auto_charge_state_path: str,
    auto_charge_event_log_path: str,
    robot_id: str,
) -> Dict[str, object]:
    expected_robot_id = str(robot_id or "").strip()
    if not expected_robot_id or expected_robot_id == "local_robot" or "REPLACE_" in expected_robot_id:
        raise CommissioningStateError("new-vehicle gate requires an explicit commercial robot_id")
    storage_metadata = _commercial_storage_metadata_snapshot(
        plan_db_path=plan_db_path,
        ops_db_path=ops_db_path,
        maps_root=maps_root,
        dock_calibration_path=dock_calibration_path,
        auto_charge_state_path=auto_charge_state_path,
        auto_charge_event_log_path=auto_charge_event_log_path,
    )
    return {
        "robot_id": expected_robot_id,
        "planning": _planning_snapshot(plan_db_path),
        "operations": _operations_snapshot(ops_db_path, expected_robot_id),
        "maps": _maps_snapshot(maps_root),
        "dock_calibration": _dock_calibration_snapshot(dock_calibration_path),
        "auto_charge": _auto_charge_snapshot(auto_charge_state_path),
        "auto_charge_event_log": _auto_charge_event_log_snapshot(
            auto_charge_event_log_path
        ),
        "storage_metadata": storage_metadata,
    }

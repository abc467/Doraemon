#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import sqlite3
import stat
import sys
import tempfile
import unittest
from unittest import mock


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
PKG_DIR = os.path.dirname(THIS_DIR)
PKG_SRC_DIR = os.path.join(PKG_DIR, "src")
if PKG_SRC_DIR not in sys.path:
    sys.path.insert(0, PKG_SRC_DIR)

from coverage_planner.new_vehicle_commissioning_state import (
    CommissioningStateError,
    EXPECTED_MAP_TF_EVENT,
    OPERATIONS_TABLES,
    PLANNING_TABLES,
    build_new_vehicle_commissioning_snapshot,
)
from coverage_planner import new_vehicle_commissioning_state as commissioning_state
from coverage_planner.ops_store.store import OperationsStore
from coverage_planner.plan_store.store import PlanStore


ROBOT_ID = "CR-001"


def _quoted(name):
    return '"%s"' % str(name).replace('"', '""')


def _create_planning_db(path):
    store = PlanStore(path)
    store.close()


def _create_operations_db(path):
    OperationsStore(path)
    conn = sqlite3.connect(path)
    try:
        conn.executemany(
            """
            INSERT INTO actuator_profiles(
              actuator_profile_name, main_brush_speed, side_brush_speed,
              brush_down_distance, water_pump_pwm, suction_machine_pwm,
              vacuum_motor_pwm, height_scrub, height_scrub_active,
              side_brush_enable, updated_ts
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                ("standard", 40, 10, 1000, 0, 0, 0, 10, 0, 1, 1.0),
                ("heavy", 60, 20, 80, 40, 50, 50, 42, 0, 1, 1.0),
                ("eco", 0, 0, 50, 0, 0, 0, 5, 0, 0, 1.0),
            ],
        )
        conn.executemany(
            """
            INSERT INTO sys_profiles(
              sys_profile_name, mbf_controller_name, actuator_profile_name,
              default_clean_mode, enabled, updated_ts
            ) VALUES (?, ?, ?, ?, 1, ?)
            """,
            [
                ("standard", "MPPI_Standard_Controller", "standard", "scrub", 1.0),
                ("heavy", "MPPI_Standard_Controller", "heavy", "scrub", 1.0),
                ("eco", "MPPI_Standard_Controller", "eco", "scrub", 1.0),
            ],
        )
        conn.execute(
            """
            INSERT INTO robot_runtime_state(
              robot_id, active_run_id, active_job_id, active_schedule_id,
              map_name, map_revision_id, localization_state, localization_valid,
              mission_state, phase, public_state, return_to_dock_on_finish,
              repeat_after_full_charge, armed, dock_state, battery_soc,
              battery_valid, executor_state, last_error_code, last_error_msg,
              updated_ts
            ) VALUES (?, '', '', '', '', '', '', 0, 'IDLE', 'IDLE', 'IDLE',
                      0, 0, 1, 'IDLE', 0.0, 0, '', '', '', 1.0)
            """,
            (ROBOT_ID,),
        )
        conn.commit()
    finally:
        conn.close()


def _insert_allowed_health_event(path):
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            """
            INSERT INTO robot_events(
              event_id, ts, scope, component, level, code, message,
              run_id, job_id, zone_id, data_json
            ) VALUES (1, 1.0, 'SYSTEM', 'TASK', 'INFO', 'HEALTH', ?, '', '', '', '')
            """,
            (EXPECTED_MAP_TF_EVENT,),
        )
        conn.commit()
    finally:
        conn.close()


class NewVehicleCommissioningStateTest(unittest.TestCase):
    def setUp(self):
        self.data_root_metadata = {
            "uid": 0,
            "gid": 0,
            "mode": "0755",
            "type": "directory",
        }
        self.data_root_gate_patcher = mock.patch.object(
            commissioning_state,
            "_commercial_data_root_metadata_snapshot",
            return_value=self.data_root_metadata,
        )
        self.data_root_gate = self.data_root_gate_patcher.start()
        self.temp_dir = tempfile.TemporaryDirectory(prefix="doraemon-new-vehicle-test-")
        self.root = self.temp_dir.name
        self.plan_db = os.path.join(self.root, "planning.db")
        self.ops_db = os.path.join(self.root, "operations.db")
        self.maps_root = os.path.join(self.root, "maps")
        self.imports_root = os.path.join(self.maps_root, "imports")
        self.dock_calibration = os.path.join(self.root, "dock_calibration.yaml")
        self.auto_charge_state = os.path.join(self.root, "auto_charge_monitor_state.json")
        self.auto_charge_event_log = os.path.join(
            self.root, "auto_charge_monitor_events.jsonl"
        )
        os.makedirs(self.imports_root)
        _create_planning_db(self.plan_db)
        _create_operations_db(self.ops_db)
        with open(self.auto_charge_state, "w", encoding="utf-8") as stream:
            json.dump(
                {
                    "schema_version": 1,
                    "attempt_count": 0,
                    "canceled_count": 0,
                    "completed_count": 0,
                    "failed_count": 0,
                    "recovery_attempt_count": 0,
                    "recovery_failed_count": 0,
                    "recovery_success_count": 0,
                    "current_cycle": None,
                    "recent_cycles": [],
                    "last_event": None,
                },
                stream,
            )
        os.chmod(self.root, 0o750)
        os.chmod(self.maps_root, 0o750)
        os.chmod(self.imports_root, 0o750)
        os.chmod(self.plan_db, 0o640)
        os.chmod(self.ops_db, 0o640)
        os.chmod(self.auto_charge_state, 0o640)

    def tearDown(self):
        self.temp_dir.cleanup()
        self.data_root_gate_patcher.stop()

    def snapshot(self):
        return build_new_vehicle_commissioning_snapshot(
            plan_db_path=self.plan_db,
            ops_db_path=self.ops_db,
            maps_root=self.maps_root,
            dock_calibration_path=self.dock_calibration,
            auto_charge_state_path=self.auto_charge_state,
            auto_charge_event_log_path=self.auto_charge_event_log,
            robot_id=ROBOT_ID,
        )

    def test_accepts_exact_empty_vehicle_with_allowed_map_tf_evidence(self):
        _insert_allowed_health_event(self.ops_db)

        result = self.snapshot()

        self.assertEqual(result["robot_id"], ROBOT_ID)
        self.assertEqual(result["maps"]["entries"], ["imports/"])
        self.assertFalse(result["dock_calibration"]["present"])
        self.assertEqual(result["operations"]["runtime"]["robot_id"], ROBOT_ID)
        self.assertTrue(
            result["operations"]["events"]["all_events_match_new_vehicle_map_tf_warning"]
        )
        self.assertEqual(result["storage_metadata"]["data_root"], self.data_root_metadata)
        self.data_root_gate.assert_called_once_with()

    def test_reads_vehicle_pollution_committed_only_to_wal(self):
        writer = sqlite3.connect(self.plan_db)
        try:
            self.assertEqual(writer.execute("PRAGMA journal_mode=WAL").fetchone()[0].lower(), "wal")
            writer.execute("PRAGMA wal_autocheckpoint=0")
            writer.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            writer.execute(
                """
                INSERT INTO map_assets(
                  map_name, yaml_path, pgm_path, created_ts, updated_ts
                ) VALUES ('wal-map', '/tmp/wal.yaml', '/tmp/wal.pgm', 1.0, 1.0)
                """
            )
            writer.commit()
            self.assertTrue(os.path.exists(self.plan_db + "-wal"))
            self.assertGreater(os.path.getsize(self.plan_db + "-wal"), 0)
            os.chmod(self.plan_db + "-wal", 0o640)
            os.chmod(self.plan_db + "-shm", 0o640)

            with self.assertRaisesRegex(CommissioningStateError, "map_assets"):
                self.snapshot()
        finally:
            writer.close()

    def test_rejects_missing_required_database_table(self):
        conn = sqlite3.connect(self.plan_db)
        try:
            conn.execute("DROP TABLE map_assets")
            conn.commit()
        finally:
            conn.close()

        with self.assertRaisesRegex(CommissioningStateError, "table inventory mismatch"):
            self.snapshot()

    def test_rejects_database_schema_or_index_tampering(self):
        conn = sqlite3.connect(self.plan_db)
        try:
            conn.execute("DROP INDEX idx_map_assets_identity")
            conn.commit()
        finally:
            conn.close()

        with self.assertRaisesRegex(CommissioningStateError, "schema mismatch"):
            self.snapshot()

    def test_rejects_map_file(self):
        with open(os.path.join(self.maps_root, "copied_map.pbstream"), "wb") as stream:
            stream.write(b"not-a-new-vehicle")

        with self.assertRaisesRegex(CommissioningStateError, "maps root must be empty"):
            self.snapshot()

    def test_rejects_dangling_dock_calibration_symlink(self):
        os.symlink(os.path.join(self.root, "missing-calibration.yaml"), self.dock_calibration)

        with self.assertRaisesRegex(CommissioningStateError, "must not contain dock calibration"):
            self.snapshot()

    def test_rejects_mission_action_history(self):
        conn = sqlite3.connect(self.ops_db)
        try:
            conn.execute(
                """
                INSERT INTO mission_runs(run_id, created_ts, start_ts, updated_ts)
                VALUES ('prior-run', 1.0, 1.0, 1.0)
                """
            )
            conn.commit()
        finally:
            conn.close()

        with self.assertRaisesRegex(CommissioningStateError, "mission_runs"):
            self.snapshot()

    def test_rejects_auto_charge_action_history(self):
        with open(self.auto_charge_state, "w", encoding="utf-8") as stream:
            payload = {
                "schema_version": 1,
                "attempt_count": 1,
                "canceled_count": 0,
                "completed_count": 0,
                "failed_count": 0,
                "recovery_attempt_count": 0,
                "recovery_failed_count": 0,
                "recovery_success_count": 0,
                "current_cycle": None,
                "recent_cycles": [],
                "last_event": None,
            }
            json.dump(payload, stream)

        with self.assertRaisesRegex(CommissioningStateError, "nonzero history"):
            self.snapshot()

    def test_accepts_empty_auto_charge_event_log(self):
        with open(self.auto_charge_event_log, "wb"):
            pass
        os.chmod(self.auto_charge_event_log, 0o640)

        result = self.snapshot()

        self.assertTrue(result["auto_charge_event_log"]["present"])
        self.assertEqual(result["auto_charge_event_log"]["size_bytes"], 0)

    def test_rejects_nonempty_auto_charge_event_log(self):
        with open(self.auto_charge_event_log, "w", encoding="utf-8") as stream:
            stream.write('{"event":"prior_charge"}\n')
        os.chmod(self.auto_charge_event_log, 0o640)

        with self.assertRaisesRegex(CommissioningStateError, "must be absent or empty"):
            self.snapshot()

    def test_rejects_auto_charge_event_log_symlink(self):
        target = os.path.join(self.root, "real-auto-charge-events.jsonl")
        with open(target, "wb"):
            pass
        os.symlink(target, self.auto_charge_event_log)

        with self.assertRaisesRegex(CommissioningStateError, "must not traverse a symlink"):
            self.snapshot()

    def test_rejects_insecure_database_mode(self):
        os.chmod(self.plan_db, 0o644)

        with self.assertRaisesRegex(CommissioningStateError, "mode mismatch"):
            self.snapshot()

    def test_rejects_database_sidecar_symlink(self):
        target = os.path.join(self.root, "outside-wal")
        with open(target, "wb"):
            pass
        os.symlink(target, self.plan_db + "-wal")

        with self.assertRaisesRegex(CommissioningStateError, "must not traverse a symlink"):
            self.snapshot()

    def test_rejects_missing_auto_charge_schema_fields(self):
        with open(self.auto_charge_state, "w", encoding="utf-8") as stream:
            json.dump({}, stream)

        with self.assertRaisesRegex(CommissioningStateError, "missing required fields"):
            self.snapshot()

    def test_rejects_wrong_auto_charge_field_types(self):
        with open(self.auto_charge_state, "r", encoding="utf-8") as stream:
            payload = json.load(stream)
        payload["attempt_count"] = False
        with open(self.auto_charge_state, "w", encoding="utf-8") as stream:
            json.dump(payload, stream)

        with self.assertRaisesRegex(CommissioningStateError, "attempt_count must be an integer"):
            self.snapshot()

    def test_rejects_missing_target_under_symlink_parent(self):
        real_parent = os.path.join(self.root, "real-state")
        linked_parent = os.path.join(self.root, "linked-state")
        os.makedirs(real_parent)
        os.symlink(real_parent, linked_parent)
        self.auto_charge_state = os.path.join(linked_parent, "missing-auto-charge.json")

        with self.assertRaisesRegex(CommissioningStateError, "parent must not traverse a symlink"):
            self.snapshot()

    def test_rejects_runtime_identity_pollution(self):
        conn = sqlite3.connect(self.ops_db)
        try:
            conn.execute(
                "UPDATE robot_runtime_state SET robot_id='local_robot' WHERE robot_id=?",
                (ROBOT_ID,),
            )
            conn.commit()
        finally:
            conn.close()

        with self.assertRaisesRegex(CommissioningStateError, "identity mismatch"):
            self.snapshot()

    def test_rejects_seed_profile_value_tampering(self):
        conn = sqlite3.connect(self.ops_db)
        try:
            conn.execute(
                "UPDATE actuator_profiles SET water_pump_pwm=99 "
                "WHERE actuator_profile_name='standard'"
            )
            conn.commit()
        finally:
            conn.close()

        with self.assertRaisesRegex(CommissioningStateError, "profile standard mismatch"):
            self.snapshot()


class CommercialDataRootMetadataTest(unittest.TestCase):
    @staticmethod
    def stat_result(mode, uid=0, gid=0):
        return os.stat_result((mode, 0, 0, 1, uid, gid, 0, 0, 0, 0))

    def snapshot_with_stat(self, info):
        with mock.patch.object(
            commissioning_state.os.path,
            "realpath",
            side_effect=lambda path: path,
        ), mock.patch.object(commissioning_state.os, "lstat", return_value=info):
            return commissioning_state._commercial_data_root_metadata_snapshot()

    def test_accepts_real_root_owned_data_directory_mode_0755(self):
        result = self.snapshot_with_stat(
            self.stat_result(stat.S_IFDIR | 0o755, uid=0, gid=0)
        )

        self.assertEqual(
            result,
            {"uid": 0, "gid": 0, "mode": "0755", "type": "directory"},
        )

    def test_rejects_data_root_symlink(self):
        with mock.patch.object(
            commissioning_state.os.path,
            "realpath",
            return_value="/srv/redirected-data",
        ):
            with self.assertRaisesRegex(CommissioningStateError, "must not traverse a symlink"):
                commissioning_state._commercial_data_root_metadata_snapshot()

    def test_rejects_non_root_data_owner(self):
        info = self.stat_result(stat.S_IFDIR | 0o755, uid=1000, gid=1000)

        with self.assertRaisesRegex(CommissioningStateError, "owner mismatch"):
            self.snapshot_with_stat(info)

    def test_rejects_insecure_data_mode(self):
        info = self.stat_result(stat.S_IFDIR | 0o775, uid=0, gid=0)

        with self.assertRaisesRegex(CommissioningStateError, "mode mismatch"):
            self.snapshot_with_stat(info)


if __name__ == "__main__":
    unittest.main()

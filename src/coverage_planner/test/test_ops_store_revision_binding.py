#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import shutil
import sys
import tempfile
import unittest


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
PKG_DIR = os.path.dirname(THIS_DIR)
SRC_DIR = os.path.join(PKG_DIR, "src")

if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from coverage_planner.ops_store.store import (
    MissionCheckpointRecord,
    OperationsStore,
    RobotRuntimeStateRecord,
    SlamJobRecord,
)


class OperationsStoreRevisionBindingTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.mkdtemp(prefix="ops_store_revision_")
        self._db_path = os.path.join(self._tmpdir, "operations.db")
        self.store = OperationsStore(self._db_path)

    def tearDown(self):
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def test_job_run_runtime_and_slam_job_round_trip_revision_ids(self):
        self.store.upsert_job(
            job_id="101",
            job_name="demo_task",
            map_name="demo_map",
            map_revision_id="rev_demo_01",
            zone_id="zone_a",
            plan_profile_name="cover_standard",
            sys_profile_name="standard",
            default_clean_mode="scrub",
        )
        self.store.create_run(
            run_id="run_101",
            job_id="101",
            map_name="demo_map",
            map_revision_id="rev_demo_01",
            zone_id="zone_a",
            plan_profile_name="cover_standard",
            clean_mode="scrub",
        )
        self.store.upsert_robot_runtime_state(
            RobotRuntimeStateRecord(
                robot_id="local_robot",
                active_run_id="run_101",
                active_job_id="101",
                map_name="demo_map",
                map_revision_id="rev_demo_01",
                mission_state="RUNNING",
            )
        )
        self.store.upsert_slam_job(
            SlamJobRecord(
                job_id="slam_101",
                requested_map_name="demo_map",
                requested_map_revision_id="rev_demo_01",
                resolved_map_name="demo_map",
                resolved_map_revision_id="rev_demo_01",
                done=True,
                success=True,
            )
        )
        self.store.upsert_mission_checkpoint(
            MissionCheckpointRecord(
                run_id="run_101",
                zone_id="zone_a",
                plan_id="plan_101",
                zone_version=1,
                exec_index=2,
                block_id=3,
                path_index=4,
                path_s=5.0,
                state="RUNNING",
                water_off_latched=False,
                map_revision_id="rev_demo_01",
                map_id="map_demo",
                map_md5="md5_demo",
            )
        )

        job = self.store.get_job("101")
        run = self.store.get_run("run_101")
        runtime = self.store.get_robot_runtime_state("local_robot")
        slam_job = self.store.get_slam_job("slam_101")
        checkpoint = self.store.get_mission_checkpoint("run_101")

        self.assertEqual(job.map_revision_id, "rev_demo_01")
        self.assertEqual(run.map_revision_id, "rev_demo_01")
        self.assertEqual(runtime.map_revision_id, "rev_demo_01")
        self.assertEqual(slam_job.requested_map_revision_id, "rev_demo_01")
        self.assertEqual(slam_job.resolved_map_revision_id, "rev_demo_01")
        self.assertEqual(checkpoint.map_revision_id, "rev_demo_01")


if __name__ == "__main__":
    unittest.main()

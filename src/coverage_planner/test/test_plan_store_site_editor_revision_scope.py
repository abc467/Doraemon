#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import shutil
import sys
import tempfile
import unittest
from types import SimpleNamespace


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
PKG_DIR = os.path.dirname(THIS_DIR)
SRC_DIR = os.path.join(PKG_DIR, "src")

if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from coverage_planner.plan_store.store import PlanStore


class PlanStoreSiteEditorRevisionScopeTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.mkdtemp(prefix="plan_store_site_editor_scope_")
        self._db_path = os.path.join(self._tmpdir, "planning.db")
        self.store = PlanStore(self._db_path)
        self.rev_a = "rev_demo_a"
        self.rev_b = "rev_demo_b"
        self._seed_revision(self.rev_a, "snapshot_a")
        self._seed_revision(self.rev_b, "snapshot_b")

    def tearDown(self):
        try:
            self.store.close()
        finally:
            shutil.rmtree(self._tmpdir, ignore_errors=True)

    def _seed_revision(self, revision_id: str, snapshot_md5: str):
        self.store.upsert_map_asset(
            map_name="demo_map",
            revision_id=revision_id,
            enabled=True,
            description="demo",
            map_id="map_shared_demo",
            map_md5="md5_shared_demo",
            yaml_path="/tmp/demo_map.yaml",
            pgm_path="/tmp/demo_map.pgm",
            pbstream_path="/tmp/demo_map.pbstream",
            frame_id="map",
            resolution=0.05,
            origin=[0.0, 0.0, 0.0],
            lifecycle_status="available",
            verification_status="verified",
            save_snapshot_md5=snapshot_md5,
        )

    def test_alignment_configs_are_scoped_by_revision(self):
        self.store.upsert_map_alignment_config(
            map_name="demo_map",
            map_revision_id=self.rev_a,
            alignment_version="align_shared",
            map_id="map_shared_demo",
            map_version="md5_shared_demo",
            aligned_frame="site_a",
            pivot_x=1.0,
        )
        self.store.upsert_map_alignment_config(
            map_name="demo_map",
            map_revision_id=self.rev_b,
            alignment_version="align_shared",
            map_id="map_shared_demo",
            map_version="md5_shared_demo",
            aligned_frame="site_b",
            pivot_x=2.0,
        )

        align_a = self.store.get_map_alignment_config(
            map_name="demo_map",
            map_revision_id=self.rev_a,
            alignment_version="align_shared",
        ) or {}
        align_b = self.store.get_map_alignment_config(
            map_name="demo_map",
            map_revision_id=self.rev_b,
            alignment_version="align_shared",
        ) or {}

        self.assertEqual(align_a.get("map_revision_id"), self.rev_a)
        self.assertEqual(align_a.get("aligned_frame"), "site_a")
        self.assertEqual(float(align_a.get("pivot_x") or 0.0), 1.0)
        self.assertEqual(align_b.get("map_revision_id"), self.rev_b)
        self.assertEqual(align_b.get("aligned_frame"), "site_b")
        self.assertEqual(float(align_b.get("pivot_x") or 0.0), 2.0)

    def test_active_alignment_pointer_is_scoped_by_revision(self):
        self.store.upsert_map_alignment_config(
            map_name="demo_map",
            map_revision_id=self.rev_a,
            alignment_version="align_a",
            map_id="map_shared_demo",
            map_version="md5_shared_demo",
        )
        self.store.upsert_map_alignment_config(
            map_name="demo_map",
            map_revision_id=self.rev_b,
            alignment_version="align_b",
            map_id="map_shared_demo",
            map_version="md5_shared_demo",
        )

        self.store.set_active_map_alignment(
            map_name="demo_map",
            map_revision_id=self.rev_a,
            alignment_version="align_a",
        )
        self.store.set_active_map_alignment(
            map_name="demo_map",
            map_revision_id=self.rev_b,
            alignment_version="align_b",
        )

        active_a = self.store.get_map_alignment_config(
            map_name="demo_map",
            map_revision_id=self.rev_a,
            active_only=True,
        ) or {}
        active_b = self.store.get_map_alignment_config(
            map_name="demo_map",
            map_revision_id=self.rev_b,
            active_only=True,
        ) or {}

        self.assertEqual(active_a.get("alignment_version"), "align_a")
        self.assertTrue(bool(active_a.get("active")))
        self.assertEqual(active_b.get("alignment_version"), "align_b")
        self.assertTrue(bool(active_b.get("active")))

    def test_constraint_snapshots_are_scoped_by_revision(self):
        self.store.replace_map_constraints(
            map_id="map_shared_demo",
            map_revision_id=self.rev_a,
            map_md5="md5_shared_demo",
            no_go_areas=[
                {
                    "area_id": "area_a",
                    "name": "Area A",
                    "polygon": [[0.0, 0.0], [1.0, 0.0], [1.0, 1.0]],
                    "enabled": True,
                }
            ],
            virtual_walls=[],
            constraint_version="constraint_a",
        )
        self.store.replace_map_constraints(
            map_id="map_shared_demo",
            map_revision_id=self.rev_b,
            map_md5="md5_shared_demo",
            no_go_areas=[
                {
                    "area_id": "area_b",
                    "name": "Area B",
                    "polygon": [[2.0, 2.0], [3.0, 2.0], [3.0, 3.0]],
                    "enabled": True,
                }
            ],
            virtual_walls=[],
            constraint_version="constraint_b",
        )

        loaded_a = self.store.load_map_constraints(
            map_id="map_shared_demo",
            map_revision_id=self.rev_a,
            create_if_missing=False,
        )
        loaded_b = self.store.load_map_constraints(
            map_id="map_shared_demo",
            map_revision_id=self.rev_b,
            create_if_missing=False,
        )

        self.assertEqual(self.store.get_active_constraint_version("map_shared_demo", self.rev_a), "constraint_a")
        self.assertEqual(self.store.get_active_constraint_version("map_shared_demo", self.rev_b), "constraint_b")
        self.assertEqual(loaded_a.get("map_revision_id"), self.rev_a)
        self.assertEqual(loaded_a.get("constraint_version"), "constraint_a")
        self.assertEqual((loaded_a.get("no_go_areas") or [])[0]["area_id"], "area_a")
        self.assertEqual(loaded_b.get("map_revision_id"), self.rev_b)
        self.assertEqual(loaded_b.get("constraint_version"), "constraint_b")
        self.assertEqual((loaded_b.get("no_go_areas") or [])[0]["area_id"], "area_b")

    def test_zone_queries_ignore_map_version_when_revision_scope_is_explicit(self):
        block = SimpleNamespace(
            block_id=0,
            path_xyyaw=[(0.0, 0.0, 0.0), (1.0, 0.0, 0.0)],
            entry_xyyaw=(0.0, 0.0, 0.0),
            exit_xyyaw=(1.0, 0.0, 0.0),
        )
        plan_result = SimpleNamespace(
            blocks=[block],
            exec_order=[0],
            total_length_m=1.0,
        )
        plan_id = self.store.commit_zone_submission(
            map_name="demo_map",
            map_revision_id=self.rev_a,
            zone_id="zone_a",
            zone_version=1,
            frame_id="map",
            display_name="Zone A",
            plan_profile_name="cover_standard",
            params={"path_step_m": 0.1},
            robot_spec={},
            outer=[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0)],
            holes=[],
            plan_result=plan_result,
            map_id="map_shared_demo",
            map_md5="md5_shared_demo",
        )

        zone = self.store.get_zone_meta(
            "zone_a",
            map_revision_id=self.rev_a,
            map_version="md5_stale_other",
        )
        zones = self.store.list_zone_metas(
            map_revision_id=self.rev_a,
            map_version="md5_stale_other",
        )

        self.assertIsNotNone(zone)
        self.assertEqual(zone.get("map_revision_id"), self.rev_a)
        self.assertEqual(len(zones), 1)
        self.assertEqual(zones[0].get("zone_id"), "zone_a")

    def test_map_name_only_scope_prefers_active_verified_revision_over_pending_head(self):
        block = SimpleNamespace(
            block_id=0,
            path_xyyaw=[(0.0, 0.0, 0.0), (1.0, 0.0, 0.0)],
            entry_xyyaw=(0.0, 0.0, 0.0),
            exit_xyyaw=(1.0, 0.0, 0.0),
        )
        plan_result = SimpleNamespace(
            blocks=[block],
            exec_order=[0],
            total_length_m=1.0,
        )
        plan_id = self.store.commit_zone_submission(
            map_name="demo_map",
            map_revision_id=self.rev_a,
            zone_id="zone_a",
            zone_version=1,
            frame_id="map",
            display_name="Zone A",
            plan_profile_name="cover_standard",
            params={"path_step_m": 0.1},
            robot_spec={},
            outer=[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0)],
            holes=[],
            plan_result=plan_result,
            map_id="map_shared_demo",
            map_md5="md5_shared_demo",
        )
        self.store.set_active_map_revision(revision_id=self.rev_a, robot_id="local_robot")
        self.store.update_map_revision_meta(
            revision_id=self.rev_b,
            robot_id="local_robot",
            description="pending candidate",
        )
        self.store.upsert_map_revision(
            revision_id=self.rev_b,
            map_name="demo_map",
            display_name="demo_map",
            enabled=True,
            description="pending candidate",
            map_id="map_shared_demo",
            map_md5="md5_shared_demo",
            yaml_path="/tmp/demo_map.yaml",
            pgm_path="/tmp/demo_map.pgm",
            pbstream_path="/tmp/demo_map.pbstream",
            frame_id="map",
            resolution=0.05,
            origin=[0.0, 0.0, 0.0],
            lifecycle_status="saved_unverified",
            verification_status="pending",
            live_snapshot_md5="snapshot_b",
            verified_runtime_map_id="",
            verified_runtime_map_md5="",
            last_error_code="",
            last_error_msg="",
            source_job_id="",
            created_ts=float((self.store.resolve_map_revision(revision_id=self.rev_b) or {}).get("created_ts") or 0.0),
            verified_ts=0.0,
            activated_ts=0.0,
        )
        self.store.upsert_map_asset(
            map_name="demo_map",
            revision_id=self.rev_b,
            enabled=True,
            description="pending head",
            map_id="map_shared_demo",
            map_md5="md5_shared_demo",
            yaml_path="/tmp/demo_map.yaml",
            pgm_path="/tmp/demo_map.pgm",
            pbstream_path="/tmp/demo_map.pbstream",
            frame_id="map",
            resolution=0.05,
            origin=[0.0, 0.0, 0.0],
            lifecycle_status="saved_unverified",
            verification_status="pending",
            save_snapshot_md5="snapshot_b",
        )

        zone = self.store.get_zone_meta("zone_a", map_name="demo_map")
        active_plan_id = self.store.get_active_plan_id(
            "zone_a",
            plan_profile_name="cover_standard",
            map_name="demo_map",
        )

        self.assertIsNotNone(zone)
        self.assertEqual(zone.get("map_revision_id"), self.rev_a)
        self.assertEqual(active_plan_id, plan_id)


if __name__ == "__main__":
    unittest.main()

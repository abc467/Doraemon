#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import shutil
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
PKG_DIR = os.path.dirname(THIS_DIR)
SRC_DIR = os.path.join(PKG_DIR, "src")

if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from coverage_planner.plan_store.store import PlanStore


class PlanStoreMapCandidateFlowTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.mkdtemp(prefix="plan_store_candidate_")
        self._db_path = os.path.join(self._tmpdir, "planning.db")
        self.store = PlanStore(self._db_path)

    def tearDown(self):
        try:
            self.store.close()
        finally:
            shutil.rmtree(self._tmpdir, ignore_errors=True)

    def test_upsert_map_asset_round_trips_candidate_metadata(self):
        self.store.upsert_map_asset(
            map_name="demo_map",
            display_name="Demo Map",
            enabled=True,
            description="candidate map",
            map_id="",
            map_md5="",
            yaml_path="/tmp/demo_map.yaml",
            pgm_path="/tmp/demo_map.pgm",
            pbstream_path="/tmp/demo_map.pbstream",
            frame_id="map",
            resolution=0.05,
            origin=[0.0, 0.0, 0.0],
            lifecycle_status="saved_unverified",
            verification_status="pending",
            save_snapshot_md5="save_md5",
            last_error_code="",
            last_error_msg="",
            source_job_id="job_1",
        )

        asset = self.store.resolve_map_asset(map_name="demo_map") or {}

        self.assertEqual(asset.get("display_name"), "Demo Map")
        self.assertEqual(asset.get("lifecycle_status"), "saved_unverified")
        self.assertEqual(asset.get("verification_status"), "pending")
        self.assertEqual(asset.get("save_snapshot_md5"), "save_md5")
        self.assertEqual(asset.get("source_job_id"), "job_1")

    def test_pending_map_switch_round_trip(self):
        self.store.upsert_map_asset(
            map_name="stable_map",
            enabled=True,
            description="stable",
            map_id="map_stable",
            map_md5="stable_md5",
            yaml_path="/tmp/stable_map.yaml",
            pgm_path="/tmp/stable_map.pgm",
            pbstream_path="/tmp/stable_map.pbstream",
        )
        self.store.upsert_map_asset(
            map_name="demo_map",
            enabled=True,
            description="candidate",
            map_id="",
            map_md5="",
            yaml_path="/tmp/demo_map.yaml",
            pgm_path="/tmp/demo_map.pgm",
            pbstream_path="/tmp/demo_map.pbstream",
        )
        self.store.upsert_pending_map_switch(
            robot_id="local_robot",
            from_map_name="stable_map",
            target_map_name="demo_map",
            requested_activate=True,
            status="verifying",
        )
        self.store.upsert_pending_map_revision(
            robot_id="local_robot",
            from_revision_id=str((self.store.resolve_map_asset(map_name="stable_map") or {}).get("revision_id") or ""),
            target_revision_id=str((self.store.resolve_map_asset(map_name="demo_map") or {}).get("revision_id") or ""),
            requested_activate=True,
            status="verifying",
        )

        pending = self.store.get_pending_map_switch(robot_id="local_robot") or {}
        self.assertEqual(pending.get("from_map_name"), "stable_map")
        self.assertEqual(pending.get("target_map_name"), "demo_map")
        self.assertTrue(pending.get("requested_activate"))
        self.assertEqual(pending.get("status"), "verifying")
        self.assertTrue(str(pending.get("from_revision_id") or "").startswith("rev_"))
        self.assertTrue(str(pending.get("target_revision_id") or "").startswith("rev_"))

        self.store.clear_pending_map_switch(robot_id="local_robot")
        self.assertIsNone(self.store.get_pending_map_switch(robot_id="local_robot"))
        self.assertIsNone(self.store.get_pending_map_revision(robot_id="local_robot"))

    def test_mark_map_asset_verification_result_promotes_candidate_identity(self):
        self.store.upsert_map_asset(
            map_name="demo_map",
            enabled=True,
            description="candidate map",
            map_id="",
            map_md5="",
            yaml_path="/tmp/demo_map.yaml",
            pgm_path="/tmp/demo_map.pgm",
            pbstream_path="/tmp/demo_map.pbstream",
            frame_id="map",
            resolution=0.05,
            origin=[0.0, 0.0, 0.0],
            lifecycle_status="saved_unverified",
            verification_status="pending",
            save_snapshot_md5="save_md5",
        )

        updated = self.store.mark_map_asset_verification_result(
            map_name="demo_map",
            verification_status="verified",
            lifecycle_status="available",
            runtime_map_md5="abcdef1234567890",
            promote_canonical_identity=True,
            error_code="",
            error_msg="",
        )

        self.assertEqual(updated.get("map_id"), "map_abcdef12")
        self.assertEqual(updated.get("map_md5"), "abcdef1234567890")
        self.assertEqual(updated.get("verified_runtime_map_md5"), "abcdef1234567890")
        self.assertEqual(updated.get("verification_status"), "verified")
        self.assertEqual(updated.get("lifecycle_status"), "available")
        self.assertEqual(updated.get("last_error_code"), "")
        self.assertEqual(updated.get("last_error_msg"), "")

    def test_asset_revision_is_created_and_active_pointer_tracks_revision(self):
        self.store.upsert_map_asset(
            map_name="demo_map",
            enabled=True,
            description="stable map",
            map_id="map_demo_01",
            map_md5="demo_md5_01",
            yaml_path="/tmp/demo_map.yaml",
            pgm_path="/tmp/demo_map.pgm",
            pbstream_path="/tmp/demo_map.pbstream",
            lifecycle_status="available",
            verification_status="verified",
            save_snapshot_md5="snapshot_md5",
        )

        asset = self.store.resolve_map_asset(map_name="demo_map") or {}
        revision_id = str(asset.get("revision_id") or "")
        revision = self.store.resolve_map_revision(revision_id=revision_id) or {}

        self.assertTrue(revision_id.startswith("rev_"))
        self.assertEqual(revision.get("map_name"), "demo_map")
        self.assertEqual(revision.get("live_snapshot_md5"), "snapshot_md5")
        self.assertEqual(revision.get("verification_status"), "verified")

        self.store.set_active_map_revision(revision_id=revision_id, robot_id="local_robot")
        active_asset = self.store.get_active_map(robot_id="local_robot") or {}
        active_revision = self.store.get_active_map_revision(robot_id="local_robot") or {}

        self.assertEqual(active_asset.get("revision_id"), revision_id)
        self.assertEqual(active_revision.get("revision_id"), revision_id)
        self.assertEqual(active_revision.get("map_name"), "demo_map")

    def test_active_map_returns_active_revision_metadata_not_latest_head_metadata(self):
        self.store.upsert_map_asset(
            map_name="demo_map",
            enabled=True,
            description="revision one",
            map_id="map_demo_v1",
            map_md5="demo_md5_v1",
            yaml_path="/tmp/demo_map_v1.yaml",
            pgm_path="/tmp/demo_map_v1.pgm",
            pbstream_path="/tmp/demo_map_v1.pbstream",
            lifecycle_status="available",
            verification_status="verified",
            save_snapshot_md5="snapshot_v1",
        )
        first_revision_id = str((self.store.resolve_map_asset(map_name="demo_map") or {}).get("revision_id") or "")
        self.assertTrue(first_revision_id.startswith("rev_"))

        self.store.upsert_map_asset(
            map_name="demo_map",
            enabled=True,
            description="revision two",
            map_id="map_demo_v2",
            map_md5="demo_md5_v2",
            yaml_path="/tmp/demo_map_v2.yaml",
            pgm_path="/tmp/demo_map_v2.pgm",
            pbstream_path="/tmp/demo_map_v2.pbstream",
            revision_id="rev_demo_map_v2",
            lifecycle_status="saved_unverified",
            verification_status="pending",
            save_snapshot_md5="snapshot_v2",
        )

        self.store.set_active_map_revision(revision_id=first_revision_id, robot_id="local_robot")
        active = self.store.get_active_map(robot_id="local_robot") or {}

        self.assertEqual(active.get("revision_id"), first_revision_id)
        self.assertEqual(active.get("map_md5"), "demo_md5_v1")
        self.assertEqual(active.get("pbstream_path"), "/tmp/demo_map_v1.pbstream")
        self.assertEqual(active.get("yaml_path"), "/tmp/demo_map_v1.yaml")
        self.assertEqual(active.get("save_snapshot_md5"), "snapshot_v1")

    def test_set_active_map_prefers_verified_revision_over_pending_head(self):
        self.store.upsert_map_asset(
            map_name="demo_map",
            enabled=True,
            description="verified revision",
            map_id="map_demo_v1",
            map_md5="demo_md5_v1",
            yaml_path="/tmp/demo_map_v1.yaml",
            pgm_path="/tmp/demo_map_v1.pgm",
            pbstream_path="/tmp/demo_map_v1.pbstream",
            lifecycle_status="available",
            verification_status="verified",
            save_snapshot_md5="snapshot_v1",
        )
        stable_revision_id = str((self.store.resolve_map_asset(map_name="demo_map") or {}).get("revision_id") or "")

        self.store.upsert_map_asset(
            map_name="demo_map",
            enabled=True,
            description="pending head",
            map_id="map_demo_v2",
            map_md5="demo_md5_v2",
            yaml_path="/tmp/demo_map_v2.yaml",
            pgm_path="/tmp/demo_map_v2.pgm",
            pbstream_path="/tmp/demo_map_v2.pbstream",
            revision_id="rev_demo_map_v2",
            lifecycle_status="saved_unverified",
            verification_status="pending",
            save_snapshot_md5="snapshot_v2",
        )

        self.store.set_active_map(map_name="demo_map", robot_id="local_robot")
        active = self.store.get_active_map(robot_id="local_robot") or {}

        self.assertEqual(active.get("revision_id"), stable_revision_id)
        self.assertEqual(active.get("map_md5"), "demo_md5_v1")

    def test_set_active_map_revision_rejects_unverified_revision(self):
        self.store.upsert_map_asset(
            map_name="demo_map",
            enabled=True,
            description="pending revision",
            map_id="map_demo_v2",
            map_md5="demo_md5_v2",
            yaml_path="/tmp/demo_map_v2.yaml",
            pgm_path="/tmp/demo_map_v2.pgm",
            pbstream_path="/tmp/demo_map_v2.pbstream",
            revision_id="rev_demo_map_v2",
            lifecycle_status="saved_unverified",
            verification_status="pending",
            save_snapshot_md5="snapshot_v2",
        )

        with self.assertRaisesRegex(ValueError, "pending verification"):
            self.store.set_active_map_revision(revision_id="rev_demo_map_v2", robot_id="local_robot")

    def test_register_map_asset_set_active_commits_by_revision(self):
        with mock.patch.object(self.store, "set_active_map_revision") as set_active_map_revision, mock.patch.object(
            self.store, "set_active_map"
        ) as set_active_map:
            revision_id = self.store.register_map_asset(
                map_name="demo_map",
                enabled=True,
                description="revision two",
                map_id="map_demo_v2",
                map_md5="demo_md5_v2",
                yaml_path="/tmp/demo_map_v2.yaml",
                pgm_path="/tmp/demo_map_v2.pgm",
                pbstream_path="/tmp/demo_map_v2.pbstream",
                revision_id="rev_demo_map_v2",
                lifecycle_status="available",
                verification_status="verified",
                robot_id="local_robot",
                set_active=True,
            )

        self.assertEqual(revision_id, "rev_demo_map_v2")
        set_active_map_revision.assert_called_once_with(revision_id="rev_demo_map_v2", robot_id="local_robot")
        set_active_map.assert_not_called()

    def test_update_map_revision_meta_non_head_revision_does_not_mutate_head_asset(self):
        self.store.upsert_map_asset(
            map_name="demo_map",
            enabled=True,
            description="stable active",
            map_id="map_demo_v1",
            map_md5="demo_md5_v1",
            yaml_path="/tmp/demo_map_v1.yaml",
            pgm_path="/tmp/demo_map_v1.pgm",
            pbstream_path="/tmp/demo_map_v1.pbstream",
            lifecycle_status="available",
            verification_status="verified",
        )
        stable_revision_id = str((self.store.resolve_map_asset(map_name="demo_map") or {}).get("revision_id") or "")
        self.store.set_active_map_revision(revision_id=stable_revision_id, robot_id="local_robot")

        self.store.upsert_map_asset(
            map_name="demo_map",
            enabled=True,
            description="candidate head",
            map_id="",
            map_md5="",
            yaml_path="/tmp/demo_map_v2.yaml",
            pgm_path="/tmp/demo_map_v2.pgm",
            pbstream_path="/tmp/demo_map_v2.pbstream",
            revision_id="rev_demo_head",
            lifecycle_status="saved_unverified",
            verification_status="pending",
        )

        updated = self.store.update_map_revision_meta(
            revision_id=stable_revision_id,
            robot_id="local_robot",
            description="stable active updated",
        )

        stable_revision = self.store.resolve_map_asset(revision_id=stable_revision_id, robot_id="local_robot") or {}
        head_asset = self.store.resolve_map_asset(map_name="demo_map", robot_id="local_robot") or {}
        self.assertEqual(updated.get("revision_id"), stable_revision_id)
        self.assertEqual(stable_revision.get("description"), "stable active updated")
        self.assertEqual(head_asset.get("revision_id"), "rev_demo_head")
        self.assertEqual(head_asset.get("description"), "candidate head")

    def test_update_map_revision_meta_syncs_head_asset_when_target_is_head(self):
        self.store.upsert_map_asset(
            map_name="demo_map",
            enabled=True,
            description="candidate head",
            map_id="",
            map_md5="",
            yaml_path="/tmp/demo_map_v2.yaml",
            pgm_path="/tmp/demo_map_v2.pgm",
            pbstream_path="/tmp/demo_map_v2.pbstream",
            revision_id="rev_demo_head",
            lifecycle_status="saved_unverified",
            verification_status="pending",
        )

        updated = self.store.update_map_revision_meta(
            revision_id="rev_demo_head",
            robot_id="local_robot",
            description="candidate disabled",
            enabled=False,
        )

        head_asset = self.store.resolve_map_asset(map_name="demo_map", robot_id="local_robot") or {}
        self.assertEqual(updated.get("revision_id"), "rev_demo_head")
        self.assertEqual(updated.get("description"), "candidate disabled")
        self.assertFalse(bool(updated.get("enabled", True)))
        self.assertEqual(head_asset.get("revision_id"), "rev_demo_head")
        self.assertEqual(head_asset.get("description"), "candidate disabled")
        self.assertFalse(bool(head_asset.get("enabled", True)))

    def test_mark_map_asset_failure_preserves_verified_asset_status(self):
        self.store.upsert_map_asset(
            map_name="stable_map",
            enabled=True,
            description="stable map",
            map_id="map_stable01",
            map_md5="stable0123456789",
            yaml_path="/tmp/stable_map.yaml",
            pgm_path="/tmp/stable_map.pgm",
            pbstream_path="/tmp/stable_map.pbstream",
            frame_id="map",
            resolution=0.05,
            origin=[0.0, 0.0, 0.0],
            lifecycle_status="available",
            verification_status="verified",
        )

        updated = self.store.mark_map_asset_verification_result(
            map_name="stable_map",
            error_code="restart_localization_failed",
            error_msg="runtime mismatch",
        )

        self.assertEqual(updated.get("verification_status"), "verified")
        self.assertEqual(updated.get("lifecycle_status"), "available")
        self.assertEqual(updated.get("last_error_code"), "restart_localization_failed")
        self.assertEqual(updated.get("last_error_msg"), "runtime mismatch")

    def test_mark_map_asset_verification_result_targets_requested_revision(self):
        self.store.upsert_map_asset(
            map_name="demo_map",
            enabled=True,
            description="revision one",
            map_id="",
            map_md5="",
            yaml_path="/tmp/demo_map_v1.yaml",
            pgm_path="/tmp/demo_map_v1.pgm",
            pbstream_path="/tmp/demo_map_v1.pbstream",
            lifecycle_status="saved_unverified",
            verification_status="pending",
            save_snapshot_md5="snapshot_v1",
        )
        first_revision_id = str((self.store.resolve_map_asset(map_name="demo_map") or {}).get("revision_id") or "")

        self.store.upsert_map_asset(
            map_name="demo_map",
            enabled=True,
            description="revision two",
            map_id="",
            map_md5="",
            yaml_path="/tmp/demo_map_v2.yaml",
            pgm_path="/tmp/demo_map_v2.pgm",
            pbstream_path="/tmp/demo_map_v2.pbstream",
            revision_id="rev_demo_map_v2",
            lifecycle_status="saved_unverified",
            verification_status="pending",
            save_snapshot_md5="snapshot_v2",
        )

        updated = self.store.mark_map_asset_verification_result(
            map_name="demo_map",
            revision_id=first_revision_id,
            verification_status="verified",
            lifecycle_status="available",
            runtime_map_md5="abcdef1234567890",
            promote_canonical_identity=True,
            error_code="",
            error_msg="",
        )

        first_revision = self.store.resolve_map_asset(revision_id=first_revision_id) or {}
        second_revision = self.store.resolve_map_asset(revision_id="rev_demo_map_v2") or {}
        head_asset = self.store.resolve_map_asset(map_name="demo_map") or {}

        self.assertEqual(updated.get("revision_id"), first_revision_id)
        self.assertEqual(first_revision.get("map_md5"), "abcdef1234567890")
        self.assertEqual(first_revision.get("verification_status"), "verified")
        self.assertEqual(second_revision.get("map_md5"), "")
        self.assertEqual(second_revision.get("verification_status"), "pending")
        self.assertEqual(head_asset.get("revision_id"), "rev_demo_map_v2")
        self.assertEqual(head_asset.get("verification_status"), "pending")

    def test_zone_and_plan_bind_to_revision_scope(self):
        self.store.upsert_map_asset(
            map_name="demo_map",
            enabled=True,
            description="revision one",
            map_id="map_demo_v1",
            map_md5="demo_md5_v1",
            yaml_path="/tmp/demo_map_v1.yaml",
            pgm_path="/tmp/demo_map_v1.pgm",
            pbstream_path="/tmp/demo_map_v1.pbstream",
            lifecycle_status="available",
            verification_status="verified",
        )
        first_revision_id = str((self.store.resolve_map_asset(map_name="demo_map") or {}).get("revision_id") or "")
        self.assertTrue(first_revision_id.startswith("rev_"))

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
            map_revision_id=first_revision_id,
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
            map_id="map_demo_v1",
            map_md5="demo_md5_v1",
        )

        second_revision_id = "rev_demo_map_v2"
        self.store.upsert_map_asset(
            map_name="demo_map",
            enabled=True,
            description="revision two",
            map_id="map_demo_v2",
            map_md5="demo_md5_v2",
            yaml_path="/tmp/demo_map_v2.yaml",
            pgm_path="/tmp/demo_map_v2.pgm",
            pbstream_path="/tmp/demo_map_v2.pbstream",
            lifecycle_status="available",
            verification_status="verified",
            revision_id=second_revision_id,
        )

        zone_v1 = self.store.get_zone_meta("zone_a", map_revision_id=first_revision_id)
        active_plan_v1 = self.store.get_active_plan_id("zone_a", "cover_standard", map_revision_id=first_revision_id)
        active_plan_current = self.store.get_active_plan_id("zone_a", "cover_standard", map_name="demo_map")
        plan_meta = self.store.load_plan_meta(plan_id)

        self.assertIsNotNone(zone_v1)
        self.assertEqual(zone_v1.get("map_revision_id"), first_revision_id)
        self.assertEqual(active_plan_v1, plan_id)
        self.assertIsNone(active_plan_current)
        self.assertEqual(plan_meta.get("map_revision_id"), first_revision_id)


if __name__ == "__main__":
    unittest.main()

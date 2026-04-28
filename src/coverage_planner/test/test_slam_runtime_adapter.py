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

from coverage_planner.slam_workflow.runtime_adapter import CartographerRuntimeAdapter, SlamWorkflowRuntimeError


class _FakeTransport:
    def __init__(self):
        self.stop_runtime_calls = 0

    def stop_runtime(self):
        self.stop_runtime_calls += 1

    def __getattr__(self, name):
        raise AssertionError("unexpected transport call: %s" % str(name or ""))


class _FakePlanStore:
    def __init__(self):
        self.set_active_calls = []
        self.set_active_revision_calls = []
        self.rebind_calls = []
        self.selected_map = {}
        self.active_revision = {}
        self.pending_switch = None
        self.pending_revision = None
        self.pending_switch_updates = []
        self.pending_switch_clears = []
        self.mark_verification_calls = []
        self.call_log = []

    def generate_map_revision_id(self, map_name: str) -> str:
        del map_name
        return "rev_saved_demo_01"

    def set_active_map(self, *, map_name: str, robot_id: str = "local_robot"):
        self.call_log.append(("set_active_map", robot_id, map_name))
        self.set_active_calls.append((robot_id, map_name))

    def set_active_map_revision(self, *, revision_id: str, robot_id: str = "local_robot"):
        self.call_log.append(("set_active_map_revision", robot_id, revision_id))
        self.set_active_revision_calls.append((robot_id, revision_id))

    def register_map_asset(self, **kwargs):
        self.register_map_asset_kwargs = dict(kwargs)
        revision_id = str(kwargs.get("revision_id") or "rev_saved_demo_01").strip()
        self.selected_map = {
            "map_name": str(kwargs.get("map_name") or ""),
            "revision_id": revision_id,
            "map_id": str(kwargs.get("map_id") or ""),
            "map_md5": str(kwargs.get("map_md5") or ""),
            "pbstream_path": str(kwargs.get("pbstream_path") or ""),
            "enabled": bool(kwargs.get("enabled", True)),
        }
        return revision_id

    def rebind_map_identity(self, **kwargs):
        self.rebind_calls.append(dict(kwargs))
        return dict(kwargs)

    def get_active_map(self, *, robot_id: str):
        del robot_id
        return dict(self.selected_map or {})

    def get_active_map_revision(self, *, robot_id: str):
        del robot_id
        return dict(self.active_revision or {})

    def ensure_map_asset_revision(self, *, map_name: str, robot_id: str = "local_robot") -> str:
        del robot_id
        if self.selected_map and str(self.selected_map.get("map_name") or "") == str(map_name or ""):
            return str(self.selected_map.get("revision_id") or "")
        return ""

    def upsert_pending_map_switch(self, **kwargs):
        self.call_log.append(("upsert_pending_map_switch", dict(kwargs)))
        self.pending_switch = dict(kwargs)
        self.pending_switch_updates.append(dict(kwargs))

    def upsert_pending_map_revision(self, **kwargs):
        self.call_log.append(("upsert_pending_map_revision", dict(kwargs)))
        self.pending_revision = dict(kwargs)

    def get_pending_map_switch(self, *, robot_id: str):
        del robot_id
        return dict(self.pending_switch or {})

    def get_pending_map_revision(self, *, robot_id: str):
        del robot_id
        return dict(self.pending_revision or {})

    def clear_pending_map_switch(self, *, robot_id: str):
        self.call_log.append(("clear_pending_map_switch", robot_id))
        self.pending_switch_clears.append(str(robot_id or ""))
        self.pending_switch = None
        self.pending_revision = None

    def mark_map_asset_verification_result(self, **kwargs):
        self.call_log.append(("mark_map_asset_verification_result", dict(kwargs)))
        self.mark_verification_calls.append(dict(kwargs))
        runtime_map_md5 = str(kwargs.get("runtime_map_md5") or "").strip()
        runtime_map_id = str(kwargs.get("runtime_map_id") or "").strip()
        return {
            "map_name": str(kwargs.get("map_name") or ""),
            "revision_id": str(kwargs.get("revision_id") or ""),
            "map_id": runtime_map_id,
            "map_md5": runtime_map_md5,
            "verification_status": str(kwargs.get("verification_status") or ""),
            "lifecycle_status": str(kwargs.get("lifecycle_status") or ""),
        }

    def resolve_map_asset(self, *, map_name: str = "", revision_id: str = "", robot_id: str = "local_robot"):
        del robot_id
        if revision_id:
            asset = dict(self.selected_map or {})
            if asset and str(asset.get("revision_id") or "") == str(revision_id or ""):
                return asset
            return None
        asset = dict(self.selected_map or {})
        if asset and str(asset.get("map_name") or "") == str(map_name or ""):
            return asset
        return None


class _FakeServiceApi:
    @staticmethod
    def response(**kwargs):
        return dict(kwargs)


class _FakeAssetHelper:
    def __init__(self, backend):
        self._backend = backend

    def resolve_asset(
        self,
        *,
        robot_id: str,
        map_name: str,
        map_revision_id: str = "",
        prefer_active_revision: bool = False,
    ):
        if self._backend.asset is not None:
            explicit_asset = dict(self._backend.asset)
            explicit_revision_id = str(explicit_asset.get("revision_id") or "").strip()
            explicit_map_name = str(explicit_asset.get("map_name") or "").strip()
            if (not map_revision_id or explicit_revision_id == str(map_revision_id or "").strip()) and (
                not map_name or explicit_map_name == str(map_name or "").strip()
            ):
                return explicit_asset
        if bool(prefer_active_revision):
            active_asset = self._backend._plan_store.get_active_map(robot_id=robot_id) or {}
            active_map_name = str(active_asset.get("map_name") or "").strip()
            active_revision_id = str(active_asset.get("revision_id") or "").strip()
            if active_revision_id and active_map_name and active_map_name == str(map_name or "").strip():
                asset = self._backend._plan_store.resolve_map_asset(
                    revision_id=active_revision_id,
                    robot_id=robot_id,
                )
                if asset is not None:
                    return asset
                return active_asset or None
        asset = dict(getattr(self._backend._plan_store, "selected_map", {}) or {})
        if map_revision_id and asset and str(asset.get("revision_id") or "") != str(map_revision_id or ""):
            return None
        if map_name and asset and str(asset.get("map_name") or "") != str(map_name or ""):
            return None
        return asset or None

    def target_paths(self, map_name: str, revision_id: str = ""):
        base_dir = self._backend.maps_root
        if str(revision_id or "").strip():
            base_dir = os.path.join(
                self._backend.maps_root,
                "revisions",
                str(map_name or "").strip(),
                str(revision_id or "").strip(),
            )
        base = os.path.join(base_dir, map_name)
        return {
            "pbstream_path": base + ".pbstream",
            "yaml_path": base + ".yaml",
            "pgm_path": base + ".pgm",
        }

    def cleanup_paths(self, *paths: str):
        self.cleaned = list(paths)

    def ensure_repo_map_link(self, asset):
        return str(asset.get("pbstream_path") or "")


class _FakeRuntimeState:
    def __init__(self):
        self.localization_calls = []

    def set_runtime_mode(self, **kwargs):
        self.mode_kwargs = dict(kwargs)

    def publish_runtime_snapshot(self, **kwargs):
        self.snapshot_kwargs = dict(kwargs)

    def set_localization_state(self, **kwargs):
        self.localization_kwargs = dict(kwargs)
        self.localization_calls.append(dict(kwargs))


class _FakeRuntimeContext:
    def __init__(self):
        self._map_md5 = ""
        self._revision_id = ""

    def reset_runtime_observations(self):
        self.reset_called = True

    def service_available(self, *_args, **_kwargs):
        return False

    def publish_initial_pose(self, **kwargs):
        self.initial_pose_kwargs = dict(kwargs)

    def runtime_param(self, key: str) -> str:
        return "/cartographer/runtime/" + str(key or "").strip()

    def runtime_map_md5(self) -> str:
        return str(self._map_md5 or "")

    def runtime_map_revision_id(self) -> str:
        return str(self._revision_id or "")

    def tracked_pose_fresh_since(self, **_kwargs) -> bool:
        return False

    def tracked_pose_stable_since(self, **_kwargs) -> bool:
        return False

    def runtime_map_ready_since(self, **_kwargs) -> bool:
        return False

    def runtime_map_matches(self, _target_md5: str, target_revision_id: str = "") -> bool:
        if str(target_revision_id or "").strip():
            return str(self._revision_id or "").strip() == str(target_revision_id or "").strip()
        return False


class _FakeBackend:
    def __init__(self, tmpdir: str):
        self.maps_root = tmpdir
        self.map_topic = "/map"
        self.command_timeout_s = 30.0
        self.ready_timeout_s = 0.5
        self.tf_parent_frame = "map"
        self.tf_child_frame = "odom"
        self.tf_poll_timeout_s = 0.1
        self.tracked_pose_fresh_timeout_s = 2.0
        self.localization_stable_window_s = 1.0
        self.localization_stable_max_pose_jump_m = 0.25
        self.localization_stable_max_yaw_jump_rad = 0.35
        self.localization_degraded_timeout_s = 6.0
        self.localization_include_unfrozen_submaps = False
        self.mapping_include_unfrozen_submaps = True
        self.mapping_config_entry = "slam"
        self._plan_store = _FakePlanStore()
        self._service_api = _FakeServiceApi()
        self._asset_helper = _FakeAssetHelper(self)
        self._runtime_state = _FakeRuntimeState()
        self._runtime_context = _FakeRuntimeContext()
        self._tf_buffer = SimpleNamespace(can_transform=lambda *_args, **_kwargs: True)
        self.allow_identity_rebind_on_localize = True
        self.asset = None


class SlamRuntimeAdapterTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.mkdtemp(prefix="slam_runtime_adapter_test_")
        self.backend = _FakeBackend(self._tmpdir)
        self.adapter = CartographerRuntimeAdapter(self.backend, transport=_FakeTransport())

    def tearDown(self):
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def test_restart_localization_returns_not_found_when_asset_missing(self):
        result = self.adapter.restart_localization(
            robot_id="local_robot",
            map_name="missing_map",
            operation=2,
        )

        self.assertFalse(result["success"])
        self.assertEqual(result["error_code"], "map_asset_not_found")

    def test_restart_localization_returns_disabled_when_asset_disabled(self):
        self.backend.asset = {
            "map_name": "demo_map",
            "pbstream_path": "/tmp/demo_map.pbstream",
            "enabled": False,
        }

        result = self.adapter.restart_localization(
            robot_id="local_robot",
            map_name="demo_map",
            operation=2,
        )

        self.assertFalse(result["success"])
        self.assertEqual(result["error_code"], "map_asset_disabled")

    def test_restart_localization_rejects_map_revision_name_mismatch(self):
        def _raise_scope_mismatch(**kwargs):
            del kwargs
            raise ValueError("map revision does not match selected map")

        self.backend._asset_helper.resolve_asset = _raise_scope_mismatch

        result = self.adapter.restart_localization(
            robot_id="local_robot",
            map_name="other_map",
            map_revision_id="rev_demo_01",
            operation=2,
        )

        self.assertFalse(result["success"])
        self.assertEqual(result["error_code"], "map_scope_mismatch")

    def test_stop_mapping_without_active_map_stops_runtime_successfully(self):
        result = self.adapter.stop_mapping(
            robot_id="local_robot",
            map_name="",
            operation=5,
        )

        self.assertTrue(result["success"])
        self.assertEqual(result["current_mode"], "localization")
        self.assertEqual(result["localization_state"], "not_localized")
        self.assertEqual(result["map_name"], "")
        self.assertEqual(self.adapter._transport.stop_runtime_calls, 1)
        self.assertEqual(self.backend._runtime_state.mode_kwargs["mode"], "localization")
        self.assertEqual(self.backend._runtime_state.localization_kwargs["state"], "not_localized")

    @mock.patch("coverage_planner.slam_workflow.runtime_adapter.rospy.set_param", side_effect=lambda *_args, **_kwargs: None)
    def test_stop_mapping_succeeds_when_relocalization_needs_manual_assist(self, _set_param):
        pbstream_path = os.path.join(self._tmpdir, "demo_map.pbstream")
        with open(pbstream_path, "w", encoding="utf-8") as fh:
            fh.write("pbstream")
        self.backend.asset = {
            "map_name": "demo_map",
            "revision_id": "rev_demo_01",
            "pbstream_path": pbstream_path,
            "enabled": True,
            "verification_status": "verified",
        }
        self.backend._plan_store.selected_map = dict(self.backend.asset)
        self.adapter._transport.start_runtime_processes = lambda **_kwargs: (321, "runtime_started")
        self.adapter.run_localization_sequence = mock.Mock(
            side_effect=SlamWorkflowRuntimeError(
                "relocalize_low_constraint_score",
                "try_global_relocate failed",
                manual_assist_required=True,
            )
        )

        result = self.adapter.stop_mapping(
            robot_id="local_robot",
            map_name="demo_map",
            map_revision_id="rev_demo_01",
            operation=5,
        )

        self.assertTrue(result["success"])
        self.assertEqual(result["error_code"], "")
        self.assertEqual(result["current_mode"], "localization")
        self.assertEqual(result["localization_state"], "manual_assist_required")
        self.assertIn("mapping stopped", result["message"])
        self.assertIn("manual assist", result["message"])

    def test_stop_mapping_handles_ros_response_object_from_restart_localization(self):
        self.adapter.restart_localization = mock.Mock(
            return_value=SimpleNamespace(
                success=False,
                error_code="relocalize_low_constraint_score",
                message="try_global_relocate failed",
                map_name="demo_map",
                map_revision_id="rev_demo_01",
                localization_state="manual_assist_required",
                current_mode="localization",
            )
        )

        result = self.adapter.stop_mapping(
            robot_id="local_robot",
            map_name="demo_map",
            map_revision_id="rev_demo_01",
            operation=5,
        )

        self.assertTrue(result["success"])
        self.assertEqual(result["error_code"], "")
        self.assertEqual(result["current_mode"], "localization")
        self.assertEqual(result["map_revision_id"], "rev_demo_01")
        self.assertIn("mapping stopped", result["message"])

    def test_save_mapping_allows_same_name_existing_asset_and_creates_candidate_revision(self):
        self.backend.asset = {
            "map_name": "demo_map",
            "revision_id": "rev_demo_old_01",
            "pbstream_path": "/tmp/demo_map.pbstream",
            "enabled": True,
        }
        def _save_pbstream(path, include_unfinished_submaps=True):
            del include_unfinished_submaps
            with open(path, "w", encoding="utf-8") as fh:
                fh.write("pbstream")
            return True, "ok"

        self.adapter._transport.save_pbstream = _save_pbstream
        occ = SimpleNamespace(
            header=SimpleNamespace(frame_id="map"),
            info=SimpleNamespace(resolution=0.05),
        )

        def _write_occupancy(_occ, out_dir, base_name):
            yaml_path = os.path.join(out_dir, base_name + ".yaml")
            pgm_path = os.path.join(out_dir, base_name + ".pgm")
            with open(yaml_path, "w", encoding="utf-8") as fh:
                fh.write("image: demo_map.pgm\n")
            with open(pgm_path, "w", encoding="utf-8") as fh:
                fh.write("P2\n1 1\n100\n0\n")
            return pgm_path, yaml_path

        with mock.patch(
            "coverage_planner.slam_workflow.runtime_adapter.rospy.wait_for_message",
            return_value=occ,
        ), mock.patch(
            "coverage_planner.slam_workflow.runtime_adapter.compute_occupancy_grid_md5",
            return_value="abc123456789",
        ), mock.patch(
            "coverage_planner.slam_workflow.runtime_adapter.write_occupancy_to_yaml_pgm",
            side_effect=_write_occupancy,
        ), mock.patch(
            "coverage_planner.slam_workflow.runtime_adapter.origin_to_jsonable",
            return_value={"x": 0.0, "y": 0.0, "yaw": 0.0},
        ):
            result = self.adapter.save_mapping(
                robot_id="local_robot",
                map_name="demo_map",
                description="",
                set_active=False,
                operation=4,
            )

        self.assertTrue(result["success"])
        self.assertEqual(result["map_revision_id"], "rev_saved_demo_01")
        self.assertEqual(self.backend._plan_store.register_map_asset_kwargs["revision_id"], "rev_saved_demo_01")
        self.assertIn("/revisions/demo_map/rev_saved_demo_01/", self.backend._plan_store.register_map_asset_kwargs["pbstream_path"])

    def test_save_mapping_rejects_existing_target_path(self):
        target_path = os.path.join(
            self._tmpdir,
            "revisions",
            "demo_map",
            "rev_saved_demo_01",
            "demo_map.pbstream",
        )
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        with open(target_path, "w", encoding="utf-8") as fh:
            fh.write("existing")

        result = self.adapter.save_mapping(
            robot_id="local_robot",
            map_name="demo_map",
            description="",
            set_active=True,
            operation=4,
        )

        self.assertFalse(result["success"])
        self.assertEqual(result["error_code"], "asset_path_exists")

    def test_save_mapping_verifies_saved_map_when_followup_enabled(self):
        def _save_pbstream(path, include_unfinished_submaps=True):
            with open(path, "w", encoding="utf-8") as fh:
                fh.write("pbstream")
            return True, "ok"

        self.adapter._transport.save_pbstream = _save_pbstream
        verify_calls = []
        activate_calls = []

        def _verify_map_revision(**kwargs):
            verify_calls.append(dict(kwargs))
            return SimpleNamespace(
                success=True,
                message="verified",
                error_code="",
                localization_state="localized",
                current_mode="localization",
            )

        def _activate_map_revision(**kwargs):
            activate_calls.append(dict(kwargs))
            return SimpleNamespace(
                success=True,
                message="activated",
                error_code="",
                localization_state="localized",
                current_mode="localization",
            )

        self.adapter.verify_map_revision = _verify_map_revision
        self.adapter.activate_map_revision = _activate_map_revision

        occ = SimpleNamespace(
            header=SimpleNamespace(frame_id="map"),
            info=SimpleNamespace(resolution=0.05),
        )

        def _write_occupancy(_occ, maps_root, base_name):
            yaml_path = os.path.join(maps_root, base_name + ".yaml")
            pgm_path = os.path.join(maps_root, base_name + ".pgm")
            with open(yaml_path, "w", encoding="utf-8") as fh:
                fh.write("image: demo_map.pgm\n")
            with open(pgm_path, "w", encoding="utf-8") as fh:
                fh.write("P2\n1 1\n100\n0\n")
            return pgm_path, yaml_path

        with mock.patch(
            "coverage_planner.slam_workflow.runtime_adapter.rospy.wait_for_message",
            return_value=occ,
        ), mock.patch(
            "coverage_planner.slam_workflow.runtime_adapter.compute_occupancy_grid_md5",
            return_value="abc123456789",
        ), mock.patch(
            "coverage_planner.slam_workflow.runtime_adapter.write_occupancy_to_yaml_pgm",
            side_effect=_write_occupancy,
        ), mock.patch(
            "coverage_planner.slam_workflow.runtime_adapter.origin_to_jsonable",
            return_value={"x": 0.0, "y": 0.0, "yaw": 0.0},
        ):
            result = self.adapter.save_mapping(
                robot_id="local_robot",
                map_name="demo_map",
                description="acceptance",
                set_active=False,
                operation=4,
                switch_to_localization_after_save=True,
                relocalize_after_switch=True,
            )

        self.assertTrue(result["success"])
        self.assertEqual(result["current_mode"], "localization")
        self.assertEqual(result["map_revision_id"], "rev_saved_demo_01")
        self.assertEqual(len(verify_calls), 1)
        self.assertEqual(len(activate_calls), 0)
        self.assertEqual(verify_calls[0]["map_name"], "demo_map")
        self.assertEqual(verify_calls[0]["map_revision_id"], "rev_saved_demo_01")
        self.assertEqual(verify_calls[0]["operation"], 21)

    def test_save_mapping_verifies_then_activates_saved_map_when_requested(self):
        def _save_pbstream(path, include_unfinished_submaps=True):
            del include_unfinished_submaps
            with open(path, "w", encoding="utf-8") as fh:
                fh.write("pbstream")
            return True, "ok"

        self.adapter._transport.save_pbstream = _save_pbstream
        verify_calls = []
        activate_calls = []

        self.adapter.verify_map_revision = lambda **kwargs: (
            verify_calls.append(dict(kwargs)) or SimpleNamespace(
                success=True,
                message="verified",
                error_code="",
                localization_state="localized",
                current_mode="localization",
            )
        )
        self.adapter.activate_map_revision = lambda **kwargs: (
            activate_calls.append(dict(kwargs)) or SimpleNamespace(
                success=True,
                message="activated",
                error_code="",
                localization_state="localized",
                current_mode="localization",
            )
        )

        occ = SimpleNamespace(
            header=SimpleNamespace(frame_id="map"),
            info=SimpleNamespace(resolution=0.05),
        )

        def _write_occupancy(_occ, maps_root, base_name):
            yaml_path = os.path.join(maps_root, base_name + ".yaml")
            pgm_path = os.path.join(maps_root, base_name + ".pgm")
            with open(yaml_path, "w", encoding="utf-8") as fh:
                fh.write("image: demo_map.pgm\n")
            with open(pgm_path, "w", encoding="utf-8") as fh:
                fh.write("P2\n1 1\n100\n0\n")
            return pgm_path, yaml_path

        with mock.patch(
            "coverage_planner.slam_workflow.runtime_adapter.rospy.wait_for_message",
            return_value=occ,
        ), mock.patch(
            "coverage_planner.slam_workflow.runtime_adapter.compute_occupancy_grid_md5",
            return_value="abc123456789",
        ), mock.patch(
            "coverage_planner.slam_workflow.runtime_adapter.write_occupancy_to_yaml_pgm",
            side_effect=_write_occupancy,
        ), mock.patch(
            "coverage_planner.slam_workflow.runtime_adapter.origin_to_jsonable",
            return_value={"x": 0.0, "y": 0.0, "yaw": 0.0},
        ):
            result = self.adapter.save_mapping(
                robot_id="local_robot",
                map_name="demo_map",
                description="acceptance",
                set_active=True,
                operation=4,
                switch_to_localization_after_save=True,
                relocalize_after_switch=True,
            )

        self.assertTrue(result["success"])
        self.assertEqual(result["map_revision_id"], "rev_saved_demo_01")
        self.assertEqual(len(verify_calls), 1)
        self.assertEqual(len(activate_calls), 1)
        self.assertEqual(verify_calls[0]["map_revision_id"], "rev_saved_demo_01")
        self.assertEqual(activate_calls[0]["map_revision_id"], "rev_saved_demo_01")
        self.assertEqual(verify_calls[0]["operation"], 21)
        self.assertEqual(activate_calls[0]["operation"], 22)

    def test_save_mapping_persists_unverified_candidate_and_defers_activation(self):
        def _save_pbstream(path, include_unfinished_submaps=True):
            del include_unfinished_submaps
            with open(path, "w", encoding="utf-8") as fh:
                fh.write("pbstream")
            return True, "ok"

        self.adapter._transport.save_pbstream = _save_pbstream
        occ = SimpleNamespace(
            header=SimpleNamespace(frame_id="map"),
            info=SimpleNamespace(resolution=0.05),
        )

        def _write_occupancy(_occ, maps_root, base_name):
            yaml_path = os.path.join(maps_root, base_name + ".yaml")
            pgm_path = os.path.join(maps_root, base_name + ".pgm")
            with open(yaml_path, "w", encoding="utf-8") as fh:
                fh.write("image: demo_map.pgm\n")
            with open(pgm_path, "w", encoding="utf-8") as fh:
                fh.write("P2\n1 1\n100\n0\n")
            return pgm_path, yaml_path

        with mock.patch(
            "coverage_planner.slam_workflow.runtime_adapter.rospy.wait_for_message",
            return_value=occ,
        ), mock.patch(
            "coverage_planner.slam_workflow.runtime_adapter.compute_occupancy_grid_md5",
            return_value="abc123456789",
        ), mock.patch(
            "coverage_planner.slam_workflow.runtime_adapter.write_occupancy_to_yaml_pgm",
            side_effect=_write_occupancy,
        ), mock.patch(
            "coverage_planner.slam_workflow.runtime_adapter.origin_to_jsonable",
            return_value={"x": 0.0, "y": 0.0, "yaw": 0.0},
        ):
            result = self.adapter.save_mapping(
                robot_id="local_robot",
                map_name="demo_map",
                description="acceptance",
                set_active=True,
                operation=4,
            )

        self.assertTrue(result["success"])
        self.assertIn("activation deferred", result["message"])
        self.assertEqual(result["map_revision_id"], "rev_saved_demo_01")
        self.assertEqual(self.backend._plan_store.set_active_calls, [])
        self.assertEqual(self.backend._plan_store.register_map_asset_kwargs["map_id"], "")
        self.assertEqual(self.backend._plan_store.register_map_asset_kwargs["map_md5"], "")
        self.assertEqual(
            self.backend._plan_store.register_map_asset_kwargs["lifecycle_status"],
            "saved_unverified",
        )
        self.assertEqual(
            self.backend._plan_store.register_map_asset_kwargs["verification_status"],
            "pending",
        )
        self.assertEqual(
            self.backend._plan_store.register_map_asset_kwargs["save_snapshot_md5"],
            "abc123456789",
        )
        self.assertEqual(
            self.backend._plan_store.register_map_asset_kwargs["revision_id"],
            "rev_saved_demo_01",
        )
        self.assertFalse(self.backend._plan_store.register_map_asset_kwargs["set_active"])

    def test_rebind_map_identity_from_runtime_skips_bound_assets(self):
        self.backend._runtime_context._map_md5 = "abc123"

        rebound = self.adapter.rebind_map_identity_from_runtime(
            map_name="demo_map",
            asset={"map_id": "map_old", "map_md5": "oldmd5"},
        )

        self.assertIsNone(rebound)
        self.assertEqual(self.backend._plan_store.rebind_calls, [])

    @mock.patch("coverage_planner.slam_workflow.runtime_adapter.rospy.is_shutdown", return_value=False)
    @mock.patch("coverage_planner.slam_workflow.runtime_adapter.rospy.sleep", side_effect=lambda *_args, **_kwargs: None)
    @mock.patch("coverage_planner.slam_workflow.runtime_adapter.time.time")
    def test_wait_until_ready_reports_requested_runtime_map_md5_mismatch(self, time_now, _sleep, _is_shutdown):
        self.backend.allow_identity_rebind_on_localize = False
        self.backend._plan_store.selected_map = {"map_name": "demo_map"}
        self.backend._runtime_context.tracked_pose_fresh_since = lambda **_kwargs: True
        self.backend._runtime_context.tracked_pose_stable_since = lambda **_kwargs: True
        self.backend._runtime_context.runtime_map_ready_since = lambda **_kwargs: True
        self.backend._runtime_context.runtime_map_matches = lambda _target_md5: False
        self.backend._runtime_context.runtime_map_md5 = lambda: "runtime_md5"
        asset = {"map_name": "demo_map", "map_md5": "requested_md5"}
        time_now.side_effect = [100.0, 100.0, 100.0, 101.0]

        ok, msg = self.adapter.wait_until_ready(
            robot_id="local_robot",
            asset=asset,
            started_after_ts=99.0,
        )

        self.assertFalse(ok)
        self.assertEqual(msg, "runtime map_md5 runtime_md5 != requested map_md5 requested_md5")

    @mock.patch("coverage_planner.slam_workflow.runtime_adapter.rospy.set_param", side_effect=lambda *_args, **_kwargs: None)
    def test_restart_localization_commits_active_only_after_success(self, _set_param):
        pbstream_path = os.path.join(self._tmpdir, "demo_map.pbstream")
        with open(pbstream_path, "w", encoding="utf-8") as fh:
            fh.write("pbstream")
        self.backend.asset = {
            "map_name": "demo_map",
            "revision_id": "rev_demo_01",
            "pbstream_path": pbstream_path,
            "enabled": True,
            "verification_status": "pending",
        }
        self.backend._plan_store.selected_map = {"map_name": "stable_map", "verification_status": "verified"}
        self.backend._runtime_context._map_md5 = "runtime_md5_1234"
        self.adapter._transport.stop_runtime = lambda: None
        self.adapter._transport.start_runtime_processes = lambda **_kwargs: (321, "runtime_started")
        self.adapter.run_localization_sequence = lambda **_kwargs: "localized"
        self.adapter.wait_until_ready = lambda **_kwargs: (True, "localized")

        result = self.adapter.restart_localization(
            robot_id="local_robot",
            map_name="demo_map",
            operation=2,
        )

        self.assertTrue(result["success"])
        self.assertEqual(self.backend._plan_store.pending_switch_updates[0]["target_map_name"], "demo_map")
        self.assertEqual(self.backend._plan_store.set_active_calls, [])
        self.assertEqual(
            self.backend._plan_store.set_active_revision_calls,
            [("local_robot", "rev_demo_01")],
        )
        self.assertEqual(self.backend._plan_store.pending_switch_clears, ["local_robot"])
        self.assertEqual(
            self.backend._plan_store.mark_verification_calls[0]["verification_status"],
            "verified",
        )
        self.assertEqual(
            self.backend._plan_store.mark_verification_calls[0]["revision_id"],
            "rev_demo_01",
        )
        self.assertEqual(self.backend._plan_store.call_log[0][0], "upsert_pending_map_switch")
        self.assertEqual(self.backend._plan_store.call_log[1][0], "upsert_pending_map_revision")
        self.assertEqual(self.backend._plan_store.call_log[2][0], "mark_map_asset_verification_result")
        self.assertEqual(self.backend._plan_store.call_log[3][0], "set_active_map_revision")

    @mock.patch("coverage_planner.slam_workflow.runtime_adapter.rospy.set_param", side_effect=lambda *_args, **_kwargs: None)
    def test_restart_localization_failure_keeps_previous_active_map(self, _set_param):
        pbstream_path = os.path.join(self._tmpdir, "demo_map.pbstream")
        with open(pbstream_path, "w", encoding="utf-8") as fh:
            fh.write("pbstream")
        self.backend.asset = {
            "map_name": "demo_map",
            "revision_id": "rev_demo_01",
            "pbstream_path": pbstream_path,
            "enabled": True,
            "verification_status": "pending",
        }
        self.backend._plan_store.selected_map = {"map_name": "stable_map", "verification_status": "verified"}
        self.adapter._transport.stop_runtime = lambda: None
        self.adapter._transport.start_runtime_processes = lambda **_kwargs: (321, "runtime_started")
        self.adapter.run_localization_sequence = lambda **_kwargs: "localized"
        self.adapter.wait_until_ready = lambda **_kwargs: (False, "runtime mismatch")

        result = self.adapter.restart_localization(
            robot_id="local_robot",
            map_name="demo_map",
            operation=2,
        )

        self.assertFalse(result["success"])
        self.assertEqual(self.backend._plan_store.set_active_calls, [])
        self.assertEqual(self.backend._plan_store.pending_switch_clears, ["local_robot"])
        self.assertEqual(
            self.backend._plan_store.mark_verification_calls[0]["verification_status"],
            "failed",
        )
        self.assertEqual(
            self.backend._plan_store.mark_verification_calls[0]["revision_id"],
            "rev_demo_01",
        )

    @mock.patch("coverage_planner.slam_workflow.runtime_adapter.rospy.set_param", side_effect=lambda *_args, **_kwargs: None)
    def test_restart_localization_failure_rolls_runtime_back_to_previous_active(self, _set_param):
        target_pbstream_path = os.path.join(self._tmpdir, "demo_map.pbstream")
        stable_pbstream_path = os.path.join(self._tmpdir, "stable_map.pbstream")
        for path in (target_pbstream_path, stable_pbstream_path):
            with open(path, "w", encoding="utf-8") as fh:
                fh.write("pbstream")
        self.backend.asset = {
            "map_name": "demo_map",
            "revision_id": "rev_demo_01",
            "pbstream_path": target_pbstream_path,
            "enabled": True,
            "verification_status": "pending",
        }
        self.backend._plan_store.selected_map = {
            "map_name": "stable_map",
            "revision_id": "rev_stable_01",
            "pbstream_path": stable_pbstream_path,
            "enabled": True,
            "verification_status": "verified",
        }
        self.backend._plan_store.active_revision = {
            "revision_id": "rev_stable_01",
            "map_name": "stable_map",
        }

        attempt_calls = []

        def _attempt(**kwargs):
            attempt_calls.append(str((kwargs.get("asset") or {}).get("map_name") or ""))
            if len(attempt_calls) == 1:
                return False, "runtime mismatch", target_pbstream_path, "rev_demo_01"
            return True, "rollback localized", stable_pbstream_path, "rev_stable_01"

        self.adapter._execute_localization_attempt = _attempt

        result = self.adapter.restart_localization(
            robot_id="local_robot",
            map_name="demo_map",
            operation=2,
        )

        self.assertFalse(result["success"])
        self.assertEqual(result["localization_state"], "localized")
        self.assertEqual(attempt_calls, ["demo_map", "stable_map"])
        self.assertEqual(self.backend._plan_store.set_active_revision_calls, [("local_robot", "rev_stable_01")])
        self.assertIn("rolled back to active map", result["message"])

    def test_reconcile_pending_map_switch_marks_target_failed_and_clears_pending(self):
        self.backend._plan_store.pending_switch = {
            "robot_id": "local_robot",
            "target_map_name": "demo_map",
            "status": "verifying",
        }
        self.backend._plan_store.active_revision = {
            "revision_id": "rev_stable_01",
            "map_name": "stable_map",
        }

        self.adapter.reconcile_pending_map_switch(robot_id="local_robot")

        self.assertEqual(self.backend._plan_store.pending_switch_clears, ["local_robot"])
        self.assertEqual(
            self.backend._plan_store.mark_verification_calls[0]["error_code"],
            "stale_pending_map_switch",
        )

    @mock.patch("coverage_planner.slam_workflow.runtime_adapter.rospy.is_shutdown", return_value=False)
    @mock.patch("coverage_planner.slam_workflow.runtime_adapter.rospy.sleep", side_effect=lambda *_args, **_kwargs: None)
    @mock.patch("coverage_planner.slam_workflow.runtime_adapter.rospy.set_param", side_effect=lambda *_args, **_kwargs: None)
    @mock.patch("coverage_planner.slam_workflow.runtime_adapter.time.time")
    def test_wait_until_ready_accepts_pending_target_before_active_commit(
        self,
        time_now,
        _set_param,
        _sleep,
        _is_shutdown,
    ):
        self.backend._plan_store.selected_map = {"map_name": "stable_map"}
        self.backend._plan_store.pending_switch = {"target_map_name": "demo_map"}
        self.backend._runtime_context.tracked_pose_fresh_since = lambda **_kwargs: True
        self.backend._runtime_context.tracked_pose_stable_since = lambda **_kwargs: True
        self.backend._runtime_context.runtime_map_ready_since = lambda **_kwargs: True
        self.backend._runtime_context.runtime_map_matches = lambda _target_md5: True
        self.backend._runtime_context.runtime_map_md5 = lambda: "runtime_md5"
        time_now.side_effect = [100.0, 100.0, 100.0]

        ok, msg = self.adapter.wait_until_ready(
            robot_id="local_robot",
            asset={"map_name": "demo_map", "map_md5": ""},
            started_after_ts=99.0,
        )

        self.assertTrue(ok)
        self.assertEqual(msg, "localized")

    @mock.patch("coverage_planner.slam_workflow.runtime_adapter.rospy.is_shutdown", return_value=False)
    @mock.patch("coverage_planner.slam_workflow.runtime_adapter.rospy.sleep", side_effect=lambda *_args, **_kwargs: None)
    @mock.patch("coverage_planner.slam_workflow.runtime_adapter.rospy.set_param", side_effect=lambda *_args, **_kwargs: None)
    @mock.patch("coverage_planner.slam_workflow.runtime_adapter.time.time")
    def test_wait_until_ready_requires_stable_window_before_localized(self, time_now, _set_param, _sleep, _is_shutdown):
        self.backend._plan_store.selected_map = {"map_name": "demo_map", "revision_id": "rev_demo_01"}
        self.backend._runtime_context.tracked_pose_fresh_since = lambda **_kwargs: True
        stable_values = iter([False, True])
        self.backend._runtime_context.tracked_pose_stable_since = lambda **_kwargs: next(stable_values)
        self.backend._runtime_context.runtime_map_ready_since = lambda **_kwargs: True
        self.backend._runtime_context.runtime_map_matches = lambda _target_md5, target_revision_id="": True
        self.backend._runtime_context.runtime_map_md5 = lambda: "runtime_md5"
        time_now.side_effect = [100.0, 100.0, 100.0, 100.2, 100.2, 100.2]

        ok, msg = self.adapter.wait_until_ready(
            robot_id="local_robot",
            asset={"map_name": "demo_map", "revision_id": "rev_demo_01", "map_md5": ""},
            started_after_ts=99.0,
        )

        self.assertTrue(ok)
        self.assertEqual(msg, "localized")
        self.assertEqual(self.backend._runtime_state.localization_calls[0]["state"], "stabilizing")
        self.assertEqual(self.backend._runtime_state.localization_calls[-1]["state"], "localized")

    @mock.patch("coverage_planner.slam_workflow.runtime_adapter.rospy.set_param", side_effect=lambda *_args, **_kwargs: None)
    def test_restart_localization_same_scope_manual_assist_sets_manual_assist_state(self, _set_param):
        pbstream_path = os.path.join(self._tmpdir, "demo_map.pbstream")
        with open(pbstream_path, "w", encoding="utf-8") as fh:
            fh.write("pbstream")
        self.backend.asset = {
            "map_name": "demo_map",
            "revision_id": "rev_demo_01",
            "pbstream_path": pbstream_path,
            "enabled": True,
            "verification_status": "verified",
        }
        self.backend._plan_store.selected_map = {
            "map_name": "demo_map",
            "revision_id": "rev_demo_01",
            "pbstream_path": pbstream_path,
            "enabled": True,
            "verification_status": "verified",
        }
        self.adapter._transport.stop_runtime = lambda: None
        self.adapter._transport.start_runtime_processes = lambda **_kwargs: (321, "runtime_started")
        self.adapter.run_localization_sequence = mock.Mock(
            side_effect=SlamWorkflowRuntimeError(
                "relocalize_low_constraint_score",
                "try_global_relocate failed",
                manual_assist_required=True,
            )
        )

        result = self.adapter.restart_localization(
            robot_id="local_robot",
            map_name="demo_map",
            map_revision_id="rev_demo_01",
            operation=2,
        )

        self.assertFalse(result["success"])
        self.assertEqual(result["localization_state"], "manual_assist_required")
        self.assertEqual(self.backend._plan_store.set_active_revision_calls, [])
        self.assertEqual(self.backend._runtime_state.localization_calls[-1]["state"], "manual_assist_required")
        self.assertIn("provide initial pose", result["message"])


if __name__ == "__main__":
    unittest.main()

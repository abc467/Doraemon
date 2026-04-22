#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import importlib.util
import os
import pathlib
import sys
import tempfile
import unittest
from unittest import mock

from nav_msgs.msg import OccupancyGrid

from coverage_planner.map_asset_import import register_imported_map_asset
from coverage_planner.map_asset_import import normalize_import_verification_mode
from coverage_planner.map_io import write_occupancy_to_yaml_pgm
from coverage_planner.plan_store.store import PlanStore


def _load_map_asset_service_module():
    script_path = pathlib.Path(__file__).resolve().parents[1] / "scripts" / "map_asset_service_node.py"
    spec = importlib.util.spec_from_file_location("map_asset_service_node_test_mod", str(script_path))
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _load_migrate_map_assets_module():
    script_path = pathlib.Path(__file__).resolve().parents[1] / "tools" / "migrate_map_assets.py"
    spec = importlib.util.spec_from_file_location("migrate_map_assets_test_mod", str(script_path))
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _load_import_map_assets_module():
    script_path = pathlib.Path(__file__).resolve().parents[1] / "tools" / "import_map_assets.py"
    spec = importlib.util.spec_from_file_location("import_map_assets_test_mod", str(script_path))
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


MAP_ASSET_SERVICE_MODULE = _load_map_asset_service_module()
MIGRATE_MAP_ASSETS_MODULE = _load_migrate_map_assets_module()
IMPORT_MAP_ASSETS_MODULE = _load_import_map_assets_module()


def _make_occ():
    occ = OccupancyGrid()
    occ.header.frame_id = "map"
    occ.info.resolution = 0.05
    occ.info.width = 2
    occ.info.height = 2
    occ.info.origin.position.x = 1.0
    occ.info.origin.position.y = -2.0
    occ.info.origin.orientation.w = 1.0
    occ.data = [0, 100, -1, 0]
    return occ


class MapAssetImportFlowTest(unittest.TestCase):
    def test_normalize_import_verification_mode_rejects_old_alias(self):
        with self.assertRaises(ValueError):
            normalize_import_verification_mode("legacy_verified")
        self.assertEqual(normalize_import_verification_mode("offline_verified"), "offline_verified")
        self.assertEqual(normalize_import_verification_mode("candidate"), "candidate")

    def test_register_imported_map_asset_returns_created_revision_not_name_head(self):
        class _FakeStore(object):
            def __init__(self):
                self.register_kwargs = None

            def register_map_asset(self, **kwargs):
                self.register_kwargs = dict(kwargs)
                return "rev_demo_expected"

            def resolve_map_asset(self, *, map_name="", robot_id="local_robot", revision_id=""):
                if str(revision_id or "") == "rev_demo_expected":
                    return {
                        "map_name": "offline_demo",
                        "revision_id": "rev_demo_expected",
                        "verification_status": "verified",
                    }
                if str(map_name or "") == "offline_demo":
                    return {
                        "map_name": "offline_demo",
                        "revision_id": "rev_demo_head",
                        "verification_status": "pending",
                    }
                return None

        asset, snapshot_md5 = register_imported_map_asset(
            _FakeStore(),
            map_name="offline_demo",
            occ=_make_occ(),
            yaml_path="/tmp/offline_demo.yaml",
            pgm_path="/tmp/offline_demo.pgm",
            pbstream_path="/tmp/offline_demo.pbstream",
            robot_id="robot_a",
            verification_mode="offline_verified",
            set_active=True,
        )

        self.assertTrue(snapshot_md5)
        self.assertEqual(str(asset.get("revision_id") or ""), "rev_demo_expected")
        self.assertEqual(str(asset.get("verification_status") or ""), "verified")

    def test_candidate_import_registers_unverified_revision(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "planning.db")
            store = PlanStore(db_path)
            occ = _make_occ()
            pgm_path, yaml_path = write_occupancy_to_yaml_pgm(occ, tmpdir, base_name="candidate_demo")
            pbstream_path = os.path.join(tmpdir, "candidate_demo.pbstream")
            with open(pbstream_path, "wb") as fh:
                fh.write(b"pbstream")

            asset, snapshot_md5 = register_imported_map_asset(
                store,
                map_name="candidate_demo",
                occ=occ,
                yaml_path=yaml_path,
                pgm_path=pgm_path,
                pbstream_path=pbstream_path,
                robot_id="robot_a",
                verification_mode="candidate",
                set_active=False,
            )

            self.assertTrue(snapshot_md5)
            self.assertEqual(asset["verification_status"], "pending")
            self.assertEqual(asset["lifecycle_status"], "saved_unverified")
            self.assertEqual(asset["map_id"], "")
            self.assertEqual(asset["map_md5"], "")
            revision = store.resolve_map_revision(revision_id=str(asset.get("revision_id") or ""))
            self.assertIsNotNone(revision)
            self.assertEqual(str(revision.get("live_snapshot_md5") or ""), snapshot_md5)
            self.assertEqual(str(revision.get("verification_status") or ""), "pending")

    def test_offline_verified_import_can_activate(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "planning.db")
            store = PlanStore(db_path)
            occ = _make_occ()
            pgm_path, yaml_path = write_occupancy_to_yaml_pgm(occ, tmpdir, base_name="offline_demo")
            pbstream_path = os.path.join(tmpdir, "offline_demo.pbstream")
            with open(pbstream_path, "wb") as fh:
                fh.write(b"pbstream")

            asset, snapshot_md5 = register_imported_map_asset(
                store,
                map_name="offline_demo",
                occ=occ,
                yaml_path=yaml_path,
                pgm_path=pgm_path,
                pbstream_path=pbstream_path,
                robot_id="robot_a",
                verification_mode="offline_verified",
                set_active=True,
            )

            self.assertTrue(snapshot_md5)
            self.assertEqual(asset["verification_status"], "verified")
            self.assertEqual(asset["lifecycle_status"], "available")
            self.assertTrue(str(asset.get("map_id") or "").startswith("map_"))
            self.assertEqual(str(asset.get("map_md5") or ""), snapshot_md5)
            active = store.get_active_map(robot_id="robot_a") or {}
            self.assertEqual(str(active.get("revision_id") or ""), str(asset.get("revision_id") or ""))

    def test_map_asset_service_import_uses_external_yaml_not_live_map(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            external_root = os.path.join(tmpdir, "external")
            maps_root = os.path.join(tmpdir, "managed")
            os.makedirs(external_root, exist_ok=True)
            os.makedirs(maps_root, exist_ok=True)
            with open(os.path.join(external_root, "demo.pbstream"), "wb") as fh:
                fh.write(b"pbstream")
            with open(os.path.join(external_root, "demo.yaml"), "w", encoding="utf-8") as fh:
                fh.write("image: demo.pgm\nresolution: 0.05\norigin: [0.0, 0.0, 0.0]\n")

            node = MAP_ASSET_SERVICE_MODULE.MapAssetServiceNode.__new__(MAP_ASSET_SERVICE_MODULE.MapAssetServiceNode)
            node.robot_id = "robot_a"
            node.external_maps_root = external_root
            node.maps_root = maps_root
            node.store = PlanStore(os.path.join(tmpdir, "planning.db"))

            with mock.patch.object(MAP_ASSET_SERVICE_MODULE, "yaml_pgm_to_occupancy", return_value=_make_occ()), mock.patch.object(
                MAP_ASSET_SERVICE_MODULE.rospy,
                "wait_for_message",
                side_effect=AssertionError("live /map should not be used during external import"),
            ):
                asset = node._import_external_map(map_name="demo", set_active=False, description="offline")

            self.assertEqual(str(asset.get("verification_status") or ""), "pending")
            self.assertEqual(str(asset.get("lifecycle_status") or ""), "saved_unverified")
            self.assertEqual(str(asset.get("map_md5") or ""), "")

    def test_map_asset_service_to_msg_includes_revision_id(self):
        node = MAP_ASSET_SERVICE_MODULE.MapAssetServiceNode.__new__(MAP_ASSET_SERVICE_MODULE.MapAssetServiceNode)
        node.store = mock.Mock()
        node.store.resolve_map_asset.return_value = {
            "map_name": "demo_map",
            "revision_id": "rev_demo_01",
            "lifecycle_status": "available",
            "verification_status": "verified",
        }

        msg = node._to_msg(
            {
                "map_name": "demo_map",
                "revision_id": "rev_demo_01",
                "description": "demo",
                "enabled": True,
                "map_id": "map_demo",
                "map_md5": "md5_demo",
                "lifecycle_status": "available",
                "verification_status": "verified",
            },
            include_map_data=False,
            active_map={"map_name": "demo_map", "revision_id": "rev_demo_01"},
            latest_head={
                "map_name": "demo_map",
                "revision_id": "rev_demo_01",
                "lifecycle_status": "available",
                "verification_status": "verified",
            },
        )

        self.assertEqual(str(msg.map_name or ""), "demo_map")
        self.assertEqual(str(msg.map_revision_id or ""), "rev_demo_01")
        self.assertTrue(bool(msg.is_active))
        self.assertTrue(bool(msg.is_latest_head))
        self.assertEqual(str(msg.lifecycle_status or ""), "available")
        self.assertEqual(str(msg.verification_status or ""), "verified")
        self.assertEqual(str(msg.active_revision_id or ""), "rev_demo_01")
        self.assertEqual(str(msg.latest_head_revision_id or ""), "rev_demo_01")
        self.assertFalse(bool(msg.has_newer_head_revision))

    def test_map_asset_service_to_msg_prefers_revision_for_is_active(self):
        node = MAP_ASSET_SERVICE_MODULE.MapAssetServiceNode.__new__(MAP_ASSET_SERVICE_MODULE.MapAssetServiceNode)
        node.store = mock.Mock()
        node.store.resolve_map_asset.return_value = {
            "map_name": "demo_map",
            "revision_id": "rev_demo_02",
            "lifecycle_status": "saved_unverified",
            "verification_status": "pending",
        }

        msg = node._to_msg(
            {
                "map_name": "demo_map",
                "revision_id": "rev_demo_02",
                "description": "candidate",
                "enabled": True,
                "map_id": "",
                "map_md5": "",
                "lifecycle_status": "saved_unverified",
                "verification_status": "pending",
            },
            include_map_data=False,
            active_map={"map_name": "demo_map", "revision_id": "rev_demo_01"},
            latest_head={
                "map_name": "demo_map",
                "revision_id": "rev_demo_02",
                "lifecycle_status": "saved_unverified",
                "verification_status": "pending",
            },
        )

        self.assertEqual(str(msg.map_revision_id or ""), "rev_demo_02")
        self.assertFalse(bool(msg.is_active))
        self.assertTrue(bool(msg.is_latest_head))
        self.assertEqual(str(msg.active_revision_id or ""), "rev_demo_01")
        self.assertEqual(str(msg.latest_head_revision_id or ""), "rev_demo_02")
        self.assertTrue(bool(msg.has_newer_head_revision))
        self.assertEqual(str(msg.latest_head_verification_status or ""), "pending")
        self.assertEqual(str(msg.latest_head_lifecycle_status or ""), "saved_unverified")

    def test_map_asset_service_get_can_resolve_revision_only_request(self):
        class _FakeStore(object):
            def __init__(self):
                self.resolve_calls = []

            def get_active_map(self, robot_id="local_robot"):
                return {}

            def resolve_map_asset(self, *, map_name="", revision_id="", robot_id="local_robot"):
                self.resolve_calls.append(
                    {
                        "map_name": map_name,
                        "revision_id": revision_id,
                        "robot_id": robot_id,
                    }
                )
                if str(revision_id or "") == "rev_demo_01":
                    return {
                        "map_name": "demo_map",
                        "revision_id": "rev_demo_01",
                        "description": "demo",
                        "enabled": True,
                        "map_id": "map_demo",
                        "map_md5": "md5_demo",
                    }
                return None

        req = type("Req", (), {})()
        req.get = 0
        req.add = 1
        req.modify = 2
        req.Delete = 3
        req.getAll = 4
        req.operation = 0
        req.map_name = ""
        req.map = type("MapArg", (), {"map_name": "", "map_revision_id": "rev_demo_01", "description": ""})()
        req.set_active = False
        req.enabled_state = 0

        node = MAP_ASSET_SERVICE_MODULE.MapAssetServiceNode.__new__(MAP_ASSET_SERVICE_MODULE.MapAssetServiceNode)
        node.robot_id = "robot_a"
        node.store = _FakeStore()

        resp = node._handle(req)

        self.assertTrue(bool(resp.success))
        self.assertEqual(str(resp.map.map_name or ""), "demo_map")
        self.assertEqual(str(resp.map.map_revision_id or ""), "rev_demo_01")
        self.assertIn(
            {
                "map_name": "",
                "revision_id": "rev_demo_01",
                "robot_id": "robot_a",
            },
            node.store.resolve_calls,
        )

    def test_map_asset_service_get_prefers_active_revision_for_same_name(self):
        class _FakeStore(object):
            def __init__(self):
                self.resolve_calls = []

            def get_active_map(self, robot_id="local_robot"):
                del robot_id
                return {
                    "map_name": "demo_map",
                    "revision_id": "rev_demo_active",
                    "description": "stable",
                    "enabled": True,
                    "map_id": "map_active",
                    "map_md5": "md5_active",
                    "yaml_path": "",
                    "lifecycle_status": "available",
                    "verification_status": "verified",
                }

            def resolve_map_asset(self, *, map_name="", revision_id="", robot_id="local_robot"):
                self.resolve_calls.append(
                    {
                        "map_name": map_name,
                        "revision_id": revision_id,
                        "robot_id": robot_id,
                    }
                )
                if str(revision_id or "") == "rev_demo_active":
                    return {
                        "map_name": "demo_map",
                        "revision_id": "rev_demo_active",
                        "description": "stable",
                        "enabled": True,
                        "map_id": "map_active",
                        "map_md5": "md5_active",
                        "yaml_path": "",
                        "lifecycle_status": "available",
                        "verification_status": "verified",
                    }
                if str(map_name or "") == "demo_map":
                    return {
                        "map_name": "demo_map",
                        "revision_id": "rev_demo_head",
                        "description": "candidate",
                        "enabled": True,
                        "map_id": "",
                        "map_md5": "",
                        "yaml_path": "",
                        "lifecycle_status": "saved_unverified",
                        "verification_status": "pending",
                    }
                return None

        req = type("Req", (), {})()
        req.get = 0
        req.add = 1
        req.modify = 2
        req.Delete = 3
        req.getAll = 4
        req.operation = 0
        req.map_name = "demo_map"
        req.map = type("MapArg", (), {"map_name": "demo_map", "map_revision_id": "", "description": ""})()
        req.set_active = False
        req.enabled_state = 0

        node = MAP_ASSET_SERVICE_MODULE.MapAssetServiceNode.__new__(MAP_ASSET_SERVICE_MODULE.MapAssetServiceNode)
        node.robot_id = "robot_a"
        node.store = _FakeStore()

        resp = node._handle(req)

        self.assertTrue(bool(resp.success))
        self.assertEqual(str(resp.map.map_revision_id or ""), "rev_demo_active")
        self.assertTrue(bool(resp.map.is_active))
        self.assertFalse(bool(resp.map.is_latest_head))
        self.assertEqual(str(resp.map.latest_head_revision_id or ""), "rev_demo_head")
        self.assertTrue(bool(resp.map.has_newer_head_revision))

    def test_map_asset_service_get_prefers_verified_revision_scope_over_pending_head(self):
        class _FakeStore(object):
            def __init__(self):
                self.resolve_calls = []
                self.resolve_revision_calls = []

            def get_active_map(self, robot_id="local_robot"):
                del robot_id
                return {
                    "map_name": "other_map",
                    "revision_id": "rev_other_active",
                    "description": "other",
                    "enabled": True,
                    "map_id": "map_other",
                    "map_md5": "md5_other",
                    "yaml_path": "",
                    "lifecycle_status": "available",
                    "verification_status": "verified",
                }

            def resolve_map_asset(self, *, map_name="", revision_id="", robot_id="local_robot"):
                self.resolve_calls.append(
                    {
                        "map_name": map_name,
                        "revision_id": revision_id,
                        "robot_id": robot_id,
                    }
                )
                if str(map_name or "") == "demo_map":
                    return {
                        "map_name": "demo_map",
                        "revision_id": "rev_demo_head",
                        "description": "candidate",
                        "enabled": True,
                        "map_id": "",
                        "map_md5": "",
                        "yaml_path": "",
                        "lifecycle_status": "saved_unverified",
                        "verification_status": "pending",
                    }
                return None

            def resolve_map_revision(self, *, map_name="", revision_id="", robot_id="local_robot"):
                self.resolve_revision_calls.append(
                    {
                        "map_name": map_name,
                        "revision_id": revision_id,
                        "robot_id": robot_id,
                    }
                )
                if str(map_name or "") == "demo_map":
                    return {
                        "map_name": "demo_map",
                        "revision_id": "rev_demo_verified",
                        "description": "stable",
                        "enabled": True,
                        "map_id": "map_demo",
                        "map_md5": "md5_demo",
                        "yaml_path": "",
                        "lifecycle_status": "available",
                        "verification_status": "verified",
                    }
                return None

        req = type("Req", (), {})()
        req.get = 0
        req.add = 1
        req.modify = 2
        req.Delete = 3
        req.getAll = 4
        req.operation = 0
        req.map_name = "demo_map"
        req.map = type("MapArg", (), {"map_name": "demo_map", "map_revision_id": "", "description": ""})()
        req.set_active = False
        req.enabled_state = 0

        node = MAP_ASSET_SERVICE_MODULE.MapAssetServiceNode.__new__(MAP_ASSET_SERVICE_MODULE.MapAssetServiceNode)
        node.robot_id = "robot_a"
        node.store = _FakeStore()

        resp = node._handle(req)

        self.assertTrue(bool(resp.success))
        self.assertEqual(str(resp.map.map_revision_id or ""), "rev_demo_verified")
        self.assertFalse(bool(resp.map.is_active))
        self.assertFalse(bool(resp.map.is_latest_head))
        self.assertEqual(str(resp.map.latest_head_revision_id or ""), "rev_demo_head")
        self.assertEqual(
            node.store.resolve_revision_calls[-1],
            {
                "map_name": "demo_map",
                "revision_id": "",
                "robot_id": "robot_a",
            },
        )

    def test_map_asset_service_get_all_returns_active_view_with_head_metadata(self):
        class _FakeStore(object):
            def list_map_assets(self):
                return [
                    {
                        "map_name": "demo_map",
                        "revision_id": "rev_demo_head",
                        "description": "candidate",
                        "enabled": True,
                        "map_id": "",
                        "map_md5": "",
                        "lifecycle_status": "saved_unverified",
                        "verification_status": "pending",
                    },
                    {
                        "map_name": "other_map",
                        "revision_id": "rev_other_head",
                        "description": "other",
                        "enabled": True,
                        "map_id": "map_other",
                        "map_md5": "md5_other",
                        "lifecycle_status": "available",
                        "verification_status": "verified",
                    },
                ]

            def get_active_map(self, robot_id="local_robot"):
                del robot_id
                return {
                    "map_name": "demo_map",
                    "revision_id": "rev_demo_active",
                    "description": "stable",
                    "enabled": True,
                    "map_id": "map_active",
                    "map_md5": "md5_active",
                    "lifecycle_status": "available",
                    "verification_status": "verified",
                }

            def resolve_map_asset(self, *, map_name="", revision_id="", robot_id="local_robot"):
                del robot_id
                if str(revision_id or "") == "rev_demo_active":
                    return {
                        "map_name": "demo_map",
                        "revision_id": "rev_demo_active",
                        "description": "stable",
                        "enabled": True,
                        "map_id": "map_active",
                        "map_md5": "md5_active",
                        "lifecycle_status": "available",
                        "verification_status": "verified",
                    }
                if str(map_name or "") == "demo_map":
                    return {
                        "map_name": "demo_map",
                        "revision_id": "rev_demo_head",
                        "description": "candidate",
                        "enabled": True,
                        "map_id": "",
                        "map_md5": "",
                        "lifecycle_status": "saved_unverified",
                        "verification_status": "pending",
                    }
                if str(map_name or "") == "other_map":
                    return {
                        "map_name": "other_map",
                        "revision_id": "rev_other_head",
                        "description": "other",
                        "enabled": True,
                        "map_id": "map_other",
                        "map_md5": "md5_other",
                        "lifecycle_status": "available",
                        "verification_status": "verified",
                    }
                return None

        req = type("Req", (), {})()
        req.get = 0
        req.add = 1
        req.modify = 2
        req.Delete = 3
        req.getAll = 4
        req.operation = 4
        req.map_name = ""
        req.map = type("MapArg", (), {"map_name": "", "map_revision_id": "", "description": ""})()
        req.set_active = False
        req.enabled_state = 0

        node = MAP_ASSET_SERVICE_MODULE.MapAssetServiceNode.__new__(MAP_ASSET_SERVICE_MODULE.MapAssetServiceNode)
        node.robot_id = "robot_a"
        node.store = _FakeStore()

        resp = node._handle(req)

        self.assertTrue(bool(resp.success))
        self.assertEqual(len(resp.maps), 2)
        demo_msg = next(msg for msg in resp.maps if str(msg.map_name or "") == "demo_map")
        other_msg = next(msg for msg in resp.maps if str(msg.map_name or "") == "other_map")
        self.assertEqual(str(demo_msg.map_revision_id or ""), "rev_demo_active")
        self.assertTrue(bool(demo_msg.is_active))
        self.assertFalse(bool(demo_msg.is_latest_head))
        self.assertEqual(str(demo_msg.latest_head_revision_id or ""), "rev_demo_head")
        self.assertEqual(str(other_msg.map_revision_id or ""), "rev_other_head")
        self.assertTrue(bool(other_msg.is_latest_head))

    def test_map_asset_service_get_all_prefers_verified_view_over_pending_head(self):
        class _FakeStore(object):
            def __init__(self):
                self.resolve_revision_calls = []

            def list_map_assets(self):
                return [
                    {
                        "map_name": "demo_map",
                        "revision_id": "rev_demo_head",
                        "description": "candidate",
                        "enabled": True,
                        "map_id": "",
                        "map_md5": "",
                        "lifecycle_status": "saved_unverified",
                        "verification_status": "pending",
                    },
                    {
                        "map_name": "other_map",
                        "revision_id": "rev_other_head",
                        "description": "other",
                        "enabled": True,
                        "map_id": "map_other",
                        "map_md5": "md5_other",
                        "lifecycle_status": "available",
                        "verification_status": "verified",
                    },
                ]

            def get_active_map(self, robot_id="local_robot"):
                del robot_id
                return {
                    "map_name": "other_map",
                    "revision_id": "rev_other_head",
                    "description": "other",
                    "enabled": True,
                    "map_id": "map_other",
                    "map_md5": "md5_other",
                    "lifecycle_status": "available",
                    "verification_status": "verified",
                }

            def resolve_map_asset(self, *, map_name="", revision_id="", robot_id="local_robot"):
                del revision_id, robot_id
                if str(map_name or "") == "demo_map":
                    return {
                        "map_name": "demo_map",
                        "revision_id": "rev_demo_head",
                        "description": "candidate",
                        "enabled": True,
                        "map_id": "",
                        "map_md5": "",
                        "lifecycle_status": "saved_unverified",
                        "verification_status": "pending",
                    }
                if str(map_name or "") == "other_map":
                    return {
                        "map_name": "other_map",
                        "revision_id": "rev_other_head",
                        "description": "other",
                        "enabled": True,
                        "map_id": "map_other",
                        "map_md5": "md5_other",
                        "lifecycle_status": "available",
                        "verification_status": "verified",
                    }
                return None

            def resolve_map_revision(self, *, map_name="", revision_id="", robot_id="local_robot"):
                self.resolve_revision_calls.append(
                    {
                        "map_name": map_name,
                        "revision_id": revision_id,
                        "robot_id": robot_id,
                    }
                )
                if str(map_name or "") == "demo_map":
                    return {
                        "map_name": "demo_map",
                        "revision_id": "rev_demo_verified",
                        "description": "stable",
                        "enabled": True,
                        "map_id": "map_demo",
                        "map_md5": "md5_demo",
                        "lifecycle_status": "available",
                        "verification_status": "verified",
                    }
                if str(map_name or "") == "other_map":
                    return {
                        "map_name": "other_map",
                        "revision_id": "rev_other_head",
                        "description": "other",
                        "enabled": True,
                        "map_id": "map_other",
                        "map_md5": "md5_other",
                        "lifecycle_status": "available",
                        "verification_status": "verified",
                    }
                return None

        req = type("Req", (), {})()
        req.get = 0
        req.add = 1
        req.modify = 2
        req.Delete = 3
        req.getAll = 4
        req.operation = 4
        req.map_name = ""
        req.map = type("MapArg", (), {"map_name": "", "map_revision_id": "", "description": ""})()
        req.set_active = False
        req.enabled_state = 0

        node = MAP_ASSET_SERVICE_MODULE.MapAssetServiceNode.__new__(MAP_ASSET_SERVICE_MODULE.MapAssetServiceNode)
        node.robot_id = "robot_a"
        node.store = _FakeStore()

        resp = node._handle(req)

        self.assertTrue(bool(resp.success))
        self.assertEqual(len(resp.maps), 2)
        demo_msg = next(msg for msg in resp.maps if str(msg.map_name or "") == "demo_map")
        self.assertEqual(str(demo_msg.map_revision_id or ""), "rev_demo_verified")
        self.assertFalse(bool(demo_msg.is_latest_head))
        self.assertEqual(str(demo_msg.latest_head_revision_id or ""), "rev_demo_head")

    def test_map_asset_service_import_allows_same_name_new_candidate_revision(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            external_root = os.path.join(tmpdir, "external")
            maps_root = os.path.join(tmpdir, "managed")
            os.makedirs(external_root, exist_ok=True)
            os.makedirs(maps_root, exist_ok=True)
            with open(os.path.join(external_root, "demo.pbstream"), "wb") as fh:
                fh.write(b"pbstream")
            with open(os.path.join(external_root, "demo.yaml"), "w", encoding="utf-8") as fh:
                fh.write("image: demo.pgm\nresolution: 0.05\norigin: [0.0, 0.0, 0.0]\n")

            store = PlanStore(os.path.join(tmpdir, "planning.db"))
            occ = _make_occ()
            existing_pgm_path, existing_yaml_path = write_occupancy_to_yaml_pgm(occ, tmpdir, base_name="demo_existing")
            existing_pbstream_path = os.path.join(tmpdir, "demo_existing.pbstream")
            with open(existing_pbstream_path, "wb") as fh:
                fh.write(b"pbstream")
            existing_asset, _ = register_imported_map_asset(
                store,
                map_name="demo",
                occ=occ,
                yaml_path=existing_yaml_path,
                pgm_path=existing_pgm_path,
                pbstream_path=existing_pbstream_path,
                robot_id="robot_a",
                verification_mode="offline_verified",
                set_active=True,
            )

            node = MAP_ASSET_SERVICE_MODULE.MapAssetServiceNode.__new__(MAP_ASSET_SERVICE_MODULE.MapAssetServiceNode)
            node.robot_id = "robot_a"
            node.external_maps_root = external_root
            node.maps_root = maps_root
            node.store = store

            with mock.patch.object(MAP_ASSET_SERVICE_MODULE, "yaml_pgm_to_occupancy", return_value=_make_occ()):
                imported_asset = node._import_external_map(map_name="demo", set_active=False, description="candidate")

            self.assertNotEqual(str(imported_asset.get("revision_id") or ""), str(existing_asset.get("revision_id") or ""))
            self.assertEqual(str(imported_asset.get("verification_status") or ""), "pending")
            self.assertIn(
                os.path.join("revisions", "demo", str(imported_asset.get("revision_id") or "")),
                str(imported_asset.get("pbstream_path") or ""),
            )
            active = store.get_active_map(robot_id="robot_a") or {}
            self.assertEqual(str(active.get("revision_id") or ""), str(existing_asset.get("revision_id") or ""))

    def test_map_asset_service_modify_defaults_to_active_revision_view(self):
        class _FakeStore(object):
            def __init__(self):
                self.update_calls = []

            def get_active_map(self, robot_id="local_robot"):
                del robot_id
                return {
                    "map_name": "demo_map",
                    "revision_id": "rev_demo_active",
                    "description": "stable",
                    "enabled": True,
                    "map_id": "map_active",
                    "map_md5": "md5_active",
                    "lifecycle_status": "available",
                    "verification_status": "verified",
                }

            def resolve_map_asset(self, *, map_name="", revision_id="", robot_id="local_robot"):
                del robot_id
                if str(revision_id or "") == "rev_demo_active":
                    return {
                        "map_name": "demo_map",
                        "revision_id": "rev_demo_active",
                        "description": "stable",
                        "enabled": True,
                        "map_id": "map_active",
                        "map_md5": "md5_active",
                        "lifecycle_status": "available",
                        "verification_status": "verified",
                    }
                if str(map_name or "") == "demo_map":
                    return {
                        "map_name": "demo_map",
                        "revision_id": "rev_demo_head",
                        "description": "candidate",
                        "enabled": True,
                        "map_id": "",
                        "map_md5": "",
                        "lifecycle_status": "saved_unverified",
                        "verification_status": "pending",
                    }
                return None

            def update_map_revision_meta(self, **kwargs):
                self.update_calls.append(dict(kwargs))
                return {
                    "map_name": "demo_map",
                    "revision_id": str(kwargs.get("revision_id") or ""),
                    "description": "stable updated",
                    "enabled": True,
                    "map_id": "map_active",
                    "map_md5": "md5_active",
                    "lifecycle_status": "available",
                    "verification_status": "verified",
                }

        req = type("Req", (), {})()
        req.get = 0
        req.add = 1
        req.modify = 2
        req.Delete = 3
        req.getAll = 4
        req.ENABLE_KEEP = 0
        req.ENABLE_DISABLE = 1
        req.ENABLE_ENABLE = 2
        req.operation = 2
        req.map_name = "demo_map"
        req.map = type("MapArg", (), {"map_name": "demo_map", "map_revision_id": "", "description": "stable updated"})()
        req.set_active = False
        req.enabled_state = 0

        node = MAP_ASSET_SERVICE_MODULE.MapAssetServiceNode.__new__(MAP_ASSET_SERVICE_MODULE.MapAssetServiceNode)
        node.robot_id = "robot_a"
        node.store = _FakeStore()

        resp = node._handle(req)

        self.assertTrue(bool(resp.success))
        self.assertEqual(str(resp.map.map_revision_id or ""), "rev_demo_active")
        self.assertEqual(node.store.update_calls[0]["revision_id"], "rev_demo_active")

    def test_map_asset_service_delete_explicit_candidate_revision_does_not_block_active_same_name(self):
        class _FakeStore(object):
            def __init__(self):
                self.disable_calls = []

            def get_active_map(self, robot_id="local_robot"):
                del robot_id
                return {
                    "map_name": "demo_map",
                    "revision_id": "rev_demo_active",
                    "description": "stable",
                    "enabled": True,
                    "map_id": "map_active",
                    "map_md5": "md5_active",
                    "lifecycle_status": "available",
                    "verification_status": "verified",
                }

            def resolve_map_asset(self, *, map_name="", revision_id="", robot_id="local_robot"):
                del robot_id
                if str(revision_id or "") == "rev_demo_active":
                    return {
                        "map_name": "demo_map",
                        "revision_id": "rev_demo_active",
                        "description": "stable",
                        "enabled": True,
                        "map_id": "map_active",
                        "map_md5": "md5_active",
                        "lifecycle_status": "available",
                        "verification_status": "verified",
                    }
                if str(revision_id or "") == "rev_demo_head":
                    return {
                        "map_name": "demo_map",
                        "revision_id": "rev_demo_head",
                        "description": "candidate",
                        "enabled": True,
                        "map_id": "",
                        "map_md5": "",
                        "lifecycle_status": "saved_unverified",
                        "verification_status": "pending",
                    }
                if str(map_name or "") == "demo_map":
                    return {
                        "map_name": "demo_map",
                        "revision_id": "rev_demo_head",
                        "description": "candidate",
                        "enabled": True,
                        "map_id": "",
                        "map_md5": "",
                        "lifecycle_status": "saved_unverified",
                        "verification_status": "pending",
                    }
                return None

            def disable_map_revision(self, **kwargs):
                self.disable_calls.append(dict(kwargs))
                return {
                    "map_name": "demo_map",
                    "revision_id": str(kwargs.get("revision_id") or ""),
                    "description": "candidate",
                    "enabled": False,
                    "map_id": "",
                    "map_md5": "",
                    "lifecycle_status": "saved_unverified",
                    "verification_status": "pending",
                }

        req = type("Req", (), {})()
        req.get = 0
        req.add = 1
        req.modify = 2
        req.Delete = 3
        req.getAll = 4
        req.ENABLE_KEEP = 0
        req.ENABLE_DISABLE = 1
        req.ENABLE_ENABLE = 2
        req.operation = 3
        req.map_name = ""
        req.map = type("MapArg", (), {"map_name": "", "map_revision_id": "rev_demo_head", "description": ""})()
        req.set_active = False
        req.enabled_state = 0

        node = MAP_ASSET_SERVICE_MODULE.MapAssetServiceNode.__new__(MAP_ASSET_SERVICE_MODULE.MapAssetServiceNode)
        node.robot_id = "robot_a"
        node.store = _FakeStore()

        resp = node._handle(req)

        self.assertTrue(bool(resp.success))
        self.assertEqual(str(resp.map.map_revision_id or ""), "rev_demo_head")
        self.assertEqual(node.store.disable_calls[0]["revision_id"], "rev_demo_head")
        self.assertFalse(bool(resp.map.is_active))
        self.assertEqual(str(resp.map.active_revision_id or ""), "rev_demo_active")

    def test_migrate_map_assets_set_active_prefers_revision_pointer(self):
        fake_store = mock.Mock()
        fake_store.list_map_assets_by_name.return_value = []
        fake_store.resolve_map_asset.return_value = None
        fake_store.get_active_map.return_value = None
        fake_store.set_active_map_revision = mock.Mock()
        fake_store.set_active_map = mock.Mock()
        fake_store.backfill_unscoped_map_scope = mock.Mock()
        fake_store.generate_map_revision_id.return_value = "rev_demo_import"

        with tempfile.TemporaryDirectory() as tmpdir:
            src_dir = os.path.join(tmpdir, "src")
            maps_root = os.path.join(tmpdir, "managed")
            os.makedirs(src_dir, exist_ok=True)
            yaml_path = os.path.join(src_dir, "demo.yaml")
            with open(yaml_path, "w", encoding="utf-8") as fh:
                fh.write("image: demo.pgm\nresolution: 0.05\norigin: [0.0, 0.0, 0.0]\n")
            argv = [
                "migrate_map_assets.py",
                "--plan-db-path",
                os.path.join(tmpdir, "planning.db"),
                "--ops-db-path",
                os.path.join(tmpdir, "operations.db"),
                "--maps-root",
                maps_root,
                "--src-glob",
                os.path.join(src_dir, "*.yaml"),
                "--verification-mode",
                "offline_verified",
                "--set-active",
            ]
            imported_asset = {
                "map_name": "demo",
                "revision_id": "rev_demo_01",
                "map_id": "map_demo_01",
                "map_md5": "demo_md5_01",
            }

            with mock.patch.object(MIGRATE_MAP_ASSETS_MODULE, "PlanStore", return_value=fake_store), mock.patch.object(
                MIGRATE_MAP_ASSETS_MODULE, "OperationsStore", return_value=mock.Mock()
            ), mock.patch.object(
                MIGRATE_MAP_ASSETS_MODULE, "yaml_pgm_to_occupancy", return_value=_make_occ()
            ), mock.patch.object(
                MIGRATE_MAP_ASSETS_MODULE,
                "write_occupancy_to_yaml_pgm",
                return_value=(os.path.join(maps_root, "demo.pgm"), os.path.join(maps_root, "demo.yaml")),
            ), mock.patch.object(
                MIGRATE_MAP_ASSETS_MODULE,
                "register_imported_map_asset",
                return_value=(imported_asset, "demo_snapshot_md5"),
            ), mock.patch.object(sys, "argv", argv):
                MIGRATE_MAP_ASSETS_MODULE.main()

        fake_store.set_active_map_revision.assert_called_once_with(
            revision_id="rev_demo_01",
            robot_id="local_robot",
        )
        fake_store.set_active_map.assert_not_called()

    def test_migrate_map_assets_allows_same_name_new_revision_with_revision_scoped_paths(self):
        fake_store = mock.Mock()
        fake_store.list_map_assets_by_name.return_value = []
        fake_store.resolve_map_asset.return_value = {
            "map_name": "demo",
            "revision_id": "rev_demo_old",
            "map_id": "map_demo_old",
            "map_md5": "demo_md5_old",
        }
        fake_store.get_active_map.return_value = None
        fake_store.generate_map_revision_id.return_value = "rev_demo_new"
        fake_store.backfill_unscoped_map_scope = mock.Mock()

        with tempfile.TemporaryDirectory() as tmpdir:
            src_dir = os.path.join(tmpdir, "src")
            maps_root = os.path.join(tmpdir, "managed")
            os.makedirs(src_dir, exist_ok=True)
            yaml_path = os.path.join(src_dir, "demo.yaml")
            with open(yaml_path, "w", encoding="utf-8") as fh:
                fh.write("image: demo.pgm\nresolution: 0.05\norigin: [0.0, 0.0, 0.0]\n")
            argv = [
                "migrate_map_assets.py",
                "--plan-db-path",
                os.path.join(tmpdir, "planning.db"),
                "--ops-db-path",
                os.path.join(tmpdir, "operations.db"),
                "--maps-root",
                maps_root,
                "--src-glob",
                os.path.join(src_dir, "*.yaml"),
            ]
            imported_asset = {
                "map_name": "demo",
                "revision_id": "rev_demo_new",
                "map_id": "map_demo_new",
                "map_md5": "demo_md5_new",
            }
            expected_out_root = os.path.join(maps_root, "revisions", "demo", "rev_demo_new")

            with mock.patch.object(MIGRATE_MAP_ASSETS_MODULE, "PlanStore", return_value=fake_store), mock.patch.object(
                MIGRATE_MAP_ASSETS_MODULE, "OperationsStore", return_value=mock.Mock()
            ), mock.patch.object(
                MIGRATE_MAP_ASSETS_MODULE, "yaml_pgm_to_occupancy", return_value=_make_occ()
            ), mock.patch.object(
                MIGRATE_MAP_ASSETS_MODULE,
                "write_occupancy_to_yaml_pgm",
                return_value=(os.path.join(expected_out_root, "demo.pgm"), os.path.join(expected_out_root, "demo.yaml")),
            ) as write_occ, mock.patch.object(
                MIGRATE_MAP_ASSETS_MODULE,
                "register_imported_map_asset",
                return_value=(imported_asset, "demo_snapshot_md5"),
            ) as register_asset, mock.patch.object(sys, "argv", argv):
                MIGRATE_MAP_ASSETS_MODULE.main()

        write_occ.assert_called_once_with(mock.ANY, expected_out_root, base_name="demo")
        self.assertEqual(register_asset.call_args.kwargs["revision_id"], "rev_demo_new")

    def test_import_map_assets_allows_same_name_new_revision_with_revision_scoped_paths(self):
        fake_store = mock.Mock()
        fake_store.list_map_assets_by_name.return_value = []
        fake_store.resolve_map_asset.return_value = {
            "map_name": "demo",
            "revision_id": "rev_demo_old",
            "map_id": "map_demo_old",
            "map_md5": "demo_md5_old",
        }
        fake_store.generate_map_revision_id.return_value = "rev_demo_new"
        fake_store.backfill_unscoped_map_scope = mock.Mock()

        with tempfile.TemporaryDirectory() as tmpdir:
            src_dir = os.path.join(tmpdir, "src")
            maps_root = os.path.join(tmpdir, "managed")
            os.makedirs(src_dir, exist_ok=True)
            yaml_path = os.path.join(src_dir, "demo.yaml")
            with open(yaml_path, "w", encoding="utf-8") as fh:
                fh.write("image: demo.pgm\nresolution: 0.05\norigin: [0.0, 0.0, 0.0]\n")
            argv = [
                "import_map_assets.py",
                "--plan-db-path",
                os.path.join(tmpdir, "planning.db"),
                "--maps-root",
                maps_root,
                "--src-glob",
                os.path.join(src_dir, "*.yaml"),
            ]
            imported_asset = {
                "map_name": "demo",
                "revision_id": "rev_demo_new",
                "map_id": "",
                "map_md5": "",
            }
            expected_out_root = os.path.join(maps_root, "revisions", "demo", "rev_demo_new")

            with mock.patch.object(IMPORT_MAP_ASSETS_MODULE, "PlanStore", return_value=fake_store), mock.patch.object(
                IMPORT_MAP_ASSETS_MODULE, "yaml_pgm_to_occupancy", return_value=_make_occ()
            ), mock.patch.object(
                IMPORT_MAP_ASSETS_MODULE,
                "write_occupancy_to_yaml_pgm",
                return_value=(os.path.join(expected_out_root, "demo.pgm"), os.path.join(expected_out_root, "demo.yaml")),
            ) as write_occ, mock.patch.object(
                IMPORT_MAP_ASSETS_MODULE,
                "register_imported_map_asset",
                return_value=(imported_asset, "demo_snapshot_md5"),
            ) as register_asset, mock.patch.object(sys, "argv", argv):
                IMPORT_MAP_ASSETS_MODULE.main()

        write_occ.assert_called_once_with(mock.ANY, expected_out_root, base_name="demo")
        self.assertEqual(register_asset.call_args.kwargs["revision_id"], "rev_demo_new")


if __name__ == "__main__":
    unittest.main()

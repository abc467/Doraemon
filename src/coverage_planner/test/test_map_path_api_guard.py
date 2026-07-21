#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import importlib.util
import os
import sys
import tempfile
import unittest
from types import SimpleNamespace

import yaml
from nav_msgs.msg import OccupancyGrid


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
PKG_DIR = os.path.dirname(THIS_DIR)
SRC_DIR = os.path.join(PKG_DIR, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from coverage_planner.slam_workflow.api_submit import SlamApiSubmitController  # noqa: E402
from coverage_planner.slam_workflow.runtime_transport import CartographerRuntimeTransport  # noqa: E402
from coverage_planner.map_io import write_occupancy_to_yaml_pgm, yaml_pgm_to_occupancy  # noqa: E402
from coverage_planner.map_path_security import MapPathSecurityError  # noqa: E402


class _Message(object):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


def _load_map_asset_service_node():
    path = os.path.join(PKG_DIR, "scripts", "map_asset_service_node.py")
    spec = importlib.util.spec_from_file_location("map_path_guard_map_asset_service", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module.MapAssetServiceNode


MapAssetServiceNode = _load_map_asset_service_node()


class SlamApiMapPathGuardTest(unittest.TestCase):
    def setUp(self):
        backend = SimpleNamespace(
            robot_id="local_robot",
            _runtime_client=object(),
            _state_controller=object(),
        )
        self.controller = SlamApiSubmitController(backend)

    @staticmethod
    def _request(*, map_name="", save_map_name="", map_revision_id=""):
        return SimpleNamespace(
            robot_id="local_robot",
            operation=3,
            map_name=map_name,
            save_map_name=save_map_name,
            map_revision_id=map_revision_id,
        )

    def _submit(self, req):
        return self.controller._handle_submit_command(
            req,
            response_cls=_Message,
            job_cls=_Message,
            job_converter=lambda value: value,
        )

    def test_rejects_map_name_with_controlled_error_before_runtime_access(self):
        response = self._submit(self._request(map_name="../../escape"))
        self.assertFalse(response.accepted)
        self.assertEqual(response.error_code, "invalid_map_name")
        self.assertEqual(response.job_id, "")

    def test_rejects_save_map_name_with_controlled_error(self):
        response = self._submit(self._request(save_map_name="/tmp/escape"))
        self.assertFalse(response.accepted)
        self.assertEqual(response.error_code, "invalid_map_name")

    def test_rejects_revision_id_with_controlled_error(self):
        response = self._submit(self._request(map_revision_id="rev/escape"))
        self.assertFalse(response.accepted)
        self.assertEqual(response.error_code, "invalid_map_revision_id")


class MapAssetApiPathGuardTest(unittest.TestCase):
    def test_invalid_map_name_is_a_controlled_map_service_response(self):
        for map_name in ("../../escape", "demo\n", "\tdemo"):
            with self.subTest(map_name=repr(map_name)):
                node = MapAssetServiceNode.__new__(MapAssetServiceNode)
                req = SimpleNamespace(
                    operation=0,
                    map_name=map_name,
                    map=SimpleNamespace(map_name="", map_revision_id=""),
                )
                response = node._handle(req, response_cls=_Message, map_cls=_Message)
                self.assertFalse(response.success)
                self.assertIn("invalid_map_name", response.message)


class RuntimeTransportMapSinkGuardTest(unittest.TestCase):
    def _transport(self, maps_root):
        context = SimpleNamespace(runtime_param=lambda key: "/cartographer/runtime/" + key)
        backend = SimpleNamespace(maps_root=maps_root, _runtime_context=context)
        return CartographerRuntimeTransport(backend)

    def test_rejects_out_of_root_and_non_pbstream_sink_before_ros_service_lookup(self):
        with tempfile.TemporaryDirectory() as maps_root, tempfile.TemporaryDirectory() as outside:
            transport = self._transport(maps_root)
            ok, message = transport.save_pbstream(os.path.join(outside, "escape.pbstream"))
            self.assertFalse(ok)
            self.assertIn("invalid_pbstream_path", message)
            ok, message = transport.save_pbstream(os.path.join(maps_root, "wrong.txt"))
            self.assertFalse(ok)
            self.assertIn("invalid_pbstream_path", message)

    def test_rejects_dangling_and_fifo_sink_before_ros_service_lookup(self):
        with tempfile.TemporaryDirectory() as maps_root:
            transport = self._transport(maps_root)
            dangling = os.path.join(maps_root, "dangling.pbstream")
            os.symlink(os.path.join(maps_root, "missing.pbstream"), dangling)
            fifo = os.path.join(maps_root, "fifo.pbstream")
            os.mkfifo(fifo)
            for path in (dangling, fifo):
                with self.subTest(path=path):
                    ok, message = transport.save_pbstream(path)
                    self.assertFalse(ok)
                    self.assertIn("invalid_pbstream_path", message)


class MapYamlContainmentIntegrationTest(unittest.TestCase):
    @staticmethod
    def _occupancy():
        msg = OccupancyGrid()
        msg.header.frame_id = "map"
        msg.info.resolution = 0.05
        msg.info.width = 1
        msg.info.height = 1
        msg.info.origin.orientation.w = 1.0
        msg.data = [0]
        return msg

    def test_generated_relative_yaml_round_trips_and_absolute_image_is_rejected(self):
        with tempfile.TemporaryDirectory() as maps_root:
            artifact_dir = os.path.join(maps_root, "revisions", "demo", "rev_demo_01")
            pgm_path, yaml_path = write_occupancy_to_yaml_pgm(
                self._occupancy(),
                artifact_dir,
                base_name="demo",
                allowed_root=maps_root,
            )
            loaded = yaml_pgm_to_occupancy(yaml_path, allowed_root=maps_root)
            self.assertEqual(loaded.info.width, 1)
            self.assertEqual(loaded.info.height, 1)

            with open(yaml_path, "r", encoding="utf-8") as handle:
                metadata = yaml.safe_load(handle)
            metadata["image"] = pgm_path
            with open(yaml_path, "w", encoding="utf-8") as handle:
                yaml.safe_dump(metadata, handle)
            with self.assertRaises(MapPathSecurityError):
                yaml_pgm_to_occupancy(yaml_path, allowed_root=maps_root)


if __name__ == "__main__":
    unittest.main()

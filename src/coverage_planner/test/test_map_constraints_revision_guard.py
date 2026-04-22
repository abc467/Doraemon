#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import importlib.util
import pathlib
import threading
import unittest
from types import SimpleNamespace
from unittest import mock


def _load_module():
    script_path = pathlib.Path(__file__).resolve().parents[1] / "scripts" / "map_constraints_node.py"
    spec = importlib.util.spec_from_file_location("map_constraints_guard_test_mod", str(script_path))
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


MAP_CONSTRAINTS_MODULE = _load_module()


class _FakeStore:
    def __init__(self, active_asset):
        self.active_asset = dict(active_asset)
        self.load_calls = []

    def get_active_map(self, *, robot_id: str):
        del robot_id
        return dict(self.active_asset)

    def load_map_constraints(self, **kwargs):
        self.load_calls.append(dict(kwargs))
        return {
            "constraint_version": "constraint_v1",
            "no_go_areas": [],
            "virtual_walls": [],
            "map_md5": str(kwargs.get("map_md5_hint") or ""),
        }


class _FakePublisher:
    def __init__(self):
        self.messages = []

    def publish(self, msg):
        self.messages.append(msg)


class MapConstraintsRevisionGuardTest(unittest.TestCase):
    def _make_node(self, active_asset):
        node = MAP_CONSTRAINTS_MODULE.MapConstraintsNode.__new__(MAP_CONSTRAINTS_MODULE.MapConstraintsNode)
        node.robot_id = "local_robot"
        node.store = _FakeStore(active_asset)
        node._store_lock = threading.RLock()
        node.pub = _FakePublisher()
        node.marker_pub = _FakePublisher()
        node.publish_markers = False
        node.default_virtual_wall_buffer_m = 0.2
        node._last_key = ("", "", "", "")
        node._resolve_map_identity = lambda: ("map_runtime", "runtime_md5_other")
        node._build_messages = lambda compiled: (
            SimpleNamespace(
                map_id=str(compiled.map_id or ""),
                map_md5=str(compiled.map_md5 or ""),
                constraint_version=str(compiled.constraint_version or ""),
                no_go_polygons=[],
                virtual_wall_keepouts=[],
            ),
            SimpleNamespace(markers=[]),
        )
        return node

    @mock.patch.object(MAP_CONSTRAINTS_MODULE.rospy, "loginfo")
    @mock.patch.object(MAP_CONSTRAINTS_MODULE.rospy, "logwarn_throttle")
    @mock.patch.object(MAP_CONSTRAINTS_MODULE, "get_runtime_map_revision_id", return_value="rev_demo_01")
    @mock.patch.object(MAP_CONSTRAINTS_MODULE, "compile_map_constraints")
    def test_publish_if_needed_prefers_revision_match_over_md5_mismatch(
        self,
        compile_constraints,
        _runtime_revision,
        _logwarn,
        _loginfo,
    ):
        node = self._make_node(
            {
                "revision_id": "rev_demo_01",
                "map_id": "map_active",
                "map_md5": "active_md5",
            }
        )
        compile_constraints.return_value = SimpleNamespace(
            map_id="map_active",
            map_md5="active_md5",
            constraint_version="constraint_v1",
            no_go_polygons=[],
            virtual_wall_keepouts=[],
        )

        node._publish_if_needed(force=True)

        self.assertEqual(len(node.pub.messages), 1)
        self.assertEqual(node.store.load_calls[-1]["map_revision_id"], "rev_demo_01")
        self.assertEqual(node.store.load_calls[-1]["map_md5_hint"], "active_md5")

    @mock.patch.object(MAP_CONSTRAINTS_MODULE.rospy, "logwarn_throttle")
    @mock.patch.object(MAP_CONSTRAINTS_MODULE, "get_runtime_map_revision_id", return_value="rev_other_02")
    @mock.patch.object(MAP_CONSTRAINTS_MODULE, "compile_map_constraints")
    def test_publish_if_needed_blocks_revision_mismatch_before_md5_fallback(
        self,
        compile_constraints,
        _runtime_revision,
        _logwarn,
    ):
        node = self._make_node(
            {
                "revision_id": "rev_demo_01",
                "map_id": "map_active",
                "map_md5": "active_md5",
            }
        )
        compile_constraints.return_value = SimpleNamespace(
            map_id="map_active",
            map_md5="active_md5",
            constraint_version="constraint_v1",
            no_go_polygons=[],
            virtual_wall_keepouts=[],
        )

        node._publish_if_needed(force=True)

        self.assertEqual(node.pub.messages, [])
        self.assertEqual(node.store.load_calls, [])


if __name__ == "__main__":
    unittest.main()

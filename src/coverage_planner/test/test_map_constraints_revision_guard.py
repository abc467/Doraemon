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
    def test_hard_and_effective_topics_must_resolve_differently(self):
        with mock.patch.object(
            MAP_CONSTRAINTS_MODULE.rospy,
            "resolve_name",
            return_value="/map_constraints/same",
        ):
            with self.assertRaises(ValueError):
                MAP_CONSTRAINTS_MODULE._validate_distinct_constraint_topics(
                    "/map_constraints/current",
                    "/map_constraints/effective",
                )

    def _make_node(self, active_asset):
        node = MAP_CONSTRAINTS_MODULE.MapConstraintsNode.__new__(MAP_CONSTRAINTS_MODULE.MapConstraintsNode)
        node.robot_id = "local_robot"
        node.store = _FakeStore(active_asset)
        node._store_lock = threading.RLock()
        node.pub = _FakePublisher()
        node.effective_pub = _FakePublisher()
        node.marker_pub = _FakePublisher()
        node.frame_id = "map"
        node.publish_markers = False
        node.default_virtual_wall_buffer_m = 0.2
        node.planning_default_no_go_buffer_m = 0.11
        node.planning_no_go_long_edge_normal_buffer_m = 0.22
        node.planning_no_go_short_edge_normal_buffer_m = 0.33
        node._last_key = ("", "", "", "")
        node._last_hard_msg = None
        node._last_effective_msg = None
        node._resolve_map_identity = lambda: ("map_runtime", "runtime_md5_other")
        node._build_constraint_message = lambda compiled, _stamp, **scope: SimpleNamespace(
            header=SimpleNamespace(stamp=_stamp),
            valid=True,
            invalid_reason="",
            map_revision_id=str(scope.get("map_revision_id") or ""),
            runtime_map_id=str(scope.get("runtime_map_id") or ""),
            runtime_map_md5=str(scope.get("runtime_map_md5") or ""),
            map_id=str(compiled.map_id or ""),
            map_md5=str(compiled.map_md5 or ""),
            constraint_version=str(compiled.constraint_version or ""),
            no_go_polygons=[],
            virtual_wall_keepouts=[],
        )
        node._build_markers = lambda _hard, _effective, _stamp: SimpleNamespace(markers=[])
        return node

    def test_messages_share_identity_and_markers_expose_both_no_go_semantics(self):
        raw_no_go = [
            {
                "area_id": "nogo_rect",
                "polygon": [[3.0, 1.5], [7.0, 1.5], [7.0, 3.5], [3.0, 3.5]],
                "enabled": True,
            }
        ]
        compile_kwargs = {
            "map_id": "map_demo",
            "map_md5": "demo-md5",
            "constraint_version": "constraint-v1",
            "no_go_areas": raw_no_go,
            "virtual_walls": [],
        }
        hard = MAP_CONSTRAINTS_MODULE.compile_navigation_map_constraints(**compile_kwargs)
        effective = MAP_CONSTRAINTS_MODULE.compile_map_constraints(
            **compile_kwargs,
            default_no_go_long_edge_normal_buffer_m=0.18,
            default_no_go_short_edge_normal_buffer_m=0.40,
        )
        node = MAP_CONSTRAINTS_MODULE.MapConstraintsNode.__new__(
            MAP_CONSTRAINTS_MODULE.MapConstraintsNode
        )
        node.frame_id = "map"
        node.publish_markers = True
        stamp = MAP_CONSTRAINTS_MODULE.rospy.Time.from_sec(123.0)

        hard_msg = node._build_constraint_message(hard, stamp)
        effective_msg = node._build_constraint_message(effective, stamp)
        markers = node._build_markers(hard, effective, stamp)

        self.assertEqual(hard_msg.map_id, effective_msg.map_id)
        self.assertEqual(hard_msg.map_md5, effective_msg.map_md5)
        self.assertEqual(hard_msg.constraint_version, effective_msg.constraint_version)
        self.assertEqual(hard_msg.header.stamp, effective_msg.header.stamp)
        self.assertTrue(hard_msg.valid)
        self.assertEqual(len(markers.markers), 3)
        self.assertEqual(markers.markers[0].action, MAP_CONSTRAINTS_MODULE.Marker.DELETEALL)
        self.assertEqual(markers.markers[1].ns, "no_go")
        self.assertEqual(markers.markers[2].ns, "no_go_effective_planning")

    @mock.patch.object(MAP_CONSTRAINTS_MODULE.rospy, "loginfo")
    @mock.patch.object(MAP_CONSTRAINTS_MODULE.rospy, "logwarn_throttle")
    @mock.patch.object(MAP_CONSTRAINTS_MODULE, "get_runtime_map_revision_id", return_value="rev_demo_01")
    @mock.patch.object(MAP_CONSTRAINTS_MODULE, "compile_map_constraints")
    @mock.patch.object(MAP_CONSTRAINTS_MODULE, "compile_navigation_map_constraints")
    def test_publish_if_needed_prefers_revision_match_over_md5_mismatch(
        self,
        compile_navigation_constraints,
        compile_planning_constraints,
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
        compile_navigation_constraints.return_value = SimpleNamespace(
            map_id="map_active",
            map_md5="active_md5",
            constraint_version="constraint_v1",
            no_go_polygons=[],
            virtual_wall_keepouts=[],
        )
        compile_planning_constraints.return_value = SimpleNamespace(
            map_id="map_active",
            map_md5="active_md5",
            constraint_version="constraint_v1",
            no_go_polygons=[],
            virtual_wall_keepouts=[],
        )

        with mock.patch.object(
            MAP_CONSTRAINTS_MODULE.rospy.Time,
            "now",
            return_value=MAP_CONSTRAINTS_MODULE.rospy.Time.from_sec(123.0),
        ):
            node._publish_if_needed(force=True)

        self.assertEqual(len(node.pub.messages), 1)
        self.assertEqual(len(node.effective_pub.messages), 1)
        self.assertTrue(node.pub.messages[0].valid)
        self.assertEqual(node.pub.messages[0].map_revision_id, "rev_demo_01")
        self.assertEqual(node.pub.messages[0].runtime_map_id, "map_runtime")
        self.assertEqual(
            node.pub.messages[0].runtime_map_md5,
            "runtime_md5_other",
        )
        self.assertEqual(node.store.load_calls[-1]["map_revision_id"], "rev_demo_01")
        self.assertEqual(node.store.load_calls[-1]["map_md5_hint"], "active_md5")
        hard_kwargs = compile_navigation_constraints.call_args.kwargs
        self.assertEqual(hard_kwargs["default_virtual_wall_buffer_m"], 0.2)
        self.assertNotIn("default_no_go_buffer_m", hard_kwargs)
        self.assertNotIn("default_no_go_long_edge_normal_buffer_m", hard_kwargs)
        self.assertNotIn("default_no_go_short_edge_normal_buffer_m", hard_kwargs)
        planning_kwargs = compile_planning_constraints.call_args.kwargs
        self.assertEqual(planning_kwargs["default_buffer_m"], 0.2)
        self.assertEqual(planning_kwargs["default_no_go_buffer_m"], 0.11)
        self.assertEqual(
            planning_kwargs["default_no_go_long_edge_normal_buffer_m"],
            0.22,
        )
        self.assertEqual(
            planning_kwargs["default_no_go_short_edge_normal_buffer_m"],
            0.33,
        )

    @mock.patch.object(MAP_CONSTRAINTS_MODULE.rospy, "logwarn_throttle")
    @mock.patch.object(MAP_CONSTRAINTS_MODULE, "get_runtime_map_revision_id", return_value="rev_other_02")
    @mock.patch.object(MAP_CONSTRAINTS_MODULE, "compile_map_constraints")
    @mock.patch.object(MAP_CONSTRAINTS_MODULE, "compile_navigation_map_constraints")
    def test_revision_mismatch_publishes_invalid_tombstone_before_md5_fallback(
        self,
        compile_navigation_constraints,
        compile_planning_constraints,
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
        compile_navigation_constraints.return_value = SimpleNamespace(
            map_id="map_active",
            map_md5="active_md5",
            constraint_version="constraint_v1",
            no_go_polygons=[],
            virtual_wall_keepouts=[],
        )
        compile_planning_constraints.return_value = SimpleNamespace(
            map_id="map_active",
            map_md5="active_md5",
            constraint_version="constraint_v1",
            no_go_polygons=[],
            virtual_wall_keepouts=[],
        )

        with mock.patch.object(
            MAP_CONSTRAINTS_MODULE.rospy.Time,
            "now",
            return_value=MAP_CONSTRAINTS_MODULE.rospy.Time.from_sec(123.0),
        ):
            node._publish_if_needed(force=True)

        self.assertEqual(len(node.pub.messages), 1)
        self.assertEqual(len(node.effective_pub.messages), 1)
        self.assertFalse(node.pub.messages[0].valid)
        self.assertIn("mismatches", node.pub.messages[0].invalid_reason)
        self.assertEqual(node.pub.messages[0].map_revision_id, "rev_other_02")
        self.assertEqual(node.store.load_calls, [])
        compile_navigation_constraints.assert_not_called()
        compile_planning_constraints.assert_not_called()

    @mock.patch.object(MAP_CONSTRAINTS_MODULE.rospy, "loginfo")
    @mock.patch.object(MAP_CONSTRAINTS_MODULE.rospy, "logwarn_throttle")
    @mock.patch.object(MAP_CONSTRAINTS_MODULE, "get_runtime_map_revision_id", return_value="rev_demo_01")
    @mock.patch.object(MAP_CONSTRAINTS_MODULE, "compile_map_constraints")
    @mock.patch.object(MAP_CONSTRAINTS_MODULE, "compile_navigation_map_constraints")
    def test_unchanged_valid_snapshot_is_republished_as_a_heartbeat(
        self,
        compile_navigation_constraints,
        compile_planning_constraints,
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
        compiled = SimpleNamespace(
            map_id="map_active",
            map_md5="active_md5",
            constraint_version="constraint_v1",
            no_go_polygons=[],
            virtual_wall_keepouts=[],
        )
        compile_navigation_constraints.return_value = compiled
        compile_planning_constraints.return_value = compiled

        with mock.patch.object(
            MAP_CONSTRAINTS_MODULE.rospy.Time,
            "now",
            return_value=MAP_CONSTRAINTS_MODULE.rospy.Time.from_sec(123.0),
        ):
            node._publish_if_needed(force=True)
            first_message = node.pub.messages[-1]
            node._publish_if_needed(force=False)

        self.assertEqual(len(node.pub.messages), 2)
        self.assertIs(node.pub.messages[-1], first_message)
        self.assertTrue(node.pub.messages[-1].valid)


if __name__ == "__main__":
    unittest.main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import importlib.util
import pathlib
import unittest
from unittest import mock


def _load_module():
    script_path = pathlib.Path(__file__).resolve().parents[1] / "scripts" / "rect_zone_planner_node.py"
    spec = importlib.util.spec_from_file_location("rect_zone_planner_guard_test_mod", str(script_path))
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


RECT_ZONE_MODULE = _load_module()


class _FakeStore:
    def __init__(self, active_asset):
        self.active_asset = dict(active_asset)

    def get_active_map(self, *, robot_id: str):
        del robot_id
        return dict(self.active_asset)

    def get_zone_meta(self, zone_id, *, map_name="", map_revision_id=""):
        del zone_id, map_name, map_revision_id
        return None


class _FakePlanClient:
    def __init__(self, wait_for_server_result=False):
        self.wait_for_server_result = bool(wait_for_server_result)
        self.sent_goals = []

    def wait_for_server(self, _duration):
        return bool(self.wait_for_server_result)

    def send_goal(self, goal):
        self.sent_goals.append(goal)


class RectZonePlannerRevisionGuardTest(unittest.TestCase):
    def _make_node(self, active_asset):
        node = RECT_ZONE_MODULE.RectZonePlannerNode.__new__(RECT_ZONE_MODULE.RectZonePlannerNode)
        node.robot_id = "local_robot"
        node.default_plan_profile_name = "cover_standard"
        node.map_topic = "/map"
        node.map_identity_timeout_s = 1.0
        node.map_frame = "map"
        node.action_connect_timeout_s = 1.0
        node.plan_action_name = "/coverage_planner_server/plan_coverage"
        node.store = _FakeStore(active_asset)
        node.plan_client = _FakePlanClient(wait_for_server_result=False)
        node.preview = {
            "valid": True,
            "message": "preview ok",
            "first_point": (0.0, 0.0),
            "second_point": (1.0, 1.0),
            "effective_outer": [(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0)],
            "geom_holes": [],
            "map_name": "demo_map",
            "map_revision_id": "rev_demo_01",
        }
        node._build_preview = lambda p0, p1: {
            "valid": True,
            "message": "preview ok",
            "first_point": tuple(p0),
            "second_point": tuple(p1),
            "effective_outer": [(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0)],
            "geom_holes": [],
        }
        node._publish_markers = lambda: None
        return node

    @mock.patch.object(RECT_ZONE_MODULE.rospy, "Duration", side_effect=lambda value: value)
    @mock.patch.object(RECT_ZONE_MODULE, "get_runtime_map_revision_id", return_value="rev_demo_01")
    @mock.patch.object(RECT_ZONE_MODULE, "ensure_map_identity", return_value=("map_runtime", "runtime_md5_other", True))
    def test_confirm_prefers_revision_match_over_md5_mismatch(
        self,
        _ensure_identity,
        _runtime_revision,
        _duration,
    ):
        node = self._make_node(
            {
                "map_name": "demo_map",
                "revision_id": "rev_demo_01",
                "map_id": "map_active",
                "map_md5": "active_md5",
            }
        )
        req = type("Req", (), {"zone_id": "zone_a", "profile_name": "", "debug_publish_markers": False})()

        resp = node._handle_confirm(req)

        self.assertFalse(resp.success)
        self.assertIn("planner action not available", str(resp.message or ""))

    @mock.patch.object(RECT_ZONE_MODULE, "get_runtime_map_revision_id", return_value="rev_other_02")
    @mock.patch.object(RECT_ZONE_MODULE, "ensure_map_identity", return_value=("map_runtime", "active_md5", True))
    def test_confirm_rejects_runtime_revision_mismatch_before_md5_fallback(
        self,
        _ensure_identity,
        _runtime_revision,
    ):
        node = self._make_node(
            {
                "map_name": "demo_map",
                "revision_id": "rev_demo_01",
                "map_id": "map_active",
                "map_md5": "active_md5",
            }
        )
        req = type("Req", (), {"zone_id": "zone_a", "profile_name": "", "debug_publish_markers": False})()

        resp = node._handle_confirm(req)

        self.assertFalse(resp.success)
        self.assertEqual(str(resp.message or ""), "runtime /map revision does not match selected map asset")


if __name__ == "__main__":
    unittest.main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import unittest


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
PKG_DIR = os.path.dirname(THIS_DIR)
SRC_DIR = os.path.join(PKG_DIR, "src")

if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from coverage_executor.plan_loader import PlanLoader


class _FakeStore:
    def __init__(self):
        self.zone_calls = []
        self.latest_calls = []
        self.active_calls = []
        self.constraint_calls = []

    def get_zone_meta(self, zone_id, *, map_name="", map_revision_id="", map_version=""):
        self.zone_calls.append((zone_id, map_name, map_revision_id, map_version))
        return {
            "zone_id": str(zone_id or ""),
            "map_name": str(map_name or "demo_map"),
            "map_revision_id": str(map_revision_id or "rev_demo_01"),
        }

    def get_latest_plan_id_by_profile(self, zone_id, plan_profile_name, zone_version=None, *, map_name="", map_revision_id=""):
        self.latest_calls.append((zone_id, plan_profile_name, zone_version, map_name, map_revision_id))
        return "plan_001"

    def get_active_plan_id(self, zone_id, plan_profile_name=None, *, map_name="", map_revision_id=""):
        self.active_calls.append((zone_id, plan_profile_name, map_name, map_revision_id))
        return ""

    def load_plan_meta(self, plan_id):
        return {
            "plan_id": str(plan_id or ""),
            "zone_id": "zone_a",
            "zone_version": 2,
            "frame_id": "map",
            "map_name": "demo_map",
            "map_revision_id": "rev_demo_01",
            "plan_profile_name": "cover_standard",
            "constraint_version": "constraint_v1",
            "blocks": 1,
            "total_length_m": 3.5,
            "map_id": "map_demo",
            "map_md5": "md5_demo",
            "planner_version": "planner_v1",
            "exec_order_json": [0],
        }

    def get_active_constraint_version(self, map_id, map_revision_id=""):
        self.constraint_calls.append((map_id, map_revision_id))
        return "constraint_v1"

    def load_block(self, plan_id, block_id):
        return {
            "block_id": int(block_id),
            "entry_x": 0.0,
            "entry_y": 0.0,
            "entry_yaw": 0.0,
            "exit_x": 1.0,
            "exit_y": 0.0,
            "exit_yaw": 0.0,
            "path_xyyaw": [(0.0, 0.0, 0.0), (1.0, 0.0, 0.0)],
            "length_m": 1.0,
            "point_count": 2,
        }


class PlanLoaderRevisionScopeTest(unittest.TestCase):
    def setUp(self):
        self.loader = PlanLoader.__new__(PlanLoader)
        self.loader.store = _FakeStore()

    def test_get_zone_meta_passes_map_revision_scope(self):
        zone = self.loader.get_zone_meta("zone_a", map_name="demo_map", map_revision_id="rev_demo_01")

        self.assertEqual(zone["map_revision_id"], "rev_demo_01")
        self.assertEqual(self.loader.store.zone_calls[-1], ("zone_a", "demo_map", "rev_demo_01", ""))

    def test_load_for_zone_prefers_revision_scoped_plan_lookup(self):
        plan = self.loader.load_for_zone(
            "zone_a",
            plan_profile_name="cover_standard",
            map_name="demo_map",
            map_revision_id="rev_demo_01",
        )

        self.assertEqual(plan.plan_id, "plan_001")
        self.assertEqual(plan.map_revision_id, "rev_demo_01")
        self.assertEqual(
            self.loader.store.latest_calls[-1],
            ("zone_a", "cover_standard", None, "demo_map", "rev_demo_01"),
        )

    def test_get_active_constraint_version_passes_revision_scope(self):
        version = self.loader.get_active_constraint_version("map_demo", "rev_demo_01")

        self.assertEqual(version, "constraint_v1")
        self.assertEqual(self.loader.store.constraint_calls[-1], ("map_demo", "rev_demo_01"))


if __name__ == "__main__":
    unittest.main()

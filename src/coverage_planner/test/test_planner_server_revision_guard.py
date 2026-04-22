#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import importlib.util
import pathlib
import unittest
from unittest import mock


def _load_module():
    script_path = pathlib.Path(__file__).resolve().parents[1] / "scripts" / "planner_server_node.py"
    spec = importlib.util.spec_from_file_location("planner_server_guard_test_mod", str(script_path))
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


PLANNER_SERVER_MODULE = _load_module()


class PlannerServerRevisionGuardTest(unittest.TestCase):
    @mock.patch.object(PLANNER_SERVER_MODULE, "get_runtime_map_revision_id", return_value="rev_demo_01")
    def test_active_asset_runtime_scope_prefers_revision_match_over_md5_mismatch(self, _runtime_revision):
        node = PLANNER_SERVER_MODULE.CoveragePlannerActionServer.__new__(PLANNER_SERVER_MODULE.CoveragePlannerActionServer)

        error = node._active_asset_runtime_scope_error(
            {
                "map_name": "demo_map",
                "revision_id": "rev_demo_01",
                "map_id": "map_active",
                "map_md5": "asset_md5",
            },
            map_id="map_runtime",
            map_md5="runtime_md5",
        )

        self.assertEqual(error, "")

    @mock.patch.object(PLANNER_SERVER_MODULE, "get_runtime_map_revision_id", return_value="rev_other_02")
    def test_active_asset_runtime_scope_rejects_revision_mismatch_before_md5_fallback(self, _runtime_revision):
        node = PLANNER_SERVER_MODULE.CoveragePlannerActionServer.__new__(PLANNER_SERVER_MODULE.CoveragePlannerActionServer)

        error = node._active_asset_runtime_scope_error(
            {
                "map_name": "demo_map",
                "revision_id": "rev_demo_01",
                "map_id": "map_active",
                "map_md5": "asset_md5",
            },
            map_id="map_active",
            map_md5="asset_md5",
        )

        self.assertEqual(
            error,
            "current /map does not match selected map asset: selected=demo_map runtime_revision=rev_other_02 asset_revision=rev_demo_01",
        )


if __name__ == "__main__":
    unittest.main()

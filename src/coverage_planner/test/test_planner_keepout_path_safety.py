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

try:
    import fields2cover  # noqa: F401

    _HAS_F2C = True
except Exception:
    _HAS_F2C = False

from coverage_planner.constraints import (
    _HAS_SHAPELY,
    compile_map_constraints,
    compile_zone_constraints,
    path_points_outside_effective_regions,
)
from coverage_planner.coverage_planner_core.planner import plan_coverage
from coverage_planner.coverage_planner_core.types import PlannerParams, RobotSpec


def _inside_rect(x, y, rect):
    minx, miny, maxx, maxy = rect
    return minx <= float(x) <= maxx and miny <= float(y) <= maxy


@unittest.skipUnless(_HAS_SHAPELY and _HAS_F2C, "Shapely and Fields2Cover are required")
class PlannerKeepoutPathSafetyTest(unittest.TestCase):
    def test_two_point_rect_no_go_touching_zone_edge_is_not_reentered_by_path(self):
        zone_outer = [
            [23.237, -4.921],
            [36.511, -4.921],
            [36.511, 0.189],
            [23.237, 0.189],
        ]
        no_go_outer = [
            [23.237, -4.977],
            [36.4, -4.977],
            [36.4, -3.477],
            [23.237, -3.477],
        ]
        original_no_go_rect = (23.237, -4.977, 36.4, -3.477)

        compiled = compile_map_constraints(
            map_id="map_a836c08e",
            map_md5="demo-md5",
            constraint_version="constraint-demo",
            no_go_areas=[
                {
                    "area_id": "nogo_rect",
                    "polygon": no_go_outer,
                    "enabled": True,
                }
            ],
            virtual_walls=[],
            default_no_go_buffer_m=0.575,
            prec=3,
        )
        zone_constraints = compile_zone_constraints(
            zone_outer=zone_outer,
            zone_holes=[],
            map_constraints=compiled,
            prec=3,
        )

        result = plan_coverage(
            frame_id="map",
            outer=zone_outer,
            holes=[],
            robot_spec=RobotSpec(cov_width=0.55, width=0.8, min_turning_radius=0.5),
            params=PlannerParams(
                path_step_m=0.05,
                turn_step_m=0.05,
                wall_margin_m=0.30,
                turn_margin_m=1.20,
                edge_corner_radius_m=0.50,
                edge_corner_pull=0.10,
                mute_stderr=True,
            ),
            effective_regions=zone_constraints.effective_regions,
        )

        self.assertTrue(result.ok, result.error_message)
        all_points = []
        for block in result.blocks or []:
            all_points.extend(list(block.path_xy or []))
            self.assertNotEqual(
                (block.stats or {}).get("edge_loop_skipped"),
                "bbox_edge_loop_rejected_by_region_guard",
                "path should be generated safely without bbox keepout leakage",
            )

        self.assertFalse(
            path_points_outside_effective_regions(all_points, zone_constraints.effective_regions),
            "planner returned points outside the keepout-filtered effective region",
        )
        offenders = [
            (round(float(x), 3), round(float(y), 3))
            for x, y in all_points
            if _inside_rect(x, y, original_no_go_rect)
        ]
        self.assertFalse(offenders[:8], offenders[:8])


if __name__ == "__main__":
    unittest.main()

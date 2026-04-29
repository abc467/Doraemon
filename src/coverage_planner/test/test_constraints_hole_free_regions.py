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

from coverage_planner.constraints import (
    _HAS_SHAPELY,
    compile_map_constraints,
    compile_zone_constraints,
    filter_effective_regions_for_planner,
    make_hole_free_effective_regions,
)
from coverage_planner.coverage_planner_core.types import PlannerParams, RobotSpec


def _ring_area(points):
    pts = list(points or [])
    if len(pts) < 3:
        return 0.0
    area = 0.0
    for idx, (x1, y1) in enumerate(pts):
        x2, y2 = pts[(idx + 1) % len(pts)]
        area += float(x1) * float(y2) - float(x2) * float(y1)
    return abs(area) * 0.5


def _ring_bounds(points):
    xs = [float(p[0]) for p in points or []]
    ys = [float(p[1]) for p in points or []]
    return min(xs), min(ys), max(xs), max(ys)


@unittest.skipUnless(_HAS_SHAPELY, "Shapely is required for constraint compilation")
class ConstraintHoleFreeRegionTest(unittest.TestCase):
    def test_inner_no_go_preserves_primary_holes_and_builds_degraded_regions(self):
        zone_outer = [
            [-0.347, -5.903],
            [9.578, -5.903],
            [9.578, -2.205],
            [-0.347, -2.205],
        ]
        no_go_outer = [
            [3.117, -4.673],
            [6.525, -4.673],
            [6.525, -3.644],
            [3.117, -3.644],
        ]
        map_constraints = compile_map_constraints(
            map_id="map_slam_map_20260424_144051",
            map_md5="demo-md5",
            constraint_version="constraints-demo",
            no_go_areas=[
                {
                    "area_id": "nogo_1777018394588",
                    "polygon": no_go_outer,
                    "enabled": True,
                }
            ],
            virtual_walls=[],
            prec=3,
        )

        zone_constraints = compile_zone_constraints(
            zone_outer=zone_outer,
            zone_holes=[],
            map_constraints=map_constraints,
            prec=3,
        )

        self.assertEqual(zone_constraints.constraint_version, "constraints-demo")
        self.assertEqual(len(zone_constraints.keepout_snapshot_rings), 1)
        self.assertEqual(len(zone_constraints.effective_regions), 1)
        self.assertEqual(len(zone_constraints.effective_regions[0].get("holes") or []), 1)

        primary_area = sum(
            _ring_area(region.get("outer") or [])
            - sum(_ring_area(hole) for hole in (region.get("holes") or []))
            for region in zone_constraints.effective_regions
        )
        expected_area = _ring_area(zone_outer) - _ring_area(no_go_outer)
        self.assertAlmostEqual(primary_area, expected_area, places=3)

        fallback_regions = make_hole_free_effective_regions(zone_constraints.effective_regions, prec=3)
        self.assertGreaterEqual(len(fallback_regions), 2)
        self.assertTrue(
            all(not region.get("holes") for region in fallback_regions),
            fallback_regions,
        )

        effective_area = sum(_ring_area(region.get("outer") or []) for region in fallback_regions)
        self.assertAlmostEqual(effective_area, expected_area, places=3)

        safe_regions, skipped, min_span_m = filter_effective_regions_for_planner(
            fallback_regions,
            RobotSpec(cov_width=0.55, width=0.8, min_turning_radius=0.5),
            PlannerParams(turn_margin_m=1.2),
        )
        self.assertAlmostEqual(min_span_m, 1.6, places=3)
        self.assertEqual(skipped, 2)
        self.assertEqual(len(safe_regions), len(fallback_regions) - 2)

        _, _, configured_min_span_m = filter_effective_regions_for_planner(
            fallback_regions,
            RobotSpec(cov_width=0.55, width=0.8, min_turning_radius=0.5),
            PlannerParams(turn_margin_m=1.2, min_plannable_span_m=1.4),
        )
        self.assertAlmostEqual(configured_min_span_m, 1.4, places=3)

    def test_planning_no_go_buffer_expands_rectangular_keepout(self):
        zone_outer = [[0.0, 0.0], [8.0, 0.0], [8.0, 5.0], [0.0, 5.0]]
        no_go_outer = [[3.0, 1.5], [5.0, 1.5], [5.0, 3.0], [3.0, 3.0]]
        map_constraints = compile_map_constraints(
            map_id="map_demo",
            map_md5="demo-md5",
            constraint_version="constraints-demo",
            no_go_areas=[
                {
                    "area_id": "nogo_rect",
                    "polygon": no_go_outer,
                    "enabled": True,
                }
            ],
            virtual_walls=[],
            default_no_go_buffer_m=0.5,
            prec=3,
        )

        zone_constraints = compile_zone_constraints(
            zone_outer=zone_outer,
            zone_holes=[],
            map_constraints=map_constraints,
            prec=3,
        )

        self.assertEqual(len(zone_constraints.keepout_snapshot_rings), 1)
        minx, miny, maxx, maxy = _ring_bounds(zone_constraints.keepout_snapshot_rings[0])
        self.assertAlmostEqual(minx, 2.5, places=3)
        self.assertAlmostEqual(miny, 1.0, places=3)
        self.assertAlmostEqual(maxx, 5.5, places=3)
        self.assertAlmostEqual(maxy, 3.5, places=3)
        self.assertEqual(len(zone_constraints.effective_regions), 1)
        self.assertEqual(len(zone_constraints.effective_regions[0].get("holes") or []), 1)


if __name__ == "__main__":
    unittest.main()

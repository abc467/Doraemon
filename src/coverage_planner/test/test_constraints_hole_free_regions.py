#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import math
import os
import sys
import unittest

try:
    from shapely.geometry import Polygon
    from shapely.ops import unary_union
except Exception:  # pragma: no cover - exercised only without test dependency
    Polygon = None
    unary_union = None


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
PKG_DIR = os.path.dirname(THIS_DIR)
SRC_DIR = os.path.join(PKG_DIR, "src")

if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from coverage_planner.constraints import (
    _HAS_SHAPELY,
    _geometry_to_region_list,
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
    def test_field_715_cross_boundary_keepout_stays_valid_after_quantization(self):
        zone_outer = [
            [-17.952219286719107, -1.323259016318092],
            [39.23789631563458, 1.294598388014979],
            [37.541556726363574, 38.35309403055115],
            [-19.648558875990116, 35.735236626218075],
        ]
        no_go_outer = [
            [-20.039, 19.675],
            [-11.339, 20.074],
            [-12.188, 38.633],
            [-20.888, 38.235],
        ]
        map_constraints = compile_map_constraints(
            map_id="field_715",
            map_md5="field-md5",
            constraint_version="field-constraints",
            no_go_areas=[{"area_id": "cross_boundary", "polygon": no_go_outer, "enabled": True}],
            virtual_walls=[],
            default_no_go_buffer_m=0.30,
            default_no_go_long_edge_normal_buffer_m=0.15,
            default_no_go_short_edge_normal_buffer_m=0.40,
            prec=3,
        )

        compiled = compile_zone_constraints(
            zone_outer=zone_outer,
            zone_holes=[],
            map_constraints=map_constraints,
            prec=3,
        )

        effective_polys = [
            Polygon(region["outer"], region.get("holes") or [])
            for region in compiled.effective_regions
        ]
        self.assertTrue(effective_polys)
        self.assertTrue(all(poly.is_valid for poly in effective_polys), compiled.effective_regions)

        # This rounded zone vertex was the extra backtracking point in the
        # field payload. A full-part difference excludes it from the effective
        # boundary; the clipped snapshot still records the actual keepout edge.
        self.assertNotIn((-19.649, 35.735), compiled.effective_regions[0]["outer"])
        self.assertIn((-19.649, 35.735), compiled.keepout_snapshot_rings[0])

        zone_poly = Polygon(zone_outer)
        keepout_polys = []
        for area in map_constraints.no_go_polygons:
            keepout_polys.extend(
                Polygon(region["outer"], region.get("holes") or [])
                for region in area.get("geometry") or []
            )
        expected = zone_poly.difference(unary_union(keepout_polys))
        actual = unary_union(effective_polys)
        self.assertAlmostEqual(actual.area, expected.area, places=1)

        # Exercise the serializer repair independently of the full-part fix:
        # this is the old clip-first geometry whose three-decimal ring became
        # self-intersecting in the field.
        legacy_clip_first = zone_poly.difference(zone_poly.intersection(unary_union(keepout_polys)))
        repaired_regions = _geometry_to_region_list(legacy_clip_first, prec=3)
        repaired_polys = [
            Polygon(region["outer"], region.get("holes") or [])
            for region in repaired_regions
        ]
        self.assertTrue(repaired_polys)
        self.assertTrue(all(poly.is_valid for poly in repaired_polys), repaired_regions)
        self.assertAlmostEqual(unary_union(repaired_polys).area, legacy_clip_first.area, places=1)

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

    def test_rectangular_keepout_expands_outward_normal_to_each_edge_type(self):
        no_go_outer = [[3.0, 1.5], [7.0, 1.5], [7.0, 3.5], [3.0, 3.5]]
        map_constraints = compile_map_constraints(
            map_id="map_demo",
            map_md5="demo-md5",
            constraint_version="constraints-demo",
            no_go_areas=[{"area_id": "nogo_rect", "polygon": no_go_outer, "enabled": True}],
            virtual_walls=[],
            default_no_go_long_edge_normal_buffer_m=0.15,
            default_no_go_short_edge_normal_buffer_m=0.40,
            prec=3,
        )

        area = map_constraints.no_go_polygons[0]
        ring = area["geometry"][0]["outer"]
        minx, miny, maxx, maxy = _ring_bounds(ring)
        self.assertAlmostEqual(minx, 2.60, places=3)
        self.assertAlmostEqual(maxx, 7.40, places=3)
        self.assertAlmostEqual(miny, 1.35, places=3)
        self.assertAlmostEqual(maxy, 3.65, places=3)
        self.assertEqual(area["buffer_mode"], "rect_anisotropic")
        self.assertAlmostEqual(area["long_edge_normal_buffer_m"], 0.15, places=3)
        self.assertAlmostEqual(area["short_edge_normal_buffer_m"], 0.40, places=3)

    def test_rotated_rectangular_keepout_uses_local_edge_normals(self):
        angle = math.radians(31.0)
        axis_u = (math.cos(angle), math.sin(angle))
        axis_v = (-math.sin(angle), math.cos(angle))
        center = (4.0, -2.0)
        half_long = 2.0
        half_short = 0.6

        no_go_outer = []
        for u, v in (
            (-half_long, -half_short),
            (half_long, -half_short),
            (half_long, half_short),
            (-half_long, half_short),
        ):
            no_go_outer.append(
                [
                    center[0] + u * axis_u[0] + v * axis_v[0],
                    center[1] + u * axis_u[1] + v * axis_v[1],
                ]
            )

        map_constraints = compile_map_constraints(
            map_id="map_demo",
            map_md5="demo-md5",
            constraint_version="constraints-demo",
            no_go_areas=[{"area_id": "nogo_rotated_rect", "polygon": no_go_outer, "enabled": True}],
            virtual_walls=[],
            default_no_go_long_edge_normal_buffer_m=0.15,
            default_no_go_short_edge_normal_buffer_m=0.40,
            prec=4,
        )

        ring = map_constraints.no_go_polygons[0]["geometry"][0]["outer"]
        projections_u = []
        projections_v = []
        for x, y in ring:
            dx = float(x) - center[0]
            dy = float(y) - center[1]
            projections_u.append(dx * axis_u[0] + dy * axis_u[1])
            projections_v.append(dx * axis_v[0] + dy * axis_v[1])

        self.assertAlmostEqual(max(projections_u), half_long + 0.40, places=3)
        self.assertAlmostEqual(min(projections_u), -half_long - 0.40, places=3)
        self.assertAlmostEqual(max(projections_v), half_short + 0.15, places=3)
        self.assertAlmostEqual(min(projections_v), -half_short - 0.15, places=3)

    def test_explicit_isotropic_buffer_overrides_anisotropic_defaults(self):
        no_go_outer = [[3.0, 1.5], [7.0, 1.5], [7.0, 3.5], [3.0, 3.5]]
        map_constraints = compile_map_constraints(
            map_id="map_demo",
            map_md5="demo-md5",
            constraint_version="constraints-demo",
            no_go_areas=[
                {
                    "area_id": "nogo_rect",
                    "polygon": no_go_outer,
                    "enabled": True,
                    "buffer_m": 0.20,
                }
            ],
            virtual_walls=[],
            default_no_go_long_edge_normal_buffer_m=0.15,
            default_no_go_short_edge_normal_buffer_m=0.40,
            prec=3,
        )

        area = map_constraints.no_go_polygons[0]
        minx, miny, maxx, maxy = _ring_bounds(area["geometry"][0]["outer"])
        self.assertAlmostEqual(minx, 2.80, places=3)
        self.assertAlmostEqual(maxx, 7.20, places=3)
        self.assertAlmostEqual(miny, 1.30, places=3)
        self.assertAlmostEqual(maxy, 3.70, places=3)
        self.assertEqual(area["buffer_mode"], "isotropic")


if __name__ == "__main__":
    unittest.main()

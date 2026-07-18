#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import math
import os
import sys
import unittest


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
PKG_DIR = os.path.dirname(THIS_DIR)
SRC_DIR = os.path.join(PKG_DIR, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

try:
    from shapely import affinity
    from shapely.geometry import MultiPolygon, Polygon, box

    _HAS_SHAPELY = True
except Exception:
    _HAS_SHAPELY = False

from coverage_planner.coverage_planner_core.site_axis_rectangle import (
    largest_site_axis_rectangle,
)


@unittest.skipUnless(_HAS_SHAPELY, "Shapely is required")
class SiteAxisRectangleTest(unittest.TestCase):
    def assertCovered(self, geometry, result):
        self.assertIsNotNone(result)
        repaired = geometry if geometry.is_valid else geometry.buffer(0.0)
        self.assertTrue(repaired.covers(result.as_polygon(inset=False)))
        self.assertTrue(repaired.covers(result.as_polygon(inset=True)))

    def test_rectangle_is_exact_then_all_four_sides_are_inset(self):
        cell = box(0.0, 0.0, 10.0, 6.0)
        result = largest_site_axis_rectangle(cell, wall_margin_m=0.38)

        self.assertCovered(cell, result)
        self.assertEqual(result.raw_bounds, (0.0, 0.0, 10.0, 6.0))
        self.assertEqual(result.source_bounds, result.raw_bounds)
        self.assertEqual(result.safe_bounds, result.inset_bounds)
        self.assertTrue(cell.covers(result.source_polygon))
        self.assertTrue(cell.covers(result.safe_polygon))
        for actual, expected in zip(result.inset_bounds, (0.38, 0.38, 9.62, 5.62)):
            self.assertAlmostEqual(actual, expected, places=9)

    def test_concave_l_cell_does_not_use_its_external_bbox(self):
        cell = Polygon(
            [(0.0, 0.0), (6.0, 0.0), (6.0, 2.0), (2.0, 2.0), (2.0, 6.0), (0.0, 6.0)]
        )
        result = largest_site_axis_rectangle(
            cell,
            wall_margin_m=0.2,
            search_step_m=0.05,
        )

        self.assertCovered(cell, result)
        self.assertAlmostEqual(result.raw_area_m2, 12.0, places=5)
        self.assertLess(result.raw_area_m2, box(*cell.bounds).area)

    def test_hole_is_never_crossed(self):
        hole = box(4.0, 2.0, 6.0, 6.0)
        cell = Polygon(
            box(0.0, 0.0, 10.0, 8.0).exterior.coords,
            [hole.exterior.coords],
        )
        result = largest_site_axis_rectangle(
            cell,
            wall_margin_m=0.25,
            search_step_m=0.05,
        )

        self.assertCovered(cell, result)
        self.assertFalse(result.as_polygon(inset=False).intersects(hole.buffer(-1e-9)))
        self.assertGreaterEqual(result.raw_area_m2, 31.99)

    def test_diamond_continuous_optimum_is_refined_and_contained(self):
        cell = Polygon([(0.0, 5.0), (5.0, 0.0), (10.0, 5.0), (5.0, 10.0)])
        result = largest_site_axis_rectangle(
            cell,
            search_step_m=0.2,
            refine_steps=2,
        )

        self.assertCovered(cell, result)
        # Exact site-axis optimum is the central 5 x 5 square (area 25).
        self.assertGreater(result.raw_area_m2, 24.9)
        self.assertLessEqual(result.raw_area_m2, 25.0 + 1e-7)

    def test_fixed_site_axes_are_not_rotated_to_polygon_axes(self):
        cell = affinity.rotate(box(-4.0, -1.0, 4.0, 1.0), 30.0, origin=(0.0, 0.0))
        result = largest_site_axis_rectangle(cell, search_step_m=0.025)

        self.assertCovered(cell, result)
        ring = result.outer_xy(inset=False)
        self.assertAlmostEqual(ring[0][1], ring[1][1], places=9)
        self.assertAlmostEqual(ring[1][0], ring[2][0], places=9)

    def test_larger_multipolygon_component_wins(self):
        geometry = MultiPolygon([box(0.0, 0.0, 2.0, 2.0), box(10.0, 10.0, 16.0, 14.0)])
        result = largest_site_axis_rectangle(geometry, wall_margin_m=0.1)

        self.assertCovered(geometry, result)
        self.assertEqual(result.raw_bounds, (10.0, 10.0, 16.0, 14.0))

    def test_invalid_polygon_is_repaired_with_shapely_18_compatible_operation(self):
        bow_tie = Polygon([(0.0, 0.0), (4.0, 4.0), (0.0, 4.0), (4.0, 0.0)])
        self.assertFalse(bow_tie.is_valid)
        result = largest_site_axis_rectangle(bow_tie, search_step_m=0.05)

        self.assertCovered(bow_tie, result)
        self.assertGreater(result.usable_area_m2, 0.1)

    def test_margin_that_consumes_every_candidate_returns_none(self):
        result = largest_site_axis_rectangle(
            box(0.0, 0.0, 0.7, 0.7),
            wall_margin_m=0.35,
            min_usable_side_m=0.05,
        )
        self.assertIsNone(result)

    def test_result_is_deterministic(self):
        cell = Polygon(
            box(0.0, 0.0, 12.0, 8.0).exterior.coords,
            [box(5.0, 2.0, 7.0, 6.0).exterior.coords],
        )
        results = [
            largest_site_axis_rectangle(cell, wall_margin_m=0.38, search_step_m=0.1)
            for _ in range(3)
        ]
        self.assertTrue(all(item == results[0] for item in results[1:]))

    def test_rejects_non_finite_or_invalid_search_arguments(self):
        cell = box(0.0, 0.0, 1.0, 1.0)
        with self.assertRaises(ValueError):
            largest_site_axis_rectangle(cell, wall_margin_m=-0.1)
        with self.assertRaises(ValueError):
            largest_site_axis_rectangle(cell, search_step_m=0.0)
        with self.assertRaises(ValueError):
            largest_site_axis_rectangle(cell, absolute_tolerance_m=math.nan)


if __name__ == "__main__":
    unittest.main()

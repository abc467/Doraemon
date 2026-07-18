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
    from shapely.geometry import LineString, box

    _HAS_GEOMETRY_RUNTIME = True
except Exception:
    _HAS_GEOMETRY_RUNTIME = False

from coverage_planner.coverage_planner_core.planner import plan_coverage
from coverage_planner.coverage_planner_core.types import PlannerParams, RobotSpec


def _params(wall_margin_m=0.38, turn_margin_m=1.20):
    return PlannerParams(
        path_step_m=0.05,
        viz_step_m=0.05,
        turn_step_m=0.05,
        wall_margin_m=float(wall_margin_m),
        turn_margin_m=float(turn_margin_m),
        min_swath_length_m=0.40,
        edge_corner_radius_m=0.40,
        mute_stderr=True,
        validate_effective_region_path=False,
    )


@unittest.skipUnless(_HAS_GEOMETRY_RUNTIME, "Shapely and Fields2Cover are required")
class PlannerSiteAxisRectangleCoreTest(unittest.TestCase):
    @staticmethod
    def _plan(outer, wall_margin_m=0.38, turn_margin_m=1.20, debug=True):
        return plan_coverage(
            frame_id="site_map",
            outer=outer,
            holes=[],
            robot_spec=RobotSpec(0.54, 0.65, 0.32, 0.30),
            params=_params(
                wall_margin_m=wall_margin_m,
                turn_margin_m=turn_margin_m,
            ),
            effective_regions=[{"outer": outer, "holes": []}],
            debug=debug,
        )

    @staticmethod
    def _short_end_clearance(block):
        """Return the smaller X-end clearance between snake and edge loop.

        The fixtures below share a 10 x 6 m main rectangle, so their swaths
        run along X and their Dubins turns occupy the two X ends.  Measuring
        the snake extrema against the edge-loop extrema includes the outward
        bulge of the turns instead of checking only the trimmed swath ends.
        """

        debug = block.debug
        if debug is None or not debug.edge_loop_xy or not debug.snake_pts_xy:
            raise AssertionError("debug edge loop and snake are required")
        edge_min_x = min(float(point[0]) for point in debug.edge_loop_xy)
        edge_max_x = max(float(point[0]) for point in debug.edge_loop_xy)
        snake_min_x = min(float(point[0]) for point in debug.snake_pts_xy)
        snake_max_x = max(float(point[0]) for point in debug.snake_pts_xy)
        return min(snake_min_x - edge_min_x, edge_max_x - snake_max_x)

    def test_tail_cell_rebuilds_edge_loop_and_swaths_from_one_safe_rectangle(self):
        # The 2 x 1 m cap is the same geometric failure mode as a height
        # mismatch between adjacent no-go rectangles: it used to survive in
        # the snake cell while making the bbox edge loop unsafe.
        outer = [
            (0.0, 0.0),
            (10.0, 0.0),
            (10.0, 6.0),
            (2.0, 6.0),
            (2.0, 7.0),
            (0.0, 7.0),
        ]
        result = plan_coverage(
            frame_id="site_map",
            outer=outer,
            holes=[],
            robot_spec=RobotSpec(
                cov_width=0.54,
                width=0.65,
                min_turning_radius=0.32,
                max_diff_curv=0.30,
            ),
            params=_params(),
            effective_regions=[{"outer": outer, "holes": []}],
            debug=True,
        )

        self.assertTrue(result.ok, result.error_message)
        self.assertEqual(len(result.blocks or []), 1)
        block = result.blocks[0]
        stats = block.stats or {}
        self.assertEqual(stats.get("cell_geometry_mode"), "site_axis_inscribed_rectangle")
        self.assertEqual(stats.get("rectangle_trigger"), "non_rectangular_or_hole_cell")
        self.assertNotIn("edge_loop_skipped", stats)
        self.assertGreater(int(stats.get("edge_pts") or 0), 4)

        source_bounds = list(stats.get("rectangle_source_bounds") or [])
        safe_bounds = list(stats.get("rectangle_safe_bounds") or [])
        for actual, expected in zip(source_bounds, (0.0, 0.0, 10.0, 6.0)):
            self.assertAlmostEqual(actual, expected, places=6)
        for actual, expected in zip(safe_bounds, (0.38, 0.38, 9.62, 5.62)):
            self.assertAlmostEqual(actual, expected, places=6)
        self.assertAlmostEqual(float(stats.get("rectangle_wall_margin_m")), 0.38, places=9)

        safe = box(*safe_bounds).buffer(1e-6)
        debug = block.debug
        self.assertIsNotNone(debug)
        self.assertTrue(debug.edge_loop_xy)
        self.assertTrue(debug.swath_segs_xyz)

        edge_x = [float(point[0]) for point in debug.edge_loop_xy]
        edge_y = [float(point[1]) for point in debug.edge_loop_xy]
        edge_bounds = (min(edge_x), min(edge_y), max(edge_x), max(edge_y))
        for actual, expected in zip(edge_bounds, safe_bounds):
            self.assertAlmostEqual(actual, expected, places=6)
        self.assertTrue(safe.covers(LineString(debug.edge_loop_xy)))

        for start, end in debug.swath_segs_xyz:
            swath = LineString(
                [
                    (float(start[0]), float(start[1])),
                    (float(end[0]), float(end[1])),
                ]
            )
            self.assertTrue(safe.covers(swath), (start, end, safe_bounds))

    def test_regular_rectangle_keeps_existing_long_side_shrink_flow(self):
        outer = [(0.0, 0.0), (10.0, 0.0), (10.0, 6.0), (0.0, 6.0)]
        result = plan_coverage(
            frame_id="site_map",
            outer=outer,
            holes=[],
            robot_spec=RobotSpec(0.54, 0.65, 0.32, 0.30),
            params=_params(wall_margin_m=0.30),
            effective_regions=[{"outer": outer, "holes": []}],
            debug=False,
        )

        self.assertTrue(result.ok, result.error_message)
        self.assertEqual(len(result.blocks or []), 1)
        stats = result.blocks[0].stats or {}
        self.assertEqual(stats.get("cell_geometry_mode"), "legacy_long_side_shrink")
        self.assertNotIn("rectangle_source_bounds", stats)

    def test_turn_setback_is_normalized_to_the_shared_edge_loop(self):
        regular_outer = [(0.0, 0.0), (10.0, 0.0), (10.0, 6.0), (0.0, 6.0)]
        tail_outer = [
            (0.0, 0.0),
            (10.0, 0.0),
            (10.0, 6.0),
            (2.0, 6.0),
            (2.0, 7.0),
            (0.0, 7.0),
        ]

        regular = self._plan(regular_outer)
        site_axis = self._plan(tail_outer)
        self.assertTrue(regular.ok, regular.error_message)
        self.assertTrue(site_axis.ok, site_axis.error_message)
        self.assertEqual(len(regular.blocks or []), 1)
        self.assertEqual(len(site_axis.blocks or []), 1)

        regular_block = regular.blocks[0]
        site_axis_block = site_axis.blocks[0]
        regular_stats = regular_block.stats or {}
        site_axis_stats = site_axis_block.stats or {}

        self.assertEqual(regular_stats.get("cell_geometry_mode"), "legacy_long_side_shrink")
        self.assertEqual(
            site_axis_stats.get("cell_geometry_mode"),
            "site_axis_inscribed_rectangle",
        )

        self.assertAlmostEqual(
            float(regular_stats.get("configured_turn_margin_m")),
            1.20,
            places=9,
        )
        self.assertAlmostEqual(
            float(regular_stats.get("preapplied_turn_margin_m")),
            0.0,
            places=9,
        )
        self.assertAlmostEqual(
            float(regular_stats.get("effective_turn_margin_m")),
            1.20,
            places=9,
        )
        self.assertAlmostEqual(
            float(site_axis_stats.get("configured_turn_margin_m")),
            1.20,
            places=9,
        )
        self.assertAlmostEqual(
            float(site_axis_stats.get("preapplied_turn_margin_m")),
            0.38,
            places=9,
        )
        self.assertAlmostEqual(
            float(site_axis_stats.get("effective_turn_margin_m")),
            0.82,
            places=9,
        )

        regular_clearance = self._short_end_clearance(regular_block)
        site_axis_clearance = self._short_end_clearance(site_axis_block)
        self.assertGreater(regular_clearance, 0.0)
        self.assertGreater(site_axis_clearance, 0.0)
        self.assertAlmostEqual(
            site_axis_clearance,
            regular_clearance,
            delta=0.02,
            msg=(
                "legacy and site-axis Dubins turns should have the same "
                "setback from their common edge-loop bounds"
            ),
        )

    def test_preapplied_margin_larger_than_turn_target_is_clamped_to_zero(self):
        tail_outer = [
            (0.0, 0.0),
            (10.0, 0.0),
            (10.0, 6.0),
            (2.0, 6.0),
            (2.0, 7.0),
            (0.0, 7.0),
        ]
        result = self._plan(
            tail_outer,
            wall_margin_m=0.38,
            turn_margin_m=0.20,
            debug=False,
        )

        self.assertTrue(result.ok, result.error_message)
        stats = result.blocks[0].stats or {}
        self.assertEqual(stats.get("cell_geometry_mode"), "site_axis_inscribed_rectangle")
        self.assertAlmostEqual(float(stats.get("effective_turn_margin_m")), 0.0, places=9)
        self.assertTrue(bool(stats.get("turn_margin_clamped")))


if __name__ == "__main__":
    unittest.main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import unittest
from unittest.mock import patch


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

from coverage_planner.constraints import _HAS_SHAPELY, compile_map_constraints, compile_zone_constraints
from coverage_planner.coverage_planner_core.isolated_runner import run_plan_coverage_isolated
from coverage_planner.coverage_planner_core.types import PlannerParams, RobotSpec


FIELD_ZONE_OUTER = [
    [-17.952219286719107, -1.323259016318092],
    [39.23789631563458, 1.294598388014979],
    [37.541556726363574, 38.35309403055115],
    [-19.648558875990116, 35.735236626218075],
]

FIELD_NO_GO_OUTER = [
    [-20.039, 19.675],
    [-11.339, 20.074],
    [-12.188, 38.633],
    [-20.888, 38.235],
]

# Exact three-decimal payload that previously reached GEOS/Fields2Cover and
# produced: TopologyException ... Self-intersection at -11.920997...,36.08895...
FIELD_INVALID_EFFECTIVE_OUTER = [
    [37.542, 38.353],
    [39.238, 1.295],
    [-17.952, -1.323],
    [-18.898, 19.327],
    [-11.171, 19.680],
    [-11.921, 36.089],
    [-19.649, 35.735],
]


def _robot_spec():
    return RobotSpec(cov_width=0.54, width=0.65, min_turning_radius=0.34, max_diff_curv=0.30)


def _planner_params():
    return PlannerParams(
        path_step_m=0.05,
        turn_step_m=0.05,
        wall_margin_m=0.38,
        turn_margin_m=1.20,
        min_plannable_span_m=0.05,
        edge_corner_radius_m=0.40,
        edge_corner_pull=0.10,
        mute_stderr=True,
        validate_effective_region_path=False,
    )


@unittest.skipUnless(_HAS_SHAPELY, "Shapely is required for geometry validation")
class IsolatedRunnerGeometryGuardTest(unittest.TestCase):
    def test_invalid_field_payload_is_rejected_before_subprocess(self):
        with patch(
            "coverage_planner.coverage_planner_core.isolated_runner.subprocess.run"
        ) as subprocess_run:
            outcome = run_plan_coverage_isolated(
                frame_id="map",
                outer=FIELD_ZONE_OUTER,
                holes=[],
                robot_spec=_robot_spec(),
                params=_planner_params(),
                effective_regions=[{"outer": FIELD_INVALID_EFFECTIVE_OUTER, "holes": []}],
                timeout_s=10.0,
            )

        subprocess_run.assert_not_called()
        self.assertFalse(outcome.crashed)
        self.assertFalse(outcome.timeout)
        self.assertIsNotNone(outcome.result)
        self.assertFalse(outcome.result.ok)
        self.assertEqual(outcome.result.error_code, "INVALID_EFFECTIVE_REGION")
        self.assertIn("Self-intersection", outcome.result.error_message)

    @unittest.skipUnless(_HAS_F2C, "Fields2Cover is required for isolated worker regression")
    def test_repaired_field_fixture_completes_isolated_worker_without_sigsegv(self):
        map_constraints = compile_map_constraints(
            map_id="field_715",
            map_md5="field-md5",
            constraint_version="field-constraints",
            no_go_areas=[
                {"area_id": "cross_boundary", "polygon": FIELD_NO_GO_OUTER, "enabled": True}
            ],
            virtual_walls=[],
            default_no_go_buffer_m=0.30,
            default_no_go_long_edge_normal_buffer_m=0.15,
            default_no_go_short_edge_normal_buffer_m=0.40,
            prec=3,
        )
        zone_constraints = compile_zone_constraints(
            zone_outer=FIELD_ZONE_OUTER,
            zone_holes=[],
            map_constraints=map_constraints,
            prec=3,
        )

        inherited_pythonpath = os.environ.get("PYTHONPATH", "")
        worker_pythonpath = SRC_DIR
        if inherited_pythonpath:
            worker_pythonpath += os.pathsep + inherited_pythonpath
        with patch.dict(os.environ, {"PYTHONPATH": worker_pythonpath}):
            outcome = run_plan_coverage_isolated(
                frame_id="map",
                outer=FIELD_ZONE_OUTER,
                holes=[],
                robot_spec=_robot_spec(),
                params=_planner_params(),
                effective_regions=zone_constraints.effective_regions,
                timeout_s=20.0,
            )

        self.assertFalse(outcome.crashed, outcome.message)
        self.assertFalse(outcome.timeout, outcome.message)
        self.assertNotIn("SIGSEGV", outcome.message)
        self.assertIsNotNone(outcome.result, outcome.message)
        self.assertTrue(outcome.result.ok, outcome.result.error_message)


if __name__ == "__main__":
    unittest.main()

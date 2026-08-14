#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Static production-wiring contracts for the Theta-prefix/State-suffix mode.

These tests deliberately do not invoke ROS actions.  They bind the launch and
configuration selected in production to the safety-critical ordering in the
planner source, while the geometric helper itself is covered by its C++ unit
tests in smac_lattice_planner_mbf.
"""

import os
import re
import unittest
import xml.etree.ElementTree as ET

import yaml


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
EXECUTOR_DIR = os.path.dirname(THIS_DIR)
SOURCE_DIR = os.path.dirname(EXECUTOR_DIR)
NAV_CONFIG = os.path.join(
    SOURCE_DIR, "cleanrobot", "config", "nav", "mbf_nav.yaml"
)
MBF_TIMING_CONFIG = os.path.join(
    SOURCE_DIR, "cleanrobot", "config", "nav", "mbf_timing.yaml"
)
MBF_LAUNCH = os.path.join(SOURCE_DIR, "cleanrobot", "launch", "mbf_nav.launch")
EXECUTOR_LAUNCH = os.path.join(
    SOURCE_DIR, "coverage_executor", "launch", "executor.launch"
)
EXECUTOR_NODE = os.path.join(
    SOURCE_DIR, "coverage_executor", "scripts", "executor_node.py"
)
EXECUTOR_FSM = os.path.join(
    SOURCE_DIR, "coverage_executor", "src", "coverage_executor", "fsm.py"
)
MBF_ABSTRACT_PARAMS = os.path.join(
    SOURCE_DIR,
    "external_navigation",
    "move_base_flex",
    "mbf_abstract_nav",
    "src",
    "mbf_abstract_nav",
    "__init__.py",
)
PLANNER_SOURCE = os.path.join(
    SOURCE_DIR,
    "smac_lattice_planner_mbf",
    "src",
    "smac_lattice_planner.cpp",
)
PLANNER_HEADER = os.path.join(
    SOURCE_DIR,
    "smac_lattice_planner_mbf",
    "include",
    "smac_lattice_planner_mbf",
    "smac_lattice_planner.hpp",
)
COARSE_SOURCE = os.path.join(
    SOURCE_DIR,
    "smac_lattice_planner_mbf",
    "src",
    "coarse_route_corridor.cpp",
)
A_STAR_IMPL = os.path.join(
    SOURCE_DIR,
    "smac_lattice_planner_mbf",
    "upstream",
    "nav2_smac_planner",
    "include",
    "nav2_smac_planner",
    "a_star_impl.hpp",
)
VALIDATOR_SOURCE = os.path.join(
    SOURCE_DIR,
    "smac_lattice_planner_mbf",
    "src",
    "live_block_validator.cpp",
)
SUFFIX_HEADER = os.path.join(
    SOURCE_DIR,
    "smac_lattice_planner_mbf",
    "include",
    "smac_lattice_planner_mbf",
    "theta_state_suffix.hpp",
)
SUFFIX_SOURCE = os.path.join(
    SOURCE_DIR,
    "smac_lattice_planner_mbf",
    "src",
    "theta_state_suffix.cpp",
)
SMOOTHER_HEADER = os.path.join(
    SOURCE_DIR,
    "smac_lattice_planner_mbf",
    "include",
    "smac_lattice_planner_mbf",
    "state_lattice_smoother.hpp",
)
SMOOTHER_SOURCE = os.path.join(
    SOURCE_DIR,
    "smac_lattice_planner_mbf",
    "src",
    "state_lattice_smoother.cpp",
)


def _read(path):
    with open(path, "r", encoding="utf-8") as handle:
        return handle.read()


def _load_nav_config():
    with open(NAV_CONFIG, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def _node_param(launch_path, node_name, param_name):
    root = ET.parse(launch_path).getroot()
    node = root.find(".//node[@name='%s']" % node_name)
    if node is None:
        raise AssertionError("missing launch node %s" % node_name)
    param = node.find("./param[@name='%s']" % param_name)
    if param is None:
        raise AssertionError("missing parameter %s/%s" % (node_name, param_name))
    return param.get("value")


class SmacCompositeSourceContractTest(unittest.TestCase):
    def test_cpp_fallback_defaults_are_fail_closed(self):
        header = _read(PLANNER_HEADER)
        source = _read(PLANNER_SOURCE)

        self.assertRegex(header, r"bool\s+allow_unknown_\{false\};")
        self.assertRegex(
            header, r"bool\s+theta_corridor_search_enabled_\{false\};"
        )
        self.assertRegex(header, r"double\s+max_planning_time_\{180\.0\};")
        self.assertRegex(
            header,
            r"double\s+theta_suffix_candidate_max_planning_time_\{60\.0\};",
        )
        self.assertRegex(
            source,
            r'private_nh\.param\(\s*"allow_unknown",\s*allow_unknown_,\s*false\s*\);',
        )
        self.assertRegex(
            source,
            r'private_nh\.param\(\s*"theta_corridor_search_enabled",\s*'
            r'theta_corridor_search_enabled_,\s*false\s*\);',
        )
        self.assertRegex(
            source,
            r'private_nh\.param\(\s*"allow_reverse_expansion",\s*'
            r'search_info_\.allow_reverse_expansion,\s*false\s*\);',
        )
        self.assertRegex(
            source,
            r'private_nh\.param\(\s*"max_planning_time",\s*'
            r'max_planning_time_,\s*180\.0\s*\);',
        )
        self.assertRegex(
            source,
            r'private_nh\.param\(\s*"theta_suffix_candidate_max_planning_time",\s*'
            r'theta_suffix_candidate_max_planning_time_,\s*60\.0\s*\);',
        )
        self.assertRegex(
            source,
            r'private_nh\.param\(\s*"change_penalty",\s*'
            r'search_info_\.change_penalty,\s*0\.45f\s*\);',
        )
        self.assertRegex(
            source,
            r'private_nh\.param\(\s*"rotation_penalty",\s*'
            r'search_info_\.rotation_penalty,\s*10\.0f\s*\);',
        )

    def test_production_launch_loads_explicit_composite_ros_parameters(self):
        config = _load_nav_config()
        smac = config["SmacLatticePlanner"]
        self.assertTrue(smac["theta_prefix_lattice_suffix_enabled"])
        self.assertFalse(smac["theta_corridor_search_enabled"])
        self.assertEqual(int(smac["theta_max_allowed_cost"]), 10)
        self.assertAlmostEqual(float(smac["theta_w_traversal_cost"]), 32.0)
        self.assertAlmostEqual(float(smac["theta_w_euc_cost"]), 1.5)
        self.assertAlmostEqual(float(smac["theta_w_heuristic_cost"]), 1.0)
        self.assertAlmostEqual(float(smac["theta_reference_spacing"]), 0.05)
        self.assertTrue(smac["theta_reference_smoothing_enabled"])
        self.assertFalse(smac["allow_reverse_expansion"])
        self.assertAlmostEqual(float(smac["change_penalty"]), 0.45)
        self.assertAlmostEqual(float(smac["non_straight_penalty"]), 1.05)
        self.assertAlmostEqual(float(smac["rotation_penalty"]), 10.0)
        self.assertAlmostEqual(float(smac["max_planning_time"]), 180.0)
        self.assertAlmostEqual(
            float(smac["theta_suffix_candidate_max_planning_time"]), 60.0
        )
        self.assertEqual(int(smac["theta_unsafe_segment_lookback_points"]), 30)
        self.assertTrue(smac["state_lattice_smoothing_enabled"])
        self.assertEqual(int(smac["state_lattice_smoother_max_iterations"]), 1000)
        self.assertAlmostEqual(float(smac["state_lattice_smoother_w_data"]), 0.2)
        self.assertAlmostEqual(float(smac["state_lattice_smoother_w_smooth"]), 0.3)
        self.assertAlmostEqual(float(smac["state_lattice_smoother_tolerance"]), 1e-10)
        self.assertTrue(smac["state_lattice_smoother_do_refinement"])
        self.assertEqual(int(smac["state_lattice_smoother_refinement_num"]), 2)

        launch_root = ET.parse(MBF_LAUNCH).getroot()
        mbf_node = launch_root.find(".//node[@name='move_base_flex']")
        self.assertIsNotNone(mbf_node)
        self.assertEqual(mbf_node.get("clear_params"), "true")
        loaded_files = [
            item.get("file")
            for item in mbf_node.findall("./rosparam[@command='load']")
        ]
        self.assertIn("$(arg mbf_nav_yaml)", loaded_files)

        planner_source = _read(PLANNER_SOURCE)
        validator_source = _read(VALIDATOR_SOURCE)
        self.assertIn(
            'private_nh.param("change_penalty", change_penalty, change_penalty);',
            validator_source,
        )
        self.assertIn(
            "search_info.change_penalty = static_cast<float>(change_penalty);",
            validator_source,
        )
        self.assertIn("double change_penalty = 0.45;", validator_source)
        self.assertIn("double non_straight_penalty = 1.05;", validator_source)
        self.assertIn("double rotation_penalty = 10.0;", validator_source)
        for parameter_name in (
            "theta_prefix_lattice_suffix_enabled",
            "theta_corridor_search_enabled",
            "theta_max_allowed_cost",
            "theta_w_traversal_cost",
            "theta_w_euc_cost",
            "theta_w_heuristic_cost",
            "theta_reference_spacing",
            "theta_reference_smoothing_enabled",
            "theta_suffix_candidate_max_planning_time",
            "theta_unsafe_segment_lookback_points",
            "state_lattice_smoothing_enabled",
            "state_lattice_smoother_max_iterations",
            "state_lattice_smoother_w_data",
            "state_lattice_smoother_w_smooth",
            "state_lattice_smoother_tolerance",
            "state_lattice_smoother_do_refinement",
            "state_lattice_smoother_refinement_num",
        ):
            self.assertIn('"%s"' % parameter_name, planner_source)

    def test_state_smoothing_is_atomic_state_only_and_validator_equivalent(self):
        planner_source = _read(PLANNER_SOURCE)
        validator_source = _read(VALIDATOR_SOURCE)
        smoother_header = _read(SMOOTHER_HEADER)
        smoother_source = _read(SMOOTHER_SOURCE)

        self.assertIn("class StateLatticeSmoother", smoother_header)
        self.assertIn("w_data{0.2}", smoother_header)
        self.assertIn("w_smooth{0.3}", smoother_header)
        self.assertIn("tolerance{1e-10}", smoother_header)
        self.assertIn("max_iterations{1000}", smoother_header)
        self.assertIn("ompl::base::DubinsStateSpace", smoother_source)
        self.assertIn("findDirectionalPathSegments", smoother_source)
        self.assertIn("updateApproximateOrientations", smoother_source)
        self.assertIn("qualityGateAccepts", smoother_source)

        for source in (planner_source, validator_source):
            self.assertIn('"state_lattice_smoothing_enabled"', source)
            self.assertIn('"state_lattice_smoother_w_data"', source)
            self.assertIn('"state_lattice_smoother_w_smooth"', source)
            self.assertIn("maybeSmoothStatePath", source)
            self.assertIn("raw_state_suffix", source)
            self.assertIn("raw_composite", source) if source == planner_source else self.assertIn(
                "raw_candidate", source
            )
            self.assertIn("containsKinematicallyContinuousForwardOrRotation", source)
            self.assertIn("terminalAvoidsStationaryYawRepair", source)

        # The production integration only passes State-generated paths to the
        # smoother; the retained Theta prefix is used solely for re-stitching.
        self.assertIn("maybeSmoothStatePath(state_suffix", planner_source)
        self.assertIn("maybeSmoothStatePath(plan, search_deadline", planner_source)
        self.assertNotIn("maybeSmoothStatePath(selection.prefix_including_cut", planner_source)
        self.assertLess(
            planner_source.index("const auto raw_state_suffix = state_suffix;"),
            planner_source.index("maybeSmoothStatePath(state_suffix"),
        )

    def test_planning_deadline_chain_preserves_the_full_state_budget(self):
        config = _load_nav_config()
        smac = config["SmacLatticePlanner"]
        with open(MBF_TIMING_CONFIG, "r", encoding="utf-8") as handle:
            timing = yaml.safe_load(handle)

        self.assertAlmostEqual(float(smac["max_planning_time"]), 180.0)
        self.assertAlmostEqual(
            float(smac["theta_suffix_candidate_max_planning_time"]), 60.0
        )
        self.assertAlmostEqual(float(timing["planner_patience"]), 190.0)

        executor_root = ET.parse(EXECUTOR_LAUNCH).getroot()
        executor_node = executor_root.find(".//node[@name='coverage_executor']")
        self.assertIsNotNone(executor_node)
        launch_params = {
            item.get("name"): float(item.get("value"))
            for item in executor_node.findall("./param")
            if item.get("name") in {
                "connect_timeout_base_s",
                "connect_no_progress_timeout_s",
            }
        }
        self.assertEqual(launch_params["connect_timeout_base_s"], 240.0)
        self.assertEqual(launch_params["connect_no_progress_timeout_s"], 210.0)
        self.assertGreater(float(timing["planner_patience"]), 180.0)
        self.assertGreater(
            launch_params["connect_no_progress_timeout_s"],
            float(timing["planner_patience"]),
        )
        self.assertGreater(
            launch_params["connect_timeout_base_s"],
            launch_params["connect_no_progress_timeout_s"],
        )

        mbf_params = _read(MBF_ABSTRACT_PARAMS)
        self.assertRegex(
            mbf_params,
            r'gen\.add\("planner_patience"[\s\S]*?5\.0,\s*0,\s*600\)',
        )
        self.assertIn(
            'rospy.get_param("~connect_timeout_base_s", 240.0)',
            _read(EXECUTOR_NODE),
        )
        self.assertIn(
            'rospy.get_param("~connect_no_progress_timeout_s", 210.0)',
            _read(EXECUTOR_NODE),
        )
        self.assertRegex(
            _read(EXECUTOR_FSM),
            r"connect_timeout_base_s:\s*float\s*=\s*240\.0",
        )
        self.assertRegex(
            _read(EXECUTOR_FSM),
            r"connect_no_progress_timeout_s:\s*float\s*=\s*210\.0",
        )

        validator_source = _read(VALIDATOR_SOURCE)
        self.assertIn("double max_planning_time = 180.0;", validator_source)
        self.assertIn(
            "double theta_suffix_candidate_max_planning_time = 60.0;",
            validator_source,
        )

    def test_unsafe_theta_segment_uses_local_adaptive_state_suffix(self):
        planner_source = _read(PLANNER_SOURCE)
        validator_source = _read(
            os.path.join(
                SOURCE_DIR,
                "smac_lattice_planner_mbf",
                "src",
                "live_block_validator.cpp",
            )
        )
        helper_source = _read(
            os.path.join(
                SOURCE_DIR,
                "smac_lattice_planner_mbf",
                "src",
                "theta_state_suffix.cpp",
            )
        )

        for source in (planner_source, validator_source):
            self.assertIn("selectThetaPrefixCutBeforeUnsafeSegment", source)
            self.assertIn("adaptive_unsafe_segment_fallback_requested", source)
            self.assertIn("adaptive_unsafe_segment_fallback_scheduled", source)
            self.assertIn("adaptive_unsafe_segment_fallback_attempted", source)
        self.assertIn("whole-route State fallback suppressed", planner_source)
        self.assertIn("suppress_full_state_fallback", validator_source)
        self.assertIn("unsafe_segment_index - lookback_points", helper_source)
        self.assertLess(
            planner_source.index(
                "if (!path_found && adaptive_unsafe_segment_fallback_requested)"
            ),
            planner_source.index(
                "if (!path_found && theta_corridor_search_enabled_"
            ),
        )
        self.assertIn(
            "if (!success && !suppress_full_state_fallback &&\n"
            "          theta_corridor_search_enabled",
            validator_source,
        )
        self.assertIn(
            "if (!success && !suppress_full_state_fallback) {",
            validator_source,
        )

    def test_short_theta_route_runs_state_once_without_duplicate_full(self):
        planner_source = _read(PLANNER_SOURCE)
        validator_source = _read(VALIDATOR_SOURCE)

        for source in (planner_source, validator_source):
            self.assertIn("short_theta_reference", source)
            self.assertIn("theta_short_route_state_full_", source)
            self.assertIn("kSuffixPointCountCandidates.front()", source)
        self.assertIn("duplicate FULL fallback suppressed", planner_source)
        self.assertIn(
            "SHORT_THETA_ROUTE_STATE_FAILED_FULL_SUPPRESSED",
            validator_source,
        )
        self.assertIn(
            "(selection_is_adaptive || selection_is_short_route_all_state)",
            planner_source,
        )
        self.assertIn(
            "(cut_is_adaptive || cut_is_short_route_all_state)",
            validator_source,
        )

    def test_connect_selects_smac_and_state_mppi_without_legacy_theta_repairs(self):
        config = _load_nav_config()
        self.assertTrue(config["ThetaStarPlanner"]["se2_refinement_enabled"])
        self.assertFalse(
            config["SmacLatticePlanner"]["theta_corridor_search_enabled"]
        )

        executor_launch = os.path.join(EXECUTOR_DIR, "launch", "executor.launch")
        task_launch = os.path.join(
            SOURCE_DIR, "coverage_task_manager", "launch", "task_manager.launch"
        )
        self.assertEqual(
            _node_param(executor_launch, "coverage_executor", "mbf_planner"),
            "SmacLatticePlanner",
        )
        self.assertEqual(
            _node_param(
                executor_launch,
                "coverage_executor",
                "mbf_connect_controller",
            ),
            "MPPI_State_Lattice_Controller",
        )
        self.assertEqual(
            _node_param(task_launch, "coverage_task_manager", "mbf_planner"),
            "SmacLatticePlanner",
        )
        self.assertEqual(
            _node_param(task_launch, "coverage_task_manager", "mbf_controller"),
            "MPPI_State_Lattice_Controller",
        )

        planner_source = _read(PLANNER_SOURCE)
        coarse_source = _read(COARSE_SOURCE)
        self.assertNotIn("se2_refinement_enabled", planner_source)
        self.assertIn('#include "theta_star_planner/theta_star.h"', coarse_source)
        self.assertNotIn("theta_star_planner/theta_star_planner.h", coarse_source)
        self.assertIn(
            "options.build_center_corridors = theta_corridor_search_enabled_;",
            planner_source,
        )

    def test_hundred_point_suffix_is_pose_count_not_one_hundred_segments(self):
        config = _load_nav_config()
        self.assertAlmostEqual(
            float(config["SmacLatticePlanner"]["theta_reference_spacing"]), 0.05
        )

        header = _read(SUFFIX_HEADER)
        source = _read(SUFFIX_SOURCE)
        constants = re.search(
            r"kSuffixPointCountCandidates\s*\{\{(.*?)\}\}",
            header,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(constants)
        self.assertEqual(
            [int(value) for value in re.findall(r"(\d+)u", constants.group(1))],
            [100, 160, 240, 400],
        )
        self.assertIn("100 poses span 99 intervals", header)
        self.assertIn(
            "const std::size_t effective_count = std::min(point_count, theta_reference.size());",
            source,
        )
        self.assertIn(
            "const std::size_t cut_index = theta_reference.size() - effective_count;",
            source,
        )
        self.assertIn(
            "theta_reference.begin() + cut_index + 1u",
            source,
        )

    def test_composite_drops_start_cell_stub_and_audits_retained_prefix(self):
        coarse = _read(COARSE_SOURCE)
        planner = _read(PLANNER_SOURCE)
        validator = _read(VALIDATOR_SOURCE)
        suffix_header = _read(SUFFIX_HEADER)

        self.assertIn("bool left_start_cell = false;", coarse)
        self.assertIn("point_mx == start_mx && point_my == start_my", coarse)
        self.assertLess(
            coarse.index("appendDistinctPoint(exact_anchors, start_wx, start_wy)"),
            coarse.index("bool left_start_cell = false;"),
        )
        self.assertLess(
            coarse.index("bool left_start_cell = false;"),
            coarse.index("appendDistinctPoint(exact_anchors, goal_wx, goal_wy)"),
        )
        self.assertIn(
            "containsKinematicallyContinuousForwardOrRotation", suffix_header
        )
        self.assertEqual(
            planner.count("containsKinematicallyContinuousForwardOrRotation("),
            4,
        )
        self.assertEqual(
            validator.count("containsKinematicallyContinuousForwardOrRotation("),
            4,
        )
        self.assertNotIn("containsOnlyForwardOrRotation(", planner)
        self.assertNotIn("containsOnlyForwardOrRotation(", validator)

        composite_begin = planner.index(
            "if (theta_prefix_lattice_suffix_enabled_ && coarse_route.succeeded()"
        )
        composite_end = planner.index(
            "if (!path_found && theta_corridor_search_enabled_", composite_begin
        )
        composite = planner[composite_begin:composite_end]
        motion_audit = composite.index(
            "containsKinematicallyContinuousForwardOrRotation("
        )
        footprint_proof = composite.index("validateContinuousPath(", motion_audit)
        state_search = composite.index("runStateSearch(", footprint_proof)
        self.assertLess(motion_audit, footprint_proof)
        self.assertLess(footprint_proof, state_search)

        validator_candidate = validator.index(
            "containsKinematicallyContinuousForwardOrRotation(\n"
            "                cut.prefix_including_cut"
        )
        validator_proof = validator.index(
            "validateContinuousPosePath(", validator_candidate
        )
        self.assertLess(validator_candidate, validator_proof)

    def test_composite_is_committed_and_published_only_after_complete_proofs(self):
        source = _read(PLANNER_SOURCE)
        composite_begin = source.index(
            "if (theta_prefix_lattice_suffix_enabled_ && coarse_route.succeeded()"
        )
        composite_end = source.index(
            "if (!path_found && theta_corridor_search_enabled_", composite_begin
        )
        composite = source[composite_begin:composite_end]

        # A failed suffix candidate never publishes or commits its Theta prefix.
        self.assertNotIn("publishPlan(", composite)
        proof = composite.index("std::string composite_reason;")
        commit = composite.index("plan = std::move(composite);")
        self.assertIn("validateContinuousPath(", composite[proof:commit])
        self.assertLess(proof, commit)

        # There is one success publication site, after both the immutable
        # planning-snapshot proof and the fresh live-snapshot proof.
        self.assertEqual(source.count("publishPlan(plan);"), 1)
        make_plan_begin = source.index("uint32_t SmacLatticePlanner::makePlan(")
        first_attempt_clear = source.index("plan.clear();", make_plan_begin)
        retry_loop = source.index("for (int attempt = 0;", first_attempt_clear)
        retry_clear = source.index("plan.clear();", retry_loop)
        planning_proof = source.index(
            "std::string validation_reason;", composite_end
        )
        live_snapshot = source.index("auto live_costmap =", planning_proof)
        live_proof = source.index("if (!validateContinuousPath(", live_snapshot)
        publish = source.index("publishPlan(plan);", live_proof)
        self.assertLess(first_attempt_clear, retry_loop)
        self.assertLess(retry_loop, retry_clear)
        self.assertLess(planning_proof, live_snapshot)
        self.assertLess(live_snapshot, live_proof)
        self.assertLess(live_proof, publish)

    def test_full_fallback_requires_success_and_a_bounded_quantized_terminal(self):
        source = _read(PLANNER_SOURCE)
        composite_begin = source.index(
            "if (theta_prefix_lattice_suffix_enabled_ && coarse_route.succeeded()"
        )
        full_begin = source.index("if (!path_found) {", composite_begin)
        full_end = source.index("std::string validation_reason;", full_begin)
        full_block = source[full_begin:full_end]

        self.assertIn('"FULL_EXACT", full_available, 0.0f', full_block)
        self.assertRegex(
            full_block,
            r"result\.termination\s*==\s*"
            r"nav2_smac_planner::SearchTermination::SUCCESS\s*&&\s*"
            r"result\.hasPath\(\)",
        )
        self.assertIn('selected_search_mode = "FULL_EXACT";', full_block)
        self.assertIn(
            "coordinatesToPosePlan(path, start, plan)", full_block
        )

        # State search ends at the nearest lattice heading bin. Conversion must
        # preserve that output and never append or overwrite it with the
        # requested continuous yaw as a same-XY repair.
        self.assertIn(
            "goal_x, goal_y, goal_bin, goal_heading_mode_, coarse_search_resolution_);",
            source,
        )
        conversion_begin = source.index("const auto coordinatesToPosePlan")
        conversion_end = source.index("bool path_found = false;", conversion_begin)
        conversion = source[conversion_begin:conversion_end]
        self.assertNotIn("replace_with_exact_goal", conversion)
        self.assertNotIn("geometry_msgs::PoseStamped exact_goal = goal;", conversion)
        self.assertNotIn("output.back() =", conversion)
        self.assertNotIn("output.push_back(std::move(exact_goal));", conversion)
        self.assertIn("terminalAvoidsStationaryYawRepair(", source)
        self.assertIn("setGoalTransitionValidator(", source)
        self.assertGreaterEqual(source.count("clearGoalTransitionValidator();"), 2)
        a_star = _read(A_STAR_IMPL)
        transition_filter = a_star.index("if (_goal_transition_validator")
        queue_insert = a_star.index("addNode(g_cost + getHeuristicCost(neighbor)")
        self.assertLess(transition_filter, queue_insert)

        terminal_begin = source.index("const double end_distance =", full_end)
        publish = source.index("publishPlan(plan);", terminal_begin)
        terminal = source[terminal_begin:publish]
        self.assertIn(
            "std::sqrt(2.0) * planning_costmap_->getResolution() + 1e-3",
            terminal,
        )
        self.assertIn("constexpr double kRequestedYawTolerance = 0.20;", terminal)
        self.assertIn("M_PI / static_cast<double>(heading_count)", terminal)
        self.assertRegex(
            terminal,
            r"if\s*\(end_distance\s*>\s*max_lattice_position_residual\s*\|\|\s*"
            r"end_yaw_error\s*>\s*kRequestedYawTolerance\s*\|\|\s*"
            r"end_yaw_error\s*>\s*max_nearest_bin_residual\)",
        )
        self.assertIn("plan.clear();", terminal)

    def test_prefix_candidate_and_both_snapshot_proofs_are_deadline_bounded(self):
        source = _read(PLANNER_SOURCE)

        validation_begin = source.index("bool SmacLatticePlanner::validateContinuousPath(")
        validation_end = source.index(
            "uint32_t SmacLatticePlanner::makePlan(", validation_begin
        )
        validation = source[validation_begin:validation_end]
        self.assertIn(
            "const std::chrono::steady_clock::time_point * deadline", validation
        )
        self.assertIn(
            "deadline != nullptr && std::chrono::steady_clock::now() >= *deadline",
            validation,
        )

        candidate_begin = source.index("auto coordinatePathIsSafe =")
        candidate_end = source.index(
            "const auto coordinatesToPosePlan", candidate_begin
        )
        candidate_validator = source[candidate_begin:candidate_end]
        self.assertIn(
            "const PlanningClock::time_point & proof_deadline",
            candidate_validator,
        )
        self.assertIn(
            "PlanningClock::now() >= proof_deadline", candidate_validator
        )

        composite_begin = source.index(
            "if (theta_prefix_lattice_suffix_enabled_ && coarse_route.succeeded()"
        )
        composite_end = source.index(
            "if (!path_found && theta_corridor_search_enabled_", composite_begin
        )
        composite = source[composite_begin:composite_end]
        self.assertRegex(
            composite,
            r"selection\.prefix_including_cut,\s*prefix_reason,\s*"
            r"&candidate_deadline,\s*&first_unsafe_segment",
        )
        self.assertRegex(
            composite,
            r"path,\s*candidate_deadline,\s*suffix_reason",
        )
        self.assertRegex(
            composite,
            r"composite,\s*composite_reason,\s*&candidate_deadline",
        )
        self.assertIn(
            "(selection_is_adaptive || selection_is_short_route_all_state) ?\n"
            "            search_deadline : corridor_deadline",
            composite,
        )
        self.assertIn(
            "(selection_is_adaptive || selection_is_short_route_all_state) ?\n"
            "            available_after_proof",
            composite,
        )
        self.assertIn("const double available_after_proof", composite)

        # FULL candidate proof and the two final complete-plan proofs all share
        # the one wall-clock deadline. Validation cannot extend planning time.
        self.assertRegex(
            source,
            r"path,\s*overall_deadline,\s*candidate_reason",
        )
        self.assertEqual(
            len(
                re.findall(
                    r"plan,\s*validation_reason,\s*&overall_deadline",
                    source,
                )
            ),
            2,
        )

    def test_validator_requires_new_snapshot_and_charges_proofs_to_deadline(self):
        source = _read(VALIDATOR_SOURCE)
        self.assertIn("bool fresh_snapshot_revalidation = true;", source)
        self.assertIn("candidate->header.stamp > message->header.stamp", source)
        self.assertIn("candidate->header.seq > message->header.seq", source)
        self.assertIn("strictly newer global costmap snapshot", source)
        self.assertIn("const double available_after_proof", source)
        self.assertIn(
            "std::min(theta_suffix_candidate_max_planning_time, available_after_proof)",
            source,
        )
        self.assertIn(
            "(cut_is_adaptive || cut_is_short_route_all_state) ?\n"
            "              search_deadline : corridor_deadline",
            source,
        )
        self.assertIn("&candidate_deadline", source)
        self.assertIn("&overall_deadline", source)


if __name__ == "__main__":
    unittest.main()

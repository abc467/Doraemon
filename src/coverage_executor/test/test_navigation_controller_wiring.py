#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Retired v10 cross-package navigation wiring contract.

Production now intentionally uses the field-validated v9 MPPI module and its
30-step parameter baseline.  Keep this module only as historical reference;
its v10 validator and 50-step assertions must not gate production builds.
"""

import os
import unittest
import xml.etree.ElementTree as ET

import yaml


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
EXECUTOR_DIR = os.path.dirname(THIS_DIR)
SOURCE_DIR = os.path.dirname(EXECUTOR_DIR)


def _read(relative_path):
    with open(os.path.join(SOURCE_DIR, relative_path), "r", encoding="utf-8") as handle:
        return handle.read()


def _nav_config():
    with open(
        os.path.join(SOURCE_DIR, "cleanrobot", "config", "nav", "mbf_nav.yaml"),
        "r",
        encoding="utf-8",
    ) as handle:
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


@unittest.skip(
    "retired v10 MPPI wiring contract; production baseline is mppi_v9_ab.yaml"
)
class NavigationControllerWiringTest(unittest.TestCase):
    def test_standard_uses_five_second_coverage_horizon_and_orientation_alignment(self):
        config = _nav_config()
        standard = config["MPPI_Standard_Controller"]

        self.assertEqual(int(standard["time_steps"]), 50)
        self.assertAlmostEqual(float(standard["model_dt"]), 0.10)
        self.assertAlmostEqual(float(standard["controller_frequency"]), 10.0)
        self.assertEqual(int(standard["batch_size"]), 1800)
        self.assertTrue(standard["publish_critic_stats"])
        self.assertEqual(int(standard["critic_stats_publish_period"]), 10)
        self.assertNotIn("path_occupancy_uses_footprint", standard)
        self.assertAlmostEqual(float(standard["vx_std"]), 0.15)
        self.assertAlmostEqual(float(standard["wz_std"]), 0.30)
        self.assertAlmostEqual(float(standard["vx_max"]), 0.36)
        self.assertAlmostEqual(float(standard["wz_max"]), 0.60)
        self.assertAlmostEqual(float(standard["prune_distance"]), 2.50)
        self.assertAlmostEqual(
            float(standard["max_robot_pose_search_dist"]), 2.50
        )
        self.assertAlmostEqual(
            float(standard["time_steps"]) * float(standard["model_dt"]),
            5.0,
        )
        self.assertAlmostEqual(
            float(standard["vx_max"])
            * float(standard["time_steps"])
            * float(standard["model_dt"]),
            1.80,
        )
        predicted_pose_count = int(standard["time_steps"]) * int(
            standard["batch_size"]
        )
        self.assertEqual(predicted_pose_count, 90000)
        self.assertLessEqual(predicted_pose_count, 108000)
        maximum_travel = (
            float(standard["vx_max"])
            * float(standard["time_steps"])
            * float(standard["model_dt"])
        )
        self.assertGreaterEqual(float(standard["prune_distance"]), maximum_travel + 0.5)

        with open(
            os.path.join(SOURCE_DIR, "cleanrobot", "config", "nav", "local_costmap.yaml"),
            "r",
            encoding="utf-8",
        ) as handle:
            local_costmap = yaml.safe_load(handle)["local_costmap"]
        with open(
            os.path.join(SOURCE_DIR, "cleanrobot", "config", "nav", "costmap_common.yaml"),
            "r",
            encoding="utf-8",
        ) as handle:
            footprint = yaml.safe_load(handle)["footprint"]
        footprint_radius = max((x * x + y * y) ** 0.5 for x, y in footprint)
        local_half_width = min(
            float(local_costmap["width"]), float(local_costmap["height"])
        ) / 2.0
        self.assertLess(maximum_travel + footprint_radius, local_half_width)
        self.assertAlmostEqual(float(standard["temperature"]), 0.30)
        path_align = standard["PathAlignCritic"]
        self.assertAlmostEqual(float(path_align["cost_weight"]), 6.0)
        self.assertEqual(int(path_align["offset_from_furthest"]), 6)
        self.assertAlmostEqual(
            float(path_align["max_path_occupancy_ratio"]), 0.07
        )
        self.assertEqual(int(path_align["trajectory_point_step"]), 4)
        self.assertTrue(path_align["use_path_orientations"])
        self.assertNotIn("use_footprint_for_path_alignment_gate", path_align)
        self.assertAlmostEqual(
            float(standard["CostCritic"]["cost_weight"]), 12.0
        )
        self.assertTrue(standard["CostCritic"]["allow_unknown"])
        self.assertTrue(standard["CostCritic"]["consider_footprint"])
        self.assertEqual(
            int(standard["CostCritic"]["trajectory_point_step"]), 1
        )
        path_angle = standard["PathAngleCritic"]
        self.assertNotIn("vx_min", path_angle)
        self.assertEqual(int(path_angle["offset_from_furthest"]), 4)
        self.assertAlmostEqual(float(path_angle["cost_weight"]), 2.2)
        self.assertAlmostEqual(float(path_angle["max_angle_to_furthest"]), 0.8)
        path_follow = standard["PathFollowCritic"]
        self.assertEqual(int(path_follow["offset_from_furthest"]), 6)
        self.assertAlmostEqual(float(path_follow["cost_weight"]), 7.0)

    def test_path_align_and_follow_share_official_center_point_validity(self):
        utils = _read("mppi_controller/include/mppi_controller/tools/utils.hpp")
        path_align = _read("mppi_controller/src/critics/path_align_critic.cpp")
        path_follow = _read("mppi_controller/src/critics/path_follow_critic.cpp")
        align_header = _read(
            "mppi_controller/include/mppi_controller/critics/path_align_critic.hpp"
        )

        self.assertIn("inline void findPathCosts(", utils)
        self.assertIn("data.path_pts_valid = std::vector<bool>", utils)
        self.assertIn("pathFollowTargetIndex", utils)
        self.assertIn("pathFollowTargetIndex", path_follow)
        self.assertIn("setPathCostsIfNotSet(data, costmap_ros_)", path_align)
        self.assertIn("setPathCostsIfNotSet(data, costmap_ros_)", path_follow)
        self.assertNotIn("path_occupancy_uses_footprint_", path_align)
        self.assertNotIn("path_occupancy_uses_footprint_", path_follow)
        self.assertNotIn("path_occupancy_uses_footprint_", align_header)
        self.assertNotIn("firstValidPathPointAtOrAfter", path_follow)
        self.assertNotIn("std::nullopt", utils)
        self.assertNotIn("evaluatePathValidity(\n        data", path_align)
        self.assertNotIn("use_footprint_for_path_alignment_gate", path_align)
        self.assertNotIn("use_footprint_for_path_alignment_gate", align_header)

    def test_only_standard_mppi_is_registered_without_yaml_inheritance(self):
        config = _nav_config()
        registered = {
            item["name"]
            for item in config["controllers"]
            if item["type"] == "local_planner/MPPIController"
        }
        self.assertEqual(
            registered,
            {"MPPI_Standard_Controller"},
        )
        configured_mppi_instances = {
            key for key in config if key.startswith("MPPI_")
        }
        self.assertEqual(configured_mppi_instances, {"MPPI_Standard_Controller"})

        yaml_source = _read("cleanrobot/config/nav/mbf_nav.yaml")
        self.assertNotIn("<<:", yaml_source)
        self.assertNotIn("&mppi_", yaml_source)
        self.assertNotIn("MPPI_Clean_Controller", yaml_source)
        self.assertNotIn("MPPI_Heavy_Clean_Controller", yaml_source)

        for controller_name in registered:
            self.assertNotIn("vx_min", config[controller_name]["PathAngleCritic"])

        launch_source = _read("cleanrobot/launch/mbf_nav.launch")
        self.assertNotIn("enable_mppi_ab_controllers", launch_source)
        self.assertNotIn("mppi_ab_controllers_yaml", launch_source)

    def test_all_coverage_modes_select_the_single_standard_controller(self):
        profiles_path = os.path.join(
            SOURCE_DIR, "coverage_task_manager", "config", "mode_profiles.yaml"
        )
        with open(profiles_path, "r", encoding="utf-8") as handle:
            profiles = yaml.safe_load(handle)["mode_profiles"]
        self.assertEqual(set(profiles), {"standard", "heavy", "eco"})
        for name, profile in profiles.items():
            self.assertEqual(
                profile["mbf_controller_name"],
                "MPPI_Standard_Controller",
                msg=name,
            )

        catalog_source = _read(
            "coverage_executor/src/coverage_executor/sys_profile_catalog.py"
        )
        self.assertIn('mbf_controller_name="MPPI_Standard_Controller"', catalog_source)
        self.assertNotIn('f"MPPI_{key.capitalize()}_Controller"', catalog_source)

    def test_critic_statistics_are_subscriber_gated_and_default_off(self):
        header = _read("mppi_controller/include/mppi_controller/critic_manager.hpp")
        source = _read("mppi_controller/src/critic_manager.cpp")
        self.assertIn('param("publish_critic_stats", publish_critic_stats_, false)', source)
        self.assertIn("getNumSubscribers() > 0u", source)
        self.assertIn('"critic_stats"', source)
        self.assertIn("cost_mean", source)
        self.assertIn("changed_ratio", source)
        self.assertIn("score_time_mean_ms", source)
        self.assertIn("publish_critic_stats_{false}", header)

    def test_local_costmap_is_stable_and_fail_closed(self):
        nav_dir = os.path.join(SOURCE_DIR, "cleanrobot", "config", "nav")
        with open(os.path.join(nav_dir, "local_costmap.yaml"), "r", encoding="utf-8") as handle:
            local = yaml.safe_load(handle)["local_costmap"]
        self.assertEqual(local["global_frame"], "odom")
        self.assertTrue(local["rolling_window"])
        self.assertTrue(local["track_unknown_space"])
        self.assertAlmostEqual(float(local["update_frequency"]), 15.0)
        self.assertAlmostEqual(float(local["publish_frequency"]), 15.0)
        self.assertAlmostEqual(float(local["width"]), 6.0)
        self.assertAlmostEqual(float(local["height"]), 6.0)
        self.assertAlmostEqual(float(local["resolution"]), 0.05)
        plugin_names = [item["name"] for item in local["plugins"]]
        self.assertIn("obstacle_layer", plugin_names)
        self.assertNotIn("camera_obstacle_layer", plugin_names)
        sources = set(local["obstacle_layer"]["observation_sources"].split())
        self.assertEqual(
            sources,
            {
                "laser_scan_sensor",
                "left_cam_source",
                "right_cam_source",
                "up_cam_source",
            },
        )

    def test_point_to_point_uses_smac_and_standard_mppi(self):
        executor_launch = os.path.join(EXECUTOR_DIR, "launch", "executor.launch")
        task_launch = os.path.join(
            SOURCE_DIR, "coverage_task_manager", "launch", "task_manager.launch"
        )
        self.assertEqual(
            _node_param(executor_launch, "coverage_executor", "mbf_planner"),
            "SmacLatticePlanner",
        )
        self.assertEqual(
            _node_param(executor_launch, "coverage_executor", "mbf_connect_controller"),
            "MPPI_Standard_Controller",
        )
        self.assertEqual(
            _node_param(task_launch, "coverage_task_manager", "mbf_planner"),
            "SmacLatticePlanner",
        )

    def test_connect_handoff_uses_the_production_mbf_tolerance(self):
        config = _nav_config()
        standard = config["MPPI_Standard_Controller"]
        with open(
            os.path.join(SOURCE_DIR, "cleanrobot", "config", "nav", "mbf_timing.yaml"),
            "r",
            encoding="utf-8",
        ) as handle:
            mbf_timing = yaml.safe_load(handle)

        self.assertAlmostEqual(float(standard["goal_tolerance"]), 0.60)
        self.assertAlmostEqual(float(standard["angle_tolerance"]), 0.40)
        self.assertAlmostEqual(float(mbf_timing["dist_tolerance"]), 0.60)
        self.assertAlmostEqual(float(mbf_timing["angle_tolerance"]), 0.52)

        # Standard must never be looser than MBF's requested-target gate. Smac
        # endpoint quantization is handled by that independent gate: it may
        # reject a miss, but it cannot report a false CONNECT success.
        self.assertLessEqual(
            float(standard["goal_tolerance"]),
            float(mbf_timing["dist_tolerance"]),
        )
        self.assertLessEqual(
            float(standard["angle_tolerance"]),
            float(mbf_timing["angle_tolerance"]),
        )

        executor_launch = os.path.join(EXECUTOR_DIR, "launch", "executor.launch")
        executor_dist = float(
            _node_param(executor_launch, "coverage_executor", "connect_handoff_dist_m")
        )
        executor_yaw = float(
            _node_param(executor_launch, "coverage_executor", "connect_handoff_yaw_rad")
        )
        self.assertAlmostEqual(executor_dist, float(mbf_timing["dist_tolerance"]))
        self.assertAlmostEqual(executor_yaw, float(mbf_timing["angle_tolerance"]))

        node_source = _read("coverage_executor/scripts/executor_node.py")
        fsm_source = _read("coverage_executor/src/coverage_executor/fsm.py")
        self.assertIn(
            'get_param("~connect_handoff_dist_m", 0.60)', node_source
        )
        self.assertIn(
            'get_param("~connect_handoff_yaw_rad", 0.52)', node_source
        )
        self.assertIn("connect_handoff_dist_m: float = 0.60", fsm_source)
        self.assertIn("connect_handoff_yaw_rad: float = 0.52", fsm_source)

    def test_state_planner_paths_are_executed_by_standard_controller(self):
        config = _nav_config()
        smac = config["SmacLatticePlanner"]
        self.assertFalse(smac["allow_reverse_expansion"])
        self.assertTrue(smac["theta_prefix_lattice_suffix_enabled"])
        self.assertFalse(smac["theta_corridor_search_enabled"])
        self.assertAlmostEqual(float(smac["max_planning_time"]), 180.0)
        self.assertIn("MPPI_Standard_Controller", config)

    def test_removed_non_upstream_parameters_are_not_loaded(self):
        config = _nav_config()
        forbidden = {
            "terminal_path_retention_distance",
            "terminal_reapproach_enabled",
            "straight_reposition_enabled",
            "connect_no_progress_timeout",
            "safe_candidate_fallback_count",
            "fixed_center_goal_rotation_enabled",
            "tracking_vx_min",
            "tracking_vx_max",
            "tracking_wz_max",
            "max_evaluation_time",
            "use_path_distance",
            "consider_path_footprint",
            "rotate_to_goal_enabled",
        }
        for controller_name in ("MPPI_Standard_Controller",):
            controller = config[controller_name]
            self.assertTrue(forbidden.isdisjoint(controller.keys()))
            for critic_name in controller.get("critics", []):
                critic = controller.get(critic_name, {})
                self.assertTrue(forbidden.isdisjoint(critic.keys()))

    def test_controller_is_a_thin_nav_core_adapter(self):
        header = _read("mppi_controller/include/mppi_controller/mppi_controller.hpp")
        source = _read("mppi_controller/src/mppi_controller.cpp")
        self.assertIn("public nav_core::BaseLocalPlanner", header)
        self.assertIn("optimizer_->evalControl(", source)
        self.assertIn("ControllerGoalToleranceAware", header)
        self.assertIn("goal_reached_evaluator_", header)
        self.assertIn("setZeroCommand(command);", source)
        self.assertIn("speedLimitScaleCallback", header + source)
        self.assertIn("optimizer_->setSpeedLimit(scale * 100.0, true)", source)
        self.assertIn("reload_parameters", source)
        self.assertIn("optimizer_.swap(replacement)", source)
        self.assertIn("path_handler_.reloadParameters()", source)
        self.assertIn("path_handler_.transformPath(robot_pose)", source)
        self.assertIn("path_handler_.getTransformedGoal().pose", source)
        self.assertNotIn("PathMotionPhase", header + source)
        self.assertNotIn("PlanExecutionContextAware", header + source)
        self.assertNotIn("terminalReapproach", header + source)
        self.assertNotIn("Reposition", header + source)
        self.assertNotIn("connectTracking", header + source)
        self.assertNotIn("rotate_to_goal", (header + source).lower())

    def test_mbf_goal_tolerance_cannot_bypass_controller_stopped_gate(self):
        abstract_controller = _read(
            "external_navigation/move_base_flex/mbf_abstract_core/"
            "include/mbf_abstract_core/abstract_controller.h"
        )
        wrapper_header = _read(
            "external_navigation/move_base_flex/mbf_costmap_nav/"
            "include/nav_core_wrapper/wrapper_local_planner.h"
        )
        wrapper = _read(
            "external_navigation/move_base_flex/mbf_costmap_nav/"
            "src/nav_core_wrapper/wrapper_local_planner.cpp"
        )
        execution = _read(
            "external_navigation/move_base_flex/mbf_abstract_nav/"
            "src/abstract_controller_execution.cpp"
        )
        self.assertIn("usesInternalGoalReachedPolicy", abstract_controller)
        self.assertIn("usesInternalGoalReachedPolicy", wrapper_header + wrapper)
        self.assertIn("ControllerGoalToleranceAware", wrapper)
        self.assertIn(
            "mbf_tolerance_check_ && !controller_->usesInternalGoalReachedPolicy()",
            execution,
        )

    def test_path_handler_matches_upstream_nearest_prune_contract(self):
        header = _read("mppi_controller/include/mppi_controller/tools/path_handler.hpp")
        source = _read("mppi_controller/src/path_handler.cpp")
        self.assertIn("findClosestPathPose", header + source)
        self.assertIn("closest == std::prev(end)", source)
        self.assertIn("closest = std::prev(closest)", source)
        self.assertIn("prunePlan(remaining_global_plan_, window.second)", source)
        self.assertNotIn("terminal_path_retention", header + source)
        self.assertNotIn("motion_phase", (header + source).lower())

    def test_optimizer_preserves_upstream_update_order_and_warm_start(self):
        controller = _read("mppi_controller/src/mppi_controller.cpp")
        optimizer = _read("mppi_controller/src/optimizer.cpp")
        set_plan = controller.split("bool MPPIController::setPlan", 1)[1].split(
            "bool MPPIController::computeVelocityCommands", 1
        )[0]
        self.assertNotIn("optimizer_.reset", set_plan)

        update = optimizer.split("void Optimizer::updateControlSequence", 1)[1].split(
            "bool Optimizer::validateOptimizedTrajectory", 1
        )[0]
        self.assertLess(update.index("savitskyGolayFilter"), update.index("applyControlSequenceConstraints"))
        self.assertIn("applyControlSequenceInterIterationConstraints();", optimizer)
        self.assertIn("state_.local_path_length", optimizer)
        self.assertIn("model_delay_vx", optimizer)
        self.assertIn("pushCommandHistory", optimizer)
        self.assertIn("regenerate_noises", optimizer)
        self.assertIn("throw std::runtime_error", optimizer)

    def test_goal_and_path_critics_use_local_path_length(self):
        for filename in (
            "goal_critic.cpp",
            "goal_angle_critic.cpp",
            "path_align_critic.cpp",
            "path_angle_critic.cpp",
            "path_follow_critic.cpp",
        ):
            source = _read("mppi_controller/src/critics/" + filename)
            self.assertIn("data.state.local_path_length", source)
            self.assertNotIn("use_path_distance", source)

        prefer_forward = _read(
            "mppi_controller/src/critics/prefer_forward_critic.cpp"
        )
        self.assertIn("data.state.local_path_length", prefer_forward)
        self.assertNotIn("withinPositionGoalTolerance", prefer_forward)

    def test_constraint_critic_uses_upstream_axis_wise_limits(self):
        header = _read(
            "mppi_controller/include/mppi_controller/critics/constraint_critic.hpp"
        )
        source = _read("mppi_controller/src/critics/constraint_critic.cpp")
        for member in ("vx_max_", "vx_min_", "vy_max_"):
            self.assertIn(member, header + source)
        self.assertIn("(vy.abs() - vy_max_).max(0.0f)", source)
        self.assertNotIn("vel_total", source)

    def test_selected_trajectory_has_independent_continuous_safety_gate(self):
        optimizer = _read("mppi_controller/src/optimizer.cpp")
        validator = _read("mppi_controller/src/optimal_trajectory_validator.cpp")
        validator_header = _read(
            "mppi_controller/include/mppi_controller/optimal_trajectory_validator.hpp"
        )
        footprint_gate = _read(
            "mppi_controller/include/mppi_controller/tools/footprint_collision.hpp"
        )
        self.assertIn("optimal_trajectory = getOptimizedTrajectory()", optimizer)
        self.assertIn("validateOptimizedTrajectory(optimal_trajectory)", optimizer)
        self.assertIn("std::make_tuple(control, std::move(optimal_trajectory))", optimizer)
        self.assertIn("std::get<1>(result)", _read("mppi_controller/src/mppi_controller.cpp"))
        self.assertIn("getFootprintCells", footprint_gate)
        self.assertIn("isFootprintPoseHardCollisionFree", validator)
        self.assertIn("DefaultOptimalTrajectoryValidator", validator_header + validator)
        self.assertIn("validator_loader_->createUnmanagedInstance", optimizer)
        self.assertIn("true);", footprint_gate)  # filled footprint
        self.assertIn("NO_INFORMATION", footprint_gate)
        self.assertIn("corner_motion", validator)

    def test_mppi_profiles_publish_softmax_without_candidate_substitution(self):
        config = _nav_config()
        standard = config["MPPI_Standard_Controller"]
        for controller_name in ("MPPI_Standard_Controller",):
            self.assertNotIn("SafeCandidateRescue", config[controller_name])

        optimizer = _read("mppi_controller/src/optimizer.cpp")
        optimizer_header = _read("mppi_controller/include/mppi_controller/optimizer.hpp")
        self.assertNotIn("SafeCandidateRescue", optimizer + optimizer_header)
        self.assertNotIn("safe_candidate_rescue", optimizer + optimizer_header)
        self.assertFalse(
            os.path.exists(
                os.path.join(
                    SOURCE_DIR,
                    "mppi_controller",
                    "src",
                    "safe_candidate_rescue.cpp",
                )
            )
        )
        self.assertIn("fallback(needs_fallback)", optimizer)
        self.assertIn(
            '"TrajectoryValidator/enabled", trajectory_validation_enabled_',
            optimizer,
        )
        self.assertIn("if (!trajectory_validation_enabled_)", optimizer)

    def test_ros1_port_exposes_current_upstream_optimizer_capabilities(self):
        settings = _read(
            "mppi_controller/include/mppi_controller/models/optimizer_settings.hpp"
        )
        motion_model = _read("mppi_controller/include/mppi_controller/motion_models.hpp")
        critic_root = ET.fromstring(_read("mppi_controller/critics.xml"))
        critic_types = {node.get("type") for node in critic_root.findall(".//class")}

        for capability in (
            "open_loop",
            "regenerate_noises",
            "clamp_raw_controls",
            "model_delay_vx",
            "model_delay_vy",
            "model_delay_wz",
            "sgf_order",
        ):
            self.assertIn(capability, settings)
        self.assertIn("pushCommandHistory", motion_model)
        self.assertIn("applyDelayShift", motion_model)
        self.assertIn("mppi::critics::ObstaclesCritic", critic_types)
        self.assertIn("mppi::critics::VelocityDeadbandCritic", critic_types)


if __name__ == "__main__":
    unittest.main()

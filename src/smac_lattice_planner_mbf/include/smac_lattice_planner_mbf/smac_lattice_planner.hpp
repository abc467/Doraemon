// Copyright 2026 Clean Robot Navigation Team
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <costmap_2d/costmap_2d_ros.h>
#include <geometry_msgs/PoseStamped.h>
#include <mbf_costmap_core/costmap_planner.h>
#include <ros/ros.h>

#include "nav2_smac_planner/a_star.hpp"
#include "nav2_smac_planner/collision_checker.hpp"
#include "nav2_smac_planner/node_lattice.hpp"
#include "smac_lattice_planner_mbf/coarse_route_corridor.hpp"
#include "smac_lattice_planner_mbf/state_lattice_smoother.hpp"
#include "smac_lattice_planner_mbf/theta_state_suffix.hpp"

namespace smac_lattice_planner_mbf
{

class SmacLatticePlanner : public mbf_costmap_core::CostmapPlanner
{
public:
  SmacLatticePlanner() = default;
  SmacLatticePlanner(std::string name, costmap_2d::Costmap2DROS * costmap_ros);

  void initialize(
    std::string name,
    costmap_2d::Costmap2DROS * costmap_ros) override;

  uint32_t makePlan(
    const geometry_msgs::PoseStamped & start,
    const geometry_msgs::PoseStamped & goal,
    double tolerance,
    std::vector<geometry_msgs::PoseStamped> & plan,
    double & cost,
    std::string & message) override;

  bool cancel() override;

private:
  bool waitForCurrentCostmap(
    std::string & reason,
    const std::chrono::steady_clock::time_point * overall_deadline = nullptr) const;
  std::unique_ptr<costmap_2d::Costmap2D> captureCostmapSnapshot(
    std::vector<geometry_msgs::Point> & footprint) const;
  static bool worldToMapContinuous(
    const costmap_2d::Costmap2D & costmap,
    double wx, double wy, float & mx, float & my);
  bool validateContinuousPath(
    const costmap_2d::Costmap2D & costmap,
    nav2_smac_planner::GridCollisionChecker & collision_checker,
    const std::vector<geometry_msgs::PoseStamped> & plan,
    std::string & reason,
    const std::chrono::steady_clock::time_point * deadline = nullptr,
    std::size_t * first_unsafe_segment = nullptr) const;
  void publishPlan(const std::vector<geometry_msgs::PoseStamped> & plan) const;

  bool initialized_{false};
  std::string name_;
  std::string global_frame_;
  std::string lattice_filepath_;
  costmap_2d::Costmap2DROS * costmap_ros_{nullptr};
  costmap_2d::Costmap2D * costmap_{nullptr};

  nav2_smac_planner::SearchInfo search_info_;
  nav2_smac_planner::LatticeMetadata metadata_;
  std::unique_ptr<costmap_2d::Costmap2D> planning_costmap_;
  std::unique_ptr<nav2_smac_planner::GridCollisionChecker> collision_checker_;
  std::unique_ptr<
    nav2_smac_planner::AStarAlgorithm<nav2_smac_planner::NodeLattice>> a_star_;
  std::unique_ptr<StateLatticeSmoother> state_lattice_smoother_;

  bool allow_unknown_{false};
  int max_iterations_{1000000};
  int max_on_approach_iterations_{1000};
  int terminal_checking_interval_{100};
  double max_planning_time_{180.0};
  double lookup_table_size_{20.0};
  double tolerance_{0.10};
  double collision_check_resolution_{0.01};
  double costmap_current_timeout_{3.0};
  double live_validation_retry_min_time_{0.25};
  float lookup_table_dim_{0.0f};
  int live_validation_retries_{1};
  int collision_checker_angle_bins_{72};
  int coarse_search_resolution_{1};
  int start_heading_seed_span_{1};
  bool theta_corridor_search_enabled_{false};
  bool theta_prefix_lattice_suffix_enabled_{true};
  bool theta_full_footprint_validation_enabled_{true};
  bool state_lattice_smoothing_enabled_{false};
  StateLatticeSmootherParams state_lattice_smoother_params_;
  int theta_max_allowed_cost_{26};
  double theta_w_traversal_cost_{8.0};
  double theta_w_euc_cost_{2.0};
  double theta_w_heuristic_cost_{1.0};
  double theta_reference_spacing_{0.05};
  bool theta_reference_smoothing_enabled_{true};
  double theta_suffix_candidate_max_planning_time_{60.0};
  double theta_prefix_candidate_max_planning_time_{20.0};
  double theta_prefix_join_max_heading_error_{0.20};
  double theta_prefix_join_max_curvature_jump_{2.5};
  int theta_unsafe_segment_lookback_points_{30};
  double coarse_route_max_planning_time_{5.0};
  double corridor_level_max_planning_time_{10.0};
  double corridor_full_search_min_time_{12.0};
  double final_validation_reserve_time_{5.0};
  double corridor_route_progress_weight_{1.0};
  nav2_smac_planner::GoalHeadingMode goal_heading_mode_{
    nav2_smac_planner::GoalHeadingMode::DEFAULT};

  std::atomic<bool> cancel_requested_{false};
  mutable std::mutex planning_mutex_;
  ros::Publisher plan_publisher_;
};

}  // namespace smac_lattice_planner_mbf

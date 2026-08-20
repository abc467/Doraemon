// Copyright 2026 Clean Robot Navigation Team
// Licensed under the Apache License, Version 2.0.

#include "smac_lattice_planner_mbf/smac_lattice_planner.hpp"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <limits>
#include <stdexcept>
#include <utility>

#include <angles/angles.h>
#include <mbf_msgs/GetPathResult.h>
#include <nav_msgs/Path.h>
#include <pluginlib/class_list_macros.h>
#include <ros/package.h>
#include <tf2/utils.h>
#include <tf2_geometry_msgs/tf2_geometry_msgs.h>

PLUGINLIB_EXPORT_CLASS(
  smac_lattice_planner_mbf::SmacLatticePlanner,
  mbf_costmap_core::CostmapPlanner)

namespace smac_lattice_planner_mbf
{

namespace
{
geometry_msgs::Quaternion quaternionFromYaw(double yaw)
{
  tf2::Quaternion quaternion;
  quaternion.setRPY(0.0, 0.0, yaw);
  return tf2::toMsg(quaternion);
}

double pathLength(const std::vector<geometry_msgs::PoseStamped> & plan)
{
  double length = 0.0;
  for (std::size_t i = 1; i < plan.size(); ++i) {
    length += std::hypot(
      plan[i].pose.position.x - plan[i - 1].pose.position.x,
      plan[i].pose.position.y - plan[i - 1].pose.position.y);
  }
  return length;
}

std::string resolvePackageUri(const std::string & uri)
{
  constexpr const char * prefix = "package://";
  if (uri.compare(0, std::char_traits<char>::length(prefix), prefix) != 0) {
    return uri;
  }
  const std::string remainder = uri.substr(std::char_traits<char>::length(prefix));
  const std::size_t separator = remainder.find('/');
  const std::string package = remainder.substr(0, separator);
  const std::string package_path = ros::package::getPath(package);
  if (package.empty() || package_path.empty()) {
    throw std::runtime_error("cannot resolve lattice package URI: " + uri);
  }
  if (separator == std::string::npos) {
    return package_path;
  }
  return package_path + remainder.substr(separator);
}

const char * searchTerminationName(nav2_smac_planner::SearchTermination termination)
{
  using nav2_smac_planner::SearchTermination;
  switch (termination) {
    case SearchTermination::SUCCESS:
      return "success";
    case SearchTermination::OPEN_EXHAUSTED:
      return "open_exhausted";
    case SearchTermination::TIMEOUT:
      return "timeout";
    case SearchTermination::ITERATION_LIMIT:
      return "iteration_limit";
    case SearchTermination::CANCELED:
      return "canceled";
  }
  return "unknown";
}
}  // namespace

SmacLatticePlanner::SmacLatticePlanner(
  std::string name,
  costmap_2d::Costmap2DROS * costmap_ros)
{
  initialize(std::move(name), costmap_ros);
}

void SmacLatticePlanner::initialize(
  std::string name,
  costmap_2d::Costmap2DROS * costmap_ros)
{
  std::lock_guard<std::mutex> lock(planning_mutex_);
  if (initialized_) {
    ROS_WARN("SmacLatticePlanner '%s' is already initialized", name_.c_str());
    return;
  }
  if (!costmap_ros || !costmap_ros->getCostmap()) {
    ROS_ERROR("SmacLatticePlanner '%s' received no costmap", name.c_str());
    return;
  }

  name_ = std::move(name);
  costmap_ros_ = costmap_ros;
  costmap_ = costmap_ros_->getCostmap();
  global_frame_ = costmap_ros_->getGlobalFrameID();
  ros::NodeHandle private_nh("~/" + name_);

  const std::string package_path = ros::package::getPath("smac_lattice_planner_mbf");
  lattice_filepath_ = package_path + "/config/diff_5cm_0p40m_32bins_forward.json";
  private_nh.param("lattice_filepath", lattice_filepath_, lattice_filepath_);
  private_nh.param("allow_unknown", allow_unknown_, false);
  private_nh.param("max_iterations", max_iterations_, 1000000);
  private_nh.param("max_on_approach_iterations", max_on_approach_iterations_, 1000);
  private_nh.param("terminal_checking_interval", terminal_checking_interval_, 100);
  private_nh.param("max_planning_time", max_planning_time_, 180.0);
  private_nh.param("lookup_table_size", lookup_table_size_, 20.0);
  private_nh.param("tolerance", tolerance_, 0.10);
  private_nh.param("collision_check_resolution", collision_check_resolution_, 0.01);
  private_nh.param("collision_checker_angle_bins", collision_checker_angle_bins_, 72);
  private_nh.param("coarse_search_resolution", coarse_search_resolution_, 1);
  private_nh.param("start_heading_seed_span", start_heading_seed_span_, 1);
  private_nh.param("costmap_current_timeout", costmap_current_timeout_, 3.0);
  private_nh.param("live_validation_retries", live_validation_retries_, 1);
  private_nh.param(
    "live_validation_retry_min_time", live_validation_retry_min_time_, 0.25);
  private_nh.param(
    "theta_corridor_search_enabled", theta_corridor_search_enabled_, false);
  private_nh.param(
    "theta_prefix_lattice_suffix_enabled",
    theta_prefix_lattice_suffix_enabled_, true);
  private_nh.param(
    "theta_full_footprint_validation_enabled",
    theta_full_footprint_validation_enabled_, true);
  private_nh.param(
    "state_lattice_smoothing_enabled",
    state_lattice_smoothing_enabled_, false);
  private_nh.param(
    "state_lattice_smoother_max_iterations",
    state_lattice_smoother_params_.max_iterations, 1000);
  private_nh.param(
    "state_lattice_smoother_w_data",
    state_lattice_smoother_params_.w_data, 0.2);
  private_nh.param(
    "state_lattice_smoother_w_smooth",
    state_lattice_smoother_params_.w_smooth, 0.3);
  private_nh.param(
    "state_lattice_smoother_tolerance",
    state_lattice_smoother_params_.tolerance, 1e-10);
  private_nh.param(
    "state_lattice_smoother_do_refinement",
    state_lattice_smoother_params_.do_refinement, true);
  private_nh.param(
    "state_lattice_smoother_refinement_num",
    state_lattice_smoother_params_.refinement_num, 2);
  private_nh.param(
    "state_lattice_smoother_max_time",
    state_lattice_smoother_params_.max_time, 1.0);
  private_nh.param(
    "state_lattice_smoother_max_path_length_ratio",
    state_lattice_smoother_params_.max_path_length_ratio, 1.05);
  private_nh.param(
    "state_lattice_smoother_max_center_cost_increase",
    state_lattice_smoother_params_.max_center_cost_increase, 0.0);
  private_nh.param(
    "state_lattice_smoother_max_mean_center_cost_increase",
    state_lattice_smoother_params_.max_mean_center_cost_increase, 0.5);
  private_nh.param(
    "state_lattice_smoother_max_curvature_regression_ratio",
    state_lattice_smoother_params_.max_curvature_regression_ratio, 1.05);
  private_nh.param(
    "state_lattice_smoother_minimum_curvature_improvement",
    state_lattice_smoother_params_.minimum_curvature_improvement, 1e-3);
  private_nh.param(
    "theta_max_allowed_cost", theta_max_allowed_cost_, 26);
  private_nh.param(
    "theta_w_traversal_cost", theta_w_traversal_cost_, 8.0);
  private_nh.param(
    "theta_w_euc_cost", theta_w_euc_cost_, 2.0);
  private_nh.param(
    "theta_w_heuristic_cost", theta_w_heuristic_cost_, 1.0);
  private_nh.param(
    "theta_reference_spacing", theta_reference_spacing_, 0.05);
  private_nh.param(
    "theta_reference_smoothing_enabled",
    theta_reference_smoothing_enabled_, true);
  private_nh.param(
    "theta_suffix_candidate_max_planning_time",
    theta_suffix_candidate_max_planning_time_, 60.0);
  private_nh.param(
    "theta_prefix_candidate_max_planning_time",
    theta_prefix_candidate_max_planning_time_, 20.0);
  private_nh.param(
    "theta_prefix_join_max_heading_error",
    theta_prefix_join_max_heading_error_, 0.20);
  private_nh.param(
    "theta_prefix_join_max_curvature_jump",
    theta_prefix_join_max_curvature_jump_, 2.5);
  private_nh.param(
    "theta_unsafe_segment_lookback_points",
    theta_unsafe_segment_lookback_points_, 30);
  private_nh.param(
    "coarse_route_max_planning_time", coarse_route_max_planning_time_, 5.0);
  private_nh.param(
    "corridor_level_max_planning_time", corridor_level_max_planning_time_, 10.0);
  private_nh.param(
    "corridor_full_search_min_time", corridor_full_search_min_time_, 12.0);
  private_nh.param(
    "final_validation_reserve_time", final_validation_reserve_time_, 5.0);
  private_nh.param(
    "corridor_route_progress_weight", corridor_route_progress_weight_, 1.0);

  private_nh.param("reverse_penalty", search_info_.reverse_penalty, 2.0f);
  private_nh.param("change_penalty", search_info_.change_penalty, 0.45f);
  private_nh.param("non_straight_penalty", search_info_.non_straight_penalty, 1.05f);
  private_nh.param("cost_penalty", search_info_.cost_penalty, 2.0f);
  private_nh.param("retrospective_penalty", search_info_.retrospective_penalty, 0.015f);
  private_nh.param("rotation_penalty", search_info_.rotation_penalty, 10.0f);
  private_nh.param("analytic_expansion_ratio", search_info_.analytic_expansion_ratio, 3.5f);
  private_nh.param(
    "analytic_expansion_max_cost", search_info_.analytic_expansion_max_cost, 200.0f);
  private_nh.param(
    "analytic_expansion_max_cost_override",
    search_info_.analytic_expansion_max_cost_override, false);
  private_nh.param(
    "prefer_forward_analytic_expansion",
    search_info_.prefer_forward_analytic_expansion, false);
  private_nh.param(
    "cache_obstacle_heuristic", search_info_.cache_obstacle_heuristic, false);
  private_nh.param("allow_reverse_expansion", search_info_.allow_reverse_expansion, false);
  private_nh.param(
    "require_forward_steering_primitives",
    search_info_.require_forward_steering_primitives, true);
  private_nh.param(
    "downsample_obstacle_heuristic", search_info_.downsample_obstacle_heuristic, true);
  private_nh.param(
    "use_quadratic_cost_penalty", search_info_.use_quadratic_cost_penalty, false);

  double analytic_expansion_max_length_m = 3.0;
  private_nh.param(
    "analytic_expansion_max_length", analytic_expansion_max_length_m, 3.0);
  std::string goal_heading_mode = "DEFAULT";
  private_nh.param("goal_heading_mode", goal_heading_mode, goal_heading_mode);
  goal_heading_mode_ = nav2_smac_planner::fromStringToGH(goal_heading_mode);

  try {
    lattice_filepath_ = resolvePackageUri(lattice_filepath_);
    metadata_ = nav2_smac_planner::LatticeMotionTable::getLatticeMetadata(lattice_filepath_);
    if (metadata_.motion_model != "diff") {
      throw std::runtime_error("the configured lattice is not a differential-drive lattice");
    }
    if (std::abs(metadata_.grid_resolution - costmap_->getResolution()) > 1e-6) {
      throw std::runtime_error(
        "lattice grid_resolution does not match the global costmap resolution");
    }
    if (goal_heading_mode_ == nav2_smac_planner::GoalHeadingMode::UNKNOWN) {
      throw std::runtime_error("invalid goal_heading_mode: " + goal_heading_mode);
    }
    if (coarse_search_resolution_ <= 0 ||
      metadata_.number_of_headings % coarse_search_resolution_ != 0)
    {
      throw std::runtime_error(
        "coarse_search_resolution must evenly divide the lattice heading count");
    }
    start_heading_seed_span_ = std::max(
      0, std::min(
        start_heading_seed_span_,
        static_cast<int>(metadata_.number_of_headings / 4u)));

    max_iterations_ = max_iterations_ <= 0 ?
      std::numeric_limits<int>::max() : max_iterations_;
    max_on_approach_iterations_ = max_on_approach_iterations_ <= 0 ?
      std::numeric_limits<int>::max() : max_on_approach_iterations_;
    terminal_checking_interval_ = std::max(1, terminal_checking_interval_);
    collision_checker_angle_bins_ = std::max(8, collision_checker_angle_bins_);
    collision_check_resolution_ = std::max(0.002, collision_check_resolution_);
    max_planning_time_ = std::max(0.01, max_planning_time_);
    tolerance_ = std::max(0.0, tolerance_);
    costmap_current_timeout_ = std::max(0.0, costmap_current_timeout_);
    live_validation_retries_ = std::max(0, live_validation_retries_);
    live_validation_retry_min_time_ = std::max(0.01, live_validation_retry_min_time_);
    coarse_route_max_planning_time_ = std::max(0.05, coarse_route_max_planning_time_);
    corridor_level_max_planning_time_ = std::max(
      0.05, corridor_level_max_planning_time_);
    corridor_full_search_min_time_ = std::max(0.0, corridor_full_search_min_time_);
    final_validation_reserve_time_ = std::max(0.0, final_validation_reserve_time_);
    corridor_route_progress_weight_ = std::max(0.0, corridor_route_progress_weight_);
    theta_max_allowed_cost_ = std::clamp(
      theta_max_allowed_cost_, 0,
      static_cast<int>(kStateCenterMaxAllowedCost));
    if (!std::isfinite(theta_w_traversal_cost_) ||
      !std::isfinite(theta_w_euc_cost_) ||
      !std::isfinite(theta_w_heuristic_cost_) ||
      theta_w_traversal_cost_ < 0.0 || theta_w_euc_cost_ < 0.0 ||
      theta_w_heuristic_cost_ < 0.0)
    {
      throw std::runtime_error("Theta composite weights must be finite and non-negative");
    }
    theta_reference_spacing_ = std::max(
      0.005, std::fabs(theta_reference_spacing_));
    theta_suffix_candidate_max_planning_time_ = std::max(
      0.05, theta_suffix_candidate_max_planning_time_);
    theta_prefix_candidate_max_planning_time_ = std::max(
      0.05, theta_prefix_candidate_max_planning_time_);
    theta_prefix_join_max_heading_error_ = std::clamp(
      std::fabs(theta_prefix_join_max_heading_error_), 0.01, M_PI_2);
    theta_prefix_join_max_curvature_jump_ = std::max(
      0.0, std::fabs(theta_prefix_join_max_curvature_jump_));
    theta_unsafe_segment_lookback_points_ = std::max(
      1, theta_unsafe_segment_lookback_points_);
    if (theta_prefix_lattice_suffix_enabled_ &&
      search_info_.cache_obstacle_heuristic)
    {
      ROS_WARN(
        "SmacLatticePlanner '%s' disables cache_obstacle_heuristic in Theta/State "
        "composite mode so a live-snapshot retry cannot reuse an old costmap heuristic",
        name_.c_str());
      search_info_.cache_obstacle_heuristic = false;
    }

    search_info_.lattice_filepath = lattice_filepath_;
    search_info_.minimum_turning_radius =
      metadata_.min_turning_radius / costmap_->getResolution();
    search_info_.analytic_expansion_max_length =
      analytic_expansion_max_length_m / costmap_->getResolution();

    // Parse and validate the complete motion table during plugin startup.
    // Waiting until the first A* collision-checker binding would leave a
    // malformed production lattice hidden until the first live goal.
    if (search_info_.require_forward_steering_primitives) {
      nav2_smac_planner::LatticeMotionTable validation_table;
      unsigned int validation_size_x = costmap_->getSizeInCellsX();
      validation_table.initMotionModel(validation_size_x, search_info_);
    }

    lookup_table_dim_ = static_cast<float>(
      lookup_table_size_ / costmap_->getResolution());
    lookup_table_dim_ = static_cast<float>(static_cast<int>(lookup_table_dim_));
    if (static_cast<int>(lookup_table_dim_) % 2 == 0) {
      lookup_table_dim_ += 1.0f;
    }

    a_star_ = std::make_unique<
      nav2_smac_planner::AStarAlgorithm<nav2_smac_planner::NodeLattice>>(
      nav2_smac_planner::MotionModel::STATE_LATTICE, search_info_);
    a_star_->initialize(
      allow_unknown_, max_iterations_, max_on_approach_iterations_,
      terminal_checking_interval_, max_planning_time_, lookup_table_dim_,
      metadata_.number_of_headings);
    state_lattice_smoother_ = std::make_unique<StateLatticeSmoother>(
      state_lattice_smoother_params_, metadata_.min_turning_radius);
  } catch (const std::exception & error) {
    ROS_ERROR("Failed to initialize SmacLatticePlanner '%s': %s", name_.c_str(), error.what());
    planning_costmap_.reset();
    collision_checker_.reset();
    a_star_.reset();
    state_lattice_smoother_.reset();
    return;
  }

  plan_publisher_ = private_nh.advertise<nav_msgs::Path>("plan", 1, true);
  initialized_ = true;
  ROS_INFO(
    "Initialized Smac State Lattice '%s': %u headings, %.3f m grid, "
    "continuous footprint step %.3f m, Theta suffix=%s, Theta full-footprint=%s, "
    "State smoothing=%s, Theta corridor=%s",
    name_.c_str(), metadata_.number_of_headings, metadata_.grid_resolution,
    collision_check_resolution_,
    theta_prefix_lattice_suffix_enabled_ ? "enabled" : "disabled",
    theta_full_footprint_validation_enabled_ ? "enabled" : "disabled",
    state_lattice_smoothing_enabled_ ? "enabled" : "disabled",
    theta_corridor_search_enabled_ ? "enabled" : "disabled");
}

bool SmacLatticePlanner::waitForCurrentCostmap(
  std::string & reason,
  const std::chrono::steady_clock::time_point * overall_deadline) const
{
  if (!costmap_ros_) {
    reason = "global costmap is unavailable";
    return false;
  }
  const auto deadline = std::chrono::steady_clock::now() +
    std::chrono::duration<double>(costmap_current_timeout_);
  while (ros::ok() && !cancel_requested_.load() && !costmap_ros_->isCurrent()) {
    const auto now = std::chrono::steady_clock::now();
    if (overall_deadline != nullptr && now >= *overall_deadline) {
      reason = "global costmap wait exhausted the overall planning deadline";
      return false;
    }
    if (costmap_current_timeout_ <= 0.0 || now >= deadline) {
      reason = "global costmap did not become current within " +
        std::to_string(costmap_current_timeout_) + " s";
      return false;
    }
    ros::WallDuration(0.02).sleep();
  }
  if (cancel_requested_.load()) {
    reason = "planning canceled while waiting for a current global costmap";
    return false;
  }
  if (!ros::ok()) {
    reason = "ROS shutdown while waiting for the global costmap";
    return false;
  }
  return true;
}

std::unique_ptr<costmap_2d::Costmap2D> SmacLatticePlanner::captureCostmapSnapshot(
  std::vector<geometry_msgs::Point> & footprint) const
{
  if (!costmap_ || !costmap_ros_) {
    return nullptr;
  }
  std::unique_lock<costmap_2d::Costmap2D::mutex_t> lock(*costmap_->getMutex());
  footprint = costmap_ros_->getRobotFootprint();
  return std::make_unique<costmap_2d::Costmap2D>(*costmap_);
}

bool SmacLatticePlanner::worldToMapContinuous(
  const costmap_2d::Costmap2D & costmap,
  double wx, double wy, float & mx, float & my)
{
  const double x = (wx - costmap.getOriginX()) / costmap.getResolution();
  const double y = (wy - costmap.getOriginY()) / costmap.getResolution();
  if (!std::isfinite(x) || !std::isfinite(y) || x < 0.0 || y < 0.0 ||
    x >= costmap.getSizeInCellsX() || y >= costmap.getSizeInCellsY())
  {
    return false;
  }
  mx = static_cast<float>(x);
  my = static_cast<float>(y);
  return true;
}

bool SmacLatticePlanner::validateContinuousPath(
  const costmap_2d::Costmap2D & costmap,
  nav2_smac_planner::GridCollisionChecker & collision_checker,
  const std::vector<geometry_msgs::PoseStamped> & plan,
  std::string & reason,
  const std::chrono::steady_clock::time_point * deadline,
  std::size_t * first_unsafe_segment) const
{
  if (first_unsafe_segment != nullptr) {
    *first_unsafe_segment = std::numeric_limits<std::size_t>::max();
  }
  if (cancel_requested_.load()) {
    reason = "path validation canceled";
    return false;
  }
  if (plan.empty()) {
    reason = "planner returned an empty path";
    return false;
  }
  float previous_x = 0.0f;
  float previous_y = 0.0f;
  if (!worldToMapContinuous(
      costmap,
      plan.front().pose.position.x, plan.front().pose.position.y,
      previous_x, previous_y))
  {
    reason = "path start is outside the costmap";
    return false;
  }
  double previous_yaw = tf2::getYaw(plan.front().pose.orientation);
  if (collision_checker.inCollisionAtYaw(
      previous_x, previous_y, previous_yaw, allow_unknown_))
  {
    reason = "path start footprint is in collision";
    return false;
  }

  for (std::size_t i = 1; i < plan.size(); ++i) {
    if (cancel_requested_.load()) {
      reason = "path validation canceled at segment " + std::to_string(i - 1u);
      return false;
    }
    if (deadline != nullptr && std::chrono::steady_clock::now() >= *deadline) {
      reason = "path validation exceeded its planning deadline at segment " +
        std::to_string(i - 1u);
      return false;
    }
    float x = 0.0f;
    float y = 0.0f;
    if (!worldToMapContinuous(
        costmap,
        plan[i].pose.position.x, plan[i].pose.position.y, x, y))
    {
      reason = "path leaves the costmap at pose " + std::to_string(i);
      return false;
    }
    const double yaw = tf2::getYaw(plan[i].pose.orientation);
    if (collision_checker.inCollisionContinuous(
        previous_x, previous_y, previous_yaw,
        x, y, yaw, allow_unknown_))
    {
      if (first_unsafe_segment != nullptr) {
        *first_unsafe_segment = i - 1u;
      }
      reason = "continuous footprint collision at segment " + std::to_string(i - 1);
      return false;
    }
    previous_x = x;
    previous_y = y;
    previous_yaw = yaw;
  }
  return true;
}

uint32_t SmacLatticePlanner::makePlan(
  const geometry_msgs::PoseStamped & start,
  const geometry_msgs::PoseStamped & goal,
  double tolerance,
  std::vector<geometry_msgs::PoseStamped> & plan,
  double & cost,
  std::string & message)
{
  std::lock_guard<std::mutex> planning_lock(planning_mutex_);
  plan.clear();
  cost = 0.0;
  message.clear();
  cancel_requested_.store(false);

  if (!initialized_ || !a_star_ || !costmap_ || !costmap_ros_) {
    message = "Smac State Lattice planner is not initialized";
    return mbf_msgs::GetPathResult::NOT_INITIALIZED;
  }
  if (start.header.frame_id != global_frame_ || goal.header.frame_id != global_frame_) {
    message = "start and goal must be in global frame '" + global_frame_ + "'";
    return mbf_msgs::GetPathResult::TF_ERROR;
  }

  using PlanningClock = std::chrono::steady_clock;
  const auto planning_started = PlanningClock::now();
  const auto clockDuration = [](double seconds) {
      return std::chrono::duration_cast<PlanningClock::duration>(
        std::chrono::duration<double>(std::max(0.0, seconds)));
    };
  const auto overall_deadline = planning_started + clockDuration(max_planning_time_);
  try {
    for (int attempt = 0; attempt <= live_validation_retries_; ++attempt) {
      plan.clear();
      // AStar keeps a raw pointer to the checker. Clear its transient callbacks
      // and destroy objects strictly checker-before-map before replacing the
      // immutable snapshot for a new attempt.
      a_star_->clearCenterDomain();
      a_star_->clearAdditionalHeuristic();
      collision_checker_.reset();
      planning_costmap_.reset();
      std::string freshness_reason;
      if (!waitForCurrentCostmap(freshness_reason, &overall_deadline)) {
        message = freshness_reason;
        if (cancel_requested_.load()) {
          return mbf_msgs::GetPathResult::CANCELED;
        }
        return PlanningClock::now() >= overall_deadline ?
               mbf_msgs::GetPathResult::NO_PATH_FOUND :
               mbf_msgs::GetPathResult::NOT_INITIALIZED;
      }

      std::vector<geometry_msgs::Point> planning_footprint;
      planning_costmap_ = captureCostmapSnapshot(planning_footprint);
      if (!planning_costmap_) {
        message = "failed to capture an immutable global costmap snapshot";
        return mbf_msgs::GetPathResult::INTERNAL_ERROR;
      }
      collision_checker_ = std::make_unique<nav2_smac_planner::GridCollisionChecker>(
        planning_costmap_.get(), static_cast<unsigned int>(collision_checker_angle_bins_));
      collision_checker_->setCollisionCheckResolution(collision_check_resolution_);
      collision_checker_->setFootprint(planning_footprint, false, 0.0);

      const auto search_deadline = overall_deadline - clockDuration(final_validation_reserve_time_);
      const auto corridor_deadline = search_deadline - clockDuration(corridor_full_search_min_time_);
      const double elapsed = std::chrono::duration<double>(
        PlanningClock::now() - planning_started).count();
      const double remaining_time = max_planning_time_ - elapsed;
      if (remaining_time <
        final_validation_reserve_time_ + live_validation_retry_min_time_)
      {
        message = "State Lattice exhausted the overall planning deadline before a fresh retry";
        return mbf_msgs::GetPathResult::NO_PATH_FOUND;
      }

      float start_x = 0.0f;
      float start_y = 0.0f;
      float goal_x = 0.0f;
      float goal_y = 0.0f;
      if (!worldToMapContinuous(
          *planning_costmap_, start.pose.position.x, start.pose.position.y,
          start_x, start_y))
      {
        message = "start is outside the global costmap";
        return mbf_msgs::GetPathResult::OUT_OF_MAP;
      }
      if (!worldToMapContinuous(
          *planning_costmap_, goal.pose.position.x, goal.pose.position.y,
          goal_x, goal_y))
      {
        message = "goal is outside the global costmap";
        return mbf_msgs::GetPathResult::OUT_OF_MAP;
      }

      // Initialize once before quantizing the exact start yaw. Every corridor
      // level resets the graph below, but the safe heading bins and their
      // physical same-position sweeps are invariant on this snapshot.
      a_star_->initialize(
        allow_unknown_, max_iterations_, max_on_approach_iterations_,
        terminal_checking_interval_, remaining_time, lookup_table_dim_,
        metadata_.number_of_headings);
      a_star_->setCollisionChecker(collision_checker_.get());

      const double requested_goal_yaw = tf2::getYaw(goal.pose.orientation);
      const unsigned int goal_bin =
        a_star_->getContext()->motion_table.getClosestAngularBin(requested_goal_yaw);
      const unsigned int heading_count = metadata_.number_of_headings;
      using HeadingSeeds = std::vector<std::pair<unsigned int, float>>;
      const auto makeSafeHeadingSeeds = [&] (
          const geometry_msgs::PoseStamped & exact_start,
          float & exact_x, float & exact_y,
          HeadingSeeds & seeds, std::string & reason)
        {
          if (!worldToMapContinuous(
              *planning_costmap_, exact_start.pose.position.x,
              exact_start.pose.position.y, exact_x, exact_y))
          {
            reason = "segment start is outside the global costmap";
            return false;
          }
          const double exact_yaw = tf2::getYaw(exact_start.pose.orientation);
          const unsigned int nearest_bin =
            a_star_->getContext()->motion_table.getClosestAngularBin(exact_yaw);
          seeds.clear();
          seeds.reserve(static_cast<std::size_t>(1 + 2 * start_heading_seed_span_));
          for (int radius = 0; radius <= start_heading_seed_span_; ++radius) {
            const int directions = radius == 0 ? 1 : 2;
            for (int direction = 0; direction < directions; ++direction) {
              const int signed_offset = radius == 0 ? 0 :
                (direction == 0 ? -radius : radius);
              const int wrapped =
                (static_cast<int>(nearest_bin) + signed_offset +
                static_cast<int>(heading_count)) % static_cast<int>(heading_count);
              const auto candidate_bin = static_cast<unsigned int>(wrapped);
              const double candidate_yaw =
                a_star_->getContext()->motion_table.getAngleFromBin(candidate_bin);
              if (collision_checker_->inCollisionContinuous(
                  exact_x, exact_y, exact_yaw,
                  exact_x, exact_y, candidate_yaw, allow_unknown_))
              {
                continue;
              }
              const double yaw_error = std::abs(
                angles::shortest_angular_distance(exact_yaw, candidate_yaw));
              const double nominal_bin_width = 2.0 * M_PI / heading_count;
              const float correction_cost = static_cast<float>(
                search_info_.rotation_penalty * yaw_error / nominal_bin_width);
              seeds.emplace_back(candidate_bin, correction_cost);
            }
          }
          if (seeds.empty()) {
            reason = "no collision-free heading quantization from the exact segment start";
            return false;
          }
          return true;
        };

      HeadingSeeds safe_start_bins;
      std::string heading_seed_reason;
      if (!makeSafeHeadingSeeds(
          start, start_x, start_y, safe_start_bins, heading_seed_reason))
      {
        message = heading_seed_reason;
        return mbf_msgs::GetPathResult::INVALID_START;
      }
      const double exact_start_yaw = tf2::getYaw(start.pose.orientation);
      nav2_smac_planner::NodeLattice::CoordinateVector path;
      const float effective_tolerance = static_cast<float>(
        (tolerance > 0.0 ? tolerance : tolerance_) /
        planning_costmap_->getResolution());
      int total_iterations = 0;
      int selected_iterations = 0;
      std::string selected_search_mode;
      std::string selected_search_termination;
      bool selected_plan_contains_unvalidated_theta = false;
      std::vector<theta_state_suffix::PosePath> selected_state_proof_paths;

      const auto validatePublishCandidate = [&] (
          const costmap_2d::Costmap2D & proof_costmap,
          nav2_smac_planner::GridCollisionChecker & proof_checker,
          const theta_state_suffix::PosePath & candidate,
          std::string & reason,
          const PlanningClock::time_point & proof_deadline)
        {
          if (!selected_plan_contains_unvalidated_theta) {
            return validateContinuousPath(
              proof_costmap, proof_checker, candidate, reason, &proof_deadline);
          }
          for (std::size_t index = 0u; index < selected_state_proof_paths.size(); ++index) {
            if (!validateContinuousPath(
                proof_costmap, proof_checker, selected_state_proof_paths[index],
                reason, &proof_deadline))
            {
              reason = "State-owned path " + std::to_string(index) + ": " + reason;
              return false;
            }
          }
          reason.clear();
          return true;
        };

      CoarseRouteCorridorResult coarse_route;
      if ((theta_prefix_lattice_suffix_enabled_ || theta_corridor_search_enabled_) &&
        PlanningClock::now() < corridor_deadline)
      {
        const auto coarse_deadline = std::min(
          corridor_deadline,
          PlanningClock::now() + clockDuration(coarse_route_max_planning_time_));
        CoarseRouteCorridorOptions options;
        options.terminal_checking_interval = 100;
        options.build_center_corridors = theta_corridor_search_enabled_;
        if (theta_prefix_lattice_suffix_enabled_) {
          options.theta_allow_unknown = false;
          options.theta_max_allowed_cost = theta_max_allowed_cost_;
          options.theta_w_traversal_cost = theta_w_traversal_cost_;
          options.theta_w_euc_cost = theta_w_euc_cost_;
          options.theta_w_heuristic_cost = theta_w_heuristic_cost_;
          options.reference_spacing_m = theta_reference_spacing_;
          options.smooth_reference = theta_reference_smoothing_enabled_;
        }
        options.cancel_checker = [this, coarse_deadline]() {
            return cancel_requested_.load() || PlanningClock::now() >= coarse_deadline;
          };
        const auto coarse_started = PlanningClock::now();
        coarse_route = CoarseRouteCorridorHelper::build(
          *planning_costmap_,
          start.pose.position.x, start.pose.position.y,
          goal.pose.position.x, goal.pose.position.y,
          options);
        const double coarse_seconds = std::chrono::duration<double>(
          PlanningClock::now() - coarse_started).count();
        if (cancel_requested_.load()) {
          message = "Smac State Lattice planning canceled during coarse route search";
          return mbf_msgs::GetPathResult::CANCELED;
        }
        if (coarse_route.succeeded()) {
          const auto & route = coarse_route.routes.front();
          ROS_INFO(
            "State Lattice Theta guide: %.3f m, %zu sparse / %zu reference points, "
            "%d opened nodes, %.3f s",
            route.length_m, route.points.size(), route.reference_points.size(),
            route.theta_nodes_opened, coarse_seconds);
        } else {
          // A coarse guide is an optimization, never the source of navigation
          // truth. If it times out or finds no route, FULL State Lattice still
          // receives the reserved search budget on the unmodified snapshot.
          ROS_WARN(
            "State Lattice coarse Theta guide unavailable after %.3f s: %s; using FULL fallback",
            coarse_seconds, coarse_route.message.c_str());
        }
      }

      struct StateSearchGoal
      {
        float x{0.0f};
        float y{0.0f};
        unsigned int heading_bin{0u};
        nav2_smac_planner::GoalHeadingMode heading_mode{
          nav2_smac_planner::GoalHeadingMode::DEFAULT};
        bool continuous_forward_only{false};
      };
      const StateSearchGoal final_search_goal{
        goal_x, goal_y, goal_bin, goal_heading_mode_, true};

      auto runStateSearch = [&] (
          float segment_start_x,
          float segment_start_y,
          double segment_start_yaw,
          const HeadingSeeds & segment_start_bins,
          const StateSearchGoal & search_goal,
          const CenterCorridorMask * center_domain,
          const RouteProgressField * route_progress,
          const std::string & mode,
          double search_time_limit,
          float search_tolerance,
          nav2_smac_planner::NodeLattice::CoordinateVector & output_path,
          int & output_iterations)
        {
          int level_max_iterations = max_iterations_;
          a_star_->initialize(
            allow_unknown_, level_max_iterations, max_on_approach_iterations_,
            terminal_checking_interval_, std::max(0.05, search_time_limit),
            lookup_table_dim_, metadata_.number_of_headings);
          a_star_->setCollisionChecker(collision_checker_.get());
          output_path.clear();
          output_iterations = 0;
          const auto search_started = PlanningClock::now();
          nav2_smac_planner::SearchResult result;
          try {
            HeadingSeeds admissible_start_bins;
            const HeadingSeeds * active_start_bins = &segment_start_bins;
            if (search_goal.continuous_forward_only) {
              a_star_->setTransitionValidator(
                [](const nav2_smac_planner::NodeLattice::Coordinates & from,
                  const nav2_smac_planner::NodeLattice::Coordinates & to) {
                  return std::hypot(to.x - from.x, to.y - from.y) > 1e-4f;
                });
              // A graph start seed is not a physical motion, but selecting a
              // distant neighboring heading bin would become an implicit
              // same-position turn when the exact start pose is restored.
              // Restrict seeds at search time so the planner explores the
              // valid forward branch instead of succeeding with a path that
              // must be rejected after conversion.
              for (const auto & seed : segment_start_bins) {
                const double seed_yaw =
                  a_star_->getContext()->motion_table.getAngleFromBin(seed.first);
                const double seed_error = std::abs(
                  angles::shortest_angular_distance(segment_start_yaw, seed_yaw));
                if (seed_error <= theta_prefix_join_max_heading_error_ + 1e-9) {
                  admissible_start_bins.push_back(seed);
                }
              }
              active_start_bins = &admissible_start_bins;
            } else {
              a_star_->clearTransitionValidator();
            }
            // Reaching the nearest goal heading remains quantized, but the
            // final discrete primitive must translate into the goal. This
            // makes the desired smooth moving arrival part of the search
            // contract instead of repeatedly rejecting the same terminal
            // in-place rotation after A* has already stopped.
            a_star_->setGoalTransitionValidator(
              [](const nav2_smac_planner::NodeLattice::Coordinates & from,
                const nav2_smac_planner::NodeLattice::Coordinates & to) {
                return std::hypot(to.x - from.x, to.y - from.y) > 1e-4f;
              });
            if (center_domain != nullptr) {
              a_star_->setCenterDomain(
                [center_domain](float mx, float my) {
                  if (!std::isfinite(mx) || !std::isfinite(my) || mx < 0.0f || my < 0.0f) {
                    return false;
                  }
                  return center_domain->isAllowed(
                    static_cast<unsigned int>(std::floor(mx)),
                    static_cast<unsigned int>(std::floor(my)));
                });
            } else {
              a_star_->clearCenterDomain();
            }
            if (route_progress != nullptr && corridor_route_progress_weight_ > 0.0) {
              const float progress_scale = static_cast<float>(
                corridor_route_progress_weight_ *
                (1.0 - search_info_.retrospective_penalty) /
                planning_costmap_->getResolution());
              a_star_->setAdditionalHeuristic(
                [route_progress, progress_scale](float mx, float my) {
                  if (!std::isfinite(mx) || !std::isfinite(my) || mx < 0.0f || my < 0.0f) {
                    return 0.0f;
                  }
                  float remaining_m = 0.0f;
                  if (!route_progress->tryRemainingArcLengthM(
                      static_cast<unsigned int>(std::floor(mx)),
                      static_cast<unsigned int>(std::floor(my)), remaining_m))
                  {
                    return 0.0f;
                  }
                  return std::max(0.0f, remaining_m * progress_scale);
                });
            } else {
              a_star_->clearAdditionalHeuristic();
            }

            if (!active_start_bins->empty()) {
              a_star_->setStart(
                segment_start_x, segment_start_y,
                active_start_bins->front().first, active_start_bins->front().second);
              for (std::size_t i = 1; i < active_start_bins->size(); ++i) {
                a_star_->addStart(
                  segment_start_x, segment_start_y,
                  (*active_start_bins)[i].first, (*active_start_bins)[i].second);
              }
              a_star_->setGoal(
                search_goal.x, search_goal.y, search_goal.heading_bin,
                search_goal.heading_mode, coarse_search_resolution_);
              result = a_star_->createPathDetailed(
                output_path, output_iterations, search_tolerance,
                [this]() {return cancel_requested_.load();});
            }
          } catch (...) {
            a_star_->clearCenterDomain();
            a_star_->clearAdditionalHeuristic();
            a_star_->clearTransitionValidator();
            a_star_->clearGoalTransitionValidator();
            throw;
          }
          a_star_->clearCenterDomain();
          a_star_->clearAdditionalHeuristic();
          a_star_->clearTransitionValidator();
          a_star_->clearGoalTransitionValidator();
          const double search_seconds = std::chrono::duration<double>(
            PlanningClock::now() - search_started).count();
          ROS_INFO(
            "State Lattice search mode=%s result=%s path=%s expansions=%d time=%.3f s",
            mode.c_str(), searchTerminationName(result.termination),
            result.hasPath() ? "yes" : "no", output_iterations, search_seconds);
          return result;
        };

      auto coordinatePathIsSafe = [&] (
          float segment_start_x,
          float segment_start_y,
          double segment_start_yaw,
          const nav2_smac_planner::NodeLattice::CoordinateVector & candidate,
          const PlanningClock::time_point & proof_deadline,
          std::string & reason)
        {
          if (candidate.empty()) {
            reason = "candidate backtrace is empty";
            return false;
          }
          float previous_x = segment_start_x;
          float previous_y = segment_start_y;
          double previous_yaw = segment_start_yaw;
          std::size_t segment = 0u;
          for (auto iterator = candidate.rbegin(); iterator != candidate.rend(); ++iterator) {
            if (cancel_requested_.load()) {
              reason = "candidate validation canceled";
              return false;
            }
            if (PlanningClock::now() >= proof_deadline) {
              reason = "candidate validation exceeded its planning deadline";
              return false;
            }
            if (collision_checker_->inCollisionContinuous(
                previous_x, previous_y, previous_yaw,
                iterator->x, iterator->y, iterator->theta, allow_unknown_))
            {
              reason = "continuous footprint collision at candidate segment " +
                std::to_string(segment);
              return false;
            }
            previous_x = iterator->x;
            previous_y = iterator->y;
            previous_yaw = iterator->theta;
            ++segment;
          }
          return true;
        };

      const auto coordinatesToPosePlan = [&] (
          const nav2_smac_planner::NodeLattice::CoordinateVector & coordinates,
          const geometry_msgs::PoseStamped & exact_segment_start,
          std::vector<geometry_msgs::PoseStamped> & output)
        {
          output.clear();
          const ros::Time stamp = ros::Time::now();
          output.reserve(coordinates.size() + 2u);
          for (auto iterator = coordinates.rbegin(); iterator != coordinates.rend(); ++iterator) {
            geometry_msgs::PoseStamped pose;
            pose.header.frame_id = global_frame_;
            pose.header.stamp = stamp;
            pose.pose.position.x = planning_costmap_->getOriginX() +
              iterator->x * planning_costmap_->getResolution();
            pose.pose.position.y = planning_costmap_->getOriginY() +
              iterator->y * planning_costmap_->getResolution();
            pose.pose.orientation = quaternionFromYaw(iterator->theta);
            if (!output.empty() && theta_state_suffix::sameSE2(
                output.back(), pose, 1e-6, 1e-6))
            {
              continue;
            }
            output.push_back(std::move(pose));
          }
          if (output.empty()) {
            return false;
          }

          geometry_msgs::PoseStamped exact_start = exact_segment_start;
          exact_start.header.frame_id = global_frame_;
          exact_start.header.stamp = stamp;
          if (!theta_state_suffix::sameSE2(exact_start, output.front(), 1e-6, 1e-6)) {
            output.insert(output.begin(), exact_start);
          }

          const double bin_width = 2.0 * M_PI / metadata_.number_of_headings;
          const double largest_seed_quantization =
            (0.5 + static_cast<double>(start_heading_seed_span_)) * bin_width + 1e-3;
          // NodeLattice backtracing repeats the first primitive boundary after
          // the selected start-heading seed.  Normalize those graph-only
          // representations before any motion or footprint contract sees the
          // path.  A genuine searched rotation remains as explicit changed-yaw
          // samples and is never removed by this operation.
          theta_state_suffix::canonicalizeInitialLatticeMotion(
            output, largest_seed_quantization,
            theta_prefix_join_max_heading_error_, 1e-4,
            0.1 * bin_width + 1e-3);

          return true;
        };

      const auto canonicalizeStatePrefixJoin = [&] (
          theta_state_suffix::PosePath & state_prefix,
          const geometry_msgs::PoseStamped & exact_join,
          std::string & reason)
        {
          if (state_prefix.size() < 3u) {
            reason = "State prefix is too short to prove a moving arrival";
            return false;
          }

          const double position_error = std::hypot(
            state_prefix.back().pose.position.x - exact_join.pose.position.x,
            state_prefix.back().pose.position.y - exact_join.pose.position.y);
          const double yaw_error = std::abs(angles::shortest_angular_distance(
              tf2::getYaw(state_prefix.back().pose.orientation),
              tf2::getYaw(exact_join.pose.orientation)));
          const double max_position_error =
            std::sqrt(2.0) * planning_costmap_->getResolution() + 1e-3;
          const double max_yaw_error =
            M_PI / static_cast<double>(metadata_.number_of_headings) + 0.002;
          if (position_error > max_position_error || yaw_error > max_yaw_error) {
            reason = "State prefix lattice terminal residual exceeds join contract "
              "(position=" + std::to_string(position_error) +
              ", yaw=" + std::to_string(yaw_error) + ")";
            return false;
          }

          // The graph goal is a quantized cell/bin. Canonicalize that one
          // endpoint to the exact Theta join, then re-run kinematic and full
          // footprint proofs on the modified final primitive.
          geometry_msgs::PoseStamped canonical_join = exact_join;
          canonical_join.header.stamp = state_prefix.back().header.stamp;
          state_prefix.back() = std::move(canonical_join);
          if (!theta_state_suffix::containsContinuousForwardOnly(
              state_prefix, reason, 1e-4, 1e-6,
              theta_prefix_join_max_heading_error_))
          {
            return false;
          }
          return true;
        };

      const auto plannedInPlaceRotationsAreSafe = [&] (
          const theta_state_suffix::PosePath & candidate,
          const PlanningClock::time_point & proof_deadline,
          std::string & reason)
        {
          // A State primitive changes one neighboring lattice heading at a
          // time (at most about 0.245 rad for this lattice).  Larger same-XY
          // jumps do not encode an execution direction clearly enough and
          // must be subdivided before publication.
          constexpr double kMaxEncodedRotationStep = M_PI / 8.0 + 1e-6;
          for (std::size_t index = 0u; index + 1u < candidate.size(); ++index) {
            const auto & first = candidate[index];
            const auto & second = candidate[index + 1u];
            const double translation = std::hypot(
              second.pose.position.x - first.pose.position.x,
              second.pose.position.y - first.pose.position.y);
            const double yaw_change = std::abs(angles::shortest_angular_distance(
                tf2::getYaw(first.pose.orientation),
                tf2::getYaw(second.pose.orientation)));
            if (translation > 1e-4 || yaw_change <= 1e-6) {
              continue;
            }
            if (yaw_change > kMaxEncodedRotationStep) {
              reason = "in-place rotation at path edge " + std::to_string(index) +
                " is not direction-explicit (yaw step=" + std::to_string(yaw_change) +
                " rad); subdivide the planned rotation";
              return false;
            }

            const theta_state_suffix::PosePath planned_rotation{first, second};
            std::string sweep_reason;
            if (!validateContinuousPath(
                *planning_costmap_, *collision_checker_, planned_rotation,
                sweep_reason, &proof_deadline))
            {
              reason = "planned signed in-place rotation at path edge " +
                std::to_string(index) + " is unsafe: " + sweep_reason;
              return false;
            }
          }
          reason.clear();
          return true;
        };

      // Smoothing is deliberately atomic and subordinate to the raw State
      // solution. A rejected/expired/colliding candidate leaves state_path
      // byte-for-byte untouched and never triggers a wider search fallback.
      const auto maybeSmoothStatePath = [&] (
          theta_state_suffix::PosePath & state_path,
          const PlanningClock::time_point & smoothing_deadline,
          const std::string & mode)
        {
          if (!state_lattice_smoothing_enabled_ || !state_lattice_smoother_) {
            return false;
          }
          const auto result = state_lattice_smoother_->smooth(
            state_path, *planning_costmap_, *collision_checker_, allow_unknown_,
            smoothing_deadline, [this]() {return cancel_requested_.load();});
          if (!result.accepted) {
            ROS_INFO(
              "State Lattice smoother kept raw %s path: %s (iterations=%d)",
              mode.c_str(), result.reason.c_str(), result.iterations);
            return false;
          }

          std::string reason;
          if (!theta_state_suffix::containsKinematicallyContinuousForwardOrRotation(
              result.path, reason))
          {
            ROS_WARN(
              "State Lattice smoother kept raw %s path because the candidate "
              "failed the Forward/Rotation audit: %s",
              mode.c_str(), reason.c_str());
            return false;
          }
          if (!theta_state_suffix::terminalAvoidsStationaryYawRepair(
              result.path, reason))
          {
            ROS_WARN(
              "State Lattice smoother kept raw %s path because the candidate "
              "introduced a stationary terminal yaw repair: %s",
              mode.c_str(), reason.c_str());
            return false;
          }
          if (!validateContinuousPath(
              *planning_costmap_, *collision_checker_, result.path, reason,
              &smoothing_deadline))
          {
            ROS_WARN(
              "State Lattice smoother kept raw %s path because the candidate "
              "failed continuous footprint proof: %s",
              mode.c_str(), reason.c_str());
            return false;
          }

          ROS_INFO(
            "State Lattice smoother accepted %s: curvature TV %.6f -> %.6f rad/m, "
            "max jump %.6f -> %.6f rad/m, p95 %.6f -> %.6f rad/m, "
            "curvature direction changes %d -> %d, center cost max %.1f -> %.1f "
            "mean %.3f -> %.3f, iterations=%d",
            mode.c_str(),
            result.raw_quality.curvature_total_variation_radpm,
            result.candidate_quality.curvature_total_variation_radpm,
            result.raw_quality.max_curvature_jump_radpm,
            result.candidate_quality.max_curvature_jump_radpm,
            result.raw_quality.p95_abs_curvature_radpm,
            result.candidate_quality.p95_abs_curvature_radpm,
            result.raw_quality.curvature_direction_changes,
            result.candidate_quality.curvature_direction_changes,
            result.raw_quality.max_center_cost,
            result.candidate_quality.max_center_cost,
            result.raw_quality.mean_center_cost,
            result.candidate_quality.mean_center_cost,
            result.iterations);
          state_path = result.path;
          return true;
        };

      bool path_found = false;
      if (theta_prefix_lattice_suffix_enabled_ && coarse_route.succeeded() &&
        coarse_route.routes.front().reference_points.size() >= 2u)
      {
        theta_state_suffix::PosePath theta_reference;
        const auto & reference_points = coarse_route.routes.front().reference_points;
        theta_reference.resize(reference_points.size());
        const ros::Time reference_stamp = ros::Time::now();
        for (std::size_t index = 0u; index < reference_points.size(); ++index) {
          auto & pose = theta_reference[index];
          pose.header.frame_id = global_frame_;
          pose.header.stamp = reference_stamp;
          pose.pose.position = reference_points[index];
          double yaw = tf2::getYaw(goal.pose.orientation);
          if (index + 1u < reference_points.size()) {
            yaw = std::atan2(
              reference_points[index + 1u].y - reference_points[index].y,
              reference_points[index + 1u].x - reference_points[index].x);
          }
          pose.pose.orientation = quaternionFromYaw(yaw);
        }
        // Preserve the measured start SE(2). A State prefix will normally own
        // the transition from this pose into the Theta tangent.
        theta_reference.front().pose.orientation = start.pose.orientation;
        theta_reference.back().pose.orientation = goal.pose.orientation;

        const bool short_theta_reference =
          reference_points.size() <
          theta_state_suffix::kMinimumThetaPosesForComposite;
        theta_state_suffix::PosePath selected_state_prefix;
        std::size_t selected_prefix_join_index = 0u;
        bool state_prefix_available = false;

        if (!short_theta_reference) {
          const auto prefix_candidates =
            theta_state_suffix::makeThetaPrefixJoinCandidates(theta_reference);
          for (const auto & prefix_selection : prefix_candidates) {
            if (prefix_selection.reaches_theta_goal ||
              prefix_selection.theta_from_join.size() < 3u)
            {
              break;
            }
            const double available = std::chrono::duration<double>(
              corridor_deadline - PlanningClock::now()).count();
            if (available < 0.05) {
              break;
            }
            const auto prefix_deadline = std::min(
              corridor_deadline,
              PlanningClock::now() + clockDuration(std::min(
                theta_prefix_candidate_max_planning_time_, available)));

            float join_x = 0.0f;
            float join_y = 0.0f;
            if (!worldToMapContinuous(
                *planning_costmap_, prefix_selection.join.pose.position.x,
                prefix_selection.join.pose.position.y, join_x, join_y))
            {
              continue;
            }
            const double join_yaw = tf2::getYaw(prefix_selection.join.pose.orientation);
            const unsigned int join_bin =
              a_star_->getContext()->motion_table.getClosestAngularBin(join_yaw);
            const StateSearchGoal prefix_goal{
              join_x, join_y, join_bin,
              nav2_smac_planner::GoalHeadingMode::DEFAULT, true};
            const double prefix_time = std::chrono::duration<double>(
              prefix_deadline - PlanningClock::now()).count();
            if (prefix_time < 0.05) {
              continue;
            }

            int prefix_iterations = 0;
            const std::string prefix_mode = "state_prefix_theta_join_" +
              std::to_string(prefix_selection.join_index) + "pt";
            const auto prefix_result = runStateSearch(
              start_x, start_y, exact_start_yaw, safe_start_bins,
              prefix_goal, nullptr, nullptr, prefix_mode, prefix_time,
              0.0f, path, prefix_iterations);
            total_iterations += prefix_iterations;
            if (prefix_result.termination == nav2_smac_planner::SearchTermination::CANCELED) {
              message = "Theta/State prefix planning canceled";
              return mbf_msgs::GetPathResult::CANCELED;
            }

            std::string prefix_reason;
            const bool exact_prefix =
              prefix_result.termination == nav2_smac_planner::SearchTermination::SUCCESS &&
              prefix_result.hasPath() && coordinatePathIsSafe(
                start_x, start_y, exact_start_yaw, path,
                prefix_deadline, prefix_reason);
            if (!exact_prefix) {
              ROS_WARN(
                "State prefix to Theta join %zu was not an exact continuously-safe "
                "result (%s%s%s); expanding the join distance",
                prefix_selection.join_index,
                searchTerminationName(prefix_result.termination),
                prefix_reason.empty() ? "" : ": ", prefix_reason.c_str());
              continue;
            }

            theta_state_suffix::PosePath state_prefix;
            if (!coordinatesToPosePlan(path, start, state_prefix) ||
              !canonicalizeStatePrefixJoin(
                state_prefix, prefix_selection.join, prefix_reason))
            {
              ROS_WARN(
                "State prefix to Theta join %zu failed the exact forward splice "
                "contract: %s; expanding the join distance",
                prefix_selection.join_index, prefix_reason.c_str());
              continue;
            }
            if (!theta_state_suffix::joinIsPositionHeadingCurvatureContinuous(
                state_prefix, prefix_selection.theta_from_join, prefix_reason,
                1e-6, 1e-6, theta_prefix_join_max_heading_error_,
                theta_prefix_join_max_curvature_jump_))
            {
              ROS_WARN(
                "State prefix to Theta join %zu failed tangent/curvature continuity: "
                "%s; expanding the join distance",
                prefix_selection.join_index, prefix_reason.c_str());
              continue;
            }
            if (!validateContinuousPath(
                *planning_costmap_, *collision_checker_, state_prefix,
                prefix_reason, &prefix_deadline))
            {
              ROS_WARN(
                "State prefix to Theta join %zu failed the canonical full-footprint "
                "proof: %s; expanding the join distance",
                prefix_selection.join_index, prefix_reason.c_str());
              continue;
            }

            const auto raw_state_prefix = state_prefix;
            if (maybeSmoothStatePath(state_prefix, prefix_deadline, prefix_mode)) {
              if (!theta_state_suffix::containsContinuousForwardOnly(
                  state_prefix, prefix_reason, 1e-4, 1e-6,
                  theta_prefix_join_max_heading_error_) ||
                !theta_state_suffix::joinIsPositionHeadingCurvatureContinuous(
                  state_prefix, prefix_selection.theta_from_join, prefix_reason,
                  1e-6, 1e-6, theta_prefix_join_max_heading_error_,
                  theta_prefix_join_max_curvature_jump_))
              {
                ROS_WARN(
                  "State Lattice smoother kept raw %s path because the smoothed "
                  "prefix failed its forward join contract: %s",
                  prefix_mode.c_str(), prefix_reason.c_str());
                state_prefix = raw_state_prefix;
              }
            }

            selected_state_prefix = std::move(state_prefix);
            selected_prefix_join_index = prefix_selection.join_index;
            state_prefix_available = true;
            ROS_INFO(
              "Theta/State selected continuous-forward State prefix at Theta index %zu "
              "(%zu poses, no in-place rotation)",
              selected_prefix_join_index, selected_state_prefix.size());
            break;
          }

          if (!state_prefix_available) {
            ROS_WARN(
              "All continuous-forward State prefix joins failed; in-place alignment "
              "fallback is disabled, handing the remaining budget to forward-only "
              "FULL State search");
          }
        }

        std::vector<theta_state_suffix::ThetaPrefixCut> suffix_candidates;
        if (short_theta_reference) {
          // If the 40-pose prefix region and the 100-pose suffix region leave
          // no translated Theta edge between them, State owns the route once.
          // This avoids computing a prefix which overlap handling must discard.
          suffix_candidates.push_back(theta_state_suffix::selectThetaPrefixCut(
              theta_reference, theta_reference.size()));
        } else if (state_prefix_available) {
          suffix_candidates =
            theta_state_suffix::makeThetaPrefixCutCandidates(theta_reference);
        }
        bool adaptive_unsafe_segment_fallback_requested = false;
        bool adaptive_unsafe_segment_fallback_scheduled = false;
        bool adaptive_unsafe_segment_fallback_attempted = false;
        std::size_t adaptive_cut_index = std::numeric_limits<std::size_t>::max();
        for (std::size_t candidate_index = 0u;
          candidate_index < suffix_candidates.size(); ++candidate_index)
        {
          const auto selection = suffix_candidates[candidate_index];
          if (state_prefix_available &&
            selection.cut_index < selected_prefix_join_index)
          {
            ROS_INFO(
              "Theta/State suffix cut index %zu would overlap the selected State "
              "prefix ending at index %zu; handing remaining budget to FULL State",
              selection.cut_index, selected_prefix_join_index);
            break;
          }
          theta_state_suffix::PosePath retained_prefix;
          if (state_prefix_available) {
            theta_state_suffix::PosePath theta_middle(
              theta_reference.begin() + selected_prefix_join_index,
              theta_reference.begin() + selection.cut_index + 1u);
            try {
              retained_prefix = theta_state_suffix::stitchThetaPrefixAndStateSuffix(
                selected_state_prefix, theta_middle, 1e-6, 1e-6);
            } catch (const std::exception & error) {
              ROS_WARN(
                "Theta/State rejected prefix join %zu -> suffix cut %zu: %s",
                selected_prefix_join_index, selection.cut_index, error.what());
              continue;
            }
          } else {
            retained_prefix = selection.prefix_including_cut;
          }
          const bool selection_is_adaptive =
            adaptive_unsafe_segment_fallback_requested &&
            selection.cut_index == adaptive_cut_index;
          const bool selection_is_short_route_all_state =
            short_theta_reference && selection.is_full_path;
          if (selection_is_adaptive) {
            adaptive_unsafe_segment_fallback_attempted = true;
          }
          // A long route's explicit full-path candidate is handled by the
          // reserved FULL fallback below. For a route no longer than the
          // configured 400-point windows, the clamped first candidate is the
          // natural all-State solution and is still attempted here.
          if (selection.is_full_path && theta_reference.size() >
            theta_state_suffix::kSuffixPointCountCandidates.back())
          {
            break;
          }
          const auto candidate_deadline =
            (selection_is_adaptive || selection_is_short_route_all_state) ?
            search_deadline : corridor_deadline;
          const double available = std::chrono::duration<double>(
            candidate_deadline - PlanningClock::now()).count();
          if (available < 0.05) {
            break;
          }

          std::string prefix_motion_reason;
          if (!theta_state_suffix::containsKinematicallyContinuousForwardOrRotation(
              retained_prefix, prefix_motion_reason))
          {
            ROS_WARN(
              "Theta/State suffix rejected %zu-point cut because its retained "
              "Theta prefix is not forward-kinematically continuous: %s",
              selection.effective_suffix_point_count, prefix_motion_reason.c_str());
            continue;
          }

          std::string prefix_reason;
          std::size_t first_unsafe_segment = std::numeric_limits<std::size_t>::max();
          if (theta_full_footprint_validation_enabled_ && !validateContinuousPath(
              *planning_costmap_, *collision_checker_,
              retained_prefix, prefix_reason, &candidate_deadline,
              &first_unsafe_segment))
          {
            ROS_WARN(
              "Theta/State suffix rejected %zu-point cut because its retained "
              "Theta prefix is not continuously footprint-safe: %s",
              selection.effective_suffix_point_count, prefix_reason.c_str());
            if (first_unsafe_segment != std::numeric_limits<std::size_t>::max()) {
              adaptive_unsafe_segment_fallback_requested = true;
              std::size_t theta_unsafe_segment = first_unsafe_segment;
              if (state_prefix_available) {
                if (first_unsafe_segment + 1u < selected_state_prefix.size()) {
                  ROS_ERROR(
                    "A previously proven State prefix became unsafe during retained-prefix "
                    "validation at segment %zu",
                    first_unsafe_segment);
                  continue;
                }
                theta_unsafe_segment = selected_prefix_join_index +
                  (first_unsafe_segment - (selected_state_prefix.size() - 1u));
              }
              const auto adaptive =
                theta_state_suffix::selectThetaPrefixCutBeforeUnsafeSegment(
                theta_reference, theta_unsafe_segment,
                static_cast<std::size_t>(theta_unsafe_segment_lookback_points_));
              // A confirmed Theta footprint collision owns the fallback
              // policy even when the fixed candidate's deadline expires on
              // this proof. Never let the deadline race fall through to the
              // unrelated whole-route State path below.
              suffix_candidates.resize(candidate_index + 1u);
              if (adaptive.cut_index < selection.cut_index) {
                adaptive_unsafe_segment_fallback_scheduled = true;
                adaptive_cut_index = adaptive.cut_index;
                // The first failed footprint proof determines the useful
                // fallback. Wider fixed tail windows that still start after
                // the failed edge cannot help, and whole-route State is not a
                // useful response to one localized Theta defect.
                suffix_candidates.push_back(adaptive);
                ROS_WARN(
                  "Theta/State retained prefix collision at segment %zu; replacing "
                  "the remaining fixed/FULL fallbacks with adaptive State hand-off "
                  "%d points before the failure (cut index %zu, suffix %zu poses)",
                  theta_unsafe_segment, theta_unsafe_segment_lookback_points_,
                  adaptive.cut_index, adaptive.effective_suffix_point_count);
              } else {
                ROS_WARN(
                  "Theta/State retained prefix collision at segment %zu lies within "
                  "the configured %d-point lookback of the route start; refusing "
                  "whole-route State fallback",
                  theta_unsafe_segment, theta_unsafe_segment_lookback_points_);
              }
            }
            if (PlanningClock::now() >= candidate_deadline &&
              !adaptive_unsafe_segment_fallback_scheduled)
            {
              break;
            }
            continue;
          }

          float segment_start_x = 0.0f;
          float segment_start_y = 0.0f;
          HeadingSeeds segment_start_bins;
          std::string segment_start_reason;
          if (!makeSafeHeadingSeeds(
              selection.cut, segment_start_x, segment_start_y,
              segment_start_bins, segment_start_reason))
          {
            ROS_WARN(
              "Theta/State suffix rejected %zu-point cut: %s",
              selection.effective_suffix_point_count,
              segment_start_reason.c_str());
            continue;
          }

          // Prefix proof and exact-cut seed construction are part of this
          // candidate's real budget. Re-read the clock after both so a long
          // proof cannot silently consume the time reserved for FULL fallback.
          const double available_after_proof = std::chrono::duration<double>(
            candidate_deadline - PlanningClock::now()).count();
          if (available_after_proof < 0.05) {
            break;
          }
          const double candidate_time =
            (selection_is_adaptive || selection_is_short_route_all_state) ?
            available_after_proof : std::min(
            theta_suffix_candidate_max_planning_time_, available_after_proof);
          int suffix_iterations = 0;
          const std::string mode = selection_is_short_route_all_state ?
            "theta_short_route_state_full_" +
            std::to_string(selection.effective_suffix_point_count) + "pt" :
            (selection_is_adaptive ?
            "theta_prefix_state_suffix_unsafe_segment_adaptive_" +
            std::to_string(selection.effective_suffix_point_count) + "pt" :
            "theta_prefix_state_suffix_" +
            std::to_string(selection.effective_suffix_point_count) + "pt");
          const double segment_start_yaw = tf2::getYaw(selection.cut.pose.orientation);
          const auto result = runStateSearch(
            segment_start_x, segment_start_y, segment_start_yaw,
            segment_start_bins, final_search_goal,
            nullptr, nullptr, mode, candidate_time,
            0.0f, path, suffix_iterations);
          total_iterations += suffix_iterations;
          if (result.termination == nav2_smac_planner::SearchTermination::CANCELED) {
            message = "Theta/State composite planning canceled";
            return mbf_msgs::GetPathResult::CANCELED;
          }

          std::string suffix_reason;
          const bool exact_search_success =
            result.termination == nav2_smac_planner::SearchTermination::SUCCESS &&
            result.hasPath();
          if (!exact_search_success || !coordinatePathIsSafe(
              segment_start_x, segment_start_y, segment_start_yaw,
              path, candidate_deadline, suffix_reason))
          {
            const std::string suffix_detail =
              suffix_reason.empty() ? std::string() : ": " + suffix_reason;
            ROS_WARN(
              "Theta/State suffix %s was not an exact continuously-safe result "
              "(%s%s); expanding the suffix window",
              mode.c_str(), searchTerminationName(result.termination),
              suffix_detail.c_str());
            continue;
          }

          theta_state_suffix::PosePath state_suffix;
          if (!coordinatesToPosePlan(path, selection.cut, state_suffix)) {
            ROS_WARN("Theta/State suffix %s returned no poses", mode.c_str());
            continue;
          }
          std::string state_direction_reason;
          if (!theta_state_suffix::containsContinuousForwardOnly(
              state_suffix, state_direction_reason, 1e-4, 1e-6,
              theta_prefix_join_max_heading_error_))
          {
            ROS_WARN(
              "Theta/State suffix %s failed the continuous-forward contract: %s; "
              "expanding the suffix window",
              mode.c_str(), state_direction_reason.c_str());
            continue;
          }
          std::string state_terminal_reason;
          if (!theta_state_suffix::terminalAvoidsStationaryYawRepair(
              state_suffix, state_terminal_reason))
          {
            ROS_WARN(
              "Theta/State suffix %s ended with a stationary goal-yaw repair: %s; "
              "expanding the suffix window",
              mode.c_str(), state_terminal_reason.c_str());
            continue;
          }

          // A cut at index zero is a whole-route State solution and has no
          // Theta->State join to prove.  Every genuine composite suffix must
          // meet the retained centreline with bounded tangent and curvature.
          const bool has_theta_suffix_join = selection.cut_index > 0u;
          if (has_theta_suffix_join) {
            std::string state_join_reason;
            if (!theta_state_suffix::joinIsPositionHeadingCurvatureContinuous(
                retained_prefix, state_suffix, state_join_reason,
                1e-6, 1e-6, theta_prefix_join_max_heading_error_,
                theta_prefix_join_max_curvature_jump_))
            {
              ROS_WARN(
                "Theta/State suffix %s failed the tangent/curvature join contract: %s; "
                "expanding the suffix window",
                mode.c_str(), state_join_reason.c_str());
              continue;
            }
          }

          theta_state_suffix::PosePath composite;
          try {
            composite = theta_state_suffix::stitchThetaPrefixAndStateSuffix(
              retained_prefix, state_suffix, 1e-6, 1e-6);
          } catch (const std::exception & error) {
            ROS_WARN(
              "Theta/State suffix %s failed the structural splice contract: %s",
              mode.c_str(), error.what());
            continue;
          }
          std::string composite_reason;
          bool composite_safe = true;
          if (theta_full_footprint_validation_enabled_) {
            composite_safe = validateContinuousPath(
              *planning_costmap_, *collision_checker_, composite, composite_reason,
              &candidate_deadline);
          } else {
            if (state_prefix_available) {
              composite_safe = validateContinuousPath(
                *planning_costmap_, *collision_checker_, selected_state_prefix,
                composite_reason, &candidate_deadline);
            }
            if (composite_safe) {
              composite_safe = validateContinuousPath(
                *planning_costmap_, *collision_checker_, state_suffix,
                composite_reason, &candidate_deadline);
            }
          }
          if (!composite_safe)
          {
            ROS_WARN(
              "Theta/State suffix %s failed the enabled snapshot footprint proof: %s; "
              "expanding the suffix window",
              mode.c_str(), composite_reason.c_str());
            if (PlanningClock::now() >= candidate_deadline) {
              break;
            }
            continue;
          }

          // The raw composite has already passed every hard contract. Try a
          // smoothed State-only replacement without changing the fallback
          // decision when smoothing is rejected for any reason.
          const auto raw_state_suffix = state_suffix;
          const auto raw_composite = composite;
          if (maybeSmoothStatePath(state_suffix, candidate_deadline, mode)) {
            try {
              if (has_theta_suffix_join) {
                std::string smoothed_join_reason;
                if (!theta_state_suffix::joinIsPositionHeadingCurvatureContinuous(
                    retained_prefix, state_suffix, smoothed_join_reason,
                    1e-6, 1e-6, theta_prefix_join_max_heading_error_,
                    theta_prefix_join_max_curvature_jump_))
                {
                  throw std::runtime_error(
                          "smoothed Theta/State join rejected: " + smoothed_join_reason);
                }
              }
              auto smoothed_composite = theta_state_suffix::stitchThetaPrefixAndStateSuffix(
                retained_prefix, state_suffix, 1e-6, 1e-6);
              std::string smoothed_composite_reason;
              const bool smoothed_safe = theta_full_footprint_validation_enabled_ ?
                validateContinuousPath(
                *planning_costmap_, *collision_checker_, smoothed_composite,
                smoothed_composite_reason, &candidate_deadline) :
                validateContinuousPath(
                *planning_costmap_, *collision_checker_, state_suffix,
                smoothed_composite_reason, &candidate_deadline);
              if (smoothed_safe)
              {
                composite = std::move(smoothed_composite);
              } else {
                ROS_WARN(
                  "State Lattice smoother kept raw %s path because the smoothed "
                  "Theta/State splice failed complete footprint proof: %s",
                  mode.c_str(), smoothed_composite_reason.c_str());
                state_suffix = raw_state_suffix;
                composite = raw_composite;
              }
            } catch (const std::exception & error) {
              ROS_WARN(
                "State Lattice smoother kept raw %s path because the smoothed "
                "splice was structurally invalid: %s",
                mode.c_str(), error.what());
              state_suffix = raw_state_suffix;
              composite = raw_composite;
            }
          }

          std::string rotation_fallback_reason;
          if (!plannedInPlaceRotationsAreSafe(
              composite, candidate_deadline, rotation_fallback_reason))
          {
            ROS_WARN(
              "Theta/State suffix %s contains an invalid or unsafe planned "
              "in-place rotation: %s; expanding the State-owned region",
              mode.c_str(), rotation_fallback_reason.c_str());
            continue;
          }

          plan = std::move(composite);
          selected_plan_contains_unvalidated_theta =
            !theta_full_footprint_validation_enabled_;
          selected_state_proof_paths.clear();
          if (state_prefix_available) {
            selected_state_proof_paths.push_back(selected_state_prefix);
          }
          selected_state_proof_paths.push_back(state_suffix);
          path_found = true;
          selected_iterations = suffix_iterations;
          selected_search_mode = mode;
          selected_search_termination = searchTerminationName(result.termination);
          ROS_INFO(
            "Theta/State composite selected %zu reference poses (cut index %zu, "
            "Theta prefix %zu poses, State suffix %zu poses)",
            selection.effective_suffix_point_count, selection.cut_index,
            retained_prefix.size(), state_suffix.size());
          break;
        }

        if (!path_found && short_theta_reference) {
          message = "short Theta route State search did not produce an exact continuously-safe "
            "path; duplicate FULL fallback suppressed";
          return mbf_msgs::GetPathResult::NO_PATH_FOUND;
        }
        if (!path_found && adaptive_unsafe_segment_fallback_requested) {
          message = adaptive_unsafe_segment_fallback_attempted ?
            "adaptive State suffix from " +
            std::to_string(theta_unsafe_segment_lookback_points_) +
            " poses before the unsafe Theta segment did not produce an exact continuously-safe "
            "path; whole-route State fallback suppressed" :
            (adaptive_unsafe_segment_fallback_scheduled ?
            "unsafe Theta segment fallback could not be attempted before the planning deadline" :
            "unsafe Theta segment lies within the configured lookback of the route start; "
            "whole-route State fallback suppressed");
          return mbf_msgs::GetPathResult::NO_PATH_FOUND;
        }
      }

      if (!path_found && theta_corridor_search_enabled_ && coarse_route.succeeded()) {
        const auto & masks = coarse_route.routes.front().center_masks;
        for (const auto & mask : masks) {
          const double available = std::chrono::duration<double>(
            corridor_deadline - PlanningClock::now()).count();
          if (available < 0.05) {
            break;
          }
          const double level_time = std::min(corridor_level_max_planning_time_, available);
          const std::string mode = "theta_corridor_" + std::to_string(mask.halfWidthM()) + "m";
          int level_iterations = 0;
          const auto result = runStateSearch(
            start_x, start_y, exact_start_yaw, safe_start_bins,
            final_search_goal, &mask, &coarse_route.routes.front().route_progress,
            mode, level_time, effective_tolerance, path, level_iterations);
          total_iterations += level_iterations;
          if (result.termination == nav2_smac_planner::SearchTermination::CANCELED) {
            message = "Smac State Lattice planning canceled";
            return mbf_msgs::GetPathResult::CANCELED;
          }
          std::string candidate_reason;
          const bool candidate_safe = result.hasPath() &&
            coordinatePathIsSafe(
              start_x, start_y, exact_start_yaw, path,
              corridor_deadline, candidate_reason);
          if (result.hasPath() && !candidate_safe) {
            ROS_WARN(
              "State Lattice rejected %s candidate during full continuous snapshot proof: %s; "
              "trying a wider domain",
              mode.c_str(), candidate_reason.c_str());
          }
          if (candidate_safe) {
            path_found = true;
            selected_iterations = level_iterations;
            selected_search_mode = mode;
            selected_search_termination = searchTerminationName(result.termination);
            break;
          }
        }
      }

      if (!path_found) {
        const double full_available = std::chrono::duration<double>(
          search_deadline - PlanningClock::now()).count();
        if (full_available < 0.05) {
          message = "Theta/State composite attempts exhausted the search deadline before FULL fallback";
          return mbf_msgs::GetPathResult::NO_PATH_FOUND;
        }
        int full_iterations = 0;
        const auto result = runStateSearch(
          start_x, start_y, exact_start_yaw, safe_start_bins,
          final_search_goal, nullptr, nullptr, "FULL_EXACT", full_available, 0.0f,
          path, full_iterations);
        total_iterations += full_iterations;
        if (result.termination == nav2_smac_planner::SearchTermination::CANCELED) {
          message = "Smac State Lattice planning canceled";
          return mbf_msgs::GetPathResult::CANCELED;
        }
        std::string candidate_reason;
        const bool exact_full_success =
          result.termination == nav2_smac_planner::SearchTermination::SUCCESS &&
          result.hasPath();
        const bool candidate_safe = exact_full_success &&
          coordinatePathIsSafe(
            start_x, start_y, exact_start_yaw, path,
            overall_deadline, candidate_reason);
        if (!candidate_safe) {
          if (exact_full_success) {
            message = "State Lattice FULL candidate failed continuous snapshot proof: " +
              candidate_reason;
            return mbf_msgs::GetPathResult::NO_PATH_FOUND;
          }
          message = "State Lattice FULL search ended " +
            std::string(searchTerminationName(result.termination)) + " after " +
            std::to_string(total_iterations) + " total expansions";
          return mbf_msgs::GetPathResult::NO_PATH_FOUND;
        }
        path_found = true;
        selected_iterations = full_iterations;
        selected_search_mode = "FULL_EXACT";
        selected_search_termination = searchTerminationName(result.termination);
      }

      if (plan.empty()) {
        if (!coordinatesToPosePlan(path, start, plan)) {
          message = "State Lattice backtrace returned no poses";
          return mbf_msgs::GetPathResult::EMPTY_PATH;
        }
        std::string direction_reason;
        if (!theta_state_suffix::containsContinuousForwardOnly(
            plan, direction_reason, 1e-4, 1e-6,
            theta_prefix_join_max_heading_error_))
        {
          plan.clear();
          message = "State Lattice A* path violates the continuous-forward contract: " +
            direction_reason;
          return mbf_msgs::GetPathResult::NO_PATH_FOUND;
        }
        std::string terminal_reason;
        if (!theta_state_suffix::terminalAvoidsStationaryYawRepair(plan, terminal_reason)) {
          plan.clear();
          message = "State Lattice A* path ended with a stationary goal-yaw repair: " +
            terminal_reason;
          return mbf_msgs::GetPathResult::NO_PATH_FOUND;
        }
        (void)maybeSmoothStatePath(plan, search_deadline, selected_search_mode);
      }


      std::string planned_rotation_reason;
      if (!plannedInPlaceRotationsAreSafe(
          plan, overall_deadline, planned_rotation_reason))
      {
        plan.clear();
        message = "State Lattice path contains an invalid planned in-place rotation: " +
          planned_rotation_reason;
        return mbf_msgs::GetPathResult::NO_PATH_FOUND;
      }

      std::string validation_reason;
      if (!validatePublishCandidate(
          *planning_costmap_, *collision_checker_, plan, validation_reason,
          overall_deadline))
      {
        if (cancel_requested_.load()) {
          plan.clear();
          message = "Smac State Lattice planning canceled";
          return mbf_msgs::GetPathResult::CANCELED;
        }
        if (attempt < live_validation_retries_) {
          ROS_WARN(
            "State Lattice planning-snapshot footprint validation invalidated attempt %d: %s; "
            "retrying on a fresh snapshot",
            attempt + 1, validation_reason.c_str());
          collision_checker_.reset();
          planning_costmap_.reset();
          continue;
        }
        plan.clear();
        message = "snapshot continuous footprint validation failed: " + validation_reason;
        return mbf_msgs::GetPathResult::NO_PATH_FOUND;
      }

      // Search never holds the live map mutex. Immediately before publishing,
      // take a second short snapshot and repeat the exact same filled-polygon
      // continuous sweep. If the map changed against the path, search once more
      // on the new snapshot while sharing the original overall deadline.
      // Drop the search checker's integral image before constructing the live
      // one so peak memory contains only one full collision index.
      collision_checker_.reset();
      freshness_reason.clear();
      if (!waitForCurrentCostmap(freshness_reason, &overall_deadline)) {
        planning_costmap_.reset();
        plan.clear();
        message = freshness_reason;
        if (cancel_requested_.load()) {
          return mbf_msgs::GetPathResult::CANCELED;
        }
        return PlanningClock::now() >= overall_deadline ?
               mbf_msgs::GetPathResult::NO_PATH_FOUND :
               mbf_msgs::GetPathResult::NOT_INITIALIZED;
      }
      std::vector<geometry_msgs::Point> live_footprint;
      auto live_costmap = captureCostmapSnapshot(live_footprint);
      if (!live_costmap) {
        planning_costmap_.reset();
        plan.clear();
        message = "failed to capture the live validation costmap snapshot";
        return mbf_msgs::GetPathResult::INTERNAL_ERROR;
      }
      planning_costmap_ = std::move(live_costmap);
      collision_checker_ = std::make_unique<nav2_smac_planner::GridCollisionChecker>(
        planning_costmap_.get(), static_cast<unsigned int>(collision_checker_angle_bins_));
      collision_checker_->setCollisionCheckResolution(collision_check_resolution_);
      collision_checker_->setFootprint(live_footprint, false, 0.0);
      // Also replaces AStar's now-stale checker pointer and releases its search
      // graph before a possible retry or the next planning request.
      a_star_->setCollisionChecker(collision_checker_.get());
      if (!plannedInPlaceRotationsAreSafe(
          plan, overall_deadline, validation_reason) ||
        !validatePublishCandidate(
          *planning_costmap_, *collision_checker_, plan, validation_reason,
          overall_deadline))
      {
        if (cancel_requested_.load()) {
          plan.clear();
          message = "Smac State Lattice planning canceled during live footprint validation";
          return mbf_msgs::GetPathResult::CANCELED;
        }
        if (attempt < live_validation_retries_) {
          ROS_WARN(
            "State Lattice live footprint validation invalidated attempt %d: %s; "
            "retrying on a fresh snapshot",
            attempt + 1, validation_reason.c_str());
          continue;
        }
        plan.clear();
        message = "live continuous footprint validation failed: " + validation_reason;
        return mbf_msgs::GetPathResult::NO_PATH_FOUND;
      }

      const double end_distance = std::hypot(
        plan.back().pose.position.x - goal.pose.position.x,
        plan.back().pose.position.y - goal.pose.position.y);
      const double end_yaw_error = std::abs(angles::shortest_angular_distance(
        tf2::getYaw(plan.back().pose.orientation), tf2::getYaw(goal.pose.orientation)));
      // A discrete goal may finish anywhere in the requested 5 cm lattice
      // cell. Bound that quantization error geometrically; State MPPI then
      // tracks this path endpoint to 0.025 m, keeping the combined error below
      // MBF's 0.10 m requested-target acceptance threshold.
      const double max_lattice_position_residual =
        std::sqrt(2.0) * planning_costmap_->getResolution() + 1e-3;
      constexpr double kRequestedYawTolerance = 0.20;
      // Generated lattice headings and DB endpoint yaws are serialized at
      // different precisions. Keep a 0.002 rad engineering margin above the
      // ideal half-bin bound (pi / 32 ~= 0.098175). Together with the State
      // MPPI 0.08 rad path-goal tolerance, the worst-case requested-goal yaw
      // error remains below MBF's outer 0.20 rad acceptance threshold.
      constexpr double kHeadingBinFloatEpsilon = 0.002;
      const double max_nearest_bin_residual =
        M_PI / static_cast<double>(heading_count) + kHeadingBinFloatEpsilon;
      if (end_distance > max_lattice_position_residual ||
        end_yaw_error > kRequestedYawTolerance ||
        end_yaw_error > max_nearest_bin_residual)
      {
        plan.clear();
        message = "State Lattice terminal exceeded the requested position/yaw or nearest-bin "
          "residual contract";
        return mbf_msgs::GetPathResult::NO_PATH_FOUND;
      }
      cost = pathLength(plan);
      message = selected_plan_contains_unvalidated_theta ?
        "Smac Theta/State path published with Theta full-footprint validation disabled; "
        "State-owned portions are continuously filled-footprint-safe on the latest "
        "costmap via " + selected_search_mode + " (" :
        "Smac State Lattice path is continuously filled-footprint-safe on the latest "
        "costmap via " + selected_search_mode + " (";
      message +=
        selected_search_termination + ", " + std::to_string(selected_iterations) +
        " selected / " + std::to_string(total_iterations) +
        " total expansions); terminal residual " + std::to_string(end_distance) +
        " m / " + std::to_string(end_yaw_error) + " rad";
      publishPlan(plan);
      return mbf_msgs::GetPathResult::SUCCESS;
    }
    message = "State Lattice exhausted live validation retries";
    return mbf_msgs::GetPathResult::NO_PATH_FOUND;
  } catch (const nav2_core::PlannerCancelled & error) {
    if (a_star_) {
      a_star_->clearCenterDomain();
      a_star_->clearAdditionalHeuristic();
    }
    message = error.what();
    plan.clear();
    return mbf_msgs::GetPathResult::CANCELED;
  } catch (const nav2_core::GoalOccupied & error) {
    if (a_star_) {
      a_star_->clearCenterDomain();
      a_star_->clearAdditionalHeuristic();
    }
    message = error.what();
    plan.clear();
    return mbf_msgs::GetPathResult::INVALID_GOAL;
  } catch (const std::exception & error) {
    if (a_star_) {
      a_star_->clearCenterDomain();
      a_star_->clearAdditionalHeuristic();
    }
    message = std::string("Smac State Lattice error: ") + error.what();
    plan.clear();
    ROS_ERROR_THROTTLE(1.0, "%s", message.c_str());
    return mbf_msgs::GetPathResult::INTERNAL_ERROR;
  }
}

bool SmacLatticePlanner::cancel()
{
  cancel_requested_.store(true);
  return true;
}

void SmacLatticePlanner::publishPlan(
  const std::vector<geometry_msgs::PoseStamped> & plan) const
{
  nav_msgs::Path message;
  message.header.frame_id = global_frame_;
  message.header.stamp = ros::Time::now();
  message.poses = plan;
  plan_publisher_.publish(message);
}

}  // namespace smac_lattice_planner_mbf

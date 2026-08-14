// Copyright 2026 Clean Robot Navigation Team
// Licensed under the Apache License, Version 2.0.

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <angles/angles.h>
#include <costmap_2d/cost_values.h>
#include <nav_msgs/OccupancyGrid.h>
#include <nlohmann/json.hpp>
#include <ros/package.h>
#include <ros/ros.h>
#include <ros/topic.h>
#include <sqlite3.h>
#include <tf/transform_datatypes.h>

#include "nav2_smac_planner/a_star.hpp"
#include "nav2_smac_planner/collision_checker.hpp"
#include "nav2_smac_planner/node_lattice.hpp"
#include "smac_lattice_planner_mbf/coarse_route_corridor.hpp"
#include "smac_lattice_planner_mbf/live_validator_support.hpp"
#include "smac_lattice_planner_mbf/state_lattice_smoother.hpp"
#include "smac_lattice_planner_mbf/theta_state_suffix.hpp"

namespace
{
using SteadyClock = std::chrono::steady_clock;

double elapsedSeconds(const SteadyClock::time_point & started)
{
  return std::chrono::duration<double>(SteadyClock::now() - started).count();
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

struct BlockEndpoints
{
  int id{0};
  double entry_x{0.0};
  double entry_y{0.0};
  double entry_yaw{0.0};
  double exit_x{0.0};
  double exit_y{0.0};
  double exit_yaw{0.0};
};

struct PersistedPose
{
  double x{0.0};
  double y{0.0};
  double yaw{0.0};
};

std::vector<PersistedPose> decodePathBlob(const void * data, int bytes)
{
  if (!data || bytes <= 0 || (bytes % 12) != 0) {
    throw std::runtime_error("invalid or empty path_blob");
  }
  const auto * raw = static_cast<const unsigned char *>(data);
  std::vector<PersistedPose> path;
  path.reserve(static_cast<std::size_t>(bytes / 12));
  for (int offset = 0; offset < bytes; offset += 12) {
    float values[3] = {0.0f, 0.0f, 0.0f};
    std::memcpy(&values[0], raw + offset, sizeof(float));
    std::memcpy(&values[1], raw + offset + 4, sizeof(float));
    std::memcpy(&values[2], raw + offset + 8, sizeof(float));
    path.push_back(PersistedPose{
      static_cast<double>(values[0]),
      static_cast<double>(values[1]),
      static_cast<double>(values[2])});
  }
  return path;
}

struct PathMotionSummary
{
  double forward_distance{0.0};
  double reverse_distance{0.0};
  int direction_changes{0};
};

struct PathQualitySummary
{
  double length_m{0.0};
  double absolute_yaw_change_rad{0.0};
  double translating_absolute_yaw_change_rad{0.0};
  double mean_absolute_curvature_radpm{0.0};
  double length_weighted_mean_absolute_curvature_radpm{0.0};
  double p95_absolute_curvature_radpm{0.0};
  double max_absolute_curvature_radpm{0.0};
  double curvature_total_variation_radpm{0.0};
  double max_curvature_jump_radpm{0.0};
  int curvature_direction_changes{0};
  int in_place_rotations{0};
};

PathQualitySummary summarizePosePathQuality(
  const smac_lattice_planner_mbf::theta_state_suffix::PosePath & path,
  double curvature_direction_threshold_radpm = 0.5)
{
  PathQualitySummary summary;
  double absolute_curvature_sum = 0.0;
  std::vector<double> absolute_curvatures;
  std::size_t translating_edges = 0u;
  int previous_curvature_direction = 0;
  double previous_curvature = 0.0;
  bool has_previous_curvature = false;
  for (std::size_t index = 1u; index < path.size(); ++index) {
    const auto & start = path[index - 1u].pose;
    const auto & end = path[index].pose;
    const double distance = std::hypot(
      end.position.x - start.position.x,
      end.position.y - start.position.y);
    const double yaw_change = angles::shortest_angular_distance(
      tf::getYaw(start.orientation), tf::getYaw(end.orientation));
    summary.absolute_yaw_change_rad += std::abs(yaw_change);
    if (distance <= 1e-4) {
      if (std::abs(yaw_change) > 1e-3) {
        ++summary.in_place_rotations;
        has_previous_curvature = false;
      }
      continue;
    }

    summary.length_m += distance;
    summary.translating_absolute_yaw_change_rad += std::abs(yaw_change);
    const double curvature = yaw_change / distance;
    const double absolute_curvature = std::abs(curvature);
    absolute_curvature_sum += absolute_curvature;
    absolute_curvatures.push_back(absolute_curvature);
    summary.max_absolute_curvature_radpm = std::max(
      summary.max_absolute_curvature_radpm, absolute_curvature);
    if (has_previous_curvature) {
      const double curvature_jump = std::abs(curvature - previous_curvature);
      summary.curvature_total_variation_radpm += curvature_jump;
      summary.max_curvature_jump_radpm = std::max(
        summary.max_curvature_jump_radpm, curvature_jump);
    }
    previous_curvature = curvature;
    has_previous_curvature = true;
    ++translating_edges;
    if (absolute_curvature < curvature_direction_threshold_radpm) {
      continue;
    }
    const int curvature_direction = curvature < 0.0 ? -1 : 1;
    if (previous_curvature_direction != 0 &&
      curvature_direction != previous_curvature_direction)
    {
      ++summary.curvature_direction_changes;
    }
    previous_curvature_direction = curvature_direction;
  }
  if (translating_edges > 0u) {
    summary.mean_absolute_curvature_radpm =
      absolute_curvature_sum / static_cast<double>(translating_edges);
  }
  if (summary.length_m > 0.0) {
    summary.length_weighted_mean_absolute_curvature_radpm =
      summary.translating_absolute_yaw_change_rad / summary.length_m;
  }
  if (!absolute_curvatures.empty()) {
    std::sort(absolute_curvatures.begin(), absolute_curvatures.end());
    const std::size_t p95_index = static_cast<std::size_t>(std::ceil(
      0.95 * static_cast<double>(absolute_curvatures.size()))) - 1u;
    summary.p95_absolute_curvature_radpm = absolute_curvatures[
      std::min(p95_index, absolute_curvatures.size() - 1u)];
  }
  return summary;
}

PathMotionSummary summarizePathMotion(
  const nav2_smac_planner::NodeLattice::CoordinateVector & reverse_path,
  double resolution)
{
  PathMotionSummary summary;
  int previous_direction = 0;
  if (reverse_path.size() < 2u) {
    return summary;
  }
  for (std::size_t index = reverse_path.size() - 1u; index > 0u; --index) {
    const auto & start = reverse_path[index];
    const auto & end = reverse_path[index - 1u];
    const double dx = end.x - start.x;
    const double dy = end.y - start.y;
    const double distance_cells = std::hypot(dx, dy);
    if (distance_cells <= 1e-4) {
      continue;
    }
    const double start_projection =
      (dx * std::cos(start.theta) + dy * std::sin(start.theta)) /
      distance_cells;
    const double end_projection =
      (dx * std::cos(end.theta) + dy * std::sin(end.theta)) /
      distance_cells;
    const double projection =
      std::abs(start_projection) >= std::abs(end_projection) ?
      start_projection : end_projection;
    const int direction = projection < -0.25 ? -1 : 1;
    const double distance_m = distance_cells * resolution;
    if (direction < 0) {
      summary.reverse_distance += distance_m;
    } else {
      summary.forward_distance += distance_m;
    }
    if (previous_direction != 0 && direction != previous_direction) {
      ++summary.direction_changes;
    }
    previous_direction = direction;
  }
  return summary;
}

class Database
{
public:
  explicit Database(const std::string & path)
  {
    const std::string uri = "file:" + path + "?mode=ro";
    if (sqlite3_open_v2(
        uri.c_str(), &database_, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, nullptr) != SQLITE_OK)
    {
      const std::string error = database_ ? sqlite3_errmsg(database_) : "open failed";
      throw std::runtime_error("cannot open planning database: " + error);
    }
  }

  ~Database()
  {
    if (database_) {
      sqlite3_close(database_);
    }
  }

  std::string latestPlanId() const
  {
    return scalarText("SELECT plan_id FROM plans ORDER BY created_ts DESC LIMIT 1");
  }

  std::vector<int> executionOrder(const std::string & plan_id) const
  {
    const std::string json_text = scalarText(
      "SELECT exec_order_json FROM plans WHERE plan_id=?", plan_id);
    return nlohmann::json::parse(json_text).get<std::vector<int>>();
  }

  std::map<int, BlockEndpoints> blocks(const std::string & plan_id) const
  {
    sqlite3_stmt * statement = nullptr;
    const char * sql =
      "SELECT block_id,entry_x,entry_y,entry_yaw,exit_x,exit_y,exit_yaw,path_blob "
      "FROM plan_blocks WHERE plan_id=?";
    if (sqlite3_prepare_v2(database_, sql, -1, &statement, nullptr) != SQLITE_OK) {
      throw std::runtime_error(sqlite3_errmsg(database_));
    }
    sqlite3_bind_text(statement, 1, plan_id.c_str(), -1, SQLITE_TRANSIENT);
    std::map<int, BlockEndpoints> result;
    while (sqlite3_step(statement) == SQLITE_ROW) {
      BlockEndpoints block;
      block.id = sqlite3_column_int(statement, 0);
      block.entry_x = sqlite3_column_double(statement, 1);
      block.entry_y = sqlite3_column_double(statement, 2);
      block.entry_yaw = sqlite3_column_double(statement, 3);
      block.exit_x = sqlite3_column_double(statement, 4);
      block.exit_y = sqlite3_column_double(statement, 5);
      block.exit_yaw = sqlite3_column_double(statement, 6);
      const auto path = decodePathBlob(
        sqlite3_column_blob(statement, 7), sqlite3_column_bytes(statement, 7));
      // The executor sends the first/last persisted coverage poses, not the
      // denormalized entry/exit columns.  Validate exactly those production
      // goals so a stale metadata yaw cannot produce a false PASS here.
      block.entry_x = path.front().x;
      block.entry_y = path.front().y;
      block.entry_yaw = path.front().yaw;
      block.exit_x = path.back().x;
      block.exit_y = path.back().y;
      block.exit_yaw = path.back().yaw;
      result.emplace(block.id, block);
    }
    sqlite3_finalize(statement);
    return result;
  }

private:
  std::string scalarText(const std::string & sql, const std::string & value = "") const
  {
    sqlite3_stmt * statement = nullptr;
    if (sqlite3_prepare_v2(database_, sql.c_str(), -1, &statement, nullptr) != SQLITE_OK) {
      throw std::runtime_error(sqlite3_errmsg(database_));
    }
    if (!value.empty()) {
      sqlite3_bind_text(statement, 1, value.c_str(), -1, SQLITE_TRANSIENT);
    }
    if (sqlite3_step(statement) != SQLITE_ROW) {
      sqlite3_finalize(statement);
      throw std::runtime_error("database query returned no row: " + sql);
    }
    const unsigned char * text = sqlite3_column_text(statement, 0);
    const std::string result = text ? reinterpret_cast<const char *>(text) : "";
    sqlite3_finalize(statement);
    return result;
  }

  sqlite3 * database_{nullptr};
};

unsigned char decodePublishedCost(int8_t value)
{
  if (value < 0) {
    return costmap_2d::NO_INFORMATION;
  }
  if (value >= 100) {
    return costmap_2d::LETHAL_OBSTACLE;
  }
  if (value == 99) {
    return costmap_2d::INSCRIBED_INFLATED_OBSTACLE;
  }
  if (value == 0) {
    return costmap_2d::FREE_SPACE;
  }
  // Conservative inverse of Costmap2DPublisher's [1,252] -> [1,98]
  // translation: choose the upper raw cost represented by this occupancy bin.
  return static_cast<unsigned char>(std::min(
    252, 1 + static_cast<int>(std::ceil((value - 1) * 251.0 / 97.0))));
}

std::unique_ptr<costmap_2d::Costmap2D> convertCostmap(
  const nav_msgs::OccupancyGrid & message)
{
  auto costmap = std::make_unique<costmap_2d::Costmap2D>(
    message.info.width, message.info.height, message.info.resolution,
    message.info.origin.position.x, message.info.origin.position.y,
    costmap_2d::NO_INFORMATION);
  for (unsigned int y = 0; y < message.info.height; ++y) {
    for (unsigned int x = 0; x < message.info.width; ++x) {
      costmap->setCost(
        x, y, decodePublishedCost(message.data[y * message.info.width + x]));
    }
  }
  return costmap;
}

nav2_smac_planner::Footprint robotFootprint()
{
  nav2_smac_planner::Footprint footprint(4);
  footprint[0].x = -0.2185;
  footprint[0].y = -0.325;
  footprint[1].x = -0.2185;
  footprint[1].y = 0.325;
  footprint[2].x = 0.6315;
  footprint[2].y = 0.325;
  footprint[3].x = 0.6315;
  footprint[3].y = -0.325;
  return footprint;
}

bool worldToMapContinuous(
  const costmap_2d::Costmap2D & costmap,
  double wx, double wy, float & mx, float & my)
{
  const double x = (wx - costmap.getOriginX()) / costmap.getResolution();
  const double y = (wy - costmap.getOriginY()) / costmap.getResolution();
  if (x < 0.0 || y < 0.0 || x >= costmap.getSizeInCellsX() ||
    y >= costmap.getSizeInCellsY())
  {
    return false;
  }
  mx = static_cast<float>(x);
  my = static_cast<float>(y);
  return true;
}

bool validateContinuousPath(
  nav2_smac_planner::GridCollisionChecker & checker,
  const nav2_smac_planner::NodeLattice::CoordinateVector & reverse_path,
  float exact_start_x, float exact_start_y, double exact_start_yaw,
  bool allow_unknown,
  std::string & reason,
  const SteadyClock::time_point * deadline = nullptr)
{
  if (reverse_path.empty()) {
    reason = "planner returned an empty path";
    return false;
  }

  if (checker.inCollisionAtYaw(
      exact_start_x, exact_start_y, exact_start_yaw, allow_unknown))
  {
    reason = "exact start footprint is in collision";
    return false;
  }

  float previous_x = exact_start_x;
  float previous_y = exact_start_y;
  double previous_yaw = exact_start_yaw;
  std::size_t forward_segment = 0u;
  for (auto iterator = reverse_path.rbegin(); iterator != reverse_path.rend(); ++iterator) {
    if (deadline != nullptr && SteadyClock::now() >= *deadline) {
      reason = "continuous candidate proof exceeded its deadline";
      return false;
    }
    if (checker.inCollisionContinuous(
        previous_x, previous_y, previous_yaw,
        iterator->x, iterator->y, iterator->theta, allow_unknown))
    {
      reason = "continuous footprint collision at segment " +
        std::to_string(forward_segment);
      return false;
    }
    previous_x = iterator->x;
    previous_y = iterator->y;
    previous_yaw = iterator->theta;
    ++forward_segment;
  }
  return true;
}

bool validateContinuousPosePath(
  const costmap_2d::Costmap2D & costmap,
  nav2_smac_planner::GridCollisionChecker & checker,
  const smac_lattice_planner_mbf::theta_state_suffix::PosePath & path,
  bool allow_unknown,
  std::string & reason,
  const SteadyClock::time_point * deadline = nullptr,
  std::size_t * first_unsafe_segment = nullptr)
{
  if (first_unsafe_segment != nullptr) {
    *first_unsafe_segment = std::numeric_limits<std::size_t>::max();
  }
  if (path.empty()) {
    reason = "composite path is empty";
    return false;
  }

  float previous_x = 0.0f;
  float previous_y = 0.0f;
  if (!worldToMapContinuous(
      costmap, path.front().pose.position.x, path.front().pose.position.y,
      previous_x, previous_y))
  {
    reason = "composite start is outside the costmap";
    return false;
  }
  double previous_yaw = tf::getYaw(path.front().pose.orientation);
  if (checker.inCollisionAtYaw(
      previous_x, previous_y, previous_yaw, allow_unknown))
  {
    reason = "composite start footprint is in collision";
    return false;
  }

  for (std::size_t index = 1u; index < path.size(); ++index) {
    if (deadline != nullptr && SteadyClock::now() >= *deadline) {
      reason = "continuous pose-path proof exceeded its deadline";
      return false;
    }
    float current_x = 0.0f;
    float current_y = 0.0f;
    if (!worldToMapContinuous(
        costmap, path[index].pose.position.x, path[index].pose.position.y,
        current_x, current_y))
    {
      reason = "composite pose " + std::to_string(index) + " is outside the costmap";
      return false;
    }
    const double current_yaw = tf::getYaw(path[index].pose.orientation);
    if (checker.inCollisionContinuous(
        previous_x, previous_y, previous_yaw,
        current_x, current_y, current_yaw, allow_unknown))
    {
      if (first_unsafe_segment != nullptr) {
        *first_unsafe_segment = index - 1u;
      }
      reason = "continuous composite footprint collision at segment " +
        std::to_string(index - 1u);
      return false;
    }
    previous_x = current_x;
    previous_y = current_y;
    previous_yaw = current_yaw;
  }
  return true;
}

smac_lattice_planner_mbf::theta_state_suffix::PosePath makeThetaReference(
  const smac_lattice_planner_mbf::CoarseRouteCandidate & route,
  const BlockEndpoints & previous,
  const BlockEndpoints & next,
  const std::string & frame)
{
  using smac_lattice_planner_mbf::theta_state_suffix::PosePath;
  PosePath reference(route.reference_points.size());
  for (std::size_t index = 0u; index < route.reference_points.size(); ++index) {
    auto & pose = reference[index];
    pose.header.frame_id = frame;
    pose.pose.position = route.reference_points[index];
    double yaw = next.entry_yaw;
    if (index + 1u < route.reference_points.size()) {
      yaw = std::atan2(
        route.reference_points[index + 1u].y - route.reference_points[index].y,
        route.reference_points[index + 1u].x - route.reference_points[index].x);
    }
    pose.pose.orientation = tf::createQuaternionMsgFromYaw(yaw);
  }
  if (!reference.empty()) {
    reference.front().pose.orientation = tf::createQuaternionMsgFromYaw(previous.exit_yaw);
    reference.back().pose.orientation = tf::createQuaternionMsgFromYaw(next.entry_yaw);
  }
  return reference;
}

bool coordinatesToPosePath(
  const nav2_smac_planner::NodeLattice::CoordinateVector & reverse_path,
  const costmap_2d::Costmap2D & costmap,
  const geometry_msgs::PoseStamped & exact_start,
  const std::string & frame,
  smac_lattice_planner_mbf::theta_state_suffix::PosePath & output)
{
  using smac_lattice_planner_mbf::theta_state_suffix::sameSE2;
  output.clear();
  output.reserve(reverse_path.size() + 1u);
  for (auto iterator = reverse_path.rbegin(); iterator != reverse_path.rend(); ++iterator) {
    geometry_msgs::PoseStamped pose;
    pose.header.frame_id = frame;
    pose.pose.position.x = costmap.getOriginX() + iterator->x * costmap.getResolution();
    pose.pose.position.y = costmap.getOriginY() + iterator->y * costmap.getResolution();
    pose.pose.orientation = tf::createQuaternionMsgFromYaw(iterator->theta);
    if (output.empty() || !sameSE2(output.back(), pose, 1e-6, 1e-6)) {
      output.push_back(std::move(pose));
    }
  }
  if (output.empty()) {
    return false;
  }
  geometry_msgs::PoseStamped normalized_start = exact_start;
  normalized_start.header.frame_id = frame;
  if (!sameSE2(normalized_start, output.front(), 1e-6, 1e-6)) {
    output.insert(output.begin(), normalized_start);
  }
  return true;
}
}  // namespace

int main(int argc, char ** argv)
{
  const auto run_started = SteadyClock::now();
  ros::init(argc, argv, "smac_lattice_live_block_validator");
  ros::NodeHandle private_nh("~");
  std::cout << std::fixed << std::setprecision(6);
  std::string database_path = "/data/coverage/planning.db";
  std::string plan_id;
  std::string costmap_topic = "/move_base_flex/global_costmap/costmap";
  std::string lattice_filepath = ros::package::getPath("smac_lattice_planner_mbf") +
    "/config/diff_5cm_0p40m_32bins.json";
  double max_planning_time = 180.0;
  bool theta_corridor_search_enabled = false;
  double coarse_route_max_planning_time = 5.0;
  double corridor_level_max_planning_time = 10.0;
  double corridor_full_search_min_time = 12.0;
  double final_validation_reserve_time = 5.0;
  double corridor_route_progress_weight = 1.0;
  bool theta_prefix_lattice_suffix_enabled = true;
  bool state_lattice_smoothing_enabled = false;
  smac_lattice_planner_mbf::StateLatticeSmootherParams smoother_params;
  int theta_max_allowed_cost = 10;
  double theta_w_traversal_cost = 32.0;
  double theta_w_euc_cost = 1.5;
  double theta_w_heuristic_cost = 1.0;
  double theta_reference_spacing = 0.05;
  bool theta_reference_smoothing_enabled = true;
  double theta_suffix_candidate_max_planning_time = 60.0;
  int theta_unsafe_segment_lookback_points = 30;
  std::string suffix_goal_heading_mode_name = "DEFAULT";
  int diagnostic_min_suffix_cut_points = 0;
  int diagnostic_only_suffix_cut_points = 0;
  bool diagnostic_evaluate_all_suffix_candidates = false;
  double suffix_local_corridor_half_width_m = 0.0;
  double full_search_tolerance = 0.0;
  bool fresh_snapshot_revalidation = true;
  double fresh_snapshot_timeout = 5.0;
  bool include_order_wraparound = true;
  bool allow_unknown = false;
  bool downsample_obstacle_heuristic = true;
  bool allow_reverse_expansion = false;
  double analytic_expansion_max_length_m = 4.0;
  double change_penalty = 0.45;
  double non_straight_penalty = 1.05;
  double rotation_penalty = 10.0;
  int start_heading_seed_span = 1;
  int terminal_checking_interval = 100;
  int explicit_from_block = -1;
  int explicit_to_block = -1;
  int only_from_block = -1;
  int only_to_block = -1;
  bool explicit_pose_pair = false;
  double explicit_start_x = 0.0;
  double explicit_start_y = 0.0;
  double explicit_start_yaw = 0.0;
  double explicit_goal_x = 0.0;
  double explicit_goal_y = 0.0;
  double explicit_goal_yaw = 0.0;
  private_nh.param("plan_db_path", database_path, database_path);
  private_nh.param("plan_id", plan_id, plan_id);
  private_nh.param("costmap_topic", costmap_topic, costmap_topic);
  private_nh.param("lattice_filepath", lattice_filepath, lattice_filepath);
  private_nh.param("max_planning_time", max_planning_time, max_planning_time);
  private_nh.param(
    "theta_corridor_search_enabled",
    theta_corridor_search_enabled, theta_corridor_search_enabled);
  private_nh.param(
    "coarse_route_max_planning_time",
    coarse_route_max_planning_time, coarse_route_max_planning_time);
  private_nh.param(
    "corridor_level_max_planning_time",
    corridor_level_max_planning_time, corridor_level_max_planning_time);
  private_nh.param(
    "corridor_full_search_min_time",
    corridor_full_search_min_time, corridor_full_search_min_time);
  private_nh.param(
    "final_validation_reserve_time",
    final_validation_reserve_time, final_validation_reserve_time);
  private_nh.param(
    "corridor_route_progress_weight",
    corridor_route_progress_weight, corridor_route_progress_weight);
  private_nh.param(
    "theta_prefix_lattice_suffix_enabled",
    theta_prefix_lattice_suffix_enabled, theta_prefix_lattice_suffix_enabled);
  private_nh.param(
    "state_lattice_smoothing_enabled",
    state_lattice_smoothing_enabled, state_lattice_smoothing_enabled);
  private_nh.param(
    "state_lattice_smoother_max_iterations",
    smoother_params.max_iterations, smoother_params.max_iterations);
  private_nh.param(
    "state_lattice_smoother_w_data",
    smoother_params.w_data, smoother_params.w_data);
  private_nh.param(
    "state_lattice_smoother_w_smooth",
    smoother_params.w_smooth, smoother_params.w_smooth);
  private_nh.param(
    "state_lattice_smoother_tolerance",
    smoother_params.tolerance, smoother_params.tolerance);
  private_nh.param(
    "state_lattice_smoother_do_refinement",
    smoother_params.do_refinement, smoother_params.do_refinement);
  private_nh.param(
    "state_lattice_smoother_refinement_num",
    smoother_params.refinement_num, smoother_params.refinement_num);
  private_nh.param(
    "state_lattice_smoother_max_time",
    smoother_params.max_time, smoother_params.max_time);
  private_nh.param(
    "state_lattice_smoother_max_path_length_ratio",
    smoother_params.max_path_length_ratio, smoother_params.max_path_length_ratio);
  private_nh.param(
    "state_lattice_smoother_max_center_cost_increase",
    smoother_params.max_center_cost_increase, smoother_params.max_center_cost_increase);
  private_nh.param(
    "state_lattice_smoother_max_mean_center_cost_increase",
    smoother_params.max_mean_center_cost_increase,
    smoother_params.max_mean_center_cost_increase);
  private_nh.param(
    "state_lattice_smoother_max_curvature_regression_ratio",
    smoother_params.max_curvature_regression_ratio,
    smoother_params.max_curvature_regression_ratio);
  private_nh.param(
    "state_lattice_smoother_minimum_curvature_improvement",
    smoother_params.minimum_curvature_improvement,
    smoother_params.minimum_curvature_improvement);
  private_nh.param(
    "theta_max_allowed_cost", theta_max_allowed_cost, theta_max_allowed_cost);
  private_nh.param(
    "theta_w_traversal_cost", theta_w_traversal_cost, theta_w_traversal_cost);
  private_nh.param("theta_w_euc_cost", theta_w_euc_cost, theta_w_euc_cost);
  private_nh.param(
    "theta_w_heuristic_cost", theta_w_heuristic_cost, theta_w_heuristic_cost);
  private_nh.param(
    "theta_reference_spacing", theta_reference_spacing, theta_reference_spacing);
  private_nh.param(
    "theta_reference_smoothing_enabled",
    theta_reference_smoothing_enabled, theta_reference_smoothing_enabled);
  private_nh.param(
    "theta_suffix_candidate_max_planning_time",
    theta_suffix_candidate_max_planning_time,
    theta_suffix_candidate_max_planning_time);
  private_nh.param(
    "theta_unsafe_segment_lookback_points",
    theta_unsafe_segment_lookback_points,
    theta_unsafe_segment_lookback_points);
  private_nh.param(
    "suffix_goal_heading_mode",
    suffix_goal_heading_mode_name, suffix_goal_heading_mode_name);
  private_nh.param(
    "diagnostic_min_suffix_cut_points",
    diagnostic_min_suffix_cut_points, diagnostic_min_suffix_cut_points);
  private_nh.param(
    "diagnostic_only_suffix_cut_points",
    diagnostic_only_suffix_cut_points, diagnostic_only_suffix_cut_points);
  private_nh.param(
    "diagnostic_evaluate_all_suffix_candidates",
    diagnostic_evaluate_all_suffix_candidates,
    diagnostic_evaluate_all_suffix_candidates);
  private_nh.param(
    "suffix_local_corridor_half_width_m",
    suffix_local_corridor_half_width_m, suffix_local_corridor_half_width_m);
  private_nh.param(
    "full_search_tolerance", full_search_tolerance, full_search_tolerance);
  private_nh.param(
    "fresh_snapshot_revalidation",
    fresh_snapshot_revalidation, fresh_snapshot_revalidation);
  private_nh.param(
    "fresh_snapshot_timeout", fresh_snapshot_timeout, fresh_snapshot_timeout);
  private_nh.param(
    "include_order_wraparound", include_order_wraparound, include_order_wraparound);
  private_nh.param("allow_unknown", allow_unknown, allow_unknown);
  private_nh.param(
    "downsample_obstacle_heuristic",
    downsample_obstacle_heuristic, downsample_obstacle_heuristic);
  private_nh.param(
    "allow_reverse_expansion", allow_reverse_expansion, allow_reverse_expansion);
  private_nh.param(
    "analytic_expansion_max_length_m",
    analytic_expansion_max_length_m, analytic_expansion_max_length_m);
  private_nh.param("change_penalty", change_penalty, change_penalty);
  private_nh.param(
    "non_straight_penalty", non_straight_penalty, non_straight_penalty);
  private_nh.param("rotation_penalty", rotation_penalty, rotation_penalty);
  private_nh.param(
    "start_heading_seed_span", start_heading_seed_span, start_heading_seed_span);
  private_nh.param(
    "terminal_checking_interval", terminal_checking_interval, terminal_checking_interval);
  private_nh.param("explicit_from_block", explicit_from_block, explicit_from_block);
  private_nh.param("explicit_to_block", explicit_to_block, explicit_to_block);
  private_nh.param("only_from_block", only_from_block, only_from_block);
  private_nh.param("only_to_block", only_to_block, only_to_block);
  private_nh.param("explicit_pose_pair", explicit_pose_pair, explicit_pose_pair);
  private_nh.param("explicit_start_x", explicit_start_x, explicit_start_x);
  private_nh.param("explicit_start_y", explicit_start_y, explicit_start_y);
  private_nh.param("explicit_start_yaw", explicit_start_yaw, explicit_start_yaw);
  private_nh.param("explicit_goal_x", explicit_goal_x, explicit_goal_x);
  private_nh.param("explicit_goal_y", explicit_goal_y, explicit_goal_y);
  private_nh.param("explicit_goal_yaw", explicit_goal_yaw, explicit_goal_yaw);

  try {
    Database database(database_path);
    if (plan_id.empty()) {
      plan_id = database.latestPlanId();
    }
    const std::vector<int> order = database.executionOrder(plan_id);
    auto blocks = database.blocks(plan_id);
    smac_lattice_planner_mbf::live_validator::ConnectionSelection selection;
    if (explicit_pose_pair) {
      const double explicit_values[] = {
        explicit_start_x, explicit_start_y, explicit_start_yaw,
        explicit_goal_x, explicit_goal_y, explicit_goal_yaw};
      if (!std::all_of(
          std::begin(explicit_values), std::end(explicit_values),
          [](double value) {return std::isfinite(value);}))
      {
        throw std::invalid_argument("explicit pose pair values must be finite");
      }
      if (explicit_from_block >= 0 || explicit_to_block >= 0 ||
        only_from_block >= 0 || only_to_block >= 0)
      {
        throw std::invalid_argument(
                "explicit_pose_pair cannot be combined with block-pair selection");
      }
      constexpr int kExplicitStartId = -100;
      constexpr int kExplicitGoalId = -101;
      BlockEndpoints start;
      start.id = kExplicitStartId;
      start.entry_x = start.exit_x = explicit_start_x;
      start.entry_y = start.exit_y = explicit_start_y;
      start.entry_yaw = start.exit_yaw = explicit_start_yaw;
      BlockEndpoints goal;
      goal.id = kExplicitGoalId;
      goal.entry_x = goal.exit_x = explicit_goal_x;
      goal.entry_y = goal.exit_y = explicit_goal_y;
      goal.entry_yaw = goal.exit_yaw = explicit_goal_yaw;
      blocks.emplace(start.id, start);
      blocks.emplace(goal.id, goal);
      selection.mode = "explicit_pose_pair";
      selection.pairs.push_back({start.id, goal.id});
    } else {
      selection = smac_lattice_planner_mbf::live_validator::selectConnections(
        order, explicit_from_block, explicit_to_block, only_from_block, only_to_block);
    }
    if (!explicit_pose_pair && include_order_wraparound && explicit_from_block < 0 &&
      order.size() >= 2u &&
      order.front() != order.back())
    {
      const smac_lattice_planner_mbf::live_validator::ConnectionIds wrap{
        order.back(), order.front()};
      if ((only_from_block < 0 || wrap.from == only_from_block) &&
        (only_to_block < 0 || wrap.to == only_to_block))
      {
        selection.pairs.push_back(wrap);
        selection.mode += "_with_wrap";
      }
    }
    for (const auto & pair : selection.pairs) {
      if (blocks.find(pair.from) == blocks.end()) {
        throw std::invalid_argument(
                "selected from block " + std::to_string(pair.from) +
                " does not exist in plan " + plan_id);
      }
      if (blocks.find(pair.to) == blocks.end()) {
        throw std::invalid_argument(
                "selected to block " + std::to_string(pair.to) +
                " does not exist in plan " + plan_id);
      }
    }

    ROS_INFO("Waiting for read-only costmap snapshot on %s", costmap_topic.c_str());
    const auto message = ros::topic::waitForMessage<nav_msgs::OccupancyGrid>(
      costmap_topic, ros::Duration(15.0));
    if (!message) {
      throw std::runtime_error("timed out waiting for global costmap snapshot");
    }
    auto costmap = convertCostmap(*message);
    if (std::abs(costmap->getResolution() - 0.05) > 1e-5) {
      throw std::runtime_error("live costmap resolution does not match 5 cm lattice");
    }

    const auto metadata =
      nav2_smac_planner::LatticeMotionTable::getLatticeMetadata(lattice_filepath);
    const double max_lattice_position_residual =
      std::sqrt(2.0) * costmap->getResolution() + 1e-3;
    const double max_nearest_bin_yaw_residual =
      M_PI / static_cast<double>(metadata.number_of_headings) + 0.002;
    start_heading_seed_span = std::max(
      0, std::min(
        start_heading_seed_span,
        static_cast<int>(metadata.number_of_headings / 4u)));
    terminal_checking_interval = std::max(1, terminal_checking_interval);
    max_planning_time = std::max(0.1, max_planning_time);
    coarse_route_max_planning_time = std::max(0.05, coarse_route_max_planning_time);
    corridor_level_max_planning_time = std::max(0.05, corridor_level_max_planning_time);
    corridor_full_search_min_time = std::max(0.0, corridor_full_search_min_time);
    final_validation_reserve_time = std::max(0.0, final_validation_reserve_time);
    corridor_route_progress_weight = std::max(0.0, corridor_route_progress_weight);
    theta_max_allowed_cost = std::clamp(
      theta_max_allowed_cost, 0,
      static_cast<int>(smac_lattice_planner_mbf::kStateCenterMaxAllowedCost));
    if (!std::isfinite(theta_w_traversal_cost) ||
      !std::isfinite(theta_w_euc_cost) ||
      !std::isfinite(theta_w_heuristic_cost) ||
      theta_w_traversal_cost < 0.0 || theta_w_euc_cost < 0.0 ||
      theta_w_heuristic_cost < 0.0)
    {
      throw std::invalid_argument("Theta composite weights must be finite and non-negative");
    }
    theta_reference_spacing = std::max(0.005, std::abs(theta_reference_spacing));
    theta_suffix_candidate_max_planning_time = std::max(
      0.05, theta_suffix_candidate_max_planning_time);
    theta_unsafe_segment_lookback_points = std::max(
      1, theta_unsafe_segment_lookback_points);
    diagnostic_min_suffix_cut_points = std::max(0, diagnostic_min_suffix_cut_points);
    diagnostic_only_suffix_cut_points = std::max(0, diagnostic_only_suffix_cut_points);
    if (!std::isfinite(suffix_local_corridor_half_width_m) ||
      suffix_local_corridor_half_width_m < 0.0)
    {
      throw std::invalid_argument(
              "suffix_local_corridor_half_width_m must be finite and non-negative");
    }
    if (!std::isfinite(full_search_tolerance) || full_search_tolerance < 0.0) {
      throw std::invalid_argument("full_search_tolerance must be finite and non-negative");
    }
    if (!std::isfinite(fresh_snapshot_timeout) || fresh_snapshot_timeout <= 0.0) {
      throw std::invalid_argument("fresh_snapshot_timeout must be finite and positive");
    }
    if (!std::isfinite(analytic_expansion_max_length_m) ||
      analytic_expansion_max_length_m <= 0.0)
    {
      throw std::invalid_argument(
              "analytic_expansion_max_length_m must be finite and positive");
    }
    if (!std::isfinite(change_penalty) || change_penalty < 0.0 ||
      !std::isfinite(non_straight_penalty) || non_straight_penalty < 1.0 ||
      !std::isfinite(rotation_penalty) || rotation_penalty < 0.0)
    {
      throw std::invalid_argument(
              "State penalties must be finite with change/rotation >= 0 and non_straight >= 1");
    }
    const auto suffix_goal_heading_mode =
      nav2_smac_planner::fromStringToGH(suffix_goal_heading_mode_name);
    if (suffix_goal_heading_mode == nav2_smac_planner::GoalHeadingMode::UNKNOWN) {
      throw std::invalid_argument(
              "suffix_goal_heading_mode must be DEFAULT, BIDIRECTIONAL, or ALL_DIRECTION");
    }
    nav2_smac_planner::SearchInfo search_info;
    search_info.lattice_filepath = lattice_filepath;
    search_info.minimum_turning_radius = metadata.min_turning_radius / costmap->getResolution();
    search_info.reverse_penalty = 4.0f;
    search_info.change_penalty = static_cast<float>(change_penalty);
    search_info.non_straight_penalty = static_cast<float>(non_straight_penalty);
    search_info.cost_penalty = 5.0f;
    search_info.retrospective_penalty = 0.015f;
    // Match the production preference for continuous forward arcs while
    // retaining rotate-in-place as a necessary-space fallback.
    search_info.rotation_penalty = static_cast<float>(rotation_penalty);
    search_info.allow_reverse_expansion = allow_reverse_expansion;
    search_info.prefer_forward_analytic_expansion = true;
    search_info.downsample_obstacle_heuristic = downsample_obstacle_heuristic;
    search_info.analytic_expansion_ratio = 3.5f;
    search_info.analytic_expansion_max_length =
      static_cast<float>(analytic_expansion_max_length_m / costmap->getResolution());
    search_info.analytic_expansion_max_cost = 200.0f;

    nav2_smac_planner::GridCollisionChecker checker(costmap.get(), 72u);
    checker.setFootprint(robotFootprint(), false, 0.0);
    checker.setCollisionCheckResolution(0.01);
    smac_lattice_planner_mbf::StateLatticeSmoother state_lattice_smoother(
      smoother_params, metadata.min_turning_radius);
    int max_iterations = 2000000;
    nav2_smac_planner::AStarAlgorithm<nav2_smac_planner::NodeLattice> planner(
      nav2_smac_planner::MotionModel::STATE_LATTICE, search_info);
    planner.initialize(
      allow_unknown, max_iterations, 1000, terminal_checking_interval,
      max_planning_time, 401.0, 32u);

    int failures = 0;
    int tested_connections = 0;
    int composite_passes = 0;
    int fallback_passes = 0;
    const double setup_seconds = elapsedSeconds(run_started);
    for (const auto & connection : selection.pairs) {
      const auto case_started = SteadyClock::now();
      const auto & previous = blocks.at(connection.from);
      const auto & next = blocks.at(connection.to);
      ++tested_connections;
      const double euclidean_distance = std::hypot(
        next.entry_x - previous.exit_x, next.entry_y - previous.exit_y);
      std::cout << "PAIR mode=" << selection.mode
                << " exact_pair=true from=" << previous.id << " to=" << next.id
                << " start_x=" << previous.exit_x
                << " start_y=" << previous.exit_y
                << " start_yaw=" << previous.exit_yaw
                << " goal_x=" << next.entry_x
                << " goal_y=" << next.entry_y
                << " goal_yaw=" << next.entry_yaw
                << " change_penalty=" << change_penalty
                << " non_straight_penalty=" << non_straight_penalty
                << " rotation_penalty=" << rotation_penalty
                << " euclidean_m=" << euclidean_distance << std::endl;
      float start_x = 0.0f;
      float start_y = 0.0f;
      float goal_x = 0.0f;
      float goal_y = 0.0f;
      if (!worldToMapContinuous(
          *costmap, previous.exit_x, previous.exit_y, start_x, start_y) ||
        !worldToMapContinuous(
          *costmap, next.entry_x, next.entry_y, goal_x, goal_y))
      {
        std::cout << "FAIL " << previous.id << "->" << next.id
                  << " mode=" << selection.mode
                  << " termination=endpoint_outside_costmap"
                  << " search_seconds=0.000000 validation_seconds=0.000000"
                  << " case_end_to_end_seconds=" << elapsedSeconds(case_started)
                  << " end_to_end_seconds=" << elapsedSeconds(run_started)
                  << std::endl;
        ++failures;
        continue;
      }

      double search_seconds = 0.0;
      double coarse_seconds = 0.0;
      double validation_seconds = 0.0;
      int iterations = 0;
      try {
        const auto maybeSmoothStatePath = [&] (
            smac_lattice_planner_mbf::theta_state_suffix::PosePath & state_path,
            const SteadyClock::time_point & smoothing_deadline,
            const std::string & smoothing_mode)
          {
            if (!state_lattice_smoothing_enabled) {
              return false;
            }
            const auto smoothing_started = SteadyClock::now();
            const auto smoothing = state_lattice_smoother.smooth(
              state_path, *costmap, checker, allow_unknown, smoothing_deadline,
              []() {return !ros::ok();});
            const double smoothing_seconds = elapsedSeconds(smoothing_started);
            std::cout << "SMOOTHER " << previous.id << "->" << next.id
                      << " mode=" << smoothing_mode
                      << " accepted=" << (smoothing.accepted ? "true" : "false")
                      << " seconds=" << smoothing_seconds
                      << " iterations=" << smoothing.iterations
                      << " raw_length_m=" << smoothing.raw_quality.length_m
                      << " candidate_length_m=" << smoothing.candidate_quality.length_m
                      << " raw_mean_center_cost=" <<
              smoothing.raw_quality.mean_center_cost
                      << " candidate_mean_center_cost=" <<
              smoothing.candidate_quality.mean_center_cost
                      << " raw_max_center_cost=" <<
              smoothing.raw_quality.max_center_cost
                      << " candidate_max_center_cost=" <<
              smoothing.candidate_quality.max_center_cost
                      << " raw_p95_curvature_radpm=" <<
              smoothing.raw_quality.p95_abs_curvature_radpm
                      << " candidate_p95_curvature_radpm=" <<
              smoothing.candidate_quality.p95_abs_curvature_radpm
                      << " raw_curvature_tv_radpm=" <<
              smoothing.raw_quality.curvature_total_variation_radpm
                      << " candidate_curvature_tv_radpm=" <<
              smoothing.candidate_quality.curvature_total_variation_radpm
                      << " raw_max_curvature_jump_radpm=" <<
              smoothing.raw_quality.max_curvature_jump_radpm
                      << " candidate_max_curvature_jump_radpm=" <<
              smoothing.candidate_quality.max_curvature_jump_radpm
                      << " raw_curvature_direction_changes=" <<
              smoothing.raw_quality.curvature_direction_changes
                      << " candidate_curvature_direction_changes=" <<
              smoothing.candidate_quality.curvature_direction_changes
                      << " reason=" << std::quoted(smoothing.reason) << std::endl;
            if (!smoothing.accepted) {
              return false;
            }

            std::string reason;
            if (!smac_lattice_planner_mbf::theta_state_suffix::
              containsKinematicallyContinuousForwardOrRotation(
                smoothing.path, reason) ||
              !smac_lattice_planner_mbf::theta_state_suffix::
              terminalAvoidsStationaryYawRepair(smoothing.path, reason))
            {
              std::cout << "SMOOTHER_REJECT " << previous.id << "->" << next.id
                        << " mode=" << smoothing_mode
                        << " stage=state_contract reason=" << std::quoted(reason)
                        << std::endl;
              return false;
            }
            const auto validation_started = SteadyClock::now();
            const bool safe = validateContinuousPosePath(
              *costmap, checker, smoothing.path, allow_unknown, reason,
              &smoothing_deadline);
            validation_seconds += elapsedSeconds(validation_started);
            if (!safe) {
              std::cout << "SMOOTHER_REJECT " << previous.id << "->" << next.id
                        << " mode=" << smoothing_mode
                        << " stage=continuous_footprint reason=" << std::quoted(reason)
                        << std::endl;
              return false;
            }
            state_path = smoothing.path;
            return true;
          };

        planner.setCollisionChecker(&checker);
        const unsigned int goal_bin =
          planner.getContext()->motion_table.getClosestAngularBin(next.entry_yaw);
        using HeadingSeeds = std::vector<std::pair<unsigned int, float>>;
        auto makeSafeHeadingSeeds = [&] (
            double wx, double wy, double exact_yaw,
            float & map_x, float & map_y,
            HeadingSeeds & seeds, std::string & reason)
          {
            seeds.clear();
            if (!worldToMapContinuous(*costmap, wx, wy, map_x, map_y)) {
              reason = "segment start is outside the costmap";
              return false;
            }
            const unsigned int start_bin =
              planner.getContext()->motion_table.getClosestAngularBin(exact_yaw);
            seeds.reserve(static_cast<std::size_t>(1 + 2 * start_heading_seed_span));
            for (int radius = 0; radius <= start_heading_seed_span; ++radius) {
              const int directions = radius == 0 ? 1 : 2;
              for (int direction = 0; direction < directions; ++direction) {
                const int signed_offset =
                  radius == 0 ? 0 : (direction == 0 ? -radius : radius);
                const int wrapped =
                  (static_cast<int>(start_bin) + signed_offset +
                  static_cast<int>(metadata.number_of_headings)) %
                  static_cast<int>(metadata.number_of_headings);
                const auto candidate_bin = static_cast<unsigned int>(wrapped);
                const double candidate_yaw =
                  planner.getContext()->motion_table.getAngleFromBin(candidate_bin);
                if (checker.inCollisionContinuous(
                    map_x, map_y, exact_yaw,
                    map_x, map_y, candidate_yaw, allow_unknown))
                {
                  continue;
                }
                const double yaw_error = std::abs(
                  angles::shortest_angular_distance(exact_yaw, candidate_yaw));
                const double bin_width = 2.0 * M_PI / metadata.number_of_headings;
                const float correction_cost = static_cast<float>(
                  search_info.rotation_penalty * yaw_error / bin_width);
                seeds.emplace_back(candidate_bin, correction_cost);
              }
            }
            if (seeds.empty()) {
              reason = "no collision-free heading quantization from the exact segment start";
              return false;
            }
            return true;
          };

        std::string start_seed_reason;
        std::vector<std::pair<unsigned int, float>> safe_start_bins;
        if (!makeSafeHeadingSeeds(
            previous.exit_x, previous.exit_y, previous.exit_yaw,
            start_x, start_y, safe_start_bins, start_seed_reason))
        {
          std::cout << "FAIL " << previous.id << "->" << next.id
                    << " mode=" << selection.mode
                    << " termination=no_collision_free_start_heading_seed"
                    << " reason=" << std::quoted(start_seed_reason)
                    << " search_seconds=0.000000 validation_seconds=0.000000"
                    << " case_end_to_end_seconds=" << elapsedSeconds(case_started)
                    << " end_to_end_seconds=" << elapsedSeconds(run_started)
                    << std::endl;
          ++failures;
          continue;
        }

        nav2_smac_planner::NodeLattice::CoordinateVector path;
        const auto toClockDuration = [](double seconds) {
            return std::chrono::duration_cast<SteadyClock::duration>(
              std::chrono::duration<double>(std::max(0.0, seconds)));
          };
        const auto search_deadline =
          case_started + toClockDuration(max_planning_time - final_validation_reserve_time);
        const auto overall_deadline =
          case_started + toClockDuration(max_planning_time);
        const auto corridor_deadline =
          search_deadline - toClockDuration(corridor_full_search_min_time);

        smac_lattice_planner_mbf::CoarseRouteCorridorResult coarse_route;
        if ((theta_prefix_lattice_suffix_enabled || theta_corridor_search_enabled) &&
          !allow_unknown &&
          SteadyClock::now() < corridor_deadline)
        {
          const auto coarse_deadline = std::min(
            corridor_deadline,
            SteadyClock::now() + toClockDuration(coarse_route_max_planning_time));
          smac_lattice_planner_mbf::CoarseRouteCorridorOptions options;
          options.terminal_checking_interval = 100;
          options.build_center_corridors = theta_corridor_search_enabled;
          if (theta_prefix_lattice_suffix_enabled) {
            options.theta_allow_unknown = false;
            options.theta_max_allowed_cost = theta_max_allowed_cost;
            options.theta_w_traversal_cost = theta_w_traversal_cost;
            options.theta_w_euc_cost = theta_w_euc_cost;
            options.theta_w_heuristic_cost = theta_w_heuristic_cost;
            options.reference_spacing_m = theta_reference_spacing;
            options.smooth_reference = theta_reference_smoothing_enabled;
          }
          options.cancel_checker = [coarse_deadline]() {
              return SteadyClock::now() >= coarse_deadline;
            };
          const auto coarse_started = SteadyClock::now();
          coarse_route = smac_lattice_planner_mbf::CoarseRouteCorridorHelper::build(
            *costmap,
            previous.exit_x, previous.exit_y,
            next.entry_x, next.entry_y,
            options);
          coarse_seconds = elapsedSeconds(coarse_started);
          if (coarse_route.succeeded()) {
            const auto & guide = coarse_route.routes.front();
            std::cout << "GUIDE " << previous.id << "->" << next.id
                      << " status=success length_m=" << guide.length_m
                      << " points=" << guide.points.size()
                      << " reference_points=" << guide.reference_points.size()
                      << " theta_nodes=" << guide.theta_nodes_opened
                      << " seconds=" << coarse_seconds << std::endl;
          } else {
            std::cout << "GUIDE " << previous.id << "->" << next.id
                      << " status=failed seconds=" << coarse_seconds
                      << " reason=" << std::quoted(coarse_route.message) << std::endl;
          }
        }

        auto runStateSearch = [&] (
            float segment_start_x,
            float segment_start_y,
            const HeadingSeeds & segment_start_bins,
            const smac_lattice_planner_mbf::CenterCorridorMask * domain,
            const smac_lattice_planner_mbf::RouteProgressField * route_progress,
            double time_limit,
            nav2_smac_planner::GoalHeadingMode goal_heading_mode,
            float search_tolerance,
            nav2_smac_planner::NodeLattice::CoordinateVector & output,
            int & level_iterations,
            double & attempt_seconds)
          {
            int level_max_iterations = max_iterations;
            planner.initialize(
              allow_unknown, level_max_iterations, 1000, terminal_checking_interval,
              std::max(0.05, time_limit), 401.0, 32u);
            planner.setCollisionChecker(&checker);
            output.clear();
            level_iterations = 0;
            const auto state_started = SteadyClock::now();
            nav2_smac_planner::SearchResult result;
            try {
              planner.setGoalTransitionValidator(
                [](const nav2_smac_planner::NodeLattice::Coordinates & from,
                  const nav2_smac_planner::NodeLattice::Coordinates & to) {
                  return std::hypot(to.x - from.x, to.y - from.y) > 1e-4f;
                });
              if (domain != nullptr) {
                planner.setCenterDomain(
                  [domain](float mx, float my) {
                    if (!std::isfinite(mx) || !std::isfinite(my) || mx < 0.0f || my < 0.0f) {
                      return false;
                    }
                    return domain->isAllowed(
                      static_cast<unsigned int>(std::floor(mx)),
                      static_cast<unsigned int>(std::floor(my)));
                  });
              } else {
                planner.clearCenterDomain();
              }
              if (route_progress != nullptr && corridor_route_progress_weight > 0.0) {
                const float progress_scale = static_cast<float>(
                  corridor_route_progress_weight *
                  (1.0 - search_info.retrospective_penalty) /
                  costmap->getResolution());
                planner.setAdditionalHeuristic(
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
                planner.clearAdditionalHeuristic();
              }
              planner.setStart(
                segment_start_x, segment_start_y,
                segment_start_bins.front().first, segment_start_bins.front().second);
              for (std::size_t seed = 1; seed < segment_start_bins.size(); ++seed) {
                planner.addStart(
                  segment_start_x, segment_start_y,
                  segment_start_bins[seed].first, segment_start_bins[seed].second);
              }
              planner.setGoal(goal_x, goal_y, goal_bin, goal_heading_mode, 1);
              result = planner.createPathDetailed(
                output, level_iterations, search_tolerance, []() {return false;});
            } catch (...) {
              planner.clearCenterDomain();
              planner.clearAdditionalHeuristic();
              planner.clearGoalTransitionValidator();
              throw;
            }
            planner.clearCenterDomain();
            planner.clearAdditionalHeuristic();
            planner.clearGoalTransitionValidator();
            attempt_seconds = elapsedSeconds(state_started);
            search_seconds += attempt_seconds;
            return result;
          };

        bool success = false;
        bool composite_success = false;
        bool suppress_full_state_fallback = false;
        smac_lattice_planner_mbf::theta_state_suffix::PosePath composite_path;
        smac_lattice_planner_mbf::theta_state_suffix::PosePath fallback_pose_path;
        nav2_smac_planner::NodeLattice::CoordinateVector selected_state_path;
        std::size_t selected_cut_points = 0u;
        std::size_t selected_cut_index = 0u;
        std::size_t selected_theta_prefix_poses = 0u;
        std::size_t selected_state_suffix_poses = 0u;
        PathQualitySummary selected_state_quality;
        double selected_state_suffix_seconds = 0.0;
        int selected_state_suffix_expansions = 0;
        int state_suffix_attempts = 0;
        double state_suffix_total_seconds = 0.0;
        int state_suffix_total_expansions = 0;
        double selected_lattice_goal_dx = 0.0;
        double selected_lattice_goal_dy = 0.0;
        double selected_lattice_goal_signed_yaw_delta = 0.0;
        double selected_lattice_goal_yaw_error = 0.0;
        std::string selected_terminal_append_kind = "not_applicable";
        std::string selected_mppi_terminal_phase = "not_applicable";
        double theta_length_m = coarse_route.succeeded() ?
          coarse_route.routes.front().length_m : 0.0;
        std::size_t theta_reference_points = coarse_route.succeeded() ?
          coarse_route.routes.front().reference_points.size() : 0u;
        std::string search_mode;
        nav2_smac_planner::SearchTermination exact_termination =
          nav2_smac_planner::SearchTermination::OPEN_EXHAUSTED;

        if (theta_prefix_lattice_suffix_enabled && coarse_route.succeeded() &&
          coarse_route.routes.front().reference_points.size() >= 2u)
        {
          const std::string frame = message->header.frame_id.empty() ? "map" :
            message->header.frame_id;
          auto theta_reference = makeThetaReference(
            coarse_route.routes.front(), previous, next, frame);
          const bool short_theta_reference =
            theta_reference.size() <=
            smac_lattice_planner_mbf::theta_state_suffix::
            kSuffixPointCountCandidates.front();
          if (!short_theta_reference) {
            std::string initial_alignment_reason;
            if (!smac_lattice_planner_mbf::theta_state_suffix::
              ensureExplicitInitialTangentRotation(
                theta_reference, initial_alignment_reason))
            {
              std::cout << "COMPOSITE_DISABLED " << previous.id << "->" << next.id
                        << " reason=" << std::quoted(initial_alignment_reason)
                        << std::endl;
              theta_reference.clear();
            }
          }
          std::vector<smac_lattice_planner_mbf::theta_state_suffix::ThetaPrefixCut>
            suffix_candidates;
          if (short_theta_reference) {
            suffix_candidates.push_back(
              smac_lattice_planner_mbf::theta_state_suffix::selectThetaPrefixCut(
                theta_reference, theta_reference.size()));
          } else if (!theta_reference.empty()) {
            suffix_candidates = smac_lattice_planner_mbf::theta_state_suffix::
              makeThetaPrefixCutCandidates(theta_reference);
          }
          bool adaptive_unsafe_segment_fallback_requested = false;
          bool adaptive_unsafe_segment_fallback_scheduled = false;
          bool adaptive_unsafe_segment_fallback_attempted = false;
          std::size_t adaptive_cut_index = std::numeric_limits<std::size_t>::max();

          for (std::size_t candidate_index = 0u;
            candidate_index < suffix_candidates.size(); ++candidate_index)
          {
            const auto cut = suffix_candidates[candidate_index];
            const bool cut_is_adaptive =
              adaptive_unsafe_segment_fallback_requested &&
              cut.cut_index == adaptive_cut_index;
            const bool cut_is_short_route_all_state =
              short_theta_reference && cut.is_full_path;
            if (cut_is_adaptive) {
              adaptive_unsafe_segment_fallback_attempted = true;
            }
            if (!cut_is_adaptive && !cut_is_short_route_all_state &&
              diagnostic_only_suffix_cut_points > 0 &&
              cut.effective_suffix_point_count !=
              static_cast<std::size_t>(diagnostic_only_suffix_cut_points))
            {
              continue;
            }
            if (!cut_is_adaptive && !cut_is_short_route_all_state &&
              diagnostic_min_suffix_cut_points > 0 &&
              cut.effective_suffix_point_count <
              static_cast<std::size_t>(diagnostic_min_suffix_cut_points))
            {
              continue;
            }
            if (cut.is_full_path && theta_reference.size() >
              smac_lattice_planner_mbf::theta_state_suffix::
              kSuffixPointCountCandidates.back())
            {
              break;
            }
            const auto candidate_deadline =
              (cut_is_adaptive || cut_is_short_route_all_state) ?
              search_deadline : corridor_deadline;
            const double available = std::chrono::duration<double>(
              candidate_deadline - SteadyClock::now()).count();
            if (available < 0.05) {
              break;
            }

            std::string prefix_motion_reason;
            const bool prefix_kinematically_continuous =
              smac_lattice_planner_mbf::theta_state_suffix::
              containsKinematicallyContinuousForwardOrRotation(
                cut.prefix_including_cut, prefix_motion_reason);
            if (!prefix_kinematically_continuous) {
              std::cout << "COMPOSITE_ATTEMPT " << previous.id << "->" << next.id
                        << " cut_points=" << cut.effective_suffix_point_count
                        << " cut_index=" << cut.cut_index
                        << " theta_prefix_poses=" << cut.prefix_including_cut.size()
                        << " prefix_kinematically_continuous=false"
                        << " prefix_safe=false suffix_started=false"
                        << " suffix_seconds=0.000000 suffix_expansions=0"
                        << " full_continuous_footprint=false"
                        << " reason=" << std::quoted(prefix_motion_reason) << std::endl;
              continue;
            }

            std::string prefix_reason;
            std::size_t first_unsafe_segment = std::numeric_limits<std::size_t>::max();
            const bool prefix_safe = validateContinuousPosePath(
              *costmap, checker, cut.prefix_including_cut,
              allow_unknown, prefix_reason, &candidate_deadline,
              &first_unsafe_segment);
            if (!prefix_safe) {
              std::cout << "COMPOSITE_ATTEMPT " << previous.id << "->" << next.id
                        << " cut_points=" << cut.effective_suffix_point_count
                        << " cut_index=" << cut.cut_index
                        << " theta_prefix_poses=" << cut.prefix_including_cut.size()
                        << " prefix_safe=false suffix_started=false"
                        << " suffix_seconds=0.000000 suffix_expansions=0"
                        << " full_continuous_footprint=false"
                        << " reason=" << std::quoted(prefix_reason) << std::endl;
              if (first_unsafe_segment != std::numeric_limits<std::size_t>::max()) {
                adaptive_unsafe_segment_fallback_requested = true;
                const auto adaptive =
                  smac_lattice_planner_mbf::theta_state_suffix::
                  selectThetaPrefixCutBeforeUnsafeSegment(
                  theta_reference, first_unsafe_segment,
                  static_cast<std::size_t>(theta_unsafe_segment_lookback_points));
                suffix_candidates.resize(candidate_index + 1u);
                if (adaptive.cut_index < cut.cut_index) {
                  adaptive_unsafe_segment_fallback_scheduled = true;
                  adaptive_cut_index = adaptive.cut_index;
                  suffix_candidates.push_back(adaptive);
                  std::cout << "COMPOSITE_ADAPTIVE_FALLBACK "
                            << previous.id << "->" << next.id
                            << " unsafe_segment=" << first_unsafe_segment
                            << " lookback_points=" << theta_unsafe_segment_lookback_points
                            << " cut_index=" << adaptive.cut_index
                            << " suffix_poses=" << adaptive.effective_suffix_point_count
                            << std::endl;
                } else {
                  std::cout << "COMPOSITE_ADAPTIVE_FALLBACK "
                            << previous.id << "->" << next.id
                            << " unsafe_segment=" << first_unsafe_segment
                            << " lookback_points=" << theta_unsafe_segment_lookback_points
                            << " cut_index=0 suffix_poses=0"
                            << " status=whole_route_state_suppressed"
                            << std::endl;
                }
              }
              if (SteadyClock::now() >= candidate_deadline &&
                !adaptive_unsafe_segment_fallback_scheduled)
              {
                break;
              }
              continue;
            }

            float suffix_start_x = 0.0f;
            float suffix_start_y = 0.0f;
            HeadingSeeds suffix_start_bins;
            std::string suffix_start_reason;
            const double suffix_start_yaw = tf::getYaw(cut.cut.pose.orientation);
            if (!makeSafeHeadingSeeds(
                cut.cut.pose.position.x, cut.cut.pose.position.y, suffix_start_yaw,
                suffix_start_x, suffix_start_y, suffix_start_bins,
                suffix_start_reason))
            {
              std::cout << "COMPOSITE_ATTEMPT " << previous.id << "->" << next.id
                        << " cut_points=" << cut.effective_suffix_point_count
                        << " cut_index=" << cut.cut_index
                        << " theta_prefix_poses=" << cut.prefix_including_cut.size()
                        << " prefix_safe=true suffix_started=false"
                        << " suffix_seconds=0.000000 suffix_expansions=0"
                        << " full_continuous_footprint=false"
                        << " reason=" << std::quoted(suffix_start_reason) << std::endl;
              continue;
            }

            int suffix_iterations = 0;
            double suffix_seconds = 0.0;
            std::vector<smac_lattice_planner_mbf::CenterCorridorMask>
              suffix_local_masks;
            smac_lattice_planner_mbf::RouteProgressField suffix_local_progress;
            const smac_lattice_planner_mbf::CenterCorridorMask * suffix_local_domain = nullptr;
            if (suffix_local_corridor_half_width_m > 0.0) {
              std::vector<geometry_msgs::Point> suffix_route;
              suffix_route.reserve(theta_reference.size() - cut.cut_index);
              for (std::size_t index = cut.cut_index;
                index < theta_reference.size(); ++index)
              {
                suffix_route.push_back(theta_reference[index].pose.position);
              }
              smac_lattice_planner_mbf::CoarseRouteCorridorHelper::buildCenterMasksForRoute(
                *costmap, suffix_route, suffix_local_masks, suffix_local_progress);
              for (const auto & mask : suffix_local_masks) {
                if (std::abs(
                    mask.halfWidthM() - suffix_local_corridor_half_width_m) <= 1e-6)
                {
                  suffix_local_domain = &mask;
                  break;
                }
              }
              if (suffix_local_domain == nullptr) {
                throw std::invalid_argument(
                        "suffix_local_corridor_half_width_m must be one of 1, 2, 4, or 8");
              }
            }
            // Prefix proof, seed construction and optional mask construction
            // are charged to the same production-equivalent corridor budget.
            // Never launch A* with the stale pre-proof availability value.
            const double available_after_proof = std::chrono::duration<double>(
              candidate_deadline - SteadyClock::now()).count();
            if (available_after_proof < 0.05) {
              break;
            }
            const auto result = runStateSearch(
              suffix_start_x, suffix_start_y, suffix_start_bins,
              suffix_local_domain, nullptr,
              (cut_is_adaptive || cut_is_short_route_all_state) ?
              available_after_proof :
              std::min(theta_suffix_candidate_max_planning_time, available_after_proof),
              suffix_goal_heading_mode,
              0.0f, path, suffix_iterations, suffix_seconds);
            ++state_suffix_attempts;
            state_suffix_total_seconds += suffix_seconds;
            state_suffix_total_expansions += suffix_iterations;
            iterations += suffix_iterations;
            exact_termination = result.termination;

            std::string suffix_reason;
            const bool exact_suffix =
              result.termination == nav2_smac_planner::SearchTermination::SUCCESS &&
              result.hasPath() && validateContinuousPath(
                checker, path, suffix_start_x, suffix_start_y,
                suffix_start_yaw, allow_unknown, suffix_reason, &candidate_deadline);
            if (!exact_suffix) {
              std::cout << "COMPOSITE_ATTEMPT " << previous.id << "->" << next.id
                        << " cut_points=" << cut.effective_suffix_point_count
                        << " cut_index=" << cut.cut_index
                        << " theta_prefix_poses=" << cut.prefix_including_cut.size()
                        << " prefix_safe=true suffix_started=true"
                        << " suffix_goal_heading_mode=" << suffix_goal_heading_mode_name
                        << " analytic_expansion_max_length_m=" <<
              analytic_expansion_max_length_m
                        << " allow_reverse_expansion=" <<
              (allow_reverse_expansion ? "true" : "false")
                        << " suffix_local_corridor_half_width_m=" <<
              suffix_local_corridor_half_width_m
                        << " suffix_termination=" << searchTerminationName(result.termination)
                        << " suffix_path=" << (result.hasPath() ? "yes" : "no")
                        << " suffix_seconds=" << suffix_seconds
                        << " suffix_expansions=" << suffix_iterations
                        << " full_continuous_footprint=false";
              if (!suffix_reason.empty()) {
                std::cout << " reason=" << std::quoted(suffix_reason);
              }
              std::cout << std::endl;
              continue;
            }

            const double lattice_goal_x = costmap->getOriginX() +
              static_cast<double>(path.front().x) * costmap->getResolution();
            const double lattice_goal_y = costmap->getOriginY() +
              static_cast<double>(path.front().y) * costmap->getResolution();
            const double lattice_goal_dx = lattice_goal_x - next.entry_x;
            const double lattice_goal_dy = lattice_goal_y - next.entry_y;
            const double lattice_goal_position_error =
              std::hypot(lattice_goal_dx, lattice_goal_dy);
            const double lattice_goal_signed_yaw_delta =
              angles::shortest_angular_distance(path.front().theta, next.entry_yaw);
            const double lattice_goal_yaw_error =
              std::abs(lattice_goal_signed_yaw_delta);
            const std::string terminal_append_kind = "none";
            const std::string mppi_terminal_phase = "non_rotation";

            if (lattice_goal_position_error > max_lattice_position_residual ||
              lattice_goal_yaw_error > 0.20 ||
              lattice_goal_yaw_error > max_nearest_bin_yaw_residual)
            {
              std::cout << "COMPOSITE_ATTEMPT " << previous.id << "->" << next.id
                        << " cut_points=" << cut.effective_suffix_point_count
                        << " prefix_safe=true suffix_started=true"
                        << " suffix_termination=" << searchTerminationName(result.termination)
                        << " suffix_seconds=" << suffix_seconds
                        << " suffix_expansions=" << suffix_iterations
                        << " lattice_goal_position_error_m=" << lattice_goal_position_error
                        << " lattice_goal_yaw_error_rad=" << lattice_goal_yaw_error
                        << " full_continuous_footprint=false"
                        << " reason=\"quantized State terminal residual exceeded contract\""
                        << std::endl;
              continue;
            }

            smac_lattice_planner_mbf::theta_state_suffix::PosePath state_suffix;
            if (!coordinatesToPosePath(
                path, *costmap, cut.cut, frame, state_suffix))
            {
              std::cout << "COMPOSITE_ATTEMPT " << previous.id << "->" << next.id
                        << " cut_points=" << cut.effective_suffix_point_count
                        << " cut_index=" << cut.cut_index
                        << " prefix_safe=true suffix_started=true"
                        << " suffix_termination=" << searchTerminationName(result.termination)
                        << " suffix_seconds=" << suffix_seconds
                        << " suffix_expansions=" << suffix_iterations
                        << " full_continuous_footprint=false"
                        << " reason=\"empty converted State suffix\"" << std::endl;
              continue;
            }
            std::string state_contract_reason;
            if (!smac_lattice_planner_mbf::theta_state_suffix::
              containsKinematicallyContinuousForwardOrRotation(
                state_suffix, state_contract_reason) ||
              !smac_lattice_planner_mbf::theta_state_suffix::
              terminalAvoidsStationaryYawRepair(state_suffix, state_contract_reason))
            {
              std::cout << "COMPOSITE_ATTEMPT " << previous.id << "->" << next.id
                        << " cut_points=" << cut.effective_suffix_point_count
                        << " prefix_safe=true suffix_started=true"
                        << " suffix_termination=" << searchTerminationName(result.termination)
                        << " suffix_seconds=" << suffix_seconds
                        << " suffix_expansions=" << suffix_iterations
                        << " full_continuous_footprint=false"
                        << " reason=" << std::quoted(state_contract_reason) << std::endl;
              continue;
            }

            smac_lattice_planner_mbf::theta_state_suffix::PosePath candidate;
            try {
              candidate =
                smac_lattice_planner_mbf::theta_state_suffix::
                stitchThetaPrefixAndStateSuffix(
                cut.prefix_including_cut, state_suffix, 1e-6, 1e-6);
            } catch (const std::exception & error) {
              std::cout << "COMPOSITE_ATTEMPT " << previous.id << "->" << next.id
                        << " cut_points=" << cut.effective_suffix_point_count
                        << " cut_index=" << cut.cut_index
                        << " prefix_safe=true suffix_started=true"
                        << " suffix_termination=" << searchTerminationName(result.termination)
                        << " suffix_seconds=" << suffix_seconds
                        << " suffix_expansions=" << suffix_iterations
                        << " full_continuous_footprint=false"
                        << " reason=" << std::quoted(error.what()) << std::endl;
              continue;
            }

            std::string composite_reason;
            const auto composite_validation_started = SteadyClock::now();
            const bool composite_safe = validateContinuousPosePath(
              *costmap, checker, candidate, allow_unknown, composite_reason,
              &candidate_deadline);
            validation_seconds += elapsedSeconds(composite_validation_started);
            std::cout << "COMPOSITE_ATTEMPT " << previous.id << "->" << next.id
                      << " cut_points=" << cut.effective_suffix_point_count
                      << " cut_index=" << cut.cut_index
                      << " theta_prefix_poses=" << cut.prefix_including_cut.size()
                      << " state_suffix_poses=" << state_suffix.size()
                      << " prefix_safe=true suffix_started=true"
                      << " suffix_goal_heading_mode=" << suffix_goal_heading_mode_name
                      << " analytic_expansion_max_length_m=" <<
              analytic_expansion_max_length_m
                      << " allow_reverse_expansion=" <<
              (allow_reverse_expansion ? "true" : "false")
                      << " suffix_local_corridor_half_width_m=" <<
              suffix_local_corridor_half_width_m
                      << " suffix_termination=" << searchTerminationName(result.termination)
                      << " suffix_seconds=" << suffix_seconds
                      << " suffix_expansions=" << suffix_iterations
                      << " lattice_goal_dx_m=" << lattice_goal_dx
                      << " lattice_goal_dy_m=" << lattice_goal_dy
                      << " lattice_goal_position_error_m=" << lattice_goal_position_error
                      << " lattice_goal_signed_yaw_delta_rad=" <<
              lattice_goal_signed_yaw_delta
                      << " lattice_goal_yaw_error_rad=" << lattice_goal_yaw_error
                      << " terminal_append_kind=" << terminal_append_kind
                      << " mppi_terminal_phase=" << mppi_terminal_phase
                      << " full_continuous_footprint="
                      << (composite_safe ? "true" : "false");
            if (!composite_safe) {
              std::cout << " reason=" << std::quoted(composite_reason);
            }
            std::cout << std::endl;
            if (!composite_safe) {
              continue;
            }

            const auto raw_state_suffix = state_suffix;
            const auto raw_candidate = candidate;
            const std::string smoothing_mode = cut_is_short_route_all_state ?
              "theta_short_route_state_full_" +
              std::to_string(cut.effective_suffix_point_count) + "pt" :
              (cut_is_adaptive ?
              "theta_prefix_state_suffix_unsafe_segment_adaptive_" +
              std::to_string(cut.effective_suffix_point_count) + "pt" :
              "theta_prefix_state_suffix_" +
              std::to_string(cut.effective_suffix_point_count) + "pt");
            if (maybeSmoothStatePath(
                state_suffix, candidate_deadline, smoothing_mode))
            {
              try {
                auto smoothed_candidate =
                  smac_lattice_planner_mbf::theta_state_suffix::
                  stitchThetaPrefixAndStateSuffix(
                    cut.prefix_including_cut, state_suffix, 1e-6, 1e-6);
                std::string smoothed_reason;
                const auto smoothed_validation_started = SteadyClock::now();
                const bool smoothed_safe = validateContinuousPosePath(
                  *costmap, checker, smoothed_candidate, allow_unknown,
                  smoothed_reason, &candidate_deadline);
                validation_seconds += elapsedSeconds(smoothed_validation_started);
                if (smoothed_safe) {
                  candidate = std::move(smoothed_candidate);
                } else {
                  std::cout << "SMOOTHER_REJECT " << previous.id << "->" << next.id
                            << " mode=" << smoothing_mode
                            << " stage=complete_composite reason=" <<
                    std::quoted(smoothed_reason) << std::endl;
                  state_suffix = raw_state_suffix;
                  candidate = raw_candidate;
                }
              } catch (const std::exception & error) {
                std::cout << "SMOOTHER_REJECT " << previous.id << "->" << next.id
                          << " mode=" << smoothing_mode
                          << " stage=splice reason=" << std::quoted(error.what())
                          << std::endl;
                state_suffix = raw_state_suffix;
                candidate = raw_candidate;
              }
            }

            success = true;
            composite_success = true;
            composite_path = std::move(candidate);
            selected_state_path = path;
            selected_cut_points = cut.effective_suffix_point_count;
            selected_cut_index = cut.cut_index;
            selected_theta_prefix_poses = cut.prefix_including_cut.size();
            selected_state_suffix_poses = state_suffix.size();
            selected_state_quality = summarizePosePathQuality(state_suffix);
            selected_state_suffix_seconds = suffix_seconds;
            selected_state_suffix_expansions = suffix_iterations;
            selected_lattice_goal_dx = lattice_goal_dx;
            selected_lattice_goal_dy = lattice_goal_dy;
            selected_lattice_goal_signed_yaw_delta = lattice_goal_signed_yaw_delta;
            selected_lattice_goal_yaw_error = lattice_goal_yaw_error;
            selected_terminal_append_kind = terminal_append_kind;
            selected_mppi_terminal_phase = mppi_terminal_phase;
            search_mode = cut_is_short_route_all_state ?
              "theta_short_route_state_full_" +
              std::to_string(cut.effective_suffix_point_count) + "pt" :
              (cut_is_adaptive ?
              "theta_prefix_state_suffix_unsafe_segment_adaptive_" +
              std::to_string(cut.effective_suffix_point_count) + "pt" :
              "theta_prefix_state_suffix_" +
              std::to_string(cut.effective_suffix_point_count) + "pt");
            if (!diagnostic_evaluate_all_suffix_candidates) {
              break;
            }
          }
          if (!success && adaptive_unsafe_segment_fallback_requested) {
            suppress_full_state_fallback = true;
            search_mode = adaptive_unsafe_segment_fallback_attempted ?
              "UNSAFE_THETA_ADAPTIVE_SUFFIX_FAILED_FULL_SUPPRESSED" :
              (adaptive_unsafe_segment_fallback_scheduled ?
              "UNSAFE_THETA_ADAPTIVE_SUFFIX_DEADLINE_FULL_SUPPRESSED" :
              "UNSAFE_THETA_COLLISION_NEAR_START_FULL_SUPPRESSED");
          }
          if (!success && short_theta_reference) {
            suppress_full_state_fallback = true;
            search_mode = "SHORT_THETA_ROUTE_STATE_FAILED_FULL_SUPPRESSED";
          }
        }

        if (!success && !suppress_full_state_fallback &&
          theta_corridor_search_enabled && coarse_route.succeeded())
        {
          for (const auto & mask : coarse_route.routes.front().center_masks) {
            const double available = std::chrono::duration<double>(
              corridor_deadline - SteadyClock::now()).count();
            if (available < 0.05) {
              break;
            }
            int level_iterations = 0;
            double level_seconds = 0.0;
            const auto result = runStateSearch(
              start_x, start_y, safe_start_bins,
              &mask, &coarse_route.routes.front().route_progress,
              std::min(corridor_level_max_planning_time, available),
              nav2_smac_planner::GoalHeadingMode::DEFAULT,
              2.0f, path, level_iterations, level_seconds);
            iterations += level_iterations;
            exact_termination = result.termination;
            std::cout << "ATTEMPT " << previous.id << "->" << next.id
                      << " search_mode=theta_corridor_" << mask.halfWidthM() << "m"
                      << " termination=" << searchTerminationName(result.termination)
                      << " path=" << (result.hasPath() ? "yes" : "no")
                      << " expansions=" << level_iterations << std::endl;
            if (result.hasPath()) {
              success = true;
              search_mode = "theta_corridor_" + std::to_string(mask.halfWidthM()) + "m";
              break;
            }
          }
        }
        if (!success && !suppress_full_state_fallback) {
          const std::string full_search_mode = full_search_tolerance <= 1e-9 ?
            "FULL_EXACT" : "FULL_TOLERANCE_" + std::to_string(full_search_tolerance);
          const double available = std::chrono::duration<double>(
            search_deadline - SteadyClock::now()).count();
          if (available >= 0.05) {
            int full_iterations = 0;
            double full_seconds = 0.0;
            const auto result = runStateSearch(
              start_x, start_y, safe_start_bins,
              nullptr, nullptr, available,
              nav2_smac_planner::GoalHeadingMode::DEFAULT,
              static_cast<float>(full_search_tolerance),
              path, full_iterations, full_seconds);
            iterations += full_iterations;
            exact_termination = result.termination;
            std::cout << "ATTEMPT " << previous.id << "->" << next.id
                      << " search_mode=" << full_search_mode
                      << " termination=" << searchTerminationName(result.termination)
                      << " path=" << (result.hasPath() ? "yes" : "no")
                      << " seconds=" << full_seconds
                      << " expansions=" << full_iterations << std::endl;
            success =
              result.termination == nav2_smac_planner::SearchTermination::SUCCESS &&
              result.hasPath();
            search_mode = full_search_mode;
          } else {
            exact_termination = nav2_smac_planner::SearchTermination::TIMEOUT;
            search_mode = full_search_mode + "_not_started";
          }
        }

        std::string validation_reason;
        bool continuously_safe = false;
        if (success) {
          const auto validation_started = SteadyClock::now();
          if (composite_success) {
            continuously_safe = validateContinuousPosePath(
              *costmap, checker, composite_path, allow_unknown, validation_reason,
              &overall_deadline);
          } else {
            const std::string frame = message->header.frame_id.empty() ? "map" :
              message->header.frame_id;
            geometry_msgs::PoseStamped exact_start;
            exact_start.header.frame_id = frame;
            exact_start.pose.position.x = previous.exit_x;
            exact_start.pose.position.y = previous.exit_y;
            exact_start.pose.orientation =
              tf::createQuaternionMsgFromYaw(previous.exit_yaw);
            continuously_safe = coordinatesToPosePath(
              path, *costmap, exact_start, frame, fallback_pose_path);
            if (continuously_safe) {
              std::string state_contract_reason;
              continuously_safe =
                smac_lattice_planner_mbf::theta_state_suffix::
                containsKinematicallyContinuousForwardOrRotation(
                  fallback_pose_path, state_contract_reason) &&
                smac_lattice_planner_mbf::theta_state_suffix::
                terminalAvoidsStationaryYawRepair(
                  fallback_pose_path, state_contract_reason);
              if (!continuously_safe) {
                validation_reason = state_contract_reason;
              }
            }
            if (continuously_safe) {
              const double terminal_position_error = std::hypot(
                fallback_pose_path.back().pose.position.x - next.entry_x,
                fallback_pose_path.back().pose.position.y - next.entry_y);
              const double terminal_yaw_error = std::abs(
                angles::shortest_angular_distance(
                  tf::getYaw(fallback_pose_path.back().pose.orientation),
                  next.entry_yaw));
              continuously_safe =
                terminal_position_error <= max_lattice_position_residual &&
                terminal_yaw_error <= 0.20 &&
                terminal_yaw_error <= max_nearest_bin_yaw_residual;
              if (!continuously_safe) {
                validation_reason = "quantized State fallback terminal residual exceeded contract";
              }
            }
            if (continuously_safe) {
              (void)maybeSmoothStatePath(
                fallback_pose_path, search_deadline, search_mode);
            }
            if (continuously_safe) {
              continuously_safe = validateContinuousPosePath(
                *costmap, checker, fallback_pose_path, allow_unknown, validation_reason,
                &overall_deadline);
            } else {
              if (fallback_pose_path.empty()) {
                validation_reason = "State fallback conversion returned no poses";
              }
            }
          }
          validation_seconds += elapsedSeconds(validation_started);
        }
        const bool planning_snapshot_safe = continuously_safe;
        bool fresh_snapshot_checked = false;
        bool fresh_snapshot_safe = false;
        double fresh_snapshot_stamp_delta = 0.0;
        if (continuously_safe && fresh_snapshot_revalidation) {
          const auto fresh_validation_started = SteadyClock::now();
          const auto fresh_wait_deadline = std::min(
            overall_deadline,
            SteadyClock::now() + toClockDuration(fresh_snapshot_timeout));
          nav_msgs::OccupancyGridConstPtr fresh_message;
          while (ros::ok() && SteadyClock::now() < fresh_wait_deadline) {
            const double remaining = std::chrono::duration<double>(
              fresh_wait_deadline - SteadyClock::now()).count();
            const auto candidate = ros::topic::waitForMessage<nav_msgs::OccupancyGrid>(
              costmap_topic, ros::Duration(std::min(0.5, std::max(0.01, remaining))));
            if (candidate) {
              const bool newer_stamp = candidate->header.stamp > message->header.stamp;
              const bool same_stamp_newer_sequence =
                candidate->header.stamp == message->header.stamp &&
                candidate->header.seq > message->header.seq;
              if (newer_stamp || same_stamp_newer_sequence) {
                fresh_message = candidate;
                break;
              }
            }
            ros::WallDuration(0.01).sleep();
          }
          fresh_snapshot_checked = true;
          if (!fresh_message) {
            validation_reason =
              "timed out waiting for a strictly newer global costmap snapshot";
            continuously_safe = false;
          } else if (
            fresh_message->header.frame_id != message->header.frame_id ||
            std::abs(fresh_message->info.resolution - message->info.resolution) > 1e-6)
          {
            validation_reason = "fresh costmap frame or resolution differs from planning snapshot";
            continuously_safe = false;
          } else {
            fresh_snapshot_stamp_delta =
              (fresh_message->header.stamp - message->header.stamp).toSec();
            auto fresh_costmap = convertCostmap(*fresh_message);
            nav2_smac_planner::GridCollisionChecker fresh_checker(fresh_costmap.get(), 72u);
            fresh_checker.setFootprint(robotFootprint(), false, 0.0);
            fresh_checker.setCollisionCheckResolution(0.01);
            std::string fresh_reason;
            const auto & validated_path = composite_success ?
              composite_path : fallback_pose_path;
            fresh_snapshot_safe = validateContinuousPosePath(
              *fresh_costmap, fresh_checker, validated_path, allow_unknown, fresh_reason,
              &overall_deadline);
            continuously_safe = fresh_snapshot_safe;
            if (!fresh_snapshot_safe) {
              validation_reason = "fresh snapshot: " + fresh_reason;
            }
          }
          validation_seconds += elapsedSeconds(fresh_validation_started);
        }
        const std::string termination = continuously_safe ?
          searchTerminationName(exact_termination) :
          (success ? "continuous_validation_failed" : searchTerminationName(exact_termination));
        if (continuously_safe) {
          if (composite_success) {
            ++composite_passes;
          } else {
            ++fallback_passes;
          }
          const auto motion = summarizePathMotion(
            composite_success ? selected_state_path : path,
            costmap->getResolution());
          const std::size_t validated_pose_count = composite_success ?
            composite_path.size() : fallback_pose_path.size();
          const double terminal_position_error = composite_success ?
            std::hypot(
              composite_path.back().pose.position.x - next.entry_x,
              composite_path.back().pose.position.y - next.entry_y) :
            std::hypot(
              fallback_pose_path.back().pose.position.x - next.entry_x,
              fallback_pose_path.back().pose.position.y - next.entry_y);
          const double terminal_yaw = composite_success ?
            tf::getYaw(composite_path.back().pose.orientation) :
            tf::getYaw(fallback_pose_path.back().pose.orientation);
          const double terminal_yaw_error = std::abs(
            angles::shortest_angular_distance(terminal_yaw, next.entry_yaw));
          std::cout << "PASS " << previous.id << "->" << next.id
                    << " mode=" << selection.mode
                    << " search_mode=" << search_mode
                    << " termination=" << termination
                    << " poses=" << validated_pose_count
                    << " expansions=" << iterations
                    << " seeds=" << safe_start_bins.size()
                    << " theta_length_m=" << theta_length_m
                    << " theta_reference_points=" << theta_reference_points
                    << " cut_points=" << selected_cut_points
                    << " cut_index=" << selected_cut_index
                    << " theta_prefix_poses=" << selected_theta_prefix_poses
                    << " state_suffix_poses=" << selected_state_suffix_poses
                    << " state_suffix_length_m=" << selected_state_quality.length_m
                    << " state_suffix_abs_yaw_change_rad=" <<
              selected_state_quality.absolute_yaw_change_rad
                    << " state_suffix_translating_abs_yaw_change_rad=" <<
              selected_state_quality.translating_absolute_yaw_change_rad
                    << " state_suffix_mean_abs_curvature_radpm=" <<
              selected_state_quality.mean_absolute_curvature_radpm
                    << " state_suffix_length_weighted_mean_abs_curvature_radpm=" <<
              selected_state_quality.length_weighted_mean_absolute_curvature_radpm
                    << " state_suffix_p95_abs_curvature_radpm=" <<
              selected_state_quality.p95_absolute_curvature_radpm
                    << " state_suffix_max_abs_curvature_radpm=" <<
              selected_state_quality.max_absolute_curvature_radpm
                    << " state_suffix_curvature_total_variation_radpm=" <<
              selected_state_quality.curvature_total_variation_radpm
                    << " state_suffix_max_curvature_jump_radpm=" <<
              selected_state_quality.max_curvature_jump_radpm
                    << " state_suffix_curvature_direction_changes=" <<
              selected_state_quality.curvature_direction_changes
                    << " state_suffix_in_place_rotations=" <<
              selected_state_quality.in_place_rotations
                    << " state_suffix_seconds=" << selected_state_suffix_seconds
                    << " state_suffix_expansions=" << selected_state_suffix_expansions
                    << " state_suffix_attempts=" << state_suffix_attempts
                    << " state_suffix_total_seconds=" << state_suffix_total_seconds
                    << " state_suffix_total_expansions=" << state_suffix_total_expansions
                    << " suffix_goal_heading_mode=" << suffix_goal_heading_mode_name
                    << " analytic_expansion_max_length_m=" <<
              analytic_expansion_max_length_m
                    << " allow_reverse_expansion=" <<
              (allow_reverse_expansion ? "true" : "false")
                    << " lattice_goal_dx_m=" << selected_lattice_goal_dx
                    << " lattice_goal_dy_m=" << selected_lattice_goal_dy
                    << " lattice_goal_signed_yaw_delta_rad=" <<
              selected_lattice_goal_signed_yaw_delta
                    << " lattice_goal_yaw_error_rad=" << selected_lattice_goal_yaw_error
                    << " terminal_append_kind=" << selected_terminal_append_kind
                    << " mppi_terminal_phase=" << selected_mppi_terminal_phase
                    << " planning_snapshot_footprint=" <<
              (planning_snapshot_safe ? "pass" : "fail")
                    << " fresh_snapshot_footprint=" <<
              (fresh_snapshot_checked ? (fresh_snapshot_safe ? "pass" : "fail") : "not_checked")
                    << " fresh_snapshot_stamp_delta=" << fresh_snapshot_stamp_delta
                    << " full_continuous_footprint=pass"
                    << " coarse_seconds=" << coarse_seconds
                    << " search_seconds=" << search_seconds
                    << " validation_seconds=" << validation_seconds
                    << " case_end_to_end_seconds=" << elapsedSeconds(case_started)
                    << " end_to_end_seconds=" << elapsedSeconds(run_started)
                    << " terminal_position_error_m=" << terminal_position_error
                    << " terminal_yaw_error_rad=" << terminal_yaw_error
                    << " forward_m=" << motion.forward_distance
                    << " reverse_m=" << motion.reverse_distance
                    << " direction_changes=" << motion.direction_changes
                    << std::endl;
        } else {
          std::cout << "FAIL " << previous.id << "->" << next.id
                    << " mode=" << selection.mode
                    << " search_mode=" << search_mode
                    << " termination=" << termination
                    << " expansions=" << iterations
                    << " theta_length_m=" << theta_length_m
                    << " theta_reference_points=" << theta_reference_points
                    << " cut_points=" << selected_cut_points
                    << " state_suffix_seconds=" << selected_state_suffix_seconds
                    << " state_suffix_expansions=" << selected_state_suffix_expansions
                    << " state_suffix_attempts=" << state_suffix_attempts
                    << " state_suffix_total_seconds=" << state_suffix_total_seconds
                    << " state_suffix_total_expansions=" << state_suffix_total_expansions
                    << " suffix_goal_heading_mode=" << suffix_goal_heading_mode_name
                    << " planning_snapshot_footprint=" <<
              (planning_snapshot_safe ? "pass" : "fail")
                    << " fresh_snapshot_footprint=" <<
              (fresh_snapshot_checked ? (fresh_snapshot_safe ? "pass" : "fail") : "not_checked")
                    << " fresh_snapshot_stamp_delta=" << fresh_snapshot_stamp_delta
                    << " full_continuous_footprint=fail"
                    << " coarse_seconds=" << coarse_seconds
                    << " search_seconds=" << search_seconds
                    << " validation_seconds=" << validation_seconds
                    << " case_end_to_end_seconds=" << elapsedSeconds(case_started)
                    << " end_to_end_seconds=" << elapsedSeconds(run_started);
          if (success) {
            std::cout << " reason=" << std::quoted(validation_reason);
          }
          std::cout << std::endl;
          ++failures;
        }
      } catch (const std::exception & error) {
        std::cout << "FAIL " << previous.id << "->" << next.id
                  << " mode=" << selection.mode
                  << " termination=exception"
                  << " expansions=" << iterations
                  << " search_seconds=" << search_seconds
                  << " validation_seconds=" << validation_seconds
                  << " case_end_to_end_seconds=" << elapsedSeconds(case_started)
                  << " end_to_end_seconds=" << elapsedSeconds(run_started)
                  << " exception=" << std::quoted(error.what()) << std::endl;
        ++failures;
      }
    }
    std::cout << "SUMMARY plan=" << plan_id
              << " mode=" << selection.mode
              << " connections=" << tested_connections
              << " failures=" << failures
              << " composite_passes=" << composite_passes
              << " fallback_passes=" << fallback_passes
              << " setup_seconds=" << setup_seconds
              << " end_to_end_seconds=" << elapsedSeconds(run_started)
              << std::endl;
    return failures == 0 ? 0 : 2;
  } catch (const std::exception & error) {
    std::cerr << "SUMMARY plan=" << (plan_id.empty() ? "<latest>" : plan_id)
              << " mode=setup_error connections=0 failures=1"
              << " termination=setup_error"
              << " end_to_end_seconds=" << elapsedSeconds(run_started)
              << " reason=" << std::quoted(error.what()) << std::endl;
    ROS_ERROR(
      "Live block validation failed after %.6f s: %s",
      elapsedSeconds(run_started), error.what());
    return 1;
  }
}

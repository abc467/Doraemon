// Copyright (c) 2021, Samsung Research America
// Copyright 2026 Clean Robot Navigation Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "smac_lattice_planner_mbf/state_lattice_smoother.hpp"

#include <algorithm>
#include <array>
#include <cmath>
#include <limits>
#include <memory>
#include <stdexcept>
#include <utility>
#include <vector>

#include <angles/angles.h>
#include <costmap_2d/cost_values.h>
#include <ompl/base/ScopedState.h>
#include <ompl/base/spaces/DubinsStateSpace.h>
#include <tf2/utils.h>

namespace smac_lattice_planner_mbf
{
namespace
{
using PosePath = theta_state_suffix::PosePath;
using Clock = StateLatticeSmoother::Clock;

constexpr double kTranslationEpsilon = 1e-4;
// A curvature sign flip is only a visible S-turn when both sides contain a
// meaningful turn and they are not separated by a sustained straight.  The
// previous implementation remembered the last sign across arbitrarily long
// straight runs and counted a single noisy opposite-curvature sample as a new
// direction change.  That made otherwise smoother paths fail the atomic
// quality gate.
constexpr double kCurvatureDirectionThresholdRadpm = 0.5;
constexpr double kCurvatureStraightResetDistanceM = 0.25;
constexpr double kMinimumMaterialTurnYawRad = 0.05;

struct PathSegment
{
  std::size_t start{0u};
  std::size_t end{0u};
};

struct BoundaryPoint
{
  double x{0.0};
  double y{0.0};
  double yaw{0.0};
};

struct BoundaryExpansion
{
  std::size_t path_end_index{0u};
  double original_path_length{0.0};
  double expansion_path_length{0.0};
  bool in_collision{true};
  std::vector<BoundaryPoint> points;
};

geometry_msgs::Quaternion quaternionFromYaw(double yaw)
{
  geometry_msgs::Quaternion quaternion;
  quaternion.x = 0.0;
  quaternion.y = 0.0;
  quaternion.z = std::sin(0.5 * yaw);
  quaternion.w = std::cos(0.5 * yaw);
  return quaternion;
}

bool worldToMapContinuous(
  const costmap_2d::Costmap2D & costmap,
  double world_x,
  double world_y,
  float & map_x,
  float & map_y)
{
  const double x = (world_x - costmap.getOriginX()) / costmap.getResolution();
  const double y = (world_y - costmap.getOriginY()) / costmap.getResolution();
  if (!std::isfinite(x) || !std::isfinite(y) || x < 0.0 || y < 0.0 ||
    x >= static_cast<double>(costmap.getSizeInCellsX()) ||
    y >= static_cast<double>(costmap.getSizeInCellsY()))
  {
    return false;
  }
  map_x = static_cast<float>(x);
  map_y = static_cast<float>(y);
  return true;
}

std::vector<PathSegment> findDirectionalPathSegments(const PosePath & path)
{
  std::vector<PathSegment> segments;
  if (path.empty()) {
    return segments;
  }
  if (path.size() < 3u) {
    segments.push_back(PathSegment{0u, path.size() - 1u});
    return segments;
  }

  PathSegment current{0u, 0u};
  for (std::size_t index = 1u; index + 1u < path.size(); ++index) {
    const double incoming_x =
      path[index].pose.position.x - path[index - 1u].pose.position.x;
    const double incoming_y =
      path[index].pose.position.y - path[index - 1u].pose.position.y;
    const double outgoing_x =
      path[index + 1u].pose.position.x - path[index].pose.position.x;
    const double outgoing_y =
      path[index + 1u].pose.position.y - path[index].pose.position.y;
    const double dot_product = incoming_x * outgoing_x + incoming_y * outgoing_y;
    const double yaw = tf2::getYaw(path[index].pose.orientation);
    const double next_yaw = tf2::getYaw(path[index + 1u].pose.orientation);
    const double yaw_change = angles::shortest_angular_distance(yaw, next_yaw);
    const bool cusp = dot_product < 0.0;
    const bool in_place_rotation =
      std::abs(outgoing_x) < kTranslationEpsilon &&
      std::abs(outgoing_y) < kTranslationEpsilon &&
      std::abs(yaw_change) > kTranslationEpsilon;
    if (cusp || in_place_rotation) {
      current.end = index;
      if (current.end > current.start) {
        segments.push_back(current);
      }
      current.start = index;
    }
  }
  current.end = path.size() - 1u;
  if (current.end > current.start) {
    segments.push_back(current);
  }
  return segments;
}

bool updateApproximateOrientations(PosePath & path, bool & reversing_segment)
{
  if (path.size() < 3u) {
    return false;
  }
  const double direction_x =
    path[2].pose.position.x - path[1].pose.position.x;
  const double direction_y =
    path[2].pose.position.y - path[1].pose.position.y;
  if (std::hypot(direction_x, direction_y) <= kTranslationEpsilon) {
    return false;
  }
  const double direction_yaw = std::atan2(direction_y, direction_x);
  const double reference_yaw = tf2::getYaw(path[1].pose.orientation);
  reversing_segment =
    std::abs(angles::shortest_angular_distance(reference_yaw, direction_yaw)) > M_PI_2;

  for (std::size_t index = 0u; index + 1u < path.size(); ++index) {
    const double dx =
      path[index + 1u].pose.position.x - path[index].pose.position.x;
    const double dy =
      path[index + 1u].pose.position.y - path[index].pose.position.y;
    if (std::abs(dx) < kTranslationEpsilon && std::abs(dy) < kTranslationEpsilon) {
      continue;
    }
    double yaw = std::atan2(dy, dx);
    if (reversing_segment) {
      yaw += M_PI;
    }
    path[index].pose.orientation = quaternionFromYaw(yaw);
  }
  return true;
}

bool canceled(const StateLatticeSmoother::CancelChecker & cancel_checker)
{
  return cancel_checker && cancel_checker();
}

bool poseIsCollisionFree(
  const geometry_msgs::PoseStamped & pose,
  const costmap_2d::Costmap2D & costmap,
  nav2_smac_planner::GridCollisionChecker & collision_checker,
  bool allow_unknown)
{
  float map_x = 0.0f;
  float map_y = 0.0f;
  if (!worldToMapContinuous(
      costmap, pose.pose.position.x, pose.pose.position.y, map_x, map_y))
  {
    return false;
  }
  return !collision_checker.inCollisionAtYaw(
    map_x, map_y, tf2::getYaw(pose.pose.orientation), allow_unknown);
}

bool pathPosesAreCollisionFree(
  const PosePath & path,
  const costmap_2d::Costmap2D & costmap,
  nav2_smac_planner::GridCollisionChecker & collision_checker,
  bool allow_unknown,
  const Clock::time_point & deadline,
  const StateLatticeSmoother::CancelChecker & cancel_checker)
{
  for (const auto & pose : path) {
    if (Clock::now() >= deadline || canceled(cancel_checker) ||
      !poseIsCollisionFree(pose, costmap, collision_checker, allow_unknown))
    {
      return false;
    }
  }
  return true;
}

std::vector<BoundaryExpansion> generateBoundaryExpansionPoints(
  const PosePath & path,
  double minimum_turning_radius,
  bool from_start)
{
  const std::array<double, 4u> distances{{
      minimum_turning_radius,
      2.0 * minimum_turning_radius,
      M_PI * minimum_turning_radius,
      2.0 * M_PI * minimum_turning_radius}};
  std::vector<BoundaryExpansion> expansions(distances.size());
  if (path.size() < 2u) {
    return expansions;
  }

  double accumulated = 0.0;
  std::size_t distance_index = 0u;
  for (std::size_t offset = 1u; offset < path.size() && distance_index < distances.size();
    ++offset)
  {
    const std::size_t previous_index = from_start ? offset - 1u : path.size() - offset;
    const std::size_t current_index = from_start ? offset : path.size() - offset - 1u;
    accumulated += std::hypot(
      path[current_index].pose.position.x - path[previous_index].pose.position.x,
      path[current_index].pose.position.y - path[previous_index].pose.position.y);
    while (distance_index < distances.size() && accumulated >= distances[distance_index]) {
      expansions[distance_index].path_end_index = offset;
      expansions[distance_index].original_path_length = accumulated;
      ++distance_index;
    }
  }
  return expansions;
}

void evaluateBoundaryExpansion(
  const geometry_msgs::Pose & start,
  const geometry_msgs::Pose & end,
  BoundaryExpansion & expansion,
  const ompl::base::StateSpacePtr & state_space,
  const costmap_2d::Costmap2D & costmap,
  nav2_smac_planner::GridCollisionChecker & collision_checker,
  bool allow_unknown,
  const Clock::time_point & deadline,
  const StateLatticeSmoother::CancelChecker & cancel_checker)
{
  if (expansion.path_end_index == 0u || expansion.original_path_length <= 0.0) {
    return;
  }

  ompl::base::ScopedState<> from(state_space);
  ompl::base::ScopedState<> to(state_space);
  ompl::base::ScopedState<> sample(state_space);
  from[0] = start.position.x;
  from[1] = start.position.y;
  from[2] = tf2::getYaw(start.orientation);
  to[0] = end.position.x;
  to[1] = end.position.y;
  to[2] = tf2::getYaw(end.orientation);

  const double dubins_length = state_space->distance(from(), to());
  if (!std::isfinite(dubins_length) ||
    dubins_length > 2.0 * expansion.original_path_length)
  {
    return;
  }

  expansion.points.clear();
  expansion.points.reserve(expansion.path_end_index + 1u);
  expansion.expansion_path_length = 0.0;
  expansion.in_collision = false;
  float previous_map_x = 0.0f;
  float previous_map_y = 0.0f;
  double previous_yaw = tf2::getYaw(start.orientation);
  bool have_previous = false;
  double previous_x = start.position.x;
  double previous_y = start.position.y;
  for (std::size_t index = 0u; index <= expansion.path_end_index; ++index) {
    if (Clock::now() >= deadline || canceled(cancel_checker)) {
      expansion.in_collision = true;
      expansion.points.clear();
      return;
    }
    const double fraction = static_cast<double>(index) /
      static_cast<double>(expansion.path_end_index);
    state_space->interpolate(from(), to(), fraction, sample());
    const auto values = sample.reals();
    const double x = values[0];
    const double y = values[1];
    const double yaw = angles::normalize_angle(values[2]);
    float map_x = 0.0f;
    float map_y = 0.0f;
    if (!worldToMapContinuous(costmap, x, y, map_x, map_y) ||
      collision_checker.inCollisionAtYaw(map_x, map_y, yaw, allow_unknown) ||
      (have_previous && collision_checker.inCollisionContinuous(
        previous_map_x, previous_map_y, previous_yaw,
        map_x, map_y, yaw, allow_unknown)))
    {
      expansion.in_collision = true;
      expansion.points.clear();
      return;
    }
    expansion.expansion_path_length += std::hypot(x - previous_x, y - previous_y);
    previous_x = x;
    previous_y = y;
    previous_map_x = map_x;
    previous_map_y = map_y;
    previous_yaw = yaw;
    have_previous = true;
    expansion.points.push_back(BoundaryPoint{x, y, yaw});
  }
}

const BoundaryExpansion * shortestValidExpansion(
  const std::vector<BoundaryExpansion> & expansions)
{
  const BoundaryExpansion * best = nullptr;
  for (const auto & expansion : expansions) {
    if (expansion.in_collision || expansion.path_end_index == 0u ||
      expansion.expansion_path_length <= 0.0 || expansion.points.empty())
    {
      continue;
    }
    if (best == nullptr ||
      expansion.expansion_path_length < best->expansion_path_length)
    {
      best = &expansion;
    }
  }
  return best;
}

void enforceStartBoundaryConditions(
  const geometry_msgs::Pose & exact_start,
  PosePath & path,
  double minimum_turning_radius,
  const ompl::base::StateSpacePtr & state_space,
  const costmap_2d::Costmap2D & costmap,
  nav2_smac_planner::GridCollisionChecker & collision_checker,
  bool allow_unknown,
  bool reversing,
  const Clock::time_point & deadline,
  const StateLatticeSmoother::CancelChecker & cancel_checker)
{
  auto expansions = generateBoundaryExpansionPoints(path, minimum_turning_radius, true);
  for (auto & expansion : expansions) {
    if (expansion.path_end_index == 0u) {
      continue;
    }
    const auto & endpoint = path[expansion.path_end_index].pose;
    evaluateBoundaryExpansion(
      reversing ? endpoint : exact_start,
      reversing ? exact_start : endpoint,
      expansion, state_space, costmap, collision_checker, allow_unknown,
      deadline, cancel_checker);
    if (reversing && !expansion.points.empty()) {
      std::reverse(expansion.points.begin(), expansion.points.end());
    }
  }
  const auto * best = shortestValidExpansion(expansions);
  if (best == nullptr || best->points.size() > path.size()) {
    return;
  }
  for (std::size_t index = 0u; index < best->points.size(); ++index) {
    path[index].pose.position.x = best->points[index].x;
    path[index].pose.position.y = best->points[index].y;
    path[index].pose.orientation = quaternionFromYaw(best->points[index].yaw);
  }
}

void enforceEndBoundaryConditions(
  const geometry_msgs::Pose & exact_end,
  PosePath & path,
  double minimum_turning_radius,
  const ompl::base::StateSpacePtr & state_space,
  const costmap_2d::Costmap2D & costmap,
  nav2_smac_planner::GridCollisionChecker & collision_checker,
  bool allow_unknown,
  bool reversing,
  const Clock::time_point & deadline,
  const StateLatticeSmoother::CancelChecker & cancel_checker)
{
  auto expansions = generateBoundaryExpansionPoints(path, minimum_turning_radius, false);
  for (auto & expansion : expansions) {
    if (expansion.path_end_index == 0u || expansion.path_end_index >= path.size()) {
      continue;
    }
    const std::size_t start_index = path.size() - expansion.path_end_index - 1u;
    const auto & start_pose = path[start_index].pose;
    evaluateBoundaryExpansion(
      reversing ? exact_end : start_pose,
      reversing ? start_pose : exact_end,
      expansion, state_space, costmap, collision_checker, allow_unknown,
      deadline, cancel_checker);
    if (reversing && !expansion.points.empty()) {
      std::reverse(expansion.points.begin(), expansion.points.end());
    }
  }
  const auto * best = shortestValidExpansion(expansions);
  if (best == nullptr || best->path_end_index >= path.size() ||
    best->points.size() != best->path_end_index + 1u)
  {
    return;
  }
  const std::size_t start_index = path.size() - best->path_end_index - 1u;
  for (std::size_t offset = 0u; offset < best->points.size(); ++offset) {
    path[start_index + offset].pose.position.x = best->points[offset].x;
    path[start_index + offset].pose.position.y = best->points[offset].y;
    path[start_index + offset].pose.orientation = quaternionFromYaw(best->points[offset].yaw);
  }
}

bool smoothSegmentOnce(
  PosePath & segment,
  const StateLatticeSmootherParams & params,
  const costmap_2d::Costmap2D & costmap,
  nav2_smac_planner::GridCollisionChecker & collision_checker,
  bool allow_unknown,
  const Clock::time_point & deadline,
  const StateLatticeSmoother::CancelChecker & cancel_checker,
  int & total_iterations,
  std::string & reason)
{
  if (segment.size() < 3u) {
    reason = "State segment has fewer than three poses";
    return false;
  }
  const PosePath source = segment;
  PosePath working = segment;
  double change = params.tolerance;
  int pass_iterations = 0;
  while (change >= params.tolerance) {
    if (Clock::now() >= deadline) {
      reason = "State smoothing exceeded its absolute deadline";
      return false;
    }
    if (canceled(cancel_checker)) {
      reason = "State smoothing canceled";
      return false;
    }
    ++pass_iterations;
    ++total_iterations;
    if (pass_iterations >= params.max_iterations) {
      reason = "State smoothing reached max_iterations before convergence";
      return false;
    }
    change = 0.0;
    for (std::size_t index = 1u; index + 1u < working.size(); ++index) {
      double & x = working[index].pose.position.x;
      double & y = working[index].pose.position.y;
      const double previous_x = x;
      const double previous_y = y;
      x += params.w_data * (source[index].pose.position.x - x) +
        params.w_smooth * (
        working[index + 1u].pose.position.x + working[index - 1u].pose.position.x -
        2.0 * x);
      y += params.w_data * (source[index].pose.position.y - y) +
        params.w_smooth * (
        working[index + 1u].pose.position.y + working[index - 1u].pose.position.y -
        2.0 * y);
      change += std::abs(x - previous_x) + std::abs(y - previous_y);
    }
    bool reversing = false;
    if (!updateApproximateOrientations(working, reversing)) {
      reason = "State segment cannot reconstruct a translating orientation";
      return false;
    }
    if (!pathPosesAreCollisionFree(
        working, costmap, collision_checker, allow_unknown, deadline, cancel_checker))
    {
      reason = "State smoothing produced an infeasible footprint pose";
      return false;
    }
  }
  segment = std::move(working);
  return true;
}

bool validateParameters(const StateLatticeSmootherParams & params)
{
  return params.max_iterations > 1 && params.refinement_num >= 0 &&
         std::isfinite(params.w_data) && params.w_data >= 0.0 &&
         std::isfinite(params.w_smooth) && params.w_smooth >= 0.0 &&
         std::isfinite(params.tolerance) && params.tolerance > 0.0 &&
         std::isfinite(params.max_time) && params.max_time > 0.0 &&
         std::isfinite(params.max_path_length_ratio) &&
         params.max_path_length_ratio >= 1.0 &&
         std::isfinite(params.max_center_cost_increase) &&
         params.max_center_cost_increase >= 0.0 &&
         std::isfinite(params.max_mean_center_cost_increase) &&
         params.max_mean_center_cost_increase >= 0.0 &&
         std::isfinite(params.max_curvature_regression_ratio) &&
         params.max_curvature_regression_ratio >= 1.0 &&
         std::isfinite(params.minimum_curvature_improvement) &&
         params.minimum_curvature_improvement >= 0.0;
}

bool qualityGateAccepts(
  const StatePathQuality & raw,
  const StatePathQuality & candidate,
  const StateLatticeSmootherParams & params,
  double minimum_turning_radius_m,
  std::string & reason)
{
  const double numerical_epsilon = 1e-6;
  if (candidate.length_m > raw.length_m * params.max_path_length_ratio + numerical_epsilon) {
    reason = "smoothed path length exceeded the acceptance ratio";
    return false;
  }
  if (candidate.max_center_cost >
    raw.max_center_cost + params.max_center_cost_increase + numerical_epsilon)
  {
    reason = "smoothed path increased maximum center cost";
    return false;
  }
  if (candidate.mean_center_cost >
    raw.mean_center_cost + params.max_mean_center_cost_increase + numerical_epsilon)
  {
    reason = "smoothed path increased mean center cost";
    return false;
  }
  if (candidate.in_place_rotations > raw.in_place_rotations) {
    reason = "smoothed path introduced an additional in-place rotation";
    return false;
  }
  if (candidate.curvature_direction_changes > raw.curvature_direction_changes) {
    reason = "smoothed path introduced an additional curvature direction change";
    return false;
  }
  // Translating curvature metrics intentionally reset across an in-place
  // rotation because ds is zero there. Comparing those raw values directly
  // against a candidate that replaced F-R-F with a finite-radius forward arc
  // unfairly makes the safer, trackable arc appear less continuous. Model an
  // in-place rotation as the limiting 0 -> 1/r -> 0 curvature transition. This
  // gives both paths the same physical continuity accounting without granting
  // credit for rotations the candidate did not actually remove.
  const double equivalent_rotation_variation = 2.0 / minimum_turning_radius_m;
  const double equivalent_rotation_jump = 1.0 / minimum_turning_radius_m;
  const double raw_effective_total_variation =
    raw.curvature_total_variation_radpm +
    static_cast<double>(raw.in_place_rotations) * equivalent_rotation_variation;
  const double candidate_effective_total_variation =
    candidate.curvature_total_variation_radpm +
    static_cast<double>(candidate.in_place_rotations) * equivalent_rotation_variation;
  const double raw_effective_max_jump = std::max(
    raw.max_curvature_jump_radpm,
    raw.in_place_rotations > 0 ? equivalent_rotation_jump : 0.0);
  const double candidate_effective_max_jump = std::max(
    candidate.max_curvature_jump_radpm,
    candidate.in_place_rotations > 0 ? equivalent_rotation_jump : 0.0);
  const double allowed_total_variation =
    raw_effective_total_variation * params.max_curvature_regression_ratio +
    numerical_epsilon;
  const double allowed_max_jump =
    raw_effective_max_jump * params.max_curvature_regression_ratio +
    numerical_epsilon;
  if (candidate_effective_total_variation > allowed_total_variation ||
    candidate_effective_max_jump > allowed_max_jump)
  {
    reason = "smoothed path regressed a curvature continuity bound";
    return false;
  }
  const double improvement = std::max({
      raw_effective_total_variation - candidate_effective_total_variation,
      raw_effective_max_jump - candidate_effective_max_jump,
      raw.p95_abs_curvature_radpm - candidate.p95_abs_curvature_radpm,
      static_cast<double>(raw.in_place_rotations - candidate.in_place_rotations) *
      equivalent_rotation_jump});
  if (improvement < params.minimum_curvature_improvement) {
    reason = "smoothed path had no measurable curvature improvement";
    return false;
  }
  return true;
}

}  // namespace

StateLatticeSmoother::StateLatticeSmoother(
  StateLatticeSmootherParams params,
  double minimum_turning_radius_m)
: params_(std::move(params)),
  minimum_turning_radius_m_(minimum_turning_radius_m)
{
  if (!validateParameters(params_)) {
    throw std::invalid_argument("invalid State Lattice smoother parameters");
  }
  if (!std::isfinite(minimum_turning_radius_m_) || minimum_turning_radius_m_ <= 0.0) {
    throw std::invalid_argument("minimum turning radius must be finite and positive");
  }
}

StateLatticeSmoothingResult StateLatticeSmoother::smooth(
  const PosePath & raw_path,
  const costmap_2d::Costmap2D & costmap,
  nav2_smac_planner::GridCollisionChecker & collision_checker,
  bool allow_unknown,
  const Clock::time_point & absolute_deadline,
  CancelChecker cancel_checker) const
{
  StateLatticeSmoothingResult result;
  result.path = raw_path;
  if (raw_path.size() < 12u) {
    result.reason = "State path has no directional segment longer than 10 intervals";
    return result;
  }
  const auto local_duration = std::chrono::duration_cast<Clock::duration>(
    std::chrono::duration<double>(params_.max_time));
  const auto deadline = std::min(absolute_deadline, Clock::now() + local_duration);
  if (Clock::now() >= deadline || canceled(cancel_checker)) {
    result.reason = canceled(cancel_checker) ?
      "State smoothing canceled" : "State smoothing has no remaining time";
    return result;
  }

  result.raw_quality = summarizeQuality(raw_path, costmap);
  PosePath candidate = raw_path;
  const auto segments = findDirectionalPathSegments(raw_path);
  bool smoothed_any_segment = false;
  ompl::base::StateSpacePtr state_space = std::make_shared<ompl::base::DubinsStateSpace>(
    minimum_turning_radius_m_);
  for (const auto & indices : segments) {
    if (indices.end <= indices.start || indices.end - indices.start <= 10u) {
      continue;
    }
    PosePath segment(
      candidate.begin() + static_cast<std::ptrdiff_t>(indices.start),
      candidate.begin() + static_cast<std::ptrdiff_t>(indices.end + 1u));
    const geometry_msgs::Pose exact_start = segment.front().pose;
    const geometry_msgs::Pose exact_end = segment.back().pose;
    const int passes = 1 + (params_.do_refinement ? params_.refinement_num : 0);
    for (int pass = 0; pass < passes; ++pass) {
      if (!smoothSegmentOnce(
          segment, params_, costmap, collision_checker, allow_unknown,
          deadline, cancel_checker, result.iterations, result.reason))
      {
        return result;
      }
    }

    bool reversing = false;
    if (!updateApproximateOrientations(segment, reversing)) {
      result.reason = "State segment orientation reconstruction failed after refinement";
      return result;
    }
    enforceStartBoundaryConditions(
      exact_start, segment, minimum_turning_radius_m_, state_space,
      costmap, collision_checker, allow_unknown, reversing, deadline, cancel_checker);
    enforceEndBoundaryConditions(
      exact_end, segment, minimum_turning_radius_m_, state_space,
      costmap, collision_checker, allow_unknown, reversing, deadline, cancel_checker);
    // The exact State segment endpoints are a hard splice/goal contract even
    // if no admissible Dubins boundary replacement was found.
    segment.front().pose = exact_start;
    segment.back().pose = exact_end;
    if (!pathPosesAreCollisionFree(
        segment, costmap, collision_checker, allow_unknown, deadline, cancel_checker))
    {
      result.reason = "State boundary enforcement produced an infeasible footprint pose";
      return result;
    }
    std::copy(
      segment.begin(), segment.end(),
      candidate.begin() + static_cast<std::ptrdiff_t>(indices.start));
    smoothed_any_segment = true;
  }

  if (!smoothed_any_segment) {
    result.reason = "State path has no directional segment eligible for smoothing";
    return result;
  }
  if (!theta_state_suffix::sameSE2(raw_path.front(), candidate.front(), 1e-8, 1e-8) ||
    !theta_state_suffix::sameSE2(raw_path.back(), candidate.back(), 1e-8, 1e-8))
  {
    result.reason = "State smoother changed an exact endpoint";
    return result;
  }
  result.candidate_quality = summarizeQuality(candidate, costmap);
  if (!qualityGateAccepts(
      result.raw_quality, result.candidate_quality, params_,
      minimum_turning_radius_m_, result.reason))
  {
    result.rejected_candidate_path = std::move(candidate);
    return result;
  }
  result.accepted = true;
  result.path = std::move(candidate);
  result.reason = "accepted";
  return result;
}

StatePathQuality StateLatticeSmoother::summarizeQuality(
  const PosePath & path,
  const costmap_2d::Costmap2D & costmap)
{
  StatePathQuality quality;
  std::vector<double> absolute_curvatures;
  double previous_curvature = 0.0;
  bool have_previous_curvature = false;
  int previous_material_curvature_direction = 0;
  int active_curvature_direction = 0;
  double active_absolute_yaw_change = 0.0;
  double low_curvature_distance = 0.0;
  double center_cost_sum = 0.0;
  std::size_t center_cost_samples = 0u;
  const auto commit_material_turn = [&]() {
      if (active_curvature_direction != 0 &&
        active_absolute_yaw_change >= kMinimumMaterialTurnYawRad)
      {
        if (previous_material_curvature_direction != 0 &&
          active_curvature_direction != previous_material_curvature_direction)
        {
          ++quality.curvature_direction_changes;
        }
        previous_material_curvature_direction = active_curvature_direction;
      }
      active_curvature_direction = 0;
      active_absolute_yaw_change = 0.0;
    };
  const auto reset_material_turn_chain = [&]() {
      commit_material_turn();
      previous_material_curvature_direction = 0;
      low_curvature_distance = 0.0;
    };
  for (const auto & pose : path) {
    unsigned int map_x = 0u;
    unsigned int map_y = 0u;
    double cost = static_cast<double>(costmap_2d::NO_INFORMATION);
    if (costmap.worldToMap(
        pose.pose.position.x, pose.pose.position.y, map_x, map_y))
    {
      cost = static_cast<double>(costmap.getCost(map_x, map_y));
    }
    center_cost_sum += cost;
    quality.max_center_cost = std::max(quality.max_center_cost, cost);
    ++center_cost_samples;
  }
  for (std::size_t index = 1u; index < path.size(); ++index) {
    const double distance = std::hypot(
      path[index].pose.position.x - path[index - 1u].pose.position.x,
      path[index].pose.position.y - path[index - 1u].pose.position.y);
    const double yaw_change = angles::shortest_angular_distance(
      tf2::getYaw(path[index - 1u].pose.orientation),
      tf2::getYaw(path[index].pose.orientation));
    if (distance <= kTranslationEpsilon) {
      if (std::abs(yaw_change) > 1e-3) {
        ++quality.in_place_rotations;
        have_previous_curvature = false;
        reset_material_turn_chain();
      }
      continue;
    }
    quality.length_m += distance;
    const double curvature = yaw_change / distance;
    const double absolute_curvature = std::abs(curvature);
    absolute_curvatures.push_back(absolute_curvature);
    quality.max_abs_curvature_radpm = std::max(
      quality.max_abs_curvature_radpm, absolute_curvature);
    if (have_previous_curvature) {
      const double jump = std::abs(curvature - previous_curvature);
      quality.curvature_total_variation_radpm += jump;
      quality.max_curvature_jump_radpm = std::max(
        quality.max_curvature_jump_radpm, jump);
    }
    previous_curvature = curvature;
    have_previous_curvature = true;
    if (absolute_curvature >= kCurvatureDirectionThresholdRadpm) {
      const int curvature_direction = curvature < 0.0 ? -1 : 1;
      if (low_curvature_distance >= kCurvatureStraightResetDistanceM) {
        reset_material_turn_chain();
      } else {
        low_curvature_distance = 0.0;
      }
      if (active_curvature_direction != 0 &&
        curvature_direction != active_curvature_direction)
      {
        commit_material_turn();
      }
      active_curvature_direction = curvature_direction;
      active_absolute_yaw_change += std::abs(yaw_change);
    } else {
      low_curvature_distance += distance;
      if (low_curvature_distance >= kCurvatureStraightResetDistanceM) {
        reset_material_turn_chain();
      }
    }
  }
  commit_material_turn();
  if (center_cost_samples > 0u) {
    quality.mean_center_cost = center_cost_sum /
      static_cast<double>(center_cost_samples);
  }
  if (!absolute_curvatures.empty()) {
    std::sort(absolute_curvatures.begin(), absolute_curvatures.end());
    const std::size_t index = std::min(
      absolute_curvatures.size() - 1u,
      static_cast<std::size_t>(std::ceil(
        0.95 * static_cast<double>(absolute_curvatures.size()))) - 1u);
    quality.p95_abs_curvature_radpm = absolute_curvatures[index];
  }
  return quality;
}

}  // namespace smac_lattice_planner_mbf

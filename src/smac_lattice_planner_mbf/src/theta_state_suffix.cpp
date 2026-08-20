// Copyright 2026 Clean Robot Navigation Team
// Licensed under the Apache License, Version 2.0.

#include "smac_lattice_planner_mbf/theta_state_suffix.hpp"

#include <algorithm>
#include <cmath>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <utility>

#include <angles/angles.h>
#include <tf2/utils.h>

namespace smac_lattice_planner_mbf
{
namespace theta_state_suffix
{
namespace
{

void requireValidEpsilon(double epsilon, const char * name)
{
  if (!std::isfinite(epsilon) || epsilon < 0.0) {
    throw std::invalid_argument(std::string(name) + " must be finite and non-negative");
  }
}

bool hasFiniteSE2(const geometry_msgs::PoseStamped & pose)
{
  const auto & position = pose.pose.position;
  const auto & orientation = pose.pose.orientation;
  const double quaternion_norm_squared =
    orientation.x * orientation.x + orientation.y * orientation.y +
    orientation.z * orientation.z + orientation.w * orientation.w;
  return std::isfinite(position.x) && std::isfinite(position.y) &&
         std::isfinite(position.z) &&
         std::isfinite(orientation.x) && std::isfinite(orientation.y) &&
         std::isfinite(orientation.z) && std::isfinite(orientation.w) &&
         std::isfinite(quaternion_norm_squared) && quaternion_norm_squared > 1e-12 &&
         std::isfinite(tf2::getYaw(orientation));
}

void validatePath(const PosePath & path, const char * name)
{
  if (path.empty()) {
    throw std::invalid_argument(std::string(name) + " must not be empty");
  }
  const std::string & frame = path.front().header.frame_id;
  for (std::size_t index = 0u; index < path.size(); ++index) {
    if (path[index].header.frame_id != frame) {
      throw std::invalid_argument(
              std::string(name) + " changes frame at pose " + std::to_string(index));
    }
    if (!hasFiniteSE2(path[index])) {
      throw std::invalid_argument(
              std::string(name) + " has invalid SE(2) data at pose " +
              std::to_string(index));
    }
  }
}

bool samePosition(
  const geometry_msgs::PoseStamped & first,
  const geometry_msgs::PoseStamped & second,
  double position_epsilon)
{
  return std::hypot(
    first.pose.position.x - second.pose.position.x,
    first.pose.position.y - second.pose.position.y) <= position_epsilon;
}

}  // namespace

std::size_t canonicalizeInitialLatticeMotion(
  PosePath & path,
  double max_seed_yaw_error,
  double max_forward_departure_heading_error,
  double position_epsilon,
  double primitive_boundary_yaw_epsilon)
{
  requireValidEpsilon(max_seed_yaw_error, "max_seed_yaw_error");
  requireValidEpsilon(
    max_forward_departure_heading_error,
    "max_forward_departure_heading_error");
  requireValidEpsilon(position_epsilon, "position_epsilon");
  requireValidEpsilon(
    primitive_boundary_yaw_epsilon,
    "primitive_boundary_yaw_epsilon");
  if (path.size() < 2u) {
    return 0u;
  }
  validatePath(path, "path");

  std::size_t removed = 0u;

  // With an exact continuous start prepended, the usual backtrace layout is:
  // exact start, selected lattice seed, first primitive start sample, ...
  // Remove the primitive sample first.  Comparing it with the seed (rather
  // than the exact yaw) also covers a non-primary start-heading seed.
  if (path.size() >= 3u &&
    samePosition(path[0], path[1], position_epsilon) &&
    samePosition(path[1], path[2], position_epsilon) &&
    std::abs(angles::shortest_angular_distance(
      tf2::getYaw(path[1].pose.orientation),
      tf2::getYaw(path[2].pose.orientation))) <= primitive_boundary_yaw_epsilon)
  {
    path.erase(path.begin() + 2u);
    ++removed;
  }

  if (path.size() < 3u || !samePosition(path[0], path[1], position_epsilon)) {
    return removed;
  }

  const double seed_yaw_error = std::abs(angles::shortest_angular_distance(
      tf2::getYaw(path[0].pose.orientation),
      tf2::getYaw(path[1].pose.orientation)));
  if (seed_yaw_error > max_seed_yaw_error) {
    return removed;
  }

  // A following same-position changed-yaw sample is the searched rotation,
  // not another seed.  Removing the seed preserves that explicit rotation as
  // a direct edge from the measured start.
  if (samePosition(path[1], path[2], position_epsilon)) {
    path.erase(path.begin() + 1u);
    return removed + 1u;
  }

  const double dx = path[2].pose.position.x - path[0].pose.position.x;
  const double dy = path[2].pose.position.y - path[0].pose.position.y;
  const double departure = std::atan2(dy, dx);
  const double departure_heading_error = std::abs(
    angles::shortest_angular_distance(
      tf2::getYaw(path[0].pose.orientation), departure));
  if (departure_heading_error > max_forward_departure_heading_error) {
    return removed;
  }

  path.erase(path.begin() + 1u);
  return removed + 1u;
}

ThetaPrefixCut selectThetaPrefixCut(
  const PosePath & theta_reference,
  std::size_t point_count)
{
  if (theta_reference.empty()) {
    throw std::invalid_argument("Theta reference path must not be empty");
  }
  if (point_count == 0u) {
    throw std::invalid_argument("Theta suffix point_count must be greater than zero");
  }

  const std::size_t effective_count = std::min(point_count, theta_reference.size());
  const std::size_t cut_index = theta_reference.size() - effective_count;
  ThetaPrefixCut selection;
  selection.requested_suffix_point_count = point_count;
  selection.effective_suffix_point_count = effective_count;
  selection.cut_index = cut_index;
  selection.is_full_path = cut_index == 0u;
  selection.prefix_including_cut.assign(
    theta_reference.begin(), theta_reference.begin() + cut_index + 1u);
  selection.cut = theta_reference[cut_index];
  return selection;
}

ThetaPrefixCut selectThetaPrefixCutBeforeUnsafeSegment(
  const PosePath & theta_reference,
  std::size_t unsafe_segment_index,
  std::size_t lookback_points)
{
  if (theta_reference.empty()) {
    throw std::invalid_argument("Theta reference path must not be empty");
  }
  if (theta_reference.size() < 2u ||
    unsafe_segment_index >= theta_reference.size() - 1u)
  {
    throw std::invalid_argument("unsafe Theta segment index is out of range");
  }
  if (lookback_points == 0u) {
    throw std::invalid_argument("unsafe-segment lookback must be greater than zero");
  }

  const std::size_t cut_index =
    unsafe_segment_index > lookback_points ? unsafe_segment_index - lookback_points : 0u;
  return selectThetaPrefixCut(theta_reference, theta_reference.size() - cut_index);
}

std::vector<ThetaPrefixCut> makeThetaPrefixCutCandidates(
  const PosePath & theta_reference)
{
  if (theta_reference.empty()) {
    throw std::invalid_argument("Theta reference path must not be empty");
  }

  std::vector<ThetaPrefixCut> candidates;
  candidates.reserve(kSuffixPointCountCandidates.size() + 1u);
  std::unordered_set<std::size_t> emitted_cut_indices;
  const auto append_unique = [&] (std::size_t requested_count) {
      ThetaPrefixCut candidate = selectThetaPrefixCut(theta_reference, requested_count);
      if (emitted_cut_indices.insert(candidate.cut_index).second) {
        candidates.push_back(std::move(candidate));
      }
    };

  for (const std::size_t count : kSuffixPointCountCandidates) {
    append_unique(count);
  }
  append_unique(theta_reference.size());
  return candidates;
}

ThetaPrefixJoin selectThetaPrefixJoin(
  const PosePath & theta_reference,
  std::size_t requested_join_index)
{
  if (theta_reference.empty()) {
    throw std::invalid_argument("Theta reference path must not be empty");
  }
  if (requested_join_index == 0u) {
    throw std::invalid_argument("Theta prefix join index must be greater than zero");
  }

  ThetaPrefixJoin selection;
  selection.requested_join_index = requested_join_index;
  selection.join_index = std::min(requested_join_index, theta_reference.size() - 1u);
  selection.reaches_theta_goal = selection.join_index + 1u == theta_reference.size();
  selection.join = theta_reference[selection.join_index];
  selection.theta_from_join.assign(
    theta_reference.begin() + selection.join_index, theta_reference.end());
  return selection;
}

std::vector<ThetaPrefixJoin> makeThetaPrefixJoinCandidates(
  const PosePath & theta_reference)
{
  if (theta_reference.empty()) {
    throw std::invalid_argument("Theta reference path must not be empty");
  }

  std::vector<ThetaPrefixJoin> candidates;
  candidates.reserve(kPrefixJoinIndexCandidates.size());
  std::unordered_set<std::size_t> emitted_join_indices;
  for (const std::size_t index : kPrefixJoinIndexCandidates) {
    auto candidate = selectThetaPrefixJoin(theta_reference, index);
    if (emitted_join_indices.insert(candidate.join_index).second) {
      candidates.push_back(std::move(candidate));
    }
  }
  return candidates;
}

bool sameSE2(
  const geometry_msgs::PoseStamped & first,
  const geometry_msgs::PoseStamped & second,
  double position_epsilon,
  double yaw_epsilon)
{
  requireValidEpsilon(position_epsilon, "position_epsilon");
  requireValidEpsilon(yaw_epsilon, "yaw_epsilon");
  if (!hasFiniteSE2(first) || !hasFiniteSE2(second) ||
    !samePosition(first, second, position_epsilon))
  {
    return false;
  }
  const double yaw_error = std::abs(angles::shortest_angular_distance(
      tf2::getYaw(first.pose.orientation), tf2::getYaw(second.pose.orientation)));
  return yaw_error <= yaw_epsilon;
}

bool ensureExplicitInitialTangentRotation(
  PosePath & path,
  std::string & reason,
  double position_epsilon,
  double yaw_epsilon,
  double max_rotation_step)
{
  requireValidEpsilon(position_epsilon, "position_epsilon");
  requireValidEpsilon(yaw_epsilon, "yaw_epsilon");
  requireValidEpsilon(max_rotation_step, "max_rotation_step");
  if (max_rotation_step <= yaw_epsilon || max_rotation_step >= M_PI) {
    throw std::invalid_argument("max_rotation_step must be greater than yaw_epsilon and less than pi");
  }
  validatePath(path, "Theta reference");
  const auto & start = path.front();
  std::size_t translated_index = path.size();
  for (std::size_t index = 1u; index < path.size(); ++index) {
    if (!samePosition(start, path[index], position_epsilon)) {
      translated_index = index;
      break;
    }
  }
  if (translated_index == path.size()) {
    reason = "Theta reference has no translated edge from its start";
    return false;
  }

  const double tangent_yaw = std::atan2(
    path[translated_index].pose.position.y - start.pose.position.y,
    path[translated_index].pose.position.x - start.pose.position.x);
  const double start_yaw = tf2::getYaw(start.pose.orientation);
  const double signed_rotation =
    angles::shortest_angular_distance(start_yaw, tangent_yaw);
  if (std::abs(signed_rotation) <= yaw_epsilon) {
    reason.clear();
    return true;
  }

  // nav_msgs/Path has no separate rotation-direction field.  Encode the
  // selected shortest signed sweep as bounded same-position yaw increments,
  // so collision validation and the controller observe one direction rather
  // than a single large, effectively ambiguous orientation jump.
  const std::size_t steps = std::max<std::size_t>(
    1u, static_cast<std::size_t>(
      std::ceil(std::abs(signed_rotation) / max_rotation_step)));
  PosePath rotation;
  rotation.reserve(steps);
  for (std::size_t step = 1u; step <= steps; ++step) {
    geometry_msgs::PoseStamped pose = start;
    const double yaw = start_yaw + signed_rotation *
      static_cast<double>(step) / static_cast<double>(steps);
    pose.pose.orientation.x = 0.0;
    pose.pose.orientation.y = 0.0;
    pose.pose.orientation.z = std::sin(0.5 * yaw);
    pose.pose.orientation.w = std::cos(0.5 * yaw);
    rotation.push_back(std::move(pose));
  }
  path.insert(path.begin() + 1, rotation.begin(), rotation.end());
  reason.clear();
  return true;
}

bool containsOnlyForwardOrRotation(
  const PosePath & path,
  std::string & reason,
  double translation_epsilon,
  double direction_cosine_tolerance)
{
  requireValidEpsilon(translation_epsilon, "translation_epsilon");
  if (!std::isfinite(direction_cosine_tolerance)) {
    throw std::invalid_argument("direction_cosine_tolerance must be finite");
  }
  direction_cosine_tolerance = std::clamp(
    std::abs(direction_cosine_tolerance), 0.0, 1.0);
  validatePath(path, "Audited path");

  for (std::size_t index = 0u; index + 1u < path.size(); ++index) {
    const auto & start = path[index];
    const auto & end = path[index + 1u];
    const double dx = end.pose.position.x - start.pose.position.x;
    const double dy = end.pose.position.y - start.pose.position.y;
    const double translation = std::hypot(dx, dy);
    if (translation <= translation_epsilon) {
      continue;
    }
    const double inverse_translation = 1.0 / translation;
    const double start_yaw = tf2::getYaw(start.pose.orientation);
    const double end_yaw = tf2::getYaw(end.pose.orientation);
    const double start_projection =
      (dx * std::cos(start_yaw) + dy * std::sin(start_yaw)) * inverse_translation;
    const double end_projection =
      (dx * std::cos(end_yaw) + dy * std::sin(end_yaw)) * inverse_translation;
    const double projection =
      std::abs(start_projection) >= std::abs(end_projection) ?
      start_projection : end_projection;
    if (projection < direction_cosine_tolerance) {
      reason = "translated edge " + std::to_string(index) +
        " is not forward (projection=" + std::to_string(projection) + ")";
      return false;
    }
  }
  reason.clear();
  return true;
}

bool containsKinematicallyContinuousForwardOrRotation(
  const PosePath & path,
  std::string & reason,
  double translation_epsilon,
  double direction_cosine_tolerance,
  double opposing_projection_tolerance,
  double short_translation_threshold,
  double near_pi_yaw_threshold)
{
  requireValidEpsilon(translation_epsilon, "translation_epsilon");
  requireValidEpsilon(short_translation_threshold, "short_translation_threshold");
  if (!std::isfinite(opposing_projection_tolerance)) {
    throw std::invalid_argument("opposing_projection_tolerance must be finite");
  }
  if (!std::isfinite(near_pi_yaw_threshold) || near_pi_yaw_threshold <= 0.0 ||
    near_pi_yaw_threshold > M_PI)
  {
    throw std::invalid_argument(
            "near_pi_yaw_threshold must be finite and in the interval (0, pi]");
  }
  opposing_projection_tolerance = std::clamp(
    std::abs(opposing_projection_tolerance), 0.0, 1.0);

  // Preserve the established forward-or-explicit-rotation contract before
  // applying the stricter endpoint-continuity checks below.
  if (!containsOnlyForwardOrRotation(
      path, reason, translation_epsilon, direction_cosine_tolerance))
  {
    return false;
  }

  for (std::size_t index = 0u; index + 1u < path.size(); ++index) {
    const auto & start = path[index];
    const auto & end = path[index + 1u];
    const double dx = end.pose.position.x - start.pose.position.x;
    const double dy = end.pose.position.y - start.pose.position.y;
    const double translation = std::hypot(dx, dy);
    if (translation <= translation_epsilon) {
      // A yaw change without translation is an explicit differential-drive
      // rotation, including a deliberate near-pi turn.
      continue;
    }

    const double inverse_translation = 1.0 / translation;
    const double start_yaw = tf2::getYaw(start.pose.orientation);
    const double end_yaw = tf2::getYaw(end.pose.orientation);
    const double start_projection =
      (dx * std::cos(start_yaw) + dy * std::sin(start_yaw)) * inverse_translation;
    const double end_projection =
      (dx * std::cos(end_yaw) + dy * std::sin(end_yaw)) * inverse_translation;
    const bool endpoint_projections_are_opposed =
      ((start_projection > 0.0 && end_projection < 0.0) ||
      (start_projection < 0.0 && end_projection > 0.0)) &&
      std::abs(start_projection) >= opposing_projection_tolerance &&
      std::abs(end_projection) >= opposing_projection_tolerance;
    if (endpoint_projections_are_opposed) {
      reason = "translated edge " + std::to_string(index) +
        " has opposite endpoint heading projections (start_projection=" +
        std::to_string(start_projection) + ", end_projection=" +
        std::to_string(end_projection) + ")";
      return false;
    }

    const double yaw_change = std::abs(angles::shortest_angular_distance(
        start_yaw, end_yaw));
    const double short_translation_comparison_epsilon = std::max(
      1e-9, short_translation_threshold * 1e-6);
    if (translation <=
      short_translation_threshold + short_translation_comparison_epsilon &&
      yaw_change >= near_pi_yaw_threshold)
    {
      reason = "short translating edge " + std::to_string(index) +
        " has a near-pi yaw flip (translation=" + std::to_string(translation) +
        ", yaw_change=" + std::to_string(yaw_change) + ")";
      return false;
    }
  }

  reason.clear();
  return true;
}

bool containsContinuousForwardOnly(
  const PosePath & path,
  std::string & reason,
  double translation_epsilon,
  double yaw_epsilon,
  double max_start_heading_error)
{
  requireValidEpsilon(translation_epsilon, "translation_epsilon");
  requireValidEpsilon(yaw_epsilon, "yaw_epsilon");
  requireValidEpsilon(max_start_heading_error, "max_start_heading_error");
  if (!containsKinematicallyContinuousForwardOrRotation(
      path, reason, translation_epsilon))
  {
    return false;
  }

  bool found_translation = false;
  for (std::size_t index = 0u; index + 1u < path.size(); ++index) {
    const auto & start = path[index];
    const auto & end = path[index + 1u];
    const double dx = end.pose.position.x - start.pose.position.x;
    const double dy = end.pose.position.y - start.pose.position.y;
    const double translation = std::hypot(
      dx, dy);
    if (translation > translation_epsilon) {
      if (!found_translation) {
        found_translation = true;
        const double departure_bearing = std::atan2(dy, dx);
        const double heading_error = std::abs(angles::shortest_angular_distance(
            tf2::getYaw(path.front().pose.orientation), departure_bearing));
        if (heading_error > max_start_heading_error) {
          reason = "first translated edge departs " + std::to_string(heading_error) +
            " rad away from the exact start yaw (limit=" +
            std::to_string(max_start_heading_error) + ")";
          return false;
        }
      }
      continue;
    }
    const double yaw_change = std::abs(angles::shortest_angular_distance(
        tf2::getYaw(start.pose.orientation), tf2::getYaw(end.pose.orientation)));
    if (yaw_change > yaw_epsilon) {
      reason = "same-position rotation at edge " + std::to_string(index) +
        " is not allowed in a continuous-forward State prefix (yaw_change=" +
        std::to_string(yaw_change) + ")";
      return false;
    }
  }
  if (!found_translation) {
    reason = "continuous-forward State prefix contains no translated edge";
    return false;
  }
  reason.clear();
  return true;
}

bool joinIsPositionHeadingCurvatureContinuous(
  const PosePath & before_join,
  const PosePath & from_join,
  std::string & reason,
  double position_epsilon,
  double yaw_epsilon,
  double max_heading_error,
  double max_curvature_jump,
  double translation_epsilon)
{
  requireValidEpsilon(position_epsilon, "position_epsilon");
  requireValidEpsilon(yaw_epsilon, "yaw_epsilon");
  requireValidEpsilon(max_heading_error, "max_heading_error");
  requireValidEpsilon(max_curvature_jump, "max_curvature_jump");
  requireValidEpsilon(translation_epsilon, "translation_epsilon");
  validatePath(before_join, "Path before join");
  validatePath(from_join, "Path from join");

  if (!sameSE2(
      before_join.back(), from_join.front(), position_epsilon, yaw_epsilon))
  {
    reason = "join poses do not share the required position and heading";
    return false;
  }

  struct Edge
  {
    double bearing{0.0};
    double length{0.0};
  };
  std::vector<Edge> incoming;
  for (std::size_t index = before_join.size() - 1u; index > 0u && incoming.size() < 2u;
    --index)
  {
    const auto & start = before_join[index - 1u];
    const auto & end = before_join[index];
    const double dx = end.pose.position.x - start.pose.position.x;
    const double dy = end.pose.position.y - start.pose.position.y;
    const double length = std::hypot(dx, dy);
    if (length > translation_epsilon) {
      incoming.push_back({std::atan2(dy, dx), length});
    }
  }
  std::reverse(incoming.begin(), incoming.end());

  std::vector<Edge> outgoing;
  for (std::size_t index = 0u;
    index + 1u < from_join.size() && outgoing.size() < 2u; ++index)
  {
    const auto & start = from_join[index];
    const auto & end = from_join[index + 1u];
    const double dx = end.pose.position.x - start.pose.position.x;
    const double dy = end.pose.position.y - start.pose.position.y;
    const double length = std::hypot(dx, dy);
    if (length > translation_epsilon) {
      outgoing.push_back({std::atan2(dy, dx), length});
    }
  }
  if (incoming.size() < 2u || outgoing.size() < 2u) {
    reason = "join does not have two translated edges on both sides for curvature proof";
    return false;
  }

  const double heading_error = std::abs(angles::shortest_angular_distance(
      incoming.back().bearing, outgoing.front().bearing));
  if (heading_error > max_heading_error) {
    reason = "join tangent error " + std::to_string(heading_error) +
      " exceeds " + std::to_string(max_heading_error);
    return false;
  }

  const auto curvature = [] (const Edge & first, const Edge & second) {
      const double mean_length = 0.5 * (first.length + second.length);
      return angles::shortest_angular_distance(first.bearing, second.bearing) /
             mean_length;
    };
  const double incoming_curvature = curvature(incoming[0], incoming[1]);
  const double outgoing_curvature = curvature(outgoing[0], outgoing[1]);
  const double curvature_jump = std::abs(outgoing_curvature - incoming_curvature);
  // Lattice JSON stores XY samples at finite decimal precision.  At the
  // configured 0.40 m radius that makes the measured 2.5 rad/m curvature up
  // to about 0.07% larger than the analytic value.  Preserve the physical
  // bound while avoiding a false join rejection caused only by serialization.
  constexpr double kCurvatureRelativeNumericalTolerance = 1e-3;
  const double allowed_curvature_jump =
    max_curvature_jump * (1.0 + kCurvatureRelativeNumericalTolerance) + 1e-6;
  if (curvature_jump > allowed_curvature_jump) {
    reason = "join curvature jump " + std::to_string(curvature_jump) +
      " rad/m exceeds " + std::to_string(allowed_curvature_jump) +
      " (incoming=" + std::to_string(incoming_curvature) +
      ", outgoing=" + std::to_string(outgoing_curvature) + ")";
    return false;
  }

  reason.clear();
  return true;
}

bool terminalAvoidsStationaryYawRepair(
  const PosePath & path,
  std::string & reason,
  double position_epsilon,
  double yaw_epsilon)
{
  requireValidEpsilon(position_epsilon, "position_epsilon");
  requireValidEpsilon(yaw_epsilon, "yaw_epsilon");
  validatePath(path, "Terminal-audited path");
  if (path.size() < 2u) {
    reason.clear();
    return true;
  }

  // Ignore any exact/epsilon-equivalent duplicates after the actual terminal
  // edge, then classify only that final meaningful SE(2) transition. Earlier
  // same-position rotations remain valid differential-drive motions.
  const auto & terminal = path.back();
  std::size_t previous_index = path.size() - 1u;
  while (previous_index > 0u && sameSE2(
      path[previous_index - 1u], terminal, position_epsilon, yaw_epsilon))
  {
    --previous_index;
  }
  if (previous_index == 0u) {
    reason.clear();
    return true;
  }

  const auto & previous = path[previous_index - 1u];
  const double translation = std::hypot(
    terminal.pose.position.x - previous.pose.position.x,
    terminal.pose.position.y - previous.pose.position.y);
  const double yaw_change = std::abs(angles::shortest_angular_distance(
      tf2::getYaw(previous.pose.orientation), tf2::getYaw(terminal.pose.orientation)));
  if (translation <= position_epsilon && yaw_change > yaw_epsilon) {
    reason = "final meaningful edge is a same-position yaw repair (translation=" +
      std::to_string(translation) + ", yaw_change=" + std::to_string(yaw_change) + ")";
    return false;
  }
  reason.clear();
  return true;
}

PosePath stitchThetaPrefixAndStateSuffix(
  const PosePath & theta_prefix_including_cut,
  const PosePath & state_suffix,
  double position_epsilon,
  double yaw_epsilon)
{
  requireValidEpsilon(position_epsilon, "position_epsilon");
  requireValidEpsilon(yaw_epsilon, "yaw_epsilon");
  validatePath(theta_prefix_including_cut, "Theta prefix");
  validatePath(state_suffix, "State suffix");

  const auto & theta_cut = theta_prefix_including_cut.back();
  const auto & state_start = state_suffix.front();
  if (theta_cut.header.frame_id != state_start.header.frame_id) {
    throw std::invalid_argument("Theta cut and State suffix start use different frames");
  }
  if (!samePosition(theta_cut, state_start, position_epsilon)) {
    throw std::invalid_argument("State suffix does not start at the Theta cut position");
  }

  PosePath stitched;
  stitched.reserve(theta_prefix_including_cut.size() + state_suffix.size());
  stitched.insert(
    stitched.end(), theta_prefix_including_cut.begin(), theta_prefix_including_cut.end());
  const std::size_t suffix_begin =
    sameSE2(theta_cut, state_start, position_epsilon, yaw_epsilon) ? 1u : 0u;
  stitched.insert(
    stitched.end(), state_suffix.begin() + suffix_begin, state_suffix.end());
  return stitched;
}

PosePath stitchStatePrefixThetaMiddleStateSuffix(
  const PosePath & state_prefix,
  const PosePath & theta_middle_including_joins,
  const PosePath & state_suffix,
  double position_epsilon,
  double yaw_epsilon)
{
  const PosePath prefix_and_middle = stitchThetaPrefixAndStateSuffix(
    state_prefix, theta_middle_including_joins, position_epsilon, yaw_epsilon);
  return stitchThetaPrefixAndStateSuffix(
    prefix_and_middle, state_suffix, position_epsilon, yaw_epsilon);
}

}  // namespace theta_state_suffix
}  // namespace smac_lattice_planner_mbf

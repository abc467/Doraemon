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
  double yaw_epsilon)
{
  requireValidEpsilon(position_epsilon, "position_epsilon");
  requireValidEpsilon(yaw_epsilon, "yaw_epsilon");
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
  if (std::abs(angles::shortest_angular_distance(start_yaw, tangent_yaw)) <= yaw_epsilon) {
    reason.clear();
    return true;
  }

  geometry_msgs::PoseStamped aligned = start;
  aligned.pose.orientation.x = 0.0;
  aligned.pose.orientation.y = 0.0;
  aligned.pose.orientation.z = std::sin(0.5 * tangent_yaw);
  aligned.pose.orientation.w = std::cos(0.5 * tangent_yaw);
  path.insert(path.begin() + 1, std::move(aligned));
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

}  // namespace theta_state_suffix
}  // namespace smac_lattice_planner_mbf

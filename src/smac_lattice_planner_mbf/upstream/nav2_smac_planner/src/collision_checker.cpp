// Copyright (c) 2021, Samsung Research America
// Copyright (c) 2026, Clean Robot Navigation Team
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
// limitations under the License. Reserved.

#include "nav2_smac_planner/collision_checker.hpp"

#include <algorithm>
#include <cmath>
#include <limits>
#include <stdexcept>

#include <angles/angles.h>
#include <base_local_planner/footprint_helper.h>
#include <Eigen/Core>

namespace nav2_smac_planner
{

GridCollisionChecker::GridCollisionChecker(
  nav2_costmap_2d::Costmap2D * costmap,
  unsigned int num_quantizations)
: costmap_(costmap)
{
  if (!costmap_) {
    throw std::invalid_argument("GridCollisionChecker requires a Costmap2D");
  }
  if (num_quantizations == 0u) {
    throw std::invalid_argument("GridCollisionChecker requires orientation bins");
  }
  world_model_ = std::make_unique<base_local_planner::CostmapModel>(*costmap_);

  const float bin_size = 2.0f * static_cast<float>(M_PI) /
    static_cast<float>(num_quantizations);
  angles_.reserve(num_quantizations);
  for (unsigned int i = 0; i < num_quantizations; ++i) {
    angles_.push_back(bin_size * static_cast<float>(i));
  }
  rebuildObstaclePrefixes();
}

void GridCollisionChecker::setFootprint(
  const Footprint & footprint,
  const bool & radius,
  const double & possible_collision_cost)
{
  possible_collision_cost_ = static_cast<float>(possible_collision_cost);
  footprint_is_radius_ = radius;
  footprint_radius_ = 0.0;
  for (const auto & point : footprint) {
    footprint_radius_ = std::max(footprint_radius_, std::hypot(point.x, point.y));
  }

  if (radius) {
    unoriented_footprint_ = footprint;
    oriented_footprints_.clear();
    return;
  }
  if (footprint.size() < 3u) {
    throw std::invalid_argument("A polygon footprint requires at least three points");
  }
  if (footprint == unoriented_footprint_) {
    return;
  }

  oriented_footprints_.clear();
  oriented_footprints_.reserve(angles_.size());
  for (const float angle : angles_) {
    const double sin_th = std::sin(angle);
    const double cos_th = std::cos(angle);
    Footprint oriented;
    oriented.reserve(footprint.size());
    for (const auto & point : footprint) {
      geometry_msgs::Point rotated;
      rotated.x = point.x * cos_th - point.y * sin_th;
      rotated.y = point.x * sin_th + point.y * cos_th;
      rotated.z = point.z;
      oriented.push_back(rotated);
    }
    oriented_footprints_.push_back(oriented);
  }
  unoriented_footprint_ = footprint;
}

void GridCollisionChecker::setCollisionCheckResolution(double sample_step_m)
{
  if (!std::isfinite(sample_step_m) || sample_step_m <= 0.0) {
    throw std::invalid_argument("collision_check_resolution must be positive");
  }
  collision_check_resolution_ = sample_step_m;
}

bool GridCollisionChecker::footprintCollision(
  const float & x, const float & y, const double yaw,
  const bool & traverse_unknown, float * max_footprint_cost)
{
  // Preserve Nav2's continuous map coordinates. ROS 1 Costmap2D::mapToWorld()
  // accepts integer cells only and would otherwise shift every sub-cell sample
  // to a cell center, defeating a 1 cm swept-volume sampling bound.
  const double wx = costmap_->getOriginX() +
    static_cast<double>(x) * costmap_->getResolution();
  const double wy = costmap_->getOriginY() +
    static_cast<double>(y) * costmap_->getResolution();
  const double sin_th = std::sin(yaw);
  const double cos_th = std::cos(yaw);

  Footprint current_footprint;
  current_footprint.reserve(unoriented_footprint_.size());
  for (const auto & point : unoriented_footprint_) {
    geometry_msgs::Point transformed;
    transformed.x = wx + point.x * cos_th - point.y * sin_th;
    transformed.y = wy + point.x * sin_th + point.y * cos_th;
    transformed.z = point.z;
    current_footprint.push_back(transformed);
  }

  geometry_msgs::Point position;
  position.x = wx;
  position.y = wy;
  const double footprint_cost = world_model_->footprintCost(
    position, current_footprint, 0.0, footprint_radius_);

  // CostmapModel already rasterizes the transformed footprint boundary to
  // obtain this value for collision checking. Preserve its maximum non-negative
  // boundary cost so lattice traversal cost reflects an offset rectangular
  // footprint instead of only the vehicle-centre cell. Hard-obstacle handling
  // below is intentionally unchanged.
  if (max_footprint_cost && footprint_cost >= 0.0) {
    *max_footprint_cost = std::max(
      *max_footprint_cost, static_cast<float>(footprint_cost));
  }

  if (footprint_cost == -2.0 && traverse_unknown) {
    // Unknown outline cells are traversable only when explicitly requested.
    // A lethal cell in the polygon interior must still be checked below.
  } else if (footprint_cost < 0.0 || footprint_cost >= OCCUPIED_COST) {
    return true;
  }

  double minimum_x = std::numeric_limits<double>::infinity();
  double minimum_y = std::numeric_limits<double>::infinity();
  double maximum_x = -std::numeric_limits<double>::infinity();
  double maximum_y = -std::numeric_limits<double>::infinity();
  for (const auto & point : current_footprint) {
    minimum_x = std::min(minimum_x, point.x);
    minimum_y = std::min(minimum_y, point.y);
    maximum_x = std::max(maximum_x, point.x);
    maximum_y = std::max(maximum_y, point.y);
  }

  unsigned int map_x = 0u;
  unsigned int map_y = 0u;
  if (!costmap_->worldToMap(minimum_x, minimum_y, map_x, map_y) ||
    !costmap_->worldToMap(maximum_x, maximum_y, map_x, map_y))
  {
    return true;
  }

  // The immutable planning snapshot lets us build these integral images once.
  // In the common free-space case they prove in O(1) that no hard cell can be
  // hidden inside the rectangle.  Keep separate lethal-only and
  // lethal-or-unknown tables so fail-closed planning is just as fast as the
  // explicitly allow-unknown mode without weakening either policy.
  if (!hardObstacleMayIntersect(
      minimum_x, minimum_y, maximum_x, maximum_y, traverse_unknown))
  {
    return false;
  }

  base_local_planner::FootprintHelper footprint_helper;
  const auto cells = footprint_helper.getFootprintCells(
    Eigen::Vector3f(
      static_cast<float>(wx), static_cast<float>(wy), static_cast<float>(yaw)),
    unoriented_footprint_, *costmap_, true);
  if (cells.empty()) {
    return true;
  }
  for (const auto & cell : cells) {
    if (cell.x < 0 || cell.y < 0 ||
      static_cast<unsigned int>(cell.x) >= costmap_->getSizeInCellsX() ||
      static_cast<unsigned int>(cell.y) >= costmap_->getSizeInCellsY())
    {
      return true;
    }
    const unsigned char cost = costmap_->getCost(
      static_cast<unsigned int>(cell.x), static_cast<unsigned int>(cell.y));
    if (cost == costmap_2d::LETHAL_OBSTACLE ||
      (cost == UNKNOWN_COST && !traverse_unknown))
    {
      return true;
    }
  }
  return false;
}

void GridCollisionChecker::rebuildObstaclePrefixes()
{
  lethal_prefix_valid_ = false;
  lethal_prefix_stride_ = 0u;
  lethal_prefix_.clear();
  blocked_prefix_.clear();
  if (!costmap_) {
    return;
  }

  const std::size_t width = costmap_->getSizeInCellsX();
  const std::size_t height = costmap_->getSizeInCellsY();
  if (width == 0u || height == 0u ||
    width > std::numeric_limits<std::uint32_t>::max() / height ||
    width > std::numeric_limits<std::size_t>::max() - 1u ||
    height > std::numeric_limits<std::size_t>::max() - 1u)
  {
    return;
  }
  const std::size_t stride = width + 1u;
  if ((height + 1u) > std::numeric_limits<std::size_t>::max() / stride) {
    return;
  }

  lethal_prefix_.assign((height + 1u) * stride, 0u);
  blocked_prefix_.assign((height + 1u) * stride, 0u);
  for (std::size_t y = 0u; y < height; ++y) {
    std::uint32_t lethal_row_sum = 0u;
    std::uint32_t blocked_row_sum = 0u;
    for (std::size_t x = 0u; x < width; ++x) {
      const unsigned char cost = costmap_->getCost(
        static_cast<unsigned int>(x), static_cast<unsigned int>(y));
      const bool lethal = cost == costmap_2d::LETHAL_OBSTACLE;
      lethal_row_sum += lethal ? 1u : 0u;
      blocked_row_sum +=
        (lethal || cost == costmap_2d::NO_INFORMATION) ? 1u : 0u;
      lethal_prefix_[(y + 1u) * stride + (x + 1u)] =
        lethal_prefix_[y * stride + (x + 1u)] + lethal_row_sum;
      blocked_prefix_[(y + 1u) * stride + (x + 1u)] =
        blocked_prefix_[y * stride + (x + 1u)] + blocked_row_sum;
    }
  }
  lethal_prefix_stride_ = stride;
  lethal_prefix_valid_ = true;
}

bool GridCollisionChecker::hardObstacleMayIntersect(
  double minimum_x, double minimum_y,
  double maximum_x, double maximum_y,
  bool traverse_unknown) const
{
  if (!lethal_prefix_valid_ || lethal_prefix_stride_ == 0u ||
    !std::isfinite(minimum_x) || !std::isfinite(minimum_y) ||
    !std::isfinite(maximum_x) || !std::isfinite(maximum_y) ||
    minimum_x > maximum_x || minimum_y > maximum_y)
  {
    return true;
  }

  unsigned int minimum_map_x = 0u;
  unsigned int minimum_map_y = 0u;
  unsigned int maximum_map_x = 0u;
  unsigned int maximum_map_y = 0u;
  if (!costmap_->worldToMap(
      minimum_x, minimum_y, minimum_map_x, minimum_map_y) ||
    !costmap_->worldToMap(
      maximum_x, maximum_y, maximum_map_x, maximum_map_y))
  {
    return true;
  }

  // FootprintHelper rasterizes grid lines. A one-cell halo makes the prefix
  // query conservative for cells touched by an edge even when their centres
  // lie just outside the floating-point AABB.
  const std::size_t width = costmap_->getSizeInCellsX();
  const std::size_t height = costmap_->getSizeInCellsY();
  const std::size_t x0 = minimum_map_x == 0u ? 0u : minimum_map_x - 1u;
  const std::size_t y0 = minimum_map_y == 0u ? 0u : minimum_map_y - 1u;
  const std::size_t x1 = std::min<std::size_t>(width - 1u, maximum_map_x + 1u) + 1u;
  const std::size_t y1 = std::min<std::size_t>(height - 1u, maximum_map_y + 1u) + 1u;
  const std::size_t stride = lethal_prefix_stride_;
  const auto & prefix = traverse_unknown ? lethal_prefix_ : blocked_prefix_;
  const std::uint64_t count =
    static_cast<std::uint64_t>(prefix[y1 * stride + x1]) +
    static_cast<std::uint64_t>(prefix[y0 * stride + x0]) -
    static_cast<std::uint64_t>(prefix[y0 * stride + x1]) -
    static_cast<std::uint64_t>(prefix[y1 * stride + x0]);
  return count != 0u;
}

bool GridCollisionChecker::inCollision(
  const float & x, const float & y, const float & angle_bin,
  const bool & traverse_unknown)
{
  const double bin_size = 2.0 * M_PI / static_cast<double>(angles_.size());
  return inCollisionAtYaw(x, y, static_cast<double>(angle_bin) * bin_size, traverse_unknown);
}

bool GridCollisionChecker::inCollisionAtYaw(
  const float & x, const float & y, const double yaw,
  const bool & traverse_unknown)
{
  if (outsideRange(costmap_->getSizeInCellsX(), x) ||
    outsideRange(costmap_->getSizeInCellsY(), y))
  {
    center_cost_ = OCCUPIED_COST;
    return true;
  }

  center_cost_ = static_cast<float>(costmap_->getCost(
      static_cast<unsigned int>(x), static_cast<unsigned int>(y)));

  if (center_cost_ == UNKNOWN_COST && !traverse_unknown) {
    return true;
  }
  if (center_cost_ == OCCUPIED_COST || center_cost_ == INSCRIBED_COST) {
    return true;
  }
  if (footprint_is_radius_) {
    return center_cost_ >= INSCRIBED_COST &&
           !(center_cost_ == UNKNOWN_COST && traverse_unknown);
  }
  if (possible_collision_cost_ > 0.0f && center_cost_ < possible_collision_cost_) {
    return false;
  }
  float max_footprint_cost = center_cost_;
  const bool collision = footprintCollision(
    x, y, yaw, traverse_unknown, &max_footprint_cost);
  center_cost_ = max_footprint_cost;
  return collision;
}

bool GridCollisionChecker::inCollisionContinuous(
  const float & x0, const float & y0, const double yaw0,
  const float & x1, const float & y1, const double yaw1,
  const bool & traverse_unknown, float * max_cost)
{
  const double delta_yaw = angles::shortest_angular_distance(yaw0, yaw1);

  // The official lattice and analytic-expansion implementations give us a
  // sequence of SE(2) states. Densify each adjacent pair from the actual rigid
  // polygon motion instead of importing the legacy planner's swept-distance
  // estimate. Refine from the measured polygon displacement until every final
  // sub-segment meets the bound, including a pure rotation where the robot
  // centre is fixed. The proportional update avoids power-of-two oversampling.
  constexpr unsigned int kMaxIntervals = 1u << 16;
  const double endpoint_displacement = maxFootprintVertexDisplacement(
    x0, y0, yaw0, x1, y1, yaw1);
  unsigned int intervals = std::max(
    1u, static_cast<unsigned int>(std::ceil(
      endpoint_displacement / collision_check_resolution_)));
  intervals = std::min(intervals, kMaxIntervals);
  while (true) {
    double max_vertex_step = 0.0;
    for (unsigned int i = 0; i < intervals; ++i) {
      const double ratio0 = static_cast<double>(i) / static_cast<double>(intervals);
      const double ratio1 = static_cast<double>(i + 1u) / static_cast<double>(intervals);
      const float sx0 = static_cast<float>(x0 + (x1 - x0) * ratio0);
      const float sy0 = static_cast<float>(y0 + (y1 - y0) * ratio0);
      const float sx1 = static_cast<float>(x0 + (x1 - x0) * ratio1);
      const float sy1 = static_cast<float>(y0 + (y1 - y0) * ratio1);
      const double syaw0 = angles::normalize_angle(yaw0 + delta_yaw * ratio0);
      const double syaw1 = angles::normalize_angle(yaw0 + delta_yaw * ratio1);
      max_vertex_step = std::max(
        max_vertex_step,
        maxFootprintVertexDisplacement(sx0, sy0, syaw0, sx1, sy1, syaw1));
    }
    if (max_vertex_step <= collision_check_resolution_ * (1.0 + 1e-9) ||
      intervals >= kMaxIntervals)
    {
      break;
    }
    const unsigned int scaled_intervals = static_cast<unsigned int>(std::ceil(
      static_cast<double>(intervals) * max_vertex_step /
      collision_check_resolution_));
    intervals = std::min(
      kMaxIntervals, std::max(intervals + 1u, scaled_intervals));
  }

  float segment_max_cost = FREE_COST;
  for (unsigned int i = 0; i <= intervals; ++i) {
    const double ratio = static_cast<double>(i) / static_cast<double>(intervals);
    const float x = static_cast<float>(x0 + (x1 - x0) * ratio);
    const float y = static_cast<float>(y0 + (y1 - y0) * ratio);
    const double yaw = angles::normalize_angle(yaw0 + delta_yaw * ratio);
    if (inCollisionAtYaw(x, y, yaw, traverse_unknown)) {
      if (max_cost) {
        *max_cost = std::max(segment_max_cost, center_cost_);
      }
      return true;
    }
    segment_max_cost = std::max(segment_max_cost, center_cost_);
  }
  if (max_cost) {
    *max_cost = segment_max_cost;
  }
  return false;
}

double GridCollisionChecker::maxFootprintVertexDisplacement(
  const float & x0, const float & y0, const double yaw0,
  const float & x1, const float & y1, const double yaw1) const
{
  const double resolution = costmap_->getResolution();
  const double dx = static_cast<double>(x1 - x0) * resolution;
  const double dy = static_cast<double>(y1 - y0) * resolution;
  const double cos0 = std::cos(yaw0);
  const double sin0 = std::sin(yaw0);
  const double cos1 = std::cos(yaw1);
  const double sin1 = std::sin(yaw1);

  double max_displacement = std::hypot(dx, dy);
  for (const auto & point : unoriented_footprint_) {
    const double rx0 = point.x * cos0 - point.y * sin0;
    const double ry0 = point.x * sin0 + point.y * cos0;
    const double rx1 = point.x * cos1 - point.y * sin1;
    const double ry1 = point.x * sin1 + point.y * cos1;
    max_displacement = std::max(
      max_displacement, std::hypot(dx + rx1 - rx0, dy + ry1 - ry0));
  }
  return max_displacement;
}

bool GridCollisionChecker::inCollision(
  const unsigned int & i, const bool & traverse_unknown)
{
  const unsigned int map_y = i / costmap_->getSizeInCellsX();
  const unsigned int map_x = i - map_y * costmap_->getSizeInCellsX();
  center_cost_ = static_cast<float>(costmap_->getCost(map_x, map_y));
  if (center_cost_ == UNKNOWN_COST && traverse_unknown) {
    return false;
  }
  return center_cost_ >= INSCRIBED_COST;
}

float GridCollisionChecker::getCost()
{
  return center_cost_;
}

bool GridCollisionChecker::outsideRange(const unsigned int & max, const float & value)
{
  return value < 0.0f || value >= static_cast<float>(max);
}

}  // namespace nav2_smac_planner

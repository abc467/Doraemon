#pragma once

#include <cmath>
#include <cstdint>
#include <vector>

#include <Eigen/Core>
#include <base_local_planner/footprint_helper.h>
#include <costmap_2d/cost_values.h>
#include <costmap_2d/costmap_2d.h>
#include <geometry_msgs/Point.h>

namespace mppi::utils
{

/**
 * @brief Check one complete physical footprint against hard costmap cells.
 *
 * INSCRIBED_INFLATED_OBSTACLE is deliberately not a hard body collision: the
 * polygon already represents the physical robot, so rejecting 253 inside it
 * would inflate the body twice. Lethal, disallowed unknown, non-finite and
 * partially out-of-map poses fail closed.
 */
inline bool isFootprintPoseHardCollisionFree(
  const costmap_2d::Costmap2D & costmap,
  const std::vector<geometry_msgs::Point> & footprint,
  base_local_planner::FootprintHelper & footprint_helper,
  double x, double y, double yaw,
  bool allow_unknown = false)
{
  if (footprint.size() < 3u || !std::isfinite(x) ||
      !std::isfinite(y) || !std::isfinite(yaw))
  {
    return false;
  }

  unsigned int center_x = 0u;
  unsigned int center_y = 0u;
  if (!costmap.worldToMap(x, y, center_x, center_y)) {
    return false;
  }
  const unsigned char center_cost = costmap.getCost(center_x, center_y);
  if (center_cost == costmap_2d::LETHAL_OBSTACLE ||
      (center_cost == costmap_2d::NO_INFORMATION && !allow_unknown))
  {
    return false;
  }

  // ROS1 FootprintHelper may return a non-empty partial polygon when a later
  // vertex leaves the map. Check all transformed vertices before consuming
  // its rasterized cells so the whole body is always fail-closed at bounds.
  const double cos_yaw = std::cos(yaw);
  const double sin_yaw = std::sin(yaw);
  for (const auto & vertex : footprint) {
    const double world_x = x + cos_yaw * vertex.x - sin_yaw * vertex.y;
    const double world_y = y + sin_yaw * vertex.x + cos_yaw * vertex.y;
    unsigned int vertex_x = 0u;
    unsigned int vertex_y = 0u;
    if (!costmap.worldToMap(world_x, world_y, vertex_x, vertex_y)) {
      return false;
    }
  }

  const auto cells = footprint_helper.getFootprintCells(
    Eigen::Vector3f(
      static_cast<float>(x), static_cast<float>(y), static_cast<float>(yaw)),
    footprint, costmap, true);
  if (cells.empty()) {
    return false;
  }
  for (const auto & cell : cells) {
    if (cell.x < 0 || cell.y < 0 ||
        cell.x >= static_cast<int64_t>(costmap.getSizeInCellsX()) ||
        cell.y >= static_cast<int64_t>(costmap.getSizeInCellsY()))
    {
      return false;
    }
    const unsigned char cost = costmap.getCost(
      static_cast<unsigned int>(cell.x),
      static_cast<unsigned int>(cell.y));
    if (cost == costmap_2d::LETHAL_OBSTACLE ||
        (cost == costmap_2d::NO_INFORMATION && !allow_unknown))
    {
      return false;
    }
  }
  return true;
}

}  // namespace mppi::utils

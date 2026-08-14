// Copyright (c) 2020, Samsung Research America
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

#ifndef NAV2_SMAC_PLANNER__COLLISION_CHECKER_HPP_
#define NAV2_SMAC_PLANNER__COLLISION_CHECKER_HPP_

#include <memory>
#include <cstddef>
#include <cstdint>
#include <vector>

#include <base_local_planner/costmap_model.h>
#include <costmap_2d/cost_values.h>
#include <geometry_msgs/Point.h>

#include "nav2_costmap_2d/costmap_2d.hpp"
#include "nav2_smac_planner/constants.hpp"

namespace nav2_smac_planner
{

using Footprint = std::vector<geometry_msgs::Point>;

/**
 * @class nav2_smac_planner::GridCollisionChecker
 * @brief Nav2 Smac footprint collision checker adapted to a ROS 1 Costmap2D.
 */
class GridCollisionChecker
{
public:
  GridCollisionChecker(
    nav2_costmap_2d::Costmap2D * costmap,
    unsigned int num_quantizations);

  void setFootprint(
    const Footprint & footprint,
    const bool & radius,
    const double & possible_collision_cost);

  void setCollisionCheckResolution(double sample_step_m);

  bool inCollision(
    const float & x,
    const float & y,
    const float & theta,
    const bool & traverse_unknown);

  bool inCollisionAtYaw(
    const float & x,
    const float & y,
    const double yaw,
    const bool & traverse_unknown);

  bool inCollision(
    const unsigned int & i,
    const bool & traverse_unknown);

  /**
   * @brief Continuously check a rigid footprint between two SE(2) poses.
   *
   * Nav2's adjacent lattice / analytic states are adaptively subdivided until
   * every corresponding polygon vertex moves no farther than the configured
   * collision-check resolution. Each interpolated state is then evaluated by
   * the Nav2 footprint checker. This also covers pure rotations.
   */
  bool inCollisionContinuous(
    const float & x0, const float & y0, const double yaw0,
    const float & x1, const float & y1, const double yaw1,
    const bool & traverse_unknown,
    float * max_cost = nullptr);

  float getCost();
  std::vector<float> & getPrecomputedAngles() {return angles_;}
  nav2_costmap_2d::Costmap2D * getCostmap() {return costmap_;}
  nav2_costmap_2d::Costmap2D * getCostmapROS() {return costmap_;}
  double getFootprintRadius() const {return footprint_radius_;}
  double getCollisionCheckResolution() const {return collision_check_resolution_;}
  bool outsideRange(const unsigned int & max, const float & value);

private:
  double maxFootprintVertexDisplacement(
    const float & x0, const float & y0, const double yaw0,
    const float & x1, const float & y1, const double yaw1) const;

  bool footprintCollision(
    const float & x, const float & y, const double yaw,
    const bool & traverse_unknown,
    float * max_footprint_cost = nullptr);

  void rebuildObstaclePrefixes();
  bool hardObstacleMayIntersect(
    double minimum_x, double minimum_y,
    double maximum_x, double maximum_y,
    bool traverse_unknown) const;

  nav2_costmap_2d::Costmap2D * costmap_{nullptr};
  std::unique_ptr<base_local_planner::CostmapModel> world_model_;
  std::vector<Footprint> oriented_footprints_;
  Footprint unoriented_footprint_;
  float center_cost_{FREE_COST};
  bool footprint_is_radius_{false};
  std::vector<float> angles_;
  float possible_collision_cost_{-1.0f};
  double footprint_radius_{0.0};
  double collision_check_resolution_{0.01};
  std::vector<std::uint32_t> lethal_prefix_;
  std::vector<std::uint32_t> blocked_prefix_;
  std::size_t lethal_prefix_stride_{0u};
  bool lethal_prefix_valid_{false};
};

}  // namespace nav2_smac_planner

#endif  // NAV2_SMAC_PLANNER__COLLISION_CHECKER_HPP_

#pragma once

#include <memory>
#include <string>

#include "costmap_2d/inflation_layer.h"
#include <costmap_2d/footprint.h>

#include "mppi_controller/critic_function.hpp"
#include "mppi_controller/models/state.hpp"
#include "mppi_controller/tools/utils.hpp"

namespace mppi::critics
{

/**
 * @class mppi::critics::CostCritic
 * @brief Critic objective function 使用costmap的代价避障
 */
class CostCritic : public CriticFunction
{
public:
  /**
    * @brief Initialize critic
    */
  void initialize() override;

  /**
   * @brief Evaluate cost related to obstacle avoidance
   *
   * @param costs [out] add obstacle cost values to this tensor
   */
  void score(CriticData & data) override;

protected:
  /**
    * @brief  检查代价值是否表示碰撞, 不考虑footprint
    * @param cost Point cost at pose center
    * @param x X of pose
    * @param y Y of pose
    * @param theta theta of pose
    * @return bool if in collision
    */
  inline bool inCollision(float cost, float x, float y, float theta)
  {
    // If consider_footprint_ check footprint scort for collision
    float score_cost = cost;
    // if (consider_footprint_ &&
    //   (cost >= possible_collision_cost_ || possible_collision_cost_ < 1.0f))
    // {
    //   score_cost = static_cast<float>(collision_checker_.footprintCostAtPose(
    //       static_cast<double>(x), static_cast<double>(y), static_cast<double>(theta),
    //       costmap_ros_->getRobotFootprint()));
    // }

    switch (static_cast<unsigned char>(score_cost)) {
      case (costmap_2d::LETHAL_OBSTACLE):
        return true;
      case (costmap_2d::INSCRIBED_INFLATED_OBSTACLE):
        return consider_footprint_ ? false : true;
      case (costmap_2d::NO_INFORMATION):
        return is_tracking_unknown_ ? false : true;
    }

    return false;
  }

  /**
    * @brief (默认不使用该函数)找到机器人在任何方向下可能处于碰撞的最小代价
    * @param costmap Costmap2DROS to get minimum inscribed cost (e.g. 128 in inflation layer documentation)
    * @return 外切半径的代价,任何更高的代价都需要进行完整的足迹碰撞检查,因为机器人的某些元素可能处于碰撞状态
    */
  inline float findCircumscribedCost(std::shared_ptr<costmap_2d::Costmap2DROS> costmap);

  /**
    * @brief An implementation of worldToMap fully using floats
    * @param wx Float world X coord
    * @param wy Float world Y coord
    * @param mx unsigned int map X coord
    * @param my unsigned into map Y coord
    * @return if successsful
    */
  inline bool worldToMapFloat(float wx, float wy, unsigned int & mx, unsigned int & my) const
  {
    if (wx < origin_x_ || wy < origin_y_) {
      return false;
    }

    mx = static_cast<unsigned int>((wx - origin_x_) / resolution_);
    my = static_cast<unsigned int>((wy - origin_y_) / resolution_);

    if (mx < size_x_ && my < size_y_) {
      return true;
    }
    return false;
  }

  /**
    * @brief A local implementation of getIndex
    * @param mx unsigned int map X coord
    * @param my unsigned into map Y coord
    * @return Index
    */
  inline unsigned int getIndex(unsigned int mx, unsigned int my) const
  {
    return my * size_x_ + mx;
  }

  float possible_collision_cost_;

  bool consider_footprint_{false};
  bool is_tracking_unknown_{true};
  float circumscribed_radius_{0.0f};
  float circumscribed_cost_{0.0f};
  float collision_cost_{0.0f};
  float critical_cost_{0.0f};
  float weight_{0};
  int trajectory_point_step_;

  float origin_x_, origin_y_, resolution_;
  int size_x_, size_y_;

  float near_goal_distance_;
  std::string inflation_layer_name_;

  int power_{0};
};

}  // namespace mppi::critics

#pragma once

#include <memory>
#include <string>

#include "costmap_2d/footprint.h"
#include "costmap_2d/inflation_layer.h"

#include "mppi_controller/critic_function.hpp"
#include "mppi_controller/models/state.hpp"
#include "mppi_controller/tools/utils.hpp"

namespace mppi::critics
{

/**
 * @class mppi::critics::ConstraintCritic
 * @brief Critic objective function for avoiding obstacles, allowing it to deviate off
 * the planned path. This is important to tune in tandem with PathAlign to make a balance
 * between path-tracking and dynamic obstacle avoidance capabilities as desirable for a
 * particular application  暂时搁置
 */
class ObstaclesCritic : public CriticFunction
{
public:
  /**
    * @brief Initialize critic
    */
  void initialize() override;

  /**
   * @brief 评估避障相关的代价
   *
   * @param costs [out] add obstacle cost values to this tensor
   */
  void score(CriticData & data) override;

protected:
  /**
    * @brief 检查是否发生碰撞
    * @param cost Costmap cost
    * @return bool if in collision
    */
  inline bool inCollision(float cost) const;

  /**
    * @brief cost at a robot pose
    * @param x X of pose
    * @param y Y of pose
    * @param theta theta of pose
    * @return Collision information at pose
    */
  inline CollisionCost costAtPose(float x, float y, float theta);

  /**
    * @brief Distance to obstacle from cost
    * @param cost Costmap cost
    * @return float Distance to the obstacle represented by cost
    */
  inline float distanceToObstacle(const CollisionCost & cost);

  /**
    * @brief Find the min cost of the inflation decay function for which the robot MAY be
    * in collision in any orientation 寻找膨胀衰减函数的最小代价，使得机器人在任意方向上可能发生碰撞。
    * @param costmap Costmap2DROS to get minimum inscribed cost (e.g. 128 in inflation layer documentation)
    * @return 若代价值高于此值，则需要进行完整的足迹碰撞检测，因为机器人的某些部分可能已处于碰撞状态。
    */
  float findCircumscribedCost(std::shared_ptr<costmap_2d::Costmap2DROS> costmap);

protected:
  bool consider_footprint_{true};
  float collision_cost_{0};
  float inflation_scale_factor_{0}, inflation_radius_{0};

  float possible_collision_cost_;
  float collision_margin_distance_;
  float near_goal_distance_;
  float circumscribed_cost_{0}, circumscribed_radius_{0};

  int power_{0};
  float repulsion_weight_, critical_weight_{0};
  std::string inflation_layer_name_;
};

}  // namespace mppi::critics


#pragma once

#include "mppi_controller/critic_function.hpp"
#include "mppi_controller/models/state.hpp"
#include "mppi_controller/tools/utils.hpp"

namespace mppi::critics
{

/**
 * @class mppi::critics::ConstraintCritic
 * @brief Critic objective function for following the path approximately
 * To allow for deviation from path in case of dynamic obstacles. Path Align
 * is what aligns the trajectories to the path more or less precisely, if desirable.
 * A higher weight here with an offset > 1 will accelerate the samples to full speed
 * faster and push the follow point further ahead, creating some shortcutting.
 * 该目标函数用于近似跟踪路径，允许在有动态障碍时偏离路径.
 * Path Align用于精确跟踪路径；
 * Path Follow会加速，尽可能快得结束路径；
 */
class PathFollowCritic : public CriticFunction
{
public:
  void initialize() override;

  /**
   * @brief Evaluate cost related to robot orientation at goal pose
   * (considered only if robot near last goal in current plan)
   * 评估候选轨迹的终点与参考路径上目标点的距离偏差
   * @param costs [out] add goal angle cost values to this tensor
   */
  void score(CriticData & data) override;

protected:
  float threshold_to_consider_{0};
  int offset_from_furthest_{0};

  int power_{0};
  float weight_{0};
};

}  // namespace mppi::critics

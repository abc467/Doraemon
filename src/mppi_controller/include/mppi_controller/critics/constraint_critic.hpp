#pragma once

#include "mppi_controller/critic_function.hpp"
#include "mppi_controller/models/state.hpp"
#include "mppi_controller/tools/utils.hpp"

namespace mppi::critics
{

/**
 * @class mppi::critics::ConstraintCritic
 * @brief Critic 目标函数确保可行性约束
 */
class ConstraintCritic : public CriticFunction
{
public:
  void initialize() override;

  /**
   * @brief Evaluate cost related to goal following
   *
   * @param costs [out] add reference cost values to this tensor
   */
  void score(CriticData & data) override;

  float getMaxVelConstraint() {return max_vel_;}
  float getMinVelConstraint() {return min_vel_;}

protected:
  int power_{0};
  float weight_{0};
  float min_vel_;
  float max_vel_;
};

}  // namespace mppi::critics

#pragma once

#include "mppi_controller/critic_function.hpp"
#include "mppi_controller/tools/utils.hpp"

namespace mppi::critics
{

/**
 * @class mppi::critics::ConstraintCritic
 * @brief Critic objective function for penalizing wiggling/twirling
 */
class TwirlingCritic : public CriticFunction
{
public:
  /**
    * @brief Initialize critic
    */
  void initialize() override;

  /**
   * @brief Evaluate cost related to robot orientation at goal pose
   * (considered only if robot near last goal in current plan)
   * 在不接近目标时，抑制不必要的转向
   *
   * @param costs [out] add goal angle cost values to this tensor
   */
  void score(CriticData & data) override;

protected:
  int power_{0};
  float weight_{0};
  float threshold_to_consider_{0};
};

}  // namespace mppi::critics


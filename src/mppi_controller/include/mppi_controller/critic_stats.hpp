#pragma once

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <limits>

#include <Eigen/Dense>

namespace mppi
{

/**
 * @brief Low-overhead accumulator for the additive cost contribution of one critic.
 *
 * Statistics are only collected when explicitly enabled and a ROS subscriber is
 * present.  Keeping this helper independent from ROS makes its numerical contract
 * directly testable.
 */
struct CriticCostAccumulator
{
  void add(
    const Eigen::ArrayXf & costs_before,
    const Eigen::ArrayXf & costs_after,
    double elapsed_ms,
    float change_epsilon = 1.0e-6f)
  {
    if (costs_before.size() != costs_after.size()) {
      nonfinite = true;
      return;
    }

    ++evaluations;
    elapsed_ms_sum += elapsed_ms;
    bool evaluation_changed = false;
    for (Eigen::Index index = 0; index < costs_after.size(); ++index) {
      const float delta = costs_after(index) - costs_before(index);
      ++samples;
      if (!std::isfinite(delta)) {
        nonfinite = true;
        continue;
      }
      cost_sum += static_cast<double>(delta);
      cost_min = std::min(cost_min, delta);
      cost_max = std::max(cost_max, delta);
      if (std::fabs(delta) > change_epsilon) {
        ++changed_samples;
        evaluation_changed = true;
      }
    }
    if (evaluation_changed) {
      ++active_evaluations;
    }
  }

  double meanCost() const
  {
    return samples == 0u ? 0.0 : cost_sum / static_cast<double>(samples);
  }

  double changedRatio() const
  {
    return samples == 0u ? 0.0 :
      static_cast<double>(changed_samples) / static_cast<double>(samples);
  }

  double meanElapsedMs() const
  {
    return evaluations == 0u ? 0.0 :
      elapsed_ms_sum / static_cast<double>(evaluations);
  }

  float minimumCost() const
  {
    return samples == 0u || !std::isfinite(cost_min) ? 0.0f : cost_min;
  }

  float maximumCost() const
  {
    return samples == 0u || !std::isfinite(cost_max) ? 0.0f : cost_max;
  }

  void reset()
  {
    *this = CriticCostAccumulator{};
  }

  std::size_t evaluations{0u};
  std::size_t active_evaluations{0u};
  std::size_t samples{0u};
  std::size_t changed_samples{0u};
  double cost_sum{0.0};
  double elapsed_ms_sum{0.0};
  float cost_min{std::numeric_limits<float>::infinity()};
  float cost_max{-std::numeric_limits<float>::infinity()};
  bool nonfinite{false};
};

}  // namespace mppi

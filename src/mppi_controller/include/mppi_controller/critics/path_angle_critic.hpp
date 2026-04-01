#pragma once

#include <string>
#include "mppi_controller/critic_function.hpp"
#include "mppi_controller/models/state.hpp"
#include "mppi_controller/tools/utils.hpp"
#include "nav_core/base_local_planner.h"

namespace mppi::critics
{

/**
 * @brief Enum type for different modes of operation
 */
enum class PathAngleMode
{
  FORWARD_PREFERENCE = 0, // 朝向路径目标点方向跟踪
  NO_DIRECTIONAL_PREFERENCE = 1,
  CONSIDER_FEASIBLE_PATH_ORIENTATIONS = 2 // 精确跟踪路径终点方向
};

/**
 * @brief Method to convert mode enum to string for printing
 */
std::string modeToStr(const PathAngleMode & mode)
{
  if (mode == PathAngleMode::FORWARD_PREFERENCE) {
    return "Forward Preference";
  } else if (mode == PathAngleMode::CONSIDER_FEASIBLE_PATH_ORIENTATIONS) {
    return "Consider Feasible Path Orientations";
  } else if (mode == PathAngleMode::NO_DIRECTIONAL_PREFERENCE) {
    return "No Directional Preference";
  } else {
    return "Invalid mode!";
  }
}

/**
 * @class mppi::critics::ConstraintCritic
 * @brief Critic objective function for aligning to path in cases of extreme misalignment
 * or turning 轨迹终点的朝向是否与路径一致
 */
class PathAngleCritic : public CriticFunction
{
public:
  void initialize() override;

  /**
   * @brief Evaluate cost related to robot orientation at goal pose
   * (considered only if robot near last goal in current plan)
   *
   * @param costs [out] add goal angle cost values to this tensor
   */
  void score(CriticData & data) override;

protected:
  float max_angle_to_furthest_{0};
  float threshold_to_consider_{0};

  int offset_from_furthest_{0};
  bool reversing_allowed_{true};
  PathAngleMode mode_{0};

  int power_{0};
  float weight_{0};
};

}  // namespace mppi::critics


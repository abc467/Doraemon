
#include "mppi_controller/critics/goal_angle_critic.hpp"

namespace mppi::critics
{

void GoalAngleCritic::initialize()
{
  std::string param_prefix = name_ + "/";

  nh_.param(param_prefix + "cost_power", power_, 1);
  nh_.param(param_prefix + "cost_weight", weight_, 3.0f);
  nh_.param(param_prefix + "threshold_to_consider", threshold_to_consider_, 0.5f);
  nh_.param(
    param_prefix + "symmetric_yaw_tolerance",
    symmetric_yaw_tolerance_, false);

  ROS_INFO(
    "GoalAngleCritic instantiated with %d power, %f weight, and %f "
    "angular threshold.",
    power_, weight_, threshold_to_consider_);
}

void GoalAngleCritic::score(CriticData & data)
{
  if (!enabled_ || data.state.local_path_length > threshold_to_consider_ ||
      data.path.x.size() == 0)
  {
    return;
  }

  const auto goal_idx = data.path.x.size() - 1;
  const float goal_yaw = data.path.yaws(goal_idx);
  auto angular_distances = utils::shortest_angular_distance(
    data.trajectories.yaws, goal_yaw).abs().eval();
  if (symmetric_yaw_tolerance_) {
    const float symmetric_goal_yaw = static_cast<float>(
      angles::normalize_angle(goal_yaw + M_PI));
    angular_distances = angular_distances.min(
      utils::shortest_angular_distance(
        data.trajectories.yaws, symmetric_goal_yaw).abs().eval());
  }

  if(power_ > 1u) {
    data.costs += ((angular_distances.rowwise().mean()) * weight_).
      pow(power_).eval().cast<float>();
  } else {
    data.costs += ((angular_distances.rowwise().mean()) * weight_).
      eval().cast<float>();
  }
}

}  // namespace mppi::critics

#include <pluginlib/class_list_macros.hpp>

PLUGINLIB_EXPORT_CLASS(
  mppi::critics::GoalAngleCritic,
  mppi::critics::CriticFunction)

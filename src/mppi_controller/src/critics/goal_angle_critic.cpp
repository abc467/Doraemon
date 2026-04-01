
#include "mppi_controller/critics/goal_angle_critic.hpp"

namespace mppi::critics
{

void GoalAngleCritic::initialize()
{
  std::string param_prefix = name_ + "/";

  nh_.param(param_prefix + "cost_power", power_, 1);
  nh_.param(param_prefix + "cost_weight", weight_, 3.0f);
  nh_.param(param_prefix + "threshold_to_consider", threshold_to_consider_, 0.5f);

  ROS_INFO(
    "GoalAngleCritic instantiated with %d power, %f weight, and %f "
    "angular threshold.",
    power_, weight_, threshold_to_consider_);
}

void GoalAngleCritic::score(CriticData & data)
{
  if (!enabled_ || !utils::withinPositionGoalTolerance(
      threshold_to_consider_, data.state.pose.pose, data.goal))
  {
    return;
  }

  const auto goal_idx = data.path.x.size() - 1;
  const float goal_yaw = data.path.yaws(goal_idx);

  if(power_ > 1u) {
    data.costs += (((utils::shortest_angular_distance(data.trajectories.yaws, goal_yaw).abs()).
      rowwise().mean()) * weight_).pow(power_).eval().cast<float>();
  } else {
    data.costs += (((utils::shortest_angular_distance(data.trajectories.yaws, goal_yaw).abs()).
      rowwise().mean()) * weight_).eval().cast<float>();
  }
}

}  // namespace mppi::critics

#include <pluginlib/class_list_macros.hpp>

PLUGINLIB_EXPORT_CLASS(
  mppi::critics::GoalAngleCritic,
  mppi::critics::CriticFunction)

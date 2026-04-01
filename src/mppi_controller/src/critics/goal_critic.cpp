
#include "mppi_controller/critics/goal_critic.hpp"

namespace mppi::critics
{


void GoalCritic::initialize()
{
  std::string param_prefix = name_ + "/";

  nh_.param(param_prefix + "cost_power", power_, 1);
  nh_.param(param_prefix + "cost_weight", weight_, 5.0f);
  nh_.param(param_prefix + "threshold_to_consider", threshold_to_consider_, 1.4f);

  ROS_INFO("GoalCritic instantiated with %d power and %f weight.",
    power_, weight_);
}

void GoalCritic::score(CriticData & data)
{
  if (!enabled_ || !utils::withinPositionGoalTolerance(
      threshold_to_consider_, data.state.pose.pose, data.goal))
  {
    return;
  }

  const auto & goal_x = data.goal.position.x;
  const auto & goal_y = data.goal.position.y;

  const auto delta_x = data.trajectories.x - goal_x;
  const auto delta_y = data.trajectories.y - goal_y;

  if(power_ > 1u) {
    data.costs += (((delta_x.square() + delta_y.square()).sqrt()).rowwise().mean() *
      weight_).pow(power_);
  } else {
    data.costs += (((delta_x.square() + delta_y.square()).sqrt()).rowwise().mean() *
      weight_).eval();
  }
}

}  // namespace mppi::critics

#include <pluginlib/class_list_macros.hpp>

PLUGINLIB_EXPORT_CLASS(mppi::critics::GoalCritic, mppi::critics::CriticFunction)


#include "mppi_controller/critics/twirling_critic.hpp"

#include <Eigen/Dense>

namespace mppi::critics
{

void TwirlingCritic::initialize()
{
  std::string param_prefix = name_ + "/";

  nh_.param(param_prefix + "cost_power", power_, 1);
  nh_.param(param_prefix + "cost_weight", weight_, 10.0f);
  // ROS1 has no GoalChecker object in CriticData. Read the adapter's goal
  // tolerance once to preserve upstream's rule of disabling twirling cost
  // inside the goal position tolerance.
  nh_.param("goal_tolerance", goal_tolerance_, 0.2f);

  ROS_INFO("TwirlingCritic instantiated with %d power and %f weight.", power_, weight_);
}

void TwirlingCritic::score(CriticData & data)
{
  if (!enabled_ || data.state.local_path_length < goal_tolerance_) {
    return;
  }
  // 抑制不必要的转向
  if (power_ > 1u) {
    data.costs += ((data.state.wz.abs().rowwise().mean()) * weight_).pow(power_).eval();
  } else {
    data.costs += ((data.state.wz.abs().rowwise().mean()) * weight_).eval();
  }
}

}  // namespace mppi::critics

#include <pluginlib/class_list_macros.hpp>

PLUGINLIB_EXPORT_CLASS(
  mppi::critics::TwirlingCritic,
  mppi::critics::CriticFunction)

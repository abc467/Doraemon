
#include "mppi_controller/critics/prefer_forward_critic.hpp"

#include <Eigen/Dense>

namespace mppi::critics
{

void PreferForwardCritic::initialize()
{
  std::string param_prefix = name_ + "/";

  nh_.param(param_prefix + "cost_power", power_, 1);
  nh_.param(param_prefix + "cost_weight", weight_, 5.0f);
  nh_.param(param_prefix + "threshold_to_consider", threshold_to_consider_, 0.5f);

  ROS_INFO("PreferForwardCritic instantiated with %d power and %f weight.", power_, weight_);
}

void PreferForwardCritic::score(CriticData & data)
{
  if (!enabled_ || utils::withinPositionGoalTolerance(
      threshold_to_consider_, data.state.pose.pose, data.goal))
  {
    return;
  }

  if (power_ > 1u) {
    // 提取后退速度分量: 若速度大于0则为0，若小于0则转化为正值
    data.costs += (
      (data.state.vx.unaryExpr([&](const float & x){return std::max(-x, 0.0f);}) *
      data.model_dt).rowwise().sum() * weight_).pow(power_);
  } else {
    data.costs += (data.state.vx.unaryExpr([&](const float & x){return std::max(-x, 0.0f);}) *
      data.model_dt).rowwise().sum() * weight_;
  }
}

}  // namespace mppi::critics

#include <pluginlib/class_list_macros.hpp>

PLUGINLIB_EXPORT_CLASS(
  mppi::critics::PreferForwardCritic,
  mppi::critics::CriticFunction)

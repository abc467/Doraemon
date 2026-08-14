
#include "mppi_controller/critics/constraint_critic.hpp"

#include <algorithm>

namespace mppi::critics
{

void ConstraintCritic::initialize()
{
  std::string param_prefix = name_ + "/";

  int power = 1;
  nh_.param(param_prefix + "cost_power", power, 1);
  power_ = static_cast<unsigned int>(std::max(1, power));
  nh_.param(param_prefix + "cost_weight", weight_, 4.0f);
  nh_.param("vx_max", vx_max_, 0.5f);
  nh_.param("vy_max", vy_max_, 0.0f);
  nh_.param("vx_min", vx_min_, -0.35f);

  ROS_INFO_STREAM("ConstraintCritic instantiated with power=" << power_
    << ", weight=" << weight_ << ", vx=[" << vx_min_ << ", " << vx_max_
    << "], |vy|<=" << vy_max_);
}

void ConstraintCritic::score(CriticData & data)
{
  if (!enabled_) {
    return;
  }

  // 差速模型
  auto diff = dynamic_cast<DiffDriveMotionModel *>(data.motion_model.get());
  if (diff != nullptr) {
    if (power_ > 1u) {
      data.costs += (((((data.state.vx - vx_max_).max(0.0f) + (vx_min_ - data.state.vx).
        max(0.0f)) * data.model_dt).rowwise().sum().eval()) * weight_).pow(power_).eval();
    } else {
      data.costs += (((((data.state.vx - vx_max_).max(0.0f) + (vx_min_ - data.state.vx).
        max(0.0f)) * data.model_dt).rowwise().sum().eval()) * weight_).eval();
    }
    return;
  }

  // 全向模型
  auto omni = dynamic_cast<OmniMotionModel *>(data.motion_model.get());
  if (omni != nullptr) {
    auto & vx = data.state.vx;
    auto & vy = data.state.vy;
    if (power_ > 1u) {
      data.costs += (((((vx - vx_max_).max(0.0f) + (vx_min_ - vx).max(0.0f) +
        (vy.abs() - vy_max_).max(0.0f)) * data.model_dt).rowwise().sum().eval()) *
        weight_).pow(power_).eval();
    } else {
      data.costs += (((((vx - vx_max_).max(0.0f) + (vx_min_ - vx).max(0.0f) +
        (vy.abs() - vy_max_).max(0.0f)) * data.model_dt).rowwise().sum().eval()) *
        weight_).eval();
    }
    return;
  }
}

}  // namespace mppi::critics  

#include <pluginlib/class_list_macros.hpp>

PLUGINLIB_EXPORT_CLASS(mppi::critics::ConstraintCritic, mppi::critics::CriticFunction)

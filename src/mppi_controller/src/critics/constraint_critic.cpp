
#include "mppi_controller/critics/constraint_critic.hpp"

namespace mppi::critics
{

void ConstraintCritic::initialize()
{
  std::string param_prefix = name_ + "/";

  bool param_exists = true;

  param_exists &= nh_.param(param_prefix + "cost_power", power_, 1); 
  param_exists &= nh_.param(param_prefix + "cost_weight", weight_, 4.0f);

  float vx_max, vy_max, vx_min;
  param_exists &= nh_.param("vx_max", vx_max, 0.5f);
  param_exists &= nh_.param("vy_max", vy_max, 0.0f);
  param_exists &= nh_.param("vx_min", vx_min, -0.4f);

  if(!param_exists){
    ROS_WARN("ConstraintCritic param doesn't exist !!!");
  }else{
    ROS_WARN("ConstraintCritic param exist !!!");
  }

  const float min_sgn = vx_min > 0.0f ? 1.0f : -1.0f;
  max_vel_ = sqrtf(vx_max * vx_max + vy_max * vy_max);
  min_vel_ = min_sgn * sqrtf(vx_min * vx_min + vy_max * vy_max);

  ROS_INFO_STREAM("Constraint critic parameters: "
    << "max_vel: " << max_vel_ << " min_vel: " << min_vel_ << " weight: " << weight_ << " power: " << power_);
}

void ConstraintCritic::score(CriticData & data)
{
  if (!enabled_) {
    return;
  }

  // 差速模型
  auto diff = dynamic_cast<DiffDriveMotionModel *>(data.motion_model.get());
  if (diff != nullptr) {
    if (power_ > 1) {
      data.costs += (((((data.state.vx - max_vel_).max(0.0f) + (min_vel_ - data.state.vx).
        max(0.0f)) * data.model_dt).rowwise().sum().eval()) * weight_).pow(power_).eval();
    } else {
      data.costs += (((((data.state.vx - max_vel_).max(0.0f) + (min_vel_ - data.state.vx).
        max(0.0f)) * data.model_dt).rowwise().sum().eval()) * weight_).eval();
    }
    return;
  }

  // 全向模型
  auto omni = dynamic_cast<OmniMotionModel *>(data.motion_model.get());
  if (omni != nullptr) {
    auto & vx = data.state.vx;
    unsigned int n_rows = data.state.vx.rows();
    unsigned int n_cols = data.state.vx.cols();
    Eigen::ArrayXXf sgn(n_rows, n_cols);
    sgn = vx.unaryExpr([](const float x){return copysignf(1.0f, x);});

    auto vel_total = sgn * (data.state.vx.square() + data.state.vy.square()).sqrt();
    if (power_ > 1) {
      data.costs += ((((vel_total - max_vel_).max(0.0f) + (min_vel_ - vel_total).
        max(0.0f)) * data.model_dt).rowwise().sum().eval() * weight_).pow(power_).eval();
    } else {
      data.costs += ((((vel_total - max_vel_).max(0.0f) + (min_vel_ - vel_total).
        max(0.0f)) * data.model_dt).rowwise().sum().eval() * weight_).eval();
    } 
    return;
  }
}

}  // namespace mppi::critics  

#include <pluginlib/class_list_macros.hpp>

PLUGINLIB_EXPORT_CLASS(mppi::critics::ConstraintCritic, mppi::critics::CriticFunction)

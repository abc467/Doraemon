
#include "mppi_controller/critics/velocity_deadband_critic.hpp"

namespace mppi::critics
{

void VelocityDeadbandCritic::initialize()
{
  std::string param_prefix = name_ + "/";

  bool param_exists = true;

  param_exists = nh_.param(param_prefix + "cost_power", power_, 1);
  param_exists &= nh_.param(param_prefix + "cost_weight", weight_, 35.0f);
  if(!param_exists){
    ROS_WARN("VelocityDeadbandCritic param doesn't exist !!!");
  }else{
    ROS_WARN("VelocityDeadbandCritic param exist !!!");
  }

  // Recast double to float
  std::vector<double> deadband_velocities{0.0, 0.0, 0.0};
  nh_.param(param_prefix + "deadband_velocities", deadband_velocities, std::vector<double>{0.0, 0.0, 0.0});
  std::transform(
    deadband_velocities.begin(), deadband_velocities.end(), deadband_velocities_.begin(),
    [](double d) {return static_cast<float>(d);});

  ROS_INFO_STREAM( "VelocityDeadbandCritic instantiated with "
      << power_ << " power, " << weight_ << " weight, deadband_velocity ["
      << deadband_velocities_.at(0) << "," << deadband_velocities_.at(1) << ","
      << deadband_velocities_.at(2) << "]");
}

void VelocityDeadbandCritic::score(CriticData & data)
{
  if (!enabled_) {
    return;
  }

  if (data.motion_model->isHolonomic()) {
    if (power_ > 1u) {
      data.costs += ((((fabs(deadband_velocities_[0]) - data.state.vx.abs()).max(0.0f) +
        (fabs(deadband_velocities_[1]) - data.state.vy.abs()).max(0.0f) +
        (fabs(deadband_velocities_[2]) - data.state.wz.abs()).max(0.0f)) *
        data.model_dt).rowwise().sum() * weight_).pow(power_).eval();
    } else {
      data.costs += ((((fabs(deadband_velocities_[0]) - data.state.vx.abs()).max(0.0f) +
        (fabs(deadband_velocities_[1]) - data.state.vy.abs()).max(0.0f) +
        (fabs(deadband_velocities_[2]) - data.state.wz.abs()).max(0.0f)) *
        data.model_dt).rowwise().sum() * weight_).eval();
    }
    return;
  }

  if (power_ > 1u) {
    data.costs += ((((fabs(deadband_velocities_[0]) - data.state.vx.abs()).max(0.0f) +
      (fabs(deadband_velocities_[2]) - data.state.wz.abs()).max(0.0f)) *
      data.model_dt).rowwise().sum() * weight_).pow(power_).eval();
  } else {
    data.costs += ((((fabs(deadband_velocities_[0]) - data.state.vx.abs()).max(0.0f) +
      (fabs(deadband_velocities_[2]) - data.state.wz.abs()).max(0.0f)) *
      data.model_dt).rowwise().sum() * weight_).eval();
  }
  return;
}

}  // namespace mppi::critics

#include <pluginlib/class_list_macros.hpp>

PLUGINLIB_EXPORT_CLASS(mppi::critics::VelocityDeadbandCritic, mppi::critics::CriticFunction)

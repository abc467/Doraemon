
#include "mppi_controller/critics/path_follow_critic.hpp"

#include <Eigen/Dense>

namespace mppi::critics
{

void PathFollowCritic::initialize()
{
  std::string param_prefix = name_ + "/";

  bool param_exists = true;

  param_exists &= nh_.param(param_prefix + "threshold_to_consider", threshold_to_consider_, 1.4f);
  param_exists &= nh_.param(param_prefix + "offset_from_furthest", offset_from_furthest_, 6);
  param_exists &= nh_.param(param_prefix + "cost_power", power_, 1);
  param_exists &= nh_.param(param_prefix + "cost_weight", weight_, 5.0f);

  if(!param_exists){
    ROS_WARN("PathFollowCritic param doesn't exist !!!");
  }else{
    ROS_WARN("PathFollowCritic param exist !!!");
  }

}

void PathFollowCritic::score(CriticData & data)
{
  if (!enabled_ || data.path.x.size() < 2 ||
    utils::withinPositionGoalTolerance(threshold_to_consider_, data.state.pose.pose, data.goal))
  {
    return;
  }

  utils::setPathFurthestPointIfNotSet(data);
  utils::setPathCostsIfNotSet(data, costmap_ros_);
  const size_t path_size = data.path.x.size() - 1;

  // 取最远点并防止越界
  auto offsetted_idx = std::min(
    *data.furthest_reached_path_point + offset_from_furthest_, path_size);

  // Drive to the first valid path point, in case of dynamic obstacles on path
  // we want to drive past it, not through it
  // 向前寻找第一个有效的路径点（避开障碍物）
  bool valid = false;
  while (!valid && offsetted_idx < path_size - 1) {
    valid = (*data.path_pts_valid)[offsetted_idx];
    if (!valid) {
      offsetted_idx++;
    }
  }

  const auto path_x = data.path.x(offsetted_idx);
  const auto path_y = data.path.y(offsetted_idx);

  const int && rightmost_idx = data.trajectories.x.cols() - 1;
  const auto last_x = data.trajectories.x.col(rightmost_idx);
  const auto last_y = data.trajectories.y.col(rightmost_idx);

  const auto delta_x = last_x - path_x;
  const auto delta_y = last_y - path_y;
  if (power_ > 1u) {
    // 计算轨迹终点与目标点的距离并转化为代价
    data.costs += (((delta_x.square() + delta_y.square()).sqrt()) * weight_).pow(power_);
  } else {
    data.costs += ((delta_x.square() + delta_y.square()).sqrt()) * weight_;
  }
}

}  // namespace mppi::critics

#include <pluginlib/class_list_macros.hpp>

PLUGINLIB_EXPORT_CLASS(
  mppi::critics::PathFollowCritic,
  mppi::critics::CriticFunction)

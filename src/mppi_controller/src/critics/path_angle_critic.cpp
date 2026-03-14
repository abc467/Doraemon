#include "mppi_controller/critics/path_angle_critic.hpp"

#include <math.h>

namespace mppi::critics
{

void PathAngleCritic::initialize()
{
  float vx_min;
  nh_.param("vx_min", vx_min, -0.35f);
  if (fabs(vx_min) < 1e-6f) {  // zero
    reversing_allowed_ = false;
  } else if (vx_min < 0.0f) {   // reversing possible
    reversing_allowed_ = true;
  }

  std::string param_prefix = name_ + "/";

  nh_.param(param_prefix + "offset_from_furthest", offset_from_furthest_, 4);
  nh_.param(param_prefix + "cost_power", power_, 1);
  nh_.param(param_prefix + "cost_weight", weight_, 2.2f);
  nh_.param(param_prefix + "threshold_to_consider", threshold_to_consider_, 0.5f);
  nh_.param(param_prefix + "max_angle_to_furthest", max_angle_to_furthest_, 0.785398f);

  int mode = 0;
  nh_.param(param_prefix + "mode", mode, mode);
  mode_ = static_cast<PathAngleMode>(mode);
  // if (!reversing_allowed_ && mode_ == PathAngleMode::NO_DIRECTIONAL_PREFERENCE) {
  //   mode_ = PathAngleMode::FORWARD_PREFERENCE;
  //   ROS_WARN("Path angle mode set to no directional preference, but controller's settings "
  //     "don't allow for reversing! Setting mode to forward preference.");
  // }
  // ROS_INFO_STREAM("Path angle critic initialized with parameters: " << " power: " << power_);
}

void PathAngleCritic::score(CriticData & data)
{
  if (!enabled_ ||
    utils::withinPositionGoalTolerance(threshold_to_consider_, data.state.pose.pose, data.goal))
  {
    return;
  }

  utils::setPathFurthestPointIfNotSet(data);
  auto offsetted_idx = std::min(
    *data.furthest_reached_path_point + offset_from_furthest_,
      static_cast<size_t>(data.path.x.size()) - 1);

  const float goal_x = data.path.x(offsetted_idx);
  const float goal_y = data.path.y(offsetted_idx);
  const float goal_yaw = data.path.yaws(offsetted_idx);
  const geometry_msgs::Pose & pose = data.state.pose.pose;

  switch (mode_) {
    case PathAngleMode::FORWARD_PREFERENCE:
      if (utils::posePointAngle(pose, goal_x, goal_y, true) < max_angle_to_furthest_) {
        return;
      }
      break;
    case PathAngleMode::NO_DIRECTIONAL_PREFERENCE:
      if (utils::posePointAngle(pose, goal_x, goal_y, false) < max_angle_to_furthest_) {
        return;
      }
      break;
    case PathAngleMode::CONSIDER_FEASIBLE_PATH_ORIENTATIONS:
      if (utils::posePointAngle(pose, goal_x, goal_y, goal_yaw) < max_angle_to_furthest_) {
        return;
      }
      break;
    default:
      ROS_ERROR("Invalid path angle mode!");
  }

  int last_idx = data.trajectories.y.cols() - 1; // 轨迹终点列索引（最后一个点）
  auto diff_y = goal_y - data.trajectories.y.col(last_idx); // 目标点与轨迹终点的坐标差
  auto diff_x = goal_x - data.trajectories.x.col(last_idx);
  auto yaws_between_points = diff_y.binaryExpr(  // 计算目标点相对于轨迹终点的方位角
    diff_x, [&](const float & y, const float & x){return atan2f(y, x);}).eval();

  switch (mode_) {
    case PathAngleMode::FORWARD_PREFERENCE:
      {
        auto last_yaws = data.trajectories.yaws.col(last_idx);
        auto yaws = utils::shortest_angular_distance(
          last_yaws, yaws_between_points).abs();
        if (power_ > 1u) {
          data.costs += (yaws * weight_).pow(power_).cast<float>();
        } else {
          data.costs += (yaws * weight_).cast<float>();
        }
        return;
      }
    case PathAngleMode::NO_DIRECTIONAL_PREFERENCE:
      {
        auto last_yaws = data.trajectories.yaws.col(last_idx);
        auto yaws_between_points_corrected = utils::normalize_yaws_between_points(last_yaws,
          yaws_between_points);
        auto corrected_yaws = utils::shortest_angular_distance(
          last_yaws, yaws_between_points_corrected).abs();
        if (power_ > 1u) {
          data.costs += (corrected_yaws * weight_).pow(power_).cast<float>();
        } else {
          data.costs += (corrected_yaws * weight_).cast<float>();
        }
        return;
      }
    case PathAngleMode::CONSIDER_FEASIBLE_PATH_ORIENTATIONS:
      {
        auto last_yaws = data.trajectories.yaws.col(last_idx);
        auto yaws_between_points_corrected = utils::normalize_yaws_between_points(goal_yaw,
          yaws_between_points);
        auto corrected_yaws = utils::shortest_angular_distance(
          last_yaws, yaws_between_points_corrected).abs();
        if (power_ > 1u) {
          data.costs += (corrected_yaws * weight_).pow(power_).cast<float>();
        } else {
          data.costs += (corrected_yaws * weight_).cast<float>();
        }
        return;
      }
  }
}

}  // namespace mppi::critics

#include <pluginlib/class_list_macros.hpp>

PLUGINLIB_EXPORT_CLASS(
  mppi::critics::PathAngleCritic,
  mppi::critics::CriticFunction)

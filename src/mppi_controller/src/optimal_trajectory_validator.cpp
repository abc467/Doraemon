#include "mppi_controller/optimal_trajectory_validator.hpp"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <stdexcept>

#include <angles/angles.h>
#include <base_local_planner/footprint_helper.h>
#include <costmap_2d/cost_values.h>
#include <tf2/utils.h>
#include <pluginlib/class_list_macros.h>

#include "mppi_controller/tools/footprint_collision.hpp"

namespace mppi
{

void DefaultOptimalTrajectoryValidator::initialize(
  const ros::NodeHandle & nh, const std::string & name)
{
  nh.param(
    name + "/maximum_corner_motion", maximum_corner_motion_, 0.025);
  if (!std::isfinite(maximum_corner_motion_) || maximum_corner_motion_ <= 0.0) {
    throw std::invalid_argument(
      "TrajectoryValidator maximum_corner_motion must be positive");
  }
}

ValidationResult DefaultOptimalTrajectoryValidator::validate(
  const costmap_2d::Costmap2D & costmap,
  const std::vector<geometry_msgs::Point> & footprint,
  const geometry_msgs::Pose & initial_pose,
  const Eigen::ArrayXXf & trajectory,
  double maximum_corner_motion) const
{
  if (maximum_corner_motion <= 0.0) {
    maximum_corner_motion = maximum_corner_motion_;
  }
  if (footprint.size() < 3u || trajectory.cols() < 3 ||
      !std::isfinite(maximum_corner_motion) || maximum_corner_motion <= 0.0)
  {
    return ValidationResult::FAILURE;
  }

  double corner_radius = 0.0;
  for (const auto & point : footprint) {
    if (!std::isfinite(point.x) || !std::isfinite(point.y)) {
      return ValidationResult::FAILURE;
    }
    corner_radius = std::max(corner_radius, std::hypot(point.x, point.y));
  }
  const double interpolation_step = std::max(
    0.005, std::min(maximum_corner_motion, costmap.getResolution() * 0.5));
  base_local_planner::FootprintHelper footprint_helper;

  const auto pose_is_free = [&](double x, double y, double yaw) {
    return utils::isFootprintPoseHardCollisionFree(
      costmap, footprint, footprint_helper, x, y, yaw, false);
  };

  double previous_x = initial_pose.position.x;
  double previous_y = initial_pose.position.y;
  double previous_yaw = tf2::getYaw(initial_pose.orientation);
  if (!pose_is_free(previous_x, previous_y, previous_yaw)) {
    return ValidationResult::SOFT_RESET;
  }

  for (Eigen::Index index = 0; index < trajectory.rows(); ++index) {
    const double next_x = trajectory(index, 0);
    const double next_y = trajectory(index, 1);
    const double next_yaw = trajectory(index, 2);
    const double yaw_delta = angles::shortest_angular_distance(
      previous_yaw, next_yaw);
    const double corner_motion = std::hypot(
      next_x - previous_x, next_y - previous_y) +
      corner_radius * std::fabs(yaw_delta);
    const int samples = std::max(
      1, static_cast<int>(std::ceil(corner_motion / interpolation_step)));
    for (int sample = 1; sample <= samples; ++sample) {
      const double ratio = static_cast<double>(sample) /
        static_cast<double>(samples);
      if (!pose_is_free(
          previous_x + (next_x - previous_x) * ratio,
          previous_y + (next_y - previous_y) * ratio,
          previous_yaw + yaw_delta * ratio))
      {
        return ValidationResult::SOFT_RESET;
      }
    }
    previous_x = next_x;
    previous_y = next_y;
    previous_yaw += yaw_delta;
  }
  return ValidationResult::SUCCESS;
}

}  // namespace mppi

PLUGINLIB_EXPORT_CLASS(
  mppi::DefaultOptimalTrajectoryValidator, mppi::OptimalTrajectoryValidator)

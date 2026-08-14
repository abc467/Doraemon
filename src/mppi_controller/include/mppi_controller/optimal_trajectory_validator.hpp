#pragma once

#include <Eigen/Dense>

#include <memory>
#include <string>
#include <vector>

#include <costmap_2d/costmap_2d.h>
#include <geometry_msgs/Point.h>
#include <geometry_msgs/Pose.h>
#include <ros/node_handle.h>

namespace mppi
{

/** Result contract used by trajectory validator plugins. */
enum class ValidationResult
{
  SUCCESS,
  SOFT_RESET,
  FAILURE
};

/**
 * @brief Final safety gate for the post-filter MPPI trajectory.
 *
 * Candidate critics shape and reject samples. This validator independently
 * checks the one trajectory that will actually produce the command, including
 * the swept motion between model samples and the filled robot footprint.
 */
class OptimalTrajectoryValidator
{
public:
  using Ptr = std::unique_ptr<OptimalTrajectoryValidator>;

  virtual ~OptimalTrajectoryValidator() = default;

  virtual void initialize(
    const ros::NodeHandle & nh, const std::string & name) = 0;

  virtual ValidationResult validate(
    const costmap_2d::Costmap2D & costmap,
    const std::vector<geometry_msgs::Point> & footprint,
    const geometry_msgs::Pose & initial_pose,
    const Eigen::ArrayXXf & trajectory,
    double maximum_corner_motion = -1.0) const = 0;
};

/** Default continuous filled-footprint validator. */
class DefaultOptimalTrajectoryValidator final : public OptimalTrajectoryValidator
{
public:
  void initialize(
    const ros::NodeHandle & nh, const std::string & name) override;

  ValidationResult validate(
    const costmap_2d::Costmap2D & costmap,
    const std::vector<geometry_msgs::Point> & footprint,
    const geometry_msgs::Pose & initial_pose,
    const Eigen::ArrayXXf & trajectory,
    double maximum_corner_motion = -1.0) const override;

private:
  double maximum_corner_motion_{0.025};
};

}  // namespace mppi

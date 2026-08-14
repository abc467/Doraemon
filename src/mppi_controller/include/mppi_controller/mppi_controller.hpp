#pragma once

#include <cstddef>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <angles/angles.h>
#include <base_local_planner/odometry_helper_ros.h>
#include <costmap_2d/costmap_2d_ros.h>
#include <geometry_msgs/PoseStamped.h>
#include <geometry_msgs/Twist.h>
#include <nav_core/base_local_planner.h>
#include <nav_msgs/Path.h>
#include <ros/ros.h>
#include <std_msgs/Float32.h>
#include <std_srvs/Trigger.h>
#include <tf2_ros/buffer.h>

#include <mbf_abstract_core/plan_execution_context.h>

#include "mppi_controller/optimizer.hpp"
#include "mppi_controller/tools/path_handler.hpp"
#include "mppi_controller/tools/goal_reached_evaluator.hpp"
#include "mppi_controller/tools/trajectory_visualizer.hpp"

namespace local_planner
{

/**
 * @brief ROS1 adapter around the Nav2-style MPPI optimizer.
 *
 * The adapter intentionally has no path phase state machine.  A State Lattice
 * plan remains one continuous SE(2) reference and PathHandler selects/prunes
 * the local window each cycle, as in upstream Nav2.  Motion feasibility,
 * obstacle avoidance, path tracking and goal approach are optimized together.
 */
class MPPIController : public nav_core::BaseLocalPlanner,
  public mbf_abstract_core::ControllerGoalToleranceAware
{
public:
  MPPIController() = default;
  MPPIController(
    std::string name, tf2_ros::Buffer * tf,
    costmap_2d::Costmap2DROS * costmap_ros);
  ~MPPIController() override = default;

  void initialize(
    std::string name, tf2_ros::Buffer * tf,
    costmap_2d::Costmap2DROS * costmap_ros) override;
  bool setPlan(
    const std::vector<geometry_msgs::PoseStamped> & plan) override;
  bool computeVelocityCommands(geometry_msgs::Twist & command) override;
  bool isGoalReached() override;
  bool isGoalReachedWithTolerances(
    double distance_tolerance, double angle_tolerance) override;

private:
  struct AdapterParameters
  {
    bool visualize{false};
    bool timing_diagnostics{false};
    double goal_tolerance{0.20};
    double angle_tolerance{0.20};
    double trans_stopped_velocity{0.02};
    double rot_stopped_velocity{0.03};
    double goal_stopped_time{0.30};
    std::string speed_limit_topic{"/coverage_executor/speed_limit_scale"};
  };

  AdapterParameters readAdapterParameters() const;
  void applyAdapterParameters(const AdapterParameters & parameters);
  void subscribeToSpeedLimit();
  bool reloadParameters(
    std_srvs::Trigger::Request & request,
    std_srvs::Trigger::Response & response);
  bool poseWithinGoalTolerance(
    const geometry_msgs::Pose & robot_pose,
    const geometry_msgs::Pose & goal_pose) const;
  void visualize(
    nav_msgs::Path path, const Eigen::ArrayXXf & optimal_trajectory,
    const ros::Time & command_stamp);
  static void setZeroCommand(geometry_msgs::Twist & command);
  void speedLimitScaleCallback(const std_msgs::Float32ConstPtr & message);

  std::shared_ptr<costmap_2d::Costmap2DROS> costmap_ros_;
  std::shared_ptr<tf2_ros::Buffer> tf_buffer_;
  std::shared_ptr<base_local_planner::OdometryHelperRos> odom_helper_;
  ros::Subscriber speed_limit_subscriber_;
  ros::ServiceServer reload_parameters_service_;
  ros::NodeHandle private_nh_;
  std::string name_;

  std::unique_ptr<mppi::Optimizer> optimizer_;
  mppi::PathHandler path_handler_;
  mppi::TrajectoryVisualizer trajectory_visualizer_;
  mppi::GoalReachedEvaluator goal_reached_evaluator_;
  nav_msgs::Path global_path_;

  bool initialized_{false};
  bool goal_reached_{false};
  bool visualize_{false};
  bool timing_diagnostics_{false};
  std::size_t timing_cycles_{0u};
  double path_time_total_ms_{0.0};
  double optimizer_time_total_ms_{0.0};
  mutable std::mutex controller_mutex_;

  double goal_tolerance_{0.20};
  double angle_tolerance_{0.20};
  double trans_stopped_velocity_{0.02};
  double rot_stopped_velocity_{0.03};
  double goal_stopped_time_{0.30};
  double speed_limit_scale_{1.0};
  std::string speed_limit_topic_;

};

}  // namespace local_planner

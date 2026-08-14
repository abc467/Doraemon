#include "mppi_controller/mppi_controller.hpp"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <stdexcept>
#include <utility>

#include <pluginlib/class_list_macros.h>
#include <tf2/utils.h>

PLUGINLIB_EXPORT_CLASS(local_planner::MPPIController, nav_core::BaseLocalPlanner)

namespace local_planner
{

MPPIController::MPPIController(
  std::string name, tf2_ros::Buffer * tf,
  costmap_2d::Costmap2DROS * costmap_ros)
{
  initialize(std::move(name), tf, costmap_ros);
}

void MPPIController::initialize(
  std::string name, tf2_ros::Buffer * tf,
  costmap_2d::Costmap2DROS * costmap_ros)
{
  if (initialized_) {
    ROS_WARN("MPPIController is already initialized");
    return;
  }
  if (tf == nullptr || costmap_ros == nullptr) {
    throw std::invalid_argument("MPPIController requires TF and costmap instances");
  }

  costmap_ros_ = std::shared_ptr<costmap_2d::Costmap2DROS>(
    costmap_ros, [](costmap_2d::Costmap2DROS *) {});
  tf_buffer_ = std::shared_ptr<tf2_ros::Buffer>(tf, [](tf2_ros::Buffer *) {});

  name_ = name;
  private_nh_ = ros::NodeHandle("~/" + name_);
  applyAdapterParameters(readAdapterParameters());

  std::string odom_topic("/odom");
  private_nh_.param("odom_topic", odom_topic, odom_topic);
  odom_helper_ = std::make_shared<base_local_planner::OdometryHelperRos>(
    odom_topic);

  optimizer_ = std::make_unique<mppi::Optimizer>();
  optimizer_->initialize(private_nh_, name_, costmap_ros_);
  path_handler_.initialize(private_nh_, name_, costmap_ros_, tf_buffer_);
  trajectory_visualizer_.initialize(
    private_nh_, name_, costmap_ros_->getGlobalFrameID());

  initialized_ = true;
  subscribeToSpeedLimit();
  reload_parameters_service_ = private_nh_.advertiseService(
    "reload_parameters", &MPPIController::reloadParameters, this);
  ROS_INFO(
    "MPPIController initialized with goal tolerance %.3fm / %.3frad and "
    "stopped gate %.3fm/s / %.3frad/s for %.3fs",
    goal_tolerance_, angle_tolerance_, trans_stopped_velocity_,
    rot_stopped_velocity_, goal_stopped_time_);
}

MPPIController::AdapterParameters MPPIController::readAdapterParameters() const
{
  AdapterParameters parameters;
  private_nh_.param("visualize", parameters.visualize, false);
  private_nh_.param("timing_diagnostics", parameters.timing_diagnostics, false);
  private_nh_.param("goal_tolerance", parameters.goal_tolerance, 0.20);
  private_nh_.param("angle_tolerance", parameters.angle_tolerance, 0.20);
  private_nh_.param(
    "trans_stopped_velocity", parameters.trans_stopped_velocity, 0.02);
  private_nh_.param(
    "rot_stopped_velocity", parameters.rot_stopped_velocity, 0.03);
  private_nh_.param("goal_stopped_time", parameters.goal_stopped_time, 0.30);
  private_nh_.param(
    "speed_limit_topic", parameters.speed_limit_topic,
    std::string("/coverage_executor/speed_limit_scale"));
  const auto require_nonnegative_finite = [](
    double value, const char * parameter_name) {
      if (!std::isfinite(value) || value < 0.0) {
        throw std::invalid_argument(
          std::string(parameter_name) + " must be finite and nonnegative");
      }
      return value;
    };
  parameters.goal_tolerance = require_nonnegative_finite(
    parameters.goal_tolerance, "goal_tolerance");
  parameters.angle_tolerance = require_nonnegative_finite(
    parameters.angle_tolerance, "angle_tolerance");
  parameters.trans_stopped_velocity = require_nonnegative_finite(
    parameters.trans_stopped_velocity, "trans_stopped_velocity");
  parameters.rot_stopped_velocity = require_nonnegative_finite(
    parameters.rot_stopped_velocity, "rot_stopped_velocity");
  parameters.goal_stopped_time = require_nonnegative_finite(
    parameters.goal_stopped_time, "goal_stopped_time");
  return parameters;
}

void MPPIController::applyAdapterParameters(
  const AdapterParameters & parameters)
{
  visualize_ = parameters.visualize;
  timing_diagnostics_ = parameters.timing_diagnostics;
  goal_tolerance_ = parameters.goal_tolerance;
  angle_tolerance_ = parameters.angle_tolerance;
  trans_stopped_velocity_ = parameters.trans_stopped_velocity;
  rot_stopped_velocity_ = parameters.rot_stopped_velocity;
  goal_stopped_time_ = parameters.goal_stopped_time;
  speed_limit_topic_ = parameters.speed_limit_topic;
  goal_reached_evaluator_.configure(
    goal_tolerance_, angle_tolerance_, trans_stopped_velocity_,
    rot_stopped_velocity_, goal_stopped_time_);
}

void MPPIController::subscribeToSpeedLimit()
{
  speed_limit_subscriber_.shutdown();
  if (!speed_limit_topic_.empty()) {
    speed_limit_subscriber_ = private_nh_.subscribe<std_msgs::Float32>(
      speed_limit_topic_, 1, &MPPIController::speedLimitScaleCallback, this);
  }
}

bool MPPIController::reloadParameters(
  std_srvs::Trigger::Request &,
  std_srvs::Trigger::Response & response)
{
  std::lock_guard<std::mutex> lock(controller_mutex_);
  if (!initialized_ || !optimizer_) {
    response.success = false;
    response.message = "MPPIController is not initialized";
    return true;
  }

  try {
    const auto adapter_parameters = readAdapterParameters();
    auto replacement = std::make_unique<mppi::Optimizer>();
    replacement->initialize(private_nh_, name_, costmap_ros_);

    if (optimizer_->isSpeedLimitActive()) {
      const auto & old_base = optimizer_->getSettings().base_constraints;
      const auto & new_base = replacement->getSettings().base_constraints;
      constexpr float epsilon = 1e-6f;
      const bool velocity_limits_changed =
        std::fabs(old_base.vx_max - new_base.vx_max) > epsilon ||
        std::fabs(old_base.vx_min - new_base.vx_min) > epsilon ||
        std::fabs(old_base.vy - new_base.vy) > epsilon ||
        std::fabs(old_base.wz - new_base.wz) > epsilon;
      if (velocity_limits_changed) {
        response.success = false;
        response.message =
          "clear the active runtime speed limit before changing velocity bounds";
        return true;
      }
      replacement->setSpeedLimit(speed_limit_scale_ * 100.0, true);
    }

    path_handler_.reloadParameters();
    optimizer_.swap(replacement);
    const bool speed_topic_changed =
      speed_limit_topic_ != adapter_parameters.speed_limit_topic;
    applyAdapterParameters(adapter_parameters);
    if (speed_topic_changed) {
      subscribeToSpeedLimit();
    }
    goal_reached_ = false;
    response.success = true;
    response.message = "MPPI parameters reloaded transactionally";
    ROS_INFO("[%s] %s", name_.c_str(), response.message.c_str());
  } catch (const std::exception & ex) {
    response.success = false;
    response.message = std::string("MPPI parameter reload rejected: ") + ex.what();
    ROS_ERROR("[%s] %s", name_.c_str(), response.message.c_str());
  }
  return true;
}

void MPPIController::speedLimitScaleCallback(
  const std_msgs::Float32ConstPtr & message)
{
  std::lock_guard<std::mutex> lock(controller_mutex_);
  if (!initialized_ || !message) {
    return;
  }
  double scale = static_cast<double>(message->data);
  if (!std::isfinite(scale)) {
    ROS_ERROR_THROTTLE(
      1.0, "MPPI received a non-finite speed-limit scale; stopping");
    scale = 0.0;
  }
  scale = std::clamp(scale, 0.0, 1.0);
  if (std::fabs(scale - speed_limit_scale_) <= 1e-6) {
    return;
  }
  speed_limit_scale_ = scale;
  optimizer_->setSpeedLimit(scale * 100.0, true);
  ROS_INFO("MPPI speed-limit scale updated to %.3f", scale);
}

bool MPPIController::setPlan(
  const std::vector<geometry_msgs::PoseStamped> & plan)
{
  std::lock_guard<std::mutex> lock(controller_mutex_);
  if (!initialized_) {
    ROS_ERROR("MPPIController must be initialized before setPlan()");
    return false;
  }
  if (plan.empty()) {
    ROS_ERROR("MPPIController rejected an empty plan");
    return false;
  }

  global_path_.header = plan.front().header;
  if (global_path_.header.frame_id.empty()) {
    global_path_.header.frame_id = costmap_ros_->getGlobalFrameID();
  }
  global_path_.header.stamp = ros::Time::now();
  global_path_.poses = plan;
  for (auto & pose : global_path_.poses) {
    if (pose.header.frame_id.empty()) {
      pose.header.frame_id = global_path_.header.frame_id;
    }
  }

  try {
    path_handler_.setPath(global_path_);
  } catch (const std::exception & ex) {
    ROS_ERROR("MPPIController rejected plan: %s", ex.what());
    return false;
  }
  goal_reached_ = false;
  goal_reached_evaluator_.restoreConfiguredTolerances();
  return true;
}

bool MPPIController::computeVelocityCommands(geometry_msgs::Twist & command)
{
  std::lock_guard<std::mutex> lock(controller_mutex_);
  setZeroCommand(command);
  if (!initialized_) {
    ROS_ERROR_THROTTLE(1.0, "MPPIController is not initialized");
    return false;
  }

  geometry_msgs::PoseStamped robot_pose;
  if (!costmap_ros_->getRobotPose(robot_pose)) {
    ROS_ERROR_THROTTLE(1.0, "MPPIController cannot obtain the robot pose");
    return false;
  }

  geometry_msgs::PoseStamped robot_velocity_pose;
  odom_helper_->getRobotVel(robot_velocity_pose);
  geometry_msgs::Twist robot_speed;
  robot_speed.linear.x = robot_velocity_pose.pose.position.x;
  robot_speed.linear.y = robot_velocity_pose.pose.position.y;
  robot_speed.angular.z = tf2::getYaw(robot_velocity_pose.pose.orientation);

  const auto path_start = std::chrono::steady_clock::now();
  nav_msgs::Path transformed_plan;
  geometry_msgs::Pose goal;
  try {
    transformed_plan = path_handler_.transformPath(robot_pose);
    // Upstream semantics: critics always receive the original global goal,
    // not the end of a truncated local-costmap window.
    goal = path_handler_.getTransformedGoal().pose;
  } catch (const std::exception & ex) {
    ROS_ERROR_THROTTLE(
      1.0, "MPPIController failed to transform path: %s", ex.what());
    return false;
  }
  const auto optimizer_start = std::chrono::steady_clock::now();

  const bool local_plan_reaches_goal =
    path_handler_.transformedPathEndsAtGoal();
  if (local_plan_reaches_goal &&
      poseWithinGoalTolerance(robot_pose.pose, goal))
  {
    const double distance = std::hypot(
      robot_pose.pose.position.x - goal.position.x,
      robot_pose.pose.position.y - goal.position.y);
    const double yaw_error = angles::shortest_angular_distance(
      tf2::getYaw(robot_pose.pose.orientation), tf2::getYaw(goal.orientation));
    goal_reached_ = goal_reached_evaluator_.update(
      distance, yaw_error, robot_speed.linear.x, robot_speed.angular.z,
      ros::WallTime::now().toSec());
    setZeroCommand(command);
    return true;
  }
  goal_reached_evaluator_.reset();
  goal_reached_ = false;

  try {
    auto result = optimizer_->evalControl(
      robot_pose, robot_speed, transformed_plan, goal);
    const auto & optimized_control = std::get<0>(result);
    command = optimized_control.twist;
    if (visualize_) {
      visualize(
        std::move(transformed_plan), std::get<1>(result),
        optimized_control.header.stamp);
    }
  } catch (const std::exception & ex) {
    setZeroCommand(command);
    ROS_ERROR_THROTTLE(
      1.0, "MPPIController failed to produce a safe control: %s", ex.what());
    return false;
  }
  if (!std::isfinite(command.linear.x) ||
      !std::isfinite(command.linear.y) ||
      !std::isfinite(command.angular.z))
  {
    setZeroCommand(command);
    ROS_ERROR_THROTTLE(1.0, "MPPIController produced a non-finite control");
    return false;
  }

  const auto optimizer_end = std::chrono::steady_clock::now();
  if (timing_diagnostics_) {
    path_time_total_ms_ += std::chrono::duration<double, std::milli>(
      optimizer_start - path_start).count();
    optimizer_time_total_ms_ += std::chrono::duration<double, std::milli>(
      optimizer_end - optimizer_start).count();
    if (++timing_cycles_ >= 50u) {
      const double count = static_cast<double>(timing_cycles_);
      ROS_INFO(
        "MPPIController cycle average: path=%.3fms optimizer=%.3fms",
        path_time_total_ms_ / count, optimizer_time_total_ms_ / count);
      timing_cycles_ = 0u;
      path_time_total_ms_ = 0.0;
      optimizer_time_total_ms_ = 0.0;
    }
  }

  return true;
}

bool MPPIController::isGoalReached()
{
  std::lock_guard<std::mutex> lock(controller_mutex_);
  return goal_reached_;
}

bool MPPIController::isGoalReachedWithTolerances(
  double distance_tolerance, double angle_tolerance)
{
  std::lock_guard<std::mutex> lock(controller_mutex_);
  goal_reached_evaluator_.applyToleranceUpperBounds(
    distance_tolerance, angle_tolerance);
  goal_reached_ = goal_reached_evaluator_.reached();
  return goal_reached_;
}

bool MPPIController::poseWithinGoalTolerance(
  const geometry_msgs::Pose & robot_pose,
  const geometry_msgs::Pose & goal_pose) const
{
  const double distance = std::hypot(
    robot_pose.position.x - goal_pose.position.x,
    robot_pose.position.y - goal_pose.position.y);
  const double yaw_error = angles::shortest_angular_distance(
    tf2::getYaw(robot_pose.orientation), tf2::getYaw(goal_pose.orientation));
  return distance <= goal_reached_evaluator_.activePositionTolerance() &&
         std::fabs(yaw_error) <= goal_reached_evaluator_.activeYawTolerance();
}

void MPPIController::visualize(
  nav_msgs::Path path, const Eigen::ArrayXXf & optimal_trajectory,
  const ros::Time & command_stamp)
{
  if (!trajectory_visualizer_.hasSubscribers()) {
    return;
  }
  if (trajectory_visualizer_.hasTrajectorySubscribers()) {
    trajectory_visualizer_.add(
      optimizer_->getGeneratedTrajectories(), "Candidate Trajectories");
  }
  if (trajectory_visualizer_.hasTrajectorySubscribers() ||
      trajectory_visualizer_.hasOptimalPathSubscribers())
  {
    trajectory_visualizer_.add(
      optimal_trajectory, "Optimal Trajectory", command_stamp);
  }
  trajectory_visualizer_.visualize(std::move(path));
}

void MPPIController::setZeroCommand(geometry_msgs::Twist & command)
{
  command = geometry_msgs::Twist();
}

}  // namespace local_planner

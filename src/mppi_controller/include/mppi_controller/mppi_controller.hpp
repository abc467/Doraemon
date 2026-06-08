#pragma once

#include <cstddef>
#include <cmath>
#include <angles/angles.h>
#include <chrono>

#include <ros/ros.h>
#include <nav_core/base_local_planner.h>
#include <base_local_planner/odometry_helper_ros.h>

#include <nav_msgs/Odometry.h>
#include <geometry_msgs/PoseStamped.h>
#include <geometry_msgs/Twist.h>
#include <base_local_planner/costmap_model.h>
#include <costmap_2d/costmap_2d_ros.h>

#include <tf2_ros/transform_listener.h>
#include <tf2_geometry_msgs/tf2_geometry_msgs.h>
#include <tf2/utils.h>

#include <dynamic_reconfigure/server.h>

#include "mppi_controller/tools/path_handler.hpp"
#include "mppi_controller/optimizer.hpp"
#include "mppi_controller/models/constraints.hpp"
#include "mppi_controller/tools/utils.hpp"
#include "mppi_controller/tools/trajectory_visualizer.hpp"

namespace local_planner
{
using namespace mppi;

class MPPIController : public nav_core::BaseLocalPlanner 
{
public:
    MPPIController();
    MPPIController(std::string name, tf2_ros::Buffer* tf, costmap_2d::Costmap2DROS* costmap_ros);
    ~MPPIController();

    void initialize(std::string name, tf2_ros::Buffer* tf, costmap_2d::Costmap2DROS* costmap_ros);
    bool setPlan(const std::vector<geometry_msgs::PoseStamped>& orig_global_plan);
    bool computeVelocityCommands(geometry_msgs::Twist& cmd_vel);
    bool isGoalReached();

private:
    // 获取 local costmap 的 global frame 下 robot 的位姿
    std::optional<geometry_msgs::PoseStamped> getRobotPose();

    // 判断是否在目标点附近
    bool isGoalReached(const geometry_msgs::Pose &robot_pose,
                     const geometry_msgs::Pose &goal_pose);

    // Check the footprint sweep before issuing an in-place goal alignment.
    bool isRotationCollisionFree(
        const geometry_msgs::Pose &robot_pose, double angular_velocity) const;

    // 可视化
    void visualize(nav_msgs::Path path);

    std::shared_ptr<costmap_2d::Costmap2DROS> costmap_ros_;
    // tf2_ros::Buffer* tf_;
    std::shared_ptr<base_local_planner::OdometryHelperRos> odom_helper_;
    std::shared_ptr<tf2_ros::Buffer> tf_buffer_;
    std::unique_ptr<tf2_ros::TransformListener> tf_listener_;

    Optimizer optimizer_;
    PathHandler path_handler_;

    TrajectoryVisualizer trajectory_visualizer_;

    std::vector<geometry_msgs::PoseStamped> global_plan_;
    nav_msgs::Path global_path_;

    bool visualize_ = false;
    bool initialized_ = false;
    bool reach_goal_ = false;
    bool first_rotate_ = false;
    bool timing_diagnostics_ = false;
    size_t timing_cycles_ = 0;
    double path_time_total_ms_ = 0.0;
    double optimizer_time_total_ms_ = 0.0;

    double goal_tolerance_;
    double angle_tolerance_;
    bool rotate_to_goal_enabled_ = true;
    double rotate_to_goal_kp_ = 0.8;
    double rotate_to_goal_min_angular_speed_ = 0.12;
    double rotate_to_goal_max_angular_speed_ = 0.4;
    double rotate_to_goal_collision_horizon_ = 0.5;
    double rotate_to_goal_collision_step_ = 0.05;

};

} // namespace local_planner

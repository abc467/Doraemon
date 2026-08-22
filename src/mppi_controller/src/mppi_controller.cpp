#include <chrono>
#include "mppi_controller/mppi_controller.hpp"
#include <pluginlib/class_list_macros.h>

// 通过该指令注册为BaseLocalPlanner插件
PLUGINLIB_EXPORT_CLASS(local_planner::MPPIController, nav_core::BaseLocalPlanner)

namespace local_planner
{

    MPPIController::MPPIController() {}

    MPPIController::MPPIController(std::string name, tf2_ros::Buffer *tf, costmap_2d::Costmap2DROS *costmap_ros)
        : costmap_ros_(nullptr), initialized_(false)
    {
        initialize(name, tf, costmap_ros);
    }

    MPPIController::~MPPIController() {}

    void MPPIController::initialize(std::string name, tf2_ros::Buffer *tf, costmap_2d::Costmap2DROS *costmap_ros)
    {
        if (!initialized_)
        {
            costmap_ros_ = std::shared_ptr<costmap_2d::Costmap2DROS>(costmap_ros, [](auto *) {}); // 创建一个std::shared_ptr 的接口

            tf_buffer_ = std::shared_ptr<tf2_ros::Buffer>(tf, [](auto *) {});
            // tf_listener_ = std::make_unique<tf2_ros::TransformListener>(*tf_buffer_);
            ros::NodeHandle private_nh("~/" + name);

            private_nh.param("visualize", visualize_, false);
            private_nh.param("timing_diagnostics", timing_diagnostics_, false);
            private_nh.param("goal_tolerance", goal_tolerance_, 0.2);
            private_nh.param("angle_tolerance", angle_tolerance_, 0.2);
            std::string odom_topic("/odom");
            private_nh.param("odom_topic", odom_topic, std::string("/odom"));
            ROS_INFO("MPPIController: goal_tolerance: %f, angle_tolerance: %f", goal_tolerance_, angle_tolerance_);
            ROS_INFO_STREAM("MPPIController: odom topic: " << odom_topic);

            odom_helper_ = std::make_shared<base_local_planner::OdometryHelperRos>(odom_topic);
            optimizer_.initialize(private_nh, name, costmap_ros_);
            path_handler_.initialize(private_nh, name, costmap_ros_, tf_buffer_);
            trajectory_visualizer_.initialize(private_nh, name, costmap_ros_->getGlobalFrameID());

            initialized_ = true;
            ROS_INFO("MPPIController initialized");
        }
        else
        {
            ROS_WARN("This controller is initialized .");
        }
    }

    bool MPPIController::setPlan(const std::vector<geometry_msgs::PoseStamped> &orig_global_plan)
    {
        if (!initialized_)
        {
            ROS_ERROR("This planner has not been initialized, please call initialize() before using this planner");
            return false;
        }

        global_path_.header.frame_id = "map";
        global_path_.header.stamp = ros::Time::now();
        global_path_.poses = orig_global_plan;
        path_handler_.setPath(global_path_);

        reach_goal_ = false;

        return true;
    }

    bool MPPIController::isGoalReached()
    {
        return reach_goal_;
    }

    bool MPPIController::computeVelocityCommands(geometry_msgs::Twist &cmd_vel)
    {
        // 获取 local costmap 全局坐标系下的 robot pose。
        geometry_msgs::PoseStamped robot_pose;
        if (!costmap_ros_->getRobotPose(robot_pose))
        {
            ROS_ERROR("MPPIController: Can not get robot pose.");
            return false;
        }
        // 获取速度
        geometry_msgs::PoseStamped robot_vel;
        odom_helper_->getRobotVel(robot_vel);
        geometry_msgs::Twist robot_speed;
        robot_speed.linear.x = robot_vel.pose.position.x;
        robot_speed.linear.y = robot_vel.pose.position.y;
        robot_speed.angular.z = tf2::getYaw(robot_vel.pose.orientation);

        geometry_msgs::Pose goal;
        nav_msgs::Path transformed_plan;
        const auto path_start = std::chrono::steady_clock::now();
        try
        {
            // 转化到局部代价地图坐标系下
            // goal = path_handler_.getTransformedGoal().pose;
            transformed_plan = path_handler_.transformPath(robot_pose);
            goal = transformed_plan.poses.back().pose;
        }
        catch (const std::exception & ex)
        {
            ROS_ERROR_THROTTLE(
                1.0, "[mppi_controller] Failed to transform path: %s", ex.what());
            return false;
        }
        catch (...)
        {
            ROS_ERROR_THROTTLE(
                1.0, "[mppi_controller] Failed to transform path: unknown exception");
            return false;
        }
        const auto optimizer_start = std::chrono::steady_clock::now();
        const bool local_plan_reaches_goal =
            path_handler_.transformedPathEndsAtGoal();

        if (local_plan_reaches_goal && isGoalReached(robot_pose.pose, goal))
        {
            reach_goal_ = true;
            cmd_vel.linear.x = 0;
            cmd_vel.linear.y = 0;
            cmd_vel.angular.z = 0;
            return true;
        }

        try
        {
            // auto start = std::chrono::high_resolution_clock::now(); // 开始计时
            cmd_vel = optimizer_.evalControl(robot_pose, robot_speed, transformed_plan, goal).twist;
            // auto end = std::chrono::high_resolution_clock::now();
            // auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
            // std::cout << "耗时: " << duration.count() << " 毫秒" << std::endl;
        }
        catch (const std::exception & ex)
        {
            // Do not turn an exhausted optimizer into a successful zero
            // command. Returning false lets MBF publish the safety stop for
            // this tick, keep retrying within controller_patience, and account
            // prolonged failures correctly.
            cmd_vel = geometry_msgs::Twist();
            ROS_ERROR_THROTTLE(
                1.0, "[mppi_controller] Failed to evaluate control: %s", ex.what());
            return false;
        }
        catch (...)
        {
            cmd_vel = geometry_msgs::Twist();
            ROS_ERROR_THROTTLE(
                1.0, "[mppi_controller] Failed to evaluate control: unknown exception");
            return false;
        }
        const auto optimizer_end = std::chrono::steady_clock::now();

        if (timing_diagnostics_)
        {
            path_time_total_ms_ += std::chrono::duration<double, std::milli>(
                optimizer_start - path_start).count();
            optimizer_time_total_ms_ += std::chrono::duration<double, std::milli>(
                optimizer_end - optimizer_start).count();

            if (++timing_cycles_ >= 50)
            {
                const double cycles = static_cast<double>(timing_cycles_);
                ROS_INFO(
                    "[mppi_controller] cycle average: path=%.3fms optimizer=%.3fms",
                    path_time_total_ms_ / cycles,
                    optimizer_time_total_ms_ / cycles);
                timing_cycles_ = 0;
                path_time_total_ms_ = 0.0;
                optimizer_time_total_ms_ = 0.0;
            }
        }

        if (visualize_)
        {
            visualize(std::move(transformed_plan));
        }

        return true;
    }

    void MPPIController::visualize(nav_msgs::Path path)
    {
        if (!trajectory_visualizer_.hasSubscribers())
        {
            return;
        }

        const bool publish_trajectories = trajectory_visualizer_.hasTrajectorySubscribers();
        const bool publish_optimal_path = trajectory_visualizer_.hasOptimalPathSubscribers();

        if (publish_trajectories)
        {
            trajectory_visualizer_.add(
                optimizer_.getGeneratedTrajectories(), "Candidate Trajectories");
        }

        if (publish_trajectories || publish_optimal_path)
        {
            trajectory_visualizer_.add(
                optimizer_.getOptimizedTrajectory(), "Optimal Trajectory", ros::Time::now());
        }

        trajectory_visualizer_.visualize(std::move(path));
    }

    bool MPPIController::isGoalReached(const geometry_msgs::Pose &robot_pose,
                                       const geometry_msgs::Pose &goal_pose)
    {
        const double dx = robot_pose.position.x - goal_pose.position.x;
        const double dy = robot_pose.position.y - goal_pose.position.y;

        const double robot_yaw = tf2::getYaw(robot_pose.orientation);
        const double goal_yaw = tf2::getYaw(goal_pose.orientation);
        const double yaw_diff = angles::shortest_angular_distance(robot_yaw, goal_yaw);
        return std::sqrt(dx * dx + dy * dy) <= goal_tolerance_ && std::fabs(yaw_diff) <= angle_tolerance_;
    }

    std::optional<geometry_msgs::PoseStamped> MPPIController::getRobotPose()
    {
        geometry_msgs::TransformStamped transformStamped;
        try
        {
            transformStamped = tf_buffer_->lookupTransform(costmap_ros_->getGlobalFrameID(), costmap_ros_->getBaseFrameID(), ros::Time(0), ros::Duration(1.0));
        }
        catch (tf2::TransformException &ex)
        {
            ROS_WARN("tf error: %s", ex.what());
            return std::nullopt;
        }

        geometry_msgs::PoseStamped result;
        result.header = transformStamped.header;
        result.pose.position.x = transformStamped.transform.translation.x;
        result.pose.position.y = transformStamped.transform.translation.y;
        result.pose.orientation = transformStamped.transform.rotation;
        return std::optional(result);
    }

} // namespace local_planner

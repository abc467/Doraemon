/**
 * docking_tracker_node.cpp (Action Server 封装版)
 * 功能：平时待机，收到 Action Goal 后执行自动回充逻辑
 * 状态：已集成虚拟目标点(0.2m)与强制停车逻辑
 */

#include <ros/ros.h>
#include <actionlib/server/simple_action_server.h>
#include <my_docking_controller/AutoDockingAction.h> // 编译后自动生成
#include <geometry_msgs/PoseStamped.h>
#include <geometry_msgs/Twist.h>
#include <tf/transform_listener.h>
#include <tf/transform_datatypes.h>
#include <string>
#include <memory>
#include <cmath>
#include <algorithm>

#include "my_docking_controller/smooth_control_law.hpp"

class DockingActionServer
{
protected:
    ros::NodeHandle nh_;
    // Action Server 对象
    actionlib::SimpleActionServer<my_docking_controller::AutoDockingAction> as_;
    std::string action_name_;
    
    // 反馈与结果
    my_docking_controller::AutoDockingFeedback feedback_;
    my_docking_controller::AutoDockingResult result_;

    // 订阅与发布
    ros::Publisher cmd_vel_pub_;
    ros::Subscriber dock_pose_sub_;
    tf::TransformListener tf_listener_;

    // 控制器指针
    std::unique_ptr<my_docking_controller::SmoothControlLaw> control_law_;
    
    // 参数变量
    std::string robot_base_frame_;
    double k_phi_, k_delta_, beta_, lambda_, v_linear_min_, v_linear_max_, v_angular_max_, slowdown_radius_;
    double default_docking_distance_; // Launch文件中配置的默认距离
    double xy_tolerance_, yaw_tolerance_;
    double pose_stale_timeout_s_, pose_lost_timeout_s_, tf_wait_timeout_s_;
    bool dock_backwards_;
    std::string dock_pose_topic_;
    std::string cmd_vel_topic_;
    
    // 运行时状态变量
    bool is_active_;           // 是否正在执行任务
    bool docking_completed_;   // 是否已完成
    double current_target_dist_; // 当前任务的目标距离
    bool have_pose_;
    ros::Time last_pose_ts_;

public:
    DockingActionServer(std::string name) : 
        as_(nh_, name, boost::bind(&DockingActionServer::executeCB, this, _1), false),
        action_name_(name),
        tf_listener_(ros::Duration(10.0)),
        is_active_(false),
        docking_completed_(false)
    {
        // 1. 加载参数 (保留您调试好的参数)
        ros::NodeHandle pn("~");
        pn.param("k_phi", k_phi_, 2.0);
        pn.param("k_delta", k_delta_, 1.5);
        pn.param("beta", beta_, 0.4);
        pn.param("lambda", lambda_, 2.0);
        pn.param("v_linear_min", v_linear_min_, 0.05);
        pn.param("v_linear_max", v_linear_max_, 0.25);
        pn.param("v_angular_max", v_angular_max_, 0.8);
        pn.param("slowdown_radius", slowdown_radius_, 0.6);
        
        pn.param<std::string>("robot_base_frame", robot_base_frame_, "base_link");
        pn.param<std::string>("dock_pose_topic", dock_pose_topic_, "/dock_pose");
        pn.param<std::string>("cmd_vel_topic", cmd_vel_topic_, "/cmd_vel");
        pn.param("dock_backwards", dock_backwards_, false);
        
        // 默认停车距离 (如 0.15m)
        pn.param("docking_distance", default_docking_distance_, 0.7); 
        pn.param("xy_tolerance", xy_tolerance_, 0.02);   
        // 角度容差建议稍微放宽，或者保持您测试过的 0.02
        pn.param("yaw_tolerance", yaw_tolerance_, 0.02);  
        pn.param("pose_stale_timeout_s", pose_stale_timeout_s_, 0.25);
        pn.param("pose_lost_timeout_s", pose_lost_timeout_s_, 1.0);
        pn.param("tf_wait_timeout_s", tf_wait_timeout_s_, 0.1);

        // 初始化控制律
        control_law_ = std::make_unique<my_docking_controller::SmoothControlLaw>(
            k_phi_, k_delta_, beta_, lambda_, slowdown_radius_, 
            v_linear_min_, v_linear_max_, v_angular_max_);

        cmd_vel_pub_ = nh_.advertise<geometry_msgs::Twist>(cmd_vel_topic_, 1);
        
        // 持续订阅感知数据，但只在 is_active_ 为 true 时处理
        dock_pose_sub_ = nh_.subscribe(dock_pose_topic_, 1, &DockingActionServer::poseCallback, this);

        // 启动服务
        as_.start();
        ROS_INFO("%s: Server Started. Waiting for goal... dock_pose=%s cmd_vel=%s base_frame=%s",
                 action_name_.c_str(), dock_pose_topic_.c_str(), cmd_vel_topic_.c_str(), robot_base_frame_.c_str());
    }

    // Action 回调：收到主控节点的 Goal 时触发
    void executeCB(const my_docking_controller::AutoDockingGoalConstPtr &goal)
    {
        ros::Rate r(30); // 30Hz 循环等待
        bool success = false;
        
        // === 1. 确定目标距离 ===
        // 如果主控节点发了 0.0，就用 Launch 文件里的默认值
        // 如果主控节点发了具体数值 (比如 1.0)，就用该数值 (方便调试)
        if (goal->target_dist > 0.001) {
            current_target_dist_ = goal->target_dist;
            ROS_INFO("Docking Goal Received: Custom distance %.3f m", current_target_dist_);
        } else {
            current_target_dist_ = default_docking_distance_;
            ROS_INFO("Docking Goal Received: Using default distance %.3f m", current_target_dist_);
        }

        // === 2. 激活控制逻辑 ===
        docking_completed_ = false;
        is_active_ = true;
        have_pose_ = false;
        last_pose_ts_ = ros::Time(0);
        const ros::Time goal_start = ros::Time::now();

        // === 3. 等待循环 ===
        while(ros::ok())
        {
            // 检查取消请求
            if (as_.isPreemptRequested() || !ros::ok())
            {
                ROS_WARN("%s: Preempted", action_name_.c_str());
                stopRobot();
                as_.setPreempted();
                is_active_ = false;
                return;
            }

            const ros::Time now = ros::Time::now();
            if (!have_pose_) {
                stopRobot();
                const double wait_s = (now - goal_start).toSec();
                feedback_.state = "WAITING_FOR_DOCK_POSE";
                feedback_.dist_to_wall = -1.0;
                feedback_.angle_error = 0.0;
                as_.publishFeedback(feedback_);
                if (pose_lost_timeout_s_ > 1e-6 && wait_s > pose_lost_timeout_s_) {
                    result_.success = false;
                    result_.message = "No dock pose received";
                    is_active_ = false;
                    as_.setAborted(result_);
                    return;
                }
                r.sleep();
                continue;
            }

            const double pose_age = (now - last_pose_ts_).toSec();
            if (pose_stale_timeout_s_ > 1e-6 && pose_age > pose_stale_timeout_s_) {
                stopRobot();
                feedback_.state = "DOCK_POSE_STALE";
                as_.publishFeedback(feedback_);
            }
            if (pose_lost_timeout_s_ > 1e-6 && pose_age > pose_lost_timeout_s_) {
                stopRobot();
                result_.success = false;
                result_.message = "Dock pose lost during docking";
                is_active_ = false;
                as_.setAborted(result_);
                return;
            }

            // 检查是否完成 (由 poseCallback 设置这个标志)
            if (docking_completed_) {
                success = true;
                break;
            }
            
            // 发布反馈 (可选)
            // feedback_.state = "Running";
            // as_.publishFeedback(feedback_);

            r.sleep();
        }

        // === 4. 结束处理 ===
        stopRobot();
        is_active_ = false;

        if(success)
        {
            ROS_INFO("%s: Succeedededed", action_name_.c_str());
            result_.success = true;
            result_.message = "Docking Complete";
            as_.setSucceeded(result_);
        }
    }

    // 感知数据回调：核心控制逻辑
    void poseCallback(const geometry_msgs::PoseStamped::ConstPtr& msg)
    {
        // 如果 Action 未激活，直接忽略数据，不占用底盘
        if (!is_active_) return;

        geometry_msgs::PoseStamped target_pose_robot;
        geometry_msgs::Twist cmd_vel;

        try {
            // TF 转换
            tf_listener_.waitForTransform(robot_base_frame_, msg->header.frame_id,
                                          ros::Time(0), ros::Duration(tf_wait_timeout_s_));
            tf_listener_.transformPose(robot_base_frame_, *msg, target_pose_robot);
        } catch (tf::TransformException &ex) {
            ROS_WARN_THROTTLE(1.0, "TF Error: %s", ex.what());
            stopRobot();
            return;
        }

        have_pose_ = true;
        last_pose_ts_ = ros::Time::now();

        // 获取当前距离墙壁的绝对距离
        double current_dist_to_wall = target_pose_robot.pose.position.x;
        double yaw = tf::getYaw(target_pose_robot.pose.orientation);
        feedback_.state = "TRACKING";
        feedback_.dist_to_wall = current_dist_to_wall;
        feedback_.angle_error = yaw;
        as_.publishFeedback(feedback_);
        
        // ==========================================================
        // 【裁判逻辑】 到达检测 (复用您验证过的逻辑)
        // ==========================================================
        if (current_dist_to_wall <= current_target_dist_) {
            ROS_INFO("Docking Reached! Wall Dist: %.3f m", current_dist_to_wall);
            stopRobot();
            docking_completed_ = true; // 通知 executeCB 结束任务
            return;
        }

        // ==========================================================
        // 【运动员逻辑】 虚拟目标点 (复用您验证过的逻辑)
        // ==========================================================
        // 瞄准墙后 0.2米，防止终点震荡
        double virtual_target_offset = 0.25; 
        double control_target_x = current_dist_to_wall - (current_target_dist_ - virtual_target_offset);

        geometry_msgs::Pose control_goal = target_pose_robot.pose;
        control_goal.position.x = control_target_x; 
        
        // 强制朝向 0 (正前方)
        control_goal.orientation = tf::createQuaternionMsgFromYaw(0.0);

        // 计算速度
        cmd_vel = control_law_->calculateRegularVelocity(control_goal, dock_backwards_);

        // 安全限速 (最后20cm)
        if (current_dist_to_wall < current_target_dist_ + 0.2) {
             cmd_vel.linear.x = std::min(cmd_vel.linear.x, 0.05); 
        }

        if (std::abs(yaw) <= yaw_tolerance_ && current_dist_to_wall <= current_target_dist_ + xy_tolerance_) {
            cmd_vel.linear.x = 0.0;
            cmd_vel.angular.z = 0.0;
        }

        cmd_vel_pub_.publish(cmd_vel);
    }

    void stopRobot() {
        geometry_msgs::Twist stop_cmd;
        stop_cmd.linear.x = 0.0;
        stop_cmd.angular.z = 0.0;
        cmd_vel_pub_.publish(stop_cmd);
    }
};

int main(int argc, char** argv)
{
    ros::init(argc, argv, "docking_action_server");
    // 使用 AsyncSpinner 允许 Action 回调和 Topic 回调并行处理
    ros::AsyncSpinner spinner(2); 
    DockingActionServer server("docking_action");
    spinner.start();
    ros::waitForShutdown();
    return 0;
}

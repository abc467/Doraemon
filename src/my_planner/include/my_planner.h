#ifndef MY_PLANNER_H_
#define MY_PLANNER_H_

#include <ros/ros.h>
#include <nav_core/base_local_planner.h>
#include <tf/transform_listener.h>
#include <costmap_2d/costmap_2d_ros.h>
#include <geometry_msgs/PoseStamped.h>
#include <geometry_msgs/Twist.h>
#include <vector>

namespace my_planner {

class MyPlanner : public nav_core::BaseLocalPlanner {
public:
    MyPlanner();
    ~MyPlanner();

    // BaseLocalPlanner 接口要求的函数
    void initialize(std::string name, tf2_ros::Buffer* tf, costmap_2d::Costmap2DROS* costmap_ros);
    bool setPlan(const std::vector<geometry_msgs::PoseStamped>& plan);
    bool computeVelocityCommands(geometry_msgs::Twist& cmd_vel);
    bool isGoalReached();

private:
    // --- 外部对象指针 ---
    costmap_2d::Costmap2DROS* costmap_ros_;
    tf::TransformListener* tf_listener_;

    // --- 状态标志位 ---
    bool pose_adjusting_; // 是否正在进行最终姿态调整
    bool goal_reached_;   // 是否到达终点

    // --- 路径规划相关 ---
    std::vector<geometry_msgs::PoseStamped> global_plan_; // 全局路径容器
    int target_index_;    // 当前追踪的路径点索引

    // --- 控制参数与变量 ---
    double current_vel_x_;      // 当前线速度 (用于平滑控制)
    double max_vel_x_;          // 最大线速度
    double acc_lim_x_;          // 最大线加速度
    double max_vel_theta_;      // 最大角速度
    double acc_lim_theta_;      // 最大角加速度
    double lookahead_dist_;     // 前瞻距离
    double goal_tolerance_;     // 到达目标点的判定距离
};

}; // namespace my_planner

#endif
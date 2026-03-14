#pragma once

#include <vector>
#include <utility>
#include <string>
#include <memory>

#include <ros/ros.h>
#include <geometry_msgs/PoseStamped.h>
#include <nav_msgs/Path.h>
#include <nav_msgs/Odometry.h>
#include <costmap_2d/costmap_2d_ros.h>
#include <nav_core/base_local_planner.h>

#include "mppi_controller/tools/utils.hpp"

namespace mppi
{

using PathIterator = std::vector<geometry_msgs::PoseStamped>::iterator;
using PathRange = std::pair<PathIterator, PathIterator>;

/**
 * @class mppi::PathHandler
 * @brief 管理参考路径的类
 */

class PathHandler
{
public:
    PathHandler() = default;

    ~PathHandler() = default;

    /**
     * @brief Initialize path handler on bringup
     * @param name Name of plugin
     * @param costmap_ros Costmap2DROS object of environment
     * @param tf TF buffer for transformations
     * @param dynamic_parameter_handler Parameter handler object
     */
    void initialize(const ros::NodeHandle &nh,
                                 const std::string &name,
                                 std::shared_ptr<costmap_2d::Costmap2DROS> costmap,
                                 std::shared_ptr<tf2_ros::Buffer> buffer);

    /**
     * @brief 设置参考路径
     * @param Plan Path to use
     */
    void setPath(const nav_msgs::Path &plan);

    /**
     * @brief 获取参考路径
     * @return Path
     */
    nav_msgs::Path &getPath();

    /**
     * @brief 获取odom坐标系下的参考路径, 并裁剪全局路径
     * @param robot_pose Pose of robot
     * @return odom坐标系下的路径
     */
    nav_msgs::Path transformPath(const geometry_msgs::PoseStamped &robot_pose);

    /**
     * @brief 将全局坐标系下的goal转换到局部坐标系
     * @return 转换后的goal坐标(局部地图的globalframe)
     */
    geometry_msgs::PoseStamped getTransformedGoal();

protected:
    /**
     * @brief 将一个坐标系下的位姿in_pose 转换到另一个坐标系frame 下
     * @param frame Frame to transform to
     * @param in_pose Input pose
     * @param out_pose Output pose
     * @return Bool if successful
     */
    bool transformPose(
        const std::string &frame, const geometry_msgs::PoseStamped &in_pose,
        geometry_msgs::PoseStamped &out_pose) const;

    /**
     * @brief 获取代价地图的最长边长度
     * @return 代价地图的最长边长度
     */
    double getMaxCostmapDist();

    /**
     * @brief 将一个位姿pose 转换到全局路径坐标系下
     * @param pose Current pose
     * @return output poose in global reference frame
     */
    geometry_msgs::PoseStamped transformToGlobalPlanFrame(const geometry_msgs::PoseStamped &pose);

    /**
     * @brief 获取局部代价地图尺寸内的参考路径
     * @param global_pose 全局坐标系下的 Robot pose
     * @return 转换到代价子图坐标系下的局部路径; 离robot最近点的迭代器, 用于裁剪global_plan_up_to_inversion_
     */
    std::pair<nav_msgs::Path, PathIterator> getGlobalPlanConsideringBoundsInCostmapFrame(
        const geometry_msgs::PoseStamped &global_pose);

    /**
     * @brief 裁剪路径到end迭代器
     * @param plan Plan to prune
     * @param end Final path iterator
     */
    void prunePlan(nav_msgs::Path &plan, const PathIterator end);

    ros::NodeHandle nh_;

    std::shared_ptr<costmap_2d::Costmap2DROS> costmap_;
    std::shared_ptr<tf2_ros::Buffer> tf_buffer_;

    nav_msgs::Path global_plan_;
    nav_msgs::Path global_plan_up_to_inversion_; // 裁剪后的原始路径

    double max_robot_pose_search_dist_{0};
    double prune_distance_{0};
    double transform_tolerance_{0};
    float inversion_xy_tolerance_{0.2};
    float inversion_yaw_tolerance{0.4};
    bool enforce_path_inversion_{false};
    unsigned int inversion_locale_{0u};
};
}
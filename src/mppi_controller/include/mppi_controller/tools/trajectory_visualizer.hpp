#pragma once

#include <Eigen/Dense>

#include <memory>
#include <string>

#include <ros/ros.h>
#include "nav_msgs/Path.h"
#include "visualization_msgs/MarkerArray.h"

#include "mppi_controller/tools/utils.hpp"
#include "mppi_controller/models/trajectories.hpp"

namespace mppi
{

/**
 * @class mppi::TrajectoryVisualizer
 * @brief Visualizes trajectories for debugging
 */
class TrajectoryVisualizer
{
public:
  /**
    * @brief Constructor for mppi::TrajectoryVisualizer
    */
  TrajectoryVisualizer() = default;

/**
   * @brief 初始化函数
   * @param nh 
   * @param frame_id 坐标系 ID
   * @param name 插件名称
   */
  void initialize(const ros::NodeHandle& nh , const std::string & name,
    const std::string & frame_id);


  /**
    * @brief 添加最优轨迹用于可视化
    * @param trajectory Optimal trajectory
    */
  void add(
    const Eigen::ArrayXXf & trajectory, const std::string & marker_namespace,
    const ros::Time & cmd_stamp);

  /**
    * @brief 添加候选轨迹集合用于可视化
    * @param trajectories 候选轨迹集合
    */
  void add(const models::Trajectories & trajectories, const std::string & marker_namespace);

  /**
    * @brief 可视化路径
    * @param plan 
    */
  void visualize(const nav_msgs::Path & plan);

  /**
    * @brief Reset object
    */
  void reset();

protected:
  ros::NodeHandle nh_;
  
  std::string frame_id_;
  ros::Publisher trajectories_publisher_;
  ros::Publisher transformed_path_pub_;
  ros::Publisher optimal_path_pub_;

  std::unique_ptr<nav_msgs::Path> optimal_path_;
  std::unique_ptr<visualization_msgs::MarkerArray> points_;
  int marker_id_ = 0;

  int trajectory_step_{0};
  int time_step_{0};

};

}  // namespace mppi


#ifndef MY_DOCKING_CONTROLLER__EGO_POLAR_COORDS_HPP_
#define MY_DOCKING_CONTROLLER__EGO_POLAR_COORDS_HPP_

#include <cmath>
#include <geometry_msgs/Pose.h>
#include <tf/transform_datatypes.h> // 适配 ROS 1

namespace my_docking_controller
{

/**
* @struct EgoPolarCoords
* @brief 机器人中心极坐标系 (移植自 Nav2 Graceful Controller)
*/
struct EgoPolarCoords
{
  double r;     // 距离 (Official: r)
  double phi;   // 目标相对LOS的角度 (Official: phi)
  double delta; // 机器人相对LOS的角度 (Official: delta)

  // 角度归一化到 [-PI, PI]
  static inline double normalize_angle(double angle)
  {
    return atan2(sin(angle), cos(angle));
  }

  // 获取 ROS 1 Pose 的 Yaw 角
  static inline double getYaw(const geometry_msgs::Pose & pose)
  {
      return tf::getYaw(pose.orientation);
  }

  /**
   * @brief 计算极坐标
   * 注意：官方逻辑使用了 -dY 来计算视线角，这是为了配合控制律的符号定义。
   */
  static inline EgoPolarCoords getEgoPolarCoords(
    const geometry_msgs::Pose & pose_robot,
    const geometry_msgs::Pose & pose_target,
    bool backward = false)
  {
    // 计算相对位置
    double dX = pose_target.position.x - pose_robot.position.x;
    double dY = pose_target.position.y - pose_robot.position.y;
    
    // 【核心逻辑】计算视线角 (Line of Sight)
    // 如果倒车，视线角需要反转 180度
    double line_of_sight = backward ? (std::atan2(-dY, dX) + M_PI) : std::atan2(-dY, dX);

    EgoPolarCoords coords;
    coords.r = std::sqrt(dX * dX + dY * dY);
    
    // Phi: 目标朝向与视线的夹角
    coords.phi = normalize_angle(getYaw(pose_target) + line_of_sight);
    
    // Delta: 机器人朝向与视线的夹角
    coords.delta = normalize_angle(getYaw(pose_robot) + line_of_sight);

    return coords;
  }
};

}  // namespace my_docking_controller

#endif
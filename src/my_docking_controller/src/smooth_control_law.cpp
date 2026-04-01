#include "my_docking_controller/smooth_control_law.hpp"
#include <algorithm>
#include <cmath>
#include <iostream>

namespace my_docking_controller
{

SmoothControlLaw::SmoothControlLaw(
  double k_phi, double k_delta, double beta, double lambda,
  double slowdown_radius, double v_linear_min, double v_linear_max,
  double v_angular_max)
: k_phi_(k_phi), k_delta_(k_delta), beta_(beta), lambda_(lambda),
  slowdown_radius_(slowdown_radius), v_linear_min_(v_linear_min),
  v_linear_max_(v_linear_max), v_angular_max_(v_angular_max)
{
}

// 【官方核心公式】基于反馈线性化的曲率计算
// 引用自: nav2_graceful_controller/src/smooth_control_law.cpp
double SmoothControlLaw::calculateCurvature(double r, double phi, double delta)
{
  if (r < 0.001) return 0.0; // 防止除以零

  // 比例项：引入 atan 防止大角度偏差时的剧烈震荡
  double prop_term = k_delta_ * (delta - std::atan(-k_phi_ * phi));
  
  // 反馈项：动态增益调整，保证轨迹平滑
  double feedback_term = (1.0 + (k_phi_ / (1.0 + std::pow(k_phi_ * phi, 2)))) * std::sin(delta);
  
  // 最终曲率 k = -1/r * (...)
  return -1.0 / r * (prop_term + feedback_term);
}

geometry_msgs::Twist SmoothControlLaw::calculateRegularVelocity(
  const geometry_msgs::Pose & target_pose_in_robot_frame, 
  const bool & backward)
{
  // 构造虚拟机器人原点 (0,0,0)
  geometry_msgs::Pose robot_pose;
  robot_pose.orientation.w = 1.0;

  // 1. 获取极坐标 (使用修复后的逻辑)
  auto coords = EgoPolarCoords::getEgoPolarCoords(robot_pose, target_pose_in_robot_frame, backward);

  // 2. 计算曲率
  double k = calculateCurvature(coords.r, coords.phi, coords.delta);

  // 3. 计算线速度 (基于曲率的减速逻辑)
  double v = v_linear_max_ / (1.0 + beta_ * std::pow(std::fabs(k), lambda_));

  // 4. 终点减速逻辑 (线性插值)
  if (coords.r < slowdown_radius_) {
    v = v_linear_min_ + (v - v_linear_min_) * (coords.r / slowdown_radius_);
  }

  // 5. 计算角速度
  double w = k * v;

  // 6. 速度限幅
  v = std::max(v_linear_min_, std::min(v, v_linear_max_));
  w = std::max(-v_angular_max_, std::min(w, v_angular_max_));

  // 7. 输出指令 (处理倒车符号)
  geometry_msgs::Twist cmd_vel;
  if (backward) {
      cmd_vel.linear.x = -v;
      cmd_vel.angular.z = w; // 对于差速车，倒车时的旋转方向逻辑通常与前进一致(左转还是左转)
  } else {
      cmd_vel.linear.x = v;
      cmd_vel.angular.z = w;
  }

  return cmd_vel;
}

}  // namespace my_docking_controller
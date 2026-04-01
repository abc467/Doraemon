#ifndef MY_DOCKING_CONTROLLER__SMOOTH_CONTROL_LAW_HPP_
#define MY_DOCKING_CONTROLLER__SMOOTH_CONTROL_LAW_HPP_

#include "geometry_msgs/Pose.h"
#include "geometry_msgs/Twist.h"
#include "my_docking_controller/ego_polar_coords.hpp"

namespace my_docking_controller
{

/**
 * @class SmoothControlLaw
 * @brief 平滑控制律的核心实现 (移植自 Nav2 Graceful Controller)
 */
class SmoothControlLaw
{
public:
  /**
   * @brief 构造函数
   */
  SmoothControlLaw(
    double k_phi, double k_delta, double beta, double lambda,
    double slowdown_radius, double v_linear_min, double v_linear_max,
    double v_angular_max);

  ~SmoothControlLaw() = default;

  /**
   * @brief 核心方法：计算速度指令
   */
  geometry_msgs::Twist calculateRegularVelocity(
    const geometry_msgs::Pose & pose,
    const bool & backward);

private:
  /**
   * @brief 计算路径曲率 (核心数学公式)
   */
  double calculateCurvature(double r, double phi, double delta);

  double k_phi_, k_delta_, beta_, lambda_, slowdown_radius_;
  double v_linear_min_, v_linear_max_, v_angular_max_;
};

}  // namespace my_docking_controller

#endif
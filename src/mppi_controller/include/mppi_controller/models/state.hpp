#pragma once

#include <Eigen/Dense>

#include <geometry_msgs/PoseStamped.h>
#include <geometry_msgs/Twist.h>


namespace mppi::models
{

/**
 * @struct mppi::models::State
 * @brief State information: 速度集的时间序列, 控制速度集的时间序列, 当前位姿, 当前速度
 */
struct State
{
  Eigen::ArrayXXf vx;
  Eigen::ArrayXXf vy;
  Eigen::ArrayXXf wz;

  Eigen::ArrayXXf cvx;
  Eigen::ArrayXXf cvy;
  Eigen::ArrayXXf cwz;

  geometry_msgs::PoseStamped pose;  // 当前位姿
  geometry_msgs::Twist speed; // 当前速度

  /**
    * @brief Reset state data
    */
  void reset(unsigned int batch_size, unsigned int time_steps)
  {
    vx.setZero(batch_size, time_steps);
    vy.setZero(batch_size, time_steps);
    wz.setZero(batch_size, time_steps);

    cvx.setZero(batch_size, time_steps);
    cvy.setZero(batch_size, time_steps);
    cwz.setZero(batch_size, time_steps);
  }
};
}  // namespace mppi::models


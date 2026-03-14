#pragma once

#include <Eigen/Dense>

namespace mppi::models
{

/**
 * @struct mppi::models::Control
 * @brief 控制集
 */
struct Control
{
  float vx, vy, wz;
};

/**
 * @struct mppi::models::ControlSequence
 * @brief 控制集的时间序列
 */
struct ControlSequence
{
  Eigen::ArrayXf vx;
  Eigen::ArrayXf vy;
  Eigen::ArrayXf wz;

  void reset(unsigned int time_steps)
  {
    vx.setZero(time_steps);
    vy.setZero(time_steps);
    wz.setZero(time_steps);
  }
};

}  // namespace mppi::models


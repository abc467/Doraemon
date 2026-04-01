#pragma once

#include <Eigen/Dense>

namespace mppi::models
{

/**
 * @class mppi::models::Trajectories
 * @brief 候选轨迹集的时间序列
 */
struct Trajectories
{
  Eigen::ArrayXXf x;
  Eigen::ArrayXXf y;
  Eigen::ArrayXXf yaws;

  /**
    * @brief Reset state data
    */
  void reset(unsigned int batch_size, unsigned int time_steps)
  {
    x.setZero(batch_size, time_steps);
    y.setZero(batch_size, time_steps);
    yaws.setZero(batch_size, time_steps);
  }
};

}  // namespace mppi::models

#pragma once

#include <Eigen/Dense>

namespace mppi::models
{

/**
 * @struct mppi::models::Path
 * @brief Path represented as a tensor
 */
struct Path
{
  Eigen::ArrayXf x;
  Eigen::ArrayXf y;
  Eigen::ArrayXf yaws;

  /**
    * @brief Reset path data
    */
  void reset(unsigned int size)
  {
    x.setZero(size);
    y.setZero(size);
    yaws.setZero(size);
  }
};

}  // namespace mppi::models


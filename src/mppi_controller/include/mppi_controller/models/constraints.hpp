#pragma once

namespace mppi::models
{

/**
 * @struct mppi::models::ControlConstraints
 * @brief 控制约束
 */
struct ControlConstraints
{
  float vx_max;
  float vx_min;
  float vy;
  float wz;
  float ax_max;
  float ax_min;
  float ay_max;
  float az_max;
};

/**
 * @struct mppi::models::SamplingStd
 * @brief 采样轨迹的噪声参数
 */
struct SamplingStd
{
  float vx;
  float vy;
  float wz;
};

}


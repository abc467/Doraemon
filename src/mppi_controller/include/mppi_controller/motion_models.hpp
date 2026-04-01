#pragma once

#include <Eigen/Dense>

#include <cstdint>
#include <string>
#include <algorithm>

#include "mppi_controller/models/control_sequence.hpp"
#include "mppi_controller/models/state.hpp"
#include "mppi_controller/models/constraints.hpp"

namespace mppi
{

// Forward declaration of utils method, since utils.hpp can't be included here due
// to recursive inclusion.
namespace utils
{
  float clamp(const float lower_bound, const float upper_bound, const float input);
}

/**
 * @class mppi::MotionModel
 * @brief 抽象车辆运动模型
 */
class MotionModel
{
public:
  MotionModel() = default;
  virtual ~MotionModel() = default;

  /**
   * @brief 初始化
   * @param control_constraints 控制约束
   * @param model_dt 单个时间步长
   */
  void initialize(const models::ControlConstraints &control_constraints, float model_dt)
  {
    control_constraints_ = control_constraints;
    model_dt_ = model_dt;
  }

  /**
   * @brief 根据控制的速度(cvx, cvy, cwz)，推导出约束后的速度(vx, vy, wz)
   * @param state 包含控制约束
   */
  virtual void predict(models::State &state)
  {
    const bool is_holo = isHolonomic();
    float max_delta_vx = model_dt_ * control_constraints_.ax_max;
    float min_delta_vx = model_dt_ * control_constraints_.ax_min;
    float max_delta_vy = model_dt_ * control_constraints_.ay_max;
    float max_delta_wz = model_dt_ * control_constraints_.az_max;

    unsigned int n_rows = state.vx.rows();
    unsigned int n_cols = state.vx.cols();

    // Eigen 中的默认布局是列主序，因此以列主序方式访问元素以尽可能利用 L1 缓存
    for (unsigned int i = 1; i != n_cols; i++)
    {
      for (unsigned int j = 0; j != n_rows; j++)
      {
        float vx_last = state.vx(j, i - 1);
        float &cvx_curr = state.cvx(j, i - 1);
        cvx_curr = utils::clamp(vx_last + min_delta_vx, vx_last + max_delta_vx, cvx_curr);
        state.vx(j, i) = cvx_curr;

        float wz_last = state.wz(j, i - 1);
        float &cwz_curr = state.cwz(j, i - 1);
        cwz_curr = utils::clamp(wz_last - max_delta_wz, wz_last + max_delta_wz, cwz_curr);
        state.wz(j, i) = cwz_curr;

        if (is_holo)
        {
          float vy_last = state.vy(j, i - 1);
          float &cvy_curr = state.cvy(j, i - 1);
          cvy_curr = utils::clamp(vy_last - max_delta_vy, vy_last + max_delta_vy, cvy_curr);
          state.vy(j, i) = cvy_curr;
        }
      }
    }
  }

  /**
   * @brief Whether the motion model is holonomic, using Y axis
   * @return Bool If holonomic
   */
  virtual bool isHolonomic() = 0;

  /**
   * @brief Apply hard vehicle constraints to a control sequence
   * @param control_sequence Control sequence to apply constraints to
   */
  virtual void applyConstraints(models::ControlSequence & /*control_sequence*/) {}

protected:
  float model_dt_{0.0};
  models::ControlConstraints control_constraints_{0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f,
                                                  0.0f};
};

/**
 * @class mppi::DiffDriveMotionModel
 * @brief Differential drive motion model
 */
class DiffDriveMotionModel : public MotionModel
{
public:
  /**
   * @brief Constructor for mppi::DiffDriveMotionModel
   */
  DiffDriveMotionModel() = default;

  /**
   * @brief Whether the motion model is holonomic, using Y axis
   * @return Bool If holonomic
   */
  bool isHolonomic() override
  {
    return false;
  }
};

/**
 * @class mppi::OmniMotionModel
 * @brief Omnidirectional motion model
 */
class OmniMotionModel : public MotionModel
{
public:
  /**
   * @brief Constructor for mppi::OmniMotionModel
   */
  OmniMotionModel() = default;

  /**
   * @brief Whether the motion model is holonomic, using Y axis
   * @return Bool If holonomic
   */
  bool isHolonomic() override
  {
    return true;
  }
};

} // namespace mppi

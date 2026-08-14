#pragma once

#include <Eigen/Dense>

#include <cstdint>
#include <string>
#include <algorithm>
#include <cmath>
#include <vector>

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
  void initialize(
    const models::ControlConstraints & control_constraints, float model_dt,
    float model_delay_vx = 0.0f, float model_delay_vy = 0.0f,
    float model_delay_wz = 0.0f, bool clamp_raw_controls = false)
  {
    control_constraints_ = control_constraints;
    model_dt_ = model_dt;
    model_delay_vx_ = model_delay_vx;
    model_delay_vy_ = model_delay_vy;
    model_delay_wz_ = model_delay_wz;
    clamp_raw_controls_ = clamp_raw_controls;
    cmd_history_vx_.assign(offsetSteps(model_delay_vx_), 0.0f);
    cmd_history_vy_.assign(offsetSteps(model_delay_vy_), 0.0f);
    cmd_history_wz_.assign(offsetSteps(model_delay_wz_), 0.0f);
  }

  void pushCommandHistory(float vx, float vy, float wz)
  {
    pushOne(cmd_history_vx_, vx);
    pushOne(cmd_history_vy_, vy);
    pushOne(cmd_history_wz_, wz);
  }

  void clearCommandHistory()
  {
    std::fill(cmd_history_vx_.begin(), cmd_history_vx_.end(), 0.0f);
    std::fill(cmd_history_vy_.begin(), cmd_history_vy_.end(), 0.0f);
    std::fill(cmd_history_wz_.begin(), cmd_history_wz_.end(), 0.0f);
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
    float min_delta_vy = model_dt_ * control_constraints_.ay_min;
    float max_delta_wz = model_dt_ * control_constraints_.az_max;

    unsigned int n_cols = state.vx.cols();

    // Only rollout velocities are dynamically clamped by default. Keeping the
    // raw samples preserves the information-theoretic MPPI control cost.
    for (unsigned int i = 1; i != n_cols; ++i) {
      const auto vx_lower = (state.vx.col(i - 1) > 0.0f).select(
        state.vx.col(i - 1) + min_delta_vx,
        state.vx.col(i - 1) - max_delta_vx);
      const auto vx_upper = (state.vx.col(i - 1) > 0.0f).select(
        state.vx.col(i - 1) + max_delta_vx,
        state.vx.col(i - 1) - min_delta_vx);
      state.vx.col(i) = state.cvx.col(i - 1)
        .cwiseMax(vx_lower).cwiseMin(vx_upper);
      if (clamp_raw_controls_) {
        state.cvx.col(i - 1) = state.vx.col(i);
      }

      state.wz.col(i) = state.cwz.col(i - 1)
        .cwiseMax(state.wz.col(i - 1) - max_delta_wz)
        .cwiseMin(state.wz.col(i - 1) + max_delta_wz);
      if (clamp_raw_controls_) {
        state.cwz.col(i - 1) = state.wz.col(i);
      }

      if (is_holo) {
        const auto vy_lower = (state.vy.col(i - 1) > 0.0f).select(
          state.vy.col(i - 1) + min_delta_vy,
          state.vy.col(i - 1) - max_delta_vy);
        const auto vy_upper = (state.vy.col(i - 1) > 0.0f).select(
          state.vy.col(i - 1) + max_delta_vy,
          state.vy.col(i - 1) - min_delta_vy);
        state.vy.col(i) = state.cvy.col(i - 1)
          .cwiseMax(vy_lower).cwiseMin(vy_upper);
        if (clamp_raw_controls_) {
          state.cvy.col(i - 1) = state.vy.col(i);
        }
      }
    }

    const unsigned int offset_vx = static_cast<unsigned int>(offsetSteps(model_delay_vx_));
    const unsigned int offset_vy = static_cast<unsigned int>(offsetSteps(model_delay_vy_));
    const unsigned int offset_wz = static_cast<unsigned int>(offsetSteps(model_delay_wz_));
    if (offset_vx > 0u || offset_wz > 0u || (is_holo && offset_vy > 0u)) {
      applyDelayShift(state, is_holo, offset_vx, offset_vy, offset_wz);
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
  void applyDelayShift(
    models::State & state, bool is_holonomic,
    unsigned int offset_vx, unsigned int offset_vy,
    unsigned int offset_wz) const
  {
    const auto shift = [](
      Eigen::ArrayXXf & velocities, unsigned int offset,
      const std::vector<float> & history) {
        const unsigned int columns = static_cast<unsigned int>(velocities.cols());
        if (offset == 0u || columns == 0u) {
          return;
        }
        for (unsigned int index = offset < columns ? columns - offset : 0u;
          index > 0u; --index)
        {
          velocities.col(offset + index - 1u) = velocities.col(index);
        }
        const unsigned int end = std::min(offset, columns);
        for (unsigned int index = 1u; index < end; ++index) {
          velocities.col(index).setConstant(history[index]);
        }
      };
    shift(state.vx, offset_vx, cmd_history_vx_);
    shift(state.wz, offset_wz, cmd_history_wz_);
    if (is_holonomic) {
      shift(state.vy, offset_vy, cmd_history_vy_);
    }
  }

  std::size_t offsetSteps(float delay) const
  {
    if (delay <= 0.0f || model_dt_ <= 0.0f) {
      return 0u;
    }
    return static_cast<std::size_t>(std::floor(delay / model_dt_ + 0.5f));
  }

  static void pushOne(std::vector<float> & values, float value)
  {
    if (values.empty()) {
      return;
    }
    std::rotate(values.begin(), values.begin() + 1, values.end());
    values.back() = value;
  }

  float model_dt_{0.0};
  float model_delay_vx_{0.0f};
  float model_delay_vy_{0.0f};
  float model_delay_wz_{0.0f};
  bool clamp_raw_controls_{false};
  std::vector<float> cmd_history_vx_;
  std::vector<float> cmd_history_vy_;
  std::vector<float> cmd_history_wz_;
  models::ControlConstraints control_constraints_{
    0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f};
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

#pragma once

#include <algorithm>
#include <cmath>

namespace mppi
{

/** Pose-and-stopped goal policy used by the ROS1 adapter. */
class GoalReachedEvaluator
{
public:
  void configure(
    double position_tolerance, double yaw_tolerance,
    double stopped_linear_velocity, double stopped_angular_velocity,
    double stopped_hold_time)
  {
    configured_position_tolerance_ = std::max(0.0, position_tolerance);
    configured_yaw_tolerance_ = std::max(0.0, yaw_tolerance);
    stopped_linear_velocity_ = std::max(0.0, stopped_linear_velocity);
    stopped_angular_velocity_ = std::max(0.0, stopped_angular_velocity);
    stopped_hold_time_ = std::max(0.0, stopped_hold_time);
    restoreConfiguredTolerances();
  }

  void restoreConfiguredTolerances()
  {
    active_position_tolerance_ = configured_position_tolerance_;
    active_yaw_tolerance_ = configured_yaw_tolerance_;
    reset();
  }

  void applyToleranceUpperBounds(double position_tolerance, double yaw_tolerance)
  {
    const double bounded_position = std::isfinite(position_tolerance) &&
      position_tolerance >= 0.0 ?
      std::min(configured_position_tolerance_, position_tolerance) :
      configured_position_tolerance_;
    const double bounded_yaw = std::isfinite(yaw_tolerance) &&
      yaw_tolerance >= 0.0 ?
      std::min(configured_yaw_tolerance_, yaw_tolerance) :
      configured_yaw_tolerance_;
    constexpr double epsilon = 1e-12;
    if (std::fabs(bounded_position - active_position_tolerance_) > epsilon ||
        std::fabs(bounded_yaw - active_yaw_tolerance_) > epsilon)
    {
      active_position_tolerance_ = bounded_position;
      active_yaw_tolerance_ = bounded_yaw;
      reset();
    }
  }

  bool update(
    double position_error, double yaw_error,
    double linear_speed, double angular_speed,
    double now_seconds)
  {
    if (!std::isfinite(position_error) || !std::isfinite(yaw_error) ||
        !std::isfinite(linear_speed) || !std::isfinite(angular_speed) ||
        !std::isfinite(now_seconds) ||
        position_error > active_position_tolerance_ ||
        std::fabs(yaw_error) > active_yaw_tolerance_ ||
        std::fabs(linear_speed) > stopped_linear_velocity_ ||
        std::fabs(angular_speed) > stopped_angular_velocity_)
    {
      reset();
      return false;
    }

    if (!candidate_started_) {
      candidate_started_ = true;
      candidate_started_at_ = now_seconds;
    }
    if (stopped_hold_time_ <= 0.0 ||
        now_seconds - candidate_started_at_ >= stopped_hold_time_)
    {
      reached_ = true;
    }
    return reached_;
  }

  void reset()
  {
    candidate_started_ = false;
    candidate_started_at_ = 0.0;
    reached_ = false;
  }

  bool reached() const {return reached_;}
  double activePositionTolerance() const {return active_position_tolerance_;}
  double activeYawTolerance() const {return active_yaw_tolerance_;}

private:
  double configured_position_tolerance_{0.0};
  double configured_yaw_tolerance_{0.0};
  double active_position_tolerance_{0.0};
  double active_yaw_tolerance_{0.0};
  double stopped_linear_velocity_{0.0};
  double stopped_angular_velocity_{0.0};
  double stopped_hold_time_{0.0};
  double candidate_started_at_{0.0};
  bool candidate_started_{false};
  bool reached_{false};
};

}  // namespace mppi


#include "mppi_controller/optimizer.hpp"

#include <memory>
#include <stdexcept>
#include <string>
#include <vector>
#include <cmath>
#include <chrono>

#include "mppi_controller/optimal_trajectory_validator.hpp"

namespace mppi
{

void Optimizer::initialize(
  const ros::NodeHandle& nh, const std::string & name,
  std::shared_ptr<costmap_2d::Costmap2DROS> costmap_ros)
{
  nh_ = nh;
  name_ = name;
  costmap_ros_ = costmap_ros;
  costmap_ = costmap_ros_->getCostmap();

  getParams();

  critic_manager_.on_configure(nh, name_, costmap_ros_);
  noise_generator_.initialize(settings_, isHolonomic());

  if (trajectory_validation_enabled_) {
    std::string validator_plugin("mppi::DefaultOptimalTrajectoryValidator");
    nh_.param("TrajectoryValidator/plugin", validator_plugin, validator_plugin);
    validator_loader_ = std::make_unique<
      pluginlib::ClassLoader<OptimalTrajectoryValidator>>(
        "mppi_controller", "mppi::OptimalTrajectoryValidator");
    trajectory_validator_.reset(
      validator_loader_->createUnmanagedInstance(validator_plugin));
    trajectory_validator_->initialize(nh_, "TrajectoryValidator");
    ROS_INFO("Loaded MPPI trajectory validator: %s", validator_plugin.c_str());
  } else {
    ROS_WARN(
      "[%s] final continuous trajectory validation is disabled; publishing the "
      "CostCritic-validated softmax control sequence directly",
      name_.c_str());
  }

  reset();
}

void Optimizer::shutdown()
{
  noise_generator_.shutdown();
  trajectory_validator_.reset();
  validator_loader_.reset();
}

void Optimizer::getParams()
{
  std::string motion_model_name;

  auto & s = settings_;
  nh_.param("model_dt", s.model_dt, 0.05f);
  nh_.param("model_delay_vx", s.model_delay_vx, 0.0f);
  nh_.param("model_delay_vy", s.model_delay_vy, 0.0f);
  nh_.param("model_delay_wz", s.model_delay_wz, 0.0f);
  nh_.param("clamp_raw_controls", s.clamp_raw_controls, false);
  nh_.param("time_steps", s.time_steps, 56);
  nh_.param("batch_size", s.batch_size, 1000);
  nh_.param("iteration_count", s.iteration_count, 1);
  nh_.param("temperature", s.temperature, 0.3f);
  nh_.param("gamma", s.gamma, 0.015f);
  nh_.param("vx_max", s.base_constraints.vx_max, 0.8f);
  nh_.param("vx_min", s.base_constraints.vx_min, -0.5f);
  nh_.param("vy_max", s.base_constraints.vy, 0.1f); 
  nh_.param("wz_max", s.base_constraints.wz, 1.9f);
  nh_.param("ax_max", s.base_constraints.ax_max, 3.0f);
  nh_.param("ax_min", s.base_constraints.ax_min, -3.0f);
  nh_.param("ay_max", s.base_constraints.ay_max, 3.0f);
  nh_.param("ay_min", s.base_constraints.ay_min, -3.0f);
  nh_.param("az_max", s.base_constraints.az_max, 3.5f);
  nh_.param("vx_std", s.sampling_std.vx, 0.2f);
  nh_.param("vy_std", s.sampling_std.vy, 0.2f);
  nh_.param("wz_std", s.sampling_std.wz, 0.4f);
  nh_.param("retry_attempt_limit", s.retry_attempt_limit, 1);
  nh_.param("open_loop", s.open_loop, false);
  nh_.param("regenerate_noises", s.regenerate_noises, false);
  int sgf_order = 2;
  nh_.param("sgf_order", sgf_order, 2);
  if (sgf_order < 1 || sgf_order > 2) {
    ROS_WARN("sgf_order must be 1 or 2; using 2");
    sgf_order = 2;
  }
  s.sgf_order = static_cast<unsigned int>(sgf_order);
  nh_.param("timing_diagnostics", timing_diagnostics_, false);
  nh_.param(
    "TrajectoryValidator/enabled", trajectory_validation_enabled_, true);

  if (!std::isfinite(s.model_dt) || s.model_dt <= 0.0f ||
      s.time_steps < 2 || s.batch_size < 1 || s.iteration_count < 1 ||
      !std::isfinite(s.temperature) || s.temperature <= 0.0f ||
      !std::isfinite(s.gamma) || s.gamma < 0.0f ||
      !std::isfinite(s.model_delay_vx) || s.model_delay_vx < 0.0f ||
      !std::isfinite(s.model_delay_vy) || s.model_delay_vy < 0.0f ||
      !std::isfinite(s.model_delay_wz) || s.model_delay_wz < 0.0f ||
      !std::isfinite(s.sampling_std.vx) || s.sampling_std.vx <= 0.0f ||
      !std::isfinite(s.sampling_std.wz) || s.sampling_std.wz <= 0.0f)
  {
    throw std::invalid_argument("Invalid MPPI horizon, sampling, or temperature parameters");
  }
  s.base_constraints.ax_max = std::fabs(s.base_constraints.ax_max);
  if (s.base_constraints.ax_min > 0.0f) {
    ROS_WARN("ax_min should be negative; correcting its sign");
    s.base_constraints.ax_min = -s.base_constraints.ax_min;
  }
  s.base_constraints.ay_max = std::fabs(s.base_constraints.ay_max);
  if (s.base_constraints.ay_min > 0.0f) {
    ROS_WARN("ay_min should be negative; correcting its sign");
    s.base_constraints.ay_min = -s.base_constraints.ay_min;
  }
  s.base_constraints.az_max = std::fabs(s.base_constraints.az_max);

  if (!std::isfinite(s.base_constraints.vx_min) ||
      !std::isfinite(s.base_constraints.vx_max) ||
      s.base_constraints.vx_min > s.base_constraints.vx_max ||
      !std::isfinite(s.base_constraints.wz) || s.base_constraints.wz <= 0.0f ||
      !std::isfinite(s.base_constraints.ax_max) || s.base_constraints.ax_max <= 0.0f ||
      !std::isfinite(s.base_constraints.ax_min) ||
      s.base_constraints.ax_min >= 0.0f ||
      !std::isfinite(s.base_constraints.ay_max) || s.base_constraints.ay_max <= 0.0f ||
      !std::isfinite(s.base_constraints.ay_min) || s.base_constraints.ay_min >= 0.0f ||
      !std::isfinite(s.base_constraints.az_max) || s.base_constraints.az_max <= 0.0f)
  {
    throw std::invalid_argument("Invalid MPPI kinematic constraints");
  }
  s.retry_attempt_limit = std::max(0, s.retry_attempt_limit);

  nh_.param("motion_model", motion_model_name, std::string("DiffDrive"));
  

  s.constraints = s.base_constraints;

  setMotionModel(motion_model_name);

  if (isHolonomic() &&
      (!std::isfinite(s.sampling_std.vy) || s.sampling_std.vy <= 0.0f ||
       !std::isfinite(s.base_constraints.vy) || s.base_constraints.vy <= 0.0f))
  {
    throw std::invalid_argument(
      "Omni MPPI requires positive finite vy_std and vy_max");
  }

  double controller_frequency;
  nh_.param("controller_frequency", controller_frequency, 20.0);
  if (!std::isfinite(controller_frequency) || controller_frequency <= 0.0) {
    throw std::invalid_argument("MPPI controller_frequency must be positive");
  }
  setOffset(controller_frequency);
}

void Optimizer::setOffset(double controller_frequency)
{
  const double controller_period = 1.0 / controller_frequency;
  settings_.controller_period = static_cast<float>(controller_period);
  constexpr double eps = 1e-6;

  if ((controller_period + eps) < settings_.model_dt) {
    ROS_WARN(
      "Controller period is less then model dt, consider setting it equal");
  } else if (abs(controller_period - settings_.model_dt) < eps) {
    ROS_WARN(
      "Controller period is equal to model dt. Control sequence shifting is ON");
    settings_.shift_control_sequence = true;
  } else {
    throw std::invalid_argument(
      "MPPI controller period is greater than model_dt; set them equal");
  }
}

void Optimizer::reset(bool reset_dynamic_speed_limits)
{
  state_.reset(settings_.batch_size, settings_.time_steps);
  control_sequence_.reset(settings_.time_steps);
  control_history_[0] = {0.0f, 0.0f, 0.0f};
  control_history_[1] = {0.0f, 0.0f, 0.0f};
  control_history_[2] = {0.0f, 0.0f, 0.0f};
  control_history_[3] = {0.0f, 0.0f, 0.0f};
  last_command_vel_ = geometry_msgs::Twist();

  if (reset_dynamic_speed_limits) {
    settings_.constraints = settings_.base_constraints;
  }

  costs_.setZero(settings_.batch_size);
  generated_trajectories_.reset(settings_.batch_size, settings_.time_steps);

  noise_generator_.reset(settings_, isHolonomic());
  motion_model_->initialize(
    settings_.constraints, settings_.model_dt,
    settings_.model_delay_vx, settings_.model_delay_vy,
    settings_.model_delay_wz, settings_.clamp_raw_controls);
  motion_model_->clearCommandHistory();
  ROS_INFO("Optimizer reset");
}

bool Optimizer::isHolonomic() const
{
  return motion_model_->isHolonomic();
}

std::tuple<geometry_msgs::TwistStamped, Eigen::ArrayXXf> Optimizer::evalControl(
  const geometry_msgs::PoseStamped & robot_pose,
  const geometry_msgs::Twist & robot_speed,
  const nav_msgs::Path & plan,
  const geometry_msgs::Pose & goal)
{
  prepare(robot_pose, robot_speed, plan, goal);

  ValidationResult validation_result = ValidationResult::SOFT_RESET;
  Eigen::ArrayXXf optimal_trajectory;
  while (true) {
    optimize();
    optimal_trajectory = getOptimizedTrajectory();
    validation_result = critics_data_.fail_flag ?
      ValidationResult::SOFT_RESET :
      validateOptimizedTrajectory(optimal_trajectory);

    if (validation_result == ValidationResult::FAILURE) {
      throw std::runtime_error(
        "MPPI trajectory validator reported a non-recoverable failure");
    }

    const bool needs_fallback = validation_result == ValidationResult::SOFT_RESET;
    if (fallback(needs_fallback)) {
      continue;
    }
    break;
  }

  auto control = getControlFromSequenceAsTwist(plan.header.stamp);
  last_command_vel_ = control.twist;

  if (settings_.shift_control_sequence) {
    shiftControlSequence();
  }

  return std::make_tuple(control, std::move(optimal_trajectory));
}

void Optimizer::optimize()
{
  for (size_t i = 0; i < settings_.iteration_count; ++i) {
    const auto rollout_start = std::chrono::steady_clock::now();
    generateNoisedTrajectories();
    const auto critics_start = std::chrono::steady_clock::now();
    critic_manager_.evalTrajectoriesScores(critics_data_);
    const auto update_start = std::chrono::steady_clock::now();
    updateControlSequence();
    const auto update_end = std::chrono::steady_clock::now();

    if (timing_diagnostics_) {
      rollout_time_total_ms_ += std::chrono::duration<double, std::milli>(
        critics_start - rollout_start).count();
      critics_time_total_ms_ += std::chrono::duration<double, std::milli>(
        update_start - critics_start).count();
      update_time_total_ms_ += std::chrono::duration<double, std::milli>(
        update_end - update_start).count();

      if (++timing_cycles_ >= 50) {
        const double cycles = static_cast<double>(timing_cycles_);
        ROS_INFO(
          "[%s] MPPI optimizer average: rollout=%.3fms critics=%.3fms update=%.3fms",
          name_.c_str(),
          rollout_time_total_ms_ / cycles,
          critics_time_total_ms_ / cycles,
          update_time_total_ms_ / cycles);
        timing_cycles_ = 0;
        rollout_time_total_ms_ = 0.0;
        critics_time_total_ms_ = 0.0;
        update_time_total_ms_ = 0.0;
      }
    }
  }
}

bool Optimizer::fallback(bool fail)
{
  if (!fail) {
    fallback_counter_ = 0u;
    return false;
  }

  reset(false);
  resetCriticStateForRetry();

  if (++fallback_counter_ > static_cast<size_t>(settings_.retry_attempt_limit)) {
    fallback_counter_ = 0u;
    throw std::runtime_error(
      "MPPI optimizer could not produce a collision-free control");
  }

  return true;
}

void Optimizer::resetCriticStateForRetry()
{
  critics_data_.fail_flag = false;
  critics_data_.furthest_reached_path_point.reset();
  critics_data_.path_pts_valid.reset();
}

void Optimizer::prepare(
  const geometry_msgs::PoseStamped & robot_pose,
  const geometry_msgs::Twist & robot_speed,
  const nav_msgs::Path & plan,
  const geometry_msgs::Pose & goal)
{
  state_.pose = robot_pose;
  if (settings_.open_loop) {
    state_.speed = last_command_vel_;
  } else {
    // Compensate one controller period of command/measurement latency, as in
    // current upstream MPPI, while remaining inside the physical acceleration
    // envelope measured from odometry.
    const auto & constraints = settings_.constraints;
    const double period = settings_.controller_period;
    state_.speed = robot_speed;
    state_.speed.linear.x = std::clamp(
      last_command_vel_.linear.x,
      robot_speed.linear.x + period * constraints.ax_min,
      robot_speed.linear.x + period * constraints.ax_max);
    state_.speed.angular.z = std::clamp(
      last_command_vel_.angular.z,
      robot_speed.angular.z - period * constraints.az_max,
      robot_speed.angular.z + period * constraints.az_max);
    if (isHolonomic()) {
      state_.speed.linear.y = std::clamp(
        last_command_vel_.linear.y,
        robot_speed.linear.y + period * constraints.ay_min,
        robot_speed.linear.y + period * constraints.ay_max);
    }
  }
  state_.local_path_length = 0.0f;
  for (std::size_t index = 1u; index < plan.poses.size(); ++index) {
    state_.local_path_length += static_cast<float>(std::hypot(
      plan.poses[index].pose.position.x -
        plan.poses[index - 1u].pose.position.x,
      plan.poses[index].pose.position.y -
        plan.poses[index - 1u].pose.position.y));
  }
  path_ = utils::toTensor(plan);
  costs_.setZero();
  goal_ = goal;

  critics_data_.fail_flag = false;
  critics_data_.motion_model = motion_model_;
  critics_data_.furthest_reached_path_point.reset();
  critics_data_.path_pts_valid.reset();
}

void Optimizer::shiftControlSequence()
{
  auto size = control_sequence_.vx.size();
  utils::shiftColumnsByOnePlace(control_sequence_.vx, -1);
  utils::shiftColumnsByOnePlace(control_sequence_.wz, -1);
  control_sequence_.vx(size - 1) = control_sequence_.vx(size - 2);
  control_sequence_.wz(size - 1) = control_sequence_.wz(size - 2);

  if (isHolonomic()) {
    utils::shiftColumnsByOnePlace(control_sequence_.vy, -1);
    control_sequence_.vy(size - 1) = control_sequence_.vy(size - 2);
  }
}

void Optimizer::generateNoisedTrajectories()
{
  applyControlSequenceInterIterationConstraints();
  noise_generator_.setNoisedControls(state_, control_sequence_);
  noise_generator_.generateNextNoises();
  updateStateVelocities(state_);
  integrateStateVelocities(generated_trajectories_, state_);
}

void Optimizer::applyControlSequenceInterIterationConstraints()
{
  auto & s = settings_;
  const float dt = s.controller_period;
  const float max_delta_vx = dt * s.constraints.ax_max;
  const float min_delta_vx = dt * s.constraints.ax_min;
  const float max_delta_vy = dt * s.constraints.ay_max;
  const float min_delta_vy = dt * s.constraints.ay_min;
  const float max_delta_wz = dt * s.constraints.az_max;
  const float speed_vx = static_cast<float>(state_.speed.linear.x);
  const float speed_wz = static_cast<float>(state_.speed.angular.z);

  if (s.shift_control_sequence) {
    // Sequence zero represents the measured state. Sequence one is the next
    // command and must remain one controller period away from it.
    control_sequence_.vx(0) = speed_vx;
    control_sequence_.wz(0) = speed_wz;
    if (isHolonomic()) {
      control_sequence_.vy(0) = static_cast<float>(state_.speed.linear.y);
    }
  } else {
    control_sequence_.vx(0) = utils::clampVelocityByAccel(
      speed_vx, control_sequence_.vx(0), min_delta_vx, max_delta_vx);
    control_sequence_.wz(0) = utils::clampVelocityByAccel(
      speed_wz, control_sequence_.wz(0), -max_delta_wz, max_delta_wz);
    if (isHolonomic()) {
      const float speed_vy = static_cast<float>(state_.speed.linear.y);
      control_sequence_.vy(0) = utils::clampVelocityByAccel(
        speed_vy, control_sequence_.vy(0), min_delta_vy, max_delta_vy);
    }
  }
}

void Optimizer::applyControlSequenceConstraints()
{
  auto & s = settings_;
  motion_model_->applyConstraints(control_sequence_);

  float max_delta_vx = s.controller_period * s.constraints.ax_max;
  float min_delta_vx = s.controller_period * s.constraints.ax_min;
  float max_delta_vy = s.controller_period * s.constraints.ay_max;
  float min_delta_vy = s.controller_period * s.constraints.ay_min;
  float max_delta_wz = s.controller_period * s.constraints.az_max;
  float vx_last = static_cast<float>(state_.speed.linear.x);
  float wz_last = static_cast<float>(state_.speed.angular.z);
  float vy_last = isHolonomic() ?
    static_cast<float>(state_.speed.linear.y) : 0.0f;

  if (s.shift_control_sequence) {
    control_sequence_.vx(0) = vx_last;
    control_sequence_.wz(0) = wz_last;
    if (isHolonomic()) {
      control_sequence_.vy(0) = vy_last;
    }
  }

  for (unsigned int i = 0; i != control_sequence_.vx.size(); i++) {
    if (i == 1u) {
      max_delta_vx = s.model_dt * s.constraints.ax_max;
      min_delta_vx = s.model_dt * s.constraints.ax_min;
      max_delta_vy = s.model_dt * s.constraints.ay_max;
      min_delta_vy = s.model_dt * s.constraints.ay_min;
      max_delta_wz = s.model_dt * s.constraints.az_max;
    }
    float & vx_curr = control_sequence_.vx(i);
    vx_curr = utils::clamp(s.constraints.vx_min, s.constraints.vx_max, vx_curr);
    vx_curr = utils::clampVelocityByAccel(
      vx_last, vx_curr, min_delta_vx, max_delta_vx);
    vx_last = vx_curr;

    float & wz_curr = control_sequence_.wz(i);
    wz_curr = utils::clamp(-s.constraints.wz, s.constraints.wz, wz_curr);
    wz_curr = utils::clampVelocityByAccel(
      wz_last, wz_curr, -max_delta_wz, max_delta_wz);
    wz_last = wz_curr;

    if (isHolonomic()) {
      float & vy_curr = control_sequence_.vy(i);
      vy_curr = utils::clamp(-s.constraints.vy, s.constraints.vy, vy_curr);
      vy_curr = utils::clampVelocityByAccel(
        vy_last, vy_curr, min_delta_vy, max_delta_vy);
      vy_last = vy_curr;
    }
  }

  motion_model_->applyConstraints(control_sequence_);
}

void Optimizer::updateStateVelocities(
  models::State & state) const
{
  updateInitialStateVelocities(state);
  propagateStateVelocitiesFromInitials(state);
}

void Optimizer::updateInitialStateVelocities(
  models::State & state) const
{
  state.vx.col(0) = static_cast<float>(state.speed.linear.x);
  state.wz.col(0) = static_cast<float>(state.speed.angular.z);

  if (isHolonomic()) {
    state.vy.col(0) = static_cast<float>(state.speed.linear.y);
  }
}

void Optimizer::propagateStateVelocitiesFromInitials(
  models::State & state) const
{
  motion_model_->predict(state);
}

void Optimizer::integrateStateVelocities(
  Eigen::Array<float, Eigen::Dynamic, 3> & trajectory,
  const Eigen::ArrayXXf & sequence) const
{
  float initial_yaw = static_cast<float>(tf2::getYaw(state_.pose.pose.orientation));

  const auto vx = sequence.col(0);
  const auto wz = sequence.col(1);

  auto traj_x = trajectory.col(0);
  auto traj_y = trajectory.col(1);
  auto traj_yaws = trajectory.col(2);

  size_t n_size = traj_yaws.size();

  traj_yaws(0) = wz(0) * settings_.model_dt + initial_yaw;
  float last_yaw = traj_yaws(0);
  for(size_t i = 1; i != n_size; i++) {
    float & curr_yaw = traj_yaws(i);
    curr_yaw = last_yaw + wz(i) * settings_.model_dt;
    last_yaw = curr_yaw;
  }

  Eigen::ArrayXf yaw_cos = traj_yaws.cos();
  Eigen::ArrayXf yaw_sin = traj_yaws.sin();
  utils::shiftColumnsByOnePlace(yaw_cos, 1);
  utils::shiftColumnsByOnePlace(yaw_sin, 1);
  yaw_cos(0) = cosf(initial_yaw);
  yaw_sin(0) = sinf(initial_yaw);

  auto dx = (vx * yaw_cos).eval();
  auto dy = (vx * yaw_sin).eval();

  if (isHolonomic()) {
    auto vy = sequence.col(2);
    dx = (dx - vy * yaw_sin).eval();
    dy = (dy + vy * yaw_cos).eval();
  }

  traj_x(0) = state_.pose.pose.position.x + dx(0) * settings_.model_dt;
  traj_y(0) = state_.pose.pose.position.y + dy(0) * settings_.model_dt;
  float last_x = traj_x(0);
  float last_y = traj_y(0);
  for(unsigned int i = 1; i != n_size; i++) {
    float & curr_x = traj_x(i);
    float & curr_y = traj_y(i);
    curr_x = last_x + dx(i) * settings_.model_dt;
    curr_y = last_y + dy(i) * settings_.model_dt;
    last_x = curr_x;
    last_y = curr_y;
  }
}

void Optimizer::integrateStateVelocities(
  models::Trajectories & trajectories,
  const models::State & state) const
{
  const float initial_yaw = static_cast<float>(tf2::getYaw(state.pose.pose.orientation));
  const unsigned int n_cols = trajectories.yaws.cols();

  trajectories.yaws.col(0) = state.wz.col(0) * settings_.model_dt + initial_yaw;
  for(unsigned int i = 1; i != n_cols; i++) {
    trajectories.yaws.col(i) = trajectories.yaws.col(i - 1) + state.wz.col(i) * settings_.model_dt;
  }

  Eigen::ArrayXXf yaw_cos = trajectories.yaws.cos();
  Eigen::ArrayXXf yaw_sin = trajectories.yaws.sin();
  utils::shiftColumnsByOnePlace(yaw_cos, 1);
  utils::shiftColumnsByOnePlace(yaw_sin, 1);
  yaw_cos.col(0) = cosf(initial_yaw);
  yaw_sin.col(0) = sinf(initial_yaw);

  auto dx = (state.vx * yaw_cos).eval();
  auto dy = (state.vx * yaw_sin).eval();

  if (isHolonomic()) {
    dx = dx - state.vy * yaw_sin;
    dy = dy + state.vy * yaw_cos;
  }

  trajectories.x.col(0) = dx.col(0) * settings_.model_dt + state.pose.pose.position.x;
  trajectories.y.col(0) = dy.col(0) * settings_.model_dt + state.pose.pose.position.y;
  for(unsigned int i = 1; i != n_cols; i++) {
    trajectories.x.col(i) = trajectories.x.col(i - 1) + dx.col(i) * settings_.model_dt;
    trajectories.y.col(i) = trajectories.y.col(i - 1) + dy.col(i) * settings_.model_dt;
  }
}

Eigen::ArrayXXf Optimizer::getOptimizedTrajectory()
{
  const bool is_holo = isHolonomic();
  Eigen::ArrayXXf sequence = Eigen::ArrayXXf(settings_.time_steps, is_holo ? 3 : 2);
  Eigen::Array<float, Eigen::Dynamic, 3> trajectories =
    Eigen::Array<float, Eigen::Dynamic, 3>(settings_.time_steps, 3);

  sequence.col(0) = control_sequence_.vx;
  sequence.col(1) = control_sequence_.wz;

  if (is_holo) {
    sequence.col(2) = control_sequence_.vy;
  }

  integrateStateVelocities(trajectories, sequence);
  return trajectories;
}

void Optimizer::updateControlSequence()
{
  const bool is_holo = isHolonomic();
  auto & s = settings_;
  auto bounded_noises_vx = state_.cvx.rowwise() - control_sequence_.vx.transpose();
  auto bounded_noises_wz = state_.cwz.rowwise() - control_sequence_.wz.transpose();
  costs_ += (s.gamma / powf(s.sampling_std.vx, 2) *
    (bounded_noises_vx.rowwise() * control_sequence_.vx.transpose()).rowwise().sum()).eval();
  costs_ += (s.gamma / powf(s.sampling_std.wz, 2) *
    (bounded_noises_wz.rowwise() * control_sequence_.wz.transpose()).rowwise().sum()).eval();
  if (is_holo) {
    auto bounded_noises_vy = state_.cvy.rowwise() - control_sequence_.vy.transpose();
    costs_ += (s.gamma / powf(s.sampling_std.vy, 2) *
      (bounded_noises_vy.rowwise() * control_sequence_.vy.transpose()).rowwise().sum()).eval();
  }

  auto costs_normalized = costs_ - costs_.minCoeff();
  auto exponents = ((-1 / settings_.temperature * costs_normalized).exp()).eval();
  auto softmaxes = (exponents / exponents.sum()).eval();

  control_sequence_.vx = (state_.cvx.colwise() * softmaxes).colwise().sum();
  control_sequence_.wz = (state_.cwz.colwise() * softmaxes).colwise().sum();
  if (is_holo) {
    control_sequence_.vy = (state_.cvy.colwise() * softmaxes).colwise().sum();
  }

  // Match current upstream order: smoothing happens before the final hard
  // kinematic projection, so filtering can never leave an invalid command.
  utils::savitskyGolayFilter(control_sequence_, control_history_, settings_);
  applyControlSequenceConstraints();
}

ValidationResult Optimizer::validateOptimizedTrajectory(
  const Eigen::ArrayXXf & trajectory, bool emit_logs) const
{
  if (!trajectory_validation_enabled_) {
    return ValidationResult::SUCCESS;
  }
  if (!costmap_ || !costmap_ros_) {
    return ValidationResult::FAILURE;
  }
  if (!trajectory_validator_) {
    return ValidationResult::FAILURE;
  }
  const auto result = trajectory_validator_->validate(
    *costmap_, costmap_ros_->getRobotFootprint(),
    state_.pose.pose, trajectory);
  if (emit_logs && result == ValidationResult::SOFT_RESET) {
    ROS_WARN_THROTTLE(
      1.0, "MPPI rejected the selected trajectory in final footprint validation");
  } else if (emit_logs && result == ValidationResult::FAILURE) {
    ROS_ERROR_THROTTLE(
      1.0, "MPPI trajectory validator reported a structural/configuration failure");
  }
  return result;
}

geometry_msgs::TwistStamped Optimizer::getControlFromSequenceAsTwist(
  const ros::Time & stamp)
{
  unsigned int offset = settings_.shift_control_sequence ? 1 : 0;

  auto vx = control_sequence_.vx(offset);
  auto wz = control_sequence_.wz(offset);
  const auto vy = isHolonomic() ? control_sequence_.vy(offset) : 0.0f;

  motion_model_->pushCommandHistory(vx, vy, wz);

  if (isHolonomic()) {
    return utils::toTwistStamped(vx, vy, wz, stamp, costmap_ros_->getBaseFrameID());
  }

  return utils::toTwistStamped(vx, wz, stamp, costmap_ros_->getBaseFrameID());
}

void Optimizer::setMotionModel(const std::string & model)
{
  if (model == "DiffDrive") {
    motion_model_ = std::make_shared<DiffDriveMotionModel>();
  } else if (model == "Omni") {
    motion_model_ = std::make_shared<OmniMotionModel>();
  } else {
    ROS_ERROR_STREAM(
            std::string(
              "Model " + model + " is not valid! Valid options are DiffDrive, Omni "));
    throw std::runtime_error("Invalid motion model");
  }
  motion_model_->initialize(
    settings_.constraints, settings_.model_dt,
    settings_.model_delay_vx, settings_.model_delay_vy,
    settings_.model_delay_wz, settings_.clamp_raw_controls);
}

void Optimizer::setSpeedLimit(double speed_limit, bool percentage)
{
  auto & s = settings_;
  double ratio = 1.0;
  if (std::isfinite(speed_limit) && speed_limit >= 0.0) {
    ratio = percentage ? speed_limit / 100.0 :
      speed_limit / std::max(1e-6f, std::fabs(s.base_constraints.vx_max));
    ratio = std::clamp(ratio, 0.0, 1.0);
  }
  s.constraints.vx_max = s.base_constraints.vx_max * ratio;
  s.constraints.vx_min = s.base_constraints.vx_min * ratio;
  s.constraints.vy = s.base_constraints.vy * ratio;
  s.constraints.wz = s.base_constraints.wz * ratio;
  motion_model_->initialize(
    s.constraints, s.model_dt,
    s.model_delay_vx, s.model_delay_vy, s.model_delay_wz,
    s.clamp_raw_controls);
}

bool Optimizer::isSpeedLimitActive() const
{
  const auto & current = settings_.constraints;
  const auto & base = settings_.base_constraints;
  constexpr float epsilon = 1e-6f;
  return std::fabs(current.vx_max - base.vx_max) > epsilon ||
         std::fabs(current.vx_min - base.vx_min) > epsilon ||
         std::fabs(current.vy - base.vy) > epsilon ||
         std::fabs(current.wz - base.wz) > epsilon;
}

models::Trajectories & Optimizer::getGeneratedTrajectories()
{
  return generated_trajectories_;
}

}  // namespace mppi

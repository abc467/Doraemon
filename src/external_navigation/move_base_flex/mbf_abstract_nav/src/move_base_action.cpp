/*
 *  Copyright 2018, Magazino GmbH, Sebastian Pütz, Jorge Santos Simón
 *
 *  Redistribution and use in source and binary forms, with or without
 *  modification, are permitted provided that the following conditions
 *  are met:
 *
 *  1. Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *
 *  2. Redistributions in binary form must reproduce the above
 *     copyright notice, this list of conditions and the following
 *     disclaimer in the documentation and/or other materials provided
 *     with the distribution.
 *
 *  3. Neither the name of the copyright holder nor the names of its
 *     contributors may be used to endorse or promote products derived
 *     from this software without specific prior written permission.
 *
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *  "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *  LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
 *  FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
 *  COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 *  INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 *  BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 *  LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 *  CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 *  LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
 *  ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 *  POSSIBILITY OF SUCH DAMAGE.
 *
 *  move_base_action.cpp
 *
 *  authors:
 *    Sebastian Pütz <spuetz@uni-osnabrueck.de>
 *    Jorge Santos Simón <santos@magazino.eu>
 *
 */

#include <algorithm>
#include <cmath>
#include <limits>
#include <sstream>

#include <angles/angles.h>
#include <mbf_utility/navigation_utility.h>
#include <tf2/utils.h>

#include "mbf_abstract_nav/MoveBaseFlexConfig.h"
#include "mbf_abstract_nav/move_base_action.h"

namespace mbf_abstract_nav
{

namespace
{

double clampUnit(double value)
{
  return std::max(0.0, std::min(1.0, value));
}

bool finitePlanPose(const geometry_msgs::Pose &pose)
{
  return std::isfinite(pose.position.x) &&
      std::isfinite(pose.position.y) &&
      std::isfinite(tf2::getYaw(pose.orientation));
}

struct PlanJoinMetrics
{
  bool has_candidate{false};
  bool admissible{false};
  double nearest_xy{std::numeric_limits<double>::infinity()};
  double yaw_at_nearest_xy{std::numeric_limits<double>::infinity()};
  double best_score{std::numeric_limits<double>::infinity()};
  double accepted_xy{std::numeric_limits<double>::infinity()};
  double accepted_yaw{std::numeric_limits<double>::infinity()};
  std::size_t accepted_segment{0u};
  double accepted_x{std::numeric_limits<double>::quiet_NaN()};
  double accepted_y{std::numeric_limits<double>::quiet_NaN()};
  double accepted_heading{std::numeric_limits<double>::quiet_NaN()};
};

void considerPlanJoinCandidate(
    double x, double y, double yaw, std::size_t segment,
    const geometry_msgs::Pose &robot_pose,
    double max_distance, double max_yaw,
    PlanJoinMetrics &metrics)
{
  if (!std::isfinite(x) || !std::isfinite(y) || !std::isfinite(yaw))
    return;

  const double distance = std::hypot(
      robot_pose.position.x - x, robot_pose.position.y - y);
  const double yaw_error = std::fabs(angles::shortest_angular_distance(
      yaw, tf2::getYaw(robot_pose.orientation)));
  if (!std::isfinite(distance) || !std::isfinite(yaw_error))
    return;

  metrics.has_candidate = true;
  if (distance < metrics.nearest_xy)
  {
    metrics.nearest_xy = distance;
    metrics.yaw_at_nearest_xy = yaw_error;
  }

  if (distance > max_distance || yaw_error > max_yaw)
    return;

  const double score = distance / std::max(1e-6, max_distance) +
      yaw_error / std::max(1e-6, max_yaw);
  if (score < metrics.best_score)
  {
    metrics.admissible = true;
    metrics.best_score = score;
    metrics.accepted_xy = distance;
    metrics.accepted_yaw = yaw_error;
    metrics.accepted_segment = segment;
    metrics.accepted_x = x;
    metrics.accepted_y = y;
    metrics.accepted_heading = yaw;
  }
}

bool forwardJoinSegment(
    const geometry_msgs::Pose &start,
    const geometry_msgs::Pose &end,
    double projected_yaw)
{
  const double dx = end.position.x - start.position.x;
  const double dy = end.position.y - start.position.y;
  const double distance = std::hypot(dx, dy);
  if (!std::isfinite(distance) || distance <= 1e-4 ||
      !std::isfinite(projected_yaw))
  {
    // A continuous update must never join inside an in-place rotation.  The
    // next translating edge is considered independently by the caller.
    return false;
  }

  constexpr double kMinimumForwardProjection = 0.25;
  const double inverse_distance = 1.0 / distance;
  const double projected_alignment =
      (dx * std::cos(projected_yaw) + dy * std::sin(projected_yaw)) *
      inverse_distance;
  const double end_yaw = tf2::getYaw(end.orientation);
  const double end_alignment =
      (dx * std::cos(end_yaw) + dy * std::sin(end_yaw)) *
      inverse_distance;
  return std::isfinite(projected_alignment) &&
      std::isfinite(end_alignment) &&
      projected_alignment >= kMinimumForwardProjection &&
      end_alignment >= kMinimumForwardProjection;
}

PlanJoinMetrics nearestReachablePathSuffix(
    const nav_msgs::Path &path,
    const geometry_msgs::Pose &robot_pose,
    double max_distance,
    double max_yaw)
{
  PlanJoinMetrics metrics;
  if (path.poses.empty() || !finitePlanPose(robot_pose))
    return metrics;

  if (path.poses.size() == 1u)
  {
    const auto &pose = path.poses.front().pose;
    if (finitePlanPose(pose))
      considerPlanJoinCandidate(
          pose.position.x, pose.position.y, tf2::getYaw(pose.orientation),
          0u, robot_pose, max_distance, max_yaw, metrics);
    return metrics;
  }

  // Project onto every segment, not merely path.front().  The selected point
  // defines a suffix from the robot's current progress to the unchanged goal.
  // This admits a robot which has advanced along a still-valid plan while a
  // bounded asynchronous replan was running.
  for (std::size_t i = 0u; i + 1u < path.poses.size(); ++i)
  {
    const auto &start = path.poses[i].pose;
    const auto &end = path.poses[i + 1u].pose;
    if (!finitePlanPose(start) || !finitePlanPose(end))
      continue;

    const double dx = end.position.x - start.position.x;
    const double dy = end.position.y - start.position.y;
    const double length_sq = dx * dx + dy * dy;
    const double start_yaw = tf2::getYaw(start.orientation);
    const double yaw_delta = angles::shortest_angular_distance(
        start_yaw, tf2::getYaw(end.orientation));
    double t = 0.0;
    if (length_sq > 1e-12)
    {
      t = clampUnit(
          ((robot_pose.position.x - start.position.x) * dx +
           (robot_pose.position.y - start.position.y) * dy) / length_sq);
    }
    else if (std::fabs(yaw_delta) > 1e-9)
    {
      // For an in-place lattice primitive, project in orientation instead of
      // arbitrarily testing only one end of the rotation.
      t = clampUnit(
          angles::shortest_angular_distance(
              start_yaw, tf2::getYaw(robot_pose.orientation)) / yaw_delta);
    }

    const double projected_yaw =
        angles::normalize_angle(start_yaw + t * yaw_delta);
    if (!forwardJoinSegment(start, end, projected_yaw))
    {
      continue;
    }
    considerPlanJoinCandidate(
        start.position.x + t * dx,
        start.position.y + t * dy,
        projected_yaw,
        i, robot_pose, max_distance, max_yaw, metrics);
  }
  return metrics;
}

bool buildJoinedPath(
    const nav_msgs::Path &path,
    const PlanJoinMetrics &metrics,
    nav_msgs::Path &joined_path)
{
  joined_path = nav_msgs::Path();
  if (!metrics.admissible || path.poses.size() < 2u ||
      metrics.accepted_segment + 1u >= path.poses.size() ||
      !std::isfinite(metrics.accepted_x) ||
      !std::isfinite(metrics.accepted_y) ||
      !std::isfinite(metrics.accepted_heading))
  {
    return false;
  }

  joined_path.header = path.header;
  geometry_msgs::PoseStamped join_pose =
      path.poses[metrics.accepted_segment];
  join_pose.header = path.poses[metrics.accepted_segment].header;
  if (join_pose.header.frame_id.empty())
  {
    join_pose.header.frame_id = path.header.frame_id;
  }
  join_pose.pose.position.x = metrics.accepted_x;
  join_pose.pose.position.y = metrics.accepted_y;
  join_pose.pose.orientation.x = 0.0;
  join_pose.pose.orientation.y = 0.0;
  join_pose.pose.orientation.z = std::sin(0.5 * metrics.accepted_heading);
  join_pose.pose.orientation.w = std::cos(0.5 * metrics.accepted_heading);
  joined_path.poses.push_back(join_pose);

  for (std::size_t index = metrics.accepted_segment + 1u;
       index < path.poses.size(); ++index)
  {
    const auto &candidate = path.poses[index];
    const auto &previous = joined_path.poses.back();
    const double position_delta = std::hypot(
        candidate.pose.position.x - previous.pose.position.x,
        candidate.pose.position.y - previous.pose.position.y);
    const double yaw_delta = std::fabs(angles::shortest_angular_distance(
        tf2::getYaw(previous.pose.orientation),
        tf2::getYaw(candidate.pose.orientation)));
    if (position_delta <= 1e-9 && yaw_delta <= 1e-9)
    {
      continue;
    }
    joined_path.poses.push_back(candidate);
  }
  return joined_path.poses.size() >= 2u;
}

}  // namespace

MoveBaseAction::MoveBaseAction(const std::string& name, const mbf_utility::RobotInformation& robot_info,
                               const std::vector<std::string>& behaviors,
                               const ContinuousPlanUpdater &continuous_plan_updater)
  : name_(name)
  , robot_info_(robot_info)
  , private_nh_("~")
  , action_client_exe_path_(private_nh_, "exe_path")
  , action_client_get_path_(private_nh_, "get_path")
  , action_client_replanning_(private_nh_, "get_path")
  , action_client_recovery_(private_nh_, "recovery")
  , continuous_plan_updater_(continuous_plan_updater)
  , oscillation_timeout_(0)
  , oscillation_distance_(0)
  , recovery_enabled_(true)
  , behaviors_(behaviors)
  , action_state_(NONE)
  , recovery_trigger_(NONE)
  , dist_to_goal_(std::numeric_limits<double>::infinity())
{
  private_nh_.param(
      "periodic_plan_max_age", periodic_plan_max_age_, 10.0);
  private_nh_.param(
      "periodic_plan_max_join_distance",
      periodic_plan_max_join_distance_, 0.15);
  private_nh_.param(
      "periodic_plan_max_join_yaw", periodic_plan_max_join_yaw_, 0.35);
  periodic_plan_max_age_ =
      std::isfinite(periodic_plan_max_age_) ?
      std::max(0.01, periodic_plan_max_age_) : 10.0;
  periodic_plan_max_join_distance_ =
      std::isfinite(periodic_plan_max_join_distance_) ?
      std::max(0.01, periodic_plan_max_join_distance_) : 0.15;
  periodic_plan_max_join_yaw_ =
      std::isfinite(periodic_plan_max_join_yaw_) ?
      std::max(0.01, std::min(M_PI, periodic_plan_max_join_yaw_)) : 0.35;

  // Start only after every lifecycle field has been initialized. The previous
  // initializer-list launch allowed the thread to observe uninitialized state.
  replanning_thread_ = boost::thread(
      boost::bind(&MoveBaseAction::replanningThread, this));
}

MoveBaseAction::~MoveBaseAction()
{
  shutting_down_.store(true);
  action_state_ = NONE;
  action_client_replanning_.cancelAllGoals();
  replanning_thread_.join();
}

void MoveBaseAction::reconfigure(
    mbf_abstract_nav::MoveBaseFlexConfig &config, uint32_t level)
{
  boost::lock_guard<boost::mutex> guard(lifecycle_mtx_);
  if (config.planner_frequency > 0.0)
  {
    replanning_period_seconds_.store(1.0 / config.planner_frequency);
  }
  else
  {
    replanning_period_seconds_.store(0.0);
    // Disabling periodic replanning (for example during precision docking)
    // also stops an already-running refresh.  Generation invalidation makes a
    // late result harmless even if a planner needs time to observe cancel().
    periodic_plan_generation_.fetch_add(1u);
    if (!action_client_replanning_.getState().isDone())
      action_client_replanning_.cancelGoal();
  }
  oscillation_timeout_ = ros::Duration(config.oscillation_timeout);
  oscillation_distance_ = config.oscillation_distance;
  recovery_enabled_ = config.recovery_enabled;
}

void MoveBaseAction::cancel()
{
  boost::lock_guard<boost::mutex> guard(lifecycle_mtx_);
  const bool settle_outer_goal = has_active_goal_.load();
  has_active_goal_ = false;
  action_state_ = CANCELED;
  execution_epoch_.fetch_add(1u);
  periodic_plan_generation_.fetch_add(1u);
  fresh_plan_generation_.fetch_add(1u);
  controller_generation_.fetch_add(1u);

  if (settle_outer_goal)
  {
    mbf_msgs::MoveBaseResult result;
    result.outcome = mbf_msgs::MoveBaseResult::CANCELED;
    result.message = "Move base action canceled";
    result.final_pose = robot_pose_;
    goal_handle_.setCanceled(result, result.message);
  }

  cancelChildActions();
}

void MoveBaseAction::cancel(GoalHandle &goal_handle)
{
  boost::lock_guard<boost::mutex> guard(lifecycle_mtx_);
  if (!has_active_goal_ || goal_handle != goal_handle_)
  {
    ROS_DEBUG_STREAM_NAMED(
        "move_base", "Ignoring cancel for non-current move_base goal id="
        << goal_handle.getGoalID().id);
    return;
  }

  // Resolve the exact handle supplied by the action server before cancelling
  // child goals. get_path callbacks are generation-gated, so waiting for a
  // RECALLED/PREEMPTED callback here can otherwise leave this outer goal in a
  // zombie ACTIVE state forever.
  has_active_goal_ = false;
  action_state_ = CANCELED;
  execution_epoch_.fetch_add(1u);
  periodic_plan_generation_.fetch_add(1u);
  fresh_plan_generation_.fetch_add(1u);
  controller_generation_.fetch_add(1u);

  mbf_msgs::MoveBaseResult move_base_result;
  move_base_result.outcome = mbf_msgs::MoveBaseResult::CANCELED;
  move_base_result.message = "Move base action canceled";
  move_base_result.final_pose = robot_pose_;
  goal_handle.setCanceled(move_base_result, move_base_result.message);

  cancelChildActions();
}

void MoveBaseAction::cancelChildActions()
{
  if (!action_client_replanning_.getState().isDone())
  {
    action_client_replanning_.cancelGoal();
  }

  if (!action_client_get_path_.getState().isDone())
  {
    action_client_get_path_.cancelGoal();
  }

  if (!action_client_exe_path_.getState().isDone())
  {
    action_client_exe_path_.cancelGoal();
  }

  if (!action_client_recovery_.getState().isDone())
  {
    action_client_recovery_.cancelGoal();
  }
}

void MoveBaseAction::start(GoalHandle &goal_handle)
{
  if (goal_handle.getGoalStatus().status == actionlib_msgs::GoalStatus::RECALLING)
  {
    mbf_msgs::MoveBaseResult result;
    result.outcome = mbf_msgs::MoveBaseResult::CANCELED;
    result.message = "Move base goal canceled before acceptance";
    goal_handle.setCanceled(result, result.message);
    return;
  }

  const mbf_msgs::MoveBaseGoal goal = *goal_handle.getGoal();

  GoalHandle superseded_goal;
  bool has_superseded_goal = false;
  std::uint64_t execution_epoch = 0u;
  std::uint64_t request_generation = 0u;
  mbf_msgs::GetPathGoal initial_get_path_goal;
  {
    boost::lock_guard<boost::mutex> guard(lifecycle_mtx_);
    if (has_active_goal_ && goal_handle != goal_handle_)
    {
      superseded_goal = goal_handle_;
      has_superseded_goal = true;
    }

    // A MoveBase goal is always a fresh controller execution. Take ownership
    // before cancelling child goals so a delayed callback cannot settle the
    // newly accepted outer goal.
    has_active_goal_ = true;
    goal_handle_ = goal_handle;
    periodic_plan_generation_.fetch_add(1u);
    execution_epoch = execution_epoch_.fetch_add(1u) + 1u;
    request_generation = fresh_plan_generation_.fetch_add(1u) + 1u;
    controller_generation_.fetch_add(1u);
    action_state_ = GET_PATH;

    // Every mutable field below belongs to this exact outer-goal epoch.  Keep
    // it in the ownership critical section so a slow, superseded start()
    // callback cannot overwrite the newer goal's target/controller/recovery
    // templates after it has lost ownership.
    dist_to_goal_ = std::numeric_limits<double>::infinity();
    goal_pose_ = goal.target_pose;
    last_oscillation_reset_ = ros::Time::now();
    recovery_trigger_ = NONE;
    recovery_behaviors_ =
        goal.recovery_behaviors.empty() ? behaviors_ : goal.recovery_behaviors;
    current_recovery_behavior_ = recovery_behaviors_.begin();
    {
      boost::lock_guard<boost::mutex> template_guard(goal_template_mtx_);
      get_path_goal_.target_pose = goal.target_pose;
      get_path_goal_.use_start_pose = false; // use the robot pose
      get_path_goal_.planner = goal.planner;
      exe_path_goal_.controller = goal.controller;
      exe_path_goal_.has_requested_target = true;
      exe_path_goal_.requested_target = goal.target_pose;
      initial_get_path_goal = get_path_goal_;
    }

    goal_handle.setAccepted();
    cancelChildActions();
  }

  if (has_superseded_goal)
  {
    mbf_msgs::MoveBaseResult superseded_result;
    superseded_result.outcome = mbf_msgs::MoveBaseResult::CANCELED;
    superseded_result.message = "Move base goal superseded by a newer goal";
    superseded_goal.setCanceled(
        superseded_result, superseded_result.message);
  }

  ROS_DEBUG_STREAM_NAMED("move_base", "Start action \"move_base\"");

  mbf_msgs::MoveBaseResult move_base_result;

  ros::Duration connection_timeout(1.0);

  // get the current robot pose only at the beginning, as exe_path will keep updating it as we move
  geometry_msgs::PoseStamped initial_robot_pose;
  if (!robot_info_.getRobotPose(initial_robot_pose))
  {
    ROS_ERROR_STREAM_NAMED("move_base", "Could not get the current robot pose!");
    move_base_result.message = "Could not get the current robot pose!";
    move_base_result.outcome = mbf_msgs::MoveBaseResult::TF_ERROR;
    boost::lock_guard<boost::mutex> guard(lifecycle_mtx_);
    if (has_active_goal_ && goal_handle == goal_handle_ &&
        execution_epoch == execution_epoch_.load())
    {
      has_active_goal_ = false;
      goal_handle.setAborted(move_base_result, move_base_result.message);
    }
    return;
  }
  {
    boost::lock_guard<boost::mutex> guard(lifecycle_mtx_);
    if (!has_active_goal_ || goal_handle != goal_handle_ ||
        execution_epoch != execution_epoch_.load())
    {
      return;
    }
    robot_pose_ = initial_robot_pose;
    last_oscillation_pose_ = initial_robot_pose;
  }

  // wait for server connections
  if (!action_client_get_path_.waitForServer(connection_timeout) ||
      !action_client_replanning_.waitForServer(connection_timeout) ||
      !action_client_exe_path_.waitForServer(connection_timeout) ||
      !action_client_recovery_.waitForServer(connection_timeout))
  {
    ROS_ERROR_STREAM_NAMED("move_base", "Could not connect to one or more of move_base_flex actions: "
        "\"get_path\", \"exe_path\", \"recovery \"!");
    move_base_result.outcome = mbf_msgs::MoveBaseResult::INTERNAL_ERROR;
    move_base_result.message = "Could not connect to the move_base_flex actions!";
    boost::lock_guard<boost::mutex> guard(lifecycle_mtx_);
    if (has_active_goal_ && goal_handle == goal_handle_ &&
        execution_epoch == execution_epoch_.load())
    {
      has_active_goal_ = false;
      goal_handle.setAborted(move_base_result, move_base_result.message);
    }
    return;
  }

  // call get_path action server to get a first plan
  {
    boost::lock_guard<boost::mutex> guard(lifecycle_mtx_);
    if (has_active_goal_ && execution_epoch == execution_epoch_.load())
    {
      action_client_get_path_.sendGoal(
          initial_get_path_goal,
          boost::bind(
              &MoveBaseAction::actionGetPathDone, this, _1, _2,
              request_generation, execution_epoch));
    }
  }
}

void MoveBaseAction::actionExePathActive(
    std::uint64_t execution_epoch,
    std::uint64_t controller_generation)
{
  boost::lock_guard<boost::mutex> guard(lifecycle_mtx_);
  if (!has_active_goal_ || execution_epoch != execution_epoch_.load() ||
      controller_generation != controller_generation_.load())
  {
    return;
  }
  ROS_DEBUG_STREAM_NAMED("move_base", "The \"exe_path\" action is active.");
}

void MoveBaseAction::actionExePathFeedback(
    const mbf_msgs::ExePathFeedbackConstPtr &feedback,
    std::uint64_t execution_epoch,
    std::uint64_t controller_generation)
{
  boost::lock_guard<boost::mutex> guard(lifecycle_mtx_);
  if (!has_active_goal_ || execution_epoch != execution_epoch_.load() ||
      controller_generation != controller_generation_.load())
  {
    return;
  }
  mbf_msgs::MoveBaseFeedback move_base_feedback;
  move_base_feedback.outcome = feedback->outcome;
  move_base_feedback.message = feedback->message;
  move_base_feedback.angle_to_goal = feedback->angle_to_goal;
  move_base_feedback.dist_to_goal = feedback->dist_to_goal;
  move_base_feedback.current_pose = feedback->current_pose;
  move_base_feedback.last_cmd_vel = feedback->last_cmd_vel;
  goal_handle_.publishFeedback(move_base_feedback);
  dist_to_goal_ = feedback->dist_to_goal;
  robot_pose_ = feedback->current_pose;

  // we create a navigation-level oscillation detection using exe_path action's feedback,
  // as the later doesn't handle oscillations created by quickly failing repeated plans

  // if oscillation detection is enabled by oscillation_timeout != 0
  if (!oscillation_timeout_.isZero())
  {
    // check if oscillating
    // moved more than the minimum oscillation distance
    if (mbf_utility::distance(robot_pose_, last_oscillation_pose_) >= oscillation_distance_)
    {
      last_oscillation_reset_ = ros::Time::now();
      last_oscillation_pose_ = robot_pose_;

      if (recovery_trigger_ == OSCILLATING)
      {
        ROS_INFO_NAMED("move_base", "Recovered from robot oscillation: restart recovery behaviors");
        current_recovery_behavior_ = recovery_behaviors_.begin();
        recovery_trigger_ = NONE;
      }
    }
    else if (last_oscillation_reset_ + oscillation_timeout_ < ros::Time::now())
    {
      std::stringstream oscillation_msgs;
      oscillation_msgs << "Robot is oscillating for " << (ros::Time::now() - last_oscillation_reset_).toSec() << "s!";
      ROS_WARN_STREAM_NAMED("move_base", oscillation_msgs.str());
      action_client_exe_path_.cancelGoal();

      if (attemptRecovery())
      {
        recovery_trigger_ = OSCILLATING;
      }
      else
      {
        periodic_plan_generation_.fetch_add(1u);
        controller_generation_.fetch_add(1u);
        action_state_ = FAILED;
        if (!action_client_replanning_.getState().isDone())
        {
          action_client_replanning_.cancelGoal();
        }
        mbf_msgs::MoveBaseResult move_base_result;
        move_base_result.outcome = mbf_msgs::MoveBaseResult::OSCILLATION;
        move_base_result.message = oscillation_msgs.str();
        move_base_result.final_pose = robot_pose_;
        move_base_result.angle_to_goal = move_base_feedback.angle_to_goal;
        move_base_result.dist_to_goal = move_base_feedback.dist_to_goal;
        has_active_goal_ = false;
        goal_handle_.setAborted(move_base_result, move_base_result.message);
      }
    }
  }
}

void MoveBaseAction::actionGetPathDone(
    const actionlib::SimpleClientGoalState &state,
    const mbf_msgs::GetPathResultConstPtr &result_ptr,
    std::uint64_t request_generation,
    std::uint64_t execution_epoch)
{
  boost::lock_guard<boost::mutex> guard(lifecycle_mtx_);
  if (!has_active_goal_ ||
      request_generation != fresh_plan_generation_.load() ||
      execution_epoch != execution_epoch_.load() ||
      action_state_.load() != GET_PATH)
  {
    ROS_WARN_STREAM_NAMED(
        "move_base", "Ignoring stale fresh planner result generation="
        << request_generation << " epoch=" << execution_epoch);
    return;
  }
  mbf_msgs::GetPathResult get_path_result;
  if (result_ptr)
  {
    get_path_result = *result_ptr;
  }
  else
  {
    get_path_result.outcome = mbf_msgs::GetPathResult::INTERNAL_ERROR;
    get_path_result.message = "get_path completed without a result payload";
  }
  mbf_msgs::MoveBaseResult move_base_result;

  // copy result from get_path action
  fillMoveBaseResult(get_path_result, move_base_result);

  switch (state.state_)
  {
    case actionlib::SimpleClientGoalState::PENDING:
      ROS_FATAL_STREAM_NAMED("move_base", "get_path PENDING state not implemented, this should not be reachable!");
      break;

    case actionlib::SimpleClientGoalState::SUCCEEDED:
    {
      if (!result_ptr)
      {
        has_active_goal_ = false;
        action_state_ = FAILED;
        goal_handle_.setAborted(
            move_base_result, get_path_result.message);
        break;
      }
      ROS_DEBUG_STREAM_NAMED("move_base", "Action \""
          << "move_base\" received a path from \""
          << "get_path\": " << state.getText());

      mbf_msgs::ExePathGoal fresh_goal;
      {
        boost::lock_guard<boost::mutex> guard(goal_template_mtx_);
        fresh_goal = exe_path_goal_;
      }
      fresh_goal.path = get_path_result.path;
      fresh_goal.execution_epoch = execution_epoch;
      fresh_goal.plan_update_mode = mbf_msgs::ExePathGoal::FRESH_EXECUTION;
      const std::uint64_t controller_generation =
          controller_generation_.fetch_add(1u) + 1u;
      ROS_DEBUG_STREAM_NAMED("move_base", "Action \""
          << "move_base\" sends the path to \""
          << "exe_path\".");

      if (recovery_trigger_ == GET_PATH)
      {
        ROS_WARN_NAMED("move_base", "Recovered from planner failure: restart recovery behaviors");
        current_recovery_behavior_ = recovery_behaviors_.begin();
        recovery_trigger_ = NONE;
      }

      action_state_ = EXE_PATH;
      action_client_exe_path_.sendGoal(
          fresh_goal,
          boost::bind(
              &MoveBaseAction::actionExePathDone, this, _1, _2,
              execution_epoch, controller_generation),
          boost::bind(
              &MoveBaseAction::actionExePathActive, this, execution_epoch,
              controller_generation),
          boost::bind(
              &MoveBaseAction::actionExePathFeedback, this, _1,
              execution_epoch, controller_generation));
      break;
    }

    case actionlib::SimpleClientGoalState::ABORTED:

      if (attemptRecovery())
      {
        recovery_trigger_ = GET_PATH;
      }
      else
      {
        // copy result from get_path action
        ROS_WARN_STREAM_NAMED("move_base", "Abort the execution of the planner: " << get_path_result.message);
        has_active_goal_ = false;
        goal_handle_.setAborted(move_base_result, state.getText());
        action_state_ = FAILED;
      }
      break;

    case actionlib::SimpleClientGoalState::RECALLED:
    case actionlib::SimpleClientGoalState::PREEMPTED:
      ROS_INFO_STREAM_NAMED("move_base", "The last action goal to \"get_path\" has been " << state.toString());
      // Outer cancellation/new-goal preemption invalidates ownership before
      // this callback, so a current PREEMPTED result is an internal planner
      // timeout/cancel and must terminate or recover rather than becoming a
      // zombie GET_PATH action.
      if (attemptRecovery())
      {
        recovery_trigger_ = GET_PATH;
      }
      else
      {
        has_active_goal_ = false;
        action_state_ = FAILED;
        goal_handle_.setAborted(move_base_result, state.getText());
      }
      break;

    case actionlib::SimpleClientGoalState::REJECTED:
      ROS_ERROR_STREAM_NAMED("move_base", "The last action goal to \"get_path\" has been " << state.toString());
      has_active_goal_ = false;
      goal_handle_.setAborted(move_base_result, state.getText());
      action_state_ = FAILED;
      break;

    case actionlib::SimpleClientGoalState::LOST:
      ROS_FATAL_STREAM_NAMED("move_base", "Connection lost to the action \"get_path\"!");
      has_active_goal_ = false;
      goal_handle_.setAborted();
      action_state_ = FAILED;
      break;

    default:
      ROS_FATAL_STREAM_NAMED("move_base", "Reached unknown action server state!");
      has_active_goal_ = false;
      goal_handle_.setAborted();
      action_state_ = FAILED;
      break;
  }
}

void MoveBaseAction::actionExePathDone(
    const actionlib::SimpleClientGoalState &state,
    const mbf_msgs::ExePathResultConstPtr &result_ptr,
    std::uint64_t execution_epoch,
    std::uint64_t controller_generation)
{
  boost::lock_guard<boost::mutex> guard(lifecycle_mtx_);
  if (!has_active_goal_ || execution_epoch != execution_epoch_.load() ||
      controller_generation != controller_generation_.load())
  {
    ROS_DEBUG_STREAM_NAMED(
        "move_base", "Ignoring stale exe_path result epoch="
        << execution_epoch << " controller_generation="
        << controller_generation);
    return;
  }
  // The authoritative controller execution is terminal.  Do not leave a
  // potentially 60-second periodic State Lattice search running after success
  // or an unrecoverable failure; its late path must never enter the next state.
  periodic_plan_generation_.fetch_add(1u);
  if (!action_client_replanning_.getState().isDone())
    action_client_replanning_.cancelGoal();
  ROS_DEBUG_STREAM_NAMED("move_base", "Action \"exe_path\" finished.");

  mbf_msgs::ExePathResult exe_path_result;
  if (result_ptr)
  {
    exe_path_result = *result_ptr;
  }
  else
  {
    exe_path_result.outcome = mbf_msgs::ExePathResult::INTERNAL_ERROR;
    exe_path_result.message = "exe_path completed without a result payload";
  }
  mbf_msgs::MoveBaseResult move_base_result;

  // copy result from exe_path action
  fillMoveBaseResult(exe_path_result, move_base_result);

  ROS_DEBUG_STREAM_NAMED("move_base", "Current state: " << state.toString());

  switch (state.state_)
  {
    case actionlib::SimpleClientGoalState::SUCCEEDED:
      if (!result_ptr)
      {
        has_active_goal_ = false;
        action_state_ = FAILED;
        goal_handle_.setAborted(
            move_base_result, exe_path_result.message);
        break;
      }
      move_base_result.outcome = mbf_msgs::MoveBaseResult::SUCCESS;
      move_base_result.message = "Action \"move_base\" succeeded!";
      ROS_INFO_STREAM_NAMED("move_base", move_base_result.message);
      has_active_goal_ = false;
      goal_handle_.setSucceeded(move_base_result, move_base_result.message);
      action_state_ = SUCCEEDED;
      break;

    case actionlib::SimpleClientGoalState::ABORTED:
      action_state_ = FAILED;

      switch (exe_path_result.outcome)
      {
        case mbf_msgs::ExePathResult::INVALID_PATH:
        case mbf_msgs::ExePathResult::TF_ERROR:
        case mbf_msgs::ExePathResult::NOT_INITIALIZED:
        case mbf_msgs::ExePathResult::INVALID_PLUGIN:
        case mbf_msgs::ExePathResult::INTERNAL_ERROR:
          // none of these errors is recoverable
          has_active_goal_ = false;
          goal_handle_.setAborted(move_base_result, state.getText());
          break;

        default:
          // all the rest are, so we start calling the recovery behaviors in sequence

          if (attemptRecovery())
          {
            recovery_trigger_ = EXE_PATH;
          }
          else
          {
            ROS_WARN_STREAM_NAMED("move_base", "Abort the execution of the controller: " << exe_path_result.message);
            has_active_goal_ = false;
            goal_handle_.setAborted(move_base_result, state.getText());
          }
          break;
      }
      break;

    case actionlib::SimpleClientGoalState::RECALLED:
    case actionlib::SimpleClientGoalState::PREEMPTED:
      ROS_INFO_STREAM_NAMED("move_base", "The last action goal to \"exe_path\" has been " << state.toString());
      // Deliberately superseded continuous plans carry an older controller
      // generation and were rejected above.  A PREEMPTED result for the
      // current generation is therefore a real controller failure.
      action_state_ = FAILED;
      if (attemptRecovery())
      {
        recovery_trigger_ = EXE_PATH;
      }
      else
      {
        has_active_goal_ = false;
        goal_handle_.setAborted(move_base_result, state.getText());
      }
      break;

    case actionlib::SimpleClientGoalState::REJECTED:
      ROS_ERROR_STREAM_NAMED("move_base", "The last action goal to \"exe_path\" has been " << state.toString());
      has_active_goal_ = false;
      goal_handle_.setAborted(move_base_result, state.getText());
      action_state_ = FAILED;
      break;

    case actionlib::SimpleClientGoalState::LOST:
      ROS_FATAL_STREAM_NAMED("move_base", "Connection lost to the action \"exe_path\"!");
      has_active_goal_ = false;
      goal_handle_.setAborted();
      action_state_ = FAILED;
      break;

    default:
      ROS_FATAL_STREAM_NAMED("move_base", "Reached unreachable case! Unknown SimpleActionServer state!");
      has_active_goal_ = false;
      goal_handle_.setAborted();
      action_state_ = FAILED;
      break;
  }
}

bool MoveBaseAction::attemptRecovery()
{
  if (!recovery_enabled_)
  {
    ROS_WARN_STREAM_NAMED("move_base", "Recovery behaviors are disabled!");
    return false;
  }

  if (current_recovery_behavior_ == recovery_behaviors_.end())
  {
    if (recovery_behaviors_.empty())
    {
      ROS_WARN_STREAM_NAMED("move_base", "No Recovery Behaviors loaded!");
    }
    else
    {
      ROS_WARN_STREAM_NAMED("move_base", "Executed all available recovery behaviors!");
    }
    return false;
  }

  recovery_goal_.behavior = *current_recovery_behavior_;
  // No periodic plan may survive across the EXE_PATH -> RECOVERY boundary.
  periodic_plan_generation_.fetch_add(1u);
  controller_generation_.fetch_add(1u);
  if (!action_client_replanning_.getState().isDone())
  {
    action_client_replanning_.cancelGoal();
  }
  action_state_ = RECOVERY;
  ROS_DEBUG_STREAM_NAMED("move_base", "Start recovery behavior\""
      << *current_recovery_behavior_ <<"\".");
  action_client_recovery_.sendGoal(
      recovery_goal_,
      boost::bind(
          &MoveBaseAction::actionRecoveryDone, this, _1, _2,
          execution_epoch_.load())
  );
  return true;
}

void MoveBaseAction::actionRecoveryDone(
    const actionlib::SimpleClientGoalState &state,
    const mbf_msgs::RecoveryResultConstPtr &result_ptr,
    std::uint64_t execution_epoch)
{
  boost::lock_guard<boost::mutex> guard(lifecycle_mtx_);
  if (!has_active_goal_ || execution_epoch != execution_epoch_.load())
  {
    ROS_DEBUG_STREAM_NAMED(
        "move_base", "Ignoring stale recovery result epoch="
        << execution_epoch);
    return;
  }
  // give the robot some time to stop oscillating after executing the recovery behavior
  last_oscillation_reset_ = ros::Time::now();

  mbf_msgs::RecoveryResult recovery_result;
  if (result_ptr)
  {
    recovery_result = *result_ptr;
  }
  else
  {
    recovery_result.outcome = mbf_msgs::RecoveryResult::INTERNAL_ERROR;
    recovery_result.message = "recovery completed without a result payload";
  }
  mbf_msgs::MoveBaseResult move_base_result;

  // copy result from recovery action
  fillMoveBaseResult(recovery_result, move_base_result);

  switch (state.state_)
  {
    case actionlib::SimpleClientGoalState::REJECTED:
    case actionlib::SimpleClientGoalState::ABORTED:
      action_state_ = FAILED;

      ROS_DEBUG_STREAM_NAMED("move_base", "The recovery behavior \""
          << *current_recovery_behavior_ << "\" has failed. ");
      ROS_DEBUG_STREAM("Recovery behavior message: " << recovery_result.message
                                    << ", outcome: " << recovery_result.outcome);

      current_recovery_behavior_++; // use next behavior;
      if (current_recovery_behavior_ == recovery_behaviors_.end())
      {
        ROS_DEBUG_STREAM_NAMED("move_base",
                               "All recovery behaviors failed. Abort recovering and abort the move_base action");
        has_active_goal_ = false;
        goal_handle_.setAborted(move_base_result, "All recovery behaviors failed.");
      }
      else
      {
        recovery_goal_.behavior = *current_recovery_behavior_;

        ROS_INFO_STREAM_NAMED("move_base", "Run the next recovery behavior \""
            << *current_recovery_behavior_ << "\".");
        action_client_recovery_.sendGoal(
            recovery_goal_,
            boost::bind(
                &MoveBaseAction::actionRecoveryDone, this, _1, _2,
                execution_epoch)
        );
      }
      break;
    case actionlib::SimpleClientGoalState::SUCCEEDED:
      if (!result_ptr)
      {
        has_active_goal_ = false;
        action_state_ = FAILED;
        goal_handle_.setAborted(
            move_base_result, recovery_result.message);
        break;
      }
      //go to planning state
      ROS_DEBUG_STREAM_NAMED("move_base", "Execution of the recovery behavior \""
          << *current_recovery_behavior_ << "\" succeeded!");
      ROS_DEBUG_STREAM_NAMED("move_base",
                             "Try planning again and increment the current recovery behavior in the list.");
      action_state_ = GET_PATH;
      current_recovery_behavior_++; // use next behavior, the next time;
      {
        const std::uint64_t execution_epoch =
            execution_epoch_.fetch_add(1u) + 1u;
        const std::uint64_t request_generation =
            fresh_plan_generation_.fetch_add(1u) + 1u;
        mbf_msgs::GetPathGoal fresh_get_path_goal;
        {
          boost::lock_guard<boost::mutex> guard(goal_template_mtx_);
          fresh_get_path_goal = get_path_goal_;
        }
        action_client_get_path_.sendGoal(
            fresh_get_path_goal,
            boost::bind(
                &MoveBaseAction::actionGetPathDone, this, _1, _2,
                request_generation, execution_epoch)
        );
      }
      break;
    case actionlib::SimpleClientGoalState::RECALLED:
    case actionlib::SimpleClientGoalState::PREEMPTED:
      ROS_INFO_STREAM_NAMED("move_base", "The last action goal to \"recovery\" has been preempted!");
      // A current preempt cannot be caused by outer cancellation/new-goal
      // ownership (those invalidate the epoch first). Treat it as a failed
      // recovery attempt and either advance or terminate the outer goal.
      action_state_ = FAILED;
      ++current_recovery_behavior_;
      if (current_recovery_behavior_ == recovery_behaviors_.end())
      {
        has_active_goal_ = false;
        goal_handle_.setAborted(
            move_base_result, "Recovery action was preempted and no fallback remains");
      }
      else
      {
        recovery_goal_.behavior = *current_recovery_behavior_;
        action_state_ = RECOVERY;
        action_client_recovery_.sendGoal(
            recovery_goal_,
            boost::bind(
                &MoveBaseAction::actionRecoveryDone, this, _1, _2,
                execution_epoch));
      }
      break;

    case actionlib::SimpleClientGoalState::LOST:
      ROS_FATAL_STREAM_NAMED("move_base", "Connection lost to the action \"recovery\"!");
      has_active_goal_ = false;
      goal_handle_.setAborted();
      action_state_ = FAILED;
      break;
    default:
      ROS_FATAL_STREAM_NAMED("move_base", "Reached unreachable case! Unknown state!");
      has_active_goal_ = false;
      goal_handle_.setAborted();
      action_state_ = FAILED;
      break;
  }
}

bool MoveBaseAction::periodicPlanIsFreshAndReachable(
    const nav_msgs::Path &path,
    const ros::WallTime &request_started_at,
    nav_msgs::Path &joined_path,
    std::string &reason) const
{
  reason.clear();
  joined_path = nav_msgs::Path();
  const double age = request_started_at.isZero() ?
      std::numeric_limits<double>::infinity() :
      (ros::WallTime::now() - request_started_at).toSec();
  if (!std::isfinite(age) || age < -0.01 ||
      age > periodic_plan_max_age_)
  {
    std::ostringstream stream;
    stream << "periodic plan age " << age << " s exceeds "
           << periodic_plan_max_age_ << " s";
    reason = stream.str();
    return false;
  }
  if (path.poses.empty())
  {
    reason = "periodic planner returned an empty path";
    return false;
  }

  geometry_msgs::PoseStamped current_pose;
  if (!robot_info_.getRobotPose(current_pose))
  {
    reason = "cannot obtain current robot pose for periodic-plan admission";
    return false;
  }
  if (!path.header.frame_id.empty() &&
      !current_pose.header.frame_id.empty() &&
      path.header.frame_id != current_pose.header.frame_id)
  {
    reason = "periodic plan and current robot pose use different frames";
    return false;
  }

  const PlanJoinMetrics metrics = nearestReachablePathSuffix(
      path, current_pose.pose,
      periodic_plan_max_join_distance_, periodic_plan_max_join_yaw_);
  if (!metrics.has_candidate)
  {
    reason = "periodic plan contains no finite SE(2) suffix candidate";
    return false;
  }
  if (!metrics.admissible)
  {
    std::ostringstream stream;
    stream << "current robot pose cannot join periodic path suffix: nearest_xy="
           << metrics.nearest_xy << " m yaw_at_nearest="
           << metrics.yaw_at_nearest_xy << " rad limits="
           << periodic_plan_max_join_distance_ << " m/"
           << periodic_plan_max_join_yaw_ << " rad";
    reason = stream.str();
    return false;
  }
  if (!buildJoinedPath(path, metrics, joined_path))
  {
    reason = "periodic plan join did not produce a finite forward suffix";
    return false;
  }

  std::ostringstream stream;
  stream << "age=" << age << " s join_segment="
         << metrics.accepted_segment << " xy=" << metrics.accepted_xy
         << " m yaw=" << metrics.accepted_yaw << " rad joined_poses="
         << joined_path.poses.size();
  reason = stream.str();
  return true;
}

bool MoveBaseAction::replanningActive() const
{
  // replan only while following a path and if replanning is enabled (can be disabled by dynamic reconfigure)
  return has_active_goal_.load() && replanning_period_seconds_.load() > 0.0 &&
      action_state_.load() == EXE_PATH && dist_to_goal_.load() > 0.1;
}

void MoveBaseAction::replanningThread()
{
  ros::Duration update_period(0.005);
  ros::Time last_replan_time(0.0);
  std::uint64_t scheduled_epoch = 0u;
  std::uint64_t request_generation = 0u;
  std::uint64_t request_epoch = 0u;
  ros::WallTime request_started_at;

  while (ros::ok() && !shutting_down_.load())
  {
    if (!action_client_replanning_.getState().isDone())
    {
      if (action_client_replanning_.waitForResult(update_period))
      {
        actionlib::SimpleClientGoalState state =
            action_client_replanning_.getState();
        mbf_msgs::GetPathResultConstPtr result =
            action_client_replanning_.getResult();
        const bool result_is_current =
            request_generation == periodic_plan_generation_.load() &&
            request_epoch == execution_epoch_.load();
        if (state == actionlib::SimpleClientGoalState::SUCCEEDED &&
            result && result_is_current && replanningActive())
        {
          std::string admission_reason;
          nav_msgs::Path joined_path;
          if (!periodicPlanIsFreshAndReachable(
                  result->path, request_started_at,
                  joined_path, admission_reason))
          {
            ROS_WARN_STREAM_NAMED(
                "move_base", "Discarding unsafe/stale periodic plan: "
                << admission_reason);
          }
          else
          {
            // The expensive TF/suffix check intentionally runs outside the
            // lifecycle lock. Recheck ownership before the atomic in-process
            // handoff so cancel/success can always win this race.
            boost::lock_guard<boost::mutex> guard(lifecycle_mtx_);
            if (!has_active_goal_ ||
                request_generation != periodic_plan_generation_.load() ||
                request_epoch != execution_epoch_.load() ||
                !replanningActive())
            {
              ROS_DEBUG_STREAM_NAMED(
                  "move_base", "Discarding periodic planner result that lost "
                  "outer-goal ownership before controller handoff");
            }
            else
            {
              mbf_msgs::ExePathGoal goal;
              {
                boost::lock_guard<boost::mutex> template_guard(goal_template_mtx_);
                goal = exe_path_goal_;
              }
              // Install only the suffix beginning at the exact projection
              // admitted above.  Passing the raw planner path would replay
              // its historical start and invalidate controller progress.
              goal.path = std::move(joined_path);
              goal.execution_epoch = request_epoch;
              goal.plan_update_mode = mbf_msgs::ExePathGoal::CONTINUOUS_UPDATE;

              std::string update_reason;
              const bool updated = continuous_plan_updater_ &&
                  continuous_plan_updater_(goal, update_reason);
              if (updated)
              {
                ROS_INFO_STREAM_NAMED(
                    "move_base", "Installed periodic plan without replacing "
                    "the ExePath goal (" << admission_reason << ")");
              }
              else
              {
                // The original ExePath action remains authoritative. If it
                // reached a terminal state during admission, its existing
                // callback will settle MoveBase normally.
                ROS_DEBUG_STREAM_NAMED(
                    "move_base", "Periodic plan handoff rejected without "
                    "preempting controller: " << update_reason);
              }
            }
          }
        }
        else if (!result_is_current)
        {
          ROS_WARN_STREAM_NAMED(
              "move_base", "Discarding stale periodic planner result generation="
              << request_generation << " epoch=" << request_epoch);
        }
        else
        {
          ROS_DEBUG_STREAM_NAMED(
              "move_base", "Periodic replanning did not produce a usable "
              "current result (state=" << state.toString() << ")");
        }
        // The configured period is the quiet interval between completed
        // refreshes, not merely between planner starts.  A long State Lattice
        // search therefore cannot cause a second search to launch
        // immediately when it returns.
        last_replan_time = ros::Time::now();
      }
      // else keep waiting for planning to complete (we already waited update_period in waitForResult)
    }
    else if (!replanningActive())
    {
      // Arm a fresh full period when a new controller epoch starts or when
      // dynamic reconfigure enables replanning again.  Without this reset the
      // zero-initialized timestamp caused an immediate redundant replan as
      // soon as the initial path entered EXE_PATH.
      scheduled_epoch = 0u;
      update_period.sleep();
    }
    else if (scheduled_epoch != execution_epoch_.load())
    {
      scheduled_epoch = execution_epoch_.load();
      last_replan_time = ros::Time::now();
      update_period.sleep();
    }
    else if (ros::Time::now() - last_replan_time >=
             ros::Duration(replanning_period_seconds_.load()))
    {
      // Serialize the final ownership check and sendGoal with cancel()/start().
      // Without this lock a stale worker can pass replanningActive(), lose its
      // outer goal, and then submit an old get_path goal after cancellation.
      boost::lock_guard<boost::mutex> lifecycle_guard(lifecycle_mtx_);
      if (!has_active_goal_ || !replanningActive())
      {
        continue;
      }
      ROS_DEBUG_STREAM_NAMED("move_base", "Next replanning cycle, using the \"get_path\" action!");
      request_epoch = execution_epoch_.load();
      request_generation = periodic_plan_generation_.fetch_add(1u) + 1u;
      mbf_msgs::GetPathGoal periodic_goal;
      {
        boost::lock_guard<boost::mutex> template_guard(goal_template_mtx_);
        periodic_goal = get_path_goal_;
      }
      action_client_replanning_.sendGoal(periodic_goal);
      request_started_at = ros::WallTime::now();
      last_replan_time = ros::Time::now();
    }
    else
    {
      // The worker has no work until the next deadline.  Sleeping here avoids
      // burning an entire CPU core while a controller is active between the
      // configured replanning ticks.
      update_period.sleep();
    }
  }
}

} /* namespace mbf_abstract_nav */

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
 *  abstract_controller_execution.cpp
 *
 *  authors:
 *    Sebastian Pütz <spuetz@uni-osnabrueck.de>
 *    Jorge Santos Simón <santos@magazino.eu>
 *
 */

#include <algorithm>
#include <cmath>
#include <sstream>

#include <mbf_msgs/ExePathResult.h>

#include "mbf_abstract_nav/abstract_controller_execution.h"

namespace mbf_abstract_nav
{

const double AbstractControllerExecution::DEFAULT_CONTROLLER_FREQUENCY = 100.0; // 100 Hz

AbstractControllerExecution::AbstractControllerExecution(
    const std::string &name,
    const mbf_abstract_core::AbstractController::Ptr &controller_ptr,
    const ros::Publisher &vel_pub,
    const ros::Publisher &goal_pub,
    const TFPtr &tf_listener_ptr,
    const MoveBaseFlexConfig &config) :
  AbstractExecutionBase(name),
    controller_(controller_ptr), tf_listener_ptr(tf_listener_ptr), state_(INITIALIZED),
    moving_(false), max_retries_(0), patience_(0), vel_pub_(vel_pub), current_goal_pub_(goal_pub),
    loop_rate_(DEFAULT_CONTROLLER_FREQUENCY)
{
  ros::NodeHandle nh;
  ros::NodeHandle private_nh("~");

  // non-dynamically reconfigurable parameters
  private_nh.param("robot_frame", robot_frame_, std::string("base_link"));
  private_nh.param("map_frame", global_frame_, std::string("map"));
  private_nh.param("force_stop_at_goal", force_stop_at_goal_, true);
  private_nh.param("force_stop_on_cancel", force_stop_on_cancel_, true);
  private_nh.param("mbf_tolerance_check", mbf_tolerance_check_, false);
  private_nh.param("dist_tolerance", dist_tolerance_, 0.1);
  private_nh.param("angle_tolerance", angle_tolerance_, M_PI / 18.0);
  private_nh.param("tf_timeout", tf_timeout_, 1.0);
  double safety_check_patience_seconds = 3.0;
  private_nh.param(
      "safety_check_patience", safety_check_patience_seconds, 3.0);
  safety_check_patience_ =
      ros::WallDuration(std::max(0.0, safety_check_patience_seconds));
  double safety_check_recovery_hold_seconds = 0.5;
  private_nh.param(
      "safety_check_recovery_hold", safety_check_recovery_hold_seconds, 0.5);
  safety_check_recovery_hold_ =
      ros::WallDuration(std::max(0.0, safety_check_recovery_hold_seconds));

  // dynamically reconfigurable parameters
  reconfigure(config);
}

AbstractControllerExecution::~AbstractControllerExecution()
{
}

bool AbstractControllerExecution::setControllerFrequency(double frequency)
{
  // set the calling duration by the moving frequency
  if (frequency <= 0.0)
  {
    ROS_ERROR("Controller frequency must be greater than 0.0! No change of the frequency!");
    return false;
  }
  loop_rate_ = ros::Rate(frequency);
  return true;
}

void AbstractControllerExecution::reconfigure(const MoveBaseFlexConfig &config)
{
  boost::lock_guard<boost::mutex> guard(configuration_mutex_);
  // Timeout granted to the controller. We keep calling it up to this time or up to max_retries times
  // If it doesn't return within time, the navigator will cancel it and abort the corresponding action
  patience_ = ros::Duration(config.controller_patience);

  // MBF exposes one global controller frequency, but a navigation server may
  // host controllers with different model time steps. Prefer an explicitly
  // configured plugin-instance frequency so a 20 Hz State controller does not
  // silently force unrelated Standard controllers away from their 10 Hz
  // model_dt. Dynamic reconfigure continues to control instances without an
  // override.
  double controller_frequency = config.controller_frequency;
  ros::NodeHandle private_nh("~");
  const std::string instance_frequency_param =
      name_ + "/controller_frequency";
  double instance_frequency = controller_frequency;
  if (private_nh.getParam(instance_frequency_param, instance_frequency))
  {
    if (std::isfinite(instance_frequency) && instance_frequency > 0.0)
    {
      controller_frequency = instance_frequency;
      ROS_INFO_STREAM("Controller '" << name_ << "' uses instance frequency "
                                      << controller_frequency << " Hz");
    }
    else
    {
      ROS_ERROR_STREAM("Ignoring invalid private parameter ~"
                       << instance_frequency_param << "="
                       << instance_frequency << "; using global "
                       << controller_frequency << " Hz");
    }
  }
  setControllerFrequency(controller_frequency);

  max_retries_ = config.controller_max_retries;
}


bool AbstractControllerExecution::start()
{
  setState(STARTED);
  if (moving_)
  {
    return false; // thread is already running.
  }
  moving_ = true;
  return AbstractExecutionBase::start();
}


void AbstractControllerExecution::setState(ControllerState state)
{
  boost::lock_guard<boost::mutex> guard(state_mtx_);
  state_ = state;
}

typename AbstractControllerExecution::ControllerState AbstractControllerExecution::getState() const
{
  boost::lock_guard<boost::mutex> guard(state_mtx_);
  return state_;
}

void AbstractControllerExecution::setNewPlan(
  const std::vector<geometry_msgs::PoseStamped> &plan,
  bool tolerance_from_action,
  double action_dist_tolerance,
  double action_angle_tolerance,
  const mbf_abstract_core::PlanExecutionContext &context)
{
  if (moving_)
  {
    // This is fine on continuous replanning
    ROS_DEBUG("Setting new plan while moving");
  }
  boost::lock_guard<boost::mutex> guard(plan_mtx_);
  new_plan_ = true;

  plan_ = plan;
  tolerance_from_action_ = tolerance_from_action;
  action_dist_tolerance_ = action_dist_tolerance;
  action_angle_tolerance_ = action_angle_tolerance;
  plan_execution_context_ = context;
}

bool AbstractControllerExecution::trySetNewPlanWhileActive(
    const std::vector<geometry_msgs::PoseStamped> &plan,
    bool tolerance_from_action,
    double action_dist_tolerance,
    double action_angle_tolerance,
    const mbf_abstract_core::PlanExecutionContext &context)
{
  // Keep this lock order (state -> plan) local to the atomic admission path.
  // Terminal state transitions take state_mtx_ before exposing their state, so
  // either this update is admitted and its action handle receives that later
  // terminal result, or it observes the terminal state and is rejected.
  boost::lock_guard<boost::mutex> state_guard(state_mtx_);
  switch (state_)
  {
    case STARTED:
    case PLANNING:
    case NO_LOCAL_CMD:
    case GOT_LOCAL_CMD:
      break;
    default:
      return false;
  }
  if (!moving_.load())
  {
    return false;
  }

  boost::lock_guard<boost::mutex> plan_guard(plan_mtx_);
  new_plan_ = true;
  plan_ = plan;
  tolerance_from_action_ = tolerance_from_action;
  action_dist_tolerance_ = action_dist_tolerance;
  action_angle_tolerance_ = action_angle_tolerance;
  plan_execution_context_ = context;
  return true;
}


bool AbstractControllerExecution::hasNewPlan()
{
  boost::lock_guard<boost::mutex> guard(plan_mtx_);
  return new_plan_;
}


std::vector<geometry_msgs::PoseStamped> AbstractControllerExecution::getNewPlan(
    mbf_abstract_core::PlanExecutionContext &context)
{
  boost::lock_guard<boost::mutex> guard(plan_mtx_);
  new_plan_ = false;
  context = plan_execution_context_;
  return plan_;
}

std::uint64_t AbstractControllerExecution::getPlanExecutionEpoch() const
{
  boost::lock_guard<boost::mutex> guard(plan_mtx_);
  return plan_execution_context_.execution_epoch;
}


bool AbstractControllerExecution::computeRobotPose()
{
  if (!mbf_utility::getRobotPose(*tf_listener_ptr, robot_frame_, global_frame_,
                                 ros::Duration(tf_timeout_), robot_pose_))
  {
    ROS_ERROR_STREAM("Could not get the robot pose in the global frame. - robot frame: \""
                         << robot_frame_ << "\"   global frame: \"" << global_frame_);
    message_ = "Could not get the robot pose";
    outcome_ = mbf_msgs::ExePathResult::TF_ERROR;
    return false;
  }
  return true;
}


uint32_t AbstractControllerExecution::computeVelocityCmd(const geometry_msgs::PoseStamped &robot_pose,
                                                         const geometry_msgs::TwistStamped &robot_velocity,
                                                         geometry_msgs::TwistStamped &vel_cmd,
                                                         std::string &message)
{
  return controller_->computeVelocityCommands(robot_pose, robot_velocity, vel_cmd, message);
}


void AbstractControllerExecution::setVelocityCmd(const geometry_msgs::TwistStamped &vel_cmd)
{
  boost::lock_guard<boost::mutex> guard(vel_cmd_mtx_);
  vel_cmd_stamped_ = vel_cmd;
  if (vel_cmd_stamped_.header.stamp.isZero())
    vel_cmd_stamped_.header.stamp = ros::Time::now();
  // TODO what happen with frame id?
  // TODO Add a queue here for handling the outcome, message and cmd_vel values bundled,
  // TODO so there should be no loss of information in the feedback stream
}

geometry_msgs::TwistStamped AbstractControllerExecution::getVelocityCmd() const
{
  boost::lock_guard<boost::mutex> guard(vel_cmd_mtx_);
  return vel_cmd_stamped_;
}

ros::Time AbstractControllerExecution::getLastPluginCallTime() const
{
  boost::lock_guard<boost::mutex> guard(lct_mtx_);
  return last_call_time_;
}

bool AbstractControllerExecution::isPatienceExceeded() const
{
  boost::lock_guard<boost::mutex> config_guard(configuration_mutex_);
  boost::lock_guard<boost::mutex> guard(lct_mtx_);
  if(!patience_.isZero() && ros::Time::now() - start_time_ > patience_) // not zero -> activated, start_time handles init case
  {
    if(ros::Time::now() - last_call_time_ > patience_)
    {
      ROS_WARN_STREAM_THROTTLE(3, "The controller plugin \"" << name_ << "\" needs more time to compute in one run than the patience time!");
      return true;
    }
    if(ros::Time::now() - last_valid_cmd_time_ > patience_)
    {
      ROS_DEBUG_STREAM("The controller plugin \"" << name_ << "\" does not return a success state (outcome < 10) for more than the patience time in multiple runs!");
      return true;
    }
  }
  return false;
}

bool AbstractControllerExecution::isMoving() const
{
  return moving_.load();
}

AbstractControllerExecution::GoalCheckResult
AbstractControllerExecution::reachedGoalCheck()
{
  double dist_tolerance;
  double angle_tolerance;
  geometry_msgs::PoseStamped goal_pose;
  mbf_abstract_core::PlanExecutionContext context;
  {
    boost::lock_guard<boost::mutex> guard(plan_mtx_);
    if (plan_.empty() || new_plan_)
    {
      // A continuous update won the race after this control cycle's first
      // hasNewPlan() check.  It must be installed in the plugin before the old
      // goal-reached state is allowed to terminate the new action handle.
      return GoalCheckResult::NOT_REACHED;
    }
    dist_tolerance = tolerance_from_action_ ?
        action_dist_tolerance_ : dist_tolerance_;
    angle_tolerance = tolerance_from_action_ ?
        action_angle_tolerance_ : angle_tolerance_;
    goal_pose = plan_.back();
    context = plan_execution_context_;
  }

  const bool endpoint_reached =
      controller_->isGoalReached(dist_tolerance, angle_tolerance) ||
      (mbf_tolerance_check_ && !controller_->usesInternalGoalReachedPolicy() &&
       mbf_utility::distance(robot_pose_, goal_pose) < dist_tolerance &&
       mbf_utility::angle(robot_pose_, goal_pose) < angle_tolerance);
  if (!endpoint_reached)
  {
    return GoalCheckResult::NOT_REACHED;
  }

  if (!context.has_requested_target)
  {
    return GoalCheckResult::REACHED;
  }

  geometry_msgs::PoseStamped requested_target = context.requested_target;
  if (requested_target.header.frame_id.empty())
  {
    requested_target.header.frame_id = global_frame_;
  }
  else if (requested_target.header.frame_id != global_frame_)
  {
    geometry_msgs::PoseStamped transformed_target;
    if (!mbf_utility::transformPose(
            *tf_listener_ptr, global_frame_, ros::Duration(tf_timeout_),
            requested_target, transformed_target))
    {
      outcome_ = mbf_msgs::ExePathResult::TF_ERROR;
      message_ = "Could not transform the original requested goal into the controller frame";
      return GoalCheckResult::REQUESTED_GOAL_MISSED;
    }
    requested_target = transformed_target;
  }

  const double requested_distance =
      mbf_utility::distance(robot_pose_, requested_target);
  const double requested_angle =
      mbf_utility::angle(robot_pose_, requested_target);
  if (requested_distance > dist_tolerance ||
      requested_angle > angle_tolerance)
  {
    std::ostringstream message;
    message << "Path endpoint reached, but original requested goal residual is "
            << requested_distance << " m / " << requested_angle
            << " rad (limits " << dist_tolerance << " m / "
            << angle_tolerance << " rad)";
    outcome_ = mbf_msgs::ExePathResult::MISSED_GOAL;
    message_ = message.str();
    return GoalCheckResult::REQUESTED_GOAL_MISSED;
  }

  return GoalCheckResult::REACHED;
}

bool AbstractControllerExecution::cancel()
{
  // Request the controller to cancel; it will return true if it takes care of stopping, returning CANCELED on
  // computeVelocityCmd when done. This allows for smooth, controlled stops.
  // If false (meaning cancel is not implemented, or that the controller defers handling it) MBF will take care.
  const bool plugin_handles_cancel = controller_->cancel();
  // Cancellation must not depend on another controller invocation. In
  // particular, safetyCheck() deliberately prevents calling the plugin while
  // the costmap is stale, so waiting for computeVelocityCommands() to return
  // CANCELED could otherwise leave the action alive indefinitely.
  cancel_ = true;
  if (plugin_handles_cancel)
  {
    ROS_INFO("Controller cancel acknowledged; MBF will stop this execution at the next cycle boundary");
  }
  else
  {
    ROS_WARN("Controller defers handling cancel; force it and wait until the current control cycle finished");
    // wait for the control cycle to stop
    if (waitForStateUpdate(boost::chrono::milliseconds(500)) == boost::cv_status::timeout)
    {
      // this situation should never happen; if it does, the action server will be unready for goals immediately sent
      ROS_WARN_STREAM("Timeout while waiting for control cycle to stop; immediately sent goals can get stuck");
      return false;
    }
  }
  return true;
}


void AbstractControllerExecution::run()
{
  {
    boost::lock_guard<boost::mutex> guard(lct_mtx_);
    start_time_ = ros::Time::now();
    last_valid_cmd_time_ = ros::Time();
  }
  safety_check_failure_start_ = ros::WallTime();
  safety_check_recovery_start_ = ros::WallTime();
  safety_check_fault_active_ = false;

  // init plan
  std::vector<geometry_msgs::PoseStamped> plan;
  if (!hasNewPlan())
  {
    setState(NO_PLAN);
    moving_ = false;
    ROS_ERROR("robot navigation moving has no plan!");
  }

  int retries = 0;
  int seq = 0;

  try
  {
    while (moving_ && ros::ok())
    {
      if (cancel_.load())
      {
        if (force_stop_on_cancel_)
        {
          publishZeroVelocity(); // command the robot to stop on canceling navigation
        }
        setState(CANCELED);
        moving_ = false;
        condition_.notify_all();
        return;
      }

      const bool safety_ok = safetyCheck();

      // A cancel request can arrive while safetyCheck() runs. It must win
      // over the stale-safety retry/MAP_ERROR path and be handled at the next
      // loop boundary above.
      if (cancel_.load())
      {
        continue;
      }

      if (!safety_ok)
      {
        // Fail closed for the complete cycle: no controller command may be
        // computed or published while the underlying safety input is stale.
        const ros::WallTime now = ros::WallTime::now();
        if (!safety_check_fault_active_)
        {
          safety_check_fault_active_ = true;
          safety_check_failure_start_ = now;
        }
        safety_check_recovery_start_ = ros::WallTime();

        outcome_ = mbf_msgs::ExePathResult::MAP_ERROR;
        message_ = "Controller safety check failed: costmap or sensor data is not current";
        geometry_msgs::TwistStamped zero_cmd;
        zero_cmd.header.stamp = ros::Time::now();
        zero_cmd.header.seq = seq++;
        setVelocityCmd(zero_cmd);
        publishZeroVelocity();

        if (safety_check_patience_.isZero() ||
            now - safety_check_failure_start_ >= safety_check_patience_)
        {
          ROS_ERROR_STREAM(
              message_ << " for "
              << (now - safety_check_failure_start_).toSec()
              << " s; aborting this execution with MAP_ERROR");
          setState(MAP_ERROR);
          moving_ = false;
        }
        else
        {
          setState(NO_LOCAL_CMD);
        }
        condition_.notify_all();

        if (moving_)
        {
          boost::this_thread::interruption_point();
          loop_rate_.sleep();
          boost::this_thread::interruption_point();
        }
        continue;
      }

      if (safety_check_fault_active_)
      {
        // A single fresh cycle is not sufficient to release a safety stop.
        // Keep the original episode deadline and require a continuously fresh
        // hold, otherwise a flapping isCurrent() signal can alternate zero and
        // non-zero commands forever without ever reaching MAP_ERROR.
        const ros::WallTime now = ros::WallTime::now();
        if (safety_check_recovery_start_.isZero())
        {
          safety_check_recovery_start_ = now;
        }

        if (!safety_check_recovery_hold_.isZero() &&
            now - safety_check_recovery_start_ < safety_check_recovery_hold_)
        {
          outcome_ = mbf_msgs::ExePathResult::MAP_ERROR;
          message_ =
              "Controller safety input is current again; waiting for stable recovery";
          geometry_msgs::TwistStamped zero_cmd;
          zero_cmd.header.stamp = ros::Time::now();
          zero_cmd.header.seq = seq++;
          setVelocityCmd(zero_cmd);
          publishZeroVelocity();

          if (safety_check_patience_.isZero() ||
              now - safety_check_failure_start_ >= safety_check_patience_)
          {
            ROS_ERROR_STREAM(
                "Controller safety input did not recover stably within "
                << (now - safety_check_failure_start_).toSec()
                << " s; aborting this execution with MAP_ERROR");
            setState(MAP_ERROR);
            moving_ = false;
          }
          else
          {
            setState(NO_LOCAL_CMD);
          }
          condition_.notify_all();

          if (moving_)
          {
            boost::this_thread::interruption_point();
            loop_rate_.sleep();
            boost::this_thread::interruption_point();
          }
          continue;
        }

        safety_check_fault_active_ = false;
        safety_check_failure_start_ = ros::WallTime();
        safety_check_recovery_start_ = ros::WallTime();
        ROS_INFO("Controller safety input recovered and remained stable; resuming motion");
      }

      // update plan dynamically
      if (hasNewPlan())
      {
        mbf_abstract_core::PlanExecutionContext plan_context;
        plan = getNewPlan(plan_context);

        // check if plan is empty
        if (plan.empty())
        {
          setState(EMPTY_PLAN);
          moving_ = false;
          condition_.notify_all();
          return;
        }

        // check if plan could be set
        controller_->setPlanExecutionContext(plan_context);
        if (!controller_->setPlan(plan))
        {
          setState(INVALID_PLAN);
          moving_ = false;
          condition_.notify_all();
          return;
        }
        current_goal_pub_.publish(plan.back());
      }

      // compute robot pose and store it in robot_pose_
      if (!computeRobotPose())
      {
        publishZeroVelocity();
        setState(INTERNAL_ERROR);
        moving_ = false;
        condition_.notify_all();
        return;
      }

      // ask planner if the goal is reached
      const GoalCheckResult goal_check = reachedGoalCheck();
      if (goal_check == GoalCheckResult::REQUESTED_GOAL_MISSED)
      {
        // Never silently turn a tolerance endpoint into the requested block
        // entry.  Stop and propagate MISSED_GOAL/TF_ERROR so MoveBase can
        // replan from the actual pose with an explicit reason.
        publishZeroVelocity();
        setState(MAX_RETRIES);
        moving_ = false;
        condition_.notify_all();
      }
      else if (goal_check == GoalCheckResult::REACHED)
      {
        ROS_DEBUG_STREAM_NAMED("abstract_controller_execution", "Reached the goal!");
        if (force_stop_at_goal_)
        {
          publishZeroVelocity();
        }
        setState(ARRIVED_GOAL);
        // goal reached, tell it the controller
        moving_ = false;
        condition_.notify_all();
        // if not, keep moving
      }
      else
      {
        setState(PLANNING);

        // save time and call the plugin
        lct_mtx_.lock();
        last_call_time_ = ros::Time::now();
        lct_mtx_.unlock();

        // call plugin to compute the next velocity command
        geometry_msgs::TwistStamped cmd_vel_stamped;
        geometry_msgs::TwistStamped robot_velocity;   // TODO pass current velocity to the plugin!
        outcome_ = computeVelocityCmd(robot_pose_, robot_velocity, cmd_vel_stamped, message_ = "");

        // Cancellation can arrive while the plugin is evaluating trajectories.
        // It must be observed before publishing that just-computed command;
        // otherwise one non-zero command may be emitted after the outer goal
        // has already been reported canceled.
        if (cancel_.load())
        {
          geometry_msgs::TwistStamped zero_cmd;
          zero_cmd.header.stamp = ros::Time::now();
          zero_cmd.header.seq = seq++;
          setVelocityCmd(zero_cmd);
          publishZeroVelocity();
          setState(CANCELED);
          moving_ = false;
          condition_.notify_all();
          return;
        }

        if (outcome_ < 10)
        {
          setState(GOT_LOCAL_CMD);
          vel_pub_.publish(cmd_vel_stamped.twist);
          {
            boost::lock_guard<boost::mutex> guard(lct_mtx_);
            last_valid_cmd_time_ = ros::Time::now();
          }
          retries = 0;
        }
        else if (outcome_ == mbf_msgs::ExePathResult::CANCELED)
        {
          ROS_INFO_STREAM("Controller-handled cancel completed");
          cancel_ = true;
          continue;
        }
        else if (outcome_ == mbf_msgs::ExePathResult::LATCHED_SAFETY_FAILURE)
        {
          // The plugin has already brought the chassis to a verified stop and
          // explicitly states that this execution cannot recover. Waiting out
          // controller_patience would only replay the same failure latch.
          ROS_WARN_STREAM(
              "Controller reported a latched safety failure; requesting a "
              "fresh MBF recovery/replan execution");
          setState(MAX_RETRIES);
          moving_ = false;
          publishZeroVelocity();
        }
        else
        {
          int max_retries;
          bool patience_enabled;
          {
            boost::lock_guard<boost::mutex> guard(configuration_mutex_);
            max_retries = max_retries_;
            patience_enabled = !patience_.isZero();
          }
          if (max_retries > 0 && ++retries > max_retries)
          {
            setState(MAX_RETRIES);
            moving_ = false;
          }
          else if (isPatienceExceeded())
          {
            // patience limit enabled and running controller for more than patience without valid commands
            setState(PAT_EXCEEDED);
            moving_ = false;
          }
          else
          {
            setState(NO_LOCAL_CMD); // useful for server feedback
            // we keep on moving if we have retries left or if the user has granted us some patience.
            moving_ = max_retries || patience_enabled;
          }
          // could not compute a valid velocity command -> stop moving the robot
          publishZeroVelocity(); // command the robot to stop; we still feedback command calculated by the plugin
        }

        // set stamped values; timestamp and frame_id should be set by the plugin; otherwise setVelocityCmd will do
        cmd_vel_stamped.header.seq = seq++; // sequence number
        setVelocityCmd(cmd_vel_stamped);
        condition_.notify_all();
      }

      if (moving_)
      {
        // The nanosleep used by ROS time is not interruptable, therefore providing an interrupt point before and after
        boost::this_thread::interruption_point();
        if (!loop_rate_.sleep())
        {
          ROS_WARN_THROTTLE(1.0, "Calculation needs too much time to stay in the moving frequency! (%.4fs > %.4fs)",
                            loop_rate_.cycleTime().toSec(), loop_rate_.expectedCycleTime().toSec());
        }
        boost::this_thread::interruption_point();
      }
    }
  }
  catch (const boost::thread_interrupted &ex)
  {
    // Controller thread interrupted; in most cases we have started a new plan
    // Can also be that robot is oscillating or we have exceeded planner patience
    ROS_DEBUG_STREAM("Controller thread interrupted!");
    publishZeroVelocity();
    setState(STOPPED);
    condition_.notify_all();
    moving_ = false;
  }
  catch (...)
  {
    message_ = "Unknown error occurred: " + boost::current_exception_diagnostic_information();
    ROS_FATAL_STREAM(message_);
    publishZeroVelocity();
    setState(INTERNAL_ERROR);
    moving_ = false;
    condition_.notify_all();
  }
}


void AbstractControllerExecution::publishZeroVelocity()
{
  geometry_msgs::Twist cmd_vel;
  cmd_vel.linear.x = 0;
  cmd_vel.linear.y = 0;
  cmd_vel.linear.z = 0;
  cmd_vel.angular.x = 0;
  cmd_vel.angular.y = 0;
  cmd_vel.angular.z = 0;
  vel_pub_.publish(cmd_vel);
}

} /* namespace mbf_abstract_nav */

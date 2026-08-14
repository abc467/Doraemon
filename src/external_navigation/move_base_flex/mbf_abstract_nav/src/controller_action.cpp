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
 *  controller_action.cpp
 *
 *  authors:
 *    Sebastian Pütz <spuetz@uni-osnabrueck.de>
 *    Jorge Santos Simón <santos@magazino.eu>
 *
 */

#include "mbf_abstract_nav/controller_action.h"

namespace mbf_abstract_nav
{

namespace
{

mbf_abstract_core::PlanExecutionContext planContext(
    const mbf_msgs::ExePathGoal &goal)
{
  mbf_abstract_core::PlanExecutionContext context;
  context.execution_epoch = goal.execution_epoch;
  context.update_mode =
      goal.plan_update_mode == mbf_msgs::ExePathGoal::CONTINUOUS_UPDATE ?
      mbf_abstract_core::PlanExecutionContext::UpdateMode::ContinuousUpdate :
      mbf_abstract_core::PlanExecutionContext::UpdateMode::FreshExecution;
  context.has_requested_target = goal.has_requested_target;
  context.requested_target = goal.requested_target;
  return context;
}

}  // namespace

ControllerAction::ControllerAction(
    const std::string &action_name,
    const mbf_utility::RobotInformation &robot_info)
    : AbstractActionBase(action_name, robot_info)
{
}

ControllerAction::~ControllerAction()
{
  // Join executions before goal_mtx_/goal_pose_ are destroyed. The base
  // destructor performs pointer cleanup after this derived state is safe.
  cancelAll();
}

bool ControllerAction::tryUpdateContinuousPlan(
    const mbf_msgs::ExePathGoal &goal,
    std::string &reason)
{
  reason.clear();
  if (goal.plan_update_mode != mbf_msgs::ExePathGoal::CONTINUOUS_UPDATE)
  {
    reason = "plan is not marked as a continuous update";
    return false;
  }
  if (goal.path.poses.empty())
  {
    reason = "continuous plan is empty";
    return false;
  }

  const uint8_t slot = goal.concurrency_slot;
  boost::lock_guard<boost::mutex> slot_guard(slot_map_mtx_);
  typename ConcurrencyMap::iterator slot_it = concurrency_slots_.find(slot);
  if (slot_it == concurrency_slots_.end() || !slot_it->second.in_use ||
      !slot_it->second.execution)
  {
    reason = "controller execution is no longer active";
    return false;
  }

  boost::lock_guard<boost::mutex> goal_guard(goal_mtx_);
  const bool same_controller =
      slot_it->second.execution->getName() == goal.controller ||
      goal.controller.empty();
  if (!same_controller)
  {
    reason = "continuous plan targets a different controller";
    return false;
  }
  if (slot_it->second.execution->getPlanExecutionEpoch() !=
      goal.execution_epoch)
  {
    reason = "continuous plan belongs to a stale execution epoch";
    return false;
  }

  if (!slot_it->second.execution->trySetNewPlanWhileActive(
          goal.path.poses,
          goal.tolerance_from_action,
          goal.dist_tolerance,
          goal.angle_tolerance,
          planContext(goal)))
  {
    // The state admission and plan handoff are atomic inside the execution.
    // Keep the existing action handle untouched: it will publish the real
    // success/failure which raced this refresh.
    reason = "controller reached a terminal state during refresh admission";
    return false;
  }

  goal_pose_ = goal.path.poses.back();
  reason = "continuous plan installed without replacing the action goal";
  return true;
}

void ControllerAction::start(
    GoalHandle &goal_handle,
    typename AbstractControllerExecution::Ptr execution_ptr
)
{
  if(goal_handle.getGoalStatus().status == actionlib_msgs::GoalStatus::RECALLING)
  {
    goal_handle.setCanceled();
    return;
  }

  uint8_t slot = goal_handle.getGoal()->concurrency_slot;

  bool update_plan = false;
  bool reject_stale_continuous_update = false;
  bool reject_empty_continuous_update = false;
  slot_map_mtx_.lock();
  std::map<uint8_t, ConcurrencySlot>::iterator slot_it = concurrency_slots_.find(slot);
  if(slot_it != concurrency_slots_.end() && slot_it->second.in_use)
  {
    boost::lock_guard<boost::mutex> goal_guard(goal_mtx_);
    const auto &incoming_goal = *goal_handle.getGoal();
    const bool same_controller =
        slot_it->second.execution->getName() == incoming_goal.controller ||
        incoming_goal.controller.empty();
    const bool continuous_update =
        incoming_goal.plan_update_mode ==
        mbf_msgs::ExePathGoal::CONTINUOUS_UPDATE;
    const bool same_epoch =
        slot_it->second.execution->getPlanExecutionEpoch() ==
        incoming_goal.execution_epoch;
    if (continuous_update && incoming_goal.path.poses.empty())
    {
      reject_empty_continuous_update = true;
    }
    else if(same_controller && continuous_update && same_epoch)
    {
      // Goal requests to run the same controller on the same concurrency slot already in use:
      // we update the goal handle and pass the new plan and tolerances from the action to the
      // execution without stopping it
      execution_ptr = slot_it->second.execution;
      update_plan = execution_ptr->trySetNewPlanWhileActive(
          incoming_goal.path.poses,
          incoming_goal.tolerance_from_action,
          incoming_goal.dist_tolerance,
          incoming_goal.angle_tolerance,
          planContext(incoming_goal));
      if (update_plan)
      {
        // Update also goal pose, so the feedback remains consistent
        goal_pose_ = incoming_goal.path.poses.back();
        mbf_msgs::ExePathResult result;
        fillExePathResult(mbf_msgs::ExePathResult::CANCELED, "Goal preempted by a new plan", result);
        concurrency_slots_[slot].goal_handle.setCanceled(result, result.message);
        concurrency_slots_[slot].goal_handle = goal_handle;
        concurrency_slots_[slot].goal_handle.setAccepted();
      }
      else
      {
        // in_use remains true until the action thread exits.  Do not attach a
        // new action goal to an execution which has already entered a terminal
        // state during that window.
        reject_stale_continuous_update = true;
      }
    }
    else if (continuous_update)
    {
      // Never let a delayed periodic callback preempt a newer controller
      // execution. Fresh plans intentionally fall through to the parent,
      // which cancels and joins the old execution before starting the new one.
      reject_stale_continuous_update = true;
    }
  }
  slot_map_mtx_.unlock();
  if(reject_empty_continuous_update)
  {
    mbf_msgs::ExePathResult result;
    fillExePathResult(
        mbf_msgs::ExePathResult::INVALID_PATH,
        "Empty continuous plan update rejected", result);
    goal_handle.setAborted(result, result.message);
    ROS_ERROR_STREAM_NAMED(name_, result.message);
    return;
  }
  if(reject_stale_continuous_update)
  {
    mbf_msgs::ExePathResult result;
    fillExePathResult(
        mbf_msgs::ExePathResult::CANCELED,
        "Stale continuous plan update ignored", result);
    goal_handle.setCanceled(result, result.message);
    ROS_WARN_STREAM_NAMED(
        name_, "Ignored stale continuous plan update for epoch "
        << goal_handle.getGoal()->execution_epoch);
    return;
  }
  if(!update_plan)
  {
    // Otherwise run parent version of this method
    AbstractActionBase::start(goal_handle, execution_ptr);
  }
}

void ControllerAction::runImpl(GoalHandle &goal_handle, AbstractControllerExecution &execution)
{
  goal_mtx_.lock();
  // Note that we always use the goal handle stored on the concurrency slots map, as it can change when replanning
  uint8_t slot = goal_handle.getGoal()->concurrency_slot;
  goal_mtx_.unlock();

  ROS_DEBUG_STREAM_NAMED(name_, "Start action "  << name_);

  // ensure we don't provide values from previous execution on case of error before filling both poses
  goal_pose_ = geometry_msgs::PoseStamped();
  robot_pose_ = geometry_msgs::PoseStamped();

  ros::NodeHandle private_nh("~");

  double oscillation_timeout_tmp;
  private_nh.param("oscillation_timeout", oscillation_timeout_tmp, 0.0);
  ros::Duration oscillation_timeout(oscillation_timeout_tmp);

  double oscillation_distance;
  private_nh.param("oscillation_distance", oscillation_distance, 0.03);

  mbf_msgs::ExePathResult result;
  mbf_msgs::ExePathFeedback feedback;

  typename AbstractControllerExecution::ControllerState state_moving_input;
  bool controller_active = true;

  goal_mtx_.lock();
  const mbf_msgs::ExePathGoal &goal = *(goal_handle.getGoal().get());

  const std::vector<geometry_msgs::PoseStamped> &plan = goal.path.poses;
  if (plan.empty())
  {
    fillExePathResult(mbf_msgs::ExePathResult::INVALID_PATH, "Controller started with an empty plan!", result);
    goal_handle.setAborted(result, result.message);
    ROS_ERROR_STREAM_NAMED(name_, result.message << " Canceling the action call.");
    controller_active = false;
    goal_mtx_.unlock();
    return;
  }

  goal_pose_ = plan.back();
  ROS_DEBUG_STREAM_NAMED(name_, "Called action \""
      << name_ << "\" with plan:" << std::endl
      << "frame: \"" << goal.path.header.frame_id << "\" " << std::endl
      << "stamp: " << goal.path.header.stamp << std::endl
      << "poses: " << goal.path.poses.size() << std::endl
      << "goal: (" << goal_pose_.pose.position.x << ", "
      << goal_pose_.pose.position.y << ", "
      << goal_pose_.pose.position.z << ")");

  goal_mtx_.unlock();


  geometry_msgs::PoseStamped oscillation_pose;
  ros::Time last_oscillation_reset = ros::Time::now();

  bool first_cycle = true;

  while (controller_active && ros::ok())
  {
    // goal_handle could change between the loop cycles due to adapting the plan
    // with a new goal received for the same concurrency slot
    if (!robot_info_.getRobotPose(robot_pose_))
    {
      controller_active = false;
      fillExePathResult(mbf_msgs::ExePathResult::TF_ERROR, "Could not get the robot pose!", result);
      goal_mtx_.lock();
      goal_handle.setAborted(result, result.message);
      goal_mtx_.unlock();
      ROS_ERROR_STREAM_NAMED(name_, result.message << " Canceling the action call.");
      break;
    }

    if (first_cycle)
    {
      // init oscillation pose
      oscillation_pose = robot_pose_;
    }

    goal_mtx_.lock();
    state_moving_input = execution.getState();

    switch (state_moving_input)
    {
      case AbstractControllerExecution::INITIALIZED:
        execution.setNewPlan(
            plan, goal.tolerance_from_action, goal.dist_tolerance,
            goal.angle_tolerance, planContext(goal));
        execution.start();
        break;

      case AbstractControllerExecution::STOPPED:
        ROS_WARN_STREAM_NAMED(name_, "The controller has been stopped rigorously!");
        controller_active = false;
        result.outcome = mbf_msgs::ExePathResult::STOPPED;
        result.message = "Controller has been stopped!";
        goal_handle.setAborted(result, result.message);
        break;

      case AbstractControllerExecution::CANCELED:
        ROS_INFO_STREAM("Action \"exe_path\" canceled");
        fillExePathResult(mbf_msgs::ExePathResult::CANCELED, "Controller canceled", result);
        goal_handle.setCanceled(result, result.message);
        controller_active = false;
        break;

      case AbstractControllerExecution::STARTED:
        ROS_DEBUG_STREAM_NAMED(name_, "The moving has been started!");
        break;

      case AbstractControllerExecution::PLANNING:
        if (execution.isPatienceExceeded())
        {
          ROS_INFO_STREAM("Try to cancel the plugin \"" << name_ << "\" after the patience time has been exceeded!");
          execution.cancel();
          controller_active = false;
          fillExePathResult(
              mbf_msgs::ExePathResult::PAT_EXCEEDED,
              "Controller patience exceeded", result);
          // A self-timeout is a controller failure, not a client cancel.  An
          // ABORTED child result lets MoveBase enter recovery instead of
          // waiting forever for a result it deliberately ignores as preempted.
          goal_handle.setAborted(result, result.message);
        }
        break;

      case AbstractControllerExecution::MAX_RETRIES:
        ROS_WARN_STREAM_NAMED(name_, "The controller has been aborted after it exceeded the maximum number of retries!");
        controller_active = false;
        fillExePathResult(execution.getOutcome(), execution.getMessage(), result);
        goal_handle.setAborted(result, result.message);
        break;

      case AbstractControllerExecution::PAT_EXCEEDED:
        ROS_WARN_STREAM_NAMED(name_, "The controller has been aborted after it exceeded the patience time");
        controller_active = false;
        fillExePathResult(mbf_msgs::ExePathResult::PAT_EXCEEDED, execution.getMessage(), result);
        goal_handle.setAborted(result, result.message);
        break;

      case AbstractControllerExecution::NO_PLAN:
        ROS_WARN_STREAM_NAMED(name_, "The controller has been started without a plan!");
        controller_active = false;
        fillExePathResult(mbf_msgs::ExePathResult::INVALID_PATH, "Controller started without a path", result);
        goal_handle.setAborted(result, result.message);
        break;

      case AbstractControllerExecution::EMPTY_PLAN:
        ROS_WARN_STREAM_NAMED(name_, "The controller has received an empty plan");
        controller_active = false;
        fillExePathResult(mbf_msgs::ExePathResult::INVALID_PATH, "Controller started with an empty plan", result);
        goal_handle.setAborted(result, result.message);
        break;

      case AbstractControllerExecution::INVALID_PLAN:
        ROS_WARN_STREAM_NAMED(name_, "The controller has received an invalid plan");
        controller_active = false;
        fillExePathResult(mbf_msgs::ExePathResult::INVALID_PATH, "Controller started with an invalid plan", result);
        goal_handle.setAborted(result, result.message);
        break;

      case AbstractControllerExecution::NO_LOCAL_CMD:
        ROS_WARN_STREAM_THROTTLE_NAMED(3, name_, "No velocity command received from controller! "
            << execution.getMessage());
        controller_active = execution.isMoving();
        if (!controller_active)
        {
          fillExePathResult(execution.getOutcome(), execution.getMessage(), result);
          goal_handle.setAborted(result, result.message);
        }
        else
        {
          publishExePathFeedback(goal_handle, execution.getOutcome(), execution.getMessage(),
                                 execution.getVelocityCmd());
        }
        break;

      case AbstractControllerExecution::GOT_LOCAL_CMD:
        if (!oscillation_timeout.isZero())
        {
          // check if oscillating
          if (mbf_utility::distance(robot_pose_, oscillation_pose) >= oscillation_distance)
          {
            last_oscillation_reset = ros::Time::now();
            oscillation_pose = robot_pose_;
          }
          else if (last_oscillation_reset + oscillation_timeout < ros::Time::now())
          {
            ROS_WARN_STREAM_NAMED(name_, "The controller is oscillating for "
                << (ros::Time::now() - last_oscillation_reset).toSec() << "s");

            execution.cancel();
            controller_active = false;
            fillExePathResult(mbf_msgs::ExePathResult::OSCILLATION, "Oscillation detected!", result);
            goal_handle.setAborted(result, result.message);
            break;
          }
        }
        publishExePathFeedback(goal_handle, execution.getOutcome(), execution.getMessage(), execution.getVelocityCmd());
        break;

      case AbstractControllerExecution::ARRIVED_GOAL:
        ROS_DEBUG_STREAM_NAMED(name_, "Controller succeeded; arrived at goal");
        controller_active = false;
        fillExePathResult(mbf_msgs::ExePathResult::SUCCESS, "Controller succeeded; arrived at goal!", result);
        goal_handle.setSucceeded(result, result.message);
        break;

      case AbstractControllerExecution::MAP_ERROR:
        ROS_ERROR_STREAM_NAMED(
            name_, "Controller safety input remained unavailable: "
            << execution.getMessage());
        controller_active = false;
        fillExePathResult(
            mbf_msgs::ExePathResult::MAP_ERROR,
            execution.getMessage(), result);
        goal_handle.setAborted(result, result.message);
        break;

      case AbstractControllerExecution::INTERNAL_ERROR:
        ROS_ERROR_STREAM_NAMED(name_, "Controller execution failed: " << execution.getMessage());
        controller_active = false;
        fillExePathResult(
            execution.getOutcome() >= mbf_msgs::ExePathResult::FAILURE ?
                execution.getOutcome() : mbf_msgs::ExePathResult::INTERNAL_ERROR,
            execution.getMessage().empty() ?
                "Internal controller execution error" : execution.getMessage(),
            result);
        goal_handle.setAborted(result, result.message);
        break;

      default:
        std::stringstream ss;
        ss << "Internal error: Unknown state in a move base flex controller execution with the number: "
           << static_cast<int>(state_moving_input);
        fillExePathResult(mbf_msgs::ExePathResult::INTERNAL_ERROR, ss.str(), result);
        ROS_FATAL_STREAM_NAMED(name_, result.message);
        goal_handle.setAborted(result, result.message);
        controller_active = false;
    }
    goal_mtx_.unlock();

    if (controller_active)
    {
      // try to sleep a bit
      // normally this thread should be woken up from the controller execution thread
      // in order to transfer the results to the controller
      execution.waitForStateUpdate(boost::chrono::milliseconds(500));
    }

    first_cycle = false;
  }  // while (controller_active && ros::ok())

  if (!controller_active)
  {
    ROS_DEBUG_STREAM_NAMED(name_, "\"" << name_ << "\" action ended properly.");
  }
  else
  {
    // normal on continuous replanning
    ROS_DEBUG_STREAM_NAMED(name_, "\"" << name_ << "\" action has been stopped!");
  }
}

void ControllerAction::publishExePathFeedback(
        GoalHandle &goal_handle,
        uint32_t outcome, const std::string &message,
        const geometry_msgs::TwistStamped &current_twist)
{
  mbf_msgs::ExePathFeedback feedback;
  feedback.outcome = outcome;
  feedback.message = message;

  feedback.last_cmd_vel = current_twist;
  if (feedback.last_cmd_vel.header.stamp.isZero())
    feedback.last_cmd_vel.header.stamp = ros::Time::now();

  feedback.current_pose = robot_pose_;
  feedback.dist_to_goal = static_cast<float>(mbf_utility::distance(robot_pose_, goal_pose_));
  feedback.angle_to_goal = static_cast<float>(mbf_utility::angle(robot_pose_, goal_pose_));
  goal_handle.publishFeedback(feedback);
}

void ControllerAction::fillExePathResult(
        uint32_t outcome, const std::string &message,
        mbf_msgs::ExePathResult &result)
{
  result.outcome = outcome;
  result.message = message;
  result.final_pose = robot_pose_;
  result.dist_to_goal = static_cast<float>(mbf_utility::distance(robot_pose_, goal_pose_));
  result.angle_to_goal = static_cast<float>(mbf_utility::angle(robot_pose_, goal_pose_));
}

} /* mbf_abstract_nav */

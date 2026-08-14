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
 *  move_base_action.h
 *
 *  authors:
 *    Sebastian Pütz <spuetz@uni-osnabrueck.de>
 *    Jorge Santos Simón <santos@magazino.eu>
 *
 */
#ifndef MBF_ABSTRACT_NAV__MOVE_BASE_ACTION_H_
#define MBF_ABSTRACT_NAV__MOVE_BASE_ACTION_H_

#include <atomic>
#include <cstdint>

#include <boost/function.hpp>

#include <actionlib/server/action_server.h>
#include <actionlib/client/simple_action_client.h>

#include <mbf_msgs/MoveBaseAction.h>
#include <mbf_msgs/GetPathAction.h>
#include <mbf_msgs/ExePathAction.h>
#include <mbf_msgs/RecoveryAction.h>

#include <mbf_utility/robot_information.h>

#include "mbf_abstract_nav/MoveBaseFlexConfig.h"


namespace mbf_abstract_nav
{

class MoveBaseAction
{
 public:

  //! Action clients for the MoveBase action
  typedef actionlib::SimpleActionClient<mbf_msgs::GetPathAction> ActionClientGetPath;
  typedef actionlib::SimpleActionClient<mbf_msgs::ExePathAction> ActionClientExePath;
  typedef actionlib::SimpleActionClient<mbf_msgs::RecoveryAction> ActionClientRecovery;

  typedef boost::function<bool(
      const mbf_msgs::ExePathGoal &, std::string &)> ContinuousPlanUpdater;

  typedef actionlib::ActionServer<mbf_msgs::MoveBaseAction>::GoalHandle GoalHandle;

  MoveBaseAction(const std::string &name,
                 const mbf_utility::RobotInformation &robot_info,
                 const std::vector<std::string> &controllers,
                 const ContinuousPlanUpdater &continuous_plan_updater);

  ~MoveBaseAction();

  void start(GoalHandle &goal_handle);

  void cancel();

  /**
   * Cancel a specific outer MoveBase goal and complete it immediately.
   *
   * The asynchronous get_path callback is intentionally invalidated on
   * cancellation, so it cannot be responsible for completing this goal.
   */
  void cancel(GoalHandle &goal_handle);

  void reconfigure(
      mbf_abstract_nav::MoveBaseFlexConfig &config, uint32_t level);

 protected:

  void actionExePathFeedback(
      const mbf_msgs::ExePathFeedbackConstPtr &feedback,
      std::uint64_t execution_epoch,
      std::uint64_t controller_generation);

  void actionGetPathDone(
      const actionlib::SimpleClientGoalState &state,
      const mbf_msgs::GetPathResultConstPtr &result,
      std::uint64_t request_generation,
      std::uint64_t execution_epoch);

  void actionExePathActive(
      std::uint64_t execution_epoch,
      std::uint64_t controller_generation);

  void actionExePathDone(
      const actionlib::SimpleClientGoalState &state,
      const mbf_msgs::ExePathResultConstPtr &result,
      std::uint64_t execution_epoch,
      std::uint64_t controller_generation);

  void actionRecoveryDone(
      const actionlib::SimpleClientGoalState &state,
      const mbf_msgs::RecoveryResultConstPtr &result,
      std::uint64_t execution_epoch);

  bool attemptRecovery();

  bool replanningActive() const;

  /**
   * Reject an old periodic result which the current robot cannot join and
   * return the executable suffix beginning at the admitted projection.
   */
  bool periodicPlanIsFreshAndReachable(
      const nav_msgs::Path &path,
      const ros::WallTime &request_started_at,
      nav_msgs::Path &joined_path,
      std::string &reason) const;

  void replanningThread();

  //! Cancel all child actions after invalidating their callbacks.
  void cancelChildActions();

  /**
   * Utility method that fills move base action result with the result of any of the action clients.
   * @tparam ResultType
   * @param result
   * @param move_base_result
   */
  template <typename ResultType>
  void fillMoveBaseResult(const ResultType& result, mbf_msgs::MoveBaseResult& move_base_result)
  {
    // copy outcome and message from action client result
    move_base_result.outcome = result.outcome;
    move_base_result.message = result.message;
    move_base_result.dist_to_goal = static_cast<float>(mbf_utility::distance(robot_pose_, goal_pose_));
    move_base_result.angle_to_goal = static_cast<float>(mbf_utility::angle(robot_pose_, goal_pose_));
    move_base_result.final_pose = robot_pose_;
  }

  mbf_msgs::ExePathGoal exe_path_goal_;
  mbf_msgs::GetPathGoal get_path_goal_;
  mbf_msgs::RecoveryGoal recovery_goal_;

  geometry_msgs::PoseStamped last_oscillation_pose_;
  ros::Time last_oscillation_reset_;

  //! timeout after a oscillation is detected
  ros::Duration oscillation_timeout_;

  //! minimal move distance to not detect an oscillation
  double oscillation_distance_;

  GoalHandle goal_handle_;

  std::string name_;

  //! current robot state
  const mbf_utility::RobotInformation &robot_info_;

  //! current robot pose; updated with exe_path action feedback
  geometry_msgs::PoseStamped robot_pose_;

  //! current goal pose; used to compute remaining distance and angle
  geometry_msgs::PoseStamped goal_pose_;

  ros::NodeHandle private_nh_;

  //! Action client used by the move_base action
  ActionClientExePath action_client_exe_path_;

  //! Action client used by the move_base action
  ActionClientGetPath action_client_get_path_;

  //! Dedicated client for periodic replanning; never shares result ownership
  //! with initial/post-recovery planning.
  ActionClientGetPath action_client_replanning_;

  //! Action client used by the move_base action
  ActionClientRecovery action_client_recovery_;

  //! In-process, atomic plan handoff which preserves the ExePath goal owner.
  ContinuousPlanUpdater continuous_plan_updater_;

  //! current distance to goal (we will stop replanning if very close to avoid destabilizing the controller)
  std::atomic<double> dist_to_goal_;

  //! Replanning period dynamically reconfigurable and read by its worker.
  std::atomic<double> replanning_period_seconds_{0.0};

  //! Maximum wall age and SE(2) join envelope for a periodic planner result.
  double periodic_plan_max_age_{10.0};
  double periodic_plan_max_join_distance_{0.75};
  double periodic_plan_max_join_yaw_{1.57};

  //! Replanning thread, running permanently
  boost::thread replanning_thread_;

  //! true, if recovery behavior for the MoveBase action is enabled.
  bool recovery_enabled_;

  std::vector<std::string> recovery_behaviors_;

  std::vector<std::string>::iterator current_recovery_behavior_;

  const std::vector<std::string> &behaviors_;

  enum MoveBaseActionState
  {
    NONE,
    GET_PATH,
    EXE_PATH,
    RECOVERY,
    OSCILLATING,
    SUCCEEDED,
    CANCELED,
    FAILED
  };

  std::atomic<MoveBaseActionState> action_state_;
  MoveBaseActionState recovery_trigger_;

  //! Monotonic ownership for controller executions and async planner calls.
  std::atomic<std::uint64_t> execution_epoch_{0u};
  std::atomic<std::uint64_t> fresh_plan_generation_{0u};
  std::atomic<std::uint64_t> periodic_plan_generation_{0u};
  std::atomic<std::uint64_t> controller_generation_{0u};
  std::atomic<bool> shutting_down_{false};

  //! Serializes outer-goal ownership with all asynchronous callbacks.
  mutable boost::mutex lifecycle_mtx_;
  std::atomic<bool> has_active_goal_{false};

  //! Protect reusable action-goal templates from start/replanning races.
  mutable boost::mutex goal_template_mtx_;
};

} /* mbf_abstract_nav */

#endif //MBF_ABSTRACT_NAV__MOVE_BASE_ACTION_H_

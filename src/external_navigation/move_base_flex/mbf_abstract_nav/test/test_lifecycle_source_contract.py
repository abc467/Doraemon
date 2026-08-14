#!/usr/bin/env python3

"""Static regression guards for MBF's cross-thread lifecycle contract.

The upstream package forcibly disables its legacy rostest block, so these
small tests stay directly runnable with Python and protect the safety-critical
ordering that is otherwise difficult to reproduce deterministically.
"""

from pathlib import Path
import re
import unittest


PACKAGE = Path(__file__).resolve().parents[1]
WORKSPACE_SRC = Path(__file__).resolve().parents[4]


def _read(relative: str) -> str:
    return (PACKAGE / relative).read_text(encoding="utf-8")


class LifecycleSourceContractTest(unittest.TestCase):
    def test_stale_cancel_requires_exact_goal_handle(self):
        source = _read("include/mbf_abstract_nav/abstract_action_base.hpp")
        self.assertRegex(
            source,
            r"in_use\s*&&\s*\n\s*slot_it->second\.goal_handle\s*==\s*goal_handle",
        )

    def test_continuous_update_is_admitted_by_live_execution(self):
        source = _read("src/controller_action.cpp")
        self.assertIn("trySetNewPlanWhileActive", source)
        self.assertIn("Empty continuous plan update rejected", source)

    def test_server_side_patience_is_not_reported_as_client_cancel(self):
        for relative in (
            "src/controller_action.cpp",
            "src/planner_action.cpp",
            "src/recovery_action.cpp",
        ):
            source = _read(relative)
            patience_block = source[source.find("isPatienceExceeded"):]
            self.assertIn("setAborted", patience_block, relative)

    def test_cancel_is_rechecked_after_controller_evaluation(self):
        source = _read("src/abstract_controller_execution.cpp")
        compute = source.index("outcome_ = computeVelocityCmd")
        cancel = source.index("if (cancel_.load())", compute)
        publish = source.index("vel_pub_.publish(cmd_vel_stamped.twist)", compute)
        self.assertLess(cancel, publish)

    def test_safety_fault_requires_stable_recovery_hold(self):
        source = _read("src/abstract_controller_execution.cpp")
        self.assertIn("safety_check_fault_active_", source)
        self.assertIn("safety_check_recovery_hold_", source)
        config = (
            WORKSPACE_SRC / "cleanrobot/config/nav/mbf_timing.yaml"
        ).read_text(encoding="utf-8")
        self.assertRegex(config, r"safety_check_recovery_hold:\s*0\.5")

    def test_controller_instance_frequency_overrides_global_default(self):
        source = _read("src/abstract_controller_execution.cpp")
        self.assertIn('name_ + "/controller_frequency"', source)
        self.assertIn("private_nh.getParam(instance_frequency_param", source)
        self.assertIn("setControllerFrequency(controller_frequency)", source)

        timing = (
            WORKSPACE_SRC / "cleanrobot/config/nav/mbf_timing.yaml"
        ).read_text(encoding="utf-8")
        self.assertRegex(timing, r"controller_frequency:\s*10\.0")

        nav = (
            WORKSPACE_SRC / "cleanrobot/config/nav/mbf_nav.yaml"
        ).read_text(encoding="utf-8")
        state = nav[nav.index("MPPI_State_Lattice_Controller:") :]
        self.assertRegex(state, r"controller_frequency:\s*10\.0")
        self.assertRegex(state, r"model_dt:\s*0\.10")

    def test_old_controller_callbacks_have_a_separate_generation(self):
        source = _read("src/move_base_action.cpp")
        self.assertIn("controller_generation_.fetch_add", source)
        self.assertIn(
            "controller_generation != controller_generation_.load()", source
        )

    def test_periodic_update_keeps_original_exe_action_owner(self):
        move_base = _read("src/move_base_action.cpp")
        controller = _read("src/controller_action.cpp")
        replan = move_base[move_base.index("void MoveBaseAction::replanningThread()") :]
        self.assertIn("continuous_plan_updater_(goal, update_reason)", replan)
        self.assertNotIn("action_client_exe_path_.sendGoal", replan)
        self.assertNotIn("controller_generation_.fetch_add", replan)
        update = controller[
            controller.index("bool ControllerAction::tryUpdateContinuousPlan") :
            controller.index("void ControllerAction::start")
        ]
        self.assertIn("trySetNewPlanWhileActive", update)
        self.assertIn("controller reached a terminal state", update)
        self.assertNotIn("goal_handle", update)
        self.assertNotIn(".setCanceled", update)

    def test_periodic_replanning_waits_a_full_period_and_preserves_execution(self):
        source = _read("src/move_base_action.cpp")
        timing = (
            WORKSPACE_SRC / "cleanrobot/config/nav/mbf_timing.yaml"
        ).read_text(encoding="utf-8")
        self.assertRegex(timing, r"planner_frequency:\s*0\.0(?:\s|$)")
        self.assertIn(
            "if (!action_client_replanning_.getState().isDone())", source
        )
        self.assertIn("scheduled_epoch != execution_epoch_.load()", source)
        self.assertIn("last_replan_time = ros::Time::now();", source)
        self.assertIn(
            "goal.plan_update_mode = mbf_msgs::ExePathGoal::CONTINUOUS_UPDATE",
            source,
        )
        self.assertIn("goal.execution_epoch = request_epoch", source)
        self.assertIn("periodicPlanIsFreshAndReachable", source)
        self.assertIn("nearestReachablePathSuffix", source)
        self.assertIn("buildJoinedPath", source)
        self.assertIn("goal.path = std::move(joined_path)", source)
        self.assertNotIn("goal.path = result->path", source)
        self.assertIn(
            "burning an entire CPU core while a controller is active", source
        )

    def test_periodic_result_envelope_is_explicitly_configured(self):
        timing = (
            WORKSPACE_SRC / "cleanrobot/config/nav/mbf_timing.yaml"
        ).read_text(encoding="utf-8")
        self.assertRegex(timing, r"periodic_plan_max_age:\s*10\.0")
        self.assertRegex(timing, r"periodic_plan_max_join_distance:\s*0\.15")
        self.assertRegex(timing, r"periodic_plan_max_join_yaw:\s*0\.35")

    def test_terminal_controller_result_cancels_periodic_planner(self):
        source = _read("src/move_base_action.cpp")
        done = source[
            source.index("void MoveBaseAction::actionExePathDone") :
            source.index("bool MoveBaseAction::attemptRecovery")
        ]
        generation = done.index("periodic_plan_generation_.fetch_add")
        state_switch = done.index("switch (state.state_)")
        self.assertLess(generation, state_switch)
        self.assertIn("action_client_replanning_.cancelGoal()", done)

    def test_slot_thread_lifecycle_is_joined_and_synchronized(self):
        source = _read("include/mbf_abstract_nav/abstract_action_base.hpp")
        start = source[source.index("virtual void start(") : source.index("virtual void cancel(")]
        self.assertIn("boost::lock_guard<boost::mutex> start_guard(start_mtx_)", start)
        join_pos = start.index("old_thread->join()")
        second_recalling = start.index(
            "actionlib_msgs::GoalStatus::RECALLING", join_pos
        )
        self.assertLess(join_pos, start.index("delete slot_it->second.thread_ptr"))
        self.assertLess(
            join_pos,
            second_recalling,
        )
        self.assertLess(
            second_recalling,
            start.index("slot_it->second.goal_handle.setAccepted()"),
        )
        run = source[source.index("virtual void run(") : source.index("virtual void reconfigureAll")]
        self.assertIn("boost::lock_guard<boost::mutex> slot_guard(slot_map_mtx_)", run)
        self.assertIn("slot.in_use = false", run)

    def test_original_requested_target_survives_planner_tolerance(self):
        context = (
            WORKSPACE_SRC
            / "external_navigation/move_base_flex/mbf_abstract_core/include/mbf_abstract_core/plan_execution_context.h"
        ).read_text(encoding="utf-8")
        controller = _read("src/abstract_controller_execution.cpp")
        self.assertIn("has_requested_target", context)
        self.assertIn("original requested goal residual", controller)
        self.assertIn("ExePathResult::MISSED_GOAL", controller)


if __name__ == "__main__":
    unittest.main()

import unittest
from unittest import mock

import actionlib

from coverage_task_manager.mbf_move_base import MBFMoveBase


class _FakeSimpleActionClient:
    def __init__(self, _action_name, _action_type):
        self.state = actionlib.GoalStatus.LOST
        self.sent_goals = []
        self.cancel_count = 0

    def send_goal(self, goal):
        self.sent_goals.append(goal)
        self.state = actionlib.GoalStatus.PENDING

    def cancel_all_goals(self):
        self.cancel_count += 1
        self.state = actionlib.GoalStatus.PREEMPTED

    def get_state(self):
        return self.state


class MBFMoveBaseGoalLifecycleTest(unittest.TestCase):
    def setUp(self):
        patcher = mock.patch(
            "coverage_task_manager.mbf_move_base.actionlib.SimpleActionClient",
            _FakeSimpleActionClient,
        )
        self.addCleanup(patcher.stop)
        patcher.start()
        self.nav = MBFMoveBase()

    def test_initial_lost_is_not_a_completed_goal(self):
        self.assertEqual(self.nav.get_state(), actionlib.GoalStatus.LOST)
        self.assertFalse(self.nav.done())

    def test_terminal_result_is_consumed_before_next_goal_dispatch(self):
        self.nav.send_goal(mock.Mock())
        self.nav._cli.state = actionlib.GoalStatus.SUCCEEDED

        self.assertTrue(self.nav.done())
        self.assertTrue(self.nav.succeeded())
        self.assertEqual(self.nav.get_state(), actionlib.GoalStatus.SUCCEEDED)

        # A new docking phase may be selected before its goal is sent. The
        # previous success must not be observed as that new phase's result.
        self.assertFalse(self.nav.done())

        self.nav.send_goal(mock.Mock())
        self.assertFalse(self.nav.done())
        self.nav._cli.state = actionlib.GoalStatus.ABORTED
        self.assertTrue(self.nav.done())
        self.assertFalse(self.nav.succeeded())
        self.assertEqual(self.nav.get_state(), actionlib.GoalStatus.ABORTED)

    def test_cancel_clears_local_goal_lifecycle(self):
        self.nav.send_goal(mock.Mock())
        self.nav.cancel_all()

        self.assertEqual(self.nav._cli.cancel_count, 1)
        self.assertFalse(self.nav.done())


if __name__ == "__main__":
    unittest.main()

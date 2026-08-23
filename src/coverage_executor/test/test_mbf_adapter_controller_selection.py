#!/usr/bin/env python3

import threading
import unittest
from types import SimpleNamespace
from unittest import mock

import actionlib

from coverage_executor import mbf_adapter


class _FakeMoveBaseGoal:
    def __init__(self):
        self.target_pose = None
        self.planner = ""
        self.controller = ""
        self.recovery_behaviors = []


class _FakeClient:
    def __init__(self):
        self.goals = []
        self.cancel_goal_count = 0
        self.state = actionlib.GoalStatus.LOST

    def send_goal(self, goal, **_kwargs):
        self.goals.append(goal)
        self.state = actionlib.GoalStatus.ACTIVE

    def cancel_goal(self):
        self.cancel_goal_count += 1
        self.state = actionlib.GoalStatus.PREEMPTED

    def get_state(self):
        return self.state

    def get_result(self):
        return SimpleNamespace(outcome=116, message="costmap stale")


class MBFAdapterControllerSelectionTest(unittest.TestCase):
    def _adapter(self):
        adapter = mbf_adapter.MBFAdapter.__new__(mbf_adapter.MBFAdapter)
        adapter.planner = "SmacLatticePlanner"
        adapter.controller = "MPPI_Standard_Controller"
        adapter.connect_controller = "MPPI_Standard_Controller"
        adapter.recovery = ""
        adapter._mb = _FakeClient()
        adapter._exe = _FakeClient()
        adapter._last_connect_result = None
        adapter._last_exe_result = None
        adapter._last_exe_pose = None
        adapter._nav_lock = threading.RLock()
        adapter._active_navigation_kind = ""
        adapter._cancel_kind = ""
        adapter._cancel_requested_monotonic = 0.0
        adapter._cancel_status_baseline = {}
        adapter._status_sequences = {
            "get_path": 0,
            "exe_path": 0,
            "recovery": 0,
        }
        adapter._status_active = {
            "get_path": False,
            "exe_path": False,
            "recovery": False,
        }
        return adapter

    @mock.patch.object(mbf_adapter, "MoveBaseGoal", _FakeMoveBaseGoal)
    def test_point_to_point_uses_configured_standard_controller(self):
        adapter = self._adapter()
        adapter.send_connect(SimpleNamespace())
        self.assertEqual(
            adapter._mb.goals[-1].planner,
            "SmacLatticePlanner",
        )
        self.assertEqual(
            adapter._mb.goals[-1].controller,
            "MPPI_Standard_Controller",
        )

    @mock.patch.object(mbf_adapter, "MoveBaseGoal", _FakeMoveBaseGoal)
    def test_explicit_controller_override_is_preserved(self):
        adapter = self._adapter()
        adapter.send_connect(SimpleNamespace(), controller="EngineeringController")
        self.assertEqual(adapter._mb.goals[-1].controller, "EngineeringController")

    def test_terminal_result_details_are_retained(self):
        adapter = self._adapter()
        result = adapter.get_connect_result()
        self.assertEqual(result.outcome, 116)
        self.assertEqual(result.message, "costmap stale")

    @mock.patch.object(mbf_adapter, "MoveBaseGoal", _FakeMoveBaseGoal)
    def test_connect_cancel_owns_only_move_base_and_waits_for_children(self):
        adapter = self._adapter()
        adapter.send_connect(SimpleNamespace())

        adapter.cancel_all()

        self.assertEqual(adapter._mb.cancel_goal_count, 1)
        self.assertEqual(adapter._exe.cancel_goal_count, 0)
        # The outer MoveBase goal is terminal, but this is intentionally not
        # enough: its get_path/exe_path/recovery children must report a fresh,
        # inactive status after cancellation.
        self.assertFalse(adapter.navigation_drained())
        for kind in ("get_path", "exe_path", "recovery"):
            adapter._status_cb(SimpleNamespace(status_list=[]), kind)
        self.assertTrue(adapter.navigation_drained())

    def test_follow_cancel_owns_only_direct_exe_path(self):
        adapter = self._adapter()
        adapter._mark_navigation_started("follow")
        adapter._exe.state = actionlib.GoalStatus.ACTIVE

        adapter.cancel_all()

        self.assertEqual(adapter._mb.cancel_goal_count, 0)
        self.assertEqual(adapter._exe.cancel_goal_count, 1)
        self.assertFalse(adapter.navigation_drained())
        adapter._status_cb(SimpleNamespace(status_list=[]), "exe_path")
        self.assertTrue(adapter.navigation_drained())


if __name__ == "__main__":
    unittest.main()

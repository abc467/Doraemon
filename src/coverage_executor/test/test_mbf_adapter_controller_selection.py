#!/usr/bin/env python3

import unittest
from types import SimpleNamespace
from unittest import mock

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

    def send_goal(self, goal, **_kwargs):
        self.goals.append(goal)

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
        adapter._last_connect_result = None
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


if __name__ == "__main__":
    unittest.main()

# -*- coding: utf-8 -*-

from typing import Optional

import rospy
import actionlib

from geometry_msgs.msg import PoseStamped
from mbf_msgs.msg import MoveBaseAction, MoveBaseGoal


class MBFMoveBase:
    """Minimal MoveBaseAction client (Task layer), used for docking/undocking."""

    def __init__(
        self,
        action_name: str = "/move_base_flex/move_base",
        planner: str = "",
        controller: str = "",
        recovery: str = "",
    ):
        self.action_name = str(action_name)
        self.planner = str(planner)
        self.controller = str(controller)
        self.recovery = str(recovery)
        self._cli = actionlib.SimpleActionClient(self.action_name, MoveBaseAction)

    def wait_for_server(self):
        rospy.loginfo("[TASK/MBF] waiting %s ...", self.action_name)
        self._cli.wait_for_server()
        rospy.loginfo("[TASK/MBF] server ready")

    def cancel_all(self):
        try:
            self._cli.cancel_all_goals()
        except Exception:
            pass

    def send_goal(self, pose: PoseStamped):
        g = MoveBaseGoal()
        g.target_pose = pose
        if hasattr(g, "planner") and self.planner:
            g.planner = self.planner
        if hasattr(g, "controller") and self.controller:
            g.controller = self.controller
        if hasattr(g, "recovery_behaviors") and self.recovery:
            g.recovery_behaviors = self.recovery
        self._cli.send_goal(g)

    def done(self) -> bool:
        return self._cli.get_state() in [
            actionlib.GoalStatus.SUCCEEDED,
            actionlib.GoalStatus.ABORTED,
            actionlib.GoalStatus.REJECTED,
            actionlib.GoalStatus.PREEMPTED,
            actionlib.GoalStatus.RECALLED,
            actionlib.GoalStatus.LOST,
        ]

    def succeeded(self) -> bool:
        return self._cli.get_state() == actionlib.GoalStatus.SUCCEEDED

    def get_state(self) -> int:
        return int(self._cli.get_state())

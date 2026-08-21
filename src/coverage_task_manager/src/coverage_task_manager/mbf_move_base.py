# -*- coding: utf-8 -*-

import threading
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
        # SimpleActionClient reports LOST before its first goal and keeps the
        # previous terminal state until the next goal is installed. The task
        # manager polls this wrapper from a different thread, so exposing that
        # raw state creates a race: a newly selected docking phase can consume
        # LOST (or the previous SUCCEEDED) before send_goal() runs.
        self._goal_lock = threading.Lock()
        self._goal_inflight = False
        self._terminal_state: Optional[int] = None

    def wait_for_server(self):
        rospy.loginfo("[TASK/MBF] waiting %s ...", self.action_name)
        self._cli.wait_for_server()
        rospy.loginfo("[TASK/MBF] server ready")

    def cancel_all(self):
        with self._goal_lock:
            try:
                self._cli.cancel_all_goals()
            except Exception:
                pass
            self._goal_inflight = False
            self._terminal_state = None

    def send_goal(self, pose: PoseStamped):
        g = MoveBaseGoal()
        g.target_pose = pose
        if hasattr(g, "planner") and self.planner:
            g.planner = self.planner
        if hasattr(g, "controller") and self.controller:
            g.controller = self.controller
        if hasattr(g, "recovery_behaviors") and self.recovery:
            g.recovery_behaviors = self.recovery
        # Hold the local lifecycle lock until SimpleActionClient has installed
        # the new goal. done() therefore cannot observe the client's initial
        # LOST state in the dispatch window.
        with self._goal_lock:
            self._goal_inflight = False
            self._terminal_state = None
            self._cli.send_goal(g)
            self._goal_inflight = True

    def done(self) -> bool:
        with self._goal_lock:
            if not self._goal_inflight:
                return False
            state = int(self._cli.get_state())
            if state not in [
                actionlib.GoalStatus.SUCCEEDED,
                actionlib.GoalStatus.ABORTED,
                actionlib.GoalStatus.REJECTED,
                actionlib.GoalStatus.PREEMPTED,
                actionlib.GoalStatus.RECALLED,
                actionlib.GoalStatus.LOST,
            ]:
                return False
            # Consume the result once. succeeded()/get_state() still see the
            # cached terminal state, while a later phase cannot mistake it for
            # a goal that has not yet been sent.
            self._terminal_state = state
            self._goal_inflight = False
            return True

    def succeeded(self) -> bool:
        with self._goal_lock:
            state = self._terminal_state
            if state is None:
                state = int(self._cli.get_state())
            return state == actionlib.GoalStatus.SUCCEEDED

    def get_state(self) -> int:
        with self._goal_lock:
            if self._terminal_state is not None:
                return int(self._terminal_state)
            return int(self._cli.get_state())

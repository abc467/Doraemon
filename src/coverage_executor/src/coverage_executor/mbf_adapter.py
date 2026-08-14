# -*- coding: utf-8 -*-
import rospy
import actionlib
from typing import Optional, Callable
from geometry_msgs.msg import PoseStamped
from nav_msgs.msg import Path
from std_srvs.srv import Empty

from mbf_msgs.msg import MoveBaseAction, MoveBaseGoal, ExePathAction, ExePathGoal


class MBFAdapter:
    """
    - connect_to: MoveBaseAction
    - execute_path: ExePathAction
    支持：
      - send goal (non-blocking)
      - poll state / feedback
      - cancel
    """

    def __init__(
        self,
        move_base_action: str = "/move_base_flex/move_base",
        exe_path_action: str = "/move_base_flex/exe_path",
        planner: str = "",
        controller: str = "",
        connect_controller: str = "",
        recovery: str = "",
        clear_costmaps_service: str = "/move_base_flex/clear_costmaps",
    ):
        self.move_base_action = move_base_action
        self.exe_path_action = exe_path_action
        self.planner = planner
        self.controller = controller
        self.connect_controller = str(connect_controller or "").strip()
        self.recovery = recovery
        self.clear_costmaps_service = str(clear_costmaps_service or "").strip()

        self._mb = actionlib.SimpleActionClient(self.move_base_action, MoveBaseAction)
        self._exe = actionlib.SimpleActionClient(self.exe_path_action, ExePathAction)

        self._last_exe_pose: Optional[PoseStamped] = None
        self._last_connect_result = None
        self._last_exe_result = None
        self._clear_costmaps_cli = None
        if self.clear_costmaps_service:
            try:
                self._clear_costmaps_cli = rospy.ServiceProxy(self.clear_costmaps_service, Empty)
            except Exception:
                self._clear_costmaps_cli = None

    def wait_for_servers(self):
        rospy.loginfo("[MBF] waiting %s ...", self.move_base_action)
        self._mb.wait_for_server()
        rospy.loginfo("[MBF] waiting %s ...", self.exe_path_action)
        self._exe.wait_for_server()
        rospy.loginfo("[MBF] servers ready")

    def set_controller_name(self, controller: str):
        self.controller = str(controller or "").strip()

    def cancel_all(self):
        try:
            self._mb.cancel_all_goals()
        except Exception:
            pass
        try:
            self._exe.cancel_all_goals()
        except Exception:
            pass
        self._last_connect_result = None
        self._last_exe_result = None

    # -------------------- CONNECT (MoveBase) --------------------
    def send_connect(self, target: PoseStamped, controller: Optional[str] = None):
        g = MoveBaseGoal()
        g.target_pose = target
        if hasattr(g, "planner") and self.planner:
            g.planner = self.planner
        # Every point-to-point action, including charge approach and undock,
        # defaults to the dedicated State Lattice controller.  The coverage
        # controller is only a fallback for legacy launch files.
        selected_controller = (
            (self.connect_controller or self.controller)
            if controller is None
            else str(controller).strip()
        )
        if hasattr(g, "controller") and selected_controller:
            g.controller = selected_controller
        if hasattr(g, "recovery_behaviors") and self.recovery:
            g.recovery_behaviors = self.recovery
        self._last_connect_result = None
        self._mb.send_goal(g)

    def connect_done(self) -> bool:
        return self._mb.get_state() in [
            actionlib.GoalStatus.SUCCEEDED,
            actionlib.GoalStatus.ABORTED,
            actionlib.GoalStatus.REJECTED,
            actionlib.GoalStatus.PREEMPTED,
            actionlib.GoalStatus.RECALLED,
            actionlib.GoalStatus.LOST,
        ]

    def connect_succeeded(self) -> bool:
        return self._mb.get_state() == actionlib.GoalStatus.SUCCEEDED

    def get_connect_result(self):
        try:
            result = self._mb.get_result()
            if result is not None:
                self._last_connect_result = result
        except Exception:
            pass
        return self._last_connect_result

    # -------------------- EXECUTE PATH (ExePath) --------------------
    def send_execute_path(self, path: Path, tolerance_from_action: float = 0.0):
        g = ExePathGoal()
        g.path = path
        if hasattr(g, "controller") and self.controller:
            g.controller = self.controller
        if hasattr(g, "tolerance_from_action") and tolerance_from_action > 0.0:
            g.tolerance_from_action = float(tolerance_from_action)

        def _fb_cb(fb):
            try:
                if hasattr(fb, "feedback") and hasattr(fb.feedback, "current_pose"):
                    self._last_exe_pose = fb.feedback.current_pose
            except Exception:
                pass

        self._last_exe_pose = None
        self._last_exe_result = None
        self._exe.send_goal(g, feedback_cb=_fb_cb)

    def exe_done(self) -> bool:
        return self._exe.get_state() in [
            actionlib.GoalStatus.SUCCEEDED,
            actionlib.GoalStatus.ABORTED,
            actionlib.GoalStatus.REJECTED,
            actionlib.GoalStatus.PREEMPTED,
            actionlib.GoalStatus.RECALLED,
            actionlib.GoalStatus.LOST,
        ]

    def exe_succeeded(self) -> bool:
        return self._exe.get_state() == actionlib.GoalStatus.SUCCEEDED

    def get_exe_result(self):
        try:
            result = self._exe.get_result()
            if result is not None:
                self._last_exe_result = result
        except Exception:
            pass
        return self._last_exe_result

    def get_last_exe_pose(self) -> Optional[PoseStamped]:
        return self._last_exe_pose


    def get_connect_state(self) -> int:
        try:
            return int(self._mb.get_state())
        except Exception:
            return -1

    def get_exe_state(self) -> int:
        try:
            return int(self._exe.get_state())
        except Exception:
            return -1

    def clear_costmaps(self) -> bool:
        if self._clear_costmaps_cli is None or (not self.clear_costmaps_service):
            return False
        try:
            rospy.wait_for_service(self.clear_costmaps_service, timeout=0.5)
            self._clear_costmaps_cli()
            rospy.logwarn("[MBF] clear_costmaps ok: %s", self.clear_costmaps_service)
            return True
        except Exception as e:
            rospy.logwarn("[MBF] clear_costmaps failed: %s", str(e))
            return False

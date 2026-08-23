# -*- coding: utf-8 -*-
import threading
import time

import rospy
import actionlib
from actionlib_msgs.msg import GoalStatus, GoalStatusArray
from typing import Optional, Callable
from geometry_msgs.msg import PoseStamped
from nav_msgs.msg import Path
from std_srvs.srv import Empty

from mbf_msgs.msg import MoveBaseAction, MoveBaseGoal, ExePathAction, ExePathGoal
from mbf_msgs.srv import CheckPose, CheckPoseRequest


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
        check_pose_cost_service: str = "/move_base_flex/check_pose_cost",
    ):
        self.move_base_action = move_base_action
        self.exe_path_action = exe_path_action
        self.planner = planner
        self.controller = controller
        self.connect_controller = str(connect_controller or "").strip()
        self.recovery = recovery
        self.clear_costmaps_service = str(clear_costmaps_service or "").strip()
        self.check_pose_cost_service = str(check_pose_cost_service or "").strip()

        self._mb = actionlib.SimpleActionClient(self.move_base_action, MoveBaseAction)
        self._exe = actionlib.SimpleActionClient(self.exe_path_action, ExePathAction)

        self._last_exe_pose: Optional[PoseStamped] = None
        self._last_connect_result = None
        self._last_exe_result = None
        self._init_navigation_lifecycle()
        self._clear_costmaps_cli = None
        self._check_pose_cost_cli = None
        if self.clear_costmaps_service:
            try:
                self._clear_costmaps_cli = rospy.ServiceProxy(self.clear_costmaps_service, Empty)
            except Exception:
                self._clear_costmaps_cli = None
        if self.check_pose_cost_service:
            try:
                self._check_pose_cost_cli = rospy.ServiceProxy(
                    self.check_pose_cost_service,
                    CheckPose,
                )
            except Exception:
                self._check_pose_cost_cli = None

    def _init_navigation_lifecycle(self):
        """Initialize action ownership and child-action drain bookkeeping.

        A coverage CONNECT owns the outer ``move_base`` action; MBF owns and
        cancels its planner/controller/recovery children.  A coverage FOLLOW
        instead owns a direct ``exe_path`` action.  These are deliberately
        distinct: broadcasting cancellation to both action names while MBF is
        also cancelling its child is a racy, redundant operation.
        """
        self._nav_lock = threading.RLock()
        self._active_navigation_kind = ""  # connect | follow | ""
        self._cancel_kind = ""
        self._cancel_requested_monotonic = 0.0
        self._cancel_status_baseline = {}
        self._status_sequences = {}
        self._status_active = {}

        namespace = str(self.move_base_action or "").rstrip("/").rsplit("/", 1)[0]
        if not namespace:
            namespace = "/move_base_flex"
        self._child_action_names = {
            "get_path": namespace + "/get_path",
            "exe_path": str(self.exe_path_action or namespace + "/exe_path").rstrip("/"),
            "recovery": namespace + "/recovery",
        }
        self._status_subscribers = []
        for kind, action_name in self._child_action_names.items():
            self._status_sequences[kind] = 0
            self._status_active[kind] = False
            try:
                self._status_subscribers.append(
                    rospy.Subscriber(
                        action_name + "/status",
                        GoalStatusArray,
                        self._status_cb,
                        callback_args=kind,
                        queue_size=1,
                    )
                )
            except Exception as exc:
                rospy.logwarn("[MBF] cannot subscribe %s/status: %s", action_name, str(exc))

    @staticmethod
    def _terminal_action_state(state: int) -> bool:
        return state in [
            actionlib.GoalStatus.SUCCEEDED,
            actionlib.GoalStatus.ABORTED,
            actionlib.GoalStatus.REJECTED,
            actionlib.GoalStatus.PREEMPTED,
            actionlib.GoalStatus.RECALLED,
            actionlib.GoalStatus.LOST,
        ]

    @staticmethod
    def _status_is_active(status: int) -> bool:
        return status in [
            GoalStatus.PENDING,
            GoalStatus.ACTIVE,
            GoalStatus.PREEMPTING,
            GoalStatus.RECALLING,
        ]

    def _status_cb(self, msg: GoalStatusArray, kind: str):
        active = any(self._status_is_active(item.status) for item in msg.status_list)
        with self._nav_lock:
            self._status_sequences[kind] = int(self._status_sequences.get(kind, 0)) + 1
            self._status_active[kind] = bool(active)

    def _mark_navigation_started(self, kind: str):
        with self._nav_lock:
            self._active_navigation_kind = str(kind)
            # A new navigation request owns a new lifecycle.  A completed
            # previous cancel must never block it.
            self._cancel_kind = ""
            self._cancel_requested_monotonic = 0.0
            self._cancel_status_baseline = {}

    def _mark_navigation_terminal(self, kind: str):
        with self._nav_lock:
            if self._active_navigation_kind == str(kind):
                self._active_navigation_kind = ""

    def cancel_active_navigation(self) -> str:
        """Cancel exactly the action currently owned by the executor.

        ``SimpleActionClient.cancel_all_goals()`` is intentionally not used:
        it is a broadcast on the action name and can race a newly dispatched
        task-manager docking goal.  During CONNECT, canceling the outer
        MoveBase goal is sufficient and lets MBF cancel its own children.
        During FOLLOW, the executor owns a standalone ExePath goal directly.
        """
        with self._nav_lock:
            kind = str(self._active_navigation_kind or "")
            self._cancel_kind = kind
            self._cancel_requested_monotonic = time.monotonic()
            self._cancel_status_baseline = dict(self._status_sequences)

        try:
            if kind == "connect":
                self._mb.cancel_goal()
            elif kind == "follow":
                self._exe.cancel_goal()
            else:
                rospy.logdebug("[MBF] cancel requested with no executor-owned navigation action")
        except Exception as exc:
            rospy.logwarn("[MBF] cancel %s action failed: %s", kind or "none", str(exc))
        return kind

    def navigation_drained(self) -> bool:
        """Return true only after the canceled owned action and MBF children drain.

        For CONNECT, MoveBase reports CANCELED *before* it sends cancellation
        to its child actions, so the outer result alone is not an adequate
        handoff barrier.  We require fresh post-cancel status observations for
        get_path/exe_path/recovery and no nonterminal child action.
        """
        with self._nav_lock:
            kind = str(self._cancel_kind or "")
            baseline = dict(self._cancel_status_baseline)

        if not kind:
            return True

        try:
            owner_state = self._mb.get_state() if kind == "connect" else self._exe.get_state()
        except Exception:
            return False
        if not self._terminal_action_state(owner_state):
            return False

        child_kinds = ("get_path", "exe_path", "recovery") if kind == "connect" else ("exe_path",)
        with self._nav_lock:
            for child_kind in child_kinds:
                # A post-cancel status update is the proof that the MBF action
                # server has processed the cancellation, rather than merely
                # accepting the outer action's canceled result.
                if int(self._status_sequences.get(child_kind, 0)) <= int(baseline.get(child_kind, 0)):
                    return False
                if bool(self._status_active.get(child_kind, False)):
                    return False
        return True

    def wait_for_navigation_drain(self, timeout_s: float, poll_s: float = 0.02) -> bool:
        deadline = time.monotonic() + max(0.0, float(timeout_s))
        while not rospy.is_shutdown():
            if self.navigation_drained():
                return True
            if time.monotonic() >= deadline:
                break
            rospy.sleep(max(0.005, float(poll_s)))
        return self.navigation_drained()

    def wait_for_servers(self):
        rospy.loginfo("[MBF] waiting %s ...", self.move_base_action)
        self._mb.wait_for_server()
        rospy.loginfo("[MBF] waiting %s ...", self.exe_path_action)
        self._exe.wait_for_server()
        rospy.loginfo("[MBF] servers ready")

    def set_controller_name(self, controller: str):
        self.controller = str(controller or "").strip()

    def cancel_all(self):
        # Kept as the public adapter API for existing FSM callers.  Its
        # semantics are now exact-owner cancellation, not an action-name-wide
        # broadcast.
        self.cancel_active_navigation()
        self._last_connect_result = None
        self._last_exe_result = None

    # -------------------- CONNECT (MoveBase) --------------------
    def send_connect(self, target: PoseStamped, controller: Optional[str] = None):
        g = MoveBaseGoal()
        g.target_pose = target
        if hasattr(g, "planner") and self.planner:
            g.planner = self.planner
        # Every point-to-point action, including charge approach and undock,
        # uses the explicitly configured connect controller. The coverage
        # controller remains the fallback for legacy launch files.
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
        self._mark_navigation_started("connect")
        self._mb.send_goal(g)

    def connect_done(self) -> bool:
        done = self._terminal_action_state(self._mb.get_state())
        if done:
            self._mark_navigation_terminal("connect")
        return done

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
        self._mark_navigation_started("follow")
        self._exe.send_goal(g, feedback_cb=_fb_cb)

    def exe_done(self) -> bool:
        done = self._terminal_action_state(self._exe.get_state())
        if done:
            self._mark_navigation_terminal("follow")
        return done

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

    def check_global_pose(self, pose: Optional[PoseStamped] = None, *, current_pose: bool = False):
        """Check the complete robot footprint on MBF's global costmap.

        Returns ``(service_ok, state, cost, message)``.  ``state`` follows
        ``mbf_msgs/CheckPose``: FREE=0, INSCRIBED=1, LETHAL=2,
        UNKNOWN=3, OUTSIDE=4.  A service failure is kept distinct from a
        collision so recovery code can fail closed without misreporting the
        map state.
        """
        if self._check_pose_cost_cli is None or (not self.check_pose_cost_service):
            return False, -1, 0, "check_pose_cost service is not configured"
        if (not current_pose) and pose is None:
            return False, -1, 0, "target pose is required"

        try:
            rospy.wait_for_service(self.check_pose_cost_service, timeout=0.5)
            request = CheckPoseRequest()
            if pose is not None:
                request.pose = pose
            request.safety_dist = 0.0
            request.lethal_cost_mult = 1.0
            request.inscrib_cost_mult = 1.0
            request.unknown_cost_mult = 1.0
            request.costmap = CheckPoseRequest.GLOBAL_COSTMAP
            request.current_pose = bool(current_pose)
            response = self._check_pose_cost_cli(request)
            return True, int(response.state), int(response.cost), ""
        except Exception as exc:
            return False, -1, 0, str(exc)

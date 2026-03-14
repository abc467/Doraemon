# -*- coding: utf-8 -*-
"""Task-layer health monitoring (commercial-grade minimal set).

This module is intentionally conservative:
  - Detect obvious preconditions for safe autonomous operation.
  - Provide clear fault codes for field debugging.
  - Optionally request executor to PAUSE on fatal faults.

We do NOT implement complex recovery classification here (per your current scope).

New in v4:
  - Localization quality checks (PoseWithCovarianceStamped-compatible topic)
  - No-motion stall checks (TF displacement over time)

Design notes:
  - Checks are best-effort and should not block node startup.
  - ERROR-level checks are only fatal when a mission is RUNNING.
"""

from __future__ import annotations

import math
import time
import threading
from dataclasses import dataclass, field
from typing import Dict, Optional

import rospy
import tf2_ros
import actionlib

from diagnostic_msgs.msg import DiagnosticStatus
from geometry_msgs.msg import PoseWithCovarianceStamped
from mbf_msgs.msg import MoveBaseAction, ExePathAction


def _wrap_pi(a: float) -> float:
    while a > math.pi:
        a -= 2 * math.pi
    while a < -math.pi:
        a += 2 * math.pi
    return a


def _yaw_from_quat(x: float, y: float, z: float, w: float) -> float:
    # yaw (Z) from quaternion (x,y,z,w)
    # robust minimal implementation (avoid heavy deps)
    siny_cosp = 2.0 * (w * z + x * y)
    cosy_cosp = 1.0 - 2.0 * (y * y + z * z)
    return math.atan2(siny_cosp, cosy_cosp)


@dataclass
class HealthResult:
    level: str = "OK"  # OK/WARN/ERROR
    code: str = ""
    msg: str = ""
    details: Dict[str, str] = field(default_factory=dict)

    def as_diag_level(self) -> int:
        if self.level == "ERROR":
            return DiagnosticStatus.ERROR
        if self.level == "WARN":
            return DiagnosticStatus.WARN
        return DiagnosticStatus.OK


def _looks_running(exec_state: str) -> bool:
    """Whether executor is in a motion-related state.

    Used for *motion* stall checks. We treat CONNECT as motion-related because
    the robot is expected to move while navigating to the entry point.
    """
    s = (exec_state or "").strip().upper()
    if not s:
        return False
    if s.startswith("RUN"):
        return True
    if "FOLLOW" in s or "CONNECT" in s:
        return True
    return False


def _looks_progress_expected(exec_state: str) -> bool:
    """Whether coverage *progress* is expected to increase.

    Progress (progress_0_1) only advances while executing the coverage path (FOLLOW).
    During CONNECT / LOADING / APPLY_PROFILE, progress can legitimately stay at 0.
    """
    s = (exec_state or "").strip().upper()
    if not s:
        return False
    if s.startswith("RUN"):
        return True
    if "FOLLOW" in s:
        return True
    return False


class HealthMonitor:
    """Evaluate health for coverage mission.

    Checks (minimal, high value):
      1) Executor heartbeat (run_progress freshness)
      2) Localization quality (stale / high covariance / jump)
      3) TF availability and age (map->base)  [navigation prerequisite]
      4) MBF action servers connectivity (move_base + exe_path)
      5) No-motion stall (pose displacement not changing while executor running)
      6) Progress stall (progress_0_1 not increasing while executor running)

    NOTES:
      - If localization topic is not available, checks degrade to WARN (when idle) or ERROR (when running, if required).
      - TF checks are still enforced because navigation relies on TF chain.
      - Motion-stall SHOULD use odom->base by default to avoid AMCL map->base jitter.
    """

    def __init__(
        self,
        *,
        frame_id: str = "map",
        base_frame: str = "base_footprint",
        mbf_move_base_action: str = "/move_base_flex/move_base",
        mbf_exe_path_action: str = "/move_base_flex/exe_path",
        exec_progress_timeout_s: float = 3.0,
        tf_timeout_s: float = 0.1,
        tf_max_age_s: float = 0.6,
        progress_stall_timeout_s: float = 20.0,
        progress_stall_min_delta: float = 0.002,
        action_server_wait_s: float = 0.05,
        # localization quality
        loc_enable: bool = True,
        loc_topic: str = "",
        loc_require: bool = False,
        loc_stale_s: float = 1.0,
        loc_cov_xy_max: float = 0.25,      # variance (m^2), sqrt=0.5m
        loc_cov_yaw_max: float = 0.30,     # variance (rad^2), sqrt~0.55rad
        loc_jump_xy_m: float = 1.0,
        loc_jump_yaw_rad: float = 1.0,
        # pose stall
        pose_stall_enable: bool = True,
        pose_stall_timeout_s: float = 15.0,
        pose_stall_min_move_m: float = 0.05,
        pose_stall_min_yaw_rad: float = 0.12,
        pose_stall_frame_id: str = "odom",
    ):
        self.frame_id = str(frame_id)
        self.base_frame = str(base_frame)
        self.exec_progress_timeout_s = float(exec_progress_timeout_s)
        self.tf_timeout_s = float(tf_timeout_s)
        self.tf_max_age_s = float(tf_max_age_s)
        self.progress_stall_timeout_s = float(progress_stall_timeout_s)
        self.progress_stall_min_delta = float(progress_stall_min_delta)
        self.action_server_wait_s = float(action_server_wait_s)

        self.loc_enable = bool(loc_enable)
        self.loc_topic = str(loc_topic or "").strip()
        self.loc_require = bool(loc_require)
        self.loc_stale_s = float(loc_stale_s)
        self.loc_cov_xy_max = float(loc_cov_xy_max)
        self.loc_cov_yaw_max = float(loc_cov_yaw_max)
        self.loc_jump_xy_m = float(loc_jump_xy_m)
        self.loc_jump_yaw_rad = float(loc_jump_yaw_rad)

        self.pose_stall_enable = bool(pose_stall_enable)
        self.pose_stall_timeout_s = float(pose_stall_timeout_s)
        self.pose_stall_min_move_m = float(pose_stall_min_move_m)
        self.pose_stall_min_yaw_rad = float(pose_stall_min_yaw_rad)
        self.pose_stall_frame_id = str(pose_stall_frame_id or "").strip() or "odom"

        self._tf_buf = tf2_ros.Buffer(cache_time=rospy.Duration(10.0))
        self._tf_listener = tf2_ros.TransformListener(self._tf_buf)

        self._mb = actionlib.SimpleActionClient(str(mbf_move_base_action), MoveBaseAction)
        self._exe = actionlib.SimpleActionClient(str(mbf_exe_path_action), ExePathAction)

        # stall tracker (progress)
        self._last_prog: Optional[float] = None
        self._last_prog_move_ts: float = 0.0

        # pose motion tracker
        self._last_pose_xyyaw: Optional[tuple] = None
        self._last_pose_move_ts: float = 0.0

        # localization cache
        self._loc_lock = threading.Lock()
        self._loc_last_recv_ts: float = 0.0
        self._loc_last_stamp: float = 0.0
        self._loc_last_xyyaw: Optional[tuple] = None
        self._loc_last_cov_xy: float = 0.0
        self._loc_last_cov_yaw: float = 0.0
        self._loc_prev_xyyaw: Optional[tuple] = None

        self._loc_sub = None
        if self.loc_enable and self.loc_topic:
            try:
                self._loc_sub = rospy.Subscriber(self.loc_topic, PoseWithCovarianceStamped, self._on_loc, queue_size=10)
            except Exception:
                self._loc_sub = None

    # ---------------- localization callback ----------------
    def _on_loc(self, msg: PoseWithCovarianceStamped):
        try:
            p = msg.pose.pose.position
            q = msg.pose.pose.orientation
            x = float(p.x)
            y = float(p.y)
            yaw = float(_yaw_from_quat(q.x, q.y, q.z, q.w))
            cov = list(msg.pose.covariance)
            cov_x = float(cov[0]) if len(cov) > 0 else 0.0
            cov_y = float(cov[7]) if len(cov) > 7 else 0.0
            cov_xy = max(cov_x, cov_y)
            cov_yaw = float(cov[35]) if len(cov) > 35 else 0.0
            stamp = 0.0
            try:
                stamp = msg.header.stamp.to_sec()
            except Exception:
                stamp = 0.0
        except Exception:
            return

        now = time.time()
        with self._loc_lock:
            self._loc_last_recv_ts = now
            self._loc_last_stamp = stamp
            self._loc_prev_xyyaw = self._loc_last_xyyaw
            self._loc_last_xyyaw = (x, y, yaw)
            self._loc_last_cov_xy = cov_xy
            self._loc_last_cov_yaw = cov_yaw

    # ------------ internal checks ------------
    def _check_executor_heartbeat(self, *, now: float, mission_running: bool, last_progress_ts: float) -> Optional[HealthResult]:
        if not mission_running:
            return None
        if last_progress_ts <= 0.0:
            return HealthResult(level="ERROR", code="EXEC_HEARTBEAT_MISSING", msg="no run_progress received yet")
        age = float(now - last_progress_ts)
        if age > self.exec_progress_timeout_s:
            return HealthResult(
                level="ERROR",
                code="EXEC_HEARTBEAT_TIMEOUT",
                msg=f"run_progress stale age={age:.2f}s > {self.exec_progress_timeout_s:.2f}s",
                details={"age_s": f"{age:.2f}"},
            )
        return None

    def _check_localization(self, *, now: float, mission_running: bool) -> Optional[HealthResult]:
        if not (self.loc_enable and self.loc_topic):
            return None

        with self._loc_lock:
            recv_ts = float(self._loc_last_recv_ts)
            stamp = float(self._loc_last_stamp)
            xyyaw = self._loc_last_xyyaw
            cov_xy = float(self._loc_last_cov_xy)
            cov_yaw = float(self._loc_last_cov_yaw)
            prev = self._loc_prev_xyyaw

        if recv_ts <= 0.0:
            lvl = "ERROR" if (mission_running and bool(self.loc_require)) else "WARN"
            return HealthResult(
                level=lvl,
                code="LOC_MISSING",
                msg=f"no localization data on {self.loc_topic}",
                details={"topic": self.loc_topic, "required": str(bool(self.loc_require))},
            )

        # stale check: prefer header.stamp; fallback to recv_ts
        base_ts = stamp if stamp > 1e-3 else recv_ts
        age = float(now - base_ts)
        if age > self.loc_stale_s:
            lvl = "ERROR" if mission_running else "WARN"
            return HealthResult(
                level=lvl,
                code="LOC_STALE",
                msg=f"localization stale age={age:.2f}s > {self.loc_stale_s:.2f}s",
                details={"age_s": f"{age:.2f}", "topic": self.loc_topic},
            )

        # covariance check
        if (cov_xy > self.loc_cov_xy_max) or (cov_yaw > self.loc_cov_yaw_max):
            lvl = "ERROR" if mission_running else "WARN"
            return HealthResult(
                level=lvl,
                code="LOC_COV_HIGH",
                msg=f"cov high xy={cov_xy:.3f} yaw={cov_yaw:.3f}",
                details={"cov_xy": f"{cov_xy:.3f}", "cov_yaw": f"{cov_yaw:.3f}"},
            )

        # jump check
        if mission_running and (prev is not None) and (xyyaw is not None):
            dx = float(xyyaw[0] - prev[0])
            dy = float(xyyaw[1] - prev[1])
            dist = math.hypot(dx, dy)
            dyaw = abs(_wrap_pi(float(xyyaw[2] - prev[2])))
            if dist > self.loc_jump_xy_m or dyaw > self.loc_jump_yaw_rad:
                return HealthResult(
                    level="ERROR",
                    code="LOC_JUMP",
                    msg=f"pose jump dist={dist:.2f}m yaw={dyaw:.2f}rad",
                    details={"dist_m": f"{dist:.2f}", "dyaw_rad": f"{dyaw:.2f}"},
                )
        return None

    def _check_tf(self, *, mission_running: bool) -> Optional[HealthResult]:
        try:
            trans = self._tf_buf.lookup_transform(
                self.frame_id,
                self.base_frame,
                rospy.Time(0),
                rospy.Duration(max(0.01, float(self.tf_timeout_s))),
            )
        except Exception as e:
            lvl = "ERROR" if mission_running else "WARN"
            return HealthResult(level=lvl, code="TF_LOOKUP_FAIL", msg=str(e))

        try:
            age = (rospy.Time.now() - trans.header.stamp).to_sec()
        except Exception:
            age = 0.0
        if age > self.tf_max_age_s:
            lvl = "ERROR" if mission_running else "WARN"
            return HealthResult(
                level=lvl,
                code="TF_STALE",
                msg=f"tf stale age={age:.2f}s > {self.tf_max_age_s:.2f}s",
                details={"age_s": f"{age:.2f}"},
            )
        return None

    def _check_mbf_servers(self, *, mission_running: bool) -> Optional[HealthResult]:
        wait_d = rospy.Duration(max(0.0, float(self.action_server_wait_s)))
        try:
            ok_mb = bool(self._mb.wait_for_server(wait_d))
        except Exception:
            ok_mb = False
        try:
            ok_exe = bool(self._exe.wait_for_server(wait_d))
        except Exception:
            ok_exe = False
        if ok_mb and ok_exe:
            return None
        lvl = "ERROR" if mission_running else "WARN"
        msg = f"mbf servers not ready: move_base={int(ok_mb)} exe_path={int(ok_exe)}"
        return HealthResult(level=lvl, code="MBF_SERVER_DOWN", msg=msg, details={"move_base": str(ok_mb), "exe_path": str(ok_exe)})

    def _check_pose_stall(self, *, now: float, mission_running: bool, exec_state: str) -> Optional[HealthResult]:
        if not (mission_running and self.pose_stall_enable and _looks_running(exec_state)):
            self._last_pose_xyyaw = None
            self._last_pose_move_ts = 0.0
            return None

        # motion stall should be based on odom->base by default (avoid AMCL map jitter)
        try:
            ref_frame = str(getattr(self, "pose_stall_frame_id", "") or "").strip() or self.frame_id
            trans = self._tf_buf.lookup_transform(ref_frame, self.base_frame, rospy.Time(0), rospy.Duration(max(0.01, float(self.tf_timeout_s))))
            t = trans.transform.translation
            q = trans.transform.rotation
            x = float(t.x)
            y = float(t.y)
            yaw = float(_yaw_from_quat(q.x, q.y, q.z, q.w))
        except Exception:
            return None

        cur = (x, y, yaw)
        if self._last_pose_xyyaw is None:
            self._last_pose_xyyaw = cur
            self._last_pose_move_ts = now
            return None

        dx = float(cur[0] - self._last_pose_xyyaw[0])
        dy = float(cur[1] - self._last_pose_xyyaw[1])
        dist = math.hypot(dx, dy)
        dyaw = abs(_wrap_pi(float(cur[2] - self._last_pose_xyyaw[2])))

        if dist >= self.pose_stall_min_move_m or dyaw >= self.pose_stall_min_yaw_rad:
            self._last_pose_xyyaw = cur
            self._last_pose_move_ts = now
            return None

        if self._last_pose_move_ts <= 0.0:
            self._last_pose_move_ts = now
            return None

        stall = float(now - self._last_pose_move_ts)
        if stall > self.pose_stall_timeout_s:
            return HealthResult(
                level="ERROR",
                code="ROBOT_NO_MOTION_STALL",
                msg=f"no motion for {stall:.1f}s (dist<{self.pose_stall_min_move_m}m yaw<{self.pose_stall_min_yaw_rad}rad) state={exec_state}",
                details={"stall_s": f"{stall:.1f}", "dist_m": f"{dist:.3f}", "dyaw_rad": f"{dyaw:.3f}", "state": str(exec_state)},
            )
        return None

    def _check_progress_stall(
        self,
        *,
        now: float,
        mission_running: bool,
        exec_state: str,
        last_progress_ts: float,
        progress_0_1: Optional[float],
    ) -> Optional[HealthResult]:
        if not mission_running:
            self._last_prog = None
            self._last_prog_move_ts = 0.0
            return None

        if progress_0_1 is None or last_progress_ts <= 0.0:
            return None

        if (now - last_progress_ts) > max(2.0, self.exec_progress_timeout_s):
            return None

        # Only require progress to move during FOLLOW/RUN* (coverage execution).
        if not _looks_progress_expected(exec_state):
            self._last_prog = float(progress_0_1)
            self._last_prog_move_ts = now
            return None

        p = float(progress_0_1)
        if self._last_prog is None:
            self._last_prog = p
            self._last_prog_move_ts = now
            return None

        if (p - float(self._last_prog)) >= self.progress_stall_min_delta:
            self._last_prog = p
            self._last_prog_move_ts = now
            return None

        if self._last_prog_move_ts <= 0.0:
            self._last_prog_move_ts = now
            return None

        stall_age = float(now - self._last_prog_move_ts)
        if stall_age > self.progress_stall_timeout_s:
            return HealthResult(
                level="ERROR",
                code="EXEC_PROGRESS_STALL",
                msg=f"progress not increasing for {stall_age:.1f}s (delta<{self.progress_stall_min_delta}) state={exec_state}",
                details={"stall_s": f"{stall_age:.1f}", "progress": f"{p:.3f}", "state": str(exec_state)},
            )
        return None

    # ------------ public API ------------
    def evaluate(
        self,
        *,
        now: Optional[float] = None,
        mission_state: str = "",
        phase: str = "",
        exec_state: str = "",
        last_progress_ts: float = 0.0,
        progress_0_1: Optional[float] = None,
    ) -> HealthResult:
        now = float(now if now is not None else time.time())
        ms = (mission_state or "").strip().upper()
        ph = (phase or "").strip().upper()
        mission_running = (ms == "RUNNING") and (not ph.startswith("AUTO_"))

        # priority-ordered fatal checks
        res = self._check_executor_heartbeat(now=now, mission_running=mission_running, last_progress_ts=float(last_progress_ts))
        if res is not None and res.level == "ERROR":
            return res

        res_loc = self._check_localization(now=now, mission_running=mission_running)
        if res_loc is not None and res_loc.level == "ERROR":
            return res_loc

        res_tf = self._check_tf(mission_running=mission_running)
        if res_tf is not None and res_tf.level == "ERROR":
            return res_tf

        res_mbf = self._check_mbf_servers(mission_running=mission_running)
        if res_mbf is not None and res_mbf.level == "ERROR":
            return res_mbf

        res_pose = self._check_pose_stall(now=now, mission_running=mission_running, exec_state=str(exec_state or ""))
        if res_pose is not None and res_pose.level == "ERROR":
            return res_pose

        res_stall = self._check_progress_stall(
            now=now,
            mission_running=mission_running,
            exec_state=str(exec_state or ""),
            last_progress_ts=float(last_progress_ts),
            progress_0_1=progress_0_1,
        )
        if res_stall is not None and res_stall.level == "ERROR":
            return res_stall

        # WARNs (report first warn if any)
        for w in (res_loc, res_tf, res_mbf, res_pose, res_stall):
            if w is not None and w.level == "WARN":
                return w

        return HealthResult(level="OK", code="", msg="")

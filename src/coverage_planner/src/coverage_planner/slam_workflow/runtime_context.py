# -*- coding: utf-8 -*-

"""Runtime context and observation helpers for the formal SLAM backend."""

from __future__ import annotations

import math
import time
from typing import Any, Optional, Tuple

import rospy
import rosservice
from geometry_msgs.msg import PoseStamped, PoseWithCovarianceStamped

from coverage_planner.map_io import compute_occupancy_grid_md5
from coverage_planner.slam_workflow.executor import RuntimeLocalizationSnapshot
from coverage_planner.slam_workflow_semantics import derive_localization_status


def _yaw_to_quat(yaw: float):
    half = 0.5 * float(yaw)
    return (0.0, 0.0, math.sin(half), math.cos(half))


class CartographerRuntimeContext:
    def __init__(self, backend: Any):
        self._backend = backend
        self._map_ts = 0.0
        self._tracked_pose_ts = 0.0
        self._tracked_pose_stable_since_ts = 0.0
        self._tracked_pose_xyyaw = None
        self._map_md5 = ""

    def runtime_param(self, key: str) -> str:
        backend = self._backend
        return backend.runtime_ns + "/" + str(key or "").strip()

    def workflow_runtime_snapshot(self, robot_id: str) -> RuntimeLocalizationSnapshot:
        backend = self._backend
        active = backend._plan_store.get_active_map(robot_id=robot_id) or {}
        current_mode = str(rospy.get_param(self.runtime_param("current_mode"), "") or "").strip()
        localization_state, localization_valid = self.derived_localization_status(
            localization_state=str(rospy.get_param(self.runtime_param("localization_state"), "") or "").strip(),
            localization_valid=bool(rospy.get_param(self.runtime_param("localization_valid"), False)),
            current_mode=current_mode,
        )
        return RuntimeLocalizationSnapshot(
            active_map_name=str(active.get("map_name") or "").strip(),
            active_map_revision_id=str(active.get("revision_id") or "").strip(),
            current_mode=current_mode,
            runtime_map_name=str(rospy.get_param(self.runtime_param("current_map_name"), "") or "").strip(),
            runtime_map_revision_id=str(
                rospy.get_param(self.runtime_param("current_map_revision_id"), "") or ""
            ).strip(),
            localization_state=localization_state,
            localization_valid=localization_valid,
        )

    def to_ros_time(self, stamp_s: float) -> rospy.Time:
        try:
            stamp_val = float(stamp_s or 0.0)
        except Exception:
            stamp_val = 0.0
        if stamp_val <= 0.0:
            return rospy.Time()
        return rospy.Time.from_sec(stamp_val)

    def make_initial_pose_msg(
        self,
        *,
        frame_id: str,
        initial_pose_x: float,
        initial_pose_y: float,
        initial_pose_yaw: float,
    ) -> PoseWithCovarianceStamped:
        backend = self._backend
        msg = PoseWithCovarianceStamped()
        msg.header.stamp = rospy.Time.now()
        msg.header.frame_id = str(frame_id or "map").strip() or "map"
        msg.pose.pose.position.x = float(initial_pose_x)
        msg.pose.pose.position.y = float(initial_pose_y)
        msg.pose.pose.position.z = 0.0
        qx, qy, qz, qw = _yaw_to_quat(float(initial_pose_yaw))
        msg.pose.pose.orientation.x = qx
        msg.pose.pose.orientation.y = qy
        msg.pose.pose.orientation.z = qz
        msg.pose.pose.orientation.w = qw
        cov = [0.0] * 36
        cov[0] = float(backend.initial_pose_cov_xy)
        cov[7] = float(backend.initial_pose_cov_xy)
        cov[35] = float(backend.initial_pose_cov_yaw)
        msg.pose.covariance = cov
        return msg

    def publish_initial_pose(
        self,
        *,
        frame_id: str,
        initial_pose_x: float,
        initial_pose_y: float,
        initial_pose_yaw: float,
    ):
        backend = self._backend
        msg = self.make_initial_pose_msg(
            frame_id=frame_id,
            initial_pose_x=initial_pose_x,
            initial_pose_y=initial_pose_y,
            initial_pose_yaw=initial_pose_yaw,
        )
        backend._initial_pose_pub.publish(msg)
        rospy.sleep(0.1)
        backend._initial_pose_pub.publish(msg)

    def service_available(self, service_name: str, expected_type: str) -> bool:
        try:
            runtime_type = str(rosservice.get_service_type(service_name) or "").strip()
            if not runtime_type:
                return False
            return (not expected_type) or (runtime_type == str(expected_type or "").strip())
        except Exception:
            return False

    def on_map(self, msg):
        self._map_ts = time.time()
        try:
            self._map_md5 = str(compute_occupancy_grid_md5(msg) or "").strip()
        except Exception:
            self._map_md5 = ""

    @staticmethod
    def _quat_to_yaw(x: float, y: float, z: float, w: float) -> float:
        return math.atan2(2.0 * (w * z + x * y), 1.0 - 2.0 * (y * y + z * z))

    @staticmethod
    def _angle_diff(a: float, b: float) -> float:
        diff = float(a) - float(b)
        while diff > math.pi:
            diff -= 2.0 * math.pi
        while diff < -math.pi:
            diff += 2.0 * math.pi
        return diff

    def _tracked_pose_jump_exceeded(self, current_pose, previous_pose) -> bool:
        backend = self._backend
        if current_pose is None or previous_pose is None:
            return False
        dx = float(current_pose[0]) - float(previous_pose[0])
        dy = float(current_pose[1]) - float(previous_pose[1])
        pose_jump = math.hypot(dx, dy)
        yaw_jump = abs(self._angle_diff(float(current_pose[2]), float(previous_pose[2])))
        return bool(
            pose_jump > float(getattr(backend, "localization_stable_max_pose_jump_m", 0.25))
            or yaw_jump > float(getattr(backend, "localization_stable_max_yaw_jump_rad", 0.35))
        )

    def on_tracked_pose(self, msg: Optional[PoseStamped] = None):
        now = time.time()
        pose = None
        if msg is not None:
            try:
                pose = (
                    float(msg.pose.position.x),
                    float(msg.pose.position.y),
                    self._quat_to_yaw(
                        float(msg.pose.orientation.x),
                        float(msg.pose.orientation.y),
                        float(msg.pose.orientation.z),
                        float(msg.pose.orientation.w),
                    ),
                )
            except Exception:
                pose = None
        if self._tracked_pose_xyyaw is None or self._tracked_pose_jump_exceeded(pose, self._tracked_pose_xyyaw):
            self._tracked_pose_stable_since_ts = now
        elif self._tracked_pose_stable_since_ts <= 0.0:
            self._tracked_pose_stable_since_ts = now
        self._tracked_pose_ts = now
        self._tracked_pose_xyyaw = pose

    def reset_runtime_observations(self):
        self._map_ts = 0.0
        self._tracked_pose_ts = 0.0
        self._tracked_pose_stable_since_ts = 0.0
        self._tracked_pose_xyyaw = None
        self._map_md5 = ""

    def runtime_map_md5(self) -> str:
        return str(self._map_md5 or "").strip()

    def runtime_map_revision_id(self) -> str:
        try:
            return str(rospy.get_param(self.runtime_param("current_map_revision_id"), "") or "").strip()
        except Exception:
            return ""

    def tracked_pose_fresh_since(self, *, started_after_ts: float, now: float, freshness_timeout_s: float) -> bool:
        return (
            self._tracked_pose_ts >= float(started_after_ts)
            and (float(now) - self._tracked_pose_ts) <= float(freshness_timeout_s)
        )

    def tracked_pose_age_s(self, *, now: Optional[float] = None) -> float:
        if self._tracked_pose_ts <= 0.0:
            return -1.0
        current_now = time.time() if now is None else float(now)
        return max(0.0, current_now - self._tracked_pose_ts)

    def tracked_pose_stable_since(
        self,
        *,
        started_after_ts: float,
        now: float,
        freshness_timeout_s: float,
        stable_window_s: float,
    ) -> bool:
        if not self.tracked_pose_fresh_since(
            started_after_ts=started_after_ts,
            now=now,
            freshness_timeout_s=freshness_timeout_s,
        ):
            return False
        stable_since_ts = max(float(started_after_ts), float(self._tracked_pose_stable_since_ts or 0.0))
        if stable_since_ts <= 0.0:
            return False
        return (float(now) - stable_since_ts) >= max(0.0, float(stable_window_s))

    def runtime_map_ready_since(self, *, started_after_ts: float) -> bool:
        return self._map_ts >= float(started_after_ts) and bool(self._map_md5)

    def derived_localization_status(
        self,
        *,
        localization_state: str,
        localization_valid: bool,
        current_mode: str = "",
        now: Optional[float] = None,
    ) -> Tuple[str, bool]:
        normalized_mode = str(current_mode or "").strip().lower()
        if normalized_mode == "mapping":
            return "mapping", False
        return derive_localization_status(
            localization_state=localization_state,
            localization_valid=localization_valid,
            tracked_pose_age_s=self.tracked_pose_age_s(now=now),
            degraded_timeout_s=float(getattr(self._backend, "localization_degraded_timeout_s", 0.0)),
        )

    def runtime_map_matches(self, target_md5: str, target_revision_id: str = "") -> bool:
        normalized_revision_id = str(target_revision_id or "").strip()
        if normalized_revision_id:
            runtime_revision_id = self.runtime_map_revision_id()
            if runtime_revision_id:
                return runtime_revision_id == normalized_revision_id
        normalized = str(target_md5 or "").strip()
        return (not normalized) or (self.runtime_map_md5() == normalized)

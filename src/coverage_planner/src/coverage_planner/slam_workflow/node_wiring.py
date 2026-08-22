# -*- coding: utf-8 -*-

"""ROS wiring helpers for thin SLAM API/runtime nodes."""

from __future__ import annotations

import math
import time
from typing import Any

import rospy
from cleanrobot_app_msgs.msg import (
    OdometryState as AppOdometryState,
    SlamJobState as AppSlamJobState,
    SlamState as AppSlamState,
)
from cleanrobot_app_msgs.srv import (
    GetSlamJob as AppGetSlamJob,
    OperateSlamRuntime as AppOperateSlamRuntime,
    GetSlamStatus as AppGetSlamStatus,
    SubmitSlamCommand as AppSubmitSlamCommand,
)
from coverage_msgs.msg import TaskState as TaskStateMsg
from geometry_msgs.msg import PoseStamped, PoseWithCovarianceStamped
from nav_msgs.msg import OccupancyGrid
from std_msgs.msg import String


def _quat_to_yaw(x: float, y: float, z: float, w: float) -> float:
    return math.atan2(2.0 * (w * z + x * y), 1.0 - 2.0 * (y * y + z * z))


def _stamp_to_sec(stamp, *, fallback_s: float) -> float:
    try:
        value = float(stamp.to_sec())
    except Exception:
        try:
            value = float(getattr(stamp, "secs", 0) or 0) + float(getattr(stamp, "nsecs", 0) or 0) / 1e9
        except Exception:
            value = 0.0
    if value <= 0.0:
        return float(fallback_s)
    return value


def _normalize_frame_id(frame_id: str) -> str:
    return str(frame_id or "").strip().lstrip("/")


class SlamApiNodeWiring:
    def __init__(self, backend: Any, rospy_module=rospy):
        self._backend = backend
        self._rospy = rospy_module

    def wire(self):
        backend = self._backend
        rospy_module = self._rospy
        rospy_module.Subscriber(backend.map_topic, OccupancyGrid, self.on_map, queue_size=2)
        rospy_module.Subscriber(backend.tracked_pose_topic, PoseStamped, self.on_tracked_pose, queue_size=20)
        rospy_module.Subscriber(backend.task_state_topic, TaskStateMsg, self.on_task_state, queue_size=10)
        rospy_module.Subscriber(
            backend.odometry_state_topic_name,
            AppOdometryState,
            self.on_odometry_state,
            queue_size=10,
        )
        rospy_module.Subscriber(
            backend.runtime_job_state_topic,
            AppSlamJobState,
            self.on_runtime_job_state,
            queue_size=10,
        )
        backend._state_pub = rospy_module.Publisher(backend.state_topic_name, AppSlamState, queue_size=1, latch=True)
        backend._job_state_pub = rospy_module.Publisher(
            backend.job_state_topic_name,
            AppSlamJobState,
            queue_size=10,
            latch=True,
        )
        backend._app_status_srv = rospy_module.Service(
            backend.app_status_service_name,
            AppGetSlamStatus,
            backend._state_controller.handle_get_status_app,
        )
        backend._app_submit_srv = rospy_module.Service(
            backend.app_submit_command_service_name,
            AppSubmitSlamCommand,
            backend._submit_controller.handle_submit_command_app,
        )
        backend._app_get_job_srv = rospy_module.Service(
            backend.app_get_job_service_name,
            AppGetSlamJob,
            backend._submit_controller.handle_get_job_app,
        )
        rospy_module.Timer(rospy_module.Duration(1.0 / backend.state_publish_hz), self.publish_state)
        rospy_module.loginfo(
            "[slam_api_service] ready state=%s app_status=%s app_submit=%s app_get_job=%s job_state=%s",
            backend.state_topic_name,
            backend.app_status_service_name,
            backend.app_submit_command_service_name,
            backend.app_get_job_service_name,
            backend.job_state_topic_name,
        )

    def on_map(self, _msg: OccupancyGrid):
        self._backend._map_ts = time.time()

    def on_tracked_pose(self, msg: PoseStamped):
        backend = self._backend
        now = time.time()
        backend._tracked_pose_ts = now
        backend._tracked_pose_frame = _normalize_frame_id(getattr(msg.header, "frame_id", ""))
        backend._tracked_pose_stamp_s = _stamp_to_sec(getattr(msg.header, "stamp", None), fallback_s=now)
        backend._tracked_pose_source = "topic:%s" % str(getattr(backend, "tracked_pose_topic", "/tracked_pose") or "")
        try:
            backend._tracked_pose_xyyaw = (
                float(msg.pose.position.x),
                float(msg.pose.position.y),
                _quat_to_yaw(
                    float(msg.pose.orientation.x),
                    float(msg.pose.orientation.y),
                    float(msg.pose.orientation.z),
                    float(msg.pose.orientation.w),
                ),
            )
        except Exception:
            backend._tracked_pose_xyyaw = None

    def on_task_state(self, msg: TaskStateMsg):
        backend = self._backend
        backend._task_state_msg = msg
        backend._task_state_ts = time.time()

    def on_odometry_state(self, msg: AppOdometryState):
        backend = self._backend
        backend._odometry_state_msg = msg
        backend._odometry_state_ts = time.time()

    def on_runtime_job_state(self, msg: AppSlamJobState):
        self._backend._runtime_state.cache_job_state(msg)

    def publish_state(self, _event):
        backend = self._backend
        try:
            backend._state_pub.publish(
                backend._state_controller.build_state(robot_id=backend.robot_id, refresh_map_identity=False)
            )
        except Exception as exc:
            self._rospy.logwarn_throttle(5.0, "[slam_api_service] publish state failed: %s", str(exc))


class SlamRuntimeNodeWiring:
    def __init__(self, backend: Any, rospy_module=rospy):
        self._backend = backend
        self._rospy = rospy_module

    def wire(self):
        backend = self._backend
        rospy_module = self._rospy
        # A runtime-manager restart invalidates every paused mapping checkpoint.
        # Clear the session token before exposing any operation service so a stale
        # checkpoint can never attach to an unproven mapping process.
        rospy_module.set_param(
            backend._runtime_context.runtime_param("mapping_session_id"),
            "",
        )
        backend._audit_event_topic_name = str(
            rospy_module.get_param(
                "~slam_audit_event_topic_name",
                "/clean_robot_server/slam_audit_events",
            )
            or "/clean_robot_server/slam_audit_events"
        ).strip()
        backend._audit_event_pub = rospy_module.Publisher(
            backend._audit_event_topic_name,
            String,
            queue_size=100,
            latch=False,
        )
        rospy_module.Subscriber(backend.map_topic, OccupancyGrid, self.on_map, queue_size=2)
        rospy_module.Subscriber(backend.tracked_pose_topic, PoseStamped, self.on_tracked_pose, queue_size=20)
        backend._initial_pose_pub = rospy_module.Publisher(
            backend.initial_pose_topic,
            PoseWithCovarianceStamped,
            queue_size=1,
        )
        backend._job_state_pub = rospy_module.Publisher(
            backend.job_state_topic_name,
            AppSlamJobState,
            queue_size=10,
            latch=True,
        )
        backend._app_operate_srv = rospy_module.Service(
            backend.app_service_name,
            AppOperateSlamRuntime,
            backend._service_api.handle_operate_app,
        )
        backend._app_submit_job_srv = rospy_module.Service(
            backend.app_submit_job_service_name,
            AppSubmitSlamCommand,
            backend._service_api.handle_submit_job_app,
        )
        backend._app_get_job_srv = rospy_module.Service(
            backend.app_get_job_service_name,
            AppGetSlamJob,
            backend._service_api.handle_get_job_app,
        )
        backend._job_state.restore_jobs_from_store()
        if getattr(backend, "_runtime_adapter", None) is not None:
            backend._runtime_adapter.reconcile_pending_map_switch(robot_id=backend.robot_id)
        rospy_module.loginfo(
            "[slam_runtime_manager] ready app_operate=%s app_submit_job=%s app_get_job=%s job_state=%s audit=%s runtime_ns=%s map_topic=%s tracked_pose_topic=%s",
            backend.app_service_name,
            backend.app_submit_job_service_name,
            backend.app_get_job_service_name,
            backend.job_state_topic_name,
            backend._audit_event_topic_name,
            backend.runtime_ns,
            backend.map_topic,
            backend.tracked_pose_topic,
        )

    def on_map(self, msg: OccupancyGrid):
        self._backend._runtime_context.on_map(msg)

    def on_tracked_pose(self, msg: PoseStamped):
        self._backend._runtime_context.on_tracked_pose(msg)

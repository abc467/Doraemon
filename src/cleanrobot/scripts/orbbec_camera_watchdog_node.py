#!/usr/bin/env python3

"""Fail-closed health supervisor for the three commissioned Orbbec cameras."""

import collections
import os
import signal
import subprocess
import threading
import time

import rospy
from cleanrobot_app_msgs.srv import ExeTask, ExeTaskRequest
from coverage_msgs.msg import TaskState
from diagnostic_msgs.msg import DiagnosticArray, DiagnosticStatus, KeyValue
from nav_msgs.msg import Odometry
from sensor_msgs.msg import Image, PointCloud2
from std_msgs.msg import Bool, String


class OrbbecCameraWatchdog:
    def __init__(self):
        self._stale_timeout = max(1.0, float(rospy.get_param("~stale_timeout", 3.0)))
        self._startup_grace = max(5.0, float(rospy.get_param("~startup_grace", 20.0)))
        self._cooldown = max(10.0, float(rospy.get_param("~recovery_cooldown", 60.0)))
        self._recovery_timeout = max(30.0, float(rospy.get_param("~recovery_timeout", 150.0)))
        self._recovery_script = rospy.get_param("~recovery_script", "")
        self._required_bad_checks = max(2, int(rospy.get_param("~required_bad_checks", 3)))
        self._pause_and_recover_on_stale = bool(
            rospy.get_param("~pause_and_recover_on_stale", True)
        )
        self._task_state_stale_timeout = max(
            1.0, float(rospy.get_param("~task_state_stale_timeout", 3.0))
        )

        self._topics = {
            "/gemini_cf/depth/image_raw": Image,
            "/gemini_cf/depth/points": PointCloud2,
            "/gemini_nj/depth/image_raw": Image,
            "/gemini_nj/depth/points": PointCloud2,
            "/gemini_front/depth/image_raw": Image,
            "/gemini_front/depth/points": PointCloud2,
        }
        self._lock = threading.RLock()
        self._last_seen = {topic: 0.0 for topic in self._topics}
        self._bad_checks = 0
        self._recovery_active = False
        self._last_recovery = -1e9
        self._attempts = collections.deque()
        self._grace_until = time.monotonic() + self._startup_grace
        self._odom_stamp = 0.0
        self._odom_linear = 0.0
        self._odom_angular = 0.0
        self._task_state_stamp = 0.0
        self._task_mission_state = ""
        self._task_phase = ""
        self._task_public_state = ""
        self._task_run_id = ""
        self._task_job_id = ""

        self._health_pub = rospy.Publisher("/depth_cameras/healthy", Bool, queue_size=1, latch=True)
        self._diag_pub = rospy.Publisher("/diagnostics", DiagnosticArray, queue_size=5)
        self._executor_cmd_pub = rospy.Publisher("/coverage_executor/cmd", String, queue_size=2)
        self._subs = [
            rospy.Subscriber(topic, msg_type, self._on_frame, callback_args=topic, queue_size=1)
            for topic, msg_type in self._topics.items()
        ]
        self._odom_sub = rospy.Subscriber("/odom", Odometry, self._on_odom, queue_size=1)
        self._task_state_sub = rospy.Subscriber(
            "/task_state", TaskState, self._on_task_state, queue_size=1
        )
        self._timer = rospy.Timer(rospy.Duration(0.5), self._on_timer)
        self._publish(False, list(self._topics), "startup grace")

    @staticmethod
    def _frame_has_content(msg):
        if isinstance(msg, PointCloud2):
            points = int(msg.width) * int(msg.height)
            return (
                points > 0
                and int(msg.point_step) > 0
                and len(msg.data) >= points * int(msg.point_step)
            )
        if isinstance(msg, Image):
            return int(msg.width) > 0 and int(msg.height) > 0 and len(msg.data) > 0
        return False

    def _on_frame(self, msg, topic):
        if not self._frame_has_content(msg):
            return
        with self._lock:
            self._last_seen[topic] = time.monotonic()

    def _on_odom(self, msg):
        with self._lock:
            self._odom_stamp = time.monotonic()
            self._odom_linear = float(msg.twist.twist.linear.x)
            self._odom_angular = float(msg.twist.twist.angular.z)

    def _on_task_state(self, msg):
        with self._lock:
            self._task_state_stamp = time.monotonic()
            self._task_mission_state = str(msg.mission_state or "").strip().upper()
            self._task_phase = str(msg.phase or "").strip().upper()
            self._task_public_state = str(msg.public_state or "").strip().upper()
            self._task_run_id = str(msg.run_id or "").strip()
            self._task_job_id = str(msg.active_job_id or "").strip()

    def _task_is_definitely_idle(self, now=None):
        current = time.monotonic() if now is None else float(now)
        with self._lock:
            fresh = (
                self._task_state_stamp > 0.0
                and current - self._task_state_stamp <= self._task_state_stale_timeout
            )
            return bool(
                fresh
                and self._task_mission_state == "IDLE"
                and self._task_phase == "IDLE"
                and self._task_public_state == "IDLE"
                and not self._task_run_id
                and not self._task_job_id
            )

    def _stale_topics(self, now):
        return [
            topic
            for topic, stamp in self._last_seen.items()
            if stamp <= 0.0 or now - stamp > self._stale_timeout
        ]

    def _publish(self, healthy, stale, message):
        self._health_pub.publish(Bool(data=healthy))
        status = DiagnosticStatus()
        status.name = "depth_cameras/three_camera_chain"
        status.hardware_id = "orbbec_left_right_front"
        status.level = DiagnosticStatus.OK if healthy else DiagnosticStatus.ERROR
        status.message = message
        status.values = [
            KeyValue(key="stale_timeout_s", value=str(self._stale_timeout)),
            KeyValue(key="stale_topics", value=",".join(stale)),
            KeyValue(key="recovery_active", value=str(self._recovery_active).lower()),
            KeyValue(
                key="pause_and_recover_on_stale",
                value=str(self._pause_and_recover_on_stale).lower(),
            ),
        ]
        msg = DiagnosticArray()
        msg.header.stamp = rospy.Time.now()
        msg.status = [status]
        self._diag_pub.publish(msg)

    def _on_timer(self, _event):
        now = time.monotonic()
        with self._lock:
            stale = self._stale_topics(now)
            if not stale:
                self._bad_checks = 0
                self._publish(True, [], "all raw depth streams fresh")
                return
            if now < self._grace_until:
                self._publish(False, stale, "waiting for camera startup")
                return
            self._bad_checks += 1
            self._publish(False, stale, "raw depth stream stale or empty")
            if self._recovery_active or self._bad_checks < self._required_bad_checks:
                return
            # This policy is deliberately read at runtime so a field test can
            # switch to monitoring-only behavior without restarting the robot.
            # Monitoring stays active and continues publishing unhealthy
            # diagnostics, but it must neither pause the mission nor restart
            # all cameras while the vehicle is moving.
            self._pause_and_recover_on_stale = bool(
                rospy.get_param(
                    "~pause_and_recover_on_stale",
                    self._pause_and_recover_on_stale,
                )
            )
            if not self._pause_and_recover_on_stale:
                rospy.logerr_throttle(
                    10.0,
                    "Depth camera chain stale (%s); monitoring-only mode keeps the task running",
                    ", ".join(stale),
                )
                return
            if now - self._last_recovery < self._cooldown:
                return
            while self._attempts and now - self._attempts[0] > 600.0:
                self._attempts.popleft()
            if len(self._attempts) >= 3:
                rospy.logerr_throttle(10.0, "Orbbec recovery inhibited: 3 attempts in 10 minutes")
                return
            self._recovery_active = True
            self._last_recovery = now
            self._attempts.append(now)
            threading.Thread(target=self._recover, args=(tuple(stale),), daemon=True).start()

    def _pause_motion(self):
        if self._task_is_definitely_idle():
            rospy.logwarn(
                "Depth camera watchdog skipped task pause: task manager confirms idle"
            )
            return
        try:
            rospy.wait_for_service("/coverage_task_manager/app/exe_task_server", timeout=2.0)
            pause = rospy.ServiceProxy("/coverage_task_manager/app/exe_task_server", ExeTask)
            response = pause(command=ExeTaskRequest.PAUSE, task_id=0)
            rospy.logwarn("Depth camera watchdog requested task pause: %s", response.message)
            if response.success:
                return
            # Task state can change between the preflight snapshot and the
            # service call.  A current, explicit idle rejection is
            # authoritative and must not be bypassed with a direct executor
            # pause, otherwise the executor's latched state becomes PAUSE_REQ
            # even though no task exists.
            if self._task_is_definitely_idle() or "pause requires running mission" in str(
                response.message or ""
            ).lower():
                rospy.logwarn(
                    "Depth camera watchdog skipped executor pause fallback: no running mission"
                )
                return
        except Exception as exc:
            rospy.logerr("Task pause service unavailable during camera failure: %s", exc)
        # The executor command is a bounded fallback. Recovery never resumes it.
        self._executor_cmd_pub.publish(String(data="pause"))
        rospy.sleep(0.2)
        self._executor_cmd_pub.publish(String(data="pause"))

    def _wait_until_stopped(self):
        deadline = time.monotonic() + 10.0
        stopped_since = None
        while not rospy.is_shutdown() and time.monotonic() < deadline:
            now = time.monotonic()
            with self._lock:
                fresh = self._odom_stamp > 0.0 and now - self._odom_stamp < 0.5
                stopped = fresh and abs(self._odom_linear) < 0.02 and abs(self._odom_angular) < 0.03
            if stopped:
                if stopped_since is None:
                    stopped_since = now
                elif now - stopped_since >= 1.0:
                    return True
            else:
                stopped_since = None
            rospy.sleep(0.1)
        return False

    def _recover(self, stale):
        rospy.logerr("Depth camera chain stale (%s); pausing task before recovery", ", ".join(stale))
        self._pause_motion()
        result = -1
        try:
            if not self._wait_until_stopped():
                raise RuntimeError("vehicle did not provide a continuous 1s stopped odometry proof")
            if not self._recovery_script or not os.path.isfile(self._recovery_script):
                raise RuntimeError("recovery script is missing: %s" % self._recovery_script)
            proc = subprocess.Popen(
                [self._recovery_script],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                start_new_session=True,
            )
            try:
                output, _ = proc.communicate(timeout=self._recovery_timeout)
            except subprocess.TimeoutExpired:
                os.killpg(proc.pid, signal.SIGTERM)
                try:
                    output, _ = proc.communicate(timeout=5.0)
                except subprocess.TimeoutExpired:
                    os.killpg(proc.pid, signal.SIGKILL)
                    output, _ = proc.communicate()
                raise RuntimeError("camera recovery timed out")
            result = proc.returncode
            for line in (output or "").splitlines():
                rospy.loginfo("[orbbec-recovery] %s", line)
            if result != 0:
                raise RuntimeError("camera recovery exited %d" % result)
            rospy.logwarn("Depth cameras recovered; task remains paused for explicit resume")
        except Exception as exc:
            rospy.logerr("Depth camera recovery failed: %s", exc)
        finally:
            with self._lock:
                self._last_seen = {topic: 0.0 for topic in self._topics}
                self._bad_checks = 0
                self._grace_until = time.monotonic() + self._startup_grace
                self._recovery_active = False


if __name__ == "__main__":
    rospy.init_node("orbbec_camera_watchdog")
    OrbbecCameraWatchdog()
    rospy.spin()

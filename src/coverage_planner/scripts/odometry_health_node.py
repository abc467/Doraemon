#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
from typing import Dict, List, Tuple

import rosnode
import rospy
from cleanrobot_app_msgs.msg import OdometryState as AppOdometryState
from cleanrobot_app_msgs.srv import (
    GetOdometryStatus as AppGetOdometryStatus,
    GetOdometryStatusResponse as AppGetOdometryStatusResponse,
)
from nav_msgs.msg import Odometry
from sensor_msgs.msg import Imu

from coverage_planner.ros_contract import build_contract_report, validate_ros_contract
from coverage_planner.service_mode import publish_contract_param

class OdometryHealthNode:
    def __init__(self):
        self.robot_id = str(rospy.get_param("~robot_id", "local_robot")).strip() or "local_robot"
        self.raw_odom_topic = str(rospy.get_param("~raw_odom_topic", "/odom_raw")).strip() or "/odom_raw"
        self.odom_topic = str(rospy.get_param("~odom_topic", "/odom")).strip() or "/odom"
        self.imu_topic = str(rospy.get_param("~imu_topic", "/imu_corrected")).strip() or "/imu_corrected"
        self.validation_mode = (
            str(rospy.get_param("~validation_mode", "odom_stream")).strip().lower() or "odom_stream"
        )
        self.require_odom_frame_ids = bool(rospy.get_param("~require_odom_frame_ids", True))
        self.emit_auxiliary_diagnostics = bool(rospy.get_param("~emit_auxiliary_diagnostics", False))
        self.state_topic_name = (
            str(rospy.get_param("~state_topic_name", "/clean_robot_server/odometry_state")).strip()
            or "/clean_robot_server/odometry_state"
        )
        self.app_service_name = (
            str(rospy.get_param("~app_service_name", "/clean_robot_server/app/get_odometry_status")).strip()
            or "/clean_robot_server/app/get_odometry_status"
        )
        self.app_contract_param_ns = (
            str(
                rospy.get_param(
                    "~app_contract_param_ns",
                    "/clean_robot_server/contracts/app/get_odometry_status",
                )
            ).strip()
            or "/clean_robot_server/contracts/app/get_odometry_status"
        )
        self.wheel_speed_node_name = (
            str(rospy.get_param("~wheel_speed_node_name", "/wheel_speed_odom")).strip()
            or "/wheel_speed_odom"
        )
        self.imu_preprocess_node_name = (
            str(rospy.get_param("~imu_preprocess_node_name", "/ekf_covariance_override")).strip()
            or "/ekf_covariance_override"
        )
        self.ekf_node_name = (
            str(rospy.get_param("~ekf_node_name", "/wheel_speed_odom_ekf")).strip()
            or "/wheel_speed_odom_ekf"
        )
        self.wheel_speed_fresh_timeout_s = max(
            0.2, float(rospy.get_param("~wheel_speed_fresh_timeout_s", 1.0))
        )
        self.imu_fresh_timeout_s = max(0.2, float(rospy.get_param("~imu_fresh_timeout_s", 1.0)))
        self.odom_fresh_timeout_s = max(0.2, float(rospy.get_param("~odom_fresh_timeout_s", 1.0)))
        self.publish_hz = max(0.2, float(rospy.get_param("~publish_hz", 1.0)))
        self.odom_source = str(rospy.get_param("~odom_source", "odom_stream")).strip() or "odom_stream"

        self._raw_odom_ts = 0.0
        self._imu_ts = 0.0
        self._odom_ts = 0.0
        self._odom_frame_id = ""
        self._odom_child_frame_id = ""

        validate_ros_contract(
            "OdometryState",
            AppOdometryState,
            required_fields=[
                "robot_id",
                "odom_source",
                "odom_topic",
                "raw_odom_topic",
                "imu_topic",
                "validation_mode",
                "connected",
                "odom_stream_ready",
                "frame_id_valid",
                "child_frame_id_valid",
                "wheel_speed_node_ready",
                "imu_preprocess_node_ready",
                "ekf_node_ready",
                "wheel_speed_fresh",
                "imu_fresh",
                "odom_fresh",
                "odom_valid",
                "error_code",
                "message",
                "warnings",
                "stamp",
            ],
        )
        validate_ros_contract(
            "AppGetOdometryStatusRequest",
            AppGetOdometryStatus._request_class,
            required_fields=["robot_id"],
        )
        validate_ros_contract(
            "AppGetOdometryStatusResponse",
            AppGetOdometryStatusResponse,
            required_fields=["success", "message", "state"],
        )
        self._app_contract = build_contract_report(
            service_name=self.app_service_name,
            contract_name="get_odometry_status_app",
            service_cls=AppGetOdometryStatus,
            request_cls=AppGetOdometryStatus._request_class,
            response_cls=AppGetOdometryStatusResponse,
            dependencies={"state": AppOdometryState},
            features=["structured_odometry_state", "generic_odom_stream_health", "cleanrobot_app_msgs_parallel"],
        )
        publish_contract_param(rospy, self.app_contract_param_ns, self._app_contract, enabled=True)

        if self.raw_odom_topic:
            rospy.Subscriber(self.raw_odom_topic, Odometry, self._on_raw_odom, queue_size=20)
        if self.imu_topic:
            rospy.Subscriber(self.imu_topic, Imu, self._on_imu, queue_size=50)
        if self.odom_topic:
            rospy.Subscriber(self.odom_topic, Odometry, self._on_odom, queue_size=20)

        self._state_pub = rospy.Publisher(self.state_topic_name, AppOdometryState, queue_size=1, latch=True)
        self._service = None
        self._app_service = rospy.Service(self.app_service_name, AppGetOdometryStatus, self._handle_get_status_app)
        rospy.Timer(rospy.Duration(1.0 / self.publish_hz), self._publish_state)

        rospy.loginfo(
            "[odometry_health] ready topic=%s app_service=%s app_contract=%s raw=%s imu=%s odom=%s",
            self.state_topic_name,
            self.app_service_name,
            self.app_contract_param_ns or "-",
            self.raw_odom_topic,
            self.imu_topic,
            self.odom_topic,
        )

    def _on_raw_odom(self, _msg: Odometry):
        self._raw_odom_ts = time.time()

    def _on_imu(self, _msg: Imu):
        self._imu_ts = time.time()

    def _on_odom(self, _msg: Odometry):
        self._odom_ts = time.time()
        self._odom_frame_id = str(getattr(_msg.header, "frame_id", "") or "").strip()
        self._odom_child_frame_id = str(getattr(_msg, "child_frame_id", "") or "").strip()

    def _node_available(self, node_name: str) -> bool:
        try:
            return str(node_name or "").strip() in set(rosnode.get_node_names())
        except Exception:
            return False

    def _age(self, stamp_s: float) -> float:
        if stamp_s <= 0.0:
            return -1.0
        return float(max(0.0, time.time() - stamp_s))

    @staticmethod
    def _display_name(name: str) -> str:
        value = str(name or "").strip()
        return value or "<unknown>"

    def _fresh(self, stamp_s: float, timeout_s: float) -> Tuple[bool, float]:
        age_s = self._age(stamp_s)
        return bool(stamp_s > 0.0 and age_s <= timeout_s), float(age_s)

    @staticmethod
    def _configured(value: str) -> bool:
        return bool(str(value or "").strip())

    def _optional_node_available(self, node_name: str) -> bool:
        if not self._configured(node_name):
            return True
        return self._node_available(node_name)

    def _optional_fresh(self, stamp_s: float, timeout_s: float, *, configured: bool) -> Tuple[bool, float]:
        if not configured:
            return True, -1.0
        return self._fresh(stamp_s, timeout_s)

    def _build_state(self, robot_id: str) -> AppOdometryState:
        raw_odom_configured = self._configured(self.raw_odom_topic)
        imu_configured = self._configured(self.imu_topic)
        wheel_speed_fresh, wheel_speed_age_s = self._optional_fresh(
            self._raw_odom_ts,
            self.wheel_speed_fresh_timeout_s,
            configured=raw_odom_configured,
        )
        imu_fresh, imu_age_s = self._optional_fresh(
            self._imu_ts,
            self.imu_fresh_timeout_s,
            configured=imu_configured,
        )
        odom_fresh, odom_age_s = self._fresh(self._odom_ts, self.odom_fresh_timeout_s)

        wheel_speed_node_ready = self._optional_node_available(self.wheel_speed_node_name)
        imu_preprocess_node_ready = self._optional_node_available(self.imu_preprocess_node_name)
        ekf_node_ready = self._optional_node_available(self.ekf_node_name)

        frame_id_valid = bool(self._odom_frame_id)
        child_frame_id_valid = bool(self._odom_child_frame_id)

        odom_stream_ready = bool(odom_fresh)
        connected = bool(odom_stream_ready)
        if self.validation_mode == "strict_chain":
            odom_valid = bool(
                wheel_speed_node_ready
                and imu_preprocess_node_ready
                and ekf_node_ready
                and wheel_speed_fresh
                and imu_fresh
                and odom_fresh
                and ((not self.require_odom_frame_ids) or (frame_id_valid and child_frame_id_valid))
            )
        else:
            odom_valid = bool(
                odom_fresh and ((not self.require_odom_frame_ids) or (frame_id_valid and child_frame_id_valid))
            )

        warnings: List[str] = []
        error_code = ""
        message = "ok"

        if not odom_fresh:
            error_code = "odom_stale"
            message = "%s is stale or missing" % self._display_name(self.odom_topic)
        elif self.require_odom_frame_ids and not frame_id_valid:
            error_code = "odom_frame_missing"
            message = "%s frame_id is missing" % self._display_name(self.odom_topic)
        elif self.require_odom_frame_ids and not child_frame_id_valid:
            error_code = "odom_child_frame_missing"
            message = "%s child_frame_id is missing" % self._display_name(self.odom_topic)
        elif self.validation_mode == "strict_chain":
            if not wheel_speed_node_ready:
                error_code = "wheel_speed_node_missing"
                message = "%s is not running" % self._display_name(self.wheel_speed_node_name)
            elif not wheel_speed_fresh:
                error_code = "wheel_speed_stale"
                message = "%s is stale or missing" % self._display_name(self.raw_odom_topic)
            elif not imu_preprocess_node_ready:
                error_code = "imu_preprocess_missing"
                message = "%s is not running" % self._display_name(self.imu_preprocess_node_name)
            elif not imu_fresh:
                error_code = "imu_stale"
                message = "%s is stale or missing" % self._display_name(self.imu_topic)
            elif not ekf_node_ready:
                error_code = "ekf_node_missing"
                message = "%s is not running" % self._display_name(self.ekf_node_name)

        if not odom_fresh:
            warnings.append("%s stale or missing" % self._display_name(self.odom_topic))
        if self.require_odom_frame_ids and not frame_id_valid:
            warnings.append("%s frame_id missing" % self._display_name(self.odom_topic))
        if self.require_odom_frame_ids and not child_frame_id_valid:
            warnings.append("%s child_frame_id missing" % self._display_name(self.odom_topic))
        if self.emit_auxiliary_diagnostics:
            if raw_odom_configured and not wheel_speed_node_ready:
                warnings.append("%s missing" % self._display_name(self.wheel_speed_node_name))
            if raw_odom_configured and not wheel_speed_fresh:
                warnings.append("%s stale or missing" % self._display_name(self.raw_odom_topic))
            if imu_configured and not imu_preprocess_node_ready:
                warnings.append("%s missing" % self._display_name(self.imu_preprocess_node_name))
            if imu_configured and not imu_fresh:
                warnings.append("%s stale or missing" % self._display_name(self.imu_topic))
            if self._configured(self.ekf_node_name) and not ekf_node_ready:
                warnings.append("%s missing" % self._display_name(self.ekf_node_name))

        msg = AppOdometryState()
        msg.robot_id = str(robot_id or self.robot_id)
        msg.odom_source = self.odom_source
        msg.odom_topic = self.odom_topic
        msg.raw_odom_topic = self.raw_odom_topic
        msg.imu_topic = self.imu_topic
        msg.validation_mode = str(self.validation_mode or "odom_stream")
        msg.connected = bool(connected)
        msg.odom_stream_ready = bool(odom_stream_ready)
        msg.frame_id_valid = bool(frame_id_valid)
        msg.child_frame_id_valid = bool(child_frame_id_valid)
        msg.wheel_speed_node_ready = bool(wheel_speed_node_ready)
        msg.imu_preprocess_node_ready = bool(imu_preprocess_node_ready)
        msg.ekf_node_ready = bool(ekf_node_ready)
        msg.wheel_speed_fresh = bool(wheel_speed_fresh)
        msg.imu_fresh = bool(imu_fresh)
        msg.odom_fresh = bool(odom_fresh)
        msg.odom_valid = bool(odom_valid)
        msg.wheel_speed_age_s = float(wheel_speed_age_s)
        msg.imu_age_s = float(imu_age_s)
        msg.odom_age_s = float(odom_age_s)
        msg.error_code = str(error_code or "")
        msg.message = str(message or "")
        msg.warnings = list(warnings)
        msg.stamp = rospy.Time.now()
        return msg

    def _publish_state(self, _event):
        self._state_pub.publish(self._build_state(self.robot_id))

    def _handle_get_status_app(self, req):
        robot_id = str(req.robot_id or self.robot_id).strip() or self.robot_id
        return AppGetOdometryStatusResponse(
            success=True,
            message="ok",
            state=self._build_state(robot_id),
        )


def main():
    rospy.init_node("odometry_health", anonymous=False)
    OdometryHealthNode()
    rospy.spin()


if __name__ == "__main__":
    main()

# -*- coding: utf-8 -*-
import math
import rospy
from geometry_msgs.msg import PoseStamped
from nav_msgs.msg import Path


def yaw_to_quat(yaw: float):
    half = 0.5 * float(yaw)
    return (0.0, 0.0, math.sin(half), math.cos(half))


def make_pose(frame_id: str, x: float, y: float, yaw: float, stamp=None) -> PoseStamped:
    ps = PoseStamped()
    ps.header.frame_id = frame_id
    ps.header.stamp = stamp if stamp is not None else rospy.Time.now()
    ps.pose.position.x = float(x)
    ps.pose.position.y = float(y)
    ps.pose.position.z = 0.0
    qx, qy, qz, qw = yaw_to_quat(yaw)
    ps.pose.orientation.x = qx
    ps.pose.orientation.y = qy
    ps.pose.orientation.z = qz
    ps.pose.orientation.w = qw
    return ps


def _quat_dot(q0, q1) -> float:
    return q0.x*q1.x + q0.y*q1.y + q0.z*q1.z + q0.w*q1.w


def make_path(frame_id: str, path_xyyaw, stamp=None) -> Path:
    msg = Path()
    msg.header.frame_id = frame_id
    msg.header.stamp = stamp if stamp is not None else rospy.Time.now()

    prev_q = None
    for (x, y, yaw) in path_xyyaw:
        ps = make_pose(frame_id, x, y, yaw, stamp=msg.header.stamp)

        # 保证 quaternion 符号连续：如果与上一个点点积为负，则翻转符号
        if prev_q is not None and _quat_dot(prev_q, ps.pose.orientation) < 0.0:
            ps.pose.orientation.x *= -1.0
            ps.pose.orientation.y *= -1.0
            ps.pose.orientation.z *= -1.0
            ps.pose.orientation.w *= -1.0

        prev_q = ps.pose.orientation
        msg.poses.append(ps)

    return msg
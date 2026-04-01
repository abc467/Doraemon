# coverage_planner_ros/ros_viz_publisher.py
# -*- coding: utf-8 -*-

import math
from typing import List, Tuple, Optional, Dict, Any

import rospy
from std_msgs.msg import ColorRGBA
from geometry_msgs.msg import Point as ROSPoint
from visualization_msgs.msg import Marker, MarkerArray


XY = Tuple[float, float]
XYZ = Tuple[float, float, float]


def to_ros_point(x, y, z=0.0):
    p = ROSPoint()
    p.x = float(x)
    p.y = float(y)
    p.z = float(z)
    return p


def yaw_to_quat(yaw):
    half = 0.5 * float(yaw)
    return (0.0, 0.0, math.sin(half), math.cos(half))


def make_deleteall(frame_id, ns, mid):
    m = Marker()
    m.header.frame_id = frame_id
    m.header.stamp = rospy.Time.now()
    m.ns = ns
    m.id = int(mid)
    m.action = Marker.DELETEALL if hasattr(Marker, "DELETEALL") else 3
    return m


def make_line_strip(frame_id, ns, mid, pts_xyz: List[XYZ], rgba, width):
    m = Marker()
    m.header.frame_id = frame_id
    m.header.stamp = rospy.Time.now()
    m.ns = ns
    m.id = int(mid)
    m.type = Marker.LINE_STRIP
    m.action = Marker.ADD
    m.pose.orientation.w = 1.0
    m.scale.x = float(width)
    m.color = ColorRGBA(*rgba)
    m.lifetime = rospy.Duration(0.0)
    m.points = [to_ros_point(x, y, z) for (x, y, z) in pts_xyz]
    return m


def make_line_list(frame_id, ns, mid, segs, rgba, width):
    m = Marker()
    m.header.frame_id = frame_id
    m.header.stamp = rospy.Time.now()
    m.ns = ns
    m.id = int(mid)
    m.type = Marker.LINE_LIST
    m.action = Marker.ADD
    m.pose.orientation.w = 1.0
    m.scale.x = float(width)
    m.color = ColorRGBA(*rgba)
    m.lifetime = rospy.Duration(0.0)
    pts = []
    for (a, b) in segs:
        pts.append(to_ros_point(a[0], a[1], a[2]))
        pts.append(to_ros_point(b[0], b[1], b[2]))
    m.points = pts
    return m


def make_sphere(frame_id, ns, mid, x, y, z, rgba, scale):
    m = Marker()
    m.header.frame_id = frame_id
    m.header.stamp = rospy.Time.now()
    m.ns = ns
    m.id = int(mid)
    m.type = Marker.SPHERE
    m.action = Marker.ADD
    m.pose.position.x = float(x)
    m.pose.position.y = float(y)
    m.pose.position.z = float(z)
    m.pose.orientation.w = 1.0
    m.scale.x = float(scale)
    m.scale.y = float(scale)
    m.scale.z = float(scale)
    m.color = ColorRGBA(*rgba)
    m.lifetime = rospy.Duration(0.0)
    return m


def make_arrow(frame_id, ns, mid, x, y, z, yaw, rgba, shaft_len=0.8, shaft_d=0.08, head_d=0.16):
    m = Marker()
    m.header.frame_id = frame_id
    m.header.stamp = rospy.Time.now()
    m.ns = ns
    m.id = int(mid)
    m.type = Marker.ARROW
    m.action = Marker.ADD
    m.pose.position.x = float(x)
    m.pose.position.y = float(y)
    m.pose.position.z = float(z)
    qx, qy, qz, qw = yaw_to_quat(yaw)
    m.pose.orientation.x = qx
    m.pose.orientation.y = qy
    m.pose.orientation.z = qz
    m.pose.orientation.w = qw
    m.scale.x = float(shaft_len)
    m.scale.y = float(shaft_d)
    m.scale.z = float(head_d)
    m.color = ColorRGBA(*rgba)
    m.lifetime = rospy.Duration(0.0)
    return m


class RosVizPublisher:
    """
    Publish exactly the same marker sets as your script:
      - <ns>/swath_markers      orange
      - <ns>/snake_markers      green
      - <ns>/edge_markers       cyan + magenta closecheck
      - <ns>/connect_markers    yellow
      - <ns>/entryexit_markers  red/blue
      - <ns>/keypts_markers     spheres
    All latched.
    """

    def __init__(self, topic_ns: str = "/f2c", frame_id: str = "map", line_w: float = 0.05):
        self.ns = topic_ns.rstrip("/")
        self.frame_id = frame_id
        self.line_w = float(line_w)

        self.swath_pub = rospy.Publisher(self.ns + "/swath_markers", MarkerArray, queue_size=1, latch=True)
        self.snake_pub = rospy.Publisher(self.ns + "/snake_markers", MarkerArray, queue_size=1, latch=True)
        self.edge_pub  = rospy.Publisher(self.ns + "/edge_markers", MarkerArray, queue_size=1, latch=True)
        self.conn_pub  = rospy.Publisher(self.ns + "/connect_markers", MarkerArray, queue_size=1, latch=True)
        self.ee_pub    = rospy.Publisher(self.ns + "/entryexit_markers", MarkerArray, queue_size=1, latch=True)
        self.kp_pub    = rospy.Publisher(self.ns + "/keypts_markers", MarkerArray, queue_size=1, latch=True)

    def publish_plan_debug(self, plan_result, viz_step_m: float = 0.05):
        """
        plan_result should contain blocks[*].debug with:
          swath_segs_xyz, snake_pts_xy, edge_loop_xy, conn1_xy, conn2_xy, keypts, entry_xy, exit_xy
        """
        swath_arr = MarkerArray(); swath_arr.markers.append(make_deleteall(self.frame_id, "swath_clear", 0))
        snake_arr = MarkerArray(); snake_arr.markers.append(make_deleteall(self.frame_id, "snake_clear", 0))
        edge_arr  = MarkerArray(); edge_arr.markers.append(make_deleteall(self.frame_id, "edge_clear", 0))
        conn_arr  = MarkerArray(); conn_arr.markers.append(make_deleteall(self.frame_id, "conn_clear", 0))
        ee_arr    = MarkerArray(); ee_arr.markers.append(make_deleteall(self.frame_id, "ee_clear", 0))
        kp_arr    = MarkerArray(); kp_arr.markers.append(make_deleteall(self.frame_id, "kp_clear", 0))

        swath_id = snake_id = edge_id = conn_id = ee_id = kp_id = 1

        for b in (plan_result.blocks or []):
            dbg = getattr(b, "debug", None)
            if dbg is None:
                continue
            bid = getattr(b, "block_id", 0)

            # swaths (orange) as LINE_LIST segments
            segs = getattr(dbg, "swath_segs_xyz", None)
            if segs:
                swath_arr.markers.append(
                    make_line_list(self.frame_id, f"swaths_b{bid}", swath_id, segs,
                                   (1.0, 0.55, 0.0, 1.0), max(0.001, self.line_w * 0.8))
                )
                swath_id += 1

            # snake (green)
            snake_xy = getattr(dbg, "snake_pts_xy", None)
            if snake_xy and len(snake_xy) >= 2:
                snake_arr.markers.append(
                    make_line_strip(
                        self.frame_id, f"snake_b{bid}", snake_id,
                        [(p[0], p[1], 0.0) for p in snake_xy],
                        (0.0, 1.0, 0.0, 1.0),
                        max(0.001, self.line_w)
                    )
                )
                snake_id += 1

            # edge (cyan) + closecheck (magenta)
            edge_xy = getattr(dbg, "edge_loop_xy", None)
            if edge_xy and len(edge_xy) >= 4:
                edge_arr.markers.append(
                    make_line_strip(
                        self.frame_id, f"edge_b{bid}", edge_id,
                        [(p[0], p[1], 0.0) for p in edge_xy],
                        (0.0, 1.0, 1.0, 1.0),
                        max(0.001, self.line_w)
                    )
                )
                edge_id += 1
                edge_arr.markers.append(
                    make_line_strip(
                        self.frame_id, f"edge_closecheck_b{bid}", edge_id,
                        [(edge_xy[-1][0], edge_xy[-1][1], 0.0), (edge_xy[0][0], edge_xy[0][1], 0.0)],
                        (1.0, 0.0, 1.0, 1.0),
                        max(0.001, self.line_w * 0.8)
                    )
                )
                edge_id += 1

            # connect (yellow)
            conn1 = getattr(dbg, "conn1_xy", None)
            if conn1 and len(conn1) >= 2:
                conn_arr.markers.append(
                    make_line_strip(
                        self.frame_id, f"conn1_b{bid}", conn_id,
                        [(p[0], p[1], 0.0) for p in conn1],
                        (1.0, 1.0, 0.0, 1.0),
                        max(0.001, self.line_w * 0.9)
                    )
                )
                conn_id += 1
            conn2 = getattr(dbg, "conn2_xy", None)
            if conn2 and len(conn2) >= 2:
                conn_arr.markers.append(
                    make_line_strip(
                        self.frame_id, f"conn2_b{bid}", conn_id,
                        [(p[0], p[1], 0.0) for p in conn2],
                        (1.0, 1.0, 0.0, 1.0),
                        max(0.001, self.line_w * 0.9)
                    )
                )
                conn_id += 1

            # entry/exit spheres + arrows (red/blue)
            entry_xy = getattr(dbg, "entry_xy", None)
            exit_xy = getattr(dbg, "exit_xy", None)
            entry_yaw = getattr(b, "entry_xyyaw", (0,0,0))[2] if hasattr(b, "entry_xyyaw") else 0.0
            exit_yaw  = getattr(b, "exit_xyyaw", (0,0,0))[2] if hasattr(b, "exit_xyyaw") else 0.0

            if entry_xy:
                ee_arr.markers.append(make_sphere(self.frame_id, f"entry_b{bid}", ee_id, entry_xy[0], entry_xy[1], 0.0, (1.0, 0.2, 0.2, 1.0), 0.18)); ee_id += 1
                ee_arr.markers.append(make_arrow(self.frame_id, f"entry_arrow_b{bid}", ee_id, entry_xy[0], entry_xy[1], 0.0, entry_yaw, (1.0, 0.2, 0.2, 1.0))); ee_id += 1
            if exit_xy:
                ee_arr.markers.append(make_sphere(self.frame_id, f"exit_b{bid}", ee_id, exit_xy[0], exit_xy[1], 0.0, (0.2, 0.4, 1.0, 1.0), 0.18)); ee_id += 1
                ee_arr.markers.append(make_arrow(self.frame_id, f"exit_arrow_b{bid}", ee_id, exit_xy[0], exit_xy[1], 0.0, exit_yaw, (0.2, 0.4, 1.0, 1.0))); ee_id += 1

            # key points
            keypts = getattr(dbg, "keypts", None)
            if keypts:
                if "P_pre" in keypts:
                    x,y = keypts["P_pre"]; kp_arr.markers.append(make_sphere(self.frame_id, f"P_pre_b{bid}", kp_id, x, y, 0.0, (0.95, 0.95, 0.95, 1.0), 0.16)); kp_id += 1
                if "E_start" in keypts:
                    x,y = keypts["E_start"]; kp_arr.markers.append(make_sphere(self.frame_id, f"E_start_b{bid}", kp_id, x, y, 0.0, (0.85, 0.85, 1.0, 1.0), 0.18)); kp_id += 1
                if "e_pre" in keypts:
                    x,y = keypts["e_pre"]; kp_arr.markers.append(make_sphere(self.frame_id, f"e_pre_b{bid}", kp_id, x, y, 0.0, (0.85, 1.0, 0.85, 1.0), 0.16)); kp_id += 1
                if "p_join" in keypts:
                    x,y = keypts["p_join"]; kp_arr.markers.append(make_sphere(self.frame_id, f"p_join_b{bid}", kp_id, x, y, 0.0, (1.0, 0.9, 0.6, 1.0), 0.18)); kp_id += 1

        self.swath_pub.publish(swath_arr)
        self.snake_pub.publish(snake_arr)
        self.edge_pub.publish(edge_arr)
        self.conn_pub.publish(conn_arr)
        self.ee_pub.publish(ee_arr)
        self.kp_pub.publish(kp_arr)

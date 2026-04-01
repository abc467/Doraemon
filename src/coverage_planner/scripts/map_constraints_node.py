#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import threading

import rospy
from geometry_msgs.msg import Point, Point32
from std_msgs.msg import Header
from std_srvs.srv import Trigger, TriggerResponse
from visualization_msgs.msg import Marker, MarkerArray

from coverage_msgs.msg import MapConstraints, Polygon2D, ZoneGeometry
from coverage_planner.constraints import DEFAULT_VIRTUAL_WALL_BUFFER_M, compile_map_constraints
from coverage_planner.map_identity import ensure_map_identity, get_runtime_map_identity
from coverage_planner.plan_store.store import PlanStore


def _polygon2d(points):
    msg = Polygon2D()
    for x, y in points or []:
        pt = Point32()
        pt.x = float(x)
        pt.y = float(y)
        pt.z = 0.0
        msg.points.append(pt)
    return msg


def _zone_geometry(frame_id, region):
    msg = ZoneGeometry()
    msg.frame_id = str(frame_id or "map")
    msg.outer = _polygon2d(region.get("outer") or [])
    msg.holes = [_polygon2d(hole) for hole in (region.get("holes") or [])]
    return msg


def _marker_color(r, g, b, a=1.0):
    c = Marker().color
    c.r = float(r)
    c.g = float(g)
    c.b = float(b)
    c.a = float(a)
    return c


def _line_marker(frame_id, ns, marker_id, points, color, scale=0.05):
    m = Marker()
    m.header = Header(frame_id=str(frame_id or "map"), stamp=rospy.Time.now())
    m.ns = str(ns)
    m.id = int(marker_id)
    m.type = Marker.LINE_STRIP
    m.action = Marker.ADD
    m.pose.orientation.w = 1.0
    m.scale.x = float(scale)
    m.color = color
    for x, y in points or []:
        p = Point()
        p.x = float(x)
        p.y = float(y)
        p.z = 0.03
        m.points.append(p)
    if len(m.points) >= 1:
        m.points.append(m.points[0])
    return m


class MapConstraintsNode:
    def __init__(self):
        legacy_db_path = rospy.get_param("~db_path", "/data/coverage/planning.db")
        self.plan_db_path = rospy.get_param("~plan_db_path", legacy_db_path)
        self.map_topic = str(rospy.get_param("~map_topic", "/map"))
        self.frame_id = str(rospy.get_param("~frame_id", "map"))
        self.publish_hz = max(0.1, float(rospy.get_param("~publish_hz", 1.0)))
        self.publish_markers = bool(rospy.get_param("~publish_markers", True))
        self.constraints_topic = str(rospy.get_param("~constraints_topic", "/map_constraints/current"))
        self.default_virtual_wall_buffer_m = float(
            rospy.get_param("~default_virtual_wall_buffer_m", DEFAULT_VIRTUAL_WALL_BUFFER_M)
        )
        self.auto_map_identity_enable = bool(rospy.get_param("~auto_map_identity_enable", True))
        self.map_identity_timeout_s = float(rospy.get_param("~map_identity_timeout_s", 2.0))

        self.store = PlanStore(self.plan_db_path)
        self._store_lock = threading.RLock()
        self.pub = rospy.Publisher(self.constraints_topic, MapConstraints, queue_size=1, latch=True)
        self.marker_pub = rospy.Publisher("~markers", MarkerArray, queue_size=1, latch=True)
        self.reload_srv = rospy.Service("~reload", Trigger, self._on_reload)

        self._last_key = ("", "", "")
        self._timer = rospy.Timer(rospy.Duration(1.0 / self.publish_hz), self._on_timer)
        self._publish_if_needed(force=True)
        rospy.loginfo("[map_constraints] ready. db=%s topic=%s", self.plan_db_path, self.constraints_topic)

    def _resolve_map_identity(self):
        if self.auto_map_identity_enable:
            map_id, map_md5, _ok = ensure_map_identity(
                map_topic=self.map_topic,
                timeout_s=self.map_identity_timeout_s,
                set_global_params=True,
                set_private_params=True,
            )
        else:
            map_id, map_md5 = get_runtime_map_identity()
        return str(map_id or "").strip(), str(map_md5 or "").strip()

    def _build_messages(self, compiled):
        msg = MapConstraints()
        msg.header = Header(frame_id=self.frame_id, stamp=rospy.Time.now())
        msg.map_id = str(compiled.map_id or "")
        msg.map_md5 = str(compiled.map_md5 or "")
        msg.constraint_version = str(compiled.constraint_version or "")
        msg.no_go_polygons = []
        for area in compiled.no_go_polygons:
            for region in area.get("geometry") or []:
                msg.no_go_polygons.append(_zone_geometry(self.frame_id, region))
        msg.virtual_wall_keepouts = []
        for wall in compiled.virtual_wall_keepouts:
            for region in wall.get("geometry") or []:
                msg.virtual_wall_keepouts.append(_zone_geometry(self.frame_id, region))

        markers = MarkerArray()
        delete_all = Marker()
        delete_all.action = Marker.DELETEALL
        markers.markers.append(delete_all)
        if self.publish_markers:
            marker_id = 0
            no_go_color = _marker_color(0.90, 0.15, 0.15, 1.0)
            wall_color = _marker_color(0.95, 0.55, 0.10, 1.0)
            for area in compiled.no_go_polygons:
                for region in area.get("geometry") or []:
                    markers.markers.append(
                        _line_marker(self.frame_id, "no_go", marker_id, region.get("outer") or [], no_go_color, scale=0.05)
                    )
                    marker_id += 1
                    for hole in region.get("holes") or []:
                        markers.markers.append(
                            _line_marker(self.frame_id, "no_go_hole", marker_id, hole, no_go_color, scale=0.03)
                        )
                        marker_id += 1
            for wall in compiled.virtual_wall_keepouts:
                for region in wall.get("geometry") or []:
                    markers.markers.append(
                        _line_marker(self.frame_id, "virtual_wall", marker_id, region.get("outer") or [], wall_color, scale=0.05)
                    )
                    marker_id += 1
                    for hole in region.get("holes") or []:
                        markers.markers.append(
                            _line_marker(self.frame_id, "virtual_wall_hole", marker_id, hole, wall_color, scale=0.03)
                        )
                        marker_id += 1
        return msg, markers

    def _publish_if_needed(self, force=False):
        map_id, map_md5 = self._resolve_map_identity()
        if not map_id:
            rospy.logwarn_throttle(5.0, "[map_constraints] map_id not ready, skip publish")
            return

        with self._store_lock:
            raw = self.store.load_map_constraints(map_id=map_id, map_md5_hint=map_md5, create_if_missing=False)
        compiled = compile_map_constraints(
            map_id=map_id,
            map_md5=map_md5 or str(raw.get("map_md5") or ""),
            constraint_version=str(raw.get("constraint_version") or ""),
            no_go_areas=raw.get("no_go_areas") or [],
            virtual_walls=raw.get("virtual_walls") or [],
            default_buffer_m=float(self.default_virtual_wall_buffer_m),
        )

        key = (str(compiled.map_id or ""), str(compiled.map_md5 or ""), str(compiled.constraint_version or ""))
        if (not force) and key == self._last_key:
            return

        msg, markers = self._build_messages(compiled)
        self.pub.publish(msg)
        if self.publish_markers:
            self.marker_pub.publish(markers)
        self._last_key = key
        rospy.loginfo(
            "[map_constraints] published map_id=%s constraint_version=%s no_go=%d virtual_wall_keepouts=%d",
            msg.map_id,
            msg.constraint_version,
            len(msg.no_go_polygons),
            len(msg.virtual_wall_keepouts),
        )

    def _on_reload(self, _req):
        try:
            self._publish_if_needed(force=True)
            return TriggerResponse(success=True, message="constraints reloaded")
        except Exception as exc:
            return TriggerResponse(success=False, message=str(exc))

    def _on_timer(self, _evt):
        try:
            self._publish_if_needed(force=False)
        except Exception as exc:
            rospy.logwarn_throttle(2.0, "[map_constraints] periodic publish failed: %s", str(exc))


def main():
    rospy.init_node("map_constraints")
    MapConstraintsNode()
    rospy.spin()


if __name__ == "__main__":
    main()

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
from coverage_planner.constraints import (
    DEFAULT_VIRTUAL_WALL_BUFFER_M,
    compile_map_constraints,
    compile_navigation_map_constraints,
)
from coverage_planner.map_identity import ensure_map_identity, get_runtime_map_identity, get_runtime_map_revision_id
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


def _line_marker(frame_id, ns, marker_id, points, color, scale=0.05, stamp=None, z=0.03):
    m = Marker()
    m.header = Header(
        frame_id=str(frame_id or "map"),
        stamp=stamp if stamp is not None else rospy.Time.now(),
    )
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
        p.z = float(z)
        m.points.append(p)
    if len(m.points) >= 1:
        m.points.append(m.points[0])
    return m


def _validate_distinct_constraint_topics(hard_topic, effective_topic):
    if rospy.resolve_name(str(hard_topic)) == rospy.resolve_name(str(effective_topic)):
        raise ValueError(
            "hard and effective map-constraint topics must be different: %s"
            % hard_topic
        )


class MapConstraintsNode:
    def __init__(self):
        self.plan_db_path = rospy.get_param("~plan_db_path", "/data/coverage/planning.db")
        self.robot_id = str(rospy.get_param("~robot_id", "local_robot"))
        self.map_topic = str(rospy.get_param("~map_topic", "/map"))
        self.frame_id = str(rospy.get_param("~frame_id", "map"))
        self.publish_hz = max(0.1, float(rospy.get_param("~publish_hz", 1.0)))
        self.publish_markers = bool(rospy.get_param("~publish_markers", True))
        self.constraints_topic = str(rospy.get_param("~constraints_topic", "/map_constraints/current"))
        self.effective_constraints_topic = str(
            rospy.get_param("~effective_constraints_topic", "/map_constraints/effective")
        )
        _validate_distinct_constraint_topics(
            self.constraints_topic,
            self.effective_constraints_topic,
        )
        self.default_virtual_wall_buffer_m = float(
            rospy.get_param("~default_virtual_wall_buffer_m", DEFAULT_VIRTUAL_WALL_BUFFER_M)
        )
        # These buffers define the effective Coverage planning geometry only.
        # The navigation-hard view always retains the stored/raw no-go polygon.
        self.planning_default_no_go_buffer_m = float(
            rospy.get_param("~default_no_go_buffer_m", 0.05)
        )
        self.planning_no_go_long_edge_normal_buffer_m = float(
            rospy.get_param("~default_no_go_long_edge_normal_buffer_m", 0.115)
        )
        self.planning_no_go_short_edge_normal_buffer_m = float(
            rospy.get_param("~default_no_go_short_edge_normal_buffer_m", 0.40)
        )
        self.auto_map_identity_enable = bool(rospy.get_param("~auto_map_identity_enable", True))
        self.map_identity_timeout_s = float(rospy.get_param("~map_identity_timeout_s", 2.0))

        self.store = PlanStore(self.plan_db_path)
        self._store_lock = threading.RLock()
        self.pub = rospy.Publisher(self.constraints_topic, MapConstraints, queue_size=1, latch=True)
        self.effective_pub = rospy.Publisher(
            self.effective_constraints_topic,
            MapConstraints,
            queue_size=1,
            latch=True,
        )
        self.marker_pub = rospy.Publisher("~markers", MarkerArray, queue_size=1, latch=True)
        self.reload_srv = rospy.Service("~reload", Trigger, self._on_reload)

        self._last_key = ("", "", "", "")
        self._last_hard_msg = None
        self._last_effective_msg = None
        self._timer = rospy.Timer(rospy.Duration(1.0 / self.publish_hz), self._on_timer)
        try:
            self._publish_if_needed(force=True)
        except Exception as exc:
            self._publish_invalid("initial constraint load failed: %s" % exc)
        rospy.loginfo(
            "[map_constraints] ready. db=%s hard_topic=%s effective_topic=%s",
            self.plan_db_path,
            self.constraints_topic,
            self.effective_constraints_topic,
        )

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

    @staticmethod
    def _set_snapshot_scope(
        msg,
        *,
        valid,
        invalid_reason="",
        map_revision_id="",
        runtime_map_id="",
        runtime_map_md5="",
    ):
        msg.valid = bool(valid)
        msg.invalid_reason = str(invalid_reason or "")
        msg.map_revision_id = str(map_revision_id or "")
        msg.runtime_map_id = str(runtime_map_id or "")
        msg.runtime_map_md5 = str(runtime_map_md5 or "")
        return msg

    def _build_constraint_message(
        self,
        compiled,
        stamp,
        *,
        map_revision_id="",
        runtime_map_id="",
        runtime_map_md5="",
    ):
        msg = MapConstraints()
        msg.header = Header(frame_id=self.frame_id, stamp=stamp)
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
        return self._set_snapshot_scope(
            msg,
            valid=True,
            map_revision_id=map_revision_id,
            runtime_map_id=runtime_map_id,
            runtime_map_md5=runtime_map_md5,
        )

    def _build_tombstone(
        self,
        stamp,
        reason,
        *,
        map_revision_id="",
        runtime_map_id="",
        runtime_map_md5="",
    ):
        msg = MapConstraints()
        msg.header = Header(frame_id=self.frame_id, stamp=stamp)
        msg.map_id = ""
        msg.map_md5 = ""
        msg.constraint_version = ""
        msg.no_go_polygons = []
        msg.virtual_wall_keepouts = []
        return self._set_snapshot_scope(
            msg,
            valid=False,
            invalid_reason=reason,
            map_revision_id=map_revision_id,
            runtime_map_id=runtime_map_id,
            runtime_map_md5=runtime_map_md5,
        )

    def _publish_invalid(
        self,
        reason,
        *,
        map_revision_id="",
        runtime_map_id="",
        runtime_map_md5="",
    ):
        """Replace both latched snapshots; never leave old keepouts authoritative."""
        stamp = rospy.Time.now()
        tombstone = self._build_tombstone(
            stamp,
            reason,
            map_revision_id=map_revision_id,
            runtime_map_id=runtime_map_id,
            runtime_map_md5=runtime_map_md5,
        )
        self.pub.publish(tombstone)
        self.effective_pub.publish(tombstone)
        markers = MarkerArray()
        delete_all = Marker()
        delete_all.action = Marker.DELETEALL
        markers.markers.append(delete_all)
        self.marker_pub.publish(markers)
        self._last_key = (
            "invalid",
            str(reason or ""),
            str(map_revision_id or ""),
            str(runtime_map_id or ""),
            str(runtime_map_md5 or ""),
        )
        self._last_hard_msg = tombstone
        self._last_effective_msg = tombstone
        rospy.logerr_throttle(
            2.0,
            "[map_constraints] published invalid tombstone: %s",
            str(reason or "unknown error"),
        )
        return False

    def _build_markers(self, hard_compiled, effective_compiled, stamp):
        markers = MarkerArray()
        delete_all = Marker()
        delete_all.action = Marker.DELETEALL
        markers.markers.append(delete_all)
        if self.publish_markers:
            hard_color = _marker_color(0.90, 0.10, 0.10, 1.0)
            effective_color = _marker_color(0.15, 0.55, 0.95, 1.0)
            wall_color = _marker_color(0.95, 0.55, 0.10, 1.0)
            marker_id = 0
            for area in hard_compiled.no_go_polygons:
                for region in area.get("geometry") or []:
                    markers.markers.append(
                        _line_marker(
                            self.frame_id,
                            "no_go",
                            marker_id,
                            region.get("outer") or [],
                            hard_color,
                            scale=0.06,
                            stamp=stamp,
                            z=0.04,
                        )
                    )
                    marker_id += 1
                    for hole in region.get("holes") or []:
                        markers.markers.append(
                            _line_marker(
                                self.frame_id,
                                "no_go_hole",
                                marker_id,
                                hole,
                                hard_color,
                                scale=0.04,
                                stamp=stamp,
                                z=0.04,
                            )
                        )
                        marker_id += 1

            marker_id = 0
            for area in effective_compiled.no_go_polygons:
                for region in area.get("geometry") or []:
                    markers.markers.append(
                        _line_marker(
                            self.frame_id,
                            "no_go_effective_planning",
                            marker_id,
                            region.get("outer") or [],
                            effective_color,
                            scale=0.03,
                            stamp=stamp,
                            z=0.06,
                        )
                    )
                    marker_id += 1
                    for hole in region.get("holes") or []:
                        markers.markers.append(
                            _line_marker(
                                self.frame_id,
                                "no_go_effective_planning_hole",
                                marker_id,
                                hole,
                                effective_color,
                                scale=0.02,
                                stamp=stamp,
                                z=0.06,
                            )
                        )
                        marker_id += 1

            marker_id = 0
            for wall in hard_compiled.virtual_wall_keepouts:
                for region in wall.get("geometry") or []:
                    markers.markers.append(
                        _line_marker(
                            self.frame_id,
                            "virtual_wall",
                            marker_id,
                            region.get("outer") or [],
                            wall_color,
                            scale=0.05,
                            stamp=stamp,
                            z=0.05,
                        )
                    )
                    marker_id += 1
                    for hole in region.get("holes") or []:
                        markers.markers.append(
                            _line_marker(
                                self.frame_id,
                                "virtual_wall_hole",
                                marker_id,
                                hole,
                                wall_color,
                                scale=0.03,
                                stamp=stamp,
                                z=0.05,
                            )
                        )
                        marker_id += 1
        return markers

    def _publish_if_needed(self, force=False):
        revision_before = str(
            get_runtime_map_revision_id("/cartographer/runtime") or ""
        ).strip()
        map_id, map_md5 = self._resolve_map_identity()
        runtime_revision_id = str(
            get_runtime_map_revision_id("/cartographer/runtime") or ""
        ).strip()
        if revision_before != runtime_revision_id:
            return self._publish_invalid(
                "runtime map revision changed while resolving identity",
                map_revision_id=runtime_revision_id,
                runtime_map_id=map_id,
                runtime_map_md5=map_md5,
            )
        if not map_id:
            return self._publish_invalid(
                "runtime map identity is not ready",
                map_revision_id=runtime_revision_id,
                runtime_map_md5=map_md5,
            )

        with self._store_lock:
            active_asset = self.store.get_active_map(robot_id=self.robot_id) or {}
            active_revision_id = str(active_asset.get("revision_id") or "").strip()
            active_map_id = str(active_asset.get("map_id") or "").strip()
            active_map_md5 = str(active_asset.get("map_md5") or "").strip()
            if active_revision_id and runtime_revision_id and active_revision_id != runtime_revision_id:
                return self._publish_invalid(
                    "runtime revision=%s mismatches active asset revision=%s"
                    % (runtime_revision_id, active_revision_id),
                    map_revision_id=runtime_revision_id,
                    runtime_map_id=map_id,
                    runtime_map_md5=map_md5,
                )
            if (not runtime_revision_id) and active_map_id and active_map_id != map_id:
                return self._publish_invalid(
                    "runtime map_id=%s mismatches active asset map_id=%s"
                    % (map_id, active_map_id),
                    runtime_map_id=map_id,
                    runtime_map_md5=map_md5,
                )
            if (not runtime_revision_id) and active_map_md5 and map_md5 and active_map_md5 != map_md5:
                return self._publish_invalid(
                    "runtime map_md5=%s mismatches active asset map_md5=%s"
                    % (map_md5, active_map_md5),
                    runtime_map_id=map_id,
                    runtime_map_md5=map_md5,
                )
            raw = self.store.load_map_constraints(
                map_id=active_map_id or map_id,
                map_revision_id=active_revision_id,
                map_md5_hint=active_map_md5 or map_md5,
                create_if_missing=False,
            )
        compiled_map_id = active_map_id or map_id
        compiled_map_md5 = active_map_md5 or map_md5 or str(raw.get("map_md5") or "")
        hard_compiled = compile_navigation_map_constraints(
            map_id=compiled_map_id,
            map_md5=compiled_map_md5,
            constraint_version=str(raw.get("constraint_version") or ""),
            no_go_areas=raw.get("no_go_areas") or [],
            virtual_walls=raw.get("virtual_walls") or [],
            default_virtual_wall_buffer_m=float(self.default_virtual_wall_buffer_m),
        )
        effective_compiled = compile_map_constraints(
            map_id=compiled_map_id,
            map_md5=compiled_map_md5,
            constraint_version=str(raw.get("constraint_version") or ""),
            no_go_areas=raw.get("no_go_areas") or [],
            virtual_walls=raw.get("virtual_walls") or [],
            default_buffer_m=float(self.default_virtual_wall_buffer_m),
            default_no_go_buffer_m=float(self.planning_default_no_go_buffer_m),
            default_no_go_long_edge_normal_buffer_m=float(
                self.planning_no_go_long_edge_normal_buffer_m
            ),
            default_no_go_short_edge_normal_buffer_m=float(
                self.planning_no_go_short_edge_normal_buffer_m
            ),
        )

        key = (
            "valid",
            str(active_revision_id or ""),
            str(runtime_revision_id or ""),
            str(map_id or ""),
            str(map_md5 or ""),
            str(hard_compiled.map_id or ""),
            str(hard_compiled.map_md5 or ""),
            str(hard_compiled.constraint_version or ""),
            float(self.default_virtual_wall_buffer_m),
            float(self.planning_default_no_go_buffer_m),
            float(self.planning_no_go_long_edge_normal_buffer_m),
            float(self.planning_no_go_short_edge_normal_buffer_m),
        )
        stamp = rospy.Time.now()
        if (
            (not force)
            and key == self._last_key
            and self._last_hard_msg is not None
            and self._last_effective_msg is not None
        ):
            # Heartbeat freshness is receipt-time based in the consumer. Reuse
            # the immutable geometry while refreshing the timestamp and latch.
            self._last_hard_msg.header.stamp = stamp
            self._last_effective_msg.header.stamp = stamp
            self.pub.publish(self._last_hard_msg)
            self.effective_pub.publish(self._last_effective_msg)
            return True

        authoritative_revision = active_revision_id or runtime_revision_id
        hard_msg = self._build_constraint_message(
            hard_compiled,
            stamp,
            map_revision_id=authoritative_revision,
            runtime_map_id=map_id,
            runtime_map_md5=map_md5,
        )
        effective_msg = self._build_constraint_message(
            effective_compiled,
            stamp,
            map_revision_id=authoritative_revision,
            runtime_map_id=map_id,
            runtime_map_md5=map_md5,
        )
        markers = self._build_markers(hard_compiled, effective_compiled, stamp)
        self.pub.publish(hard_msg)
        self.effective_pub.publish(effective_msg)
        if self.publish_markers:
            self.marker_pub.publish(markers)
        self._last_key = key
        self._last_hard_msg = hard_msg
        self._last_effective_msg = effective_msg
        rospy.loginfo(
            "[map_constraints] published revision=%s map_id=%s constraint_version=%s "
            "hard_no_go=%d effective_no_go=%d virtual_wall_keepouts=%d "
            "planning_no_go_buffer=%.3f/%.3f/%.3f",
            active_revision_id or "-",
            hard_msg.map_id,
            hard_msg.constraint_version,
            len(hard_msg.no_go_polygons),
            len(effective_msg.no_go_polygons),
            len(hard_msg.virtual_wall_keepouts),
            float(self.planning_default_no_go_buffer_m),
            float(self.planning_no_go_long_edge_normal_buffer_m),
            float(self.planning_no_go_short_edge_normal_buffer_m),
        )
        return True

    def _on_reload(self, _req):
        try:
            published = self._publish_if_needed(force=True)
            if published:
                return TriggerResponse(success=True, message="constraints reloaded")
            return TriggerResponse(
                success=False,
                message="constraints invalid for the currently loaded map",
            )
        except Exception as exc:
            self._publish_invalid("constraint reload failed: %s" % exc)
            return TriggerResponse(success=False, message=str(exc))

    def _on_timer(self, _evt):
        try:
            self._publish_if_needed(force=False)
        except Exception as exc:
            self._publish_invalid("periodic constraint load failed: %s" % exc)
            rospy.logwarn_throttle(2.0, "[map_constraints] periodic publish failed: %s", str(exc))


def main():
    rospy.init_node("map_constraints")
    MapConstraintsNode()
    rospy.spin()


if __name__ == "__main__":
    main()

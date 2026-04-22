#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import math
from typing import Dict, List, Optional, Sequence, Tuple

import actionlib
import rospy
import tf2_ros
from cleanrobot_site_msgs.srv import (
    ConfirmRectCoveragePlan as SiteConfirmRectCoveragePlan,
    ConfirmRectCoveragePlanResponse as SiteConfirmRectCoveragePlanResponse,
)
from geometry_msgs.msg import Point32, PointStamped
from std_srvs.srv import Trigger, TriggerResponse
from tf2_geometry_msgs.tf2_geometry_msgs import do_transform_point
from visualization_msgs.msg import Marker, MarkerArray

from coverage_msgs.msg import PlanCoverageAction, PlanCoverageGoal, Polygon2D, ZoneGeometry
from coverage_planner.constraints import (
    DEFAULT_VIRTUAL_WALL_BUFFER_M,
    compile_map_constraints,
    compile_zone_constraints,
)
from coverage_planner.coverage_planner_core.geom_norm import normalize_polygon
from coverage_planner.coverage_planner_ros.ros_viz_publisher import make_deleteall, make_line_strip, make_sphere
from coverage_planner.map_identity import ensure_map_identity, get_runtime_map_revision_id
from coverage_planner.plan_store.store import PlanStore
from coverage_planner.ros_contract import build_contract_report, validate_ros_contract

try:
    from shapely.geometry import GeometryCollection, Polygon
    from shapely.ops import unary_union

    _HAS_SHAPELY = True
except Exception:
    GeometryCollection = None
    Polygon = None
    unary_union = None
    _HAS_SHAPELY = False


XY = Tuple[float, float]
_CONFIRM_RECT_REQUEST_FIELDS = ["zone_id", "profile_name", "debug_publish_markers"]
_CONFIRM_RECT_RESPONSE_FIELDS = ["success", "message", "plan_id"]


def _clamp_positive(value: float, default: float) -> float:
    try:
        f = float(value)
    except Exception:
        return float(default)
    if not math.isfinite(f) or f <= 0.0:
        return float(default)
    return float(f)


def _line_pts(ring: Sequence[XY], z: float) -> List[Tuple[float, float, float]]:
    return [(float(x), float(y), float(z)) for (x, y) in ring]


def _rect_ring(x0: float, y0: float, x1: float, y1: float) -> List[XY]:
    min_x = min(float(x0), float(x1))
    min_y = min(float(y0), float(y1))
    max_x = max(float(x0), float(x1))
    max_y = max(float(y0), float(y1))
    return [
        (min_x, min_y),
        (max_x, min_y),
        (max_x, max_y),
        (min_x, max_y),
    ]


def _ring_center(ring: Sequence[XY]) -> XY:
    if not ring:
        return (0.0, 0.0)
    return (
        sum(float(p[0]) for p in ring) / float(len(ring)),
        sum(float(p[1]) for p in ring) / float(len(ring)),
    )


class RectZonePlannerNode:
    def __init__(self):
        self.plan_db_path = rospy.get_param("~plan_db_path", "/data/coverage/planning.db")
        self.robot_id = str(rospy.get_param("~robot_id", "local_robot"))
        self.map_frame = str(rospy.get_param("~map_frame", "map")).strip() or "map"
        self.clicked_topic = str(rospy.get_param("~clicked_topic", "/clicked_point")).strip() or "/clicked_point"
        self.preview_z = float(rospy.get_param("~preview_z", 0.05))
        self.preview_line_width = float(rospy.get_param("~preview_line_width", 0.04))
        self.selection_inset_m = max(0.0, float(rospy.get_param("~selection_inset_m", 0.05)))
        self.hole_outset_m = max(0.0, float(rospy.get_param("~hole_outset_m", 0.05)))
        self.min_effective_side_m = _clamp_positive(rospy.get_param("~min_effective_side_m", 0.10), 0.10)
        self.default_plan_profile_name = str(rospy.get_param("~default_plan_profile_name", "cover_standard")).strip() or "cover_standard"
        self.plan_action_name = str(rospy.get_param("~plan_action_name", "/coverage_planner_server/plan_coverage")).strip()
        self.action_connect_timeout_s = _clamp_positive(rospy.get_param("~action_connect_timeout_s", 3.0), 3.0)
        self.plan_timeout_s = _clamp_positive(rospy.get_param("~plan_timeout_s", 120.0), 120.0)
        self.map_topic = str(rospy.get_param("~map_topic", "/map")).strip() or "/map"
        self.map_identity_timeout_s = _clamp_positive(rospy.get_param("~map_identity_timeout_s", 2.0), 2.0)
        self.default_virtual_wall_buffer_m = float(
            rospy.get_param("~default_virtual_wall_buffer_m", DEFAULT_VIRTUAL_WALL_BUFFER_M)
        )
        self.site_confirm_service_name = str(
            rospy.get_param("~site_confirm_service_name", "~site/confirm_rect_plan")
        ).strip() or "~site/confirm_rect_plan"
        self.site_confirm_contract_param_ns = str(
            rospy.get_param(
                "~site_confirm_contract_param_ns",
                "/rect_zone_planner/contracts/site/confirm_rect_plan",
            )
        ).strip() or "/rect_zone_planner/contracts/site/confirm_rect_plan"

        self.store = PlanStore(self.plan_db_path)
        self.tf_buffer = tf2_ros.Buffer(cache_time=rospy.Duration(10.0))
        self.tf_listener = tf2_ros.TransformListener(self.tf_buffer)
        self.plan_client = actionlib.SimpleActionClient(self.plan_action_name, PlanCoverageAction)
        self._site_contract_report = self._prepare_site_contract_report()

        self.first_point: Optional[XY] = None
        self.preview: Optional[Dict[str, object]] = None

        self.marker_pub = rospy.Publisher("~preview_markers", MarkerArray, queue_size=1, latch=True)
        self.click_sub = rospy.Subscriber(self.clicked_topic, PointStamped, self._on_click, queue_size=10)
        self.confirm_srv = None
        self.site_confirm_srv = rospy.Service(
            self.site_confirm_service_name,
            SiteConfirmRectCoveragePlan,
            self._handle_confirm_site,
        )
        self.cancel_srv = rospy.Service("~cancel_rect_plan", Trigger, self._handle_cancel)
        if self.site_confirm_contract_param_ns:
            rospy.set_param(self.site_confirm_contract_param_ns, self._site_contract_report)

        self._publish_markers()
        rospy.loginfo(
            "[rect_zone_planner] ready clicked_topic=%s action=%s db=%s site_confirm=%s site_contract=%s inset=%.3f min_effective=%.3f",
            self.clicked_topic,
            self.plan_action_name,
            self.plan_db_path,
            self.site_confirm_service_name,
            self.site_confirm_contract_param_ns or "-",
            self.selection_inset_m,
            self.min_effective_side_m,
        )

    def _prepare_site_contract_report(self):
        validate_ros_contract(
            "SiteConfirmRectCoveragePlanRequest",
            SiteConfirmRectCoveragePlan._request_class,
            required_fields=_CONFIRM_RECT_REQUEST_FIELDS,
        )
        validate_ros_contract(
            "SiteConfirmRectCoveragePlanResponse",
            SiteConfirmRectCoveragePlanResponse,
            required_fields=_CONFIRM_RECT_RESPONSE_FIELDS,
        )
        return build_contract_report(
            service_name=rospy.resolve_name(self.site_confirm_service_name),
            contract_name="rect_confirm_plan_service_site",
            service_cls=SiteConfirmRectCoveragePlan,
            request_cls=SiteConfirmRectCoveragePlan._request_class,
            response_cls=SiteConfirmRectCoveragePlanResponse,
            dependencies={},
            features=["rviz_rect_plan_confirm", "cleanrobot_site_msgs_canonical"],
        )

    def _handle_cancel(self, _req) -> TriggerResponse:
        self._clear_selection()
        return TriggerResponse(success=True, message="selection cleared")

    def _clear_selection(self):
        self.first_point = None
        self.preview = None
        self._publish_markers()

    def _transform_to_map(self, pt_msg: PointStamped) -> PointStamped:
        frame_id = str(pt_msg.header.frame_id or "").strip()
        if (not frame_id) or frame_id == self.map_frame:
            out = PointStamped()
            out.header = pt_msg.header
            out.header.frame_id = self.map_frame
            out.point = pt_msg.point
            return out

        tf = self.tf_buffer.lookup_transform(
            self.map_frame,
            frame_id,
            pt_msg.header.stamp,
            rospy.Duration(0.2),
        )
        out = PointStamped()
        out.header.frame_id = self.map_frame
        out.header.stamp = pt_msg.header.stamp
        out.point = do_transform_point(pt_msg, tf).point
        return out

    def _preview_message(self) -> str:
        if self.preview:
            return str(self.preview.get("message") or "")
        if self.first_point is not None:
            return "first corner set, click the second corner"
        return "use RViz Publish Point tool to pick two rectangle corners"

    def _preview_summary_lines(self) -> List[str]:
        if self.preview:
            map_name = str(self.preview.get("map_name") or "-")
            raw_w = float(self.preview.get("raw_width_m") or 0.0)
            raw_h = float(self.preview.get("raw_height_m") or 0.0)
            eff_w = float(self.preview.get("effective_width_m") or 0.0)
            eff_h = float(self.preview.get("effective_height_m") or 0.0)
            holes = int(self.preview.get("hole_count") or 0)
            validity = "READY" if bool(self.preview.get("valid")) else "CHECK"
            return [
                "Rect Zone Preview [%s]" % validity,
                "Map: %s" % map_name,
                "Raw: %.2fm x %.2fm" % (raw_w, raw_h),
                "Clean: %.2fm x %.2fm" % (eff_w, eff_h),
                "Holes: %d  outset=%.2fm" % (holes, self.hole_outset_m),
                "Confirm: rosservice call /rect_zone_planner/site/confirm_rect_plan ...",
            ]
        if self.first_point is not None:
            return [
                "Rect Zone Preview [POINT A]",
                "A: (%.2f, %.2f)" % (self.first_point[0], self.first_point[1]),
                "Click point B in RViz",
                "Cancel: rosservice call /rect_zone_planner/cancel_rect_plan",
            ]
        return [
            "Rect Zone Preview [IDLE]",
            "Use RViz Publish Point tool",
            "Click A then B to create a rectangle",
            "Confirm: /rect_zone_planner/site/confirm_rect_plan",
        ]

    def _constraint_holes_for_outer(self, outer: Sequence[XY], *, active_map: Dict[str, object], prec: int = 3) -> List[List[XY]]:
        map_id = str(active_map.get("map_id") or "").strip()
        map_md5 = str(active_map.get("map_md5") or "").strip()
        map_revision_id = str(active_map.get("revision_id") or "").strip()
        if not map_id:
            return []
        raw = self.store.load_map_constraints(
            map_id=map_id,
            map_revision_id=map_revision_id,
            map_md5_hint=map_md5,
            create_if_missing=False,
        )
        compiled = compile_map_constraints(
            map_id=map_id,
            map_md5=str(raw.get("map_md5") or map_md5),
            constraint_version=str(raw.get("constraint_version") or ""),
            no_go_areas=raw.get("no_go_areas") or [],
            virtual_walls=raw.get("virtual_walls") or [],
            default_buffer_m=float(self.default_virtual_wall_buffer_m),
            prec=prec,
        )
        zone = compile_zone_constraints(
            zone_outer=outer,
            zone_holes=[],
            map_constraints=compiled,
            prec=prec,
        )
        return [list(ring or []) for ring in (zone.keepout_snapshot_rings or []) if len(ring or []) >= 3]

    def _expand_holes(self, outer: Sequence[XY], holes: Sequence[Sequence[XY]], *, prec: int = 3) -> List[List[XY]]:
        if (not holes) or (self.hole_outset_m <= 1e-9) or (not _HAS_SHAPELY):
            outer_n, holes_n = normalize_polygon(list(outer or []), [list(r) for r in (holes or [])], prec=prec)
            return [list(h[:-1]) for h in (holes_n or []) if len(h) >= 4]

        try:
            zone_poly = Polygon(list(outer or []))
            if (not zone_poly.is_valid) or zone_poly.is_empty:
                zone_poly = zone_poly.buffer(0.0)
            if zone_poly.is_empty:
                return []

            safe_zone = zone_poly.buffer(-0.001)
            if safe_zone.is_empty:
                safe_zone = zone_poly

            buffered_parts = []
            for ring in holes or []:
                poly = Polygon(list(ring or []))
                if (not poly.is_valid) or poly.is_empty:
                    poly = poly.buffer(0.0)
                if poly.is_empty:
                    continue
                grown = poly.buffer(float(self.hole_outset_m), join_style=2)
                if (not grown.is_valid) or grown.is_empty:
                    grown = grown.buffer(0.0)
                clipped = grown.intersection(safe_zone)
                if not clipped.is_empty:
                    buffered_parts.append(clipped)

            if not buffered_parts:
                return []

            merged = unary_union(buffered_parts) if len(buffered_parts) > 1 else buffered_parts[0]
            polys = []
            if merged.geom_type == "Polygon":
                polys = [merged]
            elif merged.geom_type == "MultiPolygon":
                polys = list(merged.geoms)
            elif merged.geom_type == "GeometryCollection":
                polys = [g for g in merged.geoms if getattr(g, "geom_type", "") == "Polygon" and not g.is_empty]

            out: List[List[XY]] = []
            for poly in polys:
                coords = list(poly.exterior.coords)[:-1]
                if len(coords) < 3:
                    continue
                ring = [(float(x), float(y)) for (x, y) in coords]
                _, holes_n = normalize_polygon(list(outer or []), [ring], prec=prec)
                if holes_n and len(holes_n[0]) >= 4:
                    out.append(list(holes_n[0][:-1]))
            return out
        except Exception as e:
            rospy.logwarn("[rect_zone_planner] expand holes failed, fallback to raw holes: %s", str(e))
            outer_n, holes_n = normalize_polygon(list(outer or []), [list(r) for r in (holes or [])], prec=prec)
            return [list(h[:-1]) for h in (holes_n or []) if len(h) >= 4]

    def _build_preview(self, p0: XY, p1: XY) -> Dict[str, object]:
        raw_outer = _rect_ring(p0[0], p0[1], p1[0], p1[1])
        raw_w = abs(float(p1[0]) - float(p0[0]))
        raw_h = abs(float(p1[1]) - float(p0[1]))
        active_map = self.store.get_active_map(robot_id=self.robot_id) or {}
        preview: Dict[str, object] = {
            "frame_id": self.map_frame,
            "first_point": (float(p0[0]), float(p0[1])),
            "second_point": (float(p1[0]), float(p1[1])),
            "raw_outer": raw_outer,
            "raw_width_m": raw_w,
            "raw_height_m": raw_h,
            "map_name": str(active_map.get("map_name") or ""),
            "map_revision_id": str(active_map.get("revision_id") or ""),
            "valid": False,
            "message": "",
        }

        min_x = min(float(p0[0]), float(p1[0])) + self.selection_inset_m
        min_y = min(float(p0[1]), float(p1[1])) + self.selection_inset_m
        max_x = max(float(p0[0]), float(p1[0])) - self.selection_inset_m
        max_y = max(float(p0[1]), float(p1[1])) - self.selection_inset_m
        eff_w = max_x - min_x
        eff_h = max_y - min_y
        preview["effective_width_m"] = max(0.0, eff_w)
        preview["effective_height_m"] = max(0.0, eff_h)

        if eff_w <= 0.0 or eff_h <= 0.0:
            preview["message"] = "rectangle collapses after inset, pick a larger area"
            return preview
        if eff_w < self.min_effective_side_m or eff_h < self.min_effective_side_m:
            preview["message"] = (
                "effective rectangle too small after inset: %.3fm x %.3fm"
                % (max(0.0, eff_w), max(0.0, eff_h))
            )
            preview["effective_outer"] = _rect_ring(min_x, min_y, max_x, max_y)
            return preview

        outer_n, _ = normalize_polygon(_rect_ring(min_x, min_y, max_x, max_y), [], prec=3)
        effective_outer = [(float(x), float(y)) for (x, y) in outer_n[:-1]]
        preview["effective_outer"] = effective_outer
        preview["geom_holes"] = []
        preview["constraint_version"] = ""
        preview["hole_count"] = 0

        if active_map:
            try:
                raw_holes = self._constraint_holes_for_outer(effective_outer, active_map=active_map, prec=3)
                geom_holes = self._expand_holes(effective_outer, raw_holes, prec=3)
                preview["geom_holes"] = geom_holes
                preview["hole_count"] = int(len(geom_holes))
                map_id = str(active_map.get("map_id") or "").strip()
                map_revision_id = str(active_map.get("revision_id") or "").strip()
                if map_id:
                    preview["constraint_version"] = str(
                        self.store.get_active_constraint_version(map_id, map_revision_id) or ""
                    )
            except Exception as e:
                rospy.logwarn("[rect_zone_planner] failed to derive holes from constraints: %s", str(e))

        preview["valid"] = True
        preview["message"] = (
            "preview ready: raw %.3fm x %.3fm, effective %.3fm x %.3fm, holes=%d"
            % (raw_w, raw_h, eff_w, eff_h, int(preview.get("hole_count") or 0))
        )
        return preview

    def _make_text_marker(self, text: str, x: float, y: float, z: float, rgba) -> Marker:
        m = Marker()
        m.header.frame_id = self.map_frame
        m.header.stamp = rospy.Time.now()
        m.ns = "rect_zone_planner_text"
        m.id = 100
        m.type = Marker.TEXT_VIEW_FACING
        m.action = Marker.ADD
        m.pose.position.x = float(x)
        m.pose.position.y = float(y)
        m.pose.position.z = float(z)
        m.pose.orientation.w = 1.0
        m.scale.z = 0.20
        m.color.r = float(rgba[0])
        m.color.g = float(rgba[1])
        m.color.b = float(rgba[2])
        m.color.a = float(rgba[3])
        m.text = str(text or "")
        return m

    def _make_rect_fill_marker(self, ring: Sequence[XY], *, ns: str, mid: int, rgba, z: float) -> Optional[Marker]:
        if len(ring or []) < 4:
            return None
        xs = [float(p[0]) for p in ring]
        ys = [float(p[1]) for p in ring]
        min_x, max_x = min(xs), max(xs)
        min_y, max_y = min(ys), max(ys)
        sx = max_x - min_x
        sy = max_y - min_y
        if sx <= 0.0 or sy <= 0.0:
            return None
        m = Marker()
        m.header.frame_id = self.map_frame
        m.header.stamp = rospy.Time.now()
        m.ns = ns
        m.id = int(mid)
        m.type = Marker.CUBE
        m.action = Marker.ADD
        m.pose.position.x = 0.5 * (min_x + max_x)
        m.pose.position.y = 0.5 * (min_y + max_y)
        m.pose.position.z = float(z)
        m.pose.orientation.w = 1.0
        m.scale.x = float(sx)
        m.scale.y = float(sy)
        m.scale.z = 0.01
        m.color.r = float(rgba[0])
        m.color.g = float(rgba[1])
        m.color.b = float(rgba[2])
        m.color.a = float(rgba[3])
        return m

    def _publish_markers(self):
        arr = MarkerArray()
        arr.markers.append(make_deleteall(self.map_frame, "rect_zone_planner_clear", 0))

        if self.first_point is not None:
            arr.markers.append(
                make_sphere(
                    self.map_frame,
                    "rect_zone_first_point",
                    1,
                    self.first_point[0],
                    self.first_point[1],
                    self.preview_z,
                    (0.2, 0.7, 1.0, 1.0),
                    0.12,
                )
            )
            arr.markers.append(
                self._make_text_marker(
                    "A",
                    self.first_point[0],
                    self.first_point[1],
                    self.preview_z + 0.15,
                    (0.2, 0.7, 1.0, 1.0),
                )
            )

        text_x = 0.0
        text_y = 0.0
        text_color = (1.0, 1.0, 1.0, 0.95)

        if self.preview:
            raw_outer = list(self.preview.get("raw_outer") or [])
            if raw_outer:
                raw_fill = self._make_rect_fill_marker(
                    raw_outer,
                    ns="rect_zone_raw_fill",
                    mid=20,
                    rgba=(1.0, 0.75, 0.15, 0.10),
                    z=self.preview_z - 0.01,
                )
                if raw_fill is not None:
                    arr.markers.append(raw_fill)
                raw_ring = raw_outer + [raw_outer[0]]
                arr.markers.append(
                    make_line_strip(
                        self.map_frame,
                        "rect_zone_raw",
                        2,
                        _line_pts(raw_ring, self.preview_z),
                        (1.0, 0.75, 0.15, 0.95),
                        self.preview_line_width,
                    )
                )
                text_x = sum(float(p[0]) for p in raw_outer) / float(len(raw_outer))
                text_y = sum(float(p[1]) for p in raw_outer) / float(len(raw_outer))

            effective_outer = list(self.preview.get("effective_outer") or [])
            if effective_outer:
                eff_ring = effective_outer + [effective_outer[0]]
                eff_color = (0.1, 0.9, 0.2, 0.95) if bool(self.preview.get("valid")) else (1.0, 0.3, 0.3, 0.95)
                eff_fill = self._make_rect_fill_marker(
                    effective_outer,
                    ns="rect_zone_effective_fill",
                    mid=21,
                    rgba=(eff_color[0], eff_color[1], eff_color[2], 0.12 if bool(self.preview.get("valid")) else 0.08),
                    z=self.preview_z,
                )
                if eff_fill is not None:
                    arr.markers.append(eff_fill)
                arr.markers.append(
                    make_line_strip(
                        self.map_frame,
                        "rect_zone_effective",
                        3,
                        _line_pts(eff_ring, self.preview_z + 0.01),
                        eff_color,
                        max(0.015, self.preview_line_width * 1.15),
                    )
                )
                text_color = eff_color
            elif not bool(self.preview.get("valid")):
                text_color = (1.0, 0.3, 0.3, 0.95)

            for idx, hole in enumerate(list(self.preview.get("geom_holes") or []), start=10):
                if not hole:
                    continue
                hole_ring = list(hole) + [hole[0]]
                arr.markers.append(
                    make_line_strip(
                        self.map_frame,
                        "rect_zone_holes",
                        idx,
                        _line_pts(hole_ring, self.preview_z + 0.015),
                        (0.95, 0.2, 0.2, 0.95),
                        max(0.015, self.preview_line_width),
                    )
                )

        summary = "\n".join(self._preview_summary_lines())
        if (not self.preview) and self.first_point is not None:
            text_x, text_y = self.first_point
        elif self.preview:
            eff_center = _ring_center(list(self.preview.get("effective_outer") or []))
            if eff_center != (0.0, 0.0):
                text_x, text_y = eff_center
        arr.markers.append(self._make_text_marker(summary, text_x, text_y, self.preview_z + 0.28, text_color))
        self.marker_pub.publish(arr)

    def _on_click(self, msg: PointStamped):
        try:
            msg_map = self._transform_to_map(msg)
        except Exception as e:
            rospy.logwarn("[rect_zone_planner] failed to transform click into %s: %s", self.map_frame, str(e))
            return

        xy = (float(msg_map.point.x), float(msg_map.point.y))
        if self.preview is not None:
            self.preview = None
            self.first_point = xy
            rospy.loginfo("[rect_zone_planner] new selection started at (%.3f, %.3f)", xy[0], xy[1])
            self._publish_markers()
            return

        if self.first_point is None:
            self.first_point = xy
            rospy.loginfo("[rect_zone_planner] first corner set at (%.3f, %.3f)", xy[0], xy[1])
            self._publish_markers()
            return

        p0 = self.first_point
        self.first_point = None
        self.preview = self._build_preview(p0, xy)
        rospy.loginfo("[rect_zone_planner] %s", str(self.preview.get("message") or "preview updated"))
        self._publish_markers()

    def _handle_confirm(self, req) -> SiteConfirmRectCoveragePlanResponse:
        zone_id = str(req.zone_id or "").strip()
        # ConfirmRectCoveragePlan keeps the wire field name `profile_name`.
        plan_profile_name = str(req.profile_name or "").strip() or self.default_plan_profile_name
        if not zone_id:
            return SiteConfirmRectCoveragePlanResponse(success=False, message="zone_id is required", plan_id="")

        if not self.preview:
            return SiteConfirmRectCoveragePlanResponse(success=False, message="no rectangle preview to confirm", plan_id="")
        if not bool(self.preview.get("valid")):
            return SiteConfirmRectCoveragePlanResponse(
                success=False,
                message=str(self.preview.get("message") or "preview is not valid"),
                plan_id="",
            )

        active_asset = self.store.get_active_map(robot_id=self.robot_id) or {}
        active_name = str(active_asset.get("map_name") or "").strip()
        active_revision_id = str(active_asset.get("revision_id") or "").strip()
        if not active_name:
            return SiteConfirmRectCoveragePlanResponse(success=False, message="current map is not selected", plan_id="")

        preview_map_name = str(self.preview.get("map_name") or "").strip()
        preview_map_revision_id = str(self.preview.get("map_revision_id") or "").strip()
        if preview_map_name and preview_map_name != active_name:
            return SiteConfirmRectCoveragePlanResponse(
                success=False,
                message="selected map changed after preview: preview=%s current=%s" % (preview_map_name, active_name),
                plan_id="",
            )
        if preview_map_revision_id and active_revision_id and preview_map_revision_id != active_revision_id:
            return SiteConfirmRectCoveragePlanResponse(
                success=False,
                message="selected map revision changed after preview: preview=%s current=%s" % (
                    preview_map_revision_id,
                    active_revision_id,
                ),
                plan_id="",
            )

        p0 = tuple(self.preview.get("first_point") or ())
        p1 = tuple(self.preview.get("second_point") or ())
        if len(p0) != 2 or len(p1) != 2:
            return SiteConfirmRectCoveragePlanResponse(success=False, message="preview is missing corner points", plan_id="")
        current_preview = self._build_preview((float(p0[0]), float(p0[1])), (float(p1[0]), float(p1[1])))
        current_preview["map_name"] = active_name
        current_preview["map_revision_id"] = active_revision_id
        self.preview = current_preview
        self._publish_markers()
        if not bool(current_preview.get("valid")):
            return SiteConfirmRectCoveragePlanResponse(
                success=False,
                message=str(current_preview.get("message") or "preview is not valid"),
                plan_id="",
            )

        if self.store.get_zone_meta(
            zone_id,
            map_name=active_name,
            map_revision_id=active_revision_id,
        ) is not None:
            return SiteConfirmRectCoveragePlanResponse(
                success=False,
                message="zone_id already exists on current map: %s" % zone_id,
                plan_id="",
            )

        map_id, map_md5, ok = ensure_map_identity(
            map_topic=self.map_topic,
            timeout_s=self.map_identity_timeout_s,
            set_global_params=True,
            set_private_params=False,
            refresh=True,
        )
        if not ok or not map_id:
            return SiteConfirmRectCoveragePlanResponse(success=False, message="failed to refresh runtime map identity", plan_id="")

        runtime_revision_id = str(get_runtime_map_revision_id("/cartographer/runtime") or "").strip()
        asset_id = str(active_asset.get("map_id") or "").strip()
        asset_md5 = str(active_asset.get("map_md5") or "").strip()
        if active_revision_id and runtime_revision_id and active_revision_id != runtime_revision_id:
            return SiteConfirmRectCoveragePlanResponse(
                success=False,
                message="runtime /map revision does not match selected map asset",
                plan_id="",
            )
        if (not runtime_revision_id) and asset_md5 and map_md5 and asset_md5 != map_md5:
            return SiteConfirmRectCoveragePlanResponse(
                success=False,
                message="runtime /map md5 does not match selected map asset",
                plan_id="",
            )
        if (not runtime_revision_id) and asset_id and map_id and asset_id != map_id:
            return SiteConfirmRectCoveragePlanResponse(
                success=False,
                message="runtime /map id does not match selected map asset",
                plan_id="",
            )

        if not self.plan_client.wait_for_server(rospy.Duration(self.action_connect_timeout_s)):
            return SiteConfirmRectCoveragePlanResponse(
                success=False,
                message="planner action not available: %s" % self.plan_action_name,
                plan_id="",
            )

        outer = list(current_preview.get("effective_outer") or [])
        holes = list(current_preview.get("geom_holes") or [])
        geom = ZoneGeometry()
        geom.frame_id = self.map_frame
        poly = Polygon2D()
        poly.points = [Point32(x=float(x), y=float(y), z=0.0) for (x, y) in outer]
        geom.outer = poly
        geom.holes = []
        for ring in holes:
            hole_poly = Polygon2D()
            hole_poly.points = [Point32(x=float(x), y=float(y), z=0.0) for (x, y) in ring]
            geom.holes.append(hole_poly)

        goal = PlanCoverageGoal()
        goal.zone_id = zone_id
        goal.zone_version = 1
        goal.geom = geom
        goal.polygon_json = ""
        goal.profile_name = plan_profile_name
        goal.debug_publish_markers = bool(req.debug_publish_markers)
        goal.export_input = False

        rospy.loginfo(
            "[rect_zone_planner] confirm zone=%s map=%s plan_profile_name=%s holes=%d",
            zone_id,
            active_name,
            plan_profile_name,
            len(holes),
        )
        self.plan_client.send_goal(goal)
        finished = self.plan_client.wait_for_result(rospy.Duration(self.plan_timeout_s))
        if not finished:
            try:
                self.plan_client.cancel_goal()
            except Exception:
                pass
            return SiteConfirmRectCoveragePlanResponse(success=False, message="planner action timed out", plan_id="")

        result = self.plan_client.get_result()
        if result is None:
            return SiteConfirmRectCoveragePlanResponse(success=False, message="planner returned no result", plan_id="")
        if not bool(getattr(result, "ok", False)):
            return SiteConfirmRectCoveragePlanResponse(
                success=False,
                message=str(getattr(result, "error_message", "") or getattr(result, "error_code", "planning failed")),
                plan_id="",
            )

        plan_id = str(getattr(result, "plan_id", "") or "")
        rospy.loginfo("[rect_zone_planner] plan created zone=%s plan_id=%s", zone_id, plan_id)
        self._clear_selection()
        return SiteConfirmRectCoveragePlanResponse(
            success=True,
            message="planned zone=%s map=%s plan_profile_name=%s holes=%d" % (zone_id, active_name, plan_profile_name, len(holes)),
            plan_id=plan_id,
        )

    def _handle_confirm_site(self, req) -> SiteConfirmRectCoveragePlanResponse:
        return self._handle_confirm(req)


def main():
    rospy.init_node("rect_zone_planner")
    RectZonePlannerNode()
    rospy.spin()


if __name__ == "__main__":
    main()

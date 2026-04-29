#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from dataclasses import replace
import json
import math
import uuid
from typing import Dict, List, Optional, Sequence, Tuple

import rospy
from geometry_msgs.msg import Point32, Pose2D
from cleanrobot_app_msgs.msg import (
    CoverageZone as AppCoverageZone,
    MapAlignmentConfig as AppMapAlignmentConfig,
    MapNoGoArea as AppMapNoGoArea,
    MapVirtualWall as AppMapVirtualWall,
    PolygonRegion as AppPolygonRegion,
    PolygonRing as AppPolygonRing,
)
from cleanrobot_app_msgs.srv import (
    CommitCoverageRegion as AppCommitCoverageRegion,
    CommitCoverageRegionResponse as AppCommitCoverageRegionResponse,
    ConfirmMapAlignmentByPoints as AppConfirmMapAlignmentByPoints,
    ConfirmMapAlignmentByPointsResponse as AppConfirmMapAlignmentByPointsResponse,
    GetZonePlanPath as AppGetZonePlanPath,
    GetZonePlanPathResponse as AppGetZonePlanPathResponse,
    OperateCoverageZone as AppOperateCoverageZone,
    OperateCoverageZoneResponse as AppOperateCoverageZoneResponse,
    OperateMapAlignment as AppOperateMapAlignment,
    OperateMapAlignmentResponse as AppOperateMapAlignmentResponse,
    OperateMapNoGoArea as AppOperateMapNoGoArea,
    OperateMapNoGoAreaResponse as AppOperateMapNoGoAreaResponse,
    OperateMapVirtualWall as AppOperateMapVirtualWall,
    OperateMapVirtualWallResponse as AppOperateMapVirtualWallResponse,
    PreviewAlignedRectSelection as AppPreviewAlignedRectSelection,
    PreviewAlignedRectSelectionResponse as AppPreviewAlignedRectSelectionResponse,
    PreviewCoverageRegion as AppPreviewCoverageRegion,
    PreviewCoverageRegionResponse as AppPreviewCoverageRegionResponse,
)
from cleanrobot_site_msgs.msg import (
    CoverageZone as SiteCoverageZone,
    MapAlignmentConfig as SiteMapAlignmentConfig,
    MapNoGoArea as SiteMapNoGoArea,
    MapVirtualWall as SiteMapVirtualWall,
    PolygonRegion as SitePolygonRegion,
    PolygonRing as SitePolygonRing,
)
from cleanrobot_site_msgs.srv import (
    CommitCoverageRegion as SiteCommitCoverageRegion,
    CommitCoverageRegionResponse as SiteCommitCoverageRegionResponse,
    ConfirmMapAlignmentByPoints as SiteConfirmMapAlignmentByPoints,
    ConfirmMapAlignmentByPointsResponse as SiteConfirmMapAlignmentByPointsResponse,
    GetZonePlanPath as SiteGetZonePlanPath,
    GetZonePlanPathResponse as SiteGetZonePlanPathResponse,
    OperateCoverageZone as SiteOperateCoverageZone,
    OperateCoverageZoneResponse as SiteOperateCoverageZoneResponse,
    OperateMapAlignment as SiteOperateMapAlignment,
    OperateMapAlignmentResponse as SiteOperateMapAlignmentResponse,
    OperateMapNoGoArea as SiteOperateMapNoGoArea,
    OperateMapNoGoAreaResponse as SiteOperateMapNoGoAreaResponse,
    OperateMapVirtualWall as SiteOperateMapVirtualWall,
    OperateMapVirtualWallResponse as SiteOperateMapVirtualWallResponse,
    PreviewAlignedRectSelection as SitePreviewAlignedRectSelection,
    PreviewAlignedRectSelectionResponse as SitePreviewAlignedRectSelectionResponse,
    PreviewCoverageRegion as SitePreviewCoverageRegion,
    PreviewCoverageRegionResponse as SitePreviewCoverageRegionResponse,
)

from coverage_planner.constraints import (
    DEFAULT_VIRTUAL_WALL_BUFFER_M,
    compile_map_constraints,
    compile_zone_constraints,
    filter_effective_regions_for_planner,
    make_hole_free_effective_regions,
    min_plannable_span_m,
)
from coverage_planner.coverage_planner_core.geom_norm import normalize_polygon
from coverage_planner.coverage_planner_core.isolated_runner import run_plan_coverage_isolated
from coverage_planner.coverage_planner_core.types import PlannerParams, RobotSpec
from coverage_planner.map_asset_status import map_asset_verification_error
from coverage_planner.map_alignment import (
    MapAlignment,
    aligned_to_map_point,
    aligned_to_map_polygon,
    make_axis_aligned_rect,
    map_to_aligned_point,
    map_to_aligned_polygon,
    map_to_aligned_pose,
    polygon_area,
    yaw_offset_deg_from_points,
)
from coverage_planner.plan_store.store import PlanStore
from coverage_planner.ros_contract import build_contract_report, validate_ros_contract
from coverage_planner.ros_wire_clone import clone_ros_message


CoverageZone = SiteCoverageZone
MapAlignmentConfig = SiteMapAlignmentConfig
MapNoGoArea = SiteMapNoGoArea
MapVirtualWall = SiteMapVirtualWall
PolygonRegion = SitePolygonRegion
PolygonRing = SitePolygonRing

CommitCoverageRegion = SiteCommitCoverageRegion
CommitCoverageRegionResponse = SiteCommitCoverageRegionResponse
ConfirmMapAlignmentByPoints = SiteConfirmMapAlignmentByPoints
ConfirmMapAlignmentByPointsResponse = SiteConfirmMapAlignmentByPointsResponse
GetZonePlanPath = SiteGetZonePlanPath
GetZonePlanPathResponse = SiteGetZonePlanPathResponse
OperateCoverageZone = SiteOperateCoverageZone
OperateCoverageZoneResponse = SiteOperateCoverageZoneResponse
OperateMapAlignment = SiteOperateMapAlignment
OperateMapAlignmentResponse = SiteOperateMapAlignmentResponse
OperateMapNoGoArea = SiteOperateMapNoGoArea
OperateMapNoGoAreaResponse = SiteOperateMapNoGoAreaResponse
OperateMapVirtualWall = SiteOperateMapVirtualWall
OperateMapVirtualWallResponse = SiteOperateMapVirtualWallResponse
PreviewAlignedRectSelection = SitePreviewAlignedRectSelection
PreviewAlignedRectSelectionResponse = SitePreviewAlignedRectSelectionResponse
PreviewCoverageRegion = SitePreviewCoverageRegion
PreviewCoverageRegionResponse = SitePreviewCoverageRegionResponse


XY = Tuple[float, float]
_POLYGON_RING_FIELDS = ["points"]
_POLYGON_REGION_FIELDS = ["frame_id", "outer", "holes"]
_MAP_ALIGNMENT_CONFIG_FIELDS = [
    "map_name",
    "map_revision_id",
    "map_id",
    "map_version",
    "alignment_version",
    "raw_frame",
    "aligned_frame",
    "yaw_offset_deg",
    "pivot_x",
    "pivot_y",
    "source",
    "status",
    "active",
    "created_ts",
    "updated_ts",
]
_COVERAGE_ZONE_FIELDS = [
    "map_name",
    "map_revision_id",
    "zone_id",
    "display_name",
    "enabled",
    "zone_version",
    "alignment_version",
    "display_frame",
    "storage_frame",
    "map_id",
    "map_version",
    "active_plan_id",
    "plan_profile_name",
    "estimated_length_m",
    "estimated_duration_s",
    "warnings",
    "display_region",
    "map_region",
    "updated_ts",
]
_MAP_NO_GO_AREA_FIELDS = [
    "map_name",
    "map_revision_id",
    "area_id",
    "display_name",
    "enabled",
    "alignment_version",
    "display_frame",
    "storage_frame",
    "map_id",
    "map_version",
    "display_region",
    "map_region",
    "updated_ts",
    "warnings",
]
_MAP_VIRTUAL_WALL_FIELDS = [
    "map_name",
    "map_revision_id",
    "wall_id",
    "display_name",
    "enabled",
    "alignment_version",
    "display_frame",
    "storage_frame",
    "map_id",
    "map_version",
    "display_path",
    "map_path",
    "buffer_m",
    "updated_ts",
    "warnings",
]
_OPERATE_MAP_ALIGNMENT_REQUEST_FIELDS = ["operation", "map_name", "map_revision_id", "alignment_version", "config"]
_OPERATE_MAP_ALIGNMENT_REQUEST_CONSTANTS = ["get", "add", "modify", "Delete", "getAll", "activate"]
_OPERATE_MAP_ALIGNMENT_RESPONSE_FIELDS = ["success", "message", "config", "configs"]
_CONFIRM_ALIGNMENT_POINTS_REQUEST_FIELDS = [
    "map_name",
    "map_revision_id",
    "alignment_version",
    "raw_frame",
    "aligned_frame",
    "p1",
    "p2",
    "pivot_x",
    "pivot_y",
    "source",
    "status",
    "activate",
]
_CONFIRM_ALIGNMENT_POINTS_RESPONSE_FIELDS = ["success", "message", "yaw_offset_deg", "config"]
_PREVIEW_RECT_REQUEST_FIELDS = ["map_name", "map_revision_id", "alignment_version", "p1", "p2", "min_side_m"]
_PREVIEW_RECT_RESPONSE_FIELDS = [
    "success",
    "message",
    "map_revision_id",
    "alignment_version",
    "display_frame",
    "storage_frame",
    "display_region",
    "map_region",
    "width_m",
    "height_m",
    "area_m2",
    "valid",
    "warnings",
]
_PREVIEW_COVERAGE_REQUEST_FIELDS = [
    "map_name",
    "map_revision_id",
    "alignment_version",
    "region",
    "profile_name",
    "debug_publish_markers",
]
_PREVIEW_COVERAGE_RESPONSE_FIELDS = [
    "success",
    "message",
    "valid",
    "error_code",
    "error_message",
    "map_revision_id",
    "alignment_version",
    "display_frame",
    "storage_frame",
    "display_region",
    "map_region",
    "preview_path",
    "display_preview_path",
    "entry_pose",
    "display_entry_pose",
    "estimated_length_m",
    "estimated_duration_s",
    "warnings",
]
_COMMIT_COVERAGE_REQUEST_FIELDS = [
    "map_name",
    "map_revision_id",
    "alignment_version",
    "zone_id",
    "base_zone_version",
    "display_name",
    "region",
    "profile_name",
    "set_active_plan",
]
_COMMIT_COVERAGE_RESPONSE_FIELDS = [
    "success",
    "message",
    "valid",
    "error_code",
    "map_revision_id",
    "zone_id",
    "zone_version",
    "plan_id",
    "alignment_version",
    "display_frame",
    "storage_frame",
    "display_region",
    "map_region",
    "preview_path",
    "display_preview_path",
    "entry_pose",
    "display_entry_pose",
    "estimated_length_m",
    "estimated_duration_s",
    "warnings",
]
_OPERATE_COVERAGE_ZONE_REQUEST_FIELDS = [
    "operation",
    "map_name",
    "map_revision_id",
    "zone_id",
    "alignment_version",
    "plan_profile_name",
    "include_disabled",
]
_OPERATE_COVERAGE_ZONE_REQUEST_CONSTANTS = ["get", "getAll", "Delete"]
_OPERATE_COVERAGE_ZONE_RESPONSE_FIELDS = ["success", "message", "zone", "zones"]
_GET_ZONE_PLAN_PATH_REQUEST_FIELDS = ["map_name", "map_revision_id", "zone_id", "alignment_version", "plan_profile_name"]
_GET_ZONE_PLAN_PATH_RESPONSE_FIELDS = [
    "success",
    "message",
    "map_revision_id",
    "zone_id",
    "active_plan_id",
    "plan_profile_name",
    "alignment_version",
    "display_frame",
    "storage_frame",
    "display_path",
    "map_path",
    "display_entry_pose",
    "entry_pose",
    "estimated_length_m",
    "estimated_duration_s",
    "warnings",
]
_OPERATE_MAP_NO_GO_AREA_REQUEST_FIELDS = [
    "operation",
    "map_name",
    "map_revision_id",
    "area_id",
    "alignment_version",
    "area",
    "include_disabled",
]
_OPERATE_MAP_NO_GO_AREA_REQUEST_CONSTANTS = ["get", "getAll", "add", "modify", "Delete"]
_OPERATE_MAP_NO_GO_AREA_RESPONSE_FIELDS = ["success", "message", "constraint_version", "area", "areas", "warnings"]
_OPERATE_MAP_VIRTUAL_WALL_REQUEST_FIELDS = [
    "operation",
    "map_name",
    "map_revision_id",
    "wall_id",
    "alignment_version",
    "wall",
    "include_disabled",
]
_OPERATE_MAP_VIRTUAL_WALL_REQUEST_CONSTANTS = ["get", "getAll", "add", "modify", "Delete"]
_OPERATE_MAP_VIRTUAL_WALL_RESPONSE_FIELDS = ["success", "message", "constraint_version", "wall", "walls", "warnings"]


def _point32(x: float, y: float) -> Point32:
    msg = Point32()
    msg.x = float(x)
    msg.y = float(y)
    msg.z = 0.0
    return msg


def _ring_to_xy(msg: PolygonRing) -> List[XY]:
    out: List[XY] = []
    for pt in (msg.points or []):
        out.append((float(pt.x), float(pt.y)))
    return out


def _open_ring(points: Sequence[Sequence[float]]) -> List[XY]:
    out: List[XY] = []
    for pt in points or []:
        if pt is None or len(pt) < 2:
            continue
        out.append((float(pt[0]), float(pt[1])))
    if len(out) >= 2 and abs(out[0][0] - out[-1][0]) < 1e-9 and abs(out[0][1] - out[-1][1]) < 1e-9:
        out = out[:-1]
    return out


def _xy_to_ring(points: Sequence[Sequence[float]]) -> PolygonRing:
    msg = PolygonRing()
    msg.points = [_point32(float(pt[0]), float(pt[1])) for pt in _open_ring(points)]
    return msg


def _region_to_lists(region: PolygonRegion) -> Tuple[str, List[XY], List[List[XY]]]:
    frame_id = str(region.frame_id or "").strip()
    outer = _ring_to_xy(region.outer)
    holes = [_ring_to_xy(h) for h in (region.holes or [])]
    return frame_id, outer, holes


def _lists_to_region(frame_id: str, outer: Sequence[Sequence[float]], holes: Sequence[Sequence[Sequence[float]]]) -> PolygonRegion:
    msg = PolygonRegion()
    msg.frame_id = str(frame_id or "").strip()
    msg.outer = _xy_to_ring(outer)
    msg.holes = [_xy_to_ring(ring) for ring in (holes or [])]
    return msg


def _pose2d(x: float = 0.0, y: float = 0.0, theta: float = 0.0) -> Pose2D:
    msg = Pose2D()
    msg.x = float(x)
    msg.y = float(y)
    msg.theta = float(theta)
    return msg


def _positive_float(value, default):
    try:
        v = float(value)
    except Exception:
        return float(default)
    if (not math.isfinite(v)) or v <= 0.0:
        return float(default)
    return float(v)


def _finite_float(value, default):
    try:
        v = float(value)
    except Exception:
        return float(default)
    if not math.isfinite(v):
        return float(default)
    return float(v)


def _positive_int(value, default):
    try:
        v = int(value)
    except Exception:
        return int(default)
    if v <= 0:
        return int(default)
    return int(v)


class SiteEditorServiceNode:
    def __init__(self):
        self.plan_db_path = rospy.get_param("~plan_db_path", "/data/coverage/planning.db")
        self.robot_id = str(rospy.get_param("~robot_id", "local_robot"))
        self.default_aligned_frame = str(rospy.get_param("~default_aligned_frame", "site_map")).strip() or "site_map"
        self.rect_min_side_m = _positive_float(rospy.get_param("~rect_min_side_m", 0.10), 0.10)
        self.preview_nominal_speed_mps = _positive_float(rospy.get_param("~preview_nominal_speed_mps", 0.35), 0.35)
        self.default_virtual_wall_buffer_m = float(
            rospy.get_param("~default_virtual_wall_buffer_m", DEFAULT_VIRTUAL_WALL_BUFFER_M)
        )
        self.default_no_go_buffer_m = float(
            rospy.get_param("~default_no_go_buffer_m", 0.30)
        )
        self.default_plan_profile_name = str(rospy.get_param("~default_plan_profile_name", "cover_standard")).strip() or "cover_standard"
        self.planner_worker_timeout_s = _positive_float(rospy.get_param("~planner_worker_timeout_s", 45.0), 45.0)
        self.degraded_preview_on_planner_crash = bool(
            rospy.get_param("~degraded_preview_on_planner_crash", True)
        )
        self.degraded_commit_on_planner_crash = bool(
            rospy.get_param("~degraded_commit_on_planner_crash", False)
        )

        self.app_alignment_contract_param_ns = str(
            rospy.get_param(
                "~app_alignment_contract_param_ns",
                "/database_server/contracts/app/map_alignment_service",
            )
        ).strip() or "/database_server/contracts/app/map_alignment_service"
        self.site_alignment_contract_param_ns = str(
            rospy.get_param(
                "~site_alignment_contract_param_ns",
                "/database_server/contracts/site/map_alignment_service",
            )
        ).strip() or "/database_server/contracts/site/map_alignment_service"
        self.app_alignment_points_contract_param_ns = str(
            rospy.get_param(
                "~app_alignment_points_contract_param_ns",
                "/database_server/contracts/app/map_alignment_by_points_service",
            )
        ).strip() or "/database_server/contracts/app/map_alignment_by_points_service"
        self.site_alignment_points_contract_param_ns = str(
            rospy.get_param(
                "~site_alignment_points_contract_param_ns",
                "/database_server/contracts/site/map_alignment_by_points_service",
            )
        ).strip() or "/database_server/contracts/site/map_alignment_by_points_service"
        self.app_rect_preview_contract_param_ns = str(
            rospy.get_param(
                "~app_rect_preview_contract_param_ns",
                "/database_server/contracts/app/rect_zone_preview_service",
            )
        ).strip() or "/database_server/contracts/app/rect_zone_preview_service"
        self.site_rect_preview_contract_param_ns = str(
            rospy.get_param(
                "~site_rect_preview_contract_param_ns",
                "/database_server/contracts/site/rect_zone_preview_service",
            )
        ).strip() or "/database_server/contracts/site/rect_zone_preview_service"
        self.app_rect_preview_service_name = str(
            rospy.get_param("~app_rect_preview_service_name", "/database_server/app/rect_zone_preview_service")
        ).strip() or "/database_server/app/rect_zone_preview_service"
        self.site_rect_preview_service_name = str(
            rospy.get_param("~site_rect_preview_service_name", "/database_server/site/rect_zone_preview_service")
        ).strip() or "/database_server/site/rect_zone_preview_service"
        self.app_plan_preview_contract_param_ns = str(
            rospy.get_param(
                "~app_plan_preview_contract_param_ns",
                "/database_server/contracts/app/coverage_preview_service",
            )
        ).strip() or "/database_server/contracts/app/coverage_preview_service"
        self.app_plan_preview_service_name = str(
            rospy.get_param("~app_plan_preview_service_name", "/database_server/app/coverage_preview_service")
        ).strip() or "/database_server/app/coverage_preview_service"
        self.site_plan_preview_contract_param_ns = str(
            rospy.get_param(
                "~site_plan_preview_contract_param_ns",
                "/database_server/contracts/site/coverage_preview_service",
            )
        ).strip() or "/database_server/contracts/site/coverage_preview_service"
        self.site_plan_preview_service_name = str(
            rospy.get_param("~site_plan_preview_service_name", "/database_server/site/coverage_preview_service")
        ).strip() or "/database_server/site/coverage_preview_service"
        self.app_plan_commit_contract_param_ns = str(
            rospy.get_param(
                "~app_plan_commit_contract_param_ns",
                "/database_server/contracts/app/coverage_commit_service",
            )
        ).strip() or "/database_server/contracts/app/coverage_commit_service"
        self.app_plan_commit_service_name = str(
            rospy.get_param("~app_plan_commit_service_name", "/database_server/app/coverage_commit_service")
        ).strip() or "/database_server/app/coverage_commit_service"
        self.site_plan_commit_contract_param_ns = str(
            rospy.get_param(
                "~site_plan_commit_contract_param_ns",
                "/database_server/contracts/site/coverage_commit_service",
            )
        ).strip() or "/database_server/contracts/site/coverage_commit_service"
        self.site_plan_commit_service_name = str(
            rospy.get_param("~site_plan_commit_service_name", "/database_server/site/coverage_commit_service")
        ).strip() or "/database_server/site/coverage_commit_service"
        self.app_zone_query_contract_param_ns = str(
            rospy.get_param(
                "~app_zone_query_contract_param_ns",
                "/database_server/contracts/app/coverage_zone_service",
            )
        ).strip() or "/database_server/contracts/app/coverage_zone_service"
        self.app_zone_query_service_name = str(
            rospy.get_param("~app_zone_query_service_name", "/database_server/app/coverage_zone_service")
        ).strip() or "/database_server/app/coverage_zone_service"
        self.site_zone_query_contract_param_ns = str(
            rospy.get_param(
                "~site_zone_query_contract_param_ns",
                "/database_server/contracts/site/coverage_zone_service",
            )
        ).strip() or "/database_server/contracts/site/coverage_zone_service"
        self.site_zone_query_service_name = str(
            rospy.get_param("~site_zone_query_service_name", "/database_server/site/coverage_zone_service")
        ).strip() or "/database_server/site/coverage_zone_service"
        self.app_zone_plan_path_contract_param_ns = str(
            rospy.get_param(
                "~app_zone_plan_path_contract_param_ns",
                "/database_server/contracts/app/zone_plan_path_service",
            )
        ).strip() or "/database_server/contracts/app/zone_plan_path_service"
        self.app_zone_plan_path_service_name = str(
            rospy.get_param("~app_zone_plan_path_service_name", "/database_server/app/zone_plan_path_service")
        ).strip() or "/database_server/app/zone_plan_path_service"
        self.site_zone_plan_path_contract_param_ns = str(
            rospy.get_param(
                "~site_zone_plan_path_contract_param_ns",
                "/database_server/contracts/site/zone_plan_path_service",
            )
        ).strip() or "/database_server/contracts/site/zone_plan_path_service"
        self.site_zone_plan_path_service_name = str(
            rospy.get_param("~site_zone_plan_path_service_name", "/database_server/site/zone_plan_path_service")
        ).strip() or "/database_server/site/zone_plan_path_service"
        self.app_no_go_area_contract_param_ns = str(
            rospy.get_param(
                "~app_no_go_area_contract_param_ns",
                "/database_server/contracts/app/no_go_area_service",
            )
        ).strip() or "/database_server/contracts/app/no_go_area_service"
        self.site_no_go_area_contract_param_ns = str(
            rospy.get_param(
                "~site_no_go_area_contract_param_ns",
                "/database_server/contracts/site/no_go_area_service",
            )
        ).strip() or "/database_server/contracts/site/no_go_area_service"
        self.app_no_go_area_service_name = str(
            rospy.get_param("~app_no_go_area_service_name", "/database_server/app/no_go_area_service")
        ).strip() or "/database_server/app/no_go_area_service"
        self.site_no_go_area_service_name = str(
            rospy.get_param("~site_no_go_area_service_name", "/database_server/site/no_go_area_service")
        ).strip() or "/database_server/site/no_go_area_service"
        self.app_virtual_wall_contract_param_ns = str(
            rospy.get_param(
                "~app_virtual_wall_contract_param_ns",
                "/database_server/contracts/app/virtual_wall_service",
            )
        ).strip() or "/database_server/contracts/app/virtual_wall_service"
        self.site_virtual_wall_contract_param_ns = str(
            rospy.get_param(
                "~site_virtual_wall_contract_param_ns",
                "/database_server/contracts/site/virtual_wall_service",
            )
        ).strip() or "/database_server/contracts/site/virtual_wall_service"
        self.app_virtual_wall_service_name = str(
            rospy.get_param("~app_virtual_wall_service_name", "/database_server/app/virtual_wall_service")
        ).strip() or "/database_server/app/virtual_wall_service"
        self.site_virtual_wall_service_name = str(
            rospy.get_param("~site_virtual_wall_service_name", "/database_server/site/virtual_wall_service")
        ).strip() or "/database_server/site/virtual_wall_service"
        self.app_alignment_service_name = str(
            rospy.get_param("~app_alignment_service_name", "/database_server/app/map_alignment_service")
        ).strip() or "/database_server/app/map_alignment_service"
        self.site_alignment_service_name = str(
            rospy.get_param("~site_alignment_service_name", "/database_server/site/map_alignment_service")
        ).strip() or "/database_server/site/map_alignment_service"
        self.app_alignment_points_service_name = str(
            rospy.get_param("~app_alignment_points_service_name", "/database_server/app/map_alignment_by_points_service")
        ).strip() or "/database_server/app/map_alignment_by_points_service"
        self.site_alignment_points_service_name = str(
            rospy.get_param("~site_alignment_points_service_name", "/database_server/site/map_alignment_by_points_service")
        ).strip() or "/database_server/site/map_alignment_by_points_service"

        self.store = PlanStore(self.plan_db_path)
        self.default_robot_spec = self._load_robot_spec()
        self.default_planner_params = self._load_planner_defaults()
        self._app_contract_reports = self._prepare_app_contract_reports()
        self._site_contract_reports = self._prepare_site_contract_reports()

        self.alignment_srv = None
        self.app_alignment_srv = rospy.Service(
            self.app_alignment_service_name,
            AppOperateMapAlignment,
            self._handle_alignment_app,
        )
        self.site_alignment_srv = rospy.Service(
            self.site_alignment_service_name,
            SiteOperateMapAlignment,
            self._handle_alignment_site,
        )
        self.alignment_points_srv = None
        self.app_alignment_points_srv = rospy.Service(
            self.app_alignment_points_service_name,
            AppConfirmMapAlignmentByPoints,
            self._handle_alignment_points_app,
        )
        self.site_alignment_points_srv = rospy.Service(
            self.site_alignment_points_service_name,
            SiteConfirmMapAlignmentByPoints,
            self._handle_alignment_points_site,
        )
        self.rect_preview_srv = None
        self.app_rect_preview_srv = rospy.Service(
            self.app_rect_preview_service_name,
            AppPreviewAlignedRectSelection,
            self._handle_rect_preview_app,
        )
        self.site_rect_preview_srv = rospy.Service(
            self.site_rect_preview_service_name,
            SitePreviewAlignedRectSelection,
            self._handle_rect_preview_site,
        )
        self.plan_preview_srv = None
        self.app_plan_preview_srv = rospy.Service(
            self.app_plan_preview_service_name,
            AppPreviewCoverageRegion,
            self._handle_plan_preview_app,
        )
        self.site_plan_preview_srv = rospy.Service(
            self.site_plan_preview_service_name,
            SitePreviewCoverageRegion,
            self._handle_plan_preview_site,
        )
        self.plan_commit_srv = None
        self.app_plan_commit_srv = rospy.Service(
            self.app_plan_commit_service_name,
            AppCommitCoverageRegion,
            self._handle_plan_commit_app,
        )
        self.site_plan_commit_srv = rospy.Service(
            self.site_plan_commit_service_name,
            SiteCommitCoverageRegion,
            self._handle_plan_commit_site,
        )
        self.zone_query_srv = None
        self.app_zone_query_srv = rospy.Service(
            self.app_zone_query_service_name,
            AppOperateCoverageZone,
            self._handle_zone_query_app,
        )
        self.site_zone_query_srv = rospy.Service(
            self.site_zone_query_service_name,
            SiteOperateCoverageZone,
            self._handle_zone_query_site,
        )
        self.zone_plan_path_srv = None
        self.app_zone_plan_path_srv = rospy.Service(
            self.app_zone_plan_path_service_name,
            AppGetZonePlanPath,
            self._handle_zone_plan_path_app,
        )
        self.site_zone_plan_path_srv = rospy.Service(
            self.site_zone_plan_path_service_name,
            SiteGetZonePlanPath,
            self._handle_zone_plan_path_site,
        )
        self.no_go_area_srv = None
        self.app_no_go_area_srv = rospy.Service(
            self.app_no_go_area_service_name,
            AppOperateMapNoGoArea,
            self._handle_no_go_area_app,
        )
        self.site_no_go_area_srv = rospy.Service(
            self.site_no_go_area_service_name,
            SiteOperateMapNoGoArea,
            self._handle_no_go_area_site,
        )
        self.virtual_wall_srv = None
        self.app_virtual_wall_srv = rospy.Service(
            self.app_virtual_wall_service_name,
            AppOperateMapVirtualWall,
            self._handle_virtual_wall_app,
        )
        self.site_virtual_wall_srv = rospy.Service(
            self.site_virtual_wall_service_name,
            SiteOperateMapVirtualWall,
            self._handle_virtual_wall_site,
        )
        for contract_param_ns, report in list(self._app_contract_reports) + list(self._site_contract_reports):
            if contract_param_ns:
                rospy.set_param(contract_param_ns, report)
        rospy.loginfo(
            "[site_editor_service] ready db=%s app_alignment=%s site_alignment=%s app_alignment_points=%s site_alignment_points=%s app_rect=%s site_rect=%s app_preview=%s site_preview=%s app_commit=%s site_commit=%s app_zone=%s site_zone=%s app_zone_path=%s site_zone_path=%s app_no_go=%s site_no_go=%s app_wall=%s site_wall=%s app_contracts=%d site_contracts=%d",
            self.plan_db_path,
            self.app_alignment_service_name,
            self.site_alignment_service_name,
            self.app_alignment_points_service_name,
            self.site_alignment_points_service_name,
            self.app_rect_preview_service_name,
            self.site_rect_preview_service_name,
            self.app_plan_preview_service_name,
            self.site_plan_preview_service_name,
            self.app_plan_commit_service_name,
            self.site_plan_commit_service_name,
            self.app_zone_query_service_name,
            self.site_zone_query_service_name,
            self.app_zone_plan_path_service_name,
            self.site_zone_plan_path_service_name,
            self.app_no_go_area_service_name,
            self.site_no_go_area_service_name,
            self.app_virtual_wall_service_name,
            self.site_virtual_wall_service_name,
            len(self._app_contract_reports),
            len(self._site_contract_reports),
        )

    def _prepare_contract_report(
        self,
        *,
        service_name: str,
        contract_name: str,
        service_cls,
        response_cls,
        dependencies,
        features,
        validations,
    ):
        for label, cls, required_fields, required_constants in list(validations or []):
            validate_ros_contract(
                label,
                cls,
                required_fields=required_fields,
                required_constants=required_constants,
            )
        return build_contract_report(
            service_name=service_name,
            contract_name=contract_name,
            service_cls=service_cls,
            request_cls=service_cls._request_class,
            response_cls=response_cls,
            dependencies=dict(dependencies or {}),
            features=list(features or []),
        )

    def _prepare_app_contract_reports(self):
        return [
            (
                self.app_alignment_contract_param_ns,
                self._prepare_contract_report(
                    service_name=self.app_alignment_service_name,
                    contract_name="map_alignment_service_app",
                    service_cls=AppOperateMapAlignment,
                    response_cls=AppOperateMapAlignmentResponse,
                    dependencies={"config": AppMapAlignmentConfig},
                    features=["revision_scoped_alignment_config", "alignment_activation", "cleanrobot_app_msgs_parallel"],
                    validations=[
                        ("AppMapAlignmentConfig", AppMapAlignmentConfig, _MAP_ALIGNMENT_CONFIG_FIELDS, None),
                        (
                            "AppOperateMapAlignmentRequest",
                            AppOperateMapAlignment._request_class,
                            _OPERATE_MAP_ALIGNMENT_REQUEST_FIELDS,
                            _OPERATE_MAP_ALIGNMENT_REQUEST_CONSTANTS,
                        ),
                        ("AppOperateMapAlignmentResponse", AppOperateMapAlignmentResponse, _OPERATE_MAP_ALIGNMENT_RESPONSE_FIELDS, None),
                    ],
                ),
            ),
            (
                self.app_alignment_points_contract_param_ns,
                self._prepare_contract_report(
                    service_name=self.app_alignment_points_service_name,
                    contract_name="map_alignment_by_points_service_app",
                    service_cls=AppConfirmMapAlignmentByPoints,
                    response_cls=AppConfirmMapAlignmentByPointsResponse,
                    dependencies={"config": AppMapAlignmentConfig},
                    features=["alignment_confirm_by_points", "cleanrobot_app_msgs_parallel"],
                    validations=[
                        ("AppMapAlignmentConfig", AppMapAlignmentConfig, _MAP_ALIGNMENT_CONFIG_FIELDS, None),
                        (
                            "AppConfirmMapAlignmentByPointsRequest",
                            AppConfirmMapAlignmentByPoints._request_class,
                            _CONFIRM_ALIGNMENT_POINTS_REQUEST_FIELDS,
                            None,
                        ),
                        (
                            "AppConfirmMapAlignmentByPointsResponse",
                            AppConfirmMapAlignmentByPointsResponse,
                            _CONFIRM_ALIGNMENT_POINTS_RESPONSE_FIELDS,
                            None,
                        ),
                    ],
                ),
            ),
            (
                self.app_rect_preview_contract_param_ns,
                self._prepare_contract_report(
                    service_name=self.app_rect_preview_service_name,
                    contract_name="rect_zone_preview_service_app",
                    service_cls=AppPreviewAlignedRectSelection,
                    response_cls=AppPreviewAlignedRectSelectionResponse,
                    dependencies={
                        "display_region": AppPolygonRegion,
                        "map_region": AppPolygonRegion,
                    },
                    features=["aligned_rect_selection_preview", "cleanrobot_app_msgs_parallel"],
                    validations=[
                        ("AppPolygonRing", AppPolygonRing, _POLYGON_RING_FIELDS, None),
                        ("AppPolygonRegion", AppPolygonRegion, _POLYGON_REGION_FIELDS, None),
                        (
                            "AppPreviewAlignedRectSelectionRequest",
                            AppPreviewAlignedRectSelection._request_class,
                            _PREVIEW_RECT_REQUEST_FIELDS,
                            None,
                        ),
                        (
                            "AppPreviewAlignedRectSelectionResponse",
                            AppPreviewAlignedRectSelectionResponse,
                            _PREVIEW_RECT_RESPONSE_FIELDS,
                            None,
                        ),
                    ],
                ),
            ),
            (
                self.app_plan_preview_contract_param_ns,
                self._prepare_contract_report(
                    service_name=self.app_plan_preview_service_name,
                    contract_name="coverage_preview_service_app",
                    service_cls=AppPreviewCoverageRegion,
                    response_cls=AppPreviewCoverageRegionResponse,
                    dependencies={
                        "display_region": AppPolygonRegion,
                        "map_region": AppPolygonRegion,
                        "preview_path": AppPolygonRing,
                        "display_preview_path": AppPolygonRing,
                    },
                    features=["coverage_region_preview", "cleanrobot_app_msgs_parallel"],
                    validations=[
                        ("AppPolygonRing", AppPolygonRing, _POLYGON_RING_FIELDS, None),
                        ("AppPolygonRegion", AppPolygonRegion, _POLYGON_REGION_FIELDS, None),
                        (
                            "AppPreviewCoverageRegionRequest",
                            AppPreviewCoverageRegion._request_class,
                            _PREVIEW_COVERAGE_REQUEST_FIELDS,
                            None,
                        ),
                        (
                            "AppPreviewCoverageRegionResponse",
                            AppPreviewCoverageRegionResponse,
                            _PREVIEW_COVERAGE_RESPONSE_FIELDS,
                            None,
                        ),
                    ],
                ),
            ),
            (
                self.app_plan_commit_contract_param_ns,
                self._prepare_contract_report(
                    service_name=self.app_plan_commit_service_name,
                    contract_name="coverage_commit_service_app",
                    service_cls=AppCommitCoverageRegion,
                    response_cls=AppCommitCoverageRegionResponse,
                    dependencies={
                        "display_region": AppPolygonRegion,
                        "map_region": AppPolygonRegion,
                        "preview_path": AppPolygonRing,
                        "display_preview_path": AppPolygonRing,
                    },
                    features=[
                        "coverage_region_commit",
                        "zone_version_optimistic_concurrency",
                        "cleanrobot_app_msgs_parallel",
                    ],
                    validations=[
                        ("AppPolygonRing", AppPolygonRing, _POLYGON_RING_FIELDS, None),
                        ("AppPolygonRegion", AppPolygonRegion, _POLYGON_REGION_FIELDS, None),
                        (
                            "AppCommitCoverageRegionRequest",
                            AppCommitCoverageRegion._request_class,
                            _COMMIT_COVERAGE_REQUEST_FIELDS,
                            None,
                        ),
                        (
                            "AppCommitCoverageRegionResponse",
                            AppCommitCoverageRegionResponse,
                            _COMMIT_COVERAGE_RESPONSE_FIELDS,
                            None,
                        ),
                    ],
                ),
            ),
            (
                self.app_zone_query_contract_param_ns,
                self._prepare_contract_report(
                    service_name=self.app_zone_query_service_name,
                    contract_name="coverage_zone_service_app",
                    service_cls=AppOperateCoverageZone,
                    response_cls=AppOperateCoverageZoneResponse,
                    dependencies={"zone": AppCoverageZone},
                    features=["revision_scoped_zone_query", "active_plan_projection", "cleanrobot_app_msgs_parallel"],
                    validations=[
                        ("AppPolygonRing", AppPolygonRing, _POLYGON_RING_FIELDS, None),
                        ("AppPolygonRegion", AppPolygonRegion, _POLYGON_REGION_FIELDS, None),
                        ("AppCoverageZone", AppCoverageZone, _COVERAGE_ZONE_FIELDS, None),
                        (
                            "AppOperateCoverageZoneRequest",
                            AppOperateCoverageZone._request_class,
                            _OPERATE_COVERAGE_ZONE_REQUEST_FIELDS,
                            _OPERATE_COVERAGE_ZONE_REQUEST_CONSTANTS,
                        ),
                        (
                            "AppOperateCoverageZoneResponse",
                            AppOperateCoverageZoneResponse,
                            _OPERATE_COVERAGE_ZONE_RESPONSE_FIELDS,
                            None,
                        ),
                    ],
                ),
            ),
            (
                self.app_zone_plan_path_contract_param_ns,
                self._prepare_contract_report(
                    service_name=self.app_zone_plan_path_service_name,
                    contract_name="zone_plan_path_service_app",
                    service_cls=AppGetZonePlanPath,
                    response_cls=AppGetZonePlanPathResponse,
                    dependencies={"display_path": AppPolygonRing, "map_path": AppPolygonRing},
                    features=["zone_plan_path_projection", "cleanrobot_app_msgs_parallel"],
                    validations=[
                        ("AppPolygonRing", AppPolygonRing, _POLYGON_RING_FIELDS, None),
                        (
                            "AppGetZonePlanPathRequest",
                            AppGetZonePlanPath._request_class,
                            _GET_ZONE_PLAN_PATH_REQUEST_FIELDS,
                            None,
                        ),
                        (
                            "AppGetZonePlanPathResponse",
                            AppGetZonePlanPathResponse,
                            _GET_ZONE_PLAN_PATH_RESPONSE_FIELDS,
                            None,
                        ),
                    ],
                ),
            ),
            (
                self.app_no_go_area_contract_param_ns,
                self._prepare_contract_report(
                    service_name=self.app_no_go_area_service_name,
                    contract_name="no_go_area_service_app",
                    service_cls=AppOperateMapNoGoArea,
                    response_cls=AppOperateMapNoGoAreaResponse,
                    dependencies={"area": AppMapNoGoArea},
                    features=["map_constraints_editor", "cleanrobot_app_msgs_parallel"],
                    validations=[
                        ("AppPolygonRing", AppPolygonRing, _POLYGON_RING_FIELDS, None),
                        ("AppPolygonRegion", AppPolygonRegion, _POLYGON_REGION_FIELDS, None),
                        ("AppMapNoGoArea", AppMapNoGoArea, _MAP_NO_GO_AREA_FIELDS, None),
                        (
                            "AppOperateMapNoGoAreaRequest",
                            AppOperateMapNoGoArea._request_class,
                            _OPERATE_MAP_NO_GO_AREA_REQUEST_FIELDS,
                            _OPERATE_MAP_NO_GO_AREA_REQUEST_CONSTANTS,
                        ),
                        (
                            "AppOperateMapNoGoAreaResponse",
                            AppOperateMapNoGoAreaResponse,
                            _OPERATE_MAP_NO_GO_AREA_RESPONSE_FIELDS,
                            None,
                        ),
                    ],
                ),
            ),
            (
                self.app_virtual_wall_contract_param_ns,
                self._prepare_contract_report(
                    service_name=self.app_virtual_wall_service_name,
                    contract_name="virtual_wall_service_app",
                    service_cls=AppOperateMapVirtualWall,
                    response_cls=AppOperateMapVirtualWallResponse,
                    dependencies={"wall": AppMapVirtualWall},
                    features=["map_constraints_editor", "cleanrobot_app_msgs_parallel"],
                    validations=[
                        ("AppPolygonRing", AppPolygonRing, _POLYGON_RING_FIELDS, None),
                        ("AppMapVirtualWall", AppMapVirtualWall, _MAP_VIRTUAL_WALL_FIELDS, None),
                        (
                            "AppOperateMapVirtualWallRequest",
                            AppOperateMapVirtualWall._request_class,
                            _OPERATE_MAP_VIRTUAL_WALL_REQUEST_FIELDS,
                            _OPERATE_MAP_VIRTUAL_WALL_REQUEST_CONSTANTS,
                        ),
                        (
                            "AppOperateMapVirtualWallResponse",
                            AppOperateMapVirtualWallResponse,
                            _OPERATE_MAP_VIRTUAL_WALL_RESPONSE_FIELDS,
                            None,
                        ),
                    ],
                ),
            ),
        ]

    def _prepare_site_contract_reports(self):
        return [
            (
                self.site_alignment_contract_param_ns,
                self._prepare_contract_report(
                    service_name=self.site_alignment_service_name,
                    contract_name="map_alignment_service_site",
                    service_cls=SiteOperateMapAlignment,
                    response_cls=SiteOperateMapAlignmentResponse,
                    dependencies={"config": SiteMapAlignmentConfig},
                    features=["revision_scoped_alignment_config", "alignment_activation", "cleanrobot_site_msgs_canonical"],
                    validations=[
                        ("SiteMapAlignmentConfig", SiteMapAlignmentConfig, _MAP_ALIGNMENT_CONFIG_FIELDS, None),
                        (
                            "SiteOperateMapAlignmentRequest",
                            SiteOperateMapAlignment._request_class,
                            _OPERATE_MAP_ALIGNMENT_REQUEST_FIELDS,
                            _OPERATE_MAP_ALIGNMENT_REQUEST_CONSTANTS,
                        ),
                        ("SiteOperateMapAlignmentResponse", SiteOperateMapAlignmentResponse, _OPERATE_MAP_ALIGNMENT_RESPONSE_FIELDS, None),
                    ],
                ),
            ),
            (
                self.site_alignment_points_contract_param_ns,
                self._prepare_contract_report(
                    service_name=self.site_alignment_points_service_name,
                    contract_name="map_alignment_by_points_service_site",
                    service_cls=SiteConfirmMapAlignmentByPoints,
                    response_cls=SiteConfirmMapAlignmentByPointsResponse,
                    dependencies={"config": SiteMapAlignmentConfig},
                    features=["alignment_confirm_by_points", "cleanrobot_site_msgs_canonical"],
                    validations=[
                        ("SiteMapAlignmentConfig", SiteMapAlignmentConfig, _MAP_ALIGNMENT_CONFIG_FIELDS, None),
                        (
                            "SiteConfirmMapAlignmentByPointsRequest",
                            SiteConfirmMapAlignmentByPoints._request_class,
                            _CONFIRM_ALIGNMENT_POINTS_REQUEST_FIELDS,
                            None,
                        ),
                        (
                            "SiteConfirmMapAlignmentByPointsResponse",
                            SiteConfirmMapAlignmentByPointsResponse,
                            _CONFIRM_ALIGNMENT_POINTS_RESPONSE_FIELDS,
                            None,
                        ),
                    ],
                ),
            ),
            (
                self.site_rect_preview_contract_param_ns,
                self._prepare_contract_report(
                    service_name=self.site_rect_preview_service_name,
                    contract_name="rect_zone_preview_service_site",
                    service_cls=SitePreviewAlignedRectSelection,
                    response_cls=SitePreviewAlignedRectSelectionResponse,
                    dependencies={
                        "display_region": SitePolygonRegion,
                        "map_region": SitePolygonRegion,
                    },
                    features=["aligned_rect_selection_preview", "cleanrobot_site_msgs_canonical"],
                    validations=[
                        ("SitePolygonRing", SitePolygonRing, _POLYGON_RING_FIELDS, None),
                        ("SitePolygonRegion", SitePolygonRegion, _POLYGON_REGION_FIELDS, None),
                        (
                            "SitePreviewAlignedRectSelectionRequest",
                            SitePreviewAlignedRectSelection._request_class,
                            _PREVIEW_RECT_REQUEST_FIELDS,
                            None,
                        ),
                        (
                            "SitePreviewAlignedRectSelectionResponse",
                            SitePreviewAlignedRectSelectionResponse,
                            _PREVIEW_RECT_RESPONSE_FIELDS,
                            None,
                        ),
                    ],
                ),
            ),
            (
                self.site_plan_preview_contract_param_ns,
                self._prepare_contract_report(
                    service_name=self.site_plan_preview_service_name,
                    contract_name="coverage_preview_service_site",
                    service_cls=SitePreviewCoverageRegion,
                    response_cls=SitePreviewCoverageRegionResponse,
                    dependencies={
                        "display_region": SitePolygonRegion,
                        "map_region": SitePolygonRegion,
                        "preview_path": SitePolygonRing,
                        "display_preview_path": SitePolygonRing,
                    },
                    features=["coverage_region_preview", "cleanrobot_site_msgs_canonical"],
                    validations=[
                        ("SitePolygonRing", SitePolygonRing, _POLYGON_RING_FIELDS, None),
                        ("SitePolygonRegion", SitePolygonRegion, _POLYGON_REGION_FIELDS, None),
                        (
                            "SitePreviewCoverageRegionRequest",
                            SitePreviewCoverageRegion._request_class,
                            _PREVIEW_COVERAGE_REQUEST_FIELDS,
                            None,
                        ),
                        (
                            "SitePreviewCoverageRegionResponse",
                            SitePreviewCoverageRegionResponse,
                            _PREVIEW_COVERAGE_RESPONSE_FIELDS,
                            None,
                        ),
                    ],
                ),
            ),
            (
                self.site_plan_commit_contract_param_ns,
                self._prepare_contract_report(
                    service_name=self.site_plan_commit_service_name,
                    contract_name="coverage_commit_service_site",
                    service_cls=SiteCommitCoverageRegion,
                    response_cls=SiteCommitCoverageRegionResponse,
                    dependencies={
                        "display_region": SitePolygonRegion,
                        "map_region": SitePolygonRegion,
                        "preview_path": SitePolygonRing,
                        "display_preview_path": SitePolygonRing,
                    },
                    features=["coverage_region_commit", "zone_version_optimistic_concurrency", "cleanrobot_site_msgs_canonical"],
                    validations=[
                        ("SitePolygonRing", SitePolygonRing, _POLYGON_RING_FIELDS, None),
                        ("SitePolygonRegion", SitePolygonRegion, _POLYGON_REGION_FIELDS, None),
                        (
                            "SiteCommitCoverageRegionRequest",
                            SiteCommitCoverageRegion._request_class,
                            _COMMIT_COVERAGE_REQUEST_FIELDS,
                            None,
                        ),
                        (
                            "SiteCommitCoverageRegionResponse",
                            SiteCommitCoverageRegionResponse,
                            _COMMIT_COVERAGE_RESPONSE_FIELDS,
                            None,
                        ),
                    ],
                ),
            ),
            (
                self.site_zone_query_contract_param_ns,
                self._prepare_contract_report(
                    service_name=self.site_zone_query_service_name,
                    contract_name="coverage_zone_service_site",
                    service_cls=SiteOperateCoverageZone,
                    response_cls=SiteOperateCoverageZoneResponse,
                    dependencies={"zone": SiteCoverageZone},
                    features=["revision_scoped_zone_query", "active_plan_projection", "cleanrobot_site_msgs_canonical"],
                    validations=[
                        ("SitePolygonRing", SitePolygonRing, _POLYGON_RING_FIELDS, None),
                        ("SitePolygonRegion", SitePolygonRegion, _POLYGON_REGION_FIELDS, None),
                        ("SiteCoverageZone", SiteCoverageZone, _COVERAGE_ZONE_FIELDS, None),
                        (
                            "SiteOperateCoverageZoneRequest",
                            SiteOperateCoverageZone._request_class,
                            _OPERATE_COVERAGE_ZONE_REQUEST_FIELDS,
                            _OPERATE_COVERAGE_ZONE_REQUEST_CONSTANTS,
                        ),
                        (
                            "SiteOperateCoverageZoneResponse",
                            SiteOperateCoverageZoneResponse,
                            _OPERATE_COVERAGE_ZONE_RESPONSE_FIELDS,
                            None,
                        ),
                    ],
                ),
            ),
            (
                self.site_zone_plan_path_contract_param_ns,
                self._prepare_contract_report(
                    service_name=self.site_zone_plan_path_service_name,
                    contract_name="zone_plan_path_service_site",
                    service_cls=SiteGetZonePlanPath,
                    response_cls=SiteGetZonePlanPathResponse,
                    dependencies={"display_path": SitePolygonRing, "map_path": SitePolygonRing},
                    features=["zone_plan_path_projection", "cleanrobot_site_msgs_canonical"],
                    validations=[
                        ("SitePolygonRing", SitePolygonRing, _POLYGON_RING_FIELDS, None),
                        (
                            "SiteGetZonePlanPathRequest",
                            SiteGetZonePlanPath._request_class,
                            _GET_ZONE_PLAN_PATH_REQUEST_FIELDS,
                            None,
                        ),
                        (
                            "SiteGetZonePlanPathResponse",
                            SiteGetZonePlanPathResponse,
                            _GET_ZONE_PLAN_PATH_RESPONSE_FIELDS,
                            None,
                        ),
                    ],
                ),
            ),
            (
                self.site_no_go_area_contract_param_ns,
                self._prepare_contract_report(
                    service_name=self.site_no_go_area_service_name,
                    contract_name="no_go_area_service_site",
                    service_cls=SiteOperateMapNoGoArea,
                    response_cls=SiteOperateMapNoGoAreaResponse,
                    dependencies={"area": SiteMapNoGoArea},
                    features=["map_constraints_editor", "cleanrobot_site_msgs_canonical"],
                    validations=[
                        ("SitePolygonRing", SitePolygonRing, _POLYGON_RING_FIELDS, None),
                        ("SitePolygonRegion", SitePolygonRegion, _POLYGON_REGION_FIELDS, None),
                        ("SiteMapNoGoArea", SiteMapNoGoArea, _MAP_NO_GO_AREA_FIELDS, None),
                        (
                            "SiteOperateMapNoGoAreaRequest",
                            SiteOperateMapNoGoArea._request_class,
                            _OPERATE_MAP_NO_GO_AREA_REQUEST_FIELDS,
                            _OPERATE_MAP_NO_GO_AREA_REQUEST_CONSTANTS,
                        ),
                        (
                            "SiteOperateMapNoGoAreaResponse",
                            SiteOperateMapNoGoAreaResponse,
                            _OPERATE_MAP_NO_GO_AREA_RESPONSE_FIELDS,
                            None,
                        ),
                    ],
                ),
            ),
            (
                self.site_virtual_wall_contract_param_ns,
                self._prepare_contract_report(
                    service_name=self.site_virtual_wall_service_name,
                    contract_name="virtual_wall_service_site",
                    service_cls=SiteOperateMapVirtualWall,
                    response_cls=SiteOperateMapVirtualWallResponse,
                    dependencies={"wall": SiteMapVirtualWall},
                    features=["map_constraints_editor", "cleanrobot_site_msgs_canonical"],
                    validations=[
                        ("SitePolygonRing", SitePolygonRing, _POLYGON_RING_FIELDS, None),
                        ("SiteMapVirtualWall", SiteMapVirtualWall, _MAP_VIRTUAL_WALL_FIELDS, None),
                        (
                            "SiteOperateMapVirtualWallRequest",
                            SiteOperateMapVirtualWall._request_class,
                            _OPERATE_MAP_VIRTUAL_WALL_REQUEST_FIELDS,
                            _OPERATE_MAP_VIRTUAL_WALL_REQUEST_CONSTANTS,
                        ),
                        (
                            "SiteOperateMapVirtualWallResponse",
                            SiteOperateMapVirtualWallResponse,
                            _OPERATE_MAP_VIRTUAL_WALL_RESPONSE_FIELDS,
                            None,
                        ),
                    ],
                ),
            ),
        ]

    def _clone_resp(self, response, response_cls):
        return clone_ros_message(response, response_cls)

    def _handle_alignment_app(self, req):
        return self._clone_resp(self._handle_alignment(req), AppOperateMapAlignmentResponse)

    def _handle_alignment_site(self, req):
        return self._handle_alignment(req)

    def _handle_alignment_points_app(self, req):
        return self._clone_resp(self._handle_alignment_points(req), AppConfirmMapAlignmentByPointsResponse)

    def _handle_alignment_points_site(self, req):
        return self._handle_alignment_points(req)

    def _handle_rect_preview_app(self, req):
        return self._clone_resp(self._handle_rect_preview(req), AppPreviewAlignedRectSelectionResponse)

    def _handle_rect_preview_site(self, req):
        return self._handle_rect_preview(req)

    def _handle_plan_preview_app(self, req):
        return self._clone_resp(self._handle_plan_preview(req), AppPreviewCoverageRegionResponse)

    def _handle_plan_preview_site(self, req):
        return self._handle_plan_preview(req)

    def _handle_plan_commit_app(self, req):
        return self._clone_resp(self._handle_plan_commit(req), AppCommitCoverageRegionResponse)

    def _handle_plan_commit_site(self, req):
        return self._handle_plan_commit(req)

    def _handle_zone_query_app(self, req):
        return self._clone_resp(self._handle_zone_query(req), AppOperateCoverageZoneResponse)

    def _handle_zone_query_site(self, req):
        return self._handle_zone_query(req)

    def _handle_zone_plan_path_app(self, req):
        return self._clone_resp(self._handle_zone_plan_path(req), AppGetZonePlanPathResponse)

    def _handle_zone_plan_path_site(self, req):
        return self._handle_zone_plan_path(req)

    def _handle_no_go_area_app(self, req):
        return self._clone_resp(self._handle_no_go_area(req), AppOperateMapNoGoAreaResponse)

    def _handle_no_go_area_site(self, req):
        return self._handle_no_go_area(req)

    def _handle_virtual_wall_app(self, req):
        return self._clone_resp(self._handle_virtual_wall(req), AppOperateMapVirtualWallResponse)

    def _handle_virtual_wall_site(self, req):
        return self._handle_virtual_wall(req)

    def _param_dict(self, name: str):
        raw = rospy.get_param("~" + str(name), {})
        return raw if isinstance(raw, dict) else {}

    def _private_param(self, key: str, default):
        return rospy.get_param("~" + str(key), default)

    def _cfg_value(self, cfg: dict, key: str, fallback):
        if key in cfg:
            return cfg[key]
        return fallback

    def _load_robot_spec(self) -> RobotSpec:
        cfg = self._param_dict("robot")
        cov_width = _positive_float(
            self._cfg_value(cfg, "cov_width", self._private_param("robot/cov_width", 1.0)),
            1.0,
        )
        width = _positive_float(
            self._cfg_value(cfg, "width", self._private_param("robot/width", cov_width)),
            cov_width,
        )
        min_turning_radius = _positive_float(
            self._cfg_value(
                cfg,
                "min_turning_radius",
                self._private_param("robot/min_turning_radius", 0.8),
            ),
            0.8,
        )
        max_diff_curv = _finite_float(
            self._cfg_value(cfg, "max_diff_curv", self._private_param("robot/max_diff_curv", 0.2)),
            0.2,
        )
        return RobotSpec(
            cov_width=cov_width,
            width=width,
            min_turning_radius=min_turning_radius,
            max_diff_curv=max_diff_curv,
        )

    def _load_planner_defaults(self) -> PlannerParams:
        cfg = self._param_dict("planner")
        return PlannerParams(
            split_angle_deg=_finite_float(
                self._cfg_value(cfg, "split_angle_deg", self._private_param("split_angle_deg", PlannerParams.split_angle_deg)),
                PlannerParams.split_angle_deg,
            ),
            turn_model=str(
                self._cfg_value(cfg, "turn_model", self._private_param("turn_model", PlannerParams.turn_model))
                or PlannerParams.turn_model
            ),
            viz_step_m=_positive_float(
                self._cfg_value(cfg, "viz_step_m", self._private_param("viz_step_m", PlannerParams.viz_step_m)),
                PlannerParams.viz_step_m,
            ),
            path_step_m=_positive_float(
                self._cfg_value(cfg, "path_step_m", self._private_param("path_step_m", PlannerParams.path_step_m)),
                PlannerParams.path_step_m,
            ),
            turn_step_m=_positive_float(
                self._cfg_value(cfg, "turn_step_m", self._private_param("turn_step_m", PlannerParams.turn_step_m)),
                PlannerParams.turn_step_m,
            ),
            line_w=_positive_float(
                self._cfg_value(cfg, "line_w", self._private_param("line_w", PlannerParams.line_w)),
                PlannerParams.line_w,
            ),
            mute_stderr=bool(
                self._cfg_value(cfg, "mute_stderr", self._private_param("mute_stderr", PlannerParams.mute_stderr))
            ),
            validate_effective_region_path=bool(
                self._cfg_value(
                    cfg,
                    "validate_effective_region_path",
                    self._private_param(
                        "validate_effective_region_path",
                        PlannerParams.validate_effective_region_path,
                    ),
                )
            ),
            wall_margin_m=_finite_float(
                self._cfg_value(cfg, "wall_margin_m", self._private_param("wall_margin_m", PlannerParams.wall_margin_m)),
                PlannerParams.wall_margin_m,
            ),
            turn_margin_m=_finite_float(
                self._cfg_value(cfg, "turn_margin_m", self._private_param("turn_margin_m", PlannerParams.turn_margin_m)),
                PlannerParams.turn_margin_m,
            ),
            min_plannable_span_m=_finite_float(
                self._cfg_value(
                    cfg,
                    "min_plannable_span_m",
                    self._private_param("min_plannable_span_m", PlannerParams.min_plannable_span_m),
                ),
                PlannerParams.min_plannable_span_m,
            ),
            edge_corner_radius_m=_finite_float(
                self._cfg_value(
                    cfg,
                    "edge_corner_radius_m",
                    self._private_param("edge_corner_radius_m", PlannerParams.edge_corner_radius_m),
                ),
                PlannerParams.edge_corner_radius_m,
            ),
            edge_corner_pull=_finite_float(
                self._cfg_value(cfg, "edge_corner_pull", self._private_param("edge_corner_pull", PlannerParams.edge_corner_pull)),
                PlannerParams.edge_corner_pull,
            ),
            edge_corner_min_pts=_positive_int(
                self._cfg_value(
                    cfg,
                    "edge_corner_min_pts",
                    self._private_param("edge_corner_min_pts", PlannerParams.edge_corner_min_pts),
                ),
                PlannerParams.edge_corner_min_pts,
            ),
            pre_proj_min=_finite_float(
                self._cfg_value(cfg, "pre_proj_min", self._private_param("pre_proj_min", PlannerParams.pre_proj_min)),
                PlannerParams.pre_proj_min,
            ),
            pre_proj_max=_finite_float(
                self._cfg_value(cfg, "pre_proj_max", self._private_param("pre_proj_max", PlannerParams.pre_proj_max)),
                PlannerParams.pre_proj_max,
            ),
            pre_prefix_max=_finite_float(
                self._cfg_value(cfg, "pre_prefix_max", self._private_param("pre_prefix_max", PlannerParams.pre_prefix_max)),
                PlannerParams.pre_prefix_max,
            ),
            e_pre_min=_finite_float(
                self._cfg_value(cfg, "e_pre_min", self._private_param("e_pre_min", PlannerParams.e_pre_min)),
                PlannerParams.e_pre_min,
            ),
            e_pre_max=_finite_float(
                self._cfg_value(cfg, "e_pre_max", self._private_param("e_pre_max", PlannerParams.e_pre_max)),
                PlannerParams.e_pre_max,
            ),
        )

    def _empty_alignment_resp(self, success: bool, message: str) -> OperateMapAlignmentResponse:
        return OperateMapAlignmentResponse(
            success=bool(success),
            message=str(message or ""),
            config=MapAlignmentConfig(),
            configs=[],
        )

    def _resolve_asset(self, map_name: str = "", map_revision_id: str = "") -> Dict[str, object]:
        normalized_map_name = str(map_name or "").strip()
        if normalized_map_name.endswith(".pbstream"):
            normalized_map_name = normalized_map_name[:-len(".pbstream")]
        normalized_map_revision_id = str(map_revision_id or "").strip()
        if normalized_map_revision_id:
            asset = self.store.resolve_map_asset(
                map_name=normalized_map_name,
                revision_id=normalized_map_revision_id,
                robot_id=self.robot_id,
            )
        elif normalized_map_name:
            resolve_revision = getattr(self.store, "resolve_map_revision", None)
            if callable(resolve_revision):
                asset = resolve_revision(
                    map_name=normalized_map_name,
                    robot_id=self.robot_id,
                )
            else:
                active_asset = self.store.get_active_map(robot_id=self.robot_id) or {}
                active_map_name = str((active_asset or {}).get("map_name") or "").strip()
                active_revision_id = str((active_asset or {}).get("revision_id") or "").strip()
                if active_revision_id and active_map_name == normalized_map_name:
                    asset = self.store.resolve_map_asset(
                        revision_id=active_revision_id,
                        robot_id=self.robot_id,
                    ) or active_asset
                else:
                    asset = self.store.resolve_map_asset(
                        map_name=normalized_map_name,
                        revision_id="",
                        robot_id=self.robot_id,
                    )
        else:
            asset = self.store.resolve_map_asset(
                map_name=normalized_map_name,
                revision_id=normalized_map_revision_id,
                robot_id=self.robot_id,
            )
        error = map_asset_verification_error(asset, label="map asset")
        if error:
            raise ValueError(error)
        resolved_map_name = str((asset or {}).get("map_name") or "").strip()
        if normalized_map_name and resolved_map_name and resolved_map_name != normalized_map_name:
            raise ValueError("map revision does not match selected map")
        return asset

    def _resolve_alignment(self, asset: Dict[str, object], alignment_version: str = "") -> MapAlignment:
        row = self.store.get_map_alignment_config(
            map_name=str(asset.get("map_name") or ""),
            map_revision_id=str(asset.get("revision_id") or ""),
            alignment_version=str(alignment_version or "").strip(),
            active_only=(not str(alignment_version or "").strip()),
        )
        if not row:
            raise ValueError("active map alignment config not found")
        asset_revision_id = str(asset.get("revision_id") or "").strip()
        row_revision_id = str(row.get("map_revision_id") or "").strip()
        revision_scoped_match = bool(asset_revision_id and row_revision_id and asset_revision_id == row_revision_id)
        if (not revision_scoped_match) and str(asset.get("map_id") or "").strip() and str(row.get("map_id") or "").strip():
            if str(asset.get("map_id") or "").strip() != str(row.get("map_id") or "").strip():
                raise ValueError("alignment config map_id does not match selected map")
        if (not revision_scoped_match) and str(asset.get("map_md5") or "").strip() and str(row.get("map_version") or "").strip():
            if str(asset.get("map_md5") or "").strip() != str(row.get("map_version") or "").strip():
                raise ValueError("alignment config map_version does not match selected map")
        return MapAlignment.from_dict(
            row,
            default_raw_frame=str(asset.get("frame_id") or "map"),
            default_aligned_frame=self.default_aligned_frame,
        )

    def _identity_alignment(self, asset: Dict[str, object]) -> MapAlignment:
        raw_frame = str(asset.get("frame_id") or "map").strip() or "map"
        return MapAlignment(
            map_name=str(asset.get("map_name") or ""),
            map_id=str(asset.get("map_id") or ""),
            map_version=str(asset.get("map_md5") or ""),
            alignment_version="",
            raw_frame=raw_frame,
            aligned_frame=raw_frame,
            yaw_offset_deg=0.0,
            pivot_x=0.0,
            pivot_y=0.0,
            source="identity_raw_map",
            status="active",
            active=False,
        )

    def _resolve_alignment_for_region(
        self,
        asset: Dict[str, object],
        *,
        alignment_version: str = "",
        region_frame_id: str = "",
    ) -> MapAlignment:
        raw_frame = str(asset.get("frame_id") or "map").strip() or "map"
        region_frame_id = str(region_frame_id or "").strip()
        if str(alignment_version or "").strip():
            return self._resolve_alignment(asset, alignment_version)
        if region_frame_id and region_frame_id == raw_frame:
            return self._identity_alignment(asset)
        return self._resolve_alignment(asset, "")

    def _resolve_alignment_for_frame(
        self,
        asset: Dict[str, object],
        *,
        alignment_version: str = "",
        frame_id: str = "",
    ) -> MapAlignment:
        return self._resolve_alignment_for_region(asset, alignment_version=alignment_version, region_frame_id=frame_id)

    def _no_go_item_to_msg(
        self,
        *,
        asset: Dict[str, object],
        raw: Dict[str, object],
        alignment_version: str = "",
    ) -> MapNoGoArea:
        map_name = str(asset.get("map_name") or "")
        raw_frame = str(asset.get("frame_id") or "map")
        polygon = _open_ring(list(raw.get("polygon") or []))
        polygon, _ = self._normalize_region(polygon, [])

        warnings: List[str] = []
        alignment: Optional[MapAlignment] = None
        requested_alignment = str(alignment_version or "").strip()
        if requested_alignment:
            alignment = self._resolve_alignment(asset, requested_alignment)
        else:
            try:
                alignment = self._resolve_alignment(asset, "")
            except Exception:
                alignment = None

        if alignment is not None:
            display_outer = map_to_aligned_polygon(polygon, alignment)
            display_outer, _ = self._normalize_region(display_outer, [])
            display_frame = str(alignment.aligned_frame or self.default_aligned_frame)
            effective_alignment_version = str(alignment.alignment_version or "")
        else:
            display_outer = list(polygon)
            display_frame = raw_frame
            effective_alignment_version = ""
            warnings.append("using raw map frame for display region")

        msg = MapNoGoArea()
        msg.map_name = map_name
        msg.map_revision_id = str(asset.get("revision_id") or "")
        msg.area_id = str(raw.get("area_id") or raw.get("id") or "")
        msg.display_name = str(raw.get("name") or "")
        msg.enabled = bool(raw.get("enabled", True))
        msg.alignment_version = effective_alignment_version
        msg.display_frame = display_frame
        msg.storage_frame = raw_frame
        msg.map_id = str(asset.get("map_id") or "")
        msg.map_version = str(asset.get("map_md5") or "")
        msg.display_region = _lists_to_region(display_frame, display_outer, [])
        msg.map_region = _lists_to_region(raw_frame, polygon, [])
        msg.updated_ts = 0.0
        msg.warnings = [str(w) for w in warnings]
        return msg

    def _virtual_wall_item_to_msg(
        self,
        *,
        asset: Dict[str, object],
        raw: Dict[str, object],
        alignment_version: str = "",
    ) -> MapVirtualWall:
        map_name = str(asset.get("map_name") or "")
        raw_frame = str(asset.get("frame_id") or "map")
        polyline = self._normalize_polyline(list(raw.get("polyline") or []))

        warnings: List[str] = []
        alignment: Optional[MapAlignment] = None
        requested_alignment = str(alignment_version or "").strip()
        if requested_alignment:
            alignment = self._resolve_alignment(asset, requested_alignment)
        else:
            try:
                alignment = self._resolve_alignment(asset, "")
            except Exception:
                alignment = None

        if alignment is not None:
            display_path = [map_to_aligned_point(x, y, alignment) for x, y in polyline]
            display_path = self._normalize_polyline(display_path)
            display_frame = str(alignment.aligned_frame or self.default_aligned_frame)
            effective_alignment_version = str(alignment.alignment_version or "")
        else:
            display_path = list(polyline)
            display_frame = raw_frame
            effective_alignment_version = ""
            warnings.append("using raw map frame for display path")

        msg = MapVirtualWall()
        msg.map_name = map_name
        msg.map_revision_id = str(asset.get("revision_id") or "")
        msg.wall_id = str(raw.get("wall_id") or raw.get("id") or "")
        msg.display_name = str(raw.get("name") or "")
        msg.enabled = bool(raw.get("enabled", True))
        msg.alignment_version = effective_alignment_version
        msg.display_frame = display_frame
        msg.storage_frame = raw_frame
        msg.map_id = str(asset.get("map_id") or "")
        msg.map_version = str(asset.get("map_md5") or "")
        msg.display_path = _xy_to_ring(display_path)
        msg.map_path = _xy_to_ring(polyline)
        msg.buffer_m = float(raw.get("buffer_m", self.default_virtual_wall_buffer_m) or self.default_virtual_wall_buffer_m)
        msg.updated_ts = 0.0
        msg.warnings = [str(w) for w in warnings]
        return msg

    def _alignment_to_msg(self, cfg: Optional[Dict[str, object]], *, asset: Optional[Dict[str, object]] = None) -> MapAlignmentConfig:
        msg = MapAlignmentConfig()
        if not cfg:
            return msg
        msg.map_name = str(cfg.get("map_name") or "")
        msg.map_revision_id = str((asset or {}).get("revision_id") or cfg.get("map_revision_id") or "")
        msg.map_id = str(cfg.get("map_id") or "")
        msg.map_version = str(cfg.get("map_version") or "")
        msg.alignment_version = str(cfg.get("alignment_version") or "")
        msg.raw_frame = str(cfg.get("raw_frame") or "")
        msg.aligned_frame = str(cfg.get("aligned_frame") or "")
        msg.yaw_offset_deg = float(cfg.get("yaw_offset_deg") or 0.0)
        msg.pivot_x = float(cfg.get("pivot_x") or 0.0)
        msg.pivot_y = float(cfg.get("pivot_y") or 0.0)
        msg.source = str(cfg.get("source") or "")
        msg.status = str(cfg.get("status") or "")
        msg.active = bool(cfg.get("active", False))
        msg.created_ts = float(cfg.get("created_ts") or 0.0)
        msg.updated_ts = float(cfg.get("updated_ts") or 0.0)
        return msg

    def _zone_to_msg(
        self,
        *,
        asset: Dict[str, object],
        zone_meta: Dict[str, object],
        alignment_version: str = "",
        plan_profile_name: str = "",
    ) -> CoverageZone:
        msg = CoverageZone()
        map_name = str(zone_meta.get("map_name") or asset.get("map_name") or "")
        zone_id = str(zone_meta.get("zone_id") or "")
        zone_version = int(zone_meta.get("zone_version") or zone_meta.get("current_zone_version") or 0)
        raw_frame = str(zone_meta.get("frame_id") or asset.get("frame_id") or "map")
        map_outer = _open_ring(json.loads(str(zone_meta.get("outer_json") or "[]") or "[]"))
        map_holes = [_open_ring(ring) for ring in (json.loads(str(zone_meta.get("holes_json") or "[]") or "[]") or [])]
        map_outer, map_holes = self._normalize_region(map_outer, map_holes)

        editor = self.store.get_zone_editor_metadata(
            map_name=map_name,
            map_revision_id=str(asset.get("revision_id") or ""),
            zone_id=zone_id,
            zone_version=zone_version,
        )
        requested_alignment_version = str(alignment_version or "").strip()
        alignment: Optional[MapAlignment] = None
        if requested_alignment_version:
            alignment = self._resolve_alignment(asset, requested_alignment_version)
        elif not editor:
            try:
                alignment = self._resolve_alignment(asset, "")
            except Exception:
                alignment = None

        warnings: List[str] = []
        display_outer: List[XY]
        display_holes: List[List[XY]]
        display_frame = raw_frame
        effective_alignment_version = ""
        estimated_length_m = 0.0
        estimated_duration_s = 0.0
        editor_profile_name = ""
        resolved_plan_profile_name = ""

        if editor:
            warnings.extend([str(w) for w in (editor.get("warnings") or [])])
            estimated_length_m = float(editor.get("estimated_length_m") or 0.0)
            estimated_duration_s = float(editor.get("estimated_duration_s") or 0.0)
            editor_profile_name = str(editor.get("profile_name") or "")

        if editor and (not requested_alignment_version or str(editor.get("alignment_version") or "") == requested_alignment_version):
            display_outer = _open_ring(editor.get("display_outer") or [])
            display_holes = [_open_ring(ring) for ring in (editor.get("display_holes") or [])]
            display_outer, display_holes = self._normalize_region(display_outer, display_holes)
            display_frame = str(editor.get("display_frame") or "") or (
                str(alignment.aligned_frame) if alignment is not None else self.default_aligned_frame
            )
            effective_alignment_version = str(editor.get("alignment_version") or "")
        elif alignment is not None:
            display_outer = map_to_aligned_polygon(map_outer, alignment)
            display_holes = [map_to_aligned_polygon(ring, alignment) for ring in (map_holes or [])]
            display_outer, display_holes = self._normalize_region(display_outer, display_holes)
            display_frame = str(alignment.aligned_frame or self.default_aligned_frame)
            effective_alignment_version = str(alignment.alignment_version or "")
            warnings.append("display region derived from current alignment")
        else:
            display_outer = list(map_outer)
            display_holes = [list(ring) for ring in (map_holes or [])]
            display_frame = raw_frame
            effective_alignment_version = str(editor.get("alignment_version") or "") if editor else ""
            warnings.append("using raw map frame for display region")

        preferred_profile = str(plan_profile_name or "").strip() or str(editor_profile_name or "").strip()
        active_plan_refs = self.store.list_active_plan_refs(
            zone_id,
            map_name=map_name,
            map_revision_id=str(asset.get("revision_id") or ""),
        )
        if preferred_profile:
            active_plan_id = self.store.get_active_plan_id(
                zone_id,
                preferred_profile,
                map_name=map_name,
                map_revision_id=str(asset.get("revision_id") or ""),
            )
            if active_plan_id and not resolved_plan_profile_name:
                resolved_plan_profile_name = preferred_profile
        elif len(active_plan_refs) == 1:
            active_plan_id = str(active_plan_refs[0].get("active_plan_id") or "")
            resolved_plan_profile_name = str(
                active_plan_refs[0].get("plan_profile_name") or editor_profile_name or ""
            )
        elif len(active_plan_refs) > 1:
            active_plan_id = str(active_plan_refs[0].get("active_plan_id") or "")
            resolved_plan_profile_name = str(
                active_plan_refs[0].get("plan_profile_name") or editor_profile_name or ""
            )
            warnings.append("multiple active plan profiles exist; returning latest active plan")
        else:
            active_plan_id = ""

        if active_plan_id:
            try:
                plan_meta = self.store.load_plan_meta(active_plan_id)
                if not resolved_plan_profile_name:
                    resolved_plan_profile_name = str(plan_meta.get("plan_profile_name") or "")
                if estimated_length_m <= 0.0:
                    estimated_length_m = float(plan_meta.get("total_length_m") or 0.0)
                if estimated_duration_s <= 0.0 and estimated_length_m > 0.0 and self.preview_nominal_speed_mps > 1e-6:
                    estimated_duration_s = float(estimated_length_m / self.preview_nominal_speed_mps)
                if int(plan_meta.get("zone_version") or 0) != int(zone_version):
                    warnings.append("active plan belongs to a different zone version")
            except Exception:
                pass

        msg.map_name = map_name
        msg.map_revision_id = str(asset.get("revision_id") or "")
        msg.zone_id = zone_id
        msg.display_name = str(zone_meta.get("display_name") or zone_id)
        msg.enabled = bool(zone_meta.get("enabled", False))
        msg.zone_version = int(zone_version)
        msg.alignment_version = str(effective_alignment_version or "")
        msg.display_frame = str(display_frame or "")
        msg.storage_frame = str(raw_frame or "")
        msg.map_id = str(zone_meta.get("map_id") or asset.get("map_id") or "")
        msg.map_version = str(zone_meta.get("map_md5") or asset.get("map_md5") or "")
        msg.active_plan_id = str(active_plan_id or "")
        msg.plan_profile_name = str(resolved_plan_profile_name or "")
        msg.estimated_length_m = float(estimated_length_m or 0.0)
        msg.estimated_duration_s = float(estimated_duration_s or 0.0)
        msg.warnings = [str(w) for w in warnings]
        msg.display_region = _lists_to_region(str(display_frame or ""), display_outer, display_holes)
        msg.map_region = _lists_to_region(str(raw_frame or ""), map_outer, map_holes)
        msg.updated_ts = float(zone_meta.get("updated_ts") or 0.0)
        return msg

    def _empty_zone_resp(self, success: bool, message: str) -> OperateCoverageZoneResponse:
        return OperateCoverageZoneResponse(
            success=bool(success),
            message=str(message or ""),
            zone=CoverageZone(),
            zones=[],
        )

    def _empty_zone_plan_path_resp(self, success: bool, message: str) -> GetZonePlanPathResponse:
        return GetZonePlanPathResponse(
            success=bool(success),
            message=str(message or ""),
            map_revision_id="",
            zone_id="",
            active_plan_id="",
            plan_profile_name="",
            alignment_version="",
            display_frame="",
            storage_frame="",
            display_path=PolygonRing(),
            map_path=PolygonRing(),
            display_entry_pose=Pose2D(),
            entry_pose=Pose2D(),
            estimated_length_m=0.0,
            estimated_duration_s=0.0,
            warnings=[],
        )

    def _load_plan_overlay(self, plan_id: str) -> Dict[str, object]:
        plan_meta = self.store.load_plan_meta(plan_id)
        order_raw = list(plan_meta.get("exec_order_json") or [])
        if order_raw:
            order = [int(x) for x in order_raw]
        else:
            rows = self.store.conn.execute(
                "SELECT block_id FROM plan_blocks WHERE plan_id=? ORDER BY block_id ASC;",
                (str(plan_id or "").strip(),),
            ).fetchall()
            order = [int(row["block_id"]) for row in (rows or [])]

        path_xy: List[XY] = []
        entry_pose = Pose2D()
        first_entry = True
        for block_id in order:
            block = self.store.load_block(plan_id, int(block_id))
            pts_xyyaw = list(block.get("path_xyyaw") or [])
            if not pts_xyyaw:
                continue
            pts_xy = [(float(p[0]), float(p[1])) for p in pts_xyyaw if len(p) >= 2]
            if not pts_xy:
                continue
            if first_entry:
                fallback_yaw = float(pts_xyyaw[0][2]) if len(pts_xyyaw[0]) >= 3 else 0.0
                entry_yaw = block.get("entry_yaw")
                if entry_yaw is None:
                    entry_yaw = fallback_yaw
                entry_pose = _pose2d(
                    float(block.get("entry_x") or pts_xyyaw[0][0]),
                    float(block.get("entry_y") or pts_xyyaw[0][1]),
                    float(entry_yaw),
                )
                first_entry = False
            if path_xy and pts_xy and abs(path_xy[-1][0] - pts_xy[0][0]) < 1e-6 and abs(path_xy[-1][1] - pts_xy[0][1]) < 1e-6:
                path_xy.extend(pts_xy[1:])
            else:
                path_xy.extend(pts_xy)

        return {
            "path_xy": path_xy,
            "entry_pose": entry_pose,
            "estimated_length_m": float(plan_meta.get("total_length_m") or 0.0),
            "plan_profile_name": str(plan_meta.get("plan_profile_name") or ""),
            "zone_version": int(plan_meta.get("zone_version") or 0),
        }

    def _empty_no_go_resp(
        self,
        success: bool,
        message: str,
        *,
        constraint_version: str = "",
        area: Optional[MapNoGoArea] = None,
        areas: Optional[List[MapNoGoArea]] = None,
        warnings: Optional[List[str]] = None,
    ) -> OperateMapNoGoAreaResponse:
        return OperateMapNoGoAreaResponse(
            success=bool(success),
            message=str(message or ""),
            constraint_version=str(constraint_version or ""),
            area=area if isinstance(area, MapNoGoArea) else MapNoGoArea(),
            areas=list(areas or []),
            warnings=[str(w) for w in (warnings or [])],
        )

    def _empty_wall_resp(
        self,
        success: bool,
        message: str,
        *,
        constraint_version: str = "",
        wall: Optional[MapVirtualWall] = None,
        walls: Optional[List[MapVirtualWall]] = None,
        warnings: Optional[List[str]] = None,
    ) -> OperateMapVirtualWallResponse:
        return OperateMapVirtualWallResponse(
            success=bool(success),
            message=str(message or ""),
            constraint_version=str(constraint_version or ""),
            wall=wall if isinstance(wall, MapVirtualWall) else MapVirtualWall(),
            walls=list(walls or []),
            warnings=[str(w) for w in (warnings or [])],
        )

    def _normalize_polyline(self, points: Sequence[Sequence[float]]) -> List[XY]:
        out: List[XY] = []
        for pt in points or []:
            if pt is None or len(pt) < 2:
                continue
            x = float(pt[0])
            y = float(pt[1])
            if (not math.isfinite(x)) or (not math.isfinite(y)):
                continue
            if out and abs(out[-1][0] - x) < 1e-9 and abs(out[-1][1] - y) < 1e-9:
                continue
            out.append((x, y))
        return out

    def _line_to_map_and_display(
        self,
        *,
        asset: Dict[str, object],
        alignment: MapAlignment,
        frame_id: str,
        points: Sequence[Sequence[float]],
    ) -> Tuple[List[XY], List[XY]]:
        raw_frame = str(alignment.raw_frame or asset.get("frame_id") or "map")
        aligned_frame = str(alignment.aligned_frame or self.default_aligned_frame)
        frame = str(frame_id or "").strip() or aligned_frame
        pts = self._normalize_polyline(points)
        if len(pts) < 2:
            raise ValueError("polyline must contain at least 2 points")
        if frame == aligned_frame:
            disp_pts = pts
            map_pts = [aligned_to_map_point(x, y, alignment) for x, y in disp_pts]
        elif frame == raw_frame or frame == str(asset.get("frame_id") or raw_frame):
            map_pts = pts
            disp_pts = [map_to_aligned_point(x, y, alignment) for x, y in map_pts]
        else:
            raise ValueError("unsupported path.frame_id=%s" % frame)
        return self._normalize_polyline(map_pts), self._normalize_polyline(disp_pts)

    def _load_constraint_snapshot(self, asset: Dict[str, object], *, create_if_missing: bool = False) -> Dict[str, object]:
        return self.store.load_map_constraints(
            map_id=str(asset.get("map_id") or ""),
            map_revision_id=str(asset.get("revision_id") or ""),
            map_md5_hint=str(asset.get("map_md5") or ""),
            create_if_missing=bool(create_if_missing),
        )

    def _save_constraint_snapshot(
        self,
        asset: Dict[str, object],
        *,
        no_go_areas: List[Dict[str, object]],
        virtual_walls: List[Dict[str, object]],
    ) -> str:
        return self.store.replace_map_constraints(
            map_id=str(asset.get("map_id") or ""),
            map_revision_id=str(asset.get("revision_id") or ""),
            map_md5=str(asset.get("map_md5") or ""),
            no_go_areas=no_go_areas,
            virtual_walls=virtual_walls,
        )

    def _no_go_item_to_msg(
        self,
        *,
        asset: Dict[str, object],
        raw: Dict[str, object],
        alignment_version: str = "",
    ) -> MapNoGoArea:
        map_name = str(asset.get("map_name") or "")
        raw_frame = str(asset.get("frame_id") or "map")
        polygon = _open_ring(list(raw.get("polygon") or []))
        polygon, _ = self._normalize_region(polygon, [])

        warnings: List[str] = []
        alignment: Optional[MapAlignment] = None
        requested_alignment = str(alignment_version or "").strip()
        if requested_alignment:
            alignment = self._resolve_alignment(asset, requested_alignment)
        else:
            try:
                alignment = self._resolve_alignment(asset, "")
            except Exception:
                alignment = None

        if alignment is not None:
            display_outer = map_to_aligned_polygon(polygon, alignment)
            display_outer, _ = self._normalize_region(display_outer, [])
            display_frame = str(alignment.aligned_frame or self.default_aligned_frame)
            effective_alignment_version = str(alignment.alignment_version or "")
        else:
            display_outer = list(polygon)
            display_frame = raw_frame
            effective_alignment_version = ""
            warnings.append("using raw map frame for display region")

        msg = MapNoGoArea()
        msg.map_name = map_name
        msg.map_revision_id = str(asset.get("revision_id") or "")
        msg.area_id = str(raw.get("area_id") or raw.get("id") or "")
        msg.display_name = str(raw.get("name") or "")
        msg.enabled = bool(raw.get("enabled", True))
        msg.alignment_version = effective_alignment_version
        msg.display_frame = display_frame
        msg.storage_frame = raw_frame
        msg.map_id = str(asset.get("map_id") or "")
        msg.map_version = str(asset.get("map_md5") or "")
        msg.display_region = _lists_to_region(display_frame, display_outer, [])
        msg.map_region = _lists_to_region(raw_frame, polygon, [])
        msg.updated_ts = 0.0
        msg.warnings = [str(w) for w in warnings]
        return msg

    def _virtual_wall_item_to_msg(
        self,
        *,
        asset: Dict[str, object],
        raw: Dict[str, object],
        alignment_version: str = "",
    ) -> MapVirtualWall:
        map_name = str(asset.get("map_name") or "")
        raw_frame = str(asset.get("frame_id") or "map")
        polyline = self._normalize_polyline(list(raw.get("polyline") or []))

        warnings: List[str] = []
        alignment: Optional[MapAlignment] = None
        requested_alignment = str(alignment_version or "").strip()
        if requested_alignment:
            alignment = self._resolve_alignment(asset, requested_alignment)
        else:
            try:
                alignment = self._resolve_alignment(asset, "")
            except Exception:
                alignment = None

        if alignment is not None:
            display_path = [map_to_aligned_point(x, y, alignment) for x, y in polyline]
            display_path = self._normalize_polyline(display_path)
            display_frame = str(alignment.aligned_frame or self.default_aligned_frame)
            effective_alignment_version = str(alignment.alignment_version or "")
        else:
            display_path = list(polyline)
            display_frame = raw_frame
            effective_alignment_version = ""
            warnings.append("using raw map frame for display path")

        msg = MapVirtualWall()
        msg.map_name = map_name
        msg.map_revision_id = str(asset.get("revision_id") or "")
        msg.wall_id = str(raw.get("wall_id") or raw.get("id") or "")
        msg.display_name = str(raw.get("name") or "")
        msg.enabled = bool(raw.get("enabled", True))
        msg.alignment_version = effective_alignment_version
        msg.display_frame = display_frame
        msg.storage_frame = raw_frame
        msg.map_id = str(asset.get("map_id") or "")
        msg.map_version = str(asset.get("map_md5") or "")
        msg.display_path = _xy_to_ring(display_path)
        msg.map_path = _xy_to_ring(polyline)
        msg.buffer_m = float(raw.get("buffer_m", self.default_virtual_wall_buffer_m) or self.default_virtual_wall_buffer_m)
        msg.updated_ts = 0.0
        msg.warnings = [str(w) for w in warnings]
        return msg

    def _build_no_go_raw_item(
        self,
        *,
        asset: Dict[str, object],
        req_alignment_version: str,
        msg: MapNoGoArea,
        area_id: str,
        existing: Optional[Dict[str, object]] = None,
    ) -> Dict[str, object]:
        region = None
        if len(list(getattr(msg.display_region.outer, "points", []) or [])) >= 3:
            region = msg.display_region
        elif len(list(getattr(msg.map_region.outer, "points", []) or [])) >= 3:
            region = msg.map_region
        if region is None:
            raise ValueError("area.display_region or area.map_region is required")
        if list(region.holes or []):
            raise ValueError("no-go area does not support holes in v1")
        alignment = self._resolve_alignment_for_region(
            asset,
            alignment_version=str(req_alignment_version or msg.alignment_version or "").strip(),
            region_frame_id=str(region.frame_id or "").strip(),
        )
        map_outer, map_holes, _disp_outer, _disp_holes = self._region_to_map_and_display(
            asset=asset,
            alignment=alignment,
            region=region,
        )
        if map_holes:
            raise ValueError("no-go area does not support holes in v1")
        return {
            "area_id": str(area_id or "").strip(),
            "name": str(msg.display_name or (existing or {}).get("name") or area_id).strip(),
            "polygon": [[float(x), float(y)] for x, y in map_outer],
            "enabled": bool(msg.enabled if existing is None else msg.enabled),
        }

    def _build_virtual_wall_raw_item(
        self,
        *,
        asset: Dict[str, object],
        req_alignment_version: str,
        msg: MapVirtualWall,
        wall_id: str,
        existing: Optional[Dict[str, object]] = None,
    ) -> Dict[str, object]:
        points: List[XY] = []
        frame_id = ""
        if len(list(getattr(msg.display_path, "points", []) or [])) >= 2:
            points = _ring_to_xy(msg.display_path)
            frame_id = str(msg.display_frame or "").strip()
        elif len(list(getattr(msg.map_path, "points", []) or [])) >= 2:
            points = _ring_to_xy(msg.map_path)
            frame_id = str(msg.storage_frame or "").strip()
        if len(points) < 2:
            raise ValueError("wall.display_path or wall.map_path is required")
        alignment = self._resolve_alignment_for_frame(
            asset,
            alignment_version=str(req_alignment_version or msg.alignment_version or "").strip(),
            frame_id=str(frame_id or "").strip(),
        )
        map_pts, _disp_pts = self._line_to_map_and_display(
            asset=asset,
            alignment=alignment,
            frame_id=str(frame_id or "").strip(),
            points=points,
        )
        return {
            "wall_id": str(wall_id or "").strip(),
            "name": str(msg.display_name or (existing or {}).get("name") or wall_id).strip(),
            "polyline": [[float(x), float(y)] for x, y in map_pts],
            "buffer_m": _positive_float(
                getattr(msg, "buffer_m", (existing or {}).get("buffer_m", self.default_virtual_wall_buffer_m)),
                self.default_virtual_wall_buffer_m,
            ),
            "enabled": bool(msg.enabled if existing is None else msg.enabled),
        }

    def _handle_no_go_area(self, req):
        op = int(req.operation)
        req_map_name = str(req.map_name or "").strip()
        req_map_revision_id = str(getattr(req, "map_revision_id", "") or getattr(req.area, "map_revision_id", "") or "").strip()
        req_alignment_version = str(req.alignment_version or "").strip()
        try:
            asset = self._resolve_asset(req_map_name, req_map_revision_id)
        except Exception as e:
            return self._empty_no_go_resp(False, str(e))

        if op == int(req.getAll):
            try:
                raw = self._load_constraint_snapshot(asset, create_if_missing=False)
                items = []
                for area in list(raw.get("no_go_areas") or []):
                    if (not bool(req.include_disabled)) and (not bool(area.get("enabled", True))):
                        continue
                    items.append(self._no_go_item_to_msg(asset=asset, raw=area, alignment_version=req_alignment_version))
                items.sort(key=lambda x: (str(x.display_name or x.area_id), str(x.area_id)))
                return self._empty_no_go_resp(True, "ok", constraint_version=str(raw.get("constraint_version") or ""), areas=items)
            except Exception as e:
                return self._empty_no_go_resp(False, str(e))

        target_area_id = str(req.area_id or getattr(req.area, "area_id", "") or "").strip()
        if op == int(req.get):
            try:
                if not target_area_id:
                    return self._empty_no_go_resp(False, "area_id is required")
                raw = self._load_constraint_snapshot(asset, create_if_missing=False)
                for area in list(raw.get("no_go_areas") or []):
                    if str(area.get("area_id") or "") == target_area_id:
                        return self._empty_no_go_resp(
                            True,
                            "ok",
                            constraint_version=str(raw.get("constraint_version") or ""),
                            area=self._no_go_item_to_msg(asset=asset, raw=area, alignment_version=req_alignment_version),
                        )
                return self._empty_no_go_resp(False, "area not found")
            except Exception as e:
                return self._empty_no_go_resp(False, str(e))

        if op in (int(req.add), int(req.modify), int(req.Delete)):
            try:
                raw = self._load_constraint_snapshot(asset, create_if_missing=True)
                no_go_areas = [dict(x) for x in (raw.get("no_go_areas") or [])]
                virtual_walls = [dict(x) for x in (raw.get("virtual_walls") or [])]
                idx = -1
                for i, area in enumerate(no_go_areas):
                    if str(area.get("area_id") or "") == target_area_id:
                        idx = i
                        break

                if op == int(req.Delete):
                    if not target_area_id:
                        return self._empty_no_go_resp(False, "area_id is required")
                    if idx < 0:
                        return self._empty_no_go_resp(False, "area not found")
                    del no_go_areas[idx]
                    new_version = self._save_constraint_snapshot(asset, no_go_areas=no_go_areas, virtual_walls=virtual_walls)
                    return self._empty_no_go_resp(True, "deleted", constraint_version=new_version)

                if op == int(req.add):
                    area_id = target_area_id or ("no_go_" + uuid.uuid4().hex[:8])
                    if any(str(a.get("area_id") or "") == area_id for a in no_go_areas):
                        return self._empty_no_go_resp(False, "area already exists")
                    item = self._build_no_go_raw_item(
                        asset=asset,
                        req_alignment_version=req_alignment_version,
                        msg=req.area,
                        area_id=area_id,
                    )
                    no_go_areas.append(item)
                    new_version = self._save_constraint_snapshot(asset, no_go_areas=no_go_areas, virtual_walls=virtual_walls)
                    return self._empty_no_go_resp(
                        True,
                        "added",
                        constraint_version=new_version,
                        area=self._no_go_item_to_msg(asset=asset, raw=item, alignment_version=req_alignment_version),
                    )

                if not target_area_id:
                    return self._empty_no_go_resp(False, "area_id is required")
                if idx < 0:
                    return self._empty_no_go_resp(False, "area not found")
                item = self._build_no_go_raw_item(
                    asset=asset,
                    req_alignment_version=req_alignment_version,
                    msg=req.area,
                    area_id=target_area_id,
                    existing=no_go_areas[idx],
                )
                no_go_areas[idx] = item
                new_version = self._save_constraint_snapshot(asset, no_go_areas=no_go_areas, virtual_walls=virtual_walls)
                return self._empty_no_go_resp(
                    True,
                    "modified",
                    constraint_version=new_version,
                    area=self._no_go_item_to_msg(asset=asset, raw=item, alignment_version=req_alignment_version),
                )
            except Exception as e:
                return self._empty_no_go_resp(False, str(e))

        return self._empty_no_go_resp(False, "unsupported operation=%s" % op)

    def _handle_virtual_wall(self, req):
        op = int(req.operation)
        req_map_name = str(req.map_name or "").strip()
        req_map_revision_id = str(getattr(req, "map_revision_id", "") or getattr(req.wall, "map_revision_id", "") or "").strip()
        req_alignment_version = str(req.alignment_version or "").strip()
        try:
            asset = self._resolve_asset(req_map_name, req_map_revision_id)
        except Exception as e:
            return self._empty_wall_resp(False, str(e))

        if op == int(req.getAll):
            try:
                raw = self._load_constraint_snapshot(asset, create_if_missing=False)
                items = []
                for wall in list(raw.get("virtual_walls") or []):
                    if (not bool(req.include_disabled)) and (not bool(wall.get("enabled", True))):
                        continue
                    items.append(self._virtual_wall_item_to_msg(asset=asset, raw=wall, alignment_version=req_alignment_version))
                items.sort(key=lambda x: (str(x.display_name or x.wall_id), str(x.wall_id)))
                return self._empty_wall_resp(True, "ok", constraint_version=str(raw.get("constraint_version") or ""), walls=items)
            except Exception as e:
                return self._empty_wall_resp(False, str(e))

        target_wall_id = str(req.wall_id or getattr(req.wall, "wall_id", "") or "").strip()
        if op == int(req.get):
            try:
                if not target_wall_id:
                    return self._empty_wall_resp(False, "wall_id is required")
                raw = self._load_constraint_snapshot(asset, create_if_missing=False)
                for wall in list(raw.get("virtual_walls") or []):
                    if str(wall.get("wall_id") or "") == target_wall_id:
                        return self._empty_wall_resp(
                            True,
                            "ok",
                            constraint_version=str(raw.get("constraint_version") or ""),
                            wall=self._virtual_wall_item_to_msg(asset=asset, raw=wall, alignment_version=req_alignment_version),
                        )
                return self._empty_wall_resp(False, "wall not found")
            except Exception as e:
                return self._empty_wall_resp(False, str(e))

        if op in (int(req.add), int(req.modify), int(req.Delete)):
            try:
                raw = self._load_constraint_snapshot(asset, create_if_missing=True)
                no_go_areas = [dict(x) for x in (raw.get("no_go_areas") or [])]
                virtual_walls = [dict(x) for x in (raw.get("virtual_walls") or [])]
                idx = -1
                for i, wall in enumerate(virtual_walls):
                    if str(wall.get("wall_id") or "") == target_wall_id:
                        idx = i
                        break

                if op == int(req.Delete):
                    if not target_wall_id:
                        return self._empty_wall_resp(False, "wall_id is required")
                    if idx < 0:
                        return self._empty_wall_resp(False, "wall not found")
                    del virtual_walls[idx]
                    new_version = self._save_constraint_snapshot(asset, no_go_areas=no_go_areas, virtual_walls=virtual_walls)
                    return self._empty_wall_resp(True, "deleted", constraint_version=new_version)

                if op == int(req.add):
                    wall_id = target_wall_id or ("virtual_wall_" + uuid.uuid4().hex[:8])
                    if any(str(w.get("wall_id") or "") == wall_id for w in virtual_walls):
                        return self._empty_wall_resp(False, "wall already exists")
                    item = self._build_virtual_wall_raw_item(
                        asset=asset,
                        req_alignment_version=req_alignment_version,
                        msg=req.wall,
                        wall_id=wall_id,
                    )
                    virtual_walls.append(item)
                    new_version = self._save_constraint_snapshot(asset, no_go_areas=no_go_areas, virtual_walls=virtual_walls)
                    return self._empty_wall_resp(
                        True,
                        "added",
                        constraint_version=new_version,
                        wall=self._virtual_wall_item_to_msg(asset=asset, raw=item, alignment_version=req_alignment_version),
                    )

                if not target_wall_id:
                    return self._empty_wall_resp(False, "wall_id is required")
                if idx < 0:
                    return self._empty_wall_resp(False, "wall not found")
                item = self._build_virtual_wall_raw_item(
                    asset=asset,
                    req_alignment_version=req_alignment_version,
                    msg=req.wall,
                    wall_id=target_wall_id,
                    existing=virtual_walls[idx],
                )
                virtual_walls[idx] = item
                new_version = self._save_constraint_snapshot(asset, no_go_areas=no_go_areas, virtual_walls=virtual_walls)
                return self._empty_wall_resp(
                    True,
                    "modified",
                    constraint_version=new_version,
                    wall=self._virtual_wall_item_to_msg(asset=asset, raw=item, alignment_version=req_alignment_version),
                )
            except Exception as e:
                return self._empty_wall_resp(False, str(e))

        return self._empty_wall_resp(False, "unsupported operation=%s" % op)

    def _normalize_alignment_status(self, status: str) -> str:
        value = str(status or "").strip().lower()
        if value in ("active", "draft", "archived"):
            return value
        return "draft"

    def _next_alignment_version(self) -> str:
        return "align_%s" % uuid.uuid4().hex[:12]

    def _validate_asset_version_binding(self, asset: Dict[str, object], map_version: str) -> str:
        asset_revision_id = str(asset.get("revision_id") or "").strip()
        asset_map_md5 = str(asset.get("map_md5") or "").strip()
        version = str(map_version or "").strip() or asset_map_md5
        if asset_revision_id:
            return asset_map_md5 or version
        if asset_map_md5 and version and version != asset_map_md5:
            raise ValueError("map_version does not match selected map asset")
        return version

    def _handle_alignment(self, req):
        op = int(req.operation)
        req_map_name = str(req.map_name or req.config.map_name or "").strip()
        req_map_revision_id = str(getattr(req, "map_revision_id", "") or getattr(req.config, "map_revision_id", "") or "").strip()

        if op == int(req.getAll):
            try:
                asset = self._resolve_asset(req_map_name, req_map_revision_id) if (req_map_name or req_map_revision_id) else None
                items = self.store.list_map_alignment_configs(
                    map_name=str((asset or {}).get("map_name") or req_map_name),
                    map_revision_id=str((asset or {}).get("revision_id") or ""),
                )
                return OperateMapAlignmentResponse(
                    success=True,
                    message="ok",
                    config=MapAlignmentConfig(),
                    configs=[self._alignment_to_msg(x, asset=asset) for x in items],
                )
            except Exception as e:
                return self._empty_alignment_resp(False, str(e))

        if op == int(req.get):
            try:
                asset = self._resolve_asset(req_map_name, req_map_revision_id)
                item = self.store.get_map_alignment_config(
                    map_name=str(asset.get("map_name") or ""),
                    map_revision_id=str(asset.get("revision_id") or ""),
                    alignment_version=str(req.alignment_version or req.config.alignment_version or "").strip(),
                    active_only=(not str(req.alignment_version or req.config.alignment_version or "").strip()),
                )
                if not item:
                    return self._empty_alignment_resp(False, "alignment config not found")
                return OperateMapAlignmentResponse(
                    success=True,
                    message="ok",
                    config=self._alignment_to_msg(item, asset=asset),
                    configs=[],
                )
            except Exception as e:
                return self._empty_alignment_resp(False, str(e))

        if op in (int(req.add), int(req.modify), int(req.activate), int(req.Delete)):
            try:
                asset = self._resolve_asset(req_map_name, req_map_revision_id)
                map_name = str(asset.get("map_name") or "")
                alignment_version = str(req.alignment_version or req.config.alignment_version or "").strip()
                if op == int(req.add):
                    if not alignment_version:
                        alignment_version = self._next_alignment_version()
                    existing = self.store.get_map_alignment_config(
                        map_name=map_name,
                        map_revision_id=str(asset.get("revision_id") or ""),
                        alignment_version=alignment_version,
                    )
                    if existing:
                        return self._empty_alignment_resp(False, "alignment config already exists")
                elif not alignment_version:
                    return self._empty_alignment_resp(False, "alignment_version is required")

                if op == int(req.Delete):
                    self.store.archive_map_alignment_config(
                        map_name=map_name,
                        map_revision_id=str(asset.get("revision_id") or ""),
                        alignment_version=alignment_version,
                    )
                    return self._empty_alignment_resp(True, "archived")

                map_version = self._validate_asset_version_binding(asset, str(req.config.map_version or ""))
                status = self._normalize_alignment_status(str(req.config.status or ""))
                raw_frame = str(req.config.raw_frame or asset.get("frame_id") or "map").strip() or "map"
                aligned_frame = str(req.config.aligned_frame or self.default_aligned_frame).strip() or self.default_aligned_frame
                cfg = self.store.upsert_map_alignment_config(
                    map_name=map_name,
                    map_revision_id=str(asset.get("revision_id") or ""),
                    alignment_version=alignment_version,
                    map_id=str(asset.get("map_id") or "").strip(),
                    map_version=map_version,
                    raw_frame=raw_frame,
                    aligned_frame=aligned_frame,
                    yaw_offset_deg=float(req.config.yaw_offset_deg or 0.0),
                    pivot_x=float(req.config.pivot_x or 0.0),
                    pivot_y=float(req.config.pivot_y or 0.0),
                    source=str(req.config.source or "").strip(),
                    status=("active" if op == int(req.activate) else status),
                )
                if op == int(req.activate) or str(req.config.status or "").strip().lower() == "active":
                    cfg = self.store.set_active_map_alignment(
                        map_name=map_name,
                        map_revision_id=str(asset.get("revision_id") or ""),
                        alignment_version=alignment_version,
                    ) or cfg
                message = "updated" if op == int(req.modify) else ("activated" if op == int(req.activate) else "created")
                return OperateMapAlignmentResponse(
                    success=True,
                    message=message,
                    config=self._alignment_to_msg(cfg, asset=asset),
                    configs=[],
                )
            except Exception as e:
                return self._empty_alignment_resp(False, str(e))

        return self._empty_alignment_resp(False, "unsupported operation=%s" % op)

    def _handle_alignment_points(self, req):
        try:
            asset = self._resolve_asset(
                str(req.map_name or "").strip(),
                str(getattr(req, "map_revision_id", "") or "").strip(),
            )
            map_name = str(asset.get("map_name") or "")
            alignment_version = str(req.alignment_version or "").strip() or self._next_alignment_version()
            map_version = self._validate_asset_version_binding(asset, "")
            raw_frame = str(req.raw_frame or asset.get("frame_id") or "map").strip() or "map"
            aligned_frame = str(req.aligned_frame or self.default_aligned_frame).strip() or self.default_aligned_frame
            yaw_offset_deg = yaw_offset_deg_from_points((req.p1.x, req.p1.y), (req.p2.x, req.p2.y))
            status = "active" if bool(req.activate) else self._normalize_alignment_status(str(req.status or "draft"))
            cfg = self.store.upsert_map_alignment_config(
                map_name=map_name,
                map_revision_id=str(asset.get("revision_id") or ""),
                alignment_version=alignment_version,
                map_id=str(asset.get("map_id") or "").strip(),
                map_version=map_version,
                raw_frame=raw_frame,
                aligned_frame=aligned_frame,
                yaw_offset_deg=yaw_offset_deg,
                pivot_x=float(req.pivot_x or 0.0),
                pivot_y=float(req.pivot_y or 0.0),
                source=str(req.source or "manual_points").strip() or "manual_points",
                status=status,
            )
            if bool(req.activate):
                cfg = self.store.set_active_map_alignment(
                    map_name=map_name,
                    map_revision_id=str(asset.get("revision_id") or ""),
                    alignment_version=alignment_version,
                ) or cfg
            return ConfirmMapAlignmentByPointsResponse(
                success=True,
                message="ok",
                yaw_offset_deg=float(yaw_offset_deg),
                config=self._alignment_to_msg(cfg, asset=asset),
            )
        except Exception as e:
            return ConfirmMapAlignmentByPointsResponse(
                success=False,
                message=str(e),
                yaw_offset_deg=0.0,
                config=MapAlignmentConfig(),
            )

    def _handle_zone_query(self, req):
        op = int(req.operation)
        req_map_name = str(req.map_name or "").strip()
        req_map_revision_id = str(getattr(req, "map_revision_id", "") or "").strip()
        req_zone_id = str(req.zone_id or "").strip()
        req_alignment_version = str(req.alignment_version or "").strip()
        req_plan_profile_name = str(req.plan_profile_name or "").strip()
        include_disabled = bool(req.include_disabled)

        if op == int(req.getAll):
            try:
                asset = self._resolve_asset(req_map_name, req_map_revision_id)
                items = self.store.list_zone_metas(
                    map_name=str(asset.get("map_name") or ""),
                    map_revision_id=str(asset.get("revision_id") or ""),
                    include_disabled=include_disabled,
                )
                zones = [
                    self._zone_to_msg(
                        asset=asset,
                        zone_meta=item,
                        alignment_version=req_alignment_version,
                        plan_profile_name=req_plan_profile_name,
                    )
                    for item in items
                ]
                return OperateCoverageZoneResponse(
                    success=True,
                    message="ok",
                    zone=CoverageZone(),
                    zones=zones,
                )
            except Exception as e:
                return self._empty_zone_resp(False, str(e))

        if op == int(req.get):
            try:
                asset = self._resolve_asset(req_map_name, req_map_revision_id)
                item = self.store.get_zone_meta(
                    req_zone_id,
                    map_name=str(asset.get("map_name") or ""),
                    map_revision_id=str(asset.get("revision_id") or ""),
                )
                if not item:
                    return self._empty_zone_resp(False, "zone not found")
                return OperateCoverageZoneResponse(
                    success=True,
                    message="ok",
                    zone=self._zone_to_msg(
                        asset=asset,
                        zone_meta=item,
                        alignment_version=req_alignment_version,
                        plan_profile_name=req_plan_profile_name,
                    ),
                    zones=[],
                )
            except Exception as e:
                return self._empty_zone_resp(False, str(e))

        if op == int(req.Delete):
            try:
                if not req_zone_id:
                    return self._empty_zone_resp(False, "zone_id is required")
                asset = self._resolve_asset(req_map_name, req_map_revision_id)
                item = self.store.set_zone_enabled(
                    zone_id=req_zone_id,
                    enabled=False,
                    map_name=str(asset.get("map_name") or ""),
                    map_revision_id=str(asset.get("revision_id") or ""),
                )
                if not item:
                    return self._empty_zone_resp(False, "zone not found")
                return OperateCoverageZoneResponse(
                    success=True,
                    message="disabled",
                    zone=self._zone_to_msg(
                        asset=asset,
                        zone_meta=item,
                        alignment_version=req_alignment_version,
                        plan_profile_name=req_plan_profile_name,
                    ),
                    zones=[],
                )
            except Exception as e:
                return self._empty_zone_resp(False, str(e))

        return self._empty_zone_resp(False, "unsupported operation=%s" % op)

    def _handle_zone_plan_path(self, req):
        req_map_name = str(req.map_name or "").strip()
        req_map_revision_id = str(getattr(req, "map_revision_id", "") or "").strip()
        req_zone_id = str(req.zone_id or "").strip()
        req_alignment_version = str(req.alignment_version or "").strip()
        req_plan_profile_name = str(req.plan_profile_name or "").strip()
        if not req_zone_id:
            return self._empty_zone_plan_path_resp(False, "zone_id is required")
        try:
            asset = self._resolve_asset(req_map_name, req_map_revision_id)
            zone_meta = self.store.get_zone_meta(
                req_zone_id,
                map_name=str(asset.get("map_name") or ""),
                map_revision_id=str(asset.get("revision_id") or ""),
            )
            if not zone_meta:
                return self._empty_zone_plan_path_resp(False, "zone not found")

            zone_msg = self._zone_to_msg(
                asset=asset,
                zone_meta=zone_meta,
                alignment_version=req_alignment_version,
                plan_profile_name=req_plan_profile_name,
            )
            active_plan_id = str(zone_msg.active_plan_id or "").strip()
            if not active_plan_id:
                return self._empty_zone_plan_path_resp(False, "active plan not found")

            overlay = self._load_plan_overlay(active_plan_id)
            map_path = list(overlay.get("path_xy") or [])
            if not map_path:
                return self._empty_zone_plan_path_resp(False, "active plan path is empty")

            alignment_version = str(zone_msg.alignment_version or "").strip()
            display_frame = str(zone_msg.display_frame or zone_msg.storage_frame or asset.get("frame_id") or "map")
            storage_frame = str(zone_msg.storage_frame or asset.get("frame_id") or "map")
            warnings = [str(w) for w in (zone_msg.warnings or [])]

            display_path = list(map_path)
            display_entry_pose = overlay.get("entry_pose") if isinstance(overlay.get("entry_pose"), Pose2D) else Pose2D()
            entry_pose = overlay.get("entry_pose") if isinstance(overlay.get("entry_pose"), Pose2D) else Pose2D()

            if alignment_version and display_frame != storage_frame:
                alignment = self._resolve_alignment(asset, alignment_version)
                display_path = map_to_aligned_polygon(map_path, alignment)
                dx, dy, dyaw = map_to_aligned_pose(entry_pose.x, entry_pose.y, entry_pose.theta, alignment)
                display_entry_pose = _pose2d(dx, dy, dyaw)
            elif display_frame == storage_frame:
                display_path = list(map_path)
                display_entry_pose = entry_pose
            else:
                warnings.append("display path falls back to storage frame")

            return GetZonePlanPathResponse(
                success=True,
                message="ok",
                map_revision_id=str(asset.get("revision_id") or ""),
                zone_id=req_zone_id,
                active_plan_id=active_plan_id,
                plan_profile_name=str(zone_msg.plan_profile_name or overlay.get("plan_profile_name") or ""),
                alignment_version=alignment_version,
                display_frame=display_frame,
                storage_frame=storage_frame,
                display_path=_xy_to_ring(display_path),
                map_path=_xy_to_ring(map_path),
                display_entry_pose=display_entry_pose,
                entry_pose=entry_pose,
                estimated_length_m=float(zone_msg.estimated_length_m or overlay.get("estimated_length_m") or 0.0),
                estimated_duration_s=float(zone_msg.estimated_duration_s or 0.0),
                warnings=warnings,
            )
        except Exception as e:
            return self._empty_zone_plan_path_resp(False, str(e))

    def _normalize_region(self, outer: List[XY], holes: List[List[XY]]) -> Tuple[List[XY], List[List[XY]]]:
        outer_n, holes_n = normalize_polygon(list(outer or []), [list(r) for r in (holes or [])], prec=3)
        return _open_ring(outer_n or []), [_open_ring(h) for h in (holes_n or [])]

    def _bbox_size(self, points: Sequence[Sequence[float]]) -> Tuple[float, float]:
        pts = _open_ring(points)
        if len(pts) < 3:
            return 0.0, 0.0
        xs = [float(pt[0]) for pt in pts]
        ys = [float(pt[1]) for pt in pts]
        return max(xs) - min(xs), max(ys) - min(ys)

    def _safe_wall_margin_limit(self, outer: Sequence[Sequence[float]], params: PlannerParams) -> float:
        width_m, height_m = self._bbox_size(outer)
        short_side_m = min(float(width_m), float(height_m))
        if short_side_m <= 1e-6:
            return 0.0

        edge_radius_m = float(params.edge_corner_radius_m or 0.0)
        if edge_radius_m < 0.0:
            edge_radius_m = float(self.default_robot_spec.min_turning_radius or 0.0)

        # Keep enough inner span for one effective lane plus a modest
        # maneuvering envelope. Larger margins on narrow regions are known to
        # destabilize the Fields2Cover binding and can segfault the process.
        min_safe_inner_span_m = max(
            float(self.default_robot_spec.cov_width or 0.0) * 1.5,
            float(self.default_robot_spec.width or 0.0)
            + max(edge_radius_m, float(self.default_robot_spec.min_turning_radius or 0.0)) * 1.6,
            0.25,
        )
        return max(0.0, (short_side_m - min_safe_inner_span_m) * 0.5)

    def _min_plannable_span_m(self, params: PlannerParams) -> float:
        return min_plannable_span_m(self.default_robot_spec, params)

    def _filter_effective_regions_for_planner(
        self,
        regions: Sequence[Dict[str, object]],
        params: PlannerParams,
        warnings: List[str],
    ) -> List[Dict[str, object]]:
        kept, skipped, min_span_m = filter_effective_regions_for_planner(
            regions,
            self.default_robot_spec,
            params,
        )
        if skipped:
            warnings.append(
                "skipped %d constraint-split subregion(s) below %.2fm safe planning span"
                % (int(skipped), float(min_span_m))
            )
        return kept

    def _planner_params_for_region(self, outer: Sequence[Sequence[float]], warnings: List[str]) -> PlannerParams:
        params = replace(self.default_planner_params)
        requested_wall_margin_m = max(0.0, float(params.wall_margin_m or 0.0))
        safe_wall_margin_m = self._safe_wall_margin_limit(outer, params)
        if requested_wall_margin_m > safe_wall_margin_m + 1e-9:
            params.wall_margin_m = max(0.0, round(float(safe_wall_margin_m) - 0.01, 3))
            warnings.append(
                "wall_margin_m %.2fm reduced to %.2fm for narrow-region safety"
                % (requested_wall_margin_m, float(params.wall_margin_m))
            )
        return params

    def _region_to_map_and_display(
        self,
        *,
        asset: Dict[str, object],
        alignment: MapAlignment,
        region: PolygonRegion,
    ) -> Tuple[List[XY], List[List[XY]], List[XY], List[List[XY]]]:
        frame_id, outer, holes = _region_to_lists(region)
        if not outer:
            raise ValueError("region.outer is required")
        raw_frame = str(alignment.raw_frame or asset.get("frame_id") or "map")
        aligned_frame = str(alignment.aligned_frame or self.default_aligned_frame)
        frame = frame_id or aligned_frame
        if frame == aligned_frame:
            disp_outer, disp_holes = self._normalize_region(outer, holes)
            map_outer = aligned_to_map_polygon(disp_outer, alignment)
            map_holes = [aligned_to_map_polygon(r, alignment) for r in (disp_holes or [])]
        elif frame == raw_frame or frame == str(asset.get("frame_id") or raw_frame):
            map_outer, map_holes = self._normalize_region(outer, holes)
            disp_outer = map_to_aligned_polygon(map_outer, alignment)
            disp_holes = [map_to_aligned_polygon(r, alignment) for r in (map_holes or [])]
        else:
            raise ValueError("unsupported region.frame_id=%s" % frame)

        map_outer_n, map_holes_n = self._normalize_region(map_outer, map_holes)
        disp_outer_n, disp_holes_n = self._normalize_region(disp_outer, disp_holes)
        return map_outer_n, map_holes_n, disp_outer_n, disp_holes_n

    def _build_rect_preview(self, *, asset: Dict[str, object], alignment: MapAlignment, p1: Point32, p2: Point32, min_side_m: float):
        warnings: List[str] = []
        rect_disp = make_axis_aligned_rect((p1.x, p1.y), (p2.x, p2.y))
        width_m = abs(float(p2.x) - float(p1.x))
        height_m = abs(float(p2.y) - float(p1.y))
        area_m2 = width_m * height_m
        valid = True
        if width_m < min_side_m:
            warnings.append("width below minimum %.2fm" % float(min_side_m))
            valid = False
        if height_m < min_side_m:
            warnings.append("height below minimum %.2fm" % float(min_side_m))
            valid = False
        if area_m2 <= 1e-9:
            warnings.append("selection area is zero")
            valid = False

        rect_map = aligned_to_map_polygon(rect_disp, alignment)
        rect_map, _ = self._normalize_region(rect_map, [])
        rect_disp, _ = self._normalize_region(rect_disp, [])
        return {
            "alignment_version": str(alignment.alignment_version or ""),
            "display_frame": str(alignment.aligned_frame or self.default_aligned_frame),
            "storage_frame": str(alignment.raw_frame or asset.get("frame_id") or "map"),
            "display_region": _lists_to_region(str(alignment.aligned_frame or self.default_aligned_frame), rect_disp, []),
            "map_region": _lists_to_region(str(alignment.raw_frame or asset.get("frame_id") or "map"), rect_map, []),
            "width_m": float(width_m),
            "height_m": float(height_m),
            "area_m2": float(area_m2),
            "valid": bool(valid),
            "warnings": warnings,
        }

    def _handle_rect_preview(self, req):
        try:
            asset = self._resolve_asset(
                str(req.map_name or "").strip(),
                str(getattr(req, "map_revision_id", "") or "").strip(),
            )
            # Rect preview points are authored directly in the current display frame.
            # For new maps without an active business alignment, raw-map mode is now
            # the normal default, so an empty alignment_version should fall back to
            # identity alignment on the asset frame instead of hard-failing.
            alignment = self._resolve_alignment_for_frame(
                asset,
                alignment_version=str(req.alignment_version or "").strip(),
                frame_id=str(asset.get("frame_id") or "map"),
            )
            preview = self._build_rect_preview(
                asset=asset,
                alignment=alignment,
                p1=req.p1,
                p2=req.p2,
                min_side_m=_positive_float(req.min_side_m, self.rect_min_side_m),
            )
            return PreviewAlignedRectSelectionResponse(
                success=True,
                message="ok" if preview["valid"] else "selection check required",
                map_revision_id=str(asset.get("revision_id") or ""),
                alignment_version=str(preview["alignment_version"] or ""),
                display_frame=str(preview["display_frame"] or ""),
                storage_frame=str(preview["storage_frame"] or ""),
                display_region=preview["display_region"],
                map_region=preview["map_region"],
                width_m=float(preview["width_m"] or 0.0),
                height_m=float(preview["height_m"] or 0.0),
                area_m2=float(preview["area_m2"] or 0.0),
                valid=bool(preview["valid"]),
                warnings=[str(w) for w in (preview["warnings"] or [])],
            )
        except Exception as e:
            return PreviewAlignedRectSelectionResponse(
                success=False,
                message=str(e),
                map_revision_id="",
                alignment_version="",
                display_frame="",
                storage_frame="",
                display_region=PolygonRegion(),
                map_region=PolygonRegion(),
                width_m=0.0,
                height_m=0.0,
                area_m2=0.0,
                valid=False,
                warnings=[],
            )

    def _flatten_preview_path(self, plan_result) -> List[XY]:
        blocks = list(getattr(plan_result, "blocks", []) or [])
        if not blocks:
            return []
        block_map = {int(getattr(b, "block_id", idx)): b for idx, b in enumerate(blocks)}
        order = [int(x) for x in (getattr(plan_result, "exec_order", []) or [])]
        if not order:
            order = [int(getattr(b, "block_id", idx)) for idx, b in enumerate(blocks)]
        out: List[XY] = []
        for block_id in order:
            block = block_map.get(int(block_id))
            if block is None:
                continue
            pts = [(float(p[0]), float(p[1])) for p in (getattr(block, "path_xy", []) or [])]
            if not pts:
                continue
            if out and pts and abs(out[-1][0] - pts[0][0]) < 1e-6 and abs(out[-1][1] - pts[0][1]) < 1e-6:
                out.extend(pts[1:])
            else:
                out.extend(pts)
        return out

    def _plan_region_preview(
        self,
        *,
        asset: Dict[str, object],
        alignment: MapAlignment,
        region: PolygonRegion,
        plan_profile_name: str,
        debug_publish_markers: bool,
        allow_degraded_fallback: bool = False,
    ) -> Dict[str, object]:
        warnings: List[str] = []
        map_outer, map_holes, disp_outer, disp_holes = self._region_to_map_and_display(
            asset=asset,
            alignment=alignment,
            region=region,
        )
        if polygon_area(map_outer) <= 1e-6:
            return {
                "ok": False,
                "error_code": "INVALID_REGION",
                "error_message": "region area is zero",
                "warnings": warnings,
                "map_outer": map_outer,
                "map_holes": map_holes,
                "display_outer": disp_outer,
                "display_holes": disp_holes,
            }

        map_id = str(asset.get("map_id") or "").strip()
        map_md5 = str(asset.get("map_md5") or "").strip()
        raw_constraints = {
            "map_id": map_id,
            "map_md5": map_md5,
            "constraint_version": "",
            "no_go_areas": [],
            "virtual_walls": [],
        }
        if map_id:
            raw_constraints = self.store.load_map_constraints(
                map_id=map_id,
                map_revision_id=str(asset.get("revision_id") or ""),
                map_md5_hint=map_md5,
                create_if_missing=False,
            )
        compiled_map = compile_map_constraints(
            map_id=map_id,
            map_md5=str(raw_constraints.get("map_md5") or map_md5),
            constraint_version=str(raw_constraints.get("constraint_version") or ""),
            no_go_areas=raw_constraints.get("no_go_areas") or [],
            virtual_walls=raw_constraints.get("virtual_walls") or [],
            default_buffer_m=float(self.default_virtual_wall_buffer_m),
            default_no_go_buffer_m=float(self.default_no_go_buffer_m),
            prec=3,
        )
        zone_constraints = compile_zone_constraints(
            zone_outer=map_outer,
            zone_holes=map_holes,
            map_constraints=compiled_map,
            prec=3,
        )
        if zone_constraints.keepout_snapshot_rings:
            warnings.append("active map constraints are applied to the preview")
        if not zone_constraints.effective_regions:
            return {
                "ok": False,
                "error_code": "NO_EFFECTIVE_REGION",
                "error_message": "constraints leave no reachable cleaning area in zone",
                "warnings": warnings,
                "map_outer": map_outer,
                "map_holes": map_holes,
                "display_outer": disp_outer,
                "display_holes": disp_holes,
                "constraint_version": str(zone_constraints.constraint_version or ""),
            }

        planner_params = self._planner_params_for_region(map_outer, warnings)
        frame_id = str(alignment.raw_frame or asset.get("frame_id") or "map")
        planner_regions = self._filter_effective_regions_for_planner(
            zone_constraints.effective_regions,
            planner_params,
            warnings,
        )
        if not planner_regions:
            return {
                "ok": False,
                "error_code": "REGION_TOO_NARROW",
                "error_message": "no plannable region remains after applying constraints",
                "warnings": warnings,
                "map_outer": map_outer,
                "map_holes": map_holes,
                "display_outer": disp_outer,
                "display_holes": disp_holes,
                "constraint_version": str(zone_constraints.constraint_version or ""),
                "planner_params": planner_params,
            }

        primary = run_plan_coverage_isolated(
            frame_id=frame_id,
            outer=map_outer,
            holes=map_holes,
            robot_spec=self.default_robot_spec,
            params=planner_params,
            effective_regions=planner_regions,
            debug=bool(debug_publish_markers),
            timeout_s=float(self.planner_worker_timeout_s),
        )
        plan_result = primary.result
        if plan_result is None:
            worker_msg = str(primary.message or "planner worker failed")
            if len(worker_msg) > 240:
                worker_msg = worker_msg[:237] + "..."
            warnings.append("primary holes-aware planner failed in isolated worker: %s" % worker_msg)
            if allow_degraded_fallback:
                fallback_regions = make_hole_free_effective_regions(planner_regions, prec=3)
                fallback_regions = self._filter_effective_regions_for_planner(
                    fallback_regions,
                    planner_params,
                    warnings,
                )
                if not fallback_regions:
                    return {
                        "ok": False,
                        "error_code": "REGION_TOO_NARROW",
                        "error_message": "no plannable region remains after applying constraints",
                        "warnings": warnings,
                        "map_outer": map_outer,
                        "map_holes": map_holes,
                        "display_outer": disp_outer,
                        "display_holes": disp_holes,
                        "constraint_version": str(zone_constraints.constraint_version or ""),
                        "planner_params": planner_params,
                    }
                warnings.append(
                    "degraded hole-free preview fallback used; commit still requires primary planner success"
                )
                fallback = run_plan_coverage_isolated(
                    frame_id=frame_id,
                    outer=map_outer,
                    holes=map_holes,
                    robot_spec=self.default_robot_spec,
                    params=planner_params,
                    effective_regions=fallback_regions,
                    debug=bool(debug_publish_markers),
                    timeout_s=float(self.planner_worker_timeout_s),
                )
                plan_result = fallback.result
                if plan_result is None:
                    return {
                        "ok": False,
                        "error_code": "PLANNER_NATIVE_CRASH",
                        "error_message": str(fallback.message or "degraded planner fallback failed"),
                        "warnings": warnings,
                        "map_outer": map_outer,
                        "map_holes": map_holes,
                        "display_outer": disp_outer,
                        "display_holes": disp_holes,
                        "constraint_version": str(zone_constraints.constraint_version or ""),
                        "planner_params": planner_params,
                    }
            else:
                return {
                    "ok": False,
                    "error_code": "PLANNER_TIMEOUT" if primary.timeout else "PLANNER_NATIVE_CRASH",
                    "error_message": str(primary.message or "planner worker failed"),
                    "warnings": warnings,
                    "map_outer": map_outer,
                    "map_holes": map_holes,
                    "display_outer": disp_outer,
                    "display_holes": disp_holes,
                    "constraint_version": str(zone_constraints.constraint_version or ""),
                    "planner_params": planner_params,
                }
        if not bool(getattr(plan_result, "ok", False)):
            return {
                "ok": False,
                "error_code": str(getattr(plan_result, "error_code", "") or "PLAN_FAILED"),
                "error_message": str(getattr(plan_result, "error_message", "") or "planning failed"),
                "warnings": warnings,
                "map_outer": map_outer,
                "map_holes": map_holes,
                "display_outer": disp_outer,
                "display_holes": disp_holes,
                "constraint_version": str(zone_constraints.constraint_version or ""),
                "planner_params": planner_params,
            }

        preview_path_map = self._flatten_preview_path(plan_result)
        preview_path_disp = map_to_aligned_polygon(preview_path_map, alignment)

        entry_pose = Pose2D()
        display_entry_pose = Pose2D()
        blocks = list(getattr(plan_result, "blocks", []) or [])
        order = [int(x) for x in (getattr(plan_result, "exec_order", []) or [])]
        if blocks:
            block_map = {int(getattr(b, "block_id", idx)): b for idx, b in enumerate(blocks)}
            first = block_map.get(order[0]) if order else blocks[0]
            entry_xyyaw = tuple(getattr(first, "entry_xyyaw", ()) or ())
            if len(entry_xyyaw) >= 3:
                entry_pose = _pose2d(float(entry_xyyaw[0]), float(entry_xyyaw[1]), float(entry_xyyaw[2]))
                dx, dy, dyaw = map_to_aligned_pose(
                    float(entry_xyyaw[0]),
                    float(entry_xyyaw[1]),
                    float(entry_xyyaw[2]),
                    alignment,
                )
                display_entry_pose = _pose2d(dx, dy, dyaw)

        estimated_length_m = float(getattr(plan_result, "total_length_m", 0.0) or 0.0)
        estimated_duration_s = float(estimated_length_m / self.preview_nominal_speed_mps) if self.preview_nominal_speed_mps > 1e-6 else 0.0
        warnings.append("estimated duration uses nominal speed %.2f m/s" % float(self.preview_nominal_speed_mps))

        return {
            "ok": True,
            "valid": True,
            "error_code": "",
            "error_message": "",
            "plan_profile_name": str(plan_profile_name or ""),
            "warnings": warnings,
            "map_outer": map_outer,
            "map_holes": map_holes,
            "display_outer": disp_outer,
            "display_holes": disp_holes,
            "map_region": _lists_to_region(str(alignment.raw_frame or asset.get("frame_id") or "map"), map_outer, map_holes),
            "display_region": _lists_to_region(str(alignment.aligned_frame or self.default_aligned_frame), disp_outer, disp_holes),
            "preview_path_map": preview_path_map,
            "preview_path_display": preview_path_disp,
            "entry_pose": entry_pose,
            "display_entry_pose": display_entry_pose,
            "estimated_length_m": estimated_length_m,
            "estimated_duration_s": estimated_duration_s,
            "constraint_version": str(zone_constraints.constraint_version or ""),
            "plan_result": plan_result,
            "planner_params": planner_params,
        }

    def _preview_to_response(self, preview: Dict[str, object], alignment: MapAlignment, asset: Dict[str, object], *, success: bool) -> PreviewCoverageRegionResponse:
        return PreviewCoverageRegionResponse(
            success=bool(success),
            message="ok" if bool(preview.get("ok", False)) else str(preview.get("error_message") or "preview failed"),
            valid=bool(preview.get("ok", False)),
            error_code=str(preview.get("error_code") or ""),
            error_message=str(preview.get("error_message") or ""),
            map_revision_id=str(asset.get("revision_id") or ""),
            alignment_version=str(alignment.alignment_version or ""),
            display_frame=str(alignment.aligned_frame or self.default_aligned_frame),
            storage_frame=str(alignment.raw_frame or asset.get("frame_id") or "map"),
            display_region=preview.get("display_region") if isinstance(preview.get("display_region"), PolygonRegion) else PolygonRegion(),
            map_region=preview.get("map_region") if isinstance(preview.get("map_region"), PolygonRegion) else PolygonRegion(),
            preview_path=_xy_to_ring(preview.get("preview_path_map") or []),
            display_preview_path=_xy_to_ring(preview.get("preview_path_display") or []),
            entry_pose=preview.get("entry_pose") if isinstance(preview.get("entry_pose"), Pose2D) else Pose2D(),
            display_entry_pose=preview.get("display_entry_pose") if isinstance(preview.get("display_entry_pose"), Pose2D) else Pose2D(),
            estimated_length_m=float(preview.get("estimated_length_m") or 0.0),
            estimated_duration_s=float(preview.get("estimated_duration_s") or 0.0),
            warnings=[str(w) for w in (preview.get("warnings") or [])],
        )

    def _handle_plan_preview(self, req):
        try:
            asset = self._resolve_asset(
                str(req.map_name or "").strip(),
                str(getattr(req, "map_revision_id", "") or "").strip(),
            )
            alignment = self._resolve_alignment_for_region(
                asset,
                alignment_version=str(req.alignment_version or "").strip(),
                region_frame_id=str(req.region.frame_id or "").strip(),
            )
            # PreviewCoverageRegion keeps the wire field name `profile_name`.
            plan_profile_name = str(req.profile_name or self.default_plan_profile_name).strip() or self.default_plan_profile_name
            preview = self._plan_region_preview(
                asset=asset,
                alignment=alignment,
                region=req.region,
                plan_profile_name=plan_profile_name,
                debug_publish_markers=bool(req.debug_publish_markers),
                allow_degraded_fallback=bool(self.degraded_preview_on_planner_crash),
            )
            return self._preview_to_response(preview, alignment, asset, success=bool(preview.get("ok", False)))
        except Exception as e:
            return PreviewCoverageRegionResponse(
                success=False,
                message=str(e),
                valid=False,
                error_code="EXCEPTION",
                error_message=str(e),
                map_revision_id="",
                alignment_version="",
                display_frame="",
                storage_frame="",
                display_region=PolygonRegion(),
                map_region=PolygonRegion(),
                preview_path=PolygonRing(),
                display_preview_path=PolygonRing(),
                entry_pose=Pose2D(),
                display_entry_pose=Pose2D(),
                estimated_length_m=0.0,
                estimated_duration_s=0.0,
                warnings=[],
            )

    def _next_zone_id(self) -> str:
        return "zone_%s" % uuid.uuid4().hex[:8]

    def _handle_plan_commit(self, req):
        try:
            asset = self._resolve_asset(
                str(req.map_name or "").strip(),
                str(getattr(req, "map_revision_id", "") or "").strip(),
            )
            alignment = self._resolve_alignment_for_region(
                asset,
                alignment_version=str(req.alignment_version or "").strip(),
                region_frame_id=str(req.region.frame_id or "").strip(),
            )
            # CommitCoverageRegion keeps the wire field name `profile_name`.
            plan_profile_name = str(req.profile_name or self.default_plan_profile_name).strip() or self.default_plan_profile_name
            preview = self._plan_region_preview(
                asset=asset,
                alignment=alignment,
                region=req.region,
                plan_profile_name=plan_profile_name,
                debug_publish_markers=False,
                allow_degraded_fallback=bool(self.degraded_commit_on_planner_crash),
            )
            if not bool(preview.get("ok", False)):
                return CommitCoverageRegionResponse(
                    success=False,
                    message=str(preview.get("error_message") or "preview failed"),
                    valid=False,
                    error_code=str(preview.get("error_code") or "PREVIEW_FAILED"),
                    map_revision_id=str(asset.get("revision_id") or ""),
                    zone_id=str(req.zone_id or ""),
                    zone_version=0,
                    plan_id="",
                    alignment_version=str(alignment.alignment_version or ""),
                    display_frame=str(alignment.aligned_frame or self.default_aligned_frame),
                    storage_frame=str(alignment.raw_frame or asset.get("frame_id") or "map"),
                    display_region=preview.get("display_region") if isinstance(preview.get("display_region"), PolygonRegion) else PolygonRegion(),
                    map_region=preview.get("map_region") if isinstance(preview.get("map_region"), PolygonRegion) else PolygonRegion(),
                    preview_path=_xy_to_ring(preview.get("preview_path_map") or []),
                    display_preview_path=_xy_to_ring(preview.get("preview_path_display") or []),
                    entry_pose=preview.get("entry_pose") if isinstance(preview.get("entry_pose"), Pose2D) else Pose2D(),
                    display_entry_pose=preview.get("display_entry_pose") if isinstance(preview.get("display_entry_pose"), Pose2D) else Pose2D(),
                    estimated_length_m=float(preview.get("estimated_length_m") or 0.0),
                    estimated_duration_s=float(preview.get("estimated_duration_s") or 0.0),
                    warnings=[str(w) for w in (preview.get("warnings") or [])],
                )

            zone_id = str(req.zone_id or "").strip() or self._next_zone_id()
            map_name = str(asset.get("map_name") or "")
            map_version = str(asset.get("map_md5") or "")
            map_revision_id = str(asset.get("revision_id") or "")
            existing_zone = self.store.get_zone_meta(
                zone_id,
                map_name=map_name,
                map_revision_id=map_revision_id,
                map_version=map_version,
            )
            existing_zone_any = None
            if zone_id:
                existing_zone_any = self.store.get_zone_meta(
                    zone_id,
                    map_name=map_name,
                    map_revision_id=map_revision_id,
                )
            if zone_id and existing_zone is None and existing_zone_any is not None:
                return CommitCoverageRegionResponse(
                    success=False,
                    message="zone exists but belongs to a different map version",
                    valid=False,
                    error_code="ZONE_MAP_VERSION_MISMATCH",
                    map_revision_id=str(asset.get("revision_id") or ""),
                    zone_id=zone_id,
                    zone_version=int(existing_zone_any.get("current_zone_version") or 0),
                    plan_id="",
                    alignment_version=str(alignment.alignment_version or ""),
                    display_frame=str(alignment.aligned_frame or self.default_aligned_frame),
                    storage_frame=str(alignment.raw_frame or asset.get("frame_id") or "map"),
                    display_region=preview.get("display_region") if isinstance(preview.get("display_region"), PolygonRegion) else PolygonRegion(),
                    map_region=preview.get("map_region") if isinstance(preview.get("map_region"), PolygonRegion) else PolygonRegion(),
                    preview_path=_xy_to_ring(preview.get("preview_path_map") or []),
                    display_preview_path=_xy_to_ring(preview.get("preview_path_display") or []),
                    entry_pose=preview.get("entry_pose") if isinstance(preview.get("entry_pose"), Pose2D) else Pose2D(),
                    display_entry_pose=preview.get("display_entry_pose") if isinstance(preview.get("display_entry_pose"), Pose2D) else Pose2D(),
                    estimated_length_m=float(preview.get("estimated_length_m") or 0.0),
                    estimated_duration_s=float(preview.get("estimated_duration_s") or 0.0),
                    warnings=[str(w) for w in (preview.get("warnings") or [])],
                )

            base_zone_version = int(getattr(req, "base_zone_version", 0) or 0)
            current_zone_version = int(existing_zone.get("current_zone_version") or 0) if existing_zone else 0
            if existing_zone and base_zone_version > 0 and base_zone_version != current_zone_version:
                response_warnings = [str(w) for w in (preview.get("warnings") or [])]
                response_warnings.append(
                    "requested base_zone_version=%d but current published zone_version=%d"
                    % (base_zone_version, current_zone_version)
                )
                return CommitCoverageRegionResponse(
                    success=False,
                    message="zone version conflict",
                    valid=False,
                    error_code="ZONE_VERSION_CONFLICT",
                    map_revision_id=str(asset.get("revision_id") or ""),
                    zone_id=zone_id,
                    zone_version=current_zone_version,
                    plan_id="",
                    alignment_version=str(alignment.alignment_version or ""),
                    display_frame=str(alignment.aligned_frame or self.default_aligned_frame),
                    storage_frame=str(alignment.raw_frame or asset.get("frame_id") or "map"),
                    display_region=preview.get("display_region") if isinstance(preview.get("display_region"), PolygonRegion) else PolygonRegion(),
                    map_region=preview.get("map_region") if isinstance(preview.get("map_region"), PolygonRegion) else PolygonRegion(),
                    preview_path=_xy_to_ring(preview.get("preview_path_map") or []),
                    display_preview_path=_xy_to_ring(preview.get("preview_path_display") or []),
                    entry_pose=preview.get("entry_pose") if isinstance(preview.get("entry_pose"), Pose2D) else Pose2D(),
                    display_entry_pose=preview.get("display_entry_pose") if isinstance(preview.get("display_entry_pose"), Pose2D) else Pose2D(),
                    estimated_length_m=float(preview.get("estimated_length_m") or 0.0),
                    estimated_duration_s=float(preview.get("estimated_duration_s") or 0.0),
                    warnings=response_warnings,
                )

            zone_version = current_zone_version + 1 if existing_zone else 1
            map_outer = list(preview.get("map_outer") or [])
            map_holes = [list(r) for r in (preview.get("map_holes") or [])]
            plan_result = preview.get("plan_result")
            activate = bool(req.set_active_plan)
            publish_zone_record = bool(activate or (existing_zone is None))
            display_name = str(req.display_name or "").strip()
            if not display_name and existing_zone:
                display_name = str(existing_zone.get("display_name") or "").strip()
            display_name = display_name or zone_id
            plan_id = self.store.commit_zone_submission(
                map_name=map_name,
                map_revision_id=map_revision_id,
                zone_id=zone_id,
                zone_version=zone_version,
                frame_id=str(alignment.raw_frame or asset.get("frame_id") or "map"),
                display_name=display_name,
                plan_profile_name=plan_profile_name,
                params=dict(
                    (
                        preview.get("planner_params")
                        if isinstance(preview.get("planner_params"), PlannerParams)
                        else self.default_planner_params
                    ).__dict__
                ),
                robot_spec=dict(self.default_robot_spec.__dict__),
                outer=map_outer,
                holes=map_holes,
                plan_result=plan_result,
                map_id=str(asset.get("map_id") or ""),
                map_md5=map_version,
                constraint_version=str(preview.get("constraint_version") or ""),
                planner_version="coverage_planner_preview_v1",
                set_active_plan=activate,
                publish_zone_record=publish_zone_record,
                alignment_version=str(alignment.alignment_version or ""),
                display_frame=str(alignment.aligned_frame or self.default_aligned_frame),
                display_outer=list(preview.get("display_outer") or []),
                display_holes=[list(r) for r in (preview.get("display_holes") or [])],
                estimated_length_m=float(preview.get("estimated_length_m") or 0.0),
                estimated_duration_s=float(preview.get("estimated_duration_s") or 0.0),
                warnings=[str(w) for w in (preview.get("warnings") or [])],
            )
            response_warnings = [str(w) for w in (preview.get("warnings") or [])]
            if not activate:
                if existing_zone:
                    response_warnings.append(
                        "draft zone version saved but published zone remains at version %d" % current_zone_version
                    )
                else:
                    response_warnings.append("plan saved but not activated")
            return CommitCoverageRegionResponse(
                success=True,
                message="committed" if activate else "committed (not activated)",
                valid=True,
                error_code="",
                map_revision_id=str(asset.get("revision_id") or ""),
                zone_id=zone_id,
                zone_version=int(zone_version),
                plan_id=str(plan_id or ""),
                alignment_version=str(alignment.alignment_version or ""),
                display_frame=str(alignment.aligned_frame or self.default_aligned_frame),
                storage_frame=str(alignment.raw_frame or asset.get("frame_id") or "map"),
                display_region=preview.get("display_region") if isinstance(preview.get("display_region"), PolygonRegion) else PolygonRegion(),
                map_region=preview.get("map_region") if isinstance(preview.get("map_region"), PolygonRegion) else PolygonRegion(),
                preview_path=_xy_to_ring(preview.get("preview_path_map") or []),
                display_preview_path=_xy_to_ring(preview.get("preview_path_display") or []),
                entry_pose=preview.get("entry_pose") if isinstance(preview.get("entry_pose"), Pose2D) else Pose2D(),
                display_entry_pose=preview.get("display_entry_pose") if isinstance(preview.get("display_entry_pose"), Pose2D) else Pose2D(),
                estimated_length_m=float(preview.get("estimated_length_m") or 0.0),
                estimated_duration_s=float(preview.get("estimated_duration_s") or 0.0),
                warnings=response_warnings,
            )
        except Exception as e:
            return CommitCoverageRegionResponse(
                success=False,
                message=str(e),
                valid=False,
                error_code="EXCEPTION",
                map_revision_id="",
                zone_id=str(req.zone_id or ""),
                zone_version=0,
                plan_id="",
                alignment_version="",
                display_frame="",
                storage_frame="",
                display_region=PolygonRegion(),
                map_region=PolygonRegion(),
                preview_path=PolygonRing(),
                display_preview_path=PolygonRing(),
                entry_pose=Pose2D(),
                display_entry_pose=Pose2D(),
                estimated_length_m=0.0,
                estimated_duration_s=0.0,
                warnings=[],
            )


def main():
    rospy.init_node("site_editor_service", anonymous=False)
    SiteEditorServiceNode()
    rospy.spin()


if __name__ == "__main__":
    main()

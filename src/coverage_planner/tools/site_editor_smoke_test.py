#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import math
import sys
from typing import Iterable, Tuple

import rospy
from geometry_msgs.msg import Point32

from my_msg_srv.msg import MapAlignmentConfig, PolygonRegion, PolygonRing
from my_msg_srv.srv import (
    CommitCoverageRegion,
    ConfirmMapAlignmentByPoints,
    OperateCoverageZone,
    OperateMapAlignment,
    PreviewAlignedRectSelection,
    PreviewCoverageRegion,
)


def _pt(x: float, y: float) -> Point32:
    msg = Point32()
    msg.x = float(x)
    msg.y = float(y)
    msg.z = 0.0
    return msg


def _region_from_points(frame_id: str, points: Iterable[Tuple[float, float]]) -> PolygonRegion:
    ring = PolygonRing()
    ring.points = [_pt(x, y) for x, y in points]
    region = PolygonRegion()
    region.frame_id = str(frame_id or "").strip()
    region.outer = ring
    region.holes = []
    return region


def _print_header(title: str) -> None:
    print("\n== %s ==" % title)


def _print_warnings(warnings) -> None:
    for w in list(warnings or []):
        print("  - warning:", str(w))


def main():
    ap = argparse.ArgumentParser(description="End-to-end smoke test for site editor backend services.")
    ap.add_argument("--map-name", required=True)
    ap.add_argument("--robot-id", default="local_robot")
    ap.add_argument("--alignment-version", default="")
    ap.add_argument("--aligned-frame", default="site_map")
    ap.add_argument("--raw-frame", default="map")
    ap.add_argument("--align-p1", nargs=2, type=float, metavar=("X", "Y"))
    ap.add_argument("--align-p2", nargs=2, type=float, metavar=("X", "Y"))
    ap.add_argument("--skip-alignment", action="store_true")
    ap.add_argument("--rect-p1", nargs=2, type=float, required=True, metavar=("X", "Y"))
    ap.add_argument("--rect-p2", nargs=2, type=float, required=True, metavar=("X", "Y"))
    ap.add_argument("--profile-name", default="cover_standard")
    ap.add_argument("--zone-id", default="")
    ap.add_argument("--display-name", default="")
    ap.add_argument("--set-active-plan", action="store_true")
    ap.add_argument("--exercise-update", action="store_true")
    ap.add_argument("--check-version-conflict", action="store_true")
    ap.add_argument("--update-rect-p1", nargs=2, type=float, metavar=("X", "Y"))
    ap.add_argument("--update-rect-p2", nargs=2, type=float, metavar=("X", "Y"))
    ap.add_argument("--rect-min-side-m", type=float, default=0.10)
    ap.add_argument("--service-timeout", type=float, default=5.0)
    ap.add_argument("--alignment-service", default="/database_server/map_alignment_service")
    ap.add_argument("--alignment-points-service", default="/database_server/map_alignment_by_points_service")
    ap.add_argument("--rect-preview-service", default="/database_server/rect_zone_preview_service")
    ap.add_argument("--plan-preview-service", default="/database_server/coverage_preview_service")
    ap.add_argument("--plan-commit-service", default="/database_server/coverage_commit_service")
    ap.add_argument("--zone-query-service", default="/database_server/coverage_zone_service")
    args = ap.parse_args()

    rospy.init_node("site_editor_smoke_test", anonymous=True, disable_signals=True)

    timeout = float(args.service_timeout)
    proxies = {}
    service_specs = [
        ("alignment", args.alignment_service, OperateMapAlignment),
        ("alignment_points", args.alignment_points_service, ConfirmMapAlignmentByPoints),
        ("rect_preview", args.rect_preview_service, PreviewAlignedRectSelection),
        ("plan_preview", args.plan_preview_service, PreviewCoverageRegion),
        ("plan_commit", args.plan_commit_service, CommitCoverageRegion),
        ("zone_query", args.zone_query_service, OperateCoverageZone),
    ]
    for key, name, srv_type in service_specs:
        rospy.wait_for_service(name, timeout=timeout)
        proxies[key] = rospy.ServiceProxy(name, srv_type)

    alignment_version = str(args.alignment_version or "").strip()
    if not args.skip_alignment:
        if not args.align_p1 or not args.align_p2:
            print("alignment points are required unless --skip-alignment is set", file=sys.stderr)
            return 2
        _print_header("Confirm Alignment By Points")
        resp = proxies["alignment_points"](
            map_name=str(args.map_name),
            alignment_version=alignment_version,
            raw_frame=str(args.raw_frame),
            aligned_frame=str(args.aligned_frame),
            p1=_pt(args.align_p1[0], args.align_p1[1]),
            p2=_pt(args.align_p2[0], args.align_p2[1]),
            pivot_x=0.0,
            pivot_y=0.0,
            source="site_editor_smoke_test",
            status="active",
            activate=True,
        )
        print("success =", resp.success)
        print("message =", resp.message)
        print("yaw_offset_deg =", resp.yaw_offset_deg)
        if not resp.success:
            return 3
        alignment_version = str(resp.config.alignment_version or "")
        print("alignment_version =", alignment_version)
    else:
        _print_header("Use Existing Alignment")
        if not alignment_version:
            resp = proxies["alignment"](
                operation=0,
                map_name=str(args.map_name),
                alignment_version="",
                config=MapAlignmentConfig(),
            )
            print("success =", resp.success)
            print("message =", resp.message)
            if not resp.success:
                return 4
            alignment_version = str(resp.config.alignment_version or "")
        print("alignment_version =", alignment_version or "<active>")

    _print_header("Rect Preview")
    rect_resp = proxies["rect_preview"](
        map_name=str(args.map_name),
        alignment_version=alignment_version,
        p1=_pt(args.rect_p1[0], args.rect_p1[1]),
        p2=_pt(args.rect_p2[0], args.rect_p2[1]),
        min_side_m=float(args.rect_min_side_m),
    )
    print("success =", rect_resp.success)
    print("message =", rect_resp.message)
    print("valid =", rect_resp.valid)
    print("area_m2 =", rect_resp.area_m2)
    _print_warnings(rect_resp.warnings)
    if (not rect_resp.success) or (not rect_resp.valid):
        return 5

    _print_header("Coverage Preview")
    preview_resp = proxies["plan_preview"](
        map_name=str(args.map_name),
        alignment_version=alignment_version,
        region=rect_resp.display_region,
        profile_name=str(args.profile_name),
        debug_publish_markers=False,
    )
    print("success =", preview_resp.success)
    print("message =", preview_resp.message)
    print("valid =", preview_resp.valid)
    print("estimated_length_m =", preview_resp.estimated_length_m)
    print("estimated_duration_s =", preview_resp.estimated_duration_s)
    print(
        "entry_pose = (%.3f, %.3f, %.3fdeg)"
        % (
            preview_resp.display_entry_pose.x,
            preview_resp.display_entry_pose.y,
            math.degrees(preview_resp.display_entry_pose.theta),
        )
    )
    _print_warnings(preview_resp.warnings)
    if (not preview_resp.success) or (not preview_resp.valid):
        return 6

    _print_header("Commit Coverage Region")
    zone_id = str(args.zone_id or "").strip()
    display_name = str(args.display_name or zone_id or "").strip()
    commit_resp = proxies["plan_commit"](
        map_name=str(args.map_name),
        alignment_version=alignment_version,
        zone_id=zone_id,
        base_zone_version=0,
        display_name=display_name,
        region=rect_resp.display_region,
        profile_name=str(args.profile_name),
        set_active_plan=bool(args.set_active_plan),
    )
    print("success =", commit_resp.success)
    print("message =", commit_resp.message)
    print("zone_id =", commit_resp.zone_id)
    print("zone_version =", commit_resp.zone_version)
    print("plan_id =", commit_resp.plan_id)
    _print_warnings(commit_resp.warnings)
    if not commit_resp.success:
        return 7

    _print_header("Zone Query")
    query_resp = proxies["zone_query"](
        operation=0,
        map_name=str(args.map_name),
        zone_id=str(commit_resp.zone_id),
        alignment_version=alignment_version,
        plan_profile_name=str(args.profile_name),
        include_disabled=True,
    )
    print("success =", query_resp.success)
    print("message =", query_resp.message)
    if not query_resp.success:
        return 8
    zone = query_resp.zone
    print("zone.display_name =", zone.display_name)
    print("zone.display_frame =", zone.display_frame)
    print("zone.storage_frame =", zone.storage_frame)
    print("zone.active_plan_id =", zone.active_plan_id)
    print("zone.plan_profile_name =", zone.plan_profile_name)
    print("zone.estimated_length_m =", zone.estimated_length_m)
    print("zone.estimated_duration_s =", zone.estimated_duration_s)
    _print_warnings(zone.warnings)

    if args.exercise_update:
        update_p1 = tuple(args.update_rect_p1) if args.update_rect_p1 else (float(args.rect_p1[0]) + 0.5, float(args.rect_p1[1]) + 0.5)
        update_p2 = tuple(args.update_rect_p2) if args.update_rect_p2 else (float(args.rect_p2[0]) + 0.5, float(args.rect_p2[1]) + 0.5)

        _print_header("Update Rect Preview")
        update_rect_resp = proxies["rect_preview"](
            map_name=str(args.map_name),
            alignment_version=alignment_version,
            p1=_pt(update_p1[0], update_p1[1]),
            p2=_pt(update_p2[0], update_p2[1]),
            min_side_m=float(args.rect_min_side_m),
        )
        print("success =", update_rect_resp.success)
        print("message =", update_rect_resp.message)
        print("valid =", update_rect_resp.valid)
        print("area_m2 =", update_rect_resp.area_m2)
        _print_warnings(update_rect_resp.warnings)
        if (not update_rect_resp.success) or (not update_rect_resp.valid):
            return 9

        _print_header("Update Commit")
        update_commit_resp = proxies["plan_commit"](
            map_name=str(args.map_name),
            alignment_version=alignment_version,
            zone_id=str(commit_resp.zone_id),
            base_zone_version=int(commit_resp.zone_version),
            display_name=display_name,
            region=update_rect_resp.display_region,
            profile_name=str(args.profile_name),
            set_active_plan=bool(args.set_active_plan),
        )
        print("success =", update_commit_resp.success)
        print("message =", update_commit_resp.message)
        print("zone_id =", update_commit_resp.zone_id)
        print("zone_version =", update_commit_resp.zone_version)
        print("plan_id =", update_commit_resp.plan_id)
        _print_warnings(update_commit_resp.warnings)
        if not update_commit_resp.success:
            return 10
        if int(update_commit_resp.zone_version) <= int(commit_resp.zone_version):
            print("updated zone_version did not increment", file=sys.stderr)
            return 11

        _print_header("Updated Zone Query")
        updated_query_resp = proxies["zone_query"](
            operation=0,
            map_name=str(args.map_name),
            zone_id=str(update_commit_resp.zone_id),
            alignment_version=alignment_version,
            plan_profile_name=str(args.profile_name),
            include_disabled=True,
        )
        print("success =", updated_query_resp.success)
        print("message =", updated_query_resp.message)
        if not updated_query_resp.success:
            return 12
        updated_zone = updated_query_resp.zone
        print("zone.zone_version =", updated_zone.zone_version)
        print("zone.active_plan_id =", updated_zone.active_plan_id)
        print("zone.plan_profile_name =", updated_zone.plan_profile_name)
        _print_warnings(updated_zone.warnings)

        if args.check_version_conflict:
            _print_header("Version Conflict Check")
            conflict_resp = proxies["plan_commit"](
                map_name=str(args.map_name),
                alignment_version=alignment_version,
                zone_id=str(update_commit_resp.zone_id),
                base_zone_version=int(commit_resp.zone_version),
                display_name=display_name,
                region=update_rect_resp.display_region,
                profile_name=str(args.profile_name),
                set_active_plan=bool(args.set_active_plan),
            )
            print("success =", conflict_resp.success)
            print("message =", conflict_resp.message)
            print("error_code =", conflict_resp.error_code)
            _print_warnings(conflict_resp.warnings)
            if conflict_resp.success or str(conflict_resp.error_code or "") != "ZONE_VERSION_CONFLICT":
                print("expected ZONE_VERSION_CONFLICT", file=sys.stderr)
                return 13

    print("\nSMOKE TEST PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())

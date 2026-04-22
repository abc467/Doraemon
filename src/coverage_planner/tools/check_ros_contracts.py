#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import sys
import time

import rosgraph
import rosservice

from cleanrobot_app_msgs.msg import (
    CoverageZone as AppCoverageZone,
    CleanSchedule as AppCleanSchedule,
    CleanTask as AppCleanTask,
    MapAlignmentConfig as AppMapAlignmentConfig,
    MapNoGoArea as AppMapNoGoArea,
    MapVirtualWall as AppMapVirtualWall,
    OdometryState as AppOdometryState,
    PgmData as AppPgmData,
    PolygonRegion as AppPolygonRegion,
    PolygonRing as AppPolygonRing,
    ProfileOption as AppProfileOption,
    SlamJobState as AppSlamJobState,
    SlamState as AppSlamState,
    SystemReadiness as AppSystemReadiness,
)
from cleanrobot_app_msgs.srv import (
    CommitCoverageRegion as AppCommitCoverageRegion,
    CommitCoverageRegionResponse as AppCommitCoverageRegionResponse,
    ConfirmMapAlignmentByPoints as AppConfirmMapAlignmentByPoints,
    ConfirmMapAlignmentByPointsResponse as AppConfirmMapAlignmentByPointsResponse,
    ExeTask as AppExeTask,
    ExeTaskRequest as AppExeTaskRequest,
    ExeTaskResponse as AppExeTaskResponse,
    GetOdometryStatus as AppGetOdometryStatus,
    GetOdometryStatusResponse as AppGetOdometryStatusResponse,
    GetProfileCatalog as AppGetProfileCatalog,
    GetProfileCatalogResponse as AppGetProfileCatalogResponse,
    GetSlamJob as AppGetSlamJob,
    GetSlamJobResponse as AppGetSlamJobResponse,
    GetSlamStatus as AppGetSlamStatus,
    GetSlamStatusResponse as AppGetSlamStatusResponse,
    GetSystemReadiness as AppGetSystemReadiness,
    GetSystemReadinessResponse as AppGetSystemReadinessResponse,
    GetZonePlanPath as AppGetZonePlanPath,
    GetZonePlanPathResponse as AppGetZonePlanPathResponse,
    OperateCoverageZone as AppOperateCoverageZone,
    OperateCoverageZoneRequest as AppOperateCoverageZoneRequest,
    OperateCoverageZoneResponse as AppOperateCoverageZoneResponse,
    OperateMap as AppOperateMap,
    OperateMapRequest as AppOperateMapRequest,
    OperateMapResponse as AppOperateMapResponse,
    OperateMapAlignment as AppOperateMapAlignment,
    OperateMapAlignmentRequest as AppOperateMapAlignmentRequest,
    OperateMapAlignmentResponse as AppOperateMapAlignmentResponse,
    OperateMapNoGoArea as AppOperateMapNoGoArea,
    OperateMapNoGoAreaRequest as AppOperateMapNoGoAreaRequest,
    OperateMapNoGoAreaResponse as AppOperateMapNoGoAreaResponse,
    OperateMapVirtualWall as AppOperateMapVirtualWall,
    OperateMapVirtualWallRequest as AppOperateMapVirtualWallRequest,
    OperateMapVirtualWallResponse as AppOperateMapVirtualWallResponse,
    OperateSlamRuntime as AppOperateSlamRuntime,
    OperateSlamRuntimeResponse as AppOperateSlamRuntimeResponse,
    OperateSchedule as AppOperateSchedule,
    OperateScheduleRequest as AppOperateScheduleRequest,
    OperateScheduleResponse as AppOperateScheduleResponse,
    OperateTask as AppOperateTask,
    OperateTaskRequest as AppOperateTaskRequest,
    OperateTaskResponse as AppOperateTaskResponse,
    PreviewAlignedRectSelection as AppPreviewAlignedRectSelection,
    PreviewAlignedRectSelectionResponse as AppPreviewAlignedRectSelectionResponse,
    PreviewCoverageRegion as AppPreviewCoverageRegion,
    PreviewCoverageRegionResponse as AppPreviewCoverageRegionResponse,
    RestartLocalization as AppRestartLocalization,
    RestartLocalizationResponse as AppRestartLocalizationResponse,
    SubmitSlamCommand as AppSubmitSlamCommand,
    SubmitSlamCommandResponse as AppSubmitSlamCommandResponse,
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

from coverage_planner.ros_contract import build_contract_report


def _contract_param_for_service_name(service_name: str) -> str:
    parts = [part for part in str(service_name or "").split("/") if part]
    for marker in ("app", "site"):
        if marker in parts:
            index = parts.index(marker)
            contract_parts = parts[:index] + ["contracts"] + parts[index:]
            return "/" + "/".join(contract_parts)
    raise ValueError("canonical service name missing app/site marker: %s" % str(service_name or ""))


def _local_contracts():
    return {
        "clean_task_service_app": build_contract_report(
            service_name="/database_server/app/clean_task_service",
            contract_name="clean_task_service_app",
            service_cls=AppOperateTask,
            request_cls=AppOperateTaskRequest,
            response_cls=AppOperateTaskResponse,
            dependencies={"task": AppCleanTask},
            features=[
                "CleanTask.repeat_after_full_charge",
                "OperateTask.repeat_after_full_charge_state",
                "cleanrobot_app_msgs_parallel",
            ],
        ),
        "clean_schedule_service_app": build_contract_report(
            service_name="/database_server/app/clean_schedule_service",
            contract_name="clean_schedule_service_app",
            service_cls=AppOperateSchedule,
            request_cls=AppOperateScheduleRequest,
            response_cls=AppOperateScheduleResponse,
            dependencies={"schedule": AppCleanSchedule},
            features=["CleanSchedule.repeat_after_full_charge", "cleanrobot_app_msgs_parallel"],
        ),
        "map_server_app": build_contract_report(
            service_name="/clean_robot_server/app/map_server",
            contract_name="map_server_app",
            service_cls=AppOperateMap,
            request_cls=AppOperateMapRequest,
            response_cls=AppOperateMapResponse,
            dependencies={"map": AppPgmData},
            features=[
                "map_asset_revision_view",
                "import_as_candidate_revision",
                "verified_head_vs_active_revision_projection",
                "cleanrobot_app_msgs_parallel",
            ],
        ),
        "map_alignment_service_app": build_contract_report(
            service_name="/database_server/app/map_alignment_service",
            contract_name="map_alignment_service_app",
            service_cls=AppOperateMapAlignment,
            request_cls=AppOperateMapAlignmentRequest,
            response_cls=AppOperateMapAlignmentResponse,
            dependencies={"config": AppMapAlignmentConfig},
            features=["revision_scoped_alignment_config", "alignment_activation", "cleanrobot_app_msgs_parallel"],
        ),
        "map_alignment_service_site": build_contract_report(
            service_name="/database_server/site/map_alignment_service",
            contract_name="map_alignment_service_site",
            service_cls=SiteOperateMapAlignment,
            request_cls=SiteOperateMapAlignment._request_class,
            response_cls=SiteOperateMapAlignmentResponse,
            dependencies={"config": SiteMapAlignmentConfig},
            features=["revision_scoped_alignment_config", "alignment_activation", "cleanrobot_site_msgs_canonical"],
        ),
        "map_alignment_by_points_service_app": build_contract_report(
            service_name="/database_server/app/map_alignment_by_points_service",
            contract_name="map_alignment_by_points_service_app",
            service_cls=AppConfirmMapAlignmentByPoints,
            request_cls=AppConfirmMapAlignmentByPoints._request_class,
            response_cls=AppConfirmMapAlignmentByPointsResponse,
            dependencies={"config": AppMapAlignmentConfig},
            features=["alignment_confirm_by_points", "cleanrobot_app_msgs_parallel"],
        ),
        "map_alignment_by_points_service_site": build_contract_report(
            service_name="/database_server/site/map_alignment_by_points_service",
            contract_name="map_alignment_by_points_service_site",
            service_cls=SiteConfirmMapAlignmentByPoints,
            request_cls=SiteConfirmMapAlignmentByPoints._request_class,
            response_cls=SiteConfirmMapAlignmentByPointsResponse,
            dependencies={"config": SiteMapAlignmentConfig},
            features=["alignment_confirm_by_points", "cleanrobot_site_msgs_canonical"],
        ),
        "rect_zone_preview_service_app": build_contract_report(
            service_name="/database_server/app/rect_zone_preview_service",
            contract_name="rect_zone_preview_service_app",
            service_cls=AppPreviewAlignedRectSelection,
            request_cls=AppPreviewAlignedRectSelection._request_class,
            response_cls=AppPreviewAlignedRectSelectionResponse,
            dependencies={"display_region": AppPolygonRegion, "map_region": AppPolygonRegion},
            features=["aligned_rect_selection_preview", "cleanrobot_app_msgs_parallel"],
        ),
        "rect_zone_preview_service_site": build_contract_report(
            service_name="/database_server/site/rect_zone_preview_service",
            contract_name="rect_zone_preview_service_site",
            service_cls=SitePreviewAlignedRectSelection,
            request_cls=SitePreviewAlignedRectSelection._request_class,
            response_cls=SitePreviewAlignedRectSelectionResponse,
            dependencies={"display_region": SitePolygonRegion, "map_region": SitePolygonRegion},
            features=["aligned_rect_selection_preview", "cleanrobot_site_msgs_canonical"],
        ),
        "coverage_preview_service_app": build_contract_report(
            service_name="/database_server/app/coverage_preview_service",
            contract_name="coverage_preview_service_app",
            service_cls=AppPreviewCoverageRegion,
            request_cls=AppPreviewCoverageRegion._request_class,
            response_cls=AppPreviewCoverageRegionResponse,
            dependencies={
                "display_region": AppPolygonRegion,
                "map_region": AppPolygonRegion,
                "preview_path": AppPolygonRing,
                "display_preview_path": AppPolygonRing,
            },
            features=["coverage_region_preview", "cleanrobot_app_msgs_parallel"],
        ),
        "coverage_preview_service_site": build_contract_report(
            service_name="/database_server/site/coverage_preview_service",
            contract_name="coverage_preview_service_site",
            service_cls=SitePreviewCoverageRegion,
            request_cls=SitePreviewCoverageRegion._request_class,
            response_cls=SitePreviewCoverageRegionResponse,
            dependencies={
                "display_region": SitePolygonRegion,
                "map_region": SitePolygonRegion,
                "preview_path": SitePolygonRing,
                "display_preview_path": SitePolygonRing,
            },
            features=["coverage_region_preview", "cleanrobot_site_msgs_canonical"],
        ),
        "coverage_commit_service_app": build_contract_report(
            service_name="/database_server/app/coverage_commit_service",
            contract_name="coverage_commit_service_app",
            service_cls=AppCommitCoverageRegion,
            request_cls=AppCommitCoverageRegion._request_class,
            response_cls=AppCommitCoverageRegionResponse,
            dependencies={
                "display_region": AppPolygonRegion,
                "map_region": AppPolygonRegion,
                "preview_path": AppPolygonRing,
                "display_preview_path": AppPolygonRing,
            },
            features=["coverage_region_commit", "zone_version_optimistic_concurrency", "cleanrobot_app_msgs_parallel"],
        ),
        "coverage_commit_service_site": build_contract_report(
            service_name="/database_server/site/coverage_commit_service",
            contract_name="coverage_commit_service_site",
            service_cls=SiteCommitCoverageRegion,
            request_cls=SiteCommitCoverageRegion._request_class,
            response_cls=SiteCommitCoverageRegionResponse,
            dependencies={
                "display_region": SitePolygonRegion,
                "map_region": SitePolygonRegion,
                "preview_path": SitePolygonRing,
                "display_preview_path": SitePolygonRing,
            },
            features=["coverage_region_commit", "zone_version_optimistic_concurrency", "cleanrobot_site_msgs_canonical"],
        ),
        "coverage_zone_service_app": build_contract_report(
            service_name="/database_server/app/coverage_zone_service",
            contract_name="coverage_zone_service_app",
            service_cls=AppOperateCoverageZone,
            request_cls=AppOperateCoverageZoneRequest,
            response_cls=AppOperateCoverageZoneResponse,
            dependencies={"zone": AppCoverageZone},
            features=["revision_scoped_zone_query", "active_plan_projection", "cleanrobot_app_msgs_parallel"],
        ),
        "coverage_zone_service_site": build_contract_report(
            service_name="/database_server/site/coverage_zone_service",
            contract_name="coverage_zone_service_site",
            service_cls=SiteOperateCoverageZone,
            request_cls=SiteOperateCoverageZone._request_class,
            response_cls=SiteOperateCoverageZoneResponse,
            dependencies={"zone": SiteCoverageZone},
            features=["revision_scoped_zone_query", "active_plan_projection", "cleanrobot_site_msgs_canonical"],
        ),
        "zone_plan_path_service_app": build_contract_report(
            service_name="/database_server/app/zone_plan_path_service",
            contract_name="zone_plan_path_service_app",
            service_cls=AppGetZonePlanPath,
            request_cls=AppGetZonePlanPath._request_class,
            response_cls=AppGetZonePlanPathResponse,
            dependencies={"display_path": AppPolygonRing, "map_path": AppPolygonRing},
            features=["zone_plan_path_projection", "cleanrobot_app_msgs_parallel"],
        ),
        "zone_plan_path_service_site": build_contract_report(
            service_name="/database_server/site/zone_plan_path_service",
            contract_name="zone_plan_path_service_site",
            service_cls=SiteGetZonePlanPath,
            request_cls=SiteGetZonePlanPath._request_class,
            response_cls=SiteGetZonePlanPathResponse,
            dependencies={"display_path": SitePolygonRing, "map_path": SitePolygonRing},
            features=["zone_plan_path_projection", "cleanrobot_site_msgs_canonical"],
        ),
        "no_go_area_service_app": build_contract_report(
            service_name="/database_server/app/no_go_area_service",
            contract_name="no_go_area_service_app",
            service_cls=AppOperateMapNoGoArea,
            request_cls=AppOperateMapNoGoAreaRequest,
            response_cls=AppOperateMapNoGoAreaResponse,
            dependencies={"area": AppMapNoGoArea},
            features=["map_constraints_editor", "cleanrobot_app_msgs_parallel"],
        ),
        "no_go_area_service_site": build_contract_report(
            service_name="/database_server/site/no_go_area_service",
            contract_name="no_go_area_service_site",
            service_cls=SiteOperateMapNoGoArea,
            request_cls=SiteOperateMapNoGoArea._request_class,
            response_cls=SiteOperateMapNoGoAreaResponse,
            dependencies={"area": SiteMapNoGoArea},
            features=["map_constraints_editor", "cleanrobot_site_msgs_canonical"],
        ),
        "virtual_wall_service_app": build_contract_report(
            service_name="/database_server/app/virtual_wall_service",
            contract_name="virtual_wall_service_app",
            service_cls=AppOperateMapVirtualWall,
            request_cls=AppOperateMapVirtualWallRequest,
            response_cls=AppOperateMapVirtualWallResponse,
            dependencies={"wall": AppMapVirtualWall},
            features=["map_constraints_editor", "cleanrobot_app_msgs_parallel"],
        ),
        "virtual_wall_service_site": build_contract_report(
            service_name="/database_server/site/virtual_wall_service",
            contract_name="virtual_wall_service_site",
            service_cls=SiteOperateMapVirtualWall,
            request_cls=SiteOperateMapVirtualWall._request_class,
            response_cls=SiteOperateMapVirtualWallResponse,
            dependencies={"wall": SiteMapVirtualWall},
            features=["map_constraints_editor", "cleanrobot_site_msgs_canonical"],
        ),
        "runtime_operate_app": build_contract_report(
            service_name="/cartographer/runtime/app/operate",
            contract_name="runtime_operate_app",
            service_cls=AppOperateSlamRuntime,
            request_cls=AppOperateSlamRuntime._request_class,
            response_cls=AppOperateSlamRuntimeResponse,
            dependencies={},
            features=[
                "runtime_operation_control",
                "restart_localization",
                "start_mapping",
                "save_mapping",
                "stop_mapping",
                "verify_map_revision",
                "activate_map_revision",
                "cleanrobot_app_msgs_parallel",
            ],
        ),
        "runtime_submit_job_app": build_contract_report(
            service_name="/cartographer/runtime/app/submit_job",
            contract_name="runtime_submit_job_app",
            service_cls=AppSubmitSlamCommand,
            request_cls=AppSubmitSlamCommand._request_class,
            response_cls=AppSubmitSlamCommandResponse,
            dependencies={"job": AppSlamJobState},
            features=["async_job_submission", "runtime_manager_backend", "cleanrobot_app_msgs_parallel"],
        ),
        "runtime_get_job_app": build_contract_report(
            service_name="/cartographer/runtime/app/get_job",
            contract_name="runtime_get_job_app",
            service_cls=AppGetSlamJob,
            request_cls=AppGetSlamJob._request_class,
            response_cls=AppGetSlamJobResponse,
            dependencies={"job": AppSlamJobState},
            features=["async_job_query", "runtime_manager_backend", "cleanrobot_app_msgs_parallel"],
        ),
        "exe_task_server_app": build_contract_report(
            service_name="/coverage_task_manager/app/exe_task_server",
            contract_name="exe_task_server_app",
            service_cls=AppExeTask,
            request_cls=AppExeTaskRequest,
            response_cls=AppExeTaskResponse,
            dependencies={},
            features=[
                "task_execution_control",
                "start_requires_task_id",
                "readiness_double_gate",
                "cleanrobot_app_msgs_parallel",
            ],
        ),
        "restart_localization_app": build_contract_report(
            service_name="/cartographer/runtime/app/restart_localization",
            contract_name="restart_localization_app",
            service_cls=AppRestartLocalization,
            request_cls=AppRestartLocalization._request_class,
            response_cls=AppRestartLocalizationResponse,
            dependencies={},
            features=["restart_localization_service", "cleanrobot_app_msgs_parallel"],
        ),
        "profile_catalog_service_app": build_contract_report(
            service_name="/database_server/app/profile_catalog_service",
            contract_name="profile_catalog_service_app",
            service_cls=AppGetProfileCatalog,
            request_cls=AppGetProfileCatalog._request_class,
            response_cls=AppGetProfileCatalogResponse,
            dependencies={"profile": AppProfileOption},
            features=["plan_profile_catalog", "sys_profile_catalog", "cleanrobot_app_msgs_parallel"],
        ),
        "get_slam_status_app": build_contract_report(
            service_name="/clean_robot_server/app/get_slam_status",
            contract_name="get_slam_status_app",
            service_cls=AppGetSlamStatus,
            request_cls=AppGetSlamStatus._request_class,
            response_cls=AppGetSlamStatusResponse,
            dependencies={"state": AppSlamState},
            features=["structured_slam_state", "workflow_state_projection", "cleanrobot_app_msgs_parallel"],
        ),
        "get_odometry_status_app": build_contract_report(
            service_name="/clean_robot_server/app/get_odometry_status",
            contract_name="get_odometry_status_app",
            service_cls=AppGetOdometryStatus,
            request_cls=AppGetOdometryStatus._request_class,
            response_cls=AppGetOdometryStatusResponse,
            dependencies={"state": AppOdometryState},
            features=["structured_odometry_state", "generic_odom_stream_health", "cleanrobot_app_msgs_parallel"],
        ),
        "get_system_readiness_app": build_contract_report(
            service_name="/coverage_task_manager/app/get_system_readiness",
            contract_name="get_system_readiness_app",
            service_cls=AppGetSystemReadiness,
            request_cls=AppGetSystemReadiness._request_class,
            response_cls=AppGetSystemReadinessResponse,
            dependencies={"readiness": AppSystemReadiness},
            features=["task_start_gate", "readiness_aggregation", "cleanrobot_app_msgs_parallel"],
        ),
        "submit_slam_command_app": build_contract_report(
            service_name="/clean_robot_server/app/submit_slam_command",
            contract_name="submit_slam_command_app",
            service_cls=AppSubmitSlamCommand,
            request_cls=AppSubmitSlamCommand._request_class,
            response_cls=AppSubmitSlamCommandResponse,
            dependencies={"job": AppSlamJobState},
            features=[
                "async_job_submission",
                "restart_localization",
                "start_mapping",
                "save_mapping",
                "stop_mapping",
                "prepare_for_task",
                "switch_map_and_localize",
                "relocalize",
                "verify_map_revision",
                "activate_map_revision",
                "cleanrobot_app_msgs_parallel",
            ],
        ),
        "get_slam_job_app": build_contract_report(
            service_name="/clean_robot_server/app/get_slam_job",
            contract_name="get_slam_job_app",
            service_cls=AppGetSlamJob,
            request_cls=AppGetSlamJob._request_class,
            response_cls=AppGetSlamJobResponse,
            dependencies={"job": AppSlamJobState},
            features=["async_job_query", "cleanrobot_app_msgs_parallel"],
        ),
    }


def _contract_targets(local_contracts):
    return {
        _contract_param_for_service_name((report or {}).get("service_name")): report
        for report in dict(local_contracts or {}).values()
    }


def _service_targets(local_contracts):
    return {
        str((report or {}).get("service_name") or ""): report
        for report in dict(local_contracts or {}).values()
    }


def _compare_runtime_contract(local_contract, runtime_contract):
    expected_service = dict(local_contract.get("service") or {})
    runtime_service = dict((runtime_contract or {}).get("service") or {})
    issues = []
    if not runtime_service:
        issues.append("runtime contract missing service block")
    else:
        if str(runtime_service.get("md5") or "") != str(expected_service.get("md5") or ""):
            issues.append(
                "service md5 mismatch runtime=%s local=%s"
                % (str(runtime_service.get("md5") or ""), str(expected_service.get("md5") or ""))
            )
    for dep_key, expected_dep in dict(local_contract.get("dependencies") or {}).items():
        runtime_dep = dict((runtime_contract or {}).get("dependencies", {}).get(dep_key) or {})
        if not runtime_dep:
            issues.append("runtime dependency missing: %s" % dep_key)
            continue
        if str(runtime_dep.get("md5") or "") != str(expected_dep.get("md5") or ""):
            issues.append(
                "dependency %s md5 mismatch runtime=%s local=%s"
                % (
                    dep_key,
                    str(runtime_dep.get("md5") or ""),
                    str(expected_dep.get("md5") or ""),
                )
            )
    return {
        "ok": not issues,
        "issues": issues,
    }


def _build_summary_issues(runtime):
    summary_issues = []
    for key, value in dict(runtime.get("params") or {}).items():
        if not bool(((value or {}).get("comparison") or {}).get("ok", False)):
            summary_issues.append("param %s: %s" % (key, "; ".join(((value or {}).get("comparison") or {}).get("issues", []))))
    for key, value in dict(runtime.get("services") or {}).items():
        if not bool(((value or {}).get("comparison") or {}).get("ok", False)):
            summary_issues.append("service %s: %s" % (key, "; ".join(((value or {}).get("comparison") or {}).get("issues", []))))
    return summary_issues


def _build_summary(runtime):
    blocking_issues = []
    warnings = []

    for key, value in dict(runtime.get("params") or {}).items():
        if not bool(((value or {}).get("comparison") or {}).get("ok", False)):
            issue = "param %s: %s" % (key, "; ".join(((value or {}).get("comparison") or {}).get("issues", [])))
            blocking_issues.append(issue)
    for key, value in dict(runtime.get("services") or {}).items():
        if not bool(((value or {}).get("comparison") or {}).get("ok", False)):
            issue = "service %s: %s" % (key, "; ".join(((value or {}).get("comparison") or {}).get("issues", [])))
            blocking_issues.append(issue)
    return {"ok": not blocking_issues, "issues": blocking_issues, "warnings": warnings}


def _fetch_runtime(master: rosgraph.Master, local_contracts):
    runtime = {"params": {}, "services": {}}
    for key, expected in _contract_targets(local_contracts).items():
        try:
            value = master.getParam(key)
            runtime["params"][key] = {
                "value": value,
                "comparison": _compare_runtime_contract(expected, value),
            }
        except Exception as exc:
            runtime["params"][key] = {"error": str(exc), "comparison": {"ok": False, "issues": [str(exc)]}}
    for service_name, expected in _service_targets(local_contracts).items():
        service_info = {
            "expected_type": str(expected.get("service", {}).get("type") or ""),
            "expected_md5": str(expected.get("service", {}).get("md5") or ""),
        }
        try:
            service_info["type"] = rosservice.get_service_type(service_name)
            service_info["node"] = rosservice.get_service_node(service_name)
            service_info["uri"] = rosservice.get_service_uri(service_name)
            headers = rosservice.get_service_headers(service_name, service_info["uri"])
            service_info["headers"] = headers
            runtime_md5 = str((headers or {}).get("md5sum") or "")
            runtime_type = str((headers or {}).get("type") or service_info.get("type") or "")
            issues = []
            if runtime_type != service_info["expected_type"]:
                issues.append(
                    "type mismatch runtime=%s local=%s" % (runtime_type, service_info["expected_type"])
                )
            if runtime_md5 != service_info["expected_md5"]:
                issues.append(
                    "md5 mismatch runtime=%s local=%s" % (runtime_md5, service_info["expected_md5"])
                )
            service_info["comparison"] = {"ok": not issues, "issues": issues}
        except Exception as exc:
            service_info["error"] = str(exc)
            service_info["comparison"] = {"ok": False, "issues": [str(exc)]}
        runtime["services"][service_name] = service_info
    runtime["summary"] = _build_summary(runtime)
    return runtime


def _summary_ok(payload):
    runtime = dict(payload.get("runtime") or {})
    summary = dict(runtime.get("summary") or {})
    return bool(summary.get("ok", False))


def _print_text_summary(payload):
    local = dict(payload.get("local") or {})
    runtime = dict(payload.get("runtime") or {})
    summary = dict(runtime.get("summary") or {})
    print("Local contracts:")
    for key in sorted(local.keys()):
        item = dict(local.get(key) or {})
        service = dict(item.get("service") or {})
        print(
            "- %s: %s md5=%s"
            % (
                key,
                str(service.get("type") or "-"),
                str(service.get("md5") or "-"),
            )
        )
    print("Runtime checks:")
    if "error" in runtime:
        print("- runtime error: %s" % str(runtime.get("error") or "unknown error"))
    for service_name, info in sorted(dict(runtime.get("services") or {}).items()):
        cmp_info = dict((info or {}).get("comparison") or {})
        status = "OK" if bool(cmp_info.get("ok", False)) else "FAIL"
        node = str((info or {}).get("node") or "-")
        md5 = str(dict((info or {}).get("headers") or {}).get("md5sum") or (info or {}).get("expected_md5") or "-")
        print("- %s: %s node=%s md5=%s" % (service_name, status, node, md5))
        for issue in list(cmp_info.get("issues") or []):
            print("  issue: %s" % str(issue))
    for param_name, info in sorted(dict(runtime.get("params") or {}).items()):
        cmp_info = dict((info or {}).get("comparison") or {})
        status = "OK" if bool(cmp_info.get("ok", False)) else "FAIL"
        print("- %s: %s" % (param_name, status))
        for issue in list(cmp_info.get("issues") or []):
            print("  issue: %s" % str(issue))
    if summary:
        print("Overall: %s" % ("OK" if bool(summary.get("ok", False)) else "FAIL"))
        for warning in list(summary.get("warnings") or []):
            print("  warning: %s" % str(warning))


def _collect_payload():
    payload = {"local": _local_contracts()}
    try:
        master = rosgraph.Master("/check_ros_contracts")
        master.getPid()
        payload["runtime"] = _fetch_runtime(master, payload["local"])
    except Exception as exc:
        payload["runtime"] = {"error": str(exc), "summary": {"ok": False, "issues": [str(exc)], "warnings": []}}
    return payload


def main():
    parser = argparse.ArgumentParser(description="Check local ROS service/message contracts against runtime")
    parser.add_argument("--strict", action="store_true", help="exit with code 1 when a mismatch or missing contract is found")
    parser.add_argument("--text", action="store_true", help="print a concise human-readable summary instead of JSON")
    parser.add_argument("--wait-timeout", type=float, default=0.0, help="keep checking until contracts are healthy or timeout seconds elapse")
    parser.add_argument("--wait-interval", type=float, default=2.0, help="poll interval seconds while waiting")
    args = parser.parse_args()

    payload = _collect_payload()
    deadline = time.time() + max(0.0, float(args.wait_timeout or 0.0))
    while (not _summary_ok(payload)) and float(args.wait_timeout or 0.0) > 0.0 and time.time() < deadline:
        time.sleep(max(0.2, float(args.wait_interval or 2.0)))
        payload = _collect_payload()

    if args.text:
        _print_text_summary(payload)
    else:
        json.dump(payload, sys.stdout, indent=2, sort_keys=True, ensure_ascii=False)
        sys.stdout.write("\n")
    if args.strict and (not _summary_ok(payload)):
        sys.exit(1)


if __name__ == "__main__":
    main()

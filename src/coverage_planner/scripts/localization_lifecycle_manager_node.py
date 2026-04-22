#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import threading
import time
from typing import Dict, Optional, Tuple

import rospy
import tf2_ros
from cleanrobot_app_msgs.srv import (
    GetSlamJob as AppGetSlamJob,
    OperateSlamRuntime as AppOperateSlamRuntime,
    RestartLocalization as AppRestartLocalization,
    RestartLocalizationResponse as AppRestartLocalizationResponse,
    SubmitSlamCommand as AppSubmitSlamCommand,
)
from geometry_msgs.msg import PoseStamped
from nav_msgs.msg import OccupancyGrid
from coverage_planner.manual_assist_pose import (
    DEFAULT_MANUAL_ASSIST_POSE_PARAM_NS,
    consume_manual_assist_pose_override,
    inspect_manual_assist_pose_override,
    manual_assist_pose_summary,
    record_manual_assist_pose_override_status,
)
from coverage_planner.map_io import compute_occupancy_grid_md5
from coverage_planner.ops_store.store import OperationsStore, RobotRuntimeStateRecord
from coverage_planner.plan_store.store import PlanStore
from coverage_planner.ros_contract import build_contract_report, validate_ros_contract
from coverage_planner.runtime_gate_messages import (
    runtime_map_identity_unavailable_message,
    runtime_map_mismatch_reason,
    selected_map_does_not_match_requested_map_message,
)
from coverage_planner.service_mode import publish_contract_param


def _workspace_root() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))


def _map_id_from_md5(map_md5: str) -> str:
    value = str(map_md5 or "").strip()
    if not value:
        return ""
    return "map_%s" % value[:8]


class LocalizationLifecycleManagerNode:
    def __init__(self):
        self.workspace_root = _workspace_root()
        self.plan_db_path = rospy.get_param("~plan_db_path", "/data/coverage/planning.db")
        self.ops_db_path = rospy.get_param("~ops_db_path", "/data/coverage/operations.db")
        self.robot_id = str(rospy.get_param("~robot_id", "local_robot")).strip() or "local_robot"
        self.runtime_ns = str(rospy.get_param("~runtime_ns", "/cartographer/runtime")).rstrip("/")
        self.app_service_name = str(
            rospy.get_param("~app_service_name", "/cartographer/runtime/app/restart_localization")
        ).strip() or "/cartographer/runtime/app/restart_localization"
        self.app_runtime_manager_service = str(
            rospy.get_param("~app_runtime_manager_service", self.runtime_ns + "/app/operate")
        ).strip() or (self.runtime_ns + "/app/operate")
        self.app_runtime_submit_job_service = str(
            rospy.get_param("~app_runtime_submit_job_service", self.runtime_ns + "/app/submit_job")
        ).strip() or (self.runtime_ns + "/app/submit_job")
        self.app_runtime_get_job_service = str(
            rospy.get_param("~app_runtime_get_job_service", self.runtime_ns + "/app/get_job")
        ).strip() or (self.runtime_ns + "/app/get_job")
        self.app_contract_param_ns = str(
            rospy.get_param(
                "~app_contract_param_ns",
                "/cartographer/runtime/contracts/app/restart_localization",
            )
        ).strip()
        self.map_topic = str(rospy.get_param("~map_topic", "/map")).strip() or "/map"
        self.tracked_pose_topic = str(rospy.get_param("~tracked_pose_topic", "/tracked_pose")).strip() or "/tracked_pose"
        self.tf_parent_frame = str(rospy.get_param("~tf_parent_frame", "map")).strip() or "map"
        self.tf_child_frame = str(rospy.get_param("~tf_child_frame", "odom")).strip() or "odom"
        self.ready_timeout_s = max(10.0, float(rospy.get_param("~ready_timeout_s", 120.0)))
        self.tf_poll_timeout_s = max(0.05, float(rospy.get_param("~tf_poll_timeout_s", 0.2)))
        self.tracked_pose_fresh_timeout_s = max(
            0.2, float(rospy.get_param("~tracked_pose_fresh_timeout_s", 2.0))
        )
        self.command_timeout_s = max(10.0, float(rospy.get_param("~command_timeout_s", 180.0)))
        self.runtime_job_timeout_s = max(
            10.0, float(rospy.get_param("~runtime_job_timeout_s", self.command_timeout_s))
        )
        self.allow_identity_rebind_on_localize = bool(
            rospy.get_param("~allow_identity_rebind_on_localize", True)
        )
        self.manual_assist_pose_param_ns = str(
            rospy.get_param("~manual_assist_pose_param_ns", DEFAULT_MANUAL_ASSIST_POSE_PARAM_NS)
        ).strip() or DEFAULT_MANUAL_ASSIST_POSE_PARAM_NS

        self._plan_store = PlanStore(self.plan_db_path)
        self._ops = OperationsStore(self.ops_db_path)
        self._service_lock = threading.Lock()
        self._app_contract_report = self._prepare_app_contract_report()

        self._tracked_pose_ts = 0.0
        self._map_ts = 0.0
        self._map_md5 = ""

        self._tf_buffer = tf2_ros.Buffer(cache_time=rospy.Duration(30.0))
        self._tf_listener = tf2_ros.TransformListener(self._tf_buffer)

        rospy.Subscriber(self.tracked_pose_topic, PoseStamped, self._on_tracked_pose, queue_size=20)
        rospy.Subscriber(self.map_topic, OccupancyGrid, self._on_map, queue_size=2)
        self._app_service = rospy.Service(self.app_service_name, AppRestartLocalization, self._handle_restart)
        publish_contract_param(rospy, self.app_contract_param_ns, self._app_contract_report, enabled=True)

        rospy.loginfo(
            "[localization_lifecycle] ready app_service=%s md5=%s app_contract=%s runtime_submit=%s runtime_get_job=%s runtime_operate=%s",
            self.app_service_name,
            self._app_contract_report["service"]["md5"],
            self.app_contract_param_ns or "-",
            self.app_runtime_submit_job_service or "-",
            self.app_runtime_get_job_service or "-",
            self.app_runtime_manager_service or "-",
        )

    def _prepare_app_contract_report(self) -> Dict[str, object]:
        validate_ros_contract(
            "AppRestartLocalizationRequest",
            AppRestartLocalization._request_class,
            required_fields=["robot_id", "map_name", "map_revision_id"],
        )
        validate_ros_contract(
            "AppRestartLocalizationResponse",
            AppRestartLocalizationResponse,
            required_fields=["success", "message", "map_name", "map_revision_id", "localization_state"],
        )
        return build_contract_report(
            service_name=self.app_service_name,
            contract_name="restart_localization_app",
            service_cls=AppRestartLocalization,
            request_cls=AppRestartLocalization._request_class,
            response_cls=AppRestartLocalizationResponse,
            dependencies={},
            features=[
                "restart_localization_service",
                "map->odom_and_tracked_pose_health_gating",
                "cleanrobot_app_msgs_parallel",
            ],
        )

    def _runtime_param(self, key: str) -> str:
        return self.runtime_ns + "/" + str(key or "").strip()

    @staticmethod
    def _set_global_diag_param(name: str, value) -> None:
        try:
            rospy.set_param(str(name or "").strip(), value)
        except Exception:
            pass

    @staticmethod
    def _restart_response(
        *,
        success: bool,
        message: str,
        map_name: str,
        map_revision_id: str = "",
        localization_state: str,
    ) -> AppRestartLocalizationResponse:
        return AppRestartLocalizationResponse(
            success=bool(success),
            message=str(message or ""),
            map_name=str(map_name or ""),
            map_revision_id=str(map_revision_id or ""),
            localization_state=str(localization_state or ""),
        )

    def _on_tracked_pose(self, _msg: PoseStamped):
        self._tracked_pose_ts = time.time()

    def _on_map(self, msg: OccupancyGrid):
        self._map_ts = time.time()
        try:
            self._map_md5 = str(compute_occupancy_grid_md5(msg) or "").strip()
        except Exception:
            self._map_md5 = ""

    def _update_runtime_state(
        self,
        *,
        robot_id: str,
        map_name: Optional[str] = None,
        map_revision_id: Optional[str] = None,
        localization_state: Optional[str] = None,
        localization_valid: Optional[bool] = None,
    ):
        current = self._ops.get_robot_runtime_state(robot_id) or RobotRuntimeStateRecord(robot_id=robot_id)
        self._ops.upsert_robot_runtime_state(
            RobotRuntimeStateRecord(
                robot_id=robot_id,
                active_run_id=str(current.active_run_id or ""),
                active_job_id=str(current.active_job_id or ""),
                active_schedule_id=str(current.active_schedule_id or ""),
                map_name=str(map_name if map_name is not None else current.map_name or "").strip(),
                map_revision_id=str(
                    map_revision_id if map_revision_id is not None else current.map_revision_id or ""
                ).strip(),
                localization_state=(
                    str(localization_state)
                    if localization_state is not None
                    else str(current.localization_state or "")
                ),
                localization_valid=(
                    bool(localization_valid)
                    if localization_valid is not None
                    else bool(current.localization_valid)
                ),
                mission_state=str(current.mission_state or "IDLE"),
                phase=str(current.phase or "IDLE"),
                public_state=str(current.public_state or "IDLE"),
                return_to_dock_on_finish=current.return_to_dock_on_finish,
                repeat_after_full_charge=current.repeat_after_full_charge,
                armed=bool(current.armed),
                dock_state=str(current.dock_state or ""),
                battery_soc=float(current.battery_soc or 0.0),
                battery_valid=bool(current.battery_valid),
                executor_state=str(current.executor_state or ""),
                last_error_code=str(current.last_error_code or ""),
                last_error_msg=str(current.last_error_msg or ""),
                updated_ts=time.time(),
            )
        )

    def _set_localization_state(
        self,
        *,
        robot_id: str,
        map_name: str,
        state: str,
        valid: bool,
        map_revision_id: Optional[str] = None,
    ):
        state = str(state or "not_localized").strip() or "not_localized"
        rospy.set_param(self._runtime_param("localization_state"), state)
        rospy.set_param(self._runtime_param("localization_valid"), bool(valid))
        rospy.set_param(self._runtime_param("localization_stamp"), float(rospy.Time.now().to_sec()))
        if map_name:
            rospy.set_param(self._runtime_param("map_name"), str(map_name))
            self._set_global_diag_param("/map_name", str(map_name))
        if map_revision_id is not None:
            rospy.set_param(self._runtime_param("current_map_revision_id"), str(map_revision_id or "").strip())
            self._set_global_diag_param("/map_revision_id", str(map_revision_id or "").strip())
        self._update_runtime_state(
            robot_id=robot_id,
            map_name=str(map_name or "").strip(),
            map_revision_id=map_revision_id,
            localization_state=state,
            localization_valid=bool(valid),
        )

    def _resolve_asset(self, *, robot_id: str, map_name: str, map_revision_id: str = "") -> Optional[Dict[str, object]]:
        normalized_map_name = str(map_name or "").strip()
        if normalized_map_name.endswith(".pbstream"):
            normalized_map_name = normalized_map_name[:-len(".pbstream")]
        normalized_revision_id = str(map_revision_id or "").strip()
        asset = None
        if normalized_revision_id:
            asset = self._plan_store.resolve_map_asset(
                revision_id=normalized_revision_id,
                robot_id=robot_id,
            )
            resolved_map_name = str((asset or {}).get("map_name") or "").strip()
            if normalized_map_name and resolved_map_name and resolved_map_name != normalized_map_name:
                raise ValueError("map revision does not match selected map")
            return asset
        if normalized_map_name:
            resolve_revision = getattr(self._plan_store, "resolve_map_revision", None)
            if callable(resolve_revision):
                resolved_revision = resolve_revision(
                    map_name=normalized_map_name,
                    robot_id=robot_id,
                )
                if resolved_revision is not None:
                    return resolved_revision
            active_asset = self._plan_store.get_active_map(robot_id=robot_id) or {}
            active_map_name = str((active_asset or {}).get("map_name") or "").strip()
            active_revision_id = str((active_asset or {}).get("revision_id") or "").strip()
            if active_revision_id and active_map_name == normalized_map_name:
                active_revision = self._plan_store.resolve_map_asset(
                    revision_id=active_revision_id,
                    robot_id=robot_id,
                )
                if active_revision is not None:
                    return active_revision
                return active_asset
            return self._plan_store.resolve_map_asset(
                map_name=normalized_map_name,
                robot_id=robot_id,
            )
        return self._plan_store.get_active_map(robot_id=robot_id)

    def _delegate_restart_to_runtime_manager(self, *, robot_id: str, map_name: str, map_revision_id: str = ""):
        submitted_job_id = ""
        manual_assist_pose_info = inspect_manual_assist_pose_override(
            self.manual_assist_pose_param_ns,
            requested_map_name=map_name,
            requested_revision_id=map_revision_id,
        )
        manual_assist_pose = dict(manual_assist_pose_info.get("override") or {}) or None
        manual_assist_pose_status = str(manual_assist_pose_info.get("status") or "").strip()
        if manual_assist_pose_status in ("scope_mismatch", "missing_scope", "invalid_pose", "invalid_payload"):
            manual_assist_message = str(
                manual_assist_pose_info.get("message")
                or "manual assist pose override invalid"
            ).strip()
            manual_assist_ref = {
                "param_ns": str(
                    manual_assist_pose_info.get("param_ns")
                    or self.manual_assist_pose_param_ns
                    or DEFAULT_MANUAL_ASSIST_POSE_PARAM_NS
                ).strip() or DEFAULT_MANUAL_ASSIST_POSE_PARAM_NS,
            }
            record_manual_assist_pose_override_status(
                manual_assist_ref,
                status="restart_localization_pose_%s" % manual_assist_pose_status,
                map_name=str(map_name or ""),
                map_revision_id=str(map_revision_id or "").strip(),
                message=manual_assist_message,
                localization_state="manual_assist_required",
                used=False,
            )
            rospy.logwarn(
                "[localization_lifecycle] restart_localization rejected invalid manual assist pose robot=%s map=%s revision=%s reason=%s",
                str(robot_id or self.robot_id),
                str(map_name or "") or "-",
                str(map_revision_id or "").strip() or "-",
                manual_assist_message,
            )
            return self._restart_response(
                success=False,
                message=manual_assist_message,
                map_name=str(map_name or ""),
                map_revision_id=str(map_revision_id or "").strip(),
                localization_state="manual_assist_required",
            )
        submit_service_name = str(getattr(self, "app_runtime_submit_job_service", "") or "").strip()
        submit_service_cls = AppSubmitSlamCommand
        get_job_service_name = str(getattr(self, "app_runtime_get_job_service", "") or "").strip()
        get_job_service_cls = AppGetSlamJob
        submit_get_job_ready = False
        if submit_service_name and get_job_service_name:
            try:
                rospy.wait_for_service(submit_service_name, timeout=1.0)
                rospy.wait_for_service(get_job_service_name, timeout=1.0)
                submit_get_job_ready = True
            except Exception:
                submit_get_job_ready = False
        if not submit_get_job_ready:
            if manual_assist_pose:
                rospy.logwarn(
                    "[localization_lifecycle] runtime app submit_job/get_job unavailable; restart fallback cannot consume %s",
                    manual_assist_pose_summary(manual_assist_pose),
                )
                record_manual_assist_pose_override_status(
                    manual_assist_pose,
                    status="restart_localization_runtime_operate_pose_not_supported",
                    map_name=str(map_name or ""),
                    map_revision_id=str(map_revision_id or "").strip(),
                    message="runtime app submit_job/get_job unavailable",
                    used=False,
                )
        else:
            try:
                if manual_assist_pose:
                    rospy.loginfo(
                        "[localization_lifecycle] runtime app submit_job path using %s",
                        manual_assist_pose_summary(manual_assist_pose),
                    )
                submit_cli = rospy.ServiceProxy(submit_service_name, submit_service_cls)
                get_job_cli = rospy.ServiceProxy(get_job_service_name, get_job_service_cls)
                submit_resp = submit_cli(
                    operation=int(submit_service_cls._request_class.prepare_for_task),
                    robot_id=str(robot_id or self.robot_id),
                    map_name=str(map_name or ""),
                    map_revision_id=str(map_revision_id or "").strip(),
                    set_active=False,
                    description="localization lifecycle restart_localization app path",
                    frame_id=str((manual_assist_pose or {}).get("frame_id") or "map"),
                    has_initial_pose=bool(manual_assist_pose),
                    initial_pose_x=float((manual_assist_pose or {}).get("initial_pose_x") or 0.0),
                    initial_pose_y=float((manual_assist_pose or {}).get("initial_pose_y") or 0.0),
                    initial_pose_yaw=float((manual_assist_pose or {}).get("initial_pose_yaw") or 0.0),
                    save_map_name="",
                    include_unfinished_submaps=False,
                    set_active_on_save=False,
                    switch_to_localization_after_save=False,
                    relocalize_after_switch=False,
                )
                submitted_job_id = str(getattr(submit_resp, "job_id", "") or "").strip()
                if not bool(getattr(submit_resp, "accepted", False)):
                    submit_job = getattr(submit_resp, "job", None)
                    if manual_assist_pose:
                        record_manual_assist_pose_override_status(
                            manual_assist_pose,
                            status="restart_localization_submit_rejected",
                            map_name=str(map_name or ""),
                            map_revision_id=str(map_revision_id or "").strip(),
                            message=str(
                                getattr(submit_resp, "message", "")
                                or getattr(submit_resp, "error_code", "")
                                or "runtime submit_job rejected"
                            ),
                            localization_state=str(
                                getattr(submit_job, "localization_state", "") or "not_localized"
                            ),
                            used=False,
                        )
                    return self._restart_response(
                        success=False,
                        message=str(
                            getattr(submit_resp, "message", "")
                            or getattr(submit_resp, "error_code", "")
                            or "runtime submit_job rejected"
                        ),
                        map_name=str(getattr(submit_resp, "map_name", "") or map_name or ""),
                        map_revision_id=str(
                            getattr(submit_job, "resolved_map_revision_id", "")
                            or getattr(submit_job, "requested_map_revision_id", "")
                            or map_revision_id
                            or ""
                        ).strip(),
                        localization_state=str(
                            getattr(submit_job, "localization_state", "") or "not_localized"
                        ),
                    )
                if manual_assist_pose:
                    if bool((manual_assist_pose or {}).get("consume_once", True)):
                        consume_manual_assist_pose_override(
                            manual_assist_pose,
                            status="submitted_restart_localization_manual_assist",
                            map_name=str(map_name or ""),
                            map_revision_id=str(map_revision_id or "").strip(),
                            job_id=submitted_job_id,
                            used=False,
                        )
                    else:
                        record_manual_assist_pose_override_status(
                            manual_assist_pose,
                            status="submitted_restart_localization_manual_assist",
                            map_name=str(map_name or ""),
                            map_revision_id=str(map_revision_id or "").strip(),
                            job_id=submitted_job_id,
                            used=False,
                        )
                if not submitted_job_id:
                    if manual_assist_pose:
                        record_manual_assist_pose_override_status(
                            manual_assist_pose,
                            status="restart_localization_missing_job_id",
                            map_name=str(map_name or ""),
                            map_revision_id=str(map_revision_id or "").strip(),
                            message="runtime submit_job accepted without job_id",
                            used=False,
                        )
                    return self._restart_response(
                        success=False,
                        message="runtime submit_job accepted without job_id",
                        map_name=str(getattr(submit_resp, "map_name", "") or map_name or ""),
                        map_revision_id=str(map_revision_id or "").strip(),
                        localization_state=str(
                            getattr(getattr(submit_resp, "job", None), "localization_state", "") or "not_localized"
                        ),
                    )
                deadline = time.time() + float(self.runtime_job_timeout_s)
                last_message = "waiting slam job=%s" % submitted_job_id
                last_state = str(getattr(getattr(submit_resp, "job", None), "localization_state", "") or "").strip()
                while (time.time() < deadline) and (not rospy.is_shutdown()):
                    job_resp = get_job_cli(job_id=submitted_job_id, robot_id=str(robot_id or self.robot_id))
                    if not bool(getattr(job_resp, "found", False)):
                        last_message = str(getattr(job_resp, "message", "") or "slam job not found")
                        rospy.sleep(0.2)
                        continue
                    job = getattr(job_resp, "job", None)
                    if job is None:
                        last_message = str(getattr(job_resp, "message", "") or "slam job unavailable")
                        rospy.sleep(0.2)
                        continue
                    last_message = str(
                        getattr(job, "message", "")
                        or getattr(job, "progress_text", "")
                        or getattr(job_resp, "message", "")
                        or "slam restart localization running"
                    )
                    last_state = str(getattr(job, "localization_state", "") or last_state or "").strip()
                    if bool(getattr(job, "done", False)):
                        response_success = bool(getattr(job, "success", False))
                        response_map_name = str(
                            getattr(job, "resolved_map_name", "")
                            or getattr(job, "requested_map_name", "")
                            or map_name
                            or ""
                        )
                        response_revision_id = str(
                            getattr(job, "resolved_map_revision_id", "")
                            or getattr(job, "requested_map_revision_id", "")
                            or map_revision_id
                            or ""
                        ).strip()
                        response_localization_state = str(
                            getattr(job, "localization_state", "")
                            or ("localized" if response_success else "not_localized")
                        )
                        if manual_assist_pose:
                            already_ready = response_success and ("already ready" in str(last_message or "").strip().lower())
                            if response_success:
                                final_status = (
                                    "restart_localization_already_ready"
                                    if already_ready
                                    else "restart_localization_succeeded"
                                )
                            elif str(response_localization_state or "").strip().lower() == "manual_assist_required":
                                final_status = "restart_localization_manual_assist_required"
                            else:
                                final_status = "restart_localization_failed"
                            record_manual_assist_pose_override_status(
                                manual_assist_pose,
                                status=final_status,
                                map_name=response_map_name,
                                map_revision_id=response_revision_id,
                                message=last_message,
                                localization_state=response_localization_state,
                                job_id=submitted_job_id,
                                used=False if already_ready else True,
                            )
                        return self._restart_response(
                            success=response_success,
                            message=last_message,
                            map_name=response_map_name,
                            map_revision_id=response_revision_id,
                            localization_state=response_localization_state,
                        )
                    rospy.sleep(0.2)
                if manual_assist_pose:
                    record_manual_assist_pose_override_status(
                        manual_assist_pose,
                        status="restart_localization_timeout",
                        map_name=str(map_name or ""),
                        map_revision_id=str(map_revision_id or "").strip(),
                        message="runtime submit_job timeout after %.1fs: %s"
                        % (float(self.runtime_job_timeout_s), str(last_message or "timeout")),
                        localization_state=str(last_state or "not_localized"),
                        job_id=submitted_job_id,
                        used=True,
                    )
                return self._restart_response(
                    success=False,
                    message="runtime submit_job timeout after %.1fs: %s"
                    % (float(self.runtime_job_timeout_s), str(last_message or "timeout")),
                    map_name=str(map_name or ""),
                    map_revision_id=str(map_revision_id or "").strip(),
                    localization_state=str(last_state or "not_localized"),
                )
            except Exception as exc:
                if manual_assist_pose:
                    record_manual_assist_pose_override_status(
                        manual_assist_pose,
                        status="restart_localization_query_failed" if submitted_job_id else "restart_localization_submit_failed",
                        map_name=str(map_name or ""),
                        map_revision_id=str(map_revision_id or "").strip(),
                        message=str(exc),
                        localization_state="not_localized",
                        job_id=submitted_job_id,
                        used=bool(submitted_job_id),
                    )
                if submitted_job_id:
                    return self._restart_response(
                        success=False,
                        message="runtime submit_job query failed: %s" % str(exc),
                        map_name=str(map_name or ""),
                        map_revision_id=str(map_revision_id or "").strip(),
                        localization_state="not_localized",
                    )
        operate_service_name = str(getattr(self, "app_runtime_manager_service", "") or "").strip()
        operate_service_cls = AppOperateSlamRuntime
        try:
            rospy.wait_for_service(operate_service_name, timeout=1.0)
        except Exception as exc:
            return self._restart_response(
                success=False,
                message="slam runtime manager unavailable: %s" % str(exc),
                map_name=str(map_name or ""),
                map_revision_id=str(map_revision_id or "").strip(),
                localization_state="not_localized",
            )
        cli = rospy.ServiceProxy(operate_service_name, operate_service_cls)
        resp = cli(
            operation=int(operate_service_cls._request_class.restart_localization),
            robot_id=str(robot_id or self.robot_id),
            map_name=str(map_name or ""),
            map_revision_id=str(map_revision_id or "").strip(),
            set_active=False,
            description="",
        )
        if manual_assist_pose:
            record_manual_assist_pose_override_status(
                manual_assist_pose,
                status=(
                    "restart_localization_runtime_operate_completed_without_pose"
                    if bool(getattr(resp, "success", False))
                    else "restart_localization_runtime_operate_failed_without_pose"
                ),
                map_name=str(getattr(resp, "map_name", "") or map_name or ""),
                map_revision_id=str(getattr(resp, "map_revision_id", "") or map_revision_id or "").strip(),
                message=str(getattr(resp, "message", "") or ""),
                localization_state=str(getattr(resp, "localization_state", "") or "not_localized"),
                used=False,
            )
        return self._restart_response(
            success=bool(getattr(resp, "success", False)),
            message=str(getattr(resp, "message", "") or ""),
            map_name=str(getattr(resp, "map_name", "") or map_name or ""),
            map_revision_id=str(getattr(resp, "map_revision_id", "") or map_revision_id or "").strip(),
            localization_state=str(getattr(resp, "localization_state", "") or "not_localized"),
        )

    def _rebind_map_identity_from_runtime(
        self,
        *,
        map_name: str,
        asset: Dict[str, object],
    ) -> Optional[Dict[str, object]]:
        if not self.allow_identity_rebind_on_localize:
            return None
        runtime_map_md5 = str(self._map_md5 or "").strip()
        if not map_name or not runtime_map_md5:
            return None
        asset_map_id = str((asset or {}).get("map_id") or "").strip()
        asset_map_md5 = str((asset or {}).get("map_md5") or "").strip()
        if asset_map_id or asset_map_md5:
            return None
        rebound = self._plan_store.rebind_map_identity(
            map_name=map_name,
            map_id=_map_id_from_md5(runtime_map_md5),
            map_md5=runtime_map_md5,
            old_map_id=asset_map_id,
            old_map_md5=asset_map_md5,
        )
        rospy.logwarn(
            "[localization_lifecycle] rebound map identity map=%s old_id=%s old_md5=%s new_id=%s new_md5=%s",
            map_name,
            str((asset or {}).get("map_id") or "").strip() or "-",
            str((asset or {}).get("map_md5") or "").strip() or "-",
            str((rebound or {}).get("map_id") or "").strip() or "-",
            str((rebound or {}).get("map_md5") or "").strip() or "-",
        )
        return rebound

    def _runtime_map_revision_id(self) -> str:
        try:
            return str(rospy.get_param(self._runtime_param("current_map_revision_id"), "") or "").strip()
        except Exception:
            return ""

    def _runtime_map_matches(self, *, target_md5: str = "", target_revision_id: str = "") -> bool:
        normalized_revision_id = str(target_revision_id or "").strip()
        if normalized_revision_id:
            runtime_revision_id = self._runtime_map_revision_id()
            if runtime_revision_id:
                return runtime_revision_id == normalized_revision_id
        normalized_md5 = str(target_md5 or "").strip()
        return (not normalized_md5) or (str(self._map_md5 or "").strip() == normalized_md5)

    def _wait_until_ready(self, *, robot_id: str, asset: Dict[str, object], started_after_ts: float) -> Tuple[bool, str]:
        deadline = time.time() + self.ready_timeout_s
        target_name = str((asset or {}).get("map_name") or "").strip()
        target_revision_id = str((asset or {}).get("revision_id") or "").strip()
        last_reason = "waiting_localization_ready"
        while (time.time() < deadline) and (not rospy.is_shutdown()):
            now = time.time()
            tf_ok = False
            try:
                tf_ok = bool(
                    self._tf_buffer.can_transform(
                        self.tf_parent_frame,
                        self.tf_child_frame,
                        rospy.Time(0),
                        rospy.Duration(self.tf_poll_timeout_s),
                    )
                )
            except Exception:
                tf_ok = False

            tracked_ok = (
                self._tracked_pose_ts >= started_after_ts
                and (now - self._tracked_pose_ts) <= self.tracked_pose_fresh_timeout_s
            )
            runtime_revision_id = self._runtime_map_revision_id()
            map_ready = self._map_ts >= started_after_ts and bool(self._map_md5 or runtime_revision_id)
            selected = self._plan_store.get_active_map(robot_id=robot_id) or {}
            selected_name = str(selected.get("map_name") or "").strip()
            selected_revision_id = str(selected.get("revision_id") or "").strip()
            pending_switch = self._plan_store.get_pending_map_switch(robot_id=robot_id) or {}
            pending_target_name = str(pending_switch.get("target_map_name") or "").strip()
            selected_ok = (not target_name) or (selected_name == target_name) or (pending_target_name == target_name)
            canonical_asset = dict(asset or {})
            target_md5 = str(canonical_asset.get("map_md5") or "").strip()
            map_match = self._runtime_map_matches(
                target_md5=target_md5,
                target_revision_id=target_revision_id,
            )

            if tf_ok and tracked_ok and map_ready and selected_ok and (not map_match):
                try:
                    rebound = self._rebind_map_identity_from_runtime(map_name=target_name, asset=canonical_asset)
                except Exception as exc:
                    rospy.logwarn(
                        "[localization_lifecycle] failed to rebind map identity map=%s err=%s",
                        target_name or "-",
                        exc,
                    )
                else:
                    if rebound:
                        canonical_asset = dict(rebound)
                        target_md5 = str(canonical_asset.get("map_md5") or "").strip()
                        map_match = self._runtime_map_matches(
                            target_md5=target_md5,
                            target_revision_id=target_revision_id,
                        )

            if tf_ok and tracked_ok and map_ready and map_match and selected_ok:
                map_id = str((canonical_asset or {}).get("map_id") or "").strip()
                map_md5 = str(target_md5 or self._map_md5 or "").strip()
                if map_id:
                    self._set_global_diag_param("/map_id", map_id)
                if map_md5:
                    self._set_global_diag_param("/map_md5", map_md5)
                self._set_localization_state(
                    robot_id=robot_id,
                    map_name=target_name,
                    state="localized",
                    valid=True,
                    map_revision_id=target_revision_id,
                )
                return True, "localized"

            if not tf_ok:
                last_reason = "map->odom TF not ready"
            elif not tracked_ok:
                last_reason = "tracked_pose not fresh"
            elif not map_ready:
                last_reason = runtime_map_identity_unavailable_message()
            elif not map_match:
                last_reason = (
                    runtime_map_mismatch_reason(
                        expected_label="requested map",
                        active_revision_id=target_revision_id,
                        active_map_name=target_name,
                        active_map_md5=target_md5,
                        runtime_revision_id=runtime_revision_id,
                        runtime_map_md5=str(self._map_md5 or ""),
                    )
                    or "runtime map does not match requested map"
                )
            elif not selected_ok:
                last_reason = selected_map_does_not_match_requested_map_message(
                    selected_name,
                    target_name,
                    selected_revision_id=selected_revision_id,
                    requested_revision_id=target_revision_id,
                )

            rospy.sleep(0.2)

        self._set_localization_state(
            robot_id=robot_id,
            map_name=target_name,
            state="not_localized",
            valid=False,
            map_revision_id=target_revision_id,
        )
        return False, last_reason

    def _handle_restart(self, req):
        robot_id = str(req.robot_id or self.robot_id).strip() or self.robot_id
        map_name = str(req.map_name or "").strip()
        map_revision_id = str(getattr(req, "map_revision_id", "") or "").strip()
        with self._service_lock:
            return self._delegate_restart_to_runtime_manager(
                robot_id=robot_id,
                map_name=map_name,
                map_revision_id=map_revision_id,
            )

def main():
    rospy.init_node("localization_lifecycle_manager", anonymous=False)
    LocalizationLifecycleManagerNode()
    rospy.spin()


if __name__ == "__main__":
    main()

# -*- coding: utf-8 -*-

import ast
import shlex
import math
import os
import threading
import time
import uuid
import json
import traceback
from datetime import datetime
from dataclasses import dataclass, asdict, field, is_dataclass
from typing import Optional, Tuple, Dict, List

import rospy
import rosnode
import rosservice
from std_msgs.msg import String, Bool
from std_srvs.srv import Trigger, Empty, SetBool

from cleanrobot_app_msgs.msg import (
    OdometryState,
    SlamState,
    SystemReadiness as SystemReadinessMsg,
    SubsystemReadiness,
)
from cleanrobot_app_msgs.srv import (
    ExeTask as AppExeTask,
    ExeTaskRequest as AppExeTaskRequest,
    ExeTaskResponse as AppExeTaskResponse,
    GetSlamJob as AppGetSlamJob,
    GetSystemReadiness as AppGetSystemReadiness,
    GetSystemReadinessResponse as AppGetSystemReadinessResponse,
    RestartLocalization as AppRestartLocalization,
    SubmitSlamCommand as AppSubmitSlamCommand,
)
from diagnostic_updater import Updater
from diagnostic_msgs.msg import DiagnosticStatus
from robot_platform_msgs.msg import CombinedStatus, StationStatus

from coverage_msgs.msg import TaskState as TaskStateMsg
from coverage_msgs.msg import RunProgress as RunProgressMsg
from sensor_msgs.msg import BatteryState
from coverage_planner.manual_assist_pose import (
    DEFAULT_MANUAL_ASSIST_POSE_PARAM_NS,
    consume_manual_assist_pose_override,
    inspect_manual_assist_pose_override,
    manual_assist_pose_summary,
    record_manual_assist_pose_override_status,
)
from coverage_planner.map_asset_status import map_asset_verification_error
from coverage_planner.plan_store.store import PlanStore
from coverage_planner.ops_store.store import JobRecord, OperationsStore

from .mbf_move_base import MBFMoveBase
from .task_state_store import TaskState, TaskStateStore
from .schedule_store import ScheduleRunStore
from .scheduler import Scheduler, ScheduleJob
from .mode_profiles import ModeProfileCatalog
from .nav_profile_applier import NavProfileApplier
from .mission_store import MissionStore
from coverage_planner.canonical_contract_types import (
    APP_GET_SLAM_JOB_SERVICE_TYPE,
    APP_RESTART_LOCALIZATION_SERVICE_TYPE,
    APP_SUBMIT_SLAM_COMMAND_SERVICE_TYPE,
)

from .health_monitor import HealthMonitor, HealthResult
from .map_identity import ensure_map_identity, get_runtime_map_revision_id, get_runtime_map_scope
from coverage_planner.ros_contract import build_contract_report, validate_ros_contract
from coverage_planner.runtime_gate_messages import (
    manual_assist_metadata,
    no_current_active_map_selected_message,
    odometry_not_ready_message,
    runtime_localization_relocalize_required_before_task_message,
    runtime_localization_not_ready_message,
    runtime_map_identity_unavailable_message,
    runtime_map_mismatch_reason,
    runtime_map_switch_required_before_task_message,
    selected_map_does_not_match_requested_map_message,
)
from coverage_planner.service_mode import publish_contract_param
from coverage_planner.slam_workflow_semantics import localization_is_ready, localization_needs_manual_assist

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None

try:
    import pytz
except Exception:  # pragma: no cover
    pytz = None


def _get_timezone(name: str, *, strict: bool = False):
    value = str(name or "").strip()
    if not value:
        return None
    if ZoneInfo is not None:
        try:
            return ZoneInfo(value)
        except Exception:
            pass
    if pytz is not None:
        try:
            return pytz.timezone(value)
        except Exception:
            pass
    if strict:
        raise ValueError("bad timezone='%s'" % value)
    return None


def _localize_datetime(dt: datetime, tz):
    if tz is None:
        return dt
    localize = getattr(tz, "localize", None)
    if callable(localize):
        return localize(dt)
    try:
        return dt.replace(tzinfo=tz)
    except Exception:
        return dt


def _parse_date_ymd(text: str):
    value = str(text or "").strip().replace("/", "-")
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except Exception:
        return None


def _soc_from_battery(msg: BatteryState) -> Optional[float]:
    """Return SOC in [0,1]. Accept msg.percentage as 0..1 or 0..100."""
    if msg is None:
        return None
    try:
        soc = float(msg.percentage)
    except Exception:
        return None
    if not math.isfinite(soc):
        return None
    if soc > 1.0 + 1e-6:
        soc = soc / 100.0
    return max(0.0, min(1.0, soc))


def _soc_reached_threshold(soc: Optional[float], target: float, tol: float = 1e-4) -> bool:
    if soc is None:
        return False
    try:
        return float(soc) >= (float(target) - float(tol))
    except Exception:
        return False


def _yaw_to_quat(yaw: float):
    half = 0.5 * float(yaw)
    return (0.0, 0.0, math.sin(half), math.cos(half))


def _make_pose(frame_id: str, x: float, y: float, yaw: float):
    from geometry_msgs.msg import PoseStamped
    ps = PoseStamped()
    ps.header.frame_id = str(frame_id)
    ps.header.stamp = rospy.Time.now()
    ps.pose.position.x = float(x)
    ps.pose.position.y = float(y)
    ps.pose.position.z = 0.0
    qx, qy, qz, qw = _yaw_to_quat(yaw)
    ps.pose.orientation.x = qx
    ps.pose.orientation.y = qy
    ps.pose.orientation.z = qz
    ps.pose.orientation.w = qw
    return ps


def _parse_xyyaw(val, default=(0.0, 0.0, 0.0)):
    """Support list/tuple [x,y,yaw] or string "[x,y,yaw]"."""
    if val is None:
        return tuple(default)
    if isinstance(val, (list, tuple)) and len(val) >= 3:
        return (float(val[0]), float(val[1]), float(val[2]))
    if isinstance(val, str):
        s = val.strip()
        try:
            obj = ast.literal_eval(s)
            if isinstance(obj, (list, tuple)) and len(obj) >= 3:
                return (float(obj[0]), float(obj[1]), float(obj[2]))
        except Exception:
            pass
    return tuple(default)


@dataclass
class BatterySnapshot:
    soc: Optional[float] = None
    ts: float = 0.0


@dataclass
class MsgSnapshot:
    ts: float = 0.0


@dataclass
class BoolSnapshot:
    value: Optional[bool] = None
    ts: float = 0.0


@dataclass
class OdometrySnapshot:
    msg: Optional[OdometryState] = None
    ts: float = 0.0


@dataclass
class SlamStateSnapshot:
    msg: Optional[SlamState] = None
    ts: float = 0.0


@dataclass
class ReadinessGateResult:
    blockers: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    checks: List[SubsystemReadiness] = field(default_factory=list)


@dataclass
class SlamRuntimeGateResult(ReadinessGateResult):
    active_revision_id: str = ""
    active_map_name: str = ""
    active_map_id: str = ""
    active_map_md5: str = ""
    runtime_revision_id: str = ""
    runtime_map_name: str = ""
    runtime_map_id: str = ""
    runtime_map_md5: str = ""
    manual_assist_required: bool = False
    manual_assist_map_name: str = ""
    manual_assist_map_revision_id: str = ""
    manual_assist_retry_action: str = ""
    manual_assist_guidance: str = ""


class TaskManager:
    """Task layer orchestrator.

    Responsibilities (commercial-grade split):
      - Own the formal task execution contract (`/coverage_task_manager/app/exe_task_server`).
      - Keep a separate String cmd surface only for internal/dev/debug commands.
      - Monitor battery and, when needed, suspend mission -> dock -> wait charge -> undock -> resume.
      - Forward mission controls to coverage_executor.
      - Scheduler for recurring jobs (WALL-TIME reliable even if /use_sim_time is active).
    """

    def __init__(
        self,
        *,
        ops_db_path: str = "",
        plan_db_path: str = "",
        robot_id: str = "local_robot",
        task_persist_enable: bool = True,
        task_restore_enable: bool = False,
        task_state_max_age_s: float = 7 * 24 * 3600,
        frame_id: str = "map",
        zone_id_default: str = "zone_demo",
        default_plan_profile_name: str = "cover_standard",
        default_sys_profile_name: str = "standard",
        default_clean_mode: str = "",
        default_return_to_dock_on_finish: bool = False,

        # sys profile catalog
        mode_profiles: Optional[dict] = None,
        nav_profile_apply_enable: bool = False,
        dock_sys_profile_name: str = "",
        inspect_sys_profile_name: str = "eco",
        executor_cmd_topic: str = "/coverage_executor/cmd",
        executor_state_topic: str = "/coverage_executor/state",
        executor_progress_topic: str = "/coverage_executor/run_progress",

        # sys_profile switching safety
        sys_profile_switch_policy: str = "defer",
        sys_profile_switch_wait_s: float = 20.0,

        cmd_topic: str = "~cmd",
        app_exe_task_service_name: str = "/coverage_task_manager/app/exe_task_server",
        app_exe_task_contract_param_ns: str = "/coverage_task_manager/contracts/app/exe_task_server",
        battery_topic: str = "/battery_state",
        battery_stale_timeout_s: float = 5.0,

        auto_charge_enable: bool = True,
        trigger_when_idle: bool = False,
        low_soc: float = 0.20,
        resume_soc: float = 0.80,
        rearm_soc: float = 0.30,
        dock_xyyaw: Tuple[float, float, float] = (0.0, 0.0, 0.0),
        dock_stage1_xyyaw: Optional[Tuple[float, float, float]] = None,
        dock_two_stage_enable: bool = False,
        dock_stage2_controller: str = "MyPlanner",
        dock_retry_limit: int = 2,
        undock_forward_m: float = 0.6,
        dock_timeout_s: float = 600.0,
        wait_executor_paused_s: float = 20.0,
        charge_timeout_s: float = 14400.0,
        charge_battery_stale_timeout_s: float = 300.0,

        mbf_move_base_action: str = "/move_base_flex/move_base",
        mbf_planner: str = "",
        mbf_controller: str = "",
        mbf_recovery: str = "",

        # Health monitor
        health_enable: bool = True,
        health_check_hz: float = 1.0,
        base_frame: str = "base_footprint",
        mbf_exe_path_action: str = "/move_base_flex/exe_path",
        exec_progress_timeout_s: float = 3.0,
        tf_timeout_s: float = 0.1,
        tf_max_age_s: float = 0.6,
        progress_stall_timeout_s: float = 20.0,
        progress_stall_min_delta: float = 0.002,
        auto_pause_on_health_error: bool = True,
        auto_pause_cmd: str = "pause",
        health_auto_recover_enable: bool = True,
        health_auto_recover_max: int = 2,
        health_auto_resume_delay_s: float = 3.0,
        health_auto_recover_codes: Optional[object] = None,

        # localization quality (optional)
        loc_enable: bool = False,
        loc_require: bool = False,
        loc_topic: str = '',
        loc_stale_s: float = 1.0,
        loc_cov_xy_max: float = 0.25,
        loc_cov_yaw_max: float = 0.30,
        loc_jump_xy_m: float = 1.0,
        loc_jump_yaw_rad: float = 1.0,

        # no-motion stall (TF displacement)
        pose_stall_enable: bool = True,
        pose_stall_timeout_s: float = 15.0,
        pose_stall_min_move_m: float = 0.05,
        pose_stall_min_yaw_rad: float = 0.12,

        # Scheduler (recurring jobs)
        scheduler_enable: bool = False,
        schedules: Optional[object] = None,
        scheduler_window_s: float = 60.0,
        scheduler_check_hz: float = 1.0,
        scheduler_catch_up_s: float = 600.0,
        scheduler_retry_backoff_s: float = 30.0,
        scheduler_reload_enable: bool = True,
        scheduler_reload_period_s: float = 5.0,

        # optional: real-robot dock supply manager integration
        dock_supply_enable: bool = False,
        dock_supply_start_service: str = "/dock_supply/start",
        dock_supply_cancel_service: str = "/dock_supply/cancel",
        dock_supply_state_topic: str = "/dock_supply/state",
        dock_supply_set_defer_exit_service: str = "/dock_supply/set_defer_exit",
        dock_supply_exit_service: str = "/dock_supply/exit",
        app_restart_localization_service: str = "/cartographer/runtime/app/restart_localization",
        app_slam_submit_command_service: str = "/clean_robot_server/app/submit_slam_command",
        app_slam_get_job_service: str = "/clean_robot_server/app/get_slam_job",
        restart_localization_timeout_s: float = 180.0,
        clear_costmaps_service: str = "/move_base_flex/clear_costmaps",
        manual_assist_pose_param_ns: str = DEFAULT_MANUAL_ASSIST_POSE_PARAM_NS,
        require_managed_map_asset: bool = False,
        require_runtime_localized_before_start: bool = False,
        require_runtime_map_match: bool = False,
        runtime_localization_state_param: str = "/cartographer/runtime/localization_state",
        runtime_localization_valid_param: str = "/cartographer/runtime/localization_valid",
        require_odometry_healthy_before_start: bool = True,
        odometry_state_topic: str = "/clean_robot_server/odometry_state",
        odometry_state_stale_timeout_s: float = 5.0,
        slam_state_topic: str = "/clean_robot_server/slam_state",
        slam_state_stale_timeout_s: float = 5.0,

        # System readiness aggregation
        readiness_publish_hz: float = 1.0,
        app_readiness_service_name: str = "/coverage_task_manager/app/get_system_readiness",
        app_readiness_contract_param_ns: str = "/coverage_task_manager/contracts/app/get_system_readiness",
        runtime_map_topic: str = "/map",
        combined_status_topic: str = "/combined_status",
        combined_status_stale_timeout_s: float = 5.0,
        station_status_topic: str = "/station_status",
        station_status_stale_timeout_s: float = 5.0,
        mcore_connected_topic: str = "/mcore_tcp_bridge/connected",
        station_connected_topic: str = "/station_tcp_bridge/connected",
        connected_stale_timeout_s: float = 5.0,
    ):
        self.frame_id = str(frame_id)
        self.zone_id_default = str(zone_id_default)
        self._robot_id = str(robot_id or "local_robot").strip() or "local_robot"
        self._active_zone: str = self.zone_id_default

        # Task intent params:
        self._task_plan_profile_name: str = str(default_plan_profile_name or "cover_standard").strip() or "cover_standard"
        self._default_sys_profile_name: str = str(default_sys_profile_name or "standard").strip() or "standard"
        self._task_sys_profile_name: str = self._default_sys_profile_name
        self._task_clean_mode: str = str(default_clean_mode or "").strip()
        self._default_return_to_dock_on_finish = bool(default_return_to_dock_on_finish)
        self._task_return_to_dock_on_finish: bool = self._default_return_to_dock_on_finish
        self._task_repeat_after_full_charge: bool = False
        self._task_map_name: str = ""
        self._task_map_revision_id: str = ""
        self._plan_db_path = os.path.expanduser(str(plan_db_path or "").strip())
        self._plan_store: Optional[PlanStore] = None
        self._ops_store: Optional[OperationsStore] = None
        if self._plan_db_path:
            try:
                self._plan_store = PlanStore(self._plan_db_path)
            except Exception as e:
                rospy.logerr("[TASK] map asset DB disabled: plan_db_path=%s err=%s", self._plan_db_path, str(e))
                self._plan_store = None
        self._require_managed_map_asset = bool(require_managed_map_asset)
        self._require_runtime_localized_before_start = bool(require_runtime_localized_before_start)
        self._require_runtime_map_match = bool(require_runtime_map_match)
        self._runtime_localization_state_param = (
            str(runtime_localization_state_param or "/cartographer/runtime/localization_state").strip()
            or "/cartographer/runtime/localization_state"
        )
        self._runtime_localization_valid_param = (
            str(runtime_localization_valid_param or "/cartographer/runtime/localization_valid").strip()
            or "/cartographer/runtime/localization_valid"
        )
        self._require_odometry_healthy_before_start = bool(require_odometry_healthy_before_start)
        self._odometry_state_topic = (
            str(odometry_state_topic or "/clean_robot_server/odometry_state").strip()
            or "/clean_robot_server/odometry_state"
        )
        self._odometry_state_stale_timeout_s = max(0.5, float(odometry_state_stale_timeout_s))
        self._slam_state_topic = (
            str(slam_state_topic or "/clean_robot_server/slam_state").strip()
            or "/clean_robot_server/slam_state"
        )
        self._slam_state_stale_timeout_s = max(0.5, float(slam_state_stale_timeout_s))
        self._manual_assist_pose_param_ns = (
            str(manual_assist_pose_param_ns or DEFAULT_MANUAL_ASSIST_POSE_PARAM_NS).strip()
            or DEFAULT_MANUAL_ASSIST_POSE_PARAM_NS
        )
        self._runtime_map_topic = str(runtime_map_topic or "/map").strip() or "/map"
        self._readiness_publish_hz = max(0.2, float(readiness_publish_hz))
        self._app_readiness_service_name = (
            str(app_readiness_service_name or "/coverage_task_manager/app/get_system_readiness").strip()
            or "/coverage_task_manager/app/get_system_readiness"
        )
        self._app_readiness_contract_param_ns = (
            str(
                app_readiness_contract_param_ns
                or "/coverage_task_manager/contracts/app/get_system_readiness"
            ).strip()
            or "/coverage_task_manager/contracts/app/get_system_readiness"
        )
        self._combined_status_topic = str(combined_status_topic or "/combined_status").strip() or "/combined_status"
        self._combined_status_stale_timeout_s = max(0.5, float(combined_status_stale_timeout_s))
        self._station_status_topic = str(station_status_topic or "/station_status").strip() or "/station_status"
        self._station_status_stale_timeout_s = max(0.5, float(station_status_stale_timeout_s))
        self._mcore_connected_topic = str(mcore_connected_topic or "/mcore_tcp_bridge/connected").strip() or "/mcore_tcp_bridge/connected"
        self._station_connected_topic = str(station_connected_topic or "/station_tcp_bridge/connected").strip() or "/station_tcp_bridge/connected"
        self._connected_stale_timeout_s = max(0.5, float(connected_stale_timeout_s))

        # sys profile catalog + applier
        self._mode_catalog = ModeProfileCatalog(mode_profiles or {})
        catalog_names = self._mode_catalog.names()
        if catalog_names and self._mode_catalog.get(self._default_sys_profile_name) is None:
            fallback = "standard" if self._mode_catalog.get("standard") is not None else str(catalog_names[0])
            rospy.logwarn(
                "[TASK] default_sys_profile_name=%s not found in mode_profiles, fallback=%s",
                self._default_sys_profile_name,
                fallback,
            )
            self._default_sys_profile_name = fallback
            self._task_sys_profile_name = fallback
        self._nav_applier = NavProfileApplier(enable=bool(nav_profile_apply_enable))
        self._task_clean_mode = self._resolve_effective_clean_mode(self._task_sys_profile_name, self._task_clean_mode)

        # policy profiles
        self._dock_sys_profile_name = str(dock_sys_profile_name or "").strip()
        self._inspect_sys_profile_name = str(inspect_sys_profile_name or "eco").strip()

        # Task-level persistence
        self._persist_enable = bool(task_persist_enable)
        self._restore_enable = bool(task_restore_enable)
        self._task_state_max_age_s = float(task_state_max_age_s)
        self._store = None
        self._mission_state = "IDLE"  # IDLE/RUNNING/PAUSED/ESTOP
        self._last_persist: Optional[TaskState] = None
        self._public_state = "IDLE"

        # last notable event
        self._last_event: str = ""

        self.executor_cmd_topic = str(executor_cmd_topic)
        self.executor_state_topic = str(executor_state_topic)
        self.executor_progress_topic = str(executor_progress_topic)
        self._app_exe_task_service_name = (
            str(app_exe_task_service_name or "/coverage_task_manager/app/exe_task_server").strip()
            or "/coverage_task_manager/app/exe_task_server"
        )
        self._app_exe_task_contract_param_ns = (
            str(app_exe_task_contract_param_ns or "/coverage_task_manager/contracts/app/exe_task_server").strip()
            or "/coverage_task_manager/contracts/app/exe_task_server"
        )
        self._app_readiness_contract_report = self._prepare_readiness_contract_report()
        self._app_exe_task_contract_report = self._prepare_exe_task_contract_report()

        self.battery_topic = str(battery_topic)
        self.battery_stale_timeout_s = float(battery_stale_timeout_s)

        self.auto_charge_enable = bool(auto_charge_enable)
        self.trigger_when_idle = bool(trigger_when_idle)

        self.low_soc = float(low_soc)
        self.resume_soc = float(resume_soc)
        self.rearm_soc = float(rearm_soc)

        self.dock_xyyaw = (float(dock_xyyaw[0]), float(dock_xyyaw[1]), float(dock_xyyaw[2]))
        if dock_stage1_xyyaw is None:
            dock_stage1_xyyaw = dock_xyyaw
        self.dock_stage1_xyyaw = (
            float(dock_stage1_xyyaw[0]),
            float(dock_stage1_xyyaw[1]),
            float(dock_stage1_xyyaw[2]),
        )
        self.dock_two_stage_enable = bool(dock_two_stage_enable)
        self.dock_stage2_controller = str(dock_stage2_controller or "").strip()
        self.dock_retry_limit = max(0, int(dock_retry_limit))
        self.undock_forward_m = float(undock_forward_m)
        self.dock_timeout_s = float(dock_timeout_s)
        self.wait_executor_paused_s = float(wait_executor_paused_s)
        self.charge_timeout_s = float(charge_timeout_s)
        self.charge_battery_stale_timeout_s = float(charge_battery_stale_timeout_s)

        self._lock = threading.Lock()
        self._battery = BatterySnapshot()
        self._combined_status = MsgSnapshot()
        self._station_status = MsgSnapshot()
        self._mcore_connected = BoolSnapshot()
        self._station_connected = BoolSnapshot()
        self._odometry_state = OdometrySnapshot()
        self._slam_state = SlamStateSnapshot()
        self._executor_state = ""
        # Initialize health fault fields before subscribers come online, otherwise
        # early executor callbacks can hit _snapshot() before these attributes exist.
        self._health_fault_active: bool = False
        self._health_error_code: str = ""
        self._health_error_msg: str = ""

        self._armed = True

        # Scheduler (WALL-TIME)
        self.scheduler_enable = bool(scheduler_enable)
        self.scheduler_window_s = float(scheduler_window_s)
        self.scheduler_check_hz = max(0.2, float(scheduler_check_hz))
        self.scheduler_catch_up_s = max(0.0, float(scheduler_catch_up_s))
        self._sched_retry_backoff_s = max(1.0, float(scheduler_retry_backoff_s))
        self._sched_reload_enable = bool(scheduler_reload_enable)
        self._sched_reload_period_s = max(1.0, float(scheduler_reload_period_s))
        self._sched_defaults = {
            "plan_profile_name": self._task_plan_profile_name,
            "sys_profile_name": self._task_sys_profile_name,
            "clean_mode": self._task_clean_mode,
            "return_to_dock_on_finish": self._task_return_to_dock_on_finish,
            "repeat_after_full_charge": self._task_repeat_after_full_charge,
        }
        self._sched_specs_signature = ""
        self._sched_last_reload_ts = 0.0
        self._sched_attempts: Dict[str, Dict[str, float]] = {}
        self._scheduler: Scheduler = Scheduler.from_param(
            schedules,
            defaults=self._sched_defaults,
        )
        self._sched_specs_signature = self._sched_specs_signature_of(schedules)
        self._sched_store: Optional[ScheduleRunStore] = None
        self._last_sched_tick = 0.0

        # Scheduler debug
        self._sched_debug_snapshot_period_s = float(rospy.get_param("~sched_debug_snapshot_period_s", 10.0))
        self._sched_last_snapshot_ts = 0.0
        self._sched_last_summary = ""

        # Job runner (loops)
        self._active_job_id: str = ""  # "" means manual
        self._active_schedule_id: str = ""  # non-empty only for scheduled runs
        self._job_loops_total: int = 0
        self._job_loops_done: int = 0
        self._active_run_id: str = ""
        self._active_run_loop_index: int = 0
        self._last_exec_state_seen: str = ""
        self._last_run_progress: Optional[RunProgressMsg] = None
        self._last_run_progress_ts: float = 0.0
        self._phase = "IDLE"  # IDLE/AUTO_*/MANUAL_*/PAUSED_AUTO_CHARGE
        self._stop = False
        self._dock_nav_started_ts: float = 0.0
        self._undock_nav_started_ts: float = 0.0
        self._charge_started_ts: float = 0.0
        self._charge_last_soc: Optional[float] = None
        self._charge_last_fresh_ts: float = 0.0
        self._dock_retry_count: int = 0
        self._dock_supply_exit_inflight: bool = False
        self._dock_recoverable_supply_states = {
            "FAILED_NO_DOCK_POSE_LOCK",
            "FAILED_NO_DOCK_POSE",
            "FAILED_PRECISE_DOCK_POSE_LOST",
            "FAILED_STATION_IN_PLACE",
        }

        # Dock supply integration (fine docking + station ops + charging)
        self._dock_supply_enable = bool(dock_supply_enable)
        self._dock_supply_state = "IDLE"
        self._dock_supply_state_ts = 0.0
        self._dock_supply_start_service = str(dock_supply_start_service)
        self._dock_supply_cancel_service = str(dock_supply_cancel_service)
        self._dock_supply_state_topic = str(dock_supply_state_topic)
        self._dock_supply_set_defer_exit_service = str(dock_supply_set_defer_exit_service)
        self._dock_supply_exit_service = str(dock_supply_exit_service)
        self._app_restart_localization_service = str(app_restart_localization_service)
        self._app_slam_submit_command_service = str(app_slam_submit_command_service)
        self._app_slam_get_job_service = str(app_slam_get_job_service)
        self._restart_localization_timeout_s = max(1.0, float(restart_localization_timeout_s))
        self._clear_costmaps_service = str(clear_costmaps_service)

        self._dock_supply_start_cli = None
        self._dock_supply_cancel_cli = None
        self._dock_supply_set_defer_exit_cli = None
        self._dock_supply_exit_cli = None
        self._restart_localization_cli = None
        self._slam_submit_command_cli = None
        self._slam_get_job_cli = None
        self._restart_localization_cli_service_name = ""
        self._slam_submit_command_cli_service_name = ""
        self._slam_get_job_cli_service_name = ""
        self._clear_costmaps_cli = None
        if self._dock_supply_enable:
            try:
                self._dock_supply_start_cli = rospy.ServiceProxy(self._dock_supply_start_service, Trigger)
                self._dock_supply_cancel_cli = rospy.ServiceProxy(self._dock_supply_cancel_service, Trigger)
                self._dock_supply_set_defer_exit_cli = rospy.ServiceProxy(self._dock_supply_set_defer_exit_service, SetBool)
                self._dock_supply_exit_cli = rospy.ServiceProxy(self._dock_supply_exit_service, Trigger)
                rospy.Subscriber(self._dock_supply_state_topic, String, self._on_dock_supply_state, queue_size=1)
                rospy.loginfo(
                    "[TASK] dock_supply enabled: start=%s cancel=%s defer_exit=%s exit=%s state_topic=%s",
                    self._dock_supply_start_service,
                    self._dock_supply_cancel_service,
                    self._dock_supply_set_defer_exit_service,
                    self._dock_supply_exit_service,
                    self._dock_supply_state_topic,
                )
            except Exception as e:
                rospy.logerr("[TASK] dock_supply init failed, disabled. err=%s", str(e))
                self._dock_supply_enable = False

        self._ensure_restart_localization_cli()
        self._ensure_slam_submit_command_cli()
        self._ensure_slam_get_job_cli()

        if self._clear_costmaps_service:
            try:
                self._clear_costmaps_cli = rospy.ServiceProxy(self._clear_costmaps_service, Empty)
            except Exception as e:
                rospy.logwarn(
                    "[TASK] clear costmaps service proxy init failed: service=%s err=%s",
                    self._clear_costmaps_service,
                    str(e),
                )
                self._clear_costmaps_cli = None

        # sys_profile switching policy
        self._sys_profile_switch_policy = str(sys_profile_switch_policy or "defer").strip().lower()
        if self._sys_profile_switch_policy not in ("defer", "suspend_resume"):
            rospy.logwarn("[TASK] invalid sys_profile_switch_policy=%s -> fallback defer", self._sys_profile_switch_policy)
            self._sys_profile_switch_policy = "defer"
        self._sys_profile_switch_wait_s = float(sys_profile_switch_wait_s)
        self._pending_sys_profile_name: Optional[str] = None

        # init persistence store
        if not ops_db_path:
            ops_db_path = "~/.ros/coverage/coverage.db"
        ops_db_path = os.path.expanduser(str(ops_db_path))
        try:
            self._store = TaskStateStore(str(ops_db_path), robot_id=self._robot_id) if self._persist_enable else None
        except Exception as e:
            rospy.logerr("[TASK] task persistence disabled: cannot open ops_db_path=%s err=%s", str(ops_db_path), str(e))
            self._store = None
            self._persist_enable = False

        try:
            self._ops_store = OperationsStore(str(ops_db_path))
        except Exception as e:
            rospy.logerr("[TASK] operations store disabled: cannot open ops_db_path=%s err=%s", str(ops_db_path), str(e))
            self._ops_store = None

        # Mission history store
        self._mission_store: Optional[MissionStore] = None
        if self._persist_enable:
            try:
                self._mission_store = MissionStore(str(ops_db_path))
            except Exception as e:
                rospy.logerr("[TASK] mission_store disabled: ops_db_path=%s err=%s", str(ops_db_path), str(e))
                self._mission_store = None

        # Schedule run store
        if self.scheduler_enable:
            try:
                self._sched_store = ScheduleRunStore(str(ops_db_path))
            except Exception as e:
                rospy.logerr(
                    "[TASK] scheduler disabled: cannot open schedule store ops_db_path=%s err=%s",
                    str(ops_db_path),
                    str(e),
                )
                self.scheduler_enable = False
                self._sched_store = None

        # pubs/subs
        self._cmd_pub = rospy.Publisher(self.executor_cmd_topic, String, queue_size=10)
        self._state_pub = rospy.Publisher("~state", String, queue_size=1, latch=True)
        self._event_pub = rospy.Publisher("~event", String, queue_size=50)
        self._task_state_pub = rospy.Publisher("task_state", TaskStateMsg, queue_size=10, latch=True)
        self._readiness_pub = rospy.Publisher("~system_readiness", SystemReadinessMsg, queue_size=1, latch=True)

        rospy.Subscriber(cmd_topic, String, self._on_cmd, queue_size=50)
        rospy.Subscriber(self.executor_state_topic, String, self._on_executor_state, queue_size=50)
        rospy.Subscriber(self.executor_progress_topic, RunProgressMsg, self._on_executor_progress, queue_size=50)
        rospy.Subscriber(self.battery_topic, BatteryState, self._on_battery, queue_size=10)
        rospy.Subscriber(self._combined_status_topic, CombinedStatus, self._on_combined_status, queue_size=10)
        rospy.Subscriber(self._station_status_topic, StationStatus, self._on_station_status, queue_size=10)
        rospy.Subscriber(self._mcore_connected_topic, Bool, self._on_mcore_connected, queue_size=10)
        rospy.Subscriber(self._station_connected_topic, Bool, self._on_station_connected, queue_size=10)
        if self._odometry_state_topic:
            rospy.Subscriber(self._odometry_state_topic, OdometryState, self._on_odometry_state, queue_size=10)
        if self._slam_state_topic:
            rospy.Subscriber(self._slam_state_topic, SlamState, self._on_slam_state, queue_size=10)

        self._app_readiness_srv = rospy.Service(
            self._app_readiness_service_name,
            AppGetSystemReadiness,
            self._on_get_system_readiness_app,
        )
        self._app_exe_task_srv = rospy.Service(
            self._app_exe_task_service_name,
            AppExeTask,
            self._on_exe_task_app,
        )
        publish_contract_param(rospy, self._app_readiness_contract_param_ns, self._app_readiness_contract_report, enabled=True)
        publish_contract_param(rospy, self._app_exe_task_contract_param_ns, self._app_exe_task_contract_report, enabled=True)

        self._task_state_timer = rospy.Timer(rospy.Duration(0.5), self._on_task_state_timer)
        self._readiness_timer = rospy.Timer(rospy.Duration(1.0 / self._readiness_publish_hz), self._on_readiness_timer)

        self.nav = MBFMoveBase(
            action_name=mbf_move_base_action,
            planner=mbf_planner,
            controller=mbf_controller,
            recovery=mbf_recovery,
        )
        self._dock_stage2_nav = MBFMoveBase(
            action_name=mbf_move_base_action,
            planner=mbf_planner,
            controller=self.dock_stage2_controller,
            recovery=mbf_recovery,
        )
        rospy.loginfo(
            "[TASK] dock sequence config: two_stage=%s stage1=[%.3f,%.3f,%.3f] stage2=[%.3f,%.3f,%.3f] stage2_controller=%s",
            str(self.dock_two_stage_enable),
            self.dock_stage1_xyyaw[0],
            self.dock_stage1_xyyaw[1],
            self.dock_stage1_xyyaw[2],
            self.dock_xyyaw[0],
            self.dock_xyyaw[1],
            self.dock_xyyaw[2],
            self.dock_stage2_controller or "<default>",
        )

        # --- health monitor ---
        self.health_enable = bool(health_enable)
        self._base_frame = str(base_frame or "base_footprint")
        self._mbf_exe_path_action = str(mbf_exe_path_action or "/move_base_flex/exe_path")
        self._auto_pause_on_health_error = bool(auto_pause_on_health_error)
        self._auto_pause_cmd = str(auto_pause_cmd or "pause").strip() or "pause"
        self._health_auto_recover_enable = bool(health_auto_recover_enable)
        self._health_auto_recover_max = max(0, int(health_auto_recover_max))
        self._health_auto_resume_delay_s = max(0.0, float(health_auto_resume_delay_s))
        self._health_auto_recover_codes = set(str(x).strip().upper() for x in (health_auto_recover_codes or []))
        self._health_recover_pending: bool = False
        self._health_recover_resume_after_ts: float = 0.0
        self._health_recover_run_id: str = ""
        self._health_recover_code: str = ""
        self._health_recover_count: int = 0

        self._health_monitor: Optional[HealthMonitor] = None
        self._health_last: HealthResult = HealthResult()
        self._health_prev_level: str = "OK"
        self._health_prev_code: str = ""

        self._diag_updater: Optional[Updater] = None
        self._health_timer: Optional[rospy.Timer] = None

        if self.health_enable:
            try:
                self._health_monitor = HealthMonitor(
                    frame_id=str(self.frame_id),
                    base_frame=str(self._base_frame),
                    mbf_move_base_action=str(mbf_move_base_action),
                    mbf_exe_path_action=str(self._mbf_exe_path_action),
                    exec_progress_timeout_s=float(exec_progress_timeout_s),
                    tf_timeout_s=float(tf_timeout_s),
                    tf_max_age_s=float(tf_max_age_s),
                    progress_stall_timeout_s=float(progress_stall_timeout_s),
                    progress_stall_min_delta=float(progress_stall_min_delta),
                    loc_enable=bool(loc_enable),
                    loc_require=bool(loc_require),
                    loc_topic=str(loc_topic),
                    loc_stale_s=float(loc_stale_s),
                    loc_cov_xy_max=float(loc_cov_xy_max),
                    loc_cov_yaw_max=float(loc_cov_yaw_max),
                    loc_jump_xy_m=float(loc_jump_xy_m),
                    loc_jump_yaw_rad=float(loc_jump_yaw_rad),
                    pose_stall_enable=bool(pose_stall_enable),
                    pose_stall_timeout_s=float(pose_stall_timeout_s),
                    pose_stall_min_move_m=float(pose_stall_min_move_m),
                    pose_stall_min_yaw_rad=float(pose_stall_min_yaw_rad),
                )
                self._diag_updater = Updater()
                self._diag_updater.setHardwareID("coverage_task_manager")
                self._diag_updater.add("coverage_task_manager/health", self._diag_cb_health)
                self._diag_updater.add("coverage_task_manager/battery", self._diag_cb_battery)
                hz = max(0.2, float(health_check_hz))
                self._health_timer = rospy.Timer(rospy.Duration(1.0 / hz), self._on_health_timer)
            except Exception as e:
                rospy.logwarn("[TASK] health monitor disabled: %s", str(e))
                self.health_enable = False

        rospy.on_shutdown(self.shutdown)

        # Restore persisted state BEFORE background thread starts.
        self._restore_state_if_any()
        self._publish_state(self._public_state_from_intent())

        t = threading.Thread(target=self._run, daemon=True)
        t.start()

        rospy.loginfo(
            "[TASK] ready. cmd=%s -> executor_cmd=%s app_exe_task=%s app_contract=%s battery=%s auto_charge=%s low=%.2f resume=%.2f rearm=%.2f dock=(%.2f,%.2f,%.2f)",
            rospy.resolve_name(cmd_topic),
            self.executor_cmd_topic,
            self._app_exe_task_service_name,
            self._app_exe_task_contract_param_ns or "-",
            self.battery_topic,
            str(self.auto_charge_enable),
            self.low_soc,
            self.resume_soc,
            self.rearm_soc,
            self.dock_xyyaw[0],
            self.dock_xyyaw[1],
            self.dock_xyyaw[2],
        )

        # Scheduler snapshot on ready
        if self.scheduler_enable and self._scheduler is not None:
            try:
                now_ts = time.time()
                if not self._reload_scheduler_from_db(force=True):
                    for job in self._scheduler.jobs:
                        self._scheduler.update_next_fire(job, now_ts, init=True)
                    self._log_scheduler_snapshot(now_ts, reason="ready", force=True, full=True)
            except Exception as e:
                rospy.logwarn("[TASK][SCHED] snapshot on ready failed: %s", str(e))

        self._persist_now()

    # ------------------- ROS callbacks -------------------
    def _on_executor_state(self, msg: String):
        s = str(msg.data or "")
        rid = ""
        need_mark_paused_recovery = False
        executor_state_changed = False

        with self._lock:
            prev = str(self._executor_state or "")
            self._executor_state = s
            executor_state_changed = (s != prev)

            if s == "PAUSED_RECOVERY" and prev != "PAUSED_RECOVERY":
                self._mission_state = "PAUSED"
                self._phase = "IDLE"
                rid = str(self._active_run_id or "")
                need_mark_paused_recovery = True

        if executor_state_changed:
            self._persist_if_changed()

        if need_mark_paused_recovery:
            self._mission_update_state(rid, "PAUSED", reason="exec_paused_recovery")
            self._emit("EXEC_PAUSED_RECOVERY")
            self._publish_state("PAUSED_RECOVERY")

    def _on_executor_progress(self, msg: RunProgressMsg):
        with self._lock:
            self._last_run_progress = msg
            self._last_run_progress_ts = time.time()

    def _on_battery(self, msg: BatteryState):
        soc = _soc_from_battery(msg)
        if soc is None:
            return
        with self._lock:
            self._battery.soc = soc
            self._battery.ts = time.time()

    def _on_combined_status(self, _msg: CombinedStatus):
        with self._lock:
            self._combined_status.ts = time.time()

    def _on_station_status(self, _msg: StationStatus):
        with self._lock:
            self._station_status.ts = time.time()

    def _on_mcore_connected(self, msg: Bool):
        with self._lock:
            self._mcore_connected.value = bool(msg.data)
            self._mcore_connected.ts = time.time()

    def _on_station_connected(self, msg: Bool):
        with self._lock:
            self._station_connected.value = bool(msg.data)
            self._station_connected.ts = time.time()

    def _on_odometry_state(self, msg: OdometryState):
        with self._lock:
            self._odometry_state.msg = msg
            self._odometry_state.ts = time.time()

    def _on_slam_state(self, msg: SlamState):
        with self._lock:
            self._slam_state.msg = msg
            self._slam_state.ts = time.time()

    def _get_fresh_slam_state(self, *, now: Optional[float] = None) -> Optional[SlamState]:
        ts = 0.0
        msg = None
        with self._lock:
            if hasattr(self, "_slam_state"):
                ts = float(getattr(self._slam_state, "ts", 0.0) or 0.0)
                msg = getattr(self._slam_state, "msg", None)
        if msg is None or ts <= 0.0:
            return None
        current = float(time.time() if now is None else now)
        if (current - ts) > float(self._slam_state_stale_timeout_s):
            return None
        return msg

    def _on_cmd(self, msg: String):
        cmd = (msg.data or "").strip()
        if not cmd:
            return
        self._emit(f"CMD:{cmd}")
        self._handle_cmd(cmd)

    def _node_online(self, node_name: str) -> bool:
        try:
            return str(node_name or "").strip() in rosnode.get_node_names()
        except Exception:
            return False

    def _runtime_map_snapshot(self, *, refresh: bool = False) -> Dict[str, object]:
        name = ""
        revision_id = ""
        map_id = ""
        map_md5 = ""
        ok = False
        try:
            name, _scope = get_runtime_map_scope()
        except Exception:
            name = ""
        try:
            revision_id = get_runtime_map_revision_id("/cartographer/runtime")
        except Exception:
            revision_id = ""
        try:
            map_id, map_md5, ok = ensure_map_identity(
                map_topic=str(self._runtime_map_topic),
                timeout_s=1.0,
                set_global_params=True,
                set_private_params=False,
                refresh=bool(refresh),
            )
        except Exception:
            map_id, map_md5, ok = "", "", False
        if (not name) and revision_id and self._plan_store is not None:
            try:
                revision = self._plan_store.resolve_map_revision(revision_id=revision_id) or {}
            except Exception:
                revision = {}
            if revision:
                name = str(revision.get("map_name") or "").strip()
        return {
            "revision_id": str(revision_id or "").strip(),
            "map_name": str(name or "").strip(),
            "map_id": str(map_id or "").strip(),
            "map_md5": str(map_md5 or "").strip(),
            "ok": bool(ok),
        }

    def _make_readiness_check(
        self,
        *,
        key: str,
        level: str,
        ok: bool,
        fresh: bool = True,
        stale: bool = False,
        missing: bool = False,
        age_s: float = -1.0,
        summary: str = "",
    ) -> SubsystemReadiness:
        m = SubsystemReadiness()
        m.key = str(key or "")
        m.level = str(level or "OK").upper()
        m.ok = bool(ok)
        m.fresh = bool(fresh)
        m.stale = bool(stale)
        m.missing = bool(missing)
        m.age_s = float(age_s if age_s >= 0.0 else -1.0)
        m.summary = str(summary or "")
        return m

    @staticmethod
    def _append_unique_readiness_text(items: List[str], text: str):
        normalized = str(text or "").strip()
        if normalized and normalized not in items:
            items.append(normalized)

    def _merge_readiness_gate_result(
        self,
        *,
        blockers: List[str],
        warnings: List[str],
        checks: List[SubsystemReadiness],
        result: ReadinessGateResult,
    ):
        for item in list(getattr(result, "blockers", []) or []):
            self._append_unique_readiness_text(blockers, item)
        for item in list(getattr(result, "warnings", []) or []):
            self._append_unique_readiness_text(warnings, item)
        checks.extend(list(getattr(result, "checks", []) or []))

    def _build_slam_runtime_gate(
        self,
        *,
        now: float,
        refresh_map_identity: bool,
        odometry_state: Optional[OdometryState],
        odometry_state_ts: float,
        slam_state: Optional[SlamState],
        slam_state_ts: float,
    ) -> SlamRuntimeGateResult:
        result = SlamRuntimeGateResult()
        local_slam_blockers: List[str] = []

        slam_age = (now - slam_state_ts) if slam_state_ts > 0.0 else -1.0
        slam_fresh = slam_state_ts > 0.0 and slam_age <= self._slam_state_stale_timeout_s
        slam_task_ready = bool(getattr(slam_state, "task_ready", False)) if slam_state is not None else False
        slam_runtime_authoritative = bool(slam_state is not None and slam_fresh)
        slam_blocking_reason = ""
        if slam_state is not None:
            slam_blocking_reason = str(getattr(slam_state, "blocking_reason", "") or "").strip()
            if not slam_blocking_reason:
                for item in list(getattr(slam_state, "blocking_reasons", []) or []):
                    slam_blocking_reason = str(item or "").strip()
                    if slam_blocking_reason:
                        break

        selected_asset = self._get_selected_active_map() or {}
        result.active_revision_id = str(selected_asset.get("revision_id") or "").strip()
        result.active_map_name = str(selected_asset.get("map_name") or "").strip()
        result.active_map_id = str(selected_asset.get("map_id") or "").strip()
        result.active_map_md5 = str(selected_asset.get("map_md5") or "").strip()

        runtime_map = self._runtime_map_snapshot(refresh=bool(refresh_map_identity))
        result.runtime_revision_id = str(runtime_map.get("revision_id") or "")
        result.runtime_map_name = str(runtime_map.get("map_name") or "")
        result.runtime_map_id = str(runtime_map.get("map_id") or "")
        result.runtime_map_md5 = str(runtime_map.get("map_md5") or "")

        if not result.active_map_name:
            self._append_unique_readiness_text(local_slam_blockers, no_current_active_map_selected_message())
            result.checks.append(self._make_readiness_check(
                key="active_map",
                level="WARN" if slam_runtime_authoritative else "ERROR",
                ok=False,
                fresh=False,
                missing=True,
                summary=no_current_active_map_selected_message(),
            ))
        else:
            result.checks.append(self._make_readiness_check(
                key="active_map",
                level="OK",
                ok=True,
                summary="selected map=%s" % result.active_map_name,
            ))

        runtime_missing = not (
            result.runtime_revision_id
            or result.runtime_map_name
            or result.runtime_map_id
            or result.runtime_map_md5
        )
        if runtime_missing:
            self._append_unique_readiness_text(local_slam_blockers, runtime_map_identity_unavailable_message())
            result.checks.append(self._make_readiness_check(
                key="runtime_map",
                level="WARN" if slam_runtime_authoritative else "ERROR",
                ok=False,
                fresh=False,
                missing=True,
                summary=runtime_map_identity_unavailable_message(),
            ))
        else:
            mismatch_reason = runtime_map_mismatch_reason(
                active_revision_id=result.active_revision_id,
                active_map_name=result.active_map_name,
                active_map_id=result.active_map_id,
                active_map_md5=result.active_map_md5,
                runtime_revision_id=result.runtime_revision_id,
                runtime_map_name=result.runtime_map_name,
                runtime_map_id=result.runtime_map_id,
                runtime_map_md5=result.runtime_map_md5,
            )
            if mismatch_reason:
                self._append_unique_readiness_text(local_slam_blockers, mismatch_reason)
                result.checks.append(self._make_readiness_check(
                    key="runtime_map",
                    level="WARN" if slam_runtime_authoritative else "ERROR",
                    ok=False,
                    fresh=True,
                    summary=mismatch_reason,
                ))
            else:
                result.checks.append(self._make_readiness_check(
                    key="runtime_map",
                    level="OK",
                    ok=True,
                    summary="runtime map=%s" % (result.runtime_map_name or result.runtime_map_id or result.runtime_map_md5),
                ))

        odometry_age = (now - odometry_state_ts) if odometry_state_ts > 0.0 else -1.0
        odometry_fresh = odometry_state_ts > 0.0 and odometry_age <= self._odometry_state_stale_timeout_s
        odometry_valid = bool(
            odometry_fresh
            and odometry_state is not None
            and bool(getattr(odometry_state, "odom_valid", False))
        )
        odometry_error_code = (
            str(getattr(odometry_state, "error_code", "") or "").strip()
            if odometry_state is not None
            else ""
        )
        odometry_message = (
            str(getattr(odometry_state, "message", "") or "").strip()
            if odometry_state is not None
            else ""
        )
        if odometry_state is None or odometry_state_ts <= 0.0:
            odometry_summary = "odometry state unavailable"
        elif not odometry_fresh:
            odometry_summary = "state stale age=%.1fs" % odometry_age
        else:
            odometry_summary = "mode=%s stream=%s valid=%s code=%s msg=%s" % (
                str(getattr(odometry_state, "validation_mode", "") or "-"),
                str(bool(getattr(odometry_state, "odom_stream_ready", False))).lower(),
                str(bool(odometry_valid)).lower(),
                odometry_error_code or "-",
                odometry_message or "ok",
            )
            odometry_source = str(getattr(odometry_state, "odom_source", "") or "").strip()
            if odometry_source and odometry_source != "odom_stream":
                odometry_summary += " source=%s" % odometry_source
        if self._require_odometry_healthy_before_start and (not slam_runtime_authoritative):
            if odometry_state is None or odometry_state_ts <= 0.0:
                self._append_unique_readiness_text(local_slam_blockers, "odometry health state unavailable")
            elif not odometry_fresh:
                self._append_unique_readiness_text(local_slam_blockers, "odometry health stale age=%.1fs" % odometry_age)
            elif not odometry_valid:
                self._append_unique_readiness_text(
                    local_slam_blockers,
                    odometry_not_ready_message(odometry_error_code, odometry_message),
                )
        else:
            if odometry_state is None or odometry_state_ts <= 0.0:
                result.warnings.append("odometry health state unavailable")
            elif not odometry_fresh:
                result.warnings.append("odometry health stale")
            elif not odometry_valid:
                result.warnings.append(odometry_not_ready_message(odometry_error_code, odometry_message))
        if odometry_state is not None and odometry_fresh and not odometry_valid:
            for item in list(getattr(odometry_state, "warnings", []) or []):
                text = str(item or "").strip()
                if text:
                    result.warnings.append("odometry: %s" % text)
        result.checks.append(self._make_readiness_check(
            key="odometry",
            level=(
                "OK"
                if odometry_valid
                else ("WARN" if slam_runtime_authoritative else ("ERROR" if self._require_odometry_healthy_before_start else "WARN"))
            ),
            ok=bool(odometry_valid),
            fresh=bool(odometry_fresh),
            stale=bool(odometry_state_ts > 0.0 and not odometry_fresh),
            missing=bool(odometry_state is None or odometry_state_ts <= 0.0),
            age_s=float(odometry_age) if odometry_state_ts > 0.0 else -1.0,
            summary=odometry_summary,
        ))

        localization_state = str(rospy.get_param(self._runtime_localization_state_param, "") or "").strip().lower()
        localization_valid = bool(rospy.get_param(self._runtime_localization_valid_param, False))
        if slam_runtime_authoritative:
            localization_state = str(getattr(slam_state, "localization_state", "") or localization_state).strip().lower()
            localization_valid = bool(getattr(slam_state, "localization_valid", localization_valid))
        manual_assist_required = bool(
            (
                bool(getattr(slam_state, "manual_assist_required", False))
                if slam_runtime_authoritative and slam_state is not None
                else False
            )
            or localization_needs_manual_assist(localization_state)
        )
        manual_assist_info = manual_assist_metadata(
            required=manual_assist_required,
            map_name=(
                str(getattr(slam_state, "manual_assist_map_name", "") or "").strip()
                if slam_runtime_authoritative and slam_state is not None
                else ""
            ) or result.active_map_name,
            map_revision_id=(
                str(getattr(slam_state, "manual_assist_map_revision_id", "") or "").strip()
                if slam_runtime_authoritative and slam_state is not None
                else ""
            ) or result.active_revision_id,
            retry_action=(
                str(getattr(slam_state, "manual_assist_retry_action", "") or "").strip()
                if slam_runtime_authoritative and slam_state is not None
                else ""
            ),
            default_action="prepare_for_task",
        )
        result.manual_assist_required = bool(manual_assist_required)
        result.manual_assist_map_name = str(manual_assist_info.get("map_name") or "")
        result.manual_assist_map_revision_id = str(manual_assist_info.get("map_revision_id") or "")
        result.manual_assist_retry_action = str(manual_assist_info.get("retry_action") or "")
        result.manual_assist_guidance = str(manual_assist_info.get("guidance") or "")
        localization_ok = localization_is_ready(localization_state, localization_valid)
        if not localization_ok:
            self._append_unique_readiness_text(
                local_slam_blockers,
                runtime_localization_not_ready_message(
                    localization_state,
                    localization_valid,
                    map_name=result.manual_assist_map_name,
                    map_revision_id=result.manual_assist_map_revision_id,
                    retry_action=result.manual_assist_retry_action or "prepare_for_task",
                ),
            )
        result.checks.append(self._make_readiness_check(
            key="localization",
            level="OK" if localization_ok else ("WARN" if slam_runtime_authoritative else "ERROR"),
            ok=bool(localization_ok),
            fresh=bool(localization_valid),
            missing=bool(not localization_state),
            summary="state=%s valid=%s" % (localization_state or "-", str(bool(localization_valid)).lower()),
        ))

        if slam_state is None or slam_state_ts <= 0.0:
            result.warnings.append("slam state unavailable")
            for item in local_slam_blockers:
                self._append_unique_readiness_text(result.blockers, item)
            result.checks.append(self._make_readiness_check(
                key="slam_runtime",
                level="WARN",
                ok=False,
                fresh=False,
                missing=True,
                summary="slam state unavailable",
            ))
        elif not slam_fresh:
            result.warnings.append("slam state stale")
            for item in local_slam_blockers:
                self._append_unique_readiness_text(result.blockers, item)
            result.checks.append(self._make_readiness_check(
                key="slam_runtime",
                level="WARN",
                ok=False,
                fresh=False,
                stale=True,
                age_s=float(slam_age),
                summary="slam state stale age=%.1fs" % slam_age,
            ))
        else:
            if not slam_task_ready:
                self._append_unique_readiness_text(
                    result.blockers,
                    slam_blocking_reason or (local_slam_blockers[0] if local_slam_blockers else "") or "slam runtime not ready",
                )
            result.checks.append(self._make_readiness_check(
                key="slam_runtime",
                level="OK" if slam_task_ready else "ERROR",
                ok=bool(slam_task_ready),
                fresh=True,
                summary="workflow=%s phase=%s task_ready=%s busy=%s manual_assist=%s" % (
                    str(getattr(slam_state, "workflow_state", "") or "-"),
                    str(getattr(slam_state, "workflow_phase", "") or "-"),
                    str(bool(slam_task_ready)).lower(),
                    str(bool(getattr(slam_state, "busy", False))).lower(),
                    str(bool(getattr(slam_state, "manual_assist_required", False))).lower(),
                ),
            ))

        return result

    def _build_platform_gate(
        self,
        *,
        now: float,
        mission_state: str,
        phase: str,
        public_state: str,
        executor_state: str,
        dock_supply_state: str,
        dock_supply_ts: float,
        batt_soc: Optional[float],
        batt_ts: float,
        battery_valid: bool,
        combined_ts: float,
        station_ts: float,
        mcore_connected,
        mcore_connected_ts: float,
        station_connected,
        station_connected_ts: float,
        health_fault_active: bool,
        health_error_code: str,
        health_error_msg: str,
    ) -> ReadinessGateResult:
        result = ReadinessGateResult()

        mbf_ok = self._node_online("/move_base_flex")
        if not mbf_ok:
            result.blockers.append("move_base_flex is offline")
        result.checks.append(self._make_readiness_check(
            key="move_base_flex",
            level="OK" if mbf_ok else "ERROR",
            ok=mbf_ok,
            fresh=mbf_ok,
            missing=not mbf_ok,
            summary="online" if mbf_ok else "move_base_flex is offline",
        ))

        mcore_node_ok = self._node_online("/mcore_tcp_bridge")
        mcore_fresh = mcore_connected_ts > 0.0 and (now - mcore_connected_ts) <= self._connected_stale_timeout_s
        if (not mcore_node_ok) or (mcore_connected is not True) or (not mcore_fresh):
            result.blockers.append("mcore bridge not ready")
        result.checks.append(self._make_readiness_check(
            key="mcore_bridge",
            level="OK" if (mcore_node_ok and mcore_connected is True and mcore_fresh) else "ERROR",
            ok=bool(mcore_node_ok and mcore_connected is True and mcore_fresh),
            fresh=bool(mcore_fresh),
            stale=bool(mcore_connected_ts > 0.0 and not mcore_fresh),
            missing=bool(not mcore_node_ok or mcore_connected_ts <= 0.0),
            age_s=float(max(0.0, now - mcore_connected_ts)) if mcore_connected_ts > 0.0 else -1.0,
            summary=(
                "connected"
                if (mcore_node_ok and mcore_connected is True and mcore_fresh)
                else (
                    "node offline"
                    if not mcore_node_ok
                    else ("disconnected" if mcore_connected is False else "status stale/missing")
                )
            ),
        ))

        if mission_state != "IDLE":
            result.blockers.append("task manager busy: mission=%s phase=%s public=%s" % (mission_state, phase, public_state))
        result.checks.append(self._make_readiness_check(
            key="task_manager",
            level="OK" if mission_state == "IDLE" else "ERROR",
            ok=bool(mission_state == "IDLE"),
            summary="mission=%s phase=%s public=%s" % (mission_state, phase, public_state),
        ))

        exec_idle = str(executor_state or "").strip().upper() in ("", "IDLE")
        if not exec_idle:
            result.blockers.append("executor not idle: %s" % (executor_state or "UNKNOWN"))
        result.checks.append(self._make_readiness_check(
            key="executor",
            level="OK" if exec_idle else "ERROR",
            ok=bool(exec_idle),
            summary="executor_state=%s" % (executor_state or "IDLE"),
        ))

        if health_fault_active:
            result.blockers.append("health fault active: %s %s" % (health_error_code or "ERROR", health_error_msg or ""))
        elif health_error_code:
            result.warnings.append("health warning latched: %s %s" % (health_error_code, health_error_msg or ""))
        result.checks.append(self._make_readiness_check(
            key="health",
            level="ERROR" if health_fault_active else ("WARN" if health_error_code else "OK"),
            ok=bool(not health_fault_active),
            summary=(
                "health fault: %s %s" % (health_error_code or "ERROR", health_error_msg or "")
                if health_fault_active
                else ("latched: %s %s" % (health_error_code, health_error_msg or "") if health_error_code else "OK")
            ),
        ))

        batt_age = (now - batt_ts) if batt_ts > 0.0 else -1.0
        if batt_ts <= 0.0:
            result.warnings.append("battery_state missing")
            batt_level = "WARN"
            batt_summary = "battery_state missing"
        elif not battery_valid:
            result.warnings.append("battery_state stale")
            batt_level = "WARN"
            batt_summary = "battery_state stale age=%.1fs" % batt_age
        else:
            batt_level = "OK"
            batt_summary = "soc=%.3f" % float(batt_soc if batt_soc is not None else 0.0)
        result.checks.append(self._make_readiness_check(
            key="battery",
            level=batt_level,
            ok=bool(battery_valid),
            fresh=bool(battery_valid),
            stale=bool(batt_ts > 0.0 and not battery_valid),
            missing=bool(batt_ts <= 0.0),
            age_s=float(batt_age) if batt_ts > 0.0 else -1.0,
            summary=batt_summary,
        ))

        combined_age = (now - combined_ts) if combined_ts > 0.0 else -1.0
        combined_fresh = combined_ts > 0.0 and combined_age <= self._combined_status_stale_timeout_s
        if combined_ts <= 0.0:
            result.warnings.append("combined_status missing")
        elif not combined_fresh:
            result.warnings.append("combined_status stale")
        result.checks.append(self._make_readiness_check(
            key="combined_status",
            level="OK" if combined_fresh else "WARN",
            ok=bool(combined_fresh),
            fresh=bool(combined_fresh),
            stale=bool(combined_ts > 0.0 and not combined_fresh),
            missing=bool(combined_ts <= 0.0),
            age_s=float(combined_age) if combined_ts > 0.0 else -1.0,
            summary=("fresh" if combined_fresh else ("missing" if combined_ts <= 0.0 else "stale age=%.1fs" % combined_age)),
        ))

        if self._dock_supply_enable:
            dock_busy = dock_supply_state not in ("", "IDLE", "DONE", "FAILED", "CANCELED") and (not dock_supply_state.startswith("FAILED"))
            if dock_busy:
                result.blockers.append("dock supply busy: %s" % dock_supply_state)
            result.checks.append(self._make_readiness_check(
                key="dock_supply",
                level="ERROR" if dock_busy else "OK",
                ok=bool(not dock_busy),
                fresh=bool(dock_supply_ts > 0.0),
                missing=bool(dock_supply_ts <= 0.0),
                age_s=float(max(0.0, now - dock_supply_ts)) if dock_supply_ts > 0.0 else -1.0,
                summary="state=%s" % (dock_supply_state or "IDLE"),
            ))

        station_node_ok = self._node_online("/station_tcp_bridge")
        station_conn_fresh = station_connected_ts > 0.0 and (now - station_connected_ts) <= self._connected_stale_timeout_s
        station_age = (now - station_ts) if station_ts > 0.0 else -1.0
        station_status_fresh = station_ts > 0.0 and station_age <= self._station_status_stale_timeout_s
        if not station_node_ok:
            result.warnings.append("station bridge offline")
        elif station_connected is False and station_conn_fresh:
            result.warnings.append("station bridge disconnected")
        elif not station_status_fresh:
            result.warnings.append("station_status stale or missing")
        result.checks.append(self._make_readiness_check(
            key="station_status",
            level="OK" if (station_node_ok and station_connected is True and station_status_fresh) else "WARN",
            ok=bool(station_node_ok and station_connected is True and station_status_fresh),
            fresh=bool(station_status_fresh),
            stale=bool(station_ts > 0.0 and not station_status_fresh),
            missing=bool(station_ts <= 0.0),
            age_s=float(station_age) if station_ts > 0.0 else -1.0,
            summary=(
                "connected/fresh"
                if (station_node_ok and station_connected is True and station_status_fresh)
                else (
                    "station bridge offline"
                    if not station_node_ok
                    else ("disconnected" if station_connected is False and station_conn_fresh else "stale/missing")
                )
            ),
        ))
        return result

    def _build_task_config_gate(
        self,
        *,
        task_id: int,
        active_revision_id: str,
        active_map_name: str,
        readiness: SystemReadinessMsg,
    ) -> ReadinessGateResult:
        result = ReadinessGateResult()
        if int(task_id or 0) <= 0:
            return result

        job = self._resolve_job_record(str(int(task_id)))
        if job is None:
            result.blockers.append("task not found: %s" % int(task_id))
            result.checks.append(self._make_readiness_check(
                key="task_config",
                level="ERROR",
                ok=False,
                missing=True,
                summary="task not found: %s" % int(task_id),
            ))
            return result

        readiness.task_name = str(getattr(job, "job_name", "") or "")
        readiness.task_map_name = str(job.map_name or "")
        readiness.task_map_revision_id = str(getattr(job, "map_revision_id", "") or "")
        readiness.task_zone_id = str(job.zone_id or "")
        readiness.task_plan_profile = str(job.plan_profile_name or "")
        task_map_revision_id = str(readiness.task_map_revision_id or "")
        task_msgs = []
        if not bool(job.enabled):
            task_msgs.append("task is disabled")
        if task_map_revision_id and not active_revision_id:
            task_msgs.append(
                selected_map_does_not_match_requested_map_message(
                    active_map_name or readiness.task_map_name,
                    readiness.task_map_name,
                    selected_revision_id=active_revision_id,
                    requested_revision_id=task_map_revision_id,
                )
            )
        elif active_revision_id and task_map_revision_id and task_map_revision_id != active_revision_id:
            task_msgs.append(
                selected_map_does_not_match_requested_map_message(
                    active_map_name or readiness.task_map_name,
                    readiness.task_map_name,
                    selected_revision_id=active_revision_id,
                    requested_revision_id=task_map_revision_id,
                )
            )
        elif active_map_name and readiness.task_map_name and readiness.task_map_name != active_map_name:
            task_msgs.append(
                selected_map_does_not_match_requested_map_message(
                    active_map_name,
                    readiness.task_map_name,
                )
            )
        ok, msg = self._ensure_zone_plan_ready_for_job(
            map_name=readiness.task_map_name,
            map_revision_id=task_map_revision_id,
            zone_id=readiness.task_zone_id or self.zone_id_default,
            plan_profile_name=readiness.task_plan_profile,
        )
        if not ok:
            task_msgs.append(str(msg or "task zone/plan not ready"))
        if task_msgs:
            result.blockers.extend(task_msgs)
            result.checks.append(self._make_readiness_check(
                key="task_config",
                level="ERROR",
                ok=False,
                summary="; ".join(task_msgs),
            ))
        else:
            task_name = str(getattr(job, "job_name", "") or "")
            readiness.task_name = task_name
            result.checks.append(self._make_readiness_check(
                key="task_config",
                level="OK",
                ok=True,
                summary="task=%s zone=%s plan_profile=%s" % (
                    task_name or str(int(task_id)),
                    readiness.task_zone_id or "-",
                    readiness.task_plan_profile or "-",
                ),
            ))
        return result

    def _build_system_readiness(self, *, task_id: int = 0, refresh_map_identity: bool = False) -> SystemReadinessMsg:
        now = time.time()
        with self._lock:
            mission_state = str(self._mission_state or "IDLE")
            phase = str(self._phase or "IDLE")
            public_state = str(self._public_state or "IDLE")
            executor_state = str(self._executor_state or "")
            dock_supply_state = str(self._dock_supply_state or "IDLE")
            dock_supply_ts = float(getattr(self, "_dock_supply_state_ts", 0.0) or 0.0)
            batt_soc = self._battery.soc
            batt_ts = float(self._battery.ts or 0.0)
            combined_ts = float(self._combined_status.ts or 0.0)
            station_ts = float(self._station_status.ts or 0.0)
            mcore_connected = self._mcore_connected.value
            mcore_connected_ts = float(self._mcore_connected.ts or 0.0)
            station_connected = self._station_connected.value
            station_connected_ts = float(self._station_connected.ts or 0.0)
            odometry_state = self._odometry_state.msg
            odometry_state_ts = float(self._odometry_state.ts or 0.0)
            slam_state = self._slam_state.msg
            slam_state_ts = float(self._slam_state.ts or 0.0)
            health_fault_active = bool(self._health_fault_active)
            health_error_code = str(self._health_error_code or "")
            health_error_msg = str(self._health_error_msg or "")

        readiness = SystemReadinessMsg()
        readiness.task_id = int(task_id or 0)
        readiness.mission_state = mission_state
        readiness.phase = phase
        readiness.public_state = public_state
        readiness.executor_state = executor_state
        readiness.dock_supply_state = dock_supply_state
        readiness.battery_soc = float(batt_soc if batt_soc is not None else 0.0)
        readiness.battery_valid = bool(batt_ts > 0.0 and (now - batt_ts) <= float(self.battery_stale_timeout_s))
        readiness.stamp = rospy.Time.now()

        blockers: List[str] = []
        warnings: List[str] = []
        checks: List[SubsystemReadiness] = []
        slam_gate = self._build_slam_runtime_gate(
            now=now,
            refresh_map_identity=bool(refresh_map_identity),
            odometry_state=odometry_state,
            odometry_state_ts=odometry_state_ts,
            slam_state=slam_state,
            slam_state_ts=slam_state_ts,
        )
        readiness.active_map_name = slam_gate.active_map_name
        readiness.active_map_revision_id = slam_gate.active_revision_id
        readiness.active_map_id = slam_gate.active_map_id
        readiness.active_map_md5 = slam_gate.active_map_md5
        readiness.runtime_map_name = slam_gate.runtime_map_name
        readiness.runtime_map_revision_id = slam_gate.runtime_revision_id
        readiness.runtime_map_id = slam_gate.runtime_map_id
        readiness.runtime_map_md5 = slam_gate.runtime_map_md5
        readiness.manual_assist_required = bool(slam_gate.manual_assist_required)
        readiness.manual_assist_map_name = slam_gate.manual_assist_map_name
        readiness.manual_assist_map_revision_id = slam_gate.manual_assist_map_revision_id
        readiness.manual_assist_retry_action = slam_gate.manual_assist_retry_action
        readiness.manual_assist_guidance = slam_gate.manual_assist_guidance
        self._merge_readiness_gate_result(
            blockers=blockers,
            warnings=warnings,
            checks=checks,
            result=slam_gate,
        )

        platform_gate = self._build_platform_gate(
            now=now,
            mission_state=mission_state,
            phase=phase,
            public_state=public_state,
            executor_state=executor_state,
            dock_supply_state=dock_supply_state,
            dock_supply_ts=dock_supply_ts,
            batt_soc=batt_soc,
            batt_ts=batt_ts,
            battery_valid=readiness.battery_valid,
            combined_ts=combined_ts,
            station_ts=station_ts,
            mcore_connected=mcore_connected,
            mcore_connected_ts=mcore_connected_ts,
            station_connected=station_connected,
            station_connected_ts=station_connected_ts,
            health_fault_active=health_fault_active,
            health_error_code=health_error_code,
            health_error_msg=health_error_msg,
        )
        self._merge_readiness_gate_result(
            blockers=blockers,
            warnings=warnings,
            checks=checks,
            result=platform_gate,
        )

        task_gate = self._build_task_config_gate(
            task_id=int(task_id or 0),
            active_revision_id=slam_gate.active_revision_id,
            active_map_name=slam_gate.active_map_name,
            readiness=readiness,
        )
        self._merge_readiness_gate_result(
            blockers=blockers,
            warnings=warnings,
            checks=checks,
            result=task_gate,
        )

        readiness.blocking_reasons = [str(x) for x in blockers]
        readiness.warnings = [str(x) for x in warnings]
        readiness.checks = checks
        readiness.overall_ready = bool(len(blockers) == 0)
        readiness.can_start_task = bool(len(blockers) == 0)
        return readiness

    def _on_get_system_readiness_app(self, req: AppGetSystemReadiness):
        try:
            readiness = self._build_system_readiness(
                task_id=int(getattr(req, "task_id", 0) or 0),
                refresh_map_identity=bool(getattr(req, "refresh_map_identity", False)),
            )
            return AppGetSystemReadinessResponse(
                success=True,
                message="ready" if readiness.can_start_task else (
                    readiness.blocking_reasons[0] if readiness.blocking_reasons else "not ready"
                ),
                readiness=readiness,
            )
        except Exception as e:
            return AppGetSystemReadinessResponse(
                success=False,
                message=str(e),
                readiness=SystemReadinessMsg(),
            )

    def _on_readiness_timer(self, _evt):
        try:
            self._readiness_pub.publish(self._build_system_readiness(task_id=0, refresh_map_identity=False))
        except Exception:
            pass

    def _prepare_readiness_contract_report(self):
        validate_ros_contract(
            "SystemReadiness",
            SystemReadinessMsg,
            required_fields=[
                "overall_ready",
                "can_start_task",
                "task_id",
                "task_name",
                "task_map_name",
                "task_map_revision_id",
                "task_zone_id",
                "task_plan_profile",
                "active_map_name",
                "active_map_revision_id",
                "active_map_id",
                "active_map_md5",
                "runtime_map_name",
                "runtime_map_revision_id",
                "runtime_map_id",
                "runtime_map_md5",
                "mission_state",
                "phase",
                "public_state",
                "executor_state",
                "dock_supply_state",
                "battery_soc",
                "battery_valid",
                "blocking_reasons",
                "warnings",
                "checks",
                "stamp",
            ],
        )
        validate_ros_contract(
            "SubsystemReadiness",
            SubsystemReadiness,
            required_fields=["key", "level", "ok", "fresh", "stale", "missing", "age_s", "summary"],
        )
        validate_ros_contract(
            "AppGetSystemReadinessRequest",
            AppGetSystemReadiness._request_class,
            required_fields=["task_id", "refresh_map_identity"],
        )
        validate_ros_contract(
            "AppGetSystemReadinessResponse",
            AppGetSystemReadinessResponse,
            required_fields=["success", "message", "readiness"],
        )
        return build_contract_report(
            service_name=self._app_readiness_service_name,
            contract_name="get_system_readiness_app",
            service_cls=AppGetSystemReadiness,
            request_cls=AppGetSystemReadiness._request_class,
            response_cls=AppGetSystemReadinessResponse,
            dependencies={"readiness": SystemReadinessMsg},
            features=["task_start_gate", "readiness_aggregation", "cleanrobot_app_msgs_parallel"],
        )

    def _prepare_exe_task_contract_report(self):
        validate_ros_contract(
            "AppExeTaskRequest",
            AppExeTaskRequest,
            required_fields=["command", "task_id"],
            required_constants=["START", "PAUSE", "CONTINUE", "STOP", "RETURN"],
        )
        return build_contract_report(
            service_name=self._app_exe_task_service_name,
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
        )

    def _exe_task_resp(self, success: bool, message: str, *, response_cls=AppExeTaskResponse):
        return response_cls(success=bool(success), message=str(message or ""))

    def _exe_task_runtime_summary(self) -> str:
        return "mission=%s phase=%s public=%s run=%s job=%s" % (
            str(self._mission_state or "IDLE"),
            str(self._phase or "IDLE"),
            str(self._public_state or "IDLE"),
            str(self._active_run_id or "-"),
            str(self._active_job_id or "-"),
        )

    def _ensure_exe_task_allowed(self, command: int) -> Tuple[bool, str]:
        mission = str(self._mission_state or "IDLE").upper()
        phase = str(self._phase or "IDLE").upper()
        active_run = str(self._active_run_id or "").strip()
        summary = self._exe_task_runtime_summary()

        if command == int(AppExeTaskRequest.START):
            if (mission not in ("IDLE", "ESTOP")) or (phase != "IDLE") or active_run:
                return False, "task manager busy: %s" % summary
            return True, ""
        if command == int(AppExeTaskRequest.PAUSE):
            if mission != "RUNNING":
                return False, "pause requires running mission: %s" % summary
            return True, ""
        if command == int(AppExeTaskRequest.CONTINUE):
            if mission != "PAUSED":
                return False, "continue requires paused mission: %s" % summary
            return True, ""
        if command == int(AppExeTaskRequest.STOP):
            if mission not in ("RUNNING", "PAUSED"):
                return False, "stop requires running or paused mission: %s" % summary
            return True, ""
        if command == int(AppExeTaskRequest.RETURN):
            if phase != "IDLE":
                return False, "return requires idle phase: %s" % summary
            if mission not in ("IDLE", "PAUSED"):
                return False, "return requires paused or idle mission: %s" % summary
            return True, ""
        return False, "unsupported command=%s" % int(command)

    def _on_exe_task_common(self, req, *, response_cls):
        command = int(getattr(req, "command", -1))
        allowed, message = self._ensure_exe_task_allowed(command)
        if not allowed:
            return self._exe_task_resp(False, message, response_cls=response_cls)

        if command == int(AppExeTaskRequest.START):
            task_id = int(getattr(req, "task_id", 0) or 0)
            if task_id <= 0:
                return self._exe_task_resp(False, "task_id is required for START", response_cls=response_cls)
            readiness = self._build_system_readiness(task_id=task_id, refresh_map_identity=True)
            if not bool(readiness.can_start_task):
                blockers = list(getattr(readiness, "blocking_reasons", []) or [])
                detail = str(blockers[0] if blockers else "system not ready").strip()
                return self._exe_task_resp(False, "system not ready: %s" % detail, response_cls=response_cls)
            ok, msg = self._start_task_by_id(str(task_id), source="SERVICE")
            if not ok:
                return self._exe_task_resp(False, msg, response_cls=response_cls)
            return self._exe_task_resp(True, "accepted: start_task %d" % task_id, response_cls=response_cls)

        if command == int(AppExeTaskRequest.PAUSE):
            ok, msg = self._pause_current_task()
            return self._exe_task_resp(ok, ("accepted: pause" if ok else msg), response_cls=response_cls)

        if command == int(AppExeTaskRequest.CONTINUE):
            ok, msg = self._resume_current_task()
            return self._exe_task_resp(ok, ("accepted: resume" if ok else msg), response_cls=response_cls)

        if command == int(AppExeTaskRequest.STOP):
            ok, msg = self._stop_current_task()
            return self._exe_task_resp(ok, ("accepted: stop" if ok else msg), response_cls=response_cls)

        if command == int(AppExeTaskRequest.RETURN):
            ok, msg = self._start_manual_return()
            return self._exe_task_resp(ok, ("accepted: dock" if ok else msg), response_cls=response_cls)

        return self._exe_task_resp(False, "unsupported command=%s" % command, response_cls=response_cls)

    def _on_exe_task_app(self, req: AppExeTask):
        return self._on_exe_task_common(req, response_cls=AppExeTaskResponse)

    # ------------------- public -------------------
    def spin(self):
        self.nav.wait_for_server()
        rospy.spin()

    def shutdown(self):
        with self._lock:
            self._stop = True
        try:
            self.nav.cancel_all()
        except Exception:
            pass

    # ------------------- helpers -------------------
    def _emit(self, s: str):
        s = str(s)
        self._event_pub.publish(String(data=s))
        with self._lock:
            self._last_event = s

        rid = str(self._active_run_id or "")
        if self._mission_store is not None:
            try:
                code = "EVENT"
                if s.startswith("HEALTH_") or s.startswith("HEALTH:"):
                    code = "HEALTH"
                elif s.startswith("JOB_"):
                    code = "SCHED"
                elif s.startswith("RUN_"):
                    code = "RUN"
                elif s.startswith("CMD:"):
                    code = "CMD"
                self._mission_store.add_event(run_id=rid, source="TASK", level="INFO", code=code, msg=s, data_json="")
            except Exception:
                pass

    def _publish_state(self, s: str):
        s = str(s)
        self._public_state = s
        self._state_pub.publish(String(data=s))
        self._persist_if_changed()

    # ------------------- diagnostics / health monitor -------------------
    def _diag_cb_battery(self, stat):
        soc, fresh = self._battery_ok()
        if soc is None:
            stat.summary(DiagnosticStatus.WARN, "battery: no data")
        elif not fresh:
            stat.summary(DiagnosticStatus.WARN, f"battery stale soc={soc:.3f}")
        else:
            stat.summary(DiagnosticStatus.OK, f"soc={soc:.3f}")
        try:
            stat.add("soc", f"{(soc if soc is not None else 0.0):.3f}")
            stat.add("fresh", str(bool(fresh)))
            stat.add("topic", str(self.battery_topic))
        except Exception:
            pass
        return stat

    def _diag_cb_health(self, stat):
        res: HealthResult = self._health_last if self._health_last is not None else HealthResult()
        if res.level == "OK":
            stat.summary(DiagnosticStatus.OK, "OK")
        else:
            stat.summary(res.as_diag_level(), f"{res.code}: {res.msg}")
        try:
            with self._lock:
                stat.add("mission_state", str(self._mission_state))
                stat.add("phase", str(self._phase))
                stat.add("executor_state", str(self._executor_state))
                stat.add("fault_active", str(bool(self._health_fault_active)))
            for k, v in (res.details or {}).items():
                stat.add(str(k), str(v))
        except Exception:
            pass
        return stat

    def _health_code_recoverable(self, code: str) -> bool:
        return str(code or "").strip().upper() in self._health_auto_recover_codes

    def _clear_health_auto_recover(self, reset_count: bool = False):
        with self._lock:
            self._health_recover_pending = False
            self._health_recover_resume_after_ts = 0.0
            self._health_recover_run_id = ""
            self._health_recover_code = ""
            if reset_count:
                self._health_recover_count = 0

    def _arm_health_auto_recover(self, code: str, msg: str):
        if not self._health_auto_recover_enable:
            return

        with self._lock:
            rid = str(self._active_run_id or "")
            if not rid:
                return
            if self._health_recover_pending:
                return
            if not self._health_code_recoverable(code):
                return
            if self._health_recover_count >= self._health_auto_recover_max:
                self._emit(f"HEALTH_AUTO_RECOVER_EXHAUSTED:run={rid} code={code}")
                return

            self._health_recover_count += 1
            self._health_recover_pending = True
            self._health_recover_resume_after_ts = time.time() + self._health_auto_resume_delay_s
            self._health_recover_run_id = rid
            self._health_recover_code = str(code or "")

            attempt = int(self._health_recover_count)
            max_n = int(self._health_auto_recover_max)

        self._emit(f"HEALTH_AUTO_RECOVER_ARMED:run={rid} attempt={attempt}/{max_n} code={code} msg={msg}")

    def _tick_health_auto_recover(self):
        if not self._health_auto_recover_enable:
            return

        now = time.time()
        with self._lock:
            pending = bool(self._health_recover_pending)
            rid = str(self._health_recover_run_id or "")
            active_rid = str(self._active_run_id or "")
            code = str(self._health_recover_code or "")
            resume_after = float(self._health_recover_resume_after_ts)
            mission_state = str(self._mission_state or "")
            phase = str(self._phase or "")
            exec_state = str(self._executor_state or "")
            health_fault_active = bool(self._health_fault_active)
            attempt = int(self._health_recover_count)
            max_n = int(self._health_auto_recover_max)

        if not pending:
            return
        if not rid or rid != active_rid:
            self._clear_health_auto_recover(reset_count=False)
            return
        if now < resume_after:
            return
        if mission_state != "PAUSED":
            return
        if phase != "IDLE":
            return
        if health_fault_active:
            return
        if exec_state not in ["PAUSED", "PAUSED_RECOVERY"]:
            return

        with self._lock:
            self._health_recover_pending = False
            self._health_recover_resume_after_ts = 0.0
            self._health_recover_run_id = ""
            self._health_recover_code = ""
            self._mission_state = "RUNNING"

        self._emit(f"HEALTH_AUTO_RESUME:run={rid} attempt={attempt}/{max_n} code={code}")
        self._publish_state("RUNNING")
        self._mission_update_state(rid, "RUNNING", reason=f"health_auto_resume:{code}")
        self._resume_executor_with_current_intent(run_id=rid, context="health_auto_resume")

    def _pause_by_health(self, code: str, msg: str):
        with self._lock:
            if str(self._mission_state) != "RUNNING":
                return
            rid = str(self._active_run_id or "")
            self._mission_state = "PAUSED"
        self._send_exec_cmd(str(self._auto_pause_cmd or "pause"))
        self._mission_update_state(rid, "PAUSED", reason=f"health:{code}")
        self._publish_state("PAUSED")
        self._emit(f"HEALTH_PAUSED:code={code} msg={msg}")
        self._arm_health_auto_recover(code, msg)

    def _on_health_timer(self, _evt):
        if not self.health_enable:
            return
        hm = self._health_monitor
        if hm is None:
            return
        try:
            now = time.time()
            with self._lock:
                mission_state = str(self._mission_state)
                phase = str(self._phase)
                exec_state = str(self._executor_state)
                rid = str(self._active_run_id)
                prog = self._last_run_progress
                prog_ts = float(self._last_run_progress_ts)

            p01 = None
            exec_err_code = ""
            exec_err_msg = ""
            if prog is not None and (now - prog_ts) <= 2.0:
                try:
                    if (not rid) or (str(prog.run_id) == rid):
                        p01 = float(prog.progress_0_1)
                        exec_state = str(prog.state) or exec_state
                        exec_err_code = str(getattr(prog, "error_code", "") or "")
                        exec_err_msg = str(getattr(prog, "error_msg", "") or "")
                except Exception:
                    pass

            res = hm.evaluate(
                now=now,
                mission_state=mission_state,
                phase=phase,
                exec_state=exec_state,
                last_progress_ts=float(prog_ts),
                progress_0_1=p01,
            )
            self._health_last = res

            prev_level = str(self._health_prev_level)
            prev_code = str(self._health_prev_code)
            self._health_prev_level = str(res.level)
            self._health_prev_code = str(res.code)

            if res.level == "ERROR":
                self._health_error_code = str(res.code)
                self._health_error_msg = str(res.msg)
                if not self._health_fault_active:
                    self._health_fault_active = True
                    self._emit(f"HEALTH_ERROR:{res.code}:{res.msg}")
                if self._auto_pause_on_health_error:
                    self._pause_by_health(str(res.code), str(res.msg))
            elif res.level == "WARN":
                self._health_error_code = str(res.code)
                self._health_error_msg = str(res.msg)
                if prev_level == "OK" and res.code:
                    self._emit(f"HEALTH_WARN:{res.code}:{res.msg}")
            else:
                if prev_level in ["WARN", "ERROR"]:
                    self._emit(f"HEALTH_RECOVERED:from={prev_level}:{prev_code}")
                self._health_fault_active = False
                self._health_error_code = ""
                self._health_error_msg = ""

            if (not self._health_error_code) and exec_err_code:
                self._health_error_code = exec_err_code
                self._health_error_msg = exec_err_msg

            if self._diag_updater is not None:
                self._diag_updater.update()
        except Exception as e:
            rospy.logwarn_throttle(2.0, "[TASK] health tick failed: %s", str(e))

    def _on_task_state_timer(self, _evt):
        try:
            now = time.time()
            with self._lock:
                mission_state = str(self._mission_state)
                phase = str(self._phase)
                public_state = str(self._public_state)

                active_job_id = str(self._active_job_id)
                run_id = str(self._active_run_id)
                zone_id = str(self._active_zone)
                plan_prof = str(self._task_plan_profile_name)
                sys_prof = str(self._task_sys_profile_name)
                mode = str(self._task_clean_mode)

                loops_total = int(self._job_loops_total)
                loops_done = int(self._job_loops_done)
                active_loop = int(self._active_run_loop_index)

                exec_state = str(self._executor_state)

                soc = float(self._battery.soc) if self._battery.soc is not None else 0.0
                batt_valid = (self._battery.ts > 0.0) and ((now - self._battery.ts) <= float(self.battery_stale_timeout_s))

                prog = self._last_run_progress
                prog_ts = float(self._last_run_progress_ts)

                health_code = str(self._health_error_code or "")
                health_msg = str(self._health_error_msg or "")
                last_event = str(self._last_event or "")

            p01 = 0.0
            ppct = 0.0
            plan_id = ""
            map_id = ""
            map_md5 = ""
            il_active = False
            il_reason = ""
            if prog is not None and (now - prog_ts) <= 2.0:
                try:
                    prog_run_id = str(getattr(prog, "run_id", "") or "")
                    runtime_idle = (
                        (not run_id)
                        and mission_state == "IDLE"
                        and phase == "IDLE"
                        and public_state == "IDLE"
                    )
                    if ((run_id and prog_run_id == run_id) or ((not runtime_idle) and ((not prog_run_id) or prog_run_id == run_id))):
                        p01 = float(prog.progress_0_1)
                        ppct = float(prog.progress_pct)
                        exec_state = str(prog.state) or exec_state
                        plan_id = str(getattr(prog, "plan_id", "") or "")
                        map_id = str(getattr(prog, "map_id", "") or "")
                        map_md5 = str(getattr(prog, "map_md5", "") or "")
                        il_active = bool(getattr(prog, "interlock_active", False))
                        il_reason = str(getattr(prog, "interlock_reason", "") or "")
                except Exception:
                    pass

            if (not map_id) and (not map_md5):
                try:
                    mid, mmd5, ok = ensure_map_identity(
                        map_topic="/map",
                        timeout_s=1.0,
                        set_global_params=True,
                        set_private_params=False,
                        refresh=True,
                    )
                    if ok:
                        map_id = str(mid or "")
                        map_md5 = str(mmd5 or "")
                except Exception:
                    pass

            m = TaskStateMsg()
            m.mission_state = mission_state
            m.phase = phase
            m.public_state = public_state
            m.active_job_id = active_job_id
            m.run_id = run_id
            m.zone_id = zone_id
            m.plan_profile = plan_prof
            m.sys_profile = sys_prof
            m.mode = mode
            m.plan_id = str(plan_id)
            m.map_id = str(map_id)
            m.map_md5 = str(map_md5)
            m.loops_total = max(0, int(loops_total))
            m.loops_done = max(0, int(loops_done))
            m.active_loop_index = max(0, int(active_loop))
            m.progress_0_1 = float(max(0.0, min(p01, 1.0)))
            m.progress_pct = float(max(0.0, min(ppct, 100.0)))
            m.executor_state = exec_state

            m.error_code = str(health_code)
            m.error_msg = str(health_msg)

            m.interlock_active = bool(il_active)
            m.interlock_reason = str(il_reason)
            m.last_event = str(last_event)
            m.battery_soc = float(max(0.0, min(soc, 1.0)))
            m.battery_valid = bool(batt_valid)
            m.stamp = rospy.Time.now()
            self._task_state_pub.publish(m)
        except Exception:
            pass

    def _send_exec_cmd(self, cmd: str):
        cmd = (cmd or "").strip()
        if not cmd:
            return
        self._cmd_pub.publish(String(data=cmd))
        try:
            rospy.loginfo("[TASK] -> executor cmd: %s", cmd)
        except Exception:
            pass

        rid = str(self._active_run_id or "")
        if self._mission_store is not None:
            try:
                self._mission_store.add_event(
                    run_id=rid,
                    source="TASK",
                    level="INFO",
                    code="EXEC_CMD",
                    msg=cmd,
                    data_json="",
                )
            except Exception:
                pass

    def _send_exec_resume_cmd(
        self,
        *,
        run_id: str = "",
        context: str = "resume",
        fault_public_state: str = "",
    ) -> bool:
        rid = str(run_id or self._active_run_id or "").strip()
        if rid:
            self._send_exec_cmd(f"resume run_id={rid}")
            return True

        reason = "missing active_run_id during %s" % str(context or "resume")
        self._emit("RESUME_CONTEXT_MISSING:%s" % str(context or "resume"))
        rospy.logerr("[TASK] %s", reason)
        if fault_public_state:
            self._enter_blocking_fault(fault_public_state, reason)
        return False

    def _resume_executor_with_current_intent(
        self,
        *,
        run_id: str = "",
        context: str = "resume",
        fault_public_state: str = "",
    ) -> bool:
        self._push_task_intent_to_executor()
        return self._send_exec_resume_cmd(
            run_id=run_id,
            context=context,
            fault_public_state=fault_public_state,
        )

    def _send_exec_start_cmd(self, *, zone_id: str = "", run_id: str = "") -> bool:
        zone = str(zone_id or self._active_zone or "").strip()
        if not zone:
            self._emit("START_CONTEXT_MISSING:zone_id")
            rospy.logerr("[TASK] missing zone_id during start dispatch")
            return False
        rid = str(run_id or self._active_run_id or "").strip()
        cmd = f"start zone_id={zone}"
        if rid:
            cmd += f" run_id={rid}"
        self._send_exec_cmd(cmd)
        return True

    def _new_run_id(self) -> str:
        return uuid.uuid4().hex

    def _mission_create_run(
        self,
        *,
        job_id: str,
        zone_id: str,
        loop_index: int,
        loops_total: int,
        trigger_source: str = "",
    ) -> str:
        run_id = self._new_run_id()
        self._active_run_id = run_id
        self._active_run_loop_index = int(loop_index)
        self._clear_health_auto_recover(reset_count=True)
        run_trigger_source = str(trigger_source or "").strip()
        self._emit(
            f"RUN_START:run={run_id} zone={zone_id} job={job_id or 'manual'} "
            f"loop={int(loop_index)}/{int(loops_total)} trigger={run_trigger_source or '-'}"
        )
        if self._mission_store is not None:
            try:
                sys_profile_name = self._resolve_effective_sys_profile(self._task_sys_profile_name)
                mode_profile = self._mode_catalog.get(sys_profile_name)
                controller_name = ""
                actuator_profile_name = sys_profile_name
                if mode_profile is not None:
                    controller_name = str(mode_profile.mbf_controller_name or "").strip()
                    actuator_profile_name = str(mode_profile.actuator_profile_name or actuator_profile_name).strip()
                map_id, map_md5, _ok = ensure_map_identity(
                    map_topic="/map",
                    timeout_s=1.0,
                    set_global_params=True,
                    set_private_params=False,
                    refresh=True,
                )
                runtime_map_name, _runtime_scope_name = get_runtime_map_scope()
                runtime_map_revision_id = get_runtime_map_revision_id("/cartographer/runtime")
                task_map_name = str(self._task_map_name or runtime_map_name or "").strip()
                task_map_revision_id = str(self._task_map_revision_id or runtime_map_revision_id or "").strip()
                if not run_trigger_source:
                    if str(self._active_schedule_id or "").strip():
                        run_trigger_source = f"SCHEDULE:{self._active_schedule_id}"
                    elif str(job_id or "").strip():
                        run_trigger_source = "TASK"
                    else:
                        run_trigger_source = "MANUAL"
                self._mission_store.create_run(
                    run_id=run_id,
                    job_id=str(job_id or ""),
                    map_name=task_map_name,
                    map_revision_id=task_map_revision_id,
                    zone_id=str(zone_id or ""),
                    plan_profile_name=str(self._task_plan_profile_name or ""),
                    sys_profile_name=str(sys_profile_name or ""),
                    clean_mode=str(self._resolve_effective_clean_mode(sys_profile_name, self._task_clean_mode)),
                    loop_index=int(loop_index),
                    loops_total=int(loops_total),
                    mbf_controller_name=controller_name,
                    actuator_profile_name=actuator_profile_name,
                    trigger_source=run_trigger_source,
                    map_id=str(map_id or ""),
                    map_md5=str(map_md5 or ""),
                    state="RUNNING",
                    reason="",
                )
                self._mission_store.add_event(
                    run_id=run_id,
                    source="TASK",
                    level="INFO",
                    code="RUN_START",
                    msg=f"run started: zone={zone_id} job={job_id or 'manual'} loop={loop_index}/{loops_total}",
                    data_json="",
                )
            except Exception as e:
                rospy.logerr("[TASK] create_run failed: run=%s zone=%s err=%s", run_id, zone_id, str(e))
        return run_id

    def _mission_update_state(self, run_id: str, state: str, reason: str = ""):
        if not run_id:
            return
        if self._mission_store is None:
            return
        try:
            terminal = self._is_terminal_state(state)
            self._mission_store.update_state(
                run_id,
                state=str(state),
                reason=str(reason or ""),
                set_end=terminal,
            )
        except Exception as e:
            rospy.logerr_throttle(
                2.0,
                "[TASK] mission_update_state failed run=%s state=%s err=%s",
                str(run_id), str(state), str(e),
            )

    # ------------------- task persistence -------------------
    def _snapshot(self) -> TaskState:
        now = time.time()
        batt_soc = float(self._battery.soc) if self._battery.soc is not None else 0.0
        batt_valid = (self._battery.ts > 0.0) and ((now - self._battery.ts) <= float(self.battery_stale_timeout_s))
        return TaskState(
            active_zone=str(self._active_zone),
            active_run_id=str(self._active_run_id),
            active_run_loop_index=int(self._active_run_loop_index),
            task_map_name=str(self._task_map_name),
            task_map_revision_id=str(self._task_map_revision_id),
            plan_profile_name=str(self._task_plan_profile_name),
            sys_profile_name=str(self._task_sys_profile_name),
            clean_mode=str(self._task_clean_mode),
            return_to_dock_on_finish=bool(self._task_return_to_dock_on_finish),
            repeat_after_full_charge=bool(self._task_repeat_after_full_charge),
            mission_state=str(self._mission_state),
            phase=str(self._phase),
            public_state=str(self._public_state),
            armed=bool(self._armed),

            active_job_id=str(self._active_job_id),
            active_schedule_id=str(self._active_schedule_id),
            job_loops_total=int(self._job_loops_total),
            job_loops_done=int(self._job_loops_done),
            dock_state=str(self._dock_supply_state or ""),
            battery_soc=batt_soc,
            battery_valid=bool(batt_valid),
            executor_state=str(self._executor_state or ""),
            last_error_code=str(self._health_error_code or ""),
            last_error_msg=str(self._health_error_msg or ""),
            updated_ts=now,
        )

    def _persist_if_changed(self, *, force: bool = False):
        if (not self._persist_enable) or (self._store is None):
            return
        st = self._snapshot()
        if (not force) and (self._last_persist is not None):
            lp = self._last_persist
            if (
                st.active_zone == lp.active_zone
                and st.active_run_id == lp.active_run_id
                and st.active_run_loop_index == lp.active_run_loop_index
                and st.task_map_name == lp.task_map_name
                and st.task_map_revision_id == lp.task_map_revision_id
                and st.plan_profile_name == lp.plan_profile_name
                and st.sys_profile_name == lp.sys_profile_name
                and st.clean_mode == lp.clean_mode
                and st.return_to_dock_on_finish == lp.return_to_dock_on_finish
                and st.repeat_after_full_charge == lp.repeat_after_full_charge
                and st.mission_state == lp.mission_state
                and st.phase == lp.phase
                and st.public_state == lp.public_state
                and st.armed == lp.armed
                and st.active_job_id == lp.active_job_id
                and st.active_schedule_id == lp.active_schedule_id
                and st.job_loops_total == lp.job_loops_total
                and st.job_loops_done == lp.job_loops_done
                and st.executor_state == lp.executor_state
            ):
                return
        try:
            self._store.save(st)
            self._last_persist = st
        except Exception as e:
            rospy.logerr_throttle(2.0, "[TASK] persist failed: %s", str(e))

    def _persist_now(self):
        self._persist_if_changed(force=True)

    def _public_state_from_intent(self) -> str:
        if self._public_state:
            return str(self._public_state)
        if self._phase and self._phase != "IDLE":
            return str(self._phase)
        return str(self._mission_state or "IDLE")

    def _restore_state_if_any(self):
        if (not self._restore_enable) or (self._store is None):
            return
        try:
            st = self._store.load()
        except Exception as e:
            rospy.logwarn("[TASK] restore skipped: %s", str(e))
            return
        if st is None:
            return
        if st.updated_ts > 1e-6 and self._task_state_max_age_s > 1e-3:
            age = time.time() - float(st.updated_ts)
            if age > self._task_state_max_age_s:
                rospy.logwarn("[TASK] ignore stale task_state: age=%.1fs", age)
                return

        if st.active_zone:
            self._active_zone = str(st.active_zone)
        self._task_map_name = str(getattr(st, "task_map_name", "") or "")
        self._task_map_revision_id = str(getattr(st, "task_map_revision_id", "") or "")

        plan_prof = getattr(st, "plan_profile_name", "")
        if plan_prof:
            self._task_plan_profile_name = str(plan_prof)

        sys_prof = getattr(st, "sys_profile_name", "") or ""
        if sys_prof:
            self._task_sys_profile_name = str(sys_prof)
        self._task_sys_profile_name = self._resolve_effective_sys_profile(self._task_sys_profile_name)

        if getattr(st, "clean_mode", ""):
            self._task_clean_mode = str(st.clean_mode)
        self._task_clean_mode = self._resolve_effective_clean_mode(self._task_sys_profile_name, self._task_clean_mode)
        self._task_return_to_dock_on_finish = bool(
            getattr(st, "return_to_dock_on_finish", self._default_return_to_dock_on_finish)
        )
        self._task_repeat_after_full_charge = bool(getattr(st, "repeat_after_full_charge", False))

        self._active_job_id = str(getattr(st, "active_job_id", "") or "")
        self._active_schedule_id = str(getattr(st, "active_schedule_id", "") or "")
        self._job_loops_total = int(getattr(st, "job_loops_total", 0) or 0)
        self._job_loops_done = int(getattr(st, "job_loops_done", 0) or 0)

        self._active_run_id = str(getattr(st, "active_run_id", "") or "")
        self._active_run_loop_index = int(getattr(st, "active_run_loop_index", 0) or 0)

        ms = str(st.mission_state or "IDLE").upper()
        if ms not in ["IDLE", "RUNNING", "PAUSED", "ESTOP"]:
            ms = "IDLE"
        self._mission_state = ms

        self._phase = str(st.phase or "IDLE")
        self._public_state = str(st.public_state or self._mission_state)
        self._armed = bool(st.armed)
        self._dock_supply_state = str(getattr(st, "dock_state", "") or "IDLE")
        self._last_persist = st

        rospy.loginfo(
            "[TASK] restored: zone=%s mission=%s phase=%s armed=%s public=%s job=%s loops=%d/%d",
            self._active_zone,
            self._mission_state,
            self._phase,
            str(self._armed),
            self._public_state,
            self._active_job_id or "manual",
            self._job_loops_done,
            self._job_loops_total,
        )

        # Continue an unfinished auto-charge workflow after restart (best-effort).
        if self._phase in ["AUTO_DOCKING_STAGE1", "MANUAL_DOCKING_STAGE1"]:
            self._emit("RESTORE:DOCKING_STAGE1")
            self._dock_nav_started_ts = time.time()
            try:
                self._send_exec_cmd("suspend")
            except Exception:
                pass
            self.nav.send_goal(self._dock_stage1_pose())
        elif self._phase in ["AUTO_DOCKING_STAGE2", "MANUAL_DOCKING_STAGE2"]:
            self._emit("RESTORE:DOCKING_STAGE2")
            self._dock_nav_started_ts = time.time()
            try:
                self._send_exec_cmd("suspend")
            except Exception:
                pass
            self._dock_stage2_nav.send_goal(self._dock_pose())
        elif self._phase in ["AUTO_DOCKING", "MANUAL_DOCKING"]:
            self._emit("RESTORE:DOCKING")
            self._dock_nav_started_ts = time.time()
            try:
                self._send_exec_cmd("suspend")
            except Exception:
                pass
            self.nav.send_goal(self._dock_pose())
        elif self._phase in ["AUTO_SUPPLY", "MANUAL_SUPPLY", "AUTO_CHARGING", "MANUAL_CHARGING"]:
            manual = self._phase.startswith("MANUAL_")
            if self._dock_supply_enable:
                restored_phase = self._dock_supply_owner_phase(manual=manual)
                if self._phase != restored_phase:
                    rospy.logwarn(
                        "[TASK] restore phase %s normalized to %s because dock_supply owns charge workflow",
                        self._phase,
                        restored_phase,
                    )
                    self._phase = restored_phase
                self._emit("RESTORE:SUPPLY")
                self._clear_charge_monitor()
                if self._dock_supply_state in ["READY_TO_EXIT", "EXIT_BACKING", "DONE"]:
                    self._publish_state(self._dock_supply_public_state(manual=manual))
                elif self._dock_supply_start():
                    self._publish_state(self._dock_supply_public_state(manual=manual))
                else:
                    rospy.logerr("[TASK] restore %s failed: cannot restart dock_supply workflow", self._phase)
                    self._phase = "IDLE"
                    self._publish_state("ERROR_SUPPLY_START")
            elif self._phase in ["AUTO_SUPPLY", "MANUAL_SUPPLY"]:
                rospy.logerr("[TASK] restore %s failed: dock_supply is disabled", self._phase)
                self._phase = "IDLE"
                self._publish_state("ERROR_SUPPLY_START")
            else:
                self._emit("RESTORE:CHARGING")
                self._begin_charge_monitor()
        elif self._phase in ["AUTO_UNDOCKING", "MANUAL_UNDOCKING"]:
            self._emit("RESTORE:UNDOCKING")
            if self._dock_supply_enable and self._dock_supply_state in ["READY_TO_EXIT", "EXIT_BACKING", "DONE"]:
                self._dock_supply_exit_inflight = self._dock_supply_state != "DONE"
                if self._dock_supply_state == "READY_TO_EXIT":
                    self._dock_supply_request_exit()
                self._publish_state(self._phase)
            else:
                self._undock_nav_started_ts = time.time()
                pose, _ = self._undock_pose()
                self.nav.send_goal(pose)
        elif self._phase == "AUTO_RELOCALIZING":
            self._emit("RESTORE:RELOCALIZING")
            self._set_phase_and_publish("AUTO_RELOCALIZING")
        elif self._phase == "AUTO_REDISPATCHING":
            self._emit("RESTORE:REDISPATCHING")
            self._set_phase_and_publish("AUTO_REDISPATCHING")
        elif self._phase == "FAULT":
            self._emit("RESTORE:FAULT")
            self._publish_state(self._public_state or "FAULT")
        elif self._phase == "AUTO_RESUMING":
            self._emit("RESTORE:RESUME")
            ok, msg = self._prepare_runtime_for_execution(
                run_id=self._active_run_id,
                require_localized=True,
                log_action="restore_resume",
                log_context_key="run",
                log_context_value=self._active_run_id,
                default_error="relocalize required",
            )
            if not ok:
                self._block_restore_auto_resume(str(msg or "relocalize required"))
                return
            self._resume_executor_with_current_intent(
                run_id=self._active_run_id,
                context="restore_auto_resume",
                fault_public_state="ERROR_RESTORE_RESUME_CONTEXT",
            )

    def _battery_ok(self) -> Tuple[Optional[float], bool]:
        with self._lock:
            soc = self._battery.soc
            ts = self._battery.ts
        if soc is None:
            return None, False
        if self.battery_stale_timeout_s > 1e-3 and (time.time() - ts) > self.battery_stale_timeout_s:
            return soc, False
        return soc, True

    def _get_exec_state(self) -> str:
        with self._lock:
            return str(self._executor_state)

    def _is_mission_running(self) -> bool:
        st = self._get_exec_state()
        if st.startswith("FOLLOW") or st.startswith("CONNECT"):
            return True
        if st in ["RUNNING", "START_REQ", "RESUME_REQ", "LOADING_PLAN", "APPLY_PROFILE", "FINISHING"]:
            return True
        return False

    def _task_busy(self) -> bool:
        if self._phase != "IDLE":
            return True
        if self._dock_supply_enable and str(self._dock_supply_state or "").upper() in ["READY_TO_EXIT", "EXIT_BACKING"]:
            return True
        if self._active_run_id and str(self._mission_state or "").upper() == "PAUSED":
            return True
        return self._is_mission_running()

    def _should_trigger_auto_charge(self, soc: float) -> bool:
        if not self.auto_charge_enable:
            return False
        if not self._armed:
            return False
        if soc > self.low_soc:
            return False
        if self.trigger_when_idle:
            return True
        return self._is_mission_running()

    def _refresh_dock_stage1_xyyaw_param(self):
        try:
            raw = rospy.get_param("~dock_stage1_xyyaw", None)
            if raw is None:
                return
            x, y, yaw = _parse_xyyaw(raw, default=self.dock_stage1_xyyaw)
            self.dock_stage1_xyyaw = (float(x), float(y), float(yaw))
        except Exception:
            pass

    def _refresh_dock_xyyaw_param(self):
        """Refresh dock pose from ROS param '~dock_xyyaw' if present."""
        try:
            raw = rospy.get_param("~dock_xyyaw", None)
            if raw is None:
                return
            x, y, yaw = _parse_xyyaw(raw, default=self.dock_xyyaw)
            self.dock_xyyaw = (float(x), float(y), float(yaw))
        except Exception:
            pass

    def _dock_stage1_pose(self):
        self._refresh_dock_stage1_xyyaw_param()
        x, y, yaw = self.dock_stage1_xyyaw
        return _make_pose(self.frame_id, x, y, yaw)

    def _dock_pose(self):
        self._refresh_dock_xyyaw_param()
        x, y, yaw = self.dock_xyyaw
        return _make_pose(self.frame_id, x, y, yaw)

    def _dock_public_state(self, manual: bool) -> str:
        prefix = "MANUAL" if manual else "AUTO"
        if self._is_stage1_docking_phase():
            return f"{prefix}_DOCKING_STAGE1"
        if self._is_stage2_docking_phase():
            return f"{prefix}_DOCKING_STAGE2"
        return f"{prefix}_DOCKING"

    def _dock_supply_public_state(self, manual: bool) -> str:
        prefix = "MANUAL" if manual else "AUTO"
        st = str(self._dock_supply_state or "").strip().upper()
        if st in ["RUNNING", "LOCK_DOCK_POSE", "SEARCH_DOCK_POSE", "PRECISE_DOCKING", "WAIT_STATION_IN_PLACE", "SEARCH_STATION_IN_PLACE", "MECHANICAL_CONNECT"]:
            return f"{prefix}_DOCKING_PRECISE"
        if st == "DRAINING":
            return f"{prefix}_SUPPLY_DRAIN"
        if st == "REFILLING":
            return f"{prefix}_SUPPLY_REFILL"
        if st in ["CHARGING", "CHARGE_CMD_SENT", "CHARGE_CONFIRMED", "DISABLE_CHARGING"]:
            return f"{prefix}_CHARGING"
        if st in ["MECHANICAL_DISCONNECT", "EXIT_BACKING"]:
            return f"{prefix}_UNDOCKING"
        if st == "READY_TO_EXIT":
            return f"{prefix}_SUPPLY"
        return f"{prefix}_SUPPLY"

    def _repeat_cycle_context(self) -> str:
        return (
            f"job={self._active_job_id or '-'} run={self._active_run_id or '-'} "
            f"map={self._task_map_name or '-'} zone={self._active_zone or '-'} "
            f"repeat={int(bool(self._task_repeat_after_full_charge))} "
            f"finish_dock={int(bool(self._task_return_to_dock_on_finish))} "
            f"dock_supply={str(self._dock_supply_state or 'IDLE').strip().upper() or 'IDLE'} "
            f"phase={self._phase or '-'}"
        )

    def _repeat_after_charge_enabled(self) -> bool:
        return bool(self._task_repeat_after_full_charge and self._task_return_to_dock_on_finish and self._active_job_id)

    def _dock_supply_managed_undocking(self) -> bool:
        if self._dock_supply_exit_inflight:
            return True
        if not self._dock_supply_enable:
            return False
        if self._phase not in ["AUTO_UNDOCKING", "MANUAL_UNDOCKING"]:
            return False
        return str(self._dock_supply_state or "").strip().upper() in ["READY_TO_EXIT", "EXIT_BACKING", "DONE"]

    @staticmethod
    def _supply_reason(suffix: str) -> str:
        suffix_name = str(suffix or "").strip().lower()
        if not suffix_name:
            return "supply_error"
        return f"supply_{suffix_name}"

    def _emit_supply_command_failure(self, action: str, detail: str):
        action_name = str(action or "").strip().upper() or "UNKNOWN"
        detail_s = str(detail or "").strip()
        self._emit(f"SUPPLY_{action_name}_FAILED:{detail_s}")

    def _emit_dock_goal_event(self, *, manual: bool, x: float, y: float, yaw: float):
        self._emit(f"DOCK_GOAL:manual={manual} goal=({x:.2f},{y:.2f},{yaw:.2f})")

    def _emit_dock_stage1_goal_event(
        self,
        *,
        manual: bool,
        x: float,
        y: float,
        yaw: float,
        retry_attempt: int = 0,
        retry_total: int = 0,
    ):
        if retry_attempt > 0 and retry_total > 0:
            self._emit(
                "DOCK_GOAL_STAGE1_RETRY:attempt=%d/%d manual=%s goal=(%.2f,%.2f,%.2f)"
                % (int(retry_attempt), int(retry_total), str(bool(manual)), x, y, yaw)
            )
            return
        self._emit(f"DOCK_GOAL_STAGE1:manual={manual} goal=({x:.2f},{y:.2f},{yaw:.2f})")

    def _emit_dock_stage2_goal_event(self, x: float, y: float, yaw: float, controller: str):
        controller_name = str(controller or "<default>")
        self._emit(
            "DOCK_GOAL_STAGE2:goal=(%.2f,%.2f,%.2f) controller=%s"
            % (x, y, yaw, controller_name)
        )

    def _emit_dock_retry_goal_event(self, x: float, y: float, yaw: float, *, retry_attempt: int, retry_total: int):
        self._emit(
            "DOCK_GOAL_RETRY:attempt=%d/%d goal=(%.2f,%.2f,%.2f)"
            % (int(retry_attempt), int(retry_total), x, y, yaw)
        )

    def _emit_undock_goal_event(self, x: float, y: float, yaw: float):
        self._emit(f"UNDOCK_GOAL:goal=({x:.2f},{y:.2f},{yaw:.2f})")

    def _emit_undock_exit_requested(self):
        self._emit("UNDOCK_EXIT_REQUESTED")

    def _emit_dock_succeeded(self):
        self._emit("DOCK_SUCCEEDED")

    def _emit_dock_stage1_succeeded(self):
        self._emit("DOCK_STAGE1_SUCCEEDED")

    def _emit_battery_low_event(self, soc: float):
        self._emit(f"BATTERY_LOW:soc={float(soc):.3f}")

    def _emit_battery_rearmed_event(self, soc: float):
        self._emit(f"BATTERY_REARMED:soc={float(soc):.3f}")

    def _emit_supply_ready_to_exit_event(self, *, repeat_enabled: bool):
        self._emit(
            f"SUPPLY_STATE_READY_TO_EXIT:{self._repeat_cycle_context()} "
            f"repeat_enabled={int(bool(repeat_enabled))}"
        )

    def _emit_auto_recovery_armed(self, phase: str):
        self._emit(
            f"AUTO_RECOVERY_ARMED:phase={str(phase or '').strip() or '-'} {self._repeat_cycle_context()}"
        )

    def _emit_supply_terminal_event(self, state: str):
        state_name = str(state or "").strip().upper()
        if state_name == "DONE":
            self._emit("SUPPLY_SUCCEEDED")
            return
        if state_name == "CANCELED":
            self._emit("SUPPLY_CANCELED")
            return
        self._emit(f"SUPPLY_FAILED:state={state_name or 'FAILED'}")

    def _emit_undock_succeeded(self):
        self._emit("UNDOCK_SUCCEEDED")

    def _emit_undock_terminal_event(self, state: str):
        state_name = str(state or "").strip().upper()
        if state_name == "CANCELED":
            self._emit("UNDOCK_CANCELED")
            return
        self._emit(f"UNDOCK_FAILED:state={state_name or 'FAILED'}")

    def _emit_auto_resume_confirmed(self, exec_state: str):
        self._emit(
            f"AUTO_RESUME_CONFIRMED:exec_state={str(exec_state or '').strip() or '-'}"
        )

    def _emit_undock_nav_failed(self, nav_state: str):
        self._emit(f"UNDOCK_FAILED:nav_state={str(nav_state or '').strip() or '-'}")

    def _emit_dock_nav_failed(self, *, stage: str, nav_state: str):
        self._emit(
            "DOCK_FAILED:stage=%s nav_state=%s"
            % (str(stage or "").strip() or "-", str(nav_state or "").strip() or "-")
        )

    def _dock_supply_set_defer_exit(self, enabled: bool) -> bool:
        if not self._dock_supply_enable or self._dock_supply_set_defer_exit_cli is None:
            return False
        try:
            rospy.wait_for_service(self._dock_supply_set_defer_exit_service, timeout=1.0)
            resp = self._dock_supply_set_defer_exit_cli(bool(enabled))
            ok = bool(getattr(resp, "success", False))
            if ok:
                self._emit("SUPPLY_DEFER_EXIT:%d" % int(bool(enabled)))
            else:
                self._emit_supply_command_failure("DEFER_EXIT", str(getattr(resp, "message", "") or ""))
            return ok
        except Exception as e:
            self._emit_supply_command_failure("DEFER_EXIT", str(e))
            return False

    def _dock_supply_request_exit(self) -> bool:
        if not self._dock_supply_enable or self._dock_supply_exit_cli is None:
            return False
        try:
            rospy.wait_for_service(self._dock_supply_exit_service, timeout=1.0)
            resp = self._dock_supply_exit_cli()
            ok = bool(getattr(resp, "success", False))
            if ok:
                self._dock_supply_exit_inflight = True
                self._emit("SUPPLY_EXIT_START")
            else:
                self._emit_supply_command_failure("EXIT", str(getattr(resp, "message", "") or ""))
            return ok
        except Exception as e:
            self._emit_supply_command_failure("EXIT", str(e))
            return False

    def _clear_costmaps(self) -> Tuple[bool, str]:
        if not self._clear_costmaps_service or self._clear_costmaps_cli is None:
            return False, "clear costmaps service unavailable"
        try:
            rospy.wait_for_service(self._clear_costmaps_service, timeout=2.0)
            self._clear_costmaps_cli()
            self._emit("CLEAR_COSTMAPS")
            return True, ""
        except Exception as e:
            return False, str(e)

    @staticmethod
    def _service_available(service_name: str, expected_type: str = "") -> bool:
        name = str(service_name or "").strip()
        if not name:
            return False
        try:
            runtime_type = str(rosservice.get_service_type(name) or "").strip()
        except Exception:
            return False
        if not runtime_type:
            return False
        expected = str(expected_type or "").strip()
        return (not expected) or (runtime_type == expected)

    def _ensure_named_service_proxy(
        self,
        *,
        cli_attr: str,
        cli_service_name_attr: str,
        label: str,
        app_service_name: str,
        app_service_cls,
        app_service_type: str,
    ):
        current_cli = getattr(self, cli_attr, None)
        current_service_name = str(getattr(self, cli_service_name_attr, "") or "").strip()
        if current_cli is not None and not current_service_name:
            return current_cli, str(app_service_name or "").strip(), "existing"
        service_name = str(app_service_name or "").strip()
        service_cls = app_service_cls
        transport = "app"
        if current_cli is not None and current_service_name:
            if current_service_name == service_name:
                return current_cli, current_service_name, transport
        elif current_cli is not None:
            return current_cli, service_name, transport
        try:
            cli = rospy.ServiceProxy(service_name, service_cls)
            setattr(self, cli_attr, cli)
            setattr(self, cli_service_name_attr, service_name)
            rospy.loginfo(
                "[TASK] %s proxy ready transport=%s service=%s",
                str(label or "service"),
                transport,
                service_name or "-",
            )
            return cli, service_name, transport
        except Exception as e:
            rospy.logwarn(
                "[TASK] %s proxy init failed transport=%s service=%s err=%s",
                str(label or "service"),
                transport,
                service_name or "-",
                str(e),
            )
            setattr(self, cli_attr, None)
            setattr(self, cli_service_name_attr, "")
            return None, service_name, transport

    def _ensure_restart_localization_cli(self):
        return self._ensure_named_service_proxy(
            cli_attr="_restart_localization_cli",
            cli_service_name_attr="_restart_localization_cli_service_name",
            label="restart_localization",
            app_service_name=str(
                getattr(self, "_app_restart_localization_service", "/cartographer/runtime/app/restart_localization")
                or "/cartographer/runtime/app/restart_localization"
            ),
            app_service_cls=AppRestartLocalization,
            app_service_type=APP_RESTART_LOCALIZATION_SERVICE_TYPE,
        )

    def _ensure_slam_submit_command_cli(self):
        return self._ensure_named_service_proxy(
            cli_attr="_slam_submit_command_cli",
            cli_service_name_attr="_slam_submit_command_cli_service_name",
            label="slam_submit_command",
            app_service_name=str(
                getattr(self, "_app_slam_submit_command_service", "/clean_robot_server/app/submit_slam_command")
                or "/clean_robot_server/app/submit_slam_command"
            ),
            app_service_cls=AppSubmitSlamCommand,
            app_service_type=APP_SUBMIT_SLAM_COMMAND_SERVICE_TYPE,
        )

    def _ensure_slam_get_job_cli(self):
        return self._ensure_named_service_proxy(
            cli_attr="_slam_get_job_cli",
            cli_service_name_attr="_slam_get_job_cli_service_name",
            label="slam_get_job",
            app_service_name=str(
                getattr(self, "_app_slam_get_job_service", "/clean_robot_server/app/get_slam_job")
                or "/clean_robot_server/app/get_slam_job"
            ),
            app_service_cls=AppGetSlamJob,
            app_service_type=APP_GET_SLAM_JOB_SERVICE_TYPE,
        )

    def _restart_localization_for_task(self) -> Tuple[bool, str]:
        robot_id = str(self._robot_id or "local_robot")
        requested_map = str(self._task_map_name or "")
        requested_map_revision_id = str(self._task_map_revision_id or "").strip()
        manual_assist_pose_info = inspect_manual_assist_pose_override(
            str(getattr(self, "_manual_assist_pose_param_ns", DEFAULT_MANUAL_ASSIST_POSE_PARAM_NS) or ""),
            requested_map_name=requested_map,
            requested_revision_id=requested_map_revision_id,
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
                    or getattr(self, "_manual_assist_pose_param_ns", DEFAULT_MANUAL_ASSIST_POSE_PARAM_NS)
                    or DEFAULT_MANUAL_ASSIST_POSE_PARAM_NS
                ).strip() or DEFAULT_MANUAL_ASSIST_POSE_PARAM_NS,
            }
            record_manual_assist_pose_override_status(
                manual_assist_ref,
                status="prepare_for_task_pose_%s" % manual_assist_pose_status,
                map_name=requested_map,
                map_revision_id=requested_map_revision_id,
                message=manual_assist_message,
                localization_state="manual_assist_required",
                used=False,
            )
            rospy.logwarn(
                "[TASK] prepare_for_task rejected invalid manual assist pose robot=%s requested_map=%s requested_revision=%s reason=%s %s",
                robot_id,
                requested_map or "-",
                requested_map_revision_id or "-",
                manual_assist_message,
                self._repeat_cycle_context(),
            )
            return False, manual_assist_message
        if manual_assist_pose:
            rospy.loginfo(
                "[TASK] prepare_for_task using %s %s",
                manual_assist_pose_summary(manual_assist_pose),
                self._repeat_cycle_context(),
            )
        submit_cli, submit_service_name, _submit_transport = self._ensure_slam_submit_command_cli()
        get_job_cli, get_job_service_name, _get_job_transport = self._ensure_slam_get_job_cli()
        if submit_cli is not None and get_job_cli is not None:
            try:
                rospy.loginfo(
                    "[TASK] prepare_for_task submit request service=%s robot=%s requested_map=%s requested_revision=%s timeout=%.1fs %s",
                    submit_service_name,
                    robot_id,
                    requested_map or "-",
                    requested_map_revision_id or "-",
                    float(self._restart_localization_timeout_s),
                    self._repeat_cycle_context(),
                )
                rospy.wait_for_service(
                    submit_service_name,
                    timeout=min(5.0, float(self._restart_localization_timeout_s)),
                )
                submit_resp = submit_cli(
                    operation=int(AppSubmitSlamCommand._request_class.prepare_for_task),
                    robot_id=robot_id,
                    map_name=requested_map,
                    map_revision_id=requested_map_revision_id,
                    set_active=False,
                    description="task manager prepare_for_task",
                    frame_id=str((manual_assist_pose or {}).get("frame_id") or "map"),
                    has_initial_pose=bool(manual_assist_pose),
                    initial_pose_x=float((manual_assist_pose or {}).get("initial_pose_x") or 0.0),
                    initial_pose_y=float((manual_assist_pose or {}).get("initial_pose_y") or 0.0),
                    initial_pose_yaw=float((manual_assist_pose or {}).get("initial_pose_yaw") or 0.0),
                    save_map_name="",
                    include_unfinished_submaps=True,
                    set_active_on_save=False,
                    switch_to_localization_after_save=False,
                    relocalize_after_switch=False,
                )
                if bool(getattr(submit_resp, "accepted", False)):
                    job_id = str(getattr(submit_resp, "job_id", "") or "").strip()
                    if manual_assist_pose:
                        if bool((manual_assist_pose or {}).get("consume_once", True)):
                            consume_manual_assist_pose_override(
                                manual_assist_pose,
                                status="submitted_prepare_for_task",
                                map_name=requested_map,
                                map_revision_id=requested_map_revision_id,
                                job_id=job_id,
                                used=False,
                            )
                        else:
                            record_manual_assist_pose_override_status(
                                manual_assist_pose,
                                status="submitted_prepare_for_task",
                                map_name=requested_map,
                                map_revision_id=requested_map_revision_id,
                                job_id=job_id,
                                used=False,
                            )
                    if not job_id:
                        if manual_assist_pose:
                            record_manual_assist_pose_override_status(
                                manual_assist_pose,
                                status="prepare_for_task_missing_job_id",
                                map_name=requested_map,
                                map_revision_id=requested_map_revision_id,
                                message="slam prepare_for_task accepted without job_id",
                                used=False,
                            )
                        return False, "slam prepare_for_task accepted without job_id"
                    deadline = time.time() + float(self._restart_localization_timeout_s)
                    last_message = "waiting slam job=%s" % job_id
                    while (time.time() < deadline) and (not rospy.is_shutdown()):
                        rospy.wait_for_service(
                            get_job_service_name,
                            timeout=min(5.0, max(1.0, deadline - time.time())),
                        )
                        job_resp = get_job_cli(job_id=job_id, robot_id=robot_id)
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
                            or "slam prepare_for_task running"
                        )
                        if bool(getattr(job, "done", False)):
                            final_state = str(getattr(job, "localization_state", "") or "").strip()
                            if bool(getattr(job, "success", False)):
                                response_map = str(
                                    getattr(job, "resolved_map_name", "")
                                    or getattr(job, "requested_map_name", "")
                                    or requested_map
                                    or ""
                                )
                                response_revision_id = str(
                                    getattr(job, "resolved_map_revision_id", "")
                                    or getattr(job, "requested_map_revision_id", "")
                                    or requested_map_revision_id
                                    or ""
                                ).strip()
                                self._task_map_name = response_map
                                if response_revision_id:
                                    self._task_map_revision_id = response_revision_id
                                else:
                                    selected_asset = self._get_selected_active_map() or {}
                                    if str(selected_asset.get("map_name") or "").strip() == response_map:
                                        self._task_map_revision_id = str(
                                            selected_asset.get("revision_id") or ""
                                        ).strip()
                                if manual_assist_pose:
                                    already_ready = "already ready" in str(last_message or "").strip().lower()
                                    record_manual_assist_pose_override_status(
                                        manual_assist_pose,
                                        status="prepare_for_task_already_ready" if already_ready else "prepare_for_task_succeeded",
                                        map_name=response_map,
                                        map_revision_id=response_revision_id or requested_map_revision_id,
                                        message=last_message,
                                        localization_state=final_state or "localized",
                                        job_id=job_id,
                                        used=(not already_ready),
                                    )
                                return True, last_message or "localized"
                            if bool(getattr(job, "manual_assist_required", False)) or localization_needs_manual_assist(
                                str(getattr(job, "localization_state", "") or "")
                            ):
                                if manual_assist_pose:
                                    record_manual_assist_pose_override_status(
                                        manual_assist_pose,
                                        status="prepare_for_task_manual_assist_required",
                                        map_name=requested_map,
                                        map_revision_id=requested_map_revision_id,
                                        message=last_message,
                                        localization_state=final_state or "manual_assist_required",
                                        job_id=job_id,
                                        used=True,
                                    )
                                return False, runtime_localization_not_ready_message(
                                    str(getattr(job, "localization_state", "") or "manual_assist_required"),
                                    False,
                                    map_name=requested_map,
                                    map_revision_id=requested_map_revision_id,
                                    retry_action="prepare_for_task",
                                )
                            if manual_assist_pose:
                                record_manual_assist_pose_override_status(
                                    manual_assist_pose,
                                    status="prepare_for_task_failed",
                                    map_name=requested_map,
                                    map_revision_id=requested_map_revision_id,
                                    message=last_message,
                                    localization_state=final_state or "not_localized",
                                    job_id=job_id,
                                    used=True,
                                )
                            return False, last_message or str(getattr(job_resp, "message", "") or "localize failed")
                        rospy.sleep(0.2)
                    if manual_assist_pose:
                        record_manual_assist_pose_override_status(
                            manual_assist_pose,
                            status="prepare_for_task_timeout",
                            map_name=requested_map,
                            map_revision_id=requested_map_revision_id,
                            message=last_message,
                            localization_state="not_localized",
                            job_id=job_id,
                            used=True,
                        )
                    return False, "slam prepare_for_task timeout after %.1fs: %s" % (
                        float(self._restart_localization_timeout_s),
                        last_message,
                    )
                if manual_assist_pose:
                    record_manual_assist_pose_override_status(
                        manual_assist_pose,
                        status="prepare_for_task_submit_rejected",
                        map_name=requested_map,
                        map_revision_id=requested_map_revision_id,
                        message=str(
                            getattr(submit_resp, "message", "") or getattr(submit_resp, "error_code", "") or ""
                        ),
                        used=False,
                    )
                rospy.logwarn(
                    "[TASK] prepare_for_task submit rejected service=%s robot=%s requested_map=%s err=%s %s",
                    submit_service_name,
                    robot_id,
                    requested_map or "-",
                    str(getattr(submit_resp, "message", "") or getattr(submit_resp, "error_code", "") or ""),
                    self._repeat_cycle_context(),
                )
            except Exception as e:
                if manual_assist_pose:
                    record_manual_assist_pose_override_status(
                        manual_assist_pose,
                        status="prepare_for_task_fallback_restart_localization",
                        map_name=requested_map,
                        map_revision_id=requested_map_revision_id,
                        message=str(e),
                        used=False,
                    )
                rospy.logwarn(
                    "[TASK] prepare_for_task submit flow failed; falling back to restart_localization submit=%s get_job=%s %s err=%s",
                    submit_service_name,
                    get_job_service_name,
                    self._repeat_cycle_context(),
                    str(e),
                )
                if manual_assist_pose:
                    rospy.logwarn(
                        "[TASK] prepare_for_task fallback will rely on shared manual assist pose via shared manual-assist path: %s %s",
                        manual_assist_pose_summary(manual_assist_pose),
                        self._repeat_cycle_context(),
                    )
        restart_cli, restart_service_name, _restart_transport = self._ensure_restart_localization_cli()
        if restart_cli is None:
            rospy.logerr(
                "[TASK] restart_localization proxy init failed service=%s %s",
                restart_service_name or "-",
                self._repeat_cycle_context(),
            )
            return False, "restart_localization service unavailable"
        try:
            rospy.loginfo(
                "[TASK] restart_localization request service=%s robot=%s requested_map=%s requested_revision=%s timeout=%.1fs %s",
                restart_service_name,
                robot_id,
                requested_map or "-",
                requested_map_revision_id or "-",
                float(self._restart_localization_timeout_s),
                self._repeat_cycle_context(),
            )
            rospy.wait_for_service(
                restart_service_name,
                timeout=min(5.0, float(self._restart_localization_timeout_s)),
            )
            resp = restart_cli(
                robot_id=robot_id,
                map_name=requested_map,
                map_revision_id=requested_map_revision_id,
            )
        except Exception as e:
            rospy.logerr(
                "[TASK] restart_localization call failed service=%s robot=%s requested_map=%s requested_revision=%s %s err=%s",
                restart_service_name,
                robot_id,
                requested_map or "-",
                requested_map_revision_id or "-",
                self._repeat_cycle_context(),
                str(e),
            )
            return False, str(e)
        rospy.loginfo(
            "[TASK] restart_localization response success=%s response_map=%s state=%s message=%s %s",
            str(bool(getattr(resp, "success", False))).lower(),
            str(getattr(resp, "map_name", "") or requested_map or "-"),
            str(getattr(resp, "localization_state", "") or "-"),
            str(getattr(resp, "message", "") or ""),
            self._repeat_cycle_context(),
        )
        if not bool(getattr(resp, "success", False)):
            state = str(getattr(resp, "localization_state", "") or "").strip().lower()
            if localization_needs_manual_assist(state):
                return False, runtime_localization_not_ready_message(
                    state,
                    False,
                    map_name=requested_map,
                    map_revision_id=requested_map_revision_id,
                    retry_action="prepare_for_task",
                )
            return False, str(getattr(resp, "message", "") or "restart localization failed")
        self._task_map_name = str(getattr(resp, "map_name", "") or self._task_map_name or "")
        response_revision_id = str(getattr(resp, "map_revision_id", "") or requested_map_revision_id or "").strip()
        if response_revision_id:
            self._task_map_revision_id = response_revision_id
        else:
            selected_asset = self._get_selected_active_map() or {}
            if str(selected_asset.get("map_name") or "").strip() == self._task_map_name:
                self._task_map_revision_id = str(selected_asset.get("revision_id") or "").strip()
        return True, str(getattr(resp, "message", "") or "localized")

    def _enter_blocking_fault(self, public_state: str, reason: str):
        state_name = str(public_state or "ERROR").strip() or "ERROR"
        reason_s = str(reason or "").strip()
        self._emit(f"BLOCKING_FAULT:{state_name}:{reason_s}")
        self.nav.cancel_all()
        self._dock_stage2_nav.cancel_all()
        self._clear_charge_monitor()
        self._dock_nav_started_ts = 0.0
        self._undock_nav_started_ts = 0.0
        self._dock_supply_exit_inflight = False
        self._armed = True
        self._mission_state = "IDLE"
        self._phase = "FAULT"
        self._publish_state(state_name)

    def _dock_supply_owner_phase(self, manual: bool) -> str:
        return "MANUAL_SUPPLY" if manual else "AUTO_SUPPLY"

    def _is_dock_supply_owner_phase(self) -> bool:
        return self._phase in ["AUTO_SUPPLY", "MANUAL_SUPPLY"]

    def _is_task_side_charge_phase(self) -> bool:
        return self._phase in ["AUTO_CHARGING", "MANUAL_CHARGING"]

    def _is_stage1_docking_phase(self) -> bool:
        return self._phase in ["AUTO_DOCKING_STAGE1", "MANUAL_DOCKING_STAGE1"]

    def _is_stage2_docking_phase(self) -> bool:
        return self._phase in ["AUTO_DOCKING_STAGE2", "MANUAL_DOCKING_STAGE2"]

    def _dock_nav_client(self):
        if self._is_stage2_docking_phase():
            return self._dock_stage2_nav
        return self.nav

    def _reset_dock_retry_state(self):
        self._dock_retry_count = 0

    def _is_recoverable_dock_supply_failure(self, dock_supply_state: str) -> bool:
        return str(dock_supply_state or "").strip().upper() in self._dock_recoverable_supply_states

    def _retry_dock_from_stage1(self, *, manual: bool, reason: str) -> bool:
        if self._dock_retry_count >= self.dock_retry_limit:
            return False
        self._dock_retry_count += 1
        self._emit(
            "DOCK_RETRY:%d/%d reason=%s"
            % (self._dock_retry_count, self.dock_retry_limit, str(reason or "").strip() or "-")
        )
        self.nav.cancel_all()
        self._dock_stage2_nav.cancel_all()
        self._clear_charge_monitor()
        self._undock_nav_started_ts = 0.0
        self._dock_supply_state = "IDLE"
        if self._dock_sys_profile_name:
            self._emit(f"DOCK_APPLY_SYS_PROFILE:{self._dock_sys_profile_name}")
            self._apply_nav_profile_for_sys(self._dock_sys_profile_name)
        if self.dock_two_stage_enable:
            self._phase = "MANUAL_DOCKING_STAGE1" if manual else "AUTO_DOCKING_STAGE1"
            self._dock_nav_started_ts = time.time()
            self._publish_state(self._dock_public_state(manual))
            x, y, yaw = self.dock_stage1_xyyaw
            self._emit_dock_stage1_goal_event(
                manual=manual,
                x=x,
                y=y,
                yaw=yaw,
                retry_attempt=self._dock_retry_count,
                retry_total=self.dock_retry_limit,
            )
            self.nav.send_goal(self._dock_stage1_pose())
        else:
            self._phase = "MANUAL_DOCKING" if manual else "AUTO_DOCKING"
            self._dock_nav_started_ts = time.time()
            self._publish_state(self._dock_public_state(manual))
            x, y, yaw = self.dock_xyyaw
            self._emit_dock_retry_goal_event(
                x,
                y,
                yaw,
                retry_attempt=self._dock_retry_count,
                retry_total=self.dock_retry_limit,
            )
            self.nav.send_goal(self._dock_pose())
        return True

    def _start_dock_stage2(self, *, manual: bool):
        self._phase = "MANUAL_DOCKING_STAGE2" if manual else "AUTO_DOCKING_STAGE2"
        self._dock_nav_started_ts = time.time()
        self._publish_state(self._dock_public_state(manual))
        x, y, yaw = self.dock_xyyaw
        self._emit_dock_stage2_goal_event(x, y, yaw, self.dock_stage2_controller)
        self._dock_stage2_nav.send_goal(self._dock_pose())

    def _begin_supply_or_charge_after_dock(self, *, manual: bool, soc: Optional[float], fresh: bool):
        self._emit_dock_succeeded()
        if not manual:
            if self._dock_supply_enable:
                if not self._dock_supply_set_defer_exit(True):
                    self._enter_charge_fault(
                        "ERROR_SUPPLY_START",
                        reason=self._supply_reason("defer_exit_failed"),
                        manual=False,
                    )
                    return
                if self._dock_supply_start():
                    self._clear_charge_monitor()
                    self._phase = self._dock_supply_owner_phase(manual=False)
                    self._publish_state(self._dock_supply_public_state(manual=False))
                else:
                    self._enter_charge_fault(
                        "ERROR_SUPPLY_START",
                        reason=self._supply_reason("start_failed"),
                        manual=False,
                    )
            else:
                self._phase = "AUTO_CHARGING"
                self._begin_charge_monitor(soc=soc, fresh=fresh)
                self._publish_state("AUTO_CHARGING")
        else:
            if self._dock_supply_enable:
                if not self._dock_supply_set_defer_exit(True):
                    self._enter_charge_fault(
                        "ERROR_SUPPLY_START",
                        reason=self._supply_reason("defer_exit_failed"),
                        manual=(not self._active_run_id),
                    )
                    return
                if self._dock_supply_start():
                    self._clear_charge_monitor()
                    self._phase = self._dock_supply_owner_phase(manual=True)
                    self._publish_state(self._dock_supply_public_state(manual=True))
                else:
                    self._enter_charge_fault(
                        "ERROR_SUPPLY_START",
                        reason=self._supply_reason("start_failed"),
                        manual=(not self._active_run_id),
                    )
            else:
                self._phase = "MANUAL_CHARGING"
                self._begin_charge_monitor(soc=soc, fresh=fresh)
                self._publish_state("MANUAL_CHARGING")

    def _undock_pose(self) -> Tuple[object, Tuple[float, float, float]]:
        self._refresh_dock_xyyaw_param()
        x, y, yaw = self.dock_xyyaw
        x2 = x + self.undock_forward_m * math.cos(yaw)
        y2 = y + self.undock_forward_m * math.sin(yaw)
        return _make_pose(self.frame_id, x2, y2, yaw), (x2, y2, yaw)

    def _wait(self, timeout_s: float, cond_fn, sleep_s: float = 0.05) -> bool:
        t0 = time.time()
        while not rospy.is_shutdown():
            with self._lock:
                if self._stop:
                    return False
            if (time.time() - t0) > float(timeout_s):
                return False
            if cond_fn():
                return True
            time.sleep(float(sleep_s))
        return False

    def _split_cmd(self, cmd: str) -> List[str]:
        cmd = (cmd or "").strip()
        if not cmd:
            return []
        try:
            return shlex.split(cmd, posix=True)
        except Exception:
            return cmd.split()

    def _parse_kv_tokens(self, tokens):
        kv = {}
        for t in tokens or []:
            if not t or "=" not in t:
                continue
            k, v = t.split("=", 1)
            k = (k or "").strip().lower()
            v = (v or "").strip()
            if not k:
                continue
            kv[k] = v
        return kv

    def _resolve_job_record(self, job_id: str) -> Optional[JobRecord]:
        jid = str(job_id or "").strip()
        if (not jid) or (self._ops_store is None):
            return None
        try:
            return self._ops_store.get_job(jid)
        except Exception as e:
            rospy.logerr("[TASK] get_job failed: job=%s err=%s", jid, str(e))
            return None

    def _start_job_record(
        self,
        job: JobRecord,
        *,
        source: str = "TASK",
        schedule_id: str = "",
        trigger_source: str = "",
    ) -> Tuple[bool, str]:
        if job is None:
            return False, "task not found"

        job_id = str(job.job_id or "").strip()
        zone_id = str(job.zone_id or "").strip() or self.zone_id_default
        map_name = str(job.map_name or "").strip()
        plan_profile_name = str(job.plan_profile_name or "").strip()
        sys_profile_name = str(job.sys_profile_name or "").strip()
        clean_mode = str(job.default_clean_mode or "").strip()
        return_to_dock_on_finish = bool(job.return_to_dock_on_finish)
        repeat_after_full_charge = bool(getattr(job, "repeat_after_full_charge", False))
        loops_total = max(1, int(job.default_loops or 1))

        if not bool(job.enabled):
            return False, "task is disabled"
        if not zone_id:
            return False, "task zone_id is empty"

        self._active_zone = zone_id
        if plan_profile_name:
            self._task_plan_profile_name = plan_profile_name
        if sys_profile_name:
            self._task_sys_profile_name = self._resolve_effective_sys_profile(sys_profile_name)
        else:
            self._task_sys_profile_name = self._resolve_effective_sys_profile(self._task_sys_profile_name)
        self._task_clean_mode = self._resolve_effective_clean_mode(
            self._task_sys_profile_name,
            clean_mode or self._task_clean_mode,
        )
        self._task_repeat_after_full_charge = bool(repeat_after_full_charge)
        self._task_return_to_dock_on_finish = bool(return_to_dock_on_finish or repeat_after_full_charge)
        self._task_map_name = map_name
        self._task_map_revision_id = str(getattr(job, "map_revision_id", "") or "")

        self._active_schedule_id = str(schedule_id or "").strip()
        self._active_job_id = job_id
        self._job_loops_total = loops_total
        self._job_loops_done = 0

        ok, msg = self._ensure_zone_plan_ready_for_job(
            map_name=self._task_map_name,
            map_revision_id=self._task_map_revision_id,
            zone_id=self._active_zone,
            plan_profile_name=self._task_plan_profile_name,
        )
        if not ok:
            return False, str(msg or "task zone/plan not ready")

        ok, msg = self._prepare_runtime_for_execution(
            explicit_map_name=self._task_map_name,
            require_localized=True,
            log_action="start_job",
            log_context_key="job",
            log_context_value=self._active_job_id,
            default_error="map prepare failed",
        )
        if not ok:
            return False, str(msg or "map prepare failed")

        self._enter_running_dispatch_state()
        self._emit(
            f"JOB_START:id={self._active_job_id} zone={self._active_zone} map={self._task_map_name or '-'} "
            f"loops={self._job_loops_total} plan={self._task_plan_profile_name} "
            f"sys={self._task_sys_profile_name} mode={self._task_clean_mode} "
            f"finish_dock={int(self._task_return_to_dock_on_finish)} "
            f"repeat_after_charge={int(self._task_repeat_after_full_charge)} "
            f"source={source} schedule={self._active_schedule_id or '-'}"
        )
        self._mission_create_run(
            job_id=self._active_job_id,
            zone_id=self._active_zone,
            loop_index=1,
            loops_total=self._job_loops_total,
            trigger_source=(
                str(trigger_source or "").strip()
                or (
                    f"SCHEDULE:{self._active_schedule_id}"
                    if self._active_schedule_id
                    else ("TASK" if self._active_job_id else "MANUAL")
                )
            ),
        )
        if not self._send_exec_start_cmd(zone_id=self._active_zone, run_id=self._active_run_id):
            return False, "zone_id is required for start"
        return True, ""

    def _start_task_by_id(self, task_id: str, *, source: str = "TASK") -> Tuple[bool, str]:
        tid = str(task_id or "").strip()
        if not tid:
            return False, "task_id is required"
        job = self._resolve_job_record(tid)
        if job is None:
            return False, "task not found"
        ok, msg = self._start_job_record(job, source=source, schedule_id="", trigger_source="TASK")
        if not ok:
            self._enter_idle_state(reset_job_runner=True)
            rospy.logerr("[TASK] task start rejected: task=%s err=%s", tid, msg)
        return ok, msg

    def _pause_current_task(self) -> Tuple[bool, str]:
        self._send_exec_cmd("pause")
        self._mission_update_state(self._active_run_id, "PAUSED")
        self._mission_state = "PAUSED"
        self._publish_state("PAUSED")
        return True, ""

    def _enter_idle_state(self, *, reset_job_runner: bool = False):
        if reset_job_runner:
            self._reset_job_runner()
        self._phase = "IDLE"
        self._mission_state = "IDLE"
        self._publish_state("IDLE")

    def _set_phase_and_publish(self, phase: str, *, public_state: str = ""):
        phase_name = str(phase or "").strip()
        public_name = str(public_state or phase_name).strip() or phase_name
        self._phase = phase_name
        self._publish_state(public_name)

    def _return_manual_sequence_to_idle(self):
        self._phase = "IDLE"
        self._publish_state(self._manual_sequence_idle_public_state())

    def _enter_running_dispatch_state(self):
        self._phase = "IDLE"
        self._mission_state = "RUNNING"
        self._publish_state("RUNNING")
        self._push_task_intent_to_executor()

    def _start_auto_resuming(self, *, context: str, missing_reason: str) -> bool:
        if not self._active_run_id:
            self._enter_blocking_fault(
                "ERROR_AUTO_RESUME_CONTEXT",
                str(missing_reason or "missing active_run_id during auto resume"),
            )
            return False
        self._set_phase_and_publish("AUTO_RESUMING")
        self._resume_executor_with_current_intent(
            run_id=self._active_run_id,
            context=str(context or "auto_resume"),
            fault_public_state="ERROR_AUTO_RESUME_CONTEXT",
        )
        return True

    def _arm_auto_relocalizing(self):
        self._emit_auto_recovery_armed("AUTO_RELOCALIZING")
        rospy.loginfo("[TASK] auto relocalizing armed %s", self._repeat_cycle_context())
        self._set_phase_and_publish("AUTO_RELOCALIZING")

    def _arm_auto_redispatching(self):
        self._emit_auto_recovery_armed("AUTO_REDISPATCHING")
        rospy.loginfo("[TASK] auto redispatching armed %s", self._repeat_cycle_context())
        self._set_phase_and_publish("AUTO_REDISPATCHING")

    def _handle_auto_supply_done(self):
        if self._repeat_after_charge_enabled():
            self._enter_blocking_fault(
                "ERROR_RELOCALIZE_REQUIRED",
                "dock supply exited before relocalization",
            )
            return
        self._start_auto_resuming(
            context="auto_supply_resume",
            missing_reason="missing active_run_id during auto supply resume",
        )

    def _handle_undock_success(self):
        self._emit_undock_succeeded()
        self._undock_nav_started_ts = 0.0
        if self._phase != "AUTO_UNDOCKING":
            self._return_manual_sequence_to_idle()
            return
        if self._active_run_id:
            self._start_auto_resuming(
                context="auto_undock_resume",
                missing_reason="missing active_run_id during auto undock resume",
            )
            return
        if self._repeat_after_charge_enabled():
            self._arm_auto_redispatching()
            return
        self._enter_blocking_fault(
            "ERROR_AUTO_RESUME_CONTEXT",
            "missing active_run_id during auto undock resume",
        )

    def _complete_auto_resuming_if_executor_running(self) -> bool:
        st = self._get_exec_state()
        if not (st.startswith("CONNECT") or st.startswith("FOLLOW")):
            return False
        self._emit_auto_resume_confirmed(st)
        self._set_phase_and_publish("IDLE", public_state="RUNNING")
        return True

    def _handle_dock_supply_phase(self) -> bool:
        if not self._is_dock_supply_owner_phase():
            return False
        st = str(self._dock_supply_state or "").upper()
        manual = (self._phase == "MANUAL_SUPPLY")
        if self._is_recoverable_dock_supply_failure(st):
            if self._retry_dock_from_stage1(manual=manual, reason=st.lower()):
                return True
            self._emit_supply_terminal_event(st)
            self._enter_charge_fault(
                "ERROR_DOCK",
                reason=self._supply_reason(f"state_{st.lower()}"),
                manual=(not self._active_run_id),
            )
            return False
        if st == "READY_TO_EXIT":
            repeat_enabled = self._repeat_after_charge_enabled()
            self._emit_supply_ready_to_exit_event(repeat_enabled=repeat_enabled)
            rospy.loginfo(
                "[TASK] dock supply ready_to_exit repeat_enabled=%s %s",
                str(bool(repeat_enabled)).lower(),
                self._repeat_cycle_context(),
            )
            if self._phase == "AUTO_SUPPLY":
                if repeat_enabled:
                    self._arm_auto_relocalizing()
                elif not self._transition_to_undocking(manual=False):
                    self._enter_charge_fault(
                        "ERROR_UNDOCK_PREP",
                        reason=self._supply_reason("exit_start_failed"),
                        manual=False,
                    )
            else:
                self._reset_dock_retry_state()
                self._return_manual_sequence_to_idle()
            return False
        if st == "DONE":
            self._emit_supply_terminal_event(st)
            if self._phase == "AUTO_SUPPLY":
                self._handle_auto_supply_done()
            else:
                self._reset_dock_retry_state()
                self._return_manual_sequence_to_idle()
            return False
        if st.startswith("FAILED") or st == "CANCELED":
            self._emit_supply_terminal_event(st)
            self._enter_charge_fault(
                "ERROR_SUPPLY_FAILED" if st.startswith("FAILED") else f"ERROR_SUPPLY_{st}",
                reason=self._supply_reason(f"state_{st.lower()}"),
                manual=(not self._active_run_id),
            )
        return False

    def _handle_task_side_charge_phase(self, *, soc: Optional[float], fresh: bool):
        if not self._is_task_side_charge_phase():
            return
        now = time.time()
        if fresh and soc is not None:
            if self._charge_last_soc is None or float(soc) > float(self._charge_last_soc) + 1e-3:
                self._charge_last_soc = float(soc)
                self._charge_last_fresh_ts = now
            elif self._charge_last_fresh_ts <= 0.0:
                self._charge_last_fresh_ts = now

        if self.charge_timeout_s > 1e-3 and self._charge_started_ts > 0.0 and (now - self._charge_started_ts) >= self.charge_timeout_s:
            self._enter_charge_fault(
                "ERROR_CHARGE_TIMEOUT",
                reason="charge_timeout",
                manual=(not self._active_run_id),
            )
            return
        if (not fresh) and self.charge_battery_stale_timeout_s > 1e-3 and self._charge_started_ts > 0.0 and (
            now - (self._charge_last_fresh_ts if self._charge_last_fresh_ts > 0.0 else self._charge_started_ts)
        ) >= self.charge_battery_stale_timeout_s:
            self._enter_charge_fault(
                "ERROR_CHARGE_BATTERY_STALE",
                reason="battery_stale_during_charge",
                manual=(not self._active_run_id),
            )
            return
        if fresh and _soc_reached_threshold(soc, self.resume_soc):
            self._emit(f"CHARGED:soc={soc:.3f}")
            if self._phase == "AUTO_CHARGING":
                self._transition_to_undocking(manual=False)
            else:
                self._clear_charge_monitor()
                self._return_manual_sequence_to_idle()

    def _handle_undocking_phase(self):
        if self._phase not in ["AUTO_UNDOCKING", "MANUAL_UNDOCKING"]:
            return
        if self._dock_sequence_timed_out(self._undock_nav_started_ts):
            if self._dock_supply_managed_undocking():
                self._dock_supply_cancel()
            else:
                self.nav.cancel_all()
            self._enter_charge_fault(
                "ERROR_UNDOCK_TIMEOUT",
                reason="undock_timeout",
                manual=(not self._active_run_id),
            )
        if self._dock_supply_managed_undocking():
            st = str(self._dock_supply_state or "").upper()
            if st == "READY_TO_EXIT" and (not self._dock_supply_exit_inflight):
                if not self._dock_supply_request_exit():
                    self._enter_charge_fault(
                        "ERROR_UNDOCK_PREP",
                        reason=self._supply_reason("exit_restart_failed"),
                        manual=(not self._active_run_id),
                    )
            elif st == "DONE":
                self._dock_supply_exit_inflight = False
                self._handle_undock_success()
            elif st.startswith("FAILED") or st == "CANCELED":
                self._emit_undock_terminal_event(st)
                self._enter_charge_fault(
                    "ERROR_UNDOCK",
                    reason=self._supply_reason(f"state_{st.lower()}"),
                    manual=(not self._active_run_id),
                )
            return
        if self.nav.done():
            if self.nav.succeeded():
                self._handle_undock_success()
            else:
                self._emit_undock_nav_failed(self.nav.get_state())
                self._enter_charge_fault(
                    "ERROR_UNDOCK",
                    reason=f"undock_nav_failed:{self.nav.get_state()}",
                    manual=(not self._active_run_id),
                )

    def _handle_auto_repeat_recovery_phase(self):
        if self._phase == "AUTO_RELOCALIZING":
            self._emit(f"AUTO_RELOCALIZING_START:{self._repeat_cycle_context()}")
            rospy.loginfo("[TASK] auto relocalizing start %s", self._repeat_cycle_context())
            ok, msg = self._restart_localization_for_task()
            if not ok:
                self._emit(f"AUTO_RELOCALIZING_FAILED:{msg}")
                rospy.logerr("[TASK] auto relocalizing failed %s err=%s", self._repeat_cycle_context(), str(msg))
                self._enter_blocking_fault("ERROR_RELOCALIZE_FAILED", msg)
                return
            self._emit(f"AUTO_RELOCALIZING_OK:{self._repeat_cycle_context()} message={msg}")
            rospy.loginfo("[TASK] auto relocalizing ok %s message=%s", self._repeat_cycle_context(), str(msg))
            ok, clear_msg = self._clear_costmaps()
            if not ok:
                self._emit(f"AUTO_RELOCALIZING_CLEAR_COSTMAPS_FAILED:{clear_msg}")
                self._enter_blocking_fault("ERROR_CLEAR_COSTMAPS", clear_msg)
            elif not self._transition_to_undocking(manual=False):
                self._emit("AUTO_RELOCALIZING_UNDOCK_PREP_FAILED")
                self._enter_blocking_fault(
                    "ERROR_UNDOCK_PREP",
                    self._supply_reason("exit_start_failed_after_relocalize"),
                )
            else:
                self._emit(f"AUTO_RELOCALIZING_TO_UNDOCK:{self._repeat_cycle_context()}")
                rospy.loginfo("[TASK] auto relocalizing -> undock %s", self._repeat_cycle_context())
            return
        if self._phase == "AUTO_REDISPATCHING":
            rospy.loginfo("[TASK] auto redispatching start %s", self._repeat_cycle_context())
            ok, msg = self._redispatch_current_task_template()
            if not ok:
                self._enter_blocking_fault("ERROR_REDISPATCH", msg)
            return
        if self._phase == "AUTO_RESUMING":
            self._complete_auto_resuming_if_executor_running()

    def _resume_current_task(self, *, run_id: str = "", map_name: str = "") -> Tuple[bool, str]:
        run_tok = str(run_id or "").strip()
        if run_tok:
            self._active_run_id = run_tok

        if not str(self._active_run_id or "").strip():
            return False, "run_id is required for resume"

        if self._active_run_id and (not self._active_job_id):
            self._active_schedule_id = ""
            self._rehydrate_job_context_from_run(self._active_run_id)

        ok, msg = self._prepare_runtime_for_execution(
            explicit_map_name=str(map_name or "").strip(),
            run_id=self._active_run_id,
            require_localized=True,
            log_action="resume",
            log_context_key="run",
            log_context_value=self._active_run_id,
            default_error="resume map prepare failed",
        )
        if not ok:
            return False, str(msg or "resume map prepare failed")

        self._enter_running_dispatch_state()
        self._mission_update_state(self._active_run_id, "RUNNING", reason="manual_resume")
        if not self._resume_executor_with_current_intent(run_id=self._active_run_id, context="manual_resume"):
            return False, "run_id is required for resume"
        return True, ""

    def _stop_current_task(self) -> Tuple[bool, str]:
        if self._dock_supply_enable and (
            self._is_dock_supply_owner_phase()
            or self._dock_supply_managed_undocking()
            or str(self._dock_supply_state or "").upper() == "READY_TO_EXIT"
        ):
            self._dock_supply_cancel()
        self.nav.cancel_all()
        self._send_exec_cmd("cancel")
        self._mission_update_state(self._active_run_id, "CANCELED")
        self._clear_charge_monitor()
        self._reset_dock_retry_state()
        self._dock_nav_started_ts = 0.0
        self._undock_nav_started_ts = 0.0

        if self._active_schedule_id:
            self._sched_store_mark_done(self._active_schedule_id, "CANCELED")
        self._clear_health_auto_recover(reset_count=True)
        self._enter_idle_state(reset_job_runner=True)
        return True, ""

    def _start_manual_return(self) -> Tuple[bool, str]:
        if self._phase != "IDLE":
            return False, "return requires idle phase"
        self._emit("MANUAL_DOCK")
        started = self._start_dock_sequence(manual=True)
        if not started:
            return False, "dock sequence start failed"
        return True, ""

    def _parse_int(self, s: str, default: Optional[int] = None) -> Optional[int]:
        try:
            if s is None:
                return default
            ss = str(s).strip()
            if ss == "":
                return default
            return int(ss, 10)
        except Exception:
            return default

    def _parse_bool(self, value, default: Optional[bool] = None) -> Optional[bool]:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        text = str(value).strip().lower()
        if text == "":
            return default
        if text in ("1", "true", "yes", "on", "enable", "enabled", "dock", "return"):
            return True
        if text in ("0", "false", "no", "off", "disable", "disabled", "stay", "no_return"):
            return False
        return default

    def _resolve_task_map_asset(
        self,
        *,
        explicit_map_name: str = "",
        run_id: str = "",
    ) -> Optional[Dict[str, object]]:
        explicit_map_name = str(explicit_map_name or "").strip()
        map_name = explicit_map_name
        map_revision_id = ""
        task_scope_map_name = str(getattr(self, "_task_map_name", "") or "").strip()
        task_scope_revision_id = str(getattr(self, "_task_map_revision_id", "") or "").strip()

        if (not map_name) and run_id and self._mission_store is not None:
            try:
                run = self._mission_store.get_run(str(run_id or "").strip())
                if run is not None:
                    map_name = str(run.map_name or "").strip()
                    map_revision_id = str(getattr(run, "map_revision_id", "") or "").strip()
                    if run.zone_id:
                        self._active_zone = str(run.zone_id)
            except Exception:
                pass

        if not map_name:
            map_name = task_scope_map_name
        if (not map_revision_id) and task_scope_revision_id:
            if (not explicit_map_name) or (task_scope_map_name and task_scope_map_name == map_name):
                map_revision_id = task_scope_revision_id

        if self._plan_store is None:
            return None

        asset = None
        try:
            if map_revision_id:
                asset = self._plan_store.resolve_map_revision(
                    revision_id=map_revision_id,
                    robot_id=self._robot_id,
                )
                resolved_map_name = str((asset or {}).get("map_name") or "").strip()
                if map_name and resolved_map_name and resolved_map_name != map_name:
                    raise ValueError("task map revision does not match task map name")
            if (asset is None) and map_name:
                asset = self._plan_store.resolve_map_asset(
                    map_name=map_name,
                    robot_id=self._robot_id,
                )
                active_asset = self._plan_store.get_active_map(robot_id=self._robot_id) or {}
                asset_error = map_asset_verification_error(asset or {}, label="map asset") if asset else ""
                active_matches_name = str(active_asset.get("map_name") or "").strip() == str(map_name or "").strip()
                active_error = (
                    map_asset_verification_error(active_asset, label="map asset")
                    if active_asset and active_matches_name
                    else "map asset not found"
                )
                if asset_error and active_matches_name and (not active_error):
                    asset = active_asset
            elif asset is None:
                asset = self._plan_store.get_active_map(robot_id=self._robot_id)
        except ValueError:
            raise
        except Exception:
            asset = None

        if asset:
            asset_error = map_asset_verification_error(asset, label="map asset")
            if asset_error:
                raise ValueError(asset_error)
            self._task_map_name = str(asset.get("map_name") or "")
            self._task_map_revision_id = str(asset.get("revision_id") or map_revision_id or "")
        return asset

    def _get_selected_active_map(self) -> Optional[Dict[str, object]]:
        plan_store = getattr(self, "_plan_store", None)
        if plan_store is None:
            return None
        try:
            return plan_store.get_active_map(robot_id=self._robot_id)
        except Exception:
            return None

    def _wait_for_runtime_map(self, asset: Dict[str, object], timeout_s: Optional[float] = None) -> bool:
        target_revision_id = str((asset or {}).get("revision_id") or "").strip()
        target_name = str((asset or {}).get("map_name") or "").strip()
        target_id = str((asset or {}).get("map_id") or "").strip()
        target_md5 = str((asset or {}).get("map_md5") or "").strip()
        wait_s = max(0.5, float(timeout_s if timeout_s is not None else 10.0))

        def _matches() -> bool:
            cur_name, cur_version = get_runtime_map_scope()
            cur_revision_id = get_runtime_map_revision_id("/cartographer/runtime")
            cur_id, cur_md5, _ok = ensure_map_identity(
                map_topic="/map",
                timeout_s=1.0,
                set_global_params=True,
                set_private_params=False,
                refresh=True,
            )
            cur_revision_id = str(cur_revision_id or "").strip()
            if target_revision_id:
                if cur_revision_id:
                    return cur_revision_id == target_revision_id
            if target_name and str(cur_name or "").strip() != target_name:
                return False
            if target_id and str(cur_id or "").strip() != target_id:
                return False
            if target_md5 and str(cur_md5 or "").strip() != target_md5:
                return False
            return True

        return self._wait(wait_s, _matches, sleep_s=0.1)

    def _validate_runtime_map_asset(self, asset: Optional[Dict[str, object]], *, timeout_s: Optional[float] = None) -> Tuple[bool, str]:
        if not asset:
            return False, "map asset not resolved"
        if not bool(asset.get("enabled", True)):
            return False, "map asset is disabled"

        asset_revision_id = str(asset.get("revision_id") or "").strip()
        asset_map_name = str(asset.get("map_name") or "").strip()
        slam_state = self._get_fresh_slam_state()
        slam_active_map_name = str(getattr(slam_state, "active_map_name", "") or "").strip() if slam_state is not None else ""
        slam_runtime_match = bool(getattr(slam_state, "runtime_map_match", False)) if slam_state is not None else False
        selected_asset = self._get_selected_active_map() or {}
        selected_revision_id = str(selected_asset.get("revision_id") or "").strip()
        slam_can_authoritatively_validate_map = bool(
            slam_state is not None
            and asset_map_name
            and slam_active_map_name
            and slam_active_map_name == asset_map_name
            and ((not asset_revision_id) or (selected_revision_id == asset_revision_id))
        )

        if slam_can_authoritatively_validate_map and slam_runtime_match:
            self._task_map_name = asset_map_name
            self._task_map_revision_id = asset_revision_id
            return True, "runtime_map_match"

        if self._wait_for_runtime_map(asset, timeout_s=0.2):
            self._task_map_name = asset_map_name
            self._task_map_revision_id = asset_revision_id
            return True, "runtime_map_match"

        if not self._require_runtime_map_match:
            self._task_map_name = asset_map_name
            self._task_map_revision_id = asset_revision_id
            rospy.logwarn_throttle(
                30.0,
                "[TASK] runtime map consistency gate disabled; continue with selected map asset=%s",
                self._task_map_name,
            )
            return True, "runtime_map_match_skipped"

        if slam_can_authoritatively_validate_map:
            runtime_map = {
                "revision_id": "",
                "map_name": str(getattr(slam_state, "runtime_map_name", "") or "").strip(),
                "map_id": str(getattr(slam_state, "runtime_map_id", "") or "").strip(),
                "map_md5": str(getattr(slam_state, "runtime_map_md5", "") or "").strip(),
            }
        else:
            runtime_map = self._runtime_map_snapshot(refresh=True)
        return (
            False,
            runtime_map_switch_required_before_task_message(
                selected_revision_id=str(asset.get("revision_id") or ""),
                selected_map_name=asset_map_name,
                selected_map_id=str(asset.get("map_id") or ""),
                selected_map_md5=str(asset.get("map_md5") or ""),
                runtime_revision_id=str(runtime_map.get("revision_id") or ""),
                runtime_map_name=str(runtime_map.get("map_name") or ""),
                runtime_map_id=str(runtime_map.get("map_id") or ""),
                runtime_map_md5=str(runtime_map.get("map_md5") or ""),
            ),
        )

    def _runtime_localization_status(self) -> Tuple[str, bool]:
        try:
            state = str(rospy.get_param(self._runtime_localization_state_param, "") or "").strip().lower()
        except Exception:
            state = ""
        try:
            valid = bool(rospy.get_param(self._runtime_localization_valid_param, False))
        except Exception:
            valid = False
        return state, valid

    def _ensure_runtime_localized(self) -> Tuple[bool, str]:
        slam_state = self._get_fresh_slam_state()
        if slam_state is not None:
            state = str(getattr(slam_state, "localization_state", "") or "").strip().lower()
            valid = bool(getattr(slam_state, "localization_valid", False))
            manual_assist_required = bool(getattr(slam_state, "manual_assist_required", False))
            manual_assist_map_name = str(getattr(slam_state, "manual_assist_map_name", "") or "").strip()
            manual_assist_map_revision_id = str(
                getattr(slam_state, "manual_assist_map_revision_id", "") or ""
            ).strip()
            manual_assist_retry_action = str(
                getattr(slam_state, "manual_assist_retry_action", "") or ""
            ).strip()
        else:
            state, valid = self._runtime_localization_status()
            manual_assist_required = localization_needs_manual_assist(state)
            selected_asset_getter = getattr(self, "_get_selected_active_map", None)
            selected_asset = selected_asset_getter() if callable(selected_asset_getter) else {}
            manual_assist_map_name = str(selected_asset.get("map_name") or "").strip()
            manual_assist_map_revision_id = str(selected_asset.get("revision_id") or "").strip()
            manual_assist_retry_action = "relocalize"
        if localization_is_ready(state, valid):
            return True, ""
        if bool(manual_assist_required) or localization_needs_manual_assist(state):
            return False, runtime_localization_not_ready_message(
                state,
                valid,
                map_name=manual_assist_map_name,
                map_revision_id=manual_assist_map_revision_id,
                retry_action=manual_assist_retry_action or "relocalize",
            )
        return False, runtime_localization_relocalize_required_before_task_message(state, valid)

    def _prepare_runtime_for_execution(
        self,
        *,
        explicit_map_name: str = "",
        run_id: str = "",
        require_localized: bool = True,
        log_action: str = "",
        log_context_key: str = "",
        log_context_value: str = "",
        default_error: str = "map prepare failed",
    ) -> Tuple[bool, str]:
        _asset, ok, msg = self._prepare_map_for_run(
            explicit_map_name=explicit_map_name,
            run_id=run_id,
            require_localized=require_localized,
        )
        if ok:
            return True, str(msg or "")

        detail = str(msg or default_error or "map prepare failed").strip()
        if log_action:
            rospy.logerr(
                "[TASK] %s blocked: runtime prepare failed %s=%s err=%s",
                str(log_action or "task_prepare"),
                str(log_context_key or "context"),
                str(log_context_value or "-"),
                detail,
            )
        return False, detail

    def _block_restore_auto_resume(self, reason: str):
        self._mission_state = "PAUSED"
        self._set_phase_and_publish("IDLE", public_state="WAIT_RELOCALIZE")
        self._emit("RESTORE:RESUME_BLOCKED:%s" % str(reason or "relocalize required"))

    def _prepare_map_for_run(
        self,
        *,
        explicit_map_name: str = "",
        run_id: str = "",
        require_localized: bool = True,
    ) -> Tuple[Optional[Dict[str, object]], bool, str]:
        try:
            asset = self._resolve_task_map_asset(
                explicit_map_name=explicit_map_name,
                run_id=run_id,
            )
        except ValueError as exc:
            return None, False, str(exc)
        requested_name = str(explicit_map_name or "").strip()
        requested_revision_id = str(getattr(self, "_task_map_revision_id", "") or "").strip()
        if (not requested_name) and run_id and self._mission_store is not None:
            try:
                run = self._mission_store.get_run(str(run_id or "").strip())
                if run is not None:
                    requested_name = str(run.map_name or "").strip()
                    requested_revision_id = str(getattr(run, "map_revision_id", "") or requested_revision_id).strip()
            except Exception:
                pass
        selected_asset = self._get_selected_active_map()
        if selected_asset is None and self._require_managed_map_asset:
            return None, False, no_current_active_map_selected_message()
        if selected_asset is not None and requested_name:
            selected_name = str(selected_asset.get("map_name") or "").strip()
            selected_revision_id = str(selected_asset.get("revision_id") or "").strip()
            if requested_revision_id and not selected_revision_id:
                return None, False, selected_map_does_not_match_requested_map_message(
                    selected_name or requested_name,
                    requested_name,
                    selected_revision_id=selected_revision_id,
                    requested_revision_id=requested_revision_id,
                )
            if requested_revision_id and selected_revision_id and requested_revision_id != selected_revision_id:
                return None, False, selected_map_does_not_match_requested_map_message(
                    selected_name or requested_name,
                    requested_name,
                    selected_revision_id=selected_revision_id,
                    requested_revision_id=requested_revision_id,
                )
            if selected_name and requested_name != selected_name:
                return None, False, selected_map_does_not_match_requested_map_message(
                    selected_name,
                    requested_name,
                )
        if not asset:
            requested_scope = bool(
                str(explicit_map_name or "").strip()
                or str(self._task_map_name or "").strip()
            )
            if self._require_managed_map_asset:
                return None, False, "no map asset available"
            if requested_scope:
                return None, False, "map asset not found"
            return None, True, "runtime_map_unmanaged"
        ok, msg = self._validate_runtime_map_asset(asset)
        if ok and require_localized and self._require_runtime_localized_before_start:
            ok, msg = self._ensure_runtime_localized()
        return asset, bool(ok), str(msg or "")

    def _ensure_zone_plan_ready_for_job(
        self,
        *,
        map_name: str,
        map_revision_id: str = "",
        zone_id: str,
        plan_profile_name: str,
    ) -> Tuple[bool, str]:
        if self._plan_store is None:
            return True, ""
        zone_id = str(zone_id or "").strip()
        map_name = str(map_name or "").strip()
        map_revision_id = str(map_revision_id or "").strip()
        profile = str(plan_profile_name or "").strip() or "cover_standard"
        if not zone_id:
            return False, "task zone_id is empty"
        if not map_name:
            return False, "task map_name is empty"
        try:
            zone = self._plan_store.get_zone_meta(
                zone_id,
                map_name=map_name,
                map_revision_id=map_revision_id,
            )
        except Exception as e:
            return False, "zone metadata unavailable: %s" % str(e)
        if not zone:
            return False, "zone not found on selected map"
        if not bool(zone.get("enabled", True)):
            return False, "zone is disabled"
        try:
            plan_id = self._plan_store.get_active_plan_id(
                zone_id,
                profile,
                map_name=map_name,
                map_revision_id=map_revision_id,
            )
        except Exception as e:
            return False, "active plan lookup failed: %s" % str(e)
        if not plan_id:
            return False, "active plan not found for zone/profile"
        try:
            plan_meta = self._plan_store.load_plan_meta(plan_id)
        except Exception as e:
            return False, "active plan metadata is unavailable: %s" % str(e)
        plan_map_revision_id = str(plan_meta.get("map_revision_id") or "").strip()
        plan_map_name = str(plan_meta.get("map_name") or "").strip()
        plan_zone_id = str(plan_meta.get("zone_id") or "").strip()
        plan_profile = str(plan_meta.get("plan_profile_name") or "").strip()
        if map_revision_id and plan_map_revision_id and plan_map_revision_id != map_revision_id:
            return False, "active plan revision does not match task map revision"
        if plan_map_name and plan_map_name != map_name:
            return False, "active plan map does not match task map"
        if plan_zone_id and plan_zone_id != zone_id:
            return False, "active plan zone does not match task zone"
        if plan_profile and plan_profile != profile:
            return False, "active plan profile does not match task profile"
        return True, ""

    def _resolve_effective_sys_profile(self, sys_profile_name: str) -> str:
        candidate = str(sys_profile_name or "").strip()
        if candidate:
            if self._mode_catalog.get(candidate) is not None:
                return candidate
            if self._mode_catalog.names():
                rospy.logwarn_throttle(5.0, "[TASK] unknown sys_profile=%s -> fallback=%s", candidate, self._default_sys_profile_name)
        if self._mode_catalog.names():
            return str(self._default_sys_profile_name or "standard").strip() or "standard"
        return candidate or str(self._default_sys_profile_name or "standard").strip() or "standard"

    def _resolve_effective_clean_mode(self, sys_profile_name: str, clean_mode: str) -> str:
        mode = str(clean_mode or "").strip()
        if mode:
            return mode
        mp = self._mode_catalog.get(self._resolve_effective_sys_profile(sys_profile_name))
        if mp is not None and str(mp.default_clean_mode or "").strip():
            return str(mp.default_clean_mode).strip()
        return "scrub"

    def _apply_nav_profile_for_sys(self, sys_profile_name: str) -> bool:
        sys_profile_name = self._resolve_effective_sys_profile(sys_profile_name)
        if not sys_profile_name:
            return False
        mp = self._mode_catalog.get(sys_profile_name)
        if mp is None:
            return False
        if not mp.nav_yaml:
            return True
        try:
            t0 = time.time()
            self._nav_applier.apply(
                profile_name=mp.name,
                yaml_path=mp.nav_yaml,
                namespace=mp.nav_namespace,
                reload_service=mp.nav_reload_service,
            )
            dt = time.time() - t0
            rospy.loginfo("[TASK] nav profile applied: sys=%s yaml=%s ns=%s reload=%s dt=%.2fs",
                          mp.name, mp.nav_yaml, mp.nav_namespace, mp.nav_reload_service, dt)
            return True
        except Exception as e:
            rospy.logwarn("[TASK] nav profile apply failed: sys=%s err=%s", sys_profile_name, str(e))
            return False

    def _sched_specs_signature_of(self, specs: object) -> str:
        try:
            return json.dumps(specs or [], ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        except Exception:
            return str(specs or "")

    def _reload_scheduler_from_db(self, *, force: bool = False) -> bool:
        if not self.scheduler_enable or self._ops_store is None:
            return False
        now_ts = time.time()
        if (not force) and (not self._sched_reload_enable):
            return False
        if (not force) and ((now_ts - self._sched_last_reload_ts) < self._sched_reload_period_s):
            return False
        self._sched_last_reload_ts = now_ts
        try:
            specs = self._ops_store.list_schedule_specs()
            sig = self._sched_specs_signature_of(specs)
            if (not force) and sig == self._sched_specs_signature:
                return False
            self._scheduler = Scheduler.from_param(specs, defaults=dict(self._sched_defaults))
            for job in self._scheduler.jobs:
                self._scheduler.update_next_fire(job, now_ts, init=True)
            self._sched_specs_signature = sig
            self._sched_attempts.clear()
            rospy.loginfo(
                "[TASK][SCHED] reloaded schedules: jobs=%d force=%s",
                len(self._scheduler.jobs or []),
                str(bool(force)),
            )
            self._log_scheduler_snapshot(now_ts, reason="reload", force=True, full=True)
            return True
        except Exception as e:
            rospy.logerr_throttle(5.0, "[TASK][SCHED] reload failed: %s", str(e))
            return False

    def _sched_attempt_state(self, schedule_id: str) -> Dict[str, float]:
        key = str(schedule_id or "").strip()
        st = self._sched_attempts.get(key)
        if st is None:
            st = {"slot_ts": -1.0, "last_attempt_ts": 0.0, "count": 0.0}
            self._sched_attempts[key] = st
        return st

    def _sched_attempt_allowed(self, schedule_id: str, slot_ts: float, now_ts: float) -> bool:
        st = self._sched_attempt_state(schedule_id)
        if abs(float(st.get("slot_ts", -1.0)) - float(slot_ts or 0.0)) >= 1.0:
            st["slot_ts"] = float(slot_ts or 0.0)
            st["last_attempt_ts"] = 0.0
            st["count"] = 0.0
            return True
        last_attempt_ts = float(st.get("last_attempt_ts", 0.0) or 0.0)
        if (float(now_ts) - last_attempt_ts) >= self._sched_retry_backoff_s:
            return True
        return False

    def _sched_note_attempt(self, schedule_id: str, slot_ts: float, now_ts: float) -> int:
        st = self._sched_attempt_state(schedule_id)
        if abs(float(st.get("slot_ts", -1.0)) - float(slot_ts or 0.0)) >= 1.0:
            st["slot_ts"] = float(slot_ts or 0.0)
            st["count"] = 0.0
        st["last_attempt_ts"] = float(now_ts or 0.0)
        st["count"] = float(st.get("count", 0.0) or 0.0) + 1.0
        return int(st["count"])

    def _sched_clear_attempt(self, schedule_id: str) -> None:
        self._sched_attempts.pop(str(schedule_id or "").strip(), None)

    def _push_task_intent_to_executor(self):
        plan_prof = (self._task_plan_profile_name or "").strip()
        sys_prof = self._resolve_effective_sys_profile(self._task_sys_profile_name)
        mode = self._resolve_effective_clean_mode(sys_prof, self._task_clean_mode)

        self._apply_nav_profile_for_sys(sys_prof)
        self._task_sys_profile_name = sys_prof
        self._task_clean_mode = mode

        if sys_prof:
            self._send_exec_cmd(f"set_sys_profile {sys_prof}")
        if plan_prof:
            self._send_exec_cmd(f"set_plan_profile {plan_prof}")
        if mode:
            self._send_exec_cmd(f"set_mode {mode}")

    def _switch_sys_profile_safe(self, sys_profile_name: str, *, source: str = "CMD"):
        sys_profile_name = self._resolve_effective_sys_profile(sys_profile_name)
        if not sys_profile_name:
            return

        with self._lock:
            self._task_sys_profile_name = sys_profile_name
            exec_state = str(self._executor_state or "")
            zone_id = str(self._active_zone or "")
            run_id = str(self._active_run_id or "")

        self._persist_now()

        mission_running = False
        st = str(exec_state)
        if st.startswith("FOLLOW") or st.startswith("CONNECT"):
            mission_running = True
        elif st in ["RUNNING", "START_REQ", "RESUME_REQ", "LOADING_PLAN", "APPLY_PROFILE", "FINISHING"]:
            mission_running = True

        if mission_running:
            if self._sys_profile_switch_policy == "defer":
                with self._lock:
                    self._pending_sys_profile_name = sys_profile_name
                self._emit(f"SYS_PROFILE_DEFERRED:{sys_profile_name}:policy=defer")
                rospy.logwarn("[TASK] sys_profile switch deferred during RUNNING: sys=%s", sys_profile_name)
                return

            self._emit(f"SYS_PROFILE_SWITCH_BEGIN:{sys_profile_name}:policy=suspend_resume")
            rospy.logwarn("[TASK] sys_profile switch begin (pause -> apply -> resume): sys=%s", sys_profile_name)

            self._send_exec_cmd("pause")

            def _paused_or_idle():
                st2 = self._get_exec_state()
                return st2 in ["PAUSED", "IDLE", "DONE", "FAILED", "CANCELED"] or st2.startswith("PWR_")

            ok = self._wait(self._sys_profile_switch_wait_s, _paused_or_idle, sleep_s=0.05)
            if not ok:
                self._emit(f"SYS_PROFILE_SWITCH_ABORT:{sys_profile_name}:reason=wait_pause_timeout")
                rospy.logerr("[TASK] sys_profile switch abort: wait PAUSED timeout sys=%s", sys_profile_name)
                with self._lock:
                    self._pending_sys_profile_name = sys_profile_name
                return

            nav_ok = self._apply_nav_profile_for_sys(sys_profile_name)
            if not nav_ok:
                self._emit(f"SYS_PROFILE_SWITCH_ABORT:{sys_profile_name}:reason=nav_apply_fail")
                rospy.logerr("[TASK] sys_profile switch abort: nav apply failed sys=%s (executor stays paused)", sys_profile_name)
                return

            if not self._resume_executor_with_current_intent(run_id=run_id, context="sys_profile_switch"):
                self._emit(f"SYS_PROFILE_SWITCH_ABORT:{sys_profile_name}:reason=missing_run_id")
                return
            self._emit(f"SYS_PROFILE_SWITCH_DONE:{sys_profile_name}")
            return

        self._emit(f"SYS_PROFILE_SWITCH_APPLY:{sys_profile_name}:state=not_running")
        self._push_task_intent_to_executor()

    # ------------------- scheduler store wrappers -------------------
    def _sched_store_was_fired(self, schedule_id: str, slot_ts: float) -> bool:
        if self._sched_store is None:
            return False
        try:
            return bool(self._sched_store.was_fired(str(schedule_id), float(slot_ts)))
        except Exception:
            return False

    def _sched_store_mark_fired(self, schedule_id: str, slot_ts: float, fire_wall_ts: float) -> None:
        if self._sched_store is None:
            return
        try:
            self._sched_store.mark_fired(str(schedule_id), float(slot_ts))
            return
        except Exception:
            pass
        try:
            self._sched_store.mark_fired(str(schedule_id), slot_ts=float(slot_ts), fire_ts=float(fire_wall_ts))
            return
        except Exception:
            pass
        try:
            self._sched_store.mark_fired(str(schedule_id), fire_ts=float(fire_wall_ts))
            return
        except Exception:
            pass

    def _sched_store_mark_done(self, schedule_id: str, status: str) -> None:
        if self._sched_store is None:
            return
        try:
            self._sched_store.mark_done(str(schedule_id), status=str(status))
        except Exception:
            pass

    # ------------------- scheduler helpers -------------------
    def _fmt_ts(self, ts: Optional[float]) -> str:
        if ts is None:
            return "None"
        try:
            return datetime.fromtimestamp(float(ts)).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return str(ts)

    def _json_compact(self, obj) -> str:
        try:
            return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        except Exception:
            return str(obj)

    def _sched_schedule_id(self, job) -> str:
        v = getattr(job, "schedule_id", None)
        if v:
            return str(v)
        v = getattr(job, "id", None)
        if v:
            return str(v)
        v = getattr(job, "job_id", None)
        if v:
            return str(v)
        return "<unknown>"

    def _sched_job_id(self, job) -> str:
        v = getattr(job, "job_id", None)
        if v:
            return str(v)
        v = getattr(job, "schedule_id", None)
        if v:
            return str(v)
        v = getattr(job, "id", None)
        if v:
            return str(v)
        return "<unknown>"

    def _sched_task_to_dict(self, job) -> dict:
        task = getattr(job, "task", None)
        if task is None:
            return {}
        if isinstance(task, dict):
            return dict(task)
        if hasattr(task, "to_dict") and callable(getattr(task, "to_dict")):
            try:
                return dict(task.to_dict())
            except Exception:
                pass
        if is_dataclass(task):
            try:
                return asdict(task)
            except Exception:
                pass
        out = {}
        for k in (
            "map_name",
            "zone_id",
            "loops",
            "plan_profile_name",
            "sys_profile_name",
            "clean_mode",
            "return_to_dock_on_finish",
            "repeat_after_full_charge",
        ):
            if hasattr(task, k):
                out[k] = getattr(task, k)
        return out

    def _log_scheduler_snapshot(self, now_ts: float, reason: str, force: bool = False, full: bool = False) -> None:
        if not self.scheduler_enable or self._scheduler is None:
            return
        if not force and (now_ts - self._sched_last_snapshot_ts) < self._sched_debug_snapshot_period_s:
            return

        jobs = [j for j in (self._scheduler.jobs or []) if getattr(j, "enabled", False)]
        if not jobs:
            summary = f"[TASK][SCHED] snapshot({reason}): no enabled jobs"
            if force or summary != self._sched_last_summary:
                rospy.loginfo(summary)
                self._sched_last_summary = summary
            self._sched_last_snapshot_ts = now_ts
            return

        next_job = None
        next_ts = None
        for j in jobs:
            ts = getattr(j, "next_fire_ts", None)
            if ts is None:
                continue
            if next_ts is None or ts < next_ts:
                next_ts, next_job = ts, j

        if next_ts is None:
            summary = f"[TASK][SCHED] snapshot({reason}): enabled_jobs={len(jobs)} next_fire_ts=N/A"
        else:
            summary = (
                f"[TASK][SCHED] snapshot({reason}): enabled_jobs={len(jobs)} "
                f"next={self._fmt_ts(next_ts)}(ts={int(next_ts)}) "
                f"schedule={self._sched_schedule_id(next_job)} job={self._sched_job_id(next_job)}"
            )

        if force or summary != self._sched_last_summary:
            rospy.loginfo(summary)
            self._sched_last_summary = summary
        self._sched_last_snapshot_ts = now_ts

        if full:
            for j in jobs:
                ts = getattr(j, "next_fire_ts", None)
                rospy.loginfo(
                    "[TASK][SCHED] schedule=%s job=%s type=%s next=%s(ts=%s) task=%s",
                    self._sched_schedule_id(j),
                    self._sched_job_id(j),
                    str(getattr(j, "schedule_type", "")),
                    self._fmt_ts(ts),
                    str(ts),
                    self._json_compact(self._sched_task_to_dict(j)),
                )

    def _job_slot_ts_today(self, job: ScheduleJob, now_ts: float) -> Optional[float]:
        """
        Return the scheduled slot timestamp relevant for this occurrence.
        - weekly: only if today's weekday is in job.dow
        - daily : always today
        - once  : returns job.at_ts
        """
        try:
            if str(getattr(job, "schedule_type", "")).lower() == "once":
                at_ts = getattr(job, "at_ts", None)
                if at_ts is None or not math.isfinite(float(at_ts)):
                    return None
                return float(at_ts)

            tz_name = str(getattr(job, "timezone", "") or "").strip()
            try:
                tz = _get_timezone(tz_name, strict=bool(tz_name))
            except ValueError as e:
                rospy.logerr_throttle(
                    30.0,
                    "[TASK][SCHED] skip schedule=%s due to invalid timezone: %s",
                    self._sched_schedule_id(job),
                    str(e),
                )
                return None
            dt = datetime.fromtimestamp(float(now_ts), tz) if tz is not None else datetime.fromtimestamp(float(now_ts))
            cur_date = dt.date()
            start_date = _parse_date_ymd(getattr(job, "start_date", ""))
            end_date = _parse_date_ymd(getattr(job, "end_date", ""))
            if end_date and cur_date > end_date:
                return None

            slot_date = cur_date
            if start_date and slot_date < start_date:
                slot_date = start_date

            stype = str(getattr(job, "schedule_type", "") or "").lower()
            if stype == "weekly":
                dows = list(getattr(job, "dow", []) or [])
                if dows and slot_date.weekday() not in dows:
                    return None

            hh = int(getattr(job, "hh", 0))
            mm = int(getattr(job, "mm", 0))
            if tz is not None:
                slot = _localize_datetime(datetime(slot_date.year, slot_date.month, slot_date.day, hh, mm, 0), tz)
                return float(slot.timestamp())
            slot = datetime(slot_date.year, slot_date.month, slot_date.day, hh, mm, 0)
            return float(slot.timestamp())
        except Exception:
            return None

    # ------------------- job runner -------------------
    def _reset_run_context(self):
        self._active_run_id = ""
        self._active_run_loop_index = 0

    def _reset_job_runner(self):
        self._active_job_id = ""
        self._active_schedule_id = ""
        self._job_loops_total = 0
        self._job_loops_done = 0
        self._task_repeat_after_full_charge = False
        self._dock_supply_exit_inflight = False
        self._reset_run_context()

    def _finish_active_job_keep_template(self, *, status: str):
        jid = str(self._active_job_id or "")
        sid = str(self._active_schedule_id or "")
        if sid:
            self._sched_store_mark_done(sid, status=str(status))
        if jid or sid:
            self._emit(
                f"JOB_DONE:id={jid or 'manual'} schedule={sid or '-'} "
                f"status={status} loops={self._job_loops_done}/{self._job_loops_total}"
            )
        self._active_schedule_id = ""
        self._job_loops_total = 0
        self._job_loops_done = 0
        self._reset_run_context()

    def _start_post_run_repeat_cycle(self) -> bool:
        if not self._repeat_after_charge_enabled():
            return False
        run_id = str(self._active_run_id or "")
        self._emit(
            f"POST_RUN_REPEAT_START:{self._repeat_cycle_context()} "
            f"loops_done={self._job_loops_done}/{self._job_loops_total}"
        )
        rospy.loginfo(
            "[TASK] post-run repeat start %s loops_done=%d/%d",
            self._repeat_cycle_context(),
            int(self._job_loops_done),
            int(self._job_loops_total),
        )
        if run_id:
            self._mission_update_state(run_id, "DONE")
        self._finish_active_job_keep_template(status="DONE")
        self._clear_health_auto_recover(reset_count=True)
        self._mission_state = "IDLE"
        self._phase = "IDLE"
        self._emit("POST_RUN_REPEAT")
        return bool(self._start_dock_sequence(manual=False))

    def _redispatch_current_task_template(self) -> Tuple[bool, str]:
        jid = str(self._active_job_id or "").strip()
        if not jid:
            return False, "repeat task template is missing"
        job = self._resolve_job_record(jid)
        if job is None:
            return False, "repeat task not found"
        self._emit(f"AUTO_REDISPATCH_REQUEST:{self._repeat_cycle_context()}")
        rospy.loginfo("[TASK] auto redispatch request %s", self._repeat_cycle_context())
        ok, msg = self._start_job_record(
            job,
            source="AUTO_REPEAT",
            schedule_id="",
            trigger_source="TASK_REPEAT",
        )
        if ok:
            self._emit(f"AUTO_REDISPATCH_OK:{self._repeat_cycle_context()}")
            rospy.loginfo("[TASK] auto redispatch ok %s", self._repeat_cycle_context())
        else:
            self._emit(f"AUTO_REDISPATCH_FAILED:{msg}")
            rospy.logerr("[TASK] auto redispatch failed %s err=%s", self._repeat_cycle_context(), str(msg))
        return ok, msg

    def _rehydrate_job_context_from_run(self, run_id: str):
        if not run_id or self._mission_store is None:
            return
        try:
            mr = self._mission_store.get_run(run_id)
            if mr is None:
                return
            self._active_job_id = str(mr.job_id or "")
            self._job_loops_total = max(1, int(mr.loops_total or 1))
            self._job_loops_done = max(0, int(mr.loop_index or 1) - 1)
            self._active_run_loop_index = int(mr.loop_index or 1)
            if mr.zone_id:
                self._active_zone = str(mr.zone_id)
            self._task_map_name = str(mr.map_name or "")
            self._task_map_revision_id = str(getattr(mr, "map_revision_id", "") or "")
        except Exception as e:
            rospy.logwarn("[TASK] rehydrate run context failed run=%s err=%s", str(run_id), str(e))

    def _is_terminal_state(self, status: str) -> bool:
        st = str(status or "").upper()
        return st in ["DONE", "FAILED", "CANCELED", "ESTOP"] or st.startswith("ERROR")

    def _finalize_terminal(self, status: str):
        st = str(status or "").upper()
        run_id = str(self._active_run_id or "")
        finish_dock = (st == "DONE") and bool(self._task_return_to_dock_on_finish)

        if run_id:
            self._mission_update_state(run_id, st)

        if self._active_job_id:
            self._finish_active_job(status=st)
        else:
            self._reset_job_runner()

        self._clear_health_auto_recover(reset_count=True)
        self._mission_state = "ESTOP" if st == "ESTOP" else "IDLE"
        self._phase = "IDLE"
        self._dock_supply_exit_inflight = False
        if st == "DONE":
            # Keep DONE as a mission_run terminal record, but return the live
            # runtime state to a launch-ready idle posture.
            self._executor_state = "IDLE"
            self._publish_state("IDLE")
        else:
            self._publish_state(st or "IDLE")
        if finish_dock:
            self._emit("POST_RUN_DOCK")
            self._start_dock_sequence(manual=True)

    def _start_job(self, job: ScheduleJob, *, fire_ts: Optional[float] = None, source: str = "SCHED") -> Tuple[bool, str]:
        if not job or not getattr(job, "enabled", False):
            return False, "schedule job is disabled"
        task = getattr(job, "task", None)
        if task is None:
            return False, "schedule task is empty"

        rospy.loginfo(
            "[TASK][SCHED] start_job schedule=%s job=%s fire_ts=%s(ts=%s) next_fire=%s(ts=%s) task=%s",
            self._sched_schedule_id(job),
            self._sched_job_id(job),
            self._fmt_ts(fire_ts), str(fire_ts),
            self._fmt_ts(getattr(job, "next_fire_ts", None)), str(getattr(job, "next_fire_ts", None)),
            self._json_compact(self._sched_task_to_dict(job)),
        )

        schedule_id = str(getattr(job, "schedule_id", "") or self._sched_schedule_id(job))
        job_record = JobRecord(
            job_id=str(getattr(job, "job_id", "") or self._sched_job_id(job)),
            job_name=str(getattr(job, "job_id", "") or self._sched_job_id(job)),
            map_name=str(getattr(task, "map_name", "") or ""),
            zone_id=str(getattr(task, "zone_id", "") or ""),
            plan_profile_name=str(getattr(task, "plan_profile_name", "") or ""),
            sys_profile_name=str(getattr(task, "sys_profile_name", "") or ""),
            default_clean_mode=str(getattr(task, "clean_mode", "") or ""),
            return_to_dock_on_finish=bool(getattr(task, "return_to_dock_on_finish", False)),
            repeat_after_full_charge=bool(getattr(task, "repeat_after_full_charge", False)),
            default_loops=max(1, int(getattr(task, "loops", 1) or 1)),
            enabled=bool(getattr(job, "enabled", False)),
            priority=0,
        )
        ok, msg = self._start_job_record(
            job_record,
            source=source,
            schedule_id=schedule_id,
            trigger_source="SCHEDULE:%s" % schedule_id,
        )
        if not ok:
            return self._abort_active_job_start(msg, schedule_status="ERROR_MAP_ACTIVATE")
        return True, ""

    def _finish_active_job(self, *, status: str):
        jid = str(self._active_job_id or "")
        sid = str(self._active_schedule_id or "")
        if sid:
            self._sched_store_mark_done(sid, status=str(status))
        if jid or sid:
            self._emit(
                f"JOB_DONE:id={jid or 'manual'} schedule={sid or '-'} "
                f"status={status} loops={self._job_loops_done}/{self._job_loops_total}"
            )
        self._reset_job_runner()

    def _abort_active_job_start(self, reason: str, *, schedule_status: str = "ERROR_MAP_ACTIVATE") -> Tuple[bool, str]:
        detail = str(reason or "start rejected")
        self._emit(f"JOB_ABORT:id={self._active_job_id} reason=map_activate_failed:{detail}")
        rospy.logerr("[TASK] start_job aborted: runtime prepare failed job=%s err=%s", self._active_job_id, detail)
        if self._active_schedule_id:
            self._sched_store_mark_done(self._active_schedule_id, schedule_status)
        self._enter_idle_state(reset_job_runner=True)
        return False, detail

    def _tick_exec_terminal_and_loops(self):
        st = self._get_exec_state()
        if st == self._last_exec_state_seen:
            return
        self._last_exec_state_seen = st

        if self._is_terminal_state(st):
            st_up = str(st or "").upper()

            if st_up == "DONE":
                self._mission_update_state(self._active_run_id, "DONE")
                self._emit(f"EXEC_DONE:zone={self._active_zone} run={self._active_run_id}")

                loops_total = max(1, int(self._job_loops_total or 1))
                loops_done = int(self._job_loops_done or 0) + 1
                self._job_loops_done = loops_done

                if loops_done < loops_total:
                    next_loop = loops_done + 1
                    label = (self._active_job_id or "manual")
                    self._emit(f"LOOP:{label} {next_loop}/{loops_total}")
                    self._publish_state("RUNNING")
                    self._push_task_intent_to_executor()
                    self._mission_create_run(
                        job_id=self._active_job_id,
                        zone_id=self._active_zone,
                        loop_index=next_loop,
                        loops_total=loops_total,
                        trigger_source=(
                            f"SCHEDULE:{self._active_schedule_id}"
                            if str(self._active_schedule_id or "").strip()
                            else ("TASK" if str(self._active_job_id or "").strip() else "MANUAL")
                        ),
                    )
                    if not self._send_exec_start_cmd(zone_id=self._active_zone, run_id=self._active_run_id):
                        self._enter_blocking_fault("ERROR_START_CONTEXT", "missing zone_id during repeat start")
                        return
                    return

                self._emit(f"LOOPS_DONE:status=DONE loops={loops_done}/{loops_total}")
                if self._repeat_after_charge_enabled():
                    if not self._start_post_run_repeat_cycle():
                        self._enter_blocking_fault("ERROR_POST_RUN_REPEAT", "failed to start post-run repeat dock sequence")
                    return
                self._finalize_terminal("DONE")
                return

            self._finalize_terminal(st_up)

    # ------------------- scheduler trigger (WALL-TIME reliable) -------------------
    def _tick_scheduler(self) -> None:
        if not self.scheduler_enable or self._scheduler is None:
            return

        now_ts = time.time()  # WALL TIME
        if self.scheduler_check_hz > 0.0:
            min_dt = 1.0 / max(self.scheduler_check_hz, 1e-6)
            if (now_ts - self._last_sched_tick) < min_dt:
                return
        self._last_sched_tick = now_ts

        if self._is_mission_running():
            return
        if self._phase != "IDLE":
            return
        if str(self._mission_state or "").upper() != "IDLE":
            return
        if self._dock_supply_enable and str(self._dock_supply_state or "").upper() in ["READY_TO_EXIT", "EXIT_BACKING"]:
            return

        try:
            self._reload_scheduler_from_db(force=False)
            jobs = [j for j in (self._scheduler.jobs or []) if getattr(j, "enabled", False)]
            if not jobs:
                self._log_scheduler_snapshot(now_ts, reason="tick", force=False, full=False)
                return

            # refresh next_fire_ts for logs/visibility
            for j in jobs:
                try:
                    self._scheduler.update_next_fire(j, now_ts)
                except TypeError:
                    try:
                        self._scheduler.update_next_fire(j, now_ts, init=False)
                    except Exception:
                        pass
                except Exception:
                    pass

            for job in jobs:
                schedule_id = self._sched_schedule_id(job)
                job_id = self._sched_job_id(job)
                slot_ts = self._job_slot_ts_today(job, now_ts)
                if slot_ts is None:
                    continue

                # in-memory dedup
                if getattr(job, "last_fire_ts", 0.0) and abs(float(getattr(job, "last_fire_ts", 0.0)) - float(slot_ts)) < 1.0:
                    continue

                # persisted dedup
                if self._sched_store_was_fired(schedule_id, slot_ts):
                    self._sched_clear_attempt(schedule_id)
                    continue

                dt = float(now_ts) - float(slot_ts)
                if dt < 0.0:
                    continue  # not reached yet

                due = (dt <= float(self.scheduler_window_s))
                if (not due) and (self.scheduler_catch_up_s > 0.0):
                    due = (dt <= float(self.scheduler_catch_up_s))

                if not due:
                    continue

                if not self._sched_attempt_allowed(schedule_id, slot_ts, now_ts):
                    continue

                rospy.loginfo(
                    "[TASK][SCHED] due schedule=%s job=%s slot=%s(ts=%d) now=%s(ts=%d) dt=%.1fs win=%.1fs catch_up=%.1fs task=%s",
                    schedule_id,
                    job_id,
                    self._fmt_ts(slot_ts), int(slot_ts),
                    self._fmt_ts(now_ts), int(now_ts),
                    dt,
                    float(self.scheduler_window_s),
                    float(self.scheduler_catch_up_s),
                    self._json_compact(self._sched_task_to_dict(job)),
                )

                ok, msg = self._start_job(job, fire_ts=slot_ts, source="SCHED")
                if not ok:
                    attempt_n = self._sched_note_attempt(schedule_id, slot_ts, now_ts)
                    self._sched_store_mark_done(schedule_id, "START_REJECTED:%s" % str(msg or "").strip())
                    rospy.logwarn(
                        "[TASK][SCHED] start rejected schedule=%s job=%s attempt=%d backoff=%.1fs err=%s",
                        schedule_id,
                        job_id,
                        attempt_n,
                        float(self._sched_retry_backoff_s),
                        str(msg or ""),
                    )
                    self._log_scheduler_snapshot(now_ts, reason="after_reject", force=True, full=False)
                    break

                # persist fired
                self._sched_store_mark_fired(schedule_id, slot_ts, fire_wall_ts=now_ts)
                self._sched_clear_attempt(schedule_id)
                if self._ops_store is not None and (bool(getattr(job, "oneshot", False)) or str(getattr(job, "schedule_type", "")).lower() == "once"):
                    try:
                        self._ops_store.set_schedule_enabled(schedule_id, False)
                    except Exception as e:
                        rospy.logwarn("[TASK][SCHED] persist disable once schedule failed schedule=%s err=%s", schedule_id, str(e))

                # mark fired into scheduler (handles oneshot/once disable + next fire)
                try:
                    self._scheduler.mark_fired(job, slot_ts)
                except Exception:
                    pass

                # refresh next_fire after fire
                try:
                    self._scheduler.update_next_fire(job, now_ts)
                except Exception:
                    pass

                rospy.loginfo(
                    "[TASK][SCHED] post-fire schedule=%s job=%s next_fire=%s(ts=%s)",
                    schedule_id,
                    job_id,
                    self._fmt_ts(getattr(job, "next_fire_ts", None)),
                    str(getattr(job, "next_fire_ts", None)),
                )

                self._log_scheduler_snapshot(now_ts, reason="after_fire", force=True, full=True)
                break

            self._log_scheduler_snapshot(now_ts, reason="tick", force=False, full=False)

        except Exception:
            rospy.logerr_throttle(5.0, "[TASK] tick_scheduler failed:\n%s", traceback.format_exc())

    # ------------------- cmd handling -------------------
    def _handle_cmd(self, cmd: str):
        parts = self._split_cmd(cmd)
        if not parts:
            return
        verb = parts[0].lower().strip()

        if verb == "status":
            self._emit("DUMP_STATE")
            return
        if verb == "health":
            self._emit("HEALTH_STATUS")
            return

        if verb == "start":
            if self._task_busy():
                self._emit("CMD_REJECT:START_BUSY")
                return
            kv = self._parse_kv_tokens(parts[1:])
            task_id = (kv.get("task_id") or "").strip() if kv else ""
            if task_id:
                ok, msg = self._start_task_by_id(task_id, source="CMD_TASK")
                if not ok:
                    self._emit(f"CMD_REJECT:START_TASK:{msg}")
                return

            self._emit("CMD_REJECT:START_REQUIRES_TASK_ID")
            rospy.logwarn_throttle(
                30.0,
                "[TASK] manual start requires canonical form: start task_id=<id>",
            )
            return

        if verb == "resume":
            if self._is_mission_running():
                self._emit("CMD_REJECT:RESUME_BUSY")
                return
            kv = self._parse_kv_tokens(parts[1:])

            run_tok = ""
            if kv:
                run_tok = (kv.get("run_id") or "").strip()
            map_name = ""
            if kv:
                map_name = (kv.get("map_name") or "").strip()

            ok, msg = self._resume_current_task(run_id=run_tok, map_name=map_name)
            if not ok:
                self._emit(f"CMD_REJECT:RESUME_MAP:{msg}")
            return

        if verb == "pause":
            self._pause_current_task()
            return

        if verb in ["cancel", "stop"]:
            self._stop_current_task()
            return

        if verb in ["estop", "e-stop", "emergency_stop"]:
            if self._dock_supply_enable and (
                self._is_dock_supply_owner_phase()
                or self._dock_supply_managed_undocking()
                or str(self._dock_supply_state or "").upper() == "READY_TO_EXIT"
            ):
                self._dock_supply_cancel()
            self.nav.cancel_all()
            self._send_exec_cmd("estop")
            self._mission_update_state(self._active_run_id, "ESTOP")
            self._clear_charge_monitor()
            self._reset_dock_retry_state()
            self._dock_nav_started_ts = 0.0
            self._undock_nav_started_ts = 0.0
            self._phase = "IDLE"
            self._mission_state = "ESTOP"

            if self._active_schedule_id:
                self._sched_store_mark_done(self._active_schedule_id, "ESTOP")
            self._clear_health_auto_recover(reset_count=True)
            self._reset_job_runner()
            self._publish_state("ESTOP")
            return

        if verb == "dock":
            ok, msg = self._start_manual_return()
            if not ok:
                self._emit(f"CMD_REJECT:DOCK:{msg}")
                return
            return

        if verb == "undock":
            self._emit("MANUAL_UNDOCK")
            self._start_undock_only()
            return

        if verb == "set_plan_profile":
            parts = cmd.split(maxsplit=1)
            self._task_plan_profile_name = parts[1].strip() if len(parts) >= 2 else ""
            self._emit(f"SET_PLAN_PROFILE:{self._task_plan_profile_name}")
            self._persist_now()
            self._push_task_intent_to_executor()
            return

        if verb == "set_sys_profile":
            parts = cmd.split(maxsplit=1)
            sys_prof = parts[1].strip() if len(parts) >= 2 else ""
            sys_prof = self._resolve_effective_sys_profile(sys_prof)
            self._emit(f"SET_SYS_PROFILE:{sys_prof}")
            self._switch_sys_profile_safe(sys_prof, source="CMD")
            return

        if verb == "set_mode":
            parts = cmd.split(maxsplit=1)
            self._task_clean_mode = parts[1].strip() if len(parts) >= 2 else ""
            self._emit(f"SET_MODE:{self._task_clean_mode}")
            self._persist_now()
            self._push_task_intent_to_executor()
            return

        if verb == "set_return_to_dock_on_finish":
            parts = cmd.split(maxsplit=1)
            value = parts[1].strip() if len(parts) >= 2 else ""
            resolved = self._parse_bool(value, default=None)
            if resolved is None:
                self._emit("CMD_REJECT:SET_RETURN_TO_DOCK_ON_FINISH_BAD_VALUE")
                return
            self._task_return_to_dock_on_finish = bool(resolved)
            self._emit(f"SET_RETURN_TO_DOCK_ON_FINISH:{int(self._task_return_to_dock_on_finish)}")
            self._persist_now()
            return

        if verb == "set_map":
            self._emit("CMD_REJECT:SET_MAP_USE_SLAM_WORKFLOW")
            rospy.logwarn_throttle(
                30.0,
                "[TASK] set_map command removed; switch maps through SLAM workflow instead",
            )
            return

        if verb == "relocalize":
            self._emit("CMD_REJECT:RELOCALIZE_USE_SLAM_WORKFLOW")
            rospy.logwarn_throttle(
                30.0,
                "[TASK] relocalize command removed from task manager; use SLAM workflow relocalize instead",
            )
            return

        rospy.logwarn("[TASK] unknown cmd: %s", cmd)

    # ------------------- auto charge sequence -------------------
    def _start_dock_sequence(self, manual: bool = False, reset_retry_state: bool = True):
        mission_was_running = self._is_mission_running()
        if self._is_mission_running():
            self._send_exec_cmd("suspend")
            self._mission_update_state(self._active_run_id, "SUSPENDED", reason="auto_dock")
            self._mission_state = "PAUSED"

        def _paused_or_idle():
            st = self._get_exec_state()
            return st in ["PAUSED", "IDLE", "DONE", "FAILED", "CANCELED"] or st.startswith("PWR_")

        if mission_was_running and (not self._wait(self.wait_executor_paused_s, _paused_or_idle, sleep_s=0.05)):
            self._enter_charge_fault(
                "ERROR_DOCK_PREP",
                reason="executor_suspend_timeout",
                manual=(not self._active_run_id),
            )
            return False

        if self._dock_sys_profile_name:
            self._emit(f"DOCK_APPLY_SYS_PROFILE:{self._dock_sys_profile_name}")
            self._apply_nav_profile_for_sys(self._dock_sys_profile_name)

        if reset_retry_state:
            self._reset_dock_retry_state()
        if self.dock_two_stage_enable:
            self._phase = "AUTO_DOCKING_STAGE1" if not manual else "MANUAL_DOCKING_STAGE1"
        else:
            self._phase = "AUTO_DOCKING" if not manual else "MANUAL_DOCKING"
        self._dock_nav_started_ts = time.time()
        self._undock_nav_started_ts = 0.0
        self._clear_charge_monitor()
        self._publish_state(self._dock_public_state(manual))
        if self.dock_two_stage_enable:
            x, y, yaw = self.dock_stage1_xyyaw
            self._emit_dock_stage1_goal_event(manual=manual, x=x, y=y, yaw=yaw)
            self.nav.send_goal(self._dock_stage1_pose())
        else:
            x, y, yaw = self.dock_xyyaw
            self._emit_dock_goal_event(manual=manual, x=x, y=y, yaw=yaw)
            self.nav.send_goal(self._dock_pose())
        return True

    # ------------------- dock supply integration -------------------
    def _on_dock_supply_state(self, msg: String):
        should_refresh_public_state = False
        manual = False
        try:
            dock_supply_state = str(msg.data or "").strip().upper() or "IDLE"
        except Exception:
            dock_supply_state = "IDLE"
        with self._lock:
            self._dock_supply_state = dock_supply_state
            self._dock_supply_state_ts = time.time()
            if self._phase == "AUTO_SUPPLY":
                should_refresh_public_state = True
                manual = False
            elif self._phase == "MANUAL_SUPPLY":
                should_refresh_public_state = True
                manual = True
            elif self._phase == "AUTO_UNDOCKING" and self._dock_supply_managed_undocking():
                should_refresh_public_state = True
                manual = False
            elif self._phase == "MANUAL_UNDOCKING" and self._dock_supply_managed_undocking():
                should_refresh_public_state = True
                manual = True
            if dock_supply_state in ["CHARGE_CONFIRMED", "READY_TO_EXIT", "DONE"]:
                self._reset_dock_retry_state()
            if dock_supply_state in ["DONE", "FAILED", "CANCELED"] or dock_supply_state.startswith("FAILED"):
                self._dock_supply_exit_inflight = False
        if should_refresh_public_state and dock_supply_state not in ["DONE", "FAILED", "CANCELED", "IDLE"] and (not dock_supply_state.startswith("FAILED")):
            self._publish_state(self._dock_supply_public_state(manual=manual))

    def _dock_supply_start(self) -> bool:
        if not self._dock_supply_enable or self._dock_supply_start_cli is None:
            return False
        try:
            rospy.wait_for_service(self._dock_supply_start_service, timeout=1.0)
            resp = self._dock_supply_start_cli()
            if getattr(resp, 'success', False):
                self._dock_supply_state = "RUNNING"
                self._dock_supply_state_ts = time.time()
                self._emit("SUPPLY_START")
                return True
            self._emit_supply_command_failure("START", str(getattr(resp, "message", "") or ""))
            return False
        except Exception as e:
            self._emit_supply_command_failure("START", str(e))
            return False

    def _dock_supply_cancel(self):
        if not self._dock_supply_enable or self._dock_supply_cancel_cli is None:
            return
        try:
            rospy.wait_for_service(self._dock_supply_cancel_service, timeout=1.0)
            _ = self._dock_supply_cancel_cli()
            self._emit("SUPPLY_CANCEL")
        except Exception as e:
            rospy.logwarn("[TASK] dock_supply cancel failed: %s", str(e))

    def _begin_charge_monitor(self, soc: Optional[float] = None, fresh: bool = False):
        now = time.time()
        self._charge_started_ts = now
        self._charge_last_soc = float(soc) if (soc is not None and fresh) else None
        self._charge_last_fresh_ts = now if fresh else 0.0

    def _clear_charge_monitor(self):
        self._charge_started_ts = 0.0
        self._charge_last_soc = None
        self._charge_last_fresh_ts = 0.0

    def _manual_sequence_idle_public_state(self) -> str:
        if self._active_run_id and str(self._mission_state or "").upper() == "PAUSED":
            return "PAUSED"
        if str(self._mission_state or "").upper() == "ESTOP":
            return "ESTOP"
        return "IDLE"

    def _enter_charge_fault(self, public_state: str, reason: str, *, manual: bool = False):
        state_name = str(public_state or "ERROR_CHARGE").strip() or "ERROR_CHARGE"
        reason_s = str(reason or "").strip()
        self._emit(f"CHARGE_FAULT:{state_name}:{reason_s}")
        self.nav.cancel_all()
        self._dock_stage2_nav.cancel_all()
        self._clear_charge_monitor()
        self._dock_nav_started_ts = 0.0
        self._undock_nav_started_ts = 0.0
        self._dock_supply_exit_inflight = False
        self._armed = True

        if (not manual) and self._active_run_id:
            self._mission_update_state(self._active_run_id, "PAUSED", reason=reason_s or state_name.lower())
            self._mission_state = "PAUSED"
            self._phase = "PAUSED_AUTO_CHARGE"
            self._publish_state(state_name)
            return

        self._mission_state = "IDLE"
        self._phase = "IDLE"
        self._publish_state(state_name)

    def _transition_to_undocking(self, *, manual: bool):
        self._clear_charge_monitor()
        self._reset_dock_retry_state()
        self._dock_nav_started_ts = 0.0
        self._phase = "MANUAL_UNDOCKING" if manual else "AUTO_UNDOCKING"
        self._undock_nav_started_ts = time.time()
        self._publish_state(self._phase)
        if self._dock_supply_enable and str(self._dock_supply_state or "").upper() == "READY_TO_EXIT":
            if not self._dock_supply_request_exit():
                self._dock_supply_exit_inflight = False
                return False
            self._emit_undock_exit_requested()
            return True

        pose, xyyaw = self._undock_pose()
        self._emit_undock_goal_event(xyyaw[0], xyyaw[1], xyyaw[2])
        self.nav.send_goal(pose)
        return True

    def _dock_sequence_timed_out(self, started_ts: float) -> bool:
        if self.dock_timeout_s <= 1e-3 or started_ts <= 0.0:
            return False
        return (time.time() - float(started_ts)) >= float(self.dock_timeout_s)

    def _start_undock_only(self):
        if self._dock_supply_enable and str(self._dock_supply_state or "").upper() == "READY_TO_EXIT":
            self._clear_charge_monitor()
            self._phase = "MANUAL_UNDOCKING"
            self._undock_nav_started_ts = time.time()
            self._publish_state(self._phase)
            if not self._dock_supply_request_exit():
                self._enter_charge_fault(
                    "ERROR_UNDOCK_PREP",
                    reason=self._supply_reason("exit_start_failed"),
                    manual=True,
                )
                return False
            return True

        if self._dock_supply_enable and self._is_dock_supply_owner_phase():
            self._dock_supply_cancel()

            def _supply_quiesced():
                st = str(self._dock_supply_state or "").upper()
                return st in ["IDLE", "DONE", "FAILED", "CANCELED"]

            if not self._wait(10.0, _supply_quiesced, sleep_s=0.1):
                self._enter_charge_fault(
                    "ERROR_UNDOCK_PREP",
                    reason=self._supply_reason("cancel_timeout"),
                    manual=(not self._active_run_id),
                )
                return False

        self._clear_charge_monitor()
        self._phase = "MANUAL_UNDOCKING"
        self._undock_nav_started_ts = time.time()
        self._publish_state(self._phase)
        pose, xyyaw = self._undock_pose()
        self._emit_undock_goal_event(xyyaw[0], xyyaw[1], xyyaw[2])
        self.nav.send_goal(pose)
        return True

    # ------------------- main loop (WALL-TIME) -------------------
    def _run(self):
        period_s = 0.2  # 5Hz wall-time; NOT blocked by /clock (/use_sim_time)
        while not rospy.is_shutdown():
            with self._lock:
                if self._stop:
                    return

            soc, fresh = self._battery_ok()
            if soc is not None and (not fresh):
                rospy.logwarn_throttle(2.0, "[TASK] battery stale soc=%.3f topic=%s", soc, self.battery_topic)

            # re-arm hysteresis
            if (not self._armed) and _soc_reached_threshold(soc, self.rearm_soc):
                self._armed = True
                self._emit_battery_rearmed_event(soc)

            # Auto trigger dock
            if self._phase == "IDLE" and soc is not None and fresh and self._should_trigger_auto_charge(soc):
                self._armed = False
                self._emit_battery_low_event(soc)
                self._start_dock_sequence(manual=False)

            # Docking done?
            if self._phase in ["AUTO_DOCKING", "MANUAL_DOCKING", "AUTO_DOCKING_STAGE1", "MANUAL_DOCKING_STAGE1", "AUTO_DOCKING_STAGE2", "MANUAL_DOCKING_STAGE2"]:
                if self._dock_sequence_timed_out(self._dock_nav_started_ts):
                    self._dock_nav_client().cancel_all()
                    stage_reason = "predock_stage2_nav_timeout" if self._is_stage2_docking_phase() else "predock_stage1_nav_timeout"
                    self._enter_charge_fault(
                        "ERROR_DOCK_TIMEOUT",
                        reason=stage_reason,
                        manual=(not self._active_run_id),
                    )
                active_nav = self._dock_nav_client()
                if active_nav.done():
                    if active_nav.succeeded():
                        if self._is_stage1_docking_phase():
                            self._emit_dock_stage1_succeeded()
                            self._start_dock_stage2(manual=self._phase.startswith("MANUAL"))
                        else:
                            self._begin_supply_or_charge_after_dock(
                                manual=self._phase.startswith("MANUAL"),
                                soc=soc,
                                fresh=fresh,
                            )
                    else:
                        state = active_nav.get_state()
                        stage_reason = "predock_stage2_nav_failed" if self._is_stage2_docking_phase() else "predock_stage1_nav_failed"
                        self._emit_dock_nav_failed(stage=stage_reason, nav_state=state)
                        self._enter_charge_fault(
                            "ERROR_DOCK",
                            reason=f"{stage_reason}:{state}",
                            manual=(not self._active_run_id),
                        )

            # Fine-dock + supply + charge manager done?
            if self._handle_dock_supply_phase():
                continue

            self._handle_auto_repeat_recovery_phase()

            # Charging -> undock
            self._handle_task_side_charge_phase(soc=soc, fresh=fresh)

            # Undocking -> resume
            self._handle_undocking_phase()

            if self._phase == "IDLE":
                try:
                    self._tick_health_auto_recover()
                except Exception as e:
                    rospy.logerr_throttle(2.0, "[TASK] tick_health_auto_recover failed: %s", str(e))

            # Terminal state / job loops
            if self._phase == "IDLE":
                try:
                    self._tick_exec_terminal_and_loops()
                except Exception as e:
                    rospy.logerr_throttle(2.0, "[TASK] tick_exec_terminal failed: %s", str(e))

            # Scheduler trigger (only when fully idle)
            if self._phase == "IDLE":
                try:
                    self._tick_scheduler()
                except Exception as e:
                    rospy.logerr_throttle(2.0, "[TASK] tick_scheduler failed: %s", str(e))

            self._persist_if_changed()
            time.sleep(period_s)

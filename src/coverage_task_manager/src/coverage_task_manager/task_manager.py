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
from dataclasses import dataclass, asdict, is_dataclass
from typing import Optional, Tuple, Dict, List

import rospy
from std_msgs.msg import String
from std_srvs.srv import Trigger

from diagnostic_updater import Updater
from diagnostic_msgs.msg import DiagnosticStatus
from my_msg_srv.srv import ActivateMapAsset, RelocalizeMapAsset

from coverage_msgs.msg import TaskState as TaskStateMsg
from coverage_msgs.msg import RunProgress as RunProgressMsg
from sensor_msgs.msg import BatteryState
from coverage_planner.plan_store.store import PlanStore

from .mbf_move_base import MBFMoveBase
from .task_state_store import TaskState, TaskStateStore
from .schedule_store import ScheduleRunStore
from .scheduler import Scheduler, ScheduleJob
from .mode_profiles import ModeProfileCatalog
from .nav_profile_applier import NavProfileApplier
from .mission_store import MissionStore

from .health_monitor import HealthMonitor, HealthResult
from .map_identity import get_runtime_map_identity, get_runtime_map_scope


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


class TaskManager:
    """Task layer orchestrator.

    Responsibilities (commercial-grade split):
      - Provide a single place to accept operator/dev commands (String cmd).
      - Monitor battery and, when needed, suspend mission -> dock -> wait charge -> undock -> resume.
      - Forward mission controls to coverage_executor.
      - Scheduler for recurring jobs (WALL-TIME reliable even if /use_sim_time is active).
    """

    def __init__(
        self,
        *,
        db_path: str = "",
        plan_db_path: str = "",
        robot_id: str = "local_robot",
        task_persist_enable: bool = True,
        task_restore_enable: bool = True,
        task_state_max_age_s: float = 7 * 24 * 3600,
        frame_id: str = "map",
        zone_id_default: str = "zone_demo",
        default_plan_profile_name: Optional[str] = None,
        default_profile_name: str = "cover_standard",  # legacy alias for plan profile
        default_sys_profile_name: str = "standard",
        default_clean_mode: str = "",

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
        battery_topic: str = "/battery_state",
        battery_stale_timeout_s: float = 5.0,

        auto_charge_enable: bool = True,
        trigger_when_idle: bool = False,
        low_soc: float = 0.20,
        resume_soc: float = 0.80,
        rearm_soc: float = 0.30,
        dock_xyyaw: Tuple[float, float, float] = (0.0, 0.0, 0.0),
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

        # optional: real-robot dock supply manager integration
        dock_supply_enable: bool = False,
        dock_supply_start_service: str = "/dock_supply/start",
        dock_supply_cancel_service: str = "/dock_supply/cancel",
        dock_supply_state_topic: str = "/dock_supply/state",
        activate_map_service: str = "/map_assets/activate",
        activate_map_timeout_s: float = 10.0,
        relocalize_map_service: str = "/map_assets/relocalize",
        relocalize_map_timeout_s: float = 10.0,
        require_managed_map_asset: bool = False,
        require_runtime_localized_before_start: bool = False,
        runtime_localization_state_param: str = "/cartographer/runtime/localization_state",
        runtime_localization_valid_param: str = "/cartographer/runtime/localization_valid",
    ):
        self.frame_id = str(frame_id)
        self.zone_id_default = str(zone_id_default)
        self._robot_id = str(robot_id or "local_robot").strip() or "local_robot"
        self._active_zone: str = self.zone_id_default

        # Task intent params:
        plan_default = default_plan_profile_name if (default_plan_profile_name is not None) else default_profile_name
        self._task_plan_profile_name: str = str(plan_default or "").strip()
        self._default_sys_profile_name: str = str(default_sys_profile_name or "standard").strip() or "standard"
        self._task_sys_profile_name: str = self._default_sys_profile_name
        self._task_clean_mode: str = str(default_clean_mode or "").strip()
        self._task_map_name: str = ""
        self._task_map_version: str = ""
        self._plan_db_path = os.path.expanduser(str(plan_db_path or "").strip())
        self._plan_store: Optional[PlanStore] = None
        if self._plan_db_path:
            try:
                self._plan_store = PlanStore(self._plan_db_path)
            except Exception as e:
                rospy.logerr("[TASK] map asset DB disabled: plan_db_path=%s err=%s", self._plan_db_path, str(e))
                self._plan_store = None
        self._activate_map_service = str(activate_map_service or "/map_assets/activate").strip() or "/map_assets/activate"
        self._activate_map_timeout_s = max(1.0, float(activate_map_timeout_s))
        self._activate_map_cli = None
        self._relocalize_map_service = (
            str(relocalize_map_service or "/map_assets/relocalize").strip() or "/map_assets/relocalize"
        )
        self._relocalize_map_timeout_s = max(1.0, float(relocalize_map_timeout_s))
        self._relocalize_map_cli = None
        self._require_managed_map_asset = bool(require_managed_map_asset)
        self._require_runtime_localized_before_start = bool(require_runtime_localized_before_start)
        self._runtime_localization_state_param = (
            str(runtime_localization_state_param or "/cartographer/runtime/localization_state").strip()
            or "/cartographer/runtime/localization_state"
        )
        self._runtime_localization_valid_param = (
            str(runtime_localization_valid_param or "/cartographer/runtime/localization_valid").strip()
            or "/cartographer/runtime/localization_valid"
        )

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

        self.battery_topic = str(battery_topic)
        self.battery_stale_timeout_s = float(battery_stale_timeout_s)

        self.auto_charge_enable = bool(auto_charge_enable)
        self.trigger_when_idle = bool(trigger_when_idle)

        self.low_soc = float(low_soc)
        self.resume_soc = float(resume_soc)
        self.rearm_soc = float(rearm_soc)

        self.dock_xyyaw = (float(dock_xyyaw[0]), float(dock_xyyaw[1]), float(dock_xyyaw[2]))
        self.undock_forward_m = float(undock_forward_m)
        self.dock_timeout_s = float(dock_timeout_s)
        self.wait_executor_paused_s = float(wait_executor_paused_s)
        self.charge_timeout_s = float(charge_timeout_s)
        self.charge_battery_stale_timeout_s = float(charge_battery_stale_timeout_s)

        self._lock = threading.Lock()
        self._battery = BatterySnapshot()
        self._executor_state = ""

        self._armed = True

        # Scheduler (WALL-TIME)
        self.scheduler_enable = bool(scheduler_enable)
        self.scheduler_window_s = float(scheduler_window_s)
        self.scheduler_check_hz = max(0.2, float(scheduler_check_hz))
        self.scheduler_catch_up_s = max(0.0, float(scheduler_catch_up_s))

        self._scheduler: Scheduler = Scheduler.from_param(
            schedules,
            defaults={
                "plan_profile_name": self._task_plan_profile_name,
                "sys_profile_name": self._task_sys_profile_name,
                "clean_mode": self._task_clean_mode,
            },
        )
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

        # Dock supply integration (fine docking + station ops + charging)
        self._dock_supply_enable = bool(dock_supply_enable)
        self._dock_supply_state = "IDLE"
        self._dock_supply_start_service = str(dock_supply_start_service)
        self._dock_supply_cancel_service = str(dock_supply_cancel_service)
        self._dock_supply_state_topic = str(dock_supply_state_topic)

        self._dock_supply_start_cli = None
        self._dock_supply_cancel_cli = None
        if self._dock_supply_enable:
            try:
                self._dock_supply_start_cli = rospy.ServiceProxy(self._dock_supply_start_service, Trigger)
                self._dock_supply_cancel_cli = rospy.ServiceProxy(self._dock_supply_cancel_service, Trigger)
                rospy.Subscriber(self._dock_supply_state_topic, String, self._on_dock_supply_state, queue_size=1)
                rospy.loginfo("[TASK] dock_supply enabled: start=%s cancel=%s state_topic=%s",
                              self._dock_supply_start_service, self._dock_supply_cancel_service, self._dock_supply_state_topic)
            except Exception as e:
                rospy.logerr("[TASK] dock_supply init failed, disabled. err=%s", str(e))
                self._dock_supply_enable = False

        try:
            self._activate_map_cli = rospy.ServiceProxy(self._activate_map_service, ActivateMapAsset)
        except Exception as e:
            rospy.logwarn("[TASK] map activation service proxy init failed: service=%s err=%s", self._activate_map_service, str(e))
            self._activate_map_cli = None

        # sys_profile switching policy
        self._sys_profile_switch_policy = str(sys_profile_switch_policy or "defer").strip().lower()
        if self._sys_profile_switch_policy not in ("defer", "suspend_resume"):
            rospy.logwarn("[TASK] invalid sys_profile_switch_policy=%s -> fallback defer", self._sys_profile_switch_policy)
            self._sys_profile_switch_policy = "defer"
        self._sys_profile_switch_wait_s = float(sys_profile_switch_wait_s)
        self._pending_sys_profile_name: Optional[str] = None

        # init persistence store
        if not db_path:
            db_path = "~/.ros/coverage/coverage.db"
        db_path = os.path.expanduser(str(db_path))
        try:
            self._store = TaskStateStore(str(db_path)) if self._persist_enable else None
        except Exception as e:
            rospy.logerr("[TASK] task persistence disabled: cannot open db_path=%s err=%s", str(db_path), str(e))
            self._store = None
            self._persist_enable = False

        # Mission history store
        self._mission_store: Optional[MissionStore] = None
        if self._persist_enable:
            try:
                self._mission_store = MissionStore(str(db_path))
            except Exception as e:
                rospy.logerr("[TASK] mission_store disabled: db_path=%s err=%s", str(db_path), str(e))
                self._mission_store = None

        # Schedule run store
        if self.scheduler_enable:
            try:
                self._sched_store = ScheduleRunStore(str(db_path))
            except Exception as e:
                rospy.logerr("[TASK] scheduler disabled: cannot open schedule store db_path=%s err=%s", str(db_path), str(e))
                self.scheduler_enable = False
                self._sched_store = None

        # pubs/subs
        self._cmd_pub = rospy.Publisher(self.executor_cmd_topic, String, queue_size=10)
        self._state_pub = rospy.Publisher("~state", String, queue_size=1, latch=True)
        self._event_pub = rospy.Publisher("~event", String, queue_size=50)
        self._task_state_pub = rospy.Publisher("task_state", TaskStateMsg, queue_size=10, latch=True)

        rospy.Subscriber(cmd_topic, String, self._on_cmd, queue_size=50)
        rospy.Subscriber(self.executor_state_topic, String, self._on_executor_state, queue_size=50)
        rospy.Subscriber(self.executor_progress_topic, RunProgressMsg, self._on_executor_progress, queue_size=50)
        rospy.Subscriber(self.battery_topic, BatteryState, self._on_battery, queue_size=10)

        self._task_state_timer = rospy.Timer(rospy.Duration(0.5), self._on_task_state_timer)

        self.nav = MBFMoveBase(
            action_name=mbf_move_base_action,
            planner=mbf_planner,
            controller=mbf_controller,
            recovery=mbf_recovery,
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
        self._health_fault_active: bool = False
        self._health_error_code: str = ""
        self._health_error_msg: str = ""

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
            "[TASK] ready. cmd=%s -> executor_cmd=%s battery=%s auto_charge=%s low=%.2f resume=%.2f rearm=%.2f dock=(%.2f,%.2f,%.2f)",
            rospy.resolve_name(cmd_topic),
            self.executor_cmd_topic,
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

        with self._lock:
            prev = str(self._executor_state or "")
            self._executor_state = s

            if s == "PAUSED_RECOVERY" and prev != "PAUSED_RECOVERY":
                self._mission_state = "PAUSED"
                self._phase = "IDLE"
                rid = str(self._active_run_id or "")
                need_mark_paused_recovery = True

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

    def _on_cmd(self, msg: String):
        cmd = (msg.data or "").strip()
        if not cmd:
            return
        self._emit(f"CMD:{cmd}")
        self._handle_cmd(cmd)

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
        self._push_task_intent_to_executor()
        self._mission_update_state(rid, "RUNNING", reason=f"health_auto_resume:{code}")
        self._send_exec_cmd(f"resume run={rid}")

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
                    if (not run_id) or (str(prog.run_id) == run_id):
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
                    mid, mmd5, ok = get_runtime_map_identity()
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

    def _new_run_id(self) -> str:
        return uuid.uuid4().hex

    def _mission_create_run(self, *, job_id: str, zone_id: str, loop_index: int, loops_total: int) -> str:
        run_id = self._new_run_id()
        self._active_run_id = run_id
        self._active_run_loop_index = int(loop_index)
        self._clear_health_auto_recover(reset_count=True)
        self._emit(f"RUN_START:run={run_id} zone={zone_id} job={job_id or 'manual'} loop={int(loop_index)}/{int(loops_total)}")
        if self._mission_store is not None:
            try:
                sys_profile_name = self._resolve_effective_sys_profile(self._task_sys_profile_name)
                mode_profile = self._mode_catalog.get(sys_profile_name)
                controller_name = ""
                actuator_profile_name = sys_profile_name
                if mode_profile is not None:
                    controller_name = str(mode_profile.mbf_controller_name or "").strip()
                    actuator_profile_name = str(mode_profile.actuator_profile_name or actuator_profile_name).strip()
                map_id, map_md5, _ok = get_runtime_map_identity()
                map_name, map_version = get_runtime_map_scope()
                self._mission_store.create_run(
                    run_id=run_id,
                    job_id=str(job_id or ""),
                    map_name=str(self._task_map_name or map_name or ""),
                    map_version=str(self._task_map_version or map_version or ""),
                    zone_id=str(zone_id or ""),
                    plan_profile_name=str(self._task_plan_profile_name or ""),
                    sys_profile_name=str(sys_profile_name or ""),
                    clean_mode=str(self._resolve_effective_clean_mode(sys_profile_name, self._task_clean_mode)),
                    loop_index=int(loop_index),
                    loops_total=int(loops_total),
                    mbf_controller_name=controller_name,
                    actuator_profile_name=actuator_profile_name,
                    trigger_source=(
                        f"SCHEDULE:{self._active_schedule_id}"
                        if str(self._active_schedule_id or "").strip()
                        else ("SCHEDULE" if str(job_id or "").strip() else "MANUAL")
                    ),
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
            except Exception:
                pass
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
            task_map_version=str(self._task_map_version),
            plan_profile_name=str(self._task_plan_profile_name),
            sys_profile_name=str(self._task_sys_profile_name),
            clean_mode=str(self._task_clean_mode),
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
                and st.task_map_version == lp.task_map_version
                and st.plan_profile_name == lp.plan_profile_name
                and st.sys_profile_name == lp.sys_profile_name
                and st.clean_mode == lp.clean_mode
                and st.mission_state == lp.mission_state
                and st.phase == lp.phase
                and st.public_state == lp.public_state
                and st.armed == lp.armed
                and st.active_job_id == lp.active_job_id
                and st.active_schedule_id == lp.active_schedule_id
                and st.job_loops_total == lp.job_loops_total
                and st.job_loops_done == lp.job_loops_done
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
        self._task_map_version = str(getattr(st, "task_map_version", "") or "")

        plan_prof = getattr(st, "plan_profile_name", "") or getattr(st, "profile_name", "")
        if plan_prof:
            self._task_plan_profile_name = str(plan_prof)

        sys_prof = getattr(st, "sys_profile_name", "") or ""
        if sys_prof:
            self._task_sys_profile_name = str(sys_prof)
        self._task_sys_profile_name = self._resolve_effective_sys_profile(self._task_sys_profile_name)

        if getattr(st, "clean_mode", ""):
            self._task_clean_mode = str(st.clean_mode)
        self._task_clean_mode = self._resolve_effective_clean_mode(self._task_sys_profile_name, self._task_clean_mode)

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
        if self._phase in ["AUTO_DOCKING", "MANUAL_DOCKING"]:
            self._emit("RESTORE:DOCKING")
            self._dock_nav_started_ts = time.time()
            try:
                self._send_exec_cmd("suspend")
            except Exception:
                pass
            self.nav.send_goal(self._dock_pose())
        elif self._phase in ["AUTO_SUPPLY", "MANUAL_SUPPLY"]:
            self._emit("RESTORE:SUPPLY")
            if self._dock_supply_enable:
                if self._dock_supply_start():
                    self._publish_state(self._phase)
                else:
                    rospy.logerr("[TASK] restore %s failed: cannot restart dock_supply workflow", self._phase)
                    self._phase = "IDLE"
                    self._publish_state("ERROR_SUPPLY_START")
            else:
                rospy.logerr("[TASK] restore %s failed: dock_supply is disabled", self._phase)
                self._phase = "IDLE"
                self._publish_state("ERROR_SUPPLY_START")
        elif self._phase in ["AUTO_CHARGING", "MANUAL_CHARGING"]:
            self._emit("RESTORE:CHARGING")
            self._begin_charge_monitor()
        elif self._phase in ["AUTO_UNDOCKING", "MANUAL_UNDOCKING"]:
            self._emit("RESTORE:UNDOCKING")
            self._undock_nav_started_ts = time.time()
            pose, _ = self._undock_pose()
            self.nav.send_goal(pose)
        elif self._phase == "AUTO_RESUMING":
            self._emit("RESTORE:RESUME")
            _asset, ok, msg = self._prepare_map_for_run(
                run_id=self._active_run_id,
                require_localized=True,
            )
            if not ok:
                self._phase = "IDLE"
                self._mission_state = "PAUSED"
                self._publish_state("WAIT_RELOCALIZE")
                self._emit("RESTORE:RESUME_BLOCKED:%s" % str(msg or "relocalize required"))
                return
            self._push_task_intent_to_executor()
            if self._active_run_id:
                self._send_exec_cmd(f"resume run={self._active_run_id}")
            else:
                self._send_exec_cmd(f"resume {self._active_zone}")

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

    def _dock_pose(self):
        self._refresh_dock_xyyaw_param()
        x, y, yaw = self.dock_xyyaw
        return _make_pose(self.frame_id, x, y, yaw)

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

    def _get_current_active_map(self) -> Optional[Dict[str, object]]:
        if self._plan_store is None:
            return None
        try:
            return self._plan_store.get_active_map(robot_id=self._robot_id)
        except Exception:
            return None

    def _resolve_task_map_asset(
        self,
        *,
        explicit_map_name: str = "",
        explicit_map_version: str = "",
        run_id: str = "",
        fallback_to_active: bool = True,
    ) -> Optional[Dict[str, object]]:
        map_name = str(explicit_map_name or "").strip()
        map_version = str(explicit_map_version or "").strip()

        if (not map_name) and run_id and self._mission_store is not None:
            try:
                run = self._mission_store.get_run(str(run_id or "").strip())
                if run is not None:
                    map_name = str(run.map_name or "").strip()
                    map_version = str(run.map_version or "").strip()
                    if run.zone_id:
                        self._active_zone = str(run.zone_id)
            except Exception:
                pass

        if not map_name:
            map_name = str(self._task_map_name or "").strip()
            map_version = str(self._task_map_version or "").strip() or map_version

        if self._plan_store is None:
            return None

        asset = None
        try:
            if map_name:
                asset = self._plan_store.resolve_map_asset_version(
                    map_name=map_name,
                    map_version=map_version,
                    robot_id=self._robot_id,
                )
            elif fallback_to_active:
                asset = self._plan_store.get_active_map(robot_id=self._robot_id)
        except Exception:
            asset = None

        if asset:
            self._task_map_name = str(asset.get("map_name") or "")
            self._task_map_version = str(asset.get("map_version") or "")
        return asset

    def _wait_for_runtime_map(self, asset: Dict[str, object], timeout_s: Optional[float] = None) -> bool:
        target_name = str((asset or {}).get("map_name") or "").strip()
        target_version = str((asset or {}).get("map_version") or "").strip()
        target_id = str((asset or {}).get("map_id") or "").strip()
        target_md5 = str((asset or {}).get("map_md5") or "").strip()
        wait_s = max(0.5, float(timeout_s if timeout_s is not None else self._activate_map_timeout_s))

        def _matches() -> bool:
            cur_name, cur_version = get_runtime_map_scope()
            cur_id, cur_md5 = get_runtime_map_identity()
            if target_name and str(cur_name or "").strip() != target_name:
                return False
            if target_version and str(cur_version or "").strip() != target_version:
                return False
            if target_id and str(cur_id or "").strip() != target_id:
                return False
            if target_md5 and str(cur_md5 or "").strip() != target_md5:
                return False
            return True

        return self._wait(wait_s, _matches, sleep_s=0.1)

    def _activate_map_asset(self, asset: Optional[Dict[str, object]], *, timeout_s: Optional[float] = None) -> Tuple[bool, str]:
        if not asset:
            return False, "map asset not resolved"
        if not bool(asset.get("enabled", True)):
            return False, "map asset is disabled"

        if self._wait_for_runtime_map(asset, timeout_s=0.2):
            self._task_map_name = str(asset.get("map_name") or "")
            self._task_map_version = str(asset.get("map_version") or "")
            return True, "already active"

        if self._activate_map_cli is None:
            return False, "map activation service unavailable"
        try:
            rospy.wait_for_service(self._activate_map_service, timeout=1.0)
            resp = self._activate_map_cli(
                robot_id=str(self._robot_id or "local_robot"),
                map_name=str(asset.get("map_name") or ""),
                map_version=str(asset.get("map_version") or ""),
            )
        except Exception as e:
            return False, str(e)

        if not getattr(resp, "success", False):
            return False, str(getattr(resp, "message", "") or "activate map failed")
        if not self._wait_for_runtime_map(asset, timeout_s=timeout_s):
            return False, "runtime map did not converge to requested asset"

        self._task_map_name = str(asset.get("map_name") or "")
        self._task_map_version = str(asset.get("map_version") or "")
        return True, "activated"

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
        state, valid = self._runtime_localization_status()
        if state == "localized" and valid:
            return True, ""
        if state:
            return False, "runtime localization not ready: state=%s; relocalize before start/resume" % state
        return False, "runtime localization not ready; relocalize before start/resume"

    def _manual_relocalize(
        self,
        *,
        x: float,
        y: float,
        yaw: float,
        frame_id: str = "",
        map_name: str = "",
        map_version: str = "",
    ) -> Tuple[bool, str]:
        if self._relocalize_map_cli is None:
            try:
                self._relocalize_map_cli = rospy.ServiceProxy(self._relocalize_map_service, RelocalizeMapAsset)
            except Exception as e:
                return False, str(e)
        try:
            rospy.wait_for_service(self._relocalize_map_service, timeout=float(self._relocalize_map_timeout_s))
            resp = self._relocalize_map_cli(
                robot_id=str(self._robot_id or "local_robot"),
                map_name=str(map_name or self._task_map_name or ""),
                map_version=str(map_version or self._task_map_version or ""),
                frame_id=str(frame_id or self.frame_id or "map"),
                x=float(x),
                y=float(y),
                yaw=float(yaw),
            )
        except Exception as e:
            return False, str(e)
        if not getattr(resp, "success", False):
            return False, str(getattr(resp, "message", "") or "relocalize failed")
        self._task_map_name = str(getattr(resp, "map_name", "") or self._task_map_name or "")
        self._task_map_version = str(getattr(resp, "map_version", "") or self._task_map_version or "")
        self._persist_now()
        return True, str(getattr(resp, "message", "") or "relocalized")

    def _prepare_map_for_run(
        self,
        *,
        explicit_map_name: str = "",
        explicit_map_version: str = "",
        run_id: str = "",
        require_localized: bool = True,
    ) -> Tuple[Optional[Dict[str, object]], bool, str]:
        asset = self._resolve_task_map_asset(
            explicit_map_name=explicit_map_name,
            explicit_map_version=explicit_map_version,
            run_id=run_id,
            fallback_to_active=True,
        )
        if not asset:
            requested_scope = bool(
                str(explicit_map_name or "").strip()
                or str(explicit_map_version or "").strip()
                or str(self._task_map_name or "").strip()
                or str(self._task_map_version or "").strip()
            )
            if self._require_managed_map_asset:
                return None, False, "no map asset available"
            if requested_scope:
                return None, False, "map asset not found"
            return None, True, "legacy_runtime_map"
        ok, msg = self._activate_map_asset(asset)
        if ok and require_localized and self._require_runtime_localized_before_start:
            ok, msg = self._ensure_runtime_localized()
        return asset, bool(ok), str(msg or "")

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

            self._push_task_intent_to_executor()

            if run_id:
                self._send_exec_cmd(f"resume run={run_id}")
            elif zone_id:
                self._send_exec_cmd(f"resume {zone_id}")
            else:
                self._send_exec_cmd("resume")
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
        for k in ("map_name", "map_version", "zone_id", "loops", "plan_profile_name", "sys_profile_name", "clean_mode"):
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

            dt = datetime.fromtimestamp(float(now_ts))  # local time
            stype = str(getattr(job, "schedule_type", "") or "").lower()
            if stype == "weekly":
                dows = list(getattr(job, "dow", []) or [])
                if dows and dt.weekday() not in dows:
                    return None

            hh = int(getattr(job, "hh", 0))
            mm = int(getattr(job, "mm", 0))
            slot = datetime(dt.year, dt.month, dt.day, hh, mm, 0)
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
        self._reset_run_context()

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
            self._task_map_version = str(mr.map_version or "")
        except Exception as e:
            rospy.logwarn("[TASK] rehydrate run context failed run=%s err=%s", str(run_id), str(e))

    def _is_terminal_state(self, status: str) -> bool:
        st = str(status or "").upper()
        return st in ["DONE", "FAILED", "CANCELED", "ESTOP"] or st.startswith("ERROR")

    def _finalize_terminal(self, status: str):
        st = str(status or "").upper()
        run_id = str(self._active_run_id or "")

        if run_id:
            self._mission_update_state(run_id, st)

        if self._active_job_id:
            self._finish_active_job(status=st)
        else:
            self._reset_job_runner()

        self._clear_health_auto_recover(reset_count=True)
        self._mission_state = "ESTOP" if st == "ESTOP" else "IDLE"
        self._phase = "IDLE"
        self._publish_state(st or "IDLE")

    def _start_job(self, job: ScheduleJob, *, fire_ts: Optional[float] = None, source: str = "SCHED"):
        if not job or not getattr(job, "enabled", False):
            return
        task = getattr(job, "task", None)
        if task is None:
            return

        rospy.loginfo(
            "[TASK][SCHED] start_job schedule=%s job=%s fire_ts=%s(ts=%s) next_fire=%s(ts=%s) task=%s",
            self._sched_schedule_id(job),
            self._sched_job_id(job),
            self._fmt_ts(fire_ts), str(fire_ts),
            self._fmt_ts(getattr(job, "next_fire_ts", None)), str(getattr(job, "next_fire_ts", None)),
            self._json_compact(self._sched_task_to_dict(job)),
        )

        self._active_zone = str(getattr(task, "zone_id", "") or self.zone_id_default)
        self._task_plan_profile_name = str(getattr(task, "plan_profile_name", "") or self._task_plan_profile_name)
        self._task_sys_profile_name = self._resolve_effective_sys_profile(
            str(getattr(task, "sys_profile_name", "") or self._task_sys_profile_name)
        )
        self._task_clean_mode = self._resolve_effective_clean_mode(
            self._task_sys_profile_name,
            str(getattr(task, "clean_mode", "") or self._task_clean_mode),
        )
        self._task_map_name = str(getattr(task, "map_name", "") or self._task_map_name)
        self._task_map_version = str(getattr(task, "map_version", "") or self._task_map_version)

        self._active_schedule_id = str(getattr(job, "schedule_id", "") or self._sched_schedule_id(job))
        self._active_job_id = str(getattr(job, "job_id", "") or self._sched_job_id(job))
        self._job_loops_total = int(getattr(task, "loops", 1) or 1)
        self._job_loops_total = max(1, int(self._job_loops_total))
        self._job_loops_done = 0

        asset, ok, msg = self._prepare_map_for_run(
            explicit_map_name=self._task_map_name,
            explicit_map_version=self._task_map_version,
            require_localized=True,
        )
        if not ok:
            self._emit(f"JOB_ABORT:id={self._active_job_id} reason=map_activate_failed:{msg}")
            rospy.logerr("[TASK] start_job aborted: map activation failed job=%s err=%s", self._active_job_id, msg)
            if self._active_schedule_id:
                self._sched_store_mark_done(self._active_schedule_id, "ERROR_MAP_ACTIVATE")
            self._reset_job_runner()
            self._phase = "IDLE"
            self._mission_state = "IDLE"
            self._publish_state("IDLE")
            return

        self._phase = "IDLE"
        self._mission_state = "RUNNING"
        self._emit(
            f"JOB_START:id={self._active_job_id} zone={self._active_zone} map={self._task_map_name or '-'}@{self._task_map_version or '-'} loops={self._job_loops_total} "
            f"plan={self._task_plan_profile_name} sys={self._task_sys_profile_name} "
            f"mode={self._task_clean_mode} source={source} schedule={self._active_schedule_id or '-'}"
        )
        self._publish_state("RUNNING")
        self._push_task_intent_to_executor()

        del asset
        self._mission_create_run(job_id=self._active_job_id, zone_id=self._active_zone, loop_index=1, loops_total=self._job_loops_total)
        self._send_exec_cmd(f"start {self._active_zone} run={self._active_run_id}")

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
                    )
                    self._send_exec_cmd(f"start {self._active_zone} run={self._active_run_id}")
                    return

                self._emit(f"LOOPS_DONE:status=DONE loops={loops_done}/{loops_total}")
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

        try:
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
                    continue

                dt = float(now_ts) - float(slot_ts)
                if dt < 0.0:
                    continue  # not reached yet

                due = (dt <= float(self.scheduler_window_s))
                if (not due) and (self.scheduler_catch_up_s > 0.0):
                    due = (dt <= float(self.scheduler_catch_up_s))

                if not due:
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

                self._start_job(job, fire_ts=slot_ts, source="SCHED")

                # persist fired
                self._sched_store_mark_fired(schedule_id, slot_ts, fire_wall_ts=now_ts)

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
        low = cmd.lower().strip()

        if low in ["dump", "status", "debug", "diag"]:
            self._emit("DUMP_STATE")
            return
        if low in ["health", "health_status"]:
            self._emit("HEALTH_STATUS")
            return

        if low.startswith("start"):
            if self._task_busy():
                self._emit("CMD_REJECT:START_BUSY")
                return
            parts = self._split_cmd(cmd)
            if len(parts) >= 2:
                self._active_zone = parts[1]
            kv = self._parse_kv_tokens(parts[2:])

            loops = 1
            if kv:
                loops = self._parse_int(kv.get("loops") or kv.get("loop") or kv.get("repeat"), default=1) or 1
            loops = max(1, int(loops))

            run_tok = ""
            if kv:
                run_tok = (kv.get("run") or kv.get("run_id") or "").strip()
            map_name = ""
            map_version = ""
            if kv:
                map_name = (kv.get("map") or kv.get("map_name") or "").strip()
                map_version = (kv.get("version") or kv.get("map_version") or "").strip()

            if kv:
                plan_prof = (kv.get("plan_profile") or kv.get("plan") or "").strip()
                sys_prof = (kv.get("sys_profile") or kv.get("sys") or "").strip()
                mode = (kv.get("mode") or kv.get("clean_mode") or "").strip()

                if plan_prof:
                    self._task_plan_profile_name = str(plan_prof)
                if sys_prof:
                    self._task_sys_profile_name = self._resolve_effective_sys_profile(str(sys_prof))
                if mode:
                    self._task_clean_mode = str(mode)

            self._task_sys_profile_name = self._resolve_effective_sys_profile(self._task_sys_profile_name)
            self._task_clean_mode = self._resolve_effective_clean_mode(self._task_sys_profile_name, self._task_clean_mode)
            if map_name:
                self._task_map_name = str(map_name)
                self._task_map_version = str(map_version or "")

            asset, ok, msg = self._prepare_map_for_run(
                explicit_map_name=map_name,
                explicit_map_version=map_version,
                run_id=run_tok,
                require_localized=True,
            )
            if not ok:
                self._emit(f"CMD_REJECT:START_MAP:{msg}")
                rospy.logerr("[TASK] start rejected: map prepare failed zone=%s err=%s", self._active_zone, msg)
                return
            del asset

            self._phase = "IDLE"
            self._mission_state = "RUNNING"

            self._active_job_id = ""
            self._active_schedule_id = ""
            self._job_loops_total = int(loops)
            self._job_loops_done = 0

            if run_tok:
                self._active_run_id = run_tok
                self._active_run_loop_index = 1
                self._emit(f"RUN_START:run={run_tok} zone={self._active_zone} job=manual loop=1/{self._job_loops_total}")
            else:
                self._mission_create_run(job_id="", zone_id=self._active_zone, loop_index=1, loops_total=self._job_loops_total)

            self._publish_state("RUNNING")
            self._push_task_intent_to_executor()
            self._send_exec_cmd(f"start {self._active_zone} run={self._active_run_id}")
            return

        if low.startswith("resume"):
            if self._is_mission_running():
                self._emit("CMD_REJECT:RESUME_BUSY")
                return
            parts = self._split_cmd(cmd)
            kv = self._parse_kv_tokens(parts[1:])

            run_tok = ""
            if kv:
                run_tok = (kv.get("run") or kv.get("run_id") or "").strip()
            map_name = ""
            map_version = ""
            if kv:
                map_name = (kv.get("map") or kv.get("map_name") or "").strip()
                map_version = (kv.get("version") or kv.get("map_version") or "").strip()

            if run_tok:
                self._active_run_id = run_tok

            if self._active_run_id and (not self._active_job_id):
                self._active_schedule_id = ""
                self._rehydrate_job_context_from_run(self._active_run_id)

            asset, ok, msg = self._prepare_map_for_run(
                explicit_map_name=map_name,
                explicit_map_version=map_version,
                run_id=self._active_run_id,
                require_localized=True,
            )
            if not ok:
                self._emit(f"CMD_REJECT:RESUME_MAP:{msg}")
                rospy.logerr("[TASK] resume rejected: map prepare failed run=%s err=%s", self._active_run_id, msg)
                return
            del asset

            self._phase = "IDLE"
            self._mission_state = "RUNNING"

            self._publish_state("RUNNING")
            self._push_task_intent_to_executor()

            if self._active_run_id:
                self._mission_update_state(self._active_run_id, "RUNNING", reason="manual_resume")
                self._send_exec_cmd(f"resume run={self._active_run_id}")
            else:
                self._send_exec_cmd(f"resume {self._active_zone}")
            return

        if low == "pause":
            self._send_exec_cmd("pause")
            self._mission_update_state(self._active_run_id, "PAUSED")
            self._mission_state = "PAUSED"
            self._publish_state("PAUSED")
            return

        if low in ["cancel", "stop"]:
            # if real-robot dock supply is running, request cancel (best-effort)
            if self._dock_supply_enable and self._phase in ["AUTO_SUPPLY", "AUTO_CHARGING", "MANUAL_SUPPLY", "MANUAL_CHARGING"]:
                self._dock_supply_cancel()
            self.nav.cancel_all()
            self._send_exec_cmd("cancel")
            self._mission_update_state(self._active_run_id, "CANCELED")
            self._clear_charge_monitor()
            self._dock_nav_started_ts = 0.0
            self._undock_nav_started_ts = 0.0
            self._phase = "IDLE"
            self._mission_state = "IDLE"

            if self._active_schedule_id:
                self._sched_store_mark_done(self._active_schedule_id, "CANCELED")
            self._clear_health_auto_recover(reset_count=True)
            self._reset_job_runner()
            self._publish_state("IDLE")
            return

        if low in ["estop", "e-stop", "emergency_stop"]:
            if self._dock_supply_enable and self._phase in ["AUTO_SUPPLY", "AUTO_CHARGING", "MANUAL_SUPPLY", "MANUAL_CHARGING"]:
                self._dock_supply_cancel()
            self.nav.cancel_all()
            self._send_exec_cmd("estop")
            self._mission_update_state(self._active_run_id, "ESTOP")
            self._clear_charge_monitor()
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

        if low == "dock":
            if self._phase != "IDLE":
                self._emit("CMD_REJECT:DOCK_BUSY")
                return
            self._emit("MANUAL_DOCK")
            self._start_dock_sequence(manual=True)
            return

        if low == "undock":
            self._emit("MANUAL_UNDOCK")
            self._start_undock_only()
            return

        if low.startswith("set_plan_profile"):
            parts = cmd.split(maxsplit=1)
            self._task_plan_profile_name = parts[1].strip() if len(parts) >= 2 else ""
            self._emit(f"SET_PLAN_PROFILE:{self._task_plan_profile_name}")
            self._persist_now()
            self._push_task_intent_to_executor()
            return

        if low.startswith("set_sys_profile") or low.startswith("set_act_profile"):
            parts = cmd.split(maxsplit=1)
            sys_prof = parts[1].strip() if len(parts) >= 2 else ""
            sys_prof = self._resolve_effective_sys_profile(sys_prof)
            self._emit(f"SET_SYS_PROFILE:{sys_prof}")
            self._switch_sys_profile_safe(sys_prof, source="CMD")
            return

        if low.startswith("set_mode"):
            parts = cmd.split(maxsplit=1)
            self._task_clean_mode = parts[1].strip() if len(parts) >= 2 else ""
            self._emit(f"SET_MODE:{self._task_clean_mode}")
            self._persist_now()
            self._push_task_intent_to_executor()
            return

        if low.startswith("set_map"):
            if str(self._mission_state or "").upper() != "IDLE" or str(self._phase or "").upper() != "IDLE":
                self._emit("CMD_REJECT:SET_MAP_BUSY")
                return
            parts = self._split_cmd(cmd)
            map_name = parts[1].strip() if len(parts) >= 2 else ""
            kv = self._parse_kv_tokens(parts[2:])
            map_version = (kv.get("version") or kv.get("map_version") or "").strip() if kv else ""
            if not map_name:
                self._emit("CMD_REJECT:SET_MAP_MISSING")
                return
            asset, ok, msg = self._prepare_map_for_run(
                explicit_map_name=map_name,
                explicit_map_version=map_version,
                require_localized=False,
            )
            if not ok:
                self._emit(f"CMD_REJECT:SET_MAP:{msg}")
                return
            self._emit(
                "SET_MAP:%s@%s" % (
                    str((asset or {}).get("map_name") or map_name),
                    str((asset or {}).get("map_version") or map_version),
                )
            )
            self._persist_now()
            return

        if low.startswith("relocalize"):
            if str(self._mission_state or "").upper() != "IDLE" or str(self._phase or "").upper() != "IDLE":
                self._emit("CMD_REJECT:RELOCALIZE_BUSY")
                return
            parts = self._split_cmd(cmd)
            kv = self._parse_kv_tokens(parts[1:])
            pose_val = (kv.get("pose") or kv.get("xyyaw") or "").strip() if kv else ""
            if pose_val:
                x, y, yaw = _parse_xyyaw(pose_val, default=(0.0, 0.0, 0.0))
            else:
                if not kv or ("x" not in kv) or ("y" not in kv):
                    self._emit("CMD_REJECT:RELOCALIZE_MISSING_POSE")
                    return
                x = float(kv.get("x") or 0.0)
                y = float(kv.get("y") or 0.0)
                yaw = float(kv.get("yaw") or 0.0)
            frame_id = (kv.get("frame") or kv.get("frame_id") or self.frame_id).strip() if kv else self.frame_id
            map_name = (kv.get("map") or kv.get("map_name") or "").strip() if kv else ""
            map_version = (kv.get("version") or kv.get("map_version") or "").strip() if kv else ""
            ok, msg = self._manual_relocalize(
                x=x,
                y=y,
                yaw=yaw,
                frame_id=frame_id,
                map_name=map_name,
                map_version=map_version,
            )
            if not ok:
                self._emit(f"CMD_REJECT:RELOCALIZE:{msg}")
                return
            self._emit(
                "RELOCALIZE:%s@%s pose=[%.3f,%.3f,%.3f]"
                % (self._task_map_name or "-", self._task_map_version or "-", float(x), float(y), float(yaw))
            )
            return

        rospy.logwarn("[TASK] unknown cmd: %s", cmd)

    # ------------------- auto charge sequence -------------------
    def _start_dock_sequence(self, manual: bool = False):
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

        self._phase = "AUTO_DOCKING" if not manual else "MANUAL_DOCKING"
        self._dock_nav_started_ts = time.time()
        self._undock_nav_started_ts = 0.0
        self._clear_charge_monitor()
        self._publish_state(self._phase)
        self._emit(f"DOCKING:manual={manual}")
        self.nav.send_goal(self._dock_pose())
        return True

    # ------------------- dock supply integration -------------------
    def _on_dock_supply_state(self, msg: String):
        try:
            self._dock_supply_state = str(msg.data or "").strip().upper() or "IDLE"
        except Exception:
            self._dock_supply_state = "IDLE"

    def _dock_supply_start(self) -> bool:
        if not self._dock_supply_enable or self._dock_supply_start_cli is None:
            return False
        try:
            rospy.wait_for_service(self._dock_supply_start_service, timeout=1.0)
            resp = self._dock_supply_start_cli()
            if getattr(resp, 'success', False):
                self._dock_supply_state = "RUNNING"
                self._emit("SUPPLY_START")
                return True
            self._emit(f"SUPPLY_START_FAILED:{getattr(resp,'message','')}")
            return False
        except Exception as e:
            self._emit(f"SUPPLY_START_EX:{e}")
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
        self._clear_charge_monitor()
        self._dock_nav_started_ts = 0.0
        self._undock_nav_started_ts = 0.0
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
        pose, xyyaw = self._undock_pose()
        self._clear_charge_monitor()
        self._dock_nav_started_ts = 0.0
        self._phase = "MANUAL_UNDOCKING" if manual else "AUTO_UNDOCKING"
        self._undock_nav_started_ts = time.time()
        self._publish_state(self._phase)
        self._emit(f"UNDOCKING:goal=({xyyaw[0]:.2f},{xyyaw[1]:.2f},{xyyaw[2]:.2f})")
        self.nav.send_goal(pose)

    def _dock_sequence_timed_out(self, started_ts: float) -> bool:
        if self.dock_timeout_s <= 1e-3 or started_ts <= 0.0:
            return False
        return (time.time() - float(started_ts)) >= float(self.dock_timeout_s)

    def _start_undock_only(self):
        if self._dock_supply_enable and self._phase in ["AUTO_SUPPLY", "AUTO_CHARGING", "MANUAL_SUPPLY", "MANUAL_CHARGING"]:
            self._dock_supply_cancel()

            def _supply_quiesced():
                st = str(self._dock_supply_state or "").upper()
                return st in ["IDLE", "DONE", "FAILED", "CANCELED"]

            if not self._wait(10.0, _supply_quiesced, sleep_s=0.1):
                self._enter_charge_fault("ERROR_UNDOCK_PREP", reason="dock_supply_cancel_timeout", manual=(not self._active_run_id))
                return False

        self._clear_charge_monitor()
        self._phase = "MANUAL_UNDOCKING"
        self._undock_nav_started_ts = time.time()
        self._publish_state(self._phase)
        pose, xyyaw = self._undock_pose()
        self._emit(f"UNDOCKING:goal=({xyyaw[0]:.2f},{xyyaw[1]:.2f},{xyyaw[2]:.2f})")
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
            if soc is not None and (not self._armed) and soc >= self.rearm_soc:
                self._armed = True
                self._emit(f"REARM:soc={soc:.3f}")

            # Auto trigger dock
            if self._phase == "IDLE" and soc is not None and fresh and self._should_trigger_auto_charge(soc):
                self._armed = False
                self._emit(f"LOW_BATTERY:soc={soc:.3f}")
                self._start_dock_sequence(manual=False)

            # Docking done?
            if self._phase in ["AUTO_DOCKING", "MANUAL_DOCKING"]:
                if self._dock_sequence_timed_out(self._dock_nav_started_ts):
                    self.nav.cancel_all()
                    self._enter_charge_fault(
                        "ERROR_DOCK_TIMEOUT",
                        reason="predock_nav_timeout",
                        manual=(not self._active_run_id),
                    )
                if self.nav.done():
                    if self.nav.succeeded():
                        self._emit("DOCK_OK")
                        if self._phase == "AUTO_DOCKING":
                            # Real-robot integration: after reaching pre-dock pose, start fine dock + station ops + charging.
                            if self._dock_supply_enable:
                                if self._dock_supply_start():
                                    self._phase = "AUTO_SUPPLY"
                                    self._publish_state("AUTO_SUPPLY")
                                else:
                                    self._enter_charge_fault("ERROR_SUPPLY_START", reason="dock_supply_start_failed", manual=False)
                            else:
                                self._phase = "AUTO_CHARGING"
                                self._begin_charge_monitor(soc=soc, fresh=fresh)
                                self._publish_state("AUTO_CHARGING")
                        else:
                            if self._dock_supply_enable:
                                if self._dock_supply_start():
                                    self._phase = "MANUAL_SUPPLY"
                                    self._publish_state("MANUAL_SUPPLY")
                                else:
                                    self._enter_charge_fault("ERROR_SUPPLY_START", reason="manual_dock_supply_start_failed", manual=(not self._active_run_id))
                            else:
                                self._phase = "MANUAL_CHARGING"
                                self._begin_charge_monitor(soc=soc, fresh=fresh)
                                self._publish_state("MANUAL_CHARGING")
                    else:
                        self._emit(f"DOCK_FAILED:mbf_state={self.nav.get_state()}")
                        self._enter_charge_fault(
                            "ERROR_DOCK",
                            reason=f"predock_nav_failed:{self.nav.get_state()}",
                            manual=(not self._active_run_id),
                        )

            # Fine-dock + supply + charge manager done?
            if self._phase in ["AUTO_SUPPLY", "MANUAL_SUPPLY"]:
                st = str(self._dock_supply_state or "").upper()
                if st == "DONE":
                    self._emit("SUPPLY_DONE")
                    if self._phase == "AUTO_SUPPLY":
                        self._transition_to_undocking(manual=False)
                    else:
                        self._phase = "IDLE"
                        self._publish_state(self._manual_sequence_idle_public_state())
                elif st in ["FAILED", "CANCELED"]:
                    self._emit(f"SUPPLY_{st}")
                    self._enter_charge_fault(
                        f"ERROR_SUPPLY_{st}",
                        reason=f"dock_supply_{st.lower()}",
                        manual=(not self._active_run_id),
                    )

            # Charging -> undock
            if self._phase in ["AUTO_CHARGING", "MANUAL_CHARGING"]:
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
                elif (not fresh) and self.charge_battery_stale_timeout_s > 1e-3 and self._charge_started_ts > 0.0 and (
                    now - (self._charge_last_fresh_ts if self._charge_last_fresh_ts > 0.0 else self._charge_started_ts)
                ) >= self.charge_battery_stale_timeout_s:
                    self._enter_charge_fault(
                        "ERROR_CHARGE_BATTERY_STALE",
                        reason="battery_stale_during_charge",
                        manual=(not self._active_run_id),
                    )
                elif soc is not None and fresh and soc >= self.resume_soc:
                    self._emit(f"CHARGED:soc={soc:.3f}")
                    if self._phase == "AUTO_CHARGING":
                        self._transition_to_undocking(manual=False)
                    else:
                        self._clear_charge_monitor()
                        self._phase = "IDLE"
                        self._publish_state(self._manual_sequence_idle_public_state())

            # Undocking -> resume
            if self._phase in ["AUTO_UNDOCKING", "MANUAL_UNDOCKING"]:
                if self._dock_sequence_timed_out(self._undock_nav_started_ts):
                    self.nav.cancel_all()
                    self._enter_charge_fault(
                        "ERROR_UNDOCK_TIMEOUT",
                        reason="undock_nav_timeout",
                        manual=(not self._active_run_id),
                    )
                if self.nav.done():
                    if self.nav.succeeded():
                        self._emit("UNDOCK_OK")
                        self._undock_nav_started_ts = 0.0
                        if self._phase == "AUTO_UNDOCKING":
                            self._phase = "AUTO_RESUMING"
                            self._publish_state("AUTO_RESUMING")
                            self._push_task_intent_to_executor()
                            if self._active_run_id:
                                self._send_exec_cmd(f"resume run={self._active_run_id}")
                            else:
                                self._send_exec_cmd(f"resume {self._active_zone}")
                        else:
                            self._phase = "IDLE"
                            self._publish_state(self._manual_sequence_idle_public_state())
                    else:
                        self._emit(f"UNDOCK_FAILED:mbf_state={self.nav.get_state()}")
                        self._enter_charge_fault(
                            "ERROR_UNDOCK",
                            reason=f"undock_nav_failed:{self.nav.get_state()}",
                            manual=(not self._active_run_id),
                        )

            # Resuming -> back to running monitor
            if self._phase == "AUTO_RESUMING":
                st = self._get_exec_state()
                if st.startswith("CONNECT") or st.startswith("FOLLOW"):
                    self._emit(f"RESUMED:{st}")
                    self._phase = "IDLE"
                    self._publish_state("RUNNING")

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

# -*- coding: utf-8 -*-
import math
import time
import threading
import uuid
from typing import Optional, Tuple, List, Dict, Any

import rospy
import tf2_ros
from std_msgs.msg import String, Bool, Float32

from nav_msgs.msg import Odometry

from coverage_msgs.msg import RunProgress

from .plan_loader import PlanLoader, LoadedPlan, LoadedBlock
from .mbf_adapter import MBFAdapter
from .cleaning_actuator import CleaningActuator
from .cleaning_subsystem import CleaningSubsystem
from .progress import build_arclen, project_along_segments, index_from_s
from .sys_profile_catalog import SysProfileCatalog

# 工程化 import（统一用 ros_utils / debug_bus）
from .debug_bus import DebugBus
from .ros_utils import make_pose, make_path

from .map_identity import (
    ensure_map_identity,
    get_runtime_map_identity,
    get_runtime_map_revision_id,
    get_runtime_map_scope,
)
from .run_event_store import RunEventStore
from coverage_planner.ops_store.store import MissionCheckpointRecord, OperationsStore


def _wrap_pi(a: float) -> float:
    """wrap to [-pi, pi]"""
    while a > math.pi:
        a -= 2.0 * math.pi
    while a < -math.pi:
        a += 2.0 * math.pi
    return a


def _yaw_from_quat(qx: float, qy: float, qz: float, qw: float) -> float:
    """Quaternion -> yaw"""
    siny_cosp = 2.0 * (qw * qz + qx * qy)
    cosy_cosp = 1.0 - 2.0 * (qy * qy + qz * qz)
    return math.atan2(siny_cosp, cosy_cosp)


class ExecutorFSM:
    """
    Executor FSM (threaded), with:
    - pause/cancel/estop async
    - DB checkpoint
    - in-block resume with "distance threshold + bounded projection progress"
    - water_off trigger by remaining arc-length
    - DebugBus publish debug/path & cut_path + robot/proj pose
    - CANCEL short-brake to guarantee base stops (cmd_vel=0 burst)
    - 清洁策略：zone_begin/CONNECT(通行段) 默认不清洁；仅进入 FOLLOW 时开启清洁
      （block 间 CONNECT 是否保持清洁由 keep_cleaning_on_during_connect 控制，默认 True）
    """

    def __init__(
        self,
        plan_db_path: str,
        ops_db_path: str,
        mbf: MBFAdapter,
        actuator: CleaningActuator,
        frame_id: str = "map",
        base_frame: str = "base_footprint",
        zone_id_default: str = "zone_demo",
        default_plan_profile_name: str = "cover_standard",
        default_sys_profile_name: str = "standard",
        default_clean_mode: str = "scrub",
        mode_profiles: Optional[dict] = None,

        # runtime defaults injected from launch params
        vacuum_delay_s: float = 1.5,
        keep_cleaning_on_during_connect: bool = True,

        checkpoint_hz: float = 1.0,
        pause_hold_hz: float = 10.0,
        hard_stop_s: float = 0.6,
        connect_skip_dist: float = 0.35,

        resume_backtrack_m: float = 0.5,
        resume_accept_dist: float = 1.0,
        resume_finish_thresh_m: float = 0.3,
        water_off_distance: float = 2.0,

        # 防止“向前跳进度”导致漏扫（仅在 need_connect=False 时允许小幅前进）
        resume_forward_allow_m: float = 0.3,
        # yaw 约束（弧段方向与机器人朝向差太大时，强制 need_connect）
        resume_max_yaw_err_rad: float = 1.2,

        # 调试：bounded 投影失败时，再做一次全局投影并打印（不参与决策）
        resume_debug_free_project: bool = True,

        # 执行一致性检查：zone/map 更新后，旧 plan/旧 checkpoint 不允许继续执行
        strict_zone_version_check: bool = True,

        # 执行一致性检查：map identity (map_id/map_md5)
        strict_map_check: bool = True,
        strict_resume_map_check: bool = True,
        auto_map_identity_enable: bool = True,
        map_topic: str = "/map",
        map_identity_timeout_s: float = 2.0,

        # AI inspection spot cleaning (optional)
        ai_spot_enable: bool = False,
        ai_signal_topic: str = "/ai/dirt_detected",
        ai_signal_use_string: bool = False,
        ai_spot_duration_s: float = 120.0,
        ai_spot_mode: str = "scrub",

        # actuator interlock (speed-based safety net)
        odom_topic: str = "/odom",
        interlock_enable: bool = True,
        interlock_max_lin_mps: float = 1.0,
        interlock_max_ang_rps: float = 2.0,
        interlock_mask_cleaning: bool = True,
        interlock_mask_water: bool = True,
        interlock_mask_vacuum: bool = False,

        # optional interface for controller speed limiting (publish scale)
        speed_limit_scale_enable: bool = True,
        speed_limit_scale_when_actuating: float = 1.0,

        connect_retry_max: int = 2,
        follow_retry_max: int = 2,
        retry_wait_s: float = 2.0,
        follow_retry_reconnect_on_fail: bool = True,
        retry_pause_recovery_on_exhausted: bool = True,
        retry_clear_costmaps: bool = False,

        # keep future unknown kwargs from crashing
        **_ignored,
    ):
        self.plan_db_path = str(plan_db_path or ops_db_path or "").strip()
        self.ops_db_path = str(ops_db_path or plan_db_path or "").strip()
        self.loader = PlanLoader(self.plan_db_path)
        self._ops_store = OperationsStore(self.ops_db_path)
        self.mbf = mbf

        self.frame_id = frame_id
        self.base_frame = base_frame
        self.zone_id_default = zone_id_default
        self._sys_profile_catalog = SysProfileCatalog(mode_profiles or {})
        self._default_plan_profile_name = str(default_plan_profile_name or "cover_standard").strip() or "cover_standard"
        self._default_sys_profile_name = str(default_sys_profile_name or "standard").strip() or "standard"
        self._default_clean_mode = str(default_clean_mode or "scrub").strip() or "scrub"

        self.vacuum_delay_s = float(vacuum_delay_s)
        self.keep_cleaning_on_during_connect = bool(keep_cleaning_on_during_connect)

        self.checkpoint_hz = float(checkpoint_hz)
        self.pause_hold_hz = float(pause_hold_hz)
        self.hard_stop_s = float(hard_stop_s)
        self.connect_skip_dist = float(connect_skip_dist)

        self.resume_backtrack_m = float(resume_backtrack_m)
        self.resume_accept_dist = float(resume_accept_dist)
        self.resume_finish_thresh_m = float(resume_finish_thresh_m)
        self.resume_forward_allow_m = float(resume_forward_allow_m)
        self.resume_max_yaw_err_rad = float(resume_max_yaw_err_rad)
        self.resume_debug_free_project = bool(resume_debug_free_project)

        # consistency policies
        self.strict_zone_version_check = bool(strict_zone_version_check)

        # map identity policies
        self.strict_map_check = bool(strict_map_check)
        self.strict_resume_map_check = bool(strict_resume_map_check)
        self.auto_map_identity_enable = bool(auto_map_identity_enable)
        self.map_topic = str(map_topic or "/map")
        self.map_identity_timeout_s = float(map_identity_timeout_s)

        self.water_off_distance = float(water_off_distance)

        self._lock = threading.RLock()
        self._zone_id: Optional[str] = (zone_id_default or "").strip() or None
        # Each execution instance (one loop) should use a unique run_id.
        # Task layer supplies run_id; direct operator debugging may omit it.
        self._run_id: str = ""

        # Task intent overrides (from Task layer / TaskManager / String cmd)
        # - plan_profile: choose which plan to execute (lookup in DB)
        # - sys_profile:  choose which actuator/controller params to use (heavy/standard/eco)
        self._plan_profile_override: str = self._default_plan_profile_name
        self._sys_profile_override: str = self._default_sys_profile_name
        self._clean_mode: str = self._default_clean_mode

        self._pause_req = False
        self._cancel_req = False
        self._estop = False
        self._running_thread: Optional[threading.Thread] = None

        self._state = "INIT"
        self._state_pub = rospy.Publisher("~state", String, queue_size=1, latch=True)
        self._event_pub = rospy.Publisher("~event", String, queue_size=50)
        # run progress (for Task/UI)
        self._progress_pub = rospy.Publisher("~run_progress", RunProgress, queue_size=10, latch=True)

        # Prime one heartbeat immediately so tools can see at least one latched message.
        try:
            self._publish_minimal_progress(reason='boot')
        except Exception:
            pass
        rospy.Subscriber("~cmd", String, self._on_cmd, queue_size=50)

        # DebugBus 实例化（publisher/subscriber 后）
        self.dbg = DebugBus()

        # Run event store (shares sqlite with TaskManager). Best-effort.
        self._evt_store: Optional[RunEventStore] = None
        try:
            self._evt_store = RunEventStore(self.ops_db_path)
        except Exception as e:
            try:
                rospy.logwarn("[EXEC] run_events store disabled: %s", str(e))
            except Exception:
                pass

        self._tf_buf = tf2_ros.Buffer(cache_time=rospy.Duration(10.0))
        self._tf_listener = tf2_ros.TransformListener(self._tf_buf)

        self.act = actuator
        self.clean = CleaningSubsystem(actuator)

        # --- motion / interlock ---
        self._odom_topic = str(odom_topic or '/odom')
        self._v_mps = 0.0
        self._w_rps = 0.0
        self._vw_ts = 0.0
        self._interlock_prev = False

        self._speed_limit_scale_enable = bool(speed_limit_scale_enable)
        self._speed_limit_scale_when_actuating = float(speed_limit_scale_when_actuating)
        self.connect_retry_max = max(0, int(connect_retry_max))
        self.follow_retry_max = max(0, int(follow_retry_max))
        self.retry_wait_s = max(0.0, float(retry_wait_s))
        self.follow_retry_reconnect_on_fail = bool(follow_retry_reconnect_on_fail)
        self.retry_pause_recovery_on_exhausted = bool(retry_pause_recovery_on_exhausted)
        self.retry_clear_costmaps = bool(retry_clear_costmaps)

        # configure cleaning interlock (last safety net; thresholds are meant to be conservative)
        try:
            self.clean.configure_interlock(
                enable=bool(interlock_enable),
                max_lin_mps=float(interlock_max_lin_mps),
                max_ang_rps=float(interlock_max_ang_rps),
                mask_cleaning=bool(interlock_mask_cleaning),
                mask_water=bool(interlock_mask_water),
                mask_vacuum=bool(interlock_mask_vacuum),
            )
        except Exception:
            pass

        # publish interlock + speed-limit scale for integration with controller (optional)
        self._interlock_pub = rospy.Publisher('~interlock_active', Bool, queue_size=10, latch=True)
        self._speed_scale_pub = rospy.Publisher('~speed_limit_scale', Float32, queue_size=10, latch=True)

        # odom subscription (for speed-based interlock & debug)
        try:
            rospy.Subscriber(self._odom_topic, Odometry, self._on_odom, queue_size=50)
        except Exception as e:
            rospy.logwarn('[EXEC] odom subscribe failed: %s', str(e))

        self._pause_timer: Optional[rospy.Timer] = None

        # AI spot-clean override (used for inspection/patrol mode)
        self.ai_spot_enable = bool(ai_spot_enable)
        self.ai_signal_topic = str(ai_signal_topic or "")
        self.ai_signal_use_string = bool(ai_signal_use_string)
        self.ai_spot_duration_s = max(0.0, float(ai_spot_duration_s))
        self.ai_spot_mode = str(ai_spot_mode or "scrub")
        self._ai_deadline_ts: float = 0.0
        self._ai_active: bool = False
        self._ai_timer: Optional[rospy.Timer] = None

        # short brake timer for CANCEL / transient stop (do not keep holding)
        self._brake_timer: Optional[rospy.Timer] = None
        self._brake_deadline_ts: float = 0.0

        # plan/runtime
        self._plan: Optional[LoadedPlan] = None
        self._runtime_map_name: str = ""
        self._runtime_map_revision_id: str = ""
        self._runtime_map_id: str = ""
        self._runtime_map_md5: str = ""

        # error summary for UI/ops (best-effort)
        self._error_code: str = ""
        self._error_msg: str = ""
        self._exec_index = 0
        self._block_id = -1
        self._path_index = 0
        self._path_s = 0.0
        self._block_cut_idx0 = 0
        self._water_off_latched = False

        # progress helpers
        self._plan_total_len_m: float = 0.0
        self._prefix_before_m: List[float] = []  # prefix_before_m[exec_index]
        self._exec_block_ids: List[int] = []
        self._block_cut_s0: float = 0.0  # cut start s in full block
        self._block_cut_idx0: int = 0   # cut start idx in full block path
        self._current_block_len_m: float = 0.0

        # --- run_progress publish (robust heartbeat) ---
        # rospy.Timer relies on ROS time; under /use_sim_time it can stall in some environments.
        # Publish with wall-time thread by default for robustness (also survives /clock pause).
        self._progress_pub_hz = float(rospy.get_param("~run_progress_hz", 2.0))
        self._progress_use_wall_time = bool(rospy.get_param("~run_progress_use_wall_time", True))

        self._progress_stop_evt = threading.Event()
        self._progress_thread: Optional[threading.Thread] = None
        self._progress_timer: Optional[rospy.Timer] = None

        if self._progress_use_wall_time:
            self._progress_thread = threading.Thread(target=self._progress_wall_loop, daemon=True)
            self._progress_thread.start()
            try:
                rospy.loginfo("[EXEC] run_progress: wall_time=True hz=%.2f", self._progress_pub_hz)
            except Exception:
                pass
        else:
            period = 1.0 / max(0.2, float(self._progress_pub_hz))
            self._progress_timer = rospy.Timer(rospy.Duration(period), self._on_progress_timer)
            try:
                rospy.loginfo("[EXEC] run_progress: wall_time=False hz=%.2f", self._progress_pub_hz)
            except Exception:
                pass


        # start/resume 的首段 CONNECT 视作“通行段”，避免走廊/非作业区弄湿
        self._force_transit_for_next_connect = True

        # Subscribe AI dirt signal
        if self.ai_spot_enable and self.ai_signal_topic:
            try:
                if self.ai_signal_use_string:
                    rospy.Subscriber(self.ai_signal_topic, String, self._on_ai_signal_string, queue_size=50)
                else:
                    rospy.Subscriber(self.ai_signal_topic, Bool, self._on_ai_signal_bool, queue_size=50)
                self._ai_timer = rospy.Timer(rospy.Duration(0.2), self._on_ai_timer)
                self._emit(f"AI_SPOT_READY:enable=1 topic={self.ai_signal_topic} type={'string' if self.ai_signal_use_string else 'bool'} dur={self.ai_spot_duration_s:.1f} mode={self.ai_spot_mode}")
            except Exception as e:
                self._emit(f"AI_SPOT_ERROR:{str(e)}")
                self.ai_spot_enable = False

        self._ensure_checkpoint_schema()

    # ---------- progress publish ----------
    def _rebuild_progress_index(self, plan: LoadedPlan):
        """Build prefix sums for overall progress percentage."""
        # plan_loader.load_for_zone() 已经把 plan.blocks 按 exec_order 重排好了。
        # 这里不能再按 plan.exec_order 二次索引，否则 block 顺序会错。
        exec_blocks: List[LoadedBlock] = list(plan.blocks or [])

        prefix_before: List[float] = []
        acc = 0.0
        block_ids: List[int] = []
        for blk in exec_blocks:
            prefix_before.append(float(acc))
            block_ids.append(int(getattr(blk, "block_id", -1)))
            acc += float(getattr(blk, "length_m", 0.0) or 0.0)

        total = float(getattr(plan, "total_length_m", 0.0) or 0.0)
        if total <= 1e-6:
            total = float(acc)
        self._plan_total_len_m = float(total)
        self._prefix_before_m = prefix_before
        self._exec_block_ids = block_ids

    def _compute_overall_progress(self) -> Tuple[float, float]:
        """Return (progress_0_1, progress_pct)."""
        total = float(self._plan_total_len_m or 0.0)
        if total <= 1e-6:
            return 0.0, 0.0

        ei = int(self._exec_index or 0)
        if ei < 0:
            ei = 0
        if ei >= len(self._prefix_before_m):
            # finished
            return 1.0, 100.0

        prefix = float(self._prefix_before_m[ei])
        blk_len = float(self._current_block_len_m or 0.0)
        # current progress inside block (full-block coordinate)
        cur_in_blk = float(self._block_cut_s0 or 0.0) + float(self._path_s or 0.0)
        if blk_len > 1e-6:
            cur_in_blk = max(0.0, min(cur_in_blk, blk_len))
        s_done = prefix + cur_in_blk
        p = max(0.0, min(s_done / total, 1.0))
        return float(p), float(p * 100.0)


    def _publish_minimal_progress(self, reason: str = "", lock_busy: bool = False):
        """Publish a minimal RunProgress heartbeat without taking self._lock.

        Used as a fallback when the main progress builder cannot acquire the lock
        (e.g. long blocking operations inside command handlers).
        """
        try:
            msg = RunProgress()
            msg.run_id = str(getattr(self, "_run_id", "") or "")
            msg.zone_id = str(getattr(self, "_zone_id", "") or "")
            msg.plan_id = ""
            msg.state = str(getattr(self, "_state", "") or "")
            msg.plan_profile = str(getattr(getattr(self, "_plan", None), "plan_profile_name", "") or "")
            msg.sys_profile = str((getattr(self, "_sys_profile_override", "") or "") or (getattr(self, "_plan_profile_override", "") or "") or (msg.plan_profile or ""))
            msg.mode = str(getattr(self, "_clean_mode", "") or "")
            msg.map_id = str(getattr(self, "_runtime_map_id", "") or "")
            msg.map_md5 = str(getattr(self, "_runtime_map_md5", "") or "")
            msg.error_code = str(getattr(self, "_error_code", "") or "")
            msg.error_msg = str(getattr(self, "_error_msg", "") or "")
            msg.interlock_active = False
            msg.interlock_reason = ""
            msg.v_mps = 0.0
            msg.w_rps = 0.0
            msg.exec_index = max(0, int(getattr(self, "_exec_index", 0) or 0))
            msg.block_id = int(getattr(self, "_block_id", -1) if getattr(self, "_block_id", None) is not None else -1)
            msg.path_index = max(0, int(getattr(self, "_path_index", 0) or 0))
            msg.path_s = float(getattr(self, "_path_s", 0.0) or 0.0)
            msg.block_length_m = float(getattr(self, "_current_block_len_m", 0.0) or 0.0)
            msg.total_length_m = float(getattr(self, "_plan_total_len_m", 0.0) or 0.0)
            p01 = float(getattr(self, "_last_progress_0_1", 0.0) or 0.0)
            msg.progress_0_1 = p01
            msg.progress_pct = float(p01 * 100.0)
            msg.stamp = rospy.Time.now()
            self._progress_pub.publish(msg)
            if lock_busy:
                rospy.logwarn_throttle(2.0, "[EXEC] run_progress lock busy; published minimal heartbeat. reason=%s", str(reason))
            else:
                rospy.logdebug_throttle(10.0, "[EXEC] run_progress minimal heartbeat. reason=%s", str(reason))
        except Exception as e:
            try:
                rospy.logerr_throttle(2.0, "[EXEC] publish minimal run_progress failed: %s", str(e))
            except Exception:
                pass

    def _progress_wall_loop(self):
        """Wall-time heartbeat loop for /run_progress (does not depend on /clock)."""
        period = 1.0 / max(0.2, float(getattr(self, "_progress_pub_hz", 2.0)))
        # small initial delay to let pubs/subs connect
        try:
            time.sleep(0.05)
        except Exception:
            pass
        while (not rospy.is_shutdown()) and (not self._progress_stop_evt.is_set()):
            try:
                self._on_progress_timer(None)
                # lightweight alive log (debug)
                try:
                    rospy.logdebug_throttle(10.0, "[EXEC] run_progress loop alive")
                except Exception:
                    pass
            except Exception as e:
                try:
                    rospy.logerr_throttle(2.0, "[EXEC] progress_wall_loop exception: %s", str(e))
                except Exception:
                    pass
            try:
                time.sleep(period)
            except Exception:
                pass


    def _on_progress_timer(self, _evt):
        """Publish RunProgress heartbeat.

        IMPORTANT: Do NOT call any function that also takes self._lock while holding the lock
        (threading.Lock is not re-entrant). This callback must remain deadlock-free.
        """
        try:
            if not self._lock.acquire(timeout=0.05):
                self._publish_minimal_progress(reason='timer', lock_busy=True)
                return
            try:
                plan = self._plan
                run_id = str(self._run_id or "")
                zone_id = str(self._zone_id or "")
                plan_id = str(plan.plan_id) if plan else ""
                state = str(self._state or "")

                # intent snapshot (avoid nested lock calls)
                plan_prof = str(getattr(plan, "plan_profile_name", "") or "") if plan else ""
                sys_ovr = str(self._sys_profile_override or "")
                plan_ovr = str(self._plan_profile_override or "")
                mode_ovr = str(self._clean_mode or "")

                # map identity + error summary
                map_id = str(self._runtime_map_id or "")
                map_md5 = str(self._runtime_map_md5 or "")
                err_code = str(self._error_code or "")
                err_msg = str(self._error_msg or "")
                exec_index = int(self._exec_index or 0)
                block_id = int(self._block_id if self._block_id is not None else -1)
                path_index = int(self._path_index or 0)
                path_s = float(self._path_s or 0.0)
                blk_len = float(self._current_block_len_m or 0.0)
                total_len = float(self._plan_total_len_m or (plan.total_length_m if plan else 0.0) or 0.0)

                # compute overall progress while holding the lock for consistency
                p01 = 0.0
                ppct = 0.0
                total = float(self._plan_total_len_m or 0.0)
                if total > 1e-6 and self._prefix_before_m:
                    ei = int(self._exec_index or 0)
                    if ei < 0:
                        ei = 0
                    if ei >= len(self._prefix_before_m):
                        p01 = 1.0
                        ppct = 100.0
                    else:
                        prefix = float(self._prefix_before_m[ei])
                        blk_len2 = float(self._current_block_len_m or 0.0)
                        cur_in_blk = float(self._block_cut_s0 or 0.0) + float(self._path_s or 0.0)
                        if blk_len2 > 1e-6:
                            cur_in_blk = max(0.0, min(cur_in_blk, blk_len2))
                        s_done = prefix + cur_in_blk
                        p = max(0.0, min(s_done / total, 1.0))
                        p01 = float(p)
                        ppct = float(p * 100.0)

                # store last progress snapshot for minimal heartbeat fallback
                self._last_progress_0_1 = float(p01)
            finally:
                try:
                    self._lock.release()
                except Exception:
                    pass

            # Decide effective sys_profile/mode WITHOUT taking the lock (use snapshots)
            sys_prof = (sys_ovr or plan_ovr or plan_prof or "").strip()
            mode = (mode_ovr or "").strip()

            msg = RunProgress()
            msg.run_id = run_id
            msg.zone_id = zone_id
            msg.plan_id = plan_id
            msg.state = state
            msg.plan_profile = plan_prof
            msg.sys_profile = sys_prof
            msg.mode = mode
            msg.map_id = map_id
            msg.map_md5 = map_md5
            msg.error_code = err_code
            msg.error_msg = err_msg
            try:
                il_active, il_reason, v_mps, w_rps = self.clean.get_interlock()
            except Exception:
                il_active, il_reason, v_mps, w_rps = (False, '', 0.0, 0.0)
            msg.interlock_active = bool(il_active)
            msg.interlock_reason = str(il_reason)
            msg.v_mps = float(v_mps)
            msg.w_rps = float(w_rps)
            msg.exec_index = max(0, int(exec_index))
            msg.block_id = int(block_id)
            msg.path_index = max(0, int(path_index))
            msg.path_s = float(path_s)
            msg.block_length_m = float(blk_len)
            msg.total_length_m = float(total_len)
            msg.progress_0_1 = float(p01)
            msg.progress_pct = float(ppct)
            msg.stamp = rospy.Time.now()
            self._progress_pub.publish(msg)

            # helper pubs (optional)
            try:
                self._interlock_pub.publish(Bool(data=bool(msg.interlock_active)))
            except Exception:
                pass
            if bool(self._speed_limit_scale_enable):
                try:
                    des_clean, des_vac, des_water, _lat = self.clean.get_desired_channels()
                    req_any = bool(des_clean or des_vac or des_water)
                    scale = float(self._speed_limit_scale_when_actuating) if req_any else 1.0
                    self._speed_scale_pub.publish(Float32(data=float(scale)))
                except Exception:
                    pass
        except Exception as e:
            try:
                rospy.logerr_throttle(2.0, "[EXEC] progress_timer exception: %s", str(e))
            except Exception:
                pass


            # helper pubs (optional)
            try:
                self._interlock_pub.publish(Bool(data=bool(msg.interlock_active)))
            except Exception:
                pass
            if bool(self._speed_limit_scale_enable):
                # interface hook: controller can subscribe and scale speed accordingly
                # default=1.0 (no effect); set via rosparam when integrating with MPPI
                try:
                    des_clean, des_vac, des_water, _lat = self.clean.get_desired_channels()
                    req_any = bool(des_clean or des_vac or des_water)
                    scale = float(self._speed_limit_scale_when_actuating) if req_any else 1.0
                    self._speed_scale_pub.publish(Float32(data=float(scale)))
                except Exception:
                    pass
        except Exception as e:
            try:
                rospy.logerr_throttle(2.0, "[EXEC] progress_timer exception: %s", str(e))
            except Exception:
                pass

    def enqueue_cmd(self, cmd: str):
        """Allow external manager (e.g. AutoChargeManager) to inject commands."""
        cmd = (cmd or "").strip()
        if not cmd:
            return
        self._apply_cmd(cmd)

    def publish_state_external(self, s: str):
        """Allow external manager to publish state on the same topic."""
        self._publish_state(s)

    def request_transit_cleaning_off(self):
        """Force all cleaning outputs off (used for transit like docking/undocking)."""
        # also clear any AI spot override
        try:
            self._clear_ai_spot("transit")
        except Exception:
            pass
        try:
            self.clean.enter_transit_off(water_off_latched=self._water_off_latched)
        except Exception:
            pass

    def set_force_resume_transit_once(self, enabled: bool):
        """Force next CONNECT to be treated as transit (no cleaning) until first FOLLOW."""
        with self._lock:
            self._force_transit_for_next_connect = bool(enabled)

    def shutdown(self):
        """
        Called by node shutdown hook:
        - cancel mbf actions
        - stop cleaning
        - hard stop base
        """
        try:
            rospy.logerr("[EXEC] shutdown() -> cancel_all + fail_stop + hard_stop")
        except Exception:
            pass
        # stop progress publish loop/timer (important for clean shutdown)
        try:
            if getattr(self, "_progress_timer", None) is not None:
                self._progress_timer.shutdown()
        except Exception:
            pass
        try:
            if getattr(self, "_progress_stop_evt", None) is not None:
                self._progress_stop_evt.set()
            t = getattr(self, "_progress_thread", None)
            if t is not None:
                t.join(timeout=0.2)
        except Exception:
            pass



        try:
            self.mbf.cancel_all()
        except Exception:
            pass

        # make sure base stops (burst)
        try:
            self._start_brake(max(0.5, self.hard_stop_s))
        except Exception:
            pass

        try:
            self.clean.fail_stop()
        except Exception:
            pass
        try:
            self.clean.shutdown()
        except Exception:
            pass

        try:
            self._clear_ai_spot("shutdown")
        except Exception:
            pass

        if self._ai_timer is not None:
            try:
                self._ai_timer.shutdown()
            except Exception:
                pass

        try:
            self._stop_pause_hold()
        except Exception:
            pass

        try:
            self._publish_state("IDLE")
        except Exception:
            pass

    # ---------- state ----------
    def get_state(self) -> str:
        with self._lock:
            return str(self._state)

    def get_active_run_id(self) -> str:
        with self._lock:
            return str(self._run_id or "")

    def is_running(self) -> bool:
        with self._lock:
            t = self._running_thread
            return bool(t and t.is_alive())

    def set_pause_hold(self, enabled: bool):
        if enabled:
            self._start_pause_hold()
        else:
            self._stop_pause_hold()

    # ---------- ROS IO ----------
    def _publish_state(self, s: str):
        s = str(s)
        old = ""
        rid = ""
        with self._lock:
            old = str(self._state or "")
            self._state = s
            rid = str(self._run_id or "")
            # keep an error summary for UI/ops
            if s == "IDLE":
                self._error_code = ""
                self._error_msg = ""
            elif s.startswith("ERROR"):
                self._error_code = s
        self._state_pub.publish(String(data=s))
        # Persist state transitions as run_events (best-effort)
        if rid and s != old:
            try:
                self._add_run_event(level="INFO", code="STATE", msg=s, data={"from": old})
            except Exception:
                pass
        try:
            if s != old:
                rospy.loginfo("[EXEC] state %s -> %s (run=%s)", old or "?", s, rid or "")
        except Exception:
            pass

    def _set_error(self, *, code: str, msg: str, data: Optional[Dict[str, Any]] = None):
        """Set error summary fields and write a run_event (best-effort)."""
        code = str(code or "").strip() or "ERROR"
        msg = str(msg or "").strip()
        with self._lock:
            self._error_code = code
            self._error_msg = msg
        try:
            self._add_run_event(level="ERROR", code=code, msg=msg, data=data or {})
        except Exception:
            pass

    def _emit(self, s: str):
        self._event_pub.publish(String(data=str(s)))

    def _add_run_event(self, *, level: str, code: str, msg: str, data: Optional[Dict[str, Any]] = None):
        """Write one run event into sqlite (best-effort)."""
        rid = ""
        with self._lock:
            rid = str(self._run_id or "")
        if not rid:
            return
        if self._evt_store is None:
            return
        try:
            self._evt_store.add_event(run_id=rid, source="EXEC", level=str(level), code=str(code), msg=str(msg), data=data)
        except Exception:
            pass

    def _on_cmd(self, msg: String):
        cmd = (msg.data or "").strip()
        if cmd:
            self._apply_cmd(cmd)


    def _on_odom(self, msg: Odometry):
        try:
            v = float(getattr(msg.twist.twist.linear, 'x', 0.0))
            w = float(getattr(msg.twist.twist.angular, 'z', 0.0))
            ts = time.time()
            with self._lock:
                self._v_mps = v
                self._w_rps = w
                self._vw_ts = ts
            # feed to cleaning subsystem (may trigger immediate safety masking)
            try:
                self.clean.update_motion(v, w, ts=ts)
            except Exception:
                pass
            # detect interlock edge and log for debugging
            try:
                active, reason, _, _ = self.clean.get_interlock()
                if bool(active) != bool(self._interlock_prev):
                    self._interlock_prev = bool(active)
                    if active:
                        self._emit(f'INTERLOCK_ON:{reason}')
                        self._add_run_event(level='WARN', code='INTERLOCK_ON', msg=str(reason), data={'v_mps': v, 'w_rps': w})
                        rospy.logwarn('[EXEC][INTERLOCK] ON %s', str(reason))
                    else:
                        self._emit('INTERLOCK_OFF')
                        self._add_run_event(level='INFO', code='INTERLOCK_OFF', msg='off', data={'v_mps': v, 'w_rps': w})
                        rospy.loginfo('[EXEC][INTERLOCK] OFF')
            except Exception:
                pass
        except Exception:
            pass

    # ---------- cmd handling ----------

    # ---------- task intent (profile/mode) ----------
    def _parse_kv_tokens(self, tokens) -> Dict[str, str]:
        """Parse tokens like ['plan_profile=xxx', 'mode=yyy'] into a dict."""
        kv: Dict[str, str] = {}
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

    def _apply_intent_from_kv(self, kv: Dict[str, str]):
        """Update plan/sys profile and clean_mode intent from canonical kv tokens."""
        if not kv:
            return

        plan_prof = kv.get("plan_profile")
        sys_prof = kv.get("sys_profile")
        mode = kv.get("mode")

        with self._lock:
            if plan_prof is not None and str(plan_prof).strip():
                self._plan_profile_override = str(plan_prof).strip()

            if sys_prof is not None and str(sys_prof).strip():
                self._sys_profile_override = str(sys_prof).strip()

            if mode is not None:
                self._clean_mode = str(mode).strip()

        # Push intent into cleaning subsystem (does not start cleaning)
        try:
            prof_to_apply = ""
            if sys_prof is not None and str(sys_prof).strip():
                prof_to_apply = str(sys_prof).strip()

            if prof_to_apply:
                _sys_name, _controller_name, actuator_profile_name, default_mode = self._resolve_sys_profile_spec(prof_to_apply)
                self.clean.set_profile(actuator_profile_name)
                if mode is None and default_mode:
                    self.clean.set_mode(default_mode)
            if mode is not None:
                self.clean.set_mode(str(mode).strip())
        except Exception:
            pass

    def _effective_sys_profile_for_actuator(self, plan_profile_from_db: str) -> str:
        """Decide which sys_profile to apply.

        plan_profile_from_db is ignored for controller/actuator decisions in the
        refactored system; plan_profile is reserved for path selection only.
        """
        with self._lock:
            sys_prof = (self._sys_profile_override or "").strip()
        return str(sys_prof or self._default_sys_profile_name or "standard")

    def _resolve_sys_profile_spec(self, sys_profile_name: str) -> Tuple[str, str, str, str]:
        sys_name = str(sys_profile_name or "").strip() or str(self._default_sys_profile_name or "standard")
        spec = self._sys_profile_catalog.get(sys_name)

        controller_name = ""
        actuator_profile_name = sys_name
        default_mode = ""
        if spec is not None:
            controller_name = str(spec.mbf_controller_name or "").strip()
            actuator_profile_name = str(spec.actuator_profile_name or actuator_profile_name).strip()
            default_mode = str(spec.default_clean_mode or "").strip()
        return sys_name, controller_name, actuator_profile_name, default_mode

    def _apply_sys_profile_runtime(self, sys_profile_name: str, *, water_off_latched: bool):
        sys_name, controller_name, actuator_profile_name, default_mode = self._resolve_sys_profile_spec(sys_profile_name)

        if controller_name:
            try:
                self.mbf.set_controller_name(controller_name)
            except Exception:
                self.mbf.controller = str(controller_name)

        with self._lock:
            if not self._clean_mode and default_mode:
                self._clean_mode = str(default_mode)
            mode_now = str(self._clean_mode or default_mode or self._default_clean_mode or "scrub").strip()

        self.clean.set_mode(mode_now)
        self._apply_profile_only_and_stop(actuator_profile_name, water_off_latched=water_off_latched)

    def _effective_mode_for_execution(self) -> str:
        with self._lock:
            mode = str(self._clean_mode or "").strip()
            sys_prof = str(self._sys_profile_override or self._default_sys_profile_name or "").strip()
        if mode:
            return mode
        _sys_name, _controller_name, _actuator_profile_name, default_mode = self._resolve_sys_profile_spec(sys_prof)
        return str(default_mode or self._default_clean_mode or "scrub")
# ---------- AI spot override (inspection/patrol) ----------
    def _is_inspection_mode(self, mode: str) -> bool:
        m = (mode or "").strip().lower()
        return m in [
            "inspect",
            "inspection",
            "patrol",
            "eco_inspect",
            "巡检",
        ]

    def _ai_spot_allowed_now(self) -> bool:
        if not self.ai_spot_enable:
            return False
        if self._estop:
            return False
        mode = self._effective_mode_for_execution()
        if not self._is_inspection_mode(mode):
            return False
        st = self.get_state()
        # don't allow while explicitly paused/idle
        if st in ["PAUSED", "IDLE", "DONE", "FAILED", "CANCELED"]:
            return False
        return True

    def _on_ai_signal_bool(self, msg: Bool):
        try:
            if not bool(getattr(msg, "data", False)):
                return
        except Exception:
            return
        self._trigger_ai_spot()

    def _on_ai_signal_string(self, msg: String):
        s = (msg.data or "").strip().lower()
        if not s:
            return
        if s in ["0", "false", "off", "none"]:
            return
        self._trigger_ai_spot()

    def _trigger_ai_spot(self):
        if not self._ai_spot_allowed_now():
            return
        now = time.time()
        deadline = now + float(self.ai_spot_duration_s)
        with self._lock:
            if deadline > self._ai_deadline_ts:
                self._ai_deadline_ts = deadline
            was_active = bool(self._ai_active)
            self._ai_active = True

        if was_active:
            self._emit(f"AI_SPOT_EXTEND:until={self._ai_deadline_ts:.1f}")
        else:
            self._emit(f"AI_SPOT_ON:until={self._ai_deadline_ts:.1f}")

        try:
            # Respect end-of-mission water latch (safety)
            self.clean.enter_ai_spot(self.ai_spot_mode, water_off_latched=self._water_off_latched)
        except Exception:
            pass

    def _clear_ai_spot(self, reason: str = ""):
        with self._lock:
            was = bool(self._ai_active)
            self._ai_active = False
            self._ai_deadline_ts = 0.0
        if was:
            self._emit(f"AI_SPOT_OFF:{reason}")
        # always restore to safe state
        try:
            self.clean.enter_transit_off(water_off_latched=self._water_off_latched)
        except Exception:
            pass

    def _restore_cleaning_after_ai(self):
        # restore based on current executor state
        st = self.get_state()
        try:
            if st.startswith("FOLLOW"):
                self.clean.enter_follow(water_off_latched=self._water_off_latched)
            else:
                self.clean.enter_transit_off(water_off_latched=self._water_off_latched)
        except Exception:
            pass

    def _on_ai_timer(self, _evt):
        if not self.ai_spot_enable:
            return
        now = time.time()
        with self._lock:
            active = bool(self._ai_active)
            deadline = float(self._ai_deadline_ts)
        if (not active) or (deadline <= 0.0):
            return
        if now <= deadline:
            return
        # expired
        with self._lock:
            self._ai_active = False
            self._ai_deadline_ts = 0.0
        self._emit("AI_SPOT_OFF:timeout")
        self._restore_cleaning_after_ai()


    def _apply_cmd(self, cmd: str):
        parts = cmd.split()
        if not parts:
            return
        verb = parts[0].strip().lower()
        self._emit(f"CMD:{cmd}")
        try:
            self._add_run_event(level="INFO", code="CMD", msg=str(cmd), data={})
        except Exception:
            pass

        # Task intent update (used by Task layer; keep String cmd interface)

        # Explicit: plan_profile only (affects plan selection; does NOT touch actuator)
        if verb == "set_plan_profile":
            parts = cmd.split(maxsplit=1)
            plan_prof = parts[1].strip() if len(parts) >= 2 else ""
            with self._lock:
                self._plan_profile_override = plan_prof
            rospy.loginfo("[EXEC] set_plan_profile=%s", plan_prof)
            self._publish_state("PLAN_PROFILE_SET")
            return

        # Explicit: sys_profile only (selects MBF controller + actuator profile)
        if verb == "set_sys_profile":
            parts = cmd.split(maxsplit=1)
            sys_prof = parts[1].strip() if len(parts) >= 2 else ""
            with self._lock:
                self._sys_profile_override = sys_prof
            rospy.loginfo("[EXEC] set_sys_profile=%s", sys_prof)
            try:
                self._apply_sys_profile_runtime(sys_prof, water_off_latched=self._water_off_latched)
            except Exception:
                pass
            self._publish_state("SYS_PROFILE_SET")
            return

        if verb == "set_mode":
            parts = cmd.split(maxsplit=1)
            mode = parts[1].strip() if len(parts) >= 2 else ""
            with self._lock:
                self._clean_mode = mode
            rospy.loginfo("[EXEC] set_mode=%s", mode)
            try:
                self.clean.set_mode(mode)
            except Exception:
                pass
            self._publish_state("MODE_SET")
            return


        if verb == "start":
            kv = self._parse_kv_tokens(parts[1:])
            zone = (kv.get("zone_id") or "").strip() if kv else ""
            # canonical: start zone_id=... plan_profile=... sys_profile=... mode=... run_id=...
            if not zone:
                rospy.logwarn("[EXEC] start requires zone_id")
                return
            self._apply_intent_from_kv(kv)
            req_run = (kv.get("run_id") or "").strip() if kv else ""
            with self._lock:
                self._zone_id = zone
                # always create/use a run_id for this execution
                self._run_id = req_run or uuid.uuid4().hex
                self._pause_req = False
                self._cancel_req = False
            rospy.loginfo("[EXEC] start zone_id=%s", self._zone_id)
            self._publish_state("START_REQ")
            self._start_thread(mode="start")
            return

        if verb == "resume":
            kv = self._parse_kv_tokens(parts[1:])
            zone = (kv.get("zone_id") or "").strip() if kv else ""
            # canonical: resume run_id=... [zone_id=...] [plan_profile=...] [sys_profile=...] [mode=...]
            self._apply_intent_from_kv(kv)
            req_run = (kv.get("run_id") or "").strip() if kv else ""
            if not req_run:
                rospy.logwarn("[EXEC] resume requires run_id")
                return
            with self._lock:
                if zone:
                    self._zone_id = zone
                self._run_id = req_run
                self._pause_req = False
                self._cancel_req = False
            rospy.logwarn("[EXEC] RESUME zone_id=%s", self._zone_id)
            self._stop_pause_hold()
            self._publish_state("RESUME_REQ")
            self._start_thread(mode="resume")
            return

        if verb == "pause":
            with self._lock:
                self._pause_req = True
            rospy.logwarn("[EXEC] PAUSE (async)")
            self._publish_state("PAUSE_REQ")
            try:
                self.mbf.cancel_all()
            except Exception:
                pass
            try:
                self._clear_ai_spot("pause")
            except Exception:
                pass
            # pause: stop cleaning + hold stop
            self.clean.pause_stop(self.vacuum_delay_s)
            self._start_pause_hold()
            return

        # SUSPEND: like PAUSE (preserve checkpoint & allow resume), but WITHOUT pause-hold.
        # Used by Task layer for auto-docking: executor should not fight navigation by publishing cmd_vel=0 continuously.
        if verb == "suspend":
            with self._lock:
                self._pause_req = True
            rospy.logwarn("[EXEC] SUSPEND (async, no hold)")
            self._publish_state("SUSPEND_REQ")
            try:
                self.mbf.cancel_all()
            except Exception:
                pass

            try:
                self._clear_ai_spot("suspend")
            except Exception:
                pass

            # ensure base stops, but do not keep holding
            self._start_brake(self.hard_stop_s)
            self.clean.pause_stop(self.vacuum_delay_s)
            self._stop_pause_hold()
            return

        if verb in ["cancel", "stop"]:
            with self._lock:
                t = getattr(self, "_running_thread", None)
                run_active = bool(t and t.is_alive())
                self._cancel_req = bool(run_active)
                if not run_active:
                    self._pause_req = False
            rospy.logwarn("[EXEC] CANCEL (async)" if run_active else "[EXEC] CANCEL while idle -> keep IDLE")
            self._publish_state("CANCEL_REQ" if run_active else "IDLE")
            try:
                self.mbf.cancel_all()
            except Exception:
                pass
            try:
                self._clear_ai_spot("cancel")
            except Exception:
                pass

            # 关键：立即刹车一小段时间，确保 CANCEL 后底盘立刻停
            self._start_brake(self.hard_stop_s)

            self.clean.cancel_stop(self.vacuum_delay_s)
            self._stop_pause_hold()
            return

        if verb in ["estop", "e-stop", "emergency_stop"]:
            with self._lock:
                self._estop = True
            rospy.logerr("[EXEC] E-STOP")
            self._publish_state("ESTOP")
            try:
                self.mbf.cancel_all()
            except Exception:
                pass
            try:
                self._clear_ai_spot("estop")
            except Exception:
                pass
            self.clean.fail_stop()
            self._start_pause_hold()
            return

        rospy.logwarn("[EXEC] unknown cmd: %s", cmd)

    # ---------- pause hold ----------
    def _start_pause_hold(self):
        self._stop_pause_hold()
        if self.pause_hold_hz <= 0.0:
            return
        period = 1.0 / self.pause_hold_hz

        # 立刻先停一下
        try:
            self.act.hard_stop_once()
        except Exception:
            pass

        def _tick(_evt):
            try:
                self.act.hard_stop_once()
            except Exception:
                pass

        self._pause_timer = rospy.Timer(rospy.Duration(period), _tick)

        # 再给一点时间把速度刹住
        if self.hard_stop_s > 1e-3:
            rospy.sleep(self.hard_stop_s)

    def _stop_pause_hold(self):
        if self._pause_timer is not None:
            try:
                self._pause_timer.shutdown()
            except Exception:
                pass
        self._pause_timer = None

    # ---------- short brake (for cancel) ----------
    def _start_brake(self, duration_s: float):
        """
        短时刹车：连续发布 cmd_vel=0 一小段时间，确保 CANCEL 后底盘立刻停。
        不同于 PAUSE 的 hold，这个只刹 duration_s，到点自动停。
        """
        # 先立刻来一脚
        try:
            self.act.hard_stop_once()
        except Exception:
            pass

        # 清掉旧的 brake timer
        if self._brake_timer is not None:
            try:
                self._brake_timer.shutdown()
            except Exception:
                pass
            self._brake_timer = None

        duration_s = float(max(0.0, duration_s))
        if duration_s <= 1e-3:
            return

        hz = max(20.0, float(self.pause_hold_hz) if self.pause_hold_hz > 0 else 20.0)
        period = 1.0 / hz
        self._brake_deadline_ts = time.time() + duration_s

        def _tick(_evt):
            if time.time() >= self._brake_deadline_ts:
                try:
                    if self._brake_timer is not None:
                        self._brake_timer.shutdown()
                except Exception:
                    pass
                self._brake_timer = None
                return
            try:
                self.act.hard_stop_once()
            except Exception:
                pass

        self._brake_timer = rospy.Timer(rospy.Duration(period), _tick)

    # ---------- TF ----------
    def _get_robot_xyt(self) -> Optional[Tuple[float, float, float]]:
        """(x,y,yaw) of base_frame in frame_id"""
        try:
            trans = self._tf_buf.lookup_transform(
                self.frame_id, self.base_frame, rospy.Time(0), rospy.Duration(0.2)
            )
            x = float(trans.transform.translation.x)
            y = float(trans.transform.translation.y)
            q = trans.transform.rotation
            yaw = _yaw_from_quat(q.x, q.y, q.z, q.w)
            return (x, y, yaw)
        except Exception:
            return None

    # ---------- checkpoints ----------
    def _ensure_checkpoint_schema(self):
        # OperationsStore ensures mission_checkpoints on init.
        return

    def _load_checkpoint(self, *, run_id: str = "") -> Optional[Dict[str, Any]]:
        """Load checkpoint by canonical run_id."""
        run_id = (run_id or "").strip()
        if not run_id:
            return None

        try:
            row = self._ops_store.get_mission_checkpoint(run_id)
            if row is None:
                return None
            ckpt = {
                "run_id": str(row.run_id or ""),
                "zone_id": str(row.zone_id or ""),
                "plan_id": str(row.plan_id or ""),
                "zone_version": int(row.zone_version or 0),
                "exec_index": int(row.exec_index or 0),
                "block_id": int(row.block_id if row.block_id is not None else -1),
                "path_index": int(row.path_index or 0),
                "path_s": float(row.path_s or 0.0),
                "state": str(row.state or ""),
                "water_off_latched": int(1 if row.water_off_latched else 0),
                "map_revision_id": str(getattr(row, "map_revision_id", "") or ""),
                "map_id": str(row.map_id or ""),
                "map_md5": str(row.map_md5 or ""),
            }
            rospy.logwarn(
                "[CKPT] load run=%s zone=%s plan_id=%s exec_index=%d block_id=%d path_index=%d path_s=%.3f state=%s water_off=%d",
                ckpt["run_id"], ckpt["zone_id"], ckpt["plan_id"], ckpt["exec_index"], ckpt["block_id"],
                ckpt["path_index"], ckpt["path_s"], ckpt["state"], ckpt["water_off_latched"],
            )
            return ckpt
        except Exception:
            pass
        return None

    def _write_checkpoint_v2(
        self,
        *,
        run_id: str,
        zone_id: str,
        plan_id: str,
        zone_version: int = 0,
        exec_index: int,
        block_id: int,
        path_index: int,
        path_s: float,
        state: str,
        water_off_latched: int,
        map_revision_id: str = "",
        map_id: str = "",
        map_md5: str = "",
        updated_ts: float,
    ):
        self._ops_store.upsert_mission_checkpoint(
            MissionCheckpointRecord(
                run_id=str(run_id),
                zone_id=str(zone_id),
                plan_id=str(plan_id),
                zone_version=int(zone_version or 0),
                exec_index=int(exec_index),
                block_id=int(block_id),
                path_index=int(path_index),
                path_s=float(path_s),
                state=str(state),
                water_off_latched=bool(int(water_off_latched)),
                map_revision_id=str(map_revision_id or ""),
                map_id=str(map_id or ""),
                map_md5=str(map_md5 or ""),
                updated_ts=float(updated_ts),
            )
        )

    def _save_checkpoint(self, zone_id: str, plan_id: str, state: str):
        # Always write v2 by run_id
        with self._lock:
            if not self._run_id:
                self._run_id = uuid.uuid4().hex
            rid = str(self._run_id)
        self._write_checkpoint_v2(
            run_id=rid,
            zone_id=str(zone_id or ""),
            plan_id=str(plan_id or ""),
            zone_version=int(getattr(self._plan, "zone_version", 0) or 0),
            exec_index=int(self._exec_index),
            block_id=int(self._block_id),
            path_index=int(max(0, self._block_cut_idx0 + self._path_index)),
            path_s=float(max(0.0, self._block_cut_s0 + self._path_s)),
            state=str(state),
            water_off_latched=int(1 if self._water_off_latched else 0),
            map_revision_id=str(self._runtime_map_revision_id or ""),
            map_id=str(self._runtime_map_id or ""),
            map_md5=str(self._runtime_map_md5 or ""),
            updated_ts=time.time(),
        )

    def _update_active_run_context(
        self,
        *,
        zone_id: str,
        plan: LoadedPlan,
        sys_profile_name: str,
        controller_name: str,
        actuator_profile_name: str,
        clean_mode: str,
    ):
        run_id = str(self._run_id or "").strip()
        if not run_id:
            return
        map_name, _scope_ok = self._ensure_runtime_map_name()
        map_revision_id = self._ensure_runtime_map_revision_id()
        if not map_name:
            map_name = str(getattr(plan, "map_name", "") or "").strip()
        if not map_revision_id:
            map_revision_id = str(getattr(plan, "map_revision_id", "") or "").strip()
        try:
            self._ops_store.update_run_execution_context(
                run_id,
                map_name=str(map_name or ""),
                map_revision_id=str(map_revision_id or ""),
                zone_id=str(zone_id or ""),
                plan_profile_name=str(getattr(plan, "plan_profile_name", "") or ""),
                plan_id=str(plan.plan_id or ""),
                zone_version=int(getattr(plan, "zone_version", 0) or 0),
                constraint_version=str(getattr(plan, "constraint_version", "") or ""),
                sys_profile_name=str(sys_profile_name or ""),
                mbf_controller_name=str(controller_name or ""),
                actuator_profile_name=str(actuator_profile_name or ""),
                clean_mode=str(clean_mode or ""),
                map_id=str(self._runtime_map_id or ""),
                map_md5=str(self._runtime_map_md5 or ""),
            )
        except Exception as e:
            rospy.logwarn_throttle(2.0, "[EXEC] update_run_execution_context failed: run=%s err=%s", run_id, str(e))

    def _get_run_snapshot_map_name(self, run_id: str = "") -> str:
        rid = str(run_id or self._run_id or "").strip()
        if not rid:
            return ""
        try:
            run = self._ops_store.get_run(rid)
        except Exception:
            return ""
        if run is None:
            return ""
        return str(run.map_name or "").strip()

    def _get_run_snapshot_map_revision_id(self, run_id: str = "") -> str:
        rid = str(run_id or self._run_id or "").strip()
        if not rid:
            return ""
        try:
            run = self._ops_store.get_run(rid)
        except Exception:
            return ""
        if run is None:
            return ""
        return str(getattr(run, "map_revision_id", "") or "").strip()

    def _ensure_runtime_map_name(self) -> Tuple[str, bool]:
        map_name = ""
        ok = False
        try:
            map_name, _scope_alias = get_runtime_map_scope()
            ok = bool(map_name)
        except Exception:
            ok = False

        map_name = str(map_name or "").strip()
        with self._lock:
            self._runtime_map_name = map_name
        return map_name, bool(ok)

    def _ensure_runtime_map_revision_id(self) -> str:
        revision_id = ""
        try:
            revision_id = get_runtime_map_revision_id("/cartographer/runtime")
        except Exception:
            revision_id = ""
        revision_id = str(revision_id or "").strip()
        with self._lock:
            self._runtime_map_revision_id = revision_id
        return revision_id

    def _resolve_plan_lookup_scope(self) -> Tuple[str, str]:
        map_revision_id = self._get_run_snapshot_map_revision_id()
        map_name = self._get_run_snapshot_map_name()
        if map_revision_id:
            return map_name, map_revision_id
        map_name = self._get_run_snapshot_map_name()
        if map_name:
            return map_name, ""
        map_revision_id = self._ensure_runtime_map_revision_id()
        map_name, _ok = self._ensure_runtime_map_name()
        return map_name, map_revision_id

    def _resolve_plan_lookup_map_name(self) -> str:
        map_name, _map_revision_id = self._resolve_plan_lookup_scope()
        if map_name:
            return map_name
        map_name, _ok = self._ensure_runtime_map_name()
        return map_name

    def _resolve_expected_map_revision_id(self, plan: LoadedPlan) -> str:
        map_revision_id = self._get_run_snapshot_map_revision_id()
        plan_revision_id = str(getattr(plan, "map_revision_id", "") or "").strip()
        if not map_revision_id:
            map_revision_id = plan_revision_id
        return str(map_revision_id or "").strip()

    def _resolve_expected_map_name(self, plan: LoadedPlan) -> str:
        map_name = self._get_run_snapshot_map_name()
        plan_name = str(getattr(plan, "map_name", "") or "").strip()
        if not map_name:
            map_name = plan_name
        if not map_name:
            runtime_name, _ok = self._ensure_runtime_map_name()
            if not map_name:
                map_name = runtime_name
        return str(map_name or "").strip()

    # ---------- thread control ----------
    def _start_thread(self, mode: str):
        with self._lock:
            if self._running_thread and self._running_thread.is_alive():
                rospy.logwarn("[EXEC] thread already running; ignore %s", mode)
                return
            t = threading.Thread(target=self._run_thread_main, args=(mode,), daemon=True)
            self._running_thread = t
            t.start()

    def _should_abort(self) -> str:
        with self._lock:
            if self._estop:
                return "ESTOP"
            if self._cancel_req:
                return "CANCEL"
            if self._pause_req:
                return "PAUSE"
        return ""

    # ---------- main spin ----------
    def spin(self):
        self.mbf.wait_for_servers()
        self._publish_state("IDLE")
        rospy.loginfo("[EXEC] ready. cmd=%s", rospy.resolve_name("~cmd"))
        rospy.on_shutdown(self.shutdown)
        rospy.spin()

    # ---------- run logic (start/resume) ----------
    def _run_thread_main(self, mode: str):
        with self._lock:
            zone_id = self._zone_id
            run_id = str(self._run_id or "")

        # start requires zone_id; resume requires a concrete run_id
        if mode != "resume" and not zone_id:
            self._publish_state("IDLE")
            return
        if mode == "resume" and (not run_id):
            self._publish_state("IDLE")
            return

        try:
            if mode == "resume":
                self._run_resume(zone_id, run_id)
            else:
                self._run_start(zone_id)
        except Exception as e:
            rospy.logerr("[EXEC] run failed: %s", str(e))
            self.clean.fail_stop()
            self._publish_state("FAILED")
        finally:
            with self._lock:
                if getattr(self, "_running_thread", None) is threading.current_thread():
                    self._running_thread = None

    def _apply_profile_only_and_stop(self, profile_name: str, water_off_latched: bool):
        """
        zone_begin：仅加载配置，不启动清洁机构。
        清洁机构只在进入 FOLLOW 时打开。
        """
        self.clean.set_profile(profile_name)

        self.clean.des.brush_on = False
        self.clean.des.scraper_on = False
        self.clean.des.vacuum_on = False
        self.clean.des.water_on = False
        self.clean.des.water_off_latched = bool(water_off_latched)

        # 若已经锁水（最后段）则保持锁水；否则也保持 water_off（通行段）
        self.clean.apply_full()

    def _check_zone_consistency_or_abort(self, plan: LoadedPlan) -> bool:
        """Check whether the loaded plan is still consistent with latest zone meta.

        We currently implement the most important guard for commercial robots:
          - plan.zone_version must match zones.zone_version (latest) for that zone.

        If mismatch and strict_zone_version_check==True, abort execution.
        """
        try:
            scope_name = self._resolve_expected_map_name(plan)
            scope_revision_id = self._resolve_expected_map_revision_id(plan)
            zm = self.loader.get_zone_meta(
                str(plan.zone_id),
                map_name=str(scope_name or ""),
                map_revision_id=str(scope_revision_id or ""),
            )
            if not zm:
                return True
            zver = int(zm.get("zone_version", 0) or 0)
            pver = int(getattr(plan, "zone_version", 0) or 0)
            if zver != pver:
                msg = f"zone_version_mismatch: zone={plan.zone_id} zone_ver_now={zver} plan_ver={pver} plan_id={plan.plan_id}"
                if self.strict_zone_version_check:
                    rospy.logerr("[EXEC] %s", msg)
                    self._emit(f"ERROR:ZONE_VERSION_MISMATCH:{msg}")
                    self._set_error(code="ZONE_VERSION_MISMATCH", msg=msg, data={"zone_id": str(plan.zone_id), "plan_id": str(plan.plan_id), "zone_ver_now": int(zver), "plan_ver": int(pver)})
                    self._publish_state("ERROR_ZONE_VERSION_MISMATCH")
                    return False
                rospy.logwarn("[EXEC] %s (strict_zone_version_check=0 -> continue)", msg)
        except Exception as e:
            rospy.logwarn_throttle(2.0, "[EXEC] zone consistency check skipped: %s", str(e))
        return True

    def _ensure_runtime_map_identity(self) -> Tuple[str, str, bool]:
        """Best-effort ensure /map_id and /map_md5 exist.

        Cache into self._runtime_map_id/_runtime_map_md5.
        """
        mid = ""
        mmd5 = ""
        ok = False
        try:
            if self.auto_map_identity_enable:
                mid, mmd5, ok = ensure_map_identity(
                    map_topic=self.map_topic,
                    timeout_s=self.map_identity_timeout_s,
                    set_global_params=True,
                    set_private_params=True,
                    refresh=True,
                )
            else:
                mid, mmd5 = get_runtime_map_identity()
                ok = bool(mid or mmd5)
        except Exception:
            ok = False

        mid = str(mid or "").strip()
        mmd5 = str(mmd5 or "").strip()
        with self._lock:
            self._runtime_map_id = mid
            self._runtime_map_md5 = mmd5
        return mid, mmd5, bool(ok)

    def _check_map_consistency_or_abort(self, plan: LoadedPlan, ckpt: Optional[dict] = None) -> bool:
        """Check plan/checkpoint map identity against current runtime map.

        - If plan carries map_md5/map_id, enforce strict_map_check.
        - If resume and checkpoint carries map_md5/map_id, enforce strict_resume_map_check.
        - If no expected identity is stored on the plan, skip check but keep auto-injection.
        """
        strict = bool(self.strict_resume_map_check) if ckpt else bool(self.strict_map_check)

        exp_name = self._resolve_expected_map_name(plan)
        exp_revision_id = self._resolve_expected_map_revision_id(plan)
        exp_id = str(getattr(plan, "map_id", "") or "").strip()
        exp_md5 = str(getattr(plan, "map_md5", "") or "").strip()

        # If checkpoint stores map identity, it has the highest priority for resume.
        if ckpt:
            exp_revision_id = str(ckpt.get("map_revision_id") or exp_revision_id).strip()
            exp_id = str(ckpt.get("map_id") or exp_id).strip()
            exp_md5 = str(ckpt.get("map_md5") or exp_md5).strip()

        # If plan doesn't have map fields, try zones table snapshot.
        if (not exp_name) or (not exp_revision_id) or (not exp_id) or (not exp_md5):
            try:
                zm = self.loader.get_zone_meta(
                    str(plan.zone_id),
                    map_name=str(exp_name or ""),
                    map_revision_id=str(exp_revision_id or ""),
                ) or {}
                if not exp_name:
                    exp_name = str(zm.get("map_name") or "").strip()
                if not exp_revision_id:
                    exp_revision_id = str(zm.get("map_revision_id") or "").strip()
                if not exp_id:
                    exp_id = str(zm.get("map_id") or "").strip()
                if not exp_md5:
                    exp_md5 = str(zm.get("map_md5") or "").strip()
            except Exception:
                pass

        # No expected identity stored -> don't block execution on that basis.
        exp_name = str(exp_name or "").strip()
        exp_revision_id = str(exp_revision_id or "").strip()

        if (not exp_name) and (not exp_revision_id) and (not exp_id and not exp_md5):
            # still try to inject for later plans/checkpoints
            self._ensure_runtime_map_name()
            self._ensure_runtime_map_revision_id()
            self._ensure_runtime_map_identity()
            return True

        runtime_name, scope_ok = self._ensure_runtime_map_name()
        runtime_revision_id = self._ensure_runtime_map_revision_id()
        mid, mmd5, ok = self._ensure_runtime_map_identity()

        if exp_revision_id:
            if strict and not runtime_revision_id:
                msg = (
                    "runtime_map_revision_missing: runtime_map_revision_id='' expected_map_revision_id='%s'"
                    % exp_revision_id
                )
                rospy.logerr("[EXEC] %s", msg)
                self._emit(f"ERROR:MAP_REVISION_MISSING:{msg}")
                self._set_error(
                    code="MAP_REVISION_MISSING",
                    msg=msg,
                    data={
                        "expected_map_revision_id": exp_revision_id,
                        "runtime_map_revision_id": runtime_revision_id,
                    },
                )
                self._publish_state("ERROR_MAP_MISSING")
                return False

            if runtime_revision_id and exp_revision_id != runtime_revision_id:
                msg = (
                    "map_revision_mismatch: expected=%s runtime=%s plan_id=%s zone=%s"
                    % (exp_revision_id, runtime_revision_id, plan.plan_id, plan.zone_id)
                )
                if strict:
                    rospy.logerr("[EXEC] %s", msg)
                    self._emit(f"ERROR:MAP_MISMATCH:{msg}")
                    self._set_error(
                        code="MAP_REVISION_MISMATCH",
                        msg=msg,
                        data={
                            "expected_map_revision_id": exp_revision_id,
                            "runtime_map_revision_id": runtime_revision_id,
                            "plan_id": str(plan.plan_id),
                            "zone_id": str(plan.zone_id),
                        },
                    )
                    self._publish_state("ERROR_MAP_MISMATCH")
                    return False
                rospy.logwarn("[EXEC] %s (strict_map_check=0 -> continue)", msg)

            if exp_name and runtime_name and exp_name != runtime_name:
                rospy.logwarn(
                    "[EXEC] map_name differs while map_revision_id matches: expected=%s runtime=%s revision=%s",
                    exp_name,
                    runtime_name,
                    exp_revision_id,
                )

            rospy.loginfo(
                "[EXEC] map consistency ok by revision: map=%s revision=%s",
                runtime_name,
                runtime_revision_id or exp_revision_id,
            )
            return True

        if strict and (exp_name and (not runtime_name or not scope_ok)):
            msg = (
                "runtime_map_scope_missing: runtime_map_name='%s' expected_map_name='%s'"
                % (runtime_name, exp_name)
            )
            rospy.logerr("[EXEC] %s", msg)
            self._emit(f"ERROR:MAP_SCOPE_MISSING:{msg}")
            self._set_error(
                code="MAP_SCOPE_MISSING",
                msg=msg,
                data={
                    "expected_map_name": exp_name,
                    "runtime_map_name": runtime_name,
                },
            )
            self._publish_state("ERROR_MAP_MISSING")
            return False

        if exp_name and runtime_name and exp_name != runtime_name:
            msg = f"map_name_mismatch: expected={exp_name} runtime={runtime_name} plan_id={plan.plan_id} zone={plan.zone_id}"
            if strict:
                rospy.logerr("[EXEC] %s", msg)
                self._emit(f"ERROR:MAP_MISMATCH:{msg}")
                self._set_error(
                    code="MAP_SCOPE_MISMATCH",
                    msg=msg,
                    data={
                        "expected_map_name": exp_name,
                        "runtime_map_name": runtime_name,
                        "plan_id": str(plan.plan_id),
                        "zone_id": str(plan.zone_id),
                    },
                )
                self._publish_state("ERROR_MAP_MISMATCH")
                return False
            rospy.logwarn("[EXEC] %s (strict_map_check=0 -> continue)", msg)

        # Missing runtime identity
        if strict and ((exp_md5 and not mmd5) or (exp_id and not mid) or (not ok)):
            msg = f"runtime_map_identity_missing: map_id='{mid}' map_md5='{mmd5}' expected_id='{exp_id}' expected_md5='{exp_md5}'"
            rospy.logerr("[EXEC] %s", msg)
            self._emit(f"ERROR:MAP_IDENTITY_MISSING:{msg}")
            self._set_error(code="MAP_IDENTITY_MISSING", msg=msg, data={"expected_id": exp_id, "expected_md5": exp_md5, "runtime_id": mid, "runtime_md5": mmd5})
            self._publish_state("ERROR_MAP_MISSING")
            return False

        # Compare md5 first (stronger)
        if exp_md5 and mmd5 and exp_md5 != mmd5:
            msg = f"map_md5_mismatch: expected={exp_md5} runtime={mmd5} plan_id={plan.plan_id} zone={plan.zone_id}"
            if strict:
                rospy.logerr("[EXEC] %s", msg)
                self._emit(f"ERROR:MAP_MISMATCH:{msg}")
                self._set_error(code="MAP_MD5_MISMATCH", msg=msg, data={"expected": exp_md5, "runtime": mmd5, "plan_id": str(plan.plan_id), "zone_id": str(plan.zone_id)})
                self._publish_state("ERROR_MAP_MISMATCH")
                return False
            rospy.logwarn("[EXEC] %s (strict_map_check=0 -> continue)", msg)

        # Compare map_id (we allow empty runtime id when md5 matched, but in strict mode we checked missing already)
        if exp_id and mid and exp_id != mid:
            msg = f"map_id_mismatch: expected={exp_id} runtime={mid} plan_id={plan.plan_id} zone={plan.zone_id}"
            if strict:
                rospy.logerr("[EXEC] %s", msg)
                self._emit(f"ERROR:MAP_MISMATCH:{msg}")
                self._set_error(code="MAP_ID_MISMATCH", msg=msg, data={"expected": exp_id, "runtime": mid, "plan_id": str(plan.plan_id), "zone_id": str(plan.zone_id)})
                self._publish_state("ERROR_MAP_MISMATCH")
                return False
            rospy.logwarn("[EXEC] %s (strict_map_check=0 -> continue)", msg)

        rospy.loginfo(
            "[EXEC] map consistency ok: map=%s map_id=%s map_md5=%s",
            runtime_name,
            mid,
            mmd5,
        )
        return True

    def _check_constraint_consistency_or_abort(self, plan: LoadedPlan) -> bool:
        expected = str(getattr(plan, "constraint_version", "") or "").strip()
        if not expected:
            return True

        map_id = str(getattr(plan, "map_id", "") or "").strip()
        if not map_id:
            try:
                scope_name = self._resolve_expected_map_name(plan)
                scope_revision_id = self._resolve_expected_map_revision_id(plan)
                zm = self.loader.get_zone_meta(
                    str(plan.zone_id),
                    map_name=str(scope_name or ""),
                    map_revision_id=str(scope_revision_id or ""),
                ) or {}
                map_id = str(zm.get("map_id") or "").strip()
            except Exception:
                map_id = ""
        if not map_id:
            msg = "constraint_version_check_missing_map_id: cannot verify active map constraints"
            rospy.logerr("[EXEC] %s", msg)
            self._emit(f"ERROR:CONSTRAINT_VERSION_MISMATCH:{msg}")
            self._set_error(code="CONSTRAINT_VERSION_MISMATCH", msg=msg, data={"plan_id": str(plan.plan_id), "zone_id": str(plan.zone_id)})
            self._publish_state("ERROR_CONSTRAINT_VERSION_MISMATCH")
            return False

        scope_revision_id = self._resolve_expected_map_revision_id(plan)
        current = str(
            self.loader.get_active_constraint_version(
                map_id,
                scope_revision_id,
            ) or ""
        ).strip()
        if not current:
            msg = "constraint_version_check_missing_active_version: map_id=%s revision=%s expected=%s" % (
                map_id,
                scope_revision_id or "-",
                expected,
            )
            rospy.logerr("[EXEC] %s", msg)
            self._emit(f"ERROR:CONSTRAINT_VERSION_MISMATCH:{msg}")
            self._set_error(
                code="CONSTRAINT_VERSION_MISMATCH",
                msg=msg,
                data={
                    "plan_id": str(plan.plan_id),
                    "zone_id": str(plan.zone_id),
                    "map_id": map_id,
                    "map_revision_id": scope_revision_id,
                },
            )
            self._publish_state("ERROR_CONSTRAINT_VERSION_MISMATCH")
            return False

        if current != expected:
            msg = "constraint_version_mismatch: expected=%s active=%s map_id=%s revision=%s plan_id=%s zone=%s" % (
                expected,
                current,
                map_id,
                scope_revision_id or "-",
                str(plan.plan_id),
                str(plan.zone_id),
            )
            rospy.logerr("[EXEC] %s", msg)
            self._emit(f"ERROR:CONSTRAINT_VERSION_MISMATCH:{msg}")
            self._set_error(
                code="CONSTRAINT_VERSION_MISMATCH",
                msg=msg,
                data={
                    "expected": expected,
                    "active": current,
                    "map_id": map_id,
                    "map_revision_id": scope_revision_id,
                    "plan_id": str(plan.plan_id),
                    "zone_id": str(plan.zone_id),
                },
            )
            self._publish_state("ERROR_CONSTRAINT_VERSION_MISMATCH")
            return False

        rospy.loginfo("[EXEC] constraint consistency ok: version=%s map_id=%s", current, map_id)
        return True

    def _run_start(self, zone_id: str):
        self._publish_state("LOADING_PLAN")
        with self._lock:
            prof_req = (self._plan_profile_override or "").strip()
        if not prof_req:
            self._set_error(code="ERROR_MISSING_PLAN_PROFILE", msg="plan_profile is required for start")
            self._publish_state("ERROR_MISSING_PLAN_PROFILE")
            return
        map_name, map_revision_id = self._resolve_plan_lookup_scope()
        plan = self.loader.load_for_zone(
            zone_id,
            plan_profile_name=(prof_req or None),
            map_name=str(map_name or ""),
            map_revision_id=str(map_revision_id or ""),
        )
        # rebuild progress prefix sums
        self._rebuild_progress_index(plan)
        # zone version consistency check (best-effort but safe by default)
        if not self._check_zone_consistency_or_abort(plan):
            return
        # map identity consistency check (if plan/checkpoint has map fields)
        if not self._check_map_consistency_or_abort(plan, ckpt=None):
            return
        if not self._check_constraint_consistency_or_abort(plan):
            return
        self._plan = plan

        self._exec_index = 0
        self._block_id = -1
        self._path_index = 0
        self._path_s = 0.0
        self._block_cut_idx0 = 0
        self._water_off_latched = False

        # Push task intent to cleaning subsystem (does not start cleaning)
        try:
            self.clean.set_mode(self._effective_mode_for_execution())
        except Exception:
            pass

        sys_prof_eff = self._effective_sys_profile_for_actuator(plan.plan_profile_name)
        sys_name, controller_name, actuator_profile_name, _default_mode = self._resolve_sys_profile_spec(sys_prof_eff)
        if prof_req and plan.plan_profile_name and str(plan.plan_profile_name) != prof_req:
            rospy.logwarn(
                "[EXEC] requested plan_profile=%s but loaded plan_profile_name=%s (fallback plan selection?)",
                prof_req,
                plan.plan_profile_name,
            )

        self._publish_state("APPLY_PROFILE")
        self._apply_sys_profile_runtime(sys_prof_eff, water_off_latched=False)

        self._force_transit_for_next_connect = True
        self._save_checkpoint(zone_id, plan.plan_id, state="RUNNING")
        self._update_active_run_context(
            zone_id=zone_id,
            plan=plan,
            sys_profile_name=sys_name,
            controller_name=controller_name,
            actuator_profile_name=actuator_profile_name,
            clean_mode=self._effective_mode_for_execution(),
        )

        self._execute_from_checkpoint(zone_id, plan, ckpt=None)

    def _run_resume(self, zone_id: Optional[str], run_id: str):
        ckpt = self._load_checkpoint(run_id=run_id)
        if not ckpt:
            rospy.logwarn("[EXEC] no checkpoint for run=%s", str(run_id))
            self._publish_state("IDLE")
            return

        # Ensure we lock onto the checkpoint's run_id/zone_id (supports: resume run_id=<id>)
        with self._lock:
            if ckpt.get("run_id"):
                self._run_id = str(ckpt["run_id"])
            if (not self._zone_id) and ckpt.get("zone_id"):
                self._zone_id = str(ckpt["zone_id"])

        zone_id = str(zone_id or ckpt.get("zone_id") or "")
        if not zone_id:
            rospy.logwarn("[EXEC] checkpoint missing zone_id; cannot resume")
            self._publish_state("IDLE")
            return

        self._publish_state("LOADING_PLAN")
        with self._lock:
            prof_req = (self._plan_profile_override or "").strip()
        if not prof_req:
            self._set_error(code="ERROR_MISSING_PLAN_PROFILE", msg="plan_profile is required for resume")
            self._publish_state("ERROR_MISSING_PLAN_PROFILE")
            return
        map_name, map_revision_id = self._resolve_plan_lookup_scope()
        plan = self.loader.load_for_zone(
            zone_id,
            plan_profile_name=(prof_req or None),
            map_name=str(map_name or ""),
            map_revision_id=str(map_revision_id or ""),
        )
        self._rebuild_progress_index(plan)
        if not self._check_zone_consistency_or_abort(plan):
            return
        if not self._check_map_consistency_or_abort(plan, ckpt=ckpt):
            return
        if not self._check_constraint_consistency_or_abort(plan):
            return
        self._plan = plan

        if ckpt["plan_id"] and ckpt["plan_id"] != plan.plan_id:
            msg = f"ckpt_plan_mismatch: ckpt.plan_id={ckpt['plan_id']} loaded.plan_id={plan.plan_id} zone={zone_id}"
            rospy.logerr("[EXEC] %s", msg)
            self._emit(f"ERROR:PLAN_MISMATCH:{msg}")
            self._publish_state("ERROR_PLAN_MISMATCH")
            return

        self._exec_index = ckpt["exec_index"]
        self._block_id = ckpt["block_id"]
        self._path_index = ckpt["path_index"]
        self._path_s = ckpt["path_s"]
        self._block_cut_idx0 = 0
        self._water_off_latched = bool(ckpt["water_off_latched"])

        # Push task intent to cleaning subsystem (does not start cleaning)
        try:
            self.clean.set_mode(self._effective_mode_for_execution())
        except Exception:
            pass

        sys_prof_eff = self._effective_sys_profile_for_actuator(plan.plan_profile_name)
        sys_name, controller_name, actuator_profile_name, _default_mode = self._resolve_sys_profile_spec(sys_prof_eff)
        if prof_req and plan.plan_profile_name and str(plan.plan_profile_name) != prof_req:
            rospy.logwarn(
                "[EXEC] requested plan_profile=%s but loaded plan_profile_name=%s (fallback plan selection?)",
                prof_req,
                plan.plan_profile_name,
            )

        self._publish_state("APPLY_PROFILE")
        # resume/回充回来的“通行段”期间也不清洁，进入 FOLLOW 再开
        self._apply_sys_profile_runtime(sys_prof_eff, water_off_latched=self._water_off_latched)

        self._force_transit_for_next_connect = True
        self._save_checkpoint(zone_id, plan.plan_id, state="RUNNING")
        self._update_active_run_context(
            zone_id=zone_id,
            plan=plan,
            sys_profile_name=sys_name,
            controller_name=controller_name,
            actuator_profile_name=actuator_profile_name,
            clean_mode=self._effective_mode_for_execution(),
        )
        self._execute_from_checkpoint(zone_id, plan, ckpt=ckpt)

    # ---------- exec blocks ----------
    def _execute_from_checkpoint(self, zone_id: str, plan: LoadedPlan, ckpt: Optional[dict]):
        # plan.blocks 已经是执行顺序
        exec_blocks: List[LoadedBlock] = list(plan.blocks or [])
        rate = rospy.Rate(max(1.0, self.checkpoint_hz))

        for ei in range(self._exec_index, len(exec_blocks)):
            reason = self._should_abort()
            if reason:
                rospy.logwarn("[EXEC] interrupted before block ei=%d reason=%s", ei, reason)
                self._publish_state("PAUSED" if reason == "PAUSE" else "IDLE")
                return

            blk = exec_blocks[ei]
            self._exec_index = ei
            self._block_id = blk.block_id
            self._current_block_len_m = float(getattr(blk, "length_m", 0.0) or 0.0)
            self._block_cut_s0 = 0.0
            self._block_cut_idx0 = 0
            is_last = (ei == len(exec_blocks) - 1)

            # default for start
            cut_path_xyyaw = blk.path_xyyaw
            cut_start_idx = 0
            force_connect: Optional[bool] = None  # None -> use connect_skip_dist rule

            # resume same block
            if ckpt and blk.block_id == ckpt["block_id"] and ei == ckpt["exec_index"]:
                res = self._compute_inblock_resume_decision(blk, ckpt)
                cut_path_xyyaw = res["cut_path_xyyaw"]
                cut_start_idx = res["cut_start_idx"]
                self._block_cut_s0 = float(res.get("cut_start_s", 0.0) or 0.0)
                self._block_cut_idx0 = int(res.get("cut_start_idx", 0) or 0)
                force_connect = True if res["need_connect"] else False

                if res.get("skip_block", False):
                    rospy.logwarn("[RESUME] blk=%d remaining<=finish_thresh -> skip block", blk.block_id)
                    continue

            # transit 判定：
            # - start/resume 的首段 CONNECT 视为 transit（不清洁），进入 FOLLOW 再开
            # - block 之间的 CONNECT 默认非 transit（允许 keep_cleaning_on_during_connect=True 时持续清洁）
            transit = bool(self._force_transit_for_next_connect)

            rospy.loginfo(
                "[EXEC] block ei=%d id=%d len=%.2fm pts=%d (cut_start_idx=%d cut_pts=%d force_connect=%s transit=%s)",
                ei, blk.block_id, blk.length_m, blk.point_count,
                int(cut_start_idx), len(cut_path_xyyaw),
                str(force_connect),
                str(transit),
            )

            ok = self._run_one_block(
                zone_id, plan, blk,
                cut_path_xyyaw=cut_path_xyyaw,
                cut_start_idx=cut_start_idx,
                is_last=is_last,
                rate=rate,
                force_connect=force_connect,
                transit=transit,
            )
            if not ok:
                return

            # 一旦进入过 FOLLOW，就不再把后续 CONNECT 视为 transit
            self._force_transit_for_next_connect = False

        self._publish_state("FINISHING")
        self.clean.zone_end()
        self._save_checkpoint(zone_id, plan.plan_id, state="DONE")
        self._publish_state("DONE")

    # ---------- resume helpers ----------
    def _adaptive_backtrack_cap(self, dist_to_path_m: float, yaw_err_rad: float) -> float:
        """
        自适应 backtrack 上限：机器人越贴近路径/朝向越一致 -> backtrack 越小
        目的：resume_backtrack_m=0.5 也不至于把“起点”拉到身后很远导致 PP 抽风。
        """
        d = max(0.0, float(dist_to_path_m))
        ye = abs(float(yaw_err_rad))
        cap = 0.12 + 0.7 * d
        if ye > 0.6:
            cap *= max(0.35, 1.0 - (ye - 0.6) / 1.2)
        cap = max(0.08, min(cap, self.resume_backtrack_m))
        return cap

    def _compute_inblock_resume_decision(self, blk: LoadedBlock, ckpt: dict) -> Dict[str, Any]:
        """
        关键修复点：
        - need_connect=True 时：cut_idx 以 ckpt_s 为基准（ckpt_s - backtrack），不使用 proj.s_m（防漏扫）
        - need_connect=False 时：允许 bounded projection，但 forward 只能小幅 <= resume_forward_allow_m
        """
        path = blk.path_xyyaw
        if not path or len(path) < 2:
            return {"cut_path_xyyaw": path, "cut_start_idx": 0, "cut_start_s": 0.0, "need_connect": True}

        xy = [(float(p[0]), float(p[1])) for p in path]
        arclen = build_arclen(xy)
        total = float(arclen[-1])

        ckpt_idx = int(ckpt["path_index"])
        ckpt_s = float(ckpt["path_s"])
        ckpt_idx = max(0, min(ckpt_idx, len(path) - 1))
        ckpt_pt = xy[ckpt_idx]

        r = self._get_robot_xyt()
        if r is None:
            rospy.logwarn("[RESUME] blk=%d no TF; fallback cut at ckpt_idx=%d", blk.block_id, ckpt_idx)
            return {"cut_path_xyyaw": path[ckpt_idx:], "cut_start_idx": ckpt_idx, "cut_start_s": float(arclen[ckpt_idx]), "need_connect": True}
        rx, ry, ryaw = r

        try:
            self.dbg.publish_robot_pose(self.frame_id, rx, ry, ryaw)
        except Exception:
            pass

        dist_to_ckpt = math.hypot(rx - ckpt_pt[0], ry - ckpt_pt[1])

        remaining_from_ckpt = max(0.0, total - ckpt_s)
        if self.resume_finish_thresh_m > 1e-3 and remaining_from_ckpt <= self.resume_finish_thresh_m:
            rospy.logwarn(
                "[RESUME] blk=%d ckpt_s=%.2f total=%.2f remaining=%.2f <= finish_thresh=%.2f",
                blk.block_id, ckpt_s, total, remaining_from_ckpt, self.resume_finish_thresh_m,
            )
            return {
                "cut_path_xyyaw": path[-1:],
                "cut_start_idx": len(path) - 1,
                "cut_start_s": float(total),
                "need_connect": False,
                "skip_block": True,
            }

        back_win = max(self.resume_backtrack_m + 0.5, 1.0)
        min_s = max(0.0, ckpt_s - back_win)
        max_s = min(total, ckpt_s + max(0.0, self.resume_forward_allow_m))

        prog = project_along_segments(
            xy, arclen, (rx, ry),
            hint_index=ckpt_idx,
            search_back_pts=120,
            search_fwd_pts=450,
            min_s=min_s,
            max_s=max_s,
        )

        debug_free = None
        if prog is None and self.resume_debug_free_project:
            debug_free = project_along_segments(
                xy, arclen, (rx, ry),
                hint_index=ckpt_idx,
                search_back_pts=len(xy),
                search_fwd_pts=len(xy),
                min_s=0.0,
                max_s=total,
            )

        if prog is None:
            cut_s = max(0.0, ckpt_s - self.resume_backtrack_m)
            cut_idx = int(index_from_s(arclen, cut_s))
            cut_idx = max(0, min(cut_idx, len(path) - 1))
            rospy.logwarn(
                "[RESUME] blk=%d bounded_proj=None  robot=(%.2f,%.2f yaw=%.2f) ckpt(idx=%d s=%.2f pt=(%.2f,%.2f) d_ckpt=%.2f) "
                "win=[%.2f,%.2f] -> need_connect=True cut_s=%.2f cut_idx=%d%s",
                blk.block_id, rx, ry, ryaw,
                ckpt_idx, ckpt_s, ckpt_pt[0], ckpt_pt[1], dist_to_ckpt,
                min_s, max_s, cut_s, cut_idx,
                (f" free_proj(s={debug_free.s_m:.2f} d={debug_free.dist_m:.2f} seg={debug_free.seg_i} t={debug_free.t:.2f})" if debug_free else ""),
            )
            return {"cut_path_xyyaw": path[cut_idx:], "cut_start_idx": cut_idx, "cut_start_s": float(cut_s), "need_connect": True}

        seg_i = int(prog.seg_i)
        seg_i = max(0, min(seg_i, len(xy) - 2))
        x0, y0 = xy[seg_i]
        x1, y1 = xy[seg_i + 1]
        t = float(prog.t)
        proj_x = x0 + t * (x1 - x0)
        proj_y = y0 + t * (y1 - y0)
        seg_yaw = math.atan2((y1 - y0), (x1 - x0))
        yaw_err = _wrap_pi(seg_yaw - ryaw)

        try:
            self.dbg.publish_proj_pose(self.frame_id, proj_x, proj_y, seg_yaw)
        except Exception:
            pass

        s_err = float(prog.s_m - ckpt_s)

        on_path_ok = (prog.dist_m <= self.resume_accept_dist) and (abs(yaw_err) <= self.resume_max_yaw_err_rad)
        need_connect = (not on_path_ok)

        if need_connect:
            cut_s = max(0.0, ckpt_s - self.resume_backtrack_m)
            cut_idx = int(index_from_s(arclen, cut_s))
            cut_idx = max(0, min(cut_idx, len(path) - 1))
            rospy.logwarn(
                "[RESUME] blk=%d robot=(%.2f,%.2f yaw=%.2f) ckpt(idx=%d s=%.2f d_ckpt=%.2f) "
                "bounded_proj(s=%.2f d=%.2f seg=%d t=%.2f seg_yaw=%.2f yaw_err=%.2f s_err=%.2f) "
                "win=[%.2f,%.2f] -> need_connect=True (NO_FORWARD_JUMP) cut_s=%.2f cut_idx=%d",
                blk.block_id, rx, ry, ryaw,
                ckpt_idx, ckpt_s, dist_to_ckpt,
                prog.s_m, prog.dist_m, seg_i, t, seg_yaw, yaw_err, s_err,
                min_s, max_s, cut_s, cut_idx,
            )
            return {"cut_path_xyyaw": path[cut_idx:], "cut_start_idx": cut_idx, "cut_start_s": float(cut_s), "need_connect": True}

        back_cap = self._adaptive_backtrack_cap(float(prog.dist_m), float(yaw_err))
        back_eff = min(self.resume_backtrack_m, back_cap)
        follow_s = max(0.0, float(prog.s_m) - back_eff)
        cut_idx = int(index_from_s(arclen, follow_s))
        cut_idx = max(0, min(cut_idx, len(path) - 1))

        min_follow_s = max(0.0, ckpt_s - self.resume_backtrack_m)
        if follow_s < min_follow_s:
            follow_s = min_follow_s
            cut_idx = int(index_from_s(arclen, follow_s))

        rospy.logwarn(
            "[RESUME] blk=%d robot=(%.2f,%.2f yaw=%.2f) ckpt(idx=%d s=%.2f d_ckpt=%.2f) "
            "bounded_proj(s=%.2f d=%.2f seg=%d t=%.2f seg_yaw=%.2f yaw_err=%.2f s_err=%.2f) "
            "win=[%.2f,%.2f] on_path_ok=True -> need_connect=False back_eff=%.2f follow_s=%.2f cut_idx=%d",
            blk.block_id, rx, ry, ryaw,
            ckpt_idx, ckpt_s, dist_to_ckpt,
            prog.s_m, prog.dist_m, seg_i, t, seg_yaw, yaw_err, s_err,
            min_s, max_s, back_eff, follow_s, cut_idx,
        )

        return {"cut_path_xyyaw": path[cut_idx:], "cut_start_idx": cut_idx, "cut_start_s": float(follow_s), "need_connect": False}

    # ---------- block execution ----------
    def _enter_follow_cleaning(self):
        """Enter FOLLOW: enable cleaning according to current clean_mode."""
        self.clean.enter_follow(water_off_latched=self._water_off_latched)

    def _ensure_transit_cleaning_off(self):
        """Transit segment: force all channels off."""
        self.clean.enter_transit_off(water_off_latched=self._water_off_latched)

    def _sleep_or_abort(self, seconds: float) -> str:
        deadline = time.time() + max(0.0, float(seconds))
        while not rospy.is_shutdown() and time.time() < deadline:
            reason = self._should_abort()
            if reason:
                return reason
            rospy.sleep(0.05)
        return ""

    def _make_live_ckpt(self, zone_id: str, plan_id: str, blk: LoadedBlock) -> Dict[str, Any]:
        return {
            "run_id": str(self._run_id or ""),
            "zone_id": str(zone_id or ""),
            "plan_id": str(plan_id or ""),
            "exec_index": int(self._exec_index),
            "block_id": int(blk.block_id),
            "path_index": int(max(0, self._block_cut_idx0 + self._path_index)),
            "path_s": float(max(0.0, self._block_cut_s0 + self._path_s)),
            "state": "RUNNING",
            "water_off_latched": bool(self._water_off_latched),
        }

    def _enter_paused_recovery(self, zone_id: str, plan_id: str, *, code: str, msg: str, data: Optional[Dict[str, Any]] = None):
        rospy.logwarn("[EXEC] enter PAUSED_RECOVERY code=%s msg=%s", str(code), str(msg))
        try:
            self.clean.enter_transit_off(water_off_latched=self._water_off_latched)
        except Exception:
            pass
        self._set_error(code=str(code), msg=str(msg), data=(data or {}))
        self._save_checkpoint(zone_id, plan_id, state="PAUSED")
        self._emit(f"PAUSED_RECOVERY:{code}:{msg}")
        self._publish_state("PAUSED_RECOVERY")

    def _run_one_block(
        self,
        zone_id: str,
        plan: LoadedPlan,
        blk: LoadedBlock,
        *,
        cut_path_xyyaw,
        cut_start_idx: int,
        is_last: bool,
        rate: rospy.Rate,
        force_connect: Optional[bool] = None,
        transit: bool = False,
    ) -> bool:
        if not cut_path_xyyaw:
            rospy.logerr("[EXEC] empty cut_path for block=%d", blk.block_id)
            self.clean.fail_stop()
            self._publish_state("FAILED")
            return False

        connect_failures = 0
        follow_failures = 0

        cur_cut_path = list(cut_path_xyyaw)
        cur_cut_start_idx = int(cut_start_idx)
        cur_cut_start_s = float(self._block_cut_s0 or 0.0)
        cur_force_connect = force_connect
        cur_transit = bool(transit)

        while not rospy.is_shutdown():
            if not cur_cut_path:
                rospy.logerr("[EXEC] retry cut_path empty block=%d", blk.block_id)
                self._enter_paused_recovery(
                    zone_id, plan.plan_id,
                    code="EMPTY_RETRY_PATH",
                    msg=f"block={blk.block_id} retry cut path empty",
                    data={"block_id": int(blk.block_id)},
                )
                return False

            self._block_cut_idx0 = int(cur_cut_start_idx)
            self._block_cut_s0 = float(cur_cut_start_s)

            robot = self._get_robot_xyt()
            rx, ry, _ = robot if robot else (0.0, 0.0, 0.0)
            start_xy = (float(cur_cut_path[0][0]), float(cur_cut_path[0][1]))
            d_start = math.hypot(start_xy[0] - rx, start_xy[1] - ry) if robot else 1e9

            if cur_force_connect is None:
                do_connect = (d_start > self.connect_skip_dist)
            else:
                do_connect = bool(cur_force_connect)

            connect_transit = bool(cur_transit) or (not self.keep_cleaning_on_during_connect)

            if do_connect:
                self._publish_state(f"CONNECT:block_{blk.block_id}")

                if connect_transit:
                    self._ensure_transit_cleaning_off()

                entry_pose = make_pose(plan.frame_id, start_xy[0], start_xy[1], float(cur_cut_path[0][2]))
                rospy.logwarn(
                    "[CONNECT] blk=%d retry=%d start=(%.2f,%.2f) d_start=%.2f cut_idx=%d transit=%s",
                    blk.block_id, connect_failures, start_xy[0], start_xy[1], d_start, int(cur_cut_start_idx), str(connect_transit),
                )

                self.mbf.send_connect(entry_pose)

                while not rospy.is_shutdown():
                    reason = self._should_abort()
                    if reason:
                        rospy.logwarn("[EXEC] CONNECT interrupted by %s", reason)
                        self._save_checkpoint(zone_id, plan.plan_id, state=("PAUSED" if reason == "PAUSE" else "CANCELED"))
                        self._publish_state("PAUSED" if reason == "PAUSE" else "IDLE")
                        return False
                    if self.mbf.connect_done():
                        break
                    rospy.sleep(0.05)

                if not self.mbf.connect_succeeded():
                    connect_failures += 1
                    msg = f"block={blk.block_id} connect failed attempt={connect_failures}/{self.connect_retry_max}"
                    rospy.logwarn("[EXEC] %s", msg)

                    if connect_failures <= self.connect_retry_max:
                        self._emit(f"CONNECT_RETRY:block={blk.block_id} attempt={connect_failures}/{self.connect_retry_max}")
                        try:
                            self.clean.enter_transit_off(water_off_latched=self._water_off_latched)
                        except Exception:
                            pass
                        if self.retry_clear_costmaps:
                            self.mbf.clear_costmaps()

                        reason = self._sleep_or_abort(self.retry_wait_s)
                        if reason:
                            self._save_checkpoint(zone_id, plan.plan_id, state=("PAUSED" if reason == "PAUSE" else "CANCELED"))
                            self._publish_state("PAUSED" if reason == "PAUSE" else "IDLE")
                            return False

                        cur_force_connect = True
                        cur_transit = True
                        continue

                    if self.retry_pause_recovery_on_exhausted:
                        self._enter_paused_recovery(
                            zone_id, plan.plan_id,
                            code="CONNECT_FAILED",
                            msg=msg,
                            data={"block_id": int(blk.block_id), "attempt": int(connect_failures)},
                        )
                        return False

                    rospy.logerr("[EXEC] CONNECT failed block=%d", blk.block_id)
                    self.clean.fail_stop()
                    self._save_checkpoint(zone_id, plan.plan_id, state="FAILED")
                    self._publish_state("FAILED")
                    return False

            try:
                self.dbg.publish_paths(
                    frame_id=plan.frame_id,
                    full_xyyaw=blk.path_xyyaw,
                    cut_xyyaw=cur_cut_path,
                    cut_idx=int(cur_cut_start_idx),
                    reason="enter_follow",
                    extra=f"block_id={blk.block_id}",
                )
            except Exception:
                pass

            self._publish_state(f"FOLLOW:block_{blk.block_id}")
            self._enter_follow_cleaning()

            path_msg = make_path(plan.frame_id, cur_cut_path)

            xy = [(float(p[0]), float(p[1])) for p in cur_cut_path]
            arclen = build_arclen(xy)
            total_s = float(arclen[-1]) if arclen else 0.0

            self._path_index = 0
            self._path_s = 0.0

            self.mbf.send_execute_path(path_msg)

            last_ckpt_write = time.time()
            last_follow_log = 0.0

            while not rospy.is_shutdown():
                reason = self._should_abort()
                if reason:
                    rospy.logwarn("[EXEC] FOLLOW interrupted by %s", reason)
                    self._save_checkpoint(zone_id, plan.plan_id, state=("PAUSED" if reason == "PAUSE" else "CANCELED"))
                    self._publish_state("PAUSED" if reason == "PAUSE" else "IDLE")
                    return False

                p = self._get_robot_xyt()
                if p is not None and len(xy) >= 2:
                    px, py, _ = p
                    hint = max(0, min(self._path_index, len(xy) - 1))
                    min_s = max(0.0, self._path_s - 0.5)
                    max_s = min(total_s, self._path_s + 3.0)
                    prog = project_along_segments(
                        xy, arclen, (px, py),
                        hint_index=hint,
                        search_back_pts=25,
                        search_fwd_pts=200,
                        min_s=min_s,
                        max_s=max_s,
                    )
                    if prog is not None:
                        self._path_s = float(prog.s_m)
                        self._path_index = int(index_from_s(arclen, prog.s_m))

                if is_last and (not self._water_off_latched) and self.water_off_distance > 1e-3:
                    remaining = max(0.0, total_s - self._path_s)
                    if remaining <= self.water_off_distance:
                        self._water_off_latched = True
                        self.clean.latch_water_off()
                        rospy.logwarn(
                            "[EXEC] last-block water_off by remaining_s=%.3f (<=%.3f)",
                            remaining, self.water_off_distance
                        )

                now = time.time()
                if self.checkpoint_hz > 0.0 and now - last_ckpt_write >= (1.0 / self.checkpoint_hz):
                    self._save_checkpoint(zone_id, plan.plan_id, state="RUNNING")
                    last_ckpt_write = now

                if now - last_follow_log >= 2.0:
                    rospy.loginfo(
                        "[FOLLOW] blk=%d s=%.2f/%.2f rem=%.2f idx=%d cut_idx=%d",
                        blk.block_id, self._path_s, total_s, max(0.0, total_s - self._path_s),
                        int(self._path_index), int(cur_cut_start_idx),
                    )
                    last_follow_log = now

                if self.mbf.exe_done():
                    break

                rate.sleep()

            if not self.mbf.exe_succeeded():
                follow_failures += 1
                msg = f"block={blk.block_id} follow failed attempt={follow_failures}/{self.follow_retry_max}"
                rospy.logwarn("[EXEC] %s", msg)

                if follow_failures <= self.follow_retry_max:
                    self._emit(f"FOLLOW_RETRY:block={blk.block_id} attempt={follow_failures}/{self.follow_retry_max}")

                    try:
                        self.clean.enter_transit_off(water_off_latched=self._water_off_latched)
                    except Exception:
                        pass

                    if self.retry_clear_costmaps:
                        self.mbf.clear_costmaps()

                    reason = self._sleep_or_abort(self.retry_wait_s)
                    if reason:
                        self._save_checkpoint(zone_id, plan.plan_id, state=("PAUSED" if reason == "PAUSE" else "CANCELED"))
                        self._publish_state("PAUSED" if reason == "PAUSE" else "IDLE")
                        return False

                    live_ckpt = self._make_live_ckpt(zone_id, plan.plan_id, blk)
                    res = self._compute_inblock_resume_decision(blk, live_ckpt)

                    if res.get("skip_block", False):
                        rospy.logwarn("[EXEC] retry resume says skip block=%d", blk.block_id)
                        self._save_checkpoint(zone_id, plan.plan_id, state="RUNNING")
                        return True

                    cur_cut_path = list(res["cut_path_xyyaw"])
                    cur_cut_start_idx = int(res["cut_start_idx"])
                    cur_cut_start_s = float(res.get("cut_start_s", 0.0) or 0.0)

                    if self.follow_retry_reconnect_on_fail:
                        cur_force_connect = True
                    else:
                        cur_force_connect = True if bool(res.get("need_connect", False)) else False

                    cur_transit = True
                    continue

                if self.retry_pause_recovery_on_exhausted:
                    self._enter_paused_recovery(
                        zone_id, plan.plan_id,
                        code="FOLLOW_FAILED",
                        msg=msg,
                        data={
                            "block_id": int(blk.block_id),
                            "attempt": int(follow_failures),
                            "path_s": float(self._block_cut_s0 + self._path_s),
                        },
                    )
                    return False

                rospy.logerr("[EXEC] FOLLOW failed block=%d -> fail_stop", blk.block_id)
                self.clean.fail_stop()
                self._save_checkpoint(zone_id, plan.plan_id, state="FAILED")
                self._publish_state("FAILED")
                return False

            self._path_s = total_s
            self._path_index = (len(xy) - 1) if xy else 0
            self._save_checkpoint(zone_id, plan.plan_id, state="RUNNING")
            return True

        return False

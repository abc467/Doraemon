# -*- coding: utf-8 -*-
import threading
import time
from dataclasses import dataclass

import rospy
from std_msgs.msg import Bool

from my_msg_srv.msg import CombinedStatus


@dataclass
class DesiredCleaningState:
    # brush_on   : brush plate motor + brush down command
    # scraper_on : scraper/squeegee down command
    # vacuum_on  : suction chain = suction machine (5003/0x05) + vacuum motor (5004)
    # water_on   : clean water valve + pump
    brush_on: bool = False
    scraper_on: bool = False
    vacuum_on: bool = False
    water_on: bool = False
    water_off_latched: bool = False


@dataclass
class MotionState:
    v_mps: float = 0.0
    w_rps: float = 0.0
    ts: float = 0.0


@dataclass
class FeedbackState:
    brush_position: int = -1
    scraper_position: int = -1
    ts: float = 0.0


@dataclass
class InterlockConfig:
    enable: bool = False
    max_lin_mps: float = 0.45
    max_ang_rps: float = 0.6
    mask_cleaning: bool = True
    mask_water: bool = True
    mask_vacuum: bool = False


class CleaningSubsystem:
    """Cleaning outputs orchestration with fixed clean-mode recipes.

    Layers:
      - sys_profile selects controller + actuator profile outside this class
      - clean_mode selects a fixed device recipe in this class
      - actuator executes device-capability commands over the fixed protocol
    """

    def __init__(self, actuator):
        self.act = actuator
        self.des = DesiredCleaningState()

        self._lock = threading.Lock()
        self._dispatch_lock = threading.RLock()

        self._profile_name = ""
        self._clean_mode = ""

        self._last_brush = None
        self._last_scraper = None
        self._last_vac = None
        self._last_water = None
        self._last_profile = None

        self._vacuum_job_id = 0

        self._motion = MotionState()
        self._il_cfg = InterlockConfig()
        self._interlock_active: bool = False
        self._interlock_reason: str = ""

        self._feedback = FeedbackState()
        self._mcore_connected: bool = False
        self._mcore_connected_seen: bool = False
        self._reapply_on_reconnect: bool = False

        self._cmd_ts = {
            "brush": 0.0,
            "scraper": 0.0,
            "vacuum": 0.0,
            "water": 0.0,
        }
        self._cmd_state = {
            "brush": None,
            "scraper": None,
            "vacuum": None,
            "water": None,
        }
        self._retry_until_ts = {
            "brush": 0.0,
            "scraper": 0.0,
            "vacuum": 0.0,
            "water": 0.0,
        }

        self._mcore_connected_topic = str(rospy.get_param("~mcore_connected_topic", "/mcore_tcp_bridge/connected") or "/mcore_tcp_bridge/connected")
        self._combined_status_topic = str(rospy.get_param("~combined_status_topic", "/combined_status") or "/combined_status")
        self._actuator_reconcile_hz = max(0.2, float(rospy.get_param("~actuator_reconcile_hz", 2.0)))
        self._actuator_active_refresh_s = max(0.2, float(rospy.get_param("~actuator_active_refresh_s", 1.0)))
        self._actuator_transition_retry_window_s = max(0.0, float(rospy.get_param("~actuator_transition_retry_window_s", 1.0)))
        self._actuator_retry_interval_s = max(0.1, float(rospy.get_param("~actuator_retry_interval_s", 0.35)))
        self._actuator_feedback_retry_timeout_s = max(
            self._actuator_retry_interval_s,
            float(rospy.get_param("~actuator_feedback_retry_timeout_s", 0.6)),
        )
        self._actuator_feedback_stale_s = max(
            self._actuator_feedback_retry_timeout_s,
            float(rospy.get_param("~actuator_feedback_stale_s", 2.0)),
        )

        try:
            rospy.Subscriber(self._mcore_connected_topic, Bool, self._on_mcore_connected, queue_size=10)
        except Exception as exc:
            rospy.logwarn("[CLEAN] subscribe mcore connected failed: topic=%s err=%s", self._mcore_connected_topic, str(exc))
        try:
            rospy.Subscriber(self._combined_status_topic, CombinedStatus, self._on_combined_status, queue_size=10)
        except Exception as exc:
            rospy.logwarn("[CLEAN] subscribe combined_status failed: topic=%s err=%s", self._combined_status_topic, str(exc))

        self._stop_evt = threading.Event()
        self._reconcile_thread = threading.Thread(target=self._reconcile_loop, daemon=True)
        self._reconcile_thread.start()
        rospy.loginfo(
            "[CLEAN] reliability enabled connected_topic=%s status_topic=%s hz=%.2f active_refresh=%.2fs retry_interval=%.2fs retry_window=%.2fs",
            self._mcore_connected_topic,
            self._combined_status_topic,
            self._actuator_reconcile_hz,
            self._actuator_active_refresh_s,
            self._actuator_retry_interval_s,
            self._actuator_transition_retry_window_s,
        )

    # ---------- profile / mode ----------
    def set_profile(self, profile_name: str):
        with self._lock:
            self._profile_name = str(profile_name or "")

    def set_mode(self, clean_mode: str):
        with self._lock:
            self._clean_mode = str(clean_mode or "")

    def get_profile_mode(self):
        with self._lock:
            return self._profile_name, self._clean_mode

    # ---------- motion / interlock ----------
    def configure_interlock(
        self,
        *,
        enable: bool,
        max_lin_mps: float,
        max_ang_rps: float,
        mask_cleaning: bool = True,
        mask_water: bool = True,
        mask_vacuum: bool = False,
    ):
        with self._lock:
            self._il_cfg.enable = bool(enable)
            self._il_cfg.max_lin_mps = float(max_lin_mps)
            self._il_cfg.max_ang_rps = float(max_ang_rps)
            self._il_cfg.mask_cleaning = bool(mask_cleaning)
            self._il_cfg.mask_water = bool(mask_water)
            self._il_cfg.mask_vacuum = bool(mask_vacuum)

    def update_motion(self, v_mps: float, w_rps: float, ts: float = 0.0):
        if ts <= 0.0:
            ts = time.time()
        with self._lock:
            self._motion.v_mps = float(v_mps)
            self._motion.w_rps = float(w_rps)
            self._motion.ts = float(ts)
            active_req = bool(self.des.brush_on or self.des.scraper_on or self.des.vacuum_on or self.des.water_on)
        if active_req:
            self.apply_full()

    def get_desired_channels(self):
        with self._lock:
            cleaning_req = bool(self.des.brush_on or self.des.scraper_on)
            return cleaning_req, bool(self.des.vacuum_on), bool(self.des.water_on), bool(self.des.water_off_latched)

    def get_interlock(self):
        with self._lock:
            return bool(self._interlock_active), str(self._interlock_reason or ""), float(self._motion.v_mps), float(self._motion.w_rps)

    def shutdown(self):
        self._stop_evt.set()
        t = getattr(self, "_reconcile_thread", None)
        if t is not None and t.is_alive():
            t.join(timeout=0.3)

    def _on_mcore_connected(self, msg: Bool):
        connected = bool(msg.data)
        first = False
        prev = False
        with self._lock:
            first = not self._mcore_connected_seen
            prev = bool(self._mcore_connected) if self._mcore_connected_seen else False
            self._mcore_connected_seen = True
            self._mcore_connected = connected
            if connected and (first or not prev):
                self._reapply_on_reconnect = True
        if connected and (first or not prev):
            rospy.logwarn("[CLEAN] mcore connected -> actuator replay armed")
        elif (not connected) and prev:
            rospy.logwarn("[CLEAN] mcore disconnected -> waiting for reconnect")

    def _on_combined_status(self, msg: CombinedStatus):
        with self._lock:
            self._feedback.brush_position = int(msg.brush_position)
            self._feedback.scraper_position = int(msg.scraper_position)
            self._feedback.ts = time.time()

    def _eval_interlock(self, brush_on: bool, scraper_on: bool, vacuum_on: bool, water_on: bool):
        with self._lock:
            cfg = InterlockConfig(**self._il_cfg.__dict__)
            v = float(self._motion.v_mps)
            w = float(self._motion.w_rps)

        req_any = bool(brush_on or scraper_on or vacuum_on or water_on)
        if not (cfg.enable and req_any):
            with self._lock:
                self._interlock_active = False
                self._interlock_reason = ""
            return brush_on, scraper_on, vacuum_on, water_on

        too_fast = (abs(v) > cfg.max_lin_mps) or (abs(w) > cfg.max_ang_rps)
        if not too_fast:
            with self._lock:
                self._interlock_active = False
                self._interlock_reason = ""
            return brush_on, scraper_on, vacuum_on, water_on

        masked_brush = (False if (cfg.mask_cleaning and brush_on) else brush_on)
        masked_scraper = (False if (cfg.mask_cleaning and scraper_on) else scraper_on)
        masked_vac = (False if (cfg.mask_vacuum and vacuum_on) else vacuum_on)
        masked_water = (False if (cfg.mask_water and water_on) else water_on)

        active = (
            masked_brush != brush_on
            or masked_scraper != scraper_on
            or masked_vac != vacuum_on
            or masked_water != water_on
        )
        reason = f"speed_interlock v={v:.2f}mps w={w:.2f}rps lim=({cfg.max_lin_mps:.2f},{cfg.max_ang_rps:.2f})"
        with self._lock:
            self._interlock_active = bool(active)
            self._interlock_reason = str(reason if active else "")
        return masked_brush, masked_scraper, masked_vac, masked_water

    def _snapshot_effective_state(self):
        with self._lock:
            profile = self._profile_name
            brush_on = bool(self.des.brush_on)
            scraper_on = bool(self.des.scraper_on)
            vacuum_on = bool(self.des.vacuum_on)
            water_on = bool(self.des.water_on) and (not bool(self.des.water_off_latched))
        brush_on, scraper_on, vacuum_on, water_on = self._eval_interlock(brush_on, scraper_on, vacuum_on, water_on)
        return profile, bool(brush_on), bool(scraper_on), bool(vacuum_on), bool(water_on)

    def _note_command(self, channel: str, state: bool, *, transition: bool):
        now = time.time()
        with self._lock:
            self._cmd_ts[channel] = now
            self._cmd_state[channel] = bool(state)
            if transition and self._actuator_transition_retry_window_s > 1e-3:
                self._retry_until_ts[channel] = now + self._actuator_transition_retry_window_s

    def _dispatch_channel(self, channel: str, enabled: bool, *, transition: bool):
        if channel == "brush":
            if enabled:
                self.act.brush_on()
            else:
                self.act.brush_off()
        elif channel == "scraper":
            if enabled:
                self.act.scraper_on()
            else:
                self.act.scraper_off()
        elif channel == "vacuum":
            if enabled:
                self.act.vacuum_on()
            else:
                self.act.vacuum_off()
        elif channel == "water":
            if enabled:
                self.act.water_on()
            else:
                self.act.water_off()
        else:
            raise ValueError(f"unknown channel: {channel}")
        self._note_command(channel, enabled, transition=transition)

    def _force_reapply_state(self, *, reason: str):
        profile, brush_on, scraper_on, vacuum_on, water_on = self._snapshot_effective_state()
        rospy.logwarn(
            "[CLEAN] replay state reason=%s profile=%s brush=%s scraper=%s vacuum=%s water=%s",
            str(reason),
            str(profile or ""),
            str(brush_on),
            str(scraper_on),
            str(vacuum_on),
            str(water_on),
        )
        with self._dispatch_lock:
            if profile:
                try:
                    self.act.apply_profile(profile)
                except Exception:
                    pass
            try:
                self._dispatch_channel("scraper", scraper_on, transition=True)
            except Exception:
                pass
            try:
                self._dispatch_channel("brush", brush_on, transition=True)
            except Exception:
                pass
            try:
                self._dispatch_channel("vacuum", vacuum_on, transition=True)
            except Exception:
                pass
            try:
                self._dispatch_channel("water", water_on, transition=True)
            except Exception:
                pass

    def _feedback_matches(self, channel: str, desired_on: bool, *, brush_pos: int, scraper_pos: int, feedback_age_s: float) -> bool:
        if feedback_age_s > self._actuator_feedback_stale_s:
            return True
        if channel == "brush":
            if brush_pos < 0:
                return True
            return bool(brush_pos == 1) if desired_on else bool(brush_pos == 0)
        if channel == "scraper":
            if scraper_pos < 0:
                return True
            return bool(scraper_pos == 1) if desired_on else bool(scraper_pos == 0)
        return True

    def _reconcile_loop(self):
        period_s = 1.0 / max(0.2, float(self._actuator_reconcile_hz))
        while not rospy.is_shutdown() and not self._stop_evt.is_set():
            try:
                self._reconcile_once()
            except Exception as exc:
                rospy.logwarn_throttle(2.0, "[CLEAN] reconcile failed: %s", str(exc))
            self._stop_evt.wait(period_s)

    def _reconcile_once(self):
        with self._lock:
            connected = bool(self._mcore_connected)
            replay = bool(self._reapply_on_reconnect)
            if replay:
                self._reapply_on_reconnect = False
            brush_pos = int(self._feedback.brush_position)
            scraper_pos = int(self._feedback.scraper_position)
            feedback_age_s = time.time() - float(self._feedback.ts or 0.0) if self._feedback.ts > 0.0 else 1e9
            cmd_ts = dict(self._cmd_ts)
            cmd_state = dict(self._cmd_state)
            retry_until_ts = dict(self._retry_until_ts)

        if replay and connected:
            self._force_reapply_state(reason="mcore_reconnect")
            return
        if not connected:
            return

        profile, brush_on, scraper_on, vacuum_on, water_on = self._snapshot_effective_state()
        desired = {
            "brush": bool(brush_on),
            "scraper": bool(scraper_on),
            "vacuum": bool(vacuum_on),
            "water": bool(water_on),
        }
        now = time.time()
        reapply = []

        for channel, enabled in desired.items():
            last_state = cmd_state.get(channel)
            last_ts = float(cmd_ts.get(channel, 0.0) or 0.0)
            retry_until = float(retry_until_ts.get(channel, 0.0) or 0.0)
            age = now - last_ts if last_ts > 0.0 else 1e9

            if enabled:
                if last_state is not True:
                    reapply.append((channel, True, "state_desync"))
                    continue
                if channel in ("brush", "scraper") and (not self._feedback_matches(channel, True, brush_pos=brush_pos, scraper_pos=scraper_pos, feedback_age_s=feedback_age_s)):
                    if age >= self._actuator_feedback_retry_timeout_s:
                        reapply.append((channel, True, "feedback_mismatch"))
                        continue
                if channel in ("vacuum", "water") and age >= self._actuator_active_refresh_s:
                    reapply.append((channel, True, "active_refresh"))
            else:
                if last_state is not False:
                    reapply.append((channel, False, "state_desync"))
                    continue
                if channel in ("brush", "scraper") and (not self._feedback_matches(channel, False, brush_pos=brush_pos, scraper_pos=scraper_pos, feedback_age_s=feedback_age_s)):
                    if age >= self._actuator_feedback_retry_timeout_s:
                        reapply.append((channel, False, "feedback_mismatch"))
                        continue
                if retry_until > now and age >= self._actuator_retry_interval_s:
                    reapply.append((channel, False, "transition_retry"))

        if not reapply:
            return

        with self._dispatch_lock:
            for channel, enabled, reason in reapply:
                if reason in ("state_desync", "feedback_mismatch"):
                    rospy.logwarn_throttle(
                        2.0,
                        "[CLEAN] reapply channel=%s enabled=%s reason=%s brush_pos=%d scraper_pos=%d feedback_age=%.2fs",
                        channel,
                        str(enabled),
                        reason,
                        brush_pos,
                        scraper_pos,
                        feedback_age_s,
                    )
                try:
                    self._dispatch_channel(channel, enabled, transition=False)
                except Exception:
                    pass

    # ---------- high-level entry points ----------
    def zone_end(self):
        with self._lock:
            self.des.brush_on = False
            self.des.scraper_on = False
            self.des.vacuum_on = False
            self.des.water_on = False
            self.des.water_off_latched = False
        self.apply_full()

    def latch_water_off(self):
        with self._lock:
            self.des.water_off_latched = True
            self.des.water_on = False
        self.apply_full()

    def enter_transit_off(self, *, water_off_latched: bool):
        with self._lock:
            self.des.brush_on = False
            self.des.scraper_on = False
            self.des.vacuum_on = False
            self.des.water_on = False
            self.des.water_off_latched = bool(water_off_latched)
        self.apply_full()

    def enter_follow(self, *, water_off_latched: bool):
        with self._lock:
            mode = (self._clean_mode or "").strip().lower()
        brush_on, scraper_on, vacuum_on, water_on = self._mode_to_recipe(mode, water_off_latched=bool(water_off_latched))

        with self._lock:
            self.des.brush_on = bool(brush_on)
            self.des.scraper_on = bool(scraper_on)
            self.des.vacuum_on = bool(vacuum_on)
            self.des.water_on = bool(water_on)
            self.des.water_off_latched = bool(water_off_latched)

        self.apply_full()

    def enter_ai_spot(self, spot_mode: str, *, water_off_latched: bool):
        with self._lock:
            mode = (spot_mode or "").strip().lower()
        brush_on, scraper_on, vacuum_on, water_on = self._mode_to_recipe(mode, water_off_latched=bool(water_off_latched))

        with self._lock:
            self.des.brush_on = bool(brush_on)
            self.des.scraper_on = bool(scraper_on)
            self.des.vacuum_on = bool(vacuum_on)
            self.des.water_on = bool(water_on)
            self.des.water_off_latched = bool(water_off_latched)

        self.apply_full()

    # ---------- internal helpers ----------
    def _mode_to_recipe(self, mode: str, *, water_off_latched: bool):
        m = (mode or "").strip().lower()
        if not m:
            m = "scrub"

        if m in ["inspect", "inspection", "patrol", "巡检", "eco_inspect", "inspect_eco"]:
            return (False, False, False, False)

        if m in ["vacuum", "vac", "vacuum_only", "suction", "suction_only"]:
            return (False, True, True, False)

        if m in ["dry", "sweep", "sweep_dry", "dry_sweep"]:
            return (True, True, True, False)

        if m in ["scrub", "wet", "wash", "wet_scrub", "deep", "deep_clean"]:
            return (True, True, True, (not water_off_latched))

        rospy.logwarn_throttle(2.0, "[CLEAN] unknown mode='%s', fallback to 'scrub'", m)
        return (True, True, True, (not water_off_latched))

    # ---------- apply & debounce ----------
    def apply_full(self):
        profile, brush_on, scraper_on, vacuum_on, water_on = self._snapshot_effective_state()

        with self._lock:
            prof_changed = (profile != self._last_profile)
            brush_changed = (brush_on != self._last_brush)
            scraper_changed = (scraper_on != self._last_scraper)
            vac_changed = (vacuum_on != self._last_vac)
            water_changed = (water_on != self._last_water)

            self._last_profile = profile
            self._last_brush = brush_on
            self._last_scraper = scraper_on
            self._last_vac = vacuum_on
            self._last_water = water_on

        # Apply in a fixed order:
        # - ON : scraper -> brush -> vacuum -> water
        # - OFF: water -> brush -> scraper -> vacuum
        with self._dispatch_lock:
            if prof_changed and profile:
                try:
                    self.act.apply_profile(profile)
                except Exception:
                    pass
            if scraper_changed and scraper_on:
                try:
                    self._dispatch_channel("scraper", True, transition=True)
                except Exception:
                    pass

            if brush_changed and brush_on:
                try:
                    self._dispatch_channel("brush", True, transition=True)
                except Exception:
                    pass

            if vac_changed and vacuum_on:
                try:
                    self._dispatch_channel("vacuum", True, transition=True)
                except Exception:
                    pass

            if water_changed and water_on:
                try:
                    self._dispatch_channel("water", True, transition=True)
                except Exception:
                    pass

            if water_changed and (not water_on):
                try:
                    self._dispatch_channel("water", False, transition=True)
                except Exception:
                    pass

            if brush_changed and (not brush_on):
                try:
                    self._dispatch_channel("brush", False, transition=True)
                except Exception:
                    pass

            if scraper_changed and (not scraper_on):
                try:
                    self._dispatch_channel("scraper", False, transition=True)
                except Exception:
                    pass

            if vac_changed and (not vacuum_on):
                try:
                    self._dispatch_channel("vacuum", False, transition=True)
                except Exception:
                    pass

    # ---------- stop policies ----------
    def pause_stop(self, vacuum_delay_s: float):
        with self._lock:
            self.des.water_on = False
            self.des.brush_on = False
            self.des.scraper_on = False
            self._vacuum_job_id += 1
            job_id = self._vacuum_job_id

        self.apply_full()

        def _late_vacuum_off():
            if vacuum_delay_s and vacuum_delay_s > 1e-3:
                time.sleep(float(vacuum_delay_s))
            with self._lock:
                if job_id != self._vacuum_job_id:
                    return
                self.des.vacuum_on = False
            self.apply_full()

        threading.Thread(target=_late_vacuum_off, daemon=True).start()

    def cancel_stop(self, vacuum_delay_s: float):
        with self._lock:
            self.des.water_on = False
            self.des.brush_on = False
            self.des.scraper_on = False
            self._vacuum_job_id += 1
            job_id = self._vacuum_job_id

        self.apply_full()

        def _late_vacuum_off():
            if vacuum_delay_s and vacuum_delay_s > 1e-3:
                time.sleep(float(vacuum_delay_s))
            with self._lock:
                if job_id != self._vacuum_job_id:
                    return
                self.des.vacuum_on = False
            self.apply_full()

        threading.Thread(target=_late_vacuum_off, daemon=True).start()

    def fail_stop(self):
        with self._lock:
            self._vacuum_job_id += 1
            self.des.brush_on = False
            self.des.scraper_on = False
            self.des.vacuum_on = False
            self.des.water_on = False
        self.apply_full()

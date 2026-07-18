#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import threading
import time
import uuid
from datetime import datetime
from typing import Any, Dict, Optional

import rospy
from geometry_msgs.msg import Twist
from robot_platform_msgs.msg import ControlStation
from sensor_msgs.msg import BatteryState
from std_msgs.msg import Bool, String, UInt32
from std_srvs.srv import Trigger, TriggerResponse

try:
    from coverage_msgs.msg import RunProgress
except Exception:
    RunProgress = None


ACTIVE_DOCK_STATES = {
    "RUNNING",
    "LOCK_DOCK_POSE",
    "SEARCH_DOCK_POSE",
    "PRECISE_DOCKING",
    "WAIT_STATION_IN_PLACE",
    "SEARCH_STATION_IN_PLACE",
    "MECHANICAL_CONNECT",
    "DRAINING",
    "REFILLING",
    "CHARGE_CMD_SENT",
    "CHARGE_CONFIRMED",
    "DISABLE_CHARGING",
    "MECHANICAL_DISCONNECT",
    "READY_TO_EXIT",
    "EXIT_BACKING",
}

CHARGE_SEEN_STATES = {
    "CHARGE_CMD_SENT",
    "CHARGE_CONFIRMED",
    "DISABLE_CHARGING",
    "READY_TO_EXIT",
    "EXIT_BACKING",
}

RECOVERY_ELIGIBLE_STATES = {
    "CHARGE_CMD_SENT",
    "CHARGE_CONFIRMED",
}


def _now() -> float:
    return time.time()


def _iso(ts: Optional[float] = None) -> str:
    return datetime.fromtimestamp(float(ts if ts is not None else _now())).isoformat(timespec="seconds")


def _soc_from_battery(msg: Optional[BatteryState]) -> Optional[float]:
    if msg is None:
        return None
    try:
        soc = float(msg.percentage)
    except Exception:
        return None
    if soc > 1.0 + 1e-6:
        soc = soc / 100.0
    return max(0.0, min(1.0, soc))


def _atomic_write_json(path: str, data: Dict[str, Any]) -> None:
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    tmp_path = "%s.tmp.%d.%s" % (path, os.getpid(), uuid.uuid4().hex)
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.write("\n")
    os.replace(tmp_path, path)


class AutoChargeMonitor:
    def __init__(self) -> None:
        self.dock_supply_state_topic = str(rospy.get_param("~dock_supply_state_topic", "/dock_supply/state"))
        self.task_state_topic = str(rospy.get_param("~task_state_topic", "/coverage_task_manager/state"))
        self.task_event_topic = str(rospy.get_param("~task_event_topic", "/coverage_task_manager/event"))
        self.executor_state_topic = str(rospy.get_param("~executor_state_topic", "/coverage_executor/state"))
        self.executor_progress_topic = str(rospy.get_param("~executor_progress_topic", "/coverage_executor/run_progress"))
        self.battery_topic = str(rospy.get_param("~battery_topic", "/battery_state"))
        self.state_path = str(rospy.get_param("~state_path", "/data/coverage/auto_charge_monitor_state.json"))
        self.event_log_path = str(rospy.get_param("~event_log_path", "/data/coverage/auto_charge_monitor_events.jsonl"))
        self.count_auto_only = bool(rospy.get_param("~count_auto_only", True))
        self.reset_on_start = bool(rospy.get_param("~reset_on_start", False))
        self.max_recent_cycles = max(1, int(rospy.get_param("~max_recent_cycles", 200)))
        self.publish_period_s = max(0.5, float(rospy.get_param("~publish_period_s", 5.0)))
        self.recovery_enable = bool(rospy.get_param("~recovery_enable", True))
        self.recovery_strategy = str(rospy.get_param("~recovery_strategy", "redock")).strip().lower() or "redock"
        if self.recovery_strategy not in ("redock", "contact_jog"):
            rospy.logwarn(
                "[AUTO_CHARGE_MON] invalid recovery_strategy=%s, fallback to redock",
                self.recovery_strategy,
            )
            self.recovery_strategy = "redock"
        self.recovery_no_soc_change_timeout_s = max(
            1.0, float(rospy.get_param("~recovery_no_soc_change_timeout_s", 180.0))
        )
        self.recovery_min_soc_delta = max(0.0, float(rospy.get_param("~recovery_min_soc_delta", 0.001)))
        self.recovery_max_attempts_per_cycle = max(
            0, int(rospy.get_param("~recovery_max_attempts_per_cycle", 2))
        )
        self.recovery_back_distance_m = max(0.0, float(rospy.get_param("~recovery_back_distance_m", 0.20)))
        self.recovery_forward_distance_m = max(0.0, float(rospy.get_param("~recovery_forward_distance_m", 0.205)))
        self.recovery_linear_speed_mps = max(
            0.01, abs(float(rospy.get_param("~recovery_linear_speed_mps", 0.03)))
        )
        self.recovery_cmd_hz = max(2.0, float(rospy.get_param("~recovery_cmd_hz", 10.0)))
        self.recovery_settle_s = max(0.0, float(rospy.get_param("~recovery_settle_s", 2.0)))
        self.recovery_toggle_charge = bool(rospy.get_param("~recovery_toggle_charge", False))
        self.recovery_charge_cmd_repeat = max(1, int(rospy.get_param("~recovery_charge_cmd_repeat", 2)))
        self.recovery_charge_cmd_interval_s = max(
            0.0, float(rospy.get_param("~recovery_charge_cmd_interval_s", 0.08))
        )
        self.recovery_station_charge_enable_repeat = max(
            1, int(rospy.get_param("~recovery_station_charge_enable_repeat", 3))
        )
        self.recovery_station_charge_enable_interval_s = max(
            0.0, float(rospy.get_param("~recovery_station_charge_enable_interval_s", 3.0))
        )
        self.recovery_cmd_vel_topic = str(rospy.get_param("~recovery_cmd_vel_topic", "/cmd_vel"))
        self.recovery_mcore_charge_topic = str(
            rospy.get_param("~recovery_mcore_charge_topic", "/mcore/charge_enable")
        )
        self.recovery_station_control_topic = str(
            rospy.get_param("~recovery_station_control_topic", "/station/control")
        )
        self.recovery_redock_service = str(
            rospy.get_param("~recovery_redock_service", "/coverage_task_manager/auto_charge_redock")
        )
        self.recovery_redock_service_timeout_s = max(
            0.5, float(rospy.get_param("~recovery_redock_service_timeout_s", 3.0))
        )
        self.recovery_exhausted_service = str(
            rospy.get_param(
                "~recovery_exhausted_service",
                "/coverage_task_manager/auto_charge_recovery_exhausted",
            )
        )
        self.recovery_exhausted_service_timeout_s = max(
            0.5, float(rospy.get_param("~recovery_exhausted_service_timeout_s", 3.0))
        )

        self._lock = threading.RLock()
        self._last_dock_state = ""
        self._task_state = ""
        self._executor_state = ""
        self._last_task_event = ""
        self._battery: Optional[BatteryState] = None
        self._battery_ts = 0.0
        self._progress: Optional[Any] = None
        self._progress_ts = 0.0
        self._recovery_running = False

        self._state = self._load_or_init_state(reset=self.reset_on_start)

        self.count_pub = rospy.Publisher("~completed_count", UInt32, queue_size=1, latch=True)
        self.attempt_pub = rospy.Publisher("~attempt_count", UInt32, queue_size=1, latch=True)
        self.summary_pub = rospy.Publisher("~summary", String, queue_size=1, latch=True)
        self.event_pub = rospy.Publisher("~event", String, queue_size=20)
        self.cmd_vel_pub = rospy.Publisher(self.recovery_cmd_vel_topic, Twist, queue_size=10)
        self.mcore_charge_pub = rospy.Publisher(self.recovery_mcore_charge_topic, Bool, queue_size=10)
        self.station_control_pub = rospy.Publisher(self.recovery_station_control_topic, ControlStation, queue_size=10)
        self.redock_recovery_cli = rospy.ServiceProxy(self.recovery_redock_service, Trigger)
        self.recovery_exhausted_cli = rospy.ServiceProxy(self.recovery_exhausted_service, Trigger)

        rospy.Subscriber(self.dock_supply_state_topic, String, self._on_dock_supply_state, queue_size=20)
        rospy.Subscriber(self.task_state_topic, String, self._on_task_state, queue_size=20)
        rospy.Subscriber(self.task_event_topic, String, self._on_task_event, queue_size=50)
        rospy.Subscriber(self.executor_state_topic, String, self._on_executor_state, queue_size=20)
        rospy.Subscriber(self.battery_topic, BatteryState, self._on_battery, queue_size=10)
        if RunProgress is not None:
            rospy.Subscriber(self.executor_progress_topic, RunProgress, self._on_progress, queue_size=10)

        rospy.Service("~reset", Trigger, self._srv_reset)
        rospy.Service("~snapshot", Trigger, self._srv_snapshot)
        rospy.Timer(rospy.Duration(self.publish_period_s), self._on_timer)

        self._persist()
        self._publish_summary()
        rospy.loginfo(
            "[AUTO_CHARGE_MON] started state_path=%s event_log=%s count_auto_only=%s reset_on_start=%s recovery_strategy=%s max_recovery=%d",
            self.state_path,
            self.event_log_path,
            str(self.count_auto_only),
            str(self.reset_on_start),
            self.recovery_strategy,
            int(self.recovery_max_attempts_per_cycle),
        )

    def _new_state(self) -> Dict[str, Any]:
        ts = _now()
        return {
            "schema_version": 1,
            "session_id": uuid.uuid4().hex,
            "session_started_unix": ts,
            "session_started_iso": _iso(ts),
            "updated_unix": ts,
            "updated_iso": _iso(ts),
            "attempt_count": 0,
            "completed_count": 0,
            "failed_count": 0,
            "canceled_count": 0,
            "recovery_attempt_count": 0,
            "recovery_success_count": 0,
            "recovery_failed_count": 0,
            "current_cycle": None,
            "recent_cycles": [],
            "last_event": None,
        }

    def _load_or_init_state(self, *, reset: bool) -> Dict[str, Any]:
        if reset:
            return self._new_state()
        try:
            with open(self.state_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, dict):
                raise ValueError("state file root is not object")
            data.setdefault("attempt_count", 0)
            data.setdefault("completed_count", 0)
            data.setdefault("failed_count", 0)
            data.setdefault("canceled_count", 0)
            data.setdefault("recovery_attempt_count", 0)
            data.setdefault("recovery_success_count", 0)
            data.setdefault("recovery_failed_count", 0)
            data.setdefault("current_cycle", None)
            data.setdefault("recent_cycles", [])
            data.setdefault("session_id", uuid.uuid4().hex)
            data.setdefault("session_started_unix", _now())
            data.setdefault("session_started_iso", _iso(float(data.get("session_started_unix") or _now())))
            return data
        except Exception as e:
            if os.path.exists(self.state_path):
                rospy.logwarn("[AUTO_CHARGE_MON] cannot load state file %s: %s", self.state_path, str(e))
            return self._new_state()

    def _snapshot_context(self) -> Dict[str, Any]:
        soc = _soc_from_battery(self._battery)
        progress = {}
        if self._progress is not None:
            for name in ("run_id", "zone_id", "plan_id", "state", "progress_pct", "progress_0_1", "path_s"):
                try:
                    progress[name] = getattr(self._progress, name)
                except Exception:
                    pass
        return {
            "task_state": self._task_state,
            "executor_state": self._executor_state,
            "last_task_event": self._last_task_event,
            "battery_soc": soc,
            "battery_age_s": max(0.0, _now() - self._battery_ts) if self._battery_ts > 0.0 else None,
            "progress": progress,
            "progress_age_s": max(0.0, _now() - self._progress_ts) if self._progress_ts > 0.0 else None,
        }

    def _auto_context(self) -> bool:
        st = str(self._task_state or "").strip().upper()
        if st.startswith("AUTO_"):
            return True
        event = str(self._last_task_event or "").strip().upper()
        return any(key in event for key in ("BATTERY_LOW", "SUPPLY_START", "AUTO_SUPPLY", "AUTO_CHARG"))

    def _should_track_cycle(self) -> bool:
        return (not self.count_auto_only) or self._auto_context()

    def _start_cycle(self, dock_state: str) -> None:
        ts = _now()
        self._state["attempt_count"] = int(self._state.get("attempt_count", 0) or 0) + 1
        cycle = {
            "cycle_id": uuid.uuid4().hex,
            "attempt_index": int(self._state["attempt_count"]),
            "started_unix": ts,
            "started_iso": _iso(ts),
            "finished_unix": None,
            "finished_iso": "",
            "duration_s": None,
            "status": "ACTIVE",
            "first_dock_state": dock_state,
            "last_dock_state": dock_state,
            "charge_command_seen": dock_state in CHARGE_SEEN_STATES,
            "charge_confirmed_seen": dock_state in ("CHARGE_CONFIRMED", "DISABLE_CHARGING", "READY_TO_EXIT", "EXIT_BACKING"),
            "ready_to_exit_seen": dock_state in ("READY_TO_EXIT", "EXIT_BACKING"),
            "exit_seen": dock_state == "EXIT_BACKING",
            "auto_context": self._auto_context(),
            "start_context": self._snapshot_context(),
            "end_context": None,
            "recovery_attempts": 0,
            "recovery_events": [],
        }
        if cycle["charge_command_seen"]:
            cycle["charge_started_unix"] = ts
            cycle["charge_started_iso"] = _iso(ts)
            self._ensure_charge_watch(cycle)
        self._state["current_cycle"] = cycle
        self._record_event("charge_attempt_start", cycle=cycle, dock_state=dock_state)

    def _ensure_charge_watch(self, cycle: Dict[str, Any]) -> None:
        if cycle.get("charge_watch_started_unix") is not None:
            return
        soc = _soc_from_battery(self._battery)
        if soc is None:
            return
        ts = _now()
        cycle["charge_watch_started_unix"] = ts
        cycle["charge_watch_started_iso"] = _iso(ts)
        cycle["charge_watch_start_soc"] = soc
        cycle["charge_watch_last_reset_reason"] = "charge_command_seen"

    def _update_cycle_flags(self, dock_state: str) -> None:
        cycle = self._state.get("current_cycle")
        if not isinstance(cycle, dict):
            return
        cycle["last_dock_state"] = dock_state
        if dock_state in CHARGE_SEEN_STATES:
            cycle["charge_command_seen"] = True
            if "charge_started_unix" not in cycle:
                ts = _now()
                cycle["charge_started_unix"] = ts
                cycle["charge_started_iso"] = _iso(ts)
            if bool(cycle.get("redock_recovery_pending_charge_watch_reset")):
                self._reset_charge_watch(cycle, "redock_charge_command_seen")
                cycle["redock_recovery_pending_charge_watch_reset"] = False
                cycle["redock_recovery_active"] = False
            self._ensure_charge_watch(cycle)
        if dock_state in ("CHARGE_CONFIRMED", "DISABLE_CHARGING", "READY_TO_EXIT", "EXIT_BACKING"):
            cycle["charge_confirmed_seen"] = True
            if "charge_confirmed_unix" not in cycle:
                ts = _now()
                cycle["charge_confirmed_unix"] = ts
                cycle["charge_confirmed_iso"] = _iso(ts)
        if dock_state in ("READY_TO_EXIT", "EXIT_BACKING"):
            cycle["ready_to_exit_seen"] = True
        if dock_state == "EXIT_BACKING":
            cycle["exit_seen"] = True

    def _finish_cycle(self, dock_state: str) -> None:
        cycle = self._state.get("current_cycle")
        if not isinstance(cycle, dict):
            return
        ts = _now()
        cycle["last_dock_state"] = dock_state
        cycle["finished_unix"] = ts
        cycle["finished_iso"] = _iso(ts)
        cycle["duration_s"] = max(0.0, ts - float(cycle.get("started_unix") or ts))
        cycle["end_context"] = self._snapshot_context()

        if dock_state == "DONE" and bool(cycle.get("charge_command_seen")):
            cycle["status"] = "COMPLETED"
            self._state["completed_count"] = int(self._state.get("completed_count", 0) or 0) + 1
            event_name = "charge_completed"
        elif dock_state == "CANCELED":
            cycle["status"] = "CANCELED"
            self._state["canceled_count"] = int(self._state.get("canceled_count", 0) or 0) + 1
            event_name = "charge_canceled"
        else:
            cycle["status"] = "FAILED"
            self._state["failed_count"] = int(self._state.get("failed_count", 0) or 0) + 1
            event_name = "charge_failed"

        recent = list(self._state.get("recent_cycles") or [])
        recent.append(cycle)
        self._state["recent_cycles"] = recent[-self.max_recent_cycles :]
        self._state["current_cycle"] = None
        self._record_event(event_name, cycle=cycle, dock_state=dock_state)

    def _record_event(self, name: str, *, cycle: Optional[Dict[str, Any]], dock_state: str) -> None:
        event = {
            "event": name,
            "time_unix": _now(),
            "time_iso": _iso(),
            "session_id": self._state.get("session_id", ""),
            "dock_state": dock_state,
            "attempt_count": int(self._state.get("attempt_count", 0) or 0),
            "completed_count": int(self._state.get("completed_count", 0) or 0),
            "failed_count": int(self._state.get("failed_count", 0) or 0),
            "canceled_count": int(self._state.get("canceled_count", 0) or 0),
            "recovery_attempt_count": int(self._state.get("recovery_attempt_count", 0) or 0),
            "recovery_success_count": int(self._state.get("recovery_success_count", 0) or 0),
            "recovery_failed_count": int(self._state.get("recovery_failed_count", 0) or 0),
            "cycle_id": str((cycle or {}).get("cycle_id", "")),
            "attempt_index": int((cycle or {}).get("attempt_index", 0) or 0),
            "context": self._snapshot_context(),
        }
        self._state["last_event"] = event
        try:
            directory = os.path.dirname(self.event_log_path)
            if directory:
                os.makedirs(directory, exist_ok=True)
            with open(self.event_log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")
        except Exception as e:
            rospy.logwarn("[AUTO_CHARGE_MON] failed to append event log %s: %s", self.event_log_path, str(e))
        try:
            self.event_pub.publish(String(data=json.dumps(event, ensure_ascii=False, sort_keys=True)))
        except Exception:
            pass
        rospy.loginfo(
            "[AUTO_CHARGE_MON] %s attempts=%d completed=%d failed=%d canceled=%d dock=%s",
            name,
            int(self._state.get("attempt_count", 0) or 0),
            int(self._state.get("completed_count", 0) or 0),
            int(self._state.get("failed_count", 0) or 0),
            int(self._state.get("canceled_count", 0) or 0),
            dock_state,
        )

    def _persist(self) -> None:
        self._state["updated_unix"] = _now()
        self._state["updated_iso"] = _iso()
        _atomic_write_json(self.state_path, self._state)

    def _summary(self) -> Dict[str, Any]:
        return {
            "session_id": self._state.get("session_id", ""),
            "session_started_iso": self._state.get("session_started_iso", ""),
            "updated_iso": _iso(),
            "attempt_count": int(self._state.get("attempt_count", 0) or 0),
            "completed_count": int(self._state.get("completed_count", 0) or 0),
            "failed_count": int(self._state.get("failed_count", 0) or 0),
            "canceled_count": int(self._state.get("canceled_count", 0) or 0),
            "recovery_attempt_count": int(self._state.get("recovery_attempt_count", 0) or 0),
            "recovery_success_count": int(self._state.get("recovery_success_count", 0) or 0),
            "recovery_failed_count": int(self._state.get("recovery_failed_count", 0) or 0),
            "recovery_running": bool(self._recovery_running),
            "current_cycle": self._state.get("current_cycle"),
            "last_event": self._state.get("last_event"),
            "context": self._snapshot_context(),
            "state_path": self.state_path,
            "event_log_path": self.event_log_path,
        }

    def _publish_summary(self) -> None:
        summary = self._summary()
        self.count_pub.publish(UInt32(data=int(summary["completed_count"])))
        self.attempt_pub.publish(UInt32(data=int(summary["attempt_count"])))
        self.summary_pub.publish(String(data=json.dumps(summary, ensure_ascii=False, sort_keys=True)))
        for key, value in (
            ("completed_count", summary["completed_count"]),
            ("attempt_count", summary["attempt_count"]),
            ("failed_count", summary["failed_count"]),
            ("canceled_count", summary["canceled_count"]),
            ("recovery_attempt_count", summary["recovery_attempt_count"]),
            ("recovery_success_count", summary["recovery_success_count"]),
            ("recovery_failed_count", summary["recovery_failed_count"]),
        ):
            try:
                rospy.set_param("~" + key, int(value))
            except Exception:
                pass

    def _reset_charge_watch(self, cycle: Dict[str, Any], reason: str, soc: Optional[float] = None) -> None:
        if soc is None:
            soc = _soc_from_battery(self._battery)
        if soc is None:
            cycle.pop("charge_watch_started_unix", None)
            cycle.pop("charge_watch_started_iso", None)
            cycle.pop("charge_watch_start_soc", None)
            cycle["charge_watch_last_reset_reason"] = reason
            return
        ts = _now()
        cycle["charge_watch_started_unix"] = ts
        cycle["charge_watch_started_iso"] = _iso(ts)
        cycle["charge_watch_start_soc"] = soc
        cycle["charge_watch_last_reset_reason"] = reason

    def _maybe_start_charge_recovery_locked(self) -> None:
        if not self.recovery_enable or self._recovery_running:
            return
        cycle = self._state.get("current_cycle")
        if not isinstance(cycle, dict):
            return
        # Persisted cycle state is diagnostic history, not motion authority. A
        # recovery may only start from the current live dock state.
        dock_state = str(self._last_dock_state or "").strip().upper()
        if dock_state not in RECOVERY_ELIGIBLE_STATES:
            return
        if not bool(cycle.get("charge_command_seen")):
            return
        self._ensure_charge_watch(cycle)
        watch_started = cycle.get("charge_watch_started_unix")
        start_soc = cycle.get("charge_watch_start_soc")
        current_soc = _soc_from_battery(self._battery)
        if watch_started is None or start_soc is None or current_soc is None:
            return

        now = _now()
        age_s = max(0.0, now - float(watch_started))
        delta = float(current_soc) - float(start_soc)
        if age_s < self.recovery_no_soc_change_timeout_s:
            return
        if delta >= self.recovery_min_soc_delta:
            self._reset_charge_watch(cycle, "soc_increased", current_soc)
            return

        if int(cycle.get("recovery_attempts", 0) or 0) >= self.recovery_max_attempts_per_cycle:
            self._trigger_recovery_exhausted_locked(
                cycle,
                dock_state=dock_state,
                watch_age_s=age_s,
                watch_start_soc=float(start_soc),
                current_soc=float(current_soc),
                soc_delta=delta,
            )
            return

        cycle["recovery_attempts"] = int(cycle.get("recovery_attempts", 0) or 0) + 1
        recovery_event = {
            "attempt_index": int(cycle["recovery_attempts"]),
            "status": "RUNNING",
            "started_unix": now,
            "started_iso": _iso(now),
            "reason": "soc_not_changed",
            "watch_age_s": age_s,
            "watch_start_soc": float(start_soc),
            "current_soc": float(current_soc),
            "soc_delta": delta,
            "dock_state": dock_state,
            "strategy": self.recovery_strategy,
        }
        if self.recovery_strategy == "redock":
            recovery_event["redock_service"] = self.recovery_redock_service
            recovery_event["redock_service_timeout_s"] = self.recovery_redock_service_timeout_s
        else:
            recovery_event["back_distance_m"] = self.recovery_back_distance_m
            recovery_event["forward_distance_m"] = self.recovery_forward_distance_m
            recovery_event["linear_speed_mps"] = self.recovery_linear_speed_mps
        events = list(cycle.get("recovery_events") or [])
        events.append(recovery_event)
        cycle["recovery_events"] = events
        self._state["recovery_attempt_count"] = int(self._state.get("recovery_attempt_count", 0) or 0) + 1
        self._recovery_running = True
        self._record_event("charge_recovery_start", cycle=cycle, dock_state=dock_state)
        self._persist()
        self._publish_summary()

        cycle_id = str(cycle.get("cycle_id", ""))
        thread = threading.Thread(target=self._run_charge_recovery, args=(cycle_id,), daemon=True)
        thread.start()

    def _trigger_recovery_exhausted_locked(
        self,
        cycle: Dict[str, Any],
        *,
        dock_state: str,
        watch_age_s: float,
        watch_start_soc: float,
        current_soc: float,
        soc_delta: float,
    ) -> None:
        if bool(cycle.get("recovery_exhausted_triggered")):
            return
        cycle["recovery_exhausted_triggered"] = True
        cycle["recovery_exhausted_unix"] = _now()
        cycle["recovery_exhausted_iso"] = _iso(float(cycle["recovery_exhausted_unix"]))
        cycle["recovery_exhausted_watch_age_s"] = float(watch_age_s)
        cycle["recovery_exhausted_watch_start_soc"] = float(watch_start_soc)
        cycle["recovery_exhausted_soc"] = float(current_soc)
        cycle["recovery_exhausted_soc_delta"] = float(soc_delta)
        cycle["failure_reason"] = "CHARGE_RECOVERY_EXHAUSTED"
        cycle_id = str(cycle.get("cycle_id", ""))

        self._finish_cycle("FAILED_CHARGE_RECOVERY_EXHAUSTED")
        self._recovery_running = True
        self._record_event(
            "charge_recovery_exhausted",
            cycle=cycle,
            dock_state=dock_state,
        )
        self._persist()
        self._publish_summary()

        thread = threading.Thread(
            target=self._run_recovery_exhausted,
            args=(cycle_id, cycle),
            name="charge_recovery_exhausted",
            daemon=True,
        )
        thread.start()

    def _publish_stop(self) -> None:
        stop = Twist()
        for _ in range(5):
            if rospy.is_shutdown():
                return
            self.cmd_vel_pub.publish(stop)
            rospy.sleep(0.05)

    def _publish_charge_enable(self, enabled: bool) -> None:
        desired = bool(enabled)
        station_repeats = self.recovery_station_charge_enable_repeat if desired else 1
        station_interval_s = self.recovery_station_charge_enable_interval_s if desired else 0.0

        for idx in range(self.recovery_charge_cmd_repeat):
            if rospy.is_shutdown():
                return
            self.mcore_charge_pub.publish(Bool(data=desired))
            if idx + 1 < self.recovery_charge_cmd_repeat and self.recovery_charge_cmd_interval_s > 0.0:
                rospy.sleep(self.recovery_charge_cmd_interval_s)

        station_msg = ControlStation()
        station_msg.operation = 1
        station_msg.status = desired
        for idx in range(station_repeats):
            if rospy.is_shutdown():
                return
            self.station_control_pub.publish(station_msg)
            if idx + 1 < station_repeats and station_interval_s > 0.0:
                rospy.sleep(station_interval_s)

    def _recovery_cycle_still_active(self, cycle_id: str) -> bool:
        with self._lock:
            cycle = self._state.get("current_cycle")
            if not isinstance(cycle, dict) or str(cycle.get("cycle_id", "")) != str(cycle_id):
                return False
            dock_state = str(cycle.get("last_dock_state") or self._last_dock_state or "").strip().upper()
            return dock_state in RECOVERY_ELIGIBLE_STATES

    def _sleep_recovery_settle(self, cycle_id: str) -> None:
        end_ts = time.time() + self.recovery_settle_s
        while time.time() < end_ts and not rospy.is_shutdown():
            if not self._recovery_cycle_still_active(cycle_id):
                raise RuntimeError("charge cycle is no longer active")
            rospy.sleep(min(0.2, max(0.0, end_ts - time.time())))

    def _drive_linear_recovery(self, cycle_id: str, linear_x: float, distance_m: float) -> None:
        if not self._recovery_cycle_still_active(cycle_id):
            raise RuntimeError("charge cycle is no longer active")
        distance_m = max(0.0, float(distance_m))
        speed = max(0.01, abs(float(linear_x)))
        if distance_m <= 1e-6:
            self._publish_stop()
            return
        duration_s = distance_m / speed
        rate = rospy.Rate(self.recovery_cmd_hz)
        msg = Twist()
        msg.linear.x = float(linear_x)
        msg.angular.z = 0.0
        end_ts = time.time() + duration_s
        while time.time() < end_ts and not rospy.is_shutdown():
            if not self._recovery_cycle_still_active(cycle_id):
                raise RuntimeError("charge cycle is no longer active")
            msg.linear.x = float(linear_x)
            self.cmd_vel_pub.publish(msg)
            rate.sleep()
        self._publish_stop()

    def _run_contact_jog_recovery(self, cycle_id: str) -> None:
        rospy.logwarn(
            "[AUTO_CHARGE_MON] charge recovery contact_jog start: back %.3fm forward %.3fm speed %.3fm/s",
            self.recovery_back_distance_m,
            self.recovery_forward_distance_m,
            self.recovery_linear_speed_mps,
        )
        if self.recovery_toggle_charge:
            self._publish_charge_enable(False)
            self._sleep_recovery_settle(cycle_id)
        self._drive_linear_recovery(cycle_id, -self.recovery_linear_speed_mps, self.recovery_back_distance_m)
        self._sleep_recovery_settle(cycle_id)
        self._drive_linear_recovery(cycle_id, self.recovery_linear_speed_mps, self.recovery_forward_distance_m)
        self._sleep_recovery_settle(cycle_id)
        if self.recovery_toggle_charge and self._recovery_cycle_still_active(cycle_id):
            self._publish_charge_enable(True)
        rospy.logwarn("[AUTO_CHARGE_MON] charge recovery contact_jog motion finished")

    def _call_trigger_service_bounded(self, service_name: str, client: Any, timeout_s: float) -> Any:
        rospy.wait_for_service(service_name, timeout=timeout_s)
        response = {}
        completed = threading.Event()

        def _call_service() -> None:
            try:
                response["value"] = client()
            except Exception as e:
                response["error"] = e
            finally:
                completed.set()

        caller = threading.Thread(
            target=_call_service,
            name="auto_charge_redock_service_call",
            daemon=True,
        )
        caller.start()
        if not completed.wait(timeout_s):
            raise RuntimeError("service response timeout after %.1fs: %s" % (timeout_s, service_name))
        if "error" in response:
            raise response["error"]
        resp = response.get("value")
        if resp is None:
            raise RuntimeError("service returned no response: %s" % service_name)
        return resp

    def _request_redock_recovery(self, cycle_id: str) -> str:
        if not self._recovery_cycle_still_active(cycle_id):
            raise RuntimeError("charge cycle is no longer active")
        with self._lock:
            cycle = self._state.get("current_cycle")
            if isinstance(cycle, dict) and str(cycle.get("cycle_id", "")) == str(cycle_id):
                cycle["redock_recovery_active"] = True
                cycle["redock_recovery_requesting"] = True
        rospy.logwarn(
            "[AUTO_CHARGE_MON] charge recovery redock request: service=%s response_timeout=%.1fs",
            self.recovery_redock_service,
            self.recovery_redock_service_timeout_s,
        )
        resp = self._call_trigger_service_bounded(
            self.recovery_redock_service,
            self.redock_recovery_cli,
            self.recovery_redock_service_timeout_s,
        )
        message = str(getattr(resp, "message", "") or "")
        if not bool(getattr(resp, "success", False)):
            raise RuntimeError("redock service rejected: %s" % (message or "unknown"))
        rospy.logwarn("[AUTO_CHARGE_MON] charge recovery redock accepted: %s", message or "accepted")
        return message

    def _run_recovery_exhausted(self, cycle_id: str, cycle: Dict[str, Any]) -> None:
        ok = False
        error = ""
        message = ""
        try:
            rospy.logerr(
                "[AUTO_CHARGE_MON] recovery exhausted cycle=%s; request terminal retreat via %s",
                cycle_id or "-",
                self.recovery_exhausted_service,
            )
            resp = self._call_trigger_service_bounded(
                self.recovery_exhausted_service,
                self.recovery_exhausted_cli,
                self.recovery_exhausted_service_timeout_s,
            )
            message = str(getattr(resp, "message", "") or "")
            if not bool(getattr(resp, "success", False)):
                raise RuntimeError("recovery exhausted service rejected: %s" % (message or "unknown"))
            ok = True
        except Exception as e:
            error = str(e)
            rospy.logerr("[AUTO_CHARGE_MON] recovery exhausted handling request failed: %s", error)

        with self._lock:
            cycle["recovery_exhausted_action_status"] = "ACCEPTED" if ok else "FAILED"
            cycle["recovery_exhausted_action_finished_unix"] = _now()
            cycle["recovery_exhausted_action_finished_iso"] = _iso(
                float(cycle["recovery_exhausted_action_finished_unix"])
            )
            if message:
                cycle["recovery_exhausted_action_message"] = message
            if error:
                cycle["recovery_exhausted_action_error"] = error
            self._recovery_running = False
            self._record_event(
                "charge_recovery_exhausted_action_accepted"
                if ok
                else "charge_recovery_exhausted_action_failed",
                cycle=cycle,
                dock_state=str(cycle.get("last_dock_state") or self._last_dock_state),
            )
            self._persist()
            self._publish_summary()

    def _run_charge_recovery(self, cycle_id: str) -> None:
        ok = True
        error = ""
        service_message = ""
        strategy = self.recovery_strategy
        try:
            if strategy == "redock":
                service_message = self._request_redock_recovery(cycle_id)
            else:
                self._run_contact_jog_recovery(cycle_id)
        except Exception as e:
            ok = False
            error = str(e)
            rospy.logerr("[AUTO_CHARGE_MON] charge recovery failed: %s", error)
            try:
                self._publish_stop()
            except Exception:
                pass
            try:
                if self.recovery_toggle_charge and self._recovery_cycle_still_active(cycle_id):
                    self._publish_charge_enable(True)
            except Exception:
                pass

        with self._lock:
            cycle = self._state.get("current_cycle")
            if not isinstance(cycle, dict) or str(cycle.get("cycle_id", "")) != str(cycle_id):
                self._recovery_running = False
                self._record_event("charge_recovery_done" if ok else "charge_recovery_failed", cycle=None, dock_state=self._last_dock_state)
                self._persist()
                self._publish_summary()
                return

            ts = _now()
            events = list(cycle.get("recovery_events") or [])
            if events:
                events[-1]["status"] = "DONE" if ok else "FAILED"
                events[-1]["finished_unix"] = ts
                events[-1]["finished_iso"] = _iso(ts)
                events[-1]["end_soc"] = _soc_from_battery(self._battery)
                if service_message:
                    events[-1]["service_message"] = service_message
                if error:
                    events[-1]["error"] = error
                cycle["recovery_events"] = events

            if ok:
                self._state["recovery_success_count"] = int(self._state.get("recovery_success_count", 0) or 0) + 1
            else:
                self._state["recovery_failed_count"] = int(self._state.get("recovery_failed_count", 0) or 0) + 1
            if ok and strategy == "redock":
                cycle["redock_recovery_active"] = True
                cycle["redock_recovery_requesting"] = False
                cycle["redock_recovery_pending_charge_watch_reset"] = True
                cycle["redock_recovery_last_done_unix"] = ts
                cycle["redock_recovery_last_done_iso"] = _iso(ts)
            else:
                cycle["redock_recovery_active"] = False
                cycle["redock_recovery_requesting"] = False
                cycle["redock_recovery_pending_charge_watch_reset"] = False
                self._reset_charge_watch(cycle, "recovery_done" if ok else "recovery_failed")
            self._recovery_running = False
            self._record_event(
                "charge_recovery_done" if ok else "charge_recovery_failed",
                cycle=cycle,
                dock_state=str(cycle.get("last_dock_state") or self._last_dock_state),
            )
            self._persist()
            self._publish_summary()

    def _on_dock_supply_state(self, msg: String) -> None:
        dock_state = str(msg.data or "").strip().upper() or "IDLE"
        with self._lock:
            if dock_state == self._last_dock_state:
                return
            self._last_dock_state = dock_state

            if dock_state in ACTIVE_DOCK_STATES:
                if not isinstance(self._state.get("current_cycle"), dict):
                    if self._should_track_cycle():
                        self._start_cycle(dock_state)
                    else:
                        rospy.loginfo(
                            "[AUTO_CHARGE_MON] ignore dock cycle start because auto context is not active. dock=%s task=%s",
                            dock_state,
                            self._task_state,
                        )
                self._update_cycle_flags(dock_state)
            elif dock_state == "IDLE" and isinstance(self._state.get("current_cycle"), dict):
                # A live IDLE while a cycle is persisted means the workflow was
                # interrupted (for example by a runtime restart). Close it so a
                # A future dock attempt starts with a fresh observation window.
                self._finish_cycle("CANCELED")
            elif dock_state == "DONE" or dock_state == "CANCELED" or dock_state.startswith("FAILED"):
                cycle = self._state.get("current_cycle")
                if (
                    dock_state == "CANCELED"
                    and isinstance(cycle, dict)
                    and bool(cycle.get("redock_recovery_active"))
                ):
                    cycle["last_dock_state"] = dock_state
                    cycle["redock_recovery_cancel_seen"] = True
                    cycle["redock_recovery_cancel_seen_unix"] = _now()
                    cycle["redock_recovery_cancel_seen_iso"] = _iso()
                    self._record_event("charge_recovery_redock_cancel_seen", cycle=cycle, dock_state=dock_state)
                    self._persist()
                    self._publish_summary()
                    return
                self._finish_cycle(dock_state)

            self._persist()
            self._publish_summary()

    def _on_task_state(self, msg: String) -> None:
        with self._lock:
            self._task_state = str(msg.data or "").strip()

    def _on_task_event(self, msg: String) -> None:
        with self._lock:
            self._last_task_event = str(msg.data or "").strip()

    def _on_executor_state(self, msg: String) -> None:
        with self._lock:
            self._executor_state = str(msg.data or "").strip()

    def _on_battery(self, msg: BatteryState) -> None:
        with self._lock:
            self._battery = msg
            self._battery_ts = _now()

    def _on_progress(self, msg: Any) -> None:
        with self._lock:
            self._progress = msg
            self._progress_ts = _now()

    def _on_timer(self, _event: Any) -> None:
        with self._lock:
            try:
                self._maybe_start_charge_recovery_locked()
                self._persist()
                self._publish_summary()
            except Exception as e:
                rospy.logwarn_throttle(10.0, "[AUTO_CHARGE_MON] timer update failed: %s", str(e))

    def _srv_reset(self, _req: Any) -> TriggerResponse:
        with self._lock:
            self._state = self._new_state()
            self._last_dock_state = ""
            self._record_event("monitor_reset", cycle=None, dock_state="")
            self._persist()
            self._publish_summary()
            return TriggerResponse(success=True, message=json.dumps(self._summary(), ensure_ascii=False, sort_keys=True))

    def _srv_snapshot(self, _req: Any) -> TriggerResponse:
        with self._lock:
            self._persist()
            self._publish_summary()
            return TriggerResponse(success=True, message=json.dumps(self._summary(), ensure_ascii=False, sort_keys=True))


def main() -> None:
    rospy.init_node("auto_charge_monitor", anonymous=False)
    AutoChargeMonitor()
    rospy.spin()


if __name__ == "__main__":
    main()

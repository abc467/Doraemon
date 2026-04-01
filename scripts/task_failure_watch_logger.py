#!/usr/bin/env python3

import argparse
import json
import os
import threading
import time
from typing import Any, Dict, Optional

import rospy
from coverage_msgs.msg import TaskState
from my_msg_srv.msg import CombinedStatus, StationStatus
from sensor_msgs.msg import BatteryState
from std_msgs.msg import String


def _ros_time_to_float(stamp: rospy.Time) -> float:
    try:
        return float(stamp.to_sec())
    except Exception:
        return 0.0


class JsonlWatchLogger:
    def __init__(self, out_path: str, heartbeat_s: float):
        self.out_path = os.path.abspath(out_path)
        self.snapshot_path = self.out_path + ".latest.json"
        self.heartbeat_s = max(1.0, float(heartbeat_s))
        self._lock = threading.Lock()

        self._latest_task_state: Optional[TaskState] = None
        self._latest_task_mgr_state = ""
        self._latest_executor_state = ""
        self._latest_dock_supply_state = ""
        self._latest_battery: Optional[BatteryState] = None
        self._latest_combined: Optional[CombinedStatus] = None
        self._latest_station: Optional[StationStatus] = None

        self._last_task_signature: Optional[Dict[str, Any]] = None
        self._last_failure_signature: Optional[Dict[str, Any]] = None
        self._last_heartbeat_ts = 0.0

        os.makedirs(os.path.dirname(self.out_path), exist_ok=True)
        self._fh = open(self.out_path, "a", encoding="utf-8")

        rospy.Subscriber("/task_state", TaskState, self._on_task_state, queue_size=20)
        rospy.Subscriber("/coverage_task_manager/state", String, self._on_task_mgr_state, queue_size=20)
        rospy.Subscriber("/coverage_executor/state", String, self._on_executor_state, queue_size=20)
        rospy.Subscriber("/coverage_task_manager/event", String, self._on_task_event, queue_size=100)
        rospy.Subscriber("/coverage_executor/event", String, self._on_exec_event, queue_size=100)
        rospy.Subscriber("/dock_supply/state", String, self._on_dock_supply_state, queue_size=50)
        rospy.Subscriber("/battery_state", BatteryState, self._on_battery, queue_size=20)
        rospy.Subscriber("/combined_status", CombinedStatus, self._on_combined, queue_size=20)
        rospy.Subscriber("/station_status", StationStatus, self._on_station, queue_size=20)

        self._write(
            {
                "kind": "session_start",
                "message": "task failure watch logger started",
                "heartbeat_s": self.heartbeat_s,
                "pid": os.getpid(),
                "node": rospy.get_name(),
            }
        )
        rospy.Timer(rospy.Duration(self.heartbeat_s), self._on_heartbeat)

    def close(self):
        with self._lock:
            try:
                self._fh.flush()
                self._fh.close()
            except Exception:
                pass

    def _now_entry(self) -> Dict[str, Any]:
        return {
            "ts": time.time(),
            "ts_iso": time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime()),
        }

    def _task_state_dict(self, msg: Optional[TaskState]) -> Dict[str, Any]:
        if msg is None:
            return {}
        return {
            "mission_state": str(msg.mission_state or ""),
            "phase": str(msg.phase or ""),
            "public_state": str(msg.public_state or ""),
            "active_job_id": str(msg.active_job_id or ""),
            "run_id": str(msg.run_id or ""),
            "zone_id": str(msg.zone_id or ""),
            "plan_profile": str(msg.plan_profile or ""),
            "sys_profile": str(msg.sys_profile or ""),
            "mode": str(msg.mode or ""),
            "plan_id": str(msg.plan_id or ""),
            "map_id": str(msg.map_id or ""),
            "map_md5": str(msg.map_md5 or ""),
            "loops_total": int(msg.loops_total),
            "loops_done": int(msg.loops_done),
            "active_loop_index": int(msg.active_loop_index),
            "progress_pct": float(msg.progress_pct),
            "executor_state": str(msg.executor_state or ""),
            "error_code": str(msg.error_code or ""),
            "error_msg": str(msg.error_msg or ""),
            "interlock_active": bool(msg.interlock_active),
            "interlock_reason": str(msg.interlock_reason or ""),
            "last_event": str(msg.last_event or ""),
            "battery_soc": float(msg.battery_soc),
            "battery_valid": bool(msg.battery_valid),
            "stamp": _ros_time_to_float(msg.stamp),
        }

    def _battery_dict(self, msg: Optional[BatteryState]) -> Dict[str, Any]:
        if msg is None:
            return {}
        return {
            "stamp": _ros_time_to_float(msg.header.stamp),
            "voltage": float(msg.voltage),
            "current": float(msg.current),
            "percentage": float(msg.percentage),
            "power_supply_status": int(msg.power_supply_status),
            "present": bool(msg.present),
        }

    def _combined_dict(self, msg: Optional[CombinedStatus]) -> Dict[str, Any]:
        if msg is None:
            return {}
        return {
            "battery_percentage": int(msg.battery_percentage),
            "battery_voltage": int(msg.battery_voltage),
            "sewage_level": int(msg.sewage_level),
            "clean_level": int(msg.clean_level),
            "brush_position": int(msg.brush_position),
            "scraper_position": int(msg.scraper_position),
            "region": [bool(x) for x in msg.region],
            "status": [bool(x) for x in msg.status],
        }

    def _station_dict(self, msg: Optional[StationStatus]) -> Dict[str, Any]:
        if msg is None:
            return {}
        values = [bool(x) for x in msg.status]
        def _get(idx: int) -> Optional[bool]:
            return values[idx] if idx < len(values) else None
        return {
            "status": values,
            "agv_in_place": _get(11),
            "charger_status": _get(12),
            "charger_fault": _get(13),
        }

    def _snapshot(self) -> Dict[str, Any]:
        return {
            "task_state": self._task_state_dict(self._latest_task_state),
            "task_manager_state": self._latest_task_mgr_state,
            "executor_state_topic": self._latest_executor_state,
            "dock_supply_state": self._latest_dock_supply_state,
            "battery": self._battery_dict(self._latest_battery),
            "combined_status": self._combined_dict(self._latest_combined),
            "station_status": self._station_dict(self._latest_station),
        }

    def _write(self, payload: Dict[str, Any]):
        entry = self._now_entry()
        entry.update(payload)
        line = json.dumps(entry, ensure_ascii=False, separators=(",", ":"))
        snapshot = entry.get("snapshot")
        with self._lock:
            self._fh.write(line + "\n")
            self._fh.flush()
            if snapshot is not None:
                tmp = self.snapshot_path + ".tmp"
                with open(tmp, "w", encoding="utf-8") as fh:
                    json.dump(snapshot, fh, ensure_ascii=False, indent=2)
                    fh.write("\n")
                os.replace(tmp, self.snapshot_path)

    def _write_event(self, kind: str, message: str, *, source: str = "", severity: str = "INFO"):
        self._write(
            {
                "kind": kind,
                "severity": severity,
                "source": source,
                "message": str(message or ""),
                "snapshot": self._snapshot(),
            }
        )

    def _check_failure(self):
        ts = self._latest_task_state
        failure: Dict[str, Any] = {}
        if ts is not None:
            if str(ts.error_code or "").strip():
                failure["error_code"] = str(ts.error_code)
                failure["error_msg"] = str(ts.error_msg or "")
            public_state = str(ts.public_state or "").strip().upper()
            if public_state.startswith("ERROR"):
                failure["public_state"] = public_state
        ds = str(self._latest_dock_supply_state or "").strip().upper()
        if ds.startswith("FAILED") or ds == "CANCELED":
            failure["dock_supply_state"] = ds

        if not failure:
            self._last_failure_signature = None
            return
        if failure == self._last_failure_signature:
            return
        self._last_failure_signature = dict(failure)
        self._write(
            {
                "kind": "failure_snapshot",
                "severity": "ERROR",
                "source": "watch",
                "message": "failure or error state observed",
                "failure": failure,
                "snapshot": self._snapshot(),
            }
        )

    def _on_task_state(self, msg: TaskState):
        self._latest_task_state = msg
        signature = {
            "mission_state": str(msg.mission_state or ""),
            "phase": str(msg.phase or ""),
            "public_state": str(msg.public_state or ""),
            "run_id": str(msg.run_id or ""),
            "executor_state": str(msg.executor_state or ""),
            "error_code": str(msg.error_code or ""),
            "last_event": str(msg.last_event or ""),
            "active_loop_index": int(msg.active_loop_index),
            "loops_done": int(msg.loops_done),
            "progress_bucket": int(float(msg.progress_pct) // 5.0),
        }
        if signature != self._last_task_signature:
            self._last_task_signature = signature
            self._write(
                {
                    "kind": "task_state_change",
                    "severity": "ERROR" if signature["error_code"] or signature["public_state"].startswith("ERROR") else "INFO",
                    "source": "task_state",
                    "message": (
                        f"{signature['mission_state']}/{signature['phase']}/{signature['public_state']} "
                        f"exec={signature['executor_state']} progress={float(msg.progress_pct):.1f}%"
                    ),
                    "snapshot": self._snapshot(),
                }
            )
        self._check_failure()

    def _on_task_mgr_state(self, msg: String):
        data = str(msg.data or "")
        if data != self._latest_task_mgr_state:
            self._latest_task_mgr_state = data
            self._write_event("task_manager_state", data, source="coverage_task_manager")

    def _on_executor_state(self, msg: String):
        data = str(msg.data or "")
        if data != self._latest_executor_state:
            self._latest_executor_state = data
            self._write_event("executor_state", data, source="coverage_executor")

    def _on_task_event(self, msg: String):
        self._write_event("task_event", str(msg.data or ""), source="coverage_task_manager")
        self._check_failure()

    def _on_exec_event(self, msg: String):
        self._write_event("executor_event", str(msg.data or ""), source="coverage_executor")
        self._check_failure()

    def _on_dock_supply_state(self, msg: String):
        data = str(msg.data or "")
        if data != self._latest_dock_supply_state:
            self._latest_dock_supply_state = data
            sev = "ERROR" if data.upper().startswith("FAILED") or data.upper() == "CANCELED" else "INFO"
            self._write_event("dock_supply_state", data, source="dock_supply", severity=sev)
        self._check_failure()

    def _on_battery(self, msg: BatteryState):
        self._latest_battery = msg
        self._check_failure()

    def _on_combined(self, msg: CombinedStatus):
        self._latest_combined = msg

    def _on_station(self, msg: StationStatus):
        self._latest_station = msg

    def _on_heartbeat(self, _evt):
        now = time.time()
        if (now - self._last_heartbeat_ts) < self.heartbeat_s * 0.9:
            return
        self._last_heartbeat_ts = now
        self._write(
            {
                "kind": "heartbeat",
                "severity": "INFO",
                "source": "watch",
                "message": "periodic snapshot",
                "snapshot": self._snapshot(),
            }
        )


def main():
    parser = argparse.ArgumentParser(description="Low-impact JSONL logger for long-running cleaning task failures.")
    parser.add_argument("--out", default="", help="Output JSONL path. Default: ./log/task_watch_<timestamp>.jsonl")
    parser.add_argument("--heartbeat-s", type=float, default=10.0, help="Periodic snapshot interval in seconds.")
    args, _ = parser.parse_known_args()

    default_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "log",
        "task_watch_%s.jsonl" % time.strftime("%Y%m%d_%H%M%S", time.localtime()),
    )
    out_path = os.path.abspath(args.out or default_path)

    rospy.init_node("task_failure_watch_logger", anonymous=False)
    logger = JsonlWatchLogger(out_path=out_path, heartbeat_s=args.heartbeat_s)
    rospy.loginfo("[task_failure_watch_logger] writing to %s", out_path)
    try:
        rospy.spin()
    finally:
        logger.close()


if __name__ == "__main__":
    main()

import importlib.util
import os
import threading
import time
import unittest
from unittest import mock

from std_srvs.srv import TriggerResponse


SCRIPT_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "scripts", "auto_charge_monitor_node.py")
)
SPEC = importlib.util.spec_from_file_location("auto_charge_monitor_node_under_test", SCRIPT_PATH)
MONITOR_MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MONITOR_MODULE)
AutoChargeMonitor = MONITOR_MODULE.AutoChargeMonitor


class AutoChargeMonitorRecoveryTest(unittest.TestCase):
    def test_redock_service_response_timeout_is_bounded(self):
        monitor = AutoChargeMonitor.__new__(AutoChargeMonitor)
        monitor._lock = threading.RLock()
        monitor._state = {"current_cycle": {"cycle_id": "cycle_1"}}
        monitor.recovery_redock_service = "/coverage_task_manager/auto_charge_redock"
        monitor.recovery_redock_service_timeout_s = 0.05
        monitor.recovery_back_distance_m = 0.2
        monitor.recovery_linear_speed_mps = 0.03
        monitor._recovery_cycle_still_active = lambda cycle_id: cycle_id == "cycle_1"

        release_call = threading.Event()

        def blocked_service_call():
            release_call.wait(1.0)
            return TriggerResponse(success=True, message="late response")

        monitor.redock_recovery_cli = blocked_service_call
        started = time.monotonic()
        try:
            with mock.patch.object(MONITOR_MODULE.rospy, "wait_for_service"):
                with self.assertRaisesRegex(RuntimeError, "response timeout"):
                    monitor._request_redock_recovery("cycle_1")
        finally:
            release_call.set()

        self.assertLess(time.monotonic() - started, 0.5)

    def test_recovery_requires_live_eligible_dock_state(self):
        monitor = AutoChargeMonitor.__new__(AutoChargeMonitor)
        monitor.recovery_enable = True
        monitor._recovery_running = False
        monitor._last_dock_state = "IDLE"
        monitor._state = {
            "current_cycle": {
                "cycle_id": "stale_cycle",
                "last_dock_state": "CHARGE_CONFIRMED",
                "charge_command_seen": True,
                "recovery_attempts": 0,
            }
        }

        monitor._maybe_start_charge_recovery_locked()

        self.assertFalse(monitor._recovery_running)
        self.assertEqual(monitor._state["current_cycle"]["recovery_attempts"], 0)

    def test_recovery_exhaustion_records_failed_cycle_once(self):
        monitor = AutoChargeMonitor.__new__(AutoChargeMonitor)
        monitor._lock = threading.RLock()
        monitor._recovery_running = False
        monitor._battery = None
        monitor._battery_ts = 0.0
        monitor._progress = None
        monitor._progress_ts = 0.0
        monitor._task_state = "AUTO_CHARGING"
        monitor._executor_state = "PAUSED"
        monitor._last_task_event = "SUPPLY_START"
        monitor._last_dock_state = "CHARGE_CONFIRMED"
        monitor.max_recent_cycles = 10
        monitor.state_path = "/tmp/not-used.json"
        monitor.event_log_path = "/tmp/not-used.jsonl"
        monitor._state = {
            "session_id": "session_1",
            "attempt_count": 1,
            "completed_count": 0,
            "failed_count": 0,
            "canceled_count": 0,
            "recovery_attempt_count": 2,
            "recovery_success_count": 2,
            "recovery_failed_count": 0,
            "recent_cycles": [],
            "current_cycle": {
                "cycle_id": "cycle_1",
                "attempt_index": 1,
                "started_unix": time.time() - 700.0,
                "last_dock_state": "CHARGE_CONFIRMED",
                "charge_command_seen": True,
                "recovery_attempts": 2,
            },
        }
        events = []
        monitor._record_event = lambda name, cycle, dock_state: events.append((name, dock_state))
        monitor._persist = lambda: None
        monitor._publish_summary = lambda: None

        worker = mock.Mock()
        with mock.patch.object(MONITOR_MODULE.threading, "Thread", return_value=worker):
            cycle = monitor._state["current_cycle"]
            monitor._trigger_recovery_exhausted_locked(
                cycle,
                dock_state="CHARGE_CONFIRMED",
                watch_age_s=301.0,
                watch_start_soc=0.95,
                current_soc=0.94,
                soc_delta=-0.01,
            )

        self.assertEqual(monitor._state["failed_count"], 1)
        self.assertIsNone(monitor._state["current_cycle"])
        self.assertEqual(len(monitor._state["recent_cycles"]), 1)
        self.assertEqual(cycle["status"], "FAILED")
        self.assertEqual(cycle["failure_reason"], "CHARGE_RECOVERY_EXHAUSTED")
        self.assertTrue(monitor._recovery_running)
        self.assertIn(("charge_recovery_exhausted", "CHARGE_CONFIRMED"), events)
        worker.start.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()

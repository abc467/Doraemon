#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import threading
import time
import types
import unittest
from unittest import mock

from coverage_executor.actuator_debug_lease import (
    ActuatorDebugSafetyLimits,
    ActuatorDebugSafetySnapshot,
    WallClockLease,
)
from coverage_executor.fsm import ExecutorFSM


class ActuatorDebugEntryStateTest(unittest.TestCase):
    def _fsm(self, state="DONE", thread=None):
        fsm = ExecutorFSM.__new__(ExecutorFSM)
        fsm._lock = threading.RLock()
        fsm._state = state
        fsm._running_thread = thread
        fsm._events = []

        def publish_state(next_state):
            fsm._state = str(next_state)

        fsm._publish_state = publish_state
        fsm._emit = lambda event: fsm._events.append(str(event))
        return fsm

    @mock.patch("coverage_executor.fsm.rospy.loginfo")
    def test_completed_run_without_live_thread_is_retired_to_idle(self, _loginfo):
        fsm = self._fsm(state="DONE")

        changed = ExecutorFSM._normalize_completed_state_for_actuator_debug(fsm)

        self.assertTrue(changed)
        self.assertEqual(fsm._state, "IDLE")
        self.assertEqual(fsm._events, ["ACTUATOR_DEBUG_ENTRY_READY:from=DONE"])

    @mock.patch("coverage_executor.fsm.rospy.loginfo")
    def test_done_is_not_retired_while_run_thread_is_alive(self, _loginfo):
        thread = types.SimpleNamespace(is_alive=lambda: True)
        fsm = self._fsm(state="DONE", thread=thread)

        changed = ExecutorFSM._normalize_completed_state_for_actuator_debug(fsm)

        self.assertFalse(changed)
        self.assertEqual(fsm._state, "DONE")
        self.assertEqual(fsm._events, [])

    @mock.patch("coverage_executor.fsm.rospy.loginfo")
    def test_non_success_terminal_and_paused_states_remain_blocked(self, _loginfo):
        for state in ("PAUSED", "FAILED", "ERROR_NAV", "ESTOP"):
            with self.subTest(state=state):
                fsm = self._fsm(state=state)

                changed = ExecutorFSM._normalize_completed_state_for_actuator_debug(fsm)

                self.assertFalse(changed)
                self.assertEqual(fsm._state, state)
                self.assertEqual(fsm._events, [])


class ActuatorDebugPostOffSafetyTest(unittest.TestCase):
    @staticmethod
    def _snapshot(**overrides):
        values = {
            "executor_state": "IDLE",
            "run_thread_active": False,
            "linear_speed_mps": 0.0,
            "angular_speed_rps": 0.0,
            "odom_age_s": 0.1,
            "mcore_connected_seen": True,
            "mcore_connected": True,
            "telemetry_seen": True,
            "telemetry_age_s": 0.1,
            "telemetry_generation": 1,
            "safety_status_seen": True,
            "safety_status_age_s": 0.1,
            "safety_status_generation": 1,
            "emergency_stop_active": False,
        }
        values.update(overrides)
        return ActuatorDebugSafetySnapshot(**values)

    def _fsm(self, snapshots, wait_s=3.0):
        fsm = ExecutorFSM.__new__(ExecutorFSM)
        fsm._actuator_debug_limits = ActuatorDebugSafetyLimits()
        fsm._actuator_debug_post_off_status_wait_s = wait_s
        fsm._actuator_debug_stop_evt = types.SimpleNamespace(
            is_set=lambda: False,
            wait=lambda _seconds: False,
        )
        sequence = iter(snapshots)
        fsm._actuator_debug_safety_snapshot = lambda **_kwargs: next(sequence)
        return fsm

    @mock.patch("coverage_executor.fsm.rospy.is_shutdown", return_value=False)
    def test_waits_for_new_safety_status_generation_after_all_off(self, _is_shutdown):
        fsm = self._fsm(
            [
                self._snapshot(safety_status_age_s=2.1, safety_status_generation=1),
                self._snapshot(safety_status_age_s=0.1, safety_status_generation=2),
            ]
        )

        violation = ExecutorFSM._wait_for_actuator_debug_post_off_safety(
            fsm,
            after_safety_generation=1,
            after_telemetry_generation=1,
            require_safety_confirmation=True,
        )

        self.assertIsNone(violation)

    @mock.patch("coverage_executor.fsm.rospy.is_shutdown", return_value=False)
    def test_non_status_safety_violation_fails_immediately(self, _is_shutdown):
        fsm = self._fsm([self._snapshot(mcore_connected=False)])

        violation = ExecutorFSM._wait_for_actuator_debug_post_off_safety(
            fsm,
            after_safety_generation=1,
            after_telemetry_generation=1,
            require_safety_confirmation=True,
        )

        self.assertEqual(violation, "M-core disconnected")

    @mock.patch("coverage_executor.fsm.rospy.is_shutdown", return_value=False)
    def test_zero_wait_keeps_stale_status_fail_closed(self, _is_shutdown):
        fsm = self._fsm(
            [self._snapshot(safety_status_age_s=2.1, safety_status_generation=1)],
            wait_s=0.0,
        )

        violation = ExecutorFSM._wait_for_actuator_debug_post_off_safety(
            fsm,
            after_safety_generation=1,
            after_telemetry_generation=1,
            require_safety_confirmation=True,
        )

        self.assertEqual(violation, "M-core safety status unavailable or stale")

    @mock.patch("coverage_executor.fsm.rospy.is_shutdown", return_value=False)
    def test_same_fresh_generation_after_all_off_is_not_accepted(self, _is_shutdown):
        fsm = self._fsm(
            [self._snapshot(safety_status_age_s=0.1, safety_status_generation=1)],
            wait_s=0.0,
        )

        violation = ExecutorFSM._wait_for_actuator_debug_post_off_safety(
            fsm,
            after_safety_generation=1,
            after_telemetry_generation=1,
            require_safety_confirmation=True,
        )

        self.assertEqual(violation, "M-core safety status did not refresh after all-off")

    @mock.patch("coverage_executor.fsm.rospy.is_shutdown", return_value=False)
    def test_compatibility_mode_waits_for_new_trusted_telemetry(self, _is_shutdown):
        fsm = self._fsm(
            [
                self._snapshot(
                    telemetry_generation=1,
                    safety_status_seen=False,
                    safety_status_age_s=float("inf"),
                ),
                self._snapshot(
                    telemetry_generation=2,
                    safety_status_seen=False,
                    safety_status_age_s=float("inf"),
                ),
            ]
        )

        violation = ExecutorFSM._wait_for_actuator_debug_post_off_safety(
            fsm,
            after_safety_generation=0,
            after_telemetry_generation=1,
            require_safety_confirmation=False,
        )

        self.assertIsNone(violation)

    @mock.patch("coverage_executor.fsm.rospy.logwarn")
    def test_unreadable_estop_ack_is_explicit_short_lived_and_audited(self, _logwarn):
        fsm = ExecutorFSM.__new__(ExecutorFSM)
        fsm._actuator_debug_control_lock = threading.RLock()
        fsm._lock = threading.RLock()
        fsm._state = "IDLE"
        fsm._running_thread = None
        fsm._actuator_debug_active = False
        fsm._actuator_debug_transition = False
        fsm._actuator_debug_limits = ActuatorDebugSafetyLimits(
            require_authoritative_safety_status=False
        )
        fsm._actuator_debug_unreadable_estop_ack_s = 15.0
        fsm._actuator_debug_unreadable_estop_ack_deadline = 0.0
        fsm._events = []
        fsm._emit = lambda event: fsm._events.append(str(event))
        fsm._actuator_debug_safety_snapshot = lambda **_kwargs: self._snapshot(
            safety_status_seen=False,
            safety_status_age_s=float("inf"),
        )

        response = ExecutorFSM._handle_acknowledge_actuator_debug_unreadable_estop(
            fsm, None
        )

        self.assertTrue(response.success)
        self.assertGreater(
            fsm._actuator_debug_unreadable_estop_ack_deadline, time.monotonic()
        )
        self.assertEqual(
            fsm._events,
            ["ACTUATOR_DEBUG_UNREADABLE_ESTOP_ACK:window=15.0s"],
        )

    def test_compatibility_enable_rejects_missing_operator_ack_before_physical_io(self):
        fsm = ExecutorFSM.__new__(ExecutorFSM)
        fsm._actuator_debug_control_lock = threading.RLock()
        fsm._lock = threading.RLock()
        fsm._state = "IDLE"
        fsm._running_thread = None
        fsm._actuator_debug_active = False
        fsm._actuator_debug_transition = False
        fsm._actuator_debug_limits = ActuatorDebugSafetyLimits(
            require_authoritative_safety_status=False
        )
        fsm._actuator_debug_unreadable_estop_ack_deadline = 0.0
        fsm._events = []
        fsm._emit = lambda event: fsm._events.append(str(event))
        fsm._actuator_debug_safety_snapshot = lambda **_kwargs: self._snapshot(
            safety_status_seen=False,
            safety_status_age_s=float("inf"),
        )

        response = ExecutorFSM._enable_or_renew_actuator_debug(fsm)

        self.assertFalse(response.success)
        self.assertIn("explicit operator acknowledgement required", response.message)

    @mock.patch("coverage_executor.fsm.rospy.logwarn")
    @mock.patch("coverage_executor.fsm.rospy.is_shutdown", return_value=False)
    def test_enable_uses_generation_captured_after_force_all_off(
        self, _is_shutdown, _logwarn
    ):
        generation = {"value": 1}

        class FakeClean:
            @staticmethod
            def set_reconcile_paused(_paused, *, reason=""):
                del reason

            @staticmethod
            def force_all_off(*, reason="", water_off_latched=False):
                del reason, water_off_latched
                # Simulate a complete status frame arriving while the all-off
                # sequence is still executing.
                generation["value"] = 2

        class FakeStopEvent:
            @staticmethod
            def is_set():
                return False

            @staticmethod
            def wait(_seconds):
                # The lease may activate only after the next frame, not from
                # the frame that arrived during force_all_off().
                generation["value"] = 3
                return False

        fsm = ExecutorFSM.__new__(ExecutorFSM)
        fsm._actuator_debug_control_lock = threading.RLock()
        fsm._lock = threading.RLock()
        fsm._state = "IDLE"
        fsm._running_thread = None
        fsm._actuator_debug_active = False
        fsm._actuator_debug_transition = False
        fsm._actuator_debug_limits = ActuatorDebugSafetyLimits()
        fsm._actuator_debug_post_off_status_wait_s = 3.0
        fsm._actuator_debug_stop_evt = FakeStopEvent()
        fsm._actuator_debug_lease = WallClockLease(30.0)
        fsm.clean = FakeClean()
        fsm._events = []
        fsm._publish_actuator_debug_active = lambda _active: None
        fsm._publish_state = lambda state: setattr(fsm, "_state", str(state))
        fsm._emit = lambda event: fsm._events.append(str(event))
        fsm._actuator_debug_safety_snapshot = lambda **_kwargs: self._snapshot(
            executor_state=fsm._state,
            safety_status_generation=generation["value"],
        )

        response = ExecutorFSM._enable_or_renew_actuator_debug(fsm)

        self.assertTrue(response.success)
        self.assertEqual(generation["value"], 3)
        self.assertEqual(fsm._state, "ACTUATOR_DEBUG")


if __name__ == "__main__":
    unittest.main()

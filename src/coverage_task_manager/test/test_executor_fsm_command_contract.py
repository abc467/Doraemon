import unittest
import threading
import types
from unittest import mock

from coverage_executor.fsm import ExecutorFSM


class ExecutorFsmCommandContractTest(unittest.TestCase):
    def _fsm(self):
        fsm = ExecutorFSM.__new__(ExecutorFSM)
        fsm._lock = threading.RLock()
        fsm._actuator_debug_control_lock = threading.RLock()
        fsm._actuator_debug_active = False
        fsm._actuator_debug_transition = False
        fsm._zone_id = ""
        fsm._run_id = ""
        fsm._pause_req = False
        fsm._cancel_req = False
        fsm._execution_epoch = 0
        fsm._state = "IDLE"
        fsm._error_code = ""
        fsm._error_msg = ""
        fsm._water_off_latched = False
        fsm._state_changes = []
        fsm._threads = []
        fsm._events = []
        fsm._running_thread = None
        fsm.hard_stop_s = 0.6
        fsm.vacuum_delay_s = 1.5
        fsm._apply_intent_from_kv = lambda kv: setattr(fsm, "_last_kv", dict(kv))
        fsm._publish_state = lambda state: fsm._state_changes.append(str(state))
        fsm._start_thread = lambda mode: fsm._threads.append(str(mode))
        fsm._start_brake = lambda duration: setattr(fsm, "_last_brake_s", float(duration))
        fsm._stop_pause_hold = lambda: None
        fsm._clear_ai_spot = lambda reason: fsm._events.append(f"AI_CLEAR:{reason}")
        fsm._emit = lambda msg: fsm._events.append(str(msg))
        fsm._add_run_event = lambda **kwargs: None
        fsm._start_pause_hold = lambda: setattr(fsm, "_pause_hold_started", True)
        fsm.clean = types.SimpleNamespace(
            cancel_stop=lambda _delay: setattr(fsm, "_cancel_stop_called", True),
            pause_stop=lambda _delay: setattr(fsm, "_pause_stop_called", True),
        )
        fsm.mbf = types.SimpleNamespace(cancel_all=lambda: setattr(fsm, "_cancel_all_called", True))
        fsm._parse_kv_tokens = ExecutorFSM._parse_kv_tokens.__get__(fsm, ExecutorFSM)
        return fsm

    @mock.patch("coverage_executor.fsm.rospy.logwarn")
    def test_start_requires_zone_id_key(self, logwarn):
        fsm = self._fsm()

        ExecutorFSM._apply_cmd(fsm, "start zone_demo")

        self.assertEqual(fsm._threads, [])
        logwarn.assert_called_with("[EXEC] start requires zone_id")

    @mock.patch("coverage_executor.fsm.rospy.logwarn")
    def test_resume_requires_run_id_key(self, logwarn):
        fsm = self._fsm()

        ExecutorFSM._apply_cmd(fsm, "resume zone_id=zone_demo")

        self.assertEqual(fsm._threads, [])
        logwarn.assert_called_with("[EXEC] resume requires run_id")

    def test_resume_prefix_is_not_treated_as_resume(self):
        fsm = self._fsm()

        ExecutorFSM._apply_cmd(fsm, "resume_now run_id=run_123")

        self.assertEqual(fsm._threads, [])
        self.assertEqual(fsm._state_changes, [])

    def test_start_accepts_canonical_zone_and_run(self):
        fsm = self._fsm()

        ExecutorFSM._apply_cmd(fsm, "start zone_id=zone_demo run_id=run_123")

        self.assertEqual(fsm._zone_id, "zone_demo")
        self.assertEqual(fsm._run_id, "run_123")
        self.assertEqual(fsm._threads, ["start"])
        self.assertEqual(fsm._state_changes, ["START_REQ"])

    @mock.patch("coverage_executor.fsm.rospy.logwarn")
    def test_start_during_live_execution_does_not_steal_run_ownership(self, _logwarn):
        fsm = self._fsm()
        fsm._zone_id = "zone_old"
        fsm._run_id = "run_old"
        fsm._pause_req = True
        fsm._cancel_req = True
        fsm._running_thread = types.SimpleNamespace(is_alive=lambda: True)

        ExecutorFSM._apply_cmd(fsm, "start zone_id=zone_new run_id=run_new")

        self.assertEqual(fsm._zone_id, "zone_old")
        self.assertEqual(fsm._run_id, "run_old")
        self.assertTrue(fsm._pause_req)
        self.assertTrue(fsm._cancel_req)
        self.assertEqual(fsm._threads, [])
        self.assertEqual(fsm._state_changes, [])
        self.assertIn("CMD_REJECTED:start:EXECUTION_ACTIVE", fsm._events)

    @mock.patch("coverage_executor.fsm.rospy.logwarn")
    def test_resume_during_live_execution_does_not_clear_cancel(self, _logwarn):
        fsm = self._fsm()
        fsm._zone_id = "zone_old"
        fsm._run_id = "run_old"
        fsm._cancel_req = True
        fsm._running_thread = types.SimpleNamespace(is_alive=lambda: True)

        ExecutorFSM._apply_cmd(fsm, "resume run_id=run_new")

        self.assertEqual(fsm._run_id, "run_old")
        self.assertTrue(fsm._cancel_req)
        self.assertEqual(fsm._threads, [])
        self.assertEqual(fsm._state_changes, [])
        self.assertIn("CMD_REJECTED:resume:EXECUTION_ACTIVE", fsm._events)

    def test_accepted_resume_clears_previous_recoverable_error(self):
        fsm = self._fsm()
        fsm._state = "PAUSED_RECOVERY"
        fsm._error_code = "CONNECT_FAILED"
        fsm._error_msg = "block=8 connect failed"

        ExecutorFSM._apply_cmd(fsm, "resume run_id=run_123")

        self.assertEqual(fsm._error_code, "")
        self.assertEqual(fsm._error_msg, "")
        self.assertEqual(fsm._state_changes, ["RESUME_REQ"])
        self.assertEqual(fsm._threads, ["resume"])

    @mock.patch("coverage_executor.fsm.rospy.logwarn")
    def test_rejected_resume_preserves_current_error(self, _logwarn):
        fsm = self._fsm()
        fsm._error_code = "FOLLOW_FAILED"
        fsm._error_msg = "current execution failed"
        fsm._running_thread = types.SimpleNamespace(is_alive=lambda: True)

        ExecutorFSM._apply_cmd(fsm, "resume run_id=run_new")

        self.assertEqual(fsm._error_code, "FOLLOW_FAILED")
        self.assertEqual(fsm._error_msg, "current execution failed")

    def test_motion_watchdog_accepts_rotation_as_progress(self):
        anchor, progress_ts = ExecutorFSM._advance_motion_watchdog(
            (1.0, 2.0, 0.0),
            10.0,
            (1.0, 2.0, 0.10),
            12.0,
            0.03,
            0.08,
        )

        self.assertEqual(anchor, (1.0, 2.0, 0.10))
        self.assertEqual(progress_ts, 12.0)

    def test_se2_handoff_residual_includes_wrapped_yaw(self):
        dist, yaw = ExecutorFSM._se2_residual(
            (1.1, 2.0, -3.13),
            (1.0, 2.0, 3.13),
        )

        self.assertAlmostEqual(dist, 0.1)
        self.assertLess(yaw, 0.03)

    def test_idle_progress_message_owns_no_previous_run(self):
        msg = types.SimpleNamespace(
            state="IDLE",
            run_id="old_run",
            zone_id="zone_old",
            plan_id="plan_old",
            plan_profile="cover_standard",
            sys_profile="standard",
            mode="scrub",
            error_code="",
            error_msg="",
            interlock_active=False,
            interlock_reason="",
            v_mps=0.0,
            w_rps=0.0,
            exec_index=9,
            block_id=6,
            path_index=2329,
            path_s=116.4,
            block_length_m=141.0,
            total_length_m=2188.3,
            progress_0_1=0.94064,
            progress_pct=94.064,
        )

        ExecutorFSM._normalize_idle_progress_message(msg)

        self.assertEqual(msg.run_id, "")
        self.assertEqual(msg.zone_id, "")
        self.assertEqual(msg.plan_id, "")
        self.assertEqual(msg.exec_index, 0)
        self.assertEqual(msg.block_id, -1)
        self.assertEqual(msg.path_s, 0.0)
        self.assertEqual(msg.total_length_m, 0.0)
        self.assertEqual(msg.progress_0_1, 0.0)
        self.assertEqual(msg.progress_pct, 0.0)

    def test_running_progress_message_is_not_rewritten(self):
        msg = types.SimpleNamespace(state="FOLLOW:block_6", run_id="run_1", progress_pct=42.0)

        result = ExecutorFSM._normalize_idle_progress_message(msg)

        self.assertIs(result, msg)
        self.assertEqual(msg.run_id, "run_1")
        self.assertEqual(msg.progress_pct, 42.0)

    @mock.patch("coverage_executor.fsm.rospy.logwarn")
    def test_cancel_while_idle_keeps_executor_idle(self, logwarn):
        fsm = self._fsm()

        ExecutorFSM._apply_cmd(fsm, "cancel")

        self.assertFalse(fsm._cancel_req)
        self.assertEqual(fsm._state_changes, ["IDLE"])
        self.assertTrue(getattr(fsm, "_cancel_all_called", False))
        self.assertTrue(getattr(fsm, "_cancel_stop_called", False))
        logwarn.assert_called_with("[EXEC] CANCEL while idle -> keep IDLE")

    @mock.patch("coverage_executor.fsm.rospy.logwarn")
    def test_cancel_while_running_keeps_cancel_request_semantics(self, logwarn):
        fsm = self._fsm()
        fsm._running_thread = types.SimpleNamespace(is_alive=lambda: True)

        ExecutorFSM._apply_cmd(fsm, "cancel")

        self.assertTrue(fsm._cancel_req)
        self.assertEqual(fsm._state_changes, ["CANCEL_REQ"])
        self.assertTrue(getattr(fsm, "_cancel_all_called", False))
        self.assertTrue(getattr(fsm, "_cancel_stop_called", False))
        logwarn.assert_called_with("[EXEC] CANCEL (async)")

    @mock.patch("coverage_executor.fsm.rospy.logwarn")
    def test_pause_while_idle_keeps_executor_idle(self, logwarn):
        fsm = self._fsm()

        ExecutorFSM._apply_cmd(fsm, "pause")

        self.assertFalse(fsm._pause_req)
        self.assertEqual(fsm._state_changes, ["IDLE"])
        self.assertFalse(getattr(fsm, "_pause_hold_started", False))
        self.assertFalse(getattr(fsm, "_pause_stop_called", False))
        logwarn.assert_called_with("[EXEC] PAUSE while idle -> keep IDLE")

    @mock.patch("coverage_executor.fsm.rospy.logwarn")
    def test_pause_while_running_keeps_pause_request_semantics(self, logwarn):
        fsm = self._fsm()
        fsm._running_thread = types.SimpleNamespace(is_alive=lambda: True)

        ExecutorFSM._apply_cmd(fsm, "pause")

        self.assertTrue(fsm._pause_req)
        self.assertEqual(fsm._state_changes, ["PAUSE_REQ"])
        self.assertTrue(getattr(fsm, "_pause_hold_started", False))
        self.assertTrue(getattr(fsm, "_pause_stop_called", False))
        logwarn.assert_called_with("[EXEC] PAUSE (async)")


if __name__ == "__main__":
    unittest.main()

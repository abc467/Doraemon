import unittest
import threading
import types
from unittest import mock

from coverage_executor.fsm import ExecutorFSM


class ExecutorFsmCommandContractTest(unittest.TestCase):
    def _fsm(self):
        fsm = ExecutorFSM.__new__(ExecutorFSM)
        fsm._lock = threading.RLock()
        fsm._zone_id = ""
        fsm._run_id = ""
        fsm._pause_req = False
        fsm._cancel_req = False
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
        fsm.clean = types.SimpleNamespace(cancel_stop=lambda _delay: setattr(fsm, "_cancel_stop_called", True))
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


if __name__ == "__main__":
    unittest.main()

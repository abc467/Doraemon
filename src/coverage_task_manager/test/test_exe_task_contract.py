import unittest

from cleanrobot_app_msgs.srv import ExeTaskRequest as AppExeTaskRequest, ExeTaskResponse as AppExeTaskResponse

from coverage_task_manager.task_manager import TaskManager


class TaskManagerExeTaskContractTest(unittest.TestCase):
    def _manager(self, *, mission_state="IDLE", phase="IDLE", public_state="IDLE", active_run_id="", active_job_id=""):
        mgr = TaskManager.__new__(TaskManager)
        mgr._mission_state = mission_state
        mgr._phase = phase
        mgr._public_state = public_state
        mgr._active_run_id = active_run_id
        mgr._active_job_id = active_job_id
        return mgr

    def test_start_rejects_when_readiness_blocks(self):
        mgr = self._manager()
        mgr._build_system_readiness = lambda task_id, refresh_map_identity: type(
            "Readiness",
            (),
            {"can_start_task": False, "blocking_reasons": ["runtime map mismatch"]},
        )()
        mgr._start_task_by_id = lambda task_id, source="SERVICE": (True, "")

        req = type("Req", (), {"command": int(AppExeTaskRequest.START), "task_id": 3})()
        resp = mgr._on_exe_task_app(req)

        self.assertFalse(resp.success)
        self.assertEqual(resp.message, "system not ready: runtime map mismatch")

    def test_continue_returns_accepted_resume_when_resume_succeeds(self):
        mgr = self._manager(mission_state="PAUSED", public_state="PAUSED", active_run_id="run_1")
        mgr._resume_current_task = lambda run_id="", map_name="": (True, "")

        req = type("Req", (), {"command": int(AppExeTaskRequest.CONTINUE), "task_id": 0})()
        resp = mgr._on_exe_task_app(req)

        self.assertTrue(resp.success)
        self.assertEqual(resp.message, "accepted: resume")

    def test_stop_rejects_when_mission_not_running_or_paused(self):
        mgr = self._manager(mission_state="IDLE", phase="IDLE", public_state="IDLE")

        allowed, message = mgr._ensure_exe_task_allowed(int(AppExeTaskRequest.STOP))

        self.assertFalse(allowed)
        self.assertIn("stop requires running or paused mission", message)

    def test_app_start_returns_app_response_when_task_id_missing(self):
        mgr = self._manager()

        req = type("Req", (), {"command": int(AppExeTaskRequest.START), "task_id": 0})()
        resp = mgr._on_exe_task_app(req)

        self.assertIsInstance(resp, AppExeTaskResponse)
        self.assertFalse(resp.success)
        self.assertEqual(resp.message, "task_id is required for START")


if __name__ == "__main__":
    unittest.main()

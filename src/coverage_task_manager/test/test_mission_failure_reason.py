#!/usr/bin/env python3

import os
import tempfile
import unittest

from coverage_task_manager.mission_store import MissionStore


class MissionFailureReasonTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.store = MissionStore(os.path.join(self.tmpdir.name, "operations.db"))
        self.store.create_run(
            run_id="run_1",
            job_id="job",
            zone_id="zone",
            plan_profile_name="cover_standard",
            sys_profile_name="standard",
            clean_mode="scrub",
            loop_index=1,
            loops_total=1,
        )

    def tearDown(self):
        self.store.close()
        self.tmpdir.cleanup()

    def test_latest_executor_error_can_be_bound_to_paused_run(self):
        self.store.add_event(
            "run_1",
            source="EXEC",
            level="ERROR",
            code="CONNECT_FAILED",
            msg="block=9 connect failed attempt=3/2",
        )

        event = self.store.get_latest_error_event("run_1")

        self.assertEqual(event["code"], "CONNECT_FAILED")
        self.assertEqual(event["message"], "block=9 connect failed attempt=3/2")

    def test_cancel_after_paused_failure_preserves_root_cause(self):
        root_cause = "CONNECT_FAILED:block=9 connect failed attempt=3/2"
        self.store.update_state("run_1", "PAUSED", reason=root_cause)

        self.store.update_state("run_1", "CANCELED", reason="", set_end=True)

        run = self.store.get_run("run_1")
        self.assertEqual(run.state, "CANCELED")
        self.assertEqual(run.reason, root_cause)
        self.assertGreater(run.end_ts, 0.0)

    def test_cancel_from_running_does_not_preserve_nonfailure_reason(self):
        self.store.update_state("run_1", "RUNNING", reason="manual_resume")

        self.store.update_state("run_1", "CANCELED", reason="", set_end=True)

        run = self.store.get_run("run_1")
        self.assertEqual(run.reason, "")


if __name__ == "__main__":
    unittest.main()

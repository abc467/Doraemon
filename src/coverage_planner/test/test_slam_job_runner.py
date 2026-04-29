#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import threading
import unittest
from types import SimpleNamespace


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
PKG_DIR = os.path.dirname(THIS_DIR)
SRC_DIR = os.path.join(PKG_DIR, "src")

if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from coverage_planner.slam_workflow.job_runner import CartographerSlamJobRunner
from coverage_planner.slam_workflow.job_state import CartographerSlamJobController
from coverage_planner.ops_store.store import SlamJobRecord
from coverage_planner.runtime_gate_messages import manual_assist_metadata


class _FakeJobState:
    def __init__(self):
        self.snapshots = {
            "job_1": {
                "job_id": "job_1",
                "robot_id": "local_robot",
                "operation": 5,
                "runtime_operation": 4,
                "operation_name": "stop_mapping",
                "requested_map_name": "demo_map",
                "requested_map_revision_id": "rev_demo_01",
            }
        }
        self.updates = []

    def get_job_snapshot(self, job_id):
        return dict(self.snapshots.get(job_id) or {})

    def update_job_fields(self, snapshot, **kwargs):
        updated = dict(snapshot or {})
        updated.update(kwargs)
        self.updates.append(dict(updated))
        return updated

    def publish_job_snapshot(self, snapshot, **_kwargs):
        return dict(snapshot or {})


class _FakeEvents:
    def __init__(self):
        self.started = []
        self.finished = []

    def job_started(self, snapshot):
        self.started.append(dict(snapshot or {}))

    def job_finished(self, snapshot):
        self.finished.append(dict(snapshot or {}))


class _FakeServiceApi:
    def execute_operation(self, **_kwargs):
        return SimpleNamespace(
            success=True,
            error_code="",
            message="mapping stopped; localization requires manual assist",
            map_name="demo_map",
            map_revision_id="rev_demo_01",
            current_mode="localization",
            localization_state="manual_assist_required",
        )

    def response(self, **kwargs):
        return SimpleNamespace(**kwargs)


class _FakeRuntimeContext:
    def runtime_map_revision_id(self):
        return "rev_demo_01"


class SlamJobRunnerTest(unittest.TestCase):
    def test_successful_stop_mapping_keeps_manual_assist_state(self):
        backend = SimpleNamespace(
            robot_id="local_robot",
            _lock=threading.Lock(),
            _job_state=_FakeJobState(),
            _job_events=_FakeEvents(),
            _service_api=_FakeServiceApi(),
            _runtime_context=_FakeRuntimeContext(),
        )
        runner = CartographerSlamJobRunner(backend)

        runner.run_job("job_1")

        finished = backend._job_events.finished[-1]
        self.assertTrue(finished["success"])
        self.assertEqual(finished["status"], "manual_assist_required")
        self.assertEqual(finished["phase"], "manual_assist_required")
        self.assertEqual(finished["localization_state"], "manual_assist_required")
        self.assertTrue(finished["manual_assist_required"])
        self.assertFalse(finished["localization_valid"])

    def test_persisted_success_manual_assist_snapshot_normalizes_status(self):
        controller = CartographerSlamJobController(SimpleNamespace(robot_id="local_robot"))

        snapshot = controller.record_to_snapshot(
            SlamJobRecord(
                job_id="job_old",
                robot_id="local_robot",
                operation=5,
                operation_name="stop_mapping",
                requested_map_name="demo_map",
                requested_map_revision_id="rev_demo_01",
                resolved_map_name="demo_map",
                resolved_map_revision_id="rev_demo_01",
                status="succeeded",
                phase="done",
                progress_0_1=1.0,
                done=True,
                success=True,
                error_code="",
                message="mapping stopped; localization requires manual assist",
                current_mode="localization",
                localization_state="manual_assist_required",
            )
        )

        self.assertEqual(snapshot["status"], "manual_assist_required")
        self.assertEqual(snapshot["phase"], "manual_assist_required")
        self.assertTrue(snapshot["success"])
        self.assertTrue(snapshot["manual_assist_required"])
        self.assertFalse(snapshot["localization_valid"])

    def test_stop_mapping_manual_assist_retry_action_prepares_for_task(self):
        metadata = manual_assist_metadata(
            required=True,
            operation_name="stop_mapping",
            map_name="demo_map",
            map_revision_id="rev_demo_01",
        )

        self.assertEqual(metadata["retry_action"], "prepare_for_task")
        self.assertIn("retry prepare_for_task", metadata["guidance"])


if __name__ == "__main__":
    unittest.main()

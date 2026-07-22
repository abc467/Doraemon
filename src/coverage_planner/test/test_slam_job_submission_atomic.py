#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import threading
import unittest
from types import SimpleNamespace
from unittest import mock

import rospy
from cleanrobot_app_msgs.srv import SubmitSlamCommand

from coverage_planner.slam_workflow.job_state import CartographerSlamJobController
from coverage_planner.slam_workflow.service_api import SlamRuntimeServiceController


class _Ops:
    def __init__(self):
        self.records = []

    def upsert_slam_job(self, record):
        self.records.append(record)

    def get_slam_job(self, _job_id):
        return None

    def get_latest_slam_job(self, **_kwargs):
        return None


class _RuntimeState:
    def __init__(self):
        self.updates = []

    def update_runtime_state(self, **kwargs):
        self.updates.append(dict(kwargs))


class _Publisher:
    def __init__(self):
        self.messages = []

    def publish(self, message):
        self.messages.append(message)


class _RuntimeContext:
    @staticmethod
    def runtime_param(name):
        return "/cartographer/runtime/%s" % str(name)

    @staticmethod
    def to_ros_time(value):
        return rospy.Time.from_sec(float(value or 0.0))


class _PlanStore:
    @staticmethod
    def get_active_map(**_kwargs):
        return None


class _TokenWritingRunner:
    def __init__(self, token_writes):
        self.job_ids = []
        self._token_writes = token_writes

    def run_job(self, job_id):
        self.job_ids.append(str(job_id))
        self._token_writes.append(str(job_id))


class _BarrierServiceController(SlamRuntimeServiceController):
    def __init__(self, backend, barrier):
        super().__init__(backend)
        self._submit_barrier = barrier

    def _resolve_effective_map_name(self, **kwargs):
        self._submit_barrier.wait(timeout=5.0)
        return super()._resolve_effective_map_name(**kwargs)


def _submit_request():
    request = SubmitSlamCommand._request_class()
    request.operation = int(request.start_mapping)
    request.robot_id = "CR-001"
    request.map_name = ""
    request.map_revision_id = ""
    request.set_active = False
    request.description = "atomic-submit-test"
    request.frame_id = "map"
    request.has_initial_pose = False
    request.save_map_name = ""
    request.include_unfinished_submaps = False
    request.set_active_on_save = False
    request.switch_to_localization_after_save = False
    request.relocalize_after_switch = False
    return request


def _backend():
    token_writes = []
    backend = SimpleNamespace(
        robot_id="CR-001",
        max_job_history=20,
        _ops=_Ops(),
        _runtime_state=_RuntimeState(),
        _job_state_pub=_Publisher(),
        _runtime_context=_RuntimeContext(),
        _plan_store=_PlanStore(),
        _asset_helper=SimpleNamespace(),
    )
    backend._job_state = CartographerSlamJobController(backend)
    backend._job_runner = _TokenWritingRunner(token_writes)
    return backend, token_writes


class SlamJobSubmissionAtomicTest(unittest.TestCase):
    def test_two_concurrent_submits_accept_exactly_one_and_start_one_worker(self):
        backend, token_writes = _backend()
        controller = _BarrierServiceController(backend, threading.Barrier(2))
        original_make_job = backend._job_state.make_job_record
        make_job_calls = []
        make_job_calls_lock = threading.Lock()

        def counted_make_job(**kwargs):
            with make_job_calls_lock:
                make_job_calls.append(dict(kwargs))
            return original_make_job(**kwargs)

        backend._job_state.make_job_record = counted_make_job
        # The service must use the atomic reservation primitive, never a
        # separate check whose result can become stale before publication.
        backend._job_state.job_running = mock.Mock(
            side_effect=AssertionError("non-atomic job_running check used")
        )
        responses = [None, None]
        errors = []

        def submit(index):
            try:
                responses[index] = controller.handle_submit_job_app(_submit_request())
            except Exception as exc:  # pragma: no cover - asserted below
                errors.append(exc)

        threads = [threading.Thread(target=submit, args=(index,)) for index in range(2)]
        with mock.patch(
            "coverage_planner.slam_workflow.job_state.rospy.get_param",
            side_effect=lambda _name, default=None: default,
        ):
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=5.0)

        self.assertFalse(any(thread.is_alive() for thread in threads))
        self.assertEqual(errors, [])
        accepted = [response for response in responses if bool(response.accepted)]
        rejected = [response for response in responses if not bool(response.accepted)]
        self.assertEqual(len(accepted), 1)
        self.assertEqual(len(rejected), 1)
        self.assertEqual(rejected[0].error_code, "job_in_progress")
        self.assertEqual(rejected[0].job_id, accepted[0].job_id)
        self.assertEqual(len(make_job_calls), 1)
        self.assertEqual(len(backend._ops.records), 1)
        self.assertEqual(len(backend._runtime_state.updates), 1)
        self.assertEqual(len(backend._job_state_pub.messages), 1)
        self.assertEqual(backend._job_runner.job_ids, [accepted[0].job_id])
        self.assertEqual(token_writes, [accepted[0].job_id])

    def test_failed_reservation_never_invokes_competing_factory(self):
        backend, _token_writes = _backend()
        first_factory = mock.Mock(
            return_value={
                "job_id": "job-first",
                "robot_id": "CR-001",
                "operation": 3,
                "operation_name": "start_mapping",
                "status": "queued",
                "phase": "accepted",
                "done": False,
            }
        )
        competing_factory = mock.Mock(side_effect=AssertionError("loser created a job"))

        reserved, active = backend._job_state.try_reserve_and_publish_job(first_factory)
        rejected, competing_active = backend._job_state.try_reserve_and_publish_job(
            competing_factory
        )

        self.assertIsNone(active)
        self.assertEqual(reserved["job_id"], "job-first")
        self.assertIsNone(rejected)
        self.assertEqual(competing_active["job_id"], "job-first")
        first_factory.assert_called_once_with()
        competing_factory.assert_not_called()


if __name__ == "__main__":
    unittest.main()

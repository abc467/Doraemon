#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import unittest
from types import SimpleNamespace
from unittest import mock


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
TOOLS_DIR = os.path.join(os.path.dirname(THIS_DIR), "tools")
if TOOLS_DIR not in sys.path:
    sys.path.insert(0, TOOLS_DIR)

import run_backend_runtime_smoke as smoke


def _valid_topology_snapshot():
    return {
        "service_providers": dict(smoke.STAGE_K_EXPECTED_SERVICE_PROVIDERS),
        "node_robot_ids": {
            node_name: "CR-001"
            for _service_name, node_name in smoke.STAGE_K_EXPECTED_SERVICE_PROVIDERS
        },
        "node_private_params": {
            node_name: dict(expected_params)
            for node_name, expected_params in smoke.STAGE_K_EXPECTED_NODE_PRIVATE_PARAMS.items()
        },
        "mapping_session_id": "",
    }


class _WaitClient:
    def __init__(self, job):
        self.job = job

    def get_slam_job(self, *, job_id, robot_id):
        del job_id, robot_id
        return SimpleNamespace(found=True, message="ok", error_code="", job=self.job)


def _terminal_verify_job(**overrides):
    values = {
        "job_id": "job-1",
        "robot_id": "CR-001",
        "operation": 9,
        "operation_name": "verify_map_revision",
        "requested_map_name": "demo_map",
        "requested_map_revision_id": "rev_demo_01",
        "resolved_map_name": "demo_map",
        "resolved_map_revision_id": "rev_demo_01",
        "description": "smoke:verify_map_revision",
        "job_state": "succeeded",
        "status": "succeeded",
        "phase": "done",
        "workflow_phase": "done",
        "done": True,
        "success": True,
        "result_success": True,
        "error_code": "",
        "message": "completed",
        "result_code": "ok",
        "result_message": "completed",
        "manual_assist_required": False,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _wait_for_verify(job):
    return smoke.wait_for_job(
        client=_WaitClient(job),
        job_id="job-1",
        robot_id="CR-001",
        timeout_s=0.1,
        poll_interval_s=0.01,
        expected_operation_name="verify_map_revision",
        expected_map_name="demo_map",
        expected_map_revision_id="rev_demo_01",
        expected_description="smoke:verify_map_revision",
    )


class RuntimeSmokeIdentityFailClosedTest(unittest.TestCase):
    def test_read_identity_failure_prevents_actions(self):
        client = mock.Mock()
        client.get_ros_topology_identity.return_value = _valid_topology_snapshot()
        mismatch_checks = [
            {
                "name": "slam_status",
                "ok": False,
                "issues": ["robot_id mismatch expected=CR-001 observed=CR-999"],
                "response": {
                    "success": True,
                    "state": {"robot_id": "CR-999"},
                },
            },
            {
                "name": "odometry_status",
                "ok": True,
                "issues": [],
                "response": {
                    "success": True,
                    "state": {"robot_id": "CR-001"},
                },
            },
        ]
        args = SimpleNamespace(
            profile="task_ready",
            robot_id="CR-001",
            task_id=0,
            service_timeout=1.0,
            ignore_warning=[],
            actions=["start_mapping"],
            run_task_cycle=False,
        )
        run_actions = mock.Mock(return_value=[])

        with mock.patch.object(smoke, "BackendRuntimeSmokeClient", return_value=client), mock.patch.object(
            smoke,
            "run_read_checks",
            return_value=mismatch_checks,
        ), mock.patch.object(smoke, "run_actions", run_actions):
            try:
                smoke.build_report(args)
            except (RuntimeError, ValueError):
                pass

        run_actions.assert_not_called()

    def test_write_topology_failure_prevents_reads_and_actions(self):
        client = mock.Mock()
        topology = _valid_topology_snapshot()
        topology["service_providers"][
            "/clean_robot_server/app/map_server"
        ] = "/foreign_map_server"
        client.get_ros_topology_identity.return_value = topology
        args = SimpleNamespace(
            profile="task_ready",
            robot_id="CR-001",
            task_id=0,
            service_timeout=1.0,
            ignore_warning=[],
            actions=["start_mapping"],
            run_task_cycle=False,
        )
        run_read_checks = mock.Mock(return_value=[])
        run_actions = mock.Mock(return_value=[])

        with mock.patch.object(
            smoke,
            "BackendRuntimeSmokeClient",
            return_value=client,
        ), mock.patch.object(
            smoke,
            "run_read_checks",
            run_read_checks,
        ), mock.patch.object(smoke, "run_actions", run_actions):
            report = smoke.build_report(args)

        run_read_checks.assert_not_called()
        run_actions.assert_not_called()
        topology_check = next(
            item
            for item in report["checks"]
            if item["name"] == "commercial_write_ros_topology_identity"
        )
        self.assertFalse(topology_check["ok"])
        self.assertFalse(report["summary"]["ok"])

    def test_successful_terminal_job_for_another_robot_is_failure(self):
        foreign_job = SimpleNamespace(
            job_id="job-1",
            robot_id="CR-999",
            job_state="succeeded",
            status="succeeded",
            done=True,
            success=True,
            result_success=True,
        )

        result = smoke.wait_for_job(
            client=_WaitClient(foreign_job),
            job_id="job-1",
            robot_id="CR-001",
            timeout_s=0.1,
            poll_interval_s=0.01,
        )

        self.assertFalse(result["ok"])
        self.assertEqual(result.get("terminal_state"), "identity_mismatch")
        self.assertTrue(
            any("identity mismatch" in issue for issue in list(result.get("issues") or []))
        )

    def test_wait_for_job_accepts_an_exact_job_contract(self):
        result = _wait_for_verify(_terminal_verify_job())

        self.assertTrue(result["ok"])
        self.assertEqual(result.get("terminal_state"), "succeeded")

    def test_wait_for_job_rejects_wrong_operation_map_or_revision(self):
        cases = (
            (
                "operation",
                {"operation": 10, "operation_name": "activate_map_revision"},
                "operation mismatch",
            ),
            (
                "requested map",
                {"requested_map_name": "other_map"},
                "requested map_name mismatch",
            ),
            (
                "resolved map",
                {"resolved_map_name": "other_map"},
                "resolved map_name mismatch",
            ),
            (
                "requested revision",
                {"requested_map_revision_id": "rev_other"},
                "requested map_revision_id mismatch",
            ),
            (
                "resolved revision",
                {"resolved_map_revision_id": "rev_other"},
                "resolved map_revision_id mismatch",
            ),
        )
        for label, mutation, expected_issue in cases:
            with self.subTest(label=label):
                result = _wait_for_verify(_terminal_verify_job(**mutation))
                self.assertFalse(result["ok"])
                self.assertEqual(result.get("terminal_state"), "contract_mismatch")
                self.assertTrue(
                    any(expected_issue in issue for issue in list(result.get("issues") or []))
                )

    def test_wait_for_job_rejects_failed_state_even_with_success_flags(self):
        result = _wait_for_verify(
            _terminal_verify_job(job_state="failed", status="failed")
        )

        self.assertFalse(result["ok"])
        self.assertEqual(result.get("terminal_state"), "failed")
        self.assertTrue(
            any("conflicts with success" in issue for issue in list(result.get("issues") or []))
        )


if __name__ == "__main__":
    unittest.main()

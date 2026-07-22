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

import run_revision_workflow_acceptance as workflow


def _identity_snapshot(robot_id="CR-001"):
    slam_state = {
        "robot_id": robot_id,
        "desired_mode": "localization",
        "current_mode": "localization",
        "runtime_mode": "localization",
        "mapping_session_active": False,
        "active_map_name": "stable_map",
        "active_map_revision_id": "rev_stable_01",
        "active_map_id": "stable-map-id",
        "active_map_md5": "stable-map-md5",
        "runtime_map_name": "stable_map",
        "runtime_map_revision_id": "rev_stable_01",
        "runtime_map_id": "stable-map-id",
        "runtime_map_md5": "stable-map-md5",
        "pending_map_name": "",
        "pending_map_revision_id": "",
        "pending_map_switch_status": "",
        "active_job_id": "",
        "active_job_status": "",
        "active_job_phase": "",
        "localization_valid": True,
        "runtime_map_match": True,
        "task_ready": True,
    }
    odometry_state = {"robot_id": robot_id, "odom_valid": True}
    return {
        "slam": slam_state,
        "odometry": odometry_state,
        "readiness": {
            "task_id": 0,
            "task_map_name": "stable_map",
            "task_map_revision_id": "rev_stable_01",
            "active_map_name": "stable_map",
            "active_map_revision_id": "rev_stable_01",
            "can_start_task": True,
        },
        "revision_scope": {},
        "checks": {
            "slam": {
                "name": "slam_status",
                "ok": robot_id == "CR-001",
                "issues": [] if robot_id == "CR-001" else ["robot_id mismatch"],
                "response": {"success": True, "state": slam_state},
            },
            "odometry": {
                "name": "odometry_status",
                "ok": robot_id == "CR-001",
                "issues": [] if robot_id == "CR-001" else ["robot_id mismatch"],
                "response": {"success": True, "state": odometry_state},
            },
            "readiness": {
                "name": "system_readiness",
                "ok": True,
                "issues": [],
                "response": {"success": True, "readiness": {"can_start_task": True}},
            },
        },
    }


def _mapping_snapshot():
    snapshot = _identity_snapshot()
    snapshot["slam"] = dict(snapshot["slam"])
    snapshot["slam"].update(
        {
            "desired_mode": "mapping",
            "current_mode": "mapping",
            "runtime_mode": "mapping",
            "mapping_session_active": True,
            "localization_valid": False,
            "runtime_map_match": False,
            "runtime_map_name": "",
            "runtime_map_revision_id": "",
            "runtime_map_id": "",
            "runtime_map_md5": "",
        }
    )
    snapshot["checks"]["slam"]["response"]["state"] = dict(snapshot["slam"])
    return snapshot


def _post_stop_snapshot():
    snapshot = _identity_snapshot()
    snapshot["slam"] = dict(snapshot["slam"])
    snapshot["slam"].update(
        {
            "current_mode": "localization",
            "mapping_session_active": False,
            "localization_valid": False,
            "runtime_map_match": False,
            "runtime_map_name": "",
            "runtime_map_revision_id": "",
            "runtime_map_id": "",
            "runtime_map_md5": "",
        }
    )
    snapshot["checks"]["slam"]["response"]["state"] = dict(snapshot["slam"])
    return snapshot


class _CandidateMapClient:
    def __init__(self):
        self._mapping_session_ids = [
            "job-start_mapping",
            "job-start_mapping",
            "",
        ]

    def get_mapping_session_id(self):
        return self._mapping_session_ids.pop(0)

    def get_map_view(self, map_name, map_revision_id=""):
        return SimpleNamespace(
            success=True,
            message="ok",
            map=SimpleNamespace(
                map_name=map_name,
                map_revision_id=map_revision_id,
                lifecycle_status="saved_unverified",
                verification_status="pending",
                enabled=True,
                is_active=False,
            ),
        )


def _args(**overrides):
    values = {
        "robot_id": "CR-001",
        "task_id": 0,
        "ignore_warning": [],
        "map_name": "stable_map",
        "map_revision_id": "rev_candidate_02",
        "save_map_name": "new_map",
        "frame_id": "map",
        "description_prefix": "identity-test",
        "job_timeout": 1.0,
        "poll_interval": 0.01,
        "resume_from_checkpoint": "",
        "pause_after_start_mapping": False,
        "checkpoint_path": "",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _action(name, *, ok, revision_id=""):
    return {
        "name": name,
        "ok": bool(ok),
        "issues": [] if ok else ["forced failure"],
        "submit": {
            "accepted": bool(ok),
            "message": "accepted" if ok else "forced failure",
            "error_code": "" if ok else "forced_failure",
            "job_id": "job-%s" % name if ok else "",
            "map_name": "new_map",
        },
        "job": {
            "ok": bool(ok),
            "terminal_state": "succeeded" if ok else "failed",
            "response": {
                "job": {
                    "robot_id": "CR-001",
                    "resolved_map_revision_id": revision_id,
                    "requested_map_revision_id": revision_id,
                }
            },
        },
    }


class RevisionIdentityFailClosedTest(unittest.TestCase):
    def test_first_identity_failure_submits_no_action(self):
        submit = mock.Mock(return_value=_action("verify_map_revision", ok=True))
        with mock.patch.object(
            workflow,
            "capture_snapshot",
            return_value=_identity_snapshot("CR-999"),
        ), mock.patch.object(workflow, "_submit_and_wait", submit):
            try:
                workflow._run_verify_revision_profile(object(), _args())
            except (RuntimeError, ValueError):
                pass

        submit.assert_not_called()

    def test_failed_activate_does_not_continue_to_prepare_for_task(self):
        calls = []

        def submit(_client, *, action_name, **_kwargs):
            calls.append(action_name)
            return _action(action_name, ok=False)

        with mock.patch.object(
            workflow,
            "capture_snapshot",
            return_value=_identity_snapshot(),
        ), mock.patch.object(workflow, "_submit_and_wait", side_effect=submit):
            workflow._run_activate_revision_profile(
                object(),
                _args(task_id=42),
                prepare_after_activate=True,
            )

        self.assertEqual(calls, ["activate_map_revision"])

    def test_failed_verify_does_not_continue_to_activate_or_prepare(self):
        calls = []

        def submit(_client, *, action_name, **_kwargs):
            calls.append(action_name)
            if action_name == "save_mapping":
                return _action(action_name, ok=True, revision_id="rev_candidate_02")
            if action_name == "verify_map_revision":
                return _action(action_name, ok=False, revision_id="rev_candidate_02")
            return _action(action_name, ok=True)

        snapshots = [
            _mapping_snapshot(),
            _mapping_snapshot(),
            _post_stop_snapshot(),
            _identity_snapshot(),
        ]
        with mock.patch.object(
            workflow,
            "capture_snapshot",
            side_effect=snapshots,
        ), mock.patch.object(
            workflow,
            "_submit_and_wait",
            side_effect=submit,
        ), mock.patch.object(
            workflow,
            "_load_mapping_checkpoint_context",
            return_value={
                "payload": {},
                "profile_name": "mapping_save_verify_activate",
                "save_map_name": "new_map",
                "mapping_session_id": "job-start_mapping",
                "pre_snapshot": _identity_snapshot(),
                "start_action": _action("start_mapping", ok=True),
                "mapping_snapshot": _mapping_snapshot(),
            },
        ), mock.patch.object(
            workflow,
            "_require_live_start_mapping_job",
        ), mock.patch.object(
            workflow,
            "_mark_checkpoint_resume_started",
            return_value={},
        ), mock.patch.object(
            workflow,
            "_finalize_consumed_checkpoint",
        ):
            workflow._run_mapping_workflow_profile(
                _CandidateMapClient(),
                _args(
                    task_id=42,
                    resume_from_checkpoint="/tmp/revision-checkpoint.json",
                ),
                verify_after_save=True,
                prepare_after_activate=False,
            )

        self.assertIn("verify_map_revision", calls)
        self.assertNotIn("activate_map_revision", calls)
        self.assertNotIn("prepare_for_task", calls)


if __name__ == "__main__":
    unittest.main()

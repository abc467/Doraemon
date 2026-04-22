#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import unittest


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
PKG_DIR = os.path.dirname(THIS_DIR)
TOOLS_DIR = os.path.join(PKG_DIR, "tools")

if TOOLS_DIR not in sys.path:
    sys.path.insert(0, TOOLS_DIR)

from run_backend_runtime_smoke import (
    _latest_head_scope_from_map_msg,
    _check_readiness,
    _check_slam,
    _task_cycle_issues,
    _job_to_dict,
    build_arg_parser,
    build_runtime_revision_scope,
    filter_ignored_warnings,
    job_succeeded,
    job_terminal_snapshot,
    parse_actions,
    run_actions,
    validate_args,
)


class _FakeJob:
    def __init__(self, **kwargs):
        self.done = kwargs.get("done", False)
        self.status = kwargs.get("status", "")
        self.job_state = kwargs.get("job_state", "")
        self.success = kwargs.get("success", False)
        self.result_success = kwargs.get("result_success", False)
        self.requested_map_revision_id = kwargs.get("requested_map_revision_id", "")
        self.resolved_map_revision_id = kwargs.get("resolved_map_revision_id", "")


class _FakePayload:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


class BackendRuntimeSmokeToolTest(unittest.TestCase):
    def test_parse_actions_accepts_supported_actions(self):
        self.assertEqual(parse_actions("prepare_for_task,relocalize"), ["prepare_for_task", "relocalize"])
        self.assertEqual(
            parse_actions("verify_map_revision,activate_map_revision"),
            ["verify_map_revision", "activate_map_revision"],
        )

    def test_parse_actions_rejects_unknown_action(self):
        with self.assertRaises(ValueError):
            parse_actions("prepare_for_task,explode_robot")

    def test_filter_ignored_warnings_drops_known_station_warning(self):
        messages = ["station_status stale or missing", "odom stale"]
        self.assertEqual(
            filter_ignored_warnings(messages, ["station_status stale or missing"]),
            ["odom stale"],
        )

    def test_job_terminal_snapshot_prefers_done_flag(self):
        terminal, state = job_terminal_snapshot(_FakeJob(done=True, success=True))
        self.assertTrue(terminal)
        self.assertEqual(state, "succeeded")

    def test_job_succeeded_handles_manual_assist_as_failure(self):
        job = _FakeJob(done=False, job_state="manual_assist_required", result_success=False, success=False)
        self.assertFalse(job_succeeded(job))

    def test_job_succeeded_accepts_result_success(self):
        job = _FakeJob(done=True, job_state="failed", result_success=True, success=False)
        self.assertTrue(job_succeeded(job))

    def test_job_to_dict_includes_revision_scope(self):
        payload = _job_to_dict(
            _FakeJob(
                requested_map_revision_id="rev_requested_01",
                resolved_map_revision_id="rev_resolved_02",
            )
        )

        self.assertEqual(payload["requested_map_revision_id"], "rev_requested_01")
        self.assertEqual(payload["resolved_map_revision_id"], "rev_resolved_02")
        self.assertEqual(payload["revision_scope"]["requested"]["revision_id"], "rev_requested_01")
        self.assertEqual(payload["revision_scope"]["resolved"]["revision_id"], "rev_resolved_02")

    def test_check_slam_reports_revision_scope_in_response(self):
        resp = _FakePayload(
            success=True,
            message="",
            state=_FakePayload(
                current_mode="localization",
                runtime_mode="localization",
                workflow_state="idle",
                workflow_phase="ready",
                active_map_name="site_a",
                runtime_map_name="site_a",
                active_map_revision_id="rev_site_a_01",
                runtime_map_revision_id="rev_site_a_01",
                pending_map_name="site_b",
                pending_map_revision_id="rev_site_b_02",
                pending_map_switch_status="verifying",
                localization_state="localized",
                localization_valid=True,
                runtime_map_ready=True,
                runtime_map_match=True,
                busy=False,
                task_ready=True,
                manual_assist_required=False,
                can_verify_map_revision=True,
                can_activate_map_revision=True,
                can_start_mapping=True,
                can_save_mapping=False,
                can_stop_mapping=False,
                blocking_reasons=[],
                warnings=[],
                stamp=_FakePayload(secs=0, nsecs=0),
            ),
        )

        result = _check_slam(resp, ignored_warnings=[])
        state = result["response"]["state"]
        self.assertEqual(state["active_map_revision_id"], "rev_site_a_01")
        self.assertEqual(state["runtime_map_revision_id"], "rev_site_a_01")
        self.assertEqual(state["pending_map_name"], "site_b")
        self.assertEqual(state["pending_map_revision_id"], "rev_site_b_02")
        self.assertEqual(state["pending_map_switch_status"], "verifying")
        self.assertEqual(state["revision_scope"]["active"]["revision_id"], "rev_site_a_01")
        self.assertEqual(state["revision_scope"]["runtime"]["revision_id"], "rev_site_a_01")
        self.assertEqual(state["revision_scope"]["pending_target"]["revision_id"], "rev_site_b_02")
        self.assertTrue(state["can_verify_map_revision"])
        self.assertTrue(state["can_activate_map_revision"])
        self.assertTrue(result["ok"])

    def test_validate_args_accepts_map_revision_id_for_revision_scoped_actions(self):
        parser = build_arg_parser()
        args = parser.parse_args(["--actions", "verify_map_revision", "--map-revision-id", "rev_demo_01"])

        validate_args(args)

        self.assertEqual(args.map_revision_id, "rev_demo_01")

    def test_validate_args_rejects_missing_target_for_revision_scoped_actions(self):
        parser = build_arg_parser()
        args = parser.parse_args(["--actions", "activate_map_revision"])

        with self.assertRaises(ValueError):
            validate_args(args)

    def test_validate_args_requires_task_id_for_task_cycle(self):
        parser = build_arg_parser()
        args = parser.parse_args(["--run-task-cycle", "--ops-db-path", "/tmp/operations.db"])

        with self.assertRaises(ValueError):
            validate_args(args)

    def test_validate_args_requires_ops_db_path_for_task_cycle(self):
        parser = build_arg_parser()
        args = parser.parse_args(["--run-task-cycle", "--task-id", "9"])

        with self.assertRaises(ValueError):
            validate_args(args)

    def test_check_readiness_reports_revision_scope_in_response(self):
        resp = _FakePayload(
            success=True,
            message="",
            readiness=_FakePayload(
                overall_ready=True,
                can_start_task=True,
                mission_state="idle",
                phase="ready",
                public_state="ready",
                executor_state="standby",
                task_map_revision_id="rev_task_01",
                active_map_revision_id="rev_task_01",
                runtime_map_revision_id="rev_task_01",
                active_map_name="site_a",
                runtime_map_name="site_a",
                blocking_reasons=[],
                warnings=[],
                stamp=_FakePayload(secs=0, nsecs=0),
            ),
        )

        result = _check_readiness(resp, ignored_warnings=[])
        readiness = result["response"]["readiness"]
        self.assertEqual(readiness["task_map_revision_id"], "rev_task_01")
        self.assertEqual(readiness["active_map_revision_id"], "rev_task_01")
        self.assertEqual(readiness["runtime_map_revision_id"], "rev_task_01")
        self.assertEqual(readiness["revision_scope"]["task_binding"]["revision_id"], "rev_task_01")
        self.assertEqual(readiness["revision_scope"]["active"]["revision_id"], "rev_task_01")
        self.assertEqual(readiness["revision_scope"]["runtime"]["revision_id"], "rev_task_01")
        self.assertTrue(result["ok"])

    def test_build_runtime_revision_scope_merges_slam_and_readiness(self):
        slam_check = _check_slam(
            _FakePayload(
                success=True,
                message="",
                state=_FakePayload(
                    active_map_name="site_a",
                    runtime_map_name="site_a",
                    active_map_revision_id="rev_site_a_01",
                    runtime_map_revision_id="rev_site_a_01",
                    pending_map_name="site_b",
                    pending_map_revision_id="rev_site_b_02",
                    pending_map_switch_status="verifying",
                    localization_valid=True,
                    runtime_map_ready=True,
                    runtime_map_match=True,
                    manual_assist_required=False,
                    blocking_reasons=[],
                    warnings=[],
                    stamp=_FakePayload(secs=0, nsecs=0),
                ),
            ),
            ignored_warnings=[],
        )
        readiness_check = _check_readiness(
            _FakePayload(
                success=True,
                message="",
                readiness=_FakePayload(
                    overall_ready=True,
                    can_start_task=True,
                    task_map_revision_id="rev_site_a_01",
                    active_map_revision_id="rev_site_a_01",
                    runtime_map_revision_id="rev_site_a_01",
                    active_map_name="site_a",
                    runtime_map_name="site_a",
                    blocking_reasons=[],
                    warnings=[],
                    stamp=_FakePayload(secs=0, nsecs=0),
                ),
            ),
            ignored_warnings=[],
        )

        scope = build_runtime_revision_scope(
            [slam_check, readiness_check],
            latest_head={
                "map_name": "site_a",
                "revision_id": "rev_site_a_02",
                "lifecycle_status": "available",
                "verification_status": "verified",
                "source": "map_server.get",
            },
        )

        self.assertEqual(scope["task_binding"]["revision_id"], "rev_site_a_01")
        self.assertEqual(scope["active"]["revision_id"], "rev_site_a_01")
        self.assertEqual(scope["runtime"]["revision_id"], "rev_site_a_01")
        self.assertEqual(scope["pending_target"]["revision_id"], "rev_site_b_02")
        self.assertEqual(scope["latest_head"]["revision_id"], "rev_site_a_02")
        self.assertEqual(scope["latest_head"]["verification_status"], "verified")

    def test_task_cycle_issues_accepts_done_and_idle_postconditions(self):
        issues = _task_cycle_issues(
            task_id=9,
            running_seen=True,
            run_id="run_1",
            final_run_row={
                "state": "DONE",
                "plan_id": "plan_1",
                "map_revision_id": "rev_demo_01",
            },
            final_runtime_row={
                "active_run_id": "",
                "mission_state": "IDLE",
                "phase": "IDLE",
                "public_state": "IDLE",
                "executor_state": "IDLE",
            },
            post_readiness={"ok": True},
        )

        self.assertEqual(issues, [])

    def test_task_cycle_issues_reports_terminal_runtime_and_post_readiness_failures(self):
        issues = _task_cycle_issues(
            task_id=9,
            running_seen=False,
            run_id="",
            final_run_row={"state": "FAILED"},
            final_runtime_row={
                "active_run_id": "run_1",
                "mission_state": "RUNNING",
                "phase": "IDLE",
                "public_state": "RUNNING",
                "executor_state": "DONE",
            },
            post_readiness={"ok": False},
        )

        self.assertTrue(any("task never reached running state" in item for item in issues))
        self.assertTrue(any("task run_id was never observed" in item for item in issues))
        self.assertTrue(any("mission_run terminal state=FAILED" in item for item in issues))
        self.assertTrue(any("runtime not idle after task" in item for item in issues))
        self.assertTrue(any("post task readiness failed" in item for item in issues))

    def test_latest_head_scope_from_map_msg_prefers_latest_head_fields(self):
        msg = _FakePayload(
            map_name="site_a",
            map_revision_id="rev_site_a_01",
            latest_head_revision_id="rev_site_a_03",
            latest_head_lifecycle_status="available",
            latest_head_verification_status="verified",
            lifecycle_status="available",
            verification_status="verified",
            is_latest_head=False,
        )

        scope = _latest_head_scope_from_map_msg(msg)

        self.assertEqual(scope["map_name"], "site_a")
        self.assertEqual(scope["revision_id"], "rev_site_a_03")
        self.assertEqual(scope["lifecycle_status"], "available")
        self.assertEqual(scope["verification_status"], "verified")

    def test_run_actions_preserves_submit_map_revision_id(self):
        class _FakeClient(object):
            def submit_action(self, **kwargs):
                self.submit_kwargs = dict(kwargs)
                return _FakePayload(
                    accepted=True,
                    message="accepted",
                    error_code="",
                    job_id="job_1",
                    map_name=str(kwargs.get("map_name") or ""),
                    map_revision_id="rev_saved_demo_01",
                    operation=4,
                    job=_FakeJob(
                        requested_map_revision_id="rev_saved_demo_01",
                        resolved_map_revision_id="rev_saved_demo_01",
                    ),
                )

            def get_slam_job(self, job_id, robot_id):
                return _FakePayload(
                    found=True,
                    message="done",
                    error_code="",
                    job=_FakeJob(
                        done=True,
                        success=True,
                        requested_map_revision_id="rev_saved_demo_01",
                        resolved_map_revision_id="rev_saved_demo_01",
                    ),
                )

        args = _FakePayload(
            actions=["save_mapping"],
            robot_id="local_robot",
            map_name="demo_map",
            map_revision_id="",
            frame_id="map",
            save_map_name="demo_map",
            description_prefix="smoke",
            set_active=False,
            has_initial_pose=False,
            initial_pose_x=0.0,
            initial_pose_y=0.0,
            initial_pose_yaw=0.0,
            include_unfinished_submaps=True,
            set_active_on_save=False,
            switch_to_localization_after_save=False,
            relocalize_after_switch=False,
            job_timeout=1.0,
            poll_interval=0.01,
        )

        results = run_actions(_FakeClient(), args)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["submit"]["map_revision_id"], "rev_saved_demo_01")
        self.assertTrue(results[0]["ok"])


if __name__ == "__main__":
    unittest.main()

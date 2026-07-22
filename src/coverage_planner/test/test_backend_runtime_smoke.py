#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import copy
import os
import sqlite3
import sys
import tempfile
import unittest
from unittest import mock


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
PKG_DIR = os.path.dirname(THIS_DIR)
PKG_SRC_DIR = os.path.join(PKG_DIR, "src")
TOOLS_DIR = os.path.join(PKG_DIR, "tools")

if PKG_SRC_DIR not in sys.path:
    sys.path.insert(0, PKG_SRC_DIR)
if TOOLS_DIR not in sys.path:
    sys.path.insert(0, TOOLS_DIR)

from run_backend_runtime_smoke import (
    BackendRuntimeSmokeClient,
    STAGE_K_EXPECTED_NODE_PRIVATE_PARAMS,
    STAGE_K_EXPECTED_SERVICE_PROVIDERS,
    _check_active_map_persistent_asset,
    _check_commercial_ros_topology_identity,
    _load_task_cycle_db_state,
    _latest_head_scope_from_map_msg,
    _check_readiness,
    _check_slam,
    _runtime_row_is_idle,
    _task_cycle_issues,
    _task_cycle_run_binding_issues,
    _job_to_dict,
    accepted_submit_consistency_issues,
    build_arg_parser,
    build_report,
    build_runtime_revision_scope,
    filter_ignored_warnings,
    job_contract_issues,
    job_succeeded,
    job_terminal_consistency_issues,
    job_terminal_snapshot,
    parse_actions,
    require_local_ros_master_uri,
    run_actions,
    run_task_cycle,
    validate_args,
    wait_for_job,
)


class _FakeJob:
    def __init__(self, **kwargs):
        state = str(kwargs.get("job_state", "") or kwargs.get("status", "") or "")
        terminal_phase = {
            "succeeded": "done",
            "failed": "failed",
            "canceled": "canceled",
            "manual_assist_required": "manual_assist_required",
        }.get(state, "")
        default_error_code = (
            "test_error"
            if state in {"failed", "canceled", "manual_assist_required"}
            else ""
        )
        self.job_id = kwargs.get("job_id", "")
        self.robot_id = kwargs.get("robot_id", "")
        self.operation = kwargs.get("operation", 0)
        self.operation_name = kwargs.get("operation_name", "")
        self.requested_map_name = kwargs.get("requested_map_name", "")
        self.done = kwargs.get("done", False)
        self.status = kwargs.get("status", "")
        self.phase = kwargs.get("phase", terminal_phase)
        self.job_state = kwargs.get("job_state", "")
        self.workflow_phase = kwargs.get("workflow_phase", terminal_phase)
        self.success = kwargs.get("success", False)
        self.result_success = kwargs.get("result_success", False)
        self.error_code = kwargs.get("error_code", default_error_code)
        self.message = kwargs.get("message", "completed" if state == "succeeded" else "failed")
        self.result_code = kwargs.get(
            "result_code",
            "ok" if state == "succeeded" else default_error_code,
        )
        self.result_message = kwargs.get("result_message", self.message)
        self.requested_map_revision_id = kwargs.get("requested_map_revision_id", "")
        self.resolved_map_name = kwargs.get("resolved_map_name", "")
        self.resolved_map_revision_id = kwargs.get("resolved_map_revision_id", "")
        self.description = kwargs.get("description", "")
        self.manual_assist_required = kwargs.get(
            "manual_assist_required",
            state == "manual_assist_required",
        )


class _FakePayload:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


def _action_args(action_name, **overrides):
    values = {
        "actions": [action_name],
        "robot_id": "CR-001",
        "map_name": "demo_map",
        "map_revision_id": "",
        "frame_id": "map",
        "save_map_name": "demo_map",
        "description_prefix": "smoke",
        "set_active": False,
        "has_initial_pose": False,
        "initial_pose_x": 0.0,
        "initial_pose_y": 0.0,
        "initial_pose_yaw": 0.0,
        "include_unfinished_submaps": True,
        "set_active_on_save": False,
        "switch_to_localization_after_save": False,
        "relocalize_after_switch": False,
        "job_timeout": 1.0,
        "poll_interval": 0.01,
    }
    values.update(overrides)
    return _FakePayload(**values)


def _valid_map_asset_response(**overrides):
    asset = {
        "map_name": "site_a",
        "map_revision_id": "rev_site_a_01",
        "lifecycle_status": "available",
        "verification_status": "verified",
        "is_active": True,
        "is_latest_head": True,
        "has_newer_head_revision": False,
        "active_revision_id": "rev_site_a_01",
        "latest_head_revision_id": "rev_site_a_01",
        "latest_head_lifecycle_status": "available",
        "latest_head_verification_status": "verified",
    }
    asset.update(overrides)
    return _FakePayload(
        success=True,
        message="ok",
        map=_FakePayload(**asset),
    )


def _valid_commercial_topology_snapshot():
    return {
        "service_providers": dict(STAGE_K_EXPECTED_SERVICE_PROVIDERS),
        "node_robot_ids": {
            node_name: "CR-001"
            for _service_name, node_name in STAGE_K_EXPECTED_SERVICE_PROVIDERS
        },
        "node_private_params": {
            node_name: dict(expected_params)
            for node_name, expected_params in STAGE_K_EXPECTED_NODE_PRIVATE_PARAMS.items()
        },
        "mapping_session_id": "",
    }


def _valid_task_ready_checks():
    return [
        {
            "name": "slam_status",
            "ok": True,
            "issues": [],
            "response": {
                "success": True,
                "state": {
                    "robot_id": "CR-001",
                    "active_map_name": "site_a",
                    "active_map_revision_id": "rev_site_a_01",
                    "runtime_map_name": "site_a",
                    "runtime_map_revision_id": "rev_site_a_01",
                },
            },
        },
        {
            "name": "odometry_status",
            "ok": True,
            "issues": [],
            "response": {"success": True, "state": {"robot_id": "CR-001"}},
        },
        {
            "name": "system_readiness",
            "ok": True,
            "issues": [],
            "response": {
                "success": True,
                "readiness": {
                    "active_map_name": "site_a",
                    "active_map_revision_id": "rev_site_a_01",
                    "runtime_map_name": "site_a",
                    "runtime_map_revision_id": "rev_site_a_01",
                    "task_map_revision_id": "rev_site_a_01",
                },
            },
        },
    ]


class BackendRuntimeSmokeToolTest(unittest.TestCase):
    def test_ros_master_must_be_loopback(self):
        with mock.patch.dict(os.environ, {"ROS_MASTER_URI": "http://127.0.0.1:11311"}):
            self.assertEqual(require_local_ros_master_uri(), "http://127.0.0.1:11311")
        with mock.patch.dict(os.environ, {"ROS_MASTER_URI": "http://192.168.1.9:11311"}):
            with self.assertRaisesRegex(ValueError, "ROS_MASTER_URI must be local"):
                require_local_ros_master_uri()

    def test_commercial_topology_requires_canonical_downstream_services_and_identities(self):
        expected_downstream = {
            "/cartographer/runtime/app/operate": "/slam_runtime_manager",
            "/cartographer/runtime/app/submit_job": "/slam_runtime_manager",
            "/cartographer/runtime/app/get_job": "/slam_runtime_manager",
            "/cartographer/runtime/app/restart_localization": "/localization_lifecycle_manager",
            "/clean_robot_server/app/dock_calibration_command": "/dock_calibration_service",
            "/clean_robot_server/app/get_dock_calibration_status": "/dock_calibration_service",
        }
        self.assertEqual(
            {
                service_name: node_name
                for service_name, node_name in STAGE_K_EXPECTED_SERVICE_PROVIDERS
                if service_name in expected_downstream
            },
            expected_downstream,
        )
        valid = _check_commercial_ros_topology_identity(
            _valid_commercial_topology_snapshot(),
            "CR-001",
            check_name="commercial_read_ros_topology_identity",
            require_empty_mapping_session=True,
        )
        self.assertTrue(valid["ok"], msg=valid["issues"])

        for label, mutate, expected_issue in (
            (
                "runtime provider",
                lambda snapshot: snapshot["service_providers"].__setitem__(
                    "/cartographer/runtime/app/submit_job", "/foreign_runtime"
                ),
                "service provider mismatch",
            ),
            (
                "runtime identity",
                lambda snapshot: snapshot["node_robot_ids"].__setitem__(
                    "/slam_runtime_manager", "CR-999"
                ),
                "node robot_id mismatch",
            ),
            (
                "localization identity",
                lambda snapshot: snapshot["node_robot_ids"].__setitem__(
                    "/localization_lifecycle_manager", "CR-999"
                ),
                "node robot_id mismatch",
            ),
            (
                "dock identity",
                lambda snapshot: snapshot["node_robot_ids"].__setitem__(
                    "/dock_calibration_service", "CR-999"
                ),
                "node robot_id mismatch",
            ),
            (
                "slam API planning path",
                lambda snapshot: snapshot["node_private_params"][
                    "/slam_api_service"
                ].__setitem__("plan_db_path", "/tmp/planning.db"),
                "node private parameter mismatch",
            ),
            (
                "runtime repository map root missing",
                lambda snapshot: snapshot["node_private_params"][
                    "/slam_runtime_manager"
                ].pop("repo_map_root"),
                "node private parameter mismatch",
            ),
            (
                "map asset imports root",
                lambda snapshot: snapshot["node_private_params"][
                    "/map_asset_service"
                ].__setitem__("external_maps_root", "/data/imports"),
                "node private parameter mismatch",
            ),
            (
                "dock calibration storage",
                lambda snapshot: snapshot["node_private_params"][
                    "/dock_calibration_service"
                ].__setitem__("storage_path", "/tmp/dock.yaml"),
                "node private parameter mismatch",
            ),
            (
                "unexpected service owner",
                lambda snapshot: snapshot["service_providers"].__setitem__(
                    "/foreign/write", "/foreign_node"
                ),
                "unexpected service provider ownership",
            ),
            (
                "unexpected identity owner",
                lambda snapshot: snapshot["node_robot_ids"].__setitem__(
                    "/foreign_node", "CR-001"
                ),
                "unexpected node robot_id ownership",
            ),
            (
                "unexpected private parameter owner",
                lambda snapshot: snapshot["node_private_params"].__setitem__(
                    "/foreign_node", {"plan_db_path": "/data/coverage/planning.db"}
                ),
                "unexpected node private parameter ownership",
            ),
            (
                "unexpected private parameter",
                lambda snapshot: snapshot["node_private_params"][
                    "/coverage_task_manager"
                ].__setitem__("foreign_db_path", "/data/coverage/planning.db"),
                "unexpected node private parameter ownership",
            ),
        ):
            with self.subTest(label=label):
                snapshot = _valid_commercial_topology_snapshot()
                mutate(snapshot)
                result = _check_commercial_ros_topology_identity(
                    snapshot,
                    "CR-001",
                    check_name="commercial_read_ros_topology_identity",
                    require_empty_mapping_session=True,
                )
                self.assertFalse(result["ok"])
                self.assertTrue(
                    any(expected_issue in issue for issue in result["issues"]),
                    msg=result["issues"],
                )

    def test_topology_client_reads_exact_private_storage_parameters(self):
        expected_snapshot = _valid_commercial_topology_snapshot()
        param_values = {
            "%s/robot_id" % node_name: "CR-001"
            for node_name in expected_snapshot["node_robot_ids"]
        }
        for node_name, expected_params in STAGE_K_EXPECTED_NODE_PRIVATE_PARAMS.items():
            for param_name, expected_value in expected_params.items():
                param_values["%s/%s" % (node_name, param_name)] = expected_value
        param_values["/cartographer/runtime/mapping_session_id"] = ""

        fake_rospy = mock.Mock()
        fake_rospy.get_param.side_effect = (
            lambda param_name, default=None: param_values.get(param_name, default)
        )
        fake_rosservice = mock.Mock()
        fake_rosservice.get_service_node.side_effect = dict(
            STAGE_K_EXPECTED_SERVICE_PROVIDERS
        ).__getitem__
        client = object.__new__(BackendRuntimeSmokeClient)
        client.rospy = fake_rospy

        with mock.patch.dict(sys.modules, {"rosservice": fake_rosservice}):
            snapshot = client.get_ros_topology_identity()

        self.assertEqual(snapshot, expected_snapshot)

    def test_active_map_persistent_asset_requires_exact_verified_active_head(self):
        client = mock.Mock()
        client.get_map_view.return_value = _valid_map_asset_response()
        revision_scope = {
            "active": {
                "map_name": "site_a",
                "revision_id": "rev_site_a_01",
            }
        }

        result = _check_active_map_persistent_asset(client, revision_scope)

        client.get_map_view.assert_called_once_with("site_a", "rev_site_a_01")
        self.assertTrue(result["ok"], msg=result["issues"])
        self.assertEqual(
            result["response"]["latest_head"]["revision_id"],
            "rev_site_a_01",
        )

    def test_active_map_persistent_asset_fails_closed_on_rpc_and_response_errors(self):
        revision_scope = {
            "active": {
                "map_name": "site_a",
                "revision_id": "rev_site_a_01",
            }
        }
        client = mock.Mock()
        client.get_map_view.side_effect = RuntimeError("rpc unavailable")
        result = _check_active_map_persistent_asset(client, revision_scope)
        self.assertFalse(result["ok"])
        self.assertTrue(any("map_server.get failed" in issue for issue in result["issues"]))

        client = mock.Mock()
        client.get_map_view.return_value = _FakePayload(
            success=False,
            message="not found",
            map=None,
        )
        result = _check_active_map_persistent_asset(client, revision_scope)
        self.assertFalse(result["ok"])
        self.assertTrue(any("success=false" in issue for issue in result["issues"]))
        self.assertTrue(any("missing map payload" in issue for issue in result["issues"]))

    def test_active_map_persistent_asset_rejects_every_scope_or_lifecycle_mismatch(self):
        revision_scope = {
            "active": {
                "map_name": "site_a",
                "revision_id": "rev_site_a_01",
            }
        }
        cases = (
            ("map_name", "other_site", "map_name mismatch"),
            ("map_revision_id", "rev_other_01", "map_revision_id mismatch"),
            ("lifecycle_status", "candidate", "lifecycle_status mismatch"),
            ("verification_status", "pending", "verification_status mismatch"),
            ("is_active", False, "is_active=false"),
            ("active_revision_id", "rev_other_01", "active_revision_id mismatch"),
            ("is_latest_head", False, "is_latest_head=false"),
            (
                "latest_head_revision_id",
                "rev_site_a_02",
                "latest_head_revision_id mismatch",
            ),
            (
                "latest_head_lifecycle_status",
                "candidate",
                "latest_head_lifecycle_status mismatch",
            ),
            (
                "latest_head_verification_status",
                "pending",
                "latest_head_verification_status mismatch",
            ),
            ("has_newer_head_revision", True, "has_newer_head_revision=true"),
        )
        for field_name, field_value, expected_issue in cases:
            with self.subTest(field=field_name):
                client = mock.Mock()
                client.get_map_view.return_value = _valid_map_asset_response(
                    **{field_name: field_value}
                )
                result = _check_active_map_persistent_asset(client, revision_scope)
                self.assertFalse(result["ok"])
                self.assertTrue(
                    any(expected_issue in issue for issue in result["issues"]),
                    msg=result["issues"],
                )

    def test_task_ready_report_applies_read_topology_and_persistent_asset_gates(self):
        client = mock.Mock()
        client.get_ros_topology_identity.return_value = _valid_commercial_topology_snapshot()
        client.get_map_view.return_value = _valid_map_asset_response()
        args = _FakePayload(
            profile="task_ready",
            robot_id="CR-001",
            task_id=7,
            service_timeout=1.0,
            ignore_warning=[],
            actions=[],
            run_task_cycle=False,
        )

        with mock.patch(
            "run_backend_runtime_smoke.BackendRuntimeSmokeClient",
            return_value=client,
        ), mock.patch(
            "run_backend_runtime_smoke.run_read_checks",
            return_value=_valid_task_ready_checks(),
        ):
            report = build_report(args)

        self.assertTrue(report["summary"]["ok"], msg=report["summary"]["issues"])
        self.assertTrue(
            next(
                item
                for item in report["checks"]
                if item["name"] == "commercial_read_ros_topology_identity"
            )["ok"]
        )
        self.assertTrue(
            next(
                item
                for item in report["checks"]
                if item["name"] == "active_map_persistent_asset"
            )["ok"]
        )
        client.get_map_view.assert_called_once_with("site_a", "rev_site_a_01")

    def test_persistent_asset_failure_blocks_programmatic_write_action(self):
        client = mock.Mock()
        client.get_ros_topology_identity.return_value = _valid_commercial_topology_snapshot()
        client.get_map_view.return_value = _valid_map_asset_response(
            verification_status="pending"
        )
        args = _FakePayload(
            profile="task_ready",
            robot_id="CR-001",
            task_id=7,
            service_timeout=1.0,
            ignore_warning=[],
            actions=["prepare_for_task"],
            run_task_cycle=False,
        )
        run_actions_mock = mock.Mock(return_value=[])

        with mock.patch(
            "run_backend_runtime_smoke.BackendRuntimeSmokeClient",
            return_value=client,
        ), mock.patch(
            "run_backend_runtime_smoke.run_read_checks",
            return_value=_valid_task_ready_checks(),
        ), mock.patch(
            "run_backend_runtime_smoke.run_actions",
            run_actions_mock,
        ):
            report = build_report(args)

        run_actions_mock.assert_not_called()
        self.assertFalse(report["summary"]["ok"])
        asset_check = next(
            item
            for item in report["checks"]
            if item["name"] == "active_map_persistent_asset"
        )
        self.assertFalse(asset_check["ok"])

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

    def test_job_succeeded_requires_a_consistent_success_contract(self):
        cases = (
            (
                "consistent success",
                _FakeJob(
                    done=True,
                    job_state="succeeded",
                    status="succeeded",
                    success=True,
                    result_success=True,
                ),
                True,
            ),
            (
                "failed overrides success flags",
                _FakeJob(
                    done=True,
                    job_state="failed",
                    status="failed",
                    success=True,
                    result_success=True,
                ),
                False,
            ),
            (
                "canceled overrides success flags",
                _FakeJob(
                    done=True,
                    job_state="canceled",
                    status="canceled",
                    success=True,
                    result_success=True,
                ),
                False,
            ),
            (
                "manual assist overrides success flags",
                _FakeJob(
                    done=True,
                    job_state="manual_assist_required",
                    status="manual_assist_required",
                    success=True,
                    result_success=True,
                ),
                False,
            ),
            (
                "succeeded requires success",
                _FakeJob(
                    done=True,
                    job_state="succeeded",
                    status="succeeded",
                    success=False,
                    result_success=True,
                ),
                False,
            ),
            (
                "succeeded requires done",
                _FakeJob(
                    done=False,
                    job_state="succeeded",
                    status="succeeded",
                    success=True,
                    result_success=True,
                ),
                False,
            ),
            (
                "succeeded requires result_success",
                _FakeJob(
                    done=True,
                    job_state="succeeded",
                    status="succeeded",
                    success=True,
                    result_success=False,
                ),
                False,
            ),
            (
                "terminal states must agree",
                _FakeJob(
                    done=True,
                    job_state="succeeded",
                    status="failed",
                    success=True,
                    result_success=True,
                ),
                False,
            ),
            (
                "succeeded rejects a nonterminal status",
                _FakeJob(
                    done=True,
                    job_state="succeeded",
                    status="running",
                    success=True,
                    result_success=True,
                ),
                False,
            ),
            (
                "succeeded rejects a missing job_state",
                _FakeJob(
                    done=True,
                    job_state="",
                    status="succeeded",
                    success=True,
                    result_success=True,
                ),
                False,
            ),
            (
                "manual assist flag contradicts succeeded",
                _FakeJob(
                    done=True,
                    job_state="succeeded",
                    status="succeeded",
                    success=True,
                    result_success=True,
                    manual_assist_required=True,
                ),
                False,
            ),
        )
        for label, job, expected in cases:
            with self.subTest(label=label):
                self.assertEqual(job_succeeded(job), expected)
                if expected:
                    self.assertEqual(job_terminal_consistency_issues(job), [])
                else:
                    self.assertFalse(job_succeeded(job))

    def test_terminal_job_contract_requires_exact_codes_messages_and_phases(self):
        for state in ("succeeded", "failed", "canceled", "manual_assist_required"):
            with self.subTest(state=state):
                job = _FakeJob(
                    done=True,
                    job_state=state,
                    status=state,
                    success=state == "succeeded",
                    result_success=state == "succeeded",
                )
                self.assertEqual(job_terminal_consistency_issues(job), [])

        invalid_cases = (
            (
                "success error code",
                _FakeJob(
                    done=True,
                    job_state="succeeded",
                    status="succeeded",
                    success=True,
                    result_success=True,
                    error_code="contradiction",
                ),
                "empty error_code",
            ),
            (
                "success result code",
                _FakeJob(
                    done=True,
                    job_state="succeeded",
                    status="succeeded",
                    success=True,
                    result_success=True,
                    result_code="unexpected",
                ),
                "result_code=ok",
            ),
            (
                "success phase",
                _FakeJob(
                    done=True,
                    job_state="succeeded",
                    status="succeeded",
                    success=True,
                    result_success=True,
                    workflow_phase="failed",
                ),
                "phase/workflow_phase=done",
            ),
            (
                "failed empty code",
                _FakeJob(
                    done=True,
                    job_state="failed",
                    status="failed",
                    error_code="",
                    result_code="",
                ),
                "nonempty error_code",
            ),
            (
                "failed contradictory result code",
                _FakeJob(
                    done=True,
                    job_state="failed",
                    status="failed",
                    error_code="runtime_failed",
                    result_code="ok",
                ),
                "result_code=error_code!=ok",
            ),
            (
                "canceled phase",
                _FakeJob(
                    done=True,
                    job_state="canceled",
                    status="canceled",
                    phase="failed",
                ),
                "phase/workflow_phase=canceled",
            ),
            (
                "manual assist flag",
                _FakeJob(
                    done=True,
                    job_state="manual_assist_required",
                    status="manual_assist_required",
                    manual_assist_required=False,
                ),
                "manual_assist_required=False",
            ),
            (
                "result message",
                _FakeJob(
                    done=True,
                    job_state="failed",
                    status="failed",
                    message="runtime failed",
                    result_message="different",
                ),
                "result_message/message mismatch",
            ),
        )
        for label, job, expected_issue in invalid_cases:
            with self.subTest(label=label):
                issues = job_terminal_consistency_issues(job)
                self.assertTrue(
                    any(expected_issue in issue for issue in issues),
                    msg=issues,
                )

    def test_accepted_submit_contract_rejects_contradictory_codes_and_job_payload(self):
        queued_job = {
            "job_id": "job_1",
            "robot_id": "CR-001",
            "operation": 9,
            "operation_name": "verify_map_revision",
            "requested_map_name": "site_a",
            "requested_map_revision_id": "rev_site_a_01",
            "description": "smoke:verify_map_revision",
            "status": "queued",
            "job_state": "queued",
            "phase": "accepted",
            "workflow_phase": "accepted",
            "done": False,
            "success": False,
            "result_success": False,
            "error_code": "",
            "result_code": "",
            "message": "accepted",
            "result_message": "accepted",
            "manual_assist_required": False,
        }
        valid_submit = {
            "accepted": True,
            "message": "accepted",
            "error_code": "",
            "job_id": "job_1",
            "job": queued_job,
        }
        expected = {
            "expected_job_id": "job_1",
            "expected_robot_id": "CR-001",
            "expected_operation_name": "verify_map_revision",
            "expected_map_name": "site_a",
            "expected_map_revision_id": "rev_site_a_01",
            "expected_description": "smoke:verify_map_revision",
        }
        self.assertEqual(
            accepted_submit_consistency_issues(valid_submit, **expected),
            [],
        )

        mutations = (
            (
                "accepted response code",
                lambda payload: payload.__setitem__("error_code", "contradiction"),
                "accepted submit requires empty error_code",
            ),
            (
                "queued state",
                lambda payload: payload["job"].__setitem__("job_state", "succeeded"),
                "status/job_state=queued",
            ),
            (
                "accepted phase",
                lambda payload: payload["job"].__setitem__("workflow_phase", "done"),
                "phase/workflow_phase=accepted",
            ),
            (
                "queued result code",
                lambda payload: payload["job"].__setitem__("result_code", "ok"),
                "empty error_code/result_code",
            ),
            (
                "message projection",
                lambda payload: payload["job"].__setitem__("result_message", "different"),
                "message contract mismatch",
            ),
            (
                "job identity",
                lambda payload: payload["job"].__setitem__("job_id", "foreign_job"),
                "job identity mismatch",
            ),
        )
        for label, mutate, expected_issue in mutations:
            with self.subTest(label=label):
                payload = copy.deepcopy(valid_submit)
                mutate(payload)
                issues = accepted_submit_consistency_issues(payload, **expected)
                self.assertTrue(
                    any(expected_issue in issue for issue in issues),
                    msg=issues,
                )

    def test_wait_for_job_rejects_found_response_with_error_code(self):
        client = mock.Mock()
        client.get_slam_job.return_value = _FakePayload(
            found=True,
            message="ok",
            error_code="contradictory_error",
            job=_FakeJob(
                job_id="job_1",
                robot_id="CR-001",
                operation=9,
                operation_name="verify_map_revision",
                requested_map_name="site_a",
                resolved_map_name="site_a",
                requested_map_revision_id="rev_site_a_01",
                resolved_map_revision_id="rev_site_a_01",
                description="smoke:verify_map_revision",
                done=True,
                job_state="succeeded",
                status="succeeded",
                success=True,
                result_success=True,
            ),
        )

        result = wait_for_job(
            client,
            job_id="job_1",
            robot_id="CR-001",
            timeout_s=1.0,
            poll_interval_s=0.01,
            expected_operation_name="verify_map_revision",
            expected_map_name="site_a",
            expected_map_revision_id="rev_site_a_01",
            expected_description="smoke:verify_map_revision",
        )

        self.assertFalse(result["ok"])
        self.assertEqual(result["terminal_state"], "contract_mismatch")
        self.assertIn("found job response requires empty error_code", result["issues"][0])

    def test_job_to_dict_includes_revision_scope(self):
        payload = _job_to_dict(
            _FakeJob(
                operation=9,
                operation_name="verify_map_revision",
                requested_map_name="requested_map",
                requested_map_revision_id="rev_requested_01",
                resolved_map_name="resolved_map",
                resolved_map_revision_id="rev_resolved_02",
                description="smoke:verify_map_revision",
            )
        )

        self.assertEqual(payload["operation"], 9)
        self.assertEqual(payload["operation_name"], "verify_map_revision")
        self.assertEqual(payload["requested_map_name"], "requested_map")
        self.assertEqual(payload["resolved_map_name"], "resolved_map")
        self.assertEqual(payload["description"], "smoke:verify_map_revision")
        self.assertEqual(payload["requested_map_revision_id"], "rev_requested_01")
        self.assertEqual(payload["resolved_map_revision_id"], "rev_resolved_02")
        self.assertEqual(payload["revision_scope"]["requested"]["map_name"], "requested_map")
        self.assertEqual(payload["revision_scope"]["resolved"]["map_name"], "resolved_map")
        self.assertEqual(payload["revision_scope"]["requested"]["revision_id"], "rev_requested_01")
        self.assertEqual(payload["revision_scope"]["resolved"]["revision_id"], "rev_resolved_02")

    def test_job_contract_rejects_wrong_operation_map_or_revision(self):
        base = {
            "job_id": "job_1",
            "robot_id": "CR-001",
            "operation": 9,
            "operation_name": "verify_map_revision",
            "requested_map_name": "demo_map",
            "resolved_map_name": "demo_map",
            "requested_map_revision_id": "rev_demo_01",
            "resolved_map_revision_id": "rev_demo_01",
            "description": "smoke:verify_map_revision",
        }
        self.assertEqual(
            job_contract_issues(
                base,
                expected_job_id="job_1",
                expected_robot_id="CR-001",
                expected_operation_name="verify_map_revision",
                expected_map_name="demo_map",
                expected_map_revision_id="rev_demo_01",
                expected_description="smoke:verify_map_revision",
                check_resolved_scope=True,
            ),
            [],
        )
        mutations = (
            ("operation", {"operation": 10}),
            ("operation_name", {"operation_name": "activate_map_revision"}),
            ("requested map", {"requested_map_name": "other_map"}),
            ("resolved map", {"resolved_map_name": "other_map"}),
            ("requested revision", {"requested_map_revision_id": "rev_other"}),
            ("resolved revision", {"resolved_map_revision_id": "rev_other"}),
        )
        for label, mutation in mutations:
            payload = dict(base)
            payload.update(mutation)
            with self.subTest(label=label):
                self.assertTrue(
                    job_contract_issues(
                        payload,
                        expected_job_id="job_1",
                        expected_robot_id="CR-001",
                        expected_operation_name="verify_map_revision",
                        expected_map_name="demo_map",
                        expected_map_revision_id="rev_demo_01",
                        expected_description="smoke:verify_map_revision",
                        check_resolved_scope=True,
                    )
                )

    def test_save_job_contract_allows_a_new_revision_but_not_a_new_map(self):
        payload = {
            "job_id": "job_save",
            "robot_id": "CR-001",
            "operation": 4,
            "operation_name": "save_mapping",
            "requested_map_name": "demo_map",
            "resolved_map_name": "demo_map",
            "requested_map_revision_id": "rev_previous_01",
            "resolved_map_revision_id": "rev_candidate_02",
            "description": "smoke:save_mapping",
        }
        kwargs = {
            "expected_job_id": "job_save",
            "expected_robot_id": "CR-001",
            "expected_operation_name": "save_mapping",
            "expected_map_name": "demo_map",
            "expected_map_revision_id": "rev_previous_01",
            "expected_description": "smoke:save_mapping",
            "check_resolved_scope": True,
            "allow_resolved_revision_change": True,
            "require_resolved_revision_id": True,
        }
        self.assertEqual(job_contract_issues(payload, **kwargs), [])
        for label, mutation, issue_text in (
            ("wrong map", {"resolved_map_name": "other_map"}, "resolved map_name mismatch"),
            ("empty revision", {"resolved_map_revision_id": ""}, "revision_id is empty"),
        ):
            changed = dict(payload)
            changed.update(mutation)
            with self.subTest(label=label):
                issues = job_contract_issues(changed, **kwargs)
                self.assertTrue(any(issue_text in issue for issue in issues))

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

    def test_validate_args_rejects_all_write_actions(self):
        parser = build_arg_parser()
        args = parser.parse_args([
            "--robot-id", "CR-001",
            "--actions", "verify_map_revision",
            "--map-name", "demo_map",
            "--map-revision-id", "rev_demo_01",
        ])

        with self.assertRaisesRegex(ValueError, "--actions is forbidden"):
            validate_args(args)

    def test_validate_args_rejects_missing_target_for_revision_scoped_actions(self):
        parser = build_arg_parser()
        args = parser.parse_args(["--robot-id", "CR-001", "--actions", "activate_map_revision"])

        with self.assertRaises(ValueError):
            validate_args(args)

    def test_validate_args_rejects_mapping_actions_in_generic_smoke(self):
        parser = build_arg_parser()
        args = parser.parse_args([
            "--robot-id", "CR-001",
            "--actions", "start_mapping,save_mapping,stop_mapping",
            "--save-map-name", "demo_map",
        ])

        with self.assertRaisesRegex(ValueError, "--actions is forbidden"):
            validate_args(args)

    def test_validate_args_rejects_legacy_action_only_options_without_actions(self):
        parser = build_arg_parser()
        for argv, option_name in (
            (["--map-name", "demo_map"], "map_name"),
            (["--set-active"], "set_active"),
            (["--initial-pose-x", "1.0"], "initial_pose_x"),
            (["--frame-id", "odom"], "frame_id"),
            (["--description-prefix", "legacy"], "description_prefix"),
        ):
            with self.subTest(option=option_name):
                args = parser.parse_args(["--robot-id", "CR-001"] + argv)
                with self.assertRaisesRegex(
                    ValueError,
                    "write-action-only options are forbidden.*%s" % option_name,
                ):
                    validate_args(args)

    def test_validate_args_rejects_multi_action_workflows(self):
        parser = build_arg_parser()
        args = parser.parse_args([
            "--robot-id", "CR-001",
            "--actions", "verify_map_revision,activate_map_revision",
            "--map-name", "demo_map",
            "--map-revision-id", "rev_demo_01",
        ])

        with self.assertRaisesRegex(ValueError, "--actions is forbidden"):
            validate_args(args)

    def test_validate_args_rejects_action_and_task_cycle_combination(self):
        parser = build_arg_parser()
        args = parser.parse_args([
            "--robot-id", "CR-001",
            "--actions", "prepare_for_task",
            "--map-name", "demo_map",
            "--map-revision-id", "rev_demo_01",
            "--run-task-cycle",
            "--task-id", "9",
            "--ops-db-path", "/tmp/operations.db",
        ])

        with self.assertRaisesRegex(ValueError, "--actions is forbidden"):
            validate_args(args)

    def test_validate_args_prohibits_task_cycle_in_commercial_smoke(self):
        parser = build_arg_parser()
        args = parser.parse_args([
            "--robot-id", "CR-001",
            "--run-task-cycle",
            "--task-id", "9",
            "--ops-db-path", "/data/coverage/operations.db",
        ])

        with self.assertRaisesRegex(ValueError, "prohibited by the commercial"):
            validate_args(args)

    def test_build_report_prohibits_programmatic_task_cycle_before_ros_client(self):
        args = _FakePayload(
            profile="task_ready",
            robot_id="CR-001",
            run_task_cycle=True,
        )

        with mock.patch("run_backend_runtime_smoke.BackendRuntimeSmokeClient") as client_cls:
            with self.assertRaisesRegex(ValueError, "prohibited by the commercial"):
                build_report(args)

        client_cls.assert_not_called()

    def test_validate_args_rejects_default_robot_identity(self):
        args = build_arg_parser().parse_args([])

        with self.assertRaisesRegex(ValueError, "explicit commercial --robot-id"):
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
                "run_id": "run_1",
                "job_id": "9",
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

    def test_runtime_idle_requires_active_job_id_to_be_empty(self):
        runtime = {
            "active_run_id": "",
            "active_job_id": "9",
            "mission_state": "IDLE",
            "phase": "IDLE",
            "public_state": "IDLE",
            "executor_state": "IDLE",
        }

        self.assertFalse(_runtime_row_is_idle(runtime))

    def test_task_cycle_run_binding_rejects_stale_id_time_job_and_scope(self):
        issues = _task_cycle_run_binding_issues(
            task_id=9,
            run_id="run_stale",
            run_row={
                "run_id": "run_stale",
                "job_id": "8",
                "map_name": "other_site",
                "map_revision_id": "rev_other_01",
                "plan_profile_name": "other_profile",
                "plan_id": "",
                "created_ts": 99.0,
                "start_ts": 99.0,
                "end_ts": 99.5,
                "updated_ts": 99.5,
            },
            start_requested_ts=100.0,
            baseline_run_ids={"run_stale"},
            expected_scope={
                "map_name": "site_a",
                "map_revision_id": "rev_site_a_01",
                "plan_profile_name": "commercial_default",
            },
            require_resolved_plan=True,
        )

        self.assertTrue(any("is not new" in item for item in issues))
        self.assertTrue(any("job_id mismatch" in item for item in issues))
        self.assertTrue(any("predates START request" in item for item in issues))
        self.assertTrue(any("map_name mismatch" in item for item in issues))
        self.assertTrue(any("map_revision_id mismatch" in item for item in issues))
        self.assertTrue(any("plan_profile_name mismatch" in item for item in issues))
        self.assertTrue(any("plan_id is empty" in item for item in issues))

    def test_task_cycle_db_state_filters_runtime_by_exact_robot_id(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "operations.db")
            conn = sqlite3.connect(db_path)
            try:
                conn.executescript(
                    """
                    CREATE TABLE mission_runs(
                      run_id TEXT PRIMARY KEY, job_id TEXT, state TEXT, reason TEXT,
                      map_name TEXT, map_revision_id TEXT, plan_profile_name TEXT,
                      plan_id TEXT, map_id TEXT, map_md5 TEXT, created_ts REAL,
                      start_ts REAL, end_ts REAL, updated_ts REAL
                    );
                    CREATE TABLE robot_runtime_state(
                      robot_id TEXT PRIMARY KEY, active_run_id TEXT, active_job_id TEXT,
                      mission_state TEXT, phase TEXT, public_state TEXT,
                      executor_state TEXT, updated_ts REAL
                    );
                    INSERT INTO mission_runs VALUES(
                      'run_1', '9', 'DONE', '', 'site_a', 'rev_site_a_01',
                      'commercial_default', 'plan_1', 'map_1', 'md5_1',
                      100.0, 100.0, 101.0, 101.0
                    );
                    INSERT INTO robot_runtime_state VALUES(
                      'OTHER', '', '', 'IDLE', 'IDLE', 'IDLE', 'IDLE', 101.0
                    );
                    """
                )
                conn.commit()
            finally:
                conn.close()

            state = _load_task_cycle_db_state(db_path, "run_1", "CR-001")

        self.assertEqual(state["run"]["run_id"], "run_1")
        self.assertEqual(state["runtime"], {})

    def test_run_task_cycle_does_not_accept_pre_start_latched_run_id(self):
        readiness = _FakePayload(
            success=True,
            message="",
            readiness=_FakePayload(
                overall_ready=True,
                can_start_task=True,
                task_id=9,
                task_map_name="site_a",
                task_map_revision_id="rev_site_a_01",
                task_plan_profile="commercial_default",
                blocking_reasons=[],
                warnings=[],
                checks=[],
            ),
        )
        stale_state = _FakePayload(
            mission_state="RUNNING",
            phase="CLEANING",
            public_state="RUNNING",
            executor_state="RUNNING",
            active_job_id="8",
            run_id="run_stale",
        )

        class _FakeClient(object):
            def __init__(self):
                self.start_calls = 0

            def get_system_readiness(self, task_id):
                self.task_id = task_id
                return readiness

            def wait_for_task_state(self, timeout_s):
                self.wait_timeout = timeout_s
                return stale_state

            def start_task(self, task_id):
                self.start_calls += 1
                return _FakePayload(success=True, message="accepted")

        client = _FakeClient()
        args = _FakePayload(
            task_id=9,
            ignore_warning=[],
            poll_interval=0.1,
            task_timeout=1.0,
            ops_db_path="/data/coverage/operations.db",
            robot_id="CR-001",
        )
        idle_runtime = {
            "robot_id": "CR-001",
            "active_run_id": "",
            "active_job_id": "",
            "mission_state": "IDLE",
            "phase": "IDLE",
            "public_state": "IDLE",
            "executor_state": "IDLE",
            "updated_ts": 99.0,
        }
        with mock.patch(
            "run_backend_runtime_smoke._load_task_cycle_db_baseline",
            return_value={"run_ids": frozenset({"run_stale"}), "runtime": idle_runtime},
        ), mock.patch(
            "run_backend_runtime_smoke.time.time",
            side_effect=[100.0, 100.0, 100.1, 100.1, 101.1],
        ):
            result = run_task_cycle(client, args)

        self.assertEqual(client.start_calls, 1)
        self.assertEqual(result["run_id"], "")
        self.assertFalse(result["running_seen"])
        self.assertFalse(result["ok"])
        self.assertEqual(result["candidate_rejections"][0]["run_id"], "run_stale")
        self.assertTrue(
            any("existed before START" in item for item in result["issues"]),
            result["issues"],
        )

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

    def test_run_actions_rejects_save_mapping_without_cli_validation(self):
        class _FakeClient(object):
            def submit_action(self, **kwargs):
                self.submit_kwargs = dict(kwargs)
                return _FakePayload(
                    accepted=True,
                    message="accepted",
                    error_code="",
                    job_id="job_1",
                    map_name=str(kwargs.get("save_map_name") or ""),
                    operation=4,
                    job=_FakeJob(
                        job_id="job_1",
                        robot_id="CR-001",
                        operation=4,
                        operation_name="save_mapping",
                        requested_map_name="demo_map",
                        requested_map_revision_id="rev_previous_01",
                        description="smoke:save_mapping",
                    ),
                )

            def get_slam_job(self, job_id, robot_id):
                return _FakePayload(
                    found=True,
                    message="done",
                    error_code="",
                    job=_FakeJob(
                        job_id="job_1",
                        robot_id="CR-001",
                        operation=4,
                        operation_name="save_mapping",
                        requested_map_name="demo_map",
                        resolved_map_name="demo_map",
                        done=True,
                        status="succeeded",
                        job_state="succeeded",
                        success=True,
                        result_success=True,
                        requested_map_revision_id="rev_previous_01",
                        resolved_map_revision_id="rev_saved_demo_01",
                        description="smoke:save_mapping",
                    ),
                )

        with self.assertRaisesRegex(ValueError, "write actions are forbidden"):
            run_actions(_FakeClient(), _action_args("save_mapping"))

    def test_run_actions_rejects_stop_mapping_without_cli_validation(self):
        class _FakeClient(object):
            def submit_action(self, **_kwargs):
                return _FakePayload(
                    accepted=True,
                    message="accepted",
                    error_code="",
                    job_id="job_stop",
                    map_name="",
                    operation=5,
                    job=_FakeJob(
                        job_id="job_stop",
                        robot_id="CR-001",
                        operation=5,
                        operation_name="stop_mapping",
                        requested_map_name="",
                        requested_map_revision_id="",
                        description="smoke:stop_mapping",
                    ),
                )

            def get_slam_job(self, job_id, robot_id):
                del job_id, robot_id
                return _FakePayload(
                    found=True,
                    message="done",
                    error_code="",
                    job=_FakeJob(
                        job_id="job_stop",
                        robot_id="CR-001",
                        operation=5,
                        operation_name="stop_mapping",
                        requested_map_name="",
                        requested_map_revision_id="",
                        resolved_map_name="",
                        resolved_map_revision_id="",
                        description="smoke:stop_mapping",
                        done=True,
                        status="succeeded",
                        job_state="succeeded",
                        success=True,
                        result_success=True,
                    ),
                )

        with self.assertRaisesRegex(ValueError, "write actions are forbidden"):
            run_actions(
                _FakeClient(),
                _action_args(
                    "stop_mapping",
                    map_name="demo_map",
                    map_revision_id="rev_saved_demo_01",
                ),
            )

    def test_run_actions_rejects_revision_write_without_submitting(self):
        class _FakeClient(object):
            def __init__(self):
                self.poll_count = 0

            def submit_action(self, **_kwargs):
                return _FakePayload(
                    accepted=True,
                    message="accepted",
                    error_code="",
                    job_id="job_1",
                    map_name="other_map",
                    operation=9,
                    job=_FakeJob(
                        job_id="job_1",
                        robot_id="CR-001",
                        operation=9,
                        operation_name="verify_map_revision",
                        requested_map_name="demo_map",
                        requested_map_revision_id="rev_demo_01",
                        description="smoke:activate_map_revision",
                    ),
                )

            def get_slam_job(self, job_id, robot_id):
                del job_id, robot_id
                self.poll_count += 1
                raise AssertionError("contract mismatch must not be polled")

        client = _FakeClient()
        with self.assertRaisesRegex(ValueError, "write actions are forbidden"):
            run_actions(
                client,
                _action_args("activate_map_revision", map_revision_id="rev_demo_01"),
            )

        self.assertEqual(client.poll_count, 0)

    def test_run_actions_rejects_multi_action_sequence_without_submitting(self):
        class _FakeClient(object):
            def __init__(self):
                self.submitted = []

            def submit_action(self, operation_name, **_kwargs):
                self.submitted.append(operation_name)
                return _FakePayload(
                    accepted=False,
                    message="blocked",
                    error_code="precondition_failed",
                    job_id="",
                    map_name="",
                    operation=0,
                )

        client = _FakeClient()
        args = _action_args(
            "verify_map_revision",
            actions=["verify_map_revision", "activate_map_revision"],
            map_revision_id="rev_demo_01",
        )

        with self.assertRaisesRegex(ValueError, "write actions are forbidden"):
            run_actions(client, args)

        self.assertEqual(client.submitted, [])


if __name__ == "__main__":
    unittest.main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import ast
import copy
import json
import os
import stat
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
PKG_DIR = os.path.dirname(THIS_DIR)
TOOLS_DIR = os.path.join(PKG_DIR, "tools")
MODULE_PATH = os.path.join(TOOLS_DIR, "run_revision_workflow_acceptance.py")

if TOOLS_DIR not in sys.path:
    sys.path.insert(0, TOOLS_DIR)

import run_revision_workflow_acceptance as workflow
from run_revision_workflow_acceptance import build_arg_parser, capture_snapshot, validate_args


ROBOT_ID = "CR-001"
STABLE_MAP_NAME = "stable_map"
STABLE_REVISION_ID = "rev_stable_01"
CANDIDATE_MAP_NAME = "new_map"
CANDIDATE_REVISION_ID = "rev_candidate_02"
START_MAPPING_JOB_ID = "job-start_mapping"


class _FakePayload:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


class _IdentityRecordingClient:
    def __init__(self, response_robot_id=None):
        self.slam_robot_ids = []
        self.odometry_robot_ids = []
        self.response_robot_id = response_robot_id

    def get_slam_status(self, *, robot_id):
        self.slam_robot_ids.append(robot_id)
        response_robot_id = self.response_robot_id or robot_id
        return _FakePayload(
            success=True,
            message="",
            state=_FakePayload(
                robot_id=response_robot_id,
                active_map_name="",
                active_map_revision_id="",
                runtime_map_name="",
                runtime_map_revision_id="",
                pending_map_name="",
                pending_map_revision_id="",
                pending_map_switch_status="",
                localization_valid=False,
                runtime_map_ready=False,
                runtime_map_match=False,
                manual_assist_required=False,
                blocking_reasons=[],
                warnings=[],
            ),
        )

    def get_odometry_status(self, *, robot_id):
        self.odometry_robot_ids.append(robot_id)
        response_robot_id = self.response_robot_id or robot_id
        return _FakePayload(
            success=True,
            message="",
            state=_FakePayload(
                robot_id=response_robot_id,
                odom_valid=True,
                odom_stream_ready=True,
                frame_id_valid=True,
                child_frame_id_valid=True,
                warnings=[],
            ),
        )

    def get_system_readiness(self, task_id, refresh_map_identity=True):
        del refresh_map_identity
        return _FakePayload(
            success=True,
            message="",
            readiness=_FakePayload(
                overall_ready=False,
                can_start_task=False,
                task_id=task_id,
                active_map_name="",
                active_map_revision_id="",
                runtime_map_name="",
                runtime_map_revision_id="",
                blocking_reasons=[],
                warnings=[],
            ),
        )


def _valid_topology_snapshot():
    return {
        "service_providers": dict(workflow.STAGE_K_EXPECTED_SERVICE_PROVIDERS),
        "node_robot_ids": {
            node_name: ROBOT_ID
            for _service_name, node_name in workflow.STAGE_K_EXPECTED_SERVICE_PROVIDERS
        },
        "node_private_params": {
            node_name: dict(expected_params)
            for node_name, expected_params in workflow.STAGE_K_EXPECTED_NODE_PRIVATE_PARAMS.items()
        },
        "mapping_session_id": "",
    }


def _workflow_snapshot(
    *,
    current_mode="localization",
    desired_mode=None,
    runtime_mode=None,
    mapping_session_active=False,
    active_map_name=STABLE_MAP_NAME,
    active_revision_id=STABLE_REVISION_ID,
    runtime_map_name=STABLE_MAP_NAME,
    runtime_revision_id=STABLE_REVISION_ID,
    localization_valid=True,
    runtime_map_match=True,
    task_ready=False,
    readiness_task_id=0,
    readiness_task_map_name="",
    readiness_task_revision_id="",
    can_start_task=False,
    overall_ready=False,
):
    active_map_id = "stable-map-id" if active_map_name == STABLE_MAP_NAME else "candidate-map-id"
    active_map_md5 = "stable-map-md5" if active_map_name == STABLE_MAP_NAME else "candidate-map-md5"
    slam_state = {
        "robot_id": ROBOT_ID,
        "desired_mode": current_mode if desired_mode is None else desired_mode,
        "current_mode": current_mode,
        "runtime_mode": current_mode if runtime_mode is None else runtime_mode,
        "mapping_session_active": bool(mapping_session_active),
        "active_map_name": active_map_name,
        "active_map_revision_id": active_revision_id,
        "active_map_id": active_map_id if active_map_name else "",
        "active_map_md5": active_map_md5 if active_map_name else "",
        "runtime_map_name": runtime_map_name,
        "runtime_map_revision_id": runtime_revision_id,
        "runtime_map_id": active_map_id if runtime_map_name else "",
        "runtime_map_md5": active_map_md5 if runtime_map_name else "",
        "pending_map_name": "",
        "pending_map_revision_id": "",
        "pending_map_switch_status": "",
        "active_job_id": "",
        "active_job_status": "",
        "active_job_phase": "",
        "localization_valid": bool(localization_valid),
        "runtime_map_match": bool(runtime_map_match),
        "task_ready": bool(task_ready),
        "blocking_reasons": [],
        "warnings": [],
    }
    odometry_state = {
        "robot_id": ROBOT_ID,
        "odom_valid": True,
        "blocking_reasons": [],
        "warnings": [],
    }
    readiness_state = {
        "overall_ready": bool(overall_ready),
        "task_id": int(readiness_task_id),
        "task_map_name": readiness_task_map_name,
        "task_map_revision_id": readiness_task_revision_id,
        "active_map_name": active_map_name,
        "active_map_revision_id": active_revision_id,
        "runtime_map_name": runtime_map_name,
        "runtime_map_revision_id": runtime_revision_id,
        "can_start_task": bool(can_start_task),
        "blocking_reasons": [],
        "warnings": [],
    }
    return {
        "slam": slam_state,
        "odometry": odometry_state,
        "readiness": readiness_state,
        "revision_scope": {},
        "checks": {
            "slam": {
                "name": "slam_status",
                "ok": True,
                "issues": [],
                "response": {"success": True, "state": slam_state},
            },
            "odometry": {
                "name": "odometry_status",
                "ok": True,
                "issues": [],
                "response": {"success": True, "state": odometry_state},
            },
            "readiness": {
                "name": "system_readiness",
                "ok": True,
                "issues": [],
                "response": {"success": True, "readiness": readiness_state},
            },
        },
    }


def _mapping_workflow_snapshot():
    return _workflow_snapshot(
        current_mode="mapping",
        mapping_session_active=True,
        runtime_map_name="",
        runtime_revision_id="",
        localization_valid=False,
        runtime_map_match=False,
    )


def _post_stop_snapshot():
    return _workflow_snapshot(
        current_mode="localization",
        mapping_session_active=False,
        runtime_map_name="",
        runtime_revision_id="",
        localization_valid=False,
        runtime_map_match=False,
    )


def _new_vehicle_pre_snapshot():
    return _workflow_snapshot(
        active_map_name="",
        active_revision_id="",
        runtime_map_name="",
        runtime_revision_id="",
        localization_valid=False,
        runtime_map_match=False,
    )


def _new_vehicle_mapping_snapshot():
    return _workflow_snapshot(
        current_mode="mapping",
        mapping_session_active=True,
        active_map_name="",
        active_revision_id="",
        runtime_map_name="",
        runtime_revision_id="",
        localization_valid=False,
        runtime_map_match=False,
    )


def _new_vehicle_post_stop_snapshot():
    return _workflow_snapshot(
        current_mode="localization",
        mapping_session_active=False,
        active_map_name="",
        active_revision_id="",
        runtime_map_name="",
        runtime_revision_id="",
        localization_valid=False,
        runtime_map_match=False,
    )


def _verified_candidate_without_active_map_snapshot():
    return _workflow_snapshot(
        active_map_name="",
        active_revision_id="",
        runtime_map_name=CANDIDATE_MAP_NAME,
        runtime_revision_id=CANDIDATE_REVISION_ID,
        localization_valid=True,
        runtime_map_match=False,
    )


def _candidate_runtime_snapshot(*, task_id=0, prepared=False):
    return _workflow_snapshot(
        active_map_name=CANDIDATE_MAP_NAME,
        active_revision_id=CANDIDATE_REVISION_ID,
        runtime_map_name=CANDIDATE_MAP_NAME,
        runtime_revision_id=CANDIDATE_REVISION_ID,
        localization_valid=True,
        runtime_map_match=True,
        task_ready=prepared,
        readiness_task_id=task_id,
        readiness_task_map_name=CANDIDATE_MAP_NAME if prepared else "",
        readiness_task_revision_id=CANDIDATE_REVISION_ID if prepared else "",
        can_start_task=prepared,
        overall_ready=prepared,
    )


def _workflow_args(**overrides):
    values = {
        "profile": "mapping_save_candidate",
        "robot_id": ROBOT_ID,
        "task_id": 42,
        "ignore_warning": [],
        "save_map_name": CANDIDATE_MAP_NAME,
        "map_name": CANDIDATE_MAP_NAME,
        "map_revision_id": CANDIDATE_REVISION_ID,
        "frame_id": "map",
        "description_prefix": "workflow-test",
        "service_timeout": 1.0,
        "job_timeout": 1.0,
        "poll_interval": 0.01,
        "allow_write_actions": True,
        "resume_from_checkpoint": "",
        "pause_after_start_mapping": False,
        "checkpoint_path": "",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _successful_action(action_name):
    revision_id = CANDIDATE_REVISION_ID if action_name == "save_mapping" else ""
    return {
        "name": action_name,
        "ok": True,
        "issues": [],
        "submit": {"accepted": True, "job_id": "job-%s" % action_name},
        "job": {
            "ok": True,
            "terminal_state": "succeeded",
            "response": {
                "job": {
                    "robot_id": ROBOT_ID,
                    "resolved_map_revision_id": revision_id,
                    "requested_map_revision_id": revision_id,
                }
            },
        },
    }


def _start_mapping_job_payload(
    *,
    job_id=START_MAPPING_JOB_ID,
    robot_id=ROBOT_ID,
    operation_name="start_mapping",
    operation=3,
    description="workflow-test:start_mapping",
    job_state="succeeded",
    status="succeeded",
    done=True,
    success=True,
    result_success=True,
    phase="done",
    workflow_phase="done",
    error_code="",
    message="completed",
    result_code="ok",
    result_message="completed",
):
    return {
        "job_id": job_id,
        "robot_id": robot_id,
        "operation": operation,
        "operation_name": operation_name,
        "requested_map_name": "",
        "resolved_map_name": "",
        "requested_map_revision_id": "",
        "resolved_map_revision_id": "",
        "description": description,
        "job_state": job_state,
        "status": status,
        "phase": phase,
        "workflow_phase": workflow_phase,
        "done": done,
        "success": success,
        "result_success": result_success,
        "error_code": error_code,
        "message": message,
        "result_code": result_code,
        "result_message": result_message,
        "manual_assist_required": False,
    }


def _successful_start_mapping_action(**job_overrides):
    job_payload = _start_mapping_job_payload(**job_overrides)
    submit_job_payload = copy.deepcopy(job_payload)
    submit_job_payload.update(
        job_state="queued",
        status="queued",
        phase="accepted",
        workflow_phase="accepted",
        done=False,
        success=False,
        result_success=False,
        error_code="",
        message="accepted",
        result_code="",
        result_message="accepted",
    )
    return {
        "name": "start_mapping",
        "ok": True,
        "issues": [],
        "submit": {
            "accepted": True,
            "message": "accepted",
            "error_code": "",
            "job_id": str(job_payload["job_id"]),
            "map_name": "",
            "operation": 3,
            "job": submit_job_payload,
        },
        "job": {
            "ok": True,
            "terminal_state": "succeeded",
            "issues": [],
            "response": {
                "found": True,
                "message": "",
                "error_code": "",
                "job": copy.deepcopy(job_payload),
            },
        },
    }


def _checkpoint_payload(
    args,
    *,
    start_action=None,
    mapping_session_id=START_MAPPING_JOB_ID,
    profile_name="mapping_save_candidate",
):
    return workflow._mapping_checkpoint_payload(
        profile_name=profile_name,
        args=args,
        pre_snapshot=_new_vehicle_pre_snapshot(),
        start_action=start_action or _successful_start_mapping_action(),
        mapping_snapshot=_new_vehicle_mapping_snapshot(),
        mapping_session_id=mapping_session_id,
    )


class _CheckpointClient:
    def __init__(self, *, session_ids, live_job=None, found=True):
        self.session_ids = list(session_ids)
        self.live_job = live_job or _FakePayload(**_start_mapping_job_payload())
        self.found = bool(found)
        self.session_reads = 0
        self.slam_job_calls = []

    def get_mapping_session_id(self):
        self.session_reads += 1
        if not self.session_ids:
            return ""
        if len(self.session_ids) == 1:
            return self.session_ids[0]
        return self.session_ids.pop(0)

    def get_slam_job(self, job_id, robot_id):
        self.slam_job_calls.append((job_id, robot_id))
        return _FakePayload(
            found=self.found,
            message="" if self.found else "not found",
            error_code="" if self.found else "JOB_NOT_FOUND",
            job=self.live_job,
        )


def _map_view_response(
    *,
    success=True,
    lifecycle_status="saved_unverified",
    verification_status="pending",
    map_name=CANDIDATE_MAP_NAME,
    map_revision_id=CANDIDATE_REVISION_ID,
    is_active=False,
):
    return _FakePayload(
        success=success,
        message="ok" if success else "map asset not found",
        map=_FakePayload(
            map_name=map_name if success else "",
            map_revision_id=map_revision_id if success else "",
            lifecycle_status=lifecycle_status if success else "",
            verification_status=verification_status if success else "",
            enabled=success,
            is_active=bool(is_active),
        ),
    )


class _MapViewClient(_CheckpointClient):
    def __init__(self, responses, *, session_ids=None):
        super().__init__(
            session_ids=session_ids
            or [
                START_MAPPING_JOB_ID,
                START_MAPPING_JOB_ID,
                START_MAPPING_JOB_ID,
                "",
            ]
        )
        self.responses = list(responses)
        self.calls = []

    def get_map_view(self, map_name, map_revision_id=""):
        self.calls.append((map_name, map_revision_id))
        return self.responses.pop(0)


class RevisionWorkflowAcceptanceIdentityTest(unittest.TestCase):
    def test_write_workflow_rejects_default_robot_identity(self):
        args = build_arg_parser().parse_args([
            "--profile", "mapping_save_candidate",
            "--save-map-name", "site_a",
            "--allow-write-actions",
        ])

        with self.assertRaisesRegex(ValueError, "explicit commercial --robot-id"):
            validate_args(args)

    def test_standalone_revision_profile_requires_exact_map_and_revision(self):
        for incomplete in (
            ["--map-name", "site_a"],
            ["--map-revision-id", "rev_site_a_01"],
        ):
            with self.subTest(incomplete=incomplete):
                args = build_arg_parser().parse_args(
                    [
                        "--profile",
                        "activate_revision_prepare_for_task",
                        "--robot-id",
                        ROBOT_ID,
                        "--allow-write-actions",
                    ]
                    + incomplete
                )
                with self.assertRaisesRegex(ValueError, "--map-name and --map-revision-id"):
                    validate_args(args)

    def test_initial_mapping_profile_requires_pause_and_checkpoint(self):
        base = [
            "--profile",
            "mapping_save_candidate",
            "--robot-id",
            ROBOT_ID,
            "--save-map-name",
            CANDIDATE_MAP_NAME,
            "--allow-write-actions",
        ]
        args = build_arg_parser().parse_args(base)
        with self.assertRaisesRegex(ValueError, "--pause-after-start-mapping"):
            validate_args(args)

        args = build_arg_parser().parse_args(base + ["--pause-after-start-mapping"])
        with self.assertRaisesRegex(ValueError, "--checkpoint-path"):
            validate_args(args)

    def test_activate_prepare_profile_requires_positive_task_id(self):
        args = build_arg_parser().parse_args(
            [
                "--profile",
                "activate_revision_prepare_for_task",
                "--robot-id",
                ROBOT_ID,
                "--map-name",
                CANDIDATE_MAP_NAME,
                "--map-revision-id",
                CANDIDATE_REVISION_ID,
                "--allow-write-actions",
            ]
        )

        with self.assertRaisesRegex(ValueError, "--task-id must be greater than zero"):
            validate_args(args)

    def test_mapping_activate_prepare_in_one_chain_profile_is_not_supported(self):
        self.assertNotIn(
            "mapping_save_verify_activate_prepare_for_task",
            workflow.SUPPORTED_PROFILES,
        )

    def test_capture_snapshot_uses_explicit_robot_id_for_all_identity_queries(self):
        client = _IdentityRecordingClient()

        capture_snapshot(
            client,
            robot_id="CR-001",
            task_id=0,
            ignored_warnings=[],
        )

        self.assertEqual(client.slam_robot_ids, ["CR-001"])
        self.assertEqual(client.odometry_robot_ids, ["CR-001"])

    def test_capture_snapshot_rejects_a_response_for_another_vehicle(self):
        snapshot = capture_snapshot(
            _IdentityRecordingClient(response_robot_id="other-vehicle"),
            robot_id="CR-001",
            task_id=0,
            ignored_warnings=[],
        )

        self.assertFalse(snapshot["checks"]["slam"]["ok"])
        self.assertFalse(snapshot["checks"]["odometry"]["ok"])
        self.assertTrue(
            any("robot_id mismatch" in issue for issue in snapshot["checks"]["slam"]["issues"])
        )
        self.assertTrue(
            any("robot_id mismatch" in issue for issue in snapshot["checks"]["odometry"]["issues"])
        )

    def test_every_workflow_snapshot_forwards_args_robot_id(self):
        with open(MODULE_PATH, "r", encoding="utf-8") as handle:
            tree = ast.parse(handle.read(), filename=MODULE_PATH)

        calls = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "capture_snapshot"
        ]
        self.assertGreater(len(calls), 0)

        for call in calls:
            with self.subTest(line=call.lineno):
                robot_keywords = [item for item in call.keywords if item.arg == "robot_id"]
                self.assertEqual(len(robot_keywords), 1)
                value = robot_keywords[0].value
                self.assertIsInstance(value, ast.Attribute)
                self.assertEqual(value.attr, "robot_id")
                self.assertIsInstance(value.value, ast.Name)
                self.assertEqual(value.value.id, "args")

    def test_build_report_rejects_foreign_provider_before_write_workflow(self):
        topology = _valid_topology_snapshot()
        topology["service_providers"][
            "/clean_robot_server/app/map_server"
        ] = "/foreign_map_server"
        client = mock.Mock()
        client.get_ros_topology_identity.return_value = topology

        with mock.patch.object(
            workflow,
            "BackendRuntimeSmokeClient",
            return_value=client,
        ), mock.patch.object(
            workflow,
            "_run_mapping_workflow_profile",
        ) as run_mapping:
            with self.assertRaisesRegex(ValueError, "revision write topology gate failed"):
                workflow.build_report(
                    _workflow_args(
                        pause_after_start_mapping=True,
                        checkpoint_path="/tmp/revision-checkpoint.json",
                    )
                )

        run_mapping.assert_not_called()

    def test_build_report_enforces_write_authorization_before_client_creation(self):
        args = _workflow_args(
            profile="verify_revision",
            allow_write_actions=False,
        )
        with mock.patch.object(workflow, "BackendRuntimeSmokeClient") as client_factory:
            with self.assertRaisesRegex(ValueError, "--allow-write-actions is required"):
                workflow.build_report(args)

        client_factory.assert_not_called()

    def test_build_report_enforces_mapping_pause_gate_before_client_creation(self):
        args = _workflow_args()
        with mock.patch.object(workflow, "BackendRuntimeSmokeClient") as client_factory:
            with self.assertRaisesRegex(ValueError, "--pause-after-start-mapping"):
                workflow.build_report(args)

        client_factory.assert_not_called()

    def test_build_report_rejects_insecure_checkpoint_before_client_creation(self):
        with tempfile.TemporaryDirectory() as directory:
            real_path = os.path.join(directory, "real-checkpoint.json")
            link_path = os.path.join(directory, "checkpoint-link.json")
            workflow._write_checkpoint(real_path, {})
            os.symlink(real_path, link_path)
            args = _workflow_args(
                pause_after_start_mapping=True,
                checkpoint_path=link_path,
            )
            with mock.patch.object(workflow, "BackendRuntimeSmokeClient") as client_factory:
                with self.assertRaisesRegex(ValueError, "must not be a symlink"):
                    workflow.build_report(args)

            client_factory.assert_not_called()


class RevisionWorkflowCheckpointBindingTest(unittest.TestCase):
    def _write_checkpoint(self, directory, payload):
        path = os.path.join(directory, "revision-checkpoint.json")
        workflow._write_checkpoint(path, payload)
        return path

    def test_checkpoint_write_is_atomic_private_and_round_trips(self):
        with tempfile.TemporaryDirectory() as directory:
            args = _workflow_args()
            checkpoint_path = os.path.join(directory, "revision-checkpoint.json")
            payload = _checkpoint_payload(args)

            workflow._write_checkpoint(checkpoint_path, payload)

            self.assertEqual(
                stat.S_IMODE(os.lstat(checkpoint_path).st_mode),
                0o600,
            )
            self.assertEqual(workflow._load_checkpoint(checkpoint_path), payload)
            self.assertFalse(
                any(
                    item.startswith(".revision-checkpoint.json.")
                    for item in os.listdir(directory)
                )
            )

    def test_checkpoint_replace_failure_preserves_old_file_and_cleans_temp(self):
        with tempfile.TemporaryDirectory() as directory:
            args = _workflow_args()
            checkpoint_path = self._write_checkpoint(
                directory,
                _checkpoint_payload(args),
            )
            with open(checkpoint_path, "rb") as handle:
                original = handle.read()

            with mock.patch.object(
                workflow.os,
                "replace",
                side_effect=OSError("forced replace failure"),
            ):
                with self.assertRaisesRegex(OSError, "forced replace failure"):
                    workflow._write_checkpoint(
                        checkpoint_path,
                        dict(_checkpoint_payload(args), phase="changed"),
                    )

            with open(checkpoint_path, "rb") as handle:
                self.assertEqual(handle.read(), original)
            self.assertFalse(
                any(item.endswith(".tmp") for item in os.listdir(directory))
            )

    def test_checkpoint_rejects_target_and_parent_symlinks(self):
        with tempfile.TemporaryDirectory() as directory:
            args = _workflow_args()
            real_path = self._write_checkpoint(
                directory,
                _checkpoint_payload(args),
            )
            link_path = os.path.join(directory, "checkpoint-link.json")
            os.symlink(real_path, link_path)

            for operation in (
                lambda: workflow._load_checkpoint(link_path),
                lambda: workflow._write_checkpoint(link_path, _checkpoint_payload(args)),
            ):
                with self.subTest(operation=operation):
                    with self.assertRaisesRegex(ValueError, "must not be a symlink"):
                        operation()

            linked_parent = os.path.join(directory, "linked-parent")
            os.symlink(directory, linked_parent)
            linked_child = os.path.join(linked_parent, "revision-checkpoint.json")
            with self.assertRaisesRegex(ValueError, "parent path must not contain a symlink"):
                workflow._load_checkpoint(linked_child)
            with self.assertRaisesRegex(ValueError, "parent path must not contain a symlink"):
                workflow._write_checkpoint(linked_child, _checkpoint_payload(args))

    def test_checkpoint_rejects_broad_permissions_and_foreign_owner(self):
        with tempfile.TemporaryDirectory() as directory:
            args = _workflow_args()
            checkpoint_path = self._write_checkpoint(
                directory,
                _checkpoint_payload(args),
            )
            os.chmod(checkpoint_path, 0o640)
            with self.assertRaisesRegex(ValueError, "permissions are too broad"):
                workflow._load_checkpoint(checkpoint_path)
            with self.assertRaisesRegex(ValueError, "permissions are too broad"):
                workflow._write_checkpoint(checkpoint_path, _checkpoint_payload(args))

            os.chmod(checkpoint_path, 0o600)
            with mock.patch.object(
                workflow.os,
                "geteuid",
                return_value=os.geteuid() + 1,
            ):
                with self.assertRaisesRegex(ValueError, "owner mismatch"):
                    workflow._load_checkpoint(checkpoint_path)

    def test_pause_writes_v2_checkpoint_bound_to_live_mapping_session(self):
        with tempfile.TemporaryDirectory() as directory:
            checkpoint_path = os.path.join(directory, "revision-checkpoint.json")
            args = _workflow_args(
                pause_after_start_mapping=True,
                checkpoint_path=checkpoint_path,
            )
            client = _CheckpointClient(session_ids=[START_MAPPING_JOB_ID])
            with mock.patch.object(
                workflow,
                "capture_snapshot",
                side_effect=[_new_vehicle_pre_snapshot(), _new_vehicle_mapping_snapshot()],
            ), mock.patch.object(
                workflow,
                "_submit_and_wait",
                return_value=_successful_start_mapping_action(),
            ):
                report = workflow._run_mapping_workflow_profile(
                    client,
                    args,
                    verify_after_save=False,
                    prepare_after_activate=False,
                )

            with open(checkpoint_path, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
            self.assertTrue(report["paused"])
            self.assertEqual(payload["checkpoint_version"], 2)
            self.assertEqual(payload["phase"], workflow.RESUMABLE_PHASE)
            self.assertEqual(payload["mapping_session_id"], START_MAPPING_JOB_ID)
            self.assertEqual(
                payload["mapping_session_id"],
                payload["start_action"]["submit"]["job_id"],
            )

    def test_direct_mapping_chain_without_pause_is_rejected_before_start(self):
        args = _workflow_args()
        with mock.patch.object(workflow, "capture_snapshot") as capture, mock.patch.object(
            workflow,
            "_submit_and_wait",
        ) as submit:
            with self.assertRaisesRegex(ValueError, "--pause-after-start-mapping"):
                workflow._run_mapping_workflow_profile(
                    mock.Mock(),
                    args,
                    verify_after_save=False,
                    prepare_after_activate=False,
                )

        capture.assert_not_called()
        submit.assert_not_called()

    def test_direct_mapping_prepare_chain_is_rejected_before_resume(self):
        args = _workflow_args(
            profile="mapping_save_verify_activate_prepare_for_task",
            task_id=0,
            resume_from_checkpoint="/tmp/revision-checkpoint.json",
        )
        with mock.patch.object(workflow, "capture_snapshot") as capture, mock.patch.object(
            workflow,
            "_submit_and_wait",
        ) as submit:
            with self.assertRaisesRegex(ValueError, "prepare-in-one-chain is unsupported"):
                workflow._run_mapping_workflow_profile(
                    mock.Mock(),
                    args,
                    verify_after_save=True,
                    prepare_after_activate=True,
                )

        capture.assert_not_called()
        submit.assert_not_called()

    def test_pause_refuses_missing_or_mismatched_live_mapping_session(self):
        for observed_session_id in ("", "job-from-another-session"):
            with self.subTest(observed_session_id=observed_session_id or "missing"):
                with tempfile.TemporaryDirectory() as directory:
                    checkpoint_path = os.path.join(directory, "revision-checkpoint.json")
                    args = _workflow_args(
                        pause_after_start_mapping=True,
                        checkpoint_path=checkpoint_path,
                    )
                    client = _CheckpointClient(session_ids=[observed_session_id])
                    with mock.patch.object(
                        workflow,
                        "capture_snapshot",
                        side_effect=[
                            _new_vehicle_pre_snapshot(),
                            _new_vehicle_mapping_snapshot(),
                        ],
                    ), mock.patch.object(
                        workflow,
                        "_submit_and_wait",
                        return_value=_successful_start_mapping_action(),
                    ):
                        with self.assertRaisesRegex(ValueError, "mapping_session_id mismatch"):
                            workflow._run_mapping_workflow_profile(
                                client,
                                args,
                                verify_after_save=False,
                                prepare_after_activate=False,
                            )
                    self.assertFalse(os.path.exists(checkpoint_path))

    def test_version_one_checkpoint_is_invalidated(self):
        with tempfile.TemporaryDirectory() as directory:
            args = _workflow_args()
            payload = _checkpoint_payload(args)
            payload["checkpoint_version"] = 1
            checkpoint_path = self._write_checkpoint(directory, payload)

            with self.assertRaisesRegex(ValueError, "unsupported checkpoint version"):
                workflow._load_checkpoint(checkpoint_path)

    def test_resume_rechecks_live_token_before_save_and_never_submits_on_mismatch(self):
        with tempfile.TemporaryDirectory() as directory:
            initial_args = _workflow_args()
            checkpoint_path = self._write_checkpoint(
                directory,
                _checkpoint_payload(initial_args),
            )
            args = _workflow_args(resume_from_checkpoint=checkpoint_path)
            client = _CheckpointClient(
                session_ids=[
                    START_MAPPING_JOB_ID,
                    START_MAPPING_JOB_ID,
                    "job-from-another-session",
                ]
            )
            with mock.patch.object(
                workflow,
                "capture_snapshot",
                return_value=_new_vehicle_mapping_snapshot(),
            ) as capture, mock.patch.object(workflow, "_submit_and_wait") as submit:
                with self.assertRaisesRegex(ValueError, "before save_mapping mapping_session_id mismatch"):
                    workflow._run_mapping_workflow_profile(
                        client,
                        args,
                        verify_after_save=False,
                        prepare_after_activate=False,
                    )

            self.assertEqual(capture.call_count, 1)
            submit.assert_not_called()
            with open(checkpoint_path, "r", encoding="utf-8") as handle:
                consumed_payload = json.load(handle)
            self.assertEqual(consumed_payload["phase"], "resume_in_progress")

    def test_resume_rejects_tampered_checkpoint_start_action_contract(self):
        mutations = {
            "foreign robot": lambda action: action["job"]["response"]["job"].update(
                robot_id="CR-999"
            ),
            "wrong operation": lambda action: action["job"]["response"]["job"].update(
                operation=4,
                operation_name="save_mapping",
            ),
            "wrong description": lambda action: action["job"]["response"]["job"].update(
                description="tampered:start_mapping"
            ),
            "failed terminal": lambda action: action["job"]["response"]["job"].update(
                job_state="failed",
                status="failed",
                success=False,
                result_success=False,
            ),
        }
        for label, mutate in mutations.items():
            with self.subTest(label=label):
                with tempfile.TemporaryDirectory() as directory:
                    action = _successful_start_mapping_action()
                    mutate(action)
                    args = _workflow_args()
                    checkpoint_path = self._write_checkpoint(
                        directory,
                        _checkpoint_payload(args, start_action=action),
                    )
                    args.resume_from_checkpoint = checkpoint_path

                    with self.assertRaisesRegex(ValueError, "checkpoint start_action contract"):
                        workflow._load_mapping_checkpoint_context(args)

    def test_resume_rejects_foreign_or_failed_live_start_job_before_snapshot_or_actions(self):
        live_jobs = {
            "foreign": _FakePayload(
                **_start_mapping_job_payload(robot_id="CR-999")
            ),
            "failed": _FakePayload(
                **_start_mapping_job_payload(
                    job_state="failed",
                    status="failed",
                    success=False,
                    result_success=False,
                )
            ),
        }
        for label, live_job in live_jobs.items():
            with self.subTest(label=label):
                with tempfile.TemporaryDirectory() as directory:
                    initial_args = _workflow_args()
                    checkpoint_path = self._write_checkpoint(
                        directory,
                        _checkpoint_payload(initial_args),
                    )
                    args = _workflow_args(resume_from_checkpoint=checkpoint_path)
                    client = _CheckpointClient(
                        session_ids=[START_MAPPING_JOB_ID],
                        live_job=live_job,
                    )
                    with mock.patch.object(workflow, "capture_snapshot") as capture, mock.patch.object(
                        workflow,
                        "_submit_and_wait",
                    ) as submit:
                        with self.assertRaisesRegex(ValueError, "live session/job gate failed"):
                            workflow._run_mapping_workflow_profile(
                                client,
                                args,
                                verify_after_save=False,
                                prepare_after_activate=False,
                            )
                    capture.assert_not_called()
                    submit.assert_not_called()

    def test_resume_requires_empty_live_mapping_session_after_successful_stop(self):
        with tempfile.TemporaryDirectory() as directory:
            initial_args = _workflow_args()
            checkpoint_path = self._write_checkpoint(
                directory,
                _checkpoint_payload(initial_args),
            )
            args = _workflow_args(resume_from_checkpoint=checkpoint_path)
            client = _MapViewClient(
                [_map_view_response()],
                session_ids=[
                    START_MAPPING_JOB_ID,
                    START_MAPPING_JOB_ID,
                    START_MAPPING_JOB_ID,
                    "stale-mapping-session",
                ],
            )

            def submit(_client, *, action_name, **_kwargs):
                return _successful_action(action_name)

            with mock.patch.object(
                workflow,
                "capture_snapshot",
                side_effect=[
                    _new_vehicle_mapping_snapshot(),
                    _new_vehicle_mapping_snapshot(),
                    _new_vehicle_post_stop_snapshot(),
                ],
            ), mock.patch.object(workflow, "_submit_and_wait", side_effect=submit):
                report = workflow._run_mapping_workflow_profile(
                    client,
                    args,
                    verify_after_save=False,
                    prepare_after_activate=False,
                )

            self.assertFalse(report["summary"]["ok"])
            self.assertEqual(
                report["post_stop_mapping_session"]["mapping_session_id"],
                "stale-mapping-session",
            )
            self.assertTrue(
                any(
                    "mapping_session_id must be empty after successful stop_mapping" in issue
                    for issue in report["summary"]["issues"]
                )
            )


class RevisionWorkflowStandaloneProfileTest(unittest.TestCase):
    def test_standalone_verify_requires_exact_verified_available_asset(self):
        args = _workflow_args(profile="verify_revision")
        client = _MapViewClient(
            [
                _map_view_response(
                    lifecycle_status="available",
                    verification_status="verified",
                )
            ]
        )
        with mock.patch.object(
            workflow,
            "capture_snapshot",
            side_effect=[_workflow_snapshot(), _workflow_snapshot()],
        ), mock.patch.object(
            workflow,
            "_submit_and_wait",
            return_value=_successful_action("verify_map_revision"),
        ):
            report = workflow._run_verify_revision_profile(client, args)

        self.assertTrue(report["summary"]["ok"], msg=report["summary"]["issues"])
        self.assertEqual(
            client.calls,
            [(CANDIDATE_MAP_NAME, CANDIDATE_REVISION_ID)],
        )

    def test_standalone_verify_rejects_wrong_asset_identity(self):
        args = _workflow_args(profile="verify_revision")
        client = _MapViewClient(
            [
                _map_view_response(
                    lifecycle_status="available",
                    verification_status="verified",
                    map_revision_id="wrong-revision",
                )
            ]
        )
        with mock.patch.object(
            workflow,
            "capture_snapshot",
            side_effect=[_workflow_snapshot(), _workflow_snapshot()],
        ), mock.patch.object(
            workflow,
            "_submit_and_wait",
            return_value=_successful_action("verify_map_revision"),
        ):
            report = workflow._run_verify_revision_profile(client, args)

        self.assertFalse(report["summary"]["ok"])
        self.assertTrue(
            any(
                "post standalone verify target map_revision_id mismatch" in issue
                for issue in report["summary"]["issues"]
            )
        )

    def test_standalone_activate_requires_active_verified_available_asset(self):
        args = _workflow_args(profile="activate_revision")
        client = _MapViewClient(
            [
                _map_view_response(
                    lifecycle_status="available",
                    verification_status="verified",
                    is_active=True,
                )
            ]
        )
        with mock.patch.object(
            workflow,
            "capture_snapshot",
            side_effect=[_workflow_snapshot(), _candidate_runtime_snapshot()],
        ), mock.patch.object(
            workflow,
            "_submit_and_wait",
            return_value=_successful_action("activate_map_revision"),
        ):
            report = workflow._run_activate_revision_profile(
                client,
                args,
                prepare_after_activate=False,
            )

        self.assertTrue(report["summary"]["ok"], msg=report["summary"]["issues"])

    def test_standalone_activate_rejects_asset_not_marked_active(self):
        args = _workflow_args(profile="activate_revision")
        client = _MapViewClient(
            [
                _map_view_response(
                    lifecycle_status="available",
                    verification_status="verified",
                    is_active=False,
                )
            ]
        )
        with mock.patch.object(
            workflow,
            "capture_snapshot",
            side_effect=[_workflow_snapshot(), _candidate_runtime_snapshot()],
        ), mock.patch.object(
            workflow,
            "_submit_and_wait",
            return_value=_successful_action("activate_map_revision"),
        ):
            report = workflow._run_activate_revision_profile(
                client,
                args,
                prepare_after_activate=False,
            )

        self.assertFalse(report["summary"]["ok"])
        self.assertIn(
            "post standalone activate target is_active mismatch expected=true actual=false",
            report["summary"]["issues"],
        )

    def test_activate_prepare_rejects_nonpositive_task_before_any_read_or_write(self):
        args = _workflow_args(
            profile="activate_revision_prepare_for_task",
            task_id=0,
        )
        with mock.patch.object(workflow, "capture_snapshot") as capture, mock.patch.object(
            workflow,
            "_submit_and_wait",
        ) as submit:
            with self.assertRaisesRegex(ValueError, "--task-id must be greater than zero"):
                workflow._run_activate_revision_profile(
                    mock.Mock(),
                    args,
                    prepare_after_activate=True,
                )

        capture.assert_not_called()
        submit.assert_not_called()


class RevisionWorkflowMainChainTest(unittest.TestCase):
    def _run_mapping_chain(self, *, map_views, snapshots):
        action_names = []

        def submit(_client, *, action_name, **_kwargs):
            action_names.append(action_name)
            return _successful_action(action_name)

        profile_name = "mapping_save_verify_activate"
        initial_args = _workflow_args(profile=profile_name)
        snapshots = list(snapshots)
        with tempfile.TemporaryDirectory() as directory:
            checkpoint_path = os.path.join(directory, "revision-checkpoint.json")
            workflow._write_checkpoint(
                checkpoint_path,
                _checkpoint_payload(
                    initial_args,
                    profile_name=profile_name,
                )
            )
            args = _workflow_args(
                profile=profile_name,
                resume_from_checkpoint=checkpoint_path,
            )
            client = _MapViewClient(map_views)
            with mock.patch.object(
                workflow,
                "capture_snapshot",
                side_effect=snapshots[1:],
            ), mock.patch.object(workflow, "_submit_and_wait", side_effect=submit):
                report = workflow._run_mapping_workflow_profile(
                    client,
                    args,
                    verify_after_save=True,
                    prepare_after_activate=False,
                )
        return report, [item["name"] for item in report["actions"]], client.calls

    def test_complete_five_action_mapping_chain_uses_real_stop_semantics(self):
        report, action_names, map_view_calls = self._run_mapping_chain(
            map_views=[
                _map_view_response(),
                _map_view_response(
                    lifecycle_status="available",
                    verification_status="verified",
                ),
            ],
            snapshots=[
                _new_vehicle_pre_snapshot(),
                _new_vehicle_mapping_snapshot(),
                _new_vehicle_mapping_snapshot(),
                _new_vehicle_post_stop_snapshot(),
                _verified_candidate_without_active_map_snapshot(),
                _candidate_runtime_snapshot(),
            ],
        )

        self.assertEqual(
            action_names,
            [
                "start_mapping",
                "save_mapping",
                "stop_mapping",
                "verify_map_revision",
                "activate_map_revision",
            ],
        )
        self.assertEqual(
            map_view_calls,
            [
                (CANDIDATE_MAP_NAME, CANDIDATE_REVISION_ID),
                (CANDIDATE_MAP_NAME, CANDIDATE_REVISION_ID),
            ],
        )
        self.assertTrue(report["summary"]["ok"], msg=report["summary"]["issues"])

    def test_missing_saved_candidate_does_not_continue_to_verify(self):
        report, action_names, map_view_calls = self._run_mapping_chain(
            map_views=[_map_view_response(success=False)],
            snapshots=[
                _new_vehicle_pre_snapshot(),
                _new_vehicle_mapping_snapshot(),
                _new_vehicle_mapping_snapshot(),
                _new_vehicle_post_stop_snapshot(),
            ],
        )

        self.assertEqual(action_names, ["start_mapping", "save_mapping", "stop_mapping"])
        self.assertEqual(
            map_view_calls,
            [(CANDIDATE_MAP_NAME, CANDIDATE_REVISION_ID)],
        )
        self.assertFalse(report["summary"]["ok"])
        self.assertTrue(
            any("post save candidate map revision lookup failed" in issue for issue in report["summary"]["issues"])
        )

    def test_unverified_candidate_does_not_continue_to_activate(self):
        report, action_names, map_view_calls = self._run_mapping_chain(
            map_views=[
                _map_view_response(),
                _map_view_response(),
            ],
            snapshots=[
                _new_vehicle_pre_snapshot(),
                _new_vehicle_mapping_snapshot(),
                _new_vehicle_mapping_snapshot(),
                _new_vehicle_post_stop_snapshot(),
                _verified_candidate_without_active_map_snapshot(),
            ],
        )

        self.assertEqual(
            action_names,
            ["start_mapping", "save_mapping", "stop_mapping", "verify_map_revision"],
        )
        self.assertEqual(len(map_view_calls), 2)
        self.assertFalse(report["summary"]["ok"])
        self.assertTrue(
            any("post verify candidate lifecycle_status mismatch" in issue for issue in report["summary"]["issues"])
        )

    def test_failed_stop_postcondition_does_not_continue_to_verify(self):
        invalid_post_stop = _new_vehicle_post_stop_snapshot()
        invalid_post_stop["slam"]["localization_valid"] = True

        report, action_names, _map_view_calls = self._run_mapping_chain(
            map_views=[_map_view_response()],
            snapshots=[
                _new_vehicle_pre_snapshot(),
                _new_vehicle_mapping_snapshot(),
                _new_vehicle_mapping_snapshot(),
                invalid_post_stop,
            ],
        )

        self.assertEqual(action_names, ["start_mapping", "save_mapping", "stop_mapping"])
        self.assertFalse(report["summary"]["ok"])
        self.assertIn("post stop localization_valid=true", report["summary"]["issues"])

    def test_failed_stop_desired_mode_does_not_continue_to_verify(self):
        invalid_post_stop = _new_vehicle_post_stop_snapshot()
        invalid_post_stop["slam"]["desired_mode"] = "mapping"

        report, action_names, _map_view_calls = self._run_mapping_chain(
            map_views=[_map_view_response()],
            snapshots=[
                _new_vehicle_pre_snapshot(),
                _new_vehicle_mapping_snapshot(),
                _new_vehicle_mapping_snapshot(),
                invalid_post_stop,
            ],
        )

        self.assertEqual(action_names, ["start_mapping", "save_mapping", "stop_mapping"])
        self.assertFalse(report["summary"]["ok"])
        self.assertIn(
            "post stop desired_mode mismatch expected=localization actual=mapping",
            report["summary"]["issues"],
        )

    def test_activate_requires_exact_map_name_and_revision(self):
        snapshot = _candidate_runtime_snapshot()
        snapshot["slam"]["active_map_name"] = "wrong_map"

        issues = workflow.activate_profile_issues(
            snapshot,
            _successful_action("activate_map_revision"),
            target_map_name=CANDIDATE_MAP_NAME,
            target_revision_id=CANDIDATE_REVISION_ID,
        )

        self.assertTrue(any("active map mismatch" in issue for issue in issues))

    def test_activate_requires_exact_localization_mode_triplet(self):
        snapshot = _candidate_runtime_snapshot()
        snapshot["slam"]["runtime_mode"] = "mapping"

        issues = workflow.activate_profile_issues(
            snapshot,
            _successful_action("activate_map_revision"),
            target_map_name=CANDIDATE_MAP_NAME,
            target_revision_id=CANDIDATE_REVISION_ID,
        )

        self.assertIn(
            "post activate runtime_mode mismatch expected=localization actual=mapping",
            issues,
        )

    def test_prepare_requires_exact_readiness_task_binding(self):
        snapshot = _candidate_runtime_snapshot(task_id=42, prepared=True)
        snapshot["readiness"]["task_id"] = 99
        snapshot["readiness"]["task_map_revision_id"] = "wrong_revision"

        issues = workflow.prepare_for_task_profile_issues(
            snapshot,
            _successful_action("prepare_for_task"),
            target_map_name=CANDIDATE_MAP_NAME,
            target_revision_id=CANDIDATE_REVISION_ID,
            target_task_id=42,
        )

        self.assertTrue(any("task_id mismatch" in issue for issue in issues))
        self.assertTrue(any("task revision mismatch" in issue for issue in issues))

    def test_prepare_requires_fully_ready_clean_checks(self):
        snapshot = _candidate_runtime_snapshot(task_id=42, prepared=True)
        snapshot["readiness"]["overall_ready"] = False
        snapshot["readiness"]["blocking_reasons"] = ["battery unavailable"]
        snapshot["slam"]["warnings"] = ["localization degraded"]
        snapshot["odometry"]["warnings"] = ["odom stale"]
        snapshot["checks"]["odometry"]["ok"] = False
        snapshot["checks"]["odometry"]["issues"] = ["odom stale"]

        issues = workflow.prepare_for_task_profile_issues(
            snapshot,
            _successful_action("prepare_for_task"),
            target_map_name=CANDIDATE_MAP_NAME,
            target_revision_id=CANDIDATE_REVISION_ID,
            target_task_id=42,
        )

        self.assertIn("post prepare_for_task overall_ready=false", issues)
        self.assertTrue(any("readiness blocking_reasons must be empty" in item for item in issues))
        self.assertTrue(any("slam warnings must be empty" in item for item in issues))
        self.assertTrue(any("odometry warnings must be empty" in item for item in issues))
        self.assertIn("post prepare_for_task odometry check ok=false", issues)
        self.assertTrue(any("odometry check contains issues" in item for item in issues))

    def test_preflight_without_active_map_cannot_gain_one_during_save_stop(self):
        pre_snapshot = _workflow_snapshot(
            active_map_name="",
            active_revision_id="",
            runtime_map_name="",
            runtime_revision_id="",
            localization_valid=False,
            runtime_map_match=False,
        )
        post_snapshot = _post_stop_snapshot()

        issues = workflow.mapping_save_candidate_issues(
            pre_snapshot,
            post_snapshot,
            candidate_revision_id=CANDIDATE_REVISION_ID,
        )

        self.assertTrue(any("active_map_name" in issue for issue in issues))
        self.assertTrue(any("active_map_revision_id" in issue for issue in issues))


if __name__ == "__main__":
    unittest.main()

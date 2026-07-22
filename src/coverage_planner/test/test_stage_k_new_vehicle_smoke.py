#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import unittest
from unittest import mock


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
PKG_DIR = os.path.dirname(THIS_DIR)
PKG_SRC_DIR = os.path.join(PKG_DIR, "src")
TOOLS_DIR = os.path.join(PKG_DIR, "tools")
for candidate in (PKG_SRC_DIR, TOOLS_DIR):
    if candidate not in sys.path:
        sys.path.insert(0, candidate)

import run_backend_runtime_smoke as runtime_smoke
from run_backend_runtime_smoke import (
    STAGE_K_NEW_VEHICLE_PROFILE,
    STAGE_K_OPTIONAL_READINESS_WARNINGS,
    STAGE_K_READINESS_CHECKS,
    STAGE_K_REQUIRED_READINESS_WARNINGS,
    STAGE_K_SLAM_WARNINGS,
    _check_odometry,
    _check_stage_k_new_vehicle_readiness,
    _check_stage_k_new_vehicle_slam,
    build_arg_parser,
    run_read_checks,
    validate_args,
)


ROBOT_ID = "CR-001"


class _FakePayload:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


def _valid_slam_response(robot_id=ROBOT_ID, blockers=None, warnings=None):
    localization_blocker = "runtime localization not ready: state=- valid=false"
    return _FakePayload(
        success=True,
        message="ok",
        state=_FakePayload(
            robot_id=robot_id,
            desired_mode="localization",
            current_mode="localization",
            runtime_mode="localization",
            workflow_state="IDLE",
            workflow_phase="idle",
            active_map_name="",
            active_map_revision_id="",
            active_map_id="",
            active_map_md5="",
            runtime_map_name="",
            runtime_map_revision_id="",
            runtime_map_id="",
            runtime_map_md5="",
            pending_map_name="",
            pending_map_revision_id="",
            pending_map_switch_status="",
            localization_state="",
            localization_valid=False,
            runtime_map_ready=False,
            active_map_match=False,
            runtime_map_match=False,
            lifecycle_state="steady",
            active_job_id="",
            active_job_status="",
            active_job_phase="",
            map_topic_fresh=False,
            tracked_pose_fresh=False,
            mission_state="IDLE",
            phase="IDLE",
            public_state="IDLE",
            executor_state="",
            task_running=False,
            localization_backend_available=True,
            runtime_reload_service_available=True,
            runtime_save_state_service_available=True,
            can_switch_map_and_localize=False,
            can_relocalize=False,
            can_verify_map_revision=False,
            can_activate_map_revision=False,
            can_start_mapping=True,
            can_save_mapping=False,
            can_stop_mapping=False,
            busy=False,
            mapping_session_active=False,
            task_ready=False,
            manual_assist_required=False,
            manual_assist_map_name="",
            manual_assist_map_revision_id="",
            manual_assist_retry_action="",
            manual_assist_guidance="",
            last_error_code="",
            last_error_msg="",
            blocking_reasons=(
                list(blockers)
                if blockers is not None
                else ["runtime /map identity unavailable", localization_blocker]
            ),
            warnings=list(warnings) if warnings is not None else sorted(STAGE_K_SLAM_WARNINGS),
            stamp=_FakePayload(secs=1, nsecs=0),
        ),
    )


def _valid_odometry_response(robot_id=ROBOT_ID):
    return _FakePayload(
        success=True,
        message="ok",
        state=_FakePayload(
            robot_id=robot_id,
            odom_source="odom_stream",
            odom_topic="/odom",
            raw_odom_topic="/odom_raw",
            imu_topic="/imu",
            validation_mode="odom_stream",
            connected=True,
            odom_stream_ready=True,
            frame_id_valid=True,
            child_frame_id_valid=True,
            odom_valid=True,
            error_code="",
            message="ok",
            warnings=[],
            stamp=_FakePayload(secs=1, nsecs=0),
        ),
    )


def _valid_readiness_checks():
    result = []
    for key, (level, ok) in STAGE_K_READINESS_CHECKS.items():
        if isinstance(level, tuple):
            level = "OK"
        result.append(
            _FakePayload(
                key=key,
                level=level,
                ok=ok,
                fresh=bool(ok),
                stale=False,
                missing=not bool(ok),
                age_s=-1.0,
                summary="fixture",
            )
        )
    return result


def _valid_readiness_response(blockers=None, warnings=None, checks=None):
    localization_blocker = "runtime localization not ready: state=- valid=false"
    return _FakePayload(
        success=True,
        message="no current active map selected",
        readiness=_FakePayload(
            overall_ready=False,
            can_start_task=False,
            task_id=0,
            task_name="",
            task_map_name="",
            task_map_revision_id="",
            task_zone_id="",
            task_plan_profile="",
            active_map_name="",
            active_map_revision_id="",
            active_map_id="",
            active_map_md5="",
            runtime_map_name="",
            runtime_map_revision_id="",
            runtime_map_id="",
            runtime_map_md5="",
            mission_state="IDLE",
            phase="IDLE",
            public_state="IDLE",
            executor_state="",
            dock_supply_state="IDLE",
            battery_valid=False,
            manual_assist_required=False,
            manual_assist_map_name="",
            manual_assist_map_revision_id="",
            manual_assist_retry_action="",
            manual_assist_guidance="",
            blocking_reasons=(
                list(blockers)
                if blockers is not None
                else [
                    "no current active map selected",
                    "runtime /map identity unavailable",
                    localization_blocker,
                ]
            ),
            warnings=(
                list(warnings)
                if warnings is not None
                else sorted(STAGE_K_REQUIRED_READINESS_WARNINGS)
            ),
            checks=list(checks) if checks is not None else _valid_readiness_checks(),
            stamp=_FakePayload(secs=1, nsecs=0),
        ),
    )


def _valid_stage_args(extra=None):
    argv = [
        "--profile",
        STAGE_K_NEW_VEHICLE_PROFILE,
        "--robot-id",
        ROBOT_ID,
        "--plan-db-path",
        "/data/coverage/planning.db",
        "--ops-db-path",
        "/data/coverage/operations.db",
        "--maps-root",
        "/data/maps",
        "--dock-calibration-path",
        "/data/coverage/dock_calibration.yaml",
        "--auto-charge-state-path",
        "/data/coverage/auto_charge_monitor_state.json",
        "--auto-charge-event-log-path",
        "/data/coverage/auto_charge_monitor_events.jsonl",
    ]
    argv.extend(list(extra or []))
    return build_arg_parser().parse_args(argv)


def _valid_topology_snapshot():
    return {
        "service_providers": dict(runtime_smoke.STAGE_K_EXPECTED_SERVICE_PROVIDERS),
        "node_robot_ids": {
            node_name: ROBOT_ID
            for _service_name, node_name in runtime_smoke.STAGE_K_EXPECTED_SERVICE_PROVIDERS
        },
        "node_private_params": {
            node_name: dict(expected_params)
            for node_name, expected_params in runtime_smoke.STAGE_K_EXPECTED_NODE_PRIVATE_PARAMS.items()
        },
        "mapping_session_id": "",
    }


class _FakeTopologyClient:
    def __init__(self, topology_snapshot):
        self.topology_snapshot = topology_snapshot
        self.waited = False

    def wait_for_services(self, timeout_s):
        self.waited = True

    def get_ros_topology_identity(self):
        return self.topology_snapshot


class _FailingWaitClient(_FakeTopologyClient):
    def wait_for_services(self, timeout_s):
        self.waited = True
        raise RuntimeError("service wait failed")


class StageKNewVehicleSmokeTest(unittest.TestCase):
    def _build_report_with_topology(self, topology_snapshot):
        args = _valid_stage_args()
        validate_args(args)
        client = _FakeTopologyClient(topology_snapshot)

        def commissioning_check(_args, name):
            return {
                "name": name,
                "ok": True,
                "issues": [],
                "response": {"snapshot": {"robot_id": ROBOT_ID, "stable": True}},
            }

        with mock.patch.object(
            runtime_smoke,
            "BackendRuntimeSmokeClient",
            return_value=client,
        ), mock.patch.object(
            runtime_smoke,
            "_commissioning_state_check",
            side_effect=commissioning_check,
        ), mock.patch.object(runtime_smoke, "run_read_checks", return_value=[]) as read_checks:
            report = runtime_smoke.build_report(args)
        return report, client, read_checks.call_count

    def test_stage_k_topology_identity_accepts_exact_providers_and_robot_ids(self):
        report, client, read_check_calls = self._build_report_with_topology(
            _valid_topology_snapshot()
        )

        topology_check = next(
            item for item in report["checks"]
            if item["name"] == "stage_k_ros_topology_identity"
        )
        self.assertTrue(client.waited)
        self.assertTrue(topology_check["ok"], msg=topology_check["issues"])
        self.assertEqual(read_check_calls, 1)
        self.assertTrue(report["summary"]["ok"], msg=report["summary"]["issues"])

    def test_stage_k_topology_identity_fails_closed_on_wrong_provider(self):
        snapshot = _valid_topology_snapshot()
        snapshot["service_providers"][
            "/clean_robot_server/app/get_slam_status"
        ] = "/unexpected_slam_node"

        report, _client, read_check_calls = self._build_report_with_topology(snapshot)

        topology_check = next(
            item for item in report["checks"]
            if item["name"] == "stage_k_ros_topology_identity"
        )
        self.assertFalse(topology_check["ok"])
        self.assertTrue(
            any("service provider mismatch" in issue for issue in topology_check["issues"])
        )
        self.assertEqual(read_check_calls, 0)
        self.assertEqual(report["actions"], [])
        self.assertFalse(report["summary"]["ok"])

    def test_stage_k_topology_identity_fails_closed_on_wrong_dock_provider(self):
        snapshot = _valid_topology_snapshot()
        snapshot["service_providers"][
            "/clean_robot_server/app/dock_calibration_command"
        ] = "/coverage_task_manager"

        report, _client, read_check_calls = self._build_report_with_topology(snapshot)

        topology_check = next(
            item for item in report["checks"]
            if item["name"] == "stage_k_ros_topology_identity"
        )
        self.assertFalse(topology_check["ok"])
        self.assertTrue(
            any("service provider mismatch" in issue for issue in topology_check["issues"])
        )
        self.assertEqual(read_check_calls, 0)

    def test_stage_k_topology_identity_fails_closed_on_dock_storage_path(self):
        snapshot = _valid_topology_snapshot()
        snapshot["node_private_params"]["/dock_calibration_service"][
            "storage_path"
        ] = "/tmp/dock_calibration.yaml"

        report, _client, read_check_calls = self._build_report_with_topology(snapshot)

        topology_check = next(
            item for item in report["checks"]
            if item["name"] == "stage_k_ros_topology_identity"
        )
        self.assertFalse(topology_check["ok"])
        self.assertTrue(
            any(
                "node private parameter mismatch" in issue
                and "storage_path" in issue
                for issue in topology_check["issues"]
            )
        )
        self.assertEqual(read_check_calls, 0)
        self.assertEqual(report["actions"], [])

    def test_stage_k_topology_identity_fails_closed_on_missing_runtime_repo_root(self):
        snapshot = _valid_topology_snapshot()
        del snapshot["node_private_params"]["/slam_runtime_manager"]["repo_map_root"]

        report, _client, read_check_calls = self._build_report_with_topology(snapshot)

        topology_check = next(
            item for item in report["checks"]
            if item["name"] == "stage_k_ros_topology_identity"
        )
        self.assertFalse(topology_check["ok"])
        self.assertTrue(
            any(
                "node private parameter mismatch" in issue
                and "repo_map_root" in issue
                for issue in topology_check["issues"]
            )
        )
        self.assertEqual(read_check_calls, 0)

    def test_stage_k_topology_identity_fails_closed_on_task_manager_robot_id(self):
        snapshot = _valid_topology_snapshot()
        snapshot["node_robot_ids"]["/coverage_task_manager"] = "CR-999"

        report, _client, read_check_calls = self._build_report_with_topology(snapshot)

        topology_check = next(
            item for item in report["checks"]
            if item["name"] == "stage_k_ros_topology_identity"
        )
        self.assertFalse(topology_check["ok"])
        self.assertTrue(
            any("node robot_id mismatch" in issue for issue in topology_check["issues"])
        )
        self.assertEqual(read_check_calls, 0)
        self.assertEqual(report["actions"], [])
        self.assertFalse(report["summary"]["ok"])

    def test_stage_k_topology_identity_rejects_stale_mapping_session(self):
        snapshot = _valid_topology_snapshot()
        snapshot["mapping_session_id"] = "job-old-start-mapping"

        report, _client, read_check_calls = self._build_report_with_topology(snapshot)

        topology_check = next(
            item for item in report["checks"]
            if item["name"] == "stage_k_ros_topology_identity"
        )
        self.assertFalse(topology_check["ok"])
        self.assertTrue(
            any("mapping session token" in issue for issue in topology_check["issues"])
        )
        self.assertEqual(read_check_calls, 0)

    def test_stage_k_runtime_exception_still_captures_post_snapshot(self):
        args = _valid_stage_args()
        validate_args(args)
        client = _FailingWaitClient(_valid_topology_snapshot())
        snapshot_calls = []

        def commissioning_check(_args, name):
            snapshot_calls.append(name)
            return {
                "name": name,
                "ok": True,
                "issues": [],
                "response": {"snapshot": {"robot_id": ROBOT_ID, "stable": True}},
            }

        with mock.patch.object(
            runtime_smoke,
            "BackendRuntimeSmokeClient",
            return_value=client,
        ), mock.patch.object(
            runtime_smoke,
            "_commissioning_state_check",
            side_effect=commissioning_check,
        ):
            report = runtime_smoke.build_report(args)

        self.assertEqual(
            snapshot_calls,
            ["new_vehicle_state_pre", "new_vehicle_state_post"],
        )
        self.assertTrue(
            any(item["name"] == "new_vehicle_state_post" for item in report["checks"])
        )
        self.assertTrue(
            any(item["name"] == "stage_k_runtime_inspection" for item in report["checks"])
        )
        self.assertFalse(report["summary"]["ok"])

    def test_stage_profile_accepts_only_explicit_read_only_baseline_args(self):
        args = _valid_stage_args()

        validate_args(args)

        self.assertEqual(args.actions, [])
        self.assertEqual(args.ignore_warning, [])
        self.assertFalse(args.run_task_cycle)

    def test_stage_profile_rejects_action_task_cycle_and_warning_bypasses(self):
        cases = {
            "action": ["--actions", "start_mapping"],
            "task cycle": ["--run-task-cycle"],
            "warning bypass": ["--ignore-warning", "anything"],
            "nonzero task": ["--task-id", "7"],
            "default identity": ["--robot-id", "local_robot"],
            "map target": ["--map-name", "copied-map"],
            "write flag": ["--set-active"],
            "alternate database": ["--plan-db-path", "/tmp/empty-planning.db"],
        }
        for label, extra in cases.items():
            with self.subTest(label=label):
                args = _valid_stage_args(extra)
                with self.assertRaises(ValueError):
                    validate_args(args)

    def test_accepts_exact_new_vehicle_slam_and_required_readiness_warnings(self):
        slam = _check_stage_k_new_vehicle_slam(_valid_slam_response(), ROBOT_ID)
        readiness = _check_stage_k_new_vehicle_readiness(_valid_readiness_response(), "")

        self.assertTrue(slam["ok"], msg=slam["issues"])
        self.assertTrue(readiness["ok"], msg=readiness["issues"])

    def test_accepts_only_the_exact_optional_map_tf_health_warning(self):
        warnings = set(STAGE_K_REQUIRED_READINESS_WARNINGS)
        warnings.update(STAGE_K_OPTIONAL_READINESS_WARNINGS)

        result = _check_stage_k_new_vehicle_readiness(
            _valid_readiness_response(warnings=sorted(warnings)),
            "",
        )

        self.assertTrue(result["ok"], msg=result["issues"])

    def test_rejects_extra_slam_blocker_and_warning(self):
        blockers = [
            "runtime /map identity unavailable",
            "runtime localization not ready: state=- valid=false",
            "task manager busy",
        ]
        warnings = sorted(STAGE_K_SLAM_WARNINGS | {"slam submit backend unavailable"})

        blocker_result = _check_stage_k_new_vehicle_slam(
            _valid_slam_response(blockers=blockers),
            ROBOT_ID,
        )
        warning_result = _check_stage_k_new_vehicle_slam(
            _valid_slam_response(warnings=warnings),
            ROBOT_ID,
        )

        self.assertFalse(blocker_result["ok"])
        self.assertTrue(any("extra=" in issue for issue in blocker_result["issues"]))
        self.assertFalse(warning_result["ok"])
        self.assertTrue(any("extra=" in issue for issue in warning_result["issues"]))

    def test_rejects_mapping_desired_mode(self):
        response = _valid_slam_response()
        response.state.desired_mode = "mapping"

        result = _check_stage_k_new_vehicle_slam(response, ROBOT_ID)

        self.assertFalse(result["ok"])
        self.assertIn("desired_mode must be localization", result["issues"])

    def test_rejects_revision_write_capabilities_without_any_map(self):
        for capability in (
            "can_switch_map_and_localize",
            "can_verify_map_revision",
            "can_activate_map_revision",
        ):
            with self.subTest(capability=capability):
                response = _valid_slam_response()
                setattr(response.state, capability, True)

                result = _check_stage_k_new_vehicle_slam(response, ROBOT_ID)

                self.assertFalse(result["ok"])
                self.assertIn("%s must be false" % capability, result["issues"])

    def test_rejects_extra_or_missing_readiness_warning_and_blocker(self):
        extra_warning = sorted(STAGE_K_REQUIRED_READINESS_WARNINGS | {"station bridge disconnected"})
        missing_warning = sorted(STAGE_K_REQUIRED_READINESS_WARNINGS - {"battery_state missing"})
        extra_blocker = [
            "no current active map selected",
            "runtime /map identity unavailable",
            "runtime localization not ready: state=- valid=false",
            "odometry not ready",
        ]

        results = [
            _check_stage_k_new_vehicle_readiness(
                _valid_readiness_response(warnings=extra_warning), ""
            ),
            _check_stage_k_new_vehicle_readiness(
                _valid_readiness_response(warnings=missing_warning), ""
            ),
            _check_stage_k_new_vehicle_readiness(
                _valid_readiness_response(blockers=extra_blocker), ""
            ),
        ]

        for result in results:
            self.assertFalse(result["ok"], msg=result["issues"])

    def test_rejects_unknown_or_missing_subsystem_check(self):
        checks = _valid_readiness_checks()
        checks.pop()
        checks.append(
            _FakePayload(
                key="unreviewed_actuator",
                level="ERROR",
                ok=False,
                fresh=False,
                stale=False,
                missing=True,
                age_s=-1.0,
                summary="unsafe",
            )
        )

        result = _check_stage_k_new_vehicle_readiness(
            _valid_readiness_response(checks=checks),
            "",
        )

        self.assertFalse(result["ok"])
        self.assertTrue(any("readiness checks mismatch" in issue for issue in result["issues"]))

    def test_rejects_slam_and_odometry_identity_mismatch(self):
        slam = _check_stage_k_new_vehicle_slam(
            _valid_slam_response(robot_id="local_robot"),
            ROBOT_ID,
        )
        odometry = _check_odometry(
            _valid_odometry_response(robot_id="other-vehicle"),
            ignored_warnings=[],
            expected_robot_id=ROBOT_ID,
        )

        self.assertFalse(slam["ok"])
        self.assertTrue(any("robot_id mismatch" in issue for issue in slam["issues"]))
        self.assertFalse(odometry["ok"])
        self.assertTrue(any("robot_id mismatch" in issue for issue in odometry["issues"]))

    def test_run_read_checks_sends_explicit_identity_to_both_services(self):
        class FakeClient:
            def __init__(self):
                self.slam_robot_id = None
                self.odom_robot_id = None
                self.slam_refresh = None
                self.readiness_refresh = None

            def get_slam_status(self, robot_id, refresh_map_identity=True):
                self.slam_robot_id = robot_id
                self.slam_refresh = refresh_map_identity
                return _valid_slam_response(robot_id=robot_id)

            def get_odometry_status(self, robot_id):
                self.odom_robot_id = robot_id
                return _valid_odometry_response(robot_id=robot_id)

            def get_system_readiness(self, task_id, refresh_map_identity=True):
                self.task_id = task_id
                self.readiness_refresh = refresh_map_identity
                return _valid_readiness_response()

        client = FakeClient()

        checks = run_read_checks(
            client,
            task_id=0,
            ignored_warnings=[],
            robot_id=ROBOT_ID,
            profile=STAGE_K_NEW_VEHICLE_PROFILE,
        )

        self.assertEqual(client.slam_robot_id, ROBOT_ID)
        self.assertEqual(client.odom_robot_id, ROBOT_ID)
        self.assertEqual(client.task_id, 0)
        self.assertFalse(client.slam_refresh)
        self.assertFalse(client.readiness_refresh)
        self.assertTrue(all(item["ok"] for item in checks), msg=checks)


if __name__ == "__main__":
    unittest.main()

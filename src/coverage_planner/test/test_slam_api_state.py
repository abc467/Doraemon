#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import unittest
from unittest import mock


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
PKG_DIR = os.path.dirname(THIS_DIR)
SRC_DIR = os.path.join(PKG_DIR, "src")

if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from cleanrobot_app_msgs.msg import OdometryState, SlamJobState

from coverage_planner.ops_store.store import RobotRuntimeStateRecord
from coverage_planner.slam_workflow.api_state import SlamApiStateController


class _FakePlanStore:
    def __init__(self):
        self.pending_switch = None

    def get_active_map(self, *, robot_id: str):
        del robot_id
        return {
            "revision_id": "rev_demo_01",
            "map_name": "demo_map",
            "map_id": "map_1",
            "map_md5": "md5_1",
        }

    def get_pending_map_switch(self, *, robot_id: str):
        del robot_id
        return dict(self.pending_switch or {})

    def find_map_assets_by_identity(self, *, map_id: str, map_md5: str):
        del map_id, map_md5
        return [{"map_name": "demo_map"}]

    def resolve_map_revision(self, *, revision_id: str = "", map_name: str = "", robot_id: str = "local_robot"):
        del map_name, robot_id
        if str(revision_id or "") == "rev_demo_01":
            return {"revision_id": "rev_demo_01", "map_name": "demo_map"}
        return None


class _FakeOps:
    def get_robot_runtime_state(self, *, robot_id: str):
        return RobotRuntimeStateRecord(
            robot_id=robot_id,
            map_name="demo_map",
            localization_state="localized",
            localization_valid=True,
            mission_state="IDLE",
            phase="IDLE",
            public_state="IDLE",
            executor_state="IDLE",
        )


class _FakeRuntimeClient:
    def runtime_submit_job_available(self):
        return True

    def restart_backend_available(self):
        return True

    def mapping_runtime_available(self):
        return True

    def save_runtime_available(self):
        return True

    def service_available(self, *_args, **_kwargs):
        return True


class _FakeBackend:
    def __init__(self):
        self.robot_id = "local_robot"
        self.runtime_ns = "/cartographer/runtime"
        self._runtime_client = _FakeRuntimeClient()
        self._plan_store = _FakePlanStore()
        self._ops = _FakeOps()
        self._job_state_msg = None
        self._task_state_msg = None
        self._task_state_ts = 0.0
        self._odometry_state_msg = OdometryState()
        self._odometry_state_msg.validation_mode = "odom_stream"
        self._odometry_state_msg.odom_valid = True
        self._odometry_state_msg.connected = True
        self._odometry_state_msg.odom_stream_ready = True
        self._odometry_state_msg.frame_id_valid = True
        self._odometry_state_msg.child_frame_id_valid = True
        self._odometry_state_ts = time.time()
        self._map_ts = time.time()
        self._tracked_pose_ts = time.time()
        self.map_topic = "/map"
        self.map_identity_timeout_s = 2.0
        self.map_fresh_timeout_s = 10.0
        self.tracked_pose_fresh_timeout_s = 2.0
        self.odometry_status_service_name = "/clean_robot_server/get_odometry_status"
        self.task_state_fresh_timeout_s = 10.0
        self.odometry_state_fresh_timeout_s = 10.0
        self._runtime_state = self

    def runtime_param(self, key: str) -> str:
        return self.runtime_ns + "/" + str(key or "")

    def get_runtime_record(self, robot_id: str):
        return self._ops.get_robot_runtime_state(robot_id=robot_id)


class SlamApiStateControllerTest(unittest.TestCase):
    def setUp(self):
        self.backend = _FakeBackend()
        self.controller = SlamApiStateController(self.backend)

    @mock.patch("coverage_planner.slam_workflow.api_state.ensure_map_identity", return_value=("map_1", "md5_1", True))
    @mock.patch("coverage_planner.slam_workflow.api_state.get_runtime_map_scope", return_value=("demo_map", "robot"))
    @mock.patch("coverage_planner.slam_workflow.api_state.rospy.Time.now")
    @mock.patch("coverage_planner.slam_workflow.api_state.rospy.get_param")
    def test_build_state_projects_ready_localized_state(self, get_param, time_now, _scope, _identity):
        time_now.return_value = mock.Mock()
        get_param.side_effect = lambda key, default=None: {
            "/cartographer/runtime/mode": "localization",
            "/cartographer/runtime/current_mode": "localization",
            "/cartographer/runtime/localization_state": "localized",
            "/cartographer/runtime/localization_valid": True,
        }.get(key, default)

        msg = self.controller.build_state(robot_id="local_robot", refresh_map_identity=False)

        self.assertEqual(msg.workflow_state, "LOCALIZED")
        self.assertEqual(msg.workflow_phase, "ready")
        self.assertTrue(msg.task_ready)
        self.assertTrue(msg.can_start_mapping)
        self.assertTrue(msg.active_map_match)
        self.assertTrue(msg.can_verify_map_revision)
        self.assertTrue(msg.can_activate_map_revision)
        self.assertEqual(msg.runtime_map_name, "demo_map")
        self.assertEqual(msg.active_map_revision_id, "rev_demo_01")
        self.assertEqual(msg.runtime_map_revision_id, "")

    @mock.patch("coverage_planner.slam_workflow.api_state.ensure_map_identity", return_value=("map_1", "md5_1", True))
    @mock.patch("coverage_planner.slam_workflow.api_state.get_runtime_map_scope", return_value=("demo_map", "robot"))
    @mock.patch("coverage_planner.slam_workflow.api_state.rospy.Time.now")
    @mock.patch("coverage_planner.slam_workflow.api_state.rospy.get_param")
    def test_build_state_task_ready_false_when_odometry_invalid(self, get_param, time_now, _scope, _identity):
        time_now.return_value = mock.Mock()
        self.backend._odometry_state_msg.odom_valid = False
        self.backend._odometry_state_msg.error_code = "imu_stale"
        self.backend._odometry_state_msg.message = "imu stale"
        get_param.side_effect = lambda key, default=None: {
            "/cartographer/runtime/mode": "localization",
            "/cartographer/runtime/current_mode": "localization",
            "/cartographer/runtime/localization_state": "localized",
            "/cartographer/runtime/localization_valid": True,
        }.get(key, default)

        msg = self.controller.build_state(robot_id="local_robot", refresh_map_identity=False)

        self.assertFalse(msg.task_ready)
        self.assertIn("odometry not ready: code=imu_stale message=imu stale", list(msg.blocking_reasons))
        self.assertFalse(msg.can_switch_map_and_localize)
        self.assertFalse(msg.can_relocalize)
        self.assertFalse(msg.can_verify_map_revision)
        self.assertFalse(msg.can_activate_map_revision)
        self.assertFalse(msg.can_start_mapping)

    @mock.patch("coverage_planner.slam_workflow.api_state.ensure_map_identity", return_value=("map_1", "md5_1", True))
    @mock.patch("coverage_planner.slam_workflow.api_state.get_runtime_map_scope", return_value=("demo_map", "robot"))
    @mock.patch("coverage_planner.slam_workflow.api_state.rospy.Time.now")
    @mock.patch("coverage_planner.slam_workflow.api_state.rospy.get_param")
    def test_build_state_task_ready_false_when_task_manager_busy(self, get_param, time_now, _scope, _identity):
        time_now.return_value = mock.Mock()
        self.backend._task_state_msg = type(
            "TaskState",
            (),
            {
                "mission_state": "RUNNING",
                "phase": "IDLE",
                "public_state": "RUNNING",
                "executor_state": "RUNNING",
                "run_id": "run_1",
            },
        )()
        self.backend._task_state_ts = time.time()
        get_param.side_effect = lambda key, default=None: {
            "/cartographer/runtime/mode": "localization",
            "/cartographer/runtime/current_mode": "localization",
            "/cartographer/runtime/localization_state": "localized",
            "/cartographer/runtime/localization_valid": True,
        }.get(key, default)

        msg = self.controller.build_state(robot_id="local_robot", refresh_map_identity=False)

        self.assertTrue(msg.task_running)
        self.assertFalse(msg.task_ready)
        self.assertEqual(msg.blocking_reason, "task manager busy: mission=RUNNING phase=IDLE public=RUNNING")
        self.assertFalse(msg.can_switch_map_and_localize)
        self.assertFalse(msg.can_relocalize)
        self.assertFalse(msg.can_verify_map_revision)
        self.assertFalse(msg.can_activate_map_revision)
        self.assertFalse(msg.can_start_mapping)

    @mock.patch("coverage_planner.slam_workflow.api_state.ensure_map_identity", return_value=("map_1", "md5_1", True))
    @mock.patch("coverage_planner.slam_workflow.api_state.get_runtime_map_scope", return_value=("demo_map", "robot"))
    @mock.patch("coverage_planner.slam_workflow.api_state.rospy.Time.now")
    @mock.patch("coverage_planner.slam_workflow.api_state.rospy.get_param")
    def test_build_state_does_not_treat_executor_init_as_running_task(self, get_param, time_now, _scope, _identity):
        time_now.return_value = mock.Mock()
        self.backend._task_state_msg = type(
            "TaskState",
            (),
            {
                "mission_state": "IDLE",
                "phase": "IDLE",
                "public_state": "IDLE",
                "executor_state": "INIT",
                "run_id": "",
            },
        )()
        self.backend._task_state_ts = time.time()
        get_param.side_effect = lambda key, default=None: {
            "/cartographer/runtime/mode": "localization",
            "/cartographer/runtime/current_mode": "localization",
            "/cartographer/runtime/localization_state": "not_localized",
            "/cartographer/runtime/localization_valid": False,
        }.get(key, default)

        msg = self.controller.build_state(robot_id="local_robot", refresh_map_identity=False)

        self.assertFalse(msg.task_running)
        self.assertNotIn(
            "task manager busy: mission=IDLE phase=IDLE public=IDLE",
            list(msg.blocking_reasons),
        )
        self.assertTrue(msg.can_relocalize)
        self.assertTrue(msg.can_switch_map_and_localize)
        self.assertTrue(msg.can_verify_map_revision)
        self.assertTrue(msg.can_activate_map_revision)
        self.assertTrue(msg.can_start_mapping)

    @mock.patch("coverage_planner.slam_workflow.api_state.ensure_map_identity", return_value=("map_1", "md5_1", True))
    @mock.patch("coverage_planner.slam_workflow.api_state.get_runtime_map_scope", return_value=("demo_map", "robot"))
    @mock.patch("coverage_planner.slam_workflow.api_state.rospy.Time.now")
    @mock.patch("coverage_planner.slam_workflow.api_state.rospy.get_param")
    def test_build_state_does_not_treat_executor_done_as_running_task(self, get_param, time_now, _scope, _identity):
        time_now.return_value = mock.Mock()
        self.backend._task_state_msg = type(
            "TaskState",
            (),
            {
                "mission_state": "IDLE",
                "phase": "IDLE",
                "public_state": "IDLE",
                "executor_state": "DONE",
                "run_id": "",
            },
        )()
        self.backend._task_state_ts = time.time()
        get_param.side_effect = lambda key, default=None: {
            "/cartographer/runtime/mode": "localization",
            "/cartographer/runtime/current_mode": "localization",
            "/cartographer/runtime/localization_state": "not_localized",
            "/cartographer/runtime/localization_valid": False,
        }.get(key, default)

        msg = self.controller.build_state(robot_id="local_robot", refresh_map_identity=False)

        self.assertFalse(msg.task_running)
        self.assertNotIn(
            "task manager busy: mission=IDLE phase=IDLE public=IDLE",
            list(msg.blocking_reasons),
        )
        self.assertTrue(msg.can_relocalize)
        self.assertTrue(msg.can_switch_map_and_localize)
        self.assertTrue(msg.can_verify_map_revision)
        self.assertTrue(msg.can_activate_map_revision)
        self.assertTrue(msg.can_start_mapping)

    @mock.patch("coverage_planner.slam_workflow.api_state.ensure_map_identity", return_value=("map_1", "md5_1", True))
    @mock.patch("coverage_planner.slam_workflow.api_state.get_runtime_map_scope", return_value=("demo_map", "robot"))
    @mock.patch("coverage_planner.slam_workflow.api_state.rospy.Time.now")
    @mock.patch("coverage_planner.slam_workflow.api_state.rospy.get_param")
    def test_build_state_blocks_actions_when_public_submit_backend_unavailable(self, get_param, time_now, _scope, _identity):
        time_now.return_value = mock.Mock()
        self.backend._runtime_client.runtime_submit_job_available = lambda: False
        get_param.side_effect = lambda key, default=None: {
            "/cartographer/runtime/mode": "localization",
            "/cartographer/runtime/current_mode": "localization",
            "/cartographer/runtime/localization_state": "localized",
            "/cartographer/runtime/localization_valid": True,
        }.get(key, default)

        msg = self.controller.build_state(robot_id="local_robot", refresh_map_identity=False)

        self.assertTrue(msg.localization_backend_available)
        self.assertFalse(msg.can_switch_map_and_localize)
        self.assertFalse(msg.can_relocalize)
        self.assertFalse(msg.can_verify_map_revision)
        self.assertFalse(msg.can_activate_map_revision)
        self.assertFalse(msg.can_start_mapping)
        self.assertIn("slam submit backend unavailable", list(msg.warnings))

    @mock.patch("coverage_planner.slam_workflow.api_state.ensure_map_identity", return_value=("map_1", "md5_1", True))
    @mock.patch("coverage_planner.slam_workflow.api_state.get_runtime_map_scope", return_value=("demo_map", "robot"))
    @mock.patch("coverage_planner.slam_workflow.api_state.rospy.Time.now")
    @mock.patch("coverage_planner.slam_workflow.api_state.rospy.get_param")
    def test_handle_get_status_app_returns_cleanrobot_app_msgs_state(self, get_param, time_now, _scope, _identity):
        time_now.return_value = mock.Mock()
        get_param.side_effect = lambda key, default=None: {
            "/cartographer/runtime/mode": "localization",
            "/cartographer/runtime/current_mode": "localization",
            "/cartographer/runtime/localization_state": "localized",
            "/cartographer/runtime/localization_valid": True,
        }.get(key, default)

        resp = self.controller.handle_get_status_app(
            type("Req", (), {"robot_id": "local_robot", "refresh_map_identity": False})()
        )

        self.assertTrue(resp.success)
        self.assertEqual(resp.state.robot_id, "local_robot")
        self.assertEqual(resp.state.workflow_state, "LOCALIZED")

    @mock.patch("coverage_planner.slam_workflow.api_state.ensure_map_identity", return_value=("map_1", "md5_1", True))
    @mock.patch("coverage_planner.slam_workflow.api_state.get_runtime_map_scope", return_value=("demo_map", "robot"))
    @mock.patch("coverage_planner.slam_workflow.api_state.rospy.Time.now")
    @mock.patch("coverage_planner.slam_workflow.api_state.rospy.get_param")
    def test_build_state_blocks_switch_and_relocalize_when_localization_backend_unavailable(self, get_param, time_now, _scope, _identity):
        time_now.return_value = mock.Mock()
        self.backend._runtime_client.restart_backend_available = lambda: False
        get_param.side_effect = lambda key, default=None: {
            "/cartographer/runtime/mode": "localization",
            "/cartographer/runtime/current_mode": "localization",
            "/cartographer/runtime/localization_state": "localized",
            "/cartographer/runtime/localization_valid": True,
        }.get(key, default)

        msg = self.controller.build_state(robot_id="local_robot", refresh_map_identity=False)

        self.assertFalse(msg.can_switch_map_and_localize)
        self.assertFalse(msg.can_relocalize)
        self.assertFalse(msg.can_verify_map_revision)
        self.assertFalse(msg.can_activate_map_revision)
        self.assertIn("restart localization backend unavailable", list(msg.warnings))

    @mock.patch("coverage_planner.slam_workflow.api_state.ensure_map_identity", return_value=("map_1", "md5_1", True))
    @mock.patch("coverage_planner.slam_workflow.api_state.get_runtime_map_scope", return_value=("demo_map", "robot"))
    @mock.patch("coverage_planner.slam_workflow.api_state.rospy.Time.now")
    @mock.patch("coverage_planner.slam_workflow.api_state.rospy.get_param")
    def test_build_state_exposes_pending_map_switch_scope(self, get_param, time_now, _scope, _identity):
        time_now.return_value = mock.Mock()
        self.backend._plan_store.pending_switch = {
            "target_map_name": "candidate_map",
            "target_revision_id": "rev_candidate_01",
            "status": "verifying",
        }
        get_param.side_effect = lambda key, default=None: {
            "/cartographer/runtime/mode": "localization",
            "/cartographer/runtime/current_mode": "localization",
            "/cartographer/runtime/localization_state": "localized",
            "/cartographer/runtime/localization_valid": True,
        }.get(key, default)

        msg = self.controller.build_state(robot_id="local_robot", refresh_map_identity=False)

        self.assertEqual(msg.pending_map_name, "candidate_map")
        self.assertEqual(msg.pending_map_revision_id, "rev_candidate_01")
        self.assertEqual(msg.pending_map_switch_status, "verifying")

    @mock.patch("coverage_planner.slam_workflow.api_state.ensure_map_identity", return_value=("map_other", "md5_other", True))
    @mock.patch("coverage_planner.slam_workflow.api_state.get_runtime_map_scope", return_value=("other_map", "robot"))
    @mock.patch("coverage_planner.slam_workflow.api_state.rospy.Time.now")
    @mock.patch("coverage_planner.slam_workflow.api_state.rospy.get_param")
    def test_build_state_reports_detailed_runtime_map_mismatch_reason(self, get_param, time_now, _scope, _identity):
        time_now.return_value = mock.Mock()
        get_param.side_effect = lambda key, default=None: {
            "/cartographer/runtime/mode": "localization",
            "/cartographer/runtime/current_mode": "localization",
            "/cartographer/runtime/localization_state": "localized",
            "/cartographer/runtime/localization_valid": True,
        }.get(key, default)

        msg = self.controller.build_state(robot_id="local_robot", refresh_map_identity=False)

        self.assertIn("runtime map_name other_map != active map demo_map", list(msg.blocking_reasons))
        self.assertFalse(msg.task_ready)

    @mock.patch("coverage_planner.slam_workflow.api_state.ensure_map_identity", return_value=("map_other", "md5_other", True))
    @mock.patch("coverage_planner.slam_workflow.api_state.get_runtime_map_scope", return_value=("", "robot"))
    @mock.patch("coverage_planner.slam_workflow.api_state.get_runtime_map_revision_id", return_value="rev_demo_01")
    @mock.patch("coverage_planner.slam_workflow.api_state.rospy.Time.now")
    @mock.patch("coverage_planner.slam_workflow.api_state.rospy.get_param")
    def test_build_state_prefers_runtime_revision_match_over_hash_mismatch(
        self,
        get_param,
        time_now,
        _revision,
        _scope,
        _identity,
    ):
        time_now.return_value = mock.Mock()
        get_param.side_effect = lambda key, default=None: {
            "/cartographer/runtime/mode": "localization",
            "/cartographer/runtime/current_mode": "localization",
            "/cartographer/runtime/localization_state": "localized",
            "/cartographer/runtime/localization_valid": True,
        }.get(key, default)

        msg = self.controller.build_state(robot_id="local_robot", refresh_map_identity=False)

        self.assertTrue(msg.active_map_match)
        self.assertTrue(msg.task_ready)
        self.assertEqual(msg.runtime_map_name, "demo_map")
        self.assertEqual(msg.active_map_revision_id, "rev_demo_01")
        self.assertEqual(msg.runtime_map_revision_id, "rev_demo_01")

    @mock.patch("coverage_planner.slam_workflow.api_state.ensure_map_identity", return_value=("map_1", "md5_1", True))
    @mock.patch("coverage_planner.slam_workflow.api_state.get_runtime_map_scope", return_value=("demo_map", "robot"))
    @mock.patch("coverage_planner.slam_workflow.api_state.rospy.Time.now")
    @mock.patch("coverage_planner.slam_workflow.api_state.rospy.get_param")
    def test_build_state_reports_degraded_localization_phase(self, get_param, time_now, _scope, _identity):
        time_now.return_value = mock.Mock()
        get_param.side_effect = lambda key, default=None: {
            "/cartographer/runtime/mode": "localization",
            "/cartographer/runtime/current_mode": "localization",
            "/cartographer/runtime/localization_state": "degraded",
            "/cartographer/runtime/localization_valid": False,
        }.get(key, default)

        msg = self.controller.build_state(robot_id="local_robot", refresh_map_identity=False)

        self.assertEqual(msg.workflow_state, "DEGRADED")
        self.assertEqual(msg.workflow_phase, "degraded")
        self.assertFalse(msg.task_ready)
        self.assertFalse(msg.manual_assist_required)
        self.assertIn(
            "runtime localization not ready: state=degraded valid=false; relocalize before start/resume",
            list(msg.blocking_reasons),
        )

    @mock.patch("coverage_planner.slam_workflow.api_state.ensure_map_identity", return_value=("map_1", "md5_1", True))
    @mock.patch("coverage_planner.slam_workflow.api_state.get_runtime_map_scope", return_value=("demo_map", "robot"))
    @mock.patch("coverage_planner.slam_workflow.api_state.rospy.Time.now")
    @mock.patch("coverage_planner.slam_workflow.api_state.rospy.get_param")
    def test_build_state_reports_manual_assist_required_phase(self, get_param, time_now, _scope, _identity):
        time_now.return_value = mock.Mock()
        get_param.side_effect = lambda key, default=None: {
            "/cartographer/runtime/mode": "localization",
            "/cartographer/runtime/current_mode": "localization",
            "/cartographer/runtime/localization_state": "manual_assist_required",
            "/cartographer/runtime/localization_valid": False,
        }.get(key, default)

        msg = self.controller.build_state(robot_id="local_robot", refresh_map_identity=False)

        self.assertEqual(msg.workflow_state, "MANUAL_ASSIST_REQUIRED")
        self.assertEqual(msg.workflow_phase, "manual_assist_required")
        self.assertFalse(msg.task_ready)
        self.assertTrue(msg.manual_assist_required)
        self.assertEqual(msg.manual_assist_map_name, "demo_map")
        self.assertEqual(msg.manual_assist_map_revision_id, "rev_demo_01")
        self.assertEqual(msg.manual_assist_retry_action, "prepare_for_task")
        self.assertEqual(
            msg.manual_assist_guidance,
            "manual assist required for map=demo_map revision=rev_demo_01; "
            "place robot at a known anchor point, align heading, provide initial pose, then retry prepare_for_task",
        )
        self.assertIn(
            "runtime localization not ready: state=manual_assist_required valid=false; "
            "manual assist required for map=demo_map revision=rev_demo_01; "
            "place robot at a known anchor point, align heading, provide initial pose, then retry prepare_for_task",
            list(msg.blocking_reasons),
        )

    def test_active_job_state_ignores_finished_job(self):
        job = SlamJobState()
        job.job_id = "job_1"
        job.done = True
        self.backend._job_state_msg = job

        self.assertIsNone(self.controller.active_job_state())


if __name__ == "__main__":
    unittest.main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import threading
import unittest
from types import SimpleNamespace
from unittest import mock

from cleanrobot_app_msgs.srv import GetSlamJob as AppGetSlamJob, SubmitSlamCommand as AppSubmitSlamCommand
from coverage_planner.canonical_contract_types import (
    APP_GET_SLAM_JOB_SERVICE_TYPE,
    APP_SUBMIT_SLAM_COMMAND_SERVICE_TYPE,
)
from coverage_planner.manual_assist_pose import DEFAULT_MANUAL_ASSIST_POSE_PARAM_NS
from coverage_task_manager.task_manager import TaskManager


class TaskManagerPrepareForTaskRevisionScopeTest(unittest.TestCase):
    def _manager(self):
        mgr = TaskManager.__new__(TaskManager)
        mgr._lock = threading.Lock()
        mgr._robot_id = "local_robot"
        mgr._task_map_name = "demo_map"
        mgr._task_map_revision_id = "rev_requested_01"
        mgr._slam_submit_command_cli = None
        mgr._slam_get_job_cli = None
        mgr._restart_localization_cli = None
        mgr._slam_submit_command_service = "/clean_robot_server/submit_slam_command"
        mgr._app_slam_submit_command_service = "/clean_robot_server/app/submit_slam_command"
        mgr._slam_get_job_service = "/clean_robot_server/get_slam_job"
        mgr._app_slam_get_job_service = "/clean_robot_server/app/get_slam_job"
        mgr._app_restart_localization_service = "/cartographer/runtime/app/restart_localization"
        mgr._slam_submit_command_cli_service_name = ""
        mgr._slam_get_job_cli_service_name = ""
        mgr._restart_localization_cli_service_name = ""
        mgr._restart_localization_timeout_s = 2.0
        mgr._manual_assist_pose_param_ns = DEFAULT_MANUAL_ASSIST_POSE_PARAM_NS
        mgr._repeat_cycle_context = lambda: ""
        mgr._get_selected_active_map = lambda: {
            "map_name": "demo_map",
            "revision_id": "rev_active_99",
        }
        return mgr

    @mock.patch("coverage_task_manager.task_manager.rospy.sleep", side_effect=lambda *_args, **_kwargs: None)
    @mock.patch("coverage_task_manager.task_manager.rospy.is_shutdown", return_value=False)
    @mock.patch("coverage_task_manager.task_manager.rospy.wait_for_service", side_effect=lambda *_args, **_kwargs: None)
    @mock.patch("coverage_task_manager.task_manager.time.time", return_value=100.0)
    def test_prepare_for_task_submit_flow_prefers_resolved_revision_id(
        self,
        _time_now,
        _wait_for_service,
        _is_shutdown,
        _sleep,
    ):
        mgr = self._manager()
        mgr._slam_submit_command_cli = lambda **_kwargs: SimpleNamespace(accepted=True, job_id="job_1")
        mgr._slam_get_job_cli = lambda **_kwargs: SimpleNamespace(
            found=True,
            message="ok",
            job=SimpleNamespace(
                done=True,
                success=True,
                message="localized",
                progress_text="localized",
                resolved_map_name="demo_map",
                requested_map_name="demo_map",
                resolved_map_revision_id="rev_resolved_02",
                requested_map_revision_id="rev_requested_01",
            ),
        )

        ok, msg = mgr._restart_localization_for_task()

        self.assertTrue(ok)
        self.assertEqual(msg, "localized")
        self.assertEqual(mgr._task_map_name, "demo_map")
        self.assertEqual(mgr._task_map_revision_id, "rev_resolved_02")

    @mock.patch("coverage_task_manager.task_manager.rospy.wait_for_service", side_effect=lambda *_args, **_kwargs: None)
    def test_prepare_for_task_restart_fallback_prefers_response_revision_id(self, _wait_for_service):
        mgr = self._manager()
        mgr._slam_submit_command_cli = lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("submit down"))
        mgr._slam_get_job_cli = lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("get_job down"))
        mgr._restart_localization_cli = lambda **_kwargs: SimpleNamespace(
            success=True,
            message="localized",
            map_name="demo_map",
            map_revision_id="rev_restart_03",
            localization_state="localized",
        )

        ok, msg = mgr._restart_localization_for_task()

        self.assertTrue(ok)
        self.assertEqual(msg, "localized")
        self.assertEqual(mgr._task_map_name, "demo_map")
        self.assertEqual(mgr._task_map_revision_id, "rev_restart_03")

    @mock.patch("coverage_task_manager.task_manager.rospy.set_param")
    @mock.patch("coverage_task_manager.task_manager.rospy.get_param")
    @mock.patch("coverage_task_manager.task_manager.rospy.sleep", side_effect=lambda *_args, **_kwargs: None)
    @mock.patch("coverage_task_manager.task_manager.rospy.is_shutdown", return_value=False)
    @mock.patch("coverage_task_manager.task_manager.rospy.wait_for_service", side_effect=lambda *_args, **_kwargs: None)
    @mock.patch("coverage_task_manager.task_manager.time.time", side_effect=[100.0, 100.1, 100.2, 100.3, 100.4, 100.5, 100.6, 100.7])
    def test_prepare_for_task_submit_flow_uses_manual_assist_pose_override(
        self,
        _time_now,
        _wait_for_service,
        _is_shutdown,
        _sleep,
        get_param,
        set_param,
    ):
        mgr = self._manager()
        submit_calls = []
        mgr._slam_submit_command_cli = lambda **kwargs: submit_calls.append(dict(kwargs)) or SimpleNamespace(
            accepted=True,
            job_id="job_1",
        )
        mgr._slam_get_job_cli = lambda **_kwargs: SimpleNamespace(
            found=True,
            message="ok",
            job=SimpleNamespace(
                done=True,
                success=True,
                message="localized",
                progress_text="localized",
                resolved_map_name="demo_map",
                requested_map_name="demo_map",
                resolved_map_revision_id="rev_resolved_02",
                requested_map_revision_id="rev_requested_01",
            ),
        )
        get_param.side_effect = lambda key, default=None: {
            DEFAULT_MANUAL_ASSIST_POSE_PARAM_NS: {
                "enabled": True,
                "consume_once": True,
                "map_name": "demo_map",
                "map_revision_id": "rev_requested_01",
                "frame_id": "map",
                "x": 1.2,
                "y": 3.4,
                "yaw": 0.5,
            }
        }.get(key, default)

        ok, msg = mgr._restart_localization_for_task()

        self.assertTrue(ok)
        self.assertEqual(msg, "localized")
        self.assertTrue(submit_calls[0]["has_initial_pose"])
        self.assertEqual(submit_calls[0]["initial_pose_x"], 1.2)
        self.assertEqual(submit_calls[0]["initial_pose_y"], 3.4)
        self.assertEqual(submit_calls[0]["initial_pose_yaw"], 0.5)
        self.assertGreaterEqual(set_param.call_count, 2)
        self.assertEqual(set_param.call_args_list[0][0][0], DEFAULT_MANUAL_ASSIST_POSE_PARAM_NS)
        self.assertFalse(set_param.call_args_list[0][0][1]["enabled"])
        self.assertEqual(set_param.call_args_list[-1][0][1]["last_status"], "prepare_for_task_succeeded")
        self.assertEqual(set_param.call_args_list[-1][0][1]["last_job_id"], "job_1")
        self.assertTrue(set_param.call_args_list[-1][0][1]["last_used"])

    @mock.patch("coverage_task_manager.task_manager.rospy.set_param")
    @mock.patch("coverage_task_manager.task_manager.rospy.get_param")
    def test_prepare_for_task_submit_flow_rejects_scope_mismatched_manual_assist_pose(
        self,
        get_param,
        set_param,
    ):
        mgr = self._manager()
        mgr._slam_submit_command_cli = lambda **_kwargs: (_ for _ in ()).throw(AssertionError("submit should not run"))
        mgr._slam_get_job_cli = lambda **_kwargs: (_ for _ in ()).throw(AssertionError("get_job should not run"))
        get_param.side_effect = lambda key, default=None: {
            DEFAULT_MANUAL_ASSIST_POSE_PARAM_NS: {
                "enabled": True,
                "consume_once": True,
                "map_name": "demo_map",
                "map_revision_id": "rev_other_99",
                "frame_id": "map",
                "x": 1.2,
                "y": 3.4,
                "yaw": 0.5,
            }
        }.get(key, default)

        ok, msg = mgr._restart_localization_for_task()

        self.assertFalse(ok)
        self.assertEqual(
            msg,
            "manual assist pose override revision mismatch: requested=rev_requested_01 override=rev_other_99",
        )
        self.assertEqual(set_param.call_count, 1)
        self.assertEqual(set_param.call_args_list[0][0][0], DEFAULT_MANUAL_ASSIST_POSE_PARAM_NS)
        self.assertEqual(
            set_param.call_args_list[0][0][1]["last_status"],
            "prepare_for_task_pose_scope_mismatch",
        )
        self.assertFalse(set_param.call_args_list[0][0][1]["last_used"])

    @mock.patch("coverage_task_manager.task_manager.rosservice.get_service_type")
    @mock.patch("coverage_task_manager.task_manager.rospy.ServiceProxy")
    @mock.patch("coverage_task_manager.task_manager.rospy.sleep", side_effect=lambda *_args, **_kwargs: None)
    @mock.patch("coverage_task_manager.task_manager.rospy.is_shutdown", return_value=False)
    @mock.patch("coverage_task_manager.task_manager.rospy.wait_for_service", side_effect=lambda *_args, **_kwargs: None)
    @mock.patch("coverage_task_manager.task_manager.time.time", return_value=100.0)
    def test_prepare_for_task_submit_flow_prefers_app_service_endpoints_when_available(
        self,
        _time_now,
        _wait_for_service,
        _is_shutdown,
        _sleep,
        service_proxy,
        get_service_type,
    ):
        mgr = self._manager()
        get_service_type.side_effect = lambda name: {
            "/clean_robot_server/app/submit_slam_command": APP_SUBMIT_SLAM_COMMAND_SERVICE_TYPE,
            "/clean_robot_server/app/get_slam_job": APP_GET_SLAM_JOB_SERVICE_TYPE,
        }.get(name, "")

        def _service_proxy(service_name, service_cls):
            if service_name == "/clean_robot_server/app/submit_slam_command":
                self.assertIs(service_cls, AppSubmitSlamCommand)
                return lambda **_kwargs: SimpleNamespace(accepted=True, job_id="job_1")
            if service_name == "/clean_robot_server/app/get_slam_job":
                self.assertIs(service_cls, AppGetSlamJob)
                return lambda **_kwargs: SimpleNamespace(
                    found=True,
                    message="ok",
                    job=SimpleNamespace(
                        done=True,
                        success=True,
                        message="localized",
                        progress_text="localized",
                        resolved_map_name="demo_map",
                        requested_map_name="demo_map",
                        resolved_map_revision_id="rev_resolved_02",
                        requested_map_revision_id="rev_requested_01",
                    ),
                )
            raise AssertionError("unexpected proxy service=%s" % service_name)

        service_proxy.side_effect = _service_proxy

        ok, msg = mgr._restart_localization_for_task()

        self.assertTrue(ok)
        self.assertEqual(msg, "localized")
        self.assertEqual(
            [call[0][0] for call in service_proxy.call_args_list[:2]],
            ["/clean_robot_server/app/submit_slam_command", "/clean_robot_server/app/get_slam_job"],
        )

if __name__ == "__main__":
    unittest.main()

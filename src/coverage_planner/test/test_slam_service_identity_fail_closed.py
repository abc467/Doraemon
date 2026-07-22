#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import importlib.util
import pathlib
import threading
import unittest
from types import SimpleNamespace
from unittest import mock

from cleanrobot_app_msgs.msg import SlamJobState
from cleanrobot_app_msgs.srv import OperateSlamRuntime, SubmitSlamCommand

from coverage_planner.slam_workflow.api_submit import SlamApiSubmitController
from coverage_planner.slam_workflow.api import RUNTIME_STOP_MAPPING
from coverage_planner.slam_workflow.service_api import SlamRuntimeServiceController


PACKAGE_DIR = pathlib.Path(__file__).resolve().parents[1]
LOCALIZATION_SCRIPT = PACKAGE_DIR / "scripts" / "localization_lifecycle_manager_node.py"


def _load_localization_module():
    spec = importlib.util.spec_from_file_location(
        "slam_service_identity_localization_test_mod",
        str(LOCALIZATION_SCRIPT),
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


LOCALIZATION_MODULE = _load_localization_module()


def _submit_request(*, robot_id="CR-999"):
    request = SubmitSlamCommand._request_class()
    request.operation = int(request.start_mapping)
    request.robot_id = robot_id
    request.map_name = ""
    request.map_revision_id = ""
    request.set_active = False
    request.description = "identity-guard-test"
    request.frame_id = "map"
    request.has_initial_pose = False
    request.save_map_name = ""
    request.include_unfinished_submaps = False
    request.set_active_on_save = False
    request.switch_to_localization_after_save = False
    request.relocalize_after_switch = False
    return request


class SlamServiceIdentityFailClosedTest(unittest.TestCase):
    def test_public_submit_rejects_foreign_robot_before_state_or_runtime_calls(self):
        runtime_client = mock.Mock()
        state_controller = mock.Mock()
        state_controller.build_state.side_effect = AssertionError(
            "foreign robot_id reached public submit state construction"
        )
        backend = SimpleNamespace(
            robot_id="CR-001",
            _runtime_client=runtime_client,
            _state_controller=state_controller,
            _runtime_assets=mock.Mock(),
            _runtime_state=mock.Mock(),
        )

        response = SlamApiSubmitController(backend).handle_submit_command_app(
            _submit_request()
        )

        self.assertFalse(response.accepted)
        self.assertIn("robot_id", "%s %s" % (response.error_code, response.message))
        state_controller.build_state.assert_not_called()
        runtime_client.call_runtime_submit_job.assert_not_called()

    def test_public_get_job_rejects_foreign_robot_before_runtime_or_cache_lookup(self):
        runtime_client = mock.Mock()
        backend = SimpleNamespace(
            robot_id="CR-001",
            _runtime_client=runtime_client,
            _job_state_msg=SlamJobState(job_id="job-local", robot_id="CR-001"),
        )

        response = SlamApiSubmitController(backend).handle_get_job_app(
            SimpleNamespace(job_id="job-local", robot_id="CR-999")
        )

        self.assertFalse(response.found)
        self.assertIn("robot_id", "%s %s" % (response.error_code, response.message))
        runtime_client.runtime_get_job_available.assert_not_called()
        runtime_client.call_runtime_get_job.assert_not_called()

    def test_runtime_submit_rejects_foreign_robot_before_job_creation(self):
        job_state = mock.Mock()
        job_state.job_to_msg.return_value = None
        backend = SimpleNamespace(
            robot_id="CR-001",
            _job_state=job_state,
            _asset_helper=mock.Mock(),
        )

        response = SlamRuntimeServiceController(backend).handle_submit_job_app(
            _submit_request()
        )

        self.assertFalse(response.accepted)
        self.assertIn("robot_id", "%s %s" % (response.error_code, response.message))
        job_state.job_running.assert_not_called()
        job_state.make_job_record.assert_not_called()
        job_state.publish_job_snapshot.assert_not_called()

    def test_runtime_operate_rejects_foreign_robot_before_operation_or_state_write(self):
        job_state = mock.Mock()
        job_state.job_running.return_value = False
        runtime_state = mock.Mock()
        backend = SimpleNamespace(
            robot_id="CR-001",
            _job_state=job_state,
            _asset_helper=mock.Mock(),
            _runtime_state=runtime_state,
            _lock=threading.Lock(),
        )
        controller = SlamRuntimeServiceController(backend)
        controller.execute_operation = mock.Mock(
            side_effect=AssertionError("foreign robot_id reached runtime operation")
        )
        request = OperateSlamRuntime._request_class()
        request.operation = int(request.start_mapping)
        request.robot_id = "CR-999"
        request.map_name = ""
        request.map_revision_id = ""
        request.set_active = False
        request.description = "identity-guard-test"

        response = controller.handle_operate_app(request)

        self.assertFalse(response.success)
        self.assertIn("robot_id", "%s %s" % (response.error_code, response.message))
        controller.execute_operation.assert_not_called()
        runtime_state.update_runtime_state.assert_not_called()

    def test_runtime_operate_rejects_synchronous_write_operations(self):
        for operation_name in (
            "start_mapping",
            "save_mapping",
            "stop_mapping",
            "verify_map_revision",
            "activate_map_revision",
        ):
            with self.subTest(operation_name=operation_name):
                job_state = mock.Mock()
                job_state.job_running.return_value = False
                runtime_state = mock.Mock()
                backend = SimpleNamespace(
                    robot_id="CR-001",
                    _job_state=job_state,
                    _asset_helper=mock.Mock(),
                    _runtime_state=runtime_state,
                    _lock=threading.Lock(),
                )
                controller = SlamRuntimeServiceController(backend)
                controller.execute_operation = mock.Mock(
                    side_effect=AssertionError("sync mapping reached runtime operation")
                )
                request = OperateSlamRuntime._request_class()
                request.operation = int(getattr(request, operation_name))
                request.robot_id = "CR-001"
                request.map_name = ""
                request.map_revision_id = ""
                request.set_active = False
                request.description = "sync-mapping-guard-test"

                response = controller.handle_operate_app(request)

                self.assertFalse(response.success)
                self.assertEqual(response.error_code, "async_slam_workflow_required")
                controller.execute_operation.assert_not_called()
                job_state.job_running.assert_not_called()
                runtime_state.update_runtime_state.assert_not_called()

    def test_async_runtime_stop_clears_map_scope_before_adapter_call(self):
        runtime_adapter = mock.Mock()
        runtime_adapter.stop_mapping.return_value = SimpleNamespace(success=True)
        backend = SimpleNamespace(
            robot_id="CR-001",
            _runtime_adapter=runtime_adapter,
            _workflow_executor=mock.Mock(),
            _asset_helper=mock.Mock(),
        )

        response = SlamRuntimeServiceController(backend).execute_operation(
            operation=RUNTIME_STOP_MAPPING,
            robot_id="CR-001",
            map_name="must_not_survive",
            map_revision_id="rev_must_not_survive",
            set_active=False,
            description="async stop",
        )

        self.assertTrue(response.success)
        runtime_adapter.stop_mapping.assert_called_once_with(
            robot_id="CR-001",
            map_name="",
            map_revision_id="",
            operation=RUNTIME_STOP_MAPPING,
        )

    def test_runtime_get_job_rejects_foreign_robot_before_snapshot_lookup(self):
        job_state = mock.Mock()
        job_state.job_to_msg.return_value = None
        backend = SimpleNamespace(robot_id="CR-001", _job_state=job_state)

        response = SlamRuntimeServiceController(backend).handle_get_job_app(
            SimpleNamespace(job_id="job-local", robot_id="CR-999")
        )

        self.assertFalse(response.found)
        self.assertIn("robot_id", "%s %s" % (response.error_code, response.message))
        job_state.get_job_snapshot.assert_not_called()

    def test_localization_restart_rejects_foreign_robot_before_delegate(self):
        node = LOCALIZATION_MODULE.LocalizationLifecycleManagerNode.__new__(
            LOCALIZATION_MODULE.LocalizationLifecycleManagerNode
        )
        node.robot_id = "CR-001"
        node._localization_transition_lock = threading.Lock()
        node._service_lock = threading.Lock()
        node._localization_transition_epoch = 0
        node._delegate_restart_to_runtime_manager = mock.Mock(
            return_value=node._restart_response(
                success=False,
                message="unexpected delegate",
                map_name="",
                map_revision_id="",
                localization_state="not_localized",
            )
        )

        response = node._handle_restart(
            SimpleNamespace(
                robot_id="CR-999",
                map_name="demo_map",
                map_revision_id="rev_demo_01",
            )
        )

        self.assertFalse(response.success)
        self.assertIn("robot_id", response.message)
        node._delegate_restart_to_runtime_manager.assert_not_called()


if __name__ == "__main__":
    unittest.main()

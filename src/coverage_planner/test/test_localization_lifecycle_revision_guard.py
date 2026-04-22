#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import importlib.util
import pathlib
import unittest
from types import SimpleNamespace
from unittest import mock


def _load_module():
    script_path = pathlib.Path(__file__).resolve().parents[1] / "scripts" / "localization_lifecycle_manager_node.py"
    spec = importlib.util.spec_from_file_location("localization_lifecycle_guard_test_mod", str(script_path))
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


LOCALIZATION_MODULE = _load_module()


class _FakeStore:
    def __init__(self, asset, active_asset=None, preferred_revision=None):
        self._asset = dict(asset)
        self._active_asset = dict(active_asset or {})
        self._preferred_revision = dict(preferred_revision or asset)
        self.resolve_calls = []
        self.resolve_revision_calls = []

    def resolve_map_asset(self, **kwargs):
        self.resolve_calls.append(dict(kwargs))
        revision_id = str(kwargs.get("revision_id") or "").strip()
        map_name = str(kwargs.get("map_name") or "").strip()
        if revision_id and revision_id == str(self._active_asset.get("revision_id") or ""):
            return dict(self._active_asset)
        if revision_id and revision_id == str(self._asset.get("revision_id") or ""):
            return dict(self._asset)
        if map_name and map_name == str(self._asset.get("map_name") or ""):
            return dict(self._asset)
        return dict(self._asset)

    def get_active_map(self, *, robot_id: str):
        del robot_id
        return dict(self._active_asset or {})

    def resolve_map_revision(self, **kwargs):
        self.resolve_revision_calls.append(dict(kwargs))
        revision_id = str(kwargs.get("revision_id") or "").strip()
        map_name = str(kwargs.get("map_name") or "").strip()
        if revision_id and revision_id == str(self._active_asset.get("revision_id") or ""):
            return dict(self._active_asset)
        if revision_id and revision_id == str(self._preferred_revision.get("revision_id") or ""):
            return dict(self._preferred_revision)
        if revision_id and revision_id == str(self._asset.get("revision_id") or ""):
            return dict(self._asset)
        if map_name and map_name == str(self._asset.get("map_name") or ""):
            if str(self._active_asset.get("map_name") or "") == map_name:
                return dict(self._active_asset)
            return dict(self._preferred_revision)
        return {}


class LocalizationLifecycleRevisionGuardTest(unittest.TestCase):
    def test_resolve_asset_accepts_revision_only(self):
        node = LOCALIZATION_MODULE.LocalizationLifecycleManagerNode.__new__(LOCALIZATION_MODULE.LocalizationLifecycleManagerNode)
        node._plan_store = _FakeStore(
            {
                "map_name": "demo_map",
                "revision_id": "rev_demo_01",
                "enabled": True,
            }
        )

        asset = node._resolve_asset(robot_id="robot_a", map_name="", map_revision_id="rev_demo_01")

        self.assertEqual(str(asset.get("revision_id") or ""), "rev_demo_01")
        self.assertEqual(
            node._plan_store.resolve_calls[-1],
            {
                "revision_id": "rev_demo_01",
                "robot_id": "robot_a",
            },
        )

    def test_resolve_asset_rejects_map_revision_name_mismatch(self):
        node = LOCALIZATION_MODULE.LocalizationLifecycleManagerNode.__new__(LOCALIZATION_MODULE.LocalizationLifecycleManagerNode)
        node._plan_store = _FakeStore(
            {
                "map_name": "demo_map",
                "revision_id": "rev_demo_01",
                "enabled": True,
            }
        )

        with self.assertRaisesRegex(ValueError, "map revision does not match selected map"):
            node._resolve_asset(robot_id="robot_a", map_name="other_map", map_revision_id="rev_demo_01")

    def test_resolve_asset_prefers_active_revision_for_same_name(self):
        node = LOCALIZATION_MODULE.LocalizationLifecycleManagerNode.__new__(LOCALIZATION_MODULE.LocalizationLifecycleManagerNode)
        node._plan_store = _FakeStore(
            {
                "map_name": "demo_map",
                "revision_id": "rev_demo_head",
                "verification_status": "pending",
            },
            active_asset={
                "map_name": "demo_map",
                "revision_id": "rev_demo_active",
                "verification_status": "verified",
            },
        )

        asset = node._resolve_asset(robot_id="robot_a", map_name="demo_map", map_revision_id="")

        self.assertEqual(str(asset.get("revision_id") or ""), "rev_demo_active")

    def test_resolve_asset_prefers_verified_revision_scope_over_pending_head(self):
        node = LOCALIZATION_MODULE.LocalizationLifecycleManagerNode.__new__(LOCALIZATION_MODULE.LocalizationLifecycleManagerNode)
        node._plan_store = _FakeStore(
            {
                "map_name": "demo_map",
                "revision_id": "rev_demo_head",
                "verification_status": "pending",
            },
            active_asset={
                "map_name": "other_map",
                "revision_id": "rev_other_active",
                "verification_status": "verified",
            },
            preferred_revision={
                "map_name": "demo_map",
                "revision_id": "rev_demo_verified",
                "verification_status": "verified",
            },
        )

        asset = node._resolve_asset(robot_id="robot_a", map_name="demo_map", map_revision_id="")

        self.assertEqual(str(asset.get("revision_id") or ""), "rev_demo_verified")
        self.assertEqual(
            node._plan_store.resolve_revision_calls[-1],
            {
                "map_name": "demo_map",
                "robot_id": "robot_a",
            },
        )

    @mock.patch.object(LOCALIZATION_MODULE.rospy, "get_param")
    def test_runtime_map_matches_prefers_revision_over_md5_mismatch(self, get_param):
        node = LOCALIZATION_MODULE.LocalizationLifecycleManagerNode.__new__(LOCALIZATION_MODULE.LocalizationLifecycleManagerNode)
        node.runtime_ns = "/cartographer/runtime"
        node._map_md5 = "runtime_md5_other"
        get_param.side_effect = lambda key, default=None: {
            "/cartographer/runtime/current_map_revision_id": "rev_demo_01",
        }.get(key, default)

        matched = node._runtime_map_matches(
            target_md5="requested_md5",
            target_revision_id="rev_demo_01",
        )

        self.assertTrue(matched)

    @mock.patch.object(LOCALIZATION_MODULE.time, "time")
    @mock.patch.object(LOCALIZATION_MODULE.rospy, "sleep", side_effect=lambda *_args, **_kwargs: None)
    @mock.patch.object(LOCALIZATION_MODULE.rospy, "is_shutdown", return_value=False)
    @mock.patch.object(LOCALIZATION_MODULE.rospy, "get_param")
    def test_wait_until_ready_prefers_runtime_revision_match_over_md5_mismatch(
        self,
        get_param,
        _is_shutdown,
        _sleep,
        time_now,
    ):
        node = LOCALIZATION_MODULE.LocalizationLifecycleManagerNode.__new__(LOCALIZATION_MODULE.LocalizationLifecycleManagerNode)
        node.runtime_ns = "/cartographer/runtime"
        node.ready_timeout_s = 5.0
        node.tf_parent_frame = "map"
        node.tf_child_frame = "odom"
        node.tf_poll_timeout_s = 0.2
        node.tracked_pose_fresh_timeout_s = 2.0
        node.allow_identity_rebind_on_localize = False
        node._tracked_pose_ts = 100.0
        node._map_ts = 100.0
        node._map_md5 = "runtime_md5_other"
        node._tf_buffer = type("Tf", (), {"can_transform": staticmethod(lambda *_args, **_kwargs: True)})()
        node._plan_store = type(
            "Store",
            (),
            {
                "get_active_map": staticmethod(lambda **_kwargs: {"map_name": "demo_map", "revision_id": "rev_demo_01"}),
                "get_pending_map_switch": staticmethod(lambda **_kwargs: {}),
            },
        )()
        node._set_localization_state_calls = []
        node._set_localization_state = lambda **kwargs: node._set_localization_state_calls.append(dict(kwargs))
        time_now.side_effect = [100.0, 100.0, 100.0]
        get_param.side_effect = lambda key, default=None: {
            "/cartographer/runtime/current_map_revision_id": "rev_demo_01",
        }.get(key, default)

        ok, message = node._wait_until_ready(
            robot_id="robot_a",
            asset={
                "map_name": "demo_map",
                "revision_id": "rev_demo_01",
                "map_md5": "requested_md5",
            },
            started_after_ts=99.0,
        )

        self.assertTrue(ok)
        self.assertEqual(message, "localized")
        self.assertEqual(node._set_localization_state_calls[-1]["map_revision_id"], "rev_demo_01")
        self.assertTrue(node._set_localization_state_calls[-1]["valid"])

    @mock.patch.object(LOCALIZATION_MODULE.time, "time", side_effect=[100.0, 100.1, 100.2, 100.3, 100.4, 100.5, 100.6, 100.7])
    @mock.patch.object(LOCALIZATION_MODULE.rospy, "sleep", side_effect=lambda *_args, **_kwargs: None)
    @mock.patch.object(LOCALIZATION_MODULE.rospy, "is_shutdown", return_value=False)
    @mock.patch.object(LOCALIZATION_MODULE.rospy, "set_param")
    @mock.patch.object(LOCALIZATION_MODULE.rospy, "get_param")
    @mock.patch.object(LOCALIZATION_MODULE.rospy, "wait_for_service", side_effect=lambda *_args, **_kwargs: None)
    @mock.patch.object(LOCALIZATION_MODULE.rospy, "ServiceProxy")
    def test_delegate_restart_prefers_runtime_submit_job_with_manual_assist_pose(
        self,
        service_proxy,
        _wait_for_service,
        get_param,
        set_param,
        _is_shutdown,
        _sleep,
        _time_now,
    ):
        node = LOCALIZATION_MODULE.LocalizationLifecycleManagerNode.__new__(LOCALIZATION_MODULE.LocalizationLifecycleManagerNode)
        node.robot_id = "local_robot"
        node.runtime_ns = "/cartographer/runtime"
        node.runtime_submit_job_service = "/cartographer/runtime/submit_job"
        node.runtime_get_job_service = "/cartographer/runtime/get_job"
        node.runtime_manager_service = "/cartographer/runtime/operate"
        node.app_runtime_submit_job_service = "/cartographer/runtime/app/submit_job"
        node.app_runtime_get_job_service = "/cartographer/runtime/app/get_job"
        node.app_runtime_manager_service = "/cartographer/runtime/app/operate"
        node.runtime_job_timeout_s = 2.0
        node.manual_assist_pose_param_ns = "/localization/manual_assist_pose"

        submit_calls = []
        get_job_calls = []

        def _service_proxy(name, _srv_type):
            if name == node.app_runtime_submit_job_service:
                return lambda **kwargs: submit_calls.append(dict(kwargs)) or SimpleNamespace(
                    accepted=True,
                    job_id="job_1",
                    map_name="demo_map",
                    job=SimpleNamespace(localization_state="localizing"),
                )
            if name == node.app_runtime_get_job_service:
                return lambda **kwargs: get_job_calls.append(dict(kwargs)) or SimpleNamespace(
                    found=True,
                    message="ok",
                    job=SimpleNamespace(
                        done=True,
                        success=True,
                        message="localized",
                        progress_text="localized",
                        resolved_map_name="demo_map",
                        requested_map_name="demo_map",
                        resolved_map_revision_id="rev_demo_01",
                        requested_map_revision_id="rev_demo_01",
                        localization_state="localized",
                    ),
                )
            raise AssertionError("unexpected service %s" % name)

        service_proxy.side_effect = _service_proxy
        get_param.side_effect = lambda key, default=None: {
            "/localization/manual_assist_pose": {
                "enabled": True,
                "consume_once": True,
                "map_name": "demo_map",
                "map_revision_id": "rev_demo_01",
                "frame_id": "map",
                "x": 1.0,
                "y": 2.0,
                "yaw": 0.3,
            }
        }.get(key, default)

        resp = node._delegate_restart_to_runtime_manager(
            robot_id="local_robot",
            map_name="demo_map",
            map_revision_id="rev_demo_01",
        )

        self.assertTrue(resp.success)
        self.assertEqual(resp.map_name, "demo_map")
        self.assertEqual(resp.map_revision_id, "rev_demo_01")
        self.assertEqual(resp.localization_state, "localized")
        self.assertTrue(submit_calls[0]["has_initial_pose"])
        self.assertEqual(submit_calls[0]["initial_pose_x"], 1.0)
        self.assertEqual(submit_calls[0]["initial_pose_y"], 2.0)
        self.assertEqual(submit_calls[0]["initial_pose_yaw"], 0.3)
        self.assertEqual(get_job_calls[0]["job_id"], "job_1")
        self.assertEqual(service_proxy.call_args_list[0][0][0], node.app_runtime_submit_job_service)
        self.assertEqual(service_proxy.call_args_list[1][0][0], node.app_runtime_get_job_service)
        self.assertGreaterEqual(set_param.call_count, 2)
        self.assertEqual(set_param.call_args_list[0][0][0], "/localization/manual_assist_pose")
        self.assertFalse(set_param.call_args_list[0][0][1]["enabled"])
        self.assertEqual(
            set_param.call_args_list[-1][0][1]["last_status"],
            "restart_localization_succeeded",
        )
        self.assertEqual(set_param.call_args_list[-1][0][1]["last_job_id"], "job_1")
        self.assertTrue(set_param.call_args_list[-1][0][1]["last_used"])

    @mock.patch.object(LOCALIZATION_MODULE.rospy, "set_param")
    @mock.patch.object(LOCALIZATION_MODULE.rospy, "get_param")
    @mock.patch.object(LOCALIZATION_MODULE.rospy, "wait_for_service")
    @mock.patch.object(LOCALIZATION_MODULE.rospy, "ServiceProxy")
    def test_delegate_restart_runtime_operate_path_records_pose_not_supported_status(
        self,
        service_proxy,
        wait_for_service,
        get_param,
        set_param,
    ):
        node = LOCALIZATION_MODULE.LocalizationLifecycleManagerNode.__new__(LOCALIZATION_MODULE.LocalizationLifecycleManagerNode)
        node.robot_id = "local_robot"
        node.runtime_ns = "/cartographer/runtime"
        node.runtime_submit_job_service = "/cartographer/runtime/submit_job"
        node.runtime_get_job_service = "/cartographer/runtime/get_job"
        node.runtime_manager_service = "/cartographer/runtime/operate"
        node.runtime_job_timeout_s = 2.0
        node.manual_assist_pose_param_ns = "/localization/manual_assist_pose"

        def _wait_for_service(name, timeout=None):
            del timeout
            if name in {node.runtime_submit_job_service, node.runtime_get_job_service}:
                raise RuntimeError("unavailable")
            return None

        wait_for_service.side_effect = _wait_for_service
        get_param.side_effect = lambda key, default=None: {
            "/localization/manual_assist_pose": {
                "enabled": True,
                "consume_once": True,
                "map_name": "demo_map",
                "map_revision_id": "rev_demo_01",
                "frame_id": "map",
                "x": 1.0,
                "y": 2.0,
                "yaw": 0.3,
            }
        }.get(key, default)
        service_proxy.return_value = lambda **_kwargs: SimpleNamespace(
            success=True,
            message="runtime operate localized",
            map_name="demo_map",
            map_revision_id="rev_demo_01",
            localization_state="localized",
        )

        resp = node._delegate_restart_to_runtime_manager(
            robot_id="local_robot",
            map_name="demo_map",
            map_revision_id="rev_demo_01",
        )

        self.assertTrue(resp.success)
        self.assertEqual(resp.localization_state, "localized")
        self.assertGreaterEqual(set_param.call_count, 2)
        first_payload = set_param.call_args_list[0][0][1]
        last_payload = set_param.call_args_list[-1][0][1]
        self.assertEqual(first_payload["last_status"], "restart_localization_runtime_operate_pose_not_supported")
        self.assertTrue(first_payload["enabled"])
        self.assertEqual(last_payload["last_status"], "restart_localization_runtime_operate_completed_without_pose")
        self.assertFalse(last_payload["last_used"])

    @mock.patch.object(LOCALIZATION_MODULE.rospy, "set_param")
    @mock.patch.object(LOCALIZATION_MODULE.rospy, "get_param")
    @mock.patch.object(LOCALIZATION_MODULE.rospy, "ServiceProxy")
    def test_delegate_restart_rejects_scope_mismatched_manual_assist_pose(
        self,
        service_proxy,
        get_param,
        set_param,
    ):
        node = LOCALIZATION_MODULE.LocalizationLifecycleManagerNode.__new__(LOCALIZATION_MODULE.LocalizationLifecycleManagerNode)
        node.robot_id = "local_robot"
        node.runtime_ns = "/cartographer/runtime"
        node.runtime_submit_job_service = "/cartographer/runtime/submit_job"
        node.runtime_get_job_service = "/cartographer/runtime/get_job"
        node.runtime_manager_service = "/cartographer/runtime/operate"
        node.runtime_job_timeout_s = 2.0
        node.manual_assist_pose_param_ns = "/localization/manual_assist_pose"

        get_param.side_effect = lambda key, default=None: {
            "/localization/manual_assist_pose": {
                "enabled": True,
                "consume_once": True,
                "map_name": "demo_map",
                "map_revision_id": "rev_other_99",
                "frame_id": "map",
                "x": 1.0,
                "y": 2.0,
                "yaw": 0.3,
            }
        }.get(key, default)

        resp = node._delegate_restart_to_runtime_manager(
            robot_id="local_robot",
            map_name="demo_map",
            map_revision_id="rev_demo_01",
        )

        self.assertFalse(resp.success)
        self.assertEqual(resp.localization_state, "manual_assist_required")
        self.assertEqual(
            resp.message,
            "manual assist pose override revision mismatch: requested=rev_demo_01 override=rev_other_99",
        )
        service_proxy.assert_not_called()
        self.assertEqual(set_param.call_count, 1)
        self.assertEqual(
            set_param.call_args_list[0][0][1]["last_status"],
            "restart_localization_pose_scope_mismatch",
        )
        self.assertFalse(set_param.call_args_list[0][0][1]["last_used"])


if __name__ == "__main__":
    unittest.main()

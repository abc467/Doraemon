import json
import os
import sys
import tempfile
import threading
import time
import unittest
from types import SimpleNamespace
from unittest import mock


PACKAGE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPTS_DIR = os.path.join(PACKAGE_DIR, "scripts")
if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)

import dock_calibration_service_node as dock_calibration


class DockCalibrationFailClosedTest(unittest.TestCase):
    def _node(self, storage_path):
        node = dock_calibration.DockCalibrationServiceNode.__new__(
            dock_calibration.DockCalibrationServiceNode
        )
        node.robot_id = "CR-001"
        node.frame_id = "map"
        node.storage_path = storage_path
        node.stage1_param_name = "/coverage_task_manager/dock_stage1_xyyaw"
        node.stage2_param_name = "/coverage_task_manager/dock_xyyaw"
        node.docking_target_dist_param_name = "/dock_supply_manager/docking_target_dist"
        node.docking_controller_dist_param_name = "/docking_controller/docking_distance"
        node.docking_xy_tolerance_param_name = "/docking_controller/xy_tolerance"
        node.dock_target_dist = 0.780
        node.dock_xy_tolerance = 0.005
        node._runtime_config_map_name = ""
        node._runtime_config_map_id = ""
        node._runtime_config_map_md5 = ""
        node._runtime_config_valid = False
        node.require_persisted_calibration = True
        node._calibration_source_valid = False
        node._persisted_calibration_loaded = False
        node._saved_map_name = ""
        node._saved_map_id = ""
        node._saved_map_md5 = ""
        node._trusted_stage1 = None
        node._trusted_stage2 = None
        node._lock = threading.RLock()
        node._slam_state = None
        node._slam_state_ts = 0.0
        node.slam_state_stale_timeout_s = 2.0
        node.dock_pose_stale_timeout_s = 1.0
        node.dock_score_stale_timeout_s = 1.0
        node.dock_score_threshold = 0.00012
        node.stage2_min_extra_dist_m = 0.20
        node.stage2_abs_y_max = 0.15
        node.stage2_abs_yaw_max_rad = 0.14
        node._dock_pose = None
        node._dock_pose_ts = 0.0
        node._dock_score = float("nan")
        node._dock_score_ts = 0.0
        node._state_pub = mock.Mock()
        return node

    @staticmethod
    def _slam_state(map_suffix="a", **changes):
        values = {
            "robot_id": "CR-001",
            "active_map_name": "site-%s" % map_suffix,
            "active_map_id": "map-%s" % map_suffix,
            "active_map_md5": ("a" if map_suffix == "a" else "b") * 32,
            "runtime_map_name": "site-%s" % map_suffix,
            "runtime_map_id": "map-%s" % map_suffix,
            "runtime_map_md5": ("a" if map_suffix == "a" else "b") * 32,
            "runtime_map_ready": True,
            "active_map_match": True,
            "localization_valid": True,
            "localization_state": "localized",
            "tracked_pose_fresh": True,
            "tracked_pose_age_s": 0.0,
            "tracked_pose_frame": "map",
            "tracked_pose_x": 1.0,
            "tracked_pose_y": 2.0,
            "tracked_pose_theta": 0.1,
        }
        values.update(changes)
        return SimpleNamespace(**values)

    def _set_live_map(self, node, map_suffix="a", **changes):
        node._slam_state = self._slam_state(map_suffix, **changes)
        node._slam_state_ts = time.time()

    @staticmethod
    def _prime_binding(node, map_suffix="a", stage1=(1.0, 2.0, 0.1), stage2=(3.0, 4.0, 0.2)):
        node._saved_map_name = "site-%s" % map_suffix
        node._saved_map_id = "map-%s" % map_suffix
        node._saved_map_md5 = ("a" if map_suffix == "a" else "b") * 32
        node._trusted_stage1 = stage1
        node._trusted_stage2 = stage2
        node._persisted_calibration_loaded = True
        node._calibration_source_valid = True

    @staticmethod
    def _payload(robot_id="CR-001"):
        return {
            "robot_id": robot_id,
            "frame_id": "map",
            "map": {
                "name": "site-a",
                "id": "map-a",
                "md5": "0123456789abcdef0123456789abcdef",
            },
            "dock_stage1_xyyaw": [1.0, 2.0, 0.1],
            "dock_xyyaw": [3.0, 4.0, 0.2],
            "dock_target_dist": 0.78,
            "dock_xy_tolerance": 0.005,
        }

    @staticmethod
    def _fake_rospy(param_store):
        fake = mock.Mock()
        fake.has_param.side_effect = lambda name: name in param_store
        fake.get_param.side_effect = lambda name, default=None: param_store.get(name, default)
        fake.set_param.side_effect = lambda name, value: param_store.__setitem__(name, value)
        fake.delete_param.side_effect = lambda name: param_store.pop(name, None)
        fake.Time.now.return_value = mock.Mock()
        return fake

    def _write_payload(self, directory, payload):
        path = os.path.join(directory, "dock_calibration.yaml")
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle)
        return path

    def test_unset_and_nonfinite_stage_params_are_not_reported_as_set(self):
        values = ("[]", [], [1.0, 2.0], [1.0, 2.0, float("nan")], "[1,broken,3]")
        for value in values:
            with self.subTest(value=repr(value)):
                params = {"/stage": value}
                node = self._node("/unused")
                with mock.patch.object(dock_calibration, "rospy", self._fake_rospy(params)):
                    is_set, pose = node._stage_param("/stage")
                self.assertFalse(is_set)
                self.assertIsNone(pose)

    def test_foreign_or_missing_robot_id_never_applies_and_invalidates_old_points(self):
        for robot_id in ("CR-999", ""):
            with self.subTest(robot_id=robot_id or "missing"), tempfile.TemporaryDirectory() as tmp:
                payload = self._payload(robot_id=robot_id)
                if not robot_id:
                    payload.pop("robot_id")
                path = self._write_payload(tmp, payload)
                node = self._node(path)
                params = {
                    node.stage1_param_name: [9.0, 9.0, 9.0],
                    node.stage2_param_name: [8.0, 8.0, 8.0],
                }
                fake_rospy = self._fake_rospy(params)

                with mock.patch.object(dock_calibration, "rospy", fake_rospy):
                    loaded, message = node._load_storage(apply_params=True, quiet=True)

                self.assertFalse(loaded)
                self.assertIn("robot_id mismatch", message)
                self.assertNotIn(node.stage1_param_name, params)
                self.assertNotIn(node.stage2_param_name, params)
                self.assertFalse(node._calibration_source_valid)
                self.assertFalse(node._persisted_calibration_loaded)
                self.assertEqual(node.dock_target_dist, 0.780)
                self.assertEqual(node.dock_xy_tolerance, 0.005)

    def test_valid_vehicle_bound_storage_applies_only_after_full_validation(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write_payload(tmp, self._payload())
            node = self._node(path)
            params = {}
            with mock.patch.object(dock_calibration, "rospy", self._fake_rospy(params)):
                loaded, message = node._load_storage(apply_params=True, quiet=True)

            self.assertTrue(loaded, msg=message)
            self.assertEqual(params[node.stage1_param_name], [1.0, 2.0, 0.1])
            self.assertEqual(params[node.stage2_param_name], [3.0, 4.0, 0.2])
            self.assertTrue(node._calibration_source_valid)
            self.assertTrue(node._persisted_calibration_loaded)
            self.assertEqual(node._saved_map_name, "site-a")
            self.assertEqual(node._saved_map_id, "map-a")
            self.assertEqual(node._saved_map_md5, "0123456789abcdef0123456789abcdef")

    def test_incomplete_persisted_map_identity_is_rejected_before_apply(self):
        with tempfile.TemporaryDirectory() as tmp:
            payload = self._payload()
            payload["map"]["md5"] = ""
            path = self._write_payload(tmp, payload)
            node = self._node(path)
            params = {}

            with mock.patch.object(dock_calibration, "rospy", self._fake_rospy(params)):
                loaded, message = node._load_storage(apply_params=True, quiet=True)

            self.assertFalse(loaded)
            self.assertIn("map identity is incomplete", message)
            self.assertFalse(node._persisted_calibration_loaded)
            self.assertFalse(node._calibration_source_valid)
            self.assertNotIn(node.stage1_param_name, params)
            self.assertNotIn(node.stage2_param_name, params)

    def test_persist_refuses_incomplete_live_map_and_never_sets_loaded_marker(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "dock_calibration.yaml")
            node = self._node(path)
            node._saved_map_name = "site-a"
            node._saved_map_id = "map-a"
            node._saved_map_md5 = ""
            params = {
                node.stage1_param_name: [1.0, 2.0, 0.1],
                node.stage2_param_name: [3.0, 4.0, 0.2],
            }

            with mock.patch.object(dock_calibration, "rospy", self._fake_rospy(params)):
                with self.assertRaisesRegex(ValueError, "fresh slam state"):
                    node._persist_storage(stage1=(1.0, 2.0, 0.1), stage2=(3.0, 4.0, 0.2))

            self.assertFalse(os.path.exists(path))
            self.assertFalse(node._persisted_calibration_loaded)
            self.assertFalse(node._calibration_source_valid)

    def test_stage_update_rolls_back_when_map_binding_cannot_be_persisted(self):
        with tempfile.TemporaryDirectory() as tmp:
            node = self._node(os.path.join(tmp, "dock_calibration.yaml"))
            params = {}

            with mock.patch.object(dock_calibration, "rospy", self._fake_rospy(params)):
                with self.assertRaisesRegex(ValueError, "fresh slam state"):
                    node._set_stage(1, 1.0, 2.0, 0.1, operation=1)

            self.assertNotIn(node.stage1_param_name, params)
            self.assertFalse(node._persisted_calibration_loaded)
            self.assertFalse(node._calibration_source_valid)

    def test_explicit_noncommercial_opt_out_retains_fully_bound_static_config(self):
        node = self._node("/missing/dock_calibration.yaml")
        node.require_persisted_calibration = False
        node._runtime_config_valid = True
        node._runtime_config_map_name = "site-a"
        node._runtime_config_map_id = "map-a"
        node._runtime_config_map_md5 = "0123456789abcdef0123456789abcdef"
        params = {
            node.stage1_param_name: [1.0, 2.0, 0.1],
            node.stage2_param_name: [3.0, 4.0, 0.2],
        }

        with mock.patch.object(dock_calibration, "rospy", self._fake_rospy(params)):
            loaded, _message = node._load_storage(apply_params=True, quiet=True)

        self.assertFalse(loaded)
        self.assertTrue(node._calibration_source_valid)
        self.assertFalse(node._persisted_calibration_loaded)
        self.assertIn(node.stage1_param_name, params)
        self.assertIn(node.stage2_param_name, params)

    def test_commercial_default_rejects_static_pose_and_map_without_persisted_file(self):
        node = self._node("/missing/dock_calibration.yaml")
        node._runtime_config_valid = True
        node._runtime_config_map_name = "site-a"
        node._runtime_config_map_id = "map-a"
        node._runtime_config_map_md5 = "0123456789abcdef0123456789abcdef"
        params = {
            node.stage1_param_name: [1.0, 2.0, 0.1],
            node.stage2_param_name: [3.0, 4.0, 0.2],
        }

        with mock.patch.object(dock_calibration, "rospy", self._fake_rospy(params)):
            loaded, _message = node._load_storage(apply_params=True, quiet=True)

        self.assertFalse(loaded)
        self.assertFalse(node._calibration_source_valid)
        self.assertFalse(node._persisted_calibration_loaded)
        self.assertNotIn(node.stage1_param_name, params)
        self.assertNotIn(node.stage2_param_name, params)

    def test_map_binding_requires_name_id_md5_and_exact_active_runtime_match(self):
        full = {
            "name": "site-a",
            "id": "map-a",
            "md5": "0123456789abcdef0123456789abcdef",
        }
        self.assertEqual(
            dock_calibration._map_identity_match_issue(saved=full, active=full, runtime=full),
            "",
        )
        incomplete = dict(full)
        incomplete["md5"] = ""
        self.assertIn(
            "incomplete map identity",
            dock_calibration._map_identity_match_issue(
                saved=incomplete,
                active=full,
                runtime=full,
            ),
        )
        wrong_runtime = dict(full)
        wrong_runtime["id"] = "map-b"
        self.assertIn(
            "runtime map",
            dock_calibration._map_identity_match_issue(
                saved=full,
                active=full,
                runtime=wrong_runtime,
            ),
        )

    def test_set_stage_rejects_nonfinite_and_out_of_range_coordinates_without_mutation(self):
        invalid_poses = (
            (float("nan"), 0.0, 0.0),
            (0.0, float("inf"), 0.0),
            (0.0, 0.0, float("inf")),
            (1.0e7, 0.0, 0.0),
            (0.0, 0.0, 4.0),
        )
        for pose in invalid_poses:
            with self.subTest(pose=pose), tempfile.TemporaryDirectory() as tmp:
                node = self._node(os.path.join(tmp, "dock.yaml"))
                self._set_live_map(node)
                params = {node.stage1_param_name: [9.0, 9.0, 0.0]}
                with mock.patch.object(dock_calibration, "rospy", self._fake_rospy(params)):
                    with mock.patch.object(node, "_persist_storage") as persist:
                        with self.assertRaises(ValueError):
                            node._set_stage(1, *pose, operation=3)
                self.assertEqual(params[node.stage1_param_name], [9.0, 9.0, 0.0])
                persist.assert_not_called()

    def test_nonfinite_tracked_pose_is_never_fresh_or_saved_as_origin(self):
        node = self._node("/unused")
        self._set_live_map(node, tracked_pose_x=float("nan"))

        fresh, _age, _frame, x, y, yaw = node._current_pose_from_slam(time.time())

        self.assertFalse(fresh)
        self.assertEqual((x, y, yaw), (0.0, 0.0, 0.0))

    def test_live_identity_requires_fresh_matching_robot_and_ready_exact_map(self):
        cases = {
            "wrong robot": {"robot_id": "CR-999"},
            "missing active md5": {"active_map_md5": ""},
            "runtime mismatch": {"runtime_map_id": "map-b"},
            "runtime not ready": {"runtime_map_ready": False},
            "active mismatch flag": {"active_map_match": False},
            "localization invalid": {"localization_valid": False},
        }
        for label, changes in cases.items():
            with self.subTest(label=label):
                node = self._node("/unused")
                self._set_live_map(node, **changes)
                with self.assertRaises(ValueError):
                    node._current_calibration_map_identity()

        node = self._node("/unused")
        self._set_live_map(node)
        node._slam_state_ts = time.time() - 5.0
        with self.assertRaisesRegex(ValueError, "fresh slam state"):
            node._current_calibration_map_identity()

    def test_cross_map_stage_update_starts_new_round_and_clears_other_stage(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "dock.yaml")
            node = self._node(path)
            self._prime_binding(node, "a")
            self._set_live_map(node, "b")
            params = {
                node.stage1_param_name: [1.0, 2.0, 0.1],
                node.stage2_param_name: [3.0, 4.0, 0.2],
            }
            fake_rospy = self._fake_rospy(params)
            with mock.patch.object(dock_calibration, "rospy", fake_rospy):
                with mock.patch.object(node, "_build_state", return_value=dock_calibration.DockCalibrationState()):
                    node._set_stage(1, 10.0, 20.0, 0.3, operation=3)

            self.assertEqual(params[node.stage1_param_name], [10.0, 20.0, 0.3])
            self.assertNotIn(node.stage2_param_name, params)
            self.assertEqual(node._trusted_stage1, (10.0, 20.0, 0.3))
            self.assertIsNone(node._trusted_stage2)
            with open(path, "r", encoding="utf-8") as handle:
                payload = (
                    dock_calibration.yaml.safe_load(handle)
                    if dock_calibration.yaml is not None
                    else json.load(handle)
                )
            self.assertEqual(payload["map"]["id"], "map-b")
            self.assertEqual(payload["dock_stage1_xyyaw"], [10.0, 20.0, 0.3])
            self.assertIsNone(payload["dock_xyyaw"])

    def test_cross_map_stage_failure_rolls_back_both_stages_and_binding(self):
        node = self._node("/unused")
        self._prime_binding(node, "a")
        self._set_live_map(node, "b")
        params = {
            node.stage1_param_name: [1.0, 2.0, 0.1],
            node.stage2_param_name: [3.0, 4.0, 0.2],
        }
        with mock.patch.object(dock_calibration, "rospy", self._fake_rospy(params)):
            with mock.patch.object(node, "_persist_storage", side_effect=OSError("disk full")):
                with self.assertRaisesRegex(OSError, "disk full"):
                    node._set_stage(1, 10.0, 20.0, 0.3, operation=3)

        self.assertEqual(params[node.stage1_param_name], [1.0, 2.0, 0.1])
        self.assertEqual(params[node.stage2_param_name], [3.0, 4.0, 0.2])
        self.assertEqual(node._saved_map_id, "map-a")
        self.assertEqual(node._trusted_stage1, (1.0, 2.0, 0.1))
        self.assertEqual(node._trusted_stage2, (3.0, 4.0, 0.2))
        self.assertTrue(node._persisted_calibration_loaded)

    def test_dock_parameter_update_cannot_rebind_old_stages_to_new_map(self):
        node = self._node("/unused")
        self._prime_binding(node, "a")
        self._set_live_map(node, "b")
        params = {
            node.docking_target_dist_param_name: 0.78,
            node.docking_controller_dist_param_name: 0.78,
            node.docking_xy_tolerance_param_name: 0.005,
        }
        with mock.patch.object(dock_calibration, "rospy", self._fake_rospy(params)):
            with self.assertRaisesRegex(ValueError, "map already bound"):
                node._set_dock_params(0.8, 0.01, operation=6)

        self.assertEqual(node._saved_map_id, "map-a")
        self.assertEqual(node.dock_target_dist, 0.78)
        self.assertEqual(params[node.docking_target_dist_param_name], 0.78)

    def test_public_rosparam_tamper_is_not_trusted_or_persisted(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write_payload(tmp, self._payload())
            node = self._node(path)
            params = {}
            fake_rospy = self._fake_rospy(params)
            with mock.patch.object(dock_calibration, "rospy", fake_rospy):
                loaded, message = node._load_storage(apply_params=True, quiet=True)
                self.assertTrue(loaded, msg=message)
                params[node.stage2_param_name] = [999.0, 999.0, 0.0]
                state = node._build_state()
                self.assertFalse(state.stage2_set)
                self.assertEqual((state.stage2_x, state.stage2_y, state.stage2_yaw), (3.0, 4.0, 0.2))
                self.assertTrue(any("differs from trusted" in warning for warning in state.warnings))

                self._set_live_map(
                    node,
                    active_map_name="site-a",
                    active_map_id="map-a",
                    active_map_md5="0123456789abcdef0123456789abcdef",
                    runtime_map_name="site-a",
                    runtime_map_id="map-a",
                    runtime_map_md5="0123456789abcdef0123456789abcdef",
                )
                node._set_stage(1, 5.0, 6.0, 0.3, operation=3)

            with open(path, "r", encoding="utf-8") as handle:
                payload = (
                    dock_calibration.yaml.safe_load(handle)
                    if dock_calibration.yaml is not None
                    else json.load(handle)
                )
            self.assertEqual(payload["dock_xyyaw"], [3.0, 4.0, 0.2])
            self.assertEqual(params[node.stage2_param_name], [3.0, 4.0, 0.2])

    def test_persisted_out_of_range_pose_is_rejected(self):
        for invalid_pose in ([1.0e7, 0.0, 0.0], [0.0, 0.0, 4.0]):
            with self.subTest(invalid_pose=invalid_pose), tempfile.TemporaryDirectory() as tmp:
                payload = self._payload()
                payload["dock_stage1_xyyaw"] = invalid_pose
                path = self._write_payload(tmp, payload)
                node = self._node(path)
                with mock.patch.object(dock_calibration, "rospy", self._fake_rospy({})):
                    loaded, message = node._load_storage(apply_params=True, quiet=True)
                self.assertFalse(loaded)
                self.assertIn("invalid dock_stage1_xyyaw", message)

    def test_reload_apply_failure_rolls_back_old_trusted_state_and_outputs(self):
        with tempfile.TemporaryDirectory() as tmp:
            payload = self._payload()
            payload["dock_stage1_xyyaw"] = [10.0, 20.0, 0.3]
            payload["dock_xyyaw"] = [30.0, 40.0, 0.4]
            path = self._write_payload(tmp, payload)
            node = self._node(path)
            self._prime_binding(node, "a")
            params = {
                node.stage1_param_name: [1.0, 2.0, 0.1],
                node.stage2_param_name: [3.0, 4.0, 0.2],
                node.docking_target_dist_param_name: 0.78,
                node.docking_controller_dist_param_name: 0.78,
                node.docking_xy_tolerance_param_name: 0.005,
            }
            fake_rospy = self._fake_rospy(params)
            base_set = fake_rospy.set_param.side_effect
            failed = {"once": False}

            def fail_mid_apply(name, value):
                if name == node.stage2_param_name and value == [30.0, 40.0, 0.4] and not failed["once"]:
                    failed["once"] = True
                    raise RuntimeError("injected set failure")
                return base_set(name, value)

            fake_rospy.set_param.side_effect = fail_mid_apply
            with mock.patch.object(dock_calibration, "rospy", fake_rospy):
                loaded, message = node._load_storage(apply_params=True, quiet=True)

            self.assertFalse(loaded)
            self.assertIn("atomically", message)
            self.assertEqual(params[node.stage1_param_name], [1.0, 2.0, 0.1])
            self.assertEqual(params[node.stage2_param_name], [3.0, 4.0, 0.2])
            self.assertEqual(node._trusted_stage1, (1.0, 2.0, 0.1))
            self.assertEqual(node._trusted_stage2, (3.0, 4.0, 0.2))
            self.assertTrue(node._persisted_calibration_loaded)

    def test_status_command_and_slam_stream_require_exact_robot_id(self):
        node = self._node("/unused")
        state = dock_calibration.DockCalibrationState()
        node._build_state = mock.Mock(return_value=state)
        for robot_id in ("", "CR-999"):
            status = node._handle_status(SimpleNamespace(robot_id=robot_id))
            self.assertFalse(status.success)
            command = node._handle_command(
                SimpleNamespace(
                    robot_id=robot_id,
                    operation=int(dock_calibration.OperateDockCalibrationRequest.GET),
                )
            )
            self.assertFalse(command.success)
            self.assertEqual(command.error_code, "ROBOT_ID_MISMATCH")

        wrong = self._slam_state(robot_id="CR-999")
        fake_rospy = self._fake_rospy({})
        with mock.patch.object(dock_calibration, "rospy", fake_rospy):
            node._on_slam_state(wrong)
        self.assertIsNone(node._slam_state)
        self.assertEqual(node._slam_state_ts, 0.0)


if __name__ == "__main__":
    unittest.main()

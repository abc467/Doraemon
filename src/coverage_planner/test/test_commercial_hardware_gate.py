#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import subprocess
import unittest
import xml.etree.ElementTree as ET


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(THIS_DIR)))


def read_repo_file(relative_path):
    with open(os.path.join(REPO_ROOT, relative_path), "r", encoding="utf-8") as handle:
        return handle.read()


def read_env_defaults(relative_path):
    values = {}
    for raw_line in read_repo_file(relative_path).splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key] = value
    return values


class CommercialHardwareGateTest(unittest.TestCase):
    def test_cartographer_has_no_legacy_home_debug_output(self):
        source = read_repo_file(
            "src/cartographer/cartographer/mapping/internal/2d/pose_graph_2d.cc"
        )
        self.assertNotIn("/home/lb/cartographer_web", source)
        self.assertNotIn("nodes_dense.csv", source)

    def test_vehicle_identity_template_keeps_camera_readiness_non_blocking(self):
        config = read_env_defaults("config/runtime.a26022.env")
        serials = [config["RUNTIME_ORBBEC_CAMERA%d_SERIAL_NUMBER" % index] for index in (1, 2, 3)]
        paths = [config["RUNTIME_ORBBEC_CAMERA%d_USB_PORT" % index] for index in (1, 2, 3)]
        self.assertIn("REPLACE", config["ROBOT_ID"])
        self.assertIn("REPLACE", config["DORAEMON_A_BOX_IFACE"])
        self.assertTrue(all("REPLACE" in value for value in serials + paths))
        self.assertEqual(config.get("RUNTIME_START_DEPTH_CAMERAS"), "true")
        self.assertEqual(config.get("DORAEMON_REQUIRE_DEPTH_CAMERA_IDENTITIES"), "false")
        self.assertEqual(config.get("RUNTIME_REQUIRE_DEPTH_CAMERA_TOPICS"), "false")
        self.assertEqual(config.get("RUNTIME_ENABLE_DEPTH_LEFT_CAM"), "true")
        self.assertEqual(config.get("RUNTIME_ENABLE_DEPTH_RIGHT_CAM"), "true")
        self.assertEqual(config.get("RUNTIME_ENABLE_DEPTH_UP_CAM"), "true")
        self.assertEqual(config.get("DORAEMON_NO_ACTION_ACCEPTANCE"), "true")
        self.assertEqual(config.get("DORAEMON_ACTION_TEST_APPROVED"), "false")

    def test_all_camera_launch_entries_use_external_vehicle_identity(self):
        launch_files = (
            "src/cleanrobot/launch/orbbec_dual_depth_ground.launch",
            "src/cleanrobot/launch/cleanrobot_base.launch",
            "src/cleanrobot/launch/depth_obstacle_runtime.launch",
            "src/robot_hw_bridge/launch/bringup_real_robot.launch",
        )
        for relative_path in launch_files:
            with self.subTest(relative_path=relative_path):
                root = ET.parse(os.path.join(REPO_ROOT, relative_path)).getroot()
                args = {item.get("name"): item.get("default") for item in root.findall("arg")}
                for index in (1, 2, 3):
                    self.assertEqual(
                        args.get("camera%d_serial_number" % index),
                        "$(optenv RUNTIME_ORBBEC_CAMERA%d_SERIAL_NUMBER '')" % index,
                    )
                    self.assertEqual(
                        args.get("camera%d_usb_port" % index),
                        "$(optenv RUNTIME_ORBBEC_CAMERA%d_USB_PORT '')" % index,
                    )
                source = read_repo_file(relative_path)
                self.assertNotIn("AY2816200E7", source)
                self.assertNotIn("AY2816200SB", source)
                self.assertNotIn("CPAX16300EJ", source)

    def test_camera_disabled_launch_allows_missing_vehicle_camera_environment(self):
        env = os.environ.copy()
        for index in (1, 2, 3):
            env.pop("RUNTIME_ORBBEC_CAMERA%d_SERIAL_NUMBER" % index, None)
            env.pop("RUNTIME_ORBBEC_CAMERA%d_USB_PORT" % index, None)
        source_root = os.path.join(REPO_ROOT, "src")
        existing_ros_package_path = env.get("ROS_PACKAGE_PATH", "")
        env["ROS_PACKAGE_PATH"] = source_root + (
            ":" + existing_ros_package_path if existing_ros_package_path else ""
        )

        result = subprocess.run(
            [
                "/opt/ros/noetic/bin/roslaunch",
                "--dump-params",
                "cleanrobot",
                "cleanrobot_base.launch",
                "start_depth_cameras:=false",
            ],
            cwd=REPO_ROOT,
            env=env,
            text=True,
            capture_output=True,
            timeout=10,
            check=False,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertNotIn("Unable to load", result.stdout + result.stderr)
        self.assertNotIn("/gemini_cf/camera/serial_number", result.stdout)

    def test_strict_camera_topic_mode_requires_all_three_streams(self):
        source = read_repo_file("scripts/start_runtime.sh")
        self.assertIn("normalize_boolean_variable RUNTIME_REQUIRE_DEPTH_CAMERA_TOPICS", source)
        self.assertNotIn("runtime_warn_if_optional_topic_missing /gemini_", source)
        self.assertIn("相机缺失或异常不阻塞整机启动", source)
        for namespace in ("gemini_cf", "gemini_nj", "gemini_front"):
            self.assertIn("runtime_wait_for_topic /%s/depth/image_raw" % namespace, source)
            self.assertIn("runtime_wait_for_topic /%s/depth/points" % namespace, source)
            self.assertIn("runtime_require_orbbec_serial %s" % namespace, source)

    def test_depth_obstacle_sources_are_independently_wired_and_non_blocking(self):
        source = read_repo_file("scripts/start_runtime.sh")
        for runtime_name, launch_name in (
            ("RUNTIME_ENABLE_DEPTH_LEFT_CAM", "enable_depth_left_cam"),
            ("RUNTIME_ENABLE_DEPTH_RIGHT_CAM", "enable_depth_right_cam"),
            ("RUNTIME_ENABLE_DEPTH_UP_CAM", "enable_depth_up_cam"),
        ):
            self.assertIn(
                "normalize_boolean_variable %s" % runtime_name,
                source,
            )
            self.assertIn(
                "%s:=${%s}" % (launch_name, runtime_name),
                source,
            )
        self.assertIn(
            'if [[ "${RUNTIME_ENABLE_DEPTH_UP_CAM}" == "true" ]]',
            source,
        )
        self.assertIn(
            'runtime_warn_if_optional_node_missing /up/gs_node "前向深度避障节点"',
            source,
        )
        self.assertNotIn("runtime_wait_for_node /up/gs_node", source)
        self.assertNotIn("runtime_wait_for_topic /up/obstacle_2d", source)

    def test_camera_arguments_are_protected_from_extra_args(self):
        source = read_repo_file("scripts/start_runtime.sh")
        self.assertIn("RUNTIME_BASE_EXTRA_ARGS may not override protected camera argument", source)
        self.assertGreater(
            source.find('append_shell_words base_cmd_words "${RUNTIME_BASE_EXTRA_ARGS}"'),
            -1,
        )
        protected_offset = source.find("# Append protected commercial identity arguments last")
        extra_offset = source.find('append_shell_words base_cmd_words "${RUNTIME_BASE_EXTRA_ARGS}"')
        self.assertGreater(protected_offset, extra_offset)

        script = r"""
set -euo pipefail
export DORAEMON_RUNTIME_CONFIG_FILE=/dev/null
source scripts/start_runtime.sh
ROBOT_ID=CR-TEST
DORAEMON_A_BOX_IFACE=enp1s0
DORAEMON_NO_ACTION_ACCEPTANCE=true
DORAEMON_ACTION_TEST_APPROVED=false
RUNTIME_START_DEPTH_CAMERAS=TRUE
RUNTIME_REQUIRE_DEPTH_CAMERA_TOPICS=1
DORAEMON_REQUIRE_DEPTH_CAMERA_IDENTITIES=yes
RUNTIME_ORBBEC_CAMERA1_SERIAL_NUMBER=S1
RUNTIME_ORBBEC_CAMERA2_SERIAL_NUMBER=S2
RUNTIME_ORBBEC_CAMERA3_SERIAL_NUMBER=S3
RUNTIME_ORBBEC_CAMERA1_USB_PORT=1-1
RUNTIME_ORBBEC_CAMERA2_USB_PORT=1-2
RUNTIME_ORBBEC_CAMERA3_USB_PORT=1-3
RUNTIME_BASE_EXTRA_ARGS='camera1_serial_number:=OVERRIDE'
if validate_commercial_vehicle_identity; then
  exit 9
fi
RUNTIME_BASE_EXTRA_ARGS=''
RUNTIME_ORBBEC_CAMERA2_SERIAL_NUMBER=S1
if validate_commercial_vehicle_identity; then
  exit 10
fi
RUNTIME_ORBBEC_CAMERA2_SERIAL_NUMBER=S2
RUNTIME_ORBBEC_CAMERA2_USB_PORT=not-a-topology
if validate_commercial_vehicle_identity; then
  exit 11
fi
RUNTIME_ORBBEC_CAMERA2_USB_PORT=1-2
validate_commercial_vehicle_identity
[[ "${RUNTIME_START_DEPTH_CAMERAS}" == true ]]
[[ "${RUNTIME_REQUIRE_DEPTH_CAMERA_TOPICS}" == true ]]
[[ "${DORAEMON_REQUIRE_DEPTH_CAMERA_IDENTITIES}" == true ]]
RUNTIME_REQUIRE_DEPTH_CAMERA_TOPICS=off
DORAEMON_REQUIRE_DEPTH_CAMERA_IDENTITIES=no
RUNTIME_ORBBEC_CAMERA1_SERIAL_NUMBER=
RUNTIME_ORBBEC_CAMERA2_SERIAL_NUMBER=
RUNTIME_ORBBEC_CAMERA3_SERIAL_NUMBER=
RUNTIME_ORBBEC_CAMERA1_USB_PORT=
RUNTIME_ORBBEC_CAMERA2_USB_PORT=
RUNTIME_ORBBEC_CAMERA3_USB_PORT=
validate_commercial_vehicle_identity
[[ "${RUNTIME_REQUIRE_DEPTH_CAMERA_TOPICS}" == false ]]
[[ "${DORAEMON_REQUIRE_DEPTH_CAMERA_IDENTITIES}" == false ]]
RUNTIME_START_DEPTH_CAMERAS=off
validate_commercial_vehicle_identity
[[ "${RUNTIME_START_DEPTH_CAMERAS}" == false ]]
RUNTIME_START_DEPTH_CAMERAS=true
DORAEMON_NO_ACTION_ACCEPTANCE=false
DORAEMON_ACTION_TEST_APPROVED=false
if validate_commercial_vehicle_identity; then
  exit 13
fi
DORAEMON_ACTION_TEST_APPROVED=true
MCORE_MAX_ABS_LINEAR_VELOCITY=0.4
MCORE_MAX_ABS_ANGULAR_VELOCITY=1.2
validate_commercial_vehicle_identity
DORAEMON_NO_ACTION_ACCEPTANCE=true
if validate_commercial_vehicle_identity; then
  exit 14
fi
"""
        result = subprocess.run(
            ["bash", "-c", script],
            cwd=REPO_ROOT,
            text=True,
            capture_output=True,
            timeout=5,
            check=False,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)

    def test_boot_preflight_requires_usb_serials_and_topology(self):
        source = read_repo_file("scripts/wait_robot_boot_ready.sh")
        self.assertIn("DORAEMON_REQUIRE_DEPTH_CAMERA_IDENTITIES", source)
        self.assertIn("validate_unique_camera_identities", source)
        self.assertIn("has_usb_serial", source)
        self.assertIn("has_usb_topology_path", source)
        self.assertIn("verify_orbbec_sdk_pairs.py", source)
        self.assertIn("orbbec_remaining_sec=$((TIMEOUT_SEC - $(elapsed_sec)))", source)
        self.assertIn("Orbbec SDK serial/topology stability gate failed", source)
        self.assertIn("ORBBEC_MIN_USB_SPEED", source)
        self.assertIn('COMMERCIAL_ORBBEC_VENDOR_ID="2bc5"', source)
        self.assertIn('COMMERCIAL_ORBBEC_MIN_USB_SPEED="5000"', source)
        self.assertIn("validate_orbbec_commercial_baseline", source)
        self.assertIn("validate_installed_commercial_udev_rules", source)
        self.assertIn("validate_serial_alias_permissions", source)
        self.assertIn("validate_orbbec_usb_node_permissions", source)
        self.assertIn("root:dialout 0660", source)
        self.assertIn("root:video 0660", source)

    def test_deployment_verifier_runs_hardware_preflight_as_a_hard_gate(self):
        source = read_repo_file("scripts/verify_x86_ubuntu20_deployment.sh")
        self.assertIn('"${REPO_ROOT}/scripts/wait_robot_boot_ready.sh"', source)
        self.assertIn("local hardware/udev/network commercial preflight", source)
        self.assertIn("charging-station endpoint is not reachable", source)
        self.assertNotIn('warn "device ${device} is not present"', source)

    def test_boot_preflight_rejects_orbbec_baseline_downgrades_before_hardware_wait(self):
        base_env = os.environ.copy()
        base_env.update(
            {
                "ROBOT_ID": "CR-TEST",
                "DORAEMON_A_BOX_IFACE": "enp1s0",
                "DORAEMON_NO_ACTION_ACCEPTANCE": "true",
                "DORAEMON_ACTION_TEST_APPROVED": "false",
                "RUNTIME_START_DEPTH_CAMERAS": "true",
                "RUNTIME_REQUIRE_DEPTH_CAMERA_TOPICS": "true",
                "DORAEMON_REQUIRE_DEPTH_CAMERA_IDENTITIES": "true",
                "DORAEMON_BOOT_WAIT_TIMEOUT": "1",
            }
        )
        cases = (
            (
                {"DORAEMON_ORBBEC_VENDOR_ID": "1234"},
                "DORAEMON_ORBBEC_VENDOR_ID must remain 2bc5",
            ),
            (
                {"DORAEMON_ORBBEC_MIN_USB_SPEED": "480"},
                "DORAEMON_ORBBEC_MIN_USB_SPEED must not be below 5000",
            ),
            (
                {"DORAEMON_ORBBEC_MIN_USB_SPEED": "not-a-speed"},
                "DORAEMON_ORBBEC_MIN_USB_SPEED must be numeric and at least 5000",
            ),
        )

        for overrides, expected_error in cases:
            with self.subTest(overrides=overrides):
                env = base_env.copy()
                env.update(overrides)
                result = subprocess.run(
                    ["bash", "scripts/wait_robot_boot_ready.sh"],
                    cwd=REPO_ROOT,
                    env=env,
                    text=True,
                    capture_output=True,
                    timeout=5,
                    check=False,
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(expected_error, result.stdout)

    def test_boot_preflight_allows_depth_camera_gates_to_be_disabled(self):
        source = read_repo_file("scripts/wait_robot_boot_ready.sh")
        self.assertNotIn(
            "commercial preflight requires RUNTIME_START_DEPTH_CAMERAS=true",
            source,
        )
        self.assertNotIn(
            "enabled depth cameras require both topic and identity commercial gates",
            source,
        )
        self.assertIn(
            "camera faults will not block robot startup",
            source,
        )
        self.assertIn(
            "Orbbec serial/topology/SDK readiness gate disabled",
            source,
        )

    def test_boot_preflight_rejects_unapproved_action_mode_before_hardware_wait(self):
        env = os.environ.copy()
        env.update(
            {
                "ROBOT_ID": "CR-TEST",
                "DORAEMON_A_BOX_IFACE": "enp1s0",
                "DORAEMON_NO_ACTION_ACCEPTANCE": "false",
                "DORAEMON_ACTION_TEST_APPROVED": "false",
                "RUNTIME_START_DEPTH_CAMERAS": "true",
                "RUNTIME_REQUIRE_DEPTH_CAMERA_TOPICS": "true",
                "DORAEMON_REQUIRE_DEPTH_CAMERA_IDENTITIES": "true",
                "DORAEMON_BOOT_WAIT_TIMEOUT": "1",
            }
        )
        result = subprocess.run(
            ["bash", "scripts/wait_robot_boot_ready.sh"],
            cwd=REPO_ROOT,
            env=env,
            text=True,
            capture_output=True,
            timeout=5,
            check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(
            "action-capable runtime requires DORAEMON_ACTION_TEST_APPROVED=true",
            result.stdout,
        )

    def test_boot_preflight_rejects_missing_vehicle_id_before_hardware_wait(self):
        env = os.environ.copy()
        for name in list(env):
            if name == "ROBOT_ID" or name.startswith("RUNTIME_ORBBEC_"):
                env.pop(name, None)
        env.update(
            {
                "RUNTIME_START_DEPTH_CAMERAS": "true",
                "RUNTIME_REQUIRE_DEPTH_CAMERA_TOPICS": "true",
                "DORAEMON_REQUIRE_DEPTH_CAMERA_IDENTITIES": "true",
                "DORAEMON_BOOT_WAIT_TIMEOUT": "1",
            }
        )
        result = subprocess.run(
            ["bash", "scripts/wait_robot_boot_ready.sh"],
            cwd=REPO_ROOT,
            env=env,
            text=True,
            capture_output=True,
            timeout=5,
            check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("ROBOT_ID must be the explicit vehicle asset identifier", result.stdout)


if __name__ == "__main__":
    unittest.main()

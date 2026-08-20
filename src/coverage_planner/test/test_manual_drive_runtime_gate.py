#!/usr/bin/env python3

import os
import subprocess
import unittest
import xml.etree.ElementTree as ET


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(THIS_DIR)))


def read_repo_file(relative_path):
    with open(os.path.join(REPO_ROOT, relative_path), "r", encoding="utf-8") as handle:
        return handle.read()


def bash_function(source, name):
    start = source.index(name + "() {")
    depth = 0
    for offset in range(start, len(source)):
        if source[offset] == "{":
            depth += 1
        elif source[offset] == "}":
            depth -= 1
            if depth == 0:
                return source[start : offset + 1]
    raise AssertionError("unterminated bash function: %s" % name)


def run_start_runtime_snippet(snippet):
    env = {
        "DORAEMON_RUNTIME_CONFIG_FILE": "/dev/null",
        "HOME": os.environ.get("HOME", "/tmp"),
        "LANG": "C.UTF-8",
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
    }
    return subprocess.run(
        ["bash", "-c", "set -euo pipefail\nsource scripts/start_runtime.sh\n" + snippet],
        cwd=REPO_ROOT,
        env=env,
        text=True,
        capture_output=True,
        timeout=10,
        check=False,
    )


VALID_IDENTITY = r"""
ROBOT_ID=CR-TEST
DORAEMON_A_BOX_IFACE=enp1s0
ROSBRIDGE_ADDRESS=127.0.0.1
RUNTIME_START_DEPTH_CAMERAS=true
RUNTIME_REQUIRE_DEPTH_CAMERA_TOPICS=true
DORAEMON_REQUIRE_DEPTH_CAMERA_IDENTITIES=true
RUNTIME_DEPTH_CAMERA_WATCHDOG_ENABLE=true
RUNTIME_ENABLE_DEPTH_OBSTACLE_TRACKING=true
RUNTIME_ENABLE_DEPTH_LEFT_CAM=true
RUNTIME_ENABLE_DEPTH_RIGHT_CAM=true
RUNTIME_ENABLE_DEPTH_UP_CAM=true
RUNTIME_ORBBEC_CAMERA1_SERIAL_NUMBER=S1
RUNTIME_ORBBEC_CAMERA2_SERIAL_NUMBER=S2
RUNTIME_ORBBEC_CAMERA3_SERIAL_NUMBER=S3
RUNTIME_ORBBEC_CAMERA1_USB_PORT=1-1
RUNTIME_ORBBEC_CAMERA2_USB_PORT=1-2
RUNTIME_ORBBEC_CAMERA3_USB_PORT=1-3
RUNTIME_BASE_EXTRA_ARGS=
WHEEL_ODOM_EXTRA_ARGS=
MCORE_VELOCITY_EXTRA_ARGS=
HARDWARE_BRIDGES_EXTRA_ARGS=
BACKEND_RUNTIME_SMOKE_ACTIONS=
BACKEND_RUNTIME_SMOKE_EXTRA_ARGS=
BACKEND_PRODUCTION_ACCEPTANCE_EXTRA_ARGS=
"""


class ManualDriveRuntimeGateTest(unittest.TestCase):
    def test_launches_default_off_and_wire_approval_into_node(self):
        frontend_path = os.path.join(
            REPO_ROOT, "src/coverage_planner/launch/frontend_editor_backend.launch"
        )
        planner_path = os.path.join(
            REPO_ROOT, "src/coverage_planner/launch/planner_server.launch"
        )
        for path in (frontend_path, planner_path):
            root = ET.parse(path).getroot()
            args = {item.get("name"): item.get("default") for item in root.findall("arg")}
            self.assertEqual(args.get("enable_manual_drive_service"), "false")
            self.assertEqual(args.get("manual_drive_no_action_acceptance"), "true")
            self.assertEqual(args.get("manual_drive_action_test_approved"), "false")
            self.assertEqual(args.get("manual_drive_require_role"), "false")
            self.assertEqual(args.get("manual_drive_require_slam_state"), "false")
            self.assertEqual(args.get("manual_drive_require_task_state"), "false")
            self.assertEqual(args.get("manual_drive_require_odometry_state"), "false")
            self.assertEqual(args.get("manual_drive_require_combined_status"), "true")

        planner = ET.parse(planner_path).getroot()
        manual_group = next(
            item
            for item in planner.findall("group")
            if item.get("if") == "$(arg enable_manual_drive_service)"
        )
        node = manual_group.find("node")
        self.assertEqual(node.get("name"), "manual_drive_service")
        params = {item.get("name"): item.get("value") for item in node.findall("param")}
        self.assertEqual(
            params.get("commercial_no_action_acceptance"),
            "$(arg manual_drive_no_action_acceptance)",
        )
        self.assertEqual(
            params.get("commercial_action_test_approved"),
            "$(arg manual_drive_action_test_approved)",
        )

    def test_no_action_override_disables_manual_drive_even_if_environment_enables_it(self):
        result = run_start_runtime_snippet(
            r"""
DORAEMON_NO_ACTION_ACCEPTANCE=true
DORAEMON_ACTION_TEST_APPROVED=false
ENABLE_MANUAL_DRIVE_SERVICE=true
apply_no_action_acceptance_overrides
[[ "${ENABLE_MANUAL_DRIVE_SERVICE}" == false ]]
"""
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)

    def test_action_mode_requires_platform_gate_and_rejects_business_gates(self):
        result = run_start_runtime_snippet(
            VALID_IDENTITY
            + r"""
DORAEMON_NO_ACTION_ACCEPTANCE=false
DORAEMON_ACTION_TEST_APPROVED=true
MCORE_MAX_ABS_LINEAR_VELOCITY=0.3
MCORE_MAX_ABS_ANGULAR_VELOCITY=0.5
ENABLE_MANUAL_DRIVE_SERVICE=true
business_gate_names=(
  FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_ROLE
  FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_SLAM_STATE
  FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_TASK_STATE
  FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_ODOMETRY_STATE
)
for configured_gate in "${business_gate_names[@]}"; do printf -v "${configured_gate}" '%s' false; done
FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_COMBINED_STATUS=false
if validate_commercial_vehicle_identity; then exit 20; fi
FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_COMBINED_STATUS=true
validate_commercial_vehicle_identity
for gate_name in "${business_gate_names[@]}"; do
  printf -v "${gate_name}" '%s' true
  if validate_commercial_vehicle_identity; then exit 21; fi
  printf -v "${gate_name}" '%s' false
done
"""
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)

    def test_frontend_entry_forces_no_action_off_and_rejects_incomplete_action_gate(self):
        source = read_repo_file("scripts/start_frontend_backend.sh")
        functions = "\n".join(
            (
                bash_function(source, "normalize_boolean_variable"),
                bash_function(source, "validate_manual_drive_mode"),
            )
        )
        script = (
            "set -euo pipefail\n"
            + functions
            + r"""
ENABLE_MANUAL_DRIVE_SERVICE=true
DORAEMON_NO_ACTION_ACCEPTANCE=true
DORAEMON_ACTION_TEST_APPROVED=false
FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_ROLE=false
FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_SLAM_STATE=false
FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_TASK_STATE=false
FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_ODOMETRY_STATE=false
FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_COMBINED_STATUS=false
validate_manual_drive_mode
[[ "${ENABLE_MANUAL_DRIVE_SERVICE}" == false ]]

DORAEMON_NO_ACTION_ACCEPTANCE=false
DORAEMON_ACTION_TEST_APPROVED=true
ENABLE_MANUAL_DRIVE_SERVICE=true
FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_ROLE=true
FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_SLAM_STATE=false
FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_TASK_STATE=false
FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_ODOMETRY_STATE=false
FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_COMBINED_STATUS=true
if validate_manual_drive_mode; then exit 40; fi
FRONTEND_BACKEND_MANUAL_DRIVE_REQUIRE_ROLE=false
validate_manual_drive_mode
"""
        )
        result = subprocess.run(
            ["bash", "-c", script],
            cwd=REPO_ROOT,
            text=True,
            capture_output=True,
            timeout=10,
            check=False,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn(
            'enable_manual_drive_service:="${ENABLE_MANUAL_DRIVE_SERVICE}"', source
        )

    def test_live_no_action_gate_rejects_manual_entry_and_unknown_cmd_vel_publisher(self):
        result = run_start_runtime_snippet(
            r"""
DORAEMON_NO_ACTION_ACCEPTANCE=true
NODE_OUTPUT=
SERVICE_OUTPUT=
TOPIC_OUTPUT=/cmd_vel
CMD_VEL_INFO=$'Type: geometry_msgs/Twist\n\nPublishers:\n * /coverage_executor (http://local/)\n * /move_base_flex (http://local/)\n\nSubscribers: None'
AUTO_CHARGE_VALUE=false
runtime_log_status() { :; }
runtime_run_ros_cli() {
  case "$1:$2" in
    rosnode:list) printf '%s\n' "${NODE_OUTPUT}" ;;
    rosservice:list) printf '%s\n' "${SERVICE_OUTPUT}" ;;
    rostopic:list) printf '%s\n' "${TOPIC_OUTPUT}" ;;
    rostopic:info) printf '%s\n' "${CMD_VEL_INFO}" ;;
    *) return 1 ;;
  esac
}
runtime_get_rosparam_value() { printf '%s\n' "${AUTO_CHARGE_VALUE}"; }
assert_no_action_runtime_isolated
NODE_OUTPUT=/manual_drive_service
if assert_no_action_runtime_isolated; then exit 30; fi
NODE_OUTPUT=
SERVICE_OUTPUT=/clean_robot_server/app/manual_drive_command
if assert_no_action_runtime_isolated; then exit 31; fi
SERVICE_OUTPUT=
CMD_VEL_INFO=$'Type: geometry_msgs/Twist\n\nPublishers:\n * /coverage_executor (http://local/)\n\nSubscribers:\n * /mcore_velocity_sender (http://local/)'
if assert_no_action_runtime_isolated; then exit 32; fi
CMD_VEL_INFO=$'Type: geometry_msgs/Twist\n\nPublishers:\n * /manual_drive_service (http://local/)\n\nSubscribers: None'
if assert_no_action_runtime_isolated; then exit 33; fi
"""
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)

    def test_node_requires_action_approval_and_physical_platform_gate(self):
        source = read_repo_file("src/coverage_planner/scripts/manual_drive_service_node.py")
        publisher_offset = source.index("self._cmd_pub = rospy.Publisher")
        for expected in (
            '"~commercial_no_action_acceptance"',
            '"~commercial_action_test_approved"',
            '"~require_combined_status"',
            "manual drive requires the physical platform safety gate",
        ):
            self.assertIn(expected, source[:publisher_offset])

        common = read_repo_file("scripts/runtime_common.sh")
        self.assertIn("--exclude-manual-drive", common)
        checker = read_repo_file("src/coverage_planner/tools/check_ros_contracts.py")
        self.assertIn('local_contracts.pop("manual_drive_command_app", None)', checker)
        self.assertIn('local_contracts.pop("get_manual_drive_status_app", None)', checker)


if __name__ == "__main__":
    unittest.main()

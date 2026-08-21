#!/usr/bin/env python3

import os
import subprocess
import sys
import unittest
import xml.etree.ElementTree as ET

import yaml


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(THIS_DIR)))
PKG_SRC = os.path.join(REPO_ROOT, "src", "coverage_planner", "src")
if PKG_SRC not in sys.path:
    sys.path.insert(0, PKG_SRC)

from coverage_planner.coverage_planner_core.types import PlannerParams


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


def run_start_runtime_snippet(snippet):
    env = {
        "DORAEMON_RUNTIME_CONFIG_FILE": "/dev/null",
        "HOME": os.environ.get("HOME", "/tmp"),
        "LANG": "C.UTF-8",
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
    }
    return subprocess.run(
        [
            "bash",
            "-c",
            "set -euo pipefail\nsource scripts/start_runtime.sh\n" + snippet,
        ],
        cwd=REPO_ROOT,
        env=env,
        text=True,
        capture_output=True,
        timeout=10,
        check=False,
    )


VALID_COMMERCIAL_IDENTITY = r"""
ROBOT_ID=CR-TEST
DORAEMON_A_BOX_IFACE=enp1s0
ROSBRIDGE_ADDRESS=127.0.0.1
RUNTIME_START_DEPTH_CAMERAS=true
RUNTIME_REQUIRE_DEPTH_CAMERA_TOPICS=true
DORAEMON_REQUIRE_DEPTH_CAMERA_IDENTITIES=true
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


class RuntimeParameterWiringTest(unittest.TestCase):
    def test_all_transient_sensor_obstacles_are_local_costmap_only(self):
        with open(
            os.path.join(
                REPO_ROOT,
                "src/cleanrobot/config/nav/costmap_common.yaml",
            ),
            "r",
            encoding="utf-8",
        ) as handle:
            common = yaml.safe_load(handle)
        with open(
            os.path.join(
                REPO_ROOT,
                "src/cleanrobot/config/nav/global_costmap.yaml",
            ),
            "r",
            encoding="utf-8",
        ) as handle:
            global_costmap = yaml.safe_load(handle)["global_costmap"]
        with open(
            os.path.join(
                REPO_ROOT,
                "src/cleanrobot/config/nav/local_costmap.yaml",
            ),
            "r",
            encoding="utf-8",
        ) as handle:
            local_costmap = yaml.safe_load(handle)["local_costmap"]

        camera_sources = {
            "left_cam_source",
            "right_cam_source",
            "up_cam_source",
        }
        self.assertEqual(
            common["obstacle_layer"]["observation_sources"].split(),
            ["laser_scan_sensor"],
        )
        self.assertTrue(
            camera_sources.isdisjoint(common["obstacle_layer"].keys())
        )
        self.assertNotIn("obstacle_layer", global_costmap)
        global_plugins = {
            plugin["name"] for plugin in global_costmap["plugins"]
        }
        self.assertNotIn("obstacle_layer", global_plugins)
        self.assertEqual(
            global_plugins,
            {"static_layer", "keepout_constraint_layer", "inflation_layer"},
        )

        self.assertEqual(
            local_costmap["obstacle_layer"]["observation_sources"].split(),
            [
                "laser_scan_sensor",
                "left_cam_source",
                "right_cam_source",
                "up_cam_source",
            ],
        )
        self.assertNotIn("camera_obstacle_layer", local_costmap)
        for source in camera_sources:
            with self.subTest(source=source):
                source_config = local_costmap["obstacle_layer"][source]
                self.assertEqual(source_config["data_type"], "PointCloud2")
                self.assertTrue(source_config["marking"])
                self.assertFalse(source_config["clearing"])
                self.assertNotIn("observation_ttl", source_config)

        launch_root = ET.parse(
            os.path.join(
                REPO_ROOT,
                "src/cleanrobot/launch/mbf_nav.launch",
            )
        ).getroot()
        mbf_node = launch_root.find("./node[@name='move_base_flex']")
        self.assertIsNotNone(mbf_node)
        launch_params = {
            param.get("name") for param in mbf_node.findall("param")
        }
        for source in camera_sources:
            with self.subTest(launch_source=source):
                suffix = "%s/topic" % source
                self.assertIn(
                    "local_costmap/obstacle_layer/" + suffix,
                    launch_params,
                )
                self.assertNotIn(
                    "global_costmap/obstacle_layer/" + suffix,
                    launch_params,
                )

    def test_map_constraints_receives_commercial_robot_id(self):
        launch_root = ET.parse(
            os.path.join(
                REPO_ROOT,
                "src/cleanrobot/launch/mbf_nav.launch",
            )
        ).getroot()
        map_constraints = launch_root.find("./node[@name='map_constraints']")
        self.assertIsNotNone(map_constraints)
        params = {
            item.get("name"): item.get("value")
            for item in map_constraints.findall("param")
        }
        self.assertEqual(params.get("robot_id"), "$(arg robot_id)")
        self.assertEqual(params.get("constraints_topic"), "/map_constraints/current")
        self.assertEqual(
            params.get("effective_constraints_topic"),
            "/map_constraints/effective",
        )
        self.assertNotEqual(
            params.get("constraints_topic"),
            params.get("effective_constraints_topic"),
        )
        launch_args = {
            item.get("name"): item.get("default")
            for item in launch_root.findall("arg")
        }
        self.assertEqual(
            launch_args.get("planner_defaults_yaml"),
            "$(find coverage_planner)/config/planner_server_defaults.yaml",
        )
        shared_defaults = map_constraints.find(
            "./rosparam[@command='load'][@file='$(arg planner_defaults_yaml)']"
        )
        self.assertIsNotNone(shared_defaults)

        with open(
            os.path.join(
                REPO_ROOT,
                "src/cleanrobot/config/nav/costmap_common.yaml",
            ),
            "r",
            encoding="utf-8",
        ) as handle:
            costmap = yaml.safe_load(handle)
        self.assertEqual(
            costmap["keepout_constraint_layer"]["constraints_topic"],
            "/map_constraints/current",
        )

    def test_all_constraint_compilers_load_shared_defaults(self):
        launch_root = ET.parse(
            os.path.join(
                REPO_ROOT,
                "src/coverage_planner/launch/planner_server.launch",
            )
        ).getroot()
        for node_name in (
            "coverage_planner_server",
            "rect_zone_planner",
            "site_editor_service",
        ):
            with self.subTest(node_name=node_name):
                node = launch_root.find(".//node[@name='%s']" % node_name)
                self.assertIsNotNone(node)
                shared_defaults = node.find(
                    "./rosparam[@command='load'][@file='$(arg planner_defaults_yaml)']"
                )
                self.assertIsNotNone(shared_defaults)

    def test_edge_stitch_reference_windows_match_core_defaults(self):
        defaults_path = os.path.join(
            REPO_ROOT,
            "src/coverage_planner/config/planner_server_defaults.yaml",
        )
        with open(defaults_path, "r", encoding="utf-8") as handle:
            planner = yaml.safe_load(handle)["planner"]

        expected = {
            "pre_proj_min": 0.90,
            "pre_proj_max": 1.00,
            "pre_prefix_max": 1.40,
            "e_pre_min": 0.90,
            "e_pre_max": 1.00,
        }
        core_defaults = PlannerParams()
        for name, value in expected.items():
            with self.subTest(name=name):
                self.assertAlmostEqual(float(planner[name]), value, places=9)
                self.assertAlmostEqual(
                    float(getattr(core_defaults, name)),
                    value,
                    places=9,
                )

        self.assertLessEqual(planner["pre_proj_min"], planner["pre_proj_max"])
        self.assertGreaterEqual(
            planner["pre_prefix_max"],
            planner["pre_proj_max"],
        )
        self.assertLessEqual(planner["e_pre_min"], planner["e_pre_max"])

    def test_verified_coverage_clearance_defaults_are_consistent(self):
        defaults_path = os.path.join(
            REPO_ROOT,
            "src/coverage_planner/config/planner_server_defaults.yaml",
        )
        with open(defaults_path, "r", encoding="utf-8") as handle:
            defaults = yaml.safe_load(handle)
        with open(
            os.path.join(
                REPO_ROOT,
                "src/cleanrobot/config/nav/costmap_common.yaml",
            ),
            "r",
            encoding="utf-8",
        ) as handle:
            costmap = yaml.safe_load(handle)

        planner = defaults["planner"]
        expected = {
            "wall_margin_m": 0.38,
            "edge_corner_radius_m": 0.45,
            "edge_corner_pull": 0.20,
        }
        core_defaults = PlannerParams()
        for name, value in expected.items():
            with self.subTest(name=name):
                self.assertAlmostEqual(float(planner[name]), value, places=9)
                self.assertAlmostEqual(
                    float(getattr(core_defaults, name)),
                    value,
                    places=9,
                )

        self.assertAlmostEqual(
            float(defaults["default_no_go_long_edge_normal_buffer_m"]),
            0.115,
            places=9,
        )
        self.assertAlmostEqual(
            float(defaults["default_no_go_short_edge_normal_buffer_m"]),
            0.115,
            places=9,
        )
        self.assertIs(planner["validate_effective_region_path"], True)
        self.assertAlmostEqual(float(costmap["footprint_padding"]), 0.0, places=9)
        self.assertAlmostEqual(float(defaults["robot"]["cov_width"]), 0.59, places=9)
        self.assertAlmostEqual(
            float(costmap["inflation_layer"]["cost_scaling_factor"]),
            10.0,
            places=9,
        )
        self.assertAlmostEqual(
            float(costmap["inflation_layer"]["inflation_radius"]),
            1.0,
            places=9,
        )

    def test_theta_star_keeps_connect_paths_outside_near_keepout_costs(self):
        with open(
            os.path.join(
                REPO_ROOT,
                "src/cleanrobot/config/nav/mbf_nav.yaml",
            ),
            "r",
            encoding="utf-8",
        ) as handle:
            theta_star = yaml.safe_load(handle)["ThetaStarPlanner"]

        self.assertEqual(int(theta_star["max_allowed_cost"]), 26)
        self.assertAlmostEqual(
            float(theta_star["w_traversal_cost"]),
            8.0,
            places=9,
        )
        self.assertTrue(bool(theta_star["use_footprint_path_check"]))
        self.assertTrue(bool(theta_star["se2_refinement_enabled"]))
        self.assertTrue(bool(theta_star["terminal_approach_enabled"]))
        self.assertAlmostEqual(
            float(theta_star["terminal_straight_length"]), 0.40, places=9
        )
        self.assertAlmostEqual(
            float(theta_star["terminal_min_straight_length"]), 0.0, places=9
        )
        self.assertAlmostEqual(
            float(theta_star["terminal_straight_length_step"]), 0.10, places=9
        )
        self.assertAlmostEqual(
            float(theta_star["terminal_min_turn_radius"]), 0.0, places=9
        )
        self.assertAlmostEqual(
            float(theta_star["terminal_sample_step"]), 0.05, places=9
        )
        self.assertAlmostEqual(
            float(theta_star["terminal_max_prefix_splice_distance"]),
            1.80,
            places=9,
        )

    def test_odometry_vehicle_parameters_are_explicit_launch_arguments(self):
        source = read_repo_file("scripts/start_runtime.sh")
        block_start = source.index('if [[ "${START_WHEEL_ODOM}" == "true" ]]')
        block_end = source.index(
            'if [[ "${START_MCORE_VELOCITY_SENDER}" == "true" ]]', block_start
        )
        block = source[block_start:block_end]
        expected_bindings = {
            "serial_device": "ODOM_SERIAL_DEVICE",
            "serial_baudrate": "ODOM_SERIAL_BAUDRATE",
            "protocol_mode": "ODOM_PROTOCOL_MODE",
            "use_device_timestamp": "ODOM_USE_DEVICE_TIMESTAMP",
            "publish_raw_odom_tf": "ODOM_PUBLISH_RAW_ODOM_TF",
            "frame_id": "ODOM_FRAME_ID",
            "child_frame_id": "ODOM_CHILD_FRAME_ID",
            "wheel_separation": "ODOM_WHEEL_SEPARATION",
            "wheel_diameter": "ODOM_WHEEL_DIAMETER",
            "gear_ratio": "ODOM_GEAR_RATIO",
            "encoder_pulses_per_motor_revolution": "ODOM_ENCODER_PPR",
            "left_encoder_sign": "ODOM_LEFT_ENCODER_SIGN",
            "right_encoder_sign": "ODOM_RIGHT_ENCODER_SIGN",
            "left_wheel_scale": "ODOM_LEFT_WHEEL_SCALE",
            "right_wheel_scale": "ODOM_RIGHT_WHEEL_SCALE",
            "angular_velocity_sign": "ODOM_ANGULAR_VELOCITY_SIGN",
        }
        launch_root = ET.parse(
            os.path.join(
                REPO_ROOT,
                "src/wheel_speed_odom_bridge/launch/wheel_speed_odom.launch",
            )
        ).getroot()
        launch_args = {node.get("name") for node in launch_root.findall("arg")}
        for launch_name, variable_name in expected_bindings.items():
            with self.subTest(launch_name=launch_name):
                self.assertIn(launch_name, launch_args)
                self.assertIn(
                    '%s:="${%s}"' % (launch_name, variable_name), block
                )

        self.assertLess(
            block.index('append_shell_words odom_cmd_words "${WHEEL_ODOM_EXTRA_ARGS}"'),
            block.index('serial_device:="${ODOM_SERIAL_DEVICE}"'),
        )

    def test_mcore_vehicle_parameters_are_explicit_launch_arguments(self):
        source = read_repo_file("scripts/start_runtime.sh")
        block_start = source.index(
            'if [[ "${START_MCORE_VELOCITY_SENDER}" == "true" ]]'
        )
        block_end = source.index(
            'if [[ "${START_MCORE_BRIDGE}" == "true"', block_start
        )
        block = source[block_start:block_end]
        expected_bindings = {
            "transport": "MCORE_TRANSPORT",
            "serial_device": "MCORE_SERIAL_DEVICE",
            "serial_baudrate": "MCORE_SERIAL_BAUDRATE",
            "tcp_host": "MCORE_TCP_HOST",
            "tcp_port": "MCORE_TCP_PORT",
            "cmd_vel_topic": "MCORE_CMD_VEL_TOPIC",
            "linear_velocity_scale": "MCORE_LINEAR_VELOCITY_SCALE",
            "angular_velocity_scale": "MCORE_ANGULAR_VELOCITY_SCALE",
            "linear_velocity_sign": "MCORE_LINEAR_VELOCITY_SIGN",
            "angular_velocity_sign": "MCORE_ANGULAR_VELOCITY_SIGN",
            "max_abs_linear_velocity": "MCORE_MAX_ABS_LINEAR_VELOCITY",
            "max_abs_angular_velocity": "MCORE_MAX_ABS_ANGULAR_VELOCITY",
            "enable_tx_log": "MCORE_ENABLE_TX_LOG",
            "enable_rx_log": "MCORE_ENABLE_RX_LOG",
        }
        launch_root = ET.parse(
            os.path.join(
                REPO_ROOT,
                "src/mcore_chassis_bridge/launch/mcore_velocity_sender.launch",
            )
        ).getroot()
        launch_args = {node.get("name") for node in launch_root.findall("arg")}
        for launch_name, variable_name in expected_bindings.items():
            with self.subTest(launch_name=launch_name):
                self.assertIn(launch_name, launch_args)
                self.assertIn(
                    '%s:="${%s}"' % (launch_name, variable_name), block
                )

        self.assertLess(
            block.index(
                'append_shell_words mcore_velocity_cmd_words "${MCORE_VELOCITY_EXTRA_ARGS}"'
            ),
            block.index('transport:="${MCORE_TRANSPORT}"'),
        )

    def test_vehicle_extra_args_cannot_override_wired_parameters(self):
        script = VALID_COMMERCIAL_IDENTITY + r"""
DORAEMON_NO_ACTION_ACCEPTANCE=true
DORAEMON_ACTION_TEST_APPROVED=false
odom_protected=(
  serial_device serial_baudrate protocol_mode use_device_timestamp
  publish_raw_odom_tf frame_id child_frame_id wheel_separation wheel_diameter
  gear_ratio encoder_pulses_per_motor_revolution left_encoder_sign
  right_encoder_sign left_wheel_scale right_wheel_scale angular_velocity_sign
)
for name in "${odom_protected[@]}"; do
  WHEEL_ODOM_EXTRA_ARGS="${name}:=OVERRIDE"
  if validate_commercial_vehicle_identity; then
    exit 20
  fi
done
WHEEL_ODOM_EXTRA_ARGS=
mcore_protected=(
  transport serial_device serial_baudrate tcp_host tcp_port cmd_vel_topic
  linear_velocity_scale angular_velocity_scale linear_velocity_sign
  angular_velocity_sign max_abs_linear_velocity max_abs_angular_velocity
  enable_tx_log enable_rx_log
)
for name in "${mcore_protected[@]}"; do
  MCORE_VELOCITY_EXTRA_ARGS="${name}:=OVERRIDE"
  if validate_commercial_vehicle_identity; then
    exit 21
  fi
done
MCORE_VELOCITY_EXTRA_ARGS=
validate_commercial_vehicle_identity
"""
        result = run_start_runtime_snippet(script)
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn(
            "WHEEL_ODOM_EXTRA_ARGS may not override protected vehicle argument",
            result.stderr,
        )
        self.assertIn(
            "MCORE_VELOCITY_EXTRA_ARGS may not override protected vehicle argument",
            result.stderr,
        )

    def test_action_mode_requires_positive_finite_velocity_limits(self):
        script = VALID_COMMERCIAL_IDENTITY + r"""
DORAEMON_NO_ACTION_ACCEPTANCE=false
DORAEMON_ACTION_TEST_APPROVED=true
MCORE_MAX_ABS_ANGULAR_VELOCITY=1.2
for invalid in 0 -0.1 nan inf 1e309; do
  MCORE_MAX_ABS_LINEAR_VELOCITY="${invalid}"
  if validate_commercial_vehicle_identity; then
    exit 30
  fi
done
MCORE_MAX_ABS_LINEAR_VELOCITY=0.4
for invalid in 0 -0.1 nan inf 1e309; do
  MCORE_MAX_ABS_ANGULAR_VELOCITY="${invalid}"
  if validate_commercial_vehicle_identity; then
    exit 31
  fi
done
MCORE_MAX_ABS_ANGULAR_VELOCITY=1.2
validate_commercial_vehicle_identity
"""
        result = run_start_runtime_snippet(script)
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn(
            "action-capable runtime requires MCORE_MAX_ABS_LINEAR_VELOCITY "
            "to be finite and > 0",
            result.stderr,
        )
        self.assertIn(
            "action-capable runtime requires MCORE_MAX_ABS_ANGULAR_VELOCITY "
            "to be finite and > 0",
            result.stderr,
        )

    def test_action_mode_rejects_invalid_velocity_scales_and_signs(self):
        script = VALID_COMMERCIAL_IDENTITY + r"""
DORAEMON_NO_ACTION_ACCEPTANCE=false
DORAEMON_ACTION_TEST_APPROVED=true
MCORE_MAX_ABS_LINEAR_VELOCITY=0.35
MCORE_MAX_ABS_ANGULAR_VELOCITY=0.6
MCORE_LINEAR_VELOCITY_SCALE=1000.0
MCORE_ANGULAR_VELOCITY_SCALE=1000.0
MCORE_LINEAR_VELOCITY_SIGN=1.0
MCORE_ANGULAR_VELOCITY_SIGN=1.0
for invalid in 0 -1 nan inf 1e309; do
  MCORE_LINEAR_VELOCITY_SCALE="${invalid}"
  if validate_commercial_vehicle_identity; then exit 40; fi
done
MCORE_LINEAR_VELOCITY_SCALE=1000.0
for invalid in 0 -1 nan inf 1e309; do
  MCORE_ANGULAR_VELOCITY_SCALE="${invalid}"
  if validate_commercial_vehicle_identity; then exit 41; fi
done
MCORE_ANGULAR_VELOCITY_SCALE=1000.0
for invalid in 0 0.5 -0.5 2 nan inf; do
  MCORE_LINEAR_VELOCITY_SIGN="${invalid}"
  if validate_commercial_vehicle_identity; then exit 42; fi
done
MCORE_LINEAR_VELOCITY_SIGN=-1.0
for invalid in 0 0.5 -0.5 2 nan inf; do
  MCORE_ANGULAR_VELOCITY_SIGN="${invalid}"
  if validate_commercial_vehicle_identity; then exit 43; fi
done
MCORE_ANGULAR_VELOCITY_SIGN=1.0
validate_commercial_vehicle_identity
"""
        result = run_start_runtime_snippet(script)
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn(
            "action-capable runtime requires MCORE_LINEAR_VELOCITY_SCALE "
            "to be finite and > 0",
            result.stderr,
        )
        self.assertIn(
            "action-capable runtime requires MCORE_ANGULAR_VELOCITY_SIGN "
            "to be exactly -1 or +1",
            result.stderr,
        )

    def test_mcore_tcp_action_mode_requires_exactly_one_motion_transport(self):
        script = VALID_COMMERCIAL_IDENTITY + r"""
DORAEMON_NO_ACTION_ACCEPTANCE=false
DORAEMON_ACTION_TEST_APPROVED=true
CHASSIS_DRIVER=mcore_tcp
MCORE_MAX_ABS_LINEAR_VELOCITY=0.35
MCORE_MAX_ABS_ANGULAR_VELOCITY=0.6
MCORE_LINEAR_VELOCITY_SCALE=1000.0
MCORE_ANGULAR_VELOCITY_SCALE=1000.0
MCORE_LINEAR_VELOCITY_SIGN=1.0
MCORE_ANGULAR_VELOCITY_SIGN=1.0
START_MCORE_VELOCITY_SENDER=true
START_MCORE_BRIDGE=false
MCORE_ENABLE_CMD_VEL=false
validate_commercial_vehicle_identity
START_MCORE_BRIDGE=true
MCORE_ENABLE_CMD_VEL=true
if validate_commercial_vehicle_identity; then exit 50; fi
START_MCORE_BRIDGE=false
MCORE_ENABLE_CMD_VEL=false
START_MCORE_VELOCITY_SENDER=false
if validate_commercial_vehicle_identity; then exit 51; fi
"""
        result = run_start_runtime_snippet(script)
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn(
            "mcore_tcp action mode requires exactly one motion transport",
            result.stderr,
        )

    def test_no_action_mode_keeps_zero_limit_template_and_disables_sender(self):
        defaults = read_env_defaults("config/runtime.a26022.env")
        self.assertEqual(defaults.get("MCORE_MAX_ABS_LINEAR_VELOCITY"), "0.0")
        self.assertEqual(defaults.get("MCORE_MAX_ABS_ANGULAR_VELOCITY"), "0.0")
        self.assertEqual(defaults.get("ODOM_FRAME_ID"), "odom")
        self.assertEqual(defaults.get("ODOM_CHILD_FRAME_ID"), "base_footprint")

        script = VALID_COMMERCIAL_IDENTITY + r"""
DORAEMON_NO_ACTION_ACCEPTANCE=true
DORAEMON_ACTION_TEST_APPROVED=false
MCORE_MAX_ABS_LINEAR_VELOCITY=0.0
MCORE_MAX_ABS_ANGULAR_VELOCITY=0.0
START_MCORE_VELOCITY_SENDER=true
validate_commercial_vehicle_identity
apply_no_action_acceptance_overrides
[[ "${START_MCORE_VELOCITY_SENDER}" == false ]]
"""
        result = run_start_runtime_snippet(script)
        self.assertEqual(result.returncode, 0, msg=result.stderr)


if __name__ == "__main__":
    unittest.main()

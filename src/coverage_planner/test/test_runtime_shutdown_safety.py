#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import subprocess
import tempfile
import time
import unittest
import uuid


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(THIS_DIR)))


def read_repo_file(relative_path):
    with open(os.path.join(REPO_ROOT, relative_path), "r", encoding="utf-8") as handle:
        return handle.read()


def bash_function(source, name):
    start = source.index("%s() {" % name)
    end = source.index("\n}\n", start) + 3
    return source[start:end]


def assert_in_order(test_case, source, tokens):
    cursor = -1
    for token in tokens:
        offset = source.find(token, cursor + 1)
        test_case.assertGreater(offset, cursor, msg="missing or out-of-order token: %s" % token)
        cursor = offset


class RuntimeShutdownSafetyTest(unittest.TestCase):
    def test_runtime_stop_scripts_keep_tmux_until_nodes_exit(self):
        expected = (
            "runtime_graceful_stop_runtime",
            "runtime_kill_runtime_nodes",
            "runtime_kill_runtime_processes",
            "runtime_kill_runtime_tmux_sessions",
        )
        for relative_path in ("scripts/stop_runtime.sh", "scripts/stop_all_backend.sh"):
            with self.subTest(relative_path=relative_path):
                assert_in_order(self, bash_function(read_repo_file(relative_path), "main"), expected)

    def test_repeat_start_cleanup_keeps_tmux_until_nodes_exit(self):
        function = bash_function(read_repo_file("scripts/start_runtime.sh"), "clear_previous_runtime")
        assert_in_order(
            self,
            function,
            (
                "runtime_graceful_stop_runtime",
                "runtime_kill_runtime_nodes",
                "runtime_kill_runtime_processes",
                "runtime_kill_runtime_tmux_sessions",
            ),
        )

    def test_repeat_start_cleanup_removes_orphan_mcore_sender_before_tmux_cleanup(self):
        common = read_repo_file("scripts/runtime_common.sh")
        self.assertIn('"/mcore_velocity_sender"', bash_function(common, "runtime_kill_runtime_nodes"))
        self.assertIn(
            '"mcore_velocity_sender_node"',
            bash_function(common, "runtime_kill_runtime_processes"),
        )

        token = "mcore_velocity_sender_node-%s" % uuid.uuid4().hex
        child = subprocess.Popen(
            [
                "bash",
                "-c",
                "trap '' TERM; while :; do sleep 1; done",
                token,
            ]
        )
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                pgrep = os.path.join(temp_dir, "pgrep")
                with open(pgrep, "w", encoding="utf-8") as handle:
                    handle.write(
                        "#!/usr/bin/env bash\n"
                        "if [[ \"${!#}\" == mcore_velocity_sender_node ]]; then\n"
                        "  printf '%s\\n' \"${DORAEMON_TEST_MCORE_PID}\"\n"
                        "  exit 0\n"
                        "fi\n"
                        "exit 1\n"
                    )
                os.chmod(pgrep, 0o755)

                script = r"""
set -euo pipefail
export DORAEMON_RUNTIME_CONFIG_FILE=/dev/null
source scripts/start_runtime.sh
source scripts/runtime_common.sh
runtime_log_status() { :; }
runtime_graceful_stop_runtime() { :; }
runtime_kill_runtime_nodes() { :; }
runtime_cleanup_ros_nodes() { :; }
runtime_kill_runtime_tmux_sessions() {
  if [[ -r "/proc/${DORAEMON_TEST_MCORE_PID}/stat" ]]; then
    state="$(awk '{print $3}' "/proc/${DORAEMON_TEST_MCORE_PID}/stat")"
    if [[ "${state}" != Z ]]; then
      echo "mcore sender was still live when tmux cleanup began" >&2
      return 41
    fi
  fi
}
clear_previous_runtime
"""
                env = os.environ.copy()
                env["PATH"] = temp_dir + os.pathsep + env.get("PATH", "")
                env["DORAEMON_TEST_MCORE_PID"] = str(child.pid)
                env["DORAEMON_SHUTDOWN_PROCESS_BUDGET_SEC"] = "1"
                result = subprocess.run(
                    ["bash", "-c", script],
                    cwd=REPO_ROOT,
                    env=env,
                    text=True,
                    capture_output=True,
                    timeout=6,
                    check=False,
                )
                self.assertEqual(result.returncode, 0, msg=result.stderr)
                child.wait(timeout=2)
        finally:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=2)

    def test_repeat_start_cleanup_removes_orphan_auto_charge_monitor_before_tmux_cleanup(self):
        common = read_repo_file("scripts/runtime_common.sh")
        self.assertIn('"/auto_charge_monitor"', bash_function(common, "runtime_kill_runtime_nodes"))
        process_cleanup = bash_function(common, "runtime_kill_runtime_processes")
        self.assertIn('"auto_charge_monitor.launch"', process_cleanup)
        self.assertIn('"auto_charge_monitor_node.py"', process_cleanup)

        token = "auto_charge_monitor_node.py-%s" % uuid.uuid4().hex
        child = subprocess.Popen(
            [
                "bash",
                "-c",
                "trap '' TERM; while :; do sleep 1; done",
                token,
            ]
        )
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                pgrep = os.path.join(temp_dir, "pgrep")
                with open(pgrep, "w", encoding="utf-8") as handle:
                    handle.write(
                        "#!/usr/bin/env bash\n"
                        "if [[ \"${!#}\" == auto_charge_monitor_node.py ]]; then\n"
                        "  printf '%s\\n' \"${DORAEMON_TEST_AUTO_CHARGE_PID}\"\n"
                        "  exit 0\n"
                        "fi\n"
                        "exit 1\n"
                    )
                os.chmod(pgrep, 0o755)

                script = r"""
set -euo pipefail
export DORAEMON_RUNTIME_CONFIG_FILE=/dev/null
source scripts/start_runtime.sh
source scripts/runtime_common.sh
runtime_log_status() { :; }
runtime_graceful_stop_runtime() { :; }
runtime_kill_runtime_nodes() { :; }
runtime_cleanup_ros_nodes() { :; }
runtime_kill_runtime_tmux_sessions() {
  if [[ -r "/proc/${DORAEMON_TEST_AUTO_CHARGE_PID}/stat" ]]; then
    state="$(awk '{print $3}' "/proc/${DORAEMON_TEST_AUTO_CHARGE_PID}/stat")"
    if [[ "${state}" != Z ]]; then
      echo "auto-charge monitor was still live when tmux cleanup began" >&2
      return 42
    fi
  fi
}
clear_previous_runtime
"""
                env = os.environ.copy()
                env["PATH"] = temp_dir + os.pathsep + env.get("PATH", "")
                env["DORAEMON_TEST_AUTO_CHARGE_PID"] = str(child.pid)
                env["DORAEMON_SHUTDOWN_PROCESS_BUDGET_SEC"] = "1"
                result = subprocess.run(
                    ["bash", "-c", script],
                    cwd=REPO_ROOT,
                    env=env,
                    text=True,
                    capture_output=True,
                    timeout=6,
                    check=False,
                )
                self.assertEqual(result.returncode, 0, msg=result.stderr)
                child.wait(timeout=2)
        finally:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=2)

    def test_frontend_cleanup_kills_tmux_last(self):
        source = read_repo_file("scripts/runtime_common.sh")
        function = bash_function(source, "runtime_stop_frontend_services")
        self.assertGreater(
            function.rfind("runtime_kill_tmux_session_if_exists"),
            function.rfind("runtime_cleanup_ros_nodes"),
        )
        ensure = bash_function(source, "runtime_ensure_frontend_service_session")
        self.assertIn("runtime_stop_frontend_services", ensure)
        self.assertNotIn("tmux kill-session", ensure)

    def test_failed_startup_has_transactional_cleanup(self):
        source = read_repo_file("scripts/start_runtime.sh")
        self.assertIn('LOG_DIR="${LOG_DIR:-/var/log/doraemon/startup}"', source)
        self.assertIn("validate_external_runtime_log_paths", source)
        self.assertIn("trap cleanup_failed_startup EXIT", source)
        self.assertIn("trap 'exit 143' TERM", source)
        self.assertIn("trap 'exit 130' INT", source)
        self.assertIn('STOP_MASTER=1 "${SCRIPT_DIR}/stop_all_backend.sh"', source)
        self.assertIn("STARTUP_COMMITTED=1", source)
        self.assertIn("STARTUP_TRANSACTION_STARTED=1", source)
        self.assertIn("STARTUP_TRANSACTION_STARTED == 1", source)

    def test_systemd_start_failure_is_fail_closed(self):
        for relative_path in (
            "deploy/systemd/doraemon-runtime.service",
            "scripts/install_doraemon_runtime_service.sh",
        ):
            with self.subTest(relative_path=relative_path):
                source = read_repo_file(relative_path)
                self.assertIn("Restart=no", source)
                self.assertNotIn("Restart=on-failure", source)
                self.assertNotIn("RestartSec=", source)
                self.assertIn("KillMode=mixed", source)
                self.assertIn("ExecStopPost=", source)

        verifier = read_repo_file("scripts/verify_x86_ubuntu20_deployment.sh")
        for expected in (
            "installed runtime Restart=no",
            "installed runtime KillMode=mixed",
            "runtime service must remain disabled",
            "runtime service must be inactive",
            "runtime NRestarts must be zero",
            "SLAM runtime log directory ownership and mode",
            '\\( -type f -o -type d \\) -perm /022',
        ):
            self.assertIn(expected, verifier)

    def test_installer_requires_stopped_service_and_leaves_default_disabled(self):
        source = read_repo_file("scripts/install_doraemon_runtime_service.sh")
        self.assertIn("PREINSTALL_ACTIVE_STATE", source)
        self.assertIn("must be exactly inactive and disabled before installation", source)
        self.assertIn('systemctl disable "${SERVICE_NAME}"', source)
        self.assertIn('systemctl reset-failed "${SERVICE_NAME}"', source)
        self.assertIn('systemctl is-enabled "${SERVICE_NAME}"', source)
        self.assertIn('systemctl is-active "${SERVICE_NAME}"', source)
        self.assertIn("service is disabled and inactive", source)
        self.assertIn("/var/lib/doraemon/orbbec-captures", source)

    def test_failed_service_cleanup_is_result_gated(self):
        source = read_repo_file("scripts/cleanup_failed_runtime_service.sh")
        self.assertIn("SERVICE_RESULT", source)
        self.assertIn("stop_all_backend.sh", source)

    def test_backend_does_not_auto_start_site_gateway(self):
        common = read_repo_file("scripts/runtime_common.sh")
        start = read_repo_file("scripts/start_runtime.sh")
        config = read_repo_file("config/runtime.a26022.env")
        self.assertIn('RESTART_SITE_GATEWAY_AFTER_ROSBRIDGE:-false', common)
        self.assertIn('RESTART_SITE_GATEWAY_AFTER_ROSBRIDGE="${RESTART_SITE_GATEWAY_AFTER_ROSBRIDGE:-false}"', start)
        self.assertIn("RESTART_SITE_GATEWAY_AFTER_ROSBRIDGE=false", config)

    def test_no_action_acceptance_overrides_automatic_actions(self):
        source = read_repo_file("scripts/start_runtime.sh")
        config = read_repo_file("config/runtime.a26022.env")
        self.assertIn("DORAEMON_NO_ACTION_ACCEPTANCE=true", config)
        self.assertIn("DORAEMON_ACTION_TEST_APPROVED=false", config)
        self.assertIn("action_test_approved=${DORAEMON_ACTION_TEST_APPROVED}", source)
        function = bash_function(source, "apply_no_action_acceptance_overrides")
        for expected in (
            "TASK_AUTO_CHARGE_ENABLE=false",
            "EXECUTOR_AUTO_CHARGE_ENABLE=false",
            "AUTO_CHARGE_MONITOR_ENABLE=false",
            "MCORE_ENABLE_CMD_VEL=false",
            "START_MCORE_BRIDGE=false",
            "START_MCORE_VELOCITY_SENDER=false",
            "START_WHEELTEC_BASE=false",
            "START_STATION_BRIDGE=false",
            "START_DOCK_SUPPLY_MANAGER=false",
            "START_DOCKING_STACK=false",
            "DOCK_SUPPLY_ENABLE_DRAIN=false",
            "CHARGE_VOLTAGE_CONFIRM_ENABLE=false",
            "RESTART_SITE_GATEWAY_AFTER_ROSBRIDGE=false",
        ):
            self.assertIn(expected, function)

        extra_offset = source.index(
            'append_shell_words hardware_cmd_words "${HARDWARE_BRIDGES_EXTRA_ARGS}"'
        )
        protected_tail = source[extra_offset:]
        for expected in (
            "enable_mcore_bridge:=false",
            "mcore_enable_cmd_vel:=false",
            "enable_station_bridge:=false",
            "enable_dock_supply_manager:=false",
            "enable_docking_stack:=false",
            "mechanical_connect_enable:=false",
            "direct_charge_after_precise_docking:=false",
            "dock_supply_enable_drain:=false",
            "dock_supply_enable_refill:=false",
            "charge_voltage_confirm_enable:=false",
        ):
            self.assertIn(expected, protected_tail)

    def test_no_action_validator_rejects_hardware_extra_arg_bypass(self):
        script = r"""
set -euo pipefail
export DORAEMON_RUNTIME_CONFIG_FILE=/dev/null
source scripts/start_runtime.sh
ROBOT_ID=CR-TEST
DORAEMON_A_BOX_IFACE=enp1s0
DORAEMON_NO_ACTION_ACCEPTANCE=true
DORAEMON_ACTION_TEST_APPROVED=false
RUNTIME_START_DEPTH_CAMERAS=true
RUNTIME_REQUIRE_DEPTH_CAMERA_TOPICS=true
DORAEMON_REQUIRE_DEPTH_CAMERA_IDENTITIES=true
RUNTIME_ORBBEC_CAMERA1_SERIAL_NUMBER=S1
RUNTIME_ORBBEC_CAMERA2_SERIAL_NUMBER=S2
RUNTIME_ORBBEC_CAMERA3_SERIAL_NUMBER=S3
RUNTIME_ORBBEC_CAMERA1_USB_PORT=1-1
RUNTIME_ORBBEC_CAMERA2_USB_PORT=1-2
RUNTIME_ORBBEC_CAMERA3_USB_PORT=1-3
for protected_name in \
  enable_mcore_bridge \
  mcore_enable_cmd_vel \
  enable_station_bridge \
  enable_dock_supply_manager \
  enable_docking_stack \
  mechanical_connect_enable \
  direct_charge_after_precise_docking \
  dock_supply_enable_drain \
  dock_supply_enable_refill \
  charge_voltage_confirm_enable; do
  HARDWARE_BRIDGES_EXTRA_ARGS="${protected_name}:=true"
  if validate_commercial_vehicle_identity; then
    exit 9
  fi
done
HARDWARE_BRIDGES_EXTRA_ARGS=enable_mcore_bridge:=true
DORAEMON_NO_ACTION_ACCEPTANCE=false
DORAEMON_ACTION_TEST_APPROVED=true
MCORE_MAX_ABS_LINEAR_VELOCITY=0.4
MCORE_MAX_ABS_ANGULAR_VELOCITY=1.2
validate_commercial_vehicle_identity
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

    def test_no_action_validator_rejects_write_acceptance_bypass(self):
        script = r"""
set -euo pipefail
export DORAEMON_RUNTIME_CONFIG_FILE=/dev/null
source scripts/start_runtime.sh
ROBOT_ID=CR-TEST
DORAEMON_A_BOX_IFACE=enp1s0
DORAEMON_NO_ACTION_ACCEPTANCE=true
DORAEMON_ACTION_TEST_APPROVED=false
RUNTIME_START_DEPTH_CAMERAS=true
RUNTIME_REQUIRE_DEPTH_CAMERA_TOPICS=true
DORAEMON_REQUIRE_DEPTH_CAMERA_IDENTITIES=true
RUNTIME_ORBBEC_CAMERA1_SERIAL_NUMBER=S1
RUNTIME_ORBBEC_CAMERA2_SERIAL_NUMBER=S2
RUNTIME_ORBBEC_CAMERA3_SERIAL_NUMBER=S3
RUNTIME_ORBBEC_CAMERA1_USB_PORT=1-1
RUNTIME_ORBBEC_CAMERA2_USB_PORT=1-2
RUNTIME_ORBBEC_CAMERA3_USB_PORT=1-3

BACKEND_RUNTIME_SMOKE_ACTIONS=verify_map_revision
if validate_commercial_vehicle_identity; then exit 9; fi
BACKEND_RUNTIME_SMOKE_ACTIONS=

for unsafe in '--actions verify_map_revision' '--actions=verify_map_revision' '--run-task-cycle' '--allow-write-actions'; do
  BACKEND_RUNTIME_SMOKE_EXTRA_ARGS="${unsafe}"
  if validate_commercial_vehicle_identity; then exit 10; fi
done
BACKEND_RUNTIME_SMOKE_EXTRA_ARGS=

RUN_BACKEND_PRODUCTION_ACCEPTANCE=1
BACKEND_PRODUCTION_ACCEPTANCE_PROFILE=activate_revision_gate
if validate_commercial_vehicle_identity; then exit 11; fi
BACKEND_PRODUCTION_ACCEPTANCE_PROFILE=read_only_gate
BACKEND_PRODUCTION_ACCEPTANCE_ALLOW_WRITE_ACTIONS=1
if validate_commercial_vehicle_identity; then exit 12; fi
BACKEND_PRODUCTION_ACCEPTANCE_ALLOW_WRITE_ACTIONS=0
for unsafe in '--allow-write-actions' '--run-task-cycle' '--profile activate_revision_gate' '--profile=activate_revision_gate' '--actions anything'; do
  BACKEND_PRODUCTION_ACCEPTANCE_EXTRA_ARGS="${unsafe}"
  if validate_commercial_vehicle_identity; then exit 13; fi
done
BACKEND_PRODUCTION_ACCEPTANCE_EXTRA_ARGS=
validate_commercial_vehicle_identity
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

    def test_no_action_runtime_asserts_transport_and_service_absence(self):
        source = read_repo_file("scripts/start_runtime.sh")
        function = bash_function(source, "assert_no_action_runtime_isolated")
        for forbidden in (
            "/mcore_velocity_sender",
            "/station_tcp_bridge",
            "/dock_supply_manager",
            "/docking_controller",
            "/auto_charge_monitor",
            "/dock_supply/start",
            "/dock_supply/recovery_retreat",
        ):
            self.assertIn(forbidden, function)
        self.assertIn(
            "/coverage_task_manager/auto_charge_enable", function
        )
        self.assertIn("assert_no_action_runtime_isolated", source[source.index("main() {"):])

        script = r"""
set -euo pipefail
export DORAEMON_RUNTIME_CONFIG_FILE=/dev/null
source scripts/start_runtime.sh
DORAEMON_NO_ACTION_ACCEPTANCE=true
NODE_OUTPUT=
SERVICE_OUTPUT=
TOPIC_OUTPUT=
AUTO_CHARGE_VALUE=false
runtime_log_status() { :; }
runtime_run_ros_cli() {
  case "$1:$2" in
    rosnode:list) printf '%s\n' "${NODE_OUTPUT}" ;;
    rosservice:list) printf '%s\n' "${SERVICE_OUTPUT}" ;;
    rostopic:list) printf '%s\n' "${TOPIC_OUTPUT}" ;;
    *) return 1 ;;
  esac
}
runtime_get_rosparam_value() { printf '%s\n' "${AUTO_CHARGE_VALUE}"; }
assert_no_action_runtime_isolated
NODE_OUTPUT=/station_tcp_bridge
if assert_no_action_runtime_isolated; then exit 9; fi
NODE_OUTPUT=
SERVICE_OUTPUT=/dock_supply/start
if assert_no_action_runtime_isolated; then exit 10; fi
SERVICE_OUTPUT=
AUTO_CHARGE_VALUE=true
if assert_no_action_runtime_isolated; then exit 11; fi
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

    def test_phase_h_verifier_rejects_action_capable_configuration(self):
        verifier = read_repo_file("scripts/verify_x86_ubuntu20_deployment.sh")
        self.assertIn("RUNTIME_START_DEPTH_CAMERAS=true", verifier)
        self.assertIn("DORAEMON_NO_ACTION_ACCEPTANCE=true during phases H/K", verifier)
        self.assertIn("DORAEMON_ACTION_TEST_APPROVED=false during phases H/K", verifier)

    def test_unresponsive_ros_cli_is_bounded(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            rosnode = os.path.join(temp_dir, "rosnode")
            with open(rosnode, "w", encoding="utf-8") as handle:
                handle.write("#!/usr/bin/env bash\nsleep 30\n")
            os.chmod(rosnode, 0o755)
            script = """
set -euo pipefail
source scripts/runtime_common.sh
if runtime_ros_master_available; then
  exit 9
fi
"""
            env = os.environ.copy()
            env["PATH"] = temp_dir + os.pathsep + env.get("PATH", "")
            # A value from runtime.env must not enlarge the validated stop
            # budget.  This exercises the real timeout wrapper, not only the
            # numeric resolver.
            env["DORAEMON_ROS_CLI_TIMEOUT_SEC"] = "999999999999999999999999"
            started = time.monotonic()
            result = subprocess.run(
                ["bash", "-c", script],
                cwd=REPO_ROOT,
                env=env,
                text=True,
                capture_output=True,
                timeout=5,
                check=False,
            )
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertLess(time.monotonic() - started, 4.0)

    def test_external_shutdown_budgets_are_hard_capped(self):
        script = r"""
set -euo pipefail
source scripts/runtime_common.sh
unset DORAEMON_ROS_CLI_TIMEOUT_SEC
unset DORAEMON_SHUTDOWN_NODE_BUDGET_SEC
unset DORAEMON_SHUTDOWN_PROCESS_BUDGET_SEC
printf '%s\n' \
  "$(runtime_ros_cli_timeout_sec)" \
  "$(runtime_shutdown_node_budget_sec)" \
  "$(runtime_shutdown_process_budget_sec)"
export DORAEMON_ROS_CLI_TIMEOUT_SEC=999999999999999999999999
export DORAEMON_SHUTDOWN_NODE_BUDGET_SEC=999999999999999999999999
export DORAEMON_SHUTDOWN_PROCESS_BUDGET_SEC=999999999999999999999999
printf '%s\n' \
  "$(runtime_ros_cli_timeout_sec)" \
  "$(runtime_shutdown_node_budget_sec)" \
  "$(runtime_shutdown_process_budget_sec)"
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
        self.assertEqual(
            result.stdout.splitlines(),
            ["2", "12", "8", "2", "12", "8"],
        )

    def test_external_shutdown_budgets_may_only_tighten(self):
        script = r"""
set -euo pipefail
source scripts/runtime_common.sh
printf '%s\n' \
  "$(runtime_ros_cli_timeout_sec)" \
  "$(runtime_shutdown_node_budget_sec)" \
  "$(runtime_shutdown_process_budget_sec)"
"""
        env = os.environ.copy()
        env["DORAEMON_ROS_CLI_TIMEOUT_SEC"] = "1"
        env["DORAEMON_SHUTDOWN_NODE_BUDGET_SEC"] = "1"
        env["DORAEMON_SHUTDOWN_PROCESS_BUDGET_SEC"] = "1"
        result = subprocess.run(
            ["bash", "-c", script],
            cwd=REPO_ROOT,
            env=env,
            text=True,
            capture_output=True,
            timeout=5,
            check=False,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertEqual(result.stdout.splitlines(), ["1", "1", "1"])

    def test_startup_logs_cannot_be_redirected_into_release(self):
        script = r"""
set -euo pipefail
export DORAEMON_RUNTIME_CONFIG_FILE=/dev/null
source scripts/start_runtime.sh
LOG_DIR="${PWD}/log/startup"
STATUS_LOG="${LOG_DIR}/status.log"
RESTART_LOCALIZATION_OUT="${LOG_DIR}/restart.out"
if validate_external_runtime_log_paths; then
  exit 9
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

    def test_slow_process_is_gone_before_tmux_cleanup_can_continue(self):
        token = "doraemon-slow-shutdown-%s" % uuid.uuid4().hex
        child = subprocess.Popen(
            [
                "bash",
                "-c",
                "trap '' TERM; while :; do sleep 1; done",
                token,
            ]
        )
        try:
            script = """
set -euo pipefail
source scripts/runtime_common.sh
runtime_terminate_process_patterns "$1"
"""
            env = os.environ.copy()
            env["DORAEMON_SHUTDOWN_PROCESS_BUDGET_SEC"] = "1"
            started = time.monotonic()
            result = subprocess.run(
                ["bash", "-c", script, "shutdown-test", token],
                cwd=REPO_ROOT,
                env=env,
                text=True,
                capture_output=True,
                timeout=6,
                check=False,
            )
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertLess(time.monotonic() - started, 5.0)
            child.wait(timeout=2)
        finally:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=2)


if __name__ == "__main__":
    unittest.main()

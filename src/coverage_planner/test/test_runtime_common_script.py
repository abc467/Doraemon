#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import stat
import subprocess
import tempfile
import textwrap
import unittest

from coverage_planner.canonical_contract_types import (
    APP_GET_ODOMETRY_STATUS_SERVICE_TYPE,
    APP_GET_PROFILE_CATALOG_SERVICE_TYPE,
    APP_GET_SLAM_JOB_SERVICE_TYPE,
    APP_GET_SLAM_STATUS_SERVICE_TYPE,
)


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(THIS_DIR)))
SCRIPT_PATH = os.path.join(REPO_ROOT, "scripts", "runtime_common.sh")
NON_CANONICAL_GET_PROFILE_CATALOG_SERVICE_TYPE = "retired/GetProfileCatalog"
NON_CANONICAL_GET_SLAM_JOB_SERVICE_TYPE = "retired/GetSlamJob"
NON_CANONICAL_GET_SLAM_STATUS_SERVICE_TYPE = "retired/GetSlamStatus"
NON_CANONICAL_EXE_TASK_SERVICE_TYPE = "retired/ExeTask"


FAKE_ROSSERVICE = """#!/usr/bin/env bash
set -eu
joined_args="$*"
joined_args="${joined_args//$'\\n'/ }"
printf 'rosservice %s\\n' "${joined_args}" >> "${FAKE_CALL_LOG}"
if [[ "${1:-}" == "type" ]]; then
  service_name="${2:-}"
  while IFS='=' read -r name value; do
    if [[ "${name}" == "${service_name}" ]]; then
      printf '%s\\n' "${value}"
      exit 0
    fi
  done <<< "${FAKE_ROSSERVICE_TYPES:-}"
  exit 1
fi
if [[ "${1:-}" != "call" ]]; then
  exit 1
fi
case "${2:-}" in
  /clean_robot_server/app/get_slam_status)
    if [[ -n "${FAKE_ROSSERVICE_APP_RESPONSE:-}" ]]; then
      printf '%s\\n' "${FAKE_ROSSERVICE_APP_RESPONSE}"
      exit 0
    fi
    ;;
  /coverage_task_manager/app/get_system_readiness)
    if [[ -n "${FAKE_ROSSERVICE_READINESS_APP_RESPONSE:-}" ]]; then
      printf '%s\\n' "${FAKE_ROSSERVICE_READINESS_APP_RESPONSE}"
      exit 0
    fi
    ;;
esac
exit 1
"""


FAKE_ROSPARAM = """#!/usr/bin/env bash
set -eu
printf 'rosparam %s\\n' "$*" >> "${FAKE_CALL_LOG}"
if [[ "${1:-}" != "get" ]]; then
  exit 1
fi
case "${2:-}" in
  /cartographer/runtime/localization_state)
    if [[ -n "${FAKE_ROSPARAM_LOCALIZATION_STATE:-}" ]]; then
      printf '%s\\n' "${FAKE_ROSPARAM_LOCALIZATION_STATE}"
      exit 0
    fi
    ;;
  /cartographer/runtime/localization_valid)
    if [[ -n "${FAKE_ROSPARAM_LOCALIZATION_VALID:-}" ]]; then
      printf '%s\\n' "${FAKE_ROSPARAM_LOCALIZATION_VALID}"
      exit 0
    fi
    ;;
  /cartographer/runtime/map_name)
    if [[ -n "${FAKE_ROSPARAM_RUNTIME_MAP:-}" ]]; then
      printf '%s\\n' "${FAKE_ROSPARAM_RUNTIME_MAP}"
      exit 0
    fi
    ;;
  /cartographer/runtime/current_map_revision_id)
    if [[ -n "${FAKE_ROSPARAM_RUNTIME_REVISION:-}" ]]; then
      printf '%s\\n' "${FAKE_ROSPARAM_RUNTIME_REVISION}"
      exit 0
    fi
    ;;
esac
exit 1
"""

FAKE_ROSNODE = """#!/usr/bin/env bash
set -eu
printf 'rosnode %s\\n' "$*" >> "${FAKE_CALL_LOG}"
if [[ "${1:-}" == "list" ]]; then
  exit 0
fi
exit 1
"""


class RuntimeCommonScriptTest(unittest.TestCase):
    maxDiff = None

    def _write_executable(self, path, content):
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(content)
        os.chmod(path, os.stat(path).st_mode | stat.S_IXUSR)

    def _run_localization_probe(
        self,
        *,
        expected_map,
        expected_revision="",
        app_response="",
        rosparam_state="",
        rosparam_valid="",
        rosparam_runtime_map="",
        rosparam_runtime_revision="",
    ):
        with tempfile.TemporaryDirectory() as temp_dir:
            bin_dir = os.path.join(temp_dir, "bin")
            os.makedirs(bin_dir)

            call_log = os.path.join(temp_dir, "calls.log")
            status_log = os.path.join(temp_dir, "status.log")
            restart_out = os.path.join(temp_dir, "restart_localization.out")

            self._write_executable(os.path.join(bin_dir, "rosservice"), FAKE_ROSSERVICE)
            self._write_executable(os.path.join(bin_dir, "rosparam"), FAKE_ROSPARAM)

            env = os.environ.copy()
            env["PATH"] = bin_dir + os.pathsep + env["PATH"]
            env["FAKE_CALL_LOG"] = call_log
            env["FAKE_ROSSERVICE_APP_RESPONSE"] = textwrap.dedent(app_response).strip()
            env["FAKE_ROSPARAM_LOCALIZATION_STATE"] = str(rosparam_state)
            env["FAKE_ROSPARAM_LOCALIZATION_VALID"] = str(rosparam_valid)
            env["FAKE_ROSPARAM_RUNTIME_MAP"] = str(rosparam_runtime_map)
            env["FAKE_ROSPARAM_RUNTIME_REVISION"] = str(rosparam_runtime_revision)

            command = textwrap.dedent(
                f"""
                set -euo pipefail
                source "{SCRIPT_PATH}"
                STATUS_LOG="{status_log}"
                RESTART_LOCALIZATION_OUT="{restart_out}"
                ROBOT_ID="local_robot"
                runtime_warn_if_localization_not_ready "{expected_map}" "{expected_revision}"
                """
            )

            subprocess.run(
                ["bash", "-lc", command],
                check=True,
                capture_output=True,
                text=True,
                env=env,
            )

            with open(status_log, "r", encoding="utf-8") as handle:
                status_output = handle.read()

            if os.path.exists(call_log):
                with open(call_log, "r", encoding="utf-8") as handle:
                    call_lines = [line.strip() for line in handle.readlines() if line.strip()]
            else:
                call_lines = []

            return status_output, call_lines

    def _run_wait_for_readiness(
        self,
        *,
        timeout_sec,
        readiness_app_response="",
        readiness_task_id="0",
        readiness_refresh_map_identity="false",
    ):
        with tempfile.TemporaryDirectory() as temp_dir:
            bin_dir = os.path.join(temp_dir, "bin")
            os.makedirs(bin_dir)

            call_log = os.path.join(temp_dir, "calls.log")

            self._write_executable(os.path.join(bin_dir, "rosservice"), FAKE_ROSSERVICE)
            self._write_executable(os.path.join(bin_dir, "rosparam"), FAKE_ROSPARAM)

            env = os.environ.copy()
            env["PATH"] = bin_dir + os.pathsep + env["PATH"]
            env["FAKE_CALL_LOG"] = call_log
            env["FAKE_ROSSERVICE_READINESS_APP_RESPONSE"] = textwrap.dedent(readiness_app_response).strip()

            command = textwrap.dedent(
                f"""
                set -euo pipefail
                source "{SCRIPT_PATH}"
                READINESS_TASK_ID="{readiness_task_id}"
                READINESS_REFRESH_MAP_IDENTITY="{readiness_refresh_map_identity}"
                runtime_wait_for_readiness "{timeout_sec}"
                """
            )

            subprocess.run(
                ["bash", "-lc", command],
                check=True,
                capture_output=True,
                text=True,
                env=env,
            )

            if os.path.exists(call_log):
                with open(call_log, "r", encoding="utf-8") as handle:
                    return [line.strip() for line in handle.readlines() if line.strip()]
            return []

    def _run_frontend_service_session_health_check(
        self,
        *,
        service_types,
        start_rosbridge="true",
        enable_odometry_health="false",
    ):
        with tempfile.TemporaryDirectory() as temp_dir:
            bin_dir = os.path.join(temp_dir, "bin")
            os.makedirs(bin_dir)

            call_log = os.path.join(temp_dir, "calls.log")

            self._write_executable(os.path.join(bin_dir, "rosservice"), FAKE_ROSSERVICE)
            self._write_executable(os.path.join(bin_dir, "rosparam"), FAKE_ROSPARAM)

            env = os.environ.copy()
            env["PATH"] = bin_dir + os.pathsep + env["PATH"]
            env["FAKE_CALL_LOG"] = call_log
            env["FAKE_ROSSERVICE_TYPES"] = "\n".join(
                "%s=%s" % (name, value) for name, value in list(service_types.items())
            )

            command = textwrap.dedent(
                f"""
                set -euo pipefail
                source "{SCRIPT_PATH}"
                START_ROSBRIDGE="{start_rosbridge}"
                FRONTEND_BACKEND_ENABLE_ODOMETRY_HEALTH="{enable_odometry_health}"
                runtime_frontend_service_session_healthy
                """
            )

            completed = subprocess.run(
                ["bash", "-lc", command],
                capture_output=True,
                text=True,
                env=env,
            )

            if os.path.exists(call_log):
                with open(call_log, "r", encoding="utf-8") as handle:
                    call_lines = [line.strip() for line in handle.readlines() if line.strip()]
            else:
                call_lines = []

            return completed, call_lines

    def _run_stop_task_execution(self, *, service_types):
        with tempfile.TemporaryDirectory() as temp_dir:
            bin_dir = os.path.join(temp_dir, "bin")
            os.makedirs(bin_dir)

            call_log = os.path.join(temp_dir, "calls.log")

            self._write_executable(os.path.join(bin_dir, "rosservice"), FAKE_ROSSERVICE)
            self._write_executable(os.path.join(bin_dir, "rosparam"), FAKE_ROSPARAM)
            self._write_executable(os.path.join(bin_dir, "rosnode"), FAKE_ROSNODE)

            env = os.environ.copy()
            env["PATH"] = bin_dir + os.pathsep + env["PATH"]
            env["FAKE_CALL_LOG"] = call_log
            env["FAKE_ROSSERVICE_TYPES"] = "\n".join(
                "%s=%s" % (name, value) for name, value in list(service_types.items())
            )

            command = textwrap.dedent(
                f"""
                set -euo pipefail
                source "{SCRIPT_PATH}"
                runtime_stop_task_execution_if_available
                """
            )

            subprocess.run(
                ["bash", "-lc", command],
                check=True,
                capture_output=True,
                text=True,
                env=env,
            )

            if os.path.exists(call_log):
                with open(call_log, "r", encoding="utf-8") as handle:
                    return [line.strip() for line in handle.readlines() if line.strip()]
            return []

    def test_localization_probe_prefers_app_query_service(self):
        status_output, call_lines = self._run_localization_probe(
            expected_map="active_map",
            app_response="""
            success: True
            message: ''
            state:
              active_map_name: active_map
              active_map_revision_id: rev_active_01
              localization_state: localized
              localization_valid: True
              runtime_map_name: active_map
              runtime_map_revision_id: rev_active_01
            """,
            rosparam_state="missing",
            rosparam_valid="false",
            rosparam_runtime_map="wrong_map",
        )

        self.assertIn("source=app_query", status_output)
        self.assertIn("[OK] 定位状态正常", status_output)
        self.assertEqual(
            call_lines,
            [
                "rosservice call /clean_robot_server/app/get_slam_status robot_id: 'local_robot' refresh_map_identity: false",
            ],
        )

    def test_localization_probe_falls_back_to_rosparams_when_queries_are_unavailable(self):
        status_output, call_lines = self._run_localization_probe(
            expected_map="active_map",
            expected_revision="rev_active_01",
            rosparam_state="localized",
            rosparam_valid="true",
            rosparam_runtime_map="active_map",
            rosparam_runtime_revision="rev_active_01",
        )

        self.assertIn("source=rosparam_fallback", status_output)
        self.assertIn("[OK] 定位状态正常", status_output)
        self.assertIn("runtime_revision=rev_active_01", status_output)
        self.assertEqual(
            call_lines,
            [
                "rosservice call /clean_robot_server/app/get_slam_status robot_id: 'local_robot' refresh_map_identity: false",
                "rosparam get /cartographer/runtime/localization_state",
                "rosparam get /cartographer/runtime/localization_valid",
                "rosparam get /cartographer/runtime/map_name",
                "rosparam get /cartographer/runtime/current_map_revision_id",
            ],
        )

    def test_localization_probe_warns_when_revision_mismatches_even_if_map_name_matches(self):
        status_output, _call_lines = self._run_localization_probe(
            expected_map="active_map",
            expected_revision="rev_active_01",
            app_response="""
            success: True
            message: ''
            state:
              active_map_name: active_map
              active_map_revision_id: rev_active_01
              localization_state: localized
              localization_valid: True
              runtime_map_name: active_map
              runtime_map_revision_id: rev_other_02
            """,
        )

        self.assertIn("[WARN] 定位未完全就绪", status_output)
        self.assertIn("runtime_revision=rev_other_02", status_output)
        self.assertIn("expected_revision=rev_active_01", status_output)

    def test_readiness_wait_prefers_app_query_with_explicit_request_fields(self):
        call_lines = self._run_wait_for_readiness(
            timeout_sec="1",
            readiness_app_response="""
            success: True
            message: ''
            readiness:
              can_start_task: True
            """,
            readiness_task_id="7",
            readiness_refresh_map_identity="false",
        )

        self.assertEqual(
            call_lines,
            [
                "rosservice call /coverage_task_manager/app/get_system_readiness task_id: 7 refresh_map_identity: false",
            ],
        )

    def test_frontend_service_session_health_requires_app_query_services(self):
        completed, call_lines = self._run_frontend_service_session_health_check(
            service_types={
                "/clean_robot_server/app/map_server": "cleanrobot_app_msgs/OperateMap",
                "/database_server/app/profile_catalog_service": APP_GET_PROFILE_CATALOG_SERVICE_TYPE,
                "/clean_robot_server/app/get_slam_status": APP_GET_SLAM_STATUS_SERVICE_TYPE,
                "/clean_robot_server/app/get_slam_job": APP_GET_SLAM_JOB_SERVICE_TYPE,
                "/rosapi/topics": "rosapi/Topics",
            },
            start_rosbridge="true",
        )

        self.assertEqual(completed.returncode, 0)
        self.assertEqual(
            call_lines,
            [
                "rosservice type /clean_robot_server/app/map_server",
                "rosservice type /database_server/app/profile_catalog_service",
                "rosservice type /clean_robot_server/app/get_slam_status",
                "rosservice type /clean_robot_server/app/get_slam_job",
                "rosservice type /rosapi/topics",
            ],
        )

    def test_frontend_service_session_health_rejects_stale_query_contracts(self):
        completed, call_lines = self._run_frontend_service_session_health_check(
            service_types={
                "/clean_robot_server/app/map_server": "cleanrobot_app_msgs/OperateMap",
                "/database_server/profile_catalog_service": NON_CANONICAL_GET_PROFILE_CATALOG_SERVICE_TYPE,
                "/clean_robot_server/get_slam_status": NON_CANONICAL_GET_SLAM_STATUS_SERVICE_TYPE,
                "/clean_robot_server/get_slam_job": NON_CANONICAL_GET_SLAM_JOB_SERVICE_TYPE,
                "/rosapi/topics": "rosapi/Topics",
            },
            start_rosbridge="true",
        )

        self.assertNotEqual(completed.returncode, 0)
        self.assertEqual(
            call_lines,
            [
                "rosservice type /clean_robot_server/app/map_server",
                "rosservice type /database_server/app/profile_catalog_service",
            ],
        )

    def test_frontend_service_session_health_requires_app_odometry_when_enabled(self):
        completed, call_lines = self._run_frontend_service_session_health_check(
            service_types={
                "/clean_robot_server/app/map_server": "cleanrobot_app_msgs/OperateMap",
                "/database_server/app/profile_catalog_service": APP_GET_PROFILE_CATALOG_SERVICE_TYPE,
                "/clean_robot_server/app/get_slam_status": APP_GET_SLAM_STATUS_SERVICE_TYPE,
                "/clean_robot_server/app/get_slam_job": APP_GET_SLAM_JOB_SERVICE_TYPE,
                "/rosapi/topics": "rosapi/Topics",
            },
            start_rosbridge="true",
            enable_odometry_health="true",
        )

        self.assertNotEqual(completed.returncode, 0)
        self.assertEqual(
            call_lines,
            [
                "rosservice type /clean_robot_server/app/map_server",
                "rosservice type /database_server/app/profile_catalog_service",
                "rosservice type /clean_robot_server/app/get_slam_status",
                "rosservice type /clean_robot_server/app/get_slam_job",
                "rosservice type /clean_robot_server/app/get_odometry_status",
            ],
        )

    def test_frontend_service_session_health_accepts_app_odometry_when_enabled(self):
        completed, call_lines = self._run_frontend_service_session_health_check(
            service_types={
                "/clean_robot_server/app/map_server": "cleanrobot_app_msgs/OperateMap",
                "/database_server/app/profile_catalog_service": APP_GET_PROFILE_CATALOG_SERVICE_TYPE,
                "/clean_robot_server/app/get_slam_status": APP_GET_SLAM_STATUS_SERVICE_TYPE,
                "/clean_robot_server/app/get_slam_job": APP_GET_SLAM_JOB_SERVICE_TYPE,
                "/clean_robot_server/app/get_odometry_status": APP_GET_ODOMETRY_STATUS_SERVICE_TYPE,
                "/rosapi/topics": "rosapi/Topics",
            },
            start_rosbridge="true",
            enable_odometry_health="true",
        )

        self.assertEqual(completed.returncode, 0)
        self.assertEqual(
            call_lines,
            [
                "rosservice type /clean_robot_server/app/map_server",
                "rosservice type /database_server/app/profile_catalog_service",
                "rosservice type /clean_robot_server/app/get_slam_status",
                "rosservice type /clean_robot_server/app/get_slam_job",
                "rosservice type /clean_robot_server/app/get_odometry_status",
                "rosservice type /rosapi/topics",
            ],
        )

    def test_stop_task_execution_prefers_app_service(self):
        call_lines = self._run_stop_task_execution(
            service_types={
                "/coverage_task_manager/app/exe_task_server": "cleanrobot_app_msgs/ExeTask",
                "/exe_task_server": NON_CANONICAL_EXE_TASK_SERVICE_TYPE,
            }
        )

        self.assertEqual(
            call_lines,
            [
                "rosnode list",
                "rosservice type /coverage_task_manager/app/exe_task_server",
                "rosservice call /coverage_task_manager/app/exe_task_server {command: 3, task_id: 0}",
            ],
        )

    def test_stop_task_execution_does_not_fall_back_to_stale_service(self):
        call_lines = self._run_stop_task_execution(
            service_types={
                "/exe_task_server": NON_CANONICAL_EXE_TASK_SERVICE_TYPE,
            }
        )

        self.assertEqual(
            call_lines,
            [
                "rosnode list",
                "rosservice type /coverage_task_manager/app/exe_task_server",
            ],
        )


if __name__ == "__main__":
    unittest.main()

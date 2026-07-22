#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import pathlib
import re
import subprocess
import tempfile
import unittest


REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
IDENTITY_VALIDATOR = REPO_ROOT / "scripts" / "commercial_vehicle_identity.sh"
DEPLOYMENT_VERIFIER = REPO_ROOT / "scripts" / "verify_x86_ubuntu20_deployment.sh"
START_RUNTIME = REPO_ROOT / "scripts" / "start_runtime.sh"
RUNTIME_TEMPLATE = REPO_ROOT / "config" / "runtime.a26022.env"
SYSTEMD_UNIT = REPO_ROOT / "deploy" / "systemd" / "doraemon-runtime.service"
LEGACY_RUNTIME_ENV_INSTALLER = REPO_ROOT / "scripts" / "install_a26022_runtime_env.sh"


PINNED_DEPENDENCY_ENV = """\
DORAEMON_DEPS_ROOT=/opt/doraemon/deps
DORAEMON_CMAKE_ROOT=/opt/doraemon/deps/cmake-3.20.6
DORAEMON_CMAKE_BIN=/opt/doraemon/deps/cmake-3.20.6/bin/cmake
DORAEMON_CTEST_BIN=/opt/doraemon/deps/cmake-3.20.6/bin/ctest
DORAEMON_GCC_VERSION=10.5.0
CC=/usr/bin/gcc-10
CXX=/usr/bin/g++-10
ABSEIL_ROOT=/opt/doraemon/deps/abseil-20211102.0
ORTOOLS_ROOT=/opt/doraemon/deps/ortools-9.9
FIELDS2COVER_ROOT=/opt/doraemon/deps/fields2cover-2.0.0
FLIRT_ROOT=/opt/doraemon/deps/flirt-doraemon-20260319
absl_DIR=/opt/doraemon/deps/abseil-20211102.0/lib/cmake/absl
CMAKE_PREFIX_PATH=/opt/doraemon/deps/abseil-20211102.0:/opt/doraemon/deps/ortools-9.9:/opt/doraemon/deps/fields2cover-2.0.0:/opt/doraemon/deps/flirt-doraemon-20260319
PYTHONPATH=/opt/doraemon/deps/fields2cover-2.0.0/lib/python3.8/site-packages
PATH=/opt/doraemon/deps/cmake-3.20.6/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
"""


def extract_bash_function(source, name):
    marker = "%s() {" % name
    start = source.index(marker)
    end = source.index("\n}\n", start) + 3
    return source[start:end]


def replace_assignment(source, key, replacement):
    pattern = re.compile(r"^%s=.*$" % re.escape(key), re.MULTILINE)
    updated, count = pattern.subn("%s=%s" % (key, replacement), source, count=1)
    if count != 1:
        raise AssertionError("fixture does not contain exactly one %s assignment" % key)
    return updated


class CommercialEnvironmentValidatorTest(unittest.TestCase):
    def run_validator(self, function_name, contents=None, path=None):
        if (contents is None) == (path is None):
            raise AssertionError("supply exactly one of contents or path")

        with tempfile.TemporaryDirectory(prefix="doraemon-env-validator-") as temp_dir:
            if path is None:
                path = pathlib.Path(temp_dir) / "candidate.env"
                path.write_text(contents, encoding="utf-8")
            result = subprocess.run(
                [
                    "bash",
                    "-c",
                    'set -uo pipefail; source "$1"; "$2" "$3"',
                    "validator-test",
                    str(IDENTITY_VALIDATOR),
                    function_name,
                    str(path),
                ],
                cwd=str(REPO_ROOT),
                env={"PATH": "/usr/bin:/bin", "LC_ALL": "C"},
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=5,
                check=False,
            )
        return result

    def assert_rejected(self, function_name, contents):
        result = self.run_validator(function_name, contents=contents)
        self.assertNotEqual(
            result.returncode,
            0,
            msg="unsafe environment was accepted:\n%s\nstdout:\n%s\nstderr:\n%s"
            % (contents, result.stdout, result.stderr),
        )

    def test_dependency_validator_accepts_only_complete_pinned_environment(self):
        result = self.run_validator(
            "commercial_validate_dependencies_env_file",
            contents="# generated commercial baseline\n\n" + PINNED_DEPENDENCY_ENV,
        )
        self.assertEqual(
            result.returncode,
            0,
            msg="stdout:\n%s\nstderr:\n%s" % (result.stdout, result.stderr),
        )

    def test_dependency_validator_rejects_schema_and_value_changes(self):
        cases = {
            "missing key": PINNED_DEPENDENCY_ENV.replace("CC=/usr/bin/gcc-10\n", "", 1),
            "duplicate key": PINNED_DEPENDENCY_ENV + "CC=/usr/bin/gcc-10\n",
            "unexpected key": PINNED_DEPENDENCY_ENV + "UNREVIEWED_PREFIX=/tmp/vendor\n",
            "wrong pinned value": replace_assignment(
                PINNED_DEPENDENCY_ENV,
                "DORAEMON_GCC_VERSION",
                "10.4.0",
            ),
        }
        for label, contents in cases.items():
            with self.subTest(label=label):
                self.assert_rejected(
                    "commercial_validate_dependencies_env_file", contents
                )

    def test_dependency_validator_rejects_shell_syntax_without_executing_it(self):
        with tempfile.TemporaryDirectory(prefix="doraemon-env-injection-") as temp_dir:
            sentinel = pathlib.Path(temp_dir) / "must-not-exist"
            cases = {
                "command substitution": "$(touch %s)" % sentinel,
                "semicolon": "/opt/doraemon/deps;touch %s" % sentinel,
                "backticks": "`touch %s`" % sentinel,
                "double quotes": '"/opt/doraemon/deps"',
                "single quotes": "'/opt/doraemon/deps'",
                "trailing space": "/opt/doraemon/deps ",
            }
            for label, value in cases.items():
                with self.subTest(label=label):
                    contents = replace_assignment(
                        PINNED_DEPENDENCY_ENV, "DORAEMON_DEPS_ROOT", value
                    )
                    self.assert_rejected(
                        "commercial_validate_dependencies_env_file", contents
                    )
                    self.assertFalse(
                        sentinel.exists(),
                        msg="validator executed data from the environment file",
                    )

    def test_runtime_validator_accepts_the_repository_template(self):
        result = self.run_validator(
            "commercial_validate_runtime_env_file", path=RUNTIME_TEMPLATE
        )
        self.assertEqual(
            result.returncode,
            0,
            msg="stdout:\n%s\nstderr:\n%s" % (result.stdout, result.stderr),
        )

    def test_production_storage_paths_override_hostile_parent_environment(self):
        expected = "\n".join(
            (
                "/data/coverage/planning.db",
                "/data/coverage/operations.db",
                "/data/maps",
                "/data/maps/imports",
                "/data/coverage/dock_calibration.yaml",
            )
        )
        script = r"""
set -euo pipefail
source "$1"
export PLAN_DB_PATH=/tmp/hostile-planning.db
export OPS_DB_PATH=/tmp/hostile-operations.db
export MAPS_ROOT=/tmp/hostile-maps
export EXTERNAL_MAPS_ROOT=/tmp/hostile-imports
export DOCK_CALIBRATION_STORAGE_PATH=/tmp/hostile-dock.yaml
set -a
source "$2"
set +a
commercial_pin_storage_paths
bash -c 'printf "%s\n" "$PLAN_DB_PATH" "$OPS_DB_PATH" "$MAPS_ROOT" "$EXTERNAL_MAPS_ROOT" "$DOCK_CALIBRATION_STORAGE_PATH"'
"""
        with tempfile.TemporaryDirectory(prefix="doraemon-storage-pin-") as temp_dir:
            runtime_env = pathlib.Path(temp_dir) / "runtime.env"
            # A valid file may omit these optional keys. Hostile values inherited
            # from the parent must still be replaced after the file is sourced.
            runtime_env.write_text("ROBOT_ID=CR-001\n", encoding="utf-8")
            result = subprocess.run(
                [
                    "bash",
                    "-c",
                    script,
                    "storage-pin-test",
                    str(IDENTITY_VALIDATOR),
                    str(runtime_env),
                ],
                cwd=str(REPO_ROOT),
                env={"PATH": "/usr/bin:/bin", "LC_ALL": "C"},
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=5,
                check=False,
            )

        self.assertEqual(
            result.returncode,
            0,
            msg="stdout:\n%s\nstderr:\n%s" % (result.stdout, result.stderr),
        )
        self.assertEqual(result.stdout.strip(), expected)

        start_source = START_RUNTIME.read_text(encoding="utf-8")
        runtime_env_source = start_source.index('source "${DORAEMON_RUNTIME_CONFIG_FILE}"')
        production_pin = start_source.index(
            "commercial_pin_storage_paths", runtime_env_source
        )
        production_exports = start_source.index(
            'export DORAEMON_REPO_ROOT="${REPO_ROOT}"', production_pin
        )
        self.assertLess(runtime_env_source, production_pin)
        self.assertLess(production_pin, production_exports)

    def test_runtime_validator_rejects_reserved_duplicate_and_executable_values(self):
        template = RUNTIME_TEMPLATE.read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory(prefix="doraemon-runtime-injection-") as temp_dir:
            sentinel = pathlib.Path(temp_dir) / "must-not-exist"
            cases = {
                "reserved key": template + "\nROS_MASTER_URI=http://10.0.0.1:11311\n",
                "script directory control": template + "\nSCRIPT_DIR=/tmp/hostile-scripts\n",
                "repository root control": template + "\nREPO_ROOT=/tmp/hostile-release\n",
                "production entry control": template + "\nDORAEMON_PRODUCTION_ENTRY=false\n",
                "automatic runtime smoke": template + "\nRUN_BACKEND_RUNTIME_SMOKE=1\n",
                "runtime smoke actions": template + "\nBACKEND_RUNTIME_SMOKE_ACTIONS=start_mapping\n",
                "runtime smoke extra args": template + "\nBACKEND_RUNTIME_SMOKE_EXTRA_ARGS=--actions,start_mapping\n",
                "automatic revision db health": template + "\nRUN_REVISION_DB_HEALTH_CHECK=1\n",
                "revision db strict override": template + "\nREVISION_DB_HEALTH_STRICT=1\n",
                "automatic production gate": template + "\nRUN_BACKEND_PRODUCTION_ACCEPTANCE=1\n",
                "production write profile": template + "\nBACKEND_PRODUCTION_ACCEPTANCE_PROFILE=revision_cycle_gate\n",
                "production write approval": template + "\nBACKEND_PRODUCTION_ACCEPTANCE_ALLOW_WRITE_ACTIONS=1\n",
                "production extra args": template + "\nBACKEND_PRODUCTION_ACCEPTANCE_EXTRA_ARGS=--allow-write-actions\n",
                "no-map startup override": template + "\nALLOW_NO_ACTIVE_MAP_STARTUP=0\n",
                "alternate plan database": template + "\nPLAN_DB_PATH=/tmp/planning.db\n",
                "alternate operations database": template + "\nOPS_DB_PATH=/tmp/operations.db\n",
                "alternate maps root": template + "\nMAPS_ROOT=/tmp/maps\n",
                "alternate imports root": template + "\nEXTERNAL_MAPS_ROOT=/tmp/imports\n",
                "alternate dock calibration": replace_assignment(
                    template,
                    "DOCK_CALIBRATION_STORAGE_PATH",
                    "/tmp/dock_calibration.yaml",
                ),
                "duplicate key": template + "\nROBOT_ID=CR-DUPLICATE\n",
                "command substitution": replace_assignment(
                    template, "ROBOT_ID", "$(touch %s)" % sentinel
                ),
                "semicolon": replace_assignment(
                    template, "ROBOT_ID", "CR-TEST;touch %s" % sentinel
                ),
                "backticks": replace_assignment(
                    template, "ROBOT_ID", "`touch %s`" % sentinel
                ),
            }
            for label, contents in cases.items():
                with self.subTest(label=label):
                    self.assert_rejected(
                        "commercial_validate_runtime_env_file", contents
                    )
                    self.assertFalse(
                        sentinel.exists(),
                        msg="validator executed data from the runtime environment",
                    )


class CommercialDeploymentVerifierContractTest(unittest.TestCase):
    def test_legacy_runtime_env_installer_is_fail_closed(self):
        source = LEGACY_RUNTIME_ENV_INSTALLER.read_text(encoding="utf-8")
        self.assertNotIn("sudo ", source)
        self.assertNotIn("systemctl", source)
        self.assertNotIn("install -m", source)

        result = subprocess.run(
            [str(LEGACY_RUNTIME_ENV_INSTALLER)],
            cwd=str(REPO_ROOT),
            env={"PATH": "/usr/bin:/bin", "LC_ALL": "C"},
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=5,
            check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("is retired for commercial deployments", result.stderr)
        self.assertIn("DORAEMON_ENABLE_SERVICE=0", result.stderr)

    @staticmethod
    def service_lines(unit_text):
        lines = []
        in_service = False
        for raw_line in unit_text.splitlines():
            line = raw_line.strip()
            if line == "[Service]":
                in_service = True
                continue
            if in_service and line.startswith("["):
                break
            if in_service and line and not line.startswith("#"):
                lines.append(line)
        return lines

    def run_static_contract(self, unit_text):
        verifier_source = DEPLOYMENT_VERIFIER.read_text(encoding="utf-8")
        function = extract_bash_function(
            verifier_source, "check_static_runtime_contract"
        )
        with tempfile.TemporaryDirectory(prefix="doraemon-static-contract-") as temp_dir:
            fixture = pathlib.Path(temp_dir)
            (fixture / "deploy" / "systemd").mkdir(parents=True)
            (fixture / "deploy" / "systemd" / SYSTEMD_UNIT.name).write_text(
                unit_text, encoding="utf-8"
            )
            # The static verifier also audits runtime and Orbbec sources. Keep those
            # read-only inputs pointed at the checkout while varying only the unit.
            for name in ("scripts", "src", "config", "docs"):
                os.symlink(str(REPO_ROOT / name), str(fixture / name))

            script = """\
set -uo pipefail
REPO_ROOT="$1"
FAILURES=0
ok() { :; }
fail() { printf '%%s\\n' "$*" >&2; FAILURES=$((FAILURES + 1)); }
%s
check_static_runtime_contract
printf 'failures=%%s\\n' "${FAILURES}"
[[ "${FAILURES}" -eq 0 ]]
""" % function
            return subprocess.run(
                ["bash", "-c", script, "static-contract-test", str(fixture)],
                cwd=str(REPO_ROOT),
                env={"PATH": "/usr/bin:/bin", "LC_ALL": "C"},
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=5,
                check=False,
            )

    def test_unit_template_has_exact_non_starting_commercial_contract(self):
        lines = self.service_lines(SYSTEMD_UNIT.read_text(encoding="utf-8"))
        for expected in (
            "User=a",
            "Group=a",
            "ExecStartPre=/opt/doraemon/current/scripts/wait_robot_boot_ready.sh",
            "ExecStart=/opt/doraemon/current/scripts/start_runtime.sh",
            "ExecStop=/opt/doraemon/current/scripts/stop_all_backend.sh",
            "ExecStopPost=/opt/doraemon/current/scripts/cleanup_failed_runtime_service.sh",
            "NoNewPrivileges=true",
            "UMask=0027",
            "RemainAfterExit=yes",
            "Environment=ALLOW_NO_ACTIVE_MAP_STARTUP=1",
            "Environment=RUN_BACKEND_RUNTIME_SMOKE=0",
            "Environment=RUN_REVISION_DB_HEALTH_CHECK=0",
            "Environment=RUN_BACKEND_PRODUCTION_ACCEPTANCE=0",
        ):
            self.assertEqual(lines.count(expected), 1, msg="missing exact unit line: %s" % expected)

        environment_files = [
            line for line in lines if line.startswith("EnvironmentFile=")
        ]
        self.assertEqual(
            environment_files,
            [
                "EnvironmentFile=-/etc/doraemon/runtime.env",
                "EnvironmentFile=-/etc/doraemon/deps.env",
            ],
        )

    def test_verifier_detects_each_static_systemd_contract_regression(self):
        baseline = SYSTEMD_UNIT.read_text(encoding="utf-8")
        baseline_result = self.run_static_contract(baseline)
        self.assertEqual(
            baseline_result.returncode,
            0,
            msg="baseline static contract failed:\n%s\n%s"
            % (baseline_result.stdout, baseline_result.stderr),
        )

        ordered_environment = (
            "EnvironmentFile=-/etc/doraemon/runtime.env\n"
            "EnvironmentFile=-/etc/doraemon/deps.env"
        )
        swapped_environment = (
            "EnvironmentFile=-/etc/doraemon/deps.env\n"
            "EnvironmentFile=-/etc/doraemon/runtime.env"
        )
        mutations = {
            "service user": ("User=a", "User=root"),
            "service group": ("Group=a", "Group=root"),
            "environment file order": (ordered_environment, swapped_environment),
            "preflight": (
                "ExecStartPre=/opt/doraemon/current/scripts/wait_robot_boot_ready.sh",
                "ExecStartPre=/bin/true",
            ),
            "start": (
                "ExecStart=/opt/doraemon/current/scripts/start_runtime.sh",
                "ExecStart=/bin/true",
            ),
            "stop": (
                "ExecStop=/opt/doraemon/current/scripts/stop_all_backend.sh",
                "ExecStop=/bin/true",
            ),
            "post-stop cleanup": (
                "ExecStopPost=/opt/doraemon/current/scripts/cleanup_failed_runtime_service.sh",
                "ExecStopPost=/bin/true",
            ),
            "privilege boundary": ("NoNewPrivileges=true", "NoNewPrivileges=false"),
            "umask": ("UMask=0027", "UMask=0000"),
            "oneshot state": ("RemainAfterExit=yes", "RemainAfterExit=no"),
            "automatic smoke disabled": (
                "Environment=RUN_BACKEND_RUNTIME_SMOKE=0",
                "Environment=RUN_BACKEND_RUNTIME_SMOKE=1",
            ),
            "new vehicle degraded startup": (
                "Environment=ALLOW_NO_ACTIVE_MAP_STARTUP=1",
                "Environment=ALLOW_NO_ACTIVE_MAP_STARTUP=0",
            ),
            "automatic revision db health disabled": (
                "Environment=RUN_REVISION_DB_HEALTH_CHECK=0",
                "Environment=RUN_REVISION_DB_HEALTH_CHECK=1",
            ),
            "automatic production acceptance disabled": (
                "Environment=RUN_BACKEND_PRODUCTION_ACCEPTANCE=0",
                "Environment=RUN_BACKEND_PRODUCTION_ACCEPTANCE=1",
            ),
        }
        for label, (original, replacement) in mutations.items():
            with self.subTest(label=label):
                self.assertIn(original, baseline)
                mutated = baseline.replace(original, replacement, 1)
                result = self.run_static_contract(mutated)
                self.assertNotEqual(
                    result.returncode,
                    0,
                    msg="verifier accepted %s regression:\n%s\n%s"
                    % (label, result.stdout, result.stderr),
                )

    def test_verifier_requires_zero_restarts_and_stopped_disabled_service(self):
        source = extract_bash_function(
            DEPLOYMENT_VERIFIER.read_text(encoding="utf-8"),
            "check_installed_runtime_contract",
        )
        ordered_tokens = (
            'systemctl show -p NRestarts --value "${service}"',
            '[[ "${actual}" == "0" ]]',
            'systemctl is-enabled "${service}"',
            '[[ "${actual}" == "disabled" ]]',
            'systemctl is-active "${service}"',
            '[[ "${actual}" == "inactive" ]]',
        )
        cursor = -1
        for token in ordered_tokens:
            offset = source.find(token, cursor + 1)
            self.assertGreater(offset, cursor, msg="missing or out-of-order: %s" % token)
            cursor = offset

    def test_immutable_tree_scan_command_failure_is_fail_closed(self):
        verifier_source = DEPLOYMENT_VERIFIER.read_text(encoding="utf-8")
        function = extract_bash_function(verifier_source, "check_root_immutable_tree")
        with tempfile.TemporaryDirectory(prefix="doraemon-find-failure-") as temp_dir:
            bin_dir = pathlib.Path(temp_dir) / "bin"
            bin_dir.mkdir()
            (bin_dir / "stat").write_text(
                "#!/usr/bin/env bash\nprintf 'root:root 755\\n'\n", encoding="utf-8"
            )
            (bin_dir / "find").write_text(
                "#!/usr/bin/env bash\nexit 73\n", encoding="utf-8"
            )
            os.chmod(str(bin_dir / "stat"), 0o755)
            os.chmod(str(bin_dir / "find"), 0o755)

            immutable_fixture = pathlib.Path(temp_dir) / "immutable-tree"
            immutable_fixture.mkdir()
            script = """\
set -uo pipefail
FAILURES=0
ok() { :; }
fail() { printf '%%s\\n' "$*" >&2; FAILURES=$((FAILURES + 1)); }
%s
check_root_immutable_tree "$1" 'test immutable tree'
if [[ "${FAILURES}" -eq 0 ]]; then
  echo 'find failure was incorrectly reported as immutable' >&2
  exit 90
fi
""" % function
            env = {
                "PATH": "%s:/usr/bin:/bin" % bin_dir,
                "LC_ALL": "C",
            }
            result = subprocess.run(
                ["bash", "-c", script, "immutable-scan-test", str(immutable_fixture)],
                cwd=str(REPO_ROOT),
                env=env,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=5,
                check=False,
            )
            self.assertEqual(
                result.returncode,
                0,
                msg="stdout:\n%s\nstderr:\n%s" % (result.stdout, result.stderr),
            )


if __name__ == "__main__":
    unittest.main()

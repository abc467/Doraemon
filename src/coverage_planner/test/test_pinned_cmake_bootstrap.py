#!/usr/bin/env python3

import os
import pathlib
import platform
import re
import subprocess
import tempfile
import unittest


REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
INSTALLER = REPO_ROOT / "scripts" / "install_x86_ubuntu20_dependencies.sh"
MANIFEST = REPO_ROOT / "deploy" / "manifests" / "x86_ubuntu20_versions.env"


def _manifest_values():
    values = {}
    for raw_line in MANIFEST.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        key, value = line.split("=", 1)
        values[key] = value
    return values


def _is_ubuntu_2004_x86_64():
    try:
        os_release = pathlib.Path("/etc/os-release").read_text(encoding="utf-8")
    except OSError:
        return False
    return (
        platform.machine() == "x86_64"
        and re.search(r'^ID=(?:"ubuntu"|ubuntu)$', os_release, re.MULTILINE)
        is not None
        and re.search(r'^VERSION_ID=(?:"20\.04"|20\.04)$', os_release, re.MULTILINE)
        is not None
    )


class PinnedCMakeBootstrapTest(unittest.TestCase):
    def test_manifest_pins_official_kitware_x86_64_archive(self):
        values = _manifest_values()
        self.assertEqual(values["CMAKE_VERSION"], "3.20.6")
        self.assertEqual(
            values["CMAKE_LINUX_X86_64_URL"],
            "https://github.com/Kitware/CMake/releases/download/v3.20.6/"
            "cmake-3.20.6-linux-x86_64.tar.gz",
        )
        self.assertEqual(
            values["CMAKE_LINUX_X86_64_SHA256"],
            "458777097903b0f35a0452266b923f0a2f5b62fe331e636e2dcc4b636b768e36",
        )
        self.assertRegex(values["CMAKE_LINUX_X86_64_SHA256"], r"^[0-9a-f]{64}$")
        self.assertEqual(
            values["CMAKE_INSTALLED_TREE_SHA256"],
            "d59116f0550ef490aeffa2865032f08fac5a14f1fd97c0146aa3cdf85a00dc90",
        )
        self.assertRegex(values["CMAKE_INSTALLED_TREE_SHA256"], r"^[0-9a-f]{64}$")

    def test_manifest_pins_gcc_10_and_approved_ros1_mirror(self):
        values = _manifest_values()
        self.assertEqual(values["GCC_TOOLCHAIN_MAJOR"], "10")
        self.assertEqual(values["GCC_TOOLCHAIN_VERSION"], "10.5.0")
        self.assertEqual(
            values["ROS1_APT_REPOSITORY_URL"],
            "https://mirrors.ustc.edu.cn/ros/ubuntu",
        )
        self.assertEqual(values["ROS1_APT_SUITE"], "focal")
        self.assertEqual(
            values["ROS1_APT_KEY_URL"],
            "https://raw.githubusercontent.com/ros/rosdistro/master/ros.key",
        )
        self.assertEqual(
            values["ROS1_APT_KEY_SHA256"],
            "4a91c49af0d6f0016108b93698782b596c27ccd836937e18e0e36c3347dc602f",
        )
        self.assertEqual(
            values["ROS1_APT_KEY_FINGERPRINT"],
            "C1CF6E31E6BADE8868B172B4F42ED6FBAB17C654",
        )

    def test_installer_is_fail_closed_and_uses_only_pinned_cmake(self):
        text = INSTALLER.read_text(encoding="utf-8")
        active_text = "\n".join(
            line for line in text.splitlines() if not line.lstrip().startswith("#")
        )
        flattened = " ".join(text.split())

        self.assertIn('CMAKE_PREFIX="${DEPS_ROOT}/cmake-${CMAKE_VERSION}"', text)
        self.assertIn('CMAKE_BIN="${CMAKE_PREFIX}/bin/cmake"', text)
        self.assertIn('CTEST_BIN="${CMAKE_PREFIX}/bin/ctest"', text)
        self.assertIn("CMake archive SHA256 verification failed", text)
        self.assertIn("refusing to overwrite the existing CMake prefix", text)
        self.assertIn("pinned CMake installed-tree SHA256 mismatch", text)
        self.assertIn("commercial_find_mount_below", text)
        self.assertIn('"cmake version ${CMAKE_VERSION}"', text)
        self.assertIn('"ctest version ${CMAKE_VERSION}"', text)
        self.assertIn("curl --proto '=https' --tlsv1.2 --fail --location", flattened)

        self.assertNotRegex(active_text, r"(?m)^\s*(?:sudo\s+)?cmake(?:\s|$)")
        self.assertNotRegex(active_text, r"(?m)^\s*(?:sudo\s+)?ctest(?:\s|$)")
        self.assertEqual(text.count('  "${CMAKE_BIN}" -S '), 4)
        self.assertEqual(
            text.count('  "${CMAKE_BIN}" --build "${build_dir}"'), 4
        )
        self.assertEqual(
            text.count('  sudo "${CMAKE_BIN}" --install "${build_dir}"'), 4
        )
        self.assertEqual(text.count('    -DCMAKE_C_COMPILER="${CC_BIN}"'), 4)
        self.assertEqual(text.count('    -DCMAKE_CXX_COMPILER="${CXX_BIN}"'), 4)
        self.assertEqual(
            text.count('  verify_dependency_cmake_cache "${build_dir}"'), 4
        )
        self.assertNotIn("build-essential cmake ninja-build", text)
        self.assertIn(
            'build-essential "gcc-${GCC_TOOLCHAIN_MAJOR}" '
            '"g++-${GCC_TOOLCHAIN_MAJOR}"',
            flattened,
        )
        self.assertIn("for key in CMAKE_COMMAND CMAKE_C_COMPILER CMAKE_CXX_COMPILER", text)
        self.assertIn("clean this dependency build directory before retrying", text)

        install_call = text.index("\ninstall_pinned_cmake\n", text.index("sudo -v"))
        dependency_build = text.index("\nbuild_abseil\n")
        self.assertLess(install_call, dependency_build)

        self.assertIn("DORAEMON_CMAKE_ROOT=${CMAKE_PREFIX}", text)
        self.assertIn("DORAEMON_CMAKE_BIN=${CMAKE_BIN}", text)
        self.assertIn("DORAEMON_CTEST_BIN=${CTEST_BIN}", text)
        self.assertIn("DORAEMON_GCC_VERSION=${GCC_TOOLCHAIN_VERSION}", text)
        self.assertIn("CC=${CC_BIN}", text)
        self.assertIn("CXX=${CXX_BIN}", text)
        self.assertIn(
            "PATH=${CMAKE_PREFIX}/bin:/usr/local/sbin:/usr/local/bin:"
            "/usr/sbin:/usr/bin:/sbin:/bin",
            text,
        )

    def test_ros1_source_is_unique_https_ustc_and_key_is_verified(self):
        text = INSTALLER.read_text(encoding="utf-8")
        main_start = text.index("\nsudo -v\n")
        key_install = text.index("\n  install_official_ros1_key\n", main_start)
        source_install = text.index("\n  configure_ros1_apt_source 1\n", key_install)
        package_update = text.index("\n  sudo apt-get update\n", source_install)

        self.assertLess(key_install, source_install)
        self.assertLess(source_install, package_update)
        self.assertIn(
            'ROS1_APT_LINE="deb [arch=amd64 signed-by=${ROS1_KEYRING}] '
            '${ROS1_APT_REPOSITORY_URL} ${ROS1_APT_SUITE} ${ROS1_APT_COMPONENT}"',
            text,
        )
        self.assertIn("preflight_ros1_apt_sources", text)
        self.assertIn("expected ${enabled} active ROS1 apt source", text)
        self.assertIn("non-ROS1 lines were preserved", text)
        self.assertIn("ROS1 signing-key SHA256 or fingerprint verification failed", text)
        self.assertIn("--skip-apt skips mutation, not commercial baseline verification", text)
        self.assertGreaterEqual(text.count("verify_ros1_apt_state"), 3)
        self.assertIn("expected exactly one managed ROS1 apt source", text)
        self.assertIn("ROS1 apt list must be root:root 0644", text)
        self.assertIn("ROS1 keyring must be root:root 0644", text)
        self.assertNotIn(
            'echo "deb [arch=amd64 signed-by=/usr/share/keyrings/'
            'ros-archive-keyring.gpg] http://packages.ros.org',
            text,
        )

    def test_dependency_source_cache_must_be_clean_and_exact(self):
        text = INSTALLER.read_text(encoding="utf-8")
        function_start = text.index("\nprepare_source() {")
        function_end = text.index("\n}\n\nbuild_abseil()", function_start)
        function_text = text[function_start:function_end]

        self.assertIn("refusing to initialize non-empty dependency source", function_text)
        self.assertGreaterEqual(
            function_text.count("status --porcelain --untracked-files=all"), 2
        )
        self.assertGreaterEqual(function_text.count("remote get-url --all origin"), 2)
        self.assertIn('rev-parse HEAD)" != "${commit}"', function_text)
        self.assertIn("dependency source is dirty before checkout", function_text)
        self.assertIn("dependency source is dirty after checkout", function_text)
        self.assertNotRegex(function_text, r"git[^\n]*(?:reset|clean)\b")

        checkout = function_text.index("checkout -q --detach FETCH_HEAD")
        post_status = function_text.index(
            "status --porcelain --untracked-files=all", checkout
        )
        self.assertLess(checkout, post_status)

    @unittest.skipUnless(
        _is_ubuntu_2004_x86_64(), "read-only entry point targets Ubuntu 20.04 x86_64"
    )
    def test_read_only_verifier_is_exact_and_idempotent(self):
        with tempfile.TemporaryDirectory(prefix="doraemon-cmake-verify-") as tmp:
            deps_root = pathlib.Path(tmp) / "deps"
            bin_dir = deps_root / "cmake-3.20.6" / "bin"
            bin_dir.mkdir(parents=True)
            for name in ("cmake", "ctest"):
                tool = bin_dir / name
                tool.write_text(
                    "#!/usr/bin/env bash\n"
                    f"printf '%s\\n' '{name} version 3.20.6'\n",
                    encoding="utf-8",
                )
                tool.chmod(0o755)

            env = os.environ.copy()
            env["DORAEMON_DEPS_ROOT"] = str(deps_root)
            env["DORAEMON_DEPENDENCY_TEST_MODE"] = "1"
            command = ["bash", str(INSTALLER), "--verify-cmake-only"]
            for _ in range(2):
                verified = subprocess.run(
                    command,
                    env=env,
                    text=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                self.assertEqual(
                    verified.returncode,
                    0,
                    msg=f"stdout:\n{verified.stdout}\nstderr:\n{verified.stderr}",
                )
                self.assertIn("pinned CMake 3.20.6 verified", verified.stdout)

            (bin_dir / "ctest").write_text(
                "#!/usr/bin/env bash\nprintf '%s\\n' 'ctest version 3.20.5'\n",
                encoding="utf-8",
            )
            (bin_dir / "ctest").chmod(0o755)
            rejected = subprocess.run(
                command,
                env=env,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            self.assertNotEqual(rejected.returncode, 0)
            self.assertIn("expected ctest version 3.20.6", rejected.stderr)

    @unittest.skipUnless(
        _is_ubuntu_2004_x86_64(), "read-only entry point targets Ubuntu 20.04 x86_64"
    )
    def test_path_override_is_rejected_without_explicit_read_only_test_mode(self):
        with tempfile.TemporaryDirectory(prefix="doraemon-cmake-override-") as tmp:
            env = os.environ.copy()
            env["DORAEMON_DEPS_ROOT"] = str(pathlib.Path(tmp) / "deps")
            rejected = subprocess.run(
                ["bash", str(INSTALLER), "--verify-cmake-only"],
                env=env,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            self.assertNotEqual(rejected.returncode, 0)
            self.assertIn("commercial install paths are fixed", rejected.stderr)

    @unittest.skipUnless(
        _is_ubuntu_2004_x86_64()
        and pathlib.Path("/usr/bin/gcc-10").exists()
        and pathlib.Path("/usr/bin/g++-10").exists(),
        "pinned GCC/G++ fixture is unavailable",
    )
    def test_read_only_toolchain_verifier_checks_current_compilers(self):
        verified = subprocess.run(
            ["bash", str(INSTALLER), "--verify-toolchain-only"],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self.assertEqual(
            verified.returncode,
            0,
            msg=f"stdout:\n{verified.stdout}\nstderr:\n{verified.stderr}",
        )
        self.assertIn("pinned CMake 3.20.6 verified", verified.stdout)
        self.assertIn("pinned GCC/G++ 10.5.0 verified", verified.stdout)

        with tempfile.TemporaryDirectory(
            prefix="test-cmake-cache-",
            dir="/var/tmp/doraemon-deps-build/build",
        ) as tmp:
            build_dir = pathlib.Path(tmp)
            cache = build_dir / "CMakeCache.txt"
            cache.write_text(
                "CMAKE_COMMAND:INTERNAL=/opt/doraemon/deps/cmake-3.20.6/bin/cmake\n"
                "CMAKE_C_COMPILER:FILEPATH=/usr/bin/gcc-10\n"
                "CMAKE_CXX_COMPILER:FILEPATH=/usr/bin/g++-10\n",
                encoding="utf-8",
            )
            cache_command = [
                "bash",
                str(INSTALLER),
                f"--verify-dependency-cache-only={build_dir}",
            ]
            cache_ok = subprocess.run(
                cache_command,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            self.assertEqual(
                cache_ok.returncode,
                0,
                msg=f"stdout:\n{cache_ok.stdout}\nstderr:\n{cache_ok.stderr}",
            )
            self.assertIn("cache uses the pinned toolchain", cache_ok.stdout)

            cache.write_text(
                "CMAKE_COMMAND:INTERNAL=/opt/doraemon/deps/cmake-3.20.6/bin/cmake\n"
                "CMAKE_C_COMPILER:FILEPATH=/usr/bin/gcc-10\n"
                "CMAKE_CXX_COMPILER:FILEPATH=/usr/bin/g++\n",
                encoding="utf-8",
            )
            cache_rejected = subprocess.run(
                cache_command,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            self.assertNotEqual(cache_rejected.returncode, 0)
            self.assertIn("stale CMAKE_CXX_COMPILER", cache_rejected.stderr)


if __name__ == "__main__":
    unittest.main()

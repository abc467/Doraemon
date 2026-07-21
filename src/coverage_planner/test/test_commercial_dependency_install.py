#!/usr/bin/env python3

import os
import pathlib
import shutil
import stat
import subprocess
import tempfile
import unittest


REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
INSTALLER = REPO_ROOT / "scripts" / "install_x86_ubuntu20_dependencies.sh"
BUILD_SCRIPT = REPO_ROOT / "scripts" / "build_x86_ubuntu20_workspace.sh"
HARDENER = REPO_ROOT / "scripts" / "harden_fields2cover_python_install.sh"
DEPLOYMENT_VERIFIER = REPO_ROOT / "scripts" / "verify_x86_ubuntu20_deployment.sh"
VERSIONS_MANIFEST = REPO_ROOT / "deploy" / "manifests" / "x86_ubuntu20_versions.env"
COVERAGE_PLANNER_PACKAGE = REPO_ROOT / "src" / "coverage_planner" / "package.xml"
LD_CONF_TEMPLATE = REPO_ROOT / "config" / "doraemon-deps.ld.so.conf"
INSTALLED_FIELDS_PREFIX = pathlib.Path("/opt/doraemon/deps/fields2cover-2.0.0")
INSTALLED_ORTOOLS_PREFIX = pathlib.Path("/opt/doraemon/deps/ortools-9.9")


def extract_bash_function(source, name):
    marker = "%s() {" % name
    start = source.index(marker)
    end = source.index("\n}\n", start) + 3
    return source[start:end]


class CommercialDependencyInstallTest(unittest.TestCase):
    @staticmethod
    def manifest_values():
        values = {}
        for raw_line in VERSIONS_MANIFEST.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            key, separator, value = line.partition("=")
            if separator != "=" or not key or key in values:
                raise AssertionError("invalid dependency manifest line: %s" % raw_line)
            values[key] = value
        return values

    def test_shapely_is_an_exact_focal_apt_runtime_dependency(self):
        values = self.manifest_values()
        self.assertEqual(values["SHAPELY_APT_PACKAGE"], "python3-shapely")
        self.assertEqual(values["SHAPELY_APT_VERSION"], "1.7.0-1build1")
        self.assertEqual(values["SHAPELY_APT_ARCH"], "amd64")
        self.assertEqual(
            values["SHAPELY_APT_DEB_SHA256"],
            "230c303ce98fb8fdb4906ec5ce3c32b6a662af032c767682b4c3bdcd2dfb2686",
        )
        self.assertEqual(values["SHAPELY_PYTHON_VERSION"], "1.7.0")
        self.assertIn(
            "<exec_depend>python3-shapely</exec_depend>",
            COVERAGE_PLANNER_PACKAGE.read_text(encoding="utf-8"),
        )

    def test_shapely_install_and_acceptance_are_fail_closed(self):
        installer_text = INSTALLER.read_text(encoding="utf-8")
        build_text = BUILD_SCRIPT.read_text(encoding="utf-8")
        verifier_text = DEPLOYMENT_VERIFIER.read_text(encoding="utf-8")

        self.assertIn('"${SHAPELY_APT_PACKAGE}=${SHAPELY_APT_VERSION}"', installer_text)
        self.assertIn("verify_pinned_shapely_apt_metadata", installer_text)
        self.assertGreaterEqual(installer_text.count("verify_pinned_shapely\n"), 2)
        self.assertIn("dpkg --verify", installer_text)
        self.assertIn("/usr/bin/python3 -I", installer_text)
        self.assertIn("PYTHONNOUSERSITE=1", installer_text)
        self.assertIn("unary_union", installer_text)
        self.assertIn("--verify-shapely-only", build_text)
        self.assertIn("--verify-shapely-only", verifier_text)

    def test_shapely_apt_metadata_digest_gate(self):
        source = INSTALLER.read_text(encoding="utf-8")
        functions = "\n".join(
            extract_bash_function(source, name)
            for name in (
                "validate_shapely_manifest",
                "verify_pinned_shapely_apt_metadata",
            )
        )
        metadata_template = """\
Package: python3-shapely
Architecture: amd64
Version: 1.7.0-1build1
SHA256: %s
"""
        expected_digest = (
            "230c303ce98fb8fdb4906ec5ce3c32b6a662af032c767682b4c3bdcd2dfb2686"
        )
        with tempfile.TemporaryDirectory(prefix="doraemon-shapely-apt-") as tmp:
            root = pathlib.Path(tmp)
            bin_dir = root / "bin"
            bin_dir.mkdir()
            apt_cache = bin_dir / "apt-cache"
            runner = """\
set -euo pipefail
VERSIONS_FILE=fixture
SHAPELY_APT_PACKAGE=python3-shapely
SHAPELY_APT_VERSION=1.7.0-1build1
SHAPELY_APT_ARCH=amd64
SHAPELY_APT_DEB_SHA256=230c303ce98fb8fdb4906ec5ce3c32b6a662af032c767682b4c3bdcd2dfb2686
SHAPELY_PYTHON_VERSION=1.7.0
%s
verify_pinned_shapely_apt_metadata
""" % functions

            for label, digest, expected_returncode in (
                ("approved digest", expected_digest, 0),
                ("wrong digest", "0" * 64, 1),
            ):
                with self.subTest(label=label):
                    apt_cache.write_text(
                        "#!/usr/bin/env bash\ncat <<'EOF'\n%sEOF\n"
                        % (metadata_template % digest),
                        encoding="utf-8",
                    )
                    os.chmod(apt_cache, 0o755)
                    result = subprocess.run(
                        ["bash", "-c", runner],
                        env={"PATH": "%s:/usr/bin:/bin" % bin_dir, "LC_ALL": "C"},
                        text=True,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        check=False,
                    )
                    self.assertEqual(
                        result.returncode,
                        expected_returncode,
                        msg="stdout:\n%s\nstderr:\n%s"
                        % (result.stdout, result.stderr),
                    )

    def test_read_only_shapely_gate_rejects_a_missing_or_wrong_baseline(self):
        query = subprocess.run(
            [
                "dpkg-query",
                "-W",
                "-f=${Status}|${Version}|${Architecture}",
                "python3-shapely",
            ],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        exact_package = (
            query.returncode == 0
            and query.stdout == "install ok installed|1.7.0-1build1|amd64"
        )
        verified = subprocess.run(
            [str(INSTALLER), "--verify-shapely-only"],
            cwd=str(REPO_ROOT),
            env={"PATH": "/usr/bin:/bin", "LC_ALL": "C"},
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=15,
            check=False,
        )
        if exact_package:
            self.assertEqual(
                verified.returncode,
                0,
                msg="stdout:\n%s\nstderr:\n%s" % (verified.stdout, verified.stderr),
            )
        else:
            self.assertNotEqual(verified.returncode, 0)
            self.assertRegex(
                verified.stderr,
                r"required package is not installed|expected python3-shapely",
            )

    def test_loader_configuration_is_a_fixed_release_template(self):
        expected = [
            "/opt/doraemon/deps/abseil-20211102.0/lib",
            "/opt/doraemon/deps/abseil-20211102.0/lib64",
            "/opt/doraemon/deps/ortools-9.9/lib",
            "/opt/doraemon/deps/ortools-9.9/lib64",
            "/opt/doraemon/deps/fields2cover-2.0.0/lib",
            "/opt/doraemon/deps/fields2cover-2.0.0/lib64",
            "/opt/doraemon/deps/flirt-doraemon-20260319/lib",
            "/opt/doraemon/deps/flirt-doraemon-20260319/lib64",
        ]
        actual = [
            line.strip()
            for line in LD_CONF_TEMPLATE.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        ]
        self.assertEqual(actual, expected)

        installer_text = INSTALLER.read_text(encoding="utf-8")
        verifier_text = DEPLOYMENT_VERIFIER.read_text(encoding="utf-8")
        self.assertIn("DEPS_LD_CONF_TEMPLATE", installer_text)
        self.assertIn("installed dynamic-loader configuration is not the fixed template", installer_text)
        self.assertIn("doraemon-deps.ld.so.conf", verifier_text)

    def test_installer_hardens_after_install_and_rechecks_system_loader(self):
        text = INSTALLER.read_text(encoding="utf-8")
        install_pos = text.index(
            'sudo "${CMAKE_BIN}" --install "${build_dir}"',
            text.index("build_fields2cover()"),
        )
        harden_pos = text.index("harden_fields2cover_python_install.sh", install_pos)
        ldconfig_pos = text.index("sudo ldconfig", harden_pos)
        system_verify_pos = text.index("--system-loader", ldconfig_pos)

        self.assertLess(install_pos, harden_pos)
        self.assertLess(harden_pos, ldconfig_pos)
        self.assertLess(ldconfig_pos, system_verify_pos)
        self.assertIn('sudo chown -R root:root "${FIELDS2COVER_PREFIX}"', text)
        self.assertIn("--require-root-owner", text)
        self.assertNotIn('sudo cmake --install "${build_dir}"', text)

    def test_installer_preflights_fixed_roots_before_any_dependency_install(self):
        text = INSTALLER.read_text(encoding="utf-8")
        preflight_call = text.index("preflight_dependency_install_paths\n")
        first_product_install = text.index(
            'sudo "${CMAKE_BIN}" --install "${build_dir}"'
        )
        self.assertLess(preflight_call, first_product_install)
        self.assertIn("dependency root contains a dangling or escaping symlink", text)
        self.assertIn("existing dependency prefix is not a canonical real directory", text)
        self.assertIn("dependency build root contains a nested mount", text)
        self.assertIn("dependency build path contains a dangling or escaping symlink", text)
        self.assertIn('"${BUILD_ROOT}/build"/*', text)
        self.assertNotIn('sudo install -d -m 0755 "${DEPS_ROOT}"', text)

    def test_deployment_verifier_rechecks_installed_python_extension(self):
        text = DEPLOYMENT_VERIFIER.read_text(encoding="utf-8")
        self.assertIn("harden_fields2cover_python_install.sh", text)
        self.assertIn("--verify-only", text)
        self.assertIn("--system-loader", text)
        self.assertIn("--require-root-owner", text)
        self.assertIn(
            "Fields2Cover Python install is not independent of the build cache", text
        )
        self.assertIn('ldd -r "${EXTENSION}"', HARDENER.read_text(encoding="utf-8"))

    @unittest.skipUnless(shutil.which("patchelf"), "patchelf is required")
    def test_system_loader_check_removes_inherited_library_paths(self):
        extension = (
            INSTALLED_FIELDS_PREFIX
            / "lib/python3.8/site-packages/_fields2cover_python.so"
        )
        module = (
            INSTALLED_FIELDS_PREFIX / "lib/python3.8/site-packages/fields2cover.py"
        )
        if (
            not extension.exists()
            or not module.exists()
            or not INSTALLED_ORTOOLS_PREFIX.exists()
        ):
            self.skipTest("installed Fields2Cover/OR-Tools fixture is unavailable")

        with tempfile.TemporaryDirectory(prefix="doraemon-f2c-loader-env-") as tmp:
            root = pathlib.Path(tmp)
            bin_dir = root / "bin"
            build_cache = root / "dependency-build-cache"
            bin_dir.mkdir()
            build_cache.mkdir()

            ldd_proxy = bin_dir / "ldd"
            ldd_proxy.write_text(
                """#!/usr/bin/env bash
set -euo pipefail
for variable in LD_LIBRARY_PATH LD_PRELOAD LD_AUDIT LD_ORIGIN_PATH LIBRARY_PATH; do
  if [[ -v \"${variable}\" ]]; then
    echo \"inherited loader variable reached ldd: ${variable}\" >&2
    exit 97
  fi
done
exec /usr/bin/ldd \"$@\"
""",
                encoding="utf-8",
            )
            os.chmod(ldd_proxy, 0o755)

            inherited_env = os.environ.copy()
            inherited_env["PATH"] = f"{bin_dir}:{inherited_env['PATH']}"
            inherited_env["LD_LIBRARY_PATH"] = str(build_cache)
            inherited_env["LD_PRELOAD"] = ""
            inherited_env["LD_AUDIT"] = ""
            inherited_env["LD_ORIGIN_PATH"] = str(build_cache)
            inherited_env["LIBRARY_PATH"] = str(build_cache)

            verified = subprocess.run(
                [
                    "bash",
                    str(HARDENER),
                    "--verify-only",
                    "--system-loader",
                    str(extension),
                    str(module),
                    str(INSTALLED_FIELDS_PREFIX),
                    str(INSTALLED_ORTOOLS_PREFIX),
                    str(build_cache),
                ],
                env=inherited_env,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            self.assertEqual(
                verified.returncode,
                0,
                msg=f"stdout:\n{verified.stdout}\nstderr:\n{verified.stderr}",
            )
            self.assertIn("loader=system", verified.stdout)

    @unittest.skipUnless(shutil.which("patchelf"), "patchelf is required")
    def test_hardener_replaces_build_rpath_and_removes_writable_modes(self):
        installed_extension = (
            INSTALLED_FIELDS_PREFIX
            / "lib/python3.8/site-packages/_fields2cover_python.so"
        )
        installed_module = (
            INSTALLED_FIELDS_PREFIX / "lib/python3.8/site-packages/fields2cover.py"
        )
        installed_libraries = {
            "libFields2Cover.so": INSTALLED_FIELDS_PREFIX / "lib/libFields2Cover.so",
            "libsteering_functions.so": INSTALLED_FIELDS_PREFIX / "lib/libsteering_functions.so",
            "libmatplot.so.1": INSTALLED_FIELDS_PREFIX / "lib/libmatplot.so.1",
        }
        required = [installed_extension, installed_module, *installed_libraries.values()]
        if not all(path.exists() for path in required) or not INSTALLED_ORTOOLS_PREFIX.exists():
            self.skipTest("installed Fields2Cover/OR-Tools fixture is unavailable")

        with tempfile.TemporaryDirectory(prefix="doraemon-f2c-rpath-") as tmp:
            root = pathlib.Path(tmp)
            prefix = root / "fields2cover-2.0.0"
            site_packages = prefix / "lib/python3.8/site-packages"
            lib_dir = prefix / "lib"
            share_dir = prefix / "share/generated"
            site_packages.mkdir(parents=True)
            lib_dir.mkdir(parents=True, exist_ok=True)
            share_dir.mkdir(parents=True)

            extension = site_packages / "_fields2cover_python.so"
            module = site_packages / "fields2cover.py"
            shutil.copy2(installed_extension, extension)
            shutil.copy2(installed_module, module)
            for name, source in installed_libraries.items():
                shutil.copy2(source, lib_dir / name, follow_symlinks=True)

            generated_file = share_dir / "generated.txt"
            generated_file.write_text("fixture\n", encoding="utf-8")
            os.chmod(extension, 0o775)
            os.chmod(module, 0o664)
            os.chmod(share_dir, 0o775)
            os.chmod(generated_file, 0o664)

            build_root = root / "dependency-build-cache"
            bad_rpath = (
                f"{build_root}/build/fields2cover-2.0.0:"
                f"{build_root}/build/fields2cover-2.0.0/_deps/matplot-build"
            )
            subprocess.run(
                ["patchelf", "--set-rpath", bad_rpath, str(extension)],
                check=True,
            )

            verify_bad = subprocess.run(
                [
                    "bash",
                    str(HARDENER),
                    "--verify-only",
                    str(extension),
                    str(module),
                    str(prefix),
                    str(INSTALLED_ORTOOLS_PREFIX),
                    str(build_root),
                ],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            self.assertNotEqual(verify_bad.returncode, 0)

            hardened = subprocess.run(
                [
                    "bash",
                    str(HARDENER),
                    str(extension),
                    str(module),
                    str(prefix),
                    str(INSTALLED_ORTOOLS_PREFIX),
                    str(build_root),
                ],
                check=True,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            self.assertIn("[OK] Fields2Cover Python install hardened", hardened.stdout)

            expected_rpath = f"{prefix}/lib:{INSTALLED_ORTOOLS_PREFIX}/lib"
            actual_rpath = subprocess.check_output(
                ["patchelf", "--print-rpath", str(extension)], text=True
            ).strip()
            self.assertEqual(actual_rpath, expected_rpath)
            self.assertEqual(stat.S_IMODE(extension.stat().st_mode), 0o755)
            self.assertEqual(stat.S_IMODE(module.stat().st_mode), 0o644)
            self.assertEqual(stat.S_IMODE(share_dir.stat().st_mode) & 0o022, 0)
            self.assertEqual(stat.S_IMODE(generated_file.stat().st_mode) & 0o022, 0)


if __name__ == "__main__":
    unittest.main()

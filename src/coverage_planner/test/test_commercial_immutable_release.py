#!/usr/bin/env python3

import hashlib
import pathlib
import subprocess
import tempfile
import unittest


REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
FILESYSTEM_SECURITY = REPO_ROOT / "scripts" / "commercial_filesystem_security.sh"
INSTALLER = REPO_ROOT / "scripts" / "install_doraemon_runtime_service.sh"
VERIFIER = REPO_ROOT / "scripts" / "verify_x86_ubuntu20_deployment.sh"
BUILD_SCRIPT = REPO_ROOT / "scripts" / "build_x86_ubuntu20_workspace.sh"
MANIFEST = REPO_ROOT / "deploy" / "manifests" / "x86_ubuntu20_versions.env"


def _call_security_function(function_call):
    return subprocess.run(
        ["bash", "-c", f'source "$1"; {function_call}', "bash", str(FILESYSTEM_SECURITY)],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


class CommercialImmutableReleaseTest(unittest.TestCase):
    def test_manifest_pins_backend_and_frontend_release_identities(self):
        text = MANIFEST.read_text(encoding="utf-8")
        self.assertIn(
            "DORAEMON_BACKEND_DEPLOYMENT_TAG=deployment-2026-07-22-x86-ubuntu20-v7",
            text,
        )
        self.assertIn(
            "DORAEMON_BACKEND_GIT_URL=https://github.com/abc467/Doraemon.git",
            text,
        )
        self.assertIn(
            "DORAEMON_FRONTEND_DEPLOYMENT_TAG=deployment-2026-07-21-frontend-v2",
            text,
        )
        self.assertIn("DORAEMON_FRONTEND_VERSION=0.1.0-rc.10", text)

    def test_escaping_and_dangling_symlinks_are_rejected(self):
        with tempfile.TemporaryDirectory(prefix="doraemon-release-symlink-") as tmp:
            temp_root = pathlib.Path(tmp)
            release = temp_root / "release"
            release.mkdir()
            inside = release / "inside.txt"
            outside = temp_root / "outside.txt"
            inside.write_text("inside\n", encoding="utf-8")
            outside.write_text("outside\n", encoding="utf-8")

            (release / "safe-link").symlink_to(inside)
            safe = _call_security_function(
                f'commercial_find_symlink_outside_tree "{release}"'
            )
            self.assertNotEqual(safe.returncode, 0)

            (release / "unsafe-link").symlink_to(outside)
            unsafe = _call_security_function(
                f'commercial_find_symlink_outside_tree "{release}"'
            )
            self.assertEqual(unsafe.returncode, 0)
            self.assertIn("unsafe-link", unsafe.stdout)

            (release / "unsafe-link").unlink()
            (release / "dangling-link").symlink_to(release / "missing")
            dangling = _call_security_function(
                f'commercial_find_symlink_outside_tree "{release}"'
            )
            self.assertEqual(dangling.returncode, 0)
            self.assertIn("dangling-link", dangling.stdout)

    def test_generated_runtime_artifacts_are_rejected_but_git_logs_are_ignored(self):
        with tempfile.TemporaryDirectory(prefix="doraemon-release-artifacts-") as tmp:
            release = pathlib.Path(tmp)
            (release / ".git" / "logs").mkdir(parents=True)
            clean = _call_security_function(
                f'commercial_find_forbidden_release_artifact "{release}"'
            )
            self.assertEqual(clean.returncode, 0)
            self.assertEqual(clean.stdout, "")

            for relative in (
                "logs/runtime.txt",
                "LOG/runtime.txt",
                "test_bag/test.bag",
                "captures/test.bag.partial",
                "image/capture.png",
                "point_cloud/capture.pcd",
                "source/export.log",
                "planning.db",
                "cache.sqlite-wal",
            ):
                with self.subTest(relative=relative):
                    target = release / relative
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_text("fixture\n", encoding="utf-8")
                    rejected = _call_security_function(
                        f'commercial_find_forbidden_release_artifact "{release}"'
                    )
                    self.assertEqual(rejected.returncode, 0)
                    self.assertTrue(rejected.stdout.strip())
                    if target.is_file():
                        target.unlink()
                    parent = target.parent
                    while parent != release and parent.exists() and not any(parent.iterdir()):
                        parent.rmdir()
                        parent = parent.parent

    def test_installer_and_verifier_enforce_full_release_contract(self):
        installer = INSTALLER.read_text(encoding="utf-8")
        verifier = VERIFIER.read_text(encoding="utf-8")
        for required in (
            "commercial_verify_release_git_identity",
            "commercial_find_mount_below",
            "commercial_find_unsafe_release_symlink",
            "commercial_find_forbidden_release_artifact",
        ):
            self.assertIn(required, installer)
            self.assertIn(required, verifier)
        self.assertIn(".git/shallow", FILESYSTEM_SECURITY.read_text(encoding="utf-8"))
        for path in ("/data/coverage", "/data/maps", "/data/maps/imports"):
            self.assertIn(path, verifier)
        self.assertNotIn("rm -rf", installer)

    def test_remote_tag_check_is_bounded_and_noninteractive(self):
        source = FILESYSTEM_SECURITY.read_text(encoding="utf-8")
        self.assertIn("GIT_TERMINAL_PROMPT=0", source)
        self.assertIn("GIT_ASKPASS=/bin/false", source)
        self.assertIn("SSH_ASKPASS=/bin/false", source)
        self.assertIn(
            "/usr/bin/timeout --signal=TERM --kill-after=5s 20s", source
        )
        self.assertIn("remote deployment tag check timed out after 20 seconds", source)

    def test_release_git_checks_are_fixed_read_only_operations(self):
        source = FILESYSTEM_SECURITY.read_text(encoding="utf-8")
        self.assertIn("commercial_release_git_readonly", source)
        self.assertIn("commercial_validate_frozen_release_for_root_git", source)
        self.assertIn("COMMERCIAL_FROZEN_RELEASE_GIT_AUDIT", source)
        for operation in (
            "is-shallow",
            "exact-tag",
            "tag-object-type",
            "origin-url",
            "head-commit",
            "tag-commit",
            "clean-status",
            "head-tree",
        ):
            self.assertIn(f"{operation})", source)
        self.assertIn("/usr/bin/sudo -n -- /usr/bin/env -i", source)
        self.assertIn("--no-optional-locks", source)
        self.assertIn("--no-replace-objects", source)
        self.assertIn("/usr/bin/timeout", source)
        self.assertIn("core.hooksPath=/dev/null", source)
        self.assertIn("core.fsmonitor=false", source)
        self.assertIn("submodule.recurse=false", source)
        self.assertIn("for ancestor in /opt /opt/doraemon /opt/doraemon/releases", source)
        self.assertIn("! -type f ! -type d ! -type l", source)
        self.assertNotIn("git config --global", source)

    def test_release_git_operation_allowlist_rejects_mutation(self):
        for operation in ("config", "fetch", "checkout", "reset", "clean"):
            rejected = _call_security_function(
                f'commercial_release_git_readonly "{REPO_ROOT}" {operation}'
            )
            self.assertNotEqual(rejected.returncode, 0)
            self.assertIn("operation is not allowed", rejected.stderr)

    def test_nonempty_slam_override_requires_complete_layout(self):
        source = FILESYSTEM_SECURITY.read_text(encoding="utf-8")
        self.assertIn('find "${root}" -xdev -mindepth 1 -print -quit', source)
        self.assertIn('required_layout="1"', source)

    def test_installer_checks_slam_paths_before_creating_them(self):
        installer = INSTALLER.read_text(encoding="utf-8")
        preflight = installer.index("for config_path in")
        creation = installer.index("# Create only missing path components")
        self.assertLess(preflight, creation)
        self.assertIn('if [[ -L "${config_path}"', installer[preflight:creation])
        self.assertNotIn(
            "install -d -o root -g root -m 0755 /data/config /data/config/slam",
            installer,
        )

    def test_workspace_provenance_accepts_only_current_pinned_cache(self):
        with tempfile.TemporaryDirectory(prefix="doraemon-provenance-") as tmp:
            temp_root = pathlib.Path(tmp)
            seed = temp_root / "seed"
            release = temp_root / "test-release"
            (seed / "src" / "pkg").mkdir(parents=True)
            (seed / ".gitignore").write_text("build/\ndevel/\n", encoding="utf-8")
            (seed / "src" / "pkg" / "CMakeLists.txt").write_text(
                "cmake_minimum_required(VERSION 3.10)\nproject(pkg)\n", encoding="utf-8"
            )
            subprocess.run(["git", "init", "-q", str(seed)], check=True)
            subprocess.run(["git", "-C", str(seed), "add", "."], check=True)
            subprocess.run(
                [
                    "git",
                    "-C",
                    str(seed),
                    "-c",
                    "user.name=fixture",
                    "-c",
                    "user.email=fixture@example.invalid",
                    "commit",
                    "-qm",
                    "fixture",
                ],
                check=True,
            )
            subprocess.run(
                [
                    "git",
                    "-C",
                    str(seed),
                    "-c",
                    "user.name=fixture",
                    "-c",
                    "user.email=fixture@example.invalid",
                    "tag",
                    "-am",
                    "fixture",
                    "test-release",
                ],
                check=True,
            )
            subprocess.run(
                [
                    "git",
                    "clone",
                    "-q",
                    "--depth",
                    "1",
                    "--single-branch",
                    "--branch",
                    "test-release",
                    seed.as_uri(),
                    str(release),
                ],
                check=True,
            )

            cache_dir = release / "build" / "pkg"
            cache_dir.mkdir(parents=True)
            (release / "devel").mkdir()
            (release / "devel" / "setup.bash").write_text("# fixture\n", encoding="utf-8")
            cmake = pathlib.Path("/opt/doraemon/deps/cmake-3.20.6/bin/cmake").resolve()
            cc = pathlib.Path("/usr/bin/gcc-10").resolve()
            cxx = pathlib.Path("/usr/bin/g++-10").resolve()
            cache = cache_dir / "CMakeCache.txt"
            cache.write_text(
                "\n".join(
                    (
                        f"CMAKE_COMMAND:INTERNAL={cmake}",
                        f"CMAKE_C_COMPILER:FILEPATH={cc}",
                        f"CMAKE_CXX_COMPILER:FILEPATH={cxx}",
                        f"CMAKE_HOME_DIRECTORY:INTERNAL={release / 'src' / 'pkg'}",
                        "",
                    )
                ),
                encoding="utf-8",
            )
            commit = subprocess.check_output(
                ["git", "-C", str(release), "rev-parse", "HEAD"], text=True
            ).strip()
            tree = subprocess.check_output(
                ["git", "-C", str(release), "rev-parse", "HEAD^{tree}"], text=True
            ).strip()
            hostname = subprocess.check_output(["hostname"], text=True).strip()
            machine_id_sha256 = hashlib.sha256(
                pathlib.Path("/etc/machine-id").read_bytes()
            ).hexdigest()
            marker = release / "build" / ".doraemon-commercial-build.env"
            marker.write_text(
                "\n".join(
                    (
                        "DORAEMON_BUILD_PROVENANCE_VERSION=1",
                        f"DORAEMON_BUILD_REPO_ROOT={release}",
                        "DORAEMON_BUILD_GIT_TAG=test-release",
                        f"DORAEMON_BUILD_GIT_COMMIT={commit}",
                        f"DORAEMON_BUILD_GIT_TREE={tree}",
                        f"DORAEMON_BUILD_HOSTNAME={hostname}",
                        f"DORAEMON_BUILD_MACHINE_ID_SHA256={machine_id_sha256}",
                        f"DORAEMON_BUILD_CMAKE_BIN={cmake}",
                        f"DORAEMON_BUILD_CC={cc}",
                        f"DORAEMON_BUILD_CXX={cxx}",
                        "DORAEMON_BUILD_CACHE_COUNT=1",
                        "DORAEMON_BUILD_FINISHED_UTC=20260721T120000Z",
                        "DORAEMON_BUILD_SOURCE_ATTESTATION=remote",
                        "",
                    )
                ),
                encoding="utf-8",
            )
            marker.chmod(0o644)
            call = (
                f'commercial_validate_workspace_build_provenance "{release}" '
                f'test-release "{cmake}" "{cc}" "{cxx}" "{seed.as_uri()}"'
            )
            accepted = _call_security_function(call)
            self.assertEqual(accepted.returncode, 0, accepted.stderr)

            cache.write_text(
                cache.read_text(encoding="utf-8").replace(str(cxx), "/usr/bin/g++"),
                encoding="utf-8",
            )
            rejected = _call_security_function(call)
            self.assertNotEqual(rejected.returncode, 0)
            self.assertIn("unpinned C++ compiler", rejected.stderr)

    def test_build_and_install_paths_enforce_freshness_and_fixed_service(self):
        build_script = BUILD_SCRIPT.read_text(encoding="utf-8")
        installer = INSTALLER.read_text(encoding="utf-8")
        verifier = VERIFIER.read_text(encoding="utf-8")
        self.assertIn("fresh commercial build refuses pre-existing state", build_script)
        self.assertIn('"-DCMAKE_C_COMPILER=${CC}"', build_script)
        self.assertIn('"-DCMAKE_CXX_COMPILER=${CXX}"', build_script)
        self.assertIn("DORAEMON_BUILD_MACHINE_ID_SHA256", build_script)
        self.assertIn("commercial_verify_remote_deployment_tag", build_script)
        self.assertIn("trial-release-exception.env", build_script)
        for source in (installer, verifier):
            self.assertIn("commercial_validate_workspace_build_provenance", source)
        self.assertIn(
            'if [[ "${SERVICE_NAME}" != "doraemon-runtime.service" ]]', installer
        )
        self.assertIn("must be exactly inactive and disabled", installer)


if __name__ == "__main__":
    unittest.main()

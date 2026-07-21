#!/usr/bin/env python3

import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


REPO_ROOT = Path(__file__).resolve().parents[3]
ORBBEC_ROOT = REPO_ROOT / "src" / "orbbec-ros-sdk"
CMAKE_FILE = ORBBEC_ROOT / "CMakeLists.txt"
HARDENER = ORBBEC_ROOT / "scripts" / "harden_elf_rpath.sh"


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def elf_rpath(path):
    return subprocess.check_output(
        ["patchelf", "--print-rpath", str(path)], text=True
    ).rstrip("\n")


def elf_dynamic_paths(path):
    output = subprocess.check_output(["readelf", "-d", str(path)], text=True)
    paths = []
    for line in output.splitlines():
        if "(RPATH)" not in line and "(RUNPATH)" not in line:
            continue
        if "[" not in line or "]" not in line:
            raise AssertionError(f"cannot parse dynamic path for {path}: {line}")
        paths.append(line.rsplit("[", 1)[1].split("]", 1)[0])
    return paths


def dynamic_elf(path):
    if not path.is_file() or path.is_symlink():
        return False
    return (
        subprocess.run(
            ["readelf", "-d", str(path)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        ).returncode
        == 0
    )


class OrbbecRpathHardeningTest(unittest.TestCase):
    def test_cmake_uses_hardened_copies_and_list_rpaths(self):
        source = CMAKE_FILE.read_text(encoding="utf-8")

        self.assertIn(
            "list(APPEND CMAKE_BUILD_RPATH ${ORBBEC_LIBS_DIR})", source
        )
        self.assertIn(
            "list(APPEND CMAKE_INSTALL_RPATH ${ORBBEC_LIBS_DIR})", source
        )
        self.assertNotRegex(source, r'set\(CMAKE_(?:BUILD|INSTALL)_RPATH\s+"[^\n]*:')
        self.assertIn("configure_file(${ORBBEC_VENDOR_LIBRARY}", source)
        self.assertIn("scripts/harden_elf_rpath.sh", source)
        self.assertIn("cmake/copy_hardened_sdk.cmake", source)
        self.assertIn("BUILD_WITH_INSTALL_RPATH TRUE", source)
        self.assertIn('harden_orbbec_target(${PROJECT_NAME} "$ORIGIN;', source)
        self.assertIn('harden_orbbec_target(${TARGET} "$ORIGIN/..;', source)
        self.assertIn("install(DIRECTORY ${ORBBEC_LIBS_DIR}/", source)
        self.assertNotIn("install(DIRECTORY ${ORBBEC_VENDOR_LIBS_DIR}/", source)

    @unittest.skipUnless(
        shutil.which("patchelf") and shutil.which("readelf"),
        "patchelf and readelf are required",
    )
    def test_vendor_copy_is_hardened_without_modifying_checked_in_elf(self):
        vendor = ORBBEC_ROOT / "SDK" / "lib" / "x64" / "libOrbbecSDK.so.1.10.35"
        source_hash = sha256(vendor)
        self.assertEqual(elf_rpath(vendor), "$ORIGIN:")

        with tempfile.TemporaryDirectory() as temp_dir:
            hardened = Path(temp_dir) / vendor.name
            shutil.copy2(vendor, hardened)
            result = subprocess.run(
                ["bash", str(HARDENER), str(hardened)],
                env={**os.environ, "PATCHELF": shutil.which("patchelf")},
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(elf_rpath(hardened), "$ORIGIN")

        self.assertEqual(sha256(vendor), source_hash)
        self.assertEqual(elf_rpath(vendor), "$ORIGIN:")

    @unittest.skipUnless(
        shutil.which("patchelf") and shutil.which("readelf"),
        "patchelf and readelf are required",
    )
    def test_hardener_rejects_candidate_and_current_directory_paths(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            candidate = Path(temp_dir) / "unsafe-elf"
            shutil.copy2("/bin/true", candidate)
            subprocess.run(
                [
                    "patchelf",
                    "--set-rpath",
                    "/var/tmp/doraemon-candidate-work/lib::.",
                    str(candidate),
                ],
                check=True,
            )
            result = subprocess.run(
                ["bash", str(HARDENER), str(candidate)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=False,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("unsafe Orbbec RPATH component", result.stderr)

    @unittest.skipUnless(
        shutil.which("patchelf") and shutil.which("readelf"),
        "patchelf and readelf are required",
    )
    def test_fresh_orbbec_build_has_only_safe_rpath_components(self):
        roots = [
            REPO_ROOT / "build" / "orbbec_camera" / "hardened-sdk" / "lib" / "x64",
            REPO_ROOT / "devel" / ".private" / "orbbec_camera" / "lib",
        ]
        if not all(root.is_dir() for root in roots):
            self.skipTest("Orbbec package has not been freshly built")

        elf_paths = sorted(
            {
                path
                for root in roots
                for path in root.rglob("*")
                if dynamic_elf(path)
            }
        )
        names = {path.name for path in elf_paths}
        self.assertIn("libOrbbecSDK.so.1.10.35", names)
        self.assertIn("liborbbec_camera.so", names)
        self.assertIn("orbbec_camera_node", names)
        devel_lib = roots[1]
        self.assertTrue((devel_lib / "libOrbbecSDK.so").is_symlink())
        self.assertTrue((devel_lib / "libOrbbecSDK.so.1.10").is_symlink())
        self.assertTrue((devel_lib / "libdepthengine.so").is_symlink())

        for path in elf_paths:
            for dynamic_path in elf_dynamic_paths(path):
                for component in dynamic_path.split(":"):
                    self.assertNotIn(
                        component, ("", "."), f"{path}: {dynamic_path}"
                    )
                    self.assertFalse(
                        component.startswith(
                            (
                                "/home/",
                                "/tmp/",
                                "/var/tmp/",
                                "/usr/local/",
                                "/opt/carto/",
                                "/opt/doraemon/releases/",
                            )
                        ),
                        f"{path}: {dynamic_path}",
                    )


if __name__ == "__main__":
    unittest.main()

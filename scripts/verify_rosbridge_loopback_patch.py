#!/usr/bin/env python3
"""Fail closed if the commercial rosbridge loopback overlay drifts."""

import ast
import hashlib
import inspect
import os
from pathlib import Path
import sys
import xml.etree.ElementTree as ET


REPO_ROOT = Path(__file__).resolve().parents[1]
PACKAGE_ROOT = REPO_ROOT / "src" / "rosbridge_server"
WEBSOCKET_SCRIPT = PACKAGE_ROOT / "scripts" / "rosbridge_websocket.py"
EXPECTED_VERSION = "0.11.17"
EXPECTED_BASE_COMMIT = "55a6bdb20842a3f7f0e17931b7856c3d8700f9fb"
EXPECTED_FIX_COMMIT = "f6a829abaeca9763c5d00ba5a232407e789bdbfa"
EXPECTED_SHA256 = "5630bbeb5864d0b1fa16a40250fbdd81c90e2a2ebc040b9dae429d21388f971a"
EXPECTED_LICENSE_SHA256 = "89fbedd3d60fe09728c4650b7a33a2ee80cc6a226c2e1597c37b09d7c803a37d"
EXPECTED_AUTHORS_SHA256 = "5947ce78b19a2b5dc26cc238a3174d0acb286829043865cd59fd64849840fee4"


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_manifest():
    values = {}
    manifest = REPO_ROOT / "deploy" / "manifests" / "x86_ubuntu20_versions.env"
    for raw_line in manifest.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        require(separator == "=", f"invalid manifest line: {raw_line}")
        values[key] = value
    return values


def verify_source_patch():
    require(WEBSOCKET_SCRIPT.is_file(), f"missing {WEBSOCKET_SCRIPT}")
    source_bytes = WEBSOCKET_SCRIPT.read_bytes()
    digest = hashlib.sha256(source_bytes).hexdigest()
    require(
        digest == EXPECTED_SHA256,
        f"patched rosbridge script SHA256 drifted: {digest}",
    )

    tree = ast.parse(source_bytes, filename=str(WEBSOCKET_SCRIPT))
    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "listenWS"
    ]
    require(len(calls) == 1, f"expected one listenWS call, found {len(calls)}")
    interface_values = [
        keyword.value for keyword in calls[0].keywords if keyword.arg == "interface"
    ]
    require(len(interface_values) == 1, "listenWS must have one interface keyword")
    interface = interface_values[0]
    require(
        isinstance(interface, ast.Attribute)
        and isinstance(interface.value, ast.Name)
        and interface.value.id == "factory"
        and interface.attr == "host",
        "listenWS interface must be factory.host",
    )

    package_version = ET.parse(PACKAGE_ROOT / "package.xml").getroot().findtext(
        "version"
    )
    require(package_version == EXPECTED_VERSION, f"unexpected version {package_version}")

    upstream_links = {
        "rosbridge_websocket": "rosbridge_websocket.py",
        "rosbridge_tcp": "./rosbridge_tcp.py",
        "rosbridge_udp": "rosbridge_udp.py",
    }
    for name, expected_target in upstream_links.items():
        link = PACKAGE_ROOT / "scripts" / name
        require(link.is_symlink(), f"upstream script link was flattened: {link}")
        require(
            os.readlink(str(link)) == expected_target,
            f"unexpected link target: {link}",
        )

    license_path = PACKAGE_ROOT / "LICENSE"
    authors_path = PACKAGE_ROOT / "AUTHORS.md"
    require(license_path.is_file(), "upstream LICENSE is missing")
    require(authors_path.is_file(), "upstream AUTHORS.md is missing")
    require(sha256(license_path) == EXPECTED_LICENSE_SHA256, "LICENSE drifted")
    require(sha256(authors_path) == EXPECTED_AUTHORS_SHA256, "AUTHORS.md drifted")
    require((PACKAGE_ROOT / "PROVENANCE.md").is_file(), "PROVENANCE.md is missing")

    manifest = read_manifest()
    expected_manifest = {
        "ROSBRIDGE_SERVER_VERSION": EXPECTED_VERSION,
        "ROSBRIDGE_SERVER_COMMIT": EXPECTED_BASE_COMMIT,
        "ROSBRIDGE_BINDING_FIX_COMMIT": EXPECTED_FIX_COMMIT,
        "ROSBRIDGE_WEBSOCKET_PATCHED_SHA256": EXPECTED_SHA256,
    }
    for key, expected in expected_manifest.items():
        require(manifest.get(key) == expected, f"manifest mismatch for {key}")


def verify_runtime_defaults():
    loopback_defaults = {
        "scripts/start_runtime.sh": "ROSBRIDGE_ADDRESS:-127.0.0.1",
        "scripts/start_frontend_backend.sh": "ROSBRIDGE_ADDRESS:-127.0.0.1",
        "scripts/runtime_common.sh": "ROSBRIDGE_ADDRESS:-127.0.0.1",
        "src/coverage_planner/launch/frontend_editor_backend.launch": (
            'name="rosbridge_address" default="127.0.0.1"'
        ),
    }
    for relative_path, expected in loopback_defaults.items():
        text = (REPO_ROOT / relative_path).read_text(encoding="utf-8")
        require(expected in text, f"loopback default is missing from {relative_path}")
        require(
            "ROSBRIDGE_ADDRESS:-0.0.0.0" not in text,
            f"wildcard rosbridge fallback remains in {relative_path}",
        )
        require(
            'name="rosbridge_address" default="0.0.0.0"' not in text,
            f"wildcard rosbridge launch default remains in {relative_path}",
        )

    runtime_root = REPO_ROOT / "runtime-data"
    runtime_files = [path for path in runtime_root.rglob("*") if path.is_file()]
    require(not runtime_files, f"release-local runtime data found: {runtime_files}")

    bag_files = list(REPO_ROOT.rglob("*.bag")) + list(REPO_ROOT.rglob("*.bag.active"))
    require(not bag_files, f"test bag files found in release: {bag_files}")


def verify_installed_api():
    import autobahn
    import twisted
    from autobahn.twisted.websocket import WebSocketServerFactory, listenWS

    signature = inspect.signature(listenWS)
    require("interface" in signature.parameters, "Autobahn listenWS lacks interface")
    factory = WebSocketServerFactory("ws://127.0.0.1:9090")
    require(factory.host == "127.0.0.1", f"unexpected factory host {factory.host}")
    require(factory.port == 9090, f"unexpected factory port {factory.port}")
    return autobahn.__version__, twisted.__version__


def main():
    verify_source_patch()
    verify_runtime_defaults()
    autobahn_version, twisted_version = verify_installed_api()
    print(
        "[OK] rosbridge loopback overlay verified: "
        f"rosbridge={EXPECTED_VERSION} autobahn={autobahn_version} "
        f"twisted={twisted_version}"
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"[FAIL] rosbridge loopback overlay: {error}", file=sys.stderr)
        raise SystemExit(1)

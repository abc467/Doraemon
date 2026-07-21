#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import ast
import hashlib
import pathlib
import unittest
import xml.etree.ElementTree as ET


REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
ROSBRIDGE_SCRIPT = (
    REPO_ROOT / "src" / "rosbridge_server" / "scripts" / "rosbridge_websocket.py"
)
ROSBRIDGE_LAUNCH = (
    REPO_ROOT / "src" / "rosbridge_server" / "launch" / "rosbridge_websocket.launch"
)
ROSBRIDGE_VERIFIER = REPO_ROOT / "scripts" / "verify_rosbridge_loopback_patch.py"
VERSIONS_MANIFEST = REPO_ROOT / "deploy" / "manifests" / "x86_ubuntu20_versions.env"


class RosbridgeLoopbackDefaultsTest(unittest.TestCase):
    def test_launch_default_is_loopback(self):
        root = ET.parse(str(ROSBRIDGE_LAUNCH)).getroot()
        address_args = [
            element
            for element in root.findall("arg")
            if element.attrib.get("name") == "address"
        ]
        self.assertEqual(len(address_args), 1)
        self.assertEqual(address_args[0].attrib.get("default"), "127.0.0.1")

    def test_python_default_and_empty_fallback_are_loopback(self):
        source = ROSBRIDGE_SCRIPT.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(ROSBRIDGE_SCRIPT))

        address_defaults = []
        empty_fallbacks = []
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "rospy"
                and node.func.attr == "get_param"
                and len(node.args) >= 2
                and isinstance(node.args[0], ast.Constant)
                and node.args[0].value == "~address"
            ):
                address_defaults.append(node.args[1])
            if (
                isinstance(node, ast.If)
                and isinstance(node.test, ast.UnaryOp)
                and isinstance(node.test.op, ast.Not)
                and isinstance(node.test.operand, ast.Name)
                and node.test.operand.id == "address"
            ):
                for statement in node.body:
                    if (
                        isinstance(statement, ast.Assign)
                        and any(
                            isinstance(target, ast.Name) and target.id == "address"
                            for target in statement.targets
                        )
                    ):
                        empty_fallbacks.append(statement.value)

        self.assertEqual(len(address_defaults), 1)
        self.assertIsInstance(address_defaults[0], ast.Constant)
        self.assertEqual(address_defaults[0].value, "127.0.0.1")
        self.assertEqual(len(empty_fallbacks), 1)
        self.assertIsInstance(empty_fallbacks[0], ast.Constant)
        self.assertEqual(empty_fallbacks[0].value, "127.0.0.1")

        wildcard_literals = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Constant) and node.value == "0.0.0.0"
        ]
        self.assertEqual(wildcard_literals, [])

    def test_pinned_digest_matches_source_and_manifest(self):
        digest = hashlib.sha256(ROSBRIDGE_SCRIPT.read_bytes()).hexdigest()

        verifier_tree = ast.parse(
            ROSBRIDGE_VERIFIER.read_text(encoding="utf-8"),
            filename=str(ROSBRIDGE_VERIFIER),
        )
        expected_digests = [
            node.value.value
            for node in verifier_tree.body
            if isinstance(node, ast.Assign)
            and any(
                isinstance(target, ast.Name) and target.id == "EXPECTED_SHA256"
                for target in node.targets
            )
            and isinstance(node.value, ast.Constant)
        ]
        self.assertEqual(expected_digests, [digest])

        manifest_values = {}
        for raw_line in VERSIONS_MANIFEST.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            key, separator, value = line.partition("=")
            self.assertEqual(separator, "=", msg="invalid manifest line: %s" % raw_line)
            manifest_values[key] = value
        self.assertEqual(
            manifest_values.get("ROSBRIDGE_WEBSOCKET_PATCHED_SHA256"), digest
        )


if __name__ == "__main__":
    unittest.main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import tempfile
import unittest
import xml.etree.ElementTree as ET
from unittest import mock

from coverage_planner.slam_workflow.node_bootstrap import (
    DEPLOYMENT_SLAM_CONFIG_ROOT,
    DEPLOYMENT_SLAM_RUNTIME_LOG_ROOT,
    canonical_slam_config_root,
    default_slam_config_root,
    is_reviewed_deployment_slam_config_root,
    resolve_slam_config_root,
    resolve_slam_runtime_log_root,
)


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(THIS_DIR)))
PLANNER_LAUNCH = os.path.join(
    REPO_ROOT,
    "src",
    "coverage_planner",
    "launch",
    "planner_server.launch",
)
SLAM_ENV_SCRIPT = os.path.join(REPO_ROOT, "scripts", "source_slam_runtime_env.sh")
RUNTIME_INSTALLER = os.path.join(REPO_ROOT, "scripts", "install_doraemon_runtime_service.sh")
VISUALWEB_HEADER = os.path.join(
    REPO_ROOT,
    "src",
    "cartographer_ros",
    "cartographer_ros",
    "cartographer_ros",
    "visualweb.h",
)


class SlamRuntimeLogRootTest(unittest.TestCase):
    def test_default_is_external_commercial_log_directory(self):
        for raw_path in ("", "   "):
            with self.subTest(raw_path=raw_path):
                self.assertEqual(
                    resolve_slam_runtime_log_root(raw_path, "/opt/doraemon/releases/example"),
                    "/var/log/doraemon/slam-runtime",
                )
        self.assertEqual(
            DEPLOYMENT_SLAM_RUNTIME_LOG_ROOT,
            "/var/log/doraemon/slam-runtime",
        )

    def test_explicit_external_override_is_allowed(self):
        workspace = "/opt/doraemon/releases/example"
        external = "/var/log/doraemon/slam-runtime-test"
        self.assertEqual(
            resolve_slam_runtime_log_root(external, workspace),
            external,
        )

    def test_workspace_and_descendants_are_rejected(self):
        with tempfile.TemporaryDirectory() as workspace:
            for candidate in (workspace, os.path.join(workspace, "log")):
                with self.subTest(candidate=candidate):
                    with self.assertRaisesRegex(RuntimeError, "outside the immutable workspace"):
                        resolve_slam_runtime_log_root(candidate, workspace)

    def test_symlink_alias_into_workspace_is_rejected(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            workspace = os.path.join(temp_dir, "release")
            alias = os.path.join(temp_dir, "current")
            os.makedirs(workspace)
            os.symlink(workspace, alias)
            with self.assertRaisesRegex(RuntimeError, "outside the immutable workspace"):
                resolve_slam_runtime_log_root(os.path.join(alias, "log"), workspace)

    def test_any_commercial_release_tree_path_is_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "outside the immutable releases tree"):
            resolve_slam_runtime_log_root(
                "/opt/doraemon/releases/another-tag/log",
                "/var/tmp/candidate-workspace",
            )

    def test_arbitrary_external_tree_is_rejected(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            workspace = os.path.join(temp_dir, "release")
            sibling = os.path.join(temp_dir, "release-logs")
            with self.assertRaisesRegex(RuntimeError, "under /var/log/doraemon"):
                resolve_slam_runtime_log_root(sibling, workspace)

    def test_planner_launch_wires_external_log_root_to_runtime_node(self):
        root = ET.parse(PLANNER_LAUNCH).getroot()
        args = {item.get("name"): item.get("default") for item in root.findall("arg")}
        self.assertEqual(args.get("slam_runtime_log_root"), DEPLOYMENT_SLAM_RUNTIME_LOG_ROOT)

        runtime_nodes = [
            item
            for item in root.iter("node")
            if item.get("name") == "slam_runtime_manager"
        ]
        self.assertEqual(len(runtime_nodes), 1)
        params = {item.get("name"): item.get("value") for item in runtime_nodes[0].findall("param")}
        self.assertEqual(params.get("log_root"), "$(arg slam_runtime_log_root)")

    def test_planner_launch_exposes_runtime_repository_map_root(self):
        root = ET.parse(PLANNER_LAUNCH).getroot()
        runtime_nodes = [
            item
            for item in root.iter("node")
            if item.get("name") == "slam_runtime_manager"
        ]
        self.assertEqual(len(runtime_nodes), 1)
        params = {
            item.get("name"): item.get("value")
            for item in runtime_nodes[0].findall("param")
        }
        self.assertEqual(params.get("maps_root"), "$(arg maps_root)")
        self.assertEqual(params.get("repo_map_root"), "$(arg maps_root)")


class SlamConfigRootSecurityTest(unittest.TestCase):
    def test_default_uses_only_reviewed_external_or_canonical_release_config(self):
        expected_canonical = os.path.realpath(canonical_slam_config_root(REPO_ROOT))
        resolved = default_slam_config_root(REPO_ROOT)
        if resolved == DEPLOYMENT_SLAM_CONFIG_ROOT:
            self.assertTrue(
                is_reviewed_deployment_slam_config_root(DEPLOYMENT_SLAM_CONFIG_ROOT)
            )
        else:
            self.assertEqual(resolved, expected_canonical)

    def test_explicit_arbitrary_config_root_is_rejected(self):
        with tempfile.TemporaryDirectory(prefix="doraemon-unreviewed-slam-") as candidate:
            with self.assertRaisesRegex(RuntimeError, "outside the approved locations"):
                resolve_slam_config_root(candidate, REPO_ROOT)

    def test_incomplete_external_override_cannot_be_selected_explicitly(self):
        if is_reviewed_deployment_slam_config_root(DEPLOYMENT_SLAM_CONFIG_ROOT):
            self.skipTest("a complete reviewed external override is installed")
        with self.assertRaisesRegex(RuntimeError, "not a reviewed root:a read-only tree"):
            resolve_slam_config_root(DEPLOYMENT_SLAM_CONFIG_ROOT, REPO_ROOT)

    def test_nonempty_incomplete_external_override_cannot_fall_back(self):
        with tempfile.TemporaryDirectory(prefix="doraemon-incomplete-slam-") as candidate:
            with open(os.path.join(candidate, "unexpected.txt"), "w", encoding="utf-8") as handle:
                handle.write("incomplete\n")
            with mock.patch(
                "coverage_planner.slam_workflow.node_bootstrap.DEPLOYMENT_SLAM_CONFIG_ROOT",
                candidate,
            ):
                with self.assertRaisesRegex(
                    RuntimeError, "exists but is not reviewed and read-only"
                ):
                    resolve_slam_config_root("", REPO_ROOT)

    def test_shell_and_installer_enforce_the_same_read_only_override_contract(self):
        with open(SLAM_ENV_SCRIPT, "r", encoding="utf-8") as source_file:
            source_text = source_file.read()
        with open(RUNTIME_INSTALLER, "r", encoding="utf-8") as installer_file:
            installer_text = installer_file.read()
        with open(VISUALWEB_HEADER, "r", encoding="utf-8") as visualweb_file:
            visualweb_text = visualweb_file.read()

        self.assertIn("commercial_validate_slam_config_override_tree", source_text)
        self.assertIn(
            "commercial_validate_slam_config_override_tree /data/config/slam/cartographer 0",
            installer_text,
        )
        self.assertIn("Any content makes the override authoritative", source_text)
        self.assertIn('-o root -g "${SERVICE_GROUP}" -m 0750', installer_text)
        self.assertNotIn(
            'deployment_candidate("/data/config/slam/cartographer")',
            visualweb_text,
        )


if __name__ == "__main__":
    unittest.main()

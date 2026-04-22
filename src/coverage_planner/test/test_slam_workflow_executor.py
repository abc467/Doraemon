#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import unittest


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
PKG_DIR = os.path.dirname(THIS_DIR)
SRC_DIR = os.path.join(PKG_DIR, "src")

if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from coverage_planner.slam_workflow import (
    LocalizationRequest,
    RuntimeLocalizationSnapshot,
    WorkflowRuntimeExecutor,
    resolve_target_map_name,
    should_restart_localization_after_save,
)


class SlamWorkflowExecutorTest(unittest.TestCase):
    def test_prepare_for_task_returns_ready_without_restart(self):
        restart_calls = []
        sync_calls = []

        executor = WorkflowRuntimeExecutor(
            response_factory=lambda **kwargs: dict(kwargs),
            get_runtime_snapshot=lambda _robot_id: RuntimeLocalizationSnapshot(
                active_map_name="demo_map",
                active_map_revision_id="rev_demo_01",
                current_mode="localization",
                runtime_map_name="demo_map",
                runtime_map_revision_id="rev_demo_01",
                localization_state="localized",
                localization_valid=True,
            ),
            restart_localization=lambda **kwargs: restart_calls.append(kwargs),
            sync_task_ready_state=lambda **kwargs: sync_calls.append(dict(kwargs)),
        )

        result = executor.prepare_for_task(
            LocalizationRequest(
                robot_id="local_robot",
                map_name="demo_map",
                operation=6,
            )
        )

        self.assertTrue(result["success"])
        self.assertEqual(result["message"], "task already ready")
        self.assertEqual(result["map_revision_id"], "rev_demo_01")
        self.assertEqual(restart_calls, [])
        self.assertEqual(
            sync_calls,
            [
                {
                    "robot_id": "local_robot",
                    "map_name": "demo_map",
                    "map_revision_id": "rev_demo_01",
                    "localization_state": "localized",
                    "localization_valid": True,
                    "current_mode": "localization",
                }
            ],
        )

    def test_prepare_for_task_uses_restart_when_not_ready(self):
        restart_calls = []

        def restart_localization(**kwargs):
            restart_calls.append(dict(kwargs))
            return {"success": True, "map_name": kwargs["map_name"]}

        executor = WorkflowRuntimeExecutor(
            response_factory=lambda **kwargs: dict(kwargs),
            get_runtime_snapshot=lambda _robot_id: RuntimeLocalizationSnapshot(
                active_map_name="demo_map",
                active_map_revision_id="rev_demo_01",
                current_mode="localization",
                runtime_map_name="demo_map",
                runtime_map_revision_id="rev_other",
                localization_state="not_localized",
                localization_valid=False,
            ),
            restart_localization=restart_localization,
        )

        result = executor.prepare_for_task(
            LocalizationRequest(
                robot_id="local_robot",
                map_name="",
                operation=6,
                frame_id="map",
                has_initial_pose=True,
                initial_pose_x=1.2,
                initial_pose_y=3.4,
                initial_pose_yaw=0.5,
            )
        )

        self.assertTrue(result["success"])
        self.assertEqual(len(restart_calls), 1)
        self.assertEqual(restart_calls[0]["map_name"], "demo_map")
        self.assertTrue(restart_calls[0]["has_initial_pose"])
        self.assertEqual(restart_calls[0]["map_revision_id"], "")

    def test_prepare_for_task_restarts_when_runtime_revision_differs(self):
        restart_calls = []

        executor = WorkflowRuntimeExecutor(
            response_factory=lambda **kwargs: dict(kwargs),
            get_runtime_snapshot=lambda _robot_id: RuntimeLocalizationSnapshot(
                active_map_name="demo_map",
                active_map_revision_id="rev_demo_01",
                current_mode="localization",
                runtime_map_name="demo_map",
                runtime_map_revision_id="rev_demo_02",
                localization_state="localized",
                localization_valid=True,
            ),
            restart_localization=lambda **kwargs: restart_calls.append(dict(kwargs)) or {"success": True},
        )

        result = executor.prepare_for_task(
            LocalizationRequest(
                robot_id="local_robot",
                map_name="demo_map",
                operation=6,
            )
        )

        self.assertTrue(result["success"])
        self.assertEqual(len(restart_calls), 1)
        self.assertEqual(restart_calls[0]["map_name"], "demo_map")

    def test_prepare_for_task_passes_explicit_map_revision_id_to_restart(self):
        restart_calls = []

        executor = WorkflowRuntimeExecutor(
            response_factory=lambda **kwargs: dict(kwargs),
            get_runtime_snapshot=lambda _robot_id: RuntimeLocalizationSnapshot(
                active_map_name="demo_map",
                active_map_revision_id="rev_demo_01",
                current_mode="localization",
                runtime_map_name="demo_map",
                runtime_map_revision_id="rev_other",
                localization_state="not_localized",
                localization_valid=False,
            ),
            restart_localization=lambda **kwargs: restart_calls.append(dict(kwargs)) or {"success": True},
        )

        result = executor.prepare_for_task(
            LocalizationRequest(
                robot_id="local_robot",
                map_name="demo_map",
                map_revision_id="rev_demo_03",
                operation=6,
            )
        )

        self.assertTrue(result["success"])
        self.assertEqual(restart_calls[0]["map_revision_id"], "rev_demo_03")

    def test_prepare_for_task_returns_ready_from_revision_even_if_runtime_name_missing(self):
        restart_calls = []

        executor = WorkflowRuntimeExecutor(
            response_factory=lambda **kwargs: dict(kwargs),
            get_runtime_snapshot=lambda _robot_id: RuntimeLocalizationSnapshot(
                active_map_name="demo_map",
                active_map_revision_id="rev_demo_03",
                current_mode="localization",
                runtime_map_name="",
                runtime_map_revision_id="rev_demo_03",
                localization_state="localized",
                localization_valid=True,
            ),
            restart_localization=lambda **kwargs: restart_calls.append(kwargs),
        )

        result = executor.prepare_for_task(
            LocalizationRequest(
                robot_id="local_robot",
                map_name="demo_map",
                map_revision_id="rev_demo_03",
                operation=6,
            )
        )

        self.assertTrue(result["success"])
        self.assertEqual(result["message"], "task already ready")
        self.assertEqual(result["map_revision_id"], "rev_demo_03")
        self.assertEqual(restart_calls, [])

    def test_prepare_for_task_restarts_when_localization_degraded(self):
        restart_calls = []

        executor = WorkflowRuntimeExecutor(
            response_factory=lambda **kwargs: dict(kwargs),
            get_runtime_snapshot=lambda _robot_id: RuntimeLocalizationSnapshot(
                active_map_name="demo_map",
                active_map_revision_id="rev_demo_03",
                current_mode="localization",
                runtime_map_name="demo_map",
                runtime_map_revision_id="rev_demo_03",
                localization_state="degraded",
                localization_valid=False,
            ),
            restart_localization=lambda **kwargs: restart_calls.append(dict(kwargs)) or {"success": True},
        )

        result = executor.prepare_for_task(
            LocalizationRequest(
                robot_id="local_robot",
                map_name="demo_map",
                map_revision_id="rev_demo_03",
                operation=6,
            )
        )

        self.assertTrue(result["success"])
        self.assertEqual(len(restart_calls), 1)
        self.assertEqual(restart_calls[0]["map_revision_id"], "rev_demo_03")

    def test_stop_mapping_allows_empty_active_map_for_first_mapping_cycle(self):
        restart_calls = []
        executor = WorkflowRuntimeExecutor(
            response_factory=lambda **kwargs: dict(kwargs),
            get_runtime_snapshot=lambda _robot_id: RuntimeLocalizationSnapshot(),
            restart_localization=lambda **kwargs: restart_calls.append(dict(kwargs)) or {"success": True, **kwargs},
        )

        result = executor.stop_mapping(robot_id="local_robot", map_name="", operation=5)

        self.assertTrue(result["success"])
        self.assertEqual(len(restart_calls), 1)
        self.assertEqual(restart_calls[0]["map_name"], "")

    def test_resolve_target_map_name_uses_active_map_when_request_map_missing(self):
        self.assertEqual(resolve_target_map_name("", "active_map"), "active_map")
        self.assertEqual(resolve_target_map_name("explicit_map", "active_map"), "explicit_map")

    def test_should_restart_localization_after_save_requires_both_flags(self):
        self.assertFalse(
            should_restart_localization_after_save(
                switch_to_localization_after_save=True,
                relocalize_after_switch=False,
            )
        )
        self.assertTrue(
            should_restart_localization_after_save(
                switch_to_localization_after_save=True,
                relocalize_after_switch=True,
            )
        )


if __name__ == "__main__":
    unittest.main()

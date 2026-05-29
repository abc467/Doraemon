#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import types
import unittest


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
PKG_DIR = os.path.dirname(THIS_DIR)
SRC_DIR = os.path.join(PKG_DIR, "src")

if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from coverage_planner.manual_drive_controller import ManualDriveConfig, ManualDriveSafetyController


def _ready_slam_state():
    return types.SimpleNamespace(
        current_mode="localization",
        localization_state="localized",
        localization_valid=True,
        active_map_match=True,
        busy=False,
        active_job_id="",
        task_running=False,
        pending_map_switch_status="",
    )


def _idle_task_state():
    return types.SimpleNamespace(
        mission_state="IDLE",
        public_state="IDLE",
        executor_state="IDLE",
        active_job_id="",
        run_id="",
        interlock_active=False,
        interlock_reason="",
    )


def _valid_odometry_state():
    return types.SimpleNamespace(odom_valid=True, error_code="", message="")


def _ready_combined_status():
    return types.SimpleNamespace(status=[False, False, False, False, False, False, True, True], overall_ready=True)


class ManualDriveSafetyControllerTest(unittest.TestCase):
    def setUp(self):
        self.controller = ManualDriveSafetyController(ManualDriveConfig())
        self.strict_controller = ManualDriveSafetyController(
            ManualDriveConfig(
                require_role=True,
                require_slam_state=True,
                require_task_state=True,
                require_odometry_state=True,
                require_combined_status=True,
            )
        )
        self.now = 100.0

    def _blockers(self, controller=None, **kwargs):
        defaults = dict(
            now=self.now,
            slam_state=_ready_slam_state(),
            slam_state_ts=self.now,
            task_state=_idle_task_state(),
            task_state_ts=self.now,
            odometry_state=_valid_odometry_state(),
            odometry_state_ts=self.now,
            combined_status=_ready_combined_status(),
            combined_status_ts=self.now,
            caller_role="engineer",
            caller_capabilities=[],
        )
        defaults.update(kwargs)
        return (controller or self.controller).safety_blockers(**defaults)

    def test_allows_manual_drive_by_default_when_robot_state_is_missing(self):
        blockers = self._blockers(
            slam_state=None,
            slam_state_ts=0.0,
            task_state=None,
            task_state_ts=0.0,
            odometry_state=None,
            odometry_state_ts=0.0,
            combined_status=None,
            combined_status_ts=0.0,
            caller_role="",
        )
        self.assertEqual(blockers, [])

    def test_allows_engineer_when_robot_idle_and_localized(self):
        self.assertEqual(self._blockers(), [])

    def test_allows_operator_role_by_default(self):
        blockers = self._blockers(caller_role="operator")
        self.assertEqual(blockers, [])

    def test_blocks_mapping_mode(self):
        slam = _ready_slam_state()
        slam.current_mode = "mapping"
        blockers = self._blockers(controller=self.strict_controller, slam_state=slam)
        self.assertIn("SLAM is in mapping mode", blockers)

    def test_blocks_unlocalized_runtime(self):
        slam = _ready_slam_state()
        slam.localization_state = "manual_assist_required"
        slam.localization_valid = False
        blockers = self._blockers(controller=self.strict_controller, slam_state=slam)
        self.assertIn("localization not ready: state=manual_assist_required valid=false", blockers)

    def test_blocks_paused_or_running_task(self):
        task = _idle_task_state()
        task.mission_state = "PAUSED"
        blockers = self._blockers(controller=self.strict_controller, task_state=task)
        self.assertIn("task mission_state is PAUSED", blockers)

    def test_blocks_emergency_stop(self):
        status = _ready_combined_status()
        status.status[0] = True
        blockers = self._blockers(controller=self.strict_controller, combined_status=status)
        self.assertIn("emergency stop 1 is active", blockers)

    def test_velocity_mapping_and_limits(self):
        self.assertEqual(self.controller.velocity_for_request(direction="forward", linear_mps=9.0), (0.3, 0.0, 0.0))
        self.assertEqual(self.controller.velocity_for_request(direction="backward", linear_mps=9.0), (-0.3, 0.0, 0.0))
        self.assertEqual(
            self.controller.velocity_for_request(direction="turn_left", angular_radps=9.0),
            (0.0, 0.0, 0.5),
        )
        self.assertEqual(
            self.controller.velocity_for_request(direction="turn_right", angular_radps=9.0),
            (0.0, 0.0, -0.5),
        )

    def test_watchdog_duration_is_capped(self):
        self.assertEqual(self.controller.requested_duration_ms(0), 1000)
        self.assertEqual(self.controller.requested_duration_ms(30), 100)
        self.assertEqual(self.controller.requested_duration_ms(300), 300)
        self.assertEqual(self.controller.requested_duration_ms(900), 900)
        self.assertEqual(self.controller.requested_duration_ms(1200), 1000)


if __name__ == "__main__":
    unittest.main()

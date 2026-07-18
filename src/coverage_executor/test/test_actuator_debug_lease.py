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

from coverage_executor.actuator_debug_lease import (  # noqa: E402
    ActuatorDebugSafetyLimits,
    ActuatorDebugSafetySnapshot,
    WallClockLease,
    entry_safety_violation,
    runtime_safety_violation,
)


def _safe_snapshot(**overrides):
    values = {
        "executor_state": "IDLE",
        "run_thread_active": False,
        "linear_speed_mps": 0.0,
        "angular_speed_rps": 0.0,
        "odom_age_s": 0.1,
        "mcore_connected_seen": True,
        "mcore_connected": True,
        "telemetry_seen": True,
        "telemetry_age_s": 0.1,
        "telemetry_generation": 1,
        "safety_status_seen": True,
        "safety_status_age_s": 0.1,
        "safety_status_generation": 1,
        "emergency_stop_active": False,
    }
    values.update(overrides)
    return ActuatorDebugSafetySnapshot(**values)


class ActuatorDebugSafetyPolicyTest(unittest.TestCase):
    def setUp(self):
        self.limits = ActuatorDebugSafetyLimits()

    def test_safe_idle_snapshot_can_enter(self):
        self.assertIsNone(entry_safety_violation(_safe_snapshot(), self.limits))

    def test_entry_requires_idle_and_no_run_thread(self):
        self.assertEqual(
            entry_safety_violation(_safe_snapshot(executor_state="PAUSED"), self.limits),
            "executor must be IDLE",
        )
        self.assertEqual(
            entry_safety_violation(_safe_snapshot(run_thread_active=True), self.limits),
            "executor run thread is active",
        )

    def test_runtime_fails_closed_for_platform_health(self):
        self.assertIn(
            "M-core disconnected",
            runtime_safety_violation(_safe_snapshot(mcore_connected=False), self.limits),
        )
        self.assertIn(
            "safety status",
            runtime_safety_violation(_safe_snapshot(safety_status_age_s=3.0), self.limits),
        )
        self.assertIn(
            "safety status unavailable",
            runtime_safety_violation(
                _safe_snapshot(safety_status_seen=False),
                ActuatorDebugSafetyLimits(require_authoritative_safety_status=True),
            ),
        )
        self.assertIn(
            "emergency stop",
            runtime_safety_violation(_safe_snapshot(emergency_stop_active=True), self.limits),
        )

    def test_compatibility_mode_requires_telemetry_but_allows_missing_optional_safety_byte(self):
        self.assertIsNone(
            runtime_safety_violation(
                _safe_snapshot(
                    safety_status_seen=False,
                    safety_status_age_s=float("inf"),
                ),
                ActuatorDebugSafetyLimits(require_authoritative_safety_status=False),
            )
        )
        self.assertIn(
            "telemetry unavailable",
            runtime_safety_violation(
                _safe_snapshot(telemetry_seen=False),
                ActuatorDebugSafetyLimits(require_authoritative_safety_status=False),
            ),
        )

    def test_runtime_rejects_stale_odom_and_motion(self):
        self.assertIn(
            "odometry",
            runtime_safety_violation(_safe_snapshot(odom_age_s=2.0), self.limits),
        )
        self.assertIn(
            "chassis moving",
            runtime_safety_violation(_safe_snapshot(linear_speed_mps=0.04), self.limits),
        )
        self.assertIn(
            "chassis moving",
            runtime_safety_violation(_safe_snapshot(angular_speed_rps=0.06), self.limits),
        )


class WallClockLeaseTest(unittest.TestCase):
    def test_enable_renew_expire_disable(self):
        lease = WallClockLease(30.0)
        self.assertFalse(lease.enable_or_renew(100.0))
        self.assertAlmostEqual(lease.remaining_s(110.0), 20.0)
        self.assertFalse(lease.expired(129.9))
        self.assertTrue(lease.enable_or_renew(120.0))
        self.assertFalse(lease.expired(149.9))
        self.assertTrue(lease.expired(150.0))
        lease.disable()
        self.assertFalse(lease.active)
        self.assertEqual(lease.remaining_s(151.0), 0.0)


if __name__ == "__main__":
    unittest.main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import threading
import unittest

from robot_platform_msgs.msg import CombinedStatus
from std_msgs.msg import Bool, UInt64, UInt8

from coverage_executor.cleaning_subsystem import CleaningSubsystem, FeedbackState


class CleaningSafetyStatusTest(unittest.TestCase):
    def _subsystem(self):
        subsystem = CleaningSubsystem.__new__(CleaningSubsystem)
        subsystem._lock = threading.Lock()
        subsystem._feedback = FeedbackState()
        subsystem._mcore_connected_seen = True
        subsystem._mcore_connected = True
        subsystem._physical_estop_active = False
        subsystem._telemetry_seen = False
        subsystem._telemetry_ts = 0.0
        subsystem._telemetry_generation = 0
        subsystem._safety_status_seen = False
        subsystem._safety_status_ts = 0.0
        subsystem._safety_status_generation = 0
        subsystem._safety_status_bits = 0
        return subsystem

    def test_combined_status_does_not_claim_fresh_physical_safety(self):
        subsystem = self._subsystem()
        combined = CombinedStatus()
        combined.brush_position = 1
        combined.scraper_position = 1
        combined.status = [True, True, False, False, False, False, False, False]

        CleaningSubsystem._on_combined_status(subsystem, combined)
        snapshot = CleaningSubsystem.get_platform_safety_snapshot(subsystem)

        self.assertFalse(snapshot["safety_status_seen"])
        self.assertEqual(snapshot["safety_status_generation"], 0)
        self.assertFalse(snapshot["emergency_stop_active"])

    def test_trusted_telemetry_heartbeat_updates_liveness_generation(self):
        subsystem = self._subsystem()

        CleaningSubsystem._on_telemetry_heartbeat(subsystem, UInt64(data=7))
        snapshot = CleaningSubsystem.get_platform_safety_snapshot(subsystem)

        self.assertTrue(snapshot["telemetry_seen"])
        self.assertEqual(snapshot["telemetry_generation"], 1)
        self.assertLess(snapshot["telemetry_age_s"], 1.0)

    def test_dedicated_status_bits_atomically_update_estop_and_generation(self):
        subsystem = self._subsystem()

        CleaningSubsystem._on_safety_status_bits(subsystem, UInt8(data=0x80))
        active = CleaningSubsystem.get_platform_safety_snapshot(subsystem)
        CleaningSubsystem._on_safety_status_bits(subsystem, UInt8(data=0x00))
        released = CleaningSubsystem.get_platform_safety_snapshot(subsystem)

        self.assertTrue(active["safety_status_seen"])
        self.assertEqual(active["safety_status_generation"], 1)
        self.assertTrue(active["emergency_stop_active"])
        self.assertEqual(active["safety_status_bits"], 0x80)
        self.assertEqual(released["safety_status_generation"], 2)
        self.assertFalse(released["emergency_stop_active"])

    def test_transport_reconnect_invalidates_old_safety_sample_without_reusing_generation(self):
        subsystem = self._subsystem()
        CleaningSubsystem._on_safety_status_bits(subsystem, UInt8(data=0x00))
        CleaningSubsystem._on_telemetry_heartbeat(subsystem, UInt64(data=1))
        subsystem._mcore_connected_seen = True
        subsystem._mcore_connected = False
        subsystem._reapply_on_reconnect = False

        CleaningSubsystem._on_mcore_connected(subsystem, Bool(data=True))
        reconnected = CleaningSubsystem.get_platform_safety_snapshot(subsystem)

        self.assertFalse(reconnected["safety_status_seen"])
        self.assertEqual(reconnected["safety_status_generation"], 1)
        self.assertFalse(reconnected["emergency_stop_active"])
        self.assertFalse(reconnected["telemetry_seen"])
        self.assertEqual(reconnected["telemetry_generation"], 1)


if __name__ == "__main__":
    unittest.main()

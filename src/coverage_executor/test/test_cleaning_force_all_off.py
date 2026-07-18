#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import threading
import unittest
from unittest.mock import Mock


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
PKG_DIR = os.path.dirname(THIS_DIR)
SRC_DIR = os.path.join(PKG_DIR, "src")

if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from coverage_executor.cleaning_actuator import CleaningActuator  # noqa: E402
from coverage_executor.cleaning_subsystem import (  # noqa: E402
    CleaningSubsystem,
    DesiredCleaningState,
)


class _Publisher:
    def __init__(self):
        self.messages = []

    def publish(self, message):
        self.messages.append(message)


class CleaningForceAllOffTest(unittest.TestCase):
    def test_sewage_valve_off_uses_vehicle_tap_three(self):
        actuator = CleaningActuator.__new__(CleaningActuator)
        actuator._sewage_valve_tap_id = 3
        actuator._send_tap = Mock()

        actuator.sewage_valve_off()

        actuator._send_tap.assert_called_once_with(3, 0)

    def test_force_all_off_closes_sewage_each_round_despite_other_failures(self):
        subsystem = CleaningSubsystem.__new__(CleaningSubsystem)
        subsystem.act = Mock()
        subsystem.des = DesiredCleaningState(
            brush_on=True,
            scraper_on=True,
            vacuum_on=True,
            water_on=True,
        )
        subsystem._lock = threading.Lock()
        subsystem._dispatch_lock = threading.RLock()
        subsystem._vacuum_job_id = 0
        subsystem._last_brush = True
        subsystem._last_scraper = True
        subsystem._last_vac = True
        subsystem._last_water = True
        subsystem._actuator_force_off_repeat = 2
        subsystem._actuator_force_off_repeat_interval_s = 0.0
        subsystem._charge_enable_pub = _Publisher()
        subsystem._station_control_pub = _Publisher()

        def dispatch_with_failure(channel, _enabled, transition=False):
            if channel == "water":
                raise RuntimeError("simulated water close failure")

        subsystem._dispatch_channel = dispatch_with_failure

        subsystem.force_all_off(reason="test", repeats=2)

        self.assertEqual(subsystem.act.sewage_valve_off.call_count, 2)
        self.assertEqual(subsystem.act.side_brush_off.call_count, 2)
        self.assertEqual(len(subsystem._charge_enable_pub.messages), 2)
        self.assertEqual(
            [(msg.operation, msg.status) for msg in subsystem._station_control_pub.messages],
            [(1, False), (11, False), (3, False)] * 2,
        )


if __name__ == "__main__":
    unittest.main()

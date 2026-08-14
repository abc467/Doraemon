#!/usr/bin/env python3

import os
import sys
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPT_DIR = os.path.join(os.path.dirname(THIS_DIR), "scripts")
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from dock_supply_manager import DockSupplyError, DockSupplyManager


class DockSupplyWorkflowTest(unittest.TestCase):
    def _drain_manager(self):
        manager = DockSupplyManager.__new__(DockSupplyManager)
        manager.enable_drain = True
        manager.drain_timeout_s = 10.0
        manager.enable_refill = True
        manager.target_clean_level = 74
        manager.refill_timeout_s = 10.0
        manager.refill_settle_s = 20.0
        manager.combined_status_wait_s = 1.0
        manager.combined_status_stale_timeout_s = 3.0
        manager._comb = SimpleNamespace(sewage_level=50, clean_level=0)
        manager._comb_ts = time.time()
        manager._is_canceled = Mock(return_value=False)
        manager._set_state = Mock()
        manager._station_cmd = Mock()
        manager._tap_cmd = Mock()
        manager._stop_move = Mock()
        return manager

    def test_post_charge_drain_stops_only_after_zero(self):
        manager = self._drain_manager()
        events = []
        manager._wait_for_fresh_combined_level = Mock(return_value=50)
        manager._latest_combined_level = Mock(side_effect=[25, 0])
        manager._tap_cmd = Mock(side_effect=lambda tap_id, operation: events.append(("tap", tap_id, operation)))
        manager._station_cmd = Mock(side_effect=lambda operation, enabled: events.append(("station", operation, enabled)))

        with patch("dock_supply_manager.rospy.sleep"), patch(
            "dock_supply_manager.rospy.is_shutdown", return_value=False
        ):
            manager._run_drain_phase()

        manager._set_state.assert_called_once_with("DRAINING")
        self.assertEqual(manager._tap_cmd.call_args_list[0].args, (3, 1))
        self.assertEqual(manager._tap_cmd.call_args_list[-1].args, (3, 0))
        self.assertEqual(manager._station_cmd.call_args_list[0].args, (3, True))
        self.assertEqual(manager._station_cmd.call_args_list[-1].args, (3, False))
        self.assertEqual(
            events,
            [
                ("tap", 3, 1),
                ("station", 3, True),
                ("station", 3, False),
                ("tap", 3, 0),
            ],
        )

    def test_refill_settle_holds_robot_stopped_for_twenty_seconds(self):
        manager = self._drain_manager()

        with patch("dock_supply_manager.time.monotonic", side_effect=[100.0, 100.0, 120.0]), patch(
            "dock_supply_manager.rospy.sleep"
        ), patch("dock_supply_manager.rospy.is_shutdown", return_value=False):
            manager._run_refill_settle_phase()

        manager._set_state.assert_called_once_with("REFILL_SETTLING")
        self.assertEqual(manager._stop_move.call_count, 2)

    def test_drain_timeout_fails_and_closes_outputs(self):
        manager = self._drain_manager()
        manager.drain_timeout_s = 0.0
        manager._wait_for_fresh_combined_level = Mock(return_value=50)

        with patch("dock_supply_manager.rospy.sleep"), patch(
            "dock_supply_manager.rospy.is_shutdown", return_value=False
        ):
            with self.assertRaises(DockSupplyError) as raised:
                manager._run_drain_phase()

        self.assertEqual(raised.exception.code, "FAILED_DRAIN_TIMEOUT")
        self.assertEqual(manager._tap_cmd.call_args_list[-1].args, (3, 0))
        self.assertEqual(manager._station_cmd.call_args_list[-1].args, (3, False))

    def test_missing_sewage_status_never_opens_drain(self):
        manager = self._drain_manager()
        manager._wait_for_fresh_combined_level = Mock(
            side_effect=DockSupplyError("FAILED_SEWAGE_STATUS_STALE", "stale")
        )

        with self.assertRaises(DockSupplyError) as raised:
            manager._run_drain_phase()

        self.assertEqual(raised.exception.code, "FAILED_SEWAGE_STATUS_STALE")
        manager._tap_cmd.assert_not_called()
        manager._station_cmd.assert_not_called()

    def test_sewage_zero_requires_fresh_combined_status(self):
        manager = self._drain_manager()
        manager._comb.sewage_level = 0
        manager._comb_ts = time.time()
        self.assertEqual(manager._latest_combined_level("sewage_level"), 0)

        manager._comb_ts = time.time() - 4.0
        self.assertIsNone(manager._latest_combined_level("sewage_level"))

    def test_refill_disabled_sends_no_water_commands(self):
        manager = self._drain_manager()
        manager.enable_refill = False

        manager._run_refill_phase()

        manager._tap_cmd.assert_not_called()
        manager._station_cmd.assert_not_called()

    def test_refill_keeps_running_at_74_and_stops_at_75_before_wait(self):
        manager = self._drain_manager()
        events = []
        manager._wait_for_fresh_combined_level = Mock(return_value=74)
        manager._latest_combined_level = Mock(side_effect=[74, 75])
        manager._tap_cmd = Mock(side_effect=lambda tap_id, operation: events.append(("tap", tap_id, operation)))
        manager._station_cmd = Mock(side_effect=lambda operation, enabled: events.append(("station", operation, enabled)))
        manager._run_refill_settle_phase = Mock(side_effect=lambda: events.append(("wait", 20)))

        with patch("dock_supply_manager.rospy.sleep"), patch(
            "dock_supply_manager.rospy.is_shutdown", return_value=False
        ):
            manager._run_refill_phase()

        manager._set_state.assert_called_once_with("REFILLING")
        self.assertEqual(
            events,
            [
                ("tap", 2, 1),
                ("station", 11, True),
                ("station", 11, False),
                ("tap", 2, 0),
                ("wait", 20),
            ],
        )

    def test_refill_already_above_74_skips_outputs_but_still_waits(self):
        manager = self._drain_manager()
        manager._wait_for_fresh_combined_level = Mock(return_value=75)
        manager._run_refill_settle_phase = Mock()

        manager._run_refill_phase()

        manager._tap_cmd.assert_not_called()
        manager._station_cmd.assert_not_called()
        manager._run_refill_settle_phase.assert_called_once_with()

    def test_refill_stale_status_fails_and_closes_outputs_without_wait(self):
        manager = self._drain_manager()
        manager._wait_for_fresh_combined_level = Mock(return_value=20)
        manager._latest_combined_level = Mock(return_value=None)
        manager._run_refill_settle_phase = Mock()

        with patch("dock_supply_manager.rospy.sleep"), patch(
            "dock_supply_manager.rospy.is_shutdown", return_value=False
        ):
            with self.assertRaises(DockSupplyError) as raised:
                manager._run_refill_phase()

        self.assertEqual(raised.exception.code, "FAILED_CLEAN_STATUS_STALE")
        self.assertEqual(manager._station_cmd.call_args_list[-1].args, (11, False))
        self.assertEqual(manager._tap_cmd.call_args_list[-1].args, (2, 0))
        manager._run_refill_settle_phase.assert_not_called()

    def test_production_runtime_wires_refill_threshold_timeout_and_wait(self):
        repo_root = os.path.dirname(os.path.dirname(os.path.dirname(THIS_DIR)))
        with open(os.path.join(repo_root, "config", "runtime.a26022.env"), encoding="utf-8") as handle:
            runtime_env = handle.read()
        with open(os.path.join(repo_root, "scripts", "start_runtime.sh"), encoding="utf-8") as handle:
            start_runtime = handle.read()
        with open(
            os.path.join(repo_root, "src", "robot_hw_bridge", "launch", "hardware_bridges.launch"),
            encoding="utf-8",
        ) as handle:
            hardware_launch = handle.read()

        for source in (runtime_env, start_runtime):
            self.assertIn("DOCK_SUPPLY_ENABLE_REFILL=true", source)
            self.assertIn("DOCK_SUPPLY_TARGET_CLEAN_LEVEL=74", source)
            self.assertIn("DOCK_SUPPLY_REFILL_TIMEOUT_S=600.0", source)
            self.assertIn("DOCK_SUPPLY_REFILL_SETTLE_S=20.0", source)
        self.assertIn('<arg name="dock_supply_enable_refill" default="true"/>', hardware_launch)
        self.assertIn('<arg name="dock_supply_target_clean_level" default="74"/>', hardware_launch)
        self.assertIn('<arg name="dock_supply_refill_settle_s" default="20.0"/>', hardware_launch)

    def test_workflow_charges_then_disables_charge_then_drains(self):
        manager = DockSupplyManager.__new__(DockSupplyManager)
        events = []
        manager.pre_dock_settle_s = 0.0
        manager.direct_charge_after_precise_docking = True
        manager.mechanical_connect_enable = False
        manager._run_precise_docking = Mock()
        manager._stop_move = Mock()
        manager._is_canceled = Mock(return_value=False)
        manager._set_state = Mock()
        manager._run_charge_phase = Mock(side_effect=lambda: events.append("charge"))
        manager._charge_enable = Mock(side_effect=lambda enabled: events.append("charge_on" if enabled else "charge_off"))
        manager._run_drain_phase = Mock(side_effect=lambda: events.append("drain"))
        manager._run_refill_phase = Mock(side_effect=lambda: events.append("refill"))
        manager._defer_exit_enabled = Mock(return_value=False)
        manager._run_exit_sequence = Mock(side_effect=lambda: events.append("exit"))
        manager._safe_abort = Mock()

        with patch("dock_supply_manager.rospy.sleep"):
            manager._run()

        self.assertEqual(events, ["charge", "charge_off", "drain", "refill", "exit"])
        manager._safe_abort.assert_not_called()

    def test_charge_repeat_never_reopens_after_cancel_is_observed(self):
        manager = DockSupplyManager.__new__(DockSupplyManager)
        manager.charge_cmd_repeat = 3
        manager.charge_cmd_interval_s = 0.0
        manager.station_charge_enable_repeat = 3
        manager.station_charge_enable_interval_s = 0.0
        manager._lock = threading.Lock()
        manager._charge_io_lock = threading.RLock()
        manager._cancel = False
        manager._station_cmd = Mock()

        def cancel_after_first_publish(_message):
            with manager._lock:
                manager._cancel = True

        manager._charge_pub = Mock()
        manager._charge_pub.publish.side_effect = cancel_after_first_publish

        with self.assertRaises(DockSupplyError) as raised:
            manager._charge_enable(True)

        self.assertEqual(raised.exception.code, "CANCELED")
        self.assertEqual(manager._charge_pub.publish.call_count, 1)
        manager._station_cmd.assert_not_called()

    def test_cancel_service_immediately_forces_outputs_off_for_active_workflow(self):
        manager = DockSupplyManager.__new__(DockSupplyManager)
        manager._lock = threading.Lock()
        manager._cancel = False
        manager._thread = Mock()
        manager._thread.is_alive.return_value = True
        manager._safe_abort = Mock()

        response = manager._srv_cancel_cb(None)

        self.assertTrue(response.success)
        self.assertTrue(manager._cancel)
        manager._safe_abort.assert_called_once_with()

    def test_cancel_service_is_idempotent_when_workflow_is_quiescent(self):
        manager = DockSupplyManager.__new__(DockSupplyManager)
        manager._lock = threading.Lock()
        manager._cancel = False
        manager._thread = None
        manager._safe_abort = Mock()

        response = manager._srv_cancel_cb(None)

        self.assertTrue(response.success)
        self.assertEqual(response.message, "already quiescent")
        manager._safe_abort.assert_not_called()


if __name__ == "__main__":
    unittest.main()

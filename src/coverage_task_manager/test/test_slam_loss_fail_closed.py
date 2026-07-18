import threading
import unittest
from unittest import mock

from cleanrobot_app_msgs.msg import SlamState
from std_msgs.msg import String

from coverage_task_manager.task_manager import SlamStateSnapshot, TaskManager


class _NavClient:
    def __init__(self):
        self.cancel_count = 0

    def cancel_all(self):
        self.cancel_count += 1


class _Publisher:
    def __init__(self):
        self.messages = []

    def publish(self, msg):
        self.messages.append(msg)


class SlamLossFailClosedTest(unittest.TestCase):
    def _manager(self):
        mgr = TaskManager.__new__(TaskManager)
        mgr._lock = threading.Lock()
        mgr._slam_state = SlamStateSnapshot()
        mgr._mission_state = "RUNNING"
        mgr._phase = "IDLE"
        mgr._public_state = "RUNNING"
        mgr._active_run_id = "run_72"
        mgr._slam_had_valid_localization = False
        mgr._slam_localization_lost_active = False
        mgr._slam_localization_loss_pause_applied = False
        mgr._slam_localization_loss_episode = ""
        mgr._slam_localization_loss_last_enforce_ts = 0.0
        mgr._slam_localization_lost_reason = ""
        mgr._health_fault_active = False
        mgr._health_error_code = ""
        mgr._health_error_msg = ""
        mgr._health_recover_pending = True
        mgr._health_recover_resume_after_ts = 999.0
        mgr._health_recover_run_id = "run_72"
        mgr._health_recover_code = "TF_LOOKUP_FAIL"
        mgr._executor_state = "RUNNING"
        mgr.nav = _NavClient()
        mgr._dock_stage2_nav = _NavClient()
        mgr.exec_commands = []
        mgr.mission_updates = []
        mgr.public_states = []
        mgr.events = []
        mgr._send_exec_cmd = lambda cmd: mgr.exec_commands.append(str(cmd))
        mgr._mission_update_state = lambda run_id, state, reason="": mgr.mission_updates.append(
            (str(run_id), str(state), str(reason))
        )
        mgr._publish_state = lambda state: (
            setattr(mgr, "_public_state", str(state)), mgr.public_states.append(str(state))
        )
        mgr._emit = lambda event: mgr.events.append(str(event))
        mgr._persist_if_changed = lambda: None
        return mgr

    @staticmethod
    def _slam_state(*, valid, state, manual_assist=False, reason=""):
        msg = SlamState()
        msg.localization_valid = bool(valid)
        msg.localization_state = str(state)
        msg.manual_assist_required = bool(manual_assist)
        msg.blocking_reason = str(reason)
        return msg

    @mock.patch("coverage_task_manager.task_manager.rospy.logerr")
    def test_valid_to_confirmed_loss_pauses_once_and_cancels_navigation(self, _logerr):
        mgr = self._manager()

        mgr._on_slam_state(self._slam_state(valid=True, state="localized"))
        lost = self._slam_state(
            valid=False,
            state="manual_assist_required",
            manual_assist=True,
            reason="confirmed lost episode=4",
        )
        mgr._on_slam_state(lost)
        mgr._on_slam_state(lost)

        self.assertEqual(mgr._mission_state, "PAUSED")
        self.assertEqual(mgr._phase, "IDLE")
        self.assertEqual(mgr._public_state, "PAUSED_RECOVERY")
        self.assertEqual(mgr.nav.cancel_count, 1)
        self.assertEqual(mgr._dock_stage2_nav.cancel_count, 1)
        self.assertEqual(mgr.exec_commands, ["pause"])
        self.assertEqual(
            mgr.mission_updates,
            [("run_72", "PAUSED", "slam_localization_lost")],
        )
        self.assertEqual(mgr.public_states, ["PAUSED_RECOVERY"])
        self.assertEqual(
            mgr.events,
            ["SLAM_LOCALIZATION_LOST:reason=confirmed lost episode=4"],
        )
        self.assertTrue(mgr._health_fault_active)
        self.assertEqual(mgr._health_error_code, "SLAM_LOCALIZATION_LOST")
        self.assertFalse(mgr._health_recover_pending)

    def test_resume_is_rejected_until_fresh_localized_state(self):
        mgr = self._manager()
        mgr._mission_state = "PAUSED"
        mgr._slam_localization_lost_active = True
        mgr._slam_localization_lost_reason = "confirmed lost"

        ok, message = mgr._resume_current_task()

        self.assertFalse(ok)
        self.assertIn("SLAM_LOCALIZATION_LOST", message)

        mgr._on_slam_state(self._slam_state(valid=True, state="localized"))
        self.assertFalse(mgr._slam_localization_lost_active)
        self.assertEqual(mgr._health_error_code, "")

    @mock.patch("coverage_task_manager.task_manager.rospy.logerr")
    def test_running_task_fails_closed_on_first_fresh_manual_assist_after_restart(self, _logerr):
        mgr = self._manager()

        mgr._on_slam_state(
            self._slam_state(
                valid=False,
                state="manual_assist_required",
                manual_assist=True,
            )
        )

        self.assertEqual(mgr.exec_commands, ["pause"])
        self.assertEqual(mgr.public_states, ["PAUSED_RECOVERY"])

    @mock.patch("coverage_task_manager.task_manager.rospy.logerr")
    def test_same_episode_retries_safety_actions_until_executor_acknowledges_pause(self, _logerr):
        mgr = self._manager()
        lost = self._slam_state(
            valid=False,
            state="manual_assist_required",
            manual_assist=True,
            reason="SLAM_LOCALIZATION_LOST episode=9 reason=scan divergence",
        )

        mgr._on_slam_state(lost)
        mgr._slam_localization_loss_last_enforce_ts -= 2.0
        mgr._on_slam_state(lost)

        self.assertEqual(mgr.nav.cancel_count, 2)
        self.assertEqual(mgr._dock_stage2_nav.cancel_count, 2)
        self.assertEqual(mgr.exec_commands, ["pause", "pause"])
        self.assertEqual(mgr.mission_updates, [("run_72", "PAUSED", "slam_localization_lost")])
        self.assertEqual(mgr.public_states, ["PAUSED_RECOVERY"])
        self.assertEqual(len(mgr.events), 1)
        self.assertEqual(mgr._slam_localization_loss_episode, "9")
        self.assertFalse(mgr._slam_localization_loss_pause_applied)

        mgr._on_executor_state(String(data="PAUSED"))
        mgr._slam_localization_loss_last_enforce_ts -= 2.0
        mgr._on_slam_state(lost)

        self.assertEqual(mgr.exec_commands, ["pause", "pause"])
        self.assertTrue(mgr._slam_localization_loss_pause_applied)

    def test_public_state_is_clamped_while_loss_latch_is_active(self):
        mgr = TaskManager.__new__(TaskManager)
        mgr._slam_localization_lost_active = True
        mgr._state_pub = _Publisher()
        mgr._persist_if_changed = lambda: None

        TaskManager._publish_state(mgr, "PAUSED")

        self.assertEqual(mgr._public_state, "PAUSED_RECOVERY")
        self.assertEqual(mgr._state_pub.messages[-1].data, "PAUSED_RECOVERY")

        TaskManager._publish_state(mgr, "IDLE")
        self.assertEqual(mgr._public_state, "IDLE")


if __name__ == "__main__":
    unittest.main()

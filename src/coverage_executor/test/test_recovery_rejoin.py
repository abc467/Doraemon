#!/usr/bin/env python3

import unittest
from unittest import mock

from coverage_executor.fsm import ExecutorFSM
from coverage_executor.plan_loader import LoadedBlock


def _block(count=31, step=0.1):
    path = [(float(i) * step, 0.0, 0.0) for i in range(count)]
    return LoadedBlock(
        block_id=7,
        entry_xyyaw=path[0],
        exit_xyyaw=path[-1],
        path_xyyaw=path,
        length_m=float(count - 1) * step,
        point_count=count,
    )


class _FakeMBF:
    def __init__(self, target_state, current_state=0, service_ok=True):
        self.target_state = target_state
        self.current_state = int(current_state)
        self.service_ok = bool(service_ok)

    def check_global_pose(self, pose=None, *, current_pose=False):
        if not self.service_ok:
            return False, -1, 0, "service unavailable"
        if current_pose:
            return True, self.current_state, 123, ""
        x = float(pose.pose.position.x)
        state = int(self.target_state(x))
        return True, state, int(round(x * 100.0)), ""


class RecoveryRejoinTest(unittest.TestCase):
    def _fsm(self, mbf):
        fsm = ExecutorFSM.__new__(ExecutorFSM)
        fsm.mbf = mbf
        fsm.frame_id = "map"
        fsm.recovery_rejoin_back_search_m = 1.0
        fsm.recovery_rejoin_forward_search_m = 1.0
        fsm.recovery_rejoin_stable_span_m = 0.20
        fsm.recovery_rejoin_sample_step_m = 0.10
        fsm.recovery_rejoin_allow_inscribed_goal = False
        return fsm

    @mock.patch("coverage_executor.fsm.rospy.logwarn")
    def test_invalid_anchor_moves_to_nearest_stable_free_backtrack(self, _logwarn):
        # Around anchor x=1.0 the complete footprint is lethal.  Both a
        # backward and forward free region exist; equal-distance ties prefer
        # backtracking so coverage is not silently skipped.
        mbf = _FakeMBF(
            lambda x: 0 if (x <= 0.5 or x >= 1.5) else 2,
            current_state=0,
        )
        result = self._fsm(mbf)._prepare_safe_rejoin(_block(), anchor_idx=10)

        self.assertTrue(result["ok"])
        self.assertEqual(result["cut_start_idx"], 4)
        self.assertAlmostEqual(result["cut_start_s"], 0.4)
        self.assertAlmostEqual(result["cut_path_xyyaw"][0][0], 0.4)

    def test_current_hard_collision_stops_without_guessing_escape_motion(self):
        result = self._fsm(
            _FakeMBF(lambda _x: 0, current_state=2)
        )._prepare_safe_rejoin(_block(), anchor_idx=10)

        self.assertFalse(result["ok"])
        self.assertEqual(result["code"], "RECOVERY_CURRENT_POSE_UNSAFE")

    def test_inscribed_only_goal_is_rejected_for_state_planner_contract(self):
        result = self._fsm(
            _FakeMBF(lambda _x: 1, current_state=0)
        )._prepare_safe_rejoin(_block(), anchor_idx=10)

        self.assertFalse(result["ok"])
        self.assertEqual(result["code"], "RECOVERY_REJOIN_NOT_FOUND")

    def test_pose_check_service_failure_is_fail_closed(self):
        result = self._fsm(
            _FakeMBF(lambda _x: 0, service_ok=False)
        )._prepare_safe_rejoin(_block(), anchor_idx=10)

        self.assertFalse(result["ok"])
        self.assertEqual(result["code"], "RECOVERY_POSE_CHECK_UNAVAILABLE")


if __name__ == "__main__":
    unittest.main()

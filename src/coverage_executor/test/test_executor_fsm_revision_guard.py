#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import threading
import unittest
from unittest import mock


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
PKG_DIR = os.path.dirname(THIS_DIR)
SRC_DIR = os.path.join(PKG_DIR, "src")

if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from coverage_executor.fsm import ExecutorFSM
from coverage_executor.plan_loader import LoadedPlan


class _FakeLoader:
    def get_zone_meta(self, zone_id, *, map_name="", map_revision_id=""):
        return {}


class ExecutorFSMRevisionGuardTest(unittest.TestCase):
    def _fsm(self):
        fsm = ExecutorFSM.__new__(ExecutorFSM)
        fsm._lock = threading.Lock()
        fsm.strict_map_check = True
        fsm.strict_resume_map_check = True
        fsm.loader = _FakeLoader()
        fsm._get_run_snapshot_map_name = lambda: ""
        fsm._get_run_snapshot_map_revision_id = lambda: ""
        fsm._ensure_runtime_map_name = lambda: ("runtime_map", True)
        fsm._ensure_runtime_map_revision_id = lambda: "rev_expected"
        fsm._ensure_runtime_map_identity = lambda: ("runtime_map_id", "runtime_map_md5", True)
        fsm.emitted = []
        fsm.errors = []
        fsm.states = []
        fsm._emit = lambda event: fsm.emitted.append(str(event))
        fsm._set_error = lambda **kwargs: fsm.errors.append(dict(kwargs))
        fsm._publish_state = lambda state: fsm.states.append(str(state))
        return fsm

    @staticmethod
    def _plan(**overrides):
        payload = {
            "plan_id": "plan_demo",
            "zone_id": "zone_a",
            "zone_version": 1,
            "frame_id": "map",
            "map_name": "expected_map",
            "map_revision_id": "rev_expected",
            "plan_profile_name": "cover_standard",
            "constraint_version": "constraint_v1",
            "exec_order": [],
            "blocks": [],
            "total_length_m": 0.0,
            "map_id": "expected_map_id",
            "map_md5": "expected_map_md5",
            "planner_version": "planner_v1",
        }
        payload.update(overrides)
        return LoadedPlan(**payload)

    @mock.patch("coverage_executor.fsm.rospy.loginfo")
    @mock.patch("coverage_executor.fsm.rospy.logwarn")
    def test_check_map_consistency_prefers_revision_over_stale_name_md5_identity(
        self,
        logwarn,
        loginfo,
    ):
        fsm = self._fsm()
        fsm._ensure_runtime_map_name = lambda: ("runtime_alias", True)
        fsm._ensure_runtime_map_revision_id = lambda: "rev_expected"
        fsm._ensure_runtime_map_identity = lambda: ("runtime_other_id", "runtime_other_md5", True)

        ok = fsm._check_map_consistency_or_abort(self._plan())

        self.assertTrue(ok)
        self.assertEqual(fsm.errors, [])
        self.assertEqual(fsm.states, [])
        self.assertEqual(fsm.emitted, [])
        logwarn.assert_called_once()
        loginfo.assert_called()

    @mock.patch("coverage_executor.fsm.rospy.logerr")
    def test_check_map_consistency_rejects_revision_mismatch_before_name_md5_fallback(self, logerr):
        fsm = self._fsm()
        fsm._ensure_runtime_map_revision_id = lambda: "rev_runtime_other"
        fsm._ensure_runtime_map_identity = lambda: ("expected_map_id", "expected_map_md5", True)

        ok = fsm._check_map_consistency_or_abort(self._plan())

        self.assertFalse(ok)
        self.assertEqual(fsm.states, ["ERROR_MAP_MISMATCH"])
        self.assertEqual(len(fsm.errors), 1)
        self.assertEqual(fsm.errors[0]["code"], "MAP_REVISION_MISMATCH")
        self.assertIn("expected=rev_expected", fsm.errors[0]["msg"])
        self.assertIn("runtime=rev_runtime_other", fsm.errors[0]["msg"])
        self.assertEqual(len(fsm.emitted), 1)
        self.assertIn("ERROR:MAP_MISMATCH:", fsm.emitted[0])
        logerr.assert_called()


if __name__ == "__main__":
    unittest.main()

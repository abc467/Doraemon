#!/usr/bin/env python3

import unittest

from coverage_executor.fsm import ExecutorFSM
from coverage_executor.plan_loader import LoadedBlock, LoadedPlan


def _plan():
    block = LoadedBlock(
        block_id=9,
        entry_xyyaw=(0.0, 0.0, 0.0),
        exit_xyyaw=(2.0, 0.0, 0.0),
        path_xyyaw=[(0.0, 0.0, 0.0), (1.0, 0.0, 0.0), (2.0, 0.0, 0.0)],
        length_m=2.0,
        point_count=3,
    )
    return LoadedPlan(
        plan_id="plan",
        zone_id="zone",
        zone_version=1,
        frame_id="map",
        map_name="map",
        map_revision_id="rev",
        plan_profile_name="cover_standard",
        constraint_version="constraints",
        exec_order=[9],
        blocks=[block],
        total_length_m=2.0,
    )


class CheckpointBlockOwnershipTest(unittest.TestCase):
    def setUp(self):
        self.fsm = ExecutorFSM.__new__(ExecutorFSM)
        self.events = []
        self.errors = []
        self.states = []
        self.fsm._emit = self.events.append
        self.fsm._set_error = lambda **kwargs: self.errors.append(kwargs)
        self.fsm._publish_state = self.states.append

    def test_cross_block_progress_is_reset_instead_of_skipping_block(self):
        checkpoint = {
            "exec_index": 0,
            "block_id": 9,
            "path_index": 3736,
            "path_s": 186.744,
        }

        self.assertTrue(self.fsm._validate_resume_checkpoint(_plan(), checkpoint))
        self.assertEqual(checkpoint["path_index"], 0)
        self.assertEqual(checkpoint["path_s"], 0.0)
        self.assertTrue(any("CHECKPOINT_PROGRESS_RESET" in event for event in self.events))

    def test_block_id_mismatch_is_rejected(self):
        checkpoint = {
            "exec_index": 0,
            "block_id": 11,
            "path_index": 0,
            "path_s": 0.0,
        }

        self.assertFalse(self.fsm._validate_resume_checkpoint(_plan(), checkpoint))
        self.assertEqual(self.errors[-1]["code"], "INVALID_CHECKPOINT")
        self.assertEqual(self.states[-1], "ERROR_INVALID_CHECKPOINT")


if __name__ == "__main__":
    unittest.main()

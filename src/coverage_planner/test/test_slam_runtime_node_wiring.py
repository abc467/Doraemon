#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import unittest
from unittest import mock


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
PKG_DIR = os.path.dirname(THIS_DIR)
PKG_SRC_DIR = os.path.join(PKG_DIR, "src")

if PKG_SRC_DIR not in sys.path:
    sys.path.insert(0, PKG_SRC_DIR)

from coverage_planner.slam_workflow.node_wiring import SlamRuntimeNodeWiring


class SlamRuntimeNodeWiringTest(unittest.TestCase):
    def test_runtime_restart_invalidates_mapping_session_before_services(self):
        backend = mock.Mock()
        backend.map_topic = "/map"
        backend.tracked_pose_topic = "/tracked_pose"
        backend.initial_pose_topic = "/initialpose"
        backend.job_state_topic_name = "/cartographer/runtime/job_state"
        backend.app_service_name = "/cartographer/runtime/operate"
        backend.app_submit_job_service_name = "/cartographer/runtime/submit_job"
        backend.app_get_job_service_name = "/cartographer/runtime/get_job"
        backend.runtime_ns = "/cartographer/runtime"
        backend._runtime_context.runtime_param.return_value = (
            "/cartographer/runtime/mapping_session_id"
        )
        backend._runtime_adapter = None

        rospy_module = mock.Mock()
        SlamRuntimeNodeWiring(backend, rospy_module=rospy_module).wire()

        self.assertEqual(
            rospy_module.method_calls[0],
            mock.call.set_param("/cartographer/runtime/mapping_session_id", ""),
        )
        backend._job_state.restore_jobs_from_store.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()

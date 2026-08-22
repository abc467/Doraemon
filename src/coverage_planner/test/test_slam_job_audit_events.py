#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import unittest
from types import SimpleNamespace

from coverage_planner.slam_workflow.job_events import (
    CartographerSlamJobEventLogger,
    decode_submit_audit_description,
    encode_submit_audit_description,
    infer_submit_source,
)


class _Ops:
    def __init__(self):
        self.events = []

    def add_robot_event(self, **kwargs):
        self.events.append(dict(kwargs))


class _Publisher:
    def __init__(self):
        self.messages = []

    def publish(self, message):
        self.messages.append(str(getattr(message, "data", message)))


class _FailingOps:
    @staticmethod
    def add_robot_event(**_kwargs):
        raise RuntimeError("database unavailable")


class SlamJobAuditEventTest(unittest.TestCase):
    def test_private_source_marker_is_removed_from_visible_description(self):
        encoded = encode_submit_audit_description(
            "slam_api_service",
            "operator requested retry",
        )
        source, visible = decode_submit_audit_description(encoded)

        self.assertEqual(source, "slam_api_service")
        self.assertEqual(visible, "operator requested retry")
        self.assertNotIn("__doraemon_submit_source__", visible)

    def test_internal_descriptions_are_classified_without_service_md5_change(self):
        self.assertEqual(
            infer_submit_source("localization lifecycle restart_localization app path"),
            "localization_lifecycle_manager",
        )
        self.assertEqual(
            infer_submit_source("task manager prepare_for_task"),
            "coverage_task_manager",
        )

    def test_submitted_event_is_persisted_and_published_as_correlated_json(self):
        backend = SimpleNamespace(
            robot_id="CR-001",
            _ops=_Ops(),
            _audit_event_pub=_Publisher(),
        )
        logger = CartographerSlamJobEventLogger(backend)
        logger.job_submitted(
            {
                "job_id": "slam_job_1",
                "robot_id": "CR-001",
                "operation": 8,
                "operation_name": "relocalize",
                "submit_source": "slam_api_service",
                "description": "operator requested retry",
                "requested_map_name": "factory",
                "requested_map_revision_id": "rev_1",
                "created_ts": 123.0,
                "status": "queued",
                "phase": "accepted",
            }
        )

        self.assertEqual(len(backend._ops.events), 1)
        persisted = backend._ops.events[0]
        self.assertEqual(persisted["code"], "slam_job_submitted")
        self.assertEqual(persisted["job_id"], "slam_job_1")
        self.assertEqual(persisted["data"]["submit_source"], "slam_api_service")

        self.assertEqual(len(backend._audit_event_pub.messages), 1)
        published = json.loads(backend._audit_event_pub.messages[0])
        self.assertEqual(published["event"], "slam_job_submitted")
        self.assertEqual(published["job_id"], "slam_job_1")
        self.assertEqual(published["operation_name"], "relocalize")
        self.assertEqual(published["submit_source"], "slam_api_service")
        self.assertEqual(published["created_ts"], 123.0)

    def test_audit_persistence_failure_does_not_block_event_or_job_submission(self):
        backend = SimpleNamespace(
            robot_id="CR-001",
            _ops=_FailingOps(),
            _audit_event_pub=_Publisher(),
        )
        logger = CartographerSlamJobEventLogger(backend)

        logger.job_submitted(
            {
                "job_id": "slam_job_2",
                "operation": 6,
                "operation_name": "prepare_for_task",
                "submit_source": "coverage_task_manager",
            }
        )

        self.assertEqual(len(backend._audit_event_pub.messages), 1)


if __name__ == "__main__":
    unittest.main()

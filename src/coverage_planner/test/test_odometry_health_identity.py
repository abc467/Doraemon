#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import importlib.util
import os
import unittest


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
PKG_DIR = os.path.dirname(THIS_DIR)
SCRIPT_PATH = os.path.join(PKG_DIR, "scripts", "odometry_health_node.py")

SPEC = importlib.util.spec_from_file_location("odometry_health_node_identity_test", SCRIPT_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class OdometryHealthIdentityTest(unittest.TestCase):
    def test_status_service_rejects_another_vehicle_identity(self):
        node = object.__new__(MODULE.OdometryHealthNode)
        node.robot_id = "CR-001"
        node._build_state = lambda _robot_id: self.fail("mismatch must not build a spoofed state")

        response = node._handle_get_status_app(type("Req", (), {"robot_id": "CR-999"})())

        self.assertFalse(response.success)
        self.assertIn("robot_id mismatch", response.message)
        self.assertEqual(response.state.robot_id, "CR-001")


if __name__ == "__main__":
    unittest.main()

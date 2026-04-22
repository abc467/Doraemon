#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os

import rospy

from coverage_planner.ops_store.store import OperationsStore
from coverage_planner.plan_store.store import PlanStore
from coverage_planner.slam_workflow import (
    SlamApiContractController,
    CartographerRuntimeAssetHelper,
    SlamApiNodeWiring,
    SlamApiRuntimeClient,
    SlamApiRuntimeStateController,
    SlamApiStateController,
    SlamApiSubmitController,
    apply_bootstrap,
    load_slam_api_service_bootstrap,
    normalize_map_name,
)
from coverage_planner.service_mode import publish_contract_param


class SlamApiServiceNode:
    def __init__(self):
        apply_bootstrap(self, load_slam_api_service_bootstrap(__file__))

        os.makedirs(self.maps_root, exist_ok=True)
        self._normalize_map_name = normalize_map_name
        self._plan_store = PlanStore(self.plan_db_path)
        self._ops = OperationsStore(self.ops_db_path)
        self._runtime_assets = CartographerRuntimeAssetHelper(self)
        self._contract_controller = SlamApiContractController(self)
        self._runtime_state = SlamApiRuntimeStateController(self)
        self._runtime_client = SlamApiRuntimeClient(self)
        self._state_controller = SlamApiStateController(self)
        self._submit_controller = SlamApiSubmitController(self)
        self._wiring = SlamApiNodeWiring(self)
        self._map_ts = 0.0
        self._tracked_pose_ts = 0.0
        self._task_state_ts = 0.0
        self._task_state_msg = None
        self._odometry_state_ts = 0.0
        self._odometry_state_msg = None
        self._job_state_ts = 0.0
        self._job_state_msg = None

        self._app_status_contract, self._app_submit_contract, self._app_get_job_contract = (
            self._contract_controller.build_app_contract_reports()
        )
        publish_contract_param(rospy, self.app_status_contract_param_ns, self._app_status_contract, enabled=True)
        publish_contract_param(rospy, self.app_submit_command_contract_param_ns, self._app_submit_contract, enabled=True)
        publish_contract_param(rospy, self.app_get_job_contract_param_ns, self._app_get_job_contract, enabled=True)
        self._wiring.wire()

def main():
    rospy.init_node("slam_api_service", anonymous=False)
    SlamApiServiceNode()
    rospy.spin()


if __name__ == "__main__":
    main()

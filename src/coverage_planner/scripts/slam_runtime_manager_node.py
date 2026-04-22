#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import threading
from typing import Dict

import rospy
import tf2_ros

from coverage_planner.ops_store.store import OperationsStore
from coverage_planner.plan_store.store import PlanStore
from coverage_planner.slam_workflow import (
    CartographerRuntimeAssetHelper,
    CartographerRuntimeContext,
    CartographerSlamJobEventLogger,
    CartographerSlamJobRunner,
    CartographerSlamJobController,
    SlamRuntimeServiceController,
    WorkflowRuntimeExecutor,
    apply_bootstrap,
    load_slam_runtime_manager_bootstrap,
    SlamRuntimeNodeWiring,
)
from coverage_planner.slam_workflow.runtime_adapter import CartographerRuntimeAdapter
from coverage_planner.slam_workflow.runtime_state import CartographerRuntimeStateController
from coverage_planner.slam_workflow.runtime_transport import CartographerRuntimeTransport
from coverage_planner.service_mode import publish_contract_param


class SlamRuntimeManagerNode:
    def __init__(self):
        apply_bootstrap(self, load_slam_runtime_manager_bootstrap(__file__))

        os.makedirs(self.maps_root, exist_ok=True)
        os.makedirs(self.repo_map_root, exist_ok=True)
        os.makedirs(self.log_root, exist_ok=True)

        self._plan_store = PlanStore(self.plan_db_path)
        self._ops = OperationsStore(self.ops_db_path)
        self._lock = threading.Lock()
        self._managed_procs: Dict[str, Dict[str, object]] = {}
        self._runtime_context = CartographerRuntimeContext(self)
        self._runtime_state = CartographerRuntimeStateController(self)
        self._asset_helper = CartographerRuntimeAssetHelper(self)
        self._job_events = CartographerSlamJobEventLogger(self)
        self._job_state = CartographerSlamJobController(self, events=self._job_events)
        self._service_api = SlamRuntimeServiceController(self)
        self._runtime_transport = CartographerRuntimeTransport(self)
        self._runtime_adapter = CartographerRuntimeAdapter(self, transport=self._runtime_transport)
        self._wiring = SlamRuntimeNodeWiring(self)
        self._workflow_executor = WorkflowRuntimeExecutor(
            response_factory=self._service_api.response,
            get_runtime_snapshot=self._runtime_context.workflow_runtime_snapshot,
            restart_localization=self._runtime_adapter.restart_localization,
            sync_task_ready_state=self._runtime_state.sync_task_ready_state,
        )
        self._job_runner = CartographerSlamJobRunner(self)
        self._app_operate_contract, self._app_submit_job_contract, self._app_get_job_contract = (
            self._service_api.build_app_contract_reports()
        )
        publish_contract_param(rospy, self.app_service_contract_param_ns, self._app_operate_contract, enabled=True)
        publish_contract_param(rospy, self.app_submit_job_contract_param_ns, self._app_submit_job_contract, enabled=True)
        publish_contract_param(rospy, self.app_get_job_contract_param_ns, self._app_get_job_contract, enabled=True)

        self._tf_buffer = tf2_ros.Buffer(cache_time=rospy.Duration(30.0))
        self._tf_listener = tf2_ros.TransformListener(self._tf_buffer)
        self._wiring.wire()


def main():
    rospy.init_node("slam_runtime_manager", anonymous=False)
    SlamRuntimeManagerNode()
    rospy.spin()


if __name__ == "__main__":
    main()

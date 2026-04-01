#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time

import rospy
from std_msgs.msg import String

from coverage_planner.ops_store.store import OperationsStore
from coverage_planner.plan_store.store import PlanStore
from my_msg_srv.srv import ExeTask, ExeTaskRequest, ExeTaskResponse


class ExeTaskServiceNode:
    def __init__(self):
        self.service_name = str(rospy.get_param("~service_name", "/exe_task_server")).strip() or "/exe_task_server"
        self.cmd_topic = str(rospy.get_param("~cmd_topic", "/coverage_task_manager/cmd")).strip() or "/coverage_task_manager/cmd"
        self.ops_db_path = str(rospy.get_param("~ops_db_path", "/data/coverage/operations.db")).strip() or "/data/coverage/operations.db"
        self.plan_db_path = str(rospy.get_param("~plan_db_path", "/data/coverage/planning.db")).strip() or "/data/coverage/planning.db"
        self.robot_id = str(rospy.get_param("~robot_id", "local_robot")).strip() or "local_robot"
        self.cmd_wait_s = max(0.0, float(rospy.get_param("~cmd_wait_s", 1.0)))

        self._cmd_pub = rospy.Publisher(self.cmd_topic, String, queue_size=10)
        self._ops = OperationsStore(self.ops_db_path)
        self._plans = PlanStore(self.plan_db_path)
        self._srv = rospy.Service(self.service_name, ExeTask, self._handle)
        rospy.loginfo(
            "[exe_task_service] ready service=%s cmd_topic=%s ops_db=%s plan_db=%s robot_id=%s",
            self.service_name,
            self.cmd_topic,
            self.ops_db_path,
            self.plan_db_path,
            self.robot_id,
        )

    def _resp(self, success: bool, message: str) -> ExeTaskResponse:
        return ExeTaskResponse(success=bool(success), message=str(message or ""))

    def _publish_cmd(self, cmd: str) -> ExeTaskResponse:
        payload = str(cmd or "").strip()
        if not payload:
            return self._resp(False, "empty command")
        if not self._wait_for_cmd_consumer():
            return self._resp(False, "task manager command channel is not ready")
        self._cmd_pub.publish(String(data=payload))
        return self._resp(True, "accepted: %s" % payload)

    def _wait_for_cmd_consumer(self) -> bool:
        deadline = time.time() + float(self.cmd_wait_s or 0.0)
        while not rospy.is_shutdown():
            if self._cmd_pub.get_num_connections() > 0:
                return True
            if time.time() >= deadline:
                return False
            rospy.sleep(0.05)
        return False

    def _runtime_state(self):
        try:
            return self._ops.get_robot_runtime_state(self.robot_id)
        except Exception as e:
            rospy.logwarn("[exe_task_service] get runtime state failed: %s", str(e))
            return None

    def _runtime_summary(self, state) -> str:
        if state is None:
            return "runtime state unavailable"
        return "mission=%s phase=%s public=%s run=%s job=%s" % (
            str(state.mission_state or "IDLE"),
            str(state.phase or "IDLE"),
            str(state.public_state or "IDLE"),
            str(state.active_run_id or "-"),
            str(state.active_job_id or "-"),
        )

    def _ensure_command_allowed(self, command: int):
        state = self._runtime_state()
        if command == int(ExeTaskRequest.START):
            if state is None:
                return True, "", state
            mission = str(state.mission_state or "IDLE").upper()
            phase = str(state.phase or "IDLE").upper()
            if (mission not in ("IDLE", "ESTOP")) or (phase != "IDLE") or str(state.active_run_id or "").strip():
                return False, "task manager busy: %s" % self._runtime_summary(state), state
            return True, "", state
        if state is None:
            return False, "task manager state unavailable", None

        mission = str(state.mission_state or "IDLE").upper()
        phase = str(state.phase or "IDLE").upper()
        if command == int(ExeTaskRequest.PAUSE):
            if mission != "RUNNING":
                return False, "pause requires running mission: %s" % self._runtime_summary(state), state
            return True, "", state
        if command == int(ExeTaskRequest.CONTINUE):
            if mission != "PAUSED":
                return False, "continue requires paused mission: %s" % self._runtime_summary(state), state
            return True, "", state
        if command == int(ExeTaskRequest.STOP):
            if mission not in ("RUNNING", "PAUSED"):
                return False, "stop requires running or paused mission: %s" % self._runtime_summary(state), state
            return True, "", state
        if command == int(ExeTaskRequest.RETURN):
            if phase != "IDLE":
                return False, "return requires idle phase: %s" % self._runtime_summary(state), state
            if mission not in ("IDLE", "PAUSED"):
                return False, "return requires paused or idle mission: %s" % self._runtime_summary(state), state
            return True, "", state
        return False, "unsupported command=%s" % int(command), state

    def _handle_start(self, req) -> ExeTaskResponse:
        ok, msg, _state = self._ensure_command_allowed(int(ExeTaskRequest.START))
        if not ok:
            return self._resp(False, msg)
        task_id = int(req.task_id or 0)
        if task_id <= 0:
            return self._resp(False, "task_id is required for START")
        job = self._ops.get_job(str(task_id))
        if job is None:
            return self._resp(False, "task not found")
        if not bool(job.enabled):
            return self._resp(False, "task is disabled")
        zone_id = str(job.zone_id or "").strip()
        if not zone_id:
            return self._resp(False, "task zone_id is empty")
        zone = self._plans.get_zone_meta(zone_id, map_name=str(job.map_name or "").strip())
        if not zone:
            return self._resp(False, "task zone not found on task map")
        if not bool(zone.get("enabled", True)):
            return self._resp(False, "task zone is disabled")
        plan_profile_name = str(job.plan_profile_name or "").strip() or "cover_standard"
        plan_id = self._plans.get_active_plan_id(
            zone_id,
            plan_profile_name,
            map_name=str(job.map_name or "").strip(),
        )
        if not plan_id:
            return self._resp(False, "active plan not found for task zone/profile")
        try:
            plan_meta = self._plans.load_plan_meta(plan_id)
        except Exception:
            return self._resp(False, "active plan metadata is unavailable")
        if str(plan_meta.get("map_name") or "").strip() != str(job.map_name or "").strip():
            return self._resp(False, "active plan map does not match task map")
        if str(plan_meta.get("zone_id") or "").strip() != zone_id:
            return self._resp(False, "active plan zone does not match task zone")
        if str(plan_meta.get("plan_profile_name") or plan_meta.get("profile_name") or "").strip() not in ("", plan_profile_name):
            return self._resp(False, "active plan profile does not match task profile")
        active_map = self._plans.get_active_map(robot_id=self.robot_id)
        if active_map is None:
            return self._resp(False, "current map is not selected")
        active_map_name = str(active_map.get("map_name") or "").strip()
        if active_map_name and active_map_name != str(job.map_name or "").strip():
            return self._resp(
                False,
                "current map does not match task map: current=%s task=%s" % (
                    active_map_name,
                    str(job.map_name or "").strip(),
                ),
            )
        return self._publish_cmd("start_task %d" % task_id)

    def _handle(self, req):
        cmd = int(req.command)

        if cmd == int(ExeTaskRequest.START):
            return self._handle_start(req)
        ok, msg, _state = self._ensure_command_allowed(cmd)
        if not ok:
            return self._resp(False, msg)
        if cmd == int(ExeTaskRequest.PAUSE):
            return self._publish_cmd("pause")
        if cmd == int(ExeTaskRequest.CONTINUE):
            return self._publish_cmd("resume")
        if cmd == int(ExeTaskRequest.STOP):
            return self._publish_cmd("stop")
        if cmd == int(ExeTaskRequest.RETURN):
            return self._publish_cmd("dock")
        return self._resp(False, "unsupported command=%s" % cmd)


def main():
    rospy.init_node("exe_task_service", anonymous=False)
    ExeTaskServiceNode()
    rospy.spin()


if __name__ == "__main__":
    main()

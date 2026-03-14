#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import rospy

from my_msg_srv.msg import CleanTask
from my_msg_srv.srv import OperateTask, OperateTaskResponse

from coverage_planner.ops_store.store import JobRecord, OperationsStore
from coverage_planner.plan_store.store import PlanStore


class TaskApiServiceNode:
    def __init__(self):
        self.plan_db_path = rospy.get_param("~plan_db_path", "/data/coverage/planning.db")
        self.ops_db_path = rospy.get_param("~ops_db_path", "/data/coverage/operations.db")
        self.robot_id = str(rospy.get_param("~robot_id", "local_robot"))
        self.service_name = str(rospy.get_param("~service_name", "/database_server/clean_task_service"))
        self.ops = OperationsStore(self.ops_db_path)
        self.plan_store = PlanStore(self.plan_db_path)
        self.srv = rospy.Service(self.service_name, OperateTask, self._handle)
        rospy.loginfo("[task_api_service] ready service=%s plan_db=%s ops_db=%s", self.service_name, self.plan_db_path, self.ops_db_path)

    def _empty_resp(self, *, success: bool, message: str) -> OperateTaskResponse:
        return OperateTaskResponse(success=bool(success), message=str(message or ""), task=CleanTask(), tasks=[])

    def _alloc_task_id(self) -> int:
        mx = 0
        for job in self.ops.list_jobs():
            try:
                mx = max(mx, int(str(job.job_id or "0"), 10))
            except Exception:
                continue
        return mx + 1

    def _resolve_map_snapshot(self, req, task: CleanTask):
        map_name = str(task.map_name or req.map_name or "").strip()
        map_version = str(task.map_version or req.map_version or "").strip()
        if not map_name:
            raise ValueError("map_name is required")
        asset = self.plan_store.resolve_map_asset_version(
            map_name=map_name,
            map_version=map_version,
            robot_id=self.robot_id,
        )
        if not asset:
            raise ValueError("map asset not found")
        if not bool(asset.get("enabled", True)):
            raise ValueError("map asset is disabled")
        return str(asset.get("map_name") or ""), str(asset.get("map_version") or "")

    def _zone_id_from_task(self, task: CleanTask) -> str:
        zone_id = str(task.zone_id or "").strip()
        if zone_id:
            return zone_id
        if task.cells:
            try:
                return str(int(task.cells[0].path_id))
            except Exception:
                pass
        return ""

    def _status_for_job(self, job_id: str) -> int:
        run = self.ops.get_latest_run_for_job(str(job_id or "").strip())
        if run is None:
            return 0
        state = str(run.state or "").upper()
        if state in ("RUNNING", "PAUSED", "SUSPENDED"):
            return 1
        return 0

    def _job_to_task(self, job: JobRecord) -> CleanTask:
        msg = CleanTask()
        try:
            msg.id = int(str(job.job_id or "0"), 10)
        except Exception:
            msg.id = 0
        msg.name = str(job.job_name or "")
        msg.status = int(self._status_for_job(job.job_id))
        msg.num = int(job.default_loops or 1)
        msg.is_loop = 1 if int(job.default_loops or 1) > 1 else 0
        msg.zone_id = str(job.zone_id or "")
        msg.plan_profile_name = str(job.plan_profile_name or "")
        msg.sys_profile_name = str(job.sys_profile_name or "")
        msg.clean_mode = str(job.default_clean_mode or "")
        msg.map_name = str(job.map_name or "")
        msg.map_version = str(job.map_version or "")
        msg.enabled = bool(job.enabled)
        return msg

    def _upsert_task(self, req, *, allow_new_id: bool) -> CleanTask:
        task = req.task
        task_id = int(task.id or 0)
        if task_id <= 0:
            if not allow_new_id:
                raise ValueError("task.id is required for modify")
            task_id = self._alloc_task_id()
        zone_id = self._zone_id_from_task(task)
        if not zone_id:
            raise ValueError("zone_id is required")
        map_name, map_version = self._resolve_map_snapshot(req, task)
        self.ops.upsert_job(
            job_id=str(task_id),
            job_name=str(task.name or ("task_%d" % task_id)),
            map_name=map_name,
            map_version=map_version,
            zone_id=zone_id,
            plan_profile_name=str(task.plan_profile_name or "cover_standard"),
            sys_profile_name=str(task.sys_profile_name or "standard"),
            default_clean_mode=str(task.clean_mode or "scrub"),
            default_loops=max(1, int(task.num or 1)),
            enabled=True,
            priority=0,
        )
        job = self.ops.get_job(str(task_id))
        if job is None:
            raise RuntimeError("failed to persist task")
        return self._job_to_task(job)

    def _handle(self, req):
        op = int(req.operation)

        if op == int(req.getAll):
            items = []
            for job in self.ops.list_jobs():
                if req.map_name and str(job.map_name or "") != str(req.map_name or "").strip():
                    continue
                if req.map_version and str(job.map_version or "") != str(req.map_version or "").strip():
                    continue
                items.append(self._job_to_task(job))
            return OperateTaskResponse(success=True, message="ok", task=CleanTask(), tasks=items)

        if op == int(req.get):
            job = self.ops.get_job(str(req.id))
            if job is None:
                return self._empty_resp(success=False, message="task not found")
            return OperateTaskResponse(success=True, message="ok", task=self._job_to_task(job), tasks=[])

        if op == int(req.add):
            try:
                task = self._upsert_task(req, allow_new_id=True)
                return OperateTaskResponse(success=True, message="created", task=task, tasks=[])
            except Exception as e:
                return self._empty_resp(success=False, message=str(e))

        if op == int(req.modify):
            try:
                task = self._upsert_task(req, allow_new_id=False)
                return OperateTaskResponse(success=True, message="updated", task=task, tasks=[])
            except Exception as e:
                return self._empty_resp(success=False, message=str(e))

        if op == int(req.Delete):
            job = self.ops.get_job(str(req.id))
            if job is None:
                return self._empty_resp(success=False, message="task not found")
            self.ops.delete_job(str(req.id))
            return self._empty_resp(success=True, message="deleted")

        return self._empty_resp(success=False, message="unsupported operation=%s" % op)


def main():
    rospy.init_node("task_api_service", anonymous=False)
    TaskApiServiceNode()
    rospy.spin()


if __name__ == "__main__":
    main()

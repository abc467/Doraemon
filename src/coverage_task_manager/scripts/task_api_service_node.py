#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import rospy

from my_msg_srv.msg import CleanTask
from my_msg_srv.srv import OperateTask, OperateTaskRequest, OperateTaskResponse

from coverage_planner.ops_store.store import JobRecord, OperationsStore
from coverage_planner.plan_store.store import PlanStore
from coverage_planner.ros_contract import build_contract_report, validate_ros_contract


class TaskApiServiceNode:
    def __init__(self):
        self.plan_db_path = rospy.get_param("~plan_db_path", "/data/coverage/planning.db")
        self.ops_db_path = rospy.get_param("~ops_db_path", "/data/coverage/operations.db")
        self.robot_id = str(rospy.get_param("~robot_id", "local_robot"))
        self.service_name = str(rospy.get_param("~service_name", "/database_server/clean_task_service"))
        self.contract_param_ns = str(
            rospy.get_param("~contract_param_ns", "/database_server/contracts/clean_task_service")
        ).strip()
        self.default_return_to_dock_on_finish = bool(
            rospy.get_param("~default_return_to_dock_on_finish", False)
        )
        self._contract_report = self._prepare_contract_report()
        self.ops = OperationsStore(self.ops_db_path)
        self.plan_store = PlanStore(self.plan_db_path)
        self.srv = rospy.Service(self.service_name, OperateTask, self._handle)
        if self.contract_param_ns:
            rospy.set_param(self.contract_param_ns, self._contract_report)
        rospy.loginfo(
            "[task_api_service] ready service=%s md5=%s task_md5=%s plan_db=%s ops_db=%s contract_param=%s",
            self.service_name,
            self._contract_report["service"]["md5"],
            self._contract_report["dependencies"]["task"]["md5"],
            self.plan_db_path,
            self.ops_db_path,
            self.contract_param_ns or "-",
        )

    def _prepare_contract_report(self):
        validate_ros_contract(
            "CleanTask",
            CleanTask,
            required_fields=[
                "task_id",
                "name",
                "loops",
                "zone_id",
                "plan_profile_name",
                "sys_profile_name",
                "clean_mode",
                "map_name",
                "return_to_dock_on_finish",
                "repeat_after_full_charge",
                "enabled",
            ],
        )
        validate_ros_contract(
            "OperateTaskRequest",
            OperateTaskRequest,
            required_fields=[
                "operation",
                "task_id",
                "task",
                "map_name",
                "enabled_state",
                "return_to_dock_state",
                "repeat_after_full_charge_state",
            ],
            required_constants=[
                "ENABLE_KEEP",
                "ENABLE_DISABLE",
                "ENABLE_ENABLE",
                "RETURN_TO_DOCK_KEEP",
                "RETURN_TO_DOCK_DISABLE",
                "RETURN_TO_DOCK_ENABLE",
                "REPEAT_AFTER_FULL_CHARGE_KEEP",
                "REPEAT_AFTER_FULL_CHARGE_DISABLE",
                "REPEAT_AFTER_FULL_CHARGE_ENABLE",
                "get",
                "add",
                "modify",
                "Delete",
                "getAll",
            ],
        )
        return build_contract_report(
            service_name=self.service_name,
            contract_name="clean_task_service",
            service_cls=OperateTask,
            request_cls=OperateTaskRequest,
            response_cls=OperateTaskResponse,
            dependencies={"task": CleanTask},
            features=[
                "CleanTask.repeat_after_full_charge",
                "OperateTask.repeat_after_full_charge_state",
                "return_to_dock_on_finish_forced_when_repeat_enabled",
            ],
        )

    def _empty_resp(self, *, success: bool, message: str) -> OperateTaskResponse:
        return OperateTaskResponse(success=bool(success), message=str(message or ""), task=CleanTask(), tasks=[])

    def _req_task_id(self, req) -> int:
        try:
            task_id = int(getattr(req, "task_id", 0) or 0)
            if task_id > 0:
                return task_id
            return int(getattr(req.task, "task_id", 0) or 0)
        except Exception:
            return 0

    def _enabled_from_request(self, req, *, current_enabled: bool, default_enabled: bool) -> bool:
        enabled_state = int(getattr(req, "enabled_state", int(req.ENABLE_KEEP)))
        if enabled_state == int(req.ENABLE_ENABLE):
            return True
        if enabled_state == int(req.ENABLE_DISABLE):
            return False
        return bool(default_enabled if current_enabled is None else current_enabled)

    def _return_to_dock_from_request(self, req, *, current_value):
        return_state = int(
            getattr(req, "return_to_dock_state", int(req.RETURN_TO_DOCK_KEEP))
        )
        if return_state == int(req.RETURN_TO_DOCK_ENABLE):
            return True
        if return_state == int(req.RETURN_TO_DOCK_DISABLE):
            return False
        if current_value is not None:
            return bool(current_value)
        task_flag = bool(getattr(req.task, "return_to_dock_on_finish", False))
        if task_flag:
            return True
        return bool(self.default_return_to_dock_on_finish)

    def _repeat_after_full_charge_from_request(self, req, *, current_value):
        repeat_state = int(
            getattr(
                req,
                "repeat_after_full_charge_state",
                int(req.REPEAT_AFTER_FULL_CHARGE_KEEP),
            )
        )
        if repeat_state == int(req.REPEAT_AFTER_FULL_CHARGE_ENABLE):
            return True
        if repeat_state == int(req.REPEAT_AFTER_FULL_CHARGE_DISABLE):
            return False
        if current_value is not None:
            return bool(current_value)
        return bool(getattr(req.task, "repeat_after_full_charge", False))

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
        if map_name.endswith(".pbstream"):
            map_name = map_name[:-len(".pbstream")]
        if not map_name:
            raise ValueError("map_name is required")
        asset = self.plan_store.resolve_map_asset(
            map_name=map_name,
            robot_id=self.robot_id,
        )
        if not asset:
            raise ValueError("map asset not found")
        if not bool(asset.get("enabled", True)):
            raise ValueError("map asset is disabled")
        return str(asset.get("map_name") or "")

    def _ensure_zone_plan_ready(self, *, map_name: str, zone_id: str, plan_profile_name: str) -> None:
        zone_id = str(zone_id or "").strip()
        if not zone_id:
            raise ValueError("zone_id is required")

        zone = self.plan_store.get_zone_meta(zone_id, map_name=map_name)
        if not zone:
            raise ValueError("zone not found on selected map")
        if not bool(zone.get("enabled", True)):
            raise ValueError("zone is disabled")

        profile = str(plan_profile_name or "").strip() or "cover_standard"
        plan_id = self.plan_store.get_active_plan_id(zone_id, profile, map_name=map_name)
        if not plan_id:
            raise ValueError("active plan not found for zone/profile")
        try:
            plan_meta = self.plan_store.load_plan_meta(plan_id)
        except Exception:
            raise ValueError("active plan metadata is unavailable")

        plan_map_name = str(plan_meta.get("map_name") or "").strip()
        plan_zone_id = str(plan_meta.get("zone_id") or "").strip()
        plan_profile = str(plan_meta.get("plan_profile_name") or plan_meta.get("profile_name") or "").strip()
        if plan_map_name != str(map_name or "").strip():
            raise ValueError("active plan map does not match selected map")
        if plan_zone_id != zone_id:
            raise ValueError("active plan zone does not match task zone")
        if plan_profile and plan_profile != profile:
            raise ValueError("active plan profile does not match task profile")

    def _ensure_task_not_active(self, task_id: int) -> None:
        runtime = self.ops.get_robot_runtime_state(self.robot_id)
        if runtime is None:
            return
        active_job_id = str(runtime.active_job_id or "").strip()
        if active_job_id != str(task_id):
            return
        mission = str(runtime.mission_state or "IDLE").upper()
        phase = str(runtime.phase or "IDLE").upper()
        raise ValueError(
            "task is active and cannot be deleted: mission=%s phase=%s run=%s"
            % (mission, phase, str(runtime.active_run_id or "").strip() or "-")
        )

    def _zone_id_from_task(self, task: CleanTask) -> str:
        return str(task.zone_id or "").strip()

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
            msg.task_id = int(str(job.job_id or "0"), 10)
        except Exception:
            msg.task_id = 0
        msg.name = str(job.job_name or "")
        msg.status = int(self._status_for_job(job.job_id))
        msg.loops = int(job.default_loops or 1)
        msg.zone_id = str(job.zone_id or "")
        msg.plan_profile_name = str(job.plan_profile_name or "")
        msg.sys_profile_name = str(job.sys_profile_name or "")
        msg.clean_mode = str(job.default_clean_mode or "")
        msg.map_name = str(job.map_name or "")
        msg.return_to_dock_on_finish = bool(job.return_to_dock_on_finish)
        msg.repeat_after_full_charge = bool(job.repeat_after_full_charge)
        msg.enabled = bool(job.enabled)
        return msg

    def _upsert_task(self, req, *, allow_new_id: bool) -> CleanTask:
        task = req.task
        task_id = int(task.task_id or 0)
        if task_id <= 0:
            if not allow_new_id:
                raise ValueError("task.task_id is required for modify")
            task_id = self._alloc_task_id()
        existing_job = self.ops.get_job(str(task_id))
        if (not allow_new_id) and existing_job is None:
            raise ValueError("task not found")

        zone_id = self._zone_id_from_task(task) or str((existing_job.zone_id if existing_job is not None else "") or "").strip()
        if not zone_id:
            raise ValueError("zone_id is required")

        resolved_task = CleanTask()
        resolved_task.task_id = int(task_id)
        resolved_task.name = str(task.name or (existing_job.job_name if existing_job is not None else "") or "").strip()
        resolved_task.loops = int(task.loops or (existing_job.default_loops if existing_job is not None else 1) or 1)
        resolved_task.zone_id = str(zone_id)
        resolved_task.plan_profile_name = str(
            task.plan_profile_name or (existing_job.plan_profile_name if existing_job is not None else "") or "cover_standard"
        ).strip()
        resolved_task.sys_profile_name = str(
            task.sys_profile_name or (existing_job.sys_profile_name if existing_job is not None else "") or "standard"
        ).strip()
        resolved_task.clean_mode = str(
            task.clean_mode or (existing_job.default_clean_mode if existing_job is not None else "") or "scrub"
        ).strip()
        resolved_task.map_name = str(
            task.map_name or req.map_name or (existing_job.map_name if existing_job is not None else "") or ""
        ).strip()
        resolved_task.return_to_dock_on_finish = self._return_to_dock_from_request(
            req,
            current_value=(existing_job.return_to_dock_on_finish if existing_job is not None else None),
        )
        resolved_task.repeat_after_full_charge = self._repeat_after_full_charge_from_request(
            req,
            current_value=(existing_job.repeat_after_full_charge if existing_job is not None else None),
        )
        resolved_task.enabled = self._enabled_from_request(
            req,
            current_enabled=(bool(existing_job.enabled) if existing_job is not None else None),
            default_enabled=True,
        )
        if resolved_task.repeat_after_full_charge:
            resolved_task.return_to_dock_on_finish = True

        if not resolved_task.name:
            resolved_task.name = "task_%d" % task_id
        resolved_task.loops = max(1, int(resolved_task.loops or 1))

        map_name = self._resolve_map_snapshot(req, resolved_task)
        self._ensure_zone_plan_ready(
            map_name=map_name,
            zone_id=str(resolved_task.zone_id or ""),
            plan_profile_name=str(resolved_task.plan_profile_name or ""),
        )
        self.ops.upsert_job(
            job_id=str(task_id),
            job_name=str(resolved_task.name or ("task_%d" % task_id)),
            map_name=map_name,
            zone_id=str(resolved_task.zone_id or ""),
            plan_profile_name=str(resolved_task.plan_profile_name or "cover_standard"),
            sys_profile_name=str(resolved_task.sys_profile_name or "standard"),
            default_clean_mode=str(resolved_task.clean_mode or "scrub"),
            return_to_dock_on_finish=bool(resolved_task.return_to_dock_on_finish),
            repeat_after_full_charge=bool(resolved_task.repeat_after_full_charge),
            default_loops=max(1, int(resolved_task.loops or 1)),
            enabled=bool(resolved_task.enabled),
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
            req_map_name = str(req.map_name or "").strip()
            if req_map_name.endswith(".pbstream"):
                req_map_name = req_map_name[:-len(".pbstream")]
            for job in self.ops.list_jobs():
                if req_map_name and str(job.map_name or "") != req_map_name:
                    continue
                items.append(self._job_to_task(job))
            return OperateTaskResponse(success=True, message="ok", task=CleanTask(), tasks=items)

        if op == int(req.get):
            job = self.ops.get_job(str(self._req_task_id(req)))
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
            task_id = self._req_task_id(req)
            job = self.ops.get_job(str(task_id))
            if job is None:
                return self._empty_resp(success=False, message="task not found")
            try:
                self._ensure_task_not_active(task_id)
                self.ops.delete_job(str(task_id))
                return self._empty_resp(success=True, message="deleted")
            except Exception as e:
                return self._empty_resp(success=False, message=str(e))

        return self._empty_resp(success=False, message="unsupported operation=%s" % op)


def main():
    rospy.init_node("task_api_service", anonymous=False)
    TaskApiServiceNode()
    rospy.spin()


if __name__ == "__main__":
    main()

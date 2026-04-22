#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import rospy

from cleanrobot_app_msgs.msg import CleanTask as AppCleanTask
from cleanrobot_app_msgs.srv import (
    OperateTask as AppOperateTask,
    OperateTaskRequest as AppOperateTaskRequest,
    OperateTaskResponse as AppOperateTaskResponse,
)

from coverage_planner.map_asset_status import map_asset_verification_error
from coverage_planner.ops_store.store import JobRecord, OperationsStore
from coverage_planner.plan_store.store import PlanStore
from coverage_planner.ros_contract import build_contract_report, validate_ros_contract
from coverage_planner.service_mode import publish_contract_param

# Preserve module-level symbols for existing tests/tools.
CleanTask = AppCleanTask
OperateTask = AppOperateTask
OperateTaskRequest = AppOperateTaskRequest
OperateTaskResponse = AppOperateTaskResponse

class TaskApiServiceNode:
    def __init__(self):
        self.plan_db_path = rospy.get_param("~plan_db_path", "/data/coverage/planning.db")
        self.ops_db_path = rospy.get_param("~ops_db_path", "/data/coverage/operations.db")
        self.robot_id = str(rospy.get_param("~robot_id", "local_robot"))
        self.app_service_name = str(
            rospy.get_param("~app_service_name", "/database_server/app/clean_task_service")
        ).strip() or "/database_server/app/clean_task_service"
        self.app_contract_param_ns = str(
            rospy.get_param(
                "~app_contract_param_ns",
                "/database_server/contracts/app/clean_task_service",
            )
        ).strip() or "/database_server/contracts/app/clean_task_service"
        self.default_return_to_dock_on_finish = bool(
            rospy.get_param("~default_return_to_dock_on_finish", False)
        )
        self._app_contract_report = self._prepare_contract_report(
            task_cls=AppCleanTask,
            service_cls=AppOperateTask,
            request_cls=AppOperateTaskRequest,
            response_cls=AppOperateTaskResponse,
            service_name=self.app_service_name,
            contract_name="clean_task_service_app",
            validate_label_prefix="App",
            features=[
                "CleanTask.repeat_after_full_charge",
                "OperateTask.repeat_after_full_charge_state",
                "return_to_dock_on_finish_forced_when_repeat_enabled",
                "cleanrobot_app_msgs_parallel",
            ],
        )
        self.ops = OperationsStore(self.ops_db_path)
        self.plan_store = PlanStore(self.plan_db_path)
        self.srv = None
        self.app_srv = rospy.Service(self.app_service_name, AppOperateTask, self._handle_app)
        publish_contract_param(rospy, self.app_contract_param_ns, self._app_contract_report, enabled=True)
        rospy.loginfo(
            "[task_api_service] ready app_service=%s app_md5=%s plan_db=%s ops_db=%s app_contract=%s",
            self.app_service_name,
            self._app_contract_report["service"]["md5"],
            self.plan_db_path,
            self.ops_db_path,
            self.app_contract_param_ns or "-",
        )

    def _prepare_contract_report(
        self,
        *,
        task_cls,
        service_cls,
        request_cls,
        response_cls,
        service_name: str,
        contract_name: str,
        validate_label_prefix: str,
        features,
    ):
        validate_ros_contract(
            "%sCleanTask" % str(validate_label_prefix or ""),
            task_cls,
            required_fields=[
                "task_id",
                "name",
                "loops",
                "zone_id",
                "plan_profile_name",
                "sys_profile_name",
                "clean_mode",
                "map_name",
                "map_revision_id",
                "return_to_dock_on_finish",
                "repeat_after_full_charge",
                "enabled",
            ],
        )
        validate_ros_contract(
            "%sOperateTaskRequest" % str(validate_label_prefix or ""),
            request_cls,
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
            service_name=service_name,
            contract_name=contract_name,
            service_cls=service_cls,
            request_cls=request_cls,
            response_cls=response_cls,
            dependencies={"task": task_cls},
            features=list(features or []),
        )

    def _empty_resp(self, *, response_cls=AppOperateTaskResponse, task_cls=AppCleanTask, success: bool, message: str):
        return response_cls(success=bool(success), message=str(message or ""), task=task_cls(), tasks=[])

    def _req_task_id(self, req) -> int:
        try:
            task_id = int(getattr(req, "task_id", 0) or 0)
            if task_id > 0:
                return task_id
            return int(getattr(req.task, "task_id", 0) or 0)
        except Exception:
            return 0

    def _req_task_map_filters(self, req):
        task = getattr(req, "task", None)
        map_name = str(getattr(req, "map_name", "") or getattr(task, "map_name", "") or "").strip()
        if map_name.endswith(".pbstream"):
            map_name = map_name[:-len(".pbstream")]
        map_revision_id = str(getattr(task, "map_revision_id", "") or "").strip()
        return map_name, map_revision_id

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
        task = getattr(req, "task", None)
        if task is not None and hasattr(task, "return_to_dock_on_finish"):
            # For new tasks, honor the explicit bool carried in the task payload.
            return bool(getattr(task, "return_to_dock_on_finish", False))
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

    def _resolve_map_snapshot(self, req, task, *, existing_job=None):
        explicit_map_name = str(task.map_name or req.map_name or "").strip()
        explicit_map_revision_id = str(getattr(task, "map_revision_id", "") or "").strip()
        map_name = str(explicit_map_name or (existing_job.map_name if existing_job is not None else "") or "").strip()
        if map_name.endswith(".pbstream"):
            map_name = map_name[:-len(".pbstream")]
        existing_map_name = str((existing_job.map_name if existing_job is not None else "") or "").strip()
        existing_map_revision_id = str((existing_job.map_revision_id if existing_job is not None else "") or "").strip()
        requested_map_revision_id = explicit_map_revision_id
        if (not requested_map_revision_id) and existing_map_revision_id and existing_map_name and existing_map_name == map_name:
            requested_map_revision_id = existing_map_revision_id
        if (not map_name) and requested_map_revision_id:
            revision_asset = self.plan_store.resolve_map_asset(
                revision_id=requested_map_revision_id,
                robot_id=self.robot_id,
            )
            map_name = str((revision_asset or {}).get("map_name") or "").strip()
        if not map_name:
            raise ValueError("map_name is required")
        if requested_map_revision_id:
            asset = self.plan_store.resolve_map_asset(
                map_name=map_name,
                revision_id=requested_map_revision_id,
                robot_id=self.robot_id,
            )
        else:
            asset = self.plan_store.resolve_map_revision(
                map_name=map_name,
                robot_id=self.robot_id,
            )
        error = map_asset_verification_error(
            asset,
            label=("task bound map asset" if requested_map_revision_id else "map asset"),
        )
        if error:
            raise ValueError(error)
        asset_map_name = str(asset.get("map_name") or "").strip()
        if explicit_map_name and asset_map_name and asset_map_name != map_name:
            raise ValueError("map revision does not match selected map")
        return {
            "map_name": asset_map_name,
            "map_revision_id": str(asset.get("revision_id") or ""),
        }

    def _ensure_zone_plan_ready(self, *, map_name: str, map_revision_id: str = "", zone_id: str, plan_profile_name: str) -> None:
        zone_id = str(zone_id or "").strip()
        if not zone_id:
            raise ValueError("zone_id is required")

        zone = self.plan_store.get_zone_meta(zone_id, map_name=map_name, map_revision_id=map_revision_id)
        if not zone:
            raise ValueError("zone not found on selected map")
        if not bool(zone.get("enabled", True)):
            raise ValueError("zone is disabled")

        profile = str(plan_profile_name or "").strip() or "cover_standard"
        plan_id = self.plan_store.get_active_plan_id(
            zone_id,
            profile,
            map_name=map_name,
            map_revision_id=map_revision_id,
        )
        if not plan_id:
            raise ValueError("active plan not found for zone/profile")
        try:
            plan_meta = self.plan_store.load_plan_meta(plan_id)
        except Exception:
            raise ValueError("active plan metadata is unavailable")

        plan_map_revision_id = str(plan_meta.get("map_revision_id") or "").strip()
        plan_map_name = str(plan_meta.get("map_name") or "").strip()
        plan_zone_id = str(plan_meta.get("zone_id") or "").strip()
        plan_profile = str(plan_meta.get("plan_profile_name") or "").strip()
        if map_revision_id and plan_map_revision_id and plan_map_revision_id != str(map_revision_id or "").strip():
            raise ValueError("active plan revision does not match selected map revision")
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

    def _zone_id_from_task(self, task) -> str:
        return str(task.zone_id or "").strip()

    def _status_for_job(self, job_id: str) -> int:
        run = self.ops.get_latest_run_for_job(str(job_id or "").strip())
        if run is None:
            return 0
        state = str(run.state or "").upper()
        if state in ("RUNNING", "PAUSED", "SUSPENDED"):
            return 1
        return 0

    def _job_to_task(self, job: JobRecord, *, task_cls=AppCleanTask):
        msg = task_cls()
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
        msg.map_revision_id = str(getattr(job, "map_revision_id", "") or "")
        msg.return_to_dock_on_finish = bool(job.return_to_dock_on_finish)
        msg.repeat_after_full_charge = bool(job.repeat_after_full_charge)
        msg.enabled = bool(job.enabled)
        return msg

    def _upsert_task(self, req, *, allow_new_id: bool, task_cls=AppCleanTask):
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

        resolved_task = task_cls()
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
        resolved_task.map_revision_id = str(
            getattr(task, "map_revision_id", "")
            or (existing_job.map_revision_id if existing_job is not None else "")
            or ""
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

        map_snapshot = self._resolve_map_snapshot(req, resolved_task, existing_job=existing_job)
        map_name = str(map_snapshot.get("map_name") or "")
        map_revision_id = str(map_snapshot.get("map_revision_id") or "")
        resolved_task.map_revision_id = map_revision_id
        self._ensure_zone_plan_ready(
            map_name=map_name,
            map_revision_id=map_revision_id,
            zone_id=str(resolved_task.zone_id or ""),
            plan_profile_name=str(resolved_task.plan_profile_name or ""),
        )
        self.ops.upsert_job(
            job_id=str(task_id),
            job_name=str(resolved_task.name or ("task_%d" % task_id)),
            map_name=map_name,
            map_revision_id=map_revision_id,
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
        return self._job_to_task(job, task_cls=task_cls)

    def _handle(self, req, *, response_cls=AppOperateTaskResponse, task_cls=AppCleanTask):
        op = int(req.operation)

        if op == int(req.getAll):
            items = []
            req_map_name, req_map_revision_id = self._req_task_map_filters(req)
            for job in self.ops.list_jobs():
                if req_map_name and str(job.map_name or "") != req_map_name:
                    continue
                if req_map_revision_id and str(getattr(job, "map_revision_id", "") or "").strip() != req_map_revision_id:
                    continue
                items.append(self._job_to_task(job, task_cls=task_cls))
            return response_cls(success=True, message="ok", task=task_cls(), tasks=items)

        if op == int(req.get):
            job = self.ops.get_job(str(self._req_task_id(req)))
            if job is None:
                return self._empty_resp(response_cls=response_cls, task_cls=task_cls, success=False, message="task not found")
            return response_cls(success=True, message="ok", task=self._job_to_task(job, task_cls=task_cls), tasks=[])

        if op == int(req.add):
            try:
                task = self._upsert_task(req, allow_new_id=True, task_cls=task_cls)
                return response_cls(success=True, message="created", task=task, tasks=[])
            except Exception as e:
                return self._empty_resp(response_cls=response_cls, task_cls=task_cls, success=False, message=str(e))

        if op == int(req.modify):
            try:
                task = self._upsert_task(req, allow_new_id=False, task_cls=task_cls)
                return response_cls(success=True, message="updated", task=task, tasks=[])
            except Exception as e:
                return self._empty_resp(response_cls=response_cls, task_cls=task_cls, success=False, message=str(e))

        if op == int(req.Delete):
            task_id = self._req_task_id(req)
            job = self.ops.get_job(str(task_id))
            if job is None:
                return self._empty_resp(response_cls=response_cls, task_cls=task_cls, success=False, message="task not found")
            try:
                self._ensure_task_not_active(task_id)
                self.ops.delete_job(str(task_id))
                return self._empty_resp(response_cls=response_cls, task_cls=task_cls, success=True, message="deleted")
            except Exception as e:
                return self._empty_resp(response_cls=response_cls, task_cls=task_cls, success=False, message=str(e))

        return self._empty_resp(
            response_cls=response_cls,
            task_cls=task_cls,
            success=False,
            message="unsupported operation=%s" % op,
        )

    def _handle_app(self, req):
        return self._handle(req, response_cls=AppOperateTaskResponse, task_cls=AppCleanTask)


def main():
    rospy.init_node("task_api_service", anonymous=False)
    TaskApiServiceNode()
    rospy.spin()


if __name__ == "__main__":
    main()

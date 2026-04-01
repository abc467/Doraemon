#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import rospy

from my_msg_srv.msg import CleanSchedule
from my_msg_srv.srv import OperateSchedule, OperateScheduleRequest, OperateScheduleResponse

from coverage_planner.ops_store.store import OperationsStore, ScheduleRecord
from coverage_planner.plan_store.store import PlanStore
from coverage_planner.ros_contract import build_contract_report, validate_ros_contract
from coverage_task_manager.scheduler import Scheduler


class ScheduleApiServiceNode:
    def __init__(self):
        self.plan_db_path = rospy.get_param("~plan_db_path", "/data/coverage/planning.db")
        self.ops_db_path = rospy.get_param("~ops_db_path", "/data/coverage/operations.db")
        self.robot_id = str(rospy.get_param("~robot_id", "local_robot"))
        self.service_name = str(rospy.get_param("~service_name", "/database_server/clean_schedule_service"))
        self.contract_param_ns = str(
            rospy.get_param("~contract_param_ns", "/database_server/contracts/clean_schedule_service")
        ).strip()
        self._contract_report = self._prepare_contract_report()
        self.ops = OperationsStore(self.ops_db_path)
        self.plan_store = PlanStore(self.plan_db_path)
        self.srv = rospy.Service(self.service_name, OperateSchedule, self._handle)
        if self.contract_param_ns:
            rospy.set_param(self.contract_param_ns, self._contract_report)
        rospy.loginfo(
            "[schedule_api_service] ready service=%s md5=%s schedule_md5=%s plan_db=%s ops_db=%s contract_param=%s",
            self.service_name,
            self._contract_report["service"]["md5"],
            self._contract_report["dependencies"]["schedule"]["md5"],
            self.plan_db_path,
            self.ops_db_path,
            self.contract_param_ns or "-",
        )

    def _prepare_contract_report(self):
        validate_ros_contract(
            "CleanSchedule",
            CleanSchedule,
            required_fields=[
                "schedule_id",
                "task_id",
                "task_name",
                "enabled",
                "type",
                "dow",
                "time",
                "at",
                "timezone",
                "start_date",
                "end_date",
                "map_name",
                "zone_id",
                "loops",
                "plan_profile_name",
                "sys_profile_name",
                "clean_mode",
                "return_to_dock_on_finish",
                "repeat_after_full_charge",
                "last_fire_ts",
                "last_done_ts",
                "last_status",
            ],
        )
        validate_ros_contract(
            "OperateScheduleRequest",
            OperateScheduleRequest,
            required_fields=[
                "operation",
                "schedule_id",
                "task_id",
                "schedule",
                "enabled_state",
            ],
            required_constants=[
                "ENABLE_KEEP",
                "ENABLE_DISABLE",
                "ENABLE_ENABLE",
                "get",
                "add",
                "modify",
                "Delete",
                "getAll",
            ],
        )
        return build_contract_report(
            service_name=self.service_name,
            contract_name="clean_schedule_service",
            service_cls=OperateSchedule,
            request_cls=OperateScheduleRequest,
            response_cls=OperateScheduleResponse,
            dependencies={"schedule": CleanSchedule},
            features=[
                "CleanSchedule.repeat_after_full_charge",
                "schedule_read_only_inherits_task_repeat_after_full_charge",
            ],
        )

    def _empty_resp(self, *, success: bool, message: str) -> OperateScheduleResponse:
        return OperateScheduleResponse(success=bool(success), message=str(message or ""), schedule=CleanSchedule(), schedules=[])

    def _req_schedule_id(self, req) -> str:
        value = str(getattr(req, "schedule_id", "") or "").strip()
        if value:
            return value
        return str(getattr(req.schedule, "schedule_id", "") or "").strip()

    def _req_task_id(self, req) -> int:
        try:
            task_id = int(getattr(req, "task_id", 0) or 0)
            if task_id > 0:
                return task_id
            return int(getattr(req.schedule, "task_id", 0) or 0)
        except Exception:
            return 0

    def _enabled_from_request(self, req, *, current_enabled: bool, default_enabled: bool) -> bool:
        enabled_state = int(getattr(req, "enabled_state", int(req.ENABLE_KEEP)))
        if enabled_state == int(req.ENABLE_ENABLE):
            return True
        if enabled_state == int(req.ENABLE_DISABLE):
            return False
        return bool(default_enabled if current_enabled is None else current_enabled)

    def _ensure_zone_plan_ready(self, *, map_name: str, zone_id: str, plan_profile_name: str) -> None:
        zone_id = str(zone_id or "").strip()
        if not zone_id:
            raise ValueError("zone_id is required")
        map_name = str(map_name or "").strip()
        if not map_name:
            raise ValueError("map_name is required")

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
        if plan_map_name != map_name:
            raise ValueError("active plan map does not match selected map")
        if plan_zone_id != zone_id:
            raise ValueError("active plan zone does not match task zone")
        if plan_profile and plan_profile != profile:
            raise ValueError("active plan profile does not match task profile")

    def _schedule_to_msg(self, rec: ScheduleRecord) -> CleanSchedule:
        msg = CleanSchedule()
        msg.schedule_id = str(rec.schedule_id or "")
        try:
            msg.task_id = int(str(rec.job_id or "0"), 10)
        except Exception:
            msg.task_id = 0
        msg.task_name = str(rec.job_name or "")
        msg.enabled = bool(rec.enabled)
        msg.type = str(rec.schedule_type or "")
        msg.dow = [int(x) for x in (rec.dow or [])]
        msg.time = str(rec.time_local or "")
        msg.timezone = str(rec.timezone or "Asia/Shanghai")
        msg.start_date = str(rec.start_date or "")
        msg.end_date = str(rec.end_date or "")
        if msg.type == "once":
            at = ("%s %s" % (msg.start_date, msg.time)).strip()
            msg.at = at
        else:
            msg.at = ""
        msg.map_name = str(rec.map_name or "")
        msg.zone_id = str(rec.zone_id or "")
        msg.loops = int(rec.default_loops or 1)
        msg.plan_profile_name = str(rec.plan_profile_name or "")
        msg.sys_profile_name = str(rec.sys_profile_name or "")
        msg.clean_mode = str(rec.default_clean_mode or "")
        msg.return_to_dock_on_finish = bool(rec.return_to_dock_on_finish)
        msg.repeat_after_full_charge = bool(rec.repeat_after_full_charge)
        msg.last_fire_ts = float(rec.last_fire_ts or 0.0)
        msg.last_done_ts = float(rec.last_done_ts or 0.0)
        msg.last_status = str(rec.last_status or "")
        return msg

    def _validate_schedule_spec(
        self,
        *,
        schedule_id: str,
        enabled: bool,
        schedule_type: str,
        dow,
        time_local: str,
        at: str,
        timezone: str,
        start_date: str,
        end_date: str,
        job,
    ) -> None:
        task_spec = {
            "map_name": str(job.map_name or ""),
            "zone_id": str(job.zone_id or ""),
            "loops": int(job.default_loops or 1),
            "plan_profile_name": str(job.plan_profile_name or ""),
            "sys_profile_name": str(job.sys_profile_name or ""),
            "clean_mode": str(job.default_clean_mode or ""),
            "return_to_dock_on_finish": bool(job.return_to_dock_on_finish),
            "repeat_after_full_charge": bool(job.repeat_after_full_charge),
        }
        spec = {
            "id": str(schedule_id or "").strip(),
            "job_id": str(job.job_id or "").strip(),
            "enabled": bool(enabled),
            "type": str(schedule_type or "").strip().lower(),
            "timezone": str(timezone or "Asia/Shanghai").strip() or "Asia/Shanghai",
            "start_date": str(start_date or "").strip(),
            "end_date": str(end_date or "").strip(),
            "task": task_spec,
        }
        if spec["type"] == "weekly":
            spec["dow"] = [int(x) for x in (dow or [])]
            spec["time"] = str(time_local or "").strip()
        elif spec["type"] == "daily":
            spec["time"] = str(time_local or "").strip()
        elif spec["type"] == "once":
            spec["at"] = str(at or "").strip()
        Scheduler.from_param([spec], defaults={})

    def _upsert_schedule(self, req, *, allow_create: bool) -> CleanSchedule:
        schedule = req.schedule
        schedule_id = self._req_schedule_id(req)
        if not schedule_id:
            raise ValueError("schedule_id is required")
        existing = self.ops.get_schedule(schedule_id)
        if allow_create and existing is not None:
            raise ValueError("schedule already exists")
        if (not allow_create) and existing is None:
            raise ValueError("schedule not found")

        task_id = self._req_task_id(req)
        if task_id <= 0:
            if existing is None:
                raise ValueError("task_id is required")
            task_id = int(str(existing.job_id or "0"), 10)
        job = self.ops.get_job(str(task_id))
        if job is None:
            raise ValueError("task not found")

        self._ensure_zone_plan_ready(
            map_name=str(job.map_name or ""),
            zone_id=str(job.zone_id or ""),
            plan_profile_name=str(job.plan_profile_name or ""),
        )

        schedule_type = str(schedule.type or (existing.schedule_type if existing is not None else "") or "").strip().lower()
        enabled = self._enabled_from_request(
            req,
            current_enabled=(bool(existing.enabled) if existing is not None else None),
            default_enabled=True,
        )
        dow = list(schedule.dow or (existing.dow if existing is not None else []))
        time_local = str(schedule.time or (existing.time_local if existing is not None else "") or "").strip()
        timezone = str(schedule.timezone or (existing.timezone if existing is not None else "Asia/Shanghai") or "Asia/Shanghai").strip()
        start_date = str(schedule.start_date or (existing.start_date if existing is not None else "") or "").strip()
        end_date = str(schedule.end_date or (existing.end_date if existing is not None else "") or "").strip()
        at = str(schedule.at or "").strip()

        if schedule_type == "once":
            at_norm = at.replace("/", "-").strip()
            if " " in at_norm:
                start_date, time_local = at_norm.split(" ", 1)
            elif at_norm:
                start_date = at_norm
                if not time_local:
                    time_local = "00:00"

        should_reset_state = False
        if existing is not None:
            old_dow = [int(x) for x in (existing.dow or [])]
            new_dow = [int(x) for x in (dow or [])]
            timing_changed = any(
                [
                    str(existing.schedule_type or "").strip().lower() != str(schedule_type or "").strip().lower(),
                    old_dow != new_dow,
                    str(existing.time_local or "").strip() != str(time_local or "").strip(),
                    str(existing.timezone or "Asia/Shanghai").strip() != str(timezone or "Asia/Shanghai").strip(),
                    str(existing.start_date or "").strip() != str(start_date or "").strip(),
                    str(existing.end_date or "").strip() != str(end_date or "").strip(),
                ]
            )
            rearmed = (not bool(existing.enabled)) and bool(enabled)
            should_reset_state = bool(timing_changed or rearmed)

        self._validate_schedule_spec(
            schedule_id=schedule_id,
            enabled=enabled,
            schedule_type=schedule_type,
            dow=dow,
            time_local=time_local,
            at=("%s %s" % (start_date, time_local)).strip() if schedule_type == "once" else at,
            timezone=timezone,
            start_date=start_date,
            end_date=end_date,
            job=job,
        )

        dow_mask = ",".join(str(int(x)) for x in dow) if schedule_type == "weekly" else ""
        self.ops.upsert_job_schedule(
            schedule_id=schedule_id,
            job_id=str(job.job_id or ""),
            enabled=bool(enabled),
            schedule_type=str(schedule_type or "").strip().lower(),
            dow_mask=dow_mask,
            time_local=str(time_local or "").strip(),
            timezone=timezone,
            start_date=start_date,
            end_date=end_date,
        )
        if should_reset_state:
            self.ops.clear_schedule_state(schedule_id)
        saved = self.ops.get_schedule(schedule_id)
        if saved is None:
            raise RuntimeError("failed to persist schedule")
        return self._schedule_to_msg(saved)

    def _handle(self, req):
        op = int(req.operation)

        if op == int(req.getAll):
            task_id = self._req_task_id(req)
            items = [self._schedule_to_msg(rec) for rec in self.ops.list_schedules(include_disabled=True, job_id=(str(task_id) if task_id > 0 else ""))]
            return OperateScheduleResponse(success=True, message="ok", schedule=CleanSchedule(), schedules=items)

        if op == int(req.get):
            schedule_id = self._req_schedule_id(req)
            if not schedule_id:
                return self._empty_resp(success=False, message="schedule_id is required")
            rec = self.ops.get_schedule(schedule_id)
            if rec is None:
                return self._empty_resp(success=False, message="schedule not found")
            return OperateScheduleResponse(success=True, message="ok", schedule=self._schedule_to_msg(rec), schedules=[])

        if op == int(req.add):
            try:
                msg = self._upsert_schedule(req, allow_create=True)
                return OperateScheduleResponse(success=True, message="created", schedule=msg, schedules=[])
            except Exception as e:
                return self._empty_resp(success=False, message=str(e))

        if op == int(req.modify):
            try:
                msg = self._upsert_schedule(req, allow_create=False)
                return OperateScheduleResponse(success=True, message="updated", schedule=msg, schedules=[])
            except Exception as e:
                return self._empty_resp(success=False, message=str(e))

        if op == int(req.Delete):
            schedule_id = self._req_schedule_id(req)
            if not schedule_id:
                return self._empty_resp(success=False, message="schedule_id is required")
            rec = self.ops.get_schedule(schedule_id)
            if rec is None:
                return self._empty_resp(success=False, message="schedule not found")
            try:
                self.ops.set_schedule_enabled(schedule_id, False)
                self.ops.clear_schedule_state(schedule_id)
                saved = self.ops.get_schedule(schedule_id)
                return OperateScheduleResponse(success=True, message="disabled", schedule=self._schedule_to_msg(saved or rec), schedules=[])
            except Exception as e:
                return self._empty_resp(success=False, message=str(e))

        return self._empty_resp(success=False, message="unsupported operation")


def main():
    rospy.init_node("schedule_api_service", anonymous=False)
    ScheduleApiServiceNode()
    rospy.spin()


if __name__ == "__main__":
    main()

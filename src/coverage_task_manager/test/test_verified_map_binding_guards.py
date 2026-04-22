#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import importlib.util
import pathlib
import types
import unittest


def _load_module(rel_path: str, module_name: str):
    script_path = pathlib.Path(__file__).resolve().parents[1] / "scripts" / rel_path
    spec = importlib.util.spec_from_file_location(module_name, str(script_path))
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


TASK_API_MODULE = _load_module("task_api_service_node.py", "task_api_service_node_test_mod")
SCHEDULE_API_MODULE = _load_module("schedule_api_service_node.py", "schedule_api_service_node_test_mod")


class _FakePlanStore:
    def __init__(self, asset):
        self._asset = dict(asset)
        self.resolve_calls = []

    def resolve_map_asset(self, **kwargs):
        self.resolve_calls.append(dict(kwargs))
        return dict(self._asset)

    def resolve_map_revision(self, **kwargs):
        self.resolve_calls.append(dict(kwargs))
        return dict(self._asset)


class _FakeJob:
    map_name = "demo_map"
    map_revision_id = "rev_demo"
    zone_id = "zone_a"
    plan_profile_name = "cover_standard"


class _FakeOpsForTaskList:
    def __init__(self, jobs):
        self._jobs = list(jobs)

    def list_jobs(self):
        return list(self._jobs)


class _FakeOpsForScheduleList:
    def __init__(self, schedules):
        self._schedules = list(schedules)
        self.calls = []

    def list_schedules(self, include_disabled=True, job_id=""):
        self.calls.append({"include_disabled": bool(include_disabled), "job_id": str(job_id or "")})
        if str(job_id or "").strip():
            return [item for item in self._schedules if str(getattr(item, "job_id", "") or "") == str(job_id)]
        return list(self._schedules)


class _FakeOpsForTaskUpsert:
    def __init__(self):
        self.jobs = {}
        self.last_upsert = None

    def list_jobs(self):
        return list(self.jobs.values())

    def get_job(self, job_id):
        return self.jobs.get(str(job_id))

    def upsert_job(self, **kwargs):
        self.last_upsert = dict(kwargs)
        self.jobs[str(kwargs["job_id"])] = types.SimpleNamespace(
            job_id=str(kwargs["job_id"]),
            job_name=str(kwargs.get("job_name", "")),
            default_loops=int(kwargs.get("default_loops", 1) or 1),
            zone_id=str(kwargs.get("zone_id", "")),
            plan_profile_name=str(kwargs.get("plan_profile_name", "")),
            sys_profile_name=str(kwargs.get("sys_profile_name", "")),
            default_clean_mode=str(kwargs.get("default_clean_mode", "")),
            map_name=str(kwargs.get("map_name", "")),
            map_revision_id=str(kwargs.get("map_revision_id", "")),
            return_to_dock_on_finish=bool(kwargs.get("return_to_dock_on_finish", False)),
            repeat_after_full_charge=bool(kwargs.get("repeat_after_full_charge", False)),
            enabled=bool(kwargs.get("enabled", True)),
        )


class VerifiedMapBindingGuardsTest(unittest.TestCase):
    def test_task_api_rejects_unverified_map_asset(self):
        node = TASK_API_MODULE.TaskApiServiceNode.__new__(TASK_API_MODULE.TaskApiServiceNode)
        node.robot_id = "robot_a"
        node.plan_store = _FakePlanStore(
            {
                "map_name": "demo_map",
                "revision_id": "rev_demo",
                "enabled": True,
                "verification_status": "pending",
                "lifecycle_status": "saved_unverified",
            }
        )

        req = type("Req", (), {"map_name": "demo_map"})()
        task = type("Task", (), {"map_name": "demo_map"})()
        with self.assertRaisesRegex(ValueError, "pending verification"):
            node._resolve_map_snapshot(req, task)

    def test_schedule_api_rejects_task_bound_to_unverified_map(self):
        node = SCHEDULE_API_MODULE.ScheduleApiServiceNode.__new__(SCHEDULE_API_MODULE.ScheduleApiServiceNode)
        node.robot_id = "robot_a"
        node.plan_store = _FakePlanStore(
            {
                "map_name": "demo_map",
                "revision_id": "rev_demo",
                "enabled": True,
                "verification_status": "pending",
                "lifecycle_status": "saved_unverified",
            }
        )

        with self.assertRaisesRegex(ValueError, "pending verification"):
            node._resolve_job_map_asset(_FakeJob())

    def test_task_api_preserves_existing_job_revision_when_map_name_is_unchanged(self):
        node = TASK_API_MODULE.TaskApiServiceNode.__new__(TASK_API_MODULE.TaskApiServiceNode)
        node.robot_id = "robot_a"
        node.plan_store = _FakePlanStore(
            {
                "map_name": "demo_map",
                "revision_id": "rev_demo_old",
                "enabled": True,
                "verification_status": "verified",
                "lifecycle_status": "available",
            }
        )

        req = type("Req", (), {"map_name": ""})()
        task = type("Task", (), {"map_name": ""})()
        existing_job = type("Job", (), {"map_name": "demo_map", "map_revision_id": "rev_demo_old"})()

        snapshot = node._resolve_map_snapshot(req, task, existing_job=existing_job)

        self.assertEqual(snapshot["map_revision_id"], "rev_demo_old")
        self.assertEqual(
            node.plan_store.resolve_calls[-1],
            {
                "map_name": "demo_map",
                "revision_id": "rev_demo_old",
                "robot_id": "robot_a",
            },
        )

    def test_task_api_map_switch_does_not_reuse_old_revision(self):
        node = TASK_API_MODULE.TaskApiServiceNode.__new__(TASK_API_MODULE.TaskApiServiceNode)
        node.robot_id = "robot_a"
        node.plan_store = _FakePlanStore(
            {
                "map_name": "other_map",
                "revision_id": "rev_other_head",
                "enabled": True,
                "verification_status": "verified",
                "lifecycle_status": "available",
            }
        )

        req = type("Req", (), {"map_name": "other_map"})()
        task = type("Task", (), {"map_name": ""})()
        existing_job = type("Job", (), {"map_name": "demo_map", "map_revision_id": "rev_demo_old"})()

        snapshot = node._resolve_map_snapshot(req, task, existing_job=existing_job)

        self.assertEqual(snapshot["map_revision_id"], "rev_other_head")
        self.assertEqual(
            node.plan_store.resolve_calls[-1],
            {
                "map_name": "other_map",
                "robot_id": "robot_a",
            },
        )

    def test_task_api_accepts_revision_only_request(self):
        node = TASK_API_MODULE.TaskApiServiceNode.__new__(TASK_API_MODULE.TaskApiServiceNode)
        node.robot_id = "robot_a"
        node.plan_store = _FakePlanStore(
            {
                "map_name": "demo_map",
                "revision_id": "rev_demo_only",
                "enabled": True,
                "verification_status": "verified",
                "lifecycle_status": "available",
            }
        )

        req = type("Req", (), {"map_name": ""})()
        task = type("Task", (), {"map_name": "", "map_revision_id": "rev_demo_only"})()

        snapshot = node._resolve_map_snapshot(req, task)

        self.assertEqual(snapshot["map_name"], "demo_map")
        self.assertEqual(snapshot["map_revision_id"], "rev_demo_only")
        self.assertEqual(
            node.plan_store.resolve_calls[0],
            {
                "revision_id": "rev_demo_only",
                "robot_id": "robot_a",
            },
        )
        self.assertEqual(
            node.plan_store.resolve_calls[-1],
            {
                "map_name": "demo_map",
                "revision_id": "rev_demo_only",
                "robot_id": "robot_a",
            },
        )

    def test_task_api_map_name_only_request_prefers_verified_revision_scope(self):
        node = TASK_API_MODULE.TaskApiServiceNode.__new__(TASK_API_MODULE.TaskApiServiceNode)
        node.robot_id = "robot_a"
        node.plan_store = _FakePlanStore(
            {
                "map_name": "demo_map",
                "revision_id": "rev_demo_verified",
                "enabled": True,
                "verification_status": "verified",
                "lifecycle_status": "available",
            }
        )

        req = type("Req", (), {"map_name": "demo_map"})()
        task = type("Task", (), {"map_name": "demo_map", "map_revision_id": ""})()

        snapshot = node._resolve_map_snapshot(req, task)

        self.assertEqual(snapshot["map_revision_id"], "rev_demo_verified")
        self.assertEqual(
            node.plan_store.resolve_calls[-1],
            {
                "map_name": "demo_map",
                "robot_id": "robot_a",
            },
        )

    def test_task_api_job_to_task_reports_map_revision_id(self):
        node = TASK_API_MODULE.TaskApiServiceNode.__new__(TASK_API_MODULE.TaskApiServiceNode)

        job = type(
            "Job",
            (),
            {
                "job_id": "12",
                "job_name": "demo",
                "default_loops": 2,
                "zone_id": "zone_a",
                "plan_profile_name": "cover_standard",
                "sys_profile_name": "standard",
                "default_clean_mode": "scrub",
                "map_name": "demo_map",
                "map_revision_id": "rev_demo_only",
                "return_to_dock_on_finish": True,
                "repeat_after_full_charge": False,
                "enabled": True,
            },
        )()
        node._status_for_job = lambda _job_id: 0

        task_msg = node._job_to_task(job)

        self.assertEqual(task_msg.map_revision_id, "rev_demo_only")

    def test_schedule_api_message_reports_map_revision_id(self):
        node = SCHEDULE_API_MODULE.ScheduleApiServiceNode.__new__(SCHEDULE_API_MODULE.ScheduleApiServiceNode)

        rec = type(
            "Schedule",
            (),
            {
                "schedule_id": "sched_1",
                "job_id": "12",
                "job_name": "demo",
                "enabled": True,
                "schedule_type": "daily",
                "dow": [],
                "time_local": "09:00",
                "timezone": "Asia/Shanghai",
                "start_date": "",
                "end_date": "",
                "map_name": "demo_map",
                "map_revision_id": "rev_demo_only",
                "zone_id": "zone_a",
                "default_loops": 2,
                "plan_profile_name": "cover_standard",
                "sys_profile_name": "standard",
                "default_clean_mode": "scrub",
                "return_to_dock_on_finish": True,
                "repeat_after_full_charge": False,
                "last_fire_ts": 0.0,
                "last_done_ts": 0.0,
                "last_status": "",
            },
        )()

        schedule_msg = node._schedule_to_msg(rec)

        self.assertEqual(schedule_msg.map_revision_id, "rev_demo_only")

    def test_schedule_api_prefers_verified_revision_scope_for_name_only_job_binding(self):
        node = SCHEDULE_API_MODULE.ScheduleApiServiceNode.__new__(SCHEDULE_API_MODULE.ScheduleApiServiceNode)
        node.robot_id = "robot_a"
        node.plan_store = _FakePlanStore(
            {
                "map_name": "demo_map",
                "revision_id": "rev_demo_verified",
                "enabled": True,
                "verification_status": "verified",
                "lifecycle_status": "available",
            }
        )

        job = type("Job", (), {"map_name": "demo_map", "map_revision_id": ""})()

        asset = node._resolve_job_map_asset(job)

        self.assertEqual(asset["revision_id"], "rev_demo_verified")
        self.assertEqual(
            node.plan_store.resolve_calls[-1],
            {
                "map_name": "demo_map",
                "robot_id": "robot_a",
            },
        )

    def test_task_api_get_all_filters_by_map_revision_id(self):
        node = TASK_API_MODULE.TaskApiServiceNode.__new__(TASK_API_MODULE.TaskApiServiceNode)
        node.ops = _FakeOpsForTaskList(
            [
                type(
                    "Job",
                    (),
                    {
                        "job_id": "1",
                        "job_name": "task_a",
                        "default_loops": 1,
                        "zone_id": "zone_a",
                        "plan_profile_name": "cover_standard",
                        "sys_profile_name": "standard",
                        "default_clean_mode": "scrub",
                        "map_name": "demo_map",
                        "map_revision_id": "rev_demo_01",
                        "return_to_dock_on_finish": False,
                        "repeat_after_full_charge": False,
                        "enabled": True,
                    },
                )(),
                type(
                    "Job",
                    (),
                    {
                        "job_id": "2",
                        "job_name": "task_b",
                        "default_loops": 1,
                        "zone_id": "zone_a",
                        "plan_profile_name": "cover_standard",
                        "sys_profile_name": "standard",
                        "default_clean_mode": "scrub",
                        "map_name": "demo_map",
                        "map_revision_id": "rev_demo_02",
                        "return_to_dock_on_finish": False,
                        "repeat_after_full_charge": False,
                        "enabled": True,
                    },
                )(),
            ]
        )
        node._status_for_job = lambda _job_id: 0

        req = type(
            "Req",
            (),
            {
                "getAll": int(TASK_API_MODULE.OperateTaskRequest.getAll),
                "operation": int(TASK_API_MODULE.OperateTaskRequest.getAll),
                "map_name": "",
                "task": type("Task", (), {"map_name": "", "map_revision_id": "rev_demo_02"})(),
            },
        )()

        resp = node._handle(req)

        self.assertTrue(resp.success)
        self.assertEqual([item.task_id for item in list(resp.tasks)], [2])
        self.assertEqual([item.map_revision_id for item in list(resp.tasks)], ["rev_demo_02"])

    def test_schedule_api_get_all_filters_by_map_revision_id(self):
        node = SCHEDULE_API_MODULE.ScheduleApiServiceNode.__new__(SCHEDULE_API_MODULE.ScheduleApiServiceNode)
        node.ops = _FakeOpsForScheduleList(
            [
                type(
                    "Schedule",
                    (),
                    {
                        "schedule_id": "sched_1",
                        "job_id": "1",
                        "job_name": "task_a",
                        "enabled": True,
                        "schedule_type": "daily",
                        "dow": [],
                        "time_local": "09:00",
                        "timezone": "Asia/Shanghai",
                        "start_date": "",
                        "end_date": "",
                        "map_name": "demo_map",
                        "map_revision_id": "rev_demo_01",
                        "zone_id": "zone_a",
                        "default_loops": 1,
                        "plan_profile_name": "cover_standard",
                        "sys_profile_name": "standard",
                        "default_clean_mode": "scrub",
                        "return_to_dock_on_finish": False,
                        "repeat_after_full_charge": False,
                        "last_fire_ts": 0.0,
                        "last_done_ts": 0.0,
                        "last_status": "",
                    },
                )(),
                type(
                    "Schedule",
                    (),
                    {
                        "schedule_id": "sched_2",
                        "job_id": "2",
                        "job_name": "task_b",
                        "enabled": True,
                        "schedule_type": "daily",
                        "dow": [],
                        "time_local": "10:00",
                        "timezone": "Asia/Shanghai",
                        "start_date": "",
                        "end_date": "",
                        "map_name": "demo_map",
                        "map_revision_id": "rev_demo_02",
                        "zone_id": "zone_a",
                        "default_loops": 1,
                        "plan_profile_name": "cover_standard",
                        "sys_profile_name": "standard",
                        "default_clean_mode": "scrub",
                        "return_to_dock_on_finish": False,
                        "repeat_after_full_charge": False,
                        "last_fire_ts": 0.0,
                        "last_done_ts": 0.0,
                        "last_status": "",
                    },
                )(),
            ]
        )

        req = type(
            "Req",
            (),
            {
                "getAll": int(SCHEDULE_API_MODULE.OperateScheduleRequest.getAll),
                "operation": int(SCHEDULE_API_MODULE.OperateScheduleRequest.getAll),
                "task_id": 0,
                "schedule": type("ScheduleReq", (), {"task_id": 0, "map_name": "", "map_revision_id": "rev_demo_01"})(),
            },
        )()

        resp = node._handle(req)

        self.assertTrue(resp.success)
        self.assertEqual([item.schedule_id for item in list(resp.schedules)], ["sched_1"])
        self.assertEqual([item.map_revision_id for item in list(resp.schedules)], ["rev_demo_01"])

    def test_task_api_add_honors_explicit_return_to_dock_false(self):
        node = TASK_API_MODULE.TaskApiServiceNode.__new__(TASK_API_MODULE.TaskApiServiceNode)
        node.robot_id = "robot_a"
        node.default_return_to_dock_on_finish = True
        node.ops = _FakeOpsForTaskUpsert()
        node._resolve_map_snapshot = (
            lambda req, task, existing_job=None: {
                "map_name": "demo_map",
                "map_revision_id": "rev_demo_verified",
            }
        )
        node._ensure_zone_plan_ready = lambda **kwargs: None
        node._status_for_job = lambda _job_id: 0

        req = types.SimpleNamespace(
            ENABLE_KEEP=int(TASK_API_MODULE.OperateTaskRequest.ENABLE_KEEP),
            ENABLE_DISABLE=int(TASK_API_MODULE.OperateTaskRequest.ENABLE_DISABLE),
            ENABLE_ENABLE=int(TASK_API_MODULE.OperateTaskRequest.ENABLE_ENABLE),
            RETURN_TO_DOCK_KEEP=int(TASK_API_MODULE.OperateTaskRequest.RETURN_TO_DOCK_KEEP),
            RETURN_TO_DOCK_DISABLE=int(TASK_API_MODULE.OperateTaskRequest.RETURN_TO_DOCK_DISABLE),
            RETURN_TO_DOCK_ENABLE=int(TASK_API_MODULE.OperateTaskRequest.RETURN_TO_DOCK_ENABLE),
            REPEAT_AFTER_FULL_CHARGE_KEEP=int(TASK_API_MODULE.OperateTaskRequest.REPEAT_AFTER_FULL_CHARGE_KEEP),
            REPEAT_AFTER_FULL_CHARGE_DISABLE=int(TASK_API_MODULE.OperateTaskRequest.REPEAT_AFTER_FULL_CHARGE_DISABLE),
            REPEAT_AFTER_FULL_CHARGE_ENABLE=int(TASK_API_MODULE.OperateTaskRequest.REPEAT_AFTER_FULL_CHARGE_ENABLE),
            map_name="",
            enabled_state=int(TASK_API_MODULE.OperateTaskRequest.ENABLE_KEEP),
            return_to_dock_state=int(TASK_API_MODULE.OperateTaskRequest.RETURN_TO_DOCK_KEEP),
            repeat_after_full_charge_state=int(TASK_API_MODULE.OperateTaskRequest.REPEAT_AFTER_FULL_CHARGE_KEEP),
            task=types.SimpleNamespace(
                task_id=0,
                name="task_demo",
                loops=1,
                zone_id="zone_demo",
                plan_profile_name="cover_standard",
                sys_profile_name="standard",
                clean_mode="scrub",
                map_name="demo_map",
                map_revision_id="rev_demo_verified",
                return_to_dock_on_finish=False,
                repeat_after_full_charge=False,
                enabled=True,
            ),
        )

        task_msg = node._upsert_task(req, allow_new_id=True)

        self.assertFalse(task_msg.return_to_dock_on_finish)
        self.assertIsNotNone(node.ops.last_upsert)
        self.assertFalse(node.ops.last_upsert["return_to_dock_on_finish"])


if __name__ == "__main__":
    unittest.main()

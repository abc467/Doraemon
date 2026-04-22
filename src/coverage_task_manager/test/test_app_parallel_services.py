import importlib.util
import pathlib
import types
import unittest

from cleanrobot_app_msgs.msg import CleanSchedule as AppCleanSchedule, CleanTask as AppCleanTask
from cleanrobot_app_msgs.srv import (
    OperateScheduleRequest as AppOperateScheduleRequest,
    OperateScheduleResponse as AppOperateScheduleResponse,
    OperateTaskRequest as AppOperateTaskRequest,
    OperateTaskResponse as AppOperateTaskResponse,
)


def _load_module(rel_path: str, module_name: str):
    script_path = pathlib.Path(__file__).resolve().parents[1] / "scripts" / rel_path
    spec = importlib.util.spec_from_file_location(module_name, str(script_path))
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


TASK_API_MODULE = _load_module("task_api_service_node.py", "task_api_service_node_app_test_mod")
SCHEDULE_API_MODULE = _load_module("schedule_api_service_node.py", "schedule_api_service_node_app_test_mod")


class AppParallelServiceTest(unittest.TestCase):
    def test_task_api_app_get_all_returns_cleanrobot_app_payload(self):
        node = TASK_API_MODULE.TaskApiServiceNode.__new__(TASK_API_MODULE.TaskApiServiceNode)
        node.ops = types.SimpleNamespace(
            list_jobs=lambda: [
                types.SimpleNamespace(
                    job_id="7",
                    job_name="task_demo",
                    default_loops=2,
                    zone_id="zone_a",
                    plan_profile_name="cover_standard",
                    sys_profile_name="standard",
                    default_clean_mode="scrub",
                    map_name="demo_map",
                    map_revision_id="rev_demo_01",
                    return_to_dock_on_finish=True,
                    repeat_after_full_charge=False,
                    enabled=True,
                )
            ]
        )
        node._status_for_job = lambda _job_id: 0

        req = AppOperateTaskRequest()
        req.operation = int(req.getAll)
        resp = node._handle_app(req)

        self.assertIsInstance(resp, AppOperateTaskResponse)
        self.assertEqual(len(resp.tasks), 1)
        self.assertIsInstance(resp.tasks[0], AppCleanTask)
        self.assertEqual(resp.tasks[0].map_revision_id, "rev_demo_01")

    def test_schedule_api_app_get_all_returns_cleanrobot_app_payload(self):
        node = SCHEDULE_API_MODULE.ScheduleApiServiceNode.__new__(SCHEDULE_API_MODULE.ScheduleApiServiceNode)
        node.ops = types.SimpleNamespace(
            list_schedules=lambda include_disabled, job_id: [
                types.SimpleNamespace(
                    schedule_id="sched_a",
                    job_id="7",
                    job_name="task_demo",
                    enabled=True,
                    schedule_type="daily",
                    dow=[],
                    time_local="09:00",
                    timezone="Asia/Shanghai",
                    start_date="2026-04-20",
                    end_date="",
                    map_name="demo_map",
                    map_revision_id="rev_demo_01",
                    zone_id="zone_a",
                    default_loops=2,
                    plan_profile_name="cover_standard",
                    sys_profile_name="standard",
                    default_clean_mode="scrub",
                    return_to_dock_on_finish=True,
                    repeat_after_full_charge=False,
                    last_fire_ts=0.0,
                    last_done_ts=0.0,
                    last_status="idle",
                )
            ]
        )

        req = AppOperateScheduleRequest()
        req.operation = int(req.getAll)
        resp = node._handle_app(req)

        self.assertIsInstance(resp, AppOperateScheduleResponse)
        self.assertEqual(len(resp.schedules), 1)
        self.assertIsInstance(resp.schedules[0], AppCleanSchedule)
        self.assertEqual(resp.schedules[0].map_revision_id, "rev_demo_01")


if __name__ == "__main__":
    unittest.main()

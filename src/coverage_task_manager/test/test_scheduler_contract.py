import unittest

from coverage_task_manager.scheduler import Scheduler


class SchedulerContractTest(unittest.TestCase):
    def test_rejects_old_format_profile_name_key(self):
        schedules = [
            {
                "id": "weekly_demo",
                "type": "weekly",
                "dow": [0],
                "time": "10:10",
                "task": {
                    "zone_id": "zone_demo",
                    "profile_name": "cover_standard",
                },
            }
        ]

        with self.assertRaisesRegex(ValueError, "unsupported old-format keys"):
            Scheduler.from_param(schedules)

    def test_rejects_old_format_plan_profile_key(self):
        schedules = [
            {
                "id": "weekly_demo",
                "type": "weekly",
                "dow": [0],
                "time": "10:10",
                "task": {
                    "zone_id": "zone_demo",
                    "plan_profile": "cover_standard",
                },
            }
        ]

        with self.assertRaisesRegex(ValueError, "unsupported old-format keys"):
            Scheduler.from_param(schedules)

    def test_accepts_canonical_profile_keys(self):
        schedules = [
            {
                "id": "weekly_demo",
                "type": "weekly",
                "dow": [0],
                "time": "10:10",
                "task": {
                    "zone_id": "zone_demo",
                    "plan_profile_name": "cover_standard",
                    "sys_profile_name": "standard",
                    "clean_mode": "scrub",
                },
            }
        ]

        scheduler = Scheduler.from_param(schedules)

        self.assertEqual(len(scheduler.jobs), 1)
        job = scheduler.jobs[0]
        self.assertEqual(job.task.plan_profile_name, "cover_standard")
        self.assertEqual(job.task.sys_profile_name, "standard")
        self.assertEqual(job.task.clean_mode, "scrub")


if __name__ == "__main__":
    unittest.main()

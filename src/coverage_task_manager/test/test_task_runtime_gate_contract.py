import unittest
import threading
from unittest import mock

from cleanrobot_app_msgs.msg import SlamState

from coverage_task_manager.task_manager import SlamStateSnapshot, TaskManager


class TaskManagerRuntimeGateContractTest(unittest.TestCase):
    def _manager(self):
        mgr = TaskManager.__new__(TaskManager)
        mgr._lock = threading.Lock()
        mgr._task_map_name = ""
        mgr._task_map_revision_id = ""
        mgr._mission_store = None
        mgr._plan_store = None
        mgr._robot_id = "local_robot"
        mgr._require_runtime_map_match = True
        mgr._require_managed_map_asset = True
        mgr._require_runtime_localized_before_start = True
        mgr._runtime_localization_state_param = "/cartographer/runtime/localization_state"
        mgr._runtime_localization_valid_param = "/cartographer/runtime/localization_valid"
        mgr._slam_state = SlamStateSnapshot()
        mgr._slam_state_stale_timeout_s = 5.0
        mgr._get_selected_active_map = lambda: {
            "map_name": "demo_map",
            "revision_id": "rev_demo_01",
        }
        return mgr

    def _set_slam_state(
        self,
        mgr,
        *,
        active_map_name="",
        runtime_map_match=False,
        runtime_map_name="",
        runtime_map_id="",
        runtime_map_md5="",
        localization_state="",
        localization_valid=False,
        ts=1234.5,
    ):
        msg = SlamState()
        msg.active_map_name = str(active_map_name or "")
        msg.runtime_map_match = bool(runtime_map_match)
        msg.runtime_map_name = str(runtime_map_name or "")
        msg.runtime_map_id = str(runtime_map_id or "")
        msg.runtime_map_md5 = str(runtime_map_md5 or "")
        msg.localization_state = str(localization_state or "")
        msg.localization_valid = bool(localization_valid)
        mgr._slam_state = SlamStateSnapshot(msg=msg, ts=float(ts))

    def test_validate_runtime_map_asset_reports_selected_map_mismatch_with_guidance(self):
        mgr = self._manager()
        mgr._wait_for_runtime_map = lambda asset, timeout_s=None: False
        mgr._runtime_map_snapshot = lambda refresh=False: {
            "map_name": "runtime_map",
            "map_id": "map_runtime",
            "map_md5": "md5_runtime",
        }

        ok, msg = mgr._validate_runtime_map_asset(
            {
                "map_name": "selected_map",
                "map_id": "map_selected",
                "map_md5": "md5_selected",
                "enabled": True,
            }
        )

        self.assertFalse(ok)
        self.assertEqual(
            msg,
            "runtime map_name runtime_map != selected map selected_map; "
            "complete SLAM switch_map_and_localize or prepare_for_task before start/resume",
        )

    @mock.patch("coverage_task_manager.task_manager.rospy.get_param")
    def test_ensure_runtime_localized_reuses_shared_localization_message(self, get_param):
        mgr = self._manager()
        get_param.side_effect = lambda key, default=None: {
            mgr._runtime_localization_state_param: "not_localized",
            mgr._runtime_localization_valid_param: False,
        }.get(key, default)

        ok, msg = mgr._ensure_runtime_localized()

        self.assertFalse(ok)
        self.assertEqual(
            msg,
            "runtime localization not ready: state=not_localized valid=false; "
            "relocalize before start/resume",
        )

    @mock.patch("coverage_task_manager.task_manager.time.time", return_value=1234.5)
    def test_validate_runtime_map_asset_prefers_fresh_slam_state_match(self, _time_now):
        mgr = self._manager()
        self._set_slam_state(
            mgr,
            active_map_name="selected_map",
            runtime_map_match=True,
        )
        mgr._wait_for_runtime_map = lambda asset, timeout_s=None: (_ for _ in ()).throw(
            AssertionError("should not wait when fresh slam_state already proves runtime_map_match")
        )

        ok, msg = mgr._validate_runtime_map_asset(
            {
                "map_name": "selected_map",
                "map_id": "map_selected",
                "map_md5": "md5_selected",
                "enabled": True,
            }
        )

        self.assertTrue(ok)
        self.assertEqual(msg, "runtime_map_match")

    @mock.patch("coverage_task_manager.task_manager.ensure_map_identity")
    @mock.patch("coverage_task_manager.task_manager.get_runtime_map_revision_id")
    @mock.patch("coverage_task_manager.task_manager.get_runtime_map_scope")
    def test_wait_for_runtime_map_prefers_revision_scope_over_stale_identity(
        self,
        get_runtime_map_scope,
        get_runtime_map_revision_id,
        ensure_map_identity,
    ):
        mgr = self._manager()
        mgr._wait = lambda wait_s, predicate, sleep_s=0.1: predicate()
        get_runtime_map_scope.return_value = ("selected_map", "md5_runtime_stale")
        get_runtime_map_revision_id.return_value = "rev_selected"
        ensure_map_identity.return_value = ("map_runtime_stale", "md5_runtime_stale", True)

        matched = mgr._wait_for_runtime_map(
            {
                "map_name": "selected_map",
                "revision_id": "rev_selected",
                "map_id": "map_selected",
                "map_md5": "md5_selected",
            },
            timeout_s=0.2,
        )

        self.assertTrue(matched)

    @mock.patch("coverage_task_manager.task_manager.ensure_map_identity")
    @mock.patch("coverage_task_manager.task_manager.get_runtime_map_revision_id")
    @mock.patch("coverage_task_manager.task_manager.get_runtime_map_scope")
    def test_wait_for_runtime_map_falls_back_to_name_md5_identity_when_revision_missing(
        self,
        get_runtime_map_scope,
        get_runtime_map_revision_id,
        ensure_map_identity,
    ):
        mgr = self._manager()
        mgr._wait = lambda wait_s, predicate, sleep_s=0.1: predicate()
        get_runtime_map_scope.return_value = ("selected_map", "md5_selected")
        get_runtime_map_revision_id.return_value = ""
        ensure_map_identity.return_value = ("map_selected", "md5_selected", True)

        matched = mgr._wait_for_runtime_map(
            {
                "map_name": "selected_map",
                "revision_id": "rev_selected",
                "map_id": "map_selected",
                "map_md5": "md5_selected",
            },
            timeout_s=0.2,
        )

        self.assertTrue(matched)

    @mock.patch("coverage_task_manager.task_manager.time.time", return_value=1234.5)
    @mock.patch("coverage_task_manager.task_manager.rospy.get_param")
    def test_ensure_runtime_localized_prefers_fresh_slam_state(self, get_param, _time_now):
        mgr = self._manager()
        self._set_slam_state(
            mgr,
            localization_state="localized",
            localization_valid=True,
        )
        get_param.side_effect = lambda key, default=None: {
            mgr._runtime_localization_state_param: "not_localized",
            mgr._runtime_localization_valid_param: False,
        }.get(key, default)

        ok, msg = mgr._ensure_runtime_localized()

        self.assertTrue(ok)
        self.assertEqual(msg, "")

    @mock.patch("coverage_task_manager.task_manager.rospy.get_param")
    def test_ensure_runtime_localized_reports_manual_assist_guidance(self, get_param):
        mgr = self._manager()
        get_param.side_effect = lambda key, default=None: {
            mgr._runtime_localization_state_param: "manual_assist_required",
            mgr._runtime_localization_valid_param: False,
        }.get(key, default)

        ok, msg = mgr._ensure_runtime_localized()

        self.assertFalse(ok)
        self.assertEqual(
            msg,
            "runtime localization not ready: state=manual_assist_required valid=false; "
            "manual assist required for map=demo_map revision=rev_demo_01; "
            "place robot at a known anchor point, align heading, provide initial pose, then retry relocalize",
        )

    def test_prepare_map_for_run_requires_current_active_map_when_managed_assets_enabled(self):
        mgr = self._manager()
        mgr._resolve_task_map_asset = lambda explicit_map_name="", run_id="": {
            "map_name": "selected_map",
            "enabled": True,
        }
        mgr._get_selected_active_map = lambda: None

        asset, ok, msg = mgr._prepare_map_for_run()

        self.assertIsNone(asset)
        self.assertFalse(ok)
        self.assertEqual(msg, "no current active map selected")

    def test_prepare_map_for_run_reports_selected_vs_requested_map_mismatch(self):
        mgr = self._manager()
        mgr._resolve_task_map_asset = lambda explicit_map_name="", run_id="": {
            "map_name": "requested_map",
            "enabled": True,
        }
        mgr._get_selected_active_map = lambda: {"map_name": "selected_map"}

        asset, ok, msg = mgr._prepare_map_for_run(explicit_map_name="requested_map")

        self.assertIsNone(asset)
        self.assertFalse(ok)
        self.assertEqual(msg, "active selected map selected_map != requested map requested_map")

    def test_prepare_map_for_run_requires_selected_revision_when_requested_revision_bound(self):
        mgr = self._manager()
        mgr._task_map_name = "requested_map"
        mgr._task_map_revision_id = "rev_requested_01"
        mgr._resolve_task_map_asset = lambda explicit_map_name="", run_id="": {
            "map_name": "requested_map",
            "revision_id": "rev_requested_01",
            "enabled": True,
        }
        mgr._get_selected_active_map = lambda: {"map_name": "requested_map"}

        asset, ok, msg = mgr._prepare_map_for_run(explicit_map_name="requested_map")

        self.assertIsNone(asset)
        self.assertFalse(ok)
        self.assertEqual(
            msg,
            "active selected map revision unavailable for requested map revision rev_requested_01",
        )

    def test_resolve_task_map_asset_prefers_task_revision_scope_when_name_shared(self):
        mgr = self._manager()
        mgr._task_map_name = "shared_map"
        mgr._task_map_revision_id = "rev_shared_02"

        class _PlanStore(object):
            def __init__(self):
                self.calls = []

            def resolve_map_revision(self, *, revision_id="", robot_id="local_robot", map_name=""):
                self.calls.append(("resolve_map_revision", revision_id, robot_id, map_name))
                if revision_id == "rev_shared_02":
                    return {
                        "map_name": "shared_map",
                        "revision_id": "rev_shared_02",
                        "enabled": True,
                        "verification_status": "verified",
                        "lifecycle_status": "available",
                    }
                return None

            def resolve_map_asset(self, *, map_name="", robot_id="local_robot"):
                self.calls.append(("resolve_map_asset", map_name, robot_id))
                return {
                    "map_name": "shared_map",
                    "revision_id": "rev_head_latest",
                    "enabled": True,
                    "verification_status": "verified",
                    "lifecycle_status": "available",
                }

        store = _PlanStore()
        mgr._plan_store = store

        asset = mgr._resolve_task_map_asset()

        self.assertEqual(asset["revision_id"], "rev_shared_02")
        self.assertEqual(store.calls[0][0], "resolve_map_revision")
        self.assertEqual(store.calls[0][1], "rev_shared_02")
        self.assertEqual(mgr._task_map_revision_id, "rev_shared_02")

    def test_resolve_task_map_asset_does_not_reuse_stale_task_revision_for_explicit_other_map(self):
        mgr = self._manager()
        mgr._task_map_name = "shared_map"
        mgr._task_map_revision_id = "rev_shared_02"

        class _PlanStore(object):
            def __init__(self):
                self.calls = []

            def resolve_map_revision(self, *, revision_id="", robot_id="local_robot", map_name=""):
                self.calls.append(("resolve_map_revision", revision_id, robot_id, map_name))
                return None

            def resolve_map_asset(self, *, map_name="", robot_id="local_robot"):
                self.calls.append(("resolve_map_asset", map_name, robot_id))
                return {
                    "map_name": str(map_name or ""),
                    "revision_id": "rev_other_01",
                    "enabled": True,
                    "verification_status": "verified",
                    "lifecycle_status": "available",
                }

            def get_active_map(self, *, robot_id="local_robot"):
                del robot_id
                return {}

        store = _PlanStore()
        mgr._plan_store = store

        asset = mgr._resolve_task_map_asset(explicit_map_name="other_map")

        self.assertEqual(asset["map_name"], "other_map")
        self.assertEqual(asset["revision_id"], "rev_other_01")
        self.assertEqual(store.calls[0][0], "resolve_map_asset")
        self.assertEqual(store.calls[0][1], "other_map")

    def test_resolve_task_map_asset_prefers_active_verified_revision_when_head_pending(self):
        mgr = self._manager()
        mgr._task_map_name = "shared_map"

        class _PlanStore(object):
            def resolve_map_revision(self, *, revision_id="", robot_id="local_robot", map_name=""):
                del revision_id, robot_id, map_name
                return None

            def resolve_map_asset(self, *, map_name="", robot_id="local_robot"):
                del robot_id
                return {
                    "map_name": str(map_name or ""),
                    "revision_id": "rev_head_pending",
                    "enabled": True,
                    "verification_status": "pending",
                    "lifecycle_status": "saved_unverified",
                }

            def get_active_map(self, *, robot_id="local_robot"):
                del robot_id
                return {
                    "map_name": "shared_map",
                    "revision_id": "rev_active_verified",
                    "enabled": True,
                    "verification_status": "verified",
                    "lifecycle_status": "available",
                }

        mgr._plan_store = _PlanStore()

        asset = mgr._resolve_task_map_asset()

        self.assertEqual(asset["revision_id"], "rev_active_verified")
        self.assertEqual(mgr._task_map_revision_id, "rev_active_verified")

    def test_resolve_task_map_asset_rejects_pending_head_without_verified_fallback(self):
        mgr = self._manager()
        mgr._task_map_name = "shared_map"

        class _PlanStore(object):
            def resolve_map_revision(self, *, revision_id="", robot_id="local_robot", map_name=""):
                del revision_id, robot_id, map_name
                return None

            def resolve_map_asset(self, *, map_name="", robot_id="local_robot"):
                del robot_id
                return {
                    "map_name": str(map_name or ""),
                    "revision_id": "rev_head_pending",
                    "enabled": True,
                    "verification_status": "pending",
                    "lifecycle_status": "saved_unverified",
                }

            def get_active_map(self, *, robot_id="local_robot"):
                del robot_id
                return {
                    "map_name": "other_map",
                    "revision_id": "rev_other_verified",
                    "enabled": True,
                    "verification_status": "verified",
                    "lifecycle_status": "available",
                }

        mgr._plan_store = _PlanStore()

        with self.assertRaisesRegex(ValueError, "pending verification"):
            mgr._resolve_task_map_asset()

    def test_resolve_task_map_asset_rejects_run_scope_name_revision_mismatch(self):
        mgr = self._manager()

        class _Run(object):
            map_name = "other_map"
            map_revision_id = "rev_shared_02"
            zone_id = ""

        class _MissionStore(object):
            def get_run(self, run_id):
                self.last_run_id = run_id
                return _Run()

        class _PlanStore(object):
            def resolve_map_revision(self, *, revision_id="", robot_id="local_robot", map_name=""):
                return {"map_name": "shared_map", "revision_id": str(revision_id or ""), "enabled": True}

            def resolve_map_asset(self, *, map_name="", robot_id="local_robot"):
                return {"map_name": str(map_name or ""), "revision_id": "rev_other_01", "enabled": True}

        mgr._mission_store = _MissionStore()
        mgr._plan_store = _PlanStore()

        with self.assertRaisesRegex(ValueError, "task map revision does not match task map name"):
            mgr._resolve_task_map_asset(run_id="run_1")

    def test_prepare_map_for_run_reports_run_scope_name_revision_mismatch(self):
        mgr = self._manager()

        class _Run(object):
            map_name = "other_map"
            map_revision_id = "rev_shared_02"
            zone_id = ""

        class _MissionStore(object):
            def get_run(self, run_id):
                self.last_run_id = run_id
                return _Run()

        class _PlanStore(object):
            def resolve_map_revision(self, *, revision_id="", robot_id="local_robot", map_name=""):
                return {"map_name": "shared_map", "revision_id": str(revision_id or ""), "enabled": True}

            def resolve_map_asset(self, *, map_name="", robot_id="local_robot"):
                return {"map_name": str(map_name or ""), "revision_id": "rev_other_01", "enabled": True}

        mgr._mission_store = _MissionStore()
        mgr._plan_store = _PlanStore()
        mgr._get_selected_active_map = lambda: {"map_name": "shared_map", "revision_id": "rev_shared_02"}

        asset, ok, msg = mgr._prepare_map_for_run(run_id="run_1")

        self.assertIsNone(asset)
        self.assertFalse(ok)
        self.assertEqual(msg, "task map revision does not match task map name")

    def test_ensure_zone_plan_ready_for_job_prefers_revision_scope(self):
        mgr = self._manager()

        class _PlanStore(object):
            def __init__(self):
                self.zone_calls = []
                self.plan_calls = []

            def get_zone_meta(self, zone_id, *, map_name="", map_revision_id="", map_version=""):
                self.zone_calls.append((zone_id, map_name, map_revision_id, map_version))
                return {
                    "map_name": str(map_name or ""),
                    "map_revision_id": str(map_revision_id or ""),
                    "enabled": True,
                }

            def get_active_plan_id(self, zone_id, plan_profile_name=None, *, map_name="", map_revision_id=""):
                self.plan_calls.append((zone_id, plan_profile_name, map_name, map_revision_id))
                return "plan_rev_1"

            def load_plan_meta(self, plan_id):
                return {
                    "plan_id": str(plan_id),
                    "map_name": "selected_map",
                    "map_revision_id": "rev_selected",
                    "zone_id": "zone_a",
                    "plan_profile_name": "cover_standard",
                }

        store = _PlanStore()
        mgr._plan_store = store

        ok, msg = mgr._ensure_zone_plan_ready_for_job(
            map_name="selected_map",
            map_revision_id="rev_selected",
            zone_id="zone_a",
            plan_profile_name="cover_standard",
        )

        self.assertTrue(ok)
        self.assertEqual(msg, "")
        self.assertEqual(store.zone_calls[0][2], "rev_selected")
        self.assertEqual(store.plan_calls[0][3], "rev_selected")

    def test_ensure_zone_plan_ready_for_job_rejects_plan_revision_mismatch(self):
        mgr = self._manager()

        class _PlanStore(object):
            def get_zone_meta(self, zone_id, *, map_name="", map_revision_id="", map_version=""):
                return {"enabled": True}

            def get_active_plan_id(self, zone_id, plan_profile_name=None, *, map_name="", map_revision_id=""):
                return "plan_rev_bad"

            def load_plan_meta(self, plan_id):
                return {
                    "plan_id": str(plan_id),
                    "map_name": "selected_map",
                    "map_revision_id": "rev_other",
                    "zone_id": "zone_a",
                    "plan_profile_name": "cover_standard",
                }

        mgr._plan_store = _PlanStore()

        ok, msg = mgr._ensure_zone_plan_ready_for_job(
            map_name="selected_map",
            map_revision_id="rev_selected",
            zone_id="zone_a",
            plan_profile_name="cover_standard",
        )

        self.assertFalse(ok)
        self.assertEqual(msg, "active plan revision does not match task map revision")


if __name__ == "__main__":
    unittest.main()

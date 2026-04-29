import threading
import unittest
from types import SimpleNamespace
from unittest import mock

import rospy

from cleanrobot_app_msgs.msg import OdometryState, SlamState

from coverage_task_manager.task_manager import (
    BatterySnapshot,
    BoolSnapshot,
    MsgSnapshot,
    OdometrySnapshot,
    SlamStateSnapshot,
    TaskManager,
)


class TaskManagerReadinessTest(unittest.TestCase):
    def setUp(self):
        self.now = 1234.5

    def _odometry_msg(self, *, valid=True, code="", message="ok", warnings=None):
        msg = OdometryState()
        msg.robot_id = "local_robot"
        msg.odom_source = "odom_stream"
        msg.odom_topic = "/odom"
        msg.raw_odom_topic = "/odom_raw"
        msg.imu_topic = "/imu_corrected"
        msg.validation_mode = "odom_stream"
        msg.connected = True
        msg.odom_stream_ready = True
        msg.frame_id_valid = True
        msg.child_frame_id_valid = True
        msg.wheel_speed_node_ready = True
        msg.imu_preprocess_node_ready = True
        msg.ekf_node_ready = True
        msg.wheel_speed_fresh = True
        msg.imu_fresh = True
        msg.odom_fresh = True
        msg.odom_valid = bool(valid)
        msg.error_code = str(code or "")
        msg.message = str(message or "")
        msg.warnings = list(warnings or [])
        return msg

    def _build_manager(
        self,
        *,
        odometry_msg,
        odometry_ts=None,
        require_odometry=True,
        localization_state="localized",
        localization_valid=True,
        online_nodes=None,
        mission_state="IDLE",
        phase="IDLE",
        public_state="IDLE",
        executor_state="IDLE",
        slam_task_ready=True,
        slam_blocking_reason="",
        slam_busy=False,
        slam_manual_assist=False,
        slam_localization_state=None,
        slam_localization_valid=None,
        slam_state_ts=None,
        require_mcore_bridge=False,
    ):
        mgr = TaskManager.__new__(TaskManager)
        mgr._lock = threading.Lock()
        mgr._mission_state = str(mission_state or "IDLE")
        mgr._phase = str(phase or "IDLE")
        mgr._public_state = str(public_state or "IDLE")
        mgr._executor_state = str(executor_state or "IDLE")
        mgr._dock_supply_state = "IDLE"
        mgr._dock_supply_state_ts = self.now
        mgr._battery = BatterySnapshot(soc=0.52, ts=self.now)
        mgr._combined_status = MsgSnapshot(ts=self.now)
        mgr._station_status = MsgSnapshot(ts=0.0)
        mgr._mcore_connected = BoolSnapshot(value=True, ts=self.now)
        mgr._station_connected = BoolSnapshot(value=None, ts=0.0)
        mgr._odometry_state = OdometrySnapshot(
            msg=odometry_msg,
            ts=self.now if odometry_ts is None else float(odometry_ts),
        )
        slam_state = SlamState()
        slam_state.workflow_state = "LOCALIZED" if slam_task_ready else "IDLE"
        slam_state.workflow_phase = "ready" if slam_task_ready else "idle"
        slam_state.task_ready = bool(slam_task_ready)
        slam_state.busy = bool(slam_busy)
        slam_state.manual_assist_required = bool(slam_manual_assist)
        slam_state.blocking_reason = str(slam_blocking_reason or "")
        slam_state.blocking_reasons = [str(slam_blocking_reason or "")] if slam_blocking_reason else []
        slam_state.localization_state = str(
            localization_state if slam_localization_state is None else slam_localization_state
        )
        slam_state.localization_valid = bool(
            localization_valid if slam_localization_valid is None else slam_localization_valid
        )
        mgr._slam_state = SlamStateSnapshot(
            msg=slam_state,
            ts=self.now if slam_state_ts is None else float(slam_state_ts),
        )
        mgr._health_fault_active = False
        mgr._health_error_code = ""
        mgr._health_error_msg = ""
        mgr._dock_supply_enable = False
        mgr._require_odometry_healthy_before_start = bool(require_odometry)
        mgr._odometry_state_stale_timeout_s = 5.0
        mgr._slam_state_stale_timeout_s = 5.0
        mgr._connected_stale_timeout_s = 5.0
        mgr._combined_status_stale_timeout_s = 5.0
        mgr._station_status_stale_timeout_s = 5.0
        mgr._require_mcore_bridge_for_readiness = bool(require_mcore_bridge)
        mgr.battery_stale_timeout_s = 5.0
        mgr._runtime_localization_state_param = "/cartographer/runtime/localization_state"
        mgr._runtime_localization_valid_param = "/cartographer/runtime/localization_valid"
        mgr.zone_id_default = "zone_demo"
        mgr._runtime_map_snapshot = lambda refresh=False: {
            "map_name": "site_live_saved_20260412_2122",
            "revision_id": "rev_site_01",
            "map_id": "map_7d277f7d",
            "map_md5": "7d277f7d8bd88fb3566cfcb146e2d653",
        }
        mgr._get_selected_active_map = lambda: {
            "map_name": "site_live_saved_20260412_2122",
            "revision_id": "rev_site_01",
            "map_id": "map_7d277f7d",
            "map_md5": "7d277f7d8bd88fb3566cfcb146e2d653",
        }
        online = set(
            online_nodes
            or {
                "/move_base_flex",
                "/mcore_tcp_bridge",
            }
        )
        mgr._node_online = lambda node_name: str(node_name or "").strip() in online
        mgr._resolve_job_record = lambda _task_id: None
        mgr._ensure_zone_plan_ready_for_job = lambda **kwargs: (True, "")

        param_values = {
            mgr._runtime_localization_state_param: localization_state,
            mgr._runtime_localization_valid_param: localization_valid,
        }

        get_param = lambda name, default=None: param_values.get(name, default)
        fake_now = rospy.Time.from_sec(self.now)

        return mgr, get_param, fake_now

    def test_ready_when_odometry_healthy_even_if_station_warning_exists(self):
        mgr, get_param, fake_now = self._build_manager(
            odometry_msg=self._odometry_msg(valid=True),
            require_odometry=True,
            online_nodes={"/move_base_flex", "/mcore_tcp_bridge"},
        )
        with mock.patch("coverage_task_manager.task_manager.rospy.get_param", side_effect=get_param), mock.patch(
            "coverage_task_manager.task_manager.rospy.Time.now", return_value=fake_now
        ), mock.patch("coverage_task_manager.task_manager.time.time", return_value=self.now):
            readiness = mgr._build_system_readiness(task_id=0, refresh_map_identity=False)

        self.assertTrue(readiness.overall_ready)
        self.assertTrue(readiness.can_start_task)
        self.assertIn("station bridge offline", list(readiness.warnings))
        self.assertEqual(readiness.active_map_revision_id, "rev_site_01")
        self.assertEqual(readiness.runtime_map_revision_id, "rev_site_01")
        odom_check = next(item for item in readiness.checks if item.key == "odometry")
        self.assertEqual(odom_check.level, "OK")
        self.assertEqual(odom_check.summary, "mode=odom_stream stream=true valid=true code=- msg=ok")

    def test_mcore_bridge_offline_is_diagnostic_not_readiness_blocker(self):
        mgr, get_param, fake_now = self._build_manager(
            odometry_msg=self._odometry_msg(valid=True),
            require_odometry=True,
            online_nodes={"/move_base_flex"},
        )
        mgr._mcore_connected = BoolSnapshot(value=None, ts=0.0)
        with mock.patch("coverage_task_manager.task_manager.rospy.get_param", side_effect=get_param), mock.patch(
            "coverage_task_manager.task_manager.rospy.Time.now", return_value=fake_now
        ), mock.patch("coverage_task_manager.task_manager.time.time", return_value=self.now):
            readiness = mgr._build_system_readiness(task_id=0, refresh_map_identity=False)

        self.assertTrue(readiness.overall_ready)
        self.assertTrue(readiness.can_start_task)
        self.assertNotIn("mcore bridge not ready", list(readiness.blocking_reasons))
        mcore_check = next(item for item in readiness.checks if item.key == "mcore_bridge")
        self.assertEqual(mcore_check.level, "WARN")
        self.assertEqual(mcore_check.summary, "node offline")

    def test_blocks_when_odometry_invalid(self):
        mgr, get_param, fake_now = self._build_manager(
            odometry_msg=self._odometry_msg(
                valid=False,
                code="odom_stale",
                message="odom stale",
                warnings=["/odom stale or missing"],
            ),
            require_odometry=True,
            slam_task_ready=False,
            slam_blocking_reason="odometry not ready: code=odom_stale message=odom stale",
        )
        with mock.patch("coverage_task_manager.task_manager.rospy.get_param", side_effect=get_param), mock.patch(
            "coverage_task_manager.task_manager.rospy.Time.now", return_value=fake_now
        ), mock.patch("coverage_task_manager.task_manager.time.time", return_value=self.now):
            readiness = mgr._build_system_readiness(task_id=0, refresh_map_identity=False)

        self.assertFalse(readiness.overall_ready)
        self.assertFalse(readiness.can_start_task)
        self.assertIn(
            "odometry not ready: code=odom_stale message=odom stale",
            list(readiness.blocking_reasons),
        )
        self.assertIn("odometry: /odom stale or missing", list(readiness.warnings))
        odom_check = next(item for item in readiness.checks if item.key == "odometry")
        self.assertEqual(odom_check.level, "WARN")
        self.assertEqual(odom_check.summary, "mode=odom_stream stream=true valid=false code=odom_stale msg=odom stale")

    def test_warns_but_does_not_block_when_odometry_gate_disabled(self):
        mgr, get_param, fake_now = self._build_manager(
            odometry_msg=self._odometry_msg(
                valid=False,
                code="odom_frame_missing",
                message="odom frame missing",
            ),
            require_odometry=False,
            slam_state_ts=0.0,
        )
        with mock.patch("coverage_task_manager.task_manager.rospy.get_param", side_effect=get_param), mock.patch(
            "coverage_task_manager.task_manager.rospy.Time.now", return_value=fake_now
        ), mock.patch("coverage_task_manager.task_manager.time.time", return_value=self.now):
            readiness = mgr._build_system_readiness(task_id=0, refresh_map_identity=False)

        self.assertTrue(readiness.overall_ready)
        self.assertTrue(readiness.can_start_task)
        self.assertIn(
            "odometry not ready: code=odom_frame_missing message=odom frame missing",
            list(readiness.warnings),
        )
        odom_check = next(item for item in readiness.checks if item.key == "odometry")
        self.assertEqual(odom_check.level, "WARN")
        self.assertEqual(
            odom_check.summary,
            "mode=odom_stream stream=true valid=false code=odom_frame_missing msg=odom frame missing",
        )

    def test_fresh_slam_state_ready_keeps_local_odometry_issue_as_diagnostic_only(self):
        mgr, get_param, fake_now = self._build_manager(
            odometry_msg=self._odometry_msg(
                valid=False,
                code="odom_stale",
                message="odom stale",
            ),
            require_odometry=True,
            slam_task_ready=True,
        )
        with mock.patch("coverage_task_manager.task_manager.rospy.get_param", side_effect=get_param), mock.patch(
            "coverage_task_manager.task_manager.rospy.Time.now", return_value=fake_now
        ), mock.patch("coverage_task_manager.task_manager.time.time", return_value=self.now):
            readiness = mgr._build_system_readiness(task_id=0, refresh_map_identity=False)

        self.assertTrue(readiness.overall_ready)
        self.assertTrue(readiness.can_start_task)
        self.assertNotIn(
            "odometry not ready: code=odom_stale message=odom stale",
            list(readiness.blocking_reasons),
        )
        self.assertIn(
            "odometry not ready: code=odom_stale message=odom stale",
            list(readiness.warnings),
        )
        odom_check = next(item for item in readiness.checks if item.key == "odometry")
        self.assertEqual(odom_check.level, "WARN")

    def test_fresh_slam_state_degraded_localization_blocks_start(self):
        mgr, get_param, fake_now = self._build_manager(
            odometry_msg=self._odometry_msg(valid=True),
            localization_state="localized",
            localization_valid=True,
            slam_task_ready=False,
            slam_blocking_reason="runtime localization not ready: state=degraded valid=false; relocalize before start/resume",
            slam_localization_state="degraded",
            slam_localization_valid=False,
        )
        with mock.patch("coverage_task_manager.task_manager.rospy.get_param", side_effect=get_param), mock.patch(
            "coverage_task_manager.task_manager.rospy.Time.now", return_value=fake_now
        ), mock.patch("coverage_task_manager.task_manager.time.time", return_value=self.now):
            readiness = mgr._build_system_readiness(task_id=0, refresh_map_identity=False)

        self.assertFalse(readiness.overall_ready)
        self.assertFalse(readiness.can_start_task)
        self.assertIn(
            "runtime localization not ready: state=degraded valid=false; relocalize before start/resume",
            list(readiness.blocking_reasons),
        )
        localization_check = next(item for item in readiness.checks if item.key == "localization")
        self.assertEqual(localization_check.level, "WARN")
        self.assertEqual(localization_check.summary, "state=degraded valid=false")

    def test_stale_slam_state_falls_back_to_local_odometry_blocker(self):
        mgr, get_param, fake_now = self._build_manager(
            odometry_msg=self._odometry_msg(
                valid=False,
                code="odom_stale",
                message="odom stale",
            ),
            require_odometry=True,
            slam_task_ready=True,
            slam_state_ts=self.now - 30.0,
        )
        with mock.patch("coverage_task_manager.task_manager.rospy.get_param", side_effect=get_param), mock.patch(
            "coverage_task_manager.task_manager.rospy.Time.now", return_value=fake_now
        ), mock.patch("coverage_task_manager.task_manager.time.time", return_value=self.now):
            readiness = mgr._build_system_readiness(task_id=0, refresh_map_identity=False)

        self.assertFalse(readiness.overall_ready)
        self.assertFalse(readiness.can_start_task)
        self.assertIn("slam state stale", list(readiness.warnings))
        self.assertIn(
            "odometry not ready: code=odom_stale message=odom stale",
            list(readiness.blocking_reasons),
        )
        odom_check = next(item for item in readiness.checks if item.key == "odometry")
        self.assertEqual(odom_check.level, "ERROR")

    def test_blocks_when_fresh_slam_state_reports_task_not_ready(self):
        mgr, get_param, fake_now = self._build_manager(
            odometry_msg=self._odometry_msg(valid=True),
            require_odometry=True,
            slam_task_ready=False,
            slam_blocking_reason="slam job running: job_id=job_1 op=relocalize phase=localizing",
            slam_busy=True,
        )
        with mock.patch("coverage_task_manager.task_manager.rospy.get_param", side_effect=get_param), mock.patch(
            "coverage_task_manager.task_manager.rospy.Time.now", return_value=fake_now
        ), mock.patch("coverage_task_manager.task_manager.time.time", return_value=self.now):
            readiness = mgr._build_system_readiness(task_id=0, refresh_map_identity=False)

        self.assertFalse(readiness.overall_ready)
        self.assertFalse(readiness.can_start_task)
        self.assertIn(
            "slam job running: job_id=job_1 op=relocalize phase=localizing",
            list(readiness.blocking_reasons),
        )
        slam_check = next(item for item in readiness.checks if item.key == "slam_runtime")
        self.assertEqual(slam_check.level, "ERROR")
        self.assertEqual(
            slam_check.summary,
            "workflow=IDLE phase=idle task_ready=false busy=true manual_assist=false",
        )

    def test_manual_assist_blocker_includes_scope_and_recovery_action(self):
        mgr, get_param, fake_now = self._build_manager(
            odometry_msg=self._odometry_msg(valid=True),
            require_odometry=True,
            slam_task_ready=False,
            slam_manual_assist=True,
            slam_localization_state="manual_assist_required",
            slam_localization_valid=False,
        )
        with mock.patch("coverage_task_manager.task_manager.rospy.get_param", side_effect=get_param), mock.patch(
            "coverage_task_manager.task_manager.rospy.Time.now", return_value=fake_now
        ), mock.patch("coverage_task_manager.task_manager.time.time", return_value=self.now):
            readiness = mgr._build_system_readiness(task_id=0, refresh_map_identity=False)

        self.assertFalse(readiness.overall_ready)
        self.assertFalse(readiness.can_start_task)
        self.assertIn(
            "runtime localization not ready: state=manual_assist_required valid=false; "
            "manual assist required for map=site_live_saved_20260412_2122 revision=rev_site_01; "
            "place robot at a known anchor point, align heading, provide initial pose, then retry prepare_for_task",
            list(readiness.blocking_reasons),
        )
        self.assertTrue(readiness.manual_assist_required)
        self.assertEqual(readiness.manual_assist_map_name, "site_live_saved_20260412_2122")
        self.assertEqual(readiness.manual_assist_map_revision_id, "rev_site_01")
        self.assertEqual(readiness.manual_assist_retry_action, "prepare_for_task")
        self.assertEqual(
            readiness.manual_assist_guidance,
            "manual assist required for map=site_live_saved_20260412_2122 revision=rev_site_01; "
            "place robot at a known anchor point, align heading, provide initial pose, then retry prepare_for_task",
        )
        slam_check = next(item for item in readiness.checks if item.key == "slam_runtime")
        self.assertEqual(slam_check.level, "ERROR")
        self.assertEqual(
            slam_check.summary,
            "workflow=IDLE phase=idle task_ready=false busy=false manual_assist=true",
        )

    def test_deduplicates_same_blocker_from_slam_gate_and_platform_gate(self):
        mgr, get_param, fake_now = self._build_manager(
            odometry_msg=self._odometry_msg(valid=True),
            require_odometry=True,
            mission_state="RUNNING",
            public_state="RUNNING",
            slam_task_ready=False,
            slam_blocking_reason="task manager busy: mission=RUNNING phase=IDLE public=RUNNING",
        )
        with mock.patch("coverage_task_manager.task_manager.rospy.get_param", side_effect=get_param), mock.patch(
            "coverage_task_manager.task_manager.rospy.Time.now", return_value=fake_now
        ), mock.patch("coverage_task_manager.task_manager.time.time", return_value=self.now):
            readiness = mgr._build_system_readiness(task_id=0, refresh_map_identity=False)

        self.assertEqual(
            list(readiness.blocking_reasons).count(
                "task manager busy: mission=RUNNING phase=IDLE public=RUNNING"
            ),
            1,
        )

    def test_task_specific_readiness_reports_selected_vs_task_map_mismatch(self):
        mgr, get_param, fake_now = self._build_manager(
            odometry_msg=self._odometry_msg(valid=True),
            require_odometry=True,
        )
        mgr._resolve_job_record = lambda _task_id: SimpleNamespace(
            enabled=True,
            job_name="demo_task",
            map_name="task_map",
            zone_id="zone_demo",
            plan_profile_name="cover_standard",
        )

        with mock.patch("coverage_task_manager.task_manager.rospy.get_param", side_effect=get_param), mock.patch(
            "coverage_task_manager.task_manager.rospy.Time.now", return_value=fake_now
        ), mock.patch("coverage_task_manager.task_manager.time.time", return_value=self.now):
            readiness = mgr._build_system_readiness(task_id=7, refresh_map_identity=False)

        self.assertFalse(readiness.overall_ready)
        self.assertIn(
            "active selected map site_live_saved_20260412_2122 != requested map task_map",
            list(readiness.blocking_reasons),
        )

    def test_task_specific_readiness_requires_active_revision_for_revision_bound_task(self):
        mgr, get_param, fake_now = self._build_manager(
            odometry_msg=self._odometry_msg(valid=True),
            require_odometry=True,
        )
        mgr._get_selected_active_map = lambda: {
            "map_name": "site_live_saved_20260412_2122",
            "map_id": "map_7d277f7d",
            "map_md5": "7d277f7d8bd88fb3566cfcb146e2d653",
        }
        mgr._resolve_job_record = lambda _task_id: SimpleNamespace(
            enabled=True,
            job_name="demo_task",
            map_name="site_live_saved_20260412_2122",
            map_revision_id="rev_task_02",
            zone_id="zone_demo",
            plan_profile_name="cover_standard",
        )

        with mock.patch("coverage_task_manager.task_manager.rospy.get_param", side_effect=get_param), mock.patch(
            "coverage_task_manager.task_manager.rospy.Time.now", return_value=fake_now
        ), mock.patch("coverage_task_manager.task_manager.time.time", return_value=self.now):
            readiness = mgr._build_system_readiness(task_id=7, refresh_map_identity=False)

        self.assertFalse(readiness.overall_ready)
        self.assertIn(
            "active selected map revision unavailable for requested map revision rev_task_02",
            list(readiness.blocking_reasons),
        )

    def test_get_system_readiness_app_returns_app_payload(self):
        mgr, get_param, fake_now = self._build_manager(
            odometry_msg=self._odometry_msg(valid=True),
            require_odometry=True,
        )
        with mock.patch("coverage_task_manager.task_manager.rospy.get_param", side_effect=get_param), mock.patch(
            "coverage_task_manager.task_manager.rospy.Time.now", return_value=fake_now
        ), mock.patch("coverage_task_manager.task_manager.time.time", return_value=self.now):
            resp = mgr._on_get_system_readiness_app(
                SimpleNamespace(task_id=0, refresh_map_identity=False)
            )

        self.assertTrue(resp.success)
        self.assertTrue(resp.readiness.overall_ready)
        self.assertTrue(resp.readiness.can_start_task)


if __name__ == "__main__":
    unittest.main()

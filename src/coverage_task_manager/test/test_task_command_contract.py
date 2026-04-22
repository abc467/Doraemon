import threading
import unittest
from unittest import mock

from std_msgs.msg import String

from coverage_task_manager.task_manager import TaskManager


class TaskCommandContractTest(unittest.TestCase):
    def _manager(self):
        mgr = TaskManager.__new__(TaskManager)
        mgr._lock = threading.Lock()
        mgr._active_zone = "zone_alpha"
        mgr._active_run_id = "run_alpha"
        mgr._active_job_id = ""
        mgr._active_schedule_id = ""
        mgr._phase = "IDLE"
        mgr._mission_state = "IDLE"
        mgr._public_state = "IDLE"
        mgr._emit_events = []
        mgr._exec_cmds = []
        mgr._faults = []
        mgr._charge_faults = []
        mgr._published_states = []
        mgr._sched_done = []
        mgr._health_clear_calls = []
        mgr._mission_updates = []
        mgr._reset_job_runner_calls = []
        mgr._intent_pushes = []
        mgr._emit = lambda msg: mgr._emit_events.append(str(msg))
        mgr._send_exec_cmd = lambda cmd: mgr._exec_cmds.append(str(cmd))
        mgr._enter_blocking_fault = lambda public_state, reason: mgr._faults.append((str(public_state), str(reason)))
        mgr._enter_charge_fault = lambda public_state, reason, manual=False: mgr._charge_faults.append(
            (str(public_state), str(reason), bool(manual))
        )
        mgr._publish_state = lambda state: mgr._published_states.append(str(state))
        mgr._sched_store_mark_done = lambda sid, status: mgr._sched_done.append((str(sid), str(status)))
        mgr._clear_health_auto_recover = lambda reset_count=False: mgr._health_clear_calls.append(bool(reset_count))
        mgr._mission_update_state = lambda run_id, state, reason="": mgr._mission_updates.append(
            (str(run_id), str(state), str(reason))
        )
        mgr._reset_job_runner = lambda: mgr._reset_job_runner_calls.append(True)
        mgr._push_task_intent_to_executor = lambda: mgr._intent_pushes.append(True)
        mgr._task_busy = lambda: False
        mgr._is_mission_running = lambda: False
        mgr._task_return_to_dock_on_finish = False
        mgr._persist_now = lambda: None
        mgr._persist_if_changed = lambda force=False: None
        mgr._dock_supply_enable = False
        mgr._dock_supply_managed_undocking = lambda: False
        mgr._is_dock_supply_owner_phase = lambda: False
        mgr._dock_supply_state = "IDLE"
        mgr._dock_supply_cancel = lambda: None
        mgr._charge_clears = []
        mgr._clear_charge_monitor = lambda: mgr._charge_clears.append(True)
        mgr._reset_dock_retry_state = lambda: None
        mgr._dock_nav_started_ts = 0.0
        mgr._undock_nav_started_ts = 0.0
        mgr._repeat_cycle_context = lambda: "job=demo run=run_alpha"
        mgr._repeat_after_charge_enabled = lambda: False
        mgr._dock_supply_state = "IDLE"
        mgr._is_recoverable_dock_supply_failure = lambda state: False
        mgr._retry_dock_from_stage1 = lambda manual=False, reason="": False
        mgr.charge_timeout_s = 999.0
        mgr.charge_battery_stale_timeout_s = 999.0
        mgr.resume_soc = 0.8
        mgr._charge_started_ts = 0.0
        mgr._charge_last_soc = None
        mgr._charge_last_fresh_ts = 0.0
        mgr._executor_state = "IDLE"
        mgr._get_exec_state = lambda: str(mgr._executor_state)
        mgr.nav = type("Nav", (), {"cancel_all": lambda _self: None})()
        return mgr

    def test_send_exec_start_cmd_uses_canonical_fields(self):
        mgr = self._manager()

        ok = mgr._send_exec_start_cmd(zone_id="zone_demo", run_id="run_123")

        self.assertTrue(ok)
        self.assertEqual(mgr._exec_cmds, ["start zone_id=zone_demo run_id=run_123"])

    def test_set_phase_and_publish_defaults_public_state_to_phase(self):
        mgr = self._manager()

        mgr._set_phase_and_publish("AUTO_RELOCALIZING")

        self.assertEqual(mgr._phase, "AUTO_RELOCALIZING")
        self.assertEqual(mgr._published_states, ["AUTO_RELOCALIZING"])

    def test_return_manual_sequence_to_idle_uses_manual_public_state(self):
        mgr = self._manager()
        mgr._mission_state = "PAUSED"

        mgr._return_manual_sequence_to_idle()

        self.assertEqual(mgr._phase, "IDLE")
        self.assertEqual(mgr._published_states, ["PAUSED"])

    def test_emit_supply_command_failure_uses_failed_event_name(self):
        mgr = self._manager()

        mgr._emit_supply_command_failure("start", "rpc timeout")

        self.assertEqual(mgr._emit_events, ["SUPPLY_START_FAILED:rpc timeout"])

    def test_emit_dock_goal_event_uses_canonical_name(self):
        mgr = self._manager()

        mgr._emit_dock_goal_event(manual=False, x=1.0, y=2.0, yaw=3.0)

        self.assertEqual(mgr._emit_events, ["DOCK_GOAL:manual=False goal=(1.00,2.00,3.00)"])

    def test_emit_dock_stage1_goal_event_uses_canonical_name(self):
        mgr = self._manager()

        mgr._emit_dock_stage1_goal_event(manual=True, x=1.0, y=2.0, yaw=3.0)

        self.assertEqual(mgr._emit_events, ["DOCK_GOAL_STAGE1:manual=True goal=(1.00,2.00,3.00)"])

    def test_emit_dock_stage1_goal_event_retry_uses_canonical_name(self):
        mgr = self._manager()

        mgr._emit_dock_stage1_goal_event(
            manual=False,
            x=1.0,
            y=2.0,
            yaw=3.0,
            retry_attempt=1,
            retry_total=3,
        )

        self.assertEqual(
            mgr._emit_events,
            ["DOCK_GOAL_STAGE1_RETRY:attempt=1/3 manual=False goal=(1.00,2.00,3.00)"],
        )

    def test_emit_dock_stage2_goal_event_uses_canonical_name(self):
        mgr = self._manager()

        mgr._emit_dock_stage2_goal_event(1.0, 2.0, 3.0, "dock_controller")

        self.assertEqual(
            mgr._emit_events,
            ["DOCK_GOAL_STAGE2:goal=(1.00,2.00,3.00) controller=dock_controller"],
        )

    def test_emit_dock_retry_goal_event_uses_canonical_name(self):
        mgr = self._manager()

        mgr._emit_dock_retry_goal_event(1.0, 2.0, 3.0, retry_attempt=2, retry_total=4)

        self.assertEqual(
            mgr._emit_events,
            ["DOCK_GOAL_RETRY:attempt=2/4 goal=(1.00,2.00,3.00)"],
        )

    def test_emit_undock_goal_event_uses_canonical_name(self):
        mgr = self._manager()

        mgr._emit_undock_goal_event(1.0, 2.0, 3.0)

        self.assertEqual(mgr._emit_events, ["UNDOCK_GOAL:goal=(1.00,2.00,3.00)"])

    def test_emit_undock_exit_requested_uses_canonical_name(self):
        mgr = self._manager()

        mgr._emit_undock_exit_requested()

        self.assertEqual(mgr._emit_events, ["UNDOCK_EXIT_REQUESTED"])

    def test_emit_dock_succeeded_uses_canonical_name(self):
        mgr = self._manager()

        mgr._emit_dock_succeeded()

        self.assertEqual(mgr._emit_events, ["DOCK_SUCCEEDED"])

    def test_emit_dock_stage1_succeeded_uses_canonical_name(self):
        mgr = self._manager()

        mgr._emit_dock_stage1_succeeded()

        self.assertEqual(mgr._emit_events, ["DOCK_STAGE1_SUCCEEDED"])

    def test_emit_supply_terminal_event_uses_canonical_names(self):
        mgr = self._manager()

        mgr._emit_supply_terminal_event("DONE")
        mgr._emit_supply_terminal_event("CANCELED")
        mgr._emit_supply_terminal_event("FAILED_NO_DOCK_POSE")

        self.assertEqual(
            mgr._emit_events,
            [
                "SUPPLY_SUCCEEDED",
                "SUPPLY_CANCELED",
                "SUPPLY_FAILED:state=FAILED_NO_DOCK_POSE",
            ],
        )

    def test_emit_undock_terminal_event_uses_canonical_names(self):
        mgr = self._manager()

        mgr._emit_undock_terminal_event("CANCELED")
        mgr._emit_undock_terminal_event("FAILED_BACKING")

        self.assertEqual(
            mgr._emit_events,
            [
                "UNDOCK_CANCELED",
                "UNDOCK_FAILED:state=FAILED_BACKING",
            ],
        )

    def test_emit_undock_nav_failed_uses_canonical_name(self):
        mgr = self._manager()

        mgr._emit_undock_nav_failed("ABORTED")

        self.assertEqual(mgr._emit_events, ["UNDOCK_FAILED:nav_state=ABORTED"])

    def test_emit_dock_nav_failed_uses_canonical_name(self):
        mgr = self._manager()

        mgr._emit_dock_nav_failed(stage="predock_stage1_nav_failed", nav_state="ABORTED")

        self.assertEqual(
            mgr._emit_events,
            ["DOCK_FAILED:stage=predock_stage1_nav_failed nav_state=ABORTED"],
        )

    def test_emit_battery_low_event_uses_canonical_name(self):
        mgr = self._manager()

        mgr._emit_battery_low_event(0.1234)

        self.assertEqual(mgr._emit_events, ["BATTERY_LOW:soc=0.123"])

    def test_emit_battery_rearmed_event_uses_canonical_name(self):
        mgr = self._manager()

        mgr._emit_battery_rearmed_event(0.9876)

        self.assertEqual(mgr._emit_events, ["BATTERY_REARMED:soc=0.988"])

    def test_emit_supply_ready_to_exit_event_uses_canonical_name(self):
        mgr = self._manager()

        mgr._emit_supply_ready_to_exit_event(repeat_enabled=True)

        self.assertEqual(
            mgr._emit_events,
            ["SUPPLY_STATE_READY_TO_EXIT:job=demo run=run_alpha repeat_enabled=1"],
        )

    def test_emit_auto_recovery_armed_uses_canonical_name(self):
        mgr = self._manager()

        mgr._emit_auto_recovery_armed("AUTO_RELOCALIZING")

        self.assertEqual(
            mgr._emit_events,
            ["AUTO_RECOVERY_ARMED:phase=AUTO_RELOCALIZING job=demo run=run_alpha"],
        )

    def test_emit_auto_resume_confirmed_uses_canonical_name(self):
        mgr = self._manager()

        mgr._emit_auto_resume_confirmed("FOLLOW_PATH")

        self.assertEqual(mgr._emit_events, ["AUTO_RESUME_CONFIRMED:exec_state=FOLLOW_PATH"])

    def test_send_exec_resume_cmd_requires_run_id(self):
        mgr = self._manager()
        mgr._active_run_id = ""

        ok = mgr._send_exec_resume_cmd(context="manual_resume", fault_public_state="ERROR_RESUME_CONTEXT")

        self.assertFalse(ok)
        self.assertEqual(mgr._exec_cmds, [])
        self.assertIn("RESUME_CONTEXT_MISSING:manual_resume", mgr._emit_events)
        self.assertEqual(
            mgr._faults,
            [("ERROR_RESUME_CONTEXT", "missing active_run_id during manual_resume")],
        )

    def test_resume_executor_with_current_intent_pushes_then_resumes(self):
        mgr = self._manager()

        ok = mgr._resume_executor_with_current_intent(run_id="run_123", context="restore_auto_resume")

        self.assertTrue(ok)
        self.assertEqual(mgr._intent_pushes, [True])
        self.assertEqual(mgr._exec_cmds, ["resume run_id=run_123"])

    def test_resume_executor_with_current_intent_requires_run_id_when_fault_state_set(self):
        mgr = self._manager()
        mgr._active_run_id = ""

        ok = mgr._resume_executor_with_current_intent(
            run_id="",
            context="restore_auto_resume",
            fault_public_state="ERROR_RESTORE_RESUME_CONTEXT",
        )

        self.assertFalse(ok)
        self.assertEqual(mgr._intent_pushes, [True])
        self.assertEqual(mgr._exec_cmds, [])
        self.assertEqual(
            mgr._faults,
            [("ERROR_RESTORE_RESUME_CONTEXT", "missing active_run_id during restore_auto_resume")],
        )

    def test_handle_cmd_start_accepts_task_id_key_only(self):
        mgr = self._manager()
        started = []
        mgr._start_task_by_id = lambda task_id, source="CMD_TASK": (started.append((task_id, source)) or True, "")

        mgr._handle_cmd("start task_id=42")

        self.assertEqual(started, [("42", "CMD_TASK")])
        self.assertEqual(mgr._emit_events, [])

    @mock.patch("coverage_task_manager.task_manager.rospy.logwarn_throttle")
    def test_handle_cmd_start_rejects_positional_task_id(self, _logwarn):
        mgr = self._manager()
        mgr._start_task_by_id = lambda task_id, source="CMD_TASK": (True, "")

        mgr._handle_cmd("start 42")

        self.assertEqual(mgr._exec_cmds, [])
        self.assertIn("CMD_REJECT:START_REQUIRES_TASK_ID", mgr._emit_events)

    def test_handle_cmd_start_task_prefix_is_not_treated_as_start(self):
        mgr = self._manager()
        started = []
        mgr._start_task_by_id = lambda task_id, source="CMD_TASK": (started.append((task_id, source)) or True, "")

        mgr._handle_cmd("start_task task_id=42")

        self.assertEqual(started, [])

    def test_handle_cmd_status_uses_canonical_verb(self):
        mgr = self._manager()

        mgr._handle_cmd("status")

        self.assertIn("DUMP_STATE", mgr._emit_events)

    @mock.patch("coverage_task_manager.task_manager.rospy.logwarn")
    def test_handle_cmd_dump_alias_is_not_treated_as_status(self, _logwarn):
        mgr = self._manager()

        mgr._handle_cmd("dump")

        self.assertNotIn("DUMP_STATE", mgr._emit_events)

    def test_handle_cmd_health_uses_canonical_verb(self):
        mgr = self._manager()

        mgr._handle_cmd("health")

        self.assertIn("HEALTH_STATUS", mgr._emit_events)

    @mock.patch("coverage_task_manager.task_manager.rospy.logwarn")
    def test_handle_cmd_health_status_alias_is_not_treated_as_health(self, _logwarn):
        mgr = self._manager()

        mgr._handle_cmd("health_status")

        self.assertNotIn("HEALTH_STATUS", mgr._emit_events)

    def test_handle_cmd_set_return_to_dock_on_finish_accepts_canonical_field(self):
        mgr = self._manager()

        mgr._handle_cmd("set_return_to_dock_on_finish true")

        self.assertTrue(mgr._task_return_to_dock_on_finish)
        self.assertIn("SET_RETURN_TO_DOCK_ON_FINISH:1", mgr._emit_events)

    @mock.patch("coverage_task_manager.task_manager.rospy.logwarn")
    def test_handle_cmd_set_finish_dock_alias_is_not_treated_as_canonical(self, _logwarn):
        mgr = self._manager()

        mgr._handle_cmd("set_finish_dock true")

        self.assertFalse(mgr._task_return_to_dock_on_finish)
        self.assertNotIn("SET_RETURN_TO_DOCK_ON_FINISH:1", mgr._emit_events)

    @mock.patch("coverage_task_manager.task_manager.rospy.logerr")
    def test_start_task_by_id_rejected_returns_idle_state(self, _logerr):
        mgr = self._manager()
        mgr._resolve_job_record = lambda task_id: object()
        mgr._start_job_record = lambda job, source="TASK", schedule_id="", trigger_source="TASK": (
            False,
            "runtime map mismatch",
        )

        ok, msg = mgr._start_task_by_id("42")

        self.assertFalse(ok)
        self.assertEqual(msg, "runtime map mismatch")
        self.assertEqual(mgr._reset_job_runner_calls, [True])
        self.assertEqual(mgr._phase, "IDLE")
        self.assertEqual(mgr._mission_state, "IDLE")
        self.assertEqual(mgr._published_states, ["IDLE"])

    def test_stop_current_task_resets_runner_and_returns_idle_state(self):
        mgr = self._manager()
        mgr._active_schedule_id = "sched_1"

        ok, msg = mgr._stop_current_task()

        self.assertTrue(ok)
        self.assertEqual(msg, "")
        self.assertEqual(mgr._exec_cmds, ["cancel"])
        self.assertEqual(mgr._mission_updates, [("run_alpha", "CANCELED", "")])
        self.assertEqual(mgr._sched_done, [("sched_1", "CANCELED")])
        self.assertEqual(mgr._health_clear_calls, [True])
        self.assertEqual(mgr._reset_job_runner_calls, [True])
        self.assertEqual(mgr._phase, "IDLE")
        self.assertEqual(mgr._mission_state, "IDLE")
        self.assertEqual(mgr._published_states, ["IDLE"])

    def test_finalize_done_returns_runtime_state_to_idle(self):
        mgr = self._manager()
        mgr._active_job_id = "9"
        mgr._active_schedule_id = "sched_1"
        mgr._active_run_id = "run_123"
        mgr._job_loops_total = 1
        mgr._job_loops_done = 1
        mgr._executor_state = "DONE"

        mgr._finalize_terminal("DONE")

        self.assertEqual(mgr._mission_updates, [("run_123", "DONE", "")])
        self.assertEqual(mgr._sched_done, [("sched_1", "DONE")])
        self.assertIn("JOB_DONE:id=9 schedule=sched_1 status=DONE loops=1/1", mgr._emit_events)
        self.assertEqual(mgr._mission_state, "IDLE")
        self.assertEqual(mgr._phase, "IDLE")
        self.assertEqual(mgr._executor_state, "IDLE")
        self.assertEqual(mgr._published_states, ["IDLE"])
        self.assertEqual(mgr._reset_job_runner_calls, [True])

    def test_on_executor_state_persists_when_state_changes(self):
        mgr = self._manager()
        persist_calls = []
        mgr._persist_if_changed = lambda force=False: persist_calls.append(bool(force))

        mgr._on_executor_state(String(data="CANCEL_REQ"))
        mgr._on_executor_state(String(data="CANCEL_REQ"))
        mgr._on_executor_state(String(data="IDLE"))

        self.assertEqual(mgr._executor_state, "IDLE")
        self.assertEqual(persist_calls, [False, False])

    @mock.patch("coverage_task_manager.task_manager.ensure_map_identity", return_value=("", "", False))
    @mock.patch("coverage_task_manager.task_manager.rospy.Time.now")
    @mock.patch("coverage_task_manager.task_manager.time.time", return_value=100.0)
    def test_task_state_timer_does_not_overlay_done_progress_after_runtime_returns_idle(
        self,
        _time_now,
        time_now,
        _ensure_map_identity,
    ):
        mgr = self._manager()
        published = []

        class _Pub:
            def publish(self, msg):
                published.append(msg)

        mgr._task_state_pub = _Pub()
        mgr._battery = type("Battery", (), {"soc": 0.7, "ts": 100.0})()
        mgr.battery_stale_timeout_s = 10.0
        mgr._job_loops_total = 1
        mgr._job_loops_done = 1
        mgr._active_run_loop_index = 0
        mgr._task_plan_profile_name = "plan_profile"
        mgr._task_sys_profile_name = "sys_profile"
        mgr._task_clean_mode = "coverage"
        mgr._health_error_code = ""
        mgr._health_error_msg = ""
        mgr._last_event = ""
        mgr._active_run_id = ""
        mgr._mission_state = "IDLE"
        mgr._phase = "IDLE"
        mgr._public_state = "IDLE"
        mgr._executor_state = "IDLE"
        mgr._last_run_progress = type(
            "RunProgress",
            (),
            {
                "run_id": "run_done",
                "progress_0_1": 1.0,
                "progress_pct": 100.0,
                "state": "DONE",
                "plan_id": "plan_1",
                "map_id": "map_1",
                "map_md5": "md5_1",
                "interlock_active": False,
                "interlock_reason": "",
            },
        )()
        mgr._last_run_progress_ts = 99.5
        time_now.return_value = mock.Mock()

        mgr._on_task_state_timer(None)

        self.assertEqual(len(published), 1)
        self.assertEqual(published[0].executor_state, "IDLE")
        self.assertEqual(published[0].run_id, "")
        self.assertEqual(published[0].progress_pct, 0.0)
        self.assertEqual(published[0].plan_id, "")

    def test_start_auto_resuming_uses_shared_resume_flow(self):
        mgr = self._manager()

        ok = mgr._start_auto_resuming(
            context="auto_undock_resume",
            missing_reason="missing active_run_id during auto undock resume",
        )

        self.assertTrue(ok)
        self.assertEqual(mgr._phase, "AUTO_RESUMING")
        self.assertEqual(mgr._published_states, ["AUTO_RESUMING"])
        self.assertEqual(mgr._exec_cmds, ["resume run_id=run_alpha"])
        self.assertEqual(mgr._faults, [])

    def test_start_auto_resuming_without_run_id_enters_fault(self):
        mgr = self._manager()
        mgr._active_run_id = ""

        ok = mgr._start_auto_resuming(
            context="auto_undock_resume",
            missing_reason="missing active_run_id during auto undock resume",
        )

        self.assertFalse(ok)
        self.assertEqual(mgr._exec_cmds, [])
        self.assertEqual(
            mgr._faults,
            [("ERROR_AUTO_RESUME_CONTEXT", "missing active_run_id during auto undock resume")],
        )

    @mock.patch("coverage_task_manager.task_manager.rospy.loginfo")
    def test_arm_auto_relocalizing_sets_phase_and_emits(self, _loginfo):
        mgr = self._manager()

        mgr._arm_auto_relocalizing()

        self.assertEqual(mgr._phase, "AUTO_RELOCALIZING")
        self.assertEqual(mgr._published_states, ["AUTO_RELOCALIZING"])
        self.assertIn("AUTO_RECOVERY_ARMED:phase=AUTO_RELOCALIZING job=demo run=run_alpha", mgr._emit_events)

    @mock.patch("coverage_task_manager.task_manager.rospy.loginfo")
    def test_arm_auto_redispatching_sets_phase_and_emits(self, _loginfo):
        mgr = self._manager()

        mgr._arm_auto_redispatching()

        self.assertEqual(mgr._phase, "AUTO_REDISPATCHING")
        self.assertEqual(mgr._published_states, ["AUTO_REDISPATCHING"])
        self.assertIn("AUTO_RECOVERY_ARMED:phase=AUTO_REDISPATCHING job=demo run=run_alpha", mgr._emit_events)

    def test_complete_auto_resuming_if_executor_running_returns_to_running(self):
        mgr = self._manager()
        mgr._phase = "AUTO_RESUMING"
        mgr._executor_state = "FOLLOW_PATH"

        ok = mgr._complete_auto_resuming_if_executor_running()

        self.assertTrue(ok)
        self.assertEqual(mgr._phase, "IDLE")
        self.assertEqual(mgr._published_states, ["RUNNING"])
        self.assertIn("AUTO_RESUME_CONFIRMED:exec_state=FOLLOW_PATH", mgr._emit_events)

    def test_handle_auto_supply_done_without_repeat_starts_auto_resuming(self):
        mgr = self._manager()
        mgr._phase = "AUTO_SUPPLY"

        mgr._handle_auto_supply_done()

        self.assertEqual(mgr._phase, "AUTO_RESUMING")
        self.assertEqual(mgr._published_states, ["AUTO_RESUMING"])
        self.assertEqual(mgr._exec_cmds, ["resume run_id=run_alpha"])

    def test_handle_undock_success_without_run_id_arms_redispatching_when_repeat_enabled(self):
        mgr = self._manager()
        mgr._phase = "AUTO_UNDOCKING"
        mgr._active_run_id = ""
        mgr._undock_nav_started_ts = 42.0
        mgr._repeat_after_charge_enabled = lambda: True

        mgr._handle_undock_success()

        self.assertEqual(mgr._undock_nav_started_ts, 0.0)
        self.assertEqual(mgr._phase, "AUTO_REDISPATCHING")
        self.assertEqual(mgr._published_states, ["AUTO_REDISPATCHING"])
        self.assertIn("UNDOCK_SUCCEEDED", mgr._emit_events)
        self.assertIn("AUTO_RECOVERY_ARMED:phase=AUTO_REDISPATCHING job=demo run=run_alpha", mgr._emit_events)
        self.assertEqual(mgr._exec_cmds, [])

    def test_handle_dock_supply_phase_returns_continue_when_retry_started(self):
        mgr = self._manager()
        mgr._phase = "AUTO_SUPPLY"
        mgr._dock_supply_state = "FAILED"
        mgr._is_dock_supply_owner_phase = lambda: True
        mgr._is_recoverable_dock_supply_failure = lambda state: state == "FAILED"
        retry_calls = []
        mgr._retry_dock_from_stage1 = lambda manual=False, reason="": (
            retry_calls.append((bool(manual), str(reason))) or True
        )

        should_continue = mgr._handle_dock_supply_phase()

        self.assertTrue(should_continue)
        self.assertEqual(retry_calls, [(False, "failed")])
        self.assertEqual(mgr._faults, [])

    @mock.patch("coverage_task_manager.task_manager.time.time", return_value=100.0)
    def test_handle_task_side_charge_phase_manual_charge_reaches_threshold_returns_idle(self, _time_now):
        mgr = self._manager()
        mgr._phase = "MANUAL_CHARGING"

        mgr._handle_task_side_charge_phase(soc=0.9, fresh=True)

        self.assertEqual(mgr._charge_clears, [True])
        self.assertEqual(mgr._phase, "IDLE")
        self.assertEqual(mgr._published_states, ["IDLE"])
        self.assertIn("CHARGED:soc=0.900", mgr._emit_events)

    def test_begin_supply_or_charge_after_dock_uses_canonical_supply_start_reason(self):
        mgr = self._manager()
        mgr._dock_supply_enable = True
        mgr._dock_supply_set_defer_exit = lambda enabled: False
        mgr._dock_supply_start = lambda: True

        mgr._begin_supply_or_charge_after_dock(manual=False, soc=0.5, fresh=True)

        self.assertEqual(
            mgr._charge_faults,
            [("ERROR_SUPPLY_START", "supply_defer_exit_failed", False)],
        )

    def test_start_undock_only_uses_canonical_supply_cancel_timeout_reason(self):
        mgr = self._manager()
        mgr._dock_supply_enable = True
        mgr._dock_supply_state = "RUNNING"
        mgr._is_dock_supply_owner_phase = lambda: True
        mgr._wait = lambda timeout_s, cond_fn, sleep_s=0.1: False

        ok = mgr._start_undock_only()

        self.assertFalse(ok)
        self.assertEqual(
            mgr._charge_faults,
            [("ERROR_UNDOCK_PREP", "supply_cancel_timeout", False)],
        )


if __name__ == "__main__":
    unittest.main()

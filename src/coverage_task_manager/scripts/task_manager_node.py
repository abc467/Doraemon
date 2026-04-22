#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import rospy

from coverage_task_manager.task_manager import TaskManager, _parse_xyyaw
from coverage_task_manager.map_identity import ensure_map_identity
from coverage_planner.ops_store.store import OperationsStore, seed_schedule_jobs_from_specs, seed_sys_profiles_from_param


def main():
    rospy.init_node("coverage_task_manager", anonymous=False)

    frame_id = rospy.get_param("~frame_id", "map")
    zone_id_default = rospy.get_param("~zone_id_default", "zone_demo")

    # Task intent defaults
    # - plan_profile: selects which coverage plan to load (DB key, plans.plan_profile_name)
    # - sys_profile : selects which nav/controller + actuator param set to use (heavy/standard/eco)
    default_plan_profile_name = rospy.get_param("~default_plan_profile_name", "cover_standard")
    default_sys_profile_name = rospy.get_param("~default_sys_profile_name", "standard")

    default_clean_mode = rospy.get_param("~default_clean_mode", "scrub")
    default_return_to_dock_on_finish = rospy.get_param("~default_return_to_dock_on_finish", False)

    # profile catalog for sys_profile -> (MBF controller, actuator profile, default clean_mode)
    raw_mode_profiles = rospy.get_param("~mode_profiles", {})
    nav_profile_apply_enable = rospy.get_param("~nav_profile_apply_enable", False)

    # policy profiles (optional)
    dock_sys_profile_name = rospy.get_param("~dock_sys_profile_name", "")
    inspect_sys_profile_name = rospy.get_param("~inspect_sys_profile_name", "eco")

    # persistence (same DB as executor by default)
    plan_db_path = rospy.get_param("~plan_db_path", "/data/coverage/planning.db")
    ops_db_path = rospy.get_param("~ops_db_path", "/data/coverage/operations.db")
    robot_id = rospy.get_param("~robot_id", "local_robot")
    require_managed_map_asset = rospy.get_param("~require_managed_map_asset", True)
    require_runtime_localized_before_start = rospy.get_param(
        "~require_runtime_localized_before_start", False
    )
    require_runtime_map_match = rospy.get_param("~require_runtime_map_match", False)
    runtime_localization_state_param = rospy.get_param(
        "~runtime_localization_state_param", "/cartographer/runtime/localization_state"
    )
    runtime_localization_valid_param = rospy.get_param(
        "~runtime_localization_valid_param", "/cartographer/runtime/localization_valid"
    )
    require_odometry_healthy_before_start = rospy.get_param(
        "~require_odometry_healthy_before_start", True
    )
    odometry_state_topic = rospy.get_param("~odometry_state_topic", "/clean_robot_server/odometry_state")
    odometry_state_stale_timeout_s = rospy.get_param("~odometry_state_stale_timeout_s", 5.0)
    readiness_publish_hz = rospy.get_param("~readiness_publish_hz", 1.0)
    app_readiness_service_name = rospy.get_param(
        "~app_readiness_service_name", "/coverage_task_manager/app/get_system_readiness"
    )
    app_readiness_contract_param_ns = rospy.get_param(
        "~app_readiness_contract_param_ns",
        "/coverage_task_manager/contracts/app/get_system_readiness",
    )
    runtime_map_topic = rospy.get_param("~runtime_map_topic", "/map")
    combined_status_topic = rospy.get_param("~combined_status_topic", "/combined_status")
    combined_status_stale_timeout_s = rospy.get_param("~combined_status_stale_timeout_s", 5.0)
    station_status_topic = rospy.get_param("~station_status_topic", "/station_status")
    station_status_stale_timeout_s = rospy.get_param("~station_status_stale_timeout_s", 5.0)
    mcore_connected_topic = rospy.get_param("~mcore_connected_topic", "/mcore_tcp_bridge/connected")
    station_connected_topic = rospy.get_param("~station_connected_topic", "/station_tcp_bridge/connected")
    connected_stale_timeout_s = rospy.get_param("~connected_stale_timeout_s", 5.0)
    app_exe_task_service_name = rospy.get_param(
        "~app_exe_task_service_name", "/coverage_task_manager/app/exe_task_server"
    )
    app_exe_task_contract_param_ns = rospy.get_param(
        "~app_exe_task_contract_param_ns",
        "/coverage_task_manager/contracts/app/exe_task_server",
    )
    # Map identity injection (system-wide). Does not block if /map not available.
    auto_map_identity_enable = rospy.get_param("~auto_map_identity_enable", True)
    map_topic = rospy.get_param("~map_topic", "/map")
    map_identity_timeout_s = rospy.get_param("~map_identity_timeout_s", 2.0)
    if auto_map_identity_enable:
        mid, mmd5, ok = ensure_map_identity(map_topic=str(map_topic), timeout_s=float(map_identity_timeout_s))
        if ok:
            rospy.loginfo("[TASK_NODE] map identity ready: map_id=%s map_md5=%s", mid, mmd5)
        else:
            rospy.logwarn("[TASK_NODE] map identity not available yet. map_topic=%s", str(map_topic))
    task_persist_enable = rospy.get_param("~task_persist_enable", True)
    task_restore_enable = rospy.get_param("~task_restore_enable", False)
    task_state_max_age_s = rospy.get_param("~task_state_max_age_s", 7 * 24 * 3600)

    # executor interface; String cmd stays only for internal/dev/debug control
    executor_cmd_topic = rospy.get_param("~executor_cmd_topic", "/coverage_executor/cmd")
    executor_state_topic = rospy.get_param("~executor_state_topic", "/coverage_executor/state")
    executor_progress_topic = rospy.get_param("~executor_progress_topic", "/coverage_executor/run_progress")

    # sys_profile switching safety
    sys_profile_switch_policy = rospy.get_param("~sys_profile_switch_policy", "defer")
    sys_profile_switch_wait_s = rospy.get_param("~sys_profile_switch_wait_s", 20.0)

    # battery
    battery_topic = rospy.get_param("~battery_topic", "/battery_state")
    battery_stale_timeout_s = rospy.get_param("~battery_stale_timeout_s", 5.0)

    auto_charge_enable = rospy.get_param("~auto_charge_enable", True)
    trigger_when_idle = rospy.get_param("~trigger_when_idle", False)
    low_soc = rospy.get_param("~low_soc", 0.20)
    resume_soc = rospy.get_param("~resume_soc", 0.80)
    rearm_soc = rospy.get_param("~rearm_soc", 0.30)

    dock_xyyaw_raw = rospy.get_param("~dock_xyyaw", [0.0, 0.0, 0.0])
    dock_xyyaw = _parse_xyyaw(dock_xyyaw_raw, default=(0.0, 0.0, 0.0))
    dock_stage1_xyyaw_raw = rospy.get_param("~dock_stage1_xyyaw", dock_xyyaw_raw)
    dock_stage1_xyyaw = _parse_xyyaw(dock_stage1_xyyaw_raw, default=dock_xyyaw)
    dock_two_stage_enable = rospy.get_param("~dock_two_stage_enable", False)
    dock_stage2_controller = rospy.get_param("~dock_stage2_controller", "MyPlanner")
    dock_retry_limit = rospy.get_param("~dock_retry_limit", 2)
    undock_forward_m = rospy.get_param("~undock_forward_m", 0.6)
    dock_timeout_s = rospy.get_param("~dock_timeout_s", 600.0)
    wait_executor_paused_s = rospy.get_param("~wait_executor_paused_s", 20.0)
    charge_timeout_s = rospy.get_param("~charge_timeout_s", 14400.0)
    charge_battery_stale_timeout_s = rospy.get_param("~charge_battery_stale_timeout_s", 300.0)

    # Real-robot dock supply manager integration
    dock_supply_enable = rospy.get_param("~dock_supply_enable", False)
    dock_supply_start_service = rospy.get_param("~dock_supply_start_service", "/dock_supply/start")
    dock_supply_cancel_service = rospy.get_param("~dock_supply_cancel_service", "/dock_supply/cancel")
    dock_supply_state_topic = rospy.get_param("~dock_supply_state_topic", "/dock_supply/state")
    dock_supply_set_defer_exit_service = rospy.get_param(
        "~dock_supply_set_defer_exit_service", "/dock_supply/set_defer_exit"
    )
    dock_supply_exit_service = rospy.get_param("~dock_supply_exit_service", "/dock_supply/exit")
    app_restart_localization_service = rospy.get_param(
        "~app_restart_localization_service", "/cartographer/runtime/app/restart_localization"
    )
    app_slam_submit_command_service = rospy.get_param(
        "~app_slam_submit_command_service", "/clean_robot_server/app/submit_slam_command"
    )
    app_slam_get_job_service = rospy.get_param(
        "~app_slam_get_job_service", "/clean_robot_server/app/get_slam_job"
    )
    restart_localization_timeout_s = rospy.get_param("~restart_localization_timeout_s", 180.0)
    clear_costmaps_service = rospy.get_param("~clear_costmaps_service", "/move_base_flex/clear_costmaps")
    manual_assist_pose_param_ns = rospy.get_param(
        "~manual_assist_pose_param_ns", "/localization/manual_assist_pose"
    )

    # MBF MoveBase client (for docking)
    mbf_move_base_action = rospy.get_param("~mbf_move_base_action", "/move_base_flex/move_base")
    mbf_exe_path_action = rospy.get_param("~mbf_exe_path_action", "/move_base_flex/exe_path")
    mbf_planner = rospy.get_param("~mbf_planner", "")
    mbf_controller = rospy.get_param("~mbf_controller", "")
    mbf_recovery = rospy.get_param("~mbf_recovery", "")

    # Health monitor / watchdog
    health_enable = rospy.get_param("~health_enable", True)
    health_check_hz = rospy.get_param("~health_check_hz", 1.0)
    base_frame = rospy.get_param("~base_frame", "base_footprint")
    exec_progress_timeout_s = rospy.get_param("~exec_progress_timeout_s", 3.0)
    tf_timeout_s = rospy.get_param("~tf_timeout_s", 0.1)
    tf_max_age_s = rospy.get_param("~tf_max_age_s", 0.6)
    progress_stall_timeout_s = rospy.get_param("~progress_stall_timeout_s", 20.0)
    progress_stall_min_delta = rospy.get_param("~progress_stall_min_delta", 0.002)
    auto_pause_on_health_error = rospy.get_param("~auto_pause_on_health_error", True)
    auto_pause_cmd = rospy.get_param("~auto_pause_cmd", "pause")
    health_auto_recover_enable = rospy.get_param("~health_auto_recover_enable", True)
    health_auto_recover_max = rospy.get_param("~health_auto_recover_max", 2)
    health_auto_resume_delay_s = rospy.get_param("~health_auto_resume_delay_s", 3.0)
    health_auto_recover_codes = rospy.get_param("~health_auto_recover_codes", ["ROBOT_NO_MOTION_STALL", "EXEC_PROGRESS_STALL"])

    # Localization quality health (optional)
    loc_enable = rospy.get_param("~loc_enable", False)
    loc_require = rospy.get_param("~loc_require", False)
    loc_topic = rospy.get_param("~loc_topic", "")
    loc_stale_s = rospy.get_param("~loc_stale_s", 1.0)
    loc_cov_xy_max = rospy.get_param("~loc_cov_xy_max", 0.25)
    loc_cov_yaw_max = rospy.get_param("~loc_cov_yaw_max", 0.30)
    loc_jump_xy_m = rospy.get_param("~loc_jump_xy_m", 1.0)
    loc_jump_yaw_rad = rospy.get_param("~loc_jump_yaw_rad", 1.0)

    # No-motion stall (TF displacement)
    pose_stall_enable = rospy.get_param("~pose_stall_enable", True)
    pose_stall_timeout_s = rospy.get_param("~pose_stall_timeout_s", 15.0)
    pose_stall_min_move_m = rospy.get_param("~pose_stall_min_move_m", 0.05)
    pose_stall_min_yaw_rad = rospy.get_param("~pose_stall_min_yaw_rad", 0.12)

    # Scheduler (recurring jobs)
    scheduler_enable = rospy.get_param("~scheduler_enable", False)
    load_schedules_yaml = rospy.get_param("~load_schedules_yaml", False)
    raw_schedules = rospy.get_param("~schedules", [])
    scheduler_window_s = rospy.get_param("~scheduler_window_s", 30.0)
    scheduler_check_hz = rospy.get_param("~scheduler_check_hz", 1.0)
    scheduler_catch_up_s = rospy.get_param("~scheduler_catch_up_s", 300.0)
    scheduler_retry_backoff_s = rospy.get_param("~scheduler_retry_backoff_s", 30.0)
    scheduler_reload_enable = rospy.get_param("~scheduler_reload_enable", True)
    scheduler_reload_period_s = rospy.get_param("~scheduler_reload_period_s", 5.0)

    ops_store = OperationsStore(str(ops_db_path))
    seed_sys_profiles_from_param(ops_store, raw_mode_profiles)
    if bool(load_schedules_yaml):
        seed_schedule_jobs_from_specs(ops_store, raw_schedules)
        rospy.loginfo(
            "[TASK][SCHED] seeded schedules from ROS param: count=%d",
            len(raw_schedules or []),
        )
    else:
        rospy.loginfo("[TASK][SCHED] YAML schedule seeding disabled; loading schedules from DB only")
    mode_profiles = ops_store.export_mode_profiles_dict()
    schedules = ops_store.list_schedule_specs()

    tm = TaskManager(
        ops_db_path=ops_db_path,
        plan_db_path=plan_db_path,
        robot_id=str(robot_id),
        task_persist_enable=bool(task_persist_enable),
        task_restore_enable=bool(task_restore_enable),
        task_state_max_age_s=float(task_state_max_age_s),
        frame_id=frame_id,
        zone_id_default=zone_id_default,

        default_plan_profile_name=str(default_plan_profile_name),
        default_sys_profile_name=str(default_sys_profile_name),
        default_clean_mode=str(default_clean_mode),
        default_return_to_dock_on_finish=bool(default_return_to_dock_on_finish),

        mode_profiles=mode_profiles,
        nav_profile_apply_enable=bool(nav_profile_apply_enable),
        dock_sys_profile_name=str(dock_sys_profile_name),
        inspect_sys_profile_name=str(inspect_sys_profile_name),

        executor_cmd_topic=executor_cmd_topic,
        executor_state_topic=executor_state_topic,
        executor_progress_topic=executor_progress_topic,
        sys_profile_switch_policy=str(sys_profile_switch_policy),
        sys_profile_switch_wait_s=float(sys_profile_switch_wait_s),
        cmd_topic="~cmd",
        app_exe_task_service_name=str(app_exe_task_service_name),
        app_exe_task_contract_param_ns=str(app_exe_task_contract_param_ns),
        battery_topic=battery_topic,
        battery_stale_timeout_s=float(battery_stale_timeout_s),
        auto_charge_enable=bool(auto_charge_enable),
        trigger_when_idle=bool(trigger_when_idle),
        low_soc=float(low_soc),
        resume_soc=float(resume_soc),
        rearm_soc=float(rearm_soc),
        dock_xyyaw=dock_xyyaw,
        dock_stage1_xyyaw=dock_stage1_xyyaw,
        dock_two_stage_enable=bool(dock_two_stage_enable),
        dock_stage2_controller=str(dock_stage2_controller),
        dock_retry_limit=int(dock_retry_limit),
        undock_forward_m=float(undock_forward_m),
        dock_timeout_s=float(dock_timeout_s),
        wait_executor_paused_s=float(wait_executor_paused_s),
        charge_timeout_s=float(charge_timeout_s),
        charge_battery_stale_timeout_s=float(charge_battery_stale_timeout_s),

        dock_supply_enable=bool(dock_supply_enable),
        dock_supply_start_service=str(dock_supply_start_service),
        dock_supply_cancel_service=str(dock_supply_cancel_service),
        dock_supply_state_topic=str(dock_supply_state_topic),
        dock_supply_set_defer_exit_service=str(dock_supply_set_defer_exit_service),
        dock_supply_exit_service=str(dock_supply_exit_service),
        app_restart_localization_service=str(app_restart_localization_service),
        app_slam_submit_command_service=str(app_slam_submit_command_service),
        app_slam_get_job_service=str(app_slam_get_job_service),
        restart_localization_timeout_s=float(restart_localization_timeout_s),
        clear_costmaps_service=str(clear_costmaps_service),
        manual_assist_pose_param_ns=str(manual_assist_pose_param_ns),
        require_managed_map_asset=bool(require_managed_map_asset),
        require_runtime_localized_before_start=bool(require_runtime_localized_before_start),
        require_runtime_map_match=bool(require_runtime_map_match),
        runtime_localization_state_param=str(runtime_localization_state_param),
        runtime_localization_valid_param=str(runtime_localization_valid_param),
        require_odometry_healthy_before_start=bool(require_odometry_healthy_before_start),
        odometry_state_topic=str(odometry_state_topic),
        odometry_state_stale_timeout_s=float(odometry_state_stale_timeout_s),
        readiness_publish_hz=float(readiness_publish_hz),
        app_readiness_service_name=str(app_readiness_service_name),
        app_readiness_contract_param_ns=str(app_readiness_contract_param_ns),
        runtime_map_topic=str(runtime_map_topic),
        combined_status_topic=str(combined_status_topic),
        combined_status_stale_timeout_s=float(combined_status_stale_timeout_s),
        station_status_topic=str(station_status_topic),
        station_status_stale_timeout_s=float(station_status_stale_timeout_s),
        mcore_connected_topic=str(mcore_connected_topic),
        station_connected_topic=str(station_connected_topic),
        connected_stale_timeout_s=float(connected_stale_timeout_s),
        mbf_move_base_action=mbf_move_base_action,
        mbf_exe_path_action=mbf_exe_path_action,
        mbf_planner=mbf_planner,
        mbf_controller=mbf_controller,
        mbf_recovery=mbf_recovery,

        health_enable=bool(health_enable),
        health_check_hz=float(health_check_hz),
        base_frame=str(base_frame),
        exec_progress_timeout_s=float(exec_progress_timeout_s),
        tf_timeout_s=float(tf_timeout_s),
        tf_max_age_s=float(tf_max_age_s),
        progress_stall_timeout_s=float(progress_stall_timeout_s),
        progress_stall_min_delta=float(progress_stall_min_delta),
        auto_pause_on_health_error=bool(auto_pause_on_health_error),
        auto_pause_cmd=str(auto_pause_cmd),
        health_auto_recover_enable=bool(health_auto_recover_enable),
        health_auto_recover_max=int(health_auto_recover_max),
        health_auto_resume_delay_s=float(health_auto_resume_delay_s),
        health_auto_recover_codes=health_auto_recover_codes,

        loc_enable=bool(loc_enable),
        loc_require=bool(loc_require),
        loc_topic=str(loc_topic),
        loc_stale_s=float(loc_stale_s),
        loc_cov_xy_max=float(loc_cov_xy_max),
        loc_cov_yaw_max=float(loc_cov_yaw_max),
        loc_jump_xy_m=float(loc_jump_xy_m),
        loc_jump_yaw_rad=float(loc_jump_yaw_rad),
        pose_stall_enable=bool(pose_stall_enable),
        pose_stall_timeout_s=float(pose_stall_timeout_s),
        pose_stall_min_move_m=float(pose_stall_min_move_m),
        pose_stall_min_yaw_rad=float(pose_stall_min_yaw_rad),

        scheduler_enable=bool(scheduler_enable),
        schedules=schedules,
        scheduler_window_s=float(scheduler_window_s),
        scheduler_check_hz=float(scheduler_check_hz),
        scheduler_catch_up_s=float(scheduler_catch_up_s),
        scheduler_retry_backoff_s=float(scheduler_retry_backoff_s),
        scheduler_reload_enable=bool(scheduler_reload_enable),
        scheduler_reload_period_s=float(scheduler_reload_period_s),
    )

    tm.spin()


if __name__ == "__main__":
    main()

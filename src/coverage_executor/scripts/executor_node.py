#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import ast
import rospy

from coverage_executor.mbf_adapter import MBFAdapter
from coverage_executor.cleaning_actuator import CleaningActuator
from coverage_executor.fsm import ExecutorFSM
from coverage_executor.auto_charge_manager import AutoChargeManager
from coverage_executor.map_identity import ensure_map_identity
from coverage_planner.ops_store.store import OperationsStore, seed_actuator_profiles_from_param, seed_sys_profiles_from_param


def _parse_xyyaw(val, default=(0.0, 0.0, 0.0)):
    """
    支持:
      - list/tuple: [x,y,yaw]
      - string: "[x,y,yaw]" 或 "(x,y,yaw)"
    """
    if val is None:
        return tuple(default)
    if isinstance(val, (list, tuple)) and len(val) >= 3:
        return (float(val[0]), float(val[1]), float(val[2]))
    if isinstance(val, str):
        s = val.strip()
        try:
            obj = ast.literal_eval(s)
            if isinstance(obj, (list, tuple)) and len(obj) >= 3:
                return (float(obj[0]), float(obj[1]), float(obj[2]))
        except Exception:
            pass
    return tuple(default)


def main():
    rospy.init_node("coverage_executor", anonymous=False)

    plan_db_path = rospy.get_param("~plan_db_path", "/data/coverage/planning.db")
    ops_db_path = rospy.get_param("~ops_db_path", "/data/coverage/operations.db")
    frame_id = rospy.get_param("~frame_id", "map")
    base_frame = rospy.get_param("~base_frame", "base_footprint")
    zone_id = rospy.get_param("~zone_id", "zone_demo")
    default_plan_profile_name = rospy.get_param("~default_plan_profile_name", "cover_standard")
    default_sys_profile_name = rospy.get_param("~default_sys_profile_name", "standard")
    default_clean_mode = rospy.get_param("~default_clean_mode", "scrub")
    raw_mode_profiles = rospy.get_param("~mode_profiles", {})
    raw_actuator_profiles = rospy.get_param("~actuator_profiles", {})
    ops_store = OperationsStore(str(ops_db_path))
    seed_sys_profiles_from_param(ops_store, raw_mode_profiles)
    seed_actuator_profiles_from_param(ops_store, raw_actuator_profiles)
    mode_profiles = ops_store.export_mode_profiles_dict()

    mbf_move_base_action = rospy.get_param("~mbf_move_base_action", "/move_base_flex/move_base")
    mbf_exe_path_action = rospy.get_param("~mbf_exe_path_action", "/move_base_flex/exe_path")

    planner = rospy.get_param("~mbf_planner", "")
    controller = rospy.get_param("~mbf_controller", "")
    recovery = rospy.get_param("~mbf_recovery", "")

    water_off_distance = rospy.get_param("~water_off_distance", 2.0)
    vacuum_delay_s = rospy.get_param("~vacuum_delay_s", 1.5)
    keep_cleaning_on_during_connect = rospy.get_param("~keep_cleaning_on_during_connect", True)

    checkpoint_hz = rospy.get_param("~checkpoint_hz", 1.0)
    pause_hold_hz = rospy.get_param("~pause_hold_hz", 10.0)
    hard_stop_s = rospy.get_param("~hard_stop_s", 0.6)
    connect_skip_dist = rospy.get_param("~connect_skip_dist", 0.35)

    # retry / recovery params
    connect_retry_max = rospy.get_param("~connect_retry_max", 2)
    follow_retry_max = rospy.get_param("~follow_retry_max", 2)
    retry_wait_s = rospy.get_param("~retry_wait_s", 2.0)
    follow_retry_reconnect_on_fail = rospy.get_param("~follow_retry_reconnect_on_fail", True)
    retry_pause_recovery_on_exhausted = rospy.get_param("~retry_pause_recovery_on_exhausted", True)
    retry_clear_costmaps = rospy.get_param("~retry_clear_costmaps", False)
    clear_costmaps_service = rospy.get_param("~clear_costmaps_service", "/move_base_flex/clear_costmaps")

    resume_backtrack_m = rospy.get_param("~resume_backtrack_m", 0.5)
    resume_accept_dist = rospy.get_param("~resume_accept_dist", 1.0)
    resume_finish_thresh_m = rospy.get_param("~resume_finish_thresh_m", 0.3)

    # plan/zone consistency safety
    strict_zone_version_check = rospy.get_param("~strict_zone_version_check", True)

    # map identity safety (map/zone consistency)
    strict_map_check = rospy.get_param("~strict_map_check", True)
    strict_resume_map_check = rospy.get_param("~strict_resume_map_check", True)
    auto_map_identity_enable = rospy.get_param("~auto_map_identity_enable", True)
    map_topic = rospy.get_param("~map_topic", "/map")
    map_identity_timeout_s = rospy.get_param("~map_identity_timeout_s", 2.0)

    if auto_map_identity_enable:
        mid, mmd5, ok = ensure_map_identity(map_topic=str(map_topic), timeout_s=float(map_identity_timeout_s))
        if ok:
            rospy.loginfo("[EXEC_NODE] map identity ready: map_id=%s map_md5=%s", mid, mmd5)
        else:
            rospy.logwarn("[EXEC_NODE] map identity not available yet (may block start if strict_map_check) map_topic=%s", str(map_topic))

    # 关键策略：zone_begin 不启动清洁；清洁只在 FOLLOW 开启
    start_cleaning_on_zone_begin = rospy.get_param("~start_cleaning_on_zone_begin", False)

    auto_start = rospy.get_param("~auto_start", False)
    startup_resume_run_id = str(rospy.get_param("~startup_resume_run_id", "") or "").strip()

    # AI inspection spot cleaning (巡检：AI触发短时间开启执行机构)
    ai_spot_enable = rospy.get_param("~ai_spot_enable", False)
    ai_signal_topic = rospy.get_param("~ai_signal_topic", "/ai/dirt_detected")
    ai_signal_use_string = rospy.get_param("~ai_signal_use_string", False)
    ai_spot_duration_s = rospy.get_param("~ai_spot_duration_s", 120.0)
    ai_spot_mode = rospy.get_param("~ai_spot_mode", "scrub")

    # actuator interlock + speed-limit interface (optional)
    odom_topic = rospy.get_param("~odom_topic", "/odom")
    interlock_enable = rospy.get_param("~interlock_enable", True)
    interlock_max_lin_mps = rospy.get_param("~interlock_max_lin_mps", 1.0)
    interlock_max_ang_rps = rospy.get_param("~interlock_max_ang_rps", 2.0)
    interlock_mask_cleaning = rospy.get_param("~interlock_mask_cleaning", True)
    interlock_mask_water = rospy.get_param("~interlock_mask_water", True)
    interlock_mask_vacuum = rospy.get_param("~interlock_mask_vacuum", False)
    speed_limit_scale_enable = rospy.get_param("~speed_limit_scale_enable", True)
    speed_limit_scale_when_actuating = rospy.get_param("~speed_limit_scale_when_actuating", 1.0)

    # AutoCharge params
    auto_charge_enable = rospy.get_param("~auto_charge_enable", False)
    auto_charge_check_hz = rospy.get_param("~auto_charge_check_hz", 1.0)
    battery_topic = rospy.get_param("~battery_topic", "/battery_state")
    battery_stale_timeout_s = rospy.get_param("~battery_stale_timeout_s", 5.0)

    low_soc = rospy.get_param("~low_soc", 0.2)
    resume_soc = rospy.get_param("~resume_soc", 0.8)
    rearm_soc = rospy.get_param("~rearm_soc", 0.3)

    dock_timeout_s = rospy.get_param("~dock_timeout_s", 600.0)
    charge_timeout_s = rospy.get_param("~charge_timeout_s", 10800.0)
    wait_paused_timeout_s = rospy.get_param("~wait_paused_timeout_s", 20.0)
    trigger_when_idle = rospy.get_param("~trigger_when_idle", False)

    dock_xyyaw_raw = rospy.get_param("~dock_xyyaw", [0.0, 0.0, 0.0])
    dock = _parse_xyyaw(dock_xyyaw_raw, default=(0.0, 0.0, 0.0))

    rospy.loginfo("[EXEC_NODE] raw ~dock_xyyaw=%s -> dock=(%.2f,%.2f,%.2f)", str(dock_xyyaw_raw), dock[0], dock[1], dock[2])
    rospy.loginfo("[EXEC_NODE] raw ~battery_topic=%s  auto_charge_enable=%s", str(battery_topic), str(auto_charge_enable))

    mbf = MBFAdapter(
        move_base_action=mbf_move_base_action,
        exe_path_action=mbf_exe_path_action,
        planner=planner,
        controller=controller,
        recovery=recovery,
        clear_costmaps_service=str(clear_costmaps_service),
    )
    act = CleaningActuator()

    fsm = ExecutorFSM(
        plan_db_path=plan_db_path,
        ops_db_path=ops_db_path,
        mbf=mbf,
        actuator=act,
        frame_id=frame_id,
        base_frame=base_frame,
        zone_id_default=zone_id,
        default_plan_profile_name=str(default_plan_profile_name or "cover_standard"),
        default_sys_profile_name=str(default_sys_profile_name or "standard"),
        default_clean_mode=str(default_clean_mode or ""),
        mode_profiles=mode_profiles,
        vacuum_delay_s=vacuum_delay_s,
        keep_cleaning_on_during_connect=keep_cleaning_on_during_connect,
        checkpoint_hz=checkpoint_hz,
        pause_hold_hz=pause_hold_hz,
        hard_stop_s=hard_stop_s,
        connect_skip_dist=connect_skip_dist,
        resume_backtrack_m=resume_backtrack_m,
        resume_accept_dist=resume_accept_dist,
        resume_finish_thresh_m=resume_finish_thresh_m,
        water_off_distance=water_off_distance,
        start_cleaning_on_zone_begin=start_cleaning_on_zone_begin,

        ai_spot_enable=bool(ai_spot_enable),
        ai_signal_topic=str(ai_signal_topic),
        ai_signal_use_string=bool(ai_signal_use_string),
        ai_spot_duration_s=float(ai_spot_duration_s),
        ai_spot_mode=str(ai_spot_mode),

        strict_zone_version_check=bool(strict_zone_version_check),

        strict_map_check=bool(strict_map_check),
        strict_resume_map_check=bool(strict_resume_map_check),
        auto_map_identity_enable=bool(auto_map_identity_enable),
        map_topic=str(map_topic),
        map_identity_timeout_s=float(map_identity_timeout_s),

        odom_topic=str(odom_topic),
        interlock_enable=bool(interlock_enable),
        interlock_max_lin_mps=float(interlock_max_lin_mps),
        interlock_max_ang_rps=float(interlock_max_ang_rps),
        interlock_mask_cleaning=bool(interlock_mask_cleaning),
        interlock_mask_water=bool(interlock_mask_water),
        interlock_mask_vacuum=bool(interlock_mask_vacuum),
        speed_limit_scale_enable=bool(speed_limit_scale_enable),
        speed_limit_scale_when_actuating=float(speed_limit_scale_when_actuating),

        connect_retry_max=int(connect_retry_max),
        follow_retry_max=int(follow_retry_max),
        retry_wait_s=float(retry_wait_s),
        follow_retry_reconnect_on_fail=bool(follow_retry_reconnect_on_fail),
        retry_pause_recovery_on_exhausted=bool(retry_pause_recovery_on_exhausted),
        retry_clear_costmaps=bool(retry_clear_costmaps),
    )

    # 节点被 kill / roslaunch 退出时：必须停下 + 关清洁 + 硬停
    rospy.on_shutdown(fsm.shutdown)

    # AutoCharge manager（只负责“电量触发 -> 回桩/充电/离桩 -> 触发 resume”）
    if auto_charge_enable:
        _ = AutoChargeManager(
            fsm=fsm,
            mbf=mbf,
            frame_id=frame_id,
            zone_id=zone_id,
            enabled=True,
            check_hz=float(auto_charge_check_hz),
            battery_topic=str(battery_topic),
            battery_stale_timeout_s=float(battery_stale_timeout_s),
            low_soc=float(low_soc),
            resume_soc=float(resume_soc),
            rearm_soc=float(rearm_soc),
            dock_xyyaw=dock,
            dock_timeout_s=float(dock_timeout_s),
            charge_timeout_s=float(charge_timeout_s),
            wait_paused_timeout_s=float(wait_paused_timeout_s),
            trigger_when_idle=bool(trigger_when_idle),
        )
        rospy.loginfo("[EXEC_NODE] auto_charge enabled. battery_topic=%s dock=%s", str(battery_topic), str(dock))

    # Optional debug-only startup resume requires an explicit run_id.
    if startup_resume_run_id:
        fsm.enqueue_cmd(f"resume run_id={startup_resume_run_id}")
    elif auto_start:
        fsm.enqueue_cmd(f"start zone_id={zone_id}")

    fsm.spin()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import shutil
import subprocess
import threading
import time
from typing import Dict, Optional, Tuple

import rospy
import tf2_ros
from geometry_msgs.msg import PoseStamped
from nav_msgs.msg import OccupancyGrid

from my_msg_srv.srv import RestartLocalization, RestartLocalizationResponse

from coverage_planner.map_io import compute_occupancy_grid_md5
from coverage_planner.ops_store.store import OperationsStore, RobotRuntimeStateRecord
from coverage_planner.plan_store.store import PlanStore
from coverage_planner.ros_contract import build_contract_report, validate_ros_contract


def _workspace_root() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))


def _tail_text(text: str, limit: int = 40) -> str:
    lines = [str(line) for line in str(text or "").splitlines() if str(line).strip()]
    if not lines:
        return ""
    return "\n".join(lines[-max(1, int(limit)):])


class LocalizationLifecycleManagerNode:
    def __init__(self):
        self.workspace_root = _workspace_root()
        self.plan_db_path = rospy.get_param("~plan_db_path", "/data/coverage/planning.db")
        self.ops_db_path = rospy.get_param("~ops_db_path", "/data/coverage/operations.db")
        self.robot_id = str(rospy.get_param("~robot_id", "local_robot")).strip() or "local_robot"
        self.runtime_ns = str(rospy.get_param("~runtime_ns", "/cartographer/runtime")).rstrip("/")
        self.service_name = str(
            rospy.get_param("~service_name", "/cartographer/runtime/restart_localization")
        ).strip() or "/cartographer/runtime/restart_localization"
        self.contract_param_ns = str(
            rospy.get_param("~contract_param_ns", "/cartographer/runtime/contracts/restart_localization")
        ).strip()
        self.map_topic = str(rospy.get_param("~map_topic", "/map")).strip() or "/map"
        self.tracked_pose_topic = str(rospy.get_param("~tracked_pose_topic", "/tracked_pose")).strip() or "/tracked_pose"
        self.tf_parent_frame = str(rospy.get_param("~tf_parent_frame", "map")).strip() or "map"
        self.tf_child_frame = str(rospy.get_param("~tf_child_frame", "odom")).strip() or "odom"
        self.ready_timeout_s = max(10.0, float(rospy.get_param("~ready_timeout_s", 120.0)))
        self.tf_poll_timeout_s = max(0.05, float(rospy.get_param("~tf_poll_timeout_s", 0.2)))
        self.tracked_pose_fresh_timeout_s = max(
            0.2, float(rospy.get_param("~tracked_pose_fresh_timeout_s", 2.0))
        )
        self.command_timeout_s = max(10.0, float(rospy.get_param("~command_timeout_s", 180.0)))
        self.stop_timeout_s = max(5.0, float(rospy.get_param("~stop_timeout_s", 30.0)))
        self.repo_map_root = os.path.expanduser(
            str(rospy.get_param("~repo_map_root", os.path.join(self.workspace_root, "map"))).strip()
        )
        self.log_root = os.path.expanduser(
            str(rospy.get_param("~log_root", os.path.join(self.workspace_root, "log"))).strip()
        )
        self.start_script = os.path.expanduser(
            str(rospy.get_param("~start_script", os.path.join(self.workspace_root, "scripts", "run_cartographer_nodes.sh"))).strip()
        )
        self.stop_script = os.path.expanduser(
            str(rospy.get_param("~stop_script", os.path.join(self.workspace_root, "scripts", "stop_cartographer_nodes.sh"))).strip()
        )
        self.localization_script = os.path.expanduser(
            str(
                rospy.get_param(
                    "~localization_script",
                    os.path.join(self.workspace_root, "scripts", "run_localization_with_agv_speed_odom_rviz.sh"),
                )
            ).strip()
        )

        self._plan_store = PlanStore(self.plan_db_path)
        self._ops = OperationsStore(self.ops_db_path)
        self._service_lock = threading.Lock()
        self._contract_report = self._prepare_contract_report()

        self._tracked_pose_ts = 0.0
        self._map_ts = 0.0
        self._map_md5 = ""

        self._tf_buffer = tf2_ros.Buffer(cache_time=rospy.Duration(30.0))
        self._tf_listener = tf2_ros.TransformListener(self._tf_buffer)

        os.makedirs(self.repo_map_root, exist_ok=True)
        os.makedirs(self.log_root, exist_ok=True)

        rospy.Subscriber(self.tracked_pose_topic, PoseStamped, self._on_tracked_pose, queue_size=20)
        rospy.Subscriber(self.map_topic, OccupancyGrid, self._on_map, queue_size=2)
        rospy.Service(self.service_name, RestartLocalization, self._handle_restart)
        if self.contract_param_ns:
            rospy.set_param(self.contract_param_ns, self._contract_report)

        rospy.loginfo(
            "[localization_lifecycle] ready service=%s md5=%s start=%s localize=%s stop=%s contract_param=%s",
            self.service_name,
            self._contract_report["service"]["md5"],
            self.start_script,
            self.localization_script,
            self.stop_script,
            self.contract_param_ns or "-",
        )

    def _prepare_contract_report(self) -> Dict[str, object]:
        validate_ros_contract(
            "RestartLocalizationRequest",
            RestartLocalization._request_class,
            required_fields=["robot_id", "map_name"],
        )
        validate_ros_contract(
            "RestartLocalizationResponse",
            RestartLocalizationResponse,
            required_fields=["success", "message", "map_name", "localization_state"],
        )
        return build_contract_report(
            service_name=self.service_name,
            contract_name="restart_localization",
            service_cls=RestartLocalization,
            request_cls=RestartLocalization._request_class,
            response_cls=RestartLocalizationResponse,
            dependencies={},
            features=[
                "restart_localization_service",
                "map->odom_and_tracked_pose_health_gating",
            ],
        )

    def _runtime_param(self, key: str) -> str:
        return self.runtime_ns + "/" + str(key or "").strip()

    def _on_tracked_pose(self, _msg: PoseStamped):
        self._tracked_pose_ts = time.time()

    def _on_map(self, msg: OccupancyGrid):
        self._map_ts = time.time()
        try:
            self._map_md5 = str(compute_occupancy_grid_md5(msg) or "").strip()
        except Exception:
            self._map_md5 = ""

    def _update_runtime_state(
        self,
        *,
        robot_id: str,
        map_name: Optional[str] = None,
        localization_state: Optional[str] = None,
        localization_valid: Optional[bool] = None,
    ):
        current = self._ops.get_robot_runtime_state(robot_id) or RobotRuntimeStateRecord(robot_id=robot_id)
        self._ops.upsert_robot_runtime_state(
            RobotRuntimeStateRecord(
                robot_id=robot_id,
                active_run_id=str(current.active_run_id or ""),
                active_job_id=str(current.active_job_id or ""),
                active_schedule_id=str(current.active_schedule_id or ""),
                map_name=str(map_name if map_name is not None else current.map_name or "").strip(),
                localization_state=(
                    str(localization_state)
                    if localization_state is not None
                    else str(current.localization_state or "")
                ),
                localization_valid=(
                    bool(localization_valid)
                    if localization_valid is not None
                    else bool(current.localization_valid)
                ),
                mission_state=str(current.mission_state or "IDLE"),
                phase=str(current.phase or "IDLE"),
                public_state=str(current.public_state or "IDLE"),
                return_to_dock_on_finish=current.return_to_dock_on_finish,
                repeat_after_full_charge=current.repeat_after_full_charge,
                armed=bool(current.armed),
                dock_state=str(current.dock_state or ""),
                battery_soc=float(current.battery_soc or 0.0),
                battery_valid=bool(current.battery_valid),
                executor_state=str(current.executor_state or ""),
                last_error_code=str(current.last_error_code or ""),
                last_error_msg=str(current.last_error_msg or ""),
                updated_ts=time.time(),
            )
        )

    def _set_localization_state(self, *, robot_id: str, map_name: str, state: str, valid: bool):
        state = str(state or "not_localized").strip() or "not_localized"
        rospy.set_param(self._runtime_param("localization_state"), state)
        rospy.set_param(self._runtime_param("localization_valid"), bool(valid))
        rospy.set_param(self._runtime_param("localization_stamp"), float(rospy.Time.now().to_sec()))
        if map_name:
            rospy.set_param(self._runtime_param("map_name"), str(map_name))
            rospy.set_param("/map_name", str(map_name))
        self._update_runtime_state(
            robot_id=robot_id,
            map_name=str(map_name or "").strip(),
            localization_state=state,
            localization_valid=bool(valid),
        )

    def _resolve_asset(self, *, robot_id: str, map_name: str) -> Optional[Dict[str, object]]:
        if map_name:
            return self._plan_store.resolve_map_asset(
                map_name=str(map_name or "").strip(),
                robot_id=robot_id,
            )
        return self._plan_store.get_active_map(robot_id=robot_id)

    def _ensure_repo_map_link(self, asset: Dict[str, object]) -> str:
        map_name = str((asset or {}).get("map_name") or "").strip()
        pbstream_path = os.path.expanduser(str((asset or {}).get("pbstream_path") or "").strip())
        if not map_name:
            raise RuntimeError("map asset missing map_name")
        if not pbstream_path:
            raise RuntimeError("map asset missing pbstream_path")
        if not os.path.isfile(pbstream_path):
            raise RuntimeError("pbstream not found: %s" % pbstream_path)

        target_path = os.path.join(self.repo_map_root, map_name + ".pbstream")
        src_real = os.path.realpath(pbstream_path)
        if os.path.exists(target_path):
            try:
                if os.path.realpath(target_path) == src_real:
                    return os.path.basename(target_path)
            except Exception:
                pass
            os.unlink(target_path)
        try:
            os.symlink(src_real, target_path)
        except Exception:
            shutil.copy2(src_real, target_path)
        return os.path.basename(target_path)

    def _run_sync(self, cmd, *, timeout_s: float, env: Optional[dict] = None) -> Tuple[int, str]:
        try:
            proc = subprocess.run(
                cmd,
                cwd=self.workspace_root,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=float(timeout_s),
                check=False,
            )
            return int(proc.returncode), str(proc.stdout or "")
        except subprocess.TimeoutExpired as e:
            return 124, str(e.stdout or e.stderr or "timeout")

    def _start_runtime_script(self) -> Tuple[int, str]:
        stamp = time.strftime("%Y%m%d_%H%M%S", time.localtime())
        log_path = os.path.join(self.log_root, "localization_lifecycle_run_cartographer_%s.log" % stamp)
        log_fh = open(log_path, "a", encoding="utf-8")
        proc = subprocess.Popen(
            [self.start_script],
            cwd=self.workspace_root,
            env=os.environ.copy(),
            stdin=subprocess.DEVNULL,
            stdout=log_fh,
            stderr=subprocess.STDOUT,
            preexec_fn=os.setsid,
        )
        log_fh.close()
        return int(proc.pid), log_path

    def _stop_runtime(self):
        if not os.path.isfile(self.stop_script):
            raise RuntimeError("stop script not found: %s" % self.stop_script)
        code, output = self._run_sync([self.stop_script], timeout_s=self.stop_timeout_s, env=os.environ.copy())
        if code not in (0,):
            rospy.logwarn("[localization_lifecycle] stop script exit=%s output=\n%s", code, _tail_text(output))

    def _wait_until_ready(self, *, robot_id: str, asset: Dict[str, object], started_after_ts: float) -> Tuple[bool, str]:
        deadline = time.time() + self.ready_timeout_s
        target_name = str((asset or {}).get("map_name") or "").strip()
        target_md5 = str((asset or {}).get("map_md5") or "").strip()
        last_reason = "waiting_localization_ready"
        while (time.time() < deadline) and (not rospy.is_shutdown()):
            now = time.time()
            tf_ok = False
            try:
                tf_ok = bool(
                    self._tf_buffer.can_transform(
                        self.tf_parent_frame,
                        self.tf_child_frame,
                        rospy.Time(0),
                        rospy.Duration(self.tf_poll_timeout_s),
                    )
                )
            except Exception:
                tf_ok = False

            tracked_ok = (
                self._tracked_pose_ts >= started_after_ts
                and (now - self._tracked_pose_ts) <= self.tracked_pose_fresh_timeout_s
            )
            map_ready = self._map_ts >= started_after_ts and bool(self._map_md5)
            map_match = (not target_md5) or (str(self._map_md5 or "") == target_md5)
            selected = self._plan_store.get_active_map(robot_id=robot_id) or {}
            selected_name = str(selected.get("map_name") or "").strip()
            selected_ok = (not target_name) or (selected_name == target_name)

            if tf_ok and tracked_ok and map_ready and map_match and selected_ok:
                map_id = str((asset or {}).get("map_id") or "").strip()
                map_md5 = str(target_md5 or self._map_md5 or "").strip()
                if map_id:
                    rospy.set_param("/map_id", map_id)
                if map_md5:
                    rospy.set_param("/map_md5", map_md5)
                self._set_localization_state(
                    robot_id=robot_id,
                    map_name=target_name,
                    state="localized",
                    valid=True,
                )
                return True, "localized"

            if not tf_ok:
                last_reason = "map->odom TF not ready"
            elif not tracked_ok:
                last_reason = "tracked_pose not fresh"
            elif not map_ready:
                last_reason = "runtime map identity unavailable"
            elif not map_match:
                last_reason = "runtime map_md5 mismatch"
            elif not selected_ok:
                last_reason = "active selected map does not match requested map"

            rospy.sleep(0.2)

        self._set_localization_state(
            robot_id=robot_id,
            map_name=target_name,
            state="not_localized",
            valid=False,
        )
        return False, last_reason

    def _handle_restart(self, req):
        robot_id = str(req.robot_id or self.robot_id).strip() or self.robot_id
        map_name = str(req.map_name or "").strip()
        with self._service_lock:
            asset = self._resolve_asset(robot_id=robot_id, map_name=map_name)
            if not asset:
                rospy.logwarn(
                    "[localization_lifecycle] restart request rejected robot=%s requested_map=%s reason=map_asset_not_found",
                    robot_id,
                    map_name or "-",
                )
                return RestartLocalizationResponse(
                    success=False,
                    message="map asset not found",
                    map_name=map_name,
                    localization_state="not_localized",
                )
            if not bool(asset.get("enabled", True)):
                rospy.logwarn(
                    "[localization_lifecycle] restart request rejected robot=%s requested_map=%s resolved_map=%s reason=map_asset_disabled",
                    robot_id,
                    map_name or "-",
                    str(asset.get("map_name") or map_name or "-"),
                )
                return RestartLocalizationResponse(
                    success=False,
                    message="map asset is disabled",
                    map_name=str(asset.get("map_name") or map_name),
                    localization_state="not_localized",
                )

            resolved_name = str(asset.get("map_name") or map_name or "").strip()
            try:
                rospy.loginfo(
                    "[localization_lifecycle] restart request robot=%s requested_map=%s resolved_map=%s asset_id=%s pbstream=%s",
                    robot_id,
                    map_name or "-",
                    resolved_name or "-",
                    str(asset.get("map_id") or "-"),
                    str(asset.get("pbstream_path") or asset.get("map_path") or "-"),
                )
                self._plan_store.set_active_map(map_name=resolved_name, robot_id=robot_id)
                repo_map_file = self._ensure_repo_map_link(asset)
                restart_started_ts = time.time()
                self._set_localization_state(
                    robot_id=robot_id,
                    map_name=resolved_name,
                    state="relocalizing",
                    valid=False,
                )
                rospy.loginfo(
                    "[localization_lifecycle] restart begin robot=%s map=%s repo_map_file=%s",
                    robot_id,
                    resolved_name or "-",
                    repo_map_file,
                )
                self._stop_runtime()
                start_pid, start_log = self._start_runtime_script()
                env = os.environ.copy()
                env["SKIP_RVIZ"] = "1"
                env["WAIT_TIMEOUT_SEC"] = str(int(max(30.0, self.ready_timeout_s)))
                code, output = self._run_sync(
                    [self.localization_script, repo_map_file],
                    timeout_s=self.command_timeout_s,
                    env=env,
                )
                if code != 0:
                    self._set_localization_state(
                        robot_id=robot_id,
                        map_name=resolved_name,
                        state="not_localized",
                        valid=False,
                    )
                    rospy.logerr(
                        "[localization_lifecycle] restart failed robot=%s map=%s code=%s start_pid=%s start_log=%s",
                        robot_id,
                        resolved_name or "-",
                        code,
                        start_pid,
                        start_log,
                    )
                    return RestartLocalizationResponse(
                        success=False,
                        message=(
                            "restart localization script failed code=%s start_pid=%s start_log=%s\n%s"
                            % (code, start_pid, start_log, _tail_text(output))
                        ).strip(),
                        map_name=resolved_name,
                        localization_state="not_localized",
                    )
                ok, msg = self._wait_until_ready(
                    robot_id=robot_id,
                    asset=asset,
                    started_after_ts=restart_started_ts,
                )
                rospy.loginfo(
                    "[localization_lifecycle] restart completed robot=%s map=%s success=%s state=%s message=%s",
                    robot_id,
                    resolved_name or "-",
                    str(bool(ok)).lower(),
                    "localized" if ok else "not_localized",
                    str(msg or ""),
                )
                return RestartLocalizationResponse(
                    success=bool(ok),
                    message=str(msg or ""),
                    map_name=resolved_name,
                    localization_state="localized" if ok else "not_localized",
                )
            except Exception as e:
                self._set_localization_state(
                    robot_id=robot_id,
                    map_name=resolved_name,
                    state="not_localized",
                    valid=False,
                )
                rospy.logerr(
                    "[localization_lifecycle] restart exception robot=%s map=%s err=%s",
                    robot_id,
                    resolved_name or "-",
                    str(e),
                )
                return RestartLocalizationResponse(
                    success=False,
                    message=str(e),
                    map_name=resolved_name,
                    localization_state="not_localized",
                )


def main():
    rospy.init_node("localization_lifecycle_manager", anonymous=False)
    LocalizationLifecycleManagerNode()
    rospy.spin()


if __name__ == "__main__":
    main()

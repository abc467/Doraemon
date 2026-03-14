#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import math
from typing import Optional, Tuple

import rospy
from geometry_msgs.msg import PoseWithCovarianceStamped
from nav_msgs.msg import OccupancyGrid
from std_srvs.srv import Trigger

from my_msg_srv.srv import (
    ActivateMapAsset,
    ActivateMapAssetResponse,
    RelocalizeMapAsset,
    RelocalizeMapAssetResponse,
)

from coverage_planner.map_io import compute_occupancy_grid_md5
from coverage_planner.ops_store.store import OperationsStore, RobotRuntimeStateRecord
from coverage_planner.plan_store.store import PlanStore


def _yaw_to_quat(yaw: float):
    half = 0.5 * float(yaw)
    return (0.0, 0.0, math.sin(half), math.cos(half))


class CartographerRuntimeManagerNode:
    def __init__(self):
        self.plan_db_path = rospy.get_param("~plan_db_path", "/data/coverage/planning.db")
        self.ops_db_path = rospy.get_param("~ops_db_path", "/data/coverage/operations.db")
        self.robot_id = str(rospy.get_param("~robot_id", "local_robot")).strip() or "local_robot"
        self.runtime_ns = str(rospy.get_param("~runtime_ns", "/cartographer/runtime")).rstrip("/")
        self.activate_service_name = str(rospy.get_param("~activate_service", "/map_assets/activate")).strip()
        self.relocalize_service_name = str(
            rospy.get_param("~relocalize_service", "/map_assets/relocalize")
        ).strip()
        self.reload_service_name = str(rospy.get_param("~reload_service", "/cartographer/runtime/reload")).strip()
        self.map_topic = str(rospy.get_param("~map_topic", "/map")).strip() or "/map"
        self.initial_pose_topic = str(rospy.get_param("~initial_pose_topic", "/initialpose")).strip() or "/initialpose"
        self.map_timeout_s = max(0.5, float(rospy.get_param("~map_timeout_s", 10.0)))
        self.reload_timeout_s = max(1.0, float(rospy.get_param("~reload_timeout_s", 30.0)))
        self.initial_map_name = str(rospy.get_param("~map_name", "")).strip()
        self.initial_map_version = str(rospy.get_param("~map_version", "")).strip()
        self.initial_pose_cov_xy = float(rospy.get_param("~initial_pose_cov_xy", 0.25))
        self.initial_pose_cov_yaw = float(rospy.get_param("~initial_pose_cov_yaw", 0.25))

        self._store = PlanStore(self.plan_db_path)
        self._ops = OperationsStore(self.ops_db_path)
        self._reload_cli = rospy.ServiceProxy(self.reload_service_name, Trigger)
        self._initial_pose_pub = rospy.Publisher(self.initial_pose_topic, PoseWithCovarianceStamped, queue_size=1)

        rospy.Service(self.activate_service_name, ActivateMapAsset, self._handle_activate)
        rospy.Service(self.relocalize_service_name, RelocalizeMapAsset, self._handle_relocalize)

        self._set_localization_state("not_localized", valid=False)
        rospy.Timer(rospy.Duration(1.0), self._on_startup_timer, oneshot=True)
        rospy.loginfo(
            "[cartographer_runtime_manager] ready activate=%s relocalize=%s reload=%s",
            self.activate_service_name,
            self.relocalize_service_name,
            self.reload_service_name,
        )

    def _runtime_param(self, key: str) -> str:
        return self.runtime_ns + "/" + str(key or "").strip()

    def _set_localization_state(self, state: str, *, valid: bool):
        state = str(state or "not_localized").strip() or "not_localized"
        rospy.set_param(self._runtime_param("localization_state"), state)
        rospy.set_param(self._runtime_param("localization_valid"), bool(valid))
        rospy.set_param(self._runtime_param("localization_stamp"), float(rospy.Time.now().to_sec()))

    def _update_runtime_state(
        self,
        *,
        map_name: Optional[str] = None,
        map_version: Optional[str] = None,
        localization_state: Optional[str] = None,
        localization_valid: Optional[bool] = None,
    ):
        current = self._ops.get_robot_runtime_state(self.robot_id) or RobotRuntimeStateRecord(robot_id=self.robot_id)
        self._ops.upsert_robot_runtime_state(
            RobotRuntimeStateRecord(
                robot_id=self.robot_id,
                active_run_id=str(current.active_run_id or ""),
                active_job_id=str(current.active_job_id or ""),
                active_schedule_id=str(current.active_schedule_id or ""),
                map_name=str(map_name if map_name is not None else current.map_name or ""),
                map_version=str(map_version if map_version is not None else current.map_version or ""),
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
                armed=bool(current.armed),
                dock_state=str(current.dock_state or ""),
                battery_soc=float(current.battery_soc or 0.0),
                battery_valid=bool(current.battery_valid),
                executor_state=str(current.executor_state or ""),
                last_error_code=str(current.last_error_code or ""),
                last_error_msg=str(current.last_error_msg or ""),
                updated_ts=rospy.Time.now().to_sec(),
            )
        )

    def _is_activate_allowed(self, robot_id: str) -> Tuple[bool, str]:
        state = self._ops.get_robot_runtime_state(str(robot_id or self.robot_id or "local_robot"))
        if state is None:
            return True, ""
        mission_state = str(state.mission_state or "IDLE").upper()
        phase = str(state.phase or "IDLE").upper()
        dock_state = str(state.dock_state or "").upper()
        if mission_state != "IDLE":
            return False, "map switch only allowed while mission_state=IDLE"
        if phase not in ("", "IDLE"):
            return False, "map switch only allowed while phase=IDLE"
        if dock_state not in ("", "IDLE"):
            return False, "map switch only allowed while dock_state is empty/IDLE"
        return True, ""

    def _resolve_asset(self, *, map_name: str = "", map_version: str = "") -> Optional[dict]:
        if map_name:
            return self._store.resolve_map_asset_version(
                map_name=str(map_name or "").strip(),
                map_version=str(map_version or "").strip(),
                robot_id=self.robot_id,
            )
        return self._store.get_active_map(robot_id=self.robot_id)

    def _configure_runtime(self, asset: dict):
        pbstream_path = str(asset.get("pbstream_path") or "").strip()
        if not pbstream_path:
            raise RuntimeError("map asset missing pbstream_path")
        if not rospy.get_param(self._runtime_param("mode"), ""):
            rospy.set_param(self._runtime_param("mode"), "localization")
        else:
            rospy.set_param(self._runtime_param("mode"), "localization")
        rospy.set_param(self._runtime_param("map_name"), str(asset.get("map_name") or ""))
        rospy.set_param(self._runtime_param("map_version"), str(asset.get("map_version") or ""))
        rospy.set_param(self._runtime_param("pbstream_path"), pbstream_path)

    def _wait_for_runtime_map(self, asset: dict) -> Tuple[bool, str]:
        try:
            occ = rospy.wait_for_message(self.map_topic, OccupancyGrid, timeout=self.map_timeout_s)
        except Exception as e:
            return False, "failed waiting for %s: %s" % (self.map_topic, str(e))

        runtime_md5 = compute_occupancy_grid_md5(occ)
        expected_md5 = str(asset.get("map_md5") or "").strip()
        expected_id = str(asset.get("map_id") or "").strip()
        runtime_id = "map_%s" % str(runtime_md5)[:8] if runtime_md5 else ""
        if expected_md5 and runtime_md5 and expected_md5 != runtime_md5:
            return False, "runtime map_md5 mismatch expected=%s got=%s" % (expected_md5, runtime_md5)

        rospy.set_param("/map_name", str(asset.get("map_name") or ""))
        rospy.set_param("/map_version", str(asset.get("map_version") or ""))
        rospy.set_param("/map_id", str(expected_id or runtime_id))
        rospy.set_param("/map_md5", str(expected_md5 or runtime_md5))
        return True, ""

    def _activate_asset(self, asset: dict, *, enforce_idle: bool) -> Tuple[bool, str]:
        robot_id = self.robot_id
        if enforce_idle:
            allowed, msg = self._is_activate_allowed(robot_id)
            if not allowed:
                return False, msg
        if not bool(asset.get("enabled", True)):
            return False, "map asset is disabled"
        self._configure_runtime(asset)
        rospy.wait_for_service(self.reload_service_name, timeout=2.0)
        resp = self._reload_cli()
        if not bool(resp.success):
            return False, str(resp.message or "cartographer reload failed")
        ok, msg = self._wait_for_runtime_map(asset)
        if not ok:
            return False, msg
        self._store.set_active_map(
            map_name=str(asset.get("map_name") or ""),
            map_version=str(asset.get("map_version") or ""),
            robot_id=robot_id,
        )
        self._set_localization_state("not_localized", valid=False)
        self._update_runtime_state(
            map_name=str(asset.get("map_name") or ""),
            map_version=str(asset.get("map_version") or ""),
            localization_state="not_localized",
            localization_valid=False,
        )
        return True, "activated"

    def _make_initial_pose(self, req):
        msg = PoseWithCovarianceStamped()
        msg.header.stamp = rospy.Time.now()
        msg.header.frame_id = str(req.frame_id or "map").strip() or "map"
        msg.pose.pose.position.x = float(req.x)
        msg.pose.pose.position.y = float(req.y)
        msg.pose.pose.position.z = 0.0
        qx, qy, qz, qw = _yaw_to_quat(float(req.yaw))
        msg.pose.pose.orientation.x = qx
        msg.pose.pose.orientation.y = qy
        msg.pose.pose.orientation.z = qz
        msg.pose.pose.orientation.w = qw
        cov = [0.0] * 36
        cov[0] = float(self.initial_pose_cov_xy)
        cov[7] = float(self.initial_pose_cov_xy)
        cov[35] = float(self.initial_pose_cov_yaw)
        msg.pose.covariance = cov
        return msg

    def _handle_activate(self, req):
        map_name = str(req.map_name or "").strip()
        map_version = str(req.map_version or "").strip()
        asset = self._resolve_asset(map_name=map_name, map_version=map_version)
        if not asset:
            return ActivateMapAssetResponse(
                success=False,
                message="map asset not found",
                map_name=map_name,
                map_version=map_version,
                map_id="",
                map_md5="",
                yaml_path="",
            )
        try:
            ok, msg = self._activate_asset(asset, enforce_idle=True)
        except Exception as e:
            ok, msg = False, str(e)
        if not ok:
            return ActivateMapAssetResponse(
                success=False,
                message=str(msg or "activation failed"),
                map_name=str(asset.get("map_name") or map_name),
                map_version=str(asset.get("map_version") or map_version),
                map_id="",
                map_md5="",
                yaml_path=str(asset.get("yaml_path") or ""),
            )
        return ActivateMapAssetResponse(
            success=True,
            message="activated",
            map_name=str(asset.get("map_name") or ""),
            map_version=str(asset.get("map_version") or ""),
            map_id=str(asset.get("map_id") or ""),
            map_md5=str(asset.get("map_md5") or ""),
            yaml_path=str(asset.get("yaml_path") or ""),
        )

    def _handle_relocalize(self, req):
        active = self._store.get_active_map(robot_id=self.robot_id) or {}
        active_name = str(active.get("map_name") or "")
        active_version = str(active.get("map_version") or "")
        req_name = str(req.map_name or "").strip()
        req_version = str(req.map_version or "").strip()
        if req_name and req_name != active_name:
            return RelocalizeMapAssetResponse(
                success=False,
                message="active map mismatch",
                localization_state="not_localized",
                map_name=active_name,
                map_version=active_version,
            )
        if req_version and req_version != active_version:
            return RelocalizeMapAssetResponse(
                success=False,
                message="active map version mismatch",
                localization_state="not_localized",
                map_name=active_name,
                map_version=active_version,
            )
        self._set_localization_state("relocalizing", valid=False)
        self._initial_pose_pub.publish(self._make_initial_pose(req))
        self._set_localization_state("localized", valid=True)
        self._update_runtime_state(
            map_name=active_name,
            map_version=active_version,
            localization_state="localized",
            localization_valid=True,
        )
        return RelocalizeMapAssetResponse(
            success=True,
            message="relocalized",
            localization_state="localized",
            map_name=active_name,
            map_version=active_version,
        )

    def _on_startup_timer(self, _evt):
        asset = None
        try:
            if self.initial_map_name:
                asset = self._resolve_asset(map_name=self.initial_map_name, map_version=self.initial_map_version)
            if asset is None:
                asset = self._store.get_active_map(robot_id=self.robot_id)
            if asset is None:
                rospy.logwarn("[cartographer_runtime_manager] no active map to load at startup")
                return
            ok, msg = self._activate_asset(asset, enforce_idle=False)
            if not ok:
                rospy.logerr("[cartographer_runtime_manager] startup activation failed: %s", str(msg))
            else:
                rospy.loginfo(
                    "[cartographer_runtime_manager] startup map loaded %s@%s",
                    asset.get("map_name"),
                    asset.get("map_version"),
                )
        except Exception as e:
            rospy.logerr("[cartographer_runtime_manager] startup load failed: %s", str(e))


def main():
    rospy.init_node("cartographer_runtime_manager", anonymous=False)
    CartographerRuntimeManagerNode()
    rospy.spin()


if __name__ == "__main__":
    main()

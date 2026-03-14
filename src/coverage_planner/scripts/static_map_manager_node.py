#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import threading
from typing import Tuple

import rospy
from nav_msgs.msg import MapMetaData, OccupancyGrid
from nav_msgs.srv import GetMap, GetMapResponse

from my_msg_srv.srv import ActivateMapAsset, ActivateMapAssetResponse

from coverage_planner.map_io import yaml_pgm_to_occupancy
from coverage_planner.ops_store.store import OperationsStore
from coverage_planner.plan_store.store import PlanStore


class StaticMapManagerNode:
    def __init__(self):
        self.plan_db_path = rospy.get_param("~plan_db_path", "/data/coverage/planning.db")
        self.ops_db_path = rospy.get_param("~ops_db_path", "/data/coverage/operations.db")
        self.robot_id = str(rospy.get_param("~robot_id", "local_robot"))
        self.map_topic = str(rospy.get_param("~map_topic", "/map"))
        self.metadata_topic = str(rospy.get_param("~metadata_topic", "/map_metadata"))
        self.frame_id = str(rospy.get_param("~frame_id", "map"))
        self.activate_service_name = str(rospy.get_param("~activate_service", "/map_assets/activate"))
        self.initial_map_name = str(rospy.get_param("~map_name", "") or "").strip()
        self.initial_map_version = str(rospy.get_param("~map_version", "") or "").strip()

        self._lock = threading.Lock()
        self._store = PlanStore(self.plan_db_path)
        self._ops = OperationsStore(self.ops_db_path)
        self._map_msg = OccupancyGrid()
        self._meta_msg = MapMetaData()
        self._active = None

        self._map_pub = rospy.Publisher(self.map_topic, OccupancyGrid, queue_size=1, latch=True)
        self._meta_pub = rospy.Publisher(self.metadata_topic, MapMetaData, queue_size=1, latch=True)
        self._static_srv = rospy.Service("/static_map", GetMap, self._handle_get_map)
        self._activate_srv = rospy.Service(self.activate_service_name, ActivateMapAsset, self._handle_activate)

        self._load_from_active()
        rospy.loginfo("[static_map_manager] ready service=%s db=%s", self.activate_service_name, self.plan_db_path)

    def _publish_current(self):
        with self._lock:
            map_msg = self._map_msg
            meta_msg = self._meta_msg
            active = dict(self._active or {})
        map_msg.header.stamp = rospy.Time.now()
        self._map_pub.publish(map_msg)
        self._meta_pub.publish(meta_msg)
        if active:
            rospy.set_param("/map_id", str(active.get("map_id") or ""))
            rospy.set_param("/map_md5", str(active.get("map_md5") or ""))
            rospy.set_param("/map_name", str(active.get("map_name") or ""))
            rospy.set_param("/map_version", str(active.get("map_version") or ""))

    def _load_asset(self, asset: dict):
        yaml_path = str(asset.get("yaml_path") or "").strip()
        if not yaml_path:
            raise RuntimeError("asset missing yaml_path")
        if not bool(asset.get("enabled", True)):
            raise RuntimeError("asset is disabled")
        occ = yaml_pgm_to_occupancy(yaml_path)
        occ.header.frame_id = str(asset.get("frame_id") or self.frame_id or "map")
        with self._lock:
            self._map_msg = occ
            self._meta_msg = occ.info
            self._active = dict(asset)
        self._publish_current()

    def _load_from_active(self):
        asset = None
        if self.initial_map_name:
            asset = self._store.resolve_map_asset_version(
                map_name=self.initial_map_name,
                map_version=self.initial_map_version,
                robot_id=self.robot_id,
            )
            if asset:
                self._store.set_active_map(
                    map_name=str(asset.get("map_name") or ""),
                    map_version=str(asset.get("map_version") or ""),
                    robot_id=self.robot_id,
                )
        if asset is None:
            asset = self._store.get_active_map(robot_id=self.robot_id)
        if not asset:
            rospy.logwarn_throttle(30.0, "[static_map_manager] no active map configured for robot=%s", self.robot_id)
            return
        try:
            self._load_asset(asset)
            rospy.loginfo(
                "[static_map_manager] loaded active map=%s version=%s id=%s",
                asset.get("map_name"),
                asset.get("map_version"),
                asset.get("map_id"),
            )
        except Exception as e:
            rospy.logerr("[static_map_manager] failed to load active map: %s", str(e))

    def _handle_get_map(self, _req):
        with self._lock:
            return GetMapResponse(map=self._map_msg)

    def _is_activate_allowed(self, robot_id: str) -> Tuple[bool, str]:
        try:
            state = self._ops.get_robot_runtime_state(str(robot_id or self.robot_id or "local_robot"))
        except Exception as e:
            return False, "failed to read runtime state: %s" % str(e)
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

    def _handle_activate(self, req):
        map_name = str(req.map_name or "").strip()
        map_version = str(req.map_version or "").strip()
        robot_id = str(req.robot_id or self.robot_id or "local_robot").strip()
        allowed, allow_msg = self._is_activate_allowed(robot_id)
        if not allowed:
            return ActivateMapAssetResponse(
                success=False,
                message=str(allow_msg or "map activation rejected"),
                map_name=map_name,
                map_version=map_version,
                map_id="",
                map_md5="",
                yaml_path="",
            )
        try:
            asset = self._store.resolve_map_asset_version(
                map_name=map_name,
                map_version=map_version,
                robot_id=robot_id,
            )
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
            if not bool(asset.get("enabled", True)):
                return ActivateMapAssetResponse(
                    success=False,
                    message="map asset is disabled",
                    map_name=map_name,
                    map_version=map_version,
                    map_id="",
                    map_md5="",
                    yaml_path="",
                )
            self._store.set_active_map(
                map_name=str(asset.get("map_name") or ""),
                map_version=str(asset.get("map_version") or ""),
                robot_id=robot_id,
            )
            self._load_asset(asset)
            return ActivateMapAssetResponse(
                success=True,
                message="activated",
                map_name=str(asset.get("map_name") or ""),
                map_version=str(asset.get("map_version") or ""),
                map_id=str(asset.get("map_id") or ""),
                map_md5=str(asset.get("map_md5") or ""),
                yaml_path=str(asset.get("yaml_path") or ""),
            )
        except Exception as e:
            return ActivateMapAssetResponse(
                success=False,
                message=str(e),
                map_name=map_name,
                map_version=map_version,
                map_id="",
                map_md5="",
                yaml_path="",
            )


def main():
    rospy.init_node("static_map_manager", anonymous=False)
    node = StaticMapManagerNode()
    rate = rospy.Rate(0.5)
    while not rospy.is_shutdown():
        if not node._active:
            try:
                node._load_from_active()
            except Exception:
                pass
        node._publish_current()
        rate.sleep()


if __name__ == "__main__":
    main()

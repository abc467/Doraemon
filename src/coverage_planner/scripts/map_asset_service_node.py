#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import rospy

from my_msg_srv.msg import PgmData
from my_msg_srv.srv import OperateMap, OperateMapResponse

from coverage_planner.map_io import yaml_pgm_to_occupancy
from coverage_planner.plan_store.store import PlanStore


class MapAssetServiceNode:
    def __init__(self):
        self.plan_db_path = rospy.get_param("~plan_db_path", "/data/coverage/planning.db")
        self.robot_id = str(rospy.get_param("~robot_id", "local_robot"))
        self.service_name = str(rospy.get_param("~service_name", "/clean_robot_server/map_server"))
        self.store = PlanStore(self.plan_db_path)
        self.srv = rospy.Service(self.service_name, OperateMap, self._handle)
        rospy.loginfo("[map_asset_service] ready service=%s db=%s", self.service_name, self.plan_db_path)

    def _empty_resp(self, *, success: bool, message: str) -> OperateMapResponse:
        return OperateMapResponse(success=bool(success), message=str(message or ""), map=PgmData(), maps=[])

    def _resolve_req_name_version(self, req):
        map_name = str(req.map_name or "").strip()
        map_version = str(req.map_version or "").strip()
        if (not map_name) and getattr(req, "map", None):
            map_name = str(req.map.map_name or "").strip()
        if (not map_version) and getattr(req, "map", None):
            map_version = str(req.map.map_version or "").strip()
        return map_name, map_version

    def _to_msg(self, asset: dict, *, include_map_data: bool = False, active_map=None) -> PgmData:
        msg = PgmData()
        if not asset:
            return msg
        active_map = active_map or {}
        msg.map_name = str(asset.get("map_name") or "")
        msg.map_version = str(asset.get("map_version") or "")
        msg.display_name = str(asset.get("display_name") or msg.map_name)
        msg.description = str(asset.get("description") or "")
        msg.enabled = bool(asset.get("enabled", True))
        msg.map_id = str(asset.get("map_id") or "")
        msg.map_md5 = str(asset.get("map_md5") or "")
        msg.is_active = (
            msg.map_name == str(active_map.get("map_name") or "")
            and msg.map_version == str(active_map.get("map_version") or "")
        )
        if include_map_data:
            yaml_path = str(asset.get("yaml_path") or "").strip()
            if yaml_path:
                msg.map_data = yaml_pgm_to_occupancy(yaml_path)
        return msg

    def _latest_asset_heads(self):
        active_map = self.store.get_active_map(robot_id=self.robot_id) or {}
        out = []
        seen = set()
        for asset in self.store.list_map_asset_versions():
            name = str(asset.get("map_name") or "")
            if not name or name in seen:
                continue
            seen.add(name)
            out.append(self._to_msg(asset, include_map_data=False, active_map=active_map))
        return out

    def _handle(self, req):
        op = int(req.operation)
        map_name, map_version = self._resolve_req_name_version(req)
        active_map = self.store.get_active_map(robot_id=self.robot_id) or {}

        if op == int(req.getAll):
            return OperateMapResponse(success=True, message="ok", map=PgmData(), maps=self._latest_asset_heads())

        if op == int(req.get):
            asset = self.store.resolve_map_asset_version(
                map_name=map_name,
                map_version=map_version,
                robot_id=self.robot_id,
            )
            if not asset:
                return self._empty_resp(success=False, message="map asset not found")
            return OperateMapResponse(
                success=True,
                message="ok",
                map=self._to_msg(asset, include_map_data=True, active_map=active_map),
                maps=[],
            )

        if op == int(req.add):
            return self._empty_resp(
                success=False,
                message="add is not supported; use /clean_robot_server/mapping_server",
            )

        if op == int(req.modify):
            if not map_name:
                return self._empty_resp(success=False, message="map_name is required for modify")
            current_asset = self.store.resolve_map_asset_version(
                map_name=map_name,
                map_version=map_version,
                robot_id=self.robot_id,
            )
            current_enabled = bool((current_asset or {}).get("enabled", True))
            self.store.update_map_asset_meta(
                map_name=map_name,
                display_name=(req.map.display_name if str(req.map.display_name or "").strip() else None),
                description=(req.map.description if str(req.map.description or "").strip() else None),
                enabled=(True if bool(req.map.enabled) else current_enabled),
            )
            if bool(req.set_active):
                asset = self.store.resolve_map_asset_version(
                    map_name=map_name,
                    map_version=map_version,
                    robot_id=self.robot_id,
                )
                if not asset:
                    return self._empty_resp(success=False, message="map version not found for activation")
                self.store.set_active_map(
                    map_name=str(asset.get("map_name") or ""),
                    map_version=str(asset.get("map_version") or ""),
                    robot_id=self.robot_id,
                )
                active_map = asset
            asset = self.store.resolve_map_asset_version(
                map_name=map_name,
                map_version=map_version,
                robot_id=self.robot_id,
            )
            return OperateMapResponse(
                success=True,
                message="updated",
                map=self._to_msg(asset or {}, include_map_data=False, active_map=active_map),
                maps=[],
            )

        if op == int(req.Delete):
            if not map_name:
                return self._empty_resp(success=False, message="map_name is required for Delete")
            if str(active_map.get("map_name") or "") == map_name:
                return self._empty_resp(success=False, message="cannot disable the active map")
            self.store.disable_map_asset(map_name=map_name)
            return self._empty_resp(success=True, message="disabled")

        return self._empty_resp(success=False, message="unsupported operation=%s" % op)


def main():
    rospy.init_node("map_asset_service", anonymous=False)
    MapAssetServiceNode()
    rospy.spin()


if __name__ == "__main__":
    main()

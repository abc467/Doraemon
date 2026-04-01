#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import shutil

import rospy
from nav_msgs.msg import OccupancyGrid

from my_msg_srv.msg import PgmData
from my_msg_srv.srv import OperateMap, OperateMapResponse

from coverage_planner.map_io import (
    compute_occupancy_grid_md5,
    origin_to_jsonable,
    write_occupancy_to_yaml_pgm,
    yaml_pgm_to_occupancy,
)
from coverage_planner.plan_store.store import PlanStore


def _workspace_root():
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))


class MapAssetServiceNode:
    def __init__(self):
        self.plan_db_path = rospy.get_param("~plan_db_path", "/data/coverage/planning.db")
        self.robot_id = str(rospy.get_param("~robot_id", "local_robot"))
        self.service_name = str(rospy.get_param("~service_name", "/clean_robot_server/map_server"))
        self.maps_root = os.path.expanduser(str(rospy.get_param("~maps_root", "/data/maps")))
        external_maps_root = str(rospy.get_param("~external_maps_root", "")).strip()
        if external_maps_root:
            self.external_maps_root = os.path.expanduser(external_maps_root)
        else:
            self.external_maps_root = os.path.join(_workspace_root(), "map")
        os.makedirs(self.external_maps_root, exist_ok=True)
        self.map_topic = str(rospy.get_param("~map_topic", "/map")).strip() or "/map"
        self.map_timeout_s = max(0.5, float(rospy.get_param("~map_timeout_s", 5.0)))
        os.makedirs(self.maps_root, exist_ok=True)
        self.store = PlanStore(self.plan_db_path)
        self.srv = rospy.Service(self.service_name, OperateMap, self._handle)
        rospy.loginfo(
            "[map_asset_service] ready service=%s db=%s external_maps_root=%s",
            self.service_name,
            self.plan_db_path,
            self.external_maps_root,
        )

    def _empty_resp(self, *, success: bool, message: str) -> OperateMapResponse:
        return OperateMapResponse(success=bool(success), message=str(message or ""), map=PgmData(), maps=[])

    def _resolve_req_name(self, req):
        map_name = str(req.map_name or "").strip()
        if (not map_name) and getattr(req, "map", None):
            map_name = str(req.map.map_name or "").strip()
        if map_name.endswith(".pbstream"):
            map_name = map_name[:-len(".pbstream")]
        return map_name

    def _target_paths(self, map_name: str):
        base = os.path.join(self.maps_root, str(map_name or "").strip())
        return {
            "pbstream_path": base + ".pbstream",
            "yaml_path": base + ".yaml",
            "pgm_path": base + ".pgm",
        }

    def _cleanup_paths(self, *paths: str):
        for path in paths:
            pp = str(path or "").strip()
            if not pp or not os.path.exists(pp):
                continue
            try:
                os.remove(pp)
            except Exception:
                pass

    def _set_current_map_params(self, asset: dict):
        rospy.set_param("/map_name", str(asset.get("map_name") or ""))

    def _enabled_from_request(self, req, *, current_enabled: bool, default_enabled: bool) -> bool:
        enabled_state = int(getattr(req, "enabled_state", int(req.ENABLE_KEEP)))
        if enabled_state == int(req.ENABLE_ENABLE):
            return True
        if enabled_state == int(req.ENABLE_DISABLE):
            return False
        return bool(default_enabled if current_enabled is None else current_enabled)

    def _import_external_map(self, *, map_name: str, set_active: bool, description: str = ""):
        map_name = str(map_name or "").strip()
        if not map_name:
            raise ValueError("map_name is required")
        description = str(description or "")
        asset = self.store.resolve_map_asset(
            map_name=map_name,
            robot_id=self.robot_id,
        )
        if asset is not None:
            raise ValueError("map asset already exists")

        source_pbstream = os.path.join(self.external_maps_root, map_name + ".pbstream")
        if not os.path.exists(source_pbstream):
            raise FileNotFoundError("external pbstream not found: %s" % source_pbstream)

        target_paths = self._target_paths(map_name)
        for path in target_paths.values():
            if os.path.exists(path):
                raise ValueError("target asset path already exists: %s" % path)

        try:
            occ = rospy.wait_for_message(self.map_topic, OccupancyGrid, timeout=float(self.map_timeout_s))
        except Exception as e:
            raise RuntimeError("failed to read %s: %s" % (self.map_topic, str(e)))

        map_md5 = compute_occupancy_grid_md5(occ)
        if not map_md5:
            raise RuntimeError("failed to compute map_md5 from current /map")
        map_id = "map_%s" % str(map_md5)[:8]
        pgm_path = ""
        yaml_path = ""
        pbstream_path = target_paths["pbstream_path"]
        try:
            pgm_path, yaml_path = write_occupancy_to_yaml_pgm(occ, self.maps_root, base_name=map_name)
            shutil.copy2(source_pbstream, pbstream_path)

            self.store.register_map_asset(
                map_name=map_name,
                map_id=map_id,
                map_md5=map_md5,
                yaml_path=yaml_path,
                pgm_path=pgm_path,
                pbstream_path=pbstream_path,
                frame_id=str(occ.header.frame_id or "map"),
                resolution=float(occ.info.resolution or 0.0),
                origin=origin_to_jsonable(occ),
                display_name=map_name,
                description=description,
                enabled=True,
                robot_id=self.robot_id,
                set_active=bool(set_active),
            )
        except Exception:
            self._cleanup_paths(pgm_path, yaml_path, pbstream_path)
            raise

        asset = self.store.resolve_map_asset(
            map_name=map_name,
            robot_id=self.robot_id,
        ) or {
            "map_name": map_name,
            "map_id": map_id,
            "map_md5": map_md5,
            "yaml_path": yaml_path,
            "pgm_path": pgm_path,
            "pbstream_path": pbstream_path,
            "enabled": True,
        }
        if set_active and asset:
            self._set_current_map_params(asset)
        return asset

    def _to_msg(self, asset: dict, *, include_map_data: bool = False, active_map=None) -> PgmData:
        msg = PgmData()
        if not asset:
            return msg
        active_map = active_map or {}
        msg.map_name = str(asset.get("map_name") or "")
        msg.description = str(asset.get("description") or "")
        msg.enabled = bool(asset.get("enabled", True))
        msg.map_id = str(asset.get("map_id") or "")
        msg.map_md5 = str(asset.get("map_md5") or "")
        msg.is_active = msg.map_name == str(active_map.get("map_name") or "")
        if include_map_data:
            yaml_path = str(asset.get("yaml_path") or "").strip()
            if yaml_path:
                msg.map_data = yaml_pgm_to_occupancy(yaml_path)
        return msg

    def _latest_asset_heads(self):
        active_map = self.store.get_active_map(robot_id=self.robot_id) or {}
        out = []
        seen = set()
        for asset in self.store.list_map_assets():
            name = str(asset.get("map_name") or "")
            if not name or name in seen:
                continue
            seen.add(name)
            out.append(self._to_msg(asset, include_map_data=False, active_map=active_map))
        return out

    def _handle(self, req):
        op = int(req.operation)
        map_name = self._resolve_req_name(req)
        active_map = self.store.get_active_map(robot_id=self.robot_id) or {}

        if op == int(req.getAll):
            return OperateMapResponse(success=True, message="ok", map=PgmData(), maps=self._latest_asset_heads())

        if op == int(req.get):
            asset = self.store.resolve_map_asset(
                map_name=map_name,
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
            if not map_name:
                return self._empty_resp(success=False, message="map_name is required for add")
            try:
                desired_enabled = self._enabled_from_request(req, current_enabled=None, default_enabled=True)
                if bool(req.set_active) and (not desired_enabled):
                    return self._empty_resp(success=False, message="cannot set_active on a disabled map")
                asset = self._import_external_map(
                    map_name=map_name,
                    set_active=(bool(req.set_active) and desired_enabled),
                    description=str(getattr(req.map, "description", "") or ""),
                )
                if not desired_enabled:
                    self.store.update_map_asset_meta(map_name=map_name, enabled=False)
                    asset = self.store.resolve_map_asset(map_name=map_name, robot_id=self.robot_id) or asset
                active_map = self.store.get_active_map(robot_id=self.robot_id) or active_map
                return OperateMapResponse(
                    success=True,
                    message="imported",
                    map=self._to_msg(asset, include_map_data=False, active_map=active_map),
                    maps=[],
                )
            except Exception as e:
                return self._empty_resp(success=False, message=str(e))

        if op == int(req.modify):
            if not map_name:
                return self._empty_resp(success=False, message="map_name is required for modify")
            current_asset = self.store.resolve_map_asset(
                map_name=map_name,
                robot_id=self.robot_id,
            )
            if not current_asset:
                return self._empty_resp(success=False, message="map asset not found")
            desired_enabled = self._enabled_from_request(
                req,
                current_enabled=bool(current_asset.get("enabled", True)),
                default_enabled=True,
            )
            if bool(req.set_active) and (not desired_enabled):
                return self._empty_resp(success=False, message="cannot set_active on a disabled map")
            self.store.update_map_asset_meta(
                map_name=map_name,
                display_name=map_name,
                description=(req.map.description if str(req.map.description or "").strip() else None),
                enabled=desired_enabled,
            )
            if bool(req.set_active):
                asset = self.store.resolve_map_asset(
                    map_name=map_name,
                    robot_id=self.robot_id,
                )
                if not asset:
                    return self._empty_resp(success=False, message="map asset not found for activation")
                if not bool(asset.get("enabled", True)):
                    return self._empty_resp(success=False, message="cannot activate a disabled map")
                self.store.set_active_map(
                    map_name=str(asset.get("map_name") or ""),
                    robot_id=self.robot_id,
                )
                active_map = asset
                self._set_current_map_params(asset)
            asset = self.store.resolve_map_asset(
                map_name=map_name,
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

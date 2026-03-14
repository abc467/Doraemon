#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import shutil
import time

import rospy
from nav_msgs.msg import OccupancyGrid
from std_srvs.srv import Trigger

from my_msg_srv.srv import MappingControl, MappingControlResponse

from coverage_planner.map_io import compute_occupancy_grid_md5, origin_to_jsonable, write_occupancy_to_yaml_pgm
from coverage_planner.plan_store.store import PlanStore


def _utc_version() -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())


class MapAssetManagerNode:
    def __init__(self):
        self.plan_db_path = rospy.get_param("~plan_db_path", "/data/coverage/planning.db")
        self.maps_root = os.path.expanduser(str(rospy.get_param("~maps_root", "/data/maps")))
        self.map_topic = str(rospy.get_param("~map_topic", "/map"))
        self.map_timeout_s = float(rospy.get_param("~map_timeout_s", 3.0))
        self.robot_id = str(rospy.get_param("~robot_id", "local_robot"))
        self.pbstream_source_path = os.path.expanduser(str(rospy.get_param("~pbstream_source_path", "")))
        self.runtime_ns = str(rospy.get_param("~runtime_ns", "/cartographer/runtime")).rstrip("/")
        self.cartographer_save_state_service = str(
            rospy.get_param("~cartographer_save_state_service", "/cartographer/runtime/save_state")
        ).strip()
        self.save_state_timeout_s = max(1.0, float(rospy.get_param("~save_state_timeout_s", 60.0)))
        self.require_pbstream_for_save = bool(rospy.get_param("~require_pbstream_for_save", True))
        self.set_active_on_save = bool(rospy.get_param("~set_active_on_save", True))
        self.mapping_start_service = str(rospy.get_param("~mapping_start_service", "")).strip()
        self.mapping_stop_service = str(rospy.get_param("~mapping_stop_service", "")).strip()

        os.makedirs(self.maps_root, exist_ok=True)
        self.store = PlanStore(self.plan_db_path)
        self._save_state_cli = rospy.ServiceProxy(self.cartographer_save_state_service, Trigger)
        self.srv = rospy.Service("/clean_robot_server/mapping_server", MappingControl, self._handle_mapping_control)
        rospy.loginfo("[map_asset_manager] ready maps_root=%s db=%s", self.maps_root, self.plan_db_path)

    def _runtime_param(self, key: str) -> str:
        return self.runtime_ns + "/" + str(key or "").strip()

    def _save_pbstream(self, out_dir: str) -> str:
        pbstream_dst = os.path.join(out_dir, "map.pbstream")
        try:
            rospy.set_param(self._runtime_param("save_state_filename"), pbstream_dst)
            rospy.set_param(self._runtime_param("save_state_unfinished"), True)
            rospy.wait_for_service(self.cartographer_save_state_service, timeout=1.0)
            resp = self._save_state_cli()
            if getattr(resp, "success", False) and os.path.exists(pbstream_dst):
                return pbstream_dst
            rospy.logwarn(
                "[map_asset_manager] save_state service returned failure: service=%s msg=%s",
                self.cartographer_save_state_service,
                str(getattr(resp, "message", "") or ""),
            )
        except Exception as e:
            rospy.logwarn("[map_asset_manager] save_state service unavailable: %s", str(e))

        if self.pbstream_source_path and os.path.exists(self.pbstream_source_path):
            shutil.copy2(self.pbstream_source_path, pbstream_dst)
            return pbstream_dst
        return ""

    def _call_optional_trigger(self, srv_name: str, label: str):
        if not srv_name:
            return False, "%s service not configured" % label
        try:
            rospy.wait_for_service(srv_name, timeout=1.0)
            cli = rospy.ServiceProxy(srv_name, Trigger)
            resp = cli()
            return bool(resp.success), str(resp.message or "")
        except Exception as e:
            return False, str(e)

    def _handle_mapping_control(self, req):
        op = int(req.operation)
        if op == int(req.start_mapping):
            ok, msg = self._call_optional_trigger(self.mapping_start_service, "start_mapping")
            return MappingControlResponse(success=bool(ok), message=str(msg))
        if op == int(req.stop_mapping):
            ok, msg = self._call_optional_trigger(self.mapping_stop_service, "stop_mapping")
            return MappingControlResponse(success=bool(ok), message=str(msg))
        if op != int(req.save_map):
            return MappingControlResponse(success=False, message="unsupported operation=%s" % op)

        map_name = str(req.map_name or "").strip()
        if not map_name:
            return MappingControlResponse(success=False, message="map_name is required for save_map")

        try:
            occ = rospy.wait_for_message(self.map_topic, OccupancyGrid, timeout=float(self.map_timeout_s))
        except Exception as e:
            return MappingControlResponse(success=False, message="failed to read %s: %s" % (self.map_topic, e))

        map_md5 = compute_occupancy_grid_md5(occ)
        map_id = "map_%s" % str(map_md5)[:8]
        map_version = _utc_version()
        out_dir = os.path.join(self.maps_root, map_name, map_version)
        os.makedirs(out_dir, exist_ok=True)

        pgm_path, yaml_path = write_occupancy_to_yaml_pgm(occ, out_dir, base_name="map")
        pbstream_dst = self._save_pbstream(out_dir)
        if self.require_pbstream_for_save and not pbstream_dst:
            return MappingControlResponse(
                success=False,
                message="failed to save pbstream for map asset",
            )

        self.store.register_map_asset_version(
            map_name=map_name,
            map_version=map_version,
            map_id=map_id,
            map_md5=map_md5,
            yaml_path=yaml_path,
            pgm_path=pgm_path,
            pbstream_path=pbstream_dst,
            frame_id=str(occ.header.frame_id or "map"),
            resolution=float(occ.info.resolution or 0.0),
            origin=origin_to_jsonable(occ),
            display_name=map_name,
            robot_id=self.robot_id,
            set_active=self.set_active_on_save,
        )

        msg = "saved map_name=%s map_version=%s map_id=%s yaml=%s" % (map_name, map_version, map_id, yaml_path)
        rospy.loginfo("[map_asset_manager] %s", msg)
        return MappingControlResponse(success=True, message=msg)


def main():
    rospy.init_node("map_asset_manager", anonymous=False)
    MapAssetManagerNode()
    rospy.spin()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import shutil

import rospy

from cleanrobot_app_msgs.msg import PgmData as AppPgmData
from cleanrobot_app_msgs.srv import (
    OperateMap as AppOperateMap,
    OperateMapResponse as AppOperateMapResponse,
)
from coverage_planner.map_asset_import import register_imported_map_asset
from coverage_planner.map_io import write_occupancy_to_yaml_pgm, yaml_pgm_to_occupancy
from coverage_planner.plan_store.store import PlanStore
from coverage_planner.ros_contract import build_contract_report, validate_ros_contract
from coverage_planner.service_mode import publish_contract_param

PgmData = AppPgmData
OperateMap = AppOperateMap
OperateMapResponse = AppOperateMapResponse

_PGM_DATA_FIELDS = [
    "map_name",
    "map_revision_id",
    "description",
    "enabled",
    "map_id",
    "map_md5",
    "lifecycle_status",
    "verification_status",
    "is_active",
    "is_latest_head",
    "has_newer_head_revision",
    "active_revision_id",
    "latest_head_revision_id",
    "latest_head_lifecycle_status",
    "latest_head_verification_status",
    "map_data",
]
_OPERATE_MAP_REQUEST_FIELDS = ["operation", "map_name", "map", "set_active", "enabled_state"]
_OPERATE_MAP_REQUEST_CONSTANTS = ["ENABLE_KEEP", "ENABLE_DISABLE", "ENABLE_ENABLE", "get", "add", "modify", "Delete", "getAll"]
_OPERATE_MAP_RESPONSE_FIELDS = ["success", "message", "map", "maps"]


class MapAssetServiceNode:
    ACTIVE_SWITCH_REJECT_MESSAGE = (
        "direct map activation is disabled; use the SLAM submit workflow to verify and activate a map revision"
    )

    def __init__(self):
        self.plan_db_path = rospy.get_param("~plan_db_path", "/data/coverage/planning.db")
        self.robot_id = str(rospy.get_param("~robot_id", "local_robot"))
        self.app_service_name = str(
            rospy.get_param("~app_service_name", "/clean_robot_server/app/map_server")
        ).strip() or "/clean_robot_server/app/map_server"
        self.app_contract_param_ns = str(
            rospy.get_param("~app_contract_param_ns", "/clean_robot_server/contracts/app/map_server")
        ).strip() or "/clean_robot_server/contracts/app/map_server"
        self.maps_root = os.path.expanduser(str(rospy.get_param("~maps_root", "/data/maps")))
        self.external_maps_root = os.path.expanduser(
            str(rospy.get_param("~external_maps_root", "/data/maps/imports")).strip() or "/data/maps/imports"
        )
        os.makedirs(self.external_maps_root, exist_ok=True)
        self.map_topic = str(rospy.get_param("~map_topic", "/map")).strip() or "/map"
        self.map_timeout_s = max(0.5, float(rospy.get_param("~map_timeout_s", 5.0)))
        os.makedirs(self.maps_root, exist_ok=True)
        self.store = PlanStore(self.plan_db_path)
        self._app_contract_report = self._prepare_app_contract_report()
        self.srv = None
        self.app_srv = rospy.Service(self.app_service_name, AppOperateMap, self._handle_app)
        publish_contract_param(rospy, self.app_contract_param_ns, self._app_contract_report, enabled=True)
        rospy.loginfo(
            "[map_asset_service] ready app_service=%s db=%s external_maps_root=%s app_contract=%s",
            self.app_service_name,
            self.plan_db_path,
            self.external_maps_root,
            self.app_contract_param_ns or "-",
        )

    def _prepare_app_contract_report(self):
        validate_ros_contract("AppPgmData", AppPgmData, required_fields=_PGM_DATA_FIELDS)
        validate_ros_contract(
            "AppOperateMapRequest",
            AppOperateMap._request_class,
            required_fields=_OPERATE_MAP_REQUEST_FIELDS,
            required_constants=_OPERATE_MAP_REQUEST_CONSTANTS,
        )
        validate_ros_contract(
            "AppOperateMapResponse",
            AppOperateMapResponse,
            required_fields=_OPERATE_MAP_RESPONSE_FIELDS,
        )
        return build_contract_report(
            service_name=self.app_service_name,
            contract_name="map_server_app",
            service_cls=AppOperateMap,
            request_cls=AppOperateMap._request_class,
            response_cls=AppOperateMapResponse,
            dependencies={"map": AppPgmData},
            features=[
                "map_asset_revision_view",
                "import_as_candidate_revision",
                "verified_head_vs_active_revision_projection",
                "cleanrobot_app_msgs_parallel",
            ],
        )

    def _empty_resp(self, *, success: bool, message: str, response_cls=AppOperateMapResponse, map_cls=AppPgmData):
        return response_cls(success=bool(success), message=str(message or ""), map=map_cls(), maps=[])

    def _resolve_req_name(self, req):
        map_name = str(req.map_name or "").strip()
        if (not map_name) and getattr(req, "map", None):
            map_name = str(req.map.map_name or "").strip()
        if map_name.endswith(".pbstream"):
            map_name = map_name[:-len(".pbstream")]
        return map_name

    def _resolve_req_revision_id(self, req):
        if not getattr(req, "map", None):
            return ""
        return str(getattr(req.map, "map_revision_id", "") or "").strip()

    def _target_paths(self, map_name: str, revision_id: str = ""):
        normalized_map_name = str(map_name or "").strip()
        normalized_revision_id = str(revision_id or "").strip()
        base_dir = self.maps_root
        if normalized_revision_id:
            base_dir = os.path.join(
                self.maps_root,
                "revisions",
                normalized_map_name,
                normalized_revision_id,
            )
        base = os.path.join(base_dir, normalized_map_name)
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

    def _latest_head_asset(self, map_name: str):
        normalized_map_name = str(map_name or "").strip()
        if not normalized_map_name:
            return {}
        return self.store.resolve_map_asset(
            map_name=normalized_map_name,
            robot_id=self.robot_id,
        ) or {}

    def _preferred_query_asset(self, *, map_name: str, map_revision_id: str = "", active_map=None):
        normalized_map_name = str(map_name or "").strip()
        normalized_revision_id = str(map_revision_id or "").strip()
        active_map = active_map or {}
        if normalized_revision_id:
            return self.store.resolve_map_asset(
                map_name=normalized_map_name,
                revision_id=normalized_revision_id,
                robot_id=self.robot_id,
            )
        if normalized_map_name:
            resolve_revision = getattr(self.store, "resolve_map_revision", None)
            if callable(resolve_revision):
                preferred_revision = resolve_revision(
                    map_name=normalized_map_name,
                    robot_id=self.robot_id,
                )
                if preferred_revision is not None:
                    return preferred_revision
            active_map_name = str(active_map.get("map_name") or "").strip()
            active_revision_id = str(active_map.get("revision_id") or "").strip()
            if active_revision_id and active_map_name == normalized_map_name:
                active_revision = self.store.resolve_map_asset(
                    revision_id=active_revision_id,
                    robot_id=self.robot_id,
                )
                if active_revision is not None:
                    return active_revision
                return active_map or None
            return self.store.resolve_map_asset(
                map_name=normalized_map_name,
                robot_id=self.robot_id,
            )
        return active_map or self.store.get_active_map(robot_id=self.robot_id)

    def _import_external_map(self, *, map_name: str, set_active: bool, description: str = ""):
        map_name = str(map_name or "").strip()
        if not map_name:
            raise ValueError("map_name is required")
        description = str(description or "")

        source_pbstream = os.path.join(self.external_maps_root, map_name + ".pbstream")
        if not os.path.exists(source_pbstream):
            raise FileNotFoundError("external pbstream not found: %s" % source_pbstream)
        source_yaml = os.path.join(self.external_maps_root, map_name + ".yaml")
        if not os.path.exists(source_yaml):
            raise FileNotFoundError("external yaml not found: %s" % source_yaml)

        revision_id = self.store.generate_map_revision_id(map_name)
        target_paths = self._target_paths(map_name, revision_id=revision_id)
        for path in target_paths.values():
            if os.path.exists(path):
                raise ValueError("target asset path already exists: %s" % path)

        occ = yaml_pgm_to_occupancy(source_yaml)
        pgm_path = ""
        yaml_path = ""
        pbstream_path = target_paths["pbstream_path"]
        try:
            artifact_dir = os.path.dirname(pbstream_path)
            if artifact_dir:
                os.makedirs(artifact_dir, exist_ok=True)
            pgm_path, yaml_path = write_occupancy_to_yaml_pgm(
                occ,
                artifact_dir or self.maps_root,
                base_name=map_name,
            )
            shutil.copy2(source_pbstream, pbstream_path)
            asset, _snapshot_md5 = register_imported_map_asset(
                self.store,
                map_name=map_name,
                occ=occ,
                yaml_path=yaml_path,
                pgm_path=pgm_path,
                pbstream_path=pbstream_path,
                display_name=map_name,
                description=description,
                enabled=True,
                robot_id=self.robot_id,
                verification_mode="candidate",
                set_active=False,
                revision_id=revision_id,
            )
        except Exception:
            self._cleanup_paths(pgm_path, yaml_path, pbstream_path)
            raise

        return asset or {
            "map_name": map_name,
            "map_id": "",
            "map_md5": "",
            "yaml_path": yaml_path,
            "pgm_path": pgm_path,
            "pbstream_path": pbstream_path,
            "enabled": True,
            "lifecycle_status": "saved_unverified",
            "verification_status": "pending",
        }

    def _to_msg(
        self,
        asset: dict,
        *,
        include_map_data: bool = False,
        active_map=None,
        latest_head=None,
        msg_cls=AppPgmData,
    ):
        msg = msg_cls()
        if not asset:
            return msg
        active_map = active_map or {}
        latest_head = latest_head or self._latest_head_asset(str(asset.get("map_name") or ""))
        msg.map_name = str(asset.get("map_name") or "")
        msg.map_revision_id = str(asset.get("revision_id") or "")
        msg.description = str(asset.get("description") or "")
        msg.enabled = bool(asset.get("enabled", True))
        msg.map_id = str(asset.get("map_id") or "")
        msg.map_md5 = str(asset.get("map_md5") or "")
        msg.lifecycle_status = str(asset.get("lifecycle_status") or "")
        msg.verification_status = str(asset.get("verification_status") or "")
        active_revision_id = str(active_map.get("revision_id") or "").strip()
        active_map_name = str(active_map.get("map_name") or "").strip()
        latest_head_revision_id = str(latest_head.get("revision_id") or asset.get("revision_id") or "").strip()
        if active_revision_id and msg.map_revision_id:
            msg.is_active = msg.map_revision_id == active_revision_id
        else:
            msg.is_active = msg.map_name == active_map_name
        if latest_head_revision_id and msg.map_revision_id:
            msg.is_latest_head = msg.map_revision_id == latest_head_revision_id
        else:
            msg.is_latest_head = True
        msg.active_revision_id = active_revision_id if active_map_name == msg.map_name else ""
        msg.latest_head_revision_id = latest_head_revision_id
        msg.latest_head_lifecycle_status = str(latest_head.get("lifecycle_status") or asset.get("lifecycle_status") or "")
        msg.latest_head_verification_status = str(
            latest_head.get("verification_status") or asset.get("verification_status") or ""
        )
        msg.has_newer_head_revision = bool(
            msg.active_revision_id and msg.latest_head_revision_id and msg.active_revision_id != msg.latest_head_revision_id
        )
        if include_map_data:
            yaml_path = str(asset.get("yaml_path") or "").strip()
            if yaml_path:
                msg.map_data = yaml_pgm_to_occupancy(yaml_path)
        return msg

    def _list_map_views(self, *, msg_cls=AppPgmData):
        active_map = self.store.get_active_map(robot_id=self.robot_id) or {}
        out = []
        seen = set()
        for head_asset in self.store.list_map_assets():
            name = str(head_asset.get("map_name") or "")
            if not name or name in seen:
                continue
            seen.add(name)
            view_asset = self._preferred_query_asset(
                map_name=name,
                map_revision_id="",
                active_map=active_map,
            ) or head_asset
            out.append(
                self._to_msg(
                    view_asset,
                    include_map_data=False,
                    active_map=active_map,
                    latest_head=head_asset,
                    msg_cls=msg_cls,
                )
            )
        active_map_name = str(active_map.get("map_name") or "").strip()
        if active_map_name and active_map_name not in seen:
            head_asset = self._latest_head_asset(active_map_name) or active_map
            out.append(
                self._to_msg(
                    active_map,
                    include_map_data=False,
                    active_map=active_map,
                    latest_head=head_asset,
                    msg_cls=msg_cls,
                )
            )
        return out

    def _handle(self, req, *, response_cls=AppOperateMapResponse, map_cls=AppPgmData):
        op = int(req.operation)
        map_name = self._resolve_req_name(req)
        map_revision_id = self._resolve_req_revision_id(req)
        active_map = self.store.get_active_map(robot_id=self.robot_id) or {}

        if op == int(req.getAll):
            return response_cls(success=True, message="ok", map=map_cls(), maps=self._list_map_views(msg_cls=map_cls))

        if op == int(req.get):
            asset = self._preferred_query_asset(
                map_name=map_name,
                map_revision_id=map_revision_id,
                active_map=active_map,
            )
            if not asset:
                return self._empty_resp(success=False, message="map asset not found", response_cls=response_cls, map_cls=map_cls)
            latest_head = self._latest_head_asset(str(asset.get("map_name") or "")) or asset
            return response_cls(
                success=True,
                message="ok",
                map=self._to_msg(
                    asset,
                    include_map_data=True,
                    active_map=active_map,
                    latest_head=latest_head,
                    msg_cls=map_cls,
                ),
                maps=[],
            )

        if op == int(req.add):
            if not map_name:
                return self._empty_resp(success=False, message="map_name is required for add", response_cls=response_cls, map_cls=map_cls)
            try:
                desired_enabled = self._enabled_from_request(req, current_enabled=None, default_enabled=True)
                if bool(req.set_active):
                    return self._empty_resp(success=False, message=self.ACTIVE_SWITCH_REJECT_MESSAGE, response_cls=response_cls, map_cls=map_cls)
                if bool(req.set_active) and (not desired_enabled):
                    return self._empty_resp(success=False, message="cannot set_active on a disabled map", response_cls=response_cls, map_cls=map_cls)
                asset = self._import_external_map(
                    map_name=map_name,
                    set_active=False,
                    description=str(getattr(req.map, "description", "") or ""),
                )
                if not desired_enabled:
                    asset = self.store.update_map_revision_meta(
                        revision_id=str(asset.get("revision_id") or ""),
                        robot_id=self.robot_id,
                        enabled=False,
                    )
                active_map = self.store.get_active_map(robot_id=self.robot_id) or active_map
                return response_cls(
                    success=True,
                    message="imported as unverified candidate revision",
                    map=self._to_msg(asset, include_map_data=False, active_map=active_map, msg_cls=map_cls),
                    maps=[],
                )
            except Exception as e:
                return self._empty_resp(success=False, message=str(e), response_cls=response_cls, map_cls=map_cls)

        if op == int(req.modify):
            if (not map_name) and (not map_revision_id):
                return self._empty_resp(success=False, message="map_name or map_revision_id is required for modify", response_cls=response_cls, map_cls=map_cls)
            current_asset = self._preferred_query_asset(
                map_name=map_name,
                map_revision_id=map_revision_id,
                active_map=active_map,
            )
            if not current_asset:
                return self._empty_resp(success=False, message="map revision not found", response_cls=response_cls, map_cls=map_cls)
            desired_enabled = self._enabled_from_request(
                req,
                current_enabled=bool(current_asset.get("enabled", True)),
                default_enabled=True,
            )
            if bool(req.set_active) and (not desired_enabled):
                return self._empty_resp(success=False, message="cannot set_active on a disabled map", response_cls=response_cls, map_cls=map_cls)
            if bool(req.set_active):
                return self._empty_resp(success=False, message=self.ACTIVE_SWITCH_REJECT_MESSAGE, response_cls=response_cls, map_cls=map_cls)
            asset = self.store.update_map_revision_meta(
                revision_id=str(current_asset.get("revision_id") or map_revision_id or "").strip(),
                map_name=str(current_asset.get("map_name") or map_name),
                robot_id=self.robot_id,
                display_name=str(current_asset.get("map_name") or map_name),
                description=(req.map.description if str(req.map.description or "").strip() else None),
                enabled=desired_enabled,
            )
            active_map = self.store.get_active_map(robot_id=self.robot_id) or active_map
            latest_head = self._latest_head_asset(str(asset.get("map_name") or map_name)) or asset
            return response_cls(
                success=True,
                message="updated",
                map=self._to_msg(asset or {}, include_map_data=False, active_map=active_map, latest_head=latest_head, msg_cls=map_cls),
                maps=[],
            )

        if op == int(req.Delete):
            if (not map_name) and (not map_revision_id):
                return self._empty_resp(success=False, message="map_name or map_revision_id is required for Delete", response_cls=response_cls, map_cls=map_cls)
            target_asset = self._preferred_query_asset(
                map_name=map_name,
                map_revision_id=map_revision_id,
                active_map=active_map,
            )
            if not target_asset:
                return self._empty_resp(success=False, message="map revision not found", response_cls=response_cls, map_cls=map_cls)
            if (
                str(active_map.get("revision_id") or "").strip()
                and str(target_asset.get("revision_id") or "").strip() == str(active_map.get("revision_id") or "").strip()
            ) or (
                (not str(active_map.get("revision_id") or "").strip())
                and str(active_map.get("map_name") or "").strip() == str(target_asset.get("map_name") or "").strip()
            ):
                return self._empty_resp(success=False, message="cannot disable the active map", response_cls=response_cls, map_cls=map_cls)
            asset = self.store.disable_map_revision(
                revision_id=str(target_asset.get("revision_id") or map_revision_id or "").strip(),
                map_name=str(target_asset.get("map_name") or map_name),
                robot_id=self.robot_id,
            )
            latest_head = self._latest_head_asset(str(asset.get("map_name") or map_name)) or asset
            return response_cls(
                success=True,
                message="disabled",
                map=self._to_msg(asset or {}, include_map_data=False, active_map=active_map, latest_head=latest_head, msg_cls=map_cls),
                maps=[],
            )

        return self._empty_resp(success=False, message="unsupported operation=%s" % op, response_cls=response_cls, map_cls=map_cls)

    def _handle_app(self, req):
        return self._handle(req, response_cls=AppOperateMapResponse, map_cls=AppPgmData)


def main():
    rospy.init_node("map_asset_service", anonymous=False)
    MapAssetServiceNode()
    rospy.spin()


if __name__ == "__main__":
    main()

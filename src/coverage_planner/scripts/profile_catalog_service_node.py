#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
from typing import List

import rospy

from cleanrobot_app_msgs.msg import ProfileOption as AppProfileOption
from cleanrobot_app_msgs.srv import (
    GetProfileCatalog as AppGetProfileCatalog,
    GetProfileCatalogResponse as AppGetProfileCatalogResponse,
)
from coverage_planner.ros_contract import build_contract_report, validate_ros_contract
from coverage_planner.ops_store.store import OperationsStore
from coverage_planner.plan_store.store import PlanStore
from coverage_planner.service_mode import publish_contract_param

def _normalize_map_name(map_name: str) -> str:
    value = str(map_name or "").strip()
    if value.endswith(".pbstream"):
        value = value[:-len(".pbstream")]
    return str(value or "").strip()


class ProfileCatalogServiceNode:
    def __init__(self):
        self.plan_db_path = os.path.expanduser(str(rospy.get_param("~plan_db_path", "/data/coverage/planning.db")))
        self.ops_db_path = os.path.expanduser(str(rospy.get_param("~ops_db_path", "/data/coverage/operations.db")))
        self.app_service_name = str(
            rospy.get_param("~app_service_name", "/database_server/app/profile_catalog_service")
        ).strip() or "/database_server/app/profile_catalog_service"
        self.app_contract_param_ns = str(
            rospy.get_param(
                "~app_contract_param_ns",
                "/database_server/contracts/app/profile_catalog_service",
            )
        ).strip() or "/database_server/contracts/app/profile_catalog_service"
        self.default_plan_profile_name = str(
            rospy.get_param("~default_plan_profile_name", "cover_standard")
        ).strip() or "cover_standard"
        self.default_sys_profile_name = str(
            rospy.get_param("~default_sys_profile_name", "standard")
        ).strip() or "standard"

        validate_ros_contract(
            "AppProfileOption",
            AppProfileOption,
            required_fields=[
                "profile_name",
                "display_name",
                "profile_kind",
                "enabled",
                "is_default",
                "description",
                "version",
                "tags",
                "supported_clean_modes",
                "supported_maps",
                "warnings",
            ],
        )
        validate_ros_contract(
            "AppGetProfileCatalogRequest",
            AppGetProfileCatalog._request_class,
            required_fields=["profile_kind", "include_disabled", "map_name"],
        )
        validate_ros_contract(
            "AppGetProfileCatalogResponse",
            AppGetProfileCatalogResponse,
            required_fields=["success", "message", "profiles"],
        )

        self.plan_store = PlanStore(self.plan_db_path)
        self.ops_store = OperationsStore(self.ops_db_path)
        self._app_contract = build_contract_report(
            service_name=self.app_service_name,
            contract_name="profile_catalog_service_app",
            service_cls=AppGetProfileCatalog,
            request_cls=AppGetProfileCatalog._request_class,
            response_cls=AppGetProfileCatalogResponse,
            dependencies={"profile": AppProfileOption},
            features=["plan_profile_catalog", "sys_profile_catalog", "cleanrobot_app_msgs_parallel"],
        )
        publish_contract_param(rospy, self.app_contract_param_ns, self._app_contract, enabled=True)
        self.srv = None
        self.app_srv = rospy.Service(self.app_service_name, AppGetProfileCatalog, self._handle_app)
        rospy.loginfo(
            "[profile_catalog_service] ready app_service=%s app_contract=%s plan_db=%s ops_db=%s",
            self.app_service_name,
            self.app_contract_param_ns or "-",
            self.plan_db_path,
            self.ops_db_path,
        )

    def _resp_app(
        self,
        *,
        success: bool,
        message: str,
        profiles: List[AppProfileOption] = None,
    ) -> AppGetProfileCatalogResponse:
        return AppGetProfileCatalogResponse(
            success=bool(success),
            message=str(message or ""),
            profiles=list(profiles or []),
        )

    def _plan_profile_msgs(self, *, include_disabled: bool) -> List[AppProfileOption]:
        supported_maps_by_profile = self.plan_store.list_plan_profile_supported_maps()
        out: List[AppProfileOption] = []
        for rec in self.plan_store.list_plan_profiles():
            enabled = bool(rec.get("enabled", False))
            if (not include_disabled) and (not enabled):
                continue
            name = str(rec.get("plan_profile_name") or "").strip()
            if not name:
                continue
            msg = AppProfileOption()
            msg.profile_name = name
            msg.display_name = name
            msg.profile_kind = "plan"
            msg.enabled = enabled
            msg.is_default = (name == self.default_plan_profile_name)
            msg.description = str(rec.get("description") or "")
            msg.version = ""
            usage_type = str(rec.get("usage_type") or "").strip()
            msg.tags = [usage_type] if usage_type else []
            msg.supported_clean_modes = []
            msg.supported_maps = list(supported_maps_by_profile.get(name, []) or [])
            msg.warnings = []
            if not enabled:
                msg.warnings.append("disabled profile")
            out.append(msg)
        return out

    def _sys_profile_msgs(self, *, include_disabled: bool) -> List[AppProfileOption]:
        out: List[AppProfileOption] = []
        for rec in self.ops_store.list_sys_profiles():
            enabled = bool(rec.enabled)
            if (not include_disabled) and (not enabled):
                continue
            name = str(rec.sys_profile_name or "").strip()
            if not name:
                continue
            msg = AppProfileOption()
            msg.profile_name = name
            msg.display_name = name
            msg.profile_kind = "sys"
            msg.enabled = enabled
            msg.is_default = (name == self.default_sys_profile_name)
            desc_parts = []
            if rec.mbf_controller_name:
                desc_parts.append("controller=%s" % str(rec.mbf_controller_name))
            if rec.actuator_profile_name:
                desc_parts.append("actuator=%s" % str(rec.actuator_profile_name))
            msg.description = ", ".join(desc_parts)
            msg.version = ""
            tags = []
            if rec.mbf_controller_name:
                tags.append("controller:%s" % str(rec.mbf_controller_name))
            if rec.actuator_profile_name:
                tags.append("actuator:%s" % str(rec.actuator_profile_name))
            msg.tags = tags
            msg.supported_clean_modes = [str(rec.default_clean_mode)] if str(rec.default_clean_mode or "").strip() else []
            msg.supported_maps = []
            msg.warnings = []
            if not enabled:
                msg.warnings.append("disabled profile")
            out.append(msg)
        return out

    def _collect_profiles(self, req):
        try:
            profile_kind = str(req.profile_kind or "").strip().lower()
            include_disabled = bool(req.include_disabled)
            _normalize_map_name(req.map_name)  # reserved for future map-scoped filtering
            if profile_kind not in ("", "plan", "sys"):
                return False, "invalid profile_kind=%s" % str(req.profile_kind or ""), []

            profiles: List[AppProfileOption] = []
            if profile_kind in ("", "plan"):
                profiles.extend(self._plan_profile_msgs(include_disabled=include_disabled))
            if profile_kind in ("", "sys"):
                profiles.extend(self._sys_profile_msgs(include_disabled=include_disabled))

            profiles.sort(key=lambda p: (str(p.profile_kind), str(p.profile_name)))
            return True, "ok", profiles
        except Exception as e:
            rospy.logerr("[profile_catalog_service] request failed: %s", str(e))
            return False, str(e), []

    def _handle_app(self, req):
        success, message, profiles = self._collect_profiles(req)
        return self._resp_app(success=success, message=message, profiles=profiles)


if __name__ == "__main__":
    rospy.init_node("profile_catalog_service")
    node = ProfileCatalogServiceNode()
    rospy.spin()

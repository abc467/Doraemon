#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
from typing import List

import rospy

from coverage_planner.ops_store.store import OperationsStore
from coverage_planner.plan_store.store import PlanStore
from my_msg_srv.msg import ProfileOption
from my_msg_srv.srv import GetProfileCatalog, GetProfileCatalogResponse


def _normalize_map_name(map_name: str) -> str:
    value = str(map_name or "").strip()
    if value.endswith(".pbstream"):
        value = value[:-len(".pbstream")]
    return str(value or "").strip()


class ProfileCatalogServiceNode:
    def __init__(self):
        self.plan_db_path = os.path.expanduser(str(rospy.get_param("~plan_db_path", "/data/coverage/planning.db")))
        self.ops_db_path = os.path.expanduser(str(rospy.get_param("~ops_db_path", "/data/coverage/operations.db")))
        self.service_name = str(
            rospy.get_param("~service_name", "/database_server/profile_catalog_service")
        ).strip() or "/database_server/profile_catalog_service"
        self.default_plan_profile_name = str(
            rospy.get_param("~default_plan_profile_name", rospy.get_param("~default_profile_name", "cover_standard"))
        ).strip() or "cover_standard"
        self.default_sys_profile_name = str(
            rospy.get_param("~default_sys_profile_name", "standard")
        ).strip() or "standard"

        self.plan_store = PlanStore(self.plan_db_path)
        self.ops_store = OperationsStore(self.ops_db_path)
        self.srv = rospy.Service(self.service_name, GetProfileCatalog, self._handle)
        rospy.loginfo(
            "[profile_catalog_service] ready service=%s plan_db=%s ops_db=%s",
            self.service_name,
            self.plan_db_path,
            self.ops_db_path,
        )

    def _resp(self, *, success: bool, message: str, profiles: List[ProfileOption] = None) -> GetProfileCatalogResponse:
        return GetProfileCatalogResponse(
            success=bool(success),
            message=str(message or ""),
            profiles=list(profiles or []),
        )

    def _plan_profile_msgs(self, *, include_disabled: bool) -> List[ProfileOption]:
        supported_maps_by_profile = self.plan_store.list_plan_profile_supported_maps()
        out: List[ProfileOption] = []
        for rec in self.plan_store.list_plan_profiles():
            enabled = bool(rec.get("enabled", False))
            if (not include_disabled) and (not enabled):
                continue
            name = str(rec.get("plan_profile_name") or "").strip()
            if not name:
                continue
            msg = ProfileOption()
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

    def _sys_profile_msgs(self, *, include_disabled: bool) -> List[ProfileOption]:
        out: List[ProfileOption] = []
        for rec in self.ops_store.list_sys_profiles():
            enabled = bool(rec.enabled)
            if (not include_disabled) and (not enabled):
                continue
            name = str(rec.sys_profile_name or "").strip()
            if not name:
                continue
            msg = ProfileOption()
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

    def _handle(self, req):
        try:
            profile_kind = str(req.profile_kind or "").strip().lower()
            include_disabled = bool(req.include_disabled)
            _normalize_map_name(req.map_name)  # reserved for future map-scoped filtering
            if profile_kind not in ("", "plan", "sys"):
                return self._resp(success=False, message="invalid profile_kind=%s" % str(req.profile_kind or ""))

            profiles: List[ProfileOption] = []
            if profile_kind in ("", "plan"):
                profiles.extend(self._plan_profile_msgs(include_disabled=include_disabled))
            if profile_kind in ("", "sys"):
                profiles.extend(self._sys_profile_msgs(include_disabled=include_disabled))

            profiles.sort(key=lambda p: (str(p.profile_kind), str(p.profile_name)))
            return self._resp(success=True, message="ok", profiles=profiles)
        except Exception as e:
            rospy.logerr("[profile_catalog_service] request failed: %s", str(e))
            return self._resp(success=False, message=str(e))


if __name__ == "__main__":
    rospy.init_node("profile_catalog_service")
    node = ProfileCatalogServiceNode()
    rospy.spin()

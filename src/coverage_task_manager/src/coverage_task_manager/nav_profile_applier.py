# -*- coding: utf-8 -*-

"""Navigation/controller profile applier (ROS1).

This module is intentionally simple and stable:
  - load a rosparam-style YAML file into a target namespace
  - optionally call a reload service (std_srvs/Trigger)
  - no dynamic reconfigure dependency

Why:
  You said your 'heavy/standard/eco' modes are essentially switching among
  multiple MPPI controller parameter YAMLs. In many commercial robots, this is
  owned by the Task layer (or a SystemConfig service) so that:
    - switching happens only at safe times (before start/resume, docking, etc.)
    - actuator profile switching is coordinated with navigation profile switching
"""

from typing import Optional
import os
import time

import rospy
import rosparam
from std_srvs.srv import Trigger


def _expand_path(p: str) -> str:
    p = str(p or "").strip()
    if not p:
        return ""
    return os.path.expanduser(os.path.expandvars(p))


class NavProfileApplier:
    def __init__(self, *, enable: bool = True):
        self.enable = bool(enable)
        self._last_key: str = ""

    def apply(
        self,
        *,
        profile_name: str,
        yaml_path: str,
        namespace: str = "",
        reload_service: str = "",
        wait_service_timeout_s: float = 0.8,
        call_reload: bool = True,
    ) -> bool:
        """Apply a nav profile. Returns True if something was applied."""
        if not self.enable:
            return False

        yaml_path = _expand_path(yaml_path)
        namespace = str(namespace or "").strip()
        reload_service = str(reload_service or "").strip()

        if not yaml_path:
            rospy.logwarn_throttle(2.0, "[TASK/NAVPROF] profile=%s has empty yaml_path; skip", str(profile_name))
            return False

        key = f"{profile_name}|{yaml_path}|{namespace}|{reload_service}"
        if key == self._last_key:
            return False

        if not os.path.exists(yaml_path):
            rospy.logerr("[TASK/NAVPROF] yaml not found: %s (profile=%s)", yaml_path, str(profile_name))
            return False

        # 1) load YAML -> dict list, then upload into param server
        try:
            # rosparam.load_file returns list of (params_dict, ns_from_file)
            loaded = rosparam.load_file(yaml_path, default_namespace=namespace if namespace else None)
            for params, ns in loaded:
                ns_eff = namespace if namespace else ns
                if not ns_eff:
                    ns_eff = "/"
                rosparam.upload_params(ns_eff, params)
            rospy.loginfo("[TASK/NAVPROF] applied profile=%s yaml=%s ns=%s", str(profile_name), yaml_path, namespace or "(from yaml)")
        except Exception as e:
            rospy.logerr("[TASK/NAVPROF] failed to load yaml=%s err=%s", yaml_path, str(e))
            return False

        # 2) optional reload service
        if call_reload and reload_service:
            try:
                rospy.wait_for_service(reload_service, timeout=float(wait_service_timeout_s))
                cli = rospy.ServiceProxy(reload_service, Trigger)
                resp = cli()
                if hasattr(resp, "success") and (not bool(resp.success)):
                    rospy.logwarn("[TASK/NAVPROF] reload_service reported failure: %s msg=%s", reload_service, getattr(resp, "message", ""))
                else:
                    rospy.loginfo("[TASK/NAVPROF] reload_service ok: %s", reload_service)
            except Exception as e:
                rospy.logwarn("[TASK/NAVPROF] reload_service skipped/failed: %s err=%s", reload_service, str(e))

        self._last_key = key
        return True

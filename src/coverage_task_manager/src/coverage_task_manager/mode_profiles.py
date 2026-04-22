# -*- coding: utf-8 -*-

"""Mode/profile catalog for commercial cleaning robots.

Concepts
--------
- sys_profile:
    Operator-facing mode profile, typically one of:
      heavy / standard / eco
    It maps to:
      * MBF controller name
      * cleaning actuator profile name
      * optional default clean_mode (scrub/inspect...)

- plan_profile:
    Coverage plan selection key in SQLite (plans.plan_profile_name), e.g.:
      cover_standard / cover_deep / cover_eco
    It decides which coverage plan (path) to execute.

Design
------
Task layer owns *profile decisions* and applies:
  1) Executor sys_profile via String cmd: set_sys_profile
  2) Executor plan_profile via String cmd: set_plan_profile
  3) Executor clean_mode via String cmd: set_mode

Legacy nav hot-loading fields are still parsed but optional; the first-stage
system uses pre-registered MBF controllers instead of reloading one instance.
"""

from dataclasses import dataclass
from typing import Any, Dict, Optional
import os


def _expand_path(p: str) -> str:
    p = str(p or "").strip()
    if not p:
        return ""
    # support ~ and $ENV
    p = os.path.expanduser(os.path.expandvars(p))
    return p


@dataclass(frozen=True)
class ModeProfile:
    name: str

    # Navigation/controller selection
    mbf_controller_name: str = ""
    actuator_profile_name: str = ""

    # Legacy optional nav hot-load support
    nav_yaml: str = ""             # YAML file path (rosparam-style)
    nav_namespace: str = ""        # where to load YAML on param server
    nav_reload_service: str = ""   # optional std_srvs/Trigger

    # Optional defaults
    default_clean_mode: str = ""    # e.g. scrub / inspect


class ModeProfileCatalog:
    """Load mode profiles from ROS param dict (or YAML loaded into param server).

    Expected structure (example):
      mode_profiles:
        heavy:
          mbf_controller_name: MPPI_Heavy_Controller
          actuator_profile_name: heavy
          default_clean_mode: scrub
        standard: { ... }
        eco: { ... }
    """

    def __init__(self, raw: Any):
        self._raw = raw if isinstance(raw, dict) else {}
        self._cache: Dict[str, ModeProfile] = {}

    def names(self):
        return [str(k) for k, v in self._raw.items() if str(k or "").strip() and isinstance(v, dict)]

    def has(self, name: str) -> bool:
        key = str(name or "").strip()
        return bool(key) and isinstance(self._raw.get(key), dict)

    def get(self, name: str) -> Optional[ModeProfile]:
        key = str(name or "").strip()
        if not key:
            return None
        if key in self._cache:
            return self._cache[key]
        v = self._raw.get(key)
        if not isinstance(v, dict):
            return None

        mp = ModeProfile(
            name=key,
            mbf_controller_name=str(
                v.get("mbf_controller_name")
                or v.get("controller_name")
                or v.get("mbf_controller")
                or ""
            ),
            actuator_profile_name=str(
                v.get("actuator_profile_name")
                or v.get("executor_sys_profile")
                or v.get("sys_profile")
                or key
            ),
            nav_yaml=_expand_path(v.get("nav_yaml") or v.get("nav_yaml_path") or ""),
            nav_namespace=str(v.get("nav_namespace") or v.get("nav_ns") or ""),
            nav_reload_service=str(v.get("nav_reload_service") or v.get("nav_reload_srv") or ""),
            default_clean_mode=str(v.get("default_clean_mode") or v.get("clean_mode") or ""),
        )
        self._cache[key] = mp
        return mp

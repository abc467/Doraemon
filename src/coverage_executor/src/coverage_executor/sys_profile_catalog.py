# -*- coding: utf-8 -*-
from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class SysProfile:
    name: str
    mbf_controller_name: str = ""
    actuator_profile_name: str = ""
    default_clean_mode: str = ""


class SysProfileCatalog:
    def __init__(self, raw: Any):
        self._raw = raw if isinstance(raw, dict) else {}
        self._cache: Dict[str, SysProfile] = {}

    def get(self, name: str) -> Optional[SysProfile]:
        key = str(name or "").strip()
        if not key:
            return None
        if key in self._cache:
            return self._cache[key]

        v = self._raw.get(key)
        if not isinstance(v, dict):
            prof = SysProfile(
                name=key,
                mbf_controller_name=f"MPPI_{key.capitalize()}_Controller",
                actuator_profile_name=key,
            )
            self._cache[key] = prof
            return prof

        prof = SysProfile(
            name=key,
            mbf_controller_name=str(
                v.get("mbf_controller_name")
                or v.get("controller_name")
                or v.get("mbf_controller")
                or ""
            ),
            actuator_profile_name=str(
                v.get("actuator_profile_name")
                or v.get("sys_profile")
                or key
            ),
            default_clean_mode=str(v.get("default_clean_mode") or v.get("clean_mode") or ""),
        )
        self._cache[key] = prof
        return prof

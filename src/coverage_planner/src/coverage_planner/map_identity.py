# -*- coding: utf-8 -*-
"""Map identity utilities (planner side).

See coverage_executor.map_identity for design rationale. This duplicate module
keeps packages decoupled (planner should not depend on executor).
"""

from __future__ import annotations

import hashlib
import struct
from typing import Optional, Tuple

import rospy
from nav_msgs.msg import OccupancyGrid


def compute_map_md5(map_topic: str = "/map", timeout_s: float = 2.0) -> Optional[str]:
    map_topic = (map_topic or "/map").strip() or "/map"
    try:
        msg: OccupancyGrid = rospy.wait_for_message(map_topic, OccupancyGrid, timeout=float(timeout_s))
    except Exception:
        return None

    try:
        info = msg.info
        o = info.origin
        buf = bytearray()
        buf += struct.pack("<II", int(info.width), int(info.height))
        buf += struct.pack("<f", float(info.resolution))
        buf += struct.pack(
            "<ffffff",
            float(o.position.x), float(o.position.y), float(o.position.z),
            float(o.orientation.x), float(o.orientation.y), float(o.orientation.z),
        )
        buf += struct.pack("<f", float(o.orientation.w))
        buf += bytes(((int(v) + 256) & 0xFF) for v in (msg.data or []))
        h = hashlib.md5(); h.update(buf)
        return h.hexdigest()
    except Exception:
        return None


def _get_param_first(keys) -> str:
    for k in keys:
        try:
            v = rospy.get_param(k, "")
            if isinstance(v, str):
                v = v.strip()
            if v:
                return str(v)
        except Exception:
            pass
    return ""


def get_runtime_map_identity() -> Tuple[str, str]:
    map_id = _get_param_first(["~map_id", "/map_id", "/map_uuid"])
    map_md5 = _get_param_first(["~map_md5", "/map_md5", "/map_checksum"])
    return map_id, map_md5


def get_runtime_map_scope() -> Tuple[str, str]:
    map_name = _get_param_first(["~map_name", "/map_name"])
    map_version = _get_param_first(["~map_version", "/map_version"])
    return map_name, map_version


def ensure_map_identity(
    *,
    map_topic: str = "/map",
    timeout_s: float = 2.0,
    set_global_params: bool = True,
    set_private_params: bool = True,
    map_id_prefix: str = "map_",
) -> Tuple[str, str, bool]:
    map_id, map_md5 = get_runtime_map_identity()
    ok = True
    updated = False
    if not map_md5:
        md5 = compute_map_md5(map_topic=map_topic, timeout_s=timeout_s)
        if not md5:
            ok = False
        else:
            map_md5 = md5
            updated = True
    if not map_id:
        if map_md5:
            map_id = f"{map_id_prefix}{str(map_md5)[:8]}"
            updated = True
        else:
            ok = False
    if updated and ok:
        try:
            if set_global_params:
                rospy.set_param("/map_id", str(map_id))
                rospy.set_param("/map_md5", str(map_md5))
        except Exception:
            pass
        try:
            if set_private_params:
                rospy.set_param("~map_id", str(map_id))
                rospy.set_param("~map_md5", str(map_md5))
        except Exception:
            pass
    return str(map_id), str(map_md5), bool(ok)

# -*- coding: utf-8 -*-
"""Map identity utilities.

Commercial cleaning robots frequently rebuild/replace maps. To prevent executing
stale plans or resuming on a different map, we compute a deterministic checksum
(md5) from the OccupancyGrid content (metadata + data array) and optionally
inject it into ROS params.

We intentionally do NOT include header.stamp in the checksum.
"""

from __future__ import annotations

import hashlib
import struct
import time
from typing import Optional, Tuple

import rospy
from nav_msgs.msg import OccupancyGrid


def compute_map_md5(map_topic: str = "/map", timeout_s: float = 2.0) -> Optional[str]:
    """Compute deterministic md5 of OccupancyGrid (info + data).

    Returns:
        hex md5 string, or None if no map message could be obtained.
    """
    map_topic = (map_topic or "/map").strip() or "/map"
    try:
        msg: OccupancyGrid = rospy.wait_for_message(map_topic, OccupancyGrid, timeout=float(timeout_s))
    except Exception:
        return None

    try:
        info = msg.info
        o = info.origin
        # Pack metadata deterministically
        buf = bytearray()
        buf += struct.pack("<II", int(info.width), int(info.height))
        buf += struct.pack("<f", float(info.resolution))
        buf += struct.pack(
            "<ffffff",  # position xyz + orientation xyz (leave w separately)
            float(o.position.x),
            float(o.position.y),
            float(o.position.z),
            float(o.orientation.x),
            float(o.orientation.y),
            float(o.orientation.z),
        )
        buf += struct.pack("<f", float(o.orientation.w))

        # Data: int8[] in [-1,100]. Convert to unsigned bytes deterministically.
        # Avoid list->bytes direct when values are negative.
        data = msg.data
        if data is None:
            data = []
        buf += bytes(((int(v) + 256) & 0xFF) for v in data)

        h = hashlib.md5()
        h.update(buf)
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
    """Best-effort get (map_id, map_md5) from params (global or private)."""
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
    """Ensure map identity exists on param server.

    If map_md5 is missing, compute it from OccupancyGrid and set it.
    If map_id is missing, derive it from md5 prefix.

    Returns:
        (map_id, map_md5, ok)
    """
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
        # Write both global and private params so other nodes can read either.
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

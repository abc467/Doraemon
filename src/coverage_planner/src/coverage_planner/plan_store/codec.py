# -*- coding: utf-8 -*-
import math
import struct
from typing import Iterable, List, Tuple, Optional


def polyline_length_xy(pts_xy: List[Tuple[float, float]]) -> float:
    if not pts_xy or len(pts_xy) < 2:
        return 0.0
    s = 0.0
    for i in range(1, len(pts_xy)):
        dx = pts_xy[i][0] - pts_xy[i - 1][0]
        dy = pts_xy[i][1] - pts_xy[i - 1][1]
        s += math.hypot(dx, dy)
    return float(s)


def encode_path_xyyaw_f32(path_xyyaw: Iterable[Tuple[float, float, float]]) -> bytes:
    """
    Encode path points as little-endian float32 triples: [x,y,yaw,x,y,yaw,...]
    """
    arr = []
    for x, y, yaw in path_xyyaw:
        arr.extend([float(x), float(y), float(yaw)])
    if not arr:
        return b""
    fmt = "<" + "f" * len(arr)
    return struct.pack(fmt, *arr)


def decode_path_xyyaw_f32(blob: bytes) -> List[Tuple[float, float, float]]:
    if blob is None or len(blob) == 0:
        return []
    if (len(blob) % 12) != 0:
        raise ValueError(f"blob size {len(blob)} is not multiple of 12 (3*f32)")
    n_floats = len(blob) // 4
    vals = struct.unpack("<" + "f" * n_floats, blob)
    out = []
    for i in range(0, len(vals), 3):
        out.append((float(vals[i]), float(vals[i + 1]), float(vals[i + 2])))
    return out


def xy_to_xyyaw(path_xy: List[Tuple[float, float]]) -> List[Tuple[float, float, float]]:
    """
    Convert (x,y) polyline to (x,y,yaw) with finite difference yaw.
    """
    if not path_xy:
        return []
    if len(path_xy) == 1:
        x, y = path_xy[0]
        return [(float(x), float(y), 0.0)]

    out = []
    n = len(path_xy)
    for i in range(n):
        if i < n - 1:
            dx = path_xy[i + 1][0] - path_xy[i][0]
            dy = path_xy[i + 1][1] - path_xy[i][1]
        else:
            dx = path_xy[i][0] - path_xy[i - 1][0]
            dy = path_xy[i][1] - path_xy[i - 1][1]
        yaw = math.atan2(dy, dx)
        out.append((float(path_xy[i][0]), float(path_xy[i][1]), float(yaw)))
    return out

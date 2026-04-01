# -*- coding: utf-8 -*-

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict, List, Sequence, Tuple


XY = Tuple[float, float]
XYYAW = Tuple[float, float, float]


def _wrap_deg(value: float) -> float:
    deg = float(value or 0.0)
    while deg <= -180.0:
        deg += 360.0
    while deg > 180.0:
        deg -= 360.0
    return float(deg)


def _wrap_rad(value: float) -> float:
    rad = float(value or 0.0)
    while rad <= -math.pi:
        rad += 2.0 * math.pi
    while rad > math.pi:
        rad -= 2.0 * math.pi
    return float(rad)


def yaw_offset_deg_from_points(p1: Sequence[float], p2: Sequence[float]) -> float:
    if len(p1 or []) < 2 or len(p2 or []) < 2:
        raise ValueError("two valid points are required")
    dx = float(p2[0]) - float(p1[0])
    dy = float(p2[1]) - float(p1[1])
    if (dx * dx + dy * dy) <= 1e-12:
        raise ValueError("selected points are too close")
    return _wrap_deg(math.degrees(math.atan2(dy, dx)))


@dataclass
class MapAlignment:
    map_name: str
    map_id: str
    map_version: str
    alignment_version: str
    raw_frame: str
    aligned_frame: str
    yaw_offset_deg: float
    pivot_x: float
    pivot_y: float
    source: str
    status: str
    active: bool = False
    created_ts: float = 0.0
    updated_ts: float = 0.0

    @property
    def yaw_offset_rad(self) -> float:
        return math.radians(float(self.yaw_offset_deg or 0.0))

    @classmethod
    def from_dict(cls, data: Dict[str, Any], *, default_raw_frame: str = "map", default_aligned_frame: str = "site_map"):
        raw_frame = str(data.get("raw_frame") or default_raw_frame or "map").strip() or "map"
        aligned_frame = str(data.get("aligned_frame") or default_aligned_frame or "site_map").strip() or "site_map"
        return cls(
            map_name=str(data.get("map_name") or "").strip(),
            map_id=str(data.get("map_id") or "").strip(),
            map_version=str(data.get("map_version") or "").strip(),
            alignment_version=str(data.get("alignment_version") or "").strip(),
            raw_frame=raw_frame,
            aligned_frame=aligned_frame,
            yaw_offset_deg=_wrap_deg(float(data.get("yaw_offset_deg") or 0.0)),
            pivot_x=float(data.get("pivot_x") or 0.0),
            pivot_y=float(data.get("pivot_y") or 0.0),
            source=str(data.get("source") or "").strip(),
            status=str(data.get("status") or "draft").strip() or "draft",
            active=bool(data.get("active", False)),
            created_ts=float(data.get("created_ts") or 0.0),
            updated_ts=float(data.get("updated_ts") or 0.0),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "map_name": str(self.map_name or ""),
            "map_id": str(self.map_id or ""),
            "map_version": str(self.map_version or ""),
            "alignment_version": str(self.alignment_version or ""),
            "raw_frame": str(self.raw_frame or "map"),
            "aligned_frame": str(self.aligned_frame or "site_map"),
            "yaw_offset_deg": float(_wrap_deg(self.yaw_offset_deg)),
            "pivot_x": float(self.pivot_x or 0.0),
            "pivot_y": float(self.pivot_y or 0.0),
            "source": str(self.source or ""),
            "status": str(self.status or "draft"),
            "active": bool(self.active),
            "created_ts": float(self.created_ts or 0.0),
            "updated_ts": float(self.updated_ts or 0.0),
        }


def rotate_xy(x: float, y: float, *, yaw_deg: float, pivot_x: float = 0.0, pivot_y: float = 0.0) -> XY:
    theta = math.radians(float(yaw_deg or 0.0))
    c = math.cos(theta)
    s = math.sin(theta)
    dx = float(x) - float(pivot_x)
    dy = float(y) - float(pivot_y)
    return (
        float(pivot_x) + c * dx - s * dy,
        float(pivot_y) + s * dx + c * dy,
    )


def map_to_aligned_point(x: float, y: float, cfg: MapAlignment) -> XY:
    return rotate_xy(
        float(x),
        float(y),
        yaw_deg=-float(cfg.yaw_offset_deg or 0.0),
        pivot_x=float(cfg.pivot_x or 0.0),
        pivot_y=float(cfg.pivot_y or 0.0),
    )


def aligned_to_map_point(x: float, y: float, cfg: MapAlignment) -> XY:
    return rotate_xy(
        float(x),
        float(y),
        yaw_deg=float(cfg.yaw_offset_deg or 0.0),
        pivot_x=float(cfg.pivot_x or 0.0),
        pivot_y=float(cfg.pivot_y or 0.0),
    )


def map_to_aligned_pose(x: float, y: float, yaw_rad: float, cfg: MapAlignment) -> XYYAW:
    px, py = map_to_aligned_point(x, y, cfg)
    return (px, py, _wrap_rad(float(yaw_rad or 0.0) - float(cfg.yaw_offset_rad or 0.0)))


def aligned_to_map_pose(x: float, y: float, yaw_rad: float, cfg: MapAlignment) -> XYYAW:
    px, py = aligned_to_map_point(x, y, cfg)
    return (px, py, _wrap_rad(float(yaw_rad or 0.0) + float(cfg.yaw_offset_rad or 0.0)))


def map_to_aligned_polygon(points: Sequence[Sequence[float]], cfg: MapAlignment) -> List[XY]:
    out: List[XY] = []
    for pt in points or []:
        if pt is None or len(pt) < 2:
            continue
        out.append(map_to_aligned_point(float(pt[0]), float(pt[1]), cfg))
    return out


def aligned_to_map_polygon(points: Sequence[Sequence[float]], cfg: MapAlignment) -> List[XY]:
    out: List[XY] = []
    for pt in points or []:
        if pt is None or len(pt) < 2:
            continue
        out.append(aligned_to_map_point(float(pt[0]), float(pt[1]), cfg))
    return out


def make_axis_aligned_rect(p1: Sequence[float], p2: Sequence[float]) -> List[XY]:
    if len(p1 or []) < 2 or len(p2 or []) < 2:
        raise ValueError("two valid points are required")
    x0 = float(min(float(p1[0]), float(p2[0])))
    x1 = float(max(float(p1[0]), float(p2[0])))
    y0 = float(min(float(p1[1]), float(p2[1])))
    y1 = float(max(float(p1[1]), float(p2[1])))
    return [(x0, y0), (x1, y0), (x1, y1), (x0, y1)]


def polygon_area(points: Sequence[Sequence[float]]) -> float:
    pts: List[XY] = []
    for pt in points or []:
        if pt is None or len(pt) < 2:
            continue
        pts.append((float(pt[0]), float(pt[1])))
    if len(pts) < 3:
        return 0.0
    area = 0.0
    for i in range(len(pts)):
        x1, y1 = pts[i]
        x2, y2 = pts[(i + 1) % len(pts)]
        area += x1 * y2 - x2 * y1
    return abs(float(area)) * 0.5

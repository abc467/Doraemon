# coverage_planner_core/geom_norm.py
# -*- coding: utf-8 -*-

from typing import List, Tuple
from .geom import dist_xy

XY = Tuple[float, float]


def _dedup_consecutive(pts: List[XY], eps=1e-12) -> List[XY]:
    out: List[XY] = []
    for p in pts:
        p2 = (float(p[0]), float(p[1]))
        if not out or (dist_xy(out[-1], p2) ** 2) > eps:
            out.append(p2)
    return out


def _ensure_closed(pts: List[XY]) -> List[XY]:
    if len(pts) < 1:
        return pts
    if pts[0] != pts[-1]:
        pts = pts + [pts[0]]
    return pts


def _ring_area_signed(pts_closed: List[XY]) -> float:
    if len(pts_closed) < 4:
        return 0.0
    a = 0.0
    for i in range(len(pts_closed) - 1):
        x1, y1 = pts_closed[i]
        x2, y2 = pts_closed[i + 1]
        a += (x1 * y2 - x2 * y1)
    return 0.5 * a


def _reverse_if_needed(pts_closed: List[XY], want_ccw: bool) -> List[XY]:
    area = _ring_area_signed(pts_closed)
    is_ccw = area > 0.0
    if want_ccw and (not is_ccw):
        return list(reversed(pts_closed))
    if (not want_ccw) and is_ccw:
        return list(reversed(pts_closed))
    return pts_closed


def normalize_ring(pts: List[XY], want_ccw: bool, prec: int = 3) -> List[XY]:
    if pts is None or len(pts) < 3:
        raise ValueError("ring needs >=3 points")
    pts2 = _dedup_consecutive(pts)
    if len(pts2) < 3:
        raise ValueError("ring degenerate after dedup")
    pts2 = _ensure_closed(pts2)

    # round + re-dedup
    pts3 = [(round(p[0], prec), round(p[1], prec)) for p in pts2]
    out = [pts3[0]]
    for p in pts3[1:]:
        if out[-1] != p:
            out.append(p)
    out = _ensure_closed(out)

    if len(out) < 4:
        raise ValueError("ring too short after normalize")
    out = _reverse_if_needed(out, want_ccw=want_ccw)
    return out


def normalize_polygon(outer: List[XY], holes: List[List[XY]], prec: int = 3):
    outer_n = normalize_ring(outer, want_ccw=True, prec=prec)
    holes_n = []
    for h in holes or []:
        if not h or len(h) < 3:
            continue
        holes_n.append(normalize_ring(h, want_ccw=False, prec=prec))
    # return unclosed (like your script uses for bbox/loops)? -> here keep closed for storage,
    # core will convert as needed.
    return outer_n, holes_n

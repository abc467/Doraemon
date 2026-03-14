# coverage_planner_core/geom.py
# -*- coding: utf-8 -*-

import math
from typing import List, Tuple, Dict, Optional

XY = Tuple[float, float]


def dist_xy(a: XY, b: XY) -> float:
    return math.hypot(b[0] - a[0], b[1] - a[1])


def polyline_length_xy(pts: List[XY]) -> float:
    if not pts or len(pts) < 2:
        return 0.0
    s = 0.0
    for i in range(1, len(pts)):
        s += dist_xy(pts[i - 1], pts[i])
    return s


def bbox_from_pts_xy(pts: List[XY]) -> Tuple[float, float, float, float]:
    xs = [p[0] for p in pts]
    ys = [p[1] for p in pts]
    return min(xs), min(ys), max(xs), max(ys)


# =========================
# uniform resampling
# =========================
def resample_polyline_uniform(pts_xy: List[XY], step_m: float) -> List[XY]:
    """Equal arclength resampling; keeps enough points on long straight segments."""
    if not pts_xy or len(pts_xy) < 2 or step_m <= 1e-9:
        return pts_xy[:] if pts_xy else []

    out = [pts_xy[0]]
    carry = 0.0
    prev = pts_xy[0]

    for nxt in pts_xy[1:]:
        seg_len = dist_xy(prev, nxt)
        if seg_len < 1e-12:
            prev = nxt
            continue

        dx = (nxt[0] - prev[0]) / seg_len
        dy = (nxt[1] - prev[1]) / seg_len

        while carry + seg_len >= step_m:
            need = step_m - carry
            px = prev[0] + dx * need
            py = prev[1] + dy * need
            out.append((px, py))
            prev = (px, py)
            seg_len -= need
            carry = 0.0
            if seg_len < 1e-12:
                break

        carry += seg_len
        prev = nxt

    if dist_xy(out[-1], pts_xy[-1]) > 1e-6:
        out.append(pts_xy[-1])
    return out


def yaw_list_from_pts(pts: List[XY]) -> List[float]:
    if not pts:
        return []
    if len(pts) == 1:
        return [0.0]
    yaws = []
    n = len(pts)
    for i in range(n):
        if i < n - 1:
            dx = pts[i + 1][0] - pts[i][0]
            dy = pts[i + 1][1] - pts[i][1]
        else:
            dx = pts[i][0] - pts[i - 1][0]
            dy = pts[i][1] - pts[i - 1][1]
        yaws.append(math.atan2(dy, dx))
    return yaws


# =========================
# Trim polyline endpoints by arclength
# =========================
def trim_polyline_by_ends(pts_xy: List[XY], trim_m: float) -> List[XY]:
    """Trim polyline at both ends by arclength trim_m."""
    if trim_m <= 1e-9 or not pts_xy or len(pts_xy) < 2:
        return pts_xy[:]

    d = [0.0]
    for i in range(1, len(pts_xy)):
        d.append(d[-1] + dist_xy(pts_xy[i - 1], pts_xy[i]))
    total = d[-1]
    if total <= 2.0 * trim_m + 1e-6:
        return pts_xy[:]

    start_s = trim_m
    end_s = total - trim_m

    def interp_at(s: float) -> XY:
        if s <= 0.0:
            return pts_xy[0]
        if s >= total:
            return pts_xy[-1]
        for i in range(1, len(d)):
            if d[i] >= s:
                s0 = d[i - 1]
                s1 = d[i]
                t = 0.0 if (s1 - s0) < 1e-12 else (s - s0) / (s1 - s0)
                x = pts_xy[i - 1][0] + t * (pts_xy[i][0] - pts_xy[i - 1][0])
                y = pts_xy[i - 1][1] + t * (pts_xy[i][1] - pts_xy[i - 1][1])
                return (x, y)
        return pts_xy[-1]

    out = [interp_at(start_s)]
    for i in range(1, len(pts_xy) - 1):
        if d[i] > start_s and d[i] < end_s:
            out.append(pts_xy[i])
    out.append(interp_at(end_s))
    return out


# =========================
# Edge loop (quadratic bezier corners) + densify loop uniformly
# =========================
def quad_bezier(p0: XY, pc: XY, p1: XY, n: int) -> List[XY]:
    out = []
    for i in range(n + 1):
        t = i / float(n)
        a = (1 - t) * (1 - t)
        b = 2 * (1 - t) * t
        c = t * t
        x = a * p0[0] + b * pc[0] + c * p1[0]
        y = a * p0[1] + b * pc[1] + c * p1[1]
        out.append((x, y))
    return out


def build_edge_loop_from_rect_vertices(
    xmin: float, ymin: float, xmax: float, ymax: float,
    corner_r: float, corner_pull: float,
    step_m: float, min_corner_pts: int
) -> List[XY]:
    """Return an UNCLOSED loop polyline with bezier corners, but straight edges may be sparse."""
    w = xmax - xmin
    h = ymax - ymin
    if w <= 0 or h <= 0:
        return []

    r = max(0.0, float(corner_r))
    r = min(r, 0.45 * min(w, h))
    if r < 1e-6:
        return [(xmin, ymin), (xmax, ymin), (xmax, ymax), (xmin, ymax)]

    approx_len = 0.5 * math.pi * r
    nseg = max(int(min_corner_pts), int(math.ceil(approx_len / max(1e-3, step_m))))

    # fixed tangency points
    pA = (xmin + r, ymin)
    pB = (xmax - r, ymin)
    pC = (xmax, ymin + r)
    pD = (xmax, ymax - r)
    pE = (xmax - r, ymax)
    pF = (xmin + r, ymax)
    pG = (xmin, ymax - r)
    pH = (xmin, ymin + r)

    pull = float(corner_pull)
    cBR = (xmax - r * pull, ymin + r * pull)
    cTR = (xmax - r * pull, ymax - r * pull)
    cTL = (xmin + r * pull, ymax - r * pull)
    cBL = (xmin + r * pull, ymin + r * pull)

    pts = []
    pts.append(pA)
    pts.append(pB)
    pts.extend(quad_bezier(pB, cBR, pC, nseg)[1:])
    pts.append(pD)
    pts.extend(quad_bezier(pD, cTR, pE, nseg)[1:])
    pts.append(pF)
    pts.extend(quad_bezier(pF, cTL, pG, nseg)[1:])
    pts.append(pH)
    pts.extend(quad_bezier(pH, cBL, pA, nseg)[1:])

    out = []
    for p in pts:
        if not out or dist_xy(out[-1], p) > 1e-8:
            out.append(p)
    if len(out) >= 2 and dist_xy(out[0], out[-1]) < 1e-8:
        out = out[:-1]
    return out


def densify_closed_loop_uniform(loop_pts: List[XY], step_m: float) -> List[XY]:
    """
    Make a dense loop with ~step_m spacing along the entire perimeter (including straight edges).
    Return UN-CLOSED (last != first).
    """
    if not loop_pts or len(loop_pts) < 2:
        return loop_pts[:] if loop_pts else []
    if step_m <= 1e-6:
        return loop_pts[:]

    closed = loop_pts[:] + [loop_pts[0]]
    dense_closed = resample_polyline_uniform(closed, step_m)

    if len(dense_closed) >= 2 and dist_xy(dense_closed[0], dense_closed[-1]) < 1e-6:
        dense_closed = dense_closed[:-1]

    out = []
    for p in dense_closed:
        if not out or dist_xy(out[-1], p) > 1e-8:
            out.append(p)
    return out


def rotate_polyline_to_index(poly: List[XY], idx: int) -> List[XY]:
    if not poly:
        return poly
    idx = int(idx) % len(poly)
    return poly[idx:] + poly[:idx]


def nearest_index(poly: List[XY], target_xy: XY) -> Tuple[int, float]:
    if not poly:
        return 0, 1e18
    best_i = 0
    best_d = 1e18
    for i, p in enumerate(poly):
        d = dist_xy(p, target_xy)
        if d < best_d:
            best_d = d
            best_i = i
    return best_i, best_d


def sample_straight(a_xy: XY, b_xy: XY, step_m: float) -> List[XY]:
    """Return polyline points from a->b with ~step spacing, including endpoints."""
    a = (float(a_xy[0]), float(a_xy[1]))
    b = (float(b_xy[0]), float(b_xy[1]))
    L = dist_xy(a, b)
    if L < 1e-9:
        return [a]
    n = max(1, int(math.ceil(L / max(step_m, 1e-6))))
    out = []
    for i in range(n + 1):
        t = i / float(n)
        out.append((a[0] + t * (b[0] - a[0]), a[1] + t * (b[1] - a[1])))
    return out


# =========================
# entry->P_pre by forward projection scan
# =========================
def find_P_pre_by_forward_projection(
    snake_pts: List[XY],
    entry_yaw: float,
    proj_min: float,
    proj_max: float,
    max_prefix_arclen: float
) -> XY:
    """
    Scan snake_pts from start (index order), stop when prefix arclen exceeds max_prefix_arclen.
    For each point pi, compute proj = (pi-entry)·dir(entry_yaw). pick FIRST with proj in [proj_min, proj_max].
    If not found, pick the point within scanned prefix with proj closest to mid (only if proj>0).
    """
    if not snake_pts or len(snake_pts) < 2:
        return snake_pts[0] if snake_pts else (0.0, 0.0)

    entry = snake_pts[0]
    dirx = math.cos(entry_yaw)
    diry = math.sin(entry_yaw)

    mid = 0.5 * (proj_min + proj_max)

    best = None
    best_err = 1e18

    s = 0.0
    for i in range(len(snake_pts)):
        if i > 0:
            s += dist_xy(snake_pts[i - 1], snake_pts[i])
            if s > max_prefix_arclen:
                break

        dx = snake_pts[i][0] - entry[0]
        dy = snake_pts[i][1] - entry[1]
        proj = dx * dirx + dy * diry

        if proj >= proj_min and proj <= proj_max:
            return snake_pts[i]

        if proj > 0.0:
            err = abs(proj - mid)
            if err < best_err:
                best_err = err
                best = snake_pts[i]

    return best if best is not None else snake_pts[min(1, len(snake_pts) - 1)]


# =========================
# e_pre along edge_loop by arclen window [dmin, dmax] from E_start
# =========================
def find_edge_point_by_arclen_window(edge_loop_rot: List[XY], dmin: float, dmax: float) -> XY:
    """
    edge_loop_rot: unclosed loop starting at E_start (index 0), and it is DENSE (step~path_step).
    Walk forward accumulating arclen; return FIRST point with cum in [dmin, dmax].
    If none found, return point with cum closest to mid.
    """
    if not edge_loop_rot or len(edge_loop_rot) < 2:
        return edge_loop_rot[0] if edge_loop_rot else (0.0, 0.0)

    mid = 0.5 * (dmin + dmax)

    s = 0.0
    best = edge_loop_rot[0]
    best_err = 1e18

    for i in range(1, len(edge_loop_rot)):
        s += dist_xy(edge_loop_rot[i - 1], edge_loop_rot[i])
        if s >= dmin and s <= dmax:
            return edge_loop_rot[i]
        err = abs(s - mid)
        if err < best_err:
            best_err = err
            best = edge_loop_rot[i]

    return best

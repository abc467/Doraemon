# coverage_planner_core/f2c_adapter.py
# -*- coding: utf-8 -*-

import os
import math
from contextlib import contextmanager
from typing import Any, List, Tuple, Optional

import fields2cover as f2c

from .geom import dist_xy, resample_polyline_uniform, trim_polyline_by_ends

XY = Tuple[float, float]
XYZ = Tuple[float, float, float]


# =========================
# stderr hard mute (keeps your original behavior)
# =========================
@contextmanager
def suppress_stderr(enabled: bool = True):
    if not enabled:
        yield
        return
    old = os.dup(2)
    devnull = os.open(os.devnull, os.O_WRONLY)
    os.dup2(devnull, 2)
    os.close(devnull)
    try:
        yield
    finally:
        os.dup2(old, 2)
        os.close(old)


# =========================
# Point conversion
# =========================
def f2c_point_xyz(p: Any) -> Optional[XYZ]:
    if p is None:
        return None
    if hasattr(p, "getX"):
        x = float(p.getX()); y = float(p.getY())
        z = float(p.getZ()) if hasattr(p, "getZ") else 0.0
        return x, y, z
    x = float(p.x) if hasattr(p, "x") else float(p[0])
    y = float(p.y) if hasattr(p, "y") else float(p[1])
    z = float(p.z) if hasattr(p, "z") else (float(p[2]) if len(p) > 2 else 0.0)
    return x, y, z


# =========================
# Build cells from polygon lists
# =========================
def build_cells_from_polygons(outer: List[XY], holes: List[List[XY]]) -> f2c.Cells:
    return build_cells_from_regions([{"outer": outer, "holes": holes}])


def build_cells_from_regions(regions: List[dict]) -> f2c.Cells:
    cells = f2c.Cells()
    for region in regions or []:
        outer = list(region.get("outer") or [])
        holes = list(region.get("holes") or [])
        if len(outer) < 3:
            continue

        ring = f2c.LinearRing()
        outer2 = list(outer)
        if outer2[0] != outer2[-1]:
            outer2.append(outer2[0])
        for x, y in outer2:
            ring.addPoint(f2c.Point(float(x), float(y), 0.0))
        cell = f2c.Cell(ring)

        for hole in holes:
            pts = list(hole)
            if len(pts) < 3:
                continue
            if pts[0] != pts[-1]:
                pts.append(pts[0])
            r = f2c.LinearRing()
            for x, y in pts:
                r.addPoint(f2c.Point(float(x), float(y), 0.0))
            cell.addRing(r)

        cells.addGeometry(cell)
    return cells


# =========================
# Build robot from spec (binding compatible)
# =========================
def build_robot(cov_width: float, width: float, R: float, max_diff_curv: float):
    robot = None
    for args in [(float(cov_width), float(R)), (float(cov_width),), ()]:
        try:
            robot = f2c.Robot(*args)
            break
        except Exception:
            robot = None
    if robot is None:
        raise RuntimeError("Failed to create f2c.Robot in your binding.")

    for fn, val in [
        ("setCovWidth", cov_width),
        ("setWidth", width),
        ("setMinTurningRadius", R),
        ("setMaxDiffCurv", max_diff_curv),
    ]:
        if hasattr(robot, fn):
            try:
                getattr(robot, fn)(float(val))
            except Exception:
                pass

    return robot


def build_turn_planner(turn_model: str):
    tm = (turn_model or "").strip().lower()
    if tm in ["dubinscc", "dubins_curves_cc"]:
        if hasattr(f2c, "PP_DubinsCurvesCC"):
            return f2c.PP_DubinsCurvesCC()
        return f2c.PP_DubinsCurves()
    if tm in ["dubins"]:
        return f2c.PP_DubinsCurves()
    if tm in ["reeds_shepp"] and hasattr(f2c, "PP_ReedsShepp"):
        return f2c.PP_ReedsShepp()
    if hasattr(f2c, "PP_DubinsCurvesCC"):
        return f2c.PP_DubinsCurvesCC()
    return f2c.PP_DubinsCurves()


def build_objective():
    for name in ["OBJ_NSwathModified", "OBJ_NSwath"]:
        if hasattr(f2c, name):
            try:
                return getattr(f2c, name)()
            except Exception:
                pass
    raise RuntimeError("Cannot construct objective (OBJ_NSwathModified/OBJ_NSwath)")


def build_swath_generator():
    for name in ["SG_BruteForce", "SG_AlternatingAngles"]:
        if hasattr(f2c, name):
            try:
                return getattr(f2c, name)()
            except Exception:
                pass
    raise RuntimeError("Cannot construct swath generator")


def generate_best_swaths(sg, obj, r_w: float, cell_geom, mute: bool = True):
    with suppress_stderr(enabled=mute):
        try:
            return sg.generateBestSwaths(obj, float(r_w), cell_geom)
        except Exception:
            pass
        try:
            return sg.generateBestSwaths(obj, cell_geom, float(r_w))
        except Exception:
            pass
        try:
            cs = f2c.Cells()
            cs.addGeometry(cell_geom)
            return sg.generateBestSwaths(obj, float(r_w), cs)
        except Exception:
            pass
    raise RuntimeError("generateBestSwaths failed (signature mismatch).")


# =========================
# Swaths access helpers
# =========================
def swaths_size(swaths) -> int:
    if hasattr(swaths, "size"):
        try:
            return int(swaths.size())
        except Exception:
            pass
    try:
        return int(len(swaths))
    except Exception:
        return 0


def swath_at(swaths, i: int):
    for name in ["getSwath", "at", "get", "operator[]", "__getitem__"]:
        if hasattr(swaths, name):
            try:
                fn = getattr(swaths, name)
                return fn(int(i)) if callable(fn) else fn[int(i)]
            except Exception:
                pass
    try:
        return swaths[int(i)]
    except Exception:
        return None


# =========================
# swath polyline extraction
# =========================
def _ls_num_points(ls) -> int:
    for nname in ["size", "getNumPoints", "numPoints", "nPoints"]:
        if hasattr(ls, nname):
            try:
                v = getattr(ls, nname)
                return int(v()) if callable(v) else int(v)
            except Exception:
                pass
    try:
        return int(len(ls))
    except Exception:
        return 0


def _ls_point_at(ls, i: int):
    for name in ["getGeometry", "at", "get"]:
        if hasattr(ls, name):
            try:
                return getattr(ls, name)(int(i))
            except Exception:
                pass
    try:
        return ls[int(i)]
    except Exception:
        return None


def swath_polyline_xyz(sw) -> List[XYZ]:
    if sw is None:
        return []
    for gname in ["getPath", "getCenterLine", "getLine", "path", "centerLine", "line"]:
        if hasattr(sw, gname):
            try:
                obj = getattr(sw, gname)
                ls = obj() if callable(obj) else obj
                n = _ls_num_points(ls)
                if n >= 2:
                    pts = []
                    for k in range(n):
                        pk = _ls_point_at(ls, k)
                        xyz = f2c_point_xyz(pk)
                        if xyz is not None:
                            pts.append(xyz)
                    if len(pts) >= 2:
                        return pts
            except Exception:
                pass
    try:
        n = _ls_num_points(sw)
        if n >= 2:
            pts = []
            for k in range(n):
                pk = _ls_point_at(sw, k)
                xyz = f2c_point_xyz(pk)
                if xyz is not None:
                    pts.append(xyz)
            if len(pts) >= 2:
                return pts
    except Exception:
        pass
    return []


def swath_endpoints_xyz(sw) -> Tuple[Optional[XYZ], Optional[XYZ]]:
    pts = swath_polyline_xyz(sw)
    if len(pts) >= 2:
        return pts[0], pts[-1]
    for a0, a1 in [("getStartPoint", "getEndPoint"), ("startPoint", "endPoint"), ("getStart", "getEnd")]:
        if hasattr(sw, a0) and hasattr(sw, a1):
            try:
                p0 = getattr(sw, a0)()
                p1 = getattr(sw, a1)()
                A = f2c_point_xyz(p0)
                B = f2c_point_xyz(p1)
                if A is not None and B is not None:
                    return A, B
            except Exception:
                pass
    return None, None


# =========================
# Official snake sorted swaths
# =========================
def snake_sorted_swaths(swaths_b, mute: bool = True):
    if not hasattr(f2c, "RP_Snake"):
        return None
    rp_snake = f2c.RP_Snake()
    with suppress_stderr(enabled=mute):
        for fn_name in ["genSortedSwaths", "genSortedSwath", "sortedSwaths"]:
            if hasattr(rp_snake, fn_name):
                try:
                    return getattr(rp_snake, fn_name)(swaths_b)
                except Exception:
                    pass
    return None


# =========================
# swath angles
# =========================
def _try_swath_angle(sw, names) -> Optional[float]:
    for nm in names:
        if hasattr(sw, nm):
            try:
                v = getattr(sw, nm)
                ang = v() if callable(v) else v
                return float(ang)
            except Exception:
                pass
    return None


def _swath_endpoints_and_angles(sw):
    a, b = swath_endpoints_xyz(sw)
    in_ang = _try_swath_angle(sw, ["getInAngle", "inAngle", "getInAngleRad", "in_angle"])
    out_ang = _try_swath_angle(sw, ["getOutAngle", "outAngle", "getOutAngleRad", "out_angle"])
    if a is None or b is None:
        return None, None, in_ang, out_ang
    fallback = math.atan2(b[1] - a[1], b[0] - a[0])
    if in_ang is None:
        in_ang = fallback
    if out_ang is None:
        out_ang = fallback
    return a, b, in_ang, out_ang


# =========================
# Trim swath polyline endpoints by turn_margin_m
# =========================
def swath_polyline_xy_trimmed(sw, turn_margin_m: float) -> List[XY]:
    pts = swath_polyline_xyz(sw)
    if not pts:
        a, b = swath_endpoints_xyz(sw)
        if a is None or b is None:
            return []
        pts_xy = [(a[0], a[1]), (b[0], b[1])]
    else:
        pts_xy = [(p[0], p[1]) for p in pts]
    if turn_margin_m > 1e-9:
        pts_xy = trim_polyline_by_ends(pts_xy, turn_margin_m)
    return pts_xy


# =========================
# Build continuous snake polyline (swaths + turns)
# =========================
def recon_snake_polyline(
    robot,
    sw_sorted,
    turn_planner,
    turn_step_m: float,
    turn_margin_m: float,
    mute: bool = True
) -> List[XY]:
    n = swaths_size(sw_sorted)
    if n <= 0:
        return []

    pts_xy: List[XY] = []

    with suppress_stderr(enabled=mute):
        for i in range(n):
            sw = swath_at(sw_sorted, i)
            if sw is None:
                continue

            a, b, in_ang, out_ang = _swath_endpoints_and_angles(sw)
            if a is None or b is None:
                continue

            sw_pts = swath_polyline_xy_trimmed(sw, turn_margin_m)
            if len(sw_pts) < 2:
                pts0 = swath_polyline_xyz(sw)
                if pts0 and len(pts0) >= 2:
                    sw_pts = [(pts0[0][0], pts0[0][1]), (pts0[-1][0], pts0[-1][1])]
                else:
                    sw_pts = [(a[0], a[1]), (b[0], b[1])]

            for xy in sw_pts:
                if not pts_xy or dist_xy(pts_xy[-1], xy) > 1e-6:
                    pts_xy.append((float(xy[0]), float(xy[1])))

            if i < n - 1:
                sw2 = swath_at(sw_sorted, i + 1)
                if sw2 is None:
                    continue
                c, d, in2, out2 = _swath_endpoints_and_angles(sw2)
                if c is None or d is None:
                    continue

                sw2_pts = swath_polyline_xy_trimmed(sw2, turn_margin_m)
                entry2 = sw2_pts[0] if len(sw2_pts) >= 2 else (c[0], c[1])
                exit1 = sw_pts[-1]

                p1 = f2c.Point(float(exit1[0]), float(exit1[1]), 0.0)
                p2 = f2c.Point(float(entry2[0]), float(entry2[1]), 0.0)
                ang1 = float(out_ang)
                ang2 = float(in2)

                turn = None
                try:
                    turn = turn_planner.createTurn(robot, p1, ang1, p2, ang2)
                except Exception:
                    turn = None

                if turn is None:
                    if dist_xy(pts_xy[-1], entry2) > 1e-6:
                        pts_xy.append((float(entry2[0]), float(entry2[1])))
                    continue

                try:
                    turn_path = turn.discretize(float(turn_step_m))
                    if hasattr(turn_path, "getStates"):
                        states = list(turn_path.getStates())
                        for st in states:
                            p = getattr(st, "point", None)
                            xyz = f2c_point_xyz(p)
                            if xyz is None:
                                continue
                            xy = (float(xyz[0]), float(xyz[1]))
                            if not pts_xy or dist_xy(pts_xy[-1], xy) > 1e-6:
                                pts_xy.append(xy)
                except Exception:
                    if dist_xy(pts_xy[-1], entry2) > 1e-6:
                        pts_xy.append((float(entry2[0]), float(entry2[1])))

    return pts_xy


# =========================
# Cell ring extraction
# =========================
def cell_outer_ring_xy(cell_geom) -> List[XY]:
    pts: List[XY] = []
    try:
        if hasattr(cell_geom, "size") and int(cell_geom.size()) > 0 and hasattr(cell_geom, "getGeometry"):
            ring = cell_geom.getGeometry(0)
            if hasattr(ring, "size") and hasattr(ring, "getGeometry"):
                n = int(ring.size())
                for i in range(n):
                    p = ring.getGeometry(i)
                    xyz = f2c_point_xyz(p)
                    if xyz is not None:
                        pts.append((xyz[0], xyz[1]))
    except Exception:
        pass
    if len(pts) >= 2 and pts[0] == pts[-1]:
        pts = pts[:-1]
    return pts

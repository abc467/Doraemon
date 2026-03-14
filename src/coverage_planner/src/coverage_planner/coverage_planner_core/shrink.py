# coverage_planner_core/shrink.py
# -*- coding: utf-8 -*-

from typing import List, Tuple, Optional

import fields2cover as f2c

from .geom import bbox_from_pts_xy

try:
    from shapely.geometry import Polygon, box
    _HAS_SHAPELY = True
except Exception:
    _HAS_SHAPELY = False

XY = Tuple[float, float]


def _cell_rings_to_coords(cell_geom) -> List[List[XY]]:
    rings: List[List[XY]] = []
    if not hasattr(cell_geom, "size") or not hasattr(cell_geom, "getGeometry"):
        return rings
    nr = int(cell_geom.size())
    for rid in range(nr):
        ring = cell_geom.getGeometry(rid)
        pts: List[XY] = []
        if hasattr(ring, "size") and hasattr(ring, "getGeometry"):
            n = int(ring.size())
            for i in range(n):
                p = ring.getGeometry(i)
                if hasattr(p, "getX"):
                    pts.append((float(p.getX()), float(p.getY())))
                else:
                    pts.append((float(p.x), float(p.y)))
        if len(pts) >= 2 and pts[0] == pts[-1]:
            pts = pts[:-1]
        if len(pts) >= 3:
            rings.append(pts)
    return rings


def _coords_to_f2c_cell(shell_xy: List[XY], holes_xy_list: List[List[XY]]) -> f2c.Cell:
    def add_ring_points(lr: f2c.LinearRing, pts_xy: List[XY]):
        pts_xy2 = list(pts_xy)
        if pts_xy2[0] != pts_xy2[-1]:
            pts_xy2.append(pts_xy2[0])
        for x, y in pts_xy2:
            lr.addPoint(f2c.Point(float(x), float(y), 0.0))

    outer = f2c.LinearRing()
    add_ring_points(outer, shell_xy)
    cell = f2c.Cell(outer)
    for hole in holes_xy_list:
        r = f2c.LinearRing()
        add_ring_points(r, hole)
        cell.addRing(r)
    return cell


def cell_to_shapely_polygon(cell_geom) -> Optional["Polygon"]:
    rings = _cell_rings_to_coords(cell_geom)
    if not rings:
        return None
    shell = rings[0]
    holes = rings[1:] if len(rings) > 1 else []
    try:
        return Polygon(shell=shell, holes=holes)
    except Exception:
        return None


def shapely_to_f2c_cells(poly) -> List[f2c.Cell]:
    out: List[f2c.Cell] = []
    if poly is None or poly.is_empty:
        return out
    if poly.geom_type == "Polygon":
        ext = list(poly.exterior.coords)[:-1]
        holes = [list(r.coords)[:-1] for r in poly.interiors]
        out.append(_coords_to_f2c_cell(ext, holes))
    elif poly.geom_type == "MultiPolygon":
        for g in poly.geoms:
            if g.is_empty:
                continue
            ext = list(g.exterior.coords)[:-1]
            holes = [list(r.coords)[:-1] for r in g.interiors]
            out.append(_coords_to_f2c_cell(ext, holes))
    return out


def long_side_shrink_cells(cell_geom, wall_margin_m: float) -> List[f2c.Cell]:
    """
    Long-side shrink (only two sides parallel to long edge), identical behavior to your script:
    - if bbox height>=width => shrink xmin/xmax (vertical long side)
    - else shrink ymin/ymax
    - then intersect with original polygon (keeps holes)
    """
    if wall_margin_m <= 1e-9:
        return [cell_geom]

    # bbox from outer ring
    outer = []
    try:
        ring = cell_geom.getGeometry(0)
        n = int(ring.size())
        for i in range(n):
            p = ring.getGeometry(i)
            if hasattr(p, "getX"):
                outer.append((float(p.getX()), float(p.getY())))
            else:
                outer.append((float(p.x), float(p.y)))
    except Exception:
        return [cell_geom]

    if len(outer) >= 2 and outer[0] == outer[-1]:
        outer = outer[:-1]
    if len(outer) < 3:
        return [cell_geom]

    xmin, ymin, xmax, ymax = bbox_from_pts_xy(outer)
    w = xmax - xmin
    h = ymax - ymin
    if w <= 1e-9 or h <= 1e-9:
        return [cell_geom]

    if h >= w:
        xmin2 = xmin + wall_margin_m
        xmax2 = xmax - wall_margin_m
        ymin2 = ymin
        ymax2 = ymax
    else:
        xmin2 = xmin
        xmax2 = xmax
        ymin2 = ymin + wall_margin_m
        ymax2 = ymax - wall_margin_m

    if xmax2 <= xmin2 + 0.05 or ymax2 <= ymin2 + 0.05:
        return []

    if not _HAS_SHAPELY:
        # identical intent: if shapely missing, skip long-side shrink
        return [cell_geom]

    poly = cell_to_shapely_polygon(cell_geom)
    if poly is None or poly.is_empty:
        return [cell_geom]

    cut = box(xmin2, ymin2, xmax2, ymax2)
    inter = poly.intersection(cut)
    out_cells = shapely_to_f2c_cells(inter)
    return out_cells if out_cells else []

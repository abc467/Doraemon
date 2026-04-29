# -*- coding: utf-8 -*-

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Any, Dict, List, Optional, Sequence, Tuple

try:
    from shapely.geometry import GeometryCollection, LineString, MultiPolygon, Point, Polygon
    from shapely.ops import split, triangulate, unary_union

    _HAS_SHAPELY = True
except Exception:  # pragma: no cover - runtime environment decides this
    GeometryCollection = None
    LineString = None
    MultiPolygon = None
    Polygon = None
    split = None
    triangulate = None
    unary_union = None
    _HAS_SHAPELY = False


XY = Tuple[float, float]
DEFAULT_VIRTUAL_WALL_BUFFER_M = 0.575


@dataclass
class CompiledMapConstraints:
    map_id: str
    map_md5: str
    constraint_version: str
    no_go_polygons: List[Dict[str, Any]]
    virtual_wall_keepouts: List[Dict[str, Any]]


@dataclass
class CompiledZoneConstraints:
    map_id: str
    map_md5: str
    constraint_version: str
    effective_regions: List[Dict[str, Any]]
    keepout_snapshot_rings: List[List[XY]]


def _round_xy(x: float, y: float, prec: int = 3) -> XY:
    return (round(float(x), int(prec)), round(float(y), int(prec)))


def _normalize_open_ring(points: Sequence[Sequence[float]], prec: int = 3) -> List[XY]:
    out: List[XY] = []
    for pt in points or []:
        if pt is None or len(pt) < 2:
            continue
        out.append(_round_xy(float(pt[0]), float(pt[1]), prec=prec))
    if len(out) >= 2 and out[0] == out[-1]:
        out = out[:-1]
    if len(out) < 3:
        raise ValueError("polygon ring must contain at least 3 distinct points")
    return out


def _normalize_open_polyline(points: Sequence[Sequence[float]], prec: int = 3) -> List[XY]:
    out: List[XY] = []
    for pt in points or []:
        if pt is None or len(pt) < 2:
            continue
        out.append(_round_xy(float(pt[0]), float(pt[1]), prec=prec))
    if len(out) >= 2 and out[0] == out[-1]:
        out = out[:-1]
    if len(out) < 2:
        raise ValueError("virtual wall polyline must contain at least 2 points")
    return out


def _safe_polygon(
    outer: Sequence[Sequence[float]],
    holes: Optional[Sequence[Sequence[Sequence[float]]]] = None,
) -> "Polygon":
    if not _HAS_SHAPELY:
        raise RuntimeError("Shapely is required for constraint compilation")
    poly = Polygon(_normalize_open_ring(outer), [_normalize_open_ring(r) for r in (holes or []) if r])
    if not poly.is_valid:
        poly = poly.buffer(0.0)
    if poly.is_empty:
        raise ValueError("invalid/empty polygon geometry")
    return poly


def _geometry_to_region_list(geom, *, prec: int = 3) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if (not _HAS_SHAPELY) or geom is None or geom.is_empty:
        return out

    if geom.geom_type == "Polygon":
        polys = [geom]
    elif geom.geom_type == "MultiPolygon":
        polys = list(geom.geoms)
    elif geom.geom_type == "GeometryCollection":
        polys = [g for g in geom.geoms if getattr(g, "geom_type", "") == "Polygon" and not g.is_empty]
    else:
        polys = []

    for poly in polys:
        outer = [_round_xy(x, y, prec=prec) for x, y in list(poly.exterior.coords)[:-1]]
        if len(outer) < 3:
            continue
        holes: List[List[XY]] = []
        for ring in poly.interiors:
            hole = [_round_xy(x, y, prec=prec) for x, y in list(ring.coords)[:-1]]
            if len(hole) >= 3:
                holes.append(hole)
        out.append({"outer": outer, "holes": holes})
    return out


def _iter_polygon_geometries(geom) -> List["Polygon"]:
    if (not _HAS_SHAPELY) or geom is None or geom.is_empty:
        return []
    if geom.geom_type == "Polygon":
        return [geom]
    if geom.geom_type == "MultiPolygon":
        return [g for g in geom.geoms if not g.is_empty]
    if geom.geom_type == "GeometryCollection":
        return [g for g in geom.geoms if getattr(g, "geom_type", "") == "Polygon" and not g.is_empty]
    return []


def _split_parts_by_lines(parts, lines) -> List["Polygon"]:
    out = list(parts or [])
    for line in lines or []:
        next_parts = []
        for part in out:
            if part is None or part.is_empty:
                continue
            try:
                pieces = split(part, line)
                next_parts.extend(_iter_polygon_geometries(pieces))
            except Exception:
                next_parts.append(part)
        out = next_parts
    return out


def _hole_split_lines(poly, *, orientation: str):
    minx, miny, maxx, maxy = poly.bounds
    pad = max(float(maxx - minx), float(maxy - miny), 1.0) + 1.0
    values = set()
    for ring in poly.interiors:
        for x, y in list(ring.coords):
            values.add(round(float(x if orientation == "vertical" else y), 6))
    lines = []
    for value in sorted(values):
        if orientation == "vertical":
            if value <= minx + 1e-6 or value >= maxx - 1e-6:
                continue
            lines.append(LineString([(value, miny - pad), (value, maxy + pad)]))
        else:
            if value <= miny + 1e-6 or value >= maxy - 1e-6:
                continue
            lines.append(LineString([(minx - pad, value), (maxx + pad, value)]))
    return lines


def _triangulate_hole_free(poly) -> List["Polygon"]:
    pieces = []
    try:
        triangles = triangulate(poly)
    except Exception:
        return pieces
    for tri in triangles or []:
        if tri is None or tri.is_empty:
            continue
        try:
            clipped = tri.intersection(poly)
        except Exception:
            continue
        for part in _iter_polygon_geometries(clipped):
            if part.area > 1e-6 and len(part.interiors) == 0:
                pieces.append(part)
    return pieces


def _geometry_to_hole_free_region_list(geom, *, prec: int = 3) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for poly in _iter_polygon_geometries(geom):
        if len(poly.interiors) == 0:
            out.extend(_geometry_to_region_list(poly, prec=prec))
            continue

        parts = [poly]
        parts = _split_parts_by_lines(
            parts,
            [line for part in parts for line in _hole_split_lines(part, orientation="vertical")],
        )
        if any(len(part.interiors) > 0 for part in parts):
            parts = _split_parts_by_lines(
                parts,
                [line for part in parts for line in _hole_split_lines(part, orientation="horizontal")],
            )

        for part in parts:
            if part.is_empty or part.area <= 1e-6:
                continue
            if len(part.interiors) == 0:
                out.extend(_geometry_to_region_list(part, prec=prec))
            else:
                for tri in _triangulate_hole_free(part):
                    out.extend(_geometry_to_region_list(tri, prec=prec))
    return out


def make_hole_free_effective_regions(
    regions: Sequence[Dict[str, Any]],
    *,
    prec: int = 3,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for region in list(regions or []):
        outer = list((region or {}).get("outer") or [])
        if len(outer) < 3:
            continue
        poly = _safe_polygon(outer, (region or {}).get("holes") or [])
        out.extend(_geometry_to_hole_free_region_list(poly, prec=prec))
    return out


def min_plannable_span_m(robot_spec: Any, params: Any) -> float:
    configured = float(getattr(params, "min_plannable_span_m", 0.0) or 0.0)
    if math.isfinite(configured) and configured > 0.0:
        return configured
    edge_radius_m = float(getattr(params, "edge_corner_radius_m", 0.0) or 0.0)
    min_turning_radius_m = float(getattr(robot_spec, "min_turning_radius", 0.0) or 0.0)
    if edge_radius_m < 0.0:
        edge_radius_m = min_turning_radius_m
    # turn_margin_m trims along generated swaths, so it must not define a
    # region's required short-side span.
    return max(
        float(getattr(robot_spec, "cov_width", 0.0) or 0.0) * 1.5,
        float(getattr(robot_spec, "width", 0.0) or 0.0)
        + max(edge_radius_m, min_turning_radius_m) * 1.6,
        0.25,
    )


def filter_effective_regions_for_planner(
    regions: Sequence[Dict[str, Any]],
    robot_spec: Any,
    params: Any,
) -> Tuple[List[Dict[str, Any]], int, float]:
    min_span_m = float(min_plannable_span_m(robot_spec, params))
    kept: List[Dict[str, Any]] = []
    skipped = 0
    for region in list(regions or []):
        outer = list((region or {}).get("outer") or [])
        if len(outer) < 3:
            skipped += 1
            continue
        xs = [float(pt[0]) for pt in outer if pt is not None and len(pt) >= 2]
        ys = [float(pt[1]) for pt in outer if pt is not None and len(pt) >= 2]
        if (not xs) or (not ys):
            skipped += 1
            continue
        width_m = max(xs) - min(xs)
        height_m = max(ys) - min(ys)
        if min(float(width_m), float(height_m)) + 1e-9 < min_span_m:
            skipped += 1
            continue
        kept.append(dict(region))
    return kept, int(skipped), min_span_m


def _buffer_keepout_polygon(poly: "Polygon", buffer_m: float):
    buffer_m = max(0.0, float(buffer_m or 0.0))
    if buffer_m <= 1e-9:
        return poly
    buffered = poly.buffer(buffer_m, cap_style=2, join_style=2)
    if not buffered.is_valid:
        buffered = buffered.buffer(0.0)
    return buffered if not buffered.is_empty else poly


def _compile_no_go_areas(
    no_go_areas: Sequence[Dict[str, Any]],
    *,
    default_buffer_m: float = 0.0,
    prec: int = 3,
):
    compiled = []
    geoms = []
    for idx, area in enumerate(no_go_areas or []):
        enabled = bool(area.get("enabled", True))
        if not enabled:
            continue
        polygon = area.get("polygon") or area.get("points") or area.get("outer") or []
        poly = _safe_polygon(polygon)
        buffer_m = float(area.get("buffer_m", area.get("buffer", default_buffer_m)) or 0.0)
        poly = _buffer_keepout_polygon(poly, buffer_m)
        compiled.append(
            {
                "area_id": str(area.get("area_id") or area.get("id") or f"no_go_{idx}"),
                "name": str(area.get("name") or ""),
                "buffer_m": float(max(0.0, buffer_m)),
                "geometry": _geometry_to_region_list(poly, prec=prec),
            }
        )
        geoms.append(poly)
    return compiled, geoms


def _compile_virtual_walls(
    virtual_walls: Sequence[Dict[str, Any]],
    *,
    default_buffer_m: float,
    prec: int = 3,
):
    compiled = []
    geoms = []
    for idx, wall in enumerate(virtual_walls or []):
        enabled = bool(wall.get("enabled", True))
        if not enabled:
            continue
        polyline = wall.get("polyline") or wall.get("points") or []
        pts = _normalize_open_polyline(polyline, prec=prec)
        line = LineString(pts)
        buffer_m = float(wall.get("buffer_m", default_buffer_m) or default_buffer_m)
        keepout = line.buffer(buffer_m, cap_style=2, join_style=2)
        if not keepout.is_valid:
            keepout = keepout.buffer(0.0)
        if keepout.is_empty:
            continue
        compiled.append(
            {
                "wall_id": str(wall.get("wall_id") or wall.get("id") or f"virtual_wall_{idx}"),
                "name": str(wall.get("name") or ""),
                "buffer_m": float(buffer_m),
                "geometry": _geometry_to_region_list(keepout, prec=prec),
            }
        )
        geoms.append(keepout)
    return compiled, geoms


def compile_map_constraints(
    *,
    map_id: str,
    map_md5: str,
    constraint_version: str,
    no_go_areas: Sequence[Dict[str, Any]],
    virtual_walls: Sequence[Dict[str, Any]],
    default_buffer_m: float = DEFAULT_VIRTUAL_WALL_BUFFER_M,
    default_no_go_buffer_m: float = 0.0,
    prec: int = 3,
) -> CompiledMapConstraints:
    if not _HAS_SHAPELY:
        raise RuntimeError("Shapely is required for map constraint compilation")

    no_go_compiled, _ = _compile_no_go_areas(
        no_go_areas,
        default_buffer_m=float(default_no_go_buffer_m or 0.0),
        prec=prec,
    )
    wall_compiled, _ = _compile_virtual_walls(
        virtual_walls,
        default_buffer_m=float(default_buffer_m),
        prec=prec,
    )
    return CompiledMapConstraints(
        map_id=str(map_id or "").strip(),
        map_md5=str(map_md5 or "").strip(),
        constraint_version=str(constraint_version or "").strip(),
        no_go_polygons=no_go_compiled,
        virtual_wall_keepouts=wall_compiled,
    )


def compile_zone_constraints(
    *,
    zone_outer: Sequence[Sequence[float]],
    zone_holes: Sequence[Sequence[Sequence[float]]],
    map_constraints: CompiledMapConstraints,
    prec: int = 3,
) -> CompiledZoneConstraints:
    if not _HAS_SHAPELY:
        raise RuntimeError("Shapely is required for zone constraint compilation")

    zone_poly = _safe_polygon(zone_outer, zone_holes)

    keepout_parts = []
    for area in map_constraints.no_go_polygons:
        for geom in area.get("geometry") or []:
            part = _safe_polygon(geom.get("outer") or [], geom.get("holes") or [])
            clipped = zone_poly.intersection(part)
            if not clipped.is_empty:
                keepout_parts.append(clipped)

    for wall in map_constraints.virtual_wall_keepouts:
        for geom in wall.get("geometry") or []:
            part = _safe_polygon(geom.get("outer") or [], geom.get("holes") or [])
            clipped = zone_poly.intersection(part)
            if not clipped.is_empty:
                keepout_parts.append(clipped)

    keepout_union = unary_union(keepout_parts) if keepout_parts else GeometryCollection()
    effective = zone_poly.difference(keepout_union) if keepout_parts else zone_poly
    effective_regions = _geometry_to_region_list(effective, prec=prec)
    keepout_snapshot_rings = []
    for hole in zone_holes or []:
        keepout_snapshot_rings.append(_normalize_open_ring(hole, prec=prec))
    for geom in _geometry_to_region_list(keepout_union, prec=prec):
        keepout_snapshot_rings.append(geom["outer"])

    return CompiledZoneConstraints(
        map_id=str(map_constraints.map_id or ""),
        map_md5=str(map_constraints.map_md5 or ""),
        constraint_version=str(map_constraints.constraint_version or ""),
        effective_regions=effective_regions,
        keepout_snapshot_rings=keepout_snapshot_rings,
    )


def path_points_outside_effective_regions(
    points: Sequence[Sequence[float]],
    regions: Sequence[Dict[str, Any]],
    *,
    max_examples: int = 8,
) -> List[Tuple[int, XY]]:
    if not _HAS_SHAPELY:
        return []

    polys = []
    for region in list(regions or []):
        outer = list((region or {}).get("outer") or [])
        if len(outer) < 3:
            continue
        try:
            polys.append(_safe_polygon(outer, (region or {}).get("holes") or []))
        except Exception:
            continue
    if not polys:
        return []

    allowed = unary_union(polys)
    if allowed.is_empty:
        return []
    # Tiny positive tolerance prevents numerical round-off from rejecting points
    # that lie on a generated cell boundary, while still catching keepout leaks.
    allowed = allowed.buffer(1e-6)

    offenders: List[Tuple[int, XY]] = []
    for idx, pt in enumerate(points or []):
        if pt is None or len(pt) < 2:
            continue
        xy = _round_xy(float(pt[0]), float(pt[1]), prec=6)
        if not allowed.covers(Point(float(xy[0]), float(xy[1]))):
            offenders.append((int(idx), xy))
            if len(offenders) >= int(max_examples):
                break
    return offenders

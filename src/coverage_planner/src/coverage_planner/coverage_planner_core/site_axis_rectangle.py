# -*- coding: utf-8 -*-
"""Largest usable rectangle aligned with the fixed ``site_map`` axes.

The coverage planner receives cells after the map has already been transformed
to ``site_map``.  Consequently an axis-aligned rectangle in this module is
also aligned with the building axes; the geometry must not be rotated again.

Finding the exact largest rectangle in an arbitrary polygon is a continuous
optimisation problem.  The implementation below uses deterministic horizontal
scan lines (including every exterior and hole vertex ordinate), followed by a
small local refinement around the best result.  The maximum is approximate to
``search_step_m`` while containment is *not* approximate: every returned raw
rectangle is checked with ``Polygon.covers`` before it is accepted.

Only Shapely 1.8 APIs are used.  No scipy, numpy, raster image, or GEOS
``make_valid`` dependency is required.
"""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Iterable, List, Optional, Sequence, Tuple

try:
    from shapely.geometry import LineString, Polygon, box

    _HAS_SHAPELY = True
except Exception:  # pragma: no cover - decided by the runtime image
    LineString = None
    Polygon = None
    box = None
    _HAS_SHAPELY = False


Bounds = Tuple[float, float, float, float]
Interval = Tuple[float, float]
XY = Tuple[float, float]


@dataclass(frozen=True)
class SiteAxisRectangle:
    """Result of :func:`largest_site_axis_rectangle`.

    ``raw_bounds`` is the maximum-area verified source rectangle inside the
    input polygon.  ``inset_bounds`` is produced by shrinking all four raw
    sides by exactly ``wall_margin_m``.  Candidates which would disappear after
    that shrink are rejected before the source-area comparison.
    """

    raw_bounds: Bounds
    inset_bounds: Bounds
    wall_margin_m: float
    raw_area_m2: float
    usable_area_m2: float
    effective_search_step_m: float

    # Integration-facing names: ``source`` is the maximum verified rectangle
    # before the configured margin, and ``safe`` is the four-side inset used by
    # edge-loop generation.  Keep raw/inset fields explicit for debugging and
    # backwards-friendly serialisation.
    @property
    def source_bounds(self) -> Bounds:
        return self.raw_bounds

    @property
    def safe_bounds(self) -> Bounds:
        return self.inset_bounds

    @property
    def raw_width_m(self) -> float:
        return float(self.raw_bounds[2] - self.raw_bounds[0])

    @property
    def raw_height_m(self) -> float:
        return float(self.raw_bounds[3] - self.raw_bounds[1])

    @property
    def usable_width_m(self) -> float:
        return float(self.inset_bounds[2] - self.inset_bounds[0])

    @property
    def usable_height_m(self) -> float:
        return float(self.inset_bounds[3] - self.inset_bounds[1])

    def outer_xy(self, *, inset: bool = True) -> List[XY]:
        """Return an open four-corner ring in deterministic CCW order."""

        xmin, ymin, xmax, ymax = self.inset_bounds if inset else self.raw_bounds
        return [
            (float(xmin), float(ymin)),
            (float(xmax), float(ymin)),
            (float(xmax), float(ymax)),
            (float(xmin), float(ymax)),
        ]

    def as_polygon(self, *, inset: bool = True):
        """Return the selected rectangle as a Shapely Polygon."""

        if not _HAS_SHAPELY:  # pragma: no cover
            raise RuntimeError("Shapely is required for rectangle geometry")
        return box(*(self.inset_bounds if inset else self.raw_bounds))

    @property
    def source_polygon(self):
        return self.as_polygon(inset=False)

    @property
    def safe_polygon(self):
        return self.as_polygon(inset=True)


def _iter_polygons(geometry) -> List["Polygon"]:
    if geometry is None or getattr(geometry, "is_empty", True):
        return []
    geom_type = str(getattr(geometry, "geom_type", ""))
    if geom_type == "Polygon":
        return [geometry]
    if geom_type in ("MultiPolygon", "GeometryCollection"):
        out: List["Polygon"] = []
        for part in geometry.geoms:
            out.extend(_iter_polygons(part))
        return out
    return []


def _normalise_components(geometry, absolute_tolerance_m: float) -> List["Polygon"]:
    if not _HAS_SHAPELY:
        raise RuntimeError("Shapely is required for site-axis rectangle search")
    if geometry is None or getattr(geometry, "is_empty", True):
        return []

    repaired = geometry
    if not bool(getattr(repaired, "is_valid", False)):
        # buffer(0) is supported by Shapely 1.8 and is already the repair
        # convention used by the rest of coverage_planner.
        repaired = repaired.buffer(0.0)

    min_area = max(float(absolute_tolerance_m) ** 2, 1e-14)
    return [
        poly
        for poly in _iter_polygons(repaired)
        if (not poly.is_empty) and poly.is_valid and float(poly.area) > min_area
    ]


def _ring_vertex_ys(poly: "Polygon") -> Iterable[float]:
    for _, y in poly.exterior.coords:
        yield float(y)
    for ring in poly.interiors:
        for _, y in ring.coords:
            yield float(y)


def _unique_sorted(values: Sequence[float], tolerance: float) -> List[float]:
    result: List[float] = []
    for value in sorted(float(v) for v in values if math.isfinite(float(v))):
        if not result or value - result[-1] > tolerance:
            result.append(value)
    return result


def _initial_levels(
    poly: "Polygon",
    *,
    search_step_m: float,
    max_scanlines: int,
    tolerance_m: float,
) -> Tuple[List[float], float]:
    _, ymin, _, ymax = (float(v) for v in poly.bounds)
    height = float(ymax - ymin)
    if height <= tolerance_m:
        return [], height

    requested_steps = max(1, int(math.ceil(height / search_step_m)))
    steps = min(requested_steps, max(2, int(max_scanlines) - 1))
    effective_step = height / float(steps)
    values = [ymin + effective_step * i for i in range(steps + 1)]

    # At a polygon/hole vertex, the exact horizontal intersection may contain
    # only a touching point.  Probe both adjacent linear edge bands as well.
    # The delta is deliberately tiny compared with the search resolution and
    # is scaled above GEOS floating-point noise.
    probe_delta = min(
        effective_step * 1e-5,
        max(tolerance_m * 8.0, height * 1e-11),
    )
    for vertex_y in _ring_vertex_ys(poly):
        if ymin - tolerance_m <= vertex_y <= ymax + tolerance_m:
            values.append(min(ymax, max(ymin, vertex_y)))
            if vertex_y > ymin + probe_delta:
                values.append(vertex_y - probe_delta)
            if vertex_y < ymax - probe_delta:
                values.append(vertex_y + probe_delta)

    return _unique_sorted(values, tolerance_m * 0.25), effective_step


def _collect_line_intervals(geometry, out: List[Interval], tolerance_m: float) -> None:
    if geometry is None or geometry.is_empty:
        return
    geom_type = str(geometry.geom_type)
    if geom_type in ("LineString", "LinearRing"):
        xmin, _, xmax, _ = geometry.bounds
        if float(xmax - xmin) > tolerance_m:
            out.append((float(xmin), float(xmax)))
        return
    if geom_type in ("MultiLineString", "GeometryCollection"):
        for part in geometry.geoms:
            _collect_line_intervals(part, out, tolerance_m)


def _merge_intervals(intervals: Sequence[Interval], tolerance_m: float) -> List[Interval]:
    merged: List[List[float]] = []
    for xmin, xmax in sorted(intervals):
        if xmax - xmin <= tolerance_m:
            continue
        if not merged or xmin > merged[-1][1] + tolerance_m:
            merged.append([float(xmin), float(xmax)])
        else:
            merged[-1][1] = max(merged[-1][1], float(xmax))
    return [(item[0], item[1]) for item in merged]


def _horizontal_intervals(
    poly: "Polygon",
    y: float,
    *,
    line_xmin: float,
    line_xmax: float,
    tolerance_m: float,
) -> List[Interval]:
    section = poly.intersection(LineString([(line_xmin, y), (line_xmax, y)]))
    intervals: List[Interval] = []
    _collect_line_intervals(section, intervals, tolerance_m)
    return _merge_intervals(intervals, tolerance_m)


def _intersect_interval_sets(
    left: Sequence[Interval],
    right: Sequence[Interval],
    tolerance_m: float,
) -> List[Interval]:
    result: List[Interval] = []
    i = 0
    j = 0
    while i < len(left) and j < len(right):
        xmin = max(float(left[i][0]), float(right[j][0]))
        xmax = min(float(left[i][1]), float(right[j][1]))
        if xmax - xmin > tolerance_m:
            result.append((xmin, xmax))
        if left[i][1] < right[j][1] - tolerance_m:
            i += 1
        elif right[j][1] < left[i][1] - tolerance_m:
            j += 1
        else:
            i += 1
            j += 1
    return result


def _contract_bounds(bounds: Bounds, amount: float) -> Optional[Bounds]:
    xmin, ymin, xmax, ymax = bounds
    contracted = (xmin + amount, ymin + amount, xmax - amount, ymax - amount)
    if contracted[2] <= contracted[0] or contracted[3] <= contracted[1]:
        return None
    return contracted


def _candidate_is_better(
    candidate: SiteAxisRectangle,
    incumbent: Optional[SiteAxisRectangle],
    area_tolerance: float,
) -> bool:
    if incumbent is None:
        return True
    if candidate.raw_area_m2 > incumbent.raw_area_m2 + area_tolerance:
        return True
    if abs(candidate.raw_area_m2 - incumbent.raw_area_m2) > area_tolerance:
        return False
    if candidate.usable_area_m2 > incumbent.usable_area_m2 + area_tolerance:
        return True
    if abs(candidate.usable_area_m2 - incumbent.usable_area_m2) > area_tolerance:
        return False

    candidate_min_side = min(candidate.usable_width_m, candidate.usable_height_m)
    incumbent_min_side = min(incumbent.usable_width_m, incumbent.usable_height_m)
    if candidate_min_side > incumbent_min_side + math.sqrt(area_tolerance):
        return True
    if abs(candidate_min_side - incumbent_min_side) > math.sqrt(area_tolerance):
        return False
    # Stable result when symmetric alternatives have exactly the same score.
    return tuple(candidate.raw_bounds) < tuple(incumbent.raw_bounds)


def _search_levels(
    poly: "Polygon",
    levels: Sequence[float],
    *,
    wall_margin_m: float,
    min_usable_side_m: float,
    tolerance_m: float,
    effective_step_m: float,
    incumbent: Optional[SiteAxisRectangle] = None,
) -> Optional[SiteAxisRectangle]:
    if len(levels) < 2:
        return incumbent

    xmin, _, xmax, _ = (float(v) for v in poly.bounds)
    pad = max(1.0, xmax - xmin) * 0.01 + tolerance_m
    rows = [
        _horizontal_intervals(
            poly,
            y,
            line_xmin=xmin - pad,
            line_xmax=xmax + pad,
            tolerance_m=tolerance_m,
        )
        for y in levels
    ]

    best = incumbent
    component_area = max(float(poly.area), 1.0)
    area_tolerance = max(1e-12, component_area * 1e-11)

    for bottom_idx in range(len(levels) - 1):
        common = list(rows[bottom_idx])
        if not common:
            continue
        bottom = float(levels[bottom_idx])

        for top_idx in range(bottom_idx + 1, len(levels)):
            common = _intersect_interval_sets(common, rows[top_idx], tolerance_m)
            if not common:
                break

            top = float(levels[top_idx])
            raw_height = top - bottom
            usable_height = raw_height - 2.0 * wall_margin_m
            if usable_height + tolerance_m < min_usable_side_m:
                continue

            for left, right in common:
                raw_width = float(right - left)
                usable_width = raw_width - 2.0 * wall_margin_m
                if usable_width + tolerance_m < min_usable_side_m:
                    continue
                raw_area = raw_width * raw_height
                usable_area = max(0.0, usable_width) * max(0.0, usable_height)
                if best is not None and raw_area < best.raw_area_m2 - area_tolerance:
                    continue

                raw_bounds: Optional[Bounds] = (
                    float(left),
                    float(bottom),
                    float(right),
                    float(top),
                )
                raw_rect = box(*raw_bounds)
                if not poly.covers(raw_rect):
                    # Common scan intervals are exact for linear polygon edges,
                    # but GEOS can disagree by a few ulps at touching vertices.
                    # Contract only by the declared numerical tolerance, then
                    # perform the same hard containment check again.
                    raw_bounds = _contract_bounds(raw_bounds, tolerance_m)
                    if raw_bounds is None or not poly.covers(box(*raw_bounds)):
                        continue

                inset_bounds = (
                    raw_bounds[0] + wall_margin_m,
                    raw_bounds[1] + wall_margin_m,
                    raw_bounds[2] - wall_margin_m,
                    raw_bounds[3] - wall_margin_m,
                )
                usable_width = inset_bounds[2] - inset_bounds[0]
                usable_height = inset_bounds[3] - inset_bounds[1]
                if (
                    usable_width + tolerance_m < min_usable_side_m
                    or usable_height + tolerance_m < min_usable_side_m
                ):
                    continue

                candidate = SiteAxisRectangle(
                    raw_bounds=raw_bounds,
                    inset_bounds=inset_bounds,
                    wall_margin_m=wall_margin_m,
                    raw_area_m2=(raw_bounds[2] - raw_bounds[0])
                    * (raw_bounds[3] - raw_bounds[1]),
                    usable_area_m2=usable_width * usable_height,
                    effective_search_step_m=effective_step_m,
                )
                if _candidate_is_better(candidate, best, area_tolerance):
                    best = candidate

    return best


def _refined_levels(
    levels: Sequence[float],
    best: SiteAxisRectangle,
    *,
    local_step_m: float,
    ymin: float,
    ymax: float,
    tolerance_m: float,
) -> List[float]:
    values = list(levels)
    for center in (best.raw_bounds[1], best.raw_bounds[3]):
        for multiplier in (-0.75, -0.5, -0.25, 0.25, 0.5, 0.75):
            value = center + local_step_m * multiplier
            if ymin < value < ymax:
                values.append(value)
    return _unique_sorted(values, tolerance_m * 0.25)


def largest_site_axis_rectangle(
    geometry,
    *,
    wall_margin_m: float = 0.0,
    search_step_m: float = 0.05,
    max_scanlines: int = 256,
    refine_steps: int = 2,
    min_usable_side_m: float = 0.05,
    absolute_tolerance_m: float = 1e-7,
) -> Optional[SiteAxisRectangle]:
    """Find the largest usable axis-aligned rectangle covered by ``geometry``.

    Args:
        geometry: Shapely Polygon, MultiPolygon, or polygonal collection in
            ``site_map`` coordinates.  Polygon holes remain forbidden.
        wall_margin_m: Distance subsequently removed from *each* of the four
            rectangle sides.
        search_step_m: Requested global sampling resolution for the continuous
            vertical optimisation.  Containment does not depend on this value.
        max_scanlines: Bounds runtime for very large cells.  Polygon and hole
            vertex scan lines are always retained in addition to this budget.
        refine_steps: Local quarter-step refinements around the current best
            bottom and top ordinates.
        min_usable_side_m: Reject inset rectangles with either side shorter
            than this value.
        absolute_tolerance_m: Maximum numerical-only contraction allowed when
            GEOS rejects an otherwise touching rectangle.

    Returns:
        A :class:`SiteAxisRectangle`, or ``None`` when no usable rectangle
        remains.  The returned raw and inset polygons are both guaranteed to be
        covered by one repaired component of the input geometry.
    """

    numeric_args = {
        "wall_margin_m": wall_margin_m,
        "search_step_m": search_step_m,
        "min_usable_side_m": min_usable_side_m,
        "absolute_tolerance_m": absolute_tolerance_m,
    }
    for name, value in numeric_args.items():
        if not math.isfinite(float(value)):
            raise ValueError("%s must be finite" % name)
    if wall_margin_m < 0.0:
        raise ValueError("wall_margin_m must be non-negative")
    if search_step_m <= 0.0:
        raise ValueError("search_step_m must be positive")
    if min_usable_side_m <= 0.0:
        raise ValueError("min_usable_side_m must be positive")
    if absolute_tolerance_m <= 0.0:
        raise ValueError("absolute_tolerance_m must be positive")
    if int(max_scanlines) < 3:
        raise ValueError("max_scanlines must be at least 3")
    if int(refine_steps) < 0:
        raise ValueError("refine_steps must be non-negative")

    components = _normalise_components(geometry, absolute_tolerance_m)
    global_best: Optional[SiteAxisRectangle] = None

    for poly in components:
        minx, miny, maxx, maxy = (float(v) for v in poly.bounds)
        span = max(maxx - minx, maxy - miny, 1.0)
        coordinate_scale = max(abs(minx), abs(miny), abs(maxx), abs(maxy), 1.0)
        tolerance_m = max(
            float(absolute_tolerance_m),
            span * 1e-10,
            coordinate_scale * 1e-12,
        )
        levels, effective_step = _initial_levels(
            poly,
            search_step_m=float(search_step_m),
            max_scanlines=int(max_scanlines),
            tolerance_m=tolerance_m,
        )
        best = _search_levels(
            poly,
            levels,
            wall_margin_m=float(wall_margin_m),
            min_usable_side_m=float(min_usable_side_m),
            tolerance_m=tolerance_m,
            effective_step_m=effective_step,
        )

        local_step = effective_step
        for _ in range(int(refine_steps)):
            if best is None:
                break
            levels = _refined_levels(
                levels,
                best,
                local_step_m=local_step,
                ymin=miny,
                ymax=maxy,
                tolerance_m=tolerance_m,
            )
            local_step *= 0.25
            best = _search_levels(
                poly,
                levels,
                wall_margin_m=float(wall_margin_m),
                min_usable_side_m=float(min_usable_side_m),
                tolerance_m=tolerance_m,
                effective_step_m=local_step,
                incumbent=best,
            )

        if best is not None:
            component_area = max(float(poly.area), 1.0)
            if _candidate_is_better(best, global_best, component_area * 1e-11):
                global_best = best

    return global_best


# Explicit alias for call sites that want the post-margin intent in the name.
largest_site_axis_inset_rectangle = largest_site_axis_rectangle

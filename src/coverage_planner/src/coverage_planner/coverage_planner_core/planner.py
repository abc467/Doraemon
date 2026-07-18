# coverage_planner_core/planner.py
# -*- coding: utf-8 -*-

import math
from typing import List, Tuple, Callable, Optional, Dict, Any

import fields2cover as f2c

from coverage_planner.constraints import path_points_outside_effective_regions

from .types import RobotSpec, PlannerParams, PlanResult, BlockPlan, BlockDebug
from .geom import resample_polyline_uniform, yaw_list_from_pts, polyline_length_xy
from .f2c_adapter import (
    build_cells_from_polygons, build_cells_from_regions, build_robot, build_swath_generator, build_objective,
    build_turn_planner, generate_best_swaths, snake_sorted_swaths, recon_snake_polyline,
    swaths_size, swath_at, swath_endpoints_xyz, cell_outer_ring_xy
)
from .shrink import cell_to_shapely_polygon, long_side_shrink_cells, shapely_to_f2c_cells
from .site_axis_rectangle import largest_site_axis_rectangle
from .stitch import StitchParams, build_edge_loop_from_rawcell_bbox, stitch_with_edge_loop
from .exec_order import nearest_neighbor_exec_order


XY = Tuple[float, float]


def _bbox_edge_loop_safe_for_cell(cell_geom) -> bool:
    poly = cell_to_shapely_polygon(cell_geom)
    if poly is None or poly.is_empty:
        return False
    if len(getattr(poly, "interiors", []) or []) > 0:
        return False
    minx, miny, maxx, maxy = poly.bounds
    bbox_area = float(maxx - minx) * float(maxy - miny)
    if bbox_area <= 1e-9:
        return False
    # The legacy edge pass is a rectangle-bbox sweep. It is safe only when the
    # actual cell is essentially that rectangle; concave cells/holes are handled
    # by the snake path so we do not drive through keepout cut-outs.
    return abs(float(poly.area) - bbox_area) <= max(1e-6, bbox_area * 0.005)


def _planning_cell_candidates(
    raw_cell,
    wall_margin_m: float,
    path_step_m: float,
) -> List[Dict[str, Any]]:
    """Prepare geometry before swath generation.

    The legacy rectangular-cell flow is kept unchanged.  A cell which would
    previously have produced ``edge_loop_skipped=non_rectangular_or_hole_cell``
    is instead reduced to one rectangle aligned with the already-active
    ``site_map`` axes.  Both its edge loop and swaths then derive from the same
    four-side-inset rectangle.
    """

    wall_margin = max(0.0, float(wall_margin_m))
    legacy_safe_cells = (
        long_side_shrink_cells(raw_cell, wall_margin)
        if wall_margin > 1e-9
        else [raw_cell]
    )
    if not legacy_safe_cells:
        return []

    raw_outer_xy = cell_outer_ring_xy(raw_cell)
    regular_legacy_cell = (
        len(legacy_safe_cells) == 1
        and _bbox_edge_loop_safe_for_cell(legacy_safe_cells[0])
    )
    if wall_margin <= 1e-9 or regular_legacy_cell:
        return [
            {
                "cell": cell,
                "edge_source_xy": raw_outer_xy,
                "edge_loop_safe": _bbox_edge_loop_safe_for_cell(cell),
                "geometry_mode": "legacy_long_side_shrink",
                "site_rect": None,
                "raw_cell_area_m2": None,
            }
            for cell in legacy_safe_cells
        ]

    raw_poly = cell_to_shapely_polygon(raw_cell)
    if raw_poly is not None and not raw_poly.is_empty:
        try:
            site_rect = largest_site_axis_rectangle(
                raw_poly,
                wall_margin_m=wall_margin,
                search_step_m=max(0.01, float(path_step_m)),
                min_usable_side_m=0.05,
            )
        except (RuntimeError, ValueError):
            site_rect = None

        if site_rect is not None:
            rect_cells = shapely_to_f2c_cells(site_rect.safe_polygon)
            if len(rect_cells) == 1:
                return [
                    {
                        "cell": rect_cells[0],
                        # The existing builder applies wall_margin once.  Give
                        # it the source rectangle, while swaths use safe_polygon,
                        # so both resolve to exactly the same inset bounds.
                        "edge_source_xy": site_rect.outer_xy(inset=False),
                        "edge_loop_safe": True,
                        "geometry_mode": "site_axis_inscribed_rectangle",
                        "site_rect": site_rect,
                        "raw_cell_area_m2": float(raw_poly.area),
                    }
                ]

    # Keep the pre-existing behavior only when no usable rectangle can be
    # extracted (for example, when Shapely is unavailable).  Successful
    # rectangle extraction never reaches this branch.
    return [
        {
            "cell": cell,
            "edge_source_xy": raw_outer_xy,
            "edge_loop_safe": _bbox_edge_loop_safe_for_cell(cell),
            "geometry_mode": "legacy_non_rectangular_fallback",
            "site_rect": None,
            "raw_cell_area_m2": float(raw_poly.area) if raw_poly is not None else None,
        }
        for cell in legacy_safe_cells
    ]


def _path_region_violation_message(path_xy: List[XY], effective_regions: Optional[List[Dict[str, Any]]]) -> str:
    if not effective_regions:
        return ""
    offenders = path_points_outside_effective_regions(path_xy, effective_regions, max_examples=5)
    if not offenders:
        return ""
    sample = ", ".join(
        "#%d=(%.3f,%.3f)" % (idx, xy[0], xy[1])
        for idx, xy in offenders
    )
    return "coverage path leaves effective region at %s" % sample


def plan_coverage(
    frame_id: str,
    outer: List[XY],
    holes: List[List[XY]],
    robot_spec: RobotSpec,
    params: PlannerParams,
    effective_regions: Optional[List[Dict[str, Any]]] = None,
    debug: bool = False,
    preempt_cb: Optional[Callable[[], bool]] = None,
) -> PlanResult:
    """
    Productized core planning entry.
    Keeps your R&D logic the same; no ROS; no file I/O.
    """
    preempt_cb = preempt_cb or (lambda: False)

    try:
        # build robot + f2c pipeline objects
        robot = build_robot(robot_spec.cov_width, robot_spec.width,
                            robot_spec.min_turning_radius, robot_spec.max_diff_curv)
        r_w = float(robot_spec.cov_width)
        R = float(robot_spec.min_turning_radius)

        split_angle_rad = math.radians(float(params.split_angle_deg))

        if effective_regions:
            cells = build_cells_from_regions(effective_regions)
        else:
            cells = build_cells_from_polygons(outer, holes)

        decomp = f2c.DECOMP_Boustrophedon()
        if hasattr(decomp, "setSplitAngle"):
            decomp.setSplitAngle(float(split_angle_rad))
        subcells = decomp.decompose(cells)
        n_sub = int(subcells.size()) if hasattr(subcells, "size") else 0

        sg = build_swath_generator()
        obj = build_objective()
        turn_planner = build_turn_planner(params.turn_model)

        wall_margin = max(0.0, float(params.wall_margin_m))
        turn_margin = max(0.0, float(params.turn_margin_m))

        edge_r = float(params.edge_corner_radius_m) if params.edge_corner_radius_m >= 0.0 else float(R)
        edge_r = max(edge_r, float(R))

        blocks: List[BlockPlan] = []
        block_infos: List[Dict[str, Any]] = []
        block_id = 0

        for bi in range(n_sub):
            if preempt_cb():
                return PlanResult(False, "PREEMPTED", "planning canceled", frame_id, [], [], 0.0)

            raw_cell = subcells.getGeometry(bi)

            planning_cells = _planning_cell_candidates(
                raw_cell,
                wall_margin_m=wall_margin,
                path_step_m=float(params.path_step_m),
            )
            if not planning_cells:
                continue

            for planning_cell in planning_cells:
                if preempt_cb():
                    return PlanResult(False, "PREEMPTED", "planning canceled", frame_id, [], [], 0.0)

                cell_b = planning_cell["cell"]
                site_rect = planning_cell.get("site_rect")
                # ``turn_margin`` historically measures total setback from the
                # source Cell boundary.  A site-axis rectangle has already
                # applied its four-side wall margin before swaths are created,
                # so subtract that pre-applied part instead of counting it a
                # second time at both short ends.
                preapplied_turn_margin = (
                    max(0.0, float(site_rect.wall_margin_m))
                    if site_rect is not None else 0.0
                )
                effective_turn_margin = max(
                    0.0,
                    float(turn_margin) - preapplied_turn_margin,
                )

                # 1) swaths
                swaths_b = generate_best_swaths(sg, obj, r_w, cell_b, mute=params.mute_stderr)
                nsb = swaths_size(swaths_b)
                if nsb <= 0:
                    block_id += 1
                    continue

                swath_segs = []
                if debug:
                    for si in range(nsb):
                        sw = swath_at(swaths_b, si)
                        if sw is None:
                            continue
                        a, b = swath_endpoints_xyz(sw)
                        if a is None or b is None:
                            continue
                        swath_segs.append((a, b))

                # 2) snake sorted swaths
                sw_sorted = snake_sorted_swaths(
                    swaths_b,
                    mute=params.mute_stderr,
                    min_swath_length_m=max(
                        0.0,
                        float(params.min_swath_length_m),
                    ),
                )
                ns_sorted = swaths_size(sw_sorted) if sw_sorted is not None else 0
                if ns_sorted <= 0:
                    block_id += 1
                    continue

                # 3) recon snake raw + resample
                snake_raw = recon_snake_polyline(
                    robot, sw_sorted, turn_planner,
                    turn_step_m=float(params.turn_step_m),
                    turn_margin_m=float(effective_turn_margin),
                    mute=params.mute_stderr
                )
                if len(snake_raw) < 2:
                    block_id += 1
                    continue

                snake_pts_path = resample_polyline_uniform(snake_raw, float(params.path_step_m))
                snake_pts_viz  = resample_polyline_uniform(snake_raw, float(params.viz_step_m))
                if len(snake_pts_path) < 2:
                    block_id += 1
                    continue

                yaws = yaw_list_from_pts(snake_pts_path)
                entry_xy = snake_pts_path[0]
                exit_xy  = snake_pts_path[-1]
                entry_yaw = yaws[0] if yaws else 0.0
                exit_yaw  = yaws[-1] if yaws else 0.0

                # 4) Edge loop.  Regular cells retain the legacy raw bbox
                # source; rectangle-core cells use their verified source
                # rectangle, which resolves to the same safe bounds as swaths.
                edge_source_xy = planning_cell["edge_source_xy"]

                edge_vertices: List[XY] = []
                edge_dense: List[XY] = []
                edge_close_gap = 0.0
                edge_len = 0.0
                edge_loop_skipped = ""
                if bool(planning_cell["edge_loop_safe"]):
                    edge_vertices, edge_dense, edge_close_gap, edge_len = build_edge_loop_from_rawcell_bbox(
                        edge_source_xy, wall_margin, edge_r,
                        params.edge_corner_pull, params.path_step_m, params.edge_corner_min_pts
                    )
                elif wall_margin > 1e-9:
                    edge_loop_skipped = "non_rectangular_or_hole_cell"

                # 5) stitch flow (if edge exists), else final=snake
                final_pts = snake_pts_path[:]
                stitch_dbg = None

                if len(edge_dense) >= 4:
                    sp = StitchParams(
                        path_step_m=float(params.path_step_m),
                        viz_step_m=float(params.viz_step_m),
                        wall_margin_m=float(wall_margin),
                        edge_corner_radius_m=float(edge_r),
                        edge_corner_pull=float(params.edge_corner_pull),
                        edge_corner_min_pts=int(params.edge_corner_min_pts),
                        pre_proj_min=float(params.pre_proj_min),
                        pre_proj_max=float(params.pre_proj_max),
                        pre_prefix_max=float(params.pre_prefix_max),
                        e_pre_min=float(params.e_pre_min),
                        e_pre_max=float(params.e_pre_max),
                    )
                    final_pts, stitch_dbg = stitch_with_edge_loop(
                        snake_pts_path=snake_pts_path,
                        raw_entry_yaw=entry_yaw,
                        edge_dense=edge_dense,
                        sp=sp
                    )
                    if bool(params.validate_effective_region_path):
                        violation = _path_region_violation_message(final_pts, effective_regions)
                        if violation:
                            snake_violation = _path_region_violation_message(snake_pts_path, effective_regions)
                            if snake_violation:
                                return PlanResult(False, "PATH_OUTSIDE_EFFECTIVE_REGION", snake_violation, frame_id, [], [], 0.0)
                            final_pts = snake_pts_path[:]
                            stitch_dbg = None
                            edge_loop_skipped = "bbox_edge_loop_rejected_by_region_guard"
                            edge_dense = []
                            edge_vertices = []
                            edge_close_gap = 0.0
                            edge_len = 0.0
                else:
                    if bool(params.validate_effective_region_path):
                        violation = _path_region_violation_message(final_pts, effective_regions)
                        if violation:
                            return PlanResult(False, "PATH_OUTSIDE_EFFECTIVE_REGION", violation, frame_id, [], [], 0.0)

                # debug data
                dbg = None
                if debug:
                    dbg = BlockDebug(
                        swath_segs_xyz=swath_segs if swath_segs else None,
                        snake_pts_xy=snake_pts_viz if snake_pts_viz else None,
                        edge_loop_xy=edge_dense if edge_dense else None,
                        conn1_xy=stitch_dbg.conn1 if stitch_dbg else None,
                        conn2_xy=stitch_dbg.conn2 if stitch_dbg else None,
                        keypts=stitch_dbg.keypts if stitch_dbg else None,
                        entry_xy=entry_xy,
                        exit_xy=exit_xy,
                    )

                stats = {
                    "cell_geometry_mode": str(planning_cell["geometry_mode"]),
                    "configured_turn_margin_m": float(turn_margin),
                    "preapplied_turn_margin_m": float(preapplied_turn_margin),
                    "effective_turn_margin_m": float(effective_turn_margin),
                    "turn_margin_reference": "source_cell_boundary_total",
                    "turn_margin_clamped": bool(
                        preapplied_turn_margin > float(turn_margin) + 1e-9
                    ),
                    "swaths": nsb,
                    "swaths_retained": int(ns_sorted),
                    "swaths_filtered": int(max(0, nsb - ns_sorted)),
                    "snake_raw_pts": len(snake_raw),
                    "snake_path_pts": len(snake_pts_path),
                    "edge_len": float(edge_len),
                    "edge_pts": int(len(edge_dense) if edge_dense else 0),
                    "edge_close_gap": float(edge_close_gap),
                    "final_pts": int(len(final_pts)),
                    "final_len": float(polyline_length_xy(final_pts)),
                }
                if site_rect is not None:
                    raw_cell_area = float(planning_cell.get("raw_cell_area_m2") or 0.0)
                    stats.update({
                        "rectangle_trigger": "non_rectangular_or_hole_cell",
                        "rectangle_source_bounds": [float(v) for v in site_rect.source_bounds],
                        "rectangle_safe_bounds": [float(v) for v in site_rect.safe_bounds],
                        "rectangle_source_area_m2": float(site_rect.raw_area_m2),
                        "rectangle_safe_area_m2": float(site_rect.usable_area_m2),
                        "rectangle_safe_retained_ratio": (
                            float(site_rect.usable_area_m2) / raw_cell_area
                            if raw_cell_area > 1e-9 else 0.0
                        ),
                        "rectangle_wall_margin_m": float(site_rect.wall_margin_m),
                        "rectangle_search_step_m": float(site_rect.effective_search_step_m),
                    })
                if edge_loop_skipped:
                    stats["edge_loop_skipped"] = edge_loop_skipped

                blocks.append(
                    BlockPlan(
                        block_id=block_id,
                        path_xy=final_pts,
                        entry_xyyaw=(entry_xy[0], entry_xy[1], entry_yaw),
                        exit_xyyaw=(exit_xy[0], exit_xy[1], exit_yaw),
                        stats=stats,
                        debug=dbg
                    )
                )

                block_infos.append({
                    "id": block_id,
                    "entry_xy": entry_xy,
                    "exit_xy": exit_xy,
                })

                block_id += 1

        if not blocks:
            return PlanResult(False, "NO_BLOCKS", "no blocks produced", frame_id, [], [], 0.0)

        # exec order (by entry/exit NN, fixed start=block_0)
        # NOTE: nearest_neighbor_exec_order returns indices into block_infos
        order_idx = nearest_neighbor_exec_order(block_infos)
        exec_order = [block_infos[i]["id"] for i in order_idx]

        total_len = 0.0
        for b in blocks:
            total_len += float(b.stats.get("final_len", 0.0))

        return PlanResult(True, "", "", frame_id, blocks, exec_order, float(total_len))

    except Exception as e:
        return PlanResult(False, "EXCEPTION", str(e), frame_id, [], [], 0.0)

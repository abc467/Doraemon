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
from .shrink import cell_to_shapely_polygon, long_side_shrink_cells
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

            safe_cells = long_side_shrink_cells(raw_cell, wall_margin) if wall_margin > 1e-9 else [raw_cell]
            if not safe_cells:
                continue

            for cell_b in safe_cells:
                if preempt_cb():
                    return PlanResult(False, "PREEMPTED", "planning canceled", frame_id, [], [], 0.0)

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
                sw_sorted = snake_sorted_swaths(swaths_b, mute=params.mute_stderr)
                if sw_sorted is None or swaths_size(sw_sorted) <= 0:
                    block_id += 1
                    continue

                # 3) recon snake raw + resample
                snake_raw = recon_snake_polyline(
                    robot, sw_sorted, turn_planner,
                    turn_step_m=float(params.turn_step_m),
                    turn_margin_m=float(turn_margin),
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

                # 4) edge loop from raw_cell bbox shrunken by wall_margin (all 4 sides)
                raw_outer_xy = cell_outer_ring_xy(raw_cell)

                edge_vertices: List[XY] = []
                edge_dense: List[XY] = []
                edge_close_gap = 0.0
                edge_len = 0.0
                edge_loop_skipped = ""
                if _bbox_edge_loop_safe_for_cell(cell_b):
                    edge_vertices, edge_dense, edge_close_gap, edge_len = build_edge_loop_from_rawcell_bbox(
                        raw_outer_xy, wall_margin, edge_r,
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
                    "swaths": nsb,
                    "snake_raw_pts": len(snake_raw),
                    "snake_path_pts": len(snake_pts_path),
                    "edge_len": float(edge_len),
                    "edge_pts": int(len(edge_dense) if edge_dense else 0),
                    "edge_close_gap": float(edge_close_gap),
                    "final_pts": int(len(final_pts)),
                    "final_len": float(polyline_length_xy(final_pts)),
                }
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

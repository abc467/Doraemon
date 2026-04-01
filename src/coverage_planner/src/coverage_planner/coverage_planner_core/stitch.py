# coverage_planner_core/stitch.py
# -*- coding: utf-8 -*-

from dataclasses import dataclass
from typing import List, Tuple, Dict, Optional

from .geom import (
    dist_xy, polyline_length_xy, resample_polyline_uniform,
    build_edge_loop_from_rect_vertices, densify_closed_loop_uniform,
    nearest_index, rotate_polyline_to_index,
    sample_straight, bbox_from_pts_xy,
    find_P_pre_by_forward_projection, find_edge_point_by_arclen_window,
    yaw_list_from_pts
)

XY = Tuple[float, float]


@dataclass
class StitchParams:
    path_step_m: float
    viz_step_m: float
    wall_margin_m: float
    edge_corner_radius_m: float
    edge_corner_pull: float
    edge_corner_min_pts: int
    pre_proj_min: float
    pre_proj_max: float
    pre_prefix_max: float
    e_pre_min: float
    e_pre_max: float


@dataclass
class StitchDebug:
    edge_vertices: Optional[List[XY]] = None
    edge_dense: Optional[List[XY]] = None
    edge_close_gap: float = 0.0
    edge_len: float = 0.0
    E_start_near: float = 0.0
    join_d: float = 0.0
    join_idx: int = 0
    keypts: Optional[Dict[str, XY]] = None
    conn1: Optional[List[XY]] = None
    conn2: Optional[List[XY]] = None
    edge_lap: Optional[List[XY]] = None


def build_edge_loop_from_rawcell_bbox(
    raw_cell_outer_xy: List[XY],
    wall_margin: float,
    edge_r: float,
    corner_pull: float,
    path_step_m: float,
    min_corner_pts: int
) -> Tuple[List[XY], List[XY], float, float]:
    """
    Replicates your script:
      - raw_cell bbox shrink all 4 sides by wall_margin
      - build rect loop with bezier corners
      - densify whole loop to ~path_step
    Returns: (edge_vertices, edge_dense, close_gap, edge_len)
    """
    if len(raw_cell_outer_xy) < 3 or wall_margin <= 1e-9:
        return [], [], 0.0, 0.0

    xmin, ymin, xmax, ymax = bbox_from_pts_xy(raw_cell_outer_xy)
    xmin2 = xmin + wall_margin
    ymin2 = ymin + wall_margin
    xmax2 = xmax - wall_margin
    ymax2 = ymax - wall_margin

    if (xmax2 - xmin2) <= 0.2 or (ymax2 - ymin2) <= 0.2:
        return [], [], 0.0, 0.0

    edge_vertices = build_edge_loop_from_rect_vertices(
        xmin2, ymin2, xmax2, ymax2,
        corner_r=edge_r,
        corner_pull=float(corner_pull),
        step_m=float(path_step_m),
        min_corner_pts=int(min_corner_pts)
    )
    edge_dense = densify_closed_loop_uniform(edge_vertices, float(path_step_m))

    if len(edge_dense) < 4:
        return edge_vertices, edge_dense, 0.0, 0.0

    close_gap = dist_xy(edge_dense[-1], edge_dense[0])
    edge_len = polyline_length_xy(edge_dense) + close_gap
    return edge_vertices, edge_dense, close_gap, edge_len


def stitch_with_edge_loop(
    snake_pts_path: List[XY],
    raw_entry_yaw: float,
    edge_dense: List[XY],
    sp: StitchParams
) -> Tuple[List[XY], StitchDebug]:
    """
    Implements your exact stitch flow:
    1) P_pre by forward-projection window
    2) E_start nearest to P_pre on dense edge
    3) entry->E_start straight
    4) edge loop lap (E_start ... back to E_start)
    5) e_pre by arclen window from E_start
    6) p_join nearest snake to e_pre
    7) E_start->p_join straight
    8) snake remainder from p_join
    Finally resample to path_step_m
    """
    dbg = StitchDebug()

    if not snake_pts_path or len(snake_pts_path) < 2 or len(edge_dense) < 4:
        return snake_pts_path[:], dbg

    entry_xy = snake_pts_path[0]

    # (1) P_pre
    P_pre = find_P_pre_by_forward_projection(
        snake_pts_path,
        entry_yaw=raw_entry_yaw,
        proj_min=float(sp.pre_proj_min),
        proj_max=float(sp.pre_proj_max),
        max_prefix_arclen=float(sp.pre_prefix_max)
    )

    # (2) E_start nearest to P_pre on dense edge, rotate so E_start at index 0
    i_es, d_es = nearest_index(edge_dense, P_pre)
    edge_rot = rotate_polyline_to_index(edge_dense, i_es)
    E_start = edge_rot[0]
    dbg.E_start_near = float(d_es)

    # (3) entry -> E_start (straight)
    conn1 = sample_straight(entry_xy, E_start, float(sp.path_step_m))

    # (4) edge loop lap
    edge_lap = edge_rot[:] + [E_start]

    # (5) e_pre by arclen window
    e_pre = find_edge_point_by_arclen_window(edge_rot, float(sp.e_pre_min), float(sp.e_pre_max))

    # (6) p_join nearest snake to e_pre
    join_idx = 0
    best_d = 1e18
    for i, p in enumerate(snake_pts_path):
        d = dist_xy(p, e_pre)
        if d < best_d:
            best_d = d
            join_idx = i
    p_join = snake_pts_path[join_idx]
    dbg.join_idx = int(join_idx)
    dbg.join_d = float(best_d)

    # (7) connect E_start -> p_join
    conn2 = sample_straight(E_start, p_join, float(sp.path_step_m))

    # (8) snake remainder
    snake_rem = snake_pts_path[join_idx:]

    # merge raw then resample
    final_raw: List[XY] = []
    final_raw += conn1
    final_raw += edge_lap[1:]    # avoid dup E_start
    final_raw += conn2[1:]       # avoid dup E_start
    final_raw += snake_rem[1:]   # avoid dup p_join

    final_pts = resample_polyline_uniform(final_raw, float(sp.path_step_m))

    dbg.keypts = {"P_pre": P_pre, "E_start": E_start, "e_pre": e_pre, "p_join": p_join}
    dbg.conn1 = conn1
    dbg.conn2 = conn2
    dbg.edge_lap = edge_lap
    return final_pts, dbg

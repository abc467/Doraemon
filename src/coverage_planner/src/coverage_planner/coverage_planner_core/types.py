# coverage_planner_core/types.py
# -*- coding: utf-8 -*-

from dataclasses import dataclass
from typing import List, Tuple, Optional, Dict, Any

XY = Tuple[float, float]
XYYAW = Tuple[float, float, float]


@dataclass
class RobotSpec:
    cov_width: float
    width: float
    min_turning_radius: float
    max_diff_curv: float = 0.2


@dataclass
class PlannerParams:
    split_angle_deg: float = 0.0
    turn_model: str = "dubins"

    viz_step_m: float = 0.05
    path_step_m: float = 0.05
    turn_step_m: float = 0.05
    line_w: float = 0.05
    mute_stderr: bool = False
    validate_effective_region_path: bool = True

    wall_margin_m: float = 0.05
    turn_margin_m: float = 0.0
    min_plannable_span_m: float = 0.0

    edge_corner_radius_m: float = -1.0
    edge_corner_pull: float = 1.35
    edge_corner_min_pts: int = 36

    pre_proj_min: float = 0.5
    pre_proj_max: float = 0.6
    pre_prefix_max: float = 1.0
    e_pre_min: float = 0.4
    e_pre_max: float = 0.5


@dataclass
class BlockDebug:
    swath_segs_xyz: Optional[List[Tuple[Tuple[float,float,float], Tuple[float,float,float]]]] = None
    snake_pts_xy: Optional[List[XY]] = None
    edge_loop_xy: Optional[List[XY]] = None
    conn1_xy: Optional[List[XY]] = None
    conn2_xy: Optional[List[XY]] = None
    keypts: Optional[Dict[str, XY]] = None
    entry_xy: Optional[XY] = None
    exit_xy: Optional[XY] = None


@dataclass
class BlockPlan:
    block_id: int
    path_xy: List[XY]
    entry_xyyaw: XYYAW
    exit_xyyaw: XYYAW
    stats: Dict[str, Any]
    debug: Optional[BlockDebug] = None


@dataclass
class PlanResult:
    ok: bool
    error_code: str = ""
    error_message: str = ""
    frame_id: str = "map"

    blocks: List[BlockPlan] = None
    exec_order: List[int] = None
    total_length_m: float = 0.0

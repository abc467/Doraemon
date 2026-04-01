#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from coverage_planner.coverage_planner_core.types import RobotSpec, PlannerParams
from coverage_planner.coverage_planner_core.geom_norm import normalize_polygon
from coverage_planner.coverage_planner_core.planner import plan_coverage


def main():
    # outer = [(0.775, 1.025), (49.275, 1.025), (49.275, 68.775), (0.775, 68.775)]
    # holes = [
    #     [(2.325, 62.175), (2.025, 5.025), (11.025, 4.975), (11.375, 62.075),
    #      (5.225, 62.175), (5.375, 62.075), (5.175, 62.025), (5.125, 62.175)],
    #     [(37.375, 62.525), (37.125, 4.475), (46.125, 4.475), (46.375, 62.275), (46.275, 62.475)],
    #     [(15.375, 62.625), (15.025, 4.525), (33.125, 4.475), (33.325, 62.525)]
    # ]
        # 50m x 70m rectangle (UN-CLOSED ring here; normalize_polygon will close it)
    # outer = [(0.0, 0.0), (50.0, 0.0), (50.0, 70.0), (0.0, 70.0)]

    # # 3 rectangular holes (also UN-CLOSED here)
    # holes = [
    #     # Obstacle 1: x 2.5..12.5, y 5..65 (10m x 60m)
    #     [(2.5, 5.0), (12.5, 5.0), (12.5, 65.0), (2.5, 65.0)],

    #     # Obstacle 2: x 15..35, y 5..65 (20m x 60m)
    #     [(15.0, 5.0), (35.0, 5.0), (35.0, 65.0), (15.0, 65.0)],

    #     # Obstacle 3: x 37.5..47.5, y 5..65 (10m x 60m)
    #     [(37.5, 5.0), (47.5, 5.0), (47.5, 65.0), (37.5, 65.0)],
    # ]
    outer = [
    (20.30, 6.45),
    (34.00, 6.45),
    (34.00, 66.80),
    (20.30, 66.80),
]

    holes = [
    [
        (20.45, 60.50),
        (29.85, 60.50),
        (29.85, 6.45),
        (20.45, 6.45),
    ]
    ]
  


    outer_n, holes_n = normalize_polygon(outer, holes, prec=2)

    robot = RobotSpec(cov_width=0.5, width=0.5, min_turning_radius=0.3, max_diff_curv=0.2)

    p = PlannerParams(
        split_angle_deg=0.0,
        turn_model="dubins",
        viz_step_m=0.05,
        path_step_m=0.05,
        turn_step_m=0.05,
        line_w=0.05,
        mute_stderr=True,

        wall_margin_m=0.3,
        turn_margin_m=0.80,

        edge_corner_radius_m=0.5,
        edge_corner_pull=0.10,
        edge_corner_min_pts=36,

        pre_proj_min=0.5,
        pre_proj_max=0.6,
        pre_prefix_max=1.0,
        e_pre_min=0.4,
        e_pre_max=0.5,
    )

    res = plan_coverage(
        frame_id="map",
        outer=outer_n[:-1],
        holes=[h[:-1] for h in holes_n],
        effective_regions=None,
        robot_spec=robot,
        params=p,
        debug=True
    )

    print("ok =", res.ok)
    print("blocks =", len(res.blocks))
    print("exec_order =", res.exec_order)
    print("total_length_m =", res.total_length_m)

    # optional: print per block
    for b in res.blocks:
        print(f"  block {b.block_id}: pts={len(b.path_xy)} entry={b.entry_xyyaw} exit={b.exit_xyyaw}")


if __name__ == "__main__":
    main()

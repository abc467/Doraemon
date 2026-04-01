#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import rospy
import actionlib
from geometry_msgs.msg import Point32
from coverage_msgs.msg import PlanCoverageAction, PlanCoverageGoal, ZoneGeometry, Polygon2D

def mk_poly(points):
    p = Polygon2D()
    for x, y in points:
        pt = Point32()
        pt.x = float(x); pt.y = float(y); pt.z = 0.0
        p.points.append(pt)
    return p

def main():
    rospy.init_node("send_plan_goal")

    # 50x70 + 3 holes（不闭环传入，server normalize 会闭环）
    # outer = [(0.0, 0.0), (50.0, 0.0), (50.0, 70.0), (0.0, 70.0)]
    # holes = [
    #     [(2.5, 5.0), (12.5, 5.0), (12.5, 65.0), (2.5, 65.0)],
    #     [(15.0, 5.0), (35.0, 5.0), (35.0, 65.0), (15.0, 65.0)],
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

    g = PlanCoverageGoal()
    g.zone_id = "zone_demo"
    g.zone_version = 1
    g.profile_name = "cover_standard"
    g.debug_publish_markers = True
    g.export_input = False

    geom = ZoneGeometry()
    geom.frame_id = "map"
    geom.outer = mk_poly(outer)
    geom.holes = [mk_poly(h) for h in holes]
    g.geom = geom
    g.polygon_json = ""

    # Planner tuning now has a normalized server-side config entry:
    #   ~robot/*
    #   ~planner/*
    # Leave the goal fields at their message defaults unless you really need
    # a per-request override.
    #
    # Example overrides:
    # g.path_step_m = 0.05
    # g.turn_step_m = 0.05
    # g.wall_margin_m = 0.30
    # g.turn_margin_m = 0.80

    client = actionlib.SimpleActionClient("/coverage_planner_server/plan_coverage", PlanCoverageAction)
    rospy.loginfo("waiting for /coverage_planner_server/plan_coverage ...")
    client.wait_for_server()

    rospy.loginfo("send goal")
    client.send_goal(g)
    client.wait_for_result()

    res = client.get_result()
    print("RESULT:", res.ok, res.plan_id, res.blocks, res.total_path_length_m, res.error_code, res.error_message)

if __name__ == "__main__":
    main()

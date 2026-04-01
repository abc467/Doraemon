#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import math
import sqlite3
import struct
import time

import rospy
import actionlib

from geometry_msgs.msg import PoseStamped, Quaternion
from nav_msgs.msg import Path

# TF (optional but recommended)
try:
    import tf2_ros
except Exception:
    tf2_ros = None

# MBF actions
from mbf_msgs.msg import MoveBaseAction, MoveBaseGoal
from mbf_msgs.msg import ExePathAction, ExePathGoal

# -------------------------
# geometry helpers
# -------------------------
def yaw_to_quat(yaw: float) -> Quaternion:
    q = Quaternion()
    half = 0.5 * float(yaw)
    q.z = math.sin(half)
    q.w = math.cos(half)
    q.x = 0.0
    q.y = 0.0
    return q

def mk_pose(frame_id: str, x: float, y: float, yaw: float) -> PoseStamped:
    ps = PoseStamped()
    ps.header.frame_id = frame_id
    ps.header.stamp = rospy.Time.now()
    ps.pose.position.x = float(x)
    ps.pose.position.y = float(y)
    ps.pose.position.z = 0.0
    ps.pose.orientation = yaw_to_quat(yaw)
    return ps

def mk_path(frame_id: str, pts_xyz):
    """
    pts_xyz: [(x,y,yaw), ...]
    """
    msg = Path()
    msg.header.frame_id = frame_id
    msg.header.stamp = rospy.Time.now()
    for (x, y, yaw) in pts_xyz:
        ps = mk_pose(frame_id, x, y, yaw)
        msg.poses.append(ps)
    return msg

# -------------------------
# sqlite plan loader (compatible with your current blob layout)
# -------------------------
def db_connect(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

def get_active_plan_id(conn, zone_id: str):
    row = conn.execute(
        "select active_plan_id from zones where zone_id=? order by updated_ts desc limit 1;",
        (zone_id,)
    ).fetchone()
    if not row:
        return None
    return row["active_plan_id"]

def get_latest_plan_id(conn, zone_id: str = None):
    if zone_id:
        row = conn.execute(
            "select plan_id from plans where zone_id=? order by created_ts desc limit 1;",
            (zone_id,)
        ).fetchone()
    else:
        row = conn.execute(
            "select plan_id from plans order by created_ts desc limit 1;"
        ).fetchone()
    return row["plan_id"] if row else None

def load_block_blob(conn, plan_id: str, block_id: int):
    row = conn.execute(
        "select plan_id, block_id, point_count, length_m, entry_x, entry_y, entry_yaw, exit_x, exit_y, exit_yaw, path_blob "
        "from plan_blocks where plan_id=? and block_id=? limit 1;",
        (plan_id, int(block_id))
    ).fetchone()
    if not row:
        return None
    return row

def decode_path_blob(path_blob: bytes):
    """
    Your blob is float32 triplets (x,y,yaw) in little-endian.
    """
    if path_blob is None:
        return []
    if isinstance(path_blob, memoryview):
        path_blob = path_blob.tobytes()
    n_floats = len(path_blob) // 4
    if n_floats % 3 != 0:
        raise RuntimeError(f"blob size not multiple of 3 floats: bytes={len(path_blob)}")
    floats = struct.unpack("<" + "f" * n_floats, path_blob)
    pts = []
    for i in range(0, n_floats, 3):
        x = float(floats[i + 0])
        y = float(floats[i + 1])
        yaw = float(floats[i + 2])
        pts.append((x, y, yaw))
    return pts

# -------------------------
# runtime checks
# -------------------------
def wait_for_tf(frame_a: str, frame_b: str, timeout_s: float = 3.0) -> bool:
    if tf2_ros is None:
        rospy.logwarn("tf2_ros not available in python env; skip TF check.")
        return True
    buf = tf2_ros.Buffer(cache_time=rospy.Duration(10.0))
    lis = tf2_ros.TransformListener(buf)
    t0 = time.time()
    while not rospy.is_shutdown() and (time.time() - t0) < timeout_s:
        try:
            buf.lookup_transform(frame_a, frame_b, rospy.Time(0), rospy.Duration(0.5))
            return True
        except Exception:
            rospy.sleep(0.2)
    return False

def wait_for_action(name: str, client, timeout_s: float = 5.0) -> bool:
    rospy.loginfo(f"waiting for action server: {name} ...")
    return client.wait_for_server(rospy.Duration(timeout_s))

# -------------------------
# MBF calls
# -------------------------
def mbf_move_to(move_client, target_pose: PoseStamped, planner: str = "", controller: str = "", timeout_s: float = 120.0):
    goal = MoveBaseGoal()
    goal.target_pose = target_pose
    # Optional fields exist in mbf_msgs; safe to set if present
    if hasattr(goal, "planner"):
        goal.planner = planner or ""
    if hasattr(goal, "controller"):
        goal.controller = controller or ""
    move_client.send_goal(goal)
    ok = move_client.wait_for_result(rospy.Duration(timeout_s))
    if not ok:
        move_client.cancel_goal()
        return False, "timeout"
    res = move_client.get_result()
    # mbf_msgs/MoveBaseResult usually has outcome/message
    outcome = getattr(res, "outcome", None)
    msg = getattr(res, "message", "")
    return True, f"outcome={outcome} msg={msg}"

def mbf_exe_path(exe_client, path_msg: Path, controller: str, tolerance_from_action: float = 0.0, timeout_s: float = 600.0):
    goal = ExePathGoal()
    goal.path = path_msg
    # controller name is required for exe_path
    if hasattr(goal, "controller"):
        goal.controller = controller
    # some MBF versions have tolerance
    if hasattr(goal, "tolerance"):
        goal.tolerance = float(tolerance_from_action)

    def fb_cb(fb):
        # Different MBF versions expose different feedback fields; print minimally
        # Try common ones: current_pose, dist_to_goal, current_wp
        parts = []
        if hasattr(fb, "dist_to_goal"):
            parts.append(f"dist_to_goal={getattr(fb,'dist_to_goal'):.3f}")
        if hasattr(fb, "current_wp"):
            parts.append(f"wp={getattr(fb,'current_wp')}")
        if parts:
            rospy.loginfo_throttle(1.0, "[exe_path fb] " + " ".join(parts))

    exe_client.send_goal(goal, feedback_cb=fb_cb)
    ok = exe_client.wait_for_result(rospy.Duration(timeout_s))
    if not ok:
        exe_client.cancel_goal()
        return False, "timeout"

    res = exe_client.get_result()
    outcome = getattr(res, "outcome", None)
    msg = getattr(res, "message", "")
    return True, f"outcome={outcome} msg={msg}"

# -------------------------
# main
# -------------------------
def main():
    ap = argparse.ArgumentParser(description="Minimal single-block executor using MBF + sqlite plan store.")
    ap.add_argument("--db", default="/home/y/.ros/coverage/coverage.db", help="sqlite db path")
    ap.add_argument("--zone_id", default="zone_demo", help="zone_id to use")
    ap.add_argument("--plan_id", default="", help="if set, use this plan_id directly (skip zones.active_plan_id)")
    ap.add_argument("--block_id", type=int, default=0, help="block id to execute")
    ap.add_argument("--frame_id", default="map", help="frame id used by plan")
    ap.add_argument("--use_active", action="store_true", help="use zones.active_plan_id first (fallback latest)")
    ap.add_argument("--connect_to_entry", action="store_true", help="call MBF move_base to entry pose before exe_path")
    ap.add_argument("--move_timeout_s", type=float, default=180.0)
    ap.add_argument("--exe_timeout_s", type=float, default=1200.0)
    ap.add_argument("--mbf_move_ns", default="/move_base_flex/move_base", help="MBF move_base action name")
    ap.add_argument("--mbf_exe_ns", default="/move_base_flex/exe_path", help="MBF exe_path action name")
    ap.add_argument("--controller", default="cover_standard", help="MBF local controller name for exe_path")
    ap.add_argument("--planner", default="", help="MBF global planner name for move_base (optional)")
    ap.add_argument("--tf_check", action="store_true", help="check TF frame_id->base_link exists before running")
    ap.add_argument("--publish_path_topic", default="/coverage_executor/debug/path", help="publish loaded path for RViz")
    ap.add_argument("--downsample", type=int, default=1, help="take every N-th point for exe_path (>=1)")

    args = ap.parse_args()

    rospy.init_node("coverage_single_block_runner", anonymous=True)

    # TF check
    if args.tf_check:
        ok_tf = wait_for_tf(args.frame_id, "base_link", timeout_s=5.0)
        if not ok_tf:
            rospy.logerr(f"TF not ready: {args.frame_id} -> base_link not available")
            return
        rospy.loginfo(f"TF OK: {args.frame_id} -> base_link")

    # Connect DB
    conn = db_connect(args.db)

    plan_id = args.plan_id.strip() if args.plan_id else ""
    if not plan_id:
        if args.use_active:
            plan_id = get_active_plan_id(conn, args.zone_id) or ""
        if not plan_id:
            plan_id = get_latest_plan_id(conn, zone_id=args.zone_id) or ""
    if not plan_id:
        rospy.logerr(f"Cannot find plan_id (zone_id={args.zone_id}).")
        return

    row = load_block_blob(conn, plan_id, args.block_id)
    if not row:
        rospy.logerr(f"Cannot find block: plan_id={plan_id} block_id={args.block_id}")
        return

    pts = decode_path_blob(row["path_blob"])
    if not pts:
        rospy.logerr("decoded path is empty")
        return

    if args.downsample < 1:
        args.downsample = 1
    if args.downsample > 1:
        pts = pts[::args.downsample]
        rospy.logwarn(f"downsample={args.downsample}, exe pts={len(pts)}")

    entry = (row["entry_x"], row["entry_y"], row["entry_yaw"])
    exitp = (row["exit_x"], row["exit_y"], row["exit_yaw"])

    rospy.loginfo("====================================")
    rospy.loginfo(f"PLAN: {plan_id}")
    rospy.loginfo(f"BLOCK: {args.block_id} point_count={row['point_count']} length_m={row['length_m']:.3f}")
    rospy.loginfo(f"ENTRY: ({entry[0]:.3f},{entry[1]:.3f},{entry[2]:.3f})")
    rospy.loginfo(f"EXIT : ({exitp[0]:.3f},{exitp[1]:.3f},{exitp[2]:.3f})")
    rospy.loginfo("====================================")

    # Publish path for RViz
    pub = rospy.Publisher(args.publish_path_topic, Path, queue_size=1, latch=True)
    path_msg = mk_path(args.frame_id, pts)
    pub.publish(path_msg)
    rospy.loginfo(f"published debug path: {args.publish_path_topic}")

    # MBF clients
    move_client = actionlib.SimpleActionClient(args.mbf_move_ns, MoveBaseAction)
    exe_client  = actionlib.SimpleActionClient(args.mbf_exe_ns, ExePathAction)

    if not wait_for_action(args.mbf_move_ns, move_client, timeout_s=5.0):
        rospy.logerr(f"MBF move_base action not available: {args.mbf_move_ns}")
        return
    if not wait_for_action(args.mbf_exe_ns, exe_client, timeout_s=5.0):
        rospy.logerr(f"MBF exe_path action not available: {args.mbf_exe_ns}")
        return

    # Optional connect to entry
    if args.connect_to_entry:
        target = mk_pose(args.frame_id, float(entry[0]), float(entry[1]), float(entry[2]))
        rospy.loginfo("move_base -> entry ...")
        ok, info = mbf_move_to(move_client, target, planner=args.planner, controller="", timeout_s=args.move_timeout_s)
        rospy.loginfo(f"move_base result: ok={ok} {info}")
        if not ok:
            return

    # Execute path
    rospy.loginfo(f"exe_path -> controller='{args.controller}' ...")
    ok, info = mbf_exe_path(exe_client, path_msg, controller=args.controller, timeout_s=args.exe_timeout_s)
    rospy.loginfo(f"exe_path result: ok={ok} {info}")

if __name__ == "__main__":
    main()

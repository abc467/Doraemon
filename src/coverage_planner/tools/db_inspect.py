#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
from coverage_planner.plan_store.store import PlanStore

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--zone_id", default=None)
    ap.add_argument("--plan_id", default=None)
    ap.add_argument("--block_id", type=int, default=0)
    args = ap.parse_args()

    st = PlanStore(args.db)

    # Choose plan_id
    plan_id = args.plan_id
    if plan_id is None:
        if args.zone_id:
            plan_id = st.get_active_plan_id(args.zone_id) or st.get_latest_plan_id(args.zone_id)
        else:
            # fallback: newest plan overall
            import sqlite3
            cur = st.conn.cursor()
            row = cur.execute("SELECT plan_id FROM plans ORDER BY created_ts DESC LIMIT 1;").fetchone()
            plan_id = row["plan_id"] if row else None

    if not plan_id:
        print("No plan found.")
        return

    meta = st.load_plan_meta(plan_id)
    print("PLAN META:")
    print(" plan_id =", meta["plan_id"])
    print(" zone_id =", meta["zone_id"], "zone_version =", meta["zone_version"])
    print(" blocks =", meta["blocks"], "total_length_m =", meta["total_length_m"])
    print(" exec_order =", meta.get("exec_order_json"))

    blk = st.load_block(plan_id, args.block_id)
    pts = blk["path_xyyaw"]
    print("\nBLOCK:")
    print(" block_id =", blk["block_id"])
    print(" point_count =", blk["point_count"], "length_m =", blk["length_m"])
    print(" entry =", (blk["entry_x"], blk["entry_y"], blk["entry_yaw"]))
    print(" exit  =", (blk["exit_x"], blk["exit_y"], blk["exit_yaw"]))
    print(" first5 =", pts[:5])
    print(" last5  =", pts[-5:])

if __name__ == "__main__":
    main()

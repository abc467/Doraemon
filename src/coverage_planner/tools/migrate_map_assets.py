#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import glob
import os
import shutil
import time

from coverage_planner.map_io import (
    compute_occupancy_grid_md5,
    origin_to_jsonable,
    write_occupancy_to_yaml_pgm,
    yaml_pgm_to_occupancy,
)
from coverage_planner.ops_store.store import OperationsStore
from coverage_planner.plan_store.store import PlanStore


def _utc_version() -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())


def _find_existing_asset(store: PlanStore, map_name: str, map_md5: str):
    for asset in store.list_map_asset_versions(map_name=map_name):
        if str(asset.get("map_md5") or "").strip() == str(map_md5 or "").strip():
            return asset
    return None


def main():
    ap = argparse.ArgumentParser(description="Initialize DB schemas and migrate legacy static maps into managed map assets")
    ap.add_argument("--plan-db-path", default="/data/coverage/planning.db")
    ap.add_argument("--ops-db-path", default="/data/coverage/operations.db")
    ap.add_argument("--maps-root", default="/data/maps")
    ap.add_argument("--src-glob", default="")
    ap.add_argument("--robot-id", default="local_robot")
    ap.add_argument("--set-active", action="store_true")
    ap.add_argument("--set-active-if-missing", action="store_true")
    args = ap.parse_args()

    if not str(args.src_glob or "").strip():
        raise SystemExit("--src-glob is required for explicit legacy map migration")

    os.makedirs(os.path.expanduser(args.maps_root), exist_ok=True)
    store = PlanStore(args.plan_db_path)
    _ = OperationsStore(args.ops_db_path)

    imported = 0
    skipped = 0
    backfilled = 0
    last_asset = None
    seen_map_names = set()

    yaml_files = sorted(glob.glob(os.path.expanduser(args.src_glob)))
    for yaml_path in yaml_files:
        map_name = os.path.splitext(os.path.basename(yaml_path))[0]
        seen_map_names.add(map_name)
        occ = yaml_pgm_to_occupancy(yaml_path)
        map_md5 = compute_occupancy_grid_md5(occ)
        map_id = "map_%s" % str(map_md5)[:8]

        asset = _find_existing_asset(store, map_name=map_name, map_md5=map_md5)
        if asset is None:
            map_version = _utc_version()
            out_dir = os.path.join(os.path.expanduser(args.maps_root), map_name, map_version)
            os.makedirs(out_dir, exist_ok=True)
            pgm_path, new_yaml_path = write_occupancy_to_yaml_pgm(occ, out_dir, base_name="map")

            pbstream_src = os.path.splitext(yaml_path)[0] + ".pbstream"
            pbstream_dst = ""
            if os.path.exists(pbstream_src):
                pbstream_dst = os.path.join(out_dir, "map.pbstream")
                shutil.copy2(pbstream_src, pbstream_dst)

            store.register_map_asset_version(
                map_name=map_name,
                map_version=map_version,
                map_id=map_id,
                map_md5=map_md5,
                yaml_path=new_yaml_path,
                pgm_path=pgm_path,
                pbstream_path=pbstream_dst,
                frame_id=str(occ.header.frame_id or "map"),
                resolution=float(occ.info.resolution or 0.0),
                origin=origin_to_jsonable(occ),
                display_name=map_name,
                robot_id=args.robot_id,
                set_active=False,
            )
            asset = store.resolve_map_asset_version(
                map_name=map_name,
                map_version=map_version,
                robot_id=args.robot_id,
            )
            imported += 1
            print("imported", map_name, map_version, map_id)
        else:
            skipped += 1
            print("reused", asset.get("map_name"), asset.get("map_version"), asset.get("map_id"))

        if asset:
            store.backfill_legacy_map_scope(
                map_name=str(asset.get("map_name") or ""),
                map_version=str(asset.get("map_version") or ""),
                map_id=str(asset.get("map_id") or map_id),
                map_md5=str(asset.get("map_md5") or map_md5),
            )
            backfilled += 1
            last_asset = asset

    should_set_active = bool(args.set_active)
    if args.set_active_if_missing and not should_set_active:
        active = store.get_active_map(robot_id=args.robot_id)
        should_set_active = bool(last_asset) and (active is None) and (len(seen_map_names) == 1)

    if should_set_active and last_asset:
        store.set_active_map(
            map_name=str(last_asset.get("map_name") or ""),
            map_version=str(last_asset.get("map_version") or ""),
            robot_id=args.robot_id,
        )
        print("active", last_asset.get("map_name"), last_asset.get("map_version"))

    print(
        "summary imported=%d reused=%d backfilled=%d maps_root=%s yaml_files=%d"
        % (imported, skipped, backfilled, os.path.expanduser(args.maps_root), len(yaml_files))
    )


if __name__ == "__main__":
    main()

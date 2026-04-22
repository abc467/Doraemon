#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import glob
import os
import shutil

from coverage_planner.map_io import (
    compute_occupancy_grid_md5,
    write_occupancy_to_yaml_pgm,
    yaml_pgm_to_occupancy,
)
from coverage_planner.map_asset_import import map_id_from_md5, register_imported_map_asset
from coverage_planner.map_asset_import import normalize_import_verification_mode
from coverage_planner.ops_store.store import OperationsStore
from coverage_planner.plan_store.store import PlanStore
def _find_existing_asset(store: PlanStore, map_name: str, map_md5: str):
    for asset in store.list_map_assets_by_name(map_name=map_name):
        if str(asset.get("map_md5") or "").strip() == str(map_md5 or "").strip():
            return asset
    return None


def main():
    ap = argparse.ArgumentParser(description="Initialize DB schemas and import unmanaged static maps into managed map assets")
    ap.add_argument("--plan-db-path", default="/data/coverage/planning.db")
    ap.add_argument("--ops-db-path", default="/data/coverage/operations.db")
    ap.add_argument("--maps-root", default="/data/maps")
    ap.add_argument("--src-glob", default="")
    ap.add_argument("--robot-id", default="local_robot")
    ap.add_argument(
        "--verification-mode",
        default="offline_verified",
        help="candidate or offline_verified",
    )
    ap.add_argument("--set-active", action="store_true")
    ap.add_argument("--set-active-if-missing", action="store_true")
    args = ap.parse_args()

    if not str(args.src_glob or "").strip():
        raise SystemExit("--src-glob is required for explicit offline map import")
    verification_mode = normalize_import_verification_mode(args.verification_mode)
    if bool(args.set_active or args.set_active_if_missing) and verification_mode != "offline_verified":
        raise SystemExit("--set-active and --set-active-if-missing require --verification-mode=offline_verified")

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
        map_md5 = ""
        map_id = ""
        if verification_mode == "offline_verified":
            map_md5 = str(compute_occupancy_grid_md5(occ) or "").strip()
            if not map_md5:
                raise RuntimeError("failed to compute map_md5 for %s" % yaml_path)
            map_id = map_id_from_md5(map_md5)

        asset = _find_existing_asset(store, map_name=map_name, map_md5=map_md5) if map_md5 else None
        if asset is None:
            revision_id = store.generate_map_revision_id(map_name)
            out_root = os.path.join(
                os.path.expanduser(args.maps_root),
                "revisions",
                map_name,
                revision_id,
            )
            os.makedirs(out_root, exist_ok=True)
            pgm_path, new_yaml_path = write_occupancy_to_yaml_pgm(occ, out_root, base_name=map_name)

            pbstream_src = os.path.splitext(yaml_path)[0] + ".pbstream"
            pbstream_dst = ""
            if os.path.exists(pbstream_src):
                pbstream_dst = os.path.join(out_root, map_name + ".pbstream")
                shutil.copy2(pbstream_src, pbstream_dst)

            asset, _snapshot_md5 = register_imported_map_asset(
                store,
                map_name=map_name,
                occ=occ,
                yaml_path=new_yaml_path,
                pgm_path=pgm_path,
                pbstream_path=pbstream_dst,
                display_name=map_name,
                description="",
                enabled=True,
                robot_id=args.robot_id,
                verification_mode=verification_mode,
                set_active=False,
                revision_id=revision_id,
            )
            imported += 1
            print("imported", map_name, str(asset.get("revision_id") or ""), str(asset.get("verification_status") or ""))
        else:
            skipped += 1
            print("reused", asset.get("map_name"), asset.get("map_id"))

        if asset:
            if str(asset.get("map_id") or map_id).strip() or str(asset.get("map_md5") or map_md5).strip():
                store.backfill_unscoped_map_scope(
                    map_name=str(asset.get("map_name") or ""),
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
        active_revision_id = str(last_asset.get("revision_id") or "").strip()
        if active_revision_id:
            store.set_active_map_revision(
                revision_id=active_revision_id,
                robot_id=args.robot_id,
            )
        else:
            store.set_active_map(
                map_name=str(last_asset.get("map_name") or ""),
                robot_id=args.robot_id,
            )
        print("active", last_asset.get("map_name"), active_revision_id)

    print(
        "summary imported=%d reused=%d backfilled=%d maps_root=%s yaml_files=%d"
        % (imported, skipped, backfilled, os.path.expanduser(args.maps_root), len(yaml_files))
    )


if __name__ == "__main__":
    main()

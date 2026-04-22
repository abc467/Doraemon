# -*- coding: utf-8 -*-

"""Shared helpers for importing offline map artifacts into the revision store."""

from __future__ import annotations

from typing import Any, Dict, Tuple

from coverage_planner.map_io import compute_occupancy_grid_md5, origin_to_jsonable


def normalize_import_verification_mode(verification_mode: str) -> str:
    normalized = str(verification_mode or "candidate").strip().lower() or "candidate"
    if normalized not in ("candidate", "offline_verified"):
        raise ValueError("unsupported verification_mode: %s" % str(verification_mode or ""))
    return normalized


def map_id_from_md5(map_md5: str) -> str:
    normalized = str(map_md5 or "").strip()
    if not normalized:
        return ""
    return "map_%s" % normalized[:8]


def register_imported_map_asset(
    store,
    *,
    map_name: str,
    occ,
    yaml_path: str,
    pgm_path: str,
    pbstream_path: str = "",
    display_name: str = "",
    description: str = "",
    enabled: bool = True,
    robot_id: str = "local_robot",
    verification_mode: str = "candidate",
    set_active: bool = False,
    revision_id: str = "",
) -> Tuple[Dict[str, Any], str]:
    normalized_mode = normalize_import_verification_mode(verification_mode)
    if bool(set_active) and normalized_mode != "offline_verified":
        raise ValueError("set_active requires verification_mode=offline_verified")

    snapshot_md5 = str(compute_occupancy_grid_md5(occ) or "").strip()
    if not snapshot_md5:
        raise RuntimeError("failed to compute imported map md5")
    imported_map_id = map_id_from_md5(snapshot_md5)

    canonical_map_id = imported_map_id if normalized_mode == "offline_verified" else ""
    canonical_map_md5 = snapshot_md5 if normalized_mode == "offline_verified" else ""
    lifecycle_status = "available" if normalized_mode == "offline_verified" else "saved_unverified"
    verification_status = "verified" if normalized_mode == "offline_verified" else "pending"
    verified_runtime_map_id = imported_map_id if normalized_mode == "offline_verified" else ""
    verified_runtime_map_md5 = snapshot_md5 if normalized_mode == "offline_verified" else ""

    revision_id = str(
        store.register_map_asset(
            map_name=map_name,
            map_id=canonical_map_id,
            map_md5=canonical_map_md5,
            yaml_path=yaml_path,
            pgm_path=pgm_path,
            pbstream_path=pbstream_path,
            frame_id=str(getattr(getattr(occ, "header", None), "frame_id", "") or "map"),
            resolution=float(getattr(getattr(occ, "info", None), "resolution", 0.0) or 0.0),
            origin=origin_to_jsonable(occ),
            display_name=display_name,
            description=description,
            enabled=bool(enabled),
            lifecycle_status=lifecycle_status,
            verification_status=verification_status,
            save_snapshot_md5=snapshot_md5,
            verified_runtime_map_id=verified_runtime_map_id,
            verified_runtime_map_md5=verified_runtime_map_md5,
            last_error_code="",
            last_error_msg="",
            robot_id=robot_id,
            set_active=bool(set_active),
            revision_id=str(revision_id or "").strip() or None,
        )
        or ""
    ).strip()
    asset = (
        store.resolve_map_asset(revision_id=revision_id, robot_id=robot_id)
        if revision_id
        else store.resolve_map_asset(map_name=map_name, robot_id=robot_id)
    ) or {}
    return dict(asset), snapshot_md5

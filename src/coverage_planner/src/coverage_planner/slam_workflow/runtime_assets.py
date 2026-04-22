# -*- coding: utf-8 -*-

"""Map asset and runtime file helpers for the formal SLAM backend."""

from __future__ import annotations

import os
import shutil
from typing import Any, Dict, Optional


def _normalize_map_name(map_name: str) -> str:
    value = str(map_name or "").strip()
    if value.endswith(".pbstream"):
        value = value[:-len(".pbstream")]
    return str(value or "").strip()


class CartographerRuntimeAssetHelper:
    def __init__(self, backend: Any):
        self._backend = backend

    def resolve_asset(
        self,
        *,
        robot_id: str,
        map_name: str,
        map_revision_id: str = "",
        prefer_active_revision: bool = False,
    ) -> Optional[Dict[str, object]]:
        backend = self._backend
        normalized_map_name = _normalize_map_name(map_name)
        normalized_revision_id = str(map_revision_id or "").strip()
        asset = None
        if normalized_revision_id:
            asset = backend._plan_store.resolve_map_asset(
                revision_id=normalized_revision_id,
                robot_id=robot_id,
            )
            resolved_map_name = _normalize_map_name(str((asset or {}).get("map_name") or ""))
            if normalized_map_name and resolved_map_name and resolved_map_name != normalized_map_name:
                raise ValueError("map revision does not match selected map")
            return asset
        if normalized_map_name:
            resolve_revision = getattr(backend._plan_store, "resolve_map_revision", None)
            if callable(resolve_revision):
                resolved_revision = resolve_revision(
                    map_name=normalized_map_name,
                    robot_id=robot_id,
                )
                if resolved_revision is not None:
                    return resolved_revision
            if bool(prefer_active_revision):
                active_asset = backend._plan_store.get_active_map(robot_id=robot_id) or {}
                active_map_name = _normalize_map_name(str(active_asset.get("map_name") or ""))
                active_revision_id = str(active_asset.get("revision_id") or "").strip()
                if active_revision_id and active_map_name and active_map_name == normalized_map_name:
                    active_revision = backend._plan_store.resolve_map_asset(
                        revision_id=active_revision_id,
                        robot_id=robot_id,
                    )
                    if active_revision is not None:
                        return active_revision
                    return active_asset
            return backend._plan_store.resolve_map_asset(
                map_name=normalized_map_name,
                robot_id=robot_id,
            )
        return backend._plan_store.get_active_map(robot_id=robot_id)

    def ensure_repo_map_link(self, asset: Dict[str, object]) -> str:
        backend = self._backend
        map_name = str((asset or {}).get("map_name") or "").strip()
        pbstream_path = os.path.expanduser(str((asset or {}).get("pbstream_path") or "").strip())
        if not map_name:
            raise RuntimeError("map asset missing map_name")
        if not pbstream_path:
            raise RuntimeError("map asset missing pbstream_path")
        if not os.path.isfile(pbstream_path):
            raise RuntimeError("pbstream not found: %s" % pbstream_path)

        target_path = os.path.join(backend.repo_map_root, map_name + ".pbstream")
        src_real = os.path.realpath(pbstream_path)
        if os.path.exists(target_path):
            try:
                if os.path.realpath(target_path) == src_real:
                    return os.path.basename(target_path)
            except Exception:
                pass
            os.unlink(target_path)
        try:
            os.symlink(src_real, target_path)
        except Exception:
            shutil.copy2(src_real, target_path)
        return os.path.basename(target_path)

    def cleanup_paths(self, *paths: str):
        for path in paths:
            pp = str(path or "").strip()
            if not pp or not os.path.exists(pp):
                continue
            try:
                os.remove(pp)
            except Exception:
                pass

    def target_paths(self, map_name: str, revision_id: str = "") -> Dict[str, str]:
        backend = self._backend
        normalized_map_name = _normalize_map_name(map_name) or "map"
        normalized_revision_id = str(revision_id or "").strip()
        base_dir = backend.maps_root
        if normalized_revision_id:
            base_dir = os.path.join(
                backend.maps_root,
                "revisions",
                normalized_map_name,
                normalized_revision_id,
            )
        base = os.path.join(base_dir, normalized_map_name)
        return {
            "pbstream_path": base + ".pbstream",
            "yaml_path": base + ".yaml",
            "pgm_path": base + ".pgm",
        }

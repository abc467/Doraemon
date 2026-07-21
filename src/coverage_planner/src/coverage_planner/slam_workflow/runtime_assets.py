# -*- coding: utf-8 -*-

"""Map asset and runtime file helpers for the formal SLAM backend."""

from __future__ import annotations

import os
import stat
from typing import Any, Dict, Optional

from coverage_planner.map_path_security import (
    MapPathSecurityError,
    canonical_directory_root,
    map_asset_target_paths,
    validate_existing_regular_file,
    validate_map_name,
    validate_revision_id,
)


def _normalize_map_name(map_name: str) -> str:
    return validate_map_name(map_name, allow_empty=True)


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
        normalized_revision_id = validate_revision_id(map_revision_id, allow_empty=True)
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
        map_name = validate_map_name(str((asset or {}).get("map_name") or ""))
        pbstream_path = os.path.expanduser(str((asset or {}).get("pbstream_path") or "").strip())
        if not pbstream_path:
            raise RuntimeError("map asset missing pbstream_path")
        maps_root = canonical_directory_root(backend.maps_root, label="maps_root")
        repo_root = canonical_directory_root(backend.repo_map_root, label="repo_map_root")
        src_real = validate_existing_regular_file(
            maps_root,
            pbstream_path,
            suffix=".pbstream",
            label="map asset pbstream",
        )
        target_path = os.path.join(repo_root, map_name + ".pbstream")
        try:
            if os.path.commonpath((repo_root, os.path.abspath(target_path))) != repo_root:
                raise MapPathSecurityError("invalid_map_path", "repo map link escaped repo_map_root")
        except ValueError as exc:
            raise MapPathSecurityError("invalid_map_path", "repo map link escaped repo_map_root") from exc

        if os.path.abspath(target_path) == src_real:
            return os.path.basename(target_path)

        if os.path.lexists(target_path):
            target_info = os.lstat(target_path)
            if stat.S_ISLNK(target_info.st_mode):
                if not os.path.exists(target_path):
                    raise MapPathSecurityError(
                        "invalid_map_path",
                        "repo map link is dangling: %s" % target_path,
                    )
                resolved_target = os.path.realpath(target_path)
                validate_existing_regular_file(
                    maps_root,
                    resolved_target,
                    suffix=".pbstream",
                    label="repo map link target",
                )
                if resolved_target == src_real:
                    return os.path.basename(target_path)
                # A valid contained link is managed state and may be repointed
                # to the newly selected revision of the same map.
                os.unlink(target_path)
            elif stat.S_ISREG(target_info.st_mode):
                try:
                    if os.path.samefile(target_path, src_real):
                        return os.path.basename(target_path)
                except OSError:
                    pass
                raise MapPathSecurityError(
                    "invalid_map_path",
                    "repo map target is an unmanaged regular file: %s" % target_path,
                )
            else:
                raise MapPathSecurityError(
                    "invalid_map_path",
                    "repo map target is not a regular file or symlink: %s" % target_path,
                )

        relative_source = os.path.relpath(src_real, os.path.dirname(target_path))
        try:
            os.symlink(relative_source, target_path)
        except FileExistsError as exc:
            raise MapPathSecurityError(
                "map_path_exists",
                "repo map target appeared while creating link: %s" % target_path,
            ) from exc
        if not os.path.exists(target_path) or os.path.realpath(target_path) != src_real:
            try:
                os.unlink(target_path)
            except OSError:
                pass
            raise MapPathSecurityError("invalid_map_path", "failed to create contained repo map link")
        return os.path.basename(target_path)

    def cleanup_paths(self, *paths: str):
        for path in paths:
            pp = str(path or "").strip()
            if not pp or not os.path.lexists(pp):
                continue
            try:
                os.remove(pp)
            except Exception:
                pass

    def target_paths(self, map_name: str, revision_id: str = "") -> Dict[str, str]:
        backend = self._backend
        return map_asset_target_paths(backend.maps_root, map_name, revision_id)

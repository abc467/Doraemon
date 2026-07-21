# -*- coding: utf-8 -*-

"""Fail-closed path handling for commercial map assets.

Map identifiers are deliberately filesystem-safe ASCII identifiers.  Human
readable (including Chinese) names belong in the map display-name metadata,
not in a path component.
"""

from __future__ import annotations

import errno
import os
import re
import stat
from typing import Dict, Optional


COMMERCIAL_MAPS_ROOT = "/data/maps"
COMMERCIAL_EXTERNAL_MAPS_ROOT = "/data/maps/imports"
MAX_MAP_COMPONENT_LENGTH = 128
_MAP_COMPONENT_RE = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z", re.ASCII)


class MapPathSecurityError(ValueError):
    """A controlled, user-safe error raised for an unsafe map path."""

    def __init__(self, code: str, message: str):
        super().__init__(str(message or code or "unsafe map path"))
        self.code = str(code or "invalid_map_path")


def _raw_text(value) -> str:
    return str(value or "")


def _reject_control_characters(value: str, *, code: str, label: str) -> None:
    if any(ord(ch) < 32 or ord(ch) == 127 for ch in value):
        raise MapPathSecurityError(code, "%s contains a control character" % label)


def _validate_component(value, *, label: str, code: str, allow_empty: bool) -> str:
    raw = _raw_text(value)
    _reject_control_characters(raw, code=code, label=label)
    normalized = raw.strip()
    if not normalized:
        if allow_empty:
            return ""
        raise MapPathSecurityError(code, "%s is required" % label)
    if normalized in (".", ".."):
        raise MapPathSecurityError(code, "%s must be a single path component" % label)
    if "/" in normalized or "\\" in normalized or os.path.sep in normalized:
        raise MapPathSecurityError(code, "%s must not contain a path separator" % label)
    if os.path.altsep and os.path.altsep in normalized:
        raise MapPathSecurityError(code, "%s must not contain a path separator" % label)
    if len(normalized) > MAX_MAP_COMPONENT_LENGTH:
        raise MapPathSecurityError(
            code,
            "%s exceeds %d characters" % (label, MAX_MAP_COMPONENT_LENGTH),
        )
    if not _MAP_COMPONENT_RE.fullmatch(normalized):
        raise MapPathSecurityError(
            code,
            "%s must match [A-Za-z0-9][A-Za-z0-9._-]{0,127}" % label,
        )
    return normalized


def validate_map_name(value, *, allow_empty: bool = False) -> str:
    """Normalize an optional ``.pbstream`` suffix and validate a map ID."""

    raw = _raw_text(value)
    _reject_control_characters(raw, code="invalid_map_name", label="map_name")
    normalized = raw.strip()
    if not normalized:
        if allow_empty:
            return ""
        raise MapPathSecurityError("invalid_map_name", "map_name is required")
    if normalized.endswith(".pbstream"):
        normalized = normalized[: -len(".pbstream")]
    # ``allow_empty`` applies only to a genuinely empty request.  A value such
    # as '.pbstream' must not normalize into an accepted empty identifier.
    return _validate_component(
        normalized,
        label="map_name",
        code="invalid_map_name",
        allow_empty=False,
    )


def validate_revision_id(value, *, allow_empty: bool = False) -> str:
    return _validate_component(
        value,
        label="map_revision_id",
        code="invalid_map_revision_id",
        allow_empty=allow_empty,
    )


def canonical_directory_root(root, *, label: str = "root") -> str:
    raw = _raw_text(root).strip()
    _reject_control_characters(raw, code="invalid_map_root", label=label)
    if not raw:
        raise MapPathSecurityError("invalid_map_root", "%s is required" % label)
    candidate = os.path.abspath(os.path.expanduser(raw))
    if not os.path.lexists(candidate):
        raise MapPathSecurityError("invalid_map_root", "%s does not exist: %s" % (label, candidate))
    try:
        info = os.lstat(candidate)
    except OSError as exc:
        raise MapPathSecurityError("invalid_map_root", "cannot inspect %s: %s" % (label, candidate)) from exc
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise MapPathSecurityError("invalid_map_root", "%s must be a non-symlink directory: %s" % (label, candidate))
    if os.path.realpath(candidate) != candidate:
        raise MapPathSecurityError(
            "invalid_map_root",
            "%s must not contain symlinked path components: %s" % (label, candidate),
        )
    return candidate


def _is_within(root: str, candidate: str) -> bool:
    try:
        return os.path.commonpath((root, candidate)) == root
    except (TypeError, ValueError):
        return False


def _absolute_contained_path(root: str, path, *, label: str) -> str:
    raw = _raw_text(path).strip()
    _reject_control_characters(raw, code="invalid_map_path", label=label)
    if not raw:
        raise MapPathSecurityError("invalid_map_path", "%s is required" % label)
    expanded = os.path.expanduser(raw)
    if not os.path.isabs(expanded):
        raise MapPathSecurityError("invalid_map_path", "%s must be absolute" % label)
    candidate = os.path.abspath(expanded)
    if not _is_within(root, candidate) or candidate == root:
        raise MapPathSecurityError(
            "invalid_map_path",
            "%s is outside the approved root" % label,
        )
    return candidate


def _inspect_parent_chain(root: str, target: str, *, create: bool) -> str:
    parent = os.path.dirname(target)
    if not _is_within(root, parent):
        raise MapPathSecurityError("invalid_map_path", "target parent is outside the approved root")
    relative = os.path.relpath(parent, root)
    if relative in ("", "."):
        return root

    current = root
    for component in relative.split(os.path.sep):
        if component in ("", ".", ".."):
            raise MapPathSecurityError("invalid_map_path", "target contains an unsafe path component")
        current = os.path.join(current, component)
        if not os.path.lexists(current):
            if not create:
                # Later path components cannot exist without this parent.
                return parent
            try:
                os.mkdir(current, 0o750)
            except FileExistsError:
                pass
            except OSError as exc:
                raise MapPathSecurityError(
                    "invalid_map_path", "cannot create map target directory: %s" % current
                ) from exc
        try:
            info = os.lstat(current)
        except OSError as exc:
            raise MapPathSecurityError(
                "invalid_map_path", "cannot inspect map target directory: %s" % current
            ) from exc
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            raise MapPathSecurityError(
                "invalid_map_path",
                "map target parent must be a non-symlink directory: %s" % current,
            )
        if os.path.realpath(current) != current:
            raise MapPathSecurityError(
                "invalid_map_path",
                "map target parent contains a symlink: %s" % current,
            )
    return parent


def ensure_secure_parent_directory(root, target) -> str:
    canonical_root = canonical_directory_root(root, label="maps_root")
    candidate = _absolute_contained_path(canonical_root, target, label="map target")
    return _inspect_parent_chain(canonical_root, candidate, create=True)


def validate_new_file_target(
    root,
    target,
    *,
    suffix: Optional[str] = None,
    require_parent: bool = True,
) -> str:
    canonical_root = canonical_directory_root(root, label="maps_root")
    candidate = _absolute_contained_path(canonical_root, target, label="map target")
    if suffix and not candidate.endswith(str(suffix)):
        raise MapPathSecurityError(
            "invalid_map_path",
            "map target must end with %s" % str(suffix),
        )
    parent = os.path.dirname(candidate)
    _inspect_parent_chain(canonical_root, candidate, create=False)
    if require_parent and not os.path.isdir(parent):
        raise MapPathSecurityError("invalid_map_path", "map target parent does not exist")
    if os.path.lexists(candidate):
        raise MapPathSecurityError("map_path_exists", "map target already exists")
    return candidate


def validate_existing_regular_file(
    root,
    path,
    *,
    suffix: Optional[str] = None,
    label: str = "map source",
) -> str:
    canonical_root = canonical_directory_root(root, label="maps_root")
    candidate = _absolute_contained_path(canonical_root, path, label=label)
    if suffix and not candidate.endswith(str(suffix)):
        raise MapPathSecurityError("invalid_map_path", "%s must end with %s" % (label, suffix))
    _inspect_parent_chain(canonical_root, candidate, create=False)
    if not os.path.lexists(candidate):
        raise MapPathSecurityError("map_source_not_found", "%s not found" % label)
    try:
        info = os.lstat(candidate)
    except OSError as exc:
        raise MapPathSecurityError("invalid_map_path", "cannot inspect %s" % label) from exc
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise MapPathSecurityError(
            "invalid_map_path",
            "%s must be an existing regular non-symlink file" % label,
        )
    if os.path.realpath(candidate) != candidate:
        raise MapPathSecurityError("invalid_map_path", "%s contains a symlink" % label)
    return candidate


def map_asset_target_paths(root, map_name, revision_id="") -> Dict[str, str]:
    canonical_root = canonical_directory_root(root, label="maps_root")
    normalized_name = validate_map_name(map_name)
    normalized_revision = validate_revision_id(revision_id, allow_empty=True)
    base_dir = canonical_root
    if normalized_revision:
        base_dir = os.path.join(
            canonical_root,
            "revisions",
            normalized_name,
            normalized_revision,
        )
    base = os.path.join(base_dir, normalized_name)
    paths = {
        "pbstream_path": base + ".pbstream",
        "yaml_path": base + ".yaml",
        "pgm_path": base + ".pgm",
    }
    for path in paths.values():
        _absolute_contained_path(canonical_root, path, label="map target")
        _inspect_parent_chain(canonical_root, path, create=False)
    return paths


def resolve_yaml_image_path(root, yaml_path, image_reference) -> str:
    canonical_root = canonical_directory_root(root, label="map yaml root")
    yaml_file = validate_existing_regular_file(
        canonical_root,
        yaml_path,
        suffix=".yaml",
        label="map yaml",
    )
    raw = _raw_text(image_reference)
    _reject_control_characters(raw, code="invalid_map_path", label="map yaml image")
    relative = raw.strip()
    if not relative or os.path.isabs(relative) or "\\" in relative:
        raise MapPathSecurityError(
            "invalid_map_path",
            "map yaml image must be a relative path",
        )
    components = relative.split("/")
    if any(component in ("", ".", "..") for component in components):
        raise MapPathSecurityError(
            "invalid_map_path",
            "map yaml image contains an unsafe path component",
        )
    image_path = os.path.abspath(os.path.join(os.path.dirname(yaml_file), *components))
    return validate_existing_regular_file(
        canonical_root,
        image_path,
        suffix=".pgm",
        label="map yaml image",
    )


def copy_regular_file_exclusive(
    source_root,
    source_path,
    target_root,
    target_path,
    *,
    suffix: Optional[str] = None,
) -> str:
    source = validate_existing_regular_file(
        source_root,
        source_path,
        suffix=suffix,
        label="map import source",
    )
    target = validate_new_file_target(target_root, target_path, suffix=suffix, require_parent=True)
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    source_fd = -1
    target_fd = -1
    target_created = False
    copy_succeeded = False
    try:
        source_fd = os.open(source, os.O_RDONLY | nofollow)
        if not stat.S_ISREG(os.fstat(source_fd).st_mode):
            raise MapPathSecurityError(
                "invalid_map_path", "map import source is not a regular file"
            )
        target_fd = os.open(
            target,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | nofollow,
            0o640,
        )
        target_created = True
        while True:
            chunk = os.read(source_fd, 1024 * 1024)
            if not chunk:
                break
            offset = 0
            while offset < len(chunk):
                offset += os.write(target_fd, chunk[offset:])
        os.fsync(target_fd)
        copy_succeeded = True
    except FileExistsError as exc:
        raise MapPathSecurityError("map_path_exists", "map target already exists") from exc
    except OSError as exc:
        if exc.errno == errno.ELOOP:
            raise MapPathSecurityError("invalid_map_path", "map path must not be a symlink") from exc
        raise
    finally:
        if source_fd >= 0:
            os.close(source_fd)
        if target_fd >= 0:
            os.close(target_fd)
        if target_created and not copy_succeeded:
            try:
                os.unlink(target)
            except OSError:
                pass
    return target


def validate_commercial_map_roots(
    maps_root,
    *,
    repo_map_root=None,
    external_maps_root=None,
) -> None:
    maps = canonical_directory_root(maps_root, label="maps_root")
    if maps != COMMERCIAL_MAPS_ROOT:
        raise MapPathSecurityError(
            "invalid_map_root",
            "commercial maps_root must be %s" % COMMERCIAL_MAPS_ROOT,
        )
    if repo_map_root is not None:
        repo = canonical_directory_root(repo_map_root, label="repo_map_root")
        if repo != COMMERCIAL_MAPS_ROOT:
            raise MapPathSecurityError(
                "invalid_map_root",
                "commercial repo_map_root must be %s" % COMMERCIAL_MAPS_ROOT,
            )
    if external_maps_root is not None:
        external = canonical_directory_root(external_maps_root, label="external_maps_root")
        if external != COMMERCIAL_EXTERNAL_MAPS_ROOT:
            raise MapPathSecurityError(
                "invalid_map_root",
                "commercial external_maps_root must be %s" % COMMERCIAL_EXTERNAL_MAPS_ROOT,
            )

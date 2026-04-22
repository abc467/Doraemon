# -*- coding: utf-8 -*-

"""Helpers for deciding whether a map asset/revision is safe for production use."""

from __future__ import annotations

from typing import Any, Mapping


_UNUSABLE_LIFECYCLE_STATUSES = frozenset(
    {
        "archived",
        "draft",
        "failed",
        "saved_unverified",
    }
)


def _verified_identity_present(asset: Mapping[str, Any]) -> bool:
    for key in ("verified_runtime_map_id", "verified_runtime_map_md5", "map_id", "map_md5"):
        if str(asset.get(key) or "").strip():
            return True
    return False


def map_asset_is_verified(asset: Mapping[str, Any]) -> bool:
    if not asset:
        return False
    lifecycle_status = str(asset.get("lifecycle_status") or "").strip().lower()
    if lifecycle_status in _UNUSABLE_LIFECYCLE_STATUSES:
        return False
    verification_status = str(asset.get("verification_status") or "").strip().lower()
    if verification_status:
        return verification_status == "verified"
    return _verified_identity_present(asset)


def map_asset_verification_error(asset: Mapping[str, Any], *, label: str = "map asset") -> str:
    asset_label = str(label or "map asset")
    if not asset:
        return "%s not found" % asset_label
    if not bool(asset.get("enabled", True)):
        return "%s is disabled" % asset_label

    lifecycle_status = str(asset.get("lifecycle_status") or "").strip().lower()
    if lifecycle_status == "saved_unverified":
        return "%s is pending verification" % asset_label
    if lifecycle_status == "failed":
        return "%s verification failed" % asset_label
    if lifecycle_status in ("archived", "draft"):
        return "%s is not available" % asset_label

    verification_status = str(asset.get("verification_status") or "").strip().lower()
    if verification_status and verification_status != "verified":
        if verification_status == "pending":
            return "%s is pending verification" % asset_label
        return "%s is not verified" % asset_label
    if (not verification_status) and (not _verified_identity_present(asset)):
        return "%s is not verified" % asset_label
    return ""

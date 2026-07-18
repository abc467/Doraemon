# -*- coding: utf-8 -*-

"""Fail-closed creation of a FLIRT-frame PBStream revision candidate.

The source revision is never updated.  A successful run creates one new
``saved_unverified/pending`` revision; normal runtime verification is still
required before PlanStore will permit activation.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import uuid
from typing import Any, Callable, Dict, Optional


class FlirtPbstreamMigrationError(RuntimeError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = str(code or "flirt_pbstream_migration_failed")


def sha256_file(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


class SubprocessPbstreamFlirtValidator:
    """Machine-readable wrapper around ``cartographer_pbstream``."""

    def __init__(self, binary: str = "cartographer_pbstream"):
        self.binary = str(binary or "cartographer_pbstream").strip()

    def __call__(self, source_path: str, target_path: str) -> Dict[str, Any]:
        completed = subprocess.run(
            [self.binary, "validate-flirt-migration", source_path, target_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            check=False,
        )
        payload = str(completed.stdout or "").strip()
        try:
            result = json.loads(payload)
        except Exception as exc:
            raise FlirtPbstreamMigrationError(
                "pbstream_validator_failed",
                "validator did not return JSON (exit=%d): %s"
                % (int(completed.returncode), str(completed.stderr or exc).strip()),
            )
        if not isinstance(result, dict):
            raise FlirtPbstreamMigrationError(
                "pbstream_validator_failed", "validator result is not an object"
            )
        result.setdefault("validator_exit_code", int(completed.returncode))
        return result


def _call_runtime_probe(runtime_probe: Any) -> Dict[str, Any]:
    if callable(runtime_probe):
        value = runtime_probe()
    else:
        value = runtime_probe.snapshot()
    if not isinstance(value, dict):
        raise FlirtPbstreamMigrationError(
            "runtime_snapshot_invalid", "runtime snapshot is not an object"
        )
    return dict(value)


def _call_state_writer(state_writer: Any, filename: str):
    if callable(state_writer):
        result = state_writer(filename, include_unfinished_submaps=False)
    else:
        result = state_writer.save_pbstream(
            filename, include_unfinished_submaps=False
        )
    if isinstance(result, tuple):
        ok = bool(result[0])
        message = str(result[1] if len(result) > 1 else "")
    else:
        ok = bool(result)
        message = ""
    if not ok:
        raise FlirtPbstreamMigrationError(
            "write_state_failed", message or "Cartographer write_state failed"
        )


def _source_metadata_fingerprint(asset: Dict[str, Any]) -> str:
    fields = {
        "revision_id": str(asset.get("revision_id") or ""),
        "map_name": str(asset.get("map_name") or ""),
        "pbstream_path": os.path.realpath(
            os.path.expanduser(str(asset.get("pbstream_path") or ""))
        ),
        "yaml_path": str(asset.get("yaml_path") or ""),
        "pgm_path": str(asset.get("pgm_path") or ""),
        "frame_id": str(asset.get("frame_id") or ""),
        "resolution": float(asset.get("resolution") or 0.0),
        "origin": list(asset.get("origin") or [0.0, 0.0, 0.0]),
    }
    return hashlib.sha256(
        json.dumps(fields, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


class FlirtPbstreamRevisionMigrator:
    """Orchestrates dry-run or creation of a non-active revision candidate."""

    def __init__(
        self,
        *,
        plan_store: Any,
        asset_helper: Any,
        runtime_probe: Any,
        state_writer: Any,
        validator: Callable[[str, str], Dict[str, Any]],
    ):
        self._store = plan_store
        self._assets = asset_helper
        self._runtime_probe = runtime_probe
        self._state_writer = state_writer
        self._validator = validator

    @staticmethod
    def _runtime_values(snapshot: Dict[str, Any]):
        revision_id = str(
            snapshot.get("current_map_revision_id")
            or snapshot.get("map_revision_id")
            or snapshot.get("loaded_revision_id")
            or ""
        ).strip()
        pbstream_path = os.path.expanduser(
            str(
                snapshot.get("current_pbstream_path")
                or snapshot.get("pbstream_path")
                or snapshot.get("loaded_pbstream_path")
                or ""
            ).strip()
        )
        backfill_state = str(
            snapshot.get("flirt_feature_backfill_state")
            or snapshot.get("flirt_backfill_state")
            or ""
        ).strip().upper()
        return revision_id, pbstream_path, backfill_state

    def _assert_runtime_source(
        self, snapshot: Dict[str, Any], source_revision_id: str, source_path: str
    ):
        runtime_revision, runtime_path, backfill_state = self._runtime_values(snapshot)
        if runtime_revision != source_revision_id:
            raise FlirtPbstreamMigrationError(
                "runtime_revision_mismatch",
                "runtime must have the exact source revision loaded",
            )
        if not runtime_path or os.path.realpath(runtime_path) != os.path.realpath(source_path):
            raise FlirtPbstreamMigrationError(
                "runtime_pbstream_mismatch",
                "runtime PBStream path does not match the source revision",
            )
        if backfill_state != "READY":
            raise FlirtPbstreamMigrationError(
                "flirt_backfill_not_ready",
                "FLIRT feature backfill must report READY",
            )

    @staticmethod
    def _assert_validation(result: Dict[str, Any]):
        required = (
            "ok",
            "source_node_count",
            "target_node_count",
            "source_submap_count",
            "target_submap_count",
            "target_gravity_aligned_node_count",
            "target_invalid_frame_node_count",
            "target_legacy_tag8_feature_count",
            "target_gravity_aligned_feature_count",
            "source_geometry_fingerprint",
            "target_geometry_fingerprint",
        )
        missing = [key for key in required if key not in result]
        if missing:
            raise FlirtPbstreamMigrationError(
                "pbstream_validation_incomplete",
                "validator omitted fields: %s" % ", ".join(missing),
            )
        gates_ok = (
            bool(result.get("ok"))
            and int(result["source_node_count"]) == int(result["target_node_count"])
            and int(result["source_submap_count"])
            == int(result["target_submap_count"])
            and int(result["target_gravity_aligned_node_count"])
            == int(result["target_node_count"])
            and int(result["target_invalid_frame_node_count"]) == 0
            and int(result["target_legacy_tag8_feature_count"]) == 0
            and str(result["source_geometry_fingerprint"])
            == str(result["target_geometry_fingerprint"])
        )
        if not gates_ok:
            errors = result.get("errors") or []
            raise FlirtPbstreamMigrationError(
                "pbstream_validation_failed",
                "PBStream migration validation failed: %s"
                % ("; ".join(str(item) for item in errors) or "gate mismatch"),
            )

    def prepare(
        self, *, source_revision_id: str, target_revision_id: str = ""
    ) -> Dict[str, Any]:
        source_revision_id = str(source_revision_id or "").strip()
        if not source_revision_id:
            raise FlirtPbstreamMigrationError(
                "source_revision_required", "source_revision_id is required"
            )
        source = self._store.resolve_map_revision(
            revision_id=source_revision_id
        ) or {}
        if not source:
            raise FlirtPbstreamMigrationError(
                "source_revision_not_found", "source revision was not found"
            )
        source_path = os.path.expanduser(
            str(source.get("pbstream_path") or "").strip()
        )
        if not source_path or not os.path.isfile(source_path):
            raise FlirtPbstreamMigrationError(
                "source_pbstream_not_found", "source PBStream is not a regular file"
            )

        map_name = str(source.get("map_name") or "").strip()
        target_revision_id = str(target_revision_id or "").strip()
        if not target_revision_id:
            target_revision_id = self._store.generate_map_revision_id(map_name)
        if target_revision_id == source_revision_id:
            raise FlirtPbstreamMigrationError(
                "target_revision_invalid", "target revision must differ from source"
            )
        if self._store.resolve_map_revision(revision_id=target_revision_id):
            raise FlirtPbstreamMigrationError(
                "target_revision_exists", "target revision already exists"
            )
        paths = self._assets.target_paths(map_name, revision_id=target_revision_id)
        target_path = os.path.expanduser(str(paths.get("pbstream_path") or ""))
        if not target_path:
            raise FlirtPbstreamMigrationError(
                "target_path_invalid", "target PBStream path is empty"
            )
        target_canonical = os.path.join(
            os.path.realpath(os.path.dirname(target_path) or "."),
            os.path.basename(target_path),
        )
        if target_canonical == os.path.realpath(source_path):
            raise FlirtPbstreamMigrationError(
                "source_target_same_file", "source and target resolve to the same file"
            )
        if os.path.lexists(target_path):
            raise FlirtPbstreamMigrationError(
                "target_path_exists", "target artifact already exists"
            )
        runtime_snapshot = _call_runtime_probe(self._runtime_probe)
        self._assert_runtime_source(
            runtime_snapshot, source_revision_id, source_path
        )
        return {
            "status": "preflight_ready",
            "source_revision_id": source_revision_id,
            "target_revision_id": target_revision_id,
            "map_name": map_name,
            "source_pbstream_path": source_path,
            "target_pbstream_path": target_path,
            "source_metadata_fingerprint": _source_metadata_fingerprint(source),
            "source": source,
        }

    def migrate(
        self,
        *,
        source_revision_id: str,
        target_revision_id: str = "",
        execute_candidate: bool = False,
    ) -> Dict[str, Any]:
        plan = self.prepare(
            source_revision_id=source_revision_id,
            target_revision_id=target_revision_id,
        )
        public_plan = {key: value for key, value in plan.items() if key != "source"}
        public_plan.update(
            {
                "mode": "candidate" if execute_candidate else "dry_run",
                "activation_performed": False,
                "activation_allowed": False,
            }
        )
        if not execute_candidate:
            public_plan["status"] = "dry_run_ready"
            return public_plan

        source = dict(plan["source"])
        source_path = str(plan["source_pbstream_path"])
        target_path = str(plan["target_pbstream_path"])
        target_dir = os.path.dirname(target_path) or "."
        os.makedirs(target_dir, exist_ok=True)
        lock_path = target_path + ".migration.lock"
        partial_path = target_path + ".partial." + uuid.uuid4().hex
        lock_fd: Optional[int] = None
        published = False
        registered = False
        try:
            try:
                lock_fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            except FileExistsError:
                raise FlirtPbstreamMigrationError(
                    "migration_in_progress", "target migration is already in progress"
                )
            if os.path.lexists(target_path):
                raise FlirtPbstreamMigrationError(
                    "target_path_exists", "target artifact appeared during preflight"
                )
            source_sha256_before = sha256_file(source_path)
            _call_state_writer(self._state_writer, partial_path)
            if os.path.islink(partial_path) or not os.path.isfile(partial_path):
                raise FlirtPbstreamMigrationError(
                    "write_state_artifact_invalid",
                    "write_state did not create a regular candidate file",
                )
            if sha256_file(source_path) != source_sha256_before:
                raise FlirtPbstreamMigrationError(
                    "source_pbstream_changed", "source PBStream changed during write_state"
                )

            self._assert_runtime_source(
                _call_runtime_probe(self._runtime_probe),
                str(plan["source_revision_id"]),
                source_path,
            )
            validation = dict(self._validator(source_path, partial_path) or {})
            self._assert_validation(validation)
            if sha256_file(source_path) != source_sha256_before:
                raise FlirtPbstreamMigrationError(
                    "source_pbstream_changed", "source PBStream changed during validation"
                )
            current_source = self._store.resolve_map_revision(
                revision_id=str(plan["source_revision_id"])
            ) or {}
            if _source_metadata_fingerprint(current_source) != str(
                plan["source_metadata_fingerprint"]
            ):
                raise FlirtPbstreamMigrationError(
                    "source_revision_changed", "source revision metadata changed"
                )

            candidate_sha256 = sha256_file(partial_path)
            # Hard-link publication is atomic and fails instead of overwriting
            # an artifact that raced us into the final path.
            try:
                os.link(partial_path, target_path)
            except FileExistsError:
                raise FlirtPbstreamMigrationError(
                    "target_path_exists", "target artifact appeared before publication"
                )
            published = True
            os.unlink(partial_path)
            if sha256_file(target_path) != candidate_sha256:
                raise FlirtPbstreamMigrationError(
                    "candidate_publish_mismatch", "published candidate hash changed"
                )
            if sha256_file(source_path) != source_sha256_before:
                raise FlirtPbstreamMigrationError(
                    "source_pbstream_changed", "source PBStream changed during publication"
                )

            self._store.upsert_map_revision(
                revision_id=str(plan["target_revision_id"]),
                map_name=str(plan["map_name"]),
                display_name=(str(source.get("display_name") or plan["map_name"]) + " (FLIRT migrated)"),
                enabled=True,
                description=(
                    "FLIRT NODE_GRAVITY_ALIGNED migration candidate from %s"
                    % str(plan["source_revision_id"])
                ),
                map_id="",
                map_md5="",
                yaml_path=str(source.get("yaml_path") or ""),
                pgm_path=str(source.get("pgm_path") or ""),
                pbstream_path=target_path,
                frame_id=str(source.get("frame_id") or "map"),
                resolution=float(source.get("resolution") or 0.0),
                origin=list(source.get("origin") or [0.0, 0.0, 0.0]),
                lifecycle_status="saved_unverified",
                verification_status="pending",
                live_snapshot_md5="",
                verified_runtime_map_id="",
                verified_runtime_map_md5="",
                last_error_code="",
                last_error_msg="",
                source_job_id="flirt_frame_migration:%s"
                % str(plan["source_revision_id"]),
            )
            registered = True
            if sha256_file(source_path) != source_sha256_before:
                raise FlirtPbstreamMigrationError(
                    "source_pbstream_changed", "source PBStream changed after registration"
                )
            public_plan.update(
                {
                    "status": "candidate_registered_pending_verification",
                    "source_sha256": source_sha256_before,
                    "target_sha256": candidate_sha256,
                    "validation": validation,
                    # Existing PlanStore activation rejects pending revisions.
                    "activation_allowed": False,
                }
            )
            return public_plan
        except Exception:
            if published and not registered:
                try:
                    os.unlink(target_path)
                except OSError:
                    pass
            raise
        finally:
            if lock_fd is not None:
                os.close(lock_fd)
            for path in (partial_path, lock_path):
                try:
                    os.unlink(path)
                except OSError:
                    pass


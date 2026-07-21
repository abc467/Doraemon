#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import hashlib
import os
import struct
from typing import Dict, List, Tuple

import numpy as np
import yaml
from nav_msgs.msg import MapMetaData, OccupancyGrid

from coverage_planner.map_path_security import (
    canonical_directory_root,
    ensure_secure_parent_directory,
    resolve_yaml_image_path,
    validate_existing_regular_file,
    validate_map_name,
    validate_new_file_target,
)


def read_pgm(path: str):
    """Read binary P5 PGM and return (w, h, maxval, np.uint8[h, w])."""
    with open(path, "rb") as f:
        magic = f.readline().strip()
        if magic != b"P5":
            raise RuntimeError("Unsupported PGM format: %s" % magic)

        def next_token():
            while True:
                line = f.readline()
                if not line:
                    raise RuntimeError("Unexpected EOF while reading PGM header")
                line = line.strip()
                if (not line) or line.startswith(b"#"):
                    continue
                return line

        wh = next_token().split()
        while len(wh) < 2:
            wh += next_token().split()
        w, h = int(wh[0]), int(wh[1])

        maxval = int(next_token())
        if maxval > 255:
            raise RuntimeError("Only 8-bit PGM is supported")

        img = np.frombuffer(f.read(w * h), dtype=np.uint8)
        if img.size != w * h:
            raise RuntimeError("PGM size mismatch")
        return w, h, maxval, img.reshape((h, w))


def yaml_pgm_to_occupancy(yaml_path: str, *, allowed_root: str = "") -> OccupancyGrid:
    yaml_path = os.path.abspath(os.path.expanduser(str(yaml_path or "").strip()))
    root = str(allowed_root or "").strip() or os.path.dirname(yaml_path)
    root = canonical_directory_root(root, label="map yaml root")
    yaml_path = validate_existing_regular_file(
        root,
        yaml_path,
        suffix=".yaml",
        label="map yaml",
    )
    with open(yaml_path, "r", encoding="utf-8") as f:
        meta = yaml.safe_load(f) or {}

    image_path = resolve_yaml_image_path(root, yaml_path, meta["image"])

    resolution = float(meta["resolution"])
    origin = list(meta["origin"])
    negate = int(meta.get("negate", 0))
    occ_th = float(meta.get("occupied_thresh", 0.65))
    free_th = float(meta.get("free_thresh", 0.196))

    w, h, _maxval, img = read_pgm(image_path)
    img = np.flipud(img).astype(np.float32)
    if negate == 1:
        img = 255.0 - img

    p_occ = (255.0 - img) / 255.0
    occ = np.full((h, w), -1, dtype=np.int8)
    occ[p_occ > occ_th] = 100
    occ[p_occ < free_th] = 0

    msg = OccupancyGrid()
    msg.header.frame_id = "map"
    msg.info = MapMetaData()
    msg.info.resolution = resolution
    msg.info.width = w
    msg.info.height = h
    msg.info.origin.position.x = float(origin[0])
    msg.info.origin.position.y = float(origin[1])
    msg.info.origin.position.z = 0.0
    msg.info.origin.orientation.w = 1.0
    msg.data = occ.reshape(-1).tolist()
    return msg


def occupancy_to_pgm_image(occ: OccupancyGrid) -> np.ndarray:
    info = occ.info
    data = np.array(occ.data, dtype=np.int16).reshape((info.height, info.width))
    img = np.zeros((info.height, info.width), dtype=np.uint8)
    img[data < 0] = 205
    img[data == 0] = 254
    img[data > 0] = 0
    return np.flipud(img)


def occupancy_to_yaml_dict(occ: OccupancyGrid, image_name: str = "map.pgm") -> Dict[str, object]:
    info = occ.info
    return {
        "image": str(image_name),
        "resolution": float(info.resolution),
        "origin": [float(info.origin.position.x), float(info.origin.position.y), 0.0],
        "negate": 0,
        "occupied_thresh": 0.65,
        "free_thresh": 0.196,
    }


def write_occupancy_to_yaml_pgm(
    occ: OccupancyGrid,
    out_dir: str,
    *,
    base_name: str = "map",
    allowed_root: str = "",
) -> Tuple[str, str]:
    normalized_name = validate_map_name(base_name)
    out_dir = os.path.abspath(os.path.expanduser(str(out_dir or "").strip()))
    if allowed_root:
        root = canonical_directory_root(allowed_root, label="maps_root")
        probe_target = os.path.join(out_dir, normalized_name + ".pgm")
        ensure_secure_parent_directory(root, probe_target)
    else:
        # Offline maintenance tools already choose this directory explicitly;
        # retain their creation behavior, then treat it as the security root.
        os.makedirs(out_dir, mode=0o750, exist_ok=True)
        root = canonical_directory_root(out_dir, label="map output root")

    pgm_path = validate_new_file_target(
        root,
        os.path.join(out_dir, normalized_name + ".pgm"),
        suffix=".pgm",
    )
    yaml_path = validate_new_file_target(
        root,
        os.path.join(out_dir, normalized_name + ".yaml"),
        suffix=".yaml",
    )

    img = occupancy_to_pgm_image(occ)
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    created_paths = []
    try:
        pgm_fd = os.open(
            pgm_path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | nofollow,
            0o640,
        )
        created_paths.append(pgm_path)
        with os.fdopen(pgm_fd, "wb") as f:
            f.write(f"P5\n{occ.info.width} {occ.info.height}\n255\n".encode("ascii"))
            f.write(img.tobytes())

        meta = occupancy_to_yaml_dict(occ, image_name=normalized_name + ".pgm")
        yaml_fd = os.open(
            yaml_path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | nofollow,
            0o640,
        )
        created_paths.append(yaml_path)
        with os.fdopen(yaml_fd, "w", encoding="utf-8") as f:
            yaml.safe_dump(meta, f, default_flow_style=False, sort_keys=False)
    except Exception:
        for path in reversed(created_paths):
            try:
                os.unlink(path)
            except OSError:
                pass
        raise
    return pgm_path, yaml_path


def origin_to_jsonable(occ: OccupancyGrid) -> List[float]:
    info = occ.info
    return [float(info.origin.position.x), float(info.origin.position.y), 0.0]


def compute_occupancy_grid_md5(msg: OccupancyGrid) -> str:
    info = msg.info
    o = info.origin
    buf = bytearray()
    buf += struct.pack("<II", int(info.width), int(info.height))
    buf += struct.pack("<f", float(info.resolution))
    buf += struct.pack(
        "<ffffff",
        float(o.position.x),
        float(o.position.y),
        float(o.position.z),
        float(o.orientation.x),
        float(o.orientation.y),
        float(o.orientation.z),
    )
    buf += struct.pack("<f", float(o.orientation.w))
    buf += bytes(((int(v) + 256) & 0xFF) for v in (msg.data or []))
    h = hashlib.md5()
    h.update(buf)
    return h.hexdigest()

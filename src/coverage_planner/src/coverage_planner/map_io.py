#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import hashlib
import os
from typing import Dict, List, Tuple

import numpy as np
import yaml
from nav_msgs.msg import MapMetaData, OccupancyGrid


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


def yaml_pgm_to_occupancy(yaml_path: str) -> OccupancyGrid:
    with open(yaml_path, "r") as f:
        meta = yaml.safe_load(f) or {}

    image_path = str(meta["image"])
    if not os.path.isabs(image_path):
        image_path = os.path.join(os.path.dirname(yaml_path), image_path)

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


def write_occupancy_to_yaml_pgm(occ: OccupancyGrid, out_dir: str, *, base_name: str = "map") -> Tuple[str, str]:
    out_dir = os.path.expanduser(str(out_dir))
    os.makedirs(out_dir, exist_ok=True)
    pgm_path = os.path.join(out_dir, f"{base_name}.pgm")
    yaml_path = os.path.join(out_dir, f"{base_name}.yaml")

    img = occupancy_to_pgm_image(occ)
    with open(pgm_path, "wb") as f:
        f.write(f"P5\n{occ.info.width} {occ.info.height}\n255\n".encode("ascii"))
        f.write(img.tobytes())

    meta = occupancy_to_yaml_dict(occ, image_name=f"{base_name}.pgm")
    with open(yaml_path, "w") as f:
        yaml.safe_dump(meta, f, default_flow_style=False, sort_keys=False)
    return pgm_path, yaml_path


def origin_to_jsonable(occ: OccupancyGrid) -> List[float]:
    info = occ.info
    return [float(info.origin.position.x), float(info.origin.position.y), 0.0]


def compute_occupancy_grid_md5(msg: OccupancyGrid) -> str:
    h = hashlib.md5()
    info = msg.info
    h.update(str(int(info.width)).encode("utf-8"))
    h.update(b"|")
    h.update(str(int(info.height)).encode("utf-8"))
    h.update(b"|")
    h.update(("{:.9f}".format(float(info.resolution))).encode("utf-8"))
    h.update(b"|")
    h.update(("{:.9f},{:.9f},{:.9f},{:.9f},{:.9f},{:.9f},{:.9f}".format(
        float(info.origin.position.x),
        float(info.origin.position.y),
        float(info.origin.position.z),
        float(info.origin.orientation.x),
        float(info.origin.orientation.y),
        float(info.origin.orientation.z),
        float(info.origin.orientation.w),
    )).encode("utf-8"))
    h.update(b"|")
    h.update(np.asarray(list(msg.data), dtype=np.int16).tobytes())
    return h.hexdigest()

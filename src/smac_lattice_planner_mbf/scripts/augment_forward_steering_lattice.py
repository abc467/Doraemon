#!/usr/bin/env python3

"""Add minimum-radius translating steering arcs missing from a Nav2 lattice.

Nav2's differential-drive minimum control set intentionally uses one-bin
rotate-in-place primitives to leave some axis-aligned headings.  A planner
that hard-disables all stationary motion must not use that reduced set as-is:
otherwise those headings retain only a straight translating successor.  This
tool preserves the official generated primitives and adds only the missing
forward left/right arcs, sampled from the same configured turning radius.
"""

import argparse
import json
import math
from pathlib import Path
from typing import Any, Dict, List


TRANSLATION_EPSILON = 1.0e-4
ANGLE_EPSILON = 1.0e-6


def shortest_angle(start: float, end: float) -> float:
    return (end - start + math.pi) % (2.0 * math.pi) - math.pi


def primitive_direction(
    primitive: Dict[str, Any], headings: List[float]
) -> int:
    end_pose = primitive["poses"][-1]
    if math.hypot(end_pose[0], end_pose[1]) <= TRANSLATION_EPSILON:
        return 0
    delta = shortest_angle(
        headings[primitive["start_angle_index"]],
        headings[primitive["end_angle_index"]],
    )
    if delta > ANGLE_EPSILON:
        return 1
    if delta < -ANGLE_EPSILON:
        return -1
    return 0


def make_arc(
    start_index: int,
    end_index: int,
    headings: List[float],
    radius: float,
    sample_spacing: float,
) -> Dict[str, Any]:
    start_yaw = headings[start_index]
    end_yaw = headings[end_index]
    delta = shortest_angle(start_yaw, end_yaw)
    if abs(delta) <= ANGLE_EPSILON:
        raise ValueError("steering arc must change heading")

    curvature = math.copysign(1.0 / radius, delta)
    arc_length = radius * abs(delta)
    samples = max(2, int(math.ceil(arc_length / sample_spacing)))
    poses = []
    for sample in range(1, samples + 1):
        fraction = sample / samples
        yaw = start_yaw + delta * fraction
        x = (math.sin(yaw) - math.sin(start_yaw)) / curvature
        y = -(math.cos(yaw) - math.cos(start_yaw)) / curvature
        if sample == samples:
            yaw = end_yaw
        poses.append([round(x, 5) + 0.0, round(y, 5) + 0.0, yaw % (2.0 * math.pi)])

    return {
        "trajectory_id": -1,
        "start_angle_index": start_index,
        "end_angle_index": end_index,
        "left_turn": delta > 0.0,
        "trajectory_radius": radius,
        "trajectory_length": round(arc_length, 5),
        "arc_length": round(arc_length, 5),
        "straight_length": 0.0,
        "poses": poses,
    }


def augment(document: Dict[str, Any]) -> Dict[str, Any]:
    metadata = document["lattice_metadata"]
    if metadata["motion_model"] != "diff":
        raise ValueError("base lattice must use the differential-drive motion model")
    headings = metadata["heading_angles"]
    radius = float(metadata["turning_radius"])
    sample_spacing = float(metadata["grid_resolution"])
    if len(headings) < 8 or radius <= 0.0 or sample_spacing <= 0.0:
        raise ValueError("invalid lattice metadata")

    primitives = list(document["primitives"])
    heading_count = len(headings)
    for start_index in range(heading_count):
        directions = {
            primitive_direction(primitive, headings)
            for primitive in primitives
            if primitive["start_angle_index"] == start_index
        }
        if -1 not in directions:
            primitives.append(
                make_arc(
                    start_index,
                    (start_index - 1) % heading_count,
                    headings,
                    radius,
                    sample_spacing,
                )
            )
        if 1 not in directions:
            primitives.append(
                make_arc(
                    start_index,
                    (start_index + 1) % heading_count,
                    headings,
                    radius,
                    sample_spacing,
                )
            )

    primitives.sort(
        key=lambda primitive: (
            primitive["start_angle_index"],
            primitive["end_angle_index"],
            primitive["trajectory_length"],
        )
    )
    for trajectory_id, primitive in enumerate(primitives):
        primitive["trajectory_id"] = trajectory_id

    for start_index in range(heading_count):
        available = {
            primitive_direction(primitive, headings)
            for primitive in primitives
            if primitive["start_angle_index"] == start_index
        }
        has_straight = any(
            primitive_direction(primitive, headings) == 0
            and math.hypot(
                primitive["poses"][-1][0], primitive["poses"][-1][1]
            )
            > TRANSLATION_EPSILON
            for primitive in primitives
            if primitive["start_angle_index"] == start_index
        )
        if not has_straight or -1 not in available or 1 not in available:
            raise RuntimeError(
                "heading {} lacks forward left/straight/right controls".format(start_index)
            )

    document["primitives"] = primitives
    metadata["number_of_trajectories"] = len(primitives)
    document["forward_steering_augmentation"] = {
        "base": "Nav2 differential-drive minimum control set",
        "contract": "every heading has translating left, straight, and right controls",
    }
    return document


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    with args.input.open() as source:
        document = json.load(source)
    augmented = augment(document)
    with args.output.open("w") as destination:
        json.dump(augmented, destination, indent="\t")
        destination.write("\n")


if __name__ == "__main__":
    main()

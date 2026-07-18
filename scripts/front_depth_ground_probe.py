#!/usr/bin/env python3
"""Probe front depth-camera floor alignment.

Subscribes to a PointCloud2 topic, transforms samples into base_link, fits the
dominant floor plane, and prints the roll/pitch correction that would rotate the
observed plane normal onto +Z.
"""

import math
import random
import sys
from typing import Optional, Tuple

import numpy as np
import rospy
import sensor_msgs.point_cloud2 as pc2
import tf2_ros
from sensor_msgs.msg import PointCloud2
from tf2_sensor_msgs.tf2_sensor_msgs import do_transform_cloud


def fit_plane_svd(points: np.ndarray) -> Tuple[np.ndarray, float, np.ndarray]:
    centroid = points.mean(axis=0)
    _, _, vh = np.linalg.svd(points - centroid, full_matrices=False)
    normal = vh[-1]
    if normal[2] < 0.0:
        normal = -normal
    d = -float(normal.dot(centroid))
    return normal, d, points.dot(normal) + d


def ransac_plane(points: np.ndarray, threshold: float, iterations: int) -> Optional[np.ndarray]:
    if points.shape[0] < 50:
        return None

    best_inliers = None
    best_count = 0
    rng = random.Random(20260708)
    indices = range(points.shape[0])
    for _ in range(iterations):
        i1, i2, i3 = rng.sample(indices, 3)
        p1, p2, p3 = points[i1], points[i2], points[i3]
        normal = np.cross(p2 - p1, p3 - p1)
        norm = np.linalg.norm(normal)
        if norm < 1e-6:
            continue
        normal = normal / norm
        if normal[2] < 0.0:
            normal = -normal
        if normal[2] < 0.35:
            continue
        d = -float(normal.dot(p1))
        distances = np.abs(points.dot(normal) + d)
        inliers = distances < threshold
        count = int(inliers.sum())
        if count > best_count:
            best_count = count
            best_inliers = inliers

    return best_inliers


class Probe:
    def __init__(self) -> None:
        self.input_topic = rospy.get_param("~input_topic", "/gemini_front/depth/points")
        self.target_frame = rospy.get_param("~target_frame", "base_link")
        self.samples_needed = int(rospy.get_param("~samples", 5))
        self.timeout = float(rospy.get_param("~timeout", 12.0))
        self.max_points_per_sample = int(rospy.get_param("~max_points_per_sample", 6000))
        self.ransac_threshold = float(rospy.get_param("~ransac_threshold", 0.035))
        self.ransac_iterations = int(rospy.get_param("~ransac_iterations", 1200))

        self.x_min = float(rospy.get_param("~x_min", 0.75))
        self.x_max = float(rospy.get_param("~x_max", 4.0))
        self.y_min = float(rospy.get_param("~y_min", -1.6))
        self.y_max = float(rospy.get_param("~y_max", 1.6))
        self.z_min = float(rospy.get_param("~z_min", -1.5))
        self.z_max = float(rospy.get_param("~z_max", 1.5))

        self.tf_buffer = tf2_ros.Buffer(rospy.Duration(10.0))
        self.tf_listener = tf2_ros.TransformListener(self.tf_buffer)
        self.messages = []
        self.rng = np.random.default_rng(20260708)
        self.sub = rospy.Subscriber(self.input_topic, PointCloud2, self._cloud_cb, queue_size=1)

    def _cloud_cb(self, msg: PointCloud2) -> None:
        if len(self.messages) < self.samples_needed:
            self.messages.append(msg)

    def wait(self) -> bool:
        deadline = rospy.Time.now() + rospy.Duration(self.timeout)
        rate = rospy.Rate(20)
        while not rospy.is_shutdown() and rospy.Time.now() < deadline:
            if len(self.messages) >= self.samples_needed:
                return True
            rate.sleep()
        return bool(self.messages)

    def collect_points(self) -> np.ndarray:
        chunks = []
        for msg in self.messages:
            transform = self.tf_buffer.lookup_transform(
                self.target_frame, msg.header.frame_id, msg.header.stamp, rospy.Duration(0.5)
            )
            cloud = do_transform_cloud(msg, transform)
            points = np.array(
                list(pc2.read_points(cloud, field_names=("x", "y", "z"), skip_nans=True)),
                dtype=np.float32,
            )
            if points.size == 0:
                continue
            mask = (
                (points[:, 0] > self.x_min)
                & (points[:, 0] < self.x_max)
                & (points[:, 1] > self.y_min)
                & (points[:, 1] < self.y_max)
                & (points[:, 2] > self.z_min)
                & (points[:, 2] < self.z_max)
            )
            roi = points[mask]
            if roi.shape[0] > self.max_points_per_sample:
                keep = self.rng.choice(roi.shape[0], self.max_points_per_sample, replace=False)
                roi = roi[keep]
            chunks.append(roi)

        if not chunks:
            return np.empty((0, 3), dtype=np.float32)
        return np.concatenate(chunks, axis=0)

    def run(self) -> int:
        if not self.wait():
            print(f"no point cloud messages from {self.input_topic}", file=sys.stderr)
            return 2

        points = self.collect_points()
        print(f"samples={len(self.messages)} roi_points={points.shape[0]}")
        if points.shape[0] < 200:
            print("not enough ROI points for plane fit", file=sys.stderr)
            return 3

        inliers = ransac_plane(points, self.ransac_threshold, self.ransac_iterations)
        if inliers is None or int(inliers.sum()) < 200:
            print("floor plane RANSAC failed", file=sys.stderr)
            return 4

        floor_points = points[inliers]
        normal, d, residuals = fit_plane_svd(floor_points)
        tilt = math.degrees(math.acos(max(-1.0, min(1.0, float(normal[2])))))
        a = -float(normal[0] / normal[2])
        b = -float(normal[1] / normal[2])
        c = -float(d / normal[2])

        roll_delta = math.atan2(float(normal[1]), float(normal[2]))
        pitch_delta = math.atan2(-float(normal[0]), math.hypot(float(normal[1]), float(normal[2])))

        print(f"inliers={floor_points.shape[0]} ({floor_points.shape[0] / points.shape[0] * 100.0:.1f}%)")
        print(
            "plane_normal_{}=[{:.6f}, {:.6f}, {:.6f}] d={:.6f}".format(
                self.target_frame, normal[0], normal[1], normal[2], d
            )
        )
        print(f"tilt_from_horizontal_deg={tilt:.3f}")
        print(f"plane_as_z=ax+by+c: a={a:.6f} b={b:.6f} c={c:.6f}")
        print(
            "suggest_delta_rpy_rad: roll={:.7f} pitch={:.7f} yaw=0".format(
                roll_delta, pitch_delta
            )
        )
        print(
            "suggest_delta_rpy_deg: roll={:.3f} pitch={:.3f} yaw=0".format(
                math.degrees(roll_delta), math.degrees(pitch_delta)
            )
        )
        print(
            "residual_abs_m: median={:.4f} p95={:.4f}".format(
                float(np.median(np.abs(residuals))),
                float(np.percentile(np.abs(residuals), 95)),
            )
        )
        return 0


def main() -> None:
    rospy.init_node("front_depth_ground_probe", anonymous=True)
    raise SystemExit(Probe().run())


if __name__ == "__main__":
    main()

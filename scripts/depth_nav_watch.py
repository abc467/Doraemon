#!/usr/bin/env python3
import math
import time
from typing import Dict, List, Optional, Tuple

import numpy as np
import rospy
import sensor_msgs.point_cloud2 as pc2
import tf2_ros
from actionlib_msgs.msg import GoalStatusArray
from geometry_msgs.msg import Twist
from nav_msgs.msg import OccupancyGrid, Path
from sensor_msgs.msg import PointCloud2
from tf.transformations import quaternion_matrix


class DepthNavWatch:
    def __init__(self) -> None:
        self.tf_buffer = tf2_ros.Buffer(cache_time=rospy.Duration(10.0))
        self.tf_listener = tf2_ros.TransformListener(self.tf_buffer)

        self.clouds: Dict[str, PointCloud2] = {}
        self.costmap: Optional[OccupancyGrid] = None
        self.cmd_vel: Optional[Twist] = None
        self.status: Dict[str, GoalStatusArray] = {}
        self.paths: Dict[str, Path] = {}
        self.last_print = 0.0

        for topic in ("/left/obstacle_2d", "/right/obstacle_2d", "/up/obstacle_2d"):
            rospy.Subscriber(topic, PointCloud2, self._cloud_cb, topic, queue_size=1)

        rospy.Subscriber(
            "/move_base_flex/local_costmap/costmap",
            OccupancyGrid,
            self._costmap_cb,
            queue_size=1,
        )
        rospy.Subscriber("/cmd_vel", Twist, self._cmd_cb, queue_size=1)

        for topic in ("/move_base_flex/exe_path/status", "/move_base_flex/move_base/status"):
            rospy.Subscriber(topic, GoalStatusArray, self._status_cb, topic, queue_size=1)

        for name in ("Standard", "Connect", "Heavy", "Eco"):
            topic = f"/move_base_flex/MPPI_{name}_Controller/optimal_trajectory"
            rospy.Subscriber(topic, Path, self._path_cb, name, queue_size=1)

    def _cloud_cb(self, msg: PointCloud2, topic: str) -> None:
        self.clouds[topic] = msg

    def _costmap_cb(self, msg: OccupancyGrid) -> None:
        self.costmap = msg

    def _cmd_cb(self, msg: Twist) -> None:
        self.cmd_vel = msg

    def _status_cb(self, msg: GoalStatusArray, topic: str) -> None:
        self.status[topic] = msg

    def _path_cb(self, msg: Path, name: str) -> None:
        self.paths[name] = msg

    def _transform_matrix(self, target: str, source: str):
        transform = self.tf_buffer.lookup_transform(
            target, source, rospy.Time(0), rospy.Duration(0.05)
        )
        q = transform.transform.rotation
        t = transform.transform.translation
        return quaternion_matrix([q.x, q.y, q.z, q.w])[:3, :3], np.array([t.x, t.y, t.z])

    def _cloud_summary(self, topic: str) -> str:
        msg = self.clouds.get(topic)
        if msg is None:
            return f"{topic}: no_msg"
        count = msg.width * msg.height
        age = (rospy.Time.now() - msg.header.stamp).to_sec()
        if count == 0:
            return f"{topic}: n=0 age={age:.2f}s"

        try:
            rot, trans = self._transform_matrix("base_link", msg.header.frame_id)
        except Exception as exc:
            return f"{topic}: n={count} tf_error={exc}"

        pts: List[Tuple[float, float, float]] = []
        for idx, point in enumerate(pc2.read_points(msg, field_names=("x", "y", "z"), skip_nans=True)):
            if idx % 4:
                continue
            xyz = rot.dot(np.array(point, dtype=float)) + trans
            pts.append((float(xyz[0]), float(xyz[1]), float(xyz[2])))

        if not pts:
            return f"{topic}: n={count} sampled_empty age={age:.2f}s"

        xs, ys, zs = zip(*pts)
        min_front = min(xs)
        if topic.startswith("/left"):
            side = "L"
        elif topic.startswith("/right"):
            side = "R"
        else:
            side = "F"
        return (
            f"{side}: n={count} bbox_x=[{min(xs):.2f},{max(xs):.2f}] "
            f"bbox_y=[{min(ys):.2f},{max(ys):.2f}] z=[{min(zs):.2f},{max(zs):.2f}] "
            f"front_min={min_front:.2f} age={age:.2f}s"
        )

    def _costmap_array(self):
        if self.costmap is None:
            return None
        return np.array(self.costmap.data, dtype=np.int16).reshape(
            (self.costmap.info.height, self.costmap.info.width)
        )

    def _costmap_summary(self) -> str:
        arr = self._costmap_array()
        if arr is None:
            return "costmap: no_msg"
        return (
            f"costmap: pos={int((arr > 0).sum())} "
            f"lethal={int((arr >= 99).sum())} max={int(arr.max())}"
        )

    def _path_cost_summary(self, name: str, path: Path) -> str:
        arr = self._costmap_array()
        cm = self.costmap
        if arr is None or cm is None:
            return f"{name}: no_costmap"
        if not path.poses:
            return f"{name}: empty_path"

        try:
            rot, trans = self._transform_matrix(cm.header.frame_id, path.header.frame_id)
        except Exception as exc:
            return f"{name}: tf_error={exc}"

        vals = []
        for pose in path.poses[:: max(1, len(path.poses) // 60)]:
            p = pose.pose.position
            xyz = rot.dot(np.array([p.x, p.y, p.z], dtype=float)) + trans
            mx = int((xyz[0] - cm.info.origin.position.x) / cm.info.resolution)
            my = int((xyz[1] - cm.info.origin.position.y) / cm.info.resolution)
            if 0 <= mx < cm.info.width and 0 <= my < cm.info.height:
                vals.append(int(arr[my, mx]))
            else:
                vals.append(255)

        if not vals:
            return f"{name}: path_outside"
        return (
            f"{name}: pts={len(path.poses)} max_cost={max(vals)} "
            f"lethal_samples={sum(v >= 99 for v in vals)}"
        )

    def _status_summary(self, topic: str) -> str:
        msg = self.status.get(topic)
        if msg is None or not msg.status_list:
            return f"{topic.rsplit('/', 2)[-2]}: idle"
        item = msg.status_list[-1]
        return f"{topic.rsplit('/', 2)[-2]}: status={item.status} {item.text}"

    def _cmd_summary(self) -> str:
        if self.cmd_vel is None:
            return "cmd_vel: no_msg"
        return f"cmd_vel: vx={self.cmd_vel.linear.x:.3f} wz={self.cmd_vel.angular.z:.3f}"

    def tick(self) -> None:
        now = time.time()
        if now - self.last_print < 0.5:
            return
        self.last_print = now

        path_parts = []
        for name in ("Connect", "Standard", "Heavy", "Eco"):
            path = self.paths.get(name)
            if path is not None:
                path_parts.append(self._path_cost_summary(name, path))

        parts = [
            time.strftime("%F %T"),
            self._status_summary("/move_base_flex/move_base/status"),
            self._status_summary("/move_base_flex/exe_path/status"),
            self._cmd_summary(),
            self._cloud_summary("/left/obstacle_2d"),
            self._cloud_summary("/right/obstacle_2d"),
            self._cloud_summary("/up/obstacle_2d"),
            self._costmap_summary(),
        ] + path_parts
        print(" | ".join(parts), flush=True)


def main() -> None:
    rospy.init_node("depth_nav_watch", anonymous=True)
    watch = DepthNavWatch()
    rate = rospy.Rate(20)
    while not rospy.is_shutdown():
        watch.tick()
        rate.sleep()


if __name__ == "__main__":
    main()

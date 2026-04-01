#!/usr/bin/env python3
import os
import time
import yaml
import rospy
import numpy as np

from nav_msgs.msg import OccupancyGrid, MapMetaData
from geometry_msgs.msg import PointStamped
from visualization_msgs.msg import Marker
from std_srvs.srv import Trigger, TriggerResponse

import tf2_ros
from tf2_geometry_msgs.tf2_geometry_msgs import do_transform_point
from coverage_planner.map_io import read_pgm, write_occupancy_to_yaml_pgm, yaml_pgm_to_occupancy


class RectMapEditor:
    def __init__(self):
        rospy.init_node("rect_map_editor", anonymous=False)

        self.map_topic = rospy.get_param("~map_topic", "/map")
        self.out_map_topic = rospy.get_param("~out_map_topic", "/map_edited")
        self.clicked_topic = rospy.get_param("~clicked_topic", "/clicked_point")
        self.map_frame = rospy.get_param("~map_frame", "map")

        # 两种模式：订阅 /map 或直接加载 yaml
        self.input_yaml = rospy.get_param("~input_yaml", "")
        self.input_yaml = os.path.expanduser(self.input_yaml) if self.input_yaml else ""

        # 清理后写入的值：0=free, 100=occupied, -1=unknown
        self.fill_value = int(rospy.get_param("~fill_value", 0))

        # 保存路径：默认 ~/ 
        self.save_dir = os.path.expanduser(rospy.get_param("~save_dir", "~"))
        self.save_basename = rospy.get_param("~save_basename", "cleaned_map")
        os.makedirs(self.save_dir, exist_ok=True)

        # TF
        self.tf_buffer = tf2_ros.Buffer(cache_time=rospy.Duration(10.0))
        self.tf_listener = tf2_ros.TransformListener(self.tf_buffer)

        self.map_msg = None
        self.first_pt = None

        self.map_pub = rospy.Publisher(self.out_map_topic, OccupancyGrid, queue_size=1, latch=True)
        self.marker_pub = rospy.Publisher("~rect_marker", Marker, queue_size=1, latch=True)

        rospy.Subscriber(self.clicked_topic, PointStamped, self.on_click, queue_size=10)
        rospy.Service("/save_edited_map", Trigger, self.on_save)

        # 获取地图
        if self.input_yaml:
            rospy.loginfo("Loading map from yaml: %s", self.input_yaml)
            self.map_msg = yaml_pgm_to_occupancy(self.input_yaml)
            rospy.loginfo("Loaded map: %dx%d, res=%.3f, origin=(%.3f, %.3f)",
                          self.map_msg.info.width, self.map_msg.info.height,
                          self.map_msg.info.resolution,
                          self.map_msg.info.origin.position.x,
                          self.map_msg.info.origin.position.y)
        else:
            rospy.loginfo("Waiting for map on %s ...", self.map_topic)
            self.map_msg = rospy.wait_for_message(self.map_topic, OccupancyGrid)
            rospy.loginfo("Got map: %dx%d, res=%.3f, origin=(%.3f, %.3f)",
                          self.map_msg.info.width, self.map_msg.info.height,
                          self.map_msg.info.resolution,
                          self.map_msg.info.origin.position.x,
                          self.map_msg.info.origin.position.y)

        # 发布一份到 /map_edited，方便 RViz 直接看
        self.publish_map()

        rospy.loginfo("Ready. In RViz use 'Publish Point' tool, click two corners to clear a rectangle.")
        rospy.loginfo("Edited map published on %s. Save via: rosservice call /save_edited_map", self.out_map_topic)

    def publish_map(self):
        if self.map_msg is None:
            return
        self.map_msg.header.stamp = rospy.Time.now()
        self.map_msg.header.frame_id = self.map_frame
        self.map_pub.publish(self.map_msg)

    def world_to_grid(self, x, y):
        info = self.map_msg.info
        res = info.resolution
        ox = info.origin.position.x
        oy = info.origin.position.y
        mx = int((x - ox) / res)
        my = int((y - oy) / res)
        return mx, my

    def clamp(self, v, lo, hi):
        return max(lo, min(hi, v))

    def clear_rect_in_map(self, x0, y0, x1, y1):
        info = self.map_msg.info
        w, h = info.width, info.height

        mx0, my0 = self.world_to_grid(min(x0, x1), min(y0, y1))
        mx1, my1 = self.world_to_grid(max(x0, x1), max(y0, y1))

        mx0 = self.clamp(mx0, 0, w - 1)
        mx1 = self.clamp(mx1, 0, w - 1)
        my0 = self.clamp(my0, 0, h - 1)
        my1 = self.clamp(my1, 0, h - 1)

        data = np.array(self.map_msg.data, dtype=np.int16).reshape((h, w))
        data[my0:my1 + 1, mx0:mx1 + 1] = self.fill_value
        self.map_msg.data = data.reshape(-1).astype(int).tolist()

        rospy.loginfo("Cleared rect grid: x[%d..%d], y[%d..%d] -> value=%d", mx0, mx1, my0, my1, self.fill_value)

    def publish_rect_marker(self, x0, y0, x1, y1):
        mk = Marker()
        mk.header.frame_id = self.map_frame
        mk.header.stamp = rospy.Time.now()
        mk.ns = "rect_map_editor"
        mk.id = 0
        mk.type = Marker.LINE_STRIP
        mk.action = Marker.ADD
        mk.scale.x = 0.05
        mk.color.r = 1.0
        mk.color.g = 0.3
        mk.color.b = 0.3
        mk.color.a = 1.0
        mk.pose.orientation.w = 1.0

        from geometry_msgs.msg import Point
        p = []
        p.append(Point(x=min(x0, x1), y=min(y0, y1), z=0.05))
        p.append(Point(x=max(x0, x1), y=min(y0, y1), z=0.05))
        p.append(Point(x=max(x0, x1), y=max(y0, y1), z=0.05))
        p.append(Point(x=min(x0, x1), y=max(y0, y1), z=0.05))
        p.append(Point(x=min(x0, x1), y=min(y0, y1), z=0.05))
        mk.points = p

        self.marker_pub.publish(mk)

    def transform_to_map(self, pt_msg: PointStamped) -> PointStamped:
        if pt_msg.header.frame_id == "" or pt_msg.header.frame_id == self.map_frame:
            return pt_msg

        try:
            tf = self.tf_buffer.lookup_transform(self.map_frame,
                                                 pt_msg.header.frame_id,
                                                 pt_msg.header.stamp,
                                                 rospy.Duration(0.2))
            out = PointStamped()
            out.header.frame_id = self.map_frame
            out.header.stamp = pt_msg.header.stamp
            out.point = do_transform_point(pt_msg, tf).point
            return out
        except Exception as e:
            rospy.logwarn("TF transform failed (%s -> %s): %s. Assume already in map frame.",
                          pt_msg.header.frame_id, self.map_frame, str(e))
            return pt_msg

    def on_click(self, msg: PointStamped):
        if self.map_msg is None:
            return

        msg_map = self.transform_to_map(msg)
        x, y = msg_map.point.x, msg_map.point.y

        if self.first_pt is None:
            self.first_pt = (x, y)
            rospy.loginfo("First corner set: (%.3f, %.3f). Click second corner.", x, y)
            return

        x0, y0 = self.first_pt
        x1, y1 = x, y
        self.first_pt = None

        self.clear_rect_in_map(x0, y0, x1, y1)
        self.publish_rect_marker(x0, y0, x1, y1)
        self.publish_map()

    def write_pgm_and_yaml(self, occ: OccupancyGrid, out_prefix: str):
        out_dir = os.path.dirname(out_prefix)
        base_name = os.path.basename(out_prefix)
        return write_occupancy_to_yaml_pgm(occ, out_dir, base_name=base_name)

    def on_save(self, _req):
        if self.map_msg is None:
            return TriggerResponse(success=False, message="No map loaded.")

        ts = time.strftime("%Y%m%d_%H%M%S", time.localtime())
        out_prefix = os.path.join(self.save_dir, f"{self.save_basename}_{ts}")
        pgm_path, yaml_path = self.write_pgm_and_yaml(self.map_msg, out_prefix)

        msg = f"Saved:\n  {yaml_path}\n  {pgm_path}"
        rospy.loginfo(msg)
        return TriggerResponse(success=True, message=msg)


if __name__ == "__main__":
    RectMapEditor()
    rospy.spin()

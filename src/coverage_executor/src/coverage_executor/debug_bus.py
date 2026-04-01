# coverage_executor/src/coverage_executor/debug_bus.py
# -*- coding: utf-8 -*-

import rospy
from std_msgs.msg import String
from geometry_msgs.msg import PoseStamped
from nav_msgs.msg import Path
from typing import Optional, List, Tuple, Any

from .ros_utils import make_pose, make_path


class DebugBus:
    """
    Debug 输出总线（独立模块）：
    - 全部 topic 都在 node 的私有命名空间下：~debug/*
      所以 node 叫 coverage_executor 时，会变成：
        /coverage_executor/debug/path
        /coverage_executor/debug/cut_path
        /coverage_executor/debug/robot_pose
        ...
    """

    def __init__(self):
        self.enable: bool = bool(rospy.get_param("~debug_enable", True))
        self.latch: bool = bool(rospy.get_param("~debug_latch", True))

        # 你可以用 ~debug_publish_pose 来关掉实时 pose 输出（减少 RViz/网络压力）
        self.publish_pose: bool = bool(rospy.get_param("~debug_publish_pose", True))

        if not self.enable:
            self._path_pub = None
            self._cut_path_pub = None
            self._robot_pose_pub = None
            self._proj_pose_pub = None
            self._cut_info_pub = None
            self._plan_info_pub = None
            return

        # 注意：topic 必须用 "~debug/xxx" 才能落在 /coverage_executor/debug/xxx
        self._path_pub = rospy.Publisher("~debug/path", Path, queue_size=1, latch=self.latch)
        self._cut_path_pub = rospy.Publisher("~debug/cut_path", Path, queue_size=1, latch=self.latch)
        self._robot_pose_pub = rospy.Publisher("~debug/robot_pose", PoseStamped, queue_size=10)
        self._proj_pose_pub = rospy.Publisher("~debug/proj_pose", PoseStamped, queue_size=10)
        self._cut_info_pub = rospy.Publisher("~debug/cut_info", String, queue_size=1, latch=self.latch)
        self._plan_info_pub = rospy.Publisher("~debug/plan_info", String, queue_size=1, latch=self.latch)

    def publish_plan_info(self, text: str):
        if not self.enable or self._plan_info_pub is None:
            return
        self._plan_info_pub.publish(String(data=str(text)))

    def publish_paths(
        self,
        frame_id: str,
        full_xyyaw: Optional[List[Tuple[float, float, float]]],
        cut_xyyaw: Optional[List[Tuple[float, float, float]]],
        cut_idx: int = 0,
        reason: str = "",
        extra: str = "",
    ):
        """
        发布完整路径 + 截断路径（用于 RViz 对比）
        """
        if not self.enable:
            return

        try:
            if full_xyyaw is not None and self._path_pub is not None:
                self._path_pub.publish(make_path(frame_id, full_xyyaw))
            if cut_xyyaw is not None and self._cut_path_pub is not None:
                self._cut_path_pub.publish(make_path(frame_id, cut_xyyaw))

            msg = f"cut_idx={int(cut_idx)}"
            if full_xyyaw is not None:
                msg += f" full_pts={len(full_xyyaw)}"
            if cut_xyyaw is not None:
                msg += f" cut_pts={len(cut_xyyaw)}"
            if reason:
                msg += f" reason={reason}"
            if extra:
                msg += f" {extra}"
            if self._cut_info_pub is not None:
                self._cut_info_pub.publish(String(data=msg))
        except Exception as e:
            rospy.logwarn("[DBG] publish_paths failed: %s", str(e))

    def publish_robot_pose(self, frame_id: str, x: float, y: float, yaw: float = 0.0):
        if (not self.enable) or (not self.publish_pose) or self._robot_pose_pub is None:
            return
        try:
            self._robot_pose_pub.publish(make_pose(frame_id, x, y, yaw))
        except Exception:
            pass

    def publish_proj_pose(self, frame_id: str, x: float, y: float, yaw: float = 0.0):
        if (not self.enable) or (not self.publish_pose) or self._proj_pose_pub is None:
            return
        try:
            self._proj_pose_pub.publish(make_pose(frame_id, x, y, yaw))
        except Exception:
            pass

    def clear_paths(self, frame_id: str = "map"):
        """清空 RViz 显示（发布空 Path）"""
        if not self.enable:
            return
        try:
            if self._path_pub is not None:
                self._path_pub.publish(Path(header=rospy.Header(frame_id=frame_id, stamp=rospy.Time.now())))
            if self._cut_path_pub is not None:
                self._cut_path_pub.publish(Path(header=rospy.Header(frame_id=frame_id, stamp=rospy.Time.now())))
            if self._cut_info_pub is not None:
                self._cut_info_pub.publish(String(data="cleared"))
        except Exception:
            pass

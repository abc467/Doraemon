#!/usr/bin/env python
import rospy
from sensor_msgs.msg import JointState
import math

rospy.init_node('wheel_spinner')
pub = rospy.Publisher('/joint_states', JointState, queue_size=10)

rate = rospy.Rate(30)  # 30Hz
angle = 0.0
while not rospy.is_shutdown():
    msg = JointState()
    msg.header.stamp = rospy.Time.now()
    msg.header.frame_id = 'base_link'
    
    # 同时控制左右轮子（方向相反）
    msg.name = ['left_wheel', 'right_wheel_joint']
    # msg.name = ['left_wheel', 'right_wheel']
    msg.position = [angle, angle]  # 左轮正转，右轮反转
    
    pub.publish(msg)

    angle += 0.1  # 旋转速度（弧度/帧）
    if angle > 2 * math.pi:  # 超过360度后归零
        angle = 0.0
    rate.sleep()
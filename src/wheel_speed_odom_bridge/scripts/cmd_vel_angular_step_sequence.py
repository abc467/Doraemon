#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse

import rospy
from geometry_msgs.msg import Twist


def build_twist(linear_x=0.0, angular_z=0.0):
    msg = Twist()
    msg.linear.x = linear_x
    msg.angular.z = angular_z
    return msg


def parse_args():
    parser = argparse.ArgumentParser(
        description="Publish a fixed angular-z /cmd_vel step sequence: 0.2, 0.4, 0.6 rad/s, each for 10s."
    )
    parser.add_argument("--topic", default="/cmd_vel", help="Target cmd_vel topic.")
    parser.add_argument(
        "--speeds",
        default="0.2,0.4,0.6",
        help="Comma-separated angular speeds in rad/s. Default: 0.2,0.4,0.6",
    )
    parser.add_argument(
        "--segment-duration",
        type=float,
        default=10.0,
        help="Duration of each speed segment in seconds. Default: 10.0",
    )
    parser.add_argument(
        "--stop-duration",
        type=float,
        default=1.0,
        help="How long to publish zero speed before exit. Default: 1.0",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=20.0,
        help="Publish rate in Hz. Default: 20.0",
    )
    args = parser.parse_args(rospy.myargv()[1:])
    speeds = []
    for item in args.speeds.split(","):
        item = item.strip()
        if not item:
            continue
        speeds.append(float(item))
    if not speeds:
        raise ValueError("At least one speed is required.")
    if args.segment_duration <= 0.0:
        raise ValueError("--segment-duration must be > 0.")
    if args.stop_duration < 0.0:
        raise ValueError("--stop-duration must be >= 0.")
    if args.rate <= 0.0:
        raise ValueError("--rate must be > 0.")
    args.speeds = speeds
    return args


def publish_for_duration(pub, rate, angular_z, duration):
    end_time = rospy.Time.now() + rospy.Duration.from_sec(duration)
    msg = build_twist(angular_z=angular_z)
    while not rospy.is_shutdown() and rospy.Time.now() < end_time:
        pub.publish(msg)
        rate.sleep()


def publish_stop(pub, repeat_count=5):
    msg = build_twist(0.0)
    for _ in range(repeat_count):
        if rospy.is_shutdown():
            break
        pub.publish(msg)
        rospy.sleep(0.05)


def main():
    rospy.init_node("cmd_vel_angular_step_sequence_sender", anonymous=True)
    args = parse_args()

    pub = rospy.Publisher(args.topic, Twist, queue_size=10)
    rospy.on_shutdown(lambda: publish_stop(pub))
    rate = rospy.Rate(args.rate)
    rospy.sleep(0.5)

    rospy.loginfo(
        "cmd_vel angular step sequence start: topic=%s angular_speeds=%s segment_duration=%.3fs stop_duration=%.3fs",
        args.topic,
        args.speeds,
        args.segment_duration,
        args.stop_duration,
    )

    for speed in args.speeds:
        rospy.loginfo("Publishing angular.z=%.3f rad/s for %.3f s", speed, args.segment_duration)
        publish_for_duration(pub, rate, speed, args.segment_duration)

    rospy.loginfo("Publishing zero cmd_vel for %.3f s", args.stop_duration)
    publish_for_duration(pub, rate, 0.0, args.stop_duration)
    publish_stop(pub)
    rospy.loginfo("cmd_vel angular step sequence finished.")


if __name__ == "__main__":
    main()

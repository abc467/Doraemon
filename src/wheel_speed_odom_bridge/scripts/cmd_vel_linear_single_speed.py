#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse

import rospy
from geometry_msgs.msg import Twist


def build_twist(linear_x, angular_z=0.0):
    msg = Twist()
    msg.linear.x = linear_x
    msg.angular.z = angular_z
    return msg


def parse_args():
    parser = argparse.ArgumentParser(
        description="Publish a single linear speed to /cmd_vel for a fixed duration, then stop."
    )
    parser.add_argument("--topic", default="/cmd_vel", help="Target cmd_vel topic.")
    parser.add_argument(
        "--speed",
        type=float,
        default=0.2,
        help="Linear speed in m/s. Default: 0.2",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=10.0,
        help="How long to publish the speed in seconds. Default: 10.0",
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
    if args.duration <= 0.0:
        raise ValueError("--duration must be > 0.")
    if args.stop_duration < 0.0:
        raise ValueError("--stop-duration must be >= 0.")
    if args.rate <= 0.0:
        raise ValueError("--rate must be > 0.")
    return args


def publish_for_duration(pub, rate, linear_x, duration):
    end_time = rospy.Time.now() + rospy.Duration.from_sec(duration)
    msg = build_twist(linear_x)
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
    rospy.init_node("cmd_vel_linear_single_speed_sender", anonymous=True)
    args = parse_args()

    pub = rospy.Publisher(args.topic, Twist, queue_size=10)
    rospy.on_shutdown(lambda: publish_stop(pub))
    rate = rospy.Rate(args.rate)
    rospy.sleep(0.5)

    rospy.loginfo(
        "cmd_vel linear single speed start: topic=%s speed=%.3f m/s duration=%.3fs stop_duration=%.3fs",
        args.topic,
        args.speed,
        args.duration,
        args.stop_duration,
    )

    publish_for_duration(pub, rate, args.speed, args.duration)
    rospy.loginfo("Publishing zero cmd_vel for %.3f s", args.stop_duration)
    publish_for_duration(pub, rate, 0.0, args.stop_duration)
    publish_stop(pub)
    rospy.loginfo("cmd_vel linear single speed finished.")


if __name__ == "__main__":
    main()

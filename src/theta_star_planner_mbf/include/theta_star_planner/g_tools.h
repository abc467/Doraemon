#pragma once

#include <ros/ros.h>
#include <geometry_msgs/PoseStamped.h>
#include <tf/transform_datatypes.h>
#include <tf2_ros/transform_listener.h>
#include <tf2_geometry_msgs/tf2_geometry_msgs.h>
#include <tf2/utils.h>

/* 工具函数
*/
namespace g_tools
{

    // 计算从A指向B的四元数
    geometry_msgs::Quaternion getQuaternionFromTwoPoints(const geometry_msgs::Point& pointA, const geometry_msgs::Point& pointB) {
        tf2::Vector3 dir_vec(pointB.x - pointA.x, pointB.y - pointA.y, 0);
        dir_vec.normalize();

        tf2::Quaternion quat;
        quat.setRPY(0, 0, atan2(dir_vec.y(), dir_vec.x()));

        geometry_msgs::Quaternion result;
        tf2::convert(quat, result);
        return result;
    }

    // 将角度限制在 -pi 到 pi 的范围内
    double normalizeAngle(double angle) {
        // 将角度转换为 [-2*pi, 2*pi] 区间
        angle = std::fmod(angle, 2 * M_PI);
        
        // 如果角度大于 pi，则减去 2*pi
        if (angle > M_PI) {
            angle -= 2 * M_PI;
        }
        // 如果角度小于 -pi，则加上 2*pi
        else if (angle < -M_PI) {
            angle += 2 * M_PI;
        }
        return angle;
    }

    // 判断点p相对于向量p1的位置：-1为左侧，1为右侧，0为共线
    int pointRelativeToPose(const geometry_msgs::Pose& p1, const geometry_msgs::Point& p) {
        // 1. 获取p1的朝向方向，转换为三维向量（假设我们感兴趣的是xy平面）
        tf::Quaternion q(p1.orientation.x, p1.orientation.y, p1.orientation.z, p1.orientation.w);
        tf::Matrix3x3 m(q);
        double roll, pitch, yaw;
        m.getRPY(roll, pitch, yaw);  // 获取roll, pitch, yaw欧拉角

        // 计算方向向量（假设向量指向yaw角的方向）
        geometry_msgs::Point p1_direction;
        p1_direction.x = cos(yaw);
        p1_direction.y = sin(yaw);

        // 2. 计算向量p1 -> p（从p1位置到p的向量）
        geometry_msgs::Point p1_to_p;
        p1_to_p.x = p.x - p1.position.x;
        p1_to_p.y = p.y - p1.position.y;

        // 3. 计算叉积判断点p相对于向量p1的方向
        double cross_product = p1_direction.x * p1_to_p.y - p1_direction.y * p1_to_p.x;

        if (cross_product > 0) {
            return -1;  // 点p在向量p1的左侧
        } else if (cross_product < 0) {
            return 1; // 点p在向量p1的右侧
        } else {
            return 0;  // 点p与向量p1共线
        }
    }

    // 计算两个Pose之间角度的绝对差值
    double calculateAngleDifference(const geometry_msgs::Pose& pose1, const geometry_msgs::Pose& pose2) {
        // 将四元数转换为tf2::Quaternion类型以便使用tf2相关函数
        tf2::Quaternion q1;
        tf2::fromMsg(pose1.orientation, q1);

        tf2::Quaternion q2;
        tf2::fromMsg(pose2.orientation, q2);

        // 获取两个点的偏航角
        double yaw1 = tf2::getYaw(q1);
        double yaw2 = tf2::getYaw(q2);

        // 计算角度差值
        double yaw_diff = std::fmod(std::fabs(yaw1 - yaw2) + M_PI, 2 * M_PI) - M_PI;
        if (yaw_diff < 0) {
            yaw_diff = -yaw_diff;
        }
        return yaw_diff;
    }

    // 计算曲率值
    double cal_K(const std::vector<geometry_msgs::PoseStamped>& orig_global_plan, int index){
        double res;
        //差分法求一阶导和二阶导
        double dx, dy, ddx, ddy;
        if (index == 0) {
            dx = orig_global_plan.at(1).pose.position.x -orig_global_plan.at(0).pose.position.x;
            dy = orig_global_plan.at(1).pose.position.y - orig_global_plan.at(0).pose.position.y;
            ddx = orig_global_plan.at(2).pose.position.x + orig_global_plan.at(0).pose.position.x - 2 * orig_global_plan.at(1).pose.position.x;
            ddy = orig_global_plan.at(2).pose.position.y + orig_global_plan.at(0).pose.position.y - 2 * orig_global_plan.at(1).pose.position.y;
        }
        else if (index == (orig_global_plan.size() - 1)) {
            dx = orig_global_plan.at(index).pose.position.x - orig_global_plan.at(index-1).pose.position.x;
            dy = orig_global_plan.at(index).pose.position.y - orig_global_plan.at(index-1).pose.position.y;
            ddx = orig_global_plan.at(index).pose.position.x + orig_global_plan.at(index-2).pose.position.x - 2 * orig_global_plan.at(index-1).pose.position.x;
            ddy = orig_global_plan.at(index).pose.position.y + orig_global_plan.at(index-2).pose.position.y - 2 * orig_global_plan.at(index-1).pose.position.y;
        }
        else {
            dx = orig_global_plan.at(index+1).pose.position.x - orig_global_plan.at(index).pose.position.x;
            dy = orig_global_plan.at(index+1).pose.position.y - orig_global_plan.at(index).pose.position.y;
            ddx = orig_global_plan.at(index+1).pose.position.x + orig_global_plan.at(index-1).pose.position.x - 2 * orig_global_plan.at(index).pose.position.x;
            ddy = orig_global_plan.at(index+1).pose.position.y + orig_global_plan.at(index-1).pose.position.y - 2 * orig_global_plan.at(index).pose.position.y;
        }
        res = (ddy * dx - ddx * dy) / (sqrt(pow((pow(dx, 2) + pow(dy, 2)), 3)));
        return res;
    }

    // 判断一个点是否在给定姿态（pose）的前方
    bool isPointInFrontOfPose(const geometry_msgs::Pose& pose, const geometry_msgs::Pose& point) {
        // 计算从姿态（pose）位置指向点（point）的向量在x轴方向上的分量
        double dx = point.position.x - pose.position.x;
        // 计算从姿态（pose）位置指向点（point）的向量在y轴方向上的分量
        double dy = point.position.y - pose.position.y;

        // 获取姿态（pose）的横摆角（yaw），即姿态所表示的方向角度
        double pose_yaw = tf2::getYaw(pose.orientation);

        // 计算从姿态（pose）指向点（point）的向量所对应的角度
        double point_yaw = std::atan2(dy, dx);

        // 将姿态（pose）的横摆角归一化到[-π, π]区间内，确保角度表示的一致性和准确性
        pose_yaw = std::atan2(std::sin(pose_yaw), std::cos(pose_yaw));
        // 将从姿态（pose）指向点（point）的向量角度也归一化到[-π, π]区间内
        point_yaw = std::atan2(std::sin(point_yaw), std::cos(point_yaw));

        // 计算点（point）对应的角度与姿态（pose）横摆角之间的差值，这个差值反映了两者方向上的相对关系
        double angle_diff = point_yaw - pose_yaw;

        // 再次将角度差值归一化到[-π, π]区间内，进一步规范角度差的表示范围
        angle_diff = std::atan2(std::sin(angle_diff), std::cos(angle_diff));

        // 检查角度差值是否在[-π/2, π/2]区间内，
        // 如果角度差值在这个区间内，意味着点（point）相对于姿态（pose）来说是在前方；
        // 如果角度差值小于 -π/2或者大于 π/2，则表示点（point）在姿态（pose）的后方
        return (angle_diff > -M_PI_2 && angle_diff < M_PI_2);
    }
    

    // 计算两点间的距离
    double distanceBetweenPoses(const geometry_msgs::Pose& pose1, const geometry_msgs::Pose& pose2) {
        double dx = pose1.position.x - pose2.position.x;
        double dy = pose1.position.y - pose2.position.y;

        double squared_distance = dx * dx + dy * dy;

        return std::sqrt(squared_distance);
    }
    
} // namespace tools


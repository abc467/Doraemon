#pragma once

#include <ros/ros.h>
#include <fstream>
#include <nav_msgs/Path.h>
#include <deque>
#include "g_tools.h"


/*路径处理工具函数*/
namespace path_tools
{
    // 滑动窗口滤波器(从尾部开始)
    template <typename T>
    std::vector<T> movingAverageFilter(const std::vector<T>& data, int windowSize)
    {
        int dataSize = data.size();
        if(windowSize <= 0){
            std::cerr << "windowSize must be greater than 0" << std::endl;
            return data;
        }else if(windowSize >= dataSize) {
            std::cerr << "windowSize must be less than dataSize" << std::endl;
            return data;
        }
        
        std::vector<T> filteredData(dataSize);
        std::deque<T> window; // 滑动窗口
        T sum = 0.0;

        for(int i = dataSize - 1; i  >= 0; --i){
            if(window.size() == windowSize){
                sum -= window.front();
                window.pop_front();
            }
            sum += data[i];
            window.push_back(data[i]);
            filteredData[i] = sum / window.size();
        }
        return filteredData;
    }

    /**
     * 找到路径上与给定点最接近的点
     * Find element in iterator with the minimum calculated value
     */
    template<typename Iter, typename Getter>
    inline Iter min_by(Iter begin, Iter end, Getter getCompareVal)
    {
        if (begin == end) {
            return end;
        }
        auto lowest = getCompareVal(*begin);
        Iter lowest_it = begin;
        for (Iter it = ++begin; it != end; ++it) {
            auto comp = getCompareVal(*it);
            if (comp <= lowest) {
            lowest = comp;
            lowest_it = it;
            }
        }
        return lowest_it;
    }


    /**
     * @brief 计算两点的距离 Get the L2 distance between 2 geometry_msgs::Poses
     * @param pos1 First pose
     * @param pos1 Second pose
     * @param is_3d True if a true L2 distance is desired (default false)
     * @return double euclidean distance
     */
    inline double euclidean_distance(
        const geometry_msgs::Pose & pos1,
        const geometry_msgs::Pose & pos2,
        const bool is_3d = false)
        {
        double dx = pos1.position.x - pos2.position.x;
        double dy = pos1.position.y - pos2.position.y;

        if (is_3d) {
            double dz = pos1.position.z - pos2.position.z;
            return std::hypot(dx, dy, dz);
        }

        return std::hypot(dx, dy);
    }

    inline double euclidean_distance(
        const geometry_msgs::PoseStamped & pos1,
        const geometry_msgs::PoseStamped & pos2,
        const bool is_3d = false)
        {
        double dx = pos1.pose.position.x - pos2.pose.position.x;
        double dy = pos1.pose.position.y - pos2.pose.position.y;

        if (is_3d) {
            double dz = pos1.pose.position.z - pos2.pose.position.z;
            return std::hypot(dx, dy, dz);
        }

        return std::hypot(dx, dy);
    }

    /**
     * Find first element in iterator that is greater integrated distance than comparevalue
     * 找到路径上的点，使得 累积距离 大于 comparevalue
     */
    template<typename Iter, typename Getter>
    inline Iter first_after_integrated_distance(Iter begin, Iter end, Getter getCompareVal)
    {
        if (begin == end) {
            return end;
        }
        Getter dist = 0.0;
        for (Iter it = begin; it != end - 1; it++) {
            dist += euclidean_distance(*it, *(it + 1));
            if (dist > getCompareVal) {
            return it + 1;
            }
        }
        return end;
    }

    // 对路径点进行线性插值
    std::vector<geometry_msgs::PoseStamped> inline linearInterpolate(const std::vector<geometry_msgs::PoseStamped>& input_path, double interval, double angle_interval)
    {
        std::vector<geometry_msgs::PoseStamped>  interpolated_path;
        interpolated_path.push_back(input_path.front()); // 将第一个点直接加入

        for (size_t i = 1; i < input_path.size(); ++i)
        {
            geometry_msgs::PoseStamped prev_pose = input_path[i - 1];
            geometry_msgs::PoseStamped current_pose = input_path[i];

            // 计算当前点与上一个点之间的距离
            double dist = hypot(current_pose.pose.position.x - prev_pose.pose.position.x,
                                current_pose.pose.position.y - prev_pose.pose.position.y);

            // 如果距离超过间隔，则进行插值
            if (dist > interval)
            {
                // 计算需要插入的点数量
                int num_points = std::ceil(dist / interval);
                // //计算夹角
                // double angle = atan2(current_pose.pose.position.y - prev_pose.pose.position.y, 
                //         current_pose.pose.position.x - prev_pose.pose.position.x);
                // 执行线性插值
                for (int j = 1; j < num_points; ++j)
                {
                    double ratio = static_cast<double>(j) / num_points;
                    geometry_msgs::PoseStamped new_pose;
                    new_pose.header.frame_id = "map";
                    new_pose.pose.position.x = prev_pose.pose.position.x +
                                            ratio * (current_pose.pose.position.x - prev_pose.pose.position.x);
                    new_pose.pose.position.y = prev_pose.pose.position.y +
                                            ratio * (current_pose.pose.position.y - prev_pose.pose.position.y);
                    new_pose.pose.position.z = prev_pose.pose.position.z +
                                            ratio * (current_pose.pose.position.z - prev_pose.pose.position.z);
                    new_pose.pose.orientation = prev_pose.pose.orientation; // 假设朝向不变

                    
                    // new_pose.pose.orientation.w = cos(angle/2);
                    // new_pose.pose.orientation.z = sin(angle/2);
                    interpolated_path.push_back(new_pose);
                }
            }

            // 将当前点加入到插值后的路径中
            current_pose.header.frame_id = "map";
            interpolated_path.push_back(current_pose);
        }

        return interpolated_path;
    }

/**
 * @brief 在原始路径和目标点之间插值，生成新的路径
 * @param smoothed_path 原始路径
 * @param target_point 新增的目标点
 */
void addTargetPointAndInterpolate(
    std::vector<geometry_msgs::PoseStamped>& smoothed_path,
    const geometry_msgs::PoseStamped& target_point,
    double spacing = 0.05)
{
    // 获取原始路径的最后一个点
    const geometry_msgs::PoseStamped& last_point = smoothed_path.back();

    // 计算目标点与最后一个点之间的距离
    double dx = target_point.pose.position.x - last_point.pose.position.x;
    double dy = target_point.pose.position.y - last_point.pose.position.y;
    double distance = std::hypot(dx, dy);

    // 计算插值点的数量
    size_t num_interpolated_points = static_cast<size_t>(distance / spacing);

    // 在原始路径和目标点之间插值
    for (size_t i = 1; i <= num_interpolated_points; ++i) {
        double t = static_cast<double>(i) / (num_interpolated_points + 1);
        geometry_msgs::PoseStamped interpolated_point;
        interpolated_point.header.frame_id = "map";
        interpolated_point.pose.position.x = last_point.pose.position.x + t * dx;
        interpolated_point.pose.position.y = last_point.pose.position.y + t * dy;
        smoothed_path.push_back(interpolated_point);
    }

    // 添加目标点
    smoothed_path.push_back(target_point);
}

} // namespace sparse_path

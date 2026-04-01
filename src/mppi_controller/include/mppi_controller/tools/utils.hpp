#pragma once

#include <Eigen/Dense>

#include <vector>
#include <string>

#include <ros/ros.h>
#include <geometry_msgs/PoseStamped.h>
#include <nav_msgs/Path.h>
#include <geometry_msgs/TwistStamped.h>
#include <std_msgs/ColorRGBA.h>
#include <visualization_msgs/Marker.h>
#include <costmap_2d/costmap_2d_ros.h>

#include <tf2_ros/transform_listener.h>
#include <tf2_geometry_msgs/tf2_geometry_msgs.h>
#include <tf2/utils.h>

#include <angles/angles.h>

#include "mppi_controller/models/optimizer_settings.hpp"
#include "mppi_controller/models/control_sequence.hpp"
#include "mppi_controller/models/path.hpp"
#include "mppi_controller/critic_data.hpp"

namespace mppi::utils
{
/**
 * @brief Convert data into pose
 * @param x X position
 * @param y Y position
 * @param z Z position
 * @return Pose object
 */
inline geometry_msgs::Pose createPose(double x, double y, double z)
{
  geometry_msgs::Pose pose;
  pose.position.x = x;
  pose.position.y = y;
  pose.position.z = z;
  pose.orientation.w = 1;
  pose.orientation.x = 0;
  pose.orientation.y = 0;
  pose.orientation.z = 0;
  return pose;
}

/**
 * @brief Convert data into scale
 * @param x X scale
 * @param y Y scale
 * @param z Z scale
 * @return Scale object
 */
inline geometry_msgs::Vector3 createScale(double x, double y, double z)
{
  geometry_msgs::Vector3 scale;
  scale.x = x;
  scale.y = y;
  scale.z = z;
  return scale;
}

/**
 * @brief Convert data into color
 * @param r Red component
 * @param g Green component
 * @param b Blue component
 * @param a Alpha component (transparency)
 * @return Color object
 */
inline std_msgs::ColorRGBA createColor(float r, float g, float b, float a)
{
  std_msgs::ColorRGBA color;
  color.r = r;
  color.g = g;
  color.b = b;
  color.a = a;
  return color;
}

/**
 * @brief Convert data into a Maarker
 * @param id Marker ID
 * @param pose Marker pose
 * @param scale Marker scale
 * @param color Marker color
 * @param frame Reference frame to use
 * @return Visualization Marker
 */
inline visualization_msgs::Marker createMarker(
  int id, const geometry_msgs::Pose & pose, const geometry_msgs::Vector3 & scale,
  const std_msgs::ColorRGBA & color, const std::string & frame_id, const std::string & ns)
{
  using visualization_msgs::Marker;
  Marker marker;
  marker.header.frame_id = frame_id;
  marker.header.stamp = ros::Time(0, 0);
  marker.ns = ns;
  marker.id = id;
  marker.type = Marker::SPHERE;
  marker.action = Marker::ADD;

  marker.pose = pose;
  marker.scale = scale;
  marker.color = color;
  return marker;
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

/**
 * @brief 计算两点的距离 Get the L2 distance between 2 geometry_msgs::Poses
 * @param pos1 First pose
 * @param pos1 Second pose
 * @param is_3d True if a true L2 distance is desired (default false)
 * @return double euclidean distance
 */
inline double euclidean_distance(
    const geometry_msgs::PoseStamped & pos1,
    const geometry_msgs::PoseStamped & pos2,
    const bool is_3d = false)
{
  return euclidean_distance(pos1.pose, pos2.pose, is_3d);
}

/**
  * @brief 归一化角度到 -M_PI ~ +M_PI circle
  * It takes and returns radians.
  * @param angles Angles to normalize
  * @return normalized angles
  */
template<typename T>
auto normalize_angles(const T & angles)
{
  return (angles + M_PI).unaryExpr([&](const float x) {
             float remainder = std::fmod(x, 2.0f * M_PI);
             return remainder < 0.0f ? remainder + M_PI : remainder - M_PI;
             });
}

/**
  * @brief 计算最小角度差
  *
  * Given 2 angles, this returns the shortest angular
  * difference.  The inputs and outputs are of course radians.
  *
  * The result
  * would always be -pi <= result <= pi.  Adding the result
  * to "from" will always get you an equivalent angle to "to".
  * @param from Start angle
  * @param to End angle
  * @return Shortest distance between angles
  */
template<typename F, typename T>
auto shortest_angular_distance(
  const F & from,
  const T & to)
{
  return normalize_angles(to - from);
}

/**
 * @brief 检查机器人是否在目标附近
 * @param pose_tolerance Pose tolerance to use
 * @param robot Pose of robot
 * @param goal Goal pose
 * @return bool If robot is within tolerance to the goal
 */
inline bool withinPositionGoalTolerance(
  float pose_tolerance,
  const geometry_msgs::Pose & robot,
  const geometry_msgs::Pose & goal)
{
  const double & dist_sq =
    std::pow(goal.position.x - robot.position.x, 2) +
    std::pow(goal.position.y - robot.position.y, 2);

  const float pose_tolerance_sq = pose_tolerance * pose_tolerance;

  if (dist_sq < pose_tolerance_sq) {
    return true;
  }

  return false;
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
 * @brief 使用 Savisky-Golay 滤波器优化轨迹 (固定的二次函数平滑)
 * @param control_sequence Sequence to apply filter to
 * @param control_history Recent set of controls for edge-case handling
 * @param Settings Settings to use
 */
inline void savitskyGolayFilter(
  models::ControlSequence & control_sequence,
  std::array<mppi::models::Control, 4> & control_history,
  const models::OptimizerSettings & settings)
{
  // Savitzky-Golay Quadratic, 9-point Coefficients
  Eigen::Array<float, 9, 1> filter;
  filter << -21.0f, 14.0f, 39.0f, 54.0f, 59.0f, 54.0f, 39.0f, 14.0f, -21.0f;
  filter /= 231.0f;

  // 若窗口太窄，则平滑无意义
  const unsigned int num_sequences = control_sequence.vx.size() - 1;
  if (num_sequences < 20) {
    return;
  }

  auto applyFilter = [&](const Eigen::Array<float, 9, 1> & data) -> float {
      return (data * filter).eval().sum();
    };

  auto applyFilterOverAxis =
    [&](Eigen::ArrayXf & sequence, const Eigen::ArrayXf & initial_sequence,
    const float hist_0, const float hist_1, const float hist_2, const float hist_3) -> void
    {
      float pt_m4 = hist_0;
      float pt_m3 = hist_1;
      float pt_m2 = hist_2;
      float pt_m1 = hist_3;
      float pt = initial_sequence(0);
      float pt_p1 = initial_sequence(1);
      float pt_p2 = initial_sequence(2);
      float pt_p3 = initial_sequence(3);
      float pt_p4 = initial_sequence(4);

      for (unsigned int idx = 0; idx != num_sequences; idx++) {
        Eigen::Array<float, 9, 1> data;
        data << pt_m4, pt_m3, pt_m2, pt_m1, pt, pt_p1, pt_p2, pt_p3, pt_p4;
        sequence(idx) = applyFilter(data);
        pt_m4 = pt_m3;
        pt_m3 = pt_m2;
        pt_m2 = pt_m1;
        pt_m1 = pt;
        pt = pt_p1;
        pt_p1 = pt_p2;
        pt_p2 = pt_p3;
        pt_p3 = pt_p4;

        if (idx + 5 < num_sequences) {
          pt_p4 = initial_sequence(idx + 5);
        } else {
          // Return the last point
          pt_p4 = initial_sequence(num_sequences);
        }
      }
    };

  // 轨迹滤波
  const models::ControlSequence initial_control_sequence = control_sequence;
  applyFilterOverAxis(
    control_sequence.vx, initial_control_sequence.vx, control_history[0].vx,
    control_history[1].vx, control_history[2].vx, control_history[3].vx);
  applyFilterOverAxis(
    control_sequence.vy, initial_control_sequence.vy, control_history[0].vy,
    control_history[1].vy, control_history[2].vy, control_history[3].vy);
  applyFilterOverAxis(
    control_sequence.wz, initial_control_sequence.wz, control_history[0].wz,
    control_history[1].wz, control_history[2].wz, control_history[3].wz);

  // 更新控制历史
  unsigned int offset = settings.shift_control_sequence ? 1 : 0;
  control_history[0] = control_history[1];
  control_history[1] = control_history[2];
  control_history[2] = control_history[3];
  control_history[3] = {
    control_sequence.vx(offset),
    control_sequence.vy(offset),
    control_sequence.wz(offset)};
}

/**
 * @brief Convert path to a tensor
 * @param path Path to convert
 * @return Path tensor
 */
inline models::Path toTensor(const nav_msgs::Path & path)
{
  auto result = models::Path{};
  result.reset(path.poses.size());

  for (size_t i = 0; i < path.poses.size(); ++i) {
    result.x(i) = path.poses[i].pose.position.x;
    result.y(i) = path.poses[i].pose.position.y;
    result.yaws(i) = tf2::getYaw(path.poses[i].pose.orientation);
  }

  return result;
}

/**
 * @brief Shift the columns of a 2D Eigen Array or scalar values of
 *    1D Eigen Array by 1 place.
 * @param e Eigen Array
 * @param direction direction in which Array will be shifted.
 *     1 for shift in right direction and -1 for left direction.
 */
inline void shiftColumnsByOnePlace(Eigen::Ref<Eigen::ArrayXXf> e, int direction)
{
  int size = e.size();
  if(size == 1) {return;}
  if(abs(direction) != 1) {
    throw std::logic_error("Invalid direction, only 1 and -1 are valid values.");
  }

  if((e.cols() == 1 || e.rows() == 1) && size > 1) {
    auto start_ptr = direction == 1 ? e.data() + size - 2 : e.data() + 1;
    auto end_ptr = direction == 1 ? e.data() : e.data() + size - 1;
    while(start_ptr != end_ptr) {
      *(start_ptr + direction) = *start_ptr;
      start_ptr -= direction;
    }
    *(start_ptr + direction) = *start_ptr;
  } else {
    auto start_ptr = direction == 1 ? e.data() + size - 2 * e.rows() : e.data() + e.rows();
    auto end_ptr = direction == 1 ? e.data() : e.data() + size - e.rows();
    auto span = e.rows();
    while(start_ptr != end_ptr) {
      std::copy(start_ptr, start_ptr + span, start_ptr + direction * span);
      start_ptr -= (direction * span);
    }
    std::copy(start_ptr, start_ptr + span, start_ptr + direction * span);
  }
}

/**
 * @brief Convert data into TwistStamped
 * @param vx X velocity
 * @param wz Angular velocity
 * @param stamp Timestamp
 * @param frame Reference frame to use
 */
inline geometry_msgs::TwistStamped toTwistStamped(
  float vx, float wz, const ros::Time & stamp, const std::string & frame)
{
  geometry_msgs::TwistStamped twist;
  twist.header.frame_id = frame;
  twist.header.stamp = stamp;
  twist.twist.linear.x = vx;
  twist.twist.angular.z = wz;

  return twist;
}

/**
 * @brief Convert data into TwistStamped
 * @param vx X velocity
 * @param vy Y velocity
 * @param wz Angular velocity
 * @param stamp Timestamp
 * @param frame Reference frame to use
 */
inline geometry_msgs::TwistStamped toTwistStamped(
  float vx, float vy, float wz, const ros::Time & stamp,
  const std::string & frame)
{
  auto twist = toTwistStamped(vx, wz, stamp, frame);
  twist.twist.linear.y = vy;

  return twist;
}

/**
 * @brief 这个函数用于评估在给定的一组轨迹中，机器人能够到达的路径上的最远点索引对应的轨迹id
 * @param data Data to use
 * @return 一系列轨迹中能达到最远点的轨迹id
 */
inline size_t findPathFurthestReachedPoint(const CriticData & data)
{
  int traj_cols = data.trajectories.x.cols();
  // 提取所有候选轨迹的终点坐标
  const auto traj_x = data.trajectories.x.col(traj_cols - 1);
  const auto traj_y = data.trajectories.y.col(traj_cols - 1);

  // 路径点与轨迹终点的距离矩阵计算
  const auto dx = (data.path.x.transpose()).replicate(traj_x.rows(), 1).colwise() - traj_x;
  const auto dy = (data.path.y.transpose()).replicate(traj_y.rows(), 1).colwise() - traj_y;
  // 计算每个路径点与每个轨迹终点的欧氏距离平方 [batch_size, n_path_points]
  const auto dists = dx * dx + dy * dy;

  int max_id_by_trajectories = 0, min_id_by_path = 0;
  float min_distance_by_path = std::numeric_limits<float>::max();
  size_t n_rows = dists.rows();
  size_t n_cols = dists.cols();
  for (size_t i = 0; i != n_rows; i++) {
    min_id_by_path = 0;
    min_distance_by_path = std::numeric_limits<float>::max();
    for (size_t j = max_id_by_trajectories; j != n_cols; j++) { // 找到路径上距离轨迹终点的最近点id
      const float cur_dist = dists(i, j);
      if (cur_dist < min_distance_by_path) {
        min_distance_by_path = cur_dist;
        min_id_by_path = j;
      }
    }
    max_id_by_trajectories = std::max(max_id_by_trajectories, min_id_by_path); // 找到这些id中最大的id
  }
  return max_id_by_trajectories;
}

/**
 * @brief evaluate path furthest point if it is not set  
 * 评估在给定的一组轨迹中，机器人能够到达的路径上的最远点索引对应的轨迹id
 * @param data Data to use
 */
inline void setPathFurthestPointIfNotSet(CriticData & data)
{
  if (!data.furthest_reached_path_point) {
    data.furthest_reached_path_point = findPathFurthestReachedPoint(data);
  }
}

// A struct to hold pose data in floating point resolution
struct Pose2D
{
  float x, y, theta;
};

/**
 * @brief evaluate path costs 评估参考路径点是否有效
 * @param data Data to use
 */
inline void findPathCosts(
  CriticData & data,
  std::shared_ptr<costmap_2d::Costmap2DROS> costmap_ros)
{
  auto * costmap = costmap_ros->getCostmap();
  unsigned int map_x, map_y;
  const size_t path_segments_count = data.path.x.size() - 1;
  data.path_pts_valid = std::vector<bool>(path_segments_count, false);
  const bool tracking_unknown = costmap_ros->getLayeredCostmap()->isTrackingUnknown();
  for (unsigned int idx = 0; idx < path_segments_count; idx++) {
    if (!costmap->worldToMap(data.path.x(idx), data.path.y(idx), map_x, map_y)) {
      (*data.path_pts_valid)[idx] = false;
      continue;
    }

    switch (costmap->getCost(map_x, map_y)) {
      case (costmap_2d::LETHAL_OBSTACLE):
        (*data.path_pts_valid)[idx] = false;
        continue;
      case (costmap_2d::INSCRIBED_INFLATED_OBSTACLE):
        (*data.path_pts_valid)[idx] = false;
        continue;
      case (costmap_2d::NO_INFORMATION):
        (*data.path_pts_valid)[idx] = tracking_unknown ? true : false;
        continue;
    }

    (*data.path_pts_valid)[idx] = true;
  }
}

/**
 * @brief evaluate path costs if it is not set 评估参考路径点是否有效
 * @param data Data to use
 */
inline void setPathCostsIfNotSet(
  CriticData & data,
  std::shared_ptr<costmap_2d::Costmap2DROS> costmap_ros)
{
  if (!data.path_pts_valid) {
    findPathCosts(data, costmap_ros);
  }
}

/**
 * @brief Compare to trajectory points to find closest path point along integrated distances
 * @param vec Vect to check
 * @return dist Distance to look for
 * @return init Starting index to indec from
 */
inline unsigned int findClosestPathPt(
  const std::vector<float> & vec, const float dist, const unsigned int init = 0u)
{
  float distim1 = init != 0u ? vec[init] : 0.0f;  // First is 0, no accumulated distance yet
  float disti = 0.0f;
  const unsigned int size = vec.size();
  for (unsigned int i = init + 1; i != size; i++) {
    disti = vec[i];
    if (disti > dist) {
      if (i > 0 && dist - distim1 < disti - dist) {
        return i - 1;
      }
      return i;
    }
    distim1 = disti;
  }
  return size - 1;
}


/**
 * @brief evaluate angle from pose (have angle) to point (no angle)
 * @param pose pose
 * @param point_x Point to find angle relative to X axis
 * @param point_y Point to find angle relative to Y axis
 * @param forward_preference If reversing direction is valid
 * @return Angle between two points
 */
inline float posePointAngle(
  const geometry_msgs::Pose & pose, double point_x, double point_y, bool forward_preference)
{
  float pose_x = pose.position.x;
  float pose_y = pose.position.y;
  float pose_yaw = tf2::getYaw(pose.orientation);

  float yaw = atan2f(point_y - pose_y, point_x - pose_x);

  // If no preference for forward, return smallest angle either in heading or 180 of heading
  if (!forward_preference) {
    return std::min(
      fabs(angles::shortest_angular_distance(yaw, pose_yaw)),
      fabs(angles::shortest_angular_distance(yaw, angles::normalize_angle(pose_yaw + M_PI))));
  }

  return fabs(angles::shortest_angular_distance(yaw, pose_yaw));
}

/**
 * @brief evaluate angle from pose (have angle) to point (no angle)
 * @param pose pose
 * @param point_x Point to find angle relative to X axis
 * @param point_y Point to find angle relative to Y axis
 * @param point_yaw Yaw of the point to consider along Z axis
 * @return Angle between two points
 */
inline float posePointAngle(
  const geometry_msgs::Pose & pose,
  double point_x, double point_y, double point_yaw)
{
  float pose_x = static_cast<float>(pose.position.x);
  float pose_y = static_cast<float>(pose.position.y);
  float pose_yaw = static_cast<float>(tf2::getYaw(pose.orientation));

  float yaw = atan2f(static_cast<float>(point_y) - pose_y, static_cast<float>(point_x) - pose_x);

  if (fabs(angles::shortest_angular_distance(yaw, static_cast<float>(point_yaw))) > M_PI_2) {
    yaw = angles::normalize_angle(yaw + M_PI);
  }

  return fabs(angles::shortest_angular_distance(yaw, pose_yaw));
}

/**
 * @brief Normalize the yaws between points on the basis of final yaw angle
 *    of the trajectory.
 * @param last_yaws Final yaw angles of the trajectories.
 * @param yaw_between_points Yaw angles calculated between x and y co-ordinates of the trajectories.
 * @return Normalized yaw between points.
 */
inline auto normalize_yaws_between_points(
  const Eigen::Ref<const Eigen::ArrayXf> & last_yaws,
  const Eigen::Ref<const Eigen::ArrayXf> & yaw_between_points)
{
  Eigen::ArrayXf yaws = utils::shortest_angular_distance(
          last_yaws, yaw_between_points).cast<float>().abs();
  int size = yaws.size();
  Eigen::ArrayXf yaws_between_points_corrected(size);
  for(int i = 0; i != size; i++) {
    const float & yaw_between_point = yaw_between_points[i];
    yaws_between_points_corrected[i] = yaws[i] < M_PI_2 ?
      yaw_between_point : angles::normalize_angle(yaw_between_point + M_PI);
  }
  return yaws_between_points_corrected;
}

/**
 * @brief Normalize the yaws between points on the basis of goal angle.
 * @param goal_yaw Goal yaw angle.
 * @param yaw_between_points Yaw angles calculated between x and y co-ordinates of the trajectories.
 * @return Normalized yaw between points
 */
inline auto normalize_yaws_between_points(
  const float goal_yaw, const Eigen::Ref<const Eigen::ArrayXf> & yaw_between_points)
{
  int size = yaw_between_points.size();
  Eigen::ArrayXf yaws_between_points_corrected(size);
  for(int i = 0; i != size; i++) {
    const float & yaw_between_point = yaw_between_points[i];
    yaws_between_points_corrected[i] = fabs(
      angles::normalize_angle(yaw_between_point - goal_yaw)) < M_PI_2 ?
      yaw_between_point : angles::normalize_angle(yaw_between_point + M_PI);
  }
  return yaws_between_points_corrected;
}

/**
 * @brief Clamps the input between the given lower and upper bounds.
 * @param lower_bound Lower bound.
 * @param upper_bound Upper bound.
 * @return Clamped output.
 */
inline float clamp(
  const float lower_bound, const float upper_bound, const float input)
{
  return std::min(upper_bound, std::max(input, lower_bound));
}









}
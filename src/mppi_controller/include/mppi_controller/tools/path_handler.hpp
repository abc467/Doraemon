#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include <costmap_2d/costmap_2d_ros.h>
#include <geometry_msgs/PoseStamped.h>
#include <nav_msgs/Path.h>
#include <ros/ros.h>
#include <tf2_ros/buffer.h>

#include "mppi_controller/tools/utils.hpp"

namespace mppi
{

using PathIterator = std::vector<geometry_msgs::PoseStamped>::iterator;

/** Select the closest progress pose using Nav2's bounded-search/two-point rule. */
PathIterator findClosestPathPose(
  PathIterator begin, PathIterator end,
  const geometry_msgs::PoseStamped & robot_pose,
  double maximum_search_distance);

/**
 * @brief Official-style local path windowing for the ROS1 MPPI port.
 *
 * The first pose returned to MPPI is always the closest remaining path pose
 * (or its immediate predecessor when needed to keep a terminal direction).
 * Historical poses are never prepended as a fixed-length terminal tail.
 */
class PathHandler
{
public:
  PathHandler() = default;
  ~PathHandler() = default;

  void initialize(
    const ros::NodeHandle & nh, const std::string & name,
    std::shared_ptr<costmap_2d::Costmap2DROS> costmap,
    std::shared_ptr<tf2_ros::Buffer> buffer);

  void setPath(const nav_msgs::Path & plan);
  /** Reload path-window and TF parameters without discarding plan progress. */
  void reloadParameters();
  nav_msgs::Path getPath() const;
  nav_msgs::Path transformPath(
    const geometry_msgs::PoseStamped & robot_pose);
  bool transformedPathEndsAtGoal() const;
  geometry_msgs::PoseStamped getTransformedGoal();

protected:
  bool transformPose(
    const std::string & frame,
    const geometry_msgs::PoseStamped & in_pose,
    geometry_msgs::PoseStamped & out_pose) const;
  double getMaxCostmapDist();
  geometry_msgs::PoseStamped transformToGlobalPlanFrame(
    const geometry_msgs::PoseStamped & pose);
  std::pair<nav_msgs::Path, PathIterator>
  getGlobalPlanConsideringBoundsInCostmapFrame(
    const geometry_msgs::PoseStamped & global_pose);
  void prunePlan(nav_msgs::Path & plan, const PathIterator end);
  void readParameters();

  ros::NodeHandle nh_;
  std::shared_ptr<costmap_2d::Costmap2DROS> costmap_;
  std::shared_ptr<tf2_ros::Buffer> tf_buffer_;
  nav_msgs::Path global_plan_;
  nav_msgs::Path remaining_global_plan_;
  double max_robot_pose_search_dist_{0.0};
  double prune_distance_{0.0};
  double transform_tolerance_{0.0};
  bool transformed_path_ends_at_goal_{false};
  mutable std::mutex mutex_;
};

}  // namespace mppi

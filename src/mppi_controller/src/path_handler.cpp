#include "mppi_controller/tools/path_handler.hpp"

#include <algorithm>
#include <cmath>
#include <iterator>
#include <limits>
#include <stdexcept>

#include <tf2_geometry_msgs/tf2_geometry_msgs.h>

namespace mppi
{

PathIterator findClosestPathPose(
  PathIterator begin, PathIterator end,
  const geometry_msgs::PoseStamped & robot_pose,
  double maximum_search_distance)
{
  if (begin == end) {
    return end;
  }
  auto upper_bound = utils::first_after_integrated_distance(
    begin, end, maximum_search_distance);
  if (upper_bound == begin) {
    return begin;
  }
  auto closest = utils::min_by(
    begin, upper_bound,
    [&robot_pose](const geometry_msgs::PoseStamped & pose) {
      return utils::euclidean_distance(robot_pose.pose, pose.pose);
    });

  // Current upstream #6318: retain a predecessor only at the actual end of the
  // complete remaining path. A bounded search window ending between sparse
  // poses must not make progress jump backwards.
  if (std::distance(begin, end) > 1 && closest == std::prev(end))
  {
    closest = std::prev(closest);
  }
  return closest;
}

void PathHandler::initialize(
  const ros::NodeHandle & nh, const std::string &,
  std::shared_ptr<costmap_2d::Costmap2DROS> costmap,
  std::shared_ptr<tf2_ros::Buffer> buffer)
{
  nh_ = nh;
  costmap_ = std::move(costmap);
  tf_buffer_ = std::move(buffer);
  readParameters();
}

void PathHandler::readParameters()
{
  double max_robot_pose_search_dist = getMaxCostmapDist();
  double prune_distance = 1.5;
  double transform_tolerance = 0.1;
  nh_.param(
    "max_robot_pose_search_dist", max_robot_pose_search_dist,
    max_robot_pose_search_dist);
  nh_.param("prune_distance", prune_distance, prune_distance);
  nh_.param("transform_tolerance", transform_tolerance, transform_tolerance);
  if (!std::isfinite(max_robot_pose_search_dist) ||
      !std::isfinite(prune_distance) || !std::isfinite(transform_tolerance))
  {
    throw std::invalid_argument("PathHandler parameters must be finite");
  }
  if (max_robot_pose_search_dist < 0.0) {
    ROS_WARN("Negative max_robot_pose_search_dist searches the complete path");
    max_robot_pose_search_dist = std::numeric_limits<double>::max();
  }
  max_robot_pose_search_dist_ = max_robot_pose_search_dist;
  prune_distance_ = std::max(0.0, prune_distance);
  transform_tolerance_ = std::max(0.0, transform_tolerance);
}

void PathHandler::reloadParameters()
{
  std::lock_guard<std::mutex> lock(mutex_);
  readParameters();
}

std::pair<nav_msgs::Path, PathIterator>
PathHandler::getGlobalPlanConsideringBoundsInCostmapFrame(
  const geometry_msgs::PoseStamped & global_pose)
{
  transformed_path_ends_at_goal_ = false;
  auto begin = remaining_global_plan_.poses.begin();
  auto end = remaining_global_plan_.poses.end();
  if (begin == end) {
    throw std::invalid_argument("Received plan with zero length");
  }

  auto closest_point = findClosestPathPose(
    begin, end, global_pose, max_robot_pose_search_dist_);

  auto pruned_plan_end = utils::first_after_integrated_distance(
    closest_point, end, prune_distance_);

  nav_msgs::Path transformed_plan;
  transformed_plan.header.frame_id = costmap_->getGlobalFrameID();
  transformed_plan.header.stamp = global_pose.header.stamp;

  const std::string & plan_frame = global_plan_.header.frame_id;
  const std::string & costmap_frame = costmap_->getGlobalFrameID();
  const bool transform_required = plan_frame != costmap_frame;
  geometry_msgs::TransformStamped plan_to_costmap;
  if (transform_required) {
    plan_to_costmap = tf_buffer_->lookupTransform(
      costmap_frame, plan_frame, global_pose.header.stamp,
      ros::Duration(transform_tolerance_));
  }

  unsigned int mx = 0u;
  unsigned int my = 0u;
  auto pose_it = closest_point;
  for (; pose_it != pruned_plan_end; ++pose_it) {
    geometry_msgs::PoseStamped plan_pose = *pose_it;
    plan_pose.header.stamp = global_pose.header.stamp;
    plan_pose.header.frame_id = plan_frame;
    geometry_msgs::PoseStamped costmap_pose;
    if (transform_required) {
      tf2::doTransform(plan_pose, costmap_pose, plan_to_costmap);
    } else {
      costmap_pose = plan_pose;
      costmap_pose.header.frame_id = costmap_frame;
    }
    if (!costmap_->getCostmap()->worldToMap(
        costmap_pose.pose.position.x, costmap_pose.pose.position.y, mx, my))
    {
      break;
    }
    transformed_plan.poses.push_back(std::move(costmap_pose));
  }

  transformed_path_ends_at_goal_ =
    pruned_plan_end == end && pose_it == pruned_plan_end;
  return {transformed_plan, closest_point};
}

geometry_msgs::PoseStamped PathHandler::transformToGlobalPlanFrame(
  const geometry_msgs::PoseStamped & pose)
{
  if (remaining_global_plan_.poses.empty()) {
    throw std::invalid_argument("Received plan with zero length");
  }
  const std::string & plan_frame = remaining_global_plan_.header.frame_id;
  if (pose.header.frame_id == plan_frame) {
    return pose;
  }

  const auto transform = tf_buffer_->lookupTransform(
    plan_frame, pose.header.frame_id, ros::Time(0),
    ros::Duration(transform_tolerance_));
  geometry_msgs::PoseStamped robot_pose;
  tf2::doTransform(pose, robot_pose, transform);
  return robot_pose;
}

nav_msgs::Path PathHandler::transformPath(
  const geometry_msgs::PoseStamped & robot_pose)
{
  std::lock_guard<std::mutex> lock(mutex_);
  const auto global_pose = transformToGlobalPlanFrame(robot_pose);
  auto window = getGlobalPlanConsideringBoundsInCostmapFrame(global_pose);
  prunePlan(remaining_global_plan_, window.second);
  if (window.first.poses.empty()) {
    throw std::invalid_argument("Resulting plan has 0 poses in it");
  }
  return window.first;
}

bool PathHandler::transformPose(
  const std::string & frame,
  const geometry_msgs::PoseStamped & in_pose,
  geometry_msgs::PoseStamped & out_pose) const
{
  if (in_pose.header.frame_id == frame) {
    out_pose = in_pose;
    return true;
  }
  try {
    tf_buffer_->transform(
      in_pose, out_pose, frame, ros::Duration(transform_tolerance_));
    out_pose.header.frame_id = frame;
    return true;
  } catch (const tf2::TransformException & ex) {
    ROS_ERROR("Exception in transformPose: %s", ex.what());
    return false;
  }
}

double PathHandler::getMaxCostmapDist()
{
  const auto * costmap = costmap_->getCostmap();
  return static_cast<double>(
    std::max(costmap->getSizeInCellsX(), costmap->getSizeInCellsY())) *
    costmap->getResolution() * 0.5;
}

void PathHandler::setPath(const nav_msgs::Path & plan)
{
  std::lock_guard<std::mutex> lock(mutex_);
  if (plan.poses.empty()) {
    throw std::invalid_argument("Received plan with zero length");
  }
  global_plan_ = plan;
  remaining_global_plan_ = plan;
  transformed_path_ends_at_goal_ = false;
}

nav_msgs::Path PathHandler::getPath() const
{
  std::lock_guard<std::mutex> lock(mutex_);
  return global_plan_;
}

bool PathHandler::transformedPathEndsAtGoal() const
{
  std::lock_guard<std::mutex> lock(mutex_);
  return transformed_path_ends_at_goal_;
}

void PathHandler::prunePlan(nav_msgs::Path & plan, const PathIterator end)
{
  plan.poses.erase(plan.poses.begin(), end);
}

geometry_msgs::PoseStamped PathHandler::getTransformedGoal()
{
  std::lock_guard<std::mutex> lock(mutex_);
  if (global_plan_.poses.empty()) {
    throw std::invalid_argument("Received plan with zero length");
  }
  auto goal = global_plan_.poses.back();
  goal.header.frame_id = global_plan_.header.frame_id;
  // A plan can remain active much longer than TF's cache. Transform the
  // geometric goal using the latest available transform, just as the robot
  // pose is transformed against the current control cycle.
  goal.header.stamp = ros::Time(0);
  if (goal.header.frame_id.empty()) {
    throw std::runtime_error("Goal pose has an empty frame_id");
  }
  geometry_msgs::PoseStamped transformed_goal;
  if (!transformPose(costmap_->getGlobalFrameID(), goal, transformed_goal)) {
    throw std::runtime_error("Unable to transform goal pose into costmap frame");
  }
  return transformed_goal;
}

}  // namespace mppi

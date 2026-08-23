#include "coverage_constraints_nav/static_artifact_clear_layer.h"

#include <algorithm>
#include <cmath>
#include <string>
#include <vector>

#include <costmap_2d/cost_values.h>
#include <pluginlib/class_list_macros.h>
#include <ros/ros.h>
#include <tf2/LinearMath/Transform.h>
#include <tf2_geometry_msgs/tf2_geometry_msgs.h>
#include <tf2_ros/buffer.h>

namespace coverage_constraints_nav {

namespace {

std::string runtimeMapRevision() {
  for (const auto& name : {
           std::string("/map_revision_id"),
           std::string("/cartographer/runtime/current_map_revision_id"),
           std::string("/cartographer/runtime/map_revision_id")}) {
    std::string value;
    if (ros::param::getCached(name, value) && !value.empty()) {
      return value;
    }
  }
  return std::string();
}

}  // namespace

std::size_t clearLethalArtifactCells(
    costmap_2d::Costmap2D* grid,
    const std::vector<geometry_msgs::Point>& points,
    int min_i, int min_j, int max_i, int max_j) {
  if (grid == nullptr) {
    return 0u;
  }

  std::size_t cleared = 0u;
  for (const auto& point : points) {
    unsigned int mx = 0u;
    unsigned int my = 0u;
    if (!grid->worldToMap(point.x, point.y, mx, my)) {
      continue;
    }
    if (static_cast<int>(mx) < min_i || static_cast<int>(mx) >= max_i ||
        static_cast<int>(my) < min_j || static_cast<int>(my) >= max_j) {
      continue;
    }
    // Never clear unknown or soft costs.  At this position in the plugin
    // chain, an occupied StaticLayer cell is exactly LETHAL_OBSTACLE.
    if (grid->getCost(mx, my) != costmap_2d::LETHAL_OBSTACLE) {
      continue;
    }
    grid->setCost(mx, my, costmap_2d::FREE_SPACE);
    ++cleared;
  }
  return cleared;
}

StaticArtifactClearLayer::StaticArtifactClearLayer() = default;

void StaticArtifactClearLayer::onInitialize() {
  ros::NodeHandle nh("~/" + name_);
  enabled_ = true;
  current_ = true;
  nh.param("enabled", enabled_, true);
  nh.param("source_frame", source_frame_, std::string("map"));
  nh.param("required_map_revision_id", required_map_revision_id_, std::string());

  std::vector<double> points_x;
  std::vector<double> points_y;
  nh.getParam("points_x", points_x);
  nh.getParam("points_y", points_y);
  if (points_x.size() != points_y.size()) {
    ROS_ERROR_STREAM(
        "[StaticArtifactClearLayer] points_x/points_y size mismatch: "
        << points_x.size() << " vs " << points_y.size());
    enabled_ = false;
  } else {
    configured_points_.reserve(points_x.size());
    for (std::size_t index = 0; index < points_x.size(); ++index) {
      if (!std::isfinite(points_x[index]) || !std::isfinite(points_y[index])) {
        ROS_ERROR_STREAM(
            "[StaticArtifactClearLayer] non-finite point at index " << index);
        enabled_ = false;
        configured_points_.clear();
        break;
      }
      geometry_msgs::Point point;
      point.x = points_x[index];
      point.y = points_y[index];
      point.z = 0.0;
      configured_points_.push_back(point);
    }
  }
  if (configured_points_.empty()) {
    enabled_ = false;
  }

  ROS_INFO_STREAM(
      "[StaticArtifactClearLayer] initialized enabled="
      << (enabled_ ? "true" : "false")
      << " source_frame=" << source_frame_
      << " required_revision="
      << (required_map_revision_id_.empty() ? "<none>" : required_map_revision_id_)
      << " points=" << configured_points_.size());
}

bool StaticArtifactClearLayer::revisionMatches() const {
  if (required_map_revision_id_.empty()) {
    return true;
  }
  return runtimeMapRevision() == required_map_revision_id_;
}

bool StaticArtifactClearLayer::transformConfiguredPoints(
    std::vector<geometry_msgs::Point>* points) const {
  if (points == nullptr || layered_costmap_ == nullptr) {
    return false;
  }
  points->clear();
  const std::string target_frame = layered_costmap_->getGlobalFrameID();
  if (target_frame.empty() || source_frame_.empty()) {
    return false;
  }

  tf2::Transform transform;
  if (source_frame_ == target_frame) {
    transform.setIdentity();
  } else {
    if (tf_ == nullptr) {
      return false;
    }
    try {
      const auto stamped = tf_->lookupTransform(
          target_frame, source_frame_, ros::Time(0), ros::Duration(0.05));
      tf2::fromMsg(stamped.transform, transform);
    } catch (const std::exception& error) {
      ROS_WARN_THROTTLE(
          2.0, "[StaticArtifactClearLayer] transform failed: %s", error.what());
      return false;
    }
  }

  points->reserve(configured_points_.size());
  for (const auto& source : configured_points_) {
    const tf2::Vector3 result =
        transform * tf2::Vector3(source.x, source.y, source.z);
    geometry_msgs::Point target;
    target.x = result.x();
    target.y = result.y();
    target.z = result.z();
    points->push_back(target);
  }
  return true;
}

void StaticArtifactClearLayer::updateBounds(
    double /*robot_x*/, double /*robot_y*/, double /*robot_yaw*/,
    double* min_x, double* min_y, double* max_x, double* max_y) {
  cycle_points_.clear();
  current_ = true;
  if (!enabled_ || !revisionMatches() || min_x == nullptr || min_y == nullptr ||
      max_x == nullptr || max_y == nullptr || layered_costmap_ == nullptr ||
      layered_costmap_->getCostmap() == nullptr) {
    return;
  }

  std::vector<geometry_msgs::Point> transformed;
  if (!transformConfiguredPoints(&transformed)) {
    // Failing to apply an optional free-space correction is fail-safe: the
    // original static lethal cell remains.  It must not stop the costmap.
    return;
  }

  const auto* master = layered_costmap_->getCostmap();
  const double half_cell = 0.5 * master->getResolution();
  for (const auto& point : transformed) {
    unsigned int mx = 0u;
    unsigned int my = 0u;
    if (!master->worldToMap(point.x, point.y, mx, my)) {
      continue;
    }
    cycle_points_.push_back(point);
    *min_x = std::min(*min_x, point.x - half_cell);
    *min_y = std::min(*min_y, point.y - half_cell);
    *max_x = std::max(*max_x, point.x + half_cell);
    *max_y = std::max(*max_y, point.y + half_cell);
  }
}

void StaticArtifactClearLayer::updateCosts(
    costmap_2d::Costmap2D& master_grid,
    int min_i, int min_j, int max_i, int max_j) {
  if (!enabled_ || !revisionMatches()) {
    return;
  }
  const std::size_t cleared = clearLethalArtifactCells(
      &master_grid, cycle_points_, min_i, min_j, max_i, max_j);
  if (cleared > 0u) {
    ROS_INFO_STREAM_THROTTLE(
        10.0, "[StaticArtifactClearLayer] cleared " << cleared
        << " configured static artifact cell(s)");
  }
}

void StaticArtifactClearLayer::reset() {
  cycle_points_.clear();
  current_ = true;
}

}  // namespace coverage_constraints_nav

PLUGINLIB_EXPORT_CLASS(
    coverage_constraints_nav::StaticArtifactClearLayer, costmap_2d::Layer)

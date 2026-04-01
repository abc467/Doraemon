#include "coverage_constraints_nav/keepout_constraint_layer.h"

#include <algorithm>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include <costmap_2d/cost_values.h>
#include <geometry_msgs/PointStamped.h>
#include <pluginlib/class_list_macros.h>

namespace coverage_constraints_nav {

KeepoutConstraintLayer::KeepoutConstraintLayer() : has_constraints_(false) {}

void KeepoutConstraintLayer::onInitialize() {
  ros::NodeHandle nh("~/" + name_);
  current_ = true;
  enabled_ = true;
  default_value_ = costmap_2d::FREE_SPACE;

  nh.param("enabled", enabled_, true);
  nh.param("constraints_topic", constraints_topic_, std::string("/map_constraints/current"));

  matchSize();
  sub_ = ros::NodeHandle().subscribe(constraints_topic_, 1, &KeepoutConstraintLayer::constraintsCb, this);
  ROS_INFO_STREAM("[KeepoutConstraintLayer] initialized topic=" << constraints_topic_);
}

void KeepoutConstraintLayer::matchSize() {
  CostmapLayer::matchSize();
}

void KeepoutConstraintLayer::constraintsCb(const coverage_msgs::MapConstraintsConstPtr& msg) {
  std::lock_guard<std::mutex> lock(mutex_);
  latest_msg_ = *msg;
  has_constraints_ = true;
  current_ = true;
}

bool KeepoutConstraintLayer::transformPoint(
    const geometry_msgs::Point& src,
    const std::string& source_frame,
    const std::string& target_frame,
    geometry_msgs::Point* dst) const {
  if (dst == nullptr) {
    return false;
  }
  if (source_frame.empty() || target_frame.empty() || source_frame == target_frame) {
    *dst = src;
    return true;
  }
  if (tf_ == nullptr) {
    return false;
  }
  geometry_msgs::PointStamped src_stamped;
  geometry_msgs::PointStamped dst_stamped;
  src_stamped.header.frame_id = source_frame;
  src_stamped.header.stamp = ros::Time(0);
  src_stamped.point = src;
  try {
    tf_->transform(src_stamped, dst_stamped, target_frame, ros::Duration(0.05));
  } catch (const std::exception& e) {
    ROS_WARN_THROTTLE(2.0, "[KeepoutConstraintLayer] transform failed: %s", e.what());
    return false;
  }
  *dst = dst_stamped.point;
  return true;
}

std::vector<PolygonRegion> KeepoutConstraintLayer::getTransformedRegions(const std::string& target_frame) const {
  coverage_msgs::MapConstraints msg;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!has_constraints_) {
      return {};
    }
    msg = latest_msg_;
  }

  std::vector<coverage_msgs::ZoneGeometry> raw_regions;
  raw_regions.reserve(msg.no_go_polygons.size() + msg.virtual_wall_keepouts.size());
  raw_regions.insert(raw_regions.end(), msg.no_go_polygons.begin(), msg.no_go_polygons.end());
  raw_regions.insert(raw_regions.end(), msg.virtual_wall_keepouts.begin(), msg.virtual_wall_keepouts.end());

  const std::string default_frame = msg.header.frame_id.empty() ? std::string("map") : msg.header.frame_id;
  std::vector<PolygonRegion> out;
  out.reserve(raw_regions.size());

  for (const auto& region_msg : raw_regions) {
    const std::string region_frame = region_msg.frame_id.empty() ? default_frame : region_msg.frame_id;
    PolygonRegion region;

    for (const auto& pt32 : region_msg.outer.points) {
      geometry_msgs::Point src;
      src.x = pt32.x;
      src.y = pt32.y;
      src.z = 0.0;
      geometry_msgs::Point dst;
      if (!transformPoint(src, region_frame, target_frame, &dst)) {
        region.outer.clear();
        break;
      }
      region.outer.push_back(dst);
    }
    if (region.outer.size() < 3) {
      continue;
    }

    for (const auto& hole_msg : region_msg.holes) {
      std::vector<geometry_msgs::Point> hole;
      for (const auto& pt32 : hole_msg.points) {
        geometry_msgs::Point src;
        src.x = pt32.x;
        src.y = pt32.y;
        src.z = 0.0;
        geometry_msgs::Point dst;
        if (!transformPoint(src, region_frame, target_frame, &dst)) {
          hole.clear();
          break;
        }
        hole.push_back(dst);
      }
      if (hole.size() >= 3) {
        region.holes.push_back(hole);
      }
    }
    out.push_back(region);
  }
  return out;
}

bool KeepoutConstraintLayer::pointInRing(double x, double y, const std::vector<geometry_msgs::Point>& ring) {
  if (ring.size() < 3) {
    return false;
  }
  bool inside = false;
  for (size_t i = 0, j = ring.size() - 1; i < ring.size(); j = i++) {
    const auto& pi = ring[i];
    const auto& pj = ring[j];
    const bool intersect =
        ((pi.y > y) != (pj.y > y)) &&
        (x < (pj.x - pi.x) * (y - pi.y) / ((pj.y - pi.y) + 1e-9) + pi.x);
    if (intersect) {
      inside = !inside;
    }
  }
  return inside;
}

bool KeepoutConstraintLayer::pointInRegion(double x, double y, const PolygonRegion& region) {
  if (!pointInRing(x, y, region.outer)) {
    return false;
  }
  for (const auto& hole : region.holes) {
    if (pointInRing(x, y, hole)) {
      return false;
    }
  }
  return true;
}

void KeepoutConstraintLayer::boundsFromRegion(
    const PolygonRegion& region,
    double* min_x,
    double* min_y,
    double* max_x,
    double* max_y) {
  if (region.outer.empty()) {
    return;
  }
  for (const auto& pt : region.outer) {
    *min_x = std::min(*min_x, pt.x);
    *min_y = std::min(*min_y, pt.y);
    *max_x = std::max(*max_x, pt.x);
    *max_y = std::max(*max_y, pt.y);
  }
}

void KeepoutConstraintLayer::updateBounds(
    double /*robot_x*/, double /*robot_y*/, double /*robot_yaw*/,
    double* min_x, double* min_y, double* max_x, double* max_y) {
  if (!enabled_ || min_x == nullptr || min_y == nullptr || max_x == nullptr || max_y == nullptr) {
    return;
  }
  const auto regions = getTransformedRegions(layered_costmap_->getGlobalFrameID());
  for (const auto& region : regions) {
    boundsFromRegion(region, min_x, min_y, max_x, max_y);
  }
}

void KeepoutConstraintLayer::updateCosts(
    costmap_2d::Costmap2D& master_grid,
    int min_i, int min_j, int max_i, int max_j) {
  if (!enabled_) {
    return;
  }
  const auto regions = getTransformedRegions(layered_costmap_->getGlobalFrameID());
  for (const auto& region : regions) {
    double region_min_x = std::numeric_limits<double>::max();
    double region_min_y = std::numeric_limits<double>::max();
    double region_max_x = -std::numeric_limits<double>::max();
    double region_max_y = -std::numeric_limits<double>::max();
    boundsFromRegion(region, &region_min_x, &region_min_y, &region_max_x, &region_max_y);
    if (region_min_x > region_max_x || region_min_y > region_max_y) {
      continue;
    }

    int x0 = 0;
    int y0 = 0;
    int x1 = 0;
    int y1 = 0;
    master_grid.worldToMapEnforceBounds(region_min_x, region_min_y, x0, y0);
    master_grid.worldToMapEnforceBounds(region_max_x, region_max_y, x1, y1);

    const int start_i = std::max(min_i, std::min(x0, x1));
    const int start_j = std::max(min_j, std::min(y0, y1));
    const int end_i = std::min(max_i, std::max(x0, x1) + 1);
    const int end_j = std::min(max_j, std::max(y0, y1) + 1);

    for (int j = start_j; j < end_j; ++j) {
      for (int i = start_i; i < end_i; ++i) {
        double wx = 0.0;
        double wy = 0.0;
        master_grid.mapToWorld(static_cast<unsigned int>(i), static_cast<unsigned int>(j), wx, wy);
        if (pointInRegion(wx, wy, region)) {
          master_grid.setCost(static_cast<unsigned int>(i), static_cast<unsigned int>(j), costmap_2d::LETHAL_OBSTACLE);
        }
      }
    }
  }
}

void KeepoutConstraintLayer::reset() {
  // Keepout constraints are latched and effectively static between publishes.
  // Marking the layer non-current on every reset can leave the whole local
  // costmap permanently stale until /map_constraints/current is republished.
  // That, in turn, makes MBF safetyCheck() reject cmd_vel and causes the
  // robot to move in a stop-go pattern.
  current_ = true;
}

}  // namespace coverage_constraints_nav

PLUGINLIB_EXPORT_CLASS(coverage_constraints_nav::KeepoutConstraintLayer, costmap_2d::Layer)

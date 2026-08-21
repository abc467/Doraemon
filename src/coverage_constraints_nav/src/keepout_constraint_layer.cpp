#include "coverage_constraints_nav/keepout_constraint_layer.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <costmap_2d/cost_values.h>
#include <pluginlib/class_list_macros.h>
#include <tf2/LinearMath/Transform.h>
#include <tf2_geometry_msgs/tf2_geometry_msgs.h>
#include <tf2_ros/buffer.h>

namespace coverage_constraints_nav {

namespace {

std::string trim(const std::string& value) {
  const auto first = value.find_first_not_of(" \t\r\n");
  if (first == std::string::npos) {
    return std::string();
  }
  const auto last = value.find_last_not_of(" \t\r\n");
  return value.substr(first, last - first + 1u);
}

std::string firstCachedParam(const std::vector<std::string>& names) {
  for (const auto& name : names) {
    std::string value;
    if (ros::param::getCached(name, value)) {
      value = trim(value);
      if (!value.empty()) {
        return value;
      }
    }
  }
  return std::string();
}

void setReason(std::string* reason, const std::string& value) {
  if (reason != nullptr) {
    *reason = value;
  }
}

bool samePoint(const geometry_msgs::Point& lhs, const geometry_msgs::Point& rhs) {
  constexpr double kGeometryEpsilon = 1e-6;
  return std::fabs(lhs.x - rhs.x) <= kGeometryEpsilon &&
         std::fabs(lhs.y - rhs.y) <= kGeometryEpsilon &&
         std::fabs(lhs.z - rhs.z) <= kGeometryEpsilon;
}

}  // namespace

bool constraintIdentityMatchesRuntime(
    const coverage_msgs::MapConstraints& msg,
    const std::string& runtime_revision_id,
    const std::string& runtime_map_id,
    const std::string& runtime_map_md5,
    std::string* reason) {
  if (!msg.valid) {
    setReason(reason, "publisher marked constraint snapshot invalid: " + msg.invalid_reason);
    return false;
  }

  const std::string revision = trim(runtime_revision_id);
  if (!revision.empty()) {
    if (trim(msg.map_revision_id).empty()) {
      setReason(reason, "constraint snapshot has no map revision id");
      return false;
    }
    if (trim(msg.map_revision_id) != revision) {
      setReason(reason, "constraint/runtime map revision mismatch");
      return false;
    }
    return true;
  }

  const std::string md5 = trim(runtime_map_md5);
  if (!md5.empty()) {
    const std::string message_md5 = !trim(msg.runtime_map_md5).empty() ?
        trim(msg.runtime_map_md5) : trim(msg.map_md5);
    if (message_md5.empty()) {
      setReason(reason, "constraint snapshot has no runtime map MD5");
      return false;
    }
    if (message_md5 != md5) {
      setReason(reason, "constraint/runtime map MD5 mismatch");
      return false;
    }
    return true;
  }

  const std::string map_id = trim(runtime_map_id);
  if (!map_id.empty()) {
    const std::string message_id = !trim(msg.runtime_map_id).empty() ?
        trim(msg.runtime_map_id) : trim(msg.map_id);
    if (message_id.empty()) {
      setReason(reason, "constraint snapshot has no runtime map id");
      return false;
    }
    if (message_id != map_id) {
      setReason(reason, "constraint/runtime map id mismatch");
      return false;
    }
    return true;
  }

  setReason(reason, "runtime map identity is not ready");
  return false;
}

KeepoutConstraintLayer::KeepoutConstraintLayer()
    : max_message_age_s_(5.0),
      require_map_identity_match_(true),
      has_constraints_(false),
      needs_reapply_(true) {}

void KeepoutConstraintLayer::onInitialize() {
  ros::NodeHandle nh("~/" + name_);
  current_ = false;
  enabled_ = true;
  default_value_ = costmap_2d::FREE_SPACE;

  nh.param("enabled", enabled_, true);
  nh.param("constraints_topic", constraints_topic_, std::string("/map_constraints/current"));
  nh.param("max_message_age", max_message_age_s_, 5.0);
  nh.param("require_map_identity_match", require_map_identity_match_, true);
  max_message_age_s_ = std::max(0.5, max_message_age_s_);

  matchSize();
  sub_ = ros::NodeHandle().subscribe(constraints_topic_, 1, &KeepoutConstraintLayer::constraintsCb, this);
  ROS_INFO_STREAM(
      "[KeepoutConstraintLayer] initialized topic=" << constraints_topic_
      << " identity_check=" << (require_map_identity_match_ ? "true" : "false")
      << " max_age=" << max_message_age_s_ << "s");
}

void KeepoutConstraintLayer::matchSize() {
  CostmapLayer::matchSize();
  applied_regions_.clear();
  cycle_regions_.clear();
  needs_reapply_ = true;
  current_ = false;
}

void KeepoutConstraintLayer::constraintsCb(const coverage_msgs::MapConstraintsConstPtr& msg) {
  std::lock_guard<std::mutex> lock(mutex_);
  const bool scope_changed = !has_constraints_ ||
      latest_msg_.valid != msg->valid ||
      latest_msg_.map_revision_id != msg->map_revision_id ||
      latest_msg_.runtime_map_id != msg->runtime_map_id ||
      latest_msg_.runtime_map_md5 != msg->runtime_map_md5 ||
      latest_msg_.constraint_version != msg->constraint_version;
  latest_msg_ = *msg;
  latest_msg_receipt_time_ = ros::WallTime::now();
  has_constraints_ = true;
  // A heartbeat for the already verified scope must not make the costmap stale
  // for one cycle. A changed/invalid scope is fail-closed until updateBounds
  // validates its complete identity and transforms.
  if (scope_changed || !msg->valid) {
    current_ = false;
  }
}

bool KeepoutConstraintLayer::readStableRuntimeIdentity(
    std::string* revision_id,
    std::string* map_id,
    std::string* map_md5) const {
  if (revision_id == nullptr || map_id == nullptr || map_md5 == nullptr) {
    return false;
  }
  const auto read_revision = []() {
    return firstCachedParam({
        "/map_revision_id",
        "/cartographer/runtime/current_map_revision_id",
        "/cartographer/runtime/map_revision_id"});
  };
  const auto read_id = []() {
    return firstCachedParam({"/map_id", "/map_uuid"});
  };
  const auto read_md5 = []() {
    return firstCachedParam({"/map_md5", "/map_checksum"});
  };

  const std::string revision_before = read_revision();
  const std::string id_before = read_id();
  const std::string md5_before = read_md5();
  const std::string revision_after = read_revision();
  const std::string id_after = read_id();
  const std::string md5_after = read_md5();
  if (revision_before != revision_after || id_before != id_after ||
      md5_before != md5_after) {
    ROS_WARN_THROTTLE(
        1.0, "[KeepoutConstraintLayer] runtime map identity changed during one update");
    return false;
  }
  *revision_id = revision_after;
  *map_id = id_after;
  *map_md5 = md5_after;
  return true;
}

bool KeepoutConstraintLayer::messageIsFresh(const ros::WallTime& receipt_time) const {
  if (receipt_time.isZero()) {
    return false;
  }
  const double age = (ros::WallTime::now() - receipt_time).toSec();
  return std::isfinite(age) && age >= 0.0 && age <= max_message_age_s_;
}

bool KeepoutConstraintLayer::getTransformedRegions(
    const coverage_msgs::MapConstraints& msg,
    const std::string& target_frame,
    std::vector<PolygonRegion>* regions) const {
  if (regions == nullptr || target_frame.empty()) {
    return false;
  }
  regions->clear();

  std::vector<coverage_msgs::ZoneGeometry> raw_regions;
  raw_regions.reserve(msg.no_go_polygons.size() + msg.virtual_wall_keepouts.size());
  raw_regions.insert(raw_regions.end(), msg.no_go_polygons.begin(), msg.no_go_polygons.end());
  raw_regions.insert(raw_regions.end(), msg.virtual_wall_keepouts.begin(), msg.virtual_wall_keepouts.end());

  const std::string default_frame = msg.header.frame_id.empty() ? std::string("map") : msg.header.frame_id;
  regions->reserve(raw_regions.size());
  std::unordered_map<std::string, tf2::Transform> transforms;

  for (const auto& region_msg : raw_regions) {
    const std::string region_frame = region_msg.frame_id.empty() ? default_frame : region_msg.frame_id;
    tf2::Transform transform;
    if (region_frame == target_frame) {
      transform.setIdentity();
    } else {
      const auto found = transforms.find(region_frame);
      if (found != transforms.end()) {
        transform = found->second;
      } else {
        if (tf_ == nullptr) {
          return false;
        }
        try {
          const auto stamped = tf_->lookupTransform(
              target_frame, region_frame, ros::Time(0), ros::Duration(0.05));
          tf2::fromMsg(stamped.transform, transform);
          transforms.emplace(region_frame, transform);
        } catch (const std::exception& e) {
          ROS_WARN_THROTTLE(
              2.0, "[KeepoutConstraintLayer] transform failed: %s", e.what());
          return false;
        }
      }
    }

    const auto transform_point = [&transform](const geometry_msgs::Point32& src) {
      const tf2::Vector3 transformed = transform * tf2::Vector3(src.x, src.y, src.z);
      geometry_msgs::Point dst;
      dst.x = transformed.x();
      dst.y = transformed.y();
      dst.z = transformed.z();
      return dst;
    };
    PolygonRegion region;

    for (const auto& pt32 : region_msg.outer.points) {
      region.outer.push_back(transform_point(pt32));
    }
    if (region.outer.size() < 3) {
      ROS_WARN_THROTTLE(2.0, "[KeepoutConstraintLayer] invalid outer polygon");
      return false;
    }

    for (const auto& hole_msg : region_msg.holes) {
      std::vector<geometry_msgs::Point> hole;
      for (const auto& pt32 : hole_msg.points) {
        hole.push_back(transform_point(pt32));
      }
      if (!hole.empty() && hole.size() < 3) {
        ROS_WARN_THROTTLE(2.0, "[KeepoutConstraintLayer] invalid polygon hole");
        return false;
      }
      if (!hole.empty()) {
        region.holes.push_back(std::move(hole));
      }
    }
    regions->push_back(std::move(region));
  }
  return true;
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

bool KeepoutConstraintLayer::regionsEquivalent(
    const std::vector<PolygonRegion>& lhs,
    const std::vector<PolygonRegion>& rhs) {
  if (lhs.size() != rhs.size()) {
    return false;
  }
  for (std::size_t region_index = 0; region_index < lhs.size(); ++region_index) {
    const auto& left = lhs[region_index];
    const auto& right = rhs[region_index];
    if (left.outer.size() != right.outer.size() ||
        left.holes.size() != right.holes.size()) {
      return false;
    }
    for (std::size_t index = 0; index < left.outer.size(); ++index) {
      if (!samePoint(left.outer[index], right.outer[index])) {
        return false;
      }
    }
    for (std::size_t hole_index = 0; hole_index < left.holes.size(); ++hole_index) {
      if (left.holes[hole_index].size() != right.holes[hole_index].size()) {
        return false;
      }
      for (std::size_t index = 0; index < left.holes[hole_index].size(); ++index) {
        if (!samePoint(
                left.holes[hole_index][index], right.holes[hole_index][index])) {
          return false;
        }
      }
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

void KeepoutConstraintLayer::includeClippedBounds(
    const std::vector<PolygonRegion>& regions,
    double* min_x, double* min_y, double* max_x, double* max_y) const {
  if (min_x == nullptr || min_y == nullptr || max_x == nullptr || max_y == nullptr ||
      layered_costmap_ == nullptr || layered_costmap_->getCostmap() == nullptr) {
    return;
  }
  const auto* master = layered_costmap_->getCostmap();
  const double grid_min_x = master->getOriginX();
  const double grid_min_y = master->getOriginY();
  const double grid_max_x = grid_min_x + master->getSizeInMetersX();
  const double grid_max_y = grid_min_y + master->getSizeInMetersY();
  for (const auto& region : regions) {
    double region_min_x = std::numeric_limits<double>::max();
    double region_min_y = std::numeric_limits<double>::max();
    double region_max_x = -std::numeric_limits<double>::max();
    double region_max_y = -std::numeric_limits<double>::max();
    boundsFromRegion(
        region, &region_min_x, &region_min_y, &region_max_x, &region_max_y);
    if (region_min_x > grid_max_x || region_max_x < grid_min_x ||
        region_min_y > grid_max_y || region_max_y < grid_min_y) {
      continue;
    }
    *min_x = std::min(*min_x, std::max(region_min_x, grid_min_x));
    *min_y = std::min(*min_y, std::max(region_min_y, grid_min_y));
    *max_x = std::max(*max_x, std::min(region_max_x, grid_max_x));
    *max_y = std::max(*max_y, std::min(region_max_y, grid_max_y));
  }
}

void KeepoutConstraintLayer::updateBounds(
    double /*robot_x*/, double /*robot_y*/, double /*robot_yaw*/,
    double* min_x, double* min_y, double* max_x, double* max_y) {
  if (!enabled_ || min_x == nullptr || min_y == nullptr || max_x == nullptr || max_y == nullptr) {
    return;
  }
  useExtraBounds(min_x, min_y, max_x, max_y);

  coverage_msgs::MapConstraints msg;
  ros::WallTime receipt_time;
  bool has_constraints = false;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    has_constraints = has_constraints_;
    if (has_constraints) {
      msg = latest_msg_;
      receipt_time = latest_msg_receipt_time_;
    }
  }

  std::vector<PolygonRegion> next_regions;
  bool snapshot_current = false;
  bool clear_unusable_snapshot = false;
  std::string reason;
  if (!has_constraints) {
    reason = "no constraint snapshot received";
  } else if (!msg.valid) {
    reason = "invalid constraint snapshot: " + msg.invalid_reason;
    clear_unusable_snapshot = true;
  } else {
    std::string runtime_revision;
    std::string runtime_map_id;
    std::string runtime_map_md5;
    const bool stable_identity = readStableRuntimeIdentity(
        &runtime_revision, &runtime_map_id, &runtime_map_md5);
    const bool identity_matches = !require_map_identity_match_ ||
        (stable_identity && constraintIdentityMatchesRuntime(
            msg, runtime_revision, runtime_map_id, runtime_map_md5, &reason));
    if (!identity_matches) {
      clear_unusable_snapshot = true;
    } else if (!messageIsFresh(receipt_time)) {
      reason = "constraint publisher heartbeat is stale";
      // Retain the last correctly transformed lethal geometry while stopping
      // navigation. Clearing it on a publisher outage would fail open.
      next_regions = applied_regions_;
    } else if (!getTransformedRegions(
                   msg, layered_costmap_->getGlobalFrameID(), &next_regions)) {
      reason = "one or more constraint polygons could not be transformed";
      // A transient TF failure also retains the last known safe geometry.
      next_regions = applied_regions_;
    } else {
      snapshot_current = true;
    }
  }

  if (clear_unusable_snapshot) {
    next_regions.clear();
  }
  const bool geometry_changed = needs_reapply_ ||
      !regionsEquivalent(applied_regions_, next_regions);
  if (geometry_changed) {
    // LayeredCostmap resets this union before invoking every plugin. Reporting
    // old+new bounds clears deleted keepouts without writing FREE_SPACE over a
    // wall or laser obstacle owned by another layer.
    includeClippedBounds(applied_regions_, min_x, min_y, max_x, max_y);
    includeClippedBounds(next_regions, min_x, min_y, max_x, max_y);
  } else if (layered_costmap_->isRolling()) {
    // A rolling master can reset newly exposed strips without changing the
    // polygon message. Re-report the visible keepouts so those cells are
    // painted in the new origin instead of disappearing until another source
    // happens to enlarge the update bounds.
    includeClippedBounds(next_regions, min_x, min_y, max_x, max_y);
  }
  cycle_regions_ = std::move(next_regions);
  current_ = snapshot_current;
  if (!current_) {
    ROS_WARN_THROTTLE(
        2.0, "[KeepoutConstraintLayer] fail-closed: %s", reason.c_str());
  }
}

void KeepoutConstraintLayer::updateCosts(
    costmap_2d::Costmap2D& master_grid,
    int min_i, int min_j, int max_i, int max_j) {
  if (!enabled_) {
    return;
  }
  for (const auto& region : cycle_regions_) {
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
  applied_regions_ = cycle_regions_;
  needs_reapply_ = false;
}

void KeepoutConstraintLayer::reset() {
  // Re-apply the last verified geometry on the next costmap transaction. Its
  // freshness and identity are re-evaluated there; reset cannot certify a
  // latched snapshot as current.
  needs_reapply_ = true;
  current_ = false;
}

}  // namespace coverage_constraints_nav

PLUGINLIB_EXPORT_CLASS(coverage_constraints_nav::KeepoutConstraintLayer, costmap_2d::Layer)

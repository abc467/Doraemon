#pragma once

#include <mutex>
#include <string>
#include <vector>

#include <costmap_2d/costmap_layer.h>
#include <coverage_msgs/MapConstraints.h>
#include <geometry_msgs/Point.h>
#include <ros/ros.h>

namespace coverage_constraints_nav {

struct PolygonRegion {
  std::vector<geometry_msgs::Point> outer;
  std::vector<std::vector<geometry_msgs::Point>> holes;
};

/**
 * Validate a constraint snapshot against one atomic runtime-map identity.
 * Revision identity is authoritative; runtime MD5 / id are legacy fallbacks
 * because an asset's canonical hash may differ from its loaded OccupancyGrid.
 */
bool constraintIdentityMatchesRuntime(
    const coverage_msgs::MapConstraints& msg,
    const std::string& runtime_revision_id,
    const std::string& runtime_map_id,
    const std::string& runtime_map_md5,
    std::string* reason = nullptr);

class KeepoutConstraintLayer : public costmap_2d::CostmapLayer {
public:
  KeepoutConstraintLayer();

  void onInitialize() override;
  void matchSize() override;
  void updateBounds(double robot_x, double robot_y, double robot_yaw,
                    double* min_x, double* min_y, double* max_x, double* max_y) override;
  void updateCosts(costmap_2d::Costmap2D& master_grid,
                   int min_i, int min_j, int max_i, int max_j) override;
  void reset() override;
  bool isDiscretized() const { return true; }

private:
  void constraintsCb(const coverage_msgs::MapConstraintsConstPtr& msg);
  bool getTransformedRegions(const coverage_msgs::MapConstraints& msg,
                             const std::string& target_frame,
                             std::vector<PolygonRegion>* regions) const;
  bool readStableRuntimeIdentity(std::string* revision_id,
                                 std::string* map_id,
                                 std::string* map_md5) const;
  bool messageIsFresh(const ros::WallTime& receipt_time) const;
  void includeClippedBounds(const std::vector<PolygonRegion>& regions,
                            double* min_x, double* min_y,
                            double* max_x, double* max_y) const;

  static bool pointInRing(double x, double y, const std::vector<geometry_msgs::Point>& ring);
  static bool pointInRegion(double x, double y, const PolygonRegion& region);
  static bool regionsEquivalent(const std::vector<PolygonRegion>& lhs,
                                const std::vector<PolygonRegion>& rhs);
  static void boundsFromRegion(const PolygonRegion& region,
                               double* min_x, double* min_y, double* max_x, double* max_y);

  std::string constraints_topic_;
  double max_message_age_s_;
  bool require_map_identity_match_;
  mutable std::mutex mutex_;
  ros::Subscriber sub_;
  coverage_msgs::MapConstraints latest_msg_;
  ros::WallTime latest_msg_receipt_time_;
  bool has_constraints_;
  bool needs_reapply_;

  // updateBounds promotes one immutable geometry snapshot for the matching
  // updateCosts call, so a callback cannot mix old bounds with new polygons.
  std::vector<PolygonRegion> cycle_regions_;
  std::vector<PolygonRegion> applied_regions_;
};

}  // namespace coverage_constraints_nav

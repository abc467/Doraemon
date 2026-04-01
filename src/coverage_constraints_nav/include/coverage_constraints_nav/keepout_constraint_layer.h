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
  std::vector<PolygonRegion> getTransformedRegions(const std::string& target_frame) const;
  bool transformPoint(const geometry_msgs::Point& src,
                      const std::string& source_frame,
                      const std::string& target_frame,
                      geometry_msgs::Point* dst) const;

  static bool pointInRing(double x, double y, const std::vector<geometry_msgs::Point>& ring);
  static bool pointInRegion(double x, double y, const PolygonRegion& region);
  static void boundsFromRegion(const PolygonRegion& region,
                               double* min_x, double* min_y, double* max_x, double* max_y);

  std::string constraints_topic_;
  mutable std::mutex mutex_;
  ros::Subscriber sub_;
  coverage_msgs::MapConstraints latest_msg_;
  bool has_constraints_;
};

}  // namespace coverage_constraints_nav

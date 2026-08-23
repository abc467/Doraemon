#pragma once

#include <cstddef>
#include <string>
#include <vector>

#include <costmap_2d/costmap_2d.h>
#include <costmap_2d/layer.h>
#include <geometry_msgs/Point.h>

namespace coverage_constraints_nav {

/**
 * Clear only explicitly configured lethal cells from the static-map result.
 *
 * This plugin must be ordered immediately after StaticLayer and before sensor
 * obstacle / keepout layers.  Consequently a known static-map artifact is
 * removed, while a real obstacle observation or hard keepout at the same
 * location is still able to restore a lethal cost later in the same update.
 */
class StaticArtifactClearLayer : public costmap_2d::Layer {
public:
  StaticArtifactClearLayer();

  void onInitialize() override;
  void updateBounds(double robot_x, double robot_y, double robot_yaw,
                    double* min_x, double* min_y,
                    double* max_x, double* max_y) override;
  void updateCosts(costmap_2d::Costmap2D& master_grid,
                   int min_i, int min_j, int max_i, int max_j) override;
  void reset() override;
  bool isDiscretized() const { return true; }

private:
  bool revisionMatches() const;
  bool transformConfiguredPoints(std::vector<geometry_msgs::Point>* points) const;

  std::string source_frame_;
  std::string required_map_revision_id_;
  std::vector<geometry_msgs::Point> configured_points_;
  std::vector<geometry_msgs::Point> cycle_points_;
};

/** Clear configured lethal cells that fall inside the current update window. */
std::size_t clearLethalArtifactCells(
    costmap_2d::Costmap2D* grid,
    const std::vector<geometry_msgs::Point>& points,
    int min_i, int min_j, int max_i, int max_j);

}  // namespace coverage_constraints_nav

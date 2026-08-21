#pragma once

#include <atomic>
#include <cstddef>
#include <optional>
#include <string>
#include <vector>

#include <costmap_2d/costmap_2d.h>
#include <geometry_msgs/Point.h>
#include <geometry_msgs/PoseStamped.h>

namespace theta_star
{

struct SE2RefinerConfig
{
  bool enabled{true};
  bool allow_reverse{true};
  bool allow_unknown{false};
  int yaw_bins{32};
  int max_expansions{250000};
  int max_repairs{8};
  int max_allowed_center_cost{26};
  double motion_step{0.10};
  double collision_check_step{0.025};
  double goal_position_tolerance{0.12};
  double goal_yaw_tolerance{0.20};
  std::vector<double> repair_window_lengths{1.8, 3.0, 4.5, 6.0};
  std::vector<double> corridor_widths{0.75, 1.25, 2.0};
};

struct SE2RefinementResult
{
  bool success{false};
  std::vector<geometry_msgs::PoseStamped> path;
  int repairs{0};
  int expansions{0};
  std::string message;
};

// Refines only the unsafe portions of an already topology-valid XY path.  The
// local search state is (x, y, yaw), and its motion set is differential-drive:
// forward/reverse arcs plus collision-checked in-place rotations.  Every edge
// is accepted only after a continuous, filled rectangular-footprint sweep.
class SE2PathRefiner
{
public:
  static bool isPoseSafe(
    const costmap_2d::Costmap2D & costmap,
    const std::vector<geometry_msgs::Point> & footprint,
    const geometry_msgs::PoseStamped & pose,
    bool allow_unknown = false);

  static bool isSweepSafe(
    const costmap_2d::Costmap2D & costmap,
    const std::vector<geometry_msgs::Point> & footprint,
    const geometry_msgs::PoseStamped & start,
    const geometry_msgs::PoseStamped & end,
    double sample_step,
    bool allow_unknown = false);

  static std::optional<std::size_t> firstUnsafeSegment(
    const costmap_2d::Costmap2D & costmap,
    const std::vector<geometry_msgs::Point> & footprint,
    const std::vector<geometry_msgs::PoseStamped> & path,
    double sample_step,
    bool allow_unknown = false);

  static SE2RefinementResult refine(
    const costmap_2d::Costmap2D & costmap,
    const std::vector<geometry_msgs::Point> & footprint,
    const std::vector<geometry_msgs::PoseStamped> & topology_path,
    const SE2RefinerConfig & config,
    const std::atomic<bool> * cancel_requested = nullptr);
};

}  // namespace theta_star

// Copyright 2026 Clean Robot Navigation Team
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include <costmap_2d/cost_values.h>
#include <costmap_2d/costmap_2d.h>
#include <geometry_msgs/Point.h>

namespace smac_lattice_planner_mbf
{

// Keep the coarse centre-line guide on exactly the same centre-cell policy as
// State Lattice: unknown, inscribed (253), and lethal (254) cells are blocked;
// soft inflation costs through 252 remain traversable and influence Theta*'s
// traversal cost.
constexpr unsigned char kStateCenterMaxAllowedCost =
  costmap_2d::INSCRIBED_INFLATED_OBSTACLE - 1u;

enum class CoarseRouteStatus
{
  SUCCESS,
  INVALID_INPUT,
  INVALID_START,
  INVALID_GOAL,
  CANCELED,
  NO_ROUTE
};

/**
 * @brief Packed, map-aligned mask of centres allowed inside one route corridor.
 *
 * A set bit means both:
 *  - the cell centre lies within half_width_m of the coarse route; and
 *  - the immutable source snapshot has State-Lattice-traversable centre cost.
 *
 * This is a search-domain mask, not a replacement collision map. Consumers
 * must still check the complete footprint against the original costmap.
 */
class CenterCorridorMask
{
public:
  CenterCorridorMask() = default;

  bool isAllowed(unsigned int mx, unsigned int my) const noexcept;
  bool isAllowedWorld(double wx, double wy) const noexcept;

  double halfWidthM() const noexcept {return half_width_m_;}
  unsigned int sizeX() const noexcept {return size_x_;}
  unsigned int sizeY() const noexcept {return size_y_;}
  double resolution() const noexcept {return resolution_;}
  double originX() const noexcept {return origin_x_;}
  double originY() const noexcept {return origin_y_;}
  std::size_t allowedCellCount() const noexcept {return allowed_cell_count_;}
  const std::vector<std::uint64_t> & words() const noexcept {return words_;}

private:
  friend class CoarseRouteCorridorHelper;

  CenterCorridorMask(const costmap_2d::Costmap2D & snapshot, double half_width_m);
  void setAllowed(unsigned int mx, unsigned int my) noexcept;

  double half_width_m_{0.0};
  unsigned int size_x_{0u};
  unsigned int size_y_{0u};
  double resolution_{0.0};
  double origin_x_{0.0};
  double origin_y_{0.0};
  std::size_t allowed_cell_count_{0u};
  std::vector<std::uint64_t> words_;
};

/**
 * @brief Read-only map-aligned progress lookup around the coarse route.
 *
 * Each traversable cell in the outer (8 m) centre corridor stores the
 * remaining coarse-route arc length at its nearest projection onto the route.
 * Storage is dense only inside the clipped route bounding box, so lookup is
 * O(1) without allocating one float for every cell of a much larger map.
 */
class RouteProgressField
{
public:
  RouteProgressField() = default;

  bool tryRemainingArcLengthM(
    unsigned int mx, unsigned int my, float & remaining_m) const noexcept;
  bool tryRemainingArcLengthWorld(
    double wx, double wy, float & remaining_m) const noexcept;

  unsigned int minX() const noexcept {return min_x_;}
  unsigned int minY() const noexcept {return min_y_;}
  unsigned int width() const noexcept {return width_;}
  unsigned int height() const noexcept {return height_;}
  std::size_t storedCellCapacity() const noexcept {return remaining_arc_length_m_.size();}
  std::size_t valuedCellCount() const noexcept {return valued_cell_count_;}

private:
  friend class CoarseRouteCorridorHelper;

  RouteProgressField(
    const costmap_2d::Costmap2D & snapshot,
    const std::vector<geometry_msgs::Point> & route,
    double half_width_m);
  void updateIfNearer(
    unsigned int mx, unsigned int my,
    float distance_squared_m, float remaining_arc_length_m) noexcept;
  void finishBuild();

  unsigned int map_size_x_{0u};
  unsigned int map_size_y_{0u};
  unsigned int min_x_{0u};
  unsigned int min_y_{0u};
  unsigned int width_{0u};
  unsigned int height_{0u};
  double resolution_{0.0};
  double origin_x_{0.0};
  double origin_y_{0.0};
  std::size_t valued_cell_count_{0u};
  std::vector<float> remaining_arc_length_m_;
  // Temporary nearest-distance workspace released before returning a result.
  std::vector<float> nearest_distance_squared_m_;
};

struct CoarseRouteCandidate
{
  // Theta* cell centres in start-to-goal order. These points are guidance
  // only; no ROS path is published by this helper.
  std::vector<geometry_msgs::Point> points;
  // Optional dense start-to-goal centre-line reference. Unlike points, this
  // includes the caller's exact world start and goal and is sampled no farther
  // apart than reference_spacing_m. It is used by the Theta-prefix / State-
  // suffix composite planner, never directly published without SE(2) proof.
  std::vector<geometry_msgs::Point> reference_points;
  std::vector<CenterCorridorMask> center_masks;
  RouteProgressField route_progress;
  double length_m{0.0};
  int theta_nodes_opened{0};
};

struct CoarseRouteCorridorOptions
{
  // Public result shape already permits up to three future independent route
  // candidates. The initial implementation deliberately returns one canonical
  // Theta* route and never mutates the snapshot to manufacture alternatives.
  std::size_t max_routes{1u};
  int terminal_checking_interval{1000};
  std::function<bool()> cancel_checker;

  // Theta search policy. Defaults preserve the original coarse State guide;
  // the production composite mode explicitly supplies the field-proven
  // ThetaStarPlanner weights/clearance while retaining fail-closed unknown
  // handling.
  bool theta_allow_unknown{false};
  int theta_max_allowed_cost{static_cast<int>(kStateCenterMaxAllowedCost)};
  double theta_w_traversal_cost{1.0};
  double theta_w_euc_cost{2.0};
  double theta_w_heuristic_cost{1.0};

  // A positive spacing requests a dense exact-endpoint reference for suffix
  // selection. The sparse Theta points remain separate so building large
  // corridor masks does not become quadratic in the dense pose count.
  double reference_spacing_m{0.0};
  std::size_t max_reference_points{200000u};
  bool smooth_reference{false};
  bool build_center_corridors{true};
};

struct CoarseRouteCorridorResult
{
  CoarseRouteStatus status{CoarseRouteStatus::NO_ROUTE};
  std::string message;
  std::vector<CoarseRouteCandidate> routes;

  bool succeeded() const noexcept
  {
    return status == CoarseRouteStatus::SUCCESS && !routes.empty();
  }
};

/**
 * @brief Build a read-only coarse Theta* guide and 1/2/4/8 m centre masks.
 *
 * The input is const and is copied before being passed to the legacy Theta*
 * core, whose API takes a non-const Costmap2D pointer. Neither the caller's
 * immutable snapshot nor the copy is edited, and this helper has no publisher.
 */
class CoarseRouteCorridorHelper
{
public:
  static constexpr std::array<double, 4u> kCorridorHalfWidthsM{{1.0, 2.0, 4.0, 8.0}};
  static constexpr std::size_t kMaximumRoutes = 3u;

  static CoarseRouteCorridorResult build(
    const costmap_2d::Costmap2D & snapshot,
    double start_wx, double start_wy,
    double goal_wx, double goal_wy,
    const CoarseRouteCorridorOptions & options = CoarseRouteCorridorOptions());

  /**
   * @brief Build the standard 1/2/4/8 m centre domains for an existing route.
   *
   * This is the read-only corridor half of build(), exposed for callers which
   * already own a Theta suffix polyline. The route is interpreted in world
   * coordinates and the output objects stay aligned to snapshot. The source
   * costmap and route are never modified; complete-footprint collision checks
   * remain the caller's responsibility.
   *
   * Both outputs are replaced. An empty route produces four empty masks and
   * an empty progress field.
   */
  static void buildCenterMasksForRoute(
    const costmap_2d::Costmap2D & snapshot,
    const std::vector<geometry_msgs::Point> & route,
    std::vector<CenterCorridorMask> & masks,
    RouteProgressField & route_progress);

  static bool isStateCenterTraversable(unsigned char cost) noexcept;

private:
  static void buildCenterMasks(
    const costmap_2d::Costmap2D & snapshot,
    const std::vector<geometry_msgs::Point> & route,
    std::vector<CenterCorridorMask> & masks,
    RouteProgressField & route_progress);
};

}  // namespace smac_lattice_planner_mbf

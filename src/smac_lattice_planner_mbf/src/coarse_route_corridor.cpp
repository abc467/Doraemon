// Copyright 2026 Clean Robot Navigation Team
// Licensed under the Apache License, Version 2.0.

#include "smac_lattice_planner_mbf/coarse_route_corridor.hpp"

#include <algorithm>
#include <cmath>
#include <limits>
#include <utility>

#include <geometry_msgs/PoseStamped.h>

#include "theta_star_planner/theta_star.h"

namespace smac_lattice_planner_mbf
{

namespace
{

std::size_t checkedCellCount(unsigned int size_x, unsigned int size_y)
{
  if (size_x == 0u || size_y == 0u ||
    static_cast<std::size_t>(size_x) >
    std::numeric_limits<std::size_t>::max() / static_cast<std::size_t>(size_y))
  {
    return 0u;
  }
  return static_cast<std::size_t>(size_x) * static_cast<std::size_t>(size_y);
}

double pointSegmentDistanceSquared(
  double px, double py,
  double x0, double y0,
  double x1, double y1,
  double * projection_ratio = nullptr)
{
  const double dx = x1 - x0;
  const double dy = y1 - y0;
  const double length_squared = dx * dx + dy * dy;
  if (length_squared <= std::numeric_limits<double>::epsilon()) {
    if (projection_ratio != nullptr) {
      *projection_ratio = 0.0;
    }
    return (px - x0) * (px - x0) + (py - y0) * (py - y0);
  }
  const double ratio = std::clamp(
    ((px - x0) * dx + (py - y0) * dy) / length_squared, 0.0, 1.0);
  if (projection_ratio != nullptr) {
    *projection_ratio = ratio;
  }
  const double nearest_x = x0 + ratio * dx;
  const double nearest_y = y0 + ratio * dy;
  return (px - nearest_x) * (px - nearest_x) +
         (py - nearest_y) * (py - nearest_y);
}

int lowerCellForWorld(double world, double origin, double resolution)
{
  // Cell i has centre origin + (i + 0.5) * resolution.
  return static_cast<int>(std::ceil((world - origin) / resolution - 0.5));
}

int upperCellForWorld(double world, double origin, double resolution)
{
  return static_cast<int>(std::floor((world - origin) / resolution - 0.5));
}

double routeLength(const std::vector<geometry_msgs::Point> & points)
{
  double length = 0.0;
  for (std::size_t index = 1u; index < points.size(); ++index) {
    length += std::hypot(
      points[index].x - points[index - 1u].x,
      points[index].y - points[index - 1u].y);
  }
  return length;
}

void appendDistinctPoint(
  std::vector<geometry_msgs::Point> & points,
  double x, double y)
{
  if (!points.empty() &&
    std::hypot(points.back().x - x, points.back().y - y) <= 1e-9)
  {
    return;
  }
  geometry_msgs::Point point;
  point.x = x;
  point.y = y;
  point.z = 0.0;
  points.push_back(point);
}

bool densifyReference(
  const std::vector<geometry_msgs::Point> & anchors,
  double spacing,
  std::size_t maximum_points,
  std::vector<geometry_msgs::Point> & dense)
{
  dense.clear();
  if (anchors.empty() || !std::isfinite(spacing) || spacing <= 0.0 ||
    maximum_points == 0u)
  {
    return false;
  }
  dense.reserve(std::min<std::size_t>(anchors.size() * 2u, maximum_points));
  dense.push_back(anchors.front());
  for (std::size_t index = 1u; index < anchors.size(); ++index) {
    const auto & first = anchors[index - 1u];
    const auto & second = anchors[index];
    const double distance = std::hypot(second.x - first.x, second.y - first.y);
    if (distance <= 1e-12) {
      continue;
    }
    const std::size_t segments = std::max<std::size_t>(
      1u, static_cast<std::size_t>(std::ceil(distance / spacing)));
    if (segments > maximum_points - dense.size()) {
      dense.clear();
      return false;
    }
    for (std::size_t segment = 1u; segment <= segments; ++segment) {
      const double ratio = static_cast<double>(segment) /
        static_cast<double>(segments);
      geometry_msgs::Point point;
      point.x = first.x + ratio * (second.x - first.x);
      point.y = first.y + ratio * (second.y - first.y);
      point.z = first.z + ratio * (second.z - first.z);
      dense.push_back(point);
    }
  }
  return !dense.empty();
}

std::vector<geometry_msgs::Point> downsampleReference(
  const std::vector<geometry_msgs::Point> & dense,
  double spacing)
{
  std::vector<geometry_msgs::Point> anchors;
  if (dense.empty() || !std::isfinite(spacing) || spacing <= 0.0) {
    return anchors;
  }
  anchors.push_back(dense.front());
  double accumulated = 0.0;
  for (std::size_t index = 1u; index < dense.size(); ++index) {
    accumulated += std::hypot(
      dense[index].x - dense[index - 1u].x,
      dense[index].y - dense[index - 1u].y);
    if (accumulated + 1e-12 >= spacing) {
      anchors.push_back(dense[index]);
      accumulated = std::max(0.0, accumulated - spacing);
    }
  }
  if (std::hypot(
      anchors.back().x - dense.back().x,
      anchors.back().y - dense.back().y) > 1e-9)
  {
    anchors.push_back(dense.back());
  }
  return anchors;
}

void applyHistoricMovingAverage(std::vector<geometry_msgs::Point> & anchors)
{
  constexpr std::size_t kWindowSize = 5u;
  if (anchors.size() <= kWindowSize) {
    return;
  }
  std::vector<geometry_msgs::Point> filtered = anchors;
  double sum_x = 0.0;
  double sum_y = 0.0;
  std::vector<double> window_x;
  std::vector<double> window_y;
  window_x.reserve(kWindowSize);
  window_y.reserve(kWindowSize);
  // Match the legacy filter: process from the goal backwards and average the
  // current point with up to four points ahead of it.
  for (std::size_t reverse = anchors.size(); reverse-- > 0u;) {
    if (window_x.size() == kWindowSize) {
      sum_x -= window_x.front();
      sum_y -= window_y.front();
      window_x.erase(window_x.begin());
      window_y.erase(window_y.begin());
    }
    sum_x += anchors[reverse].x;
    sum_y += anchors[reverse].y;
    window_x.push_back(anchors[reverse].x);
    window_y.push_back(anchors[reverse].y);
    filtered[reverse].x = sum_x / static_cast<double>(window_x.size());
    filtered[reverse].y = sum_y / static_cast<double>(window_y.size());
  }
  // Legacy smoothing preserves the exact endpoints.
  for (std::size_t index = 1u; index + 1u < anchors.size(); ++index) {
    anchors[index].x = filtered[index].x;
    anchors[index].y = filtered[index].y;
  }
}

bool referenceCenterlineIsSafe(
  const costmap_2d::Costmap2D & snapshot,
  const theta_star::ThetaStar & theta,
  const std::vector<geometry_msgs::Point> & points)
{
  if (points.empty()) {
    return false;
  }
  unsigned int previous_x = 0u;
  unsigned int previous_y = 0u;
  if (!snapshot.worldToMap(points.front().x, points.front().y, previous_x, previous_y) ||
    !theta.isSafe(static_cast<int>(previous_x), static_cast<int>(previous_y)))
  {
    // Keep the original unsmoothed start-escape route when Connect begins in
    // a higher soft-cost band; smoothing must not manufacture a diagonal exit.
    return false;
  }
  for (std::size_t index = 1u; index < points.size(); ++index) {
    unsigned int current_x = 0u;
    unsigned int current_y = 0u;
    if (!snapshot.worldToMap(points[index].x, points[index].y, current_x, current_y) ||
      !theta.isLineSafe(
        static_cast<int>(previous_x), static_cast<int>(previous_y),
        static_cast<int>(current_x), static_cast<int>(current_y)))
    {
      return false;
    }
    previous_x = current_x;
    previous_y = current_y;
  }
  return true;
}

}  // namespace

CenterCorridorMask::CenterCorridorMask(
  const costmap_2d::Costmap2D & snapshot,
  double half_width_m)
: half_width_m_(half_width_m),
  size_x_(snapshot.getSizeInCellsX()),
  size_y_(snapshot.getSizeInCellsY()),
  resolution_(snapshot.getResolution()),
  origin_x_(snapshot.getOriginX()),
  origin_y_(snapshot.getOriginY())
{
  const std::size_t cell_count = checkedCellCount(size_x_, size_y_);
  if (cell_count > 0u) {
    words_.assign((cell_count + 63u) / 64u, 0u);
  }
}

bool CenterCorridorMask::isAllowed(unsigned int mx, unsigned int my) const noexcept
{
  if (mx >= size_x_ || my >= size_y_) {
    return false;
  }
  const std::size_t index =
    static_cast<std::size_t>(my) * static_cast<std::size_t>(size_x_) + mx;
  const std::size_t word = index / 64u;
  return word < words_.size() && (words_[word] & (std::uint64_t{1u} << (index % 64u))) != 0u;
}

bool CenterCorridorMask::isAllowedWorld(double wx, double wy) const noexcept
{
  if (!std::isfinite(wx) || !std::isfinite(wy) || resolution_ <= 0.0 ||
    wx < origin_x_ || wy < origin_y_)
  {
    return false;
  }
  const double continuous_x = (wx - origin_x_) / resolution_;
  const double continuous_y = (wy - origin_y_) / resolution_;
  if (continuous_x >= static_cast<double>(size_x_) ||
    continuous_y >= static_cast<double>(size_y_))
  {
    return false;
  }
  const auto mx = static_cast<unsigned int>(continuous_x);
  const auto my = static_cast<unsigned int>(continuous_y);
  return isAllowed(mx, my);
}

void CenterCorridorMask::setAllowed(unsigned int mx, unsigned int my) noexcept
{
  if (mx >= size_x_ || my >= size_y_) {
    return;
  }
  const std::size_t index =
    static_cast<std::size_t>(my) * static_cast<std::size_t>(size_x_) + mx;
  const std::size_t word = index / 64u;
  if (word >= words_.size()) {
    return;
  }
  const std::uint64_t bit = std::uint64_t{1u} << (index % 64u);
  if ((words_[word] & bit) == 0u) {
    words_[word] |= bit;
    ++allowed_cell_count_;
  }
}

RouteProgressField::RouteProgressField(
  const costmap_2d::Costmap2D & snapshot,
  const std::vector<geometry_msgs::Point> & route,
  double half_width_m)
: map_size_x_(snapshot.getSizeInCellsX()),
  map_size_y_(snapshot.getSizeInCellsY()),
  resolution_(snapshot.getResolution()),
  origin_x_(snapshot.getOriginX()),
  origin_y_(snapshot.getOriginY())
{
  if (route.empty() || map_size_x_ == 0u || map_size_y_ == 0u ||
    resolution_ <= 0.0 || !std::isfinite(half_width_m) || half_width_m < 0.0)
  {
    return;
  }

  double minimum_world_x = route.front().x;
  double maximum_world_x = route.front().x;
  double minimum_world_y = route.front().y;
  double maximum_world_y = route.front().y;
  for (const auto & point : route) {
    minimum_world_x = std::min(minimum_world_x, point.x);
    maximum_world_x = std::max(maximum_world_x, point.x);
    minimum_world_y = std::min(minimum_world_y, point.y);
    maximum_world_y = std::max(maximum_world_y, point.y);
  }
  const int lower_x = std::max(
    0, lowerCellForWorld(minimum_world_x - half_width_m, origin_x_, resolution_));
  const int upper_x = std::min(
    static_cast<int>(map_size_x_) - 1,
    upperCellForWorld(maximum_world_x + half_width_m, origin_x_, resolution_));
  const int lower_y = std::max(
    0, lowerCellForWorld(minimum_world_y - half_width_m, origin_y_, resolution_));
  const int upper_y = std::min(
    static_cast<int>(map_size_y_) - 1,
    upperCellForWorld(maximum_world_y + half_width_m, origin_y_, resolution_));
  if (lower_x > upper_x || lower_y > upper_y) {
    return;
  }

  min_x_ = static_cast<unsigned int>(lower_x);
  min_y_ = static_cast<unsigned int>(lower_y);
  width_ = static_cast<unsigned int>(upper_x - lower_x + 1);
  height_ = static_cast<unsigned int>(upper_y - lower_y + 1);
  const std::size_t cell_count = checkedCellCount(width_, height_);
  if (cell_count == 0u) {
    width_ = 0u;
    height_ = 0u;
    return;
  }
  remaining_arc_length_m_.assign(
    cell_count, std::numeric_limits<float>::infinity());
  nearest_distance_squared_m_.assign(
    cell_count, std::numeric_limits<float>::infinity());
}

bool RouteProgressField::tryRemainingArcLengthM(
  unsigned int mx, unsigned int my, float & remaining_m) const noexcept
{
  if (mx < min_x_ || my < min_y_ || mx >= min_x_ + width_ || my >= min_y_ + height_) {
    return false;
  }
  const std::size_t index =
    static_cast<std::size_t>(my - min_y_) * static_cast<std::size_t>(width_) +
    static_cast<std::size_t>(mx - min_x_);
  if (index >= remaining_arc_length_m_.size() ||
    !std::isfinite(remaining_arc_length_m_[index]))
  {
    return false;
  }
  remaining_m = remaining_arc_length_m_[index];
  return true;
}

bool RouteProgressField::tryRemainingArcLengthWorld(
  double wx, double wy, float & remaining_m) const noexcept
{
  if (!std::isfinite(wx) || !std::isfinite(wy) || resolution_ <= 0.0 ||
    wx < origin_x_ || wy < origin_y_)
  {
    return false;
  }
  const double continuous_x = (wx - origin_x_) / resolution_;
  const double continuous_y = (wy - origin_y_) / resolution_;
  if (continuous_x >= static_cast<double>(map_size_x_) ||
    continuous_y >= static_cast<double>(map_size_y_))
  {
    return false;
  }
  return tryRemainingArcLengthM(
    static_cast<unsigned int>(continuous_x),
    static_cast<unsigned int>(continuous_y), remaining_m);
}

void RouteProgressField::updateIfNearer(
  unsigned int mx, unsigned int my,
  float distance_squared_m, float remaining_arc_length_m) noexcept
{
  if (mx < min_x_ || my < min_y_ || mx >= min_x_ + width_ || my >= min_y_ + height_) {
    return;
  }
  const std::size_t index =
    static_cast<std::size_t>(my - min_y_) * static_cast<std::size_t>(width_) +
    static_cast<std::size_t>(mx - min_x_);
  if (index >= nearest_distance_squared_m_.size()) {
    return;
  }
  const float previous_distance = nearest_distance_squared_m_[index];
  constexpr float kTieEpsilon = 1e-6f;
  if (distance_squared_m + kTieEpsilon < previous_distance ||
    (std::abs(distance_squared_m - previous_distance) <= kTieEpsilon &&
    remaining_arc_length_m < remaining_arc_length_m_[index]))
  {
    if (!std::isfinite(remaining_arc_length_m_[index])) {
      ++valued_cell_count_;
    }
    nearest_distance_squared_m_[index] = distance_squared_m;
    remaining_arc_length_m_[index] = remaining_arc_length_m;
  }
}

void RouteProgressField::finishBuild()
{
  std::vector<float>().swap(nearest_distance_squared_m_);
}

bool CoarseRouteCorridorHelper::isStateCenterTraversable(unsigned char cost) noexcept
{
  return cost != costmap_2d::NO_INFORMATION && cost <= kStateCenterMaxAllowedCost;
}

CoarseRouteCorridorResult CoarseRouteCorridorHelper::build(
  const costmap_2d::Costmap2D & snapshot,
  double start_wx, double start_wy,
  double goal_wx, double goal_wy,
  const CoarseRouteCorridorOptions & options)
{
  CoarseRouteCorridorResult result;
  if (!std::isfinite(start_wx) || !std::isfinite(start_wy) ||
    !std::isfinite(goal_wx) || !std::isfinite(goal_wy) ||
    snapshot.getSizeInCellsX() == 0u || snapshot.getSizeInCellsY() == 0u ||
    snapshot.getResolution() <= 0.0 ||
    options.theta_max_allowed_cost < 0 ||
    options.theta_max_allowed_cost > static_cast<int>(kStateCenterMaxAllowedCost) ||
    !std::isfinite(options.theta_w_traversal_cost) ||
    !std::isfinite(options.theta_w_euc_cost) ||
    !std::isfinite(options.theta_w_heuristic_cost) ||
    options.theta_w_traversal_cost < 0.0 ||
    options.theta_w_euc_cost < 0.0 ||
    options.theta_w_heuristic_cost < 0.0 ||
    !std::isfinite(options.reference_spacing_m) ||
    options.reference_spacing_m < 0.0 ||
    options.max_reference_points == 0u)
  {
    result.status = CoarseRouteStatus::INVALID_INPUT;
    result.message = "coarse route received invalid coordinates or an empty costmap";
    return result;
  }

  unsigned int start_mx = 0u;
  unsigned int start_my = 0u;
  unsigned int goal_mx = 0u;
  unsigned int goal_my = 0u;
  if (!snapshot.worldToMap(start_wx, start_wy, start_mx, start_my)) {
    result.status = CoarseRouteStatus::INVALID_START;
    result.message = "coarse route start is outside the immutable costmap snapshot";
    return result;
  }
  if (!snapshot.worldToMap(goal_wx, goal_wy, goal_mx, goal_my)) {
    result.status = CoarseRouteStatus::INVALID_GOAL;
    result.message = "coarse route goal is outside the immutable costmap snapshot";
    return result;
  }
  const unsigned char start_cost = snapshot.getCost(start_mx, start_my);
  const unsigned char goal_cost = snapshot.getCost(goal_mx, goal_my);
  const bool start_traversable = start_cost != costmap_2d::NO_INFORMATION &&
    start_cost < costmap_2d::INSCRIBED_INFLATED_OBSTACLE;
  const bool goal_traversable =
    (goal_cost == costmap_2d::NO_INFORMATION) ? options.theta_allow_unknown :
    static_cast<int>(goal_cost) <= options.theta_max_allowed_cost;
  if (!start_traversable) {
    result.status = CoarseRouteStatus::INVALID_START;
    result.message = "coarse route start is unknown or in a hard centre-cell obstacle";
    return result;
  }
  if (!goal_traversable) {
    result.status = CoarseRouteStatus::INVALID_GOAL;
    result.message = "coarse route goal violates the configured Theta clearance policy";
    return result;
  }
  if (options.cancel_checker && options.cancel_checker()) {
    result.status = CoarseRouteStatus::CANCELED;
    result.message = "coarse route canceled before search";
    return result;
  }

  // ThetaStar's historic API takes Costmap2D*. Give it a private copy so the
  // caller's immutable snapshot is protected even if that implementation ever
  // regresses to editing a start cell.
  costmap_2d::Costmap2D theta_snapshot(snapshot);
  theta_star::ThetaStar theta;
  theta.costmap_ = &theta_snapshot;
  theta.allow_unknown_ = options.theta_allow_unknown;
  theta.max_allowed_cost_ = options.theta_max_allowed_cost;
  theta.w_traversal_cost_ = options.theta_w_traversal_cost;
  theta.w_euc_cost_ = options.theta_w_euc_cost;
  theta.w_heuristic_cost_ = options.theta_w_heuristic_cost;
  theta.terminal_checking_interval_ = std::max(1, options.terminal_checking_interval);
  theta.setCancelChecker(options.cancel_checker);

  geometry_msgs::PoseStamped start;
  start.pose.position.x = start_wx;
  start.pose.position.y = start_wy;
  start.pose.orientation.w = 1.0;
  geometry_msgs::PoseStamped goal;
  goal.pose.position.x = goal_wx;
  goal.pose.position.y = goal_wy;
  goal.pose.orientation.w = 1.0;
  theta.setStartAndGoal(start, goal);

  std::vector<coordsW> raw_route;
  if (!theta.generatePath(raw_route)) {
    if (options.cancel_checker && options.cancel_checker()) {
      result.status = CoarseRouteStatus::CANCELED;
      result.message = "coarse Theta route canceled";
    } else {
      result.status = CoarseRouteStatus::NO_ROUTE;
      result.message = "no coarse Theta route under State Lattice centre-cell semantics";
    }
    return result;
  }
  if (raw_route.empty()) {
    result.status = CoarseRouteStatus::NO_ROUTE;
    result.message = "coarse Theta route backtrace was empty";
    return result;
  }

  CoarseRouteCandidate candidate;
  candidate.theta_nodes_opened = theta.nodes_opened;
  candidate.points.reserve(raw_route.size());
  for (const auto & point : raw_route) {
    geometry_msgs::Point world;
    world.x = point.x;
    world.y = point.y;
    world.z = 0.0;
    candidate.points.push_back(world);
  }
  candidate.length_m = routeLength(candidate.points);
  if (options.reference_spacing_m > 0.0) {
    std::vector<geometry_msgs::Point> exact_anchors;
    exact_anchors.reserve(candidate.points.size() + 2u);
    appendDistinctPoint(exact_anchors, start_wx, start_wy);
    // Theta searches cell indices and its backtrace therefore starts at the
    // centre of the cell containing the exact robot pose.  Prepending the
    // exact pose and then retaining that centre can create a short edge in the
    // opposite direction to the first real route edge.  Only discard the
    // leading backtrace points that are still in the start cell.  Once the
    // route has left it, every later topology point is retained even if a
    // future route legitimately enters the start cell again.
    bool left_start_cell = false;
    for (const auto & point : candidate.points) {
      if (!left_start_cell) {
        unsigned int point_mx = 0u;
        unsigned int point_my = 0u;
        const bool point_is_in_start_cell =
          snapshot.worldToMap(point.x, point.y, point_mx, point_my) &&
          point_mx == start_mx && point_my == start_my;
        if (point_is_in_start_cell) {
          continue;
        }
        left_start_cell = true;
      }
      appendDistinctPoint(exact_anchors, point.x, point.y);
    }
    appendDistinctPoint(exact_anchors, goal_wx, goal_wy);
    if (!densifyReference(
        exact_anchors, options.reference_spacing_m,
        options.max_reference_points, candidate.reference_points))
    {
      result.status = CoarseRouteStatus::NO_ROUTE;
      result.message = "coarse Theta reference exceeded its dense-pose safety bound";
      return result;
    }

    if (options.smooth_reference) {
      auto smoothed_anchors = downsampleReference(candidate.reference_points, 0.4);
      if (smoothed_anchors.size() > 5u) {
        applyHistoricMovingAverage(smoothed_anchors);
        std::vector<geometry_msgs::Point> smoothed;
        if (densifyReference(
            smoothed_anchors, options.reference_spacing_m,
            options.max_reference_points, smoothed) &&
          referenceCenterlineIsSafe(snapshot, theta, smoothed))
        {
          candidate.reference_points = std::move(smoothed);
        }
      }
    }
  }
  if (options.build_center_corridors) {
    buildCenterMasksForRoute(
      snapshot, candidate.points, candidate.center_masks, candidate.route_progress);
  }

  // The result container is intentionally future-proofed for at most three
  // independent candidates. Initial production wiring consumes one canonical
  // route; do not synthesize alternatives by changing any costmap cells.
  (void)std::clamp<std::size_t>(options.max_routes, 1u, kMaximumRoutes);
  result.routes.push_back(std::move(candidate));
  result.status = CoarseRouteStatus::SUCCESS;
  result.message = options.build_center_corridors ?
    "coarse Theta route and 1/2/4/8 m centre masks created read-only" :
    "coarse Theta route and dense reference created read-only";
  return result;
}

void CoarseRouteCorridorHelper::buildCenterMasksForRoute(
  const costmap_2d::Costmap2D & snapshot,
  const std::vector<geometry_msgs::Point> & route,
  std::vector<CenterCorridorMask> & masks,
  RouteProgressField & route_progress)
{
  buildCenterMasks(snapshot, route, masks, route_progress);
}

void CoarseRouteCorridorHelper::buildCenterMasks(
  const costmap_2d::Costmap2D & snapshot,
  const std::vector<geometry_msgs::Point> & route,
  std::vector<CenterCorridorMask> & masks,
  RouteProgressField & route_progress)
{
  masks.clear();
  route_progress = RouteProgressField();
  masks.reserve(kCorridorHalfWidthsM.size());
  for (const double half_width : kCorridorHalfWidthsM) {
    // Construct in the friend context before handing the value to vector's
    // allocator (the allocator itself is not a friend of CenterCorridorMask).
    masks.push_back(CenterCorridorMask(snapshot, half_width));
  }
  if (route.empty() || masks.empty()) {
    return;
  }

  route_progress = RouteProgressField(
    snapshot, route, kCorridorHalfWidthsM.back());

  const double resolution = snapshot.getResolution();
  const double origin_x = snapshot.getOriginX();
  const double origin_y = snapshot.getOriginY();
  const int size_x = static_cast<int>(snapshot.getSizeInCellsX());
  const int size_y = static_cast<int>(snapshot.getSizeInCellsY());
  const double maximum_width = kCorridorHalfWidthsM.back();
  std::array<double, kCorridorHalfWidthsM.size()> squared_widths{};
  for (std::size_t index = 0u; index < kCorridorHalfWidthsM.size(); ++index) {
    squared_widths[index] =
      kCorridorHalfWidthsM[index] * kCorridorHalfWidthsM[index];
  }

  std::vector<double> cumulative_arc_length(route.size(), 0.0);
  for (std::size_t index = 1u; index < route.size(); ++index) {
    cumulative_arc_length[index] = cumulative_arc_length[index - 1u] + std::hypot(
      route[index].x - route[index - 1u].x,
      route[index].y - route[index - 1u].y);
  }
  const double total_arc_length = cumulative_arc_length.back();

  const std::size_t segment_count = route.size() == 1u ? 1u : route.size() - 1u;
  for (std::size_t segment = 0u; segment < segment_count; ++segment) {
    const auto & first = route[segment];
    const auto & second = route.size() == 1u ? route.front() : route[segment + 1u];
    const double minimum_world_x = std::min(first.x, second.x) - maximum_width;
    const double maximum_world_x = std::max(first.x, second.x) + maximum_width;
    const double minimum_world_y = std::min(first.y, second.y) - maximum_width;
    const double maximum_world_y = std::max(first.y, second.y) + maximum_width;
    const int minimum_x = std::max(
      0, lowerCellForWorld(minimum_world_x, origin_x, resolution));
    const int maximum_x = std::min(
      size_x - 1, upperCellForWorld(maximum_world_x, origin_x, resolution));
    const int minimum_y = std::max(
      0, lowerCellForWorld(minimum_world_y, origin_y, resolution));
    const int maximum_y = std::min(
      size_y - 1, upperCellForWorld(maximum_world_y, origin_y, resolution));

    for (int my = minimum_y; my <= maximum_y; ++my) {
      const double wy = origin_y + (static_cast<double>(my) + 0.5) * resolution;
      for (int mx = minimum_x; mx <= maximum_x; ++mx) {
        const auto map_x = static_cast<unsigned int>(mx);
        const auto map_y = static_cast<unsigned int>(my);
        if (!isStateCenterTraversable(snapshot.getCost(map_x, map_y))) {
          continue;
        }
        const double wx = origin_x + (static_cast<double>(mx) + 0.5) * resolution;
        double projection_ratio = 0.0;
        const double distance_squared = pointSegmentDistanceSquared(
          wx, wy, first.x, first.y, second.x, second.y, &projection_ratio);
        for (std::size_t mask_index = 0u; mask_index < masks.size(); ++mask_index) {
          // A tiny scale-aware epsilon includes cells whose centres lie exactly
          // on the requested metric boundary despite floating-point rounding.
          const double epsilon =
            std::max(1e-12, squared_widths[mask_index] * 1e-12);
          if (distance_squared <= squared_widths[mask_index] + epsilon) {
            masks[mask_index].setAllowed(map_x, map_y);
          }
        }
        if (distance_squared <= squared_widths.back() +
          std::max(1e-12, squared_widths.back() * 1e-12))
        {
          const double segment_length = std::hypot(
            second.x - first.x, second.y - first.y);
          const double projection_arc_length =
            cumulative_arc_length[segment] + projection_ratio * segment_length;
          route_progress.updateIfNearer(
            map_x, map_y, static_cast<float>(distance_squared),
            static_cast<float>(std::max(0.0, total_arc_length - projection_arc_length)));
        }
      }
    }
  }
  route_progress.finishBuild();
}

}  // namespace smac_lattice_planner_mbf

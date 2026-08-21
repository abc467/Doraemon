// Copyright 2026 Clean Robot Navigation Team
// Licensed under the Apache License, Version 2.0.

#include <algorithm>
#include <cmath>
#include <cstring>
#include <limits>
#include <vector>

#include <gtest/gtest.h>

#include <costmap_2d/cost_values.h>
#include <costmap_2d/costmap_2d.h>

#include "smac_lattice_planner_mbf/coarse_route_corridor.hpp"

namespace smac_lattice_planner_mbf
{
namespace
{

TEST(CoarseRouteCorridor, IsReadOnlyAndBuildsMonotonicPackedMasks)
{
  constexpr unsigned int kWidth = 240u;
  constexpr unsigned int kHeight = 160u;
  constexpr double kResolution = 0.05;
  costmap_2d::Costmap2D snapshot(
    kWidth, kHeight, kResolution, 0.0, 0.0, costmap_2d::FREE_SPACE);

  // A lethal wall with one broad opening forces a nontrivial Theta* route.
  for (unsigned int y = 0u; y < kHeight; ++y) {
    if (y < 60u || y > 100u) {
      snapshot.setCost(120u, y, costmap_2d::LETHAL_OBSTACLE);
    }
  }
  // State-centre semantics must reject unknown and inscribed cells even when
  // they geometrically fall inside a corridor dilation.
  snapshot.setCost(30u, 30u, costmap_2d::NO_INFORMATION);
  snapshot.setCost(31u, 30u, costmap_2d::INSCRIBED_INFLATED_OBSTACLE);

  const std::size_t byte_count =
    static_cast<std::size_t>(kWidth) * static_cast<std::size_t>(kHeight);
  std::vector<unsigned char> before(snapshot.getCharMap(), snapshot.getCharMap() + byte_count);

  CoarseRouteCorridorOptions options;
  options.max_routes = 3u;  // Initial implementation remains a single safe route.
  const auto result = CoarseRouteCorridorHelper::build(
    snapshot, 1.0, 1.0, 11.0, 7.0, options);

  ASSERT_TRUE(result.succeeded()) << result.message;
  ASSERT_EQ(result.routes.size(), 1u);
  const auto & route = result.routes.front();
  ASSERT_GE(route.points.size(), 2u);
  ASSERT_EQ(route.center_masks.size(), 4u);
  EXPECT_GT(route.length_m, 0.0);
  EXPECT_GT(route.theta_nodes_opened, 0);
  EXPECT_EQ(
    std::memcmp(before.data(), snapshot.getCharMap(), byte_count), 0)
    << "coarse route helper modified its immutable source snapshot";

  for (std::size_t index = 0u; index < route.center_masks.size(); ++index) {
    EXPECT_DOUBLE_EQ(
      route.center_masks[index].halfWidthM(),
      CoarseRouteCorridorHelper::kCorridorHalfWidthsM[index]);
    EXPECT_GT(route.center_masks[index].allowedCellCount(), 0u);
  }

  // Every returned centre is traversable and belongs to all four masks.
  for (const auto & point : route.points) {
    unsigned int mx = 0u;
    unsigned int my = 0u;
    ASSERT_TRUE(snapshot.worldToMap(point.x, point.y, mx, my));
    EXPECT_TRUE(CoarseRouteCorridorHelper::isStateCenterTraversable(
      snapshot.getCost(mx, my)));
    for (const auto & mask : route.center_masks) {
      EXPECT_TRUE(mask.isAllowed(mx, my));
    }
  }

  EXPECT_FALSE(route.center_masks.back().isAllowed(30u, 30u));
  EXPECT_FALSE(route.center_masks.back().isAllowed(31u, 30u));
  EXPECT_FALSE(route.center_masks.back().isAllowed(120u, 10u));

  // The requested widths are nested search domains.
  for (unsigned int y = 0u; y < kHeight; ++y) {
    for (unsigned int x = 0u; x < kWidth; ++x) {
      for (std::size_t index = 1u; index < route.center_masks.size(); ++index) {
        if (route.center_masks[index - 1u].isAllowed(x, y)) {
          EXPECT_TRUE(route.center_masks[index].isAllowed(x, y));
        }
      }
    }
  }
}

TEST(CoarseRouteCorridor, PublicRouteApiBuildsNestedReadOnlyMasksForArbitrarySuffix)
{
  constexpr unsigned int kWidth = 320u;
  constexpr unsigned int kHeight = 240u;
  constexpr double kResolution = 0.10;
  costmap_2d::Costmap2D snapshot(
    kWidth, kHeight, kResolution, -2.0, -2.0, costmap_2d::FREE_SPACE);

  // The public API must use the original snapshot as a filter, not clear it
  // while dilating this caller-supplied (non-Theta-owned) suffix polyline.
  snapshot.setCost(80u, 80u, costmap_2d::LETHAL_OBSTACLE);
  snapshot.setCost(81u, 80u, costmap_2d::NO_INFORMATION);
  const std::size_t byte_count =
    static_cast<std::size_t>(kWidth) * static_cast<std::size_t>(kHeight);
  const std::vector<unsigned char> before(
    snapshot.getCharMap(), snapshot.getCharMap() + byte_count);

  const auto cellCentre = [&snapshot](unsigned int mx, unsigned int my) {
      geometry_msgs::Point point;
      snapshot.mapToWorld(mx, my, point.x, point.y);
      point.z = 0.0;
      return point;
    };
  const std::vector<geometry_msgs::Point> suffix{
    cellCentre(40u, 50u),
    cellCentre(140u, 50u),
    cellCentre(140u, 130u),
    cellCentre(240u, 130u)};

  std::vector<CenterCorridorMask> masks;
  RouteProgressField progress;
  CoarseRouteCorridorHelper::buildCenterMasksForRoute(
    snapshot, suffix, masks, progress);

  ASSERT_EQ(masks.size(), CoarseRouteCorridorHelper::kCorridorHalfWidthsM.size());
  EXPECT_EQ(
    std::memcmp(before.data(), snapshot.getCharMap(), byte_count), 0)
    << "public route-mask API modified its immutable source snapshot";
  EXPECT_EQ(progress.valuedCellCount(), masks.back().allowedCellCount());

  for (std::size_t index = 0u; index < masks.size(); ++index) {
    EXPECT_DOUBLE_EQ(
      masks[index].halfWidthM(),
      CoarseRouteCorridorHelper::kCorridorHalfWidthsM[index]);
    EXPECT_GT(masks[index].allowedCellCount(), 0u);
  }

  // Every supplied route vertex is a traversable centre and must belong to
  // every widening domain, including both sides of the right-angle turn.
  float previous_remaining = std::numeric_limits<float>::infinity();
  for (const auto & point : suffix) {
    unsigned int mx = 0u;
    unsigned int my = 0u;
    ASSERT_TRUE(snapshot.worldToMap(point.x, point.y, mx, my));
    for (const auto & mask : masks) {
      EXPECT_TRUE(mask.isAllowed(mx, my));
    }
    float remaining = 0.0f;
    ASSERT_TRUE(progress.tryRemainingArcLengthM(mx, my, remaining));
    EXPECT_LE(remaining, previous_remaining + 1e-4f);
    previous_remaining = remaining;
  }

  // The masks are nested cell-for-cell and keep blocked snapshot cells out.
  for (unsigned int y = 0u; y < kHeight; ++y) {
    for (unsigned int x = 0u; x < kWidth; ++x) {
      for (std::size_t index = 1u; index < masks.size(); ++index) {
        if (masks[index - 1u].isAllowed(x, y)) {
          EXPECT_TRUE(masks[index].isAllowed(x, y));
        }
      }
    }
  }
  EXPECT_FALSE(masks.back().isAllowed(80u, 80u));
  EXPECT_FALSE(masks.back().isAllowed(81u, 80u));
}

TEST(CoarseRouteCorridor, PublicRouteApiReplacesStaleProgressForEmptyRoute)
{
  costmap_2d::Costmap2D snapshot(
    100u, 100u, 0.10, 0.0, 0.0, costmap_2d::FREE_SPACE);
  geometry_msgs::Point point;
  snapshot.mapToWorld(50u, 50u, point.x, point.y);

  std::vector<CenterCorridorMask> masks;
  RouteProgressField progress;
  CoarseRouteCorridorHelper::buildCenterMasksForRoute(
    snapshot, std::vector<geometry_msgs::Point>{point}, masks, progress);
  ASSERT_GT(progress.valuedCellCount(), 0u);

  CoarseRouteCorridorHelper::buildCenterMasksForRoute(
    snapshot, std::vector<geometry_msgs::Point>{}, masks, progress);
  ASSERT_EQ(masks.size(), CoarseRouteCorridorHelper::kCorridorHalfWidthsM.size());
  for (const auto & mask : masks) {
    EXPECT_EQ(mask.allowedCellCount(), 0u);
  }
  EXPECT_EQ(progress.valuedCellCount(), 0u);
  EXPECT_EQ(progress.storedCellCapacity(), 0u);
}

TEST(CoarseRouteCorridor, RejectsStateBlockedEndpointsWithoutClearingThem)
{
  costmap_2d::Costmap2D snapshot(80u, 80u, 0.05, 0.0, 0.0, costmap_2d::FREE_SPACE);
  unsigned int goal_x = 0u;
  unsigned int goal_y = 0u;
  ASSERT_TRUE(snapshot.worldToMap(3.0, 3.0, goal_x, goal_y));
  snapshot.setCost(goal_x, goal_y, costmap_2d::INSCRIBED_INFLATED_OBSTACLE);

  const auto result = CoarseRouteCorridorHelper::build(snapshot, 0.5, 0.5, 3.0, 3.0);
  EXPECT_EQ(result.status, CoarseRouteStatus::INVALID_GOAL);
  EXPECT_TRUE(result.routes.empty());
  EXPECT_EQ(snapshot.getCost(goal_x, goal_y), costmap_2d::INSCRIBED_INFLATED_OBSTACLE);
}

TEST(CoarseRouteCorridor, RouteProgressIsMonotonicFromStartToGoal)
{
  constexpr double kResolution = 0.05;
  costmap_2d::Costmap2D snapshot(
    300u, 100u, kResolution, 0.0, 0.0, costmap_2d::FREE_SPACE);

  const auto result = CoarseRouteCorridorHelper::build(
    snapshot, 1.025, 2.525, 12.025, 2.525);
  ASSERT_TRUE(result.succeeded()) << result.message;
  const auto & route = result.routes.front();
  ASSERT_GE(route.points.size(), 2u);
  EXPECT_GT(route.route_progress.storedCellCapacity(), 0u);
  EXPECT_EQ(
    route.route_progress.valuedCellCount(),
    route.center_masks.back().allowedCellCount());

  float previous_remaining = std::numeric_limits<float>::infinity();
  for (const auto & point : route.points) {
    float remaining = 0.0f;
    ASSERT_TRUE(route.route_progress.tryRemainingArcLengthWorld(
      point.x, point.y, remaining));
    EXPECT_LE(remaining, previous_remaining + 1e-4f);
    previous_remaining = remaining;
  }

  float start_remaining = 0.0f;
  float goal_remaining = 0.0f;
  ASSERT_TRUE(route.route_progress.tryRemainingArcLengthWorld(
    route.points.front().x, route.points.front().y, start_remaining));
  ASSERT_TRUE(route.route_progress.tryRemainingArcLengthWorld(
    route.points.back().x, route.points.back().y, goal_remaining));
  EXPECT_NEAR(start_remaining, route.length_m, kResolution);
  EXPECT_NEAR(goal_remaining, 0.0, kResolution);
  EXPECT_GT(start_remaining, goal_remaining);

  float outside_value = 0.0f;
  EXPECT_FALSE(route.route_progress.tryRemainingArcLengthM(
    snapshot.getSizeInCellsX(), snapshot.getSizeInCellsY(),
    outside_value));
}

TEST(CoarseRouteCorridor, BuildsDenseExactEndpointReferenceForCompositeMode)
{
  constexpr double kResolution = 0.05;
  costmap_2d::Costmap2D snapshot(
    260u, 80u, kResolution, 0.0, 0.0, costmap_2d::FREE_SPACE);
  CoarseRouteCorridorOptions options;
  options.theta_allow_unknown = false;
  options.theta_max_allowed_cost = 26;
  options.theta_w_traversal_cost = 8.0;
  options.theta_w_euc_cost = 2.0;
  options.theta_w_heuristic_cost = 1.0;
  options.reference_spacing_m = kResolution;
  options.smooth_reference = true;
  options.build_center_corridors = false;

  constexpr double kStartX = 0.113;
  constexpr double kStartY = 1.217;
  constexpr double kGoalX = 11.931;
  constexpr double kGoalY = 1.217;
  const auto result = CoarseRouteCorridorHelper::build(
    snapshot, kStartX, kStartY, kGoalX, kGoalY, options);

  ASSERT_TRUE(result.succeeded()) << result.message;
  ASSERT_EQ(result.routes.size(), 1u);
  const auto & route = result.routes.front();
  ASSERT_GT(route.reference_points.size(), 100u);
  EXPECT_TRUE(route.center_masks.empty());
  EXPECT_NEAR(route.reference_points.front().x, kStartX, 1e-12);
  EXPECT_NEAR(route.reference_points.front().y, kStartY, 1e-12);
  EXPECT_NEAR(route.reference_points.back().x, kGoalX, 1e-12);
  EXPECT_NEAR(route.reference_points.back().y, kGoalY, 1e-12);
  for (std::size_t index = 1u; index < route.reference_points.size(); ++index) {
    EXPECT_LE(
      std::hypot(
        route.reference_points[index].x - route.reference_points[index - 1u].x,
        route.reference_points[index].y - route.reference_points[index - 1u].y),
      kResolution + 1e-9);
  }
}

TEST(CoarseRouteCorridor, DropsLeadingStartCellCentreThatPointsBehindRealDeparture)
{
  constexpr unsigned int kWidth = 140u;
  constexpr unsigned int kHeight = 60u;
  constexpr double kResolution = 0.05;
  constexpr unsigned int kStartCellX = 10u;
  constexpr unsigned int kStartCellY = 30u;
  costmap_2d::Costmap2D snapshot(
    kWidth, kHeight, kResolution, 0.0, 0.0, costmap_2d::FREE_SPACE);

  double start_cell_center_x = 0.0;
  double start_cell_center_y = 0.0;
  snapshot.mapToWorld(
    kStartCellX, kStartCellY, start_cell_center_x, start_cell_center_y);

  // Reproduce the field geometry: the exact pose lies 1.54 cm east of its
  // cell centre while the real Theta departure is east.  Retaining Theta's
  // leading cell-centre backtrace would manufacture a short westbound stub.
  constexpr double kFieldStubLength = 0.015396269552871307;
  const double exact_start_x = start_cell_center_x + kFieldStubLength;
  const double exact_start_y = start_cell_center_y;
  double exact_goal_x = 0.0;
  double exact_goal_y = 0.0;
  snapshot.mapToWorld(120u, kStartCellY, exact_goal_x, exact_goal_y);

  const std::size_t byte_count =
    static_cast<std::size_t>(kWidth) * static_cast<std::size_t>(kHeight);
  const std::vector<unsigned char> before(
    snapshot.getCharMap(), snapshot.getCharMap() + byte_count);

  CoarseRouteCorridorOptions options;
  options.theta_max_allowed_cost = 26;
  options.reference_spacing_m = kResolution;
  // Exercise the production composite setting as well: smoothing preserves
  // the exact endpoint, but must not recreate the removed start-cell stub.
  options.smooth_reference = true;
  options.build_center_corridors = false;
  const auto result = CoarseRouteCorridorHelper::build(
    snapshot, exact_start_x, exact_start_y,
    exact_goal_x, exact_goal_y, options);

  ASSERT_TRUE(result.succeeded()) << result.message;
  ASSERT_EQ(result.routes.size(), 1u);
  const auto & reference = result.routes.front().reference_points;
  ASSERT_GE(reference.size(), 2u);
  EXPECT_NEAR(reference.front().x, exact_start_x, 1e-12);
  EXPECT_NEAR(reference.front().y, exact_start_y, 1e-12);
  EXPECT_NEAR(reference.back().x, exact_goal_x, 1e-12);
  EXPECT_NEAR(reference.back().y, exact_goal_y, 1e-12);

  const double departure_dx = reference[1].x - reference[0].x;
  const double departure_dy = reference[1].y - reference[0].y;
  ASSERT_GT(std::hypot(departure_dx, departure_dy), 1e-9);
  EXPECT_GT(departure_dx, 0.0);
  EXPECT_NEAR(departure_dy, 0.0, 1e-12);
  EXPECT_NEAR(std::atan2(departure_dy, departure_dx), 0.0, 1e-12);
  EXPECT_GT(
    std::hypot(
      reference[1].x - start_cell_center_x,
      reference[1].y - start_cell_center_y),
    kFieldStubLength);
  EXPECT_EQ(
    std::memcmp(before.data(), snapshot.getCharMap(), byte_count), 0)
    << "coarse reference cleanup modified its immutable source snapshot";
}

TEST(CoarseRouteCorridor, PreservesDistinctExactGoalInsideStartCell)
{
  constexpr double kResolution = 0.05;
  constexpr unsigned int kCellX = 10u;
  constexpr unsigned int kCellY = 10u;
  costmap_2d::Costmap2D snapshot(
    40u, 40u, kResolution, 0.0, 0.0, costmap_2d::FREE_SPACE);

  double center_x = 0.0;
  double center_y = 0.0;
  snapshot.mapToWorld(kCellX, kCellY, center_x, center_y);
  const double exact_start_x = center_x + 0.015;
  const double exact_start_y = center_y;
  // The raw Theta start/goal cell centre is deliberately the exact requested
  // goal. It is removed as a leading raw point and must then be re-appended as
  // the exact goal rather than disappearing with the start-cell cleanup.
  const double exact_goal_x = center_x;
  const double exact_goal_y = center_y;

  CoarseRouteCorridorOptions options;
  options.reference_spacing_m = kResolution;
  options.smooth_reference = false;
  options.build_center_corridors = false;
  const auto result = CoarseRouteCorridorHelper::build(
    snapshot, exact_start_x, exact_start_y,
    exact_goal_x, exact_goal_y, options);

  ASSERT_TRUE(result.succeeded()) << result.message;
  ASSERT_EQ(result.routes.size(), 1u);
  const auto & reference = result.routes.front().reference_points;
  ASSERT_EQ(reference.size(), 2u);
  EXPECT_NEAR(reference.front().x, exact_start_x, 1e-12);
  EXPECT_NEAR(reference.front().y, exact_start_y, 1e-12);
  EXPECT_NEAR(reference.back().x, exact_goal_x, 1e-12);
  EXPECT_NEAR(reference.back().y, exact_goal_y, 1e-12);
  EXPECT_LT(reference.back().x - reference.front().x, 0.0);
}

TEST(CoarseRouteCorridor, PreservesCoincidentExactStartAndGoal)
{
  constexpr double kResolution = 0.05;
  costmap_2d::Costmap2D snapshot(
    40u, 40u, kResolution, 0.0, 0.0, costmap_2d::FREE_SPACE);
  constexpr double kExactX = 0.537;
  constexpr double kExactY = 0.519;

  CoarseRouteCorridorOptions options;
  options.reference_spacing_m = kResolution;
  options.smooth_reference = false;
  options.build_center_corridors = false;
  const auto result = CoarseRouteCorridorHelper::build(
    snapshot, kExactX, kExactY, kExactX, kExactY, options);

  ASSERT_TRUE(result.succeeded()) << result.message;
  ASSERT_EQ(result.routes.size(), 1u);
  const auto & reference = result.routes.front().reference_points;
  ASSERT_EQ(reference.size(), 1u);
  EXPECT_NEAR(reference.front().x, kExactX, 1e-12);
  EXPECT_NEAR(reference.front().y, kExactY, 1e-12);
}

TEST(CoarseRouteCorridor, CompositeClearanceRejectsGoalAboveConfiguredCost)
{
  costmap_2d::Costmap2D snapshot(
    100u, 100u, 0.05, 0.0, 0.0, costmap_2d::FREE_SPACE);
  unsigned int goal_x = 0u;
  unsigned int goal_y = 0u;
  ASSERT_TRUE(snapshot.worldToMap(4.0, 4.0, goal_x, goal_y));
  snapshot.setCost(goal_x, goal_y, 27u);

  CoarseRouteCorridorOptions options;
  options.theta_allow_unknown = false;
  options.theta_max_allowed_cost = 26;
  options.reference_spacing_m = 0.05;
  options.build_center_corridors = false;
  const auto result = CoarseRouteCorridorHelper::build(
    snapshot, 0.5, 0.5, 4.0, 4.0, options);
  EXPECT_EQ(result.status, CoarseRouteStatus::INVALID_GOAL);
  EXPECT_TRUE(result.routes.empty());
  EXPECT_EQ(snapshot.getCost(goal_x, goal_y), 27u);
}

TEST(CoarseRouteCorridor, CompositeAllowsMonotonicEscapeFromAHighCostStart)
{
  costmap_2d::Costmap2D snapshot(
    120u, 60u, 0.05, 0.0, 0.0, costmap_2d::FREE_SPACE);
  constexpr unsigned int kStartX = 10u;
  constexpr unsigned int kStartY = 30u;
  snapshot.setCost(kStartX, kStartY, 100u);
  snapshot.setCost(kStartX + 1u, kStartY, 80u);
  snapshot.setCost(kStartX + 2u, kStartY, 40u);
  snapshot.setCost(kStartX + 3u, kStartY, 20u);

  CoarseRouteCorridorOptions options;
  options.theta_allow_unknown = false;
  options.theta_max_allowed_cost = 26;
  options.theta_w_traversal_cost = 8.0;
  options.theta_w_euc_cost = 2.0;
  options.theta_w_heuristic_cost = 1.0;
  options.reference_spacing_m = 0.05;
  options.build_center_corridors = false;
  const double start_wx = (static_cast<double>(kStartX) + 0.5) * 0.05;
  const double start_wy = (static_cast<double>(kStartY) + 0.5) * 0.05;
  const auto result = CoarseRouteCorridorHelper::build(
    snapshot, start_wx, start_wy, 5.025, start_wy, options);

  ASSERT_TRUE(result.succeeded()) << result.message;
  ASSERT_FALSE(result.routes.front().reference_points.empty());
  EXPECT_NEAR(result.routes.front().reference_points.front().x, start_wx, 1e-12);
  EXPECT_EQ(snapshot.getCost(kStartX, kStartY), 100u);
}

}  // namespace
}  // namespace smac_lattice_planner_mbf

int main(int argc, char ** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

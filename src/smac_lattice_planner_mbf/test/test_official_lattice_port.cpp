// Copyright 2026 Clean Robot Navigation Team
// Licensed under the Apache License, Version 2.0.

#include <algorithm>
#include <cmath>
#include <limits>
#include <memory>
#include <string>

#include <angles/angles.h>
#include <gtest/gtest.h>
#include <ros/package.h>

#include "nav2_smac_planner/a_star.hpp"
#include "nav2_smac_planner/collision_checker.hpp"
#include "nav2_smac_planner/distance_heuristic.hpp"
#include "nav2_smac_planner/node_basic.hpp"
#include "nav2_smac_planner/node_lattice.hpp"
#include "smac_lattice_planner_mbf/live_validator_support.hpp"

namespace
{
nav2_smac_planner::Footprint robotFootprint()
{
  nav2_smac_planner::Footprint footprint(4);
  footprint[0].x = -0.2185;
  footprint[0].y = -0.325;
  footprint[1].x = -0.2185;
  footprint[1].y = 0.325;
  footprint[2].x = 0.6315;
  footprint[2].y = 0.325;
  footprint[3].x = 0.6315;
  footprint[3].y = -0.325;
  return footprint;
}

void setLethal(costmap_2d::Costmap2D & costmap, double wx, double wy)
{
  unsigned int mx = 0;
  unsigned int my = 0;
  ASSERT_TRUE(costmap.worldToMap(wx, wy, mx, my));
  costmap.setCost(mx, my, costmap_2d::LETHAL_OBSTACLE);
}

class LatticeAStarHeuristicProbe
  : public nav2_smac_planner::AStarAlgorithm<nav2_smac_planner::NodeLattice>
{
public:
  using Base = nav2_smac_planner::AStarAlgorithm<nav2_smac_planner::NodeLattice>;
  using Base::Base;

  float heuristicAtStart()
  {
    return this->getHeuristicCost(this->getStart());
  }

  void resetBestAnchor()
  {
    this->_best_heuristic_node = {std::numeric_limits<float>::max(), 0u};
  }

  float bestAnchor() const
  {
    return this->_best_heuristic_node.first;
  }

  nav2_smac_planner::NodeLattice * singleGoal()
  {
    auto & goals = this->_goal_manager.getGoalsState();
    return goals.size() == 1u ? goals.front().goal : nullptr;
  }

  bool inputsAreValid()
  {
    return this->areInputsValid();
  }

};

bool containsReverseTranslation(
  const nav2_smac_planner::NodeLattice::CoordinateVector & reverse_path)
{
  for (std::size_t index = reverse_path.size(); index > 1u; --index) {
    const auto & start = reverse_path[index - 1u];
    const auto & end = reverse_path[index - 2u];
    const float dx = end.x - start.x;
    const float dy = end.y - start.y;
    const float distance = std::hypot(dx, dy);
    if (distance <= 1e-4f) {
      continue;
    }
    const float start_projection =
      (dx * std::cos(start.theta) + dy * std::sin(start.theta)) / distance;
    const float end_projection =
      (dx * std::cos(end.theta) + dy * std::sin(end.theta)) / distance;
    const float projection =
      std::abs(start_projection) >= std::abs(end_projection) ?
      start_projection : end_projection;
    if (projection < -0.25f) {
      return true;
    }
  }
  return false;
}
}  // namespace

TEST(StateLatticeDistanceHeuristic, MirroredZeroHeadingWrapsToZero)
{
  EXPECT_EQ(nav2_smac_planner::mirrorAngleBin(0u, 32u), 0u);
  EXPECT_EQ(nav2_smac_planner::mirrorAngleBin(1u, 32u), 31u);
  EXPECT_EQ(nav2_smac_planner::mirrorAngleBin(31u, 32u), 1u);
}

TEST(StateLatticeTraversalCost, FirstEdgePaysSoftAndReversePenalties)
{
  nav2_smac_planner::NodeLattice::NodeContext context;
  context.motion_table.lattice_metadata.grid_resolution = 0.05f;
  context.motion_table.travel_distance_reward = 1.0f;
  context.motion_table.cost_penalty = 5.0f;
  context.motion_table.reverse_penalty = 4.0f;
  context.motion_table.use_quadratic_cost_penalty = false;

  nav2_smac_planner::NodeLattice start(1u, &context);
  nav2_smac_planner::NodeLattice child(2u, &context);
  nav2_smac_planner::MotionPrimitive first_edge;
  first_edge.trajectory_length = 0.50f;
  first_edge.arc_length = 0.0f;
  child.setMotionPrimitive(&first_edge);
  child.setCost(126.0f);
  child.backwards(true);

  const float edge_cells = 0.50f / 0.05f;
  const float normalized_cost = 126.0f / 252.0f;
  const float expected = edge_cells * (1.0f + 5.0f * normalized_cost) * 4.0f;
  EXPECT_FLOAT_EQ(start.getTraversalCost(&child), expected);
}

TEST(OfficialGridCollisionChecker, DetectsCornerSweepDuringPureRotation)
{
  costmap_2d::Costmap2D costmap(200, 200, 0.05, -5.0, -5.0, costmap_2d::FREE_SPACE);
  nav2_smac_planner::GridCollisionChecker checker(&costmap, 72u);
  checker.setFootprint(robotFootprint(), false, 0.0);
  checker.setCollisionCheckResolution(0.01);

  // This cell is outside the rectangular footprint at both endpoint yaws,
  // but the front-right corner crosses it near yaw=45 degrees.
  setLethal(costmap, 0.675, 0.225);
  const float map_x = static_cast<float>((0.0 - costmap.getOriginX()) / costmap.getResolution());
  const float map_y = static_cast<float>((0.0 - costmap.getOriginY()) / costmap.getResolution());

  EXPECT_FALSE(checker.inCollisionAtYaw(map_x, map_y, 0.0, false));
  EXPECT_FALSE(checker.inCollisionAtYaw(map_x, map_y, M_PI_2, false));
  EXPECT_TRUE(checker.inCollisionContinuous(
    map_x, map_y, 0.0, map_x, map_y, M_PI_2, false));
}

TEST(OfficialGridCollisionChecker, RejectsLethalCellInsideFilledRectangle)
{
  costmap_2d::Costmap2D costmap(200, 200, 0.05, -5.0, -5.0, costmap_2d::FREE_SPACE);
  // This obstacle is strictly inside the footprint: it is not on the outline
  // and it is not the robot centre. Outline-only CostmapModel checks miss it.
  setLethal(costmap, 0.30, 0.0);
  nav2_smac_planner::GridCollisionChecker refreshed(&costmap, 72u);
  refreshed.setFootprint(robotFootprint(), false, 0.0);
  refreshed.setCollisionCheckResolution(0.01);
  const float map_x = static_cast<float>((0.0 - costmap.getOriginX()) / costmap.getResolution());
  const float map_y = static_cast<float>((0.0 - costmap.getOriginY()) / costmap.getResolution());

  // Lethal cells are hard obstacles under both unknown-space policies.  This
  // specifically exercises the filled interior, where an outline-only check
  // would miss the obstacle and where both prefix variants must fall back to
  // the exact polygon scan.
  EXPECT_TRUE(refreshed.inCollisionAtYaw(map_x, map_y, 0.0, false));
  EXPECT_TRUE(refreshed.inCollisionAtYaw(map_x, map_y, 0.0, true));
}

TEST(OfficialGridCollisionChecker, DoesNotRejectHardCellInAabbOutsideRotatedFootprint)
{
  constexpr double yaw = M_PI_4;
  const auto check_outside_cell = [yaw](unsigned char cost)
    {
      costmap_2d::Costmap2D costmap(
        200, 200, 0.05, -5.0, -5.0, costmap_2d::FREE_SPACE);
      unsigned int obstacle_x = 0u;
      unsigned int obstacle_y = 0u;
      // At yaw=45 degrees the footprint AABB reaches about (0.676, 0.676),
      // while the front polygon edge is near x+y=0.893.  The centre of this
      // cell is therefore inside the AABB (and its conservative halo) but
      // strictly outside the actual rotated rectangle.
      ASSERT_TRUE(costmap.worldToMap(0.60, 0.60, obstacle_x, obstacle_y));
      costmap.setCost(obstacle_x, obstacle_y, cost);

      const double obstacle_wx = costmap.getOriginX() +
        (static_cast<double>(obstacle_x) + 0.5) * costmap.getResolution();
      const double obstacle_wy = costmap.getOriginY() +
        (static_cast<double>(obstacle_y) + 0.5) * costmap.getResolution();
      double minimum_x = std::numeric_limits<double>::infinity();
      double minimum_y = std::numeric_limits<double>::infinity();
      double maximum_x = -std::numeric_limits<double>::infinity();
      double maximum_y = -std::numeric_limits<double>::infinity();
      for (const auto & point : robotFootprint()) {
        const double rotated_x = point.x * std::cos(yaw) - point.y * std::sin(yaw);
        const double rotated_y = point.x * std::sin(yaw) + point.y * std::cos(yaw);
        minimum_x = std::min(minimum_x, rotated_x);
        minimum_y = std::min(minimum_y, rotated_y);
        maximum_x = std::max(maximum_x, rotated_x);
        maximum_y = std::max(maximum_y, rotated_y);
      }
      EXPECT_GE(obstacle_wx, minimum_x);
      EXPECT_LE(obstacle_wx, maximum_x);
      EXPECT_GE(obstacle_wy, minimum_y);
      EXPECT_LE(obstacle_wy, maximum_y);
      // Inverse rotation puts the cell beyond the footprint's front edge,
      // proving that it is not merely assumed to lie outside the polygon.
      const double obstacle_body_x =
        obstacle_wx * std::cos(yaw) + obstacle_wy * std::sin(yaw);
      EXPECT_GT(obstacle_body_x, 0.6315);

      nav2_smac_planner::GridCollisionChecker checker(&costmap, 72u);
      checker.setFootprint(robotFootprint(), false, 0.0);
      checker.setCollisionCheckResolution(0.01);
      const float map_x = static_cast<float>(
        (0.0 - costmap.getOriginX()) / costmap.getResolution());
      const float map_y = static_cast<float>(
        (0.0 - costmap.getOriginY()) / costmap.getResolution());

      // A positive prefix query is only a reason to run the exact scan.  It
      // must never by itself turn the AABB into the robot footprint.
      EXPECT_FALSE(checker.inCollisionAtYaw(map_x, map_y, yaw, false));
      EXPECT_FALSE(checker.inCollisionAtYaw(map_x, map_y, yaw, true));
    };

  check_outside_cell(costmap_2d::LETHAL_OBSTACLE);
  check_outside_cell(costmap_2d::NO_INFORMATION);
}

TEST(OfficialGridCollisionChecker, KeepsInteriorSoftInflationTraversable)
{
  costmap_2d::Costmap2D costmap(200, 200, 0.05, -5.0, -5.0, costmap_2d::FREE_SPACE);
  costmap.setCost(106u, 100u, costmap_2d::INSCRIBED_INFLATED_OBSTACLE);
  nav2_smac_planner::GridCollisionChecker checker(&costmap, 72u);
  checker.setFootprint(robotFootprint(), false, 0.0);
  checker.setCollisionCheckResolution(0.01);
  const float map_x = static_cast<float>((0.0 - costmap.getOriginX()) / costmap.getResolution());
  const float map_y = static_cast<float>((0.0 - costmap.getOriginY()) / costmap.getResolution());

  // Only raw lethal cells are hard obstacles in the polygon interior. The
  // centre and outline retain Nav2's inscribed-cost behavior.
  EXPECT_FALSE(checker.inCollisionAtYaw(map_x, map_y, 0.0, false));
}

TEST(OfficialGridCollisionChecker, ReportsSoftCostAtAnOffsetFrontCorner)
{
  costmap_2d::Costmap2D costmap(200, 200, 0.05, -5.0, -5.0, costmap_2d::FREE_SPACE);
  // The centre remains free while the front-right footprint corner lies in
  // this inflated cell. It must bias traversal cost without becoming a hard
  // collision.
  unsigned int mx = 0u;
  unsigned int my = 0u;
  ASSERT_TRUE(costmap.worldToMap(0.625, 0.325, mx, my));
  costmap.setCost(mx, my, 200u);
  nav2_smac_planner::GridCollisionChecker checker(&costmap, 72u);
  checker.setFootprint(robotFootprint(), false, 0.0);
  const float map_x = static_cast<float>((0.0 - costmap.getOriginX()) / costmap.getResolution());
  const float map_y = static_cast<float>((0.0 - costmap.getOriginY()) / costmap.getResolution());

  EXPECT_FALSE(checker.inCollisionAtYaw(map_x, map_y, 0.0, false));
  EXPECT_FLOAT_EQ(checker.getCost(), 200.0f);

  // This is the exact value NodeLattice snapshots onto the candidate edge for
  // A* traversal cost, rather than a post-search-only collision diagnostic.
  nav2_smac_planner::NodeLattice::NodeContext context;
  context.motion_table.lattice_metadata.heading_angles = {0.0f};
  nav2_smac_planner::NodeLattice node(1u, &context);
  node.setPose(nav2_smac_planner::NodeLattice::Coordinates(map_x, map_y, 0.0f));
  EXPECT_TRUE(node.isNodeValid(false, &checker));
  EXPECT_FLOAT_EQ(node.getCost(), 200.0f);
}

TEST(OfficialGridCollisionChecker, RespectsUnknownTraversalInsideFilledRectangle)
{
  costmap_2d::Costmap2D costmap(200, 200, 0.05, -5.0, -5.0, costmap_2d::FREE_SPACE);
  costmap.setCost(106u, 100u, costmap_2d::NO_INFORMATION);
  nav2_smac_planner::GridCollisionChecker checker(&costmap, 72u);
  checker.setFootprint(robotFootprint(), false, 0.0);
  checker.setCollisionCheckResolution(0.01);
  const float map_x = static_cast<float>((0.0 - costmap.getOriginX()) / costmap.getResolution());
  const float map_y = static_cast<float>((0.0 - costmap.getOriginY()) / costmap.getResolution());

  EXPECT_TRUE(checker.inCollisionAtYaw(map_x, map_y, 0.0, false));
  EXPECT_FALSE(checker.inCollisionAtYaw(map_x, map_y, 0.0, true));
}

TEST(OfficialGridCollisionChecker, SnapshotDoesNotObserveLaterLiveMapMutation)
{
  costmap_2d::Costmap2D live(200, 200, 0.05, -5.0, -5.0, costmap_2d::FREE_SPACE);
  costmap_2d::Costmap2D snapshot(live);
  setLethal(live, 0.30, 0.0);
  nav2_smac_planner::GridCollisionChecker snapshot_checker(&snapshot, 72u);
  snapshot_checker.setFootprint(robotFootprint(), false, 0.0);
  nav2_smac_planner::GridCollisionChecker live_checker(&live, 72u);
  live_checker.setFootprint(robotFootprint(), false, 0.0);
  const float map_x = static_cast<float>((0.0 - live.getOriginX()) / live.getResolution());
  const float map_y = static_cast<float>((0.0 - live.getOriginY()) / live.getResolution());

  EXPECT_FALSE(snapshot_checker.inCollisionAtYaw(map_x, map_y, 0.0, false));
  EXPECT_TRUE(live_checker.inCollisionAtYaw(map_x, map_y, 0.0, false));
}

TEST(StateLatticeEdgeValidation, RechecksEveryPrimitiveEnteringCachedNode)
{
  costmap_2d::Costmap2D costmap(100, 100, 0.05, 0.0, 0.0, costmap_2d::FREE_SPACE);
  nav2_smac_planner::GridCollisionChecker checker(&costmap, 1u);
  checker.setFootprint({}, true, 0.0);
  checker.setCollisionCheckResolution(0.01);

  nav2_smac_planner::NodeLattice::NodeContext context;
  context.motion_table.lattice_metadata.grid_resolution = 0.05f;
  context.motion_table.lattice_metadata.heading_angles = {0.0f};

  nav2_smac_planner::NodeLattice target(1u, &context);
  target.setPose(nav2_smac_planner::NodeLattice::Coordinates(50.0f, 50.0f, 0.0f));

  nav2_smac_planner::MotionPrimitive safe;
  safe.start_angle = 0u;
  safe.end_angle = 0u;
  safe.poses.emplace_back(
    0.25f, -0.50f, 0.0f, nav2_smac_planner::TurnDirection::FORWARD);
  safe.poses.emplace_back(
    0.50f, 0.0f, 0.0f, nav2_smac_planner::TurnDirection::FORWARD);

  nav2_smac_planner::MotionPrimitive blocked = safe;
  blocked.poses[0]._y = 0.50f;
  setLethal(costmap, 2.25, 3.00);

  // Both primitives terminate at the exact same discrete/continuous node.
  // Only the second incoming edge crosses the lethal cell. The upstream node
  // cache used to return the first edge's result without checking the second.
  EXPECT_TRUE(target.isNodeValid(false, &checker, &safe, false));
  EXPECT_FALSE(target.isNodeValid(false, &checker, &blocked, false));
}

TEST(StateLatticeEdgeValidation, RechecksReversePrimitiveEnteringCachedNode)
{
  costmap_2d::Costmap2D costmap(100, 100, 0.05, 0.0, 0.0, costmap_2d::FREE_SPACE);
  nav2_smac_planner::GridCollisionChecker checker(&costmap, 2u);
  checker.setFootprint({}, true, 0.0);
  checker.setCollisionCheckResolution(0.01);

  nav2_smac_planner::NodeLattice::NodeContext context;
  context.motion_table.lattice_metadata.grid_resolution = 0.05f;
  context.motion_table.lattice_metadata.heading_angles = {0.0f, static_cast<float>(M_PI)};

  nav2_smac_planner::NodeLattice target(1u, &context);
  target.setPose(nav2_smac_planner::NodeLattice::Coordinates(50.0f, 50.0f, 1.0f));

  nav2_smac_planner::MotionPrimitive safe_forward;
  safe_forward.start_angle = 1u;
  safe_forward.end_angle = 1u;
  safe_forward.poses.emplace_back(
    0.25f, -0.50f, static_cast<float>(M_PI),
    nav2_smac_planner::TurnDirection::FORWARD);
  safe_forward.poses.emplace_back(
    0.50f, 0.0f, static_cast<float>(M_PI),
    nav2_smac_planner::TurnDirection::FORWARD);

  nav2_smac_planner::MotionPrimitive blocked_reverse;
  blocked_reverse.start_angle = 0u;
  blocked_reverse.end_angle = 0u;
  blocked_reverse.poses.emplace_back(
    0.25f, 0.50f, 0.0f, nav2_smac_planner::TurnDirection::FORWARD);
  blocked_reverse.poses.emplace_back(
    0.50f, 0.0f, 0.0f, nav2_smac_planner::TurnDirection::FORWARD);
  setLethal(costmap, 2.25, 3.00);

  // A reverse primitive mirrors its body heading by pi, but its swept edge
  // must still be checked independently after a safe forward edge populated
  // the same discrete target node.
  EXPECT_TRUE(target.isNodeValid(false, &checker, &safe_forward, false));
  EXPECT_FALSE(target.isNodeValid(false, &checker, &blocked_reverse, true));
}

TEST(StateLatticeQueueState, RestoresParentPrimitiveAndSweptCostAtomically)
{
  nav2_smac_planner::NodeLattice::NodeContext context;
  nav2_smac_planner::NodeLattice parent_a(1u, &context);
  nav2_smac_planner::NodeLattice parent_b(2u, &context);
  nav2_smac_planner::NodeLattice child(3u, &context);
  nav2_smac_planner::MotionPrimitive primitive_a;
  nav2_smac_planner::MotionPrimitive primitive_b;

  child.pose = nav2_smac_planner::NodeLattice::Coordinates(10.25f, 20.50f, 3.0f);
  child.parent = &parent_a;
  child.setMotionPrimitive(&primitive_a);
  child.setCost(17.0f);
  child.backwards(false);

  nav2_smac_planner::NodeBasic<nav2_smac_planner::NodeLattice> queued(child.getIndex());
  auto child_ptr = &child;
  queued.populateSearchNode(child_ptr);

  child.pose = nav2_smac_planner::NodeLattice::Coordinates(30.0f, 40.0f, 7.0f);
  child.parent = &parent_b;
  child.setMotionPrimitive(&primitive_b);
  child.setCost(211.0f);
  child.backwards(true);

  queued.processSearchNode();
  EXPECT_FLOAT_EQ(child.pose.x, 10.25f);
  EXPECT_FLOAT_EQ(child.pose.y, 20.50f);
  EXPECT_FLOAT_EQ(child.pose.theta, 3.0f);
  EXPECT_EQ(child.parent, &parent_a);
  EXPECT_EQ(child.getMotionPrimitive(), &primitive_a);
  EXPECT_FLOAT_EQ(child.getCost(), 17.0f);
  EXPECT_FALSE(child.isBackward());
}

TEST(OfficialStateLattice, PlansWithGeneratedDifferentialDrivePrimitives)
{
  const std::string lattice = ros::package::getPath("smac_lattice_planner_mbf") +
    "/config/diff_5cm_0p40m_32bins.json";
  const auto metadata =
    nav2_smac_planner::LatticeMotionTable::getLatticeMetadata(lattice);
  ASSERT_EQ(metadata.motion_model, "diff");
  ASSERT_EQ(metadata.number_of_headings, 32u);
  ASSERT_NEAR(metadata.grid_resolution, 0.05, 1e-6);

  costmap_2d::Costmap2D costmap(100, 100, 0.05, 0.0, 0.0, costmap_2d::FREE_SPACE);
  nav2_smac_planner::GridCollisionChecker checker(&costmap, 72u);
  checker.setFootprint(robotFootprint(), false, 0.0);
  checker.setCollisionCheckResolution(0.01);

  nav2_smac_planner::SearchInfo search_info;
  search_info.lattice_filepath = lattice;
  search_info.minimum_turning_radius = metadata.min_turning_radius / costmap.getResolution();
  search_info.allow_reverse_expansion = true;
  search_info.analytic_expansion_max_length = 3.0 / costmap.getResolution();

  int max_iterations = 100000;
  nav2_smac_planner::AStarAlgorithm<nav2_smac_planner::NodeLattice> planner(
    nav2_smac_planner::MotionModel::STATE_LATTICE, search_info);
  planner.initialize(true, max_iterations, 1000, 100, 5.0, 101.0, 32u);
  planner.setCollisionChecker(&checker);
  planner.setStart(20.0f, 50.0f, 0u);
  planner.setGoal(70.0f, 50.0f, 0u);

  nav2_smac_planner::NodeLattice::CoordinateVector path;
  int iterations = 0;
  ASSERT_TRUE(planner.createPath(path, iterations, 0.0f, []() {return false;}));
  ASSERT_GT(path.size(), 2u);

  for (std::size_t i = path.size() - 1; i > 0; --i) {
    EXPECT_FALSE(checker.inCollisionContinuous(
      path[i].x, path[i].y, path[i].theta,
      path[i - 1].x, path[i - 1].y, path[i - 1].theta,
      true));
  }
}

TEST(OfficialStateLattice, PlansWithGeneratedHalfMeterDifferentialDrivePrimitives)
{
  const std::string lattice = ros::package::getPath("smac_lattice_planner_mbf") +
    "/config/diff_5cm_0p50m_32bins.json";
  const auto metadata =
    nav2_smac_planner::LatticeMotionTable::getLatticeMetadata(lattice);
  ASSERT_EQ(metadata.motion_model, "diff");
  ASSERT_EQ(metadata.number_of_headings, 32u);
  ASSERT_EQ(metadata.number_of_trajectories, 232u);
  ASSERT_NEAR(metadata.grid_resolution, 0.05, 1e-6);
  ASSERT_NEAR(metadata.min_turning_radius, 0.5, 1e-6);

  costmap_2d::Costmap2D costmap(100, 100, 0.05, 0.0, 0.0, costmap_2d::FREE_SPACE);
  nav2_smac_planner::GridCollisionChecker checker(&costmap, 72u);
  checker.setFootprint(robotFootprint(), false, 0.0);
  checker.setCollisionCheckResolution(0.01);

  nav2_smac_planner::SearchInfo search_info;
  search_info.lattice_filepath = lattice;
  search_info.minimum_turning_radius = metadata.min_turning_radius / costmap.getResolution();
  search_info.allow_reverse_expansion = false;
  search_info.analytic_expansion_max_length = 3.0 / costmap.getResolution();

  int max_iterations = 100000;
  nav2_smac_planner::AStarAlgorithm<nav2_smac_planner::NodeLattice> planner(
    nav2_smac_planner::MotionModel::STATE_LATTICE, search_info);
  planner.initialize(true, max_iterations, 1000, 100, 5.0, 101.0, 32u);
  planner.setCollisionChecker(&checker);
  planner.setStart(20.0f, 50.0f, 0u);
  planner.setGoal(70.0f, 50.0f, 0u);

  nav2_smac_planner::NodeLattice::CoordinateVector path;
  int iterations = 0;
  ASSERT_TRUE(planner.createPath(path, iterations, 0.0f, []() {return false;}));
  ASSERT_GT(path.size(), 2u);

  for (std::size_t i = path.size() - 1; i > 0; --i) {
    EXPECT_FALSE(checker.inCollisionContinuous(
      path[i].x, path[i].y, path[i].theta,
      path[i - 1].x, path[i - 1].y, path[i - 1].theta,
      true));
  }
}

TEST(OfficialStateLattice, PlansWithGeneratedPointFourFiveMeterDifferentialDrivePrimitives)
{
  const std::string lattice = ros::package::getPath("smac_lattice_planner_mbf") +
    "/config/diff_5cm_0p45m_32bins.json";
  const auto metadata =
    nav2_smac_planner::LatticeMotionTable::getLatticeMetadata(lattice);
  ASSERT_EQ(metadata.motion_model, "diff");
  ASSERT_EQ(metadata.number_of_headings, 32u);
  ASSERT_NEAR(metadata.grid_resolution, 0.05, 1e-6);
  ASSERT_NEAR(metadata.min_turning_radius, 0.45, 1e-6);

  costmap_2d::Costmap2D costmap(100, 100, 0.05, 0.0, 0.0, costmap_2d::FREE_SPACE);
  nav2_smac_planner::GridCollisionChecker checker(&costmap, 72u);
  checker.setFootprint(robotFootprint(), false, 0.0);
  checker.setCollisionCheckResolution(0.01);

  nav2_smac_planner::SearchInfo search_info;
  search_info.lattice_filepath = lattice;
  search_info.minimum_turning_radius = metadata.min_turning_radius / costmap.getResolution();
  search_info.allow_reverse_expansion = false;
  search_info.analytic_expansion_max_length = 3.0 / costmap.getResolution();

  int max_iterations = 100000;
  nav2_smac_planner::AStarAlgorithm<nav2_smac_planner::NodeLattice> planner(
    nav2_smac_planner::MotionModel::STATE_LATTICE, search_info);
  planner.initialize(true, max_iterations, 1000, 100, 5.0, 101.0, 32u);
  planner.setCollisionChecker(&checker);
  planner.setStart(20.0f, 50.0f, 0u);
  planner.setGoal(70.0f, 50.0f, 0u);

  nav2_smac_planner::NodeLattice::CoordinateVector path;
  int iterations = 0;
  ASSERT_TRUE(planner.createPath(path, iterations, 0.0f, []() {return false;}));
  ASSERT_GT(path.size(), 2u);

  for (std::size_t i = path.size() - 1; i > 0; --i) {
    EXPECT_FALSE(checker.inCollisionContinuous(
      path[i].x, path[i].y, path[i].theta,
      path[i - 1].x, path[i - 1].y, path[i - 1].theta,
      true));
  }
}

TEST(OfficialStateLattice, CancelsOnTheNextExpansionRegardlessOfTerminalInterval)
{
  const std::string lattice = ros::package::getPath("smac_lattice_planner_mbf") +
    "/config/diff_5cm_0p40m_32bins.json";
  const auto metadata =
    nav2_smac_planner::LatticeMotionTable::getLatticeMetadata(lattice);
  costmap_2d::Costmap2D costmap(100, 100, 0.05, 0.0, 0.0, costmap_2d::FREE_SPACE);
  // Block the direct analytic connection while leaving the start and goal
  // valid, so the second loop iteration is required to observe cancellation.
  for (unsigned int my = 0; my < costmap.getSizeInCellsY(); ++my) {
    costmap.setCost(45u, my, costmap_2d::LETHAL_OBSTACLE);
  }
  nav2_smac_planner::GridCollisionChecker checker(&costmap, 72u);
  checker.setFootprint(robotFootprint(), false, 0.0);

  nav2_smac_planner::SearchInfo search_info;
  search_info.lattice_filepath = lattice;
  search_info.minimum_turning_radius =
    metadata.min_turning_radius / costmap.getResolution();
  int max_iterations = 2;
  nav2_smac_planner::AStarAlgorithm<nav2_smac_planner::NodeLattice> planner(
    nav2_smac_planner::MotionModel::STATE_LATTICE, search_info);
  planner.initialize(true, max_iterations, 1000, 1000, 5.0, 101.0, 32u);
  planner.setCollisionChecker(&checker);
  planner.setStart(20.0f, 50.0f, 0u);
  planner.setGoal(70.0f, 50.0f, 0u);

  nav2_smac_planner::NodeLattice::CoordinateVector path;
  int iterations = 0;
  const auto detailed_result = planner.createPathDetailed(
    path, iterations, 0.0f, []() {return true;});
  EXPECT_FALSE(detailed_result.path_found);
  EXPECT_EQ(
    detailed_result.termination,
    nav2_smac_planner::SearchTermination::CANCELED);

  // Reset the graph and verify the established bool API still throws.
  planner.setCollisionChecker(&checker);
  planner.setStart(20.0f, 50.0f, 0u);
  planner.setGoal(70.0f, 50.0f, 0u);
  path.clear();
  iterations = 0;
  int cancel_checks = 0;
  EXPECT_THROW(
    planner.createPath(
      path, iterations, 0.0f,
      [&cancel_checks]() {return ++cancel_checks >= 2;}),
    nav2_core::PlannerCancelled);
  EXPECT_EQ(cancel_checks, 2);
}

TEST(OfficialStateLattice, CenterDomainConstrainsDiscreteAndAnalyticExpansion)
{
  const std::string lattice = ros::package::getPath("smac_lattice_planner_mbf") +
    "/config/diff_5cm_0p40m_32bins.json";
  const auto metadata =
    nav2_smac_planner::LatticeMotionTable::getLatticeMetadata(lattice);
  costmap_2d::Costmap2D costmap(
    100, 100, 0.05, 0.0, 0.0, costmap_2d::FREE_SPACE);
  nav2_smac_planner::GridCollisionChecker checker(&costmap, 72u);
  checker.setFootprint(robotFootprint(), false, 0.0);
  checker.setCollisionCheckResolution(0.01);

  nav2_smac_planner::SearchInfo search_info;
  search_info.lattice_filepath = lattice;
  search_info.minimum_turning_radius =
    metadata.min_turning_radius / costmap.getResolution();
  search_info.allow_reverse_expansion = true;
  // Make a direct analytic completion possible in FULL. The disconnected
  // centre domain below must reject that shortcut as well as graph neighbors.
  search_info.analytic_expansion_max_length = 4.0 / costmap.getResolution();

  int max_iterations = 100000;
  nav2_smac_planner::AStarAlgorithm<nav2_smac_planner::NodeLattice> planner(
    nav2_smac_planner::MotionModel::STATE_LATTICE, search_info);
  planner.initialize(true, max_iterations, 1000, 100, 5.0, 161.0, 32u);
  planner.setCollisionChecker(&checker);
  planner.setStart(20.0f, 50.0f, 0u);
  planner.setGoal(80.0f, 50.0f, 0u);
  planner.setCenterDomain(
    [](float x, float y) {
      const bool in_narrow_band = y >= 48.0f && y <= 52.0f;
      const bool outside_gap = x < 35.0f || x > 65.0f;
      return in_narrow_band && outside_gap;
    });

  nav2_smac_planner::NodeLattice::CoordinateVector path;
  int iterations = 0;
  const auto restricted_result = planner.createPathDetailed(
    path, iterations, 0.0f, []() {return false;});
  EXPECT_FALSE(restricted_result.path_found);
  EXPECT_EQ(
    restricted_result.termination,
    nav2_smac_planner::SearchTermination::OPEN_EXHAUSTED);

  // The empty callback is explicitly the unrestricted FULL domain.
  planner.clearCenterDomain();
  planner.setCollisionChecker(&checker);
  planner.setStart(20.0f, 50.0f, 0u);
  planner.setGoal(80.0f, 50.0f, 0u);
  path.clear();
  iterations = 0;
  const auto full_result = planner.createPathDetailed(
    path, iterations, 0.0f, []() {return false;});
  EXPECT_TRUE(full_result.path_found);
  EXPECT_EQ(
    full_result.termination,
    nav2_smac_planner::SearchTermination::SUCCESS);
  EXPECT_GT(path.size(), 2u);
}

TEST(OfficialStateLattice, AdditionalHeuristicOnlyChangesQueueOrderingValue)
{
  const std::string lattice = ros::package::getPath("smac_lattice_planner_mbf") +
    "/config/diff_5cm_0p40m_32bins.json";
  const auto metadata =
    nav2_smac_planner::LatticeMotionTable::getLatticeMetadata(lattice);
  costmap_2d::Costmap2D costmap(
    100, 100, 0.05, 0.0, 0.0, costmap_2d::FREE_SPACE);
  nav2_smac_planner::GridCollisionChecker checker(&costmap, 72u);
  checker.setFootprint(robotFootprint(), false, 0.0);

  nav2_smac_planner::SearchInfo search_info;
  search_info.lattice_filepath = lattice;
  search_info.minimum_turning_radius =
    metadata.min_turning_radius / costmap.getResolution();
  search_info.allow_reverse_expansion = true;

  int max_iterations = 100000;
  LatticeAStarHeuristicProbe planner(
    nav2_smac_planner::MotionModel::STATE_LATTICE, search_info);
  planner.initialize(true, max_iterations, 1000, 100, 5.0, 101.0, 32u);
  planner.setCollisionChecker(&checker);
  planner.setStart(20.0f, 50.0f, 0u);
  planner.setGoal(70.0f, 50.0f, 0u);

  const float anchor = planner.heuristicAtStart();
  ASSERT_GT(anchor, 0.0f);
  planner.resetBestAnchor();

  int callback_calls = 0;
  planner.setAdditionalHeuristic(
    [anchor, &callback_calls](float x, float y) {
      ++callback_calls;
      EXPECT_FLOAT_EQ(x, 20.0f);
      EXPECT_FLOAT_EQ(y, 50.0f);
      return anchor + 123.0f;
    });
  EXPECT_FLOAT_EQ(planner.heuristicAtStart(), anchor + 123.0f);
  EXPECT_EQ(callback_calls, 1);
  // Closest/tolerance bookkeeping deliberately ignores the extra guide.
  EXPECT_FLOAT_EQ(planner.bestAnchor(), anchor);

  planner.clearAdditionalHeuristic();
  planner.resetBestAnchor();
  EXPECT_FLOAT_EQ(planner.heuristicAtStart(), anchor);
  EXPECT_FLOAT_EQ(planner.bestAnchor(), anchor);
}

TEST(OfficialStateLattice, SelectsLowestCostSafeHeadingSeed)
{
  const std::string lattice = ros::package::getPath("smac_lattice_planner_mbf") +
    "/config/diff_5cm_0p40m_32bins.json";
  const auto metadata =
    nav2_smac_planner::LatticeMotionTable::getLatticeMetadata(lattice);
  costmap_2d::Costmap2D costmap(
    240, 240, 0.05, 0.0, 0.0, costmap_2d::FREE_SPACE);
  nav2_smac_planner::GridCollisionChecker checker(&costmap, 72u);
  checker.setFootprint(robotFootprint(), false, 0.0);
  checker.setCollisionCheckResolution(0.01);

  nav2_smac_planner::SearchInfo search_info;
  search_info.lattice_filepath = lattice;
  search_info.minimum_turning_radius =
    metadata.min_turning_radius / costmap.getResolution();
  search_info.allow_reverse_expansion = true;
  search_info.analytic_expansion_max_length = 3.0 / costmap.getResolution();
  int max_iterations = 100000;
  nav2_smac_planner::AStarAlgorithm<nav2_smac_planner::NodeLattice> planner(
    nav2_smac_planner::MotionModel::STATE_LATTICE, search_info);
  planner.initialize(true, max_iterations, 1000, 100, 5.0, 101.0, 32u);
  planner.setCollisionChecker(&checker);

  // Both seeds have the same physical position. The pi-facing seed represents
  // a safe adjacent heading correction and has the lower correction cost.
  planner.setStart(120.0f, 120.0f, 0u, 100.0f);
  planner.addStart(120.0f, 120.0f, 16u, 0.0f);
  planner.setGoal(100.0f, 120.0f, 16u);

  nav2_smac_planner::NodeLattice::CoordinateVector path;
  int iterations = 0;
  ASSERT_TRUE(planner.createPath(path, iterations, 0.0f, []() {return false;}));
  ASSERT_GT(path.size(), 1u);
  EXPECT_NEAR(angles::shortest_angular_distance(path.back().theta, M_PI), 0.0, 1e-3);
}

TEST(OfficialStateLattice, PrefersForwardAnalyticConnectionInOpenSpace)
{
  const std::string lattice = ros::package::getPath("smac_lattice_planner_mbf") +
    "/config/diff_5cm_0p40m_32bins.json";
  const auto metadata =
    nav2_smac_planner::LatticeMotionTable::getLatticeMetadata(lattice);
  costmap_2d::Costmap2D costmap(
    240, 240, 0.05, 0.0, 0.0, costmap_2d::FREE_SPACE);
  nav2_smac_planner::GridCollisionChecker checker(&costmap, 72u);
  checker.setFootprint(robotFootprint(), false, 0.0);
  checker.setCollisionCheckResolution(0.01);

  nav2_smac_planner::SearchInfo search_info;
  search_info.lattice_filepath = lattice;
  search_info.minimum_turning_radius =
    metadata.min_turning_radius / costmap.getResolution();
  search_info.allow_reverse_expansion = true;
  search_info.prefer_forward_analytic_expansion = true;
  search_info.reverse_penalty = 4.0f;
  search_info.rotation_penalty = 2.5f;
  search_info.analytic_expansion_max_length = 4.0f / costmap.getResolution();

  int max_iterations = 500000;
  nav2_smac_planner::AStarAlgorithm<nav2_smac_planner::NodeLattice> planner(
    nav2_smac_planner::MotionModel::STATE_LATTICE, search_info);
  planner.initialize(true, max_iterations, 1000, 100, 10.0, 161.0, 32u);
  planner.setCollisionChecker(&checker);
  // The goal is one metre directly behind the start with the same heading.
  // Reverse is shorter, but open space permits a smooth forward-only loop.
  planner.setStart(120.0f, 120.0f, 0u);
  planner.setGoal(100.0f, 120.0f, 0u);

  nav2_smac_planner::NodeLattice::CoordinateVector path;
  int iterations = 0;
  ASSERT_TRUE(planner.createPath(path, iterations, 0.0f, []() {return false;}));
  ASSERT_GT(path.size(), 2u);

  bool contains_reverse_translation = false;
  for (std::size_t i = path.size() - 1u; i > 0u; --i) {
    const auto & current = path[i];
    const auto & next = path[i - 1u];
    const float dx = next.x - current.x;
    const float dy = next.y - current.y;
    const float distance = std::hypot(dx, dy);
    if (distance <= 1e-4f) {
      continue;
    }
    const float projection =
      (dx * std::cos(current.theta) + dy * std::sin(current.theta)) /
      distance;
    if (projection < -0.25f) {
      contains_reverse_translation = true;
      break;
    }
  }
  EXPECT_FALSE(contains_reverse_translation);
}

TEST(OfficialStateLattice, GoalTransitionValidatorRejectsRotationBeforeGoalVisit)
{
  const std::string lattice = ros::package::getPath("smac_lattice_planner_mbf") +
    "/config/diff_5cm_0p40m_32bins.json";
  const auto metadata =
    nav2_smac_planner::LatticeMotionTable::getLatticeMetadata(lattice);
  costmap_2d::Costmap2D costmap(
    180, 180, 0.05, 0.0, 0.0, costmap_2d::FREE_SPACE);
  nav2_smac_planner::GridCollisionChecker checker(&costmap, 72u);
  checker.setFootprint(robotFootprint(), false, 0.0);
  checker.setCollisionCheckResolution(0.01);

  nav2_smac_planner::SearchInfo search_info;
  search_info.lattice_filepath = lattice;
  search_info.minimum_turning_radius =
    metadata.min_turning_radius / costmap.getResolution();
  search_info.allow_reverse_expansion = false;
  search_info.analytic_expansion_max_length = 0.0f;
  int max_iterations = 200000;
  nav2_smac_planner::AStarAlgorithm<nav2_smac_planner::NodeLattice> planner(
    nav2_smac_planner::MotionModel::STATE_LATTICE, search_info);
  planner.initialize(true, max_iterations, 1000, 100, 5.0, 161.0, 32u);
  planner.setCollisionChecker(&checker);

  // The adjacent heading at the same XY is directly reachable by a rotation
  // primitive. The opt-in policy must reject that incoming edge before the
  // goal state is visited, leaving a moving arrival free to compete.
  int rejected_stationary_goals = 0;
  planner.setGoalTransitionValidator(
    [&rejected_stationary_goals](
      const nav2_smac_planner::NodeLattice::Coordinates & from,
      const nav2_smac_planner::NodeLattice::Coordinates & to) {
      const bool translates = std::hypot(to.x - from.x, to.y - from.y) > 1e-4f;
      if (!translates) {
        ++rejected_stationary_goals;
      }
      return translates;
    });
  planner.setStart(90.0f, 90.0f, 0u);
  planner.setGoal(90.0f, 90.0f, 1u);
  nav2_smac_planner::NodeLattice::CoordinateVector filtered_path;
  int filtered_iterations = 0;
  const auto filtered_result = planner.createPathDetailed(
    filtered_path, filtered_iterations, 0.0f, []() {return false;});
  EXPECT_GT(rejected_stationary_goals, 0);
  ASSERT_TRUE(filtered_result.hasPath());
  ASSERT_EQ(filtered_result.termination, nav2_smac_planner::SearchTermination::SUCCESS);
  ASSERT_GT(filtered_path.size(), 1u);
  EXPECT_GT(
    std::hypot(
      filtered_path[0].x - filtered_path[1].x,
      filtered_path[0].y - filtered_path[1].y),
    1e-4f);

  // Clearing the opt-in policy restores the upstream behavior exactly: the
  // direct same-position rotation is again a valid goal transition.
  planner.clearGoalTransitionValidator();
  planner.setCollisionChecker(&checker);
  planner.setStart(90.0f, 90.0f, 0u);
  planner.setGoal(90.0f, 90.0f, 1u);
  nav2_smac_planner::NodeLattice::CoordinateVector default_path;
  int default_iterations = 0;
  ASSERT_TRUE(planner.createPath(
      default_path, default_iterations, 0.0f, []() {return false;}));
  ASSERT_GT(default_path.size(), 1u);
  EXPECT_LE(
    std::hypot(
      default_path[0].x - default_path[1].x,
      default_path[0].y - default_path[1].y),
    1e-4f);
}

TEST(OfficialStateLattice, AnalyticTerminalUsesNearestBinWithoutStationaryAppend)
{
  constexpr float requested_yaw = 1.312569976f;
  constexpr float goal_x = 150.25f;
  constexpr float goal_y = 150.50f;
  constexpr float approach_distance_cells = 60.0f;
  const std::string lattice = ros::package::getPath("smac_lattice_planner_mbf") +
    "/config/diff_5cm_0p40m_32bins.json";
  const auto metadata =
    nav2_smac_planner::LatticeMotionTable::getLatticeMetadata(lattice);
  ASSERT_EQ(metadata.number_of_headings, 32u);

  costmap_2d::Costmap2D costmap(
    260, 260, 0.05, 0.0, 0.0, costmap_2d::FREE_SPACE);
  nav2_smac_planner::GridCollisionChecker checker(&costmap, 72u);
  checker.setFootprint(robotFootprint(), false, 0.0);
  checker.setCollisionCheckResolution(0.01);

  nav2_smac_planner::SearchInfo search_info;
  search_info.lattice_filepath = lattice;
  search_info.minimum_turning_radius =
    metadata.min_turning_radius / costmap.getResolution();
  search_info.allow_reverse_expansion = false;
  search_info.prefer_forward_analytic_expansion = true;
  search_info.analytic_expansion_max_length = 4.0f / costmap.getResolution();
  int max_iterations = 100000;
  LatticeAStarHeuristicProbe planner(
    nav2_smac_planner::MotionModel::STATE_LATTICE, search_info);
  planner.initialize(true, max_iterations, 1000, 100, 5.0, 161.0, 32u);
  planner.setCollisionChecker(&checker);
  planner.setGoalTransitionValidator(
    [](const nav2_smac_planner::NodeLattice::Coordinates & from,
      const nav2_smac_planner::NodeLattice::Coordinates & to) {
      return std::hypot(to.x - from.x, to.y - from.y) > 1e-4f;
    });

  const unsigned int goal_bin =
    planner.getContext()->motion_table.getClosestAngularBin(requested_yaw);
  ASSERT_EQ(goal_bin, 7u);
  const float bin_yaw = planner.getContext()->motion_table.getAngleFromBin(goal_bin);
  ASSERT_NEAR(
    std::abs(angles::shortest_angular_distance(requested_yaw, bin_yaw)),
    0.0132477, 1e-5);
  const float start_x = goal_x - approach_distance_cells * std::cos(requested_yaw);
  const float start_y = goal_y - approach_distance_cells * std::sin(requested_yaw);
  planner.setStart(start_x, start_y, goal_bin);
  planner.setGoal(
    goal_x, goal_y, goal_bin, nav2_smac_planner::GoalHeadingMode::DEFAULT,
    1);

  auto * graph_goal = planner.singleGoal();
  ASSERT_NE(graph_goal, nullptr);
  EXPECT_FLOAT_EQ(graph_goal->pose.theta, static_cast<float>(goal_bin));
  EXPECT_EQ(graph_goal->getIndex() % metadata.number_of_headings, goal_bin);

  nav2_smac_planner::NodeLattice::CoordinateVector path;
  int iterations = 0;
  const auto result = planner.createPathDetailed(
    path, iterations, 0.0f, []() {return false;});
  ASSERT_TRUE(result.path_found);
  ASSERT_EQ(result.termination, nav2_smac_planner::SearchTermination::SUCCESS);
  ASSERT_GT(path.size(), 2u);
  EXPECT_NEAR(path.front().x, goal_x, 1e-5);
  EXPECT_NEAR(path.front().y, goal_y, 1e-5);
  EXPECT_NEAR(
    angles::shortest_angular_distance(path.front().theta, bin_yaw), 0.0, 1e-6);
  EXPECT_LE(
    std::abs(angles::shortest_angular_distance(path.front().theta, requested_yaw)),
    M_PI / static_cast<double>(metadata.number_of_headings) + 1e-5);
  EXPECT_GT(
    std::hypot(
      path.front().x - path[1].x,
      path.front().y - path[1].y),
    1e-3);
  EXPECT_FALSE(containsReverseTranslation(path));

  for (std::size_t index = path.size(); index > 1u; --index) {
    const auto & start = path[index - 1u];
    const auto & end = path[index - 2u];
    EXPECT_FALSE(checker.inCollisionContinuous(
      start.x, start.y, start.theta, end.x, end.y, end.theta, true));
  }
}

TEST(OfficialStateLattice, DiscreteGoalUsesRequestedNearestHeadingBin)
{
  const std::string lattice = ros::package::getPath("smac_lattice_planner_mbf") +
    "/config/diff_5cm_0p40m_32bins.json";
  const auto metadata =
    nav2_smac_planner::LatticeMotionTable::getLatticeMetadata(lattice);
  costmap_2d::Costmap2D costmap(
    100, 100, 0.05, 0.0, 0.0, costmap_2d::FREE_SPACE);
  nav2_smac_planner::GridCollisionChecker checker(&costmap, 72u);
  checker.setFootprint(robotFootprint(), false, 0.0);

  nav2_smac_planner::SearchInfo search_info;
  search_info.lattice_filepath = lattice;
  search_info.minimum_turning_radius =
    metadata.min_turning_radius / costmap.getResolution();
  search_info.allow_reverse_expansion = false;
  search_info.analytic_expansion_max_length = 0.0f;
  int max_iterations = 500;
  nav2_smac_planner::AStarAlgorithm<nav2_smac_planner::NodeLattice> planner(
    nav2_smac_planner::MotionModel::STATE_LATTICE, search_info);
  planner.initialize(true, max_iterations, 1000, 20, 2.0, 101.0, 32u);
  planner.setCollisionChecker(&checker);
  planner.setStart(20.0f, 50.0f, 0u);
  planner.setGoal(
    40.0f, 50.0f, 0u, nav2_smac_planner::GoalHeadingMode::DEFAULT,
    1);

  nav2_smac_planner::NodeLattice::CoordinateVector path;
  int iterations = 0;
  const auto result = planner.createPathDetailed(
    path, iterations, 0.0f, []() {return false;});
  EXPECT_TRUE(result.path_found);
  EXPECT_EQ(result.termination, nav2_smac_planner::SearchTermination::SUCCESS);
  ASSERT_FALSE(path.empty());
  EXPECT_NEAR(path.front().theta, 0.0f, 1e-6);
}

TEST(OfficialStateLattice, HeadingBinGoalMayUseDiscreteArrival)
{
  const std::string lattice = ros::package::getPath("smac_lattice_planner_mbf") +
    "/config/diff_5cm_0p40m_32bins.json";
  const auto metadata =
    nav2_smac_planner::LatticeMotionTable::getLatticeMetadata(lattice);
  costmap_2d::Costmap2D costmap(
    100, 100, 0.05, 0.0, 0.0, costmap_2d::FREE_SPACE);
  nav2_smac_planner::GridCollisionChecker checker(&costmap, 72u);
  checker.setFootprint(robotFootprint(), false, 0.0);

  nav2_smac_planner::SearchInfo search_info;
  search_info.lattice_filepath = lattice;
  search_info.minimum_turning_radius =
    metadata.min_turning_radius / costmap.getResolution();
  search_info.allow_reverse_expansion = false;
  search_info.analytic_expansion_max_length = 0.0f;
  int max_iterations = 100000;
  nav2_smac_planner::AStarAlgorithm<nav2_smac_planner::NodeLattice> planner(
    nav2_smac_planner::MotionModel::STATE_LATTICE, search_info);
  planner.initialize(true, max_iterations, 1000, 100, 5.0, 101.0, 32u);
  planner.setCollisionChecker(&checker);
  planner.setStart(20.0f, 50.0f, 0u);
  planner.setGoal(
    40.0f, 50.0f, 0u, nav2_smac_planner::GoalHeadingMode::DEFAULT,
    1);

  nav2_smac_planner::NodeLattice::CoordinateVector path;
  int iterations = 0;
  const auto result = planner.createPathDetailed(
    path, iterations, 0.0f, []() {return false;});
  ASSERT_TRUE(result.path_found);
  ASSERT_EQ(result.termination, nav2_smac_planner::SearchTermination::SUCCESS);
  ASSERT_FALSE(path.empty());
  EXPECT_NEAR(path.front().x, 40.0f, 1e-5);
  EXPECT_NEAR(path.front().y, 50.0f, 1e-5);
  EXPECT_NEAR(path.front().theta, 0.0f, 1e-6);
}

TEST(OfficialStateLattice, SubDiagonalCellGoalUsesDiscreteHeading)
{
  const std::string lattice = ros::package::getPath("smac_lattice_planner_mbf") +
    "/config/diff_5cm_0p40m_32bins.json";
  const auto metadata =
    nav2_smac_planner::LatticeMotionTable::getLatticeMetadata(lattice);
  costmap_2d::Costmap2D costmap(
    100, 100, 0.05, 0.0, 0.0, costmap_2d::FREE_SPACE);
  nav2_smac_planner::GridCollisionChecker checker(&costmap, 72u);
  checker.setFootprint(robotFootprint(), false, 0.0);
  checker.setCollisionCheckResolution(0.01);

  nav2_smac_planner::SearchInfo search_info;
  search_info.lattice_filepath = lattice;
  search_info.minimum_turning_radius =
    metadata.min_turning_radius / costmap.getResolution();
  search_info.allow_reverse_expansion = false;
  search_info.analytic_expansion_max_length = 1.0f / costmap.getResolution();
  int max_iterations = 1000;
  nav2_smac_planner::AStarAlgorithm<nav2_smac_planner::NodeLattice> planner(
    nav2_smac_planner::MotionModel::STATE_LATTICE, search_info);
  planner.initialize(true, max_iterations, 1000, 10, 2.0, 101.0, 32u);
  planner.setCollisionChecker(&checker);
  planner.setStart(50.0f, 50.0f, 0u);
  planner.setGoal(
    51.0f, 50.0f, 0u, nav2_smac_planner::GoalHeadingMode::DEFAULT,
    1);

  nav2_smac_planner::NodeLattice::CoordinateVector path;
  int iterations = 0;
  const auto result = planner.createPathDetailed(
    path, iterations, 0.0f, []() {return false;});
  ASSERT_TRUE(result.path_found);
  ASSERT_EQ(result.termination, nav2_smac_planner::SearchTermination::SUCCESS);
  ASSERT_GE(path.size(), 2u);
  EXPECT_NEAR(path.front().x, 51.0f, 1e-5);
  EXPECT_NEAR(path.front().y, 50.0f, 1e-5);
  EXPECT_NEAR(path.front().theta, 0.0, 1e-6);
  EXPECT_GT(std::hypot(
    path.front().x - path[1].x,
    path.front().y - path[1].y), 1e-3);
  EXPECT_FALSE(checker.inCollisionContinuous(
    path[1].x, path[1].y, path[1].theta,
    path.front().x, path.front().y, path.front().theta, true));
}

TEST(OfficialStateLattice, GoalValidityUsesDiscreteGraphBinYaw)
{
  constexpr float requested_yaw = 0.12f;
  constexpr float goal_x = 100.0f;
  constexpr float goal_y = 100.0f;
  const std::string lattice = ros::package::getPath("smac_lattice_planner_mbf") +
    "/config/diff_5cm_0p40m_32bins.json";
  const auto metadata =
    nav2_smac_planner::LatticeMotionTable::getLatticeMetadata(lattice);

  const auto make_probe = [&](costmap_2d::Costmap2D & costmap,
                              nav2_smac_planner::GridCollisionChecker & checker)
    {
      nav2_smac_planner::SearchInfo search_info;
      search_info.lattice_filepath = lattice;
      search_info.minimum_turning_radius =
        metadata.min_turning_radius / costmap.getResolution();
      search_info.allow_reverse_expansion = false;
      int max_iterations = 1000;
      auto planner = std::make_unique<LatticeAStarHeuristicProbe>(
        nav2_smac_planner::MotionModel::STATE_LATTICE, search_info);
      planner->initialize(true, max_iterations, 1000, 100, 1.0, 101.0, 32u);
      planner->setCollisionChecker(&checker);
      planner->setStart(40.0f, goal_y, 0u);
      planner->setGoal(
        goal_x, goal_y, 0u, nav2_smac_planner::GoalHeadingMode::DEFAULT,
        1);
      return planner;
    };

  // The requested continuous yaw is not an executed State node. A cell that
  // only intersects that yaw must not reject the collision-free graph bin.
  costmap_2d::Costmap2D requested_yaw_blocked(
    200, 200, 0.05, 0.0, 0.0, costmap_2d::FREE_SPACE);
  setLethal(requested_yaw_blocked, 5.575, 5.375);
  nav2_smac_planner::GridCollisionChecker requested_yaw_blocked_checker(
    &requested_yaw_blocked, 72u);
  requested_yaw_blocked_checker.setFootprint(robotFootprint(), false, 0.0);
  ASSERT_FALSE(requested_yaw_blocked_checker.inCollisionAtYaw(
    goal_x, goal_y, 0.0, true));
  ASSERT_TRUE(requested_yaw_blocked_checker.inCollisionAtYaw(
    goal_x, goal_y, requested_yaw, true));
  auto accepted_planner = make_probe(
    requested_yaw_blocked, requested_yaw_blocked_checker);
  EXPECT_TRUE(accepted_planner->inputsAreValid());

  // Conversely, a cell inside the executed graph-bin footprint must reject
  // the goal even when the unquantized request would be clear.
  costmap_2d::Costmap2D bin_blocked(
    200, 200, 0.05, 0.0, 0.0, costmap_2d::FREE_SPACE);
  setLethal(bin_blocked, 5.575, 4.700);
  nav2_smac_planner::GridCollisionChecker bin_blocked_checker(&bin_blocked, 72u);
  bin_blocked_checker.setFootprint(robotFootprint(), false, 0.0);
  ASSERT_TRUE(bin_blocked_checker.inCollisionAtYaw(goal_x, goal_y, 0.0, true));
  ASSERT_FALSE(bin_blocked_checker.inCollisionAtYaw(
    goal_x, goal_y, requested_yaw, true));
  auto blocked_planner = make_probe(bin_blocked, bin_blocked_checker);
  EXPECT_THROW(blocked_planner->inputsAreValid(), nav2_core::GoalOccupied);
}

TEST(OfficialStateLattice, RejectsFilledFootprintBlockedGoalBeforeSearching)
{
  const std::string lattice = ros::package::getPath("smac_lattice_planner_mbf") +
    "/config/diff_5cm_0p40m_32bins.json";
  const auto metadata =
    nav2_smac_planner::LatticeMotionTable::getLatticeMetadata(lattice);
  costmap_2d::Costmap2D costmap(100, 100, 0.05, 0.0, 0.0, costmap_2d::FREE_SPACE);
  // Goal centre is (3.50, 2.50). Place the lethal core only in the
  // rectangular interior, where the former outline check could not see it.
  setLethal(costmap, 3.80, 2.50);
  nav2_smac_planner::GridCollisionChecker checker(&costmap, 72u);
  checker.setFootprint(robotFootprint(), false, 0.0);
  checker.setCollisionCheckResolution(0.01);

  nav2_smac_planner::SearchInfo search_info;
  search_info.lattice_filepath = lattice;
  search_info.minimum_turning_radius = metadata.min_turning_radius / costmap.getResolution();
  search_info.allow_reverse_expansion = true;
  search_info.analytic_expansion_max_length = 3.0 / costmap.getResolution();
  int max_iterations = 100000;
  nav2_smac_planner::AStarAlgorithm<nav2_smac_planner::NodeLattice> planner(
    nav2_smac_planner::MotionModel::STATE_LATTICE, search_info);
  planner.initialize(true, max_iterations, 1000, 100, 5.0, 101.0, 32u);
  planner.setCollisionChecker(&checker);
  planner.setStart(20.0f, 50.0f, 0u);
  planner.setGoal(70.0f, 50.0f, 0u);

  nav2_smac_planner::NodeLattice::CoordinateVector path;
  int iterations = 0;
  EXPECT_THROW(
    planner.createPath(path, iterations, 0.0f, []() {return false;}),
    nav2_core::GoalOccupied);
  EXPECT_EQ(iterations, 0);
}

TEST(LiveValidatorSelection, ExplicitPairMayBeNonAdjacentAndReversed)
{
  const auto selection =
    smac_lattice_planner_mbf::live_validator::selectConnections(
    {0, 9, 6, 12}, 12, 0, -1, -1);

  EXPECT_EQ(selection.mode, "explicit");
  ASSERT_EQ(selection.pairs.size(), 1u);
  EXPECT_EQ(selection.pairs.front().from, 12);
  EXPECT_EQ(selection.pairs.front().to, 0);
}

TEST(LiveValidatorSelection, UnmatchedOrderedFilterIsAnError)
{
  EXPECT_THROW(
    smac_lattice_planner_mbf::live_validator::selectConnections(
      {0, 9, 6, 12}, -1, -1, 12, 0),
    std::invalid_argument);
}

TEST(LiveValidatorSelection, RejectsPartialOrAmbiguousExplicitPair)
{
  EXPECT_THROW(
    smac_lattice_planner_mbf::live_validator::selectConnections(
      {0, 9}, 12, -1, -1, -1),
    std::invalid_argument);
  EXPECT_THROW(
    smac_lattice_planner_mbf::live_validator::selectConnections(
      {0, 9}, 12, 0, 12, 0),
    std::invalid_argument);
}

TEST(LiveValidatorTermination, ReportsObservableSearchOutcomeConservatively)
{
  using smac_lattice_planner_mbf::live_validator::classifySearchTermination;
  EXPECT_EQ(classifySearchTermination(true, true, 20, 100, 0.2, 1.0), "success");
  EXPECT_EQ(
    classifySearchTermination(true, false, 20, 100, 0.2, 1.0),
    "continuous_validation_failed");
  EXPECT_EQ(
    classifySearchTermination(false, false, 100, 100, 0.2, 1.0),
    "max_iterations");
  EXPECT_EQ(
    classifySearchTermination(false, false, 20, 100, 1.1, 1.0),
    "max_planning_time_or_late_exhaustion");
  EXPECT_EQ(
    classifySearchTermination(false, false, 20, 100, 0.2, 1.0),
    "search_exhausted");
}

int main(int argc, char ** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

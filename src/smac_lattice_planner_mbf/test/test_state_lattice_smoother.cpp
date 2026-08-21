// Copyright 2026 Clean Robot Navigation Team
// Licensed under the Apache License, Version 2.0.

#include <chrono>
#include <cmath>
#include <set>
#include <string>

#include <gtest/gtest.h>
#include <tf2/utils.h>

#include "smac_lattice_planner_mbf/state_lattice_smoother.hpp"

namespace
{
using smac_lattice_planner_mbf::StateLatticeSmoother;
using smac_lattice_planner_mbf::StateLatticeSmootherParams;
using smac_lattice_planner_mbf::theta_state_suffix::PosePath;

geometry_msgs::Quaternion quaternionFromYaw(double yaw)
{
  geometry_msgs::Quaternion quaternion;
  quaternion.z = std::sin(0.5 * yaw);
  quaternion.w = std::cos(0.5 * yaw);
  return quaternion;
}

PosePath makePath(bool zigzag)
{
  PosePath path(61u);
  for (std::size_t index = 0u; index < path.size(); ++index) {
    path[index].header.frame_id = "map";
    path[index].pose.position.x = -2.5 + 0.075 * static_cast<double>(index);
    path[index].pose.position.y = zigzag ?
      0.10 * std::sin(0.55 * static_cast<double>(index)) : 0.0;
  }
  for (std::size_t index = 0u; index + 1u < path.size(); ++index) {
    path[index].pose.orientation = quaternionFromYaw(std::atan2(
        path[index + 1u].pose.position.y - path[index].pose.position.y,
        path[index + 1u].pose.position.x - path[index].pose.position.x));
  }
  path.back().pose.orientation = path[path.size() - 2u].pose.orientation;
  return path;
}

PosePath makeCurvatureSequence(const std::vector<double> & yaw_changes)
{
  PosePath path(yaw_changes.size() + 1u);
  double yaw = 0.0;
  for (std::size_t index = 0u; index < path.size(); ++index) {
    path[index].header.frame_id = "map";
    path[index].pose.position.x = -2.5 + 0.05 * static_cast<double>(index);
    path[index].pose.position.y = 0.0;
    path[index].pose.orientation = quaternionFromYaw(yaw);
    if (index < yaw_changes.size()) {
      yaw += yaw_changes[index];
    }
  }
  return path;
}

PosePath makeForwardRotationForwardPath()
{
  constexpr std::size_t first_straight_poses = 16u;
  constexpr std::size_t second_straight_poses = 31u;
  constexpr double yaw_after_rotation = 0.24497866312686414;
  PosePath path(first_straight_poses + second_straight_poses);
  for (std::size_t index = 0u; index < first_straight_poses; ++index) {
    path[index].header.frame_id = "map";
    path[index].pose.position.x = -2.5 + 0.05 * static_cast<double>(index);
    path[index].pose.position.y = 0.0;
    path[index].pose.orientation = quaternionFromYaw(0.0);
  }
  const double anchor_x = path[first_straight_poses - 1u].pose.position.x;
  path[first_straight_poses].header.frame_id = "map";
  path[first_straight_poses].pose.position.x = anchor_x;
  path[first_straight_poses].pose.position.y = 0.0;
  path[first_straight_poses].pose.orientation = quaternionFromYaw(yaw_after_rotation);
  for (std::size_t offset = 1u; offset < second_straight_poses; ++offset) {
    const std::size_t index = first_straight_poses + offset;
    path[index].header.frame_id = "map";
    path[index].pose.position.x =
      anchor_x + 0.05 * static_cast<double>(offset) * std::cos(yaw_after_rotation);
    path[index].pose.position.y =
      0.05 * static_cast<double>(offset) * std::sin(yaw_after_rotation);
    path[index].pose.orientation = quaternionFromYaw(yaw_after_rotation);
  }
  return path;
}

std::vector<geometry_msgs::Point> footprint()
{
  std::vector<geometry_msgs::Point> value(4u);
  value[0].x = 0.40;
  value[0].y = 0.30;
  value[1].x = 0.40;
  value[1].y = -0.30;
  value[2].x = -0.40;
  value[2].y = -0.30;
  value[3].x = -0.40;
  value[3].y = 0.30;
  return value;
}

struct FixtureData
{
  FixtureData()
  : costmap(220u, 160u, 0.05, -5.5, -4.0, 0u), checker(&costmap, 72u)
  {
    checker.setFootprint(footprint(), false, 0.0);
    checker.setCollisionCheckResolution(0.01);
  }

  costmap_2d::Costmap2D costmap;
  nav2_smac_planner::GridCollisionChecker checker;
};

TEST(StateLatticeSmoother, OfficialDefaultsImproveZigzagAtomically)
{
  FixtureData fixture;
  StateLatticeSmootherParams params;
  params.tolerance = 1e-8;
  params.max_curvature_regression_ratio = 10.0;
  // This synthetic high-frequency zigzag isolates Nav2's numerical smoothing
  // behavior; use a permissive model radius here. Production-radius behavior
  // is covered separately by the F-R-F and hard-radius contract tests.
  StateLatticeSmoother smoother(params, 0.1);
  const PosePath raw = makePath(true);
  const auto result = smoother.smooth(
    raw, fixture.costmap, fixture.checker, false,
    StateLatticeSmoother::Clock::now() + std::chrono::seconds(2));

  ASSERT_TRUE(result.accepted) << result.reason;
  EXPECT_EQ(result.path.size(), raw.size());
  EXPECT_TRUE(smac_lattice_planner_mbf::theta_state_suffix::sameSE2(
      result.path.front(), raw.front(), 1e-9, 1e-9));
  EXPECT_TRUE(smac_lattice_planner_mbf::theta_state_suffix::sameSE2(
      result.path.back(), raw.back(), 1e-9, 1e-9));
  EXPECT_LT(
    result.candidate_quality.curvature_total_variation_radpm,
    result.raw_quality.curvature_total_variation_radpm);
  for (std::size_t index = 1u; index < result.path.size(); ++index) {
    const auto to_map = [&fixture] (const geometry_msgs::PoseStamped & pose) {
        return std::pair<float, float>{
          static_cast<float>((pose.pose.position.x - fixture.costmap.getOriginX()) /
          fixture.costmap.getResolution()),
          static_cast<float>((pose.pose.position.y - fixture.costmap.getOriginY()) /
          fixture.costmap.getResolution())};
      };
    const auto previous = to_map(result.path[index - 1u]);
    const auto current = to_map(result.path[index]);
    EXPECT_FALSE(fixture.checker.inCollisionContinuous(
        previous.first, previous.second,
        tf2::getYaw(result.path[index - 1u].pose.orientation),
        current.first, current.second,
        tf2::getYaw(result.path[index].pose.orientation), false));
  }
}

TEST(StateLatticeSmoother, StraightPathKeepsRawWhenThereIsNoQualityGain)
{
  FixtureData fixture;
  StateLatticeSmootherParams params;
  params.tolerance = 1e-8;
  StateLatticeSmoother smoother(params, 0.4);
  const PosePath raw = makePath(false);
  const auto result = smoother.smooth(
    raw, fixture.costmap, fixture.checker, false,
    StateLatticeSmoother::Clock::now() + std::chrono::seconds(2));

  EXPECT_FALSE(result.accepted);
  EXPECT_NE(result.reason.find("no measurable curvature improvement"), std::string::npos);
  ASSERT_EQ(result.path.size(), raw.size());
  for (std::size_t index = 0u; index < raw.size(); ++index) {
    EXPECT_DOUBLE_EQ(result.path[index].pose.position.x, raw[index].pose.position.x);
    EXPECT_DOUBLE_EQ(result.path[index].pose.position.y, raw[index].pose.position.y);
  }
}

TEST(StateLatticeSmoother, RejectsCandidateBelowConfiguredTurningRadiusAtomically)
{
  FixtureData fixture;
  StateLatticeSmootherParams params;
  params.tolerance = 1e-8;
  params.minimum_curvature_improvement = 0.0;
  // The same zigzag has an admissible smoothing result for the production
  // 0.40 m model, but cannot satisfy a deliberately large 10 m radius.
  StateLatticeSmoother smoother(params, 10.0);
  const PosePath raw = makePath(true);
  const auto result = smoother.smooth(
    raw, fixture.costmap, fixture.checker, false,
    StateLatticeSmoother::Clock::now() + std::chrono::seconds(2));

  EXPECT_FALSE(result.accepted);
  EXPECT_NE(result.reason.find("minimum turning radius"), std::string::npos) << result.reason;
  EXPECT_FALSE(result.rejected_candidate_path.empty());
  ASSERT_EQ(result.path.size(), raw.size());
  EXPECT_DOUBLE_EQ(result.path[20].pose.position.x, raw[20].pose.position.x);
  EXPECT_DOUBLE_EQ(result.path[20].pose.position.y, raw[20].pose.position.y);
}

TEST(StateLatticeSmoother, MaterialBackToBackTurnsCountAsOneDirectionChange)
{
  FixtureData fixture;
  const PosePath path = makeCurvatureSequence({0.04, 0.04, -0.04, -0.04});
  const auto quality = StateLatticeSmoother::summarizeQuality(path, fixture.costmap);
  EXPECT_EQ(quality.curvature_direction_changes, 1);
}

TEST(StateLatticeSmoother, SustainedStraightSeparatesOtherwiseOppositeTurns)
{
  FixtureData fixture;
  const PosePath path = makeCurvatureSequence(
    {0.04, 0.04, 0.0, 0.0, 0.0, 0.0, 0.0, -0.04, -0.04});
  const auto quality = StateLatticeSmoother::summarizeQuality(path, fixture.costmap);
  EXPECT_EQ(quality.curvature_direction_changes, 0);
}

TEST(StateLatticeSmoother, SubMaterialCurvatureNoiseDoesNotCreateAChange)
{
  FixtureData fixture;
  const PosePath path = makeCurvatureSequence({0.04, 0.04, -0.026, 0.0});
  const auto quality = StateLatticeSmoother::summarizeQuality(path, fixture.costmap);
  EXPECT_EQ(quality.curvature_direction_changes, 0);
}

TEST(StateLatticeSmoother, RotationAwareGateCanReplaceForwardRotationForwardWithArc)
{
  FixtureData fixture;
  StateLatticeSmootherParams params;
  params.tolerance = 1e-8;
  params.minimum_curvature_improvement = 0.0;
  // The synthetic two-ray case has a deliberately abrupt single-bin corner.
  // A ratio above its measured 1.37 equivalent-variation ratio proves that
  // the raw in-place rotation participates in the gate; without the
  // rotation-aware accounting any finite ratio still compares against zero.
  params.max_curvature_regression_ratio = 1.4;
  StateLatticeSmoother smoother(params, 0.4);
  const PosePath raw = makeForwardRotationForwardPath();
  const auto result = smoother.smooth(
    raw, fixture.costmap, fixture.checker, false,
    StateLatticeSmoother::Clock::now() + std::chrono::seconds(2));

  ASSERT_TRUE(result.accepted) << result.reason
    << " raw_tv=" << result.raw_quality.curvature_total_variation_radpm
    << " candidate_tv=" << result.candidate_quality.curvature_total_variation_radpm
    << " raw_jump=" << result.raw_quality.max_curvature_jump_radpm
    << " candidate_jump=" << result.candidate_quality.max_curvature_jump_radpm
    << " raw_rotations=" << result.raw_quality.in_place_rotations
    << " candidate_rotations=" << result.candidate_quality.in_place_rotations;
  EXPECT_LT(
    result.candidate_quality.in_place_rotations,
    result.raw_quality.in_place_rotations);
  EXPECT_LE(result.candidate_quality.max_abs_curvature_radpm, 2.51);
  EXPECT_TRUE(smac_lattice_planner_mbf::theta_state_suffix::sameSE2(
      result.path.front(), raw.front(), 1e-9, 1e-9));
  EXPECT_TRUE(smac_lattice_planner_mbf::theta_state_suffix::sameSE2(
      result.path.back(), raw.back(), 1e-9, 1e-9));
}

TEST(StateLatticeSmoother, CancellationCannotMutateRawPath)
{
  FixtureData fixture;
  StateLatticeSmootherParams params;
  StateLatticeSmoother smoother(params, 0.4);
  const PosePath raw = makePath(true);
  const auto result = smoother.smooth(
    raw, fixture.costmap, fixture.checker, false,
    StateLatticeSmoother::Clock::now() + std::chrono::seconds(2),
    []() {return true;});

  EXPECT_FALSE(result.accepted);
  EXPECT_EQ(result.reason, "State smoothing canceled");
  ASSERT_EQ(result.path.size(), raw.size());
  EXPECT_DOUBLE_EQ(result.path[10].pose.position.y, raw[10].pose.position.y);
}

TEST(StateLatticeSmoother, CandidateCollisionIsAtomicAndKeepsRawPath)
{
  FixtureData free_fixture;
  free_fixture.checker.setFootprint({}, true, 0.0);
  StateLatticeSmootherParams params;
  params.tolerance = 1e-8;
  params.max_curvature_regression_ratio = 10.0;
  StateLatticeSmoother smoother(params, 0.1);
  const PosePath raw = makePath(true);
  const auto free_result = smoother.smooth(
    raw, free_fixture.costmap, free_fixture.checker, false,
    StateLatticeSmoother::Clock::now() + std::chrono::seconds(2));
  ASSERT_TRUE(free_result.accepted) << free_result.reason;

  std::set<std::pair<unsigned int, unsigned int>> raw_cells;
  for (const auto & pose : raw) {
    unsigned int map_x = 0u;
    unsigned int map_y = 0u;
    ASSERT_TRUE(free_fixture.costmap.worldToMap(
        pose.pose.position.x, pose.pose.position.y, map_x, map_y));
    raw_cells.emplace(map_x, map_y);
  }
  std::set<std::pair<unsigned int, unsigned int>> candidate_only_cells;
  for (std::size_t index = 2u; index + 2u < free_result.path.size(); ++index) {
    unsigned int map_x = 0u;
    unsigned int map_y = 0u;
    ASSERT_TRUE(free_fixture.costmap.worldToMap(
        free_result.path[index].pose.position.x,
        free_result.path[index].pose.position.y, map_x, map_y));
    if (raw_cells.count({map_x, map_y}) == 0u) {
      candidate_only_cells.emplace(map_x, map_y);
    }
  }
  ASSERT_FALSE(candidate_only_cells.empty());

  FixtureData blocked_fixture;
  for (const auto & cell : candidate_only_cells) {
    blocked_fixture.costmap.setCost(
      cell.first, cell.second, costmap_2d::LETHAL_OBSTACLE);
  }
  nav2_smac_planner::GridCollisionChecker blocked_checker(
    &blocked_fixture.costmap, 72u);
  blocked_checker.setFootprint({}, true, 0.0);
  blocked_checker.setCollisionCheckResolution(0.01);
  const auto blocked_result = smoother.smooth(
    raw, blocked_fixture.costmap, blocked_checker, false,
    StateLatticeSmoother::Clock::now() + std::chrono::seconds(2));
  EXPECT_FALSE(blocked_result.accepted);
  EXPECT_NE(blocked_result.reason.find("infeasible footprint pose"), std::string::npos) <<
    blocked_result.reason;
  ASSERT_EQ(blocked_result.path.size(), raw.size());
  for (std::size_t index = 0u; index < raw.size(); ++index) {
    EXPECT_DOUBLE_EQ(
      blocked_result.path[index].pose.position.x, raw[index].pose.position.x);
    EXPECT_DOUBLE_EQ(
      blocked_result.path[index].pose.position.y, raw[index].pose.position.y);
  }
}

TEST(StateLatticeSmoother, ExpiredDeadlineCannotMutateRawPath)
{
  FixtureData fixture;
  StateLatticeSmoother smoother(StateLatticeSmootherParams(), 0.4);
  const PosePath raw = makePath(true);
  const auto result = smoother.smooth(
    raw, fixture.costmap, fixture.checker, false,
    StateLatticeSmoother::Clock::now() - std::chrono::milliseconds(1));
  EXPECT_FALSE(result.accepted);
  EXPECT_EQ(result.reason, "State smoothing has no remaining time");
  EXPECT_DOUBLE_EQ(result.path[25].pose.position.y, raw[25].pose.position.y);
}

TEST(StateLatticeSmoother, RejectsInvalidConfiguration)
{
  StateLatticeSmootherParams params;
  params.w_smooth = -0.1;
  EXPECT_THROW(StateLatticeSmoother(params, 0.4), std::invalid_argument);
  params.w_smooth = 0.3;
  EXPECT_THROW(StateLatticeSmoother(params, 0.0), std::invalid_argument);
}

}  // namespace

int main(int argc, char ** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

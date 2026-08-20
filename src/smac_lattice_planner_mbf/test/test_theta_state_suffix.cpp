// Copyright 2026 Clean Robot Navigation Team
// Licensed under the Apache License, Version 2.0.

#include <cmath>
#include <cstddef>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

#include <angles/angles.h>
#include <gtest/gtest.h>
#include <tf2_geometry_msgs/tf2_geometry_msgs.h>
#include <tf2/utils.h>

#include "smac_lattice_planner_mbf/theta_state_suffix.hpp"

namespace
{
using smac_lattice_planner_mbf::theta_state_suffix::PosePath;

geometry_msgs::PoseStamped makePose(double x, double y, double yaw, const std::string & frame = "map")
{
  geometry_msgs::PoseStamped pose;
  pose.header.frame_id = frame;
  pose.pose.position.x = x;
  pose.pose.position.y = y;
  tf2::Quaternion quaternion;
  quaternion.setRPY(0.0, 0.0, yaw);
  pose.pose.orientation = tf2::toMsg(quaternion);
  return pose;
}

PosePath straightPath(std::size_t pose_count)
{
  PosePath path;
  path.reserve(pose_count);
  for (std::size_t index = 0u; index < pose_count; ++index) {
    path.push_back(makePose(static_cast<double>(index) * 0.05, 0.0, 0.0));
  }
  return path;
}

}  // namespace

TEST(ThetaStateSuffixSelection, HundredPointsAreNinetyNineFiveCentimeterIntervals)
{
  using smac_lattice_planner_mbf::theta_state_suffix::selectThetaPrefixCut;
  const PosePath theta = straightPath(500u);
  const auto selected = selectThetaPrefixCut(theta, 100u);

  EXPECT_EQ(selected.requested_suffix_point_count, 100u);
  EXPECT_EQ(selected.effective_suffix_point_count, 100u);
  EXPECT_EQ(selected.cut_index, 400u);
  EXPECT_EQ(selected.prefix_including_cut.size(), 401u);
  EXPECT_DOUBLE_EQ(selected.cut.pose.position.x, 20.0);
  EXPECT_DOUBLE_EQ(
    theta.back().pose.position.x - selected.cut.pose.position.x,
    99.0 * 0.05);
  EXPECT_FALSE(selected.is_full_path);
}

TEST(ThetaStateSuffixSelection, CandidateWindowsWidenAndEndWithFullPath)
{
  using smac_lattice_planner_mbf::theta_state_suffix::makeThetaPrefixCutCandidates;
  const auto candidates = makeThetaPrefixCutCandidates(straightPath(500u));

  ASSERT_EQ(candidates.size(), 5u);
  EXPECT_EQ(candidates[0].effective_suffix_point_count, 100u);
  EXPECT_EQ(candidates[1].effective_suffix_point_count, 160u);
  EXPECT_EQ(candidates[2].effective_suffix_point_count, 240u);
  EXPECT_EQ(candidates[3].effective_suffix_point_count, 400u);
  EXPECT_EQ(candidates[4].effective_suffix_point_count, 500u);
  EXPECT_EQ(candidates[0].cut_index, 400u);
  EXPECT_EQ(candidates[1].cut_index, 340u);
  EXPECT_EQ(candidates[2].cut_index, 260u);
  EXPECT_EQ(candidates[3].cut_index, 100u);
  EXPECT_EQ(candidates[4].cut_index, 0u);
  EXPECT_TRUE(candidates.back().is_full_path);
  ASSERT_EQ(candidates.back().prefix_including_cut.size(), 1u);
  EXPECT_EQ(candidates.back().cut.header.frame_id, "map");
}

TEST(ThetaStateSuffixSelection, ClampedCandidatesWithTheSameCutAreEmittedOnce)
{
  using smac_lattice_planner_mbf::theta_state_suffix::makeThetaPrefixCutCandidates;
  const auto candidates = makeThetaPrefixCutCandidates(straightPath(200u));

  ASSERT_EQ(candidates.size(), 3u);
  EXPECT_EQ(candidates[0].requested_suffix_point_count, 100u);
  EXPECT_EQ(candidates[0].cut_index, 100u);
  EXPECT_EQ(candidates[1].requested_suffix_point_count, 160u);
  EXPECT_EQ(candidates[1].cut_index, 40u);
  EXPECT_EQ(candidates[2].requested_suffix_point_count, 240u);
  EXPECT_EQ(candidates[2].effective_suffix_point_count, 200u);
  EXPECT_EQ(candidates[2].cut_index, 0u);
  EXPECT_TRUE(candidates[2].is_full_path);

  const auto very_short = makeThetaPrefixCutCandidates(straightPath(80u));
  ASSERT_EQ(very_short.size(), 1u);
  EXPECT_TRUE(very_short.front().is_full_path);
  EXPECT_EQ(very_short.front().cut_index, 0u);
}

TEST(ThetaStateSuffixSelection, RejectsEmptyPathAndZeroPointCount)
{
  using smac_lattice_planner_mbf::theta_state_suffix::makeThetaPrefixCutCandidates;
  using smac_lattice_planner_mbf::theta_state_suffix::selectThetaPrefixCut;
  const PosePath empty;
  EXPECT_THROW(selectThetaPrefixCut(empty, 100u), std::invalid_argument);
  EXPECT_THROW(makeThetaPrefixCutCandidates(empty), std::invalid_argument);
  EXPECT_THROW(selectThetaPrefixCut(straightPath(2u), 0u), std::invalid_argument);
}

TEST(ThetaStatePrefixSelection, StartsAtFortyAndWidensWithoutDuplicateClampedJoins)
{
  using smac_lattice_planner_mbf::theta_state_suffix::makeThetaPrefixJoinCandidates;
  EXPECT_EQ(
    smac_lattice_planner_mbf::theta_state_suffix::kMinimumThetaPosesForComposite,
    141u);
  const auto candidates = makeThetaPrefixJoinCandidates(straightPath(130u));

  ASSERT_EQ(candidates.size(), 4u);
  EXPECT_EQ(candidates[0].requested_join_index, 40u);
  EXPECT_EQ(candidates[0].join_index, 40u);
  EXPECT_DOUBLE_EQ(candidates[0].join.pose.position.x, 2.0);
  EXPECT_EQ(candidates[0].theta_from_join.size(), 90u);
  EXPECT_FALSE(candidates[0].reaches_theta_goal);
  EXPECT_EQ(candidates[1].join_index, 80u);
  EXPECT_EQ(candidates[2].join_index, 120u);
  EXPECT_EQ(candidates[3].join_index, 129u);
  EXPECT_TRUE(candidates[3].reaches_theta_goal);

  const auto short_candidates = makeThetaPrefixJoinCandidates(straightPath(20u));
  ASSERT_EQ(short_candidates.size(), 1u);
  EXPECT_EQ(short_candidates.front().join_index, 19u);
  EXPECT_TRUE(short_candidates.front().reaches_theta_goal);
}

TEST(ThetaStatePrefixSelection, RejectsEmptyPathAndZeroJoinIndex)
{
  using smac_lattice_planner_mbf::theta_state_suffix::makeThetaPrefixJoinCandidates;
  using smac_lattice_planner_mbf::theta_state_suffix::selectThetaPrefixJoin;
  EXPECT_THROW(makeThetaPrefixJoinCandidates(PosePath{}), std::invalid_argument);
  EXPECT_THROW(selectThetaPrefixJoin(straightPath(10u), 0u), std::invalid_argument);
}

TEST(ThetaStateSuffixSelection, CutsThirtyPosesBeforeTheUnsafeSegment)
{
  using smac_lattice_planner_mbf::theta_state_suffix::
    selectThetaPrefixCutBeforeUnsafeSegment;
  const PosePath theta = straightPath(1500u);
  const auto selected = selectThetaPrefixCutBeforeUnsafeSegment(theta, 1048u, 30u);

  EXPECT_EQ(selected.requested_suffix_point_count, 482u);
  EXPECT_EQ(selected.effective_suffix_point_count, 482u);
  EXPECT_EQ(selected.cut_index, 1018u);
  ASSERT_EQ(selected.prefix_including_cut.size(), 1019u);
  EXPECT_DOUBLE_EQ(selected.cut.pose.position.x, 50.9);
  EXPECT_FALSE(selected.is_full_path);
  // The failed edge 1048 -> 1049 is owned by State, with poses 1018..1048
  // providing the requested deterministic approach region.
  EXPECT_LT(selected.prefix_including_cut.size(), 1049u);
}

TEST(ThetaStateSuffixSelection, UnsafeSegmentLookbackClampsAtPathStart)
{
  using smac_lattice_planner_mbf::theta_state_suffix::
    selectThetaPrefixCutBeforeUnsafeSegment;
  const auto selected = selectThetaPrefixCutBeforeUnsafeSegment(
    straightPath(100u), 20u, 30u);

  EXPECT_EQ(selected.cut_index, 0u);
  EXPECT_TRUE(selected.is_full_path);
  EXPECT_EQ(selected.effective_suffix_point_count, 100u);
  ASSERT_EQ(selected.prefix_including_cut.size(), 1u);
}

TEST(ThetaStateSuffixSelection, RejectsInvalidUnsafeSegmentSelection)
{
  using smac_lattice_planner_mbf::theta_state_suffix::
    selectThetaPrefixCutBeforeUnsafeSegment;
  const PosePath empty;
  EXPECT_THROW(
    selectThetaPrefixCutBeforeUnsafeSegment(empty, 0u, 30u),
    std::invalid_argument);
  EXPECT_THROW(
    selectThetaPrefixCutBeforeUnsafeSegment(straightPath(1u), 0u, 30u),
    std::invalid_argument);
  EXPECT_THROW(
    selectThetaPrefixCutBeforeUnsafeSegment(straightPath(3u), 2u, 30u),
    std::invalid_argument);
  EXPECT_THROW(
    selectThetaPrefixCutBeforeUnsafeSegment(straightPath(3u), 0u, 0u),
    std::invalid_argument);
  EXPECT_THROW(
    selectThetaPrefixCutBeforeUnsafeSegment(
      straightPath(3u), std::numeric_limits<std::size_t>::max(), 30u),
    std::invalid_argument);
}

TEST(ThetaStateSuffixStitch, RemovesOnlyTheSharedIdenticalSE2Pose)
{
  using smac_lattice_planner_mbf::theta_state_suffix::stitchThetaPrefixAndStateSuffix;
  const PosePath prefix{makePose(0.0, 0.0, 0.0), makePose(1.0, 0.0, 0.0)};
  const PosePath suffix{makePose(1.0, 0.0, 0.0), makePose(2.0, 0.0, 0.0)};
  const PosePath stitched = stitchThetaPrefixAndStateSuffix(prefix, suffix);

  ASSERT_EQ(stitched.size(), 3u);
  EXPECT_DOUBLE_EQ(stitched[0].pose.position.x, 0.0);
  EXPECT_DOUBLE_EQ(stitched[1].pose.position.x, 1.0);
  EXPECT_DOUBLE_EQ(stitched[2].pose.position.x, 2.0);
}

TEST(ThetaStateSuffixStitch, PreservesSamePositionWithDifferentYawAsRotation)
{
  using smac_lattice_planner_mbf::theta_state_suffix::stitchThetaPrefixAndStateSuffix;
  const PosePath prefix{makePose(0.0, 0.0, 0.0), makePose(1.0, 0.0, 0.0)};
  const PosePath suffix{makePose(1.0, 0.0, M_PI_2), makePose(1.0, 1.0, M_PI_2)};
  const PosePath stitched = stitchThetaPrefixAndStateSuffix(prefix, suffix);

  ASSERT_EQ(stitched.size(), 4u);
  EXPECT_DOUBLE_EQ(stitched[1].pose.position.x, stitched[2].pose.position.x);
  EXPECT_DOUBLE_EQ(stitched[1].pose.position.y, stitched[2].pose.position.y);
  EXPECT_FALSE(smac_lattice_planner_mbf::theta_state_suffix::sameSE2(
    stitched[1], stitched[2]));
}

TEST(ThetaStateSuffixStitch, TreatsWrappedYawAsTheSameSE2Pose)
{
  using smac_lattice_planner_mbf::theta_state_suffix::stitchThetaPrefixAndStateSuffix;
  const PosePath prefix{makePose(0.0, 0.0, 0.0), makePose(1.0, 0.0, M_PI)};
  const PosePath suffix{makePose(1.0, 0.0, -M_PI), makePose(0.0, 0.0, M_PI)};
  const PosePath stitched = stitchThetaPrefixAndStateSuffix(prefix, suffix);

  ASSERT_EQ(stitched.size(), 3u);
  EXPECT_TRUE(smac_lattice_planner_mbf::theta_state_suffix::sameSE2(
    prefix.back(), suffix.front()));
}

TEST(ThetaStateSuffixStitch, RejectsUncheckedPositionGapOrFrameMismatch)
{
  using smac_lattice_planner_mbf::theta_state_suffix::stitchThetaPrefixAndStateSuffix;
  const PosePath prefix{makePose(0.0, 0.0, 0.0), makePose(1.0, 0.0, 0.0)};
  EXPECT_THROW(
    stitchThetaPrefixAndStateSuffix(
      prefix, PosePath{makePose(1.01, 0.0, 0.0), makePose(2.0, 0.0, 0.0)}),
    std::invalid_argument);
  EXPECT_THROW(
    stitchThetaPrefixAndStateSuffix(
      prefix, PosePath{makePose(1.0, 0.0, 0.0, "odom")}),
    std::invalid_argument);
}

TEST(ThetaStateSuffixStitch, RejectsInvalidPoseAndInvalidTolerance)
{
  using smac_lattice_planner_mbf::theta_state_suffix::stitchThetaPrefixAndStateSuffix;
  PosePath suffix{makePose(1.0, 0.0, 0.0)};
  suffix.front().pose.position.x = std::numeric_limits<double>::quiet_NaN();
  EXPECT_THROW(
    stitchThetaPrefixAndStateSuffix(
      PosePath{makePose(1.0, 0.0, 0.0)}, suffix),
    std::invalid_argument);
  EXPECT_THROW(
    stitchThetaPrefixAndStateSuffix(
      PosePath{makePose(1.0, 0.0, 0.0)},
      PosePath{makePose(1.0, 0.0, 0.0)}, -1.0),
    std::invalid_argument);
}

TEST(ThetaStateSuffixDirection, MakesOpposedMeasuredStartAnExplicitRotation)
{
  using smac_lattice_planner_mbf::theta_state_suffix::containsOnlyForwardOrRotation;
  using smac_lattice_planner_mbf::theta_state_suffix::ensureExplicitInitialTangentRotation;
  PosePath path{
    makePose(0.0, 0.0, M_PI),
    makePose(1.0, 0.0, 0.0),
    makePose(2.0, 0.0, 0.0)};
  std::string reason;
  EXPECT_FALSE(containsOnlyForwardOrRotation(path, reason));

  ASSERT_TRUE(ensureExplicitInitialTangentRotation(path, reason)) << reason;
  ASSERT_GT(path.size(), 4u);
  EXPECT_NEAR(tf2::getYaw(path[0].pose.orientation), M_PI, 1e-9);
  for (std::size_t index = 0u; index + 1u < path.size() - 2u; ++index) {
    EXPECT_DOUBLE_EQ(path[index].pose.position.x, path[index + 1u].pose.position.x);
    EXPECT_DOUBLE_EQ(path[index].pose.position.y, path[index + 1u].pose.position.y);
    EXPECT_LE(
      std::abs(angles::shortest_angular_distance(
        tf2::getYaw(path[index].pose.orientation),
        tf2::getYaw(path[index + 1u].pose.orientation))),
      M_PI / 16.0 + 1e-9);
  }
  EXPECT_NEAR(tf2::getYaw(path[path.size() - 3u].pose.orientation), 0.0, 1e-9);
  EXPECT_TRUE(containsOnlyForwardOrRotation(path, reason)) << reason;
}

TEST(ThetaStateSuffixCanonicalStart, RemovesSeedAndPrimitiveBoundaryBeforeForwardMotion)
{
  using smac_lattice_planner_mbf::theta_state_suffix::
    canonicalizeInitialLatticeMotion;
  using smac_lattice_planner_mbf::theta_state_suffix::containsContinuousForwardOnly;
  PosePath path{
    makePose(0.0, 0.0, 0.0),
    makePose(0.0, 0.0, 0.005),
    makePose(0.0, 0.0, 0.008),
    makePose(0.05, 0.002, 0.04),
    makePose(0.10, 0.004, 0.04)};

  EXPECT_EQ(canonicalizeInitialLatticeMotion(path, 0.30, 0.20, 1e-6, 0.02), 2u);
  ASSERT_EQ(path.size(), 3u);
  std::string reason;
  EXPECT_TRUE(containsContinuousForwardOnly(path, reason, 1e-4, 1e-6, 0.20)) << reason;
}

TEST(ThetaStateSuffixCanonicalStart, RemovesObservedSubMillimetreQuantizedSeed)
{
  using smac_lattice_planner_mbf::theta_state_suffix::
    canonicalizeInitialLatticeMotion;
  using smac_lattice_planner_mbf::theta_state_suffix::containsContinuousForwardOnly;
  constexpr double kObservedYawResidual = 0.007895;
  // Costmap/world conversion may leave a few 1e-5 metres between the exact
  // start and the graph seed. The motion audit deliberately treats that as
  // the same position, so start normalization must use the identical 1e-4 m
  // threshold. This is a heading-bin representation, not a searched rotation.
  PosePath path{
    makePose(0.0, 0.0, 0.0),
    makePose(0.00004, -0.00003, kObservedYawResidual),
    makePose(0.05, 0.001, 0.02),
    makePose(0.10, 0.002, 0.02)};

  EXPECT_EQ(canonicalizeInitialLatticeMotion(path, 0.30, 0.20, 1e-4, 0.02), 1u);
  ASSERT_EQ(path.size(), 3u);
  EXPECT_DOUBLE_EQ(path.front().pose.position.x, 0.0);
  EXPECT_DOUBLE_EQ(tf2::getYaw(path.front().pose.orientation), 0.0);
  std::string reason;
  EXPECT_TRUE(containsContinuousForwardOnly(path, reason, 1e-4, 1e-6, 0.20)) << reason;
}

TEST(ThetaStateSuffixCanonicalStart, PreservesTheSearchedInitialRotation)
{
  using smac_lattice_planner_mbf::theta_state_suffix::
    canonicalizeInitialLatticeMotion;
  using smac_lattice_planner_mbf::theta_state_suffix::containsContinuousForwardOnly;
  PosePath path{
    makePose(0.0, 0.0, 0.0),
    makePose(0.0, 0.0, 0.005),
    makePose(0.0, 0.0, 0.008),
    makePose(0.0, 0.0, 0.20),
    makePose(0.05 * std::cos(0.20), 0.05 * std::sin(0.20), 0.20)};

  EXPECT_EQ(canonicalizeInitialLatticeMotion(path, 0.30, 0.20, 1e-6, 0.02), 2u);
  ASSERT_EQ(path.size(), 3u);
  EXPECT_NEAR(tf2::getYaw(path[1].pose.orientation), 0.20, 1e-9);
  std::string reason;
  EXPECT_FALSE(containsContinuousForwardOnly(path, reason, 1e-4, 1e-6, 0.20));
  EXPECT_NE(reason.find("same-position rotation"), std::string::npos) << reason;
}

TEST(ThetaStateSuffixCanonicalStart, DoesNotHideARealMisalignedStartSeed)
{
  using smac_lattice_planner_mbf::theta_state_suffix::
    canonicalizeInitialLatticeMotion;
  using smac_lattice_planner_mbf::theta_state_suffix::containsContinuousForwardOnly;
  constexpr double kSeedYaw = 0.25;
  PosePath path{
    makePose(0.0, 0.0, 0.0),
    makePose(0.0, 0.0, kSeedYaw),
    makePose(0.0, 0.0, kSeedYaw + 0.002),
    makePose(0.05 * std::cos(kSeedYaw), 0.05 * std::sin(kSeedYaw), kSeedYaw)};

  EXPECT_EQ(canonicalizeInitialLatticeMotion(path, 0.30, 0.20, 1e-6, 0.02), 1u);
  ASSERT_EQ(path.size(), 3u);
  EXPECT_NEAR(tf2::getYaw(path[1].pose.orientation), kSeedYaw, 1e-9);
  std::string reason;
  EXPECT_FALSE(containsContinuousForwardOnly(path, reason, 1e-4, 1e-6, 0.20));
}

TEST(ThetaStateSuffixDirection, RejectsReverseAndSidewaysTranslation)
{
  using smac_lattice_planner_mbf::theta_state_suffix::containsOnlyForwardOrRotation;
  std::string reason;
  EXPECT_FALSE(containsOnlyForwardOrRotation(
      PosePath{makePose(0.0, 0.0, 0.0), makePose(-1.0, 0.0, 0.0)}, reason));
  EXPECT_FALSE(containsOnlyForwardOrRotation(
      PosePath{makePose(0.0, 0.0, 0.0), makePose(0.0, 1.0, 0.0)}, reason));
  EXPECT_TRUE(containsOnlyForwardOrRotation(
      PosePath{
        makePose(0.0, 0.0, 0.0),
        makePose(0.0, 0.0, M_PI_2),
        makePose(0.0, 1.0, M_PI_2)}, reason)) << reason;
}

TEST(ThetaStateSuffixDirection, RejectsObservedShortEdgeWithOpposedEndpointHeadings)
{
  using smac_lattice_planner_mbf::theta_state_suffix::
    containsKinematicallyContinuousForwardOrRotation;
  using smac_lattice_planner_mbf::theta_state_suffix::containsOnlyForwardOrRotation;

  constexpr double edge_length = 0.01539626955;
  constexpr double edge_yaw = -1.740293628;
  constexpr double next_yaw = 1.325817664;
  const double start_x = -5.1318;
  const double start_y = -17.6574;
  const PosePath observed{
    makePose(start_x, start_y, 1.753976),
    makePose(start_x, start_y, edge_yaw),
    makePose(
      start_x + edge_length * std::cos(edge_yaw),
      start_y + edge_length * std::sin(edge_yaw), next_yaw)};

  std::string reason;
  // The legacy edge rule sees the perfect +1 start projection and therefore
  // demonstrates why a separate endpoint-continuity audit is necessary.
  ASSERT_TRUE(containsOnlyForwardOrRotation(observed, reason)) << reason;
  EXPECT_FALSE(containsKinematicallyContinuousForwardOrRotation(observed, reason));
  EXPECT_NE(reason.find("opposite endpoint heading projections"), std::string::npos)
    << reason;
}

TEST(ThetaStateSuffixDirection, RejectsShortNearPiFlipBelowOpposingProjectionThreshold)
{
  using smac_lattice_planner_mbf::theta_state_suffix::
    containsKinematicallyContinuousForwardOrRotation;

  // Along an eastbound edge, 70 and -100 degrees produce +0.342 and -0.174
  // projections. They are not both beyond the default 0.25 opposed threshold,
  // but a 170-degree yaw jump over one nominal 5 cm costmap cell is still
  // discontinuous. The float-derived cell size is deliberately just above
  // 0.05 m to cover the production-resolution rounding boundary.
  const PosePath short_flip{
    makePose(0.0, 0.0, 70.0 * M_PI / 180.0),
    makePose(0.050000000745, 0.0, -100.0 * M_PI / 180.0)};
  std::string reason;
  EXPECT_FALSE(containsKinematicallyContinuousForwardOrRotation(short_flip, reason));
  EXPECT_NE(reason.find("short translating edge"), std::string::npos) << reason;
  EXPECT_NE(reason.find("near-pi yaw flip"), std::string::npos) << reason;
}

TEST(ThetaStateSuffixDirection, AllowsNormalStartForwardArcsAndExplicitRotation)
{
  using smac_lattice_planner_mbf::theta_state_suffix::
    containsKinematicallyContinuousForwardOrRotation;
  std::string reason;

  const PosePath normal_start_and_arcs{
    makePose(0.0, 0.0, 0.0),
    makePose(0.03, 0.002, 0.10),
    makePose(0.08, 0.010, 0.20),
    makePose(1.08, 1.010, M_PI_2)};
  EXPECT_TRUE(containsKinematicallyContinuousForwardOrRotation(
      normal_start_and_arcs, reason)) << reason;

  const PosePath explicit_rotation_then_forward{
    makePose(0.0, 0.0, M_PI),
    makePose(0.0, 0.0, 0.0),
    makePose(1.0, 0.0, 0.0)};
  EXPECT_TRUE(containsKinematicallyContinuousForwardOrRotation(
      explicit_rotation_then_forward, reason)) << reason;
}

TEST(ThetaStatePrefixDirection, RequiresContinuousForwardAndRejectsInPlaceRotation)
{
  using smac_lattice_planner_mbf::theta_state_suffix::containsContinuousForwardOnly;
  std::string reason;
  EXPECT_TRUE(containsContinuousForwardOnly(
      PosePath{
        makePose(0.0, 0.0, 0.0),
        makePose(0.10, 0.01, 0.10),
        makePose(0.20, 0.03, 0.20)}, reason)) << reason;
  EXPECT_FALSE(containsContinuousForwardOnly(
      PosePath{
        makePose(0.0, 0.0, 0.0),
        makePose(0.0, 0.0, 0.10),
        makePose(0.10, 0.01, 0.10)}, reason));
  EXPECT_NE(reason.find("same-position rotation"), std::string::npos) << reason;

  EXPECT_FALSE(containsContinuousForwardOnly(
      PosePath{
        makePose(0.0, 0.0, M_PI_2),
        makePose(0.10, 0.0, 0.0),
        makePose(0.20, 0.0, 0.0)}, reason));
  EXPECT_NE(reason.find("exact start yaw"), std::string::npos) << reason;

  EXPECT_FALSE(containsContinuousForwardOnly(
      PosePath{
        makePose(0.0, 0.0, 0.0),
        makePose(0.0, 0.0, 0.0)}, reason));
  EXPECT_NE(reason.find("no translated edge"), std::string::npos) << reason;
}

TEST(ThetaStatePrefixJoin, AcceptsMatchingTangentAndCurvature)
{
  using smac_lattice_planner_mbf::theta_state_suffix::
    joinIsPositionHeadingCurvatureContinuous;
  const PosePath state_prefix{
    makePose(0.0, 0.0, 0.0),
    makePose(0.10, 0.0, 0.0),
    makePose(0.20, 0.0, 0.0)};
  const PosePath theta_from_join{
    makePose(0.20, 0.0, 0.0),
    makePose(0.30, 0.0, 0.0),
    makePose(0.40, 0.0, 0.0)};
  std::string reason;
  EXPECT_TRUE(joinIsPositionHeadingCurvatureContinuous(
      state_prefix, theta_from_join, reason)) << reason;
}

TEST(ThetaStatePrefixJoin, RejectsHeadingCurvatureAndPositionDiscontinuities)
{
  using smac_lattice_planner_mbf::theta_state_suffix::
    joinIsPositionHeadingCurvatureContinuous;
  const PosePath state_prefix{
    makePose(0.0, 0.0, 0.0),
    makePose(0.10, 0.0, 0.0),
    makePose(0.20, 0.0, 0.0)};
  std::string reason;
  EXPECT_FALSE(joinIsPositionHeadingCurvatureContinuous(
      state_prefix,
      PosePath{
        makePose(0.21, 0.0, 0.0),
        makePose(0.31, 0.0, 0.0),
        makePose(0.41, 0.0, 0.0)}, reason));

  EXPECT_FALSE(joinIsPositionHeadingCurvatureContinuous(
      state_prefix,
      PosePath{
        makePose(0.20, 0.0, 0.0),
        makePose(0.20, 0.10, M_PI_2),
        makePose(0.20, 0.20, M_PI_2)}, reason));
  EXPECT_NE(reason.find("tangent error"), std::string::npos) << reason;

  EXPECT_FALSE(joinIsPositionHeadingCurvatureContinuous(
      state_prefix,
      PosePath{
        makePose(0.20, 0.0, 0.0),
        makePose(0.30, 0.0, 0.0),
        makePose(0.30, 0.10, M_PI_2)}, reason));
  EXPECT_NE(reason.find("curvature jump"), std::string::npos) << reason;
}

TEST(ThetaStatePrefixJoin, AllowsSerializedMinimumRadiusNumericalErrorOnly)
{
  using smac_lattice_planner_mbf::theta_state_suffix::
    joinIsPositionHeadingCurvatureContinuous;
  const auto make_incoming = [] (double curvature) {
      constexpr double edge_length = 0.05;
      const double first_bearing = -curvature * edge_length;
      return PosePath{
        makePose(
          -edge_length - edge_length * std::cos(first_bearing),
          -edge_length * std::sin(first_bearing), first_bearing),
        makePose(-edge_length, 0.0, 0.0),
        makePose(0.0, 0.0, 0.0)};
    };
  const PosePath straight_from_join{
    makePose(0.0, 0.0, 0.0),
    makePose(0.05, 0.0, 0.0),
    makePose(0.10, 0.0, 0.0)};
  std::string reason;

  // Five-decimal lattice coordinates measure the nominal 2.5 rad/m arc as
  // about 2.50172 rad/m.  That serialization error must not reject a valid
  // 0.40 m-radius join.
  EXPECT_TRUE(joinIsPositionHeadingCurvatureContinuous(
      make_incoming(2.50172), straight_from_join, reason,
      1e-6, 1e-6, 0.20, 2.5)) << reason;

  EXPECT_FALSE(joinIsPositionHeadingCurvatureContinuous(
      make_incoming(2.51), straight_from_join, reason,
      1e-6, 1e-6, 0.20, 2.5));
  EXPECT_NE(reason.find("curvature jump"), std::string::npos) << reason;
}

TEST(ThetaStateSuffixDirection, MissingTranslationForcesStateFallback)
{
  using smac_lattice_planner_mbf::theta_state_suffix::ensureExplicitInitialTangentRotation;
  PosePath path{makePose(1.0, 2.0, 0.0), makePose(1.0, 2.0, M_PI_2)};
  std::string reason;
  EXPECT_FALSE(ensureExplicitInitialTangentRotation(path, reason));
  EXPECT_FALSE(reason.empty());
}

TEST(ThetaStateSuffixTerminal, RejectsOnlyTheFinalStationaryYawRepair)
{
  using smac_lattice_planner_mbf::theta_state_suffix::terminalAvoidsStationaryYawRepair;
  std::string reason;
  const PosePath terminal_repair{
    makePose(0.0, 0.0, 0.0),
    makePose(1.0, 0.0, 0.0),
    makePose(1.0, 0.0, 0.1)};
  EXPECT_FALSE(terminalAvoidsStationaryYawRepair(terminal_repair, reason));
  EXPECT_FALSE(reason.empty());

  // A duplicate after the repair must not hide the last meaningful edge.
  PosePath hidden_repair = terminal_repair;
  hidden_repair.push_back(terminal_repair.back());
  EXPECT_FALSE(terminalAvoidsStationaryYawRepair(hidden_repair, reason));
}

TEST(ThetaStateSuffixTerminal, AllowsEarlierRotationAndMovingArrival)
{
  using smac_lattice_planner_mbf::theta_state_suffix::terminalAvoidsStationaryYawRepair;
  std::string reason;
  PosePath path{
    makePose(0.0, 0.0, 0.0),
    makePose(0.0, 0.0, M_PI_2),
    makePose(0.0, 1.0, M_PI_2)};
  EXPECT_TRUE(terminalAvoidsStationaryYawRepair(path, reason)) << reason;

  // Benign duplicate terminal samples do not change the meaningful arrival.
  path.push_back(path.back());
  EXPECT_TRUE(terminalAvoidsStationaryYawRepair(path, reason)) << reason;
}

TEST(ThetaStateCompositeStitch, JoinsPrefixMiddleAndSuffixExactlyOnce)
{
  using smac_lattice_planner_mbf::theta_state_suffix::
    stitchStatePrefixThetaMiddleStateSuffix;
  const PosePath prefix{
    makePose(0.0, 0.0, 0.0), makePose(1.0, 0.0, 0.0)};
  const PosePath middle{
    makePose(1.0, 0.0, 0.0), makePose(2.0, 0.0, 0.0)};
  const PosePath suffix{
    makePose(2.0, 0.0, 0.0), makePose(3.0, 0.0, 0.0)};
  const auto stitched = stitchStatePrefixThetaMiddleStateSuffix(
    prefix, middle, suffix);
  ASSERT_EQ(stitched.size(), 4u);
  for (std::size_t index = 0u; index < stitched.size(); ++index) {
    EXPECT_DOUBLE_EQ(stitched[index].pose.position.x, static_cast<double>(index));
  }
}

int main(int argc, char ** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

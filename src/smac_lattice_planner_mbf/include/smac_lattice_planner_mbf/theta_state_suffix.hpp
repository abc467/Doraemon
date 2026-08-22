// Copyright 2026 Clean Robot Navigation Team
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <array>
#include <cstddef>
#include <string>
#include <vector>

#include <geometry_msgs/PoseStamped.h>

namespace smac_lattice_planner_mbf
{
namespace theta_state_suffix
{

using PosePath = std::vector<geometry_msgs::PoseStamped>;

// These values count trailing Theta poses, not metric distance and not path
// intervals. In a strictly 5 cm sampled path, 100 poses span 99 intervals
// (approximately 4.95 m). The caller may use a different count if it requires
// exactly 100 intervals / 5.00 m.
constexpr std::array<std::size_t, 4u> kSuffixPointCountCandidates{{
    100u, 160u, 240u, 400u}};

// These are forward indices measured from the exact Theta start. Index 40 is
// therefore the point after 40 reference intervals (about 2.0 m at the
// configured 5 cm maximum spacing). Wider joins are attempted only when the
// nearer State prefix cannot arrive as one continuous forward motion.
constexpr std::array<std::size_t, 4u> kPrefixJoinIndexCandidates{{
    40u, 80u, 120u, 160u}};

// A useful three-part composite must retain at least one translated Theta
// interval between the first State-prefix join and the first State-suffix
// window. With join index 40 and a 100-pose suffix, that requires 141 poses:
// prefix ends at 40, Theta contributes edge 40->41, suffix starts at 41.
constexpr std::size_t kMinimumThetaPosesForComposite =
  kPrefixJoinIndexCandidates.front() + kSuffixPointCountCandidates.front() + 1u;

struct ThetaPrefixJoin
{
  std::size_t requested_join_index{0u};
  std::size_t join_index{0u};
  bool reaches_theta_goal{false};
  geometry_msgs::PoseStamped join;
  PosePath theta_from_join;
};

/**
 * @brief Select a forward Theta point for an exact-start State prefix.
 *
 * The returned Theta subpath starts at the shared join pose. A requested index
 * beyond the path is clamped to the final pose. Callers normally treat that as
 * a full-State request rather than constructing an empty Theta middle.
 */
ThetaPrefixJoin selectThetaPrefixJoin(
  const PosePath & theta_reference,
  std::size_t requested_join_index);

/** @brief Build unique 40/80/120/160 forward join candidates. */
std::vector<ThetaPrefixJoin> makeThetaPrefixJoinCandidates(
  const PosePath & theta_reference);

/**
 * @brief One deterministic Theta-prefix / State-suffix hand-off.
 *
 * prefix_including_cut always contains the original Theta start and ends at
 * cut. The State suffix must start at this exact cut SE(2) pose. A full-path
 * candidate therefore has cut_index == 0 and a one-pose Theta prefix; State
 * Lattice owns the complete start-to-goal motion.
 */
struct ThetaPrefixCut
{
  std::size_t requested_suffix_point_count{0u};
  std::size_t effective_suffix_point_count{0u};
  std::size_t cut_index{0u};
  bool is_full_path{false};
  PosePath prefix_including_cut;
  geometry_msgs::PoseStamped cut;
};

/**
 * @brief Select a cut using a count of trailing Theta poses.
 *
 * If point_count exceeds the path size it is clamped to the full path. The
 * selected cut is the first pose of that trailing suffix, and is deliberately
 * shared by prefix and suffix.
 *
 * @throws std::invalid_argument for an empty path or point_count == 0.
 */
ThetaPrefixCut selectThetaPrefixCut(
  const PosePath & theta_reference,
  std::size_t point_count);

/**
 * @brief Select a State hand-off before a failed Theta footprint segment.
 *
 * unsafe_segment_index identifies the edge joining poses i and i + 1. The
 * returned cut is moved toward the path start by lookback_points poses, so the
 * State suffix owns both the failed edge and a deterministic approach region.
 * If the failure lies closer to the start than the requested lookback, the
 * cut is clamped to pose zero.
 *
 * @throws std::invalid_argument for an empty path, an out-of-range segment,
 * or lookback_points == 0.
 */
ThetaPrefixCut selectThetaPrefixCutBeforeUnsafeSegment(
  const PosePath & theta_reference,
  std::size_t unsafe_segment_index,
  std::size_t lookback_points = 30u);

/**
 * @brief Build the 100/160/240/400/full candidates in widening order.
 *
 * Candidates which clamp to the same effective cut are returned once. This
 * is important for short Theta paths, where several requested windows and the
 * explicit full candidate all mean cut_index == 0.
 *
 * @throws std::invalid_argument for an empty path.
 */
std::vector<ThetaPrefixCut> makeThetaPrefixCutCandidates(
  const PosePath & theta_reference);

/** @brief Compare planar position and wrapped yaw; z/roll/pitch are ignored. */
bool sameSE2(
  const geometry_msgs::PoseStamped & first,
  const geometry_msgs::PoseStamped & second,
  double position_epsilon = 1e-9,
  double yaw_epsilon = 1e-9);

/**
 * @brief Remove representation-only poses at a State-Lattice path start.
 *
 * NodeLattice backtracing emits both the selected start-heading seed and the
 * first motion primitive's start sample.  When the measured (continuous)
 * start is prepended, those graph representations can look like one or two
 * same-position rotations even though the first searched primitive actually
 * translates.  This helper removes only those redundant boundary poses.
 *
 * A seed before a translating primitive is removed only when the first
 * departure is already aligned with the measured start yaw.  If the searched
 * path really begins with an in-place rotation, its first changed-yaw sample
 * remains in the path.  The caller must still run its normal motion audit and
 * continuous footprint proof after canonicalization.
 *
 * position_epsilon must match the translation epsilon used by the subsequent
 * motion audit.  Otherwise a sub-millimetre floating-point displacement can
 * survive this normalization and then be reclassified as a same-position
 * rotation by the audit.
 *
 * @return Number of removed poses (zero, one, or two).
 */
std::size_t canonicalizeInitialLatticeMotion(
  PosePath & path,
  double max_seed_yaw_error,
  double max_forward_departure_heading_error,
  double position_epsilon = 1e-4,
  double primitive_boundary_yaw_epsilon = 0.02);

/**
 * @brief Return the directional Voronoi half-cell around a lattice heading.
 *
 * State-Lattice heading tables are not necessarily uniformly spaced.  The
 * admissible quantization residual therefore depends on both the selected bin
 * and the side of that bin on which the requested yaw lies.  The returned
 * value is half the angular gap to the neighboring heading on that side plus
 * the supplied serialization margin.
 *
 * @throws std::invalid_argument for malformed headings, an invalid selected
 * bin, or non-finite inputs.
 */
double directionalHeadingResidualLimit(
  const std::vector<float> & heading_angles,
  std::size_t selected_bin,
  double requested_yaw,
  double serialization_margin = 0.002);

/**
 * @brief Make the measured start-to-Theta-tangent alignment an explicit
 * direction-encoded sequence of bounded same-position rotation edges.
 *
 * The shortest signed rotation is subdivided so nav_msgs/Path does not carry
 * one large ambiguous yaw jump. The caller must continuously validate the
 * inserted sweep. If no translated Theta edge exists, false is returned so
 * State/FULL planning can take over.
 */
bool ensureExplicitInitialTangentRotation(
  PosePath & path,
  std::string & reason,
  double position_epsilon = 1e-6,
  double yaw_epsilon = 1e-6,
  double max_rotation_step = 0.19634954084936207);

/**
 * @brief Audit differential-drive path semantics using MPPI's edge rule.
 *
 * Every nonzero translation must project forward at either its start or end
 * heading. Same-position yaw changes are allowed as explicit rotations.
 */
bool containsOnlyForwardOrRotation(
  const PosePath & path,
  std::string & reason,
  double translation_epsilon = 1e-4,
  double direction_cosine_tolerance = 0.25);

/**
 * @brief Audit a Theta/State path for forward kinematic continuity.
 *
 * This includes containsOnlyForwardOrRotation() and additionally rejects a
 * translated edge when its start/end heading projections are clearly
 * opposed. It also rejects a short translated edge carrying a near-pi yaw
 * flip, even when one endpoint projection falls below the opposed-direction
 * threshold. Same-position yaw changes remain valid explicit rotations.
 * Normal forward arcs may change yaw while translating as long as they do not
 * satisfy either discontinuity condition.
 */
bool containsKinematicallyContinuousForwardOrRotation(
  const PosePath & path,
  std::string & reason,
  double translation_epsilon = 1e-4,
  double direction_cosine_tolerance = 0.25,
  double opposing_projection_tolerance = 0.25,
  double short_translation_threshold = 0.05,
  double near_pi_yaw_threshold = 2.6179938779914944);

/**
 * @brief Require a path to contain continuous forward translation only.
 *
 * Exact duplicate poses are harmless, but a meaningful same-position yaw
 * change is rejected. The first translated edge must also depart within
 * max_start_heading_error of the exact measured start yaw, so removing a
 * lattice-heading seed cannot hide an implicit initial rotation. This is the
 * primary contract for the State-owned path. Representation-only start seeds
 * must be removed before calling this function; genuine in-place alignment is
 * deliberately rejected.
 */
bool containsContinuousForwardOnly(
  const PosePath & path,
  std::string & reason,
  double translation_epsilon = 1e-4,
  double yaw_epsilon = 1e-6,
  double max_start_heading_error = 0.20);

/**
 * @brief Check geometric C1-like continuity and local curvature at a join.
 *
 * The first path must end at the same SE(2) pose where the second starts.
 * Bearings from the nearest two translated edges on each side are used to
 * check tangent error and the jump between incoming/outgoing curvature. Paths
 * without enough translated support are rejected instead of claiming an
 * unmeasured curvature match.
 */
bool joinIsPositionHeadingCurvatureContinuous(
  const PosePath & before_join,
  const PosePath & from_join,
  std::string & reason,
  double position_epsilon = 1e-6,
  double yaw_epsilon = 1e-6,
  double max_heading_error = 0.20,
  double max_curvature_jump = 2.5,
  double translation_epsilon = 1e-4);

/**
 * @brief Reject a terminal same-position yaw repair while allowing earlier rotations.
 *
 * Trailing duplicate SE(2) poses are ignored. The final meaningful edge must
 * either translate by more than position_epsilon or have no meaningful yaw
 * change. This keeps the selected lattice-bin yaw on the incoming State motion
 * rather than accepting a post-hoc in-place correction at the goal.
 */
bool terminalAvoidsStationaryYawRepair(
  const PosePath & path,
  std::string & reason,
  double position_epsilon = 1e-4,
  double yaw_epsilon = 1e-6);

/**
 * @brief Join a Theta prefix and State suffix without hiding an SE(2) motion.
 *
 * Both paths must be non-empty, internally frame-consistent, finite, and meet
 * at the same planar position. The State suffix's first pose is removed only
 * when it is also the same wrapped yaw as the Theta cut. Equal XY with a
 * different yaw is retained as an explicit in-place rotation. A positional
 * gap is rejected rather than silently manufacturing an unchecked edge.
 *
 * This helper performs structural stitching only. The caller must still run
 * continuous filled-footprint validation on every resulting segment.
 *
 * @throws std::invalid_argument when the path contract is violated.
 */
PosePath stitchThetaPrefixAndStateSuffix(
  const PosePath & theta_prefix_including_cut,
  const PosePath & state_suffix,
  double position_epsilon = 1e-9,
  double yaw_epsilon = 1e-9);

/**
 * @brief Structurally join State-prefix, Theta-middle and State-suffix paths.
 *
 * Shared identical SE(2) join poses are emitted once. Positional gaps or
 * frame mismatches throw; the caller remains responsible for motion, junction
 * and continuous footprint proofs.
 */
PosePath stitchStatePrefixThetaMiddleStateSuffix(
  const PosePath & state_prefix,
  const PosePath & theta_middle_including_joins,
  const PosePath & state_suffix,
  double position_epsilon = 1e-9,
  double yaw_epsilon = 1e-9);

}  // namespace theta_state_suffix
}  // namespace smac_lattice_planner_mbf

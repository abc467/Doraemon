// Copyright (c) 2021, Samsung Research America
// Copyright 2026 Clean Robot Navigation Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <chrono>
#include <functional>
#include <string>

#include <costmap_2d/costmap_2d.h>

#include "nav2_smac_planner/collision_checker.hpp"
#include "smac_lattice_planner_mbf/theta_state_suffix.hpp"

namespace smac_lattice_planner_mbf
{

/**
 * @brief ROS 1 parameters for Nav2 Smac's State Lattice smoother algorithm.
 *
 * The numerical smoothing defaults intentionally match Nav2. The remaining
 * fields are adapter-side atomic acceptance gates: smoothing is an optional
 * quality optimization and may never invalidate an already safe raw State
 * path.
 */
struct StateLatticeSmootherParams
{
  int max_iterations{1000};
  double w_data{0.2};
  double w_smooth{0.3};
  double tolerance{1e-10};
  bool do_refinement{true};
  int refinement_num{2};
  double max_time{1.0};

  double max_path_length_ratio{1.05};
  double max_center_cost_increase{0.0};
  double max_mean_center_cost_increase{0.5};
  double max_curvature_regression_ratio{1.05};
  double minimum_curvature_improvement{1e-3};
};

struct StatePathQuality
{
  double length_m{0.0};
  double mean_center_cost{0.0};
  double max_center_cost{0.0};
  double p95_abs_curvature_radpm{0.0};
  double max_abs_curvature_radpm{0.0};
  double curvature_total_variation_radpm{0.0};
  double max_curvature_jump_radpm{0.0};
  int curvature_direction_changes{0};
  int in_place_rotations{0};
};

struct StateLatticeSmoothingResult
{
  bool accepted{false};
  theta_state_suffix::PosePath path;
  // Populated only when a fully generated candidate is rejected by an atomic
  // quality gate. Production continues to use `path` (the untouched raw
  // solution), while offline diagnostics can inspect why the candidate lost.
  theta_state_suffix::PosePath rejected_candidate_path;
  StatePathQuality raw_quality;
  StatePathQuality candidate_quality;
  int iterations{0};
  std::string reason;
};

/**
 * @brief ROS 1 adapter of Nav2 Smac's iterative State Lattice smoother.
 *
 * It reproduces Nav2's directional segmentation, data/smooth update,
 * orientation reconstruction, optional refinement, and Dubins start/end
 * boundary enforcement. It works on a private path copy and returns an
 * accepted candidate only after collision and quality checks. The caller must
 * still run the planner's continuous full-footprint proof before committing.
 */
class StateLatticeSmoother
{
public:
  using Clock = std::chrono::steady_clock;
  using CancelChecker = std::function<bool()>;

  StateLatticeSmoother(
    StateLatticeSmootherParams params,
    double minimum_turning_radius_m);

  StateLatticeSmoothingResult smooth(
    const theta_state_suffix::PosePath & raw_path,
    const costmap_2d::Costmap2D & costmap,
    nav2_smac_planner::GridCollisionChecker & collision_checker,
    bool allow_unknown,
    const Clock::time_point & absolute_deadline,
    CancelChecker cancel_checker = CancelChecker()) const;

  static StatePathQuality summarizeQuality(
    const theta_state_suffix::PosePath & path,
    const costmap_2d::Costmap2D & costmap);

private:
  StateLatticeSmootherParams params_;
  double minimum_turning_radius_m_{0.0};
};

}  // namespace smac_lattice_planner_mbf

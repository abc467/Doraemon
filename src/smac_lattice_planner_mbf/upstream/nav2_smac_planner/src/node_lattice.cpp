// Copyright (c) 2021, Samsung Research America
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
// limitations under the License. Reserved.

#include <algorithm>
#include <chrono>
#include <cmath>
#include <fstream>
#include <limits>
#include <memory>
#include <queue>
#include <sstream>
#include <string>
#include <vector>

#include "angles/angles.h"

#include "ompl/base/ScopedState.h"
#include "ompl/base/spaces/DubinsStateSpace.h"
#include "ompl/base/spaces/ReedsSheppStateSpace.h"
#include "ompl/base/spaces/SE2StateSpace.h"

#include "nav2_smac_planner/node_lattice.hpp"

using namespace std::chrono;  // NOLINT

namespace nav2_smac_planner
{

void LatticeMotionTable::validateForwardSteeringPrimitives() const
{
  constexpr float kTranslationEpsilon = 1e-4f;
  constexpr double kAngleEpsilon = 1e-5;
  constexpr double kCurvatureRelativeTolerance = 1e-3;
  constexpr double kMinimumForwardProjection = 0.95;
  const auto heading_count = lattice_metadata.heading_angles.size();
  if (heading_count == 0u || lattice_metadata.number_of_headings != heading_count) {
    throw std::runtime_error("lattice heading metadata is empty or inconsistent");
  }
  if (!std::isfinite(lattice_metadata.min_turning_radius) ||
    lattice_metadata.min_turning_radius <= 0.0f)
  {
    throw std::runtime_error("lattice minimum turning radius must be finite and positive");
  }
  if (motion_primitives.size() != heading_count) {
    throw std::runtime_error("lattice primitive headings do not match metadata");
  }

  std::size_t primitive_count = 0u;
  std::vector<std::vector<std::size_t>> heading_successors(heading_count);
  const double maximum_curvature =
    (1.0 / static_cast<double>(lattice_metadata.min_turning_radius)) *
    (1.0 + kCurvatureRelativeTolerance) + 1e-6;
  for (std::size_t heading = 0; heading < heading_count; ++heading) {
    bool has_left = false;
    bool has_straight = false;
    bool has_right = false;
    const double start_yaw = lattice_metadata.heading_angles[heading];
    if (!std::isfinite(start_yaw)) {
      throw std::runtime_error("lattice contains a non-finite heading angle");
    }
    for (const auto & primitive : motion_primitives[heading]) {
      ++primitive_count;
      if (primitive.poses.empty()) {
        throw std::runtime_error("lattice contains an empty motion primitive");
      }
      const double rounded_start = std::round(primitive.start_angle);
      const double rounded_end = std::round(primitive.end_angle);
      if (!std::isfinite(primitive.start_angle) || !std::isfinite(primitive.end_angle) ||
        std::abs(primitive.start_angle - rounded_start) > kAngleEpsilon ||
        std::abs(primitive.end_angle - rounded_end) > kAngleEpsilon ||
        rounded_start < 0.0 || rounded_end < 0.0 ||
        rounded_start >= static_cast<double>(heading_count) ||
        rounded_end >= static_cast<double>(heading_count) ||
        static_cast<std::size_t>(rounded_start) != heading)
      {
        throw std::runtime_error("lattice primitive has an invalid heading index");
      }
      if (!std::isfinite(primitive.trajectory_length) ||
        primitive.trajectory_length < 0.0f)
      {
        throw std::runtime_error("lattice primitive has an invalid trajectory length");
      }

      double accumulated_length = 0.0;
      double previous_x = 0.0;
      double previous_y = 0.0;
      double previous_yaw = start_yaw;
      bool has_translation = false;
      for (const auto & pose : primitive.poses) {
        if (!std::isfinite(pose._x) || !std::isfinite(pose._y) ||
          !std::isfinite(pose._theta))
        {
          throw std::runtime_error("lattice primitive contains a non-finite pose");
        }
        const double dx = static_cast<double>(pose._x) - previous_x;
        const double dy = static_cast<double>(pose._y) - previous_y;
        const double distance = std::hypot(dx, dy);
        const double yaw_delta = angles::shortest_angular_distance(
          previous_yaw, static_cast<double>(pose._theta));
        if (distance > kTranslationEpsilon) {
          has_translation = true;
          accumulated_length += distance;
          const double bearing = std::atan2(dy, dx);
          const double midpoint_yaw = previous_yaw + 0.5 * yaw_delta;
          const double forward_projection = std::cos(
            angles::shortest_angular_distance(midpoint_yaw, bearing));
          if (forward_projection < kMinimumForwardProjection) {
            throw std::runtime_error(
                    "lattice translating primitive contains a non-forward segment");
          }
          const double curvature = std::abs(yaw_delta) / distance;
          if (curvature > maximum_curvature) {
            std::ostringstream message;
            message << "lattice primitive curvature " << curvature
                    << " rad/m exceeds minimum-radius bound " << maximum_curvature;
            throw std::runtime_error(message.str());
          }
        } else if (std::abs(yaw_delta) <= kAngleEpsilon &&
          distance > 0.0)
        {
          accumulated_length += distance;
        }
        previous_x = pose._x;
        previous_y = pose._y;
        previous_yaw = pose._theta;
      }

      const auto end_heading = static_cast<std::size_t>(rounded_end);
      const double metadata_end_yaw = lattice_metadata.heading_angles[end_heading];
      if (!std::isfinite(metadata_end_yaw) ||
        std::abs(angles::shortest_angular_distance(
          previous_yaw, metadata_end_yaw)) > kAngleEpsilon)
      {
        throw std::runtime_error("lattice primitive endpoint yaw disagrees with metadata");
      }
      const auto & endpoint = primitive.poses.back();
      if (!has_translation || std::hypot(endpoint._x, endpoint._y) <= kTranslationEpsilon) {
        if (primitive.trajectory_length > kTranslationEpsilon) {
          throw std::runtime_error(
                  "stationary lattice primitive has a non-zero trajectory length");
        }
        continue;
      }
      const double length_tolerance = std::max(1e-3, 0.01 * accumulated_length);
      if (std::abs(
          accumulated_length - static_cast<double>(primitive.trajectory_length)) >
        length_tolerance)
      {
        throw std::runtime_error(
                "lattice primitive trajectory length disagrees with sampled poses");
      }
      heading_successors[heading].push_back(end_heading);
      const double yaw_delta = angles::shortest_angular_distance(start_yaw, metadata_end_yaw);
      if (yaw_delta > kAngleEpsilon) {
        has_left = true;
      } else if (yaw_delta < -kAngleEpsilon) {
        has_right = true;
      } else {
        has_straight = true;
      }
    }
    if (!has_left || !has_straight || !has_right) {
      std::ostringstream message;
      message << "heading bin " << heading << " lacks translating forward ";
      if (!has_left) {
        message << "left ";
      }
      if (!has_straight) {
        message << "straight ";
      }
      if (!has_right) {
        message << "right ";
      }
      message << "primitive(s); refusing a forward-only search that can lose steering";
      throw std::runtime_error(message.str());
    }
  }

  if (primitive_count != lattice_metadata.number_of_trajectories) {
    throw std::runtime_error("lattice primitive count disagrees with metadata");
  }

  std::vector<bool> reachable(heading_count, false);
  std::vector<std::size_t> pending{0u};
  reachable[0] = true;
  while (!pending.empty()) {
    const std::size_t heading = pending.back();
    pending.pop_back();
    for (const std::size_t successor : heading_successors[heading]) {
      if (!reachable[successor]) {
        reachable[successor] = true;
        pending.push_back(successor);
      }
    }
  }
  if (std::find(reachable.begin(), reachable.end(), false) != reachable.end()) {
    throw std::runtime_error(
            "translating lattice heading graph is disconnected in forward-only mode");
  }
}

// Each of these tables are the projected motion models through
// time and space applied to the search on the current node in
// continuous map-coordinates (e.g. not meters but partial map cells)
// Currently, these are set to project *at minimum* into a neighboring
// cell. Though this could be later modified to project a certain
// amount of time or particular distance forward.
void LatticeMotionTable::initMotionModel(
  unsigned int & size_x_in,
  SearchInfo & search_info)
{
  size_x = size_x_in;
  change_penalty = search_info.change_penalty;
  non_straight_penalty = search_info.non_straight_penalty;
  cost_penalty = search_info.cost_penalty;
  reverse_penalty = search_info.reverse_penalty;
  travel_distance_reward = 1.0f - search_info.retrospective_penalty;
  allow_reverse_expansion = search_info.allow_reverse_expansion;
  rotation_penalty = search_info.rotation_penalty;
  min_turning_radius = search_info.minimum_turning_radius;
  downsample_obstacle_heuristic = search_info.downsample_obstacle_heuristic;
  use_quadratic_cost_penalty = search_info.use_quadratic_cost_penalty;

  if (current_lattice_filepath == search_info.lattice_filepath) {
    if (search_info.require_forward_steering_primitives) {
      validateForwardSteeringPrimitives();
    }
    return;
  }
  current_lattice_filepath = search_info.lattice_filepath;

  // Get the metadata about this minimum control set
  lattice_metadata = getLatticeMetadata(current_lattice_filepath);
  std::ifstream latticeFile(current_lattice_filepath);
  if (!latticeFile.is_open()) {
    throw std::runtime_error("Could not open lattice file");
  }
  nlohmann::json json;
  latticeFile >> json;
  num_angle_quantization = lattice_metadata.number_of_headings;

  if (!state_space) {
    if (lattice_metadata.motion_model == "omni") {
      // Holonomic robots: straight-line analytic expansion
      state_space = std::make_shared<ompl::base::SE2StateSpace>();
      motion_model = MotionModel::OMNI;
    } else if (!allow_reverse_expansion) {
      state_space = std::make_shared<ompl::base::DubinsStateSpace>(
        lattice_metadata.min_turning_radius);
      motion_model = MotionModel::DUBIN;
    } else {
      state_space = std::make_shared<ompl::base::ReedsSheppStateSpace>(
        lattice_metadata.min_turning_radius);
      motion_model = MotionModel::REEDS_SHEPP;
    }
  }

  // Populate the motion primitives at each heading angle
  float prev_start_angle = 0.0;
  std::vector<MotionPrimitive> primitives;
  nlohmann::json json_primitives = json["primitives"];
  for (unsigned int i = 0; i < json_primitives.size(); ++i) {
    MotionPrimitive new_primitive;
    fromJsonToMotionPrimitive(json_primitives[i], new_primitive);

    if (prev_start_angle != new_primitive.start_angle) {
      motion_primitives.push_back(primitives);
      primitives.clear();
      prev_start_angle = new_primitive.start_angle;
    }
    primitives.push_back(new_primitive);
  }
  motion_primitives.push_back(primitives);

  if (search_info.require_forward_steering_primitives) {
    validateForwardSteeringPrimitives();
  }

  // Populate useful precomputed values to be leveraged
  trig_values.reserve(lattice_metadata.number_of_headings);
  for (unsigned int i = 0; i < lattice_metadata.heading_angles.size(); ++i) {
    trig_values.emplace_back(
      cos(lattice_metadata.heading_angles[i]),
      sin(lattice_metadata.heading_angles[i]));
  }
}

MotionPrimitivePtrs LatticeMotionTable::getMotionPrimitives(
  const NodeLattice * node,
  unsigned int & direction_change_index)
{
  MotionPrimitives & prims_at_heading = motion_primitives[node->pose.theta];
  MotionPrimitivePtrs primitive_projection_list;
  for (unsigned int i = 0; i != prims_at_heading.size(); i++) {
    primitive_projection_list.push_back(&prims_at_heading[i]);
  }

  // direction change index
  direction_change_index = static_cast<unsigned int>(primitive_projection_list.size());

  if (allow_reverse_expansion) {
    // Find normalized heading bin of the reverse expansion
    double reserve_heading = node->pose.theta - (num_angle_quantization / 2);
    if (reserve_heading < 0) {
      reserve_heading += num_angle_quantization;
    }
    if (reserve_heading > num_angle_quantization) {
      reserve_heading -= num_angle_quantization;
    }

    MotionPrimitives & prims_at_reverse_heading = motion_primitives[reserve_heading];
    for (unsigned int i = 0; i != prims_at_reverse_heading.size(); i++) {
      primitive_projection_list.push_back(&prims_at_reverse_heading[i]);
    }
  }

  return primitive_projection_list;
}

LatticeMetadata LatticeMotionTable::getLatticeMetadata(const std::string & lattice_filepath)
{
  std::ifstream lattice_file(lattice_filepath);
  if (!lattice_file.is_open()) {
    throw std::runtime_error("Could not open lattice file!");
  }

  nlohmann::json j;
  lattice_file >> j;
  LatticeMetadata metadata;
  fromJsonToMetaData(j["lattice_metadata"], metadata);
  return metadata;
}

unsigned int LatticeMotionTable::getClosestAngularBin(const double & theta)
{
  float min_dist = std::numeric_limits<float>::max();
  unsigned int closest_idx = 0;
  float dist = 0.0;
  for (unsigned int i = 0; i != lattice_metadata.heading_angles.size(); i++) {
    dist = fabs(angles::shortest_angular_distance(theta, lattice_metadata.heading_angles[i]));
    if (dist < min_dist) {
      min_dist = dist;
      closest_idx = i;
    }
  }
  return closest_idx;
}

float & LatticeMotionTable::getAngleFromBin(const unsigned int & bin_idx)
{
  return lattice_metadata.heading_angles[bin_idx];
}

double LatticeMotionTable::getAngle(const double & theta)
{
  return getClosestAngularBin(theta);
}

NodeLattice::NodeLattice(const uint64_t index, NodeContext * ctx)
: parent(nullptr),
  pose(0.0f, 0.0f, 0.0f),
  _cell_cost(std::numeric_limits<float>::quiet_NaN()),
  _accumulated_cost(std::numeric_limits<float>::max()),
  _index(index),
  _was_visited(false),
  _motion_primitive(nullptr),
  _backwards(false),
  _is_node_valid(false),
  _ctx(ctx)
{
}

NodeLattice::~NodeLattice()
{
  parent = nullptr;
}

void NodeLattice::reset()
{
  parent = nullptr;
  _cell_cost = std::numeric_limits<float>::quiet_NaN();
  _accumulated_cost = std::numeric_limits<float>::max();
  _was_visited = false;
  pose.x = 0.0f;
  pose.y = 0.0f;
  pose.theta = 0.0f;
  _motion_primitive = nullptr;
  _backwards = false;
  _is_node_valid = false;
}

bool NodeLattice::isNodeValid(
  const bool & traverse_unknown,
  GridCollisionChecker * collision_checker,
  MotionPrimitive * motion_primitive,
  bool is_backwards)
{
  // Collision validity is an incoming-edge property for a continuous lattice.
  // Multiple geometrically different primitives (and sub-cell poses) may map
  // to the same discrete x/y/yaw index. Never reuse another edge's result.
  // The priority queue snapshots the selected edge metadata after A* accepts a
  // lower-cost parent, so re-evaluating here cannot corrupt a queued solution.

  // Check this candidate primitive's exact end pose.
  // Convert grid quantization of primitives to radians, then collision checker quantization
  const double bin_size = 2.0 * M_PI / collision_checker->getPrecomputedAngles().size();
  const double angle = std::fmod(
    _ctx->motion_table.getAngleFromBin(this->pose.theta),
    2.0 * M_PI) / bin_size;
  if (collision_checker->inCollision(
      this->pose.x, this->pose.y, angle /*bin in collision checker*/, traverse_unknown))
  {
    _is_node_valid = false;
    _cell_cost = collision_checker->getCost();
    return false;
  }

  // Store the highest cost across this candidate incoming primitive. A* uses
  // it only for this edge's traversal cost and snapshots it on queue insertion.
  float max_cell_cost = collision_checker->getCost();

  // Check the complete primitive sweep. Nav2's original implementation sampled
  // intermediary poses from XY separation. For a differential-drive lattice,
  // pure rotation has zero XY separation and a long rectangular footprint can
  // sweep a large arc between two heading bins. The ROS 1 adaptation therefore
  // delegates every primitive segment to GridCollisionChecker's bounded SE(2)
  // sweep, including the transition from the parent pose to the first primitive
  // sample.
  if (motion_primitive) {
    const float & grid_resolution = _ctx->motion_table.lattice_metadata.grid_resolution;
    const float initial_x =
      this->pose.x - (motion_primitive->poses.back()._x / grid_resolution);
    const float initial_y =
      this->pose.y - (motion_primitive->poses.back()._y / grid_resolution);
    double previous_yaw =
      _ctx->motion_table.getAngleFromBin(motion_primitive->start_angle);
    if (is_backwards) {
      previous_yaw = angles::normalize_angle(previous_yaw + M_PI);
    }
    float previous_x = initial_x;
    float previous_y = initial_y;

    for (const auto & primitive_pose : motion_primitive->poses) {
      const float primitive_x = initial_x + primitive_pose._x / grid_resolution;
      const float primitive_y = initial_y + primitive_pose._y / grid_resolution;
      double primitive_yaw = primitive_pose._theta;
      if (is_backwards) {
        primitive_yaw += M_PI;
      }
      primitive_yaw = angles::normalize_angle(primitive_yaw);

      float segment_cost = FREE_COST;
      if (collision_checker->inCollisionContinuous(
          previous_x, previous_y, previous_yaw,
          primitive_x, primitive_y, primitive_yaw,
          traverse_unknown, &segment_cost))
      {
        _is_node_valid = false;
        _cell_cost = std::max(max_cell_cost, segment_cost);
        return false;
      }
      max_cell_cost = std::max(max_cell_cost, segment_cost);
      previous_x = primitive_x;
      previous_y = primitive_y;
      previous_yaw = primitive_yaw;
    }
  }

  _cell_cost = max_cell_cost;
  _is_node_valid = true;
  return _is_node_valid;
}

float NodeLattice::getTraversalCost(const NodePtr & child)
{
  const float normalized_cost = child->getCost() / 252.0;
  if (std::isnan(normalized_cost)) {
    throw std::runtime_error(
            "Node attempted to get traversal "
            "cost without a known collision cost!");
  }

  // The current node has no incoming primitive only when it is a search seed.
  // The outgoing edge must still pay its complete soft-cost, rotation and
  // reverse penalties.  Returning only geometric length here made the first
  // edge an unpenalized shortcut through inflated space (and made a reverse
  // launch artificially cheap).
  const MotionPrimitive * prim = this->getMotionPrimitive();
  const MotionPrimitive * transition_prim = child->getMotionPrimitive();
  const float prim_length =
    transition_prim->trajectory_length / _ctx->motion_table.lattice_metadata.grid_resolution;

  // Pure rotation in place 1 angular bin in either direction
  if (transition_prim->trajectory_length < 1e-4) {
    return _ctx->motion_table.rotation_penalty *
           (1.0 + _ctx->motion_table.cost_penalty * normalized_cost);
  }

  float travel_cost = 0.0;
  float travel_cost_raw = 0.0;
  if (_ctx->motion_table.use_quadratic_cost_penalty) {
    travel_cost_raw = prim_length *
      (_ctx->motion_table.travel_distance_reward +
      _ctx->motion_table.cost_penalty * normalized_cost * normalized_cost);
  } else {
    travel_cost_raw = prim_length *
      (_ctx->motion_table.travel_distance_reward +
      _ctx->motion_table.cost_penalty * normalized_cost);
  }

  if (transition_prim->arc_length < 0.001) {
    // New motion is a straight motion, no additional costs to be applied
    travel_cost = travel_cost_raw;
  } else {
    if (prim == nullptr || prim->left_turn == transition_prim->left_turn) {
      // Turning motion but keeps in same general direction: encourages to commit to actions
      travel_cost = travel_cost_raw * _ctx->motion_table.non_straight_penalty;
    } else {
      // Turning motion and velocity directions: penalizes wiggling.
      travel_cost = travel_cost_raw *
        (_ctx->motion_table.non_straight_penalty + _ctx->motion_table.change_penalty);
    }
  }

  // If backwards flag is set, this primitive is moving in reverse
  if (child->isBackward()) {
    // reverse direction
    travel_cost *= _ctx->motion_table.reverse_penalty;
  }

  return travel_cost;
}

float NodeLattice::getHeuristicCost(
  const Coordinates & node_coords,
  const CoordinateVector & goals_coords)
{
  // get obstacle heuristic value
  // obstacle heuristic does not depend on goal heading
  const float obstacle_heuristic = _ctx->obstacle_heuristic->getObstacleHeuristic(
    node_coords, _ctx->motion_table.cost_penalty,
    _ctx->motion_table.use_quadratic_cost_penalty,
      _ctx->motion_table.downsample_obstacle_heuristic);
  float distance_heuristic = std::numeric_limits<float>::max();
  for (unsigned int i = 0; i < goals_coords.size(); i++) {
    distance_heuristic = std::min(
      distance_heuristic,
      _ctx->distance_heuristic->getDistanceHeuristic(node_coords, goals_coords[i],
        obstacle_heuristic, _ctx->motion_table));
  }
  return std::max(obstacle_heuristic, distance_heuristic);
}

void NodeLattice::initMotionModel(
  NodeContext * ctx,
  const MotionModel & motion_model,
  unsigned int & size_x,
  unsigned int & /*size_y*/,
  unsigned int & /*num_angle_quantization*/,
  SearchInfo & search_info)
{
  if (motion_model != MotionModel::STATE_LATTICE) {
    throw std::runtime_error(
            "Invalid motion model for Lattice node. Please select"
            " STATE_LATTICE and provide a valid lattice file.");
  }

  ctx->motion_table.initMotionModel(size_x, search_info);
}

void NodeLattice::getNeighbors(
  std::function<bool(const uint64_t &,
  nav2_smac_planner::NodeLattice * &)> & NeighborGetter,
  GridCollisionChecker * collision_checker,
  const bool & traverse_unknown,
  NodeVector & neighbors)
{
  uint64_t index = 0;
  bool backwards = false;
  NodePtr neighbor = nullptr;
  Coordinates initial_node_coords, motion_projection;
  unsigned int direction_change_index = 0;
  MotionPrimitivePtrs motion_primitives = _ctx->motion_table.getMotionPrimitives(
    this,
    direction_change_index);
  const float & grid_resolution = _ctx->motion_table.lattice_metadata.grid_resolution;

  for (unsigned int i = 0; i != motion_primitives.size(); i++) {
    const MotionPose & end_pose = motion_primitives[i]->poses.back();
    motion_projection.x = this->pose.x + (end_pose._x / grid_resolution);
    motion_projection.y = this->pose.y + (end_pose._y / grid_resolution);
    motion_projection.theta = motion_primitives[i]->end_angle /*this is the ending angular bin*/;

    // if i >= idx, then we're in a reversing primitive. In that situation,
    // the orientation of the robot is mirrored from what it would otherwise
    // appear to be from the motion primitives file. We want to take this into
    // account in case the robot base footprint is asymmetric.
    backwards = false;
    if (i >= direction_change_index) {
      backwards = true;
      float opposite_heading_theta =
        motion_projection.theta - (_ctx->motion_table.num_angle_quantization / 2);
      if (opposite_heading_theta < 0) {
        opposite_heading_theta += _ctx->motion_table.num_angle_quantization;
      }
      if (opposite_heading_theta > _ctx->motion_table.num_angle_quantization) {
        opposite_heading_theta -= _ctx->motion_table.num_angle_quantization;
      }
      motion_projection.theta = opposite_heading_theta;
    }

    index = NodeLattice::getIndex(
      static_cast<unsigned int>(motion_projection.x),
      static_cast<unsigned int>(motion_projection.y),
      static_cast<unsigned int>(motion_projection.theta),
      _ctx->motion_table.size_x, _ctx->motion_table.num_angle_quantization);

    if (NeighborGetter(index, neighbor) && !neighbor->wasVisited()) {
      // Cache the initial pose in case it was visited but valid
      // don't want to disrupt continuous coordinate expansion
      initial_node_coords = neighbor->pose;

      neighbor->setPose(
        Coordinates(
          motion_projection.x,
          motion_projection.y,
          motion_projection.theta));

      // Using a special isNodeValid API here, giving the motion primitive to use to
      // validity check the transition of the current node to the new node over
      if (neighbor->isNodeValid(
          traverse_unknown, collision_checker, motion_primitives[i], backwards))
      {
        neighbor->setMotionPrimitive(motion_primitives[i]);
        // Marking if this search was obtained in the reverse direction
        neighbor->backwards(backwards);
        neighbors.push_back(neighbor);
      } else {
        neighbor->setPose(initial_node_coords);
      }
    }
  }
}

bool NodeLattice::backtracePath(CoordinateVector & path)
{
  if (!this->parent) {
    return false;
  }

  NodePtr current_node = this;

  while (current_node->parent) {
    addNodeToPath(current_node, path);
    current_node = current_node->parent;
  }

  // add start to path
  addNodeToPath(current_node, path);

  return true;
}

void NodeLattice::addNodeToPath(
  NodeLattice::NodePtr current_node,
  NodeLattice::CoordinateVector & path)
{
  Coordinates initial_pose, prim_pose;
  const MotionPrimitive * prim = current_node->getMotionPrimitive();
  const float & grid_resolution = _ctx->motion_table.lattice_metadata.grid_resolution;
  // if motion primitive is valid, then was searched (rather than analytically expanded),
  // include dense path of subpoints making up the primitive at grid resolution
  if (prim) {
    initial_pose.x = current_node->pose.x - (prim->poses.back()._x / grid_resolution);
    initial_pose.y = current_node->pose.y - (prim->poses.back()._y / grid_resolution);
    initial_pose.theta = _ctx->motion_table.getAngleFromBin(prim->start_angle);

    for (auto it = prim->poses.crbegin(); it != prim->poses.crend(); ++it) {
      // Convert primitive pose into grid space if it should be checked
      prim_pose.x = initial_pose.x + (it->_x / grid_resolution);
      prim_pose.y = initial_pose.y + (it->_y / grid_resolution);
      // If reversing, invert the angle because the robot is backing into the primitive
      // not driving forward with it
      if (current_node->isBackward()) {
        prim_pose.theta = std::fmod(it->_theta + M_PI, 2.0 * M_PI);
      } else {
        prim_pose.theta = it->_theta;
      }
      path.push_back(prim_pose);
    }
  } else {
    // For analytic expansion nodes where there is no valid motion primitive
    path.push_back(current_node->pose);
    path.back().theta = _ctx->motion_table.getAngleFromBin(path.back().theta);
  }
}

}  // namespace nav2_smac_planner

// Copyright (c) 2020, Samsung Research America
// Copyright (c) 2020, Applied Electric Vehicles Pty Ltd
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

#ifndef NAV2_SMAC_PLANNER__A_STAR_IMPL_HPP_
#define NAV2_SMAC_PLANNER__A_STAR_IMPL_HPP_

#include <algorithm>
#include <chrono>
#include <cmath>
#include <limits>
#include <memory>
#include <stdexcept>
#include <thread>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "nav2_smac_planner/a_star.hpp"

namespace nav2_smac_planner
{
using namespace std::chrono;  // NOLINT

template<typename NodeT>
AStarAlgorithm<NodeT>::AStarAlgorithm(
  const MotionModel & motion_model,
  const SearchInfo & search_info)
: _traverse_unknown(true),
  _is_initialized(false),
  _max_iterations(0),
  _terminal_checking_interval(5000),
  _max_planning_time(0),
  _x_size(0),
  _y_size(0),
  _search_info(search_info),
  _start(nullptr),
  _goal_manager(GoalManagerT()),
  _motion_model(motion_model)
{
  _graph.reserve(100000);
}

template<typename NodeT>
AStarAlgorithm<NodeT>::~AStarAlgorithm()
{
}

template<typename NodeT>
void AStarAlgorithm<NodeT>::initialize(
  const bool & allow_unknown,
  int & max_iterations,
  const int & max_on_approach_iterations,
  const int & terminal_checking_interval,
  const double & max_planning_time,
  const float & lookup_table_size,
  const unsigned int & dim_3_size)
{
  _traverse_unknown = allow_unknown;
  _max_iterations = max_iterations;
  _max_on_approach_iterations = max_on_approach_iterations;
  _terminal_checking_interval = terminal_checking_interval;
  _max_planning_time = max_planning_time;

  if constexpr (std::is_base_of_v<Node2D, NodeT>) {
    // Node2D-specific initialization: no distance heuristic precomputation
    _shared_ctx = std::make_shared<NodeContext>();
    if (dim_3_size != 1) {
      throw std::runtime_error("Node type Node2D cannot be given non-1 dim 3 quantization.");
    }
  } else {
    // SE2 node initialization: precompute distance heuristic
    if (!_is_initialized) {
      _shared_ctx = std::make_shared<NodeContext>();
      _shared_ctx->distance_heuristic->precomputeDistanceHeuristic(
        lookup_table_size, _motion_model,
        dim_3_size,
        _search_info, _shared_ctx->motion_table);
    }
  }

  _is_initialized = true;
  _dim3_size = dim_3_size;
  _expander = std::make_unique<AnalyticExpansion<NodeT>>(
    _motion_model, _search_info, _traverse_unknown, _dim3_size);
  _expander->setCenterDomain(_center_domain);
}

template<typename NodeT>
void AStarAlgorithm<NodeT>::setCollisionChecker(GridCollisionChecker * collision_checker)
{
  _collision_checker = collision_checker;
  _costmap = collision_checker->getCostmap();
  unsigned int x_size = _costmap->getSizeInCellsX();
  unsigned int y_size = _costmap->getSizeInCellsY();

  clearGraph();

  if (getSizeX() != x_size || getSizeY() != y_size) {
    _x_size = x_size;
    _y_size = y_size;
  }

  // Always refresh the motion model so dynamic penalty parameters take effect immediately
  NodeT::initMotionModel(
    _shared_ctx.get(), _motion_model, _x_size, _y_size, _dim3_size,
    _search_info);

  // Always set context pointers to ensure newly allocated objects get their contexts restored
  _goal_manager.setContext(_shared_ctx.get());
  _expander->setContext(_shared_ctx.get());
  _expander->setCollisionChecker(_collision_checker);
}

template<typename NodeT>
void AStarAlgorithm<NodeT>::setCenterDomain(CenterDomainCallback center_domain)
{
  _center_domain = center_domain;
  if (_expander) {
    _expander->setCenterDomain(_center_domain);
  }
}

template<typename NodeT>
void AStarAlgorithm<NodeT>::clearCenterDomain()
{
  setCenterDomain(CenterDomainCallback());
}

template<typename NodeT>
void AStarAlgorithm<NodeT>::setAdditionalHeuristic(
  AdditionalHeuristicCallback additional_heuristic)
{
  _additional_heuristic = additional_heuristic;
}

template<typename NodeT>
void AStarAlgorithm<NodeT>::clearAdditionalHeuristic()
{
  setAdditionalHeuristic(AdditionalHeuristicCallback());
}

template<typename NodeT>
void AStarAlgorithm<NodeT>::setTransitionValidator(
  TransitionValidator validator)
{
  _transition_validator = std::move(validator);
}

template<typename NodeT>
void AStarAlgorithm<NodeT>::clearTransitionValidator()
{
  setTransitionValidator(TransitionValidator());
}

template<typename NodeT>
void AStarAlgorithm<NodeT>::setGoalTransitionValidator(
  GoalTransitionValidator validator)
{
  _goal_transition_validator = std::move(validator);
}

template<typename NodeT>
void AStarAlgorithm<NodeT>::clearGoalTransitionValidator()
{
  setGoalTransitionValidator(GoalTransitionValidator());
}

template<typename NodeT>
typename AStarAlgorithm<NodeT>::NodePtr AStarAlgorithm<NodeT>::addToGraph(
  const uint64_t & index)
{
  auto iter = _graph.find(index);
  if (iter != _graph.end()) {
    return &(iter->second);
  }

  return &(_graph.emplace(index, NodeT(index, _shared_ctx.get())).first->second);
}

template<typename NodeT>
void AStarAlgorithm<NodeT>::setStart(
  const float & mx,
  const float & my,
  const unsigned int & dim_3,
  const float & initial_cost)
{
  _starts.clear();
  if constexpr (std::is_base_of_v<Node2D, NodeT>) {
    // Node2D-specific: different getIndex signature, no pose setting
    if (dim_3 != 0) {
      throw std::runtime_error("Node type Node2D cannot be given non-zero starting dim 3.");
    }
    _start = addToGraph(
      Node2D::getIndex(
        static_cast<unsigned int>(mx),
        static_cast<unsigned int>(my),
        getSizeX()));
  } else {
    // SE2 node: use full index and set pose
    _start = addToGraph(
      getIndex(
        static_cast<unsigned int>(mx),
        static_cast<unsigned int>(my),
        dim_3));
    _start->setPose(Coordinates(mx, my, dim_3));
  }
  _starts.emplace_back(_start, std::max(0.0f, initial_cost));
}

template<typename NodeT>
void AStarAlgorithm<NodeT>::addStart(
  const float & mx,
  const float & my,
  const unsigned int & dim_3,
  const float & initial_cost)
{
  if constexpr (std::is_base_of_v<Node2D, NodeT>) {
    throw std::runtime_error("Multiple heading seeds are only supported for SE2 nodes.");
  } else {
    NodePtr seed = addToGraph(
      getIndex(
        static_cast<unsigned int>(mx),
        static_cast<unsigned int>(my),
        dim_3));
    seed->setPose(Coordinates(mx, my, dim_3));
    const auto duplicate = std::find_if(
      _starts.begin(), _starts.end(),
      [seed](const auto & entry) {return entry.first == seed;});
    if (duplicate == _starts.end()) {
      _starts.emplace_back(seed, std::max(0.0f, initial_cost));
    }
  }
}

template<typename NodeT>
void AStarAlgorithm<NodeT>::populateExpansionsLog(
  const NodePtr & node,
  std::vector<std::tuple<float, float, float>> * expansions_log)
{
  if constexpr (std::is_base_of_v<Node2D, NodeT>) {
    // Node2D: no theta
    Node2D::Coordinates coords = node->getCoords(node->getIndex());
    expansions_log->emplace_back(
      _costmap->getOriginX() + ((coords.x + 0.5) * _costmap->getResolution()),
      _costmap->getOriginY() + ((coords.y + 0.5) * _costmap->getResolution()),
      0.0);
  } else {
    // SE2 node: include theta
    typename NodeT::Coordinates coords = node->pose;
    expansions_log->emplace_back(
      _costmap->getOriginX() + ((coords.x + 0.5) * _costmap->getResolution()),
      _costmap->getOriginY() + ((coords.y + 0.5) * _costmap->getResolution()),
      _shared_ctx->motion_table.getAngleFromBin(coords.theta));
  }
}

template<typename NodeT>
void AStarAlgorithm<NodeT>::setGoal(
  const float & mx,
  const float & my,
  const unsigned int & dim_3,
  const GoalHeadingMode & goal_heading_mode,
  const int & coarse_search_resolution)
{
  if constexpr (std::is_base_of_v<Node2D, NodeT>) {
    // Node2D-specific: simplified goal setting, no heading modes
    if (dim_3 != 0) {
      throw std::runtime_error("Node type Node2D cannot be given non-zero goal dim 3.");
    }
    _goal_manager.clear();
    auto goal = addToGraph(
      Node2D::getIndex(
        static_cast<unsigned int>(mx),
        static_cast<unsigned int>(my),
        getSizeX()));

    goal->setPose(Node2D::Coordinates(mx, my));
    _goal_manager.addGoal(goal);

    _coarse_search_resolution = 1;
  } else {
    // SE2 node: full goal handling with heading modes
    // Default to minimal resolution unless overridden for ALL_DIRECTION
    _coarse_search_resolution = 1;

    _goal_manager.clear();
    Coordinates ref_goal_coord(mx, my, static_cast<float>(dim_3));

    if (!_search_info.cache_obstacle_heuristic ||
      _goal_manager.hasGoalChanged(ref_goal_coord))
    {
      if (!_start) {
        throw std::runtime_error("Start must be set before goal.");
      }

      _shared_ctx->obstacle_heuristic->resetObstacleHeuristic(
        _collision_checker->getCostmapROS(), _start->pose.x, _start->pose.y, mx, my,
        _shared_ctx->motion_table.downsample_obstacle_heuristic);
    }

    _goal_manager.setRefGoalCoordinates(ref_goal_coord);

    unsigned int num_bins = _shared_ctx->motion_table.num_angle_quantization;
    // set goal based on heading mode
    switch (goal_heading_mode) {
      case GoalHeadingMode::DEFAULT:
        {
          // add a single goal node with single heading
          auto goal = addToGraph(
            getIndex(
              static_cast<unsigned int>(mx),
              static_cast<unsigned int>(my),
              dim_3));
          goal->setPose(typename NodeT::Coordinates(mx, my, static_cast<float>(dim_3)));
          _goal_manager.addGoal(goal);
          break;
        }

      case GoalHeadingMode::BIDIRECTIONAL:
        {
          // Add two goals, one for each direction
          // add goal in original direction
          auto goal = addToGraph(
            getIndex(
              static_cast<unsigned int>(mx),
              static_cast<unsigned int>(my),
              dim_3));
          goal->setPose(typename NodeT::Coordinates(mx, my, static_cast<float>(dim_3)));
          _goal_manager.addGoal(goal);

          // Add goal node in opposite (180°) direction
          unsigned int opposite_heading = (dim_3 + (num_bins / 2)) % num_bins;
          auto opposite_goal = addToGraph(
            getIndex(
              static_cast<unsigned int>(mx),
              static_cast<unsigned int>(my),
              opposite_heading));
          opposite_goal->setPose(
            typename NodeT::Coordinates(mx, my, static_cast<float>(opposite_heading)));
          _goal_manager.addGoal(opposite_goal);
          break;
        }

      case GoalHeadingMode::ALL_DIRECTION:
        {
          // Set the coarse search resolution only for all direction
          _coarse_search_resolution = coarse_search_resolution;

          // Add goal nodes for all headings
          for (unsigned int i = 0; i < num_bins; ++i) {
            auto goal = addToGraph(
              getIndex(
                static_cast<unsigned int>(mx),
                static_cast<unsigned int>(my),
                i));
            goal->setPose(typename NodeT::Coordinates(mx, my, static_cast<float>(i)));
            _goal_manager.addGoal(goal);
          }
          break;
        }
      case GoalHeadingMode::UNKNOWN:
        throw std::runtime_error("Goal heading is UNKNOWN.");
    }
  }
}

template<typename NodeT>
bool AStarAlgorithm<NodeT>::areInputsValid()
{
  // Check if graph was filled in
  if (_graph.empty()) {
    throw std::runtime_error("Failed to compute path, no costmap given.");
  }

  // Check if points were filled in
  if (!_start || _goal_manager.goalsIsEmpty()) {
    throw std::runtime_error("Failed to compute path, no valid start or goal given.");
  }

  // remove invalid goals
  _goal_manager.removeInvalidGoals(getToleranceHeuristic(), _collision_checker, _traverse_unknown);

  // Check if ending point is valid
  if (_goal_manager.getGoalsSet().empty()) {
    throw nav2_core::GoalOccupied("Goal was in lethal cost");
  }

  // Note: We do not check the if the start is valid because it is cleared
  return true;
}

template<typename NodeT>
bool AStarAlgorithm<NodeT>::getClosestPathWithinTolerance(CoordinateVector & path)
{
  if (_best_heuristic_node.first < getToleranceHeuristic()) {
    _graph.at(_best_heuristic_node.second).backtracePath(path);
    return true;
  }

  return false;
}

template<typename NodeT>
bool AStarAlgorithm<NodeT>::createPath(
  CoordinateVector & path, int & iterations,
  const float & tolerance,
  std::function<bool()> cancel_checker,
  std::vector<std::tuple<float, float, float>> * expansions_log)
{
  const SearchResult result = createPathDetailed(
    path, iterations, tolerance, cancel_checker, expansions_log);
  if (result.termination == SearchTermination::CANCELED) {
    // Preserve the established createPath() cancellation contract for all
    // existing adapters while exposing CANCELED to new callers above it.
    throw nav2_core::PlannerCancelled("Planner was cancelled");
  }
  return result.path_found;
}

template<typename NodeT>
SearchResult AStarAlgorithm<NodeT>::createPathDetailed(
  CoordinateVector & path, int & iterations,
  const float & tolerance,
  std::function<bool()> cancel_checker,
  std::vector<std::tuple<float, float, float>> * expansions_log)
{
  steady_clock::time_point start_time = steady_clock::now();
  _tolerance = tolerance;
  _best_heuristic_node = {std::numeric_limits<float>::max(), 0};
  clearQueue();

  if (!areInputsValid()) {
    return {false, SearchTermination::OPEN_EXHAUSTED};
  }

  NodeVector coarse_check_goals, fine_check_goals;
  _goal_manager.prepareGoalsForAnalyticExpansion(
    coarse_check_goals, fine_check_goals,
    _coarse_search_resolution);

  // 0) Add every collision-checked heading seed to the open set. The primary
  // start remains available for the obstacle heuristic, while the final
  // backtrace naturally terminates at whichever safe quantization won.
  for (auto & seed : _starts) {
    seed.first->parent = nullptr;
    seed.first->setAccumulatedCost(seed.second);
    addNode(seed.second, seed.first);
  }

  // Optimization: preallocate all variables
  NodePtr current_node = nullptr;
  NodePtr neighbor = nullptr;
  NodePtr expansion_result = nullptr;
  float g_cost = 0.0;
  NodeVector neighbors;
  int approach_iterations = 0;
  NeighborIterator neighbor_iterator;
  int analytic_iterations = 0;
  int closest_distance = std::numeric_limits<int>::max();

  // Given an index, return a node ptr reference if its collision-free and valid
  const uint64_t max_index = static_cast<uint64_t>(getSizeX()) *
    static_cast<uint64_t>(getSizeY()) *
    static_cast<uint64_t>(getSizeDim3());
  NodeGetter neighborGetter =
    [&, this](const uint64_t & index, NodePtr & neighbor_rtn) -> bool
    {
      if (index >= max_index) {
        return false;
      }

      if (_center_domain) {
        const Coordinates coordinates =
          NodeT::getCoords(index, getSizeX(), getSizeDim3());
        if (!_center_domain(coordinates.x, coordinates.y)) {
          return false;
        }
      }

      neighbor_rtn = addToGraph(index);
      return true;
    };

  while (iterations < getMaxIterations() && !_queue.empty()) {
    // Cancellation is externally visible and must not be delayed by a large
    // user-configured terminal-check interval. Keep the comparatively more
    // expensive wall-clock timeout query at that interval.
    if (cancel_checker()) {
      return {false, SearchTermination::CANCELED};
    }
    if (iterations % _terminal_checking_interval == 0) {
      std::chrono::duration<double> planning_duration =
        std::chrono::duration_cast<std::chrono::duration<double>>(steady_clock::now() - start_time);
      if (static_cast<double>(planning_duration.count()) >= _max_planning_time) {
        // In case of timeout, return the path that is closest, if within tolerance.
        return {
          getClosestPathWithinTolerance(path),
          SearchTermination::TIMEOUT};
      }
    }

    // 1) Pick Nbest from O s.t. min(f(Nbest)), remove from queue
    current_node = getNextNode();

    // Save current node coordinates for debug
    if (expansions_log) {
      populateExpansionsLog(current_node, expansions_log);
    }

    // We allow for nodes to be queued multiple times in case
    // shorter paths result in it, but we can visit only once
    // Also a chance to perform last-checks necessary.
    if (onVisitationCheckNode(current_node)) {
      continue;
    }

    iterations++;

    // 2) Mark Nbest as visited
    current_node->visited();

    // 2.1) Use an analytic expansion (if available) to generate a path
    expansion_result = nullptr;
    expansion_result = _expander->tryAnalyticExpansion(
      current_node, coarse_check_goals, fine_check_goals,
      _goal_manager.getGoalsCoordinates(), neighborGetter, analytic_iterations, closest_distance);
    if (expansion_result != nullptr) {
      current_node = expansion_result;
    }

    // 3) Check if we're at the goal, backtrace if required
    if (_goal_manager.isGoal(current_node)) {
      const bool path_found = current_node->backtracePath(path);
      return {
        path_found,
        path_found ? SearchTermination::SUCCESS : SearchTermination::OPEN_EXHAUSTED};
    } else if (_best_heuristic_node.first < getToleranceHeuristic()) {
      // Optimization: Let us find when in tolerance and refine within reason
      approach_iterations++;
      if (approach_iterations >= getOnApproachMaxIterations()) {
        const bool path_found =
          _graph.at(_best_heuristic_node.second).backtracePath(path);
        return {
          path_found,
          path_found ? SearchTermination::SUCCESS : SearchTermination::OPEN_EXHAUSTED};
      }
    }

    // 4) Expand neighbors of Nbest not visited
    neighbors.clear();
    current_node->getNeighbors(neighborGetter, _collision_checker, _traverse_unknown, neighbors);

    for (neighbor_iterator = neighbors.begin();
      neighbor_iterator != neighbors.end(); ++neighbor_iterator)
    {
      neighbor = *neighbor_iterator;

      // Apply an optional scoped motion-set restriction before the node is
      // queued or visited. A rejected edge must not claim the destination
      // state, because another admissible primitive may still reach it.
      if (_transition_validator &&
        !_transition_validator(current_node->pose, neighbor->pose))
      {
        continue;
      }

      // A goal state can have several geometrically different incoming
      // primitives. Apply an optional policy before queueing/visiting it so a
      // rejected terminal primitive cannot hide a later acceptable arrival to
      // the same discrete SE(2) state.
      if (_goal_transition_validator && _goal_manager.isGoal(neighbor) &&
        !_goal_transition_validator(current_node->pose, neighbor->pose))
      {
        continue;
      }

      // 4.1) Compute the cost to go to this node
      g_cost = current_node->getAccumulatedCost() + current_node->getTraversalCost(neighbor);

      // 4.2) If this is a lower cost than prior, we set this as the new cost and new approach
      if (g_cost < neighbor->getAccumulatedCost()) {
        neighbor->setAccumulatedCost(g_cost);
        neighbor->parent = current_node;

        // 4.3) Add to queue with heuristic cost
        addNode(g_cost + getHeuristicCost(neighbor), neighbor);
      }
    }
  }

  // If we run out of search options, return the path that is closest, if within tolerance.
  const SearchTermination termination = _queue.empty() ?
    SearchTermination::OPEN_EXHAUSTED : SearchTermination::ITERATION_LIMIT;
  return {getClosestPathWithinTolerance(path), termination};
}

template<typename NodeT>
typename AStarAlgorithm<NodeT>::NodePtr & AStarAlgorithm<NodeT>::getStart()
{
  return _start;
}

template<typename NodeT>
typename AStarAlgorithm<NodeT>::NodePtr AStarAlgorithm<NodeT>::getNextNode()
{
  NodeBasic<NodeT> node = _queue.top().second;
  _queue.pop();
  node.processSearchNode();
  return node.graph_node_ptr;
}

template<typename NodeT>
void AStarAlgorithm<NodeT>::addNode(const float & cost, NodePtr & node)
{
  NodeBasic<NodeT> queued_node(node->getIndex());
  queued_node.populateSearchNode(node);
  _queue.emplace(cost, queued_node);
}

template<typename NodeT>
float AStarAlgorithm<NodeT>::getHeuristicCost(const NodePtr & node)
{
  const Coordinates node_coords =
    NodeT::getCoords(node->getIndex(), getSizeX(), getSizeDim3());
  const float anchor =
    node->getHeuristicCost(node_coords, _goal_manager.getGoalsCoordinates());
  if (anchor < _best_heuristic_node.first) {
    // Tolerance semantics must remain tied to the official admissible anchor,
    // never to an optional route-ordering preference.
    _best_heuristic_node = {anchor, node->getIndex()};
  }

  if (_additional_heuristic) {
    return std::max(anchor, _additional_heuristic(node_coords.x, node_coords.y));
  }
  return anchor;
}

template<typename NodeT>
bool AStarAlgorithm<NodeT>::onVisitationCheckNode(const NodePtr & current_node)
{
  return current_node->wasVisited();
}

template<typename NodeT>
void AStarAlgorithm<NodeT>::clearQueue()
{
  NodeQueue q;
  std::swap(_queue, q);
}

template<typename NodeT>
void AStarAlgorithm<NodeT>::clearGraph()
{
  Graph g;
  std::swap(_graph, g);
  _graph.reserve(100000);
  _start = nullptr;
  _starts.clear();
}

template<typename NodeT>
uint64_t AStarAlgorithm<NodeT>::getIndex(
  const unsigned int & x, const unsigned int & y,
  const unsigned int & dim_3)
{
  if constexpr (std::is_base_of_v<Node2D, NodeT>) {
    return Node2D::getIndex(x, y, dim_3);
  } else {
    return NodeT::getIndex(
      x, y, dim_3, _shared_ctx->motion_table.size_x,
      _shared_ctx->motion_table.num_angle_quantization);
  }
}

template<typename NodeT>
int & AStarAlgorithm<NodeT>::getMaxIterations()
{
  return _max_iterations;
}

template<typename NodeT>
int & AStarAlgorithm<NodeT>::getOnApproachMaxIterations()
{
  return _max_on_approach_iterations;
}

template<typename NodeT>
float & AStarAlgorithm<NodeT>::getToleranceHeuristic()
{
  return _tolerance;
}

template<typename NodeT>
unsigned int & AStarAlgorithm<NodeT>::getSizeX()
{
  return _x_size;
}

template<typename NodeT>
unsigned int & AStarAlgorithm<NodeT>::getSizeY()
{
  return _y_size;
}

template<typename NodeT>
unsigned int & AStarAlgorithm<NodeT>::getSizeDim3()
{
  return _dim3_size;
}

template<typename NodeT>
unsigned int AStarAlgorithm<NodeT>::getCoarseSearchResolution()
{
  return _coarse_search_resolution;
}

template<typename NodeT>
typename AStarAlgorithm<NodeT>::GoalManagerT AStarAlgorithm<NodeT>::getGoalManager()
{
  return _goal_manager;
}

template<typename NodeT>
typename AStarAlgorithm<NodeT>::NodeContext * AStarAlgorithm<NodeT>::getContext()
{
  return _shared_ctx.get();
}

}  // namespace nav2_smac_planner

#endif  // NAV2_SMAC_PLANNER__A_STAR_IMPL_HPP_

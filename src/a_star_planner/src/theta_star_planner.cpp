/**
 * *********************************************************
 *
 * @file: theta_star.cpp
 * @brief: Contains the Theta* planner class
 * @author: Wu Maojia, Yang Haodong
 * @date: 2023-10-01
 * @version: 1.3
 *
 * Copyright (c) 2024, Wu Maojia, Yang Haodong.
 * All rights reserved.
 *
 * --------------------------------------------------------
 *
 * ********************************************************
 */
#include <algorithm>
#include <queue>
#include <unordered_map>

#include <costmap_2d/cost_values.h>

#include "a_star_planner/collision_checker.h"
#include "a_star_planner/theta_star_planner.h"

namespace rmp
{
namespace path_planner
{
namespace
{
using CollisionChecker = rmp::common::geometry::CollisionChecker;
}

/**
 * @brief Construct a new ThetaStar object
 * @param costmap   the environment for path planning
 * @param obstacle_factor obstacle factor(greater means obstacles)
 */
ThetaStarPathPlanner::ThetaStarPathPlanner(costmap_2d::Costmap2DROS* costmap_ros, double obstacle_factor)
  : PathPlanner(costmap_ros, obstacle_factor){};

void ThetaStarPathPlanner::setCancelChecker(std::function<bool()> cancel_checker)
{
  cancel_checker_ = std::move(cancel_checker);
}

void ThetaStarPathPlanner::configurePlanning(bool record_expansion, int cancel_check_interval, double max_planning_time_s)
{
  record_expansion_ = record_expansion;
  cancel_check_interval_ = std::max(1, cancel_check_interval);
  max_planning_time_s_ = std::max(0.0, max_planning_time_s);
}

ThetaStarPathPlanner::ExitCode ThetaStarPathPlanner::getLastExitCode() const
{
  return last_exit_code_;
}

std::size_t ThetaStarPathPlanner::getLastExpandedNodes() const
{
  return last_expanded_nodes_;
}

/**
 * @brief Theta* implementation
 * @param start         start node
 * @param goal          goal node
 * @param path          optimal path consists of Node
 * @param expand        containing the node been search during the process
 * @return  true if path found, else false
 */
bool ThetaStarPathPlanner::plan(const Point3d& start, const Point3d& goal, Points3d& path, Points3d& expand)
{
  last_exit_code_ = ExitCode::NO_PATH;
  last_expanded_nodes_ = 0;

  double m_start_x, m_start_y, m_goal_x, m_goal_y;
  if ((!validityCheck(start.x(), start.y(), m_start_x, m_start_y)) ||
      (!validityCheck(goal.x(), goal.y(), m_goal_x, m_goal_y)))
  {
    return false;
  }

  // initialize
  Node start_node(m_start_x, m_start_y);
  Node goal_node(m_goal_x, m_goal_y);
  start_node.set_id(grid2Index(start_node.x(), start_node.y()));
  goal_node.set_id(grid2Index(goal_node.x(), goal_node.y()));
  path.clear();
  expand.clear();
  if (record_expansion_)
  {
    expand.reserve(1024);
  }

  // open list and closed list
  std::priority_queue<Node, std::vector<Node>, Node::compare_cost> open_list;
  std::unordered_map<int, Node> closed_list;
  std::unordered_map<int, double> best_g;

  const auto straight_line_cells = std::hypot(goal_node.x() - start_node.x(), goal_node.y() - start_node.y());
  const std::size_t reserve_hint = static_cast<std::size_t>(
      std::max(2048.0, std::min(static_cast<double>(map_size_), straight_line_cells * 32.0)));
  closed_list.reserve(reserve_hint);
  best_g.reserve(reserve_hint);

  open_list.push(start_node);
  best_g.emplace(start_node.id(), start_node.g());

  const ros::WallTime wall_start = ros::WallTime::now();
  std::size_t loop_counter = 0;

  // main process
  while (!open_list.empty())
  {
    if ((loop_counter++ % static_cast<std::size_t>(cancel_check_interval_)) == 0U)
    {
      if (cancel_checker_ && cancel_checker_())
      {
        last_exit_code_ = ExitCode::CANCELED;
        return false;
      }
      if (max_planning_time_s_ > 0.0 &&
          (ros::WallTime::now() - wall_start).toSec() >= max_planning_time_s_)
      {
        last_exit_code_ = ExitCode::TIMED_OUT;
        return false;
      }
    }

    // pop current node from open list
    auto current = open_list.top();
    open_list.pop();

    // current node does not exist in closed list
    if (closed_list.find(current.id()) != closed_list.end())
      continue;

    const auto best_it = best_g.find(current.id());
    if (best_it != best_g.end() && current.g() > best_it->second)
      continue;

    closed_list.insert(std::make_pair(current.id(), current));
    ++last_expanded_nodes_;
    if (record_expansion_)
    {
      expand.emplace_back(current.x(), current.y());
    }

    // goal found
    if (current == goal_node)
    {
      const auto& backtrace = _convertClosedListToPath<Node>(closed_list, start_node, goal_node);
      for (auto iter = backtrace.rbegin(); iter != backtrace.rend(); ++iter)
      {
        // convert to world frame
        double wx, wy;
        costmap_->mapToWorld(iter->x(), iter->y(), wx, wy);
        path.emplace_back(wx, wy);
      }
      last_exit_code_ = ExitCode::SUCCESS;
      return true;
    }

    // explore neighbor of current node
    for (const auto& m : motions)
    {
      // explore a new node
      // path 1
      auto node_new = current + m;  // add the x_, y_, g_
      node_new.set_g(current.g() + m.g());
      node_new.set_h(std::hypot(node_new.x() - goal_node.x(), node_new.y() - goal_node.y()));
      node_new.set_id(grid2Index(node_new.x(), node_new.y()));
      node_new.set_pid(current.id());

      // current node do not exist in closed list
      if (closed_list.find(node_new.id()) != closed_list.end())
        continue;

      // next node hit the boundary or obstacle
      if ((node_new.id() < 0) || (node_new.id() >= map_size_) ||
          (costmap_->getCharMap()[node_new.id()] >= costmap_2d::LETHAL_OBSTACLE * obstacle_factor_ &&
           costmap_->getCharMap()[node_new.id()] >= costmap_->getCharMap()[current.id()]))
        continue;

      // get the coordinate of parent node
      Node parent;
      parent.set_id(current.pid());
      int tmp_x, tmp_y;
      index2Grid(parent.id(), tmp_x, tmp_y);
      parent.set_x(tmp_x);
      parent.set_y(tmp_y);

      // update g value
      auto find_parent = closed_list.find(parent.id());
      if (find_parent != closed_list.end())
      {
        parent = find_parent->second;
        _updateVertex(parent, node_new);
      }

      const auto best_child = best_g.find(node_new.id());
      if (best_child != best_g.end() && node_new.g() >= best_child->second)
        continue;

      best_g[node_new.id()] = node_new.g();
      open_list.push(node_new);
    }
  }

  last_exit_code_ = ExitCode::NO_PATH;
  return false;
}

/**
 * @brief update the g value of child node
 * @param parent
 * @param child
 */
void ThetaStarPathPlanner::_updateVertex(const Node& parent, Node& child)
{
  auto isCollision = [&](const Node& node1, const Node& node2) {
    return CollisionChecker::BresenhamCollisionDetection(node1, node2, [&](const Node& node) {
      return costmap_->getCharMap()[grid2Index(node.x(), node.y())] >= costmap_2d::LETHAL_OBSTACLE * obstacle_factor_;
    });
  };

  if (!isCollision(parent, child))
  {
    // path 2
    const double dist = std::hypot(parent.x() - child.x(), parent.y() - child.y());
    if (parent.g() + dist < child.g())
    {
      child.set_g(parent.g() + dist);
      child.set_pid(parent.id());
    }
  }
}
}  // namespace path_planner
}  // namespace rmp

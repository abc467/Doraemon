/**
 * *********************************************************
 *
 * @file: theta_star_planner.h
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
#ifndef RMP_PATH_PLANNER_GRAPH_PLANNER_THETA_STAR_H_
#define RMP_PATH_PLANNER_GRAPH_PLANNER_THETA_STAR_H_

#include <functional>
#include <vector>

#include "a_star_planner/node.h"
#include "a_star_planner/point.h"
#include "a_star_planner/path_planner.h"


namespace rmp
{
namespace path_planner
{
/**
 * @brief Class for objects that plan using the Theta* algorithm
 */
class ThetaStarPathPlanner : public PathPlanner
{
private:
  using Node = rmp::common::structure::Node<int>;

public:
  enum class ExitCode
  {
    SUCCESS,
    NO_PATH,
    CANCELED,
    TIMED_OUT,
  };

  /**
   * @brief Construct a new ThetaStar object
   * @param costmap   the environment for path planning
   * @param obstacle_factor obstacle factor(greater means obstacles)
   */
  ThetaStarPathPlanner(costmap_2d::Costmap2DROS* costmap_ros, double obstacle_factor = 1.0);

  /**
   * @brief Theta* implementation
   * @param start         start node
   * @param goal          goal node
   * @param path          optimal path consists of Node
   * @param expand        containing the node been search during the process
   * @return  true if path found, else false
   */
  bool plan(const Point3d& start, const Point3d& goal, Points3d& path, Points3d& expand) override;

  void setCancelChecker(std::function<bool()> cancel_checker);

  void configurePlanning(bool record_expansion, int cancel_check_interval, double max_planning_time_s);

  ExitCode getLastExitCode() const;

  std::size_t getLastExpandedNodes() const;

protected:
  /**
   * @brief update the g value of child node
   * @param parent
   * @param child
   */
  void _updateVertex(const Node& parent, Node& child);

private:
  const std::vector<Node> motions = {
    { 0, 1, 1.0 },          { 1, 0, 1.0 },           { 0, -1, 1.0 },          { -1, 0, 1.0 },
    { 1, 1, std::sqrt(2) }, { 1, -1, std::sqrt(2) }, { -1, 1, std::sqrt(2) }, { -1, -1, std::sqrt(2) },
  };

  std::function<bool()> cancel_checker_;
  bool record_expansion_{false};
  int cancel_check_interval_{1024};
  double max_planning_time_s_{0.0};
  ExitCode last_exit_code_{ExitCode::NO_PATH};
  std::size_t last_expanded_nodes_{0};
};
}  // namespace path_planner
}  // namespace rmp
#endif

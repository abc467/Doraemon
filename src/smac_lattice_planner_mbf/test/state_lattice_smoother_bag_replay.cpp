// Copyright 2026 Clean Robot Navigation Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include <angles/angles.h>
#include <costmap_2d/cost_values.h>
#include <nav_msgs/OccupancyGrid.h>
#include <nav_msgs/Path.h>
#include <ros/ros.h>
#include <rosbag/bag.h>
#include <rosbag/view.h>
#include <tf2/utils.h>

#include "nav2_smac_planner/collision_checker.hpp"
#include "smac_lattice_planner_mbf/state_lattice_smoother.hpp"
#include "smac_lattice_planner_mbf/theta_state_suffix.hpp"

namespace
{
using PosePath = smac_lattice_planner_mbf::theta_state_suffix::PosePath;
using Quality = smac_lattice_planner_mbf::StatePathQuality;

constexpr char kPlanTopic[] = "/move_base_flex/SmacLatticePlanner/plan";
constexpr char kCostmapTopic[] = "/move_base_flex/global_costmap/costmap";

unsigned char decodePublishedCost(int8_t value)
{
  if (value < 0) {
    return costmap_2d::NO_INFORMATION;
  }
  if (value >= 100) {
    return costmap_2d::LETHAL_OBSTACLE;
  }
  if (value == 99) {
    return costmap_2d::INSCRIBED_INFLATED_OBSTACLE;
  }
  if (value == 0) {
    return costmap_2d::FREE_SPACE;
  }
  // Conservative inverse of Costmap2DPublisher's [1,252] -> [1,98].
  return static_cast<unsigned char>(std::min(
    252, 1 + static_cast<int>(std::ceil((value - 1) * 251.0 / 97.0))));
}

std::unique_ptr<costmap_2d::Costmap2D> convertCostmap(
  const nav_msgs::OccupancyGrid & message)
{
  auto costmap = std::make_unique<costmap_2d::Costmap2D>(
    message.info.width, message.info.height, message.info.resolution,
    message.info.origin.position.x, message.info.origin.position.y,
    costmap_2d::NO_INFORMATION);
  for (unsigned int y = 0; y < message.info.height; ++y) {
    for (unsigned int x = 0; x < message.info.width; ++x) {
      costmap->setCost(
        x, y, decodePublishedCost(message.data[y * message.info.width + x]));
    }
  }
  return costmap;
}

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

bool worldToMapContinuous(
  const costmap_2d::Costmap2D & costmap, double wx, double wy,
  float & mx, float & my)
{
  const double x = (wx - costmap.getOriginX()) / costmap.getResolution();
  const double y = (wy - costmap.getOriginY()) / costmap.getResolution();
  if (!std::isfinite(x) || !std::isfinite(y) || x < 0.0 || y < 0.0 ||
    x >= static_cast<double>(costmap.getSizeInCellsX()) ||
    y >= static_cast<double>(costmap.getSizeInCellsY()))
  {
    return false;
  }
  mx = static_cast<float>(x);
  my = static_cast<float>(y);
  return true;
}

bool validateContinuous(
  const PosePath & path, const costmap_2d::Costmap2D & costmap,
  nav2_smac_planner::GridCollisionChecker & checker)
{
  if (path.empty()) {
    return false;
  }
  float previous_x = 0.0f;
  float previous_y = 0.0f;
  double previous_yaw = 0.0;
  bool have_previous = false;
  for (const auto & pose : path) {
    float x = 0.0f;
    float y = 0.0f;
    const double yaw = tf2::getYaw(pose.pose.orientation);
    if (!worldToMapContinuous(
        costmap, pose.pose.position.x, pose.pose.position.y, x, y) ||
      checker.inCollisionAtYaw(x, y, yaw, false) ||
      (have_previous && checker.inCollisionContinuous(
        previous_x, previous_y, previous_yaw, x, y, yaw, false)))
    {
      return false;
    }
    previous_x = x;
    previous_y = y;
    previous_yaw = yaw;
    have_previous = true;
  }
  return true;
}

struct EdgeStats
{
  int moving_yaw_over_010{0};
  int moving_yaw_over_020{0};
  int in_place_rotations{0};
  double max_moving_yaw{0.0};
  double rms_moving_yaw{0.0};
};

EdgeStats summarizeEdges(const PosePath & path)
{
  EdgeStats result;
  double squared_sum = 0.0;
  int moving_count = 0;
  for (std::size_t i = 1u; i < path.size(); ++i) {
    const double distance = std::hypot(
      path[i].pose.position.x - path[i - 1u].pose.position.x,
      path[i].pose.position.y - path[i - 1u].pose.position.y);
    const double yaw = std::abs(angles::shortest_angular_distance(
      tf2::getYaw(path[i - 1u].pose.orientation),
      tf2::getYaw(path[i].pose.orientation)));
    if (distance <= 1e-4) {
      if (yaw > 1e-3) {
        ++result.in_place_rotations;
      }
      continue;
    }
    ++moving_count;
    squared_sum += yaw * yaw;
    result.max_moving_yaw = std::max(result.max_moving_yaw, yaw);
    result.moving_yaw_over_010 += yaw > 0.10 ? 1 : 0;
    result.moving_yaw_over_020 += yaw > 0.20 ? 1 : 0;
  }
  result.rms_moving_yaw = moving_count > 0 ?
    std::sqrt(squared_sum / static_cast<double>(moving_count)) : 0.0;
  return result;
}

void printQuality(const char * name, const Quality & q, const EdgeStats & e)
{
  std::cout << std::fixed << std::setprecision(9)
            << name << " length_m=" << q.length_m
            << " mean_center_cost=" << q.mean_center_cost
            << " max_center_cost=" << q.max_center_cost
            << " p95_abs_curvature=" << q.p95_abs_curvature_radpm
            << " max_abs_curvature=" << q.max_abs_curvature_radpm
            << " curvature_tv=" << q.curvature_total_variation_radpm
            << " max_curvature_jump=" << q.max_curvature_jump_radpm
            << " curvature_direction_changes=" << q.curvature_direction_changes
            << " in_place_rotations=" << q.in_place_rotations
            << " moving_dyaw_gt_010=" << e.moving_yaw_over_010
            << " moving_dyaw_gt_020=" << e.moving_yaw_over_020
            << " max_moving_dyaw=" << e.max_moving_yaw
            << " rms_moving_dyaw=" << e.rms_moving_yaw << '\n';
}

void writeCsv(const std::string & filename, const PosePath & path)
{
  std::ofstream stream(filename);
  if (!stream) {
    throw std::runtime_error("cannot open output file: " + filename);
  }
  stream << "index,x,y,yaw\n";
  stream << std::setprecision(16);
  for (std::size_t i = 0u; i < path.size(); ++i) {
    stream << i << ',' << path[i].pose.position.x << ','
           << path[i].pose.position.y << ','
           << tf2::getYaw(path[i].pose.orientation) << '\n';
  }
}

nav_msgs::Path::ConstPtr loadPlan(const std::string & bag_path, std::size_t pose_count)
{
  rosbag::Bag bag(bag_path, rosbag::bagmode::Read);
  rosbag::View view(bag, rosbag::TopicQuery(std::string(kPlanTopic)));
  for (const auto & message : view) {
    const auto plan = message.instantiate<nav_msgs::Path>();
    if (plan && plan->poses.size() == pose_count) {
      bag.close();
      return plan;
    }
  }
  bag.close();
  throw std::runtime_error("requested plan pose count was not found in bag");
}

nav_msgs::OccupancyGrid::ConstPtr loadCostmap(const std::string & bag_path)
{
  rosbag::Bag bag(bag_path, rosbag::bagmode::Read);
  rosbag::View view(bag, rosbag::TopicQuery(std::string(kCostmapTopic)));
  nav_msgs::OccupancyGrid::ConstPtr costmap;
  for (const auto & message : view) {
    const auto candidate = message.instantiate<nav_msgs::OccupancyGrid>();
    if (candidate) {
      costmap = candidate;
    }
  }
  bag.close();
  if (!costmap) {
    throw std::runtime_error("no full global costmap was found in bag");
  }
  return costmap;
}
}  // namespace

int main(int argc, char ** argv)
{
  ros::init(argc, argv, "state_lattice_smoother_bag_replay",
    ros::init_options::AnonymousName | ros::init_options::NoSigintHandler);
  if (argc != 6) {
    std::cerr << "usage: " << argv[0]
              << " PATH_BAG COSTMAP_BAG PLAN_POSE_COUNT SUFFIX_START OUTPUT_PREFIX\n";
    return 2;
  }
  try {
    const std::size_t plan_pose_count = std::stoul(argv[3]);
    const std::size_t suffix_start = std::stoul(argv[4]);
    const auto plan_message = loadPlan(argv[1], plan_pose_count);
    const auto costmap_message = loadCostmap(argv[2]);
    if (suffix_start >= plan_message->poses.size()) {
      throw std::runtime_error("suffix start is outside the selected plan");
    }
    PosePath raw(
      plan_message->poses.begin() + static_cast<std::ptrdiff_t>(suffix_start),
      plan_message->poses.end());
    auto costmap = convertCostmap(*costmap_message);
    nav2_smac_planner::GridCollisionChecker checker(costmap.get(), 72u);
    checker.setFootprint(robotFootprint(), false, 0.0);
    checker.setCollisionCheckResolution(0.01);

    smac_lattice_planner_mbf::StateLatticeSmootherParams params;
    params.max_time = 10.0;
    smac_lattice_planner_mbf::StateLatticeSmoother smoother(params, 0.4);
    const auto result = smoother.smooth(
      raw, *costmap, checker, false,
      smac_lattice_planner_mbf::StateLatticeSmoother::Clock::now() +
      std::chrono::seconds(10));
    const PosePath & candidate = result.accepted ?
      result.path : result.rejected_candidate_path;

    std::string raw_kinematic_reason;
    const bool raw_kinematic =
      smac_lattice_planner_mbf::theta_state_suffix::
      containsKinematicallyContinuousForwardOrRotation(raw, raw_kinematic_reason);
    std::string candidate_kinematic_reason;
    const bool candidate_kinematic = !candidate.empty() &&
      smac_lattice_planner_mbf::theta_state_suffix::
      containsKinematicallyContinuousForwardOrRotation(
      candidate, candidate_kinematic_reason);
    const bool raw_continuous_safe = validateContinuous(raw, *costmap, checker);
    const bool candidate_continuous_safe =
      !candidate.empty() && validateContinuous(candidate, *costmap, checker);

    std::cout << "RESULT accepted=" << std::boolalpha << result.accepted
              << " reason=\"" << result.reason << "\""
              << " iterations=" << result.iterations
              << " raw_poses=" << raw.size()
              << " candidate_poses=" << candidate.size()
              << " raw_continuous_safe=" << raw_continuous_safe
              << " candidate_continuous_safe=" << candidate_continuous_safe
              << " raw_kinematic=" << raw_kinematic
              << " candidate_kinematic=" << candidate_kinematic << '\n';
    if (!raw_kinematic_reason.empty()) {
      std::cout << "RAW_KINEMATIC_REASON " << raw_kinematic_reason << '\n';
    }
    if (!candidate_kinematic_reason.empty()) {
      std::cout << "CANDIDATE_KINEMATIC_REASON "
                << candidate_kinematic_reason << '\n';
    }
    printQuality("RAW", result.raw_quality, summarizeEdges(raw));
    if (!candidate.empty()) {
      printQuality(
        "SMOOTHED", result.candidate_quality, summarizeEdges(candidate));
    }
    const std::string output_prefix = argv[5];
    writeCsv(output_prefix + "_raw.csv", raw);
    if (!candidate.empty()) {
      writeCsv(output_prefix + "_smoothed.csv", candidate);
    }
    return result.accepted && candidate_continuous_safe && candidate_kinematic ? 0 : 1;
  } catch (const std::exception & error) {
    std::cerr << "ERROR " << error.what() << '\n';
    return 3;
  }
}

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstdlib>
#include <iostream>
#include <optional>
#include <string>
#include <vector>

#include <geometry_msgs/PoseStamped.h>
#include <nav_msgs/OccupancyGrid.h>
#include <ros/ros.h>
#include <tf/transform_datatypes.h>

#include "theta_star_planner/se2_path_refiner.h"
#include "theta_star_planner/theta_star.h"
#include "theta_star_planner/theta_star_planner.h"

namespace
{

geometry_msgs::PoseStamped poseAt(double x, double y, double yaw)
{
  geometry_msgs::PoseStamped pose;
  pose.header.frame_id = "map";
  pose.pose.position.x = x;
  pose.pose.position.y = y;
  pose.pose.orientation = tf::createQuaternionMsgFromYaw(yaw);
  return pose;
}

unsigned char occupancyToRawCost(std::int8_t occupancy)
{
  if (occupancy < 0) {
    return costmap_2d::NO_INFORMATION;
  }
  if (occupancy == 0) {
    return costmap_2d::FREE_SPACE;
  }
  if (occupancy >= 100) {
    return costmap_2d::LETHAL_OBSTACLE;
  }
  if (occupancy >= 99) {
    return costmap_2d::INSCRIBED_INFLATED_OBSTACLE;
  }
  // Invert Costmap2DPublisher's 1..98 soft-cost translation conservatively:
  // choose the highest raw value that could have produced this occupancy.
  return static_cast<unsigned char>(std::clamp(
    1 + static_cast<int>(std::ceil(
      static_cast<double>(occupancy - 1) * 251.0 / 97.0)),
    1, 252));
}

class AuditPlanner : public mbf_global_planner::ThetaStarPlanner
{
public:
  using mbf_global_planner::ThetaStarPlanner::assignPathOrientations;
  using mbf_global_planner::ThetaStarPlanner::buildTerminalApproachGeometry;
  using mbf_global_planner::ThetaStarPlanner::linearInterpolation;

  explicit AuditPlanner(costmap_2d::Costmap2D * costmap)
  {
    costmap_ = costmap;
    costmap_ros_ = nullptr;
    planner_ = std::make_unique<theta_star::ThetaStar>();
    planner_->costmap_ = costmap;
    planner_->max_allowed_cost_ = 26;
    planner_->allow_unknown_ = true;
    planner_->w_traversal_cost_ = 8.0;
    planner_->w_euc_cost_ = 2.0;
    planner_->w_heuristic_cost_ = 1.0;
    planner_->terminal_checking_interval_ = 1024;
  }

  bool topologyPath(
    const geometry_msgs::PoseStamped & start,
    const geometry_msgs::PoseStamped & goal,
    std::vector<geometry_msgs::PoseStamped> & path,
    std::string & reason)
  {
    planner_->setStartAndGoal(start, goal);
    if (planner_->isUnsafeToPlan()) {
      reason = "unsafe start or goal centre";
      return false;
    }
    std::vector<coordsW> raw;
    if (!planner_->generatePath(raw)) {
      reason = "Theta topology search failed";
      return false;
    }
    std::vector<coordsW> points;
    points.push_back({start.pose.position.x, start.pose.position.y});
    for (const auto & point : raw) {
      if (std::hypot(
          point.x - points.back().x,
          point.y - points.back().y) > 1e-9)
      {
        points.push_back(point);
      }
    }
    if (std::hypot(
        goal.pose.position.x - points.back().x,
        goal.pose.position.y - points.back().y) > 1e-9)
    {
      points.push_back({goal.pose.position.x, goal.pose.position.y});
    }
    path = linearInterpolation(points, costmap_->getResolution());
    assignPathOrientations(path, tf::getYaw(goal.pose.orientation));
    path.front().pose.orientation = start.pose.orientation;
    if (!isCentrelineCollisionFree(path)) {
      reason = "interpolated topology path violates centre clearance";
      return false;
    }
    return true;
  }

  bool centrelineSafe(
    const std::vector<geometry_msgs::PoseStamped> & path)
  {
    return isCentrelineCollisionFree(path);
  }
};

}  // namespace

int main(int argc, char ** argv)
{
  ros::init(argc, argv, "theta_star_connection_audit");
  if (argc < 7 || (argc - 1) % 6 != 0) {
    std::cerr << "usage: theta_star_connection_audit "
              << "sx sy syaw gx gy gyaw [...]\n";
    return 2;
  }

  ros::NodeHandle node;
  const auto map_message = ros::topic::waitForMessage<nav_msgs::OccupancyGrid>(
    "/move_base_flex/global_costmap/costmap", node, ros::Duration(10.0));
  if (!map_message) {
    std::cerr << "failed to receive live costmap\n";
    return 3;
  }

  costmap_2d::Costmap2D costmap(
    map_message->info.width,
    map_message->info.height,
    map_message->info.resolution,
    map_message->info.origin.position.x,
    map_message->info.origin.position.y,
    costmap_2d::NO_INFORMATION);
  for (unsigned int y = 0; y < map_message->info.height; ++y) {
    for (unsigned int x = 0; x < map_message->info.width; ++x) {
      costmap.setCost(
        x, y, occupancyToRawCost(
          map_message->data[y * map_message->info.width + x]));
    }
  }
  // The published footprint topic is already transformed into map frame and
  // therefore cannot be passed to CostmapModel as a base_link polygon. Use
  // the exact production footprint loaded by costmap_common.yaml.
  std::vector<geometry_msgs::Point> footprint(4);
  footprint[0].x = -0.2185; footprint[0].y = -0.325;
  footprint[1].x = -0.2185; footprint[1].y = 0.325;
  footprint[2].x = 0.6315; footprint[2].y = 0.325;
  footprint[3].x = 0.6315; footprint[3].y = -0.325;

  AuditPlanner planner(&costmap);
  theta_star::SE2RefinerConfig config;
  config.enabled = true;
  config.allow_reverse = true;
  // Match the production ThetaStarPlanner/se2_allow_unknown setting so the
  // live connection audit does not reject small unknown-map holes differently.
  config.allow_unknown = true;
  config.yaw_bins = 32;
  config.max_expansions = 300000;
  config.max_repairs = 8;
  config.max_allowed_center_cost = 26;
  config.motion_step = 0.10;
  config.collision_check_step = 0.025;
  config.goal_position_tolerance = 0.12;
  config.goal_yaw_tolerance = 0.20;
  config.repair_window_lengths = {1.8, 3.0, 4.5, 6.0};
  config.corridor_widths = {0.75, 1.25, 2.0};

  bool all_safe = true;
  int transition = 0;
  for (int arg = 1; arg + 5 < argc; arg += 6, ++transition) {
    const auto start = poseAt(
      std::stod(argv[arg]), std::stod(argv[arg + 1]),
      std::stod(argv[arg + 2]));
    const auto goal = poseAt(
      std::stod(argv[arg + 3]), std::stod(argv[arg + 4]),
      std::stod(argv[arg + 5]));
    std::vector<geometry_msgs::PoseStamped> topology;
    std::string reason;
    if (!planner.topologyPath(start, goal, topology, reason)) {
      std::cout << "AUDIT " << transition
                << " topology=FAIL safety=FAIL reason=\""
                << reason << "\"\n";
      all_safe = false;
      continue;
    }

    std::vector<geometry_msgs::PoseStamped> candidate = topology;
    bool terminal_geometry = false;
    for (double straight = 0.40; straight >= -1e-9; straight -= 0.10) {
      const auto shaped = AuditPlanner::buildTerminalApproachGeometry(
        topology, goal, std::max(0.0, straight), 0.0, 0.05, 1.80,
        [&planner](const auto & path) {
          return planner.centrelineSafe(path);
        });
      if (shaped.has_value()) {
        candidate = shaped.value();
        terminal_geometry = true;
        break;
      }
    }
    candidate.front().pose.orientation = start.pose.orientation;

    const auto refined = theta_star::SE2PathRefiner::refine(
      costmap, footprint, candidate, config);
    const bool swept_safe = refined.success &&
      !theta_star::SE2PathRefiner::firstUnsafeSegment(
        costmap, footprint, refined.path, 0.025, config.allow_unknown).has_value();
    std::cout << "AUDIT " << transition
              << " topology=PASS safety=" << (swept_safe ? "PASS" : "FAIL")
              << " terminal=" << (terminal_geometry ? "SHAPED" : "FALLBACK")
              << " repairs=" << refined.repairs
              << " expansions=" << refined.expansions
              << " points=" << refined.path.size()
              << " reason=\"" << refined.message << "\"\n";
    if (!swept_safe) {
      const auto unsafe = theta_star::SE2PathRefiner::firstUnsafeSegment(
        costmap, footprint, candidate, 0.025, config.allow_unknown);
      if (unsafe.has_value() && unsafe.value() < candidate.size()) {
        const auto & pose = candidate[unsafe.value()];
        std::cout << "DETAIL " << transition
                  << " unsafe_index=" << unsafe.value()
                  << " unsafe_x=" << pose.pose.position.x
                  << " unsafe_y=" << pose.pose.position.y
                  << " unsafe_yaw=" << tf::getYaw(pose.pose.orientation)
                  << " start_safe=" << theta_star::SE2PathRefiner::isPoseSafe(
                       costmap, footprint, candidate.front(), config.allow_unknown)
                  << " goal_safe=" << theta_star::SE2PathRefiner::isPoseSafe(
                       costmap, footprint, candidate.back(), config.allow_unknown)
                  << "\n";
      }
    }
    all_safe = all_safe && swept_safe;
  }
  return all_safe ? 0 : 1;
}

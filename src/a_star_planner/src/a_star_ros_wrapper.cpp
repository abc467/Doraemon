#include "a_star_planner/a_star_ros_wrapper.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <utility>

PLUGINLIB_EXPORT_CLASS(a_star_planner::AStarROSWrapper, nav_core::BaseGlobalPlanner)
PLUGINLIB_EXPORT_CLASS(a_star_planner::AStarROSWrapper, mbf_costmap_core::CostmapPlanner)

namespace a_star_planner
{
namespace
{
constexpr uint32_t MBF_SUCCESS = 0;
constexpr uint32_t MBF_CANCELED = 51;
constexpr uint32_t MBF_INVALID_START = 52;
constexpr uint32_t MBF_INVALID_GOAL = 53;
constexpr uint32_t MBF_NO_PATH_FOUND = 54;
constexpr uint32_t MBF_PAT_EXCEEDED = 55;
constexpr uint32_t MBF_EMPTY_PATH = 56;
constexpr uint32_t MBF_INTERNAL_ERROR = 60;
}  // namespace

using Point = rmp::path_planner::PathPlanner::Point3d;
using Path  = rmp::path_planner::PathPlanner::Points3d;
using Node  = rmp::common::structure::Node<int>;

static inline geometry_msgs::Quaternion yawToQuat(double yaw)
{
  geometry_msgs::Quaternion q;
  q.x = 0.0;
  q.y = 0.0;
  q.z = std::sin(yaw * 0.5);
  q.w = std::cos(yaw * 0.5);
  return q;
}

static inline double quatToYaw(const geometry_msgs::Quaternion& q)
{
  const double siny_cosp = 2.0 * (q.w * q.z + q.x * q.y);
  const double cosy_cosp = 1.0 - 2.0 * (q.y * q.y + q.z * q.z);
  return std::atan2(siny_cosp, cosy_cosp);
}

static inline double normalizeAngle(double a)
{
  while (a > M_PI) a -= 2.0 * M_PI;
  while (a < -M_PI) a += 2.0 * M_PI;
  return a;
}

Point quadraticBezierPoint(const Point& p0, const Point& p1, const Point& p2, double t)
{
  const double u = 1.0 - t;
  const double tt = t * t;
  const double uu = u * u;
  const double x = uu * p0.x() + 2.0 * u * t * p1.x() + tt * p2.x();
  const double y = uu * p0.y() + 2.0 * u * t * p1.y() + tt * p2.y();
  return Point(x, y);
}

AStarROSWrapper::AStarROSWrapper()
  : initialized_(false)
{
}

void AStarROSWrapper::initialize(std::string name, costmap_2d::Costmap2DROS* costmap_ros)
{
  if (initialized_) {
    return;
  }

  costmap_ros_ = costmap_ros;
  frame_id_ = costmap_ros_->getGlobalFrameID();
  ros::NodeHandle private_nh("~/" + name);

  private_nh.param("obstacle_factor", obstacle_factor_, 0.08);
  private_nh.param("final_path_collision_threshold", final_path_collision_threshold_, 200);
  private_nh.param("interpolation_resolution", interpolation_resolution_, 0.1);
  private_nh.param("smoothing_factor", smoothing_factor_, 0.4);
  private_nh.param("enable_post_processing", enable_post_processing_, true);
  private_nh.param("append_goal_orientation_point", append_goal_orientation_point_, true);
  private_nh.param("goal_append_pos_tolerance", goal_append_pos_tolerance_, 1e-3);
  private_nh.param("goal_yaw_append_threshold", goal_yaw_append_threshold_, 1e-3);
  private_nh.param("prefer_straight_line", prefer_straight_line_, true);
  private_nh.param("validate_final_path", validate_final_path_, false);
  private_nh.param("record_expansion", record_expansion_, false);
  private_nh.param("cancel_check_interval", cancel_check_interval_, 1024);
  private_nh.param("max_planning_time", max_planning_time_s_, 0.0);

  planner_ = std::make_unique<rmp::path_planner::ThetaStarPathPlanner>(costmap_ros_, obstacle_factor_);
  planner_->setCancelChecker([this]() { return cancel_requested_.load(); });
  planner_->configurePlanning(record_expansion_, cancel_check_interval_, max_planning_time_s_);

  plan_pub_ = private_nh.advertise<nav_msgs::Path>("final_plan", 1);
  waypoints_pub_ = private_nh.advertise<visualization_msgs::Marker>("waypoints", 1);
  raw_waypoints_pub_ = private_nh.advertise<visualization_msgs::Marker>("raw_waypoints", 1);
  smoothed_waypoints_pub_ = private_nh.advertise<visualization_msgs::Marker>("smoothed_waypoints", 1);

  initialized_ = true;

  ROS_INFO("A* Planner: Using core search obstacle_factor: %.2f", obstacle_factor_);
  ROS_INFO("A* Planner: Using post-processing: %s", enable_post_processing_ ? "true" : "false");
  ROS_INFO("A* Planner: append_goal_orientation_point: %s", append_goal_orientation_point_ ? "true" : "false");
  ROS_INFO("A* Planner: prefer_straight_line: %s", prefer_straight_line_ ? "true" : "false");
  ROS_INFO("A* Planner: validate_final_path: %s", validate_final_path_ ? "true" : "false");
  ROS_INFO("A* Planner: record_expansion: %s", record_expansion_ ? "true" : "false");
  ROS_INFO("A* Planner: cancel_check_interval: %d", cancel_check_interval_);
  if (max_planning_time_s_ > 0.0) {
    ROS_INFO("A* Planner: internal max_planning_time: %.3f s", max_planning_time_s_);
  }
  if (enable_post_processing_) {
    ROS_INFO("A* Planner:  - Interpolation resolution: %.2f", interpolation_resolution_);
    ROS_INFO("A* Planner:  - Smoothing factor: %.2f", smoothing_factor_);
  }
  ROS_INFO("A* global planner plugin initialized.");
}

bool AStarROSWrapper::makePlan(const geometry_msgs::PoseStamped& start,
                               const geometry_msgs::PoseStamped& goal,
                               std::vector<geometry_msgs::PoseStamped>& plan)
{
  auto result = makePlanCommon(start, goal, 0.0);
  plan = std::move(result.plan);
  publishMarkers(plan, 2, 1.0f, 0.0f, 0.0f, "waypoints");
  return result.status == PlanStatus::SUCCESS && !plan.empty();
}

uint32_t AStarROSWrapper::makePlan(const geometry_msgs::PoseStamped& start,
                                   const geometry_msgs::PoseStamped& goal,
                                   double tolerance,
                                   std::vector<geometry_msgs::PoseStamped>& plan,
                                   double& cost,
                                   std::string& message)
{
  auto result = makePlanCommon(start, goal, tolerance);
  plan = std::move(result.plan);
  cost = result.cost;
  message = result.message;
  publishMarkers(plan, 2, 1.0f, 0.0f, 0.0f, "waypoints");
  return toMBFOutcome(result.status);
}

bool AStarROSWrapper::cancel()
{
  cancel_requested_.store(true);
  return true;
}

AStarROSWrapper::PlanResult AStarROSWrapper::makePlanCommon(const geometry_msgs::PoseStamped& start,
                                                            const geometry_msgs::PoseStamped& goal,
                                                            double tolerance)
{
  PlanResult result;
  result.status = PlanStatus::INTERNAL_ERROR;

  if (!initialized_ || planner_ == nullptr || costmap_ros_ == nullptr || costmap_ros_->getCostmap() == nullptr) {
    result.message = "planner is not initialized";
    ROS_ERROR("A* planner: %s", result.message.c_str());
    return result;
  }

  cancel_requested_.store(false);
  planner_->configurePlanning(record_expansion_, cancel_check_interval_, max_planning_time_s_);

  ROS_INFO("A* planner: Got a new goal.");

  Path empty_path;
  publishMarkers(empty_path, 0, 0.0f, 0.0f, 1.0f, "raw_waypoints");
  publishMarkers(empty_path, 1, 1.0f, 1.0f, 0.0f, "smoothed_waypoints");
  publishMarkers(empty_path, 2, 1.0f, 0.0f, 0.0f, "waypoints");

  if (!isPoseTraversable(start)) {
    result.status = PlanStatus::INVALID_START;
    result.message = "start pose is outside map or on a blocked cell";
    ROS_WARN("A* planner: %s", result.message.c_str());
    return result;
  }

  geometry_msgs::PoseStamped resolved_goal = goal;
  std::string goal_msg;
  if (!resolveGoalPose(goal, tolerance, resolved_goal, goal_msg)) {
    result.status = PlanStatus::INVALID_GOAL;
    result.message = goal_msg.empty() ? "goal pose is invalid" : goal_msg;
    ROS_WARN("A* planner: %s", result.message.c_str());
    return result;
  }

  if (prefer_straight_line_ && hasLineOfSight(start, resolved_goal)) {
    result.plan.reserve(3);

    auto start_pose = start;
    start_pose.header.frame_id = frame_id_;
    start_pose.header.stamp = ros::Time::now();
    result.plan.push_back(start_pose);

    geometry_msgs::PoseStamped goal_pose = resolved_goal;
    goal_pose.header.frame_id = frame_id_;
    goal_pose.header.stamp = start_pose.header.stamp;

    const double heading = std::atan2(goal_pose.pose.position.y - start_pose.pose.position.y,
                                      goal_pose.pose.position.x - start_pose.pose.position.x);
    result.plan.front().pose.orientation = yawToQuat(heading);
    goal_pose.pose.orientation = append_goal_orientation_point_ ? resolved_goal.pose.orientation : yawToQuat(heading);
    result.plan.push_back(goal_pose);

    if (append_goal_orientation_point_) {
      const double goal_yaw = quatToYaw(resolved_goal.pose.orientation);
      if (std::fabs(normalizeAngle(goal_yaw - heading)) > goal_yaw_append_threshold_) {
        geometry_msgs::PoseStamped yaw_goal = goal_pose;
        yaw_goal.pose.orientation = resolved_goal.pose.orientation;
        result.plan.push_back(yaw_goal);
      }
    }

    if (plan_pub_.getNumSubscribers() > 0U) {
      nav_msgs::Path path_msg;
      path_msg.header = goal_pose.header;
      path_msg.header.frame_id = frame_id_;
      path_msg.poses = result.plan;
      plan_pub_.publish(path_msg);
    }

    result.cost = computePlanCost(result.plan);
    result.status = PlanStatus::SUCCESS;
    result.message = goal_msg;
    ROS_INFO("A* planner: Used direct line path with %zu poses%s",
             result.plan.size(), goal_msg.empty() ? "" : " [goal relaxed]");
    return result;
  }

  Path rmp_path;
  Path expand_nodes;
  const Point start_point(start.pose.position.x, start.pose.position.y);
  const Point goal_point(resolved_goal.pose.position.x, resolved_goal.pose.position.y);

  const ros::WallTime plan_start = ros::WallTime::now();
  const bool success = planner_->plan(start_point, goal_point, rmp_path, expand_nodes);
  const double elapsed = (ros::WallTime::now() - plan_start).toSec();

  if (!success) {
    switch (planner_->getLastExitCode()) {
      case rmp::path_planner::ThetaStarPathPlanner::ExitCode::CANCELED:
        result.status = PlanStatus::CANCELED;
        result.message = "planning canceled";
        break;
      case rmp::path_planner::ThetaStarPathPlanner::ExitCode::TIMED_OUT:
        result.status = PlanStatus::TIMED_OUT;
        result.message = "planning timed out";
        break;
      case rmp::path_planner::ThetaStarPathPlanner::ExitCode::NO_PATH:
      default:
        result.status = PlanStatus::NO_PATH;
        result.message = "failed to find a path";
        break;
    }
    ROS_WARN("A* planner: %s in %.3f s (expanded=%zu)", result.message.c_str(), elapsed,
             planner_->getLastExpandedNodes());
    return result;
  }

  if (rmp_path.empty()) {
    result.status = PlanStatus::EMPTY_PATH;
    result.message = "planner returned an empty path";
    ROS_WARN("A* planner: %s", result.message.c_str());
    return result;
  }

  publishMarkers(rmp_path, 0, 0.0f, 0.0f, 1.0f, "raw_waypoints");

  Path final_path = enable_post_processing_ ? postProcessPath(rmp_path) : rmp_path;
  if (final_path.empty()) {
    result.status = PlanStatus::EMPTY_PATH;
    result.message = "post processing produced an empty path";
    ROS_WARN("A* planner: %s", result.message.c_str());
    return result;
  }

  result.plan.reserve(final_path.size() + 1);
  const ros::Time plan_time = ros::Time::now();

  for (const auto& pt : final_path) {
    geometry_msgs::PoseStamped pose;
    pose.header.stamp = plan_time;
    pose.header.frame_id = frame_id_;
    pose.pose.position.x = pt.x();
    pose.pose.position.y = pt.y();
    pose.pose.position.z = 0.0;
    pose.pose.orientation.w = 1.0;
    result.plan.push_back(pose);
  }

  if (result.plan.size() >= 2U) {
    std::vector<double> yaws(result.plan.size(), 0.0);
    for (std::size_t i = 0; i + 1U < result.plan.size(); ++i) {
      const double x0 = result.plan[i].pose.position.x;
      const double y0 = result.plan[i].pose.position.y;
      const double x1 = result.plan[i + 1U].pose.position.x;
      const double y1 = result.plan[i + 1U].pose.position.y;
      const double dx = x1 - x0;
      const double dy = y1 - y0;
      if (std::hypot(dx, dy) > 1e-6) {
        yaws[i] = std::atan2(dy, dx);
      } else if (i > 0U) {
        yaws[i] = yaws[i - 1U];
      }
    }
    yaws.back() = yaws[result.plan.size() - 2U];
    for (std::size_t i = 0; i < result.plan.size(); ++i) {
      result.plan[i].pose.orientation = yawToQuat(yaws[i]);
    }
  } else if (result.plan.size() == 1U) {
    result.plan[0].pose.orientation = resolved_goal.pose.orientation;
  }

  const double gx = resolved_goal.pose.position.x;
  const double gy = resolved_goal.pose.position.y;
  if (!result.plan.empty()) {
    const double lx = result.plan.back().pose.position.x;
    const double ly = result.plan.back().pose.position.y;
    if (std::hypot(gx - lx, gy - ly) > goal_append_pos_tolerance_) {
      geometry_msgs::PoseStamped gpose = result.plan.back();
      gpose.pose.position.x = gx;
      gpose.pose.position.y = gy;
      const double dx = gx - lx;
      const double dy = gy - ly;
      if (std::hypot(dx, dy) > 1e-6) {
        gpose.pose.orientation = yawToQuat(std::atan2(dy, dx));
      }
      result.plan.push_back(gpose);
    }
  }

  if (append_goal_orientation_point_ && !result.plan.empty()) {
    const double goal_yaw = quatToYaw(resolved_goal.pose.orientation);
    const double last_yaw = quatToYaw(result.plan.back().pose.orientation);
    if (std::fabs(normalizeAngle(goal_yaw - last_yaw)) > goal_yaw_append_threshold_) {
      geometry_msgs::PoseStamped last = result.plan.back();
      last.pose.position.x = gx;
      last.pose.position.y = gy;
      last.pose.orientation = resolved_goal.pose.orientation;
      result.plan.push_back(last);
    }
  }

  if (validate_final_path_ && result.plan.size() > 1U) {
    const unsigned char validation_threshold = static_cast<unsigned char>(final_path_collision_threshold_);
    auto is_collision = [&](const Node& node) {
      return costmap_ros_->getCostmap()->getCost(node.x(), node.y()) >= validation_threshold;
    };

    for (std::size_t i = 0; i + 1U < result.plan.size(); ++i) {
      const auto& p_start_world = result.plan[i].pose.position;
      const auto& p_end_world = result.plan[i + 1U].pose.position;
      if (std::hypot(p_end_world.x - p_start_world.x, p_end_world.y - p_start_world.y) < 1e-6) {
        continue;
      }

      unsigned int mx_start = 0;
      unsigned int my_start = 0;
      unsigned int mx_end = 0;
      unsigned int my_end = 0;
      if (!costmap_ros_->getCostmap()->worldToMap(p_start_world.x, p_start_world.y, mx_start, my_start) ||
          !costmap_ros_->getCostmap()->worldToMap(p_end_world.x, p_end_world.y, mx_end, my_end)) {
        result.plan.clear();
        result.status = PlanStatus::NO_PATH;
        result.message = "validated path goes outside map";
        ROS_WARN("A* planner: %s", result.message.c_str());
        return result;
      }

      const Node node_start(static_cast<int>(mx_start), static_cast<int>(my_start));
      const Node node_end(static_cast<int>(mx_end), static_cast<int>(my_end));
      if (rmp::common::geometry::CollisionChecker::BresenhamCollisionDetection(node_start, node_end, is_collision)) {
        result.plan.clear();
        result.status = PlanStatus::NO_PATH;
        result.message = "validated path collides with costmap obstacle";
        ROS_WARN("A* planner: %s", result.message.c_str());
        return result;
      }
    }
  }

  if (plan_pub_.getNumSubscribers() > 0U) {
    nav_msgs::Path path_msg;
    path_msg.header.stamp = ros::Time::now();
    path_msg.header.frame_id = frame_id_;
    path_msg.poses = result.plan;
    plan_pub_.publish(path_msg);
  }

  result.cost = computePlanCost(result.plan);
  result.status = PlanStatus::SUCCESS;
  result.message = goal_msg;

  ROS_INFO("A* planner: Found a path with %zu poses in %.3f s (expanded=%zu)%s",
           result.plan.size(), elapsed, planner_->getLastExpandedNodes(),
           goal_msg.empty() ? "" : " [goal relaxed]");
  return result;
}

bool AStarROSWrapper::resolveGoalPose(const geometry_msgs::PoseStamped& goal,
                                      double tolerance,
                                      geometry_msgs::PoseStamped& resolved_goal,
                                      std::string& message) const
{
  resolved_goal = goal;
  const auto* costmap = costmap_ros_->getCostmap();
  if (costmap == nullptr) {
    message = "costmap is not available";
    return false;
  }

  unsigned int goal_mx = 0;
  unsigned int goal_my = 0;
  if (!costmap->worldToMap(goal.pose.position.x, goal.pose.position.y, goal_mx, goal_my)) {
    message = "goal pose is outside the map";
    return false;
  }

  if (isCellTraversable(goal_mx, goal_my)) {
    return true;
  }

  if (tolerance <= 0.0) {
    message = "goal pose is on a blocked cell";
    return false;
  }

  const int radius_cells = std::max(1, static_cast<int>(std::ceil(tolerance / costmap->getResolution())));
  double best_dist_sq = std::numeric_limits<double>::max();
  int best_mx = -1;
  int best_my = -1;

  for (int dy = -radius_cells; dy <= radius_cells; ++dy) {
    for (int dx = -radius_cells; dx <= radius_cells; ++dx) {
      const double dist_sq = static_cast<double>(dx * dx + dy * dy);
      if (dist_sq > static_cast<double>(radius_cells * radius_cells)) {
        continue;
      }

      const int mx = static_cast<int>(goal_mx) + dx;
      const int my = static_cast<int>(goal_my) + dy;
      if (mx < 0 || my < 0 ||
          mx >= static_cast<int>(costmap->getSizeInCellsX()) ||
          my >= static_cast<int>(costmap->getSizeInCellsY())) {
        continue;
      }
      if (!isCellTraversable(static_cast<unsigned int>(mx), static_cast<unsigned int>(my))) {
        continue;
      }
      if (dist_sq < best_dist_sq) {
        best_dist_sq = dist_sq;
        best_mx = mx;
        best_my = my;
      }
    }
  }

  if (best_mx < 0 || best_my < 0) {
    message = "goal pose is blocked and no free cell was found within tolerance";
    return false;
  }

  double wx = 0.0;
  double wy = 0.0;
  costmap->mapToWorld(static_cast<unsigned int>(best_mx), static_cast<unsigned int>(best_my), wx, wy);
  resolved_goal.pose.position.x = wx;
  resolved_goal.pose.position.y = wy;
  message = "goal pose relaxed within tolerance";
  return true;
}

bool AStarROSWrapper::isPoseTraversable(const geometry_msgs::PoseStamped& pose) const
{
  const auto* costmap = costmap_ros_->getCostmap();
  if (costmap == nullptr) {
    return false;
  }

  unsigned int mx = 0;
  unsigned int my = 0;
  if (!costmap->worldToMap(pose.pose.position.x, pose.pose.position.y, mx, my)) {
    return false;
  }
  return isCellTraversable(mx, my);
}

bool AStarROSWrapper::hasLineOfSight(const geometry_msgs::PoseStamped& start,
                                     const geometry_msgs::PoseStamped& goal) const
{
  const auto* costmap = costmap_ros_->getCostmap();
  if (costmap == nullptr) {
    return false;
  }

  unsigned int start_mx = 0;
  unsigned int start_my = 0;
  unsigned int goal_mx = 0;
  unsigned int goal_my = 0;
  if (!costmap->worldToMap(start.pose.position.x, start.pose.position.y, start_mx, start_my) ||
      !costmap->worldToMap(goal.pose.position.x, goal.pose.position.y, goal_mx, goal_my)) {
    return false;
  }

  if (!isCellTraversable(start_mx, start_my) || !isCellTraversable(goal_mx, goal_my)) {
    return false;
  }

  const Node start_node(static_cast<int>(start_mx), static_cast<int>(start_my));
  const Node goal_node(static_cast<int>(goal_mx), static_cast<int>(goal_my));
  return !rmp::common::geometry::CollisionChecker::BresenhamCollisionDetection(
      start_node, goal_node, [this](const Node& node) {
        return !isCellTraversable(static_cast<unsigned int>(node.x()), static_cast<unsigned int>(node.y()));
      });
}

bool AStarROSWrapper::isCellTraversable(unsigned int mx, unsigned int my) const
{
  const auto* costmap = costmap_ros_->getCostmap();
  if (costmap == nullptr || mx >= costmap->getSizeInCellsX() || my >= costmap->getSizeInCellsY()) {
    return false;
  }

  const unsigned char cost = costmap->getCost(mx, my);
  if (cost == costmap_2d::NO_INFORMATION) {
    return false;
  }

  const double obstacle_threshold = std::max(1.0, std::round(costmap_2d::LETHAL_OBSTACLE * obstacle_factor_));
  return cost < obstacle_threshold;
}

double AStarROSWrapper::computePlanCost(const std::vector<geometry_msgs::PoseStamped>& plan) const
{
  double cost = 0.0;
  for (std::size_t i = 1; i < plan.size(); ++i) {
    const double dx = plan[i].pose.position.x - plan[i - 1U].pose.position.x;
    const double dy = plan[i].pose.position.y - plan[i - 1U].pose.position.y;
    cost += std::hypot(dx, dy);
  }
  return cost;
}

uint32_t AStarROSWrapper::toMBFOutcome(PlanStatus status) const
{
  switch (status) {
    case PlanStatus::SUCCESS:
      return MBF_SUCCESS;
    case PlanStatus::CANCELED:
      return MBF_CANCELED;
    case PlanStatus::TIMED_OUT:
      return MBF_PAT_EXCEEDED;
    case PlanStatus::INVALID_START:
      return MBF_INVALID_START;
    case PlanStatus::INVALID_GOAL:
      return MBF_INVALID_GOAL;
    case PlanStatus::NO_PATH:
      return MBF_NO_PATH_FOUND;
    case PlanStatus::EMPTY_PATH:
      return MBF_EMPTY_PATH;
    case PlanStatus::INTERNAL_ERROR:
    default:
      return MBF_INTERNAL_ERROR;
  }
}

AStarROSWrapper::Path AStarROSWrapper::postProcessPath(const Path& sparse_path)
{
  if (sparse_path.size() < 2) {
    return sparse_path;
  }

  Path geometric_path;
  if (sparse_path.size() < 3) {
    geometric_path = sparse_path;
    publishMarkers(geometric_path, 1, 1.0f, 1.0f, 0.0f, "smoothed_waypoints");
  } else {
    Path smoothed_key_points;
    smoothed_key_points.push_back(sparse_path.front());

    geometric_path.push_back(sparse_path.front());
    for (std::size_t i = 1; i + 1U < sparse_path.size(); ++i) {
      const Point& p_prev = sparse_path[i - 1U];
      const Point& p_curr = sparse_path[i];
      const Point& p_next = sparse_path[i + 1U];

      const double prev_dist = std::hypot(p_curr.x() - p_prev.x(), p_curr.y() - p_prev.y());
      const double next_dist = std::hypot(p_next.x() - p_curr.x(), p_next.y() - p_curr.y());
      if (prev_dist < 1e-6 || next_dist < 1e-6) {
        continue;
      }

      const double cut_dist = std::min({prev_dist * smoothing_factor_, next_dist * smoothing_factor_,
                                        prev_dist / 2.0, next_dist / 2.0});
      const Point bezier_start(p_curr.x() - cut_dist * (p_curr.x() - p_prev.x()) / prev_dist,
                               p_curr.y() - cut_dist * (p_curr.y() - p_prev.y()) / prev_dist);
      const Point bezier_end(p_curr.x() + cut_dist * (p_next.x() - p_curr.x()) / next_dist,
                             p_curr.y() + cut_dist * (p_next.y() - p_curr.y()) / next_dist);

      smoothed_key_points.push_back(bezier_start);
      smoothed_key_points.push_back(p_curr);
      smoothed_key_points.push_back(bezier_end);

      geometric_path.push_back(bezier_start);
      constexpr int segments = 10;
      for (int j = 1; j < segments; ++j) {
        const double t = static_cast<double>(j) / static_cast<double>(segments);
        geometric_path.push_back(quadraticBezierPoint(bezier_start, p_curr, bezier_end, t));
      }
      geometric_path.push_back(bezier_end);
    }
    geometric_path.push_back(sparse_path.back());
    smoothed_key_points.push_back(sparse_path.back());
    publishMarkers(smoothed_key_points, 1, 1.0f, 1.0f, 0.0f, "smoothed_waypoints");
  }

  if (geometric_path.size() < 2) {
    return geometric_path;
  }

  Path final_path;
  final_path.push_back(geometric_path.front());

  double dist_accumulator = 0.0;
  for (std::size_t i = 0; i + 1U < geometric_path.size(); ++i) {
    const Point& p_start = geometric_path[i];
    const Point& p_end = geometric_path[i + 1U];

    const double segment_dist = std::hypot(p_end.x() - p_start.x(), p_end.y() - p_start.y());
    if (segment_dist < 1e-6) {
      continue;
    }

    const Point vector((p_end.x() - p_start.x()) / segment_dist,
                       (p_end.y() - p_start.y()) / segment_dist);

    double dist_walked_in_segment = 0.0;
    while (dist_walked_in_segment < segment_dist) {
      const double dist_to_next_sample = interpolation_resolution_ - dist_accumulator;
      if (dist_walked_in_segment + dist_to_next_sample < segment_dist) {
        dist_walked_in_segment += dist_to_next_sample;
        final_path.emplace_back(p_start.x() + dist_walked_in_segment * vector.x(),
                                p_start.y() + dist_walked_in_segment * vector.y());
        dist_accumulator = 0.0;
      } else {
        dist_accumulator += segment_dist - dist_walked_in_segment;
        break;
      }
    }
  }

  if (std::hypot(final_path.back().x() - geometric_path.back().x(),
                 final_path.back().y() - geometric_path.back().y()) > 1e-6) {
    final_path.push_back(geometric_path.back());
  }

  return final_path;
}

void AStarROSWrapper::publishMarkers(const Path& path, int id, float r, float g, float b, const std::string& ns)
{
  visualization_msgs::Marker points;
  points.header.frame_id = frame_id_;
  points.header.stamp = ros::Time::now();
  points.ns = ns;
  points.id = id;
  points.type = visualization_msgs::Marker::SPHERE_LIST;
  points.action = visualization_msgs::Marker::ADD;
  points.pose.orientation.w = 1.0;
  points.scale.x = (ns == "raw_waypoints") ? 0.2 : 0.15;
  points.scale.y = points.scale.x;
  points.scale.z = points.scale.x;
  points.color.r = r;
  points.color.g = g;
  points.color.b = b;
  points.color.a = 1.0;

  if (!path.empty()) {
    points.points.reserve(path.size());
    for (const auto& pt : path) {
      geometry_msgs::Point p;
      p.x = pt.x();
      p.y = pt.y();
      p.z = 0.1;
      points.points.push_back(p);
    }
  }

  if (ns == "raw_waypoints") {
    raw_waypoints_pub_.publish(points);
  } else if (ns == "smoothed_waypoints") {
    smoothed_waypoints_pub_.publish(points);
  } else if (ns == "waypoints") {
    waypoints_pub_.publish(points);
  }
}

void AStarROSWrapper::publishMarkers(const std::vector<geometry_msgs::PoseStamped>& path,
                                     int id,
                                     float r,
                                     float g,
                                     float b,
                                     const std::string& ns)
{
  Path temp_path;
  temp_path.reserve(path.size());
  for (const auto& pose : path) {
    temp_path.emplace_back(pose.pose.position.x, pose.pose.position.y);
  }
  publishMarkers(temp_path, id, r, g, b, ns);
}

}  // namespace a_star_planner

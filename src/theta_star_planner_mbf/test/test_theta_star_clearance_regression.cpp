#include <algorithm>
#include <cmath>
#include <vector>

#include <gtest/gtest.h>

#include <costmap_2d/costmap_2d.h>

#include "theta_star_planner/theta_star.h"
#include "theta_star_planner/theta_star_planner.h"

namespace
{

geometry_msgs::PoseStamped poseAtCell(
  const costmap_2d::Costmap2D & costmap, unsigned int mx, unsigned int my)
{
  geometry_msgs::PoseStamped pose;
  pose.header.frame_id = "map";
  costmap.mapToWorld(mx, my, pose.pose.position.x, pose.pose.position.y);
  pose.pose.orientation.w = 1.0;
  return pose;
}

class TestableThetaStarPlanner : public mbf_global_planner::ThetaStarPlanner
{
public:
  using mbf_global_planner::ThetaStarPlanner::assignPathOrientations;
  using mbf_global_planner::ThetaStarPlanner::buildTerminalApproachGeometry;
  using mbf_global_planner::ThetaStarPlanner::linearInterpolation;
  using mbf_global_planner::ThetaStarPlanner::maximumDiscreteCurvature;

  void configureForOpenMap(costmap_2d::Costmap2D * costmap)
  {
    costmap_ = costmap;
    costmap_ros_ = nullptr;
    planner_ = std::make_unique<theta_star::ThetaStar>();
    planner_->costmap_ = costmap;
    planner_->max_allowed_cost_ = 26;
    planner_->allow_unknown_ = false;
    planner_->setCancelChecker([this]() {return cancel_requested_.load();});
    use_footprint_path_check_ = false;
    terminal_approach_enabled_ = true;
    terminal_straight_length_ = 0.40;
    terminal_min_straight_length_ = 0.0;
    terminal_straight_length_step_ = 0.10;
    terminal_min_turn_radius_ = 0.0;
    terminal_sample_step_ = 0.05;
    terminal_max_prefix_splice_distance_ = 1.80;
    initialized_ = true;
  }

  void setTerminalApproachEnabled(bool enabled)
  {
    terminal_approach_enabled_ = enabled;
  }

  void requireFullTerminalStraightForTest()
  {
    terminal_min_straight_length_ = terminal_straight_length_;
  }
};

geometry_msgs::PoseStamped poseAt(double x, double y, double yaw = 0.0)
{
  geometry_msgs::PoseStamped pose;
  pose.header.frame_id = "map";
  pose.pose.position.x = x;
  pose.pose.position.y = y;
  pose.pose.orientation = tf::createQuaternionMsgFromYaw(yaw);
  return pose;
}

std::vector<geometry_msgs::Point> productionFootprint()
{
  std::vector<geometry_msgs::Point> footprint(4);
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

void expectGeneratedSegmentsSafe(
  theta_star::ThetaStar & planner,
  costmap_2d::Costmap2D & costmap,
  unsigned int start_x, unsigned int start_y,
  unsigned int goal_x, unsigned int goal_y)
{
  const auto start = poseAtCell(costmap, start_x, start_y);
  const auto goal = poseAtCell(costmap, goal_x, goal_y);
  planner.setStartAndGoal(start, goal);

  std::vector<coordsW> raw_path;
  ASSERT_TRUE(planner.generatePath(raw_path));
  ASSERT_GE(raw_path.size(), 2u);

  bool detoured = false;
  for (size_t i = 0; i < raw_path.size(); ++i) {
    unsigned int mx = 0;
    unsigned int my = 0;
    ASSERT_TRUE(costmap.worldToMap(raw_path[i].x, raw_path[i].y, mx, my));
    if ((start_y == goal_y && my != start_y) ||
        (start_x == goal_x && mx != start_x)) {
      detoured = true;
    }

    if (i == 0) {
      continue;
    }

    unsigned int previous_mx = 0;
    unsigned int previous_my = 0;
    ASSERT_TRUE(costmap.worldToMap(
      raw_path[i - 1].x, raw_path[i - 1].y, previous_mx, previous_my));
    EXPECT_TRUE(planner.isLineSafe(
      static_cast<int>(previous_mx), static_cast<int>(previous_my),
      static_cast<int>(mx), static_cast<int>(my)));
  }
  EXPECT_TRUE(detoured);
}

TEST(ThetaStarClearanceRegression, HorizontalBlockedCentreCellForcesDetour)
{
  costmap_2d::Costmap2D costmap(9, 7, 0.05, 0.0, 0.0, costmap_2d::FREE_SPACE);
  costmap.setCost(4, 3, 27);

  theta_star::ThetaStar planner;
  planner.costmap_ = &costmap;
  planner.max_allowed_cost_ = 26;

  coordsM blocked_cell{};
  unsigned char blocked_cost = 0;
  EXPECT_FALSE(planner.isLineSafe(1, 3, 7, 3, nullptr, &blocked_cell, &blocked_cost));
  EXPECT_EQ(blocked_cell.x, 4);
  EXPECT_EQ(blocked_cell.y, 3);
  EXPECT_EQ(blocked_cost, 27);

  expectGeneratedSegmentsSafe(planner, costmap, 1, 3, 7, 3);
}

TEST(ThetaStarClearanceRegression, VerticalBlockedCentreCellForcesDetour)
{
  costmap_2d::Costmap2D costmap(9, 7, 0.05, 0.0, 0.0, costmap_2d::FREE_SPACE);
  costmap.setCost(4, 3, 27);

  theta_star::ThetaStar planner;
  planner.costmap_ = &costmap;
  planner.max_allowed_cost_ = 26;

  EXPECT_FALSE(planner.isLineSafe(4, 1, 4, 5));
  expectGeneratedSegmentsSafe(planner, costmap, 4, 1, 4, 5);
}

TEST(ThetaStarClearanceRegression, DiagonalCannotCutBlockedCorner)
{
  costmap_2d::Costmap2D costmap(5, 5, 0.05, 0.0, 0.0, costmap_2d::FREE_SPACE);
  costmap.setCost(1, 0, 27);

  theta_star::ThetaStar planner;
  planner.costmap_ = &costmap;
  planner.max_allowed_cost_ = 26;

  EXPECT_FALSE(planner.isLineSafe(0, 0, 2, 2));
}

TEST(ThetaStarClearanceRegression, LastQueuedGoalStillReturnsAPath)
{
  costmap_2d::Costmap2D costmap(2, 1, 0.05, 0.0, 0.0, costmap_2d::FREE_SPACE);
  theta_star::ThetaStar planner;
  planner.costmap_ = &costmap;
  planner.max_allowed_cost_ = 252;
  planner.allow_unknown_ = false;
  planner.setStartAndGoal(
    poseAtCell(costmap, 0, 0), poseAtCell(costmap, 1, 0));

  std::vector<coordsW> path;
  ASSERT_TRUE(planner.generatePath(path));
  ASSERT_GE(path.size(), 2u);
  EXPECT_NEAR(path.front().x, 0.025, 1e-9);
  EXPECT_NEAR(path.back().x, 0.075, 1e-9);
}

TEST(ThetaStarClearanceRegression, ClearStartNeverMutatesMasterCostmap)
{
  costmap_2d::Costmap2D costmap(
    20, 20, 0.05, 0.0, 0.0, costmap_2d::FREE_SPACE);
  costmap.setCost(4, 5, 117);

  theta_star::ThetaStar planner;
  planner.costmap_ = &costmap;
  planner.max_allowed_cost_ = 26;
  planner.setStartAndGoal(
    poseAtCell(costmap, 4, 5), poseAtCell(costmap, 15, 5));
  planner.clearStart();

  EXPECT_EQ(costmap.getCost(4, 5), 117);
}

TEST(ThetaStarClearanceRegression, HighSoftCostStartEscapesWithoutMapWrite)
{
  costmap_2d::Costmap2D costmap(
    20, 9, 0.05, 0.0, 0.0, costmap_2d::FREE_SPACE);
  // Surround the start with a monotonic ramp. The start exceeds the normal
  // cost-26 clearance threshold but remains outside hard/inscribed collision.
  for (unsigned int y = 0; y < costmap.getSizeInCellsY(); ++y) {
    costmap.setCost(2, y, 100);
    costmap.setCost(3, y, 75);
    costmap.setCost(4, y, 50);
    costmap.setCost(5, y, 26);
  }

  theta_star::ThetaStar planner;
  planner.costmap_ = &costmap;
  planner.max_allowed_cost_ = 26;
  planner.allow_unknown_ = false;
  planner.setStartAndGoal(
    poseAtCell(costmap, 2, 4), poseAtCell(costmap, 16, 4));

  ASSERT_FALSE(planner.isUnsafeToPlan());
  std::vector<coordsW> path;
  ASSERT_TRUE(planner.generatePath(path));
  ASSERT_GE(path.size(), 2u);
  EXPECT_EQ(costmap.getCost(2, 4), 100);

  unsigned char previous_cost = 255;
  bool reached_normal_band = false;
  for (const auto & point : path) {
    unsigned int mx = 0;
    unsigned int my = 0;
    ASSERT_TRUE(costmap.worldToMap(point.x, point.y, mx, my));
    const unsigned char cost = costmap.getCost(mx, my);
    if (!reached_normal_band) {
      EXPECT_LE(cost, previous_cost);
      reached_normal_band = cost <= 26;
    } else {
      EXPECT_LE(cost, 26);
    }
    previous_cost = cost;
  }
  EXPECT_TRUE(reached_normal_band);
}

TEST(SE2FootprintRegression, RepairsTopologyPathThatClipsOffsetWall)
{
  costmap_2d::Costmap2D costmap(
    160, 120, 0.05, -4.0, -3.0, costmap_2d::FREE_SPACE);
  // The XY centre line y=0 remains free, but the robot's +0.325 m side clips
  // this finite wall. A valid differential-drive detour exists below it.
  for (double x = -0.80; x <= 0.80 + 1e-9; x += 0.05) {
    unsigned int mx = 0;
    unsigned int my = 0;
    ASSERT_TRUE(costmap.worldToMap(x, 0.30, mx, my));
    costmap.setCost(mx, my, costmap_2d::LETHAL_OBSTACLE);
  }

  std::vector<geometry_msgs::PoseStamped> topology_path;
  for (double x = -2.0; x <= 2.0 + 1e-9; x += 0.05) {
    topology_path.push_back(poseAt(x, 0.0, 0.0));
  }
  topology_path.back() = poseAt(2.0, 0.0, 0.0);
  const auto footprint = productionFootprint();
  ASSERT_TRUE(theta_star::SE2PathRefiner::firstUnsafeSegment(
    costmap, footprint, topology_path, 0.025, false).has_value());

  theta_star::SE2RefinerConfig config;
  config.allow_unknown = false;
  config.yaw_bins = 32;
  config.max_expansions = 180000;
  config.max_repairs = 4;
  config.motion_step = 0.10;
  config.collision_check_step = 0.025;
  config.repair_window_lengths = {1.0, 1.8, 3.0};
  config.corridor_widths = {0.60, 1.0};
  const auto result = theta_star::SE2PathRefiner::refine(
    costmap, footprint, topology_path, config);

  ASSERT_TRUE(result.success) << result.message;
  EXPECT_GT(result.repairs, 0);
  EXPECT_FALSE(theta_star::SE2PathRefiner::firstUnsafeSegment(
    costmap, footprint, result.path, 0.025, false).has_value());
  ASSERT_FALSE(result.path.empty());
  EXPECT_NEAR(result.path.front().pose.position.x, -2.0, 1e-9);
  EXPECT_NEAR(result.path.back().pose.position.x, 2.0, 1e-9);
  EXPECT_NEAR(result.path.back().pose.position.y, 0.0, 1e-9);
  EXPECT_NEAR(tf2::getYaw(result.path.back().pose.orientation), 0.0, 1e-9);
}

TEST(ThetaStarInterpolationRegression, IncludesBothEndpointsAndBoundsSpacing)
{
  const std::vector<coordsW> raw_path{{0.50, 0.50}, {0.62, 0.50}, {0.67, 0.55}};
  const auto dense_path = TestableThetaStarPlanner::linearInterpolation(raw_path, 0.05);

  ASSERT_GE(dense_path.size(), 4u);
  EXPECT_DOUBLE_EQ(dense_path.front().pose.position.x, raw_path.front().x);
  EXPECT_DOUBLE_EQ(dense_path.front().pose.position.y, raw_path.front().y);
  EXPECT_DOUBLE_EQ(dense_path.back().pose.position.x, raw_path.back().x);
  EXPECT_DOUBLE_EQ(dense_path.back().pose.position.y, raw_path.back().y);

  for (size_t i = 1; i < dense_path.size(); ++i) {
    const double spacing = std::hypot(
      dense_path[i].pose.position.x - dense_path[i - 1].pose.position.x,
      dense_path[i].pose.position.y - dense_path[i - 1].pose.position.y);
    EXPECT_LE(spacing, 0.05 + 1e-12);
  }
}

TEST(ThetaStarTerminalApproach, ReplacesYawJumpWithCurvatureBoundedGeometry)
{
  // The prefix arrives at the terminal anchor from below. The requested goal
  // is east-facing, so the connector must turn in geometry before entering
  // the final 0.4 m straight tail.
  const std::vector<geometry_msgs::PoseStamped> prefix{
    poseAt(0.00, -0.45),
    poseAt(0.25, -0.35),
    poseAt(0.50, -0.20),
    poseAt(0.85, 0.00),
  };
  const auto goal = poseAt(2.00, 0.00, 0.0);

  const auto result = TestableThetaStarPlanner::buildTerminalApproachGeometry(
    prefix, goal, 0.40, 0.40, 0.05, 1.80,
    [](const auto &) {return true;});

  ASSERT_TRUE(result.has_value());
  const auto & path = result.value();
  ASSERT_GE(path.size(), 8u);
  EXPECT_NEAR(path.back().pose.position.x, 2.00, 1e-12);
  EXPECT_NEAR(path.back().pose.position.y, 0.00, 1e-12);
  EXPECT_NEAR(tf2::getYaw(path.back().pose.orientation), 0.0, 1e-9);
  EXPECT_LE(
    TestableThetaStarPlanner::maximumDiscreteCurvature(path),
    1.05 / 0.40 + 1e-9);

  for (size_t index = 1; index < path.size(); ++index) {
    const double spacing = path_tools::euclidean_distance(
      path[index - 1], path[index]);
    EXPECT_GT(spacing, 1e-9);
  }

  // Every point in the final straight tail is tangent to the goal yaw. There
  // is no same-XY duplicate whose only purpose is to change orientation.
  for (auto it = path.rbegin(); it != path.rend(); ++it) {
    const double distance_to_goal = std::hypot(
      goal.pose.position.x - it->pose.position.x,
      goal.pose.position.y - it->pose.position.y);
    if (distance_to_goal > 0.40 + 1e-9) {
      break;
    }
    EXPECT_NEAR(tf2::getYaw(it->pose.orientation), 0.0, 1e-6);
  }
}

TEST(ThetaStarTerminalApproach, RejectsEveryGeometryWhenClearanceFails)
{
  const std::vector<geometry_msgs::PoseStamped> prefix{
    poseAt(0.0, 0.0), poseAt(0.5, 0.0), poseAt(0.9, 0.0)};
  const auto goal = poseAt(2.0, 0.0, 0.0);
  int validation_calls = 0;
  const auto result = TestableThetaStarPlanner::buildTerminalApproachGeometry(
    prefix, goal, 0.40, 0.40, 0.05, 1.80,
    [&](const auto &) {
      ++validation_calls;
      return false;
    });

  EXPECT_FALSE(result.has_value());
  EXPECT_GT(validation_calls, 0);
}

TEST(ThetaStarTerminalApproach, DifferentialDriveNeedsNoMinimumTurnRadius)
{
  const std::vector<geometry_msgs::PoseStamped> prefix{
    poseAt(0.0, 0.0), poseAt(0.5, 0.0), poseAt(0.9, 0.0)};
  const auto goal = poseAt(1.2, 0.5, M_PI_2);
  const auto result = TestableThetaStarPlanner::buildTerminalApproachGeometry(
    prefix, goal, 0.0, 0.0, 0.05, 1.80,
    [](const auto &) {return true;});

  ASSERT_TRUE(result.has_value());
  EXPECT_NEAR(result->back().pose.position.x, goal.pose.position.x, 1e-12);
  EXPECT_NEAR(result->back().pose.position.y, goal.pose.position.y, 1e-12);
  EXPECT_NEAR(
    tf2::getYaw(result->back().pose.orientation),
    tf2::getYaw(goal.pose.orientation), 1e-6);
}

TEST(ThetaStarTerminalApproach, ObservedBlockFourYawMismatchHasGeometricSolution)
{
  // Relative geometry from the observed failure: the robot was about 0.575 m
  // from the block start with roughly 102 degrees of yaw error. Planning to
  // the 1.1 m rear anchor creates room for a forward terminal connector.
  constexpr double goal_yaw = -2.3173;
  const auto goal = poseAt(0.0, 0.0, goal_yaw);
  std::optional<std::vector<geometry_msgs::PoseStamped>> result;
  for (double approach_length = 1.10;
       approach_length <= 1.80 + 1e-9 && !result.has_value();
       approach_length += 0.10) {
    const double anchor_x = -approach_length * std::cos(goal_yaw);
    const double anchor_y = -approach_length * std::sin(goal_yaw);
    const std::vector<coordsW> prefix_points{
      {-0.365, 0.444}, {anchor_x, anchor_y}};
    auto prefix = TestableThetaStarPlanner::linearInterpolation(
      prefix_points, 0.05);
    TestableThetaStarPlanner::assignPathOrientations(prefix);
    result = TestableThetaStarPlanner::buildTerminalApproachGeometry(
      prefix, goal, 0.40, 0.40, 0.05, 0.80,
      [](const auto &) {return true;});
  }

  ASSERT_TRUE(result.has_value());
  EXPECT_LE(
    TestableThetaStarPlanner::maximumDiscreteCurvature(result.value()),
    1.05 / 0.40 + 1e-9);
  EXPECT_NEAR(
    tf2::getYaw(result->back().pose.orientation), goal_yaw, 1e-6);
}

TEST(ThetaStarTerminalApproach, MakePlanUsesPoseApproachForObservedGeometry)
{
  costmap_2d::Costmap2D costmap(
    200, 200, 0.05, -5.0, -5.0, costmap_2d::FREE_SPACE);
  TestableThetaStarPlanner planner;
  planner.configureForOpenMap(&costmap);

  const auto start = poseAt(-0.365, 0.444, -0.5435);
  const auto goal = poseAt(0.0, 0.0, -2.3173);
  std::vector<geometry_msgs::PoseStamped> path;
  double cost = 0.0;
  std::string message;
  const auto outcome = planner.makePlan(
    start, goal, 0.0, path, cost, message);

  ASSERT_EQ(outcome, mbf_msgs::GetPathResult::SUCCESS) << message;
  EXPECT_EQ(message, "success with differential-drive terminal geometry");
  ASSERT_GT(path.size(), 10u);
  EXPECT_NEAR(
    tf2::getYaw(path.front().pose.orientation),
    tf2::getYaw(start.pose.orientation), 1e-6);
  EXPECT_NEAR(path.back().pose.position.x, goal.pose.position.x, 1e-12);
  EXPECT_NEAR(path.back().pose.position.y, goal.pose.position.y, 1e-12);
  EXPECT_NEAR(
    tf2::getYaw(path.back().pose.orientation),
    tf2::getYaw(goal.pose.orientation), 1e-6);
  EXPECT_GT(cost, std::hypot(
    goal.pose.position.x - start.pose.position.x,
    goal.pose.position.y - start.pose.position.y));
  for (size_t index = 1; index < path.size(); ++index) {
    const double spacing = path_tools::euclidean_distance(
      path[index - 1], path[index]);
    EXPECT_GT(spacing, 1e-9);
    EXPECT_LE(spacing, 0.05 + 1e-6);
  }
}

TEST(ThetaStarTerminalApproach, ReshapingFailureKeepsReachablePositionPath)
{
  costmap_2d::Costmap2D costmap(
    200, 200, 0.05, -5.0, -5.0, costmap_2d::FREE_SPACE);
  TestableThetaStarPlanner planner;
  planner.configureForOpenMap(&costmap);
  planner.requireFullTerminalStraightForTest();

  // This path is deliberately shorter than the minimum useful splice suffix,
  // so terminal reshaping has no candidate. XY reachability must still win.
  const auto start = poseAt(0.0, 0.0, 0.0);
  const auto goal = poseAt(0.06, 0.0, M_PI);
  std::vector<geometry_msgs::PoseStamped> path;
  double cost = 0.0;
  std::string message;
  const auto outcome = planner.makePlan(
    start, goal, 0.0, path, cost, message);

  ASSERT_EQ(outcome, mbf_msgs::GetPathResult::SUCCESS) << message;
  EXPECT_EQ(message, "success with reachable XY terminal fallback");
  ASSERT_GE(path.size(), 2u);
  EXPECT_NEAR(path.back().pose.position.x, goal.pose.position.x, 1e-12);
  EXPECT_NEAR(path.back().pose.position.y, goal.pose.position.y, 1e-12);
  EXPECT_NEAR(
    tf2::getYaw(path.back().pose.orientation),
    tf2::getYaw(goal.pose.orientation), 1e-6);
}

TEST(ThetaStarTerminalApproach, PositionModeDoesNotRequireTerminalConnector)
{
  costmap_2d::Costmap2D costmap(
    200, 200, 0.05, -5.0, -5.0, costmap_2d::FREE_SPACE);
  TestableThetaStarPlanner planner;
  planner.configureForOpenMap(&costmap);
  planner.setTerminalApproachEnabled(false);

  const auto start = poseAt(-0.365, 0.444, -0.5435);
  const auto goal = poseAt(0.0, 0.0, -2.3173);
  std::vector<geometry_msgs::PoseStamped> path;
  double cost = 0.0;
  std::string message;
  const auto outcome = planner.makePlan(
    start, goal, 0.0, path, cost, message);

  ASSERT_EQ(outcome, mbf_msgs::GetPathResult::SUCCESS) << message;
  ASSERT_GE(path.size(), 2u);
  EXPECT_EQ(message, "success");
  EXPECT_NEAR(path.back().pose.position.x, goal.pose.position.x, 1e-12);
  EXPECT_NEAR(path.back().pose.position.y, goal.pose.position.y, 1e-12);
  EXPECT_NEAR(
    tf2::getYaw(path.back().pose.orientation),
    tf2::getYaw(goal.pose.orientation), 1e-6);
  EXPECT_GT(
    path_tools::euclidean_distance(path[path.size() - 2], path.back()),
    1e-9);
}

}  // namespace

int main(int argc, char ** argv)
{
  ros::Time::init();
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

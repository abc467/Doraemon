#include <gtest/gtest.h>

#include <cmath>
#include <utility>
#include <vector>

#include <costmap_2d/cost_values.h>
#include <pluginlib/class_loader.hpp>
#include <tf2/LinearMath/Quaternion.h>
#include <tf2_geometry_msgs/tf2_geometry_msgs.h>

#include "mppi_controller/optimal_trajectory_validator.hpp"
#include "mppi_controller/critics/cost_critic.hpp"
#include "mppi_controller/critic_stats.hpp"
#include "mppi_controller/critic_data.hpp"
#include "mppi_controller/motion_models.hpp"
#include "mppi_controller/optimizer.hpp"
#include "mppi_controller/critics/path_angle_critic.hpp"
#include "mppi_controller/tools/goal_reached_evaluator.hpp"
#include "mppi_controller/tools/path_handler.hpp"
#include "mppi_controller/tools/utils.hpp"

namespace
{

class TestCostCritic : public mppi::critics::CostCritic
{
public:
  void configureFootprintMode(bool enabled, bool allow_unknown = false)
  {
    consider_footprint_ = enabled;
    allow_unknown_ = allow_unknown;
    possible_collision_cost_ = 1.0f;
  }

  bool collides(
    float center_cost, const costmap_2d::Costmap2D & costmap,
    const std::vector<geometry_msgs::Point> & footprint,
    float x = 1.50f, float y = 1.0f, float yaw = 0.0f)
  {
    base_local_planner::FootprintHelper helper;
    return inCollision(
      center_cost, x, y, yaw, costmap, helper, footprint);
  }
};

class TestOptimizer : public mppi::Optimizer
{
public:
  void configureSpeedLimitTest()
  {
    settings_.base_constraints = {
      0.4f, 0.0f, 0.0f, 0.8f, 1.0f, -1.0f, -1.0f, 1.0f, 1.0f};
    settings_.constraints = settings_.base_constraints;
    settings_.model_dt = 0.1f;
    motion_model_ = std::make_shared<mppi::DiffDriveMotionModel>();
    motion_model_->initialize(
      settings_.constraints, settings_.model_dt, 0.0f, 0.0f, 0.0f, false);
  }

  float vxMaximum() const {return settings_.constraints.vx_max;}
  float wzMaximum() const {return settings_.constraints.wz;}
  void resetPreservingSpeedLimit()
  {
    settings_.batch_size = 1u;
    settings_.time_steps = 2u;
    settings_.sampling_std = {0.1f, 0.1f, 0.1f};
    noise_generator_.initialize(settings_, false);
    reset(false);
    noise_generator_.shutdown();
  }

  void markCriticStateFailed()
  {
    critics_data_.fail_flag = true;
    critics_data_.furthest_reached_path_point = 4u;
    critics_data_.path_pts_valid = std::vector<bool>{true, false};
  }

  void clearCriticStateForRetry() {resetCriticStateForRetry();}
  void configureFallbackTest()
  {
    configureSpeedLimitTest();
    settings_.batch_size = 1u;
    settings_.time_steps = 2u;
    settings_.retry_attempt_limit = 1;
    settings_.sampling_std = {0.1f, 0.1f, 0.1f};
    noise_generator_.initialize(settings_, false);
  }
  bool invokeFallback(bool failed) {return fallback(failed);}
  bool criticFailed() const {return critics_data_.fail_flag;}
  bool hasFurthestPoint() const
  {
    return critics_data_.furthest_reached_path_point.has_value();
  }
  bool hasPathValidityCache() const
  {
    return critics_data_.path_pts_valid.has_value();
  }

  mppi::ValidationResult validateWithFinalGateDisabled()
  {
    trajectory_validation_enabled_ = false;
    return validateOptimizedTrajectory(Eigen::ArrayXXf());
  }
};

class TestPathHandler : public mppi::PathHandler
{
public:
  void configureTransformBuffer(
    const std::shared_ptr<tf2_ros::Buffer> & buffer, double tolerance)
  {
    tf_buffer_ = buffer;
    transform_tolerance_ = tolerance;
  }

  bool transform(
    const std::string & frame, const geometry_msgs::PoseStamped & input,
    geometry_msgs::PoseStamped & output) const
  {
    return transformPose(frame, input, output);
  }
};

geometry_msgs::PoseStamped stampedPose(double x, double y, double yaw = 0.0)
{
  geometry_msgs::PoseStamped result;
  result.header.frame_id = "map";
  result.pose.position.x = x;
  result.pose.position.y = y;
  tf2::Quaternion q;
  q.setRPY(0.0, 0.0, yaw);
  result.pose.orientation = tf2::toMsg(q);
  return result;
}

std::vector<geometry_msgs::Point> squareFootprint(double half_width = 0.20)
{
  std::vector<geometry_msgs::Point> result(4);
  result[0].x = -half_width;
  result[0].y = -half_width;
  result[1].x = -half_width;
  result[1].y = half_width;
  result[2].x = half_width;
  result[2].y = half_width;
  result[3].x = half_width;
  result[3].y = -half_width;
  return result;
}

Eigen::ArrayXXf straightTrajectory(double start_x, double end_x, double y)
{
  Eigen::ArrayXXf result(2, 3);
  result <<
    static_cast<float>(start_x), static_cast<float>(y), 0.0f,
    static_cast<float>(end_x), static_cast<float>(y), 0.0f;
  return result;
}

mppi::models::Path compactUTurnPath()
{
  std::vector<std::pair<float, float>> points;
  for (int index = 0; index <= 10; ++index) {
    points.emplace_back(0.05f * static_cast<float>(index), 0.0f);
  }
  constexpr float radius = 0.30f;
  constexpr int arc_intervals = 19;
  for (int index = 1; index <= arc_intervals; ++index) {
    const float angle = -static_cast<float>(M_PI_2) +
      static_cast<float>(M_PI) * static_cast<float>(index) /
      static_cast<float>(arc_intervals);
    points.emplace_back(
      0.50f + radius * std::cos(angle),
      0.30f + radius * std::sin(angle));
  }
  for (int index = 1; index <= 10; ++index) {
    points.emplace_back(0.50f - 0.05f * static_cast<float>(index), 0.60f);
  }

  mppi::models::Path path;
  path.reset(points.size());
  for (size_t index = 0; index < points.size(); ++index) {
    path.x(index) = points[index].first;
    path.y(index) = points[index].second;
  }
  return path;
}

mppi::models::Trajectories compactUTurnCandidates(bool reverse_rows)
{
  mppi::models::Trajectories trajectories;
  trajectories.reset(2u, 13u);
  for (int step = 0; step < 13; ++step) {
    const float ratio = static_cast<float>(step) / 12.0f;
    // The first short rollout ends exactly on the nearby return leg, but it
    // only travels 0.60 m and cannot have traversed the full U-turn.
    trajectories.x(0, step) = 0.0f;
    trajectories.y(0, step) = 0.60f * ratio;
    trajectories.x(1, step) = 0.40f * ratio;
    trajectories.y(1, step) = 0.0f;
  }
  if (reverse_rows) {
    trajectories.x.row(0).swap(trajectories.x.row(1));
    trajectories.y.row(0).swap(trajectories.y.row(1));
  }
  return trajectories;
}

size_t furthestReachedOnCompactUTurn(bool reverse_rows)
{
  mppi::models::State state;
  state.reset(2u, 13u);
  auto trajectories = compactUTurnCandidates(reverse_rows);
  auto path = compactUTurnPath();
  geometry_msgs::Pose goal;
  Eigen::ArrayXf costs = Eigen::ArrayXf::Zero(2);
  float model_dt = 0.1f;
  mppi::CriticData data{state, trajectories, path, goal, costs, model_dt};
  return mppi::utils::findPathFurthestReachedPoint(data);
}

}  // namespace

TEST(OfficialFurthestPathPoint, CompactUTurnCannotAliasReturnLeg)
{
  const auto path = compactUTurnPath();
  const size_t furthest = furthestReachedOnCompactUTurn(false);
  ASSERT_LT(furthest, static_cast<size_t>(path.x.size()));

  float reachable_arc = 0.0f;
  for (size_t index = 1; index <= furthest; ++index) {
    reachable_arc += std::hypot(
      path.x(index) - path.x(index - 1),
      path.y(index) - path.y(index - 1));
  }
  // lower_bound may include one path interval beyond the 0.60 m rollout.
  EXPECT_LE(reachable_arc, 0.651f);
  EXPECT_LT(furthest, static_cast<size_t>(path.x.size() - 1));
}

TEST(OfficialFurthestPathPoint, CandidateBatchOrderDoesNotChangeProgress)
{
  EXPECT_EQ(
    furthestReachedOnCompactUTurn(false),
    furthestReachedOnCompactUTurn(true));
}

TEST(PathOccupancy, OfficialCenterModeDoesNotInspectFootprintInterior)
{
  costmap_2d::Costmap2D costmap(100, 100, 0.05, 0.0, 0.0, 0u);
  unsigned int obstacle_x = 0u;
  unsigned int obstacle_y = 0u;
  ASSERT_TRUE(costmap.worldToMap(1.68, 1.0, obstacle_x, obstacle_y));
  costmap.setCost(obstacle_x, obstacle_y, costmap_2d::LETHAL_OBSTACLE);

  EXPECT_TRUE(mppi::utils::isPathPoseValid(
    costmap, squareFootprint(0.20), 1.50f, 1.0f, 0.0f,
    false, false));
}

TEST(PathOccupancy, FilledFootprintRejectsOffCenterLethalObstacle)
{
  costmap_2d::Costmap2D costmap(100, 100, 0.05, 0.0, 0.0, 0u);
  unsigned int obstacle_x = 0u;
  unsigned int obstacle_y = 0u;
  ASSERT_TRUE(costmap.worldToMap(1.68, 1.0, obstacle_x, obstacle_y));
  costmap.setCost(obstacle_x, obstacle_y, costmap_2d::LETHAL_OBSTACLE);

  EXPECT_FALSE(mppi::utils::isPathPoseValid(
    costmap, squareFootprint(0.20), 1.50f, 1.0f, 0.0f,
    true, false));
}

TEST(PathOccupancy, FilledFootprintDoesNotDoubleCountSoftInflation)
{
  costmap_2d::Costmap2D costmap(100, 100, 0.05, 0.0, 0.0, 0u);
  unsigned int inflated_x = 0u;
  unsigned int inflated_y = 0u;
  ASSERT_TRUE(costmap.worldToMap(1.68, 1.0, inflated_x, inflated_y));
  costmap.setCost(
    inflated_x, inflated_y, costmap_2d::INSCRIBED_INFLATED_OBSTACLE);

  EXPECT_TRUE(mppi::utils::isPathPoseValid(
    costmap, squareFootprint(0.20), 1.50f, 1.0f, 0.0f,
    true, false));
}

TEST(PathOccupancy, FilledFootprintRejectsUnknownAndMapBoundary)
{
  costmap_2d::Costmap2D costmap(40, 40, 0.05, 0.0, 0.0, 0u);
  unsigned int unknown_x = 0u;
  unsigned int unknown_y = 0u;
  ASSERT_TRUE(costmap.worldToMap(0.90, 1.0, unknown_x, unknown_y));
  costmap.setCost(unknown_x, unknown_y, costmap_2d::NO_INFORMATION);

  EXPECT_FALSE(mppi::utils::isPathPoseValid(
    costmap, squareFootprint(0.20), 1.0f, 1.0f, 0.0f,
    true, true));
  EXPECT_FALSE(mppi::utils::isPathPoseValid(
    costmap, squareFootprint(0.20), 1.92f, 1.0f, 0.0f,
    true, true));
}

TEST(OfficialPathWindow, KeepsOnlyOneTerminalPredecessor)
{
  std::vector<geometry_msgs::PoseStamped> path;
  for (int index = 0; index < 11; ++index) {
    path.push_back(stampedPose(0.1 * index, 0.0));
  }
  const auto closest = mppi::findClosestPathPose(
    path.begin(), path.end(), stampedPose(1.0, 0.0), 10.0);
  ASSERT_NE(closest, path.end());
  EXPECT_EQ(std::distance(path.begin(), closest), 9);
}

TEST(OfficialPathWindow, DoesNotReplayFixedHistoricalTail)
{
  std::vector<geometry_msgs::PoseStamped> path;
  for (int index = 0; index < 21; ++index) {
    path.push_back(stampedPose(0.05 * index, 0.0));
  }
  const auto closest = mppi::findClosestPathPose(
    path.begin(), path.end(), stampedPose(0.76, 0.01), 10.0);
  ASSERT_NE(closest, path.end());
  EXPECT_GE(std::distance(path.begin(), closest), 14);
  EXPECT_LE(std::distance(path.begin(), closest), 16);
}

TEST(OfficialPathWindow, ZeroSearchWindowKeepsCurrentProgress)
{
  std::vector<geometry_msgs::PoseStamped> path{
    stampedPose(0.0, 0.0), stampedPose(0.1, 0.0)};
  EXPECT_EQ(
    mppi::findClosestPathPose(
      path.begin(), path.end(), stampedPose(0.1, 0.0), 0.0),
    path.begin());
}

TEST(OfficialPathWindow, SmallSearchDistanceDoesNotReplaySparsePath)
{
  std::vector<geometry_msgs::PoseStamped> path{
    stampedPose(0.0, 0.0), stampedPose(1.0, 0.0), stampedPose(2.0, 0.0)};
  const auto closest = mppi::findClosestPathPose(
    path.begin(), path.end(), stampedPose(0.9, 0.0), 0.5);
  EXPECT_EQ(closest, path.begin());
}

TEST(PathHandlerTf, TransformsKnownFrameAndRejectsMissingFrame)
{
  auto buffer = std::make_shared<tf2_ros::Buffer>();
  buffer->setUsingDedicatedThread(true);
  geometry_msgs::TransformStamped map_from_odom;
  map_from_odom.header.frame_id = "map";
  map_from_odom.child_frame_id = "odom";
  map_from_odom.transform.translation.x = 1.0;
  map_from_odom.transform.translation.y = 2.0;
  map_from_odom.transform.rotation.w = 1.0;
  ASSERT_TRUE(buffer->setTransform(map_from_odom, "unit_test", true));

  TestPathHandler handler;
  handler.configureTransformBuffer(buffer, 0.01);
  geometry_msgs::PoseStamped input = stampedPose(0.5, 0.25);
  input.header.frame_id = "odom";
  geometry_msgs::PoseStamped output;
  ASSERT_TRUE(handler.transform("map", input, output));
  EXPECT_EQ(output.header.frame_id, "map");
  EXPECT_NEAR(output.pose.position.x, 1.5, 1e-9);
  EXPECT_NEAR(output.pose.position.y, 2.25, 1e-9);

  input.header.frame_id = "missing";
  EXPECT_FALSE(handler.transform("map", input, output));
}

TEST(TrajectoryValidatorPlugin, DefaultPluginLoads)
{
  pluginlib::ClassLoader<mppi::OptimalTrajectoryValidator> loader(
    "mppi_controller", "mppi::OptimalTrajectoryValidator");
  std::unique_ptr<mppi::OptimalTrajectoryValidator> validator(
    loader.createUnmanagedInstance(
      "mppi::DefaultOptimalTrajectoryValidator"));
  ASSERT_NE(validator, nullptr);
  validator->initialize(ros::NodeHandle("~validator_test"), "TrajectoryValidator");

  costmap_2d::Costmap2D costmap(40, 40, 0.05, 0.0, 0.0, 0u);
  geometry_msgs::Pose initial;
  initial.position.x = 1.0;
  initial.position.y = 1.0;
  initial.orientation.w = 1.0;
  EXPECT_EQ(validator->validate(
    costmap, squareFootprint(0.20), initial,
    straightTrajectory(1.05, 1.10, 1.0)), mppi::ValidationResult::SUCCESS);
}

TEST(TrajectoryValidatorPlugin, DisabledGatePublishesSoftmaxResultDirectly)
{
  TestOptimizer optimizer;
  EXPECT_EQ(
    optimizer.validateWithFinalGateDisabled(),
    mppi::ValidationResult::SUCCESS);
}

TEST(FallbackState, ClearsFailureAndPerAttemptCriticCaches)
{
  TestOptimizer optimizer;
  optimizer.configureFallbackTest();
  optimizer.markCriticStateFailed();
  ASSERT_TRUE(optimizer.criticFailed());
  ASSERT_TRUE(optimizer.hasFurthestPoint());
  ASSERT_TRUE(optimizer.hasPathValidityCache());
  EXPECT_TRUE(optimizer.invokeFallback(true));
  EXPECT_FALSE(optimizer.criticFailed());
  EXPECT_FALSE(optimizer.hasFurthestPoint());
  EXPECT_FALSE(optimizer.hasPathValidityCache());
}

TEST(SpeedLimit, ScalesAndRestoresBaseConstraints)
{
  TestOptimizer optimizer;
  optimizer.configureSpeedLimitTest();
  optimizer.setSpeedLimit(50.0, true);
  EXPECT_TRUE(optimizer.isSpeedLimitActive());
  EXPECT_NEAR(optimizer.vxMaximum(), 0.20f, 1e-6f);
  EXPECT_NEAR(optimizer.wzMaximum(), 0.40f, 1e-6f);

  optimizer.resetPreservingSpeedLimit();
  EXPECT_TRUE(optimizer.isSpeedLimitActive());
  EXPECT_NEAR(optimizer.vxMaximum(), 0.20f, 1e-6f);
  EXPECT_NEAR(optimizer.wzMaximum(), 0.40f, 1e-6f);

  optimizer.setSpeedLimit(0.10, false);
  EXPECT_NEAR(optimizer.vxMaximum(), 0.10f, 1e-6f);
  EXPECT_NEAR(optimizer.wzMaximum(), 0.20f, 1e-6f);

  optimizer.setSpeedLimit(-1.0, false);
  EXPECT_FALSE(optimizer.isSpeedLimitActive());
  EXPECT_NEAR(optimizer.vxMaximum(), 0.40f, 1e-6f);
  EXPECT_NEAR(optimizer.wzMaximum(), 0.80f, 1e-6f);
}

TEST(GoalReachedEvaluator, RequiresPoseVelocityAndStableHold)
{
  mppi::GoalReachedEvaluator evaluator;
  evaluator.configure(0.4, 0.3, 0.02, 0.03, 0.3);
  EXPECT_FALSE(evaluator.update(0.2, 0.1, 0.10, 0.0, 1.0));
  EXPECT_FALSE(evaluator.update(0.2, 0.1, 0.0, 0.0, 1.1));
  EXPECT_FALSE(evaluator.update(0.2, 0.1, 0.0, 0.0, 1.39));
  EXPECT_TRUE(evaluator.update(0.2, 0.1, 0.0, 0.0, 1.41));
}

TEST(GoalReachedEvaluator, MbfToleranceOnlyTightensPluginPolicy)
{
  mppi::GoalReachedEvaluator evaluator;
  evaluator.configure(0.6, 0.4, 0.02, 0.03, 0.0);
  evaluator.applyToleranceUpperBounds(0.4, 0.5);
  EXPECT_DOUBLE_EQ(evaluator.activePositionTolerance(), 0.4);
  EXPECT_DOUBLE_EQ(evaluator.activeYawTolerance(), 0.4);
  EXPECT_FALSE(evaluator.update(0.5, 0.1, 0.0, 0.0, 1.0));
  EXPECT_TRUE(evaluator.update(0.3, 0.3, 0.0, 0.0, 1.1));
}

TEST(PathAngleCritic, InvalidModeFailsDuringConfiguration)
{
  EXPECT_EQ(
    mppi::critics::pathAngleModeFromInt(0),
    mppi::critics::PathAngleMode::FORWARD_PREFERENCE);
  EXPECT_EQ(
    mppi::critics::pathAngleModeFromInt(2),
    mppi::critics::PathAngleMode::CONSIDER_FEASIBLE_PATH_ORIENTATIONS);
  EXPECT_THROW(mppi::critics::pathAngleModeFromInt(-1), std::invalid_argument);
  EXPECT_THROW(mppi::critics::pathAngleModeFromInt(3), std::invalid_argument);
}

TEST(OfficialMotionModel, ClampsRolloutButPreservesRawSamples)
{
  mppi::models::ControlConstraints constraints{
    1.0f, -1.0f, 0.0f, 1.0f,
    1.0f, -2.0f, -1.0f, 1.0f, 1.0f};
  mppi::DiffDriveMotionModel model;
  model.initialize(constraints, 0.1f, 0.0f, 0.0f, 0.0f, false);
  mppi::models::State state;
  state.reset(1u, 4u);
  state.cvx.setConstant(1.0f);
  model.predict(state);
  // Upstream treats exactly zero as the non-positive branch. The first step
  // is therefore bounded by the magnitude of ax_min; later positive steps use
  // ax_max.
  EXPECT_NEAR(state.vx(0, 1), 0.2f, 1e-6f);
  EXPECT_NEAR(state.vx(0, 2), 0.3f, 1e-6f);
  EXPECT_NEAR(state.cvx(0, 0), 1.0f, 1e-6f);
}

TEST(OfficialMotionModel, OptionalRawControlClampAndReverseEnvelope)
{
  mppi::models::ControlConstraints constraints{
    1.0f, -1.0f, 0.0f, 1.0f,
    1.0f, -2.0f, -1.0f, 1.0f, 1.0f};
  mppi::DiffDriveMotionModel model;
  model.initialize(constraints, 0.1f, 0.0f, 0.0f, 0.0f, true);
  mppi::models::State state;
  state.reset(1u, 3u);
  state.vx(0, 0) = -0.5f;
  state.cvx.setConstant(-1.0f);
  model.predict(state);
  EXPECT_NEAR(state.vx(0, 1), -0.6f, 1e-6f);
  EXPECT_NEAR(state.cvx(0, 0), -0.6f, 1e-6f);
}

TEST(CriticCostStatistics, DetectsContributionsEvenWhenTheirSumCancels)
{
  Eigen::ArrayXf before(3);
  Eigen::ArrayXf after(3);
  before << 1.0f, 2.0f, 3.0f;
  after << 2.0f, 1.0f, 3.0f;

  mppi::CriticCostAccumulator accumulator;
  accumulator.add(before, after, 2.5);

  EXPECT_EQ(accumulator.evaluations, 1u);
  EXPECT_EQ(accumulator.active_evaluations, 1u);
  EXPECT_EQ(accumulator.samples, 3u);
  EXPECT_EQ(accumulator.changed_samples, 2u);
  EXPECT_DOUBLE_EQ(accumulator.cost_sum, 0.0);
  EXPECT_DOUBLE_EQ(accumulator.meanCost(), 0.0);
  EXPECT_NEAR(accumulator.changedRatio(), 2.0 / 3.0, 1e-9);
  EXPECT_FLOAT_EQ(accumulator.minimumCost(), -1.0f);
  EXPECT_FLOAT_EQ(accumulator.maximumCost(), 1.0f);
  EXPECT_DOUBLE_EQ(accumulator.meanElapsedMs(), 2.5);
  EXPECT_FALSE(accumulator.nonfinite);
}

TEST(CriticCostStatistics, AccumulatesWindowsAndResetsWithoutStaleValues)
{
  Eigen::ArrayXf before = Eigen::ArrayXf::Zero(2);
  Eigen::ArrayXf first(2);
  Eigen::ArrayXf second(2);
  first << 1.0f, 2.0f;
  second << 0.0f, 0.0f;

  mppi::CriticCostAccumulator accumulator;
  accumulator.add(before, first, 1.0);
  accumulator.add(before, second, 3.0);
  EXPECT_EQ(accumulator.evaluations, 2u);
  EXPECT_EQ(accumulator.active_evaluations, 1u);
  EXPECT_EQ(accumulator.samples, 4u);
  EXPECT_DOUBLE_EQ(accumulator.cost_sum, 3.0);
  EXPECT_DOUBLE_EQ(accumulator.meanCost(), 0.75);
  EXPECT_DOUBLE_EQ(accumulator.meanElapsedMs(), 2.0);

  accumulator.reset();
  EXPECT_EQ(accumulator.evaluations, 0u);
  EXPECT_EQ(accumulator.samples, 0u);
  EXPECT_DOUBLE_EQ(accumulator.meanCost(), 0.0);
  EXPECT_FLOAT_EQ(accumulator.minimumCost(), 0.0f);
  EXPECT_FLOAT_EQ(accumulator.maximumCost(), 0.0f);
}

TEST(OptimalTrajectoryValidator, AcceptsFreeContinuousMotion)
{
  costmap_2d::Costmap2D costmap(100, 100, 0.05, 0.0, 0.0, 0u);
  mppi::DefaultOptimalTrajectoryValidator validator;
  EXPECT_EQ(validator.validate(
    costmap, squareFootprint(), stampedPose(1.0, 1.0).pose,
    straightTrajectory(1.5, 2.0, 1.0)), mppi::ValidationResult::SUCCESS);
}

TEST(OptimalTrajectoryValidator, DetectsObstacleBetweenModelSamples)
{
  costmap_2d::Costmap2D costmap(100, 100, 0.05, 0.0, 0.0, 0u);
  unsigned int obstacle_x = 0u;
  unsigned int obstacle_y = 0u;
  ASSERT_TRUE(costmap.worldToMap(1.25, 1.0, obstacle_x, obstacle_y));
  costmap.setCost(obstacle_x, obstacle_y, costmap_2d::LETHAL_OBSTACLE);
  mppi::DefaultOptimalTrajectoryValidator validator;
  EXPECT_EQ(validator.validate(
    costmap, squareFootprint(0.10), stampedPose(1.0, 1.0).pose,
    straightTrajectory(1.5, 2.0, 1.0)), mppi::ValidationResult::SOFT_RESET);
}

TEST(OptimalTrajectoryValidator, ChecksFilledFootprintInterior)
{
  costmap_2d::Costmap2D costmap(100, 100, 0.05, 0.0, 0.0, 0u);
  unsigned int obstacle_x = 0u;
  unsigned int obstacle_y = 0u;
  ASSERT_TRUE(costmap.worldToMap(1.65, 1.0, obstacle_x, obstacle_y));
  costmap.setCost(obstacle_x, obstacle_y, costmap_2d::LETHAL_OBSTACLE);
  mppi::DefaultOptimalTrajectoryValidator validator;
  Eigen::ArrayXXf stationary(1, 3);
  stationary << 1.50f, 1.0f, 0.0f;
  EXPECT_EQ(validator.validate(
    costmap, squareFootprint(0.20), stampedPose(1.50, 1.0).pose, stationary),
    mppi::ValidationResult::SOFT_RESET);
}

TEST(OptimalTrajectoryValidator, AllowsInscribedInflationInsideFootprint)
{
  costmap_2d::Costmap2D costmap(100, 100, 0.05, 0.0, 0.0, 0u);
  unsigned int inflated_x = 0u;
  unsigned int inflated_y = 0u;
  ASSERT_TRUE(costmap.worldToMap(1.65, 1.0, inflated_x, inflated_y));
  costmap.setCost(
    inflated_x, inflated_y, costmap_2d::INSCRIBED_INFLATED_OBSTACLE);

  mppi::DefaultOptimalTrajectoryValidator validator;
  Eigen::ArrayXXf stationary(1, 3);
  stationary << 1.50f, 1.0f, 0.0f;
  EXPECT_EQ(validator.validate(
    costmap, squareFootprint(0.20), stampedPose(1.50, 1.0).pose, stationary),
    mppi::ValidationResult::SUCCESS);
}

TEST(CostCriticFootprintCollision, InscribedCenterIsSoftWhenFilledBodyIsHardCellFree)
{
  costmap_2d::Costmap2D costmap(100, 100, 0.05, 0.0, 0.0, 0u);
  unsigned int center_x = 0u;
  unsigned int center_y = 0u;
  ASSERT_TRUE(costmap.worldToMap(1.50, 1.0, center_x, center_y));
  costmap.setCost(
    center_x, center_y, costmap_2d::INSCRIBED_INFLATED_OBSTACLE);

  TestCostCritic critic;
  critic.configureFootprintMode(true);
  EXPECT_FALSE(critic.collides(
    costmap_2d::INSCRIBED_INFLATED_OBSTACLE,
    costmap, squareFootprint(0.20)));

  critic.configureFootprintMode(false);
  EXPECT_TRUE(critic.collides(
    costmap_2d::INSCRIBED_INFLATED_OBSTACLE,
    costmap, squareFootprint(0.20)));
}

TEST(CostCriticFootprintCollision, FilledBodyRejectsOffCenterHardCells)
{
  costmap_2d::Costmap2D costmap(100, 100, 0.05, 0.0, 0.0, 0u);
  unsigned int obstacle_x = 0u;
  unsigned int obstacle_y = 0u;
  ASSERT_TRUE(costmap.worldToMap(1.65, 1.0, obstacle_x, obstacle_y));

  TestCostCritic critic;
  critic.configureFootprintMode(true);
  costmap.setCost(obstacle_x, obstacle_y, costmap_2d::LETHAL_OBSTACLE);
  EXPECT_TRUE(critic.collides(100.0f, costmap, squareFootprint(0.20)));

  costmap.setCost(obstacle_x, obstacle_y, costmap_2d::NO_INFORMATION);
  EXPECT_TRUE(critic.collides(100.0f, costmap, squareFootprint(0.20)));

  critic.configureFootprintMode(true, true);
  EXPECT_FALSE(critic.collides(100.0f, costmap, squareFootprint(0.20)));
}

TEST(CostCriticFootprintCollision, MapBoundaryFailsClosed)
{
  costmap_2d::Costmap2D costmap(40, 40, 0.05, 0.0, 0.0, 0u);
  TestCostCritic critic;
  critic.configureFootprintMode(true);
  EXPECT_TRUE(critic.collides(
    100.0f, costmap, squareFootprint(0.20), 1.92f, 1.0f));
}

TEST(OptimalTrajectoryValidator, AllowsInscribedInflationAtRobotCenter)
{
  costmap_2d::Costmap2D costmap(100, 100, 0.05, 0.0, 0.0, 0u);
  unsigned int center_x = 0u;
  unsigned int center_y = 0u;
  ASSERT_TRUE(costmap.worldToMap(1.50, 1.0, center_x, center_y));
  costmap.setCost(
    center_x, center_y, costmap_2d::INSCRIBED_INFLATED_OBSTACLE);

  mppi::DefaultOptimalTrajectoryValidator validator;
  Eigen::ArrayXXf stationary(1, 3);
  stationary << 1.50f, 1.0f, 0.0f;
  EXPECT_EQ(validator.validate(
    costmap, squareFootprint(0.20), stampedPose(1.50, 1.0).pose, stationary),
    mppi::ValidationResult::SUCCESS);
}

TEST(OptimalTrajectoryValidator, UnknownAndMapBoundaryFailClosed)
{
  costmap_2d::Costmap2D costmap(40, 40, 0.05, 0.0, 0.0, 0u);
  unsigned int unknown_x = 0u;
  unsigned int unknown_y = 0u;
  ASSERT_TRUE(costmap.worldToMap(0.75, 0.75, unknown_x, unknown_y));
  costmap.setCost(unknown_x, unknown_y, costmap_2d::NO_INFORMATION);
  mppi::DefaultOptimalTrajectoryValidator validator;
  Eigen::ArrayXXf unknown_pose(1, 3);
  unknown_pose << 0.75f, 0.75f, 0.0f;
  EXPECT_EQ(validator.validate(
    costmap, squareFootprint(0.10), stampedPose(0.75, 0.75).pose,
    unknown_pose), mppi::ValidationResult::SOFT_RESET);

  Eigen::ArrayXXf outside(1, 3);
  outside << 1.98f, 1.98f, 0.0f;
  EXPECT_EQ(validator.validate(
    costmap, squareFootprint(0.20), stampedPose(1.70, 1.70).pose, outside),
    mppi::ValidationResult::SOFT_RESET);
}

TEST(OptimalTrajectoryValidator, PartiallyClippedNonemptyFootprintFailsClosed)
{
  costmap_2d::Costmap2D costmap(40, 40, 0.05, 0.0, 0.0, 0u);
  mppi::DefaultOptimalTrajectoryValidator validator;
  Eigen::ArrayXXf partially_outside(1, 3);
  // With this footprint ordering the left edge is rasterized before a right
  // vertex leaves the 2 m map, so ROS1 FootprintHelper returns non-empty cells.
  partially_outside << 1.85f, 1.0f, 0.0f;
  EXPECT_EQ(validator.validate(
    costmap, squareFootprint(0.20), stampedPose(1.85, 1.0).pose,
    partially_outside), mppi::ValidationResult::SOFT_RESET);
}

TEST(OptimalTrajectoryValidator, SamplesRotationalCornerSweep)
{
  costmap_2d::Costmap2D costmap(100, 100, 0.05, 0.0, 0.0, 0u);
  unsigned int obstacle_x = 0u;
  unsigned int obstacle_y = 0u;
  ASSERT_TRUE(costmap.worldToMap(1.30, 1.30, obstacle_x, obstacle_y));
  costmap.setCost(obstacle_x, obstacle_y, costmap_2d::LETHAL_OBSTACLE);
  mppi::DefaultOptimalTrajectoryValidator validator;
  Eigen::ArrayXXf rotate(1, 3);
  rotate << 1.0f, 1.0f, static_cast<float>(M_PI_2);
  EXPECT_EQ(validator.validate(
    costmap, squareFootprint(0.35), stampedPose(1.0, 1.0, 0.0).pose, rotate),
    mppi::ValidationResult::SOFT_RESET);
}

TEST(OptimalTrajectoryValidator, StructuralInputFailureIsNotRetried)
{
  costmap_2d::Costmap2D costmap(40, 40, 0.05, 0.0, 0.0, 0u);
  mppi::DefaultOptimalTrajectoryValidator validator;
  Eigen::ArrayXXf malformed(1, 2);
  malformed << 1.0f, 1.0f;
  EXPECT_EQ(validator.validate(
    costmap, squareFootprint(0.20), stampedPose(1.0, 1.0).pose, malformed),
    mppi::ValidationResult::FAILURE);
}

int main(int argc, char ** argv)
{
  ros::init(
    argc, argv, "test_official_mppi_core",
    ros::init_options::AnonymousName | ros::init_options::NoSigintHandler);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

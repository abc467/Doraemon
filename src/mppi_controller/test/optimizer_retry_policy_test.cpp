#include <gtest/gtest.h>

#include <memory>
#include <stdexcept>
#include <vector>

#include <ros/ros.h>

#include "mppi_controller/optimizer.hpp"

namespace mppi
{
namespace
{

class RetryPolicyOptimizer : public Optimizer
{
public:
  void initializeForTest(const int retry_limit)
  {
    settings_.batch_size = 4;
    settings_.time_steps = 3;
    settings_.retry_attempt_limit = retry_limit;
    settings_.sampling_std = {0.1f, 0.0f, 0.1f};
    motion_model_ = std::make_shared<DiffDriveMotionModel>();
    noise_generator_.initialize(settings_, false);
  }

  void markFailedBatch()
  {
    critics_data_.fail_flag = true;
    critics_data_.furthest_reached_path_point = 2u;
    critics_data_.path_pts_valid = std::vector<bool>{true, false, true};
  }

  bool invokeFallback(const bool failed)
  {
    return fallback(failed);
  }

  bool failFlag() const
  {
    return critics_data_.fail_flag;
  }

  bool hasTrajectoryCaches() const
  {
    return critics_data_.furthest_reached_path_point.has_value() ||
           critics_data_.path_pts_valid.has_value();
  }
};

TEST(OptimizerRetryPolicy, FreshRetryClearsFailureAndTrajectoryCaches)
{
  RetryPolicyOptimizer optimizer;
  optimizer.initializeForTest(2);
  optimizer.markFailedBatch();

  EXPECT_TRUE(optimizer.invokeFallback(true));
  EXPECT_FALSE(optimizer.failFlag());
  EXPECT_FALSE(optimizer.hasTrajectoryCaches());
}

TEST(OptimizerRetryPolicy, ExhaustionThrowsInsteadOfReturningZeroControlAsSuccess)
{
  RetryPolicyOptimizer optimizer;
  optimizer.initializeForTest(2);

  optimizer.markFailedBatch();
  EXPECT_TRUE(optimizer.invokeFallback(true));
  optimizer.markFailedBatch();
  EXPECT_TRUE(optimizer.invokeFallback(true));
  optimizer.markFailedBatch();
  EXPECT_THROW(optimizer.invokeFallback(true), std::runtime_error);

  // Exhaustion starts the next control tick with a fresh retry budget.
  optimizer.markFailedBatch();
  EXPECT_TRUE(optimizer.invokeFallback(true));
}

TEST(OptimizerRetryPolicy, SuccessfulBatchResetsRetryBudget)
{
  RetryPolicyOptimizer optimizer;
  optimizer.initializeForTest(1);

  optimizer.markFailedBatch();
  EXPECT_TRUE(optimizer.invokeFallback(true));
  EXPECT_FALSE(optimizer.invokeFallback(false));

  optimizer.markFailedBatch();
  EXPECT_TRUE(optimizer.invokeFallback(true));
}

}  // namespace
}  // namespace mppi

int main(int argc, char ** argv)
{
  ros::init(
    argc, argv, "optimizer_retry_policy_test",
    ros::init_options::AnonymousName |
    ros::init_options::NoSigintHandler |
    ros::init_options::NoRosout);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

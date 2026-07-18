#include "cartographer/mapping/internal/2d/flirt_relocation.h"

#include <memory>

#include "Eigen/Geometry"
#include "cartographer/common/time.h"
#include "cartographer/transform/rigid_transform_test_helpers.h"
#include "cartographer/transform/transform.h"
#include "gtest/gtest.h"

namespace cartographer {
namespace mapping {
namespace {

constexpr double kPi = 3.14159265358979323846;

TrajectoryNode::Data MakeReferenceData(
    const transform::Rigid2d& trajectory_from_reference,
    const Eigen::Quaterniond& gravity_alignment) {
  return TrajectoryNode::Data{
      common::FromUniversal(42), gravity_alignment, {}, {}, {}, {},
      transform::Embed3D(trajectory_from_reference) *
          transform::Rigid3d::Rotation(gravity_alignment)};
}

class FlirtRelocationHeadingTest
    : public ::testing::TestWithParam<double> {};

TEST_P(FlirtRelocationHeadingTest,
       ComposesReferenceFromQueryInGravityAlignedLocalFrame) {
  const transform::Rigid2d trajectory_from_reference({2.75, -1.5},
                                                      GetParam());
  const Eigen::Quaterniond gravity_alignment(
      Eigen::AngleAxisd(0.23, Eigen::Vector3d::UnitZ()));
  const auto reference_data =
      MakeReferenceData(trajectory_from_reference, gravity_alignment);
  const transform::Rigid2d reference_from_query({0.6, -0.35}, -0.31);

  EXPECT_THAT(
      ComputeFlirtQueryPoseInTrajectory(reference_data, reference_from_query),
      transform::IsNearly(trajectory_from_reference * reference_from_query,
                          1e-9));
}

INSTANTIATE_TEST_SUITE_P(ReferenceHeadings, FlirtRelocationHeadingTest,
                         ::testing::Values(0.0, 0.5 * kPi, -0.5 * kPi, kPi));

TEST(FlirtRelocationTest, OptimizedGlobalPoseDoesNotParticipate) {
  const transform::Rigid2d trajectory_from_reference({4.0, 3.0}, -0.7);
  auto reference_data = std::make_shared<const TrajectoryNode::Data>(
      MakeReferenceData(trajectory_from_reference,
                        Eigen::Quaterniond::Identity()));
  const TrajectoryNode before_optimization{
      reference_data, transform::Rigid3d::Identity()};
  const TrajectoryNode after_optimization{
      reference_data,
      transform::Rigid3d(
          {100.0, -50.0, 0.0},
          Eigen::Quaterniond(Eigen::AngleAxisd(2.1,
                                               Eigen::Vector3d::UnitZ())))};
  const transform::Rigid2d reference_from_query({-0.4, 0.2}, 0.9);

  const auto pose_before = ComputeFlirtQueryPoseInTrajectory(
      *before_optimization.constant_data, reference_from_query);
  const auto pose_after = ComputeFlirtQueryPoseInTrajectory(
      *after_optimization.constant_data, reference_from_query);
  EXPECT_THAT(pose_after, transform::IsNearly(pose_before, 1e-12));
}

}  // namespace
}  // namespace mapping
}  // namespace cartographer

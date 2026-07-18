/*
 * Copyright 2017 The Cartographer Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "cartographer/mapping/trajectory_node.h"

#include <limits>
#include <memory>
#include <utility>

#include "Eigen/Core"
#include "cartographer/common/time.h"
#include "cartographer/mapping/proto/trajectory_node_data.pb.h"
#include "cartographer/transform/rigid_transform_test_helpers.h"
#include "gtest/gtest.h"

namespace cartographer {
namespace mapping {
namespace {

std::unique_ptr<InterestPoint> MakeInterestPoint() {
  auto descriptor = std::make_unique<ShapeContext>();
  descriptor->getHistogram() = {{1., 2.}, {3., 4.}};
  descriptor->setDistanceFunction(flirt::get_distance_function());

  auto point = std::make_unique<InterestPoint>();
  point->setPosition({1.25, -2.5, 0.75});
  point->setSupport({{0.1, 0.2}, {0.3, 0.4}});
  point->setScale(1.5);
  point->setScaleLevel(3);
  point->setDescriptor(descriptor.get());
  return point;
}

TEST(TrajectoryNodeTest, ToAndFromProto) {
  const TrajectoryNode::Data expected{
      common::FromUniversal(42),
      Eigen::Quaterniond(1., 2., -3., -4.),
      sensor::CompressedPointCloud(
          sensor::PointCloud({{Eigen::Vector3f{1.f, 2.f, 0.f}},
                              {Eigen::Vector3f{0.f, 0.f, 1.f}}}))
          .Decompress(),
      sensor::CompressedPointCloud(
          sensor::PointCloud({{Eigen::Vector3f{2.f, 3.f, 4.f}}}))
          .Decompress(),
      sensor::CompressedPointCloud(
          sensor::PointCloud({{Eigen::Vector3f{-1.f, 2.f, 0.f}}}))
          .Decompress(),
      Eigen::VectorXf::Unit(20, 4),
      transform::Rigid3d({1., 2., 3.},
                         Eigen::Quaterniond(4., 5., -6., -7.).normalized())};
  flirt::FeatureSet::OwnedPoints expected_points;
  expected_points.emplace_back(MakeInterestPoint());
  expected.SetFlirtFeatures(
      flirt::AdoptFeatureSet(std::move(expected_points)));

  const proto::TrajectoryNodeData proto = ToProto(expected);
  EXPECT_EQ(proto.interest_point_frame(),
            proto::TrajectoryNodeData::NODE_GRAVITY_ALIGNED);
  EXPECT_EQ(proto.interest_points_size(), 0);
  ASSERT_EQ(proto.gravity_aligned_interest_points_size(), 1);

  const TrajectoryNode::Data actual = FromProto(proto);
  EXPECT_EQ(expected.time, actual.time);
  EXPECT_TRUE(actual.gravity_alignment.isApprox(expected.gravity_alignment));
  EXPECT_EQ(expected.filtered_gravity_aligned_point_cloud.points(),
            actual.filtered_gravity_aligned_point_cloud.points());
  EXPECT_EQ(expected.high_resolution_point_cloud.points(),
            actual.high_resolution_point_cloud.points());
  EXPECT_EQ(expected.low_resolution_point_cloud.points(),
            actual.low_resolution_point_cloud.points());
  EXPECT_EQ(expected.rotational_scan_matcher_histogram,
            actual.rotational_scan_matcher_histogram);
  EXPECT_THAT(actual.local_pose,
              transform::IsNearly(expected.local_pose, 1e-9));

  const auto actual_features = actual.GetFlirtFeatures();
  ASSERT_NE(actual_features, nullptr);
  ASSERT_EQ(actual_features->size(), 1);
  const auto& actual_point = actual_features->at(0);
  EXPECT_DOUBLE_EQ(actual_point.getPosition().x, 1.25);
  EXPECT_DOUBLE_EQ(actual_point.getPosition().y, -2.5);
  EXPECT_DOUBLE_EQ(actual_point.getPosition().theta, 0.75);
  EXPECT_DOUBLE_EQ(actual_point.getScale(), 1.5);
  EXPECT_DOUBLE_EQ(actual_point.getScaleLevel(), 3.);
  ASSERT_EQ(actual_point.getSupport().size(), 2);
  const auto* actual_descriptor =
      dynamic_cast<const ShapeContext*>(actual_point.getDescriptor());
  ASSERT_NE(actual_descriptor, nullptr);
  EXPECT_EQ(actual_descriptor->getHistogram(),
            (std::vector<std::vector<double>>{{1., 2.}, {3., 4.}}));
}

TEST(TrajectoryNodeTest, LegacyFlirtFeaturesAreInvalidated) {
  TrajectoryNode::Data data{
      common::FromUniversal(42), Eigen::Quaterniond::Identity(), {}, {}, {},
      Eigen::VectorXf(), transform::Rigid3d::Identity()};
  auto proto = ToProto(data);
  proto.add_interest_points();

  EXPECT_EQ(proto.interest_point_frame(),
            proto::TrajectoryNodeData::UNSPECIFIED);
  EXPECT_EQ(FromProto(proto).GetFlirtFeatures(), nullptr);
}

TEST(TrajectoryNodeTest, UnknownFlirtFeatureFrameIsInvalidated) {
  TrajectoryNode::Data data{
      common::FromUniversal(42), Eigen::Quaterniond::Identity(), {}, {}, {},
      Eigen::VectorXf(), transform::Rigid3d::Identity()};
  auto proto = ToProto(data);
  proto.add_interest_points();
  proto.set_interest_point_frame(
      static_cast<proto::TrajectoryNodeData::InterestPointFrame>(123));

  EXPECT_EQ(FromProto(proto).GetFlirtFeatures(), nullptr);
}

TEST(TrajectoryNodeTest, EmptyComputedFeatureSetSurvivesRoundTrip) {
  TrajectoryNode::Data data{
      common::FromUniversal(42), Eigen::Quaterniond::Identity(), {}, {}, {},
      Eigen::VectorXf(), transform::Rigid3d::Identity()};
  data.SetFlirtFeatures(flirt::AdoptFeatureSet({}));

  const auto proto = ToProto(data);
  EXPECT_EQ(proto.interest_point_frame(),
            proto::TrajectoryNodeData::NODE_GRAVITY_ALIGNED);
  EXPECT_EQ(proto.interest_points_size(), 0);
  EXPECT_EQ(proto.gravity_aligned_interest_points_size(), 0);

  const auto round_trip = FromProto(proto).GetFlirtFeatures();
  ASSERT_NE(round_trip, nullptr);
  EXPECT_TRUE(round_trip->empty());
}

TEST(TrajectoryNodeTest, DataCopiesShareImmutableFeaturesSafely) {
  TrajectoryNode::Data original{
      common::FromUniversal(42), Eigen::Quaterniond::Identity(), {}, {}, {},
      Eigen::VectorXf(), transform::Rigid3d::Identity()};
  flirt::FeatureSet::OwnedPoints points;
  points.emplace_back(MakeInterestPoint());
  original.SetFlirtFeatures(flirt::AdoptFeatureSet(std::move(points)));

  TrajectoryNode::Data copy = original;
  EXPECT_EQ(copy.GetFlirtFeatures(), original.GetFlirtFeatures());

  copy.SetFlirtFeatures(flirt::AdoptFeatureSet({}));
  ASSERT_NE(copy.GetFlirtFeatures(), nullptr);
  EXPECT_TRUE(copy.GetFlirtFeatures()->empty());
  ASSERT_NE(original.GetFlirtFeatures(), nullptr);
  EXPECT_EQ(original.GetFlirtFeatures()->size(), 1);
}

TEST(TrajectoryNodeTest, TaggedTransitionTag8FeaturesRemainReadable) {
  TrajectoryNode::Data data{
      common::FromUniversal(42), Eigen::Quaterniond::Identity(), {}, {}, {},
      Eigen::VectorXf(), transform::Rigid3d::Identity()};
  flirt::FeatureSet::OwnedPoints points;
  points.emplace_back(MakeInterestPoint());
  data.SetFlirtFeatures(flirt::AdoptFeatureSet(std::move(points)));

  auto proto = ToProto(data);
  ASSERT_EQ(proto.gravity_aligned_interest_points_size(), 1);
  *proto.add_interest_points() = proto.gravity_aligned_interest_points(0);
  proto.clear_gravity_aligned_interest_points();

  const auto transition_features = FromProto(proto).GetFlirtFeatures();
  ASSERT_NE(transition_features, nullptr);
  EXPECT_EQ(transition_features->size(), 1);
}

}  // namespace
}  // namespace mapping
}  // namespace cartographer

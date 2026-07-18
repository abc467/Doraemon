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

#include "Eigen/Core"
#include "cartographer/common/time.h"
#include "cartographer/sensor/compressed_point_cloud.h"
#include "cartographer/transform/transform.h"
#include "glog/logging.h"

namespace cartographer {
namespace mapping {

proto::TrajectoryNodeData ToProto(const TrajectoryNode::Data& constant_data) {
  proto::TrajectoryNodeData proto;
  proto.set_timestamp(common::ToUniversal(constant_data.time));
  *proto.mutable_gravity_alignment() =
      transform::ToProto(constant_data.gravity_alignment);
  *proto.mutable_filtered_gravity_aligned_point_cloud() =
      sensor::CompressedPointCloud(
          constant_data.filtered_gravity_aligned_point_cloud)
          .ToProto();
  *proto.mutable_high_resolution_point_cloud() =
      sensor::CompressedPointCloud(constant_data.high_resolution_point_cloud)
          .ToProto();
  *proto.mutable_low_resolution_point_cloud() =
      sensor::CompressedPointCloud(constant_data.low_resolution_point_cloud)
          .ToProto();
  for (Eigen::VectorXf::Index i = 0;
       i != constant_data.rotational_scan_matcher_histogram.size(); ++i) {
    proto.add_rotational_scan_matcher_histogram(
        constant_data.rotational_scan_matcher_histogram(i));
  }
  *proto.mutable_local_pose() = transform::ToProto(constant_data.local_pose);

  // FLIRT. A null FeatureSet is deliberately not serialized as the new frame:
  // it means the features have not been computed (or legacy features need to
  // be rebuilt). A non-null empty FeatureSet is serialized with the frame so
  // it remains distinguishable after a round trip.
  const auto features = constant_data.GetFlirtFeatures();
  if (features != nullptr) {
    proto.set_interest_point_frame(
        proto::TrajectoryNodeData::NODE_GRAVITY_ALIGNED);
    proto.mutable_gravity_aligned_interest_points()->Reserve(features->size());
    for (const InterestPoint* const p : features->raw()) {
      auto interest_point =
          proto.mutable_gravity_aligned_interest_points()->Add();
      // position
      auto pos = p->getPosition();
      interest_point->mutable_position()->mutable_position()->set_x(pos.x);
      interest_point->mutable_position()->mutable_position()->set_y(pos.y);
      interest_point->mutable_position()->set_theta(pos.theta);
      // support points
      interest_point->mutable_support_points()->Reserve(p->getSupport().size());
      for (const auto& sp : p->getSupport()) {
        auto serialized_support =
            interest_point->mutable_support_points()->Add();
        serialized_support->set_x(sp.x);
        serialized_support->set_y(sp.y);
      }
      // scale
      interest_point->set_scale(p->getScale());
      // scale level
      interest_point->set_scalelevel(p->getScaleLevel());
      // descriptor
      const auto* descriptor =
          dynamic_cast<const ShapeContext*>(p->getDescriptor());
      CHECK(descriptor != nullptr)
          << "Only ShapeContext FLIRT descriptors can be serialized";
      auto desc = interest_point->mutable_desc();
      // descriptor - Histogram
      desc->mutable_histogram()->Reserve(descriptor->getHistogram().size());
      for (const auto& row : descriptor->getHistogram()) {
        auto serialized_row = desc->mutable_histogram()->Add();
        for (const double value : row) {
          serialized_row->add_values(value);
        }
      }
    }
  }
  return proto;
}

TrajectoryNode::Data FromProto(const proto::TrajectoryNodeData& proto) {
  Eigen::VectorXf rotational_scan_matcher_histogram(
      proto.rotational_scan_matcher_histogram_size());
  for (int i = 0; i != proto.rotational_scan_matcher_histogram_size(); ++i) {
    rotational_scan_matcher_histogram(i) =
        proto.rotational_scan_matcher_histogram(i);
  }
  // Legacy streams have no frame tag. Their interest points were generated in
  // ambiguous/global coordinates and must not be reused. Only the explicitly
  // tagged gravity-aligned representation is loaded.
  std::shared_ptr<const flirt::FeatureSet> flirt_features;
  if (proto.interest_point_frame() ==
      proto::TrajectoryNodeData::NODE_GRAVITY_ALIGNED) {
    // New writers use tag 10. Reading tagged tag-8 data is retained only for
    // PBStreams produced by the short-lived, not-deployed transition format.
    const auto& serialized_interest_points =
        proto.gravity_aligned_interest_points_size() > 0
            ? proto.gravity_aligned_interest_points()
            : proto.interest_points();
    flirt::FeatureSet::OwnedPoints interest_points;
    interest_points.reserve(serialized_interest_points.size());
    for (const auto& pp : serialized_interest_points) {
      auto p = std::make_unique<InterestPoint>();

      // Position
      p->setPosition({
          pp.position().position().x(),
          pp.position().position().y(),
          pp.position().theta(),
      });
      // support point
      std::vector<Point2D> support_points;
      support_points.reserve(pp.support_points().size());
      for (const auto& sp : pp.support_points()) {
        support_points.push_back({sp.x(), sp.y()});
      }
      p->setSupport(support_points);
      // scale
      p->setScale(pp.scale());
      // scale level
      p->setScaleLevel(pp.scalelevel());
      // descriptor
      auto desc = std::make_unique<ShapeContext>();
      // descriptor - histogram
      auto& histogram = desc->getHistogram();
      histogram.reserve(pp.desc().histogram().size());
      for (const auto& j : pp.desc().histogram()) {
        std::vector<double> v;
        v.reserve(j.values().size());
        for (const auto& m : j.values()) {
          v.emplace_back(m);
        }
        histogram.emplace_back(std::move(v));
      }
      // descriptor - function
      desc->setDistanceFunction(flirt::get_distance_function());
      p->setDescriptor(desc.get());
      interest_points.emplace_back(std::move(p));
    }
    flirt_features = flirt::AdoptFeatureSet(std::move(interest_points));
  }

  return TrajectoryNode::Data{
      common::FromUniversal(proto.timestamp()),
      transform::ToEigen(proto.gravity_alignment()),
      sensor::CompressedPointCloud(proto.filtered_gravity_aligned_point_cloud())
          .Decompress(),
      sensor::CompressedPointCloud(proto.high_resolution_point_cloud())
          .Decompress(),
      sensor::CompressedPointCloud(proto.low_resolution_point_cloud())
          .Decompress(),
      rotational_scan_matcher_histogram,
      transform::ToRigid3(proto.local_pose()),
      std::move(flirt_features)};
}

}  // namespace mapping
}  // namespace cartographer

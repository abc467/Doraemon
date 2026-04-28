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

  // FLIRT
  proto.mutable_interest_points()->Reserve(
      constant_data.interest_points.size());
  for (std::size_t i = 0; i < constant_data.interest_points.size(); i++) {
    const auto& p = constant_data.interest_points.at(i);
    auto interest_point = proto.mutable_interest_points()->Add();
    // position
    auto pos = p->getPosition();
    interest_point->mutable_position()->mutable_position()->set_x(pos.x);
    interest_point->mutable_position()->mutable_position()->set_y(pos.y);
    interest_point->mutable_position()->set_theta(pos.theta);
    // support points
    interest_point->mutable_support_points()->Reserve(p->getSupport().size());
    for (auto&& sp : p->getSupport()) {
      auto _sp = interest_point->mutable_support_points()->Add();
      _sp->set_x(sp.x);
      _sp->set_y(sp.y);
    }
    // scale
    interest_point->set_scale(p->getScale());
    // scale level
    interest_point->set_scalelevel(p->getScaleLevel());
    // descriptor
    auto descriptor = dynamic_cast<ShapeContext*>(p->getDescriptor());
    auto desc = interest_point->mutable_desc();
    // descriptor - Histogramn
    desc->mutable_histogram()->Reserve(descriptor->getHistogram().size());
    for (auto&& j : descriptor->getHistogram()) {
      auto m = desc->mutable_histogram()->Add();
      for (auto&& k : j) {
        m->add_values(k);
      }
    }
    // descriptor - Variance
    // desc->mutable_variance()->Reserve(descriptor->getVariance().size());
    // for (auto&& j : descriptor->getVariance()) {
    //   auto m = desc->mutable_variance()->Add();
    //   for (auto&& k : j) {
    //     m->add_values(k);
    //   }
    // }
    // // descriptor - Hit
    // desc->mutable_hit()->Reserve(descriptor->getHit().size());
    // for (auto&& j : descriptor->getHit()) {
    //   auto m = desc->mutable_hit()->Add();
    //   for (auto&& k : j) {
    //     m->add_values(k);
    //   }
    // }
    // // descriptor - Miss
    // desc->mutable_miss()->Reserve(descriptor->getMiss().size());
    // for (auto&& j : descriptor->getMiss()) {
    //   auto m = desc->mutable_miss()->Add();
    //   for (auto&& k : j) {
    //     m->add_values(k);
    //   }
    // }
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
  // FLIRT
  std::vector<InterestPoint*> interest_points;
  interest_points.reserve(proto.interest_points().size());
  for (auto&& pp : proto.interest_points()) {
    auto p = new InterestPoint;
    interest_points.emplace_back(p);

    // Position
    p->setPosition({
        pp.position().position().x(),
        pp.position().position().y(),
        pp.position().theta(),
    });
    // support point
    std::vector<Point2D> support_points;
    support_points.reserve(pp.support_points().size());
    for (auto&& sp : pp.support_points()) {
      support_points.push_back({sp.x(), sp.y()});
    }
    p->setSupport(support_points);
    // scale
    p->setScale(pp.scale());
    // scale level
    p->setScaleLevel(pp.scalelevel());
    // descriptor
    // auto desc = new BetaGrid;
    auto desc = new ShapeContext;
    // descriptor - histogram
    auto& histogram = desc->getHistogram();
    histogram.reserve(pp.desc().histogram().size());
    for (auto&& j : pp.desc().histogram()) {
      std::vector<double> v;
      v.reserve(j.values().size());
      for (auto&& m : j.values()) {
        v.emplace_back(m);
      }
      histogram.emplace_back(std::move(v));
    }
    // descriptor - hit
    // desc->m_hit.reserve(pp.desc().hit().size());
    // for (auto&& j : pp.desc().hit()) {
    //   std::vector<double> v;
    //   v.reserve(j.values().size());
    //   for (auto&& m : j.values()) {
    //     v.emplace_back(m);
    //   }
    //   desc->m_hit.emplace_back(std::move(v));
    // }
    // // descriptor - miss
    // desc->m_miss.reserve(pp.desc().miss().size());
    // for (auto&& j : pp.desc().miss()) {
    //   std::vector<double> v;
    //   v.reserve(j.values().size());
    //   for (auto&& m : j.values()) {
    //     v.emplace_back(m);
    //   }
    //   desc->m_miss.emplace_back(std::move(v));
    // }
    // // descriptor - variance
    // desc->m_variance.reserve(pp.desc().variance().size());
    // for (auto&& j : pp.desc().variance()) {
    //   std::vector<double> v;
    //   v.reserve(j.values().size());
    //   for (auto&& m : j.values()) {
    //     v.emplace_back(m);
    //   }
    //   desc->m_variance.emplace_back(std::move(v));
    // }
    // descriptor - function
    desc->setDistanceFunction(flirt::get_distance_function());
    p->setDescriptor(dynamic_cast<Descriptor*>(desc));
    delete desc;
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
      interest_points};
}

}  // namespace mapping
}  // namespace cartographer

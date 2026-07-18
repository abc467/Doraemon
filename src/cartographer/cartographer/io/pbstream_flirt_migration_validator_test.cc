// Copyright 2026 The Cartographer Authors

#include "cartographer/io/pbstream_flirt_migration_validator.h"

#include <cstdio>
#include <string>

#include "cartographer/io/internal/mapping_state_serialization.h"
#include "cartographer/io/proto_stream.h"
#include "cartographer/mapping/proto/serialization.pb.h"
#include "gtest/gtest.h"

namespace cartographer {
namespace io {
namespace {

void WriteState(const std::string& filename, const bool valid_frame,
                const int num_range_data = 7,
                const bool write_legacy_target_features = false,
                const int timestamp = 0) {
  ProtoStreamWriter writer(filename);
  mapping::proto::SerializationHeader header;
  header.set_format_version(kMappingStateSerializationFormatVersion);
  writer.WriteProto(header);

  mapping::proto::SerializedData pose_graph_data;
  auto* trajectory = pose_graph_data.mutable_pose_graph()->add_trajectory();
  trajectory->set_trajectory_id(0);
  trajectory->add_node()->set_node_index(0);
  trajectory->add_submap()->set_submap_index(0);
  writer.WriteProto(pose_graph_data);

  mapping::proto::SerializedData options_data;
  options_data.mutable_all_trajectory_builder_options()
      ->add_options_with_sensor_ids();
  writer.WriteProto(options_data);

  mapping::proto::SerializedData submap_data;
  submap_data.mutable_submap()->mutable_submap_id()->set_trajectory_id(0);
  submap_data.mutable_submap()->mutable_submap_id()->set_submap_index(0);
  submap_data.mutable_submap()->mutable_submap_2d()->set_num_range_data(
      num_range_data);
  writer.WriteProto(submap_data);

  mapping::proto::SerializedData node_data;
  node_data.mutable_node()->mutable_node_id()->set_trajectory_id(0);
  node_data.mutable_node()->mutable_node_id()->set_node_index(0);
  node_data.mutable_node()->mutable_node_data()->set_timestamp(timestamp);
  if (valid_frame) {
    node_data.mutable_node()->mutable_node_data()->set_interest_point_frame(
        mapping::proto::TrajectoryNodeData::NODE_GRAVITY_ALIGNED);
    node_data.mutable_node()
        ->mutable_node_data()
        ->add_gravity_aligned_interest_points();
  }
  if (write_legacy_target_features) {
    node_data.mutable_node()->mutable_node_data()->add_interest_points();
  }
  writer.WriteProto(node_data);
  ASSERT_TRUE(writer.Close());
}

TEST(PbstreamFlirtMigrationValidatorTest, AcceptsOnlyTagChange) {
  const std::string source = "flirt_migration_source.pbstream";
  const std::string target = "flirt_migration_target.pbstream";
  WriteState(source, false);
  WriteState(target, true);

  const auto result = ValidatePbstreamFlirtMigration(source, target);
  EXPECT_TRUE(result.ok) << result.ToJson();
  EXPECT_EQ(result.source_node_count, 1);
  EXPECT_EQ(result.target_gravity_aligned_node_count, 1);
  EXPECT_EQ(result.target_invalid_frame_node_count, 0);
  EXPECT_EQ(result.target_legacy_tag8_feature_count, 0);
  EXPECT_EQ(result.target_gravity_aligned_feature_count, 1);

  std::remove(source.c_str());
  std::remove(target.c_str());
}

TEST(PbstreamFlirtMigrationValidatorTest, RejectsLegacyTag8InTarget) {
  const std::string source = "flirt_migration_source_tag8.pbstream";
  const std::string target = "flirt_migration_target_tag8.pbstream";
  WriteState(source, false);
  WriteState(target, true, 7, true);

  const auto result = ValidatePbstreamFlirtMigration(source, target);
  EXPECT_FALSE(result.ok);
  EXPECT_EQ(result.target_legacy_tag8_feature_count, 1);

  std::remove(source.c_str());
  std::remove(target.c_str());
}

TEST(PbstreamFlirtMigrationValidatorTest, RejectsMissingTargetTag) {
  const std::string source = "flirt_migration_source_untagged.pbstream";
  const std::string target = "flirt_migration_target_untagged.pbstream";
  WriteState(source, false);
  WriteState(target, false);

  const auto result = ValidatePbstreamFlirtMigration(source, target);
  EXPECT_FALSE(result.ok);
  EXPECT_EQ(result.target_invalid_frame_node_count, 1);

  std::remove(source.c_str());
  std::remove(target.c_str());
}

TEST(PbstreamFlirtMigrationValidatorTest, RejectsSubmapGeometryChange) {
  const std::string source = "flirt_migration_source_geometry.pbstream";
  const std::string target = "flirt_migration_target_geometry.pbstream";
  WriteState(source, false, 7);
  WriteState(target, true, 8);

  const auto result = ValidatePbstreamFlirtMigration(source, target);
  EXPECT_FALSE(result.ok);
  EXPECT_NE(result.source_geometry_fingerprint,
            result.target_geometry_fingerprint);

  std::remove(source.c_str());
  std::remove(target.c_str());
}

TEST(PbstreamFlirtMigrationValidatorTest, RejectsNonFlirtNodeChange) {
  const std::string source = "flirt_migration_source_node.pbstream";
  const std::string target = "flirt_migration_target_node.pbstream";
  WriteState(source, false, 7, false, 100);
  WriteState(target, true, 7, false, 101);

  const auto result = ValidatePbstreamFlirtMigration(source, target);
  EXPECT_FALSE(result.ok);

  std::remove(source.c_str());
  std::remove(target.c_str());
}

TEST(PbstreamFlirtMigrationValidatorTest, MissingFilesFailWithoutAbort) {
  const auto result = ValidatePbstreamFlirtMigration(
      "does-not-exist-source.pbstream", "does-not-exist-target.pbstream");
  EXPECT_FALSE(result.ok);
  EXPECT_FALSE(result.errors.empty());
}

}  // namespace
}  // namespace io
}  // namespace cartographer

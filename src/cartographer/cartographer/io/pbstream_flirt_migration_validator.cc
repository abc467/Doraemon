// Copyright 2026 The Cartographer Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "cartographer/io/pbstream_flirt_migration_validator.h"

#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <utility>

#include "cartographer/io/proto_stream.h"
#include "cartographer/io/internal/mapping_state_serialization.h"
#include "cartographer/mapping/proto/serialization.pb.h"
#include "glog/logging.h"

namespace cartographer {
namespace io {
namespace {

using mapping::proto::SerializedData;

std::string IdKey(const mapping::proto::NodeId& id) {
  return std::to_string(id.trajectory_id()) + ":" +
         std::to_string(id.node_index());
}

std::string IdKey(const mapping::proto::SubmapId& id) {
  return std::to_string(id.trajectory_id()) + ":" +
         std::to_string(id.submap_index());
}

std::uint64_t Fnv1a(const std::string& value, std::uint64_t hash) {
  constexpr std::uint64_t kPrime = 1099511628211ULL;
  for (const unsigned char byte : value) {
    hash ^= byte;
    hash *= kPrime;
  }
  return hash;
}

std::string JsonEscape(const std::string& value) {
  std::ostringstream out;
  for (const unsigned char ch : value) {
    switch (ch) {
      case '\\': out << "\\\\"; break;
      case '"': out << "\\\""; break;
      case '\b': out << "\\b"; break;
      case '\f': out << "\\f"; break;
      case '\n': out << "\\n"; break;
      case '\r': out << "\\r"; break;
      case '\t': out << "\\t"; break;
      default:
        if (ch < 0x20) {
          out << "\\u" << std::hex << std::setw(4) << std::setfill('0')
              << static_cast<int>(ch) << std::dec;
        } else {
          out << ch;
        }
    }
  }
  return out.str();
}

struct Summary {
  int serialized_nodes = 0;
  int serialized_submaps = 0;
  int pose_graph_nodes = 0;
  int pose_graph_submaps = 0;
  int gravity_aligned_nodes = 0;
  int invalid_frame_nodes = 0;
  int legacy_tag8_features = 0;
  int gravity_aligned_features = 0;
  std::set<std::string> node_ids;
  std::map<std::string, std::string> nodes_without_flirt;
  std::map<std::string, std::string> submaps;
  std::map<std::string, std::string> global_submap_poses;
  std::uint64_t geometry_fingerprint = 1469598103934665603ULL;
  std::vector<std::string> errors;
};

bool HasValidPbstreamFraming(const std::string& filename) {
  constexpr std::uint64_t kMagic = 0x7b1d1f7b5bf501dbULL;
  std::ifstream input(filename, std::ios::in | std::ios::binary);
  auto read_uint64 = [&input](std::uint64_t* const value,
                              const bool allow_clean_eof) {
    *value = 0;
    for (int i = 0; i < 8; ++i) {
      const int byte = input.get();
      if (byte == std::char_traits<char>::eof()) {
        return allow_clean_eof && i == 0;
      }
      *value |= static_cast<std::uint64_t>(static_cast<unsigned char>(byte))
                << (8 * i);
    }
    return true;
  };

  std::uint64_t actual_magic = 0;
  if (!read_uint64(&actual_magic, false) || actual_magic != kMagic) {
    return false;
  }
  for (;;) {
    const std::streampos chunk_header = input.tellg();
    std::uint64_t compressed_size = 0;
    if (!read_uint64(&compressed_size, true)) return false;
    if (input.eof()) {
      // A clean EOF immediately after a complete chunk is valid.
      input.clear();
      input.seekg(chunk_header);
      return input.peek() == std::char_traits<char>::eof();
    }
    input.seekg(0, std::ios::end);
    const std::streampos end = input.tellg();
    const std::streampos payload = chunk_header + std::streamoff(8);
    if (end < payload ||
        compressed_size > static_cast<std::uint64_t>(end - payload)) {
      return false;
    }
    input.seekg(payload + static_cast<std::streamoff>(compressed_size));
    if (!input.good()) return false;
  }
}

Summary ReadSummary(const std::string& filename) {
  Summary result;
  if (!HasValidPbstreamFraming(filename)) {
    result.errors.push_back(
        "file is missing, unreadable, corrupt, or has invalid framing");
    return result;
  }

  try {
    ProtoStreamReader reader(filename);
    mapping::proto::SerializationHeader header;
    if (!reader.ReadProto(&header)) {
      result.errors.push_back("missing serialization header");
      return result;
    }
    if (header.format_version() != kMappingStateSerializationFormatVersion &&
        header.format_version() != kFormatVersionWithoutSubmapHistograms) {
      result.errors.push_back("unsupported serialization format");
      return result;
    }
    SerializedData pose_graph_data;
    if (!reader.ReadProto(&pose_graph_data) || !pose_graph_data.has_pose_graph()) {
      result.errors.push_back("missing pose graph");
      return result;
    }
    SerializedData options_data;
    if (!reader.ReadProto(&options_data) ||
        !options_data.has_all_trajectory_builder_options()) {
      result.errors.push_back("missing trajectory builder options");
      return result;
    }
    if (pose_graph_data.pose_graph().trajectory_size() !=
        options_data.all_trajectory_builder_options()
            .options_with_sensor_ids_size()) {
      result.errors.push_back("trajectory/options count mismatch");
      return result;
    }

  for (const auto& trajectory : pose_graph_data.pose_graph().trajectory()) {
    result.pose_graph_nodes += trajectory.node_size();
    result.pose_graph_submaps += trajectory.submap_size();
    for (const auto& submap : trajectory.submap()) {
      const std::string key = std::to_string(trajectory.trajectory_id()) + ":" +
                              std::to_string(submap.submap_index());
      if (!result.global_submap_poses
               .emplace(key, submap.pose().SerializeAsString())
               .second) {
        result.errors.push_back("duplicate pose-graph submap id " + key);
      }
    }
  }

  SerializedData data;
  while (reader.ReadProto(&data)) {
    if (data.has_node()) {
      ++result.serialized_nodes;
      const std::string key = IdKey(data.node().node_id());
      if (!result.node_ids.insert(key).second) {
        result.errors.push_back("duplicate serialized node id " + key);
      }
      auto node_without_flirt = data.node();
      node_without_flirt.mutable_node_data()->clear_interest_points();
      node_without_flirt.mutable_node_data()
          ->clear_gravity_aligned_interest_points();
      node_without_flirt.mutable_node_data()->set_interest_point_frame(
          mapping::proto::TrajectoryNodeData::UNSPECIFIED);
      if (!result.nodes_without_flirt
               .emplace(key, node_without_flirt.SerializeAsString())
               .second) {
        result.errors.push_back("duplicate non-FLIRT node id " + key);
      }
      result.legacy_tag8_features +=
          data.node().node_data().interest_points_size();
      result.gravity_aligned_features +=
          data.node().node_data().gravity_aligned_interest_points_size();
      if (data.node().node_data().interest_point_frame() ==
          mapping::proto::TrajectoryNodeData::NODE_GRAVITY_ALIGNED) {
        ++result.gravity_aligned_nodes;
      } else {
        ++result.invalid_frame_nodes;
      }
    } else if (data.has_submap()) {
      ++result.serialized_submaps;
      const std::string key = IdKey(data.submap().submap_id());
      if (!result.submaps.emplace(key, data.submap().SerializeAsString())
               .second) {
        result.errors.push_back("duplicate serialized submap id " + key);
      }
    }
  }
  if (!reader.eof()) {
    result.errors.push_back("corrupt or truncated serialized data");
  }

  if (result.serialized_nodes != result.pose_graph_nodes) {
    result.errors.push_back("serialized/pose-graph node count mismatch");
  }
  if (result.serialized_submaps != result.pose_graph_submaps) {
    result.errors.push_back("serialized/pose-graph submap count mismatch");
  }

  for (const auto& entry : result.submaps) {
    result.geometry_fingerprint = Fnv1a(entry.first, result.geometry_fingerprint);
    result.geometry_fingerprint = Fnv1a(entry.second, result.geometry_fingerprint);
  }
  for (const auto& entry : result.global_submap_poses) {
    result.geometry_fingerprint = Fnv1a(entry.first, result.geometry_fingerprint);
    result.geometry_fingerprint = Fnv1a(entry.second, result.geometry_fingerprint);
  }
  } catch (const std::exception& error) {
    result.errors.push_back(std::string("failed to decode pbstream: ") +
                            error.what());
  }
  return result;
}

void AppendErrors(const std::string& prefix, const std::vector<std::string>& in,
                  std::vector<std::string>* const out) {
  for (const auto& error : in) out->push_back(prefix + error);
}

}  // namespace

std::string PbstreamFlirtMigrationValidation::ToJson() const {
  std::ostringstream out;
  out << "{\"ok\":" << (ok ? "true" : "false")
      << ",\"source_node_count\":" << source_node_count
      << ",\"target_node_count\":" << target_node_count
      << ",\"source_submap_count\":" << source_submap_count
      << ",\"target_submap_count\":" << target_submap_count
      << ",\"target_gravity_aligned_node_count\":"
      << target_gravity_aligned_node_count
      << ",\"target_invalid_frame_node_count\":"
      << target_invalid_frame_node_count
      << ",\"target_legacy_tag8_feature_count\":"
      << target_legacy_tag8_feature_count
      << ",\"target_gravity_aligned_feature_count\":"
      << target_gravity_aligned_feature_count
      << ",\"source_geometry_fingerprint\":\"" << std::hex
      << source_geometry_fingerprint
      << "\",\"target_geometry_fingerprint\":\""
      << target_geometry_fingerprint << std::dec << "\",\"errors\":[";
  for (std::size_t i = 0; i < errors.size(); ++i) {
    if (i != 0) out << ',';
    out << '"' << JsonEscape(errors[i]) << '"';
  }
  out << "]}";
  return out.str();
}

PbstreamFlirtMigrationValidation ValidatePbstreamFlirtMigration(
    const std::string& source_filename, const std::string& target_filename) {
  PbstreamFlirtMigrationValidation result;
  if (source_filename == target_filename) {
    result.errors.push_back("source and target filenames are identical");
    return result;
  }

  const Summary source = ReadSummary(source_filename);
  const Summary target = ReadSummary(target_filename);
  result.source_node_count = source.serialized_nodes;
  result.target_node_count = target.serialized_nodes;
  result.source_submap_count = source.serialized_submaps;
  result.target_submap_count = target.serialized_submaps;
  result.target_gravity_aligned_node_count = target.gravity_aligned_nodes;
  result.target_invalid_frame_node_count = target.invalid_frame_nodes;
  result.target_legacy_tag8_feature_count = target.legacy_tag8_features;
  result.target_gravity_aligned_feature_count =
      target.gravity_aligned_features;
  result.source_geometry_fingerprint = source.geometry_fingerprint;
  result.target_geometry_fingerprint = target.geometry_fingerprint;
  AppendErrors("source: ", source.errors, &result.errors);
  AppendErrors("target: ", target.errors, &result.errors);

  if (source.node_ids != target.node_ids) {
    result.errors.push_back("node id set changed");
  }
  if (source.nodes_without_flirt != target.nodes_without_flirt) {
    result.errors.push_back("non-FLIRT trajectory node content changed");
  }
  if (source.submaps != target.submaps) {
    result.errors.push_back("serialized submap geometry/content changed");
  }
  if (source.global_submap_poses != target.global_submap_poses) {
    result.errors.push_back("pose-graph submap geometry changed");
  }
  if (source.serialized_nodes != target.serialized_nodes) {
    result.errors.push_back("node count changed");
  }
  if (source.serialized_submaps != target.serialized_submaps) {
    result.errors.push_back("submap count changed");
  }
  if (target.invalid_frame_nodes != 0) {
    result.errors.push_back("target contains nodes without NODE_GRAVITY_ALIGNED frame");
  }
  if (target.legacy_tag8_features != 0) {
    result.errors.push_back(
        "target contains legacy tag-8 FLIRT features; only tag 10 is allowed");
  }
  if (target.gravity_aligned_nodes != target.serialized_nodes) {
    result.errors.push_back("not every target node has a valid FLIRT frame tag");
  }
  result.ok = result.errors.empty();
  return result;
}

int pbstream_validate_flirt_migration(int argc, char** argv) {
  if (argc != 4) {
    LOG(ERROR) << "Usage: " << argv[0] << " " << argv[1]
               << " <source.pbstream> <target.pbstream>";
    return EXIT_FAILURE;
  }
  const auto result = ValidatePbstreamFlirtMigration(argv[2], argv[3]);
  // JSON is deliberately written to stdout for machine consumers. Cartographer
  // logs remain on stderr.
  std::cout << result.ToJson() << std::endl;
  return result.ok ? EXIT_SUCCESS : EXIT_FAILURE;
}

}  // namespace io
}  // namespace cartographer

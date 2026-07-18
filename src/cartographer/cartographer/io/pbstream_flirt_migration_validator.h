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

#ifndef CARTOGRAPHER_IO_PBSTREAM_FLIRT_MIGRATION_VALIDATOR_H_
#define CARTOGRAPHER_IO_PBSTREAM_FLIRT_MIGRATION_VALIDATOR_H_

#include <cstdint>
#include <string>
#include <vector>

namespace cartographer {
namespace io {

// Structural result used as the activation gate for a FLIRT frame migration.
// Source-file immutability is intentionally checked by the orchestration layer
// with SHA-256; this validator focuses on the contents of the two pbstreams.
struct PbstreamFlirtMigrationValidation {
  bool ok = false;
  int source_node_count = 0;
  int target_node_count = 0;
  int source_submap_count = 0;
  int target_submap_count = 0;
  int target_gravity_aligned_node_count = 0;
  int target_invalid_frame_node_count = 0;
  int target_legacy_tag8_feature_count = 0;
  int target_gravity_aligned_feature_count = 0;
  std::uint64_t source_geometry_fingerprint = 0;
  std::uint64_t target_geometry_fingerprint = 0;
  std::vector<std::string> errors;

  std::string ToJson() const;
};

PbstreamFlirtMigrationValidation ValidatePbstreamFlirtMigration(
    const std::string& source_filename, const std::string& target_filename);

// Entry point for `cartographer_pbstream validate-flirt-migration`.
int pbstream_validate_flirt_migration(int argc, char** argv);

}  // namespace io
}  // namespace cartographer

#endif  // CARTOGRAPHER_IO_PBSTREAM_FLIRT_MIGRATION_VALIDATOR_H_

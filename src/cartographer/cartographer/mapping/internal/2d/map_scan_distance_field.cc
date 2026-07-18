/*
 * Copyright 2026 The Cartographer Authors
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

#include "cartographer/mapping/internal/2d/map_scan_distance_field.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <limits>
#include <map>
#include <sstream>
#include <utility>

namespace cartographer {
namespace mapping {
namespace map_scan_distance_field {
namespace {

constexpr char kCacheMagicV1[] = "DORAMON_MAP_SCAN_DF_V1";
constexpr char kCacheMagicV2[] = "DORAMON_MAP_SCAN_DF_V2";
constexpr char kEncodingV1[] = "uint16_mm_then_uint8_known";
constexpr char kEncodingV2[] = "uint16_known_bit15_distance_mm";

void SetError(const std::string& message, std::string* error) {
  if (error != nullptr) {
    *error = message;
  }
}

bool IsFinite(double value) { return std::isfinite(value); }

bool NearlyEqual(double lhs, double rhs) {
  return std::fabs(lhs - rhs) <=
         1e-9 * std::max(1., std::max(std::fabs(lhs), std::fabs(rhs)));
}

template <typename T>
bool ParseNumber(const std::string& text, T* value) {
  std::istringstream stream(text);
  stream >> *value;
  if (!stream) {
    return false;
  }
  stream >> std::ws;
  return stream.eof();
}

const std::string* FindValue(const std::map<std::string, std::string>& header,
                             const std::string& key) {
  const auto it = header.find(key);
  return it == header.end() ? nullptr : &it->second;
}

bool ReadHeader(std::istream* input, std::string* magic,
                std::map<std::string, std::string>* header,
                std::string* error) {
  if (input == nullptr || !std::getline(*input, *magic)) {
    SetError("missing cache magic", error);
    return false;
  }

  std::string line;
  bool found_end = false;
  while (std::getline(*input, line)) {
    if (line == "END_HEADER") {
      found_end = true;
      break;
    }
    std::istringstream line_stream(line);
    std::string key;
    if (!(line_stream >> key)) {
      continue;
    }
    std::string value;
    std::getline(line_stream, value);
    if (!value.empty() && value.front() == ' ') {
      value.erase(value.begin());
    }
    if (value.empty() || !header->emplace(key, value).second) {
      SetError("invalid or duplicate cache header key: " + key, error);
      return false;
    }
  }
  if (!found_end) {
    SetError("missing END_HEADER", error);
    return false;
  }
  return true;
}

bool ValidateCacheOptions(const CacheOptions& options, std::string* error) {
  if (options.cache_key.empty()) {
    SetError("cache key is empty", error);
    return false;
  }
  if (!IsFinite(options.max_distance_m) || options.max_distance_m <= 0. ||
      options.max_distance_m * kDistanceScale >
          kMaxStoredDistanceMillimeters + 1e-9) {
    SetError("max distance must be in (0, 1] metres", error);
    return false;
  }
  if (!IsFinite(options.occupied_probability_threshold) ||
      options.occupied_probability_threshold < 0. ||
      options.occupied_probability_threshold > 1.) {
    SetError("occupied probability threshold is outside [0, 1]", error);
    return false;
  }
  return true;
}

bool ValidateField(const Field& field, std::string* error) {
  size_t cell_count = 0;
  if (!field.valid || !CheckedCellCount(field.width, field.height,
                                        &cell_count, error)) {
    if (!field.valid) {
      SetError("distance field is not valid", error);
    }
    return false;
  }
  if (!IsFinite(field.resolution) || field.resolution <= 0. ||
      !IsFinite(field.origin_x) || !IsFinite(field.origin_y) ||
      !IsFinite(field.max_distance_m) || field.max_distance_m <= 0. ||
      field.max_distance_m * kDistanceScale >
          kMaxStoredDistanceMillimeters + 1e-9 ||
      field.frozen_finished_submap_count < 0) {
    SetError("invalid distance field metadata", error);
    return false;
  }
  if (field.cells.size() != cell_count) {
    SetError("distance field payload size does not match dimensions", error);
    return false;
  }
  const uint16_t max_distance_mm = static_cast<uint16_t>(
      std::lround(field.max_distance_m * kDistanceScale));
  for (const uint16_t cell : field.cells) {
    if (DistanceMillimeters(cell) > max_distance_mm) {
      SetError("distance field cell exceeds configured maximum", error);
      return false;
    }
  }
  return true;
}

struct DistanceTransformWorkspace {
  std::vector<int> sites;
  std::vector<float> site_values;
  std::vector<double> boundaries;

  explicit DistanceTransformWorkspace(int length)
      : sites(length), site_values(length), boundaries(length + 1) {}
};

// Applies the lower envelope of parabolas to a strided line in-place. The
// envelope stores the original site value, so writing output cannot overwrite
// a value that is still needed by a later sample.
void DistanceTransformLine(std::vector<float>* squared_distance, size_t offset,
                           size_t stride, int length,
                           DistanceTransformWorkspace* workspace) {
  int site_count = 0;
  for (int q = 0; q < length; ++q) {
    const float value = (*squared_distance)[offset + stride * q];
    if (!std::isfinite(value)) {
      continue;
    }

    double intersection = -std::numeric_limits<double>::infinity();
    while (site_count > 0) {
      const int previous_site = workspace->sites[site_count - 1];
      const double previous_value =
          workspace->site_values[site_count - 1];
      intersection =
          ((static_cast<double>(value) + static_cast<double>(q) * q) -
           (previous_value +
            static_cast<double>(previous_site) * previous_site)) /
          (2. * (q - previous_site));
      if (intersection > workspace->boundaries[site_count - 1]) {
        break;
      }
      --site_count;
    }
    if (site_count == 0) {
      intersection = -std::numeric_limits<double>::infinity();
    }
    workspace->sites[site_count] = q;
    workspace->site_values[site_count] = value;
    workspace->boundaries[site_count] = intersection;
    ++site_count;
    workspace->boundaries[site_count] =
        std::numeric_limits<double>::infinity();
  }

  if (site_count == 0) {
    return;
  }
  int envelope_index = 0;
  for (int q = 0; q < length; ++q) {
    while (envelope_index + 1 < site_count &&
           workspace->boundaries[envelope_index + 1] < q) {
      ++envelope_index;
    }
    const double delta = q - workspace->sites[envelope_index];
    (*squared_distance)[offset + stride * q] = static_cast<float>(
        workspace->site_values[envelope_index] + delta * delta);
  }
}

bool ReadExact(std::istream* input, char* data, size_t size,
               std::string* error) {
  if (size > static_cast<size_t>(std::numeric_limits<std::streamsize>::max())) {
    SetError("cache payload is too large for stream I/O", error);
    return false;
  }
  input->read(data, static_cast<std::streamsize>(size));
  if (input->gcount() != static_cast<std::streamsize>(size)) {
    SetError("truncated cache payload", error);
    return false;
  }
  return true;
}

bool HasExactPayloadLength(std::istream* input, std::string* error) {
  if (input->peek() != std::char_traits<char>::eof()) {
    SetError("cache payload has trailing bytes", error);
    return false;
  }
  return true;
}

}  // namespace

bool CheckedCellCount(int width, int height, size_t* cell_count,
                      std::string* error) {
  if (cell_count == nullptr) {
    SetError("cell count output is null", error);
    return false;
  }
  *cell_count = 0;
  if (width <= 0 || height <= 0) {
    SetError("distance field dimensions must be positive", error);
    return false;
  }
  const size_t width_size = static_cast<size_t>(width);
  const size_t height_size = static_cast<size_t>(height);
  if (width_size > std::numeric_limits<size_t>::max() / height_size) {
    SetError("distance field dimensions overflow size_t", error);
    return false;
  }
  const size_t count = width_size * height_size;
  if (count > kMaxCellCount) {
    SetError("distance field exceeds 32,000,000 cell limit", error);
    return false;
  }
  *cell_count = count;
  return true;
}

uint16_t PackCell(bool known, uint16_t distance_millimeters) {
  const uint16_t clamped_distance =
      std::min(distance_millimeters, kMaxStoredDistanceMillimeters);
  return static_cast<uint16_t>((known ? kKnownCellBit : 0) |
                               clamped_distance);
}

bool IsKnown(uint16_t cell) { return (cell & kKnownCellBit) != 0; }

uint16_t DistanceMillimeters(uint16_t cell) {
  return cell & kDistanceMillimetersMask;
}

bool Rasterize(const std::vector<RasterSource>& sources, double margin_m,
               int frozen_finished_submap_count, Raster* raster,
               std::string* error) {
  if (raster == nullptr) {
    SetError("raster output is null", error);
    return false;
  }
  *raster = Raster{};
  if (sources.empty()) {
    SetError("no raster sources", error);
    return false;
  }
  if (!IsFinite(margin_m) || margin_m < 0. ||
      frozen_finished_submap_count < 0) {
    SetError("invalid rasterization metadata", error);
    return false;
  }

  double resolution = std::numeric_limits<double>::infinity();
  double min_x = std::numeric_limits<double>::infinity();
  double min_y = std::numeric_limits<double>::infinity();
  double max_x = -std::numeric_limits<double>::infinity();
  double max_y = -std::numeric_limits<double>::infinity();
  for (const RasterSource& source : sources) {
    if (!IsFinite(source.resolution) || source.resolution <= 0. ||
        !IsFinite(source.local_min_x) ||
        !IsFinite(source.local_min_y) ||
        !IsFinite(source.local_max_x) ||
        !IsFinite(source.local_max_y) ||
        source.local_min_x >= source.local_max_x ||
        source.local_min_y >= source.local_max_y ||
        !IsFinite(source.global_from_local_translation_x) ||
        !IsFinite(source.global_from_local_translation_y) ||
        !IsFinite(source.global_from_local_rotation) || !source.sample) {
      SetError("invalid raster source", error);
      return false;
    }
    resolution = std::min(resolution, source.resolution);
    const double cosine = std::cos(source.global_from_local_rotation);
    const double sine = std::sin(source.global_from_local_rotation);
    const std::array<std::pair<double, double>, 4> corners = {{
        {source.local_min_x, source.local_min_y},
        {source.local_min_x, source.local_max_y},
        {source.local_max_x, source.local_min_y},
        {source.local_max_x, source.local_max_y},
    }};
    for (const auto& corner : corners) {
      const double global_x = source.global_from_local_translation_x +
                              cosine * corner.first - sine * corner.second;
      const double global_y = source.global_from_local_translation_y +
                              sine * corner.first + cosine * corner.second;
      min_x = std::min(min_x, global_x);
      min_y = std::min(min_y, global_y);
      max_x = std::max(max_x, global_x);
      max_y = std::max(max_y, global_y);
    }
  }

  const double origin_x = std::floor((min_x - margin_m) / resolution) *
                          resolution;
  const double origin_y = std::floor((min_y - margin_m) / resolution) *
                          resolution;
  const double width_cells =
      std::ceil((max_x + margin_m - origin_x) / resolution - 1e-9);
  const double height_cells =
      std::ceil((max_y + margin_m - origin_y) / resolution - 1e-9);
  if (!IsFinite(origin_x) || !IsFinite(origin_y) ||
      !IsFinite(width_cells) || !IsFinite(height_cells) || width_cells < 1. ||
      height_cells < 1. ||
      width_cells > std::numeric_limits<int>::max() ||
      height_cells > std::numeric_limits<int>::max()) {
    SetError("rasterized geometry exceeds integer dimensions", error);
    return false;
  }

  Raster result;
  result.resolution = resolution;
  result.origin_x = origin_x;
  result.origin_y = origin_y;
  result.width = static_cast<int>(width_cells);
  result.height = static_cast<int>(height_cells);
  result.frozen_finished_submap_count = frozen_finished_submap_count;
  size_t cell_count = 0;
  if (!CheckedCellCount(result.width, result.height, &cell_count, error)) {
    return false;
  }
  result.known.assign(cell_count, 0);
  result.occupied.assign(cell_count, 0);

  for (const RasterSource& source : sources) {
    const double cosine = std::cos(source.global_from_local_rotation);
    const double sine = std::sin(source.global_from_local_rotation);
    double source_min_x = std::numeric_limits<double>::infinity();
    double source_min_y = std::numeric_limits<double>::infinity();
    double source_max_x = -std::numeric_limits<double>::infinity();
    double source_max_y = -std::numeric_limits<double>::infinity();
    const std::array<std::pair<double, double>, 4> corners = {{
        {source.local_min_x, source.local_min_y},
        {source.local_min_x, source.local_max_y},
        {source.local_max_x, source.local_min_y},
        {source.local_max_x, source.local_max_y},
    }};
    for (const auto& corner : corners) {
      const double global_x = source.global_from_local_translation_x +
                              cosine * corner.first - sine * corner.second;
      const double global_y = source.global_from_local_translation_y +
                              sine * corner.first + cosine * corner.second;
      source_min_x = std::min(source_min_x, global_x);
      source_min_y = std::min(source_min_y, global_y);
      source_max_x = std::max(source_max_x, global_x);
      source_max_y = std::max(source_max_y, global_y);
    }
    const int first_x = std::max(
        0, static_cast<int>(std::floor(
               (source_min_x - result.origin_x) / result.resolution)));
    const int first_y = std::max(
        0, static_cast<int>(std::floor(
               (source_min_y - result.origin_y) / result.resolution)));
    const int last_x = std::min(
        result.width - 1,
        static_cast<int>(std::ceil(
            (source_max_x - result.origin_x) / result.resolution)));
    const int last_y = std::min(
        result.height - 1,
        static_cast<int>(std::ceil(
            (source_max_y - result.origin_y) / result.resolution)));

    for (int y = first_y; y <= last_y; ++y) {
      for (int x = first_x; x <= last_x; ++x) {
        const double global_x =
            result.origin_x + (static_cast<double>(x) + 0.5) * resolution;
        const double global_y =
            result.origin_y + (static_cast<double>(y) + 0.5) * resolution;
        const double translated_x =
            global_x - source.global_from_local_translation_x;
        const double translated_y =
            global_y - source.global_from_local_translation_y;
        const double local_x = cosine * translated_x + sine * translated_y;
        const double local_y = -sine * translated_x + cosine * translated_y;
        if (local_x < source.local_min_x || local_x >= source.local_max_x ||
            local_y < source.local_min_y || local_y >= source.local_max_y) {
          continue;
        }
        const RasterCell sampled = source.sample(local_x, local_y);
        if (sampled.occupied && !sampled.known) {
          SetError("raster source returned occupied unknown cell", error);
          return false;
        }
        const size_t target_index = static_cast<size_t>(y) * result.width + x;
        result.known[target_index] |= sampled.known;
        result.occupied[target_index] |= sampled.occupied;
      }
    }
  }

  *raster = std::move(result);
  return true;
}

bool Build(const Raster& raster, double max_distance_m, Field* field,
           std::string* error) {
  if (field == nullptr) {
    SetError("distance field output is null", error);
    return false;
  }
  *field = Field{};
  size_t cell_count = 0;
  if (!CheckedCellCount(raster.width, raster.height, &cell_count, error)) {
    return false;
  }
  if (!IsFinite(raster.resolution) || raster.resolution <= 0. ||
      !IsFinite(raster.origin_x) || !IsFinite(raster.origin_y) ||
      raster.frozen_finished_submap_count < 0) {
    SetError("invalid raster metadata", error);
    return false;
  }
  if (!IsFinite(max_distance_m) || max_distance_m <= 0. ||
      max_distance_m * kDistanceScale >
          kMaxStoredDistanceMillimeters + 1e-9) {
    SetError("max distance must be in (0, 1] metres", error);
    return false;
  }
  if (raster.known.size() != cell_count ||
      raster.occupied.size() != cell_count) {
    SetError("raster payload size does not match dimensions", error);
    return false;
  }

  std::vector<float> squared_distance(
      cell_count, std::numeric_limits<float>::infinity());
  bool has_occupied_cell = false;
  for (size_t i = 0; i < cell_count; ++i) {
    if (raster.known[i] > 1 || raster.occupied[i] > 1 ||
        (raster.occupied[i] && !raster.known[i])) {
      SetError("raster masks must be binary and occupied implies known", error);
      return false;
    }
    if (raster.occupied[i]) {
      squared_distance[i] = 0.f;
      has_occupied_cell = true;
    }
  }
  if (!has_occupied_cell) {
    SetError("raster contains no occupied cells", error);
    return false;
  }

  DistanceTransformWorkspace workspace(
      std::max(raster.width, raster.height));
  for (int y = 0; y < raster.height; ++y) {
    DistanceTransformLine(&squared_distance,
                          static_cast<size_t>(y) * raster.width, 1,
                          raster.width, &workspace);
  }
  for (int x = 0; x < raster.width; ++x) {
    DistanceTransformLine(&squared_distance, x, raster.width, raster.height,
                          &workspace);
  }

  const uint16_t max_distance_mm = static_cast<uint16_t>(
      std::lround(max_distance_m * kDistanceScale));
  Field result;
  result.resolution = raster.resolution;
  result.origin_x = raster.origin_x;
  result.origin_y = raster.origin_y;
  result.width = raster.width;
  result.height = raster.height;
  result.frozen_finished_submap_count =
      raster.frozen_finished_submap_count;
  result.max_distance_m = max_distance_m;
  result.cells.resize(cell_count);
  for (size_t i = 0; i < cell_count; ++i) {
    const double distance_m =
        std::sqrt(static_cast<double>(squared_distance[i])) *
        raster.resolution;
    const uint16_t distance_mm = static_cast<uint16_t>(std::min<long>(
        max_distance_mm, std::lround(distance_m * kDistanceScale)));
    result.cells[i] = PackCell(raster.known[i] != 0, distance_mm);
  }
  result.valid = true;
  *field = std::move(result);
  return true;
}

SampleResult Sample(const Field& field, double x, double y) {
  SampleResult result;
  if (!field.valid || !IsFinite(field.resolution) ||
      field.resolution <= 0. || !IsFinite(x) || !IsFinite(y)) {
    return result;
  }
  size_t cell_count = 0;
  if (!CheckedCellCount(field.width, field.height, &cell_count, nullptr) ||
      field.cells.size() != cell_count) {
    return result;
  }

  const double cell_x = std::floor((x - field.origin_x) / field.resolution);
  const double cell_y = std::floor((y - field.origin_y) / field.resolution);
  if (cell_x < 0. || cell_x >= field.width || cell_y < 0. ||
      cell_y >= field.height) {
    return result;
  }
  const int grid_x = static_cast<int>(cell_x);
  const int grid_y = static_cast<int>(cell_y);
  const uint16_t cell = field.cells[static_cast<size_t>(grid_y) * field.width +
                                    grid_x];
  result.in_bounds = true;
  result.known = IsKnown(cell);
  result.distance_m =
      static_cast<float>(DistanceMillimeters(cell)) / kDistanceScale;
  return result;
}

bool LoadCache(std::istream* input, const CacheOptions& options, Field* field,
               CacheFormat* format, std::string* error) {
  if (field == nullptr || format == nullptr) {
    SetError("cache output is null", error);
    return false;
  }
  *field = Field{};
  *format = CacheFormat::kUnknown;
  if (input == nullptr || !ValidateCacheOptions(options, error)) {
    if (input == nullptr) {
      SetError("cache input is null", error);
    }
    return false;
  }

  std::string magic;
  std::map<std::string, std::string> header;
  if (!ReadHeader(input, &magic, &header, error)) {
    return false;
  }

  const std::string* version_text = FindValue(header, "version");
  const std::string* cache_key = FindValue(header, "cache_key");
  const std::string* resolution_text = FindValue(header, "resolution");
  const std::string* origin_x_text = FindValue(header, "origin_x");
  const std::string* origin_y_text = FindValue(header, "origin_y");
  const std::string* width_text = FindValue(header, "width");
  const std::string* height_text = FindValue(header, "height");
  const std::string* submap_count_text = FindValue(header, "submap_count");
  const std::string* max_distance_text = FindValue(header, "max_distance_m");
  const std::string* threshold_text =
      FindValue(header, "occupied_probability_threshold");
  const std::string* scale_text = FindValue(header, "distance_scale");
  const std::string* encoding = FindValue(header, "encoding");
  if (version_text == nullptr || cache_key == nullptr ||
      resolution_text == nullptr || origin_x_text == nullptr ||
      origin_y_text == nullptr || width_text == nullptr ||
      height_text == nullptr || submap_count_text == nullptr ||
      max_distance_text == nullptr || threshold_text == nullptr ||
      scale_text == nullptr || encoding == nullptr) {
    SetError("incomplete cache header", error);
    return false;
  }

  long long version = 0;
  long long width = 0;
  long long height = 0;
  long long submap_count = 0;
  long long distance_scale = 0;
  double resolution = 0.;
  double origin_x = 0.;
  double origin_y = 0.;
  double max_distance_m = 0.;
  double occupied_probability_threshold = 0.;
  if (!ParseNumber(*version_text, &version) ||
      !ParseNumber(*resolution_text, &resolution) ||
      !ParseNumber(*origin_x_text, &origin_x) ||
      !ParseNumber(*origin_y_text, &origin_y) ||
      !ParseNumber(*width_text, &width) ||
      !ParseNumber(*height_text, &height) ||
      !ParseNumber(*submap_count_text, &submap_count) ||
      !ParseNumber(*max_distance_text, &max_distance_m) ||
      !ParseNumber(*threshold_text, &occupied_probability_threshold) ||
      !ParseNumber(*scale_text, &distance_scale)) {
    SetError("invalid numeric cache header value", error);
    return false;
  }

  CacheFormat detected_format = CacheFormat::kUnknown;
  if (magic == kCacheMagicV1 && version == 1 && *encoding == kEncodingV1) {
    detected_format = CacheFormat::kV1;
  } else if (magic == kCacheMagicV2 && version == 2 &&
             *encoding == kEncodingV2) {
    detected_format = CacheFormat::kV2;
  } else {
    SetError("unsupported cache magic, version or encoding", error);
    return false;
  }
  if (*cache_key != options.cache_key || distance_scale != kDistanceScale ||
      !NearlyEqual(max_distance_m, options.max_distance_m) ||
      !NearlyEqual(occupied_probability_threshold,
                   options.occupied_probability_threshold)) {
    SetError("cache does not match the requested map or parameters", error);
    return false;
  }
  if (!IsFinite(resolution) || resolution <= 0. || !IsFinite(origin_x) ||
      !IsFinite(origin_y) || width <= 0 ||
      width > std::numeric_limits<int>::max() || height <= 0 ||
      height > std::numeric_limits<int>::max() || submap_count < 0 ||
      submap_count > std::numeric_limits<int>::max()) {
    SetError("invalid cache geometry metadata", error);
    return false;
  }

  size_t cell_count = 0;
  if (!CheckedCellCount(static_cast<int>(width), static_cast<int>(height),
                        &cell_count, error)) {
    return false;
  }
  Field result;
  result.resolution = resolution;
  result.origin_x = origin_x;
  result.origin_y = origin_y;
  result.width = static_cast<int>(width);
  result.height = static_cast<int>(height);
  result.frozen_finished_submap_count = static_cast<int>(submap_count);
  result.max_distance_m = max_distance_m;
  result.cells.resize(cell_count);
  const uint16_t max_distance_mm = static_cast<uint16_t>(
      std::lround(max_distance_m * kDistanceScale));

  if (detected_format == CacheFormat::kV1) {
    std::vector<uint16_t> distance_mm(cell_count);
    std::vector<uint8_t> known(cell_count);
    if (!ReadExact(input, reinterpret_cast<char*>(distance_mm.data()),
                   distance_mm.size() * sizeof(uint16_t), error) ||
        !ReadExact(input, reinterpret_cast<char*>(known.data()), known.size(),
                   error)) {
      return false;
    }
    for (size_t i = 0; i < cell_count; ++i) {
      if (known[i] > 1 || distance_mm[i] > max_distance_mm) {
        SetError("invalid V1 cache cell", error);
        return false;
      }
      result.cells[i] = PackCell(known[i] != 0, distance_mm[i]);
    }
  } else {
    if (!ReadExact(input, reinterpret_cast<char*>(result.cells.data()),
                   result.cells.size() * sizeof(uint16_t), error)) {
      return false;
    }
    for (const uint16_t cell : result.cells) {
      if (DistanceMillimeters(cell) > max_distance_mm) {
        SetError("invalid V2 cache cell", error);
        return false;
      }
    }
  }
  if (!HasExactPayloadLength(input, error)) {
    return false;
  }

  result.valid = true;
  *field = std::move(result);
  *format = detected_format;
  return true;
}

bool SaveCacheV2(std::ostream* output, const CacheOptions& options,
                 const Field& field, std::string* error) {
  if (output == nullptr) {
    SetError("cache output is null", error);
    return false;
  }
  if (!ValidateCacheOptions(options, error) ||
      !ValidateField(field, error)) {
    return false;
  }
  if (!NearlyEqual(field.max_distance_m, options.max_distance_m)) {
    SetError("field maximum distance does not match cache options", error);
    return false;
  }

  *output << kCacheMagicV2 << "\n"
          << "version 2\n"
          << "cache_key " << options.cache_key << "\n"
          << "resolution " << std::setprecision(17) << field.resolution
          << "\n"
          << "origin_x " << std::setprecision(17) << field.origin_x << "\n"
          << "origin_y " << std::setprecision(17) << field.origin_y << "\n"
          << "width " << field.width << "\n"
          << "height " << field.height << "\n"
          << "submap_count " << field.frozen_finished_submap_count << "\n"
          << "max_distance_m " << std::setprecision(17)
          << options.max_distance_m << "\n"
          << "occupied_probability_threshold " << std::setprecision(17)
          << options.occupied_probability_threshold << "\n"
          << "distance_scale " << kDistanceScale << "\n"
          << "encoding " << kEncodingV2 << "\n"
          << "END_HEADER\n";
  output->write(reinterpret_cast<const char*>(field.cells.data()),
                static_cast<std::streamsize>(field.cells.size() *
                                             sizeof(uint16_t)));
  if (!*output) {
    SetError("failed while writing V2 cache", error);
    return false;
  }
  return true;
}

bool LoadCache(const std::string& filename, const CacheOptions& options,
               Field* field, CacheFormat* format, std::string* error) {
  std::ifstream input(filename, std::ios::binary);
  if (!input.is_open()) {
    SetError("failed to open cache file: " + filename, error);
    return false;
  }
  return LoadCache(&input, options, field, format, error);
}

bool SaveCacheV2(const std::string& filename, const CacheOptions& options,
                 const Field& field, std::string* error) {
  if (filename.empty()) {
    SetError("cache filename is empty", error);
    return false;
  }
  const std::string temporary_filename = filename + ".tmp";
  std::ofstream output(temporary_filename,
                       std::ios::binary | std::ios::trunc);
  if (!output.is_open()) {
    SetError("failed to open temporary cache file: " + temporary_filename,
             error);
    return false;
  }
  if (!SaveCacheV2(&output, options, field, error)) {
    output.close();
    std::remove(temporary_filename.c_str());
    return false;
  }
  output.close();
  if (!output) {
    SetError("failed while closing temporary cache file: " +
                 temporary_filename,
             error);
    std::remove(temporary_filename.c_str());
    return false;
  }
  if (std::rename(temporary_filename.c_str(), filename.c_str()) != 0) {
    SetError("failed to atomically replace cache file: " +
                 std::string(std::strerror(errno)),
             error);
    std::remove(temporary_filename.c_str());
    return false;
  }
  return true;
}

}  // namespace map_scan_distance_field
}  // namespace mapping
}  // namespace cartographer

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

#ifndef CARTOGRAPHER_MAPPING_INTERNAL_2D_MAP_SCAN_DISTANCE_FIELD_H_
#define CARTOGRAPHER_MAPPING_INTERNAL_2D_MAP_SCAN_DISTANCE_FIELD_H_

#include <cstddef>
#include <cstdint>
#include <functional>
#include <istream>
#include <ostream>
#include <string>
#include <vector>

namespace cartographer {
namespace mapping {
namespace map_scan_distance_field {

// The V2 payload uses bit 15 for the known-area mask and the low 15 bits for
// a millimetre distance. Cartographer's map-scan gate is intentionally capped
// at one metre, so values above this limit are never serialized.
constexpr uint16_t kKnownCellBit = uint16_t{1} << 15;
constexpr uint16_t kDistanceMillimetersMask = kKnownCellBit - 1;
constexpr uint16_t kMaxStoredDistanceMillimeters = 1000;
constexpr int kDistanceScale = 1000;
constexpr size_t kMaxCellCount = 32000000;

struct Raster {
  double resolution = 0.;
  double origin_x = 0.;
  double origin_y = 0.;
  int width = 0;
  int height = 0;
  int frozen_finished_submap_count = 0;

  // Both vectors are row-major (index = y * width + x). An occupied cell must
  // also be known. Callers that rasterize rotated submaps should classify a
  // global cell by inverse-projecting its center into each source submap.
  std::vector<uint8_t> known;
  std::vector<uint8_t> occupied;
};

struct RasterCell {
  bool known = false;
  bool occupied = false;
};

// A lightweight view of a cropped source grid. The callback samples the cell
// containing a point in the source's local frame, so callers can expose an
// existing submap without copying its probability payload. Local bounds are
// the outer edges of the cropped grid and use [min, max) semantics.
struct RasterSource {
  double resolution = 0.;
  double local_min_x = 0.;
  double local_min_y = 0.;
  double local_max_x = 0.;
  double local_max_y = 0.;
  double global_from_local_translation_x = 0.;
  double global_from_local_translation_y = 0.;
  double global_from_local_rotation = 0.;
  std::function<RasterCell(double local_x, double local_y)> sample;
};

struct Field {
  bool valid = false;
  double resolution = 0.;
  double origin_x = 0.;
  double origin_y = 0.;
  int width = 0;
  int height = 0;
  int frozen_finished_submap_count = 0;
  double max_distance_m = 0.;

  // Row-major V2 cells. See kKnownCellBit and kDistanceMillimetersMask.
  std::vector<uint16_t> cells;
};

struct SampleResult {
  bool in_bounds = false;
  bool known = false;
  float distance_m = 0.f;
};

struct CacheOptions {
  std::string cache_key;
  double max_distance_m = 1.;
  double occupied_probability_threshold = 0.55;
};

enum class CacheFormat {
  kUnknown = 0,
  kV1 = 1,
  kV2 = 2,
};

// Checks positive dimensions, multiplication overflow and kMaxCellCount.
bool CheckedCellCount(int width, int height, size_t* cell_count,
                      std::string* error);

uint16_t PackCell(bool known, uint16_t distance_millimeters);
bool IsKnown(uint16_t cell);
uint16_t DistanceMillimeters(uint16_t cell);

// Rasterizes source grids into a common global grid at the finest source
// resolution. Each target cell center is inverse-projected into every
// overlapping source. This avoids the holes produced by forward-scattering
// rotated source cells. Overlapping known/occupied masks are OR-combined.
bool Rasterize(const std::vector<RasterSource>& sources, double margin_m,
               int frozen_finished_submap_count, Raster* raster,
               std::string* error);

// Builds an exact squared Euclidean distance transform using the separable
// Felzenszwalb-Huttenlocher algorithm. Distances are rounded to millimetres and
// clamped to max_distance_m (which must not exceed one metre).
bool Build(const Raster& raster, double max_distance_m, Field* field,
           std::string* error);

// Samples the cell containing (x, y). Grid indexing deliberately uses floor;
// no interpolation is performed.
SampleResult Sample(const Field& field, double x, double y);

// Stream functions are exposed for deterministic unit tests. Load accepts the
// legacy V1 split payload and the packed V2 payload. Save always writes V2.
bool LoadCache(std::istream* input, const CacheOptions& options, Field* field,
               CacheFormat* format, std::string* error);
bool SaveCacheV2(std::ostream* output, const CacheOptions& options,
                 const Field& field, std::string* error);

// File SaveCacheV2 writes to '<filename>.tmp' and atomically renames it.
bool LoadCache(const std::string& filename, const CacheOptions& options,
               Field* field, CacheFormat* format, std::string* error);
bool SaveCacheV2(const std::string& filename, const CacheOptions& options,
                 const Field& field, std::string* error);

}  // namespace map_scan_distance_field
}  // namespace mapping
}  // namespace cartographer

#endif  // CARTOGRAPHER_MAPPING_INTERNAL_2D_MAP_SCAN_DISTANCE_FIELD_H_

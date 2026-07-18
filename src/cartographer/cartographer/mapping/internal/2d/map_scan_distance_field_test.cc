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

#include <cmath>
#include <cstdint>
#include <limits>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

namespace cartographer {
namespace mapping {
namespace map_scan_distance_field {
namespace {

struct SyntheticGrid {
  double resolution;
  double min_x;
  double min_y;
  int width;
  int height;
  std::vector<uint8_t> known;
  std::vector<uint8_t> occupied;

  SyntheticGrid(double resolution_in, double min_x_in, double min_y_in,
                int width_in, int height_in)
      : resolution(resolution_in),
        min_x(min_x_in),
        min_y(min_y_in),
        width(width_in),
        height(height_in),
        known(static_cast<size_t>(width) * height, 1),
        occupied(static_cast<size_t>(width) * height, 0) {}

  RasterCell Sample(double x, double y) const {
    const int cell_x = static_cast<int>(std::floor((x - min_x) / resolution));
    const int cell_y = static_cast<int>(std::floor((y - min_y) / resolution));
    if (cell_x < 0 || cell_x >= width || cell_y < 0 || cell_y >= height) {
      return RasterCell{};
    }
    const size_t index = static_cast<size_t>(cell_y) * width + cell_x;
    return RasterCell{known[index] != 0, occupied[index] != 0};
  }

  RasterSource Source(double translation_x, double translation_y,
                      double rotation) const {
    RasterSource source;
    source.resolution = resolution;
    source.local_min_x = min_x;
    source.local_min_y = min_y;
    source.local_max_x = min_x + width * resolution;
    source.local_max_y = min_y + height * resolution;
    source.global_from_local_translation_x = translation_x;
    source.global_from_local_translation_y = translation_y;
    source.global_from_local_rotation = rotation;
    source.sample = [this](double x, double y) { return Sample(x, y); };
    return source;
  }

  void SetKnown(int x, int y, bool value) {
    known[static_cast<size_t>(y) * width + x] = value;
    if (!value) {
      occupied[static_cast<size_t>(y) * width + x] = 0;
    }
  }

  void SetOccupied(int x, int y) {
    known[static_cast<size_t>(y) * width + x] = 1;
    occupied[static_cast<size_t>(y) * width + x] = 1;
  }
};

struct AnalyticRasterSample {
  bool inside = false;
  bool known = false;
  bool occupied = false;
};

AnalyticRasterSample SampleSources(
    const std::vector<RasterSource>& sources, double global_x,
    double global_y) {
  AnalyticRasterSample result;
  for (const RasterSource& source : sources) {
    const double cosine = std::cos(source.global_from_local_rotation);
    const double sine = std::sin(source.global_from_local_rotation);
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
    result.inside = true;
    const RasterCell cell = source.sample(local_x, local_y);
    result.known |= cell.known;
    result.occupied |= cell.occupied;
  }
  return result;
}

void ExpectRasterMatchesInverseProjection(
    const std::vector<RasterSource>& sources, const Raster& raster,
    int* interior_unknown_count, int* known_count, int* occupied_count) {
  *interior_unknown_count = 0;
  *known_count = 0;
  *occupied_count = 0;
  for (int y = 0; y < raster.height; ++y) {
    for (int x = 0; x < raster.width; ++x) {
      const double global_x =
          raster.origin_x + (static_cast<double>(x) + 0.5) * raster.resolution;
      const double global_y =
          raster.origin_y + (static_cast<double>(y) + 0.5) * raster.resolution;
      const AnalyticRasterSample expected =
          SampleSources(sources, global_x, global_y);
      const size_t index = static_cast<size_t>(y) * raster.width + x;
      EXPECT_EQ(raster.known[index] != 0, expected.known)
          << "known at target (" << x << ", " << y << ")";
      EXPECT_EQ(raster.occupied[index] != 0, expected.occupied)
          << "occupied at target (" << x << ", " << y << ")";
      if (expected.inside && !expected.known) {
        ++*interior_unknown_count;
      }
      *known_count += expected.known;
      *occupied_count += expected.occupied;
    }
  }
}

void ExpectExactEdtMatchesBruteForce(const Raster& raster,
                                     const Field& field,
                                     double max_distance_m) {
  std::vector<std::pair<int, int>> occupied;
  for (int y = 0; y < raster.height; ++y) {
    for (int x = 0; x < raster.width; ++x) {
      if (raster.occupied[static_cast<size_t>(y) * raster.width + x]) {
        occupied.emplace_back(x, y);
      }
    }
  }
  ASSERT_FALSE(occupied.empty());
  const uint16_t max_distance_mm =
      static_cast<uint16_t>(std::lround(max_distance_m * kDistanceScale));
  for (int y = 0; y < raster.height; ++y) {
    for (int x = 0; x < raster.width; ++x) {
      double best_squared_cells = std::numeric_limits<double>::infinity();
      for (const auto& occupied_cell : occupied) {
        const double dx = x - occupied_cell.first;
        const double dy = y - occupied_cell.second;
        best_squared_cells = std::min(best_squared_cells, dx * dx + dy * dy);
      }
      const uint16_t expected_mm = static_cast<uint16_t>(std::min<long>(
          max_distance_mm,
          std::lround(std::sqrt(best_squared_cells) * raster.resolution *
                      kDistanceScale)));
      const size_t index = static_cast<size_t>(y) * raster.width + x;
      EXPECT_EQ(DistanceMillimeters(field.cells[index]), expected_mm)
          << "distance at target (" << x << ", " << y << ")";
      EXPECT_EQ(IsKnown(field.cells[index]), raster.known[index] != 0)
          << "known mask at target (" << x << ", " << y << ")";
    }
  }
}

CacheOptions TestCacheOptions() {
  CacheOptions options;
  options.cache_key = "md5:test:size:123";
  options.max_distance_m = 1.;
  options.occupied_probability_threshold = 0.55;
  return options;
}

std::string V2Header(int width, int height,
                     const std::string& encoding =
                         "uint16_known_bit15_distance_mm") {
  std::ostringstream output;
  output << "DORAMON_MAP_SCAN_DF_V2\n"
         << "version 2\n"
         << "cache_key md5:test:size:123\n"
         << "resolution 0.05\n"
         << "origin_x -1\n"
         << "origin_y 2\n"
         << "width " << width << "\n"
         << "height " << height << "\n"
         << "submap_count 3\n"
         << "max_distance_m 1\n"
         << "occupied_probability_threshold 0.55\n"
         << "distance_scale 1000\n"
         << "encoding " << encoding << "\n"
         << "END_HEADER\n";
  return output.str();
}

TEST(MapScanDistanceFieldTest,
     InverseProjectionAndExactEdtMatchOracleAtRequiredRotations) {
  constexpr double kDegreesToRadians =
      3.14159265358979323846 / 180.;
  for (const double angle_degrees : {0., 17., 45.}) {
    SyntheticGrid grid(0.1, -0.6, -0.5, 12, 10);
    for (const std::pair<int, int> cell :
         {std::make_pair(4, 4), std::make_pair(5, 4),
          std::make_pair(4, 5), std::make_pair(5, 5)}) {
      grid.SetKnown(cell.first, cell.second, false);
    }
    for (const std::pair<int, int> cell :
         {std::make_pair(1, 1), std::make_pair(10, 2),
          std::make_pair(2, 8), std::make_pair(8, 7)}) {
      grid.SetOccupied(cell.first, cell.second);
    }

    const std::vector<RasterSource> sources = {
        grid.Source(0.23, -0.17, angle_degrees * kDegreesToRadians)};
    Raster raster;
    std::string error;
    ASSERT_TRUE(Rasterize(sources, 0.25, 1, &raster, &error))
        << "angle=" << angle_degrees << ": " << error;
    EXPECT_DOUBLE_EQ(raster.resolution, grid.resolution);
    EXPECT_EQ(raster.frozen_finished_submap_count, 1);

    int interior_unknown_count = 0;
    int known_count = 0;
    int occupied_count = 0;
    ExpectRasterMatchesInverseProjection(
        sources, raster, &interior_unknown_count, &known_count,
        &occupied_count);
    EXPECT_GT(interior_unknown_count, 0) << "angle=" << angle_degrees;
    EXPECT_GT(known_count, 0) << "angle=" << angle_degrees;
    EXPECT_GT(occupied_count, 0) << "angle=" << angle_degrees;

    Field field;
    ASSERT_TRUE(Build(raster, 1., &field, &error))
        << "angle=" << angle_degrees << ": " << error;
    ExpectExactEdtMatchesBruteForce(raster, field, 1.);
  }
}

TEST(MapScanDistanceFieldTest,
     OverlappingDifferentResolutionSourcesOrMasksAndMatchEdtOracle) {
  SyntheticGrid coarse(0.2, -0.6, -0.4, 6, 4);
  coarse.SetKnown(2, 1, false);
  coarse.SetKnown(3, 1, false);
  coarse.SetOccupied(0, 0);

  SyntheticGrid fine(0.1, -0.4, -0.3, 8, 6);
  fine.SetKnown(1, 4, false);
  fine.SetKnown(2, 4, false);
  fine.SetOccupied(6, 4);

  const std::vector<RasterSource> sources = {
      coarse.Source(0., 0., 0.), fine.Source(0.1, 0.05, 0.)};
  Raster raster;
  std::string error;
  ASSERT_TRUE(Rasterize(sources, 0.2, 2, &raster, &error)) << error;
  EXPECT_DOUBLE_EQ(raster.resolution, 0.1);
  EXPECT_EQ(raster.frozen_finished_submap_count, 2);

  int interior_unknown_count = 0;
  int known_count = 0;
  int occupied_count = 0;
  ExpectRasterMatchesInverseProjection(
      sources, raster, &interior_unknown_count, &known_count,
      &occupied_count);
  EXPECT_GT(known_count, 0);
  EXPECT_GT(occupied_count, 0);

  int rescued_known_count = 0;
  for (int y = 0; y < raster.height; ++y) {
    for (int x = 0; x < raster.width; ++x) {
      const double global_x =
          raster.origin_x + (static_cast<double>(x) + 0.5) * raster.resolution;
      const double global_y =
          raster.origin_y + (static_cast<double>(y) + 0.5) * raster.resolution;
      const AnalyticRasterSample coarse_sample =
          SampleSources({sources[0]}, global_x, global_y);
      const AnalyticRasterSample fine_sample =
          SampleSources({sources[1]}, global_x, global_y);
      if (coarse_sample.inside && !coarse_sample.known &&
          fine_sample.known) {
        const size_t index = static_cast<size_t>(y) * raster.width + x;
        EXPECT_TRUE(raster.known[index]);
        ++rescued_known_count;
      }
    }
  }
  EXPECT_GT(rescued_known_count, 0);

  Field field;
  ASSERT_TRUE(Build(raster, 1., &field, &error)) << error;
  ExpectExactEdtMatchesBruteForce(raster, field, 1.);
}

TEST(MapScanDistanceFieldTest, ExactEdtMatchesBruteForceOracle) {
  Raster raster;
  raster.resolution = 0.073;
  raster.origin_x = -0.4;
  raster.origin_y = 1.2;
  raster.width = 8;
  raster.height = 7;
  raster.frozen_finished_submap_count = 2;
  const size_t cell_count = raster.width * raster.height;
  raster.known.assign(cell_count, 1);
  raster.occupied.assign(cell_count, 0);
  const std::vector<std::pair<int, int>> occupied = {{1, 1}, {6, 4}};
  for (const auto& cell : occupied) {
    raster.occupied[cell.second * raster.width + cell.first] = 1;
  }
  // An unknown hole still carries a distance but must sample as unknown.
  raster.known[3 * raster.width + 4] = 0;

  Field field;
  std::string error;
  ASSERT_TRUE(Build(raster, 1., &field, &error)) << error;
  ASSERT_TRUE(field.valid);
  ASSERT_EQ(field.cells.size(), cell_count);
  for (int y = 0; y < raster.height; ++y) {
    for (int x = 0; x < raster.width; ++x) {
      double best_squared_cells = std::numeric_limits<double>::infinity();
      for (const auto& occupied_cell : occupied) {
        const double dx = x - occupied_cell.first;
        const double dy = y - occupied_cell.second;
        best_squared_cells =
            std::min(best_squared_cells, dx * dx + dy * dy);
      }
      const uint16_t expected_mm = static_cast<uint16_t>(std::lround(
          std::sqrt(best_squared_cells) * raster.resolution * kDistanceScale));
      const uint16_t packed = field.cells[y * raster.width + x];
      EXPECT_EQ(DistanceMillimeters(packed), expected_mm)
          << "at (" << x << ", " << y << ")";
      EXPECT_EQ(IsKnown(packed), raster.known[y * raster.width + x] != 0);
    }
  }
}

TEST(MapScanDistanceFieldTest, EdtClampsAtConfiguredMaximum) {
  Raster raster;
  raster.resolution = 0.1;
  raster.width = 20;
  raster.height = 1;
  raster.frozen_finished_submap_count = 1;
  raster.known.assign(20, 1);
  raster.occupied.assign(20, 0);
  raster.occupied.front() = 1;

  Field field;
  std::string error;
  ASSERT_TRUE(Build(raster, 0.4, &field, &error)) << error;
  EXPECT_EQ(DistanceMillimeters(field.cells[3]), 300);
  EXPECT_EQ(DistanceMillimeters(field.cells[4]), 400);
  EXPECT_EQ(DistanceMillimeters(field.cells[19]), 400);
}

TEST(MapScanDistanceFieldTest, SampleUsesFloorAndPreservesKnownMask) {
  Field field;
  field.valid = true;
  field.resolution = 0.5;
  field.origin_x = -1.;
  field.origin_y = 2.;
  field.width = 2;
  field.height = 2;
  field.frozen_finished_submap_count = 1;
  field.max_distance_m = 1.;
  field.cells = {PackCell(true, 100), PackCell(false, 200),
                 PackCell(true, 300), PackCell(true, 400)};

  SampleResult sample = Sample(field, -0.500001, 2.499999);
  EXPECT_TRUE(sample.in_bounds);
  EXPECT_TRUE(sample.known);
  EXPECT_FLOAT_EQ(sample.distance_m, 0.1f);

  sample = Sample(field, -0.5, 2.499999);
  EXPECT_TRUE(sample.in_bounds);
  EXPECT_FALSE(sample.known);
  EXPECT_FLOAT_EQ(sample.distance_m, 0.2f);

  sample = Sample(field, -1.000001, 2.);
  EXPECT_FALSE(sample.in_bounds);
  sample = Sample(field, 0., 2.);
  EXPECT_FALSE(sample.in_bounds);
}

TEST(MapScanDistanceFieldTest, V2RoundTripPreservesPackedCellsAndMetadata) {
  Field source;
  source.valid = true;
  source.resolution = 0.05;
  source.origin_x = -4.25;
  source.origin_y = 8.5;
  source.width = 3;
  source.height = 2;
  source.frozen_finished_submap_count = 17;
  source.max_distance_m = 1.;
  source.cells = {PackCell(true, 0),   PackCell(false, 27),
                  PackCell(true, 200), PackCell(true, 999),
                  PackCell(false, 1),  PackCell(true, 1000)};

  std::stringstream stream(std::ios::in | std::ios::out | std::ios::binary);
  std::string error;
  ASSERT_TRUE(SaveCacheV2(&stream, TestCacheOptions(), source, &error))
      << error;
  EXPECT_EQ(stream.str().find("DORAMON_MAP_SCAN_DF_V2\n"), 0u);
  stream.seekg(0);

  Field loaded;
  CacheFormat format = CacheFormat::kUnknown;
  ASSERT_TRUE(
      LoadCache(&stream, TestCacheOptions(), &loaded, &format, &error))
      << error;
  EXPECT_EQ(format, CacheFormat::kV2);
  EXPECT_TRUE(loaded.valid);
  EXPECT_DOUBLE_EQ(loaded.resolution, source.resolution);
  EXPECT_DOUBLE_EQ(loaded.origin_x, source.origin_x);
  EXPECT_DOUBLE_EQ(loaded.origin_y, source.origin_y);
  EXPECT_EQ(loaded.width, source.width);
  EXPECT_EQ(loaded.height, source.height);
  EXPECT_EQ(loaded.frozen_finished_submap_count,
            source.frozen_finished_submap_count);
  EXPECT_EQ(loaded.cells, source.cells);
}

TEST(MapScanDistanceFieldTest, LoadsExistingV1SplitPayload) {
  std::stringstream stream(std::ios::in | std::ios::out | std::ios::binary);
  stream << "DORAMON_MAP_SCAN_DF_V1\n"
         << "version 1\n"
         << "cache_key md5:test:size:123\n"
         << "resolution 0.05\n"
         << "origin_x -1.25\n"
         << "origin_y 2.5\n"
         << "width 3\n"
         << "height 2\n"
         << "submap_count 4\n"
         << "max_distance_m 1\n"
         << "hit_radius_m 0.2\n"
         << "occupied_probability_threshold 0.55\n"
         << "distance_scale 1000\n"
         << "encoding uint16_mm_then_uint8_known\n"
         << "END_HEADER\n";
  const std::vector<uint16_t> distances = {0, 17, 200, 555, 999, 1000};
  const std::vector<uint8_t> known = {1, 0, 1, 1, 0, 1};
  stream.write(reinterpret_cast<const char*>(distances.data()),
               distances.size() * sizeof(uint16_t));
  stream.write(reinterpret_cast<const char*>(known.data()), known.size());
  stream.seekg(0);

  Field loaded;
  CacheFormat format = CacheFormat::kUnknown;
  std::string error;
  ASSERT_TRUE(
      LoadCache(&stream, TestCacheOptions(), &loaded, &format, &error))
      << error;
  EXPECT_EQ(format, CacheFormat::kV1);
  ASSERT_EQ(loaded.cells.size(), distances.size());
  for (size_t i = 0; i < distances.size(); ++i) {
    EXPECT_EQ(DistanceMillimeters(loaded.cells[i]), distances[i]);
    EXPECT_EQ(IsKnown(loaded.cells[i]), known[i] != 0);
  }
}

TEST(MapScanDistanceFieldTest, RejectsTruncatedAndTrailingPayloads) {
  Field source;
  source.valid = true;
  source.resolution = 0.05;
  source.width = 2;
  source.height = 1;
  source.frozen_finished_submap_count = 1;
  source.max_distance_m = 1.;
  source.cells = {PackCell(true, 5), PackCell(false, 1000)};
  std::stringstream output(std::ios::in | std::ios::out | std::ios::binary);
  std::string error;
  ASSERT_TRUE(SaveCacheV2(&output, TestCacheOptions(), source, &error))
      << error;
  const std::string serialized = output.str();

  for (const std::string invalid :
       {serialized.substr(0, serialized.size() - 1), serialized + "x"}) {
    std::istringstream input(invalid, std::ios::binary);
    Field loaded;
    CacheFormat format = CacheFormat::kUnknown;
    EXPECT_FALSE(
        LoadCache(&input, TestCacheOptions(), &loaded, &format, &error));
    EXPECT_FALSE(loaded.valid);
    EXPECT_EQ(format, CacheFormat::kUnknown);
  }
}

TEST(MapScanDistanceFieldTest, RejectsMaliciousDimensionsBeforeAllocation) {
  for (const std::pair<int, int> dimensions :
       {std::make_pair(32000001, 1),
        std::make_pair(std::numeric_limits<int>::max(),
                       std::numeric_limits<int>::max())}) {
    std::istringstream input(V2Header(dimensions.first, dimensions.second),
                             std::ios::binary);
    Field loaded;
    CacheFormat format = CacheFormat::kUnknown;
    std::string error;
    EXPECT_FALSE(
        LoadCache(&input, TestCacheOptions(), &loaded, &format, &error));
    EXPECT_NE(error.find("32,000,000 cell limit"), std::string::npos)
        << error;
    EXPECT_TRUE(loaded.cells.empty());
  }
}

TEST(MapScanDistanceFieldTest, RejectsWrongEncodingAndParameterMismatch) {
  {
    std::istringstream input(V2Header(1, 1, "uint16_mm_then_uint8_known"),
                             std::ios::binary);
    Field loaded;
    CacheFormat format = CacheFormat::kUnknown;
    std::string error;
    EXPECT_FALSE(
        LoadCache(&input, TestCacheOptions(), &loaded, &format, &error));
  }
  {
    std::string bytes = V2Header(1, 1);
    const uint16_t cell = PackCell(true, 0);
    bytes.append(reinterpret_cast<const char*>(&cell), sizeof(cell));
    std::istringstream input(bytes, std::ios::binary);
    CacheOptions mismatched = TestCacheOptions();
    mismatched.occupied_probability_threshold = 0.6;
    Field loaded;
    CacheFormat format = CacheFormat::kUnknown;
    std::string error;
    EXPECT_FALSE(LoadCache(&input, mismatched, &loaded, &format, &error));
  }
}

TEST(MapScanDistanceFieldTest, RejectsInvalidRasterMasksAndNoObstacles) {
  Raster raster;
  raster.resolution = 0.05;
  raster.width = 2;
  raster.height = 1;
  raster.known = {1, 0};
  raster.occupied = {0, 1};
  Field field;
  std::string error;
  EXPECT_FALSE(Build(raster, 1., &field, &error));

  raster.known = {1, 1};
  raster.occupied = {0, 0};
  EXPECT_FALSE(Build(raster, 1., &field, &error));
}

}  // namespace
}  // namespace map_scan_distance_field
}  // namespace mapping
}  // namespace cartographer

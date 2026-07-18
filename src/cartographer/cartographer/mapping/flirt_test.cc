#include "cartographer/mapping/flirt.h"

#include <cstddef>
#include <vector>

#include "gtest/gtest.h"

namespace flirt {
namespace {

TEST(FeatureSetTest, SparseReadingsPublishComputedEmptyFeatureSet) {
  // This intentionally does not call flirt::init(). Sparse readings must be
  // handled before touching the third-party detector.
  for (std::size_t point_count = 0; point_count < 5; ++point_count) {
    const std::vector<double> phi(point_count, 0.0);
    const std::vector<double> rho(point_count, 1.0);
    LaserReading reading(phi, rho);
    reading.setLaserPose({0.0, 0.0, 0.0});

    const auto features = BuildFeatureSet(reading);
    ASSERT_NE(features, nullptr) << "point_count=" << point_count;
    EXPECT_TRUE(features->empty()) << "point_count=" << point_count;
  }
}

}  // namespace
}  // namespace flirt

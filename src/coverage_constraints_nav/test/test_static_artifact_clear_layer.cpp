#include <gtest/gtest.h>

#include <costmap_2d/cost_values.h>

#include "coverage_constraints_nav/static_artifact_clear_layer.h"

namespace coverage_constraints_nav {
namespace {

geometry_msgs::Point pointAt(double x, double y) {
  geometry_msgs::Point point;
  point.x = x;
  point.y = y;
  point.z = 0.0;
  return point;
}

TEST(StaticArtifactClearLayerTest, ClearsOnlyTheConfiguredLethalCell) {
  costmap_2d::Costmap2D grid(20u, 20u, 0.05, -0.5, -0.5, costmap_2d::FREE_SPACE);
  unsigned int target_x = 0u;
  unsigned int target_y = 0u;
  unsigned int adjacent_x = 0u;
  unsigned int adjacent_y = 0u;
  ASSERT_TRUE(grid.worldToMap(0.025, 0.025, target_x, target_y));
  ASSERT_TRUE(grid.worldToMap(0.075, 0.025, adjacent_x, adjacent_y));
  grid.setCost(target_x, target_y, costmap_2d::LETHAL_OBSTACLE);
  grid.setCost(adjacent_x, adjacent_y, costmap_2d::LETHAL_OBSTACLE);

  EXPECT_EQ(
      clearLethalArtifactCells(
          &grid, {pointAt(0.025, 0.025)}, 0, 0, 20, 20),
      1u);
  EXPECT_EQ(grid.getCost(target_x, target_y), costmap_2d::FREE_SPACE);
  EXPECT_EQ(grid.getCost(adjacent_x, adjacent_y), costmap_2d::LETHAL_OBSTACLE);
}

TEST(StaticArtifactClearLayerTest, DoesNotClearUnknownOrSoftCosts) {
  costmap_2d::Costmap2D grid(20u, 20u, 0.05, -0.5, -0.5, costmap_2d::FREE_SPACE);
  unsigned int unknown_x = 0u;
  unsigned int unknown_y = 0u;
  unsigned int soft_x = 0u;
  unsigned int soft_y = 0u;
  ASSERT_TRUE(grid.worldToMap(0.025, 0.025, unknown_x, unknown_y));
  ASSERT_TRUE(grid.worldToMap(0.075, 0.025, soft_x, soft_y));
  grid.setCost(unknown_x, unknown_y, costmap_2d::NO_INFORMATION);
  grid.setCost(soft_x, soft_y, costmap_2d::INSCRIBED_INFLATED_OBSTACLE);

  EXPECT_EQ(
      clearLethalArtifactCells(
          &grid,
          {pointAt(0.025, 0.025), pointAt(0.075, 0.025)},
          0, 0, 20, 20),
      0u);
  EXPECT_EQ(grid.getCost(unknown_x, unknown_y), costmap_2d::NO_INFORMATION);
  EXPECT_EQ(
      grid.getCost(soft_x, soft_y),
      costmap_2d::INSCRIBED_INFLATED_OBSTACLE);
}

TEST(StaticArtifactClearLayerTest, RespectsTheLayeredCostmapUpdateWindow) {
  costmap_2d::Costmap2D grid(20u, 20u, 0.05, -0.5, -0.5, costmap_2d::FREE_SPACE);
  unsigned int target_x = 0u;
  unsigned int target_y = 0u;
  ASSERT_TRUE(grid.worldToMap(0.025, 0.025, target_x, target_y));
  grid.setCost(target_x, target_y, costmap_2d::LETHAL_OBSTACLE);

  EXPECT_EQ(
      clearLethalArtifactCells(
          &grid, {pointAt(0.025, 0.025)}, 0, 0, 5, 5),
      0u);
  EXPECT_EQ(grid.getCost(target_x, target_y), costmap_2d::LETHAL_OBSTACLE);
}

}  // namespace
}  // namespace coverage_constraints_nav

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

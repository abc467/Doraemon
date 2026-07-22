#include <gtest/gtest.h>

#include <limits>
#include <stdexcept>

#include <mcore_chassis_bridge/velocity_command_units.h>

namespace {

using mcore_chassis_bridge::ToProtocolVelocity;

TEST(VelocityCommandUnits, PreservesProtocolRawOutputBelowSiLimit) {
  EXPECT_DOUBLE_EQ(200.0, ToProtocolVelocity(0.2, 1.0, 0.35, 1000.0));
  EXPECT_DOUBLE_EQ(34.9, ToProtocolVelocity(0.0349, 1.0, 0.3, 1000.0));
}

TEST(VelocityCommandUnits, ClampsInSiBeforeProtocolScaling) {
  EXPECT_DOUBLE_EQ(350.0, ToProtocolVelocity(0.8, 1.0, 0.35, 1000.0));
  EXPECT_DOUBLE_EQ(-300.0, ToProtocolVelocity(-0.9, 1.0, 0.3, 1000.0));
}

TEST(VelocityCommandUnits, AppliesConfiguredSignBeforeSiClamp) {
  EXPECT_DOUBLE_EQ(-350.0, ToProtocolVelocity(0.8, -1.0, 0.35, 1000.0));
  EXPECT_DOUBLE_EQ(300.0, ToProtocolVelocity(-0.9, -1.0, 0.3, 1000.0));
}

TEST(VelocityCommandUnits, ZeroLimitKeepsLegacyUnlimitedMeaning) {
  EXPECT_DOUBLE_EQ(-420.0, ToProtocolVelocity(0.42, -1.0, 0.0, 1000.0));
}

TEST(VelocityCommandUnits, RejectsInvalidConfiguration) {
  const double nan = std::numeric_limits<double>::quiet_NaN();
  const double inf = std::numeric_limits<double>::infinity();

  EXPECT_THROW(ToProtocolVelocity(0.2, 0.0, 0.35, 1000.0), std::invalid_argument);
  EXPECT_THROW(ToProtocolVelocity(0.2, 0.5, 0.35, 1000.0), std::invalid_argument);
  EXPECT_THROW(ToProtocolVelocity(0.2, 1.0, -0.35, 1000.0), std::invalid_argument);
  EXPECT_THROW(ToProtocolVelocity(0.2, 1.0, nan, 1000.0), std::invalid_argument);
  EXPECT_THROW(ToProtocolVelocity(0.2, 1.0, 0.35, 0.0), std::invalid_argument);
  EXPECT_THROW(ToProtocolVelocity(0.2, 1.0, 0.35, inf), std::invalid_argument);
  EXPECT_THROW(ToProtocolVelocity(0.2, 1.0, 1.0e308, 1.0e308),
               std::invalid_argument);
}

TEST(VelocityCommandUnits, RejectsNonFiniteCommand) {
  const double nan = std::numeric_limits<double>::quiet_NaN();
  const double inf = std::numeric_limits<double>::infinity();

  EXPECT_THROW(ToProtocolVelocity(nan, 1.0, 0.35, 1000.0), std::invalid_argument);
  EXPECT_THROW(ToProtocolVelocity(inf, 1.0, 0.35, 1000.0), std::invalid_argument);
  EXPECT_THROW(ToProtocolVelocity(1.0e308, 1.0, 0.0, 1.0e308),
               std::overflow_error);
}

}  // namespace

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

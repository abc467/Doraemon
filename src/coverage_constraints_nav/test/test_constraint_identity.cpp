#include <gtest/gtest.h>

#include <coverage_constraints_nav/keepout_constraint_layer.h>

namespace coverage_constraints_nav {
namespace {

coverage_msgs::MapConstraints validSnapshot() {
  coverage_msgs::MapConstraints msg;
  msg.valid = true;
  msg.map_id = "canonical_map";
  msg.map_md5 = "canonical_md5";
  msg.map_revision_id = "revision_42";
  msg.runtime_map_id = "runtime_map";
  msg.runtime_map_md5 = "runtime_md5";
  return msg;
}

TEST(ConstraintIdentity, MatchingRevisionIsAuthoritative) {
  auto msg = validSnapshot();
  std::string reason;
  EXPECT_TRUE(constraintIdentityMatchesRuntime(
      msg, "revision_42", "different_runtime_id", "different_runtime_md5", &reason));
  EXPECT_TRUE(reason.empty());
}

TEST(ConstraintIdentity, RejectsWrongRevisionWithoutFallingBack) {
  const auto msg = validSnapshot();
  std::string reason;
  EXPECT_FALSE(constraintIdentityMatchesRuntime(
      msg, "revision_other", "runtime_map", "runtime_md5", &reason));
  EXPECT_NE(reason.find("revision mismatch"), std::string::npos);
}

TEST(ConstraintIdentity, UsesRuntimeMd5WhenRevisionIsUnavailable) {
  auto msg = validSnapshot();
  msg.map_revision_id.clear();
  EXPECT_TRUE(constraintIdentityMatchesRuntime(
      msg, "", "different_id", "runtime_md5", nullptr));
  EXPECT_FALSE(constraintIdentityMatchesRuntime(
      msg, "", "runtime_map", "other_md5", nullptr));
}

TEST(ConstraintIdentity, InvalidTombstoneAlwaysFailsClosed) {
  auto msg = validSnapshot();
  msg.valid = false;
  msg.invalid_reason = "map transition";
  std::string reason;
  EXPECT_FALSE(constraintIdentityMatchesRuntime(
      msg, "revision_42", "runtime_map", "runtime_md5", &reason));
  EXPECT_NE(reason.find("map transition"), std::string::npos);
}

TEST(ConstraintIdentity, MissingRuntimeIdentityFailsClosed) {
  const auto msg = validSnapshot();
  std::string reason;
  EXPECT_FALSE(constraintIdentityMatchesRuntime(msg, "", "", "", &reason));
  EXPECT_NE(reason.find("not ready"), std::string::npos);
}

}  // namespace
}  // namespace coverage_constraints_nav

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

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

#include "cartographer/mapping/localization_health.h"

#include <thread>
#include <vector>

#include "gtest/gtest.h"

namespace cartographer {
namespace mapping {
namespace {

TEST(LocalizationHealthTest, AutomaticRelocationCompatibilityKeysStayZero) {
  ResetLocalizationHealth();

  RecordLocalizationHealthAutoRelocationTrigger(17);
  RecordLocalizationHealthAutoRelocationResult(42, true);

  const LocalizationHealthSnapshot snapshot =
      GetLocalizationHealthSnapshot();
  EXPECT_FALSE(snapshot.observed);
  EXPECT_FALSE(snapshot.automatic_relocation_enabled);
  EXPECT_EQ(snapshot.auto_relocation_trigger_count, 0);
  EXPECT_EQ(snapshot.auto_relocation_result_count, 0);
  EXPECT_EQ(snapshot.auto_relocation_success_count, 0);
  EXPECT_EQ(snapshot.latest_auto_relocation_queue_size, -1);
  EXPECT_EQ(snapshot.max_auto_relocation_queue_size, -1);
  EXPECT_EQ(snapshot.latest_auto_relocation_return_code, 0);
  EXPECT_EQ(snapshot.total_auto_relocation_trigger_count, 0);
  EXPECT_EQ(snapshot.total_auto_relocation_success_count, 0);

  EXPECT_EQ(snapshot.map_scan_distance_field_source, "unavailable");
  EXPECT_EQ(snapshot.map_scan_distance_field_generation, 0);
  EXPECT_EQ(snapshot.map_scan_distance_field_cells, 0u);
  EXPECT_EQ(snapshot.map_scan_distance_field_resident_bytes, 0u);
  EXPECT_EQ(snapshot.map_scan_distance_field_load_count, 0);
  EXPECT_EQ(snapshot.map_scan_distance_field_build_count, 0);
  EXPECT_EQ(snapshot.map_scan_distance_field_invalidation_count, 0);
  EXPECT_EQ(snapshot.map_scan_distance_field_query_count, 0);
}

TEST(LocalizationHealthTest, TracksDistanceFieldLifecycleAndTimings) {
  ResetLocalizationHealth();

  RecordLocalizationHealthMapScanDistanceFieldInvalidation(
      7, "map_revision_switch");
  RecordLocalizationHealthMapScanDistanceFieldLoad(
      false, "cache_v2", 7, 100, 200, -4.0);
  RecordLocalizationHealthMapScanDistanceFieldLoad(
      true, "cache_v1", 7, 1024, 2048, 4.25);

  // A successfully computed but stale result is not published and must not
  // replace the current cache-backed field diagnostics.
  RecordLocalizationHealthMapScanDistanceFieldBuild(
      true, false, "cold_build", 6, 4096, 8192, 12.5,
      "stale_generation");
  LocalizationHealthSnapshot snapshot = GetLocalizationHealthSnapshot();
  EXPECT_TRUE(snapshot.observed);
  EXPECT_EQ(snapshot.map_scan_distance_field_source, "cache_v1");
  EXPECT_EQ(snapshot.map_scan_distance_field_generation, 7);
  EXPECT_EQ(snapshot.map_scan_distance_field_cells, 1024u);
  EXPECT_EQ(snapshot.map_scan_distance_field_resident_bytes, 2048u);
  EXPECT_EQ(snapshot.map_scan_distance_field_load_count, 2);
  EXPECT_EQ(snapshot.map_scan_distance_field_load_failure_count, 1);
  EXPECT_EQ(snapshot.map_scan_distance_field_last_load_source, "cache_v1");
  EXPECT_EQ(snapshot.map_scan_distance_field_last_load_result, "success");
  EXPECT_DOUBLE_EQ(snapshot.map_scan_distance_field_last_load_duration_ms,
                   4.25);
  EXPECT_EQ(snapshot.map_scan_distance_field_build_count, 1);
  EXPECT_EQ(snapshot.map_scan_distance_field_build_failure_count, 0);
  EXPECT_EQ(snapshot.map_scan_distance_field_build_published_count, 0);
  EXPECT_EQ(snapshot.map_scan_distance_field_build_unpublished_count, 1);
  EXPECT_EQ(snapshot.map_scan_distance_field_last_build_result,
            "stale_generation");
  EXPECT_FALSE(snapshot.map_scan_distance_field_last_build_published);
  EXPECT_DOUBLE_EQ(snapshot.map_scan_distance_field_last_build_duration_ms,
                   12.5);
  EXPECT_EQ(snapshot.map_scan_distance_field_invalidation_count, 1);
  EXPECT_EQ(snapshot.map_scan_distance_field_last_invalidation_reason,
            "map_revision_switch");

  RecordLocalizationHealthMapScanDistanceFieldBuild(
      false, false, "cold_build", 7, 0, 0, 6.0, "cell_limit_exceeded");
  RecordLocalizationHealthMapScanDistanceFieldBuild(
      true, true, "cold_build", 8, 2048, 4096, 8.75, "published");
  RecordLocalizationHealthMapScanDistanceFieldQuery(0.1);
  RecordLocalizationHealthMapScanDistanceFieldQuery(0.3);
  RecordLocalizationHealthMapScanDistanceFieldQuery(-1.0);

  snapshot = GetLocalizationHealthSnapshot();
  EXPECT_EQ(snapshot.map_scan_distance_field_source, "cold_build");
  EXPECT_EQ(snapshot.map_scan_distance_field_generation, 8);
  EXPECT_EQ(snapshot.map_scan_distance_field_cells, 2048u);
  EXPECT_EQ(snapshot.map_scan_distance_field_resident_bytes, 4096u);
  EXPECT_EQ(snapshot.map_scan_distance_field_build_count, 3);
  EXPECT_EQ(snapshot.map_scan_distance_field_build_failure_count, 1);
  EXPECT_EQ(snapshot.map_scan_distance_field_build_published_count, 1);
  EXPECT_EQ(snapshot.map_scan_distance_field_build_unpublished_count, 2);
  EXPECT_EQ(snapshot.map_scan_distance_field_last_build_source, "cold_build");
  EXPECT_EQ(snapshot.map_scan_distance_field_last_build_result, "published");
  EXPECT_TRUE(snapshot.map_scan_distance_field_last_build_published);
  EXPECT_DOUBLE_EQ(snapshot.map_scan_distance_field_last_build_duration_ms,
                   8.75);
  EXPECT_EQ(snapshot.map_scan_distance_field_query_count, 3);
  EXPECT_DOUBLE_EQ(snapshot.map_scan_distance_field_last_query_duration_ms,
                   0.0);
  EXPECT_NEAR(snapshot.map_scan_distance_field_mean_query_duration_ms,
              0.4 / 3.0, 1e-12);
  EXPECT_DOUBLE_EQ(snapshot.map_scan_distance_field_max_query_duration_ms,
                   0.3);

  RecordLocalizationHealthMapScanDistanceFieldInvalidation(9,
                                                            "frozen_submap_trim");
  snapshot = GetLocalizationHealthSnapshot();
  EXPECT_EQ(snapshot.map_scan_distance_field_source, "unavailable");
  EXPECT_EQ(snapshot.map_scan_distance_field_generation, 9);
  EXPECT_EQ(snapshot.map_scan_distance_field_cells, 0u);
  EXPECT_EQ(snapshot.map_scan_distance_field_resident_bytes, 0u);
  EXPECT_EQ(snapshot.map_scan_distance_field_invalidation_count, 2);
  EXPECT_EQ(snapshot.map_scan_distance_field_last_invalidation_reason,
            "frozen_submap_trim");
}

TEST(LocalizationHealthTest,
     ConfirmedLossIsLatchedUntilExplicitRelocationSucceeds) {
  ResetLocalizationHealth();

  RecordLocalizationHealthRecoveryState(
      "LOST_CONFIRMED", "confirmed_lost_recovery_failed");
  RecordLocalizationHealthRecoveryState("OK", "ordinary_constraint");
  RecordLocalizationHealthRecoveryFullSearch(12, "another_trajectory");

  LocalizationHealthSnapshot snapshot = GetLocalizationHealthSnapshot();
  EXPECT_TRUE(snapshot.localization_lost_confirmed);
  EXPECT_EQ(snapshot.recovery_state, "LOST_CONFIRMED");
  EXPECT_EQ(snapshot.recovery_reason, "confirmed_lost_recovery_failed");
  EXPECT_EQ(snapshot.localization_loss_episode, 1);
  EXPECT_EQ(snapshot.total_localization_loss_count, 1);
  EXPECT_EQ(snapshot.recovery_full_search_count, 1);

  // Re-reporting the same global loss episode must not allocate a new episode.
  RecordLocalizationHealthRecoveryState("LOST_CONFIRMED", "still_lost");
  snapshot = GetLocalizationHealthSnapshot();
  EXPECT_EQ(snapshot.localization_loss_episode, 1);
  EXPECT_EQ(snapshot.total_localization_loss_count, 1);

  RecordLocalizationHealthExplicitRelocationSuccess(
      "explicit_relocation_waiting_constraint");
  snapshot = GetLocalizationHealthSnapshot();
  EXPECT_FALSE(snapshot.localization_lost_confirmed);
  EXPECT_EQ(snapshot.recovery_state, "DEGRADED");
  EXPECT_EQ(snapshot.recovery_reason,
            "explicit_relocation_waiting_constraint");

  RecordLocalizationHealthRecoveryState(
      "LOST_CONFIRMED", "new_confirmed_loss");
  snapshot = GetLocalizationHealthSnapshot();
  EXPECT_TRUE(snapshot.localization_lost_confirmed);
  EXPECT_EQ(snapshot.localization_loss_episode, 2);
  EXPECT_EQ(snapshot.total_localization_loss_count, 2);
}

TEST(LocalizationHealthTest, QueryAccountingIsThreadSafe) {
  ResetLocalizationHealth();
  constexpr int kThreads = 4;
  constexpr int kQueriesPerThread = 250;
  std::vector<std::thread> threads;
  for (int thread_index = 0; thread_index < kThreads; ++thread_index) {
    threads.emplace_back([]() {
      for (int i = 0; i < kQueriesPerThread; ++i) {
        RecordLocalizationHealthMapScanDistanceFieldQuery(1.0);
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }

  const LocalizationHealthSnapshot snapshot =
      GetLocalizationHealthSnapshot();
  EXPECT_EQ(snapshot.map_scan_distance_field_query_count,
            kThreads * kQueriesPerThread);
  EXPECT_DOUBLE_EQ(snapshot.map_scan_distance_field_last_query_duration_ms,
                   1.0);
  EXPECT_DOUBLE_EQ(snapshot.map_scan_distance_field_mean_query_duration_ms,
                   1.0);
  EXPECT_DOUBLE_EQ(snapshot.map_scan_distance_field_max_query_duration_ms,
                   1.0);
}

}  // namespace
}  // namespace mapping
}  // namespace cartographer

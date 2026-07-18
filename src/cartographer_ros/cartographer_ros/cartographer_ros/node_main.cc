/*
 * Copyright 2016 The Cartographer Authors
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

#include <chrono>
#include <iomanip>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
// #include "absl/memory/memory.h"
#include "cartographer/mapping/localization_health.h"
#include "cartographer/mapping/map_builder.h"
#include "cartographer_ros/node.h"
#include "cartographer_ros/node_options.h"
#include "cartographer_ros/ros_log_sink.h"
#include "cartographer_ros/visualweb.h"
#include "diagnostic_msgs/DiagnosticArray.h"
#include "diagnostic_msgs/DiagnosticStatus.h"
#include "diagnostic_msgs/KeyValue.h"
#include "gflags/gflags.h"
#include "ros/ros.h"
#include "tf2_ros/transform_listener.h"

// DEFINE_bool(collect_metrics, false,
//             "Activates the collection of runtime metrics. If activated, the "
//             "metrics can be accessed via a ROS service.");
// DEFINE_string(configuration_directory, "",
//               "First directory in which configuration files are searched, "
//               "second is always the Cartographer installation to allow "
//               "including files from there.");
// DEFINE_string(configuration_basename, "",
//               "Basename, i.e. not containing any directory prefix, of the "
//               "configuration file.");
// DEFINE_string(load_state_filename, "",
//               "If non-empty, filename of a .pbstream file to load, containing "
//               "a saved SLAM state.");
// DEFINE_bool(load_frozen_state, true,
//             "Load the saved state as frozen (non-optimized) trajectories.");
// DEFINE_bool(
//     start_trajectory_with_default_topics, true,
//     "Enable to immediately start the first trajectory with default topics.");
// DEFINE_string(
//     save_state_filename, "",
//     "If non-empty, serialize state and write it to disk before shutting down.");

namespace
{
constexpr int kWorkQueueWarnThreshold = 5000;
constexpr int kWorkQueueErrorThreshold = 20000;
constexpr double kLowMatchScoreThresholdPercent = 65.0;
constexpr int kLowMatchScoreWarnMinCount = 3;
constexpr double kLowMatchScoreWarnMinRatio = 0.20;
// Keep the confirmed-loss fail-closed chain comfortably below its 2 s safety
// budget even when the downstream SlamState publisher is between ticks.
constexpr double kLocalizationHealthPublishPeriodSeconds = 0.2;

bool HasLowMatchScoreWarning(
    const cartographer::mapping::LocalizationHealthSnapshot &snapshot)
{
  if (snapshot.match_score_count <= 0 ||
      snapshot.low_match_score_count < kLowMatchScoreWarnMinCount)
  {
    return false;
  }
  return static_cast<double>(snapshot.low_match_score_count) /
             static_cast<double>(snapshot.match_score_count) >=
         kLowMatchScoreWarnMinRatio;
}

std::string FormatDouble(const double value)
{
  std::ostringstream out;
  out << std::fixed << std::setprecision(3) << value;
  return out.str();
}

void AddValue(diagnostic_msgs::DiagnosticStatus *const status,
              const std::string &key, const std::string &value)
{
  diagnostic_msgs::KeyValue item;
  item.key = key;
  item.value = value;
  status->values.push_back(item);
}

template <typename T>
void AddValue(diagnostic_msgs::DiagnosticStatus *const status,
              const std::string &key, const T &value)
{
  std::ostringstream out;
  out << value;
  AddValue(status, key, out.str());
}

diagnostic_msgs::DiagnosticArray BuildLocalizationHealthMessage()
{
  const auto snapshot =
      cartographer::mapping::GetLocalizationHealthSnapshot(60.0);

  diagnostic_msgs::DiagnosticArray message;
  message.header.stamp = ros::Time::now();

  diagnostic_msgs::DiagnosticStatus status;
  status.name = "cartographer/localization_health";
  status.hardware_id = "cartographer";

  if (!snapshot.observed)
  {
    status.level = diagnostic_msgs::DiagnosticStatus::STALE;
    status.message = "waiting_for_cartographer_health_samples";
  }
  else if (snapshot.localization_lost_confirmed)
  {
    status.level = diagnostic_msgs::DiagnosticStatus::ERROR;
    status.message = "localization_lost_confirmed";
  }
  else if (snapshot.latest_work_queue_size >= kWorkQueueErrorThreshold)
  {
    status.level = diagnostic_msgs::DiagnosticStatus::ERROR;
    status.message = "work_queue_blocked";
  }
  else if (snapshot.latest_work_queue_size >= kWorkQueueWarnThreshold ||
           snapshot.backpressure_count > 0 ||
           HasLowMatchScoreWarning(snapshot) ||
           snapshot.pure_localization_force_opt_count > 0 ||
           snapshot.active_frozen_ambiguous_reject_count > 0 ||
           snapshot.active_frozen_geometry_reject_count > 0 ||
           snapshot.active_frozen_full_map_reject_count > 0 ||
           snapshot.active_frozen_consistency_reject_count > 0 ||
           snapshot.recovery_state != "OK")
  {
    status.level = diagnostic_msgs::DiagnosticStatus::WARN;
    status.message = "localization_health_degraded";
  }
  else
  {
    status.level = diagnostic_msgs::DiagnosticStatus::OK;
    status.message = "ok";
  }

  AddValue(&status, "window_seconds", FormatDouble(snapshot.window_seconds));
  AddValue(&status, "work_queue_warn_threshold",
           kWorkQueueWarnThreshold);
  AddValue(&status, "work_queue_error_threshold",
           kWorkQueueErrorThreshold);
  AddValue(&status, "latest_work_queue_size",
           snapshot.latest_work_queue_size);
  AddValue(&status, "max_work_queue_size", snapshot.max_work_queue_size);

  AddValue(&status, "backpressure_count", snapshot.backpressure_count);
  AddValue(&status, "latest_backpressure_keep_every_n",
           snapshot.latest_backpressure_keep_every_n);
  AddValue(&status, "max_backpressure_keep_every_n",
           snapshot.max_backpressure_keep_every_n);
  AddValue(&status, "total_backpressure_count",
           snapshot.total_backpressure_count);

  AddValue(&status, "low_match_score_threshold_percent",
           FormatDouble(kLowMatchScoreThresholdPercent));
  AddValue(&status, "match_score_count", snapshot.match_score_count);
  AddValue(&status, "low_match_score_count",
           snapshot.low_match_score_count);
  AddValue(&status, "low_match_score_warn_min_count",
           kLowMatchScoreWarnMinCount);
  AddValue(&status, "low_match_score_warn_min_ratio",
           FormatDouble(kLowMatchScoreWarnMinRatio));
  AddValue(&status, "latest_match_score_percent",
           FormatDouble(snapshot.latest_match_score_percent));
  AddValue(&status, "min_match_score_percent",
           FormatDouble(snapshot.min_match_score_percent));
  AddValue(&status, "mean_match_score_percent",
           FormatDouble(snapshot.mean_match_score_percent));
  AddValue(&status, "total_match_score_count",
           snapshot.total_match_score_count);

  AddValue(&status, "constraint_result_events",
           snapshot.constraint_result_events);
  AddValue(&status, "constraint_computation_count",
           snapshot.constraint_computation_count);
  AddValue(&status, "additional_constraint_count",
           snapshot.additional_constraint_count);

  AddValue(&status, "active_frozen_accepted_constraint_count",
           snapshot.active_frozen_accepted_constraint_count);
  AddValue(&status, "last_active_frozen_accepted_age_s",
           FormatDouble(snapshot.last_active_frozen_accepted_age_s));
  AddValue(&status, "total_active_frozen_accepted_constraint_count",
           snapshot.total_active_frozen_accepted_constraint_count);
  AddValue(&status, "active_frozen_top1_score",
           FormatDouble(snapshot.active_frozen_top1_score));
  AddValue(&status, "active_frozen_top2_score",
           FormatDouble(snapshot.active_frozen_top2_score));
  AddValue(&status, "active_frozen_margin",
           FormatDouble(snapshot.active_frozen_margin));
  AddValue(&status, "active_frozen_hit20",
           FormatDouble(snapshot.active_frozen_hit20));
  AddValue(&status, "active_frozen_mean_distance",
           FormatDouble(snapshot.active_frozen_mean_distance));
  AddValue(&status, "active_frozen_free_space_conflict_ratio",
           FormatDouble(snapshot.active_frozen_free_space_conflict_ratio));
  AddValue(&status, "active_frozen_known_ratio",
           FormatDouble(snapshot.active_frozen_known_ratio));
  AddValue(&status, "active_frozen_sector_coverage",
           FormatDouble(snapshot.active_frozen_sector_coverage));
  AddValue(&status, "active_frozen_candidate_map_hit20",
           FormatDouble(snapshot.active_frozen_candidate_map_hit20));
  AddValue(&status, "active_frozen_candidate_map_mean_distance",
           FormatDouble(snapshot.active_frozen_candidate_map_mean_distance));
  AddValue(&status, "active_frozen_candidate_map_known_ratio",
           FormatDouble(snapshot.active_frozen_candidate_map_known_ratio));
  AddValue(&status, "active_frozen_candidate_map_hit20_improvement",
           FormatDouble(snapshot.active_frozen_candidate_map_hit20_improvement));
  AddValue(&status, "active_frozen_candidate_map_mean_distance_improvement",
           FormatDouble(
               snapshot.active_frozen_candidate_map_mean_distance_improvement));
  AddValue(&status, "implied_correction_translation_m",
           FormatDouble(snapshot.implied_correction_translation_m));
  AddValue(&status, "implied_correction_yaw_deg",
           FormatDouble(snapshot.implied_correction_yaw_deg));
  AddValue(&status, "current_pose_scan_map_hit20",
           FormatDouble(snapshot.current_pose_scan_map_hit20));
  AddValue(&status, "current_pose_scan_map_mean_distance",
           FormatDouble(snapshot.current_pose_scan_map_mean_distance));
  AddValue(&status, "current_pose_scan_map_sampled_points",
           snapshot.current_pose_scan_map_sampled_points);
  AddValue(&status, "current_pose_scan_map_checked_submaps",
           snapshot.current_pose_scan_map_checked_submaps);
  AddValue(&status, "current_pose_scan_map_bad_count",
           snapshot.current_pose_scan_map_bad_count);
  AddValue(&status, "total_current_pose_scan_map_bad_count",
           snapshot.total_current_pose_scan_map_bad_count);
  AddValue(&status, "map_scan_hit20",
           FormatDouble(snapshot.map_scan_hit20));
  AddValue(&status, "map_scan_mean_distance",
           FormatDouble(snapshot.map_scan_mean_distance));
  AddValue(&status, "map_scan_known_ratio",
           FormatDouble(snapshot.map_scan_known_ratio));
  AddValue(&status, "map_scan_sampled_points",
           snapshot.map_scan_sampled_points);
  AddValue(&status, "map_scan_checked_submaps",
           snapshot.map_scan_checked_submaps);
  AddValue(&status, "map_scan_bad_count",
           snapshot.map_scan_bad_count);
  AddValue(&status, "total_map_scan_bad_count",
           snapshot.total_map_scan_bad_count);
  AddValue(&status, "active_frozen_ambiguous_reject_count",
           snapshot.active_frozen_ambiguous_reject_count);
  AddValue(&status, "active_frozen_geometry_reject_count",
           snapshot.active_frozen_geometry_reject_count);
  AddValue(&status, "active_frozen_full_map_reject_count",
           snapshot.active_frozen_full_map_reject_count);
  AddValue(&status, "active_frozen_consistency_reject_count",
           snapshot.active_frozen_consistency_reject_count);
  AddValue(&status, "active_frozen_consistency_accept_count",
           snapshot.active_frozen_consistency_accept_count);
  AddValue(&status, "total_active_frozen_ambiguous_reject_count",
           snapshot.total_active_frozen_ambiguous_reject_count);
  AddValue(&status, "total_active_frozen_geometry_reject_count",
           snapshot.total_active_frozen_geometry_reject_count);
  AddValue(&status, "total_active_frozen_full_map_reject_count",
           snapshot.total_active_frozen_full_map_reject_count);
  AddValue(&status, "total_active_frozen_consistency_reject_count",
           snapshot.total_active_frozen_consistency_reject_count);
  AddValue(&status, "total_active_frozen_consistency_accept_count",
           snapshot.total_active_frozen_consistency_accept_count);
  AddValue(&status, "recovery_state", snapshot.recovery_state);
  AddValue(&status, "recovery_reason", snapshot.recovery_reason);
  AddValue(&status, "automatic_relocation_enabled",
           snapshot.automatic_relocation_enabled ? "true" : "false");
  AddValue(&status, "localization_lost_confirmed",
           snapshot.localization_lost_confirmed ? "true" : "false");
  AddValue(&status, "localization_loss_episode",
           snapshot.localization_loss_episode);
  AddValue(&status, "localization_lost_reason", snapshot.recovery_reason);
  AddValue(&status, "total_localization_loss_count",
           snapshot.total_localization_loss_count);
  AddValue(&status, "map_scan_distance_field_source",
           snapshot.map_scan_distance_field_source);
  AddValue(&status, "map_scan_distance_field_generation",
           snapshot.map_scan_distance_field_generation);
  AddValue(&status, "map_scan_distance_field_cells",
           snapshot.map_scan_distance_field_cells);
  AddValue(&status, "map_scan_distance_field_resident_bytes",
           snapshot.map_scan_distance_field_resident_bytes);
  AddValue(&status, "map_scan_distance_field_load_count",
           snapshot.map_scan_distance_field_load_count);
  AddValue(&status, "map_scan_distance_field_load_failure_count",
           snapshot.map_scan_distance_field_load_failure_count);
  AddValue(&status, "map_scan_distance_field_last_load_source",
           snapshot.map_scan_distance_field_last_load_source);
  AddValue(&status, "map_scan_distance_field_last_load_result",
           snapshot.map_scan_distance_field_last_load_result);
  AddValue(&status, "map_scan_distance_field_last_load_duration_ms",
           FormatDouble(
               snapshot.map_scan_distance_field_last_load_duration_ms));
  AddValue(&status, "map_scan_distance_field_build_count",
           snapshot.map_scan_distance_field_build_count);
  AddValue(&status, "map_scan_distance_field_build_failure_count",
           snapshot.map_scan_distance_field_build_failure_count);
  AddValue(&status, "map_scan_distance_field_build_published_count",
           snapshot.map_scan_distance_field_build_published_count);
  AddValue(&status, "map_scan_distance_field_build_unpublished_count",
           snapshot.map_scan_distance_field_build_unpublished_count);
  AddValue(&status, "map_scan_distance_field_last_build_source",
           snapshot.map_scan_distance_field_last_build_source);
  AddValue(&status, "map_scan_distance_field_last_build_result",
           snapshot.map_scan_distance_field_last_build_result);
  AddValue(&status, "map_scan_distance_field_last_build_published",
           snapshot.map_scan_distance_field_last_build_published ? "true"
                                                                 : "false");
  AddValue(&status, "map_scan_distance_field_last_build_duration_ms",
           FormatDouble(
               snapshot.map_scan_distance_field_last_build_duration_ms));
  AddValue(&status, "map_scan_distance_field_invalidation_count",
           snapshot.map_scan_distance_field_invalidation_count);
  AddValue(&status, "map_scan_distance_field_last_invalidation_reason",
           snapshot.map_scan_distance_field_last_invalidation_reason);
  AddValue(&status, "map_scan_distance_field_query_count",
           snapshot.map_scan_distance_field_query_count);
  AddValue(&status, "map_scan_distance_field_last_query_duration_ms",
           FormatDouble(
               snapshot.map_scan_distance_field_last_query_duration_ms));
  AddValue(&status, "map_scan_distance_field_mean_query_duration_ms",
           FormatDouble(
               snapshot.map_scan_distance_field_mean_query_duration_ms));
  AddValue(&status, "map_scan_distance_field_max_query_duration_ms",
           FormatDouble(
               snapshot.map_scan_distance_field_max_query_duration_ms));
  AddValue(&status, "recovery_full_search_count",
           snapshot.recovery_full_search_count);
  AddValue(&status, "latest_recovery_full_search_submap_count",
           snapshot.latest_recovery_full_search_submap_count);
  AddValue(&status, "total_recovery_full_search_count",
           snapshot.total_recovery_full_search_count);

  AddValue(&status, "pure_localization_force_opt_count",
           snapshot.pure_localization_force_opt_count);
  AddValue(&status, "latest_active_submaps",
           snapshot.latest_active_submaps);
  AddValue(&status, "max_active_submaps", snapshot.max_active_submaps);

  AddValue(&status, "auto_relocation_trigger_count",
           snapshot.auto_relocation_trigger_count);
  AddValue(&status, "auto_relocation_result_count",
           snapshot.auto_relocation_result_count);
  AddValue(&status, "auto_relocation_success_count",
           snapshot.auto_relocation_success_count);
  AddValue(&status, "latest_auto_relocation_queue_size",
           snapshot.latest_auto_relocation_queue_size);
  AddValue(&status, "max_auto_relocation_queue_size",
           snapshot.max_auto_relocation_queue_size);
  AddValue(&status, "latest_auto_relocation_return_code",
           snapshot.latest_auto_relocation_return_code);
  AddValue(&status, "total_auto_relocation_trigger_count",
           snapshot.total_auto_relocation_trigger_count);
  AddValue(&status, "total_auto_relocation_success_count",
           snapshot.total_auto_relocation_success_count);

  message.status.push_back(status);
  return message;
}
} // namespace

int main(int argc, char **argv)
{
  google::InitGoogleLogging(argv[0]);
  ::ros::init(argc, argv, "cartographer_node");
  ::ros::start();

  cartographer_ros::ScopedRosLogSink ros_log_sink;
  ::cartographer_ros::visualweb web;
  web.start();
  ::ros::NodeHandle node_handle;
  auto localization_health_publisher =
      node_handle.advertise<diagnostic_msgs::DiagnosticArray>(
          "/cartographer/localization_health", 1, true);
  auto localization_health_timer = node_handle.createWallTimer(
      ::ros::WallDuration(kLocalizationHealthPublishPeriodSeconds),
      [&localization_health_publisher](const ::ros::WallTimerEvent &)
      {
        localization_health_publisher.publish(
            BuildLocalizationHealthMessage());
      });
  (void)localization_health_timer;
  while (::ros::ok())
  {
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  }
  ::ros::shutdown();
}

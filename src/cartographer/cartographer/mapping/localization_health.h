#ifndef CARTOGRAPHER_MAPPING_LOCALIZATION_HEALTH_H_
#define CARTOGRAPHER_MAPPING_LOCALIZATION_HEALTH_H_

#include <cstddef>
#include <cstdint>
#include <string>

namespace cartographer {
namespace mapping {

struct LocalizationHealthSnapshot {
  bool observed = false;
  double window_seconds = 60.0;

  int latest_work_queue_size = -1;
  int max_work_queue_size = -1;
  int latest_backpressure_keep_every_n = 1;
  int max_backpressure_keep_every_n = 1;

  int match_score_count = 0;
  int low_match_score_count = 0;
  double latest_match_score_percent = 0.0;
  double min_match_score_percent = 0.0;
  double mean_match_score_percent = 0.0;

  int constraint_result_events = 0;
  int constraint_computation_count = 0;
  int additional_constraint_count = 0;

  int backpressure_count = 0;

  int active_frozen_accepted_constraint_count = 0;
  double last_active_frozen_accepted_age_s = -1.0;
  double active_frozen_top1_score = 0.0;
  double active_frozen_top2_score = 0.0;
  double active_frozen_margin = 0.0;
  double active_frozen_hit20 = -1.0;
  double active_frozen_mean_distance = -1.0;
  double active_frozen_free_space_conflict_ratio = -1.0;
  double active_frozen_known_ratio = -1.0;
  double active_frozen_sector_coverage = -1.0;
  double active_frozen_candidate_map_hit20 = -1.0;
  double active_frozen_candidate_map_mean_distance = -1.0;
  double active_frozen_candidate_map_known_ratio = -1.0;
  double active_frozen_candidate_map_hit20_improvement = -1.0;
  double active_frozen_candidate_map_mean_distance_improvement = -1.0;
  double implied_correction_translation_m = 0.0;
  double implied_correction_yaw_deg = 0.0;
  double current_pose_scan_map_hit20 = -1.0;
  double current_pose_scan_map_mean_distance = -1.0;
  int current_pose_scan_map_sampled_points = 0;
  int current_pose_scan_map_checked_submaps = 0;
  int current_pose_scan_map_bad_count = 0;
  double map_scan_hit20 = -1.0;
  double map_scan_mean_distance = -1.0;
  double map_scan_known_ratio = -1.0;
  int map_scan_sampled_points = 0;
  int map_scan_checked_submaps = 0;
  int map_scan_bad_count = 0;
  int active_frozen_ambiguous_reject_count = 0;
  int active_frozen_geometry_reject_count = 0;
  int active_frozen_full_map_reject_count = 0;
  int active_frozen_consistency_reject_count = 0;
  int active_frozen_consistency_accept_count = 0;
  int recovery_full_search_count = 0;
  int latest_recovery_full_search_submap_count = 0;
  std::string recovery_state = "OK";
  std::string recovery_reason;

  int pure_localization_force_opt_count = 0;
  int latest_active_submaps = 0;
  int max_active_submaps = 0;

  int auto_relocation_trigger_count = 0;
  int auto_relocation_result_count = 0;
  int auto_relocation_success_count = 0;
  int latest_auto_relocation_queue_size = -1;
  int max_auto_relocation_queue_size = -1;
  int latest_auto_relocation_return_code = 0;

  std::int64_t total_match_score_count = 0;
  std::int64_t total_backpressure_count = 0;
  std::int64_t total_active_frozen_accepted_constraint_count = 0;
  std::int64_t total_active_frozen_ambiguous_reject_count = 0;
  std::int64_t total_active_frozen_geometry_reject_count = 0;
  std::int64_t total_active_frozen_full_map_reject_count = 0;
  std::int64_t total_active_frozen_consistency_reject_count = 0;
  std::int64_t total_active_frozen_consistency_accept_count = 0;
  std::int64_t total_current_pose_scan_map_bad_count = 0;
  std::int64_t total_map_scan_bad_count = 0;
  std::int64_t total_recovery_full_search_count = 0;
  std::int64_t total_auto_relocation_trigger_count = 0;
  std::int64_t total_auto_relocation_success_count = 0;
};

void ResetLocalizationHealth();
void RecordLocalizationHealthMatchScore(double score_percent);
void RecordLocalizationHealthConstraintResult(int computations,
                                              int additional_constraints);
void RecordLocalizationHealthWorkQueueSize(std::size_t queue_size);
void RecordLocalizationHealthBackpressure(std::size_t queue_size,
                                          std::size_t keep_every_n);
void RecordLocalizationHealthActiveFrozenAcceptedConstraint();
void RecordLocalizationHealthActiveFrozenCandidate(
    double top1_score, double top2_score, double margin, double hit20,
    double mean_distance, double free_space_conflict_ratio, double known_ratio,
    double sector_coverage, double implied_translation_m, double implied_yaw_rad);
void RecordLocalizationHealthActiveFrozenCandidateFullMap(
    double hit20, double mean_distance, double known_ratio,
    double hit20_improvement, double mean_distance_improvement);
void RecordLocalizationHealthCurrentPoseScanMapQuality(
    double hit20, double mean_distance, int sampled_points,
    int checked_submaps, bool bad);
void RecordLocalizationHealthMapScanQuality(
    double hit20, double mean_distance, double known_ratio, int sampled_points,
    int checked_submaps, bool bad);
void RecordLocalizationHealthRecoveryState(const std::string& state,
                                           const std::string& reason);
void RecordLocalizationHealthRecoveryFullSearch(int checked_submaps,
                                                const std::string& reason);
void RecordLocalizationHealthActiveFrozenAmbiguousReject();
void RecordLocalizationHealthActiveFrozenGeometryReject();
void RecordLocalizationHealthActiveFrozenFullMapReject();
void RecordLocalizationHealthActiveFrozenConsistencyReject();
void RecordLocalizationHealthActiveFrozenConsistencyAccept();
void RecordLocalizationHealthPureLocalizationForceOptimization(
    int active_submaps);
void RecordLocalizationHealthAutoRelocationTrigger(std::size_t queue_size);
void RecordLocalizationHealthAutoRelocationResult(int return_code,
                                                  bool success);
LocalizationHealthSnapshot GetLocalizationHealthSnapshot(
    double window_seconds = 60.0);

}  // namespace mapping
}  // namespace cartographer

#endif  // CARTOGRAPHER_MAPPING_LOCALIZATION_HEALTH_H_

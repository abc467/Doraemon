#include "cartographer/mapping/localization_health.h"

#include <algorithm>
#include <chrono>
#include <deque>
#include <mutex>
#include <numeric>
#include <string>

namespace cartographer {
namespace mapping {
namespace {

using Clock = std::chrono::steady_clock;
using TimePoint = Clock::time_point;

struct ScoreEvent {
  TimePoint time;
  double score_percent;
};

struct QueueEvent {
  TimePoint time;
  int queue_size;
};

struct ConstraintResultEvent {
  TimePoint time;
  int computations;
  int additional_constraints;
};

struct BackpressureEvent {
  TimePoint time;
  int queue_size;
  int keep_every_n;
};

struct ActiveFrozenCandidateEvent {
  TimePoint time;
  double top1_score;
  double top2_score;
  double margin;
  double hit20;
  double mean_distance;
  double free_space_conflict_ratio;
  double known_ratio;
  double sector_coverage;
  double implied_translation_m;
  double implied_yaw_rad;
};

struct ActiveFrozenCandidateFullMapEvent {
  TimePoint time;
  double hit20;
  double mean_distance;
  double known_ratio;
  double hit20_improvement;
  double mean_distance_improvement;
};

struct CurrentPoseScanMapEvent {
  TimePoint time;
  double hit20;
  double mean_distance;
  int sampled_points;
  int checked_submaps;
  bool bad;
};

struct MapScanEvent {
  TimePoint time;
  double hit20;
  double mean_distance;
  double known_ratio;
  int sampled_points;
  int checked_submaps;
  bool bad;
};

struct RecoveryFullSearchEvent {
  TimePoint time;
  int checked_submaps;
  std::string reason;
};

struct ForceOptEvent {
  TimePoint time;
  int active_submaps;
};

struct AutoRelocationTriggerEvent {
  TimePoint time;
  int queue_size;
};

struct AutoRelocationResultEvent {
  TimePoint time;
  int return_code;
  bool success;
};

template <typename EventDeque>
void PruneByTime(EventDeque* events, const TimePoint cutoff) {
  while (!events->empty() && events->front().time < cutoff) {
    events->pop_front();
  }
}

class LocalizationHealthStore {
 public:
  void Reset() {
    std::lock_guard<std::mutex> lock(mutex_);
    observed_ = false;
    latest_work_queue_size_ = -1;
    latest_backpressure_keep_every_n_ = 1;
    latest_active_submaps_ = 0;
    latest_auto_relocation_queue_size_ = -1;
    latest_auto_relocation_return_code_ = 0;
    latest_match_score_percent_ = 0.0;
    latest_active_frozen_top1_score_ = 0.0;
    latest_active_frozen_top2_score_ = 0.0;
    latest_active_frozen_margin_ = 0.0;
    latest_active_frozen_hit20_ = -1.0;
    latest_active_frozen_mean_distance_ = -1.0;
    latest_active_frozen_free_space_conflict_ratio_ = -1.0;
    latest_active_frozen_known_ratio_ = -1.0;
    latest_active_frozen_sector_coverage_ = -1.0;
    latest_active_frozen_candidate_map_hit20_ = -1.0;
    latest_active_frozen_candidate_map_mean_distance_ = -1.0;
    latest_active_frozen_candidate_map_known_ratio_ = -1.0;
    latest_active_frozen_candidate_map_hit20_improvement_ = -1.0;
    latest_active_frozen_candidate_map_mean_distance_improvement_ = -1.0;
    latest_implied_correction_translation_m_ = 0.0;
    latest_implied_correction_yaw_rad_ = 0.0;
    latest_current_pose_scan_map_hit20_ = -1.0;
    latest_current_pose_scan_map_mean_distance_ = -1.0;
    latest_current_pose_scan_map_sampled_points_ = 0;
    latest_current_pose_scan_map_checked_submaps_ = 0;
    latest_map_scan_hit20_ = -1.0;
    latest_map_scan_mean_distance_ = -1.0;
    latest_map_scan_known_ratio_ = -1.0;
    latest_map_scan_sampled_points_ = 0;
    latest_map_scan_checked_submaps_ = 0;
    latest_recovery_full_search_submap_count_ = 0;
    recovery_state_ = "OK";
    recovery_reason_.clear();
    last_active_frozen_accepted_constraint_time_ = TimePoint();
    total_match_score_count_ = 0;
    total_backpressure_count_ = 0;
    total_active_frozen_accepted_constraint_count_ = 0;
    total_active_frozen_ambiguous_reject_count_ = 0;
    total_active_frozen_geometry_reject_count_ = 0;
    total_active_frozen_full_map_reject_count_ = 0;
    total_active_frozen_consistency_reject_count_ = 0;
    total_active_frozen_consistency_accept_count_ = 0;
    total_current_pose_scan_map_bad_count_ = 0;
    total_map_scan_bad_count_ = 0;
    total_recovery_full_search_count_ = 0;
    total_auto_relocation_trigger_count_ = 0;
    total_auto_relocation_success_count_ = 0;
    scores_.clear();
    queue_sizes_.clear();
    constraint_results_.clear();
    backpressure_.clear();
    active_frozen_candidates_.clear();
    active_frozen_candidate_full_map_.clear();
    current_pose_scan_map_.clear();
    map_scan_.clear();
    recovery_full_searches_.clear();
    active_frozen_ambiguous_rejects_.clear();
    active_frozen_geometry_rejects_.clear();
    active_frozen_full_map_rejects_.clear();
    active_frozen_consistency_rejects_.clear();
    active_frozen_consistency_accepts_.clear();
    active_frozen_accepted_constraints_.clear();
    force_optimizations_.clear();
    auto_relocation_triggers_.clear();
    auto_relocation_results_.clear();
  }

  void RecordScore(const double score_percent) {
    std::lock_guard<std::mutex> lock(mutex_);
    const TimePoint now = Clock::now();
    scores_.push_back({now, score_percent});
    latest_match_score_percent_ = score_percent;
    ++total_match_score_count_;
    observed_ = true;
  }

  void RecordConstraintResult(const int computations,
                              const int additional_constraints) {
    std::lock_guard<std::mutex> lock(mutex_);
    constraint_results_.push_back(
        {Clock::now(), computations, additional_constraints});
    observed_ = true;
  }

  void RecordQueueSize(const std::size_t queue_size) {
    std::lock_guard<std::mutex> lock(mutex_);
    const int value = static_cast<int>(queue_size);
    latest_work_queue_size_ = value;
    queue_sizes_.push_back({Clock::now(), value});
    observed_ = true;
  }

  void RecordBackpressure(const std::size_t queue_size,
                          const std::size_t keep_every_n) {
    std::lock_guard<std::mutex> lock(mutex_);
    latest_backpressure_keep_every_n_ = static_cast<int>(keep_every_n);
    backpressure_.push_back({Clock::now(), static_cast<int>(queue_size),
                             static_cast<int>(keep_every_n)});
    ++total_backpressure_count_;
    observed_ = true;
  }

  void RecordActiveFrozenAcceptedConstraint() {
    std::lock_guard<std::mutex> lock(mutex_);
    const TimePoint now = Clock::now();
    active_frozen_accepted_constraints_.push_back(now);
    last_active_frozen_accepted_constraint_time_ = now;
    ++total_active_frozen_accepted_constraint_count_;
    observed_ = true;
  }

  void RecordActiveFrozenCandidate(const double top1_score,
                                   const double top2_score,
                                   const double margin,
                                   const double hit20,
                                   const double mean_distance,
                                   const double free_space_conflict_ratio,
                                   const double known_ratio,
                                   const double sector_coverage,
                                   const double implied_translation_m,
                                   const double implied_yaw_rad) {
    std::lock_guard<std::mutex> lock(mutex_);
    const TimePoint now = Clock::now();
    latest_active_frozen_top1_score_ = top1_score;
    latest_active_frozen_top2_score_ = top2_score;
    latest_active_frozen_margin_ = margin;
    latest_active_frozen_hit20_ = hit20;
    latest_active_frozen_mean_distance_ = mean_distance;
    latest_active_frozen_free_space_conflict_ratio_ =
        free_space_conflict_ratio;
    latest_active_frozen_known_ratio_ = known_ratio;
    latest_active_frozen_sector_coverage_ = sector_coverage;
    latest_implied_correction_translation_m_ = implied_translation_m;
    latest_implied_correction_yaw_rad_ = implied_yaw_rad;
    active_frozen_candidates_.push_back(
        {now, top1_score, top2_score, margin, hit20, mean_distance,
         free_space_conflict_ratio, known_ratio, sector_coverage,
         implied_translation_m, implied_yaw_rad});
    observed_ = true;
  }

  void RecordActiveFrozenCandidateFullMap(
      const double hit20, const double mean_distance, const double known_ratio,
      const double hit20_improvement,
      const double mean_distance_improvement) {
    std::lock_guard<std::mutex> lock(mutex_);
    const TimePoint now = Clock::now();
    latest_active_frozen_candidate_map_hit20_ = hit20;
    latest_active_frozen_candidate_map_mean_distance_ = mean_distance;
    latest_active_frozen_candidate_map_known_ratio_ = known_ratio;
    latest_active_frozen_candidate_map_hit20_improvement_ = hit20_improvement;
    latest_active_frozen_candidate_map_mean_distance_improvement_ =
        mean_distance_improvement;
    active_frozen_candidate_full_map_.push_back(
        {now, hit20, mean_distance, known_ratio, hit20_improvement,
         mean_distance_improvement});
    observed_ = true;
  }

  void RecordCurrentPoseScanMapQuality(const double hit20,
                                       const double mean_distance,
                                       const int sampled_points,
                                       const int checked_submaps,
                                       const bool bad) {
    std::lock_guard<std::mutex> lock(mutex_);
    const TimePoint now = Clock::now();
    latest_current_pose_scan_map_hit20_ = hit20;
    latest_current_pose_scan_map_mean_distance_ = mean_distance;
    latest_current_pose_scan_map_sampled_points_ = sampled_points;
    latest_current_pose_scan_map_checked_submaps_ = checked_submaps;
    current_pose_scan_map_.push_back(
        {now, hit20, mean_distance, sampled_points, checked_submaps, bad});
    if (bad) {
      ++total_current_pose_scan_map_bad_count_;
    }
    observed_ = true;
  }

  void RecordMapScanQuality(const double hit20,
                            const double mean_distance,
                            const double known_ratio,
                            const int sampled_points,
                            const int checked_submaps,
                            const bool bad) {
    std::lock_guard<std::mutex> lock(mutex_);
    const TimePoint now = Clock::now();
    latest_map_scan_hit20_ = hit20;
    latest_map_scan_mean_distance_ = mean_distance;
    latest_map_scan_known_ratio_ = known_ratio;
    latest_map_scan_sampled_points_ = sampled_points;
    latest_map_scan_checked_submaps_ = checked_submaps;
    map_scan_.push_back({now, hit20, mean_distance, known_ratio,
                         sampled_points, checked_submaps, bad});
    if (bad) {
      ++total_map_scan_bad_count_;
    }
    observed_ = true;
  }

  void RecordRecoveryState(const std::string& state,
                           const std::string& reason) {
    std::lock_guard<std::mutex> lock(mutex_);
    recovery_state_ = state;
    recovery_reason_ = reason;
    observed_ = true;
  }

  void RecordRecoveryFullSearch(const int checked_submaps,
                                const std::string& reason) {
    std::lock_guard<std::mutex> lock(mutex_);
    latest_recovery_full_search_submap_count_ = checked_submaps;
    recovery_full_searches_.push_back({Clock::now(), checked_submaps, reason});
    recovery_state_ = "RECOVERY_SEARCH";
    recovery_reason_ = reason;
    ++total_recovery_full_search_count_;
    observed_ = true;
  }

  void RecordActiveFrozenAmbiguousReject() {
    std::lock_guard<std::mutex> lock(mutex_);
    active_frozen_ambiguous_rejects_.push_back(Clock::now());
    ++total_active_frozen_ambiguous_reject_count_;
    observed_ = true;
  }

  void RecordActiveFrozenGeometryReject() {
    std::lock_guard<std::mutex> lock(mutex_);
    active_frozen_geometry_rejects_.push_back(Clock::now());
    ++total_active_frozen_geometry_reject_count_;
    observed_ = true;
  }

  void RecordActiveFrozenFullMapReject() {
    std::lock_guard<std::mutex> lock(mutex_);
    active_frozen_full_map_rejects_.push_back(Clock::now());
    ++total_active_frozen_full_map_reject_count_;
    observed_ = true;
  }

  void RecordActiveFrozenConsistencyReject() {
    std::lock_guard<std::mutex> lock(mutex_);
    active_frozen_consistency_rejects_.push_back(Clock::now());
    ++total_active_frozen_consistency_reject_count_;
    observed_ = true;
  }

  void RecordActiveFrozenConsistencyAccept() {
    std::lock_guard<std::mutex> lock(mutex_);
    active_frozen_consistency_accepts_.push_back(Clock::now());
    ++total_active_frozen_consistency_accept_count_;
    observed_ = true;
  }

  void RecordForceOptimization(const int active_submaps) {
    std::lock_guard<std::mutex> lock(mutex_);
    latest_active_submaps_ = active_submaps;
    force_optimizations_.push_back({Clock::now(), active_submaps});
    observed_ = true;
  }

  void RecordAutoRelocationTrigger(const std::size_t queue_size) {
    std::lock_guard<std::mutex> lock(mutex_);
    latest_auto_relocation_queue_size_ = static_cast<int>(queue_size);
    auto_relocation_triggers_.push_back(
        {Clock::now(), latest_auto_relocation_queue_size_});
    ++total_auto_relocation_trigger_count_;
    observed_ = true;
  }

  void RecordAutoRelocationResult(const int return_code, const bool success) {
    std::lock_guard<std::mutex> lock(mutex_);
    latest_auto_relocation_return_code_ = return_code;
    auto_relocation_results_.push_back({Clock::now(), return_code, success});
    if (success) {
      ++total_auto_relocation_success_count_;
    }
    observed_ = true;
  }

  LocalizationHealthSnapshot Snapshot(const double requested_window_seconds) {
    std::lock_guard<std::mutex> lock(mutex_);
    const double window_seconds = std::max(1.0, requested_window_seconds);
    const TimePoint now = Clock::now();
    const TimePoint cutoff =
        now - std::chrono::duration_cast<Clock::duration>(
                  std::chrono::duration<double>(window_seconds));
    Prune(cutoff);

    LocalizationHealthSnapshot snapshot;
    snapshot.observed = observed_;
    snapshot.window_seconds = window_seconds;
    snapshot.latest_work_queue_size = latest_work_queue_size_;
    snapshot.latest_backpressure_keep_every_n =
        latest_backpressure_keep_every_n_;
    snapshot.latest_match_score_percent = latest_match_score_percent_;
    snapshot.latest_active_submaps = latest_active_submaps_;
    snapshot.latest_auto_relocation_queue_size =
        latest_auto_relocation_queue_size_;
    snapshot.latest_auto_relocation_return_code =
        latest_auto_relocation_return_code_;
    snapshot.active_frozen_top1_score = latest_active_frozen_top1_score_;
    snapshot.active_frozen_top2_score = latest_active_frozen_top2_score_;
    snapshot.active_frozen_margin = latest_active_frozen_margin_;
    snapshot.active_frozen_hit20 = latest_active_frozen_hit20_;
    snapshot.active_frozen_mean_distance =
        latest_active_frozen_mean_distance_;
    snapshot.active_frozen_free_space_conflict_ratio =
        latest_active_frozen_free_space_conflict_ratio_;
    snapshot.active_frozen_known_ratio = latest_active_frozen_known_ratio_;
    snapshot.active_frozen_sector_coverage =
        latest_active_frozen_sector_coverage_;
    snapshot.active_frozen_candidate_map_hit20 =
        latest_active_frozen_candidate_map_hit20_;
    snapshot.active_frozen_candidate_map_mean_distance =
        latest_active_frozen_candidate_map_mean_distance_;
    snapshot.active_frozen_candidate_map_known_ratio =
        latest_active_frozen_candidate_map_known_ratio_;
    snapshot.active_frozen_candidate_map_hit20_improvement =
        latest_active_frozen_candidate_map_hit20_improvement_;
    snapshot.active_frozen_candidate_map_mean_distance_improvement =
        latest_active_frozen_candidate_map_mean_distance_improvement_;
    snapshot.implied_correction_translation_m =
        latest_implied_correction_translation_m_;
    snapshot.implied_correction_yaw_deg =
        latest_implied_correction_yaw_rad_ * 180.0 / 3.14159265358979323846;
    snapshot.current_pose_scan_map_hit20 =
        latest_current_pose_scan_map_hit20_;
    snapshot.current_pose_scan_map_mean_distance =
        latest_current_pose_scan_map_mean_distance_;
    snapshot.current_pose_scan_map_sampled_points =
        latest_current_pose_scan_map_sampled_points_;
    snapshot.current_pose_scan_map_checked_submaps =
        latest_current_pose_scan_map_checked_submaps_;
    snapshot.map_scan_hit20 = latest_map_scan_hit20_;
    snapshot.map_scan_mean_distance = latest_map_scan_mean_distance_;
    snapshot.map_scan_known_ratio = latest_map_scan_known_ratio_;
    snapshot.map_scan_sampled_points = latest_map_scan_sampled_points_;
    snapshot.map_scan_checked_submaps = latest_map_scan_checked_submaps_;
    snapshot.recovery_state = recovery_state_;
    snapshot.recovery_reason = recovery_reason_;
    snapshot.latest_recovery_full_search_submap_count =
        latest_recovery_full_search_submap_count_;
    snapshot.total_match_score_count = total_match_score_count_;
    snapshot.total_backpressure_count = total_backpressure_count_;
    snapshot.total_active_frozen_accepted_constraint_count =
        total_active_frozen_accepted_constraint_count_;
    snapshot.total_active_frozen_ambiguous_reject_count =
        total_active_frozen_ambiguous_reject_count_;
    snapshot.total_active_frozen_geometry_reject_count =
        total_active_frozen_geometry_reject_count_;
    snapshot.total_active_frozen_full_map_reject_count =
        total_active_frozen_full_map_reject_count_;
    snapshot.total_active_frozen_consistency_reject_count =
        total_active_frozen_consistency_reject_count_;
    snapshot.total_active_frozen_consistency_accept_count =
        total_active_frozen_consistency_accept_count_;
    snapshot.total_current_pose_scan_map_bad_count =
        total_current_pose_scan_map_bad_count_;
    snapshot.total_map_scan_bad_count = total_map_scan_bad_count_;
    snapshot.total_recovery_full_search_count =
        total_recovery_full_search_count_;
    snapshot.total_auto_relocation_trigger_count =
        total_auto_relocation_trigger_count_;
    snapshot.total_auto_relocation_success_count =
        total_auto_relocation_success_count_;

    if (!queue_sizes_.empty()) {
      snapshot.max_work_queue_size =
          std::max_element(queue_sizes_.begin(), queue_sizes_.end(),
                           [](const QueueEvent& a, const QueueEvent& b) {
                             return a.queue_size < b.queue_size;
                           })
              ->queue_size;
    }

    snapshot.match_score_count = static_cast<int>(scores_.size());
    if (!scores_.empty()) {
      double sum = 0.0;
      double min_score = scores_.front().score_percent;
      int low_count = 0;
      for (const ScoreEvent& event : scores_) {
        sum += event.score_percent;
        min_score = std::min(min_score, event.score_percent);
        if (event.score_percent < LowScoreThresholdPercent()) {
          ++low_count;
        }
      }
      snapshot.low_match_score_count = low_count;
      snapshot.min_match_score_percent = min_score;
      snapshot.mean_match_score_percent =
          sum / static_cast<double>(scores_.size());
    }

    snapshot.constraint_result_events =
        static_cast<int>(constraint_results_.size());
    for (const ConstraintResultEvent& event : constraint_results_) {
      snapshot.constraint_computation_count += event.computations;
      snapshot.additional_constraint_count += event.additional_constraints;
    }

    snapshot.backpressure_count = static_cast<int>(backpressure_.size());
    for (const BackpressureEvent& event : backpressure_) {
      snapshot.max_backpressure_keep_every_n =
          std::max(snapshot.max_backpressure_keep_every_n, event.keep_every_n);
    }

    snapshot.active_frozen_accepted_constraint_count =
        static_cast<int>(active_frozen_accepted_constraints_.size());
    snapshot.active_frozen_ambiguous_reject_count =
        static_cast<int>(active_frozen_ambiguous_rejects_.size());
    snapshot.active_frozen_geometry_reject_count =
        static_cast<int>(active_frozen_geometry_rejects_.size());
    snapshot.active_frozen_full_map_reject_count =
        static_cast<int>(active_frozen_full_map_rejects_.size());
    snapshot.active_frozen_consistency_reject_count =
        static_cast<int>(active_frozen_consistency_rejects_.size());
    snapshot.active_frozen_consistency_accept_count =
        static_cast<int>(active_frozen_consistency_accepts_.size());
    for (const CurrentPoseScanMapEvent& event : current_pose_scan_map_) {
      if (event.bad) {
        ++snapshot.current_pose_scan_map_bad_count;
      }
    }
    for (const MapScanEvent& event : map_scan_) {
      if (event.bad) {
        ++snapshot.map_scan_bad_count;
      }
    }
    snapshot.recovery_full_search_count =
        static_cast<int>(recovery_full_searches_.size());
    if (last_active_frozen_accepted_constraint_time_ != TimePoint()) {
      snapshot.last_active_frozen_accepted_age_s =
          std::chrono::duration<double>(
              now - last_active_frozen_accepted_constraint_time_)
              .count();
    }

    snapshot.pure_localization_force_opt_count =
        static_cast<int>(force_optimizations_.size());
    for (const ForceOptEvent& event : force_optimizations_) {
      snapshot.max_active_submaps =
          std::max(snapshot.max_active_submaps, event.active_submaps);
    }

    snapshot.auto_relocation_trigger_count =
        static_cast<int>(auto_relocation_triggers_.size());
    for (const AutoRelocationTriggerEvent& event :
         auto_relocation_triggers_) {
      snapshot.max_auto_relocation_queue_size =
          std::max(snapshot.max_auto_relocation_queue_size, event.queue_size);
    }
    snapshot.auto_relocation_result_count =
        static_cast<int>(auto_relocation_results_.size());
    for (const AutoRelocationResultEvent& event : auto_relocation_results_) {
      if (event.success) {
        ++snapshot.auto_relocation_success_count;
      }
    }
    return snapshot;
  }

 private:
  static double LowScoreThresholdPercent() { return 65.0; }

  void Prune(const TimePoint cutoff) {
    PruneByTime(&scores_, cutoff);
    PruneByTime(&queue_sizes_, cutoff);
    PruneByTime(&constraint_results_, cutoff);
    PruneByTime(&backpressure_, cutoff);
    PruneByTime(&active_frozen_candidates_, cutoff);
    PruneByTime(&active_frozen_candidate_full_map_, cutoff);
    PruneByTime(&current_pose_scan_map_, cutoff);
    PruneByTime(&map_scan_, cutoff);
    PruneByTime(&recovery_full_searches_, cutoff);
    PruneByTime(&force_optimizations_, cutoff);
    PruneByTime(&auto_relocation_triggers_, cutoff);
    PruneByTime(&auto_relocation_results_, cutoff);
    while (!active_frozen_ambiguous_rejects_.empty() &&
           active_frozen_ambiguous_rejects_.front() < cutoff) {
      active_frozen_ambiguous_rejects_.pop_front();
    }
    while (!active_frozen_geometry_rejects_.empty() &&
           active_frozen_geometry_rejects_.front() < cutoff) {
      active_frozen_geometry_rejects_.pop_front();
    }
    while (!active_frozen_full_map_rejects_.empty() &&
           active_frozen_full_map_rejects_.front() < cutoff) {
      active_frozen_full_map_rejects_.pop_front();
    }
    while (!active_frozen_consistency_rejects_.empty() &&
           active_frozen_consistency_rejects_.front() < cutoff) {
      active_frozen_consistency_rejects_.pop_front();
    }
    while (!active_frozen_consistency_accepts_.empty() &&
           active_frozen_consistency_accepts_.front() < cutoff) {
      active_frozen_consistency_accepts_.pop_front();
    }
    while (!active_frozen_accepted_constraints_.empty() &&
           active_frozen_accepted_constraints_.front() < cutoff) {
      active_frozen_accepted_constraints_.pop_front();
    }
  }

  std::mutex mutex_;
  bool observed_ = false;
  int latest_work_queue_size_ = -1;
  int latest_backpressure_keep_every_n_ = 1;
  int latest_active_submaps_ = 0;
  int latest_auto_relocation_queue_size_ = -1;
  int latest_auto_relocation_return_code_ = 0;
  double latest_match_score_percent_ = 0.0;
  double latest_active_frozen_top1_score_ = 0.0;
  double latest_active_frozen_top2_score_ = 0.0;
  double latest_active_frozen_margin_ = 0.0;
  double latest_active_frozen_hit20_ = -1.0;
  double latest_active_frozen_mean_distance_ = -1.0;
  double latest_active_frozen_free_space_conflict_ratio_ = -1.0;
  double latest_active_frozen_known_ratio_ = -1.0;
  double latest_active_frozen_sector_coverage_ = -1.0;
  double latest_active_frozen_candidate_map_hit20_ = -1.0;
  double latest_active_frozen_candidate_map_mean_distance_ = -1.0;
  double latest_active_frozen_candidate_map_known_ratio_ = -1.0;
  double latest_active_frozen_candidate_map_hit20_improvement_ = -1.0;
  double latest_active_frozen_candidate_map_mean_distance_improvement_ = -1.0;
  double latest_implied_correction_translation_m_ = 0.0;
  double latest_implied_correction_yaw_rad_ = 0.0;
  double latest_current_pose_scan_map_hit20_ = -1.0;
  double latest_current_pose_scan_map_mean_distance_ = -1.0;
  int latest_current_pose_scan_map_sampled_points_ = 0;
  int latest_current_pose_scan_map_checked_submaps_ = 0;
  double latest_map_scan_hit20_ = -1.0;
  double latest_map_scan_mean_distance_ = -1.0;
  double latest_map_scan_known_ratio_ = -1.0;
  int latest_map_scan_sampled_points_ = 0;
  int latest_map_scan_checked_submaps_ = 0;
  int latest_recovery_full_search_submap_count_ = 0;
  std::string recovery_state_ = "OK";
  std::string recovery_reason_;
  TimePoint last_active_frozen_accepted_constraint_time_;
  std::int64_t total_match_score_count_ = 0;
  std::int64_t total_backpressure_count_ = 0;
  std::int64_t total_active_frozen_accepted_constraint_count_ = 0;
  std::int64_t total_active_frozen_ambiguous_reject_count_ = 0;
  std::int64_t total_active_frozen_geometry_reject_count_ = 0;
  std::int64_t total_active_frozen_full_map_reject_count_ = 0;
  std::int64_t total_active_frozen_consistency_reject_count_ = 0;
  std::int64_t total_active_frozen_consistency_accept_count_ = 0;
  std::int64_t total_current_pose_scan_map_bad_count_ = 0;
  std::int64_t total_map_scan_bad_count_ = 0;
  std::int64_t total_recovery_full_search_count_ = 0;
  std::int64_t total_auto_relocation_trigger_count_ = 0;
  std::int64_t total_auto_relocation_success_count_ = 0;

  std::deque<ScoreEvent> scores_;
  std::deque<QueueEvent> queue_sizes_;
  std::deque<ConstraintResultEvent> constraint_results_;
  std::deque<BackpressureEvent> backpressure_;
  std::deque<ActiveFrozenCandidateEvent> active_frozen_candidates_;
  std::deque<ActiveFrozenCandidateFullMapEvent>
      active_frozen_candidate_full_map_;
  std::deque<CurrentPoseScanMapEvent> current_pose_scan_map_;
  std::deque<MapScanEvent> map_scan_;
  std::deque<RecoveryFullSearchEvent> recovery_full_searches_;
  std::deque<TimePoint> active_frozen_ambiguous_rejects_;
  std::deque<TimePoint> active_frozen_geometry_rejects_;
  std::deque<TimePoint> active_frozen_full_map_rejects_;
  std::deque<TimePoint> active_frozen_consistency_rejects_;
  std::deque<TimePoint> active_frozen_consistency_accepts_;
  std::deque<TimePoint> active_frozen_accepted_constraints_;
  std::deque<ForceOptEvent> force_optimizations_;
  std::deque<AutoRelocationTriggerEvent> auto_relocation_triggers_;
  std::deque<AutoRelocationResultEvent> auto_relocation_results_;
};

LocalizationHealthStore& Store() {
  static LocalizationHealthStore store;
  return store;
}

}  // namespace

void ResetLocalizationHealth() { Store().Reset(); }

void RecordLocalizationHealthMatchScore(const double score_percent) {
  Store().RecordScore(score_percent);
}

void RecordLocalizationHealthConstraintResult(
    const int computations, const int additional_constraints) {
  Store().RecordConstraintResult(computations, additional_constraints);
}

void RecordLocalizationHealthWorkQueueSize(const std::size_t queue_size) {
  Store().RecordQueueSize(queue_size);
}

void RecordLocalizationHealthBackpressure(const std::size_t queue_size,
                                          const std::size_t keep_every_n) {
  Store().RecordBackpressure(queue_size, keep_every_n);
}

void RecordLocalizationHealthActiveFrozenAcceptedConstraint() {
  Store().RecordActiveFrozenAcceptedConstraint();
}

void RecordLocalizationHealthActiveFrozenCandidate(
    const double top1_score, const double top2_score, const double margin,
    const double hit20, const double mean_distance,
    const double free_space_conflict_ratio, const double known_ratio,
    const double sector_coverage,
    const double implied_translation_m, const double implied_yaw_rad) {
  Store().RecordActiveFrozenCandidate(top1_score, top2_score, margin, hit20,
                                      mean_distance,
                                      free_space_conflict_ratio, known_ratio,
                                      sector_coverage, implied_translation_m,
                                      implied_yaw_rad);
}

void RecordLocalizationHealthActiveFrozenCandidateFullMap(
    const double hit20, const double mean_distance, const double known_ratio,
    const double hit20_improvement,
    const double mean_distance_improvement) {
  Store().RecordActiveFrozenCandidateFullMap(
      hit20, mean_distance, known_ratio, hit20_improvement,
      mean_distance_improvement);
}

void RecordLocalizationHealthCurrentPoseScanMapQuality(
    const double hit20, const double mean_distance, const int sampled_points,
    const int checked_submaps, const bool bad) {
  Store().RecordCurrentPoseScanMapQuality(hit20, mean_distance, sampled_points,
                                          checked_submaps, bad);
}

void RecordLocalizationHealthMapScanQuality(
    const double hit20, const double mean_distance, const double known_ratio,
    const int sampled_points, const int checked_submaps, const bool bad) {
  Store().RecordMapScanQuality(hit20, mean_distance, known_ratio,
                               sampled_points, checked_submaps, bad);
}

void RecordLocalizationHealthRecoveryState(const std::string& state,
                                           const std::string& reason) {
  Store().RecordRecoveryState(state, reason);
}

void RecordLocalizationHealthRecoveryFullSearch(
    const int checked_submaps, const std::string& reason) {
  Store().RecordRecoveryFullSearch(checked_submaps, reason);
}

void RecordLocalizationHealthActiveFrozenAmbiguousReject() {
  Store().RecordActiveFrozenAmbiguousReject();
}

void RecordLocalizationHealthActiveFrozenGeometryReject() {
  Store().RecordActiveFrozenGeometryReject();
}

void RecordLocalizationHealthActiveFrozenFullMapReject() {
  Store().RecordActiveFrozenFullMapReject();
}

void RecordLocalizationHealthActiveFrozenConsistencyReject() {
  Store().RecordActiveFrozenConsistencyReject();
}

void RecordLocalizationHealthActiveFrozenConsistencyAccept() {
  Store().RecordActiveFrozenConsistencyAccept();
}

void RecordLocalizationHealthPureLocalizationForceOptimization(
    const int active_submaps) {
  Store().RecordForceOptimization(active_submaps);
}

void RecordLocalizationHealthAutoRelocationTrigger(
    const std::size_t queue_size) {
  Store().RecordAutoRelocationTrigger(queue_size);
}

void RecordLocalizationHealthAutoRelocationResult(const int return_code,
                                                  const bool success) {
  Store().RecordAutoRelocationResult(return_code, success);
}

LocalizationHealthSnapshot GetLocalizationHealthSnapshot(
    const double window_seconds) {
  return Store().Snapshot(window_seconds);
}

}  // namespace mapping
}  // namespace cartographer

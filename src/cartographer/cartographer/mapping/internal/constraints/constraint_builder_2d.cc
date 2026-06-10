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

#include "cartographer/mapping/internal/constraints/constraint_builder_2d.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <functional>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <tuple>
#include <utility>

#include "Eigen/Eigenvalues"
#include "absl/memory/memory.h"
#include "cartographer/common/math.h"
#include "cartographer/common/thread_pool.h"
#include "cartographer/mapping/flirt.h"
#include "cartographer/mapping/localization_health.h"
#include "cartographer/mapping/proto/scan_matching/ceres_scan_matcher_options_2d.pb.h"
#include "cartographer/mapping/proto/scan_matching/fast_correlative_scan_matcher_options_2d.pb.h"
#include "cartographer/metrics/counter.h"
#include "cartographer/metrics/gauge.h"
#include "cartographer/metrics/histogram.h"
#include "cartographer/transform/transform.h"
#include "constraint_builder_2d.h"
#include "glog/logging.h"

namespace cartographer {
namespace mapping {
namespace constraints {

static auto* kConstraintsSearchedMetric = metrics::Counter::Null();
static auto* kConstraintsFoundMetric = metrics::Counter::Null();
static auto* kGlobalConstraintsSearchedMetric = metrics::Counter::Null();
static auto* kGlobalConstraintsFoundMetric = metrics::Counter::Null();
static auto* kQueueLengthMetric = metrics::Gauge::Null();
static auto* kConstraintScoresMetric = metrics::Histogram::Null();
static auto* kGlobalConstraintScoresMetric = metrics::Histogram::Null();
static auto* kNumSubmapScanMatchersMetric = metrics::Gauge::Null();

// 返回submap的原点在local坐标系下的二维坐标
transform::Rigid2d ComputeSubmapPose(const Submap2D& submap) {
  return transform::Project2D(submap.local_pose());
}

double NormalizeAngleDifference(const double angle) {
  return std::atan2(std::sin(angle), std::cos(angle));
}

constexpr int kTopCandidateCount = 5;
constexpr float kTopCandidateShadowScoreMargin = 0.02f;
constexpr double kGeometryHitRadiusMeters = 0.20;
constexpr double kGeometrySearchRadiusMeters = 1.00;
constexpr double kOccupiedProbabilityThreshold = 0.55;
constexpr double kFreeSpaceProbabilityThreshold = 0.45;
constexpr int kGeometryMaxSampledPoints = 384;
constexpr int kGeometrySectorCount = 12;
constexpr auto kGlobalRelocationConstraintTimeout = std::chrono::seconds(10);

ConstraintBuilder2D::GeometryQuality ComputeGeometryQuality(
    const Grid2D& grid, const sensor::PointCloud& point_cloud,
    const transform::Rigid2d& pose_estimate) {
  ConstraintBuilder2D::GeometryQuality quality;
  if (point_cloud.empty()) {
    return quality;
  }

  const double resolution = grid.limits().resolution();
  const int hit_radius_cells =
      std::max(1, common::RoundToInt(kGeometryHitRadiusMeters / resolution));
  const int search_radius_cells =
      std::max(hit_radius_cells,
               common::RoundToInt(kGeometrySearchRadiusMeters / resolution));
  const int sample_step = std::max(
      1, static_cast<int>(point_cloud.size() / kGeometryMaxSampledPoints));

  int sampled_points = 0;
  int hit20_count = 0;
  int known_count = 0;
  int free_space_conflict_count = 0;
  std::array<bool, kGeometrySectorCount> observed_sectors{};
  double distance_sum = 0.0;

  for (size_t point_index = 0; point_index < point_cloud.size();
       point_index += sample_step) {
    const Eigen::Vector2d local_point(
        point_cloud[point_index].position.x(),
        point_cloud[point_index].position.y());
    const double local_angle = std::atan2(local_point.y(), local_point.x());
    int sector =
        static_cast<int>(std::floor((local_angle + M_PI) /
                                    (2.0 * M_PI) * kGeometrySectorCount));
    sector = std::max(0, std::min(kGeometrySectorCount - 1, sector));
    observed_sectors[sector] = true;

    const Eigen::Vector2f point = (pose_estimate * local_point).cast<float>();
    const Eigen::Array2i cell_index = grid.limits().GetCellIndex(point);
    ++sampled_points;

    if (grid.limits().Contains(cell_index) && grid.IsKnown(cell_index)) {
      ++known_count;
      const double probability = 1.0 - grid.GetCorrespondenceCost(cell_index);
      if (probability <= kFreeSpaceProbabilityThreshold) {
        ++free_space_conflict_count;
      }
    }

    double best_distance = kGeometrySearchRadiusMeters;
    bool found_occupied = false;
    for (int dx = -search_radius_cells; dx <= search_radius_cells; ++dx) {
      for (int dy = -search_radius_cells; dy <= search_radius_cells; ++dy) {
        const Eigen::Array2i candidate_index =
            cell_index + Eigen::Array2i(dx, dy);
        if (!grid.limits().Contains(candidate_index) ||
            !grid.IsKnown(candidate_index)) {
          continue;
        }
        const double probability =
            1.0 - grid.GetCorrespondenceCost(candidate_index);
        if (probability < kOccupiedProbabilityThreshold) {
          continue;
        }
        const Eigen::Vector2d cell_center =
            grid.limits().GetCellCenter(candidate_index).cast<double>();
        const double distance = (cell_center - point.cast<double>()).norm();
        if (distance < best_distance) {
          best_distance = distance;
          found_occupied = true;
        }
      }
    }
    if (found_occupied && best_distance <= kGeometryHitRadiusMeters) {
      ++hit20_count;
    }
    distance_sum += found_occupied ? best_distance : kGeometrySearchRadiusMeters;
  }

  if (sampled_points > 0) {
    quality.hit20 = static_cast<double>(hit20_count) /
                    static_cast<double>(sampled_points);
    quality.mean_distance = distance_sum / static_cast<double>(sampled_points);
    quality.free_space_conflict_ratio =
        static_cast<double>(free_space_conflict_count) /
        static_cast<double>(sampled_points);
    quality.known_ratio =
        static_cast<double>(known_count) / static_cast<double>(sampled_points);
    const int sector_count =
        std::count(observed_sectors.begin(), observed_sectors.end(), true);
    quality.sector_coverage =
        static_cast<double>(sector_count) /
        static_cast<double>(kGeometrySectorCount);
  }
  return quality;
}

ConstraintBuilder2D::ConstraintBuilder2D(
    const constraints::proto::ConstraintBuilderOptions& options,
    common::ThreadPoolInterface* const thread_pool)
    : options_(options),
      thread_pool_(thread_pool),
      finish_node_task_(absl::make_unique<common::Task>()),
      when_done_task_(absl::make_unique<common::Task>()),
      ceres_scan_matcher_(options.ceres_scan_matcher_options()) {}

ConstraintBuilder2D::~ConstraintBuilder2D() {
  absl::MutexLock locker(&mutex_);
  CHECK_EQ(finish_node_task_->GetState(), common::Task::NEW);
  CHECK_EQ(when_done_task_->GetState(), common::Task::NEW);
  CHECK_EQ(constraints_.size(), 0) << "WhenDone() was not called";
  CHECK_EQ(num_started_nodes_, num_finished_nodes_);
  CHECK(when_done_ == nullptr);
}

/**
 * @brief 进行局部搜索窗口的约束计算(对局部子图进行回环检测)
 * 
 * @param[in] submap_id submap的id
 * @param[in] submap 单个submap
 * @param[in] node_id 节点的id
 * @param[in] constant_data 节点的数据
 * @param[in] initial_relative_pose 约束的初值
 */
void ConstraintBuilder2D::MaybeAddConstraint(
    const SubmapId& submap_id, const Submap2D* const submap,
    const NodeId& node_id, const TrajectoryNode::Data* const constant_data,
    const transform::Rigid2d& initial_relative_pose,
    const bool collect_top_candidates) {
  // 超过范围的不进行约束的计算
  if (initial_relative_pose.translation().norm() >
      options_.max_constraint_distance()) {
    return;
  }
  if (!per_submap_sampler_
           .emplace(std::piecewise_construct, std::forward_as_tuple(submap_id),
                    std::forward_as_tuple(options_.sampling_ratio()))
           .first->second.Pulse()) {
    return;
  }

  absl::MutexLock locker(&mutex_);
  if (when_done_) {
    LOG(WARNING)
        << "MaybeAddConstraint was called while WhenDone was scheduled.";
  }
  // 在队列中新建一个指向Constraint数据的指针
  constraints_.emplace_back();
  kQueueLengthMetric->Set(constraints_.size());
  auto* const constraint = &constraints_.back();
  // 为子图新建一个匹配器
  const auto* scan_matcher =
      DispatchScanMatcherConstruction(submap_id, submap->grid());
  // 生成个计算约束的任务
  auto constraint_task = absl::make_unique<common::Task>();
  constraint_task->SetWorkItem([=]() LOCKS_EXCLUDED(mutex_) {
    ComputeConstraint(submap_id, submap, node_id, false, /* match_full_submap */
                      collect_top_candidates,
                      constant_data, initial_relative_pose, *scan_matcher,
                      constraint);
  });
  constraint_task->AddDependency(scan_matcher->creation_task_handle);
  auto constraint_task_handle =
      thread_pool_->Schedule(std::move(constraint_task));
  finish_node_task_->AddDependency(constraint_task_handle);
}

void ConstraintBuilder2D::MaybeAddGlobalConstraint(
    const SubmapId& submap_id, const Submap2D* const submap,
    const NodeId& node_id, const TrajectoryNode::Data* const constant_data,
    const bool collect_top_candidates) {
  absl::MutexLock locker(&mutex_);
  if (when_done_) {
    LOG(WARNING)
        << "MaybeAddGlobalConstraint was called while WhenDone was scheduled.";
  }
  // note: 对整体子图进行回环检测时没有距离的限制
  constraints_.emplace_back();
  kQueueLengthMetric->Set(constraints_.size());
  auto* const constraint = &constraints_.back();
  const auto* scan_matcher =
      DispatchScanMatcherConstruction(submap_id, submap->grid());
  auto constraint_task = absl::make_unique<common::Task>();
  // 生成个计算全局约束的任务
  constraint_task->SetWorkItem([=]() LOCKS_EXCLUDED(mutex_) {
    ComputeConstraint(submap_id, submap, node_id, true, /* match_full_submap */
                      collect_top_candidates,
                      constant_data, transform::Rigid2d::Identity(),
                      *scan_matcher, constraint);
  });
  constraint_task->AddDependency(scan_matcher->creation_task_handle);
  auto constraint_task_handle =
      thread_pool_->Schedule(std::move(constraint_task));
  finish_node_task_->AddDependency(constraint_task_handle);
}
// 告诉ConstraintBuilder2D的对象, 刚刚完成了一个节点的约束的计算
void ConstraintBuilder2D::NotifyEndOfNode() {
  absl::MutexLock locker(&mutex_);
  CHECK(finish_node_task_ != nullptr);
  // 生成个任务: 将num_finished_nodes_自加, 记录完成约束计算节点的总个数
  finish_node_task_->SetWorkItem([this] {
    absl::MutexLock locker(&mutex_);
    ++num_finished_nodes_;
  });
  auto finish_node_task_handle =
      thread_pool_->Schedule(std::move(finish_node_task_));
  finish_node_task_ = absl::make_unique<common::Task>();
  when_done_task_->AddDependency(finish_node_task_handle);
  ++num_started_nodes_;
}

// 约束计算完成之后执行一下回调函数
void ConstraintBuilder2D::WhenDone(
    const std::function<void(const ConstraintBuilder2D::Result&)>& callback) {
  absl::MutexLock locker(&mutex_);
  CHECK(when_done_ == nullptr);
  // TODO(gaschler): Consider using just std::function, it can also be empty.
  when_done_ = absl::make_unique<std::function<void(const Result&)>>(callback);
  CHECK(when_done_task_ != nullptr);
  when_done_task_->SetWorkItem([this] { RunWhenDoneCallback(); });
  thread_pool_->Schedule(std::move(when_done_task_));
  when_done_task_ = absl::make_unique<common::Task>();
}

const ConstraintBuilder2D::SubmapScanMatcher*
ConstraintBuilder2D::DispatchScanMatcherConstruction(const SubmapId& submap_id,
                                                     const Grid2D* const grid) {
  CHECK(grid);
  if (submap_scan_matchers_.count(submap_id) != 0) {
    return &submap_scan_matchers_.at(submap_id);
  }
  auto& submap_scan_matcher = submap_scan_matchers_[submap_id];
  kNumSubmapScanMatchersMetric->Set(submap_scan_matchers_.size());
  submap_scan_matcher.grid = grid;
  auto& scan_matcher_options = options_.fast_correlative_scan_matcher_options();
  auto scan_matcher_task = absl::make_unique<common::Task>();
  scan_matcher_task->SetWorkItem(
      [&submap_scan_matcher, &scan_matcher_options]() {
        submap_scan_matcher.fast_correlative_scan_matcher =
            absl::make_unique<scan_matching::FastCorrelativeScanMatcher2D>(
                *submap_scan_matcher.grid, scan_matcher_options);
      });
  submap_scan_matcher.creation_task_handle =
      thread_pool_->Schedule(std::move(scan_matcher_task));
  return &submap_scan_matchers_.at(submap_id);
}

/**
 * @brief 计算节点和子图之间的一个约束(回环检测)
 *        用基于分支定界算法的匹配器进行粗匹配,然后用ceres进行精匹配
 * 
 * @param[in] submap_id submap的id
 * @param[in] submap 地图数据
 * @param[in] node_id 节点id
 * @param[in] match_full_submap 是局部匹配还是全子图匹配
 * @param[in] constant_data 节点数据
 * @param[in] initial_relative_pose 约束的初值
 * @param[in] submap_scan_matcher 匹配器
 * @param[out] constraint 计算出的约束
 */
void ConstraintBuilder2D::ComputeConstraint(
    const SubmapId& submap_id, const Submap2D* const submap,
    const NodeId& node_id, bool match_full_submap,
    const bool collect_top_candidates,
    const TrajectoryNode::Data* const constant_data,
    const transform::Rigid2d& initial_relative_pose,
    const SubmapScanMatcher& submap_scan_matcher,
    std::unique_ptr<ConstraintBuilder2D::ConstraintCandidate>* constraint) {
  CHECK(submap_scan_matcher.fast_correlative_scan_matcher);
    // Step:1 得到节点在local frame下的坐标
  const transform::Rigid2d initial_pose =
      ComputeSubmapPose(*submap) * initial_relative_pose;

  // The 'constraint_transform' (submap i <- node j) is computed from:
  // - a 'filtered_gravity_aligned_point_cloud' in node j,
  // - the initial guess 'initial_pose' for (map <- node j),
  // - the result 'pose_estimate' of Match() (map <- node j).
  // - the ComputeSubmapPose() (map <- submap i)
  float score = 0.;
  transform::Rigid2d pose_estimate = transform::Rigid2d::Identity();

  // Compute 'pose_estimate' in three stages:
  // 1. Fast estimate using the fast correlative scan matcher.
  // 2. Prune if the score is too low.
  // 3. Refine.

  // Step:2 使用基于分支定界算法的匹配器进行粗匹配
  std::vector<scan_matching::FastCorrelativeScanMatcher2D::ScoredPose>
      top_candidates;
  if (match_full_submap) {
    kGlobalConstraintsSearchedMetric->Increment();
    const bool matched =
        collect_top_candidates
            ? submap_scan_matcher.fast_correlative_scan_matcher
                  ->MatchFullSubmapWithTopCandidates(
                      constant_data->filtered_gravity_aligned_point_cloud,
                      options_.global_localization_min_score(),
                      kTopCandidateCount, kTopCandidateShadowScoreMargin,
                      &score, &pose_estimate, &top_candidates)
            : submap_scan_matcher.fast_correlative_scan_matcher
                  ->MatchFullSubmap(
                      constant_data->filtered_gravity_aligned_point_cloud,
                      options_.global_localization_min_score(), &score,
                      &pose_estimate);
    if (matched) {
      CHECK_GT(score, options_.global_localization_min_score());
      CHECK_GE(node_id.trajectory_id, 0);
      CHECK_GE(submap_id.trajectory_id, 0);

      // double x = pose_estimate.translation().x();
      // double y = pose_estimate.translation().y();
      // double angle = pose_estimate.rotation().angle();
      // LOG(WARNING) << "[Fast] Original Pose=" << x << "|" << y << "|" <<
      // angle;

      kGlobalConstraintsFoundMetric->Increment();
      kGlobalConstraintScoresMetric->Observe(score);
    } else {
      return;
    }
  } else {
    kConstraintsSearchedMetric->Increment();
    const bool matched =
        collect_top_candidates
            ? submap_scan_matcher.fast_correlative_scan_matcher
                  ->MatchWithTopCandidates(
                      initial_pose,
                      constant_data->filtered_gravity_aligned_point_cloud,
                      options_.min_score(), kTopCandidateCount,
                      kTopCandidateShadowScoreMargin, &score, &pose_estimate,
                      &top_candidates)
            : submap_scan_matcher.fast_correlative_scan_matcher->Match(
                  initial_pose,
                  constant_data->filtered_gravity_aligned_point_cloud,
                  options_.min_score(), &score, &pose_estimate);
    if (matched) {
      // We've reported a successful local match.
      CHECK_GT(score, options_.min_score());
      kConstraintsFoundMetric->Increment();
      kConstraintScoresMetric->Observe(score);
    } else {
      return;
    }
  }
  {
    absl::MutexLock locker(&mutex_);
    score_histogram_.Add(score);
  }
  RecordLocalizationHealthMatchScore(100. * score);

  // Use the CSM estimate as both the initial and previous pose. This has the
  // effect that, in the absence of better information, we prefer the original
  // CSM estimate.
    // Step:3 使用ceres进行精匹配, 就是前端扫描匹配使用的函数
  ceres::Solver::Summary unused_summary;
  ceres_scan_matcher_.Match(pose_estimate.translation(), pose_estimate,
                            constant_data->filtered_gravity_aligned_point_cloud,
                            *submap_scan_matcher.grid, &pose_estimate,
                            &unused_summary);

  // Step:4 获取节点到submap坐标系原点间的坐标变换
  const transform::Rigid2d constraint_transform =
      ComputeSubmapPose(*submap).inverse() * pose_estimate;
  auto candidate = absl::make_unique<ConstraintCandidate>();
  candidate->constraint =
      Constraint{submap_id,
                 node_id,
                 {transform::Embed3D(constraint_transform),
                  options_.loop_closure_translation_weight(),
                  options_.loop_closure_rotation_weight()},
                 Constraint::INTER_SUBMAP};
  candidate->fast_score = score;
  candidate->match_full_submap = match_full_submap;
  candidate->top_candidates = top_candidates;
  if (collect_top_candidates) {
    candidate->geometry_quality = ComputeGeometryQuality(
        *submap_scan_matcher.grid,
        constant_data->filtered_gravity_aligned_point_cloud, pose_estimate);
  }
  *constraint = std::move(candidate);
  // if (submap_id.trajectory_id != node_id.trajectory_id) {
  //   common::reloced = 1;
  //   LOG(WARNING) << "Relocation finished";
  // }

  if (options_.log_matches()) {
    std::ostringstream info;
    info << "Node " << node_id << " with "
         << constant_data->filtered_gravity_aligned_point_cloud.size()
         << " points on submap " << submap_id << std::fixed;
    if (match_full_submap) {
      info << " matches";
    } else {
      const transform::Rigid2d difference =
          initial_pose.inverse() * pose_estimate;
      info << " differs by translation " << std::setprecision(2)
           << difference.translation().norm() << " rotation "
           << std::setprecision(3) << std::abs(difference.normalized_angle());
    }
    info << " with score " << std::setprecision(1) << 100. * score << "%.";
    LOG(INFO) << info.str();
  }
}

// 将临时保存的所有约束数据传入回调函数, 并执行回调函数
void ConstraintBuilder2D::RunWhenDoneCallback() {
  Result result;
  std::unique_ptr<std::function<void(const Result&)>> callback;
  {
    absl::MutexLock locker(&mutex_);
    CHECK(when_done_ != nullptr);
    for (const std::unique_ptr<ConstraintCandidate>& constraint : constraints_) {
      if (constraint == nullptr) continue;
      result.push_back(*constraint);
    }
    RecordLocalizationHealthConstraintResult(
        static_cast<int>(constraints_.size()), static_cast<int>(result.size()));
    if (options_.log_matches()) {
      LOG(INFO) << constraints_.size() << " computations resulted in "
                << result.size() << " additional constraints.";
      LOG(INFO) << "Score histogram:\n" << score_histogram_.ToString(10);
    }
    constraints_.clear();
    callback = std::move(when_done_);
    when_done_.reset();
    kQueueLengthMetric->Set(constraints_.size());
  }
  (*callback)(result);
}

int ConstraintBuilder2D::GetNumFinishedNodes() {
  absl::MutexLock locker(&mutex_);
  return num_finished_nodes_;
}

void ConstraintBuilder2D::DeleteScanMatcher(const SubmapId& submap_id) {
  absl::MutexLock locker(&mutex_);
  if (when_done_) {
    LOG(WARNING)
        << "DeleteScanMatcher was called while WhenDone was scheduled.";
  }
  submap_scan_matchers_.erase(submap_id);
  per_submap_sampler_.erase(submap_id);
  kNumSubmapScanMatchersMetric->Set(submap_scan_matchers_.size());
}

void ConstraintBuilder2D::RegisterMetrics(metrics::FamilyFactory* factory) {
  auto* counts = factory->NewCounterFamily(
      "mapping_constraints_constraint_builder_2d_constraints",
      "Constraints computed");
  kConstraintsSearchedMetric =
      counts->Add({{"search_region", "local"}, {"matcher", "searched"}});
  kConstraintsFoundMetric =
      counts->Add({{"search_region", "local"}, {"matcher", "found"}});
  kGlobalConstraintsSearchedMetric =
      counts->Add({{"search_region", "global"}, {"matcher", "searched"}});
  kGlobalConstraintsFoundMetric =
      counts->Add({{"search_region", "global"}, {"matcher", "found"}});
  auto* queue_length = factory->NewGaugeFamily(
      "mapping_constraints_constraint_builder_2d_queue_length", "Queue length");
  kQueueLengthMetric = queue_length->Add({});
  auto boundaries = metrics::Histogram::FixedWidth(0.05, 20);
  auto* scores = factory->NewHistogramFamily(
      "mapping_constraints_constraint_builder_2d_scores",
      "Constraint scores built", boundaries);
  kConstraintScoresMetric = scores->Add({{"search_region", "local"}});
  kGlobalConstraintScoresMetric = scores->Add({{"search_region", "global"}});
  auto* num_matchers = factory->NewGaugeFamily(
      "mapping_constraints_constraint_builder_2d_num_submap_scan_matchers",
      "Current number of constructed submap scan matchers");
  kNumSubmapScanMatchersMetric = num_matchers->Add({});
}

#ifdef CARTOGRAPHER_RELOCATE

bool ConstraintBuilder2D::ComputeConstraintWithEstimatedPoses(
    std::vector<EstimatedPose> poses,
    const TrajectoryNode::Data* const constant_data,
    std::string* const rejection_reason) {
  // This part will compute FastCorrelativeScanMatcher score for each poses.
  // Only the top One can be the final constraint.
  // Promise Model:
  // DispatchScanMatcher -> ComputeConstraints[1,2,...,N] -> AddConstaint

  const float min_score = static_cast<float>(flirt::relocation_min_score.load());
  const auto scan_matcher_options =
      options_.fast_correlative_scan_matcher_options();

  struct ScanMatchResult {
    float score = 0.f;
    cartographer::transform::Rigid2d matched_pose;
    const EstimatedPose* estimated_pose = nullptr;
  };

  struct WaitState {
    std::mutex mutex;
    std::condition_variable cv;
    bool done = false;
    bool timed_out = false;
    bool constraint_added = false;
    std::string rejection_reason;
    std::vector<EstimatedPose> poses;
    std::vector<ScanMatchResult> match_result;
  };

  auto state = std::make_shared<WaitState>();
  state->poses = std::move(poses);
  state->match_result.resize(state->poses.size());

  auto finish = [state](const bool constraint_added,
                        const std::string& reason) {
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      state->constraint_added = constraint_added;
      if (!reason.empty()) {
        state->rejection_reason = reason;
      }
      state->done = true;
    }
    state->cv.notify_all();
  };

  auto timed_out = [state]() {
    std::lock_guard<std::mutex> lock(state->mutex);
    return state->timed_out;
  };

  auto add_constrain_task = absl::make_unique<common::Task>();
  add_constrain_task->SetWorkItem([this, state, finish, timed_out, min_score,
                                   constant_data]() LOCKS_EXCLUDED(mutex_) {
    // Get the final Constraint, according to the score
    if (state->match_result.empty() || timed_out()) {
      finish(false, timed_out() ? "relocation_timeout"
                                : "relocation_no_candidate");
      return;
    }

    for (auto&& i : state->match_result) {
      LOG(WARNING) << "Match Score of Submap" << i.estimated_pose->submap_id
                   << "=" << i.score;
    }

    std::sort(
        state->match_result.begin(), state->match_result.end(),
        [](const ScanMatchResult& lhs, const ScanMatchResult& rhs) -> bool {
          return lhs.score > rhs.score;
        });

    auto& result = state->match_result.at(0);

    if (result.score < min_score) {
      LOG(WARNING) << "[GlobalRelocation]Rejected low score=" << result.score
                   << " min_score=" << min_score
                   << " submap=" << result.estimated_pose->submap_id;
      flirt::reset_relocation_consistency();
      finish(false, "relocation_low_score");
      return;
    }

    const double x = result.matched_pose.translation().x();
    const double y = result.matched_pose.translation().y();
    const double theta = result.matched_pose.rotation().angle();
    const int required_hits =
        std::max(1, flirt::relocation_required_consistent_hits.load());
    const int max_submap_delta =
        std::max(0, flirt::relocation_consistency_max_submap_index_delta.load());
    const double max_translation =
        std::max(0.0, flirt::relocation_consistency_max_translation_m.load());
    const double max_rotation =
        std::max(0.0, flirt::relocation_consistency_max_rotation_rad.load());

    int consistency_hits = 1;
    bool consistent_with_last = true;
    {
      std::lock_guard<std::mutex> consistency_lock(
          flirt::relocation_consistency_lock);
      if (flirt::relocation_has_last_candidate) {
        const bool same_trajectory =
            result.estimated_pose->submap_id.trajectory_id ==
            flirt::relocation_last_trajectory_id;
        const bool adjacent_submap =
            std::abs(result.estimated_pose->submap_id.submap_index -
                     flirt::relocation_last_submap_index) <= max_submap_delta;
        const double translation_delta =
            std::hypot(x - flirt::relocation_last_x,
                       y - flirt::relocation_last_y);
        const double rotation_delta = std::abs(
            NormalizeAngleDifference(theta - flirt::relocation_last_theta));
        consistent_with_last = same_trajectory && adjacent_submap &&
                               translation_delta <= max_translation &&
                               rotation_delta <= max_rotation;
      }

      flirt::relocation_consistency_hits =
          (flirt::relocation_has_last_candidate && consistent_with_last)
              ? flirt::relocation_consistency_hits + 1
              : 1;
      consistency_hits = flirt::relocation_consistency_hits;
      flirt::relocation_has_last_candidate = true;
      flirt::relocation_last_trajectory_id =
          result.estimated_pose->submap_id.trajectory_id;
      flirt::relocation_last_submap_index =
          result.estimated_pose->submap_id.submap_index;
      flirt::relocation_last_x = x;
      flirt::relocation_last_y = y;
      flirt::relocation_last_theta = theta;
      flirt::relocation_last_score = result.score;
    }

    LOG(WARNING) << "[GlobalRelocation]Consistency hits=" << consistency_hits
                 << "/" << required_hits
                 << " consistent=" << (consistent_with_last ? "true" : "false")
                 << " score=" << result.score << " min_score=" << min_score
                 << " submap=("
                 << result.estimated_pose->submap_id.trajectory_id << ", "
                 << result.estimated_pose->submap_id.submap_index << ")"
                 << " pose=" << x << "|" << y << "|" << theta;

    if (consistency_hits < required_hits) {
      LOG(WARNING) << "[GlobalRelocation]Waiting for consistent relocation "
                   << "candidates before adding constraint.";
      finish(false, "relocation_consistency");
      return;
    }

    kGlobalConstraintsFoundMetric->Increment();
    kGlobalConstraintScoresMetric->Observe(result.score);

    LOG(WARNING) << "[Fast] Pose= " << x << "|" << y << "|" << theta;

    // Use CSM optimization
    ceres::Solver::Summary unused_summary;
    ceres_scan_matcher_.Match(
        result.matched_pose.translation(), result.matched_pose,
        constant_data->filtered_gravity_aligned_point_cloud,
        *result.estimated_pose->submap->grid(), &result.matched_pose,
        &unused_summary);

    if (timed_out()) {
      LOG(WARNING) << "[GlobalRelocation]Discard timed-out relocation "
                   << "constraint after Ceres submap="
                   << result.estimated_pose->submap_id;
      finish(false, "relocation_timeout");
      return;
    }

    const transform::Rigid2d constraint_transform =
        ComputeSubmapPose(*result.estimated_pose->submap).inverse() *
        result.matched_pose;

    const double ceres_x = result.matched_pose.translation().x();
    const double ceres_y = result.matched_pose.translation().y();
    const double ceres_theta = result.matched_pose.rotation().angle();
    LOG(WARNING) << "[Ceres] Pose= " << ceres_x << "|" << ceres_y << "|"
                 << ceres_theta;

    auto candidate = absl::make_unique<ConstraintCandidate>();
    candidate->constraint =
        Constraint{result.estimated_pose->submap_id,
                   result.estimated_pose->node_id,
                   {transform::Embed3D(constraint_transform),
                    options_.loop_closure_translation_weight(),
                    options_.loop_closure_rotation_weight()},
                   Constraint::INTER_SUBMAP};
    candidate->fast_score = result.score;
    candidate->match_full_submap = true;
    {
      absl::MutexLock locker(&mutex_);
      if (timed_out()) {
        LOG(WARNING) << "[GlobalRelocation]Discard timed-out relocation "
                     << "constraint before enqueue submap="
                     << result.estimated_pose->submap_id;
        finish(false, "relocation_timeout");
        return;
      }
      constraints_.emplace_back();
      kQueueLengthMetric->Set(constraints_.size());
      constraints_.back() = std::move(candidate);
    }

    flirt::need_optimizing.store(true);
    flirt::reset_relocation_consistency();
    finish(true, "");
  });

  // Create tasks to generate scan_matcher.
  for (size_t i = 0; i < state->poses.size(); ++i) {
    auto& pose = state->poses[i];
    ScanMatchResult* result = &state->match_result[i];
    result->estimated_pose = &pose;

    // Compute Constaint Task

    auto scan_match_task = absl::make_unique<common::Task>();
    scan_match_task->SetWorkItem(
        [state, i, constant_data, scan_matcher_options,
         timed_out]() LOCKS_EXCLUDED(mutex_) {
          if (timed_out()) {
            return;
          }
          auto& pose = state->poses[i];
          auto& result = state->match_result[i];
          if (pose.submap == nullptr || pose.submap->grid() == nullptr) {
            LOG(WARNING) << "[GlobalRelocation]Skip relocation candidate with "
                         << "invalid submap=" << pose.submap_id;
            return;
          }
          scan_matching::FastCorrelativeScanMatcher2D scan_matcher(
              *pose.submap->grid(), scan_matcher_options);
          scan_matcher.MatchFullSubmapWithPose(
              pose.pose, constant_data->filtered_gravity_aligned_point_cloud,
              &result.score, &result.matched_pose);
        });

    auto constraint_task_handle =
        thread_pool_->Schedule(std::move(scan_match_task));

    add_constrain_task->AddDependency(constraint_task_handle);
  }

  // Make sure that all of the dependencies have been added.
  thread_pool_->Schedule(std::move(add_constrain_task));

  LOG(WARNING) << "Scheduled";

  std::unique_lock<std::mutex> wait_lock(state->mutex);
  if (!state->cv.wait_for(wait_lock, kGlobalRelocationConstraintTimeout,
                          [&state]() { return state->done; })) {
    state->timed_out = true;
    wait_lock.unlock();
    state->cv.notify_all();
    LOG(WARNING) << "[GlobalRelocation]Timed out waiting for relocation "
                 << "constraint after "
                 << kGlobalRelocationConstraintTimeout.count() << "s.";
    flirt::reset_relocation_consistency();
    if (rejection_reason != nullptr) {
      *rejection_reason = "relocation_timeout";
    }
    return false;
  }
  if (rejection_reason != nullptr) {
    *rejection_reason = state->rejection_reason;
  }
  return state->constraint_added;
}

#endif

}  // namespace constraints
}  // namespace mapping
}  // namespace cartographer

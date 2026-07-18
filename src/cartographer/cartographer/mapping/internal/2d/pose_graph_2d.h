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

#ifndef CARTOGRAPHER_MAPPING_INTERNAL_2D_POSE_GRAPH_2D_H_
#define CARTOGRAPHER_MAPPING_INTERNAL_2D_POSE_GRAPH_2D_H_

#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <future>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "Eigen/Core"
#include "Eigen/Geometry"
#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "cartographer/common/fixed_ratio_sampler.h"
#include "cartographer/common/thread_pool.h"
#include "cartographer/common/time.h"
#include "cartographer/mapping/2d/submap_2d.h"
#include "cartographer/mapping/internal/2d/map_scan_distance_field.h"
#include "cartographer/mapping/internal/constraints/constraint_builder_2d.h"
#include "cartographer/mapping/internal/optimization/optimization_problem_2d.h"
#include "cartographer/mapping/internal/pose_graph_data.h"
#include "cartographer/mapping/internal/trajectory_connectivity_state.h"
#include "cartographer/mapping/internal/work_queue.h"
#include "cartographer/mapping/pose_graph.h"
#include "cartographer/mapping/pose_graph_trimmer.h"
#include "cartographer/mapping/value_conversion_tables.h"
#include "cartographer/metrics/family_factory.h"
#include "cartographer/sensor/fixed_frame_pose_data.h"
#include "cartographer/sensor/landmark_data.h"
#include "cartographer/sensor/odometry_data.h"
#include "cartographer/sensor/point_cloud.h"
#include "cartographer/transform/rigid_transform.h"
#include "cartographer/transform/transform.h"

namespace cartographer {
namespace mapping {

// Implements the loop closure method called Sparse Pose Adjustment (SPA) from
// Konolige, Kurt, et al. "Efficient sparse pose adjustment for 2d mapping."
// Intelligent Robots and Systems (IROS), 2010 IEEE/RSJ International Conference
// on (pp. 22--29). IEEE, 2010.
//
// It is extended for submapping:
// Each node has been matched against one or more submaps (adding a constraint
// for each match), both poses of nodes and of submaps are to be optimized.
// All constraints are between a submap i and a node j.
class PoseGraph2D : public PoseGraph {
 public:
  PoseGraph2D(
      const proto::PoseGraphOptions& options,
      std::unique_ptr<optimization::OptimizationProblem2D> optimization_problem,
      common::ThreadPool* thread_pool);
  ~PoseGraph2D() override;

  PoseGraph2D(const PoseGraph2D&) = delete;
  PoseGraph2D& operator=(const PoseGraph2D&) = delete;

  bool DetectAndDescribe(const NodeId& node_id);

  bool PushNodeForDetect(const NodeId& node_id);
  bool ComputeFlirtFeaturesForAllNodes() override;
  void SetFlirtFeatureBackfillState(
      FlirtFeatureBackfillState state) override;
  FlirtFeatureBackfillState GetFlirtFeatureBackfillState() const override;
  void LockFlirtFeatureSerialization() override;
  void UnlockFlirtFeatureSerialization() override;

  // Adds a new node with 'constant_data'. Its 'constant_data->local_pose' was
  // determined by scan matching against 'insertion_submaps.front()' and the
  // node data was inserted into the 'insertion_submaps'. If
  // 'insertion_submaps.front().finished()' is 'true', data was inserted into
  // this submap for the last time.
  NodeId AddNode(
      std::shared_ptr<const TrajectoryNode::Data> constant_data,
      int trajectory_id,
      const std::vector<std::shared_ptr<const Submap2D>>& insertion_submaps)
      LOCKS_EXCLUDED(mutex_);

  void AddImuData(int trajectory_id, const sensor::ImuData& imu_data) override
      LOCKS_EXCLUDED(mutex_);
  void AddOdometryData(int trajectory_id,
                       const sensor::OdometryData& odometry_data) override
      LOCKS_EXCLUDED(mutex_);
  void AddFixedFramePoseData(
      int trajectory_id,
      const sensor::FixedFramePoseData& fixed_frame_pose_data) override
      LOCKS_EXCLUDED(mutex_);
  void AddLandmarkData(int trajectory_id,
                       const sensor::LandmarkData& landmark_data) override
      LOCKS_EXCLUDED(mutex_);

  void DeleteTrajectory(int trajectory_id) override;
  void FinishTrajectory(int trajectory_id) override;
  bool IsTrajectoryFinished(int trajectory_id) const override
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void FreezeTrajectory(int trajectory_id) override;
  bool IsTrajectoryFrozen(int trajectory_id) const override
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void AddSubmapFromProto(const transform::Rigid3d& global_submap_pose,
                          const proto::Submap& submap) override;
  void AddNodeFromProto(const transform::Rigid3d& global_pose,
                        const proto::Node& node) override;
  void SetTrajectoryDataFromProto(const proto::TrajectoryData& data) override;
  void AddNodeToSubmap(const NodeId& node_id,
                       const SubmapId& submap_id) override;
  void AddSerializedConstraints(
      const std::vector<Constraint>& constraints) override;
  void AddTrimmer(std::unique_ptr<PoseGraphTrimmer> trimmer) override;
  void RunOptimizationOnce() override;
  void RunFinalOptimization() override;
  void ConfigureMapScanDistanceFieldCache(
      const std::string& cache_filename,
      const std::string& cache_key) override LOCKS_EXCLUDED(mutex_);
  bool BuildAndSaveMapScanDistanceFieldCache(
      const std::string& cache_filename,
      const std::string& cache_key) override LOCKS_EXCLUDED(mutex_);
  std::vector<std::vector<int>> GetConnectedTrajectories() const override
      LOCKS_EXCLUDED(mutex_);
  PoseGraphInterface::SubmapData GetSubmapData(const SubmapId& submap_id) const
      LOCKS_EXCLUDED(mutex_) override;
  MapById<SubmapId, PoseGraphInterface::SubmapData> GetAllSubmapData() const
      LOCKS_EXCLUDED(mutex_) override;
  MapById<SubmapId, SubmapPose> GetAllSubmapPoses() const
      LOCKS_EXCLUDED(mutex_) override;
  transform::Rigid3d GetLocalToGlobalTransform(int trajectory_id) const
      LOCKS_EXCLUDED(mutex_) override;
  MapById<NodeId, TrajectoryNode> GetTrajectoryNodes() const override
      LOCKS_EXCLUDED(mutex_);
  MapById<NodeId, TrajectoryNodePose> GetTrajectoryNodePoses() const override
      LOCKS_EXCLUDED(mutex_);
  std::map<int, TrajectoryState> GetTrajectoryStates() const override
      LOCKS_EXCLUDED(mutex_);
  std::map<std::string, transform::Rigid3d> GetLandmarkPoses() const override
      LOCKS_EXCLUDED(mutex_);
  void SetLandmarkPose(const std::string& landmark_id,
                       const transform::Rigid3d& global_pose,
                       const bool frozen = false) override
      LOCKS_EXCLUDED(mutex_);
  sensor::MapByTime<sensor::ImuData> GetImuData() const override
      LOCKS_EXCLUDED(mutex_);
  sensor::MapByTime<sensor::OdometryData> GetOdometryData() const override
      LOCKS_EXCLUDED(mutex_);
  sensor::MapByTime<sensor::FixedFramePoseData> GetFixedFramePoseData()
      const override LOCKS_EXCLUDED(mutex_);
  std::map<std::string /* landmark ID */, PoseGraph::LandmarkNode>
  GetLandmarkNodes() const override LOCKS_EXCLUDED(mutex_);
  std::map<int, TrajectoryData> GetTrajectoryData() const override
      LOCKS_EXCLUDED(mutex_);
  std::vector<Constraint> constraints() const override LOCKS_EXCLUDED(mutex_);
  void SetInitialTrajectoryPose(int from_trajectory_id, int to_trajectory_id,
                                const transform::Rigid3d& pose,
                                const common::Time time) override
      LOCKS_EXCLUDED(mutex_);
  void SetGlobalSlamOptimizationCallback(
      PoseGraphInterface::GlobalSlamOptimizationCallback callback) override;
  transform::Rigid3d GetInterpolatedGlobalTrajectoryPose(
      int trajectory_id, const common::Time time) const
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  static void RegisterMetrics(metrics::FamilyFactory* family_factory);

 private:
  MapById<SubmapId, PoseGraphInterface::SubmapData> GetSubmapDataUnderLock()
      const EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Handles a new work item.
  enum class WorkItemPriority {
    kCritical,
    kHighRateSensorData,
    kLowRateSensorData,
  };

  void AddWorkItem(const std::function<WorkItem::Result()>& work_item)
      LOCKS_EXCLUDED(mutex_) LOCKS_EXCLUDED(work_queue_mutex_);
  void AddWorkItem(const std::function<WorkItem::Result()>& work_item,
                   WorkItemPriority priority)
      LOCKS_EXCLUDED(mutex_) LOCKS_EXCLUDED(work_queue_mutex_);
  size_t GetWorkQueueSize() LOCKS_EXCLUDED(work_queue_mutex_);

  // Adds connectivity and sampler for a trajectory if it does not exist.
  void AddTrajectoryIfNeeded(int trajectory_id)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Appends the new node and submap (if needed) to the internal data
  // structures.
  NodeId AppendNode(
      std::shared_ptr<const TrajectoryNode::Data> constant_data,
      int trajectory_id,
      const std::vector<std::shared_ptr<const Submap2D>>& insertion_submaps,
      const transform::Rigid3d& optimized_pose) LOCKS_EXCLUDED(mutex_);

  // Grows the optimization problem to have an entry for every element of
  // 'insertion_submaps'. Returns the IDs for the 'insertion_submaps'.
  std::vector<SubmapId> InitializeGlobalSubmapPoses(
      int trajectory_id, const common::Time time,
      const std::vector<std::shared_ptr<const Submap2D>>& insertion_submaps)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Adds constraints for a node, and starts scan matching in the background.
  WorkItem::Result ComputeConstraintsForNode(
      const NodeId& node_id,
      std::vector<std::shared_ptr<const Submap2D>> insertion_submaps,
      bool newly_finished_submap) LOCKS_EXCLUDED(mutex_);

  // Computes constraints for a node and submap pair.
  void ComputeConstraint(const NodeId& node_id, const SubmapId& submap_id)
      LOCKS_EXCLUDED(mutex_);

  int ExecuteGlobalRelocationForNode(const NodeId& ref_node_id);

  bool DetectAndDescribeData(const TrajectoryNode::Data* constant_data);
  std::shared_ptr<const flirt::FeatureSet> BuildFlirtFeatures(
      const TrajectoryNode::Data* constant_data);
  std::shared_ptr<const flirt::FeatureSet> BuildFlirtFeaturesUnderLock(
      const TrajectoryNode::Data* constant_data);

  void ComputeConstraintForGlobal(const NodeId& node_id,
                                  const SubmapId& submap_id);

  // Deletes trajectories waiting for deletion. Must not be called during
  // constraint search.
  void DeleteTrajectoriesIfNeeded() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Runs the optimization, executes the trimmers and processes the work queue.
  void HandleWorkQueue(const constraints::ConstraintBuilder2D::Result& result)
      LOCKS_EXCLUDED(mutex_) LOCKS_EXCLUDED(work_queue_mutex_);

  // Process pending tasks in the work queue on the calling thread, until the
  // queue is either empty or an optimization is required.
  void DrainWorkQueue() LOCKS_EXCLUDED(mutex_)
      LOCKS_EXCLUDED(work_queue_mutex_);

  // Waits until we caught up (i.e. nothing is waiting to be scheduled), and
  // all computations have finished.
  void WaitForAllComputations() LOCKS_EXCLUDED(mutex_)
      LOCKS_EXCLUDED(work_queue_mutex_);

  // Runs the optimization. Callers have to make sure, that there is only one
  // optimization being run at a time.
  void RunOptimization() LOCKS_EXCLUDED(mutex_);

  bool CanAddWorkItemModifying(int trajectory_id)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Computes the local to global map frame transform based on the given
  // 'global_submap_poses'.
  transform::Rigid3d ComputeLocalToGlobalTransform(
      const MapById<SubmapId, optimization::SubmapSpec2D>& global_submap_poses,
      int trajectory_id) const EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  SubmapData GetSubmapDataUnderLock(const SubmapId& submap_id) const
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  common::Time GetLatestNodeTime(const NodeId& node_id,
                                 const SubmapId& submap_id) const
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Updates the trajectory connectivity structure with a new constraint.
  void UpdateTrajectoryConnectivity(const Constraint& constraint)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void MaybeDeclareLocalizationLost(const NodeId& node_id,
                                    std::size_t queued_work_items)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void UpdateLocalizationRecoveryFromAcceptedConstraint(
      const Constraint& constraint)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  bool IsTrajectoryActive(int trajectory_id) const
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  bool HasFrozenTrajectoryForLocalization() const
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  bool HasRecentActiveFrozenConnection(int active_trajectory_id,
                                       int active_node_index) const
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  bool IsActiveNodeToFrozenSubmapConstraint(const Constraint& constraint) const
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  bool IsActiveFrozenConstraint(const Constraint& constraint) const
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  struct ActiveFrozenImpliedCorrection {
    int active_trajectory_id = -1;
    transform::Rigid2d delta = transform::Rigid2d::Identity();
    double translation_m = 0.0;
    double yaw_rad = 0.0;
  };

  struct ActiveFrozenCorrectionObservation {
    NodeId node_id;
    ActiveFrozenImpliedCorrection correction;
  };

  bool ComputeActiveFrozenImpliedCorrection(
      const Constraint& constraint,
      ActiveFrozenImpliedCorrection* correction) const
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  bool PassesActiveFrozenConsistencyGate(
      const NodeId& node_id, const ActiveFrozenImpliedCorrection& correction,
      const std::string& gate_reason, bool count_as_ambiguous_reject)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  std::vector<Constraint> FilterConstraintsByQuality(
      const constraints::ConstraintBuilder2D::Result& result)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  struct CurrentPoseScanMapQuality {
    double hit20 = -1.0;
    double mean_distance = -1.0;
    double known_ratio = -1.0;
    int sampled_points = 0;
    int checked_submaps = 0;
  };

  using MapScanDistanceField = map_scan_distance_field::Field;

  struct MapScanDistanceFieldSubmapSnapshot {
    std::shared_ptr<const Submap2D> submap;
    transform::Rigid2d global_pose = transform::Rigid2d::Identity();
  };

  struct LocalizationRecoveryRuntime {
    int consecutive_scan_map_bad_count = 0;
    int consecutive_scan_map_severe_bad_count = 0;
    double latest_scan_map_hit20 = -1.0;
    double latest_scan_map_mean_distance = -1.0;
    bool scan_map_bad = false;
    bool scan_map_severe_bad = false;
    int ambiguous_reject_count_since_accept = 0;
    int geometry_reject_count_since_accept = 0;
    int consistency_reject_count_since_accept = 0;
    int large_correction_consistency_reject_count_since_accept = 0;
    int recovery_full_search_attempts_since_accept = 0;
    int last_recovery_full_search_node_index = -1;
    std::string recovery_state = "OK";
    std::string recovery_reason;
  };

  std::shared_ptr<const MapScanDistanceField>
  GetMapScanDistanceFieldIfReadyOrStartAsync()
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  std::vector<MapScanDistanceFieldSubmapSnapshot>
  SnapshotMapScanDistanceFieldSubmaps(bool include_unfrozen_finished_submaps)
      const EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  std::shared_ptr<const MapScanDistanceField> BuildMapScanDistanceField(
      const std::vector<MapScanDistanceFieldSubmapSnapshot>& submaps) const;
  std::shared_ptr<const MapScanDistanceField> LoadMapScanDistanceFieldCache(
      const std::string& cache_filename, const std::string& cache_key,
      std::string* source) const;
  bool SaveMapScanDistanceFieldCache(
      const std::string& cache_filename, const std::string& cache_key,
      const MapScanDistanceField& field) const;
  void MaybeCollectFinishedMapScanDistanceFieldTask()
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void InvalidateMapScanDistanceField(const std::string& reason)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  CurrentPoseScanMapQuality ComputeMapScanQuality(
      const TrajectoryNode::Data& constant_data,
      const transform::Rigid2d& global_pose,
      const MapScanDistanceField& distance_field,
      int max_sampled_points) const;
  void UpdateLocalizationRecoveryFromMapScanQuality(
      const NodeId& node_id, const CurrentPoseScanMapQuality& quality)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  int NodesSinceLastActiveFrozenConstraint(const NodeId& node_id) const
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  bool HasLocalizationRecoveryEvidence(
      const LocalizationRecoveryRuntime& recovery) const
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  bool IsLocalizationDegraded(
      const NodeId& node_id, const LocalizationRecoveryRuntime& recovery) const
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  bool ShouldRunRecoveryFullSearch(
      const NodeId& node_id, std::size_t queued_work_items_at_start,
      LocalizationRecoveryRuntime* recovery, std::string* reason)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  std::vector<SubmapId> SelectActiveFrozenSubmapsForSearch(
      const NodeId& node_id,
      const std::vector<std::pair<double, SubmapId>>& candidates,
      bool local_search_window, bool recovery_full_search,
      std::size_t queued_work_items_at_start)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  std::atomic<bool> working{false};
  void process_queue_for_detect();
  std::thread th_process_flirt;

  std::mutex detecting_lock_;
  std::mutex flirt_feature_serialization_lock_;
  std::mutex queue_for_detect_lock_;
  std::deque<NodeId> queue_for_detect_;
  std::atomic<FlirtFeatureBackfillState> flirt_feature_backfill_state_{
      FlirtFeatureBackfillState::kReady};
  const proto::PoseGraphOptions options_;
  GlobalSlamOptimizationCallback global_slam_optimization_callback_;
  mutable absl::Mutex mutex_;
  absl::Mutex work_queue_mutex_;

  // If it exists, further work items must be added to this queue, and will be
  // considered later.
  std::unique_ptr<WorkQueue> work_queue_ GUARDED_BY(work_queue_mutex_);
  size_t high_rate_sensor_data_backpressure_counter_
      GUARDED_BY(work_queue_mutex_) = 0;

  // We globally localize a fraction of the nodes from each trajectory.
  absl::flat_hash_map<int, std::unique_ptr<common::FixedRatioSampler>>
      global_localization_samplers_ GUARDED_BY(mutex_);

  // Number of nodes added since last loop closure.
  int num_nodes_since_last_loop_closure_ GUARDED_BY(mutex_) = 0;

  int last_active_to_frozen_constraint_trajectory_id_ GUARDED_BY(mutex_) = -1;
  int last_active_to_frozen_constraint_node_index_ GUARDED_BY(mutex_) = -1;
  std::map<int, std::size_t> active_frozen_global_search_cursor_
      GUARDED_BY(mutex_);
  std::map<int, int> last_active_frozen_global_search_node_index_
      GUARDED_BY(mutex_);
  std::map<int, std::deque<ActiveFrozenCorrectionObservation>>
      active_frozen_consistency_windows_ GUARDED_BY(mutex_);
  std::map<int, LocalizationRecoveryRuntime> localization_recovery_
      GUARDED_BY(mutex_);
  std::shared_ptr<const MapScanDistanceField> map_scan_distance_field_
      GUARDED_BY(mutex_);
  std::string map_scan_distance_field_cache_filename_ GUARDED_BY(mutex_);
  std::string map_scan_distance_field_cache_key_ GUARDED_BY(mutex_);
  int map_scan_distance_field_generation_ GUARDED_BY(mutex_) = 0;
  std::uint64_t map_scan_distance_field_build_token_ GUARDED_BY(mutex_) = 0;
  int map_scan_distance_field_failed_generation_ GUARDED_BY(mutex_) = -1;
  bool map_scan_distance_field_build_in_progress_ GUARDED_BY(mutex_) = false;
  bool map_scan_distance_field_configuration_in_progress_
      GUARDED_BY(mutex_) = false;
  std::future<void> map_scan_distance_field_future_ GUARDED_BY(mutex_);

  // Current optimization problem.
  std::unique_ptr<optimization::OptimizationProblem2D> optimization_problem_;
  constraints::ConstraintBuilder2D constraint_builder_;

  // Thread pool used for handling the work queue.
  common::ThreadPool* const thread_pool_;

  // List of all trimmers to consult when optimizations finish.
  std::vector<std::unique_ptr<PoseGraphTrimmer>> trimmers_ GUARDED_BY(mutex_);

  PoseGraphData data_ GUARDED_BY(mutex_);

  ValueConversionTables conversion_tables_;

  // Allows querying and manipulating the pose graph by the 'trimmers_'. The
  // 'mutex_' of the pose graph is held while this class is used.
  class TrimmingHandle : public Trimmable {
   public:
    TrimmingHandle(PoseGraph2D* parent);
    ~TrimmingHandle() override {}

    int num_submaps(int trajectory_id) const override;
    std::vector<SubmapId> GetSubmapIds(int trajectory_id) const override;
    MapById<SubmapId, SubmapData> GetOptimizedSubmapData() const override
        EXCLUSIVE_LOCKS_REQUIRED(parent_->mutex_);
    const MapById<NodeId, TrajectoryNode>& GetTrajectoryNodes() const override
        EXCLUSIVE_LOCKS_REQUIRED(parent_->mutex_);
    const std::vector<Constraint>& GetConstraints() const override
        EXCLUSIVE_LOCKS_REQUIRED(parent_->mutex_);
    void TrimSubmap(const SubmapId& submap_id)
        EXCLUSIVE_LOCKS_REQUIRED(parent_->mutex_) override;
    bool IsFinished(int trajectory_id) const override
        EXCLUSIVE_LOCKS_REQUIRED(parent_->mutex_);
    void SetTrajectoryState(int trajectory_id, TrajectoryState state) override
        EXCLUSIVE_LOCKS_REQUIRED(parent_->mutex_);

   private:
    PoseGraph2D* const parent_;
  };
};

}  // namespace mapping
}  // namespace cartographer

#endif  // CARTOGRAPHER_MAPPING_INTERNAL_2D_POSE_GRAPH_2D_H_

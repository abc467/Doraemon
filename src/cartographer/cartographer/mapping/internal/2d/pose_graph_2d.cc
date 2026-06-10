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

#include "cartographer/mapping/internal/2d/pose_graph_2d.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <utility>

#include "Eigen/Eigenvalues"
#include "absl/memory/memory.h"
#include "cartographer/common/math.h"
#include "cartographer/mapping/internal/2d/overlapping_submaps_trimmer_2d.h"
#include "cartographer/mapping/localization_health.h"
#include "cartographer/mapping/proto/pose_graph/constraint_builder_options.pb.h"
#include "cartographer/sensor/compressed_point_cloud.h"
#include "cartographer/sensor/internal/voxel_filter.h"
#include "cartographer/transform/transform.h"
#include "glog/logging.h"
#include "pose_graph_2d.h"

namespace cartographer
{
    namespace mapping
    {

        static auto *kWorkQueueDelayMetric = metrics::Gauge::Null();
        static auto *kWorkQueueSizeMetric = metrics::Gauge::Null();
        static auto *kConstraintsSameTrajectoryMetric = metrics::Gauge::Null();
        static auto *kConstraintsDifferentTrajectoryMetric = metrics::Gauge::Null();
        static auto *kActiveSubmapsMetric = metrics::Gauge::Null();
        static auto *kFrozenSubmapsMetric = metrics::Gauge::Null();
        static auto *kDeletedSubmapsMetric = metrics::Gauge::Null();

        constexpr int kAutoRelocationCooldownNodeGap = 60;
        constexpr int kAutoRelocationMaxFailuresPerEpisode = 3;
        constexpr int kAutoRelocationSuppressionNodeGap = 300;
        constexpr double kAutoRelocationMinScore = 0.68;
        constexpr int kAutoRelocationConsistentHits = 2;
        constexpr double kAutoRelocationMinFlirtCandidateScore = 0.12;
        constexpr double kAutoRelocationMinFlirtScoreMargin = 0.05;
        constexpr int kStableActiveFrozenConnectionNodeGap = 40;
        constexpr size_t kMaxStableActiveFrozenSubmapsPerNode = 24;
        constexpr size_t kMaxSoftBackloggedActiveFrozenSubmapsPerNode = 4;
        constexpr size_t kMaxHardBackloggedActiveFrozenSubmapsPerNode = 2;
        constexpr size_t kWorkQueueConstraintSoftBacklogThreshold = 5000;
        constexpr size_t kWorkQueueSensorMediumBacklogThreshold = 10000;
        constexpr size_t kWorkQueueDropSensorDataThreshold = 20000;
        constexpr size_t kWorkQueueHardWarningThreshold = 50000;
        constexpr int kPureLocalizationForceOptimizeSubmaps = 6;
        constexpr double kSameSubmapAmbiguousScoreMargin = 0.03;
        constexpr double kCrossSubmapAmbiguousScoreMargin = 0.03;
        constexpr double kSameSubmapAmbiguousTranslationMeters = 0.50;
        constexpr double kSameSubmapAmbiguousRotationRadians =
            0.05235987755982989; // 3 degrees.
        constexpr double kCrossSubmapAmbiguousTranslationMeters = 1.0;
        constexpr double kCrossSubmapAmbiguousRotationRadians =
            0.05235987755982989; // 3 degrees.
        constexpr double kLargeCorrectionTranslationMeters = 1.0;
        constexpr double kLargeCorrectionRotationRadians =
            0.05235987755982989; // 3 degrees.
        constexpr double kSoftActiveFrozenConstraintTranslationMeters = 1.5;
        constexpr double kSoftActiveFrozenConstraintRotationRadians =
            0.08726646259971647; // 5 degrees.
        constexpr double kHardActiveFrozenConstraintTranslationMeters = 3.0;
        constexpr double kHardActiveFrozenConstraintRotationRadians =
            0.08726646259971647; // 5 degrees.
        constexpr double kRejectOnlyNormalGeometryMinHit20 = 0.45;
        constexpr double kRejectOnlyNormalGeometryMaxMeanDistance = 0.45;
        constexpr double kRejectOnlyLargeGeometryMinHit20 = 0.55;
        constexpr double kRejectOnlyLargeGeometryMaxMeanDistance = 0.35;
        constexpr double kRejectOnlyNormalGeometryMaxFreeSpaceConflictRatio =
            0.30;
        constexpr double kRejectOnlyLargeGeometryMaxFreeSpaceConflictRatio =
            0.25;
        constexpr double kConsistencyTranslationToleranceMeters = 0.60;
        constexpr double kConsistencyRotationToleranceRadians =
            0.05235987755982989; // 3 degrees.
        constexpr int kConsistencyWindowSize = 3;
        constexpr int kConsistencyRequiredHits = 2;
        constexpr int kMapScanHealthOkNodeGap = 30;
        constexpr int kMapScanHealthRecoveryNodeGap = 10;
        constexpr int kMapScanHealthOkMaxSampledPoints = 128;
        constexpr int kMapScanHealthRecoveryMaxSampledPoints = 256;
        constexpr int kCurrentPoseScanMapMaxSampledPoints = 256;
        constexpr size_t kCurrentPoseScanMapCheckedSubmaps = 8;
        constexpr double kCurrentPoseScanMapHitRadiusMeters = 0.20;
        constexpr double kCurrentPoseScanMapSearchRadiusMeters = 1.00;
        constexpr double kCurrentPoseScanMapOccupiedProbabilityThreshold = 0.55;
        constexpr double kMapScanBadHit20 = 0.55;
        constexpr double kMapScanBadMeanDistance = 0.35;
        constexpr int kMapScanBadRequiredSamples = 6;
        constexpr double kMapScanSevereBadHit20 = 0.35;
        constexpr double kMapScanSevereBadMeanDistance = 0.60;
        constexpr int kMapScanSevereBadRequiredSamples = 4;
        constexpr int kRecoveryNoConstraintNodeGap = 200;
        constexpr int kRecoveryConfirmedLostNodeGap = 200;
        constexpr int kRecoveryFullSearchNodeGap = 10;
        constexpr int kRecoveryFullSearchMaxAttempts = 3;
        constexpr size_t kRecoveryFullSearchMaxSubmaps = 96;
        constexpr size_t kRecoveryFullSearchNearestSubmaps = 48;
        constexpr size_t kRecoveryWorkQueueTriggerThreshold = 1000;
        constexpr double kCandidateFullMapMinHit20 = 0.60;
        constexpr double kCandidateFullMapMaxMeanDistance = 0.30;
        constexpr double kCandidateFullMapMinKnownRatio = 0.75;
        constexpr double kCandidateFullMapMinHit20Improvement = 0.10;
        constexpr double kCandidateFullMapMinDistanceImprovement = 0.08;
        constexpr char kMapScanDistanceFieldCacheMagic[] =
            "DORAMON_MAP_SCAN_DF_V1";
        constexpr int kMapScanDistanceFieldCacheVersion = 1;
        constexpr int kMapScanDistanceFieldCacheDistanceScale = 1000;

        struct ScopedInterestPoints
        {
            std::vector<InterestPoint *> points;

            ~ScopedInterestPoints()
            {
                for (InterestPoint *point : points)
                {
                    delete point;
                }
            }

            ScopedInterestPoints(const ScopedInterestPoints &) = delete;
            ScopedInterestPoints &operator=(const ScopedInterestPoints &) = delete;
            ScopedInterestPoints() = default;
        };

        size_t HighRateSensorKeepEveryN(const size_t queue_size)
        {
            if (queue_size >= kWorkQueueHardWarningThreshold)
            {
                return 20;
            }
            if (queue_size >= kWorkQueueDropSensorDataThreshold)
            {
                return 10;
            }
            if (queue_size >= kWorkQueueSensorMediumBacklogThreshold)
            {
                return 5;
            }
            if (queue_size >= kWorkQueueConstraintSoftBacklogThreshold)
            {
                return 2;
            }
            return 1;
        }
        /**
         * @brief 构造函数
         *
         * @param[in] options 位姿图的参数配置
         * @param[in] optimization_problem 优化问题
         * @param[in] thread_pool map_builder中构造的线程池
         */
        PoseGraph2D::PoseGraph2D(
            const proto::PoseGraphOptions &options,
            std::unique_ptr<optimization::OptimizationProblem2D> optimization_problem,
            common::ThreadPool *thread_pool)
            : options_(options),
              optimization_problem_(std::move(optimization_problem)),
              constraint_builder_(options_.constraint_builder_options(), thread_pool),
              thread_pool_(thread_pool)
        {
            if (options.has_overlapping_submaps_trimmer_2d())
            {
                const auto &trimmer_options = options.overlapping_submaps_trimmer_2d();
                AddTrimmer(absl::make_unique<OverlappingSubmapsTrimmer2D>(
                    trimmer_options.fresh_submaps_count(),
                    trimmer_options.min_covered_area(),
                    trimmer_options.min_added_submaps_count()));
            }
            if (flirt::use_flirt)
            {
                this->working = true;
                this->th_process_flirt =
                    std::thread(&PoseGraph2D::process_queue_for_detect, this);
                this->automatic_relocation_worker_running_ = true;
                this->automatic_relocation_thread_ =
                    std::thread(&PoseGraph2D::ProcessAutomaticGlobalRelocationQueue,
                                this);
            }
        }

        PoseGraph2D::~PoseGraph2D()
        {
            StopAutomaticGlobalRelocationWorker();
            WaitForAllComputations();
            std::future<void> map_scan_distance_field_future;
            {
                absl::MutexLock locker(&mutex_);
                map_scan_distance_field_future =
                    std::move(map_scan_distance_field_future_);
            }
            if (map_scan_distance_field_future.valid())
            {
                map_scan_distance_field_future.wait();
            }
            this->working = false;
            if (this->th_process_flirt.joinable())
            {
                this->th_process_flirt.join();
            }

            absl::MutexLock locker(&work_queue_mutex_);
            CHECK(work_queue_ == nullptr);
        }
        // 返回指定轨迹id下的正处于活跃状态下的子图的SubmapId
        std::vector<SubmapId> PoseGraph2D::InitializeGlobalSubmapPoses(
            const int trajectory_id, const common::Time time,
            const std::vector<std::shared_ptr<const Submap2D>> &insertion_submaps)
        {
            CHECK(!insertion_submaps.empty());
            // submap_data中存的: key 为 SubmapId, values 为对应id的Submap在global坐标系下的全局位姿
            const auto &submap_data = optimization_problem_->submap_data();
            // 只有slam刚启动时子图的个数才为1
            if (insertion_submaps.size() == 1)
            {
                // If we don't already have an entry for the first submap, add one.
                if (submap_data.SizeOfTrajectoryOrZero(trajectory_id) == 0)
                {
                    // 如果没设置初始位姿就是0, 设置了就是1
                    if (data_.initial_trajectory_poses.count(trajectory_id) > 0)
                    {
                        // 把该trajectory_id与其初始位姿的基准轨迹的id关联起来
                        data_.trajectory_connectivity_state.Connect(
                            trajectory_id,
                            data_.initial_trajectory_poses.at(trajectory_id).to_trajectory_id,
                            time);
                    }
                    // 将该submap的global pose加入到optimization_problem_中
                    optimization_problem_->AddSubmap(
                        trajectory_id, transform::Project2D(
                                           ComputeLocalToGlobalTransform(
                                               data_.global_submap_poses_2d, trajectory_id) *
                                           insertion_submaps[0]->local_pose()));
                }
                CHECK_EQ(1, submap_data.SizeOfTrajectoryOrZero(trajectory_id));
                // 因为是第一个submap, 所以该submap的ID是(trajectory_id,0), 其中0是submap的index, 从0开始
                const SubmapId submap_id{trajectory_id, 0};
                CHECK(data_.submap_data.at(submap_id).submap == insertion_submaps.front());
                return {submap_id};
            }
            CHECK_EQ(2, insertion_submaps.size());
            // 获取 submap_data 的末尾 trajectory_id
            const auto end_it = submap_data.EndOfTrajectory(trajectory_id);
            CHECK(submap_data.BeginOfTrajectory(trajectory_id) != end_it);
            // end_it是最后一个元素的下一个位置, 所以它之前的一个submap的id就是submap_data中的最后一个元素
            // 注意, 这里的last_submap_id 是 optimization_problem_->submap_data() 中的
            const SubmapId last_submap_id = std::prev(end_it)->id;
            // 如果是等于第一个子图, 说明insertion_submaps的第二个子图还没有加入到optimization_problem_中
            // 拿着optimization_problem_中子图的索引, 根据这个索引在data_.submap_data中获取地图的指针
            if (data_.submap_data.at(last_submap_id).submap ==
                insertion_submaps.front())
            {
                // In this case, 'last_submap_id' is the ID of
                // 'insertions_submaps.front()' and 'insertions_submaps.back()' is new.
                const auto &first_submap_pose = submap_data.at(last_submap_id).global_pose;
                // 这种情况下, 要给新的submap分配id, 并把它加到OptimizationProblem的submap_data_这个容器中
                optimization_problem_->AddSubmap(
                    trajectory_id,
                    first_submap_pose *
                        // first_submap_pose * constraints::ComputeSubmapPose(*insertion_submaps[0]).inverse() = globla指向local的坐标变换
                        // globla指向local的坐标变换 * 第二个子图原点在local下的坐标 = 第二个子图原点在global下的坐标
                        constraints::ComputeSubmapPose(*insertion_submaps[0]).inverse() *
                        constraints::ComputeSubmapPose(*insertion_submaps[1]));
                return {last_submap_id,
                        SubmapId{trajectory_id, last_submap_id.submap_index + 1}};
            }
            CHECK(data_.submap_data.at(last_submap_id).submap ==
                  insertion_submaps.back());
            const SubmapId front_submap_id{trajectory_id,
                                           last_submap_id.submap_index - 1};
            CHECK(data_.submap_data.at(front_submap_id).submap ==
                  insertion_submaps.front());
            return {front_submap_id, last_submap_id};
        }
        /**
         * @brief 向节点列表中添加一个新的节点, 并保存新生成的submap
         *
         * @param[in] constant_data 节点数据的指针
         * @param[in] trajectory_id 轨迹id
         * @param[in] insertion_submaps 子地图指针的vector
         * @param[in] optimized_pose 当前节点在global坐标系下的坐标
         * @return NodeId 返回新生成的节点id
         */
        std::ofstream node_out;
        // 将兴趣点数据存入文件中
        void append_interest_points(const std::string &id,
                                    const std::vector<InterestPoint *> &kpts)
        {
            if (!node_out.is_open())
            {
                node_out.open("/home/lb/cartographer_web/nodes_dense.csv",
                              std::ios::out | std::ios::trunc);
                if (!node_out.is_open())
                {
                    return;
                }
            }

            std::stringstream ss;
            ss << id << ",";
            for (auto i = kpts.cbegin(); i != kpts.cend(); i++)
            {
                auto pos = (*i)->getPosition();
                ss << pos.x << "," << pos.y << "," << pos.theta << ",";
            }

            node_out << ss.str() << std::endl;
            node_out.flush();
        }
        // 兴趣点集对应的pose
        void append_node_data_xya(const std::string &id, double x, double y,
                                  double theta)
        {
            if (!node_out.is_open())
            {
                node_out.open("/home/lb/cartographer_web/nodes_dense.csv",
                              std::ios::out | std::ios::trunc);
                if (!node_out.is_open())
                {
                    return;
                }

                std::stringstream ss;
                ss << id << ",";
                ss << std::to_string(x) << ",";
                ss << std::to_string(y) << ",";
                ss << std::to_string(theta) << ",";

                node_out << ss.str() << std::endl;
                node_out.flush();
            }
        }

        void append_node_data_pose(const std::string &id,
                                   const transform::Rigid3d &pose,
                                   const sensor::PointCloud &point_cloud)
        {
            if (!node_out.is_open())
            {
                node_out.open("/home/lb/cartographer_web/nodes_dense.csv",
                              std::ios::out | std::ios::trunc);
                if (!node_out.is_open())
                {
                    return;
                }
            }
            double local_x = pose.translation().x();
            double local_y = pose.translation().y();
            double local_z = pose.translation().z();
            double local_qw = pose.rotation().w();
            double local_qx = pose.rotation().x();
            double local_qy = pose.rotation().y();
            double local_qz = pose.rotation().z();

            std::stringstream ss;
            ss << id << ",";
            ss << std::to_string(local_x) << ",";
            ss << std::to_string(local_y) << ",";
            ss << std::to_string(local_z) << ",";
            ss << std::to_string(local_qw) << ",";
            ss << std::to_string(local_qx) << ",";
            ss << std::to_string(local_qy) << ",";
            ss << std::to_string(local_qz) << ",";

            for (auto i = point_cloud.begin(); i != point_cloud.end(); i++)
            {
                auto x = (*i).position.x();
                auto y = (*i).position.y();
                auto z = (*i).position.z();
                ss << x << "," << y << "," << z << ",";
            }
            node_out << ss.str() << std::endl;
            node_out.flush();
        }
        // 处理兴趣点检测的进程
        void PoseGraph2D::process_queue_for_detect()
        {
            while (working)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));

                // Get NodeID
                NodeId node_id(-1, -1);
                queue_for_detect_lock_.lock();
                if (queue_for_detect_.empty())
                {
                    // WAIT
                    queue_for_detect_lock_.unlock();
                    continue;
                }
                else
                {
                    node_id = queue_for_detect_.front();
                    queue_for_detect_.pop_front();
                    queue_for_detect_lock_.unlock();
                }

                DetectAndDescribe(node_id);
            }
        }
        //
        bool PoseGraph2D::PushNodeForDetect(const NodeId &node_id)
        {
            bool flag = false;

            queue_for_detect_lock_.lock();
            if (queue_for_detect_.size() == 0)
            {
                queue_for_detect_.push_back(node_id);
                flag = true;
            }
            queue_for_detect_lock_.unlock();

            // if (queue_for_detect_lock_.try_lock()) {
            // }
            return flag;
        }

        void PoseGraph2D::DetectAndDescribeData(
            TrajectoryNode::Data *constant_data,
            const transform::Rigid3d &global_pose)
        {
            if (constant_data == nullptr)
            {
                return;
            }

            // 不能仅仅依靠size来判读是否完成
            std::lock_guard<std::mutex> detect_lock(detecting_lock_);
            if (constant_data->interest_points.size() > 0)
            {
                return;
            }

            std::vector<double> phi;
            std::vector<double> rho;
            for (auto &&p : constant_data->filtered_gravity_aligned_point_cloud)
            {
                double x = p.position.x();
                double y = p.position.y();
                double _rho = std::sqrt(x * x + y * y);
                double _phi = std::atan2(y, x);
                rho.emplace_back(_rho);
                phi.emplace_back(_phi);
            }

            LaserReading scan(phi, rho);
            double robot_x = global_pose.translation().x();
            double robot_y = global_pose.translation().y();
            double robot_theta = 0;
            scan.setLaserPose({robot_x, robot_y, robot_theta});

            flirt::detect(scan, constant_data->interest_points);
            for (auto &&i : constant_data->interest_points)
            {
                i->setDescriptor(flirt::describe(*i, scan));
            }
            LOG(WARNING) << "Finish detect and describe";
        }

        std::vector<InterestPoint *> PoseGraph2D::BuildFlirtFeatures(
            const TrajectoryNode::Data *constant_data,
            const transform::Rigid3d &feature_pose)
        {
            std::vector<InterestPoint *> interest_points;
            if (constant_data == nullptr)
            {
                return interest_points;
            }

            std::lock_guard<std::mutex> detect_lock(detecting_lock_);
            std::vector<double> phi;
            std::vector<double> rho;
            phi.reserve(constant_data->filtered_gravity_aligned_point_cloud.size());
            rho.reserve(constant_data->filtered_gravity_aligned_point_cloud.size());
            for (const auto &p : constant_data->filtered_gravity_aligned_point_cloud)
            {
                const double x = p.position.x();
                const double y = p.position.y();
                rho.emplace_back(std::sqrt(x * x + y * y));
                phi.emplace_back(std::atan2(y, x));
            }

            LaserReading scan(phi, rho);
            scan.setLaserPose({feature_pose.translation().x(),
                               feature_pose.translation().y(), 0.0});
            flirt::detect(scan, interest_points);
            for (auto &&point : interest_points)
            {
                point->setDescriptor(flirt::describe(*point, scan));
            }
            return interest_points;
        }

        void PoseGraph2D::DetectAndDescribe(const NodeId &node_id)
        {
            std::shared_ptr<const TrajectoryNode::Data> constant_data;
            transform::Rigid3d global_pose;
            {
                absl::MutexLock locker(&mutex_);
                auto node = this->data_.trajectory_nodes.find(node_id);
                if (node == this->data_.trajectory_nodes.end())
                {
                    return;
                }
                constant_data = node->data.constant_data;
                global_pose = node->data.global_pose;
            }

            DetectAndDescribeData(
                const_cast<TrajectoryNode::Data *>(constant_data.get()),
                global_pose);
        }

        void PoseGraph2D::ComputeFlirtFeaturesForAllNodes()
        {
            if (!flirt::use_flirt.load())
            {
                return;
            }

            std::vector<NodeId> nodes_to_detect;
            {
                absl::MutexLock locker(&mutex_);
                nodes_to_detect.reserve(data_.trajectory_nodes.size());
                for (const auto &node_id_data : data_.trajectory_nodes)
                {
                    if (node_id_data.data.constant_data == nullptr)
                    {
                        continue;
                    }
                    if (!node_id_data.data.constant_data->interest_points.empty())
                    {
                        continue;
                    }
                    nodes_to_detect.push_back(node_id_data.id);
                }
            }

            if (nodes_to_detect.empty())
            {
                LOG(WARNING) << "[FLIRT]Backfill skipped, all trajectory nodes "
                                "already have interest points.";
                return;
            }

            LOG(WARNING) << "[FLIRT]Backfill start, missing_nodes="
                         << nodes_to_detect.size();
            for (const NodeId &node_id : nodes_to_detect)
            {
                DetectAndDescribe(node_id);
            }
            LOG(WARNING) << "[FLIRT]Backfill finished, processed_nodes="
                         << nodes_to_detect.size();
        }

        NodeId PoseGraph2D::AppendNode(
            std::shared_ptr<const TrajectoryNode::Data> constant_data,
            const int trajectory_id,
            const std::vector<std::shared_ptr<const Submap2D>> &insertion_submaps,
            const transform::Rigid3d &optimized_pose)
        {
            absl::MutexLock locker(&mutex_);
            AddTrajectoryIfNeeded(trajectory_id);

            // append_node_data(constant_data);

            if (!CanAddWorkItemModifying(trajectory_id))
            {
                LOG(WARNING) << "AddNode was called for finished or deleted trajectory.";
            }

            const NodeId node_id = data_.trajectory_nodes.Append(
                trajectory_id, TrajectoryNode{constant_data, optimized_pose});
            ++data_.num_trajectory_nodes;

            // Push to Detect and describe
            this->PushNodeForDetect(node_id);

            // Test if the 'insertion_submap.back()' is one we never saw before.
            if (data_.submap_data.SizeOfTrajectoryOrZero(trajectory_id) == 0 ||
                std::prev(data_.submap_data.EndOfTrajectory(trajectory_id))
                        ->data.submap != insertion_submaps.back())
            {
                // We grow 'data_.submap_data' as needed. This code assumes that the first
                // time we see a new submap is as 'insertion_submaps.back()'.
                const SubmapId submap_id =
                    data_.submap_data.Append(trajectory_id, InternalSubmapData());
                data_.submap_data.at(submap_id).submap = insertion_submaps.back();
                LOG(INFO) << "Inserted submap " << submap_id << ".";
                kActiveSubmapsMetric->Increment();
                // compute descriptor for key frame
                // this->DetectAndDescribe(node_id);
            }
            return node_id;
        }

        NodeId PoseGraph2D::AddNode(
            std::shared_ptr<const TrajectoryNode::Data> constant_data,
            const int trajectory_id,
            const std::vector<std::shared_ptr<const Submap2D>> &insertion_submaps)
        {
            const transform::Rigid3d optimized_pose(
                GetLocalToGlobalTransform(trajectory_id) * constant_data->local_pose);

            const NodeId node_id = AppendNode(constant_data, trajectory_id,
                                              insertion_submaps, optimized_pose);

            bool run_automatic_global_relocation = false;
            int automatic_relocation_last_cross_trajectory_id = -1;
            int automatic_relocation_last_cross_node_index = -1;
            std::string automatic_relocation_trigger_reason;
            const size_t queued_work_items_for_relocation = GetWorkQueueSize();
            {
                absl::MutexLock locker(&mutex_);
                run_automatic_global_relocation =
                    ShouldRunAutomaticGlobalRelocation(
                        node_id, queued_work_items_for_relocation,
                        &automatic_relocation_trigger_reason);
                if (run_automatic_global_relocation)
                {
                    automatic_relocation_last_cross_trajectory_id =
                        last_active_to_frozen_constraint_trajectory_id_;
                    automatic_relocation_last_cross_node_index =
                        last_active_to_frozen_constraint_node_index_;
                }
            }

            const bool newly_finished_submap =
                insertion_submaps.front()->insertion_finished();

            if (flirt::need_flirt.load())
            {
                bool acquired_manual_relocation_slot = false;
                {
                    std::lock_guard<std::mutex> lock(flirt::flirt_busy_lock);
                    if (!flirt::flirt_working.load())
                    {
                        flirt::flirt_working.store(true);
                        acquired_manual_relocation_slot = true;
                    }
                }
                if (!acquired_manual_relocation_slot)
                {
                    LOG(WARNING)
                        << "[GlobalRelocation]Manual request is pending but "
                           "another relocation is running, node="
                        << node_id;
                    AddWorkItem([=]() LOCKS_EXCLUDED(mutex_)
                                { return ComputeConstraintsForNode(
                                      node_id, insertion_submaps,
                                      newly_finished_submap); });
                    return node_id;
                }
                flirt::cv_flirt_busy.notify_all();
                flirt::flirt_return_code = ExecuteGlobalRelocationForNode(node_id);
                flirt::flirt_working.store(false);

                flirt::need_flirt.store(false);
                flirt::cv_flirt_busy.notify_all();

                AddWorkItem([=]() LOCKS_EXCLUDED(mutex_)
                            { return ComputeConstraintsForNode(node_id, insertion_submaps,
                                                               newly_finished_submap); });
                return node_id;
            }

            // Keep normal localization constraints ahead of automatic relocation.
            AddWorkItem([=]() LOCKS_EXCLUDED(mutex_)
                        { return ComputeConstraintsForNode(node_id, insertion_submaps,
                                                           newly_finished_submap); });

            if (!run_automatic_global_relocation)
            {
                return node_id;
            }

            bool acquired_relocation_slot = false;
            {
                std::lock_guard<std::mutex> lock(flirt::flirt_busy_lock);
                if (!flirt::need_flirt.load() && !flirt::flirt_working.load())
                {
                    flirt::relocation_min_score.store(kAutoRelocationMinScore);
                    flirt::relocation_required_consistent_hits.store(
                        kAutoRelocationConsistentHits);
                    flirt::reset_relocation_consistency();
                    flirt::flirt_return_code = flirt::kRelocationIdle;
                    flirt::flirt_working.store(true);
                    acquired_relocation_slot = true;
                }
            }

            if (!acquired_relocation_slot)
            {
                {
                    absl::MutexLock locker(&mutex_);
                    LocalizationRecoveryRuntime &recovery =
                        localization_recovery_[node_id.trajectory_id];
                    recovery.recovery_state = "DEGRADED";
                    recovery.recovery_reason = "relocation_slot_busy";
                    RecordLocalizationHealthRecoveryState(
                        recovery.recovery_state, recovery.recovery_reason);
                }
                LOG(WARNING) << "[AutoRelocation]Skip node=" << node_id
                             << " because relocation is already running.";
                return node_id;
            }
            {
                absl::MutexLock locker(&mutex_);
                last_automatic_global_relocation_trajectory_id_ =
                    node_id.trajectory_id;
                last_automatic_global_relocation_node_index_ =
                    node_id.node_index;
            }

            const int automatic_relocation_node_gap =
                automatic_relocation_last_cross_node_index < 0
                    ? node_id.node_index + 1
                    : node_id.node_index -
                          automatic_relocation_last_cross_node_index;
            LOG(WARNING)
                << "[AutoRelocation]Background trigger node=" << node_id
                << " last_cross_trajectory="
                << automatic_relocation_last_cross_trajectory_id
                << " last_cross_node="
                << automatic_relocation_last_cross_node_index
                << " node_gap=" << automatic_relocation_node_gap
                << " reason=" << automatic_relocation_trigger_reason
                << " min_score=" << kAutoRelocationMinScore
                << " consistent_hits=" << kAutoRelocationConsistentHits;
            flirt::cv_flirt_busy.notify_all();
            EnqueueAutomaticGlobalRelocation(
                AutomaticGlobalRelocationRequest{
                    node_id, constant_data, optimized_pose,
                    automatic_relocation_last_cross_trajectory_id,
                    automatic_relocation_last_cross_node_index,
                    automatic_relocation_trigger_reason});
            return node_id;
        }

        // 将任务放入到任务队列中等待被执行
        void PoseGraph2D::AddWorkItem(const std::function<WorkItem::Result()> &work_item)
        {
            AddWorkItem(work_item, WorkItemPriority::kCritical);
        }

        void PoseGraph2D::AddWorkItem(
            const std::function<WorkItem::Result()> &work_item,
            const WorkItemPriority priority)
        {
            absl::MutexLock locker(&work_queue_mutex_);
            if (work_queue_ == nullptr)
            {
                work_queue_ = absl::make_unique<WorkQueue>();
                auto task = absl::make_unique<common::Task>();
                task->SetWorkItem([this]()
                                  { DrainWorkQueue(); });
                thread_pool_->Schedule(std::move(task));
            }
            if (priority == WorkItemPriority::kHighRateSensorData)
            {
                const size_t keep_every_n =
                    HighRateSensorKeepEveryN(work_queue_->size());
                if (keep_every_n > 1 &&
                    (++high_rate_sensor_data_backpressure_counter_ %
                     keep_every_n) != 0)
                {
                    kWorkQueueSizeMetric->Set(work_queue_->size());
                    RecordLocalizationHealthBackpressure(work_queue_->size(),
                                                         keep_every_n);
                    LOG_EVERY_N(WARNING, 200)
                        << "[PoseGraphBackpressure]Downsample high-rate "
                        << "sensor work item, queue_size="
                        << work_queue_->size()
                        << " keep_every_n=" << keep_every_n;
                    return;
                }
            }
            if (priority == WorkItemPriority::kLowRateSensorData &&
                work_queue_->size() >= kWorkQueueHardWarningThreshold)
            {
                LOG_EVERY_N(WARNING, 100)
                    << "[PoseGraphBackpressure]Keep low-rate sensor work "
                    << "item despite high queue, "
                    << "queue_size=" << work_queue_->size()
                    << " warning_threshold=" << kWorkQueueHardWarningThreshold;
            }
            const auto now = std::chrono::steady_clock::now();
            work_queue_->push_back({now, work_item});
            kWorkQueueSizeMetric->Set(work_queue_->size());
            RecordLocalizationHealthWorkQueueSize(work_queue_->size());
            if (work_queue_->size() >= kWorkQueueHardWarningThreshold)
            {
                LOG_EVERY_N(WARNING, 500)
                    << "[PoseGraphBackpressure]High work_queue size="
                    << work_queue_->size()
                    << " sensor_hard_backlog_threshold="
                    << kWorkQueueDropSensorDataThreshold
                    << " constraint_soft_backlog_threshold="
                    << kWorkQueueConstraintSoftBacklogThreshold;
            }
            kWorkQueueDelayMetric->Set(
                std::chrono::duration_cast<std::chrono::duration<double>>(
                    now - work_queue_->front().time)
                    .count());
        }

        size_t PoseGraph2D::GetWorkQueueSize()
        {
            absl::MutexLock locker(&work_queue_mutex_);
            return work_queue_ == nullptr ? 0 : work_queue_->size();
        }
        // 如果轨迹不存在, 则将轨迹添加到连接状态里并添加采样器
        void PoseGraph2D::AddTrajectoryIfNeeded(const int trajectory_id)
        {
            data_.trajectories_state[trajectory_id];
            CHECK(data_.trajectories_state.at(trajectory_id).state !=
                  TrajectoryState::FINISHED);
            CHECK(data_.trajectories_state.at(trajectory_id).state !=
                  TrajectoryState::DELETED);
            CHECK(data_.trajectories_state.at(trajectory_id).deletion_state ==
                  InternalTrajectoryState::DeletionState::NORMAL);
            data_.trajectory_connectivity_state.Add(trajectory_id);
            // Make sure we have a sampler for this trajectory.
            if (!global_localization_samplers_[trajectory_id])
            {
                global_localization_samplers_[trajectory_id] =
                    absl::make_unique<common::FixedRatioSampler>(
                        options_.global_sampling_ratio());
            }
        }
        // 将 把里程计数据加入到优化问题中 这个任务放入到任务队列中
        void PoseGraph2D::AddImuData(const int trajectory_id, const sensor::ImuData &imu_data)
        {
            AddWorkItem([=]() LOCKS_EXCLUDED(mutex_)
                        {
                absl::MutexLock locker(&mutex_);
                if (CanAddWorkItemModifying(trajectory_id)) {
                    optimization_problem_->AddImuData(trajectory_id, imu_data);
                }
                return WorkItem::Result::kDoNotRunOptimization; },
                        WorkItemPriority::kHighRateSensorData);
        }

        void PoseGraph2D::AddOdometryData(const int trajectory_id,
                                          const sensor::OdometryData &odometry_data)
        {
            AddWorkItem([=]() LOCKS_EXCLUDED(mutex_)
                        {
            absl::MutexLock locker(&mutex_);
            if (CanAddWorkItemModifying(trajectory_id)) {
            optimization_problem_->AddOdometryData(trajectory_id, odometry_data);
            }
            return WorkItem::Result::kDoNotRunOptimization; },
                        WorkItemPriority::kHighRateSensorData);
        }

        void PoseGraph2D::AddFixedFramePoseData(
            const int trajectory_id,
            const sensor::FixedFramePoseData &fixed_frame_pose_data)
        {
            AddWorkItem([=]() LOCKS_EXCLUDED(mutex_)
                        {
    absl::MutexLock locker(&mutex_);
    if (CanAddWorkItemModifying(trajectory_id)) {
      optimization_problem_->AddFixedFramePoseData(trajectory_id,
                                                   fixed_frame_pose_data);
    }
    return WorkItem::Result::kDoNotRunOptimization; },
                        WorkItemPriority::kLowRateSensorData);
        }

        // 将 把landmark数据加入到data_.landmark_nodes中 这个任务放入到任务队列中
        void PoseGraph2D::AddLandmarkData(int trajectory_id,
                                          const sensor::LandmarkData &landmark_data)
        {
            AddWorkItem([=]() LOCKS_EXCLUDED(mutex_)
                        {
                absl::MutexLock locker(&mutex_);
                if (CanAddWorkItemModifying(trajectory_id)) {
                for (const auto& observation : landmark_data.landmark_observations) {
                    data_.landmark_nodes[observation.id].landmark_observations.emplace_back(
                        PoseGraphInterface::LandmarkNode::LandmarkObservation{
                            trajectory_id, landmark_data.time,
                            observation.landmark_to_tracking_transform,
                            observation.translation_weight, observation.rotation_weight});
                }
            }
            return WorkItem::Result::kDoNotRunOptimization; },
                        WorkItemPriority::kLowRateSensorData);
        }

        void PoseGraph2D::EnqueueAutomaticGlobalRelocation(
            AutomaticGlobalRelocationRequest request)
        {
            {
                std::lock_guard<std::mutex> lock(automatic_relocation_queue_lock_);
                if (!automatic_relocation_worker_running_)
                {
                    LOG(WARNING) << "[AutoRelocation]Background worker is stopped, "
                                    "drop request node="
                                 << request.node_id;
                    std::lock_guard<std::mutex> busy_lock(flirt::flirt_busy_lock);
                    flirt::flirt_return_code =
                        flirt::kRelocationWorkerUnavailable;
                    flirt::flirt_working.store(false);
                    flirt::cv_flirt_busy.notify_all();
                    RecordLocalizationHealthAutoRelocationResult(
                        flirt::kRelocationWorkerUnavailable, false);
                    return;
                }
                automatic_relocation_queue_.push_back(std::move(request));
                RecordLocalizationHealthAutoRelocationTrigger(
                    automatic_relocation_queue_.size());
            }
            automatic_relocation_queue_cv_.notify_one();
        }

        void PoseGraph2D::StopAutomaticGlobalRelocationWorker()
        {
            {
                std::lock_guard<std::mutex> lock(automatic_relocation_queue_lock_);
                automatic_relocation_worker_running_ = false;
            }
            automatic_relocation_queue_cv_.notify_all();
            if (automatic_relocation_thread_.joinable())
            {
                automatic_relocation_thread_.join();
            }
        }

        void PoseGraph2D::ProcessAutomaticGlobalRelocationQueue()
        {
            while (true)
            {
                AutomaticGlobalRelocationRequest request;
                {
                    std::unique_lock<std::mutex> lock(
                        automatic_relocation_queue_lock_);
                    automatic_relocation_queue_cv_.wait(
                        lock, [this]()
                        {
                            return !automatic_relocation_worker_running_ ||
                                   !automatic_relocation_queue_.empty();
                        });
                    if (!automatic_relocation_worker_running_ &&
                        automatic_relocation_queue_.empty())
                    {
                        return;
                    }
                    request = std::move(automatic_relocation_queue_.front());
                    automatic_relocation_queue_.pop_front();
                }

                RecordLocalizationHealthRecoveryState(
                    "AUTO_RELOCATING",
                    request.trigger_reason);
                std::string failure_reason;
                const int return_code =
                    ExecuteAutomaticGlobalRelocationForNode(request,
                                                           &failure_reason);
                const bool relocation_success =
                    return_code == flirt::kRelocationSuccess;
                RecordLocalizationHealthAutoRelocationResult(
                    return_code, relocation_success);
                {
                    std::lock_guard<std::mutex> lock(flirt::flirt_busy_lock);
                    flirt::flirt_return_code = return_code;
                    flirt::flirt_working.store(false);
                }
                {
                    absl::MutexLock locker(&mutex_);
                    LocalizationRecoveryRuntime &recovery =
                        localization_recovery_[request.node_id.trajectory_id];
                    recovery.recovery_state = relocation_success
                                                  ? "COOLDOWN"
                                                  : "DEGRADED";
                    if (relocation_success)
                    {
                        recovery.auto_relocation_failures_since_accept = 0;
                        recovery.auto_relocation_suppressed_until_node_index =
                            -1;
                        recovery.recovery_reason = "auto_relocation_success";
                    }
                    else
                    {
                        ++recovery.auto_relocation_failures_since_accept;
                        if (failure_reason.empty())
                        {
                            failure_reason = "auto_relocation_failed";
                        }
                        if (recovery.auto_relocation_failures_since_accept >=
                            kAutoRelocationMaxFailuresPerEpisode)
                        {
                            recovery.auto_relocation_suppressed_until_node_index =
                                request.node_id.node_index +
                                kAutoRelocationSuppressionNodeGap;
                            recovery.recovery_reason = "relocation_exhausted";
                        }
                        else
                        {
                            recovery.recovery_reason = failure_reason;
                        }
                    }
                    RecordLocalizationHealthRecoveryState(
                        recovery.recovery_state, recovery.recovery_reason);
                }
                LOG(WARNING) << "[AutoRelocation]Background result node="
                             << request.node_id
                             << " return_code=" << return_code
                             << " reason="
                             << (failure_reason.empty() ? "success"
                                                        : failure_reason);
                flirt::cv_flirt_busy.notify_all();

                if (relocation_success)
                {
                    AddWorkItem([=]() LOCKS_EXCLUDED(mutex_)
                                {
                                    LOG(WARNING)
                                        << "[AutoRelocation]Request optimization "
                                           "after background relocation node="
                                        << request.node_id;
                                    return WorkItem::Result::kRunOptimization;
                                });
                }
            }
        }

        bool PoseGraph2D::BuildAutomaticGlobalRelocationSnapshot(
            const NodeId &ref_node_id,
            std::vector<AutomaticRelocationCandidate> *candidates)
        {
            bool has_frozen_trajectory = false;
            for (const auto &trajectory_state : data_.trajectories_state)
            {
                const int trajectory_id = trajectory_state.first;
                if (trajectory_state.second.state == TrajectoryState::FROZEN &&
                    data_.trajectory_nodes.SizeOfTrajectoryOrZero(trajectory_id) > 0 &&
                    data_.submap_data.SizeOfTrajectoryOrZero(trajectory_id) > 0)
                {
                    has_frozen_trajectory = true;
                    break;
                }
            }
            if (!has_frozen_trajectory)
            {
                return false;
            }

            for (const auto &node_id_data : data_.trajectory_nodes)
            {
                const auto node_state_it =
                    data_.trajectories_state.find(node_id_data.id.trajectory_id);
                if (node_state_it == data_.trajectories_state.end() ||
                    node_state_it->second.state != TrajectoryState::FROZEN)
                {
                    continue;
                }

                const auto &candidate_data = node_id_data.data.constant_data;
                if (candidate_data == nullptr ||
                    candidate_data->interest_points.empty())
                {
                    continue;
                }

                auto submap_it =
                    data_.submap_data.BeginOfTrajectory(node_id_data.id.trajectory_id);
                const auto submaps_end =
                    data_.submap_data.EndOfTrajectory(node_id_data.id.trajectory_id);
                for (; submap_it != submaps_end; submap_it.operator++())
                {
                    const auto node_id_in_submap =
                        submap_it->data.node_ids.find(node_id_data.id);
                    if (node_id_in_submap != submap_it->data.node_ids.end())
                    {
                        break;
                    }
                }

                if (submap_it == submaps_end ||
                    submap_it->data.submap == nullptr)
                {
                    LOG(WARNING)
                        << "[AutoRelocation]Snapshot skipped frozen node="
                        << node_id_data.id
                        << " because containing submap was not found.";
                    continue;
                }

                const auto first_node =
                    data_.trajectory_nodes.BeginOfTrajectory(
                        node_id_data.id.trajectory_id);
                candidates->push_back(
                    AutomaticRelocationCandidate{
                        node_id_data.id, submap_it->id, candidate_data,
                        submap_it->data.submap,
                        transform::Project2D(first_node->data.global_pose)});
            }

            LOG(WARNING) << "[AutoRelocation]Snapshot node=" << ref_node_id
                         << " frozen_candidates=" << candidates->size();
            return true;
        }

        int PoseGraph2D::ExecuteAutomaticGlobalRelocationForNode(
            const AutomaticGlobalRelocationRequest &request,
            std::string *const failure_reason)
        {
            auto SetFailureReason = [failure_reason](const std::string &reason)
            {
                if (failure_reason != nullptr)
                {
                    *failure_reason = reason;
                }
            };
            LOG(WARNING) << "[AutoRelocation]Background execute node="
                         << request.node_id << " last_cross_trajectory="
                         << request.last_cross_trajectory_id
                         << " last_cross_node=" << request.last_cross_node_index;

            if (request.constant_data == nullptr)
            {
                SetFailureReason("relocation_no_interest_points");
                flirt::reset_relocation_consistency();
                return flirt::kRelocationNoInterestPoints;
            }

            const TrajectoryNode::Data *constant_data =
                request.constant_data.get();
            ScopedInterestPoints query_interest_points;
            query_interest_points.points =
                BuildFlirtFeatures(constant_data, transform::Rigid3d::Identity());
            if (query_interest_points.points.empty())
            {
                SetFailureReason("relocation_no_interest_points");
                flirt::reset_relocation_consistency();
                return flirt::kRelocationNoInterestPoints;
            }
            LOG(WARNING) << "[AutoRelocation]Query local FLIRT features node="
                         << request.node_id
                         << " interest_points="
                         << query_interest_points.points.size();

            std::vector<AutomaticRelocationCandidate> candidates;
            {
                absl::MutexLock locker(&mutex_);
                const auto ref_state_it =
                    data_.trajectories_state.find(request.node_id.trajectory_id);
                if (ref_state_it == data_.trajectories_state.end() ||
                    ref_state_it->second.state != TrajectoryState::ACTIVE)
                {
                    SetFailureReason("relocation_need_more_trajectories");
                    flirt::reset_relocation_consistency();
                    return flirt::kRelocationNeedMoreTrajectories;
                }
                if (!BuildAutomaticGlobalRelocationSnapshot(
                        request.node_id, &candidates))
                {
                    SetFailureReason("relocation_need_more_trajectories");
                    flirt::reset_relocation_consistency();
                    return flirt::kRelocationNeedMoreTrajectories;
                }
            }

            std::vector<constraints::EstimatedPose> estimated_pose;
            for (const auto &candidate : candidates)
            {
                OrientedPoint2D matched_transform;
                std::vector<std::pair<InterestPoint *, InterestPoint *>> corres;
                flirt::match(candidate.constant_data->interest_points,
                             query_interest_points.points, matched_transform,
                             corres);

                const float score =
                    static_cast<float>(corres.size()) /
                    static_cast<float>(query_interest_points.points.size());
                if (score < kAutoRelocationMinFlirtCandidateScore)
                {
                    continue;
                }

                const transform::Rigid2d node_global_pose =
                    transform::Rigid2d({matched_transform.x, matched_transform.y},
                                       matched_transform.theta);
                const transform::Rigid2d node_local_pose =
                    candidate.initial_trajectory_pose.inverse() * node_global_pose;
                const auto submap_2d =
                    std::dynamic_pointer_cast<const Submap2D>(candidate.submap);
                if (submap_2d == nullptr)
                {
                    continue;
                }
                estimated_pose.push_back(
                    {NodeId(request.node_id), SubmapId(candidate.submap_id), score,
                     node_local_pose, submap_2d.get(), submap_2d});
            }

            std::sort(
                estimated_pose.begin(), estimated_pose.end(),
                [](const constraints::EstimatedPose &lhs,
                   const constraints::EstimatedPose &rhs) -> bool
                {
                    return lhs.score > rhs.score;
                });
            if (estimated_pose.empty())
            {
                SetFailureReason("relocation_no_candidate");
                flirt::reset_relocation_consistency();
                return flirt::kRelocationNoCandidatePose;
            }

            const float best_score = estimated_pose.front().score;
            const float second_score =
                estimated_pose.size() > 1 ? estimated_pose[1].score : 0.f;
            if (estimated_pose.size() > 1 &&
                best_score - second_score <
                    kAutoRelocationMinFlirtScoreMargin)
            {
                LOG(WARNING)
                    << "[AutoRelocation]Rejected ambiguous FLIRT candidates "
                    << "node=" << request.node_id
                    << " best_score=" << best_score
                    << " second_score=" << second_score
                    << " min_margin=" << kAutoRelocationMinFlirtScoreMargin;
                SetFailureReason("relocation_ambiguous");
                flirt::reset_relocation_consistency();
                return flirt::kRelocationLowConstraintScore;
            }

            auto GetTopk = [&estimated_pose](size_t k)
            {
                std::vector<constraints::EstimatedPose> poses;
                for (size_t i = 0; i < estimated_pose.size() &&
                                   poses.size() < k;
                     i++)
                {
                    poses.push_back(estimated_pose.at(i));
                    const float score = estimated_pose.at(i).score;
                    const double x = estimated_pose.at(i).pose.translation().x();
                    const double y = estimated_pose.at(i).pose.translation().y();
                    const double theta =
                        estimated_pose.at(i).pose.rotation().angle();
                    const int trajectory_id =
                        estimated_pose.at(i).submap_id.trajectory_id;
                    const int submap_index =
                        estimated_pose.at(i).submap_id.submap_index;
                    LOG(WARNING) << "[AutoRelocation]FLIRT score=" << score
                                 << " pose=" << x << "|" << y << "|" << theta
                                 << " in Submap(" << trajectory_id << ", "
                                 << submap_index << ")";
                }
                return poses;
            };

            std::string constraint_rejection_reason;
            const bool constraint_added =
                constraint_builder_.ComputeConstraintWithEstimatedPoses(
                    GetTopk(3), constant_data, &constraint_rejection_reason);
            if (!constraint_added)
            {
                SetFailureReason(constraint_rejection_reason.empty()
                                     ? "relocation_low_score"
                                     : constraint_rejection_reason);
                return flirt::kRelocationLowConstraintScore;
            }

            SetFailureReason("");
            return flirt::kRelocationSuccess;
        }

        // 使用flirt执行全局重定位
        int PoseGraph2D::ExecuteGlobalRelocationForNode(const NodeId &ref_node_id)
        {
            // check enable flag
            LOG(WARNING) << "ExecuteGlobalRelocationForNode=" << ref_node_id.trajectory_id
                         << "|" << ref_node_id.node_index;

            const auto ref_state_it =
                data_.trajectories_state.find(ref_node_id.trajectory_id);
            if (ref_state_it == data_.trajectories_state.end() ||
                ref_state_it->second.state != TrajectoryState::ACTIVE)
            {
                flirt::reset_relocation_consistency();
                return flirt::kRelocationNeedMoreTrajectories;
            }

            bool has_frozen_trajectory = false;
            for (const auto &trajectory_state : data_.trajectories_state)
            {
                const int trajectory_id = trajectory_state.first;
                if (trajectory_state.second.state == TrajectoryState::FROZEN &&
                    data_.trajectory_nodes.SizeOfTrajectoryOrZero(trajectory_id) > 0 &&
                    data_.submap_data.SizeOfTrajectoryOrZero(trajectory_id) > 0)
                {
                    has_frozen_trajectory = true;
                    break;
                }
            }
            if (!has_frozen_trajectory)
            {
                flirt::reset_relocation_consistency();
                return flirt::kRelocationNeedMoreTrajectories;
            }

            // define pose structure
            std::vector<constraints::EstimatedPose> estimated_pose;

            const TrajectoryNode::Data *constant_data =
                data_.trajectory_nodes.at(ref_node_id).constant_data.get();

            ScopedInterestPoints query_interest_points;
            query_interest_points.points =
                BuildFlirtFeatures(constant_data, transform::Rigid3d::Identity());

            // Check again, make sure that the current scan has interest points.
            if (query_interest_points.points.empty())
            {
                flirt::reset_relocation_consistency();
                return flirt::kRelocationNoInterestPoints;
            }
            LOG(WARNING) << "[GlobalRelocation]Query local FLIRT features node="
                         << ref_node_id
                         << " interest_points="
                         << query_interest_points.points.size();

            // 这里是用于输出点云信息到csv文件，用来做测试的
            // append_node_data_pose("NewNodeData", constant_data->local_pose,
            //                       constant_data->filtered_gravity_aligned_point_cloud);

            // append_interest_points("^KptsXYA^", constant_data->interest_points);

            auto node_it = data_.trajectory_nodes.begin();
            auto nodes_end = data_.trajectory_nodes.end();
            LOG(WARNING) << "Search range in frozen trajectories.";
            for (; node_it != nodes_end; node_it.operator++())
            {
                const auto node_state_it =
                    data_.trajectories_state.find(node_it->id.trajectory_id);
                if (node_state_it == data_.trajectories_state.end() ||
                    node_state_it->second.state != TrajectoryState::FROZEN)
                {
                    continue;
                }
                // check the nodes in the old trajectories.
                // The InterestPoint must NOT be Empty.
                if (node_it->data.constant_data->interest_points.empty())
                {
                    continue;
                }
                // match with the old nodes
                OrientedPoint2D _transform;
                std::vector<std::pair<InterestPoint *, InterestPoint *>> _corres;
                flirt::match(node_it->data.constant_data->interest_points,
                             query_interest_points.points, _transform, _corres);

                // compute score
                float score = (float)_corres.size() /
                              (float)query_interest_points.points.size();
                if (score > 0.1)
                {
                    // 寻找冻结轨迹中包含这个历史node的submap。
                    auto submap_it = data_.submap_data.BeginOfTrajectory(node_it->id.trajectory_id);
                    auto submaps_end = data_.submap_data.EndOfTrajectory(node_it->id.trajectory_id);

                    for (; submap_it != submaps_end; submap_it.operator++())
                    {
                        auto r = submap_it->data.node_ids.find(node_it->id);
                        if (r != submap_it->data.node_ids.end())
                        {
                            break;
                        }
                    }

                    if (submap_it == submaps_end)
                    {
                        // 原来的版本中这里是return但是这样处理会有问题
                        // 假设拓建了2次地图，则现在一共有3个Trajectories：
                        // ID    状态
                        //  0    冻结
                        //  1    冻结
                        //  2    激活
                        // 如果在第二次拓建的区域内（ID=1）范围内重定位，首先肯定会有一些分数大于0.1的node
                        // 并且是按照顺序去查找的，比如在TrajecotyID=0里面找到了
                        // 但是如果我去找这个
                        // return -4;
                        LOG(WARNING) << "failed to find submap_id in trajecotry" << node_it->id.trajectory_id;
                        continue;
                    }

                    //
                    // 计算出相对当前trajectory而言的位姿
                    //
                    // FLIRT算出的全局位姿（相对[0,0,0]而言的
                    // 第一种方法是使用node的pose
                    // transform::Rigid2d node_global_pose = transform::Project2D(node_it->data.global_pose);
                    // 第二种方法是使用FLIRT算出来的pose
                    transform::Rigid2d node_global_pose = transform::Rigid2d({_transform.x, _transform.y}, _transform.theta);

                    // 我们需要获取当前trajectory的位姿，将FLIRT的全局位姿转换到相对Trajectory而言的位姿
                    // 第一种方法是使用initial_trajectory_poses
                    // 但是！！里面全是空的……

                    // LOG(WARNING) << "Total Size=" << data_.initial_trajectory_poses.size();
                    // for (auto &&i : data_.initial_trajectory_poses)
                    // {
                    //     LOG(WARNING) << "TrajectoryPose, ID=" << i.first << "="
                    //                  << i.second.relative_pose.translation().x() << "|"
                    //                  << i.second.relative_pose.translation().y() << "|"
                    //                  << i.second.relative_pose.translation().z();
                    // }
                    //

                    // 第二种方法，就是取trajectory下的第一个node的global pose作为trajectory相对于世界的pose
                    transform::Rigid3d initial_trajectory_pose = data_.trajectory_nodes.BeginOfTrajectory(node_it->id.trajectory_id)->data.global_pose;
                    transform::Rigid2d initial_trajectory_post2d = transform::Project2D(initial_trajectory_pose);
                    transform::Rigid2d node_local_pose = initial_trajectory_post2d.inverse() * node_global_pose;

                    //
                    // 这里是把符合条件的位姿找到，加到队列中，作为候选位姿。
                    //
                    const auto submap_2d =
                        std::dynamic_pointer_cast<const Submap2D>(
                            submap_it->data.submap);
                    if (submap_2d == nullptr)
                    {
                        continue;
                    }
                    estimated_pose.push_back(
                        {NodeId(ref_node_id), SubmapId(submap_it->id), score,
                         node_local_pose, submap_2d.get(), submap_2d});
                }
            }

            auto compare_func = [](const constraints::EstimatedPose &lhs,
                                   const constraints::EstimatedPose &rhs) -> bool
            {
                return lhs.score > rhs.score;
            };

            std::sort(estimated_pose.begin(), estimated_pose.end(), compare_func);
            if (estimated_pose.empty())
            {
                flirt::reset_relocation_consistency();
                return flirt::kRelocationNoCandidatePose;
            }
            // 选取flirt得分最高的top k个待选psoe
            auto GetTopk = [&estimated_pose](size_t k)
            {
                std::vector<constraints::EstimatedPose> poses;
                for (size_t i = 0; i < estimated_pose.size() && i < k; i++)
                {
                    poses.push_back(estimated_pose.at(i));
                    float score = estimated_pose.at(i).score;
                    double x = estimated_pose.at(i).pose.translation().x();
                    double y = estimated_pose.at(i).pose.translation().y();
                    double theta = estimated_pose.at(i).pose.rotation().angle();
                    int trajectory_id = estimated_pose.at(i).submap_id.trajectory_id;
                    int submap_index = estimated_pose.at(i).submap_id.submap_index;
                    LOG(WARNING) << "[GlobalRelocation]" << "Score=" << score
                                 << "  Pose=" << x << "|" << y << "|" << theta
                                 << " in Submap(" << trajectory_id << ", " << submap_index
                                 << ")";
                }
                return poses;
            };

            const bool constraint_added =
                constraint_builder_.ComputeConstraintWithEstimatedPoses(
                    GetTopk(3), constant_data);
            if (!constraint_added)
            {
                return flirt::kRelocationLowConstraintScore;
            }

            return flirt::kRelocationSuccess;
        }
        /**
         * @brief 进行子图间约束计算, 也可以说成是回环检测
         *
         * @param[in] node_id 节点的id
         * @param[in] submap_id submap的id
         */
        void PoseGraph2D::ComputeConstraint(const NodeId &node_id,
                                            const SubmapId &submap_id)
        {
            bool maybe_add_local_constraint = false;
            bool maybe_add_global_constraint = false;
            bool active_node_to_frozen_submap = false;

            TrajectoryNode::Data *constant_data;
            const Submap2D *submap;
            {
                absl::MutexLock locker(&mutex_);
                CHECK(data_.submap_data.at(submap_id).state == SubmapState::kFinished);
                // 如果是未完成状态的地图不进行约束计算
                if (!data_.submap_data.at(submap_id).submap->insertion_finished())
                {
                    // Uplink server only receives grids when they are finished, so skip
                    // constraint search before that.
                    return;
                }
                // 获取该 node 和该 submap 中的 node 中较新的时间
                const common::Time node_time = GetLatestNodeTime(node_id, submap_id);
                const common::Time last_connection_time =
                    data_.trajectory_connectivity_state.LastConnectionTime(
                        node_id.trajectory_id, submap_id.trajectory_id);
                // 如果节点和子图属于同一轨迹, 或者时间小于阈值
                // 则只需进行 局部搜索窗口 的约束计算(对局部子图进行回环检测)
                if (node_id.trajectory_id == submap_id.trajectory_id ||
                    node_time <
                        last_connection_time +
                            common::FromSeconds(
                                options_.global_constraint_search_after_n_seconds()))
                {
                    // If the node and the submap belong to the same trajectory or if there
                    // has been a recent global constraint that ties that node's trajectory to
                    // the submap's trajectory, it suffices to do a match constrained to a
                    // local search window.
                    maybe_add_local_constraint = true;
                }
                else if (global_localization_samplers_[node_id.trajectory_id]->Pulse())
                {
                    maybe_add_global_constraint = true;
                }
                constant_data = const_cast<TrajectoryNode::Data *>(
                    data_.trajectory_nodes.at(node_id).constant_data.get());
                submap = static_cast<const Submap2D *>(
                    data_.submap_data.at(submap_id).submap.get());
                active_node_to_frozen_submap =
                    node_id.trajectory_id != submap_id.trajectory_id &&
                    IsTrajectoryActive(node_id.trajectory_id) &&
                    IsTrajectoryFrozen(submap_id.trajectory_id);
            }
            // 建图时只会执行这块, 通过局部搜索进行回环检测
            if (maybe_add_local_constraint)
            {
                const transform::Rigid2d initial_relative_pose =
                    optimization_problem_->submap_data()
                        .at(submap_id)
                        .global_pose.inverse() *
                    optimization_problem_->node_data().at(node_id).global_pose_2d;
                constraint_builder_.MaybeAddConstraint(
                    submap_id, submap, node_id, constant_data,
                    initial_relative_pose, active_node_to_frozen_submap);
            }
            else if (maybe_add_global_constraint)
            {
                constraint_builder_.MaybeAddGlobalConstraint(submap_id, submap, node_id,
                                                             constant_data,
                                                             active_node_to_frozen_submap);
            }
        }

        /**
         * @brief 保存节点, 计算子图内约束, 查找回环
         *
         * @param[in] node_id 刚加入的节点ID
         * @param[in] insertion_submaps active_submaps
         * @param[in] newly_finished_submap 是否是新finished的submap
         * @return WorkItem::Result 是否需要执行全局优化
         */
        WorkItem::Result PoseGraph2D::ComputeConstraintsForNode(
            const NodeId &node_id,
            std::vector<std::shared_ptr<const Submap2D>> insertion_submaps,
            const bool newly_finished_submap)
        {
            const size_t queued_work_items_at_start = GetWorkQueueSize();
            std::vector<SubmapId> submap_ids;
            std::vector<SubmapId> finished_submap_ids;
            std::set<NodeId> newly_finished_submap_node_ids;
            std::set<int> frozen_trajectory_ids;
            bool has_recent_active_frozen_connection = false;
            bool newly_finished_submap_on_active_trajectory = false;
            bool force_optimization_for_pure_localization_trim = false;
            bool localization_degraded = false;
            bool recovery_full_search = false;
            int active_submap_count = 0;
            int skipped_active_frozen_submaps = 0;
            std::string recovery_search_reason;
            std::shared_ptr<const TrajectoryNode::Data>
                scan_map_health_constant_data;
            transform::Rigid2d scan_map_health_global_pose =
                transform::Rigid2d::Identity();
            std::vector<CurrentPoseScanMapSubmap> scan_map_health_submaps;
            std::shared_ptr<const MapScanDistanceField>
                map_scan_health_distance_field;
            int map_scan_health_max_sampled_points =
                kMapScanHealthOkMaxSampledPoints;
            // 保存节点与计算子图内约束
            {
                absl::MutexLock locker(&mutex_);
                const auto &constant_data =
                    data_.trajectory_nodes.at(node_id).constant_data;
                submap_ids = InitializeGlobalSubmapPoses(
                    node_id.trajectory_id, constant_data->time, insertion_submaps);
                CHECK_EQ(submap_ids.size(), insertion_submaps.size());
                const SubmapId matching_id = submap_ids.front();
                const transform::Rigid2d local_pose_2d =
                    transform::Project2D(constant_data->local_pose *
                                         transform::Rigid3d::Rotation(
                                             constant_data->gravity_alignment.inverse()));
                const transform::Rigid2d global_pose_2d =
                    optimization_problem_->submap_data().at(matching_id).global_pose *
                    constraints::ComputeSubmapPose(*insertion_submaps.front()).inverse() *
                    local_pose_2d;
                // 把该节点的信息加入到OptimizationProblem中
                optimization_problem_->AddTrajectoryNode(
                    matching_id.trajectory_id,
                    optimization::NodeSpec2D{constant_data->time, local_pose_2d,
                                             global_pose_2d,
                                             constant_data->gravity_alignment});
                const bool node_on_active_trajectory =
                    IsTrajectoryActive(node_id.trajectory_id);
                has_recent_active_frozen_connection =
                    node_on_active_trajectory &&
                    HasRecentActiveFrozenConnection(node_id.trajectory_id,
                                                    node_id.node_index);
                if (node_on_active_trajectory &&
                    HasFrozenTrajectoryForLocalization())
                {
                    LocalizationRecoveryRuntime &recovery =
                        localization_recovery_[node_id.trajectory_id];
                    localization_degraded =
                        IsLocalizationDegraded(node_id, recovery);
                    if (localization_degraded)
                    {
                        recovery_full_search = ShouldRunRecoveryFullSearch(
                            node_id, queued_work_items_at_start, &recovery,
                            &recovery_search_reason);
                        if (!recovery_full_search)
                        {
                            recovery.recovery_state = "DEGRADED";
                            recovery.recovery_reason = recovery_search_reason;
                            RecordLocalizationHealthRecoveryState(
                                recovery.recovery_state,
                                recovery.recovery_reason);
                        }
                    }
                    else if (recovery.recovery_state != "OK" &&
                             recovery.recovery_state != "COOLDOWN" &&
                             recovery.recovery_state != "AUTO_RELOCATING")
                    {
                        recovery.recovery_state = "OK";
                        recovery.recovery_reason.clear();
                        RecordLocalizationHealthRecoveryState("OK", "");
                    }
                }
                // 遍历2个子图, 将节点加入子图的节点列表中, 计算子图原点与及节点间的约束(子图内约束)
                for (size_t i = 0; i < insertion_submaps.size(); ++i)
                {
                    const SubmapId submap_id = submap_ids[i];
                    // Even if this was the last node added to 'submap_id', the submap will
                    // only be marked as finished in 'data_.submap_data' further below.
                    CHECK(data_.submap_data.at(submap_id).state ==
                          SubmapState::kNoConstraintSearch);
                    data_.submap_data.at(submap_id).node_ids.emplace(node_id);
                    const transform::Rigid2d constraint_transform =
                        constraints::ComputeSubmapPose(*insertion_submaps[i]).inverse() *
                        local_pose_2d;
                    data_.constraints.push_back(
                        Constraint{submap_id,
                                   node_id,
                                   {transform::Embed3D(constraint_transform),
                                    options_.matcher_translation_weight(),
                                    options_.matcher_rotation_weight()},
                                   Constraint::INTRA_SUBMAP});
                }

                // TODO(gaschler): Consider not searching for constraints against
                // trajectories scheduled for deletion.
                // TODO(danielsievers): Add a member variable and avoid having to copy
                // them out here.
                std::vector<std::pair<double, SubmapId>>
                    active_frozen_submap_candidates;
                for (const auto &submap_id_data : data_.submap_data)
                {
                    if (submap_id_data.data.state == SubmapState::kFinished)
                    {
                        CHECK_EQ(submap_id_data.data.node_ids.count(node_id), 0);
                        const bool active_node_to_frozen_submap =
                            node_on_active_trajectory &&
                            node_id.trajectory_id !=
                                submap_id_data.id.trajectory_id &&
                            IsTrajectoryFrozen(submap_id_data.id.trajectory_id);
                        if (active_node_to_frozen_submap &&
                            (has_recent_active_frozen_connection ||
                             localization_degraded))
                        {
                            if (!optimization_problem_->submap_data().Contains(
                                    submap_id_data.id))
                            {
                                finished_submap_ids.emplace_back(
                                    submap_id_data.id);
                                continue;
                            }
                            const auto &submap_pose =
                                optimization_problem_->submap_data()
                                    .at(submap_id_data.id)
                                    .global_pose;
                            const double distance =
                                (submap_pose.inverse() * global_pose_2d)
                                    .translation()
                                    .norm();
                            active_frozen_submap_candidates.emplace_back(
                                distance, submap_id_data.id);
                            continue;
                        }
                        finished_submap_ids.emplace_back(submap_id_data.id);
                    }
                }
                if (!active_frozen_submap_candidates.empty())
                {
                    std::sort(active_frozen_submap_candidates.begin(),
                              active_frozen_submap_candidates.end(),
                              [](const std::pair<double, SubmapId> &lhs,
                                 const std::pair<double, SubmapId> &rhs)
                              {
                                  return lhs.first < rhs.first;
                              });
                    const std::vector<SubmapId> selected_active_frozen_submaps =
                        SelectActiveFrozenSubmapsForSearch(
                            active_frozen_submap_candidates,
                            recovery_full_search, queued_work_items_at_start);
                    for (const SubmapId &submap_id :
                         selected_active_frozen_submaps)
                    {
                        finished_submap_ids.emplace_back(submap_id);
                    }
                    skipped_active_frozen_submaps =
                        static_cast<int>(active_frozen_submap_candidates.size() -
                                         selected_active_frozen_submaps.size());
                    if (recovery_full_search)
                    {
                        RecordLocalizationHealthRecoveryFullSearch(
                            static_cast<int>(
                                selected_active_frozen_submaps.size()),
                            recovery_search_reason);
                    }
                }
                if (newly_finished_submap)
                {
                    const SubmapId newly_finished_submap_id = submap_ids.front();
                    InternalSubmapData &finished_submap_data =
                        data_.submap_data.at(newly_finished_submap_id);
                    CHECK(finished_submap_data.state == SubmapState::kNoConstraintSearch);
                    finished_submap_data.state = SubmapState::kFinished;
                    newly_finished_submap_node_ids = finished_submap_data.node_ids;
                    newly_finished_submap_on_active_trajectory =
                        IsTrajectoryActive(newly_finished_submap_id.trajectory_id);
                }
                for (const auto &trajectory_state : data_.trajectories_state)
                {
                    if (trajectory_state.second.state == TrajectoryState::FROZEN)
                    {
                        frozen_trajectory_ids.insert(trajectory_state.first);
                    }
                }
                active_submap_count =
                    data_.submap_data.SizeOfTrajectoryOrZero(node_id.trajectory_id);
                force_optimization_for_pure_localization_trim =
                    node_on_active_trajectory && HasFrozenTrajectoryForLocalization() &&
                    active_submap_count > kPureLocalizationForceOptimizeSubmaps;
                if (node_on_active_trajectory &&
                    HasFrozenTrajectoryForLocalization() &&
                    node_id.node_index > 0)
                {
                    LocalizationRecoveryRuntime &recovery =
                        localization_recovery_[node_id.trajectory_id];
                    const bool recovery_attention =
                        recovery.recovery_state != "OK" ||
                        recovery.scan_map_bad ||
                        recovery.scan_map_severe_bad ||
                        recovery.recovery_full_search_attempts_since_accept > 0;
                    const bool queue_backlogged =
                        queued_work_items_at_start >=
                        kRecoveryWorkQueueTriggerThreshold;
                    const bool use_recovery_frequency =
                        recovery_attention && !queue_backlogged;
                    const int scan_map_health_node_gap =
                        use_recovery_frequency ? kMapScanHealthRecoveryNodeGap
                                               : kMapScanHealthOkNodeGap;
                    if (node_id.node_index %
                            scan_map_health_node_gap ==
                        0)
                    {
                        scan_map_health_constant_data = constant_data;
                        scan_map_health_global_pose = global_pose_2d;
                        map_scan_health_max_sampled_points =
                            use_recovery_frequency
                                ? kMapScanHealthRecoveryMaxSampledPoints
                                : kMapScanHealthOkMaxSampledPoints;
                        map_scan_health_distance_field =
                            GetMapScanDistanceFieldIfReadyOrStartAsync();
                        std::vector<CurrentPoseScanMapSubmap> candidates;
                        for (const auto &submap_id_data : data_.submap_data)
                        {
                            if (submap_id_data.data.state !=
                                    SubmapState::kFinished ||
                                submap_id_data.id.trajectory_id ==
                                    node_id.trajectory_id ||
                                !IsTrajectoryFrozen(
                                    submap_id_data.id.trajectory_id) ||
                                !optimization_problem_->submap_data().Contains(
                                    submap_id_data.id) ||
                                submap_id_data.data.submap == nullptr)
                            {
                                continue;
                            }
                            const auto submap_2d =
                                std::static_pointer_cast<const Submap2D>(
                                    submap_id_data.data.submap);
                            const auto &submap_pose =
                                optimization_problem_->submap_data()
                                    .at(submap_id_data.id)
                                    .global_pose;
                            const double distance =
                                (submap_pose.inverse() * global_pose_2d)
                                    .translation()
                                    .norm();
                            candidates.push_back(
                                CurrentPoseScanMapSubmap{
                                    submap_id_data.id, submap_2d,
                                    submap_pose, distance});
                        }
                        std::sort(candidates.begin(), candidates.end(),
                                  [](const CurrentPoseScanMapSubmap &lhs,
                                     const CurrentPoseScanMapSubmap &rhs)
                                  { return lhs.distance < rhs.distance; });
                        const size_t checked_submaps =
                            std::min(kCurrentPoseScanMapCheckedSubmaps,
                                     candidates.size());
                        scan_map_health_submaps.assign(
                            candidates.begin(),
                            candidates.begin() + checked_submaps);
                    }
                }
            }

            if (scan_map_health_constant_data != nullptr &&
                map_scan_health_distance_field != nullptr)
            {
                const CurrentPoseScanMapQuality map_quality =
                    ComputeMapScanQuality(
                        *scan_map_health_constant_data,
                        scan_map_health_global_pose,
                        *map_scan_health_distance_field,
                        map_scan_health_max_sampled_points);
                absl::MutexLock locker(&mutex_);
                UpdateLocalizationRecoveryFromMapScanQuality(node_id,
                                                             map_quality);
            }

            if (scan_map_health_constant_data != nullptr &&
                !scan_map_health_submaps.empty())
            {
                const CurrentPoseScanMapQuality quality =
                    ComputeCurrentPoseScanMapQuality(
                        *scan_map_health_constant_data,
                        scan_map_health_global_pose,
                        scan_map_health_submaps);
                const bool bad =
                    quality.sampled_points > 0 &&
                    quality.checked_submaps > 0 &&
                    quality.hit20 >= 0.0 &&
                    quality.mean_distance >= 0.0 &&
                    (quality.hit20 < kMapScanBadHit20 ||
                     quality.mean_distance > kMapScanBadMeanDistance);
                RecordLocalizationHealthCurrentPoseScanMapQuality(
                    quality.hit20, quality.mean_distance,
                    quality.sampled_points, quality.checked_submaps, bad);
            }

            if (skipped_active_frozen_submaps > 0)
            {
                LOG_EVERY_N(WARNING, 20)
                    << "[ConstraintSearchGate]Limit active-frozen submap "
                    << "search node=" << node_id
                    << " skipped_submaps=" << skipped_active_frozen_submaps
                    << " stable_connection="
                    << has_recent_active_frozen_connection
                    << " queue_size=" << queued_work_items_at_start
                    << " soft_backlog_threshold="
                    << kWorkQueueConstraintSoftBacklogThreshold
                    << " hard_backlog_threshold="
                    << kWorkQueueDropSensorDataThreshold;
            }
            if (recovery_full_search)
            {
                LOG(WARNING)
                    << "[LocalizationRecovery]Run recovery active-frozen "
                    << "search node=" << node_id
                    << " reason=" << recovery_search_reason
                    << " queue_size=" << queued_work_items_at_start;
            }
            if (force_optimization_for_pure_localization_trim)
            {
                RecordLocalizationHealthPureLocalizationForceOptimization(
                    active_submap_count);
                LOG_EVERY_N(WARNING, 20)
                    << "[PureLocalizationTrimmer]Force optimization soon, "
                    << "trajectory=" << node_id.trajectory_id
                    << " active_submaps=" << active_submap_count
                    << " force_threshold="
                    << kPureLocalizationForceOptimizeSubmaps;
            }

            for (const auto &submap_id : finished_submap_ids)
            {
                ComputeConstraint(node_id, submap_id);
            }

            if (newly_finished_submap)
            {
                const SubmapId newly_finished_submap_id = submap_ids.front();
                // We have a new completed submap, so we look into adding constraints for
                // old nodes.
                int skipped_frozen_nodes_for_active_submap = 0;
                for (const auto &node_id_data : optimization_problem_->node_data())
                {
                    const NodeId &old_node_id = node_id_data.id;
                    const bool frozen_node_to_active_submap =
                        newly_finished_submap_on_active_trajectory &&
                        frozen_trajectory_ids.count(old_node_id.trajectory_id) > 0;
                    if (frozen_node_to_active_submap &&
                        has_recent_active_frozen_connection)
                    {
                        ++skipped_frozen_nodes_for_active_submap;
                        continue;
                    }
                    if (newly_finished_submap_node_ids.count(old_node_id) == 0)
                    {
                        ComputeConstraint(old_node_id, newly_finished_submap_id);
                    }
                }
                if (skipped_frozen_nodes_for_active_submap > 0)
                {
                    LOG_EVERY_N(WARNING, 10)
                        << "[ConstraintSearchGate]Skip frozen-node to active "
                        << "submap searches submap=" << newly_finished_submap_id
                        << " skipped_nodes="
                        << skipped_frozen_nodes_for_active_submap
                        << " stable_connection="
                        << has_recent_active_frozen_connection
                        << " queue_size=" << queued_work_items_at_start;
                }
            }
            constraint_builder_.NotifyEndOfNode();
            {
                absl::MutexLock locker(&mutex_);
                ++num_nodes_since_last_loop_closure_;
                if (force_optimization_for_pure_localization_trim)
                {
                    return WorkItem::Result::kRunOptimization;
                }
                if (options_.optimize_every_n_nodes() > 0 &&
                    num_nodes_since_last_loop_closure_ >
                        options_.optimize_every_n_nodes())
                {
                    return WorkItem::Result::kRunOptimization;
                }
            }

            if (flirt::need_optimizing.load())
            {
                flirt::need_optimizing.store(false);
                LOG(WARNING) << "Try optimization once!";
                return WorkItem::Result::kRunOptimization;
            }

            return WorkItem::Result::kDoNotRunOptimization;
        }

        PoseGraph2D::CurrentPoseScanMapQuality
        PoseGraph2D::ComputeCurrentPoseScanMapQuality(
            const TrajectoryNode::Data &constant_data,
            const transform::Rigid2d &global_pose,
            const std::vector<CurrentPoseScanMapSubmap> &submaps) const
        {
            CurrentPoseScanMapQuality best_quality;
            best_quality.checked_submaps =
                static_cast<int>(submaps.size());
            const sensor::PointCloud &point_cloud =
                constant_data.filtered_gravity_aligned_point_cloud;
            if (point_cloud.empty() || submaps.empty())
            {
                return best_quality;
            }

            for (const CurrentPoseScanMapSubmap &submap_data : submaps)
            {
                if (submap_data.submap == nullptr ||
                    submap_data.submap->grid() == nullptr)
                {
                    continue;
                }
                const Grid2D &grid = *submap_data.submap->grid();
                const transform::Rigid2d pose_in_submap =
                    submap_data.global_pose.inverse() * global_pose;
                const double resolution = grid.limits().resolution();
                const int hit_radius_cells = std::max(
                    1, common::RoundToInt(
                           kCurrentPoseScanMapHitRadiusMeters / resolution));
                const int search_radius_cells = std::max(
                    hit_radius_cells,
                    common::RoundToInt(
                        kCurrentPoseScanMapSearchRadiusMeters / resolution));
                const size_t sample_step = std::max<size_t>(
                    1, (point_cloud.size() +
                        kCurrentPoseScanMapMaxSampledPoints - 1) /
                           kCurrentPoseScanMapMaxSampledPoints);

                int sampled_points = 0;
                int hit20_count = 0;
                double distance_sum = 0.0;
                for (size_t point_index = 0; point_index < point_cloud.size();
                     point_index += sample_step)
                {
                    const Eigen::Vector2d local_point(
                        point_cloud[point_index].position.x(),
                        point_cloud[point_index].position.y());
                    const Eigen::Vector2f point =
                        (pose_in_submap * local_point).cast<float>();
                    const Eigen::Array2i cell_index =
                        grid.limits().GetCellIndex(point);
                    ++sampled_points;

                    double best_distance =
                        kCurrentPoseScanMapSearchRadiusMeters;
                    bool found_occupied = false;
                    for (int dx = -search_radius_cells;
                         dx <= search_radius_cells; ++dx)
                    {
                        for (int dy = -search_radius_cells;
                             dy <= search_radius_cells; ++dy)
                        {
                            const Eigen::Array2i candidate_index =
                                cell_index + Eigen::Array2i(dx, dy);
                            if (!grid.limits().Contains(candidate_index) ||
                                !grid.IsKnown(candidate_index))
                            {
                                continue;
                            }
                            const double probability =
                                1.0 - grid.GetCorrespondenceCost(
                                          candidate_index);
                            if (probability <
                                kCurrentPoseScanMapOccupiedProbabilityThreshold)
                            {
                                continue;
                            }
                            const Eigen::Vector2d cell_center =
                                grid.limits()
                                    .GetCellCenter(candidate_index)
                                    .cast<double>();
                            const double distance =
                                (cell_center - point.cast<double>()).norm();
                            if (distance < best_distance)
                            {
                                best_distance = distance;
                                found_occupied = true;
                            }
                        }
                    }
                    if (found_occupied &&
                        best_distance <=
                            kCurrentPoseScanMapHitRadiusMeters)
                    {
                        ++hit20_count;
                    }
                    distance_sum +=
                        found_occupied
                            ? best_distance
                            : kCurrentPoseScanMapSearchRadiusMeters;
                }

                if (sampled_points <= 0)
                {
                    continue;
                }
                CurrentPoseScanMapQuality quality;
                quality.hit20 = static_cast<double>(hit20_count) /
                                static_cast<double>(sampled_points);
                quality.mean_distance =
                    distance_sum / static_cast<double>(sampled_points);
                quality.sampled_points = sampled_points;
                quality.checked_submaps = static_cast<int>(submaps.size());
                if (best_quality.sampled_points == 0 ||
                    quality.hit20 > best_quality.hit20 ||
                    (quality.hit20 == best_quality.hit20 &&
                     quality.mean_distance < best_quality.mean_distance))
                {
                    best_quality = quality;
                }
            }
            return best_quality;
        }

        std::vector<PoseGraph2D::MapScanDistanceFieldSubmapSnapshot>
        PoseGraph2D::SnapshotMapScanDistanceFieldSubmaps(
            const bool include_unfrozen_finished_submaps) const
        {
            std::vector<MapScanDistanceFieldSubmapSnapshot> submaps;
            for (const auto &submap_id_data : data_.submap_data)
            {
                if (submap_id_data.data.state != SubmapState::kFinished ||
                    (!include_unfrozen_finished_submaps &&
                     !IsTrajectoryFrozen(submap_id_data.id.trajectory_id)) ||
                    !optimization_problem_->submap_data().Contains(
                        submap_id_data.id) ||
                    submap_id_data.data.submap == nullptr)
                {
                    continue;
                }
                const auto submap_2d =
                    std::static_pointer_cast<const Submap2D>(
                        submap_id_data.data.submap);
                if (submap_2d->grid() == nullptr)
                {
                    continue;
                }
                submaps.push_back(MapScanDistanceFieldSubmapSnapshot{
                    submap_2d,
                    optimization_problem_->submap_data()
                        .at(submap_id_data.id)
                        .global_pose});
            }
            return submaps;
        }

        std::shared_ptr<const PoseGraph2D::MapScanDistanceField>
        PoseGraph2D::BuildMapScanDistanceField(
            const std::vector<MapScanDistanceFieldSubmapSnapshot> &submaps) const
        {
            auto field = std::make_shared<MapScanDistanceField>();
            field->frozen_finished_submap_count =
                static_cast<int>(submaps.size());
            if (submaps.empty())
            {
                return field;
            }

            double resolution =
                submaps.front().submap->grid()->limits().resolution();
            for (const MapScanDistanceFieldSubmapSnapshot &submap_snapshot : submaps)
            {
                resolution = std::min(
                    resolution,
                    submap_snapshot.submap->grid()->limits().resolution());
            }
            field->resolution = resolution;

            double min_x = std::numeric_limits<double>::infinity();
            double min_y = std::numeric_limits<double>::infinity();
            double max_x = -std::numeric_limits<double>::infinity();
            double max_y = -std::numeric_limits<double>::infinity();
            for (const MapScanDistanceFieldSubmapSnapshot &submap_snapshot : submaps)
            {
                const Grid2D &grid = *submap_snapshot.submap->grid();
                const auto &limits = grid.limits();
                const int max_cell_x = limits.cell_limits().num_x_cells - 1;
                const int max_cell_y = limits.cell_limits().num_y_cells - 1;
                const std::array<Eigen::Array2i, 4> corners = {{
                    Eigen::Array2i(0, 0),
                    Eigen::Array2i(max_cell_x, 0),
                    Eigen::Array2i(0, max_cell_y),
                    Eigen::Array2i(max_cell_x, max_cell_y)}};
                for (const Eigen::Array2i &corner : corners)
                {
                    const Eigen::Vector2d global =
                        submap_snapshot.global_pose *
                        limits.GetCellCenter(corner).cast<double>();
                    min_x = std::min(min_x, global.x());
                    min_y = std::min(min_y, global.y());
                    max_x = std::max(max_x, global.x());
                    max_y = std::max(max_y, global.y());
                }
            }

            const double margin = kCurrentPoseScanMapSearchRadiusMeters +
                                  2.0 * resolution;
            const int min_ix =
                static_cast<int>(std::floor((min_x - margin) / resolution));
            const int min_iy =
                static_cast<int>(std::floor((min_y - margin) / resolution));
            const int max_ix =
                static_cast<int>(std::ceil((max_x + margin) / resolution));
            const int max_iy =
                static_cast<int>(std::ceil((max_y + margin) / resolution));
            field->origin_x = min_ix * resolution;
            field->origin_y = min_iy * resolution;
            field->width = std::max(1, max_ix - min_ix + 1);
            field->height = std::max(1, max_iy - min_iy + 1);

            const size_t cell_count =
                static_cast<size_t>(field->width) *
                static_cast<size_t>(field->height);
            field->known.assign(cell_count, 0);
            std::vector<float> distance_cells(
                cell_count, std::numeric_limits<float>::infinity());
            auto flat_index = [width = field->width](const int x,
                                                     const int y)
            {
                return static_cast<size_t>(y) * static_cast<size_t>(width) +
                       static_cast<size_t>(x);
            };

            int occupied_cells = 0;
            for (const MapScanDistanceFieldSubmapSnapshot &submap_snapshot : submaps)
            {
                const Grid2D &grid = *submap_snapshot.submap->grid();
                const auto &limits = grid.limits();
                Eigen::Array2i cropped_offset;
                CellLimits cropped_limits;
                grid.ComputeCroppedLimits(&cropped_offset, &cropped_limits);
                for (int x = 0; x < cropped_limits.num_x_cells; ++x)
                {
                    for (int y = 0; y < cropped_limits.num_y_cells; ++y)
                    {
                        const Eigen::Array2i cell_index =
                            cropped_offset + Eigen::Array2i(x, y);
                        if (!grid.IsKnown(cell_index))
                        {
                            continue;
                        }
                        const Eigen::Vector2d global =
                            submap_snapshot.global_pose *
                            limits.GetCellCenter(cell_index).cast<double>();
                        const int gx = static_cast<int>(
                            std::floor((global.x() - field->origin_x) /
                                       resolution));
                        const int gy = static_cast<int>(
                            std::floor((global.y() - field->origin_y) /
                                       resolution));
                        if (gx < 0 || gx >= field->width ||
                            gy < 0 || gy >= field->height)
                        {
                            continue;
                        }
                        const size_t index = flat_index(gx, gy);
                        field->known[index] = 1;
                        const double probability =
                            1.0 - grid.GetCorrespondenceCost(cell_index);
                        if (probability >=
                            kCurrentPoseScanMapOccupiedProbabilityThreshold)
                        {
                            if (distance_cells[index] != 0.0f)
                            {
                                ++occupied_cells;
                            }
                            distance_cells[index] = 0.0f;
                        }
                    }
                }
            }

            if (occupied_cells <= 0)
            {
                return field;
            }

            const float diagonal = static_cast<float>(std::sqrt(2.0));
            for (int y = 0; y < field->height; ++y)
            {
                for (int x = 0; x < field->width; ++x)
                {
                    const size_t index = flat_index(x, y);
                    float best = distance_cells[index];
                    if (x > 0)
                    {
                        best = std::min(best, distance_cells[flat_index(x - 1, y)] + 1.0f);
                    }
                    if (y > 0)
                    {
                        best = std::min(best, distance_cells[flat_index(x, y - 1)] + 1.0f);
                    }
                    if (x > 0 && y > 0)
                    {
                        best = std::min(best, distance_cells[flat_index(x - 1, y - 1)] + diagonal);
                    }
                    if (x + 1 < field->width && y > 0)
                    {
                        best = std::min(best, distance_cells[flat_index(x + 1, y - 1)] + diagonal);
                    }
                    distance_cells[index] = best;
                }
            }
            for (int y = field->height - 1; y >= 0; --y)
            {
                for (int x = field->width - 1; x >= 0; --x)
                {
                    const size_t index = flat_index(x, y);
                    float best = distance_cells[index];
                    if (x + 1 < field->width)
                    {
                        best = std::min(best, distance_cells[flat_index(x + 1, y)] + 1.0f);
                    }
                    if (y + 1 < field->height)
                    {
                        best = std::min(best, distance_cells[flat_index(x, y + 1)] + 1.0f);
                    }
                    if (x + 1 < field->width && y + 1 < field->height)
                    {
                        best = std::min(best, distance_cells[flat_index(x + 1, y + 1)] + diagonal);
                    }
                    if (x > 0 && y + 1 < field->height)
                    {
                        best = std::min(best, distance_cells[flat_index(x - 1, y + 1)] + diagonal);
                    }
                    distance_cells[index] = best;
                }
            }

            field->distance_m.resize(cell_count);
            for (size_t i = 0; i < cell_count; ++i)
            {
                const float distance_m =
                    std::isfinite(distance_cells[i])
                        ? distance_cells[i] * static_cast<float>(resolution)
                        : static_cast<float>(
                              kCurrentPoseScanMapSearchRadiusMeters);
                field->distance_m[i] = std::min(
                    distance_m,
                    static_cast<float>(kCurrentPoseScanMapSearchRadiusMeters));
            }
            field->valid = true;
            LOG(WARNING) << "[MapScanHealth]Built frozen map distance field "
                         << "submaps="
                         << field->frozen_finished_submap_count
                         << " size=" << field->width << "x"
                         << field->height
                         << " resolution=" << field->resolution
                         << " occupied_cells=" << occupied_cells;
            return field;
        }

        std::shared_ptr<const PoseGraph2D::MapScanDistanceField>
        PoseGraph2D::LoadMapScanDistanceFieldCache(
            const std::string &cache_filename,
            const std::string &cache_key) const
        {
            if (cache_filename.empty() || cache_key.empty())
            {
                return nullptr;
            }
            std::ifstream input(cache_filename, std::ios::binary);
            if (!input.is_open())
            {
                return nullptr;
            }
            std::string line;
            if (!std::getline(input, line) ||
                line != kMapScanDistanceFieldCacheMagic)
            {
                LOG(WARNING) << "[MapScanHealth]Ignore invalid distance field "
                             << "cache magic: " << cache_filename;
                return nullptr;
            }

            std::map<std::string, std::string> header;
            while (std::getline(input, line))
            {
                if (line == "END_HEADER")
                {
                    break;
                }
                std::istringstream line_stream(line);
                std::string key;
                if (!(line_stream >> key))
                {
                    continue;
                }
                std::string value;
                std::getline(line_stream, value);
                if (!value.empty() && value.front() == ' ')
                {
                    value.erase(value.begin());
                }
                header[key] = value;
            }

            auto find_value = [&header](const std::string &key)
                                  -> const std::string *
            {
                const auto it = header.find(key);
                return it == header.end() ? nullptr : &it->second;
            };
            try
            {
                const std::string *version = find_value("version");
                const std::string *stored_cache_key = find_value("cache_key");
                const std::string *resolution = find_value("resolution");
                const std::string *origin_x = find_value("origin_x");
                const std::string *origin_y = find_value("origin_y");
                const std::string *width = find_value("width");
                const std::string *height = find_value("height");
                const std::string *submap_count = find_value("submap_count");
                const std::string *max_distance_m =
                    find_value("max_distance_m");
                const std::string *occupied_probability_threshold =
                    find_value("occupied_probability_threshold");
                if (version == nullptr || stored_cache_key == nullptr ||
                    resolution == nullptr || origin_x == nullptr ||
                    origin_y == nullptr || width == nullptr ||
                    height == nullptr || submap_count == nullptr ||
                    max_distance_m == nullptr ||
                    occupied_probability_threshold == nullptr)
                {
                    LOG(WARNING)
                        << "[MapScanHealth]Ignore incomplete distance field "
                        << "cache header: " << cache_filename;
                    return nullptr;
                }
                if (std::stoi(*version) !=
                        kMapScanDistanceFieldCacheVersion ||
                    *stored_cache_key != cache_key ||
                    std::fabs(std::stod(*max_distance_m) -
                              kCurrentPoseScanMapSearchRadiusMeters) > 1e-9 ||
                    std::fabs(std::stod(*occupied_probability_threshold) -
                              kCurrentPoseScanMapOccupiedProbabilityThreshold) >
                        1e-9)
                {
                    LOG(INFO) << "[MapScanHealth]Distance field cache does "
                              << "not match current map/parameters: "
                              << cache_filename;
                    return nullptr;
                }
                auto field = std::make_shared<MapScanDistanceField>();
                field->resolution = std::stod(*resolution);
                field->origin_x = std::stod(*origin_x);
                field->origin_y = std::stod(*origin_y);
                field->width = std::stoi(*width);
                field->height = std::stoi(*height);
                field->frozen_finished_submap_count =
                    std::stoi(*submap_count);
                if (field->width <= 0 || field->height <= 0 ||
                    field->resolution <= 0.0)
                {
                    return nullptr;
                }
                const size_t cell_count =
                    static_cast<size_t>(field->width) *
                    static_cast<size_t>(field->height);
                std::vector<uint16_t> distance_mm(cell_count);
                input.read(reinterpret_cast<char *>(distance_mm.data()),
                           static_cast<std::streamsize>(
                               distance_mm.size() * sizeof(uint16_t)));
                if (!input)
                {
                    LOG(WARNING)
                        << "[MapScanHealth]Truncated distance field cache "
                        << "distance block: " << cache_filename;
                    return nullptr;
                }
                field->known.resize(cell_count);
                input.read(reinterpret_cast<char *>(field->known.data()),
                           static_cast<std::streamsize>(
                               field->known.size() * sizeof(uint8_t)));
                if (!input)
                {
                    LOG(WARNING)
                        << "[MapScanHealth]Truncated distance field cache "
                        << "known block: " << cache_filename;
                    return nullptr;
                }
                field->distance_m.resize(cell_count);
                for (size_t i = 0; i < cell_count; ++i)
                {
                    field->distance_m[i] =
                        static_cast<float>(distance_mm[i]) /
                        static_cast<float>(
                            kMapScanDistanceFieldCacheDistanceScale);
                }
                field->valid = true;
                LOG(INFO) << "[MapScanHealth]Loaded map distance field cache "
                          << cache_filename << " submaps="
                          << field->frozen_finished_submap_count << " size="
                          << field->width << "x" << field->height
                          << " resolution=" << field->resolution;
                return field;
            }
            catch (const std::exception &e)
            {
                LOG(WARNING) << "[MapScanHealth]Failed to parse distance "
                             << "field cache " << cache_filename << ": "
                             << e.what();
                return nullptr;
            }
        }

        bool PoseGraph2D::SaveMapScanDistanceFieldCache(
            const std::string &cache_filename, const std::string &cache_key,
            const MapScanDistanceField &field) const
        {
            if (cache_filename.empty() || cache_key.empty() || !field.valid ||
                field.width <= 0 || field.height <= 0 ||
                field.distance_m.empty() || field.known.empty())
            {
                return false;
            }
            const size_t cell_count =
                static_cast<size_t>(field.width) *
                static_cast<size_t>(field.height);
            if (field.distance_m.size() != cell_count ||
                field.known.size() != cell_count)
            {
                LOG(WARNING) << "[MapScanHealth]Skip distance field cache "
                             << "write due to inconsistent field size.";
                return false;
            }
            std::vector<uint16_t> distance_mm(cell_count);
            for (size_t i = 0; i < cell_count; ++i)
            {
                const double clamped_distance = std::max(
                    0.0, std::min<double>(
                             field.distance_m[i],
                             kCurrentPoseScanMapSearchRadiusMeters));
                distance_mm[i] = static_cast<uint16_t>(std::lround(
                    clamped_distance *
                    kMapScanDistanceFieldCacheDistanceScale));
            }

            const std::string tmp_filename = cache_filename + ".tmp";
            std::ofstream output(tmp_filename,
                                 std::ios::binary | std::ios::trunc);
            if (!output.is_open())
            {
                LOG(WARNING) << "[MapScanHealth]Failed to open distance field "
                             << "cache temp file: " << tmp_filename;
                return false;
            }
            output << kMapScanDistanceFieldCacheMagic << "\n"
                   << "version " << kMapScanDistanceFieldCacheVersion << "\n"
                   << "cache_key " << cache_key << "\n"
                   << "resolution " << std::setprecision(17)
                   << field.resolution << "\n"
                   << "origin_x " << std::setprecision(17) << field.origin_x
                   << "\n"
                   << "origin_y " << std::setprecision(17) << field.origin_y
                   << "\n"
                   << "width " << field.width << "\n"
                   << "height " << field.height << "\n"
                   << "submap_count "
                   << field.frozen_finished_submap_count << "\n"
                   << "max_distance_m " << std::setprecision(17)
                   << kCurrentPoseScanMapSearchRadiusMeters << "\n"
                   << "hit_radius_m " << std::setprecision(17)
                   << kCurrentPoseScanMapHitRadiusMeters << "\n"
                   << "occupied_probability_threshold "
                   << std::setprecision(17)
                   << kCurrentPoseScanMapOccupiedProbabilityThreshold << "\n"
                   << "distance_scale "
                   << kMapScanDistanceFieldCacheDistanceScale << "\n"
                   << "encoding uint16_mm_then_uint8_known\n"
                   << "END_HEADER\n";
            output.write(reinterpret_cast<const char *>(distance_mm.data()),
                         static_cast<std::streamsize>(
                             distance_mm.size() * sizeof(uint16_t)));
            output.write(reinterpret_cast<const char *>(field.known.data()),
                         static_cast<std::streamsize>(
                             field.known.size() * sizeof(uint8_t)));
            output.close();
            if (!output)
            {
                LOG(WARNING) << "[MapScanHealth]Failed while writing "
                             << "distance field cache temp file: "
                             << tmp_filename;
                std::remove(tmp_filename.c_str());
                return false;
            }
            if (std::rename(tmp_filename.c_str(), cache_filename.c_str()) != 0)
            {
                LOG(WARNING) << "[MapScanHealth]Failed to atomically replace "
                             << "distance field cache " << cache_filename
                             << ": " << std::strerror(errno);
                std::remove(tmp_filename.c_str());
                return false;
            }
            LOG(INFO) << "[MapScanHealth]Saved map distance field cache "
                      << cache_filename << " submaps="
                      << field.frozen_finished_submap_count << " size="
                      << field.width << "x" << field.height
                      << " resolution=" << field.resolution;
            return true;
        }

        void PoseGraph2D::MaybeCollectFinishedMapScanDistanceFieldTask()
        {
            if (map_scan_distance_field_future_.valid() &&
                map_scan_distance_field_future_.wait_for(
                    std::chrono::seconds(0)) == std::future_status::ready)
            {
                map_scan_distance_field_future_.get();
            }
        }

        void PoseGraph2D::InvalidateMapScanDistanceField()
        {
            map_scan_distance_field_.reset();
            ++map_scan_distance_field_generation_;
        }

        std::shared_ptr<const PoseGraph2D::MapScanDistanceField>
        PoseGraph2D::GetMapScanDistanceFieldIfReadyOrStartAsync()
        {
            MaybeCollectFinishedMapScanDistanceFieldTask();
            const std::vector<MapScanDistanceFieldSubmapSnapshot> submaps =
                SnapshotMapScanDistanceFieldSubmaps(
                    false /* include_unfrozen_finished_submaps */);
            const int frozen_finished_submap_count =
                static_cast<int>(submaps.size());
            if (frozen_finished_submap_count <= 0)
            {
                return nullptr;
            }
            if (map_scan_distance_field_ != nullptr &&
                map_scan_distance_field_->valid &&
                map_scan_distance_field_->frozen_finished_submap_count ==
                    frozen_finished_submap_count)
            {
                return map_scan_distance_field_;
            }
            if (!map_scan_distance_field_cache_filename_.empty() &&
                !map_scan_distance_field_cache_key_.empty())
            {
                std::shared_ptr<const MapScanDistanceField> loaded =
                    LoadMapScanDistanceFieldCache(
                        map_scan_distance_field_cache_filename_,
                        map_scan_distance_field_cache_key_);
                if (loaded != nullptr && loaded->valid &&
                    loaded->frozen_finished_submap_count ==
                        frozen_finished_submap_count)
                {
                    map_scan_distance_field_ = loaded;
                    return map_scan_distance_field_;
                }
            }
            if (map_scan_distance_field_future_.valid() &&
                map_scan_distance_field_future_.wait_for(
                    std::chrono::seconds(0)) != std::future_status::ready)
            {
                return nullptr;
            }
            if (map_scan_distance_field_build_in_progress_)
            {
                return nullptr;
            }
            InvalidateMapScanDistanceField();
            const int generation = map_scan_distance_field_generation_;
            const std::string cache_filename =
                map_scan_distance_field_cache_filename_;
            const std::string cache_key = map_scan_distance_field_cache_key_;
            map_scan_distance_field_build_in_progress_ = true;
            map_scan_distance_field_future_ =
                std::async(std::launch::async,
                           [this, submaps, generation, cache_filename,
                            cache_key]()
                           {
                               const auto field =
                                   BuildMapScanDistanceField(submaps);
                               if (field != nullptr && field->valid &&
                                   !cache_filename.empty() &&
                                   !cache_key.empty())
                               {
                                   SaveMapScanDistanceFieldCache(
                                       cache_filename, cache_key, *field);
                               }
                               absl::MutexLock locker(&mutex_);
                               if (generation ==
                                   map_scan_distance_field_generation_)
                               {
                                   map_scan_distance_field_ = field;
                               }
                               map_scan_distance_field_build_in_progress_ =
                                   false;
                           });
            LOG(INFO) << "[MapScanHealth]Started async map distance field "
                      << "build submaps=" << frozen_finished_submap_count
                      << " cache="
                      << (cache_filename.empty() ? "<none>" : cache_filename);
            return nullptr;
        }

        void PoseGraph2D::ConfigureMapScanDistanceFieldCache(
            const std::string &cache_filename, const std::string &cache_key)
        {
            std::future<void> previous_future;
            {
                absl::MutexLock locker(&mutex_);
                if (map_scan_distance_field_future_.valid())
                {
                    previous_future =
                        std::move(map_scan_distance_field_future_);
                    map_scan_distance_field_build_in_progress_ = false;
                    InvalidateMapScanDistanceField();
                }
            }
            if (previous_future.valid())
            {
                previous_future.wait();
            }

            std::shared_ptr<const MapScanDistanceField> loaded =
                LoadMapScanDistanceFieldCache(cache_filename, cache_key);
            {
                absl::MutexLock locker(&mutex_);
                map_scan_distance_field_cache_filename_ = cache_filename;
                map_scan_distance_field_cache_key_ = cache_key;
                InvalidateMapScanDistanceField();
                if (loaded != nullptr)
                {
                    map_scan_distance_field_ = loaded;
                    LOG(INFO) << "[MapScanHealth]Using cached map distance "
                              << "field: " << cache_filename;
                }
                else
                {
                    GetMapScanDistanceFieldIfReadyOrStartAsync();
                }
            }
        }

        bool PoseGraph2D::BuildAndSaveMapScanDistanceFieldCache(
            const std::string &cache_filename, const std::string &cache_key)
        {
            if (cache_filename.empty() || cache_key.empty())
            {
                return false;
            }
            std::future<void> previous_future;
            {
                absl::MutexLock locker(&mutex_);
                if (map_scan_distance_field_future_.valid())
                {
                    previous_future =
                        std::move(map_scan_distance_field_future_);
                    map_scan_distance_field_build_in_progress_ = false;
                    InvalidateMapScanDistanceField();
                }
            }
            if (previous_future.valid())
            {
                previous_future.wait();
            }

            std::vector<MapScanDistanceFieldSubmapSnapshot> submaps;
            {
                absl::MutexLock locker(&mutex_);
                submaps = SnapshotMapScanDistanceFieldSubmaps(
                    true /* include_unfrozen_finished_submaps */);
            }
            const auto field = BuildMapScanDistanceField(submaps);
            if (field == nullptr || !field->valid)
            {
                LOG(WARNING)
                    << "[MapScanHealth]Failed to build map distance field "
                    << "cache: no valid finished submaps.";
                return false;
            }
            const bool saved =
                SaveMapScanDistanceFieldCache(cache_filename, cache_key,
                                              *field);
            {
                absl::MutexLock locker(&mutex_);
                map_scan_distance_field_cache_filename_ = cache_filename;
                map_scan_distance_field_cache_key_ = cache_key;
                InvalidateMapScanDistanceField();
                map_scan_distance_field_ = field;
            }
            return saved;
        }

        PoseGraph2D::CurrentPoseScanMapQuality
        PoseGraph2D::ComputeMapScanQuality(
            const TrajectoryNode::Data &constant_data,
            const transform::Rigid2d &global_pose,
            const MapScanDistanceField &distance_field,
            const int max_sampled_points) const
        {
            CurrentPoseScanMapQuality quality;
            quality.checked_submaps =
                distance_field.frozen_finished_submap_count;
            const sensor::PointCloud &point_cloud =
                constant_data.filtered_gravity_aligned_point_cloud;
            if (!distance_field.valid || point_cloud.empty() ||
                distance_field.distance_m.empty() ||
                distance_field.width <= 0 || distance_field.height <= 0)
            {
                return quality;
            }

            const int sample_limit = std::max(1, max_sampled_points);
            const size_t sample_step = std::max<size_t>(
                1, (point_cloud.size() + sample_limit - 1) / sample_limit);
            int sampled_points = 0;
            int known_points = 0;
            int hit20_count = 0;
            double distance_sum = 0.0;
            auto flat_index =
                [width = distance_field.width](const int x, const int y)
            {
                return static_cast<size_t>(y) * static_cast<size_t>(width) +
                       static_cast<size_t>(x);
            };

            for (size_t point_index = 0; point_index < point_cloud.size();
                 point_index += sample_step)
            {
                const Eigen::Vector2d local_point(
                    point_cloud[point_index].position.x(),
                    point_cloud[point_index].position.y());
                const Eigen::Vector2d global_point =
                    global_pose * local_point;
                const int gx = static_cast<int>(
                    std::floor((global_point.x() -
                                distance_field.origin_x) /
                               distance_field.resolution));
                const int gy = static_cast<int>(
                    std::floor((global_point.y() -
                                distance_field.origin_y) /
                               distance_field.resolution));
                ++sampled_points;
                double distance =
                    kCurrentPoseScanMapSearchRadiusMeters;
                if (gx >= 0 && gx < distance_field.width &&
                    gy >= 0 && gy < distance_field.height)
                {
                    const size_t index = flat_index(gx, gy);
                    if (distance_field.known[index])
                    {
                        ++known_points;
                    }
                    distance = std::min<double>(
                        distance_field.distance_m[index],
                        kCurrentPoseScanMapSearchRadiusMeters);
                }
                if (distance <= kCurrentPoseScanMapHitRadiusMeters)
                {
                    ++hit20_count;
                }
                distance_sum += distance;
            }

            if (sampled_points <= 0)
            {
                return quality;
            }
            quality.hit20 = static_cast<double>(hit20_count) /
                            static_cast<double>(sampled_points);
            quality.mean_distance =
                distance_sum / static_cast<double>(sampled_points);
            quality.known_ratio = static_cast<double>(known_points) /
                                  static_cast<double>(sampled_points);
            quality.sampled_points = sampled_points;
            return quality;
        }

        void PoseGraph2D::UpdateLocalizationRecoveryFromMapScanQuality(
            const NodeId &node_id,
            const CurrentPoseScanMapQuality &quality)
        {
            const bool valid = quality.sampled_points > 0 &&
                               quality.checked_submaps > 0 &&
                               quality.hit20 >= 0.0 &&
                               quality.mean_distance >= 0.0;
            const bool bad =
                valid && (quality.hit20 < kMapScanBadHit20 ||
                          quality.mean_distance >
                              kMapScanBadMeanDistance);
            const bool severe_bad =
                valid && (quality.hit20 < kMapScanSevereBadHit20 ||
                          quality.mean_distance >
                              kMapScanSevereBadMeanDistance);
            RecordLocalizationHealthMapScanQuality(
                quality.hit20, quality.mean_distance, quality.known_ratio,
                quality.sampled_points, quality.checked_submaps, bad);

            LocalizationRecoveryRuntime &recovery =
                localization_recovery_[node_id.trajectory_id];
            if (valid)
            {
                recovery.latest_scan_map_hit20 = quality.hit20;
                recovery.latest_scan_map_mean_distance = quality.mean_distance;
                if (bad)
                {
                    ++recovery.consecutive_scan_map_bad_count;
                }
                else
                {
                    recovery.consecutive_scan_map_bad_count = 0;
                }
                if (severe_bad)
                {
                    ++recovery.consecutive_scan_map_severe_bad_count;
                }
                else
                {
                    recovery.consecutive_scan_map_severe_bad_count = 0;
                }
                recovery.scan_map_bad =
                    recovery.consecutive_scan_map_bad_count >=
                    kMapScanBadRequiredSamples;
                recovery.scan_map_severe_bad =
                    recovery.consecutive_scan_map_severe_bad_count >=
                    kMapScanSevereBadRequiredSamples;
            }
            if (valid && !bad &&
                recovery.recovery_reason ==
                    "active_frozen_accepted_waiting_scan_map")
            {
                recovery.consecutive_scan_map_bad_count = 0;
                recovery.consecutive_scan_map_severe_bad_count = 0;
                recovery.scan_map_bad = false;
                recovery.scan_map_severe_bad = false;
                recovery.ambiguous_reject_count_since_accept = 0;
                recovery.geometry_reject_count_since_accept = 0;
                recovery.consistency_reject_count_since_accept = 0;
                recovery.large_correction_consistency_reject_count_since_accept = 0;
                recovery.recovery_full_search_attempts_since_accept = 0;
                recovery.last_recovery_full_search_node_index = -1;
                recovery.auto_relocation_failures_since_accept = 0;
                recovery.auto_relocation_suppressed_until_node_index = -1;
                recovery.recovery_state = "OK";
                recovery.recovery_reason = "scan_map_recovered";
                active_frozen_consistency_windows_.erase(
                    node_id.trajectory_id);
                last_active_to_frozen_constraint_trajectory_id_ =
                    node_id.trajectory_id;
                last_active_to_frozen_constraint_node_index_ =
                    node_id.node_index;
                RecordLocalizationHealthRecoveryState(
                    recovery.recovery_state, recovery.recovery_reason);
                return;
            }

            const bool degraded =
                IsLocalizationDegraded(node_id, recovery);
            const int nodes_since_last_auto =
                (last_automatic_global_relocation_trajectory_id_ !=
                     node_id.trajectory_id ||
                 last_automatic_global_relocation_node_index_ < 0)
                    ? kAutoRelocationCooldownNodeGap
                    : node_id.node_index -
                          last_automatic_global_relocation_node_index_;
            const bool in_cooldown =
                nodes_since_last_auto < kAutoRelocationCooldownNodeGap;
            if (recovery.recovery_state == "AUTO_RELOCATING")
            {
                RecordLocalizationHealthRecoveryState(
                    recovery.recovery_state, recovery.recovery_reason);
                return;
            }
            if (in_cooldown && recovery.recovery_state == "COOLDOWN")
            {
                RecordLocalizationHealthRecoveryState(
                    recovery.recovery_state, recovery.recovery_reason);
                return;
            }
            if (recovery.auto_relocation_suppressed_until_node_index >=
                node_id.node_index)
            {
                recovery.recovery_state = "DEGRADED";
                recovery.recovery_reason = "auto_relocation_suppressed";
                RecordLocalizationHealthRecoveryState(
                    recovery.recovery_state, recovery.recovery_reason);
                return;
            }
            if (degraded)
            {
                recovery.recovery_state = "DEGRADED";
                recovery.recovery_reason =
                    recovery.scan_map_severe_bad
                        ? "map_scan_severe_bad"
                        : (recovery.scan_map_bad
                               ? "map_scan_bad"
                               : "active_frozen_constraint_gap");
                RecordLocalizationHealthRecoveryState(
                    recovery.recovery_state, recovery.recovery_reason);
            }
            else if (recovery.recovery_state != "OK")
            {
                recovery.recovery_state = "OK";
                recovery.recovery_reason.clear();
                RecordLocalizationHealthRecoveryState("OK", "");
            }
        }

        int PoseGraph2D::NodesSinceLastActiveFrozenConstraint(
            const NodeId &node_id) const
        {
            return (last_active_to_frozen_constraint_trajectory_id_ !=
                        node_id.trajectory_id ||
                    last_active_to_frozen_constraint_node_index_ < 0)
                       ? node_id.node_index + 1
                       : node_id.node_index -
                             last_active_to_frozen_constraint_node_index_;
        }

        bool PoseGraph2D::HasLocalizationRecoveryEvidence(
            const LocalizationRecoveryRuntime &recovery) const
        {
            return recovery.scan_map_bad || recovery.scan_map_severe_bad;
        }

        bool PoseGraph2D::IsLocalizationDegraded(
            const NodeId &node_id,
            const LocalizationRecoveryRuntime &recovery) const
        {
            return NodesSinceLastActiveFrozenConstraint(node_id) >=
                       kRecoveryNoConstraintNodeGap &&
                   HasLocalizationRecoveryEvidence(recovery);
        }

        bool PoseGraph2D::ShouldRunRecoveryFullSearch(
            const NodeId &node_id,
            const std::size_t queued_work_items_at_start,
            LocalizationRecoveryRuntime *const recovery,
            std::string *const reason)
        {
            CHECK(recovery != nullptr);
            CHECK(reason != nullptr);
            if (queued_work_items_at_start >=
                kRecoveryWorkQueueTriggerThreshold)
            {
                *reason = "work_queue_backlogged";
                return false;
            }
            if (recovery->recovery_state == "AUTO_RELOCATING" ||
                recovery->recovery_state == "COOLDOWN")
            {
                *reason = recovery->recovery_state;
                return false;
            }
            if (recovery->recovery_full_search_attempts_since_accept >=
                kRecoveryFullSearchMaxAttempts)
            {
                *reason = "recovery_search_exhausted";
                return false;
            }
            if (recovery->last_recovery_full_search_node_index >= 0 &&
                node_id.node_index -
                        recovery->last_recovery_full_search_node_index <
                    kRecoveryFullSearchNodeGap)
            {
                *reason = "waiting_recovery_interval";
                return false;
            }

            ++recovery->recovery_full_search_attempts_since_accept;
            recovery->last_recovery_full_search_node_index =
                node_id.node_index;
            recovery->recovery_state = "RECOVERY_SEARCH";
            if (recovery->scan_map_bad)
            {
                *reason = recovery->scan_map_severe_bad
                              ? "map_scan_severe_bad"
                              : "map_scan_bad";
            }
            else
            {
                *reason = "active_frozen_constraint_gap";
            }
            recovery->recovery_reason = *reason;
            RecordLocalizationHealthRecoveryState("RECOVERY_SEARCH", *reason);
            return true;
        }

        std::vector<SubmapId>
        PoseGraph2D::SelectActiveFrozenSubmapsForSearch(
            const std::vector<std::pair<double, SubmapId>> &candidates,
            const bool recovery_full_search,
            const std::size_t queued_work_items_at_start) const
        {
            std::vector<SubmapId> selected;
            selected.reserve(candidates.size());
            for (const auto &candidate : candidates)
            {
                selected.push_back(candidate.second);
            }
            return selected;
        }
        // 获取该 node 和该 submap 中的 node 中较新的时间
        common::Time PoseGraph2D::GetLatestNodeTime(const NodeId &node_id,
                                                    const SubmapId &submap_id) const
        {
            common::Time time = data_.trajectory_nodes.at(node_id).constant_data->time;
            const InternalSubmapData &submap_data = data_.submap_data.at(submap_id);
            if (!submap_data.node_ids.empty())
            {
                const NodeId last_submap_node_id =
                    *data_.submap_data.at(submap_id).node_ids.rbegin();
                time = std::max(
                    time,
                    data_.trajectory_nodes.at(last_submap_node_id).constant_data->time);
            }
            return time;
        }
        // 根据新计算出的约束更新子图轨迹id与节点轨迹id的连接关系
        void PoseGraph2D::UpdateTrajectoryConnectivity(const Constraint &constraint)
        {
            CHECK_EQ(constraint.tag, Constraint::INTER_SUBMAP);
            const common::Time time =
                GetLatestNodeTime(constraint.node_id, constraint.submap_id);
            data_.trajectory_connectivity_state.Connect(
                constraint.node_id.trajectory_id, constraint.submap_id.trajectory_id,
                time);
        }

        bool PoseGraph2D::IsTrajectoryActive(const int trajectory_id) const
        {
            const auto it = data_.trajectories_state.find(trajectory_id);
            return it != data_.trajectories_state.end() &&
                   it->second.state == TrajectoryState::ACTIVE;
        }

        bool PoseGraph2D::HasFrozenTrajectoryForLocalization() const
        {
            for (const auto &trajectory_state : data_.trajectories_state)
            {
                const int trajectory_id = trajectory_state.first;
                if (trajectory_state.second.state == TrajectoryState::FROZEN &&
                    data_.trajectory_nodes.SizeOfTrajectoryOrZero(trajectory_id) > 0 &&
                    data_.submap_data.SizeOfTrajectoryOrZero(trajectory_id) > 0)
                {
                    return true;
                }
            }
            return false;
        }

        bool PoseGraph2D::HasRecentActiveFrozenConnection(
            const int active_trajectory_id, const int active_node_index) const
        {
            if (last_active_to_frozen_constraint_trajectory_id_ !=
                    active_trajectory_id ||
                last_active_to_frozen_constraint_node_index_ < 0 ||
                active_node_index < last_active_to_frozen_constraint_node_index_)
            {
                return false;
            }
            return active_node_index - last_active_to_frozen_constraint_node_index_ <=
                   kStableActiveFrozenConnectionNodeGap;
        }

        bool PoseGraph2D::IsActiveNodeToFrozenSubmapConstraint(
            const Constraint &constraint) const
        {
            return constraint.tag == Constraint::INTER_SUBMAP &&
                   constraint.node_id.trajectory_id !=
                       constraint.submap_id.trajectory_id &&
                   IsTrajectoryActive(constraint.node_id.trajectory_id) &&
                   IsTrajectoryFrozen(constraint.submap_id.trajectory_id);
        }

        bool PoseGraph2D::IsActiveFrozenConstraint(
            const Constraint &constraint) const
        {
            if (constraint.tag != Constraint::INTER_SUBMAP ||
                constraint.node_id.trajectory_id ==
                    constraint.submap_id.trajectory_id)
            {
                return false;
            }
            const bool node_active =
                IsTrajectoryActive(constraint.node_id.trajectory_id);
            const bool submap_active =
                IsTrajectoryActive(constraint.submap_id.trajectory_id);
            const bool node_frozen =
                IsTrajectoryFrozen(constraint.node_id.trajectory_id);
            const bool submap_frozen =
                IsTrajectoryFrozen(constraint.submap_id.trajectory_id);
            return (node_active && submap_frozen) ||
                   (submap_active && node_frozen);
        }

        bool PoseGraph2D::ComputeActiveFrozenImpliedCorrection(
            const Constraint &constraint,
            ActiveFrozenImpliedCorrection *const correction) const
        {
            CHECK(correction != nullptr);
            if (!IsActiveFrozenConstraint(constraint))
            {
                return false;
            }
            if (!optimization_problem_->node_data().Contains(constraint.node_id) ||
                !optimization_problem_->submap_data().Contains(constraint.submap_id))
            {
                return false;
            }

            int active_trajectory_id = -1;
            transform::Rigid2d proposed_local_to_global =
                transform::Rigid2d::Identity();
            const transform::Rigid2d constraint_transform =
                transform::Project2D(constraint.pose.zbar_ij);

            if (IsTrajectoryActive(constraint.node_id.trajectory_id) &&
                IsTrajectoryFrozen(constraint.submap_id.trajectory_id))
            {
                active_trajectory_id = constraint.node_id.trajectory_id;
                const transform::Rigid2d proposed_node_global =
                    optimization_problem_->submap_data()
                        .at(constraint.submap_id)
                        .global_pose *
                    constraint_transform;
                proposed_local_to_global =
                    proposed_node_global *
                    optimization_problem_->node_data()
                        .at(constraint.node_id)
                        .local_pose_2d.inverse();
            }
            else if (IsTrajectoryActive(constraint.submap_id.trajectory_id) &&
                     IsTrajectoryFrozen(constraint.node_id.trajectory_id))
            {
                if (!data_.submap_data.Contains(constraint.submap_id) ||
                    data_.submap_data.at(constraint.submap_id).submap == nullptr)
                {
                    return true;
                }
                active_trajectory_id = constraint.submap_id.trajectory_id;
                const transform::Rigid2d proposed_submap_global =
                    optimization_problem_->node_data()
                        .at(constraint.node_id)
                        .global_pose_2d *
                    constraint_transform.inverse();
                proposed_local_to_global =
                    proposed_submap_global *
                    transform::Project2D(data_.submap_data.at(constraint.submap_id)
                                             .submap->local_pose())
                        .inverse();
            }
            else
            {
                return false;
            }

            const transform::Rigid2d current_local_to_global =
                transform::Project2D(ComputeLocalToGlobalTransform(
                    data_.global_submap_poses_2d, active_trajectory_id));
            const transform::Rigid2d delta =
                proposed_local_to_global * current_local_to_global.inverse();
            const double translation_delta = delta.translation().norm();
            const double rotation_delta =
                std::abs(common::NormalizeAngleDifference(
                    delta.rotation().angle()));
            correction->active_trajectory_id = active_trajectory_id;
            correction->delta = delta;
            correction->translation_m = translation_delta;
            correction->yaw_rad = rotation_delta;
            return true;
        }

        bool PoseGraph2D::PassesActiveFrozenConsistencyGate(
            const NodeId &node_id,
            const ActiveFrozenImpliedCorrection &correction,
            const std::string &gate_reason,
            const bool count_as_ambiguous_reject)
        {
            std::deque<ActiveFrozenCorrectionObservation> &window =
                active_frozen_consistency_windows_[correction.active_trajectory_id];
            window.erase(
                std::remove_if(window.begin(), window.end(),
                               [&node_id](const ActiveFrozenCorrectionObservation
                                              &observation)
                               {
                                   return observation.node_id == node_id;
                               }),
                window.end());
            window.push_back({node_id, correction});
            while (static_cast<int>(window.size()) > kConsistencyWindowSize)
            {
                window.pop_front();
            }

            int consistent_hits = 0;
            for (const ActiveFrozenCorrectionObservation &observation : window)
            {
                const transform::Rigid2d delta_between =
                    observation.correction.delta * correction.delta.inverse();
                const double translation =
                    delta_between.translation().norm();
                const double rotation =
                    std::abs(common::NormalizeAngleDifference(
                        delta_between.rotation().angle()));
                if (translation <= kConsistencyTranslationToleranceMeters &&
                    rotation <= kConsistencyRotationToleranceRadians)
                {
                    ++consistent_hits;
                }
            }
            if (consistent_hits >= kConsistencyRequiredHits)
            {
                RecordLocalizationHealthActiveFrozenConsistencyAccept();
                return true;
            }
            RecordLocalizationHealthActiveFrozenConsistencyReject();
            LocalizationRecoveryRuntime &recovery =
                localization_recovery_[correction.active_trajectory_id];
            ++recovery.consistency_reject_count_since_accept;
            if (count_as_ambiguous_reject)
            {
                RecordLocalizationHealthActiveFrozenAmbiguousReject();
                ++recovery.ambiguous_reject_count_since_accept;
            }
            if (correction.translation_m >= kLargeCorrectionTranslationMeters ||
                correction.yaw_rad >= kLargeCorrectionRotationRadians)
            {
                ++recovery.large_correction_consistency_reject_count_since_accept;
            }
            LOG(WARNING) << "[ActiveFrozenQualityGate]Reject "
                         << gate_reason
                         << " waiting for consistency node=" << node_id
                         << " active_trajectory="
                         << correction.active_trajectory_id
                         << " consistent_hits=" << consistent_hits
                         << "/" << kConsistencyRequiredHits
                         << " implied_translation_m="
                         << correction.translation_m
                         << " implied_yaw_rad=" << correction.yaw_rad;
            return false;
        }

        std::vector<PoseGraph2D::Constraint>
        PoseGraph2D::FilterConstraintsByQuality(
            const constraints::ConstraintBuilder2D::Result &result)
        {
            struct QualityCandidate
            {
                const constraints::ConstraintBuilder2D::ConstraintCandidate *candidate =
                    nullptr;
                bool active_frozen = false;
                bool same_submap_ambiguous = false;
                bool cross_submap_ambiguous = false;
                ActiveFrozenImpliedCorrection correction;
            };

            auto pose_difference_large =
                [](const transform::Rigid2d &lhs,
                   const transform::Rigid2d &rhs,
                   const double translation_threshold,
                   const double rotation_threshold)
            {
                const transform::Rigid2d delta = lhs.inverse() * rhs;
                return delta.translation().norm() >= translation_threshold ||
                       std::abs(common::NormalizeAngleDifference(
                           delta.rotation().angle())) >= rotation_threshold;
            };
            auto compute_candidate_global_pose =
                [this](const Constraint &constraint,
                       transform::Rigid2d *const global_pose)
            {
                CHECK(global_pose != nullptr);
                if (!IsActiveNodeToFrozenSubmapConstraint(constraint) ||
                    !optimization_problem_->submap_data().Contains(
                        constraint.submap_id))
                {
                    return false;
                }
                const transform::Rigid2d constraint_transform =
                    transform::Project2D(constraint.pose.zbar_ij);
                *global_pose =
                    optimization_problem_->submap_data()
                        .at(constraint.submap_id)
                        .global_pose *
                    constraint_transform;
                return true;
            };

            std::vector<QualityCandidate> candidates;
            candidates.reserve(result.size());
            for (const auto &candidate : result)
            {
                QualityCandidate quality_candidate;
                quality_candidate.candidate = &candidate;
                quality_candidate.active_frozen =
                    IsActiveNodeToFrozenSubmapConstraint(candidate.constraint) &&
                    ComputeActiveFrozenImpliedCorrection(
                        candidate.constraint, &quality_candidate.correction);
                if (quality_candidate.active_frozen &&
                    candidate.top_candidates.size() >= 2)
                {
                    const auto &top1 = candidate.top_candidates[0];
                    const auto &top2 = candidate.top_candidates[1];
                    if (top1.score - top2.score <
                            kSameSubmapAmbiguousScoreMargin &&
                        pose_difference_large(
                            top1.pose, top2.pose,
                            kSameSubmapAmbiguousTranslationMeters,
                            kSameSubmapAmbiguousRotationRadians))
                    {
                        quality_candidate.same_submap_ambiguous = true;
                    }
                }
                candidates.push_back(quality_candidate);
            }

            for (size_t i = 0; i < candidates.size(); ++i)
            {
                if (!candidates[i].active_frozen)
                {
                    continue;
                }
                for (size_t j = i + 1; j < candidates.size(); ++j)
                {
                    if (!candidates[j].active_frozen ||
                        candidates[i].candidate->constraint.node_id !=
                            candidates[j].candidate->constraint.node_id)
                    {
                        continue;
                    }
                    if (std::abs(candidates[i].candidate->fast_score -
                                 candidates[j].candidate->fast_score) >=
                        kCrossSubmapAmbiguousScoreMargin)
                    {
                        continue;
                    }
                    const transform::Rigid2d delta_between =
                        candidates[i].correction.delta *
                        candidates[j].correction.delta.inverse();
                    const double translation =
                        delta_between.translation().norm();
                    const double rotation =
                        std::abs(common::NormalizeAngleDifference(
                            delta_between.rotation().angle()));
                    if (translation >= kCrossSubmapAmbiguousTranslationMeters ||
                        rotation >= kCrossSubmapAmbiguousRotationRadians)
                    {
                        candidates[i].cross_submap_ambiguous = true;
                        candidates[j].cross_submap_ambiguous = true;
                    }
                }
            }

            std::vector<Constraint> filtered_result;
            filtered_result.reserve(result.size());
            for (QualityCandidate &quality_candidate : candidates)
            {
                const auto &candidate = *quality_candidate.candidate;
                if (!quality_candidate.active_frozen)
                {
                    filtered_result.push_back(candidate.constraint);
                    continue;
                }

                const double top1_score =
                    candidate.top_candidates.empty()
                        ? candidate.fast_score
                        : candidate.top_candidates[0].score;
                const double top2_score =
                    candidate.top_candidates.size() >= 2
                        ? candidate.top_candidates[1].score
                        : 0.0;
                const double margin =
                    candidate.top_candidates.size() >= 2
                        ? top1_score - top2_score
                        : 1.0;
                RecordLocalizationHealthActiveFrozenCandidate(
                    top1_score, top2_score, margin,
                    candidate.geometry_quality.hit20,
                    candidate.geometry_quality.mean_distance,
                    candidate.geometry_quality.free_space_conflict_ratio,
                    candidate.geometry_quality.known_ratio,
                    candidate.geometry_quality.sector_coverage,
                    quality_candidate.correction.translation_m,
                    quality_candidate.correction.yaw_rad);

                if (candidate.top_candidates.empty() &&
                    candidate.geometry_quality.hit20 < 0.0)
                {
                    filtered_result.push_back(candidate.constraint);
                    continue;
                }

                const bool ambiguous =
                    quality_candidate.same_submap_ambiguous ||
                    quality_candidate.cross_submap_ambiguous;
                const bool large_correction =
                    quality_candidate.correction.translation_m >=
                        kLargeCorrectionTranslationMeters ||
                    quality_candidate.correction.yaw_rad >=
                        kLargeCorrectionRotationRadians;
                const bool beyond_soft_correction_gate =
                    quality_candidate.correction.translation_m >
                        kSoftActiveFrozenConstraintTranslationMeters ||
                    quality_candidate.correction.yaw_rad >
                        kSoftActiveFrozenConstraintRotationRadians;
                const bool too_large_for_recovery_constraint =
                    quality_candidate.correction.translation_m >
                        kHardActiveFrozenConstraintTranslationMeters ||
                    quality_candidate.correction.yaw_rad >
                        kHardActiveFrozenConstraintRotationRadians;
                const bool suspicious =
                    large_correction || candidate.match_full_submap ||
                    margin < 0.05 || ambiguous;
                LocalizationRecoveryRuntime &recovery =
                    localization_recovery_[candidate.constraint.node_id
                                               .trajectory_id];
                if (suspicious && candidate.geometry_quality.hit20 >= 0.0)
                {
                    const double min_hit20 =
                        large_correction
                            ? kRejectOnlyLargeGeometryMinHit20
                            : kRejectOnlyNormalGeometryMinHit20;
                    const double max_mean_distance =
                        large_correction
                            ? kRejectOnlyLargeGeometryMaxMeanDistance
                            : kRejectOnlyNormalGeometryMaxMeanDistance;
                    const double max_free_space_conflict_ratio =
                        large_correction
                            ? kRejectOnlyLargeGeometryMaxFreeSpaceConflictRatio
                            : kRejectOnlyNormalGeometryMaxFreeSpaceConflictRatio;
                    const bool free_space_conflict_bad =
                        candidate.geometry_quality
                                .free_space_conflict_ratio >= 0.0 &&
                        candidate.geometry_quality
                                .free_space_conflict_ratio >
                            max_free_space_conflict_ratio;
                    if (candidate.geometry_quality.hit20 < min_hit20 ||
                        candidate.geometry_quality.mean_distance >
                            max_mean_distance ||
                        free_space_conflict_bad)
                    {
                        RecordLocalizationHealthActiveFrozenGeometryReject();
                        ++recovery.geometry_reject_count_since_accept;
                        LOG(WARNING)
                            << "[ActiveFrozenQualityGate]Reject geometry "
                            << "constraint node="
                            << candidate.constraint.node_id
                            << " submap="
                            << candidate.constraint.submap_id
                            << " hit20="
                            << candidate.geometry_quality.hit20
                            << " mean_distance="
                            << candidate.geometry_quality.mean_distance
                            << " free_space_conflict_ratio="
                            << candidate.geometry_quality
                                   .free_space_conflict_ratio
                            << " max_free_space_conflict_ratio="
                            << max_free_space_conflict_ratio
                            << " known_ratio="
                            << candidate.geometry_quality.known_ratio
                            << " sector_coverage="
                            << candidate.geometry_quality.sector_coverage
                            << " large_correction=" << large_correction;
                        continue;
                    }
                }

                if (too_large_for_recovery_constraint)
                {
                    RecordLocalizationHealthActiveFrozenConsistencyReject();
                    LocalizationRecoveryRuntime &recovery =
                        localization_recovery_[candidate.constraint.node_id
                                                   .trajectory_id];
                    ++recovery.consistency_reject_count_since_accept;
                    ++recovery
                          .large_correction_consistency_reject_count_since_accept;
                    LOG(WARNING)
                        << "[ActiveFrozenQualityGate]Reject hard excessive "
                        << "correction before backend constraint node="
                        << candidate.constraint.node_id
                        << " submap=" << candidate.constraint.submap_id
                        << " score=" << candidate.fast_score
                        << " implied_translation_m="
                        << quality_candidate.correction.translation_m
                        << " hard_max_translation_m="
                        << kHardActiveFrozenConstraintTranslationMeters
                        << " implied_yaw_rad="
                        << quality_candidate.correction.yaw_rad
                        << " hard_max_yaw_rad="
                        << kHardActiveFrozenConstraintRotationRadians;
                    continue;
                }

                const bool recovery_path =
                    recovery.recovery_state != "OK" ||
                    recovery.scan_map_bad ||
                    recovery.scan_map_severe_bad ||
                    recovery.recovery_full_search_attempts_since_accept > 0;
                const bool needs_candidate_full_map_gate =
                    recovery_path || beyond_soft_correction_gate;
                if (needs_candidate_full_map_gate)
                {
                    bool reject_candidate_full_map = false;
                    std::string reject_reason;
                    CurrentPoseScanMapQuality candidate_full_map_quality;
                    double candidate_hit20_improvement = -1.0;
                    double candidate_mean_distance_improvement = -1.0;
                    transform::Rigid2d candidate_global_pose =
                        transform::Rigid2d::Identity();
                    const bool current_map_quality_valid =
                        recovery.latest_scan_map_hit20 >= 0.0 &&
                        recovery.latest_scan_map_mean_distance >= 0.0;
                    const bool current_map_already_ok =
                        current_map_quality_valid && !recovery.scan_map_bad &&
                        !recovery.scan_map_severe_bad &&
                        recovery.latest_scan_map_hit20 >=
                            kMapScanBadHit20 &&
                        recovery.latest_scan_map_mean_distance <=
                            kMapScanBadMeanDistance;
                    if (!data_.trajectory_nodes.Contains(
                            candidate.constraint.node_id) ||
                        data_.trajectory_nodes
                                .at(candidate.constraint.node_id)
                                .constant_data == nullptr ||
                        !compute_candidate_global_pose(
                            candidate.constraint, &candidate_global_pose))
                    {
                        reject_candidate_full_map = true;
                        reject_reason = "missing_candidate_pose";
                    }
                    else
                    {
                        const std::shared_ptr<const MapScanDistanceField>
                            distance_field =
                                GetMapScanDistanceFieldIfReadyOrStartAsync();
                        if (distance_field == nullptr ||
                            !distance_field->valid)
                        {
                            reject_candidate_full_map = true;
                            reject_reason = "map_distance_field_not_ready";
                        }
                        else
                        {
                            candidate_full_map_quality =
                                ComputeMapScanQuality(
                                    *data_.trajectory_nodes
                                         .at(candidate.constraint.node_id)
                                         .constant_data,
                                    candidate_global_pose, *distance_field,
                                    kMapScanHealthRecoveryMaxSampledPoints);
                        }
                        const bool valid =
                            candidate_full_map_quality.sampled_points > 0 &&
                            candidate_full_map_quality.checked_submaps > 0 &&
                            candidate_full_map_quality.hit20 >= 0.0 &&
                            candidate_full_map_quality.mean_distance >= 0.0;
                        if (valid && current_map_quality_valid)
                        {
                            candidate_hit20_improvement =
                                candidate_full_map_quality.hit20 -
                                recovery.latest_scan_map_hit20;
                            candidate_mean_distance_improvement =
                                recovery.latest_scan_map_mean_distance -
                                candidate_full_map_quality.mean_distance;
                        }
                        RecordLocalizationHealthActiveFrozenCandidateFullMap(
                            candidate_full_map_quality.hit20,
                            candidate_full_map_quality.mean_distance,
                            candidate_full_map_quality.known_ratio,
                            candidate_hit20_improvement,
                            candidate_mean_distance_improvement);
                        const bool known_ratio_bad =
                            candidate_full_map_quality.known_ratio >= 0.0 &&
                            candidate_full_map_quality.known_ratio <
                                kCandidateFullMapMinKnownRatio;
                        if (!reject_candidate_full_map && !valid)
                        {
                            reject_candidate_full_map = true;
                            reject_reason = "invalid_candidate_full_map";
                        }
                        else if (!reject_candidate_full_map &&
                                 (candidate_full_map_quality.hit20 <
                                     kCandidateFullMapMinHit20 ||
                                 candidate_full_map_quality.mean_distance >
                                     kCandidateFullMapMaxMeanDistance ||
                                  known_ratio_bad))
                        {
                            reject_candidate_full_map = true;
                            reject_reason = "candidate_full_map_bad";
                        }
                        else if (!reject_candidate_full_map &&
                                 beyond_soft_correction_gate &&
                                 !current_map_quality_valid)
                        {
                            reject_candidate_full_map = true;
                            reject_reason = "current_full_map_unavailable";
                        }
                        else if (!reject_candidate_full_map &&
                                 beyond_soft_correction_gate &&
                                 current_map_already_ok)
                        {
                            reject_candidate_full_map = true;
                            reject_reason = "current_full_map_not_bad";
                        }
                        else if (!reject_candidate_full_map &&
                                 current_map_quality_valid &&
                                 (candidate_hit20_improvement <
                                      kCandidateFullMapMinHit20Improvement ||
                                  candidate_mean_distance_improvement <
                                      kCandidateFullMapMinDistanceImprovement))
                        {
                            reject_candidate_full_map = true;
                            reject_reason =
                                "candidate_full_map_not_improved";
                        }
                    }
                    if (reject_candidate_full_map)
                    {
                        RecordLocalizationHealthActiveFrozenFullMapReject();
                        LOG(WARNING)
                            << "[ActiveFrozenQualityGate]Reject candidate "
                            << "full-map consistency node="
                            << candidate.constraint.node_id
                            << " submap=" << candidate.constraint.submap_id
                            << " reason=" << reject_reason
                            << " recovery_path=" << recovery_path
                            << " beyond_soft_correction_gate="
                            << beyond_soft_correction_gate
                            << " score=" << candidate.fast_score
                            << " candidate_map_hit20="
                            << candidate_full_map_quality.hit20
                            << " candidate_map_mean_distance="
                            << candidate_full_map_quality.mean_distance
                            << " candidate_map_known_ratio="
                            << candidate_full_map_quality.known_ratio
                            << " candidate_map_sampled_points="
                            << candidate_full_map_quality.sampled_points
                            << " candidate_map_checked_submaps="
                            << candidate_full_map_quality.checked_submaps
                            << " candidate_hit20_improvement="
                            << candidate_hit20_improvement
                            << " candidate_mean_distance_improvement="
                            << candidate_mean_distance_improvement
                            << " latest_hit20="
                            << recovery.latest_scan_map_hit20
                            << " latest_mean_distance="
                            << recovery.latest_scan_map_mean_distance
                            << " implied_translation_m="
                            << quality_candidate.correction.translation_m
                            << " implied_yaw_rad="
                            << quality_candidate.correction.yaw_rad;
                        continue;
                    }
                }

                if (ambiguous &&
                    !PassesActiveFrozenConsistencyGate(
                        candidate.constraint.node_id,
                        quality_candidate.correction, "ambiguous constraint",
                        /*count_as_ambiguous_reject=*/true))
                {
                    LOG(WARNING) << "[ActiveFrozenQualityGate]Hold ambiguous "
                                 << "constraint node="
                                 << candidate.constraint.node_id
                                 << " submap="
                                 << candidate.constraint.submap_id
                                 << " score=" << candidate.fast_score
                                 << " margin=" << margin
                                 << " same_submap="
                                 << quality_candidate.same_submap_ambiguous
                                 << " cross_submap="
                                 << quality_candidate.cross_submap_ambiguous;
                    continue;
                }
                if (ambiguous)
                {
                    LOG(WARNING) << "[ActiveFrozenQualityGate]Accept "
                                 << "ambiguous constraint after consistency "
                                 << "node=" << candidate.constraint.node_id
                                 << " submap="
                                 << candidate.constraint.submap_id
                                 << " score=" << candidate.fast_score
                                 << " margin=" << margin
                                 << " same_submap="
                                 << quality_candidate.same_submap_ambiguous
                                 << " cross_submap="
                                 << quality_candidate.cross_submap_ambiguous;
                }

                if (large_correction &&
                    !ambiguous &&
                    !PassesActiveFrozenConsistencyGate(
                        candidate.constraint.node_id,
                        quality_candidate.correction,
                        beyond_soft_correction_gate
                            ? "soft excessive correction"
                            : "large correction",
                        /*count_as_ambiguous_reject=*/false))
                {
                    continue;
                }
                if (beyond_soft_correction_gate && large_correction)
                {
                    LOG(WARNING)
                        << "[ActiveFrozenQualityGate]Accept soft excessive "
                        << "correction after consistency node="
                        << candidate.constraint.node_id
                        << " submap=" << candidate.constraint.submap_id
                        << " score=" << candidate.fast_score
                        << " implied_translation_m="
                        << quality_candidate.correction.translation_m
                        << " soft_translation_m="
                        << kSoftActiveFrozenConstraintTranslationMeters
                        << " hard_translation_m="
                        << kHardActiveFrozenConstraintTranslationMeters
                        << " implied_yaw_rad="
                        << quality_candidate.correction.yaw_rad;
                }

                filtered_result.push_back(candidate.constraint);
            }
            return filtered_result;
        }

        bool PoseGraph2D::ShouldRunAutomaticGlobalRelocation(
            const NodeId &node_id, const std::size_t queued_work_items,
            std::string *const trigger_reason)
        {
            CHECK(trigger_reason != nullptr);
            if (!flirt::use_flirt.load())
            {
                return false;
            }
            if (!IsTrajectoryActive(node_id.trajectory_id))
            {
                return false;
            }
            if (!HasFrozenTrajectoryForLocalization())
            {
                return false;
            }
            if (flirt::need_flirt.load() || flirt::flirt_working.load())
            {
                return false;
            }
            if (queued_work_items >= kRecoveryWorkQueueTriggerThreshold)
            {
                LocalizationRecoveryRuntime &recovery =
                    localization_recovery_[node_id.trajectory_id];
                if (IsLocalizationDegraded(node_id, recovery))
                {
                    recovery.recovery_state = "DEGRADED";
                    recovery.recovery_reason = "work_queue_backlogged";
                    RecordLocalizationHealthRecoveryState(
                        recovery.recovery_state, recovery.recovery_reason);
                }
                return false;
            }

            const int nodes_since_cross_constraint =
                NodesSinceLastActiveFrozenConstraint(node_id);
            LocalizationRecoveryRuntime &recovery =
                localization_recovery_[node_id.trajectory_id];
            if (recovery.auto_relocation_suppressed_until_node_index >=
                node_id.node_index)
            {
                recovery.recovery_state = "DEGRADED";
                recovery.recovery_reason = "auto_relocation_suppressed";
                RecordLocalizationHealthRecoveryState(
                    recovery.recovery_state, recovery.recovery_reason);
                return false;
            }
            if (recovery.auto_relocation_suppressed_until_node_index >= 0)
            {
                recovery.auto_relocation_suppressed_until_node_index = -1;
                recovery.auto_relocation_failures_since_accept = 0;
            }
            if (recovery.auto_relocation_failures_since_accept >=
                kAutoRelocationMaxFailuresPerEpisode)
            {
                recovery.auto_relocation_suppressed_until_node_index =
                    node_id.node_index + kAutoRelocationSuppressionNodeGap;
                recovery.recovery_state = "DEGRADED";
                recovery.recovery_reason = "relocation_exhausted";
                RecordLocalizationHealthRecoveryState(
                    recovery.recovery_state, recovery.recovery_reason);
                return false;
            }

            const bool recovery_failed_trigger =
                recovery.recovery_full_search_attempts_since_accept >=
                kRecoveryFullSearchMaxAttempts;
            const bool confirmed_lost_trigger =
                nodes_since_cross_constraint >= kRecoveryConfirmedLostNodeGap &&
                recovery.scan_map_severe_bad;
            if (!recovery_failed_trigger || !confirmed_lost_trigger)
            {
                return false;
            }

            const int nodes_since_last_auto_relocation =
                (last_automatic_global_relocation_trajectory_id_ !=
                     node_id.trajectory_id ||
                 last_automatic_global_relocation_node_index_ < 0)
                    ? kAutoRelocationCooldownNodeGap
                    : node_id.node_index -
                          last_automatic_global_relocation_node_index_;
            if (nodes_since_last_auto_relocation <
                kAutoRelocationCooldownNodeGap)
            {
                return false;
            }
            *trigger_reason = "confirmed_lost_recovery_failed";
            recovery.recovery_state = "AUTO_RELOCATING";
            recovery.recovery_reason = *trigger_reason;
            RecordLocalizationHealthRecoveryState(recovery.recovery_state,
                                                  recovery.recovery_reason);
            return true;
        }

        void PoseGraph2D::UpdateAutomaticGlobalRelocationState(
            const Constraint &constraint)
        {
            if (!IsActiveNodeToFrozenSubmapConstraint(constraint))
            {
                return;
            }
            RecordLocalizationHealthActiveFrozenAcceptedConstraint();
            LocalizationRecoveryRuntime &recovery =
                localization_recovery_[constraint.node_id.trajectory_id];
            if (recovery.scan_map_bad || recovery.scan_map_severe_bad)
            {
                recovery.recovery_state = "DEGRADED";
                recovery.recovery_reason =
                    "active_frozen_accepted_waiting_scan_map";
                RecordLocalizationHealthRecoveryState(
                    recovery.recovery_state, recovery.recovery_reason);
                LOG(WARNING)
                    << "[LocalizationRecovery]Active-frozen constraint "
                    << "accepted but scan-map health is still bad; keep "
                    << "recovery degraded until current pose health recovers. "
                    << "node=" << constraint.node_id
                    << " submap=" << constraint.submap_id
                    << " latest_hit20=" << recovery.latest_scan_map_hit20
                    << " latest_mean_distance="
                    << recovery.latest_scan_map_mean_distance;
                return;
            }
            recovery.consecutive_scan_map_bad_count = 0;
            recovery.consecutive_scan_map_severe_bad_count = 0;
            recovery.scan_map_bad = false;
            recovery.scan_map_severe_bad = false;
            recovery.ambiguous_reject_count_since_accept = 0;
            recovery.geometry_reject_count_since_accept = 0;
            recovery.consistency_reject_count_since_accept = 0;
            recovery.large_correction_consistency_reject_count_since_accept = 0;
            recovery.recovery_full_search_attempts_since_accept = 0;
            recovery.last_recovery_full_search_node_index = -1;
            recovery.auto_relocation_failures_since_accept = 0;
            recovery.auto_relocation_suppressed_until_node_index = -1;
            recovery.recovery_state = "OK";
            recovery.recovery_reason = "active_frozen_accepted";
            active_frozen_consistency_windows_.erase(
                constraint.node_id.trajectory_id);
            RecordLocalizationHealthRecoveryState("OK", "active_frozen_accepted");

            if (constraint.node_id.trajectory_id !=
                    last_active_to_frozen_constraint_trajectory_id_ ||
                constraint.node_id.node_index >
                    last_active_to_frozen_constraint_node_index_)
            {
                last_active_to_frozen_constraint_trajectory_id_ =
                    constraint.node_id.trajectory_id;
                last_active_to_frozen_constraint_node_index_ =
                    constraint.node_id.node_index;
                LOG(WARNING) << "[AutoRelocation]Cross constraint node="
                             << constraint.node_id
                             << " submap=" << constraint.submap_id
                             << " reset no-cross counter.";
            }
        }
        // 根据轨迹状态删除轨迹
        void PoseGraph2D::DeleteTrajectoriesIfNeeded()
        {
            TrimmingHandle trimming_handle(this);
            for (auto &it : data_.trajectories_state)
            {
                if (it.second.deletion_state ==
                    InternalTrajectoryState::DeletionState::WAIT_FOR_DELETION)
                {
                    // TODO(gaschler): Consider directly deleting from data_, which may be
                    // more complete.
                    auto submap_ids = trimming_handle.GetSubmapIds(it.first);
                    for (auto &submap_id : submap_ids)
                    {
                        trimming_handle.TrimSubmap(submap_id);
                    }
                    it.second.state = TrajectoryState::DELETED;
                    it.second.deletion_state = InternalTrajectoryState::DeletionState::NORMAL;
                }
            }
        }
        // 将计算完的约束结果进行保存, 并执行优化
        void PoseGraph2D::HandleWorkQueue(
            const constraints::ConstraintBuilder2D::Result &result)
        {
            std::vector<Constraint> filtered_result;
            {
                absl::MutexLock locker(&mutex_);
                filtered_result = FilterConstraintsByQuality(result);
                data_.constraints.insert(data_.constraints.end(),
                                         filtered_result.begin(),
                                         filtered_result.end());
            }
            RunOptimization();

            if (global_slam_optimization_callback_)
            {
                std::map<int, NodeId> trajectory_id_to_last_optimized_node_id;
                std::map<int, SubmapId> trajectory_id_to_last_optimized_submap_id;
                {
                    absl::MutexLock locker(&mutex_);
                    const auto &submap_data = optimization_problem_->submap_data();
                    const auto &node_data = optimization_problem_->node_data();
                    for (const int trajectory_id : node_data.trajectory_ids())
                    {
                        if (node_data.SizeOfTrajectoryOrZero(trajectory_id) == 0 ||
                            submap_data.SizeOfTrajectoryOrZero(trajectory_id) == 0)
                        {
                            continue;
                        }
                        trajectory_id_to_last_optimized_node_id.emplace(
                            trajectory_id,
                            std::prev(node_data.EndOfTrajectory(trajectory_id))->id);
                        trajectory_id_to_last_optimized_submap_id.emplace(
                            trajectory_id,
                            std::prev(submap_data.EndOfTrajectory(trajectory_id))->id);
                    }
                }
                global_slam_optimization_callback_(
                    trajectory_id_to_last_optimized_submap_id,
                    trajectory_id_to_last_optimized_node_id);
            }

            {
                absl::MutexLock locker(&mutex_);
                for (const Constraint &constraint : filtered_result)
                {
                    UpdateTrajectoryConnectivity(constraint);
                    UpdateAutomaticGlobalRelocationState(constraint);
                }
                DeleteTrajectoriesIfNeeded();
                TrimmingHandle trimming_handle(this);
                for (auto &trimmer : trimmers_)
                {
                    trimmer->Trim(&trimming_handle);
                }
                trimmers_.erase(
                    std::remove_if(trimmers_.begin(), trimmers_.end(),
                                   [](std::unique_ptr<PoseGraphTrimmer> &trimmer)
                                   {
                                       return trimmer->IsFinished();
                                   }),
                    trimmers_.end());

                num_nodes_since_last_loop_closure_ = 0;

                // Update the gauges that count the current number of constraints.
                double inter_constraints_same_trajectory = 0;
                double inter_constraints_different_trajectory = 0;
                for (const auto &constraint : data_.constraints)
                {
                    if (constraint.tag ==
                        cartographer::mapping::PoseGraph::Constraint::INTRA_SUBMAP)
                    {
                        continue;
                    }
                    if (constraint.node_id.trajectory_id ==
                        constraint.submap_id.trajectory_id)
                    {
                        ++inter_constraints_same_trajectory;
                    }
                    else
                    {
                        ++inter_constraints_different_trajectory;
                    }
                }
                kConstraintsSameTrajectoryMetric->Set(inter_constraints_same_trajectory);
                kConstraintsDifferentTrajectoryMetric->Set(
                    inter_constraints_different_trajectory);
            }

            DrainWorkQueue();
        }
        // 在调用线程上执行工作队列中的待处理任务, 直到队列为空或需要优化时退出循环
        void PoseGraph2D::DrainWorkQueue()
        {
            bool process_work_queue = true;
            size_t work_queue_size;
            while (process_work_queue)
            {
                std::function<WorkItem::Result()> work_item;
                {
                    absl::MutexLock locker(&work_queue_mutex_);
                    if (work_queue_->empty())
                    {
                        work_queue_.reset();
                        return;
                    }
                    work_item = work_queue_->front().task;
                    work_queue_->pop_front();
                    work_queue_size = work_queue_->size();
                    kWorkQueueSizeMetric->Set(work_queue_size);
                    RecordLocalizationHealthWorkQueueSize(work_queue_size);
                }
                process_work_queue = work_item() == WorkItem::Result::kDoNotRunOptimization;
            }
            LOG(WARNING) << "Remaining work items in queue: " << work_queue_size;
            // We have to optimize again.
            constraint_builder_.WhenDone(
                [this](const constraints::ConstraintBuilder2D::Result &result)
                {
                    HandleWorkQueue(result);
                });
        }

        void PoseGraph2D::WaitForAllComputations()
        {
            int num_trajectory_nodes;
            {
                absl::MutexLock locker(&mutex_);
                num_trajectory_nodes = data_.num_trajectory_nodes;
            }

            const int num_finished_nodes_at_start =
                constraint_builder_.GetNumFinishedNodes();

            auto report_progress = [this, num_trajectory_nodes,
                                    num_finished_nodes_at_start]()
            {
                // Log progress on nodes only when we are actually processing nodes.
                if (num_trajectory_nodes != num_finished_nodes_at_start)
                {
                    std::ostringstream progress_info;
                    progress_info << "Optimizing: " << std::fixed << std::setprecision(1)
                                  << 100. *
                                         (constraint_builder_.GetNumFinishedNodes() -
                                          num_finished_nodes_at_start) /
                                         (num_trajectory_nodes - num_finished_nodes_at_start)
                                  << "%...";
                    std::cout << "\r\x1b[K" << progress_info.str() << std::flush;
                }
            };

            // First wait for the work queue to drain so that it's safe to schedule
            // a WhenDone() callback.
            {
                const auto predicate = [this]()
                                           EXCLUSIVE_LOCKS_REQUIRED(work_queue_mutex_)
                {
                    return work_queue_ == nullptr;
                };
                absl::MutexLock locker(&work_queue_mutex_);
                while (!work_queue_mutex_.AwaitWithTimeout(
                    absl::Condition(&predicate),
                    absl::FromChrono(common::FromSeconds(1.))))
                {
                    report_progress();
                }
            }

            // Now wait for any pending constraint computations to finish.
            absl::MutexLock locker(&mutex_);
            bool notification = false;
            constraint_builder_.WhenDone(
                [this,
                 &notification](const constraints::ConstraintBuilder2D::Result &result)
                    LOCKS_EXCLUDED(mutex_)
                {
                    absl::MutexLock locker(&mutex_);
                    const std::vector<Constraint> filtered_result =
                        FilterConstraintsByQuality(result);
                    data_.constraints.insert(data_.constraints.end(),
                                             filtered_result.begin(),
                                             filtered_result.end());
                    notification = true;
                });
            const auto predicate = [&notification]() EXCLUSIVE_LOCKS_REQUIRED(mutex_)
            {
                return notification;
            };
            while (!mutex_.AwaitWithTimeout(absl::Condition(&predicate),
                                            absl::FromChrono(common::FromSeconds(1.))))
            {
                report_progress();
            }
            CHECK_EQ(constraint_builder_.GetNumFinishedNodes(), num_trajectory_nodes);
            std::cout << "\r\x1b[KOptimizing: Done.     " << std::endl;
        }

        void PoseGraph2D::DeleteTrajectory(const int trajectory_id)
        {
            {
                absl::MutexLock locker(&mutex_);
                auto it = data_.trajectories_state.find(trajectory_id);
                if (it == data_.trajectories_state.end())
                {
                    LOG(WARNING) << "Skipping request to delete non-existing trajectory_id: "
                                 << trajectory_id;
                    return;
                }
                it->second.deletion_state =
                    InternalTrajectoryState::DeletionState::SCHEDULED_FOR_DELETION;
            }
            AddWorkItem([this, trajectory_id]() LOCKS_EXCLUDED(mutex_)
                        {
    absl::MutexLock locker(&mutex_);
    CHECK(data_.trajectories_state.at(trajectory_id).state !=
          TrajectoryState::ACTIVE);
    CHECK(data_.trajectories_state.at(trajectory_id).state !=
          TrajectoryState::DELETED);
    CHECK(data_.trajectories_state.at(trajectory_id).deletion_state ==
          InternalTrajectoryState::DeletionState::SCHEDULED_FOR_DELETION);
    data_.trajectories_state.at(trajectory_id).deletion_state =
        InternalTrajectoryState::DeletionState::WAIT_FOR_DELETION;
    return WorkItem::Result::kDoNotRunOptimization; });
        }
        // 结束指定id的轨迹
        void PoseGraph2D::FinishTrajectory(const int trajectory_id)
        {
            AddWorkItem([this, trajectory_id]() LOCKS_EXCLUDED(mutex_)
                        {
    absl::MutexLock locker(&mutex_);
    CHECK(!IsTrajectoryFinished(trajectory_id));
    data_.trajectories_state[trajectory_id].state = TrajectoryState::FINISHED;

    for (const auto& submap : data_.submap_data.trajectory(trajectory_id)) {
      data_.submap_data.at(submap.id).state = SubmapState::kFinished;
    }
    return WorkItem::Result::kRunOptimization; });
        }
        // 将指定轨迹id设置为FROZEN状态
        bool PoseGraph2D::IsTrajectoryFinished(const int trajectory_id) const
        {
            return data_.trajectories_state.count(trajectory_id) != 0 &&
                   data_.trajectories_state.at(trajectory_id).state ==
                       TrajectoryState::FINISHED;
        }

        void PoseGraph2D::FreezeTrajectory(const int trajectory_id)
        {
            {
                absl::MutexLock locker(&mutex_);
                data_.trajectory_connectivity_state.Add(trajectory_id);
            }
            AddWorkItem([this, trajectory_id]() LOCKS_EXCLUDED(mutex_)
                        {
    absl::MutexLock locker(&mutex_);
    CHECK(!IsTrajectoryFrozen(trajectory_id));
    // Connect multiple frozen trajectories among each other.
    // This is required for localization against multiple frozen trajectories
    // because we lose inter-trajectory constraints when freezing.
    for (const auto& entry : data_.trajectories_state) {
      const int other_trajectory_id = entry.first;
      if (!IsTrajectoryFrozen(other_trajectory_id)) {
        continue;
      }
      if (data_.trajectory_connectivity_state.TransitivelyConnected(
              trajectory_id, other_trajectory_id)) {
        // Already connected, nothing to do.
        continue;
      }
      data_.trajectory_connectivity_state.Connect(
          trajectory_id, other_trajectory_id, common::FromUniversal(0));
    }
    data_.trajectories_state[trajectory_id].state = TrajectoryState::FROZEN;
    InvalidateMapScanDistanceField();
    return WorkItem::Result::kDoNotRunOptimization; });
        }

        bool PoseGraph2D::IsTrajectoryFrozen(const int trajectory_id) const
        {
            return data_.trajectories_state.count(trajectory_id) != 0 &&
                   data_.trajectories_state.at(trajectory_id).state ==
                       TrajectoryState::FROZEN;
        }
        // 从proto流数据中添加Submap
        void PoseGraph2D::AddSubmapFromProto(
            const transform::Rigid3d &global_submap_pose, const proto::Submap &submap)
        {
            if (!submap.has_submap_2d())
            {
                return;
            }

            const SubmapId submap_id = {submap.submap_id().trajectory_id(),
                                        submap.submap_id().submap_index()};

            const transform::Rigid2d global_submap_pose_2d =
                transform::Project2D(global_submap_pose);
            {
                absl::MutexLock locker(&mutex_);
                const std::shared_ptr<const Submap2D> submap_ptr =
                    std::make_shared<const Submap2D>(submap.submap_2d(),
                                                     &conversion_tables_);
                AddTrajectoryIfNeeded(submap_id.trajectory_id);
                if (!CanAddWorkItemModifying(submap_id.trajectory_id))
                    return;
                data_.submap_data.Insert(submap_id, InternalSubmapData());
                data_.submap_data.at(submap_id).submap = submap_ptr;
                InvalidateMapScanDistanceField();
                // Immediately show the submap at the 'global_submap_pose'.
                data_.global_submap_poses_2d.Insert(
                    submap_id, optimization::SubmapSpec2D{global_submap_pose_2d});
            }

            // TODO(MichaelGrupp): MapBuilder does freezing before deserializing submaps,
            // so this should be fine.
            if (IsTrajectoryFrozen(submap_id.trajectory_id))
            {
                kFrozenSubmapsMetric->Increment();
            }
            else
            {
                kActiveSubmapsMetric->Increment();
            }

            AddWorkItem(
                [this, submap_id, global_submap_pose_2d]() LOCKS_EXCLUDED(mutex_)
                {
                    absl::MutexLock locker(&mutex_);
                    data_.submap_data.at(submap_id).state = SubmapState::kFinished;
                    optimization_problem_->InsertSubmap(submap_id, global_submap_pose_2d);
                    InvalidateMapScanDistanceField();
                    return WorkItem::Result::kDoNotRunOptimization;
                });
        }

        void PoseGraph2D::AddNodeFromProto(const transform::Rigid3d &global_pose,
                                           const proto::Node &node)
        {
            const NodeId node_id = {node.node_id().trajectory_id(),
                                    node.node_id().node_index()};
            std::shared_ptr<const TrajectoryNode::Data> constant_data =
                std::make_shared<const TrajectoryNode::Data>(FromProto(node.node_data()));

            {
                absl::MutexLock locker(&mutex_);
                AddTrajectoryIfNeeded(node_id.trajectory_id);
                if (!CanAddWorkItemModifying(node_id.trajectory_id))
                    return;
                data_.trajectory_nodes.Insert(node_id,
                                              TrajectoryNode{constant_data, global_pose});
            }

            AddWorkItem([this, node_id, global_pose]() LOCKS_EXCLUDED(mutex_)
                        {
    absl::MutexLock locker(&mutex_);
    const auto& constant_data =
        data_.trajectory_nodes.at(node_id).constant_data;
    const auto gravity_alignment_inverse = transform::Rigid3d::Rotation(
        constant_data->gravity_alignment.inverse());
    optimization_problem_->InsertTrajectoryNode(
        node_id,
        optimization::NodeSpec2D{
            constant_data->time,
            transform::Project2D(constant_data->local_pose *
                                 gravity_alignment_inverse),
            transform::Project2D(global_pose * gravity_alignment_inverse),
            constant_data->gravity_alignment});
    return WorkItem::Result::kDoNotRunOptimization; });
        }

        void PoseGraph2D::SetTrajectoryDataFromProto(
            const proto::TrajectoryData &data)
        {
            TrajectoryData trajectory_data;
            // gravity_constant and imu_calibration are omitted as its not used in 2d

            if (data.has_fixed_frame_origin_in_map())
            {
                trajectory_data.fixed_frame_origin_in_map =
                    transform::ToRigid3(data.fixed_frame_origin_in_map());

                const int trajectory_id = data.trajectory_id();
                AddWorkItem([this, trajectory_id, trajectory_data]()
                                LOCKS_EXCLUDED(mutex_)
                            {
                      absl::MutexLock locker(&mutex_);
                      if (CanAddWorkItemModifying(trajectory_id)) {
                        optimization_problem_->SetTrajectoryData(
                            trajectory_id, trajectory_data);
                      }
                      return WorkItem::Result::kDoNotRunOptimization; });
            }
        }

        void PoseGraph2D::AddNodeToSubmap(const NodeId &node_id,
                                          const SubmapId &submap_id)
        {
            AddWorkItem([this, node_id, submap_id]() LOCKS_EXCLUDED(mutex_)
                        {
    absl::MutexLock locker(&mutex_);
    if (CanAddWorkItemModifying(submap_id.trajectory_id)) {
      data_.submap_data.at(submap_id).node_ids.insert(node_id);
    }
    return WorkItem::Result::kDoNotRunOptimization; });
        }

        void PoseGraph2D::AddSerializedConstraints(
            const std::vector<Constraint> &constraints)
        {
            AddWorkItem([this, constraints]() LOCKS_EXCLUDED(mutex_)
                        {
    absl::MutexLock locker(&mutex_);
    for (const auto& constraint : constraints) {
      CHECK(data_.trajectory_nodes.Contains(constraint.node_id));
      CHECK(data_.submap_data.Contains(constraint.submap_id));
      CHECK(data_.trajectory_nodes.at(constraint.node_id).constant_data !=
            nullptr);
      CHECK(data_.submap_data.at(constraint.submap_id).submap != nullptr);
      switch (constraint.tag) {
        case Constraint::Tag::INTRA_SUBMAP:
          CHECK(data_.submap_data.at(constraint.submap_id)
                    .node_ids.emplace(constraint.node_id)
                    .second);
          break;
        case Constraint::Tag::INTER_SUBMAP:
          UpdateTrajectoryConnectivity(constraint);
          break;
      }
      const Constraint::Pose pose = {
          constraint.pose.zbar_ij *
              transform::Rigid3d::Rotation(
                  data_.trajectory_nodes.at(constraint.node_id)
                      .constant_data->gravity_alignment.inverse()),
          constraint.pose.translation_weight, constraint.pose.rotation_weight};
      data_.constraints.push_back(Constraint{
          constraint.submap_id, constraint.node_id, pose, constraint.tag});
    }
    LOG(INFO) << "Loaded " << constraints.size() << " constraints.";
    return WorkItem::Result::kDoNotRunOptimization; });
        }
        // map_builder.cc中调用, 纯定位时添加PureLocalizationTrimmer
        void PoseGraph2D::AddTrimmer(std::unique_ptr<PoseGraphTrimmer> trimmer)
        {
            // C++11 does not allow us to move a unique_ptr into a lambda.
            PoseGraphTrimmer *const trimmer_ptr = trimmer.release();
            AddWorkItem([this, trimmer_ptr]() LOCKS_EXCLUDED(mutex_)
                        {
    absl::MutexLock locker(&mutex_);
    trimmers_.emplace_back(trimmer_ptr);
    return WorkItem::Result::kDoNotRunOptimization; });
        }

        void PoseGraph2D::RunOptimizationOnce() { RunOptimization(); }
        // 执行最后一次的优化, 等待所有的计算任务结束
        void PoseGraph2D::RunFinalOptimization()
        {
            {
                AddWorkItem([this]() LOCKS_EXCLUDED(mutex_)
                            {
      absl::MutexLock locker(&mutex_);
      optimization_problem_->SetMaxNumIterations(
          options_.max_num_final_iterations());
      return WorkItem::Result::kRunOptimization; });
                AddWorkItem([this]() LOCKS_EXCLUDED(mutex_)
                            {
      absl::MutexLock locker(&mutex_);
      optimization_problem_->SetMaxNumIterations(
          options_.optimization_problem_options()
              .ceres_solver_options()
              .max_num_iterations());
      return WorkItem::Result::kDoNotRunOptimization; });
            }
            WaitForAllComputations();
        }

        void PoseGraph2D::RunOptimization()
        {
            if (optimization_problem_->submap_data().empty())
            {
                return;
            }

            // No other thread is accessing the optimization_problem_,
            // data_.constraints, data_.frozen_trajectories and data_.landmark_nodes
            // when executing the Solve. Solve is time consuming, so not taking the mutex
            // before Solve to avoid blocking foreground processing.

            // Solve 比较耗时, 所以在执行 Solve 之前不要加互斥锁, 以免阻塞其他的任务处理
            // landmark直接参与优化问题
            optimization_problem_->Solve(data_.constraints, GetTrajectoryStates(),
                                         data_.landmark_nodes);
            absl::MutexLock locker(&mutex_);

            const auto &submap_data = optimization_problem_->submap_data();
            const auto &node_data = optimization_problem_->node_data();
            for (const int trajectory_id : node_data.trajectory_ids())
            {
                for (const auto &node : node_data.trajectory(trajectory_id))
                {
                    auto &mutable_trajectory_node = data_.trajectory_nodes.at(node.id);
                    mutable_trajectory_node.global_pose =
                        transform::Embed3D(node.data.global_pose_2d) *
                        transform::Rigid3d::Rotation(
                            mutable_trajectory_node.constant_data->gravity_alignment);
                }

                // Extrapolate all point cloud poses that were not included in the
                // 'optimization_problem_' yet.
                const auto local_to_new_global =
                    ComputeLocalToGlobalTransform(submap_data, trajectory_id);
                const auto local_to_old_global = ComputeLocalToGlobalTransform(
                    data_.global_submap_poses_2d, trajectory_id);
                const transform::Rigid3d old_global_to_new_global =
                    local_to_new_global * local_to_old_global.inverse();

                const NodeId last_optimized_node_id =
                    std::prev(node_data.EndOfTrajectory(trajectory_id))->id;
                auto node_it =
                    std::next(data_.trajectory_nodes.find(last_optimized_node_id));
                for (; node_it != data_.trajectory_nodes.EndOfTrajectory(trajectory_id);
                     ++node_it)
                {
                    auto &mutable_trajectory_node = data_.trajectory_nodes.at(node_it->id);
                    mutable_trajectory_node.global_pose =
                        old_global_to_new_global * mutable_trajectory_node.global_pose;
                }
            }
            for (const auto &landmark : optimization_problem_->landmark_data())
            {
                data_.landmark_nodes[landmark.first].global_landmark_pose = landmark.second;
            }
            data_.global_submap_poses_2d = submap_data;
        }
        // 根据轨迹状态判断是否可以添加任务
        bool PoseGraph2D::CanAddWorkItemModifying(int trajectory_id)
        {
            auto it = data_.trajectories_state.find(trajectory_id);
            if (it == data_.trajectories_state.end())
            {
                return true;
            }
            if (it->second.state == TrajectoryState::FINISHED)
            {
                // TODO(gaschler): Replace all FATAL to WARNING after some testing.
                LOG(FATAL) << "trajectory_id " << trajectory_id
                           << " has finished "
                              "but modification is requested, skipping.";
                return false;
            }
            if (it->second.deletion_state !=
                InternalTrajectoryState::DeletionState::NORMAL)
            {
                LOG(FATAL) << "trajectory_id " << trajectory_id
                           << " has been scheduled for deletion "
                              "but modification is requested, skipping.";
                return false;
            }
            if (it->second.state == TrajectoryState::DELETED)
            {
                LOG(FATAL) << "trajectory_id " << trajectory_id
                           << " has been deleted "
                              "but modification is requested, skipping.";
                return false;
            }
            return true;
        }

        MapById<NodeId, TrajectoryNode> PoseGraph2D::GetTrajectoryNodes() const
        {
            absl::MutexLock locker(&mutex_);
            return data_.trajectory_nodes;
        }

        MapById<NodeId, TrajectoryNodePose> PoseGraph2D::GetTrajectoryNodePoses() const
        {
            MapById<NodeId, TrajectoryNodePose> node_poses;
            absl::MutexLock locker(&mutex_);
            for (const auto &node_id_data : data_.trajectory_nodes)
            {
                absl::optional<TrajectoryNodePose::ConstantPoseData> constant_pose_data;
                if (node_id_data.data.constant_data != nullptr)
                {
                    constant_pose_data = TrajectoryNodePose::ConstantPoseData{
                        node_id_data.data.constant_data->time,
                        node_id_data.data.constant_data->local_pose};
                }
                node_poses.Insert(
                    node_id_data.id,
                    TrajectoryNodePose{node_id_data.data.global_pose, constant_pose_data});
            }
            return node_poses;
        }

        std::map<int, PoseGraphInterface::TrajectoryState>
        PoseGraph2D::GetTrajectoryStates() const
        {
            std::map<int, PoseGraphInterface::TrajectoryState> trajectories_state;
            absl::MutexLock locker(&mutex_);
            for (const auto &it : data_.trajectories_state)
            {
                trajectories_state[it.first] = it.second.state;
            }
            return trajectories_state;
        }
        // 获取所有的landmark的位姿
        std::map<std::string, transform::Rigid3d> PoseGraph2D::GetLandmarkPoses()
            const
        {
            std::map<std::string, transform::Rigid3d> landmark_poses;
            absl::MutexLock locker(&mutex_);
            for (const auto &landmark : data_.landmark_nodes)
            {
                // Landmark without value has not been optimized yet.
                if (!landmark.second.global_landmark_pose.has_value())
                    continue;
                landmark_poses[landmark.first] =
                    landmark.second.global_landmark_pose.value();
            }
            return landmark_poses;
        }
        // 设置landmark在global坐标系下的坐标, 只有在从proto加载状态时进行使用
        void PoseGraph2D::SetLandmarkPose(const std::string &landmark_id,
                                          const transform::Rigid3d &global_pose,
                                          const bool frozen)
        {
            AddWorkItem([=]() LOCKS_EXCLUDED(mutex_)
                        {
    absl::MutexLock locker(&mutex_);
    data_.landmark_nodes[landmark_id].global_landmark_pose = global_pose;
    data_.landmark_nodes[landmark_id].frozen = frozen;
    return WorkItem::Result::kDoNotRunOptimization; });
        }
        // 获取优化问题中的imu数据, 会返回空值, 因为2D优化中不使用IMU数据
        sensor::MapByTime<sensor::ImuData> PoseGraph2D::GetImuData() const
        {
            absl::MutexLock locker(&mutex_);
            return optimization_problem_->imu_data();
        }

        sensor::MapByTime<sensor::OdometryData> PoseGraph2D::GetOdometryData() const
        {
            absl::MutexLock locker(&mutex_);
            return optimization_problem_->odometry_data();
        }

        std::map<std::string /* landmark ID */, PoseGraphInterface::LandmarkNode>
        PoseGraph2D::GetLandmarkNodes() const
        {
            absl::MutexLock locker(&mutex_);
            return data_.landmark_nodes;
        }

        std::map<int, PoseGraphInterface::TrajectoryData>
        PoseGraph2D::GetTrajectoryData() const
        {
            absl::MutexLock locker(&mutex_);
            return optimization_problem_->trajectory_data();
        }

        sensor::MapByTime<sensor::FixedFramePoseData>
        PoseGraph2D::GetFixedFramePoseData() const
        {
            absl::MutexLock locker(&mutex_);
            return optimization_problem_->fixed_frame_pose_data();
        }
        // 返回位姿图结构中的所有的约束
        std::vector<PoseGraphInterface::Constraint> PoseGraph2D::constraints() const
        {
            std::vector<PoseGraphInterface::Constraint> result;
            absl::MutexLock locker(&mutex_);
            for (const Constraint &constraint : data_.constraints)
            {
                result.push_back(Constraint{
                    constraint.submap_id, constraint.node_id,
                    Constraint::Pose{constraint.pose.zbar_ij *
                                         transform::Rigid3d::Rotation(
                                             data_.trajectory_nodes.at(constraint.node_id)
                                                 .constant_data->gravity_alignment),
                                     constraint.pose.translation_weight,
                                     constraint.pose.rotation_weight},
                    constraint.tag});
            }
            return result;
        }

        void PoseGraph2D::SetInitialTrajectoryPose(const int from_trajectory_id,
                                                   const int to_trajectory_id,
                                                   const transform::Rigid3d &pose,
                                                   const common::Time time)
        {
            absl::MutexLock locker(&mutex_);
            data_.initial_trajectory_poses[from_trajectory_id] =
                InitialTrajectoryPose{to_trajectory_id, pose, time};
        }

        // 线性插值计算指定时间的global_pose
        transform::Rigid3d PoseGraph2D::GetInterpolatedGlobalTrajectoryPose(
            const int trajectory_id, const common::Time time) const
        {
            CHECK_GT(data_.trajectory_nodes.SizeOfTrajectoryOrZero(trajectory_id), 0);
            const auto it = data_.trajectory_nodes.lower_bound(trajectory_id, time);
            if (it == data_.trajectory_nodes.BeginOfTrajectory(trajectory_id))
            {
                return data_.trajectory_nodes.BeginOfTrajectory(trajectory_id)
                    ->data.global_pose;
            }
            if (it == data_.trajectory_nodes.EndOfTrajectory(trajectory_id))
            {
                return std::prev(data_.trajectory_nodes.EndOfTrajectory(trajectory_id))
                    ->data.global_pose;
            }
            return transform::Interpolate(
                       transform::TimestampedTransform{std::prev(it)->data.time(),
                                                       std::prev(it)->data.global_pose},
                       transform::TimestampedTransform{it->data.time(),
                                                       it->data.global_pose},
                       time)
                .transform;
        }
        // 计算 global frame 指向 local frame 的坐标变换
        transform::Rigid3d PoseGraph2D::GetLocalToGlobalTransform(
            const int trajectory_id) const
        {
            absl::MutexLock locker(&mutex_);
            return ComputeLocalToGlobalTransform(data_.global_submap_poses_2d,
                                                 trajectory_id);
        }

        std::vector<std::vector<int>> PoseGraph2D::GetConnectedTrajectories() const
        {
            absl::MutexLock locker(&mutex_);
            return data_.trajectory_connectivity_state.Components();
        }
        // 获取指定id的submap地图
        PoseGraphInterface::SubmapData PoseGraph2D::GetSubmapData(
            const SubmapId &submap_id) const
        {
            absl::MutexLock locker(&mutex_);
            return GetSubmapDataUnderLock(submap_id);
        }
        // 获取所有的submap地图
        MapById<SubmapId, PoseGraphInterface::SubmapData>
        PoseGraph2D::GetAllSubmapData() const
        {
            absl::MutexLock locker(&mutex_);
            return GetSubmapDataUnderLock();
        }
        // 获取所有的submap的原点的坐标
        MapById<SubmapId, PoseGraphInterface::SubmapPose>
        PoseGraph2D::GetAllSubmapPoses() const
        {
            absl::MutexLock locker(&mutex_);
            MapById<SubmapId, SubmapPose> submap_poses;
            for (const auto &submap_id_data : data_.submap_data)
            {
                auto submap_data = GetSubmapDataUnderLock(submap_id_data.id);
                submap_poses.Insert(
                    submap_id_data.id,
                    PoseGraph::SubmapPose{submap_data.submap->num_range_data(),
                                          submap_data.pose});
            }
            return submap_poses;
        }
        // 计算 global frame 指向 local frame 的坐标变换
        transform::Rigid3d PoseGraph2D::ComputeLocalToGlobalTransform(
            const MapById<SubmapId, optimization::SubmapSpec2D> &global_submap_poses,
            const int trajectory_id) const
        {
            auto begin_it = global_submap_poses.BeginOfTrajectory(trajectory_id);
            auto end_it = global_submap_poses.EndOfTrajectory(trajectory_id);
            if (begin_it == end_it)
            {
                const auto it = data_.initial_trajectory_poses.find(trajectory_id);
                if (it != data_.initial_trajectory_poses.end())
                {
                    return GetInterpolatedGlobalTrajectoryPose(it->second.to_trajectory_id,
                                                               it->second.time) *
                           it->second.relative_pose;
                }
                else
                {
                    return transform::Rigid3d::Identity();
                }
            }
            const SubmapId last_optimized_submap_id = std::prev(end_it)->id;
            // Accessing 'local_pose' in Submap is okay, since the member is const.
            // 通过最后一个优化后的 global_pose * local_pose().inverse() 获取 global_pose->local_pose的坐标变换
            return transform::Embed3D(
                       global_submap_poses.at(last_optimized_submap_id).global_pose) *
                   data_.submap_data.at(last_optimized_submap_id)
                       .submap->local_pose()
                       .inverse();
        }

        PoseGraphInterface::SubmapData PoseGraph2D::GetSubmapDataUnderLock(
            const SubmapId &submap_id) const
        {
            const auto it = data_.submap_data.find(submap_id);
            if (it == data_.submap_data.end())
            {
                return {};
            }
            auto submap = it->data.submap;
            if (data_.global_submap_poses_2d.Contains(submap_id))
            {
                // We already have an optimized pose.
                return {submap,
                        transform::Embed3D(
                            data_.global_submap_poses_2d.at(submap_id).global_pose)};
            }
            // We have to extrapolate.
            return {submap, ComputeLocalToGlobalTransform(data_.global_submap_poses_2d,
                                                          submap_id.trajectory_id) *
                                submap->local_pose()};
        }

        PoseGraph2D::TrimmingHandle::TrimmingHandle(PoseGraph2D *const parent)
            : parent_(parent) {}

        int PoseGraph2D::TrimmingHandle::num_submaps(const int trajectory_id) const
        {
            const auto &submap_data = parent_->optimization_problem_->submap_data();
            return submap_data.SizeOfTrajectoryOrZero(trajectory_id);
        }

        MapById<SubmapId, PoseGraphInterface::SubmapData>
        PoseGraph2D::TrimmingHandle::GetOptimizedSubmapData() const
        {
            MapById<SubmapId, PoseGraphInterface::SubmapData> submaps;
            for (const auto &submap_id_data : parent_->data_.submap_data)
            {
                if (submap_id_data.data.state != SubmapState::kFinished ||
                    !parent_->data_.global_submap_poses_2d.Contains(submap_id_data.id))
                {
                    continue;
                }
                submaps.Insert(
                    submap_id_data.id,
                    SubmapData{submap_id_data.data.submap,
                               transform::Embed3D(parent_->data_.global_submap_poses_2d
                                                      .at(submap_id_data.id)
                                                      .global_pose)});
            }
            return submaps;
        }

        std::vector<SubmapId> PoseGraph2D::TrimmingHandle::GetSubmapIds(
            int trajectory_id) const
        {
            std::vector<SubmapId> submap_ids;
            const auto &submap_data = parent_->optimization_problem_->submap_data();
            for (const auto &it : submap_data.trajectory(trajectory_id))
            {
                submap_ids.push_back(it.id);
            }
            return submap_ids;
        }

        const MapById<NodeId, TrajectoryNode> &
        PoseGraph2D::TrimmingHandle::GetTrajectoryNodes() const
        {
            return parent_->data_.trajectory_nodes;
        }

        const std::vector<PoseGraphInterface::Constraint> &
        PoseGraph2D::TrimmingHandle::GetConstraints() const
        {
            return parent_->data_.constraints;
        }

        // 轨迹结束了, 裁剪器就结束
        bool PoseGraph2D::TrimmingHandle::IsFinished(const int trajectory_id) const
        {
            return parent_->IsTrajectoryFinished(trajectory_id);
        }

        void PoseGraph2D::TrimmingHandle::SetTrajectoryState(int trajectory_id,
                                                             TrajectoryState state)
        {
            parent_->data_.trajectories_state[trajectory_id].state = state;
        }

        // 删除指定id的子图, 并删除相关的约束,匹配器,与节点
        void PoseGraph2D::TrimmingHandle::TrimSubmap(const SubmapId &submap_id)
        {
            // TODO(hrapp): We have to make sure that the trajectory has been finished
            // if we want to delete the last submaps.
            CHECK(parent_->data_.submap_data.at(submap_id).state ==
                  SubmapState::kFinished);

            // Compile all nodes that are still INTRA_SUBMAP constrained to other submaps
            // once the submap with 'submap_id' is gone.
            // We need to use node_ids instead of constraints here to be also compatible
            // with frozen trajectories that don't have intra-constraints.
            // 找到在submap_id的子图内部同时不在别的子图内的节点, 这些节点需要删除
            std::set<NodeId> nodes_to_retain;
            for (const auto &submap_data : parent_->data_.submap_data)
            {
                if (submap_data.id != submap_id)
                {
                    nodes_to_retain.insert(submap_data.data.node_ids.begin(),
                                           submap_data.data.node_ids.end());
                }
            }

            // Remove all nodes that are exlusively associated to 'submap_id'.
            std::set<NodeId> nodes_to_remove;
            std::set_difference(parent_->data_.submap_data.at(submap_id).node_ids.begin(),
                                parent_->data_.submap_data.at(submap_id).node_ids.end(),
                                nodes_to_retain.begin(), nodes_to_retain.end(),
                                std::inserter(nodes_to_remove, nodes_to_remove.begin()));

            // Remove all 'data_.constraints' related to 'submap_id'.
            {
                std::vector<Constraint> constraints;
                for (const Constraint &constraint : parent_->data_.constraints)
                {
                    if (constraint.submap_id != submap_id)
                    {
                        constraints.push_back(constraint);
                    }
                }
                parent_->data_.constraints = std::move(constraints);
            }

            // Remove all 'data_.constraints' related to 'nodes_to_remove'.
            // If the removal lets other submaps lose all their inter-submap constraints,
            // delete their corresponding constraint submap matchers to save memory.
            {
                std::vector<Constraint> constraints;
                std::set<SubmapId> other_submap_ids_losing_constraints;
                for (const Constraint &constraint : parent_->data_.constraints)
                {
                    if (nodes_to_remove.count(constraint.node_id) == 0)
                    {
                        constraints.push_back(constraint);
                    }
                    else
                    {
                        // A constraint to another submap will be removed, mark it as affected.
                        other_submap_ids_losing_constraints.insert(constraint.submap_id);
                    }
                }
                parent_->data_.constraints = std::move(constraints);
                // Go through the remaining constraints to ensure we only delete scan
                // matchers of other submaps that have no inter-submap constraints left.
                for (const Constraint &constraint : parent_->data_.constraints)
                {
                    if (constraint.tag == Constraint::Tag::INTRA_SUBMAP)
                    {
                        continue;
                    }
                    else if (other_submap_ids_losing_constraints.count(
                                 constraint.submap_id))
                    {
                        // This submap still has inter-submap constraints - ignore it.
                        other_submap_ids_losing_constraints.erase(constraint.submap_id);
                    }
                }
                // Delete scan matchers of the submaps that lost all constraints.
                // TODO(wohe): An improvement to this implementation would be to add the
                // caching logic at the constraint builder which could keep around only
                // recently used scan matchers.
                for (const SubmapId &submap_id : other_submap_ids_losing_constraints)
                {
                    parent_->constraint_builder_.DeleteScanMatcher(submap_id);
                }
            }

            // Mark the submap with 'submap_id' as trimmed and remove its data.
            CHECK(parent_->data_.submap_data.at(submap_id).state ==
                  SubmapState::kFinished);
            parent_->data_.submap_data.Trim(submap_id);
            parent_->constraint_builder_.DeleteScanMatcher(submap_id);
            parent_->optimization_problem_->TrimSubmap(submap_id);
            parent_->InvalidateMapScanDistanceField();

            // We have one submap less, update the gauge metrics.
            kDeletedSubmapsMetric->Increment();
            if (parent_->IsTrajectoryFrozen(submap_id.trajectory_id))
            {
                kFrozenSubmapsMetric->Decrement();
            }
            else
            {
                kActiveSubmapsMetric->Decrement();
            }

            // Remove the 'nodes_to_remove' from the pose graph and the optimization
            // problem.
            for (const NodeId &node_id : nodes_to_remove)
            {
                parent_->data_.trajectory_nodes.Trim(node_id);
                parent_->optimization_problem_->TrimTrajectoryNode(node_id);
            }
        }

        MapById<SubmapId, PoseGraphInterface::SubmapData>
        PoseGraph2D::GetSubmapDataUnderLock() const
        {
            MapById<SubmapId, PoseGraphInterface::SubmapData> submaps;
            for (const auto &submap_id_data : data_.submap_data)
            {
                submaps.Insert(submap_id_data.id,
                               GetSubmapDataUnderLock(submap_id_data.id));
            }
            return submaps;
        }

        void PoseGraph2D::SetGlobalSlamOptimizationCallback(
            PoseGraphInterface::GlobalSlamOptimizationCallback callback)
        {
            global_slam_optimization_callback_ = callback;
        }

        void PoseGraph2D::RegisterMetrics(metrics::FamilyFactory *family_factory)
        {
            auto *latency = family_factory->NewGaugeFamily(
                "mapping_2d_pose_graph_work_queue_delay",
                "Age of the oldest entry in the work queue in seconds");
            kWorkQueueDelayMetric = latency->Add({});
            auto *queue_size =
                family_factory->NewGaugeFamily("mapping_2d_pose_graph_work_queue_size",
                                               "Number of items in the work queue");
            kWorkQueueSizeMetric = queue_size->Add({});
            auto *constraints = family_factory->NewGaugeFamily(
                "mapping_2d_pose_graph_constraints",
                "Current number of constraints in the pose graph");
            kConstraintsDifferentTrajectoryMetric =
                constraints->Add({{"tag", "inter_submap"}, {"trajectory", "different"}});
            kConstraintsSameTrajectoryMetric =
                constraints->Add({{"tag", "inter_submap"}, {"trajectory", "same"}});
            auto *submaps = family_factory->NewGaugeFamily(
                "mapping_2d_pose_graph_submaps", "Number of submaps in the pose graph.");
            kActiveSubmapsMetric = submaps->Add({{"state", "active"}});
            kFrozenSubmapsMetric = submaps->Add({{"state", "frozen"}});
            kDeletedSubmapsMetric = submaps->Add({{"state", "deleted"}});
        }

    } // namespace mapping
} // namespace cartographer

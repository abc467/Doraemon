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
#include <cmath>
#include <cstdio>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <limits>
#include <memory>
#include <sstream>
#include <string>

#include "Eigen/Eigenvalues"
#include "absl/memory/memory.h"
#include "cartographer/common/math.h"
#include "cartographer/mapping/internal/2d/overlapping_submaps_trimmer_2d.h"
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
            }
        }

        PoseGraph2D::~PoseGraph2D()
        {
            WaitForAllComputations();
            absl::MutexLock locker(&work_queue_mutex_);
            this->working = false;
            this->th_process_flirt.join();

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

                // Detect and Describe
                auto node = this->data_.trajectory_nodes.find(node_id);
                if (node == this->data_.trajectory_nodes.end())
                {
                    continue;
                }

                // Attention! InterestPoints may be zero.
                detecting_lock_.lock();
                if (node->data.constant_data->interest_points.size() > 0)
                {
                    detecting_lock_.unlock();
                    continue;
                }

                auto constant_data =
                    const_cast<TrajectoryNode::Data *>(node->data.constant_data.get());
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
                double robot_x = node->data.global_pose.translation().x();
                double robot_y = node->data.global_pose.translation().y();
                double robot_theta = 0;
                scan.setLaserPose({robot_x, robot_y, robot_theta});

                flirt::detect(scan, constant_data->interest_points);
                for (auto &&i : constant_data->interest_points)
                {
                    i->setDescriptor(flirt::describe(*i, scan));
                }
                LOG(WARNING) << "Finish detect and describe, NodeID=" << node_id.trajectory_id << "|" << node_id.node_index;
                detecting_lock_.unlock();
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

        void PoseGraph2D::DetectAndDescribe(const NodeId &node_id)
        {
            auto node = this->data_.trajectory_nodes.find(node_id);
            if (node == this->data_.trajectory_nodes.end())
            {
                return;
            }

            auto constant_data =
                const_cast<TrajectoryNode::Data *>(node->data.constant_data.get());

            // 不能仅仅依靠size来判读是否完成
            detecting_lock_.lock();
            if (constant_data->interest_points.size() > 0)
            {
                detecting_lock_.unlock();
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
            double robot_x = node->data.global_pose.translation().x();
            double robot_y = node->data.global_pose.translation().y();
            double robot_theta = 0;
            scan.setLaserPose({robot_x, robot_y, robot_theta});

            flirt::detect(scan, constant_data->interest_points);
            for (auto &&i : constant_data->interest_points)
            {
                i->setDescriptor(flirt::describe(*i, scan));
            }
            LOG(WARNING) << "Finish detect and describe";
            detecting_lock_.unlock();
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

            if (flirt::need_flirt.load())
            {
                flirt::flirt_working.store(true);
                flirt::cv_flirt_busy.notify_all();
                flirt::flirt_return_code = ExecuteGlobalRelocationForNode(node_id);
                flirt::flirt_working.store(false);

                flirt::need_flirt.store(false);
                flirt::cv_flirt_busy.notify_all();
            }

            // We have to check this here, because it might have changed by the time
            // we execute the lambda.
            const bool newly_finished_submap =
                insertion_submaps.front()->insertion_finished();
            AddWorkItem([=]() LOCKS_EXCLUDED(mutex_)
                        { return ComputeConstraintsForNode(node_id, insertion_submaps,
                                                           newly_finished_submap); });
            return node_id;
        }

        // 将任务放入到任务队列中等待被执行
        void PoseGraph2D::AddWorkItem(const std::function<WorkItem::Result()> &work_item)
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
            const auto now = std::chrono::steady_clock::now();
            work_queue_->push_back({now, work_item});
            kWorkQueueSizeMetric->Set(work_queue_->size());
            kWorkQueueDelayMetric->Set(
                std::chrono::duration_cast<std::chrono::duration<double>>(
                    now - work_queue_->front().time)
                    .count());
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
                return WorkItem::Result::kDoNotRunOptimization; });
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
            return WorkItem::Result::kDoNotRunOptimization; });
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
    return WorkItem::Result::kDoNotRunOptimization; });
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
                return WorkItem::Result::kDoNotRunOptimization; });
        }

        // 使用flirt执行全局重定位
        int PoseGraph2D::ExecuteGlobalRelocationForNode(const NodeId &ref_node_id)
        {
            // check enable flag
            LOG(WARNING) << "ExecuteGlobalRelocationForNode=" << ref_node_id.trajectory_id
                         << "|" << ref_node_id.node_index;

            // check trajectory id, must be greater than Zero.
            if (ref_node_id.trajectory_id < 1)
            {
                return flirt::kRelocationNeedMoreTrajectories;
            }

            // define pose structure
            std::vector<constraints::EstimatedPose> estimated_pose;

            // Make sure this node has been Detected and Described,
            // else, this function will try to do that.
            this->DetectAndDescribe(ref_node_id);

            const TrajectoryNode::Data *constant_data =
                data_.trajectory_nodes.at(ref_node_id).constant_data.get();

            // Check again, make sure that the current Node has Interest Points.
            if (constant_data->interest_points.empty())
            {
                return flirt::kRelocationNoInterestPoints;
            }

            // 这里是用于输出点云信息到csv文件，用来做测试的
            // append_node_data_pose("NewNodeData", constant_data->local_pose,
            //                       constant_data->filtered_gravity_aligned_point_cloud);

            // append_interest_points("^KptsXYA^", constant_data->interest_points);

            auto node_it = data_.trajectory_nodes.BeginOfTrajectory(0);
            auto nodes_end = data_.trajectory_nodes.EndOfTrajectory(ref_node_id.trajectory_id - 1); // 可以考虑用data_.trajectories_state来代替
            LOG(WARNING) << "Search range in trajectory=[0" << "," << ref_node_id.trajectory_id - 1 << "]";
            for (; node_it != nodes_end; node_it.operator++())
            {
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
                             constant_data->interest_points, _transform, _corres);

                // compute score
                float score = (float)_corres.size() / (float)constant_data->interest_points.size();
                if (score > 0.1)
                {
                    // 寻找哪些submap包含了这个node
                    // 这是V1.0的代码，但是因为我们知道这个node是属于哪个trajectory下的，所以我们没有必要再遍历所有
                    // 我们只需要在node所属的trajectory下遍历submap即可，找到该node属于哪个submap
                    // auto submap_it = data_.submap_data.BeginOfTrajectory(0);
                    // auto submaps_end = data_.submap_data.EndOfTrajectory(ref_node_id.trajectory_id - 1);
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
                    estimated_pose.push_back(
                        {NodeId(ref_node_id), SubmapId(submap_it->id), score,
                         node_local_pose,
                         static_cast<const Submap2D *>(submap_it->data.submap.get())});
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
                    submap_id, submap, node_id, constant_data, initial_relative_pose);
            }
            else if (maybe_add_global_constraint)
            {
                constraint_builder_.MaybeAddGlobalConstraint(submap_id, submap, node_id,
                                                             constant_data);
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
            std::vector<SubmapId> submap_ids;
            std::vector<SubmapId> finished_submap_ids;
            std::set<NodeId> newly_finished_submap_node_ids;
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
                for (const auto &submap_id_data : data_.submap_data)
                {
                    if (submap_id_data.data.state == SubmapState::kFinished)
                    {
                        CHECK_EQ(submap_id_data.data.node_ids.count(node_id), 0);
                        finished_submap_ids.emplace_back(submap_id_data.id);
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
                }
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
                for (const auto &node_id_data : optimization_problem_->node_data())
                {
                    const NodeId &node_id = node_id_data.id;
                    if (newly_finished_submap_node_ids.count(node_id) == 0)
                    {
                        ComputeConstraint(node_id, newly_finished_submap_id);
                    }
                }
            }
            constraint_builder_.NotifyEndOfNode();
            {
                absl::MutexLock locker(&mutex_);
                ++num_nodes_since_last_loop_closure_;
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
            {
                absl::MutexLock locker(&mutex_);
                data_.constraints.insert(data_.constraints.end(), result.begin(),
                                         result.end());
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
                for (const Constraint &constraint : result)
                {
                    UpdateTrajectoryConnectivity(constraint);
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
                    data_.constraints.insert(data_.constraints.end(), result.begin(),
                                             result.end());
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

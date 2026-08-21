-- Copyright 2016 The Cartographer Authors
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

include "map_builder.lua"
include "trajectory_builder.lua"

options = {
  map_builder = MAP_BUILDER,
  trajectory_builder = TRAJECTORY_BUILDER,
  map_frame = "map",
  tracking_frame = "gyro_link",
  -- External odom is available in this mode: consume /odom and publish only
  -- map -> odom so Cartographer does not conflict with the wheel odom TF.
  published_frame = "odom",
  odom_frame = "odom",
  provide_odom_frame = false,
  -- Keep map/odom planar for the 2D navigation stack and RViz overlays.
  publish_frame_projected_to_2d = true,
  use_pose_extrapolator = false,
  publish_tracked_pose = true,
  use_odometry = true,
  use_nav_sat = false,
  -- 运行时没有 /landmark 发布，先关闭额外锚点输入，避免空订阅干扰排障。
  use_landmarks = false,
  num_laser_scans = 1,
  num_multi_echo_laser_scans = 0,
  num_subdivisions_per_laser_scan = 2,
  num_point_clouds = 0,
  lookup_transform_timeout_sec = 0.2,
  submap_publish_period_sec = 0.3,
  pose_publish_period_sec = 5e-3,
  trajectory_publish_period_sec = 30e-3,
  rangefinder_sampling_ratio = 1.,
  odometry_sampling_ratio = 1.,
  fixed_frame_pose_sampling_ratio = 1.,
  imu_sampling_ratio = 1.,
  landmarks_sampling_ratio = 1.,
  ignore_out_of_order = true,
}

MAP_BUILDER.use_trajectory_builder_2d = true

-- 工厂环境优先保证窄通道和长直道中的稳定性，减少 scan matcher 在重复结构上的过度跳动。
TRAJECTORY_BUILDER_2D.num_accumulated_range_data = 2
-- 让激光栅格一致性在匹配里更有发言权，不再过度跟随 odom 先验。
TRAJECTORY_BUILDER_2D.ceres_scan_matcher.occupied_space_weight = 8.
TRAJECTORY_BUILDER_2D.ceres_scan_matcher.translation_weight = 5.
TRAJECTORY_BUILDER_2D.ceres_scan_matcher.rotation_weight = 12.


TRAJECTORY_BUILDER_2D.min_range = 0.1
TRAJECTORY_BUILDER_2D.max_range = 20. --定位保留中距离结构，同时限制长走廊远端重复结构干扰
TRAJECTORY_BUILDER_2D.adaptive_voxel_filter.max_length = 0.35
TRAJECTORY_BUILDER_2D.adaptive_voxel_filter.min_num_points = 260
TRAJECTORY_BUILDER_2D.adaptive_voxel_filter.max_range = 20.
TRAJECTORY_BUILDER_2D.missing_data_ray_length = 1.
TRAJECTORY_BUILDER_2D.use_imu_data = true
TRAJECTORY_BUILDER_2D.use_online_correlative_scan_matching = true
TRAJECTORY_BUILDER_2D.real_time_correlative_scan_matcher.linear_search_window = 0.10
TRAJECTORY_BUILDER_2D.real_time_correlative_scan_matcher.angular_search_window = math.rad(10.)
TRAJECTORY_BUILDER_2D.real_time_correlative_scan_matcher.translation_delta_cost_weight = 4.
TRAJECTORY_BUILDER_2D.real_time_correlative_scan_matcher.rotation_delta_cost_weight = 1.5e-1
-- 当前 Cartographer 版本未消费 turn_low_score_protection，先注释以保证配置可读取。
-- TRAJECTORY_BUILDER_2D.turn_low_score_protection = {
--   enabled = true,
--   low_score_threshold = 0.70,
--   release_score_threshold = 0.73,
--   consecutive_low_score_count = 2,
--   release_high_score_count = 3,
--   min_angular_velocity = 0.15,
--   protected_linear_search_window = 0.02,
--   protected_angular_search_window = math.rad(10.),
--   protected_translation_delta_cost_weight = 90.,
--   protected_rotation_delta_cost_weight = 4e-1,
--   max_translation_correction = 0.03,
-- }

TRAJECTORY_BUILDER_2D.submaps.num_range_data = 80 
POSE_GRAPH.optimization_problem.huber_scale = 1e1
POSE_GRAPH.optimize_every_n_nodes = 30 --提高后端优化频率，让定位轨迹更快被冻结地图约束拉回
-- active->frozen 已固定使用当前位姿附近的局部搜索，不再因连接超时切到
-- 全图轮询。该时间仅保留给其他跨轨迹约束逻辑。
POSE_GRAPH.global_constraint_search_after_n_seconds = 30.
POSE_GRAPH.global_sampling_ratio = 0.005 --active->frozen 不做全图轮询；该参数仅限制其他全局约束
POSE_GRAPH.constraint_builder.min_score = 0.58
POSE_GRAPH.constraint_builder.global_localization_min_score = 0.66 --提高全局重定位弱匹配门槛，降低重复结构误匹配
-- active->frozen 后端约束只用近距离端点。前端仍保留 20 m 激光；这样避免
-- 长通道 8~20 m 的掠射点因微小角度误差跨越多个 5 cm 栅格、拉低整帧 CSM 分数。
POSE_GRAPH.constraint_builder.active_frozen_constraint_max_range = 8.
POSE_GRAPH.constraint_builder.active_frozen_constraint_min_points = 100
-- 这里只是 active->frozen 局部 FCSM 的候选入口门槛。低于 0.62 的候选
-- 必须继续通过严格几何质量、小修正量和跨节点一致性检查，不能直接进入后端。
POSE_GRAPH.constraint_builder.active_frozen_local_min_score = 0.50

TRAJECTORY_BUILDER.pure_localization_trimmer.max_submaps_to_keep = 4 --trim 触发时保留更多上下文，减少 pure localization 重对齐跳变
TRAJECTORY_BUILDER_2D.motion_filter.max_time_seconds = 3.0 --限制静止/低速时 node 过密，减少旋转后端优化集中触发
TRAJECTORY_BUILDER_2D.motion_filter.max_distance_meters = 0.15
TRAJECTORY_BUILDER_2D.motion_filter.max_angle_radians = math.rad(1.)
TRAJECTORY_BUILDER_2D.submaps.grid_options_2d.resolution = 0.05 -- 默认0.05 
  








return options

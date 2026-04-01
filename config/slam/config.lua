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
  tracking_frame = "gyro_link",--base_link --跟踪的坐标系，可以是imu、小车、雷达
  --published_frame = "base_footprint",--base_footprint ----cartographer发布pose的坐标
  published_frame = "base_footprint",
  odom_frame = "odom",--cartographer的里程计坐标系
  provide_odom_frame = false,-- cartographer是否发布里程计坐标
  publish_frame_projected_to_2d = false,
  publish_tracked_pose = true,-- 发布机器人globle pose --landmark必要的topic
  use_pose_extrapolator = false,
  use_odometry = false,
  use_nav_sat = false,
  use_landmarks = true,--是否使用landmark
  num_laser_scans = 1,
  num_multi_echo_laser_scans = 0,
  num_subdivisions_per_laser_scan = 1,
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
}

MAP_BUILDER.use_trajectory_builder_2d = true

TRAJECTORY_BUILDER_2D.num_accumulated_range_data = 1
TRAJECTORY_BUILDER_2D.ceres_scan_matcher.occupied_space_weight = 10.
TRAJECTORY_BUILDER_2D.ceres_scan_matcher.translation_weight = 1.
TRAJECTORY_BUILDER_2D.ceres_scan_matcher.rotation_weight = 1.

TRAJECTORY_BUILDER_2D.submaps.num_range_data = 50 --每个submap的关键帧数量

TRAJECTORY_BUILDER_2D.min_range = 0.1 --使用雷达数据的最小范围
TRAJECTORY_BUILDER_2D.max_range = 30. --使用雷达数据的最大范围 --最好不超过雷达有效量程 ，也不要过小 --716mini--25.0; 719---35.0
TRAJECTORY_BUILDER_2D.missing_data_ray_length = 1.
TRAJECTORY_BUILDER_2D.use_imu_data = true --是否使用imu
TRAJECTORY_BUILDER_2D.use_online_correlative_scan_matching = false --发布当前scan数据
TRAJECTORY_BUILDER_2D.real_time_correlative_scan_matcher.linear_search_window = 0.1 
TRAJECTORY_BUILDER_2D.real_time_correlative_scan_matcher.translation_delta_cost_weight = 10.
TRAJECTORY_BUILDER_2D.real_time_correlative_scan_matcher.rotation_delta_cost_weight = 1e-1



POSE_GRAPH.optimization_problem.huber_scale = 1e1--1e2
POSE_GRAPH.optimize_every_n_nodes = 30 --每多少个关键帧优化一次
POSE_GRAPH.global_sampling_ratio = 0.005 --0.003 --对局部子图进行回环检测时的计算频率, 数值越大, 计算次数越多
TRAJECTORY_BUILDER_2D.motion_filter.max_distance_meters = 0.2 --机器人每移动0.2米插入一个关键帧
TRAJECTORY_BUILDER_2D.motion_filter.max_angle_radians = math.rad(1.) --机器人每旋转1度插入一个关键帧
TRAJECTORY_BUILDER_2D.submaps.grid_options_2d.resolution = 0.05 --地图分辨率

POSE_GRAPH.constraint_builder.min_score = 0.6 -- 对局部子图进行回环检测时的最低分数阈值

return options

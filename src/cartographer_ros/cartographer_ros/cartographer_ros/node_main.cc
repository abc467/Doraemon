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

#include <memory>
// #include "absl/memory/memory.h"
#include "cartographer/mapping/map_builder.h"
#include "cartographer_ros/node.h"
#include "cartographer_ros/node_options.h"
#include "cartographer_ros/ros_log_sink.h"
#include "cartographer_ros/visualweb.h"
#include "gflags/gflags.h"
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

int main(int argc, char **argv)
{
  google::InitGoogleLogging(argv[0]);
  ::ros::init(argc, argv, "cartographer_node");
  ::ros::start();

  cartographer_ros::ScopedRosLogSink ros_log_sink;
  ::cartographer_ros::visualweb web;
  web.start();
  while (::ros::ok())
  {
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  }
  ::ros::shutdown();
}

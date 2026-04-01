cmake_minimum_required(VERSION 3.0.2)
project(coverage_msgs)

find_package(catkin REQUIRED COMPONENTS
  message_generation
  actionlib_msgs
  std_msgs
  geometry_msgs
)

## ===== Messages =====
add_message_files(
  FILES
  Polygon2D.msg
  ZoneGeometry.msg
  MapConstraints.msg
  RunProgress.msg
  TaskState.msg
)

## ===== Actions =====
add_action_files(
  FILES
  PlanCoverage.action
)

generate_messages(
  DEPENDENCIES
  std_msgs
  geometry_msgs
  actionlib_msgs
)

catkin_package(
  CATKIN_DEPENDS
    message_runtime
    std_msgs
    geometry_msgs
    actionlib_msgs
)

include_directories(
  ${catkin_INCLUDE_DIRS}
)

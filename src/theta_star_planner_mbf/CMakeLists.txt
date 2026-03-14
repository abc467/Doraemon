cmake_minimum_required(VERSION 3.10)
project(theta_star_planner_mbf)
set(CMAKE_CXX_STANDARD 17)

find_package(catkin REQUIRED COMPONENTS
  angles
  costmap_2d
  geometry_msgs
  mbf_costmap_core
  mbf_msgs
  nav_msgs
  pluginlib
  roscpp
  rospy
  std_msgs
  tf
  tf2
  tf2_geometry_msgs
  tf2_ros
)

catkin_package(
  INCLUDE_DIRS include
  LIBRARIES theta_star_planner_mbf_lib
  CATKIN_DEPENDS
    angles
    costmap_2d
    geometry_msgs
    mbf_costmap_core
    mbf_msgs
    nav_msgs
    pluginlib
    roscpp
    rospy
    std_msgs
    tf
    tf2
    tf2_geometry_msgs
    tf2_ros
)

include_directories(
  include
  ${catkin_INCLUDE_DIRS}
)

add_library(theta_star_planner_mbf_lib src/theta_star.cpp src/theta_star_planner.cpp)
target_link_libraries(theta_star_planner_mbf_lib ${catkin_LIBRARIES})

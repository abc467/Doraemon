cmake_minimum_required(VERSION 3.10)
project(theta_star_planner_mbf)
set(CMAKE_CXX_STANDARD 17)

find_package(catkin REQUIRED COMPONENTS
  angles
  base_local_planner
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
    base_local_planner
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

add_library(theta_star_planner_mbf_lib
  src/theta_star.cpp
  src/theta_star_planner.cpp
  src/se2_path_refiner.cpp)
target_link_libraries(theta_star_planner_mbf_lib ${catkin_LIBRARIES})

if(CATKIN_ENABLE_TESTING)
  add_executable(theta_star_connection_audit
    test/theta_star_connection_audit.cpp)
  target_link_libraries(theta_star_connection_audit
    theta_star_planner_mbf_lib
    ${catkin_LIBRARIES}
  )
  add_dependencies(theta_star_connection_audit ${catkin_EXPORTED_TARGETS})

  catkin_add_gtest(test_theta_star_clearance_regression
    test/test_theta_star_clearance_regression.cpp)
  if(TARGET test_theta_star_clearance_regression)
    target_link_libraries(test_theta_star_clearance_regression
      theta_star_planner_mbf_lib
      ${catkin_LIBRARIES}
    )
    add_dependencies(test_theta_star_clearance_regression ${catkin_EXPORTED_TARGETS})
  endif()
endif()

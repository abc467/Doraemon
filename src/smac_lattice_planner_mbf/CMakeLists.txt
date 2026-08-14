cmake_minimum_required(VERSION 3.10)
project(smac_lattice_planner_mbf)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(catkin REQUIRED COMPONENTS
  angles
  base_local_planner
  costmap_2d
  geometry_msgs
  mbf_costmap_core
  mbf_msgs
  nav_msgs
  pluginlib
  rosbag
  roscpp
  roslib
  theta_star_planner_mbf
  tf
  tf2
)
find_package(nlohmann_json REQUIRED)
find_package(ompl REQUIRED)
find_package(SQLite3 REQUIRED)

catkin_package(
  INCLUDE_DIRS include upstream/nav2_smac_planner/include
  LIBRARIES smac_lattice_planner_mbf
  CATKIN_DEPENDS
    angles
    base_local_planner
    costmap_2d
    geometry_msgs
    mbf_costmap_core
    mbf_msgs
    nav_msgs
    pluginlib
    rosbag
    roscpp
    roslib
    theta_star_planner_mbf
    tf
    tf2
)

include_directories(
  include
  upstream/nav2_smac_planner/include
  ${catkin_INCLUDE_DIRS}
  ${OMPL_INCLUDE_DIRS}
)

add_library(smac_lattice_planner_mbf
  upstream/nav2_smac_planner/src/collision_checker.cpp
  upstream/nav2_smac_planner/src/distance_heuristic.cpp
  upstream/nav2_smac_planner/src/node_hybrid.cpp
  upstream/nav2_smac_planner/src/node_lattice.cpp
  upstream/nav2_smac_planner/src/obstacle_heuristic.cpp
  src/coarse_route_corridor.cpp
  src/state_lattice_smoother.cpp
  src/theta_state_suffix.cpp
  src/smac_lattice_planner.cpp
)
target_link_libraries(smac_lattice_planner_mbf
  ${catkin_LIBRARIES}
  ${OMPL_LIBRARIES}
  nlohmann_json::nlohmann_json
)
add_dependencies(smac_lattice_planner_mbf ${catkin_EXPORTED_TARGETS})

add_executable(smac_lattice_live_block_validator
  src/live_block_validator.cpp)
target_link_libraries(smac_lattice_live_block_validator
  smac_lattice_planner_mbf
  ${catkin_LIBRARIES}
  ${OMPL_LIBRARIES}
  SQLite::SQLite3
)
add_dependencies(smac_lattice_live_block_validator ${catkin_EXPORTED_TARGETS})

if(CATKIN_ENABLE_TESTING)
  catkin_add_gtest(test_official_lattice_port
    test/test_official_lattice_port.cpp)
  if(TARGET test_official_lattice_port)
    target_link_libraries(test_official_lattice_port
      smac_lattice_planner_mbf
      ${catkin_LIBRARIES}
      ${OMPL_LIBRARIES}
    )
  endif()

  catkin_add_gtest(test_coarse_route_corridor
    test/test_coarse_route_corridor.cpp)
  if(TARGET test_coarse_route_corridor)
    target_link_libraries(test_coarse_route_corridor
      smac_lattice_planner_mbf
      ${catkin_LIBRARIES}
      ${OMPL_LIBRARIES}
    )
  endif()

  catkin_add_gtest(test_theta_state_suffix
    test/test_theta_state_suffix.cpp)
  if(TARGET test_theta_state_suffix)
    target_link_libraries(test_theta_state_suffix
      smac_lattice_planner_mbf
      ${catkin_LIBRARIES}
    )
  endif()

  catkin_add_gtest(test_state_lattice_smoother
    test/test_state_lattice_smoother.cpp)
  if(TARGET test_state_lattice_smoother)
    target_link_libraries(test_state_lattice_smoother
      smac_lattice_planner_mbf
      ${catkin_LIBRARIES}
      ${OMPL_LIBRARIES}
    )
  endif()

  add_executable(state_lattice_smoother_bag_replay
    test/state_lattice_smoother_bag_replay.cpp)
  target_link_libraries(state_lattice_smoother_bag_replay
    smac_lattice_planner_mbf
    ${catkin_LIBRARIES}
    ${OMPL_LIBRARIES}
  )
  add_dependencies(state_lattice_smoother_bag_replay ${catkin_EXPORTED_TARGETS})
endif()

install(TARGETS smac_lattice_planner_mbf
  ARCHIVE DESTINATION ${CATKIN_PACKAGE_LIB_DESTINATION}
  LIBRARY DESTINATION ${CATKIN_PACKAGE_LIB_DESTINATION}
  RUNTIME DESTINATION ${CATKIN_PACKAGE_BIN_DESTINATION}
)
install(TARGETS smac_lattice_live_block_validator
  RUNTIME DESTINATION ${CATKIN_PACKAGE_BIN_DESTINATION}
)
install(DIRECTORY include/${PROJECT_NAME}/
  DESTINATION ${CATKIN_PACKAGE_INCLUDE_DESTINATION}
)
install(DIRECTORY include/nav2_core/
  DESTINATION ${CATKIN_GLOBAL_INCLUDE_DESTINATION}/nav2_core
)
install(DIRECTORY include/nav2_costmap_2d/
  DESTINATION ${CATKIN_GLOBAL_INCLUDE_DESTINATION}/nav2_costmap_2d
)
install(DIRECTORY upstream/nav2_smac_planner/include/nav2_smac_planner/
  DESTINATION ${CATKIN_GLOBAL_INCLUDE_DESTINATION}/nav2_smac_planner
)
install(FILES smac_lattice_plugin.xml
  DESTINATION ${CATKIN_PACKAGE_SHARE_DESTINATION}
)
install(DIRECTORY config
  DESTINATION ${CATKIN_PACKAGE_SHARE_DESTINATION}
)
install(FILES LICENSE UPSTREAM.md
  DESTINATION ${CATKIN_PACKAGE_SHARE_DESTINATION}
)

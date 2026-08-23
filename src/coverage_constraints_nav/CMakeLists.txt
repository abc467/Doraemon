cmake_minimum_required(VERSION 3.0.2)
project(coverage_constraints_nav)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(catkin REQUIRED COMPONENTS
  costmap_2d
  coverage_msgs
  geometry_msgs
  pluginlib
  roscpp
  tf2
  tf2_geometry_msgs
  tf2_ros
)

catkin_package(
  INCLUDE_DIRS include
  LIBRARIES coverage_constraints_nav
  CATKIN_DEPENDS
    costmap_2d
    coverage_msgs
    geometry_msgs
    pluginlib
    roscpp
    tf2
    tf2_geometry_msgs
    tf2_ros
)

include_directories(
  include
  ${catkin_INCLUDE_DIRS}
)

add_library(coverage_constraints_nav
  src/keepout_constraint_layer.cpp
  src/static_artifact_clear_layer.cpp
)
target_link_libraries(coverage_constraints_nav
  ${catkin_LIBRARIES}
)

if(CATKIN_ENABLE_TESTING)
  catkin_add_gtest(test_constraint_identity
    test/test_constraint_identity.cpp
  )
  if(TARGET test_constraint_identity)
    target_link_libraries(test_constraint_identity
      coverage_constraints_nav
      ${catkin_LIBRARIES}
    )
  endif()

  catkin_add_gtest(test_static_artifact_clear_layer
    test/test_static_artifact_clear_layer.cpp
  )
  if(TARGET test_static_artifact_clear_layer)
    target_link_libraries(test_static_artifact_clear_layer
      coverage_constraints_nav
      ${catkin_LIBRARIES}
    )
  endif()
endif()

install(TARGETS coverage_constraints_nav
  ARCHIVE DESTINATION ${CATKIN_PACKAGE_LIB_DESTINATION}
  LIBRARY DESTINATION ${CATKIN_PACKAGE_LIB_DESTINATION}
  RUNTIME DESTINATION ${CATKIN_PACKAGE_BIN_DESTINATION}
)

install(DIRECTORY include/${PROJECT_NAME}/
  DESTINATION ${CATKIN_PACKAGE_INCLUDE_DESTINATION}
)

install(FILES plugin.xml
  DESTINATION ${CATKIN_PACKAGE_SHARE_DESTINATION}
)

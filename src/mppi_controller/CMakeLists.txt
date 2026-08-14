cmake_minimum_required(VERSION 3.10)
project(mppi_controller)

set(CMAKE_CXX_STANDARD 17)

find_package(catkin REQUIRED COMPONENTS
  angles
  base_local_planner
  costmap_2d
  diagnostic_msgs
  geometry_msgs
  mbf_abstract_core
  nav_core
  nav_msgs
  pluginlib
  roscpp
  std_msgs
  std_srvs
  tf
  tf2
  tf2_geometry_msgs
)
find_package(Eigen3 REQUIRED)
find_package(OpenMP REQUIRED)
catkin_package(
  INCLUDE_DIRS include
  LIBRARIES mppi_controller_lib mppi_critics_lib
  CATKIN_DEPENDS
    angles base_local_planner costmap_2d diagnostic_msgs geometry_msgs mbf_abstract_core nav_core nav_msgs
    pluginlib roscpp std_msgs std_srvs tf tf2 tf2_geometry_msgs
)

include_directories(
  include
  ${catkin_INCLUDE_DIRS}
)

# 检查并启用 FMA（融合乘加）指令集以加速数学计算
include(CheckCXXCompilerFlag)
check_cxx_compiler_flag("-mfma" COMPILER_SUPPORTS_FMA)
if(COMPILER_SUPPORTS_FMA)
  add_compile_options(-mfma)
endif()
 
add_library(mppi_controller_lib SHARED
  src/mppi_controller.cpp
  src/critic_manager.cpp
  src/noise_generator.cpp
  src/optimal_trajectory_validator.cpp
  src/optimizer.cpp
  src/path_handler.cpp
  src/trajectory_visualizer.cpp)
target_compile_options(mppi_controller_lib PUBLIC -O3)
target_include_directories(mppi_controller_lib  PUBLIC
  "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
  "$<INSTALL_INTERFACE:include/${PROJECT_NAME}>")
target_link_libraries(mppi_controller_lib 
  ${catkin_LIBRARIES})



add_library(mppi_critics_lib SHARED
  src/critics/constraint_critic.cpp
  src/critics/cost_critic.cpp
  src/critics/obstacles_critic.cpp
  src/critics/goal_critic.cpp
  src/critics/goal_angle_critic.cpp
  src/critics/path_align_critic.cpp
  src/critics/path_angle_critic.cpp
  src/critics/path_follow_critic.cpp
  src/critics/prefer_forward_critic.cpp
  src/critics/twirling_critic.cpp
  src/critics/velocity_deadband_critic.cpp
)
target_compile_options(mppi_critics_lib PUBLIC -O3)
target_include_directories(mppi_critics_lib  PUBLIC
  "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
  "$<INSTALL_INTERFACE:include/${PROJECT_NAME}>")

target_link_libraries(mppi_critics_lib
  mppi_controller_lib
  ${catkin_LIBRARIES}
  OpenMP::OpenMP_CXX)

if(CATKIN_ENABLE_TESTING)
  catkin_add_gtest(test_official_mppi_core
    test/test_official_mppi_core.cpp)
  if(TARGET test_official_mppi_core)
    target_link_libraries(test_official_mppi_core
      mppi_controller_lib
      mppi_critics_lib
      ${catkin_LIBRARIES}
    )
    add_dependencies(test_official_mppi_core ${catkin_EXPORTED_TARGETS})
  endif()
endif()


install(TARGETS mppi_controller_lib mppi_critics_lib
  ARCHIVE DESTINATION ${CATKIN_PACKAGE_LIB_DESTINATION}
  LIBRARY DESTINATION ${CATKIN_PACKAGE_LIB_DESTINATION}
  RUNTIME DESTINATION ${CATKIN_PACKAGE_BIN_DESTINATION}
)

install(DIRECTORY include/
  DESTINATION ${CATKIN_PACKAGE_INCLUDE_DESTINATION}
)

install(FILES lp_plugin.xml critics.xml validators.xml UPSTREAM.md
  DESTINATION ${CATKIN_PACKAGE_SHARE_DESTINATION}
)

install(DIRECTORY config/
  DESTINATION ${CATKIN_PACKAGE_SHARE_DESTINATION}/config
)


######TEST######

# add_executable(tf_test
#   test/tf_test.cpp
# )
# target_link_libraries(tf_test ${catkin_LIBRARIES})

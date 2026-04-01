cmake_minimum_required(VERSION 3.0.2)
project(ground_segmentation_tracking)

## Compile as C++17 + �Ż� & ����
add_compile_options(-std=c++17 -O3 -Wall -Wextra)

## Find catkin macros and libraries
find_package(catkin REQUIRED COMPONENTS
  roscpp
  std_msgs
  sensor_msgs
  nav_msgs
  visualization_msgs
  tf
  tf2_ros
  tf2_sensor_msgs
  tf2_eigen
  pcl_ros
  pcl_conversions
)

## PCL
find_package(PCL REQUIRED COMPONENTS common io filters segmentation kdtree surface)

catkin_package(
  CATKIN_DEPENDS
    roscpp std_msgs sensor_msgs nav_msgs visualization_msgs
    tf tf2_ros tf2_sensor_msgs tf2_eigen
    pcl_ros pcl_conversions
  DEPENDS PCL
)

include_directories(
  include
  ${catkin_INCLUDE_DIRS}
  ${PCL_INCLUDE_DIRS}
)


add_executable(gs_node src/ground_segmentation_node_tracking.cpp)
target_link_libraries(gs_node
  ${catkin_LIBRARIES}
  ${PCL_LIBRARIES}
)
add_dependencies(gs_node ${${PROJECT_NAME}_EXPORTED_TARGETS} ${catkin_EXPORTED_TARGETS})



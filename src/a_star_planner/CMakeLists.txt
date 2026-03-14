cmake_minimum_required(VERSION 3.0.2)
project(a_star_planner)

# 查找Catkin和所有必需的ROS组件
find_package(catkin REQUIRED COMPONENTS
  costmap_2d
  geometry_msgs
  mbf_costmap_core
  nav_core
  pluginlib
  roscpp
  tf2_ros
)

# Catkin包的构建配置
catkin_package(
  INCLUDE_DIRS include
  LIBRARIES ${PROJECT_NAME}_lib
  CATKIN_DEPENDS
    costmap_2d
    geometry_msgs
    mbf_costmap_core
    nav_core
    pluginlib
    roscpp
    tf2_ros
)

# 指定头文件所在的目录
include_directories(
  include
  ${catkin_INCLUDE_DIRS}
)

# 将所有源文件编译成一个共享库
# 库的名称为 a_star_planner_lib
add_library(${PROJECT_NAME}_lib
  src/a_star_ros_wrapper.cpp
  src/path_planner.cpp
  src/theta_star_planner.cpp
)

# 链接该库需要的所有依赖项
target_link_libraries(${PROJECT_NAME}_lib
  ${catkin_LIBRARIES}
)

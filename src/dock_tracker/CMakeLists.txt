cmake_minimum_required(VERSION 3.0.2)
project(dock_tracker)

## 1. 查找依赖
find_package(catkin REQUIRED COMPONENTS
  geometry_msgs
  pcl_conversions
  pcl_ros
  roscpp
  sensor_msgs
  std_msgs
  tf
)

## 查找 PCL 库
find_package(PCL REQUIRED)

## 【关键修复】 声明这是一个 catkin 包
## 没有这一行，可执行文件就不会被放到 devel/lib 下！
catkin_package(
  CATKIN_DEPENDS geometry_msgs pcl_conversions pcl_ros roscpp sensor_msgs std_msgs tf
)

## 2. 包含头文件
include_directories(
  ${catkin_INCLUDE_DIRS}
  ${PCL_INCLUDE_DIRS}
)

## 3. 定义可执行文件
add_executable(dock_tracker_node src/dock_tracker_node.cpp)

## 4. 链接库
target_link_libraries(dock_tracker_node
  ${catkin_LIBRARIES}
  ${PCL_LIBRARIES}
)

cmake_minimum_required(VERSION 3.0.2)
project(wheel_speed_odom_bridge)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(catkin REQUIRED COMPONENTS
  geometry_msgs
  nav_msgs
  roscpp
  roslib
  sensor_msgs
  tf2_ros
)

catkin_package(
  CATKIN_DEPENDS geometry_msgs nav_msgs roscpp roslib sensor_msgs tf2_ros
)

include_directories(
  ${catkin_INCLUDE_DIRS}
)

add_executable(wheel_speed_odom_node src/wheel_speed_odom_node.cpp)
add_executable(receiver_cmd_vel_node src/receiver_cmd_vel_node.cpp)
add_executable(ekf_covariance_override_node src/ekf_covariance_override_node.cpp)

target_compile_options(wheel_speed_odom_node PRIVATE
  -finput-charset=UTF-8
  -fexec-charset=UTF-8
)

target_compile_options(receiver_cmd_vel_node PRIVATE
  -finput-charset=UTF-8
  -fexec-charset=UTF-8
)

target_compile_options(ekf_covariance_override_node PRIVATE
  -finput-charset=UTF-8
  -fexec-charset=UTF-8
)

target_link_libraries(wheel_speed_odom_node
  ${catkin_LIBRARIES}
)

target_link_libraries(receiver_cmd_vel_node
  ${catkin_LIBRARIES}
)

target_link_libraries(ekf_covariance_override_node
  ${catkin_LIBRARIES}
)

cmake_minimum_required(VERSION 3.0.2)
project(robot_runtime_flags)

find_package(catkin REQUIRED COMPONENTS
  roscpp
  rospy
  std_msgs
  robot_runtime_flags_msgs
)

catkin_package(
  CATKIN_DEPENDS roscpp rospy std_msgs robot_runtime_flags_msgs
)

include_directories(
  ${catkin_INCLUDE_DIRS}
)

add_executable(runtime_flag_server_node src/runtime_flag_server_node.cpp)
add_dependencies(runtime_flag_server_node ${${PROJECT_NAME}_EXPORTED_TARGETS} ${catkin_EXPORTED_TARGETS})
target_link_libraries(runtime_flag_server_node
  ${catkin_LIBRARIES}
)

install(TARGETS runtime_flag_server_node
  RUNTIME DESTINATION ${CATKIN_PACKAGE_BIN_DESTINATION}
)

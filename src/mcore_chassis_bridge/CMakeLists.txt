cmake_minimum_required(VERSION 3.0.2)
project(mcore_chassis_bridge)

find_package(catkin REQUIRED COMPONENTS
  geometry_msgs
  roscpp
  std_msgs
)

catkin_package(
  CATKIN_DEPENDS geometry_msgs roscpp std_msgs
)

include_directories(${catkin_INCLUDE_DIRS})

add_compile_options(-std=c++11)

add_executable(mcore_velocity_sender_node
  src/mcore_velocity_sender_node.cpp
)
add_dependencies(mcore_velocity_sender_node ${catkin_EXPORTED_TARGETS})
target_link_libraries(mcore_velocity_sender_node ${catkin_LIBRARIES})

install(TARGETS mcore_velocity_sender_node
  RUNTIME DESTINATION ${CATKIN_PACKAGE_BIN_DESTINATION}
)

install(DIRECTORY launch
  DESTINATION ${CATKIN_PACKAGE_SHARE_DESTINATION}
)

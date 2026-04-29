cmake_minimum_required(VERSION 2.8.3)
project(turn_on_wheeltec_robot)

find_package(catkin REQUIRED COMPONENTS
  geometry_msgs
  message_generation
  nav_msgs
  roscpp
  serial
  sensor_msgs
  std_msgs
  tf
  turtlesim
)

add_message_files(FILES supersonic.msg)

generate_messages(DEPENDENCIES std_msgs)

# ulimit -c unlimited
# ssh firefly@192.168.1.76
add_compile_options(-g -std=c++11)    #-g -> core dumped

catkin_package(
  CATKIN_DEPENDS geometry_msgs message_runtime nav_msgs roscpp serial sensor_msgs std_msgs tf turtlesim
)

# defind robot status massage
# command atkin_make -DCATKIN_WHITELIST_PACKAGES="new_msg"
include_directories(include ${catkin_INCLUDE_DIRS})

add_executable(wheeltec_robot_node src/wheeltec_robot.cpp src/Quaternion_Solution.cpp)
add_dependencies(wheeltec_robot_node ${${PROJECT_NAME}_EXPORTED_TARGETS} ${catkin_EXPORTED_TARGETS})
target_link_libraries(wheeltec_robot_node ${catkin_LIBRARIES})

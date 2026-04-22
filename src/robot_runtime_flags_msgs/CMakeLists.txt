cmake_minimum_required(VERSION 3.0.2)
project(robot_runtime_flags_msgs)

find_package(catkin REQUIRED COMPONENTS
  std_msgs
  message_generation
)

add_message_files(
  FILES
  ParamChanges.msg
)

add_service_files(
  FILES
  SetParam.srv
  GetParam.srv
)

generate_messages(
  DEPENDENCIES
  std_msgs
)

catkin_package(
  CATKIN_DEPENDS std_msgs message_runtime
)

include_directories(
  ${catkin_INCLUDE_DIRS}
)

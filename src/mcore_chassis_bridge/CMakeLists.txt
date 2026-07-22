cmake_minimum_required(VERSION 3.0.2)
project(mcore_chassis_bridge)

find_package(catkin REQUIRED COMPONENTS
  geometry_msgs
  robot_platform_msgs
  rospy
  roscpp
  sensor_msgs
  std_msgs
)

catkin_package(
  INCLUDE_DIRS include
  CATKIN_DEPENDS geometry_msgs robot_platform_msgs rospy roscpp sensor_msgs std_msgs
)

include_directories(include ${catkin_INCLUDE_DIRS})

add_compile_options(-std=c++11)

add_executable(mcore_velocity_sender_node
  src/mcore_velocity_sender_node.cpp
)
add_dependencies(mcore_velocity_sender_node ${catkin_EXPORTED_TARGETS})
target_link_libraries(mcore_velocity_sender_node ${catkin_LIBRARIES})

if(CATKIN_ENABLE_TESTING)
  catkin_add_gtest(test_velocity_command_units
    test/test_velocity_command_units.cpp
  )
endif()

install(TARGETS mcore_velocity_sender_node
  RUNTIME DESTINATION ${CATKIN_PACKAGE_BIN_DESTINATION}
)

install(DIRECTORY include/${PROJECT_NAME}/
  DESTINATION ${CATKIN_PACKAGE_INCLUDE_DESTINATION}
)

catkin_install_python(PROGRAMS
  scripts/mcore_realtime_cleaning_test_node.py
  DESTINATION ${CATKIN_PACKAGE_BIN_DESTINATION}
)

install(DIRECTORY launch
  DESTINATION ${CATKIN_PACKAGE_SHARE_DESTINATION}
)

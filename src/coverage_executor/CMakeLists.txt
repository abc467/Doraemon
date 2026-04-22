cmake_minimum_required(VERSION 3.0.2)
project(coverage_executor)

find_package(catkin REQUIRED COMPONENTS
  rospy
  actionlib
  std_msgs
  sensor_msgs
  geometry_msgs
  nav_msgs
  tf2_ros
  tf2_geometry_msgs
  mbf_msgs
  robot_platform_msgs
  coverage_msgs
  coverage_planner
)

catkin_python_setup()

catkin_package()

catkin_install_python(PROGRAMS
  scripts/executor_node.py
  DESTINATION ${CATKIN_PACKAGE_BIN_DESTINATION}
)

install(DIRECTORY launch
  DESTINATION ${CATKIN_PACKAGE_SHARE_DESTINATION}
)

install(DIRECTORY config
  DESTINATION ${CATKIN_PACKAGE_SHARE_DESTINATION}
)

if(CATKIN_ENABLE_TESTING)
  catkin_add_nosetests(test)
endif()

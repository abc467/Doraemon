cmake_minimum_required(VERSION 3.0.2)
project(coverage_task_manager)

find_package(catkin REQUIRED COMPONENTS
  rospy
  actionlib
  std_msgs
  std_srvs
  sensor_msgs
  tf2_ros
  diagnostic_updater
  diagnostic_msgs
  geometry_msgs
  mbf_msgs
  coverage_msgs
  coverage_planner
  cleanrobot_app_msgs
  robot_platform_msgs
)

catkin_python_setup()

catkin_package(
  CATKIN_DEPENDS rospy actionlib std_msgs std_srvs sensor_msgs tf2_ros diagnostic_updater diagnostic_msgs geometry_msgs mbf_msgs coverage_msgs coverage_planner cleanrobot_app_msgs robot_platform_msgs
)

catkin_install_python(PROGRAMS
  scripts/task_manager_node.py
  scripts/task_api_service_node.py
  scripts/schedule_api_service_node.py
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

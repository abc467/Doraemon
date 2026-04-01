cmake_minimum_required(VERSION 3.0.2)
project(robot_hw_bridge)

find_package(catkin REQUIRED COMPONENTS
  rospy
  std_msgs
  geometry_msgs
  sensor_msgs
  std_srvs
  actionlib
  actionlib_msgs
  my_msg_srv
  my_docking_controller
)

catkin_package()

catkin_install_python(PROGRAMS
  scripts/mcore_tcp_bridge.py
  scripts/station_tcp_bridge.py
  scripts/dock_supply_manager.py
  DESTINATION ${CATKIN_PACKAGE_BIN_DESTINATION}
)

install(DIRECTORY launch
  DESTINATION ${CATKIN_PACKAGE_SHARE_DESTINATION}
)

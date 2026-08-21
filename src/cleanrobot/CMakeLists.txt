cmake_minimum_required(VERSION 3.10)
project(cleanrobot)

find_package(catkin REQUIRED)

catkin_package()

catkin_install_python(PROGRAMS
  scripts/orbbec_camera_watchdog_node.py
  DESTINATION ${CATKIN_PACKAGE_BIN_DESTINATION}
)

install(DIRECTORY launch/
  DESTINATION ${CATKIN_PACKAGE_SHARE_DESTINATION}/launch
  USE_SOURCE_PERMISSIONS
  OPTIONAL
  PATTERN "cartographer_localization.launch" EXCLUDE
  PATTERN "cartographer_mapping.launch" EXCLUDE
)

install(DIRECTORY config
  DESTINATION ${CATKIN_PACKAGE_SHARE_DESTINATION}
  USE_SOURCE_PERMISSIONS
  OPTIONAL
)

if(CATKIN_ENABLE_TESTING)
  catkin_add_nosetests(test/test_orbbec_runtime_contract.py)
endif()

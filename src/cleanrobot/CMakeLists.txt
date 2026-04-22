cmake_minimum_required(VERSION 3.10)
project(cleanrobot)

find_package(catkin REQUIRED)

catkin_package()

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

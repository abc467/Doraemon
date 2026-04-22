cmake_minimum_required(VERSION 3.10)

project(cleanrobot_description)

find_package(catkin REQUIRED)

catkin_package()

find_package(roslaunch)

foreach(dir launch meshes urdf)
  install(DIRECTORY ${dir}/
    DESTINATION ${CATKIN_PACKAGE_SHARE_DESTINATION}/${dir})
endforeach()

cmake_minimum_required(VERSION 3.0.2)
project(modbus_imu_driver)

add_compile_options(-std=c++11)

find_package(catkin REQUIRED COMPONENTS
  roscpp
  sensor_msgs
  serial
  tf
  xmlrpcpp
)

catkin_package()

include_directories(
  ${catkin_INCLUDE_DIRS}
)

add_executable(modbus_imu_serial_node src/modbus_imu_serial_node.cpp)
target_link_libraries(modbus_imu_serial_node ${catkin_LIBRARIES})

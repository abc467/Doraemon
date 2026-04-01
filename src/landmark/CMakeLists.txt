cmake_minimum_required(VERSION 3.0.2)
project(landmark)

find_package(catkin REQUIRED COMPONENTS
  roscpp
  rospy
  std_msgs
  message_generation
  sensor_msgs
  tf2_ros
  tf2_geometry_msgs
  cartographer_ros_msgs
  visualization_msgs
  param_space
)
 
# generate_messages(
#   DEPENDENCIES
#   std_msgs
#   sensor_msgs
#   geometry_msgs
# )
 
catkin_package(
#  INCLUDE_DIRS include
#  LIBRARIES pub_landmark
 CATKIN_DEPENDS roscpp rospy std_msgs message_runtime
#  DEPENDS system_lib
)
 
include_directories(
  include
  ${catkin_INCLUDE_DIRS}
)
 
 
 
add_executable(pub_landmark src/pub_landmark.cpp)
 
add_dependencies(pub_landmark ${${PROJECT_NAME}_EXPORTED_TARGETS} ${catkin_EXPORTED_TARGETS})
 
target_link_libraries(pub_landmark ${catkin_LIBRARIES})

# add_executable(pub_landmark_719 src/pub_landmark_719.cpp)
 
# add_dependencies(pub_landmark_719 ${${PROJECT_NAME}_EXPORTED_TARGETS} ${catkin_EXPORTED_TARGETS})
 
# target_link_libraries(pub_landmark_719 ${catkin_LIBRARIES})



# Define executable
add_executable(tcp_server_node src/tcp_server_node.cpp)

# Link libraries
target_link_libraries(tcp_server_node
  ${catkin_LIBRARIES}
)

# Install the executable
install(TARGETS pub_landmark tcp_server_node
  RUNTIME DESTINATION ${CATKIN_PACKAGE_BIN_DESTINATION}
)

install(DIRECTORY launch/
  DESTINATION ${CATKIN_PACKAGE_SHARE_DESTINATION}/launch
)

install(DIRECTORY config/
  DESTINATION ${CATKIN_PACKAGE_SHARE_DESTINATION}/config
)
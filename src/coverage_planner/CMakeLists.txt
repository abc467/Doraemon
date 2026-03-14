cmake_minimum_required(VERSION 3.0.2)
project(coverage_planner)

find_package(catkin REQUIRED COMPONENTS
  rospy
  std_msgs
  std_srvs
  geometry_msgs
  nav_msgs
  visualization_msgs
  actionlib
  actionlib_msgs
  coverage_msgs
  my_msg_srv
)

catkin_package(
  CATKIN_DEPENDS
    rospy std_msgs std_srvs geometry_msgs nav_msgs visualization_msgs actionlib actionlib_msgs coverage_msgs my_msg_srv
)

include_directories(
  ${catkin_INCLUDE_DIRS}
)

############
## Python ##
############

# Install python package in src/coverage_planner so it can be imported after source devel/setup.bash
catkin_python_setup()

# Install executable python scripts
catkin_install_python(PROGRAMS
  scripts/cartographer_runtime_manager_node.py
  scripts/map_asset_manager_node.py
  scripts/map_asset_service_node.py
  scripts/static_map_manager_node.py
  scripts/planner_server_node.py
  scripts/map_constraints_node.py
  tools/import_map_assets.py
  tools/migrate_map_assets.py
  tools/map_constraints_tool.py
  tools/test_plan_coverage.py
  DESTINATION ${CATKIN_PACKAGE_BIN_DESTINATION}
)

# Install launch/config if you have them
install(DIRECTORY launch config
  DESTINATION ${CATKIN_PACKAGE_SHARE_DESTINATION}
  USE_SOURCE_PERMISSIONS
  OPTIONAL
)

cmake_minimum_required(VERSION 3.0.2)
project(coverage_planner)

find_package(catkin REQUIRED COMPONENTS
  rospy
  std_msgs
  std_srvs
  tf2_ros
  tf2_geometry_msgs
  geometry_msgs
  nav_msgs
  sensor_msgs
  visualization_msgs
  actionlib
  actionlib_msgs
  cartographer_ros_msgs
  robot_runtime_flags
  robot_runtime_flags_msgs
  cleanrobot_app_msgs
  cleanrobot_site_msgs
  coverage_msgs
)

catkin_package(
  CATKIN_DEPENDS
    rospy std_msgs std_srvs tf2_ros tf2_geometry_msgs geometry_msgs nav_msgs sensor_msgs visualization_msgs actionlib actionlib_msgs cartographer_ros_msgs robot_runtime_flags robot_runtime_flags_msgs cleanrobot_app_msgs cleanrobot_site_msgs coverage_msgs
)

include_directories(
  ${catkin_INCLUDE_DIRS}
)

############
## Python ##
############

# Install python package in src/coverage_planner so it can be imported from install/devel workspace setups
catkin_python_setup()

# Install executable python scripts
catkin_install_python(PROGRAMS
  scripts/slam_runtime_manager_node.py
  scripts/odometry_health_node.py
  scripts/localization_lifecycle_manager_node.py
  scripts/slam_api_service_node.py
  scripts/map_asset_service_node.py
  scripts/planner_server_node.py
  scripts/profile_catalog_service_node.py
  scripts/map_constraints_node.py
  scripts/rect_zone_planner_node.py
  scripts/site_editor_service_node.py
  tools/import_map_assets.py
  tools/migrate_map_assets.py
  tools/check_ros_contracts.py
  tools/check_revision_db_health.py
  tools/run_backend_runtime_smoke.py
  tools/run_backend_production_acceptance.py
  tools/run_revision_workflow_acceptance.py
  DESTINATION ${CATKIN_PACKAGE_BIN_DESTINATION}
)

# Install launch/config if you have them
install(DIRECTORY launch config
  DESTINATION ${CATKIN_PACKAGE_SHARE_DESTINATION}
  USE_SOURCE_PERMISSIONS
  OPTIONAL
)

if(CATKIN_ENABLE_TESTING)
  catkin_add_nosetests(test)
endif()

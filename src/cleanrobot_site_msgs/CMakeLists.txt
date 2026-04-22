cmake_minimum_required(VERSION 3.0.2)
project(cleanrobot_site_msgs)

find_package(catkin REQUIRED COMPONENTS
  geometry_msgs
  message_generation
)

add_message_files(
  FILES
  CoverageZone.msg
  MapAlignmentConfig.msg
  MapNoGoArea.msg
  MapVirtualWall.msg
  PolygonRegion.msg
  PolygonRing.msg
)

add_service_files(
  FILES
  CommitCoverageRegion.srv
  ConfirmMapAlignmentByPoints.srv
  ConfirmRectCoveragePlan.srv
  GetZonePlanPath.srv
  OperateCoverageZone.srv
  OperateMapNoGoArea.srv
  OperateMapAlignment.srv
  OperateMapVirtualWall.srv
  PreviewCoverageRegion.srv
  PreviewAlignedRectSelection.srv
)

generate_messages(
  DEPENDENCIES
  geometry_msgs
)

catkin_package(
  CATKIN_DEPENDS geometry_msgs message_runtime
)

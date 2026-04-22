cmake_minimum_required(VERSION 3.0.2)
project(cleanrobot_app_msgs)

find_package(catkin REQUIRED COMPONENTS
  geometry_msgs
  message_generation
  nav_msgs
)

add_message_files(
  FILES
  CoverageZone.msg
  CleanSchedule.msg
  CleanTask.msg
  CleanTaskCell.msg
  MapAlignmentConfig.msg
  MapNoGoArea.msg
  MapVirtualWall.msg
  OdometryState.msg
  PgmData.msg
  PolygonRegion.msg
  PolygonRing.msg
  ProfileOption.msg
  SlamJobState.msg
  SlamState.msg
  SubsystemReadiness.msg
  SystemReadiness.msg
)

add_service_files(
  FILES
  CommitCoverageRegion.srv
  ConfirmMapAlignmentByPoints.srv
  ExeTask.srv
  GetOdometryStatus.srv
  GetProfileCatalog.srv
  GetSlamJob.srv
  GetSlamStatus.srv
  GetSystemReadiness.srv
  GetZonePlanPath.srv
  OperateCoverageZone.srv
  OperateMap.srv
  OperateMapAlignment.srv
  OperateMapNoGoArea.srv
  OperateMapVirtualWall.srv
  OperateSchedule.srv
  OperateSlamRuntime.srv
  OperateTask.srv
  PreviewAlignedRectSelection.srv
  PreviewCoverageRegion.srv
  RestartLocalization.srv
  SubmitSlamCommand.srv
)

generate_messages(
  DEPENDENCIES
  geometry_msgs
  nav_msgs
)

catkin_package(
  CATKIN_DEPENDS geometry_msgs message_runtime nav_msgs
)

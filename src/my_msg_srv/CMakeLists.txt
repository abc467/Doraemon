cmake_minimum_required(VERSION 3.0.2)
project(my_msg_srv)

find_package(catkin REQUIRED COMPONENTS
  message_generation
  geometry_msgs
  nav_msgs
  roscpp
  rospy
  std_msgs
)

add_message_files(
   FILES
   PgmData.msg
   VirtualWall.msg
   ProhibitionArea.msg
   CleanTaskCell.msg
   CleanTask.msg
   CleanSchedule.msg
   CoverageZone.msg
   ProfileOption.msg
   MapNoGoArea.msg
   MapAlignmentConfig.msg
   MapVirtualWall.msg
   SubsystemReadiness.msg
   SystemReadiness.msg
   PolygonRing.msg
   PolygonRegion.msg
   TaskProgress.msg
   TeachPath.msg
   CcppPath.msg

   UltrasonicDistance.msg
   ControlCleanTools.msg
   ControlMotor.msg
   ControlWaterTap.msg
   CleaningParams.msg
   CombinedStatus.msg

   ControlStation.msg
   StationStatus.msg
   ControlStationPower.msg
   StationPowerStatus.msg

   ControlVoice.msg
   User.msg
   RobotPose.msg
)

add_service_files(
   FILES
   ConfirmRectCoveragePlan.srv
   ActivateMapAsset.srv
   RelocalizeMapAsset.srv
   RestartLocalization.srv
   MappingControl.srv
   OperateVirtualWall.srv
   OperateProhibitionArea.srv
   OperateMap.srv
   OperateCcppPath.srv
   GenerateCcppPath.srv
   OperateTask.srv
   OperateSchedule.srv
   OperateCoverageZone.srv
   OperateMapNoGoArea.srv
   OperateMapAlignment.srv
   OperateMapVirtualWall.srv
   ConfirmMapAlignmentByPoints.srv
   PreviewAlignedRectSelection.srv
   PreviewCoverageRegion.srv
   CommitCoverageRegion.srv
   GetZonePlanPath.srv
   GetProfileCatalog.srv
   GetSystemReadiness.srv
   ExeTask.srv
   TeachMode.srv
   OperateParkingPoint.srv
   test_ccpp.srv
   OperateTeachPath.srv

   UserManagement.srv
  #  GetPathInZone.srv
  #  GetPath.srv

   start_line_extraction.srv

   test_protobuf.srv
   test_gene_cpath.srv
   test_vwall.srv
)

generate_messages(
  DEPENDENCIES
  std_msgs
  geometry_msgs
  nav_msgs
)

catkin_package(
#  INCLUDE_DIRS include
#  LIBRARIES my_msg_srv
 CATKIN_DEPENDS message_runtime
#  DEPENDS system_lib
)


include_directories(
# include
  ${catkin_INCLUDE_DIRS}
)

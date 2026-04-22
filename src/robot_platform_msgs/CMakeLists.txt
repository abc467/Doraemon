cmake_minimum_required(VERSION 3.0.2)
project(robot_platform_msgs)

find_package(catkin REQUIRED COMPONENTS
  message_generation
)

add_message_files(
  FILES
  CleaningParams.msg
  CombinedStatus.msg
  ControlCleanTools.msg
  ControlMotor.msg
  ControlStation.msg
  ControlStationPower.msg
  ControlVoice.msg
  ControlWaterTap.msg
  StationPowerStatus.msg
  StationStatus.msg
  UltrasonicDistance.msg
)

generate_messages()

catkin_package(
  CATKIN_DEPENDS message_runtime
)

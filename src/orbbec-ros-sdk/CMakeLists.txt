cmake_minimum_required(VERSION 3.0.2)
project(orbbec_camera)

# Compiler settings
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC -O3")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC -O3")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fPIC -g3")
set(CMAKE_BUILD_TYPE "Release")


option(ENABLE_SANITIZER "Enable sanitizer options" OFF)

if (ENABLE_SANITIZER)
    set(SANITIZER_FLAGS "-fsanitize=address -static-libasan ")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${SANITIZER_FLAGS}")

    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${SANITIZER_FLAGS}")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} ${SANITIZER_FLAGS}")
endif()

# Options
option(USE_RK_HW_DECODER "Use Rockchip hardware decoder" OFF)
option(USE_NV_HW_DECODER "Use Nvidia hardware decoder" OFF)
# Detect machine type
execute_process(COMMAND uname -m OUTPUT_VARIABLE MACHINES)
execute_process(COMMAND getconf LONG_BIT OUTPUT_VARIABLE MACHINES_BIT)
if ((${MACHINES} MATCHES "x86_64") AND (${MACHINES_BIT} MATCHES "64"))
  set(HOST_PLATFORM "x64")
elseif (${MACHINES} MATCHES "arm")
  set(HOST_PLATFORM "arm32")
elseif ((${MACHINES} MATCHES "aarch64") AND (${MACHINES_BIT} MATCHES "64"))
  set(HOST_PLATFORM "arm64")
elseif ((${MACHINES} MATCHES "aarch64") AND (${MACHINES_BIT} MATCHES "32"))
  set(HOST_PLATFORM "arm32")
endif ()

# Paths
set(ORBBEC_VENDOR_LIBS_DIR ${CMAKE_CURRENT_SOURCE_DIR}/SDK/lib/${HOST_PLATFORM})
set(ORBBEC_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/SDK/include/)

# Never patch the checked-in SDK binaries. Copy the selected architecture into
# the package build tree and remove unsafe empty RPATH components there. The
# hardened copies are the only vendor binaries used for linking or installation.
find_program(ORBBEC_PATCHELF_EXECUTABLE NAMES patchelf)
find_program(ORBBEC_BASH_EXECUTABLE NAMES bash)
if (NOT ORBBEC_PATCHELF_EXECUTABLE OR NOT ORBBEC_BASH_EXECUTABLE)
  message(FATAL_ERROR "patchelf and bash are required to harden Orbbec SDK RPATHs")
endif ()

set(ORBBEC_RPATH_HARDENER
  ${CMAKE_CURRENT_SOURCE_DIR}/scripts/harden_elf_rpath.sh)
set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS
  ${ORBBEC_RPATH_HARDENER})
set(ORBBEC_LIBS_DIR
  ${CMAKE_CURRENT_BINARY_DIR}/hardened-sdk/lib/${HOST_PLATFORM})
file(MAKE_DIRECTORY ${ORBBEC_LIBS_DIR})
file(GLOB ORBBEC_VENDOR_LIBRARY_ENTRIES
  ${ORBBEC_VENDOR_LIBS_DIR}/*.so
  ${ORBBEC_VENDOR_LIBS_DIR}/*.so.*)
foreach (ORBBEC_VENDOR_LIBRARY ${ORBBEC_VENDOR_LIBRARY_ENTRIES})
  get_filename_component(ORBBEC_VENDOR_LIBRARY_NAME
    ${ORBBEC_VENDOR_LIBRARY} NAME)
  set(ORBBEC_HARDENED_LIBRARY
    ${ORBBEC_LIBS_DIR}/${ORBBEC_VENDOR_LIBRARY_NAME})
  if (IS_SYMLINK ${ORBBEC_VENDOR_LIBRARY})
    file(READ_SYMLINK ${ORBBEC_VENDOR_LIBRARY} ORBBEC_VENDOR_LINK_TARGET)
    file(REMOVE ${ORBBEC_HARDENED_LIBRARY})
    execute_process(
      COMMAND ${CMAKE_COMMAND} -E create_symlink
        ${ORBBEC_VENDOR_LINK_TARGET} ${ORBBEC_HARDENED_LIBRARY}
      RESULT_VARIABLE ORBBEC_LINK_RESULT)
    if (NOT ORBBEC_LINK_RESULT EQUAL 0)
      message(FATAL_ERROR
        "failed to copy Orbbec SDK symlink ${ORBBEC_VENDOR_LIBRARY_NAME}")
    endif ()
  else ()
    configure_file(${ORBBEC_VENDOR_LIBRARY} ${ORBBEC_HARDENED_LIBRARY} COPYONLY)
    execute_process(
      COMMAND ${CMAKE_COMMAND} -E env
        PATCHELF=${ORBBEC_PATCHELF_EXECUTABLE}
        ${ORBBEC_BASH_EXECUTABLE} ${ORBBEC_RPATH_HARDENER}
        ${ORBBEC_HARDENED_LIBRARY}
      RESULT_VARIABLE ORBBEC_HARDEN_RESULT)
    if (NOT ORBBEC_HARDEN_RESULT EQUAL 0)
      message(FATAL_ERROR
        "failed to harden Orbbec SDK library ${ORBBEC_VENDOR_LIBRARY_NAME}")
    endif ()
  endif ()
endforeach ()

# CMake RPATH variables are lists. list(APPEND) avoids manufacturing a leading
# empty component when the inherited list is empty.
list(APPEND CMAKE_BUILD_RPATH ${ORBBEC_LIBS_DIR})
list(APPEND CMAKE_INSTALL_RPATH ${ORBBEC_LIBS_DIR})
if (USE_NV_HW_DECODER)
  set(JETSON_MULTI_MEDIA_API_DIR /usr/src/jetson_multimedia_api)
  set(JETSON_MULTI_MEDIA_API_CLASS_DIR ${JETSON_MULTI_MEDIA_API_DIR}/samples/common/classes)
  set(JETSON_MULTI_MEDIA_API_INCLUDE_DIR ${JETSON_MULTI_MEDIA_API_DIR}/include/)
  set(LIBJPEG8B_INCLUDE_DIR ${JETSON_MULTI_MEDIA_API_INCLUDE_DIR}/libjpeg-8b)
  set(TEGRA_ARMABI /usr/lib/aarch64-linux-gnu/)
  set(NV_LIBRARIES
    -lnvjpeg -lnvbufsurface -lnvbufsurftransform -lyuv -lv4l2
  )
  list(APPEND NV_LIBRARIES
    -L${TEGRA_ARMABI} -L${TEGRA_ARMABI}/tegra)
endif ()
# Dependencies
find_package(catkin REQUIRED
  camera_info_manager
  cv_bridge
  dynamic_reconfigure
  image_geometry
  image_transport
  message_filters
  message_generation
  roscpp
  sensor_msgs
  std_srvs
  tf2
  tf2_ros
  pluginlib
  nodelet
  diagnostic_updater
)
set(SERVICE_FILES
  GetBool.srv
  SetBool.srv
  GetCameraInfo.srv
  GetCameraParams.srv
  GetDeviceInfo.srv
  GetInt32.srv
  GetString.srv
  SetInt32.srv
  SetString.srv
)

find_package(OpenCV REQUIRED)
find_package(Eigen3 REQUIRED)
find_package(Boost REQUIRED)
find_package(PkgConfig REQUIRED)
find_package(Threads REQUIRED)
find_package(Boost COMPONENTS filesystem system REQUIRED)
# check if boost was found
if(Boost_FOUND)
    message(STATUS "Boost found!")
    message(STATUS "Boost_INCLUDE_DIRS: ${Boost_INCLUDE_DIRS}")
    message(STATUS "Boost_LIBRARIES: ${Boost_LIBRARIES}")
else()
    message(FATAL_ERROR "Boost not found!")
endif()

# Additional dependencies based on options
if (USE_RK_HW_DECODER)
  pkg_search_module(RK_MPP REQUIRED rockchip_mpp)
  if (NOT RK_MPP_FOUND)
    message(FATAL_ERROR "Rockchip MPP not found")
  endif ()
  pkg_search_module(RGA librga)
  if (NOT RGA_FOUND)
    message(STATUS "Rockchip RGA not found, use libyuv instead")
    add_definitions(-DUSE_LIBYUV)
    add_compile_options(-lyuv)
  endif ()
endif ()

# Message generation
add_message_files(FILES DeviceInfo.msg Extrinsics.msg Metadata.msg IMUInfo.msg)
add_service_files(FILES ${SERVICE_FILES})
generate_messages(DEPENDENCIES std_msgs sensor_msgs)

# Catkin package
catkin_package(
  INCLUDE_DIRS include
  LIBRARIES ${PROJECT_NAME}
  CATKIN_DEPENDS
  camera_info_manager
  cv_bridge
  dynamic_reconfigure
  image_geometry
  image_transport
  message_filters
  message_runtime
  roscpp
  sensor_msgs
  std_srvs
  tf2
  tf2_ros
)

# Include directories
set(COMMON_INCLUDE_DIRS
  $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/include>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>
  ${catkin_INCLUDE_DIRS}
  ${Boost_INCLUDE_DIRS}
  ${OpenCV_INCLUDED_DIRS}
  ${ORBBEC_INCLUDE_DIR}
)
if (USE_NV_HW_DECODER)
  list(APPEND COMMON_INCLUDE_DIRS
    ${JETSON_MULTI_MEDIA_API_INCLUDE_DIR}
    ${LIBJPEG8B_INCLUDE_DIR}
  )
endif ()

if (USE_RK_HW_DECODER)
  list(APPEND COMMON_INCLUDE_DIRS
    ${RK_MPP_INCLUDE_DIRS}
    ${RGA_INCLUDE_DIR})
endif ()

# Source files
set(SOURCE_FILES
  src/d2c_viewer.cpp
  src/ob_camera_node.cpp
  src/ob_camera_node_driver.cpp
  src/ros_sensor.cpp
  src/ros_service.cpp
  src/utils.cpp
  src/ros_setup.cpp
  src/jpeg_decoder.cpp
)

# Additional source files based on options
if (USE_RK_HW_DECODER)
  add_definitions(-DUSE_RK_HW_DECODER)
  list(APPEND SOURCE_FILES src/rk_mpp_decoder.cpp)
endif ()


if (USE_NV_HW_DECODER)
  add_definitions(-DUSE_NV_HW_DECODER)
  list(APPEND SOURCE_FILES src/jetson_nv_decoder.cpp)
  # append jetson_multimedia_api source files
  list(APPEND SOURCE_FILES
    ${JETSON_MULTI_MEDIA_API_CLASS_DIR}/NvBuffer.cpp
    ${JETSON_MULTI_MEDIA_API_CLASS_DIR}/NvElement.cpp
    ${JETSON_MULTI_MEDIA_API_CLASS_DIR}/NvElementProfiler.cpp
    ${JETSON_MULTI_MEDIA_API_CLASS_DIR}/NvJpegDecoder.cpp
    ${JETSON_MULTI_MEDIA_API_CLASS_DIR}/NvJpegEncoder.cpp
    ${JETSON_MULTI_MEDIA_API_CLASS_DIR}/NvLogging.cpp
    ${JETSON_MULTI_MEDIA_API_CLASS_DIR}/NvUtils.cpp
    ${JETSON_MULTI_MEDIA_API_CLASS_DIR}/NvV4l2Element.cpp
    ${JETSON_MULTI_MEDIA_API_CLASS_DIR}/NvV4l2ElementPlane.cpp
    ${JETSON_MULTI_MEDIA_API_CLASS_DIR}/NvVideoDecoder.cpp
    ${JETSON_MULTI_MEDIA_API_CLASS_DIR}/NvVideoEncoder.cpp
    ${JETSON_MULTI_MEDIA_API_CLASS_DIR}/NvBufSurface.cpp
  )
endif ()

# Link libraries
set(COMMON_LINK_LIBRARIES
  ${catkin_LIBRARIES}
  ${OpenCV_LIBS}
  ${Boost_LIBRARIES}
  -lOrbbecSDK
  -L${ORBBEC_LIBS_DIR}
  Threads::Threads
  -lrt
)

if (USE_RK_HW_DECODER)
  list(APPEND COMMON_LINK_LIBRARIES
    ${RK_MPP_LIBRARIES}
    ${RGA_LIBRARIES}
  )
elseif (USE_NV_HW_DECODER)
  list(APPEND COMMON_LINK_LIBRARIES
    ${NV_LIBRARIES}
  )
endif ()

# The isolated catkin devel layout and the install layout both place the
# package's shared libraries in lib/, while node executables live one level
# below it. Keep hardened vendor libraries alongside the package library so
# every runtime lookup can be expressed relative to the loading ELF.
set(ORBBEC_DEVEL_LIB_DIR
  ${CATKIN_DEVEL_PREFIX}/${CATKIN_PACKAGE_LIB_DESTINATION})
add_custom_target(orbbec_hardened_sdk_copy ALL
  COMMAND ${CMAKE_COMMAND}
    -DSOURCE_DIR=${ORBBEC_LIBS_DIR}
    -DDESTINATION_DIR=${ORBBEC_DEVEL_LIB_DIR}
    -P ${CMAKE_CURRENT_SOURCE_DIR}/cmake/copy_hardened_sdk.cmake
  COMMENT "Copying hardened Orbbec SDK libraries into the catkin devel space"
  VERBATIM)

function(harden_orbbec_target TARGET_NAME TARGET_RPATH)
  # BUILD_WITH_INSTALL_RPATH suppresses CMake's absolute build-tree RPATH.
  # The same relative layout is valid in isolated devel and install spaces.
  set_target_properties(${TARGET_NAME} PROPERTIES
    BUILD_WITH_INSTALL_RPATH TRUE
    INSTALL_RPATH "${TARGET_RPATH}")
  add_dependencies(${TARGET_NAME} orbbec_hardened_sdk_copy)
  add_custom_command(TARGET ${TARGET_NAME} POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E env
      PATCHELF=${ORBBEC_PATCHELF_EXECUTABLE}
      ${ORBBEC_BASH_EXECUTABLE} ${ORBBEC_RPATH_HARDENER}
      $<TARGET_FILE:${TARGET_NAME}>
    COMMENT "Verifying ${TARGET_NAME} has a relocatable, non-empty RPATH"
    VERBATIM)
endfunction()


# Add libraries
add_library(${PROJECT_NAME} ${SOURCE_FILES})
target_link_libraries(${PROJECT_NAME} PRIVATE ${COMMON_LINK_LIBRARIES})
target_include_directories(${PROJECT_NAME} PUBLIC ${COMMON_INCLUDE_DIRS})
harden_orbbec_target(${PROJECT_NAME} "$ORIGIN;/opt/ros/noetic/lib")

add_library(${PROJECT_NAME}_nodelet ${SOURCE_FILES} src/ros_nodelet.cpp)
target_link_libraries(${PROJECT_NAME}_nodelet ${COMMON_LINK_LIBRARIES})
target_include_directories(${PROJECT_NAME}_nodelet PUBLIC ${COMMON_INCLUDE_DIRS})
harden_orbbec_target(${PROJECT_NAME}_nodelet "$ORIGIN;/opt/ros/noetic/lib")


# Add dependencies
add_dependencies(${PROJECT_NAME} ${PROJECT_NAME}_generate_messages_cpp)
add_dependencies(${PROJECT_NAME}_nodelet ${PROJECT_NAME}_generate_messages_cpp)

# Macro for adding executables
macro(add_orbbec_executable TARGET SOURCES)
  add_executable(${TARGET} ${SOURCES})
  target_link_libraries(${TARGET} ${COMMON_LINK_LIBRARIES} ${PROJECT_NAME})
  target_include_directories(${TARGET} PUBLIC ${COMMON_INCLUDE_DIRS})
  harden_orbbec_target(${TARGET} "$ORIGIN/..;/opt/ros/noetic/lib")
endmacro()

# Add nodes
add_orbbec_executable(list_devices_node src/list_devices_node.cpp)
add_orbbec_executable(list_depth_work_mode_node src/list_depth_work_mode.cpp)
add_orbbec_executable(list_camera_profile_mode_node src/list_camera_profile_mode.cpp)
add_orbbec_executable(orbbec_camera_node src/main.cpp)

# Install
install(TARGETS ${PROJECT_NAME} ${PROJECT_NAME}_nodelet ${EXECUTABLES}
  orbbec_camera_node
  list_devices_node
  list_depth_work_mode_node
  list_camera_profile_mode_node
  ARCHIVE DESTINATION ${CATKIN_PACKAGE_LIB_DESTINATION}
  LIBRARY DESTINATION ${CATKIN_PACKAGE_LIB_DESTINATION}
  RUNTIME DESTINATION ${CATKIN_PACKAGE_BIN_DESTINATION}
)

if(DEFINED ENV{BUILDING_PACKAGE})
  # Install udev rules
  install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/scripts/99-obsensor-ros1-libusb.rules
    DESTINATION /etc/udev/rules.d
  )
endif()

install(DIRECTORY include DESTINATION ${CATKIN_PACKAGE_INCLUDE_DESTINATION}/)
install(DIRECTORY ${ORBBEC_INCLUDE_DIR} DESTINATION ${CATKIN_PACKAGE_INCLUDE_DESTINATION}/)
install(DIRECTORY launch DESTINATION ${CATKIN_PACKAGE_SHARE_DESTINATION})
install(DIRECTORY config DESTINATION ${CATKIN_PACKAGE_SHARE_DESTINATION})
install(FILES nodelet_plugins.xml DESTINATION ${CATKIN_PACKAGE_SHARE_DESTINATION})
install(FILES LICENSE DESTINATION ${CATKIN_PACKAGE_SHARE_DESTINATION})
install(DIRECTORY ${ORBBEC_LIBS_DIR}/
  DESTINATION ${CATKIN_PACKAGE_LIB_DESTINATION}/
  FILES_MATCHING PATTERN "*.so"
  PATTERN "*.so.*"
)

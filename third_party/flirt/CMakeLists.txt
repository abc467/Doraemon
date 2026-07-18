cmake_minimum_required(VERSION 3.16)

project(flirt)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

find_package(Boost REQUIRED COMPONENTS serialization)
include_directories(${Boost_INCLUDE_DIRS})
include_directories(${CMAKE_CURRENT_SOURCE_DIR})

add_subdirectory(geometry)
add_subdirectory(sensors)
add_subdirectory(feature)
add_subdirectory(utils)

install(FILES COPYING COPYING.LESSER DESTINATION share/licenses/flirt)

# Copyright 2016 The Cartographer Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if(NOT GMock_FOUND)
  find_path(GMOCK_INCLUDE_DIR gmock/gmock.h
    HINTS
      ENV GMOCK_DIR
    PATH_SUFFIXES include
    PATHS
      /usr
  )

  # Find system-wide installed gmock. Keep the individual cache variables
  # separate from GMOCK_LIBRARIES: Catkin caches GMOCK_LIBRARIES as a list
  # after the first configure, so reusing it in find_library() makes a later
  # configure mistake a target list for an installed gmock_main library.
  find_library(GMOCK_MAIN_LIBRARY
    NAMES gmock_main
    HINTS
      ENV GMOCK_DIR
    PATH_SUFFIXES lib
    PATHS
      /usr
  )
  find_library(GMOCK_LIBRARY
    NAMES gmock
    HINTS
      ENV GMOCK_DIR
    PATH_SUFFIXES lib
    PATHS
      /usr
  )
  find_library(GTEST_LIBRARY
    NAMES gtest
    HINTS
      ENV GMOCK_DIR
    PATH_SUFFIXES lib
    PATHS
      /usr
  )

  # Find system-wide gtest header.
  find_path(GTEST_INCLUDE_DIR gtest/gtest.h
    HINTS
      ENV GTEST_DIR
    PATH_SUFFIXES include
    PATHS
      /usr
  )

  if(GMOCK_MAIN_LIBRARY AND GMOCK_LIBRARY AND GTEST_LIBRARY
      AND GMOCK_INCLUDE_DIR AND GTEST_INCLUDE_DIR)
    set(GMOCK_LIBRARIES
      ${GMOCK_MAIN_LIBRARY}
      ${GMOCK_LIBRARY}
      ${GTEST_LIBRARY}
    )
    set(GMOCK_INCLUDE_DIRS ${GMOCK_INCLUDE_DIR} ${GTEST_INCLUDE_DIR})
  else()
    # A partial system installation is not usable. Clear any plural list
    # cached by Catkin so the source fallback remains deterministic across
    # repeated CMake configure/check cycles.
    set(GMOCK_LIBRARIES "")

    # If no system-wide gmock found, then find src version.
    # Ubuntu might have this.
    find_path(GMOCK_SRC_DIR src/gmock.cc
      HINTS
        ENV GMOCK_DIR
      PATHS
        /usr/src/googletest/googlemock
        /usr/src/gmock
    )
    if(GMOCK_SRC_DIR)
      # If src version found, build it.
      if(NOT TARGET gmock)
        add_subdirectory(${GMOCK_SRC_DIR} "${CMAKE_CURRENT_BINARY_DIR}/gmock"
          EXCLUDE_FROM_ALL)
      endif()
      set(GMOCK_MAIN_LIBRARY gmock_main)
      set(GMOCK_LIBRARY gmock)
      set(GTEST_LIBRARY gtest)
      set(GMOCK_LIBRARIES ${GMOCK_MAIN_LIBRARY})
      set(GMOCK_INCLUDE_DIRS
        ${GMOCK_SRC_DIR}/include
        ${GMOCK_SRC_DIR}/../googletest/include
      )

      # Keep the source targets above as normal variables, but remove failed
      # system probes and stale plural variables from CMakeCache. Otherwise a
      # dependency audit can report GMOCK_LIBRARY-NOTFOUND even though this
      # valid source fallback is selected.
      unset(GMOCK_INCLUDE_DIR CACHE)
      unset(GMOCK_INCLUDE_DIRS CACHE)
      unset(GMOCK_MAIN_LIBRARY CACHE)
      unset(GMOCK_LIBRARY CACHE)
      unset(GMOCK_LIBRARIES CACHE)
      unset(GTEST_LIBRARY CACHE)
    endif()
  endif()

  # System-wide installed gmock library might require pthreads.
  find_package(Threads REQUIRED)
  list(APPEND GMOCK_LIBRARIES ${CMAKE_THREAD_LIBS_INIT})
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(GMock DEFAULT_MSG GMOCK_LIBRARIES
                                  GMOCK_INCLUDE_DIRS)

# Mitigate build issue with Catkin
set(GMOCK_FOUND FALSE)

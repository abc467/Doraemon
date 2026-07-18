include(FindPackageHandleStandardArgs)

set(FLIRT_ROOT "" CACHE PATH "FLIRT installation prefix")
set(_FLIRT_HINTS)
if(FLIRT_ROOT)
  list(APPEND _FLIRT_HINTS "${FLIRT_ROOT}")
endif()

find_path(
  FLIRT_INC_ROOT
  NAMES flirtlib/feature/ShapeContext.h
  HINTS ${_FLIRT_HINTS}
  PATH_SUFFIXES include
)

set(FLIRT_INCLUDE_DIR
  "${FLIRT_INC_ROOT}"
  "${FLIRT_INC_ROOT}/flirtlib"
)

set(FLIRT_LIBRARY)
foreach(_FLIRT_LIB_NAME
    flirtlib_feature
    flirtlib_geometry
    flirtlib_sensors
    flirtlib_utils)
  string(TOUPPER "${_FLIRT_LIB_NAME}" _FLIRT_LIB_VAR_SUFFIX)
  find_library(
    FLIRT_${_FLIRT_LIB_VAR_SUFFIX}_LIBRARY
    NAMES "${_FLIRT_LIB_NAME}"
    HINTS ${_FLIRT_HINTS}
    PATH_SUFFIXES lib
  )
  list(APPEND FLIRT_LIBRARY "${FLIRT_${_FLIRT_LIB_VAR_SUFFIX}_LIBRARY}")
endforeach()

find_package_handle_standard_args(
  FLIRT
  REQUIRED_VARS
    FLIRT_INC_ROOT
    FLIRT_FLIRTLIB_FEATURE_LIBRARY
    FLIRT_FLIRTLIB_GEOMETRY_LIBRARY
    FLIRT_FLIRTLIB_SENSORS_LIBRARY
    FLIRT_FLIRTLIB_UTILS_LIBRARY
)

mark_as_advanced(FLIRT_INC_ROOT FLIRT_LIBRARY)

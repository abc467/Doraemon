set(INCLUDE_OK ON)
set(LIB_OK ON)

# Include
unset(FLIRT_INC_ROOT CACHE)
find_path(FLIRT_INC_ROOT flirtlib /usr/local/include /usr/include)

set(FLIRT_INCLUDE_DIR)
list(APPEND FLIRT_INCLUDE_DIR ${FLIRT_INC_ROOT})
list(APPEND FLIRT_INCLUDE_DIR ${FLIRT_INC_ROOT}/flirtlib)

message([FLIRT] ${FLIRT_INCLUDE_DIR})

# Library
set(LIBS)
list(APPEND LIBS flirtlib_feature)
list(APPEND LIBS flirtlib_geometry)
list(APPEND LIBS flirtlib_sensors)
list(APPEND LIBS flirtlib_utils)

set(FLIRT_LIBRARY)
foreach(LIBNAME ${LIBS})
    unset(LIBPATH CACHE)
    find_library(LIBPATH ${LIBNAME})
    if(LIBPATH)
        list(APPEND FLIRT_LIBRARY ${LIBPATH})
    else()
        set(LIB_OK OFF)
    endif()
endforeach()

unset(LIBS)

if(${INCLUDE_OK} AND ${LIB_OK})
    set(FLIRT_FOUND TRUE)
endif()

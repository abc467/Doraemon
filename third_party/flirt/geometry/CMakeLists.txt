set(geometry_SRCS
    point.cpp
)

set(geometry_HDRS
    point.h
)

add_library(flirtlib_geometry SHARED ${geometry_SRCS})
target_link_libraries(flirtlib_geometry ${Boost_SERIALIZATION_LIBRARY})

install(TARGETS flirtlib_geometry DESTINATION lib)
install(FILES ${geometry_HDRS} DESTINATION include/flirtlib/geometry)


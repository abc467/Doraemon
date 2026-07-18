SET(utils_SRCS
    Regression.cpp
    SimplePeakFinder.cpp
    SimpleMinMaxPeakFinder.cpp
    PoseEstimation.cpp
)

SET(utils_HDRS
    Convolution.h
    Convolution.hpp
    HistogramDistances.h
    HistogramDistances.hpp
    PeakFinder.h
    PoseEstimation.h
    Regression.h
    SimpleMinMaxPeakFinder.h
    SimplePeakFinder.h
)

ADD_LIBRARY(flirtlib_utils SHARED ${utils_SRCS})

install(TARGETS flirtlib_utils DESTINATION lib)

install(FILES ${utils_HDRS} DESTINATION include/flirtlib/utils)

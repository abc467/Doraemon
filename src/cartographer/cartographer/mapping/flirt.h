#ifndef FLIRT_HPP
#define FLIRT_HPP

#include <atomic>
#include <condition_variable>
#include <mutex>

#include "cartographer/common/sml_config.h"

#include "cartographer/mapping/proto/flirt_data.pb.h"
#include "flirtlib/feature/BetaGrid.h"
#include "flirtlib/feature/ShapeContext.h"
#include "flirtlib/feature/CurvatureDetector.h"
#include "flirtlib/feature/RangeDetector.h"
#include "flirtlib/feature/NormalDetector.h"
#include "flirtlib/feature/NormalEdgeDetector.h"
#include "flirtlib/feature/NormalBlobDetector.h"
#include "flirtlib/feature/RansacFeatureSetMatcher.h"
#include "flirtlib/geometry/point.h"
#include "flirtlib/sensors/LaserReading.h"
#include "flirtlib/utils/HistogramDistances.h"
#include "flirtlib/utils/SimpleMinMaxPeakFinder.h"
#include "flirtlib/utils/SimplePeakFinder.h"

namespace flirt
{
    constexpr int kRelocationIdle = -1;
    constexpr int kRelocationSuccess = 0;
    constexpr int kRelocationNeedMoreTrajectories = -2;
    constexpr int kRelocationNoInterestPoints = -3;
    constexpr int kRelocationSubmapNotFound = -4;
    constexpr int kRelocationNoCandidatePose = -8;
    constexpr int kRelocationLowConstraintScore = -9;

    extern std::atomic<bool> use_flirt;
    extern std::atomic<bool> need_flirt;
    extern std::atomic<bool> need_optimizing;
    extern std::atomic<bool> flirt_working;

    extern std::condition_variable cv_flirt_busy;
    extern std::mutex flirt_busy_lock;
    extern volatile int flirt_return_code;

    void init();
    EuclideanDistance<double> *get_distance_function();
    void detect(const LaserReading &reading, std::vector<InterestPoint *> &point);
    Descriptor *describe(const InterestPoint &point, const LaserReading &reading);
    void match(const std::vector<InterestPoint *> &reference, const std::vector<InterestPoint *> &data, OrientedPoint2D &transformation,
               std::vector<std::pair<InterestPoint *, InterestPoint *>> &correspondences);
} // namespace flirt

#endif

#ifndef FLIRT_HPP
#define FLIRT_HPP

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <vector>

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
    // Owns a complete, immutable set of FLIRT interest points. FLIRT's matcher
    // API requires vectors of mutable raw pointers, so raw_points() exposes a
    // non-owning view while owned_points_ remains the sole owner.
    class FeatureSet final
    {
    public:
        using OwnedPoint = std::unique_ptr<InterestPoint>;
        using OwnedPoints = std::vector<OwnedPoint>;
        using RawPoints = std::vector<InterestPoint *>;

        // Detects and describes all features in 'reading'. The Descriptor
        // returned by FLIRT is released after InterestPoint::setDescriptor()
        // clones it.
        static std::shared_ptr<const FeatureSet> Build(
            const LaserReading &reading);

        // Takes ownership of already constructed interest points. This is used
        // by deserialization and keeps ownership explicit at the call site.
        static std::shared_ptr<const FeatureSet> Adopt(OwnedPoints points);

        FeatureSet(const FeatureSet &) = delete;
        FeatureSet &operator=(const FeatureSet &) = delete;

        const RawPoints &raw_points() const { return raw_points_; }
        const RawPoints &raw() const { return raw_points_; }
        std::size_t size() const { return owned_points_.size(); }
        bool empty() const { return owned_points_.empty(); }
        const InterestPoint &at(const std::size_t index) const
        {
            return *owned_points_.at(index);
        }

    private:
        explicit FeatureSet(OwnedPoints points);

        OwnedPoints owned_points_;
        RawPoints raw_points_;
    };

    std::shared_ptr<const FeatureSet> BuildFeatureSet(
        const LaserReading &reading);
    std::shared_ptr<const FeatureSet> AdoptFeatureSet(
        FeatureSet::OwnedPoints points);

    constexpr int kRelocationIdle = -1;
    constexpr int kRelocationSuccess = 0;
    constexpr int kRelocationNeedMoreTrajectories = -2;
    constexpr int kRelocationNoInterestPoints = -3;
    constexpr int kRelocationSubmapNotFound = -4;
    constexpr int kRelocationNoCandidatePose = -8;
    constexpr int kRelocationLowConstraintScore = -9;
    constexpr int kRelocationWorkerUnavailable = -11;
    constexpr int kRelocationFeaturesNotReady = -12;

    extern std::atomic<bool> use_flirt;
    extern std::atomic<bool> need_flirt;
    extern std::atomic<bool> need_optimizing;
    extern std::atomic<bool> flirt_working;

    extern std::condition_variable cv_flirt_busy;
    extern std::mutex flirt_busy_lock;
    extern std::atomic<int> flirt_return_code;

    extern std::atomic<double> relocation_min_score;
    extern std::atomic<int> relocation_required_consistent_hits;
    extern std::atomic<int> relocation_consistency_max_submap_index_delta;
    extern std::atomic<double> relocation_consistency_max_translation_m;
    extern std::atomic<double> relocation_consistency_max_rotation_rad;

    extern std::mutex relocation_consistency_lock;
    extern bool relocation_has_last_candidate;
    extern int relocation_consistency_hits;
    extern int relocation_last_trajectory_id;
    extern int relocation_last_submap_index;
    extern double relocation_last_x;
    extern double relocation_last_y;
    extern double relocation_last_theta;
    extern double relocation_last_score;

    void init();
    void reset_relocation_consistency();
    EuclideanDistance<double> *get_distance_function();
    void match(const std::vector<InterestPoint *> &reference, const std::vector<InterestPoint *> &data, OrientedPoint2D &transformation,
               std::vector<std::pair<InterestPoint *, InterestPoint *>> &correspondences);
} // namespace flirt

#endif

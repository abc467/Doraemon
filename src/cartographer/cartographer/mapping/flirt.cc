#include "flirt.h"

BOOST_CLASS_EXPORT_IMPLEMENT(BetaGrid)
BOOST_CLASS_EXPORT_IMPLEMENT(ShapeContext)

namespace flirt
{
  std::atomic<bool> use_flirt;
  std::atomic<bool> need_flirt;
  std::atomic<bool> need_optimizing;
  std::atomic<bool> flirt_working;

  std::condition_variable cv_flirt_busy;
  std::mutex flirt_busy_lock;
  volatile int flirt_return_code;

  std::atomic<double> relocation_min_score{0.60};
  std::atomic<int> relocation_required_consistent_hits{1};
  std::atomic<int> relocation_consistency_max_submap_index_delta{1};
  std::atomic<double> relocation_consistency_max_translation_m{0.50};
  std::atomic<double> relocation_consistency_max_rotation_rad{0.15};

  std::mutex relocation_consistency_lock;
  bool relocation_has_last_candidate = false;
  int relocation_consistency_hits = 0;
  int relocation_last_trajectory_id = -1;
  int relocation_last_submap_index = -1;
  double relocation_last_x = 0.0;
  double relocation_last_y = 0.0;
  double relocation_last_theta = 0.0;
  double relocation_last_score = 0.0;

  // detector
  std::unique_ptr<SimpleMinMaxPeakFinder> peak_finder;
  std::unique_ptr<CurvatureDetector> g_detector;
  // describer
  EuclideanDistance<double> distance_function;
  std::unique_ptr<ShapeContextGenerator> g_descriptor_generator;
  // matcher
  std::unique_ptr<RansacFeatureSetMatcher> g_matcher;

  // // detector
  // SimpleMinMaxPeakFinder peak_finder(0.3, 0.001);
  // CurvatureDetector g_detector(&peak_finder, 5, 0.2, 1.4, 2);
  // // describer
  // EuclideanDistance<double> distance_function;
  // ShapeContextGenerator g_descriptor_generator(0.02, 5, 4, 4);
  // // matcher
  // double acceptanceSigma = 0.1;
  // double success = 0.95, inlier = 0.4, matchingThreshold = 0.4;
  // RansacFeatureSetMatcher g_matcher(0.1, success, inlier, matchingThreshold,
  //                                   0.3,
  //                                   false);
  //从sml中获取参数并初始化参数
  void init()
  {
    peak_finder = std::make_unique<SimpleMinMaxPeakFinder>(sml_config::get_fatal<float>(sml_config::sml_global_relocation, sml_config::CONFIG_PEAKFINDER_MIN_VALUE),
                                                           sml_config::get_fatal<float>(sml_config::sml_global_relocation, sml_config::CONFIG_PEAKFINDER_MIN_DIFFERENCE));

    g_detector = std::make_unique<CurvatureDetector>(peak_finder.get(),
                                                     sml_config::get_fatal<int>(sml_config::sml_global_relocation, sml_config::CONFIG_DETECTOR_SCALES),
                                                     sml_config::get_fatal<float>(sml_config::sml_global_relocation, sml_config::CONFIG_DETECTOR_SIGMA),
                                                     sml_config::get_fatal<float>(sml_config::sml_global_relocation, sml_config::CONFIG_DETECTOR_STEP),
                                                     sml_config::get_fatal<int>(sml_config::sml_global_relocation, sml_config::CONFIG_DETECTOR_DMST));

    g_descriptor_generator = std::make_unique<ShapeContextGenerator>(sml_config::get_fatal<float>(sml_config::sml_global_relocation, sml_config::CONFIG_DESCRIBER_MIN_RHO),
                                                                     sml_config::get_fatal<float>(sml_config::sml_global_relocation, sml_config::CONFIG_DESCRIBER_MAX_RHO),
                                                                     sml_config::get_fatal<int>(sml_config::sml_global_relocation, sml_config::CONFIG_DESCRIBER_BIN_RHO),
                                                                     sml_config::get_fatal<int>(sml_config::sml_global_relocation, sml_config::CONFIG_DESCRIBER_BIN_PHI));

    g_matcher = std::make_unique<RansacFeatureSetMatcher>(sml_config::get_fatal<float>(sml_config::sml_global_relocation, sml_config::CONFIG_MATCHER_ACCEPTANCE_THRESHOLD),
                                                          sml_config::get_fatal<float>(sml_config::sml_global_relocation, sml_config::CONFIG_MATCHER_SUCCESS_PROBABILITY),
                                                          sml_config::get_fatal<float>(sml_config::sml_global_relocation, sml_config::CONFIG_MATCHER_INLIER_PROBABILITY),
                                                          sml_config::get_fatal<float>(sml_config::sml_global_relocation, sml_config::CONFIG_MATCHER_DISTANCE_THRESHOLD),
                                                          sml_config::get_fatal<float>(sml_config::sml_global_relocation, sml_config::CONFIG_MATCHER_RIGIDITY_THRESHOLD),
                                                          sml_config::get_fatal<int>(sml_config::sml_global_relocation, sml_config::CONFIG_MATCHER_ADAPTIVE));

    need_flirt.store(false);
    need_optimizing.store(false);
    flirt_working.store(false);
    flirt_return_code = kRelocationIdle;
    relocation_min_score.store(0.60);
    relocation_required_consistent_hits.store(1);
    relocation_consistency_max_submap_index_delta.store(1);
    relocation_consistency_max_translation_m.store(0.50);
    relocation_consistency_max_rotation_rad.store(0.15);
    reset_relocation_consistency();

    g_detector->setUseMaxRange(true);
    g_descriptor_generator->setDistanceFunction(&distance_function);
  }

  void reset_relocation_consistency()
  {
    std::lock_guard<std::mutex> lock(relocation_consistency_lock);
    relocation_has_last_candidate = false;
    relocation_consistency_hits = 0;
    relocation_last_trajectory_id = -1;
    relocation_last_submap_index = -1;
    relocation_last_x = 0.0;
    relocation_last_y = 0.0;
    relocation_last_theta = 0.0;
    relocation_last_score = 0.0;
  }

  EuclideanDistance<double> *get_distance_function()
  {
    return &distance_function;
  }

  void detect(const LaserReading &reading, std::vector<InterestPoint *> &point)
  {
    g_detector->detect(reading, point);
  }

  Descriptor *describe(const InterestPoint &point, const LaserReading &reading)
  {
    return g_descriptor_generator->describe(point, reading);
  }

  void match(const std::vector<InterestPoint *> &reference, const std::vector<InterestPoint *> &data, OrientedPoint2D &transformation,
             std::vector<std::pair<InterestPoint *, InterestPoint *>> &correspondences)
  {
    g_matcher->matchSets(reference, data, transformation, correspondences);
  }

} // namespace flirt

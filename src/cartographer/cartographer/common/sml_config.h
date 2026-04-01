#ifndef SML_CONFIG_H
#define SML_CONFIG_H

/**
 * SML_CONFIG:
 * 这个文件用于读取和保存sml相关的配置文件
 */

#include "sml.h"
#include <stdio.h>

namespace sml_config
{
    constexpr const char *CONFIG_PEAKFINDER_MIN_VALUE = "peakfinder_min_value";
    constexpr const char *CONFIG_PEAKFINDER_MIN_DIFFERENCE = "peakfinder_min_difference";

    constexpr const char *CONFIG_DETECTOR_SCALES = "detector_scales";
    constexpr const char *CONFIG_DETECTOR_SIGMA = "detector_sigma";
    constexpr const char *CONFIG_DETECTOR_STEP = "detector_step";
    constexpr const char *CONFIG_DETECTOR_DMST = "detector_dmst";

    constexpr const char *CONFIG_DESCRIBER_MIN_RHO = "describer_min_rho";
    constexpr const char *CONFIG_DESCRIBER_MAX_RHO = "describer_max_rho";
    constexpr const char *CONFIG_DESCRIBER_BIN_RHO = "describer_bin_rho";
    constexpr const char *CONFIG_DESCRIBER_BIN_PHI = "describer_bin_phi";

    constexpr const char *CONFIG_MATCHER_ACCEPTANCE_THRESHOLD = "matcher_acceptance_threshold";
    constexpr const char *CONFIG_MATCHER_SUCCESS_PROBABILITY = "matcher_success_probability";
    constexpr const char *CONFIG_MATCHER_INLIER_PROBABILITY = "matcher_inlier_probability";
    constexpr const char *CONFIG_MATCHER_DISTANCE_THRESHOLD = "matcher_distance_threshold";
    constexpr const char *CONFIG_MATCHER_RIGIDITY_THRESHOLD = "matcher_rigidity_threshold";
    constexpr const char *CONFIG_MATCHER_ADAPTIVE = "matcher_adaptive";

    constexpr const char *CONFIG_MOVEBASE_CONTROLLER = "movebase_controller";

    extern cyber::sml sml_global_relocation;
    bool sml_init();

    template <typename _T>
    _T get_fatal(cyber::sml &sml, const std::string &key)
    {
        cyber::sml_result<_T> r = sml.get<_T>(key);
        if (!r.ok)
        {
            printf("Try to get SML key:%s failed! This is fatal! Please check type and key name!", key.c_str());
            std::exit(-1);
        }
        return r.value;
    }
} // namespace sml_config

#endif
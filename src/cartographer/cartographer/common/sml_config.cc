#include "sml_config.h"

#include <iostream>

namespace sml_config
{
    std::string slam_root;

    cyber::sml sml_global_relocation;
    void fill_global_relocation_default(cyber::sml &sml)
    {
        // peak finder
        sml.put(CONFIG_PEAKFINDER_MIN_VALUE, 0.03f);
        sml.put(CONFIG_PEAKFINDER_MIN_DIFFERENCE, 0.001f);
        // detector
        sml.put(CONFIG_DETECTOR_SCALES, 5);
        sml.put(CONFIG_DETECTOR_SIGMA, 0.2f);
        sml.put(CONFIG_DETECTOR_STEP, 1.4f);
        sml.put(CONFIG_DETECTOR_DMST, 2);
        // describer
        sml.put(CONFIG_DESCRIBER_MIN_RHO, 0.02f);
        sml.put(CONFIG_DESCRIBER_MAX_RHO, 5.0f);
        sml.put(CONFIG_DESCRIBER_BIN_RHO, 4);
        sml.put(CONFIG_DESCRIBER_BIN_PHI, 4);
        // matcher
        sml.put(CONFIG_MATCHER_ACCEPTANCE_THRESHOLD, 0.1f);
        sml.put(CONFIG_MATCHER_SUCCESS_PROBABILITY, 0.95f);
        sml.put(CONFIG_MATCHER_INLIER_PROBABILITY, 0.4f);
        sml.put(CONFIG_MATCHER_DISTANCE_THRESHOLD, 0.4f);
        sml.put(CONFIG_MATCHER_RIGIDITY_THRESHOLD, 0.3f);
        sml.put(CONFIG_MATCHER_ADAPTIVE, 0);
        // movebase
        sml.put(CONFIG_MOVEBASE_CONTROLLER, "RppController");
    }

    void load_global_relocation(const std::string &path)
    {
        auto r = sml_global_relocation.load(path);
        if (!r)
        {
            printf("Load global_relocation.sml failed, use default values.\n");
            fill_global_relocation_default(sml_global_relocation);
            sml_global_relocation.save(path);
        }
    }

    bool sml_init()
    {
        char *root = getenv("SLAM_ROOT");
        if (root == 0)
        {
            std::cout << "Cannot find env `SLAM_ROOT`, eg. /home/user/cartographer" << std::endl;
            std::exit(-1);
        }
        slam_root = std::string(root);
        printf("SLAM_ROOT=%s\n", slam_root.c_str());
        load_global_relocation(slam_root + "/config/global_relocation.sml");
        return true;
    }

} // namespace sml_config

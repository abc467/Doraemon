#pragma once

#include <Eigen/Dense>

#include <string>
#include <memory>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <random>

#include "mppi_controller/models/constraints.hpp"
#include "mppi_controller/models/optimizer_settings.hpp"
#include "mppi_controller/models/control_sequence.hpp"
#include "mppi_controller/models/state.hpp"

namespace mppi
{

/**
 * @class mppi::NoiseGenerator
 * @brief 噪声轨迹生成器
 */
class NoiseGenerator
{
public:
    NoiseGenerator() = default;

    /**
     * @brief Initialize noise generator with settings and model types
     * @param settings Settings of controller
     * @param is_holonomic If base is holonomic
     */
    void initialize(
        mppi::models::OptimizerSettings &settings,
        bool is_holonomic = false);

    /**
     * @brief Shutdown noise generator thread
     */
    void shutdown();

    /**
     * @brief Signal to the noise thread the controller is ready to generate a new
     * noised control for the next iteration
     */
    void generateNextNoises();

    /**
     * @brief set noised control_sequence to state controls
     * @return noises vx, vy, wz
     */
    void setNoisedControls(models::State &state, const models::ControlSequence &control_sequence);

    /**
     * @brief Reset noise generator with settings and model types
     * @param settings Settings of controller
     * @param is_holonomic If base is holonomic
     */
    void reset(mppi::models::OptimizerSettings &settings, bool is_holonomic);

protected:
    /**
     * @brief Thread to execute noise generation process
     */
    void noiseThread();

    /**
     * @brief Generate random controls by gaussian noise with mean in
     * control_sequence_
     *
     * @return tensor of shape [ batch_size_, time_steps_, 2]
     * where 2 stands for v, w
     */
    void generateNoisedControls();

    Eigen::ArrayXXf noises_vx_;
    Eigen::ArrayXXf noises_vy_;
    Eigen::ArrayXXf noises_wz_;

    std::default_random_engine generator_;
    std::normal_distribution<float> ndistribution_vx_;
    std::normal_distribution<float> ndistribution_wz_;
    std::normal_distribution<float> ndistribution_vy_;

    mppi::models::OptimizerSettings settings_;
    bool is_holonomic_;

    std::thread noise_thread_;
    std::condition_variable noise_cond_;
    std::mutex noise_lock_;
    bool active_{false}, ready_{false}, regenerate_noises_{false};
};

}

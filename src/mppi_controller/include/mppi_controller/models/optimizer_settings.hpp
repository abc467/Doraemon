#pragma once

#include <cstddef>
#include "mppi_controller/models/constraints.hpp"

namespace mppi::models
{

/**
 * @struct mppi::models::OptimizerSettings
 * @brief Settings for the optimizer to use
 */
struct OptimizerSettings
{
  models::ControlConstraints base_constraints{0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f};
  models::ControlConstraints constraints{0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f};
  models::SamplingStd sampling_std{0.0f, 0.0f, 0.0f};
  float model_dt{0.0f};
  float temperature{0.0f};
  float gamma{0.0f};
  int batch_size{0u};
  int time_steps{0u};
  int iteration_count{0u};
  bool shift_control_sequence{false};
  int retry_attempt_limit{0};
};

}  // namespace mppi::models

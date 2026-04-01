#pragma once

#include <Eigen/Dense>

#include <memory>
#include <vector>

#include "geometry_msgs/PoseStamped.h"
#include "mppi_controller/models/state.hpp"
#include "mppi_controller/models/trajectories.hpp"
#include "mppi_controller/models/path.hpp"
#include "mppi_controller/motion_models.hpp"


namespace mppi
{

/**
 * @struct mppi::CriticData
 * @brief 用于给critics来评分的数据, 包含 state, trajectories,
 * pruned path, global goal, costs, and important parameters to share
 */
struct CriticData
{
  const models::State & state;
  const models::Trajectories & trajectories; // 候选轨迹
  const models::Path & path; // 裁剪后的参考路径
  const geometry_msgs::Pose & goal;

  Eigen::ArrayXf & costs;
  float & model_dt;

  bool fail_flag;
  std::shared_ptr<MotionModel> motion_model;
  std::optional<std::vector<bool>> path_pts_valid;
  std::optional<size_t> furthest_reached_path_point; // 一系列轨迹中能达到最远点的轨迹id
};

}  // namespace mppi


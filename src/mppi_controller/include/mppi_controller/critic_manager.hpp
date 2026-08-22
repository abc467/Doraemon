#pragma once

#include <cstddef>
#include <memory>
#include <string>
#include <vector>
#include <pluginlib/class_loader.hpp>
#include <ros/publisher.h>

#include "geometry_msgs/TwistStamped.h"
#include "costmap_2d/costmap_2d_ros.h"

#include "mppi_controller/tools/utils.hpp"
#include "mppi_controller/critic_data.hpp"
#include "mppi_controller/critic_function.hpp"

namespace mppi
{

/**
 * @class mppi::CriticManager
 * @brief 目标函数插件的管理类
 */
class CriticManager
{
public:
  typedef std::vector<std::unique_ptr<critics::CriticFunction>> Critics;

  CriticManager() = default;

  virtual ~CriticManager() = default;

  /**
    * @brief 配置插件管理器，加载插件
    * @param nh ros句柄
    * @param name 控制器名称
    * @param costmap_ros
    */
  void on_configure(const ros::NodeHandle & nh, 
    const std::string & name, std::shared_ptr<costmap_2d::Costmap2DROS>);

  /**
    * @brief 通过加载critics来评估轨迹
    * @param CriticData 用于给critics来评分的数据, 包含 state, trajectories, pruned path, global goal, costs, and important parameters to share
    */
  void evalTrajectoriesScores(CriticData & data) const;

protected:
  /**
    * @brief 从参数服务器中加载critics名字
    */
  void getParams();

  /**
    * @brief 加载 critic 插件实例并初始化
    */
  virtual void loadCritics();

  /**
    * @brief 获取插件的完整类名
    */
  std::string getFullName(const std::string & name);

protected:
  ros::NodeHandle nh_;
  std::shared_ptr<costmap_2d::Costmap2DROS> costmap_ros_;
  std::string name_;

  std::vector<std::string> critic_names_;
  std::unique_ptr<pluginlib::ClassLoader<critics::CriticFunction>> loader_;
  Critics critics_;

  bool timing_diagnostics_{false};
  mutable size_t timing_cycles_{0};
  mutable std::vector<double> critic_time_totals_ms_;

  // Subscriber-gated diagnostics.  These accumulators never participate in
  // trajectory scoring; when no recorder subscribes, the hot path performs
  // only one inexpensive subscriber-count check per control cycle.
  struct CriticStatsAccumulator
  {
    double cost_mean_sum{0.0};
    double cost_max_sum{0.0};
    double changed_ratio_sum{0.0};
    double score_time_ms_sum{0.0};
    size_t executed_cycles{0};
    size_t fail_cycles{0};
  };
  bool publish_critic_stats_{false};
  int critic_stats_publish_period_{10};
  ros::Publisher critic_stats_pub_;
  mutable size_t critic_stats_cycles_{0};
  mutable std::vector<CriticStatsAccumulator> critic_stats_accumulators_;
};

}  // namespace mppi

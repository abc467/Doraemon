#pragma once

#include <string>
#include <memory>

#include "costmap_2d/costmap_2d_ros.h"
#include "mppi_controller/critic_data.hpp"

namespace mppi::critics
{

/**
 * @class mppi::critics::CollisionCost
 * @brief Utility for storing cost information
 */
struct CollisionCost
{
  float cost{0};
  bool using_footprint{false};
};

/**
 * @class mppi::critics::CriticFunction
 * @brief 抽象的critic目标函数用于评估轨迹
 */
class CriticFunction
{
public:
  CriticFunction() = default;

  virtual ~CriticFunction() = default;

  /**
   * @brief Configure critic on bringup
   * @param name 插件名
   * @param costmap_ros Costmap2DROS object of environment
   * @param dynamic_parameter_handler Parameter handler object
   */
  void on_configure(
      const ros::NodeHandle& nh,
      const std::string &name,
      std::shared_ptr<costmap_2d::Costmap2DROS> costmap_ros)
  {
    nh_ = nh;
    name_ = name; // critic name
    costmap_ros_ = costmap_ros;
    costmap_ = costmap_ros_->getCostmap();

    nh_.param(name +"/enabled", enabled_, true);

    initialize();
  }

  /**
   * @brief Main function to score trajectory
   * @param data Critic data to use in scoring
   */
  virtual void score(CriticData &data) = 0;

  /**
   * @brief Initialize critic
   */
  virtual void initialize() = 0;

  /**
   * @brief Get name of critic
   */
  std::string getName()
  {
    return name_;
  }

protected:
  ros::NodeHandle nh_;
  bool enabled_;
  std::string name_;
  std::shared_ptr<costmap_2d::Costmap2DROS> costmap_ros_;
  costmap_2d::Costmap2D *costmap_{nullptr};
};

} // namespace mppi::critics

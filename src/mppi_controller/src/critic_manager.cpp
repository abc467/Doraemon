
#include "mppi_controller/critic_manager.hpp"

#include <algorithm>
#include <chrono>
#include <sstream>

namespace mppi
{

  void CriticManager::on_configure(
      const ros::NodeHandle &nh, const std::string &name,
      std::shared_ptr<costmap_2d::Costmap2DROS> costmap_ros)
  {
    nh_ = nh;
    costmap_ros_ = costmap_ros;
    name_ = name;

    getParams();
    loadCritics();
  }

  void CriticManager::getParams()
  {
    nh_.param("critics", critic_names_, std::vector<std::string>());
    nh_.param("timing_diagnostics", timing_diagnostics_, false);
  }

  void CriticManager::loadCritics()
  {
    if (!loader_)
    {
      loader_ = std::make_unique<pluginlib::ClassLoader<critics::CriticFunction>>(
          "mppi_controller", "mppi::critics::CriticFunction");
    }

    critics_.clear();
    critic_time_totals_ms_.clear();
    for (auto name : critic_names_)
    {
      std::string fullname = getFullName(name);
      auto instance = std::unique_ptr<critics::CriticFunction>(
          loader_->createUnmanagedInstance(fullname));
      critics_.push_back(std::move(instance));
      critics_.back()->on_configure(nh_, name, costmap_ros_);
      ROS_INFO("critic_names_: %s", fullname.c_str());
    }
    critic_time_totals_ms_.assign(critics_.size(), 0.0);
  }

  std::string CriticManager::getFullName(const std::string &name)
  {
    return "mppi::critics::" + name;
  }

  void CriticManager::evalTrajectoriesScores(
      CriticData &data) const
  {
    for (size_t i = 0; i < critics_.size(); ++i)
    {
      if (data.fail_flag)
      {
        break;
      }

      const auto start = std::chrono::steady_clock::now();
      critics_[i]->score(data);
      if (timing_diagnostics_)
      {
        const auto end = std::chrono::steady_clock::now();
        critic_time_totals_ms_[i] +=
          std::chrono::duration<double, std::milli>(end - start).count();
      }
    }

    if (timing_diagnostics_ && ++timing_cycles_ >= 50)
    {
      std::ostringstream timing;
      timing << "[" << name_ << "] MPPI critic average:";
      for (size_t i = 0; i < critics_.size(); ++i)
      {
        timing << " " << critics_[i]->getName() << "="
               << critic_time_totals_ms_[i] / static_cast<double>(timing_cycles_)
               << "ms";
      }
      ROS_INFO_STREAM(timing.str());

      timing_cycles_ = 0;
      std::fill(critic_time_totals_ms_.begin(), critic_time_totals_ms_.end(), 0.0);
    }
  }

} // namespace mppi

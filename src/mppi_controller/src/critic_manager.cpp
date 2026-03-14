
#include "mppi_controller/critic_manager.hpp"

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
  }

  void CriticManager::loadCritics()
  {
    if (!loader_)
    {
      loader_ = std::make_unique<pluginlib::ClassLoader<critics::CriticFunction>>(
          "mppi_controller", "mppi::critics::CriticFunction");
    }

    critics_.clear();
    for (auto name : critic_names_)
    {
      std::string fullname = getFullName(name);
      auto instance = std::unique_ptr<critics::CriticFunction>(
          loader_->createUnmanagedInstance(fullname));
      critics_.push_back(std::move(instance));
      critics_.back()->on_configure(nh_, name, costmap_ros_);
      ROS_INFO("critic_names_: %s", fullname.c_str());
    }
  }

  std::string CriticManager::getFullName(const std::string &name)
  {
    return "mppi::critics::" + name;
  }

  void CriticManager::evalTrajectoriesScores(
      CriticData &data) const
  {
    for (const auto &critic : critics_)
    {
      if (data.fail_flag)
      {
        break;
      }
      critic->score(data);
    }
  }

} // namespace mppi

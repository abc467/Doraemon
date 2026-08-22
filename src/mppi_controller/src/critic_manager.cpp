
#include "mppi_controller/critic_manager.hpp"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <iomanip>
#include <sstream>

#include <std_msgs/String.h>

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
    nh_.param("publish_critic_stats", publish_critic_stats_, false);
    nh_.param("critic_stats_publish_period", critic_stats_publish_period_, 10);
    critic_stats_publish_period_ = std::max(1, critic_stats_publish_period_);
    if (publish_critic_stats_)
    {
      critic_stats_pub_ = nh_.advertise<std_msgs::String>("critic_stats", 10);
    }
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
    critic_stats_accumulators_.assign(critics_.size(), CriticStatsAccumulator{});
  }

  std::string CriticManager::getFullName(const std::string &name)
  {
    return "mppi::critics::" + name;
  }

  void CriticManager::evalTrajectoriesScores(
      CriticData &data) const
  {
    const bool collect_stats = publish_critic_stats_ &&
      critic_stats_pub_ && critic_stats_pub_.getNumSubscribers() > 0u;
    if (!collect_stats && critic_stats_cycles_ > 0u)
    {
      critic_stats_cycles_ = 0u;
      std::fill(
        critic_stats_accumulators_.begin(),
        critic_stats_accumulators_.end(),
        CriticStatsAccumulator{});
    }

    for (size_t i = 0; i < critics_.size(); ++i)
    {
      if (data.fail_flag)
      {
        break;
      }

      Eigen::ArrayXf costs_before;
      if (collect_stats)
      {
        costs_before = data.costs;
      }
      const auto start = std::chrono::steady_clock::now();
      critics_[i]->score(data);
      if (timing_diagnostics_ || collect_stats)
      {
        const auto end = std::chrono::steady_clock::now();
        const double elapsed_ms =
          std::chrono::duration<double, std::milli>(end - start).count();
        if (timing_diagnostics_)
        {
          critic_time_totals_ms_[i] += elapsed_ms;
        }
        if (collect_stats)
        {
          auto &acc = critic_stats_accumulators_[i];
          double delta_sum = 0.0;
          double delta_max = 0.0;
          size_t finite_count = 0u;
          size_t changed_count = 0u;
          const Eigen::Index count = std::min(costs_before.size(), data.costs.size());
          for (Eigen::Index j = 0; j < count; ++j)
          {
            const double before = static_cast<double>(costs_before(j));
            const double after = static_cast<double>(data.costs(j));
            if (!std::isfinite(before) || !std::isfinite(after))
            {
              continue;
            }
            const double delta = std::abs(after - before);
            delta_sum += delta;
            delta_max = std::max(delta_max, delta);
            ++finite_count;
            if (delta > 1e-6)
            {
              ++changed_count;
            }
          }
          acc.cost_mean_sum += finite_count > 0u ? delta_sum / finite_count : 0.0;
          acc.cost_max_sum += delta_max;
          acc.changed_ratio_sum += finite_count > 0u ?
            static_cast<double>(changed_count) / finite_count : 0.0;
          acc.score_time_ms_sum += elapsed_ms;
          ++acc.executed_cycles;
          if (data.fail_flag)
          {
            ++acc.fail_cycles;
          }
        }
      }
    }

    if (collect_stats && ++critic_stats_cycles_ >=
      static_cast<size_t>(critic_stats_publish_period_))
    {
      std::ostringstream out;
      out << std::fixed << std::setprecision(6);
      out << "{\"controller\":\"" << name_ << "\",\"cycles\":"
          << critic_stats_cycles_ << ",\"batch_size\":" << data.costs.size();
      out << ",\"path_points\":" << data.path.x.size();
      if (data.furthest_reached_path_point.has_value())
      {
        out << ",\"furthest_path_point\":"
            << data.furthest_reached_path_point.value();
      }
      else
      {
        out << ",\"furthest_path_point\":null";
      }
      if (data.path_pts_valid.has_value() && !data.path_pts_valid->empty())
      {
        const auto &valid = data.path_pts_valid.value();
        const size_t valid_count = static_cast<size_t>(
          std::count(valid.begin(), valid.end(), true));
        out << ",\"path_valid_ratio\":"
            << static_cast<double>(valid_count) / valid.size();
      }
      else
      {
        out << ",\"path_valid_ratio\":null";
      }
      out << ",\"final_fail_flag\":" << (data.fail_flag ? "true" : "false");
      out << ",\"critics\":[";
      for (size_t i = 0; i < critics_.size(); ++i)
      {
        if (i > 0u)
        {
          out << ',';
        }
        const auto &acc = critic_stats_accumulators_[i];
        const double denom = static_cast<double>(std::max<size_t>(1u, acc.executed_cycles));
        out << "{\"name\":\"" << critics_[i]->getName() << "\""
            << ",\"executed_cycles\":" << acc.executed_cycles
            << ",\"fail_cycles\":" << acc.fail_cycles
            << ",\"cost_mean\":" << acc.cost_mean_sum / denom
            << ",\"cost_max_mean\":" << acc.cost_max_sum / denom
            << ",\"changed_ratio\":" << acc.changed_ratio_sum / denom
            << ",\"score_time_mean_ms\":" << acc.score_time_ms_sum / denom
            << '}';
      }
      out << "]}";

      std_msgs::String message;
      message.data = out.str();
      critic_stats_pub_.publish(message);
      critic_stats_cycles_ = 0u;
      std::fill(
        critic_stats_accumulators_.begin(),
        critic_stats_accumulators_.end(),
        CriticStatsAccumulator{});
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

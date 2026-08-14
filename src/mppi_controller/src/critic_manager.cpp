
#include "mppi_controller/critic_manager.hpp"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <iomanip>
#include <limits>
#include <sstream>

#include <diagnostic_msgs/DiagnosticStatus.h>
#include <diagnostic_msgs/KeyValue.h>

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
    critic_cost_accumulators_.clear();
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
    critic_cost_accumulators_.resize(critics_.size());
    if (publish_critic_stats_)
    {
      critic_stats_pub_ = nh_.advertise<diagnostic_msgs::DiagnosticArray>(
        "critic_stats", 1, false);
      ROS_INFO(
        "[%s] lightweight MPPI critic statistics enabled every %d evaluated cycles",
        name_.c_str(), critic_stats_publish_period_);
    }
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
    if (!collect_stats && critic_stats_cycles_ != 0u)
    {
      resetCriticStatistics();
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
      const auto end = std::chrono::steady_clock::now();
      const double elapsed_ms =
        std::chrono::duration<double, std::milli>(end - start).count();
      if (timing_diagnostics_)
      {
        critic_time_totals_ms_[i] += elapsed_ms;
      }
      if (collect_stats)
      {
        critic_cost_accumulators_[i].add(costs_before, data.costs, elapsed_ms);
      }
    }

    if (collect_stats)
    {
      if (data.furthest_reached_path_point && data.path.x.size() > 0)
      {
        const size_t furthest_index = std::min(
          *data.furthest_reached_path_point,
          static_cast<size_t>(data.path.x.size() - 1));
        double furthest_arc = 0.0;
        for (size_t index = 1u; index <= furthest_index; ++index)
        {
          furthest_arc += std::hypot(
            data.path.x(index) - data.path.x(index - 1),
            data.path.y(index) - data.path.y(index - 1));
        }

        double max_candidate_arc = 0.0;
        const int trajectory_columns = data.trajectories.x.cols();
        if (data.trajectories.x.rows() > 0 && trajectory_columns > 1)
        {
          max_candidate_arc =
            ((data.trajectories.x.rightCols(trajectory_columns - 1) -
              data.trajectories.x.leftCols(trajectory_columns - 1)).square() +
             (data.trajectories.y.rightCols(trajectory_columns - 1) -
              data.trajectories.y.leftCols(trajectory_columns - 1)).square())
            .sqrt().rowwise().sum().maxCoeff();
        }

        ++path_progress_samples_;
        furthest_path_index_sum_ += static_cast<double>(furthest_index);
        furthest_path_arc_sum_ += furthest_arc;
        max_candidate_arc_sum_ += max_candidate_arc;
        furthest_path_index_max_ = std::max(
          furthest_path_index_max_, static_cast<double>(furthest_index));
        furthest_path_arc_max_ = std::max(furthest_path_arc_max_, furthest_arc);
        max_candidate_arc_max_ = std::max(max_candidate_arc_max_, max_candidate_arc);
      }
      ++critic_stats_cycles_;
      if (data.fail_flag)
      {
        ++critic_stats_fail_cycles_;
      }
      if (critic_stats_cycles_ >= static_cast<size_t>(critic_stats_publish_period_))
      {
        publishCriticStatistics();
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

  namespace
  {
  diagnostic_msgs::KeyValue keyValue(const std::string & key, const std::string & value)
  {
    diagnostic_msgs::KeyValue item;
    item.key = key;
    item.value = value;
    return item;
  }

  template<typename T>
  std::string numberString(const T value)
  {
    std::ostringstream stream;
    stream << std::setprecision(9) << value;
    return stream.str();
  }
  }  // namespace

  void CriticManager::publishCriticStatistics() const
  {
    diagnostic_msgs::DiagnosticArray message;
    message.header.stamp = ros::Time::now();
    message.status.reserve(critics_.size() + 1u);

    diagnostic_msgs::DiagnosticStatus overall;
    overall.name = name_ + "/MPPI critic statistics";
    overall.hardware_id = "mppi_controller";
    overall.level = critic_stats_fail_cycles_ == 0u ?
      diagnostic_msgs::DiagnosticStatus::OK : diagnostic_msgs::DiagnosticStatus::WARN;
    overall.message = critic_stats_fail_cycles_ == 0u ?
      "critic scoring window completed" : "one or more scoring cycles failed";
    overall.values.push_back(keyValue("window_cycles", numberString(critic_stats_cycles_)));
    overall.values.push_back(keyValue("failed_cycles", numberString(critic_stats_fail_cycles_)));
    if (path_progress_samples_ > 0u)
    {
      const double denominator = static_cast<double>(path_progress_samples_);
      overall.values.push_back(keyValue(
        "furthest_path_index_mean", numberString(furthest_path_index_sum_ / denominator)));
      overall.values.push_back(keyValue(
        "furthest_path_index_max", numberString(furthest_path_index_max_)));
      overall.values.push_back(keyValue(
        "furthest_path_arc_mean_m", numberString(furthest_path_arc_sum_ / denominator)));
      overall.values.push_back(keyValue(
        "furthest_path_arc_max_m", numberString(furthest_path_arc_max_)));
      overall.values.push_back(keyValue(
        "max_candidate_arc_mean_m", numberString(max_candidate_arc_sum_ / denominator)));
      overall.values.push_back(keyValue(
        "max_candidate_arc_max_m", numberString(max_candidate_arc_max_)));
    }
    message.status.push_back(std::move(overall));

    for (size_t index = 0u; index < critics_.size(); ++index)
    {
      const auto & accumulator = critic_cost_accumulators_[index];
      diagnostic_msgs::DiagnosticStatus status;
      status.name = name_ + "/MPPI critic/" + critics_[index]->getName();
      status.hardware_id = "mppi_controller";
      status.level = accumulator.nonfinite ?
        diagnostic_msgs::DiagnosticStatus::ERROR : diagnostic_msgs::DiagnosticStatus::OK;
      status.message = accumulator.nonfinite ? "non-finite cost contribution" :
        (accumulator.active_evaluations == 0u ? "inactive in this window" : "active");
      status.values.push_back(keyValue("evaluations", numberString(accumulator.evaluations)));
      status.values.push_back(keyValue(
        "active_evaluations", numberString(accumulator.active_evaluations)));
      status.values.push_back(keyValue("samples", numberString(accumulator.samples)));
      status.values.push_back(keyValue(
        "changed_samples", numberString(accumulator.changed_samples)));
      status.values.push_back(keyValue(
        "changed_ratio", numberString(accumulator.changedRatio())));
      status.values.push_back(keyValue("cost_sum", numberString(accumulator.cost_sum)));
      status.values.push_back(keyValue("cost_mean", numberString(accumulator.meanCost())));
      status.values.push_back(keyValue("cost_min", numberString(accumulator.minimumCost())));
      status.values.push_back(keyValue("cost_max", numberString(accumulator.maximumCost())));
      status.values.push_back(keyValue(
        "score_time_mean_ms", numberString(accumulator.meanElapsedMs())));
      message.status.push_back(std::move(status));
    }

    critic_stats_pub_.publish(message);
    resetCriticStatistics();
  }

  void CriticManager::resetCriticStatistics() const
  {
    critic_stats_cycles_ = 0u;
    critic_stats_fail_cycles_ = 0u;
    path_progress_samples_ = 0u;
    furthest_path_index_sum_ = 0.0;
    furthest_path_arc_sum_ = 0.0;
    max_candidate_arc_sum_ = 0.0;
    furthest_path_index_max_ = 0.0;
    furthest_path_arc_max_ = 0.0;
    max_candidate_arc_max_ = 0.0;
    for (auto & accumulator : critic_cost_accumulators_)
    {
      accumulator.reset();
    }
  }

} // namespace mppi

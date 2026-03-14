

#include "mppi_controller/critics/path_align_critic.hpp"

namespace mppi::critics
{

  void PathAlignCritic::initialize()
  {
    std::string param_prefix = name_ + "/";
    
    nh_.param(param_prefix + "cost_power", power_, 1);
    nh_.param(param_prefix + "cost_weight", weight_, 10.0f);

    nh_.param(param_prefix + "max_path_occupancy_ratio", max_path_occupancy_ratio_, 0.07f);
    nh_.param(param_prefix + "offset_from_furthest", offset_from_furthest_, 20);
    nh_.param(param_prefix + "trajectory_point_step", trajectory_point_step_, 4);
    nh_.param(param_prefix + "threshold_to_consider", threshold_to_consider_, 0.5f);
    nh_.param(param_prefix + "use_path_orientations", use_path_orientations_, false);

    ROS_INFO(
        "ReferenceTrajectoryCritic instantiated with %d power and %f weight",
        power_, weight_);
  }

  void PathAlignCritic::score(CriticData &data)
  {
    // 当接近目标时不使用，让goal critic接管
    if (!enabled_ || utils::withinPositionGoalTolerance(
                         threshold_to_consider_, data.state.pose.pose, data.goal))
    {
      return;
    }

    // 在首次获取相对于路径的方位时不应用
    utils::setPathFurthestPointIfNotSet(data);
    // 仅处理至最远点为止，路径处理器返回的最近路径点索引始终为 0
    const size_t path_segments_count = *data.furthest_reached_path_point;
    float path_segments_flt = static_cast<float>(path_segments_count);
    // 路径长度不足:如果最远可达路径点数量太少（小于offset_from_furthest_），则跳过评分
    if (path_segments_count < offset_from_furthest_)
    {
      return;
    }

    // 当动态障碍物阻塞局部路径时跳过评分
    utils::setPathCostsIfNotSet(data, costmap_ros_);
    std::vector<bool> &path_pts_valid = *data.path_pts_valid;
    float invalid_ctr = 0.0f;
    for (size_t i = 0; i < path_segments_count; i++)
    {
      if (!path_pts_valid[i])
      {
        invalid_ctr += 1.0f;
      }
      if (invalid_ctr / path_segments_flt > max_path_occupancy_ratio_ && invalid_ctr > 2.0f)
      {
        return;
      }
    }

    const size_t batch_size = data.trajectories.x.rows();
    Eigen::ArrayXf cost(data.costs.rows());
    cost.setZero();

    // 计算路径的累积距离
    std::vector<float> path_integrated_distances(path_segments_count, 0.0f);
    std::vector<utils::Pose2D> path(path_segments_count);
    float dx = 0.0f, dy = 0.0f;
    for (unsigned int i = 1; i != path_segments_count; i++)
    {
      auto &pose = path[i - 1];
      pose.x = data.path.x(i - 1);
      pose.y = data.path.y(i - 1);
      pose.theta = data.path.yaws(i - 1);

      dx = data.path.x(i) - pose.x;
      dy = data.path.y(i) - pose.y;
      path_integrated_distances[i] = path_integrated_distances[i - 1] + sqrtf(dx * dx + dy * dy);
    }

    // Finish populating the path vector
    auto &final_pose = path[path_segments_count - 1];
    final_pose.x = data.path.x(path_segments_count - 1);
    final_pose.y = data.path.y(path_segments_count - 1);
    final_pose.theta = data.path.yaws(path_segments_count - 1);

    float summed_path_dist = 0.0f, dyaw = 0.0f;
    unsigned int num_samples = 0u;
    unsigned int path_pt = 0u;
    float traj_integrated_distance = 0.0f;

    int strided_traj_rows = data.trajectories.x.rows();
    int strided_traj_cols = floor((data.trajectories.x.cols() - 1) / trajectory_point_step_) + 1;
    int outer_stride = strided_traj_rows * trajectory_point_step_;
    // 对轨迹进行采样（步长为 trajectory_point_step_ ）
    const auto T_x = Eigen::Map<const Eigen::ArrayXXf, 0,
                                Eigen::Stride<-1, -1>>(data.trajectories.x.data(),
                                                       strided_traj_rows, strided_traj_cols, Eigen::Stride<-1, -1>(outer_stride, 1));
    const auto T_y = Eigen::Map<const Eigen::ArrayXXf, 0,
                                Eigen::Stride<-1, -1>>(data.trajectories.y.data(),
                                                       strided_traj_rows, strided_traj_cols, Eigen::Stride<-1, -1>(outer_stride, 1));
    const auto T_yaw = Eigen::Map<const Eigen::ArrayXXf, 0,
                                  Eigen::Stride<-1, -1>>(data.trajectories.yaws.data(), strided_traj_rows, strided_traj_cols,
                                                         Eigen::Stride<-1, -1>(outer_stride, 1));
    const auto traj_sampled_size = T_x.cols();

    // 对每条轨迹计算对齐代价
    for (size_t t = 0; t < batch_size; ++t)
    {
      summed_path_dist = 0.0f;
      num_samples = 0u;
      traj_integrated_distance = 0.0f;
      path_pt = 0u;
      float Tx_m1 = T_x(t, 0);
      float Ty_m1 = T_y(t, 0);
      // 遍历采样的轨迹点
      for (int p = 1; p < traj_sampled_size; p++)
      {
        const float Tx = T_x(t, p);
        const float Ty = T_y(t, p);
        dx = Tx - Tx_m1;
        dy = Ty - Ty_m1;
        Tx_m1 = Tx;
        Ty_m1 = Ty;
        traj_integrated_distance += sqrtf(dx * dx + dy * dy);
        // 找到轨迹点在路径上的对应点（基于累积距离）
        path_pt = utils::findClosestPathPt(
            path_integrated_distances, traj_integrated_distance, path_pt);

        // 只对有效路径点（无障碍）计算对齐代价
        if (path_pts_valid[path_pt])
        {
          const auto &pose = path[path_pt];
          dx = pose.x - Tx;
          dy = pose.y - Ty;
          num_samples++;
          // 计算位置偏差和（可选）朝向偏差
          if (use_path_orientations_)
          {
            dyaw = angles::shortest_angular_distance(pose.theta, T_yaw(t, p));
            summed_path_dist += sqrtf(dx * dx + dy * dy + dyaw * dyaw);
          }
          else
          {
            summed_path_dist += sqrtf(dx * dx + dy * dy);
          }
        }
      }
      // 计算平均对齐代价
      if (num_samples > 0u)
      {
        cost(t) = summed_path_dist / static_cast<float>(num_samples);
      }
      else
      {
        cost(t) = 0.0f;
      }
    }

    // 应用权重和指数变换
    if (power_ > 1u)
    {
      data.costs += (cost * weight_).pow(power_).eval();
    }
    else
    {
      data.costs += (cost * weight_).eval();
    }
  }

} // namespace mppi::critics

#include <pluginlib/class_list_macros.hpp>

PLUGINLIB_EXPORT_CLASS(
    mppi::critics::PathAlignCritic,
    mppi::critics::CriticFunction)



#include <cmath>
#include "mppi_controller/critics/cost_critic.hpp"

namespace mppi::critics
{

void CostCritic::initialize()
{
  std::string param_prefix = name_ + "/";

  nh_.param(param_prefix + "consider_footprint", consider_footprint_, false);
  nh_.param(param_prefix + "cost_power", power_, 1);
  nh_.param(param_prefix + "cost_weight", weight_, 3.81f);
  nh_.param(param_prefix + "critical_cost", critical_cost_, 300.0f);
  nh_.param(param_prefix + "collision_cost", collision_cost_, 1000000.0f);
  nh_.param(param_prefix + "near_goal_distance", near_goal_distance_, 0.5f);
  nh_.param(param_prefix + "inflation_layer_name", inflation_layer_name_, std::string(""));
  nh_.param(param_prefix + "trajectory_point_step", trajectory_point_step_, 2);

  // 将权重归一化到与其他权重相同范围内
  weight_ /= 254.0f;

  // When footprint collision checking is disabled, there is no reason to
  // inspect the layered costmap at startup. That extra walk is both slow and,
  // in some bringup sequences, can observe an invalid plugin pointer.
  if (consider_footprint_) {
    possible_collision_cost_ = findCircumscribedCost(costmap_ros_);
  } else {
    possible_collision_cost_ = -1.0f;
  }

  if (consider_footprint_ && possible_collision_cost_ < 1.0f) {
    ROS_ERROR(
      "Inflation layer either not found or inflation is not set sufficiently for "
      "optimized non-circular collision checking capabilities. It is HIGHLY recommended to set"
      " the inflation radius to be at MINIMUM half of the robot's largest cross-section. See "
      "github.com/ros-planning/navigation2/tree/main/nav2_smac_planner#potential-fields"
      " for full instructions. This will substantially impact run-time performance.");
  }

  ROS_INFO(
    "Cost Critic with %d power and %f / %f weights. "
    "Critic will collision check based on %s cost.",
    power_, critical_cost_, weight_, consider_footprint_ ?
    "footprint" : "circular ");
}

float CostCritic::findCircumscribedCost(
  std::shared_ptr<costmap_2d::Costmap2DROS> costmap)
{
  if (!costmap) {
    ROS_WARN("CostCritic: costmap_ros is null while looking for inflation layer.");
    return -1.0f;
  }

  auto * layered_costmap = costmap->getLayeredCostmap();
  if (!layered_costmap) {
    ROS_WARN("CostCritic: layered costmap is null while looking for inflation layer.");
    return -1.0f;
  }

  double result = -1.0;
  const double circum_radius = layered_costmap->getCircumscribedRadius();
  if (static_cast<float>(circum_radius) == circumscribed_radius_) {
    // early return if footprint size is unchanged
    return circumscribed_cost_;
  }

  costmap_2d::InflationLayer* inflation_layer = nullptr;
  auto * plugins = layered_costmap->getPlugins();
  if (!plugins) {
    ROS_WARN("CostCritic: layered costmap plugin container is null.");
    return -1.0f;
  }

  for (const auto & layer : *plugins) {
    if (!layer) {
      ROS_WARN("CostCritic: encountered null layer plugin, skipping.");
      continue;
    }

    const std::string & layer_name = layer->getName();
    ROS_INFO("layer: %s", layer_name.c_str());

    auto * candidate = dynamic_cast<costmap_2d::InflationLayer *>(layer.get());
    if (!candidate) {
      continue;
    }

    if (inflation_layer_name_.empty() || layer_name == inflation_layer_name_) {
      inflation_layer = candidate;
      break;
    }
  }

  if (inflation_layer != nullptr) {
    const double resolution = costmap->getCostmap()->getResolution();
    result = inflation_layer->computeCost(circum_radius / resolution);
  } else {
    ROS_WARN(
      "No inflation layer found in costmap configuration. "
      "If this is an SE2-collision checking plugin, it cannot use costmap potential "
      "field to speed up collision checking by only checking the full footprint "
      "when robot is within possibly-inscribed radius of an obstacle. This may "
      "significantly slow down planning times and not avoid anything but absolute collisions!");
  }

  circumscribed_radius_ = static_cast<float>(circum_radius);
  circumscribed_cost_ = static_cast<float>(result);

  return circumscribed_cost_;
}

void CostCritic::score(CriticData & data)
{
  if (!enabled_) {
    return;
  }

  // Setup cost information for various parts of the critic
  is_tracking_unknown_ = costmap_ros_->getLayeredCostmap()->isTrackingUnknown();
  auto * costmap = costmap_ros_->getCostmap();
  origin_x_ = static_cast<float>(costmap->getOriginX());
  origin_y_ = static_cast<float>(costmap->getOriginY());
  resolution_ = static_cast<float>(costmap->getResolution());
  size_x_ = costmap->getSizeInCellsX();
  size_y_ = costmap->getSizeInCellsY();

  // 默认不考虑footprint
  if (consider_footprint_) {
    // footprint may have changed since initialization if user has dynamic footprints
    possible_collision_cost_ = findCircumscribedCost(costmap_ros_);
  }

  // 如果靠近目标，则不要应用优先项，因为目标接近障碍物
  bool near_goal = false;
  if (utils::withinPositionGoalTolerance(near_goal_distance_, data.state.pose.pose, data.goal)) {
    near_goal = true;
  }

  Eigen::ArrayXf repulsive_cost(data.costs.rows());
  repulsive_cost.setZero();
  bool all_trajectories_collide = true;

  int strided_traj_cols = floor((data.trajectories.x.cols() - 1) / trajectory_point_step_) + 1;
  int strided_traj_rows = data.trajectories.x.rows();
  int outer_stride = strided_traj_rows * trajectory_point_step_;

  // 采样后的轨迹（使用Eigen::Map直接映射）
  const auto traj_x = Eigen::Map<const Eigen::ArrayXXf, 0,
      Eigen::Stride<-1, -1>>(data.trajectories.x.data(), strided_traj_rows, strided_traj_cols,
      Eigen::Stride<-1, -1>(outer_stride, 1));
  const auto traj_y = Eigen::Map<const Eigen::ArrayXXf, 0,
      Eigen::Stride<-1, -1>>(data.trajectories.y.data(), strided_traj_rows, strided_traj_cols,
      Eigen::Stride<-1, -1>(outer_stride, 1));
  const auto traj_yaw = Eigen::Map<const Eigen::ArrayXXf, 0,
      Eigen::Stride<-1, -1>>(data.trajectories.yaws.data(), strided_traj_rows, strided_traj_cols,
      Eigen::Stride<-1, -1>(outer_stride, 1));

  for (int i = 0; i < strided_traj_rows; ++i) {
    bool trajectory_collide = false;
    float pose_cost = 0.0f;
    float & traj_cost = repulsive_cost(i);

    for (int j = 0; j < strided_traj_cols; j++) {
      float Tx = traj_x(i, j);
      float Ty = traj_y(i, j);
      unsigned int x_i = 0u, y_i = 0u;

      // The getCost doesn't use orientation
      // The footprintCostAtPose will always return "INSCRIBED" if footprint is over it
      // So the center point has more information than the footprint
      // 直接使用中心点代价，而不是使用footprint
      if (!worldToMapFloat(Tx, Ty, x_i, y_i)) {
        if (!is_tracking_unknown_) {
          traj_cost = collision_cost_;
          trajectory_collide = true;
          break;
        }
        pose_cost = 255.0f;  // NO_INFORMATION in float
      } else {
        pose_cost = static_cast<float>(costmap->getCost(x_i, y_i));
        if (pose_cost < 1.0f) {
          continue;  // In free space
        }
        if (inCollision(pose_cost, Tx, Ty, traj_yaw(i, j))) {
          traj_cost = collision_cost_;
          trajectory_collide = true;
          break;
        }
      }

      // Let near-collision trajectory points be punished severely
      // Note that we collision check based on the footprint actual,
      // but score based on the center-point cost regardless
      // 远离靠近障碍的轨迹点
      if (pose_cost >= 253.0f /*INSCRIBED_INFLATED_OBSTACLE in float*/) {
        traj_cost += critical_cost_;
      } else if (!near_goal) {
        traj_cost += pose_cost;
      }
    }

    if (!trajectory_collide) {
      all_trajectories_collide = false;
    }
  }

  if (power_ > 1u) {
    data.costs += (repulsive_cost *
      (weight_ / static_cast<float>(strided_traj_cols))).pow(power_);
  } else {
    data.costs += repulsive_cost * (weight_ / static_cast<float>(strided_traj_cols));
  }

  data.fail_flag = all_trajectories_collide;
}

}  // namespace mppi::critics

#include <pluginlib/class_list_macros.hpp>

PLUGINLIB_EXPORT_CLASS(
  mppi::critics::CostCritic,
  mppi::critics::CriticFunction)

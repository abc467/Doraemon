#ifndef A_STAR_ROS_WRAPPER_H_
#define A_STAR_ROS_WRAPPER_H_

#include <atomic>

#include <ros/ros.h>
#include <nav_core/base_global_planner.h>
#include <mbf_costmap_core/costmap_planner.h>
#include <costmap_2d/cost_values.h>
#include <costmap_2d/costmap_2d_ros.h>
#include <geometry_msgs/PoseStamped.h>
#include <nav_msgs/Path.h>
#include <pluginlib/class_list_macros.h>
#include <visualization_msgs/Marker.h>

#include <memory>
#include <string>
#include <vector>

#include "a_star_planner/theta_star_planner.h"
#include "a_star_planner/node.h"
#include "a_star_planner/collision_checker.h"

namespace a_star_planner
{

class AStarROSWrapper : public nav_core::BaseGlobalPlanner, public mbf_costmap_core::CostmapPlanner
{
public:
  enum class PlanStatus
  {
    SUCCESS,
    CANCELED,
    TIMED_OUT,
    INVALID_START,
    INVALID_GOAL,
    NO_PATH,
    EMPTY_PATH,
    INTERNAL_ERROR,
  };

  struct PlanResult
  {
    PlanStatus status{PlanStatus::INTERNAL_ERROR};
    double cost{0.0};
    std::string message;
    std::vector<geometry_msgs::PoseStamped> plan;
  };

  AStarROSWrapper();
  ~AStarROSWrapper() = default;

  void initialize(std::string name, costmap_2d::Costmap2DROS* costmap_ros) override;

  bool makePlan(const geometry_msgs::PoseStamped& start,
                const geometry_msgs::PoseStamped& goal,
                std::vector<geometry_msgs::PoseStamped>& plan) override;

  uint32_t makePlan(const geometry_msgs::PoseStamped& start,
                    const geometry_msgs::PoseStamped& goal,
                    double tolerance,
                    std::vector<geometry_msgs::PoseStamped>& plan,
                    double& cost,
                    std::string& message) override;

  bool cancel() override;

private:
  using Point = rmp::path_planner::PathPlanner::Point3d;
  using Path  = rmp::path_planner::PathPlanner::Points3d;
  using Node  = rmp::common::structure::Node<int>;

  PlanResult makePlanCommon(const geometry_msgs::PoseStamped& start,
                            const geometry_msgs::PoseStamped& goal,
                            double tolerance);
  bool resolveGoalPose(const geometry_msgs::PoseStamped& goal,
                       double tolerance,
                       geometry_msgs::PoseStamped& resolved_goal,
                       std::string& message) const;
  bool hasLineOfSight(const geometry_msgs::PoseStamped& start,
                      const geometry_msgs::PoseStamped& goal) const;
  bool isPoseTraversable(const geometry_msgs::PoseStamped& pose) const;
  bool isCellTraversable(unsigned int mx, unsigned int my) const;
  double computePlanCost(const std::vector<geometry_msgs::PoseStamped>& plan) const;
  uint32_t toMBFOutcome(PlanStatus status) const;
  Path postProcessPath(const Path& sparse_path);

  void publishMarkers(const Path& path, int id, float r, float g, float b, const std::string& ns);
  void publishMarkers(const std::vector<geometry_msgs::PoseStamped>& path, int id, float r, float g, float b, const std::string& ns);

  // --- 工程化参数：末尾追加“goal yaw 点” ---
  bool   append_goal_orientation_point_{true};
  double goal_append_pos_tolerance_{1e-3};   // 如果最终点不在 goal 上，允许补一个 goal 点
  double goal_yaw_append_threshold_{1e-3};   // yaw 差小于该阈值就不追加末端 yaw 点

  bool prefer_straight_line_{true};
  bool validate_final_path_{false};
  bool record_expansion_{false};
  int cancel_check_interval_{1024};
  double max_planning_time_s_{0.0};

  bool initialized_{false};
  std::unique_ptr<rmp::path_planner::ThetaStarPathPlanner> planner_;
  costmap_2d::Costmap2DROS* costmap_ros_{nullptr};
  std::string frame_id_;

  double obstacle_factor_{0.08};
  int    final_path_collision_threshold_{200};
  double interpolation_resolution_{0.1};
  double smoothing_factor_{0.4};
  bool   enable_post_processing_{true};

  ros::Publisher plan_pub_;
  ros::Publisher waypoints_pub_;
  ros::Publisher raw_waypoints_pub_;
  ros::Publisher smoothed_waypoints_pub_;
  std::atomic_bool cancel_requested_{false};
};

}  // namespace a_star_planner

#endif  // A_STAR_ROS_WRAPPER_H_

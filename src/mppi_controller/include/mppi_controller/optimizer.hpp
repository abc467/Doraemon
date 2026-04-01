#pragma once

#include <Eigen/Dense>

#include <string>
#include <memory>

#include "costmap_2d/costmap_2d_ros.h"
#include "geometry_msgs/PoseStamped.h"
#include "geometry_msgs/TwistStamped.h"
#include "nav_msgs/Path.h"

#include "mppi_controller/models/optimizer_settings.hpp"
#include "mppi_controller/motion_models.hpp"
#include "mppi_controller/critic_manager.hpp"
#include "mppi_controller/models/state.hpp"
#include "mppi_controller/models/trajectories.hpp"
#include "mppi_controller/models/path.hpp"
#include "mppi_controller/tools/noise_generator.hpp"
#include "mppi_controller/tools/utils.hpp"

namespace mppi
{

/**
 * @class mppi::Optimizer
 * @brief MPPI控制器主优化算法
 */
class Optimizer
{
public:
  Optimizer() = default;

  ~Optimizer() {shutdown();}


  /**
   * @brief Initializes optimizer on startup
   * @param nh
   * @param name 控制器名称
   * @param costmap_ros Costmap2DROS object of environment
   */
  void initialize(
    const ros::NodeHandle& nh, const std::string & name,
    std::shared_ptr<costmap_2d::Costmap2DROS> costmap_ros);

  /**
   * @brief Shutdown for optimizer at process end
   */
  void shutdown();

  /**
   * @brief 使用MPPI计算控制指令
   * @param robot_pose Pose of the robot at given time
   * @param robot_speed Speed of the robot at given time
   * @param plan Path plan to track
   * @param goal Given Goal pose to reach.
   * @return TwistStamped of the MPPI control
   */
  geometry_msgs::TwistStamped evalControl(
    const geometry_msgs::PoseStamped & robot_pose,
    const geometry_msgs::Twist & robot_speed, const nav_msgs::Path & plan,
    const geometry_msgs::Pose & goal);

  /**
   * @brief 用于可视化的所有轨迹
   * @return Set of trajectories evaluated in cycle
   */
  models::Trajectories & getGeneratedTrajectories();

  /**
   * @brief 用于可视化的最优轨迹
   * @return Optimal trajectory
   */
  Eigen::ArrayXXf getOptimizedTrajectory();

  /**
   * @brief (ROS1中不使用) Set the maximum speed based on the speed limits callback
   * @param speed_limit Limit of the speed for use
   * @param percentage Whether the speed limit is absolute or relative
   */
  void setSpeedLimit(double speed_limit, bool percentage);

  /**
   * @brief Reset the optimization problem to initial conditions
   */
  void reset();

protected:
  /**
   * @brief Main function to generate, score, and return trajectories
   */
  void optimize();

  /**
   * @brief Prepare 每次请求时，先初始化状态信息用于轨迹推演
   * @param robot_pose Pose of the robot at given time
   * @param robot_speed Speed of the robot at given time
   * @param plan Path plan to track
   */
  void prepare(
    const geometry_msgs::PoseStamped & robot_pose,
    const geometry_msgs::Twist & robot_speed,
    const nav_msgs::Path & plan,
    const geometry_msgs::Pose & goal);

  /**
   * @brief 从参数服务器中提取控制器参数
   */
  void getParams();

  /**
   * @brief Set the motion model of the vehicle platform
   * @param model Model string to use
   */
  void setMotionModel(const std::string & model);

  /**
   * @brief 平移最优控制序列用于下一次迭代的热启动
   */
  void shiftControlSequence();

  /**
   * @brief 从上一个循环的最优控制序列生成噪声轨迹
   */
  void generateNoisedTrajectories();

  /**
   * @brief 对控制序列应用硬车辆约束
   */
  void applyControlSequenceConstraints();

  /**
   * @brief 根据当前速度以及控制速度序列，更新state中的速度状态
   * @param state fill state with velocities on each step
   */
  void updateStateVelocities(models::State & state) const;

  /**
   * @brief 根据当前速度，更新state中的初始速度
   * @param state fill state
   */
  void updateInitialStateVelocities(models::State & state) const;

  /**
   * @brief 根据控制速度序列，更新state中的速度状态
   * @param state fill state
   */
  void propagateStateVelocitiesFromInitials(models::State & state) const;

  /**
   * @brief 根据速度序列推演出轨迹序列
   * @param trajectories to rollout
   * @param state fill state
   */
  void integrateStateVelocities(
    models::Trajectories & trajectories,
    const models::State & state) const;

  /**
   * @brief 根据速度序列推演出轨迹序列
   * @param trajectories to rollout
   * @param state fill state
   */
  void integrateStateVelocities(
    Eigen::Array<float, Eigen::Dynamic, 3> & trajectories,
    const Eigen::ArrayXXf & state) const;

  /**
   * @brief 更新控制序列: 
   * 1. 计算每个轨迹的噪声偏差; 
   * 2. 惩罚偏差较大的轨迹; 
   * 3. 计算软最大化权重并计算加权平均得到最优控制序列;
   * 4. 对控制序列施加约束;
   */
  void updateControlSequence();

  /**
   * @brief 提取控制序列中的最优控制
   * @param stamp Timestamp to use
   * @return TwistStamped of command to send to robot base
   */
  geometry_msgs::TwistStamped
  getControlFromSequenceAsTwist(const ros::Time& stamp);

  /**
   * @brief Whether the motion model is holonomic
   * @return Bool if holonomic to populate `y` axis of state
   */
  bool isHolonomic() const;

  /**
   * @brief 使用控制频率和时间步长，确定是否应使用偏移量来填充下一个循环的初始状态
   * @param controller_frequency Frequency of controller
   */
  void setOffset(double controller_frequency);

  /**
   * @brief 执行回退行为用于 尝试从一组轨迹中恢复
   * @param fail Whether the system failed to recover from 
   */
  bool fallback(bool fail);

protected:
  ros::NodeHandle nh_;
  std::shared_ptr<costmap_2d::Costmap2DROS> costmap_ros_;
  costmap_2d::Costmap2D * costmap_;
  std::string name_;

  std::shared_ptr<MotionModel> motion_model_;

  CriticManager critic_manager_;
  NoiseGenerator noise_generator_;

  models::OptimizerSettings settings_;

  models::State state_;
  models::ControlSequence control_sequence_;
  std::array<mppi::models::Control, 4> control_history_;
  models::Trajectories generated_trajectories_;
  models::Path path_;
  geometry_msgs::Pose goal_;
  Eigen::ArrayXf costs_;

  CriticData critics_data_ = {
    state_, generated_trajectories_, path_, goal_,
    costs_, settings_.model_dt, false, nullptr,
    std::nullopt, std::nullopt};
};

}  // namespace mppi

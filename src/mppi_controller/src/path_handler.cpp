#include "mppi_controller/tools/path_handler.hpp"

namespace mppi
{

  void PathHandler::initialize(const ros::NodeHandle &nh,
                               const std::string &name,
                               std::shared_ptr<costmap_2d::Costmap2DROS> costmap,
                               std::shared_ptr<tf2_ros::Buffer> buffer)
  {
    nh_ = nh;
    costmap_ = costmap;
    tf_buffer_ = buffer;

    nh.param("max_robot_pose_search_dist", max_robot_pose_search_dist_, getMaxCostmapDist()); // 对应的param为 自动添加的前缀"/move_base_flex/name/" + "max_robot_pose_search_dist"；
    nh.param("prune_distance", prune_distance_, 1.5);
    nh.param("transform_tolerance", transform_tolerance_, 0.1);
  }

  std::pair<nav_msgs::Path, PathIterator>
  PathHandler::getGlobalPlanConsideringBoundsInCostmapFrame(
      const geometry_msgs::PoseStamped &global_pose)
  {
    auto begin = global_plan_up_to_inversion_.poses.begin();

    // 搜索最近点（限制搜索范围）
    auto closest_pose_upper_bound = utils::first_after_integrated_distance(
        global_plan_up_to_inversion_.poses.begin(), global_plan_up_to_inversion_.poses.end(), max_robot_pose_search_dist_);

    // 在begin ~ closest_pose_upper_bound范围内，找到离robot最近的点
    auto closest_point = utils::min_by(begin, closest_pose_upper_bound, [&global_pose](const geometry_msgs::PoseStamped &ps)
                                       { return utils::euclidean_distance(global_pose.pose, ps.pose); });

    nav_msgs::Path transformed_plan;
    transformed_plan.header.frame_id = costmap_->getGlobalFrameID(); // odom 坐标系
    transformed_plan.header.stamp = global_pose.header.stamp;

    // 从最近点开始，保留累计距离不超过 prune_distance_ 的路径点，避免处理过长路径
    auto pruned_plan_end = utils::first_after_integrated_distance(closest_point, global_plan_up_to_inversion_.poses.end(), prune_distance_);

    unsigned int mx, my;
    // 考虑代价地图边界，裁剪出局部路径
    // 将路径点从全局坐标系转换到代价地图坐标系
    for (auto global_plan_pose = closest_point; global_plan_pose != pruned_plan_end; ++global_plan_pose)
    {
      // Transform from global plan frame to costmap frame
      geometry_msgs::PoseStamped costmap_plan_pose;
      global_plan_pose->header.stamp = global_pose.header.stamp;
      global_plan_pose->header.frame_id = global_plan_.header.frame_id;
      transformPose(costmap_->getGlobalFrameID(), *global_plan_pose, costmap_plan_pose);

      // 检查是否在costmap内
      if (!costmap_->getCostmap()->worldToMap(
              costmap_plan_pose.pose.position.x, costmap_plan_pose.pose.position.y, mx, my))
      {
        return {transformed_plan, closest_point};
      }

      // 填充局部路径transformed_plan
      transformed_plan.poses.push_back(costmap_plan_pose);
    }

    return {transformed_plan, closest_point};
  }

  geometry_msgs::PoseStamped PathHandler::transformToGlobalPlanFrame(
      const geometry_msgs::PoseStamped &pose)
  {
    if (global_plan_up_to_inversion_.poses.empty())
    {
      throw std::invalid_argument("Received plan with zero length");
    }

    geometry_msgs::PoseStamped robot_pose;
    if (!transformPose(global_plan_up_to_inversion_.header.frame_id, pose, robot_pose))
    {
      throw std::runtime_error(std::string("Unable to transform robot pose into global plan's frame: "));
    }

    return robot_pose;
  }

  nav_msgs::Path PathHandler::transformPath(
      const geometry_msgs::PoseStamped &robot_pose)
  {
    // Find relevant bounds of path to use
    geometry_msgs::PoseStamped global_pose = transformToGlobalPlanFrame(robot_pose);
    auto [transformed_plan, lower_bound] = getGlobalPlanConsideringBoundsInCostmapFrame(global_pose);

    prunePlan(global_plan_up_to_inversion_, lower_bound);

    if (transformed_plan.poses.empty())
    {
      throw std::invalid_argument("Resulting plan has 0 poses in it.");
    }

    return transformed_plan;
  }

  bool PathHandler::transformPose(
      const std::string &frame, const geometry_msgs::PoseStamped &in_pose,
      geometry_msgs::PoseStamped &out_pose) const
  {
    if (in_pose.header.frame_id == frame)
    {
      out_pose = in_pose;
      return true;
    }

    try
    {
      tf_buffer_->transform(in_pose, out_pose, frame, ros::Duration(0.5));
      out_pose.header.frame_id = frame;
      return true;
    }
    catch (tf2::TransformException &ex)
    {
      ROS_ERROR("Exception in transformPose: %s", ex.what());
    }
    return false;
  }

  double PathHandler::getMaxCostmapDist()
  {
    const auto &costmap = costmap_->getCostmap();
    return static_cast<double>(std::max(costmap->getSizeInCellsX(), costmap->getSizeInCellsY())) *
           costmap->getResolution() * 0.50;
  }

  void PathHandler::setPath(const nav_msgs::Path &plan)
  {
    global_plan_ = plan;
    global_plan_up_to_inversion_ = global_plan_;
  }

  nav_msgs::Path &PathHandler::getPath() { return global_plan_; }

  void PathHandler::prunePlan(nav_msgs::Path &plan, const PathIterator end)
  {
    plan.poses.erase(plan.poses.begin(), end);
  }

  geometry_msgs::PoseStamped PathHandler::getTransformedGoal()
  {
    auto goal = global_plan_.poses.back();
    goal.header.frame_id = global_plan_.header.frame_id;
    if (goal.header.frame_id.empty())
    {
      ROS_ERROR("Goal pose has an empty frame_id");
    }
    geometry_msgs::PoseStamped transformed_goal;
    if (!transformPose(costmap_->getGlobalFrameID(), goal, transformed_goal))
    {
      ROS_ERROR("Unable to transform goal pose into costmap frame");
    }
    return transformed_goal;
  }

}
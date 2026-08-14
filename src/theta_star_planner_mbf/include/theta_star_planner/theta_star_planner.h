#pragma once

#include <ros/ros.h>
#include <costmap_2d/costmap_2d_ros.h>
#include <costmap_2d/costmap_2d.h>
#include <base_local_planner/costmap_model.h>
#include <mbf_costmap_core/costmap_planner.h>  // mbf接口
#include <mbf_msgs/GetPathResult.h>
#include <geometry_msgs/PoseStamped.h>
#include <angles/angles.h>
#include <nav_msgs/Path.h>
#include "theta_star_planner/theta_star.h"
#include "theta_star_planner/path_tools.h"
#include "theta_star_planner/se2_path_refiner.h"
#include <tf2_ros/transform_listener.h>
#include <tf2_geometry_msgs/tf2_geometry_msgs.h>
#include <tf2/utils.h>
#include <vector>
#include <optional>
#include <functional>

namespace mbf_global_planner
{
class ThetaStarPlanner : public mbf_costmap_core::CostmapPlanner
{
public:
    ThetaStarPlanner();
    ThetaStarPlanner(std::string name, costmap_2d::Costmap2DROS* costmap_ros);
    void initialize(std::string name, costmap_2d::Costmap2DROS* costmap_ros);
    uint32_t makePlan(const geometry_msgs::PoseStamped& start, const geometry_msgs::PoseStamped& goal,
                      double tolerance, std::vector<geometry_msgs::PoseStamped>& plan, double &cost,
                      std::string &message);
    bool cancel();
    
protected:
    std::string name_;
    bool initialized_ = false;
    costmap_2d::Costmap2DROS* costmap_ros_;
    costmap_2d::Costmap2D* costmap_;
    std::unique_ptr<theta_star::ThetaStar> planner_;
    ros::Publisher path_pub_;
    ros::Publisher path_pub_raw_;

    std::atomic<bool> cancel_requested_{false};  // 取消请求标志（原子变量，线程安全）
    std::atomic<bool> is_planning_{false};       // 规划状态标志（是否正在规划）

    // 新增：历史路径复用相关变量
    geometry_msgs::PoseStamped last_goal_;  // 上一次规划的终点
    std::vector<geometry_msgs::PoseStamped> last_valid_path_;  // 上一次的有效路径
    ros::Time last_path_timestamp_;  // 上一次路径的生成时间（可选，用于过期判断）
    double goal_tolerance_ = 0.05;  // 终点位置容忍误差（米），可从参数加载
    double path_check_interval_ = 0.05;  // 路径碰撞检查的间隔（米）
    double path_max_age_ = 20.0;
    bool use_footprint_path_check_ = true;
    double goal_orientation_tolerance_ = 0.05;

    // Theta* first guarantees XY reachability. The terminal layer then reshapes
    // the suffix of that successful path so its geometric tangent agrees with
    // the requested goal yaw. It is deliberately non-fatal: Connect MPPI can
    // still optimize the reachable base path if no reshaped suffix is safe.
    bool terminal_approach_enabled_ = true;
    double terminal_straight_length_ = 0.40;
    double terminal_min_straight_length_ = 0.0;
    double terminal_straight_length_step_ = 0.10;
    // Zero means differential-drive mode: no car-like minimum turn radius.
    // A positive value retains the optional curvature/Dubins constraint for
    // other chassis types.
    double terminal_min_turn_radius_ = 0.0;
    double terminal_sample_step_ = 0.05;
    double terminal_max_prefix_splice_distance_ = 1.80;
    theta_star::SE2RefinerConfig se2_refiner_config_;

    static std::vector<geometry_msgs::PoseStamped> linearInterpolation(
        const std::vector<coordsW> & raw_path, const double & dist_bw_points);

    void removeClosePoses(std::vector<geometry_msgs::PoseStamped>& path, double min_distance);
    std::vector<geometry_msgs::PoseStamped> downsamplePath(const std::vector<geometry_msgs::PoseStamped>& orig_global_plan, double sampling_distance);
    std::optional<std::vector<geometry_msgs::PoseStamped>> smoothPath(const std::vector<geometry_msgs::PoseStamped>& orig_global_plan);
    void printPoseStampedVector(const std::vector<geometry_msgs::PoseStamped>& poses);
    bool generatePositionPath(
        const geometry_msgs::PoseStamped& start,
        const geometry_msgs::PoseStamped& position_goal,
        std::vector<geometry_msgs::PoseStamped>& path,
        std::string& failure_reason);
    std::vector<double> terminalStraightLengths() const;
    bool isCentrelineCollisionFree(
        const std::vector<geometry_msgs::PoseStamped>& path);

    static void assignPathOrientations(
        std::vector<geometry_msgs::PoseStamped>& path,
        const std::optional<double>& final_yaw = std::nullopt);
    static double maximumDiscreteCurvature(
        const std::vector<geometry_msgs::PoseStamped>& path);
    static std::optional<std::vector<geometry_msgs::PoseStamped>>
    buildTerminalApproachGeometry(
        const std::vector<geometry_msgs::PoseStamped>& prefix,
        const geometry_msgs::PoseStamped& goal,
        double straight_length,
        double minimum_turn_radius,
        double sample_step,
        double maximum_prefix_splice_distance,
        const std::function<bool(
            const std::vector<geometry_msgs::PoseStamped>&)>& validator);

    bool canReusePath(const geometry_msgs::PoseStamped& current_start, 
                                        const geometry_msgs::PoseStamped& current_goal) {
        // 条件1：历史路径为空 → 不可复用
        if (last_valid_path_.empty()) {
            return false;
        }

        // 条件2：新终点与历史终点不一致（位置误差超过容忍阈值）→ 不可复用
        double goal_dist = hypot(
            current_goal.pose.position.x - last_goal_.pose.position.x,
            current_goal.pose.position.y - last_goal_.pose.position.y
        );
        if (goal_dist > goal_tolerance_) {
            ROS_DEBUG("Goal changed (distance: %.2f m), cannot reuse path", goal_dist);
            return false;
        }
        const double goal_yaw_error = angles::shortest_angular_distance(
            tf2::getYaw(last_goal_.pose.orientation),
            tf2::getYaw(current_goal.pose.orientation));
        if (std::fabs(goal_yaw_error) > goal_orientation_tolerance_) {
            ROS_DEBUG(
                "Goal orientation changed (error: %.3f rad), cannot reuse path",
                goal_yaw_error);
            return false;
        }

        // 条件3：检查历史路径是否无碰撞 → 遍历路径点，判断是否在障碍物区域
        if (!isPathCollisionFree(last_valid_path_)) {
            ROS_DEBUG("Last path has collision, cannot reuse");
            return false;
        }

        // （可选）条件4：路径未过期
        if ((ros::Time::now() - last_path_timestamp_).toSec() > path_max_age_) {
            ROS_DEBUG("Last path expired (age: %.2f s), cannot reuse", (ros::Time::now() - last_path_timestamp_).toSec());
            return false;
        }

        return true;  // 满足所有条件，可复用
    }

    bool isPathCollisionFree(const std::vector<geometry_msgs::PoseStamped>& path) {
        if (!isCentrelineCollisionFree(path)) {
            return false;
        }
        if (!use_footprint_path_check_) {
            return true;
        }
        const auto footprint = costmap_ros_ ?
            costmap_ros_->getRobotFootprint() :
            std::vector<geometry_msgs::Point>{};
        if (footprint.size() < 3) {
            return false;
        }
        return !theta_star::SE2PathRefiner::firstUnsafeSegment(
            *costmap_, footprint, path,
            std::min(path_check_interval_, 0.5 * costmap_->getResolution()),
            false).has_value();
    }

    bool cropPathToStart(const geometry_msgs::PoseStamped& current_start, 
                                        const std::vector<geometry_msgs::PoseStamped>& last_path,
                                        std::vector<geometry_msgs::PoseStamped>& cropped_path) {
        cropped_path.clear();
        if (last_path.size() < 2) {
            return false;
        }
        // 找到历史路径中与当前起点最近的点（作为裁剪起点）
        size_t closest_idx = 0;
        double min_dist = INFINITY;
        for (size_t i = 0; i < last_path.size(); ++i) {
            double dist = hypot(
                current_start.pose.position.x - last_path[i].pose.position.x,
                current_start.pose.position.y - last_path[i].pose.position.y
            );
            if (dist < min_dist) {
                min_dist = dist;
                closest_idx = i;
            }
        }

        // 若最近点距离过远（如 > 0.5米），说明当前起点不在历史路径附近 → 裁剪无效
        if (min_dist > 0.5) {  // 可作为参数调整
            ROS_DEBUG("Current start is too far from last path (%.2f m), cannot crop", min_dist);
            return false;
        }

        // A one-pose suffix has no approach geometry. Replacing that sole pose
        // with current_start used to discard the exact goal position/yaw and
        // turn a periodic replan into a false success at the robot pose.
        if (closest_idx + 1 >= last_path.size()) {
            ROS_DEBUG("Cached suffix has no terminal geometry; replanning");
            return false;
        }

        // 裁剪：从最近点到终点的路径
        for (size_t i = closest_idx; i < last_path.size(); ++i) {
            cropped_path.push_back(last_path[i]);
        }

        // 确保裁剪后的路径起点是当前起点（替换最近点为当前起点，避免微小偏差）
        cropped_path[0] = current_start;

        // 重新计算裁剪后路径的朝向（与原逻辑一致）
        for (size_t i = 0; i < cropped_path.size() - 1; ++i) {
            double angle = atan2(
                cropped_path[i+1].pose.position.y - cropped_path[i].pose.position.y,
                cropped_path[i+1].pose.position.x - cropped_path[i].pose.position.x
            );
            cropped_path[i].pose.orientation = tf::createQuaternionMsgFromYaw(angle);
        }
        cropped_path.front().pose.orientation = current_start.pose.orientation;
        // Preserve the exact terminal SE(2) state even after recomputing all
        // tangent orientations in the cropped prefix.
        cropped_path.back() = last_path.back();
        if (cropped_path.size() < 2) {
            cropped_path.clear();
            return false;
        }
        return true;
    }

    void publishPath(const std::vector<geometry_msgs::PoseStamped>& plan) {
        nav_msgs::Path path_msg;
        path_msg.header.frame_id = "map";
        path_msg.header.stamp = ros::Time::now();
        path_msg.poses = plan;
        path_pub_.publish(path_msg);
    }

};
}

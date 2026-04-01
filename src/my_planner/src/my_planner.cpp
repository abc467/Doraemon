#include "my_planner.h"
#include <pluginlib/class_list_macros.h>
#include <tf/transform_listener.h>
#include <costmap_2d/cost_values.h>
#include <costmap_2d/costmap_2d.h>
#include <cmath>
#include <algorithm>

// 注册插件
PLUGINLIB_EXPORT_CLASS(my_planner::MyPlanner, nav_core::BaseLocalPlanner)

namespace my_planner 
{
    MyPlanner::MyPlanner()
        : costmap_ros_(NULL), tf_listener_(NULL), 
          current_vel_x_(0.0), pose_adjusting_(false), goal_reached_(false)
    {
    }

    MyPlanner::~MyPlanner()
    {
        if(tf_listener_) delete tf_listener_;
    }

    void MyPlanner::initialize(std::string name, tf2_ros::Buffer* tf, costmap_2d::Costmap2DROS* costmap_ros)
    {
        if(!costmap_ros_)
        {
            costmap_ros_ = costmap_ros;
            tf_listener_ = new tf::TransformListener();

            ros::NodeHandle private_nh("~/" + name);

            // --- 初始化参数 ---
            private_nh.param("max_vel_x", max_vel_x_, 0.3);       // 最大直线速度
            private_nh.param("acc_lim_x", acc_lim_x_, 0.3);       // 直线加速度
            private_nh.param("max_vel_theta", max_vel_theta_, 0.3); // 最大角速度
            private_nh.param("acc_lim_theta", acc_lim_theta_, 1.0); // 角加速度
            private_nh.param("lookahead_dist", lookahead_dist_, 0.5); // 基础前瞻距离
            private_nh.param("goal_tolerance", goal_tolerance_, 0.02); // 到达容差 (建议 0.05)

            ROS_INFO("MyPlanner Initialized: max_v=%.2f, lookahead=%.2f", max_vel_x_, lookahead_dist_);
            
            current_vel_x_ = 0.0;
        }
    }

    bool MyPlanner::setPlan(const std::vector<geometry_msgs::PoseStamped>& plan)
    {
        if(plan.empty()) return false;

        target_index_ = 0;
        global_plan_ = plan;
        pose_adjusting_ = false;
        goal_reached_ = false;
        
        // 每次接收新路径时重置速度
        current_vel_x_ = 0.0; 
        
        return true;
    }

    bool MyPlanner::computeVelocityCommands(geometry_msgs::Twist& cmd_vel)
    {
        // if(!costmap_ros_) return false;

        // costmap_2d::Costmap2D* costmap = costmap_ros_->getCostmap();

        // // ============================================
        // // 1. 碰撞检测 (优化版：允许从膨胀区逃逸)
        // // ============================================
        // int check_len = std::min((int)global_plan_.size(), target_index_ + 20); 
        
        // // [API 修复] 获取机器人当前位置 (ROS Noetic 兼容写法)
        // geometry_msgs::PoseStamped robot_pose;
        // if (!costmap_ros_->getRobotPose(robot_pose)) {
        //     ROS_WARN_THROTTLE(1.0, "[MyPlanner] Unable to get robot pose");
        //     return false;
        // }
        
        // double robot_x = robot_pose.pose.position.x;
        // double robot_y = robot_pose.pose.position.y;

        // for(int i = target_index_; i < check_len; i++)
        // {
        //     geometry_msgs::PoseStamped pose_costmap;
        //     try{
        //         geometry_msgs::PoseStamped check_pose = global_plan_[i];
        //         check_pose.header.stamp = ros::Time(0); // TF 时间戳修复

        //         tf_listener_->transformPose(costmap_ros_->getGlobalFrameID(), check_pose, pose_costmap);
        //     }
        //     catch(tf::TransformException& ex){
        //         continue; 
        //     }

        //     unsigned int mx, my;
        //     if(costmap->worldToMap(pose_costmap.pose.position.x, pose_costmap.pose.position.y, mx, my))
        //     {
        //         unsigned char cost = costmap->getCost(mx, my);
                
        //         // 计算该路径点距离机器人的距离
        //         double dist_from_robot = std::hypot(pose_costmap.pose.position.x - robot_x, 
        //                                             pose_costmap.pose.position.y - robot_y);

        //         // --- 优化的检测逻辑 ---
                
        //         // 1. 致命障碍 (254) 或 未知区域 (255) -> 必须停
        //         bool is_lethal = (cost == costmap_2d::LETHAL_OBSTACLE || cost == costmap_2d::NO_INFORMATION);
                
        //         // 2. 内切膨胀 (253) 
        //         //    如果离得很近 (< 0.2m)，说明刚起步就在膨胀圈里，允许走 (忽略 253 以防止卡死)
        //         //    如果离得较远 (> 0.2m)，说明前方路变窄了，不允许走 (重视 253)
        //         bool is_inflated = (cost == costmap_2d::INSCRIBED_INFLATED_OBSTACLE);
                
        //         if (is_lethal || (is_inflated && dist_from_robot > 0.2)) 
        //         {
        //             ROS_WARN_THROTTLE(1.0, "[MyPlanner] Obstacle detected (Cost=%d, Dist=%.2f), stopping!", cost, dist_from_robot);
        //             cmd_vel.linear.x = 0.0;
        //             cmd_vel.angular.z = 0.0;
        //             current_vel_x_ = 0.0; 
        //             return false; // 返回 false，触发 move_base/MBF 进行重规划
        //         }
        //     }
        // }

        // ============================================
        // 2. 终点判定与姿态调整 logic
        // ============================================
        int final_index = global_plan_.size() - 1;
        geometry_msgs::PoseStamped pose_final_base;
        
        try {
            geometry_msgs::PoseStamped goal_pose = global_plan_[final_index];
            goal_pose.header.stamp = ros::Time(0);

            // 等待并转换终点坐标到 base_link
            tf_listener_->waitForTransform("base_link", goal_pose.header.frame_id, ros::Time(0), ros::Duration(0.1));
            tf_listener_->transformPose("base_link", goal_pose, pose_final_base);
        } catch (tf::TransformException& ex) {
            ROS_WARN_THROTTLE(1.0, "[MyPlanner] Final pose transform failed: %s", ex.what());
            cmd_vel.linear.x = 0.0;
            cmd_vel.angular.z = 0.0;
            return true; // 暂时停车等待 TF
        }

        double dist_to_goal = std::hypot(pose_final_base.pose.position.x, pose_final_base.pose.position.y);

        // 到达 XY 终点，进入原地旋转调整模式
        if (!pose_adjusting_ && dist_to_goal < goal_tolerance_) 
        {
            pose_adjusting_ = true;
            ROS_INFO("[MyPlanner] Reached XY goal, starting fine rotation...");
            current_vel_x_ = 0.0; // 刹车
        }

        // 执行原地旋转调整
        if (pose_adjusting_)
        {
            double final_yaw = tf::getYaw(pose_final_base.pose.orientation);
            
            cmd_vel.linear.x = 0.0; // 纯旋转，禁止直线运动
            cmd_vel.angular.z = final_yaw * 0.8; // P控制

            // 旋转速度限幅
            if(cmd_vel.angular.z > max_vel_theta_) cmd_vel.angular.z = max_vel_theta_;
            if(cmd_vel.angular.z < -max_vel_theta_) cmd_vel.angular.z = -max_vel_theta_;

            // 最小摩擦力补偿 (防止速度太小电机不动)
            if(std::abs(cmd_vel.angular.z) < 0.1 && std::abs(final_yaw) > 0.03) {
                cmd_vel.angular.z = (final_yaw > 0) ? 0.1 : -0.1;
            }

            // 角度误差判定
            if (std::abs(final_yaw) < 0.03)
            {
                goal_reached_ = true;
                cmd_vel.angular.z = 0.0;
                ROS_INFO("[MyPlanner] Goal Reached!");
            }
            return true;
        }

        // ============================================
        // 3. 胡萝卜路径跟随 (动态前瞻)
        // ============================================
        
        geometry_msgs::PoseStamped target_pose;
        bool found_target = false;

        // 动态前瞻距离：速度越快，看越远 (0.5m ~ 1.0m)
        double dyn_lookahead = std::max(lookahead_dist_, std::abs(current_vel_x_) * 1.5);
        if(dyn_lookahead > 1.0) dyn_lookahead = 1.0;

        for(int i = target_index_; i < global_plan_.size(); i++)
        {
            geometry_msgs::PoseStamped pose_base;
            try {
                geometry_msgs::PoseStamped plan_pose = global_plan_[i];
                plan_pose.header.stamp = ros::Time(0); 

                tf_listener_->transformPose("base_link", plan_pose, pose_base);
            } catch (tf::TransformException& ex) { continue; }

            double dist = std::hypot(pose_base.pose.position.x, pose_base.pose.position.y);

            // 找到第一个大于前瞻距离的点
            if (dist > dyn_lookahead)
            {
                target_pose = pose_base;
                target_index_ = i;
                found_target = true;
                break;
            }
        }

        if(!found_target)
        {
            target_pose = pose_final_base;
            target_index_ = final_index;
        }

        // ============================================
        // 4. 启动时的朝向对齐逻辑 (防止初始急转)
        // ============================================
        double angle_to_target = std::atan2(target_pose.pose.position.y, target_pose.pose.position.x);

        // 条件：角度偏差大 (>30度) 且 离终点还有一段距离
        if (std::abs(angle_to_target) > 0.5 && dist_to_goal > 0.5)
        {
            cmd_vel.linear.x = 0.0; 
            current_vel_x_ = 0.0;   
            
            cmd_vel.angular.z = angle_to_target * 0.8; 
            
            if (cmd_vel.angular.z > max_vel_theta_) cmd_vel.angular.z = max_vel_theta_;
            if (cmd_vel.angular.z < -max_vel_theta_) cmd_vel.angular.z = -max_vel_theta_;
            
            return true; // 原地旋转中
        }

        // ============================================
        // 5. 速度计算 (软着陆 + Ramp Control)
        // ============================================
        
        double target_v_x = 0.0;
        
        // 减速策略
        if (dist_to_goal < 1.0)
        {
            target_v_x = max_vel_x_ * (dist_to_goal / 1.0); 

            if (dist_to_goal > 0.1) {
                if(target_v_x < 0.1) target_v_x = 0.1; // 维持最小巡航速度
            } else {
                if(target_v_x < 0.05) target_v_x = 0.05; // 允许极低速进站
            }
        }
        else
        {
            target_v_x = max_vel_x_;
        }

        // 加速度平滑
        double dt = 0.1; // 控制周期 10Hz
        double increment = acc_lim_x_ * dt;

        if (current_vel_x_ < target_v_x)
        {
            current_vel_x_ += increment;
            if (current_vel_x_ > target_v_x) current_vel_x_ = target_v_x;
        }
        else if (current_vel_x_ > target_v_x)
        {
            current_vel_x_ -= increment;
            if (current_vel_x_ < target_v_x) current_vel_x_ = target_v_x;
        }

        cmd_vel.linear.x = current_vel_x_;

        // ============================================
        // 6. 角速度计算 (纯追踪 Pure Pursuit)
        // ============================================
        
        double lookahead_sq = std::pow(target_pose.pose.position.x, 2) + std::pow(target_pose.pose.position.y, 2);
        
        if (lookahead_sq > 0.001) {
             // kappa = 2 * y / L^2
             double curvature = 2.0 * target_pose.pose.position.y / lookahead_sq;
             cmd_vel.angular.z = current_vel_x_ * curvature;
        } else {
            cmd_vel.angular.z = 0.0;
        }

        // 角速度限幅
        if (cmd_vel.angular.z > max_vel_theta_) cmd_vel.angular.z = max_vel_theta_;
        if (cmd_vel.angular.z < -max_vel_theta_) cmd_vel.angular.z = -max_vel_theta_;

        return true;
    }

    bool MyPlanner::isGoalReached()
    {
        return goal_reached_;
    }

} // namespace my_planner
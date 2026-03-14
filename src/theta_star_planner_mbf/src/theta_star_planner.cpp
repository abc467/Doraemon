#include "theta_star_planner/theta_star_planner.h"
#include <pluginlib/class_list_macros.h>

PLUGINLIB_EXPORT_CLASS(mbf_global_planner::ThetaStarPlanner, mbf_costmap_core::CostmapPlanner)

namespace mbf_global_planner {

ThetaStarPlanner::ThetaStarPlanner() {}

ThetaStarPlanner::ThetaStarPlanner(std::string name, costmap_2d::Costmap2DROS* costmap_ros)
{
    initialize(name, costmap_ros);
}

void ThetaStarPlanner::initialize(std::string name, costmap_2d::Costmap2DROS* costmap_ros)
{
    if(!initialized_){
        costmap_ros_ = costmap_ros;
        costmap_ = costmap_ros_->getCostmap();
        planner_ = std::make_unique<theta_star::ThetaStar>();
        planner_->costmap_ = costmap_;
        planner_->setCancelChecker([this]() { return cancel_requested_.load(); });

        ros::NodeHandle private_nh("~/" + name);
        private_nh.param("allow_unknown", planner_->allow_unknown_, true);
        private_nh.param("terminal_checking_interval", planner_->terminal_checking_interval_, 4);
        private_nh.param("w_traversal_cost", planner_->w_traversal_cost_, 1.0); // costmap中单元格遍历成本的权重
        private_nh.param("w_euc_cost", planner_->w_euc_cost_, 2.0); // 欧几里得距离成本的权重（用于计算 g_cost）
        private_nh.param("w_heuristic_cost", planner_->w_heuristic_cost_, 1.0); // 启发式成本的权重（用于 h_cost 的计算）

        // 新增：加载路径复用相关参数
        private_nh.param("goal_tolerance", goal_tolerance_, 0.05);  // 终点位置误差容忍度
        private_nh.param("path_check_interval", path_check_interval_, 0.1);  // 路径碰撞检查间隔
        private_nh.param("path_max_age", path_max_age_, 20.0);  // 路径最大有效期（秒，可选）

        path_pub_ = private_nh.advertise<nav_msgs::Path>("theta_star_plan", 1);

        initialized_ = true;
        ROS_INFO("mbf theta* planner is initialized... ");
    }else{
        ROS_WARN("This planner has already been initialized, doing nothing.");
    }
}

uint32_t ThetaStarPlanner::makePlan(const geometry_msgs::PoseStamped& start, const geometry_msgs::PoseStamped& goal,
                        double tolerance, std::vector<geometry_msgs::PoseStamped>& plan, double &cost,
                        std::string &message){
    // 启动规划前，重置标志
    cancel_requested_ = false;
    is_planning_ = true;  // 标记为正在规划
    plan.clear();
    cost = 0.0;
    message.clear();
        
    if(!initialized_){
        ROS_ERROR("This planner has not been initialized, please call initialize() before using this planner");
        is_planning_ = false;
        message = "planner is not initialized";
        return mbf_msgs::GetPathResult::NOT_INITIALIZED;
    }

    // 新增：检查是否满足路径复用条件
    if (canReusePath(start, goal)) {
        // 裁剪历史路径并复用
        if (cropPathToStart(start, last_valid_path_, plan)) {
            ROS_DEBUG("Reusing existing path (goal unchanged and collision-free)");
            publishPath(plan);
            is_planning_ = false;
            message = "reused cached path";
            for (size_t i = 1; i < plan.size(); ++i) {
                cost += path_tools::euclidean_distance(plan[i - 1], plan[i]);
            }
            return mbf_msgs::GetPathResult::SUCCESS;
        }
    }

    auto start_time = std::chrono::steady_clock::now();

    unsigned int mx_start, my_start, mx_goal, my_goal;
    if(!planner_->costmap_->worldToMap(start.pose.position.x, start.pose.position.y, mx_start, my_start)){
        is_planning_ = false;
        message = "start position is out of map";
        return mbf_msgs::GetPathResult::INVALID_START;
    }
    if(!planner_->costmap_->worldToMap(goal.pose.position.x, goal.pose.position.y, mx_goal, my_goal)){
        is_planning_ = false;
        message = "goal position is out of map";
        return mbf_msgs::GetPathResult::OUT_OF_MAP;
    }
    if(planner_->costmap_->getCost(mx_goal, my_goal) == costmap_2d::LETHAL_OBSTACLE){
        is_planning_ = false;
        message = "goal position is blocked";
        return mbf_msgs::GetPathResult::INVALID_GOAL;
    }

    // 起点和终点重合
    if(mx_start == mx_goal && my_start == my_goal){
        plan.clear();
        plan.push_back(start);
        plan.push_back(goal);
        is_planning_ = false;
        cost = path_tools::euclidean_distance(start, goal);
        message = "start and goal are in the same cell";
        return mbf_msgs::GetPathResult::SUCCESS;
    }

    planner_->clearStart();
    planner_->setStartAndGoal(start, goal);
    std::vector<coordsW> raw_path;
    if(cancel_requested_){
        is_planning_ = false;
        return mbf_msgs::GetPathResult::CANCELED;
    }
    if(planner_->isUnsafeToPlan()){
        is_planning_ = false;
        message = "start or goal position is unsafe";
        return mbf_msgs::GetPathResult::FAILURE;
    }else if(planner_->generatePath(raw_path)){
        if(cancel_requested_){
            is_planning_ = false;
            message = "planning canceled";
            return mbf_msgs::GetPathResult::CANCELED;
        }
        std::cout << "theta* raw_path.size(): " << raw_path.size() << std::endl;
        plan = linearInterpolation(raw_path, planner_->costmap_->getResolution());
        plan.back().pose.position = goal.pose.position;
        std::cout << "theta* interpolate plan.size(): " << plan.size() << std::endl;
        // 降采样路径
        auto downsampled_path = downsamplePath(plan, 0.4);
        std::cout << "theta* downsampled path.size(): " << downsampled_path.size() << std::endl;
        if(downsampled_path.size() > 5){
            // 平滑路径
            auto s_path = smoothPath(downsampled_path);
            if(s_path.has_value()){
                plan = s_path.value();
                std::cout << "theta* smoothed plan.size(): " << plan.size() << std::endl;
            }
        }

        if(1){
            publishPath(plan);
        }

        // 计算路径角度
        // for(unsigned int i = 0; i < plan.size() - 1; ++i){
        //     double angle = std::atan2(plan[i + 1].pose.position.y - plan[i].pose.position.y,
        //                                 plan[i + 1].pose.position.x - plan[i].pose.position.x);
        //     plan[i].pose.orientation = tf::createQuaternionMsgFromYaw(angle);
        // }
        // plan.back().pose.orientation = goal.pose.orientation;
// --- 推荐的修改 ---
        // 1. 确保路径至少有两个点
        if (plan.size() >= 2) {
            // 2. 像以前一样，计算从[0]到[n-2]（倒数第二个点）的所有朝向
            for (unsigned int i = 0; i < plan.size() - 1; ++i) {
                double angle = std::atan2(plan[i + 1].pose.position.y - plan[i].pose.position.y,
                                            plan[i + 1].pose.position.x - plan[i].pose.position.x);
                plan[i].pose.orientation = tf::createQuaternionMsgFromYaw(angle);
            }

            // 3. 创建一个新点，作为“原地旋转”的目标
            geometry_msgs::PoseStamped final_goal_pose = plan.back();
            // 4. 将这个新点的朝向设置为最终的朝向
            final_goal_pose.pose.orientation = goal.pose.orientation;

            // 5. 将“原始”最后一个点(plan.back())的朝向设置为与倒数第二个点一致，以确保平滑到达
            plan.back().pose.orientation = plan[plan.size() - 2].pose.orientation;
            
            // 6. 将“原地旋转”的目标点添加到路径的末尾
            plan.push_back(final_goal_pose);

        } else if (!plan.empty()) {
            // 路径只有一个点，直接设置其朝向
            plan.back().pose.orientation = goal.pose.orientation;
        }
        // --- 修改结束 ---
        // printPoseStampedVector(plan);

        auto stop_time = std::chrono::steady_clock::now();
        auto dur = std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time);
        std::cout << "ThetaStarPlanner::makePlan() time: " << dur.count() << " ms" << std::endl;

        for (size_t i = 1; i < plan.size(); ++i) {
            cost += path_tools::euclidean_distance(plan[i - 1], plan[i]);
        }

        // 规划成功后，更新历史路径信息（新增）
        if (plan.size() > 0) {
            last_goal_ = goal;
            last_valid_path_ = plan;
            last_path_timestamp_ = ros::Time::now();
        }
        is_planning_ = false;
        message = "success";
        return mbf_msgs::GetPathResult::SUCCESS;
    }else{
        is_planning_ = false;
        message = cancel_requested_ ? "planning canceled" : "failed to find a path";
        if (cancel_requested_) {
            return mbf_msgs::GetPathResult::CANCELED;
        }
        return mbf_msgs::GetPathResult::NO_PATH_FOUND;
    }
}

bool ThetaStarPlanner::cancel(){
    if (is_planning_.load()) {  // 若正在规划
        cancel_requested_.store(true);  // 设置取消标志
        return true;  // 成功请求取消
    }
    return true;  // 未在规划，无需取消
}

// 线性插值，同时将路径转化为ros格式
std::vector<geometry_msgs::PoseStamped> ThetaStarPlanner::linearInterpolation(
    const std::vector<coordsW> & raw_path, const double & dist_bw_points)
{
    std::vector<geometry_msgs::PoseStamped> pa;
    geometry_msgs::PoseStamped p1;
    p1.header.frame_id = "map";
    for(unsigned int j = 0; j < raw_path.size() - 1; j++){
        coordsW pt1 = raw_path[j];
        p1.pose.position.x = pt1.x;
        p1.pose.position.y = pt1.y;
        pa.push_back(p1);

        coordsW pt2 = raw_path[j + 1];
        double distance = std::hypot(pt2.x - pt1.x, pt2.y - pt1.y);
        int loops = static_cast<int>(distance / dist_bw_points);
        double sin_alpha = (pt2.y - pt1.y) / distance;
        double cos_alpha = (pt2.x - pt1.x) / distance;
        for(int i = 1; i < loops; i++){
            p1.pose.position.x = pt1.x + i * dist_bw_points * cos_alpha;
            p1.pose.position.y = pt1.y + i * dist_bw_points * sin_alpha;
            pa.push_back(p1);
        }
    }
    return pa;
}

// 对路径进行降采样, 0.4m 间隔
std::vector<geometry_msgs::PoseStamped> ThetaStarPlanner::downsamplePath(const std::vector<geometry_msgs::PoseStamped>& orig_global_plan, double sampling_distance) {

    std::vector<geometry_msgs::PoseStamped> downsampled_path;
    downsampled_path.push_back(orig_global_plan[0]);

    double accumulated_distance = 0.0;
    for (size_t i = 1; i < orig_global_plan.size(); ++i) {
        double distance = g_tools::distanceBetweenPoses(orig_global_plan[i - 1].pose, orig_global_plan[i].pose);
        accumulated_distance += distance;
        if (accumulated_distance >= sampling_distance) {
            downsampled_path.push_back(orig_global_plan[i]);
            accumulated_distance -= sampling_distance;
        }
    }
    // 添加最后一个点（确保首尾点都保留）
    downsampled_path.push_back(orig_global_plan.back());

    return downsampled_path;
}

std::optional<std::vector<geometry_msgs::PoseStamped>>  ThetaStarPlanner::smoothPath(const std::vector<geometry_msgs::PoseStamped>& orig_global_plan){
    if (orig_global_plan.size() <= 3) {
        ROS_WARN("ThetaStarPlanner: smoothed path size <= 3");
        return std::nullopt;
    }

    std::vector<double> filtered_x;
    std::vector<double> filtered_y;
    filtered_x.reserve(orig_global_plan.size());
    filtered_y.reserve(orig_global_plan.size());
    for (const auto& pose : orig_global_plan) {
        filtered_x.push_back(pose.pose.position.x);
        filtered_y.push_back(pose.pose.position.y);
    }

    constexpr int kWindowSize = 5;
    if (filtered_x.size() > static_cast<size_t>(kWindowSize)) {
        filtered_x = path_tools::movingAverageFilter<double>(filtered_x, kWindowSize);
        filtered_y = path_tools::movingAverageFilter<double>(filtered_y, kWindowSize);
    }

    std::vector<geometry_msgs::PoseStamped> smoothed_anchor_path = orig_global_plan;
    for (size_t i = 1; i + 1 < smoothed_anchor_path.size(); ++i) {
        smoothed_anchor_path[i].pose.position.x = filtered_x[i];
        smoothed_anchor_path[i].pose.position.y = filtered_y[i];
    }

    auto smoothed_path = path_tools::linearInterpolate(smoothed_anchor_path, 0.05, 0.0);
    if (smoothed_path.size() < 2) {
        return std::nullopt;
    }

    for (size_t i = 0; i + 1 < smoothed_path.size(); ++i) {
        double dx = smoothed_path[i + 1].pose.position.x - smoothed_path[i].pose.position.x;
        double dy = smoothed_path[i + 1].pose.position.y - smoothed_path[i].pose.position.y;
        double angle = std::atan2(dy, dx);
        tf2::Quaternion quaternion;
        quaternion.setRPY(0., 0., angle);
        smoothed_path[i].pose.orientation = tf2::toMsg(quaternion);
    }
    smoothed_path.back().pose.orientation = orig_global_plan.back().pose.orientation;
    return smoothed_path;
}


// 打印 std::vector<geometry_msgs::PoseStamped> 数据的函数
void ThetaStarPlanner::printPoseStampedVector(const std::vector<geometry_msgs::PoseStamped>& poses) {
    for (const auto& pose : poses) {
        // 打印时间戳
        std::cout << "Stamp: sec=" << pose.header.stamp.sec << ", nsec=" << pose.header.stamp.nsec << std::endl;
        // 打印坐标系
        std::cout << "Frame: " << pose.header.frame_id << std::endl;
        // 打印位置信息
        std::cout << "Position: x=" << pose.pose.position.x
                  << ", y=" << pose.pose.position.y
                  << ", z=" << pose.pose.position.z << std::endl;
        // 打印姿态信息（四元数）
        std::cout << "Orientation: x=" << pose.pose.orientation.x
                  << ", y=" << pose.pose.orientation.y
                  << ", z=" << pose.pose.orientation.z
                  << ", w=" << pose.pose.orientation.w << std::endl;
        std::cout << "------------------------" << std::endl;
    }
}

// 删除路径中过近的pose
// TODO: 函数逻辑有误,带修正
void ThetaStarPlanner::removeClosePoses(std::vector<geometry_msgs::PoseStamped>& path, double min_distance) {
    if (path.empty()) {
        return;
    }

    auto distanceBetweenPoses = [](const geometry_msgs::Pose& pose1, const geometry_msgs::Pose& pose2) {
        double dx = pose1.position.x - pose2.position.x;
        double dy = pose1.position.y - pose2.position.y;
        return std::sqrt(dx * dx + dy * dy);
    };

    std::vector<geometry_msgs::PoseStamped> new_poses;
    new_poses.push_back(path[0]);

    for (size_t i = 1; i < path.size(); ++i) {
        double distance = distanceBetweenPoses(path[i - 1].pose, path[i].pose);
        if (distance > min_distance) {
            new_poses.push_back(path[i]);
        }
    }
    path = new_poses;
}

}

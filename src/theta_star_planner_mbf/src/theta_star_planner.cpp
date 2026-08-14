#include "theta_star_planner/theta_star_planner.h"
#include <pluginlib/class_list_macros.h>

#include <array>
#include <limits>

PLUGINLIB_EXPORT_CLASS(mbf_global_planner::ThetaStarPlanner, mbf_costmap_core::CostmapPlanner)

namespace mbf_global_planner {

namespace
{

double positiveModuloTwoPi(double angle)
{
    const double wrapped = std::fmod(angle, 2.0 * M_PI);
    return wrapped < 0.0 ? wrapped + 2.0 * M_PI : wrapped;
}

struct DubinsCandidate
{
    std::array<char, 3> segment_types{{'S', 'S', 'S'}};
    std::array<double, 3> normalized_lengths{{0.0, 0.0, 0.0}};

    double totalLength() const
    {
        return normalized_lengths[0] + normalized_lengths[1] +
            normalized_lengths[2];
    }
};

std::vector<DubinsCandidate> dubinsCandidates(
    double start_x, double start_y, double start_yaw,
    double goal_x, double goal_y, double goal_yaw,
    double radius)
{
    std::vector<DubinsCandidate> result;
    if (!std::isfinite(radius) || radius <= 0.0) {
        return result;
    }
    const double dx = goal_x - start_x;
    const double dy = goal_y - start_y;
    const double distance = std::hypot(dx, dy) / radius;
    const double theta = std::atan2(dy, dx);
    const double alpha = positiveModuloTwoPi(start_yaw - theta);
    const double beta = positiveModuloTwoPi(goal_yaw - theta);
    const double sin_alpha = std::sin(alpha);
    const double sin_beta = std::sin(beta);
    const double cos_alpha = std::cos(alpha);
    const double cos_beta = std::cos(beta);
    const double cos_delta = std::cos(alpha - beta);

    auto add = [&](char first, char second, char third,
                   double t, double p, double q) {
        if (std::isfinite(t) && std::isfinite(p) && std::isfinite(q) &&
            t >= 0.0 && p >= 0.0 && q >= 0.0) {
            result.push_back({
                {{first, second, third}}, {{t, p, q}}});
        }
    };

    // LSL
    {
        const double p_squared = 2.0 + distance * distance -
            2.0 * cos_delta + 2.0 * distance * (sin_alpha - sin_beta);
        if (p_squared >= 0.0) {
            const double tmp = std::atan2(
                cos_beta - cos_alpha,
                distance + sin_alpha - sin_beta);
            add('L', 'S', 'L',
                positiveModuloTwoPi(-alpha + tmp),
                std::sqrt(p_squared),
                positiveModuloTwoPi(beta - tmp));
        }
    }
    // RSR
    {
        const double p_squared = 2.0 + distance * distance -
            2.0 * cos_delta + 2.0 * distance * (-sin_alpha + sin_beta);
        if (p_squared >= 0.0) {
            const double tmp = std::atan2(
                cos_alpha - cos_beta,
                distance - sin_alpha + sin_beta);
            add('R', 'S', 'R',
                positiveModuloTwoPi(alpha - tmp),
                std::sqrt(p_squared),
                positiveModuloTwoPi(-beta + tmp));
        }
    }
    // LSR
    {
        const double p_squared = -2.0 + distance * distance +
            2.0 * cos_delta + 2.0 * distance * (sin_alpha + sin_beta);
        if (p_squared >= 0.0) {
            const double p = std::sqrt(p_squared);
            const double tmp = std::atan2(
                -cos_alpha - cos_beta,
                distance + sin_alpha + sin_beta) - std::atan2(-2.0, p);
            add('L', 'S', 'R',
                positiveModuloTwoPi(-alpha + tmp), p,
                positiveModuloTwoPi(-beta + tmp));
        }
    }
    // RSL
    {
        const double p_squared = distance * distance - 2.0 +
            2.0 * cos_delta - 2.0 * distance * (sin_alpha + sin_beta);
        if (p_squared >= 0.0) {
            const double p = std::sqrt(p_squared);
            const double tmp = std::atan2(
                cos_alpha + cos_beta,
                distance - sin_alpha - sin_beta) - std::atan2(2.0, p);
            add('R', 'S', 'L',
                positiveModuloTwoPi(alpha - tmp), p,
                positiveModuloTwoPi(beta - tmp));
        }
    }
    // RLR
    {
        const double value = (6.0 - distance * distance +
            2.0 * cos_delta +
            2.0 * distance * (sin_alpha - sin_beta)) / 8.0;
        if (std::fabs(value) <= 1.0) {
            const double p = positiveModuloTwoPi(
                2.0 * M_PI - std::acos(std::clamp(value, -1.0, 1.0)));
            const double t = positiveModuloTwoPi(
                alpha - std::atan2(
                    cos_alpha - cos_beta,
                    distance - sin_alpha + sin_beta) + 0.5 * p);
            add('R', 'L', 'R', t, p,
                positiveModuloTwoPi(alpha - beta - t + p));
        }
    }
    // LRL
    {
        const double value = (6.0 - distance * distance +
            2.0 * cos_delta +
            2.0 * distance * (-sin_alpha + sin_beta)) / 8.0;
        if (std::fabs(value) <= 1.0) {
            const double p = positiveModuloTwoPi(
                2.0 * M_PI - std::acos(std::clamp(value, -1.0, 1.0)));
            const double t = positiveModuloTwoPi(
                -alpha - std::atan2(
                    cos_alpha - cos_beta,
                    distance + sin_alpha - sin_beta) + 0.5 * p);
            add('L', 'R', 'L', t, p,
                positiveModuloTwoPi(beta - alpha - t + p));
        }
    }

    std::sort(result.begin(), result.end(), [](const auto& left, const auto& right) {
        return left.totalLength() < right.totalLength();
    });
    return result;
}

std::optional<std::vector<geometry_msgs::PoseStamped>> sampleDubinsCandidate(
    const geometry_msgs::PoseStamped& start,
    const geometry_msgs::PoseStamped& goal,
    const DubinsCandidate& candidate,
    double radius,
    double sample_step)
{
    std::vector<geometry_msgs::PoseStamped> path;
    path.push_back(start);
    double x = start.pose.position.x;
    double y = start.pose.position.y;
    double yaw = tf2::getYaw(start.pose.orientation);

    for (size_t segment_index = 0; segment_index < 3; ++segment_index) {
        const char type = candidate.segment_types[segment_index];
        double remaining =
            candidate.normalized_lengths[segment_index] * radius;
        while (remaining > 1e-9) {
            const double distance = std::min(sample_step, remaining);
            if (type == 'S') {
                x += distance * std::cos(yaw);
                y += distance * std::sin(yaw);
            } else {
                const double sign = type == 'L' ? 1.0 : -1.0;
                const double next_yaw = yaw + sign * distance / radius;
                x += radius / sign * (std::sin(next_yaw) - std::sin(yaw));
                y += radius / sign * (-std::cos(next_yaw) + std::cos(yaw));
                yaw = next_yaw;
            }
            remaining -= distance;
            geometry_msgs::PoseStamped pose = goal;
            pose.pose.position.x = x;
            pose.pose.position.y = y;
            pose.pose.orientation = tf::createQuaternionMsgFromYaw(yaw);
            path.push_back(pose);
        }
    }

    const double position_error = std::hypot(
        x - goal.pose.position.x, y - goal.pose.position.y);
    const double yaw_error = std::fabs(angles::shortest_angular_distance(
        yaw, tf2::getYaw(goal.pose.orientation)));
    if (position_error > 1e-5 || yaw_error > 1e-5) {
        return std::nullopt;
    }
    path.back() = goal;
    return path;
}

}  // namespace

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
        private_nh.param("max_allowed_cost", planner_->max_allowed_cost_, LETHAL_COST - 1);
        planner_->max_allowed_cost_ = std::clamp(planner_->max_allowed_cost_, 0, OBS_COST - 1);

        // 新增：加载路径复用相关参数
        private_nh.param("goal_tolerance", goal_tolerance_, 0.05);  // 终点位置误差容忍度
        private_nh.param("path_check_interval", path_check_interval_, 0.1);  // 路径碰撞检查间隔
        private_nh.param("path_max_age", path_max_age_, 20.0);  // 路径最大有效期（秒，可选）
        private_nh.param("use_footprint_path_check", use_footprint_path_check_, true);
        private_nh.param(
            "goal_orientation_tolerance", goal_orientation_tolerance_, 0.05);
        private_nh.param(
            "terminal_approach_enabled", terminal_approach_enabled_, true);
        private_nh.param(
            "terminal_straight_length", terminal_straight_length_, 0.40);
        private_nh.param(
            "terminal_min_straight_length",
            terminal_min_straight_length_, 0.0);
        private_nh.param(
            "terminal_straight_length_step",
            terminal_straight_length_step_, 0.10);
        private_nh.param(
            "terminal_min_turn_radius", terminal_min_turn_radius_, 0.0);
        private_nh.param(
            "terminal_sample_step", terminal_sample_step_, 0.05);
        private_nh.param(
            "terminal_max_prefix_splice_distance",
            terminal_max_prefix_splice_distance_, 1.80);
        private_nh.param("se2_refinement_enabled", se2_refiner_config_.enabled, true);
        private_nh.param("se2_allow_reverse", se2_refiner_config_.allow_reverse, true);
        private_nh.param("se2_allow_unknown", se2_refiner_config_.allow_unknown, false);
        private_nh.param("se2_yaw_bins", se2_refiner_config_.yaw_bins, 32);
        private_nh.param("se2_max_expansions", se2_refiner_config_.max_expansions, 250000);
        private_nh.param("se2_max_repairs", se2_refiner_config_.max_repairs, 8);
        private_nh.param("se2_motion_step", se2_refiner_config_.motion_step, 0.10);
        private_nh.param(
            "se2_collision_check_step",
            se2_refiner_config_.collision_check_step, 0.025);
        private_nh.param(
            "se2_goal_position_tolerance",
            se2_refiner_config_.goal_position_tolerance, 0.12);
        private_nh.param(
            "se2_goal_yaw_tolerance",
            se2_refiner_config_.goal_yaw_tolerance, 0.20);
        private_nh.getParam(
            "se2_repair_window_lengths",
            se2_refiner_config_.repair_window_lengths);
        private_nh.getParam(
            "se2_corridor_widths",
            se2_refiner_config_.corridor_widths);

        goal_orientation_tolerance_ = std::clamp(
            std::fabs(goal_orientation_tolerance_), 0.0, M_PI);
        terminal_sample_step_ = std::max(
            0.01, std::fabs(terminal_sample_step_));
        terminal_straight_length_ = std::max(
            0.0, std::fabs(terminal_straight_length_));
        terminal_min_straight_length_ = std::clamp(
            std::fabs(terminal_min_straight_length_),
            0.0, terminal_straight_length_);
        terminal_straight_length_step_ = std::max(
            terminal_sample_step_,
            std::fabs(terminal_straight_length_step_));
        terminal_min_turn_radius_ = std::max(
            0.0, std::fabs(terminal_min_turn_radius_));
        terminal_max_prefix_splice_distance_ = std::max(
            terminal_straight_length_ + 2.0 * terminal_sample_step_,
            std::fabs(terminal_max_prefix_splice_distance_));
        se2_refiner_config_.yaw_bins = std::max(8, se2_refiner_config_.yaw_bins);
        se2_refiner_config_.max_expansions = std::max(
            1000, se2_refiner_config_.max_expansions);
        se2_refiner_config_.max_repairs = std::max(
            1, se2_refiner_config_.max_repairs);
        se2_refiner_config_.motion_step = std::max(
            1.5 * costmap_->getResolution(),
            std::fabs(se2_refiner_config_.motion_step));
        se2_refiner_config_.collision_check_step = std::max(
            0.005, std::min(
                std::fabs(se2_refiner_config_.collision_check_step),
                0.5 * costmap_->getResolution()));
        se2_refiner_config_.max_allowed_center_cost = planner_->max_allowed_cost_;

        path_pub_ = private_nh.advertise<nav_msgs::Path>("theta_star_plan", 1);

        initialized_ = true;
        ROS_INFO(
            "mbf theta* planner is initialized: max_allowed_cost=%d "
            "w_traversal_cost=%.2f footprint_check=%s terminal_approach=%s "
            "straight=%.2f..%.2f splice=%.2f min_radius=%.2f se2=%s",
            planner_->max_allowed_cost_,
            planner_->w_traversal_cost_,
            use_footprint_path_check_ ? "true" : "false",
            terminal_approach_enabled_ ? "true" : "false",
            terminal_straight_length_,
            terminal_min_straight_length_,
            terminal_max_prefix_splice_distance_,
            terminal_min_turn_radius_,
            se2_refiner_config_.enabled ? "true" : "false");
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
            if (!isPathCollisionFree(plan)) {
                ROS_DEBUG("Cropped cached path is not collision-free, replanning");
                plan.clear();
            } else {
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
    }

    (void)tolerance;
    const auto start_time = std::chrono::steady_clock::now();

    unsigned int mx_start = 0;
    unsigned int my_start = 0;
    unsigned int mx_goal = 0;
    unsigned int my_goal = 0;
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
    if(!planner_->isSafe(static_cast<int>(mx_goal), static_cast<int>(my_goal))){
        is_planning_ = false;
        message = "goal position exceeds the configured clearance cost";
        return mbf_msgs::GetPathResult::INVALID_GOAL;
    }

    std::string position_failure;
    bool terminal_geometry_applied = false;
    bool terminal_geometry_fallback = false;
    bool se2_geometry_repaired = false;
    std::vector<geometry_msgs::PoseStamped> base_plan;
    if (generatePositionPath(start, goal, base_plan, position_failure)) {
        const double goal_yaw = tf2::getYaw(goal.pose.orientation);
        assignPathOrientations(base_plan, goal_yaw);
        // The first SE(2) state is the robot's measured pose, not the tangent
        // of the newly planned XY polyline. Replacing it with that tangent can
        // make a physically safe current pose appear to be an already-colliding
        // rotated footprint and prevents the local repair search from starting.
        base_plan.front().pose.orientation = start.pose.orientation;
        plan = base_plan;

        if (terminal_approach_enabled_) {
            for (const double straight_length : terminalStraightLengths()) {
                const auto terminal_path = buildTerminalApproachGeometry(
                    base_plan, goal,
                    straight_length,
                    terminal_min_turn_radius_,
                    terminal_sample_step_,
                    terminal_max_prefix_splice_distance_,
                    [this](const std::vector<geometry_msgs::PoseStamped>& candidate) {
                        // Terminal shaping is allowed to propose an XY-safe
                        // suffix.  The SE(2) layer below performs the final
                        // continuous rectangular-footprint validation and,
                        // when needed, repairs that suffix locally.
                        return isCentrelineCollisionFree(candidate);
                    });
                if (!terminal_path.has_value()) {
                    continue;
                }

                plan = terminal_path.value();
                terminal_geometry_applied = true;
                ROS_INFO(
                    "ThetaStarPlanner: reshaped terminal suffix "
                    "straight=%.2f m splice_window=%.2f m points=%zu "
                    "min_radius=%.2f m",
                    straight_length,
                    terminal_max_prefix_splice_distance_,
                    plan.size(), terminal_min_turn_radius_);
                break;
            }
            if (!terminal_geometry_applied) {
                terminal_geometry_fallback = true;
                ROS_WARN(
                    "ThetaStarPlanner: no safe terminal reshaping candidate; "
                    "keeping reachable XY path for Connect MPPI");
            }
        }
    }

    if (!plan.empty()) {
        plan.front().pose.orientation = start.pose.orientation;
    }

    if (!plan.empty() && use_footprint_path_check_) {
        const auto footprint = costmap_ros_ ?
            costmap_ros_->getRobotFootprint() :
            std::vector<geometry_msgs::Point>{};
        if (footprint.size() < 3) {
            plan.clear();
            position_failure =
                "robot footprint is unavailable for SE2 safety validation";
        } else if (se2_refiner_config_.enabled) {
            const auto refinement = theta_star::SE2PathRefiner::refine(
                *costmap_, footprint, plan, se2_refiner_config_,
                &cancel_requested_);
            if (!refinement.success) {
                ROS_WARN(
                    "ThetaStarPlanner: topology path reached the goal, but "
                    "SE2 footprint repair failed after %d expansions: %s",
                    refinement.expansions, refinement.message.c_str());
                plan.clear();
                position_failure =
                    "topology connected but footprint-safe SE2 repair failed: " +
                    refinement.message;
            } else {
                plan = refinement.path;
                se2_geometry_repaired = refinement.repairs > 0;
                ROS_INFO(
                    "ThetaStarPlanner: SE2 validation passed, repairs=%d "
                    "expansions=%d points=%zu",
                    refinement.repairs, refinement.expansions, plan.size());
            }
        } else if (!isPathCollisionFree(plan)) {
            plan.clear();
            position_failure =
                "topology connected but rectangular footprint is unsafe";
        }
    }

    if (cancel_requested_) {
        is_planning_ = false;
        message = "planning canceled";
        return mbf_msgs::GetPathResult::CANCELED;
    }
    if (plan.empty()) {
        is_planning_ = false;
        message = position_failure;
        return mbf_msgs::GetPathResult::NO_PATH_FOUND;
    }

    publishPath(plan);
    const auto stop_time = std::chrono::steady_clock::now();
    const auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        stop_time - start_time);
    ROS_INFO(
        "ThetaStarPlanner::makePlan() time: %ld ms",
        static_cast<long>(duration.count()));

    for (size_t i = 1; i < plan.size(); ++i) {
        cost += path_tools::euclidean_distance(plan[i - 1], plan[i]);
    }

    last_goal_ = goal;
    last_valid_path_ = plan;
    last_path_timestamp_ = ros::Time::now();
    is_planning_ = false;
    message = se2_geometry_repaired ?
        "success with footprint-safe SE2 repair" :
        (terminal_geometry_applied ?
        "success with differential-drive terminal geometry" :
        (terminal_geometry_fallback ?
            "success with reachable XY terminal fallback" : "success"));
    return mbf_msgs::GetPathResult::SUCCESS;
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
    if (raw_path.empty() || dist_bw_points <= 0.0) {
        return pa;
    }

    auto make_pose = [](const coordsW & point) {
        geometry_msgs::PoseStamped pose;
        pose.header.frame_id = "map";
        pose.pose.position.x = point.x;
        pose.pose.position.y = point.y;
        return pose;
    };

    pa.push_back(make_pose(raw_path.front()));
    for (size_t j = 1; j < raw_path.size(); ++j) {
        const coordsW & pt1 = raw_path[j - 1];
        const coordsW & pt2 = raw_path[j];
        const double distance = std::hypot(pt2.x - pt1.x, pt2.y - pt1.y);
        if (distance <= 1e-12) {
            continue;
        }

        const int segments = std::max(1, static_cast<int>(std::ceil(distance / dist_bw_points)));
        for (int i = 1; i <= segments; ++i) {
            const double ratio = static_cast<double>(i) / segments;
            pa.push_back(make_pose({
                pt1.x + ratio * (pt2.x - pt1.x),
                pt1.y + ratio * (pt2.y - pt1.y)}));
        }
    }
    return pa;
}

bool ThetaStarPlanner::isCentrelineCollisionFree(
    const std::vector<geometry_msgs::PoseStamped>& path)
{
    if (path.empty() || costmap_ == nullptr || planner_ == nullptr) {
        return false;
    }

    unsigned int first_x = 0;
    unsigned int first_y = 0;
    if (!costmap_->worldToMap(
            path.front().pose.position.x, path.front().pose.position.y,
            first_x, first_y)) {
        return false;
    }

    unsigned char escape_ceiling = costmap_->getCost(first_x, first_y);
    bool escaping = escape_ceiling != costmap_2d::NO_INFORMATION &&
        static_cast<int>(escape_ceiling) > planner_->max_allowed_cost_ &&
        escape_ceiling < costmap_2d::INSCRIBED_INFLATED_OBSTACLE;

    auto visit = [&](int x, int y, bool update_escape_ceiling) {
        if (x < 0 || y < 0 ||
            x >= static_cast<int>(costmap_->getSizeInCellsX()) ||
            y >= static_cast<int>(costmap_->getSizeInCellsY())) {
            return false;
        }
        const unsigned char raw_cost = costmap_->getCost(x, y);
        if (raw_cost == costmap_2d::NO_INFORMATION) {
            return planner_->allow_unknown_ && !escaping;
        }
        if (raw_cost >= costmap_2d::INSCRIBED_INFLATED_OBSTACLE) {
            return false;
        }
        if (!escaping) {
            return static_cast<int>(raw_cost) <= planner_->max_allowed_cost_;
        }
        if (raw_cost > escape_ceiling) {
            return false;
        }
        if (update_escape_ceiling) {
            escape_ceiling = raw_cost;
            if (static_cast<int>(raw_cost) <= planner_->max_allowed_cost_) {
                escaping = false;
            }
        }
        return true;
    };

    if (!visit(static_cast<int>(first_x), static_cast<int>(first_y), true)) {
        return false;
    }

    unsigned int previous_x = first_x;
    unsigned int previous_y = first_y;
    for (std::size_t index = 1; index < path.size(); ++index) {
        unsigned int current_x = 0;
        unsigned int current_y = 0;
        if (!costmap_->worldToMap(
                path[index].pose.position.x, path[index].pose.position.y,
                current_x, current_y)) {
            return false;
        }

        int x = static_cast<int>(previous_x);
        int y = static_cast<int>(previous_y);
        const int target_x = static_cast<int>(current_x);
        const int target_y = static_cast<int>(current_y);
        const int nx = std::abs(target_x - x);
        const int ny = std::abs(target_y - y);
        const int step_x = (target_x > x) - (target_x < x);
        const int step_y = (target_y > y) - (target_y < y);
        int ix = 0;
        int iy = 0;

        while (ix < nx || iy < ny) {
            const std::int64_t lhs =
                static_cast<std::int64_t>(1 + 2 * ix) * ny;
            const std::int64_t rhs =
                static_cast<std::int64_t>(1 + 2 * iy) * nx;
            if (lhs == rhs) {
                if (ix < nx && iy < ny &&
                    (!visit(x + step_x, y, false) ||
                     !visit(x, y + step_y, false))) {
                    return false;
                }
                if (ix < nx) {
                    x += step_x;
                    ++ix;
                }
                if (iy < ny) {
                    y += step_y;
                    ++iy;
                }
            } else if (lhs < rhs) {
                x += step_x;
                ++ix;
            } else {
                y += step_y;
                ++iy;
            }
            if (!visit(x, y, true)) {
                return false;
            }
        }
        previous_x = current_x;
        previous_y = current_y;
    }
    return true;
}

bool ThetaStarPlanner::generatePositionPath(
    const geometry_msgs::PoseStamped& start,
    const geometry_msgs::PoseStamped& position_goal,
    std::vector<geometry_msgs::PoseStamped>& path,
    std::string& failure_reason)
{
    path.clear();
    unsigned int start_x = 0;
    unsigned int start_y = 0;
    unsigned int goal_x = 0;
    unsigned int goal_y = 0;
    if (!costmap_->worldToMap(
            start.pose.position.x, start.pose.position.y,
            start_x, start_y)) {
        failure_reason = "position-path start is outside the map";
        return false;
    }
    if (!costmap_->worldToMap(
            position_goal.pose.position.x,
            position_goal.pose.position.y,
            goal_x, goal_y)) {
        failure_reason = "position-path goal is outside the map";
        return false;
    }

    if (start_x == goal_x && start_y == goal_y) {
        path.push_back(start);
        if (path_tools::euclidean_distance(start, position_goal) > 1e-9) {
            path.push_back(position_goal);
            assignPathOrientations(path);
        }
        if (!isCentrelineCollisionFree(path)) {
            path.clear();
            failure_reason = "same-cell position path violates clearance";
            return false;
        }
        return true;
    }

    planner_->setStartAndGoal(start, position_goal);
    if (planner_->isUnsafeToPlan()) {
        failure_reason = "position-path start or goal is unsafe";
        return false;
    }

    std::vector<coordsW> raw_path;
    if (!planner_->generatePath(raw_path)) {
        failure_reason = cancel_requested_ ?
            "position-path planning canceled" :
            "Theta* could not reach the goal position";
        return false;
    }
    if (cancel_requested_) {
        failure_reason = "position-path planning canceled";
        return false;
    }

    std::vector<coordsW> interpolation_points;
    interpolation_points.reserve(raw_path.size() + 2);
    interpolation_points.push_back(
        {start.pose.position.x, start.pose.position.y});
    for (const auto& point : raw_path) {
        if (std::hypot(
                point.x - interpolation_points.back().x,
                point.y - interpolation_points.back().y) > 1e-9) {
            interpolation_points.push_back(point);
        }
    }
    if (std::hypot(
            position_goal.pose.position.x - interpolation_points.back().x,
            position_goal.pose.position.y - interpolation_points.back().y) >
        1e-9) {
        interpolation_points.push_back(
            {position_goal.pose.position.x,
             position_goal.pose.position.y});
    }

    auto dense_path = linearInterpolation(
        interpolation_points, costmap_->getResolution());
    if (dense_path.empty()) {
        failure_reason = "Theta* generated an empty interpolated path";
        return false;
    }
    assignPathOrientations(dense_path);

    std::vector<geometry_msgs::PoseStamped> candidate = dense_path;
    const auto downsampled = downsamplePath(dense_path, 0.4);
    if (downsampled.size() > 5) {
        auto smoothed = smoothPath(downsampled);
        if (smoothed.has_value()) {
            assignPathOrientations(smoothed.value());
            if (isCentrelineCollisionFree(smoothed.value())) {
                candidate = std::move(smoothed.value());
            }
        }
    }

    if (!isCentrelineCollisionFree(candidate)) {
        candidate = dense_path;
        assignPathOrientations(candidate);
        if (!isCentrelineCollisionFree(candidate)) {
            failure_reason =
                "position path violates centre-line clearance constraints";
            return false;
        }
    }

    path = std::move(candidate);
    return true;
}

std::vector<double> ThetaStarPlanner::terminalStraightLengths() const
{
    std::vector<double> lengths;
    for (double length = terminal_straight_length_;
         length >= terminal_min_straight_length_ - 1e-9;
         length -= terminal_straight_length_step_) {
        lengths.push_back(std::max(
            length, terminal_min_straight_length_));
    }
    if (lengths.empty() ||
        lengths.back() - terminal_min_straight_length_ > 1e-9) {
        lengths.push_back(terminal_min_straight_length_);
    }
    return lengths;
}

// 对路径进行降采样, 0.4m 间隔
std::vector<geometry_msgs::PoseStamped> ThetaStarPlanner::downsamplePath(const std::vector<geometry_msgs::PoseStamped>& orig_global_plan, double sampling_distance) {

    std::vector<geometry_msgs::PoseStamped> downsampled_path;
    if (orig_global_plan.empty()) {
        return downsampled_path;
    }
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
    // 添加最后一个点（确保首尾点都保留），但不生成同坐标重复点。
    if (path_tools::euclidean_distance(
            downsampled_path.back(), orig_global_plan.back()) > 1e-9) {
        downsampled_path.push_back(orig_global_plan.back());
    }

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

void ThetaStarPlanner::assignPathOrientations(
    std::vector<geometry_msgs::PoseStamped>& path,
    const std::optional<double>& final_yaw)
{
    if (path.empty()) {
        return;
    }

    for (size_t i = 0; i + 1 < path.size(); ++i) {
        size_t next = i + 1;
        while (next < path.size() &&
               path_tools::euclidean_distance(path[i], path[next]) <= 1e-9) {
            ++next;
        }
        if (next < path.size()) {
            const double yaw = std::atan2(
                path[next].pose.position.y - path[i].pose.position.y,
                path[next].pose.position.x - path[i].pose.position.x);
            path[i].pose.orientation = tf::createQuaternionMsgFromYaw(yaw);
        } else if (i > 0) {
            path[i].pose.orientation = path[i - 1].pose.orientation;
        }
    }

    if (final_yaw.has_value()) {
        path.back().pose.orientation =
            tf::createQuaternionMsgFromYaw(final_yaw.value());
    } else if (path.size() >= 2) {
        path.back().pose.orientation = path[path.size() - 2].pose.orientation;
    }
}

double ThetaStarPlanner::maximumDiscreteCurvature(
    const std::vector<geometry_msgs::PoseStamped>& path)
{
    double maximum = 0.0;
    for (size_t i = 1; i + 1 < path.size(); ++i) {
        const auto& a = path[i - 1].pose.position;
        const auto& b = path[i].pose.position;
        const auto& c = path[i + 1].pose.position;
        const double ab = std::hypot(b.x - a.x, b.y - a.y);
        const double bc = std::hypot(c.x - b.x, c.y - b.y);
        const double ac = std::hypot(c.x - a.x, c.y - a.y);
        if (ab <= 1e-9 || bc <= 1e-9 || ac <= 1e-9) {
            continue;
        }
        const double twice_area = std::fabs(
            (b.x - a.x) * (c.y - a.y) -
            (b.y - a.y) * (c.x - a.x));
        maximum = std::max(maximum, 2.0 * twice_area / (ab * bc * ac));
    }
    return maximum;
}

std::optional<std::vector<geometry_msgs::PoseStamped>>
ThetaStarPlanner::buildTerminalApproachGeometry(
    const std::vector<geometry_msgs::PoseStamped>& prefix,
    const geometry_msgs::PoseStamped& goal,
    double straight_length,
    double minimum_turn_radius,
    double sample_step,
    double maximum_prefix_splice_distance,
    const std::function<bool(
        const std::vector<geometry_msgs::PoseStamped>&)>& validator)
{
    if (prefix.empty() || !std::isfinite(straight_length) ||
        !std::isfinite(minimum_turn_radius) ||
        !std::isfinite(sample_step) ||
        !std::isfinite(maximum_prefix_splice_distance) ||
        straight_length < 0.0 || minimum_turn_radius < 0.0 ||
        sample_step <= 0.0 || maximum_prefix_splice_distance <= 0.0) {
        return std::nullopt;
    }

    const double goal_yaw = tf2::getYaw(goal.pose.orientation);
    const double target_x = std::cos(goal_yaw);
    const double target_y = std::sin(goal_yaw);
    const double end_x = goal.pose.position.x - straight_length * target_x;
    const double end_y = goal.pose.position.y - straight_length * target_y;
    const bool curvature_constrained = minimum_turn_radius > 1e-9;
    const double maximum_curvature = curvature_constrained ?
        1.0 / minimum_turn_radius :
        std::numeric_limits<double>::infinity();
    // Differential drive has no car-like minimum radius. Broader control-point
    // scales let the suffix optimizer represent both gentle early alignment
    // and compact high-curvature turns; collision validation still gates every
    // accepted candidate.
    const std::array<double, 7> control_scales{{
        0.15, 0.30, 0.55, 0.85, 1.15, 1.50, 2.00}};

    std::vector<size_t> splice_indices;
    double distance_from_anchor = 0.0;
    for (size_t index = prefix.size(); index-- > 0;) {
        if (index + 1 < prefix.size()) {
            distance_from_anchor += path_tools::euclidean_distance(
                prefix[index], prefix[index + 1]);
        }
        if (distance_from_anchor > maximum_prefix_splice_distance + 1e-9) {
            break;
        }
        // Do not splice at the goal itself and then loop away from it. Keep
        // enough of the original suffix available to replace it with a real
        // approach curve and the requested straight tail.
        if (distance_from_anchor + 1e-9 <
            straight_length + 2.0 * sample_step) {
            continue;
        }
        // A 10 cm stride is enough for candidate selection while the accepted
        // connector itself is still sampled at the configured 5 cm interval.
        if (splice_indices.empty() ||
            path_tools::euclidean_distance(
                prefix[index], prefix[splice_indices.back()]) >=
                2.0 * sample_step ||
            index == 0) {
            splice_indices.push_back(index);
        }
    }
    std::reverse(splice_indices.begin(), splice_indices.end());

    std::optional<std::vector<geometry_msgs::PoseStamped>> best;
    double best_score = std::numeric_limits<double>::infinity();
    auto append_if_distinct = [](auto& path, const auto& pose) {
        if (path.empty() ||
            path_tools::euclidean_distance(path.back(), pose) > 1e-9) {
            path.push_back(pose);
        }
    };

    for (const size_t splice : splice_indices) {
        const auto& start_point = prefix[splice];
        double tangent_x = 0.0;
        double tangent_y = 0.0;
        if (splice > 0) {
            tangent_x = start_point.pose.position.x -
                prefix[splice - 1].pose.position.x;
            tangent_y = start_point.pose.position.y -
                prefix[splice - 1].pose.position.y;
        } else if (prefix.size() > 1) {
            tangent_x = prefix[1].pose.position.x -
                start_point.pose.position.x;
            tangent_y = prefix[1].pose.position.y -
                start_point.pose.position.y;
        } else {
            const double start_yaw = tf2::getYaw(
                start_point.pose.orientation);
            tangent_x = std::cos(start_yaw);
            tangent_y = std::sin(start_yaw);
        }
        const double tangent_norm = std::hypot(tangent_x, tangent_y);
        if (tangent_norm <= 1e-9) {
            continue;
        }
        tangent_x /= tangent_norm;
        tangent_y /= tangent_norm;

        const double chord = std::hypot(
            end_x - start_point.pose.position.x,
            end_y - start_point.pose.position.y);
        if (chord <= sample_step) {
            continue;
        }

        // A cubic connector is compact when the prefix already approaches the
        // goal from a compatible direction.  When the robot is close to the
        // goal but points across the requested entry direction (the observed
        // block-4 failure), a forward-only U-turn may be required.  Enumerate
        // all six Dubins families so that this maneuver is represented as real
        // constant-curvature geometry instead of a yaw label jump.
        geometry_msgs::PoseStamped dubins_start = start_point;
        dubins_start.pose.orientation = tf::createQuaternionMsgFromYaw(
            std::atan2(tangent_y, tangent_x));
        geometry_msgs::PoseStamped dubins_end = goal;
        dubins_end.pose.position.x = end_x;
        dubins_end.pose.position.y = end_y;
        dubins_end.pose.orientation = tf::createQuaternionMsgFromYaw(goal_yaw);
        for (const auto& dubins : dubinsCandidates(
                 dubins_start.pose.position.x,
                 dubins_start.pose.position.y,
                 tf2::getYaw(dubins_start.pose.orientation),
                 end_x, end_y, goal_yaw, minimum_turn_radius)) {
            const auto sampled = sampleDubinsCandidate(
                dubins_start, dubins_end, dubins,
                minimum_turn_radius, sample_step);
            if (!sampled.has_value()) {
                continue;
            }

            std::vector<geometry_msgs::PoseStamped> candidate(
                prefix.begin(), prefix.begin() + splice + 1);
            for (size_t index = 1; index < sampled->size(); ++index) {
                append_if_distinct(candidate, sampled.value()[index]);
            }
            const int straight_segments = std::max(
                1, static_cast<int>(std::ceil(
                    straight_length / sample_step)));
            for (int segment = 1; segment <= straight_segments; ++segment) {
                const double ratio = static_cast<double>(segment) /
                    static_cast<double>(straight_segments);
                geometry_msgs::PoseStamped pose = goal;
                pose.pose.position.x = end_x +
                    ratio * (goal.pose.position.x - end_x);
                pose.pose.position.y = end_y +
                    ratio * (goal.pose.position.y - end_y);
                append_if_distinct(candidate, pose);
            }
            assignPathOrientations(candidate, goal_yaw);

            const size_t curvature_begin = splice > 0 ? splice - 1 : 0;
            const std::vector<geometry_msgs::PoseStamped> terminal_section(
                candidate.begin() + curvature_begin, candidate.end());
            const double curvature = maximumDiscreteCurvature(
                terminal_section);
            if (!std::isfinite(curvature) ||
                (curvature_constrained &&
                    curvature > 1.05 * maximum_curvature) ||
                (validator && !validator(candidate))) {
                continue;
            }

            double terminal_length = 0.0;
            for (size_t index = 1; index < terminal_section.size(); ++index) {
                terminal_length += path_tools::euclidean_distance(
                    terminal_section[index - 1], terminal_section[index]);
            }
            const double score = curvature + 0.02 * terminal_length;
            if (score < best_score) {
                best_score = score;
                best = std::move(candidate);
            }
        }

        for (const double first_scale : control_scales) {
            for (const double second_scale : control_scales) {
                const double p0x = start_point.pose.position.x;
                const double p0y = start_point.pose.position.y;
                const double p1x = p0x + chord * first_scale * tangent_x;
                const double p1y = p0y + chord * first_scale * tangent_y;
                const double p2x = end_x - chord * second_scale * target_x;
                const double p2y = end_y - chord * second_scale * target_y;
                const double p3x = end_x;
                const double p3y = end_y;
                const double control_polygon_length =
                    std::hypot(p1x - p0x, p1y - p0y) +
                    std::hypot(p2x - p1x, p2y - p1y) +
                    std::hypot(p3x - p2x, p3y - p2y);
                const double maximum_control_edge = std::max({
                    std::hypot(p1x - p0x, p1y - p0y),
                    std::hypot(p2x - p1x, p2y - p1y),
                    std::hypot(p3x - p2x, p3y - p2y)});
                // The cubic derivative magnitude is bounded by three times
                // the longest control-polygon edge. This segment count keeps
                // every sampled chord at or below sample_step, not merely the
                // average chord length.
                const int curve_segments = std::max(
                    2, std::max(
                        static_cast<int>(std::ceil(
                            control_polygon_length / sample_step)),
                        static_cast<int>(std::ceil(
                            3.0 * maximum_control_edge / sample_step))));

                std::vector<geometry_msgs::PoseStamped> candidate(
                    prefix.begin(), prefix.begin() + splice + 1);
                for (int segment = 1; segment <= curve_segments; ++segment) {
                    const double t = static_cast<double>(segment) /
                        static_cast<double>(curve_segments);
                    const double one_minus_t = 1.0 - t;
                    const double x =
                        one_minus_t * one_minus_t * one_minus_t * p0x +
                        3.0 * one_minus_t * one_minus_t * t * p1x +
                        3.0 * one_minus_t * t * t * p2x +
                        t * t * t * p3x;
                    const double y =
                        one_minus_t * one_minus_t * one_minus_t * p0y +
                        3.0 * one_minus_t * one_minus_t * t * p1y +
                        3.0 * one_minus_t * t * t * p2y +
                        t * t * t * p3y;
                    geometry_msgs::PoseStamped pose = goal;
                    pose.pose.position.x = x;
                    pose.pose.position.y = y;
                    append_if_distinct(candidate, pose);
                }

                const int straight_segments = std::max(
                    1, static_cast<int>(std::ceil(
                        straight_length / sample_step)));
                for (int segment = 1; segment <= straight_segments; ++segment) {
                    const double ratio = static_cast<double>(segment) /
                        static_cast<double>(straight_segments);
                    geometry_msgs::PoseStamped pose = goal;
                    pose.pose.position.x = end_x +
                        ratio * (goal.pose.position.x - end_x);
                    pose.pose.position.y = end_y +
                        ratio * (goal.pose.position.y - end_y);
                    append_if_distinct(candidate, pose);
                }
                assignPathOrientations(candidate, goal_yaw);

                const size_t curvature_begin = splice > 0 ? splice - 1 : 0;
                const std::vector<geometry_msgs::PoseStamped> terminal_section(
                    candidate.begin() + curvature_begin, candidate.end());
                const double curvature = maximumDiscreteCurvature(
                    terminal_section);
                if (!std::isfinite(curvature) ||
                    (curvature_constrained &&
                        curvature > 1.05 * maximum_curvature)) {
                    continue;
                }
                if (validator && !validator(candidate)) {
                    continue;
                }

                double terminal_length = 0.0;
                for (size_t index = 1; index < terminal_section.size(); ++index) {
                    terminal_length += path_tools::euclidean_distance(
                        terminal_section[index - 1], terminal_section[index]);
                }
                const double score = curvature + 0.02 * terminal_length;
                if (score < best_score) {
                    best_score = score;
                    best = std::move(candidate);
                }
            }
        }
    }
    return best;
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

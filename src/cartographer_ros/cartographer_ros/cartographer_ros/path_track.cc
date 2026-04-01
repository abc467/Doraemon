#include "path_track.h"

path_t current_path;
robot_pose current_robot_pose;
std::atomic<bool> task_loaded{false};

enum class TaskStatus : int
{
    IDLE = 0,
    NAVIGATING,
    PAUSED,
};
std::atomic<TaskStatus> navigation_status{TaskStatus::IDLE};

bool has_task_loaded()
{
    return task_loaded.load();
}

bool start_navigation()
{
    if (task_loaded.load())
    {
        navigation_status.store(TaskStatus::NAVIGATING);
        return true;
    }
    else
    {
        return false;
    }
}

void pause_navigation()
{
    navigation_status.store(TaskStatus::PAUSED);
}

void stop_navigation()
{
    navigation_status.store(TaskStatus::IDLE);
}

bool is_navigating()
{
    return navigation_status.load() == TaskStatus::NAVIGATING || navigation_status.load() == TaskStatus::PAUSED;
}

bool is_nav_paused()
{
    return navigation_status.load() == TaskStatus::PAUSED;
}

bool is_goal_id(const std::string &id)
{
    if (current_path.goal_id.empty())
    {
        return false;
    }
    if (current_path.goal_id == id)
    {
        return true;
    }
    else
    {
        return false;
    }
}

bool parse_path(const std::string &pathjson)
{
    try
    {
        task_loaded = false;
        current_path.path_name = "";
        current_path.path2d = true;
        current_path.flags.clear();

        nlohmann::json pathdata = nlohmann::json::parse(pathjson);
        current_path.path_name = pathdata["path_name"].get<std::string>();
        current_path.goal_id = pathdata["goal_id"].get<std::string>();
        current_path.path2d = pathdata["2D"].get<bool>();
        for (auto flag = pathdata["flags"].begin(); flag != pathdata["flags"].end(); flag++)
        {
            path_flag f;
            f.id = (*flag)["flag_id"].get<int>();

            f.prev_anchor.x = (*flag)["prev_anchor_pos"]["x"].get<double>();
            f.prev_anchor.y = (*flag)["prev_anchor_pos"]["y"].get<double>();
            f.prev_anchor.z = (*flag)["prev_anchor_pos"]["z"].get<double>();

            f.flag.x = (*flag)["flag_pos"]["x"].get<double>();
            f.flag.y = (*flag)["flag_pos"]["y"].get<double>();
            if (current_path.path2d)
            {
                f.flag.z = 0;
            }
            else
            {
                f.flag.z = (*flag)["flag_pos"]["z"].get<double>();
            }

            f.next_anchor.x = (*flag)["next_anchor_pos"]["x"].get<double>();
            f.next_anchor.y = (*flag)["next_anchor_pos"]["y"].get<double>();
            f.next_anchor.z = (*flag)["next_anchor_pos"]["z"].get<double>();

            f.backward = (*flag)["backward"].get<bool>();
            f.acceleration = (*flag)["acceleration"].get<double>();
            f.velocity = (*flag)["velocity"].get<double>();

            current_path.flags.emplace_back(f);
        }
        task_loaded = true;

        return true;
    }
    catch (const std::exception &e)
    {
        std::cout << e.what() << std::endl;
        return false;
    }
}

path_vec3_t cubic_bezier(double t, const path_vec3_t &beg, const path_vec3_t &anc1, const path_vec3_t &anc2, const path_vec3_t &end)
{
    path_vec3_t dst;
    double k1 = (1 - t) * (1 - t) * (1 - t);
    double k2 = 3 * (1 - t) * (1 - t) * t;
    double k3 = 3 * (1 - t) * t * t;
    double k4 = t * t * t;

    dst.x = k1 * beg.x + k2 * anc1.x + k3 * anc2.x + k4 * end.x;
    dst.y = k1 * beg.y + k2 * anc1.y + k3 * anc2.y + k4 * end.y;
    dst.z = k1 * beg.z + k2 * anc1.z + k3 * anc2.z + k4 * end.z;
    return dst;
}

path_vec3_t cubic_bezier_derivative(double t, const path_vec3_t &beg, const path_vec3_t &anc1, const path_vec3_t &anc2, const path_vec3_t &end)
{
    path_vec3_t derivative;
    // 计算贝塞尔曲线在t处的导数
    derivative.x = 3 * (1 - t) * (1 - t) * (anc1.x - beg.x) + 6 * (1 - t) * t * (anc2.x - anc1.x) + 3 * t * t * (end.x - anc2.x);
    derivative.y = 3 * (1 - t) * (1 - t) * (anc1.y - beg.y) + 6 * (1 - t) * t * (anc2.y - anc1.y) + 3 * t * t * (end.y - anc2.y);
    derivative.z = 3 * (1 - t) * (1 - t) * (anc1.z - beg.z) + 6 * (1 - t) * t * (anc2.z - anc1.z) + 3 * t * t * (end.z - anc2.z);

    return derivative;
}

double compute_l2_distance(path_vec3_t a, path_vec3_t b)
{
    double x = a.x - b.x;
    double y = a.y - b.y;
    double z = a.z - b.z;
    double s = std::sqrt(x * x + y * y + z * z);
    return s;
}

void start_track(std::function<void(geometry_msgs::PoseStamped &)> publish_pose_stamp_cb)
{
    start_navigation();
    uint32_t seq = 0;
    for (std::size_t i = 1; i < current_path.flags.size(); i++)
    {
        path_vec3_t beg = current_path.flags[i - 1].flag;
        path_vec3_t anc1 = current_path.flags[i - 1].next_anchor;
        path_vec3_t anc2 = current_path.flags[i].prev_anchor;
        path_vec3_t end = current_path.flags[i].flag;
        anc1.x += beg.x;
        anc1.y += beg.y;
        anc1.z += beg.z;

        anc2.x += end.x;
        anc2.y += end.y;
        anc2.z += end.z;

        for (float t = 0; t < 1; t += 0.05)
        {
            auto dst = cubic_bezier(t, beg, anc1, anc2, end);
            path_vec3_t dst_vec = cubic_bezier_derivative(t, beg, anc1, anc2, end);
            // auto d_dst = cubic_bezier(t + 0.05, beg, anc1, anc2, end);
            // path_vec3_t dst_vec;
            // dst_vec.x = d_dst.x - dst.x;
            // dst_vec.y = d_dst.y - dst.y;
            // dst_vec.z = d_dst.z - dst.z;
            double l2_norm = std::sqrt(dst_vec.x * dst_vec.x + dst_vec.y * dst_vec.y + dst_vec.z * dst_vec.z);
            dst_vec.x /= l2_norm;
            dst_vec.y /= l2_norm;
            dst_vec.z /= l2_norm;

            path_vec3_t rotation_vec;
            rotation_vec.x = 0;
            rotation_vec.y = -dst_vec.z;
            rotation_vec.z = dst_vec.y;

            double cos_theta = dst_vec.x;
            double theta = std::acos(cos_theta);
            double cos_half_theta = std::cos(theta / 2);
            double sin_half_theta = std::sin(theta / 2);

            double dst_qw = cos_half_theta;
            double dst_qx = 0;
            double dst_qy = sin_half_theta * rotation_vec.y;
            double dst_qz = sin_half_theta * rotation_vec.z;

            // publish goal
            geometry_msgs::PoseStamped goal;
            goal.header.frame_id = "map";
            goal.header.stamp = ros::Time::now();
            goal.header.seq = seq++;

            goal.pose.position.x = dst.x;
            goal.pose.position.y = dst.y;
            goal.pose.position.z = dst.z;
            goal.pose.orientation.w = dst_qw;
            goal.pose.orientation.x = dst_qx;
            goal.pose.orientation.y = dst_qy;
            goal.pose.orientation.z = dst_qz;
            publish_pose_stamp_cb(goal);
            // _move_goal.publish(goal);
            // ROS_WARN("NAV: x=%.4f y=%.4f z=%.4f  qw=%.4f  qx=%.4f  qy=%.4f  qz=%.4f", dst.x, dst.y, dst.z, dst_qw, dst_qx, dst_qy, dst_qz);
            // wait for arrival
            while (is_navigating())
            {
                double pos_err = std::abs(current_robot_pose.x - goal.pose.position.x) +
                                 std::abs(current_robot_pose.y - goal.pose.position.y);
                if (pos_err < 0.5)
                {
                    // ROS_WARN("Arrival");
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }
            // std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            if (!is_navigating())
            {
                break;
            }
        }
        if (!is_navigating())
        {
            break;
        }
    }
    // ROS_WARN("Navigation Finished!");
    stop_navigation();
}

void publish_track_path(std::function<void(nav_msgs::Path &)> publish_path_cb)
{
    std::vector<geometry_msgs::PoseStamped> pose_stamps;
    uint32_t path_seq = 0;
    uint32_t seq = 0;
    for (std::size_t i = 1; i < current_path.flags.size(); i++)
    {
        path_vec3_t beg = current_path.flags[i - 1].flag;
        path_vec3_t anc1 = current_path.flags[i - 1].next_anchor;
        path_vec3_t anc2 = current_path.flags[i].prev_anchor;
        path_vec3_t end = current_path.flags[i].flag;
        anc1.x += beg.x;
        anc1.y += beg.y;
        anc1.z += beg.z;

        anc2.x += end.x;
        anc2.y += end.y;
        anc2.z += end.z;

        // 计算路径路程
        double path_distance = 0;
        uint32_t t_index = 0;
        path_vec3_t last_point;
        std::vector<double> tset;
        tset.reserve(5000);

        // 这里用于导航点的精度（贝塞尔点与点的物理距离, 单位meter）
        double resolution = 0.05;
        auto _beg = cubic_bezier(0, beg, anc1, anc2, end);
        auto _end = cubic_bezier(0.99999, beg, anc1, anc2, end);
        double s = compute_l2_distance(_beg, _end);
        double delta = 1 / (5 * (s / resolution));

        for (double t = 0; t < 1; t += delta)
        {
            auto dst = cubic_bezier(t, beg, anc1, anc2, end);
            if (t_index == 0)
            {
                tset.push_back(t);
            }
            else
            {
                double s = compute_l2_distance(dst, last_point);
                path_distance += s;
                if (path_distance >= resolution)
                {
                    tset.push_back(t);
                    path_distance = 0;
                }
            }
            last_point.x = dst.x;
            last_point.y = dst.y;
            last_point.z = dst.z;
            t_index++;
        }

        for (auto &&t : tset)
        {
            auto dst = cubic_bezier(t, beg, anc1, anc2, end);
            auto d_dst = cubic_bezier(t + 0.05, beg, anc1, anc2, end);
            path_vec3_t dst_vec;
            dst_vec.x = d_dst.x - dst.x;
            dst_vec.y = d_dst.y - dst.y;
            dst_vec.z = d_dst.z - dst.z;
            double norm_dst_vec = std::sqrt(dst_vec.x * dst_vec.x + dst_vec.y * dst_vec.y + dst_vec.z * dst_vec.z);

            path_vec3_t rotation_vec;
            rotation_vec.x = 0;
            rotation_vec.y = -dst_vec.z;
            rotation_vec.z = dst_vec.y;

            double l2_norm = std::sqrt(rotation_vec.x * rotation_vec.x + rotation_vec.y * rotation_vec.y + rotation_vec.z * rotation_vec.z);
            rotation_vec.x /= l2_norm;
            rotation_vec.y /= l2_norm;
            rotation_vec.z /= l2_norm;

            double cos_theta = dst_vec.x / norm_dst_vec;
            double theta = std::acos(cos_theta);
            double cos_half_theta = std::cos(theta / 2);
            double sin_half_theta = std::sin(theta / 2);

            double dst_qw = cos_half_theta;
            double dst_qx = 0;
            double dst_qy = sin_half_theta * rotation_vec.y;
            double dst_qz = sin_half_theta * rotation_vec.z;

            // publish goal
            geometry_msgs::PoseStamped goal;
            goal.header.frame_id = "map";
            goal.header.stamp = ros::Time::now();
            goal.header.seq = seq++;

            goal.pose.position.x = dst.x;
            goal.pose.position.y = dst.y;
            goal.pose.position.z = dst.z;
            goal.pose.orientation.w = dst_qw;
            goal.pose.orientation.x = dst_qx;
            goal.pose.orientation.y = dst_qy;
            goal.pose.orientation.z = dst_qz;
            pose_stamps.emplace_back(goal);
        }
    }
    nav_msgs::Path path;
    path.header.frame_id = "map";
    path.header.stamp = ros::Time::now();
    path.header.seq = path_seq++;
    path.poses = std::move(pose_stamps);
    publish_path_cb(path);
}
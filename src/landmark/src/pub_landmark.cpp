/*this code is wirted for three kinds of landmarks with cartographer
---this node need to coordinate with cartographer and DM-reader--tcp_node
---make sure cartographer publish topic:/tracked_pose--open from cartographer configure file
*/

#include <ros/ros.h>
#include <sensor_msgs/LaserScan.h>
#include <visualization_msgs/MarkerArray.h>
#include <cartographer_ros_msgs/LandmarkList.h>
#include <cartographer_ros_msgs/LandmarkEntry.h>
#include <geometry_msgs/PoseStamped.h>
#include <Eigen/Dense>
#include <unordered_map>
#include <vector>
#include <cmath>
#include <tf/tf.h>
#include <regex>
#include <fstream>
#include <iostream>
#include <queue>
#include <tf2_ros/buffer.h>
#include <tf2_ros/transform_listener.h>
#include <tf2_geometry_msgs/tf2_geometry_msgs.h>
#include <tf2/utils.h>          
#include <tf2/transform_datatypes.h> 

#include "param_space/ParamChanges.h"
#include "param_space/SetParam.h"
#include "param_space/GetParam.h"
// Parameter
bool use_reflect_panel;
bool use_reflect_column;
double intensity_threshold;
double min_colum_points_per_cluster;
double min_panel_points_per_cluster;
double max_circle_fit_error;
double max_line_fit_error;
double column_radius;
double position_threshold;
double panel_length;
double panel_length_noise;
double translation_weight;
bool debug_mode = false;
std::string frame_id;
std::string tracking_frame;
//
// 全局TF对象

std::shared_ptr<tf2_ros::Buffer> tf_buffer;
std::shared_ptr<tf2_ros::TransformListener> tf_listener;
//
ros::NodeHandle *g_nh;
std::vector<ros::Subscriber> subscribers;
bool slam_mode = true;
volatile bool flag_started = false;
ros::ServiceClient client_get_param;
ros::Publisher landmark_pub;
geometry_msgs::PoseStamped robot_gl_pose;
std_msgs::Header scan_header;
std::unordered_map<std::string, geometry_msgs::Pose> landmark_cricle_pose_map;
std::unordered_map<std::string, geometry_msgs::Pose> landmark_line_pose_map;
int current_landmark_id = 0;
//
size_t continue_count = 0;
size_t line_count = 0;
size_t circle_count = 0;
//
struct intensity_ponit
{
    double x;
    double y;
    double intensity;
};
class Fit
{
public:
    // double mean_intensity;
    double line_fit_error;
    Eigen::Vector2d line_center;
    double cricle_fit_error;
    Eigen::Vector2d circle_center;
    double radius;
    double max_lenth;
    Fit(/* args */);
    void fit_line(const std::vector<intensity_ponit> &points);
    void fit_circle(const std::vector<intensity_ponit> &points);
    void lenth(const std::vector<intensity_ponit> &points);
};

Fit::Fit(/* args */)
{
}
// 最小二乘拟合直线
void Fit::fit_line(const std::vector<intensity_ponit> &points)
{
    double m, b, sum_x, sum_y, sum_intens;
    int n = points.size();
    Eigen::MatrixXd A(n, 2);
    Eigen::VectorXd B(n);
    for (int i = 0; i < n; ++i)
    {
        A(i, 0) = points[i].x; // x
        A(i, 1) = 1.0;         // 截距项 b
        B(i) = points[i].y;    // y
    }
    Eigen::Vector2d solution = A.bdcSvd(Eigen::ComputeThinU | Eigen::ComputeThinV).solve(B);
    m = solution(0); // 直线的斜率
    b = solution(1); // 直线的截距
    double mse = 0.0;
    sum_x = 0.0;
    sum_y = 0.0;
    for (const auto &point : points)
    {
        sum_x += point.x;
        sum_y += point.y;
        double predicted_y = m * point.x + b;
        mse += std::fabs(predicted_y - point.y);
    }
    double mean_x = sum_x / n;
    double mean_y = sum_y / n;
    line_fit_error = (mse / n) * 100;
    line_center.x() = mean_x;
    line_center.y() = mean_y;
    if (debug_mode)
        std::cout << "line_center:" << line_center.x() << "," << line_center.y() << "line_fit_error:" << line_fit_error << "\n"
                  << std::endl;
}
// 最小二乘拟合圆
void Fit::fit_circle(const std::vector<intensity_ponit> &points)
{
    int n = points.size();
    Eigen::MatrixXd A(n, 3);
    Eigen::VectorXd B(n);

    for (int i = 0; i < n; ++i)
    {
        double x = points[i].x;
        double y = points[i].y;
        A(i, 0) = 2 * x;
        A(i, 1) = 2 * y;
        A(i, 2) = 1;
        B(i) = (x * x + y * y);
    }
    Eigen::Vector3d X = A.bdcSvd(Eigen::ComputeThinU | Eigen::ComputeThinV).solve(B);
    double cx = X(0);
    double cy = X(1);
    double c = X(2);
    circle_center.x() = cx;
    circle_center.y() = cy;
    radius = (sqrt(cx * cx + cy * cy + c)) * 100;
    // Calculate circle fit error
    double MSE = 0.0;
    for (const auto &point : points)
    {
        double x = point.x;
        double y = point.y;
        double dx = x - circle_center.x();
        double dy = y - circle_center.y();
        double distance = (sqrt(dx * dx + dy * dy)) * 100;
        double mse = abs(distance - radius);
        MSE += mse;
    }
    cricle_fit_error = (MSE / n);
    if (debug_mode)
        std::cout << "circle_center:" << circle_center.x() << "," << circle_center.y() << "radius:"
                  << radius << "cricle_fit_error:" << cricle_fit_error << "\n"
                  << std::endl;
}
// 提取强度高于阈值的点
std::vector<intensity_ponit> withdraw_high_intensity_points(const sensor_msgs::LaserScan::ConstPtr &scan)
{
    std::vector<intensity_ponit> intens_points;
    intensity_ponit point;
    for (size_t i = 0; i < scan->ranges.size(); ++i)
    {
        if (scan->intensities[i] > intensity_threshold)
        {
            double angle = scan->angle_min + i * scan->angle_increment;
            point.x = scan->ranges[i] * cos(angle);
            point.y = scan->ranges[i] * sin(angle);
            point.intensity = scan->intensities[i];
            intens_points.push_back(point);
        }
    }
    return intens_points;
}
// 聚类
std::vector<std::vector<intensity_ponit>> Cluster_points(const std::vector<intensity_ponit> &points)
{
    std::vector<std::vector<intensity_ponit>> clusters;
    if (points.empty())
    {
        return clusters;
    }

    std::vector<bool> visited(points.size(), false);
    for (size_t i = 0; i < points.size(); ++i)
    {
        if (!visited[i])
        {
            std::vector<intensity_ponit> cluster;
            std::queue<size_t> q;
            q.push(i);
            visited[i] = true;

            while (!q.empty())
            {
                size_t current = q.front();
                q.pop(); // Removes first element.
                cluster.push_back(points[current]);

                // 遍历所有点，寻找未访问的邻近点
                for (size_t j = 0; j < points.size(); ++j)
                {
                    if (!visited[j])
                    {
                        Eigen::Vector2d p1(points[current].x, points[current].y);
                        Eigen::Vector2d p2(points[j].x, points[j].y);
                        if ((p1 - p2).norm() < 0.03)
                        {
                            q.push(j);
                            visited[j] = true;
                        }
                    }
                }
            }
            clusters.push_back(cluster);
        }
    }
    return clusters;
}
// calcuate landmarks globle pose
geometry_msgs::Pose calculateWorldPose(const geometry_msgs::Pose &lidar_landmark_pose, const geometry_msgs::Pose &robot_gl_pose)
{
    geometry_msgs::TransformStamped tf;
    tf = tf_buffer->lookupTransform(tracking_frame, scan_header.frame_id, ros::Time(0),ros::Duration(0.1));
        // 1. 将地标从雷达坐标系转换到base_link坐标系
    geometry_msgs::Pose landmark_in_base;
    tf2::doTransform(lidar_landmark_pose, landmark_in_base, tf);

    geometry_msgs::Pose world_pose;
    // 2. 获取机器人位姿信息
    double rx = robot_gl_pose.position.x;
    double ry = robot_gl_pose.position.y;
    double rz = robot_gl_pose.position.z;
    const geometry_msgs::Pose &q = robot_gl_pose;
    double robot_yaw=tf2::getYaw(q.orientation);

    world_pose.position.x = rx + landmark_in_base.position.x * cos(robot_yaw) 
                          - landmark_in_base.position.y * sin(robot_yaw);
    world_pose.position.y = ry + landmark_in_base.position.x * sin(robot_yaw) 
                          + landmark_in_base.position.y * cos(robot_yaw);
    world_pose.position.z = rz + landmark_in_base.position.z;
    
    world_pose.orientation.w = 1.0;
    world_pose.orientation.x = 0.0;
    world_pose.orientation.y = 0.0;
    world_pose.orientation.z = 0.0;

    return world_pose;
}

// mean intensity of cluster
double mean_intensity(const std::vector<intensity_ponit> &cluster)
{
    double mean = 0;
    for (size_t i = 0; i < cluster.size(); i++)
    {
        mean += cluster.at(i).intensity;
    }
    return mean / cluster.size();
}
// calculate lenth
void Fit::lenth(const std::vector<intensity_ponit> &cluster)
{
    max_lenth = 0;
    for (size_t i = 0; i < cluster.size(); i++)
    {
        for (size_t j = 0; j < cluster.size(); j++)
        {
            double dx = cluster.at(i).x - cluster.at(j).x;
            double dy = cluster.at(i).y - cluster.at(j).y;
            double lenth = sqrt(dx * dx + dy * dy);
            if (lenth > max_lenth)
            {
                max_lenth = lenth;
            }
        }
    }
}
//
void scanCallback(const sensor_msgs::LaserScan::ConstPtr &scan)
{
    // 在全局重定位成功之前，先不要进行检测Landmark
    scan_header = scan->header;
    if (!flag_started)
    {
        return;
    }
    Fit fit;
    bool maybe_landmark = false;
    cartographer_ros_msgs::LandmarkList landmark_list_msg;
    cartographer_ros_msgs::LandmarkEntry landmark_circle;
    cartographer_ros_msgs::LandmarkEntry landmark_line;
    landmark_list_msg.header.stamp = scan->header.stamp;
    landmark_list_msg.header.frame_id = scan->header.frame_id;
    // 提取强度异常点
    std::vector<intensity_ponit> high_intens_points = withdraw_high_intensity_points(scan);
    // 聚类
    std::vector<std::vector<intensity_ponit>> clusters_points = Cluster_points(high_intens_points);
    if (debug_mode)
        std::cout << "clusters_size:" << clusters_points.size() << std::endl;

    for (const auto cluster : clusters_points)
    {
        // if (cluster.size() > min_colum_points_per_cluster)
        {
            maybe_landmark = true;
            bool is_circle = false;
            bool is_line = false;
            double mean_intens = mean_intensity(cluster);
            if (debug_mode)
                std::cout << "mean_intensity:" << mean_intens << "  cluster_size:" << cluster.size() << "\n"
                          << std::endl;
            fit.fit_circle(cluster);
            fit.fit_line(cluster);
            fit.lenth(cluster);
            if (debug_mode)
                std::cout << "max_lenth:" << fit.max_lenth << std::endl;
            if (use_reflect_column)
            {
                if ((fit.cricle_fit_error < max_circle_fit_error) &&
                    (fit.radius < (column_radius + 2.0)) &&
                    ((column_radius - 1.0) < fit.radius) &&
                    (cluster.size() > min_colum_points_per_cluster) &&
                    (((2 * column_radius - 2) * 0.7) < (fit.max_lenth * 100)) &&
                    (((2 * column_radius + 4) * 0.9) > (fit.max_lenth * 100)))
                {
                    circle_count++;
                    is_circle = true;
                    // finish_fit = true;
                    // 圆拟合误差较低，选择圆
                    if (debug_mode)
                        std::cout << "反光柱" << std::endl;
                    landmark_circle.tracking_from_landmark_transform.position.x = fit.circle_center.x();
                    landmark_circle.tracking_from_landmark_transform.position.y = fit.circle_center.y();
                    landmark_circle.tracking_from_landmark_transform.position.z = 0.0;
                    landmark_circle.tracking_from_landmark_transform.orientation.w = 1.0;
                    landmark_circle.translation_weight = translation_weight;
                    landmark_circle.rotation_weight = 0;

                    // 计算世界坐标
                    geometry_msgs::Pose lidar_landmark_pose;
                    lidar_landmark_pose.position.x = landmark_circle.tracking_from_landmark_transform.position.x;
                    lidar_landmark_pose.position.y = landmark_circle.tracking_from_landmark_transform.position.y;
                    geometry_msgs::Pose world_landmark_pose = calculateWorldPose(lidar_landmark_pose,robot_gl_pose.pose);
                    std::cout << "当前landmark世界位置:" << world_landmark_pose.position.x << "," << world_landmark_pose.position.y << std::endl;
                    if (landmark_cricle_pose_map.empty())
                    {
                        landmark_circle.id = "landmark0" + std::to_string(current_landmark_id);
                        std::cout<<"第一个反光柱landmark"<<std::endl;
                    }
                    else
                    {
                        bool found_similar_landmark = false;
                        int max_existing_id = 0;
                        for (const auto &item : landmark_cricle_pose_map)
                        {
                            std::string id = item.first;
                            geometry_msgs::Pose stored_pose = item.second;
                            double dx = stored_pose.position.x - world_landmark_pose.position.x;
                            double dy = stored_pose.position.y - world_landmark_pose.position.y;
                            double dz = stored_pose.position.z - world_landmark_pose.position.z;
                            double distance = sqrt(dx * dx + dy * dy + dz * dz);
                            if (distance < position_threshold)
                            {
                                landmark_circle.id = id;
                                found_similar_landmark = true;
                                std::cout<<"已有的反光柱landmark:"<<id<<std::endl;
                                break;
                            }

                            int num_id = std::stoi(id.substr(9)); // 假设ID格式为"landmarkX"
                            if (num_id > max_existing_id)
                            {
                                max_existing_id = num_id;
                            }
                        }
                        if (!found_similar_landmark)
                        {
                            landmark_circle.id = "landmark0" + std::to_string(max_existing_id + 1);
                            std::cout<<"新的反光柱landmark:"<<landmark_circle.id<<std::endl;
                        }
                    }
                }
            }
            if (use_reflect_panel)
            {
                if (debug_mode)
                    std::cout << "Start to judge pannel:" << std::endl;

                if (debug_mode)
                {
                    if (fit.line_fit_error < max_line_fit_error)
                    {
                        std::cout << "Condition <fit.line_fit_error < max_line_fit_error> passed!" << std::endl;
                    }
                    if (mean_intens > intensity_threshold)
                    {
                        std::cout << "Condition <mean_intens > intensity_threshold> passed!" << std::endl;
                    }
                    if (cluster.size() > min_panel_points_per_cluster)
                    {
                        std::cout << "Condition <cluster.size() > min_panel_points_per_cluster> passed!" << std::endl;
                    }
                    else
                    {
                        std::cout << "cluster.size()=" << cluster.size() << std::endl;
                        std::cout << "min_panel_points_per_cluster=" << min_panel_points_per_cluster << std::endl;
                    }
                    if (fit.max_lenth < (panel_length + panel_length_noise))
                    {
                        std::cout << "Condition <fit.max_lenth < (panel_length + panel_length_noise)> passed!" << std::endl;
                    }
                    if (fit.max_lenth > (panel_length - panel_length_noise))
                    {
                        std::cout << "Condition <fit.max_lenth > (panel_length - panel_length_noise)> passed!" << std::endl;
                    }
                }
                if ((fit.line_fit_error < max_line_fit_error) &&
                    (mean_intens > intensity_threshold) &&
                    (cluster.size() > min_panel_points_per_cluster) &&
                    (fit.max_lenth < (panel_length + panel_length_noise)) &&
                    (fit.max_lenth > (panel_length - panel_length_noise)))
                {
                    // if (fit.cricle_fit_error > 0.6 || fit.radius > 20)
                    { // 直线拟合误差较低，选择直线
                        line_count++;
                        is_line = true;
                        if (debug_mode)
                            std::cout << "反光贴" << std::endl;
                        landmark_line.tracking_from_landmark_transform.position.x = fit.line_center.x();
                        landmark_line.tracking_from_landmark_transform.position.y = fit.line_center.y();
                        landmark_line.tracking_from_landmark_transform.position.z = 0.0;
                        landmark_line.tracking_from_landmark_transform.orientation.w = 1.0;
                        landmark_line.translation_weight = translation_weight; // 0.55e4;
                        landmark_line.rotation_weight = 0;

                        // 计算世界坐标
                        geometry_msgs::Pose lidar_landmark_pose;
                        lidar_landmark_pose.position.x = landmark_line.tracking_from_landmark_transform.position.x;
                        lidar_landmark_pose.position.y = landmark_line.tracking_from_landmark_transform.position.y;
                        geometry_msgs::Pose world_landmark_pose = calculateWorldPose(lidar_landmark_pose,robot_gl_pose.pose);
                        std::cout << "当前landmark世界位置:" << world_landmark_pose.position.x << "," << world_landmark_pose.position.y << std::endl;
                        // ID管理
                        if (landmark_line_pose_map.empty())
                        {
                            landmark_line.id = "landmark1" + std::to_string(current_landmark_id);
                            std::cout<<"第一个反光贴landmark"<<std::endl;
                        }
                        else
                        {
                            bool found_similar_landmark = false;
                            int max_existing_id = 0;
                            for (const auto &item : landmark_line_pose_map)
                            {
                                std::string id = item.first;
                                geometry_msgs::Pose stored_pose = item.second;
                                double dx = stored_pose.position.x - world_landmark_pose.position.x;
                                double dy = stored_pose.position.y - world_landmark_pose.position.y;
                                double dz = stored_pose.position.z - world_landmark_pose.position.z;
                                double distance = sqrt(dx * dx + dy * dy + dz * dz);
                                if (distance < position_threshold)
                                {
                                    landmark_line.id = id;
                                    std::cout<<"已有的反光贴landmark:"<<id<<std::endl;
                                    found_similar_landmark = true;
                                    break;
                                }

                                int num_id = std::stoi(id.substr(9)); // 假设ID格式为"landmarkX"
                                if (num_id > max_existing_id)
                                {
                                    max_existing_id = num_id;
                                }
                            }
                            if (!found_similar_landmark)
                            {
                                landmark_line.id = "landmark1" + std::to_string(max_existing_id + 1);
                                std::cout<<"新的反光柱landmark:"<<landmark_line.id<<std::endl;
                            }
                        }
                    }
                }
            }
            if (!is_circle && !is_line)
            {
                // ROS_WARN("fit_false");
                continue;
            }
            if (is_circle && circle_count > (line_count + 1))
            {
                landmark_list_msg.landmarks.push_back(landmark_circle);
            }
            if (is_line && line_count > (circle_count + 1))
            {
                landmark_list_msg.landmarks.push_back(landmark_line);
            }
        }
    }
    if (!maybe_landmark)
    {
        continue_count++;
    }
    if (continue_count > 25)
    {
        continue_count = 0;
        circle_count = 0;
        line_count = 0;
    }
    // std::cout << "continue_count: " << continue_count << " circle_count: "
    //           << circle_count << " line_count:" << line_count << std::endl;
    landmark_pub.publish(landmark_list_msg);
}
// get robot globle pose
void tracedPoseCallback(const geometry_msgs::PoseStamped::ConstPtr &msg)
{
    robot_gl_pose = *msg;
}
// get landmarks globle pose
void landmarkPoseListCallback(const visualization_msgs::MarkerArray::ConstPtr &msg)
{

    for (const auto &marker : msg->markers)
    {
        std::string text_id = marker.text;
        geometry_msgs::Pose pose = marker.pose;
        if (text_id[8] == '0')

        {
            if (landmark_cricle_pose_map.find(text_id) != landmark_cricle_pose_map.end())
            {
                landmark_cricle_pose_map[text_id] = pose;
            }
            else
            {
                landmark_cricle_pose_map.insert({text_id, pose});
            }
        }
        else if (text_id[8] == '1')
        {
            if (landmark_line_pose_map.find(text_id) != landmark_line_pose_map.end())
            {
                landmark_line_pose_map[text_id] = pose;
            }
            else
            {
                landmark_line_pose_map.insert({text_id, pose});
            }
        }
        else
        {
            continue;
        }
    }
}
// publish DM_landmark
void DMcallback(const cartographer_ros_msgs::LandmarkList &qr_pose)
{
    // if (scan_header.frame_id.empty())
    // {
    //     return;
    // }
    cartographer_ros_msgs::LandmarkList qr_msg = qr_pose;
    qr_msg.header.stamp = scan_header.stamp;
    qr_msg.header.frame_id = frame_id;
    // for (auto &&i : qr_msg.landmarks)
    // {
    //     i.translation_weight = translation_weight;
    //     i.rotation_weight = translation_weight;
    // }

    landmark_pub.publish(qr_msg);
    ROS_WARN("publish_DM_landmark");
}
//
void start()
{
    if (!flag_started)
    {
        std::cout << "start!" << std::endl;
        // subscribers.push_back(g_nh->subscribe("scan", 10, scanCallback));
        subscribers.push_back(g_nh->subscribe("tracked_pose", 10, tracedPoseCallback));
        subscribers.push_back(g_nh->subscribe("landmark_poses_list", 10, landmarkPoseListCallback));
        flag_started = true;
    }
}

// 用于接受系统当前状态信息，决定是否开启landmark功能
void param_changes_handler(const param_space::ParamChangesConstPtr &changes)
{

    if (flag_started)
    {
        return;
    }
    bool is_slam_mode = false;
    bool is_global_relocated = false;
    bool success_relocated = false;
    for (auto &&i : changes->changed_params)
    {
        if (i == "slam_mode")
        {
            ROS_WARN("slam_mode");
            is_slam_mode = true;
        }
        else if (i == "global_relocated")
        {
            ROS_WARN("relocation_mode");
            is_global_relocated = true;
        }
    }

    if (is_slam_mode == true)
    {
        param_space::GetParam srv;
        srv.request.keys.push_back("slam_mode");
        client_get_param.call(srv);
        if (srv.response.opcode == 0)
        {
            if (srv.response.vals.at(0) == "true")
            {
                slam_mode = true;
                start();
            }
            else
            {
                slam_mode = false;
            }
        }
    }

    if (is_global_relocated)
    {
        param_space::GetParam srv;
        srv.request.keys.push_back("global_relocated");
        client_get_param.call(srv);
        if (srv.response.opcode == 0)
        {
            if (srv.response.vals.at(0) == "true")
            {
                success_relocated = true;
                start();
            }
            else
            {
                success_relocated = false;
            }
        }
    }
}

int main(int argc, char **argv)
{
    ros::init(argc, argv, "landmark_publisher");
    ros::NodeHandle nh;
    g_nh = &nh;
    nh.param<bool>("/landmark_publisher_param/use_reflect_panel", use_reflect_panel, true);
    nh.param<bool>("/landmark_publisher_param/use_reflect_column", use_reflect_column, true);
    nh.param<double>("/landmark_publisher_param/intensity_threshold", intensity_threshold, 100000.0);
    nh.param<double>("/landmark_publisher_param/min_colum_points_per_cluster", min_colum_points_per_cluster, 100);
    nh.param<double>("/landmark_publisher_param/min_panel_points_per_cluster", min_panel_points_per_cluster, 200);
    nh.param<double>("/landmark_publisher_param/max_circle_fit_error", max_circle_fit_error, 0.2);
    nh.param<double>("/landmark_publisher_param/max_line_fit_error", max_line_fit_error, 4.0);
    nh.param<double>("/landmark_publisher_param/column_radius", column_radius, 5.0);
    nh.param<double>("/landmark_publisher_param/position_threshold", position_threshold, 1.0);
    nh.param<double>("/landmark_publisher_param/panel_length", panel_length, 0.2);
    nh.param<double>("/landmark_publisher_param/panel_length_noise", panel_length_noise, 0.05);
    nh.param<double>("/landmark_publisher_param/translation_weight", translation_weight, 50);
    nh.param<bool>("/landmark_publisher_param/debug_mode", debug_mode, false);
    nh.param<std::string>("/landmark_publisher_param/frame_id", frame_id, "base_link");
    nh.param<std::string>("/landmark_publisher_param/tracking_frame", tracking_frame, "base_link");

    // 初始化TF
    tf_buffer.reset(new tf2_ros::Buffer());
    tf_listener.reset(new tf2_ros::TransformListener(*tf_buffer));

    // subscribers
    auto sub_scan = nh.subscribe("/scan", 10, scanCallback);
    auto sub_param_changes = nh.subscribe<param_space::ParamChanges>("/param_changes", 10, param_changes_handler);
    auto sub_qr_pose = nh.subscribe("qr_pose", 10, DMcallback);
    // publishers
    landmark_pub = nh.advertise<cartographer_ros_msgs::LandmarkList>("landmark", 10);
    // service
    client_get_param = nh.serviceClient<param_space::GetParam>("/get_param");
    ros::spin();
    return 0;
}

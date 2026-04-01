/**
 * dock_tracker_node.cpp (Final Version)
 * * 功能：
 * 1. 坐标系: 自动将雷达点云转到 base_link (解决安装角度问题)
 * 2. 预处理: Z轴归零 (解决3D高度差), 宽视野 ROI (防止梯形被切)
 * 3. 初始猜测: 最近点定距离 + 重心定左右
 * 4. 模板修正: 侧翼向负X延伸 (解决180度反向匹配问题)
 * 5. 输出: 发布 /dock_pose 给控制层使用
 */

#include <ros/ros.h>
#include <sensor_msgs/LaserScan.h>
#include <sensor_msgs/PointCloud2.h>
#include <geometry_msgs/PoseStamped.h>
#include <pcl_conversions/pcl_conversions.h>
#include <pcl/point_cloud.h>
#include <pcl/point_types.h>
#include <pcl/registration/icp.h>
#include <pcl/filters/passthrough.h>
#include <pcl/common/centroid.h>
#include <tf/transform_listener.h>
#include <pcl_ros/transforms.h>
#include <tf/transform_datatypes.h>
#include <cmath>
#include <string>

typedef pcl::PointXYZ PointT;
typedef pcl::PointCloud<PointT> PointCloudT;

class DockTracker {
public:
    DockTracker()
        : pnh_("~")
    {
        pnh_.param<std::string>("scan_topic", scan_topic_, "/scan");
        pnh_.param<std::string>("base_frame", base_frame_, "base_link");
        pnh_.param<std::string>("dock_pose_topic", dock_pose_topic_, "/dock_pose");
        pnh_.param<std::string>("debug_cloud_topic", debug_cloud_topic_, "/dock_debug_cloud");
        pnh_.param<std::string>("roi_cloud_topic", roi_cloud_topic_, "/dock_roi_cloud");
        pnh_.param("roi_x_min", roi_x_min_, 0.3);
        pnh_.param("roi_x_max", roi_x_max_, 2.0);
        pnh_.param("roi_y_min", roi_y_min_, -1.0);
        pnh_.param("roi_y_max", roi_y_max_, 1.0);
        pnh_.param("tf_wait_timeout_s", tf_wait_timeout_s_, 0.1);
        pnh_.param("icp_max_corr_dist", icp_max_corr_dist_, 0.5);
        pnh_.param("icp_fitness_thresh", icp_fitness_thresh_, 0.00009);
        pnh_.param("angle_reject_abs_rad", angle_reject_abs_rad_, 1.0);

        // 1. 订阅
        sub_scan_ = nh_.subscribe(scan_topic_, 1, &DockTracker::scanCallback, this);

        // 2. 发布
        pub_dock_pose_ = nh_.advertise<geometry_msgs::PoseStamped>(dock_pose_topic_, 1);
        pub_debug_cloud_ = nh_.advertise<sensor_msgs::PointCloud2>(debug_cloud_topic_, 1);
        pub_roi_cloud_ = nh_.advertise<sensor_msgs::PointCloud2>(roi_cloud_topic_, 1);

        // 3. 生成模板
        template_cloud_ = generateTrapezoidTemplate();
        ROS_INFO("Dock Tracker Started. scan=%s dock_pose=%s base_frame=%s icp_fitness_thresh=%.5f",
                 scan_topic_.c_str(), dock_pose_topic_.c_str(), base_frame_.c_str(), icp_fitness_thresh_);
    }

    // 生成梯形模板
    // 【关键修正】: 让梯形开口朝向负X方向 (即朝向机器人)
    PointCloudT::Ptr generateTrapezoidTemplate() {
        PointCloudT::Ptr model(new PointCloudT());
        
        // === 您的参数 ===
        float back_wall_len = 0.16f;
        float side_wing_len = 0.13f;
        float angle_deg = 135.0f;
        float resolution = 0.005f;

        float wing_angle_rad = (180.0f - angle_deg) * M_PI / 180.0f; 
        float dx_wing = side_wing_len * sin(wing_angle_rad); 
        float dy_wing = side_wing_len * cos(wing_angle_rad); 

        // 1. 后壁 (中心在 0,0)
        int steps_back = std::ceil(back_wall_len / resolution);
        for (int i = 0; i <= steps_back; ++i) {
            float y = -back_wall_len / 2.0f + i * resolution;
            if (y > back_wall_len / 2.0f) y = back_wall_len / 2.0f;
            model->points.push_back(PointT(0.0f, y, 0.0f));
        }
        
        // 2. 侧翼
        int steps_wing = std::ceil(side_wing_len / resolution);
        for (int i = 1; i <= steps_wing; ++i) { 
            float ratio = (float)i * resolution / side_wing_len;
            if (ratio > 1.0f) ratio = 1.0f;
            
            // 【修正点】: 加负号!
            // 原来是正数，表示梯形开口背对机器人。
            // 现在改为负数，表示侧翼比后壁更靠近机器人 (符合真实雷达数据特征)
            float x = -1.0f * ratio * dx_wing; 
            float dy = ratio * dy_wing;
            
            // 左翼
            model->points.push_back(PointT(x, (back_wall_len / 2.0f) + dy, 0.0f));
            // 右翼
            model->points.push_back(PointT(x, -(back_wall_len / 2.0f) - dy, 0.0f));
        }
        
        model->width = model->points.size();
        model->height = 1;
        model->is_dense = true;
        model->header.frame_id = base_frame_;
        return model;
    }

    void scanCallback(const sensor_msgs::LaserScan::ConstPtr& scan_msg) {
        // 1. LaserScan -> PCL
        PointCloudT::Ptr raw_cloud(new PointCloudT);
        float angle = scan_msg->angle_min;
        for (size_t i = 0; i < scan_msg->ranges.size(); ++i) {
            float range = scan_msg->ranges[i];
            if (std::isfinite(range) && range >= scan_msg->range_min && range <= scan_msg->range_max) {
                float x = range * cos(angle);
                float y = range * sin(angle);
                raw_cloud->points.push_back(PointT(x, y, 0.0f)); 
            }
            angle += scan_msg->angle_increment;
        }
        raw_cloud->header.frame_id = scan_msg->header.frame_id;

        // 2. 坐标系变换 (转到 base_link)
        PointCloudT::Ptr base_cloud(new PointCloudT);
        try {
            if (!tf_listener_.waitForTransform(base_frame_, raw_cloud->header.frame_id,
                                               scan_msg->header.stamp, ros::Duration(tf_wait_timeout_s_))) {
                return;
            }
            pcl_ros::transformPointCloud(base_frame_, *raw_cloud, *base_cloud, tf_listener_);
        }
        catch (tf::TransformException& ex) {
            ROS_WARN_THROTTLE(2.0, "TF Error: %s", ex.what());
            return;
        }

        // 3. 强制Z轴归零 (扁平化)
        for (auto& pt : base_cloud->points) {
            pt.z = 0.0f; 
        }

        // 4. ROI 过滤 (宽视野)
        pcl::PassThrough<PointT> pass;
        pass.setInputCloud(base_cloud);
        
        // X: 0.4 ~ 3.0米
        pass.setFilterFieldName("x");
        pass.setFilterLimits(roi_x_min_, roi_x_max_);
        pass.filter(*base_cloud);
        
        // Y: -1.0 ~ 1.0米 (足够宽以包含完整梯形)
        pass.setFilterFieldName("y");
        pass.setFilterLimits(roi_y_min_, roi_y_max_);
        pass.filter(*base_cloud);

        // [调试] 发布ROI
        if (base_cloud->points.size() > 0) {
            sensor_msgs::PointCloud2 roi_msg;
            pcl::toROSMsg(*base_cloud, roi_msg);
            roi_msg.header.frame_id = base_frame_;
            roi_msg.header.stamp = ros::Time::now();
            pub_roi_cloud_.publish(roi_msg);
        }

        if (base_cloud->points.size() < 10) return; 

        // 5. 初始猜测 (Initial Guess)
        // 找最近点
        float min_x = 100.0;
        for (const auto& pt : base_cloud->points) {
            if (pt.x < min_x) min_x = pt.x;
        }
        // 找重心
        Eigen::Vector4f centroid;
        pcl::compute3DCentroid(*base_cloud, centroid);

        Eigen::Matrix4f initial_guess = Eigen::Matrix4f::Identity();
        // X = 最近点 + 0.09m (梯形深度补偿)
        initial_guess(0, 3) = min_x + 0.09; 
        // Y = 重心Y
        initial_guess(1, 3) = centroid[1];  

        // 6. ICP 配准
        pcl::IterativeClosestPoint<PointT, PointT> icp;
        icp.setInputSource(template_cloud_); // 模板
        icp.setInputTarget(base_cloud);      // 场景
        
        icp.setMaxCorrespondenceDistance(icp_max_corr_dist_);
        icp.setMaximumIterations(50);
        icp.setTransformationEpsilon(1e-8);
        icp.setEuclideanFitnessEpsilon(1e-5);

        PointCloudT Final;
        icp.align(Final, initial_guess); 

        // 7. 结果判定
        if (icp.hasConverged()) {
            double score = icp.getFitnessScore();
            Eigen::Matrix4f transform = icp.getFinalTransformation();
            
            float x = transform(0, 3);
            float y = transform(1, 3);
            float theta = atan2(transform(1, 0), transform(0, 0));

            // 阈值判断
            if (score < icp_fitness_thresh_) {
                // [安检] 角度合理性检查
                // 正常匹配应该是面对面 (0度左右)
                // 如果反向匹配 (180度左右)，说明没对准或者形状对称导致的误判
                if (std::abs(theta) > angle_reject_abs_rad_) {
                    ROS_WARN_THROTTLE(1.0, "Ignoring flipped match (Ang: %.1f deg)", theta * 180.0 / M_PI);
                    return; 
                }

                ROS_INFO("[Found] Score: %.5f | X: %.3f | Y: %.3f | Ang: %.1f", 
                         score, x, y, theta * 180.0 / M_PI);
                
                // 发布绿框
                sensor_msgs::PointCloud2 output_msg;
                pcl::toROSMsg(Final, output_msg);
                output_msg.header.frame_id = base_frame_;
                output_msg.header.stamp = ros::Time::now();
                pub_debug_cloud_.publish(output_msg);

                // 发布位姿
                geometry_msgs::PoseStamped pose_msg;
                pose_msg.header = output_msg.header;
                pose_msg.pose.position.x = x;
                pose_msg.pose.position.y = y;
                pose_msg.pose.position.z = 0.0;
                
                tf::Quaternion q;
                q.setRPY(0, 0, theta);
                pose_msg.pose.orientation.x = q.x();
                pose_msg.pose.orientation.y = q.y();
                pose_msg.pose.orientation.z = q.z();
                pose_msg.pose.orientation.w = q.w();
                
                pub_dock_pose_.publish(pose_msg);

            } else {
                ROS_INFO_THROTTLE(1.0, "Searching... Score: %.5f", score);
                
                // 调试：分数高时显示猜测位置
                sensor_msgs::PointCloud2 guess_msg;
                PointCloudT::Ptr guess_cloud(new PointCloudT);
                pcl::transformPointCloud(*template_cloud_, *guess_cloud, initial_guess);
                pcl::toROSMsg(*guess_cloud, guess_msg);
                guess_msg.header.frame_id = base_frame_;
                guess_msg.header.stamp = ros::Time::now();
                pub_debug_cloud_.publish(guess_msg);
            }
        } else {
            ROS_WARN_THROTTLE(1.0, "ICP failed to converge.");
        }
    }

private:
    ros::NodeHandle nh_;
    ros::NodeHandle pnh_;
    ros::Subscriber sub_scan_;
    ros::Publisher pub_debug_cloud_;
    ros::Publisher pub_roi_cloud_;
    ros::Publisher pub_dock_pose_;
    tf::TransformListener tf_listener_;
    PointCloudT::Ptr template_cloud_;
    std::string scan_topic_;
    std::string base_frame_;
    std::string dock_pose_topic_;
    std::string debug_cloud_topic_;
    std::string roi_cloud_topic_;
    double roi_x_min_;
    double roi_x_max_;
    double roi_y_min_;
    double roi_y_max_;
    double tf_wait_timeout_s_;
    double icp_max_corr_dist_;
    double icp_fitness_thresh_;
    double angle_reject_abs_rad_;
};

int main(int argc, char** argv) {
    ros::init(argc, argv, "dock_tracker_node");
    DockTracker tracker;
    ros::spin();
    return 0;
}

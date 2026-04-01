// ground_segmentation_2d_single.cpp
// -----------------------------------------------------------------------------
// 单路深度点云：TF→ROI→VoxelGrid→水平面分割(去地面, 平面低通+滞回)→SOR→ROR→投影到XY平面(z=0)
// 通过私有参数(~)配置 input_topic / output_topic / target_frame 等
// 适配：ROS Noetic | PCL 1.10
// -----------------------------------------------------------------------------

#include <ros/ros.h>
#include <sensor_msgs/PointCloud2.h>

#include <tf2_ros/buffer.h>
#include <tf2_ros/transform_listener.h>
#include <tf2_sensor_msgs/tf2_sensor_msgs.h>
#include <pcl_ros/transforms.h>
#include <pcl_conversions/pcl_conversions.h>

#include <pcl/point_types.h>
#include <pcl/filters/passthrough.h>
#include <pcl/filters/voxel_grid.h>
#include <pcl/filters/radius_outlier_removal.h>
#include <pcl/filters/statistical_outlier_removal.h>
#include <pcl/filters/extract_indices.h>
#include <pcl/segmentation/sac_segmentation.h>
#include <pcl/segmentation/extract_clusters.h>
#include <pcl/search/kdtree.h>

#include <Eigen/Dense>
#include <cmath>
#include <string>

using Cloud = pcl::PointCloud<pcl::PointXYZ>;

class Node {
public:
  Node() : tf_buf_(ros::Duration(10.0)), tf_listener_(tf_buf_) {
    ros::NodeHandle nh, p("~");

    // ---------------- 参数 ----------------
    p.param("input_topic",  input_topic_,  std::string("/camera/points"));
    p.param("output_topic", output_topic_, std::string("/obstacle_2d"));
    p.param("target_frame", target_frame_, std::string("base_link"));
    p.param("queue_size",   queue_size_,   5);

    // ROI
    p.param("x_min", x_min_, -1.2f);  p.param("x_max", x_max_,  1.5f);
    p.param("y_min", y_min_, -0.6f);  p.param("y_max", y_max_,  0.6f);
    p.param("z_min", z_min_, -5.0f);  p.param("z_max", z_max_,  5.0f);

    // 滤波 / 分割
    p.param("leaf", leaf_, 0.015f);                    // 体素叶大小（示例）
    p.param("ror_r", ror_r_, 0.020f);                  // ROR 半径（leaf≈0.015时）
    p.param("ror_n", ror_n_, 10);                      // ROR 邻居数
    p.param("mean_k", mean_k_, 50);                    // SOR K
    p.param("std_mul", std_mul_, 1.1f);                // SOR 阈值倍数

    float eps_deg; p.param("eps_angle_deg", eps_deg, 15.0f);    // 地面法向与Z轴夹角上限
    p.param("dist_thresh", dist_thresh_, 0.05f);                 // RANSAC 距离阈值(只用于拟合)
    p.param("ransac_max_iter", ransac_max_iter_, 200);

    // ★ 地面稳定化：平面低通 + 滞回双阈值（用于分类）
    p.param("plane_alpha", plane_alpha_, 0.1f);  // 0.05~0.2 越小越稳
    p.param("dist_lo",     dist_lo_,     0.03f);
    p.param("dist_hi",     dist_hi_,     0.05f);
    p.param("self_filter_enabled", self_filter_enabled_, true);
    p.param("self_x_min", self_x_min_, -0.65f);
    p.param("self_x_max", self_x_max_,  0.65f);
    p.param("self_y_min", self_y_min_, -0.55f);
    p.param("self_y_max", self_y_max_,  0.55f);
    p.param("cluster_filter_enabled", cluster_filter_enabled_, true);
    p.param("cluster_tolerance", cluster_tolerance_, 0.10f);
    p.param("min_cluster_size", min_cluster_size_, 12);
    p.param("max_cluster_size", max_cluster_size_, 10000);
    p.param("min_obstacle_points", min_obstacle_points_, 12);

    // ---------------- 订阅/发布 ----------------
    sub_ = nh.subscribe(input_topic_, queue_size_, &Node::cb, this);
    pub_obstacle_2d_ = nh.advertise<sensor_msgs::PointCloud2>(output_topic_, 1);

    // ---------------- 滤波/分割器配置 ----------------
    voxel_.setLeafSize(leaf_, leaf_, leaf_);

    sac_.setOptimizeCoefficients(true);
    sac_.setModelType(pcl::SACMODEL_PERPENDICULAR_PLANE);
    sac_.setMethodType(pcl::SAC_RANSAC);
    sac_.setAxis(Eigen::Vector3f::UnitZ()); // 地面法向接近+Z
    sac_.setEpsAngle(eps_deg * static_cast<float>(M_PI) / 180.f);
    sac_.setDistanceThreshold(dist_thresh_);
    sac_.setMaxIterations(ransac_max_iter_);

    sor_.setMeanK(mean_k_);
    sor_.setStddevMulThresh(std_mul_);

    ror_.setRadiusSearch(ror_r_);
    ror_.setMinNeighborsInRadius(ror_n_);

    ROS_INFO_STREAM("[gs2d_single] input=" << input_topic_
                    << ", output=" << output_topic_
                    << ", frame=" << target_frame_
                    << ", plane_alpha=" << plane_alpha_
                    << ", hyst=(" << dist_lo_ << "," << dist_hi_ << ")"
                    << ", SOR(K=" << mean_k_ << ", std=" << std_mul_ << ")"
                    << ", ROR(r=" << ror_r_ << ", n=" << ror_n_ << ")"
                    << ", self_filter=" << (self_filter_enabled_ ? "on" : "off")
                    << " box=[" << self_x_min_ << "," << self_x_max_
                    << "]x[" << self_y_min_ << "," << self_y_max_ << "]"
                    << ", cluster_filter=" << (cluster_filter_enabled_ ? "on" : "off")
                    << " tol=" << cluster_tolerance_
                    << " size=[" << min_cluster_size_ << "," << max_cluster_size_ << "]"
                    << ", min_obstacle_points=" << min_obstacle_points_);
  }

private:
  void cb(const sensor_msgs::PointCloud2ConstPtr& msg) {
    // 1) TF 到 target_frame
    sensor_msgs::PointCloud2 tf_pc;
    try {
      tf_buf_.transform(*msg, tf_pc, target_frame_, ros::Duration(0.1));
    } catch (const tf2::TransformException& e) {
      ROS_WARN_STREAM_THROTTLE(5.0, "TF: " << e.what());
      publish2D(Cloud::Ptr(new Cloud), msg->header.stamp);
      return;
    }

    // 2) ROS→PCL
    Cloud::Ptr raw(new Cloud);
    pcl::fromROSMsg(tf_pc, *raw);
    if (raw->empty()) { publish2D(Cloud::Ptr(new Cloud), msg->header.stamp); return; }

    // 3) ROI 裁剪：x → y → z
    Cloud::Ptr roi_x(new Cloud), roi_xy(new Cloud), roi_xyz(new Cloud);
    pcl::PassThrough<pcl::PointXYZ> pass;
    pass.setInputCloud(raw);
    pass.setFilterFieldName("x"); pass.setFilterLimits(x_min_, x_max_); pass.filter(*roi_x);
    pass.setInputCloud(roi_x);
    pass.setFilterFieldName("y"); pass.setFilterLimits(y_min_, y_max_); pass.filter(*roi_xy);
    pass.setInputCloud(roi_xy);
    pass.setFilterFieldName("z"); pass.setFilterLimits(z_min_, z_max_); pass.filter(*roi_xyz);
    if (roi_xyz->empty()) { publish2D(Cloud::Ptr(new Cloud), msg->header.stamp); return; }

    // 4) 体素降采样
    Cloud::Ptr ds(new Cloud);
    voxel_.setInputCloud(roi_xyz);
    voxel_.filter(*ds);
    if (ds->empty()) { publish2D(Cloud::Ptr(new Cloud), msg->header.stamp); return; }

    // 5) 地面分割（★ 稳定化：平面低通 + 滞回双阈值）
    Cloud::Ptr obst(new Cloud);
    if (!segmentGroundStable(ds, obst)) { publish2D(Cloud::Ptr(new Cloud), msg->header.stamp); return; }
    if (obst->empty()) { publish2D(Cloud::Ptr(new Cloud), msg->header.stamp); return; }

    // 6) SOR → 7) ROR
    Cloud::Ptr sor_out(new Cloud);
    sor_.setInputCloud(obst);
    sor_.filter(*sor_out);
    if (sor_out->empty()) { publish2D(Cloud::Ptr(new Cloud), msg->header.stamp); return; }

    Cloud::Ptr clean(new Cloud);
    ror_.setInputCloud(sor_out);
    ror_.filter(*clean);
    if (clean->empty()) { publish2D(Cloud::Ptr(new Cloud), msg->header.stamp); return; }

    // 过滤机器人本体附近残留点，减少自体/近地噪点误检
    Cloud::Ptr filtered(new Cloud);
    filterSelfPoints(clean, filtered);
    if (filtered->size() < static_cast<size_t>(min_obstacle_points_)) {
      publish2D(Cloud::Ptr(new Cloud), msg->header.stamp);
      return;
    }

    // 仅保留规模足够的稠密障碍簇，抑制空地散点误检
    Cloud::Ptr clustered(new Cloud);
    clusterObstacles(filtered, clustered);
    if (clustered->size() < static_cast<size_t>(min_obstacle_points_)) {
      publish2D(Cloud::Ptr(new Cloud), msg->header.stamp);
      return;
    }

    // 8) 3D→2D 投影（z=0）并发布
    Cloud::Ptr proj2d(new Cloud);
    proj2d->reserve(clustered->size());
    for (const auto& p : clustered->points) proj2d->points.emplace_back(p.x, p.y, 0.0f);
    publish2D(proj2d, msg->header.stamp);
  }

  // ---------- 平面稳定化分割：RANSAC观测 → IIR平滑 → 双阈值分类 ----------
  static inline void normalizePlane(Eigen::Vector3f& n, float& d) {
    float s = n.norm(); if (s == 0) { n = {0,0,1}; d = 0; return; }
    n /= s; d /= s;
  }

  bool segmentGroundStable(const Cloud::Ptr& in, Cloud::Ptr& obst_out) {
    pcl::PointIndices::Ptr inliers(new pcl::PointIndices);
    pcl::ModelCoefficients::Ptr coef(new pcl::ModelCoefficients);
    sac_.setInputCloud(in);
    sac_.segment(*inliers, *coef);
    if (coef->values.size() < 4) return false;

    // 本帧观测
    Eigen::Vector3f n_t(coef->values[0], coef->values[1], coef->values[2]);
    float d_t = coef->values[3];
    normalizePlane(n_t, d_t);

    // 方向一致
    if (plane_inited_ && plane_n_.dot(n_t) < 0) { n_t = -n_t; d_t = -d_t; }

    // 指数平滑
    if (!plane_inited_) { plane_n_ = n_t; plane_d_ = d_t; plane_inited_ = true; }
    else {
      plane_n_ = (1.0f - plane_alpha_) * plane_n_ + plane_alpha_ * n_t;
      plane_n_.normalize();
      plane_d_ = (1.0f - plane_alpha_) * plane_d_ + plane_alpha_ * d_t;
    }

    // 双阈值分类：<dist_lo 为地面，>dist_hi 为障碍，中间带忽略
    obst_out.reset(new Cloud);
    obst_out->points.reserve(in->size());
    for (const auto& p : in->points) {
      float dist = std::abs(plane_n_.dot(Eigen::Vector3f(p.x,p.y,p.z)) + plane_d_);
      if (dist > dist_hi_) obst_out->points.push_back(p);
      // else if (dist < dist_lo_) -> ground (忽略)
      // else -> 中间带忽略
    }
    return true;
  }

  void filterSelfPoints(const Cloud::Ptr& in, Cloud::Ptr& out) const {
    out.reset(new Cloud);
    out->points.reserve(in->size());
    for (const auto& p : in->points) {
      if (self_filter_enabled_ &&
          p.x >= self_x_min_ && p.x <= self_x_max_ &&
          p.y >= self_y_min_ && p.y <= self_y_max_) {
        continue;
      }
      out->points.push_back(p);
    }
  }

  void clusterObstacles(const Cloud::Ptr& in, Cloud::Ptr& out) const {
    if (!cluster_filter_enabled_) {
      out = in;
      return;
    }

    out.reset(new Cloud);
    if (in->size() < static_cast<size_t>(min_cluster_size_)) return;

    pcl::search::KdTree<pcl::PointXYZ>::Ptr tree(new pcl::search::KdTree<pcl::PointXYZ>);
    tree->setInputCloud(in);

    std::vector<pcl::PointIndices> cluster_indices;
    pcl::EuclideanClusterExtraction<pcl::PointXYZ> ec;
    ec.setClusterTolerance(cluster_tolerance_);
    ec.setMinClusterSize(min_cluster_size_);
    ec.setMaxClusterSize(max_cluster_size_);
    ec.setSearchMethod(tree);
    ec.setInputCloud(in);
    ec.extract(cluster_indices);

    for (const auto& indices : cluster_indices) {
      for (const int idx : indices.indices) {
        out->points.push_back(in->points[idx]);
      }
    }
  }

  void publish2D(const Cloud::Ptr& c, const ros::Time& stamp) {
    sensor_msgs::PointCloud2 out;
    pcl::toROSMsg(*c, out);
    out.header.frame_id = target_frame_;
    out.header.stamp = stamp;
    pub_obstacle_2d_.publish(out);
  }

private:
  // ROS
  ros::Subscriber sub_;
  ros::Publisher  pub_obstacle_2d_;
  tf2_ros::Buffer tf_buf_;
  tf2_ros::TransformListener tf_listener_;

  // 过滤/分割器
  pcl::VoxelGrid<pcl::PointXYZ> voxel_;
  pcl::RadiusOutlierRemoval<pcl::PointXYZ> ror_;
  pcl::StatisticalOutlierRemoval<pcl::PointXYZ> sor_;
  pcl::SACSegmentation<pcl::PointXYZ> sac_;

  // 参数
  std::string input_topic_, output_topic_, target_frame_;
  int queue_size_;
  float x_min_, x_max_, y_min_, y_max_, z_min_, z_max_;
  float leaf_, ror_r_, dist_thresh_, std_mul_;
  int ror_n_, ransac_max_iter_, mean_k_;

  // ★ 地面稳定化
  bool  plane_inited_{false};
  Eigen::Vector3f plane_n_{0,0,1};
  float plane_d_{0.0f};
  float plane_alpha_;
  float dist_lo_, dist_hi_;
  bool self_filter_enabled_;
  float self_x_min_, self_x_max_, self_y_min_, self_y_max_;
  bool cluster_filter_enabled_;
  float cluster_tolerance_;
  int min_cluster_size_, max_cluster_size_;
  int min_obstacle_points_;
};

int main(int argc, char** argv) {
  ros::init(argc, argv, "ground_segmentation_2d_single");
  Node n;
  ros::spin();
  return 0;
}


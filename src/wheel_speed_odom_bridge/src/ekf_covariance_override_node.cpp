#include <algorithm>
#include <array>
#include <cmath>
#include <sstream>
#include <string>
#include <vector>

#include <ros/ros.h>
#include <sensor_msgs/Imu.h>
#include <xmlrpcpp/XmlRpcValue.h>

namespace {

bool XmlRpcNumberToDouble(const XmlRpc::XmlRpcValue& value, double* result) {
  if (result == nullptr) {
    return false;
  }

  if (value.getType() == XmlRpc::XmlRpcValue::TypeInt) {
    *result = static_cast<int>(value);
    return true;
  }
  if (value.getType() == XmlRpc::XmlRpcValue::TypeDouble) {
    *result = static_cast<double>(value);
    return true;
  }
  return false;
}

bool LoadFixedSizeDoubleArrayParam(const ros::NodeHandle& nh, const std::string& name,
                                   std::vector<double>* values) {
  if (values == nullptr) {
    return false;
  }

  XmlRpc::XmlRpcValue raw_value;
  if (!nh.getParam(name, raw_value)) {
    return false;
  }
  if (raw_value.getType() != XmlRpc::XmlRpcValue::TypeArray ||
      raw_value.size() != static_cast<int>(values->size())) {
    ROS_WARN_STREAM("Parameter " << name << " must be an array with " << values->size()
                                 << " numeric entries. Keep the current default.");
    return false;
  }

  std::vector<double> parsed(values->size(), 0.0);
  for (int i = 0; i < raw_value.size(); ++i) {
    if (!XmlRpcNumberToDouble(raw_value[i], &parsed[static_cast<size_t>(i)])) {
      ROS_WARN_STREAM("Parameter " << name << "[" << i
                                   << "] must be numeric. Keep the current default.");
      return false;
    }
  }

  *values = parsed;
  return true;
}

std::array<double, 9> Diagonal3ToCovariance9(const std::vector<double>& diagonal) {
  std::array<double, 9> covariance{};
  covariance.fill(0.0);
  const size_t limit = std::min<size_t>(3, diagonal.size());
  for (size_t i = 0; i < limit; ++i) {
    covariance[i * 3 + i] = diagonal[i];
  }
  return covariance;
}

std::string FormatVector(const std::vector<double>& values) {
  std::ostringstream oss;
  oss << "[";
  for (size_t i = 0; i < values.size(); ++i) {
    if (i > 0) {
      oss << ", ";
    }
    oss << values[i];
  }
  oss << "]";
  return oss.str();
}

std::string FormatArray3(const std::array<double, 3>& values) {
  std::ostringstream oss;
  oss << "[" << values[0] << ", " << values[1] << ", " << values[2] << "]";
  return oss.str();
}

std::array<double, 3> VectorToArray3(const std::vector<double>& values) {
  std::array<double, 3> result{};
  result.fill(0.0);
  const size_t limit = std::min<size_t>(3, values.size());
  for (size_t i = 0; i < limit; ++i) {
    result[i] = values[i];
  }
  return result;
}

double Vector3Norm(double x, double y, double z) {
  return std::sqrt(x * x + y * y + z * z);
}

}  // namespace

class EkfCovarianceOverrideNode {
 public:
  EkfCovarianceOverrideNode() : nh_(), pnh_("~") {
    pnh_.param<std::string>("input_imu_topic", input_imu_topic_, "/imu");
    pnh_.param<std::string>("output_imu_topic", output_imu_topic_, "/imu_for_ekf");
    pnh_.param<int>("imu_queue_size", imu_queue_size_, 100);
    pnh_.param<bool>("override_imu_orientation_covariance",
                     override_imu_orientation_covariance_, false);
    pnh_.param<bool>("override_imu_angular_velocity_covariance",
                     override_imu_angular_velocity_covariance_, true);
    pnh_.param<bool>("override_imu_linear_acceleration_covariance",
                     override_imu_linear_acceleration_covariance_, false);
    pnh_.param<bool>("enable_imu_angular_velocity_bias_compensation",
                     enable_imu_angular_velocity_bias_compensation_, false);
    pnh_.param<bool>("estimate_imu_angular_velocity_bias_on_startup",
                     estimate_imu_angular_velocity_bias_on_startup_, false);
    pnh_.param<double>("imu_angular_velocity_bias_estimation_duration_sec",
                       imu_angular_velocity_bias_estimation_duration_sec_, 5.0);
    pnh_.param<double>("imu_angular_velocity_bias_stationary_threshold_rad_s",
                       imu_angular_velocity_bias_stationary_threshold_rad_s_, 0.02);
    pnh_.param<int>("imu_angular_velocity_bias_min_samples",
                    imu_angular_velocity_bias_min_samples_, 200);

    LoadFixedSizeDoubleArrayParam(pnh_, "imu_orientation_covariance_diagonal",
                                  &imu_orientation_covariance_diagonal_);
    LoadFixedSizeDoubleArrayParam(pnh_, "imu_angular_velocity_covariance_diagonal",
                                  &imu_angular_velocity_covariance_diagonal_);
    LoadFixedSizeDoubleArrayParam(pnh_, "imu_linear_acceleration_covariance_diagonal",
                                  &imu_linear_acceleration_covariance_diagonal_);
    LoadFixedSizeDoubleArrayParam(pnh_, "imu_angular_velocity_bias",
                                  &imu_angular_velocity_bias_);

    imu_orientation_covariance_ = Diagonal3ToCovariance9(imu_orientation_covariance_diagonal_);
    imu_angular_velocity_covariance_ =
        Diagonal3ToCovariance9(imu_angular_velocity_covariance_diagonal_);
    imu_linear_acceleration_covariance_ =
        Diagonal3ToCovariance9(imu_linear_acceleration_covariance_diagonal_);
    configured_imu_angular_velocity_bias_ = VectorToArray3(imu_angular_velocity_bias_);
    applied_imu_angular_velocity_bias_ = configured_imu_angular_velocity_bias_;

    imu_queue_size_ = std::max(1, imu_queue_size_);
    imu_angular_velocity_bias_estimation_duration_sec_ =
        std::max(0.0, imu_angular_velocity_bias_estimation_duration_sec_);
    imu_angular_velocity_bias_stationary_threshold_rad_s_ =
        std::max(0.0, imu_angular_velocity_bias_stationary_threshold_rad_s_);
    imu_angular_velocity_bias_min_samples_ = std::max(1, imu_angular_velocity_bias_min_samples_);

    if (input_imu_topic_ == output_imu_topic_) {
      ROS_FATAL_STREAM("input_imu_topic and output_imu_topic must be different to avoid loops: "
                       << input_imu_topic_);
      ros::shutdown();
      return;
    }

    imu_pub_ = nh_.advertise<sensor_msgs::Imu>(output_imu_topic_, imu_queue_size_);
    imu_sub_ = nh_.subscribe(input_imu_topic_, imu_queue_size_,
                             &EkfCovarianceOverrideNode::ImuCallback, this);

    ROS_INFO_STREAM("EKF covariance override imu: " << input_imu_topic_ << " -> "
                                                    << output_imu_topic_);
    if (override_imu_orientation_covariance_) {
      ROS_INFO_STREAM("imu orientation covariance diagonal: "
                      << FormatVector(imu_orientation_covariance_diagonal_));
    }
    if (override_imu_angular_velocity_covariance_) {
      ROS_INFO_STREAM("imu angular velocity covariance diagonal: "
                      << FormatVector(imu_angular_velocity_covariance_diagonal_));
    }
    if (override_imu_linear_acceleration_covariance_) {
      ROS_INFO_STREAM("imu linear acceleration covariance diagonal: "
                      << FormatVector(imu_linear_acceleration_covariance_diagonal_));
    }
    if (enable_imu_angular_velocity_bias_compensation_) {
      ROS_INFO_STREAM("imu angular velocity bias compensation enabled with configured bias "
                      << FormatArray3(configured_imu_angular_velocity_bias_));
      if (estimate_imu_angular_velocity_bias_on_startup_) {
        ROS_INFO_STREAM("startup imu bias estimation enabled: duration_sec="
                        << imu_angular_velocity_bias_estimation_duration_sec_
                        << ", stationary_threshold_rad_s="
                        << imu_angular_velocity_bias_stationary_threshold_rad_s_
                        << ", min_samples=" << imu_angular_velocity_bias_min_samples_);
      }
    }
  }

 private:
  ros::Time ResolveSampleStamp(const sensor_msgs::Imu& msg) const {
    if (!msg.header.stamp.isZero()) {
      return msg.header.stamp;
    }
    return ros::Time::now();
  }

  void FinalizeAngularVelocityBiasEstimate() {
    if (angular_velocity_bias_estimation_completed_) {
      return;
    }

    angular_velocity_bias_estimation_completed_ = true;
    if (angular_velocity_bias_estimation_sample_count_ < imu_angular_velocity_bias_min_samples_) {
      ROS_WARN_STREAM("imu angular velocity startup bias estimation collected only "
                      << angular_velocity_bias_estimation_sample_count_
                      << " stationary samples, below min_samples="
                      << imu_angular_velocity_bias_min_samples_
                      << ". Keep configured bias "
                      << FormatArray3(configured_imu_angular_velocity_bias_) << ".");
      applied_imu_angular_velocity_bias_ = configured_imu_angular_velocity_bias_;
      return;
    }

    for (size_t i = 0; i < applied_imu_angular_velocity_bias_.size(); ++i) {
      applied_imu_angular_velocity_bias_[i] =
          angular_velocity_bias_sum_[i] /
          static_cast<double>(angular_velocity_bias_estimation_sample_count_);
    }
    ROS_INFO_STREAM("imu angular velocity startup bias estimated from "
                    << angular_velocity_bias_estimation_sample_count_
                    << " stationary samples: "
                    << FormatArray3(applied_imu_angular_velocity_bias_));
  }

  void UpdateAngularVelocityBiasEstimate(const sensor_msgs::Imu& msg) {
    if (!enable_imu_angular_velocity_bias_compensation_ ||
        !estimate_imu_angular_velocity_bias_on_startup_ ||
        angular_velocity_bias_estimation_completed_) {
      return;
    }

    const ros::Time sample_stamp = ResolveSampleStamp(msg);
    if (!angular_velocity_bias_estimation_started_) {
      angular_velocity_bias_estimation_started_ = true;
      angular_velocity_bias_estimation_start_stamp_ = sample_stamp;
    }

    const double angular_velocity_norm =
        Vector3Norm(msg.angular_velocity.x, msg.angular_velocity.y, msg.angular_velocity.z);
    if (angular_velocity_norm <= imu_angular_velocity_bias_stationary_threshold_rad_s_) {
      angular_velocity_bias_sum_[0] += msg.angular_velocity.x;
      angular_velocity_bias_sum_[1] += msg.angular_velocity.y;
      angular_velocity_bias_sum_[2] += msg.angular_velocity.z;
      ++angular_velocity_bias_estimation_sample_count_;
    }

    if ((sample_stamp - angular_velocity_bias_estimation_start_stamp_).toSec() >=
        imu_angular_velocity_bias_estimation_duration_sec_) {
      FinalizeAngularVelocityBiasEstimate();
    }
  }

  void ImuCallback(const sensor_msgs::Imu::ConstPtr& msg) {
    if (msg == nullptr) {
      return;
    }

    sensor_msgs::Imu output = *msg;
    UpdateAngularVelocityBiasEstimate(output);
    if (enable_imu_angular_velocity_bias_compensation_) {
      output.angular_velocity.x -= applied_imu_angular_velocity_bias_[0];
      output.angular_velocity.y -= applied_imu_angular_velocity_bias_[1];
      output.angular_velocity.z -= applied_imu_angular_velocity_bias_[2];
    }
    if (override_imu_orientation_covariance_) {
      std::copy(imu_orientation_covariance_.begin(), imu_orientation_covariance_.end(),
                output.orientation_covariance.begin());
    }
    if (override_imu_angular_velocity_covariance_) {
      std::copy(imu_angular_velocity_covariance_.begin(),
                imu_angular_velocity_covariance_.end(),
                output.angular_velocity_covariance.begin());
    }
    if (override_imu_linear_acceleration_covariance_) {
      std::copy(imu_linear_acceleration_covariance_.begin(),
                imu_linear_acceleration_covariance_.end(),
                output.linear_acceleration_covariance.begin());
    }
    imu_pub_.publish(output);
  }

  ros::NodeHandle nh_;
  ros::NodeHandle pnh_;
  ros::Publisher imu_pub_;
  ros::Subscriber imu_sub_;

  std::string input_imu_topic_ = "/imu";
  std::string output_imu_topic_ = "/imu_for_ekf";
  int imu_queue_size_ = 100;

  bool override_imu_orientation_covariance_ = false;
  bool override_imu_angular_velocity_covariance_ = true;
  bool override_imu_linear_acceleration_covariance_ = false;
  bool enable_imu_angular_velocity_bias_compensation_ = false;
  bool estimate_imu_angular_velocity_bias_on_startup_ = false;
  double imu_angular_velocity_bias_estimation_duration_sec_ = 5.0;
  double imu_angular_velocity_bias_stationary_threshold_rad_s_ = 0.02;
  int imu_angular_velocity_bias_min_samples_ = 200;

  std::vector<double> imu_orientation_covariance_diagonal_ = {1000000.0, 1000000.0, 0.1};
  std::vector<double> imu_angular_velocity_covariance_diagonal_ = {1000000.0, 1000000.0, 0.05};
  std::vector<double> imu_linear_acceleration_covariance_diagonal_ = {1000000.0, 1000000.0,
                                                                       1000000.0};
  std::vector<double> imu_angular_velocity_bias_ = {0.0, 0.0, 0.0};

  std::array<double, 9> imu_orientation_covariance_{};
  std::array<double, 9> imu_angular_velocity_covariance_{};
  std::array<double, 9> imu_linear_acceleration_covariance_{};
  std::array<double, 3> configured_imu_angular_velocity_bias_{};
  std::array<double, 3> applied_imu_angular_velocity_bias_{};
  std::array<double, 3> angular_velocity_bias_sum_{};
  ros::Time angular_velocity_bias_estimation_start_stamp_;
  bool angular_velocity_bias_estimation_started_ = false;
  bool angular_velocity_bias_estimation_completed_ = false;
  int angular_velocity_bias_estimation_sample_count_ = 0;
};

int main(int argc, char** argv) {
  ros::init(argc, argv, "ekf_covariance_override");
  EkfCovarianceOverrideNode node;
  ros::spin();
  return 0;
}

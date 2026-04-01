#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <termios.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <limits>
#include <sstream>
#include <string>
#include <vector>

#include <geometry_msgs/TransformStamped.h>
#include <nav_msgs/Odometry.h>
#include <ros/package.h>
#include <ros/ros.h>
#include <sensor_msgs/Imu.h>
#include <tf2_ros/transform_broadcaster.h>
#include <xmlrpcpp/XmlRpcValue.h>

namespace {

constexpr uint8_t kHeader0 = 0x43;
constexpr uint8_t kHeader1 = 0x4C;
constexpr uint8_t kTail = 0xDA;
constexpr uint8_t kAltTail = 0x3B;
constexpr uint8_t kEscape = 0x5C;
constexpr uint8_t kCmd0 = 0x40;
constexpr uint8_t kCmdData = 0x01;
constexpr uint8_t kCmdHeartbeat = 0x05;
constexpr size_t kMinFrameBytes = 8;
constexpr size_t kMaxLengthField = 4096;
constexpr size_t kMaxBufferBytes = 32768;
constexpr double kPi = 3.14159265358979323846;

constexpr size_t kAbsoluteTimeOffset = 0;
constexpr size_t kLeftWheelTimeOffset = 4;
constexpr size_t kRightWheelTimeOffset = 8;
constexpr size_t kLeftEncoderOffset = 12;
constexpr size_t kRightEncoderOffset = 16;
constexpr size_t kWheelSpeedField0Offset = 20;
constexpr size_t kWheelSpeedField1Offset = 24;
constexpr size_t kDiagnosticDataBytes = 20;
constexpr size_t kWheelSpeedDataBytes = 28;
constexpr double kTimeJumpWarnSec = 10.0;
constexpr double kMinOdomStampStepSec = 1e-6;

enum class OdomTransportType {
  kTcp,
  kSerial,
};

enum class OdomPublishMode {
  kOnFrame,
  kFixedRate,
};

enum class WheelSpeedFieldOrder {
  kLeftRight,
  kRightLeft,
};

enum class OdomTimeSource {
  kWheelTime,
  kAbsoluteTime,
};

enum class WheelTimeMode {
  kAverage,
  kLeft,
  kRight,
};

uint32_t ReadUInt32Le(const uint8_t* p) {
  return static_cast<uint32_t>(p[0]) |
         (static_cast<uint32_t>(p[1]) << 8) |
         (static_cast<uint32_t>(p[2]) << 16) |
         (static_cast<uint32_t>(p[3]) << 24);
}

float ReadFloat32Le(const uint8_t* p) {
  const uint32_t raw = ReadUInt32Le(p);
  float value = 0.0f;
  std::memcpy(&value, &raw, sizeof(value));
  return value;
}

speed_t ToTermiosBaud(int baudrate) {
  switch (baudrate) {
    case 1200:
      return B1200;
    case 2400:
      return B2400;
    case 4800:
      return B4800;
    case 9600:
      return B9600;
    case 19200:
      return B19200;
    case 38400:
      return B38400;
    case 57600:
      return B57600;
    case 115200:
      return B115200;
    case 230400:
      return B230400;
    default:
      return static_cast<speed_t>(0);
  }
}

std::string ToLowerCopy(const std::string& value) {
  std::string normalized = value;
  std::transform(normalized.begin(), normalized.end(), normalized.begin(),
                 [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  return normalized;
}

bool IsAbsolutePath(const std::string& path) {
  if (path.empty()) {
    return false;
  }
  if (path[0] == '/' || path[0] == '\\') {
    return true;
  }
  return path.size() > 1 && path[1] == ':';
}

std::string GetWorkspaceRootPath() {
  const std::string package_path = ros::package::getPath("wheel_speed_odom_bridge");
  if (package_path.empty()) {
    return std::string();
  }
  return package_path + "/../..";
}

double NormalizeAngle(double angle) {
  while (angle > kPi) {
    angle -= 2.0 * kPi;
  }
  while (angle < -kPi) {
    angle += 2.0 * kPi;
  }
  return angle;
}

bool IsNativeOdomDataCommand(uint8_t cmd0, uint8_t cmd1) {
  return cmd0 == kCmd0 && cmd1 == kCmdData;
}

uint32_t ComputeUInt32ForwardDelta(uint32_t current, uint32_t previous) {
  return current - previous;
}

int64_t ComputeUInt32SignedDelta(uint32_t current, uint32_t previous) {
  int64_t delta = static_cast<int64_t>(current) - static_cast<int64_t>(previous);
  const int64_t half_range = 1LL << 31;
  const int64_t full_range = 1LL << 32;
  if (delta > half_range) {
    delta -= full_range;
  } else if (delta < -half_range) {
    delta += full_range;
  }
  return delta;
}

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

bool ShouldPublishTfForTopic(const ros::NodeHandle& nh, const std::string& topic_name) {
  return nh.resolveName(topic_name) == "/odom";
}

std::array<double, 36> Diagonal6ToCovariance36(const std::vector<double>& diagonal) {
  std::array<double, 36> covariance{};
  covariance.fill(0.0);
  const size_t limit = std::min<size_t>(6, diagonal.size());
  for (size_t i = 0; i < limit; ++i) {
    covariance[i * 6 + i] = diagonal[i];
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

}  // namespace

class WheelSpeedOdomNode {
 public:
  WheelSpeedOdomNode() : nh_(), pnh_("~") {
    pnh_.param<std::string>("transport_type", transport_type_name_, "tcp");
    pnh_.param<std::string>("server_ip", server_ip_, "192.168.127.10");
    pnh_.param<int>("server_port", server_port_, 5001);
    pnh_.param<std::string>("serial_device", serial_device_, "/dev/odom");
    pnh_.param<int>("serial_baudrate", serial_baudrate_, 115200);
    pnh_.param<std::string>("publish_topic", publish_topic_, "/odom");
    pnh_.param<std::string>("frame_id", frame_id_, "odom");
    pnh_.param<std::string>("child_frame_id", child_frame_id_, "base_footprint");
    pnh_.param<bool>("publish_odom_tf", publish_odom_tf_, true);
    pnh_.param<bool>("enable_odom_rx", enable_odom_rx_, true);
    pnh_.param<bool>("enable_odom_publish", enable_odom_publish_, true);
    pnh_.param<std::string>("publish_mode", publish_mode_name_, "fixed_rate");
    pnh_.param<double>("publish_rate_hz", publish_rate_hz_, 50.0);
    pnh_.param<double>("stale_timeout_sec", stale_timeout_sec_, 0.2);
    pnh_.param<int>("max_repeat_publish_count", max_repeat_publish_count_, 10);
    pnh_.param<double>("reconnect_interval_sec", reconnect_interval_sec_, 1.0);
    pnh_.param<double>("warn_interval_sec", warn_interval_sec_, 5.0);
    pnh_.param<double>("loop_rate_hz", loop_rate_hz_, 100.0);
    pnh_.param<bool>("enable_imu_diagnostic", enable_imu_diagnostic_, true);
    pnh_.param<std::string>("imu_topic", imu_topic_, "/imu");

    pnh_.param<std::string>("wheel_speed_field_order", wheel_speed_field_order_name_, "left_right");
    pnh_.param<double>("wheel_speed_unit_to_mps", wheel_speed_unit_to_mps_, 0.001);
    pnh_.param<double>("left_wheel_speed_scale", left_wheel_speed_scale_, 1.0);
    pnh_.param<double>("right_wheel_speed_scale", right_wheel_speed_scale_, 1.0);
    pnh_.param<double>("wheel_separation", wheel_separation_, 0.725);
    pnh_.param<std::string>("odom_time_source", odom_time_source_name_, "wheel_time");
    pnh_.param<std::string>("wheel_time_mode", wheel_time_mode_name_, "average");
    pnh_.param<double>("wheel_time_unit_to_sec", wheel_time_unit_to_sec_, 0.001);
    pnh_.param<double>("absolute_time_unit_to_sec", absolute_time_unit_to_sec_, 0.001);
    pnh_.param<double>("min_valid_odom_dt_sec", min_valid_odom_dt_sec_, 0.005);
    pnh_.param<double>("max_valid_odom_dt_sec", max_valid_odom_dt_sec_, 0.06);
    pnh_.param<double>("max_abs_linear_speed", max_abs_linear_speed_, 0.0);
    pnh_.param<double>("max_abs_angular_speed", max_abs_angular_speed_, 0.0);
    pnh_.param<double>("max_abs_linear_accel", max_abs_linear_accel_, 3.0);
    pnh_.param<double>("max_abs_angular_accel", max_abs_angular_accel_, 6.0);

    pnh_.param<bool>("enable_diagnostic_log", enable_diagnostic_log_, true);
    pnh_.param<std::string>("diagnostic_log_path", diagnostic_log_path_,
                            "wheel_speed_odom_debug.csv");
    LoadFixedSizeDoubleArrayParam(pnh_, "odom_pose_covariance_diagonal",
                                  &odom_pose_covariance_diagonal_);
    LoadFixedSizeDoubleArrayParam(pnh_, "odom_twist_covariance_diagonal",
                                  &odom_twist_covariance_diagonal_);
    odom_pose_covariance_ = Diagonal6ToCovariance36(odom_pose_covariance_diagonal_);
    odom_twist_covariance_ = Diagonal6ToCovariance36(odom_twist_covariance_diagonal_);
    resolved_publish_topic_ = nh_.resolveName(publish_topic_);
    if (publish_odom_tf_ && !ShouldPublishTfForTopic(nh_, publish_topic_)) {
      ROS_WARN_STREAM("publish_odom_tf requested for topic " << resolved_publish_topic_
                      << ", but only /odom is allowed to publish TF. Disable odom TF.");
      publish_odom_tf_ = false;
    }

    transport_type_name_ = ToLowerCopy(transport_type_name_);
    if (transport_type_name_ == "serial") {
      transport_type_ = OdomTransportType::kSerial;
    } else {
      if (transport_type_name_ != "tcp") {
        ROS_WARN_STREAM("Invalid parameter: transport_type must be tcp or serial. Fallback to tcp.");
      }
      transport_type_name_ = "tcp";
      transport_type_ = OdomTransportType::kTcp;
    }

    publish_mode_name_ = ToLowerCopy(publish_mode_name_);
    if (publish_mode_name_ == "on_frame") {
      publish_mode_ = OdomPublishMode::kOnFrame;
    } else {
      if (publish_mode_name_ != "fixed_rate") {
        ROS_WARN_STREAM(
            "Invalid parameter: publish_mode must be on_frame or fixed_rate. Fallback to "
            "fixed_rate.");
      }
      publish_mode_name_ = "fixed_rate";
      publish_mode_ = OdomPublishMode::kFixedRate;
    }

    wheel_speed_field_order_name_ = ToLowerCopy(wheel_speed_field_order_name_);
    if (wheel_speed_field_order_name_ == "right_left") {
      wheel_speed_field_order_ = WheelSpeedFieldOrder::kRightLeft;
    } else {
      if (wheel_speed_field_order_name_ != "left_right") {
        ROS_WARN_STREAM("Invalid parameter: wheel_speed_field_order must be left_right or "
                        "right_left. Fallback to left_right.");
      }
      wheel_speed_field_order_name_ = "left_right";
      wheel_speed_field_order_ = WheelSpeedFieldOrder::kLeftRight;
    }

    odom_time_source_name_ = ToLowerCopy(odom_time_source_name_);
    if (odom_time_source_name_ == "absolute_time") {
      odom_time_source_ = OdomTimeSource::kAbsoluteTime;
    } else {
      if (odom_time_source_name_ != "wheel_time") {
        ROS_WARN_STREAM("Invalid parameter: odom_time_source must be wheel_time or "
                        "absolute_time. Fallback to wheel_time.");
      }
      odom_time_source_name_ = "wheel_time";
      odom_time_source_ = OdomTimeSource::kWheelTime;
    }

    wheel_time_mode_name_ = ToLowerCopy(wheel_time_mode_name_);
    if (wheel_time_mode_name_ == "left") {
      wheel_time_mode_ = WheelTimeMode::kLeft;
    } else if (wheel_time_mode_name_ == "right") {
      wheel_time_mode_ = WheelTimeMode::kRight;
    } else {
      if (wheel_time_mode_name_ != "average") {
        ROS_WARN_STREAM("Invalid parameter: wheel_time_mode must be average, left or "
                        "right. Fallback to average.");
      }
      wheel_time_mode_name_ = "average";
      wheel_time_mode_ = WheelTimeMode::kAverage;
    }

    max_abs_linear_speed_ = std::max(0.0, max_abs_linear_speed_);
    max_abs_angular_speed_ = std::max(0.0, max_abs_angular_speed_);
    if (!std::isfinite(reconnect_interval_sec_) || reconnect_interval_sec_ <= 0.0) {
      ROS_WARN_STREAM(
          "Invalid parameter: reconnect_interval_sec must be > 0. Fallback to 1.0.");
      reconnect_interval_sec_ = 1.0;
    }
    if (!std::isfinite(warn_interval_sec_) || warn_interval_sec_ <= 0.0) {
      ROS_WARN_STREAM("Invalid parameter: warn_interval_sec must be > 0. Fallback to 5.0.");
      warn_interval_sec_ = 5.0;
    }
    if (!std::isfinite(loop_rate_hz_) || loop_rate_hz_ <= 0.0) {
      ROS_WARN_STREAM("Invalid parameter: loop_rate_hz must be > 0. Fallback to 100.0.");
      loop_rate_hz_ = 100.0;
    }
    if (!std::isfinite(publish_rate_hz_) || publish_rate_hz_ <= 0.0) {
      ROS_WARN_STREAM("Invalid parameter: publish_rate_hz must be > 0. Fallback to 50.0.");
      publish_rate_hz_ = 50.0;
    }
    if (!std::isfinite(stale_timeout_sec_) || stale_timeout_sec_ < 0.0) {
      ROS_WARN_STREAM("Invalid parameter: stale_timeout_sec must be >= 0. Fallback to 0.2.");
      stale_timeout_sec_ = 0.2;
    }
    if (max_repeat_publish_count_ < 0) {
      ROS_WARN_STREAM(
          "Invalid parameter: max_repeat_publish_count must be >= 0. Fallback to 10.");
      max_repeat_publish_count_ = 10;
    }
    if (!std::isfinite(wheel_speed_unit_to_mps_) || wheel_speed_unit_to_mps_ == 0.0) {
      ROS_WARN_STREAM(
          "Invalid parameter: wheel_speed_unit_to_mps must be non-zero. Fallback to 0.001.");
      wheel_speed_unit_to_mps_ = 0.001;
    }
    if (!std::isfinite(left_wheel_speed_scale_) || left_wheel_speed_scale_ == 0.0) {
      ROS_WARN_STREAM(
          "Invalid parameter: left_wheel_speed_scale must be non-zero. Fallback to "
          "1.0.");
      left_wheel_speed_scale_ = 1.0;
    }
    if (!std::isfinite(right_wheel_speed_scale_) || right_wheel_speed_scale_ == 0.0) {
      ROS_WARN_STREAM(
          "Invalid parameter: right_wheel_speed_scale must be non-zero. Fallback to 1.0.");
      right_wheel_speed_scale_ = 1.0;
    }
    if (!std::isfinite(wheel_separation_) || wheel_separation_ <= 0.0) {
      ROS_WARN_STREAM(
          "Invalid parameter: wheel_separation must be > 0. Fallback to 0.725.");
      wheel_separation_ = 0.725;
    }
    if (!std::isfinite(wheel_time_unit_to_sec_) || wheel_time_unit_to_sec_ <= 0.0) {
      ROS_WARN_STREAM(
          "Invalid parameter: wheel_time_unit_to_sec must be > 0. Fallback to 0.001.");
      wheel_time_unit_to_sec_ = 0.001;
    }
    if (!std::isfinite(absolute_time_unit_to_sec_) || absolute_time_unit_to_sec_ <= 0.0) {
      ROS_WARN_STREAM(
          "Invalid parameter: absolute_time_unit_to_sec must be > 0. Fallback to 0.001.");
      absolute_time_unit_to_sec_ = 0.001;
    }
    if (!std::isfinite(min_valid_odom_dt_sec_) || min_valid_odom_dt_sec_ < 0.0) {
      ROS_WARN_STREAM(
          "Invalid parameter: min_valid_odom_dt_sec must be >= 0. Fallback to 0.005.");
      min_valid_odom_dt_sec_ = 0.005;
    }
    if (!std::isfinite(max_valid_odom_dt_sec_) || max_valid_odom_dt_sec_ <= 0.0) {
      ROS_WARN_STREAM(
          "Invalid parameter: max_valid_odom_dt_sec must be > 0. Fallback to 0.06.");
      max_valid_odom_dt_sec_ = 0.06;
    }
    if (max_valid_odom_dt_sec_ < min_valid_odom_dt_sec_) {
      ROS_WARN_STREAM("Invalid parameter combination: max_valid_odom_dt_sec < "
                      "min_valid_odom_dt_sec. Clamp max_valid_odom_dt_sec to "
                      "min_valid_odom_dt_sec.");
      max_valid_odom_dt_sec_ = min_valid_odom_dt_sec_;
    }
    max_abs_linear_accel_ = std::max(0.0, max_abs_linear_accel_);
    max_abs_angular_accel_ = std::max(0.0, max_abs_angular_accel_);

    ResolveDiagnosticLogPath();
    OpenDiagnosticLog();
    ResetDiagnosticMetrics();

    if (enable_odom_publish_) {
      odom_pub_ = nh_.advertise<nav_msgs::Odometry>(publish_topic_, 50);
      if (publish_mode_ == OdomPublishMode::kFixedRate) {
        odom_publish_timer_ = nh_.createTimer(ros::Duration(1.0 / publish_rate_hz_),
                                              &WheelSpeedOdomNode::OdomPublishTimerCallback,
                                              this);
      }
    }
    if (enable_imu_diagnostic_) {
      imu_sub_ = nh_.subscribe(imu_topic_, 200, &WheelSpeedOdomNode::ImuCallback, this);
      ROS_INFO_STREAM("IMU diagnostic topic: " << imu_topic_);
    }
    ROS_INFO_STREAM("odom rx enabled: " << (enable_odom_rx_ ? "true" : "false")
                    << ", odom publish enabled: " << (enable_odom_publish_ ? "true" : "false"));
    ROS_INFO_STREAM("odom transport=" << transport_type_name_
                    << ", endpoint=" << InputEndpointSummary());
    if (enable_odom_publish_) {
      ROS_INFO_STREAM("odom publish topic: " << resolved_publish_topic_);
      ROS_INFO_STREAM("odom TF publish enabled: " << (publish_odom_tf_ ? "true" : "false"));
      ROS_INFO_STREAM("odom publish mode=" << publish_mode_name_);
      if (publish_mode_ == OdomPublishMode::kFixedRate) {
        ROS_INFO_STREAM("fixed-rate odom publish config: publish_rate_hz=" << publish_rate_hz_
                        << ", stale_timeout_sec=" << stale_timeout_sec_
                        << ", max_repeat_publish_count=" << max_repeat_publish_count_);
        if (loop_rate_hz_ < publish_rate_hz_) {
          ROS_WARN_STREAM("loop_rate_hz (" << loop_rate_hz_
                          << ") is lower than publish_rate_hz (" << publish_rate_hz_
                          << "). Fixed-rate publish timing may be less stable than requested.");
        }
      }
    }
    ROS_INFO_STREAM("wheel_speed_field_order=" << wheel_speed_field_order_name_
                    << ", wheel_speed_unit_to_mps=" << wheel_speed_unit_to_mps_
                    << ", left_wheel_speed_scale=" << left_wheel_speed_scale_
                    << ", right_wheel_speed_scale=" << right_wheel_speed_scale_
                    << ", wheel_separation=" << wheel_separation_
                    << ", odom_time_source=" << odom_time_source_name_
                    << ", wheel_time_mode=" << wheel_time_mode_name_
                    << ", wheel_time_unit_to_sec=" << wheel_time_unit_to_sec_
                    << ", absolute_time_unit_to_sec=" << absolute_time_unit_to_sec_
                    << ", min_valid_odom_dt_sec=" << min_valid_odom_dt_sec_
                    << ", max_valid_odom_dt_sec=" << max_valid_odom_dt_sec_
                    << ", max_abs_linear_accel=" << max_abs_linear_accel_
                    << ", max_abs_angular_accel=" << max_abs_angular_accel_);
    ROS_INFO_STREAM("raw odom pose covariance diagonal: "
                    << FormatVector(odom_pose_covariance_diagonal_));
    ROS_INFO_STREAM("raw odom twist covariance diagonal: "
                    << FormatVector(odom_twist_covariance_diagonal_));
  }

  ~WheelSpeedOdomNode() {
    CloseInput();
    if (diagnostic_log_stream_.is_open()) {
      diagnostic_log_stream_.close();
    }
  }

  void Run() {
    ros::Rate loop_rate(loop_rate_hz_);

    while (ros::ok()) {
      if (input_fd_ < 0) {
        if (!ConnectInput()) {
          WarnEvery("Input error: failed to open odom source " + InputEndpointSummary() + ".");
          ros::spinOnce();
          loop_rate.sleep();
          ros::Duration(reconnect_interval_sec_).sleep();
          continue;
        }
        ROS_INFO_STREAM("Opened odom input via " << transport_type_name_ << ": "
                                                 << InputEndpointSummary());
      }

      ReadAvailableInputBytes();

      ros::spinOnce();
      if (enable_odom_rx_) {
        MaybeWarnNoDataFrame();
      }
      loop_rate.sleep();
    }
  }

 private:
  void ImuCallback(const sensor_msgs::Imu::ConstPtr& msg) {
    if (!enable_imu_diagnostic_ || msg == nullptr) {
      return;
    }

    const double wz = msg->angular_velocity.z;
    if (!std::isfinite(wz)) {
      return;
    }

    has_imu_sample_ = true;
    last_imu_angular_z_ = wz;
  }

  bool ConnectInput() {
    CloseInput();
    if (transport_type_ == OdomTransportType::kSerial) {
      if (!OpenSerial()) {
        return false;
      }
    } else if (!ConnectTcpServer()) {
      return false;
    }
    ResetInputSessionState();
    return true;
  }

  bool ConnectTcpServer() {
    input_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (input_fd_ < 0) {
      return false;
    }

    timeval timeout;
    timeout.tv_sec = 1;
    timeout.tv_usec = 0;
    setsockopt(input_fd_, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

    sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(server_port_));

    if (inet_pton(AF_INET, server_ip_.c_str(), &addr.sin_addr) != 1) {
      ROS_ERROR_STREAM("Invalid parameter: server_ip is not a valid IPv4 address: "
                       << server_ip_);
      CloseInput();
      return false;
    }

    if (connect(input_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
      CloseInput();
      return false;
    }
    return true;
  }

  bool OpenSerial() {
    input_fd_ = open(serial_device_.c_str(), O_RDONLY | O_NOCTTY | O_NONBLOCK);
    if (input_fd_ < 0) {
      return false;
    }

    termios tty;
    std::memset(&tty, 0, sizeof(tty));
    if (tcgetattr(input_fd_, &tty) != 0) {
      WarnEvery(std::string("Serial error: tcgetattr() failed: ") + std::strerror(errno));
      CloseInput();
      return false;
    }

    cfmakeraw(&tty);
    speed_t baud = ToTermiosBaud(serial_baudrate_);
    if (baud == 0) {
      ROS_WARN_STREAM("Unsupported serial_baudrate=" << serial_baudrate_
                      << ", fallback to 115200.");
      baud = B115200;
    }

    cfsetispeed(&tty, baud);
    cfsetospeed(&tty, baud);
    tty.c_cflag |= (CLOCAL | CREAD);
    tty.c_cflag &= ~CRTSCTS;
    tty.c_cflag &= ~PARENB;
    tty.c_cflag &= ~CSTOPB;
    tty.c_cflag &= ~CSIZE;
    tty.c_cflag |= CS8;
    tty.c_cc[VMIN] = 0;
    tty.c_cc[VTIME] = 0;

    if (tcsetattr(input_fd_, TCSANOW, &tty) != 0) {
      WarnEvery(std::string("Serial error: tcsetattr() failed: ") + std::strerror(errno));
      CloseInput();
      return false;
    }

    tcflush(input_fd_, TCIFLUSH);
    return true;
  }

  void ResetInputSessionState() {
    rx_buffer_.clear();
    connected_since_ = ros::Time::now();
    last_valid_frame_stamp_ = ros::Time();
    last_data_frame_stamp_ = ros::Time();
    last_no_data_warn_stamp_ = ros::Time();
    rx_total_bytes_since_connect_ = 0;
    valid_frame_count_since_connect_ = 0;
    data_frame_count_since_connect_ = 0;
    parse_error_count_since_connect_ = 0;
    fallback_frame_count_since_connect_ = 0;
    has_diagnostic_sample_ = false;
    has_absolute_time_sample_ = false;
    last_absolute_time_ = 0U;
    last_odom_stamp_ = ros::Time();
    last_left_encoder_ = 0U;
    last_right_encoder_ = 0U;
    last_left_wheel_time_ = 0U;
    last_right_wheel_time_ = 0U;
    has_motion_sample_ = false;
    last_motion_stamp_ = ros::Time();
    current_odom_linear_velocity_ = 0.0;
    current_odom_angular_velocity_ = 0.0;
    has_latest_odom_msg_ = false;
    latest_odom_update_wall_time_ = ros::Time();
    latest_odom_generation_ = 0U;
    last_published_odom_generation_ = 0U;
    latest_odom_repeat_publish_count_ = 0;
    ResetDiagnosticMetrics();
  }

  void CloseInput() {
    if (input_fd_ >= 0) {
      close(input_fd_);
      input_fd_ = -1;
    }
  }

  void ReadAvailableInputBytes() {
    uint8_t chunk[1024];

    while (ros::ok() && input_fd_ >= 0) {
      errno = 0;
      const ssize_t n =
          transport_type_ == OdomTransportType::kTcp
              ? recv(input_fd_, chunk, sizeof(chunk), MSG_DONTWAIT)
              : read(input_fd_, chunk, sizeof(chunk));
      if (n > 0) {
        rx_total_bytes_since_connect_ += static_cast<uint64_t>(n);
        if (enable_odom_rx_) {
          rx_buffer_.insert(rx_buffer_.end(), chunk, chunk + n);
          if (rx_buffer_.size() > kMaxBufferBytes) {
            WarnEvery("RX buffer overflow: dropped the oldest half of buffered data.");
            rx_buffer_.erase(rx_buffer_.begin(),
                             rx_buffer_.begin() +
                                 static_cast<std::ptrdiff_t>(rx_buffer_.size() / 2));
          }
          ParseBuffer();
        }
        continue;
      }

      if (n == 0) {
        if (transport_type_ == OdomTransportType::kTcp) {
          WarnEvery("Input error: TCP peer closed the connection.");
          CloseInput();
          ros::Duration(reconnect_interval_sec_).sleep();
        }
        return;
      }

      if (errno == EINTR) {
        continue;
      }
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        return;
      }

      if (transport_type_ == OdomTransportType::kTcp) {
        WarnEvery(std::string("Network error: recv() failed: ") + std::strerror(errno));
      } else {
        WarnEvery(std::string("Serial error: read() failed: ") + std::strerror(errno));
      }
      CloseInput();
      ros::Duration(reconnect_interval_sec_).sleep();
      return;
    }
  }

  std::string FormatDouble(double value, int precision = 6) const {
    if (!std::isfinite(value)) {
      return "NaN";
    }
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(precision) << value;
    return oss.str();
  }

  std::string FormatUint16Hex(uint16_t value) const {
    std::ostringstream oss;
    oss << "0x" << std::uppercase << std::hex << std::setw(4) << std::setfill('0')
        << static_cast<unsigned int>(value);
    return oss.str();
  }

  std::string FormatUint8Hex(uint8_t value) const {
    std::ostringstream oss;
    oss << "0x" << std::uppercase << std::hex << std::setw(2) << std::setfill('0')
        << static_cast<unsigned int>(value);
    return oss.str();
  }

  std::string FormatBytesHex(const uint8_t* bytes, size_t len) const {
    if (bytes == nullptr || len == 0) {
      return "";
    }

    std::ostringstream oss;
    oss << std::uppercase << std::hex << std::setfill('0');
    for (size_t i = 0; i < len; ++i) {
      if (i > 0) {
        oss << ' ';
      }
      oss << std::setw(2) << static_cast<unsigned int>(bytes[i]);
    }
    return oss.str();
  }

  std::string HexPreview(size_t max_bytes) const {
    std::ostringstream oss;
    oss << std::uppercase << std::hex << std::setfill('0');
    const size_t limit = std::min(max_bytes, rx_buffer_.size());
    for (size_t i = 0; i < limit; ++i) {
      if (i > 0) {
        oss << ' ';
      }
      oss << std::setw(2) << static_cast<unsigned int>(rx_buffer_[i]);
    }
    if (rx_buffer_.size() > limit) {
      oss << " ...";
    }
    return oss.str();
  }

  std::string FormatCandidateLengths(const std::vector<size_t>& candidates) const {
    std::ostringstream oss;
    oss << "[";
    for (size_t i = 0; i < candidates.size(); ++i) {
      if (i > 0) {
        oss << ",";
      }
      oss << candidates[i];
    }
    oss << "]";
    return oss.str();
  }

  std::string FormatSinceLastDataFrame() const {
    if (connected_since_.isZero()) {
      return "未知";
    }

    const ros::Time base =
        last_data_frame_stamp_.isZero() ? connected_since_ : last_data_frame_stamp_;
    return FormatDouble((ros::Time::now() - base).toSec(), 3);
  }

  size_t FindHeaderFrom(size_t start_index) const {
    if (rx_buffer_.size() < 2 || start_index >= rx_buffer_.size()) {
      return std::string::npos;
    }

    for (size_t i = start_index; i + 1 < rx_buffer_.size(); ++i) {
      if (rx_buffer_[i] == kHeader0 && rx_buffer_[i + 1] == kHeader1) {
        return i;
      }
    }
    return std::string::npos;
  }

  size_t FindHeader() const { return FindHeaderFrom(0); }

  bool ResolveTailAndChecksumIndex(size_t frame_len, size_t& checksum_index) const {
    if (frame_len < kMinFrameBytes || frame_len > rx_buffer_.size()) {
      return false;
    }

    if (frame_len >= 3 && rx_buffer_[frame_len - 2] == kEscape &&
        rx_buffer_[frame_len - 1] == kAltTail) {
      checksum_index = frame_len - 3;
      return true;
    }
    if (rx_buffer_[frame_len - 1] == kTail || rx_buffer_[frame_len - 1] == kAltTail) {
      checksum_index = frame_len - 2;
      return true;
    }
    return false;
  }

  uint8_t ComputeIncomingChecksum(size_t checksum_index) const {
    uint32_t sum = 0U;
    for (size_t i = 0; i < checksum_index; ++i) {
      if (rx_buffer_[i] == kEscape && (i + 1) < checksum_index &&
          rx_buffer_[i + 1] == kAltTail) {
        continue;
      }
      sum += rx_buffer_[i];
    }
    return static_cast<uint8_t>(sum & 0xFFU);
  }

  bool HasValidChecksum(size_t frame_len) const {
    if (frame_len < kMinFrameBytes || frame_len > rx_buffer_.size()) {
      return false;
    }

    size_t checksum_index = 0;
    if (!ResolveTailAndChecksumIndex(frame_len, checksum_index)) {
      return false;
    }
    if (checksum_index == 0 || checksum_index >= frame_len) {
      return false;
    }

    return ComputeIncomingChecksum(checksum_index) == rx_buffer_[checksum_index];
  }

  std::vector<size_t> BuildCandidateFrameLengths(uint16_t len_be) const {
    std::vector<size_t> candidates;
    if (len_be == 0 || len_be > kMaxLengthField) {
      return candidates;
    }

    for (size_t extra = 0; extra <= 10; ++extra) {
      candidates.push_back(static_cast<size_t>(len_be) + extra);
    }

    std::sort(candidates.begin(), candidates.end());
    candidates.erase(std::unique(candidates.begin(), candidates.end()), candidates.end());
    return candidates;
  }

  size_t FindFallbackFrameLength() const {
    if (rx_buffer_.size() < kMinFrameBytes || rx_buffer_[0] != kHeader0 ||
        rx_buffer_[1] != kHeader1 ||
        !IsNativeOdomDataCommand(rx_buffer_[4], rx_buffer_[5])) {
      return 0;
    }

    const size_t next_header = FindHeaderFrom(2);
    const size_t search_end = next_header == std::string::npos
                                  ? rx_buffer_.size()
                                  : std::min(next_header, rx_buffer_.size());

    size_t best_frame_len = 0;
    int best_score = std::numeric_limits<int>::min();
    for (size_t frame_len = kMinFrameBytes; frame_len <= search_end; ++frame_len) {
      size_t checksum_index = 0;
      if (!ResolveTailAndChecksumIndex(frame_len, checksum_index) ||
          !HasValidChecksum(frame_len)) {
        continue;
      }

      const uint8_t cmd1 = rx_buffer_[5];
      if (cmd1 != kCmdData) {
        continue;
      }

      const size_t data_len = checksum_index > 6 ? checksum_index - 6 : 0;
      int score = 0;
      score += 4;
      if (data_len >= kWheelSpeedDataBytes) {
        score += 3;
      } else if (data_len >= kDiagnosticDataBytes) {
        score += 2;
      }

      const size_t remain = rx_buffer_.size() - frame_len;
      if (remain >= 2 && rx_buffer_[frame_len] == kHeader0 &&
          rx_buffer_[frame_len + 1] == kHeader1) {
        score += 3;
      } else if (remain == 0) {
        score += 1;
      }

      if (score > best_score ||
          (score == best_score && (best_frame_len == 0 || frame_len < best_frame_len))) {
        best_score = score;
        best_frame_len = frame_len;
      }
    }
    return best_frame_len;
  }

  size_t FindForeignFrameLengthByScan(uint16_t len_be, bool* need_more) const {
    if (need_more != nullptr) {
      *need_more = false;
    }
    if (rx_buffer_.size() < kMinFrameBytes || rx_buffer_[0] != kHeader0 ||
        rx_buffer_[1] != kHeader1) {
      return 0;
    }

    const size_t next_header = FindHeaderFrom(2);
    const size_t search_end = next_header == std::string::npos
                                  ? rx_buffer_.size()
                                  : std::min(next_header, rx_buffer_.size());
    if (next_header == std::string::npos && need_more != nullptr) {
      *need_more = true;
    }

    size_t best_frame_len = 0;
    int best_score = std::numeric_limits<int>::min();
    for (size_t frame_len = kMinFrameBytes; frame_len <= search_end; ++frame_len) {
      size_t checksum_index = 0;
      if (!ResolveTailAndChecksumIndex(frame_len, checksum_index) ||
          !HasValidChecksum(frame_len)) {
        continue;
      }

      const uint8_t cmd0 = rx_buffer_[4];
      const uint8_t cmd1 = rx_buffer_[5];
      if (IsNativeOdomDataCommand(cmd0, cmd1)) {
        continue;
      }

      int score = 0;
      if (frame_len == len_be) {
        score += 4;
      } else if (frame_len == static_cast<size_t>(len_be) + 1) {
        score += 2;
      } else if (frame_len == static_cast<size_t>(len_be) + 2) {
        score += 1;
      }
      if (cmd0 == kCmd0) {
        score += 2;
      }

      const size_t remain = rx_buffer_.size() - frame_len;
      if (remain >= 2 && rx_buffer_[frame_len] == kHeader0 &&
          rx_buffer_[frame_len + 1] == kHeader1) {
        score += 3;
      } else if (remain == 0) {
        score += 1;
      }

      if (score > best_score ||
          (score == best_score && (best_frame_len == 0 || frame_len < best_frame_len))) {
        best_score = score;
        best_frame_len = frame_len;
      }
    }

    if (best_frame_len > 0 && need_more != nullptr) {
      *need_more = false;
    }
    return best_frame_len;
  }

  size_t FindForeignFrameLength(uint16_t len_be, const std::vector<size_t>& candidates,
                                bool* need_more) const {
    if (need_more != nullptr) {
      *need_more = false;
    }
    if (rx_buffer_.size() < kMinFrameBytes || rx_buffer_[0] != kHeader0 ||
        rx_buffer_[1] != kHeader1) {
      return 0;
    }

    size_t best_frame_len = 0;
    int best_score = std::numeric_limits<int>::min();
    for (size_t frame_len : candidates) {
      if (frame_len < kMinFrameBytes || frame_len > kMaxBufferBytes) {
        continue;
      }
      if (rx_buffer_.size() < frame_len) {
        if (need_more != nullptr) {
          *need_more = true;
        }
        continue;
      }
      if (!HasValidChecksum(frame_len)) {
        continue;
      }

      const uint8_t cmd0 = rx_buffer_[4];
      const uint8_t cmd1 = rx_buffer_[5];
      if (IsNativeOdomDataCommand(cmd0, cmd1)) {
        continue;
      }

      int score = 0;
      if (frame_len == len_be) {
        score += 4;
      } else if (frame_len == static_cast<size_t>(len_be) + 1) {
        score += 2;
      } else if (frame_len == static_cast<size_t>(len_be) + 2) {
        score += 1;
      }

      const size_t remain = rx_buffer_.size() - frame_len;
      if (remain >= 2 && rx_buffer_[frame_len] == kHeader0 &&
          rx_buffer_[frame_len + 1] == kHeader1) {
        score += 3;
      } else if (remain == 0) {
        score += 1;
      }

      if (score > best_score ||
          (score == best_score && (best_frame_len == 0 || frame_len < best_frame_len))) {
        best_score = score;
        best_frame_len = frame_len;
      }
    }

    if (best_frame_len == 0) {
      best_frame_len = FindForeignFrameLengthByScan(len_be, need_more);
    }
    return best_frame_len;
  }

  size_t DetermineFastResyncDropBytes() const {
    const size_t next_header = FindHeaderFrom(2);
    if (next_header != std::string::npos) {
      return next_header;
    }
    return 1;
  }

  void RecordFastResync(size_t dropped_bytes) {
    if (dropped_bytes == 0) {
      return;
    }
    ++fast_resync_count_since_connect_;
    fast_resync_dropped_bytes_since_connect_ += dropped_bytes;
  }

  void ParseBuffer() {
    while (true) {
      if (rx_buffer_.size() < kMinFrameBytes) {
        return;
      }

      const size_t header_pos = FindHeader();
      if (header_pos == std::string::npos) {
        if (!rx_buffer_.empty() && rx_buffer_.back() == kHeader0) {
          rx_buffer_.erase(rx_buffer_.begin(), rx_buffer_.end() - 1);
        } else {
          rx_buffer_.clear();
        }
        return;
      }

      if (header_pos > 0) {
        rx_buffer_.erase(rx_buffer_.begin(),
                         rx_buffer_.begin() + static_cast<std::ptrdiff_t>(header_pos));
      }

      if (rx_buffer_.size() < 4) {
        return;
      }

      // 协议长度字段固定按大端解析；payload 内的时间/编码器/轮速字段固定按小端解析。
      // Length field is fixed big-endian. Payload time/encoder/speed fields stay little-endian.
      // Length field is fixed big-endian. Payload fields stay little-endian.
      const uint16_t len_field_be =
          (static_cast<uint16_t>(rx_buffer_[2]) << 8) | static_cast<uint16_t>(rx_buffer_[3]);
      const std::vector<size_t> candidates = BuildCandidateFrameLengths(len_field_be);
      if (candidates.empty()) {
        ++parse_error_count_since_connect_;
        const size_t dropped_bytes = DetermineFastResyncDropBytes();
        RecordFastResync(dropped_bytes);
        WarnEvery("Frame parse error: invalid big-endian length field. len(BE)=" +
                  FormatUint16Hex(len_field_be) + ", buffer_size=" +
                  std::to_string(rx_buffer_.size()) + ", seconds_since_last_data=" +
                  FormatSinceLastDataFrame() + ", buffer_prefix=" + HexPreview(32) +
                  ". Dropped " + std::to_string(dropped_bytes) + " bytes to resync.");
        rx_buffer_.erase(rx_buffer_.begin(),
                         rx_buffer_.begin() + static_cast<std::ptrdiff_t>(dropped_bytes));
        continue;
      }

      bool need_more = false;
      size_t best_frame_len = 0;
      int best_score = std::numeric_limits<int>::min();

      for (size_t frame_len : candidates) {
        if (frame_len < kMinFrameBytes || frame_len > kMaxBufferBytes) {
          continue;
        }
        if (rx_buffer_.size() < frame_len) {
          need_more = true;
          continue;
        }
        if (rx_buffer_[0] != kHeader0 || rx_buffer_[1] != kHeader1 ||
            !IsNativeOdomDataCommand(rx_buffer_[4], rx_buffer_[5])) {
          continue;
        }
        if (!HasValidChecksum(frame_len)) {
          continue;
        }

        int score = 0;
        if (frame_len == len_field_be) {
          score += 4;
        } else if (frame_len == static_cast<size_t>(len_field_be) + 1) {
          score += 2;
        } else if (frame_len == static_cast<size_t>(len_field_be) + 2) {
          score += 1;
        }

        score += 2;

        const size_t remain = rx_buffer_.size() - frame_len;
        if (remain >= 2 && rx_buffer_[frame_len] == kHeader0 &&
            rx_buffer_[frame_len + 1] == kHeader1) {
          score += 3;
        } else if (remain == 0) {
          score += 1;
        }

        if (score > best_score ||
            (score == best_score && (best_frame_len == 0 || frame_len < best_frame_len))) {
          best_score = score;
          best_frame_len = frame_len;
        }
      }

      if (best_frame_len > 0) {
        ProcessFrame(best_frame_len);
        rx_buffer_.erase(rx_buffer_.begin(),
                         rx_buffer_.begin() + static_cast<std::ptrdiff_t>(best_frame_len));
        continue;
      }

      const size_t fallback_frame_len = FindFallbackFrameLength();
      if (fallback_frame_len > 0) {
        ++fallback_frame_count_since_connect_;
        WarnEvery("Frame parse warning: length field did not match actual frame length. "
                  "Used checksum/tail fallback. len(BE)=" +
                  FormatUint16Hex(len_field_be) + ", candidate_lengths=" +
                  FormatCandidateLengths(candidates) + ", chosen_frame_len=" +
                  std::to_string(fallback_frame_len) + ", buffer_size=" +
                  std::to_string(rx_buffer_.size()) + ", buffer_prefix=" + HexPreview(32) +
                  ".");
        ProcessFrame(fallback_frame_len);
        rx_buffer_.erase(rx_buffer_.begin(),
                         rx_buffer_.begin() + static_cast<std::ptrdiff_t>(fallback_frame_len));
        continue;
      }

      bool foreign_need_more = false;
      const size_t foreign_frame_len =
          FindForeignFrameLength(len_field_be, candidates, &foreign_need_more);
      if (foreign_frame_len > 0) {
        ++foreign_frame_count_since_connect_;
        WarnEvery("Frame parse notice: skipped non-odom frame. cmd=" +
                  FormatUint8Hex(rx_buffer_[4]) + " " + FormatUint8Hex(rx_buffer_[5]) +
                  ", len(BE)=" + FormatUint16Hex(len_field_be) + ", chosen_frame_len=" +
                  std::to_string(foreign_frame_len) + ", buffer_size=" +
                  std::to_string(rx_buffer_.size()) + ", buffer_prefix=" + HexPreview(32) + ".");
        rx_buffer_.erase(rx_buffer_.begin(),
                         rx_buffer_.begin() + static_cast<std::ptrdiff_t>(foreign_frame_len));
        continue;
      }

      if (need_more || foreign_need_more) {
        return;
      }

      ++parse_error_count_since_connect_;
      const size_t dropped_bytes = DetermineFastResyncDropBytes();
      RecordFastResync(dropped_bytes);
      WarnEvery("Frame parse error: current buffer cannot be matched to a valid frame. "
                "len(BE)=" + FormatUint16Hex(len_field_be) + ", candidate_lengths=" +
                FormatCandidateLengths(candidates) + ", buffer_size=" +
                std::to_string(rx_buffer_.size()) + ", valid_frames=" +
                std::to_string(valid_frame_count_since_connect_) + ", data_frames=" +
                std::to_string(data_frame_count_since_connect_) +
                ", seconds_since_last_data=" + FormatSinceLastDataFrame() +
                ", buffer_prefix=" + HexPreview(32) +
                ". Dropped " + std::to_string(dropped_bytes) + " bytes to resync.");
      rx_buffer_.erase(rx_buffer_.begin(),
                       rx_buffer_.begin() + static_cast<std::ptrdiff_t>(dropped_bytes));
    }
  }

  void ProcessFrame(size_t frame_len) {
    if (frame_len < kMinFrameBytes || frame_len > rx_buffer_.size()) {
      return;
    }
    if (!IsNativeOdomDataCommand(rx_buffer_[4], rx_buffer_[5])) {
      return;
    }

    const ros::Time now = ros::Time::now();
    ++valid_frame_count_since_connect_;
    last_valid_frame_stamp_ = now;

    size_t checksum_index = 0;
    if (!ResolveTailAndChecksumIndex(frame_len, checksum_index)) {
      return;
    }
    if (checksum_index <= 6 || checksum_index > frame_len) {
      return;
    }

    const size_t data_len = checksum_index - 6;
    if (data_len < kDiagnosticDataBytes) {
      ++parse_error_count_since_connect_;
      WarnEvery("Frame parse error: payload too short for timestamp/encoder diagnostic fields. "
                "data_len=" +
                std::to_string(data_len) + ", required_min=" +
                std::to_string(kDiagnosticDataBytes) + " bytes.");
      return;
    }

    const uint8_t* data = rx_buffer_.data() + 6;
    ResetDiagnosticMetrics();
    diag_raw_frame_hex_ = FormatBytesHex(rx_buffer_.data(), frame_len);
    ++data_frame_count_since_connect_;
    last_data_frame_stamp_ = now;

    const uint32_t absolute_time = ReadUInt32Le(data + kAbsoluteTimeOffset);
    const uint32_t left_wheel_time = ReadUInt32Le(data + kLeftWheelTimeOffset);
    const uint32_t right_wheel_time = ReadUInt32Le(data + kRightWheelTimeOffset);
    const uint32_t left_encoder = ReadUInt32Le(data + kLeftEncoderOffset);
    const uint32_t right_encoder = ReadUInt32Le(data + kRightEncoderOffset);

    diag_absolute_time_ = absolute_time;
    diag_left_wheel_time_ = left_wheel_time;
    diag_right_wheel_time_ = right_wheel_time;
    diag_left_encoder_ = left_encoder;
    diag_right_encoder_ = right_encoder;
    UpdateDiagnosticDeltas(left_wheel_time, right_wheel_time, left_encoder, right_encoder);
    ros::Time odom_stamp;
    if (!ResolveOdomStamp(now, absolute_time, left_wheel_time, right_wheel_time, &odom_stamp)) {
      ResetMotionInterval(odom_stamp);
      RememberDiagnosticSample(left_wheel_time, right_wheel_time, left_encoder, right_encoder);
      LogDiagnosticFrame();
      return;
    }
    double left_wheel_speed = std::numeric_limits<double>::quiet_NaN();
    double right_wheel_speed = std::numeric_limits<double>::quiet_NaN();
    if (!ResolveReportedWheelSpeeds(data, data_len, &left_wheel_speed, &right_wheel_speed)) {
      RememberDiagnosticSample(left_wheel_time, right_wheel_time, left_encoder, right_encoder);
      LogDiagnosticFrame();
      return;
    }

    UpdateWheelSpeedOdom(odom_stamp, left_wheel_speed, right_wheel_speed);
    RememberDiagnosticSample(left_wheel_time, right_wheel_time, left_encoder, right_encoder);
    LogDiagnosticFrame();
  }

  bool TryConvertRawTimeDeltaToSec(uint32_t delta_raw, double unit_to_sec, double* delta_sec) const {
    if (delta_sec == nullptr) {
      return false;
    }
    const double delta_sec_value = static_cast<double>(delta_raw) * unit_to_sec;
    if (!std::isfinite(delta_sec_value) || delta_sec_value < 0.0 ||
        delta_sec_value > kTimeJumpWarnSec) {
      return false;
    }
    *delta_sec = delta_sec_value;
    return true;
  }

  bool TryResolveAbsoluteTimeDeltaSec(uint32_t absolute_time_raw, double* delta_sec) {
    if (delta_sec == nullptr || !has_absolute_time_sample_ || last_odom_stamp_.isZero()) {
      return false;
    }

    const uint32_t absolute_time_delta_raw =
        ComputeUInt32ForwardDelta(absolute_time_raw, last_absolute_time_);
    if (!TryConvertRawTimeDeltaToSec(absolute_time_delta_raw, absolute_time_unit_to_sec_,
                                     delta_sec)) {
      ++parse_error_count_since_connect_;
      WarnEvery("Odom timestamp warning: abnormal absolute_time jump. "
                "Resynced using fallback time. absolute_time_raw=" +
                std::to_string(absolute_time_raw) + ", last_absolute_time=" +
                std::to_string(last_absolute_time_) + ", delta_raw=" +
                std::to_string(absolute_time_delta_raw) + ".");
      return false;
    }
    return true;
  }

  bool TryResolveWheelTimeDeltaSec(uint32_t left_wheel_time_raw, uint32_t right_wheel_time_raw,
                                   double* delta_sec) {
    if (delta_sec == nullptr || !has_wheel_time_reference_ || last_odom_stamp_.isZero()) {
      return false;
    }

    const uint32_t left_delta_raw =
        ComputeUInt32ForwardDelta(left_wheel_time_raw, last_time_left_wheel_time_);
    const uint32_t right_delta_raw =
        ComputeUInt32ForwardDelta(right_wheel_time_raw, last_time_right_wheel_time_);
    double left_delta_sec = 0.0;
    double right_delta_sec = 0.0;
    const bool left_valid =
        TryConvertRawTimeDeltaToSec(left_delta_raw, wheel_time_unit_to_sec_, &left_delta_sec);
    const bool right_valid =
        TryConvertRawTimeDeltaToSec(right_delta_raw, wheel_time_unit_to_sec_, &right_delta_sec);

    switch (wheel_time_mode_) {
      case WheelTimeMode::kLeft:
        if (left_valid) {
          *delta_sec = left_delta_sec;
          return true;
        }
        ++parse_error_count_since_connect_;
        WarnEvery("Odom timestamp warning: abnormal left_wheel_time jump. left_time_raw=" +
                  std::to_string(left_wheel_time_raw) + ", last_left_wheel_time=" +
                  std::to_string(last_time_left_wheel_time_) + ", delta_raw=" +
                  std::to_string(left_delta_raw) + ".");
        return false;

      case WheelTimeMode::kRight:
        if (right_valid) {
          *delta_sec = right_delta_sec;
          return true;
        }
        ++parse_error_count_since_connect_;
        WarnEvery("Odom timestamp warning: abnormal right_wheel_time jump. right_time_raw=" +
                  std::to_string(right_wheel_time_raw) + ", last_right_wheel_time=" +
                  std::to_string(last_time_right_wheel_time_) + ", delta_raw=" +
                  std::to_string(right_delta_raw) + ".");
        return false;

      case WheelTimeMode::kAverage:
      default:
        if (left_valid && right_valid) {
          *delta_sec = 0.5 * (left_delta_sec + right_delta_sec);
          return true;
        }
        if (left_valid || right_valid) {
          *delta_sec = left_valid ? left_delta_sec : right_delta_sec;
          ++parse_error_count_since_connect_;
          WarnEvery("Odom timestamp warning: one wheel_time delta invalid in average mode. "
                    "Use the valid side only. left_delta_raw=" +
                    std::to_string(left_delta_raw) + ", right_delta_raw=" +
                    std::to_string(right_delta_raw) + ".");
          return true;
        }
        ++parse_error_count_since_connect_;
        WarnEvery("Odom timestamp warning: both wheel_time deltas invalid. left_delta_raw=" +
                  std::to_string(left_delta_raw) + ", right_delta_raw=" +
                  std::to_string(right_delta_raw) + ".");
        return false;
    }
  }

  bool IsAcceptedOdomDeltaSec(double delta_sec) const {
    if (!std::isfinite(delta_sec)) {
      return false;
    }
    if (delta_sec < min_valid_odom_dt_sec_) {
      return false;
    }
    if (max_valid_odom_dt_sec_ > 0.0 && delta_sec > max_valid_odom_dt_sec_) {
      return false;
    }
    return true;
  }

  void UpdateTimeReference(const ros::Time& odom_stamp, uint32_t absolute_time_raw,
                           uint32_t left_wheel_time_raw, uint32_t right_wheel_time_raw) {
    has_absolute_time_sample_ = true;
    last_absolute_time_ = absolute_time_raw;
    has_wheel_time_reference_ = true;
    last_time_left_wheel_time_ = left_wheel_time_raw;
    last_time_right_wheel_time_ = right_wheel_time_raw;
    last_odom_stamp_ = odom_stamp;
  }

  bool ResolveOdomStamp(const ros::Time& fallback_stamp, uint32_t absolute_time_raw,
                        uint32_t left_wheel_time_raw, uint32_t right_wheel_time_raw,
                        ros::Time* resolved_stamp) {
    if (resolved_stamp == nullptr) {
      return false;
    }

    if (last_odom_stamp_.isZero() || !has_absolute_time_sample_ || !has_wheel_time_reference_) {
      *resolved_stamp = fallback_stamp;
      UpdateTimeReference(*resolved_stamp, absolute_time_raw, left_wheel_time_raw,
                          right_wheel_time_raw);
      return true;
    }

    double time_delta_sec = 0.0;
    bool resolved_from_delta = false;

    if (odom_time_source_ == OdomTimeSource::kWheelTime) {
      resolved_from_delta =
          TryResolveWheelTimeDeltaSec(left_wheel_time_raw, right_wheel_time_raw, &time_delta_sec);
      if (!resolved_from_delta) {
        resolved_from_delta = TryResolveAbsoluteTimeDeltaSec(absolute_time_raw, &time_delta_sec);
      }
    } else {
      resolved_from_delta = TryResolveAbsoluteTimeDeltaSec(absolute_time_raw, &time_delta_sec);
    }

    if (!resolved_from_delta) {
      ++timing_reject_count_since_connect_;
      *resolved_stamp = fallback_stamp;
      UpdateTimeReference(*resolved_stamp, absolute_time_raw, left_wheel_time_raw,
                          right_wheel_time_raw);
      WarnEvery("Odom timestamp reject: protocol timestamps invalid. Resynced time base "
                "without integrating this frame.");
      return false;
    }

    if (!IsAcceptedOdomDeltaSec(time_delta_sec)) {
      ++timing_reject_count_since_connect_;
      *resolved_stamp = fallback_stamp;
      UpdateTimeReference(*resolved_stamp, absolute_time_raw, left_wheel_time_raw,
                          right_wheel_time_raw);
      WarnEvery("Odom timestamp reject: dt out of accepted range. dt_sec=" +
                FormatDouble(time_delta_sec, 6) + ", min_valid_odom_dt_sec=" +
                FormatDouble(min_valid_odom_dt_sec_, 6) + ", max_valid_odom_dt_sec=" +
                FormatDouble(max_valid_odom_dt_sec_, 6) +
                ". Resynced time base without integrating this frame.");
      return false;
    }

    *resolved_stamp = last_odom_stamp_ + ros::Duration(time_delta_sec);
    if (*resolved_stamp <= last_odom_stamp_) {
      ++timing_reject_count_since_connect_;
      *resolved_stamp = fallback_stamp;
      UpdateTimeReference(*resolved_stamp, absolute_time_raw, left_wheel_time_raw,
                          right_wheel_time_raw);
      WarnEvery("Odom timestamp reject: non-monotonic stamp after delta integration. "
                "Resynced time base without integrating this frame.");
      return false;
    }

    UpdateTimeReference(*resolved_stamp, absolute_time_raw, left_wheel_time_raw,
                        right_wheel_time_raw);
    return true;
  }

  void UpdateDiagnosticDeltas(uint32_t left_wheel_time_raw, uint32_t right_wheel_time_raw,
                              uint32_t left_encoder_raw, uint32_t right_encoder_raw) {
    if (!has_diagnostic_sample_) {
      diag_left_wheel_time_delta_ = 0U;
      diag_right_wheel_time_delta_ = 0U;
      diag_left_encoder_step_ = 0;
      diag_right_encoder_step_ = 0;
      return;
    }

    diag_left_wheel_time_delta_ =
        ComputeUInt32ForwardDelta(left_wheel_time_raw, last_left_wheel_time_);
    diag_right_wheel_time_delta_ =
        ComputeUInt32ForwardDelta(right_wheel_time_raw, last_right_wheel_time_);
    diag_left_encoder_step_ = ComputeUInt32SignedDelta(left_encoder_raw, last_left_encoder_);
    diag_right_encoder_step_ = ComputeUInt32SignedDelta(right_encoder_raw, last_right_encoder_);
  }

  void RememberDiagnosticSample(uint32_t left_wheel_time_raw, uint32_t right_wheel_time_raw,
                                uint32_t left_encoder_raw, uint32_t right_encoder_raw) {
    last_left_encoder_ = left_encoder_raw;
    last_right_encoder_ = right_encoder_raw;
    last_left_wheel_time_ = left_wheel_time_raw;
    last_right_wheel_time_ = right_wheel_time_raw;
    has_diagnostic_sample_ = true;
  }

  bool ResolveReportedWheelSpeeds(const uint8_t* data, size_t data_len, double* left_wheel_speed,
                                  double* right_wheel_speed) {
    if (left_wheel_speed == nullptr || right_wheel_speed == nullptr) {
      return false;
    }
    if (data == nullptr || data_len < kWheelSpeedDataBytes) {
      ++parse_error_count_since_connect_;
      WarnEvery("Wheel speed field missing: payload shorter than 28 bytes. Odom update skipped.");
      return false;
    }

    const double wheel_speed_field0 =
        static_cast<double>(ReadFloat32Le(data + kWheelSpeedField0Offset));
    const double wheel_speed_field1 =
        static_cast<double>(ReadFloat32Le(data + kWheelSpeedField1Offset));
    if (!std::isfinite(wheel_speed_field0) || !std::isfinite(wheel_speed_field1)) {
      ++parse_error_count_since_connect_;
      WarnEvery("Wheel speed field error: non-finite left/right wheel speed received. "
                "Odom update skipped.");
      return false;
    }

    const double left_wheel_speed_raw =
        wheel_speed_field_order_ == WheelSpeedFieldOrder::kLeftRight ? wheel_speed_field0
                                                                     : wheel_speed_field1;
    const double right_wheel_speed_raw =
        wheel_speed_field_order_ == WheelSpeedFieldOrder::kLeftRight ? wheel_speed_field1
                                                                     : wheel_speed_field0;
    *left_wheel_speed = left_wheel_speed_raw * wheel_speed_unit_to_mps_ * left_wheel_speed_scale_;
    *right_wheel_speed =
        right_wheel_speed_raw * wheel_speed_unit_to_mps_ * right_wheel_speed_scale_;
    diag_reported_left_wheel_speed_ = *left_wheel_speed;
    diag_reported_right_wheel_speed_ = *right_wheel_speed;
    return true;
  }

  void ResetMotionInterval(const ros::Time&) {
    has_motion_sample_ = false;
    last_motion_stamp_ = ros::Time();
  }

  void UpdateWheelSpeedOdom(const ros::Time& odom_stamp, double left_wheel_speed,
                            double right_wheel_speed) {
    if (!std::isfinite(left_wheel_speed) || !std::isfinite(right_wheel_speed)) {
      return;
    }
    const double linear_velocity = 0.5 * (left_wheel_speed + right_wheel_speed);
    const double angular_velocity = (right_wheel_speed - left_wheel_speed) / wheel_separation_;
    diag_odom_linear_ = linear_velocity;
    diag_odom_angular_ = angular_velocity;

    bool rejected = false;
    std::string reject_reason;
    if (max_abs_linear_speed_ > 0.0 && std::fabs(linear_velocity) > max_abs_linear_speed_) {
      rejected = true;
      reject_reason = "velocity limit exceeded";
    }
    if (!rejected && max_abs_angular_speed_ > 0.0 &&
        std::fabs(angular_velocity) > max_abs_angular_speed_) {
      rejected = true;
      reject_reason = "velocity limit exceeded";
    }

    if (rejected) {
      ++parse_error_count_since_connect_;
      ++motion_reject_count_since_connect_;
      WarnEvery("Wheel-speed odom frame error: " + reject_reason + ". Frame dropped. "
                "linear_velocity=" + FormatDouble(linear_velocity) +
                ", angular_velocity=" + FormatDouble(angular_velocity) +
                ", max_abs_linear_speed=" + FormatDouble(max_abs_linear_speed_) +
                ", max_abs_angular_speed=" + FormatDouble(max_abs_angular_speed_) + ".");
      return;
    }

    if (!has_motion_sample_) {
      current_odom_linear_velocity_ = linear_velocity;
      current_odom_angular_velocity_ = angular_velocity;
      has_motion_sample_ = true;
      last_motion_stamp_ = odom_stamp;
      HandleOdomStateUpdated(odom_stamp);
      return;
    }

    const double dt_sec = (odom_stamp - last_motion_stamp_).toSec();
    if (!std::isfinite(dt_sec) || dt_sec <= 0.0) {
      ++motion_reject_count_since_connect_;
      WarnEvery("Wheel-speed odom frame reject: invalid motion dt. Frame skipped without "
                "resetting state. dt_sec=" + FormatDouble(dt_sec, 6) + ".");
      return;
    }

    const double linear_accel = (linear_velocity - current_odom_linear_velocity_) / dt_sec;
    const double angular_accel = (angular_velocity - current_odom_angular_velocity_) / dt_sec;
    if (max_abs_linear_accel_ > 0.0 && std::fabs(linear_accel) > max_abs_linear_accel_) {
      ++motion_reject_count_since_connect_;
      WarnEvery("Wheel-speed odom frame reject: linear acceleration limit exceeded. "
                "Frame skipped without resetting state. linear_accel=" +
                FormatDouble(linear_accel) + ", dt_sec=" + FormatDouble(dt_sec, 6) +
                ", max_abs_linear_accel=" + FormatDouble(max_abs_linear_accel_) + ".");
      return;
    }
    if (max_abs_angular_accel_ > 0.0 && std::fabs(angular_accel) > max_abs_angular_accel_) {
      ++motion_reject_count_since_connect_;
      WarnEvery("Wheel-speed odom frame reject: angular acceleration limit exceeded. "
                "Frame skipped without resetting state. angular_accel=" +
                FormatDouble(angular_accel) + ", dt_sec=" + FormatDouble(dt_sec, 6) +
                ", max_abs_angular_accel=" + FormatDouble(max_abs_angular_accel_) + ".");
      return;
    }

    if (std::isfinite(dt_sec) && dt_sec > 0.0) {
      const double delta_s = linear_velocity * dt_sec;
      const double delta_yaw = angular_velocity * dt_sec;
      const double heading = yaw_ + 0.5 * delta_yaw;
      x_ += delta_s * std::cos(heading);
      y_ += delta_s * std::sin(heading);
      yaw_ = NormalizeAngle(yaw_ + delta_yaw);
    }
    current_odom_linear_velocity_ = linear_velocity;
    current_odom_angular_velocity_ = angular_velocity;
    last_motion_stamp_ = odom_stamp;
    HandleOdomStateUpdated(odom_stamp);
  }

  void HandleOdomStateUpdated(const ros::Time& stamp) {
    if (!enable_odom_publish_) {
      return;
    }

    latest_odom_msg_ = BuildOdomMessage(stamp);
    has_latest_odom_msg_ = true;
    latest_odom_update_wall_time_ = ros::Time::now();
    ++latest_odom_generation_;
    latest_odom_repeat_publish_count_ = 0;

    if (publish_mode_ == OdomPublishMode::kOnFrame) {
      last_published_odom_generation_ = latest_odom_generation_;
      PublishOdomMessage(latest_odom_msg_);
    }
  }

  nav_msgs::Odometry BuildOdomMessage(const ros::Time& stamp) const {
    nav_msgs::Odometry odom;
    odom.header.stamp = stamp;
    odom.header.frame_id = frame_id_;
    odom.child_frame_id = child_frame_id_;

    odom.pose.pose.position.x = x_;
    odom.pose.pose.position.y = y_;
    odom.pose.pose.position.z = 0.0;
    odom.pose.pose.orientation.x = 0.0;
    odom.pose.pose.orientation.y = 0.0;
    odom.pose.pose.orientation.z = std::sin(yaw_ * 0.5);
    odom.pose.pose.orientation.w = std::cos(yaw_ * 0.5);

    odom.twist.twist.linear.x = current_odom_linear_velocity_;
    odom.twist.twist.linear.y = 0.0;
    odom.twist.twist.linear.z = 0.0;
    odom.twist.twist.angular.x = 0.0;
    odom.twist.twist.angular.y = 0.0;
    odom.twist.twist.angular.z = current_odom_angular_velocity_;

    std::copy(odom_pose_covariance_.begin(), odom_pose_covariance_.end(),
              odom.pose.covariance.begin());
    std::copy(odom_twist_covariance_.begin(), odom_twist_covariance_.end(),
              odom.twist.covariance.begin());
    return odom;
  }

  void PublishOdomMessage(const nav_msgs::Odometry& odom) {
    odom_pub_.publish(odom);
    if (publish_odom_tf_) {
      PublishOdomTransform(odom.header.stamp);
    }
  }

  void OdomPublishTimerCallback(const ros::TimerEvent&) {
    if (!enable_odom_publish_ || publish_mode_ != OdomPublishMode::kFixedRate ||
        !has_latest_odom_msg_) {
      return;
    }

    if (!latest_odom_update_wall_time_.isZero() && stale_timeout_sec_ > 0.0) {
      const double stale_age_sec = (ros::Time::now() - latest_odom_update_wall_time_).toSec();
      if (stale_age_sec > stale_timeout_sec_) {
        return;
      }
    }

    if (last_published_odom_generation_ == latest_odom_generation_) {
      if (latest_odom_repeat_publish_count_ >= max_repeat_publish_count_) {
        return;
      }
      ++latest_odom_repeat_publish_count_;
    } else {
      last_published_odom_generation_ = latest_odom_generation_;
      latest_odom_repeat_publish_count_ = 0;
    }

    PublishOdomMessage(latest_odom_msg_);
  }

  void PublishOdomTransform(const ros::Time& stamp) {
    geometry_msgs::TransformStamped transform;
    transform.header.stamp = stamp;
    transform.header.frame_id = frame_id_;
    transform.child_frame_id = child_frame_id_;
    transform.transform.translation.x = x_;
    transform.transform.translation.y = y_;
    transform.transform.translation.z = 0.0;
    transform.transform.rotation.x = 0.0;
    transform.transform.rotation.y = 0.0;
    transform.transform.rotation.z = std::sin(yaw_ * 0.5);
    transform.transform.rotation.w = std::cos(yaw_ * 0.5);
    tf_broadcaster_.sendTransform(transform);
  }

  void MaybeWarnNoDataFrame() {
    if (input_fd_ < 0 || connected_since_.isZero()) {
      return;
    }

    const ros::Time now = ros::Time::now();
    const ros::Time base =
        last_data_frame_stamp_.isZero() ? connected_since_ : last_data_frame_stamp_;
    const double silent_sec = (now - base).toSec();
    if (silent_sec < warn_interval_sec_) {
      return;
    }
    if (!last_no_data_warn_stamp_.isZero() &&
        (now - last_no_data_warn_stamp_).toSec() < warn_interval_sec_) {
      return;
    }

    ROS_WARN_STREAM("Runtime warning: odom source " << InputEndpointSummary()
                    << " is connected, but no valid data frame has been parsed for "
                    << FormatDouble(silent_sec, 3)
                    << " seconds. rx_bytes=" << rx_total_bytes_since_connect_
                    << ", valid_frames=" << valid_frame_count_since_connect_
                    << ", data_frames=" << data_frame_count_since_connect_
                    << ", foreign_frames=" << foreign_frame_count_since_connect_
                    << ", parse_errors=" << parse_error_count_since_connect_
                    << ", fallback_matches=" << fallback_frame_count_since_connect_
                    << ", fast_resyncs=" << fast_resync_count_since_connect_
                    << ", fast_resync_drop_bytes=" << fast_resync_dropped_bytes_since_connect_
                    << ", timing_rejects=" << timing_reject_count_since_connect_
                    << ", motion_rejects=" << motion_reject_count_since_connect_
                    << ", buffer_size=" << rx_buffer_.size()
                    << (rx_buffer_.empty() ? "" : ", buffer_prefix=" + HexPreview(32)));
    last_no_data_warn_stamp_ = now;
  }

  void ResolveDiagnosticLogPath() {
    if (IsAbsolutePath(diagnostic_log_path_)) {
      resolved_diagnostic_log_path_ = diagnostic_log_path_;
      return;
    }

    const std::string workspace_root = GetWorkspaceRootPath();
    if (!workspace_root.empty()) {
      resolved_diagnostic_log_path_ = workspace_root + "/" + diagnostic_log_path_;
      return;
    }

    resolved_diagnostic_log_path_ = diagnostic_log_path_;
  }

  void OpenDiagnosticLog() {
    if (!enable_diagnostic_log_) {
      return;
    }

    diagnostic_log_stream_.open(resolved_diagnostic_log_path_.c_str(),
                                std::ios::out | std::ios::trunc);
    if (!diagnostic_log_stream_.is_open()) {
      ROS_WARN_STREAM("File error: failed to open diagnostic log file "
                      << resolved_diagnostic_log_path_);
      return;
    }

    ROS_INFO_STREAM("Diagnostic log file: " << resolved_diagnostic_log_path_);
    diagnostic_log_stream_
        << "# absolute_time,left_wheel_time,right_wheel_time,"
        << "left_wheel_time_delta,right_wheel_time_delta,"
        << "left_encoder,right_encoder,left_encoder_delta,right_encoder_delta,"
        << "reported_left_wheel_speed,reported_right_wheel_speed,"
        << "odom_linear,odom_angular,"
        << "imu_wz,raw_frame_hex\n";
    diagnostic_log_stream_.flush();
  }

  void ResetDiagnosticMetrics() {
    const double nan = std::numeric_limits<double>::quiet_NaN();
    diag_absolute_time_ = 0U;
    diag_left_wheel_time_ = 0U;
    diag_right_wheel_time_ = 0U;
    diag_left_wheel_time_delta_ = 0U;
    diag_right_wheel_time_delta_ = 0U;
    diag_left_encoder_ = 0U;
    diag_right_encoder_ = 0U;
    diag_left_encoder_step_ = 0;
    diag_right_encoder_step_ = 0;
    diag_reported_left_wheel_speed_ = nan;
    diag_reported_right_wheel_speed_ = nan;
    diag_odom_linear_ = nan;
    diag_odom_angular_ = nan;
    diag_raw_frame_hex_.clear();
  }

  void LogDiagnosticFrame() {
    if (!enable_diagnostic_log_ || !diagnostic_log_stream_.is_open()) {
      return;
    }

    const double imu_wz = has_imu_sample_
                              ? last_imu_angular_z_
                              : std::numeric_limits<double>::quiet_NaN();

    diagnostic_log_stream_ << diag_absolute_time_ << ","
                           << diag_left_wheel_time_ << ","
                           << diag_right_wheel_time_ << ","
                           << diag_left_wheel_time_delta_ << ","
                           << diag_right_wheel_time_delta_ << ","
                           << diag_left_encoder_ << ","
                           << diag_right_encoder_ << ","
                           << diag_left_encoder_step_ << ","
                           << diag_right_encoder_step_ << ","
                           << std::fixed << std::setprecision(6)
                           << diag_reported_left_wheel_speed_ << ","
                           << diag_reported_right_wheel_speed_ << ","
                           << std::fixed << std::setprecision(6)
                           << diag_odom_linear_ << ","
                           << diag_odom_angular_ << ","
                           << std::fixed << std::setprecision(6) << imu_wz << ","
                           << diag_raw_frame_hex_ << "\n";
    diagnostic_log_stream_.flush();
  }

  void WarnEvery(const std::string& msg) {
    const ros::Time now = ros::Time::now();
    if (last_warn_stamp_.isZero() ||
        (now - last_warn_stamp_).toSec() >= warn_interval_sec_) {
      ROS_WARN_STREAM(msg);
      last_warn_stamp_ = now;
    }
  }

  std::string InputEndpointSummary() const {
    if (transport_type_ == OdomTransportType::kSerial) {
      return serial_device_ + " (baud=" + std::to_string(serial_baudrate_) + ")";
    }
    return server_ip_ + ":" + std::to_string(server_port_);
  }

 private:
  ros::NodeHandle nh_;
  ros::NodeHandle pnh_;
  ros::Publisher odom_pub_;
  ros::Subscriber imu_sub_;
  ros::Timer odom_publish_timer_;
  tf2_ros::TransformBroadcaster tf_broadcaster_;

  std::string transport_type_name_ = "tcp";
  OdomTransportType transport_type_ = OdomTransportType::kTcp;
  std::string server_ip_;
  int server_port_ = 5001;
  std::string serial_device_ = "/dev/odom";
  int serial_baudrate_ = 115200;
  std::string publish_topic_;
  std::string resolved_publish_topic_;
  std::string frame_id_;
  std::string child_frame_id_;
  bool publish_odom_tf_ = true;
  bool enable_odom_rx_ = true;
  bool enable_odom_publish_ = true;
  std::string publish_mode_name_ = "fixed_rate";
  OdomPublishMode publish_mode_ = OdomPublishMode::kFixedRate;
  double publish_rate_hz_ = 50.0;
  double stale_timeout_sec_ = 0.2;
  int max_repeat_publish_count_ = 10;
  double reconnect_interval_sec_ = 1.0;
  double warn_interval_sec_ = 5.0;
  double loop_rate_hz_ = 100.0;
  bool enable_imu_diagnostic_ = true;
  std::string imu_topic_ = "/imu";
  std::string wheel_speed_field_order_name_ = "left_right";
  WheelSpeedFieldOrder wheel_speed_field_order_ = WheelSpeedFieldOrder::kLeftRight;
  double wheel_speed_unit_to_mps_ = 0.001;
  double left_wheel_speed_scale_ = 1.0;
  double right_wheel_speed_scale_ = 1.0;
  double wheel_separation_ = 0.725;
  std::string odom_time_source_name_ = "wheel_time";
  OdomTimeSource odom_time_source_ = OdomTimeSource::kWheelTime;
  std::string wheel_time_mode_name_ = "average";
  WheelTimeMode wheel_time_mode_ = WheelTimeMode::kAverage;
  double wheel_time_unit_to_sec_ = 0.001;
  double absolute_time_unit_to_sec_ = 0.001;
  double min_valid_odom_dt_sec_ = 0.005;
  double max_valid_odom_dt_sec_ = 0.06;
  double max_abs_linear_speed_ = 0.0;
  double max_abs_angular_speed_ = 0.0;
  double max_abs_linear_accel_ = 3.0;
  double max_abs_angular_accel_ = 6.0;
  std::vector<double> odom_pose_covariance_diagonal_ = {0.05, 0.05, 1000000.0,
                                                        1000000.0, 1000000.0, 0.1};
  std::vector<double> odom_twist_covariance_diagonal_ = {0.02, 1000000.0, 1000000.0,
                                                         1000000.0, 1000000.0, 0.05};
  std::array<double, 36> odom_pose_covariance_{};
  std::array<double, 36> odom_twist_covariance_{};

  bool enable_diagnostic_log_ = true;
  std::string diagnostic_log_path_;
  std::string resolved_diagnostic_log_path_;
  std::ofstream diagnostic_log_stream_;
  bool has_diagnostic_sample_ = false;
  uint32_t last_left_encoder_ = 0U;
  uint32_t last_right_encoder_ = 0U;
  uint32_t last_left_wheel_time_ = 0U;
  uint32_t last_right_wheel_time_ = 0U;
  bool has_absolute_time_sample_ = false;
  uint32_t last_absolute_time_ = 0U;
  bool has_wheel_time_reference_ = false;
  uint32_t last_time_left_wheel_time_ = 0U;
  uint32_t last_time_right_wheel_time_ = 0U;
  ros::Time last_odom_stamp_;

  int input_fd_ = -1;
  std::vector<uint8_t> rx_buffer_;

  double x_ = 0.0;
  double y_ = 0.0;
  double yaw_ = 0.0;
  bool has_motion_sample_ = false;
  ros::Time last_motion_stamp_;
  double current_odom_linear_velocity_ = 0.0;
  double current_odom_angular_velocity_ = 0.0;
  nav_msgs::Odometry latest_odom_msg_;
  bool has_latest_odom_msg_ = false;
  ros::Time latest_odom_update_wall_time_;
  uint64_t latest_odom_generation_ = 0U;
  uint64_t last_published_odom_generation_ = 0U;
  int latest_odom_repeat_publish_count_ = 0;

  bool has_imu_sample_ = false;
  double last_imu_angular_z_ = 0.0;

  ros::Time connected_since_;
  ros::Time last_valid_frame_stamp_;
  ros::Time last_data_frame_stamp_;
  ros::Time last_no_data_warn_stamp_;
  ros::Time last_warn_stamp_;
  uint64_t rx_total_bytes_since_connect_ = 0;
  uint64_t valid_frame_count_since_connect_ = 0;
  uint64_t data_frame_count_since_connect_ = 0;
  uint64_t foreign_frame_count_since_connect_ = 0;
  uint64_t parse_error_count_since_connect_ = 0;
  uint64_t fallback_frame_count_since_connect_ = 0;
  uint64_t fast_resync_count_since_connect_ = 0;
  uint64_t fast_resync_dropped_bytes_since_connect_ = 0;
  uint64_t timing_reject_count_since_connect_ = 0;
  uint64_t motion_reject_count_since_connect_ = 0;

  uint32_t diag_absolute_time_ = 0U;
  uint32_t diag_left_wheel_time_ = 0U;
  uint32_t diag_right_wheel_time_ = 0U;
  uint32_t diag_left_wheel_time_delta_ = 0U;
  uint32_t diag_right_wheel_time_delta_ = 0U;
  uint32_t diag_left_encoder_ = 0U;
  uint32_t diag_right_encoder_ = 0U;
  int64_t diag_left_encoder_step_ = 0;
  int64_t diag_right_encoder_step_ = 0;
  double diag_reported_left_wheel_speed_ = 0.0;
  double diag_reported_right_wheel_speed_ = 0.0;
  double diag_odom_linear_ = 0.0;
  double diag_odom_angular_ = 0.0;
  std::string diag_raw_frame_hex_;
};

int main(int argc, char** argv) {
  ros::init(argc, argv, "wheel_speed_odom");
  WheelSpeedOdomNode node;
  node.Run();
  return 0;
}

#include <errno.h>
#include <fcntl.h>
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

constexpr size_t kStampMsOffset = 0;
constexpr size_t kLeftEncoderCountOffset = 4;
constexpr size_t kRightEncoderCountOffset = 8;
constexpr size_t kSeqOffset = 20;
constexpr size_t kStatusOffset = 24;
constexpr size_t kPayloadBytes = 28;

constexpr double kTimeJumpWarnSec = 10.0;
constexpr uint32_t kStatusLeftValidMask = 1u << 0;
constexpr uint32_t kStatusRightValidMask = 1u << 1;
constexpr uint32_t kStatusLeftTimeoutMask = 1u << 2;
constexpr uint32_t kStatusRightTimeoutMask = 1u << 3;
constexpr uint32_t kStatusSampleSkewMask = 1u << 4;

enum class MotionGuardMode {
  kReject,
  kWarnOnly,
  kOff,
};

struct ProtocolTimestampState {
  bool initialized = false;
  uint32_t last_raw = 0U;
  uint64_t last_unwrapped_raw = 0U;
  double offset_sec = 0.0;
  ros::Time last_stamp;
  uint64_t resync_count = 0U;
};

uint32_t ReadUInt32Le(const uint8_t* p) {
  return static_cast<uint32_t>(p[0]) |
         (static_cast<uint32_t>(p[1]) << 8) |
         (static_cast<uint32_t>(p[2]) << 16) |
         (static_cast<uint32_t>(p[3]) << 24);
}

int32_t ReadInt32Le(const uint8_t* p) {
  const uint32_t raw = ReadUInt32Le(p);
  int32_t value = 0;
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

int64_t ComputeInt32WrappedDelta(int32_t current, int32_t previous) {
  const uint32_t current_raw = static_cast<uint32_t>(current);
  const uint32_t previous_raw = static_cast<uint32_t>(previous);
  return ComputeUInt32SignedDelta(current_raw, previous_raw);
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

std::string FormatStatusFlags(uint32_t status) {
  std::vector<std::string> flags;
  if ((status & kStatusLeftValidMask) == 0U) {
    flags.emplace_back("left_invalid");
  }
  if ((status & kStatusRightValidMask) == 0U) {
    flags.emplace_back("right_invalid");
  }
  if ((status & kStatusLeftTimeoutMask) != 0U) {
    flags.emplace_back("left_timeout");
  }
  if ((status & kStatusRightTimeoutMask) != 0U) {
    flags.emplace_back("right_timeout");
  }
  if ((status & kStatusSampleSkewMask) != 0U) {
    flags.emplace_back("sample_skew_gt_15ms");
  }
  if (flags.empty()) {
    return "ok";
  }

  std::ostringstream oss;
  for (size_t i = 0; i < flags.size(); ++i) {
    if (i > 0) {
      oss << "|";
    }
    oss << flags[i];
  }
  return oss.str();
}

}  // namespace

class WheelSpeedOdomNode {
 public:
  WheelSpeedOdomNode() : nh_(), pnh_("~") {
    pnh_.param<std::string>("serial_device", serial_device_, "/dev/odom");
    pnh_.param<int>("serial_baudrate", serial_baudrate_, 115200);
    pnh_.param<std::string>("publish_topic", publish_topic_, "/odom");
    pnh_.param<std::string>("frame_id", frame_id_, "odom");
    pnh_.param<std::string>("child_frame_id", child_frame_id_, "base_footprint");
    pnh_.param<bool>("publish_odom_tf", publish_odom_tf_, true);
    pnh_.param<bool>("enable_odom_rx", enable_odom_rx_, true);
    pnh_.param<bool>("enable_odom_publish", enable_odom_publish_, true);
    pnh_.param<double>("reconnect_interval_sec", reconnect_interval_sec_, 1.0);
    pnh_.param<double>("warn_interval_sec", warn_interval_sec_, 5.0);
    pnh_.param<double>("loop_rate_hz", loop_rate_hz_, 100.0);
    pnh_.param<bool>("enable_imu_diagnostic", enable_imu_diagnostic_, true);
    pnh_.param<std::string>("imu_topic", imu_topic_, "/imu");
    pnh_.param<bool>("use_device_timestamp", use_device_timestamp_, true);

    pnh_.param<double>("angular_velocity_sign", angular_velocity_sign_, 1.0);
    pnh_.param<double>("wheel_separation", wheel_separation_, 0.725);
    pnh_.param<double>("encoder_distance_per_count", encoder_distance_per_count_, 0.0);
    pnh_.param<double>("left_wheel_scale", left_wheel_scale_, 1.0);
    pnh_.param<double>("right_wheel_scale", right_wheel_scale_, 1.0);
    pnh_.param<double>("wheel_diameter", wheel_diameter_, 0.18);
    pnh_.param<double>("gear_ratio", gear_ratio_, 1.0);
    pnh_.param<double>("encoder_pulses_per_motor_revolution",
                       encoder_pulses_per_motor_revolution_, 1.0);
    pnh_.param<double>("max_abs_linear_speed", max_abs_linear_speed_, 2.0);
    pnh_.param<double>("max_abs_angular_speed", max_abs_angular_speed_, 1.5);
    pnh_.param<double>("max_abs_linear_accel", max_abs_linear_accel_, 3.0);
    pnh_.param<double>("max_abs_angular_accel", max_abs_angular_accel_, 6.0);
    pnh_.param<std::string>("motion_guard_mode", motion_guard_mode_name_, "reject");
    pnh_.param<double>("stamp_unit_to_sec", stamp_unit_to_sec_, 0.001);
    pnh_.param<double>("stamp_resync_threshold_sec", stamp_resync_threshold_sec_, 0.5);
    pnh_.param<double>("min_valid_odom_dt_sec", min_valid_odom_dt_sec_, 0.005);
    pnh_.param<double>("max_valid_odom_dt_sec", max_valid_odom_dt_sec_, 0.06);

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

    motion_guard_mode_name_ = ToLowerCopy(motion_guard_mode_name_);
    if (motion_guard_mode_name_ == "warn_only") {
      motion_guard_mode_ = MotionGuardMode::kWarnOnly;
    } else if (motion_guard_mode_name_ == "off") {
      motion_guard_mode_ = MotionGuardMode::kOff;
    } else {
      if (motion_guard_mode_name_ != "reject") {
        ROS_WARN_STREAM("Invalid parameter: motion_guard_mode must be reject, warn_only or "
                        "off. Fallback to reject.");
      }
      motion_guard_mode_name_ = "reject";
      motion_guard_mode_ = MotionGuardMode::kReject;
    }

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
    if (!std::isfinite(angular_velocity_sign_) || angular_velocity_sign_ == 0.0) {
      ROS_WARN_STREAM(
          "Invalid parameter: angular_velocity_sign must be non-zero. Fallback to 1.0.");
      angular_velocity_sign_ = 1.0;
    }
    if (!std::isfinite(wheel_diameter_) || wheel_diameter_ <= 0.0) {
      ROS_WARN_STREAM("Invalid parameter: wheel_diameter must be > 0. Fallback to 0.18.");
      wheel_diameter_ = 0.18;
    }
    if (!std::isfinite(gear_ratio_) || gear_ratio_ <= 0.0) {
      ROS_WARN_STREAM("Invalid parameter: gear_ratio must be > 0. Fallback to 1.0.");
      gear_ratio_ = 1.0;
    }
    if (!std::isfinite(encoder_pulses_per_motor_revolution_) ||
        encoder_pulses_per_motor_revolution_ <= 0.0) {
      ROS_WARN_STREAM("Invalid parameter: encoder_pulses_per_motor_revolution must be > 0. "
                      "Fallback to 1.0.");
      encoder_pulses_per_motor_revolution_ = 1.0;
    }
    if (!std::isfinite(wheel_separation_) || wheel_separation_ <= 0.0) {
      ROS_WARN_STREAM(
          "Invalid parameter: wheel_separation must be > 0. Fallback to 0.725.");
      wheel_separation_ = 0.725;
    }
    if (!std::isfinite(encoder_distance_per_count_) || encoder_distance_per_count_ < 0.0) {
      ROS_WARN_STREAM("Invalid parameter: encoder_distance_per_count must be >= 0. "
                      "Fallback to auto-compute from wheel_diameter / gear_ratio / "
                      "encoder_pulses_per_motor_revolution.");
      encoder_distance_per_count_ = 0.0;
    }
    if (!std::isfinite(left_wheel_scale_) || left_wheel_scale_ <= 0.0) {
      ROS_WARN_STREAM("Invalid parameter: left_wheel_scale must be > 0. Fallback to 1.0.");
      left_wheel_scale_ = 1.0;
    }
    if (!std::isfinite(right_wheel_scale_) || right_wheel_scale_ <= 0.0) {
      ROS_WARN_STREAM("Invalid parameter: right_wheel_scale must be > 0. Fallback to 1.0.");
      right_wheel_scale_ = 1.0;
    }
    if (!std::isfinite(stamp_unit_to_sec_) || stamp_unit_to_sec_ <= 0.0) {
      ROS_WARN_STREAM("Invalid parameter: stamp_unit_to_sec must be > 0. Fallback to 0.001.");
      stamp_unit_to_sec_ = 0.001;
    }
    if (!std::isfinite(stamp_resync_threshold_sec_) || stamp_resync_threshold_sec_ <= 0.0) {
      ROS_WARN_STREAM(
          "Invalid parameter: stamp_resync_threshold_sec must be > 0. Fallback to 0.5.");
      stamp_resync_threshold_sec_ = 0.5;
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
    max_abs_linear_speed_ = std::max(0.0, max_abs_linear_speed_);
    max_abs_angular_speed_ = std::max(0.0, max_abs_angular_speed_);
    max_abs_linear_accel_ = std::max(0.0, max_abs_linear_accel_);
    max_abs_angular_accel_ = std::max(0.0, max_abs_angular_accel_);

    ResolveDiagnosticLogPath();
    OpenDiagnosticLog();
    ResetDiagnosticMetrics();

    if (enable_odom_publish_) {
      odom_pub_ = nh_.advertise<nav_msgs::Odometry>(publish_topic_, 50);
    }
    if (enable_imu_diagnostic_) {
      imu_sub_ = nh_.subscribe(imu_topic_, 200, &WheelSpeedOdomNode::ImuCallback, this);
      ROS_INFO_STREAM("IMU diagnostic topic: " << imu_topic_);
    }
    ROS_INFO_STREAM("odom rx enabled: " << (enable_odom_rx_ ? "true" : "false")
                    << ", odom publish enabled: " << (enable_odom_publish_ ? "true" : "false"));
    ROS_INFO_STREAM("odom serial endpoint=" << InputEndpointSummary());
    if (enable_odom_publish_) {
      ROS_INFO_STREAM("odom publish topic: " << resolved_publish_topic_);
      ROS_INFO_STREAM("odom TF publish enabled: " << (publish_odom_tf_ ? "true" : "false"));
      ROS_INFO_STREAM("raw odom publish policy=publish once per accepted frame");
    }
    ROS_INFO_STREAM("raw odom protocol=header 43 4C, cmd 40 01, payload="
                    << "[last_wheel_update_ms,left_encoder_count,right_encoder_count,left_speed,"
                    << "right_speed,seq,status], checksum+tail unchanged");
    ROS_INFO_STREAM("timestamp sync params: use_device_timestamp="
                    << (use_device_timestamp_ ? "true" : "false")
                    << ", stamp_unit_to_sec=" << stamp_unit_to_sec_
                    << ", stamp_resync_threshold_sec=" << stamp_resync_threshold_sec_);
    ROS_INFO_STREAM("encoder odom params: angular_velocity_sign=" << angular_velocity_sign_
                    << ", wheel_separation=" << wheel_separation_
                    << ", encoder_distance_per_count=" << EncoderDistancePerCount()
                    << ", encoder_distance_source="
                    << (encoder_distance_per_count_ > 0.0 ? "launch_override"
                                                         : "wheel_diameter/gear_ratio/ppr")
                    << ", left_wheel_scale=" << left_wheel_scale_
                    << ", right_wheel_scale=" << right_wheel_scale_
                    << ", wheel_diameter=" << wheel_diameter_
                    << ", gear_ratio=" << gear_ratio_
                    << ", encoder_pulses_per_motor_revolution="
                    << encoder_pulses_per_motor_revolution_
                    << ", min_valid_odom_dt_sec=" << min_valid_odom_dt_sec_
                    << ", max_valid_odom_dt_sec=" << max_valid_odom_dt_sec_
                    << ", max_abs_linear_speed=" << max_abs_linear_speed_
                    << ", max_abs_angular_speed=" << max_abs_angular_speed_
                    << ", max_abs_linear_accel=" << max_abs_linear_accel_
                    << ", max_abs_angular_accel=" << max_abs_angular_accel_
                    << ", motion_guard_mode=" << motion_guard_mode_name_);
    ROS_INFO_STREAM("raw odom integration source=encoder_count delta with shared stamp_ms;"
                    << " left_speed/right_speed fields are ignored for odom integration.");
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
        ROS_INFO_STREAM("Opened odom serial input: " << InputEndpointSummary());
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
    if (!OpenSerial()) {
      return false;
    }
    ResetInputSessionState();
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
    foreign_frame_count_since_connect_ = 0;
    parse_error_count_since_connect_ = 0;
    fallback_frame_count_since_connect_ = 0;
    fast_resync_count_since_connect_ = 0;
    fast_resync_dropped_bytes_since_connect_ = 0;
    timing_reject_count_since_connect_ = 0;
    status_reject_count_since_connect_ = 0;
    motion_reject_count_since_connect_ = 0;
    has_diagnostic_sample_ = false;
    protocol_timestamp_state_ = ProtocolTimestampState();
    last_left_encoder_count_ = 0;
    last_right_encoder_count_ = 0;
    last_diag_stamp_ms_ = 0U;
    has_motion_sample_ = false;
    last_motion_stamp_ = ros::Time();
    last_motion_left_encoder_count_ = 0;
    last_motion_right_encoder_count_ = 0;
    current_odom_linear_velocity_ = 0.0;
    current_odom_angular_velocity_ = 0.0;
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
      const ssize_t n = read(input_fd_, chunk, sizeof(chunk));
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
        return;
      }

      if (errno == EINTR) {
        continue;
      }
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        return;
      }

      WarnEvery(std::string("Serial error: read() failed: ") + std::strerror(errno));
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

  std::string FormatUint32Hex(uint32_t value) const {
    std::ostringstream oss;
    oss << "0x" << std::uppercase << std::hex << std::setw(8) << std::setfill('0')
        << static_cast<unsigned long>(value);
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
      if (data_len >= kPayloadBytes) {
        score += 3;
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
    const uint8_t* data = rx_buffer_.data() + 6;
    ResetDiagnosticMetrics();
    diag_raw_frame_hex_ = FormatBytesHex(rx_buffer_.data(), frame_len);
    ++data_frame_count_since_connect_;
    last_data_frame_stamp_ = now;
    ProcessEncoderCountFrame(now, data, data_len);
  }

  void ProcessEncoderCountFrame(const ros::Time& now, const uint8_t* data, size_t data_len) {
    if (data == nullptr || data_len < kPayloadBytes) {
      ++parse_error_count_since_connect_;
      WarnEvery("Frame parse error: payload too short for encoder_count mode. data_len=" +
                std::to_string(data_len) + ", required_min=" +
                std::to_string(kPayloadBytes) + " bytes.");
      return;
    }

    const uint32_t stamp_ms = ReadUInt32Le(data + kStampMsOffset);
    const int32_t left_encoder_count = ReadInt32Le(data + kLeftEncoderCountOffset);
    const int32_t right_encoder_count = ReadInt32Le(data + kRightEncoderCountOffset);
    const uint32_t seq = ReadUInt32Le(data + kSeqOffset);
    const uint32_t status = ReadUInt32Le(data + kStatusOffset);

    diag_stamp_ms_ = stamp_ms;
    diag_left_encoder_count_ = left_encoder_count;
    diag_right_encoder_count_ = right_encoder_count;
    diag_seq_ = seq;
    diag_status_ = status;
    diag_status_flags_ = FormatStatusFlags(status);
    UpdateEncoderDiagnosticDeltas(stamp_ms, left_encoder_count, right_encoder_count);

    ros::Time odom_stamp;
    if (!ResolveOdomStamp(now, stamp_ms, &odom_stamp)) {
      ResetMotionInterval();
      RememberEncoderDiagnosticSample(stamp_ms, left_encoder_count, right_encoder_count);
      LogDiagnosticFrame();
      return;
    }

    if (!HasUsableEncoderStatus(status)) {
      ResetMotionInterval();
      RememberEncoderDiagnosticSample(stamp_ms, left_encoder_count, right_encoder_count);
      LogDiagnosticFrame();
      return;
    }

    UpdateWheelEncoderOdom(odom_stamp, left_encoder_count, right_encoder_count);
    RememberEncoderDiagnosticSample(stamp_ms, left_encoder_count, right_encoder_count);
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

  bool TryResolveStampDeltaSec(uint32_t stamp_ms_raw, double* delta_sec) {
    if (delta_sec == nullptr || !protocol_timestamp_state_.initialized) {
      return false;
    }

    const int64_t stamp_delta_signed =
        ComputeUInt32SignedDelta(stamp_ms_raw, protocol_timestamp_state_.last_raw);
    if (stamp_delta_signed <= 0) {
      ++parse_error_count_since_connect_;
      WarnEvery("Odom timestamp warning: non-forward stamp_ms received. stamp_ms_raw=" +
                std::to_string(stamp_ms_raw) + ", last_stamp_ms=" +
                std::to_string(protocol_timestamp_state_.last_raw) + ", delta_signed=" +
                std::to_string(stamp_delta_signed) + ".");
      return false;
    }

    const uint32_t stamp_delta_raw = static_cast<uint32_t>(stamp_delta_signed);
    if (!TryConvertRawTimeDeltaToSec(stamp_delta_raw, stamp_unit_to_sec_, delta_sec)) {
      ++parse_error_count_since_connect_;
      WarnEvery("Odom timestamp warning: abnormal stamp_ms jump. stamp_ms_raw=" +
                std::to_string(stamp_ms_raw) + ", last_stamp_ms=" +
                std::to_string(protocol_timestamp_state_.last_raw) + ", delta_raw=" +
                std::to_string(stamp_delta_raw) + ".");
      return false;
    }
    return true;
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

  void SetProtocolTimestampState(uint32_t stamp_ms_raw, uint64_t unwrapped_raw,
                                 double offset_sec, const ros::Time& stamp) {
    protocol_timestamp_state_.initialized = true;
    protocol_timestamp_state_.last_raw = stamp_ms_raw;
    protocol_timestamp_state_.last_unwrapped_raw = unwrapped_raw;
    protocol_timestamp_state_.offset_sec = offset_sec;
    protocol_timestamp_state_.last_stamp = stamp;
  }

  void InitializeProtocolTimestampState(uint32_t stamp_ms_raw, const ros::Time& host_stamp) {
    const double sensor_time_sec = static_cast<double>(stamp_ms_raw) * stamp_unit_to_sec_;
    SetProtocolTimestampState(stamp_ms_raw, static_cast<uint64_t>(stamp_ms_raw),
                              host_stamp.toSec() - sensor_time_sec, host_stamp);
  }

  void ResyncProtocolTimestampState(uint32_t stamp_ms_raw, const ros::Time& host_stamp) {
    InitializeProtocolTimestampState(stamp_ms_raw, host_stamp);
    ++protocol_timestamp_state_.resync_count;
  }

  bool ResolveOdomStamp(const ros::Time& fallback_stamp, uint32_t stamp_ms_raw,
                        ros::Time* resolved_stamp) {
    if (resolved_stamp == nullptr) {
      return false;
    }
    if (!use_device_timestamp_) {
      *resolved_stamp = fallback_stamp;
      return true;
    }

    const double initial_sensor_time_sec =
        static_cast<double>(stamp_ms_raw) * stamp_unit_to_sec_;
    if (!std::isfinite(initial_sensor_time_sec)) {
      *resolved_stamp = fallback_stamp;
      return false;
    }

    if (!protocol_timestamp_state_.initialized) {
      *resolved_stamp = fallback_stamp;
      InitializeProtocolTimestampState(stamp_ms_raw, *resolved_stamp);
      return true;
    }

    double time_delta_sec = 0.0;
    if (!TryResolveStampDeltaSec(stamp_ms_raw, &time_delta_sec)) {
      ++timing_reject_count_since_connect_;
      *resolved_stamp = fallback_stamp;
      ResyncProtocolTimestampState(stamp_ms_raw, *resolved_stamp);
      WarnEvery("Odom timestamp reject: protocol stamp_ms invalid. Resynced time base "
                "without integrating this frame.");
      return false;
    }

    if (!IsAcceptedOdomDeltaSec(time_delta_sec)) {
      ++timing_reject_count_since_connect_;
      *resolved_stamp = fallback_stamp;
      ResyncProtocolTimestampState(stamp_ms_raw, *resolved_stamp);
      WarnEvery("Odom timestamp reject: dt out of accepted range. dt_sec=" +
                FormatDouble(time_delta_sec, 6) + ", min_valid_odom_dt_sec=" +
                FormatDouble(min_valid_odom_dt_sec_, 6) + ", max_valid_odom_dt_sec=" +
                FormatDouble(max_valid_odom_dt_sec_, 6) +
                ". Resynced time base without integrating this frame.");
      return false;
    }

    const uint32_t stamp_delta_raw =
        ComputeUInt32ForwardDelta(stamp_ms_raw, protocol_timestamp_state_.last_raw);
    const uint64_t unwrapped_raw =
        protocol_timestamp_state_.last_unwrapped_raw + static_cast<uint64_t>(stamp_delta_raw);
    const double sensor_time_sec = static_cast<double>(unwrapped_raw) * stamp_unit_to_sec_;
    const double observed_offset_sec = fallback_stamp.toSec() - sensor_time_sec;
    if (!std::isfinite(sensor_time_sec) || !std::isfinite(observed_offset_sec)) {
      ++timing_reject_count_since_connect_;
      *resolved_stamp = fallback_stamp;
      ResyncProtocolTimestampState(stamp_ms_raw, *resolved_stamp);
      WarnEvery("Odom timestamp reject: mapped sensor time became non-finite. "
                "Resynced time base without integrating this frame.");
      return false;
    }

    const double offset_delta_sec =
        std::fabs(observed_offset_sec - protocol_timestamp_state_.offset_sec);
    if (offset_delta_sec > stamp_resync_threshold_sec_) {
      ++timing_reject_count_since_connect_;
      *resolved_stamp = fallback_stamp;
      ResyncProtocolTimestampState(stamp_ms_raw, *resolved_stamp);
      WarnEvery("Odom timestamp reject: mapped offset jumped by " +
                FormatDouble(offset_delta_sec, 6) + " s, threshold=" +
                FormatDouble(stamp_resync_threshold_sec_, 6) +
                ". Resynced time base without integrating this frame.");
      return false;
    }

    const double mapped_offset_sec =
        std::min(protocol_timestamp_state_.offset_sec, observed_offset_sec);
    resolved_stamp->fromSec(sensor_time_sec + mapped_offset_sec);
    if (*resolved_stamp > fallback_stamp) {
      *resolved_stamp = fallback_stamp;
    }

    if (*resolved_stamp <= protocol_timestamp_state_.last_stamp) {
      ++timing_reject_count_since_connect_;
      *resolved_stamp = fallback_stamp;
      ResyncProtocolTimestampState(stamp_ms_raw, *resolved_stamp);
      WarnEvery("Odom timestamp reject: mapped stamp became non-monotonic. "
                "Resynced time base without integrating this frame.");
      return false;
    }

    SetProtocolTimestampState(stamp_ms_raw, unwrapped_raw, mapped_offset_sec, *resolved_stamp);
    return true;
  }

  void UpdateEncoderDiagnosticDeltas(uint32_t stamp_ms_raw, int32_t left_encoder_raw,
                                     int32_t right_encoder_raw) {
    if (!has_diagnostic_sample_) {
      diag_stamp_ms_delta_ = 0U;
      diag_left_encoder_step_ = 0;
      diag_right_encoder_step_ = 0;
      return;
    }

    diag_stamp_ms_delta_ = ComputeUInt32ForwardDelta(stamp_ms_raw, last_diag_stamp_ms_);
    diag_left_encoder_step_ = ComputeInt32WrappedDelta(left_encoder_raw, last_left_encoder_count_);
    diag_right_encoder_step_ = ComputeInt32WrappedDelta(right_encoder_raw, last_right_encoder_count_);
  }

  void RememberEncoderDiagnosticSample(uint32_t stamp_ms_raw, int32_t left_encoder_raw,
                                       int32_t right_encoder_raw) {
    last_left_encoder_count_ = left_encoder_raw;
    last_right_encoder_count_ = right_encoder_raw;
    last_diag_stamp_ms_ = stamp_ms_raw;
    has_diagnostic_sample_ = true;
  }

  bool HandleMotionGuardViolation(const std::string& diagnostic_status,
                                  const std::string& base_message) {
    if (motion_guard_mode_ == MotionGuardMode::kOff) {
      return false;
    }

    if (motion_guard_mode_ == MotionGuardMode::kWarnOnly) {
      diag_motion_guard_status_ = diagnostic_status;
      WarnEvery(base_message + " Accepted because motion_guard_mode=warn_only.");
      return false;
    }

    diag_motion_guard_status_ = diagnostic_status;
    ++parse_error_count_since_connect_;
    ++motion_reject_count_since_connect_;
    WarnEvery(base_message);
    return true;
  }

  bool HasUsableEncoderStatus(uint32_t status_raw) {
    if ((status_raw & kStatusLeftValidMask) == 0U || (status_raw & kStatusRightValidMask) == 0U ||
        (status_raw & kStatusLeftTimeoutMask) != 0U ||
        (status_raw & kStatusRightTimeoutMask) != 0U ||
        (status_raw & kStatusSampleSkewMask) != 0U) {
      ++parse_error_count_since_connect_;
      ++status_reject_count_since_connect_;
      diag_motion_guard_status_ = "reject_status_bits";
      WarnEvery("Wheel encoder odom frame reject: protocol status indicates unusable synchronized "
                "wheel data. status=" + FormatUint32Hex(status_raw) +
                ", flags=" + diag_status_flags_ + ".");
      return false;
    }
    return true;
  }

  void ResetMotionInterval() {
    has_motion_sample_ = false;
    last_motion_stamp_ = ros::Time();
    last_motion_left_encoder_count_ = 0;
    last_motion_right_encoder_count_ = 0;
    current_odom_linear_velocity_ = 0.0;
    current_odom_angular_velocity_ = 0.0;
  }

  void UpdateWheelEncoderOdom(const ros::Time& odom_stamp, int32_t left_encoder_count,
                              int32_t right_encoder_count) {
    diag_motion_dt_sec_ = std::numeric_limits<double>::quiet_NaN();
    diag_linear_accel_ = std::numeric_limits<double>::quiet_NaN();
    diag_angular_accel_ = std::numeric_limits<double>::quiet_NaN();

    if (!has_motion_sample_) {
      has_motion_sample_ = true;
      last_motion_stamp_ = odom_stamp;
      last_motion_left_encoder_count_ = left_encoder_count;
      last_motion_right_encoder_count_ = right_encoder_count;
      current_odom_linear_velocity_ = 0.0;
      current_odom_angular_velocity_ = 0.0;
      diag_odom_linear_ = 0.0;
      diag_odom_angular_ = 0.0;
      diag_motion_guard_status_ = "initialized_encoder_reference";
      HandleOdomStateUpdated(odom_stamp);
      return;
    }

    const double dt_sec = (odom_stamp - last_motion_stamp_).toSec();
    diag_motion_dt_sec_ = dt_sec;
    if (!std::isfinite(dt_sec) || dt_sec <= 0.0) {
      ++motion_reject_count_since_connect_;
      diag_motion_guard_status_ = "reject_invalid_motion_dt";
      WarnEvery("Wheel encoder odom frame reject: invalid motion dt. Frame skipped. dt_sec=" +
                FormatDouble(dt_sec, 6) + ".");
      return;
    }

    const int64_t left_delta_count =
        ComputeInt32WrappedDelta(left_encoder_count, last_motion_left_encoder_count_);
    const int64_t right_delta_count =
        ComputeInt32WrappedDelta(right_encoder_count, last_motion_right_encoder_count_);
    const double distance_per_count = EncoderDistancePerCount();
    const double left_distance =
        static_cast<double>(left_delta_count) * distance_per_count * left_wheel_scale_;
    const double right_distance =
        static_cast<double>(right_delta_count) * distance_per_count * right_wheel_scale_;
    const double delta_s = 0.5 * (left_distance + right_distance);
    const double delta_yaw =
        angular_velocity_sign_ * (right_distance - left_distance) / wheel_separation_;
    const double linear_velocity = delta_s / dt_sec;
    const double angular_velocity = delta_yaw / dt_sec;
    diag_odom_linear_ = linear_velocity;
    diag_odom_angular_ = angular_velocity;
    diag_linear_accel_ = (linear_velocity - current_odom_linear_velocity_) / dt_sec;
    diag_angular_accel_ = (angular_velocity - current_odom_angular_velocity_) / dt_sec;

    if (motion_guard_mode_ != MotionGuardMode::kOff &&
        max_abs_linear_speed_ > 0.0 && std::fabs(linear_velocity) > max_abs_linear_speed_) {
      if (HandleMotionGuardViolation(
              motion_guard_mode_ == MotionGuardMode::kWarnOnly ? "warn_linear_speed_limit"
                                                               : "reject_linear_speed_limit",
              "Encoder-count odom frame error: linear velocity limit exceeded. " +
                  std::string(motion_guard_mode_ == MotionGuardMode::kWarnOnly
                                  ? "Frame accepted"
                                  : "Frame dropped") +
                  ". linear_velocity=" + FormatDouble(linear_velocity) +
                  ", max_abs_linear_speed=" + FormatDouble(max_abs_linear_speed_) + ".")) {
        return;
      }
    }
    if (motion_guard_mode_ != MotionGuardMode::kOff &&
        max_abs_angular_speed_ > 0.0 &&
        std::fabs(angular_velocity) > max_abs_angular_speed_) {
      if (HandleMotionGuardViolation(
              motion_guard_mode_ == MotionGuardMode::kWarnOnly ? "warn_angular_speed_limit"
                                                               : "reject_angular_speed_limit",
              "Encoder-count odom frame error: angular velocity limit exceeded. " +
                  std::string(motion_guard_mode_ == MotionGuardMode::kWarnOnly
                                  ? "Frame accepted"
                                  : "Frame dropped") +
                  ". angular_velocity=" + FormatDouble(angular_velocity) +
                  ", max_abs_angular_speed=" + FormatDouble(max_abs_angular_speed_) + ".")) {
        return;
      }
    }
    if (motion_guard_mode_ != MotionGuardMode::kOff &&
        max_abs_linear_accel_ > 0.0 && std::fabs(diag_linear_accel_) > max_abs_linear_accel_) {
      if (HandleMotionGuardViolation(
              motion_guard_mode_ == MotionGuardMode::kWarnOnly ? "warn_linear_accel_limit"
                                                               : "reject_linear_accel_limit",
              "Encoder-count odom frame reject: linear acceleration limit exceeded. " +
                  std::string(motion_guard_mode_ == MotionGuardMode::kWarnOnly
                                  ? "Frame accepted"
                                  : "Frame skipped") +
                  ". linear_accel=" + FormatDouble(diag_linear_accel_) +
                  ", dt_sec=" + FormatDouble(dt_sec, 6) +
                  ", max_abs_linear_accel=" + FormatDouble(max_abs_linear_accel_) + ".")) {
        return;
      }
    }
    if (motion_guard_mode_ != MotionGuardMode::kOff &&
        max_abs_angular_accel_ > 0.0 &&
        std::fabs(diag_angular_accel_) > max_abs_angular_accel_) {
      if (HandleMotionGuardViolation(
              motion_guard_mode_ == MotionGuardMode::kWarnOnly ? "warn_angular_accel_limit"
                                                               : "reject_angular_accel_limit",
              "Encoder-count odom frame reject: angular acceleration limit exceeded. " +
                  std::string(motion_guard_mode_ == MotionGuardMode::kWarnOnly
                                  ? "Frame accepted"
                                  : "Frame skipped") +
                  ". angular_accel=" + FormatDouble(diag_angular_accel_) +
                  ", dt_sec=" + FormatDouble(dt_sec, 6) +
                  ", max_abs_angular_accel=" +
                  FormatDouble(max_abs_angular_accel_) + ".")) {
        return;
      }
    }

    const double heading = yaw_ + 0.5 * delta_yaw;
    x_ += delta_s * std::cos(heading);
    y_ += delta_s * std::sin(heading);
    yaw_ = NormalizeAngle(yaw_ + delta_yaw);
    current_odom_linear_velocity_ = linear_velocity;
    current_odom_angular_velocity_ = angular_velocity;
    last_motion_stamp_ = odom_stamp;
    last_motion_left_encoder_count_ = left_encoder_count;
    last_motion_right_encoder_count_ = right_encoder_count;
    diag_motion_guard_status_ = "accepted_encoder_delta";
    HandleOdomStateUpdated(odom_stamp);
  }

  double EncoderDistancePerCount() const {
    if (encoder_distance_per_count_ > 0.0) {
      return encoder_distance_per_count_;
    }
    return (kPi * wheel_diameter_) /
           (gear_ratio_ * encoder_pulses_per_motor_revolution_);
  }

  void HandleOdomStateUpdated(const ros::Time& stamp) {
    if (!enable_odom_publish_) {
      return;
    }

    PublishOdomMessage(BuildOdomMessage(stamp));
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
                    << ", status_rejects=" << status_reject_count_since_connect_
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
        << "# time_ms,dt_sec,"
        << "left_encoder_count,right_encoder_count,left_encoder_delta,right_encoder_delta,"
        << "linear_velocity,angular_velocity,imu_angular_velocity_z,raw_frame_hex\n";
    diagnostic_log_stream_.flush();
  }

  void ResetDiagnosticMetrics() {
    const double nan = std::numeric_limits<double>::quiet_NaN();
    diag_stamp_ms_ = 0U;
    diag_stamp_ms_delta_ = 0U;
    diag_left_encoder_count_ = 0;
    diag_right_encoder_count_ = 0;
    diag_left_encoder_step_ = 0;
    diag_right_encoder_step_ = 0;
    diag_seq_ = 0U;
    diag_status_ = 0U;
    diag_status_flags_ = "unknown";
    diag_odom_linear_ = nan;
    diag_odom_angular_ = nan;
    diag_motion_dt_sec_ = nan;
    diag_linear_accel_ = nan;
    diag_angular_accel_ = nan;
    diag_motion_guard_status_ = "not_evaluated";
    diag_raw_frame_hex_.clear();
  }

  void LogDiagnosticFrame() {
    if (!enable_diagnostic_log_ || !diagnostic_log_stream_.is_open()) {
      return;
    }

    const double imu_wz = has_imu_sample_
                              ? last_imu_angular_z_
                              : std::numeric_limits<double>::quiet_NaN();
    const double dt_sec = static_cast<double>(diag_stamp_ms_delta_) * stamp_unit_to_sec_;
    diagnostic_log_stream_ << diag_stamp_ms_ << ","
                           << std::fixed << std::setprecision(6) << dt_sec << ","
                           << diag_left_encoder_count_ << ","
                           << diag_right_encoder_count_ << ","
                           << diag_left_encoder_step_ << ","
                           << diag_right_encoder_step_ << ","
                           << std::fixed << std::setprecision(6)
                           << diag_odom_linear_ << ","
                           << diag_odom_angular_ << ","
                           << imu_wz << ","
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
    return serial_device_ + " (baud=" + std::to_string(serial_baudrate_) + ")";
  }

 private:
  ros::NodeHandle nh_;
  ros::NodeHandle pnh_;
  ros::Publisher odom_pub_;
  ros::Subscriber imu_sub_;
  tf2_ros::TransformBroadcaster tf_broadcaster_;

  std::string serial_device_ = "/dev/odom";
  int serial_baudrate_ = 115200;
  std::string publish_topic_;
  std::string resolved_publish_topic_;
  std::string frame_id_;
  std::string child_frame_id_;
  bool publish_odom_tf_ = true;
  bool enable_odom_rx_ = true;
  bool enable_odom_publish_ = true;
  double reconnect_interval_sec_ = 1.0;
  double warn_interval_sec_ = 5.0;
  double loop_rate_hz_ = 100.0;
  bool enable_imu_diagnostic_ = true;
  std::string imu_topic_ = "/imu";
  bool use_device_timestamp_ = true;
  double angular_velocity_sign_ = 1.0;
  double wheel_diameter_ = 0.18;
  double gear_ratio_ = 1.0;
  double encoder_pulses_per_motor_revolution_ = 1.0;
  double encoder_distance_per_count_ = 0.0;
  double left_wheel_scale_ = 1.0;
  double right_wheel_scale_ = 1.0;
  double wheel_separation_ = 0.725;
  double max_abs_linear_speed_ = 2.0;
  double max_abs_angular_speed_ = 1.5;
  double max_abs_linear_accel_ = 3.0;
  double max_abs_angular_accel_ = 6.0;
  std::string motion_guard_mode_name_ = "reject";
  MotionGuardMode motion_guard_mode_ = MotionGuardMode::kReject;
  double stamp_unit_to_sec_ = 0.001;
  double stamp_resync_threshold_sec_ = 0.5;
  double min_valid_odom_dt_sec_ = 0.005;
  double max_valid_odom_dt_sec_ = 0.06;
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
  int32_t last_left_encoder_count_ = 0;
  int32_t last_right_encoder_count_ = 0;
  uint32_t last_diag_stamp_ms_ = 0U;
  ProtocolTimestampState protocol_timestamp_state_;

  int input_fd_ = -1;
  std::vector<uint8_t> rx_buffer_;

  double x_ = 0.0;
  double y_ = 0.0;
  double yaw_ = 0.0;
  bool has_motion_sample_ = false;
  ros::Time last_motion_stamp_;
  int32_t last_motion_left_encoder_count_ = 0;
  int32_t last_motion_right_encoder_count_ = 0;
  double current_odom_linear_velocity_ = 0.0;
  double current_odom_angular_velocity_ = 0.0;

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
  uint64_t status_reject_count_since_connect_ = 0;
  uint64_t motion_reject_count_since_connect_ = 0;

  uint32_t diag_stamp_ms_ = 0U;
  uint32_t diag_stamp_ms_delta_ = 0U;
  int32_t diag_left_encoder_count_ = 0;
  int32_t diag_right_encoder_count_ = 0;
  int64_t diag_left_encoder_step_ = 0;
  int64_t diag_right_encoder_step_ = 0;
  uint32_t diag_seq_ = 0U;
  uint32_t diag_status_ = 0U;
  std::string diag_status_flags_;
  double diag_odom_linear_ = 0.0;
  double diag_odom_angular_ = 0.0;
  double diag_motion_dt_sec_ = 0.0;
  double diag_linear_accel_ = 0.0;
  double diag_angular_accel_ = 0.0;
  std::string diag_motion_guard_status_;
  std::string diag_raw_frame_hex_;
};

int main(int argc, char** argv) {
  ros::init(argc, argv, "wheel_speed_odom");
  WheelSpeedOdomNode node;
  node.Run();
  return 0;
}

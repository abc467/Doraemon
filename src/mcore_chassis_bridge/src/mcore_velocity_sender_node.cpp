#include <errno.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <fcntl.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <termios.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <limits>
#include <sstream>
#include <string>
#include <vector>

#include <geometry_msgs/Twist.h>
#include <robot_platform_msgs/CleaningParams.h>
#include <robot_platform_msgs/CombinedStatus.h>
#include <robot_platform_msgs/ControlCleanTools.h>
#include <robot_platform_msgs/ControlMotor.h>
#include <robot_platform_msgs/ControlWaterTap.h>
#include <ros/ros.h>
#include <sensor_msgs/BatteryState.h>
#include <std_msgs/Bool.h>
#include <std_msgs/Float32.h>
#include <std_msgs/Int16.h>
#include <std_msgs/UInt16.h>
#include <std_msgs/UInt64.h>
#include <std_msgs/UInt8.h>

#include <mcore_chassis_bridge/velocity_command_units.h>

namespace {

constexpr uint8_t kHead0 = 0x43;
constexpr uint8_t kHead1 = 0x4E;
constexpr uint8_t kTail = 0xDA;
constexpr uint16_t kVelocityCommand = 0x4060;
constexpr uint16_t kStatusCommand = 0x4070;
constexpr uint16_t kBatteryRemainingCommand = 0x6102;
constexpr uint16_t kCleanWaterLevelCommand = 0x610A;
constexpr uint16_t kSewageLevelCommand = 0x610B;
constexpr uint16_t kBatteryChargingCommand = 0x6022;
constexpr uint16_t kMainBrushCommand = 0x6001;
constexpr uint16_t kSideBrushCommand = 0x6002;
constexpr uint16_t kCleanWaterPumpCommand = 0x6003;
constexpr uint16_t kBrushLiftCommand = 0x6004;
constexpr uint16_t kSqueegeeLiftCommand = 0x6005;
constexpr uint16_t kSuctionFanCommand = 0x6006;
constexpr uint16_t kCleanWaterValveCommand = 0x6023;
constexpr uint16_t kSewageValveCommand = 0x6024;
constexpr size_t kVelocityDataBytes = 8;
constexpr size_t kVelocityFrameBytes = kVelocityDataBytes + 8;
constexpr size_t kNoDataFrameBytes = 8;
constexpr size_t kInt16DataBytes = 2;
constexpr size_t kInt16FrameBytes = kInt16DataBytes + 8;
constexpr size_t kMinFrameBytes = 8;
constexpr size_t kMaxFrameBytes = 512;
constexpr size_t kMaxRxBufferBytes = 4096;

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
#ifdef B460800
    case 460800:
      return B460800;
#endif
#ifdef B921600
    case 921600:
      return B921600;
#endif
    default:
      return static_cast<speed_t>(0);
  }
}

void AppendFloat32LittleEndian(float value, uint8_t* out) {
  static_assert(sizeof(float) == 4, "M-core velocity protocol requires 32-bit floats.");
  static_assert(std::numeric_limits<float>::is_iec559,
                "M-core velocity protocol requires IEEE-754 floats.");
  uint32_t raw = 0;
  std::memcpy(&raw, &value, sizeof(raw));
  out[0] = static_cast<uint8_t>(raw & 0xFF);
  out[1] = static_cast<uint8_t>((raw >> 8) & 0xFF);
  out[2] = static_cast<uint8_t>((raw >> 16) & 0xFF);
  out[3] = static_cast<uint8_t>((raw >> 24) & 0xFF);
}

void AppendInt16LittleEndian(int16_t value, uint8_t* out) {
  const uint16_t raw = static_cast<uint16_t>(value);
  out[0] = static_cast<uint8_t>(raw & 0xFF);
  out[1] = static_cast<uint8_t>((raw >> 8) & 0xFF);
}

float ReadFloat32LittleEndian(const uint8_t* data) {
  static_assert(sizeof(float) == 4, "M-core velocity protocol requires 32-bit floats.");
  const uint32_t raw = static_cast<uint32_t>(data[0]) |
                       (static_cast<uint32_t>(data[1]) << 8) |
                       (static_cast<uint32_t>(data[2]) << 16) |
                       (static_cast<uint32_t>(data[3]) << 24);
  float value = 0.0f;
  std::memcpy(&value, &raw, sizeof(value));
  return value;
}

std::array<uint8_t, kVelocityFrameBytes> BuildVelocityFrame(float vx, float wz) {
  std::array<uint8_t, kVelocityFrameBytes> frame{};
  const uint16_t len = static_cast<uint16_t>(frame.size());

  frame[0] = kHead0;
  frame[1] = kHead1;
  frame[2] = static_cast<uint8_t>((len >> 8) & 0xFF);
  frame[3] = static_cast<uint8_t>(len & 0xFF);
  frame[4] = static_cast<uint8_t>((kVelocityCommand >> 8) & 0xFF);
  frame[5] = static_cast<uint8_t>(kVelocityCommand & 0xFF);
  AppendFloat32LittleEndian(vx, &frame[6]);
  AppendFloat32LittleEndian(wz, &frame[10]);

  uint8_t checksum = 0;
  for (size_t i = 0; i < frame.size() - 2; ++i) {
    checksum = static_cast<uint8_t>((checksum + frame[i]) & 0xFF);
  }
  frame[frame.size() - 2] = checksum;
  frame[frame.size() - 1] = kTail;
  return frame;
}

std::array<uint8_t, kNoDataFrameBytes> BuildNoDataFrame(uint16_t command) {
  std::array<uint8_t, kNoDataFrameBytes> frame{};
  const uint16_t len = static_cast<uint16_t>(frame.size());

  frame[0] = kHead0;
  frame[1] = kHead1;
  frame[2] = static_cast<uint8_t>((len >> 8) & 0xFF);
  frame[3] = static_cast<uint8_t>(len & 0xFF);
  frame[4] = static_cast<uint8_t>((command >> 8) & 0xFF);
  frame[5] = static_cast<uint8_t>(command & 0xFF);

  uint8_t checksum = 0;
  for (size_t i = 0; i < frame.size() - 2; ++i) {
    checksum = static_cast<uint8_t>((checksum + frame[i]) & 0xFF);
  }
  frame[frame.size() - 2] = checksum;
  frame[frame.size() - 1] = kTail;
  return frame;
}

std::array<uint8_t, kInt16FrameBytes> BuildInt16Frame(uint16_t command, int16_t value) {
  std::array<uint8_t, kInt16FrameBytes> frame{};
  const uint16_t len = static_cast<uint16_t>(frame.size());

  frame[0] = kHead0;
  frame[1] = kHead1;
  frame[2] = static_cast<uint8_t>((len >> 8) & 0xFF);
  frame[3] = static_cast<uint8_t>(len & 0xFF);
  frame[4] = static_cast<uint8_t>((command >> 8) & 0xFF);
  frame[5] = static_cast<uint8_t>(command & 0xFF);
  AppendInt16LittleEndian(value, &frame[6]);

  uint8_t checksum = 0;
  for (size_t i = 0; i < frame.size() - 2; ++i) {
    checksum = static_cast<uint8_t>((checksum + frame[i]) & 0xFF);
  }
  frame[frame.size() - 2] = checksum;
  frame[frame.size() - 1] = kTail;
  return frame;
}

std::string HexString(const uint8_t* data, size_t len) {
  std::ostringstream oss;
  oss << std::uppercase << std::hex << std::setfill('0');
  for (size_t i = 0; i < len; ++i) {
    if (i > 0) {
      oss << ' ';
    }
    oss << std::setw(2) << static_cast<unsigned int>(data[i]);
  }
  return oss.str();
}

bool IsFiniteTwist(const geometry_msgs::Twist& msg) {
  return std::isfinite(msg.linear.x) && std::isfinite(msg.angular.z);
}

int ClampInt(int value, int lo, int hi) {
  return std::max(lo, std::min(hi, value));
}

int16_t ClampInt16(int value, int lo, int hi) {
  return static_cast<int16_t>(ClampInt(value, lo, hi));
}

uint16_t ReadUInt16LittleEndian(const uint8_t* data) {
  return static_cast<uint16_t>(data[0]) | (static_cast<uint16_t>(data[1]) << 8);
}

std::string Lowercase(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(), [](unsigned char c) {
    return static_cast<char>(std::tolower(c));
  });
  return value;
}

bool IsFrameHead(uint8_t value) {
  return value == 0x43 || value == 0x42;
}

bool IsTrustedTelemetryFrame(const uint8_t* frame, size_t len) {
  if (frame == nullptr || len < kMinFrameBytes || !IsFrameHead(frame[0]) ||
      (frame[1] != 0x4E && frame[1] != 0x4F) || frame[len - 1] != kTail) {
    return false;
  }

  const size_t declared_len =
      (static_cast<size_t>(frame[2]) << 8) | static_cast<size_t>(frame[3]);
  if (declared_len != len) {
    return false;
  }

  uint8_t checksum = 0;
  for (size_t i = 0; i < len - 2; ++i) {
    checksum = static_cast<uint8_t>((checksum + frame[i]) & 0xFF);
  }
  return checksum == frame[len - 2];
}

const char* PositionName(uint8_t pos) {
  switch (pos) {
    case 0:
      return "origin";
    case 1:
      return "in_place";
    case 2:
      return "moving";
    default:
      return "unknown";
  }
}

}  // namespace

class MCoreVelocitySenderNode {
 public:
  MCoreVelocitySenderNode() : nh_(), pnh_("~") {
    pnh_.param<std::string>("transport", transport_, "tcp");
    pnh_.param<std::string>("serial_device", serial_device_, "/dev/mcore");
    pnh_.param<int>("serial_baudrate", serial_baudrate_, 115200);
    pnh_.param<std::string>("tcp_host", tcp_host_, "192.168.127.10");
    pnh_.param<int>("tcp_port", tcp_port_, 8080);
    pnh_.param<double>("tcp_connect_timeout_sec", tcp_connect_timeout_sec_, 2.0);
    pnh_.param<bool>("tcp_no_delay", tcp_no_delay_, true);
    pnh_.param<bool>("tcp_keepalive", tcp_keepalive_, true);
    pnh_.param<std::string>("cmd_vel_topic", cmd_vel_topic_, "/cmd_vel");
    pnh_.param<double>("send_rate_hz", send_rate_hz_, 10.0);
    pnh_.param<double>("cmd_timeout_sec", cmd_timeout_sec_, 0.5);
    pnh_.param<double>("reconnect_interval_sec", reconnect_interval_sec_, 1.0);
    pnh_.param<double>("write_timeout_sec", write_timeout_sec_, 0.05);
    pnh_.param<double>("tx_min_interval_sec", tx_min_interval_sec_, 0.02);
    pnh_.param<double>("linear_velocity_scale", linear_velocity_scale_, 1000.0);
    pnh_.param<double>("angular_velocity_scale", angular_velocity_scale_, 1000.0);
    pnh_.param<double>("linear_velocity_sign", linear_velocity_sign_, 1.0);
    pnh_.param<double>("angular_velocity_sign", angular_velocity_sign_, 1.0);
    pnh_.param<double>("max_abs_linear_velocity", max_abs_linear_velocity_, 0.0);
    pnh_.param<double>("max_abs_angular_velocity", max_abs_angular_velocity_, 0.0);
    pnh_.param<bool>("send_immediately", send_immediately_, true);
    pnh_.param<bool>("repeat_last_cmd_vel", repeat_last_cmd_vel_, false);
    pnh_.param<bool>("send_zero_on_connect", send_zero_on_connect_, true);
    pnh_.param<bool>("send_zero_on_shutdown", send_zero_on_shutdown_, true);
    pnh_.param<bool>("drain_after_write", drain_after_write_, true);
    pnh_.param<bool>("enable_tx_log", enable_tx_log_, true);
    pnh_.param<double>("tx_log_interval_sec", tx_log_interval_sec_, 1.0);
    pnh_.param<bool>("enable_rx_log", enable_rx_log_, true);
    pnh_.param<bool>("enable_rx_raw_log", enable_rx_raw_log_, false);
    pnh_.param<double>("rx_log_interval_sec", rx_log_interval_sec_, 1.0);
    pnh_.param<double>("rx_silence_warn_sec", rx_silence_warn_sec_, 5.0);
    pnh_.param<double>("rx_poll_hz", rx_poll_hz_, 100.0);
    pnh_.param<double>("telemetry_velocity_quiet_sec", telemetry_velocity_quiet_sec_, 0.0);
    pnh_.param<double>("telemetry_control_cooldown_sec",
                       telemetry_control_cooldown_sec_,
                       0.3);
    pnh_.param<bool>("enable_cleaning_control", enable_cleaning_control_, true);
    pnh_.param<std::string>("clean_tools_topic", clean_tools_topic_, "/mcore/control_clean_tools");
    pnh_.param<std::string>("water_tap_topic", water_tap_topic_, "/mcore/control_water_tap");
    pnh_.param<std::string>("vacuum_motor_topic", vacuum_motor_topic_, "/mcore/control_motor");
    pnh_.param<std::string>("cleaning_params_topic", cleaning_params_topic_, "/mcore/cleaning_params/set");
    pnh_.param<std::string>("cleaning_params_state_topic", cleaning_params_state_topic_,
                            "/mcore/cleaning_params/current");
    pnh_.param<std::string>("combined_status_topic", combined_status_topic_,
                            "/combined_status");
    pnh_.param<std::string>("safety_status_bits_topic", safety_status_bits_topic_,
                            "/mcore_velocity_sender/safety_status_bits");
    pnh_.param<std::string>("telemetry_heartbeat_topic", telemetry_heartbeat_topic_,
                            "/mcore_velocity_sender/telemetry_heartbeat");
    pnh_.param<std::string>("battery_state_topic", battery_state_topic_, "/battery_state");
    pnh_.param<std::string>("battery_remaining_topic", battery_remaining_topic_,
                            "/mcore/battery_remaining");
    pnh_.param<std::string>("clean_water_level_topic", clean_water_level_topic_,
                            "/mcore/clean_water_level");
    pnh_.param<std::string>("sewage_level_topic", sewage_level_topic_,
                            "/mcore/sewage_level");
    pnh_.param<bool>("enable_battery_remaining_poll", enable_battery_remaining_poll_, true);
    pnh_.param<double>("battery_remaining_poll_hz", battery_remaining_poll_hz_, 0.2);
    pnh_.param<double>("battery_full_capacity", battery_full_capacity_, 150.0);
    pnh_.param<bool>("enable_water_level_poll", enable_water_level_poll_, true);
    pnh_.param<double>("water_level_poll_hz", water_level_poll_hz_, 0.2);
    pnh_.param<double>("telemetry_poll_interval_sec", telemetry_poll_interval_sec_, 1.3);
    pnh_.param<bool>("enable_status_poll", enable_status_poll_, false);
    pnh_.param<double>("status_poll_hz", status_poll_hz_, 2.0);
    pnh_.param<std::string>("charge_enable_topic", charge_enable_topic_, "/mcore/charge_enable");
    pnh_.param<int>("charge_cmd_repeat", charge_cmd_repeat_, 3);
    pnh_.param<double>("charge_cmd_interval_sec", charge_cmd_interval_sec_, 0.05);
    pnh_.param<bool>("side_brush_follows_main_brush", side_brush_follows_main_brush_, false);
    pnh_.param<bool>("send_cleaning_stop_on_connect", send_cleaning_stop_on_connect_, false);
    pnh_.param<bool>("send_cleaning_stop_on_shutdown", send_cleaning_stop_on_shutdown_, true);
    pnh_.param<bool>("use_height_scrub_for_brush_distance",
                     use_height_scrub_for_brush_distance_, false);
    pnh_.param<int>("main_brush_speed", main_brush_speed_, 0);
    pnh_.param<int>("side_brush_on_value", side_brush_on_value_, 0);
    pnh_.param<int>("side_brush_off_value", side_brush_off_value_, 0);
    pnh_.param<int>("brush_down_distance", brush_down_distance_, 0);
    pnh_.param<int>("brush_home_value", brush_home_value_, -10000);
    pnh_.param<int>("brush_stop_value", brush_stop_value_, -10001);
    pnh_.param<int>("squeegee_forward_value", squeegee_forward_value_, 1);
    pnh_.param<int>("squeegee_backward_value", squeegee_backward_value_, -1);
    pnh_.param<int>("squeegee_stop_value", squeegee_stop_value_, 0);

    transport_ = Lowercase(transport_);
    if (transport_ != "serial" && transport_ != "tcp") {
      ROS_WARN_STREAM("Unsupported M-core transport=\"" << transport_
                      << "\"; falling back to tcp.");
      transport_ = "tcp";
    }
    tcp_port_ = ClampInt(tcp_port_, 1, 65535);
    tcp_connect_timeout_sec_ = std::max(0.1, tcp_connect_timeout_sec_);
    send_rate_hz_ = std::max(1.0, send_rate_hz_);
    reconnect_interval_sec_ = std::max(0.1, reconnect_interval_sec_);
    write_timeout_sec_ = std::max(0.001, write_timeout_sec_);
    tx_min_interval_sec_ = std::max(0.0, tx_min_interval_sec_);
    rx_poll_hz_ = std::max(1.0, rx_poll_hz_);
    telemetry_velocity_quiet_sec_ =
        std::max(0.0, std::min(1.0, telemetry_velocity_quiet_sec_));
    telemetry_control_cooldown_sec_ =
        std::max(0.0, telemetry_control_cooldown_sec_);
    battery_remaining_poll_hz_ = std::max(0.01, battery_remaining_poll_hz_);
    battery_full_capacity_ = std::max(0.001, battery_full_capacity_);
    water_level_poll_hz_ = std::max(0.01, water_level_poll_hz_);
    telemetry_poll_interval_sec_ = std::max(0.2, telemetry_poll_interval_sec_);
    status_poll_hz_ = std::max(0.2, status_poll_hz_);
    charge_cmd_repeat_ = std::max(1, charge_cmd_repeat_);
    charge_cmd_interval_sec_ = std::max(0.0, charge_cmd_interval_sec_);
    main_brush_speed_ = ClampInt(main_brush_speed_, 0, 100);
    side_brush_on_value_ = ClampInt(side_brush_on_value_, 0, 100);
    side_brush_off_value_ = ClampInt(side_brush_off_value_, 0, 100);
    brush_down_distance_ = ClampInt(brush_down_distance_, 0, 1800);
    brush_home_value_ = ClampInt(brush_home_value_, -10000, 1800);
    brush_stop_value_ = ClampInt(brush_stop_value_, -10001, 1800);
    squeegee_forward_value_ = ClampInt(squeegee_forward_value_, -1, 1);
    squeegee_backward_value_ = ClampInt(squeegee_backward_value_, -1, 1);
    squeegee_stop_value_ = ClampInt(squeegee_stop_value_, -1, 1);
    if (!mcore_chassis_bridge::IsValidVelocityConversionConfig(
            linear_velocity_sign_, max_abs_linear_velocity_, linear_velocity_scale_) ||
        !mcore_chassis_bridge::IsValidVelocityConversionConfig(
            angular_velocity_sign_, max_abs_angular_velocity_, angular_velocity_scale_)) {
      throw std::invalid_argument(
          "M-core velocity scale must be finite and > 0, sign must be -1 or +1, "
          "and max_abs velocity must be finite and >= 0");
    }
    cleaning_params_.profile_name = pnh_.param<std::string>("profile_name", "default");
    cleaning_params_.vel_water_pump =
        static_cast<uint8_t>(ClampInt(pnh_.param<int>("vel_water_pump", 0), 0, 100));
    cleaning_params_.vel_water_suction =
        static_cast<uint8_t>(ClampInt(pnh_.param<int>("vel_water_suction", 0), 0, 100));
    cleaning_params_.height_scrub =
        static_cast<uint8_t>(ClampInt(pnh_.param<int>("height_scrub", 0), 0, 255));
    cleaning_params_.main_brush_speed = static_cast<uint8_t>(main_brush_speed_);
    cleaning_params_.side_brush_speed = static_cast<uint8_t>(side_brush_on_value_);
    cleaning_params_.brush_down_distance = static_cast<uint16_t>(brush_down_distance_);
    cleaning_params_.side_brush_enable = side_brush_on_value_ > 0;
    last_height_scrub_ = static_cast<int>(cleaning_params_.height_scrub);

    connected_pub_ = pnh_.advertise<std_msgs::Bool>("connected", 1, true);
    cleaning_params_pub_ =
        nh_.advertise<robot_platform_msgs::CleaningParams>(cleaning_params_state_topic_, 1, true);
    combined_status_pub_ =
        nh_.advertise<robot_platform_msgs::CombinedStatus>(combined_status_topic_, 10);
    // Non-latched by design: consumers must observe a current, full 0x4070
    // frame containing the physical safety byte. Battery/water replies must
    // never make an old emergency-stop value appear fresh.
    safety_status_bits_pub_ = nh_.advertise<std_msgs::UInt8>(safety_status_bits_topic_, 10, false);
    // A heartbeat is emitted only after a checksum-valid response from the
    // current M-core transport. It proves current link liveness even on
    // firmware that omits the optional 0x4070 physical-safety byte.
    telemetry_heartbeat_pub_ =
        nh_.advertise<std_msgs::UInt64>(telemetry_heartbeat_topic_, 10, false);
    battery_pub_ = nh_.advertise<sensor_msgs::BatteryState>(battery_state_topic_, 10);
    battery_remaining_pub_ = nh_.advertise<std_msgs::Float32>(battery_remaining_topic_, 10);
    clean_water_level_pub_ = nh_.advertise<std_msgs::UInt16>(clean_water_level_topic_, 10);
    sewage_level_pub_ = nh_.advertise<std_msgs::UInt16>(sewage_level_topic_, 10);
    battery_msg_.power_supply_technology = sensor_msgs::BatteryState::POWER_SUPPLY_TECHNOLOGY_LION;
    battery_msg_.present = true;
    battery_msg_.current = std::numeric_limits<float>::quiet_NaN();
    battery_msg_.charge = std::numeric_limits<float>::quiet_NaN();
    battery_msg_.capacity = std::numeric_limits<float>::quiet_NaN();
    battery_msg_.design_capacity = std::numeric_limits<float>::quiet_NaN();
    battery_msg_.percentage = std::numeric_limits<float>::quiet_NaN();
    PublishConnected(false, true);
    PublishCleaningParams();

    cmd_vel_sub_ = nh_.subscribe(cmd_vel_topic_, 20, &MCoreVelocitySenderNode::CmdVelCallback, this);
    charge_enable_sub_ = nh_.subscribe(charge_enable_topic_, 10,
                                       &MCoreVelocitySenderNode::ChargeEnableCallback, this);
    if (enable_cleaning_control_) {
      clean_tools_sub_ = nh_.subscribe(clean_tools_topic_, 10,
                                       &MCoreVelocitySenderNode::ControlCleanToolsCallback, this);
      water_tap_sub_ = nh_.subscribe(water_tap_topic_, 10,
                                     &MCoreVelocitySenderNode::ControlWaterTapCallback, this);
      vacuum_motor_sub_ = nh_.subscribe(vacuum_motor_topic_, 10,
                                        &MCoreVelocitySenderNode::ControlMotorCallback, this);
      cleaning_params_sub_ = nh_.subscribe(cleaning_params_topic_, 10,
                                           &MCoreVelocitySenderNode::CleaningParamsCallback, this);

      main_brush_cmd_sub_ = pnh_.subscribe("main_brush_speed_cmd", 5,
                                           &MCoreVelocitySenderNode::MainBrushDirectCallback, this);
      side_brush_cmd_sub_ = pnh_.subscribe("side_brush_cmd", 5,
                                           &MCoreVelocitySenderNode::SideBrushDirectCallback, this);
      water_pump_cmd_sub_ = pnh_.subscribe("water_pump_cmd", 5,
                                           &MCoreVelocitySenderNode::WaterPumpDirectCallback, this);
      brush_lift_cmd_sub_ = pnh_.subscribe("brush_lift_cmd", 5,
                                           &MCoreVelocitySenderNode::BrushLiftDirectCallback, this);
      squeegee_cmd_sub_ = pnh_.subscribe("squeegee_cmd", 5,
                                         &MCoreVelocitySenderNode::SqueegeeDirectCallback, this);
      suction_fan_cmd_sub_ = pnh_.subscribe("suction_fan_cmd", 5,
                                            &MCoreVelocitySenderNode::SuctionFanDirectCallback, this);
    } else {
      ROS_WARN("M-core cleaning serial control disabled.");
    }
    send_timer_ = nh_.createTimer(ros::Duration(1.0 / send_rate_hz_),
                                  &MCoreVelocitySenderNode::SendTimerCallback,
                                  this);
    rx_poll_timer_ = nh_.createTimer(ros::Duration(1.0 / rx_poll_hz_),
                                     &MCoreVelocitySenderNode::RxPollTimerCallback,
                                     this);
    if (enable_battery_remaining_poll_ || enable_water_level_poll_) {
      telemetry_poll_timer_ =
          nh_.createTimer(ros::Duration(telemetry_poll_interval_sec_),
                          &MCoreVelocitySenderNode::TelemetryPollTimerCallback,
                          this);
    }
    if (enable_status_poll_) {
      status_poll_timer_ =
          nh_.createTimer(ros::Duration(1.0 / status_poll_hz_),
                          &MCoreVelocitySenderNode::StatusPollTimerCallback,
                          this);
    }

    ROS_INFO_STREAM("M-core velocity sender topic=" << cmd_vel_topic_
                    << ", transport=" << transport_
                    << ", serial_device=" << serial_device_
                    << ", baudrate=" << serial_baudrate_
                    << ", tcp_host=" << tcp_host_
                    << ", tcp_port=" << tcp_port_
                    << ", tcp_connect_timeout_sec=" << tcp_connect_timeout_sec_
                    << ", send_rate_hz=" << send_rate_hz_
                    << ", cmd_timeout_sec=" << cmd_timeout_sec_
                    << ", tx_min_interval_sec=" << tx_min_interval_sec_
                    << ", send_immediately="
                    << (send_immediately_ ? "true" : "false")
                    << ", repeat_last_cmd_vel="
                    << (repeat_last_cmd_vel_ ? "true" : "false")
                    << ", linear_velocity_scale=" << linear_velocity_scale_
                    << ", angular_velocity_scale=" << angular_velocity_scale_
                    << ", linear_velocity_sign=" << linear_velocity_sign_
                    << ", angular_velocity_sign=" << angular_velocity_sign_
                    << ", max_abs_linear_velocity_si=" << max_abs_linear_velocity_
                    << ", max_abs_angular_velocity_si=" << max_abs_angular_velocity_
                    << ", drain_after_write=" << (drain_after_write_ ? "true" : "false")
                    << ", enable_tx_log=" << (enable_tx_log_ ? "true" : "false")
                    << ", enable_rx_log=" << (enable_rx_log_ ? "true" : "false")
                    << ", rx_poll_hz=" << rx_poll_hz_
                    << ", telemetry_velocity_quiet_sec="
                    << telemetry_velocity_quiet_sec_
                    << ", telemetry_control_cooldown_sec="
                    << telemetry_control_cooldown_sec_
                    << ", enable_cleaning_control="
                    << (enable_cleaning_control_ ? "true" : "false")
                    << ", clean_tools_topic=" << clean_tools_topic_
                    << ", water_tap_topic=" << water_tap_topic_
                    << ", vacuum_motor_topic=" << vacuum_motor_topic_
                    << ", combined_status_topic=" << combined_status_topic_
                    << ", safety_status_bits_topic=" << safety_status_bits_topic_
                    << ", telemetry_heartbeat_topic=" << telemetry_heartbeat_topic_
                    << ", enable_status_poll="
                    << (enable_status_poll_ ? "true" : "false")
                    << ", status_poll_hz=" << status_poll_hz_
                    << ", enable_battery_remaining_poll="
                    << (enable_battery_remaining_poll_ ? "true" : "false")
                    << ", battery_full_capacity=" << battery_full_capacity_
                    << ", enable_water_level_poll="
                    << (enable_water_level_poll_ ? "true" : "false")
                    << ", telemetry_poll_interval_sec=" << telemetry_poll_interval_sec_
                    << ", water_level_encoding=cumulative_bitmask"
                    << ", charge_enable_topic=" << charge_enable_topic_
                    << ", charge_cmd_repeat=" << charge_cmd_repeat_);
  }

  ~MCoreVelocitySenderNode() {
    if (send_cleaning_stop_on_shutdown_ && serial_fd_ >= 0) {
      SendCleaningStopAll("shutdown_cleaning_stop");
    }
    if (send_zero_on_shutdown_ && serial_fd_ >= 0) {
      WriteCommand(0.0, 0.0, "shutdown_zero", true);
    }
    CloseSerial();
  }

 private:
  void CmdVelCallback(const geometry_msgs::Twist& msg) {
    if (!IsFiniteTwist(msg)) {
      ROS_WARN_THROTTLE(1.0, "Ignoring cmd_vel with non-finite linear.x or angular.z.");
      return;
    }

    last_vx_ = mcore_chassis_bridge::ToProtocolVelocity(
        msg.linear.x, linear_velocity_sign_, max_abs_linear_velocity_, linear_velocity_scale_);
    last_wz_ = mcore_chassis_bridge::ToProtocolVelocity(
        msg.angular.z, angular_velocity_sign_, max_abs_angular_velocity_,
        angular_velocity_scale_);
    last_cmd_time_ = ros::Time::now();
    have_cmd_ = true;
    timeout_logged_ = false;

    if (send_immediately_) {
      EnsureSerialOpen();
      if (serial_fd_ >= 0) {
        WriteCommand(last_vx_, last_wz_, "cmd_vel", true);
      }
    }
  }

  void CleaningParamsCallback(const robot_platform_msgs::CleaningParams& msg) {
    cleaning_params_ = msg;
    last_height_scrub_ = static_cast<int>(msg.height_scrub);
    main_brush_speed_ = ClampInt(static_cast<int>(msg.main_brush_speed), 0, 100);
    side_brush_on_value_ = msg.side_brush_enable
                               ? ClampInt(static_cast<int>(msg.side_brush_speed), 0, 100)
                               : 0;
    brush_down_distance_ = ClampInt(static_cast<int>(msg.brush_down_distance), 0, 1800);
    PublishCleaningParams();
    ROS_INFO_STREAM("M-core cleaning params updated profile=" << msg.profile_name
                    << ", main_brush=" << static_cast<unsigned int>(msg.main_brush_speed)
                    << ", side_brush=" << static_cast<unsigned int>(msg.side_brush_speed)
                    << ", brush_down=" << static_cast<unsigned int>(msg.brush_down_distance)
                    << ", side_brush_enable=" << (msg.side_brush_enable ? "true" : "false")
                    << ", water_pump=" << static_cast<unsigned int>(msg.vel_water_pump)
                    << ", suction=" << static_cast<unsigned int>(msg.vel_water_suction)
                    << ", height_scrub=" << static_cast<unsigned int>(msg.height_scrub));
  }

  void ChargeEnableCallback(const std_msgs::Bool& msg) {
    EnsureSerialOpen();
    if (serial_fd_ < 0) {
      return;
    }

    const bool enable = static_cast<bool>(msg.data);
    const int16_t value = enable ? 1 : 0;
    ROS_INFO_STREAM("M-core charge_enable=" << (enable ? "true" : "false")
                    << ", cmd=0x6022, repeat=" << charge_cmd_repeat_);
    for (int i = 0; i < charge_cmd_repeat_; ++i) {
      WriteInt16Command(kBatteryChargingCommand, value, "battery_charging");
      if (i + 1 < charge_cmd_repeat_ && charge_cmd_interval_sec_ > 0.0) {
        ros::Duration(charge_cmd_interval_sec_).sleep();
      }
    }
  }

  void ControlCleanToolsCallback(const robot_platform_msgs::ControlCleanTools& msg) {
    EnsureSerialOpen();
    if (serial_fd_ < 0) {
      return;
    }

    const int tool = static_cast<int>(msg.tool_id);
    const int op = static_cast<int>(msg.operation);

    if (tool == 0x01) {
      HandleBrushToolOperation(op);
      return;
    }

    if (tool == 0x02) {
      HandleSqueegeeToolOperation(op);
      return;
    }

    if (tool == 0x03) {
      const int16_t value = (op == 0 || op == 4) ? ClampInt16(side_brush_off_value_, 0, 100)
                                                 : ClampInt16(side_brush_on_value_, 0, 100);
      WriteInt16Command(kSideBrushCommand, value, "side_brush_tool");
      return;
    }

    ROS_WARN_STREAM_THROTTLE(1.0, "Ignoring unsupported clean tool_id="
                                      << tool << " operation=" << op);
  }

  void ControlWaterTapCallback(const robot_platform_msgs::ControlWaterTap& msg) {
    EnsureSerialOpen();
    if (serial_fd_ < 0) {
      return;
    }

    const int tap = static_cast<int>(msg.tap_id);
    const int op = static_cast<int>(msg.operation);
    if (tap == 0x01) {
      WriteInt16Command(kCleanWaterPumpCommand, ClampPercent(op), "clean_water_pump");
      return;
    }
    if (tap == 0x05) {
      WriteInt16Command(kSuctionFanCommand, ClampPercent(op), "suction_fan_tap");
      return;
    }
    if (tap == 0x02) {
      WriteInt16Command(kCleanWaterValveCommand,
                        static_cast<int16_t>(op == 0 ? 0 : 1),
                        "clean_water_valve");
      return;
    }
    if (tap == 0x03) {
      WriteInt16Command(kSewageValveCommand,
                        static_cast<int16_t>(op == 0 ? 0 : 1),
                        "sewage_valve");
      return;
    }
    ROS_WARN_STREAM_THROTTLE(1.0, "Ignoring unsupported water tap_id="
                                      << tap << " operation=" << op);
  }

  void ControlMotorCallback(const robot_platform_msgs::ControlMotor& msg) {
    EnsureSerialOpen();
    if (serial_fd_ < 0) {
      return;
    }
    WriteInt16Command(kSuctionFanCommand, ClampPercent(static_cast<int>(msg.vel)),
                      "suction_fan_motor");
  }

  void MainBrushDirectCallback(const std_msgs::Int16& msg) {
    EnsureSerialOpen();
    if (serial_fd_ >= 0) {
      WriteInt16Command(kMainBrushCommand, ClampPercent(static_cast<int>(msg.data)),
                        "direct_main_brush");
    }
  }

  void SideBrushDirectCallback(const std_msgs::Int16& msg) {
    EnsureSerialOpen();
    if (serial_fd_ >= 0) {
      WriteInt16Command(kSideBrushCommand, ClampInt16(static_cast<int>(msg.data), 0, 100),
                        "direct_side_brush");
    }
  }

  void WaterPumpDirectCallback(const std_msgs::Int16& msg) {
    EnsureSerialOpen();
    if (serial_fd_ >= 0) {
      WriteInt16Command(kCleanWaterPumpCommand, ClampPercent(static_cast<int>(msg.data)),
                        "direct_water_pump");
    }
  }

  void BrushLiftDirectCallback(const std_msgs::Int16& msg) {
    EnsureSerialOpen();
    if (serial_fd_ >= 0) {
      WriteInt16Command(kBrushLiftCommand,
                        ClampInt16(static_cast<int>(msg.data), -10001, 1800),
                        "direct_brush_lift");
    }
  }

  void SqueegeeDirectCallback(const std_msgs::Int16& msg) {
    EnsureSerialOpen();
    if (serial_fd_ >= 0) {
      WriteInt16Command(kSqueegeeLiftCommand, ClampInt16(static_cast<int>(msg.data), -1, 1),
                        "direct_squeegee");
    }
  }

  void SuctionFanDirectCallback(const std_msgs::Int16& msg) {
    EnsureSerialOpen();
    if (serial_fd_ >= 0) {
      WriteInt16Command(kSuctionFanCommand, ClampPercent(static_cast<int>(msg.data)),
                        "direct_suction_fan");
    }
  }

  int16_t ClampPercent(int value) const {
    return ClampInt16(value, 0, 100);
  }

  int16_t BrushDownDistance() const {
    if (use_height_scrub_for_brush_distance_) {
      return ClampInt16(last_height_scrub_, 0, 1800);
    }
    return ClampInt16(brush_down_distance_, 0, 1800);
  }

  void HandleBrushToolOperation(int op) {
    switch (op) {
      case 0x00:
        WriteInt16Command(kBrushLiftCommand, ClampInt16(brush_stop_value_, -10001, 1800),
                          "brush_lift_stop");
        break;
      case 0x01:
        WriteInt16Command(kBrushLiftCommand, ClampInt16(brush_home_value_, -10000, 1800),
                          "brush_lift_home");
        break;
      case 0x02:
        WriteInt16Command(kBrushLiftCommand, BrushDownDistance(), "brush_lift_down");
        break;
      case 0x03:
        WriteInt16Command(kMainBrushCommand, ClampPercent(main_brush_speed_), "main_brush_on");
        if (side_brush_follows_main_brush_) {
          WriteInt16Command(kSideBrushCommand, ClampInt16(side_brush_on_value_, 0, 100),
                            "side_brush_on");
        }
        break;
      case 0x04:
        if (side_brush_follows_main_brush_) {
          WriteInt16Command(kSideBrushCommand, ClampInt16(side_brush_off_value_, 0, 100),
                            "side_brush_off");
        }
        WriteInt16Command(kMainBrushCommand, 0, "main_brush_off");
        break;
      default:
        ROS_WARN_STREAM_THROTTLE(1.0, "Ignoring unsupported brush operation=" << op);
        break;
    }
  }

  void HandleSqueegeeToolOperation(int op) {
    switch (op) {
      case 0x00:
        WriteInt16Command(kSqueegeeLiftCommand, ClampInt16(squeegee_stop_value_, -1, 1),
                          "squeegee_stop");
        break;
      case 0x01:
        WriteInt16Command(kSqueegeeLiftCommand, ClampInt16(squeegee_backward_value_, -1, 1),
                          "squeegee_backward");
        break;
      case 0x02:
        WriteInt16Command(kSqueegeeLiftCommand, ClampInt16(squeegee_forward_value_, -1, 1),
                          "squeegee_forward");
        break;
      case 0x03:
        WriteInt16Command(kSqueegeeLiftCommand, ClampInt16(squeegee_forward_value_, -1, 1),
                          "squeegee_open");
        break;
      case 0x04:
        WriteInt16Command(kSqueegeeLiftCommand, ClampInt16(squeegee_backward_value_, -1, 1),
                          "squeegee_close");
        break;
      default:
        ROS_WARN_STREAM_THROTTLE(1.0, "Ignoring unsupported squeegee operation=" << op);
        break;
    }
  }

  void PublishCleaningParams() {
    cleaning_params_pub_.publish(cleaning_params_);
  }

  void SendTimerCallback(const ros::TimerEvent&) {
    EnsureSerialOpen();
    if (serial_fd_ < 0) {
      return;
    }

    ReadAvailableBytes();
    WarnIfRxSilent();

    double vx = 0.0;
    double wz = 0.0;
    const bool fresh = HasFreshCommand();
    if (fresh) {
      if (!repeat_last_cmd_vel_) {
        return;
      }
      vx = last_vx_;
      wz = last_wz_;
    } else if (!have_cmd_) {
      return;
    } else if (!timeout_logged_) {
      ROS_WARN_STREAM("cmd_vel timeout after " << cmd_timeout_sec_
                      << "s; sending zero velocity.");
      timeout_logged_ = true;
    }

    WriteCommand(vx, wz, fresh ? "timer_repeat" : "timeout_zero", false);
  }

  void RxPollTimerCallback(const ros::TimerEvent&) {
    EnsureSerialOpen();
    if (serial_fd_ < 0) {
      return;
    }
    if (TelemetryShouldYieldToControl()) {
      return;
    }

    ReadAvailableBytes();
    WarnIfRxSilent();
  }

  void TelemetryPollTimerCallback(const ros::TimerEvent&) {
    EnsureSerialOpen();
    if (serial_fd_ < 0) {
      return;
    }
    if (TelemetryShouldYieldToControl()) {
      return;
    }

    for (int attempts = 0; attempts < 3; ++attempts) {
      const size_t poll_index = telemetry_poll_index_;
      telemetry_poll_index_ = (telemetry_poll_index_ + 1) % 3;

      if (poll_index == 0 && enable_battery_remaining_poll_) {
        if (WriteNoDataCommand(kBatteryRemainingCommand, "battery_remaining_poll")) {
          ArmTelemetryVelocityQuietWindow();
        }
        return;
      }
      if (poll_index == 1 && enable_water_level_poll_) {
        if (WriteNoDataCommand(kCleanWaterLevelCommand, "clean_water_level_poll")) {
          ArmTelemetryVelocityQuietWindow();
        }
        return;
      }
      if (poll_index == 2 && enable_water_level_poll_) {
        if (WriteNoDataCommand(kSewageLevelCommand, "sewage_level_poll")) {
          ArmTelemetryVelocityQuietWindow();
        }
        return;
      }
    }
  }

  void StatusPollTimerCallback(const ros::TimerEvent&) {
    EnsureSerialOpen();
    if (serial_fd_ < 0 || TelemetryShouldYieldToControl()) {
      return;
    }
    if (WriteNoDataCommand(kStatusCommand, "safety_status_poll")) {
      ArmTelemetryVelocityQuietWindow();
    }
  }

  void ArmTelemetryVelocityQuietWindow() {
    if (telemetry_velocity_quiet_sec_ <= 0.0) {
      return;
    }
    telemetry_velocity_quiet_until_ =
        ros::WallTime::now() + ros::WallDuration(telemetry_velocity_quiet_sec_);
  }

  bool InTelemetryVelocityQuietWindow() const {
    return telemetry_velocity_quiet_sec_ > 0.0 &&
           !telemetry_velocity_quiet_until_.isZero() &&
           ros::WallTime::now() < telemetry_velocity_quiet_until_;
  }

  bool TelemetryShouldYieldToControl() const {
    return telemetry_control_cooldown_sec_ > 0.0 &&
           !last_control_tx_time_.isZero() &&
           (ros::WallTime::now() - last_control_tx_time_).toSec() <
               telemetry_control_cooldown_sec_;
  }

  bool HasFreshCommand() const {
    if (!have_cmd_) {
      return false;
    }
    if (cmd_timeout_sec_ <= 0.0) {
      return true;
    }
    return (ros::Time::now() - last_cmd_time_).toSec() <= cmd_timeout_sec_;
  }

  void EnsureSerialOpen() {
    if (serial_fd_ >= 0) {
      return;
    }

    const ros::WallTime now = ros::WallTime::now();
    if (!last_open_attempt_.isZero() &&
        (now - last_open_attempt_).toSec() < reconnect_interval_sec_) {
      return;
    }
    last_open_attempt_ = now;

    OpenSerial();
  }

  bool IsTcpTransport() const {
    return transport_ == "tcp";
  }

  std::string ConnectionDescription() const {
    if (IsTcpTransport()) {
      std::ostringstream oss;
      oss << tcp_host_ << ":" << tcp_port_;
      return oss.str();
    }
    return serial_device_;
  }

  bool OpenSerial() {
    CloseSerial();

    const bool opened = IsTcpTransport() ? OpenTcpConnection() : OpenSerialConnection();
    if (!opened) {
      return false;
    }

    rx_buffer_.clear();
    last_rx_byte_time_ = ros::WallTime::now();
    last_rx_silence_warn_time_ = ros::WallTime();
    PublishConnected(true);
    ROS_INFO_STREAM("Opened M-core " << transport_ << " connection: "
                    << ConnectionDescription());

    if (send_zero_on_connect_ && !WriteCommand(0.0, 0.0, "connect_zero", true)) {
      CloseSerial();
      return false;
    }
    if (send_cleaning_stop_on_connect_) {
      SendCleaningStopAll("connect_cleaning_stop");
    }
    return true;
  }

  bool OpenSerialConnection() {
    serial_fd_ = open(serial_device_.c_str(), O_RDWR | O_NOCTTY | O_NONBLOCK);
    if (serial_fd_ < 0) {
      ROS_WARN_STREAM_THROTTLE(2.0, "M-core serial open(\"" << serial_device_
                               << "\") failed: " << std::strerror(errno));
      return false;
    }

    termios tty;
    std::memset(&tty, 0, sizeof(tty));
    if (tcgetattr(serial_fd_, &tty) != 0) {
      ROS_WARN_STREAM("M-core serial tcgetattr() failed: " << std::strerror(errno));
      CloseSerial();
      return false;
    }

    cfmakeraw(&tty);
    const speed_t baud = ToTermiosBaud(serial_baudrate_);
    if (baud == 0) {
      ROS_WARN_STREAM("Unsupported serial_baudrate=" << serial_baudrate_
                      << "; fallback to 115200.");
      cfsetispeed(&tty, B115200);
      cfsetospeed(&tty, B115200);
    } else {
      cfsetispeed(&tty, baud);
      cfsetospeed(&tty, baud);
    }

    tty.c_cflag |= (CLOCAL | CREAD);
    tty.c_cflag &= ~CRTSCTS;
    tty.c_cflag &= ~PARENB;
    tty.c_cflag &= ~CSTOPB;
    tty.c_cflag &= ~CSIZE;
    tty.c_cflag |= CS8;
    tty.c_cc[VMIN] = 0;
    tty.c_cc[VTIME] = 0;

    if (tcsetattr(serial_fd_, TCSANOW, &tty) != 0) {
      ROS_WARN_STREAM("M-core serial tcsetattr() failed: " << std::strerror(errno));
      CloseSerial();
      return false;
    }

    tcflush(serial_fd_, TCIOFLUSH);
    return true;
  }

  bool OpenTcpConnection() {
    if (tcp_host_.empty()) {
      ROS_WARN("M-core tcp_host is empty.");
      return false;
    }

    serial_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (serial_fd_ < 0) {
      ROS_WARN_STREAM_THROTTLE(2.0, "M-core TCP socket() failed: "
                               << std::strerror(errno));
      return false;
    }

    int flags = fcntl(serial_fd_, F_GETFL, 0);
    if (flags < 0 || fcntl(serial_fd_, F_SETFL, flags | O_NONBLOCK) != 0) {
      ROS_WARN_STREAM("M-core TCP fcntl(O_NONBLOCK) failed: " << std::strerror(errno));
      CloseSerial();
      return false;
    }

    sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(tcp_port_));
    if (inet_pton(AF_INET, tcp_host_.c_str(), &addr.sin_addr) != 1) {
      ROS_WARN_STREAM("M-core TCP invalid IPv4 address: " << tcp_host_);
      CloseSerial();
      return false;
    }

    const int rc = connect(serial_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    if (rc != 0 && errno != EINPROGRESS) {
      ROS_WARN_STREAM_THROTTLE(2.0, "M-core TCP connect(" << ConnectionDescription()
                               << ") failed: " << std::strerror(errno));
      CloseSerial();
      return false;
    }

    if (rc != 0 && !WaitForTcpConnect()) {
      CloseSerial();
      return false;
    }

    if (tcp_no_delay_) {
      const int yes = 1;
      if (setsockopt(serial_fd_, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes)) != 0) {
        ROS_WARN_STREAM("M-core TCP TCP_NODELAY setup failed: " << std::strerror(errno));
      }
    }
    if (tcp_keepalive_) {
      const int yes = 1;
      if (setsockopt(serial_fd_, SOL_SOCKET, SO_KEEPALIVE, &yes, sizeof(yes)) != 0) {
        ROS_WARN_STREAM("M-core TCP SO_KEEPALIVE setup failed: " << std::strerror(errno));
      }
    }
    return true;
  }

  bool WaitForTcpConnect() {
    fd_set write_fds;
    FD_ZERO(&write_fds);
    FD_SET(serial_fd_, &write_fds);

    timeval tv;
    tv.tv_sec = static_cast<int>(tcp_connect_timeout_sec_);
    tv.tv_usec = static_cast<int>((tcp_connect_timeout_sec_ - tv.tv_sec) * 1000000.0);

    const int rc = select(serial_fd_ + 1, nullptr, &write_fds, nullptr, &tv);
    if (rc == 0) {
      ROS_WARN_STREAM_THROTTLE(2.0, "M-core TCP connect(" << ConnectionDescription()
                               << ") timed out after " << tcp_connect_timeout_sec_ << "s.");
      return false;
    }
    if (rc < 0) {
      ROS_WARN_STREAM_THROTTLE(2.0, "M-core TCP connect select() failed: "
                               << std::strerror(errno));
      return false;
    }

    int so_error = 0;
    socklen_t len = sizeof(so_error);
    if (getsockopt(serial_fd_, SOL_SOCKET, SO_ERROR, &so_error, &len) != 0) {
      ROS_WARN_STREAM("M-core TCP getsockopt(SO_ERROR) failed: " << std::strerror(errno));
      return false;
    }
    if (so_error != 0) {
      ROS_WARN_STREAM_THROTTLE(2.0, "M-core TCP connect(" << ConnectionDescription()
                               << ") failed: " << std::strerror(so_error));
      return false;
    }
    return true;
  }

  void CloseSerial() {
    if (serial_fd_ >= 0) {
      close(serial_fd_);
      serial_fd_ = -1;
    }
    PublishConnected(false);
  }

  bool WriteCommand(double vx, double wz, const std::string& source, bool force_log) {
    const auto frame = BuildVelocityFrame(static_cast<float>(vx), static_cast<float>(wz));
    const bool wrote = WriteAll(frame.data(), frame.size());
    LogTxFrame(source, vx, wz, frame, wrote, force_log);
    return wrote;
  }

  bool WriteInt16Command(uint16_t command, int16_t value, const std::string& source) {
    const auto frame = BuildInt16Frame(command, value);
    const bool wrote = WriteAll(frame.data(), frame.size());
    if (wrote) {
      last_control_tx_time_ = ros::WallTime::now();
    }
    LogTxInt16Frame(source, command, value, frame, wrote);
    return wrote;
  }

  bool WriteNoDataCommand(uint16_t command, const std::string& source) {
    const auto frame = BuildNoDataFrame(command);
    const bool wrote = WriteAll(frame.data(), frame.size());
    LogTxNoDataFrame(source, command, frame, wrote);
    return wrote;
  }

  void SendCleaningStopAll(const std::string& source) {
    WriteInt16Command(kCleanWaterPumpCommand, 0, source + "_water_pump");
    WriteInt16Command(kCleanWaterValveCommand, 0, source + "_water_valve");
    WriteInt16Command(kSewageValveCommand, 0, source + "_sewage_valve");
    WriteInt16Command(kSuctionFanCommand, 0, source + "_suction_fan");
    WriteInt16Command(kSideBrushCommand, ClampInt16(side_brush_off_value_, 0, 100),
                      source + "_side_brush");
    WriteInt16Command(kMainBrushCommand, 0, source + "_main_brush");
    WriteInt16Command(kBrushLiftCommand, ClampInt16(brush_stop_value_, -10001, 1800),
                      source + "_brush_lift");
    WriteInt16Command(kSqueegeeLiftCommand, ClampInt16(squeegee_stop_value_, -1, 1),
                      source + "_squeegee");
  }

  bool WriteAll(const uint8_t* data, size_t len) {
    if (serial_fd_ < 0) {
      return false;
    }

    WaitForTxSpacing();
    size_t offset = 0;
    const ros::WallTime start = ros::WallTime::now();
    while (offset < len) {
      const ssize_t n = write(serial_fd_, data + offset, len - offset);
      if (n > 0) {
        offset += static_cast<size_t>(n);
        continue;
      }

      if (n < 0 && errno == EINTR) {
        continue;
      }

      if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        if ((ros::WallTime::now() - start).toSec() > write_timeout_sec_) {
          ROS_WARN_STREAM("M-core " << transport_ << " write timeout after "
                          << write_timeout_sec_ << "s.");
          CloseSerial();
          return false;
        }
        usleep(1000);
        continue;
      }

      ROS_WARN_STREAM("M-core " << transport_ << " write() failed: " << std::strerror(errno));
      CloseSerial();
      return false;
    }
    if (!IsTcpTransport() && drain_after_write_ && !DrainSerialOutput()) {
      return false;
    }
    last_tx_write_time_ = ros::WallTime::now();
    return offset == len;
  }

  void WaitForTxSpacing() {
    if (tx_min_interval_sec_ <= 0.0 || last_tx_write_time_.isZero()) {
      return;
    }
    while (serial_fd_ >= 0) {
      const double elapsed = (ros::WallTime::now() - last_tx_write_time_).toSec();
      if (elapsed >= tx_min_interval_sec_) {
        return;
      }
      ReadAvailableBytes();
      usleep(1000);
    }
  }

  bool DrainSerialOutput() {
    while (true) {
      if (tcdrain(serial_fd_) == 0) {
        return true;
      }
      if (errno == EINTR) {
        continue;
      }
      ROS_WARN_STREAM("M-core serial tcdrain() failed: " << std::strerror(errno));
      CloseSerial();
      return false;
    }
  }

  void PublishConnected(bool connected, bool force = false) {
    if (!force && connected == connected_) {
      return;
    }
    connected_ = connected;
    std_msgs::Bool msg;
    msg.data = connected_;
    connected_pub_.publish(msg);
  }

  void LogTxFrame(const std::string& source,
                  double vx,
                  double wz,
                  const std::array<uint8_t, kVelocityFrameBytes>& frame,
                  bool wrote,
                  bool force_log) {
    if (!enable_tx_log_) {
      return;
    }

    const ros::WallTime now = ros::WallTime::now();
    if (!force_log && tx_log_interval_sec_ > 0.0 && !last_tx_log_time_.isZero() &&
        (now - last_tx_log_time_).toSec() < tx_log_interval_sec_) {
      return;
    }
    last_tx_log_time_ = now;

    ROS_INFO_STREAM("M-core tx [" << source << "] wrote=" << (wrote ? "true" : "false")
                    << ", vx=" << vx
                    << ", wz=" << wz
                    << ", bytes=" << frame.size()
                    << ", frame=" << HexString(frame.data(), frame.size()));
  }

  void LogTxInt16Frame(const std::string& source,
                       uint16_t command,
                       int16_t value,
                       const std::array<uint8_t, kInt16FrameBytes>& frame,
                       bool wrote) {
    if (!enable_tx_log_) {
      return;
    }

    ROS_INFO_STREAM("M-core tx [" << source << "] wrote=" << (wrote ? "true" : "false")
                    << ", cmd=0x" << std::uppercase << std::hex << std::setw(4)
                    << std::setfill('0') << static_cast<unsigned int>(command)
                    << std::dec << ", value=" << static_cast<int>(value)
                    << ", bytes=" << frame.size()
                    << ", frame=" << HexString(frame.data(), frame.size()));
  }

  void LogTxNoDataFrame(const std::string& source,
                        uint16_t command,
                        const std::array<uint8_t, kNoDataFrameBytes>& frame,
                        bool wrote) {
    if (!enable_tx_log_) {
      return;
    }

    ROS_INFO_STREAM("M-core tx [" << source << "] wrote=" << (wrote ? "true" : "false")
                    << ", cmd=0x" << std::uppercase << std::hex << std::setw(4)
                    << std::setfill('0') << static_cast<unsigned int>(command)
                    << std::dec
                    << ", bytes=" << frame.size()
                    << ", frame=" << HexString(frame.data(), frame.size()));
  }

  void ReadAvailableBytes() {
    uint8_t chunk[256];

    while (true) {
      const ssize_t n = read(serial_fd_, chunk, sizeof(chunk));
      if (n > 0) {
        last_rx_byte_time_ = ros::WallTime::now();
        if (enable_rx_raw_log_) {
          ROS_INFO_STREAM("M-core rx raw bytes=" << n << ", data="
                          << HexString(chunk, static_cast<size_t>(n)));
        }
        rx_buffer_.insert(rx_buffer_.end(), chunk, chunk + n);
        if (rx_buffer_.size() > kMaxRxBufferBytes) {
          rx_buffer_.erase(rx_buffer_.begin(),
                           rx_buffer_.begin() +
                               static_cast<std::ptrdiff_t>(rx_buffer_.size() / 2));
        }
        ParseRxBuffer();
        continue;
      }

      if (n == 0) {
        if (IsTcpTransport()) {
          ROS_WARN_STREAM("M-core TCP connection closed by peer: "
                          << ConnectionDescription());
          CloseSerial();
        }
        return;
      }

      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        return;
      }

      if (errno == EINTR) {
        continue;
      }

      ROS_WARN_STREAM("M-core " << transport_ << " read() failed: " << std::strerror(errno));
      CloseSerial();
      return;
    }
  }

  void ParseRxBuffer() {
    while (rx_buffer_.size() >= kMinFrameBytes) {
      const auto head = std::find_if(rx_buffer_.begin(), rx_buffer_.end(), IsFrameHead);
      if (head == rx_buffer_.end()) {
        rx_buffer_.clear();
        return;
      }
      if (head != rx_buffer_.begin()) {
        rx_buffer_.erase(rx_buffer_.begin(), head);
      }
      if (rx_buffer_.size() < kMinFrameBytes) {
        return;
      }

      const size_t len16 =
          (static_cast<size_t>(rx_buffer_[2]) << 8) | static_cast<size_t>(rx_buffer_[3]);
      const size_t len_low = static_cast<size_t>(rx_buffer_[3]);
      size_t frame_len = len16;
      if (frame_len < kMinFrameBytes || frame_len > kMaxFrameBytes) {
        frame_len = len_low;
      }
      if (frame_len < kMinFrameBytes || frame_len > kMaxFrameBytes) {
        rx_buffer_.erase(rx_buffer_.begin());
        continue;
      }
      if (rx_buffer_.size() < frame_len) {
        return;
      }

      if (rx_buffer_[frame_len - 1] != kTail) {
        rx_buffer_.erase(rx_buffer_.begin());
        continue;
      }

      const uint16_t cmd =
          (static_cast<uint16_t>(rx_buffer_[4]) << 8) | static_cast<uint16_t>(rx_buffer_[5]);
      HandleRxFrame(cmd, rx_buffer_.data(), frame_len);
      rx_buffer_.erase(rx_buffer_.begin(),
                       rx_buffer_.begin() + static_cast<std::ptrdiff_t>(frame_len));
    }
  }

  bool ShouldLogRxFrame() {
    if (!enable_rx_log_) {
      return false;
    }

    const ros::WallTime now = ros::WallTime::now();
    if (rx_log_interval_sec_ > 0.0 && !last_rx_log_time_.isZero() &&
        (now - last_rx_log_time_).toSec() < rx_log_interval_sec_) {
      return false;
    }
    last_rx_log_time_ = now;
    return true;
  }

  void HandleRxFrame(uint16_t cmd, const uint8_t* frame, size_t len) {
    const bool trusted = IsTrustedTelemetryFrame(frame, len);
    if (trusted &&
        (cmd == kStatusCommand || cmd == kVelocityCommand ||
         cmd == kBatteryRemainingCommand || cmd == kCleanWaterLevelCommand ||
         cmd == kSewageLevelCommand)) {
      std_msgs::UInt64 heartbeat;
      heartbeat.data = ++telemetry_heartbeat_sequence_;
      telemetry_heartbeat_pub_.publish(heartbeat);
    }

    if (cmd == kStatusCommand) {
      HandleStatusFrame(frame, len, trusted);
      return;
    }

    if (cmd == kVelocityCommand) {
      HandleVelocityFeedbackFrame(frame, len);
      return;
    }

    if (cmd == kBatteryRemainingCommand) {
      HandleBatteryRemainingFrame(frame, len);
      return;
    }

    if (cmd == kCleanWaterLevelCommand) {
      HandleWaterLevelFrame("clean_water_level", cmd, frame, len);
      return;
    }

    if (cmd == kSewageLevelCommand) {
      HandleWaterLevelFrame("sewage_level", cmd, frame, len);
      return;
    }

    if (ShouldLogRxFrame()) {
      ROS_INFO_STREAM("M-core rx cmd=0x" << std::uppercase << std::hex << std::setw(4)
                      << std::setfill('0') << static_cast<unsigned int>(cmd)
                      << std::dec << ", bytes=" << len
                      << ", frame=" << HexString(frame, len));
    }
  }

  void HandleVelocityFeedbackFrame(const uint8_t* frame, size_t len) {
    if (len < kVelocityFrameBytes) {
      if (ShouldLogRxFrame()) {
        ROS_INFO_STREAM("M-core rx velocity feedback too short, bytes=" << len
                        << ", frame=" << HexString(frame, len));
      }
      return;
    }

    const float raw_vx = ReadFloat32LittleEndian(&frame[6]);
    const float raw_wz = ReadFloat32LittleEndian(&frame[10]);
    const double ros_vx = linear_velocity_scale_ == 0.0 ? 0.0 : raw_vx / linear_velocity_scale_;
    const double ros_wz = angular_velocity_scale_ == 0.0 ? 0.0 : raw_wz / angular_velocity_scale_;
    if (ShouldLogRxFrame()) {
      ROS_INFO_STREAM("M-core rx velocity_feedback raw_vx=" << raw_vx
                      << ", raw_wz=" << raw_wz
                      << ", approx_ros_vx=" << ros_vx
                      << ", approx_ros_wz=" << ros_wz
                      << ", bytes=" << len
                      << ", frame=" << HexString(frame, len));
    }
  }

  void HandleStatusFrame(const uint8_t* frame, size_t len, bool trusted) {
    if (len < 16) {
      if (ShouldLogRxFrame()) {
        ROS_INFO_STREAM("M-core rx status too short, bytes=" << len
                        << ", frame=" << HexString(frame, len));
      }
      return;
    }

    const uint8_t* d = &frame[6];
    const uint8_t battery_percent = d[0];
    const uint16_t voltage_mv =
        (static_cast<uint16_t>(d[1]) << 8) | static_cast<uint16_t>(d[2]);
    const uint8_t waste_level_bits = d[3];
    const uint8_t clean_water_percent = d[4];
    const uint8_t brush_pos = (d[5] >> 6) & 0x03;
    const uint8_t squeegee_pos = (d[5] >> 4) & 0x03;
    const uint8_t obstacle_bits = d[6];
    const uint8_t front_scan_bits = d[7];
    const bool has_state = len >= 17;
    const uint8_t state = has_state ? d[8] : 0;

    battery_msg_.header.stamp = ros::Time::now();
    battery_msg_.voltage = static_cast<float>(voltage_mv) / 1000.0f;
    battery_msg_.percentage =
        std::max(0.0f, std::min(1.0f, static_cast<float>(battery_percent) / 100.0f));
    battery_pub_.publish(battery_msg_);

    combined_status_msg_.battery_percentage = battery_percent;
    combined_status_msg_.battery_voltage = voltage_mv;
    combined_status_msg_.sewage_level = WasteBitsToLevel(waste_level_bits);
    combined_status_msg_.clean_level = clean_water_percent;
    combined_status_msg_.brush_position = brush_pos;
    combined_status_msg_.scraper_position = squeegee_pos;
    for (size_t i = 0; i < combined_status_msg_.obstacle_status.size(); ++i) {
      combined_status_msg_.obstacle_status[i] =
          (obstacle_bits & (static_cast<uint8_t>(0x80) >> i)) != 0;
    }
    for (size_t i = 0; i < combined_status_msg_.region.size(); ++i) {
      combined_status_msg_.region[i] =
          (front_scan_bits & (static_cast<uint8_t>(0x80) >> i)) != 0;
    }
    for (size_t i = 0; i < combined_status_msg_.status.size(); ++i) {
      combined_status_msg_.status[i] =
          has_state && ((state & (static_cast<uint8_t>(0x80) >> i)) != 0);
    }
    combined_status_pub_.publish(combined_status_msg_);

    if (has_state && trusted) {
      std_msgs::UInt8 safety_msg;
      safety_msg.data = state;
      safety_status_bits_pub_.publish(safety_msg);
    } else if (has_state) {
      ROS_WARN_THROTTLE(2.0,
                        "M-core 0x4070 physical safety byte failed strict "
                        "header/length/checksum validation; actuator debug "
                        "will not trust it.");
    } else {
      ROS_WARN_THROTTLE(2.0,
                        "M-core 0x4070 status frame has no physical safety byte; "
                        "actuator debug remains fail-closed.");
    }

    if (ShouldLogRxFrame()) {
      ROS_INFO_STREAM("M-core rx status battery=" << static_cast<unsigned int>(battery_percent)
                      << "%, voltage=" << voltage_mv
                      << "mV, waste_bits=0x" << std::uppercase << std::hex << std::setw(2)
                      << std::setfill('0') << static_cast<unsigned int>(waste_level_bits)
                      << ", clean_water=" << std::dec
                      << static_cast<unsigned int>(clean_water_percent)
                      << "%, brush=" << PositionName(brush_pos)
                      << ", squeegee=" << PositionName(squeegee_pos)
                      << ", obstacle=0x" << std::uppercase << std::hex << std::setw(2)
                      << std::setfill('0') << static_cast<unsigned int>(obstacle_bits)
                      << ", front_scan=0x" << std::setw(2)
                      << static_cast<unsigned int>(front_scan_bits)
                      << std::dec
                      << (has_state ? StatusBitsString(state) : std::string(", state=absent"))
                      << ", frame=" << HexString(frame, len));
    }
  }

  void HandleBatteryRemainingFrame(const uint8_t* frame, size_t len) {
    if (len < kInt16FrameBytes) {
      if (ShouldLogRxFrame()) {
        ROS_INFO_STREAM("M-core rx battery remaining too short, bytes=" << len
                        << ", frame=" << HexString(frame, len));
      }
      return;
    }

    const uint8_t* d = &frame[6];
    const uint16_t raw = ReadUInt16LittleEndian(d);
    const float remaining = static_cast<float>(raw) * 0.1f;

    std_msgs::Float32 remaining_msg;
    remaining_msg.data = remaining;
    battery_remaining_pub_.publish(remaining_msg);

    battery_msg_.header.stamp = ros::Time::now();
    battery_msg_.present = true;
    battery_msg_.charge = remaining;
    battery_msg_.capacity = static_cast<float>(battery_full_capacity_);
    battery_msg_.design_capacity = static_cast<float>(battery_full_capacity_);
    battery_msg_.percentage = static_cast<float>(
        mcore_chassis_bridge::ClampAbs(
            remaining / static_cast<float>(battery_full_capacity_), 1.0));
    battery_pub_.publish(battery_msg_);
    combined_status_msg_.battery_percentage = static_cast<uint8_t>(
        ClampInt(static_cast<int>(std::lround(battery_msg_.percentage * 100.0f)), 0, 100));
    combined_status_pub_.publish(combined_status_msg_);

    if (ShouldLogRxFrame()) {
      ROS_INFO_STREAM("M-core rx battery_remaining raw=" << raw
                      << ", remaining=" << remaining
                      << "A, percentage=" << battery_msg_.percentage
                      << ", bytes=" << len
                      << ", frame=" << HexString(frame, len));
    }
  }

  void HandleWaterLevelFrame(const char* name, uint16_t cmd, const uint8_t* frame, size_t len) {
    if (len < kInt16FrameBytes) {
      if (ShouldLogRxFrame()) {
        ROS_INFO_STREAM("M-core rx " << name << " too short, bytes=" << len
                        << ", frame=" << HexString(frame, len));
      }
      return;
    }

    const uint16_t raw = ReadUInt16LittleEndian(&frame[6]);
    const uint8_t gear = WaterLevelToGear(raw);
    const uint8_t percent = WaterLevelToPercent(raw);

    std_msgs::UInt16 raw_msg;
    raw_msg.data = raw;

    if (cmd == kCleanWaterLevelCommand) {
      clean_water_level_pub_.publish(raw_msg);
      combined_status_msg_.clean_level = percent;
    } else if (cmd == kSewageLevelCommand) {
      sewage_level_pub_.publish(raw_msg);
      combined_status_msg_.sewage_level = percent;
    }
    combined_status_pub_.publish(combined_status_msg_);

    if (ShouldLogRxFrame()) {
      ROS_INFO_STREAM("M-core rx " << name
                      << " raw=" << raw
                      << ", gear=" << static_cast<unsigned int>(gear)
                      << ", percent=" << static_cast<unsigned int>(percent)
                      << "%, bytes=" << len
                      << ", frame=" << HexString(frame, len));
    }
  }

  uint8_t WaterLevelToGear(uint16_t raw) const {
    uint16_t bits = static_cast<uint16_t>(raw & 0x000F);
    uint8_t gear = 0;
    while (bits != 0) {
      gear = static_cast<uint8_t>(gear + (bits & 0x0001));
      bits >>= 1;
    }
    return static_cast<uint8_t>(std::min(4, static_cast<int>(gear)));
  }

  uint8_t WaterLevelToPercent(uint16_t raw) const {
    return static_cast<uint8_t>(ClampInt(static_cast<int>(WaterLevelToGear(raw)) * 25, 0, 100));
  }

  uint8_t WasteBitsToLevel(uint8_t bits) const {
    uint8_t level = 0;
    if (bits & 0x80) {
      level = 1;
    }
    if (bits & 0x40) {
      level = 2;
    }
    if (bits & 0x20) {
      level = 3;
    }
    if (bits & 0x10) {
      level = 4;
    }
    // CombinedStatus.sewage_level is a percentage everywhere else (including
    // the dedicated 0x610B polling path). Keep the unsolicited 0x4070 status
    // frame on the same 0/25/50/75/100 contract so consumers never see the
    // value jump between gear numbers and percentages.
    return static_cast<uint8_t>(ClampInt(static_cast<int>(level) * 25, 0, 100));
  }

  std::string StatusBitsString(uint8_t state) const {
    std::ostringstream oss;
    oss << ", state=0x" << std::uppercase << std::hex << std::setw(2) << std::setfill('0')
        << static_cast<unsigned int>(state) << std::dec
        << " estop1=" << ((state & 0x80) ? "1" : "0")
        << " estop2=" << ((state & 0x40) ? "1" : "0")
        << " forward=" << ((state & 0x20) ? "1" : "0")
        << " backward=" << ((state & 0x10) ? "1" : "0")
        << " reset=" << ((state & 0x08) ? "1" : "0")
        << " brake=" << ((state & 0x04) ? "1" : "0")
        << " power_feedback=" << ((state & 0x02) ? "1" : "0")
        << " lidar_ready=" << ((state & 0x01) ? "1" : "0");
    return oss.str();
  }

  void WarnIfRxSilent() {
    if (!enable_rx_log_ || rx_silence_warn_sec_ <= 0.0 || serial_fd_ < 0) {
      return;
    }

    const ros::WallTime now = ros::WallTime::now();
    if (!last_rx_byte_time_.isZero() &&
        (now - last_rx_byte_time_).toSec() < rx_silence_warn_sec_) {
      return;
    }
    if (!last_rx_silence_warn_time_.isZero() &&
        (now - last_rx_silence_warn_time_).toSec() < rx_silence_warn_sec_) {
      return;
    }
    last_rx_silence_warn_time_ = now;

    ROS_WARN_STREAM("No M-core rx bytes for " << rx_silence_warn_sec_
                    << "s; TX can still be successful even if the lower controller"
                    << " is not replying.");
  }

  ros::NodeHandle nh_;
  ros::NodeHandle pnh_;
  ros::Subscriber cmd_vel_sub_;
  ros::Subscriber clean_tools_sub_;
  ros::Subscriber water_tap_sub_;
  ros::Subscriber vacuum_motor_sub_;
  ros::Subscriber cleaning_params_sub_;
  ros::Subscriber charge_enable_sub_;
  ros::Subscriber main_brush_cmd_sub_;
  ros::Subscriber side_brush_cmd_sub_;
  ros::Subscriber water_pump_cmd_sub_;
  ros::Subscriber brush_lift_cmd_sub_;
  ros::Subscriber squeegee_cmd_sub_;
  ros::Subscriber suction_fan_cmd_sub_;
  ros::Publisher connected_pub_;
  ros::Publisher cleaning_params_pub_;
  ros::Publisher combined_status_pub_;
  ros::Publisher safety_status_bits_pub_;
  ros::Publisher telemetry_heartbeat_pub_;
  ros::Publisher battery_pub_;
  ros::Publisher battery_remaining_pub_;
  ros::Publisher clean_water_level_pub_;
  ros::Publisher sewage_level_pub_;
  ros::Timer send_timer_;
  ros::Timer rx_poll_timer_;
  ros::Timer telemetry_poll_timer_;
  ros::Timer status_poll_timer_;

  std::string transport_ = "tcp";
  std::string serial_device_;
  int serial_baudrate_ = 115200;
  std::string tcp_host_ = "192.168.127.10";
  int tcp_port_ = 8080;
  double tcp_connect_timeout_sec_ = 2.0;
  bool tcp_no_delay_ = true;
  bool tcp_keepalive_ = true;
  std::string cmd_vel_topic_;
  std::string clean_tools_topic_;
  std::string water_tap_topic_;
  std::string vacuum_motor_topic_;
  std::string cleaning_params_topic_;
  std::string cleaning_params_state_topic_;
  std::string combined_status_topic_;
  std::string safety_status_bits_topic_;
  std::string telemetry_heartbeat_topic_;
  std::string battery_state_topic_;
  std::string battery_remaining_topic_;
  std::string clean_water_level_topic_;
  std::string sewage_level_topic_;
  std::string charge_enable_topic_;
  double send_rate_hz_ = 10.0;
  double cmd_timeout_sec_ = 0.5;
  double reconnect_interval_sec_ = 1.0;
  double write_timeout_sec_ = 0.05;
  double tx_min_interval_sec_ = 0.02;
  double linear_velocity_scale_ = 1000.0;
  double angular_velocity_scale_ = 1000.0;
  double linear_velocity_sign_ = 1.0;
  double angular_velocity_sign_ = 1.0;
  double max_abs_linear_velocity_ = 0.0;
  double max_abs_angular_velocity_ = 0.0;
  bool send_immediately_ = true;
  bool repeat_last_cmd_vel_ = false;
  bool send_zero_on_connect_ = true;
  bool send_zero_on_shutdown_ = true;
  bool drain_after_write_ = true;
  bool enable_tx_log_ = true;
  double tx_log_interval_sec_ = 1.0;
  bool enable_rx_log_ = true;
  bool enable_rx_raw_log_ = false;
  double rx_log_interval_sec_ = 1.0;
  double rx_silence_warn_sec_ = 5.0;
  double rx_poll_hz_ = 100.0;
  double telemetry_velocity_quiet_sec_ = 0.0;
  double telemetry_control_cooldown_sec_ = 0.3;
  bool enable_battery_remaining_poll_ = true;
  double battery_remaining_poll_hz_ = 0.2;
  double battery_full_capacity_ = 150.0;
  bool enable_water_level_poll_ = true;
  double water_level_poll_hz_ = 0.2;
  double telemetry_poll_interval_sec_ = 1.3;
  bool enable_status_poll_ = false;
  double status_poll_hz_ = 2.0;
  size_t telemetry_poll_index_ = 0;
  int charge_cmd_repeat_ = 3;
  double charge_cmd_interval_sec_ = 0.05;
  bool enable_cleaning_control_ = true;
  bool side_brush_follows_main_brush_ = false;
  bool send_cleaning_stop_on_connect_ = false;
  bool send_cleaning_stop_on_shutdown_ = true;
  bool use_height_scrub_for_brush_distance_ = false;
  int main_brush_speed_ = 0;
  int side_brush_on_value_ = 0;
  int side_brush_off_value_ = 0;
  int brush_down_distance_ = 0;
  int brush_home_value_ = -10000;
  int brush_stop_value_ = -10001;
  int squeegee_forward_value_ = 1;
  int squeegee_backward_value_ = -1;
  int squeegee_stop_value_ = 0;
  int last_height_scrub_ = 0;
  uint64_t telemetry_heartbeat_sequence_ = 0;
  robot_platform_msgs::CleaningParams cleaning_params_;
  robot_platform_msgs::CombinedStatus combined_status_msg_;
  sensor_msgs::BatteryState battery_msg_;

  int serial_fd_ = -1;
  bool connected_ = false;
  ros::WallTime last_open_attempt_;
  ros::Time last_cmd_time_;
  bool have_cmd_ = false;
  bool timeout_logged_ = false;
  double last_vx_ = 0.0;
  double last_wz_ = 0.0;
  ros::WallTime last_tx_log_time_;
  ros::WallTime last_tx_write_time_;
  ros::WallTime last_control_tx_time_;
  ros::WallTime last_rx_log_time_;
  ros::WallTime last_rx_byte_time_;
  ros::WallTime last_rx_silence_warn_time_;
  ros::WallTime telemetry_velocity_quiet_until_;
  std::vector<uint8_t> rx_buffer_;
};

int main(int argc, char** argv) {
  ros::init(argc, argv, "mcore_velocity_sender");
  MCoreVelocitySenderNode node;
  ros::spin();
  return 0;
}

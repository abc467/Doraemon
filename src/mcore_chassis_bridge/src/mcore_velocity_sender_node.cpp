#include <errno.h>
#include <fcntl.h>
#include <termios.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <limits>
#include <sstream>
#include <string>
#include <vector>

#include <geometry_msgs/Twist.h>
#include <ros/ros.h>
#include <std_msgs/Bool.h>

namespace {

constexpr uint8_t kHead0 = 0x43;
constexpr uint8_t kHead1 = 0x4E;
constexpr uint8_t kTail = 0xDA;
constexpr uint16_t kVelocityCommand = 0x4060;
constexpr uint16_t kStatusCommand = 0x4070;
constexpr size_t kVelocityDataBytes = 8;
constexpr size_t kVelocityFrameBytes = kVelocityDataBytes + 8;
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

double ClampAbs(double value, double max_abs) {
  const double limit = std::fabs(max_abs);
  if (limit <= 0.0) {
    return value;
  }
  return std::max(-limit, std::min(limit, value));
}

bool IsFiniteTwist(const geometry_msgs::Twist& msg) {
  return std::isfinite(msg.linear.x) && std::isfinite(msg.angular.z);
}

bool IsFrameHead(uint8_t value) {
  return value == 0x43 || value == 0x42;
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
    pnh_.param<std::string>("serial_device", serial_device_, "/dev/mcore");
    pnh_.param<int>("serial_baudrate", serial_baudrate_, 115200);
    pnh_.param<std::string>("cmd_vel_topic", cmd_vel_topic_, "/cmd_vel");
    pnh_.param<double>("send_rate_hz", send_rate_hz_, 20.0);
    pnh_.param<double>("cmd_timeout_sec", cmd_timeout_sec_, 0.5);
    pnh_.param<double>("reconnect_interval_sec", reconnect_interval_sec_, 1.0);
    pnh_.param<double>("write_timeout_sec", write_timeout_sec_, 0.05);
    pnh_.param<double>("linear_velocity_scale", linear_velocity_scale_, 1000.0);
    pnh_.param<double>("angular_velocity_scale", angular_velocity_scale_, 1000.0);
    pnh_.param<double>("linear_velocity_sign", linear_velocity_sign_, 1.0);
    pnh_.param<double>("angular_velocity_sign", angular_velocity_sign_, 1.0);
    pnh_.param<double>("max_abs_linear_velocity", max_abs_linear_velocity_, 0.0);
    pnh_.param<double>("max_abs_angular_velocity", max_abs_angular_velocity_, 0.0);
    pnh_.param<bool>("send_immediately", send_immediately_, true);
    pnh_.param<bool>("send_zero_on_connect", send_zero_on_connect_, true);
    pnh_.param<bool>("send_zero_on_shutdown", send_zero_on_shutdown_, true);
    pnh_.param<bool>("drain_after_write", drain_after_write_, true);
    pnh_.param<bool>("enable_tx_log", enable_tx_log_, true);
    pnh_.param<double>("tx_log_interval_sec", tx_log_interval_sec_, 1.0);
    pnh_.param<bool>("enable_rx_log", enable_rx_log_, true);
    pnh_.param<bool>("enable_rx_raw_log", enable_rx_raw_log_, false);
    pnh_.param<double>("rx_log_interval_sec", rx_log_interval_sec_, 1.0);
    pnh_.param<double>("rx_silence_warn_sec", rx_silence_warn_sec_, 5.0);

    send_rate_hz_ = std::max(1.0, send_rate_hz_);
    reconnect_interval_sec_ = std::max(0.1, reconnect_interval_sec_);
    write_timeout_sec_ = std::max(0.001, write_timeout_sec_);

    connected_pub_ = pnh_.advertise<std_msgs::Bool>("connected", 1, true);
    PublishConnected(false, true);

    cmd_vel_sub_ = nh_.subscribe(cmd_vel_topic_, 20, &MCoreVelocitySenderNode::CmdVelCallback, this);
    send_timer_ = nh_.createTimer(ros::Duration(1.0 / send_rate_hz_),
                                  &MCoreVelocitySenderNode::SendTimerCallback,
                                  this);

    ROS_INFO_STREAM("M-core velocity sender topic=" << cmd_vel_topic_
                    << ", serial_device=" << serial_device_
                    << ", baudrate=" << serial_baudrate_
                    << ", send_rate_hz=" << send_rate_hz_
                    << ", cmd_timeout_sec=" << cmd_timeout_sec_
                    << ", linear_velocity_scale=" << linear_velocity_scale_
                    << ", angular_velocity_scale=" << angular_velocity_scale_
                    << ", drain_after_write=" << (drain_after_write_ ? "true" : "false")
                    << ", enable_tx_log=" << (enable_tx_log_ ? "true" : "false")
                    << ", enable_rx_log=" << (enable_rx_log_ ? "true" : "false"));
  }

  ~MCoreVelocitySenderNode() {
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

    last_vx_ = ClampAbs(msg.linear.x * linear_velocity_scale_ * linear_velocity_sign_,
                        max_abs_linear_velocity_);
    last_wz_ = ClampAbs(msg.angular.z * angular_velocity_scale_ * angular_velocity_sign_,
                        max_abs_angular_velocity_);
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
      vx = last_vx_;
      wz = last_wz_;
    } else if (have_cmd_ && !timeout_logged_) {
      ROS_WARN_STREAM("cmd_vel timeout after " << cmd_timeout_sec_
                      << "s; sending zero velocity.");
      timeout_logged_ = true;
    }

    WriteCommand(vx, wz, fresh ? "timer_repeat" : "timeout_zero", false);
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

  bool OpenSerial() {
    CloseSerial();

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
    rx_buffer_.clear();
    last_rx_byte_time_ = ros::WallTime::now();
    last_rx_silence_warn_time_ = ros::WallTime();
    PublishConnected(true);
    ROS_INFO_STREAM("Opened M-core serial device: " << serial_device_);

    if (send_zero_on_connect_ && !WriteCommand(0.0, 0.0, "connect_zero", true)) {
      CloseSerial();
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

  bool WriteAll(const uint8_t* data, size_t len) {
    if (serial_fd_ < 0) {
      return false;
    }

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
          ROS_WARN_STREAM("M-core serial write timeout after " << write_timeout_sec_ << "s.");
          CloseSerial();
          return false;
        }
        usleep(1000);
        continue;
      }

      ROS_WARN_STREAM("M-core serial write() failed: " << std::strerror(errno));
      CloseSerial();
      return false;
    }
    if (drain_after_write_ && !DrainSerialOutput()) {
      return false;
    }
    return offset == len;
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

      if (n == 0 || errno == EAGAIN || errno == EWOULDBLOCK) {
        return;
      }

      if (errno == EINTR) {
        continue;
      }

      ROS_WARN_STREAM("M-core serial read() failed: " << std::strerror(errno));
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
      LogRxFrame(cmd, rx_buffer_.data(), frame_len);
      rx_buffer_.erase(rx_buffer_.begin(),
                       rx_buffer_.begin() + static_cast<std::ptrdiff_t>(frame_len));
    }
  }

  void LogRxFrame(uint16_t cmd, const uint8_t* frame, size_t len) {
    if (!enable_rx_log_) {
      return;
    }

    const ros::WallTime now = ros::WallTime::now();
    if (rx_log_interval_sec_ > 0.0 && !last_rx_log_time_.isZero() &&
        (now - last_rx_log_time_).toSec() < rx_log_interval_sec_) {
      return;
    }
    last_rx_log_time_ = now;

    if (cmd == kStatusCommand) {
      LogStatusFrame(frame, len);
      return;
    }

    if (cmd == kVelocityCommand) {
      LogVelocityFeedbackFrame(frame, len);
      return;
    }

    ROS_INFO_STREAM("M-core rx cmd=0x" << std::uppercase << std::hex << std::setw(4)
                    << std::setfill('0') << static_cast<unsigned int>(cmd)
                    << std::dec << ", bytes=" << len
                    << ", frame=" << HexString(frame, len));
  }

  void LogVelocityFeedbackFrame(const uint8_t* frame, size_t len) {
    if (len < kVelocityFrameBytes) {
      ROS_INFO_STREAM("M-core rx velocity feedback too short, bytes=" << len
                      << ", frame=" << HexString(frame, len));
      return;
    }

    const float raw_vx = ReadFloat32LittleEndian(&frame[6]);
    const float raw_wz = ReadFloat32LittleEndian(&frame[10]);
    const double ros_vx = linear_velocity_scale_ == 0.0 ? 0.0 : raw_vx / linear_velocity_scale_;
    const double ros_wz = angular_velocity_scale_ == 0.0 ? 0.0 : raw_wz / angular_velocity_scale_;
    ROS_INFO_STREAM("M-core rx velocity_feedback raw_vx=" << raw_vx
                    << ", raw_wz=" << raw_wz
                    << ", approx_ros_vx=" << ros_vx
                    << ", approx_ros_wz=" << ros_wz
                    << ", bytes=" << len
                    << ", frame=" << HexString(frame, len));
  }

  void LogStatusFrame(const uint8_t* frame, size_t len) {
    if (len < 16) {
      ROS_INFO_STREAM("M-core rx status too short, bytes=" << len
                      << ", frame=" << HexString(frame, len));
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
  ros::Publisher connected_pub_;
  ros::Timer send_timer_;

  std::string serial_device_;
  int serial_baudrate_ = 115200;
  std::string cmd_vel_topic_;
  double send_rate_hz_ = 20.0;
  double cmd_timeout_sec_ = 0.5;
  double reconnect_interval_sec_ = 1.0;
  double write_timeout_sec_ = 0.05;
  double linear_velocity_scale_ = 1000.0;
  double angular_velocity_scale_ = 1000.0;
  double linear_velocity_sign_ = 1.0;
  double angular_velocity_sign_ = 1.0;
  double max_abs_linear_velocity_ = 0.0;
  double max_abs_angular_velocity_ = 0.0;
  bool send_immediately_ = true;
  bool send_zero_on_connect_ = true;
  bool send_zero_on_shutdown_ = true;
  bool drain_after_write_ = true;
  bool enable_tx_log_ = true;
  double tx_log_interval_sec_ = 1.0;
  bool enable_rx_log_ = true;
  bool enable_rx_raw_log_ = false;
  double rx_log_interval_sec_ = 1.0;
  double rx_silence_warn_sec_ = 5.0;

  int serial_fd_ = -1;
  bool connected_ = false;
  ros::WallTime last_open_attempt_;
  ros::Time last_cmd_time_;
  bool have_cmd_ = false;
  bool timeout_logged_ = false;
  double last_vx_ = 0.0;
  double last_wz_ = 0.0;
  ros::WallTime last_tx_log_time_;
  ros::WallTime last_rx_log_time_;
  ros::WallTime last_rx_byte_time_;
  ros::WallTime last_rx_silence_warn_time_;
  std::vector<uint8_t> rx_buffer_;
};

int main(int argc, char** argv) {
  ros::init(argc, argv, "mcore_velocity_sender");
  MCoreVelocitySenderNode node;
  ros::spin();
  return 0;
}

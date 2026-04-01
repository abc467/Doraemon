#include <errno.h>
#include <fcntl.h>
#include <termios.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <sstream>
#include <string>
#include <vector>

#include <geometry_msgs/Twist.h>
#include <ros/ros.h>

namespace {

constexpr size_t kFrameBytes = 14;
constexpr size_t kMaxBufferBytes = 4096;
constexpr std::array<uint8_t, 2> kFrameHeader = {0x48, 0x44};
constexpr uint8_t kFrameTail = 0xDA;
constexpr size_t kControlByteOffset = 10;  // 1-based byte 11.

constexpr uint8_t kControlStop = 0xF4;
constexpr uint8_t kControlForward = 0x74;
constexpr uint8_t kControlBackward = 0xB4;
constexpr uint8_t kControlRotateLeft = 0xD4;
constexpr uint8_t kControlRotateRight = 0xE4;

enum class CommandType {
  kStop,
  kForward,
  kBackward,
  kRotateLeft,
  kRotateRight,
};

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

bool StartsWithFrameHeader(const std::vector<uint8_t>& buffer) {
  return buffer.size() >= kFrameHeader.size() && buffer[0] == kFrameHeader[0] &&
         buffer[1] == kFrameHeader[1];
}

bool HasCompleteFrameAtFront(const std::vector<uint8_t>& buffer) {
  return buffer.size() >= kFrameBytes && StartsWithFrameHeader(buffer) &&
         buffer[kFrameBytes - 1] == kFrameTail;
}

}  // namespace

class ReceiverCmdVelNode {
 public:
  ReceiverCmdVelNode() : nh_(), pnh_("~") {
    pnh_.param<std::string>("serial_device", serial_device_, "/dev/joy_receiver");
    pnh_.param<int>("serial_baudrate", serial_baudrate_, 115200);
    pnh_.param<std::string>("cmd_vel_topic", cmd_vel_topic_, "/cmd_vel");
    pnh_.param<double>("linear_speed", linear_speed_, 0.4);
    pnh_.param<double>("angular_speed", angular_speed_, 0.4);
    pnh_.param<double>("reconnect_interval_sec", reconnect_interval_sec_, 1.0);
    pnh_.param<double>("warn_interval_sec", warn_interval_sec_, 5.0);
    pnh_.param<double>("loop_rate_hz", loop_rate_hz_, 100.0);
    pnh_.param<bool>("publish_stop_on_disconnect", publish_stop_on_disconnect_, true);
    pnh_.param<bool>("enable_rx_log", enable_rx_log_, false);
    pnh_.param<bool>("enable_rx_raw_log", enable_rx_raw_log_, false);

    reconnect_interval_sec_ = std::max(0.1, reconnect_interval_sec_);
    warn_interval_sec_ = std::max(0.1, warn_interval_sec_);
    loop_rate_hz_ = std::max(1.0, loop_rate_hz_);
    linear_speed_ = std::max(0.0, linear_speed_);
    angular_speed_ = std::max(0.0, angular_speed_);

    cmd_vel_pub_ = nh_.advertise<geometry_msgs::Twist>(cmd_vel_topic_, 20);
    ROS_INFO_STREAM("Receiver cmd_vel topic: " << cmd_vel_topic_);
    ROS_INFO_STREAM("Receiver serial device: " << serial_device_
                    << ", baudrate=" << serial_baudrate_
                    << ", linear_speed=" << linear_speed_
                    << ", angular_speed=" << angular_speed_);
  }

  ~ReceiverCmdVelNode() {
    CloseSerial();
  }

  void Run() {
    ros::Rate loop_rate(loop_rate_hz_);

    while (ros::ok()) {
      if (serial_fd_ < 0) {
        if (!OpenSerial()) {
          WarnEvery("Serial error: failed to open receiver serial device " + serial_device_ + ".");
          ros::spinOnce();
          loop_rate.sleep();
          ros::Duration(reconnect_interval_sec_).sleep();
          continue;
        }
        ROS_INFO_STREAM("Opened receiver serial device: " << serial_device_);
      }

      ReadAvailableBytes();
      ros::spinOnce();
      loop_rate.sleep();
    }
  }

 private:
  bool OpenSerial() {
    CloseSerial();

    serial_fd_ = open(serial_device_.c_str(), O_RDONLY | O_NOCTTY | O_NONBLOCK);
    if (serial_fd_ < 0) {
      return false;
    }

    termios tty;
    std::memset(&tty, 0, sizeof(tty));
    if (tcgetattr(serial_fd_, &tty) != 0) {
      WarnEvery(std::string("Serial error: tcgetattr() failed: ") + std::strerror(errno));
      CloseSerial();
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

    if (tcsetattr(serial_fd_, TCSANOW, &tty) != 0) {
      WarnEvery(std::string("Serial error: tcsetattr() failed: ") + std::strerror(errno));
      CloseSerial();
      return false;
    }

    tcflush(serial_fd_, TCIFLUSH);
    rx_buffer_.clear();
    return true;
  }

  void CloseSerial() {
    if (serial_fd_ >= 0) {
      close(serial_fd_);
      serial_fd_ = -1;
    }
  }

  void ReadAvailableBytes() {
    uint8_t chunk[256];

    while (ros::ok()) {
      const ssize_t n = read(serial_fd_, chunk, sizeof(chunk));
      if (n > 0) {
        if (enable_rx_raw_log_) {
          ROS_INFO_STREAM("Receiver raw bytes (" << n << "): "
                          << HexString(chunk, static_cast<size_t>(n)));
        }
        rx_buffer_.insert(rx_buffer_.end(), chunk, chunk + n);
        if (rx_buffer_.size() > kMaxBufferBytes) {
          rx_buffer_.erase(rx_buffer_.begin(),
                           rx_buffer_.begin() +
                               static_cast<std::ptrdiff_t>(rx_buffer_.size() / 2));
        }
        ParseBuffer();
        continue;
      }

      if (n == 0 || errno == EAGAIN || errno == EWOULDBLOCK) {
        return;
      }

      if (errno == EINTR) {
        continue;
      }

      WarnEvery(std::string("Serial error: read() failed: ") + std::strerror(errno));
      CloseSerial();
      PublishStopIfNeeded();
      return;
    }
  }

  void ParseBuffer() {
    while (rx_buffer_.size() >= 2) {
      const auto header_pos =
          std::search(rx_buffer_.begin(), rx_buffer_.end(), kFrameHeader.begin(), kFrameHeader.end());
      if (header_pos == rx_buffer_.end()) {
        rx_buffer_.clear();
        return;
      }

      if (header_pos != rx_buffer_.begin()) {
        rx_buffer_.erase(rx_buffer_.begin(), header_pos);
      }

      if (rx_buffer_.size() < kFrameBytes) {
        return;
      }

      if (!HasCompleteFrameAtFront(rx_buffer_)) {
        if (enable_rx_log_ && StartsWithFrameHeader(rx_buffer_)) {
          ROS_WARN_STREAM("Malformed receiver frame: "
                          << HexString(rx_buffer_.data(), kFrameBytes));
        }
        rx_buffer_.erase(rx_buffer_.begin());
        continue;
      }

      CommandType command;
      if (DecodeCommand(&command)) {
        PublishCommand(command);
        rx_buffer_.erase(rx_buffer_.begin(), rx_buffer_.begin() + static_cast<std::ptrdiff_t>(kFrameBytes));
        continue;
      }

      if (enable_rx_log_) {
        ROS_WARN_STREAM("Unknown receiver control byte 0x"
                        << std::uppercase << std::hex << std::setw(2) << std::setfill('0')
                        << static_cast<unsigned int>(rx_buffer_[kControlByteOffset])
                        << std::dec << " in frame: "
                        << HexString(rx_buffer_.data(), kFrameBytes));
      }
      rx_buffer_.erase(rx_buffer_.begin(),
                       rx_buffer_.begin() + static_cast<std::ptrdiff_t>(kFrameBytes));
    }
  }

  bool DecodeCommand(CommandType* command) const {
    if (command == nullptr || !HasCompleteFrameAtFront(rx_buffer_)) {
      return false;
    }

    switch (rx_buffer_[kControlByteOffset]) {
      case kControlStop:
        *command = CommandType::kStop;
        return true;
      case kControlForward:
        *command = CommandType::kForward;
        return true;
      case kControlBackward:
        *command = CommandType::kBackward;
        return true;
      case kControlRotateLeft:
        *command = CommandType::kRotateLeft;
        return true;
      case kControlRotateRight:
        *command = CommandType::kRotateRight;
        return true;
    }

    return false;
  }

  void PublishCommand(CommandType command) {
    geometry_msgs::Twist msg;
    switch (command) {
      case CommandType::kStop:
        break;
      case CommandType::kForward:
        msg.linear.x = linear_speed_;
        break;
      case CommandType::kBackward:
        msg.linear.x = -linear_speed_;
        break;
      case CommandType::kRotateLeft:
        msg.angular.z = angular_speed_;
        break;
      case CommandType::kRotateRight:
        msg.angular.z = -angular_speed_;
        break;
    }

    cmd_vel_pub_.publish(msg);
    has_published_command_ = true;
    if (!has_last_command_ || last_command_ != command) {
      ROS_INFO_STREAM("Published receiver cmd_vel command: " << CommandName(command)
                      << " linear.x=" << msg.linear.x
                      << " angular.z=" << msg.angular.z);
      last_command_ = command;
      has_last_command_ = true;
    }
  }

  void PublishStopIfNeeded() {
    if (!publish_stop_on_disconnect_ || !has_published_command_) {
      return;
    }
    PublishCommand(CommandType::kStop);
  }

  std::string CommandName(CommandType command) const {
    switch (command) {
      case CommandType::kStop:
        return "stop";
      case CommandType::kForward:
        return "forward";
      case CommandType::kBackward:
        return "backward";
      case CommandType::kRotateLeft:
        return "rotate_left";
      case CommandType::kRotateRight:
        return "rotate_right";
    }
    return "unknown";
  }

  void WarnEvery(const std::string& msg) {
    const ros::Time now = ros::Time::now();
    if (last_warn_stamp_.isZero() ||
        (now - last_warn_stamp_).toSec() >= warn_interval_sec_) {
      ROS_WARN_STREAM(msg);
      last_warn_stamp_ = now;
    }
  }

  ros::NodeHandle nh_;
  ros::NodeHandle pnh_;
  ros::Publisher cmd_vel_pub_;

  std::string serial_device_;
  int serial_baudrate_ = 115200;
  std::string cmd_vel_topic_ = "/cmd_vel";
  double linear_speed_ = 0.4;
  double angular_speed_ = 0.4;
  double reconnect_interval_sec_ = 1.0;
  double warn_interval_sec_ = 5.0;
  double loop_rate_hz_ = 100.0;
  bool publish_stop_on_disconnect_ = true;
  bool enable_rx_log_ = false;
  bool enable_rx_raw_log_ = false;

  int serial_fd_ = -1;
  std::vector<uint8_t> rx_buffer_;
  ros::Time last_warn_stamp_;
  bool has_last_command_ = false;
  bool has_published_command_ = false;
  CommandType last_command_ = CommandType::kStop;
};

int main(int argc, char** argv) {
  ros::init(argc, argv, "receiver_cmd_vel");
  ReceiverCmdVelNode node;
  node.Run();
  return 0;
}

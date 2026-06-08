#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include <ros/ros.h>
#include <sensor_msgs/Imu.h>
#include <serial/serial.h>
#include <tf/transform_datatypes.h>
#include <xmlrpcpp/XmlRpcValue.h>

namespace {

constexpr uint8_t kModbusReadHoldingRegisters = 0x03;
constexpr uint16_t kAccelGyroRegisterStart = 0x0064;
constexpr uint16_t kAccelGyroRegisterCount = 0x000c;
constexpr uint16_t kAngleRegisterStart = 0x0084;
constexpr uint16_t kAngleRegisterCount = 0x000c;
constexpr double kGravityMps2 = 9.80665;
constexpr double kPi = 3.14159265358979323846;
constexpr double kDegToRad = kPi / 180.0;

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

bool LoadDoubleVectorParam(const ros::NodeHandle& nh, const std::string& name,
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
    ROS_WARN_STREAM("Parameter " << name << " must be an array with "
                                  << values->size()
                                  << " numeric entries. Keep current default.");
    return false;
  }

  std::vector<double> parsed(values->size(), 0.0);
  for (int i = 0; i < raw_value.size(); ++i) {
    if (!XmlRpcNumberToDouble(raw_value[i], &parsed[static_cast<size_t>(i)])) {
      ROS_WARN_STREAM("Parameter " << name << "[" << i
                                    << "] must be numeric. Keep current default.");
      return false;
    }
  }
  *values = parsed;
  return true;
}

std::array<double, 3> VectorToArray3(const std::vector<double>& values) {
  std::array<double, 3> result = {{0.0, 0.0, 0.0}};
  for (size_t i = 0; i < std::min<size_t>(3, values.size()); ++i) {
    result[i] = values[i];
  }
  return result;
}

std::array<double, 9> Diagonal3ToCovariance9(const std::vector<double>& diagonal) {
  std::array<double, 9> covariance = {{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0}};
  for (size_t i = 0; i < std::min<size_t>(3, diagonal.size()); ++i) {
    covariance[i * 3 + i] = diagonal[i];
  }
  return covariance;
}

std::string FormatArray3(const std::array<double, 3>& values) {
  std::ostringstream oss;
  oss << "[" << values[0] << ", " << values[1] << ", " << values[2] << "]";
  return oss.str();
}

double VectorNorm3(double x, double y, double z) {
  return std::sqrt(x * x + y * y + z * z);
}

uint16_t ModbusCrc16(const uint8_t* data, const size_t length) {
  uint16_t crc = 0xffff;
  for (size_t i = 0; i < length; ++i) {
    crc ^= data[i];
    for (int bit = 0; bit < 8; ++bit) {
      if ((crc & 0x0001) != 0) {
        crc >>= 1;
        crc ^= 0xa001;
      } else {
        crc >>= 1;
      }
    }
  }
  return crc;
}

float ParseFloatAbcd(const std::vector<uint8_t>& bytes, const size_t offset) {
  const uint32_t word = (static_cast<uint32_t>(bytes[offset]) << 24) |
                        (static_cast<uint32_t>(bytes[offset + 1]) << 16) |
                        (static_cast<uint32_t>(bytes[offset + 2]) << 8) |
                        static_cast<uint32_t>(bytes[offset + 3]);
  float value = 0.0f;
  std::memcpy(&value, &word, sizeof(value));
  return value;
}

void CopyCovariance(const std::array<double, 9>& source,
                    boost::array<double, 9>* target) {
  if (target == nullptr) {
    return;
  }
  std::copy(source.begin(), source.end(), target->begin());
}

}  // namespace

class ModbusImuSerialNode {
 public:
  ModbusImuSerialNode() : nh_(), pnh_("~") {
    LoadParams();

    imu_pub_ = nh_.advertise<sensor_msgs::Imu>(imu_topic_, queue_size_);
    if (publish_raw_imu_) {
      raw_imu_pub_ = nh_.advertise<sensor_msgs::Imu>(raw_imu_topic_, queue_size_);
    }

    OpenSerial();
    ROS_INFO_STREAM("Modbus IMU serial node ready: port=" << serial_port_
                    << ", baud=" << serial_baud_
                    << ", slave_address=" << static_cast<int>(slave_address_)
                    << ", imu_topic=" << imu_topic_
                    << ", frame_id=" << frame_id_);
    if (enable_gyro_bias_compensation_) {
      ROS_INFO_STREAM("Gyro bias compensation enabled. Initial bias rad/s="
                      << FormatArray3(applied_gyro_bias_rad_s_));
    }
  }

  void Spin() {
    ros::Rate rate(std::max(1.0, poll_rate_hz_));
    int loop_count = 0;
    while (ros::ok()) {
      Sample sample;
      if (ReadAccelGyroSample(&sample)) {
        if (publish_orientation_ && angle_poll_divider_ > 0 &&
            loop_count % angle_poll_divider_ == 0) {
          ReadAngleSample(&sample);
        }

        UpdateGyroBiasEstimate(sample);
        UpdateAccelBiasEstimate(sample);

        const sensor_msgs::Imu raw_msg = BuildImuMessage(sample, false);
        if (publish_raw_imu_) {
          raw_imu_pub_.publish(raw_msg);
        }

        if (ShouldPublishCorrectedImu()) {
          imu_pub_.publish(BuildImuMessage(sample, true));
        }
      }

      ros::spinOnce();
      rate.sleep();
      ++loop_count;
    }
  }

 private:
  struct Sample {
    ros::Time request_stamp;
    ros::Time response_stamp;
    ros::Time stamp;
    std::array<double, 3> accel_g = {{0.0, 0.0, 0.0}};
    std::array<double, 3> gyro_rad_s = {{0.0, 0.0, 0.0}};
    std::array<double, 3> rpy_rad = {{0.0, 0.0, 0.0}};
    bool has_orientation = false;
  };

  void LoadParams() {
    pnh_.param<std::string>("port", serial_port_, "/dev/imu");
    pnh_.param<int>("baud", serial_baud_, 115200);
    pnh_.param<int>("serial_timeout_ms", serial_timeout_ms_, 50);
    pnh_.param<int>("queue_size", queue_size_, 100);
    pnh_.param<int>("slave_address", slave_address_param_, 1);
    pnh_.param<double>("poll_rate_hz", poll_rate_hz_, 100.0);
    pnh_.param<std::string>("imu_topic", imu_topic_, "/imu");
    pnh_.param<std::string>("raw_imu_topic", raw_imu_topic_, "/imu_raw");
    pnh_.param<std::string>("frame_id", frame_id_, "gyro_link");
    pnh_.param<bool>("publish_raw_imu", publish_raw_imu_, false);
    pnh_.param<bool>("debug", debug_, false);

    pnh_.param<bool>("use_midpoint_timestamp", use_midpoint_timestamp_, true);
    pnh_.param<bool>("enforce_monotonic_timestamps", enforce_monotonic_timestamps_, true);
    pnh_.param<double>("timestamp_offset_sec", timestamp_offset_sec_, 0.0);
    pnh_.param<double>("min_timestamp_step_sec", min_timestamp_step_sec_, 1e-6);
    pnh_.param<double>("max_future_timestamp_sec", max_future_timestamp_sec_, 0.02);

    pnh_.param<bool>("enable_gyro_bias_compensation",
                     enable_gyro_bias_compensation_, true);
    pnh_.param<bool>("estimate_gyro_bias_on_startup",
                     estimate_gyro_bias_on_startup_, true);
    pnh_.param<double>("gyro_bias_estimation_duration_sec",
                       gyro_bias_estimation_duration_sec_, 5.0);
    pnh_.param<double>("gyro_bias_stationary_threshold_rad_s",
                       gyro_bias_stationary_threshold_rad_s_, 0.02);
    pnh_.param<int>("gyro_bias_min_samples", gyro_bias_min_samples_, 200);

    pnh_.param<bool>("enable_accel_bias_compensation",
                     enable_accel_bias_compensation_, false);
    pnh_.param<bool>("estimate_accel_bias_on_startup",
                     estimate_accel_bias_on_startup_, false);
    pnh_.param<double>("accel_bias_estimation_duration_sec",
                       accel_bias_estimation_duration_sec_, 5.0);
    pnh_.param<double>("accel_bias_stationary_tolerance_g",
                       accel_bias_stationary_tolerance_g_, 0.15);
    pnh_.param<int>("accel_bias_min_samples", accel_bias_min_samples_, 200);
    pnh_.param<bool>("publish_during_calibration", publish_during_calibration_, false);

    pnh_.param<bool>("publish_orientation", publish_orientation_, false);
    pnh_.param<int>("angle_poll_divider", angle_poll_divider_, 5);
    pnh_.param<std::string>("angle_source", angle_source_, "dynamic");

    std::vector<double> linear_acceleration_axis_signs = {1.0, 1.0, 1.0};
    std::vector<double> angular_velocity_axis_signs = {1.0, 1.0, 1.0};
    std::vector<double> orientation_rpy_signs = {1.0, 1.0, 1.0};
    std::vector<double> gyro_bias_rad_s = {0.0, 0.0, 0.0};
    std::vector<double> accel_bias_g = {0.0, 0.0, 0.0};
    std::vector<double> accel_bias_target_g = {0.0, 0.0, 1.0};
    std::vector<double> orientation_covariance_diagonal = {1000000.0, 1000000.0, 1000000.0};
    std::vector<double> angular_velocity_covariance_diagonal = {1000000.0, 1000000.0, 0.05};
    std::vector<double> linear_acceleration_covariance_diagonal = {1000.0, 1000.0, 1000.0};

    LoadDoubleVectorParam(pnh_, "linear_acceleration_axis_signs",
                          &linear_acceleration_axis_signs);
    LoadDoubleVectorParam(pnh_, "angular_velocity_axis_signs",
                          &angular_velocity_axis_signs);
    LoadDoubleVectorParam(pnh_, "orientation_rpy_signs", &orientation_rpy_signs);
    LoadDoubleVectorParam(pnh_, "gyro_bias_rad_s", &gyro_bias_rad_s);
    LoadDoubleVectorParam(pnh_, "accel_bias_g", &accel_bias_g);
    LoadDoubleVectorParam(pnh_, "accel_bias_target_g", &accel_bias_target_g);
    LoadDoubleVectorParam(pnh_, "orientation_covariance_diagonal",
                          &orientation_covariance_diagonal);
    LoadDoubleVectorParam(pnh_, "angular_velocity_covariance_diagonal",
                          &angular_velocity_covariance_diagonal);
    LoadDoubleVectorParam(pnh_, "linear_acceleration_covariance_diagonal",
                          &linear_acceleration_covariance_diagonal);

    linear_acceleration_axis_signs_ = VectorToArray3(linear_acceleration_axis_signs);
    angular_velocity_axis_signs_ = VectorToArray3(angular_velocity_axis_signs);
    orientation_rpy_signs_ = VectorToArray3(orientation_rpy_signs);
    configured_gyro_bias_rad_s_ = VectorToArray3(gyro_bias_rad_s);
    applied_gyro_bias_rad_s_ = configured_gyro_bias_rad_s_;
    configured_accel_bias_g_ = VectorToArray3(accel_bias_g);
    applied_accel_bias_g_ = configured_accel_bias_g_;
    accel_bias_target_g_ = VectorToArray3(accel_bias_target_g);
    orientation_covariance_ = Diagonal3ToCovariance9(orientation_covariance_diagonal);
    angular_velocity_covariance_ =
        Diagonal3ToCovariance9(angular_velocity_covariance_diagonal);
    linear_acceleration_covariance_ =
        Diagonal3ToCovariance9(linear_acceleration_covariance_diagonal);

    slave_address_param_ = std::max(1, std::min(247, slave_address_param_));
    slave_address_ = static_cast<uint8_t>(slave_address_param_);
    serial_timeout_ms_ = std::max(1, serial_timeout_ms_);
    queue_size_ = std::max(1, queue_size_);
    poll_rate_hz_ = std::max(1.0, poll_rate_hz_);
    gyro_bias_estimation_duration_sec_ =
        std::max(0.0, gyro_bias_estimation_duration_sec_);
    gyro_bias_stationary_threshold_rad_s_ =
        std::max(0.0, gyro_bias_stationary_threshold_rad_s_);
    gyro_bias_min_samples_ = std::max(1, gyro_bias_min_samples_);
    accel_bias_estimation_duration_sec_ =
        std::max(0.0, accel_bias_estimation_duration_sec_);
    accel_bias_stationary_tolerance_g_ =
        std::max(0.0, accel_bias_stationary_tolerance_g_);
    accel_bias_min_samples_ = std::max(1, accel_bias_min_samples_);
    angle_poll_divider_ = std::max(1, angle_poll_divider_);
    min_timestamp_step_sec_ = std::max(1e-9, min_timestamp_step_sec_);
    max_future_timestamp_sec_ = std::max(0.0, max_future_timestamp_sec_);
  }

  void OpenSerial() {
    try {
      serial_.setPort(serial_port_);
      serial_.setBaudrate(serial_baud_);
      serial_.setFlowcontrol(serial::flowcontrol_none);
      serial_.setParity(serial::parity_none);
      serial_.setStopbits(serial::stopbits_one);
      serial_.setBytesize(serial::eightbits);
      serial::Timeout timeout = serial::Timeout::simpleTimeout(serial_timeout_ms_);
      serial_.setTimeout(timeout);
      serial_.open();
      serial_.flushInput();
    } catch (const serial::IOException& e) {
      ROS_FATAL_STREAM("Unable to open IMU serial port " << serial_port_ << ": "
                       << e.what());
      throw;
    }

    if (!serial_.isOpen()) {
      throw std::runtime_error("IMU serial port did not open");
    }
  }

  std::vector<uint8_t> BuildReadRequest(const uint16_t start_register,
                                        const uint16_t register_count) const {
    std::vector<uint8_t> request;
    request.reserve(8);
    request.push_back(slave_address_);
    request.push_back(kModbusReadHoldingRegisters);
    request.push_back(static_cast<uint8_t>((start_register >> 8) & 0xff));
    request.push_back(static_cast<uint8_t>(start_register & 0xff));
    request.push_back(static_cast<uint8_t>((register_count >> 8) & 0xff));
    request.push_back(static_cast<uint8_t>(register_count & 0xff));
    const uint16_t crc = ModbusCrc16(request.data(), request.size());
    request.push_back(static_cast<uint8_t>(crc & 0xff));
    request.push_back(static_cast<uint8_t>((crc >> 8) & 0xff));
    return request;
  }

  bool ReadExact(const size_t size, std::vector<uint8_t>* output) {
    if (output == nullptr) {
      return false;
    }
    output->clear();
    output->reserve(size);

    const ros::WallTime deadline =
        ros::WallTime::now() + ros::WallDuration(serial_timeout_ms_ / 1000.0);
    while (ros::ok() && output->size() < size && ros::WallTime::now() < deadline) {
      const size_t remaining = size - output->size();
      std::vector<uint8_t> chunk;
      const size_t got = serial_.read(chunk, remaining);
      if (got > 0) {
        output->insert(output->end(), chunk.begin(), chunk.end());
      } else {
        ros::WallDuration(0.001).sleep();
      }
    }

    return output->size() == size;
  }

  bool ReadRegisters(const uint16_t start_register, const uint16_t register_count,
                     std::vector<uint8_t>* payload, ros::Time* request_stamp,
                     ros::Time* response_stamp) {
    if (payload == nullptr || request_stamp == nullptr || response_stamp == nullptr) {
      return false;
    }
    if (!serial_.isOpen()) {
      ROS_ERROR_THROTTLE(2.0, "IMU serial port is not open.");
      return false;
    }

    const std::vector<uint8_t> request = BuildReadRequest(start_register, register_count);
    try {
      serial_.flushInput();
      *request_stamp = ros::Time::now();
      const size_t written = serial_.write(request);
      if (written != request.size()) {
        ROS_WARN_THROTTLE(2.0, "Incomplete IMU Modbus request write.");
        return false;
      }

      std::vector<uint8_t> header;
      if (!ReadExact(3, &header)) {
        ROS_WARN_THROTTLE(2.0, "Timed out reading IMU Modbus response header.");
        return false;
      }

      const uint8_t response_address = header[0];
      const uint8_t response_function = header[1];
      const uint8_t byte_count = header[2];
      if (response_address != slave_address_) {
        ROS_WARN_STREAM_THROTTLE(2.0, "Unexpected IMU Modbus slave address: "
                                         << static_cast<int>(response_address));
        return false;
      }

      if ((response_function & 0x80) != 0) {
        std::vector<uint8_t> crc_bytes;
        ReadExact(2, &crc_bytes);
        ROS_WARN_STREAM_THROTTLE(2.0, "IMU Modbus exception response. function=0x"
                                         << std::hex << static_cast<int>(response_function)
                                         << ", code=0x" << static_cast<int>(byte_count)
                                         << std::dec);
        return false;
      }

      const uint8_t expected_byte_count =
          static_cast<uint8_t>(register_count * 2);
      if (response_function != kModbusReadHoldingRegisters ||
          byte_count != expected_byte_count) {
        ROS_WARN_STREAM_THROTTLE(2.0, "Unexpected IMU Modbus response header. function=0x"
                                         << std::hex << static_cast<int>(response_function)
                                         << std::dec << ", byte_count="
                                         << static_cast<int>(byte_count)
                                         << ", expected="
                                         << static_cast<int>(expected_byte_count));
        return false;
      }

      std::vector<uint8_t> tail;
      if (!ReadExact(static_cast<size_t>(byte_count) + 2, &tail)) {
        ROS_WARN_THROTTLE(2.0, "Timed out reading IMU Modbus response payload.");
        return false;
      }
      *response_stamp = ros::Time::now();

      std::vector<uint8_t> frame = header;
      frame.insert(frame.end(), tail.begin(), tail.end());
      const uint16_t received_crc =
          static_cast<uint16_t>(frame[frame.size() - 2]) |
          (static_cast<uint16_t>(frame[frame.size() - 1]) << 8);
      const uint16_t calculated_crc = ModbusCrc16(frame.data(), frame.size() - 2);
      if (received_crc != calculated_crc) {
        ROS_WARN_STREAM_THROTTLE(2.0, "IMU Modbus CRC mismatch. received=0x"
                                         << std::hex << received_crc
                                         << ", calculated=0x" << calculated_crc
                                         << std::dec);
        return false;
      }

      payload->assign(frame.begin() + 3, frame.end() - 2);
      return true;
    } catch (const std::exception& e) {
      ROS_WARN_STREAM_THROTTLE(2.0, "IMU Modbus serial read failed: " << e.what());
      return false;
    }
  }

  ros::Time ResolveTimestamp(const ros::Time& request_stamp,
                             const ros::Time& response_stamp) {
    ros::Time stamp = response_stamp;
    if (use_midpoint_timestamp_ && !request_stamp.isZero() && !response_stamp.isZero() &&
        response_stamp >= request_stamp) {
      stamp = request_stamp +
              ros::Duration((response_stamp - request_stamp).toSec() * 0.5);
    }
    stamp += ros::Duration(timestamp_offset_sec_);

    const ros::Time max_future_stamp =
        response_stamp + ros::Duration(max_future_timestamp_sec_);
    if (!response_stamp.isZero() && stamp > max_future_stamp) {
      stamp = response_stamp;
    }

    if (enforce_monotonic_timestamps_ && !last_stamp_.isZero() &&
        stamp <= last_stamp_) {
      stamp = last_stamp_ + ros::Duration(min_timestamp_step_sec_);
    }
    last_stamp_ = stamp;
    return stamp;
  }

  bool ReadAccelGyroSample(Sample* sample) {
    if (sample == nullptr) {
      return false;
    }

    std::vector<uint8_t> payload;
    ros::Time request_stamp;
    ros::Time response_stamp;
    if (!ReadRegisters(kAccelGyroRegisterStart, kAccelGyroRegisterCount, &payload,
                       &request_stamp, &response_stamp)) {
      return false;
    }
    if (payload.size() != 24) {
      ROS_WARN_STREAM_THROTTLE(2.0, "Unexpected accel/gyro payload size: "
                                       << payload.size());
      return false;
    }

    sample->request_stamp = request_stamp;
    sample->response_stamp = response_stamp;
    sample->stamp = ResolveTimestamp(request_stamp, response_stamp);
    for (size_t i = 0; i < 3; ++i) {
      const double accel_g = ParseFloatAbcd(payload, i * 4);
      sample->accel_g[i] = accel_g * linear_acceleration_axis_signs_[i];
    }
    for (size_t i = 0; i < 3; ++i) {
      const double gyro_deg_s = ParseFloatAbcd(payload, (i + 3) * 4);
      sample->gyro_rad_s[i] =
          gyro_deg_s * kDegToRad * angular_velocity_axis_signs_[i];
    }

    if (debug_) {
      ROS_INFO_STREAM_THROTTLE(1.0, "IMU accel_g="
                                      << FormatArray3(sample->accel_g)
                                      << ", gyro_rad_s="
                                      << FormatArray3(sample->gyro_rad_s));
    }
    return true;
  }

  bool ReadAngleSample(Sample* sample) {
    if (sample == nullptr) {
      return false;
    }

    std::vector<uint8_t> payload;
    ros::Time request_stamp;
    ros::Time response_stamp;
    if (!ReadRegisters(kAngleRegisterStart, kAngleRegisterCount, &payload,
                       &request_stamp, &response_stamp)) {
      return false;
    }
    if (payload.size() != 24) {
      ROS_WARN_STREAM_THROTTLE(2.0, "Unexpected angle payload size: "
                                       << payload.size());
      return false;
    }

    const size_t angle_offset = angle_source_ == "static" ? 0 : 12;
    for (size_t i = 0; i < 3; ++i) {
      const double angle_deg = ParseFloatAbcd(payload, angle_offset + i * 4);
      sample->rpy_rad[i] = angle_deg * kDegToRad * orientation_rpy_signs_[i];
    }
    sample->has_orientation = true;
    return true;
  }

  bool IsStationaryForBias(const Sample& sample) const {
    const double gyro_norm = VectorNorm3(sample.gyro_rad_s[0], sample.gyro_rad_s[1],
                                        sample.gyro_rad_s[2]);
    const double accel_norm =
        VectorNorm3(sample.accel_g[0], sample.accel_g[1], sample.accel_g[2]);
    return gyro_norm <= gyro_bias_stationary_threshold_rad_s_ &&
           std::fabs(accel_norm - 1.0) <= accel_bias_stationary_tolerance_g_;
  }

  void UpdateGyroBiasEstimate(const Sample& sample) {
    if (!enable_gyro_bias_compensation_ || !estimate_gyro_bias_on_startup_ ||
        gyro_bias_estimation_completed_) {
      return;
    }

    if (!gyro_bias_estimation_started_) {
      gyro_bias_estimation_started_ = true;
      gyro_bias_estimation_start_stamp_ = sample.stamp;
      ROS_INFO_STREAM("Startup gyro bias estimation started for "
                      << gyro_bias_estimation_duration_sec_ << " s. Keep IMU still.");
    }

    if (IsStationaryForBias(sample)) {
      for (size_t i = 0; i < 3; ++i) {
        gyro_bias_sum_rad_s_[i] += sample.gyro_rad_s[i];
      }
      ++gyro_bias_sample_count_;
    }

    if ((sample.stamp - gyro_bias_estimation_start_stamp_).toSec() >=
        gyro_bias_estimation_duration_sec_) {
      gyro_bias_estimation_completed_ = true;
      if (gyro_bias_sample_count_ < gyro_bias_min_samples_) {
        applied_gyro_bias_rad_s_ = configured_gyro_bias_rad_s_;
        ROS_WARN_STREAM("Startup gyro bias estimation collected only "
                        << gyro_bias_sample_count_
                        << " stationary samples, below min_samples="
                        << gyro_bias_min_samples_
                        << ". Keep configured bias rad/s="
                        << FormatArray3(configured_gyro_bias_rad_s_));
        return;
      }

      for (size_t i = 0; i < 3; ++i) {
        applied_gyro_bias_rad_s_[i] =
            gyro_bias_sum_rad_s_[i] /
            static_cast<double>(gyro_bias_sample_count_);
      }
      ROS_INFO_STREAM("Startup gyro bias estimated from "
                      << gyro_bias_sample_count_ << " samples. bias rad/s="
                      << FormatArray3(applied_gyro_bias_rad_s_));
    }
  }

  void UpdateAccelBiasEstimate(const Sample& sample) {
    if (!enable_accel_bias_compensation_ || !estimate_accel_bias_on_startup_ ||
        accel_bias_estimation_completed_) {
      return;
    }

    if (!accel_bias_estimation_started_) {
      accel_bias_estimation_started_ = true;
      accel_bias_estimation_start_stamp_ = sample.stamp;
      ROS_INFO_STREAM("Startup accel bias estimation started. target_g="
                      << FormatArray3(accel_bias_target_g_));
    }

    if (IsStationaryForBias(sample)) {
      for (size_t i = 0; i < 3; ++i) {
        accel_bias_sum_g_[i] += sample.accel_g[i] - accel_bias_target_g_[i];
      }
      ++accel_bias_sample_count_;
    }

    if ((sample.stamp - accel_bias_estimation_start_stamp_).toSec() >=
        accel_bias_estimation_duration_sec_) {
      accel_bias_estimation_completed_ = true;
      if (accel_bias_sample_count_ < accel_bias_min_samples_) {
        applied_accel_bias_g_ = configured_accel_bias_g_;
        ROS_WARN_STREAM("Startup accel bias estimation collected only "
                        << accel_bias_sample_count_
                        << " stationary samples, below min_samples="
                        << accel_bias_min_samples_
                        << ". Keep configured accel bias g="
                        << FormatArray3(configured_accel_bias_g_));
        return;
      }

      for (size_t i = 0; i < 3; ++i) {
        applied_accel_bias_g_[i] =
            accel_bias_sum_g_[i] / static_cast<double>(accel_bias_sample_count_);
      }
      ROS_INFO_STREAM("Startup accel bias estimated from "
                      << accel_bias_sample_count_ << " samples. bias g="
                      << FormatArray3(applied_accel_bias_g_));
    }
  }

  bool ShouldPublishCorrectedImu() const {
    if (publish_during_calibration_) {
      return true;
    }
    const bool gyro_ready = !enable_gyro_bias_compensation_ ||
                            !estimate_gyro_bias_on_startup_ ||
                            gyro_bias_estimation_completed_;
    const bool accel_ready = !enable_accel_bias_compensation_ ||
                             !estimate_accel_bias_on_startup_ ||
                             accel_bias_estimation_completed_;
    return gyro_ready && accel_ready;
  }

  sensor_msgs::Imu BuildImuMessage(const Sample& sample, const bool apply_bias) const {
    sensor_msgs::Imu msg;
    msg.header.stamp = sample.stamp;
    msg.header.frame_id = frame_id_;

    if (publish_orientation_ && sample.has_orientation) {
      msg.orientation =
          tf::createQuaternionMsgFromRollPitchYaw(sample.rpy_rad[0],
                                                  sample.rpy_rad[1],
                                                  sample.rpy_rad[2]);
      CopyCovariance(orientation_covariance_, &msg.orientation_covariance);
    } else {
      msg.orientation.w = 1.0;
      msg.orientation_covariance[0] = -1.0;
    }

    for (size_t i = 0; i < 3; ++i) {
      const double gyro_bias =
          apply_bias && enable_gyro_bias_compensation_ ? applied_gyro_bias_rad_s_[i] : 0.0;
      const double accel_bias =
          apply_bias && enable_accel_bias_compensation_ ? applied_accel_bias_g_[i] : 0.0;
      SetAngularVelocityComponent(i, sample.gyro_rad_s[i] - gyro_bias, &msg);
      SetLinearAccelerationComponent(i, (sample.accel_g[i] - accel_bias) * kGravityMps2,
                                     &msg);
    }

    CopyCovariance(angular_velocity_covariance_, &msg.angular_velocity_covariance);
    CopyCovariance(linear_acceleration_covariance_, &msg.linear_acceleration_covariance);
    return msg;
  }

  void SetAngularVelocityComponent(const size_t index, const double value,
                                   sensor_msgs::Imu* msg) const {
    if (index == 0) {
      msg->angular_velocity.x = value;
    } else if (index == 1) {
      msg->angular_velocity.y = value;
    } else {
      msg->angular_velocity.z = value;
    }
  }

  void SetLinearAccelerationComponent(const size_t index, const double value,
                                      sensor_msgs::Imu* msg) const {
    if (index == 0) {
      msg->linear_acceleration.x = value;
    } else if (index == 1) {
      msg->linear_acceleration.y = value;
    } else {
      msg->linear_acceleration.z = value;
    }
  }

  ros::NodeHandle nh_;
  ros::NodeHandle pnh_;
  ros::Publisher imu_pub_;
  ros::Publisher raw_imu_pub_;
  serial::Serial serial_;

  std::string serial_port_ = "/dev/imu";
  int serial_baud_ = 115200;
  int serial_timeout_ms_ = 50;
  int queue_size_ = 100;
  int slave_address_param_ = 1;
  uint8_t slave_address_ = 1;
  double poll_rate_hz_ = 100.0;
  std::string imu_topic_ = "/imu";
  std::string raw_imu_topic_ = "/imu_raw";
  std::string frame_id_ = "gyro_link";
  bool publish_raw_imu_ = false;
  bool debug_ = false;

  bool use_midpoint_timestamp_ = true;
  bool enforce_monotonic_timestamps_ = true;
  double timestamp_offset_sec_ = 0.0;
  double min_timestamp_step_sec_ = 1e-6;
  double max_future_timestamp_sec_ = 0.02;
  ros::Time last_stamp_;

  bool enable_gyro_bias_compensation_ = true;
  bool estimate_gyro_bias_on_startup_ = true;
  double gyro_bias_estimation_duration_sec_ = 5.0;
  double gyro_bias_stationary_threshold_rad_s_ = 0.02;
  int gyro_bias_min_samples_ = 200;
  bool enable_accel_bias_compensation_ = false;
  bool estimate_accel_bias_on_startup_ = false;
  double accel_bias_estimation_duration_sec_ = 5.0;
  double accel_bias_stationary_tolerance_g_ = 0.15;
  int accel_bias_min_samples_ = 200;
  bool publish_during_calibration_ = false;

  bool publish_orientation_ = false;
  int angle_poll_divider_ = 5;
  std::string angle_source_ = "dynamic";

  std::array<double, 3> linear_acceleration_axis_signs_ = {{1.0, 1.0, 1.0}};
  std::array<double, 3> angular_velocity_axis_signs_ = {{1.0, 1.0, 1.0}};
  std::array<double, 3> orientation_rpy_signs_ = {{1.0, 1.0, 1.0}};
  std::array<double, 3> configured_gyro_bias_rad_s_ = {{0.0, 0.0, 0.0}};
  std::array<double, 3> applied_gyro_bias_rad_s_ = {{0.0, 0.0, 0.0}};
  std::array<double, 3> gyro_bias_sum_rad_s_ = {{0.0, 0.0, 0.0}};
  int gyro_bias_sample_count_ = 0;
  bool gyro_bias_estimation_started_ = false;
  bool gyro_bias_estimation_completed_ = false;
  ros::Time gyro_bias_estimation_start_stamp_;

  std::array<double, 3> configured_accel_bias_g_ = {{0.0, 0.0, 0.0}};
  std::array<double, 3> applied_accel_bias_g_ = {{0.0, 0.0, 0.0}};
  std::array<double, 3> accel_bias_target_g_ = {{0.0, 0.0, 1.0}};
  std::array<double, 3> accel_bias_sum_g_ = {{0.0, 0.0, 0.0}};
  int accel_bias_sample_count_ = 0;
  bool accel_bias_estimation_started_ = false;
  bool accel_bias_estimation_completed_ = false;
  ros::Time accel_bias_estimation_start_stamp_;

  std::array<double, 9> orientation_covariance_;
  std::array<double, 9> angular_velocity_covariance_;
  std::array<double, 9> linear_acceleration_covariance_;
};

int main(int argc, char** argv) {
  ros::init(argc, argv, "modbus_imu_serial_node");
  try {
    ModbusImuSerialNode node;
    node.Spin();
  } catch (const std::exception& e) {
    ROS_FATAL_STREAM("modbus_imu_serial_node exited: " << e.what());
    return 1;
  }
  return 0;
}

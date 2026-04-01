#include <chrono>
#include <condition_variable>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>

#include <libobsensor/ObSensor.hpp>

static const char *sensorTypeName(OBSensorType type) {
  switch(type) {
    case OB_SENSOR_DEPTH:
      return "DEPTH";
    case OB_SENSOR_COLOR:
      return "COLOR";
    case OB_SENSOR_IR:
      return "IR";
    case OB_SENSOR_IR_LEFT:
      return "IR_LEFT";
    case OB_SENSOR_IR_RIGHT:
      return "IR_RIGHT";
    case OB_SENSOR_ACCEL:
      return "ACCEL";
    case OB_SENSOR_GYRO:
      return "GYRO";
    case OB_SENSOR_RAW_PHASE:
      return "RAW_PHASE";
    case OB_SENSOR_UNKNOWN:
    default:
      return "UNKNOWN";
  }
}

int main(int argc, char **argv) {
  const char *config_path = argc > 1 ? argv[1] : "";
  const char *target_uid = argc > 2 ? argv[2] : "";
  const bool start_depth = argc > 3 ? std::string(argv[3]) == "start_depth" : false;
  try {
    ob::Context::setLoggerToConsole(OB_LOG_SEVERITY_DEBUG);
    ob::Context ctx(config_path);
    auto list = ctx.queryDeviceList();
    std::cout << "device_count=" << list->deviceCount() << std::endl;
    if(list->deviceCount() == 0) {
      return 0;
    }

    std::shared_ptr<ob::Device> dev;
    if(target_uid && target_uid[0] != '\0') {
      std::cout << "select_by_uid=" << target_uid << std::endl;
      dev = list->getDeviceByUid(target_uid);
    } else {
      dev = list->getDevice(0);
    }
    std::cout << "got_device" << std::endl;

    try {
      auto info = dev->getDeviceInfo();
      std::cout << "name=" << (info && info->name() ? info->name() : "") << std::endl;
      std::cout << "pid=0x" << std::hex << info->pid() << std::dec << std::endl;
      std::cout << "vid=0x" << std::hex << info->vid() << std::dec << std::endl;
      std::cout << "uid=" << (info && info->uid() ? info->uid() : "") << std::endl;
      std::cout << "serial=" << (info && info->serialNumber() ? info->serialNumber() : "") << std::endl;
      std::cout << "conn=" << (info && info->connectionType() ? info->connectionType() : "") << std::endl;
      std::cout << "fw=" << (info && info->firmwareVersion() ? info->firmwareVersion() : "") << std::endl;
      std::cout << "asic=" << (info && info->asicName() ? info->asicName() : "") << std::endl;
    } catch(const std::exception &e) {
      std::cout << "getDeviceInfo_exception=" << e.what() << std::endl;
    }

    try {
      auto sensors = dev->getSensorList();
      if(!sensors) {
        std::cout << "sensor_list=null" << std::endl;
        return 0;
      }
      std::cout << "sensor_count=" << sensors->count() << std::endl;
      for(uint32_t i = 0; i < sensors->count(); ++i) {
        auto type = sensors->type(i);
        std::cout << "sensor[" << i << "]=" << sensorTypeName(type)
                  << "(" << static_cast<int>(type) << ")" << std::endl;
      }

      if(start_depth) {
        std::shared_ptr<ob::Sensor> depth_sensor;
        for(uint32_t i = 0; i < sensors->count(); ++i) {
          if(sensors->type(i) == OB_SENSOR_DEPTH) {
            depth_sensor = sensors->getSensor(i);
            break;
          }
        }
        if(!depth_sensor) {
          std::cout << "depth_sensor=missing" << std::endl;
          return 0;
        }
        auto profiles = depth_sensor->getStreamProfileList();
        std::shared_ptr<ob::VideoStreamProfile> profile;
        try {
          profile = profiles->getVideoStreamProfile(1280, 800, OB_FORMAT_Y16, 30);
        } catch(const std::exception &e) {
          std::cout << "depth_profile_exact_exception=" << e.what() << std::endl;
        }
        if(!profile) {
          profile = profiles->getProfile(0)->as<ob::VideoStreamProfile>();
        }
        std::cout << "depth_profile="
                  << profile->width() << "x" << profile->height()
                  << "@" << profile->fps()
                  << " format=" << static_cast<int>(profile->format()) << std::endl;

        std::mutex mu;
        std::condition_variable cv;
        bool got_frame = false;
        std::shared_ptr<ob::DepthFrame> depth_frame;
        depth_sensor->start(profile, [&](std::shared_ptr<ob::Frame> frame) {
          try {
            auto casted = frame->as<ob::DepthFrame>();
            {
              std::lock_guard<std::mutex> lk(mu);
              depth_frame = casted;
              got_frame = true;
            }
            cv.notify_one();
          } catch(const std::exception &e) {
            std::cout << "depth_callback_exception=" << e.what() << std::endl;
          }
        });
        {
          std::unique_lock<std::mutex> lk(mu);
          cv.wait_for(lk, std::chrono::seconds(3), [&] { return got_frame; });
        }
        if(depth_frame) {
          std::cout << "depth_frame="
                    << depth_frame->width() << "x" << depth_frame->height()
                    << " bytes=" << depth_frame->dataSize()
                    << " scale=" << depth_frame->getValueScale() << std::endl;
        } else {
          std::cout << "depth_frame=timeout" << std::endl;
        }
        depth_sensor->stop();
      }
    } catch(const std::exception &e) {
      std::cout << "getSensorList_exception=" << e.what() << std::endl;
    }
  } catch(const std::exception &e) {
    std::cerr << "fatal_exception=" << e.what() << std::endl;
    return 2;
  }

  return 0;
}

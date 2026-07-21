/*******************************************************************************
 * Copyright (c) 2023 Orbbec 3D Technology, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
#include <ros/ros.h>
#include <orbbec_camera/logging.h>
#include <orbbec_camera/types.h>
#include <orbbec_camera/utils.h>
#include <iostream>
#include <regex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace {

constexpr char kMachineRecordPrefix[] = "DORAEMON_ORBBEC_DEVICE_V1|";
constexpr char kMachineErrorPrefix[] = "DORAEMON_ORBBEC_ENUM_ERROR_V1|";
constexpr int kSdkErrorExitCode = 2;
constexpr int kStandardErrorExitCode = 3;
constexpr int kUnknownErrorExitCode = 4;
constexpr int kInvalidRecordExitCode = 5;

bool isSafeSerial(const std::string &serial) {
  static const std::regex serial_regex("^[A-Za-z0-9_./:@,+-]{1,128}$",
                                       std::regex_constants::ECMAScript);
  return std::regex_match(serial, serial_regex);
}

bool isSafeUsbTopology(const std::string &topology) {
  static const std::regex topology_regex("^[0-9]+-[0-9]+([.][0-9]+)*$",
                                         std::regex_constants::ECMAScript);
  return std::regex_match(topology, topology_regex) && topology.size() <= 128;
}

}  // namespace

std::string parseUsbPort(const std::string &line) {
  std::string port_id;
  std::regex self_regex("(?:[^ ]+/usb[0-9]+[0-9./-]*/){0,1}([0-9.-]+)(:){0,1}[^ ]*",
                        std::regex_constants::ECMAScript);
  std::smatch base_match;
  bool found = std::regex_match(line, base_match, self_regex);
  if (found) {
    port_id = base_match[1].str();
    if (base_match[2].str().empty())  // This is libuvc string. Remove counter is exists.
    {
      std::regex end_regex = std::regex(".+(-[0-9]+$)", std::regex_constants::ECMAScript);
      bool found_end = std::regex_match(port_id, base_match, end_regex);
      if (found_end) {
        port_id = port_id.substr(0, port_id.size() - base_match[1].str().size());
      }
    }
  }
  return port_id;
}
int main() {
  try {
    // Logger configuration must precede Context construction: Context creation
    // itself emits logs and otherwise creates ./Log in its working directory.
    orbbec_camera::disableOrbbecSdkFileLogging();
    ob::Context::setLoggerToConsole(OBLogSeverity::OB_LOG_SEVERITY_OFF);
    auto context = std::make_shared<ob::Context>();
    auto list = context->queryDeviceList();
    std::vector<std::pair<std::string, std::string>> device_records;
    device_records.reserve(list->deviceCount());
    for (size_t i = 0; i < list->deviceCount(); i++) {
      auto device = list->getDevice(i);
      auto device_info = device->getDeviceInfo();
      std::string serial = device_info->serialNumber();
      std::string uid = device_info->uid();
      auto port_id = parseUsbPort(uid);
      if (!isSafeSerial(serial) || !isSafeUsbTopology(port_id)) {
        std::cerr << kMachineErrorPrefix << "invalid-record" << std::endl;
        return kInvalidRecordExitCode;
      }
      device_records.emplace_back(std::move(serial), std::move(port_id));
    }
    // Emit only after the complete SDK snapshot has been collected. An SDK
    // exception can therefore never leave a partial, apparently valid stdout.
    for (const auto &record : device_records) {
      std::cout << kMachineRecordPrefix << record.first << '|' << record.second << '\n';
    }
  } catch (ob::Error &) {
    std::cerr << kMachineErrorPrefix << "sdk" << std::endl;
    return kSdkErrorExitCode;
  } catch (const std::exception &) {
    std::cerr << kMachineErrorPrefix << "standard" << std::endl;
    return kStandardErrorExitCode;
  } catch (...) {
    std::cerr << kMachineErrorPrefix << "unknown" << std::endl;
    return kUnknownErrorExitCode;
  }
  return 0;
}

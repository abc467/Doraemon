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

#pragma once

#include "libobsensor/ObSensor.hpp"

namespace orbbec_camera {

// A commercial release is immutable. Keep every Orbbec SDK and crash log outside
// the workspace so a Context created while the release is the working directory
// cannot fall back to the SDK default of ./Log.
constexpr char kOrbbecLogDirectory[] = "/var/log/doraemon/orbbec";

inline void disableOrbbecSdkFileLogging() {
  // Normal diagnostics are captured by ROS/systemd-journald. Three camera
  // processes must not rotate and contend for one SDK file set.
  ob::Context::setLoggerToFile(OBLogSeverity::OB_LOG_SEVERITY_OFF,
                               kOrbbecLogDirectory);
}

}  // namespace orbbec_camera

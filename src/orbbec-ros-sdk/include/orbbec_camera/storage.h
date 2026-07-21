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

#include <boost/filesystem.hpp>
#include <boost/system/error_code.hpp>

#include <string>

namespace orbbec_camera {

// Camera captures are mutable runtime data. They must never be written below
// the immutable release working directory.
constexpr char kOrbbecCaptureDirectory[] = "/var/lib/doraemon/orbbec-captures";

inline bool isSafeOrbbecCaptureComponent(const std::string& component) {
  return !component.empty() && component != "." && component != ".." &&
         component.find('/') == std::string::npos &&
         component.find('\\') == std::string::npos &&
         component.find('\0') == std::string::npos;
}

inline bool makeOrbbecCapturePath(const std::string& category, const std::string& filename,
                                  boost::filesystem::path& output_path,
                                  std::string& error_message) {
  output_path.clear();
  error_message.clear();
  if (!isSafeOrbbecCaptureComponent(category) ||
      !isSafeOrbbecCaptureComponent(filename)) {
    error_message = "unsafe capture path component";
    return false;
  }

  const boost::filesystem::path root(kOrbbecCaptureDirectory);
  boost::system::error_code error;
  const auto root_status = boost::filesystem::symlink_status(root, error);
  if (error) {
    error_message = "cannot inspect capture root " + root.string() + ": " + error.message();
    return false;
  }
  if (!boost::filesystem::exists(root_status) ||
      !boost::filesystem::is_directory(root_status) ||
      boost::filesystem::is_symlink(root_status)) {
    error_message = "capture root is missing, not a directory, or a symlink: " + root.string();
    return false;
  }

  const boost::filesystem::path directory = root / category;
  boost::filesystem::create_directory(directory, error);
  if (error) {
    error_message = "cannot create capture directory " + directory.string() + ": " +
                    error.message();
    return false;
  }

  const auto directory_status = boost::filesystem::symlink_status(directory, error);
  if (error) {
    error_message = "cannot inspect capture directory " + directory.string() + ": " +
                    error.message();
    return false;
  }
  if (!boost::filesystem::is_directory(directory_status) ||
      boost::filesystem::is_symlink(directory_status)) {
    error_message = "capture directory is not a real directory: " + directory.string();
    return false;
  }

  const auto canonical_root = boost::filesystem::canonical(root, error);
  if (error) {
    error_message = "cannot resolve capture root " + root.string() + ": " + error.message();
    return false;
  }
  const auto canonical_directory = boost::filesystem::canonical(directory, error);
  if (error) {
    error_message = "cannot resolve capture directory " + directory.string() + ": " +
                    error.message();
    return false;
  }
  if (canonical_directory.parent_path() != canonical_root) {
    error_message = "capture directory escaped the configured root: " +
                    canonical_directory.string();
    return false;
  }

  output_path = canonical_directory / filename;
  return true;
}

}  // namespace orbbec_camera

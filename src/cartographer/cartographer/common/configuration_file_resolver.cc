/*
 * Copyright 2016 The Cartographer Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "cartographer/common/configuration_file_resolver.h"

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <streambuf>
#include <sys/stat.h>

#include "cartographer/common/config.h"
#include "glog/logging.h"

namespace cartographer {
namespace common {
namespace {

void CheckIsSafeBasename(const std::string& basename) {
  if (basename.empty() || basename == "." || basename == ".." ||
      basename.find('/') != std::string::npos ||
      basename.find('\\') != std::string::npos ||
      basename.find('\0') != std::string::npos) {
    LOG(FATAL) << "Unsafe configuration filename: expected a non-empty "
                  "single filename without path separators.";
  }
}

std::string RealPathOrDie(const std::string& path,
                          const std::string& description) {
  char* const resolved_path = realpath(path.c_str(), nullptr);
  if (resolved_path == nullptr) {
    LOG(FATAL) << "Failed to resolve " << description << " '" << path
               << "': " << std::strerror(errno);
  }
  const std::string result(resolved_path);
  std::free(resolved_path);
  return result;
}

bool IsPathContainedInDirectory(const std::string& path,
                                const std::string& directory) {
  if (directory == "/") {
    return path.size() > 1 && path.front() == '/';
  }
  return path.size() > directory.size() + 1 &&
         path.compare(0, directory.size(), directory) == 0 &&
         path[directory.size()] == '/';
}

bool PathDoesNotExist(const int error) {
  return error == ENOENT || error == ENOTDIR;
}

}  // namespace

ConfigurationFileResolver::ConfigurationFileResolver(
    const std::vector<std::string>& configuration_files_directories)
    : configuration_files_directories_(configuration_files_directories) {
  configuration_files_directories_.push_back(kConfigurationFilesDirectory);
}

std::string ConfigurationFileResolver::GetFullPathOrDie(
    const std::string& basename) {
  CheckIsSafeBasename(basename);
  for (const auto& path : configuration_files_directories_) {
    if (path.empty() || path.find('\0') != std::string::npos) {
      LOG(FATAL) << "Configuration search directory must be a non-empty "
                    "filesystem path.";
    }

    struct stat path_status;
    if (lstat(path.c_str(), &path_status) != 0) {
      const int error = errno;
      if (PathDoesNotExist(error)) {
        continue;
      }
      LOG(FATAL) << "Failed to inspect configuration search directory '"
                 << path << "': " << std::strerror(error);
    }

    const std::string canonical_directory =
        RealPathOrDie(path, "configuration search directory");
    if (stat(canonical_directory.c_str(), &path_status) != 0) {
      LOG(FATAL) << "Failed to inspect resolved configuration search "
                    "directory '"
                 << canonical_directory << "': " << std::strerror(errno);
    }
    if (!S_ISDIR(path_status.st_mode)) {
      LOG(FATAL) << "Configuration search path '" << canonical_directory
                 << "' is not a directory.";
    }

    const std::string candidate =
        canonical_directory == "/"
            ? canonical_directory + basename
            : canonical_directory + "/" + basename;
    struct stat candidate_status;
    if (lstat(candidate.c_str(), &candidate_status) != 0) {
      const int error = errno;
      if (PathDoesNotExist(error)) {
        continue;
      }
      LOG(FATAL) << "Failed to inspect configuration file candidate '"
                 << candidate << "': " << std::strerror(error);
    }

    const std::string canonical_candidate =
        RealPathOrDie(candidate, "configuration file candidate");
    if (!IsPathContainedInDirectory(canonical_candidate,
                                    canonical_directory)) {
      LOG(FATAL) << "Resolved configuration file '" << canonical_candidate
                 << "' escapes configuration directory '"
                 << canonical_directory << "'.";
    }
    if (stat(canonical_candidate.c_str(), &candidate_status) != 0) {
      LOG(FATAL) << "Failed to inspect resolved configuration file '"
                 << canonical_candidate << "': " << std::strerror(errno);
    }
    if (!S_ISREG(candidate_status.st_mode)) {
      LOG(FATAL) << "Resolved configuration path '" << canonical_candidate
                 << "' is not a regular file.";
    }

    LOG(INFO) << "Found '" << canonical_candidate << "' for '" << basename
              << "'.";
    return canonical_candidate;
  }
  LOG(FATAL) << "File '" << basename << "' was not found.";
}

std::string ConfigurationFileResolver::GetFileContentOrDie(
    const std::string& basename) {
  const std::string filename = GetFullPathOrDie(basename);
  std::ifstream stream(filename.c_str());
  CHECK(stream.good()) << "Failed to open resolved configuration file '"
                       << filename << "'.";
  const std::string content((std::istreambuf_iterator<char>(stream)),
                            std::istreambuf_iterator<char>());
  CHECK(!stream.bad()) << "Failed while reading resolved configuration file '"
                       << filename << "'.";
  return content;
}

}  // namespace common
}  // namespace cartographer

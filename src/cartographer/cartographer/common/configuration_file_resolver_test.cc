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

#include <cstdlib>
#include <fstream>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

#include "glog/logging.h"
#include "gtest/gtest.h"

namespace cartographer {
namespace common {
namespace {

class TemporaryTree {
 public:
  TemporaryTree() {
    char path_template[] = "/tmp/cartographer_config_resolver_test.XXXXXX";
    char* const result = mkdtemp(path_template);
    CHECK(result != nullptr);
    root_ = result;
    paths_.push_back(root_);
  }

  ~TemporaryTree() {
    for (auto iterator = paths_.rbegin(); iterator != paths_.rend();
         ++iterator) {
      if (unlink(iterator->c_str()) != 0) {
        rmdir(iterator->c_str());
      }
    }
  }

  const std::string& root() const { return root_; }

  std::string CreateDirectory(const std::string& basename) {
    const std::string path = root_ + "/" + basename;
    CHECK_EQ(mkdir(path.c_str(), 0700), 0);
    paths_.push_back(path);
    return path;
  }

  std::string WriteFile(const std::string& directory,
                        const std::string& basename,
                        const std::string& content) {
    const std::string path = directory + "/" + basename;
    std::ofstream stream(path);
    CHECK(stream.good());
    stream << content;
    stream.close();
    CHECK(stream.good());
    paths_.push_back(path);
    return path;
  }

  std::string CreateSymlink(const std::string& directory,
                            const std::string& basename,
                            const std::string& target) {
    const std::string path = directory + "/" + basename;
    CHECK_EQ(symlink(target.c_str(), path.c_str()), 0);
    paths_.push_back(path);
    return path;
  }

 private:
  std::string root_;
  std::vector<std::string> paths_;
};

TEST(ConfigurationFileResolverTest, ResolvesSimpleFilename) {
  TemporaryTree tree;
  const std::string search_directory = tree.CreateDirectory("config");
  const std::string expected =
      tree.WriteFile(search_directory, "robot.lua", "return {}\n");
  ConfigurationFileResolver resolver({search_directory});

  EXPECT_EQ(expected, resolver.GetFullPathOrDie("robot.lua"));
  EXPECT_EQ("return {}\n", resolver.GetFileContentOrDie("robot.lua"));
}

TEST(ConfigurationFileResolverTest, RejectsUnsafeBasenames) {
  TemporaryTree tree;
  const std::string search_directory = tree.CreateDirectory("config");
  ConfigurationFileResolver resolver({search_directory});

  EXPECT_DEATH(resolver.GetFullPathOrDie(""),
               "Unsafe configuration filename");
  EXPECT_DEATH(resolver.GetFullPathOrDie("."),
               "Unsafe configuration filename");
  EXPECT_DEATH(resolver.GetFullPathOrDie(".."),
               "Unsafe configuration filename");
  EXPECT_DEATH(resolver.GetFullPathOrDie("/tmp/robot.lua"),
               "Unsafe configuration filename");
  EXPECT_DEATH(resolver.GetFullPathOrDie("nested/robot.lua"),
               "Unsafe configuration filename");
  EXPECT_DEATH(resolver.GetFullPathOrDie("nested\\robot.lua"),
               "Unsafe configuration filename");

  std::string basename_with_nul = "robot.lua";
  basename_with_nul.push_back('\0');
  basename_with_nul.append("../outside.lua");
  EXPECT_DEATH(resolver.GetFullPathOrDie(basename_with_nul),
               "Unsafe configuration filename");
}

TEST(ConfigurationFileResolverTest, RejectsNonRegularFile) {
  TemporaryTree tree;
  const std::string search_directory = tree.CreateDirectory("config");
  tree.CreateDirectory("config/robot.lua");
  ConfigurationFileResolver resolver({search_directory});

  EXPECT_DEATH(resolver.GetFullPathOrDie("robot.lua"),
               "not a regular file");
}

TEST(ConfigurationFileResolverTest, AllowsContainedSymlink) {
  TemporaryTree tree;
  const std::string search_directory = tree.CreateDirectory("config");
  const std::string expected =
      tree.WriteFile(search_directory, "actual.lua", "return {}\n");
  tree.CreateSymlink(search_directory, "robot.lua", "actual.lua");
  ConfigurationFileResolver resolver({search_directory});

  EXPECT_EQ(expected, resolver.GetFullPathOrDie("robot.lua"));
}

TEST(ConfigurationFileResolverTest, RejectsSymlinkEscapeWithoutFallback) {
  TemporaryTree tree;
  const std::string unsafe_directory = tree.CreateDirectory("unsafe");
  const std::string safe_directory = tree.CreateDirectory("safe");
  const std::string outside =
      tree.WriteFile(tree.root(), "outside.lua", "return {}\n");
  tree.CreateSymlink(unsafe_directory, "robot.lua", outside);
  tree.WriteFile(safe_directory, "robot.lua", "return {}\n");
  ConfigurationFileResolver resolver({unsafe_directory, safe_directory});

  EXPECT_DEATH(resolver.GetFullPathOrDie("robot.lua"),
               "escapes configuration directory");
}

}  // namespace
}  // namespace common
}  // namespace cartographer

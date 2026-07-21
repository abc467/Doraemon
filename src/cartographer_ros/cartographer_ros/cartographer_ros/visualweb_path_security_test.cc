#include "cartographer_ros/visualweb.h"

#include <fstream>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

#include "glog/logging.h"
#include "gtest/gtest.h"

namespace cartographer_ros
{
namespace
{

class TemporaryTree
{
public:
    TemporaryTree()
    {
        char path_template[] = "/tmp/cartographer_visualweb_path_test.XXXXXX";
        char *const result = mkdtemp(path_template);
        CHECK(result != nullptr);
        root_ = result;
        paths_.push_back(root_);
    }

    ~TemporaryTree()
    {
        for (auto iterator = paths_.rbegin(); iterator != paths_.rend();
             ++iterator)
        {
            if (unlink(iterator->c_str()) != 0)
            {
                rmdir(iterator->c_str());
            }
        }
    }

    const std::string &root() const
    {
        return root_;
    }

    std::string CreateDirectory(const std::string &relative_path)
    {
        const std::string path = root_ + "/" + relative_path;
        CHECK_EQ(mkdir(path.c_str(), 0700), 0);
        paths_.push_back(path);
        return path;
    }

    std::string WriteFile(const std::string &directory,
                          const std::string &basename,
                          const std::string &content = "test\n")
    {
        const std::string path = directory + "/" + basename;
        std::ofstream stream(path);
        CHECK(stream.good());
        stream << content;
        stream.close();
        CHECK(stream.good());
        paths_.push_back(path);
        return path;
    }

    std::string CreateSymlink(const std::string &directory,
                              const std::string &basename,
                              const std::string &target)
    {
        const std::string path = directory + "/" + basename;
        CHECK_EQ(symlink(target.c_str(), path.c_str()), 0);
        paths_.push_back(path);
        return path;
    }

    std::string CreateFifo(const std::string &directory,
                           const std::string &basename)
    {
        const std::string path = directory + "/" + basename;
        CHECK_EQ(mkfifo(path.c_str(), 0600), 0);
        paths_.push_back(path);
        return path;
    }

private:
    std::string root_;
    std::vector<std::string> paths_;
};

TEST(VisualwebPathSecurityTest, ConfigEntryUsesExactAllowlist)
{
    EXPECT_TRUE(visualweb::is_allowed_config_entry("slam"));
    EXPECT_TRUE(visualweb::is_allowed_config_entry("pure_location"));
    EXPECT_TRUE(visualweb::is_allowed_config_entry("pure_location_odom"));
    EXPECT_FALSE(visualweb::is_allowed_config_entry(""));
    EXPECT_FALSE(visualweb::is_allowed_config_entry("../slam"));
    EXPECT_FALSE(visualweb::is_allowed_config_entry("slam/config.lua"));
    EXPECT_FALSE(visualweb::is_allowed_config_entry("SLAM"));
}

TEST(VisualwebPathSecurityTest, LoadAcceptsOnlyContainedRegularPbstream)
{
    TemporaryTree tree;
    const std::string maps = tree.CreateDirectory("maps");
    const std::string outside = tree.CreateDirectory("outside");
    const std::string inside_file = tree.WriteFile(maps, "inside.pbstream");
    const std::string outside_file = tree.WriteFile(outside, "outside.pbstream");
    tree.WriteFile(maps, "wrong.txt");
    tree.CreateDirectory("maps/directory.pbstream");
    tree.CreateSymlink(maps, "linked.pbstream", inside_file);
    tree.CreateSymlink(maps, "outside_link.pbstream", outside_file);
    tree.CreateSymlink(maps, "dangling.pbstream",
                       maps + "/missing.pbstream");
    tree.CreateSymlink(maps, "escape", outside);
    tree.CreateFifo(maps, "special.pbstream");

    boost::filesystem::path resolved;
    std::string error;
    EXPECT_TRUE(visualweb::resolve_existing_pbstream_path_for_root(
        "inside.pbstream", maps, &resolved, &error));
    EXPECT_EQ(boost::filesystem::canonical(inside_file), resolved);
    EXPECT_TRUE(visualweb::resolve_existing_pbstream_path_for_root(
        inside_file, maps, &resolved, &error));

    EXPECT_FALSE(visualweb::resolve_existing_pbstream_path_for_root(
        "wrong.txt", maps, &resolved, &error));
    EXPECT_FALSE(visualweb::resolve_existing_pbstream_path_for_root(
        "directory.pbstream", maps, &resolved, &error));
    EXPECT_TRUE(visualweb::resolve_existing_pbstream_path_for_root(
        "linked.pbstream", maps, &resolved, &error));
    EXPECT_EQ(boost::filesystem::canonical(inside_file), resolved);
    EXPECT_FALSE(visualweb::resolve_existing_pbstream_path_for_root(
        "outside_link.pbstream", maps, &resolved, &error));
    EXPECT_FALSE(visualweb::resolve_existing_pbstream_path_for_root(
        "dangling.pbstream", maps, &resolved, &error));
    EXPECT_FALSE(visualweb::resolve_existing_pbstream_path_for_root(
        "escape/outside.pbstream", maps, &resolved, &error));
    EXPECT_FALSE(visualweb::resolve_existing_pbstream_path_for_root(
        outside_file, maps, &resolved, &error));
    EXPECT_FALSE(visualweb::resolve_existing_pbstream_path_for_root(
        "special.pbstream", maps, &resolved, &error));

    std::string embedded_nul = "inside.pbstream";
    embedded_nul.push_back('\0');
    embedded_nul.append("../outside.pbstream");
    EXPECT_FALSE(visualweb::resolve_existing_pbstream_path_for_root(
        embedded_nul, maps, &resolved, &error));
}

TEST(VisualwebPathSecurityTest, SaveRequiresContainedCanonicalParentAndSafeTarget)
{
    TemporaryTree tree;
    const std::string maps = tree.CreateDirectory("maps");
    const std::string subdirectory = tree.CreateDirectory("maps/sub");
    const std::string outside = tree.CreateDirectory("outside");
    const std::string existing = tree.WriteFile(maps, "existing.pbstream");
    const std::string outside_file = tree.WriteFile(outside, "outside.pbstream");
    tree.CreateSymlink(maps, "linked.pbstream", existing);
    tree.CreateSymlink(maps, "dangling.pbstream",
                       maps + "/missing.pbstream");
    tree.CreateSymlink(maps, "escape", outside);
    tree.CreateFifo(maps, "special.pbstream");
    tree.CreateDirectory("maps/directory.pbstream");

    boost::filesystem::path resolved;
    std::string error;
    EXPECT_TRUE(visualweb::resolve_pbstream_save_path_for_root(
        "sub/new.pbstream", maps, &resolved, &error));
    EXPECT_EQ(boost::filesystem::canonical(subdirectory) / "new.pbstream",
              resolved);
    EXPECT_TRUE(visualweb::resolve_pbstream_save_path_for_root(
        existing, maps, &resolved, &error));

    EXPECT_FALSE(visualweb::resolve_pbstream_save_path_for_root(
        "sub/new.txt", maps, &resolved, &error));
    EXPECT_FALSE(visualweb::resolve_pbstream_save_path_for_root(
        outside + "/new.pbstream", maps, &resolved, &error));
    EXPECT_FALSE(visualweb::resolve_pbstream_save_path_for_root(
        outside_file, maps, &resolved, &error));
    EXPECT_FALSE(visualweb::resolve_pbstream_save_path_for_root(
        "missing/new.pbstream", maps, &resolved, &error));
    EXPECT_FALSE(visualweb::resolve_pbstream_save_path_for_root(
        "escape/new.pbstream", maps, &resolved, &error));
    EXPECT_FALSE(visualweb::resolve_pbstream_save_path_for_root(
        "linked.pbstream", maps, &resolved, &error));
    EXPECT_FALSE(visualweb::resolve_pbstream_save_path_for_root(
        "dangling.pbstream", maps, &resolved, &error));
    EXPECT_FALSE(visualweb::resolve_pbstream_save_path_for_root(
        "special.pbstream", maps, &resolved, &error));
    EXPECT_FALSE(visualweb::resolve_pbstream_save_path_for_root(
        "directory.pbstream", maps, &resolved, &error));
}

TEST(VisualwebPathSecurityTest, DirlistRejectsTraversalAndSymlinkEscape)
{
    TemporaryTree tree;
    const std::string maps = tree.CreateDirectory("maps");
    const std::string subdirectory = tree.CreateDirectory("maps/sub");
    const std::string outside = tree.CreateDirectory("outside");
    tree.WriteFile(maps, "not_a_directory.pbstream");
    tree.CreateSymlink(maps, "inside_link", subdirectory);
    tree.CreateSymlink(maps, "escape", outside);

    boost::filesystem::path resolved;
    std::string error;
    EXPECT_TRUE(visualweb::resolve_map_directory_path_for_root(
        "map", maps, &resolved, &error));
    EXPECT_EQ(boost::filesystem::canonical(maps), resolved);
    EXPECT_TRUE(visualweb::resolve_map_directory_path_for_root(
        "/map", maps, &resolved, &error));
    EXPECT_TRUE(visualweb::resolve_map_directory_path_for_root(
        "sub", maps, &resolved, &error));
    EXPECT_EQ(boost::filesystem::canonical(subdirectory), resolved);
    EXPECT_TRUE(visualweb::resolve_map_directory_path_for_root(
        subdirectory, maps, &resolved, &error));
    EXPECT_TRUE(visualweb::resolve_map_directory_path_for_root(
        "inside_link", maps, &resolved, &error));

    EXPECT_FALSE(visualweb::resolve_map_directory_path_for_root(
        "../outside", maps, &resolved, &error));
    EXPECT_FALSE(visualweb::resolve_map_directory_path_for_root(
        "sub/../sub", maps, &resolved, &error));
    EXPECT_FALSE(visualweb::resolve_map_directory_path_for_root(
        outside, maps, &resolved, &error));
    EXPECT_FALSE(visualweb::resolve_map_directory_path_for_root(
        "escape", maps, &resolved, &error));
    EXPECT_FALSE(visualweb::resolve_map_directory_path_for_root(
        "not_a_directory.pbstream", maps, &resolved, &error));
    EXPECT_FALSE(visualweb::resolve_map_directory_path_for_root(
        "", maps, &resolved, &error));
}

TEST(VisualwebPathSecurityTest, RejectsSymlinkMapRoot)
{
    TemporaryTree tree;
    const std::string maps = tree.CreateDirectory("maps");
    const std::string linked_root =
        tree.CreateSymlink(tree.root(), "linked_maps", maps);

    boost::filesystem::path resolved;
    std::string error;
    EXPECT_FALSE(visualweb::resolve_map_directory_path_for_root(
        "map", linked_root, &resolved, &error));
    EXPECT_FALSE(visualweb::resolve_existing_pbstream_path_for_root(
        "anything.pbstream", linked_root, &resolved, &error));
    EXPECT_FALSE(visualweb::resolve_pbstream_save_path_for_root(
        "anything.pbstream", linked_root, &resolved, &error));
}

} // namespace
} // namespace cartographer_ros

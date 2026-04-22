#ifndef VISUAL_WEB_HPP
#define VISUAL_WEB_HPP

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <memory>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <boost/filesystem.hpp>

#include "robot_runtime_flags_msgs/SetParam.h"

#include "ros/package.h"
#include "ros/ros.h"

#include "cartographer/common/configuration_file_resolver.h"
#include "cartographer/common/sml_config.h"
#include "cartographer/mapping/flirt.h"
#include "cartographer/mapping/map_builder.h"
#include "cartographer_ros/json.hpp"
#include "cartographer_ros/node.h"
#include "cartographer_ros/node_options.h"
#include "cartographer_ros/trajectory_options.h"
#include "cartographer_ros_msgs/VisualCommand.h"
#include "tf2_ros/transform_listener.h"

namespace cartographer_ros
{

class visualweb
{
public:
    struct CommandResult
    {
        int code = 0;
        std::string msg;
        nlohmann::json data = nlohmann::json::object();
    };

    std::unique_ptr<Node> node;
    std::unique_ptr<::ros::NodeHandle> root_node_handle;
    NodeOptions node_options;
    TrajectoryOptions trajectory_options;

    std::unique_ptr<tf2_ros::Buffer> tf_buffer;
    std::unique_ptr<tf2_ros::TransformListener> tf;
    std::unique_ptr<::ros::AsyncSpinner> async_spinner;

    ::ros::ServiceServer srv_visual_command;
    ::ros::ServiceClient client_set_param;
    std::string slam_root;
    std::string config_root;
    std::string map_root;

    visualweb() = default;
    ~visualweb() = default;

    int ros_spinner_thread_count() const
    {
        if (const char *env = std::getenv("CARTO_ROS_SPINNER_THREADS"))
        {
            try
            {
                const int parsed = std::stoi(env);
                if (parsed > 0)
                {
                    return parsed;
                }
            }
            catch (const std::exception &)
            {
            }
        }

        const unsigned int hw = std::thread::hardware_concurrency();
        const unsigned int fallback = hw == 0 ? 4u : hw;
        return static_cast<int>(std::max(4u, std::min(8u, fallback)));
    }

    static bool has_runtime_layout(const boost::filesystem::path &root)
    {
        if (root.empty() || !boost::filesystem::is_directory(root))
        {
            return false;
        }
        return boost::filesystem::is_directory(root / "src") ||
               boost::filesystem::is_directory(root / "scripts") ||
               boost::filesystem::is_directory(root / "devel") ||
               boost::filesystem::is_directory(root / "install");
    }

    static bool has_map_layout(const boost::filesystem::path &root)
    {
        return !root.empty() && boost::filesystem::is_directory(root);
    }

    static bool has_config_layout(const boost::filesystem::path &root)
    {
        if (root.empty() || !boost::filesystem::is_directory(root))
        {
            return false;
        }
        return boost::filesystem::is_regular_file(root / "slam" / "config.lua") &&
               boost::filesystem::is_regular_file(root / "pure_location_odom" / "config.lua");
    }

    std::string resolve_slam_root() const
    {
        const std::string env_root = std::string(std::getenv("SLAM_ROOT") ? std::getenv("SLAM_ROOT") : "");
        if (!env_root.empty())
        {
            const boost::filesystem::path candidate = boost::filesystem::path(env_root);
            if (has_runtime_layout(candidate))
            {
                return candidate.string();
            }
        }

        try
        {
            const boost::filesystem::path cwd = boost::filesystem::current_path();
            if (has_runtime_layout(cwd))
            {
                return cwd.string();
            }
        }
        catch (const std::exception &)
        {
        }

        for (const auto &pkg_name : {"cleanrobot", "coverage_planner", "cartographer_ros"})
        {
            const std::string pkg_path = ros::package::getPath(pkg_name);
            if (pkg_path.empty())
            {
                continue;
            }

            boost::filesystem::path candidate = boost::filesystem::path(pkg_path);
            for (int i = 0; i < 6 && !candidate.empty(); ++i)
            {
                if (has_runtime_layout(candidate))
                {
                    return candidate.string();
                }
                const boost::filesystem::path parent = candidate.parent_path();
                if (parent == candidate)
                {
                    break;
                }
                candidate = parent;
            }
        }

        return env_root;
    }

    std::string resolve_config_root() const
    {
        const std::string env_root = std::string(std::getenv("SLAM_CONFIG_ROOT") ? std::getenv("SLAM_CONFIG_ROOT") : "");
        if (!env_root.empty())
        {
            const boost::filesystem::path candidate = boost::filesystem::path(env_root);
            if (has_config_layout(candidate))
            {
                return candidate.string();
            }
        }

        const boost::filesystem::path deployment_candidate("/data/config/slam/cartographer");
        if (has_config_layout(deployment_candidate))
        {
            return deployment_candidate.string();
        }

        const std::string cleanrobot_path = ros::package::getPath("cleanrobot");
        if (!cleanrobot_path.empty())
        {
            const boost::filesystem::path candidate =
                boost::filesystem::path(cleanrobot_path) / "config" / "slam" / "cartographer";
            if (has_config_layout(candidate))
            {
                return candidate.string();
            }
        }

        return env_root;
    }

    std::string resolve_map_root() const
    {
        const std::string env_root = std::string(std::getenv("SLAM_MAP_ROOT") ? std::getenv("SLAM_MAP_ROOT") : "");
        if (!env_root.empty())
        {
            const boost::filesystem::path candidate = boost::filesystem::path(env_root);
            if (has_map_layout(candidate))
            {
                return candidate.string();
            }
        }

        const boost::filesystem::path deployment_candidate("/data/maps");
        if (has_map_layout(deployment_candidate))
        {
            return deployment_candidate.string();
        }

        if (!this->slam_root.empty())
        {
            const boost::filesystem::path legacy_candidate = boost::filesystem::path(this->slam_root) / "map";
            if (has_map_layout(legacy_candidate))
            {
                return legacy_candidate.string();
            }
        }

        return env_root;
    }

    bool initialize_runtime_paths()
    {
        this->slam_root = resolve_slam_root();
        this->config_root = resolve_config_root();
        this->map_root = resolve_map_root();

        if (this->slam_root.empty())
        {
            ROS_ERROR("failed to resolve cartographer runtime root; set SLAM_ROOT to the workspace root");
            return false;
        }

        if (this->config_root.empty())
        {
            ROS_ERROR("failed to resolve cartographer config root; set SLAM_CONFIG_ROOT or provide /data/config/slam/cartographer or cleanrobot/config/slam/cartographer");
            return false;
        }

        if (this->map_root.empty())
        {
            ROS_ERROR("failed to resolve cartographer map root; set SLAM_MAP_ROOT or provide /data/maps");
            return false;
        }

        ROS_INFO_STREAM("cartographer runtime root: " << this->slam_root);
        ROS_INFO_STREAM("cartographer config root: " << this->config_root);
        ROS_INFO_STREAM("cartographer map root: " << this->map_root);
        return true;
    }

    void ensure_async_spinner()
    {
        if (this->async_spinner)
        {
            return;
        }
        const int spinner_threads = ros_spinner_thread_count();
        this->async_spinner = std::make_unique<::ros::AsyncSpinner>(spinner_threads);
        this->async_spinner->start();
        ROS_INFO_STREAM("visual command AsyncSpinner threads: " << spinner_threads);
    }

    void start()
    {
        if (!root_node_handle)
        {
            root_node_handle = std::make_unique<::ros::NodeHandle>("");
        }
        ensure_async_spinner();
        if (!initialize_runtime_paths())
        {
            throw std::runtime_error("cartographer runtime/config root unavailable");
        }
        srv_visual_command = root_node_handle->advertiseService("visual_command", &visualweb::ros_visual_command_handler, this);
        ROS_INFO("legacy cartographer HTTP/WS visualweb endpoints removed; ROS service /visual_command remains available");
    }

    void ros_init()
    {
        ensure_async_spinner();
        client_set_param = this->node->node_handle()->serviceClient<robot_runtime_flags_msgs::SetParam>("/set_param");
    }

    bool init_from_config(const std::string &config_entry)
    {
        if (this->node)
        {
            return false;
        }

        if (config_entry == "slam")
        {
            flirt::use_flirt = true;
            std::cout << "FLIRT Enabled" << std::endl;
        }
        else
        {
            flirt::use_flirt = false;
            std::cout << "FLIRT Disabled" << std::endl;
        }

        sml_config::sml_init();
        flirt::init();

        const boost::filesystem::path config_dir = boost::filesystem::path(this->config_root) / config_entry;
        std::ifstream stream((config_dir / "config.lua").string());
        if (!stream.good())
        {
            return false;
        }

        constexpr double kTfBufferCacheTimeInSeconds = 10.;
        this->tf_buffer = std::make_unique<tf2_ros::Buffer>(::ros::Duration(kTfBufferCacheTimeInSeconds));
        this->tf = std::make_unique<tf2_ros::TransformListener>(*this->tf_buffer.get());

        auto file_resolver = std::make_unique<cartographer::common::ConfigurationFileResolver>(
            std::vector<std::string>{config_dir.string()});
        const std::string code = std::string((std::istreambuf_iterator<char>(stream)), std::istreambuf_iterator<char>());
        cartographer::common::LuaParameterDictionary lua_parameter_dictionary(code, std::move(file_resolver));

        this->node_options = CreateNodeOptions(&lua_parameter_dictionary);
        this->trajectory_options = CreateTrajectoryOptions(&lua_parameter_dictionary);

        auto map_builder = cartographer::mapping::CreateMapBuilder(node_options.map_builder_options);
        this->node = std::make_unique<Node>(node_options, std::move(map_builder), tf_buffer.get(), false);

        this->ros_init();

        robot_runtime_flags_msgs::SetParam srv;
        srv.request.keys = {"slam_mode"};
        srv.request.vals = {config_entry == "slam" ? "true" : "false"};
        client_set_param.call(srv);

        return true;
    }

    static CommandResult make_result(int code, std::string msg, nlohmann::json data = nlohmann::json::object())
    {
        CommandResult result;
        result.code = code;
        result.msg = std::move(msg);
        result.data = std::move(data);
        return result;
    }

    static nlohmann::json parse_payload_json(const std::string &payload_json)
    {
        if (payload_json.empty())
        {
            return nlohmann::json::object();
        }
        return nlohmann::json::parse(payload_json);
    }

    CommandResult handle_get_trajectory_state()
    {
        if (this->node == nullptr)
        {
            return make_result(0, "System is not ready! Please load configs");
        }

        auto state = this->node->map_builder_bridge_.buider()->pose_graph()->GetTrajectoryStates();
        nlohmann::json data = nlohmann::json::object();
        for (const auto &entry : state)
        {
            const auto key = std::to_string(entry.first);
            if (entry.second == ::cartographer::mapping::PoseGraphInterface::TrajectoryState::ACTIVE)
            {
                data[key] = "active";
            }
            else if (entry.second == ::cartographer::mapping::PoseGraphInterface::TrajectoryState::FINISHED)
            {
                data[key] = "finished";
            }
            else if (entry.second == ::cartographer::mapping::PoseGraphInterface::TrajectoryState::FROZEN)
            {
                data[key] = "frozen";
            }
            else if (entry.second == ::cartographer::mapping::PoseGraphInterface::TrajectoryState::DELETED)
            {
                data[key] = "deleted";
            }
        }
        return make_result(0, "", std::move(data));
    }

    CommandResult handle_load_config(const std::string &payload_json)
    {
        try
        {
            const nlohmann::json payload = parse_payload_json(payload_json);
            const std::string entry = payload.at("config_entry").get<std::string>();
            if (init_from_config(entry))
            {
                return make_result(0, "");
            }
            return make_result(2, "Failed, please do NOT reload repeatedly. 加载失败，请勿重复加载。");
        }
        catch (const std::exception &e)
        {
            return make_result(1, "Exception:" + std::string(e.what()));
        }
    }

    CommandResult handle_load_state(const std::string &payload_json)
    {
        if (this->node == nullptr)
        {
            return make_result(1, "System is not ready! Please load configs, 请先加载配置。");
        }

        try
        {
            const nlohmann::json payload = parse_payload_json(payload_json);
            const std::string filename = payload.at("filename").get<std::string>();
            const bool frozen = payload.at("frozen").get<bool>();
            const boost::filesystem::path requested(filename);
            const std::string full_path = requested.is_absolute()
                                              ? requested.string()
                                              : (boost::filesystem::path(this->map_root) / requested).string();

            std::ifstream stream(full_path);
            if (!stream.good())
            {
                return make_result(1, "Not Found");
            }
            stream.close();
            this->node->LoadState(full_path, frozen);
            return make_result(0, "");
        }
        catch (const std::exception &e)
        {
            return make_result(1, "Exception:" + std::string(e.what()));
        }
    }

    CommandResult handle_save_state(const std::string &payload_json)
    {
        if (this->node == nullptr)
        {
            return make_result(1, "System is not ready! Please load configs");
        }

        try
        {
            const nlohmann::json payload = parse_payload_json(payload_json);
            const std::string filename = payload.at("filename").get<std::string>();
            const bool include_unfinished = payload.at("unfinished").get<bool>();
            const boost::filesystem::path requested(filename);
            const std::string full_path = requested.is_absolute()
                                              ? requested.string()
                                              : (boost::filesystem::path(this->map_root) / requested).string();
            this->node->RunFinalOptimization();
            const bool ok = this->node->SerializeState(full_path, include_unfinished);
            if (ok)
            {
                return make_result(0, "");
            }
            return make_result(2, "Failed to save.");
        }
        catch (const std::exception &e)
        {
            return make_result(1, "Exception:" + std::string(e.what()));
        }
    }

    CommandResult handle_get_dirlist(const std::string &payload_json)
    {
        try
        {
            std::vector<std::string> filenames;
            const nlohmann::json payload = parse_payload_json(payload_json);
            const std::string dirname = payload.at("dirname").get<std::string>();
            boost::filesystem::path dirpath;
            if (dirname == "/map" || dirname == "map")
            {
                dirpath = boost::filesystem::path(this->map_root);
            }
            else
            {
                const boost::filesystem::path requested(dirname);
                dirpath = requested.is_absolute()
                              ? requested
                              : (boost::filesystem::path(this->slam_root) / requested);
            }
            if (boost::filesystem::exists(dirpath) && boost::filesystem::is_directory(dirpath))
            {
                for (const auto &entry : boost::filesystem::directory_iterator(dirpath))
                {
                    if (boost::filesystem::status(entry.path()).type() == boost::filesystem::regular_file)
                    {
                        filenames.push_back(entry.path().filename().string());
                    }
                }
            }
            return make_result(0, "Successfully", {{"filenames", filenames}});
        }
        catch (const std::exception &e)
        {
            return make_result(1, "Exception:" + std::string(e.what()));
        }
    }

    CommandResult handle_add_trajectory()
    {
        if (this->node == nullptr)
        {
            return make_result(1, "System is not ready! Please load configs");
        }
        const auto id = this->node->AddTrajectory(this->trajectory_options);
        return make_result(0, "", {{"trajectory_id", id}});
    }

    CommandResult handle_finish_trajectory(const std::string &payload_json)
    {
        if (this->node == nullptr)
        {
            return make_result(1, "System is not ready! Please load configs");
        }

        try
        {
            const nlohmann::json payload = parse_payload_json(payload_json);
            const uint32_t id = payload.at("id").get<uint32_t>();
            if (this->node->FinishTrajectory(id))
            {
                return make_result(0, "");
            }
            return make_result(2, "Failed to finish trajecotry.");
        }
        catch (const std::exception &e)
        {
            return make_result(1, "Exception:" + std::string(e.what()));
        }
    }

    CommandResult handle_finish()
    {
        if (this->node == nullptr)
        {
            return make_result(1, "System is not ready! Please load configs");
        }
        this->node->FinishAllTrajectories();
        return make_result(0, "");
    }

    CommandResult handle_optimize()
    {
        if (this->node == nullptr)
        {
            return make_result(-1, "尚未加载配置，请加载配置");
        }

        this->node->RunOptimization();
        robot_runtime_flags_msgs::SetParam srv;
        srv.request.keys = {"global_relocated"};
        srv.request.vals = {"true"};
        client_set_param.call(srv);
        return make_result(0, "");
    }

    CommandResult handle_try_global_relocate()
    {
        auto set_global_relocated = [this](bool value)
        {
            robot_runtime_flags_msgs::SetParam srv;
            srv.request.keys = {"global_relocated"};
            srv.request.vals = {value ? "true" : "false"};
            client_set_param.call(srv);
        };

        if (this->node == nullptr)
        {
            return make_result(-1, "尚未加载配置，请加载配置");
        }

        {
            std::lock_guard<std::mutex> lock(flirt::flirt_busy_lock);
            if (flirt::need_flirt.load() || flirt::flirt_working.load())
            {
                return make_result(-5, "全局重定位正在执行，请稍后重试。");
            }
            flirt::flirt_return_code = flirt::kRelocationIdle;
            flirt::need_flirt.store(true);
        }

        std::unique_lock<std::mutex> lock(flirt::flirt_busy_lock);
        constexpr auto kGlobalRelocateStartTimeout = std::chrono::seconds(10);
        const bool started_or_completed = flirt::cv_flirt_busy.wait_for(
            lock, kGlobalRelocateStartTimeout, []()
            {
                return flirt::flirt_working.load() ||
                       (!flirt::need_flirt.load() && !flirt::flirt_working.load());
            });

        if (!started_or_completed)
        {
            flirt::need_flirt.store(false);
            lock.unlock();
            set_global_relocated(false);
            return make_result(-6, "等待新的激光/节点数据超时，未执行重定位。请确认雷达数据正常且系统仍在持续建图或定位。");
        }

        if (flirt::flirt_working.load())
        {
            flirt::cv_flirt_busy.wait(
                lock, []()
                { return !flirt::need_flirt.load() && !flirt::flirt_working.load(); });
        }
        lock.unlock();

        bool relocation_success = false;
        CommandResult result;

        if (flirt::flirt_return_code == flirt::kRelocationSuccess)
        {
            result = make_result(0, "");
            relocation_success = true;
        }
        else if (flirt::flirt_return_code == flirt::kRelocationNeedMoreTrajectories)
        {
            result = make_result(-2, "重定位需要至少2个Trajectories(轨迹)");
        }
        else if (flirt::flirt_return_code == flirt::kRelocationNoInterestPoints)
        {
            result = make_result(-3, "当前机器人位置没有关键特征，无法重定位，请改变机器人位置。");
        }
        else if (flirt::flirt_return_code == flirt::kRelocationSubmapNotFound)
        {
            result = make_result(-4, "匹配到的Node中，无法找到对应Submap!");
        }
        else if (flirt::flirt_return_code == flirt::kRelocationNoCandidatePose)
        {
            result = make_result(-8, "未在历史轨迹中找到可用的重定位候选位姿。");
        }
        else if (flirt::flirt_return_code == flirt::kRelocationLowConstraintScore)
        {
            result = make_result(-9, "候选位姿存在，但精匹配分数不足，未生成有效重定位约束。");
        }
        else
        {
            result = make_result(-7, "全局重定位失败，返回了未知状态码: " + std::to_string(flirt::flirt_return_code));
        }

        set_global_relocated(relocation_success);
        return result;
    }

    bool invoke_visual_command(const std::string &raw_command,
                               const std::string &payload_json,
                               int &code,
                               std::string &msg,
                               nlohmann::json &data)
    {
        std::string command = raw_command;
        while (!command.empty() && command.front() == '/')
        {
            command.erase(command.begin());
        }

        CommandResult result;
        if (command == "trajectory_state")
        {
            result = handle_get_trajectory_state();
        }
        else if (command == "load_config")
        {
            result = handle_load_config(payload_json);
        }
        else if (command == "load_state")
        {
            result = handle_load_state(payload_json);
        }
        else if (command == "save_state")
        {
            result = handle_save_state(payload_json);
        }
        else if (command == "dirlist")
        {
            result = handle_get_dirlist(payload_json);
        }
        else if (command == "add_trajectory")
        {
            result = handle_add_trajectory();
        }
        else if (command == "finish_trajectory")
        {
            result = handle_finish_trajectory(payload_json);
        }
        else if (command == "finish")
        {
            result = handle_finish();
        }
        else if (command == "optimize")
        {
            result = handle_optimize();
        }
        else if (command == "try_global_relocate")
        {
            result = handle_try_global_relocate();
        }
        else
        {
            result = make_result(-404, "Unknown visual command: " + command);
        }

        code = result.code;
        msg = std::move(result.msg);
        data = std::move(result.data);
        return code == 0;
    }

    bool ros_visual_command_handler(cartographer_ros_msgs::VisualCommand::Request &req,
                                    cartographer_ros_msgs::VisualCommand::Response &res)
    {
        int code = -500;
        std::string msg;
        nlohmann::json data = nlohmann::json::object();
        invoke_visual_command(req.command, req.payload_json, code, msg, data);
        res.code = code;
        res.msg = msg;
        res.data_json = data.dump();
        return true;
    }
};

} // namespace cartographer_ros

#endif

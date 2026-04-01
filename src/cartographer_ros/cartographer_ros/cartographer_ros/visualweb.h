#ifndef VISUAL_WEB_HPP
#define VISUAL_WEB_HPP

#define USE(x) (void)x
// std
#include <algorithm>
#include <assert.h>
#include <cstdlib>

#include <memory>
#include <queue>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <map>
// Boost
#include <boost/filesystem.hpp>
// depandencies
#include "path_track.h"
#include "movelib.h"
#include "param_space/ParamChanges.h"
#include "param_space/GetParam.h"
#include "param_space/SetParam.h"
// ros
#include "ros/ros.h"
// cartographer
#include "cartographer/common/configuration_file_resolver.h"
#include "cartographer/io/submap_painter.h"
#include "cartographer/mapping/map_builder.h"
#include "cartographer/mapping/proto/submap_visualization.pb.h"
#include "cartographer/mapping/proto/trajectory_list.pb.h"
#include "cartographer/mapping/proto/slam_result.pb.h"
#include "cartographer/transform/transform.h"
// cartographer ros
#include "cartographer_ros/json.hpp"
#include "cartographer_ros/json_fwd.hpp"
#include "cartographer_ros/map_builder_bridge.h"
#include "cartographer_ros/msg_conversion.h"
#include "cartographer_ros/node.h"
#include "cartographer_ros/node_constants.h"
#include "cartographer_ros/node_options.h"
#include "cartographer_ros/ros_log_sink.h"
#include "cartographer_ros/trajectory_options.h"
#include "cartographer_ros/visualweb_interface.hpp"
// cartographer ros msgs
#include "cartographer_ros_msgs/StatusCode.h"
#include "cartographer_ros_msgs/SubmapQuery.h"
#include "cartographer_ros_msgs/VisualCommand.h"
#include "visualization_msgs/MarkerArray.h"
// visual web defined msgs
#include "cartographer_ros_msgs/WebLog.h"
#include "tf2_ros/transform_listener.h"
// Other msgs
#include "geometry_msgs/Twist.h"

#include "keydog.h"

// Rviz 发出的2D Nav话题：
//  1. /move_base/current_goal
//  2. /move_base_simple/goal

typedef struct _submap_texture
{
    int32_t trajectory_id;
    int32_t submap_index;
    int32_t submap_version;
    std::unique_ptr<::cartographer::io::SubmapTextures> submap_textures;
} submap_texture_t;

typedef struct _landmark_info
{
    std::string id;
    double x, y, z;
    double qw, qx, qy, qz;
} landmark_info_t;

namespace cartographer_ros
{
    class visualweb : public visualweb_interface
    {
    public:
        std::unique_ptr<Node> node;
        std::unique_ptr<::ros::NodeHandle> root_node_handle;
        NodeOptions node_options;
        TrajectoryOptions trajectory_options;

        std::unique_ptr<tf2_ros::Buffer> tf_buffer;
        std::unique_ptr<tf2_ros::TransformListener> tf;

        // ROS
        std::unique_ptr<::ros::AsyncSpinner> async_spinner;

        // navigation
        volatile bool navigation_working;
        movelib _movelib;
        ::ros::Publisher _pub_path;
        ::ros::Publisher _pub_path_stamp;

        // manual control
        volatile bool remote_control_enable = false;
        ::ros::Publisher _teleop_cmd_vel;
        ::ros::Subscriber _nav_cmd_vel;
        ::ros::ServiceServer _srv_visual_command;

        // landmark
        ::ros::Subscriber _sub_landmark_list;
        std::mutex landmark_set_lock;
        std::vector<landmark_info_t> landmark_set;

        // param_space
        ::ros::Subscriber _sub_param_changes;
        ros::ServiceClient client_set_param;
        ros::ServiceClient client_get_param;

        // used to debug, because the console window is polluted by ROS.
        ::ros::Publisher _log_publisher;
        std::thread th_log;

        visualweb() {}
        ~visualweb() {}

        void vs_init() {}

        time_t to_timestamp(const std::string &datetime)
        {
            std::tm tm = {};
            std::istringstream ss(datetime);
            ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
            if (ss.fail())
            {
                std::cout << "Format error" << std::endl;
            }
            auto ts = mktime(&tm);
            return ts;
        }

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

        void start()
        {
            // auto now = std::chrono::system_clock::now();
            // auto ts = std::chrono::system_clock::to_time_t(now);
            // auto beg_ts = to_timestamp("2025-04-01 06:00:00");
            // auto end_ts = to_timestamp("2025-05-01 06:00:00");
            // std::cout << "TS Now=" << ts << std::endl;
            // std::cout << "TS Beg=" << beg_ts << std::endl;
            // std::cout << "TS End=" << end_ts << std::endl;
        //     if (keydog::validate_key() || true)
        //     {
        //         start_visualweb();
        //     }
        //     else
        //     {
        //         std::exit(0);
        //     }

            if (!root_node_handle)
            {
                root_node_handle = std::make_unique<::ros::NodeHandle>("");
            }
            if (!this->async_spinner)
            {
                // ROS services, timers, and sensor callbacks share the same
                // callback queue. Mapping traffic can otherwise starve service
                // callbacks such as /visual_command and /finish_trajectory.
                const int spinner_threads = ros_spinner_thread_count();
                this->async_spinner =
                    std::make_unique<::ros::AsyncSpinner>(spinner_threads);
                this->async_spinner->start();
                ROS_INFO_STREAM("visualweb AsyncSpinner threads: "
                                << spinner_threads);
            }
            _srv_visual_command = root_node_handle->advertiseService("visual_command", &visualweb::ros_visual_command_handler, this);

            start_visualweb();

        }

        void ros_init()
        {
            // ROS Init
            if (!this->async_spinner)
            {
                const int spinner_threads = ros_spinner_thread_count();
                this->async_spinner =
                    std::make_unique<::ros::AsyncSpinner>(spinner_threads);
                this->async_spinner->start();
                ROS_INFO_STREAM("visualweb AsyncSpinner threads: "
                                << spinner_threads);
            }
            // publish
            _teleop_cmd_vel = this->node->node_handle()->advertise<geometry_msgs::Twist>("cmd_vel", 100, false);
            _pub_path = this->node->node_handle()->advertise<nav_msgs::Path>("nav_path", 10, true);
            _pub_path_stamp = this->node->node_handle()->advertise<geometry_msgs::PoseStamped>("nav_path_stamp", 10, true);

            // subscribe
            _sub_param_changes = this->node->node_handle()->subscribe<param_space::ParamChanges>("/param_changes", 10, boost::bind(&visualweb::param_changes_handler, this, boost::placeholders::_1));

            // service client
            client_set_param = this->node->node_handle()->serviceClient<param_space::SetParam>("/set_param");
            client_get_param = this->node->node_handle()->serviceClient<param_space::GetParam>("/get_param");

            // the log can be seen by command "rostopic echo /visualweb_log"
            _log_publisher = this->node->node_handle()->advertise<cartographer_ros_msgs::WebLog>("visualweb_log", 1000, false);
            // th_log = std::thread(&visualweb::log_process, this);
            // th_log.detach();

            // ohter init
            _movelib.set_done_cb([this]()
                                 { stop_navigation(); });
        }

        int32_t ver = -1;

        void log_process()
        {
            while (true)
            {
                // Attention: Must sleep for a while
                std::this_thread::sleep_for(std::chrono::milliseconds(10000));
            }
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

            _movelib.set_controller(sml_config::get_fatal<std::string>(sml_config::sml_global_relocation, sml_config::CONFIG_MOVEBASE_CONTROLLER));

            std::ifstream stream(this->slam_root + "/config/" + config_entry + "/config.lua");
            if (!stream.good())
            {
                return false;
            }

            constexpr double kTfBufferCacheTimeInSeconds = 10.;
            this->tf_buffer = std::make_unique<tf2_ros::Buffer>(::ros::Duration(kTfBufferCacheTimeInSeconds));
            this->tf = std::make_unique<tf2_ros::TransformListener>(*this->tf_buffer.get());

            auto file_resolver = std::make_unique<cartographer::common::ConfigurationFileResolver>(std::vector<std::string>{this->slam_root + "/config/" + config_entry});
            const std::string code = std::string((std::istreambuf_iterator<char>(stream)), std::istreambuf_iterator<char>());
            cartographer::common::LuaParameterDictionary lua_parameter_dictionary(code, std::move(file_resolver));

            this->node_options = CreateNodeOptions(&lua_parameter_dictionary);
            this->trajectory_options = CreateTrajectoryOptions(&lua_parameter_dictionary);

            auto map_builder = cartographer::mapping::CreateMapBuilder(node_options.map_builder_options);
            this->node = std::make_unique<Node>(node_options, std::move(map_builder), tf_buffer.get(), false);

            this->ros_init();

            param_space::SetParam srv;
            srv.request.keys = {"slam_mode"};
            if (config_entry == "slam")
            {
                srv.request.vals = {"true"};
            }
            else
            {
                srv.request.vals = {"false"};
            }
            client_set_param.call(srv);

            return true;
        }

        void param_changes_handler(const param_space::ParamChangesConstPtr &changes)
        {
            // std::cout << "Changes:" << std::endl;
            // for (auto &&i : changes->changed_params)
            // {
            //     std::cout << i << std::endl;
            // }
            // std::cout << "Values:" << std::endl;
            // param_space::GetParam srv;
            // srv.request.keys = changes->changed_params;
            // client_get_param.call(srv);
            // for (auto &&i : srv.response.vals)
            // {
            //     std::cout << i << std::endl;
            // }
        }

        void nav_cmd_vel_handler(const boost::shared_ptr<geometry_msgs::Twist const> &msg)
        {
            if (!remote_control_enable)
            {
                geometry_msgs::Twist cmd;
                cmd.linear.x = msg->linear.x;
                cmd.linear.y = msg->linear.y;
                cmd.linear.z = msg->linear.z;
                cmd.angular.x = msg->angular.x;
                cmd.angular.y = msg->angular.y;
                cmd.angular.z = msg->angular.z;
                if (cmd.linear.x < 0)
                {
                    cmd.angular.z = -cmd.angular.z;
                }
                _teleop_cmd_vel.publish(cmd);
            }
        }

        void landmark_handler(const boost::shared_ptr<cartographer_ros_msgs::LandmarkList const> &msg)
        {
            // Landmark Circle ID: landmark_circle.id = "landmark0" + std::to_string(max_existing_id + 1);
            // Landmark Plane  ID: landmark_line.id = "landmark1" + std::to_string(current_landmark_id);
            // Landmark Code   ID: "landmark2" + std::to_string(tag_number);
            // 上面可能会变动，具体请查看landmark包下面的pub_landmark.cpp和tcp_server_node.cpp
            // 即使landmark是空的也需要发送

            // 用于检测导航是否到终点
            if (is_navigating())
            {
                // 遍历当前最新发布的landmark中，有没有符合goal_id的
                for (auto &&landmark : msg->landmarks)
                {
                    std::string id = landmark.id.substr(9);
                    if (is_goal_id(id))
                    {
                        _movelib.stop_navigation();
                        stop_navigation();
                        break;
                    }
                }
            }

            // 只需要landmark的id用于给前端显示当前扫描到了什么东西
            std::lock_guard<std::mutex> glock(landmark_set_lock);
            landmark_set.clear();
            if (msg->landmarks.size() > 0)
            {
                landmark_set.reserve(msg->landmarks.size());
            }
            for (auto &&landmark : msg->landmarks)
            {
                _landmark_info info;
                info.id = landmark.id;
                info.x = landmark.tracking_from_landmark_transform.position.x;
                info.y = landmark.tracking_from_landmark_transform.position.y;
                info.z = landmark.tracking_from_landmark_transform.position.z;

                info.qw = landmark.tracking_from_landmark_transform.orientation.w;
                info.qx = landmark.tracking_from_landmark_transform.orientation.x;
                info.qy = landmark.tracking_from_landmark_transform.orientation.y;
                info.qz = landmark.tracking_from_landmark_transform.orientation.z;
                landmark_set.push_back(std::move(info));
            }
        }

        // ----------------------------WebSocket---------------------------------

        void websocket_connection_init()
        {
            ROS_WARN("Visual SLAM has been connected!");
        }

        void ws_slam_result_onmsg(std::string &&msg)
        {
            cartographer::mapping::proto::SLAMResult slam_result;
            if (this->node == nullptr)
            {
                std::string buf;
                slam_result.SerializeToString(&buf);
                this->ws_slam_result->write(std::move(buf));
                return;
            }

            auto trajectory_data = this->node->map_builder_bridge_.GetLocalTrajectoryData();
            for (auto &&data : trajectory_data)
            {
                // data.second.local_slam_data->
                int32_t trajectory_id = data.first;
                auto local_to_map = cartographer_ros::ToGeometryMsgPose(data.second.local_to_map);
                auto local_pose = cartographer_ros::ToGeometryMsgPose(data.second.local_slam_data->local_pose);

                // compute robot pose
                auto robot_pose = std::move(data.second.local_to_map * data.second.local_slam_data->local_pose);
                auto robot_pose_msg = cartographer_ros::ToGeometryMsgPose(robot_pose);
                current_robot_pose.x = robot_pose_msg.position.x;
                current_robot_pose.y = robot_pose_msg.position.y;
                current_robot_pose.z = robot_pose_msg.position.z;
                current_robot_pose.qw = robot_pose_msg.orientation.w;
                current_robot_pose.qx = robot_pose_msg.orientation.x;
                current_robot_pose.qy = robot_pose_msg.orientation.y;
                current_robot_pose.qz = robot_pose_msg.orientation.z;

                cartographer::mapping::proto::SLAMResult::Result result;
                result.set_trajectory_id(trajectory_id);
                result.mutable_local_to_map()->mutable_translation()->set_x(local_to_map.position.x);
                result.mutable_local_to_map()->mutable_translation()->set_y(local_to_map.position.y);
                result.mutable_local_to_map()->mutable_translation()->set_z(local_to_map.position.z);
                result.mutable_local_to_map()->mutable_rotation()->set_w(local_to_map.orientation.w);
                result.mutable_local_to_map()->mutable_rotation()->set_x(local_to_map.orientation.x);
                result.mutable_local_to_map()->mutable_rotation()->set_y(local_to_map.orientation.y);
                result.mutable_local_to_map()->mutable_rotation()->set_z(local_to_map.orientation.z);

                result.mutable_local_pose()->mutable_translation()->set_x(local_pose.position.x);
                result.mutable_local_pose()->mutable_translation()->set_y(local_pose.position.y);
                result.mutable_local_pose()->mutable_translation()->set_z(local_pose.position.z);
                result.mutable_local_pose()->mutable_rotation()->set_w(local_pose.orientation.w);
                result.mutable_local_pose()->mutable_rotation()->set_x(local_pose.orientation.x);
                result.mutable_local_pose()->mutable_rotation()->set_y(local_pose.orientation.y);
                result.mutable_local_pose()->mutable_rotation()->set_z(local_pose.orientation.z);
                (*slam_result.mutable_results())[trajectory_id] = result;
            }
            std::string buf;
            slam_result.SerializeToString(&buf);
            this->ws_slam_result->write(std::move(buf));
        }

        void ws_trajectory_list_onmsg(std::string &&msg)
        {
            cartographer::mapping::proto::TrajectoryList list;
            if (this->node == nullptr)
            {
                std::string buf;
                list.SerializeToString(&buf);
                this->ws_trajectory_list->write(std::move(buf));
                return;
            }

            auto submap_list = this->node->map_builder_bridge_.GetSubmapList();
            for (auto &&entry : submap_list.submap)
            {
                int32_t trajectory_id = entry.trajectory_id;
                int32_t submap_index = entry.submap_index;
                int32_t submap_version = entry.submap_version;

                cartographer::mapping::proto::TrajectoryList::Submap submap;
                submap.set_trajectory_id(trajectory_id);
                submap.set_submap_index(submap_index);
                submap.set_submap_version(submap_version);
                submap.mutable_pose()->mutable_translation()->set_x(entry.pose.position.x);
                submap.mutable_pose()->mutable_translation()->set_y(entry.pose.position.y);
                submap.mutable_pose()->mutable_translation()->set_z(entry.pose.position.z);
                submap.mutable_pose()->mutable_rotation()->set_w(entry.pose.orientation.w);
                submap.mutable_pose()->mutable_rotation()->set_x(entry.pose.orientation.x);
                submap.mutable_pose()->mutable_rotation()->set_y(entry.pose.orientation.y);
                submap.mutable_pose()->mutable_rotation()->set_z(entry.pose.orientation.z);
                auto pb_trajectory = list.mutable_trajectories();
                auto pb_submap = (*pb_trajectory)[trajectory_id].mutable_submaps();
                (*pb_submap)[submap_index] = submap;
            }
            std::string buf;
            list.SerializeToString(&buf);
            this->ws_trajectory_list->write(std::move(buf));
        }

        void ws_submap_query_onmsg(std::string &&msg)
        {
            cartographer::mapping::proto::SubmapQuery submap;
            if (this->node == nullptr)
            {
                std::string buf;
                submap.SerializeToString(&buf);
                this->ws_submap_query->write(std::move(buf));
                return;
            }

            auto req = submap.mutable_request();
            req->ParseFromString(msg);
            int32_t trajectory_id = req->trajectory_id();
            int32_t submap_index = req->submap_index();
            // handle
            submap.mutable_request()->set_trajectory_id(trajectory_id);
            submap.mutable_request()->set_submap_index(submap_index);
            try
            {
                this->node->map_builder_bridge_.SubmapToProto(trajectory_id, submap_index, submap.mutable_response());
            }
            catch (const std::exception &e)
            {
                std::cerr << e.what() << '\n';
                this->ws_submap_query->write("");
                return;
            }
            std::string buf;
            submap.SerializeToString(&buf);
            this->ws_submap_query->write(std::move(buf));
        }

        void ws_control_onmsg(std::string &&msg)
        {
            try
            {
                nlohmann::json data = nlohmann::json::parse(msg);
                bool enable = data["ENABLE"].get<bool>();
                if (enable)
                {
                    if (remote_control_enable == false)
                    {
                        remote_control_enable = true;
                        ROS_WARN("Control ON");
                    }
                }
                else
                {
                    if (remote_control_enable == true)
                    {
                        remote_control_enable = false;
                        ROS_WARN("Control OFF!!!!!!!!!!!");
                    }
                }
                float base_velocity = data["BASE_VELOCITY"].get<float>();
                float base_angular = data["BASE_ANGULAR"].get<float>();
                float base_acceleration = data["BASE_ACCELERATION"].get<float>();
                float LX = data["LX"].get<float>();
                float LY = data["LY"].get<float>();
                float RX = data["RX"].get<float>();
                float RY = data["RY"].get<float>();
                float LB1 = data["LB1"].get<float>();
                float LB2 = data["LB2"].get<float>();
                float RB1 = data["RB1"].get<float>();
                float RB2 = data["RB2"].get<float>();
                float LS1 = data["LS1"].get<float>();
                float LS2 = data["LS2"].get<float>();
                float RS1 = data["RS1"].get<float>();
                float RS2 = data["RS2"].get<float>();
                USE(base_velocity);
                USE(base_angular);
                USE(base_acceleration);
                USE(LX);
                USE(RY);
                USE(LB1);
                USE(LB2);
                USE(RB1);
                USE(RB2);
                USE(LS1);
                USE(LS2);
                USE(RS1);
                USE(RS2);

                geometry_msgs::Twist msg;
                msg.linear.x = base_velocity * LY;
                if (msg.linear.x >= 0)
                {
                    msg.angular.z = -base_angular * RX;
                }
                else
                {
                    msg.angular.z = base_angular * RX;
                }
                if (remote_control_enable)
                {
                    _teleop_cmd_vel.publish(msg);
                }
            }
            catch (const std::exception &e)
            {
            }
            this->ws_control->write("ACK");
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

            auto req = std::make_shared<cyber::http_request>();
            auto res = std::make_shared<cyber::http_response>();
            req->content = payload_json;

            if (command == "trajectory_state")
            {
                service_get_trajectory_state(req, res);
            }
            else if (command == "load_config")
            {
                service_load_config(req, res);
            }
            else if (command == "load_state")
            {
                service_load_state(req, res);
            }
            else if (command == "save_state")
            {
                service_save_state(req, res);
            }
            else if (command == "dirlist")
            {
                service_get_dirlist(req, res);
            }
            else if (command == "add_trajectory")
            {
                service_add_trajectory(req, res);
            }
            else if (command == "finish_trajectory")
            {
                service_finish_trajectory(req, res);
            }
            else if (command == "finish")
            {
                service_finish(req, res);
            }
            else if (command == "optimize")
            {
                service_optimize(req, res);
            }
            else if (command == "try_global_relocate")
            {
                service_try_global_relocate(req, res);
            }
            else
            {
                code = -404;
                msg = "Unknown visual command: " + command;
                data = nlohmann::json::object();
                return false;
            }

            try
            {
                const auto response = nlohmann::json::parse(res->content);
                code = response.value("code", -500);
                msg = response.value("msg", std::string());
                data = response.value("data", nlohmann::json::object());
            }
            catch (const std::exception &e)
            {
                code = -500;
                msg = "Failed to parse command response: " + std::string(e.what());
                data = nlohmann::json::object();
                return false;
            }

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

        // ---------------------------------------Service---------------------------------------------

        void RetCode(const cyber::HttpResponsePtr &res, int code, std::string &&msg, const nlohmann::json &v)
        {
            nlohmann::json json = {{"code", code}, {"msg", msg}, {"data", v}};
            res->code = cyber::HttpCode::OK;
            res->content = json.dump();
        }

        void service_reboot(const cyber::HttpRequestPtr &req, const cyber::HttpResponsePtr &res)
        {
            exit(0);
        }

        void service_get_trajectory_state(const cyber::HttpRequestPtr &req, const cyber::HttpResponsePtr &res)
        {
            if (this->node == nullptr)
            {
                RetCode(res, 0, "System is not ready! Please load configs", {});
                return;
            }
            auto state = this->node->map_builder_bridge_.buider()->pose_graph()->GetTrajectoryStates();
            nlohmann::json v = {};
            for (auto &&s : state)
            {
                const auto key = std::to_string(s.first);
                if (s.second == ::cartographer::mapping::PoseGraphInterface::TrajectoryState::ACTIVE)
                {
                    v[key] = "active";
                }
                else if (s.second == ::cartographer::mapping::PoseGraphInterface::TrajectoryState::FINISHED)
                {
                    v[key] = "finished";
                }
                else if (s.second == ::cartographer::mapping::PoseGraphInterface::TrajectoryState::FROZEN)
                {
                    v[key] = "frozen";
                }
                else if (s.second == ::cartographer::mapping::PoseGraphInterface::TrajectoryState::DELETED)
                {
                    v[key] = "deleted";
                }
            }
            RetCode(res, 0, "", v);
        }

        void service_load_config(const cyber::HttpRequestPtr &req, const cyber::HttpResponsePtr &res)
        {
            try
            {
                nlohmann::json data = nlohmann::json::parse(req->content);
                std::string entry = data["config_entry"].get<std::string>();
                bool ok = init_from_config(entry);
                if (ok)
                {
                    RetCode(res, 0, "", {});
                }
                else
                {
                    RetCode(res, 2, "Failed, please do NOT reload repeatedly. 加载失败，请勿重复加载。", {});
                }
            }
            catch (const std::exception &e)
            {
                RetCode(res, 1, "Exception:" + std::string(e.what()), {});
            }
        }

        void service_load_state(const cyber::HttpRequestPtr &req, const cyber::HttpResponsePtr &res)
        {
            if (this->node == nullptr)
            {
                RetCode(res, 1, "System is not ready! Please load configs, 请先加载配置。", {});
                return;
            }
            try
            {
                nlohmann::json data = nlohmann::json::parse(req->content);
                auto filename = data["filename"].get<std::string>();
                auto frozen = data["frozen"].get<bool>();

                std::ifstream stream(this->slam_root + std::string("/map/") + filename);
                if (!stream.good())
                {
                    RetCode(res, 1, "Not Found", {});
                    return;
                }
                stream.close();
                this->node->LoadState(this->slam_root + std::string("/map/") + filename, frozen);
                RetCode(res, 0, "", {});
            }
            catch (const std::exception &e)
            {
                RetCode(res, 1, "Exception:" + std::string(e.what()), {});
            }
        }

        void service_save_state(const cyber::HttpRequestPtr &req, const cyber::HttpResponsePtr &res)
        {
            if (this->node == nullptr)
            {
                RetCode(res, 1, "System is not ready! Please load configs", {});
                return;
            }
            try
            {
                nlohmann::json data = nlohmann::json::parse(req->content);
                auto filename = data["filename"].get<std::string>();
                auto includeUnfinished = data["unfinished"].get<bool>();
                this->node->RunFinalOptimization();
                bool ok = this->node->SerializeState(this->slam_root + std::string("/map/") + filename, includeUnfinished);
                if (ok)
                {
                    RetCode(res, 0, "", {});
                }
                else
                {
                    RetCode(res, 2, "Failed to save.", {});
                }
            }
            catch (const std::exception &e)
            {
                RetCode(res, 1, "Exception:" + std::string(e.what()), {});
            }
        }

        void service_get_dirlist(const cyber::HttpRequestPtr &req, const cyber::HttpResponsePtr &res)
        {
            try
            {
                // 这里的dirname是相对于SLAM_ROOT而言的相对路径，
                // 例如SLAM_ROOT=/home/user/carto
                // dirname为/map
                // 表示遍历map下所有的文件名
                std::vector<std::string> filenames;
                nlohmann::json data = nlohmann::json::parse(req->content);
                auto dirname = data["dirname"].get<std::string>();
                boost::filesystem::path dirpath = slam_root + dirname;
                if (boost::filesystem::exists(dirpath) && boost::filesystem::is_directory(dirpath))
                {
                    for (auto &&entry : boost::filesystem::directory_iterator(dirpath))
                    {
                        if (boost::filesystem::status(entry.path()).type() == boost::filesystem::regular_file)
                        {
                            filenames.push_back(entry.path().filename().string());
                        }
                    }
                }
                RetCode(res, 0, "Successfully", {{"filenames", filenames}});
            }
            catch (const std::exception &e)
            {
                RetCode(res, 1, "Exception:" + std::string(e.what()), {});
            }
        }

        void service_add_trajectory(const cyber::HttpRequestPtr &req, const cyber::HttpResponsePtr &res)
        {
            if (this->node == nullptr)
            {
                RetCode(res, 1, "System is not ready! Please load configs", {});
                return;
            }
            auto id = this->node->AddTrajectory(this->trajectory_options);
            RetCode(res, 0, "", {{"trajectory_id", id}});
        }

        void service_finish_trajectory(const cyber::HttpRequestPtr &req, const cyber::HttpResponsePtr &res)
        {
            if (this->node == nullptr)
            {
                RetCode(res, 1, "System is not ready! Please load configs", {});
                return;
            }
            try
            {
                nlohmann::json data = nlohmann::json::parse(req->content);
                uint32_t id = data["id"].get<uint32_t>();
                bool ok = this->node->FinishTrajectory(id);
                if (ok)
                {
                    RetCode(res, 0, "", {});
                }
                else
                {
                    RetCode(res, 2, "Failed to finish trajecotry.", {});
                }
            }
            catch (const std::exception &e)
            {
                RetCode(res, 1, "Exception:" + std::string(e.what()), {});
            }
        }

        void service_finish(const cyber::HttpRequestPtr &req, const cyber::HttpResponsePtr &res)
        {
            if (this->node == nullptr)
            {
                RetCode(res, 1, "System is not ready! Please load configs", {});
                return;
            }
            this->node->FinishAllTrajectories();
            RetCode(res, 0, "", {});
        }

        void service_load_path(const cyber::HttpRequestPtr &req, const cyber::HttpResponsePtr &res)
        {
            try
            {
                nlohmann::json data = nlohmann::json::parse(req->content);
                std::string filename = data["filename"].get<std::string>();
                std::ifstream in(this->slam_root + "/path/" + filename, std::ios::in);
                if (!in.good())
                {
                    RetCode(res, 2, "Failed to read, cannot open the file.", {});
                    return;
                }
                else
                {
                    std::stringstream ss;
                    ss << in.rdbuf();
                    std::string pathjson = ss.str();
                    in.close();

                    if (parse_path(pathjson))
                    {
                        RetCode(res, 0, "", {{"pathjson", pathjson}});
                    }
                    else
                    {
                        RetCode(res, 1, "", {{"pathjson", pathjson}});
                    }
                }
            }
            catch (const std::exception &e)
            {
                RetCode(res, 1, "Exception:" + std::string(e.what()), {});
            }
        }

        void service_save_path(const cyber::HttpRequestPtr &req, const cyber::HttpResponsePtr &res)
        {
            try
            {
                nlohmann::json data = nlohmann::json::parse(req->content);
                std::string filename = data["filename"].get<std::string>();
                std::string pathjson = data["pathjson"].get<std::string>();
                std::ofstream out(this->slam_root + "/path/" + filename, std::ios::out);
                if (!out.is_open())
                {
                    RetCode(res, 2, "Failed to save, cannot open the file.", {});
                }
                else
                {
                    out << pathjson;
                    out.close();
                    RetCode(res, 0, "", {});
                }
            }
            catch (const std::exception &e)
            {
                RetCode(res, 1, "Exception:" + std::string(e.what()), {});
            }
        }

        void service_query_landmark(const cyber::HttpRequestPtr &req, const cyber::HttpResponsePtr &res)
        {

            if (this->node == nullptr)
            {
                RetCode(res, 1, "failed to query landmark: node is nullptr", {});
                return;
            }

            nlohmann::json markers;
            auto array_data = this->node->map_builder_bridge_.GetLandmarkPosesList();
            for (auto i = array_data.markers.cbegin(); i != array_data.markers.cend(); i++)
            {
                double x = (*i).pose.position.x;
                double y = (*i).pose.position.y;
                double z = (*i).pose.position.z;
                double qw = (*i).pose.orientation.w;
                double qx = (*i).pose.orientation.x;
                double qy = (*i).pose.orientation.y;
                double qz = (*i).pose.orientation.z;
                double color_r = (*i).color.r;
                double color_g = (*i).color.g;
                double color_b = (*i).color.b;
                double color_a = (*i).color.a;
                markers.push_back({{"id", i->id},
                                   {"text", i->text}, // 这里给landmark检测程序，用来区分不同的landmark
                                   {"position", std::vector<double>({x, y, z})},
                                   {"orientation", std::vector<double>({qw, qx, qy, qz})},
                                   {"rgba", std::vector<double>({color_r, color_g, color_b, color_a})}});
            }

            std::lock_guard<std::mutex> glock(landmark_set_lock);
            // 下面所发送的两个的区别：
            // makers: 这个是landmark进入了cartographer后端之后才会有，相比landmark
            //         会更加滞后一些
            // landmarks：是用于前端UI进行显示是否扫描到，更加注重实时性一些
            nlohmann::json landmarks;
            for (auto i = landmark_set.cbegin(); i != landmark_set.cend(); i++)
            {
                landmarks.push_back({{"id", i->id},
                                     {"position", std::vector<double>({i->x, i->y, i->z})},
                                     {"orientation", std::vector<double>({i->qw, i->qx, i->qy, i->qz})}});
            }

            nlohmann::json data = {
                {"markers", markers},
                {"landmarks", landmarks},
            };
            RetCode(res, 0, "", data);
        }

        void service_load_task(const cyber::HttpRequestPtr &req, const cyber::HttpResponsePtr &res)
        {
            std::ifstream in(this->slam_root + "/path/" + "path.json", std::ios::in);
            if (!in.is_open())
            {
                RetCode(res, 2, "Failed to read, cannot open the file.", {});
                return;
            }
            std::stringstream ss;
            ss << in.rdbuf();
            std::string pathjson = ss.str();
            in.close();

            if (parse_path(pathjson))
            {
                RetCode(res, 0, "successfully", {});
            }
            else
            {
                RetCode(res, 1, "Failed", {});
            }
        }

        void service_start_task(const cyber::HttpRequestPtr &req, const cyber::HttpResponsePtr &res)
        {
            if (is_navigating())
            {
                if (is_nav_paused())
                {
                    _movelib.resume_navigation();
                    start_navigation();
                    RetCode(res, 0, "Task resumed!", {});
                }
                else
                {
                    RetCode(res, 1, "Task Busy!", {});
                }
            }
            else
            {

                if (remote_control_enable)
                {
                    RetCode(res, 1, "Manual Control is on, cannot start navigation.", {});
                    return;
                }
                if (!has_task_loaded())
                {
                    RetCode(res, 2, "No task has been loaded", {});
                    return;
                }
                if (is_navigating())
                {
                    RetCode(res, 3, "Navigation task is still working.", {});
                    return;
                }
                publish_track_path([this](nav_msgs::Path &path)
                                   { 
                                _pub_path.publish(path);
                                // 可以用于动态观察 路径中的pose_stamp的方向
                                // for (size_t i = 0; i < path.poses.size(); i++)
                                // {
                                //     auto stamp = path.poses.at(i);
                                //     _pub_path_stamp.publish(stamp);
                                //     std::this_thread::sleep_for(std::chrono::milliseconds(50));
                                // }
                                start_navigation();
                                _movelib.set_path(path);
                                _movelib.start_navigation(); });
                RetCode(res, 0, "Task begins!", {});
            }
        }

        void service_pause_task(const cyber::HttpRequestPtr &req, const cyber::HttpResponsePtr &res)
        {
            if (is_navigating() && !is_nav_paused())
            {
                _movelib.pause_navigation();
                pause_navigation();
                RetCode(res, 0, "Successfully", {});
            }
            else
            {
                RetCode(res, 1, "No task is running!", {});
            }
        }

        void service_stop_task(const cyber::HttpRequestPtr &req, const cyber::HttpResponsePtr &res)
        {
            if (is_navigating())
            {
                _movelib.stop_navigation();
                stop_navigation();
                RetCode(res, 0, "Successfully", {});
            }
            else
            {
                RetCode(res, 1, "No task is running.", {});
            }
        }

        void service_optimize(const cyber::HttpRequestPtr &req, const cyber::HttpResponsePtr &res)
        {
            param_space::SetParam srv;
            if (this->node == nullptr)
            {
                RetCode(res, -1, "尚未加载配置，请加载配置", {});
                return;
            }
            this->node->RunOptimization();
            srv.request.keys = {"global_relocated"};
            srv.request.vals = {"true"};
            client_set_param.call(srv);
            RetCode(res, 0, "", {});
        }

        void service_try_global_relocate(const cyber::HttpRequestPtr &req, const cyber::HttpResponsePtr &res)
        {
            USE(req);

            auto set_global_relocated = [this](bool value)
            {
                param_space::SetParam srv;
                srv.request.keys = {"global_relocated"};
                srv.request.vals = {value ? "true" : "false"};
                client_set_param.call(srv);
            };

            if (this->node == nullptr)
            {
                RetCode(res, -1, "尚未加载配置，请加载配置", {});
                return;
            }

            {
                std::lock_guard<std::mutex> lock(flirt::flirt_busy_lock);
                if (flirt::need_flirt.load() || flirt::flirt_working.load())
                {
                    RetCode(res, -5, "全局重定位正在执行，请稍后重试。", {});
                    return;
                }
                flirt::flirt_return_code = flirt::kRelocationIdle;
                flirt::need_flirt.store(true);
            }

            std::unique_lock<std::mutex> ulock(flirt::flirt_busy_lock);
            constexpr auto kGlobalRelocateStartTimeout = std::chrono::seconds(10);
            const bool started_or_completed = flirt::cv_flirt_busy.wait_for(
                ulock, kGlobalRelocateStartTimeout, []()
                {
                    return flirt::flirt_working.load() ||
                           (!flirt::need_flirt.load() && !flirt::flirt_working.load());
                });

            if (!started_or_completed)
            {
                flirt::need_flirt.store(false);
                ulock.unlock();
                set_global_relocated(false);
                RetCode(res, -6, "等待新的激光/节点数据超时，未执行重定位。请确认雷达数据正常且系统仍在持续建图或定位。", {});
                return;
            }

            if (flirt::flirt_working.load())
            {
                flirt::cv_flirt_busy.wait(
                    ulock, []()
                    { return !flirt::need_flirt.load() && !flirt::flirt_working.load(); });
            }
            ulock.unlock();

            bool relocation_success = false;

            if (flirt::flirt_return_code == flirt::kRelocationSuccess)
            {
                RetCode(res, 0, "", {});
                relocation_success = true;
            }
            else if (flirt::flirt_return_code == flirt::kRelocationNeedMoreTrajectories)
            {
                RetCode(res, -2, "重定位需要至少2个Trajectories(轨迹)", {});
            }
            else if (flirt::flirt_return_code == flirt::kRelocationNoInterestPoints)
            {
                RetCode(res, -3, "当前机器人位置没有关键特征，无法重定位，请改变机器人位置。", {});
            }
            else if (flirt::flirt_return_code == flirt::kRelocationSubmapNotFound)
            {
                RetCode(res, -4, "匹配到的Node中，无法找到对应Submap!", {});
            }
            else if (flirt::flirt_return_code == flirt::kRelocationNoCandidatePose)
            {
                RetCode(res, -8, "未在历史轨迹中找到可用的重定位候选位姿。", {});
            }
            else if (flirt::flirt_return_code == flirt::kRelocationLowConstraintScore)
            {
                RetCode(res, -9, "候选位姿存在，但精匹配分数不足，未生成有效重定位约束。", {});
            }
            else
            {
                RetCode(res, -7, "全局重定位失败，返回了未知状态码: " + std::to_string(flirt::flirt_return_code), {});
            }

            set_global_relocated(relocation_success);
        }
    };

} // namespace cartographer_ros
#endif

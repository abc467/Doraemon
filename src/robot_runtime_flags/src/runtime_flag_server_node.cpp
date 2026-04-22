#include "ros/ros.h"
#include "robot_runtime_flags_msgs/GetParam.h"
#include "robot_runtime_flags_msgs/ParamChanges.h"
#include "robot_runtime_flags_msgs/SetParam.h"

#include <string>
#include <unordered_map>

namespace
{
std::unordered_map<std::string, std::string> g_runtime_flags;
ros::Publisher g_param_changes_pub;

bool set_param_callback(robot_runtime_flags_msgs::SetParam::Request &req,
                        robot_runtime_flags_msgs::SetParam::Response &res)
{
    if (req.keys.size() != req.vals.size())
    {
        res.opcode = -1;
        res.msg = "the size of keys and the size of vals is different!";
        return false;
    }

    for (std::size_t i = 0; i < req.keys.size(); i++)
    {
        g_runtime_flags[req.keys.at(i)] = req.vals.at(i);
    }

    robot_runtime_flags_msgs::ParamChanges msg;
    msg.changed_params = req.keys;
    g_param_changes_pub.publish(msg);

    res.opcode = 0;
    res.msg = "successfully";
    return true;
}

bool get_param_callback(robot_runtime_flags_msgs::GetParam::Request &req,
                        robot_runtime_flags_msgs::GetParam::Response &res)
{
    for (std::size_t i = 0; i < req.keys.size(); i++)
    {
        res.vals.push_back(g_runtime_flags[req.keys.at(i)]);
    }
    res.opcode = 0;
    res.msg = "successfully";
    return true;
}
}  // namespace

int main(int argc, char *argv[])
{
    ros::init(argc, argv, "runtime_flag_server");
    ros::NodeHandle node;
    ros::ServiceServer set_param_service = node.advertiseService("/set_param", set_param_callback);
    ros::ServiceServer get_param_service = node.advertiseService("/get_param", get_param_callback);
    g_param_changes_pub = node.advertise<robot_runtime_flags_msgs::ParamChanges>("/param_changes", 50, false);
    ros::spin();
    return 0;
}

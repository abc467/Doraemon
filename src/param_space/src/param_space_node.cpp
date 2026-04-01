#include "ros/ros.h"
#include "param_space/ParamChanges.h"
#include "param_space/SetParam.h"
#include "param_space/GetParam.h"
#include <unordered_map>
#include <string>

std::unordered_map<std::string, std::string> paramters;

ros::Publisher pub_param_changes;

bool set_param_callback(param_space::SetParam::Request &req,
                        param_space::SetParam::Response &res)
{
    if (req.keys.size() != req.vals.size())
    {
        res.opcode = -1;
        res.msg = "the size of keys and the size of vals is different!";
        return false;
    }

    for (std::size_t i = 0; i < req.keys.size(); i++)
    {
        paramters[req.keys.at(i)] = req.vals.at(i);
    }

    // 通知订阅者，哪些参数被改变了
    param_space::ParamChanges msg;
    msg.changed_params = req.keys;
    pub_param_changes.publish(msg);

    res.opcode = 0;
    res.msg = "successfully";
    return true;
}

bool get_param_callback(param_space::GetParam::Request &req,
                        param_space::GetParam::Response &res)
{
    for (std::size_t i = 0; i < req.keys.size(); i++)
    {
        res.vals.push_back(paramters[req.keys.at(i)]);
    }
    return true;
}

int main(int argc, char *argv[])
{
    ros::init(argc, argv, "param_space_server");
    ros::NodeHandle n;
    ros::ServiceServer service_set_param = n.advertiseService("/set_param", set_param_callback);
    ros::ServiceServer service_get_param = n.advertiseService("/get_param", get_param_callback);
    pub_param_changes = n.advertise<param_space::ParamChanges>("/param_changes", 50, false);
    // ros::Rate rate(10);
    // while (ros::ok())
    // {
    //     ros::spinOnce();
    //     rate.sleep();
    // }
    ros::spin();
    return 0;
}

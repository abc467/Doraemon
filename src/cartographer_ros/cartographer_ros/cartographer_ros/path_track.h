#ifndef PATH_TRACK_H
#define PATH_TRACK_H

#include <thread>
#include <atomic>
#include "cartographer_ros/json.hpp"
#include "cartographer_ros/json_fwd.hpp"

#include "geometry_msgs/PoseStamped.h"
#include "actionlib_msgs/GoalID.h"
#include "nav_msgs/Path.h"

typedef struct _robot_pose
{
    double x;
    double y;
    double z;
    double qw;
    double qx;
    double qy;
    double qz;
} robot_pose;

typedef struct _path_vec3_
{
    double x;
    double y;
    double z;
} path_vec3_t;

typedef struct _path_flag_
{
    unsigned int id;
    path_vec3_t prev_anchor;
    path_vec3_t flag;
    path_vec3_t next_anchor;
    bool backward;
    double acceleration;
    double velocity;
} path_flag;

typedef struct _path_
{
    std::string path_name;
    std::string goal_id;
    bool path2d;
    std::vector<path_flag> flags;
} path_t;

extern robot_pose current_robot_pose;

bool has_task_loaded();
bool start_navigation();
void pause_navigation();
void stop_navigation();
bool is_navigating();
bool is_nav_paused();
bool is_goal_id(const std::string &id);
bool parse_path(const std::string &pathjson);
void start_track(std::function<void(geometry_msgs::PoseStamped &)> publish_pose_stamp_cb);
void publish_track_path(std::function<void(nav_msgs::Path &)> publish_path_cb);

#endif
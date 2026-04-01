#ifndef MOVELIB_H
#define MOVELIB_H

#include <functional>

#include <actionlib/client/simple_action_client.h>
#include <actionlib/client/simple_client_goal_state.h>
#include <actionlib/client/simple_goal_state.h>
#include <move_base_msgs/MoveBaseFeedback.h>
#include <mbf_msgs/ExePathAction.h>
#include <mbf_msgs/ExePathGoal.h>
#include <mbf_msgs/ExePathFeedback.h>

using ExeClient = actionlib::SimpleActionClient<mbf_msgs::ExePathAction>; // exepath action客户端

class movelib
{
private:
    std::shared_ptr<ExeClient> exe_client_ptr_; // exepath action客户端指针
    void exe_done(const actionlib::SimpleClientGoalState &state,
                  const mbf_msgs::ExePathResultConstPtr &result);
    void exe_active();
    // void exe_feedback(const move_base_msgs::MoveBaseFeedbackConstPtr &feedback);
    void exe_feedback(const mbf_msgs::ExePathFeedbackConstPtr &feedback);
    volatile uint32_t current_path_seq;
    volatile uint32_t current_stamp_seq;
    nav_msgs::Path current_path;
    bool send_goal(const uint32_t start_index);

    std::function<void()> done_cb;

    std::string controller_name;
public:
    movelib(/* args */);
    ~movelib();
    void set_controller(const std::string &name);
    void set_done_cb(std::function<void()> cb);
    void set_path(const nav_msgs::Path &path);
    void set_path(const nav_msgs::Path &&path);
    bool start_navigation();
    bool pause_navigation();
    bool resume_navigation();
    bool stop_navigation();
};

#endif
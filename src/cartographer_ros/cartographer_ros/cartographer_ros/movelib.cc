#include "movelib.h"

movelib::movelib(/* args */)
{
    exe_client_ptr_ = std::make_shared<ExeClient>("move_base_flex/exe_path");
}

movelib::~movelib()
{
}

void movelib::set_controller(const std::string &name)
{
    this->controller_name = name;
}

void movelib::exe_active()
{
    ROS_WARN("Start following path");
}

void movelib::exe_feedback(const mbf_msgs::ExePathFeedbackConstPtr &feedback)
{
    this->current_stamp_seq = feedback->current_pose.header.seq;
}

void movelib::exe_done(const actionlib::SimpleClientGoalState &state,
                       const mbf_msgs::ExePathResultConstPtr &result)
{
    if (done_cb)
    {
        done_cb();
    }
}

void movelib::set_done_cb(std::function<void()> cb)
{
    this->done_cb = cb;
}

void movelib::set_path(const nav_msgs::Path &path)
{
    this->current_path = path;
}

void movelib::set_path(const nav_msgs::Path &&path)
{
    this->current_path = std::move(path);
}

bool movelib::send_goal(const uint32_t start_index)
{
    mbf_msgs::ExePathGoal goal;
    goal.path.header = this->current_path.header;
    goal.path.poses.assign(this->current_path.poses.begin() + start_index,
                           this->current_path.poses.end());
    // goal.controller = "lqr_controller";
    // goal.controller = "TebLocalPlannerROS";
    goal.controller = controller_name;
    ROS_WARN("Start navigation");
    exe_client_ptr_->sendGoal(goal,
                              boost::bind(&movelib::exe_done, this, _1, _2),
                              boost::bind(&movelib::exe_active, this),
                              boost::bind(&movelib::exe_feedback, this, _1));
    return true;
}

bool movelib::start_navigation()
{
    this->send_goal(0);
    return true;
}

bool movelib::pause_navigation()
{
    this->stop_navigation();
    return true;
}

bool movelib::resume_navigation()
{
    this->send_goal(this->current_stamp_seq);
    return true;
}

bool movelib::stop_navigation()
{
    if (!exe_client_ptr_->getState().isDone())
    {
        exe_client_ptr_->cancelAllGoals();
    }
    this->current_stamp_seq = 0;
    return true;
}
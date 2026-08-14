/*
 *  Copyright 2018, Sebastian Pütz
 *
 *  Redistribution and use in source and binary forms, with or without
 *  modification, are permitted provided that the following conditions
 *  are met:
 *
 *  1. Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *
 *  2. Redistributions in binary form must reproduce the above
 *     copyright notice, this list of conditions and the following
 *     disclaimer in the documentation and/or other materials provided
 *     with the distribution.
 *
 *  3. Neither the name of the copyright holder nor the names of its
 *     contributors may be used to endorse or promote products derived
 *     from this software without specific prior written permission.
 *
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *  "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *  LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
 *  FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
 *  COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 *  INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 *  BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 *  LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 *  CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 *  LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
 *  ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 *  POSSIBILITY OF SUCH DAMAGE.
 *
 *  abstract_action.h
 *
 *  author: Sebastian Pütz <spuetz@uni-osnabrueck.de>
 *
 */

#ifndef MBF_ABSTRACT_NAV__ABSTRACT_ACTION_BASE_H_
#define MBF_ABSTRACT_NAV__ABSTRACT_ACTION_BASE_H_

#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/lock_guard.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/bind.hpp>

#include <string>
#include <map>
#include <utility>
#include <vector>

#include <actionlib/server/action_server.h>
#include <mbf_utility/robot_information.h>

#include "mbf_abstract_nav/MoveBaseFlexConfig.h"
#include "mbf_abstract_nav/abstract_execution_base.h"

namespace mbf_abstract_nav
{

/**
 * Base class for managing multiple concurrent executions.
 *
 * @tparam Action an actionlib-compatible action
 * @tparam Execution a class implementing the AbstractExecutionBase
 *
 * Place the implementation specific code into AbstractActionBase::runImpl.
 * Also it is required, that you define MyExecution::Ptr as a shared pointer
 * for your execution.
 *
 */
template <typename Action, typename Execution>
class AbstractActionBase
{
 public:
  typedef boost::shared_ptr<AbstractActionBase> Ptr;
  typedef typename actionlib::ActionServer<Action>::GoalHandle GoalHandle;

  /// @brief POD holding info for one execution
  struct ConcurrencySlot{
    ConcurrencySlot() : thread_ptr(NULL), in_use(false){}
    typename Execution::Ptr execution;
    boost::thread* thread_ptr; ///< Owned pointer to a thread
    GoalHandle goal_handle;
    bool in_use;
  };

protected:
  // not part of the public interface
  // todo change to unordered_map
  typedef std::map<uint8_t, ConcurrencySlot> ConcurrencyMap;
public:

  /**
   * @brief Construct a new AbstractActionBase
   *
   * @param name name of the AbstractActionBase
   * @param robot_info robot information
   *
   * @warning Both arguments are stored by ref. You have to ensure, that
   * the lifetime of name and robot_info exceeds the lifetime of this object.
   */
  AbstractActionBase(
      const std::string &name,
      const mbf_utility::RobotInformation &robot_info
  ) : name_(name), robot_info_(robot_info){}

  virtual ~AbstractActionBase()
  {
    // No execution may outlive the derived action object.  In particular, do
    // not hold slot_map_mtx_ while joining: run() takes that same mutex for its
    // final in_use transition.  start_mtx_ prevents a concurrent replacement
    // from racing this shutdown sequence.
    boost::lock_guard<boost::mutex> start_guard(start_mtx_);
    std::vector<boost::thread *> threads_to_join;
    {
      boost::lock_guard<boost::mutex> slot_guard(slot_map_mtx_);
      typename ConcurrencyMap::iterator slot_it = concurrency_slots_.begin();
      for (; slot_it != concurrency_slots_.end(); ++slot_it)
      {
        if (slot_it->second.execution)
          slot_it->second.execution->cancel();
        if (slot_it->second.thread_ptr)
          threads_to_join.push_back(slot_it->second.thread_ptr);
      }
    }

    for (boost::thread *thread : threads_to_join)
    {
      if (thread->joinable())
        thread->join();
    }

    boost::lock_guard<boost::mutex> slot_guard(slot_map_mtx_);
    typename ConcurrencyMap::iterator slot_it = concurrency_slots_.begin();
    for (; slot_it != concurrency_slots_.end(); ++slot_it)
    {
      if (slot_it->second.thread_ptr)
      {
        // Every pointer is joined above.  A joinable thread object is never
        // deleted, including the narrow interval after run() clears in_use.
        threads_.remove_thread(slot_it->second.thread_ptr);
        delete slot_it->second.thread_ptr;
        slot_it->second.thread_ptr = NULL;
      }
      slot_it->second.in_use = false;
    }
  }

  virtual void start(
      GoalHandle &goal_handle,
      typename Execution::Ptr execution_ptr
  )
  {
    uint8_t slot = goal_handle.getGoal()->concurrency_slot;

    if(goal_handle.getGoalStatus().status == actionlib_msgs::GoalStatus::RECALLING)
    {
      goal_handle.setCanceled();
    }
    else
    {
      // Serialize slot replacement, but release slot_map_mtx_ while joining so
      // the retiring run() thread can publish its final in_use=false state.
      boost::lock_guard<boost::mutex> start_guard(start_mtx_);
      boost::thread *old_thread = NULL;
      {
        boost::lock_guard<boost::mutex> slot_guard(slot_map_mtx_);
        typename ConcurrencyMap::iterator slot_it = concurrency_slots_.find(slot);
        if (slot_it != concurrency_slots_.end())
        {
          if (slot_it->second.in_use && slot_it->second.execution)
            slot_it->second.execution->cancel();
          old_thread = slot_it->second.thread_ptr;
        }
      }

      if (old_thread && old_thread->joinable())
        old_thread->join();

      boost::lock_guard<boost::mutex> slot_guard(slot_map_mtx_);
      typename ConcurrencyMap::iterator slot_it = concurrency_slots_.find(slot);
      if(slot_it != concurrency_slots_.end())
      {
        // cleanup previous execution; otherwise we will leak threads
        if (slot_it->second.thread_ptr)
        {
          // old_thread was joined above. start_mtx_ guarantees that no other
          // replacement can swap this pointer between the join and deletion.
          threads_.remove_thread(slot_it->second.thread_ptr);
          delete slot_it->second.thread_ptr;
          slot_it->second.thread_ptr = NULL;
        }
      }

      // Joining a non-cancelable plugin can take time.  The incoming action
      // goal may have entered RECALLING while we waited; never accept it after
      // that cancellation.  The retired slot is already joined and cleaned.
      if (goal_handle.getGoalStatus().status ==
          actionlib_msgs::GoalStatus::RECALLING)
      {
        if (slot_it != concurrency_slots_.end())
          slot_it->second.in_use = false;
        goal_handle.setCanceled();
        return;
      }

      if(slot_it == concurrency_slots_.end())
      {
        // create a new map object in order to avoid costly lookups
        // note: currently unchecked
        slot_it = concurrency_slots_.insert(std::make_pair(slot, ConcurrencySlot())).first;
      }

      // fill concurrency slot with the new goal handle, execution, and working thread
      slot_it->second.in_use = true;
      slot_it->second.goal_handle = goal_handle;
      slot_it->second.goal_handle.setAccepted();
      slot_it->second.execution = execution_ptr;
      slot_it->second.thread_ptr =
        threads_.create_thread(boost::bind(&AbstractActionBase::run, this, boost::ref(concurrency_slots_[slot])));
    }
  }

  virtual void cancel(GoalHandle &goal_handle)
  {
    uint8_t slot = goal_handle.getGoal()->concurrency_slot;

    boost::lock_guard<boost::mutex> guard(slot_map_mtx_);
    typename ConcurrencyMap::iterator slot_it = concurrency_slots_.find(slot);
    if (slot_it != concurrency_slots_.end() &&
        slot_it->second.in_use &&
        slot_it->second.goal_handle == goal_handle)
    {
      slot_it->second.execution->cancel();
    }
    else
    {
      // A delayed cancel callback for a superseded goal must never stop the
      // newer execution which happens to reuse the same concurrency slot.
      ROS_DEBUG_STREAM_NAMED(
          name_, "Ignoring cancel for non-current goal in concurrency slot "
          << static_cast<unsigned int>(slot));
    }
  }

  virtual void runImpl(GoalHandle &goal_handle, Execution& execution) {};

  virtual void run(ConcurrencySlot &slot)
  {
    slot.execution->preRun();
    runImpl(slot.goal_handle, *slot.execution);
    ROS_DEBUG_STREAM_NAMED(name_, "Finished action \"" << name_ << "\" run method, waiting for execution thread to finish.");
    slot.execution->join();
    ROS_DEBUG_STREAM_NAMED(name_, "Execution completed with goal status "
                           << (int)slot.goal_handle.getGoalStatus().status << ": "<< slot.goal_handle.getGoalStatus().text);
    slot.execution->postRun();
    // All reads and writes of in_use/thread_ptr are synchronized by the same
    // map mutex.  The thread object is deleted only after start()/destructor
    // has joined this function completely.
    boost::lock_guard<boost::mutex> slot_guard(slot_map_mtx_);
    slot.in_use = false;
  }

  virtual void reconfigureAll(
      mbf_abstract_nav::MoveBaseFlexConfig &config, uint32_t level)
  {
    boost::lock_guard<boost::mutex> guard(slot_map_mtx_);

    typename ConcurrencyMap::iterator iter;
    for(iter = concurrency_slots_.begin(); iter != concurrency_slots_.end(); ++iter)
    {
      iter->second.execution->reconfigure(config);
    }
  }

  virtual void cancelAll()
  {
    ROS_INFO_STREAM_NAMED(name_, "Cancel all goals for \"" << name_ << "\".");
    boost::lock_guard<boost::mutex> start_guard(start_mtx_);
    std::vector<boost::thread *> threads_to_join;
    {
      boost::lock_guard<boost::mutex> slot_guard(slot_map_mtx_);
      typename ConcurrencyMap::iterator iter;
      for(iter = concurrency_slots_.begin(); iter != concurrency_slots_.end(); ++iter)
      {
        if (iter->second.execution)
          iter->second.execution->cancel();
        if (iter->second.thread_ptr)
          threads_to_join.push_back(iter->second.thread_ptr);
      }
    }
    // Never join with slot_map_mtx_ held; run() needs it to finish.
    for (boost::thread *thread : threads_to_join)
    {
      if (thread->joinable())
        thread->join();
    }
  }

protected:
  const std::string &name_;
  const mbf_utility::RobotInformation &robot_info_;

  boost::thread_group threads_;
  ConcurrencyMap concurrency_slots_;

  //! Serializes replacement/cleanup while slot_map_mtx_ is released for join.
  boost::mutex start_mtx_;

  //! Sole mutex protecting ConcurrencySlot::in_use and thread_ptr.
  boost::mutex slot_map_mtx_;

};

}

#endif /* MBF_ABSTRACT_NAV__ABSTRACT_ACTION_BASE_H_ */

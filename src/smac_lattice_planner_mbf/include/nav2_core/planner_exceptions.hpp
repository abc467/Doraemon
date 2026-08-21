// ROS 1 compatibility adapter for the exceptions used by the Nav2 Smac core.
#pragma once

#include <stdexcept>
#include <string>

namespace nav2_core
{
class PlannerException : public std::runtime_error
{
public:
  explicit PlannerException(const std::string & what) : std::runtime_error(what) {}
};

class GoalOccupied : public PlannerException
{
public:
  using PlannerException::PlannerException;
};

class PlannerCancelled : public PlannerException
{
public:
  using PlannerException::PlannerException;
};
}  // namespace nav2_core

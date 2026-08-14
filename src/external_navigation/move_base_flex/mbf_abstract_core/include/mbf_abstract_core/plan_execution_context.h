#ifndef MBF_ABSTRACT_CORE__PLAN_EXECUTION_CONTEXT_H_
#define MBF_ABSTRACT_CORE__PLAN_EXECUTION_CONTEXT_H_

#include <cstdint>

#include <geometry_msgs/PoseStamped.h>

namespace mbf_abstract_core
{

/**
 * Describes who owns an incoming controller plan.
 *
 * A continuous update belongs to the currently running controller execution
 * and must preserve its physical/safety state. A fresh execution is a new
 * navigation attempt and must not inherit a failure latch or optimizer state
 * from the previous attempt.
 */
struct PlanExecutionContext
{
  enum class UpdateMode : std::uint8_t
  {
    FreshExecution = 0u,
    ContinuousUpdate = 1u,
  };

  std::uint64_t execution_epoch{0u};
  UpdateMode update_mode{UpdateMode::FreshExecution};

  // The global planner may legally finish at a tolerance pose which is not
  // exactly the requested navigation target.  Preserve that original target
  // through ExePath so controller success can be checked against the request,
  // rather than silently redefining success as reaching plan.back().
  bool has_requested_target{false};
  geometry_msgs::PoseStamped requested_target;
};

/** Optional extension implemented by lifecycle-aware nav_core plugins. */
class PlanExecutionContextAware
{
public:
  virtual ~PlanExecutionContextAware() = default;
  virtual void setPlanExecutionContext(
      const PlanExecutionContext & context) = 0;
};

/** Optional status extension for nav_core's otherwise boolean failure API. */
class ControllerExecutionStatusAware
{
public:
  virtual ~ControllerExecutionStatusAware() = default;
  virtual bool hasLatchedSafetyFailure() const = 0;
};

/** Optional extension for nav_core plugins that can honor MBF goal tolerances. */
class ControllerGoalToleranceAware
{
public:
  virtual ~ControllerGoalToleranceAware() = default;
  virtual bool isGoalReachedWithTolerances(
      double distance_tolerance, double angle_tolerance) = 0;
};

}  // namespace mbf_abstract_core

#endif  // MBF_ABSTRACT_CORE__PLAN_EXECUTION_CONTEXT_H_

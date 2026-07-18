#include "cartographer/mapping/internal/2d/flirt_relocation.h"

#include "cartographer/transform/transform.h"

namespace cartographer {
namespace mapping {

transform::Rigid2d ComputeFlirtQueryPoseInTrajectory(
    const TrajectoryNode::Data& reference_data,
    const transform::Rigid2d& reference_from_query) {
  const transform::Rigid2d trajectory_from_reference = transform::Project2D(
      reference_data.local_pose * transform::Rigid3d::Rotation(
                                      reference_data.gravity_alignment.inverse()));
  return trajectory_from_reference * reference_from_query;
}

}  // namespace mapping
}  // namespace cartographer

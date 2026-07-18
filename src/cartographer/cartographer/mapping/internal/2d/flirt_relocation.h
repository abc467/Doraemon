#ifndef CARTOGRAPHER_MAPPING_INTERNAL_2D_FLIRT_RELOCATION_H_
#define CARTOGRAPHER_MAPPING_INTERNAL_2D_FLIRT_RELOCATION_H_

#include "cartographer/mapping/trajectory_node.h"
#include "cartographer/transform/rigid_transform.h"

namespace cartographer {
namespace mapping {

// Converts FLIRT's T_reference<-query result into the frozen trajectory-local
// frame expected by the 2D scan matchers. Global optimized poses deliberately
// are not an input: node features live in their immutable gravity-aligned
// local frame.
transform::Rigid2d ComputeFlirtQueryPoseInTrajectory(
    const TrajectoryNode::Data& reference_data,
    const transform::Rigid2d& reference_from_query);

}  // namespace mapping
}  // namespace cartographer

#endif  // CARTOGRAPHER_MAPPING_INTERNAL_2D_FLIRT_RELOCATION_H_

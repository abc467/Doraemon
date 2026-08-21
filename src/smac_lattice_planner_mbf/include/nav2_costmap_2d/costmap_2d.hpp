// ROS 1 compatibility adapter for the Nav2 Smac search core.
// The search algorithms use only the Costmap2D data API, which is shared by
// the ROS 1 navigation costmap implementation.
#pragma once

#include <costmap_2d/costmap_2d.h>

namespace nav2_costmap_2d
{
using Costmap2D = costmap_2d::Costmap2D;
}  // namespace nav2_costmap_2d

#include "theta_star_planner/se2_path_refiner.h"

#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <limits>
#include <queue>
#include <unordered_map>
#include <utility>

#include <base_local_planner/costmap_model.h>
#include <base_local_planner/footprint_helper.h>
#include <costmap_2d/cost_values.h>
#include <tf/transform_datatypes.h>
#include <tf2/utils.h>

namespace theta_star
{
namespace
{

constexpr double kTwoPi = 2.0 * M_PI;

double normalizeAngle(double angle)
{
  return std::remainder(angle, kTwoPi);
}

double poseDistance(
  const geometry_msgs::PoseStamped & first,
  const geometry_msgs::PoseStamped & second)
{
  return std::hypot(
    second.pose.position.x - first.pose.position.x,
    second.pose.position.y - first.pose.position.y);
}

geometry_msgs::PoseStamped interpolatePose(
  const geometry_msgs::PoseStamped & start,
  const geometry_msgs::PoseStamped & end,
  double ratio)
{
  geometry_msgs::PoseStamped sample = start;
  sample.pose.position.x +=
    ratio * (end.pose.position.x - start.pose.position.x);
  sample.pose.position.y +=
    ratio * (end.pose.position.y - start.pose.position.y);
  sample.pose.position.z +=
    ratio * (end.pose.position.z - start.pose.position.z);
  const double start_yaw = tf2::getYaw(start.pose.orientation);
  const double yaw_delta = normalizeAngle(
    tf2::getYaw(end.pose.orientation) - start_yaw);
  sample.pose.orientation = tf::createQuaternionMsgFromYaw(
    start_yaw + ratio * yaw_delta);
  return sample;
}

double footprintRadius(const std::vector<geometry_msgs::Point> & footprint)
{
  double radius = 0.0;
  for (const auto & point : footprint) {
    radius = std::max(radius, std::hypot(point.x, point.y));
  }
  return radius;
}

unsigned char centreCost(
  const costmap_2d::Costmap2D & costmap,
  const geometry_msgs::PoseStamped & pose)
{
  unsigned int mx = 0;
  unsigned int my = 0;
  if (!costmap.worldToMap(
      pose.pose.position.x, pose.pose.position.y, mx, my))
  {
    return costmap_2d::NO_INFORMATION;
  }
  return costmap.getCost(mx, my);
}

bool centreTransitionAllowed(
  const costmap_2d::Costmap2D & costmap,
  const geometry_msgs::PoseStamped & start,
  const geometry_msgs::PoseStamped & end,
  const SE2RefinerConfig & config)
{
  const unsigned char start_cost = centreCost(costmap, start);
  const unsigned char end_cost = centreCost(costmap, end);
  const auto traversable = [&](unsigned char cost) {
      if (cost == costmap_2d::NO_INFORMATION) {
        return config.allow_unknown;
      }
      return cost < costmap_2d::INSCRIBED_INFLATED_OBSTACLE;
    };
  if (!traversable(start_cost) || !traversable(end_cost)) {
    return false;
  }
  if (static_cast<int>(end_cost) <= config.max_allowed_center_cost) {
    return true;
  }
  // A coverage path may leave the robot inside the stricter Connect
  // centre-line band.  It may leave that band only through non-increasing
  // soft cost; it may never move deeper into it.
  return static_cast<int>(start_cost) > config.max_allowed_center_cost &&
    end_cost <= start_cost;
}

struct SearchBounds
{
  unsigned int min_x{0};
  unsigned int max_x{0};
  unsigned int min_y{0};
  unsigned int max_y{0};
};

struct SearchNode
{
  geometry_msgs::PoseStamped pose;
  unsigned int mx{0};
  unsigned int my{0};
  int yaw_bin{0};
  double g{std::numeric_limits<double>::infinity()};
  double f{std::numeric_limits<double>::infinity()};
  int parent{-1};
};

struct QueueEntry
{
  double f{0.0};
  int node{-1};
  bool operator<(const QueueEntry & other) const
  {
    return f > other.f;
  }
};

std::uint64_t stateKey(
  unsigned int mx, unsigned int my, int yaw_bin,
  unsigned int map_width, int yaw_bins)
{
  return (static_cast<std::uint64_t>(my) * map_width + mx) *
    static_cast<std::uint64_t>(yaw_bins) +
    static_cast<std::uint64_t>(yaw_bin);
}

int yawToBin(double yaw, int bins)
{
  const double positive = std::fmod(
    std::fmod(yaw, kTwoPi) + kTwoPi, kTwoPi);
  return static_cast<int>(std::llround(
    positive * static_cast<double>(bins) / kTwoPi)) % bins;
}

double binToYaw(int bin, int bins)
{
  return normalizeAngle(
    kTwoPi * static_cast<double>(bin) / static_cast<double>(bins));
}

SearchBounds makeBounds(
  const costmap_2d::Costmap2D & costmap,
  const std::vector<geometry_msgs::PoseStamped> & path,
  std::size_t begin, std::size_t end, double margin)
{
  double min_x = std::numeric_limits<double>::infinity();
  double min_y = std::numeric_limits<double>::infinity();
  double max_x = -std::numeric_limits<double>::infinity();
  double max_y = -std::numeric_limits<double>::infinity();
  for (std::size_t index = begin; index <= end; ++index) {
    min_x = std::min(min_x, path[index].pose.position.x);
    min_y = std::min(min_y, path[index].pose.position.y);
    max_x = std::max(max_x, path[index].pose.position.x);
    max_y = std::max(max_y, path[index].pose.position.y);
  }
  const double map_min_x = costmap.getOriginX();
  const double map_min_y = costmap.getOriginY();
  const double resolution = costmap.getResolution();
  auto clamp_x = [&](double world) {
      const int cell = static_cast<int>(std::floor(
        (world - map_min_x) / resolution));
      return static_cast<unsigned int>(std::clamp(
        cell, 0, static_cast<int>(costmap.getSizeInCellsX()) - 1));
    };
  auto clamp_y = [&](double world) {
      const int cell = static_cast<int>(std::floor(
        (world - map_min_y) / resolution));
      return static_cast<unsigned int>(std::clamp(
        cell, 0, static_cast<int>(costmap.getSizeInCellsY()) - 1));
    };
  return {
    clamp_x(min_x - margin), clamp_x(max_x + margin),
    clamp_y(min_y - margin), clamp_y(max_y + margin)};
}

std::optional<std::vector<geometry_msgs::PoseStamped>> localSearch(
  const costmap_2d::Costmap2D & costmap,
  const std::vector<geometry_msgs::Point> & footprint,
  const geometry_msgs::PoseStamped & start,
  const geometry_msgs::PoseStamped & goal,
  const SearchBounds & bounds,
  const SE2RefinerConfig & config,
  const std::atomic<bool> * cancel_requested,
  int & total_expansions)
{
  if (!SE2PathRefiner::isPoseSafe(
      costmap, footprint, start, config.allow_unknown) ||
    !SE2PathRefiner::isPoseSafe(
      costmap, footprint, goal, config.allow_unknown))
  {
    return std::nullopt;
  }

  unsigned int start_mx = 0;
  unsigned int start_my = 0;
  if (!costmap.worldToMap(
      start.pose.position.x, start.pose.position.y, start_mx, start_my))
  {
    return std::nullopt;
  }

  const int yaw_bins = std::max(8, config.yaw_bins);
  const double yaw_step = kTwoPi / static_cast<double>(yaw_bins);
  const double motion_step = std::max(
    1.5 * costmap.getResolution(), config.motion_step);
  // Keep one difficult narrow window from consuming the whole repair budget;
  // later, wider windows must still get a chance.  The configured value is a
  // global budget across every adaptive attempt.
  const int remaining_expansions = std::max(
    0, config.max_expansions - total_expansions);
  const int maximum_expansions = std::min(60000, remaining_expansions);
  if (maximum_expansions <= 0) {
    return std::nullopt;
  }

  std::vector<SearchNode> nodes;
  nodes.reserve(std::min(maximum_expansions, 100000));
  std::unordered_map<std::uint64_t, int> best;
  best.reserve(std::min(maximum_expansions, 100000));
  std::priority_queue<QueueEntry> queue;

  auto heuristic = [&](const geometry_msgs::PoseStamped & pose) {
      const double distance = poseDistance(pose, goal);
      const double yaw_error = std::fabs(normalizeAngle(
        tf2::getYaw(goal.pose.orientation) -
        tf2::getYaw(pose.pose.orientation)));
      return distance + 0.20 * yaw_error;
    };

  SearchNode initial;
  initial.pose = start;
  initial.mx = start_mx;
  initial.my = start_my;
  initial.yaw_bin = yawToBin(tf2::getYaw(start.pose.orientation), yaw_bins);
  initial.g = 0.0;
  // Weighted A* is intentional here: Theta* has already supplied the global
  // topology, so this bounded search values finding a safe local repair within
  // MBF's planning deadline over proving a second globally optimal path.
  constexpr double kHeuristicWeight = 1.5;
  initial.f = kHeuristicWeight * heuristic(start);
  nodes.push_back(initial);
  best[stateKey(
    start_mx, start_my, initial.yaw_bin,
    costmap.getSizeInCellsX(), yaw_bins)] = 0;
  queue.push({initial.f, 0});

  int goal_node = -1;
  int local_expansions = 0;
  while (!queue.empty() && local_expansions < maximum_expansions) {
    if (cancel_requested != nullptr && cancel_requested->load()) {
      return std::nullopt;
    }
    const QueueEntry entry = queue.top();
    queue.pop();
    if (entry.node < 0 || entry.node >= static_cast<int>(nodes.size()) ||
      entry.f > nodes[entry.node].f + 1e-9)
    {
      continue;
    }
    const SearchNode current = nodes[entry.node];
    ++local_expansions;

    const double goal_distance = poseDistance(current.pose, goal);
    const double goal_yaw_error = std::fabs(normalizeAngle(
      tf2::getYaw(goal.pose.orientation) -
      tf2::getYaw(current.pose.pose.orientation)));
    if (goal_distance <= std::max(
        config.goal_position_tolerance, 1.5 * motion_step) &&
      goal_yaw_error <= std::max(config.goal_yaw_tolerance, 1.5 * yaw_step) &&
      centreTransitionAllowed(costmap, current.pose, goal, config) &&
      SE2PathRefiner::isSweepSafe(
        costmap, footprint, current.pose, goal,
        config.collision_check_step, config.allow_unknown))
    {
      SearchNode terminal;
      terminal.pose = goal;
      terminal.g = current.g + goal_distance + 0.20 * goal_yaw_error;
      terminal.f = terminal.g;
      terminal.parent = entry.node;
      nodes.push_back(terminal);
      goal_node = static_cast<int>(nodes.size()) - 1;
      break;
    }

    struct Motion
    {
      int turn_bins;
      int direction;
      bool rotate;
    };
    static constexpr std::array<Motion, 8> motions{{
      {-1, 1, false}, {0, 1, false}, {1, 1, false},
      {-1, -1, false}, {0, -1, false}, {1, -1, false},
      {-1, 0, true}, {1, 0, true}}};

    for (const auto & motion : motions) {
      if (motion.direction < 0 && !config.allow_reverse) {
        continue;
      }
      geometry_msgs::PoseStamped next = current.pose;
      const double current_yaw = tf2::getYaw(current.pose.pose.orientation);
      const double yaw_delta = motion.turn_bins * yaw_step;
      const double next_yaw = normalizeAngle(current_yaw + yaw_delta);
      if (!motion.rotate) {
        const double signed_distance = motion.direction * motion_step;
        if (std::fabs(yaw_delta) <= 1e-12) {
          next.pose.position.x += signed_distance * std::cos(current_yaw);
          next.pose.position.y += signed_distance * std::sin(current_yaw);
        } else {
          const double radius = signed_distance / yaw_delta;
          next.pose.position.x += radius *
            (std::sin(next_yaw) - std::sin(current_yaw));
          next.pose.position.y -= radius *
            (std::cos(next_yaw) - std::cos(current_yaw));
        }
      }
      next.pose.orientation = tf::createQuaternionMsgFromYaw(next_yaw);

      unsigned int next_mx = 0;
      unsigned int next_my = 0;
      if (!costmap.worldToMap(
          next.pose.position.x, next.pose.position.y, next_mx, next_my) ||
        next_mx < bounds.min_x || next_mx > bounds.max_x ||
        next_my < bounds.min_y || next_my > bounds.max_y)
      {
        continue;
      }
      const int next_yaw_bin = yawToBin(next_yaw, yaw_bins);
      if (next_mx == current.mx && next_my == current.my &&
        next_yaw_bin == current.yaw_bin)
      {
        continue;
      }
      if (!centreTransitionAllowed(costmap, current.pose, next, config) ||
        !SE2PathRefiner::isSweepSafe(
          costmap, footprint, current.pose, next,
          config.collision_check_step, config.allow_unknown))
      {
        continue;
      }

      const unsigned char raw_cost = centreCost(costmap, next);
      const double translation_cost = motion.rotate ? 0.0 : motion_step;
      const double reverse_factor = motion.direction < 0 ? 1.20 : 1.0;
      const double turn_cost = motion.rotate ?
        0.25 * std::fabs(yaw_delta) : 0.05 * std::fabs(yaw_delta);
      const double soft_cost = raw_cost == costmap_2d::NO_INFORMATION ?
        0.25 : 0.002 * static_cast<double>(raw_cost);
      const double new_g = current.g +
        reverse_factor * translation_cost + turn_cost + soft_cost;
      const std::uint64_t key = stateKey(
        next_mx, next_my, next_yaw_bin,
        costmap.getSizeInCellsX(), yaw_bins);
      const auto found = best.find(key);
      if (found != best.end() && nodes[found->second].g <= new_g + 1e-9) {
        continue;
      }

      SearchNode node;
      node.pose = next;
      node.mx = next_mx;
      node.my = next_my;
      node.yaw_bin = next_yaw_bin;
      node.g = new_g;
      node.f = new_g + kHeuristicWeight * heuristic(next);
      node.parent = entry.node;
      nodes.push_back(node);
      const int index = static_cast<int>(nodes.size()) - 1;
      best[key] = index;
      queue.push({node.f, index});
    }
  }

  total_expansions += local_expansions;
  if (goal_node < 0) {
    return std::nullopt;
  }

  std::vector<geometry_msgs::PoseStamped> reversed;
  for (int node = goal_node; node >= 0; node = nodes[node].parent) {
    reversed.push_back(nodes[node].pose);
  }
  std::reverse(reversed.begin(), reversed.end());

  std::vector<geometry_msgs::PoseStamped> dense;
  dense.push_back(reversed.front());
  const double radius = footprintRadius(footprint);
  for (std::size_t index = 1; index < reversed.size(); ++index) {
    const double translation = poseDistance(reversed[index - 1], reversed[index]);
    const double rotation = std::fabs(normalizeAngle(
      tf2::getYaw(reversed[index].pose.orientation) -
      tf2::getYaw(reversed[index - 1].pose.orientation)));
    const double corner_motion = translation + radius * rotation;
    const int samples = std::max(1, static_cast<int>(std::ceil(
      corner_motion / std::max(1e-3, config.collision_check_step))));
    for (int sample = 1; sample <= samples; ++sample) {
      dense.push_back(interpolatePose(
        reversed[index - 1], reversed[index],
        static_cast<double>(sample) / static_cast<double>(samples)));
    }
  }
  return dense;
}

std::size_t moveBackwardByDistance(
  const std::vector<geometry_msgs::PoseStamped> & path,
  std::size_t index, double distance)
{
  double accumulated = 0.0;
  while (index > 0 && accumulated < distance) {
    accumulated += poseDistance(path[index - 1], path[index]);
    --index;
  }
  return index;
}

std::size_t moveForwardByDistance(
  const std::vector<geometry_msgs::PoseStamped> & path,
  std::size_t index, double distance)
{
  double accumulated = 0.0;
  while (index + 1 < path.size() && accumulated < distance) {
    accumulated += poseDistance(path[index], path[index + 1]);
    ++index;
  }
  return index;
}

}  // namespace

bool SE2PathRefiner::isPoseSafe(
  const costmap_2d::Costmap2D & costmap,
  const std::vector<geometry_msgs::Point> & footprint,
  const geometry_msgs::PoseStamped & pose,
  bool allow_unknown)
{
  if (footprint.size() < 3) {
    return false;
  }
  const double yaw = tf2::getYaw(pose.pose.orientation);
  if (!std::isfinite(yaw)) {
    return false;
  }
  base_local_planner::CostmapModel model(costmap);
  const double outline_cost = model.footprintCost(
    pose.pose.position.x, pose.pose.position.y, yaw, footprint);
  if (!std::isfinite(outline_cost) || outline_cost < 0.0) {
    if (!(allow_unknown && outline_cost == -2.0)) {
      return false;
    }
  }

  base_local_planner::FootprintHelper helper;
  const auto cells = helper.getFootprintCells(
    Eigen::Vector3f(
      static_cast<float>(pose.pose.position.x),
      static_cast<float>(pose.pose.position.y),
      static_cast<float>(yaw)),
    footprint, costmap, true);
  if (cells.empty()) {
    return false;
  }
  for (const auto & cell : cells) {
    if (cell.x < 0 || cell.y < 0 ||
      static_cast<unsigned int>(cell.x) >= costmap.getSizeInCellsX() ||
      static_cast<unsigned int>(cell.y) >= costmap.getSizeInCellsY())
    {
      return false;
    }
    const unsigned char cost = costmap.getCost(
      static_cast<unsigned int>(cell.x),
      static_cast<unsigned int>(cell.y));
    if (cost == costmap_2d::LETHAL_OBSTACLE ||
      (cost == costmap_2d::NO_INFORMATION && !allow_unknown))
    {
      return false;
    }
  }
  return true;
}

bool SE2PathRefiner::isSweepSafe(
  const costmap_2d::Costmap2D & costmap,
  const std::vector<geometry_msgs::Point> & footprint,
  const geometry_msgs::PoseStamped & start,
  const geometry_msgs::PoseStamped & end,
  double sample_step,
  bool allow_unknown)
{
  const double translation = poseDistance(start, end);
  const double yaw_delta = std::fabs(normalizeAngle(
    tf2::getYaw(end.pose.orientation) -
    tf2::getYaw(start.pose.orientation)));
  const double corner_motion = translation + footprintRadius(footprint) * yaw_delta;
  const double bounded_step = std::max(
    1e-3, std::min(std::fabs(sample_step), 0.5 * costmap.getResolution()));
  const int samples = std::max(
    1, static_cast<int>(std::ceil(corner_motion / bounded_step)));
  for (int sample = 0; sample <= samples; ++sample) {
    if (!isPoseSafe(
        costmap, footprint,
        interpolatePose(start, end,
          static_cast<double>(sample) / static_cast<double>(samples)),
        allow_unknown))
    {
      return false;
    }
  }
  return true;
}

std::optional<std::size_t> SE2PathRefiner::firstUnsafeSegment(
  const costmap_2d::Costmap2D & costmap,
  const std::vector<geometry_msgs::Point> & footprint,
  const std::vector<geometry_msgs::PoseStamped> & path,
  double sample_step,
  bool allow_unknown)
{
  if (path.empty() || !isPoseSafe(
      costmap, footprint, path.front(), allow_unknown))
  {
    return 0u;
  }
  for (std::size_t index = 1; index < path.size(); ++index) {
    if (!isSweepSafe(
        costmap, footprint, path[index - 1], path[index],
        sample_step, allow_unknown))
    {
      return index - 1;
    }
  }
  return std::nullopt;
}

SE2RefinementResult SE2PathRefiner::refine(
  const costmap_2d::Costmap2D & costmap,
  const std::vector<geometry_msgs::Point> & footprint,
  const std::vector<geometry_msgs::PoseStamped> & topology_path,
  const SE2RefinerConfig & config,
  const std::atomic<bool> * cancel_requested)
{
  SE2RefinementResult result;
  if (!config.enabled) {
    result.success = true;
    result.path = topology_path;
    result.message = "SE2 refinement disabled";
    return result;
  }
  if (topology_path.size() < 2 || footprint.size() < 3) {
    result.message = "SE2 refinement requires a path and polygon footprint";
    return result;
  }

  result.path = topology_path;
  for (int repair = 0; repair <= std::max(0, config.max_repairs); ++repair) {
    const auto unsafe = firstUnsafeSegment(
      costmap, footprint, result.path,
      config.collision_check_step, config.allow_unknown);
    if (!unsafe.has_value()) {
      result.success = true;
      result.repairs = repair;
      result.message = repair == 0 ?
        "topology path already footprint-safe" :
        "unsafe path sections repaired in SE2";
      return result;
    }
    if (repair == std::max(0, config.max_repairs)) {
      break;
    }

    bool repaired = false;
    for (const double window : config.repair_window_lengths) {
      const std::size_t begin = moveBackwardByDistance(
        result.path, unsafe.value(), std::max(0.5, window));
      const std::size_t end = moveForwardByDistance(
        result.path, std::min(unsafe.value() + 1, result.path.size() - 1),
        std::max(0.5, window));
      if (begin >= end) {
        continue;
      }
      for (const double corridor : config.corridor_widths) {
        const SearchBounds bounds = makeBounds(
          costmap, result.path, begin, end,
          std::max(0.25, corridor));
        const auto local = localSearch(
          costmap, footprint, result.path[begin], result.path[end],
          bounds, config, cancel_requested, result.expansions);
        if (!local.has_value() || local->size() < 2) {
          continue;
        }

        std::vector<geometry_msgs::PoseStamped> candidate;
        candidate.reserve(
          begin + local->size() + result.path.size() - end);
        candidate.insert(candidate.end(), result.path.begin(), result.path.begin() + begin);
        candidate.insert(candidate.end(), local->begin(), local->end());
        candidate.insert(candidate.end(), result.path.begin() + end + 1, result.path.end());

        const auto next_unsafe = firstUnsafeSegment(
          costmap, footprint, candidate,
          config.collision_check_step, config.allow_unknown);
        if (next_unsafe.has_value() &&
          next_unsafe.value() <= begin + local->size() - 1)
        {
          continue;
        }
        result.path = std::move(candidate);
        repaired = true;
        break;
      }
      if (repaired) {
        break;
      }
    }
    if (!repaired) {
      result.message = "no footprint-safe SE2 repair found in adaptive windows";
      return result;
    }
  }
  result.message = "SE2 repair budget exhausted";
  return result;
}

}  // namespace theta_star

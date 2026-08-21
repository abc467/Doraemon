// Copyright 2026 Clean Robot Navigation Team
// Licensed under the Apache License, Version 2.0.

#ifndef SMAC_LATTICE_PLANNER_MBF__LIVE_VALIDATOR_SUPPORT_HPP_
#define SMAC_LATTICE_PLANNER_MBF__LIVE_VALIDATOR_SUPPORT_HPP_

#include <cstddef>
#include <stdexcept>
#include <string>
#include <vector>

namespace smac_lattice_planner_mbf
{
namespace live_validator
{

struct ConnectionIds
{
  int from{-1};
  int to{-1};
};

struct ConnectionSelection
{
  std::string mode;
  std::vector<ConnectionIds> pairs;
};

// Explicit mode validates one arbitrary persisted block endpoint pair and does
// not require the pair to be adjacent in exec_order_json.  The legacy only_*
// parameters remain filters over adjacent execution-order connections.
inline ConnectionSelection selectConnections(
  const std::vector<int> & execution_order,
  int explicit_from_block,
  int explicit_to_block,
  int only_from_block,
  int only_to_block)
{
  const bool has_explicit_from = explicit_from_block >= 0;
  const bool has_explicit_to = explicit_to_block >= 0;
  if (has_explicit_from != has_explicit_to) {
    throw std::invalid_argument(
            "explicit_from_block and explicit_to_block must be specified together");
  }
  if ((has_explicit_from || has_explicit_to) &&
    (only_from_block >= 0 || only_to_block >= 0))
  {
    throw std::invalid_argument(
            "explicit block pair cannot be combined with only_from_block/only_to_block filters");
  }

  ConnectionSelection selection;
  if (has_explicit_from) {
    selection.mode = "explicit";
    selection.pairs.push_back(ConnectionIds{explicit_from_block, explicit_to_block});
    return selection;
  }

  if (execution_order.size() < 2u) {
    throw std::invalid_argument("plan has fewer than two execution-order blocks");
  }
  selection.mode =
    (only_from_block >= 0 || only_to_block >= 0) ? "ordered_filtered" : "ordered_all";
  for (std::size_t index = 1; index < execution_order.size(); ++index) {
    const ConnectionIds pair{execution_order[index - 1u], execution_order[index]};
    if ((only_from_block >= 0 && pair.from != only_from_block) ||
      (only_to_block >= 0 && pair.to != only_to_block))
    {
      continue;
    }
    selection.pairs.push_back(pair);
  }

  if ((only_from_block >= 0 || only_to_block >= 0) && selection.pairs.empty()) {
    throw std::invalid_argument(
            "requested execution-order filter matched zero connections");
  }
  return selection;
}

inline std::string classifySearchTermination(
  bool search_success,
  bool continuously_safe,
  int iterations,
  int max_iterations,
  double search_seconds,
  double max_planning_time)
{
  if (search_success) {
    return continuously_safe ? "success" : "continuous_validation_failed";
  }
  if (iterations >= max_iterations) {
    return "max_iterations";
  }
  // The existing AStar boolean API does not expose whether the queue became
  // empty in the same terminal interval in which the deadline elapsed.  Keep
  // this label explicit rather than claiming a more precise cause.
  if (search_seconds >= max_planning_time) {
    return "max_planning_time_or_late_exhaustion";
  }
  return "search_exhausted";
}

}  // namespace live_validator
}  // namespace smac_lattice_planner_mbf

#endif  // SMAC_LATTICE_PLANNER_MBF__LIVE_VALIDATOR_SUPPORT_HPP_

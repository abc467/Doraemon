#include "theta_star_planner/theta_star.h"

namespace theta_star
{
ThetaStar::ThetaStar()
: w_traversal_cost_(1.0),
  w_euc_cost_(2.0),
  w_heuristic_cost_(1.0),
  how_many_corners_(8),
  allow_unknown_(true),
  max_allowed_cost_(LETHAL_COST - 1),
  size_x_(0),
  size_y_(0),
  terminal_checking_interval_(5000),
  index_generated_(0)
{
  exp_node = new tree_node;
}

void ThetaStar::setStartAndGoal(
  const geometry_msgs::PoseStamped & start,
  const geometry_msgs::PoseStamped & goal)
{
  unsigned int s[2], d[2];
  costmap_->worldToMap(start.pose.position.x, start.pose.position.y, s[0], s[1]);
  costmap_->worldToMap(goal.pose.position.x, goal.pose.position.y, d[0], d[1]);

  src_ = {static_cast<int>(s[0]), static_cast<int>(s[1])};
  dst_ = {static_cast<int>(d[0]), static_cast<int>(d[1])};
}

bool ThetaStar::generatePath(std::vector<coordsW> & raw_path)
{
  resetContainers();
  addToNodesData(index_generated_);
  double src_g_cost = getTraversalCost(src_.x, src_.y), src_h_cost = getHCost(src_.x, src_.y);
  nodes_data_[index_generated_] =
  {src_.x, src_.y, src_g_cost, src_h_cost, &nodes_data_[index_generated_], true,
    src_g_cost + src_h_cost};
  queue_.push({&nodes_data_[index_generated_]});
  addIndex(src_.x, src_.y, &nodes_data_[index_generated_]);
  tree_node * curr_data = &nodes_data_[index_generated_];
  index_generated_++;
  nodes_opened = 0;

  while (!queue_.empty()) {
    nodes_opened++;

    if (terminal_checking_interval_ > 0 &&
        nodes_opened % terminal_checking_interval_ == 0 &&
        cancel_checker_ && cancel_checker_())
    {
      raw_path.clear();
      clearQueue();
      return false;
    }

    if (isGoal(*curr_data)) {
      break;
    }

    resetParent(curr_data);
    setNeighbors(curr_data);

    if (queue_.empty()) {
      break;
    }

    curr_data = queue_.top();
    queue_.pop();
  }

  // The goal may have been the last queued node. In that case popping it
  // makes queue_ empty before the loop condition can observe isGoal() again;
  // success is determined by curr_data, not by whether unrelated work remains.
  if (!isGoal(*curr_data)) {
    raw_path.clear();
    return false;
  }

  backtrace(raw_path, curr_data);
  clearQueue();

  return true;
}

void ThetaStar::resetParent(tree_node * curr_data)
{
  double g_cost, los_cost = 0;
  curr_data->is_in_queue = false;
  const tree_node * curr_par = curr_data->parent_id;
  const tree_node * maybe_par = curr_par->parent_id;

  // Line-of-sight shortcuts use the normal clearance threshold.  Keep the
  // short monotonic start-escape chain explicit until it reaches that band.
  if (!isSafe(curr_data->x, curr_data->y) ||
      !isSafe(curr_par->x, curr_par->y) ||
      !isSafe(maybe_par->x, maybe_par->y)) {
    return;
  }

  if (losCheck(curr_data->x, curr_data->y, maybe_par->x, maybe_par->y, los_cost)) {
    g_cost = maybe_par->g +
      getEuclideanCost(curr_data->x, curr_data->y, maybe_par->x, maybe_par->y) + los_cost;

    if (g_cost < curr_data->g) {
      curr_data->parent_id = maybe_par;
      curr_data->g = g_cost;
      curr_data->f = g_cost + curr_data->h;
    }
  }
}

void ThetaStar::setNeighbors(const tree_node * curr_data)
{
  int mx, my;
  tree_node * m_id = nullptr;
  double g_cost, h_cost, cal_cost;

  for (int i = 0; i < how_many_corners_; i++) {
    mx = curr_data->x + moves[i].x;
    my = curr_data->y + moves[i].y;

    if (!withinLimits(mx, my)) {
      continue;
    }

    const bool start_escape = isStartEscapeTransition(
      curr_data->x, curr_data->y, mx, my);
    if (!isSafe(mx, my) && !start_escape) {
      continue;
    }

    // Use four-connected moves while leaving the high soft-cost start band.
    // This prevents a diagonal from slipping between two harder side cells.
    if (start_escape && i >= 4) {
      continue;
    }

    // Validate the whole edge, not only its destination.  In particular this
    // prevents an 8-connected diagonal step from cutting between blocked
    // orthogonal neighbours.
    if (!start_escape &&
        !isLineSafe(curr_data->x, curr_data->y, mx, my)) {
      continue;
    }

    g_cost = curr_data->g + getEuclideanCost(curr_data->x, curr_data->y, mx, my) +
      getTraversalCost(mx, my);

    m_id = getIndex(mx, my);

    if (m_id == nullptr) {
      addToNodesData(index_generated_);
      m_id = &nodes_data_[index_generated_];
      addIndex(mx, my, m_id);
      index_generated_++;
    }

    exp_node = m_id;

    h_cost = getHCost(mx, my);
    cal_cost = g_cost + h_cost;
    if (exp_node->f > cal_cost) {
      exp_node->g = g_cost;
      exp_node->h = h_cost;
      exp_node->f = cal_cost;
      exp_node->parent_id = curr_data;
      if (!exp_node->is_in_queue) {
        exp_node->x = mx;
        exp_node->y = my;
        exp_node->is_in_queue = true;
        queue_.push({m_id});
      }
    }
  }
}

bool ThetaStar::isStartEscapeTransition(
  int current_x, int current_y, int next_x, int next_y) const
{
  if (isSafe(current_x, current_y)) {
    return false;
  }
  const unsigned char current_cost = costmap_->getCost(current_x, current_y);
  const unsigned char next_cost = costmap_->getCost(next_x, next_y);
  if (current_cost == UNKNOWN_COST || next_cost == UNKNOWN_COST) {
    return allow_unknown_ && next_cost == current_cost;
  }
  if (current_cost >= costmap_2d::INSCRIBED_INFLATED_OBSTACLE ||
      next_cost >= costmap_2d::INSCRIBED_INFLATED_OBSTACLE) {
    return false;
  }
  return next_cost <= current_cost;
}

void ThetaStar::backtrace(std::vector<coordsW> & raw_points, const tree_node * curr_n) const
{
  std::vector<coordsW> path_rev;
  coordsW world{};
  do {
    costmap_->mapToWorld(curr_n->x, curr_n->y, world.x, world.y);
    path_rev.push_back(world);
    if (path_rev.size() > 1) {
      curr_n = curr_n->parent_id;
    }
  } while (curr_n->parent_id != curr_n);
  costmap_->mapToWorld(curr_n->x, curr_n->y, world.x, world.y);
  path_rev.push_back(world);

  raw_points.reserve(path_rev.size());
  for (int i = static_cast<int>(path_rev.size()) - 1; i >= 0; i--) {
    raw_points.push_back(path_rev[i]);
  }
}

bool ThetaStar::losCheck(
  const int & x0, const int & y0, const int & x1, const int & y1,
  double & sl_cost) const
{
  return isLineSafe(x0, y0, x1, y1, &sl_cost);
}

bool ThetaStar::isLineSafe(
  int x0, int y0, int x1, int y1,
  double * traversal_cost,
  coordsM * blocked_cell,
  unsigned char * blocked_cost) const
{
  if (traversal_cost != nullptr) {
    *traversal_cost = 0.0;
  }

  auto reject = [&](int x, int y, unsigned char cost) {
      if (blocked_cell != nullptr) {
        *blocked_cell = {x, y};
      }
      if (blocked_cost != nullptr) {
        *blocked_cost = cost;
      }
      return false;
    };

  auto visit = [&](int x, int y, bool add_traversal_cost) {
      if (x < 0 || y < 0 ||
          x >= static_cast<int>(costmap_->getSizeInCellsX()) ||
          y >= static_cast<int>(costmap_->getSizeInCellsY())) {
        return reject(x, y, static_cast<unsigned char>(UNKNOWN_COST));
      }

      const unsigned char raw_cost = costmap_->getCost(x, y);
      if (raw_cost == UNKNOWN_COST) {
        if (!allow_unknown_) {
          return reject(x, y, raw_cost);
        }
        if (add_traversal_cost && traversal_cost != nullptr) {
          const double unknown_cost = OBS_COST - 1;
          *traversal_cost +=
            w_traversal_cost_ * unknown_cost * unknown_cost / LETHAL_COST / LETHAL_COST;
        }
        return true;
      }

      if (static_cast<int>(raw_cost) > max_allowed_cost_) {
        return reject(x, y, raw_cost);
      }

      if (add_traversal_cost && traversal_cost != nullptr) {
        const double scaled_cost = getCost(x, y);
        *traversal_cost +=
          w_traversal_cost_ * scaled_cost * scaled_cost / LETHAL_COST / LETHAL_COST;
      }
      return true;
    };

  int x = x0;
  int y = y0;
  if (!visit(x, y, false)) {
    return false;
  }

  const int nx = std::abs(x1 - x0);
  const int ny = std::abs(y1 - y0);
  const int step_x = (x1 > x0) - (x1 < x0);
  const int step_y = (y1 > y0) - (y1 < y0);
  int ix = 0;
  int iy = 0;

  // Integer grid traversal between cell centres.  At an exact corner crossing
  // visit both side cells before the diagonal cell (supercover semantics).
  while (ix < nx || iy < ny) {
    const std::int64_t lhs = static_cast<std::int64_t>(1 + 2 * ix) * ny;
    const std::int64_t rhs = static_cast<std::int64_t>(1 + 2 * iy) * nx;

    if (lhs == rhs) {
      if (ix < nx && iy < ny) {
        if (!visit(x + step_x, y, true) || !visit(x, y + step_y, true)) {
          return false;
        }
      }
      if (ix < nx) {
        x += step_x;
        ++ix;
      }
      if (iy < ny) {
        y += step_y;
        ++iy;
      }
    } else if (lhs < rhs) {
      x += step_x;
      ++ix;
    } else {
      y += step_y;
      ++iy;
    }

    if (!visit(x, y, true)) {
      return false;
    }
  }

  return true;
}

void ThetaStar::resetContainers()
{
  index_generated_ = 0;
  int last_size_x = size_x_;
  int last_size_y = size_y_;
  int curr_size_x = static_cast<int>(costmap_->getSizeInCellsX());
  int curr_size_y = static_cast<int>(costmap_->getSizeInCellsY());
  if (((last_size_x != curr_size_x) || (last_size_y != curr_size_y)) &&
    static_cast<int>(node_position_.size()) < (curr_size_x * curr_size_y))
  {
    initializePosn(curr_size_y * curr_size_x - last_size_y * last_size_x);
    nodes_data_.reserve(curr_size_x * curr_size_y);
  } else {
    initializePosn();
  }
  size_x_ = curr_size_x;
  size_y_ = curr_size_y;
}

void ThetaStar::initializePosn(int size_inc)
{
  if (!node_position_.empty()) {
    for (int i = 0; i < size_x_ * size_y_; i++) {
      node_position_[i] = nullptr;
    }
  }

  for (int i = 0; i < size_inc; i++) {
    node_position_.push_back(nullptr);
  }
}

void ThetaStar::clearStart()
{
  // Kept for source compatibility.  Mutating one cell in the layered master
  // costmap created a free island surrounded by the unchanged inflation band
  // and could also clear the previous request's start.  Start escape is now a
  // search-local monotonic-cost rule in setNeighbors().
}
    
} // namespace theta_star

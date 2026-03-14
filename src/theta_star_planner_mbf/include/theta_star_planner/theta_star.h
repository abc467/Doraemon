#pragma once

#include <cmath>
#include <chrono>
#include <functional>
#include <vector>
#include <queue>
#include <algorithm>
#include <iostream>
#include "geometry_msgs/PoseStamped.h"
#include <costmap_2d/costmap_2d_ros.h>
#include <costmap_2d/costmap_2d.h>

const double INF_COST = DBL_MAX;
const int UNKNOWN_COST = 255;
const int OBS_COST = 254;
const int LETHAL_COST = 252;  // 252

struct coordsM
{
  int x, y;
};

struct coordsW
{
  double x, y;
};

struct tree_node
{
  int x, y;
  double g = INF_COST;
  double h = INF_COST;
  const tree_node * parent_id = nullptr;
  bool is_in_queue = false;
  double f = INF_COST;
};

struct comp
{
  bool operator()(const tree_node * p1, const tree_node * p2)
  {
    return (p1->f) > (p2->f);
  }
};

namespace theta_star
{
class ThetaStar
{
public:
    coordsM src_{}, dst_{};
    costmap_2d::Costmap2D* costmap_;

  /// costmap中单元格遍历成本的权重
  double w_traversal_cost_;
  /// 欧几里得距离成本的权重（用于计算 g_cost）
  double w_euc_cost_;
  /// 启发式成本的权重（用于 h_cost 的计算）
  double w_heuristic_cost_;
  /// 设置要搜索的相邻节点数量
  int how_many_corners_;
  /// 设置规划器是否可以通过未知空间进行规划
  bool allow_unknown_;
  /// 地图在 x 方向和 y 方向上的长度
  int size_x_, size_y_;
  /// 规划器检查是否已被取消的时间间隔
  int terminal_checking_interval_;

  ThetaStar();

  ~ThetaStar() = default;

  void setCancelChecker(std::function<bool()> cancel_checker)
  {
    cancel_checker_ = std::move(cancel_checker);
  }

  /**
   * @brief 该函数迭代地在队列（open list）中的节点进行搜索，直到当前节点是目标点或队列为空
   * @param raw_path 用于返回执行算法后获得的路径
   * @return 如果找到路径返回 true，如果在起点和目标点之间未找到路径返回 false
   */
  bool generatePath(std::vector<coordsW> & raw_path);

  /**
   * @brief 该函数检查成本图中某点 (cx, cy) 的成本是否小于 LETHAL_COST
   * @return 返回检查结果
   */
  inline bool isSafe(const int & cx, const int & cy) const
  {
    return (costmap_->getCost(
             cx,
             cy) == UNKNOWN_COST && allow_unknown_) || costmap_->getCost(cx, cy) < LETHAL_COST;
  }

  /**
   * @brief 初始化起点和目标点的值
   */
  void setStartAndGoal(
    const geometry_msgs::PoseStamped & start,
    const geometry_msgs::PoseStamped & goal);

  /**
   * @brief 检查起点和目标点的成本是否大于 LETHAL_COST
   * @return 如果任意一个点的成本大于 LETHAL_COST 返回 true
   */
  bool isUnsafeToPlan() const
  {
    return !(isSafe(src_.x, src_.y)) || !(isSafe(dst_.x, dst_.y));
  }

  /**
   * @brief 清除起点信息
   */
  void clearStart();

  int nodes_opened = 0;  // 记录已打开的节点数量

protected:
  /// 对于坐标 (x, y)，在 node_position_[size_x_ * y + x] 存储指向 nodes_data_ 中该节点数据位置的指针
  /// 初始大小为 size_x_ * size_y_，并根据地图大小变化而调整；
  /// 用于存储节点指针的容器
  std::vector<tree_node *> node_position_;

  /// 向量 nodes_data_ 存储所有已搜索节点的坐标、成本和父节点索引，以及节点是否在队列 queue_ 中的信息
  /// 初始为空，根据搜索的节点数量动态增加；
  /// 用于存储已访问节点数据的容器
  std::vector<tree_node> nodes_data_;

  /// 优先队列（开放列表）用于选择下一个要扩展的节点；
  /// 用于存储待扩展节点的优先队列
  std::priority_queue<tree_node *, std::vector<tree_node *>, comp> queue_;

  /// 计数器变量，用于生成连续的索引
  /// 使得所有节点（包括开放列表和关闭列表中的节点）的数据在 nodes_data_ 中连续存储；
  /// 记录节点索引的计数器
  int index_generated_;

  std::function<bool()> cancel_checker_;

  const coordsM moves[8] = {{0, 1},
    {0, -1},
    {1, 0},
    {-1, 0},
    {1, -1},
    {-1, 1},
    {1, 1},
    {-1, -1}};

  tree_node * exp_node;

  /**
   * @brief 执行当前节点与其父节点之间的视线检查（LOS）
   *        如果存在LOS且新计算的成本较低，则更新当前节点的成本和父节点
   * @param 当前节点的数据
   */
  void resetParent(tree_node * curr_data);

  /**
   * @brief 扩展当前节点
   * @param curr_data 用于传递当前节点的数据
   * @param curr_id 用于传递当前节点在 nodes_position_ 中的索引
   */
  void setNeighbors(const tree_node * curr_data);

  /**
   * @brief 使用 Bresenham 算法执行视线检查，并计算两点 (x0, y0) 和 (x1, y1) 之间直线路径的遍历成本
   * @param sl_cost 用于返回累计的成本
   * @return 如果两点之间存在视线返回 true，否则返回 false
   */
  bool losCheck(
    const int & x0, const int & y0, const int & x1, const int & y1,
    double & sl_cost) const;

  /**
   * @brief 通过回溯从目标节点到起始节点，使用它们的父节点返回路径
   * @param raw_points 用于返回找到的路径
   * @param curr_n 传递目标节点的指针
   */
  void backtrace(std::vector<coordsW> & raw_points, const tree_node * curr_n) const;

  /**
   * @brief 重载函数，用于在执行 LOS 检查时简化成本计算
   * @param cost 表示总直线遍历成本；每次累加节点 (cx, cy) 的遍历成本；该值也会被返回
   * @return 如果遍历成本大于或等于 LETHAL_COST 返回 false，否则返回 true
   */
  bool isSafe(const int & cx, const int & cy, double & cost) const
  {
    double curr_cost = getCost(cx, cy);
    if ((costmap_->getCost(cx, cy) == UNKNOWN_COST && allow_unknown_) || curr_cost < LETHAL_COST) {
      if (costmap_->getCost(cx, cy) == UNKNOWN_COST) {
        curr_cost = OBS_COST - 1;
      }
      cost += w_traversal_cost_ * curr_cost * curr_cost / LETHAL_COST / LETHAL_COST;
      return true;
    } else {
      return false;
    }
  }

  /**
   * @brief 该函数通过将原点平移到 25，然后将实际成本图成本乘以 0.9 来缩放成本图成本，
   *        以确保输出在 [25, 255) 范围内
   * @param cx x 坐标
   * @param cy y 坐标
   * @return 缩放后的成本
   */
  inline double getCost(const int & cx, const int & cy) const
  {
    return 26 + 0.9 * costmap_->getCost(cx, cy);
  }

  /**
   * @brief 计算点 (cx, cy) 的遍历成本，公式为
   *        <参数> * (<实际遍历成本>)^2 / (<最大成本>)^2
   * @param cx x 坐标
   * @param cy y 坐标
   * @return 计算得到的遍历成本
   */
  inline double getTraversalCost(const int & cx, const int & cy)
  {
    double curr_cost = getCost(cx, cy);
    return w_traversal_cost_ * curr_cost * curr_cost / LETHAL_COST / LETHAL_COST;
  }

  /**
   * @brief 计算两点 (ax, ay) 和 (bx, by) 之间的分段直线欧几里得距离，公式为
   *        <欧几里得距离参数> * <两点之间的欧几里得距离>
   * @param ax 第一个点的 x 坐标
   * @param ay 第一个点的 y 坐标
   * @param bx 第二个点的 x 坐标
   * @param by 第二个点的 y 坐标
   * @return 计算得到的距离
   */
  inline double getEuclideanCost(const int & ax, const int & ay, const int & bx, const int & by)
  {
    return w_euc_cost_ * std::hypot(ax - bx, ay - by);
  }

  /**
   * @brief 计算点 (cx, cy) 的启发式成本，公式为
   *        <启发式成本参数> * <点到目标点的欧几里得距离>
   * @param cx x 坐标
   * @param cy y 坐标
   * @return 计算得到的启发式成本
   */
  inline double getHCost(const int & cx, const int & cy)
  {
    return w_heuristic_cost_ * std::hypot(cx - dst_.x, cy - dst_.y);
  }

  /**
   * @brief 检查给定坐标 (cx, cy) 是否在地图范围内
   * @param cx x 坐标
   * @param cy y 坐标
   * @return 检查结果，如果在范围内返回 true，否则返回 false
   */
  inline bool withinLimits(const int & cx, const int & cy) const
  {
    return cx >= 0 && cx < size_x_ && cy >= 0 && cy < size_y_;
  }

  /**
   * @brief 检查节点的坐标是否为目标点
   * @param this_node 节点对象
   * @return 检查结果，如果是目标点返回 true，否则返回 false
   */
  inline bool isGoal(const tree_node & this_node) const
  {
    return this_node.x == dst_.x && this_node.y == dst_.y;
  }

  /**
   * @brief 初始化 node_position_ 向量，将所有点 (x, y) 的索引初始化为 -1
   * @param size_inc 如果地图大小增加，用于增加 node_position_ 中的元素数量
   */
  void initializePosn(int size_inc = 0);

  /**
   * @brief 在 node_position_ 中存储 id_this，索引为 [size_x_ * cy + cx]
   * @param cx x 坐标
   * @param cy y 坐标
   * @param node_this 指向 nodes_data_ 中该点数据位置的指针
   */
  inline void addIndex(const int & cx, const int & cy, tree_node * node_this)
  {
    node_position_[size_x_ * cy + cx] = node_this;
  }

  /**
   * @brief 获取存储在 nodes_data_ 中点 (cx, cy) 数据位置的指针
   * @param cx x 坐标
   * @param cy y 坐标
   * @return 指向该位置的指针
   */
  inline tree_node * getIndex(const int & cx, const int & cy)
  {
    return node_position_[size_x_ * cy + cx];
  }

  /**
   * @brief 根据 nodes_data_ 向量的大小分配空间，存储节点 (x, y) 的数据
   * @param id_this 节点数据存储的索引
   */
  void addToNodesData(const int & id_this)
  {
    if (static_cast<int>(nodes_data_.size()) <= id_this) {
      nodes_data_.push_back({});
    } else {
      nodes_data_[id_this] = {};
    }
  }

  /**
   * @brief 在 generatePath 函数执行开始时初始化全局变量的值
   */
  void resetContainers();

  /**
   * @brief 每次执行 generatePath 函数后清空优先队列
   */
  void clearQueue()
  {
    queue_ = std::priority_queue<tree_node *, std::vector<tree_node *>, comp>();
  }
};
}   //  namespace theta_star

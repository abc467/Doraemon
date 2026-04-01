# 前端整体联调验收总结 v1

这份文档用于对当前前端一期联调结果做总收口，覆盖：

- 地图编辑工作台只读链路
- Zone Editor
- Constraint Editor
- 任务 / 调度 / 执行控制 / 运行监控
- 真实命令流与轻量真实运动联调

本文档面向：

- 前端开发
- 后端开发
- 联调与测试
- 项目阶段验收与留档

---

## 1. 总体结论

当前前端一期主链已经联调通过，可以认为具备了商用清洁机器人项目的第一阶段可交付前端能力：

- 地图底图展示
- 区域查询、预览、提交、更新
- 禁行区 / 虚拟墙查询与编辑
- 任务管理
- 调度管理
- 执行控制
- 运行监控

同时，前后端之间已经完成了一轮基于 live ROS 的真实联调，而不只是本地 build / lint 或 mock 演示。

当前结论是：

- **前端一期主链验收通过**
- **真实命令流验收通过**
- **轻量真实运动联调通过**
- **自动回充验收不在本轮结论内**

---

## 2. 本轮验收范围

### 2.1 已纳入验收

- Read-only Map Workbench
- Zone Editor 一期
- Constraint Editor 一期
- Task Management 一期
- Schedule Management 一期
- Execution Control 一期
- Runtime Monitoring 一期
- `START / PAUSE / CONTINUE / STOP / RETURN` 命令链前端联调

### 2.2 未纳入本轮验收

- 自动回充功能验收
- Docking / 充电成功率产品化验收
- 机器人底盘视觉观察验收
- 任务报表 / 历史记录 / 审计页
- 权限系统
- 多机器人管理

---

## 3. live 联调环境结论

本轮前端 live 联调使用：

- `rosbridge websocket`
  - `ws://10.0.0.174:9090`

已实际用于联调的后端主链包括：

- `/clean_robot_server/map_server`
- `/database_server/map_alignment_service`
- `/database_server/map_alignment_by_points_service`
- `/database_server/coverage_zone_service`
- `/database_server/rect_zone_preview_service`
- `/database_server/coverage_preview_service`
- `/database_server/coverage_commit_service`
- `/database_server/no_go_area_service`
- `/database_server/virtual_wall_service`
- `/database_server/clean_task_service`
- `/database_server/clean_schedule_service`
- `/exe_task_server`

已实际用于联调的运行态 topic 包括：

- `/coverage_task_manager/state`
- `/coverage_task_manager/event`
- `/coverage_executor/state`
- `/coverage_executor/run_progress`
- `/dock_supply/state`
- `/battery_state`
- `/combined_status`
- `/station_status`
- `/task_state`

---

## 4. 分模块验收结果

### 4.1 地图编辑工作台只读链路

结论：**通过**

已确认能力：

- rosbridge 连接稳定
- 地图底图通过 `/clean_robot_server/map_server get(map_name)` 的 `OccupancyGrid` 渲染
- 不是依赖图片 URL，也不是依赖 Cartographer `/map`
- zone 图层可叠加显示
- diagnostics/debug 区可用并默认折叠

真实验证时确认的底图关键字段：

- `map_name = yeyeyeye`
- `width = 1009`
- `height = 1262`
- `resolution = 0.05`
- `data.length = 1273358`

---

### 4.2 Zone Editor 一期

结论：**通过**

已完成并通过 live 验证的能力：

- alignment 查询
- 两点确认主方向
- 两点矩形选区
- 矩形预览
- 覆盖规划预览
- 新建 zone 提交
- 既有 zone 更新
- `ZONE_VERSION_CONFLICT` 冲突处理

真实联调确认：

- `map_alignment_service` 真实类型为 `my_msg_srv/OperateMapAlignment`
- `map_alignment_by_points_service` 真实类型为 `my_msg_srv/ConfirmMapAlignmentByPoints`
- `rect_zone_preview_service` 真实类型为 `my_msg_srv/PreviewAlignedRectSelection`
- `coverage_preview_service` 真实类型为 `my_msg_srv/PreviewCoverageRegion`
- `coverage_commit_service` 真实类型为 `my_msg_srv/CommitCoverageRegion`

已确认的重要事实：

- live backend 中 alignment 可能已经存在，不一定是“未配置”
- `display_frame / storage_frame / aligned_frame` 在 live 环境里可能都是 `map`
- `region` 真实结构是 `PolygonRegion`，不是简化点集
- 更新已有 zone 时，`zone_id` 保持不变，`zone_version` 递增，zone 总数不增加

---

### 4.3 Constraint Editor 一期

结论：**通过**

已完成能力：

- No-Go Area 只读层与详情
- Virtual Wall 列表 / 详情 / 新建 / 修改 / 删除
- enabled 状态编辑
- 画布选中与高亮

真实联调重点结论：

- `/database_server/no_go_area_service` 已按 live 类型接入
- `/database_server/virtual_wall_service` 的 `getAll / add / modify / delete` 已真实跑通
- `enabled=false` 可真实落库
- 当前前端使用 `include_disabled=false`，所以禁用后的 wall 会从列表和画布中隐藏

说明：

- 当前 live 环境中 no-go 数据可能为 0 条，因此 no-go 的页面级真实点击证据相对少于 virtual wall
- 这不影响当前一期“约束编辑前端主链通过”的结论

详细阶段总结见：

- [constraint_editor_frontend_acceptance_v1.md](/home/sunnybaer/Doraemon/docs/constraint_editor_frontend_acceptance_v1.md)

---

### 4.4 任务 / 调度 / 执行控制 / 运行监控 一期

结论：**通过**

#### 任务管理页

- `getAll / get / add / modify / Delete` 已 live 验证通过
- 已按真实后端契约修正：
  - `task_id: int32`
  - `status: int8`
  - 顶层 `enabled_state` 才是真正启停控制位
  - `add` 时 `task_id=0` 由后端自动分配

#### 调度管理页

- `getAll / get / add / modify / Delete` 已 live 验证通过
- 支持：
  - `weekly`
  - `daily`
  - `once`
- 已按真实后端契约修正：
  - `CleanSchedule.dow: int32[]`
  - `invalid timezone` 是 payload 内 `success=false`
  - `Delete` 真实语义是软删除 / 禁用，不会从 `getAll` 里消失

#### 执行控制页

- `/exe_task_server` 已 live 验证通过
- 已支持：
  - `START`
  - `PAUSE`
  - `CONTINUE`
  - `STOP`
  - `RETURN`
- 已确认失败命令不是 transport error，而是正常响应中的 `success=false`

#### 运行监控页

- 已接入并订阅：
  - `/coverage_task_manager/state`
  - `/coverage_task_manager/event`
  - `/coverage_executor/state`
  - `/coverage_executor/run_progress`
  - `/dock_supply/state`
  - `/battery_state`
  - `/combined_status`
  - `/station_status`
- 已实现：
  - task state 卡片
  - executor state 卡片
  - run progress 卡片
  - battery / robot status 卡片
  - dock / supply status 卡片
  - topic health 摘要卡
- 已正确处理：
  - `live`
  - `waiting`
  - `stale`
  - `unavailable`
  - `disconnected`

详细阶段总结见：

- [task_schedule_execution_frontend_acceptance_v1.md](/home/sunnybaer/Doraemon/docs/task_schedule_execution_frontend_acceptance_v1.md)

---

## 5. 真实命令流与轻量真实运动联调结果

结论：**通过**

已通过前端真实页面完成：

- `START -> PAUSE -> CONTINUE -> STOP`
- 单独 `RETURN`
- 一次失败路径验证：
  - 当后端处于 `PAUSED_AUTO_CHARGE / ERROR_DOCK_TIMEOUT` 时
  - `START` 与 `PAUSE` 的失败原始消息均已真实验证

### 5.1 命令流结论

- `START`
  - 前端收到：`accepted: start_task 1`
  - runtime 从 `IDLE` 进入 `RUNNING`
- `PAUSE`
  - 前端收到：`accepted: pause`
  - runtime 进入 `PAUSED`
- `CONTINUE`
  - 前端收到：`accepted: resume`
  - runtime 回到 `RUNNING`
- `STOP`
  - 前端收到：`accepted: stop`
  - runtime 回到 `IDLE`
- `RETURN`
  - 前端收到：`accepted: dock`
  - `task_manager_state` 进入 `MANUAL_DOCKING_STAGE1`
  - `executor_state / run_progress_state` 保持 `IDLE`

### 5.2 三页状态同步结论

以下页面在真实命令流下保持一致：

- Task Management
- Execution Control
- Runtime Monitoring

已确认一致的内容：

- `focused task_id`
- 最近一次命令结果
- 最近一次 raw backend message
- 当前任务状态 / 执行器状态 / 运行进度摘要

说明：

- `LOADING_PLAN -> APPLY_PROFILE -> CONNECT:block_0` 这类中间态切换很快
- 不同页面若在不同秒点抓快照，可能看到相邻阶段
- 这属于 live 状态推进，不是不同步

### 5.3 真实运动结论边界

本轮可以确认：

- 命令被真实接受
- runtime 状态真实推进
- 机器人执行链真实发生变化

本轮不能宣称：

- 已做视觉层面的物理底盘观察验收
- 已做自动回充验收
- 已做回桩成功率验收

---

## 6. 当前已知限制

### 6.1 地图 / alignment 相关

- live 环境中不保证始终存在 `site_map`
- `display_frame / storage_frame / aligned_frame` 当前可能都为 `map`
- 前端不得硬编码 `site_map`

### 6.2 调度相关

- `Delete Schedule` 当前真实语义是“禁用”，不是物理删除
- 前端后续文案建议更贴近“禁用班表”

### 6.3 运行监控相关

以下 topic 在不同运行态下可能不稳定或无活跃 publisher：

- `/dock_supply/state`
- `/battery_state`
- `/combined_status`
- `/station_status`

这会影响监控卡片完整度，但不影响本轮一期主链验收结论。

### 6.4 自动回充相关

- 当前前端已经具备回桩命令入口与部分状态显示能力
- 但自动回充 / 对接 / 充电本身不在本轮验收范围内

---

## 7. 当前可对外交付的口径

从当前阶段结果出发，可以对外给出如下口径：

1. 地图编辑工作台前端已可用
2. 区域编辑与约束编辑前端已可用
3. 任务 / 调度 / 执行控制 / 运行监控前端已可用
4. 前后端已完成一轮真实 ROS live 联调
5. 任务命令流与运行态同步已经通过
6. 自动回充与完整运动验收不在本轮交付范围内

---

## 8. 下一步建议

建议下一步按下面顺序推进：

1. 补运行监控缺失 publisher 的稳定性
2. 视需要补一轮自动回充专项前端验收
3. 收口页面文案、空态、错误态和交互细节
4. 再进入报表、历史、审计、多机器人等二期能力

如果继续做真实运动与回桩联调，建议把“前端验收”和“机器人行为验收”继续分开留档。

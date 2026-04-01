# 任务 / 调度 / 执行监控前端一期验收总结 v1

这份文档用于记录本轮“任务 / 调度 / 执行控制 / 运行监控前端一期”的开发范围、实际联调结果、当前已知限制和后续建议。

本文档面向：

- 前端开发
- 后端开发
- 联调与测试
- 项目阶段验收与留档

---

## 1. 一期目标范围

本期前端目标是完成以下能力：

- 任务管理页
  - 列表 / 详情 / 新建 / 编辑 / 删除
- 调度管理页
  - 列表 / 详情 / 新建 / 编辑 / 删除(软删除/禁用)
- 执行控制页
  - `START / PAUSE / CONTINUE / STOP / RETURN`
- 运行监控页
  - 任务状态
  - 执行器状态
  - 运行进度
  - 电池 / 机器人状态
  - 回充 / 补给状态

本期不包含：

- 真实机器人运动验收
- 真实导航 / 回桩运动闭环验收
- 报表与历史审计页
- 权限系统
- 多机器人管理

---

## 2. 对接后端接口

本期前端对接的后端接口如下：

- `/database_server/clean_task_service`
  - 类型：`my_msg_srv/OperateTask`
- `/database_server/clean_schedule_service`
  - 类型：`my_msg_srv/OperateSchedule`
- `/exe_task_server`
  - 类型：`my_msg_srv/ExeTask`

辅助依赖接口与主题：

- `rosbridge websocket`
  - `ws://10.0.0.174:9090`
- `/coverage_task_manager/state`
- `/coverage_task_manager/event`
- `/coverage_executor/state`
- `/coverage_executor/run_progress`
- `/dock_supply/state`
- `/battery_state`
- `/combined_status`
- `/station_status`

---

## 3. 当前前端实现结论

### 3.1 已完成能力

- Task Management 页面
- Schedule Management 页面
- Execution Control 页面
- Runtime Monitoring 页面
- 顶层导航入口切换
- 跨页共享的 live 命令上下文
- 成功 / 失败原始 backend message 展示
- 运行态卡片持续刷新
- 对 unavailable / waiting / stale / disconnected 状态的降级显示

### 3.2 当前实现原则

当前前端遵循的规则是：

- live `rosapi` 和真实回包优先于文档
- 服务层失败(`success=false`)不当作网络异常吞掉
- 每个 milestone 完成后保持页面可运行
- 真实机器人运动与“命令/状态联调”分开验收

---

## 4. Milestone 验收结果

### 4.1 Milestone 1：任务管理页

结论：**通过**

已完成能力：

- 任务列表 `getAll`
- 单任务详情 `get`
- 新建任务 `add`
- 编辑任务 `modify`
- 删除任务 `Delete`
- 删除失败时显示后端原始拒绝信息

#### live 验证结论

真实服务：

- `/database_server/clean_task_service`
- 类型：`my_msg_srv/OperateTask`

关键 live 字段差异：

- `task_id` 真实类型是 `int32`
- `status` 真实类型是 `int8`
- 顶层请求里真实还有：
  - `map_name`
  - `enabled_state`
- `task.enabled` 不是唯一生效位
- `add` 时传 `task_id=0`，后端会自动分配新 id

真实 CRUD 结果：

- `getAll` 成功，验证时任务数为 `3`
- `get` 成功，真实查到 `task_id=1`
- `add` 成功，新建任务 `task_id=4`
- `modify` 成功，同一 `task_id=4` 更新名称和 `loops`
- `delete` 成功，删除后数量回到 `3`

额外确认：

- 仅改 `task.enabled=false` 而 `enabled_state=0` 时，后端会保留原值
- `enabled_state=1` 时，禁用才真实生效

---

### 4.2 Milestone 2：调度管理页

结论：**通过**

已完成能力：

- 调度列表 `getAll`
- 单调度详情 `get`
- 新建调度 `add`
- 编辑调度 `modify`
- 删除 / 禁用调度 `Delete`
- `weekly / daily / once` 三种类型支持
- 任务下拉选择 `task_id`
- 后端原始错误消息展示，尤其覆盖 invalid timezone

#### live 验证结论

真实服务：

- `/database_server/clean_schedule_service`
- 类型：`my_msg_srv/OperateSchedule`

关键 live 字段差异：

- `schedule_id: string`
- `task_id: int32`
- `schedule: my_msg_srv/CleanSchedule`
- `enabled_state: int8`
- `CleanSchedule.dow` 真实类型是 `int32[]`

invalid timezone 语义：

- 不是 transport 失败
- 而是：
  - `result=true`
  - payload 内 `success=false`
  - `message="bad timezone='Mars/Nowhere'"`

Delete 真实语义：

- 当前 live backend 的 `Delete` 是软删除 / 禁用
- 返回消息是 `disabled`
- 对象不会从 `getAll` 里物理消失

真实 CRUD 结果：

- 初始 `getAll` 数量：`7`
- `once add` 成功
- `weekly add` 成功
- 数量变化：`7 -> 9`
- `modify once` 成功
  - `at` 从 `2026-12-31 23:58` 改为 `2026-12-30 07:15`
  - `time` 从 `23:58` 改为 `07:15`
  - `timezone` 从 `Asia/Shanghai` 改为 `UTC`
- `modify weekly` 成功
  - `enabled: false -> true`
  - `time: 22:10 -> 06:45`
  - `timezone: Asia/Shanghai -> UTC`
- `disable weekly` 成功
  - 详情查询确认已生效
- `delete` 成功，但语义为 `disabled`
  - 数量仍保持 `9`
- invalid timezone 真实拒绝成功验证
  - 原始消息：`bad timezone='Mars/Nowhere'`

---

### 4.3 Milestone 3：执行控制页

结论：**通过**

已完成能力：

- `/exe_task_server` 集成
- 命令按钮：
  - `START`
  - `PAUSE`
  - `CONTINUE`
  - `STOP`
  - `RETURN`
- 任务下拉选择
- 手动 `task_id` 输入
- 最近一次命令结果展示
- 成功 / 失败都显示原始 backend message
- 按钮 loading / disabled 状态

#### live 验证结论

真实服务：

- `/exe_task_server`
- 类型：`my_msg_srv/ExeTask`

真实请求字段：

- `command: uint8`
- `task_id: int32`

真实命令常量：

- `START = 0`
- `PAUSE = 1`
- `CONTINUE = 2`
- `STOP = 3`
- `RETURN = 4`

真实响应字段：

- `success: bool`
- `message: string`

关键行为：

- 命令失败时不是 transport 失败
- 而是正常返回 response，且 `success=false`
- 前端必须展示原始 `message`

真实命令验证结果：

使用 `task_id=1` 做 live 调用，验证到：

1. 第一组：失败消息 + 跨页同步

- 初始状态：
  - `mission=PAUSED`
  - `phase=PAUSED_AUTO_CHARGE`
  - `public=ERROR_DOCK_TIMEOUT`
- `START`
  - `success=false`
  - 原始消息：`task manager busy: ...`
- `PAUSE`
  - `success=false`
  - 原始消息：`pause requires running ...`
- `CONTINUE`
  - `success=true`
  - `message="accepted: resume"`
- `STOP`
  - `success=true`
  - `message="accepted: stop"`

2. 第二组：成功版命令流

- 先把状态收回 `IDLE`
- `START`
  - `success=true`
  - `message="accepted: start_task 1"`
- `PAUSE`
  - `success=true`
  - `message="accepted: pause"`
- `CONTINUE`
  - `success=true`
  - `message="accepted: resume"`
- `STOP`
  - `success=true`
  - `message="accepted: stop"`

---

### 4.4 Milestone 4：运行监控页

结论：**通过**

已完成能力：

- Runtime Monitoring 页面
- 订阅并展示以下 8 个 live topic：
  - `/coverage_task_manager/state`
  - `/coverage_task_manager/event`
  - `/coverage_executor/state`
  - `/coverage_executor/run_progress`
  - `/dock_supply/state`
  - `/battery_state`
  - `/combined_status`
  - `/station_status`
- 运行态卡片：
  - task state
  - executor state
  - run progress
  - battery / robot status
  - dock/supply status
- Topic Health 摘要卡
- 页面持续 live 更新，不需要手动刷新
- 对 `live / waiting / stale / unavailable / disconnected` 状态做了区分

#### live topic 差异

- `/coverage_task_manager/state`
  - `std_msgs/String`
  - 字段只有 `data`
- `/coverage_task_manager/event`
  - `std_msgs/String`
  - 字段只有 `data`
- `/coverage_executor/state`
  - `std_msgs/String`
  - 字段只有 `data`
- `/dock_supply/state`
  - `std_msgs/String`
  - 字段只有 `data`
- `/coverage_executor/run_progress`
  - 类型：`coverage_msgs/RunProgress`
  - 核心字段包括：
    - `run_id`
    - `zone_id`
    - `plan_id`
    - `state`
    - `plan_profile`
    - `sys_profile`
    - `mode`
    - `error_code`
    - `error_msg`
    - `interlock_active`
    - `interlock_reason`
    - `v_mps`
    - `w_rps`
    - `exec_index`
    - `block_id`
    - `path_index`
    - `path_s`
    - `block_length_m`
    - `total_length_m`
    - `progress_0_1`
    - `progress_pct`
    - `stamp`
- `/battery_state`
  - 类型：`sensor_msgs/BatteryState`
- `/combined_status`
  - 类型：`my_msg_srv/CombinedStatus`
- `/station_status`
  - 当前 live `rosapi` 返回空 type

#### live 订阅结果

- 已通过 live `rosapi` 确认 topic type 和 message details
- 已通过前端真实页面验证订阅链路
- `Task State` 卡片真实显示过：
  - `ERROR_DOCK_TIMEOUT`
- `Executor State` 卡片真实显示过：
  - `PAUSED`
- `Run Progress` 真实持续刷新：
  - 2.5 秒内 `message_count` 从 `1 -> 6`
  - `stamp` 持续推进
- 对当前无 publisher 或无 type 的 topic，页面正确显示 `unavailable`

---

### 4.5 Milestone 5：轻量真实联调验收

结论：**通过**

这轮不是机器人运动验收，而是：

- live 命令流验证
- live topic 订阅流验证
- Task / Execution / Runtime 三页一致性验证

#### 已实际验证的 live 命令流

第一组：

- 初始 live 状态：
  - `mission=PAUSED`
  - `phase=PAUSED_AUTO_CHARGE`
  - `public=ERROR_DOCK_TIMEOUT`
- `START`
  - 失败
  - 原始 message 保留
- `PAUSE`
  - 失败
  - 原始 message 保留
- `CONTINUE`
  - 成功
- `STOP`
  - 成功

第二组：

- 先回到 `IDLE`
- `START`
  - 成功
- `PAUSE`
  - 成功
- `CONTINUE`
  - 成功
- `STOP`
  - 成功

#### 三页一致性结论

前端补了轻量共享上下文层，三页共享：

- focused `task_id`
- 最近一次命令结果
- 最近一次 raw backend message
- 当前 task manager / executor / run progress 摘要

实际 live 观察到：

- Execution Control 页点命令后，alert 和共享上下文立即更新
- Runtime Monitoring 页不需要刷新，task state / executor state / run progress 自动更新
- Task Management 页也能显示同一份 live 命令上下文

真实检查结果：

- `START` 后 Task 页显示：
  - `RUNNING / APPLY_PROFILE / LOADING_PLAN`
- `STOP` 后回到：
  - `IDLE / IDLE / IDLE`

这说明三页状态同步是一致的。

#### 当前缺失 publisher / topic

本轮明确观察到：

- `/coverage_task_manager/event`
  - 本次会话里没有观察到新消息
- `/dock_supply/state`
  - 当前无 active publisher
- `/battery_state`
  - 当前无 active publisher
- `/combined_status`
  - 当前无 active publisher
- `/station_status`
  - 当前无有效 type，publishers / subscribers 为空

前端当前会把这些显示为：

- `waiting`
- `unavailable`

不会误报成前端故障。

---

## 5. 一期总体验收结论

结论：**任务 / 调度 / 执行控制 / 运行监控前端一期可视为验收通过。**

当前已经具备可交付的前端能力：

- 任务 CRUD
- 调度 CRUD
- 命令控制
- 运行态监控
- 页面间 live 状态同步
- 命令失败原始信息展示

到这里，“地图编辑工作台 + 区域/约束编辑 + 任务/调度/执行监控”三条前端一期主链都已经收口。

---

## 6. 当前已知限制

### 6.1 这不是机器人真实运动验收

本期通过的是：

- service 层
- topic 层
- 页面状态同步层

不是：

- 真实清扫运动验收
- 真实回桩 / 回充运动验收

### 6.2 当前运行监控不是全量可观测

当前 live 环境下，以下 topic 没有持续活跃 publisher：

- `/dock_supply/state`
- `/battery_state`
- `/combined_status`
- `/station_status`

因此 Runtime Monitoring 页虽然已实现，但部分卡片当前只能展示：

- `waiting`
- `unavailable`

### 6.3 Delete 真实语义需在 UI 文案上继续产品化

- 任务 Delete 当前是真删除
- 调度 Delete 当前是真正的“软删除 / 禁用”

后续前端文案建议进一步区分：

- `Delete Task`
- `Disable Schedule`

### 6.4 当前依然不要求 Cartographer 常驻

在本期验收范围内：

- 任务 / 调度 CRUD
- 执行控制 service 调用
- 状态页联调

都不要求 Cartographer 常驻启动。

只有进入真实导航 / 真实运动联调时，Cartographer 和导航链才需要补齐。

---

## 7. 当前建议的对外交付口径

如果需要向项目内外汇报，建议使用这版口径：

- 地图工作台只读链已通过
- Zone Editor 一期已通过
- Constraint Editor 一期已通过
- Task / Schedule / Execution / Runtime Monitoring 一期已通过
- 当前已具备：
  - 地图编辑
  - 区域编辑
  - 禁行区 / 虚拟墙编辑
  - 任务管理
  - 调度管理
  - 执行控制
  - 运行状态监控
- 当前尚未进入：
  - 真实机器人运动验收
  - 真实回桩 / 回充运动验收

---

## 8. 下一步建议

建议按这个顺序推进：

1. 先做一点产品化收口
   - loading / empty state / error 文案统一
   - 删除 / 禁用语义文案统一
   - debug 面板改成更明确的开发开关

2. 再决定是否进入真实运动联调准备
   - 启动导航链
   - 启动硬件桥
   - 视需要启动 Cartographer

3. 如果进入二期产品化，再考虑：
   - 历史记录 / 审计页
   - 报表
   - 多机器人支持

---

## 9. 文档关联

本总结对应的开发/接口文档如下：

- [frontend_backend_interface_v1.md](/home/sunnybaer/Doraemon/docs/frontend_backend_interface_v1.md)
- [task_schedule_execution_frontend_tasks_v1.md](/home/sunnybaer/Doraemon/docs/task_schedule_execution_frontend_tasks_v1.md)
- [zone_editor_frontend_flow_v1.md](/home/sunnybaer/Doraemon/docs/zone_editor_frontend_flow_v1.md)
- [zone_editor_frontend_tasks_v2.md](/home/sunnybaer/Doraemon/docs/zone_editor_frontend_tasks_v2.md)
- [constraint_editor_frontend_flow_v1.md](/home/sunnybaer/Doraemon/docs/constraint_editor_frontend_flow_v1.md)
- [constraint_editor_frontend_tasks_v1.md](/home/sunnybaer/Doraemon/docs/constraint_editor_frontend_tasks_v1.md)
- [constraint_editor_frontend_acceptance_v1.md](/home/sunnybaer/Doraemon/docs/constraint_editor_frontend_acceptance_v1.md)

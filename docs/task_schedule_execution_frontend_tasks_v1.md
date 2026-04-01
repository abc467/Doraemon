# 任务 / 调度 / 执行控制前端开发任务拆解清单 v1

这份文档是给 Windows 上 VSCode 里的 Codex 插件直接执行的。

当前前端已经完成并验收通过：

- 地图工作台只读版
- Zone Editor 一期
- Constraint Editor 一期

下一阶段正式进入：

- 任务管理
- 调度管理
- 执行控制
- 运行状态监控

---

## 1. 当前后端条件

当前 live 后端已经具备这几条主链：

- `/database_server/clean_task_service`
- `/database_server/clean_schedule_service`
- `/exe_task_server`
- `rosbridge websocket`
  - `ws://10.0.0.174:9090`

当前也已具备可订阅的运行状态主题，包括但不限于：

- `/coverage_task_manager/state`
- `/coverage_task_manager/event`
- `/coverage_executor/state`
- `/coverage_executor/run_progress`
- `/dock_supply/state`
- `/battery_state`
- `/combined_status`
- `/station_status`

---

## 2. 这轮总目标

完成一个可用的任务运营前端闭环：

1. 任务列表 / 新建 / 编辑 / 删除
2. 调度列表 / 新建 / 编辑 / 删除 / 启停
3. 任务启动 / 暂停 / 继续 / 停止 / 回桩
4. 运行状态、进度、电量、回充状态监控

注意：

- 这轮先做“前端可用闭环”
- 不做更复杂的报表与历史审计页
- 不做权限系统
- 不做 HTTP 网关改造，继续直接走 rosbridge + ROS service/topic

---

## 3. 关键原则

### 3.1 先做管理页，再做执行页

推荐顺序：

1. 任务管理页
2. 调度管理页
3. 执行控制页
4. 运行状态页

原因：

- 任务和调度是纯 CRUD，更稳定
- 执行控制依赖任务配置与状态订阅
- 运行状态页需要前面模型已经收口

### 3.2 先做数据闭环，再做真实运动联调

这轮先完成：

- service 调用
- topic 订阅
- 页面状态同步

再做：

- 真实启动任务
- 真实暂停 / 继续 / 停止
- 真实回桩

### 3.3 Cartographer 不是这轮每一步都必须

当前结论：

- 任务 CRUD / 调度 CRUD / 运行状态 UI 开发：**不要求 Cartographer 必须启动**
- 执行控制页的真实 `START/RETURN` 联调、以及机器人真的动起来：**需要定位 / 导航链正常，届时再起 Cartographer 和导航链**

不要一上来把页面开发和真实导航联调绑死。

---

## 4. 本轮对接接口

### 4.1 任务管理

- 服务：`/database_server/clean_task_service`
- 类型：`my_msg_srv/OperateTask`

支持：

- `getAll`
- `get`
- `add`
- `modify`
- `Delete`

### 4.2 调度管理

- 服务：`/database_server/clean_schedule_service`
- 类型：`my_msg_srv/OperateSchedule`

支持：

- `getAll`
- `get`
- `add`
- `modify`
- `Delete`

### 4.3 执行控制

- 服务：`/exe_task_server`
- 类型：`my_msg_srv/ExeTask`

支持命令：

- `START`
- `PAUSE`
- `CONTINUE`
- `STOP`
- `RETURN`

### 4.4 状态监控

建议前端订阅：

- `/coverage_task_manager/state`
- `/coverage_task_manager/event`
- `/coverage_executor/state`
- `/coverage_executor/run_progress`
- `/dock_supply/state`
- `/battery_state`
- `/combined_status`
- `/station_status`

---

## 5. 页面拆分建议

建议新增 4 个页面或模块：

1. `TaskManagementPage`
2. `ScheduleManagementPage`
3. `ExecutionControlPage`
4. `RuntimeStatusPanel`

其中：

- `RuntimeStatusPanel` 可以先做成可嵌入卡片
- 后面再决定是否独立成整页

---

## 6. Milestone 1：任务管理页

### 6.1 目标

先把任务 CRUD 做完整。

### 6.2 页面能力

- 任务列表
- 单任务详情
- 新建任务
- 编辑任务
- 删除未运行任务

任务创建 / 编辑交互要求：

- `zone_id` 不再使用手动文本输入
- 前端应根据当前 `map_name` 调 `/database_server/coverage_zone_service getAll`
- 把 zone 做成 Select：
  - 主显示：`display_name`
  - 次显示：`zone_id`
  - 可附带展示：`plan_profile_name / estimated_length_m / estimated_duration_s`
- 操作员选择的是“区域”，不是复制粘贴 `zone_id`
- 前端最终提交时，仍然向后端传字符串 `zone_id`

### 6.3 必须显示的关键字段

- `task_id`
- `name`
- `enabled`
- `status`
- `map_name`
- `zone_id`
- `plan_profile_name`
- `sys_profile_name`
- `clean_mode`
- `return_to_dock_on_finish`
- `loops`

### 6.4 验收标准

- `getAll` 可正确显示任务列表
- `get` 可正确显示任务详情
- `add/modify/Delete` 能真实调用 live backend
- 删除运行中任务时能正确显示后端拒绝信息
- 新建 / 编辑任务时，`zone` 选择来自当前地图区域列表，而不是手输 `zone_id`
- 选择某个 zone 后，最终提交给后端的 `zone_id` 正确
- 新建 / 编辑任务时，应提供“任务完成后回桩补给/充电”的开关
- 修改任务时，如果前端没有显式改这个开关，不应把旧值误覆盖

---

## 7. Milestone 2：调度管理页

### 7.1 目标

完成调度 CRUD。

### 7.2 页面能力

- 调度列表
- 单调度详情
- 新建调度
- 编辑调度
- 删除 / 禁用调度

### 7.3 必须显示的关键字段

- `schedule_id`
- `task_id`
- `task_name`
- `enabled`
- `type`
- `dow`
- `time`
- `at`
- `timezone`
- `start_date`
- `end_date`
- `return_to_dock_on_finish`
- `last_fire_ts`
- `last_done_ts`
- `last_status`

### 7.4 必须支持的调度类型

- `weekly`
- `daily`
- `once`

### 7.5 验收标准

- 新建 `once` 调度成功
- 新建 `weekly` 调度成功
- 无效 `timezone` 时正确显示后端错误
- 调度启用 / 禁用操作生效

---

## 8. Milestone 3：执行控制页

### 8.1 目标

完成手动控制入口，但先不要求真实机器人移动联调通过。

### 8.2 页面能力

- 选择任务
- `START`
- `PAUSE`
- `CONTINUE`
- `STOP`
- `RETURN`

### 8.3 页面要求

- 执行控制按钮必须有 loading / 禁用态
- 当前无可执行任务时要有明确提示
- 收到后端失败消息时要显示原始 message

### 8.4 验收标准

- 服务调用链正常
- 命令发出后页面状态更新正确
- 不要求本 milestone 就完成真实清扫运动

---

## 9. Milestone 4：运行状态与监控

### 9.1 目标

把运行时状态卡片做完整。

### 9.2 必做状态卡

#### 任务状态卡

- `coverage_task_manager/state`
- 最近 `event`

#### 执行器状态卡

- `coverage_executor/state`
- `run_progress`

#### 电池 / 机器人状态卡

- `battery_state`
- `combined_status`

#### 回充 / 补给状态卡

- `dock_supply/state`
- `station_status`

### 9.3 验收标准

- 所有卡片在 topic 变化时实时刷新
- 空状态与未连接状态处理正确
- 不依赖页面手工刷新

---

## 10. Milestone 5：真实联调验收

这一步再做真实机器人联调，不要前置到前几个 milestone。

### 10.1 需要的后端条件

真实执行联调时，通常需要：

- 任务系统已启动
- 硬件桥已启动
- 若要真实导航执行，则还需要定位 / 导航链正常
- 这时再视需要启动 Cartographer

### 10.2 建议验证顺序

1. 任务 CRUD live 验证
2. 调度 CRUD live 验证
3. `ExeTask` 命令 live 验证
4. 状态 topic live 验证
5. 最后才做真实 `START` 后机器人运动验证

---

## 11. 当前明确不做的内容

这一轮先不做：

- 清扫历史报表
- 调度审计历史独立页面
- 多机器人管理
- 用户权限
- HTTP/REST 网关
- 真正复杂的运维后台布局优化

---

## 12. 建议的开发顺序

请严格按这个顺序做：

1. `Milestone 1`：任务管理页
2. `Milestone 2`：调度管理页
3. `Milestone 3`：执行控制页
4. `Milestone 4`：运行状态卡
5. `Milestone 5`：真实联调验收

不要跳步。

---

## 13. 给 Windows Codex 的执行要求

每完成一个 milestone，必须回报：

1. 已完成内容
2. 变更文件
3. 如何测试
4. `build/lint` 是否通过
5. 是否已做 live 后端验证

如果 live backend 字段与文档不一致：

- 优先以 live `rosapi` 和真实 service/topic 回包为准
- 再回写前端实现
- 最后把差异记录下来

# 前端对接用后端接口清单 v1

这份清单面向前端工程使用，目标是帮助前端基于当前 ROS 后端快速落地页面、类型定义和联调逻辑。

当前建议前端只依赖“稳定接口 + 状态”，不要直接绑定底层桥接、导航内核、精对接 action、内部命令 topic。

区域编辑页面的实际调用顺序说明，见：

- [zone_editor_frontend_flow_v1.md](/home/sunnybaer/Doraemon/docs/zone_editor_frontend_flow_v1.md)
- [constraint_editor_frontend_flow_v1.md](/home/sunnybaer/Doraemon/docs/constraint_editor_frontend_flow_v1.md)
- [zone_editor_frontend_tasks_v2.md](/home/sunnybaer/Doraemon/docs/zone_editor_frontend_tasks_v2.md)
- [constraint_editor_frontend_tasks_v1.md](/home/sunnybaer/Doraemon/docs/constraint_editor_frontend_tasks_v1.md)
- [task_schedule_execution_frontend_tasks_v1.md](/home/sunnybaer/Doraemon/docs/task_schedule_execution_frontend_tasks_v1.md)

相关阶段验收总结，见：

- [constraint_editor_frontend_acceptance_v1.md](/home/sunnybaer/Doraemon/docs/constraint_editor_frontend_acceptance_v1.md)
- [task_schedule_execution_frontend_acceptance_v1.md](/home/sunnybaer/Doraemon/docs/task_schedule_execution_frontend_acceptance_v1.md)
- [frontend_integration_acceptance_v1.md](/home/sunnybaer/Doraemon/docs/frontend_integration_acceptance_v1.md)

## 0. 新增建议优先接入：系统就绪态 / 运行前检查

为减少现场“点 START 才发现底层没准备好”的联调成本，后端已新增统一 readiness 聚合接口：

- 服务名：`/coverage_task_manager/get_system_readiness`
- 类型：`my_msg_srv/GetSystemReadiness`
- 话题名：`/coverage_task_manager/system_readiness`
- 类型：`my_msg_srv/SystemReadiness`

用途：

- 在前端真正允许 `START` 前，统一判断当前系统是否 ready
- 聚合地图一致性、导航链、底盘桥、任务主状态、执行器残留状态、电池/综合状态 freshness
- 可选按 `task_id` 检查具体任务的 `map / zone / active plan` 是否 ready

请求字段：

- `task_id`
  - `0` 表示只做系统级 readiness 检查
  - 非 `0` 表示同时检查该任务本身的 zone/plan/config 是否 ready
- `refresh_map_identity`
  - 是否强制刷新运行时 `/map` 身份

返回核心字段：

- `overall_ready`
- `can_start_task`
- `blocking_reasons[]`
- `warnings[]`
- `checks[]`
  - 每个子系统一条明细，包含：
    - `key`
    - `level`
    - `ok`
    - `fresh`
    - `stale`
    - `missing`
    - `age_s`
    - `summary`

当前后端聚合的检查项包括：

- active map 选择状态
- runtime `/map` 身份与 active map asset 是否一致
- `move_base_flex` 是否在线
- `mcore_tcp_bridge` 是否在线且 connected
- `coverage_task_manager` 是否空闲
- `coverage_executor` 是否残留非空闲态
- battery freshness
- combined_status freshness
- dock_supply 当前状态
- station bridge / station_status freshness
- 可选：指定 `task_id` 的 task config/zone/active plan 是否 ready

## 1. 总体结论

从商用清洁机器人应用落地的角度看，当前后端这几块主链已经基本具备，前端可以开始写：

- 地图管理
- 任务管理
- 调度管理
- 执行控制
- 运行状态监控
- 回充/补给状态监控

当前唯一仍偏工程态、不建议直接作为前端正式契约的模块是：

- 区域选区 / 规划确认

这部分现在仍然更适合 RViz 调试，而不是 Web / APP 前端直接调用。

## 2. 推荐前端直接依赖的稳定接口

### 2.0 配置目录 / Profile 列表

- 服务名：`/database_server/profile_catalog_service`
- 类型：`my_msg_srv/GetProfileCatalog`

用途：

- 给前端返回合法可选的 `plan profile / sys profile`
- 替代前端手动输入 `cover_standard / standard / eco ...` 这类字符串
- 供 Zone、Task、Schedule、Runtime 页面统一展示和下拉选择

请求字段：

- `profile_kind`
  - `"plan"`：只返回规划 profile
  - `"sys"`：只返回运行 profile
  - `""`：全部返回
- `include_disabled`
  - 是否连已禁用 profile 一起返回
- `map_name`
  - 当前版本保留该字段
  - 目前 plan/sys profile 还没有严格的地图级约束时，后端会忽略它

响应数据类型：

- `my_msg_srv/ProfileOption[]`

`ProfileOption` 关键字段：

- `profile_name`
- `display_name`
- `profile_kind`
- `enabled`
- `is_default`
- `description`
- `version`
- `tags[]`
- `supported_clean_modes[]`
- `supported_maps[]`
- `warnings[]`

当前后端行为：

- `plan` profile 来源于 `planning.db.plan_profiles`
- `sys` profile 来源于 `operations.db.sys_profiles`
- `plan` profile 当前会附带已有 plan 观测到的 `supported_maps[]`
- `sys` profile 当前会附带默认 `supported_clean_modes[]`
- 若 `include_disabled=false`，已禁用 profile 不会出现在默认下拉里
- 若 `include_disabled=true`，前端可以把历史 profile 显示为“已禁用/历史配置”

前端建议使用方式：

- Zone 页面：
  - `profile_name` 改成 Select
  - 数据来源：`profile_kind="plan"`
- Task 页面：
  - `plan_profile_name` 改成 Select
  - `sys_profile_name` 改成 Select
- Schedule / Runtime 页面：
  - 对已有任务/调度里出现的 profile_name，优先显示 `display_name`
  - 如果 profile 已禁用，保留展示并增加“已禁用”标记

兼容性：

- 现有 `preview / commit / task add / modify / schedule add / modify` 接口字段不变
- 后端仍然继续接收 `profile_name / plan_profile_name / sys_profile_name` 的字符串值
- 新接口只负责提供“合法可选值列表”

### 2.1 地图管理

- 服务名：`/clean_robot_server/map_server`
- 类型：`my_msg_srv/OperateMap`

用途：

- `getAll`：获取地图列表
- `get`：获取单张地图详情
- `add`：纳管外部地图资产
- `modify`：修改地图元数据、启停状态、设为当前地图
- `Delete`：软删除 / 禁用地图

核心数据类型：

- `my_msg_srv/PgmData`

前端建议页面：

- 地图列表
- 当前地图切换
- 地图启停管理

备注：

- 当前地图切换是通过 `modify + set_active=true` 完成的
- 地图资产是“纳管外部地图”，不是前端直接上传原始 SLAM 文件的完整产品化接口
- 当前标准外部地图目录为：`/home/sunnybaer/Doraemon/map`
- 前端 `Import Current Map Asset` 应按 `map_name` 从该目录读取 `<map_name>.pbstream` 后纳管为正式地图资产

### 2.2 任务管理

- 服务名：`/database_server/clean_task_service`
- 类型：`my_msg_srv/OperateTask`

用途：

- `getAll`
- `get`
- `add`
- `modify`
- `Delete`

核心数据类型：

- `my_msg_srv/CleanTask`

`CleanTask` 关键字段：

- `task_id`
- `name`
- `status`
- `loops`
- `zone_id`
- `plan_profile_name`
- `sys_profile_name`
- `clean_mode`
- `map_name`
- `return_to_dock_on_finish`
- `enabled`

补充说明：

- `return_to_dock_on_finish`
  - `true`：任务最终完成后，自动进入回桩补给/充电流程
  - `false`：任务完成后直接停在现场，不自动回桩
- 任务 CRUD 请求里还新增了：
  - `OperateTask.return_to_dock_state`
    - `0 = KEEP`
    - `1 = DISABLE`
    - `2 = ENABLE`
  - 用途是避免前端在“修改任务但未显式改这个开关”时，把旧值误覆盖掉

前端交互建议：

- 不要让操作员手动输入 `zone_id`
- 应通过 `/database_server/coverage_zone_service getAll`
  - 按当前 `map_name` 获取可选区域
  - 下拉主显示 `display_name`
  - 次显示 `zone_id / plan_profile_name / estimated_length_m / estimated_duration_s`
- 前端最终提交给任务服务时，仍然继续传字符串 `zone_id`
- `display_name` 是给人看的区域名称；`zone_id` 是后端内部唯一键
- 如果某条已有任务引用了已禁用 zone，详情页仍应保留展示，不要直接显示为空

后端已做的关键校验：

- 地图必须存在且启用
- 区域必须存在且启用
- 激活 plan 必须匹配 `map / zone / profile`
- 正在运行的任务不能删除

前端建议页面：

- 任务列表
- 新建 / 编辑任务
- 启停状态展示

### 2.3 调度管理

- 服务名：`/database_server/clean_schedule_service`
- 类型：`my_msg_srv/OperateSchedule`

用途：

- `getAll`
- `get`
- `add`
- `modify`
- `Delete`

核心数据类型：

- `my_msg_srv/CleanSchedule`

`CleanSchedule` 关键字段：

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

当前后端能力：

- 支持 `weekly / daily / once`
- 支持 `timezone / start_date / end_date`
- 调度失败自动退避重试
- 调度配置支持热重载
- `once` 触发后自动失效
- 无效时区直接拒绝

前端建议页面：

- 班表列表
- 新建周任务 / 日任务 / 单次任务
- 启用 / 禁用班表
- 上次触发时间、上次结果展示

### 2.4 执行控制

- 服务名：`/exe_task_server`
- 类型：`my_msg_srv/ExeTask`

支持命令：

- `START`
- `PAUSE`
- `CONTINUE`
- `STOP`
- `RETURN`

用途：

- 启动指定任务
- 暂停任务
- 继续任务
- 停止任务
- 手动触发回桩

前端建议页面：

- 任务启动按钮
- 暂停 / 继续 / 停止按钮
- 手动回桩按钮

### 2.5 禁行区与虚拟墙管理

- 禁行区服务：`/database_server/no_go_area_service`
- 类型：`my_msg_srv/OperateMapNoGoArea`

- 虚拟墙服务：`/database_server/virtual_wall_service`
- 类型：`my_msg_srv/OperateMapVirtualWall`

用途：

- `getAll/get/add/modify/Delete`

核心数据类型：

- `my_msg_srv/MapNoGoArea`
- `my_msg_srv/MapVirtualWall`

后端已做的关键能力：

- 支持 `site_map <-> map` 双向几何转换
- 前端展示几何与正式存储几何同时返回
- 与当前地图 active alignment 打通
- 如果当前地图没有 active alignment，默认退回 `display_frame = map`，这在新地图场景下属于正常工作态
- 更新后会进入现有 map constraint 快照链，供导航 keepout layer 使用

前端建议页面：

- 禁行区列表 / 编辑
- 虚拟墙列表 / 编辑

### 2.6 Zone 查询与删除

- 服务：`/database_server/coverage_zone_service`
- 类型：`my_msg_srv/OperateCoverageZone`

用途：

- `get`
- `getAll`
- `Delete`

说明：

- `Delete` 当前语义是**软删除 / 禁用 zone**
- 默认 `include_disabled=false` 时，删除后的 zone 不再出现在列表里
- 使用 `include_disabled=true` 仍可查询到已禁用 zone

### 2.7 Zone Active Plan 路径查询

- 服务：`/database_server/zone_plan_path_service`
- 类型：`my_msg_srv/GetZonePlanPath`

用途：

- 按 `zone_id` 查询当前 active plan 的路径叠加数据
- 供前端在区域列表点选后，把“这个区域对应的覆盖路径”画到地图上

建议请求：

- `map_name = 当前地图名`
- `zone_id = 目标 zone_id`
- `alignment_version = ''`
- `plan_profile_name = ''`

返回结果里前端重点使用：

- `active_plan_id`
- `plan_profile_name`
- `display_frame`
- `storage_frame`
- `display_path`
- `map_path`
- `display_entry_pose`
- `entry_pose`
- `estimated_length_m`
- `estimated_duration_s`
- `warnings`

说明：

- 这是轻量查询接口，不会重跑规划，只读取当前 active plan 的已存储路径
- 如果 zone 当前没有 active plan，后端会返回明确错误
- 前端可按需懒加载，不需要在 zone 列表初次加载时一次性拉全量路径

## 3. 前端建议订阅的状态话题

### 3.1 任务与执行总状态

- `/coverage_task_manager/state`
  - 类型：`std_msgs/String`
  - 用途：任务层对外总状态

- `/coverage_task_manager/event`
  - 类型：`std_msgs/String`
  - 用途：事件流 / 故障流 / 恢复流

- `/coverage_executor/state`
  - 类型：`std_msgs/String`
  - 用途：执行器内部运行阶段

- `/coverage_executor/run_progress`
  - 类型：`coverage_msgs/RunProgress`
  - 用途：清扫进度、速度、当前 block/path 信息

- `/coverage_task_manager/task_state`
  - 类型：`coverage_msgs/TaskState`
  - 用途：更适合 UI 的任务快照聚合状态

### 3.2 回充 / 补给 / 电池 / 设备状态

- `/dock_supply/state`
  - 类型：`std_msgs/String`
  - 用途：回充补给子流程状态

- `/battery_state`
  - 类型：`sensor_msgs/BatteryState`
  - 用途：电量、电压
  - 备注：当前最可信的是 `percentage` 和 `voltage`

- `/combined_status`
  - 类型：`my_msg_srv/CombinedStatus`
  - 用途：污水位、清水位、执行机构位置、避障与底盘综合状态

- `/station_status`
  - 类型：`my_msg_srv/StationStatus`
  - 用途：充电桩侧状态、IR 到位、充电机状态等

前端建议页面：

- 顶部全局状态条
- 清扫进度卡片
- 回充 / 补给流程状态机
- 故障与告警面板
- 设备状态面板

## 4. 建议前端直接建模的数据结构

### 4.1 任务快照：`coverage_msgs/TaskState`

关键字段：

- `mission_state`
- `phase`
- `public_state`
- `active_job_id`
- `run_id`
- `zone_id`
- `plan_profile`
- `sys_profile`
- `mode`
- `plan_id`
- `map_id`
- `map_md5`
- `loops_total`
- `loops_done`
- `active_loop_index`
- `progress_0_1`
- `progress_pct`
- `executor_state`
- `error_code`
- `error_msg`
- `interlock_active`
- `interlock_reason`
- `last_event`
- `battery_soc`
- `battery_valid`
- `stamp`

前端用途建议：

- 任务详情页主数据源
- 任务状态卡片
- 异常状态提示

### 4.2 清扫进度：`coverage_msgs/RunProgress`

关键字段：

- `run_id`
- `zone_id`
- `plan_id`
- `state`
- `plan_profile`
- `sys_profile`
- `mode`
- `map_id`
- `map_md5`
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

前端用途建议：

- 进度条
- 当前速度
- 当前清扫路径信息

### 4.3 设备综合状态：`my_msg_srv/CombinedStatus`

关键字段：

- `battery_percentage`
- `battery_voltage`
- `sewage_level`
- `clean_level`
- `brush_position`
- `scraper_position`
- `obstacle_status`
- `region`
- `status`

前端用途建议：

- 水位展示
- 清洁机构状态展示
- 避障传感状态展示

### 4.4 充电桩状态：`my_msg_srv/StationStatus`

字段：

- `bool[14] status`

当前代码注释含义：

- `0`: 急停1
- `1`: 急停2
- `2`: 清洗液开关
- `3`: 自动
- `4`: 手动
- `5`: 电缸伸出
- `6`: 电缸缩回
- `7`: 伸出到位
- `8`: 缩回到位
- `9`: 清洁液面
- `10`: 污水液面
- `11`: AGV到位
- `12`: 充电机状态
- `13`: 充电机故障

前端用途建议：

- 回桩到位提示
- 桩状态 / 故障灯状态

## 5. 当前常见状态字符串

### 5.1 `/coverage_task_manager/state`

当前前端最值得关心的常见值：

- `IDLE`
- `RUNNING`
- `PAUSED`
- `ESTOP`
- `WAIT_RELOCALIZE`
- `AUTO_DOCKING_STAGE1`
- `AUTO_DOCKING_STAGE2`
- `AUTO_DOCKING_PRECISE`
- `AUTO_SUPPLY_DRAIN`
- `AUTO_SUPPLY_REFILL`
- `AUTO_CHARGING`
- `AUTO_UNDOCKING`
- `MANUAL_DOCKING_STAGE1`
- `MANUAL_DOCKING_STAGE2`
- `MANUAL_DOCKING_PRECISE`
- `MANUAL_SUPPLY_DRAIN`
- `MANUAL_SUPPLY_REFILL`
- `MANUAL_CHARGING`
- `MANUAL_UNDOCKING`
- `ERROR_DOCK`
- `ERROR_SUPPLY_FAILED`

说明：

- 这是字符串状态，不是严格枚举类型
- 前端建议按“已知状态 + 未知状态兜底”方式实现

### 5.2 `/dock_supply/state`

当前常见值：

- `IDLE`
- `RUNNING`
- `LOCK_DOCK_POSE`
- `SEARCH_DOCK_POSE`
- `PRECISE_DOCKING`
- `WAIT_STATION_IN_PLACE`
- `SEARCH_STATION_IN_PLACE`
- `DRAINING`
- `REFILLING`
- `CHARGE_CMD_SENT`
- `CHARGE_CONFIRMED`
- `DISABLE_CHARGING`
- `EXIT_BACKING`
- `DONE`
- `CANCELED`
- `FAILED_*`

前端建议：

- 用流程图展示
- 对 `FAILED_*` 做前缀匹配，不要只写死一个失败值

## 6. 当前不建议前端直接依赖的接口

这些接口更适合内部编排、底层桥接、导航调试，不建议前端直接作为稳定契约：

- `/coverage_task_manager/cmd`
- `/station/control`
- `/mcore/charge_enable`
- `/mcore/control_*`
- `/move_base_flex/*`
- `docking_action`

原因：

- 它们语义更底层
- 稳定性不如上层服务接口
- 容易把前端和内部实现耦合死

## 7. 当前仍偏工程态、建议单列的能力

区域选区 / 规划确认目前主要依赖：

- `/clicked_point`
- `/rect_zone_planner/confirm_rect_plan`
- `/rect_zone_planner/cancel_rect_plan`

这套接口更像 RViz 调试链，不适合直接给 Web / APP 前端做正式区域编辑。

如果前端后续需要“地图上框选区域并生成 plan”，建议二期单独补一套前端友好的接口：

- 直接提交矩形 / 多边形点集
- 返回 `zone_id / plan_id`
- 查询已有 `zone / plan` 列表与详情

## 8. 前端联调测试建议

### 8.1 地图页

- 获取地图列表
- 切换当前地图
- 禁用地图后创建任务应失败

### 8.2 任务页

- 新建任务
- 修改任务
- 删除未运行任务
- 删除运行中任务应被拒绝

### 8.3 调度页

- 新建单次班表
- 新建周班表
- 无效时区应被拒绝
- `once` 到点后应自动失效

### 8.4 运行页

- `START / PAUSE / CONTINUE / STOP / RETURN`
- 状态流和进度流实时更新

### 8.5 回充页

- `dock_supply/state`
- `battery_state`
- `combined_status`
- `station_status`
- 联动展示补给 / 充电流程

## 9. 当前默认约束

- 前端 v1 先按 ROS service / topic 原生接口对接
- 不额外抽象成 HTTP / WebSocket 契约
- 前端 v1 优先覆盖：
  - 地图管理
  - 任务管理
  - 调度管理
  - 运行控制
  - 状态监控
- 区域绘制 / 规划编辑暂时视为二期能力

## 10. 建议的前端模块划分

建议前端先按下面几块拆分：

- 地图管理模块
- 任务管理模块
- 调度管理模块
- 运行监控模块
- 回充 / 补给监控模块
- 故障 / 事件日志模块

这几块已经和当前后端接口形态基本对齐，可以直接开始做。

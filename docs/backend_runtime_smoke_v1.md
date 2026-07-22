# 后端 Runtime Smoke 回归说明 v1

## 定位说明

这份文档是当前后端 runtime 回归的主入口。

适用场景：

- 日常只读 smoke
- runtime 启动后的只读基础回归
- 阶段 L 已完成现场批准和物理安全门后，通过独立 acceptance 工具执行受控 workflow

如果你只是想确认“后端主链现在是否健康”，应先看这份文档，而不是直接从更长的人工操作顺序文档开始。

## 本轮内容校验依据

本轮已按当前 smoke 与 contract 检查工具核对这份说明，重点依据包括：

- `rosrun coverage_planner run_backend_runtime_smoke.py`
- `rosrun coverage_planner run_revision_workflow_acceptance.py`
- `rosrun coverage_planner check_ros_contracts.py`
- [check_frontend_services.sh](../scripts/check_frontend_services.sh)

这份文档对应两个职责分离的工具：

- `rosrun coverage_planner run_backend_runtime_smoke.py`：严格只读，所有写动作参数均 fail closed
- `rosrun coverage_planner run_revision_workflow_acceptance.py`：只在现场批准后执行受控写动作

只读 smoke 覆盖后端一期最重要的 3 条主链：

- SLAM
- Odom
- System Readiness

其中阶段 K 的 `stage_k_new_vehicle_no_map` profile 还会额外只读检查 Dock Calibration
状态；它不会调用标定命令或生成标定文件。

以下 workflow 不属于通用 smoke；只有通过独立 acceptance 工具和阶段 L 门禁才可执行：

- `prepare_for_task`
- `relocalize`
- `switch_map_and_localize`
- `start_mapping`
- `save_mapping`
- `stop_mapping`

## 1. 默认模式

默认只做只读 smoke，不改现场状态：

```bash
export ROS_MASTER_URI=http://127.0.0.1:11311
rosrun coverage_planner run_backend_runtime_smoke.py --robot-id CR-001 --text
```

默认检查：

- `/clean_robot_server/app/get_slam_status`
- `/clean_robot_server/app/get_odometry_status`
- `/coverage_task_manager/app/get_system_readiness`

补充说明：

- 只读 smoke 现优先走并行 `cleanrobot_app_msgs` query service
- 写动作提交仍保持 `/clean_robot_server/app/submit_slam_command`
- 异步 job 轮询改走 `/clean_robot_server/app/get_slam_job`
- 所有 SLAM、里程计及 workflow 前后快照都使用显式 `--robot-id`；商业现场不得依赖
  `local_robot` 默认值，示例中的 `CR-001` 必须替换为当前车辆资产编号
- 验收工具只接受本机 ROS master：运行前必须显式设置
  `ROS_MASTER_URI=http://127.0.0.1:11311`（或等价的 `http://localhost:11311`）

默认忽略告警：

- `station_status stale or missing`

### 1.1 阶段 K 全新车辆无地图门禁

仅对从未建图、从未迁移其他车辆数据且没有充电桩标定的全新车辆使用：

```bash
rosrun coverage_planner run_backend_runtime_smoke.py \
  --profile stage_k_new_vehicle_no_map \
  --robot-id CR-001 \
  --plan-db-path /data/coverage/planning.db \
  --ops-db-path /data/coverage/operations.db \
  --maps-root /data/maps \
  --dock-calibration-path /data/coverage/dock_calibration.yaml \
  --auto-charge-state-path /data/coverage/auto_charge_monitor_state.json \
  --auto-charge-event-log-path /data/coverage/auto_charge_monitor_events.jsonl \
  --text
```

阶段 K 的商业数据路径是不可替换的固定契约：`planning.db`、`operations.db`、地图根目录、
地图导入目录、充电桩标定、自动充电状态和自动充电事件日志必须分别使用
`/data/coverage/planning.db`、`/data/coverage/operations.db`、`/data/maps`、
`/data/maps/imports`、`/data/coverage/dock_calibration.yaml`、
`/data/coverage/auto_charge_monitor_state.json` 和
`/data/coverage/auto_charge_monitor_events.jsonl`。不得通过 `runtime.env`、调用终端环境、
`*_EXTRA_ARGS`、另一个路径或符号链接替换它们；任一路径不一致都是停止条件。

商业阶段 K 的验收必须由部署人员显式执行上述命令并归档输出。
`/etc/doraemon/runtime.env` 不得设置或启用 `RUN_BACKEND_RUNTIME_SMOKE`、
`RUN_REVISION_DB_HEALTH_CHECK`、`RUN_BACKEND_PRODUCTION_ACCEPTANCE`、任何自动验收 profile，
也不得设置验收 action/extra-args。后端或 systemd 启动成功不等于阶段 K 验收通过。

该 profile 要求地图、任务、动作历史和标定均为空，自动充电状态与事件日志也为空，
里程计健康，并把
`overall_ready=false`、`can_start_task=false` 作为尚未建图时的正确 fail-closed 状态。
它还会只读调用 `/clean_robot_server/app/get_dock_calibration_status`：返回的 `robot_id`
必须精确等于本车、`frame_id=map`、`storage_path` 必须固定为
`/data/coverage/dock_calibration.yaml`，`stage1_set`/`stage2_set` 必须均为 `false`，
且 saved/active/runtime 的地图 name/id/md5 必须全部为空。该服务检查前后仍由同一语义
快照证明没有生成或修改标定文件。
它要求 readiness 精确包含 `battery_state missing`、`combined_status missing` 和
`station bridge offline`。无地图时只额外允许下面这一条完整匹配的可选 warning：

```text
health warning latched: TF_LOOKUP_FAIL "map" passed to lookupTransform argument target_frame does not exist.
```

该例外只适用于本 profile 的全新无地图状态；大小写、标点或内容不同的 TF/health warning
都不得忽略。除上述三条必需 warning 和这一条精确可选 warning 外，出现任何 warning
都必须使门禁失败并停止阶段 K。
它拒绝 `--actions`、`--run-task-cycle` 和自定义 `--ignore-warning`。已有/迁移车辆、普通
smoke 失败或阶段 L 建图后均不得回退使用它。

## 2. 写动作入口

`run_backend_runtime_smoke.py` 是严格只读门禁，明确拒绝所有 `--actions` 和
`--run-task-cycle`。验证、激活、准备任务和建图均属于真实写动作，必须改用第 6 节的
`run_revision_workflow_acceptance.py` 商业流程；建图必须使用 checkpoint v2 的
`pause / inspect / resume` 会话绑定，不能用通用 smoke 拼接动作序列。

## 3. 输出语义

脚本输出分两块：

- `checks`
  - 只读状态检查
- `actions`
  - 通用 smoke 中必须为空；真实写动作由专用 workflow 单独报告

最终以 `Summary: OK/FAIL` 为准。

## 4. 当前推荐使用方式

日常回归推荐先跑：

```bash
rosrun coverage_planner check_ros_contracts.py --strict --text
rosrun coverage_planner run_backend_runtime_smoke.py --robot-id CR-001 --text
```

需要真实写动作时，先完成阶段 L 的现场批准和物理安全门，再使用第 6 节的专用入口。

## 5. 启动脚本自动 smoke

[start_runtime.sh](../scripts/start_runtime.sh) 现在会在 runtime readiness 通过后，默认追加一轮只读 smoke。

默认行为：

- `RUN_BACKEND_RUNTIME_SMOKE=1`
- 只跑只读检查
- 不下发任何动作
- `RUN_REVISION_DB_HEALTH_CHECK=0`
- revision DB 健康检查默认不自动跑
- `RUN_BACKEND_PRODUCTION_ACCEPTANCE=0`
- production acceptance gate 默认不自动跑

商业 systemd unit 明确覆盖为 `RUN_BACKEND_RUNTIME_SMOKE=0`、
`RUN_REVISION_DB_HEALTH_CHECK=0` 和 `RUN_BACKEND_PRODUCTION_ACCEPTANCE=0`；无 active map
的 degraded 启动分支也会在后置 smoke 前返回。因此商业阶段 K 必须按部署手册显式运行
对应 profile，不能把本节的脚本默认值理解为 systemd 已自动验收。

不得通过 `BACKEND_RUNTIME_SMOKE_ACTIONS` 或
`BACKEND_RUNTIME_SMOKE_EXTRA_ARGS` 把写动作挂到启动流程；通用 smoke 会硬拒绝写动作。

如果不想在启动时跑 smoke：

```bash
RUN_BACKEND_RUNTIME_SMOKE=0 scripts/start_runtime.sh
```

如果你想在启动收口前顺手检查一次 `planning.db + operations.db` 的 revision 绑定健康度：

```bash
RUN_REVISION_DB_HEALTH_CHECK=1 scripts/start_runtime.sh
```

如果你希望 warning 也直接让启动流程失败：

```bash
RUN_REVISION_DB_HEALTH_CHECK=1 REVISION_DB_HEALTH_STRICT=1 scripts/start_runtime.sh
```

如果你想把启动后的收口升级成固定顺序的只读 production gate：

```bash
RUN_BACKEND_PRODUCTION_ACCEPTANCE=1 \
BACKEND_PRODUCTION_ACCEPTANCE_PROFILE=read_only_gate \
scripts/start_runtime.sh
```

不要把 revision 写 profile 挂到自动启动流程；它们只能在阶段 L 的现场批准和物理安全门
完成后，以显式命令受控执行。

商业 production wrapper 禁止把 `start_mapping -> save_mapping` 建图整链挂到启动后
自动执行。建图必须改用下文的 `run_revision_workflow_acceptance.py` checkpoint v2
`pause / inspect / resume` 流程，由现场人员在人工推车完成后恢复，不能用启动环境变量绕过。

说明：

- 一旦 `RUN_BACKEND_PRODUCTION_ACCEPTANCE=1`，`start_runtime.sh` 会把后置收口切到 production acceptance 总入口
- 这一步已经内含 `revision db health + runtime smoke`
- 所以不会再额外单独重复跑一遍启动后置 smoke/db health

## 6. 受控 acceptance 与 production gate

只读 smoke 与受控写动作分别使用：

- `rosrun coverage_planner run_backend_runtime_smoke.py`
- `rosrun coverage_planner run_revision_workflow_acceptance.py`
- `rosrun coverage_planner run_backend_production_acceptance.py`

常用受控 profile：

```bash
rosrun coverage_planner run_backend_runtime_smoke.py --robot-id CR-001 --text
```

通用 smoke 不提供建图或其他写动作。现场建图只能使用下文 checkpoint v2 流程。

只验证 candidate revision：

```bash
rosrun coverage_planner run_revision_workflow_acceptance.py \
  --profile verify_revision \
  --robot-id CR-001 \
  --map-name site_a \
  --map-revision-id rev_demo_01 \
  --allow-write-actions \
  --text
```

激活已验证 revision 并收到 task-ready：

```bash
rosrun coverage_planner run_revision_workflow_acceptance.py \
  --profile activate_revision_prepare_for_task \
  --robot-id CR-001 \
  --task-id 1 \
  --map-name site_a \
  --map-revision-id rev_demo_01 \
  --allow-write-actions \
  --text
```

如果你要验证 revision 商业流程本身，优先看：

- `rosrun coverage_planner run_revision_workflow_acceptance.py`
- `activate_revision_prepare_for_task`
- `mapping_save_verify_activate`

如果你要按“DB health -> runtime smoke -> revision acceptance”固定顺序做现场闭环，优先看：

- `rosrun coverage_planner run_backend_production_acceptance.py`
- `activate_revision_prepare_for_task_gate`

production wrapper 只接受 `read_only_gate`、`verify_revision_gate`、
`activate_revision_gate` 和 `activate_revision_prepare_for_task_gate`。它会硬拒绝
`candidate_save_gate`、`revision_cycle_gate` 和
`revision_cycle_prepare_for_task_gate`，也不提供“预检失败后继续”的选项。
production wrapper 的 runtime smoke 是只读预检，因此同样硬拒绝 `--run-task-cycle`。

最常用的 production gate 入口可以直接这样跑。

只读 gate：

```bash
rosrun coverage_planner run_backend_production_acceptance.py \
  --profile read_only_gate \
  --robot-id CR-001 \
  --plan-db-path /data/coverage/planning.db \
  --ops-db-path /data/coverage/operations.db \
  --text
```

激活后直接拉到 task-ready：

```bash
rosrun coverage_planner run_backend_production_acceptance.py \
  --profile activate_revision_prepare_for_task_gate \
  --robot-id CR-001 \
  --task-id 1 \
  --plan-db-path /data/coverage/planning.db \
  --ops-db-path /data/coverage/operations.db \
  --map-name site_a \
  --map-revision-id rev_demo_01 \
  --allow-write-actions \
  --text
```

补充说明：

- 商业写 profile 只有在 revision DB health 与 runtime smoke 都通过后才会执行；任一预检失败都会停止写入，没有 fail-open 选项。
- 写 profile 的前置 runtime smoke 固定使用 `task_id=0`，只证明写入前的平台/runtime 基线，
  避免用尚未激活的目标 revision 任务提前阻断激活；它不算任务资产验收。正整数 task ID
  会在后续 revision acceptance 中严格核对，写动作通过后还必须用同一 task ID 重跑只读
  `task_ready`/production gate。
- `run_backend_production_acceptance.py` 不提供建图 profile，也不提供 `pause / resume checkpoint`。
- 现场建图必须先 `start_mapping`，等人工推车完成后再继续 `save_mapping`，并使用 `run_revision_workflow_acceptance.py` checkpoint v2：

```bash
rosrun coverage_planner run_revision_workflow_acceptance.py \
  --profile mapping_save_verify_activate \
  --robot-id CR-001 \
  --save-map-name acceptance_map_YYYYMMDD_HHMM \
  --allow-write-actions \
  --pause-after-start-mapping \
  --checkpoint-path /data/coverage/revision_acceptance_checkpoint.json \
  --text
```

恢复前只读检查 checkpoint：

```bash
rosrun coverage_planner run_revision_workflow_acceptance.py \
  --inspect-checkpoint \
  --checkpoint-path /data/coverage/revision_acceptance_checkpoint.json \
  --require-resumable \
  --text
```

恢复：

```bash
rosrun coverage_planner run_revision_workflow_acceptance.py \
  --profile mapping_save_verify_activate \
  --robot-id CR-001 \
  --allow-write-actions \
  --resume-from-checkpoint /data/coverage/revision_acceptance_checkpoint.json \
  --text
```

resume 成功后先记录工具返回的精确 `map_name / map_revision_id`。首张地图此时还没有与该
revision 绑定的 zone、plan 和 task，不能在同一条链里直接 `prepare_for_task`。必须先在前端
为该 revision 创建并复核 zone、plan 和 task，取得正整数 task ID；随后再运行上文的
`activate_revision_prepare_for_task --task-id <本车任务ID>`。旧的“建图后立即 prepare”一键
profile 已禁用，防止在任务资产尚不存在时先改变地图状态、最后才失败。

暂停/恢复 checkpoint 当前版本为 v2，并与本次 `start_mapping` job 强绑定：

- `--inspect-checkpoint --require-resumable` 只检查 checkpoint 文件的版本、字段和暂停阶段，
  不是动作放行。车辆身份、start job 和 live session token 的严格校验发生在实际 resume；
  inspect 显示 `Resumable: yes` 也不能替代现场批准或 resume 门禁。
- checkpoint 由工具以当前运行用户所有、`0600` 权限原子写入；目标或任一父路径是符号链接、
  文件不是普通文件、owner 不一致或存在 group/other 权限时均会拒绝。旧的宽权限 checkpoint
  不得手工改内容继续使用，应在安全现场重新开始本次流程。
- 写入 checkpoint 前，运行态参数 `/cartographer/runtime/mapping_session_id` 必须非空，且必须
  与 `start_mapping` 返回的 job ID 完全一致；不一致时不写 checkpoint。
- 恢复时会严格复核 checkpoint 内的车辆编号、job ID、操作类型、空地图作用域、描述和
  succeeded 终态，再以同一车辆编号查询 live job；任一字段不一致、job 不存在、失败或
  需要人工协助都会停止，且不会提交 `save_mapping` 或 `stop_mapping`。
- 捕获恢复现场快照前及提交 `save_mapping` 前，live mapping session token 都必须仍与该
  job ID 完全一致。SLAM runtime manager 重启会清除此 token，因此重启后不能沿用旧
  checkpoint，必须先现场确认建图状态并重新开始受控流程。
- v1 checkpoint 不具备上述会话绑定，工具会直接拒绝；不得手工把版本号改成 v2，也不得
  编辑 checkpoint 中的 job、车辆或快照字段来绕过门禁。

如果你怀疑不是运行态问题，而是数据库绑定已经串了，先跑：

- `rosrun coverage_planner check_revision_db_health.py`

最小用法：

```bash
rosrun coverage_planner check_revision_db_health.py \
  --plan-db-path /data/coverage/planning.db \
  --ops-db-path /data/coverage/operations.db \
  --text
```

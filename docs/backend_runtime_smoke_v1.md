# 后端 Runtime Smoke 回归说明 v1

## 定位说明

这份文档是当前后端 runtime 回归的主入口。

适用场景：

- 日常只读 smoke
- runtime 启动后的基础回归
- 在现场明确允许时，追加最小 workflow 动作验证

如果你只是想确认“后端主链现在是否健康”，应先看这份文档，而不是直接从更长的人工操作顺序文档开始。

## 本轮内容校验依据

本轮已按当前 smoke 与 contract 检查工具核对这份说明，重点依据包括：

- `rosrun coverage_planner run_backend_runtime_smoke.py`
- `rosrun coverage_planner run_revision_workflow_acceptance.py`
- `rosrun coverage_planner check_ros_contracts.py`
- [check_frontend_services.sh](/home/sunnybaer/Doraemon/scripts/check_frontend_services.sh)

这份文档对应统一 smoke 工具：

- `rosrun coverage_planner run_backend_runtime_smoke.py`
- `rosrun coverage_planner run_revision_workflow_acceptance.py`

目标是把后端一期最重要的 3 条主链收成一个统一入口：

- SLAM
- Odom
- System Readiness

并且在你显式允许时，顺手跑最小 workflow 动作：

- `prepare_for_task`
- `relocalize`
- `switch_map_and_localize`
- `start_mapping`
- `save_mapping`
- `stop_mapping`

## 1. 默认模式

默认只做只读 smoke，不改现场状态：

```bash
rosrun coverage_planner run_backend_runtime_smoke.py --text
```

默认检查：

- `/clean_robot_server/app/get_slam_status`
- `/clean_robot_server/app/get_odometry_status`
- `/coverage_task_manager/app/get_system_readiness`

补充说明：

- 只读 smoke 现优先走并行 `cleanrobot_app_msgs` query service
- 写动作提交仍保持 `/clean_robot_server/app/submit_slam_command`
- 异步 job 轮询改走 `/clean_robot_server/app/get_slam_job`

默认忽略告警：

- `station_status stale or missing`

## 2. 最小写动作 smoke

### 2.1 `prepare_for_task`

```bash
rosrun coverage_planner run_backend_runtime_smoke.py --actions prepare_for_task --text
```

### 2.2 `relocalize`

```bash
rosrun coverage_planner run_backend_runtime_smoke.py --actions relocalize --text
```

### 2.3 mapping 闭环

```bash
rosrun coverage_planner run_backend_runtime_smoke.py \
  --actions start_mapping,save_mapping,stop_mapping \
  --save-map-name codex_smoke_map_YYYYMMDD_HHMM \
  --text
```

注意：

- `start_mapping / save_mapping / stop_mapping` 会真实改现场状态
- 只有在现场允许时再跑

## 3. 输出语义

脚本输出分两块：

- `checks`
  - 只读状态检查
- `actions`
  - 可选动作 job 执行结果

最终以 `Summary: OK/FAIL` 为准。

## 4. 当前推荐使用方式

日常回归推荐先跑：

```bash
rosrun coverage_planner check_ros_contracts.py --strict --text
rosrun coverage_planner run_backend_runtime_smoke.py --text
```

如果要做最小 live workflow 验证，再加：

```bash
rosrun coverage_planner run_backend_runtime_smoke.py --actions prepare_for_task --text
```

## 5. 启动脚本自动 smoke

[start_runtime.sh](/home/sunnybaer/Doraemon/scripts/start_runtime.sh) 现在会在 runtime readiness 通过后，默认追加一轮只读 smoke。

默认行为：

- `RUN_BACKEND_RUNTIME_SMOKE=1`
- 只跑只读检查
- 不下发任何动作
- `RUN_REVISION_DB_HEALTH_CHECK=0`
- revision DB 健康检查默认不自动跑
- `RUN_BACKEND_PRODUCTION_ACCEPTANCE=0`
- production acceptance gate 默认不自动跑

如果你明确想在启动后顺手做最小动作验证，可以显式传环境变量：

```bash
BACKEND_RUNTIME_SMOKE_ACTIONS=prepare_for_task scripts/start_runtime.sh
```

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

如果你想把启动后的收口直接升级成固定顺序的 production gate：

```bash
RUN_BACKEND_PRODUCTION_ACCEPTANCE=1 \
BACKEND_PRODUCTION_ACCEPTANCE_PROFILE=activate_revision_prepare_for_task_gate \
BACKEND_PRODUCTION_ACCEPTANCE_EXTRA_ARGS='--map-revision-id rev_demo_01' \
scripts/start_runtime.sh
```

如果要在真机现场把 `save -> verify -> activate -> prepare_for_task` 整链也挂到启动后自动验收，需要再显式允许写动作：

```bash
RUN_BACKEND_PRODUCTION_ACCEPTANCE=1 \
BACKEND_PRODUCTION_ACCEPTANCE_PROFILE=revision_cycle_prepare_for_task_gate \
BACKEND_PRODUCTION_ACCEPTANCE_ALLOW_WRITE_ACTIONS=1 \
BACKEND_PRODUCTION_ACCEPTANCE_EXTRA_ARGS='--save-map-name acceptance_map_YYYYMMDD_HHMM' \
scripts/start_runtime.sh
```

说明：

- 一旦 `RUN_BACKEND_PRODUCTION_ACCEPTANCE=1`，`start_runtime.sh` 会把后置收口切到 production acceptance 总入口
- 这一步已经内含 `revision db health + runtime smoke`
- 所以不会再额外单独重复跑一遍启动后置 smoke/db health

## 6. 受控 acceptance 与 production gate

如果你想把最小动作验收也收成固定入口，直接用：

- `rosrun coverage_planner run_backend_runtime_smoke.py`
- `rosrun coverage_planner run_revision_workflow_acceptance.py`

常用受控 profile：

```bash
rosrun coverage_planner run_backend_runtime_smoke.py --text
rosrun coverage_planner run_backend_runtime_smoke.py --actions relocalize --text
rosrun coverage_planner run_backend_runtime_smoke.py --actions prepare_for_task --text
```

如果现场允许建图写动作：

```bash
rosrun coverage_planner run_backend_runtime_smoke.py \
  --actions start_mapping,save_mapping,stop_mapping \
  --save-map-name acceptance_map_YYYYMMDD_HHMM \
  --text
```

只验证 candidate revision：

```bash
rosrun coverage_planner run_revision_workflow_acceptance.py \
  --profile verify_revision \
  --map-revision-id rev_demo_01 \
  --allow-write-actions \
  --text
```

激活已验证 revision 并收到 task-ready：

```bash
rosrun coverage_planner run_revision_workflow_acceptance.py \
  --profile activate_revision_prepare_for_task \
  --map-revision-id rev_demo_01 \
  --allow-write-actions \
  --text
```

如果你要验证 revision 商业流程本身，优先看：

- `rosrun coverage_planner run_revision_workflow_acceptance.py`
- `activate_revision_prepare_for_task`
- `mapping_save_verify_activate_prepare_for_task`

如果你要按“DB health -> runtime smoke -> revision acceptance”固定顺序做现场闭环，优先看：

- `rosrun coverage_planner run_backend_production_acceptance.py`
- `activate_revision_prepare_for_task_gate`
- `revision_cycle_prepare_for_task_gate`

最常用的 production gate 入口可以直接这样跑。

只读 gate：

```bash
rosrun coverage_planner run_backend_production_acceptance.py \
  --profile read_only_gate \
  --plan-db-path /data/coverage/planning.db \
  --ops-db-path /data/coverage/operations.db \
  --text
```

激活后直接拉到 task-ready：

```bash
rosrun coverage_planner run_backend_production_acceptance.py \
  --profile activate_revision_prepare_for_task_gate \
  --plan-db-path /data/coverage/planning.db \
  --ops-db-path /data/coverage/operations.db \
  --map-revision-id rev_demo_01 \
  --allow-write-actions \
  --text
```

从新图建图一路跑到 task-ready：

```bash
rosrun coverage_planner run_backend_production_acceptance.py \
  --profile revision_cycle_prepare_for_task_gate \
  --plan-db-path /data/coverage/planning.db \
  --ops-db-path /data/coverage/operations.db \
  --save-map-name acceptance_map_YYYYMMDD_HHMM \
  --allow-write-actions \
  --text
```

补充说明：

- `run_backend_production_acceptance.py` 主打固定顺序的一次性 gate，不提供 `pause / resume checkpoint`
- 如果现场需要先 `start_mapping`，等人工推车完成后再继续 `save_mapping`，直接用 `run_revision_workflow_acceptance.py`：

```bash
rosrun coverage_planner run_revision_workflow_acceptance.py \
  --profile mapping_save_verify_activate_prepare_for_task \
  --save-map-name acceptance_map_YYYYMMDD_HHMM \
  --allow-write-actions \
  --pause-after-start-mapping \
  --checkpoint-path /tmp/revision_acceptance_checkpoint.json \
  --text
```

恢复前只读检查 checkpoint：

```bash
rosrun coverage_planner run_revision_workflow_acceptance.py \
  --inspect-checkpoint \
  --checkpoint-path /tmp/revision_acceptance_checkpoint.json \
  --require-resumable \
  --text
```

恢复：

```bash
rosrun coverage_planner run_revision_workflow_acceptance.py \
  --profile mapping_save_verify_activate_prepare_for_task \
  --allow-write-actions \
  --resume-from-checkpoint /tmp/revision_acceptance_checkpoint.json \
  --text
```

如果你怀疑不是运行态问题，而是数据库绑定已经串了，先跑：

- `rosrun coverage_planner check_revision_db_health.py`

最小用法：

```bash
rosrun coverage_planner check_revision_db_health.py \
  --plan-db-path /data/coverage/planning.db \
  --ops-db-path /data/coverage/operations.db \
  --text
```

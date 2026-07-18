# 充电桩两点标定前端接口

## 目标

前端提供一个“充电桩标定”页面，让现场人员通过手动点动小车，观察当前地图位姿、梯形桩识别质量和两个预对接点状态，然后保存：

- 第一预对接点：`dock_stage1_xyyaw`
- 第二预对接点：`dock_xyyaw`

第二预对接点的识别质量由前端展示给现场人员判断；后端默认不强制拦截保存。

## 关键 ROS 接口

### 状态话题

- `/clean_robot_server/dock_calibration_state`
- 类型：`cleanrobot_app_msgs/DockCalibrationState`

建议前端或 Site Gateway 以 1-2Hz 订阅/轮询展示。

关键字段：

- `tracked_pose_fresh`
- `tracked_pose_frame`
- `current_x / current_y / current_yaw`
- `stage1_set / stage1_x / stage1_y / stage1_yaw`
- `stage2_set / stage2_x / stage2_y / stage2_yaw`
- `dock_pose_fresh`
- `dock_pose_x / dock_pose_y / dock_pose_yaw`
- `dock_score_fresh`
- `dock_score`
- `dock_score_threshold`
- `dock_score_lower_is_better`
- `dock_pose_quality_ok`
- `stage2_save_recommended`
- `dock_target_dist`
- `dock_xy_tolerance`
- `dock_success_threshold`
- `warnings[]`
- `storage_path`

### 查询服务

- `/clean_robot_server/app/get_dock_calibration_status`
- 类型：`cleanrobot_app_msgs/GetDockCalibrationStatus`

请求：

```yaml
robot_id: "local_robot"
```

响应：

```yaml
success: true
message: "ok"
state: DockCalibrationState
```

### 操作服务

- `/clean_robot_server/app/dock_calibration_command`
- 类型：`cleanrobot_app_msgs/OperateDockCalibration`

操作枚举：

- `GET = 0`
- `SAVE_STAGE1 = 1`
- `SAVE_STAGE2 = 2`
- `SET_STAGE1 = 3`
- `SET_STAGE2 = 4`
- `RELOAD = 5`
- `SET_DOCK_PARAMS = 6`

保存当前位姿为第一预对接点：

```yaml
operation: 1
robot_id: "local_robot"
require_stage2_quality: false
```

保存当前位姿为第二预对接点：

```yaml
operation: 2
robot_id: "local_robot"
require_stage2_quality: false
```

如果前端想强制后端按推荐质量拦截第二点，可以把 `require_stage2_quality` 设为 `true`。当前建议默认 `false`，由现场人员根据界面判断。

设置精对接停车/成功参数：

```yaml
operation: 6   # SET_DOCK_PARAMS
robot_id: "local_robot"
dock_target_dist: 0.780
dock_xy_tolerance: 0.005
```

参数含义：

- `dock_target_dist`：精对接目标距离，单位 m。
- `dock_xy_tolerance`：精对接距离容差，单位 m。
- `dock_success_threshold = dock_target_dist + dock_xy_tolerance`。
- 想让小车距离充电桩更远：调大 `dock_target_dist`，或小幅调大 `dock_xy_tolerance`。
- 想让小车更靠近充电桩：调小 `dock_target_dist`，或调小 `dock_xy_tolerance`。
- 现场建议优先微调 `dock_target_dist`，每次 0.005 m；`dock_xy_tolerance` 建议保持较小，例如 0.005 m。

手动写入某个点：

```yaml
operation: 3   # SET_STAGE1
robot_id: "local_robot"
x: 1.0
y: 2.0
yaw: 1.57
```

```yaml
operation: 4   # SET_STAGE2
robot_id: "local_robot"
x: 1.2
y: 2.1
yaw: 1.57
```

## 识别分数

`dock_tracker` 现在发布：

- `/dock_pose_score`
- 类型：`std_msgs/Float32`
- 语义：ICP fitness score，越小越好

成功识别时仍发布 `/dock_pose`。前端判断第二预对接点时建议同时展示：

- score 当前值
- score 阈值 `dock_score_threshold`
- `/dock_pose` 是否 fresh
- `dock_pose_x / dock_pose_y / dock_pose_yaw`
- `stage2_save_recommended`
- `warnings[]`

## 推荐前端流程

1. 页面进入后持续展示当前小车地图位姿。
2. 现场人员手动点动小车到第一预对接点。
3. 点击“保存第一预对接点”，调用 `SAVE_STAGE1`。
4. 现场人员手动点动小车到第二预对接点。
5. 页面展示梯形桩识别 score、相对位姿、推荐状态和 warnings。
6. 现场人员确认质量后点击“保存第二预对接点”，调用 `SAVE_STAGE2`。
7. 提供“开始回桩验证”按钮，调用现有任务命令 `dock`。
8. 展示任务状态中的 `MANUAL_DOCKING_STAGE1 / MANUAL_DOCKING_STAGE2 / MANUAL_DOCKING_PRECISE`。

## 给前端 Codex 的提示词

请在前端新增“充电桩标定”功能页。页面面向现场工程师，不做营销说明，直接提供可操作界面。接入后端：

- 轮询或订阅 `/clean_robot_server/dock_calibration_state`，或通过 `GET /api/dock-calibration/status` 获取 `DockCalibrationState`。
- 调用 `POST /api/dock-calibration/command`，映射 ROS 服务 `/clean_robot_server/app/dock_calibration_command`。
- 操作枚举：`SAVE_STAGE1=1`，`SAVE_STAGE2=2`，`SET_STAGE1=3`，`SET_STAGE2=4`，`RELOAD=5`。

页面布局：

- 顶部显示定位状态：`tracked_pose_fresh`、`tracked_pose_frame`、当前 `x/y/yaw`。
- 中部左右两块分别显示第一预对接点和第二预对接点的已保存坐标，并提供“保存当前位姿”按钮。
- 第二预对接点区域必须突出显示梯形桩识别质量：`dock_score`、`dock_score_threshold`、`dock_score_lower_is_better`、`dock_pose_fresh`、`dock_pose_x/y/yaw`、`stage2_save_recommended`、`warnings[]`。
- 页面增加“精对接参数调试”区域，展示 `dock_target_dist`、`dock_xy_tolerance`、`dock_success_threshold`，并提供输入框保存 `SET_DOCK_PARAMS=6`。
- 参数说明必须放在输入框旁边：成功/停车阈值 = `dock_target_dist + dock_xy_tolerance`；调大参数会让小车更早停车、距离充电桩更远，调小参数会让小车更靠近充电桩。
- score 用颜色表达：fresh 且 `dock_score <= dock_score_threshold` 为通过；stale 或超阈值为警告。注意 score 越小越好。
- 提供手动输入 `x/y/yaw` 的高级折叠区，分别调用 `SET_STAGE1/SET_STAGE2`。
- 默认保存第二点时传 `require_stage2_quality=false`，让现场人员人工确认；可以提供一个“强制按推荐质量校验”的开关，打开时传 `true`。
- 接入现有手动点动控制，让工程师在同一页面微调小车位置。
- 提供“回桩验证”按钮，调用现有任务命令 `dock`，并显示任务状态流转。

交互要求：

- 当 `tracked_pose_fresh=false` 或 `tracked_pose_frame!="map"` 时，保存按钮禁用。
- 保存成功后立即刷新状态。
- warnings 不要隐藏，直接显示为现场调整建议。
- 所有角度同时显示弧度和角度。

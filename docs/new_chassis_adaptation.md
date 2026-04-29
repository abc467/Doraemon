# 新运动底盘适配记录

本文记录 Doraemon 当前运动底盘链路和新底盘接入开关，避免新底盘适配时破坏已经验证过的老 M-core 底盘。

## 当前运动链路

上层导航和任务系统统一使用 ROS 标准接口：

- 运动命令：`/cmd_vel`，消息类型 `geometry_msgs/Twist`
- 主里程计：`/odom`，消息类型 `nav_msgs/Odometry`
- 原始轮速里程计：`/odom_raw`
- 机器人坐标系：`odom -> base_footprint -> base_link`

老底盘默认链路：

1. MBF / docking / executor 发布 `/cmd_vel`。
2. `robot_hw_bridge/scripts/mcore_tcp_bridge.py` 订阅 `/cmd_vel`。
3. M-core TCP 协议用 `0x4060` 下发 `linear.x` 和 `angular.z` 两个 float。
4. `/dev/odom` 串口由 `wheel_speed_odom_node` 解码，EKF 输出 `/odom`。

## 已提供的兼容开关

`hardware_bridges.launch` 现在可以独立控制 M-core 是否接管运动：

```bash
roslaunch robot_hw_bridge hardware_bridges.launch \
  enable_mcore_bridge:=true \
  mcore_enable_cmd_vel:=false
```

这种模式适合“新运动底盘负责 `/cmd_vel`，老 M-core 仍负责清洁/电池/补给状态”的过渡接入。

如果新底盘完全替代 M-core：

```bash
roslaunch robot_hw_bridge hardware_bridges.launch \
  enable_mcore_bridge:=false \
  enable_station_bridge:=false \
  enable_dock_supply_manager:=false \
  enable_docking_stack:=false
```

如果新底盘仍使用 M-core 协议，但速度方向或比例不同，可以只调参数：

```bash
roslaunch robot_hw_bridge hardware_bridges.launch \
  mcore_linear_velocity_sign:=-1.0 \
  mcore_angular_velocity_sign:=1.0 \
  mcore_linear_velocity_scale:=1.0 \
  mcore_angular_velocity_scale:=1.0 \
  mcore_max_abs_linear_velocity:=0.35 \
  mcore_max_abs_angular_velocity:=0.30
```

`start_runtime.sh` 对应环境变量：

```bash
MCORE_ENABLE_CMD_VEL=false scripts/start_runtime.sh
```

新底盘自己提供 `/odom` 时，可以关闭老轮速里程计：

```bash
START_WHEEL_ODOM=false scripts/start_runtime.sh
```

如果 systemd 开机等待阶段不再有 `/dev/odom`，需要同步设置：

```bash
DORAEMON_REQUIRE_ODOM_DEVICE=false
```

## 轮趣 senior_diff 接入方式

已接入的新底盘模式：

```bash
CHASSIS_DRIVER=wheeltec_senior_diff scripts/start_runtime.sh
```

该模式默认执行以下切换：

- 启动 `robot_hw_bridge/launch/wheeltec_senior_diff_base.launch`。
- 使用 `turn_on_wheeltec_robot` 的 `wheeltec_robot_node` 订阅 `/cmd_vel`。
- 由轮趣底盘发布 `/odom`、`/imu`，并打开 `odom -> base_footprint` TF。
- 使用 `src/turn_on_wheeltec_robot/urdf/senior_diff_robot.urdf` 作为机器人模型。
- 该 URDF 已移除深度相机 `camera_link` / `camera_joint`，当前实车不再安装深度相机。
- 激光雷达为万集 WLR-719，安装在左右差速轮连线中心附近，朝向与车头一致。当前按现场微调后设为 `base_footprint -> laser = (-0.117945, 0.001155, 0.311), yaw=0`。其中 z=0.311m 按雷达底部离地 0.275m、WLR-719 外形高度约 0.062m、扫描平面暂取机身中心估算，再按现场要求上移 0.005m；后续若测到真实扫描窗中心高度，应同步修正 `laser_joint`。
- 发布 `base_footprint -> base_link` 静态 TF，z 高度为 `0.0374`。
- 发布 `base_footprint -> gyro_link` 静态 TF，满足当前 Cartographer `tracking_frame=gyro_link`。
- 启动无界面的 `joint_state_publisher`，给左右差速轮和后万向轮发布零位姿关节状态，避免 RViz 中 `left_wheel_link` / `right_wheel_link` / `quanxiang_wheel_link` 缺少 TF。
- 关闭旧 `/dev/odom` 轮速里程计、旧 AHRS 校正链、M-core 运动桥、补给站桥、docking stack、Orbbec 深度相机和深度避障链的默认启动。
- 里程计健康检查改用轮趣底盘的 `/imu`，不再等待旧 `/imu_corrected`。
- 任务 readiness 不再把旧 M-core 桥作为硬阻塞；M-core 离线只显示为诊断 WARN。

首次在新车架上部署时，旧活动地图对应的是旧车体和旧激光雷达外参，不应该作为启动硬门槛。可以先关闭开机自动重定位，让后端、网页网关、建图服务先正常启动：

```bash
CHASSIS_DRIVER=wheeltec_senior_diff \
STARTUP_RELOCALIZE_ENABLE=false \
scripts/start_runtime.sh
```

这个模式只表示“开机不强行匹配旧活动地图”。清扫任务仍然需要等新地图创建、激活并完成定位后才能启动。新车架地图验证完成后，把 `STARTUP_RELOCALIZE_ENABLE` 恢复为 `true`，日常开机就会继续自动重定位到活动地图。

常用现场参数：

```bash
CHASSIS_DRIVER=wheeltec_senior_diff \
WHEELTEC_SERIAL_DEVICE=/dev/wheeltec_controller \
scripts/start_runtime.sh
```

systemd 开机等待阶段可同步使用：

```bash
DORAEMON_CHASSIS_DRIVER=wheeltec_senior_diff
DORAEMON_CHASSIS_DEVICE=/dev/serial/by-id/usb-WCH.CN_USB_Single_Serial_0002-if00
```

`deploy/systemd/doraemon-runtime.service` 支持可选环境文件 `/etc/doraemon/runtime.env`。新主控板部署轮趣底盘时，可以参考 `deploy/systemd/runtime.env.wheeltec.example`。如果希望继续使用 `/dev/wheeltec_controller` 这个短名字，可安装 `deploy/udev/99-doraemon-wheeltec.rules`。

老底盘不设置 `CHASSIS_DRIVER` 时仍走 `legacy_mcore` 默认链路。

## 新底盘需要确认的信息

真正写新底盘驱动前，需要底盘厂家或协议文档确认：

1. 底盘是否直接订阅 ROS `/cmd_vel`，还是需要串口/CAN/TCP 转协议。
2. 通信方式：串口设备名、波特率；或 CAN 接口/ID；或 TCP IP/端口。
3. 速度单位：线速度是否 m/s，角速度是否 rad/s。
4. 正方向：`linear.x > 0` 是否前进，`angular.z > 0` 是否逆时针左转。
5. 安全机制：底盘是否需要心跳，多久没收到命令会自动停车。
6. 里程计来源：是否提供 ROS `/odom`，还是提供轮速/编码器，需要本工程积分。
7. 机器人几何：轮距、轮径、编码器分辨率、最大线速度、最大角速度。
8. 是否还需要老 M-core 的清洁、补水、回充、电池状态。

## 适配原则

优先保持上层接口不变：

- 新底盘驱动只需要消费 `/cmd_vel`。
- 新底盘驱动必须发布 `/odom` 或提供可被转换成 `/odom` 的原始数据。
- 上层建图、定位、任务、前端不直接依赖底盘私有协议。

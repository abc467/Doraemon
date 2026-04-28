# Doraemon ARM64 Ubuntu 20.04 编译部署记录

本文档基于 2026-04-25 在 RK3588/ARM64 设备上实际编译 `github.com/abc467/Doraemon` 的过程整理，目标是让新的 Ubuntu 系统可以按阶段复现 Doraemon 工作空间部署。

## 1. 适用范围

- CPU 架构：`aarch64`
- 系统版本：Ubuntu 20.04 `focal`
- ROS：Noetic
- Python：3.8
- 工作空间：`/home/linaro/Doraemon`
- 第三方主安装路径：`/opt/doraemon/deps`
- 本次验证仓库：`https://github.com/abc467/Doraemon.git`
- 本次验证提交：`5a0ae7c5d9ef5aa1fa98ec6f082057a99e75d7f1`

不要从其他机器复制 `.so`、`build`、`devel` 等产物。OR-Tools、Fields2Cover、Doraemon 都应在目标 ARM64 主机上构建或安装。

## 2. 系统检查

先确认系统符合要求，不符合就先停止。

```bash
uname -m
lsb_release -a
python3 --version
df -h /
free -h
swapon --show
```

期望结果：

- `uname -m` 必须是 `aarch64`
- Ubuntu codename 为 `focal`
- Python 为 `3.8.x`
- 磁盘建议至少预留 20 GB
- 内存较小的板卡建议准备 swap；如果确认不建 swap，也要避免并发过高

## 3. 基础环境

建议先把 Ubuntu 和 ROS APT 源配置好。ARM64 Ubuntu 20.04 推荐使用 `ubuntu-ports` 镜像源，国内环境可使用清华源。

如果出现旧 ROS 源报错，例如：

```text
E: 仓库 "https://packages.ros.org/ros/ubuntu focal Release" 没有 Release 文件
```

先移除或备份旧的 `/etc/apt/sources.list.d/ros1-latest.list`，再重新配置可用 ROS Noetic 源和 key。之前还遇到过 ROS key 缺失：

```text
NO_PUBKEY F42ED6FBAB17C654
```

这类错误本质是 ROS APT 源和 key 不匹配，先修源和 key，再执行 `apt update`。

安装基础工具：

```bash
sudo apt update
sudo apt install -y \
  git curl ca-certificates gnupg lsb-release \
  build-essential cmake ninja-build pkg-config \
  python3 python3-pip python3-dev python3-catkin-tools \
  python3-vcstool python3-empy python3-numpy \
  patchelf
```

安装 ROS Noetic：

```bash
sudo apt install -y ros-noetic-desktop
echo 'source /opt/ros/noetic/setup.bash' >> ~/.bashrc
source /opt/ros/noetic/setup.bash
```

不要在执行 ROS `setup.bash` 的 shell 里开启 `set -u`，ROS 脚本内部会读取一些可能未定义的变量。

## 4. rosdepc

国内网络下建议使用 `rosdepc`，不要直接依赖 `rosdep`。

```bash
sudo pip3 install -U rosdepc
sudo rosdepc init || true
rosdepc update
```

如果 `rosdepc update` 中间有个别镜像超时，但最后提示更新完成，可以继续；后续缺包时再按编译错误补装。

## 5. 第三方依赖

Doraemon 当前编译依赖以下几类第三方库。

### 5.1 OR-Tools v9.9

安装路径必须是：

```bash
/opt/doraemon/deps/ortools-9.9
```

验收：

```bash
file /opt/doraemon/deps/ortools-9.9/lib/libortools.so.9.9.9999
find /opt/doraemon/deps/ortools-9.9/lib/cmake -name 'ortoolsConfig.cmake' -print
```

`file` 必须显示 `ARM aarch64`。

### 5.2 Fields2Cover v2.0.0

安装路径必须是：

```bash
/opt/doraemon/deps/fields2cover-2.0.0
```

验收：

```bash
python3 -c "import fields2cover; print('OK', fields2cover.__file__)"
find /opt/doraemon/deps/fields2cover-2.0.0/lib/cmake -name 'Fields2CoverConfig.cmake' -print
```

若 Python import 失败，需要检查 `.pth`、`RPATH/RUNPATH` 和 `ldd`。

### 5.3 Cartographer/FLIRT/snap7/absl

本次 Doraemon 编译环境中还使用了：

```bash
/usr/local/lib/cmake/absl/abslConfig.cmake
/usr/local/lib/libflirtlib_feature.so
/usr/local/lib/libflirtlib_geometry.so
/usr/local/lib/libflirtlib_sensors.so
/usr/local/lib/libflirtlib_sensorstream.so
/usr/local/lib/libflirtlib_utils.so
/usr/local/lib/libsnap7.so
```

验收：

```bash
ldconfig -p | grep -E 'libflirtlib|libsnap7|libabsl' | head
ls /usr/local/lib/cmake/absl/abslConfig.cmake
```

如果新系统没有这些库，需要在本机 ARM64 上源码构建安装，不能复制其他机器的库文件。

## 6. Doraemon 依赖包

先安装这次实际编译过程中补齐过的依赖：

```bash
sudo apt install -y \
  libudev-dev libdw-dev libusb-1.0-0-dev \
  ros-noetic-pcl-ros \
  ros-noetic-tf2-sensor-msgs \
  ros-noetic-costmap-2d \
  ros-noetic-base-local-planner \
  ros-noetic-camera-info-manager \
  ros-noetic-image-geometry \
  ros-noetic-image-transport \
  ros-noetic-nodelet \
  ros-noetic-diagnostic-updater \
  ros-noetic-move-base-msgs
```

如果需要启动 `coverage_planner/frontend_editor_backend.launch` 的默认 rosbridge，则还需要：

```bash
sudo apt install -y ros-noetic-rosbridge-server
```

如果只是做本机 contract smoke，可用 `start_rosbridge:=false` 跳过 rosbridge。

## 7. 拉取 Doraemon

```bash
cd /home/linaro
git clone https://github.com/abc467/Doraemon.git
cd /home/linaro/Doraemon
git checkout main
git rev-parse HEAD
```

本次验证提交：

```text
5a0ae7c5d9ef5aa1fa98ec6f082057a99e75d7f1
```

## 8. 编译前源码适配

本次实际遇到两个源码/路径问题。如果这些修复还没有进入仓库，新环境需要先处理。

### 8.1 FLIRT ShapeContext 成员访问

错误现象：`cartographer` 编译时访问 `ShapeContext::m_histogram`，但该成员在当前 FLIRT 头文件中不可直接访问。

修复位置：

```text
src/cartographer/cartographer/mapping/trajectory_node.cc
```

修复方式：使用 `getHistogram()` 获取引用。

```cpp
auto& histogram = desc->getHistogram();
histogram.reserve(pp.desc().histogram().size());
...
histogram.emplace_back(std::move(v));
```

### 8.2 vanjee_driverConfig.cmake 硬编码路径

错误风险：文件里写死了其他机器路径：

```text
/home/sunnybaer/Doraemon
```

修复位置：

```text
src/vanjee_lidar_sdk/src/vanjee_driver/cmake/vanjee_driverConfig.cmake
```

临时修复为当前工作空间路径：

```text
/home/linaro/Doraemon
```

更推荐后续把该文件改成相对路径或由 CMake 自动生成，避免新机器继续踩坑。

## 9. 配置 catkin

```bash
cd /home/linaro/Doraemon
source /opt/ros/noetic/setup.bash
catkin init
catkin config --extend /opt/ros/noetic
catkin config --cmake-args \
  -DFields2Cover_DIR=/opt/doraemon/deps/fields2cover-2.0.0/lib/cmake/Fields2Cover \
  -Dortools_DIR=/opt/doraemon/deps/ortools-9.9/lib/cmake/ortools
```

确认配置：

```bash
catkin config
```

关键项应包含：

```text
Extending: /opt/ros/noetic
Additional CMake Args:
  -DFields2Cover_DIR=/opt/doraemon/deps/fields2cover-2.0.0/lib/cmake/Fields2Cover
  -Dortools_DIR=/opt/doraemon/deps/ortools-9.9/lib/cmake/ortools
```

## 10. 编译

建议先用 `-j4`，ARM 板卡更稳。

```bash
mkdir -p /opt/doraemon/logs
cd /home/linaro/Doraemon
source /opt/ros/noetic/setup.bash
catkin build -j4 2>&1 | tee /opt/doraemon/logs/doraemon_catkin_build.log
```

成功标志：

```text
[build] Summary: All 34 packages succeeded!
[build] Failed: No packages failed.
```

本次实际成功日志：

```text
/opt/doraemon/logs/75_doraemon_catkin_build_j4_retry6.log
```

编译完成后：

```bash
source /home/linaro/Doraemon/devel/setup.bash
```

## 11. 编译后快速检查

```bash
source /opt/ros/noetic/setup.bash
source /home/linaro/Doraemon/devel/setup.bash
rospack find coverage_planner
rospack find coverage_task_manager
rosrun coverage_planner check_ros_contracts.py --help
rosrun coverage_planner run_backend_runtime_smoke.py --help
```

这些命令能找到包和脚本，说明 workspace overlay 基本正常。

## 12. Contract smoke

单独运行 `check_ros_contracts.py` 前，需要先启动能注册服务和 contract 参数的后端节点。

开一个终端启动 ROS master：

```bash
source /opt/ros/noetic/setup.bash
source /home/linaro/Doraemon/devel/setup.bash
ROS_MASTER_URI=http://127.0.0.1:11311 roscore
```

第二个终端启动 coverage planner 后端：

```bash
source /opt/ros/noetic/setup.bash
source /home/linaro/Doraemon/devel/setup.bash
mkdir -p /tmp/doraemon_smoke/coverage /tmp/doraemon_smoke/maps /tmp/doraemon_smoke/maps/imports
ROS_MASTER_URI=http://127.0.0.1:11311 roslaunch coverage_planner frontend_editor_backend.launch \
  start_rosbridge:=false \
  plan_db_path:=/tmp/doraemon_smoke/coverage/planning.db \
  ops_db_path:=/tmp/doraemon_smoke/coverage/operations.db \
  maps_root:=/tmp/doraemon_smoke/maps \
  external_maps_root:=/tmp/doraemon_smoke/maps/imports \
  enable_rect_zone_planner:=true
```

第三个终端启动 task system 的最小软件后端：

```bash
source /opt/ros/noetic/setup.bash
source /home/linaro/Doraemon/devel/setup.bash
ROS_MASTER_URI=http://127.0.0.1:11311 roslaunch coverage_task_manager task_system.launch \
  plan_db_path:=/tmp/doraemon_smoke/coverage/planning.db \
  ops_db_path:=/tmp/doraemon_smoke/coverage/operations.db \
  maps_root:=/tmp/doraemon_smoke/maps \
  dock_supply_enable:=false \
  start_odometry_health:=false \
  start_localization_lifecycle_manager:=false \
  require_managed_map_asset:=false \
  require_odometry_healthy_before_start:=false \
  scheduler_enable:=false \
  load_schedules_yaml:=false
```

第四个终端补启动 restart localization contract：

```bash
source /opt/ros/noetic/setup.bash
source /home/linaro/Doraemon/devel/setup.bash
ROS_MASTER_URI=http://127.0.0.1:11311 rosrun coverage_planner localization_lifecycle_manager_node.py \
  __name:=localization_lifecycle_manager \
  _plan_db_path:=/tmp/doraemon_smoke/coverage/planning.db \
  _ops_db_path:=/tmp/doraemon_smoke/coverage/operations.db \
  _robot_id:=local_robot \
  _runtime_ns:=/cartographer/runtime \
  _app_service_name:=/cartographer/runtime/app/restart_localization \
  _app_contract_param_ns:=/cartographer/runtime/contracts/app/restart_localization \
  _map_topic:=/map \
  _tracked_pose_topic:=/tracked_pose
```

确认服务已注册：

```bash
source /opt/ros/noetic/setup.bash
source /home/linaro/Doraemon/devel/setup.bash
ROS_MASTER_URI=http://127.0.0.1:11311 rosservice list | grep -E 'get_slam_status|exe_task_server|map_server|restart_localization'
```

运行 contract 检查：

```bash
source /opt/ros/noetic/setup.bash
source /home/linaro/Doraemon/devel/setup.bash
ROS_MASTER_URI=http://127.0.0.1:11311 rosrun coverage_planner check_ros_contracts.py \
  2>&1 | tee /opt/doraemon/logs/coverage_planner_ros_contracts.log
```

成功标志：

```json
"summary": {
  "issues": [],
  "ok": true,
  "warnings": []
}
```

本次实际通过日志：

```text
/opt/doraemon/logs/85_coverage_planner_ros_contracts_full_backend.log
```

## 13. Runtime smoke

用户指定命令：

```bash
source /opt/ros/noetic/setup.bash
source /home/linaro/Doraemon/devel/setup.bash
ROS_MASTER_URI=http://127.0.0.1:11311 rosrun coverage_planner run_backend_runtime_smoke.py --text
```

注意：这个 smoke 不是纯编译 smoke，它会检查真实运行态健康：

- `/odom`
- `/map` identity
- `/tracked_pose`
- `move_base_flex`
- `mcore_tcp_bridge`
- 电池、组合状态、station 等运行输入

本次在没有真实机器人/仿真运行输入的情况下，服务已经注册，但 runtime smoke 失败是预期结果：

```text
Summary: FAIL
- slam_status: localization_valid=false; runtime_map_ready=false; runtime_map_match=false
- odometry_status: odom_valid=false; odom_stream_ready=false
- system_readiness: move_base_flex is offline, mcore bridge not ready
```

因此部署判断建议分两级：

- 编译部署验收：`catkin build` 成功 + `check_ros_contracts.py` 的 `summary.ok=true`
- 真实运行验收：启动完整机器人或仿真后，再要求 `run_backend_runtime_smoke.py --text` 通过

## 14. 常见问题

### 14.1 catkin 没有 extend ROS

现象：

```text
The catkin CMake module was not found
Workspace is not extending any other result space
```

处理：

```bash
source /opt/ros/noetic/setup.bash
cd /home/linaro/Doraemon
catkin config --extend /opt/ros/noetic
catkin build -j4
```

### 14.2 Fields2Cover 或 OR-Tools 找不到

现象：`find_package(Fields2Cover)` 或 `find_package(ortools)` 失败。

处理：

```bash
catkin config --cmake-args \
  -DFields2Cover_DIR=/opt/doraemon/deps/fields2cover-2.0.0/lib/cmake/Fields2Cover \
  -Dortools_DIR=/opt/doraemon/deps/ortools-9.9/lib/cmake/ortools
```

并确认：

```bash
ls /opt/doraemon/deps/fields2cover-2.0.0/lib/cmake/Fields2Cover/Fields2CoverConfig.cmake
ls /opt/doraemon/deps/ortools-9.9/lib/cmake/ortools/ortoolsConfig.cmake
```

### 14.3 缺 ROS 包

现象：CMake 报找不到 `pcl_ros`、`tf2_sensor_msgs`、`costmap_2d`、`base_local_planner`、`image_transport` 等。

处理：补装第 6 节的 ROS 包后重新编译。

### 14.4 rosbridge 找不到

现象：启动 `frontend_editor_backend.launch` 时找不到 `rosbridge_server`。

处理：

```bash
sudo apt install -y ros-noetic-rosbridge-server
```

或者 smoke 时显式关闭：

```bash
roslaunch coverage_planner frontend_editor_backend.launch start_rosbridge:=false
```

### 14.5 runtime smoke 等服务卡住

如果只启动了 `roscore`，没有启动后端节点，`run_backend_runtime_smoke.py` 会一直等待核心服务，例如：

```text
/clean_robot_server/app/get_slam_status
/coverage_task_manager/app/exe_task_server
/clean_robot_server/app/map_server
```

先按第 12 节拉起最小后端，再运行 smoke。建议调试时用 `timeout` 防止一直挂住：

```bash
timeout 45s rosrun coverage_planner run_backend_runtime_smoke.py --text
```

### 14.6 Contract 检查缺 restart_localization

现象：

```text
param /cartographer/runtime/contracts/app/restart_localization is not set
service /cartographer/runtime/app/restart_localization has an invalid RPC URI [None]
```

处理：启动 `localization_lifecycle_manager_node.py`，见第 12 节。

## 15. 推荐部署顺序

1. 系统检查，确认 `aarch64`、Ubuntu 20.04、Python 3.8。
2. 配置 Ubuntu/ROS 源，安装 ROS Noetic 和基础工具。
3. 安装 `rosdepc`。
4. 在 `/opt/doraemon/deps` 源码构建 OR-Tools v9.9。
5. 在 `/opt/doraemon/deps` 源码构建 Fields2Cover v2.0.0。
6. 安装或源码构建 absl、FLIRT、snap7。
7. 拉取 Doraemon。
8. 补装第 6 节 ROS/系统依赖。
9. 做第 8 节源码适配，或确认这些修复已进入仓库。
10. `catkin config --extend /opt/ros/noetic` 并设置 `Fields2Cover_DIR`、`ortools_DIR`。
11. `catkin build -j4`。
12. `source devel/setup.bash`。
13. 启动最小后端，运行 `check_ros_contracts.py`。
14. 启动真实机器人或仿真运行态后，再运行 `run_backend_runtime_smoke.py --text`。

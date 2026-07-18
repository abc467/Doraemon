# 清洁机器人 x86 Ubuntu 20.04 商业部署手册

版本：2026-07-19 v1

本文用于把 Doraemon 清洁机器人后端、清洁机器人前端和 Site Gateway
部署到全新的 x86 主板。目标系统为 Ubuntu 20.04，部署操作用户为 `a`。
本文不设置固定的剩余磁盘容量门槛；部署人员应根据实际地图、日志、升级包
和现场留存策略检查可用空间。

本文对应不可变代码基线：

| 工程 | GitHub | 部署标签 |
| --- | --- | --- |
| 后端 | `https://github.com/abc467/Doraemon.git` | `deployment-2026-07-19-x86-ubuntu20-v1` |
| 前端 | `https://github.com/yeqiangsheng/clean-robot-frontend.git` | `deployment-2026-07-19-frontend-v1` |

不要用仓库默认分支或 `latest` 做批量生产。部署标签、依赖清单和验收记录
必须一起冻结。

## 1. 给新系统 Codex 的执行约束

新系统上的 Codex 应完整阅读本文后按阶段执行，并遵守以下规则：

1. 每完成一阶段，记录命令、结果、主机名、车辆编号、Git 提交和异常。
2. 任一标为 `[停止条件]` 的检查失败时停止，不猜测设备身份，不跳过。
3. 不把登录密码、Wi-Fi 密码、前端账号密码或客户凭据写入 Git、文档、终端历史和日志。
4. 不复制旧机器的 `build/`、`devel/`、`install/`、`log/`、测试 bag 或完整 `.git` 历史。
5. 初次启动前保持急停有效、驱动轮离地，并让刷盘、水泵等清洁执行器处于不会伤人的状态。
6. systemd 安装器默认只安装服务，不启动，也不开启开机自启动。完成现场验收后再显式启用。
7. USB 串口必须在新主板上重新识别。不得复制旧主板的 `ID_PATH`。
8. 每台新车必须使用独立 hostname、外部管理地址、前端账号口令和验收记录。
9. 充电桩标定属于车辆和充电桩配对数据，新车必须现场重新标定。

## 2. 目标目录和权限

生产运行目录如下：

| 内容 | 路径 | 所有权/性质 |
| --- | --- | --- |
| 后端版本 | `/opt/doraemon/releases/<tag>` | 验收后 root 只读 |
| 后端当前版本链接 | `/opt/doraemon/current` | 指向已验收版本 |
| 固定第三方依赖 | `/opt/doraemon/deps/<name-version>` | root 只读 |
| 依赖临时源码和构建缓存 | `/var/tmp/doraemon-deps-build` | 验收后可清理 |
| 后端配置 | `/etc/doraemon` | root 管理 |
| ROS 运行状态 | `/var/lib/doraemon/ros` | 服务用户可写 |
| 后端日志 | `/var/log/doraemon` 和 journald | 服务用户可写 |
| 地图和任务数据库 | `/data/maps`、`/data/coverage` | 服务用户可写 |
| 前端版本 | `/opt/clean-robot-site/releases/<version>` | root 只读 |
| 前端当前版本链接 | `/opt/clean-robot-site/current` | 指向已验收版本 |
| 前端配置 | `/etc/clean-robot-site` | root 管理 |
| 前端 SQLite 和状态 | `/var/lib/clean-robot-site` | 服务用户可写 |

这个布局替代旧的 `/home/third_party`、`/usr/local`、`/opt/carto/.ThirdParty`
和发布目录内 `.tmp` 数据。运行配置和现场数据不会因代码升级被覆盖。

## 3. 部署前登记和安全准备

每台车先登记：

| 项目 | 示例 |
| --- | --- |
| 车辆资产编号 | `CR-001` |
| hostname | `clean-robot-001` |
| 后端标签 | `deployment-2026-07-19-x86-ubuntu20-v1` |
| 前端标签 | `deployment-2026-07-19-frontend-v1` |
| 机器人内部网口 | 现场识别，例如 `eno1` |
| A-box 地址 | `192.168.127.11/24` |
| M-box 地址 | `192.168.127.10` |
| LiDAR 地址 | `192.168.127.23` |
| 充电桩地址 | `192.168.127.12` |
| 外部管理地址 | 每台车唯一，按现场网络分配 |
| IMU/里程计/M-core USB 物理口 | 现场测量 |
| 前端口令保管位置 | 公司批准的密码管理系统 |
| 部署人、验收人、日期 | 实际记录 |

安全准备：

- 车辆放在平整、隔离区域。
- 初次软件启动时驱动轮离地或整车架起。
- 急停功能已由硬件确认。
- 初次无动作验收时关闭或断开刷盘、水泵、风机等执行器。
- LiDAR、深度相机、IMU、轮速里程计和 M-box 已按最终线束接好。
- 一名人员操作电脑，一名人员观察车辆和急停。

## 4. 阶段 A：系统基线

### 4.1 确认系统

```bash
whoami
uname -m
lsb_release -ds
python3 --version
df -h
free -h
ip -br link
ip -br address
```

期望：

- `whoami` 为 `a`
- `uname -m` 为 `x86_64`
- 系统为 Ubuntu 20.04
- Python 为 Ubuntu 20.04 自带的 3.8 系列

`[停止条件]` 架构或系统版本不符时，不执行本文安装脚本。

不要把系统登录密码写进脚本或配置。`sudo` 需要认证时由现场人员交互输入。

### 4.2 设置唯一身份和时间

把示例主机名替换为车辆登记值：

```bash
sudo hostnamectl set-hostname clean-robot-001
sudo timedatectl set-timezone Asia/Shanghai
hostnamectl
timedatectl
cat /etc/machine-id
```

批量克隆系统镜像时，必须确保每台车的 machine-id、SSH host key 和 hostname
不同。不要让多台车共用一个系统身份。

### 4.3 更新系统基础包

```bash
sudo apt-get update
sudo apt-get install -y ca-certificates curl git openssl network-manager
```

Ubuntu 20.04 和 ROS Noetic 已进入长期维护/EOL 场景。量产前应使用公司批准的
Ubuntu 维护方案和受控 APT/ROS 镜像，保存安装包与校验值，不依赖交付现场公网。

## 5. 阶段 B：获取后端不可变版本

先创建版本目录：

```bash
sudo install -d -o a -g a -m 0755 /opt/doraemon/releases
sudo install -d -o a -g a -m 0755 /opt/doraemon/deps
```

只浅克隆指定标签，不下载历史大对象：

```bash
git clone \
  --depth 1 \
  --single-branch \
  --branch deployment-2026-07-19-x86-ubuntu20-v1 \
  https://github.com/abc467/Doraemon.git \
  /opt/doraemon/releases/deployment-2026-07-19-x86-ubuntu20-v1
```

验证：

```bash
cd /opt/doraemon/releases/deployment-2026-07-19-x86-ubuntu20-v1
git describe --tags --exact-match
git status --short
git rev-parse HEAD
du -sh . .git
find . -type f \( -name '*.bag' -o -name '*.bag.active' \) -print
```

最后一条命令应无输出，`git status --short` 也应无输出。

创建管理用链接：

```bash
sudo ln -sfn \
  /opt/doraemon/releases/deployment-2026-07-19-x86-ubuntu20-v1 \
  /opt/doraemon/current
```

## 6. 阶段 C：安装固定第三方依赖

### 6.1 依赖基线

版本定义在：

```text
deploy/manifests/x86_ubuntu20_versions.env
```

当前固定项包括：

| 依赖 | 版本/来源 | 安装路径 |
| --- | --- | --- |
| abseil | `20211102.0` 固定提交 | `/opt/doraemon/deps/abseil-20211102.0` |
| OR-Tools | `9.9` 固定提交 | `/opt/doraemon/deps/ortools-9.9` |
| Fields2Cover | `2.0.0` 固定提交 | `/opt/doraemon/deps/fields2cover-2.0.0` |
| FLIRT | Doraemon 兼容快照 | `/opt/doraemon/deps/flirt-doraemon-20260319` |
| ROS | Noetic/Ubuntu 20.04 amd64 | `/opt/ros/noetic` |

FLIRT 兼容源码已小体积纳入 `third_party/flirt`，来源和修改说明见
`third_party/flirt/PROVENANCE.md`。不得再依赖某台开发机上的
`/opt/carto/.ThirdParty/flirt`。

### 6.2 执行安装

依赖编译可能较久。并发数应按主板内存调整；首次部署建议从 4 开始：

```bash
cd /opt/doraemon/current
DORAEMON_BUILD_JOBS=4 ./scripts/install_x86_ubuntu20_dependencies.sh
```

脚本将：

1. 校验 x86_64 和 Ubuntu 20.04。
2. 配置 ROS Noetic APT 源并安装系统/ROS 依赖。
3. 运行 rosdep 补齐工作空间声明的依赖。
4. 按固定 Git 提交编译 abseil、OR-Tools 和 Fields2Cover。
5. 编译仓库内 FLIRT 兼容快照。
6. 写入 `/etc/ld.so.conf.d/doraemon-deps.conf`。
7. 写入 `/etc/doraemon/deps.env` 和 `/etc/profile.d/doraemon-deps.sh`。
8. 验证 Fields2Cover Python 导入和动态库解析。

`[停止条件]` 任一依赖下载、提交校验、编译或 `ldd` 检查失败时停止。
不要改成任意新版本来绕过错误。

### 6.3 依赖验收

```bash
cat /etc/doraemon/deps.env
source /etc/profile.d/doraemon-deps.sh
python3 -c "import fields2cover; print(fields2cover.__file__)"
ldd /opt/doraemon/deps/fields2cover-2.0.0/lib/libFields2Cover.so
find /opt/doraemon/deps -maxdepth 2 -type d -print
```

`ldd` 不得包含 `not found`，输出路径不得指向 `/home/third_party`、
`/usr/local` 或 `/opt/carto/.ThirdParty`。

## 7. 阶段 D：编译后端工作空间

```bash
cd /opt/doraemon/current
DORAEMON_BUILD_JOBS=4 ./scripts/build_x86_ubuntu20_workspace.sh
```

脚本会把 `absl_DIR` 和 `FLIRT_ROOT` 显式传给 CMake，并在完成后验证
Fields2Cover、`coverage_planner`、`coverage_task_manager` 和
`robot_hw_bridge`。

检查：

```bash
cd /opt/doraemon/current
source /opt/ros/noetic/setup.bash
source /etc/profile.d/doraemon-deps.sh
source devel/setup.bash
catkin list
rospack find cartographer
rospack find coverage_planner
python3 -c "import fields2cover"
```

不要从旧机器复制 `build/` 或 `devel/`。这些目录包含架构、编译器和绝对路径信息。

## 8. 阶段 E：网络配置

机器人内部设备网建议使用独立有线网口，不配置默认网关，避免影响用于联网维护
的另一个网口或 Wi-Fi。

### 8.1 识别网口

```bash
nmcli device status
ip -br link
sudo ethtool <候选网口>
```

通过插拔网线或观察 `Link detected` 确认实际网口，不能假设新主板仍叫 `eno1`。

### 8.2 配置内部静态地址

把 `<ROBOT_IFACE>` 替换为实际名称：

```bash
sudo nmcli connection add \
  type ethernet \
  ifname <ROBOT_IFACE> \
  con-name doraemon-robot-internal \
  ipv4.method manual \
  ipv4.addresses 192.168.127.11/24 \
  ipv4.never-default yes \
  ipv6.method disabled
sudo nmcli connection up doraemon-robot-internal
```

如果同名连接已经存在，使用 `nmcli connection modify` 修改，不要重复创建。

验证：

```bash
ip -4 address show dev <ROBOT_IFACE>
ip route
ping -c 3 192.168.127.10
ping -c 3 192.168.127.23
ping -c 3 192.168.127.12
```

M-box 和 LiDAR 是后端启动必需项。充电桩暂未上电时可以记录为待验收，但不能
把地址错误当成正常。

## 9. 阶段 F：USB、相机和 udev

### 9.1 识别串口

先记录初始状态：

```bash
ls -l /dev/ttyUSB* /dev/serial/by-id/* 2>/dev/null
```

让现场人员每次只插入一个设备，依次确认 IMU、轮速里程计和串口 M-core：

```bash
udevadm info --query=property --name=/dev/ttyUSB0
```

优先使用真实唯一的 `ID_SERIAL_SHORT`。当前 CH340 转换器通常没有唯一序列号，
此时只能在最终线束固定后使用 `ID_PATH`，并把 USB 端口纳入装配工艺。

复制模板并编辑：

```bash
sudo install -m 0644 \
  /opt/doraemon/current/deploy/udev/99-doraemon-a26022-serial.rules.example \
  /etc/udev/rules.d/99-doraemon-a26022-serial.rules
sudoedit /etc/udev/rules.d/99-doraemon-a26022-serial.rules
```

必须替换所有 `REPLACE_*_ID_PATH`。完成后：

```bash
grep -n 'REPLACE_' /etc/udev/rules.d/99-doraemon-a26022-serial.rules
sudo udevadm control --reload-rules
sudo udevadm trigger
sudo udevadm settle
ls -l /dev/imu /dev/wheel_odom /dev/mcore
```

`grep` 应无输出。串口权限应为 `root:dialout`、`0660`，不要使用全局 `0666`。
当前 TCP M-core 方案可以没有 `/dev/mcore`，但 IMU 和里程计别名必须正确。

### 9.2 安装 Orbbec 规则

```bash
sudo install -m 0644 \
  /opt/doraemon/current/src/orbbec-ros-sdk/scripts/99-obsensor-ros1-libusb.rules \
  /etc/udev/rules.d/99-obsensor-ros1-libusb.rules
sudo udevadm control --reload-rules
sudo udevadm trigger
lsusb
```

最终插拔一次 USB 设备，确认别名重建。不要只依赖当前 `/dev/ttyUSBN` 顺序。

## 10. 阶段 G：后端配置和持久化数据

### 10.1 安装服务和初始配置

```bash
cd /opt/doraemon/current
./scripts/install_doraemon_runtime_service.sh
```

该命令会：

- 创建 `/etc/doraemon/runtime.env`
- 创建 `/data/coverage`、`/data/maps`、`/var/lib/doraemon` 和日志目录
- 把用户 `a` 加入 `dialout`、`plugdev`、`video`
- 安装 `doraemon-runtime.service`
- 保持服务停止且不开机启动

执行后重新登录一次，或在完成所有步骤后重启，以刷新交互式用户组。

### 10.2 编辑每台车配置

```bash
sudoedit /etc/doraemon/runtime.env
```

至少核对：

```text
DORAEMON_A_BOX_IFACE=<实际内部网口>
DORAEMON_A_BOX_IP=192.168.127.11
DORAEMON_MBOX_IP=192.168.127.10
DORAEMON_LIDAR_IP=192.168.127.23
STATION_SERVER_IP=192.168.127.12
STATION_SERVER_PORT=5007
DORAEMON_IMU_DEVICE=/dev/imu
DORAEMON_ODOM_DEVICE=/dev/wheel_odom
ODOM_SERIAL_DEVICE=/dev/wheel_odom
MCORE_TRANSPORT=tcp
MCORE_TCP_HOST=192.168.127.10
MCORE_TCP_PORT=8080
ROSBRIDGE_ADDRESS=127.0.0.1
DOCK_CALIBRATION_STORAGE_PATH=/data/coverage/dock_calibration.yaml
```

保留当前已经验证的底盘方向、轮径、轮距、编码器和停靠参数，除非机械/算法负责人
有带版本的变更单。不得通过修改源码给单车做参数差异。

校验环境文件语法：

```bash
bash -n /etc/doraemon/runtime.env
sudo systemctl cat doraemon-runtime.service
systemctl is-enabled doraemon-runtime.service
systemctl is-active doraemon-runtime.service
```

此时后两项应分别为 `disabled` 和 `inactive`。

### 10.3 新车与换主板的数据边界

全新车辆：

- 不复制其他车的 `planning.db`、`operations.db`、地图和标定。
- 不复制 `dock_calibration.yaml`。
- 部署后现场建图、创建任务并执行充电桩标定。

同一物理车辆更换主板：

- 停止旧系统后，可受控迁移 `/data/maps` 和 `/data/coverage`。
- 迁移前后保存 SHA256 清单。
- 仍需复核地图坐标、轮速方向、充电桩位置和标定有效性。
- 不迁移 bag、构建目录、日志缓存或前端 SQLite 会话。

## 11. 阶段 H：无动作后端预检

不要启动服务，先执行：

```bash
cd /opt/doraemon/current
./scripts/verify_x86_ubuntu20_deployment.sh
```

允许充电桩未上电产生网络 warning；架构、ROS、依赖、工作空间和配置的 failure
必须全部解决。

再检查：

```bash
ldd devel/lib/cartographer_ros/cartographer_node | grep 'not found'
find /opt/doraemon/current -type f \( -name '*.bag' -o -name '*.bag.active' \) -print
sudo journalctl --disk-usage
```

`ldd` 和 bag 检查应无输出。

## 12. 阶段 I：获取和构建前端

### 12.1 浅克隆固定版本

```bash
sudo install -d -o a -g a -m 0755 /opt/clean-robot-site/releases
git clone \
  --depth 1 \
  --single-branch \
  --branch deployment-2026-07-19-frontend-v1 \
  https://github.com/yeqiangsheng/clean-robot-frontend.git \
  /home/a/clean-robot-frontend-build
cd /home/a/clean-robot-frontend-build
git describe --tags --exact-match
git status --short
```

### 12.2 安装固定 Node.js

```bash
cd /home/a/clean-robot-frontend-build
./scripts/install-node22-linux-x64.sh
node --version
npm --version
```

脚本从 Node.js 官方发布目录下载固定 x64 版本，并用仓库内固定 SHA256 校验，安装到
`/opt/nodejs/node-v22.23.1-linux-x64`。

### 12.3 验证并生成最小生产包

```bash
cd /home/a/clean-robot-frontend-build
npm ci
npm run verify
npm run package:trial
cd release/clean-robot-site-v0.1.0-rc.9
npm ci --omit=dev
```

`npm run package:trial` 内部已经执行完整验证；再次列出 `npm run verify` 是为了
让部署记录明确保存验收输出。若工厂使用已经签名和校验的前端制品，可省略目标车
编译，但不得省略制品 SHA256 验证。

安装版本：

```bash
sudo install -d -m 0755 /opt/clean-robot-site/releases/0.1.0-rc.9
sudo cp -a \
  /home/a/clean-robot-frontend-build/release/clean-robot-site-v0.1.0-rc.9/. \
  /opt/clean-robot-site/releases/0.1.0-rc.9/
sudo chown -R root:root /opt/clean-robot-site/releases/0.1.0-rc.9
sudo ln -sfn \
  /opt/clean-robot-site/releases/0.1.0-rc.9 \
  /opt/clean-robot-site/current
```

## 13. 阶段 J：前端独立配置和服务

### 13.1 创建单车配置

前端仓库不再携带可直接使用的共享出厂账号。历史版本中出现过的任何共享默认
口令都必须视为已泄露，不得继续使用。

```bash
sudo install -d -m 0750 -o root -g a /etc/clean-robot-site
sudo install -m 0640 -o root -g a \
  /opt/clean-robot-site/current/site-gateway/site-config.field.example.json \
  /etc/clean-robot-site/site-config.json
sudo install -m 0640 -o root -g a \
  /opt/clean-robot-site/current/public/app-config.json \
  /etc/clean-robot-site/app-config.json
```

为 `operator`、`service`、`engineer` 分别生成独立随机口令：

```bash
openssl rand -base64 24
```

每次输出只进入公司批准的密码管理系统和当前车辆外置配置。不要把口令作为命令行
参数。用 `sudoedit` 交互替换三个 `replace-with-site-secret-*`：

```bash
sudoedit /etc/clean-robot-site/site-config.json
sudoedit /etc/clean-robot-site/app-config.json
```

同时设置：

- `site-config.json` 的 `robotId`、`siteName` 和角色权限。
- `app-config.json` 的 `robotId`、`siteName`、启用模块和支持联系方式。
- `rosbridgeUrl` 保持 `ws://127.0.0.1:9090`。
- `mapImportPbstreamDir` 保持 `/data/maps/imports`。
- 客户交付时按最小权限原则关闭不需要的工程师功能。

检查占位符：

```bash
sudo grep -R -n 'replace-with\\|change-me\\|bulibusan' /etc/clean-robot-site
```

应无输出。

### 13.2 安装但不启动服务

```bash
cd /opt/clean-robot-site/current
sudo SITE_SERVICE_USER=a \
  SITE_ROSBRIDGE_URL=ws://127.0.0.1:9090 \
  ./scripts/install-site-systemd.sh
```

安装器会验证外置配置、账号和生产依赖，并把 SQLite 放到
`/var/lib/clean-robot-site/site-gateway.sqlite`。它不会从 root 账号运行
`npm install`，也不会自动拉起后端。

检查：

```bash
systemctl is-enabled clean-robot-site-gateway.service
systemctl is-active clean-robot-site-gateway.service
sudo systemctl cat clean-robot-site-gateway.service
```

此时应为 `disabled`、`inactive`。

## 14. 阶段 K：首次受控启动

### 14.1 再次确认物理安全

- 急停可立即切断运动。
- 驱动轮离地。
- 清洁执行器处于安全断开/禁用状态。
- 车辆周围无人员、线缆和障碍物。
- 两名人员均已就位。

### 14.2 启动后端

```bash
sudo systemctl start doraemon-runtime.service
systemctl status doraemon-runtime.service --no-pager
sudo journalctl -u doraemon-runtime.service -n 200 --no-pager
```

检查 ROS：

```bash
source /opt/ros/noetic/setup.bash
source /etc/profile.d/doraemon-deps.sh
source /opt/doraemon/current/devel/setup.bash
rosnode list
rostopic list
rostopic hz /scan
rostopic hz /imu
rostopic hz /odom
rosrun coverage_planner check_ros_contracts.py --strict --text
rosrun coverage_planner run_backend_runtime_smoke.py --text
```

最后两个命令为只读/非执行动作验收入口。此阶段不要运行带
`--allow-write-actions` 的脚本。

### 14.3 启动前端

```bash
sudo systemctl start clean-robot-site-gateway.service
systemctl status clean-robot-site-gateway.service --no-pager
curl -fsS http://127.0.0.1:4173/api/health
sudo journalctl -u clean-robot-site-gateway.service -n 100 --no-pager
```

浏览器访问：

```text
http://127.0.0.1:4173/
```

或从受控管理网访问：

```text
http://<机器人管理地址>:4173/
```

确认前端显示的 `robotId`、车辆编号、地图、模块和账号权限都属于当前车辆。

## 15. 阶段 L：车辆、地图和充电桩现场验收

以下阶段必须由机器人测试负责人批准后进行。

### 15.1 低速底盘验收

1. 先在轮子离地状态核对左右轮方向、里程计正负号和急停。
2. 落地后设置限速，短距离验证前进、后退、左转、右转和停止。
3. 检查 `/cmd_vel` 停止后底盘是否及时停止。
4. 检查 LiDAR、深度相机和安全区是否能阻止危险运动。
5. 记录轮径、轮距、比例和方向参数版本。

### 15.2 建图和任务验收

全新车辆必须建立当前现场地图，不使用其他车辆测试地图：

```bash
rosrun coverage_planner run_backend_production_acceptance.py \
  --profile read_only_gate \
  --plan-db-path /data/coverage/planning.db \
  --ops-db-path /data/coverage/operations.db \
  --text
```

需要写入地图/任务的生产验收入口见 `docs/backend_runtime_smoke_v1.md`。
只有明确核对 profile、地图名和现场状态后，才允许添加
`--allow-write-actions`。

### 15.3 充电桩标定验收

本版本包含充电桩标定功能。每台物理车辆与充电桩组合都要单独完成：

1. 在前端以授权的 service/engineer 角色进入充电桩标定。
2. 确认当前地图和充电桩均属于当前车辆现场。
3. 按界面流程采集、保存并读取标定。
4. 确认文件写入 `/data/coverage/dock_calibration.yaml`。
5. 先做低速、有人监护的回桩测试。
6. 验证停止距离、航向、充电触点和充电状态。
7. 重启后端，再次确认标定可读取并完成一次回桩。

不要把 A 车的 `dock_calibration.yaml` 复制给 B 车。更换雷达、底盘、充电桩、
机械安装位置或地图坐标后，原标定必须重新评估。

### 15.4 清洁执行器验收

按刷盘、水泵、风机、排水、补水顺序逐项测试，每次只开放一个动作。现场必须有
急停人员。记录命令、实际动作、反馈状态、超时和停止结果。

## 16. 阶段 M：启用开机运行

只有全部验收通过后执行：

```bash
sudo systemctl enable doraemon-runtime.service
sudo systemctl enable clean-robot-site-gateway.service
systemctl is-enabled doraemon-runtime.service
systemctl is-enabled clean-robot-site-gateway.service
```

重启验收：

```bash
sudo reboot
```

重启后：

```bash
systemctl status doraemon-runtime.service --no-pager
systemctl status clean-robot-site-gateway.service --no-pager
curl -fsS http://127.0.0.1:4173/api/health
sudo journalctl -b -u doraemon-runtime.service --no-pager
sudo journalctl -b -u clean-robot-site-gateway.service --no-pager
```

确认系统在没有活动地图时进入“服务可用但任务未就绪”，而不是误执行旧任务。

## 17. 可选：触摸屏 kiosk

仅在车辆需要本机触摸屏时配置。安装 Chromium 和 X11 工具：

```bash
sudo apt-get install -y chromium-browser x11-xserver-utils
install -d -m 0755 /home/a/.local/bin /home/a/.config/autostart
```

创建 `/home/a/.local/bin/clean-robot-kiosk.sh`：

```bash
#!/usr/bin/env bash
set -e

until curl -fsS http://127.0.0.1:4173/api/health >/dev/null; do
  sleep 2
done

xset s off -dpms 2>/dev/null || true
exec chromium-browser \
  --kiosk http://127.0.0.1:4173/ \
  --no-first-run \
  --disable-session-crashed-bubble \
  --password-store=basic \
  --user-data-dir="$HOME/.config/clean-robot-kiosk-chromium"
```

授权：

```bash
chmod 0755 /home/a/.local/bin/clean-robot-kiosk.sh
```

创建 `/home/a/.config/autostart/clean-robot-kiosk.desktop`：

```ini
[Desktop Entry]
Type=Application
Name=Clean Robot Kiosk
Exec=/home/a/.local/bin/clean-robot-kiosk.sh
X-GNOME-Autostart-enabled=true
```

如启用 GDM 自动登录，只允许专用 kiosk 用户；不要配置全局免密 sudo。设备无人值守
时还应限制物理键盘、TTY、浏览器导航和外部管理网访问。

## 18. 网络和账号加固

1. rosbridge 默认只监听 `127.0.0.1:9090`，外部浏览器只访问 Site Gateway。
2. `4173/tcp` 只对批准的管理网段开放，不向公网开放。
3. SSH 采用公司批准的密钥认证；量产后关闭不需要的口令登录。
4. 前端三类账号使用每车独立随机口令，客户交付前完成轮换。
5. `/etc/doraemon` 和 `/etc/clean-robot-site` 仅 root 和服务组可读。
6. 不在日志中打印令牌、口令和客户 Wi-Fi 信息。
7. 对外发布前完成第三方许可证清单、源代码义务和安全评审。

UFW 示例中的网段必须替换为实际管理网：

```bash
sudo ufw allow from <MANAGEMENT_SUBNET> to any port 22 proto tcp
sudo ufw allow from <MANAGEMENT_SUBNET> to any port 4173 proto tcp
sudo ufw enable
sudo ufw status verbose
```

不要在未确认远程维护链路前启用防火墙。

## 19. 量产方式

本文的源码编译流程适合首台工程样机和工厂母版验证。批量生产应改为：

1. CI 在固定 Ubuntu 20.04 x86_64 构建环境中构建一次。
2. 对后端工作空间、依赖目录和前端发布包生成 SHA256 清单。
3. 保存 Git 标签、提交、依赖 manifest、APT 包清单和许可证清单。
4. 对制品签名并发布到公司制品库或离线介质。
5. 每台车只安装同一批次制品，不在现场重新解析“最新”依赖。
6. 通过配置工位写入单车 hostname、网络、USB 规则、robotId 和独立口令。
7. 通过标定工位生成该车地图/传感器/充电桩标定。
8. 自动生成不可修改的单车验收报告。

建议制品清单至少包含：

```bash
git -C /opt/doraemon/current rev-parse HEAD
git -C /home/a/clean-robot-frontend-build rev-parse HEAD
dpkg-query -W -f='${Package}\t${Version}\n'
find /opt/doraemon/deps -type f -name '*.so*' -print0 | sort -z | xargs -0 sha256sum
find /opt/clean-robot-site/current -type f -print0 | sort -z | xargs -0 sha256sum
```

## 20. 升级和回滚

### 20.1 后端升级

1. 浅克隆新的不可变标签到 `/opt/doraemon/releases/<new-tag>`。
2. 比较新旧 `deploy/manifests/x86_ubuntu20_versions.env`。
3. 依赖有变化时安装新版本目录；不得覆盖旧版本依赖。
4. 在新目录构建并运行无动作检查。
5. 停止服务，备份 `/etc/doraemon` 和 `/data`。
6. 从新目录重新运行 `install_doraemon_runtime_service.sh`。
7. 切换 `/opt/doraemon/current`，受控启动并验收。

回滚时从旧版本目录重新安装 systemd unit，恢复配置备份并启动。不要用
`git reset --hard` 在生产目录原地回退。

### 20.2 前端升级

1. 把新发布包安装到 `/opt/clean-robot-site/releases/<new-version>`。
2. 保留 `/etc/clean-robot-site` 和 `/var/lib/clean-robot-site`。
3. 切换 `/opt/clean-robot-site/current`。
4. 从新版本重新运行 `install-site-systemd.sh`。
5. 启动并检查 `/api/health`、登录、权限和 ROS 连接。

回滚只切回旧版本并重新安装 unit；不要回滚或覆盖现场 SQLite，除非数据库迁移说明
明确要求。

## 21. 备份

代码由 Git 标签和制品库备份，车辆只备份外置配置和业务数据：

```bash
sudo systemctl stop clean-robot-site-gateway.service
sudo systemctl stop doraemon-runtime.service
sudo tar -C / -czf /tmp/clean-robot-config-and-data-$(date +%F).tar.gz \
  etc/doraemon \
  etc/clean-robot-site \
  data/coverage \
  data/maps \
  var/lib/clean-robot-site
sudo sha256sum /tmp/clean-robot-config-and-data-*.tar.gz
```

备份中包含账号配置和现场数据，必须加密、访问受控，并按公司保留策略转移后删除
车辆上的临时包。

## 22. 清理策略

完成验收且确认不再需要依赖构建缓存后：

```bash
sudo find /var/tmp/doraemon-deps-build -mindepth 1 -delete
sudo rmdir /var/tmp/doraemon-deps-build
sudo journalctl --vacuum-time=14d
```

定期检查：

```bash
find /opt/doraemon /opt/clean-robot-site /data \
  -type f \( -name '*.bag' -o -name '*.bag.active' \) -print
du -sh /opt/doraemon /opt/clean-robot-site /data /var/log/doraemon
sudo journalctl --disk-usage
```

生产车默认不保存测试 bag。确需现场录包时，应设置工单、最大时长、脱敏要求、
转移位置和自动清理期限。显式运行 `record_localization_debug_bag.sh` 时默认输出到
`/var/log/doraemon/debug-bags`，不得写回代码仓库。

## 23. 常见故障

### Fields2Cover 导入失败

```bash
source /etc/profile.d/doraemon-deps.sh
python3 -c "import fields2cover; print(fields2cover.__file__)"
ldd /opt/doraemon/deps/fields2cover-2.0.0/lib/libFields2Cover.so
```

检查 `/etc/doraemon/deps.env` 和 `ldconfig -p`。不要把库复制到 `/usr/local`。

### Cartographer 找不到 FLIRT

```bash
grep FLIRT_ROOT /etc/doraemon/deps.env
find /opt/doraemon/deps/flirt-doraemon-20260319 -maxdepth 3 -type f
catkin build cartographer cartographer_ros --force-cmake
```

确认 CMake 使用仓库内 `FindFLIRT.cmake`，缓存路径不指向旧机器目录。

### `/dev/imu` 或 `/dev/wheel_odom` 不存在

```bash
udevadm info --query=property --name=/dev/ttyUSB0
sudo udevadm test /sys/class/tty/ttyUSB0
ls -l /etc/udev/rules.d/99-doraemon-a26022-serial.rules
id a
```

核对最终 USB 物理口和 `ID_PATH`，不要通过交换别名碰运气。

### 后端服务启动超时

```bash
sudo journalctl -u doraemon-runtime.service -n 250 --no-pager
ip -4 address
ping -c 3 192.168.127.10
ping -c 3 192.168.127.23
ls -l /dev/imu /dev/wheel_odom
```

启动前置检查会等待 A-box 地址、M-box、LiDAR、IMU 和里程计。

### 前端健康但 ROS 未连接

```bash
curl -fsS http://127.0.0.1:4173/api/health
ss -ltnp | grep 9090
sudo journalctl -u clean-robot-site-gateway.service -n 150 --no-pager
```

检查 rosbridge 是否在本机 `127.0.0.1:9090`，Site Gateway 会自动重连，
无需通过重启前端拉起机器人后端。

### 前端安装器提示 bootstrapUsers 为空或口令不安全

编辑 `/etc/clean-robot-site/site-config.json`，为三种角色写入每车独立随机口令，
清除所有占位符后重新运行安装器。不要改校验代码绕过。

## 24. 最终验收清单

- [ ] x86_64 Ubuntu 20.04、hostname、machine-id 和时间正确
- [ ] 后端和前端均为本文固定标签，工作区无未提交改动
- [ ] 依赖全部位于 `/opt/doraemon/deps`，`ldd` 无 `not found`
- [ ] 没有运行时依赖 `/home/third_party`、`/usr/local` 或旧用户目录
- [ ] 内部网口、M-box、LiDAR 和充电桩地址已登记
- [ ] IMU、里程计、M-core udev 规则在本机实测
- [ ] Orbbec 深度相机和 LiDAR 数据稳定
- [ ] `/etc/doraemon/runtime.env` 已按本车复核
- [ ] `/etc/clean-robot-site` 使用每车独立账号口令
- [ ] 前端 SQLite 位于 `/var/lib/clean-robot-site`
- [ ] 地图和数据库位于 `/data`，没有复制其他新车数据
- [ ] 只读 contract 和 runtime smoke 通过
- [ ] 急停、低速底盘、传感器安全链通过
- [ ] 当前车辆完成建图和任务流程验收
- [ ] 充电桩标定已保存、重启后可读取、回桩和充电通过
- [ ] 清洁执行器逐项验收通过
- [ ] 后端和前端仅在验收后启用开机启动
- [ ] 重启后系统、前端和日志检查通过
- [ ] 代码、依赖、配置和制品 SHA256 已归档
- [ ] 无测试 bag、历史发布包和无用大文件
- [ ] 第三方许可证、安全和 Ubuntu/ROS 生命周期风险已签字确认

## 25. 上游生命周期和许可证

- ROS Noetic 目标平台为 Ubuntu 20.04 amd64/Python 3.8，支持周期已在 2025 年
  5 月结束：
  `https://www.ros.org/reps/rep-0003.html`
- Ubuntu 20.04 的标准安全维护已在 2025 年 5 月结束；Ubuntu Pro/ESM 可把安全
  维护延长到 2030 年 5 月，具体覆盖应以 Canonical 官方页面为准：
  `https://ubuntu.com/about/release-cycle`
- Node.js 22 当前处于 Maintenance LTS，计划在 2027 年 4 月结束支持。量产负责人
  必须在此之前验证并冻结后续 LTS 基线：
  `https://github.com/nodejs/Release`
- FLIRT 快照保留 LGPL 文件，但对外分发前仍需由公司完成第三方许可证和商业使用评审。

EOL 不代表当前代码立即不能运行，但意味着后续安全更新、APT 可用性和新硬件兼容
不能依赖上游持续提供。商业量产必须保存可复现制品，并制定系统迁移计划。

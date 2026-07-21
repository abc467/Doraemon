# 清洁机器人 x86 Ubuntu 20.04 商业部署手册

版本：2026-07-21 v4

本文用于把 Doraemon 清洁机器人后端、清洁机器人前端和 Site Gateway
部署到全新的 x86 主板。目标系统为 Ubuntu 20.04，部署操作用户为 `a`。
本文不设置固定的剩余磁盘容量门槛；部署人员应根据实际地图、日志、升级包
和现场留存策略检查可用空间。

本文对应不可变代码基线：

| 工程 | GitHub | 部署标签 |
| --- | --- | --- |
| 后端 | `https://github.com/abc467/Doraemon.git` | `deployment-2026-07-21-x86-ubuntu20-v4` |
| 前端 | `https://github.com/yeqiangsheng/clean-robot-frontend.git` | `deployment-2026-07-21-frontend-v2` |

不要用仓库默认分支或 `latest` 做批量生产。部署标签、依赖清单和验收记录
必须一起冻结。

首次批量部署试验允许在 `/var/tmp` 的独立候选工作区边诊断边修改，但不得原地修改
`/opt/doraemon/releases/<tag>`。每轮通过软件回归后必须创建新的固定标签，再从该标签
做一次不携带旧机 `build/`、`devel/`、日志或完整历史的全新浅克隆和干净构建；只有
这个重建结果可以进入商业发布目录。新主板从固定标签干净生成的 `build/`、`devel/`
是运行所需制品，可以随同 release 保留并冻结；候选工作区本身不是交付制品。

## 1. 给新系统 Codex 的执行约束

新系统上的 Codex 应完整阅读本文后按阶段执行，并遵守以下规则：

1. 每完成一阶段，记录命令、结果、主机名、车辆编号、Git 提交和异常。
2. 任一标为 `[停止条件]` 的检查失败时停止，不猜测设备身份，不跳过。
3. 不把登录密码、Wi-Fi 密码、前端账号密码或客户凭据写入 Git、文档、终端历史和日志。
4. 不复制旧机器的 `build/`、`devel/`、`install/`、任何日志、测试 bag 或完整 `.git` 历史。
5. 初次启动前保持急停有效、驱动轮离地，并让刷盘、水泵等清洁执行器处于不会伤人的状态。
6. systemd 安装器默认只安装服务，不启动，也不开启开机自启动。完成现场验收后再显式启用。
7. USB 串口必须在新主板上重新识别。不得复制旧主板的 `ID_PATH`。
8. 每台新车必须使用独立 hostname、外部管理地址、前端账号口令和验收记录。
9. 充电桩标定属于车辆和充电桩配对数据，新车必须现场重新标定。

每一阶段都必须以一条明确的阶段报告收尾，至少包含：车辆资产编号、hostname、
后端/前端标签及提交、该阶段执行的检查、设备路径或网络身份、结果、异常和执行/复核人。
只有本阶段全部检查通过且没有未关闭的 `[停止条件]`，才可报告“通过”并进入下一阶段；
阶段被批准暂缓时必须报告“暂缓”、原因和恢复入口，不得把“暂缓”写成“通过”。报告中
不得出现任何口令明文。

## 2. 目标目录和权限

生产运行目录如下：

| 内容 | 路径 | 所有权/性质 |
| --- | --- | --- |
| 后端版本父目录 | `/opt/doraemon/releases` | `root:root`、`0755` |
| 后端版本 | `/opt/doraemon/releases/<tag>` | 验收后 root 只读 |
| 后端当前版本链接 | `/opt/doraemon/current` | 指向已验收版本 |
| 固定依赖父目录 | `/opt/doraemon/deps` | `root:root`、`0755` |
| 固定第三方依赖 | `/opt/doraemon/deps/<name-version>` | root 只读 |
| 依赖临时源码和构建缓存 | `/var/tmp/doraemon-deps-build` | 验收后可清理 |
| 后端配置 | `/etc/doraemon` | root 管理 |
| ROS 运行状态 | `/var/lib/doraemon/ros` | 服务用户可写 |
| Orbbec 人工抓图/点云 | `/var/lib/doraemon/orbbec-captures` | 服务用户可写 |
| 后端日志 | `/var/log/doraemon` 和 journald | 服务用户可写 |
| 地图和任务数据库 | `/data/maps`、`/data/coverage` | 服务用户可写 |
| 前端版本父目录 | `/opt/clean-robot-site/releases` | `root:root`、`0755` |
| 前端版本 | `/opt/clean-robot-site/releases/<version>` | root 只读 |
| 前端当前版本链接 | `/opt/clean-robot-site/current` | 指向已验收版本 |
| 前端配置 | `/etc/clean-robot-site` | root 管理 |
| 前端 SQLite 和状态 | `/var/lib/clean-robot-site` | 服务用户可写 |
| 可选 SLAM 配置覆盖 | `/data/config/slam/cartographer` | `root:a`、`0750`、经审查、服务不可写 |

这个布局替代旧的 `/home/third_party`、`/usr/local`、`/opt/carto/.ThirdParty`
和发布目录内 `.tmp` 数据。运行配置和现场数据不会因代码升级被覆盖。
三个 `releases`/`deps` 父目录不得交给用户 `a`，也不得让 group/other 可写。单个目标
版本可以在 clone/build 期间临时由 `a:a` 持有；安装 systemd unit 或切换为生产运行
版本前，必须把整个 child（包括目录、文件和符号链接本身）冻结为 `root:root` 并移除
group/other 写权限。`current` 符号链接本身也必须为 `root:root`。

后端 release 内不得出现运行或测试生成物：源码树中的 `.git/logs` 浅克隆元数据除外，
其余任何名为 `log`、`logs` 或 `Log` 的目录、`test_bag`、`image`、`point_cloud`、任何
`*.bag`/`*.bag.*`、`export.log`、`planning.db`、`operations.db`、Site Gateway SQLite
或其他运行数据库都禁止进入 release。它们必须分别写入 `/var/log/doraemon`、
`/var/lib/doraemon/orbbec-captures`、`/data` 或 `/var/lib/clean-robot-site`。

## 3. 部署前登记和安全准备

每台车先登记：

| 项目 | 示例 |
| --- | --- |
| 车辆资产编号 | `<公司资产系统中的唯一编号>` |
| hostname | `clean-robot-<唯一序号>` |
| 后端标签 | `deployment-2026-07-21-x86-ubuntu20-v4` |
| 前端标签 | `deployment-2026-07-21-frontend-v2` |
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
LC_ALL=C nmcli -t -f ACTIVE,SSID device wifi | grep -Fx 'yes:shebei'
```

期望：

- `whoami` 为 `a`
- `uname -m` 为 `x86_64`
- 系统为 Ubuntu 20.04
- Python 为 Ubuntu 20.04 自带的 3.8 系列
- 当前维护 Wi-Fi `shebei` 为 active，并在本轮部署全程保持连接

`[停止条件]` 架构或系统版本不符时，不执行本文安装脚本。

不要把系统登录密码写进脚本或配置。`sudo` 需要认证时由现场人员交互输入。

### 4.2 设置唯一身份和时间

从单车部署记录输入已经批准的 hostname：

```bash
read -r -p '输入单车部署记录中的唯一 hostname: ' DORAEMON_HOSTNAME
[[ "${DORAEMON_HOSTNAME}" =~ ^[a-z0-9][a-z0-9-]{0,62}$ ]]
sudo hostnamectl set-hostname "${DORAEMON_HOSTNAME}"
unset DORAEMON_HOSTNAME
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

`[停止条件]` 用户不是 `a`、hostname/车辆资产对应关系不唯一、machine-id 与批次内
其他车辆重复、时间配置错误、基础包安装失败，或维护网络不可用时停止阶段 A。不得为
继续安装而记录错误身份；阶段 A 报告必须写入最终 hostname 和 machine-id（不含凭据）。

## 5. 阶段 B：获取后端不可变版本

父目录固定为 root 管理；只把本次尚未冻结的目标 child 临时交给构建用户：

```bash
sudo install -d -o root -g root -m 0755 /opt/doraemon/releases
sudo install -d -o root -g root -m 0755 /opt/doraemon/deps
sudo install -d -o a -g a -m 0755 \
  /opt/doraemon/releases/deployment-2026-07-21-x86-ubuntu20-v4
stat -c '%U:%G %a %n' /opt/doraemon/releases /opt/doraemon/deps
```

只浅克隆指定标签，不下载历史大对象：

```bash
git clone \
  --depth 1 \
  --single-branch \
  --branch deployment-2026-07-21-x86-ubuntu20-v4 \
  https://github.com/abc467/Doraemon.git \
  /opt/doraemon/releases/deployment-2026-07-21-x86-ubuntu20-v4
```

验证：

```bash
cd /opt/doraemon/releases/deployment-2026-07-21-x86-ubuntu20-v4
git describe --tags --exact-match
git status --porcelain=v1 --untracked-files=all
git rev-parse HEAD
git rev-parse --is-shallow-repository
du -sh . .git
release=/opt/doraemon/releases/deployment-2026-07-21-x86-ubuntu20-v4
find "${release}" -xdev \
  -path "${release}/.git" -prune -o \
  \( \
    -type d \( -iname build -o -iname devel -o -iname install -o \
      -iname log -o -iname logs -o -iname test_bag -o \
      -iname image -o -iname point_cloud \) -o \
    -type f \( -iname '*.bag' -o -iname '*.bag.*' -o \
      -iname export.log -o -iname '*.db' -o -iname '*.sqlite*' \) \
  \) -print
```

`git status`、最后一条 `find` 都应无输出，`--is-shallow-repository` 必须输出 `true`。

还必须确认这是浅克隆，且标签和提交已写入单车部署记录：

```bash
test -f .git/shallow
```

此时先不创建或切换 `/opt/doraemon/current`，也不冻结 target；阶段 C、D、F 使用上述
物理 release 路径。阶段 G 在干净构建完成后统一冻结 child、创建 root 所有的
`current` 链接，再从冻结版本安装服务。

`[停止条件]` 父目录不是 `root:root 755`、标签不精确、工作区有改动、不是浅克隆、
发现任何上述生成物，或远端不存在本手册固定标签时停止。不得从本机候选工作区复制
`.git`、`build/`、`devel/`、`install/` 或日志来代替从固定标签重建。

## 6. 阶段 C：安装固定第三方依赖

### 6.1 依赖基线

版本定义在：

```text
deploy/manifests/x86_ubuntu20_versions.env
```

当前固定项包括：

| 依赖 | 版本/来源 | 安装路径 |
| --- | --- | --- |
| CMake/CTest | Kitware 官方 x86_64 归档 `3.20.6`，固定 SHA256 | `/opt/doraemon/deps/cmake-3.20.6` |
| GCC/G++ | Ubuntu 包 `10.5.0`，固定 `/usr/bin/gcc-10`、`/usr/bin/g++-10` | `/usr/bin` |
| Shapely | Ubuntu focal 签名包 `python3-shapely=1.7.0-1build1`，固定包摘要和 Python 模块版本 `1.7.0` | `/usr/lib/python3/dist-packages/shapely` |
| abseil | `20211102.0` 固定提交 | `/opt/doraemon/deps/abseil-20211102.0` |
| OR-Tools | `9.9` 固定提交 | `/opt/doraemon/deps/ortools-9.9` |
| Fields2Cover | `2.0.0` 固定提交 | `/opt/doraemon/deps/fields2cover-2.0.0` |
| FLIRT | Doraemon 兼容快照 | `/opt/doraemon/deps/flirt-doraemon-20260319` |
| ROS | Noetic/Ubuntu 20.04 amd64；USTC ROS1 HTTPS 镜像、官方 ROS 签名 key | `/opt/ros/noetic` |

FLIRT 兼容源码已小体积纳入 `third_party/flirt`，来源和修改说明见
`third_party/flirt/PROVENANCE.md`。不得再依赖某台开发机上的
`/opt/carto/.ThirdParty/flirt`。

### 6.2 执行安装

依赖编译可能较久。并发数应按主板内存调整；首次部署建议从 4 开始：

```bash
cd /opt/doraemon/releases/deployment-2026-07-21-x86-ubuntu20-v4
DORAEMON_BUILD_JOBS=4 ./scripts/install_x86_ubuntu20_dependencies.sh
```

脚本将：

1. 校验 x86_64 和 Ubuntu 20.04。
2. 校验并安装 Kitware 官方 `CMake/CTest 3.20.6` 到固定前缀，并按 manifest 固定值
   校验规范化安装树 SHA256，而不只检查 `cmake --version`。
3. 安装并固定使用 `/usr/bin/gcc-10`、`/usr/bin/g++-10`，完整版本必须为 `10.5.0`。
4. 配置唯一的 USTC ROS1 HTTPS APT 源，校验官方 ROS key 的 SHA256 和 fingerprint，
   再安装系统/ROS 依赖。
5. 从 Ubuntu 20.04 签名仓库精确安装 `python3-shapely=1.7.0-1build1`，校验 APT
   元数据中的包摘要、dpkg 完整性、隔离导入路径、模块版本和基本 GEOS 几何运算；不使用
   pip wheel 或现场源码编译替代。
6. 运行 rosdep 补齐工作空间声明的依赖。
7. 按固定 Git 提交、固定 CMake 和固定编译器编译 abseil、OR-Tools 和 Fields2Cover。
8. 编译仓库内 FLIRT 兼容快照。
9. 从固定模板 `config/doraemon-deps.ld.so.conf` 逐字写入并复核
   `/etc/ld.so.conf.d/doraemon-deps.conf`。
10. 写入 `/etc/doraemon/deps.env` 和 `/etc/profile.d/doraemon-deps.sh`。
11. 验证 Fields2Cover Python 导入、RPATH 和动态库解析。

`[停止条件]` 任一依赖下载、SHA256/fingerprint/提交校验、ROS APT 源唯一性、编译、
RPATH 或 `ldd` 检查失败时停止。缓存中的 CMake/编译器与固定值不一致，或依赖源码
工作区有改动时，脚本会 fail closed；只能记录并经批准后清理对应构建缓存，不得自动
reset/clean，也不得改成任意新版本来绕过错误。

### 6.3 依赖验收

```bash
cd /opt/doraemon/releases/deployment-2026-07-21-x86-ubuntu20-v4
cat /etc/doraemon/deps.env
source /etc/profile.d/doraemon-deps.sh
/opt/doraemon/deps/cmake-3.20.6/bin/cmake --version
/opt/doraemon/deps/cmake-3.20.6/bin/ctest --version
/usr/bin/gcc-10 -dumpfullversion -dumpversion
/usr/bin/g++-10 -dumpfullversion -dumpversion
./scripts/install_x86_ubuntu20_dependencies.sh --verify-cmake-only
./scripts/install_x86_ubuntu20_dependencies.sh --verify-toolchain-only
./scripts/install_x86_ubuntu20_dependencies.sh --verify-shapely-only
./scripts/install_x86_ubuntu20_dependencies.sh --verify-ros1-apt-only
test "$(stat -c '%U:%G %a' /etc/ld.so.conf.d/doraemon-deps.conf)" = \
  'root:root 644'
cmp -s config/doraemon-deps.ld.so.conf \
  /etc/ld.so.conf.d/doraemon-deps.conf
cmake_tree_sha256="$(LC_ALL=C tar --sort=name --mtime=@0 \
  --owner=0 --group=0 --numeric-owner --format=gnu -cf - \
  -C /opt/doraemon/deps/cmake-3.20.6 . | sha256sum | awk '{print $1}')"
test "${cmake_tree_sha256}" = \
  'd59116f0550ef490aeffa2865032f08fac5a14f1fd97c0146aa3cdf85a00dc90'
cat /etc/apt/sources.list.d/ros1.list
ros_source_count="$(sudo grep -RhsE \
  '(deb |URIs:).*https?://[^[:space:]]*/ros/ubuntu' \
  /etc/apt/sources.list /etc/apt/sources.list.d 2>/dev/null | wc -l)"
test "${ros_source_count}" -eq 1
sha256sum /usr/share/keyrings/ros-archive-keyring.gpg
gpg --batch --no-options --no-default-keyring --keyring /dev/null \
  --show-keys --with-colons /usr/share/keyrings/ros-archive-keyring.gpg | \
  awk -F: '$1 == "fpr" {print $10; exit}'
python3 -c "import fields2cover; print(fields2cover.__file__)"
env -u PYTHONHOME -u PYTHONPATH PYTHONNOUSERSITE=1 \
  /usr/bin/python3 -I -c \
  "import shapely; print(shapely.__version__, shapely.__file__)"
ldd /opt/doraemon/deps/fields2cover-2.0.0/lib/libFields2Cover.so
find /opt/doraemon/deps -maxdepth 2 -type d -print
```

期望 CMake/CTest 均为 `3.20.6`，两个编译器均为 `10.5.0`；ROS1 生效源只能有一条：

```text
deb [arch=amd64 signed-by=/usr/share/keyrings/ros-archive-keyring.gpg] https://mirrors.ustc.edu.cn/ros/ubuntu focal main
```

key SHA256 必须为
`4a91c49af0d6f0016108b93698782b596c27ccd836937e18e0e36c3347dc602f`，fingerprint
必须为 `C1CF6E31E6BADE8868B172B4F42ED6FBAB17C654`。`ldd` 不得包含 `not found`，
输出路径不得指向 `/home/third_party`、`/usr/local`、`/opt/carto/.ThirdParty` 或
`/var/tmp/doraemon-deps-build`。

Shapely 验收必须报告 dpkg 包 `python3-shapely`、版本 `1.7.0-1build1`、架构 `amd64`，
隔离 Python 导入必须报告模块版本 `1.7.0` 且路径为
`/usr/lib/python3/dist-packages/shapely/__init__.py`。仅在普通 shell 中能导入某个 pip/user-site
副本不算通过；依赖或全量回归因缺少 Shapely 跳过几何测试也不算通过。

`config/doraemon-deps.ld.so.conf` 与系统文件必须逐字一致；不允许增加现场搜索路径。
CMake 安装树摘要必须等于 manifest 中的 `CMAKE_INSTALLED_TREE_SHA256`，摘要算法会规范化
文件名顺序、时间戳和所有者，但保留内容、权限和符号链接差异。版本字符串正确而安装树
摘要不符仍是 `[停止条件]`。

依赖验收通过后冻结整个依赖树；构建缓存仍保留在 `/var/tmp`，不属于冻结依赖：

```bash
sudo chown -hR root:root /opt/doraemon/deps
sudo chmod -R go-w /opt/doraemon/deps
sudo chown root:root /opt/doraemon/deps
sudo chmod 0755 /opt/doraemon/deps
test -z "$(find /opt/doraemon/deps -xdev \
  \( -type f -o -type d -o -type l \) \
  \( ! -user root -o ! -group root \) -print -quit)"
test -z "$(find /opt/doraemon/deps -xdev \
  \( -type f -o -type d \) -perm /022 -print -quit)"
```

`[停止条件]` 上述任一固定版本、源、key、目录所有权、权限、Python 导入、RPATH 或
动态库检查不符时停止，不进入阶段 D。

## 7. 阶段 D：编译后端工作空间

```bash
cd /opt/doraemon/releases/deployment-2026-07-21-x86-ubuntu20-v4
DORAEMON_BUILD_JOBS=4 ./scripts/build_x86_ubuntu20_workspace.sh
```

脚本会把 `absl_DIR` 和 `FLIRT_ROOT` 显式传给 CMake，并在完成后验证
Fields2Cover、`coverage_planner`、`coverage_task_manager` 和
`robot_hw_bridge`。

检查：

```bash
cd /opt/doraemon/releases/deployment-2026-07-21-x86-ubuntu20-v4
source /opt/ros/noetic/setup.bash
source /etc/profile.d/doraemon-deps.sh
source devel/setup.bash
catkin list
rospack find cartographer
rospack find coverage_planner
python3 -c "import fields2cover"
./scripts/install_x86_ubuntu20_dependencies.sh --verify-shapely-only
```

不要从旧机器复制 `build/` 或 `devel/`。这些目录包含架构、编译器和绝对路径信息。
本阶段由新主板从精确标签生成的 `build/`、`devel/` 是运行制品，阶段 G 复核后可随
release 保留并冻结；catkin 的 `log`/`logs` 只是构建日志，不属于交付制品，结果归档后
必须在冻结前从 release 清除。

`[停止条件]` 干净构建失败、任一要求的 ROS package 找不到、Fields2Cover 不能导入、
固定 Shapely 包/隔离导入/几何运算校验失败或几何回归因缺少 Shapely 被跳过、
构建缓存引用固定版本之外的 CMake/编译器/依赖前缀，或 build/devel 中出现旧机器绝对
路径时停止。不得复制旧构建产物补齐，也不得在尚未完成阶段 G 冻结前安装 systemd unit。

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
nmcli connection show --active
LC_ALL=C nmcli -t -f ACTIVE,SSID device wifi | grep -Fx 'yes:shebei'
ping -c 3 192.168.127.10
ping -c 3 192.168.127.23
ping -c 3 192.168.127.12
```

M-box 和 LiDAR 是后端启动必需项。充电桩暂未上电时可以记录为待验收，但不能
把地址错误当成正常。
本部署批次当前登记的联网维护 SSID 为 `shebei`，必须保持连接；本阶段只配置机器人内部
有线网，不关闭 Wi-Fi，也不给内部网口配置默认路由。后续批次如变更维护 SSID，必须先在
该批次受控手册和单车部署记录中更新，而不是在现场临时猜测。

`[停止条件]` 未经本机插拔/链路状态确认就猜测网口、内部地址冲突、内部网口获得默认
路由、维护 Wi-Fi `shebei` 被断开，或 M-box/LiDAR 不可达时停止。充电桩未上电只能登记为阶段 L
前必须关闭的待验收项。

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
  /opt/doraemon/releases/deployment-2026-07-21-x86-ubuntu20-v4/deploy/udev/99-doraemon-a26022-serial.rules.example \
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
  /opt/doraemon/releases/deployment-2026-07-21-x86-ubuntu20-v4/src/orbbec-ros-sdk/scripts/99-obsensor-ros1-libusb.rules \
  /etc/udev/rules.d/99-obsensor-ros1-libusb.rules
sudo udevadm control --reload-rules
sudo udevadm trigger
lsusb
```

最终插拔一次 USB 设备，确认别名重建。不要只依赖当前 `/dev/ttyUSBN` 顺序。

`[停止条件]` 任一 `REPLACE_*` 未替换、IMU/里程计别名不是本机插拔确认的设备、
串口权限不符、三台 Orbbec 的序列号与本机 USB3 拓扑未逐台确认，或插拔后身份不能
稳定重建时停止。不得复制旧主板的 `ID_PATH` 或按 `/dev/ttyUSBN` 顺序猜测。

## 10. 阶段 G：后端配置和持久化数据

### 10.1 冻结后端版本并安装初始配置

先确认旧 unit（若存在）未运行且未启用。活动服务不会由安装器代为停止：

```bash
systemctl is-active doraemon-runtime.service || true
systemctl is-enabled doraemon-runtime.service || true
```

已有 unit 时必须分别为 `inactive` 和 `disabled`；首次安装时允许 unit 尚不存在。然后
从已完成干净构建的物理 release 目录冻结 child，最后才创建 `current` 链接。先把软件
回归结果写入单车部署记录，再清理仅由本次构建产生的 release-local catkin 日志；不要
清理或替换 `build/`、`devel/`：

```bash
release=/opt/doraemon/releases/deployment-2026-07-21-x86-ubuntu20-v4
cd "${release}"
git describe --tags --exact-match
git status --porcelain=v1 --untracked-files=all
git rev-parse --is-shallow-repository
test -d build
test -d devel
for generated_log_dir in log logs Log; do
  test ! -L "${release}/${generated_log_dir}"
  if test -d "${release}/${generated_log_dir}"; then
    find "${release}/${generated_log_dir}" -xdev -mindepth 1 -delete
    rmdir "${release}/${generated_log_dir}"
  fi
done
test -z "$(find "${release}" -xdev \
  -path "${release}/.git" -prune -o \
  \( \
    -type d \( -iname log -o -iname logs -o -iname test_bag -o \
      -iname image -o -iname point_cloud \) -o \
    -type f \( -iname '*.bag' -o -iname '*.bag.*' -o \
      -iname export.log -o -iname '*.db' -o -iname '*.sqlite*' \) \
  \) -print -quit)"
sudo chown -hR root:root \
  /opt/doraemon/releases/deployment-2026-07-21-x86-ubuntu20-v4
sudo chmod -R go-w \
  /opt/doraemon/releases/deployment-2026-07-21-x86-ubuntu20-v4
sudo chown root:root /opt/doraemon/releases
sudo chmod 0755 /opt/doraemon/releases
sudo ln -sfn \
  /opt/doraemon/releases/deployment-2026-07-21-x86-ubuntu20-v4 \
  /opt/doraemon/current
sudo chown -h root:root /opt/doraemon/current

test -z "$(find /opt/doraemon/releases/deployment-2026-07-21-x86-ubuntu20-v4 \
  -xdev \( -type f -o -type d -o -type l \) \
  \( ! -user root -o ! -group root \) -print -quit)"
test -z "$(find /opt/doraemon/releases/deployment-2026-07-21-x86-ubuntu20-v4 \
  -xdev \( -type f -o -type d \) -perm /022 -print -quit)"
test "$(stat -c '%U:%G %a' /opt/doraemon/releases)" = 'root:root 755'
test "$(stat -c '%U:%G' /opt/doraemon/current)" = 'root:root'
test "$(readlink -f /opt/doraemon/current)" = \
  '/opt/doraemon/releases/deployment-2026-07-21-x86-ubuntu20-v4'
test "$(sudo git \
  -c safe.directory=/opt/doraemon/releases/deployment-2026-07-21-x86-ubuntu20-v4 \
  -C /opt/doraemon/releases/deployment-2026-07-21-x86-ubuntu20-v4 \
  describe --tags --exact-match)" = \
  'deployment-2026-07-21-x86-ubuntu20-v4'
test -z "$(sudo git \
  -c safe.directory=/opt/doraemon/releases/deployment-2026-07-21-x86-ubuntu20-v4 \
  -C /opt/doraemon/releases/deployment-2026-07-21-x86-ubuntu20-v4 \
  status --porcelain=v1 --untracked-files=all)"
sudo git \
  -c safe.directory=/opt/doraemon/releases/deployment-2026-07-21-x86-ubuntu20-v4 \
  -C /opt/doraemon/releases/deployment-2026-07-21-x86-ubuntu20-v4 \
  rev-parse HEAD
```

冻结以后不得再以 `a` 修改 release，也不得把 owner 改回 `a:a`。如需修复代码，回到
候选工作区创建新提交/新标签，并重新浅克隆、干净构建和冻结。

冻结后的 Git 检查必须使用上面的单次
`sudo git -c safe.directory=<物理-release> -C <物理-release> ...` 形式；不要把生产目录
永久加入任一用户的全局 `safe.directory`。
Ubuntu 20.04 当前 Git 2.25.1 不接受部署用户通过命令行 `-c safe.directory` 直接读取
root-owned worktree。商业安装器和 verifier 会先确认物理 release 为规范单层路径、全树
`root:root`、无 group/other 写权限、无嵌套挂载和危险 Git 本地配置，再通过固定操作
allowlist 执行一次性 `sudo -n git` 只读检查；它们不得改用全局 `safe.directory`，也不得
把 release owner 改回 `a`。若 sudo 凭据已失效，先停止并由操作员在终端手动执行
`sudo -v`，再从未产生安装写入的入口重试。
保留 `build/`、`devel/` 的前提是它们确由本新主板从精确标签干净生成，并和整个 child
一起冻结；任何来自旧主板或候选工作区的构建目录仍属于 `[停止条件]`。

只从这个已冻结的物理 release 安装，显式保持“不启用”：

```bash
cd /opt/doraemon/releases/deployment-2026-07-21-x86-ubuntu20-v4
DORAEMON_ENABLE_SERVICE=0 ./scripts/install_doraemon_runtime_service.sh
```

该命令会：

- 创建 `/etc/doraemon/runtime.env`
- 创建 `/data/coverage`、`/data/maps`、`/var/lib/doraemon`、
  `/var/lib/doraemon/orbbec-captures`、
  `/var/log/doraemon/startup` 和 `/var/log/doraemon/slam-runtime`
- 创建空的可选覆盖目录 `/data/config/slam/cartographer`，保持 `root:a 0750`
- 把用户 `a` 加入 `dialout`、`plugdev`、`video`
- 安装 `doraemon-runtime.service`
- 保持服务停止且不开机启动
- 使用 `Restart=no` 使首次启动门禁失败时保持停止，不允许自动重试

发布目录必须保持只读。SLAM runtime 及其 Cartographer 子进程只能写入
`/var/log/doraemon/slam-runtime`，不得在 `/opt/doraemon/releases/<tag>` 内创建
`log/`。Orbbec 的人工保存图像和点云只能写入
`/var/lib/doraemon/orbbec-captures`，不得在发布目录内创建 `image/` 或
`point_cloud/`。

执行后重新登录一次，或在完成所有步骤后重启，以刷新交互式用户组。

### 10.2 编辑每台车配置

```bash
sudoedit /etc/doraemon/runtime.env
```

至少核对：

```text
ROBOT_ID=<已登记的唯一车辆资产编号>
DORAEMON_A_BOX_IFACE=<实际内部网口>
DORAEMON_A_BOX_IP=192.168.127.11
DORAEMON_MBOX_IP=192.168.127.10
DORAEMON_LIDAR_IP=192.168.127.23
STATION_SERVER_IP=192.168.127.12
STATION_SERVER_PORT=5007
DORAEMON_IMU_DEVICE=/dev/imu
DORAEMON_ODOM_DEVICE=/dev/wheel_odom
ODOM_SERIAL_DEVICE=/dev/wheel_odom
RUNTIME_ORBBEC_CAMERA1_SERIAL_NUMBER=<本车左相机序列号>
RUNTIME_ORBBEC_CAMERA2_SERIAL_NUMBER=<本车右相机序列号>
RUNTIME_ORBBEC_CAMERA3_SERIAL_NUMBER=<本车前低障相机序列号>
RUNTIME_ORBBEC_CAMERA1_USB_PORT=<本主板现场路径>
RUNTIME_ORBBEC_CAMERA2_USB_PORT=<本主板现场路径>
RUNTIME_ORBBEC_CAMERA3_USB_PORT=<本主板现场路径>
RUNTIME_START_DEPTH_CAMERAS=true
RUNTIME_REQUIRE_DEPTH_CAMERA_TOPICS=true
DORAEMON_REQUIRE_DEPTH_CAMERA_IDENTITIES=true
RESTART_SITE_GATEWAY_AFTER_ROSBRIDGE=false
DORAEMON_NO_ACTION_ACCEPTANCE=true
DORAEMON_ACTION_TEST_APPROVED=false
ENABLE_MANUAL_DRIVE_SERVICE=false
MCORE_TRANSPORT=tcp
MCORE_TCP_HOST=192.168.127.10
MCORE_TCP_PORT=8080
ROSBRIDGE_ADDRESS=127.0.0.1
DOCK_CALIBRATION_STORAGE_PATH=/data/coverage/dock_calibration.yaml
```

模板中的 `REPLACE_*` 是有意保留的停止门。只要车辆编号、任一相机序列号或
USB3 拓扑仍为占位符，服务必须拒绝启动。相机在生产入口始终按序列号绑定；
USB3 拓扑用于独立审计，启动前还会用目标版本内的 Orbbec SDK 验证
`序列号 ↔ 拓扑` 配对、厂商和 SuperSpeed，不允许在失败时自动降级到另一个设备。

保留当前已经验证的底盘方向、轮径、轮距、编码器和停靠参数，除非机械/算法负责人
有带版本的变更单。不得通过修改源码给单车做参数差异。

首次阶段 K 必须同时保持 `DORAEMON_NO_ACTION_ACCEPTANCE=true` 和
`DORAEMON_ACTION_TEST_APPROVED=false`。该组合会强制关闭底盘 `cmd_vel`、低电量
自动回桩、充电恢复、充电桩 TCP、供排水/对桩控制栈和 Gateway 自动启动。只有进入阶段 L 且机器人测试
负责人现场批准后，才允许先停止服务，把前者改为 `false`、后者改为 `true`，再重新
受控启动；缺少任一条件都必须拒绝动作测试。这不是普通软件部署步骤。

校验环境文件语法：

```bash
bash -n /etc/doraemon/runtime.env
sudo systemctl cat doraemon-runtime.service
systemctl is-enabled doraemon-runtime.service
systemctl is-active doraemon-runtime.service
systemctl show doraemon-runtime.service -p NRestarts --value
```

此时后三项必须分别为 `disabled`、`inactive`、`0`。

`[停止条件]` 标签/浅克隆/clean status 复核失败，`build`/`devel` 不是本机干净构建，
release 内仍有禁止生成物，后端 release 或其父目录未冻结、`current` 不归 root 或指向
错误、配置校验失败、unit 引用了非冻结路径、安装前服务仍活动，或安装后不是
`disabled`/`inactive`/`NRestarts=0` 时停止。不得通过启动服务来“验证安装”。

### 10.3 可选 SLAM 配置覆盖

`/data/config/slam/cartographer` 只用于经代码审查的现场 Cartographer 配置覆盖，不是
服务可写目录。新车默认保持它为空，此时运行时必须使用冻结 release 内置的
`src/cleanrobot/config/slam/cartographer`。空目录本身不是错误，也不要为“避免为空”
从旧车复制配置。

只有具备审查记录和 SHA256 清单的完整覆盖才可由 root 安装。目录及其所有子目录必须
精确为 `root:a 0750`，普通文件必须精确为 `root:a 0640`；不得包含符号链接、挂载点、
socket/device/FIFO 等特殊文件或 group/other 写权限。检查：

```bash
slam_override=/data/config/slam/cartographer
test ! -L "${slam_override}"
test "$(realpath -e "${slam_override}")" = "${slam_override}"
test "$(stat -c '%U:%G %a' "${slam_override}")" = 'root:a 750'
test -z "$(findmnt -rn -o TARGET | awk -v root="${slam_override}" \
  '$0 == root || index($0, root "/") == 1 {print; exit}')"
test -z "$(find "${slam_override}" -xdev -type l -print -quit)"
test -z "$(find "${slam_override}" -xdev \
  \( -type f -o -type d \) \( ! -user root -o ! -group a \) \
  -print -quit)"
test -z "$(find "${slam_override}" -xdev \
  \( -type d ! -perm 0750 -o -type f ! -perm 0640 -o \
     ! -type d ! -type f ! -type l \) -print -quit)"
if test -n "$(find "${slam_override}" -mindepth 1 -print -quit)"; then
  test -f "${slam_override}/slam/config.lua"
  test -f "${slam_override}/pure_location_odom/config.lua"
  test -f "${slam_override}/relocalization/global_relocation.sml"
fi
```

`[停止条件]` 非空覆盖缺少规定布局、审查/摘要/批准记录，或上面任一身份、类型、权限、
符号链接、挂载检查失败时停止。非空但不安全的覆盖必须 fail closed，不得静默回退到
release 配置；先停止并由配置负责人修正或受控清空后重新验收。

### 10.4 新车与换主板的数据边界

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
source scripts/commercial_filesystem_security.sh
backend_release="$(readlink -f /opt/doraemon/current)"
test -z "$(commercial_find_forbidden_release_artifact "${backend_release}")"
commercial_validate_slam_config_override_tree \
  /data/config/slam/cartographer 0
sudo journalctl --disk-usage
```

`ldd` 和禁止生成物检查应无输出；空的 SLAM 覆盖目录必须通过身份/权限检查并使用
release 内置配置。

`[停止条件]` verifier 的任何 failure、`ldd` 的 `not found`、发布树内出现禁止的
日志、capture、bag、`export.log` 或运行数据库，
release/deps 所有权或不可写检查失败、外置可写目录落入 release，或服务不再保持
`disabled`、`inactive`、`NRestarts=0` 时停止。仅手册明确允许的充电桩未上电 warning
可以记录为待阶段 L 关闭的项目；不得忽略其他 warning。

阶段 H 通过后先报告后端无动作安装/预检结果，后端服务继续保持停止且 disabled；可以
继续阶段 I、J 的前端无动作安装，但不能启动任何服务。阶段 J 完成后才请求本次阶段 K
的五项现场安全确认和受控启动批准，不得复用此前批准。

## 12. 阶段 I：获取和构建前端

### 12.1 浅克隆固定版本

```bash
sudo install -d -o root -g root -m 0755 /opt/clean-robot-site/releases
stat -c '%U:%G %a %n' /opt/clean-robot-site/releases
git clone \
  --depth 1 \
  --single-branch \
  --branch deployment-2026-07-21-frontend-v2 \
  https://github.com/yeqiangsheng/clean-robot-frontend.git \
  /home/a/clean-robot-frontend-build
cd /home/a/clean-robot-frontend-build
git describe --tags --exact-match
git status --short
test -f .git/shallow
```

`[停止条件]` 前端固定标签不存在或不精确、工作区有改动、不是浅克隆，或前端
`releases` 父目录不是 `root:root 755` 时停止。

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
cd release/clean-robot-site-v0.1.0-rc.10
npm ci --omit=dev
```

`npm run package:trial` 内部已经执行完整验证；再次列出 `npm run verify` 是为了
让部署记录明确保存验收输出。若工厂使用已经签名和校验的前端制品，可省略目标车
编译，但不得省略制品 SHA256 验证。

安装版本：

```bash
sudo install -d -o root -g root -m 0755 \
  /opt/clean-robot-site/releases/0.1.0-rc.10
sudo cp -a \
  /home/a/clean-robot-frontend-build/release/clean-robot-site-v0.1.0-rc.10/. \
  /opt/clean-robot-site/releases/0.1.0-rc.10/
sudo chown -hR root:root /opt/clean-robot-site/releases/0.1.0-rc.10
sudo chmod -R go-w /opt/clean-robot-site/releases/0.1.0-rc.10
sudo chown root:root /opt/clean-robot-site/releases
sudo chmod 0755 /opt/clean-robot-site/releases
sudo ln -sfn \
  /opt/clean-robot-site/releases/0.1.0-rc.10 \
  /opt/clean-robot-site/current
sudo chown -h root:root /opt/clean-robot-site/current

test -z "$(find /opt/clean-robot-site/releases/0.1.0-rc.10 -xdev \
  \( -type f -o -type d -o -type l \) \
  \( ! -user root -o ! -group root \) -print -quit)"
test -z "$(find /opt/clean-robot-site/releases/0.1.0-rc.10 -xdev \
  \( -type f -o -type d \) -perm /022 -print -quit)"
test "$(stat -c '%U:%G %a' /opt/clean-robot-site/releases)" = 'root:root 755'
test "$(stat -c '%U:%G' /opt/clean-robot-site/current)" = 'root:root'
test "$(readlink -f /opt/clean-robot-site/current)" = \
  '/opt/clean-robot-site/releases/0.1.0-rc.10'
```

`[停止条件]` Node/npm 固定版本检查、`npm ci`、`npm run verify`、生产打包或生产依赖
安装失败，制品内容不完整，release/父目录冻结失败，或 `current` 不是 root 所有并精确
指向 `0.1.0-rc.10` 时停止。不得从旧机复制 `node_modules`、发布包或前端 SQLite。

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

`[停止条件]` 三个角色缺失、使用历史公共默认口令、外置配置权限不符，或
`robotId`/`siteName`/ROS 地址不属于当前登记车辆时停止。已知公共默认口令不能以工程
偏差名义继续使用。

三个角色未使用当前车辆各自独立的强口令，或支持名称、电话、邮箱仍为占位值，属于
阶段 M 的 `[停止条件]` 和正式商业发布阻断。首次批量工程试验如确需暂缓，只能由负责人
逐车批准并在该车
部署记录中登记偏差、范围、到期时间和复验入口，不得在本通用手册中写入口令或单车偏差。
这种批准最多允许继续明确限定的无动作工程验收，不能记为商业验收通过，也不得进入阶段
L、M 或交付客户。关闭偏差时必须分别轮换三角色强口令、补齐真实支持信息，并重新执行
安装器、三角色登录/权限、车辆身份和支持信息显示检查；记录保管位置和结果，不记录口令
明文。

### 13.2 安装但不启动服务

安装前先检查已有前端 unit（若存在）。已有 unit 必须已经是 `inactive`、`disabled`；
首次安装允许 unit 尚不存在，但绝不允许安装器替现场停止一个活动服务：

```bash
if systemctl cat clean-robot-site-gateway.service >/dev/null 2>&1; then
  test "$(systemctl is-active clean-robot-site-gateway.service)" = inactive
  test "$(systemctl is-enabled clean-robot-site-gateway.service)" = disabled
fi
```

```bash
cd /opt/clean-robot-site/current
sudo SITE_SERVICE_USER=a \
  SITE_ROSBRIDGE_URL=ws://127.0.0.1:9090 \
  SITE_ENABLE_SERVICE=0 \
  SITE_START_SERVICE=0 \
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
systemctl show clean-robot-site-gateway.service -p NRestarts --value
```

安装后必须精确为 `disabled`、`inactive`、`NRestarts=0`。前端 release 和
`/opt/clean-robot-site/releases` 仍必须保持上一阶段的 root 冻结状态。

`[停止条件]` 安装前已有 unit 不是 inactive/disabled、安装器尝试启动或启用、安装后
不是 `disabled`/`inactive`/`NRestarts=0`，unit 引用非冻结 release，或 release/父目录
所有权和权限发生变化时停止。此阶段不得用 `systemctl start` 做试运行。

阶段 J 结束时必须报告前后端标签/提交、外置配置权限、账号与支持信息复核结果，以及
两个 unit 的 `disabled`、`inactive`、`NRestarts=0` 状态。随后停止推进，取得针对本次
启动重新作出的阶段 K 五项现场安全确认和明确批准后，才可启动后端。

## 14. 阶段 K：首次受控启动

### 14.1 再次确认物理安全

- 急停可立即切断运动。
- 驱动轮离地。
- 清洁执行器处于安全断开/禁用状态。
- 车辆周围无人员、线缆和障碍物。
- 两名人员均已就位。

`[停止条件]` 每次首次进入或重新进入阶段 K 前，必须由现场负责人逐项重新人工确认
上述五项，并在单车部署记录中写明确认人、确认时间、五项结果和本次启动批准。不得复用
此前某次启动的确认；未取得本次明确批准时，不得执行下面的 `systemctl start`。

本次启动前还必须确认两个 unit 仍未启用、没有自动重启记录：

```bash
test "$(systemctl is-enabled doraemon-runtime.service)" = disabled
test "$(systemctl is-enabled clean-robot-site-gateway.service)" = disabled
test "$(systemctl show doraemon-runtime.service -p NRestarts --value)" = 0
test "$(systemctl show clean-robot-site-gateway.service -p NRestarts --value)" = 0
```

任一检查失败均为 `[停止条件]`。先调查并记录此前启动/重启原因，不得通过 enable、
自动重启或反复启动绕过首次受控启动门。

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
rosparam get /coverage_task_manager/auto_charge_enable
rosnode list | grep -E '^/(mcore_velocity_sender|mcore_tcp_bridge|station_tcp_bridge|dock_supply_manager|dock_tracker|docking_controller|auto_charge_monitor|manual_drive_service|wheeltec_robot)$'
rosservice list | grep -E '^(/dock_supply/(start|exit|recovery_retreat)|/clean_robot_server/app/(manual_drive_command|get_manual_drive_status))$'
rostopic hz /scan
rostopic hz /imu
rostopic hz /odom
rosservice call /gemini_cf/get_serial '{}'
rosservice call /gemini_nj/get_serial '{}'
rosservice call /gemini_front/get_serial '{}'
rostopic hz /gemini_cf/depth/image_raw
rostopic hz /gemini_nj/depth/image_raw
rostopic hz /gemini_front/depth/image_raw
rostopic hz /gemini_cf/depth/points
rostopic hz /gemini_nj/depth/points
rostopic hz /gemini_front/depth/points
rosrun coverage_planner check_ros_contracts.py --strict --exclude-manual-drive --text
rosrun coverage_planner run_backend_runtime_smoke.py --text
```

最后两个命令为只读/非执行动作验收入口。此阶段不要运行带
`--allow-write-actions`、`--actions` 或 `--run-task-cycle` 的脚本。当
`DORAEMON_NO_ACTION_ACCEPTANCE=true` 时，启动脚本会拒绝非空的
`BACKEND_RUNTIME_SMOKE_ACTIONS`、`BACKEND_RUNTIME_SMOKE_EXTRA_ARGS` 和
`BACKEND_PRODUCTION_ACCEPTANCE_EXTRA_ARGS`；如启用合并生产验收，只允许
`read_only_gate` 且必须禁用写动作。
`/coverage_task_manager/auto_charge_enable` 必须为 `false`，且 `rosnode list` 中不得
出现上述任一动作传输节点，两个 `grep` 命令都应无输出；否则立即停止。

无动作模式要求 manual-drive 节点/服务不存在，且 `/cmd_vel` 不能连接任何底盘订阅者。
规划栈启动后可能静态注册 `/coverage_executor`、`/move_base_flex` 两个已审计 publisher；
除此以外不允许其他 publisher，尤其不允许 `/manual_drive_service`。执行以下硬门：

```bash
cmd_vel_info="$(rostopic info /cmd_vel 2>/dev/null || true)"
cmd_vel_publishers="$(awk '
  /^Publishers:$/ {in_publishers=1; next}
  /^Subscribers:$/ {in_publishers=0}
  in_publishers && /^[[:space:]]*\*[[:space:]]+\// {
    line=$0
    sub(/^[[:space:]]*\*[[:space:]]+/, "", line)
    sub(/[[:space:]].*$/, "", line)
    print line
  }
' <<<"${cmd_vel_info}")"
test -z "$(grep -Ev '^/(coverage_executor|move_base_flex)$' <<<"${cmd_vel_publishers}")"
test -z "$(awk '
  /^Subscribers:$/ {in_subscribers=1; next}
  in_subscribers && /^[[:space:]]*\*[[:space:]]+\// {print}
' <<<"${cmd_vel_info}")"
```

然后实际检查 ROS master 和 rosbridge 的监听地址。不能只检查配置文件，也不能用 UFW
替代进程绑定检查：

```bash
check_loopback_listener() {
  local port="$1" endpoint seen=0
  while IFS= read -r endpoint; do
    seen=1
    case "${endpoint}" in
      "127.0.0.1:${port}"|"[::1]:${port}") ;;
      *) echo "[FAIL] non-loopback listener: ${endpoint}" >&2; return 1 ;;
    esac
  done < <(sudo ss -H -ltn | awk -v port="${port}" '$4 ~ (":" port "$") {print $4}')
  test "${seen}" -eq 1
}

sudo ss -H -ltnp | awk '$4 ~ /:(9090|11311)$/ {print}'
check_loopback_listener 9090
check_loopback_listener 11311
test "$(systemctl is-enabled doraemon-runtime.service)" = disabled
test "$(systemctl show doraemon-runtime.service -p NRestarts --value)" = 0
```

`9090`、`11311` 任一没有 listener，或出现 `0.0.0.0`、`[::]`、`*`、管理/设备网 IP
等非 `127.0.0.1`/`::1` 地址，manual-drive 服务、非 allowlist 的 `/cmd_vel` publisher
或任一 `/cmd_vel` subscriber 存在，
backend unit 被 enable，或 `NRestarts` 不为 `0`，均为阶段 K `[停止条件]`。立即停止
本次启动并保留日志，不进入前端启动，也不得靠防火墙掩盖 wildcard bind。

保持三台相机连续出流至少 5–10 分钟，同时在另一个终端记录：

```bash
sudo journalctl -k --since '10 minutes ago' --no-pager | \
  grep -Ei 'usb|xhci|disconnect|reset|over-current|-71|-110'
```

三个 namespace 返回的序列号必须分别等于本车配置；六个图像/点云 topic 必须持续
有稳定频率。活动流期间出现 USB disconnect/reset、`-71`、`-110`、过流、带宽或
供电错误属于阶段 K 停止条件。启动脚本的单帧就绪门不能替代本项持续稳定性验收。

### 14.3 启动前端

后端不得自动启动或重启 Site Gateway；只有 14.2 无动作检查通过后，才单独执行：

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

前端启动后重复最终无动作硬门：

```bash
check_loopback_listener() {
  local port="$1" endpoint seen=0
  while IFS= read -r endpoint; do
    seen=1
    case "${endpoint}" in
      "127.0.0.1:${port}"|"[::1]:${port}") ;;
      *) echo "[FAIL] non-loopback listener: ${endpoint}" >&2; return 1 ;;
    esac
  done < <(sudo ss -H -ltn | awk -v port="${port}" '$4 ~ (":" port "$") {print $4}')
  test "${seen}" -eq 1
}

check_loopback_listener 9090
check_loopback_listener 11311
for service in doraemon-runtime.service clean-robot-site-gateway.service; do
  test "$(systemctl is-enabled "${service}")" = disabled
  test "$(systemctl show "${service}" -p NRestarts --value)" = 0
done
```

`[停止条件]` 前端健康检查、车辆身份/权限显示、loopback listener、服务 disabled、
`NRestarts=0` 或任一阶段 K 无动作检查失败时，执行
`sudo systemctl stop clean-robot-site-gateway.service doraemon-runtime.service`，保存日志并
停止阶段 K。无动作验收完成后必须先向现场负责人报告结果；只有取得新的动作测试批准，
才可进入阶段 L，阶段 K 本身不启用开机启动。

## 15. 阶段 L：车辆、地图和充电桩现场验收

以下阶段必须由机器人测试负责人批准后进行。批准后先停止后端，在
`/etc/doraemon/runtime.env` 中设置：

```text
DORAEMON_NO_ACTION_ACCEPTANCE=false
DORAEMON_ACTION_TEST_APPROVED=true
MCORE_MAX_ABS_LINEAR_VELOCITY=<机器人测试负责人批准的有限正数，单位 m/s>
MCORE_MAX_ABS_ANGULAR_VELOCITY=<机器人测试负责人批准的有限正数，单位 rad/s>
```

模板中的两个 M-core 上限是有意设置的 `0.0`，用于让动作模式 fail closed。不得把
`0.0` 当成可接受限速，也不得由部署人员猜测数值。两个值都必须是机器人测试负责人
批准的有限正数；单车部署记录必须写明数值、单位、批准人、批准时间和依据（测试方案、
机械/控制参数版本或变更单）。空值、非数字、`NaN`、`Inf`、零或负数都是
`[停止条件]`。

若阶段 L 要使用前端 manual drive，还必须显式设置 `ENABLE_MANUAL_DRIVE_SERVICE=true`，
并让以下五个 manual-drive 门全部为 `true`：

```text
MANUAL_DRIVE_REQUIRE_ROLE=true
MANUAL_DRIVE_REQUIRE_SLAM_STATE=true
MANUAL_DRIVE_REQUIRE_TASK_STATE=true
MANUAL_DRIVE_REQUIRE_ODOMETRY_STATE=true
MANUAL_DRIVE_REQUIRE_COMBINED_STATUS=true
```

任一门关闭时启动必须 fail closed。若本次不用 manual drive，则保持
`ENABLE_MANUAL_DRIVE_SERVICE=false`，不得为“方便测试”临时绕过门禁。

复核物理安全条件后再受控启动。两项动作批准变量不是上述组合、M-core 两个限速未按
要求批准记录、manual-drive 启用但五门未全开、批准未记录或服务未先停止时，均为
`[停止条件]`，不得执行任何运动、回桩、充电、供排水或清洁执行器测试。

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

`[停止条件]` 急停、任一方向/里程计符号、限速、传感器安全链、任务、充电桩标定、
回桩/充电、执行器反馈或停止结果不符合批准的验收标准时，立即停止相关动作和后端，
保存记录并停止阶段 L。不得带未关闭的动作问题进入阶段 M。

## 16. 阶段 M：启用开机运行

只有全部验收通过后执行。任一车辆的三个角色只要没有使用该车各自独立的强口令，或支持
名称、电话、邮箱任一项仍为模板值，就属于最终商业验收阻断，不得执行本阶段、不得交付。

阶段 K 的进程级网络门也必须再次通过：`9090` 和 `11311` 只能监听
`127.0.0.1`/`::1`。任一端口监听 `0.0.0.0`、`[::]`、`*` 或任一本机管理/设备网 IP
都属于 ROS wildcard 暴露，是阶段 M `[停止条件]`；UFW 规则不能替代修复 listener。
同时复核两个服务在 enable 前仍为 `NRestarts=0`，并把 `ss` 和 `systemctl show` 输出
归档到单车部署记录。

满足全部门禁后才执行：

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

`[停止条件]` 任一阶段 A–L 验收未关闭、独立强口令/真实支持信息未复验、ROS listener
不是 loopback-only、`NRestarts` 不为 `0`、充电桩标定/动作验收未通过，或本车记录不完整
时，不得 enable、reboot 或交付。

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

`9090` 或 `11311` 的 wildcard/non-loopback 监听不是“后续加固项”，而是阶段 K 立即
停止条件和阶段 M 商业放行阻断。必须先修正进程绑定并重新执行 K；不得用 UFW、路由器
ACL 或“现场网络可信”替代 loopback-only。

本轮批量部署测试仍依赖现场 Wi-Fi `shebei` 进行联网维护，因此现在保持该连接，不关闭
NetworkManager Wi-Fi，也不在本轮配置或启用 UFW。只有替代维护链路、实际管理网段、
回滚方式和现场批准均已确认后，才能另开受控变更执行防火墙配置；该后续变更也不得
改变机器人内部有线网“无默认路由”的要求。

后续受控变更单中的 UFW 规则至少应达到以下意图（本轮不要执行）：

```text
allow from <已批准的管理网段> to tcp/22
allow from <已批准的管理网段> to tcp/4173
deny other unapproved ingress
```

`[停止条件]` 未确认远程维护链路、真实管理网段、现场回滚路径和批准记录时，不得把
上述意图转成 `ufw` 命令；尤其不得在当前依赖 Wi-Fi 的部署过程中启用防火墙或关闭
Wi-Fi。

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
backend_release="$(readlink -f /opt/doraemon/current)"
sudo git -c safe.directory="${backend_release}" \
  -C "${backend_release}" rev-parse HEAD
git -C /home/a/clean-robot-frontend-build rev-parse HEAD
dpkg-query -W -f='${Package}\t${Version}\n'
find /opt/doraemon/deps -type f -name '*.so*' -print0 | sort -z | xargs -0 sha256sum
find /opt/clean-robot-site/current -type f -print0 | sort -z | xargs -0 sha256sum
```

## 20. 升级和回滚

### 20.1 后端升级

1. 保持 `/opt/doraemon/releases`、`/opt/doraemon/deps` 为 `root:root 0755`；仅预创建
   `/opt/doraemon/releases/<new-tag>` 为 `a:a`，浅克隆新的不可变标签。
2. 比较新旧 `deploy/manifests/x86_ubuntu20_versions.env`；依赖有变化时安装到新的固定
   前缀，不得覆盖旧版本依赖。
3. 在 new child 中干净构建并运行可离线完成的软件检查；此时不要安装 unit。
4. 按物理安全流程停止并 disable 现有后端，确认 `inactive`，备份 `/etc/doraemon`
   和 `/data`。
5. 把 new child `chown -hR root:root`、移除 group/other 写权限；复核 releases 父目录
   `root:root 0755`。冻结后的标签、提交和 clean status 必须用单次
   `sudo git -c safe.directory=<new-release> -C <new-release> ...` 复核，不得写全局
   `safe.directory`。
6. 从冻结 new child 以 `DORAEMON_ENABLE_SERVICE=0` 重新运行
   `install_doraemon_runtime_service.sh`，确认 unit 为 `disabled`、`inactive`、
   `NRestarts=0`。
7. 以 root 切换 `/opt/doraemon/current` 并 `chown -h root:root`，再次运行阶段 H；随后
   必须重新取得阶段 K 五项现场批准，先无动作验收，再按阶段 L/M 恢复动作和开机运行。

回滚时先停止并 disable 服务，确认旧 release 仍为完整的 root 冻结版本，再从其物理
路径以 `DORAEMON_ENABLE_SERVICE=0` 重新安装 unit，恢复经校验的配置备份并以 root
切换 `current`。回滚同样从阶段 H/K 重新验收，不能直接启动或自动恢复 enable。不要用
`git reset --hard` 在生产目录原地回退。

### 20.2 前端升级

1. 保持 `/opt/clean-robot-site/releases` 为 `root:root 0755`，把新发布包安装到
   `/opt/clean-robot-site/releases/<new-version>`。
2. 校验制品后把 new child 冻结为 `root:root` 且移除 group/other 写权限；保留
   `/etc/clean-robot-site` 和 `/var/lib/clean-robot-site`。
3. 停止并 disable 现有 Site Gateway，确认 `inactive`、`disabled`。
4. 以 root 切换 `/opt/clean-robot-site/current`，并确认链接本身为 `root:root`。
5. 从新版本以 `SITE_ENABLE_SERVICE=0 SITE_START_SERVICE=0` 重新运行
   `install-site-systemd.sh`，确认仍为 `inactive`、`disabled`、`NRestarts=0`。
6. 重新取得阶段 K 现场批准后受控启动，检查 `/api/health`、三角色登录/权限、车辆
   身份和 ROS loopback 连接；验收前不得恢复 enable。

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
backend_release="$(readlink -f /opt/doraemon/current)"
source "${backend_release}/scripts/commercial_filesystem_security.sh"
test -z "$(commercial_find_forbidden_release_artifact "${backend_release}")"
find /opt/doraemon /opt/clean-robot-site /data \
  -type f \( -iname '*.bag' -o -iname '*.bag.*' \) -print
du -sh /opt/doraemon /opt/clean-robot-site /data /var/log/doraemon
sudo journalctl --disk-usage
```

第一项 release 禁止生成物检查和第二项全机 bag 检查都应无输出。生产车默认不保存测试
bag。确需现场录包时，应设置工单、最大时长、脱敏要求、
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
- [ ] 后端/依赖/前端三个父目录均为 `root:root 0755`，两个 release child 和依赖树已冻结，两个 `current` 链接归 root
- [ ] 后端 `build/`、`devel/` 由本新主板从固定标签干净生成并冻结，未复制旧机或候选工作区制品
- [ ] 后端 release 无 `log`/`logs`/`Log`、capture、任何 bag、`export.log` 或运行数据库
- [ ] CMake/CTest 为固定 `3.20.6` 且安装树摘要匹配 manifest，GCC/G++ 为固定 `10.5.0`，ROS1 仅使用已校验 key 的 USTC HTTPS 源
- [ ] Shapely 为 Ubuntu focal 固定包 `python3-shapely=1.7.0-1build1`，隔离导入、dpkg 完整性和几何回归通过且没有缺依赖跳过
- [ ] `/etc/ld.so.conf.d/doraemon-deps.conf` 为 `root:root 0644` 且与 `config/doraemon-deps.ld.so.conf` 逐字一致
- [ ] 依赖全部位于 `/opt/doraemon/deps`，`ldd` 无 `not found`
- [ ] 没有运行时依赖 `/home/third_party`、`/usr/local` 或旧用户目录
- [ ] 内部网口、M-box、LiDAR 和充电桩地址已登记
- [ ] IMU、里程计、M-core udev 规则在本机实测
- [ ] Orbbec 深度相机和 LiDAR 数据稳定
- [ ] `/etc/doraemon/runtime.env` 已按本车复核
- [ ] `/data/config/slam/cartographer` 为空并使用 release 内置配置，或非空覆盖已完成审查、摘要及 `root:a 0750/0640` 安全复核
- [ ] `operator`、`service`、`engineer` 已从临时共用弱口令轮换为本车三组独立强口令，且三角色登录和权限复验通过
- [ ] 支持名称、电话和邮箱已替换为真实批准值，前端显示复验通过
- [ ] 前端 SQLite 位于 `/var/lib/clean-robot-site`
- [ ] 地图和数据库位于 `/data`，没有复制其他新车数据
- [ ] 只读 contract 和 runtime smoke 通过
- [ ] 无动作阶段 manual-drive 节点/服务不存在，`/cmd_vel` 无 subscriber 且 publisher 仅限已审计 allowlist
- [ ] `9090`、`11311` 实测仅监听 loopback，阶段 K 两服务保持 disabled 且 `NRestarts=0`
- [ ] 急停、低速底盘、传感器安全链通过
- [ ] 两个 M-core 动作限速为测试负责人批准的有限正数，数值、单位和依据已归档
- [ ] 当前车辆完成建图和任务流程验收
- [ ] 充电桩标定已保存、重启后可读取、回桩和充电通过
- [ ] 清洁执行器逐项验收通过
- [ ] 后端和前端仅在验收后启用开机启动
- [ ] 重启后系统、前端和日志检查通过
- [ ] 代码、依赖、配置和制品 SHA256 已归档
- [ ] 无测试 bag、历史发布包和无用大文件
- [ ] 当前维护 Wi-Fi `shebei` 未被部署流程关闭；UFW 延后变更的链路、网段、回滚和批准已明确记录
- [ ] 阶段 A–M 均有结果报告，所有停止条件和暂缓项均已关闭
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

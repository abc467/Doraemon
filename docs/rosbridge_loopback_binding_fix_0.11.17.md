# rosbridge 0.11.17 回环监听修复说明

适用后端发布标签：`deployment-2026-07-21-x86-ubuntu20-v5`；v5 延续 v4，v4 延续 v3
中已加强的 v2 回环监听修复，本次发布未改变该补丁内容。

## 问题与边界

ROS 参数 `/rosbridge_websocket/address` 已配置为 `127.0.0.1`，但 Ubuntu
20.04 / ROS Noetic 的 `rosbridge_server 0.11.17` 调用 Autobahn `listenWS`
时没有传入 `interface`，因此内核实际监听 `0.0.0.0:9090`。这违反商业部署
手册“浏览器只能经 Site Gateway 访问 ROS”的网络边界。

## 固定修复

本版本在 `src/rosbridge_server` 纳入官方 `0.11.17` 包源码，并逐字应用上游
修复提交 `f6a829abaeca9763c5d00ba5a232407e789bdbfa` 的单行修改：

```python
listenWS(factory, context_factory, interface=factory.host)
```

同时把 vendored `rosbridge_server` 的 Python 空值回退、通用 launch 默认值和
Doraemon 自有运行入口全部收紧为 `127.0.0.1`；商业入口仍显式传入回环地址，
任何空值都不能退回通配监听。完整来源、哈希和许可证见
`src/rosbridge_server/PROVENANCE.md`。

## 构建门禁

```bash
python3 scripts/verify_rosbridge_loopback_patch.py
DORAEMON_BUILD_JOBS=2 ./scripts/build_x86_ubuntu20_workspace.sh
source /opt/ros/noetic/setup.bash
source devel/setup.bash
rospack find rosbridge_server
```

最后一条必须指向当前不可变发布目录中的 `src/rosbridge_server`，不得解析到
`/opt/ros/noetic/share/rosbridge_server`。

## 阶段 K 门禁

受控启动后必须同时满足：

1. `ss -ltnp '( sport = :9090 )'` 仅出现 `127.0.0.1:9090`。
2. `127.0.0.1:9090` WebSocket 可连接。
3. 主板 Wi-Fi 地址和机器人内部有线地址的 `9090/tcp` 均不可连接。
4. Site Gateway `/api/health` 显示 ROS 已连接。
5. 实际 rosbridge 可执行文件和 `rospack find` 都来自当前发布 overlay。
6. 两个 systemd 服务验收后仍保持 `disabled`，Wi-Fi 连接不变。

任一项失败均为停止条件：立即停止 Gateway 和后端，不进入动作验收，不启用
systemd 开机启动。

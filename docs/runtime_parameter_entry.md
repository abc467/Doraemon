# Doraemon 商用运行参数入口

正式整机启动入口是 `scripts/start_runtime.sh`，每台车唯一的现场配置入口是
`/etc/doraemon/runtime.env`。冻结 release 中的 `config/runtime.a26022.env` 只用于
首次安装模板，不得把 release 改回可写后直接编辑。

## 优先级

1. 前端“充电桩标定/精对接参数调试”保存的值，持久化在
   `/data/coverage/dock_calibration.yaml`，启动后由
   `dock_calibration_service` 写回 ROS 参数。
2. `/etc/doraemon/runtime.env`，由 `start_runtime.sh` 校验所有权、权限和内容后读取，
   再显式传给各级 launch。
3. 各级 launch 文件中的 `default`，只作为单独启动模块时的兜底值。
4. 节点代码内部默认值，只作为最后兜底。

## 现场常调参数

`DOCK_TARGET_DIST=0.783`

精对接目标距离，单位 m。

`DOCK_XY_TOLERANCE=0.003`

精对接容差，单位 m。当前停车/成功阈值为
`DOCK_TARGET_DIST + DOCK_XY_TOLERANCE`。想让小车距离充电桩更远，调大
`DOCK_TARGET_DIST` 或 `DOCK_XY_TOLERANCE`；想更靠近，调小。

`DOCK_YAW_TOLERANCE=0.04`

精对接角度容差，单位 rad，0.04rad 约等于 2.3 度。距离进入停车阈值后会先
硬停车；该角度只决定这次是否按正常精对接成功结束，不能让小车继续向前顶桩。

`DOCK_POSE_SCORE_THRESH=0.00012`

充电桩梯形 ICP 匹配分数阈值。

`STATION_SERVER_IP=192.168.127.12`

桩侧 TCP bridge 连接 IP。

`AUTO_CHARGE_LOW_SOC=0.15`
`AUTO_CHARGE_RESUME_SOC=0.95`
`AUTO_CHARGE_REARM_SOC=0.95`
`AUTO_CHARGE_TARGET_SOC=0.95`

自动回充触发、恢复、重置和目标电量阈值。

`DOCK_SUPPLY_ENABLE_DRAIN=true`
`DOCK_SUPPLY_ENABLE_REFILL=false`
`DOCK_SUPPLY_DRAIN_TIMEOUT_S=600.0`
`DOCK_SUPPLY_DRAIN_SETTLE_S=30.0`
`DOCK_SUPPLY_COMBINED_STATUS_WAIT_S=5.0`
`DOCK_SUPPLY_COMBINED_STATUS_STALE_TIMEOUT_S=3.0`

精对接补给流程当前为：充电达到目标 SOC、关闭车体和桩侧充电、排污，收到新鲜的
`/combined_status.sewage_level == 0` 后立即关闭桩侧排污和车体污水阀，原地静止
30 秒，随后结束补给并按 `2.2m / 0.10m/s` 进入离桩流程。加清水功能关闭。
污水数据缺失、失联或 600 秒内未降到 0 时，流程关闭排污输出并以明确故障结束，
不会把未知污水值当作排空成功。

`AUTO_CHARGE_MONITOR_RECOVERY_TIMEOUT_S=180.0`
`AUTO_CHARGE_MONITOR_RECOVERY_MAX_ATTEMPTS=2`

自动回充监控恢复参数：进入充电命令阶段后，3 分钟内 SOC 上涨不足
`0.001` 时，执行标准离桩并重新运行完整自动回充流程，每个回充周期最多恢复
2 次。最后一次恢复后再次观察 3 分钟，仍未达到阈值则判定恢复耗尽，标准离桩、
暂停任务并提示人工检查。

## 生效方式

日常现场调参必须使用 `sudoedit` 修改本车的外部配置：

```bash
sudoedit /etc/doraemon/runtime.env
```

修改后先按商业部署手册重新检查语法、权限、车辆身份和无动作门禁。只有已进入获批的
受控启动/调试阶段，才允许重启 runtime：

```bash
sudo systemctl restart doraemon-runtime.service
```

旧入口 `./scripts/install_a26022_runtime_env.sh` 已退役并会拒绝执行，避免它覆盖本车
配置或擅自启用服务。首次安装或修复 systemd 服务时，只能从已冻结的精确标签 release
显式执行以下商业安装入口；它会保持服务 `disabled`、`inactive`，且不会启动：

```bash
DORAEMON_ENABLE_SERVICE=0 ./scripts/install_doraemon_runtime_service.sh
```

如果通过前端保存精对接参数，保存值会写入
`/data/coverage/dock_calibration.yaml`，重启后仍会优先生效。

# Doraemon x86 Ubuntu 20.04 v9 发布说明

固定发布组合：

- 后端：`deployment-2026-07-24-x86-ubuntu20-v9`
- 前端：`deployment-2026-07-22-frontend-v3`
- 前端版本：`0.1.0-rc.11`

## 本版变更

- `map_constraints` 显式接收当前车辆的 `robot_id`，避免地图约束查询落入空车辆作用域。
- 地图 revision 的 `hardDelete` 和 `cleanupDisabled` 增加运行审计引用门禁。
  当 `mission_runs`、`mission_checkpoints`、`robot_runtime_state` 或未完成
  `slam_jobs` 仍引用该 revision 时，即使请求 `cascade=true` 也会 fail closed，
  防止物理删除后留下孤立运行记录。
- 无法完整扫描 `planning.db` / `operations.db`、固定审计表或列缺失时，地图物理
  删除 fail closed；实际删除文件前还会重新解析 revision 并复核全部引用，防止
  候选扫描后新出现的引用被静默孤立。
- v9 完整继承 v8 的 rosbridge 回环监听、Orbbec 枚举、M-core 动作链、地图
  revision 切换和商业启动门禁；未放宽既有安全边界。
- 商业部署手册、固定版本 manifest 和不可变发布测试已统一到上述后端/前端组合。

## CR-001 数据说明

CR-001 在 v9 安装前对一个已经物理删除的旧测试地图进行了受控数据修复：
先使用 SQLite backup API 生成一致性回滚库并导出历史记录，再定向清理由旧版本
删除流程遗留的孤立 run/checkpoint/event。该操作属于有备份、有人批准的部署修复，
不是 v9 自动迁移；v9 通过删除前审计门禁防止同类状态再次产生。

## 发布与验证要求

- 候选工作区必须 clean，提交后创建注解标签。
- 从该标签执行全新浅克隆和干净构建，不复用 v8 的 `build/`、`devel/` 或日志。
- 运行地图资产、revision 数据健康、运行参数、不可变发布、rosbridge 回环及
  后端商业验收测试。
- 前端 v3/rc.11 不修改；重新打包时必须与已验收制品逐文件一致。
- 本地标签未推送远端时只能作为限时试产基线，不得宣称已完成公司制品库发布。

## CR-001 未发布标签试产偏差

本次按负责人批准不推送远端，因此不满足商业部署手册阶段 B“规范远端存在固定标签”
的正式交付门禁。CR-001 如需从本地标签构建，只能将逐车试产例外精确绑定到 v9
提交并保留原到期时间 `20260728T235959Z`；到期或换车后不得复用。正式批量交付前
仍须把注解标签发布到规范远端，再从该远端重新浅克隆、构建并归档。

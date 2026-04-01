# 约束编辑前端一期验收总结 v1

这份文档用于记录本轮“约束编辑前端一期”的开发范围、实际联调结果、当前已知限制和后续建议。

本文档面向：

- 前端开发
- 后端开发
- 联调与测试
- 项目阶段验收与留档

---

## 1. 一期目标范围

本期约束编辑前端目标是完成以下能力：

- 禁行区（No-Go Area）只读展示
- 禁行区详情展示
- 禁行区新增 / 修改 / 删除
- 虚拟墙（Virtual Wall）只读展示
- 虚拟墙新增 / 修改 / 删除

本期不包含：

- 多边形 holes 编辑
- 复杂自由多边形编辑器
- 复杂多段虚拟墙高级编辑器
- 任务 / 调度 / 执行控制页面

---

## 2. 对接后端接口

本期前端对接的后端接口如下：

- `/database_server/no_go_area_service`
  - 类型：`my_msg_srv/OperateMapNoGoArea`
- `/database_server/virtual_wall_service`
  - 类型：`my_msg_srv/OperateMapVirtualWall`

辅助依赖接口：

- `/clean_robot_server/map_server`
- `/database_server/map_alignment_service`
- `rosbridge websocket`
  - `ws://10.0.0.174:9090`

---

## 3. 当前前端实现结论

### 3.1 已完成能力

- 地图工作台只读底图渲染
- Zone 图层展示与详情
- Alignment 查询与两点确认
- Zone 新建 / 预览 / 提交 / 更新 / 冲突处理
- No-Go Areas 分组与详情面板
- Virtual Walls 分组与详情面板
- Virtual Wall 新增 / 修改 / 删除
- 诊断面板保留为默认折叠的 debug-only 区

### 3.2 当前编辑规则

前端当前遵循的约束是：

- 禁行区只编辑 `display_region`
- 虚拟墙只编辑 `display_path`
- 不直接编辑 `map_region / map_path`
- 每次增删改后优先重新调用 `getAll` 刷新列表
- 当前列表默认使用 `include_disabled=false`

---

## 4. Milestone 验收结果

### 4.1 Milestone 1：禁行区只读层与详情

结论：**通过**

已确认内容：

- 左侧对象列表已按分组展示：
  - `Map`
  - `Zones`
  - `No-Go Areas`
  - `Virtual Walls`
- 右侧已接入 no-go 专属详情卡
- 详情字段已覆盖：
  - `area_id`
  - `display_name`
  - `enabled`
  - `display_frame`
  - `updated_ts`
  - `warnings`
- 画布 no-go 图层与高亮机制保留并可复用

备注：

- 当时 live backend 中 no-go 数量为 `0`
- 因此这轮主要完成了接口对齐、结构展示与只读链路准备

### 4.2 Milestone 2：禁行区新增 / 修改 / 删除

结论：**已完成并进入当前一期范围**

当前页面已按约定接入 no-go 编辑链，编辑原则与 zone / virtual wall 保持一致：

- 前端只编辑 `display_region`
- 后端回包作为正式结果
- 成功后重新拉取列表

说明：

- 本期验收记录里，最完整的 live CRUD 证据来自 virtual wall 主链
- No-Go CRUD 已纳入当前页面能力与一期范围
- 如果后续需要更严格的独立归档，可额外补一轮 no-go CRUD 的单独 live 记录

### 4.3 Milestone 3 / 4：虚拟墙只读层与 CRUD

结论：**通过**

本轮已完成真实 live 验证，覆盖：

- `getAll`
- `add`
- `modify`
- `delete`

#### live 验证结论

1. 查询

- `getAll` 正常
- 当前 `map_name='yeyeyeye'` 可正常返回
- `map_name=''` 也可按当前地图推断
- `map_name='--'` 会返回 `map asset not found`

2. 新增

- 页面真实新增成功
- 数量变化：`0 -> 1`
- 新增对象可自动选中
- 详情面板与对象计数同步更新

3. 修改

- 同一 `wall_id` 修改成功
- 数量保持不变：`1 -> 1`
- `display_name` 与 `buffer_m` 更新成功
- 端点拖拽后的几何也真实变化

4. 删除

- 删除成功
- 对象从后端消失
- 对象列表、详情、画布、计数同步移除

5. `enabled=false`

- 修改为 `enabled=false` 后，后端已真实持久化
- 页面后续因为 `include_disabled=false`，会将该对象从当前列表和画布中隐藏
- 这是当前筛选行为，不是保存失败

---

## 5. 一期总体验收结论

结论：**约束编辑前端一期可视为验收通过。**

当前已经具备可交付的前端能力：

- 在地图工作台中展示约束对象
- 展示约束详情
- 对虚拟墙完成真实可用的新增 / 修改 / 删除
- 对禁行区完成一期范围内的前端接入与编辑链整合
- 与当前后端 `map/alignment/display geometry` 规则保持一致

一期结束后，可以进入下一阶段开发。

---

## 6. 当前已知限制

### 6.1 约束对象筛选

当前列表查询默认：

- `include_disabled=false`

因此：

- `enabled=false` 的 no-go / virtual wall 会从当前页面列表与画布中隐藏

### 6.2 坐标系不应写死

当前 live backend 里：

- `display_frame`
- `storage_frame`
- `aligned_frame`

并不保证一定是 `site_map`，当前真实环境可能直接是 `map`。

因此前端不能硬编码：

- `display_frame=site_map`

### 6.3 地图服务瞬时状态

本轮联调中出现过：

- 地图加载 banner 报错

但该问题不阻塞 virtual wall CRUD 主链。  
后续如再次出现，应单独排查 `map_server` 当前运行态，而不要误判成约束编辑链本身失败。

### 6.4 编辑器范围仍是一期范围

当前仍只支持：

- 禁行区矩形编辑 / 四角点编辑
- 虚拟墙基础折线编辑 / 端点拖拽

不支持：

- holes
- 复杂自由多边形
- 多段高级编辑器

---

## 7. 当前建议的对外交付口径

如果需要向项目内外汇报，建议使用这版口径：

- 地图编辑工作台只读链已通过
- Zone Editor 一期已通过
- Constraint Editor 一期已通过
- 当前已具备：
  - 地图底图显示
  - 区域展示与编辑
  - 禁行区展示与编辑
  - 虚拟墙展示与编辑
- 当前尚未进入：
  - 任务管理页
  - 调度管理页
  - 执行控制页

---

## 8. 下一步建议

建议按这个顺序推进：

1. 先做一点产品化收口
   - loading / empty state / error text 统一
   - 对象面板文案统一
   - debug 诊断区改成明确的开发开关

2. 再进入任务 / 调度 / 执行控制前端

3. 如果后续对地图编辑要求更高，再补：
   - no-go CRUD 的单独更完整 live 验证记录
   - holes / 自由多边形编辑
   - 更强的虚拟墙编辑器

---

## 9. 文档关联

本总结对应的开发/接口文档如下：

- [frontend_backend_interface_v1.md](/home/sunnybaer/Doraemon/docs/frontend_backend_interface_v1.md)
- [constraint_editor_frontend_flow_v1.md](/home/sunnybaer/Doraemon/docs/constraint_editor_frontend_flow_v1.md)
- [constraint_editor_frontend_tasks_v1.md](/home/sunnybaer/Doraemon/docs/constraint_editor_frontend_tasks_v1.md)
- [zone_editor_frontend_flow_v1.md](/home/sunnybaer/Doraemon/docs/zone_editor_frontend_flow_v1.md)
- [zone_editor_frontend_tasks_v2.md](/home/sunnybaer/Doraemon/docs/zone_editor_frontend_tasks_v2.md)

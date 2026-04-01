# 禁行区 / 虚拟墙编辑前端开发任务拆解清单 v1

这份文档是给 Windows 上 VSCode 里的 Codex 插件直接执行的。

当前只读地图工作台和 Zone Editor 一期主链已经验收通过，下一步继续做：

- 禁行区编辑
- 虚拟墙编辑

注意：

- 这一轮只做 `constraint editor`
- 不做任务 / 调度页面
- 不做禁行区 holes
- 不做复杂多段虚拟墙高级编辑器

## 1. 当前前提

当前前端已经完成并验收通过：

- 地图底图渲染
- zone 列表与详情展示
- alignment 查询与两点方向校正
- 矩形 zone 新建、预览、提交
- 已有 zone 更新
- zone 版本冲突处理

当前后端已经提供：

- `/database_server/no_go_area_service`
- `/database_server/virtual_wall_service`

## 2. 这轮总目标

完成一个可用的约束编辑闭环：

1. 查询全部禁行区
2. 新增禁行区
3. 修改禁行区
4. 删除禁行区
5. 查询全部虚拟墙
6. 新增虚拟墙
7. 修改虚拟墙
8. 删除虚拟墙

## 3. 必须遵守的规则

### 3.1 前端只编辑 display geometry

禁行区编辑态必须使用：

- `area.display_region`
- `area.display_frame`

虚拟墙编辑态必须使用：

- `wall.display_path`
- `wall.display_frame`

不要直接编辑：

- `map_region`
- `map_path`

### 3.2 后端是真值源

正式结果必须以后端返回为准：

- polygon / polyline 的正式存储坐标以后端回包为准
- `constraint_version` 以后端回包为准
- 新增/修改后的对象以后端回包为准

### 3.3 整对象回写

当前后端 v1 是整对象覆盖式更新。

这意味着前端在 `modify` 时必须把完整对象回传，不要只传局部字段。

## 4. 这一轮对接的后端接口

### 4.1 禁行区

- `/database_server/no_go_area_service`
- 类型：`my_msg_srv/OperateMapNoGoArea`

### 4.2 虚拟墙

- `/database_server/virtual_wall_service`
- 类型：`my_msg_srv/OperateMapVirtualWall`

## 5. 关键数据结构

### 5.1 MapNoGoArea

重点字段：

- `map_name`
- `area_id`
- `display_name`
- `enabled`
- `alignment_version`
- `display_frame`
- `storage_frame`
- `display_region`
- `map_region`
- `updated_ts`
- `warnings`

### 5.2 MapVirtualWall

重点字段：

- `map_name`
- `wall_id`
- `display_name`
- `enabled`
- `alignment_version`
- `display_frame`
- `storage_frame`
- `display_path`
- `map_path`
- `buffer_m`
- `updated_ts`
- `warnings`

## 6. 页面能力拆分

建议拆成 4 个 Milestone。

## 7. Milestone 1：禁行区只读 + 选中详情

### 7.1 目标

先把禁行区图层从“有机制但空实现”补成真正可用的只读层。

### 7.2 页面改动

在左栏对象列表里加一个分组：

- `No-Go Areas`

在画布里新增真正的禁行区图层组件：

- 根据 `areas[]` 渲染 polygon
- 支持选中高亮

右侧详情面板显示：

- `area_id`
- `display_name`
- `enabled`
- `display_frame`
- `updated_ts`
- `warnings`

### 7.3 验收标准

- 后端返回的禁行区能在地图上显示
- 能点选
- 右侧详情显示正确

## 8. Milestone 2：禁行区新增 / 修改 / 删除

### 8.1 目标

完成禁行区编辑主链。

### 8.2 编辑范围

这一轮先只支持：

- 矩形禁行区
- 四角点编辑

不要做：

- 自由多边形绘制
- holes 编辑

### 8.3 页面交互

新增按钮：

- `New No-Go Area`
- `Edit No-Go Area`
- `Delete No-Go Area`

### 8.4 新增链路

建议流程：

1. 用户进入 `creating-no-go` 模式
2. 在画布上点两个角点
3. 前端本地生成矩形 `display_region`
4. 调 `/database_server/no_go_area_service` 的 `add`
5. 成功后刷新禁行区列表

说明：

- 禁行区这轮没有独立的“预览服务”
- 所以前端可以在编辑态本地维护临时 polygon
- 正式结果以后端回包为准

### 8.5 修改链路

流程：

1. 选中已有禁行区
2. 点击 `Edit No-Go Area`
3. 进入 `editing-no-go`
4. 四角点编辑 `display_region`
5. 调 `modify`
6. 成功后刷新列表

### 8.6 删除链路

流程：

1. 选中已有禁行区
2. 点击 `Delete No-Go Area`
3. 确认后调 `Delete`
4. 成功后刷新列表

### 8.7 验收标准

- add 成功后图层与列表立即出现
- modify 成功后图形与详情同步更新
- delete 成功后对象消失
- `constraint_version` 更新后前端状态同步

## 9. Milestone 3：虚拟墙只读 + 新增 / 修改 / 删除

### 9.1 目标

完成虚拟墙编辑主链。

### 9.2 编辑范围

这一轮先只支持：

- 两点线段
- 端点拖动
- `buffer_m` 编辑

不要做：

- 多段折线高级编辑
- 吸附
- 拖整条线平移

### 9.3 页面交互

新增按钮：

- `New Virtual Wall`
- `Edit Virtual Wall`
- `Delete Virtual Wall`

### 9.4 新增链路

流程：

1. 用户进入 `creating-wall`
2. 在画布上点击两个点
3. 前端本地生成 `display_path`
4. 右侧填写：
   - `display_name`
   - `buffer_m`
5. 调 `/database_server/virtual_wall_service` 的 `add`
6. 成功后刷新列表

### 9.5 修改链路

流程：

1. 选中已有虚拟墙
2. 点击 `Edit Virtual Wall`
3. 拖动两个端点
4. 可修改 `display_name / enabled / buffer_m`
5. 调 `modify`
6. 成功后刷新列表

### 9.6 删除链路

流程：

1. 选中已有虚拟墙
2. 点击 `Delete Virtual Wall`
3. 调 `Delete`
4. 成功后刷新列表

### 9.7 验收标准

- 虚拟墙线段能正常渲染
- 端点编辑后提交成功
- `buffer_m` 修改能保存
- 删除后图层与列表消失

## 10. Milestone 4：页面收口与真实联调验证

### 10.1 目标

把禁行区 / 虚拟墙编辑从“实现完成”收口到“真实可验收”。

### 10.2 必做 live 验证

#### 禁行区

- getAll live 成功
- add live 成功
- modify live 成功
- delete live 成功

#### 虚拟墙

- getAll live 成功
- add live 成功
- modify live 成功
- delete live 成功

### 10.3 验证重点

- 新增后列表数量变化正确
- 修改后对象 id 不变
- 删除后数量减少
- 页面刷新链路正确
- 错误提示正常

## 11. 建议的文件拆分

请沿用现有工作台结构，新增这些文件或组件：

```text
src/
  components/
    no-go-editor/
      NoGoEditorToolbar.tsx
      NoGoRegionEditorLayer.tsx
      NoGoDetailsPanel.tsx
    wall-editor/
      VirtualWallEditorToolbar.tsx
      VirtualWallEditorLayer.tsx
      VirtualWallDetailsPanel.tsx
  hooks/
    useNoGoAreas.ts
    useVirtualWalls.ts
  stores/
    constraintEditorStore.ts
  utils/
    constraint-editor.ts
```

## 12. 页面状态建议

请在 store 里明确区分这些状态：

- `mode`
- `selectedNoGoAreaId`
- `selectedWallId`
- `draftNoGoRegion`
- `draftWallPath`
- `draftWallBufferM`
- `saveLoading`
- `deleteLoading`
- `lastError`

建议模式至少有：

- `idle`
- `creating-no-go`
- `editing-no-go`
- `creating-wall`
- `editing-wall`

## 13. 真实约束与注意事项

### 13.1 禁行区当前不支持 holes

如果后端返回：

- `no-go area does not support holes in v1`

前端应正常显示错误，不要继续提交。

### 13.2 当前可能没有 active alignment

如果当前地图没有 active alignment：

- 前端仍然可以继续工作
- 但 `display_frame` 可能退回 `map`

不要把 `site_map` 写死。

### 13.3 每次修改后建议重新拉列表

这一轮不要做复杂乐观更新。

更稳的做法是：

- add / modify / delete 成功后
- 重新调用一次 `getAll`

## 14. 开发要求

### 14.1 不要一次做太多

必须按 Milestone 顺序推进，不要把禁行区和虚拟墙全部混在一个大提交里。

### 14.2 每个阶段都要可运行

每完成一个 Milestone，都要保证：

- 页面能运行
- `npm run build` 通过
- `npm run lint` 通过

### 14.3 保留诊断区

当前诊断区继续保留为：

- 折叠
- debug-only

不要提前删掉。

## 15. 完成后如何汇报

每完成一个 Milestone，请汇报：

1. 已完成内容
2. 新增/修改文件
3. 当前页面怎么操作
4. 当前还缺什么
5. build/lint 是否通过

## 16. 你现在应该立刻开始做的内容

现在请直接开始：

1. 完成 Milestone 1
2. 先把禁行区只读层和详情卡做好
3. 确认图层选中与右侧详情正常

完成后再继续做禁行区新增 / 修改 / 删除。

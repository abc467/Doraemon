# Zone Editor 前端二期开发任务拆解清单 v1

这份文档是给 Windows 上 VSCode 里的 Codex 插件直接执行的。

当前只读版地图工作台已经验收通过，现阶段要继续往下推进：

- 地图业务方向校正
- zone 新建
- zone 规划预览
- zone 正式提交
- 已有 zone 更新

注意：

- 这一轮先做 `zone editor`
- 禁行区 / 虚拟墙编辑先不做
- 任务 / 调度页面先不做

## 1. 当前前提

当前前端已经完成并验收通过：

- 能连接 `rosbridge`
- 能获取当前地图详情
- 能用 `OccupancyGrid` 渲染地图底图
- 能叠加显示 zone
- 能叠加显示禁行区 / 虚拟墙图层机制
- 右侧详情面板可显示选中对象

当前后端状态请以前端实时查询结果为准，不要把运行态写死在代码里。

特别注意：

- live 后端可能已经存在 active alignment
- active alignment 的 `aligned_frame` 也可能是 `map`，不一定是 `site_map`
- 两点确认主方向后，后端也可能更新已有 `alignment_version`，而不是总是创建新版本

## 2. 这轮总目标

完成一个可用的 `zone editor` 闭环：

1. 查询当前地图 active alignment
2. 通过两点确认业务主方向
3. 新建矩形 zone
4. 做覆盖规划预览
5. 正式提交 zone
6. 编辑已有 zone
7. 处理版本冲突

## 3. 必须遵守的规则

### 3.1 前端只编辑 display geometry

前端编辑态必须使用：

- `display_region`
- `display_frame`

不要直接编辑：

- `map_region`

### 3.2 后端是真值源

正式结果必须以后端返回为准：

- 矩形由后端生成
- 规划预览以后端为准
- 正式提交后的 `zone_version / plan_id / map_region` 以后端回包为准

### 3.3 更新必须带版本

更新已有 zone 时，必须带：

- `zone_id`
- `base_zone_version`

如果后端返回 `ZONE_VERSION_CONFLICT`，前端不能强行覆盖，必须提示用户刷新。

## 4. 这一轮对接的后端接口

### 4.1 alignment 查询

- `/database_server/map_alignment_service`

### 4.2 两点确认主方向

- `/database_server/map_alignment_by_points_service`

### 4.3 两点矩形选区

- `/database_server/rect_zone_preview_service`

### 4.4 规划预览

- `/database_server/coverage_preview_service`

### 4.5 zone 正式提交

- `/database_server/coverage_commit_service`

### 4.6 zone 查询

- `/database_server/coverage_zone_service`

## 5. 页面能力拆分

建议把这一轮拆成 4 个 Milestone。

## 6. Milestone 1：Alignment 状态与方向校正

### 6.1 目标

让页面知道“当前有没有 active alignment”，并支持在需要时人工两点校正。

### 6.2 页面改动

在工作台左栏顶部新增一个 `AlignmentCard`：

- 当前状态：`configured / not configured`
- 如果 `not configured`，页面应理解为“当前使用原始地图坐标”，不是阻断错误
- 当前 `aligned_frame`
- 当前 `alignment_version`
- 当前 `yaw_offset_deg`
- 操作按钮：
  - `Start Alignment`
  - `Cancel`

### 6.3 交互模式

新增一个页面模式：

- `idle`
- `aligning`
- `creating-zone`
- `editing-zone`
- `previewing`

进入 `aligning` 后：

- 用户在画布上点击 2 个点
- 前端把两点发给：
  - `/database_server/map_alignment_by_points_service`

### 6.4 成功后行为

调用成功后：

1. 重新拉取 active alignment
2. 重新拉取 zone 列表
3. 刷新地图图层
4. 退出 `aligning` 模式

### 6.5 验收标准

- 能正确显示当前 active alignment 状态
- 用户点击两点后能成功创建 alignment
- 成功后页面状态更新

## 7. Milestone 2：新建矩形 zone

### 7.1 目标

完成“新建 zone 的几何草稿链”。

### 7.2 页面交互

新增按钮：

- `New Zone`

点击后进入 `creating-zone` 模式：

- 用户在地图上点击两个角点
- 前端不要自己算矩形
- 调 `/database_server/rect_zone_preview_service`

### 7.3 前端需要显示

矩形预览成功后，在画布临时图层显示：

- `display_region`

右侧预览面板显示：

- `width_m`
- `height_m`
- `area_m2`
- `warnings`

### 7.4 失败处理

如果后端返回失败：

- 保持当前页面
- 弹错误提示
- 不进入下一步 preview

### 7.5 验收标准

- 两点后能画出后端返回的标准矩形
- 右侧能看到尺寸、面积和 warning

## 8. Milestone 3：规划预览与正式提交

### 8.1 目标

完成从矩形选区到 preview 再到 commit 的完整新建闭环。

### 8.2 页面交互

在矩形预览成功后，右侧显示：

- `Preview Plan`
- `Commit Zone`
- `Cancel`

### 8.3 Preview

点击 `Preview Plan`：

- 调 `/database_server/coverage_preview_service`

请求内容：

- `map_name`
- `alignment_version`
- `region = display_region`
- `profile_name`

### 8.4 Preview 成功后显示

在中间画布临时图层显示：

- `display_preview_path`
- `display_entry_pose`

右侧显示：

- `estimated_length_m`
- `estimated_duration_s`
- `warnings`
- `valid`

如果 `valid=false`：

- 禁止提交

### 8.5 Commit

点击 `Commit Zone`：

- 调 `/database_server/coverage_commit_service`

新建 zone 时传：

- `zone_id = ''`
- `base_zone_version = 0`
- `display_name`
- `region = display_region`
- `profile_name`
- `set_active_plan = true`

### 8.6 Commit 成功后行为

1. 刷新 zone 列表
2. 自动选中新建 zone
3. 清理临时图层
4. 退出 `creating-zone`

### 8.7 验收标准

- Preview 成功时路径和入口点能显示
- Commit 成功后 zone 列表立即刷新
- 新 zone 能在画布中显示

## 9. Milestone 4：已有 zone 更新

### 9.1 目标

完成已有 zone 的编辑更新闭环。

### 9.2 页面交互

在 zone 详情面板里增加：

- `Edit Zone`

点击后进入 `editing-zone` 模式：

- 读取当前 zone 的：
  - `zone_id`
  - `zone_version`
  - `display_region`
  - `display_name`
  - `plan_profile_name`

### 9.3 编辑方式

这一轮先不要做自由多边形编辑器。

只做：

- 矩形 zone 的拖拽式编辑
- 或四角点编辑

前端改的是：

- `display_region`

### 9.4 更新链

更新流程和新建类似：

1. 用户修改矩形
2. 点 `Preview Plan`
3. 调 `/database_server/coverage_preview_service`
4. 点 `Save Changes`
5. 调 `/database_server/coverage_commit_service`

这次必须带：

- `zone_id = 当前 zone_id`
- `base_zone_version = 当前 zone_version`

### 9.5 版本冲突

如果返回：

- `ZONE_VERSION_CONFLICT`

前端必须：

- 弹提示：“该区域已被其他修改，请刷新后重试”
- 保留当前草稿，但不自动覆盖

### 9.6 成功后行为

1. 刷新该 zone 详情
2. 刷新 zone 列表
3. 清理临时 preview
4. 退出编辑态

## 10. 当前不做的内容

这一轮不要提前展开：

- 多边形自由绘制
- holes 编辑
- draft 列表管理
- 多 profile 同屏对比
- alignment 自动建议
- 禁行区/虚拟墙编辑增强

## 11. 建议的文件拆分

请沿用现有只读工作台工程结构，新增这些文件或组件：

```text
src/
  components/
    alignment/
      AlignmentCard.tsx
      AlignmentPointPicker.tsx
    zone-editor/
      ZoneEditorToolbar.tsx
      ZonePreviewPanel.tsx
      ZoneCommitDialog.tsx
      ZoneRectEditorLayer.tsx
  hooks/
    useAlignment.ts
    useZonePreview.ts
    useZoneCommit.ts
  stores/
    zoneEditorStore.ts
  utils/
    zone-editor.ts
```

## 12. 页面状态建议

请在 store 里明确区分这些状态：

- `mode`
- `selectedZoneId`
- `activeAlignment`
- `draftRectPoints`
- `draftDisplayRegion`
- `draftPreview`
- `previewLoading`
- `commitLoading`
- `lastError`

## 13. 开发要求

### 13.1 不要一次做太多

必须按 Milestone 顺序推进，不要把 alignment、preview、commit、edit 一口气混在一个大提交里。

### 13.2 每个阶段都要可运行

每完成一个 Milestone，都要保证：

- 页面能运行
- `npm run build` 通过
- `npm run lint` 通过

### 13.3 先功能正确，再谈交互润色

优先保证：

- 服务调用顺序正确
- 状态流正确
- 后端回包正确显示

## 14. 完成后如何汇报

每完成一个 Milestone，请汇报：

1. 已完成内容
2. 新增/修改文件
3. 当前页面怎么操作
4. 当前还缺什么
5. build/lint 是否通过

## 15. 你现在应该立刻开始做的内容

现在请直接开始：

1. 实现 `AlignmentCard`
2. 实现 alignment 查询
3. 实现两点方向校正模式
4. 跑通 Milestone 1

完成后再继续做矩形 zone 新建。

## 16. 后续增强：区域点选高亮与路径叠加

### 16.1 目标

让操作员在区域列表里点哪个，地图上就高亮哪个；并能按需显示该区域当前 active plan 的覆盖路径。

### 16.2 页面能力

- 左侧区域列表点选
- 地图高亮当前选中 `display_region`
- 图层开关：
  - `显示全部区域`
  - `只显示当前区域`
  - `显示区域路径`
- 当前选中 zone 时，懒加载其 active plan 路径并叠加显示

### 16.3 后端接口

- `/database_server/coverage_zone_service`
  - `getAll`
  - 提供 `display_region / display_name / zone_id`
- `/database_server/zone_plan_path_service`
  - `GetZonePlanPath`
  - 提供 `display_path / display_entry_pose / plan_profile_name / estimated_length_m / estimated_duration_s`

### 16.4 验收标准

- 点击左侧某个 zone，地图高亮对应区域
- `只显示当前区域` 打开后，非选中区域隐藏
- `显示区域路径` 打开后，只为当前选中 zone 显示覆盖路径
- 切换 zone 时，旧路径清掉，新路径正确加载
- 当前 zone 无 active plan 时，页面能显示明确错误，不崩溃

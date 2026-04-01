# Zone 编辑页面调用顺序说明 v1

这份文档面向前端页面开发，目标是把“地图业务方向校正 + 区域编辑 + 规划预览 + 正式提交”的调用顺序说清楚。

本文只覆盖当前已经实现完成、适合前端直接对接的后端能力，不讨论 RViz 调试链。

## 1. 适用范围

当前后端已经支持以下闭环：

- 地图业务方向校正配置查询/保存
- 两点确定主方向
- 两点矩形选区
- 规划预览
- 区域正式提交
- 既有 zone 查询
- 已有 zone 更新

当前推荐前端把区域编辑页面拆成这几类场景：

- 页面初始化
- 新建区域
- 编辑已有区域
- 只保存草稿
- 正式发布

## 2. 核心约定

### 2.1 坐标系约定

- `map`
  - 原始执行坐标系
  - 用于正式存储、导航、覆盖规划执行
- `site_map`
  - 业务编辑坐标系
  - 用于前端区域编辑、矩形框选、规划确认展示

注意：

- 当前后端默认推荐业务编辑坐标系是 `site_map`
- 但 live 后端返回的 active alignment 里，`aligned_frame` 也可能就是 `map`
- 前端不要写死 `site_map`，必须以后端实际返回的 `aligned_frame / display_frame / storage_frame` 为准

前端编辑时，默认应使用：

- `display_region`
- `display_frame`

前端不要直接编辑：

- `map_region`

因为 `map_region` 是后端正式存储/执行的坐标表达。

### 2.2 后端真值源

正式几何结果以后端返回为准：

- 两点矩形不是前端自己算
- 规划预览不是前端自己估
- 正式提交后的 `map_region / zone_version / active_plan_id` 以后端回包为准

### 2.3 版本保护

更新已有区域时，前端必须带上：

- `zone_id`
- `base_zone_version`

这样后端才能做乐观锁保护，避免两个人同时改同一个区域时互相覆盖。

## 3. 后端接口清单

### 3.1 对齐配置

- `/database_server/map_alignment_service`
  - `OperateMapAlignment`
- `/database_server/map_alignment_by_points_service`
  - `ConfirmMapAlignmentByPoints`

### 3.2 矩形选区

- `/database_server/rect_zone_preview_service`
  - `PreviewAlignedRectSelection`

### 3.3 规划预览

- `/database_server/coverage_preview_service`
  - `PreviewCoverageRegion`

### 3.4 正式提交

- `/database_server/coverage_commit_service`
  - `CommitCoverageRegion`

### 3.5 已有区域查询

- `/database_server/coverage_zone_service`
  - `OperateCoverageZone`

### 3.6 已有区域删除

- `/database_server/coverage_zone_service`
  - `OperateCoverageZone`

说明：

- 当前 `Delete` 语义是**软删除 / 禁用 zone**
- 删除后：
  - 默认 `include_disabled=false` 的列表里不会再显示
  - `include_disabled=true` 仍可查到历史记录
- 当前不会物理删除 `zone_versions / plans / zone_editor_metadata`

## 4. 页面初始化顺序

推荐页面初始化时按下面顺序做：

1. 获取当前地图
2. 获取当前地图的 active alignment
3. 获取当前地图下的 zone 列表
4. 用 `display_region` 在前端地图上渲染已有区域

### 4.1 获取 active alignment

前端可以调用：

- `/database_server/map_alignment_service`

建议请求：

- `operation = get`
- `map_name = 当前地图名`
- `alignment_version = ''`

说明：

- `alignment_version` 为空时，后端会返回当前地图的 active alignment
- 如果当前地图还没有 active alignment，这在新地图场景下是正常情况；页面应提示“当前使用原始地图坐标”
- live 后端在“重新确认两点方向”时，可能更新已有 alignment，而不是总是生成新的 `alignment_version`

### 4.2 获取 zone 列表

前端调用：

- `/database_server/coverage_zone_service`

建议请求：

- `operation = getAll`
- `map_name = 当前地图名`
- `alignment_version = ''`
- `plan_profile_name = ''`
- `include_disabled = false`

返回结果里前端重点使用：

- `zone_id`
- `display_name`
- `zone_version`
- `display_region`
- `display_frame`
- `active_plan_id`
- `plan_profile_name`
- `estimated_length_m`
- `estimated_duration_s`
- `warnings`

如果当前 zone 有保存过编辑元数据，后端会优先返回当时的 `display_region(site_map)`。

### 4.2.1 列表点选与区域高亮

建议前端行为：

- 左侧区域列表点击哪个，地图上就高亮哪个 `display_region`
- 再加两个图层开关：
  - `显示全部区域`
  - `只显示当前区域`
- 默认仍显示全部区域，但当前选中区域描边更明显、填充更亮
- 这一步前端不需要新增后端接口，直接复用 `coverage_zone_service getAll` 的 `display_region`

### 4.2.2 查询当前区域对应路径

前端按需调用：

- `/database_server/zone_plan_path_service`

建议请求：

- `map_name = 当前地图名`
- `zone_id = 当前选中的 zone_id`
- `alignment_version = ''`
- `plan_profile_name = ''`

建议前端行为：

- 不要在列表初次加载时一次性拉全部路径
- 只在“选中某个 zone 且用户打开路径开关”时懒加载
- 地图上用单独图层显示 `display_path`
- 再加一个开关：
  - `显示区域路径`

返回结果里前端重点使用：

- `active_plan_id`
- `plan_profile_name`
- `display_path`
- `display_entry_pose`
- `estimated_length_m`
- `estimated_duration_s`
- `warnings`

### 4.3 删除 zone

前端调用：

- `/database_server/coverage_zone_service`

建议请求：

- `operation = Delete`
- `map_name = 当前地图名`
- `zone_id = 目标 zone_id`
- `alignment_version = ''`
- `plan_profile_name = ''`
- `include_disabled = false`

建议前端行为：

- 删除前弹确认框
- 删除成功后重新 `getAll(include_disabled=false)` 刷新列表和画布
- 如需要查看历史，可单独用 `include_disabled=true` 查询

## 5. 新建区域页面调用顺序

### 5.1 两点矩形框选

用户在前端地图上点两个点后，不要自己算矩形，直接调用：

- `/database_server/rect_zone_preview_service`

请求关键字段：

- `map_name`
- `alignment_version`
- `p1`
- `p2`
- `min_side_m`

注意：

- `p1 / p2` 默认属于当前 active alignment 对应的业务编辑坐标系
- 不要在前端写死它一定是 `site_map`
- `alignment_version` 建议传当前页面加载到的 active alignment 版本

前端可直接使用返回：

- `valid`
- `display_frame`
- `storage_frame`
- `display_region`
- `width_m`
- `height_m`
- `area_m2`
- `warnings`

同时保留：

- `map_region`

但前端不要直接拿它画编辑态。

### 5.2 规划预览

前端在用户确认矩形后，调用：

- `/database_server/coverage_preview_service`

请求建议：

- `map_name`
- `alignment_version`
- `region = 上一步返回的 display_region`
- `profile_name`

注意：

- `region` 的真实入参类型是结构化 `PolygonRegion`
- 也就是：
  - `frame_id`
  - `outer.points[]`
  - `holes[]`
- 前端不要把它当作“简单点集数组”处理

前端重点展示：

- `display_preview_path`
- `display_entry_pose`
- `estimated_length_m`
- `estimated_duration_s`
- `warnings`
- `valid`

如果 `valid=false`，前端不应允许用户继续提交。

### 5.3 正式提交

用户点“保存并发布”后，调用：

- `/database_server/coverage_commit_service`

请求建议：

- `map_name`
- `alignment_version`
- `zone_id = ''`
- `base_zone_version = 0`
- `display_name`
- `region = 当前编辑区 display_region`
- `profile_name`
- `set_active_plan = true`

注意：

- `region` 的真实入参也是结构化 `PolygonRegion`
- 提交时必须带完整的 `frame_id / outer / holes`

返回结果中前端重点记录：

- `zone_id`
- `zone_version`
- `plan_id`
- `display_region`
- `map_region`
- `estimated_length_m`
- `estimated_duration_s`
- `warnings`

提交成功后，前端应刷新 zone 列表或直接把返回结果合并进本地状态。

## 6. 编辑已有区域页面调用顺序

这是前端最需要严格遵守的一条链。

### 6.1 打开编辑页

先查询当前 zone：

- `/database_server/coverage_zone_service`

请求建议：

- `operation = get`
- `map_name`
- `zone_id`
- `alignment_version = ''`
- `plan_profile_name = 当前页面选择的 profile 或空`
- `include_disabled = true`

前端进入编辑态时应保存：

- `zone_id`
- `zone_version`
- `display_name`
- `display_region`
- `plan_profile_name`

其中：

- `zone_version` 就是之后更新时要传的 `base_zone_version`

### 6.2 用户调整形状

用户在前端修改区域后，前端得到新的 `display_region`。

此时建议重新调用规划预览：

- `/database_server/coverage_preview_service`

请求：

- `map_name`
- `alignment_version`
- `region = 用户修改后的 display_region`
- `profile_name`

### 6.3 提交更新

用户点“更新并发布”时，调用：

- `/database_server/coverage_commit_service`

请求建议：

- `map_name`
- `alignment_version`
- `zone_id = 既有 zone_id`
- `base_zone_version = 打开编辑页时拿到的 zone_version`
- `display_name`
- `region = 当前编辑后的 display_region`
- `profile_name`
- `set_active_plan = true`

成功后：

- 后端会创建新的 `zone_version`
- 后端会生成新的 `plan_id`
- 后端会把该 profile 的 active plan 切到新版本

### 6.4 版本冲突处理

如果用户编辑期间，别人已经发布了新版本，后端会返回：

- `success = false`
- `error_code = ZONE_VERSION_CONFLICT`

前端建议处理：

1. 提示“区域已被其他人更新”
2. 重新调用 zone 查询接口
3. 让用户决定是否重新编辑

前端不要在本地强行覆盖。

## 7. 草稿保存与正式发布

当前接口已经支持：

- `set_active_plan = false`

但要注意现在的实际语义：

- 新建 zone 且 `set_active_plan=false`
  - zone/version/plan 会落库
  - 但不会切 active plan
- 更新已有 zone 且 `set_active_plan=false`
  - 会保存新的 draft zone version 和 plan
  - 当前已发布 zone 仍保持旧版本

这意味着：

- 前端如果支持草稿，需要自己明确区分“草稿版本”和“当前发布版本”
- 当前 `coverage_zone_service` 查询到的仍是已发布版本，不会自动把未发布 draft 替换成当前展示内容

如果前端 v1 不急着做草稿能力，建议：

- 页面只开放 `set_active_plan=true`

这样契约最简单。

## 8. 错误码与前端处理建议

### 8.1 `ZONE_VERSION_CONFLICT`

含义：

- 当前区域已经被别人更新，你拿着旧版本提交了修改

前端动作：

- 刷新 zone
- 提示用户重开编辑

### 8.2 `ZONE_MAP_VERSION_MISMATCH`

含义：

- 当前 `zone_id` 属于同一地图名下的其他地图版本

前端动作：

- 提示“当前地图版本与区域版本不一致”
- 禁止继续编辑

### 8.3 `PREVIEW_FAILED / invalid=false`

含义：

- 当前区域几何或规划 profile 无法生成可用覆盖路径

前端动作：

- 禁止提交
- 展示 `warnings / error_message`

### 8.4 alignment 相关错误

常见含义：

- 当前地图没有 active alignment，但如果页面当前直接使用 `map` 坐标，这通常不是阻断错误
- 请求的 alignment_version 不存在
- alignment 与当前地图版本不一致

前端动作：

- 如果用户当前在 `map` 坐标下工作，只做提示，不要阻断
- 只有当用户明确在用 `site_map` / 对齐坐标编辑时，才提示先完成地图业务方向校正

## 9. 前端状态管理建议

前端编辑页建议至少维护这几个状态：

- `mapName`
- `alignmentVersion`
- `zoneId`
- `baseZoneVersion`
- `displayRegion`
- `profileName`
- `previewPath`
- `entryPose`
- `estimatedLengthM`
- `estimatedDurationS`
- `warnings`
- `dirty`

推荐规则：

- 页面初次打开时，把 `zone_version` 保存到 `baseZoneVersion`
- 只要用户改了区域或 profile，就把 `dirty=true`
- 提交成功后，用回包里的 `zone_version` 覆盖本地 `baseZoneVersion`

## 10. 推荐前端页面流程

### 10.1 区域列表页

- 加载 zone 列表
- 展示 `display_region`
- 展示长度/时长摘要
- 展示 warning 标识

### 10.2 新建区域页

- 两点框选
- 规划预览
- 保存并发布

### 10.3 编辑区域页

- 查询单个 zone
- 加载 `display_region`
- 用户修改
- 规划预览
- 更新并发布

### 10.4 地图方向校正规则页

- 查询当前 active alignment
- 通过两点确认主方向
- 激活 alignment

## 11. 推荐联调顺序

前端联调建议按这个顺序进行：

1. 先打通 active alignment 查询
2. 再打通 zone 列表查询
3. 再打通两点矩形选区
4. 再打通规划预览
5. 再打通新建区域提交
6. 最后打通已有区域更新和版本冲突处理

这样最稳。

## 12. 当前不建议前端直接做的事

- 不要自己在前端算正式 `map_polygon`
- 不要自己在前端估算覆盖长度和时长
- 不要直接依赖 RViz 调试接口
- 不要跳过 `base_zone_version`
- 不要直接修改 `map_region`

## 13. 一句话总结

前端真正应该遵循的核心顺序是：

- 先查 alignment
- 再查已有 zone
- 编辑时始终使用 `display_region`
- 提交前必须先 preview
- 更新时必须带 `base_zone_version`
- 正式结果以后端回包为准

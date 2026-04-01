# 禁行区与虚拟墙页面调用顺序说明 v1

这份文档面向前端页面开发，说明如何基于当前后端接口完成：

- 禁行区查询 / 新增 / 修改 / 删除
- 虚拟墙查询 / 新增 / 修改 / 删除

当前这两类能力已经接入：

- `site_map / map` 双坐标系转换
- 当前地图 active alignment 复用
- 正式存储统一回 `map`
- 导航约束发布链复用现有 `planning.db -> map_constraints_node -> costmap keepout layer`

## 1. 当前可直接对接的服务

### 1.1 禁行区

- 服务名：`/database_server/no_go_area_service`
- 类型：`my_msg_srv/OperateMapNoGoArea`

### 1.2 虚拟墙

- 服务名：`/database_server/virtual_wall_service`
- 类型：`my_msg_srv/OperateMapVirtualWall`

## 2. 数据结构约定

### 2.1 禁行区：`my_msg_srv/MapNoGoArea`

前端重点字段：

- `area_id`
- `display_name`
- `enabled`
- `alignment_version`
- `display_frame`
- `storage_frame`
- `display_region`
- `map_region`

说明：

- 前端编辑态应只使用 `display_region`
- 正式 `map_region` 由后端统一换算并返回
- 当前 v1 只支持单外环 polygon，不支持 holes

### 2.2 虚拟墙：`my_msg_srv/MapVirtualWall`

前端重点字段：

- `wall_id`
- `display_name`
- `enabled`
- `alignment_version`
- `display_frame`
- `storage_frame`
- `display_path`
- `map_path`
- `buffer_m`

说明：

- 前端编辑态应只使用 `display_path`
- 正式 `map_path` 由后端统一换算并返回
- `buffer_m` 是虚拟墙在导航侧生成 keepout 带的缓冲宽度

## 3. 页面初始化顺序

建议前端按这个顺序初始化：

1. 获取当前地图
2. 获取当前地图 active alignment
3. 查询禁行区列表
4. 查询虚拟墙列表
5. 用 `display_region / display_path` 在前端地图上渲染

## 4. 禁行区页面调用顺序

### 4.1 查询全部禁行区

调用：

- `/database_server/no_go_area_service`

请求建议：

- `operation = getAll`
- `map_name = 当前地图名`
- `alignment_version = ''`
- `include_disabled = false`

返回结果里前端重点用：

- `areas[]`
- `constraint_version`

### 4.2 查询单个禁行区

调用：

- `/database_server/no_go_area_service`

请求建议：

- `operation = get`
- `map_name`
- `area_id`
- `alignment_version = ''`

### 4.3 新增禁行区

前端提交时建议使用：

- `area.display_region`
- `area.display_region.frame_id = site_map`

调用：

- `/database_server/no_go_area_service`

请求建议：

- `operation = add`
- `map_name`
- `area_id = ''` 或前端自定义 ID
- `alignment_version = 当前 active alignment`
- `area.display_name`
- `area.enabled = true`
- `area.display_region = 前端编辑后的 polygon`

后端会：

- 把 `display_region(site_map)` 转成 `map_region`
- 更新约束快照
- 返回新的 `constraint_version`
- 返回正式落库后的区域对象

### 4.4 修改禁行区

调用：

- `/database_server/no_go_area_service`

请求建议：

- `operation = modify`
- `map_name`
- `area_id = 已有 area_id`
- `alignment_version = 当前 active alignment`
- `area.display_name`
- `area.enabled`
- `area.display_region`

说明：

- 当前 v1 是整对象覆盖式更新
- 前端修改时应把完整对象回传，不要只传局部字段

### 4.5 删除禁行区

调用：

- `/database_server/no_go_area_service`

请求建议：

- `operation = Delete`
- `map_name`
- `area_id`

说明：

- 删除后后端会生成新的 `constraint_version`

## 5. 虚拟墙页面调用顺序

### 5.1 查询全部虚拟墙

调用：

- `/database_server/virtual_wall_service`

请求建议：

- `operation = getAll`
- `map_name = 当前地图名`
- `alignment_version = ''`
- `include_disabled = false`

### 5.2 查询单个虚拟墙

调用：

- `/database_server/virtual_wall_service`

请求建议：

- `operation = get`
- `map_name`
- `wall_id`
- `alignment_version = ''`

### 5.3 新增虚拟墙

调用：

- `/database_server/virtual_wall_service`

请求建议：

- `operation = add`
- `map_name`
- `wall_id = ''` 或前端自定义 ID
- `alignment_version = 当前 active alignment`
- `wall.display_name`
- `wall.enabled = true`
- `wall.display_frame = site_map`
- `wall.display_path = 前端编辑后的折线`
- `wall.buffer_m = 0.575` 或前端指定值

说明：

- `display_path` 至少 2 个点
- 后端会自动转成 `map_path`

### 5.4 修改虚拟墙

调用：

- `/database_server/virtual_wall_service`

请求建议：

- `operation = modify`
- `map_name`
- `wall_id`
- `alignment_version = 当前 active alignment`
- `wall.display_name`
- `wall.enabled`
- `wall.display_frame`
- `wall.display_path`
- `wall.buffer_m`

### 5.5 删除虚拟墙

调用：

- `/database_server/virtual_wall_service`

请求建议：

- `operation = Delete`
- `map_name`
- `wall_id`

## 6. 前端需要特别注意的点

### 6.1 不要直接编辑 `map_region / map_path`

前端编辑态只使用：

- 禁行区：`display_region`
- 虚拟墙：`display_path`

### 6.2 当前禁行区不支持 holes

如果前端传带洞 polygon，后端会直接拒绝。

### 6.3 修改是“整对象回写”

当前 v1 不是 patch 语义，所以前端修改时应把：

- `display_name`
- `enabled`
- 几何
- `buffer_m`（虚拟墙）

一起回传。

### 6.4 没有 active alignment 时的兜底

如果当前地图没有 active alignment：

- 查询接口会退回 `display_frame = map`
- 新增/修改时如果你传的是 `map` 几何，后端仍能接受
- 如果你传的是 `site_map` 几何但没有 alignment，后端会拒绝

## 7. 推荐前端页面结构

建议拆成：

- 约束列表页
  - 标签页：禁行区 / 虚拟墙
- 禁行区编辑弹窗
- 虚拟墙编辑弹窗

列表页展示建议：

- 名称
- 启用状态
- 几何预览
- `constraint_version`

## 8. 一句话总结

前端真正应该遵循的核心顺序是：

- 先查当前地图和 active alignment
- 查询现有禁行区/虚拟墙
- 编辑时始终只用 `display_region / display_path`
- 提交后以后端回包为准刷新本地状态

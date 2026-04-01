# Windows 前端工程开工文档（发给 VSCode Codex 插件）

这份文档的目标不是给人看思路，而是给 Windows 上的 Codex 插件直接执行。

你需要把这份文档完整作为任务上下文发给 Codex，让它在 Windows 本机上：

1. 安装前端开发环境
2. 新建前端工程
3. 连上 Ubuntu 机器人侧的 `rosbridge`
4. 先做“地图编辑工作台”只读版
5. 把 `地图 + zone + 禁行区 + 虚拟墙` 四层正确显示出来

## 1. 你的角色

你现在要作为本项目的资深前端工程师，负责在 Windows 开发机上从零搭建前端工程，并和当前 ROS1 后端联调。

你不是只生成代码片段，你要：

- 自己检查 Windows 环境是否满足
- 缺什么就安装什么
- 新建工程
- 跑起来开发服务器
- 接入 rosbridge
- 逐步完成页面

除非遇到真正的系统级阻塞，否则不要停留在分析阶段。

## 2. 项目背景

这是一个商用清洁机器人项目，当前后端已经完成了以下前端可直接对接的能力：

- 地图管理
- 地图业务方向校正（map alignment）
- zone 查询 / 创建 / 规划预览 / 提交 / 更新
- 禁行区查询 / 新增 / 修改 / 删除
- 虚拟墙查询 / 新增 / 修改 / 删除

当前前端第一阶段目标不是做全量业务，而是先做一个“地图编辑工作台”页面，验证前后端联调主链。

## 3. 本次前端一期目标

本次先只做“只读 + 基础展示”版本，必须先跑通这 4 层：

- 当前地图
- zone
- 禁行区
- 虚拟墙

先不要做：

- zone 编辑
- zone 规划预览
- zone 提交
- 禁行区编辑
- 虚拟墙编辑
- 任务/调度页面
- 登录权限

第一阶段先把图层正确显示出来。

## 4. 技术栈要求

请按下面固定方案执行，不要自作主张更换：

- 前端框架：`React`
- 语言：`TypeScript`
- 工具链：`Vite`
- UI：`Ant Design`
- ROS 接入：`roslibjs`
- 服务/请求管理：`@tanstack/react-query`
- 前端本地状态：`zustand`
- 地图绘制：`react-konva + konva`

## 5. Windows 本机环境要求

你要先自行检查并安装这些：

- `Git`
- `Node.js LTS`
- `npm`
- `VSCode` 已安装即可

优先要求：

- Node 版本 `>= 20`
- npm 版本与当前 Node 匹配

### 5.1 建议安装方式

如果机器没有 Node.js：

- 优先安装 `Node.js LTS`
- 如果系统里已有旧版 Node，先确认是否可直接用，不行再升级

如果机器没有 Git：

- 安装 Git for Windows

### 5.2 你必须做的检查

安装后请在终端执行并确认：

```powershell
node -v
npm -v
git --version
```

如果这些命令不能正常执行，先修复环境，再开始建工程。

## 6. 工程目录要求

请在 Windows 本机新建一个独立前端工程，不要改 Ubuntu 上的 ROS catkin 工程。

建议目录：  

```text
D:\work\clean-robot-frontend
```

如果 `D:` 盘不存在，可以用：

```text
C:\work\clean-robot-frontend
```

## 7. 前端工程初始化命令

请在 Windows PowerShell 中执行：

```powershell
cd D:\work
npm create vite@latest clean-robot-frontend -- --template react-ts
cd .\clean-robot-frontend
npm install
npm install antd zustand @tanstack/react-query roslib react-konva konva
```

如果目录用的是 `C:\work`，自行改路径。

## 8. 开发服务器启动要求

请配置并启动 Vite 开发服务器。

正常启动命令：

```powershell
npm run dev
```

如果需要局域网访问：

```powershell
npm run dev -- --host
```

## 9. ROS 后端联调前提

Ubuntu 机器人/ROS 侧会由我们来启动，前端只要连 `rosbridge`。

默认 rosbridge 地址：

```text
ws://<Ubuntu机器IP>:9090
```

请把这个地址做成前端可配置项，不要写死到业务组件里。

建议：

- `.env.development`
- `VITE_ROSBRIDGE_URL`

例如：

```env
VITE_ROSBRIDGE_URL=ws://192.168.1.100:9090
```

## 10. 你要对接的后端接口

前端一期只需要先接这几类：

### 10.1 地图管理

- `/clean_robot_server/map_server`

### 10.2 地图方向校正

- `/database_server/map_alignment_service`

### 10.3 zone 查询

- `/database_server/coverage_zone_service`

### 10.4 禁行区查询

- `/database_server/no_go_area_service`

### 10.5 虚拟墙查询

- `/database_server/virtual_wall_service`

## 11. 你必须先阅读的后端文档

在开始写代码前，请先阅读这些文档：

- `/home/sunnybaer/Doraemon/docs/frontend_backend_interface_v1.md`
- `/home/sunnybaer/Doraemon/docs/zone_editor_frontend_flow_v1.md`
- `/home/sunnybaer/Doraemon/docs/constraint_editor_frontend_flow_v1.md`

重点先读：

1. 稳定接口清单
2. zone 查询返回字段
3. 禁行区/虚拟墙查询返回字段
4. `display_frame / storage_frame`
5. `display_region / map_region`
6. `display_path / map_path`

## 12. 页面目标：地图编辑工作台

先实现一个页面：

- 页面名建议：`MapWorkbenchPage`

页面结构建议固定成三栏：

### 左栏

- 当前地图信息
- alignment 信息
- 图层开关
- 对象列表

### 中间

- 地图画布
- 图层渲染

### 右栏

- 当前选中对象详情
- warning / metadata 信息

## 13. 一期必须完成的显示图层

中间画布最少要有这几层：

1. 地图底图
2. zone 图层
3. 禁行区图层
4. 虚拟墙图层

先只要求“正确显示”，不要求“可编辑”。

## 14. 前端数据加载顺序

页面初始化时按这个顺序：

1. 连接 rosbridge
2. 获取当前地图
3. 获取当前地图 active alignment
4. 获取 zone 列表
5. 获取禁行区列表
6. 获取虚拟墙列表
7. 把四层渲染到画布

如果某一步失败，请按下面原则处理：

- 地图获取失败：页面不可用，显示错误
- alignment 获取失败：页面仍可显示地图，并默认按原始 `map` 坐标继续工作；提示“当前使用原始地图坐标”
- zone / 禁行区 / 虚拟墙获取失败：只影响对应图层，不阻断整页

## 15. 工程目录建议

请至少建立以下目录结构：

```text
src/
  api/
    ros/
      client.ts
      services.ts
  pages/
    MapWorkbenchPage.tsx
  components/
    canvas/
      MapCanvas.tsx
      ZoneLayer.tsx
      NoGoAreaLayer.tsx
      VirtualWallLayer.tsx
  stores/
    mapWorkbenchStore.ts
  hooks/
    useRosConnection.ts
    useMapWorkbenchData.ts
  types/
    ros.ts
    map-editor.ts
  utils/
    geometry.ts
```

## 16. 开发原则

### 16.1 坐标原则

前端展示时优先使用后端给的：

- `display_region`
- `display_path`
- `display_frame`

不要自己去推导正式几何。

### 16.2 正式几何真值源

后端才是正式几何规则的真值源。

前端可以做：

- 图形展示
- 临时交互
- 高亮

但不应在前端重新定义：

- 矩形生成规则
- polygon 正式坐标换算规则
- 规划长度/时长估算规则

### 16.3 代码风格

请优先：

- 小组件拆分
- 明确类型定义
- 不要把 rosbridge 调用散落在页面里
- service 调用统一封装
- topic 订阅统一封装

## 17. 第一阶段交付物

你必须先交付这几项：

1. Windows 环境安装完成
2. 前端工程初始化完成
3. rosbridge 连接成功
4. 地图编辑工作台页面可打开
5. 地图 + zone + 禁行区 + 虚拟墙 四层正确显示
6. 图层开关可用
7. 当前选中对象信息可展示

## 18. 第二阶段暂不做

这些后面再做，这一轮不要提前展开：

- 两点矩形选区
- 规划预览
- zone 提交
- zone 更新
- 禁行区编辑
- 虚拟墙编辑
- 任务/调度页面

## 19. 你完成后应该怎么汇报

每完成一个阶段，请给出：

1. 已完成内容
2. 改了哪些文件
3. 当前如何运行
4. 还缺什么
5. 阻塞点是什么

如果环境安装失败，请明确告诉我：

- 哪一步失败
- 终端输出是什么
- 你已经尝试了什么修复

## 20. 你现在就应该开始做的事情

现在不要继续讨论方案。

请立即开始：

1. 检查 Windows 是否安装 `node / npm / git`
2. 没有就安装
3. 新建前端工程
4. 安装依赖
5. 搭好最小目录结构
6. 建立 rosbridge 连接层
7. 开始做地图编辑工作台只读版

## 21. 补充说明

这不是一个纯 UI 项目，而是一个 ROS 后端联调项目。

所以你在实现时必须优先保证：

- 能稳定连接 rosbridge
- service 调用封装干净
- 类型定义清晰
- 图层显示正确

先把“联调通”放在“页面精美”前面。

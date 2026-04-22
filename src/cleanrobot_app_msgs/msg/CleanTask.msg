int32 task_id # 任务id
string name # 任务名称
int8 status # 任务状态 0: 未运行 1: 运行中/暂停中
int32 loops # 任务执行次数
string zone_id # 当前地图下的区域ID
string plan_profile_name # 规划配置名
string sys_profile_name # 运行配置名
string clean_mode # 清扫模式
string map_name # 任务绑定地图
string map_revision_id # 任务绑定地图 revision
bool return_to_dock_on_finish # 任务完成后是否自动回桩补给/充电
bool repeat_after_full_charge # 任务完成后充满电是否自动重启定位并重新执行
bool enabled # 任务是否启用

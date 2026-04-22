string schedule_id # 班表ID
int32 task_id # 绑定的任务ID(job_id)
string task_name # 绑定任务名称（只读回显）
bool enabled # 班表是否启用
string type # weekly / daily / once
int32[] dow # weekly: Monday=0 ... Sunday=6
string time # weekly/daily: HH:MM
string at # once: YYYY-MM-DD HH:MM
string timezone # 预留字段，当前默认 Asia/Shanghai
string start_date # 预留字段，weekly/daily 可选
string end_date # 预留字段，weekly/daily 可选
string map_name # 绑定任务地图（只读回显）
string map_revision_id # 绑定任务地图 revision（只读回显）
string zone_id # 绑定任务区域（只读回显）
int32 loops # 绑定任务圈数（只读回显）
string plan_profile_name # 绑定任务规划配置（只读回显）
string sys_profile_name # 绑定任务运行配置（只读回显）
string clean_mode # 绑定任务模式（只读回显）
bool return_to_dock_on_finish # 绑定任务完成后是否自动回桩补给（只读回显）
bool repeat_after_full_charge # 绑定任务完成后充满电是否自动重新执行（只读回显）
float64 last_fire_ts # 上次触发时间（epoch）
float64 last_done_ts # 上次完成时间（epoch）
string last_status # 上次完成状态

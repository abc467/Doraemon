int32 id # 任务id
string name # 任务名称
int8 status # 任务状态 0: 未开始 1: 进行中 (TODO: 暂未实现)
int32 num # 任务执行次数
uint8 is_loop #是否回扫(TODO: 暂未实现)
CleanTaskCell[] cells #单个任务由多个任务单元组成
string zone_id
string plan_profile_name
string sys_profile_name
string clean_mode
string map_name
string map_version
bool enabled

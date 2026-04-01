# 上报任务进度消息
string name     # 任务名称
int32 id       # 任务ID
int32 total_progress  # 总的任务进度 , 例: 2502
int32 cur_progress    # 当前任务的进度 , 例: 526 (即526/2502)
int32 total_num   # 总的任务轮数 , 例: 5
int32 cur_num     # 当前正在执行第几轮任务 , 例: 3 (即第3轮)
int32 total_cell  # 当前任务总共有多少个子任务 , 例: 5
int32 cur_cell    # 当前正在执行第几个子任务 , 例: 2  (即第2个子任务)
int8 return_status # 0:无, 1:正在返回补水站

# 话题名: "/task_progress"
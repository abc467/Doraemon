string mission_state
string phase
string public_state

string active_job_id

string run_id
string zone_id
string plan_profile
string sys_profile
string mode

# best-effort: resolved plan_id for current run
string plan_id

# best-effort: current map identity (for UI/ops)
string map_id
string map_md5

uint32 loops_total
uint32 loops_done
uint32 active_loop_index

float32 progress_0_1
float32 progress_pct

string executor_state

# error summary (from health monitor / executor)
string error_code
string error_msg

# actuation interlock (best-effort)
bool interlock_active
string interlock_reason

# last notable event (best-effort)
string last_event

float32 battery_soc
bool battery_valid

time stamp

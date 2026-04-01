string run_id
string zone_id
string plan_id
string state

# --- task intent snapshot (best-effort) ---
string plan_profile
string sys_profile
string mode

# --- map identity (best-effort) ---
string map_id
string map_md5

# --- error summary (best-effort) ---
string error_code
string error_msg

# --- actuation interlock (best-effort) ---
bool interlock_active
string interlock_reason

# --- velocity snapshot (best-effort) ---
float32 v_mps
float32 w_rps

uint32 exec_index
int32 block_id
uint32 path_index

float32 path_s
float32 block_length_m
float32 total_length_m

float32 progress_0_1
float32 progress_pct

time stamp

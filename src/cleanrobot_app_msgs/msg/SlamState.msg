string robot_id

string desired_mode
string current_mode

string active_map_name
string active_map_revision_id
string active_map_id
string active_map_md5

string runtime_map_name
string runtime_map_revision_id
string runtime_map_id
string runtime_map_md5

string pending_map_name
string pending_map_revision_id
string pending_map_switch_status

string localization_state
bool localization_valid

bool runtime_map_ready
bool active_map_match

string lifecycle_state
string active_job_id
string active_job_status
string active_job_phase
float32 active_job_progress_0_1

bool map_topic_fresh
float32 map_age_s
bool tracked_pose_fresh
float32 tracked_pose_age_s

string mission_state
string phase
string public_state
string executor_state
bool task_running

bool localization_backend_available
# Historical wire field names retained for message compatibility.
# They now report canonical runtime capability on the app surface,
# not whether old reload/save_state services are publicly exposed.
bool runtime_reload_service_available
bool runtime_save_state_service_available

bool can_switch_map_and_localize
bool can_relocalize
bool can_verify_map_revision
bool can_activate_map_revision
bool can_start_mapping
bool can_save_mapping
bool can_stop_mapping

string last_error_code
string last_error_msg
string[] blocking_reasons
string[] warnings

time stamp

string runtime_mode
string workflow_state
string workflow_phase
bool busy
bool runtime_map_match
bool mapping_session_active
bool task_ready
bool manual_assist_required
string progress_text
string blocking_reason
string last_error_message
string manual_assist_map_name
string manual_assist_map_revision_id
string manual_assist_retry_action
string manual_assist_guidance

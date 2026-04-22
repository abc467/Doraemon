string robot_id
# Canonical odom stream summary fields.
string odom_source
string odom_topic
string raw_odom_topic
string imu_topic
string validation_mode
bool connected
bool odom_stream_ready
bool frame_id_valid
bool child_frame_id_valid
# Implementation-specific diagnostics retained for debug/observability.
bool wheel_speed_node_ready
bool imu_preprocess_node_ready
bool ekf_node_ready
bool wheel_speed_fresh
bool imu_fresh
bool odom_fresh
bool odom_valid
float32 wheel_speed_age_s
float32 imu_age_s
float32 odom_age_s
string error_code
string message
string[] warnings
time stamp

# mcore_chassis_bridge

Serial bridge for the new M-core chassis velocity command.

The node subscribes to `geometry_msgs/Twist`, sends command `0x4060` over the
serial port, and encodes the frame exactly as the M-core protocol describes:

```text
43 4E LEN_H LEN_L 40 60 vx(float32 LE) wz(float32 LE) CHECKSUM DA
```

On the current chassis firmware, `vx` is expected as `mm/s` and `wz` is expected
as `mrad/s`, even though ROS uses `m/s` and `rad/s`. The launch file therefore
defaults both velocity scales to `1000.0`. For example, ROS `linear.x=0.05`
becomes protocol `vx=50.0`, and ROS `angular.z=0.0349` becomes protocol
`wz=34.9`.

Example:

```bash
roslaunch mcore_chassis_bridge mcore_velocity_sender.launch serial_device:=/dev/ttyUSB0 serial_baudrate:=115200
```

For a simple motion test:

```bash
rostopic pub -r 10 /cmd_vel geometry_msgs/Twist \
  "{linear: {x: 0.05, y: 0.0, z: 0.0}, angular: {x: 0.0, y: 0.0, z: 0.0}}"
```

The bridge republishes the latest command at `send_rate_hz` and sends zero
velocity after `cmd_timeout_sec` without a fresh command.

Frame logging is enabled by default during bringup. To reduce log output:

```bash
roslaunch mcore_chassis_bridge mcore_velocity_sender.launch enable_tx_log:=false
```

Receive status logging is also enabled by default. If M-core sends command
`0x4070`, the node prints battery, water level, obstacle, emergency stop,
brake, power feedback, and lidar-ready bits.

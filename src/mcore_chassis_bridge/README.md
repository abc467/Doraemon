# mcore_chassis_bridge

Bridge for the new M-core chassis velocity and cleaning mechanism commands.
The default transport is TCP to `192.168.127.10:8080`; the same protocol frames
can still be carried over a serial port by launching with `transport:=serial`.

The node subscribes to `geometry_msgs/Twist`, sends command `0x4060` over the
selected transport, and encodes the frame exactly as the M-core protocol
describes:

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
roslaunch mcore_chassis_bridge mcore_velocity_sender.launch \
  transport:=tcp tcp_host:=192.168.127.10 tcp_port:=8080
```

Serial fallback:

```bash
roslaunch mcore_chassis_bridge mcore_velocity_sender.launch \
  transport:=serial serial_device:=/dev/ttyUSB0 serial_baudrate:=115200
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

When firmware sends a complete `0x4070` frame, the bridge publishes the raw
physical safety byte on `/mcore_velocity_sender/safety_status_bits`
(`std_msgs/UInt8`). This topic is deliberately non-latched and is emitted only
after strict header, length and checksum validation. The safety byte is optional
in the current protocol, so undocumented active `0x4070` polling is disabled by
default; it can be enabled with `enable_status_poll:=true` only on firmware that
is known to support that query.

Every checksum-valid known M-core response also advances the non-latched
`/mcore_velocity_sender/telemetry_heartbeat` (`std_msgs/UInt64`). This proves
current transport liveness for compatibility firmware, but it must not be
presented as readable physical emergency-stop state.

The node polls M-core read commands for battery and tank levels by default:

```text
battery remaining 0x6102: 43 4E 00 08 61 02 FC DA
clean water level 0x610A: 43 4E 00 08 61 0A 04 DA
sewage level      0x610B: 43 4E 00 08 61 0B 05 DA
```

Telemetry polling is staggered for production use: the bridge sends only one
read command per `telemetry_poll_interval_sec` tick. The default tick is `1.3s`,
so with battery, clean-water, and sewage polling all enabled, each value is
requested about once every three seconds.

`0x6102` response data is parsed as an unsigned little-endian 16-bit value with
unit `0.1A`. The parsed value is published on `/mcore/battery_remaining` and
copied into `/battery_state.charge`. The bridge also converts it to
`/battery_state.percentage` with `remaining / battery_full_capacity`; the default
full-capacity value is `150.0`.

`0x610A` and `0x610B` response data are published as raw unsigned 16-bit tank
levels on `/mcore/clean_water_level` and `/mcore/sewage_level`. Firmware reports
the four tank levels as cumulative low-bit masks:

```text
raw 1  / 0b0001 = gear 1
raw 3  / 0b0011 = gear 2
raw 7  / 0b0111 = gear 3
raw 15 / 0b1111 = gear 4
```

The bridge maps these gears into `/combined_status.clean_level` and
`/combined_status.sewage_level` as `25/50/75/100`; raw `0` maps to `0`.

The same node also accepts the existing cleaning-control topics and
encodes the new M-core cleaning commands:

| Topic | Message | Serial command |
| --- | --- | --- |
| `/mcore/control_clean_tools` | `robot_platform_msgs/ControlCleanTools` | brush lift `0x6004`, squeegee `0x6005`, main brush `0x6001`, side brush `0x6002` |
| `/mcore/control_water_tap` | `robot_platform_msgs/ControlWaterTap` | clean water pump `0x6003`, clean water valve `0x6023`, sewage valve `0x6024`, suction fan `0x6006` |
| `/mcore/control_motor` | `robot_platform_msgs/ControlMotor` | suction fan `0x6006` |
| `/mcore/cleaning_params/set` | `robot_platform_msgs/CleaningParams` | updates active profile values for main brush, side brush, brush-down distance, water pump, and suction |
| `/mcore/charge_enable` | `std_msgs/Bool` | battery charging switch `0x6022`, data `1` enables charging and `0` disables charging |

When auto docking reaches the charge phase, `dock_supply_manager` publishes
`/mcore/charge_enable=true`. The bridge repeats the `0x6022` command by
default to make the charge-enable command robust during docking contact:

```text
charge off: 43 4E 00 0A 60 22 00 00 1D DA
charge on : 43 4E 00 0A 60 22 01 00 1E DA
```

Side brush `0x6002` is treated as a 0-100 speed/strength value by the current
M-core firmware. Runtime cleaning strengths should come from the active
`actuator_profiles.yaml` profile; the bridge falls back to safe off values until
it receives `/mcore/cleaning_params/set`.

Direct bringup test topics are available under the node namespace:

```bash
rostopic pub -1 /mcore_velocity_sender/brush_lift_cmd std_msgs/Int16 "data: 50"
rostopic pub -1 /mcore_velocity_sender/side_brush_cmd std_msgs/Int16 "data: 10"
rostopic pub -1 /mcore_velocity_sender/squeegee_cmd std_msgs/Int16 "data: -1"
rostopic pub -1 /mcore_velocity_sender/main_brush_speed_cmd std_msgs/Int16 "data: 40"
rostopic pub -1 /mcore_velocity_sender/water_pump_cmd std_msgs/Int16 "data: 100"
rostopic pub -1 /mcore_velocity_sender/suction_fan_cmd std_msgs/Int16 "data: 70"
```

The clean water valve is controlled through `/mcore/control_water_tap` with
`tap_id: 2`; operation `1` opens it and operation `0` closes it. The sewage
valve uses the same topic with `tap_id: 3`:

```text
sewage valve off: 43 4E 00 0A 60 24 00 00 1F DA
sewage valve on : 43 4E 00 0A 60 24 01 00 20 DA
```

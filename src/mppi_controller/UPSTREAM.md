# Nav2 MPPI ROS1 port

This package ports the controller algorithms and path-handling semantics from
the upstream Nav2 `main` branch into the project's ROS1 `nav_core` / MBF plugin
interface.

- Upstream repository: https://github.com/ros-navigation/navigation2
- Reviewed revision: `538e76858d7940b427d2dd987a9f943a6f332335`
- Upstream components: `nav2_mppi_controller` and
  `nav2_controller/plugins/feasible_path_handler.cpp`
- Upstream license: Apache-2.0

The port keeps the MPPI sampling optimizer, warm-started control sequence,
motion-model constraints and delay compensation, noise policy, Savitzky-Golay
filter order, path/goal critics, and official nearest/prune path-window
semantics. The ROS2 lifecycle, controller-server, dynamic parameter, and
Costmap2DROS APIs remain a thin ROS1 adapter rather than being copied into the
workspace. ROS1 has no direct equivalent of Nav2's typed parameter-event
handler, so each controller exposes `~/<controller>/reload_parameters`
(`std_srvs/Trigger`). Reload builds a complete replacement optimizer, critics,
motion model and trajectory validator first, then swaps it under the controller
mutex. Invalid updates leave the running instance untouched. Velocity-bound
changes are rejected while a runtime speed limit is active.

Intentional project safety additions are fail-closed unknown-space handling
and an independent, densely interpolated filled-footprint validation of the
selected post-filter trajectory. The differential-drive production vehicle
uses the DiffDrive model; the unused upstream Ackermann plugin is not part of
this ROS1 package.

The optimizer publishes the constrained, filtered softmax-weighted control
sequence produced by MPPI. It never substitutes one sampled rollout or a blend
toward a sampled rollout for that weighted result. When the optional independent
continuous validator rejects the selected trajectory, the existing bounded full
optimizer reset/retry path runs; a repeated rejection remains fail-closed.

The Standard and State production profiles currently opt out with
`TrajectoryValidator/enabled: false`. This mode retains per-rollout,
per-model-step filled-footprint collision checking in `CostCritic`, but
publishes the constrained and filtered softmax control sequence directly,
matching the v9 selection semantics.

There is no full-path Forward/Rotation/Reverse phase state machine, fixed
terminal-tail replay, independent terminal controller, or deterministic
rotate-to-goal bypass around MPPI.

## Lightweight critic statistics

The ROS1 adapter provides an opt-in, subscriber-gated equivalent of upstream
`CriticsStats` without requiring a custom ROS message or trajectory Marker
construction:

- `publish_critic_stats` (default `false`) advertises
  `~/<controller_name>/critic_stats` as `diagnostic_msgs/DiagnosticArray`.
- `critic_stats_publish_period` (default `10`) selects the number of evaluated
  control cycles aggregated into each message.
- No critic-cost vectors are copied unless the feature is enabled and the
  topic has an active subscriber.

Each critic reports its additive cost mean/min/max/sum, changed-sample ratio,
active evaluation count and mean scoring time. This makes critic balancing
observable without enabling the expensive candidate-trajectory visualizer.
The overall status also reports the furthest reachable reference index and
arc length together with the maximum candidate-trajectory arc length. These
values verify that compact or self-near paths cannot alias a geometrically
close path segment beyond the rollout's physically reachable distance.

`config/mppi_state_official_baseline.yaml` records the upstream default values
at the reviewed revision. It is intentionally not registered or loaded by the
production navigation launch; generic upstream velocity defaults are not live
vehicle limits.

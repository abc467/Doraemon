# rosbridge_server source overlay provenance

This directory is a source overlay for the Ubuntu 20.04 / ROS Noetic
commercial release. It exists because the Noetic binary package reports the
configured `address` parameter but rosbridge_server 0.11.17 does not pass that
address to Autobahn's listening API.

Upstream baseline:

- Repository: `https://github.com/RobotWebTools/rosbridge_suite.git`
- Release tag: `0.11.17`
- Baseline commit: `55a6bdb20842a3f7f0e17931b7856c3d8700f9fb`
- `rosbridge_server` tree: `41a5113788a5a6769a3b0de86efe93618669033b`
- License: BSD-3-Clause; the upstream `LICENSE` and `AUTHORS.md` are retained
  in this directory.

Security patch:

- Upstream fix commit: `f6a829abaeca9763c5d00ba5a232407e789bdbfa`
- Upstream subject: `Fix IP binding of rosbridge_server (#1047)`
- Modified file: `scripts/rosbridge_websocket.py`
- Exact hunk: pass `interface=factory.host` to `listenWS`.
- Commercial hardening: both the launch argument and Python empty-value
  fallback default to `127.0.0.1`; no wildcard-listen default remains.
- Baseline script SHA256:
  `c3f3d27b1f32f0fe80aaf427178b384c2ad361857c9c6f8a45d0fdbdcd6f8275`
- Patched script SHA256:
  `f5b31634e8a759ee54db9b36bf1a64b3ee96ab4bfbf5f1d8e8ade21c5c707021`

The complete upstream `rosbridge_server` package was copied with executable
bits and script symlinks preserved. No upstream Git directory and no other
rosbridge_suite packages are included. Runtime dependencies continue to come
from the pinned ROS Noetic/Ubuntu packages. The package version remains
`0.11.17`; the Doraemon release commit and tag identify this patched build.

Validated Ubuntu 20.04 build-baseline package versions:

- `python3-autobahn 17.10.1+dfsg1-6`
- `python3-twisted 18.9.0-11ubuntu0.20.04.5`
- `ros-noetic-rosbridge-server 0.11.17-1focal.20250520.012730`
- `ros-noetic-rosbridge-library 0.11.17-1focal.20250520.012154`
- `ros-noetic-rosbridge-msgs 0.11.17-1focal.20250426.012736`
- `ros-noetic-rosapi 0.11.17-1focal.20250520.012523`

Run `scripts/verify_rosbridge_loopback_patch.py` from the Doraemon release root
before and after building. Stage K must additionally prove at kernel level that
port 9090 listens only on `127.0.0.1` and rejects the machine's Wi-Fi and robot
LAN addresses.

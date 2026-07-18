# FLIRT compatibility snapshot

This directory is the FLIRT source snapshot used by the Doraemon Cartographer
integration that was validated on the x86_64 Ubuntu 20.04 robot controller in
March 2026.

The upstream project is:

- `https://github.com/OpenSLAM-org/openslam_flirtlib`
- reference commit `56bf33effbdae8280be4039309523203b8e4257c`

The snapshot contains compatibility changes accumulated by the Doraemon
integration, including target names (`flirtlib_*`), the reduced non-GUI build,
and descriptor access used by Cartographer serialization.

FLIRT is distributed under LGPL terms; see `COPYING` and `COPYING.LESSER`.
Commercial release owners must keep the license notices and complete the
project's third-party license review before customer distribution.

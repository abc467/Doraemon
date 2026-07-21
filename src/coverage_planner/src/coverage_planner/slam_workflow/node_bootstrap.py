# -*- coding: utf-8 -*-

import grp
import os
import stat
from dataclasses import dataclass

import rospy


DEPLOYMENT_SLAM_CONFIG_ROOT = "/data/config/slam/cartographer"
DEPLOYMENT_SLAM_RUNTIME_LOG_ROOT = "/var/log/doraemon/slam-runtime"
DEPLOYMENT_RELEASES_ROOT = "/opt/doraemon/releases"
DEPLOYMENT_RUNTIME_LOGS_ROOT = "/var/log/doraemon"


def workspace_root_from_node(node_file: str) -> str:
    return os.path.abspath(os.path.join(os.path.dirname(node_file), "..", "..", ".."))


def workspace_setup_path(workspace_root: str) -> str:
    for candidate in (
        os.path.join(workspace_root, "install", "setup.bash"),
        os.path.join(workspace_root, "devel", "setup.bash"),
    ):
        if os.path.isfile(candidate):
            return candidate
    return ""


def canonical_slam_config_root(workspace_root: str) -> str:
    return os.path.join(workspace_root, "src", "cleanrobot", "config", "slam", "cartographer")


def is_slam_config_root(path: str) -> bool:
    candidate = os.path.abspath(os.path.expanduser(str(path or "").strip()))
    if not candidate:
        return False
    required = (
        os.path.join(candidate, "slam", "config.lua"),
        os.path.join(candidate, "pure_location_odom", "config.lua"),
        os.path.join(candidate, "relocalization", "global_relocation.sml"),
    )
    return all(os.path.isfile(item) for item in required)


def _decode_mountinfo_path(value: str) -> str:
    return (
        str(value)
        .replace("\\040", " ")
        .replace("\\011", "\t")
        .replace("\\012", "\n")
        .replace("\\134", "\\")
    )


def _contains_mount_point(path: str) -> bool:
    root = os.path.realpath(path)
    try:
        with open("/proc/self/mountinfo", "r", encoding="utf-8") as handle:
            for raw_line in handle:
                left = raw_line.split(" - ", 1)[0].split()
                if len(left) < 5:
                    continue
                target = os.path.realpath(_decode_mountinfo_path(left[4]))
                try:
                    if os.path.commonpath((root, target)) == root:
                        return True
                except ValueError:
                    continue
    except OSError:
        return True
    return False


def is_reviewed_deployment_slam_config_root(path: str) -> bool:
    """Require the external Lua/SML tree to be root-managed and read-only."""
    candidate = os.path.abspath(os.path.expanduser(str(path or "").strip()))
    if candidate != DEPLOYMENT_SLAM_CONFIG_ROOT or os.path.realpath(candidate) != candidate:
        return False
    if not is_slam_config_root(candidate) or _contains_mount_point(candidate):
        return False
    try:
        expected_gid = grp.getgrnam("a").gr_gid
        for current_root, dirnames, filenames in os.walk(
            candidate, topdown=True, followlinks=False
        ):
            paths = [current_root]
            paths.extend(os.path.join(current_root, name) for name in dirnames)
            paths.extend(os.path.join(current_root, name) for name in filenames)
            for item in paths:
                info = os.lstat(item)
                if stat.S_ISLNK(info.st_mode):
                    return False
                if info.st_uid != 0 or info.st_gid != expected_gid:
                    return False
                mode = stat.S_IMODE(info.st_mode)
                if stat.S_ISDIR(info.st_mode):
                    if mode != 0o750:
                        return False
                elif stat.S_ISREG(info.st_mode):
                    if mode != 0o640:
                        return False
                else:
                    return False
    except (KeyError, OSError):
        return False
    return True


def deployment_slam_config_override_has_entries(path: str) -> bool:
    """Return whether an installed override contains anything, failing closed."""
    if not os.path.lexists(path):
        return False
    try:
        with os.scandir(path) as entries:
            return next(entries, None) is not None
    except OSError as exc:
        raise RuntimeError(
            "cannot inspect external SLAM config override: %s" % path
        ) from exc


def resolve_slam_config_root(raw_path: str, workspace_root: str) -> str:
    canonical = os.path.realpath(canonical_slam_config_root(workspace_root))
    external = DEPLOYMENT_SLAM_CONFIG_ROOT
    raw_candidate = str(raw_path or "").strip()

    if raw_candidate:
        candidate = os.path.realpath(os.path.expanduser(raw_candidate))
        if candidate == external:
            if not is_reviewed_deployment_slam_config_root(external):
                raise RuntimeError(
                    "external SLAM config root is not a reviewed root:a read-only tree: %s"
                    % external
                )
            return external
        if candidate == canonical and is_slam_config_root(canonical):
            return canonical
        raise RuntimeError("SLAM config root is outside the approved locations: %s" % candidate)

    # Only a completely empty staging directory may fall back to the immutable
    # release. Any entry makes the override authoritative, so incomplete or
    # unreviewed content must stop startup instead of being ignored.
    if deployment_slam_config_override_has_entries(external):
        if not is_reviewed_deployment_slam_config_root(external):
            raise RuntimeError(
                "external SLAM config override exists but is not reviewed and read-only: %s"
                % external
            )
        return external
    if is_slam_config_root(canonical):
        return canonical
    raise RuntimeError("no complete SLAM config root exists in the approved locations")


def default_slam_config_root(workspace_root: str) -> str:
    return resolve_slam_config_root("", workspace_root)


def resolve_slam_runtime_log_root(raw_path: str, workspace_root: str) -> str:
    """Resolve an external writable log root and reject release-tree paths."""
    raw_workspace = str(workspace_root or "").strip()
    workspace = os.path.realpath(os.path.expanduser(raw_workspace)) if raw_workspace else ""
    raw_candidate = str(raw_path or "").strip() or DEPLOYMENT_SLAM_RUNTIME_LOG_ROOT
    candidate = os.path.realpath(os.path.expanduser(raw_candidate))
    releases_root = os.path.realpath(DEPLOYMENT_RELEASES_ROOT)
    runtime_logs_root = os.path.realpath(DEPLOYMENT_RUNTIME_LOGS_ROOT)

    def is_inside(root: str) -> bool:
        if not root:
            return False
        try:
            return os.path.commonpath((root, candidate)) == root
        except ValueError:
            return False

    if is_inside(workspace):
        raise RuntimeError(
            "slam runtime log_root must be outside the immutable workspace: %s" % candidate
        )
    if is_inside(releases_root):
        raise RuntimeError(
            "slam runtime log_root must be outside the immutable releases tree: %s"
            % candidate
        )
    if not is_inside(runtime_logs_root):
        raise RuntimeError(
            "slam runtime log_root must stay under /var/log/doraemon: %s" % candidate
        )
    return candidate


def map_id_from_md5(map_md5: str) -> str:
    value = str(map_md5 or "").strip()
    if not value:
        return ""
    return "map_%s" % value[:8]


def iter_candidate_prefixes(workspace_root: str, environ=None):
    env = os.environ if environ is None else environ
    seen = set()
    for prefix in str(env.get("CMAKE_PREFIX_PATH", "")).split(os.pathsep):
        candidate = os.path.abspath(os.path.expanduser(str(prefix or "").strip()))
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        yield candidate

    for candidate in (
        os.path.join(workspace_root, "devel"),
        os.path.join(workspace_root, "install"),
        os.path.join(workspace_root, "install_isolated"),
    ):
        candidate = os.path.abspath(candidate)
        if candidate in seen:
            continue
        seen.add(candidate)
        yield candidate


def resolve_binary_path(
    explicit_path: str,
    package_name: str,
    binary_name: str,
    workspace_root: str,
    environ=None,
) -> str:
    raw_path = str(explicit_path or "").strip()
    if raw_path:
        path = os.path.abspath(os.path.expanduser(raw_path))
        if os.path.isfile(path) and os.access(path, os.X_OK):
            return path
        raise RuntimeError("binary not executable: %s" % path)

    checked = []
    for prefix in iter_candidate_prefixes(workspace_root, environ=environ):
        candidate = os.path.join(prefix, "lib", package_name, binary_name)
        checked.append(candidate)
        if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
            return candidate

    raise RuntimeError(
        "failed to resolve %s/%s from workspace prefixes, checked: %s"
        % (package_name, binary_name, ", ".join(checked))
    )


def apply_bootstrap(target, bootstrap) -> None:
    target.__dict__.update(vars(bootstrap))


@dataclass
class SlamRuntimeManagerBootstrap:
    workspace_root: str
    workspace_setup_path: str
    slam_config_root: str
    plan_db_path: str
    ops_db_path: str
    maps_root: str
    repo_map_root: str
    log_root: str
    robot_id: str
    runtime_ns: str
    app_service_name: str
    app_submit_job_service_name: str
    app_get_job_service_name: str
    job_state_topic_name: str
    app_service_contract_param_ns: str
    app_submit_job_contract_param_ns: str
    app_get_job_contract_param_ns: str
    map_topic: str
    tracked_pose_topic: str
    initial_pose_topic: str
    tf_parent_frame: str
    tf_child_frame: str
    odom_topic: str
    odometry_status_service: str
    set_param_service: str
    get_param_service: str
    mapping_config_entry: str
    localization_config_entry: str
    runtime_flag_server_bin: str
    cartographer_node_bin: str
    occupancy_grid_node_bin: str
    include_frozen_submaps: bool
    localization_include_unfrozen_submaps: bool
    mapping_include_unfrozen_submaps: bool
    map_resolution: float
    map_publish_period_sec: float
    runtime_startup_delay_s: float
    runtime_process_ready_timeout_s: float
    odometry_ready_timeout_s: float
    ready_timeout_s: float
    tf_poll_timeout_s: float
    tracked_pose_fresh_timeout_s: float
    localization_stable_window_s: float
    localization_stable_max_pose_jump_m: float
    localization_stable_max_yaw_jump_rad: float
    localization_degraded_timeout_s: float
    command_timeout_s: float
    stop_timeout_s: float
    allow_identity_rebind_on_localize: bool
    max_job_history: int
    initial_pose_cov_xy: float
    initial_pose_cov_yaw: float


def load_slam_runtime_manager_bootstrap(node_file: str, rospy_module=rospy) -> SlamRuntimeManagerBootstrap:
    workspace_root = workspace_root_from_node(node_file)
    default_config_root = default_slam_config_root(workspace_root)
    default_maps_root = os.path.expanduser(str(rospy_module.get_param("~maps_root", "/data/maps")).strip())
    return SlamRuntimeManagerBootstrap(
        workspace_root=workspace_root,
        workspace_setup_path=workspace_setup_path(workspace_root),
        slam_config_root=resolve_slam_config_root(
            rospy_module.get_param("~config_root", default_config_root),
            workspace_root,
        ),
        plan_db_path=rospy_module.get_param("~plan_db_path", "/data/coverage/planning.db"),
        ops_db_path=rospy_module.get_param("~ops_db_path", "/data/coverage/operations.db"),
        maps_root=default_maps_root,
        repo_map_root=os.path.expanduser(
            str(rospy_module.get_param("~repo_map_root", default_maps_root)).strip()
        ),
        log_root=resolve_slam_runtime_log_root(
            rospy_module.get_param("~log_root", DEPLOYMENT_SLAM_RUNTIME_LOG_ROOT),
            workspace_root,
        ),
        robot_id=str(rospy_module.get_param("~robot_id", "local_robot")).strip() or "local_robot",
        runtime_ns=str(rospy_module.get_param("~runtime_ns", "/cartographer/runtime")).rstrip("/"),
        app_service_name=(
            str(rospy_module.get_param("~app_service_name", "/cartographer/runtime/app/operate")).strip()
            or "/cartographer/runtime/app/operate"
        ),
        app_submit_job_service_name=(
            str(rospy_module.get_param("~app_submit_job_service_name", "/cartographer/runtime/app/submit_job")).strip()
            or "/cartographer/runtime/app/submit_job"
        ),
        app_get_job_service_name=(
            str(rospy_module.get_param("~app_get_job_service_name", "/cartographer/runtime/app/get_job")).strip()
            or "/cartographer/runtime/app/get_job"
        ),
        job_state_topic_name=(
            str(rospy_module.get_param("~job_state_topic_name", "/cartographer/runtime/job_state")).strip()
            or "/cartographer/runtime/job_state"
        ),
        app_service_contract_param_ns=(
            str(
                rospy_module.get_param(
                    "~app_service_contract_param_ns",
                    "/cartographer/runtime/contracts/app/operate",
                )
            ).strip()
            or "/cartographer/runtime/contracts/app/operate"
        ),
        app_submit_job_contract_param_ns=(
            str(
                rospy_module.get_param(
                    "~app_submit_job_contract_param_ns",
                    "/cartographer/runtime/contracts/app/submit_job",
                )
            ).strip()
            or "/cartographer/runtime/contracts/app/submit_job"
        ),
        app_get_job_contract_param_ns=(
            str(
                rospy_module.get_param(
                    "~app_get_job_contract_param_ns",
                    "/cartographer/runtime/contracts/app/get_job",
                )
            ).strip()
            or "/cartographer/runtime/contracts/app/get_job"
        ),
        map_topic=str(rospy_module.get_param("~map_topic", "/map")).strip() or "/map",
        tracked_pose_topic=(
            str(rospy_module.get_param("~tracked_pose_topic", "/tracked_pose")).strip() or "/tracked_pose"
        ),
        initial_pose_topic=(
            str(rospy_module.get_param("~initial_pose_topic", "/initialpose")).strip() or "/initialpose"
        ),
        tf_parent_frame=str(rospy_module.get_param("~tf_parent_frame", "map")).strip() or "map",
        tf_child_frame=str(rospy_module.get_param("~tf_child_frame", "odom")).strip() or "odom",
        odom_topic=str(rospy_module.get_param("~odom_topic", "/odom")).strip() or "/odom",
        odometry_status_service=(
            str(
                rospy_module.get_param(
                    "~odometry_status_service",
                    "/clean_robot_server/app/get_odometry_status",
                )
            ).strip()
            or "/clean_robot_server/app/get_odometry_status"
        ),
        set_param_service=str(rospy_module.get_param("~set_param_service", "/set_param")).strip() or "/set_param",
        get_param_service=str(rospy_module.get_param("~get_param_service", "/get_param")).strip() or "/get_param",
        mapping_config_entry=str(rospy_module.get_param("~mapping_config_entry", "slam")).strip() or "slam",
        localization_config_entry=(
            str(rospy_module.get_param("~localization_config_entry", "pure_location_odom")).strip()
            or "pure_location_odom"
        ),
        runtime_flag_server_bin=resolve_binary_path(
            rospy_module.get_param("~runtime_flag_server_bin", ""),
            "robot_runtime_flags",
            "runtime_flag_server_node",
            workspace_root,
        ),
        cartographer_node_bin=resolve_binary_path(
            rospy_module.get_param("~cartographer_node_bin", ""),
            "cartographer_ros",
            "cartographer_node",
            workspace_root,
        ),
        occupancy_grid_node_bin=resolve_binary_path(
            rospy_module.get_param("~occupancy_grid_node_bin", ""),
            "cartographer_ros",
            "cartographer_occupancy_grid_node",
            workspace_root,
        ),
        include_frozen_submaps=bool(rospy_module.get_param("~include_frozen_submaps", True)),
        localization_include_unfrozen_submaps=bool(
            rospy_module.get_param("~localization_include_unfrozen_submaps", False)
        ),
        mapping_include_unfrozen_submaps=bool(rospy_module.get_param("~mapping_include_unfrozen_submaps", True)),
        map_resolution=float(rospy_module.get_param("~map_resolution", 0.05)),
        map_publish_period_sec=float(rospy_module.get_param("~map_publish_period_sec", 1.0)),
        runtime_startup_delay_s=max(0.0, float(rospy_module.get_param("~runtime_startup_delay_s", 0.5))),
        runtime_process_ready_timeout_s=max(
            5.0, float(rospy_module.get_param("~runtime_process_ready_timeout_s", 30.0))
        ),
        odometry_ready_timeout_s=max(
            5.0,
            float(
                rospy_module.get_param(
                    "~odometry_ready_timeout_s",
                    rospy_module.get_param("~wheel_speed_ready_timeout_s", 30.0),
                )
            ),
        ),
        ready_timeout_s=max(10.0, float(rospy_module.get_param("~ready_timeout_s", 120.0))),
        tf_poll_timeout_s=max(0.05, float(rospy_module.get_param("~tf_poll_timeout_s", 0.2))),
        tracked_pose_fresh_timeout_s=max(
            0.2, float(rospy_module.get_param("~tracked_pose_fresh_timeout_s", 2.0))
        ),
        localization_stable_window_s=max(
            0.0, float(rospy_module.get_param("~localization_stable_window_s", 3.0))
        ),
        localization_stable_max_pose_jump_m=max(
            0.0, float(rospy_module.get_param("~localization_stable_max_pose_jump_m", 0.25))
        ),
        localization_stable_max_yaw_jump_rad=max(
            0.0, float(rospy_module.get_param("~localization_stable_max_yaw_jump_rad", 0.35))
        ),
        localization_degraded_timeout_s=max(
            0.5,
            float(
                rospy_module.get_param(
                    "~localization_degraded_timeout_s",
                    max(
                        6.0,
                        float(rospy_module.get_param("~tracked_pose_fresh_timeout_s", 2.0)) * 3.0,
                    ),
                )
            ),
        ),
        command_timeout_s=max(10.0, float(rospy_module.get_param("~command_timeout_s", 180.0))),
        stop_timeout_s=max(5.0, float(rospy_module.get_param("~stop_timeout_s", 30.0))),
        allow_identity_rebind_on_localize=bool(
            rospy_module.get_param("~allow_identity_rebind_on_localize", True)
        ),
        max_job_history=max(5, int(rospy_module.get_param("~max_job_history", 20))),
        initial_pose_cov_xy=float(rospy_module.get_param("~initial_pose_cov_xy", 0.25)),
        initial_pose_cov_yaw=float(rospy_module.get_param("~initial_pose_cov_yaw", 0.25)),
    )


@dataclass
class SlamApiServiceBootstrap:
    plan_db_path: str
    ops_db_path: str
    maps_root: str
    robot_id: str
    runtime_ns: str
    map_topic: str
    tracked_pose_topic: str
    task_state_topic: str
    state_topic_name: str
    odometry_state_topic_name: str
    odometry_status_service_name: str
    app_status_service_name: str
    app_submit_command_service_name: str
    app_get_job_service_name: str
    job_state_topic_name: str
    app_runtime_submit_job_service: str
    app_runtime_get_job_service: str
    runtime_job_state_topic: str
    app_status_contract_param_ns: str
    app_submit_command_contract_param_ns: str
    app_get_job_contract_param_ns: str
    map_identity_timeout_s: float
    map_fresh_timeout_s: float
    tracked_pose_fresh_timeout_s: float
    localization_degraded_timeout_s: float
    command_timeout_s: float
    state_publish_hz: float
    task_state_fresh_timeout_s: float
    odometry_state_fresh_timeout_s: float


def load_slam_api_service_bootstrap(node_file: str, rospy_module=rospy) -> SlamApiServiceBootstrap:
    return SlamApiServiceBootstrap(
        plan_db_path=rospy_module.get_param("~plan_db_path", "/data/coverage/planning.db"),
        ops_db_path=rospy_module.get_param("~ops_db_path", "/data/coverage/operations.db"),
        maps_root=os.path.expanduser(str(rospy_module.get_param("~maps_root", "/data/maps")).strip()),
        robot_id=str(rospy_module.get_param("~robot_id", "local_robot")).strip() or "local_robot",
        runtime_ns=str(rospy_module.get_param("~runtime_ns", "/cartographer/runtime")).rstrip("/"),
        map_topic=str(rospy_module.get_param("~map_topic", "/map")).strip() or "/map",
        tracked_pose_topic=(
            str(rospy_module.get_param("~tracked_pose_topic", "/tracked_pose")).strip() or "/tracked_pose"
        ),
        task_state_topic=(
            str(rospy_module.get_param("~task_state_topic", "/task_state")).strip()
            or "/task_state"
        ),
        state_topic_name=(
            str(rospy_module.get_param("~state_topic_name", "/clean_robot_server/slam_state")).strip()
            or "/clean_robot_server/slam_state"
        ),
        odometry_state_topic_name=(
            str(rospy_module.get_param("~odometry_state_topic_name", "/clean_robot_server/odometry_state")).strip()
            or "/clean_robot_server/odometry_state"
        ),
        odometry_status_service_name=(
            str(
                rospy_module.get_param(
                    "~odometry_status_service_name",
                    "/clean_robot_server/app/get_odometry_status",
                )
            ).strip()
            or "/clean_robot_server/app/get_odometry_status"
        ),
        app_status_service_name=(
            str(
                rospy_module.get_param(
                    "~app_status_service_name",
                    "/clean_robot_server/app/get_slam_status",
                )
            ).strip()
            or "/clean_robot_server/app/get_slam_status"
        ),
        app_submit_command_service_name=(
            str(
                rospy_module.get_param(
                    "~app_submit_command_service_name",
                    "/clean_robot_server/app/submit_slam_command",
                )
            ).strip()
            or "/clean_robot_server/app/submit_slam_command"
        ),
        app_get_job_service_name=(
            str(
                rospy_module.get_param(
                    "~app_get_job_service_name",
                    "/clean_robot_server/app/get_slam_job",
                )
            ).strip()
            or "/clean_robot_server/app/get_slam_job"
        ),
        job_state_topic_name=(
            str(rospy_module.get_param("~job_state_topic_name", "/clean_robot_server/slam_job_state")).strip()
            or "/clean_robot_server/slam_job_state"
        ),
        app_runtime_submit_job_service=(
            str(
                rospy_module.get_param(
                    "~app_runtime_submit_job_service",
                    "/cartographer/runtime/app/submit_job",
                )
            ).strip()
            or "/cartographer/runtime/app/submit_job"
        ),
        app_runtime_get_job_service=(
            str(
                rospy_module.get_param(
                    "~app_runtime_get_job_service",
                    "/cartographer/runtime/app/get_job",
                )
            ).strip()
            or "/cartographer/runtime/app/get_job"
        ),
        runtime_job_state_topic=(
            str(rospy_module.get_param("~runtime_job_state_topic", "/cartographer/runtime/job_state")).strip()
            or "/cartographer/runtime/job_state"
        ),
        app_status_contract_param_ns=(
            str(
                rospy_module.get_param(
                    "~app_status_contract_param_ns",
                    "/clean_robot_server/contracts/app/get_slam_status",
                )
            ).strip()
            or "/clean_robot_server/contracts/app/get_slam_status"
        ),
        app_submit_command_contract_param_ns=(
            str(
                rospy_module.get_param(
                    "~app_submit_command_contract_param_ns",
                    "/clean_robot_server/contracts/app/submit_slam_command",
                )
            ).strip()
            or "/clean_robot_server/contracts/app/submit_slam_command"
        ),
        app_get_job_contract_param_ns=(
            str(
                rospy_module.get_param(
                    "~app_get_job_contract_param_ns",
                    "/clean_robot_server/contracts/app/get_slam_job",
                )
            ).strip()
            or "/clean_robot_server/contracts/app/get_slam_job"
        ),
        map_identity_timeout_s=max(0.5, float(rospy_module.get_param("~map_identity_timeout_s", 2.0))),
        map_fresh_timeout_s=max(0.5, float(rospy_module.get_param("~map_fresh_timeout_s", 10.0))),
        tracked_pose_fresh_timeout_s=max(
            0.2,
            float(rospy_module.get_param("~tracked_pose_fresh_timeout_s", 2.0)),
        ),
        localization_degraded_timeout_s=max(
            0.5,
            float(
                rospy_module.get_param(
                    "~localization_degraded_timeout_s",
                    max(
                        6.0,
                        float(rospy_module.get_param("~tracked_pose_fresh_timeout_s", 2.0)) * 3.0,
                    ),
                )
            ),
        ),
        command_timeout_s=max(1.0, float(rospy_module.get_param("~command_timeout_s", 60.0))),
        state_publish_hz=max(1.0, float(rospy_module.get_param("~state_publish_hz", 2.0))),
        task_state_fresh_timeout_s=max(1.0, float(rospy_module.get_param("~task_state_fresh_timeout_s", 10.0))),
        odometry_state_fresh_timeout_s=max(
            1.0, float(rospy_module.get_param("~odometry_state_fresh_timeout_s", 10.0))
        ),
    )

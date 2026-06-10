#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# shellcheck disable=SC1091
source "${SCRIPT_DIR}/runtime_common.sh"

OUT_DIR="${BAG_OUTPUT_DIR:-${REPO_ROOT}/test_bag}"
PREFIX="${BAG_PREFIX:-loc_debug}"
PROFILE="${BAG_PROFILE:-analysis}"
SPLIT_DURATION="${BAG_SPLIT_DURATION:-5m}"
MAX_SPLITS="${BAG_MAX_SPLITS:-}"
BAG_SIZE_MB="${BAG_SIZE_MB:-1024}"
MIN_SPACE="${BAG_MIN_SPACE:-5G}"
BUFFSIZE_MB="${BAG_BUFFSIZE_MB:-1024}"
COMPRESSION="${BAG_COMPRESSION:-lz4}"
SPLIT_ENABLED=1
QUIET=0
DRY_RUN=0
WITH_NAV=0
WITH_DEPTH=0
WITH_HEAVY=0
WITH_CARTO_DEBUG=0
WITHOUT_MAP=0

EXTRA_TOPICS=()
EXCLUDED_TOPICS=()

usage() {
  cat <<'EOF'
Usage: record_localization_debug_bag.sh [options]

Record a ROS 1 bag for localization debugging. Defaults focus on Cartographer,
laser, odometry, IMU, TF, and runtime state topics while avoiding large depth
camera streams.

Options:
  --out DIR              Output directory. Default: $REPO_ROOT/test_bag
  --prefix NAME          Bag filename prefix. Default: loc_debug
  --profile NAME         core, analysis, or full. Default: analysis
  --duration DURATION    Split duration passed to rosbag. Default: 5m
  --max-splits N         Rolling retention: keep at most N split bags, deleting oldest.
                         Default: unlimited; no bag files are deleted by count.
  --no-max-splits        Disable rolling retention even if BAG_MAX_SPLITS is set.
  --size-mb MB           Also split when a bag reaches MB.
  --min-space SIZE       Stop before free space drops below SIZE. Default: 5G
  --buffsize MB          rosbag subscriber buffer size. Default: 1024
  --no-split             Write one bag until interrupted.
  --compression MODE     lz4, bz2, or none. Default: lz4
  --with-nav             Include MBF/costmap/controller debug topics.
  --with-carto-debug     Include large Cartographer debug topics.
  --with-depth           Include depth camera point clouds.
  --with-heavy           Include extra point clouds and docking debug clouds.
  --without-map          Do not record /map.
  --topic TOPIC          Add an extra topic. Can be repeated.
  --exclude-topic TOPIC  Remove a topic from the final set. Can be repeated.
  --quiet                Pass --quiet to rosbag record.
  --dry-run              Print the resolved command without recording.
  -h, --help             Show this help.

Examples:
  scripts/record_localization_debug_bag.sh
  scripts/record_localization_debug_bag.sh --profile full --dry-run
  scripts/record_localization_debug_bag.sh --with-nav --topic /my_debug_topic
EOF
}

die() {
  echo "[ERROR] $*" >&2
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out)
      OUT_DIR="${2:?missing value for --out}"
      shift 2
      ;;
    --prefix)
      PREFIX="${2:?missing value for --prefix}"
      shift 2
      ;;
    --profile)
      PROFILE="${2:?missing value for --profile}"
      shift 2
      ;;
    --duration)
      SPLIT_DURATION="${2:?missing value for --duration}"
      shift 2
      ;;
    --max-splits)
      MAX_SPLITS="${2:?missing value for --max-splits}"
      shift 2
      ;;
    --no-max-splits)
      MAX_SPLITS=""
      shift
      ;;
    --size-mb)
      BAG_SIZE_MB="${2:?missing value for --size-mb}"
      shift 2
      ;;
    --min-space)
      MIN_SPACE="${2:?missing value for --min-space}"
      shift 2
      ;;
    --buffsize)
      BUFFSIZE_MB="${2:?missing value for --buffsize}"
      shift 2
      ;;
    --no-split)
      SPLIT_ENABLED=0
      shift
      ;;
    --compression)
      COMPRESSION="${2:?missing value for --compression}"
      shift 2
      ;;
    --with-nav)
      WITH_NAV=1
      shift
      ;;
    --with-carto-debug)
      WITH_CARTO_DEBUG=1
      shift
      ;;
    --with-depth)
      WITH_DEPTH=1
      shift
      ;;
    --with-heavy)
      WITH_HEAVY=1
      shift
      ;;
    --without-map)
      WITHOUT_MAP=1
      shift
      ;;
    --topic)
      EXTRA_TOPICS+=("${2:?missing value for --topic}")
      shift 2
      ;;
    --exclude-topic)
      EXCLUDED_TOPICS+=("${2:?missing value for --exclude-topic}")
      shift 2
      ;;
    --quiet)
      QUIET=1
      shift
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

case "${COMPRESSION}" in
  lz4|bz2|none) ;;
  *) die "unsupported compression: ${COMPRESSION}" ;;
esac

case "${PROFILE}" in
  core|analysis|full) ;;
  *) die "unsupported profile: ${PROFILE}" ;;
esac

CORE_TOPICS=(
  /scan
  /odom
  /odom_raw
  /odom_ekf_balanced
  /odom_ekf_aggressive
  /imu
  /tf
  /tf_static
  /tracked_pose
  /cmd_vel
  /initialpose
  /set_pose
  /diagnostics
  /cartographer/localization_health
  /rosout
  /param_changes
  /cartographer/runtime/job_state
  /clean_robot_server/slam_state
  /clean_robot_server/slam_job_state
  /clean_robot_server/odometry_state
)

ANALYSIS_TOPICS=(
  /map
  /submap_list
)

CARTO_DEBUG_TOPICS=(
  /scan_matched_points2
  /trajectory_node_list
  /active_trajectory_node_list
  /submap_list
  /constraint_list
  /landmark_poses_list
  /trajectories
  /rosout_agg
)

NAV_TOPICS=(
  /current_goal
  /move_base_flex/current_goal
  /move_base_flex/global_costmap/costmap
  /move_base_flex/global_costmap/costmap_updates
  /move_base_flex/local_costmap/costmap
  /move_base_flex/local_costmap/costmap_updates
  /move_base_flex/global_costmap/footprint
  /move_base_flex/local_costmap/footprint
  /move_base_flex/MPPI_Eco_Controller/optimal_trajectory
  /move_base_flex/MPPI_Standard_Controller/optimal_trajectory
  /move_base_flex/MPPI_Heavy_Controller/optimal_trajectory
  /move_base_flex/ThetaStarPlanner/theta_star_plan
  /coverage_executor/state
  /coverage_executor/run_progress
  /coverage_executor/debug/robot_pose
  /coverage_executor/debug/path
)

DEPTH_TOPICS=(
  /gemini_cf/depth/points
  /gemini_nj/depth/points
  /left/obstacle_2d
  /right/obstacle_2d
)

HEAVY_TOPICS=(
  /cloud
  /dock_debug_cloud
  /dock_roi_cloud
  /dock_pose
)

runtime_common_init
ROS_MASTER_READY=0
if runtime_ros_master_available; then
  ROS_MASTER_READY=1
elif [[ "${DRY_RUN}" == "1" ]]; then
  echo "[WARN] ROS master is not available; skipping live metadata in dry-run." >&2
else
  runtime_wait_for_master 15
  ROS_MASTER_READY=1
fi

mkdir -p "${OUT_DIR}"

RUN_ID="${PREFIX}_$(date '+%Y%m%d_%H%M%S')"
OUT_PREFIX="${OUT_DIR%/}/${RUN_ID}"
META_DIR="${OUT_PREFIX}_metadata"
mkdir -p "${META_DIR}"

TOPICS=("${CORE_TOPICS[@]}")
if [[ "${PROFILE}" == "analysis" || "${PROFILE}" == "full" ]]; then
  TOPICS+=("${ANALYSIS_TOPICS[@]}")
fi
if [[ "${PROFILE}" == "full" || "${WITH_CARTO_DEBUG}" == "1" ]]; then
  TOPICS+=("${CARTO_DEBUG_TOPICS[@]}")
fi
if [[ "${WITH_NAV}" == "1" ]]; then
  TOPICS+=("${NAV_TOPICS[@]}")
fi
if [[ "${WITH_DEPTH}" == "1" ]]; then
  TOPICS+=("${DEPTH_TOPICS[@]}")
fi
if [[ "${WITH_HEAVY}" == "1" ]]; then
  TOPICS+=("${HEAVY_TOPICS[@]}")
fi
TOPICS+=("${EXTRA_TOPICS[@]}")
if [[ "${WITHOUT_MAP}" == "1" ]]; then
  EXCLUDED_TOPICS+=("/map")
fi

DEDUPED_TOPICS=()
declare -A SEEN_TOPICS=()
declare -A EXCLUDED_TOPIC_SET=()
for topic in "${EXCLUDED_TOPICS[@]}"; do
  [[ -n "${topic}" ]] || continue
  EXCLUDED_TOPIC_SET["${topic}"]=1
done
for topic in "${TOPICS[@]}"; do
  [[ -n "${topic}" ]] || continue
  if [[ -n "${EXCLUDED_TOPIC_SET[${topic}]:-}" ]]; then
    continue
  fi
  if [[ -z "${SEEN_TOPICS[${topic}]:-}" ]]; then
    DEDUPED_TOPICS+=("${topic}")
    SEEN_TOPICS["${topic}"]=1
  fi
done

if [[ "${ROS_MASTER_READY}" == "1" ]]; then
  rostopic list | sort > "${META_DIR}/topics.txt"
  rosnode list | sort > "${META_DIR}/nodes.txt"
  rosparam dump "${META_DIR}/params.yaml" >/dev/null
else
  : > "${META_DIR}/topics.txt"
  : > "${META_DIR}/nodes.txt"
  printf '{}\n' > "${META_DIR}/params.yaml"
fi
{
  echo "run_id: ${RUN_ID}"
  echo "start_time: $(date --iso-8601=seconds)"
  echo "repo_root: ${REPO_ROOT}"
  echo "ros_master_uri: ${ROS_MASTER_URI:-}"
  echo "workspace_setup: ${DORAEMON_WORKSPACE_SETUP:-}"
  echo "ros_master_ready: ${ROS_MASTER_READY}"
  echo "profile: ${PROFILE}"
  echo "compression: ${COMPRESSION}"
  echo "split_enabled: ${SPLIT_ENABLED}"
  echo "split_duration: ${SPLIT_DURATION}"
  echo "split_size_mb: ${BAG_SIZE_MB:-}"
  echo "max_splits: ${MAX_SPLITS:-unlimited}"
  echo "min_space: ${MIN_SPACE}"
  echo "buffsize_mb: ${BUFFSIZE_MB}"
  echo "with_carto_debug: ${WITH_CARTO_DEBUG}"
  echo "without_map: ${WITHOUT_MAP}"
} > "${META_DIR}/recording_context.txt"
printf '%s\n' "${DEDUPED_TOPICS[@]}" > "${META_DIR}/requested_topics.txt"
printf '%s\n' "${EXCLUDED_TOPICS[@]}" > "${META_DIR}/excluded_topics.txt"

if [[ "${ROS_MASTER_READY}" == "1" ]]; then
  {
    while IFS= read -r topic; do
      if ! grep -Fxq "${topic}" "${META_DIR}/topics.txt"; then
        echo "${topic}"
      fi
    done < "${META_DIR}/requested_topics.txt"
  } > "${META_DIR}/missing_topics_at_start.txt"
else
  : > "${META_DIR}/missing_topics_at_start.txt"
fi

CMD=(rosbag record --repeat-latched --buffsize "${BUFFSIZE_MB}" --min-space "${MIN_SPACE}")
case "${COMPRESSION}" in
  lz4) CMD+=(--lz4) ;;
  bz2) CMD+=(--bz2) ;;
  none) ;;
esac
if [[ "${QUIET}" == "1" ]]; then
  CMD+=(--quiet)
fi
if [[ "${SPLIT_ENABLED}" == "1" ]]; then
  CMD+=(--split --duration "${SPLIT_DURATION}")
  if [[ -n "${MAX_SPLITS}" ]]; then
    CMD+=(--max-splits "${MAX_SPLITS}")
  fi
  if [[ -n "${BAG_SIZE_MB:-}" ]]; then
    CMD+=(--size "${BAG_SIZE_MB}")
  fi
fi
CMD+=(--output-prefix "${OUT_PREFIX}")
CMD+=("${DEDUPED_TOPICS[@]}")

echo "[INFO] Metadata: ${META_DIR}"
echo "[INFO] Bag prefix: ${OUT_PREFIX}"
echo "[INFO] Requested topics: ${#DEDUPED_TOPICS[@]}"
if [[ -s "${META_DIR}/missing_topics_at_start.txt" ]]; then
  echo "[WARN] Some requested topics are not currently advertised; rosbag will wait for them:"
  sed 's/^/[WARN]   /' "${META_DIR}/missing_topics_at_start.txt"
fi
printf '[INFO] Command:'
printf ' %q' "${CMD[@]}"
printf '\n'

if [[ "${DRY_RUN}" == "1" ]]; then
  exit 0
fi

echo "[INFO] Recording. Press Ctrl-C to stop."
exec "${CMD[@]}"

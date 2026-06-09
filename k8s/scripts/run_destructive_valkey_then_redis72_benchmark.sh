#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

RUN_ID="${RUN_ID:-$(date +%Y%m%d-%H%M%S)}"
OUT_ROOT="${1:-./results/memtier-valkey-redis72-${RUN_ID}}"

BENCHMARK_RUNS="${BENCHMARK_RUNS:-${N:-5}}"
MEMTIER_CPUS="${MEMTIER_CPUS:-1 2}"
MEMTIER_TEST_TIME="${MEMTIER_TEST_TIME:-120}"
IMAGE_PULL_POLICY="${IMAGE_PULL_POLICY:-IfNotPresent}"
POD_WAIT_TIMEOUT="${POD_WAIT_TIMEOUT:-43200}"
POD_HOLD_SECONDS="${POD_HOLD_SECONDS:-3600}"
ROLLOUT_TIMEOUT="${ROLLOUT_TIMEOUT:-900s}"
CLUSTER_HEALTH_TIMEOUT="${CLUSTER_HEALTH_TIMEOUT:-600}"

VALKEY_NS="${VALKEY_NS:-${NS:-vk}}"
VALKEY_RELEASE="${VALKEY_RELEASE:-${RELEASE:-valkey}}"
VALKEY_STS="${VALKEY_STS:-${VALKEY_RELEASE}}"
VALKEY_CONTAINER="${VALKEY_CONTAINER:-valkey}"
VALKEY_CHART_PATH="${VALKEY_CHART_PATH:-${HELM_CHART_PATH:-../valkey-helm/valkey}}"
VALKEY_VALUES_FILE="${VALKEY_VALUES_FILE:-./k8s/manifests/values.yaml}"
VALKEY_HOST="${VALKEY_HOST:-${VALKEY_RELEASE}.${VALKEY_NS}.svc.cluster.local}"
VALKEY_PORT="${VALKEY_PORT:-6379}"
FRESH_VALKEY="${FRESH_VALKEY:-false}"
DELETE_VALKEY_PVCS_AFTER_UNINSTALL="${DELETE_VALKEY_PVCS_AFTER_UNINSTALL:-false}"

REDIS_NS="${REDIS_NS:-${VALKEY_NS}}"
REDIS_RELEASE="${REDIS_RELEASE:-redis72}"
REDIS_STS="${REDIS_STS:-${REDIS_RELEASE}-redis-cluster}"
REDIS_CONTAINER="${REDIS_CONTAINER:-${REDIS_STS}}"
REDIS_CHART_PATH="${REDIS_CHART_PATH:-oci://registry-1.docker.io/bitnamicharts/redis-cluster}"
REDIS_VALUES_FILE="${REDIS_VALUES_FILE:-./k8s/manifests/values-redis72.yaml}"
REDIS_HOST="${REDIS_HOST:-${REDIS_STS}.${REDIS_NS}.svc.cluster.local}"
REDIS_PORT="${REDIS_PORT:-6379}"
DELETE_REDIS_PVCS_BEFORE_INSTALL="${DELETE_REDIS_PVCS_BEFORE_INSTALL:-true}"

ALLOW_EXISTING_OUTPUT="${ALLOW_EXISTING_OUTPUT:-false}"
RUN_ANALYSIS="${RUN_ANALYSIS:-false}"
PROMETHEUS_URL="${PROMETHEUS_URL:-http://localhost:9090}"

VALKEY_SELECTOR="app.kubernetes.io/instance=${VALKEY_RELEASE},app.kubernetes.io/name=valkey"
REDIS_SELECTOR="app.kubernetes.io/instance=${REDIS_RELEASE},app.kubernetes.io/name=redis-cluster"

bool_true() {
  case "${1:-}" in
    1|true|TRUE|yes|YES|on|ON)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

ensure_output_dir() {
  if [ -e "${OUT_ROOT}" ] && [ "$(find "${OUT_ROOT}" -mindepth 1 -maxdepth 1 2>/dev/null | wc -l | tr -d '[:space:]')" != "0" ]; then
    if ! bool_true "${ALLOW_EXISTING_OUTPUT}"; then
      echo "ERROR: output directory already exists and is not empty: ${OUT_ROOT}" >&2
      echo "Set ALLOW_EXISTING_OUTPUT=true to reuse it, or pass a new output directory." >&2
      exit 1
    fi
  fi

  mkdir -p "${OUT_ROOT}"
}

ensure_namespace() {
  local namespace="$1"
  kubectl create namespace "${namespace}" --dry-run=client -o yaml | kubectl apply -f -
}

wait_for_selector_absent() {
  local namespace="$1"
  local selector="$2"
  local timeout_s="${3:-300}"
  local deadline=$((SECONDS + timeout_s))

  while (( SECONDS < deadline )); do
    if ! kubectl get pods -n "${namespace}" -l "${selector}" -o name 2>/dev/null | grep -q .; then
      return 0
    fi
    sleep 3
  done

  echo "ERROR: pods still exist after ${timeout_s}s for selector ${selector} in namespace ${namespace}" >&2
  kubectl get pods -n "${namespace}" -l "${selector}" -o wide || true
  return 1
}

wait_cluster_healthy() {
  local label="$1"
  local namespace="$2"
  local admin_pod="$3"
  local container="$4"
  local cli_bin="$5"
  local min_masters="$6"
  local timeout_s="${7:-600}"
  local elapsed=0

  echo "==> Waiting for ${label} cluster health (max ${timeout_s}s)..."
  while [ "${elapsed}" -lt "${timeout_s}" ]; do
    local info nodes state slots_ok masters
    info="$(kubectl exec "${admin_pod}" -n "${namespace}" -c "${container}" -- \
      "${cli_bin}" cluster info 2>/dev/null || true)"
    nodes="$(kubectl exec "${admin_pod}" -n "${namespace}" -c "${container}" -- \
      "${cli_bin}" cluster nodes 2>/dev/null || true)"
    state="$(printf '%s\n' "${info}" | awk -F: '$1 == "cluster_state" {gsub(/\r/, "", $2); print $2; exit}')"
    slots_ok="$(printf '%s\n' "${info}" | awk -F: '$1 == "cluster_slots_ok" {gsub(/\r/, "", $2); print $2; exit}')"
    masters="$(printf '%s\n' "${nodes}" | awk '$3 ~ /master/ && $3 !~ /fail/ {count++} END {print count + 0}')"

    if [ "${state}" = "ok" ] && [ "${slots_ok}" = "16384" ] && [ "${masters:-0}" -ge "${min_masters}" ]; then
      echo "==> ${label} cluster healthy after ${elapsed}s (${masters} masters)."
      return 0
    fi

    sleep 5
    elapsed=$((elapsed + 5))
  done

  echo "ERROR: ${label} cluster did not become healthy after ${timeout_s}s" >&2
  kubectl get pods -n "${namespace}" -o wide || true
  kubectl exec "${admin_pod}" -n "${namespace}" -c "${container}" -- "${cli_bin}" cluster info || true
  kubectl exec "${admin_pod}" -n "${namespace}" -c "${container}" -- "${cli_bin}" cluster nodes || true
  return 1
}

delete_pvcs_for_selector() {
  local namespace="$1"
  local selector="$2"
  local label="$3"

  echo "==> Deleting ${label} PVCs in namespace ${namespace} with selector: ${selector}"
  kubectl delete pvc -n "${namespace}" -l "${selector}" --ignore-not-found --wait=true || true
}

uninstall_release() {
  local release="$1"
  local namespace="$2"
  local selector="$3"
  local label="$4"

  echo "==> Uninstalling ${label} Helm release ${release} from namespace ${namespace}..."
  helm uninstall "${release}" -n "${namespace}" --ignore-not-found || true
  wait_for_selector_absent "${namespace}" "${selector}" 600
}

install_valkey() {
  ensure_namespace "${VALKEY_NS}"

  if bool_true "${FRESH_VALKEY}"; then
    uninstall_release "${VALKEY_RELEASE}" "${VALKEY_NS}" "${VALKEY_SELECTOR}" "Valkey"
    delete_pvcs_for_selector "${VALKEY_NS}" "${VALKEY_SELECTOR}" "Valkey"
  fi

  echo "==> Installing/upgrading Valkey..."
  helm upgrade --install "${VALKEY_RELEASE}" "${VALKEY_CHART_PATH}" \
    -n "${VALKEY_NS}" \
    -f "${VALKEY_VALUES_FILE}" \
    --create-namespace
  kubectl rollout status "sts/${VALKEY_STS}" -n "${VALKEY_NS}" --timeout="${ROLLOUT_TIMEOUT}"
  wait_cluster_healthy "Valkey" "${VALKEY_NS}" "${VALKEY_STS}-0" "${VALKEY_CONTAINER}" valkey-cli 3 "${CLUSTER_HEALTH_TIMEOUT}"
}

install_redis72() {
  ensure_namespace "${REDIS_NS}"

  uninstall_release "${REDIS_RELEASE}" "${REDIS_NS}" "${REDIS_SELECTOR}" "Redis 7.2"
  if bool_true "${DELETE_REDIS_PVCS_BEFORE_INSTALL}"; then
    delete_pvcs_for_selector "${REDIS_NS}" "${REDIS_SELECTOR}" "Redis 7.2"
  fi

  echo "==> Installing Redis 7.2..."
  helm upgrade --install "${REDIS_RELEASE}" "${REDIS_CHART_PATH}" \
    -n "${REDIS_NS}" \
    -f "${REDIS_VALUES_FILE}" \
    --create-namespace
  kubectl rollout status "sts/${REDIS_STS}" -n "${REDIS_NS}" --timeout="${ROLLOUT_TIMEOUT}"
  wait_cluster_healthy "Redis 7.2" "${REDIS_NS}" "${REDIS_STS}-0" "${REDIS_CONTAINER}" redis-cli 3 "${CLUSTER_HEALTH_TIMEOUT}"
}

benchmark_env_common() {
  local -n env_ref="$1"

  env_ref+=(
    "N=${BENCHMARK_RUNS}"
    "MEMTIER_CPUS=${MEMTIER_CPUS}"
    "MEMTIER_TEST_TIME=${MEMTIER_TEST_TIME}"
    "IMAGE_PULL_POLICY=${IMAGE_PULL_POLICY}"
    "POD_WAIT_TIMEOUT=${POD_WAIT_TIMEOUT}"
    "POD_HOLD_SECONDS=${POD_HOLD_SECONDS}"
    "ROLLOUT_TIMEOUT=${ROLLOUT_TIMEOUT}"
  )

  local var
  for var in \
    MEMTIER_IMAGE \
    MEMTIER_PAYLOADS \
    MEMTIER_RATIOS \
    MEMTIER_THREADS \
    MEMTIER_CLIENTS \
    MEMTIER_PIPELINE \
    MEMTIER_KEYS \
    MEMTIER_TARGET_DATASET_MB \
    MEMTIER_VALUE_OVERHEAD_BYTES \
    MEMTIER_RANDOM_DATA \
    RESET_BETWEEN_RUNS \
    RESET_COMMAND_TIMEOUT \
    WARMUP_BEFORE_RUNS \
    MEMTIER_WARMUP_THREADS \
    MEMTIER_WARMUP_CLIENTS \
    MEMTIER_WARMUP_PIPELINE \
    MEMTIER_WARMUP_RATIO \
    MEMTIER_WARMUP_KEY_PATTERN
  do
    if [ -n "${!var:-}" ]; then
      env_ref+=("${var}=${!var}")
    fi
  done

  if [ "${MEMTIER_STATSD_HOST+x}" ]; then
    env_ref+=("MEMTIER_STATSD_HOST=${MEMTIER_STATSD_HOST}")
  fi
  if [ -n "${MEMTIER_STATSD_PORT:-}" ]; then
    env_ref+=("MEMTIER_STATSD_PORT=${MEMTIER_STATSD_PORT}")
  fi
}

run_valkey_benchmark() {
  local env_args=(
    "PROVIDER=valkey"
    "VARIANT=valkey"
    "BENCHMARKED_SYSTEM=Valkey Cluster in Kubernetes"
    "NS=${VALKEY_NS}"
    "RELEASE=${VALKEY_RELEASE}"
    "STS=${VALKEY_STS}"
    "CONTAINER=${VALKEY_CONTAINER}"
    "HOST=${VALKEY_HOST}"
    "PORT=${VALKEY_PORT}"
    "HELM_CHART_PATH=${VALKEY_CHART_PATH}"
    "VALUES_FILE=${VALKEY_VALUES_FILE}"
  )
  benchmark_env_common env_args

  echo "==> Running Valkey memtier benchmark..."
  env "${env_args[@]}" "${SCRIPT_DIR}/run_benchmark.sh" "${OUT_ROOT}/valkey"
}

run_redis72_benchmark() {
  local env_args=(
    "PROVIDER=redis72"
    "VARIANT=redis72"
    "BENCHMARKED_SYSTEM=Redis 7.2 Cluster in Kubernetes"
    "NS=${REDIS_NS}"
    "RELEASE=${REDIS_RELEASE}"
    "STS=${REDIS_STS}"
    "CONTAINER=${REDIS_CONTAINER}"
    "HOST=${REDIS_HOST}"
    "PORT=${REDIS_PORT}"
  )
  benchmark_env_common env_args

  echo "==> Running Redis 7.2 memtier benchmark..."
  env "${env_args[@]}" "${SCRIPT_DIR}/run_benchmark.sh" "${OUT_ROOT}/redis72"
}

run_analysis() {
  local plots_root="${PLOTS_ROOT:-./plots/benchmark/$(basename "${OUT_ROOT}")}"

  echo "==> Running benchmark analysis..."
  python cli.py benchmark \
    --input "${OUT_ROOT}/valkey" \
    --output-dir "${plots_root}/valkey" \
    --prometheus-url "${PROMETHEUS_URL}"
  python cli.py benchmark \
    --input "${OUT_ROOT}/redis72" \
    --output-dir "${plots_root}/redis72" \
    --prometheus-url "${PROMETHEUS_URL}"
}

write_summary() {
  local finished_ts="$1"
  local summary_file="${OUT_ROOT}/orchestration_summary.json"

  python3 - "${summary_file}" <<PY
import json
import os
import time

summary = {
    "scenario": "destructive_valkey_then_redis72_memtier",
    "run_id": os.environ["RUN_ID"],
    "started_at_epoch_s": int(os.environ["START_TS"]),
    "finished_at_epoch_s": int(${finished_ts}),
    "total_duration_s": int(${finished_ts}) - int(os.environ["START_TS"]),
    "benchmark_runs": int(os.environ["BENCHMARK_RUNS"]),
    "memtier_cpus": os.environ["MEMTIER_CPUS"],
    "memtier_test_time_s": int(os.environ["MEMTIER_TEST_TIME"]),
    "output_root": os.environ["OUT_ROOT"],
    "valkey": {
        "namespace": os.environ["VALKEY_NS"],
        "release": os.environ["VALKEY_RELEASE"],
        "statefulset": os.environ["VALKEY_STS"],
        "values_file": os.environ["VALKEY_VALUES_FILE"],
        "results": os.path.join(os.environ["OUT_ROOT"], "valkey"),
    },
    "redis72": {
        "namespace": os.environ["REDIS_NS"],
        "release": os.environ["REDIS_RELEASE"],
        "statefulset": os.environ["REDIS_STS"],
        "values_file": os.environ["REDIS_VALUES_FILE"],
        "results": os.path.join(os.environ["OUT_ROOT"], "redis72"),
    },
}

with open(summary["output_root"] + "/orchestration_summary.json", "w", encoding="utf-8") as fh:
    json.dump(summary, fh, indent=2)
    fh.write("\\n")
PY

  echo "==> Wrote ${summary_file}"
}

print_config() {
  cat <<EOF
==> DESTRUCTIVE Valkey -> Redis 7.2 benchmark configuration
This script benchmarks Valkey, uninstalls Valkey, installs Redis 7.2, and benchmarks Redis.

OUT_ROOT=${OUT_ROOT}
BENCHMARK_RUNS=${BENCHMARK_RUNS}
MEMTIER_CPUS=${MEMTIER_CPUS}
MEMTIER_TEST_TIME=${MEMTIER_TEST_TIME}
MEMTIER_IMAGE=${MEMTIER_IMAGE:-<run_benchmark default>}
IMAGE_PULL_POLICY=${IMAGE_PULL_POLICY}
POD_WAIT_TIMEOUT=${POD_WAIT_TIMEOUT}

Valkey:
  namespace=${VALKEY_NS}
  release=${VALKEY_RELEASE}
  statefulset=${VALKEY_STS}
  chart=${VALKEY_CHART_PATH}
  values=${VALKEY_VALUES_FILE}
  host=${VALKEY_HOST}:${VALKEY_PORT}
  fresh_valkey=${FRESH_VALKEY}
  delete_pvcs_after_uninstall=${DELETE_VALKEY_PVCS_AFTER_UNINSTALL}

Redis 7.2:
  namespace=${REDIS_NS}
  release=${REDIS_RELEASE}
  statefulset=${REDIS_STS}
  chart=${REDIS_CHART_PATH}
  values=${REDIS_VALUES_FILE}
  host=${REDIS_HOST}:${REDIS_PORT}
  delete_pvcs_before_install=${DELETE_REDIS_PVCS_BEFORE_INSTALL}
EOF
}

cd "${REPO_ROOT}"
START_TS="$(date +%s)"
export RUN_ID OUT_ROOT START_TS BENCHMARK_RUNS MEMTIER_CPUS MEMTIER_TEST_TIME
export VALKEY_NS VALKEY_RELEASE VALKEY_STS VALKEY_VALUES_FILE
export REDIS_NS REDIS_RELEASE REDIS_STS REDIS_VALUES_FILE

ensure_output_dir
print_config

install_valkey
run_valkey_benchmark

uninstall_release "${VALKEY_RELEASE}" "${VALKEY_NS}" "${VALKEY_SELECTOR}" "Valkey"
if bool_true "${DELETE_VALKEY_PVCS_AFTER_UNINSTALL}"; then
  delete_pvcs_for_selector "${VALKEY_NS}" "${VALKEY_SELECTOR}" "Valkey"
fi

install_redis72
run_redis72_benchmark

FINISHED_TS="$(date +%s)"
write_summary "${FINISHED_TS}"

if bool_true "${RUN_ANALYSIS}"; then
  run_analysis
fi

cat <<EOF
==> Done.
Valkey results: ${OUT_ROOT}/valkey
Redis 7.2 results: ${OUT_ROOT}/redis72
Summary: ${OUT_ROOT}/orchestration_summary.json
EOF

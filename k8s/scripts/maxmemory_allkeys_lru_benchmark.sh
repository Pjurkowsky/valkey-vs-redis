#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<EOF
Usage: $0 [target-mb|auto] [output-dir]

Uses the backup/restore seed helper to quickly load a self-hosted
Redis-compatible cluster under maxmemory-policy=allkeys-lru, then runs memtier
for TEST_TIME seconds on the loaded cluster.

Environment:
  PROVIDER=valkey|redis72            default: valkey
  NS=vk|redis                        namespace override
  RELEASE=valkey|redis72             Helm release override
  HOST=<cluster service DNS>          endpoint override
  PORT=6379
  N=1
  TEST_TIME=30
  SEED_ALLOW_PARTIAL=true           keep seed report even when maxmemory rejects writes
  STOP_AFTER_ERRORS=50
  RANDOM_DATA=false
  TTL_SECONDS=0
  MAXMEMORY_POLICY=allkeys-lru
  CONFIGURE_POLICY=true
  CONFIGURE_MAXMEMORY=               optional CONFIG SET maxmemory value
  FLUSH_BETWEEN_RUNS=true
  FLUSH_AFTER_RUNS=false
  MEMTIER_IMAGE=.../memtier_k8s:1
  BACKUP_IMAGE=.../backup_restore:1

Examples:
  $0 auto ./results/valkey_maxmemory_allkeys_lru_n5
  PROVIDER=redis72 N=5 $0 auto ./results/redis72_maxmemory_allkeys_lru_n5
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

TARGET_MB="${1:-auto}"
LOCAL_OUT="${2:-./results/maxmemory_allkeys_lru}"
N="${N:-1}"
PROVIDER="${PROVIDER:-valkey}"

case "${PROVIDER}" in
  valkey)
    DEFAULT_NS="vk"
    DEFAULT_RELEASE="valkey"
    DEFAULT_CLI_BIN="valkey-cli"
    DEFAULT_SYSTEM_NAME="Valkey"
    ;;
  redis|redis72)
    PROVIDER="redis72"
    DEFAULT_NS="redis"
    DEFAULT_RELEASE="redis72"
    DEFAULT_CLI_BIN="redis-cli"
    DEFAULT_SYSTEM_NAME="Redis 7.2"
    ;;
  *)
    echo "ERROR: PROVIDER must be valkey or redis72." >&2
    exit 1
    ;;
esac

NS="${NS:-${DEFAULT_NS}}"
RELEASE="${RELEASE:-${DEFAULT_RELEASE}}"
case "${PROVIDER}" in
  valkey)
    DEFAULT_STS="${RELEASE}"
    DEFAULT_SERVICE_NAME="${RELEASE}"
    DEFAULT_POD_SELECTOR="app.kubernetes.io/name=valkey,app.kubernetes.io/instance=${RELEASE}"
    ;;
  redis72)
    DEFAULT_STS="${RELEASE}-redis-cluster"
    DEFAULT_SERVICE_NAME="${DEFAULT_STS}"
    DEFAULT_POD_SELECTOR="app.kubernetes.io/name=redis-cluster,app.kubernetes.io/instance=${RELEASE}"
    ;;
esac

STS="${STS:-${DEFAULT_STS}}"
CLI_POD="${CLI_POD:-${STS}-0}"
CLI_BIN="${CLI_BIN:-${DEFAULT_CLI_BIN}}"
SYSTEM_NAME="${SYSTEM_NAME:-${DEFAULT_SYSTEM_NAME}}"
SERVICE_NAME="${SERVICE_NAME:-${DEFAULT_SERVICE_NAME}}"
POD_SELECTOR="${POD_SELECTOR:-${DEFAULT_POD_SELECTOR}}"
HOST="${HOST:-${SERVICE_NAME}.${NS}.svc.cluster.local}"
PORT="${PORT:-6379}"

LOCATION="${LOCATION:-europe-central2}"
PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null || true)}"
ARTIFACT_REPO="${ARTIFACT_REPO:-valkey-bench}"
if [[ -n "${MEMTIER_IMAGE:-}" ]]; then
  MEMTIER_IMAGE="${MEMTIER_IMAGE}"
elif [[ -n "${PROJECT_ID}" && "${PROJECT_ID}" != "(unset)" ]]; then
  MEMTIER_IMAGE="${LOCATION}-docker.pkg.dev/${PROJECT_ID}/${ARTIFACT_REPO}/memtier_k8s:1"
else
  MEMTIER_IMAGE="memtier_k8s:1"
fi
if [[ -n "${BACKUP_IMAGE:-}" ]]; then
  BACKUP_IMAGE="${BACKUP_IMAGE}"
elif [[ -n "${PROJECT_ID}" && "${PROJECT_ID}" != "(unset)" ]]; then
  BACKUP_IMAGE="${LOCATION}-docker.pkg.dev/${PROJECT_ID}/${ARTIFACT_REPO}/backup_restore:1"
else
  BACKUP_IMAGE="backup_restore:1"
fi

REMOTE_OUT="/work/results/maxmemory_allkeys_lru"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

source "${SCRIPT_DIR}/pod_results.sh"

MAXMEMORY_POLICY="${MAXMEMORY_POLICY:-allkeys-lru}"
CONFIGURE_POLICY="${CONFIGURE_POLICY:-true}"
CONFIGURE_MAXMEMORY="${CONFIGURE_MAXMEMORY:-${VALKEY_MAXMEMORY:-}}"
SEED_ALLOW_PARTIAL="${SEED_ALLOW_PARTIAL:-true}"
TTL_SECONDS="${TTL_SECONDS:-0}"
FLUSH_BETWEEN_RUNS="${FLUSH_BETWEEN_RUNS:-true}"
FLUSH_AFTER_RUNS="${FLUSH_AFTER_RUNS:-false}"
OVERWRITE_RESULTS="${OVERWRITE_RESULTS:-false}"
RANDOM_DATA="${RANDOM_DATA:-false}"
STOP_AFTER_ERRORS="${STOP_AFTER_ERRORS:-50}"
THREADS="${THREADS:-4}"
CLIENTS="${CLIENTS:-16}"
TEST_TIME="${TEST_TIME:-30}"
KEYS="${KEYS:-100000}"
DATA_SIZE="${DATA_SIZE:-1024}"
RATIO="${RATIO:-1:1}"
VARIANT="${VARIANT:-${PROVIDER}_maxmemory_allkeys_lru}"

mkdir -p "${LOCAL_OUT}"

PYTHON_BIN="$(command -v python3 || command -v python || true)"
if [[ -z "${PYTHON_BIN}" ]]; then
  echo "ERROR: python3 or python is required." >&2
  exit 1
fi

json_string() {
  "${PYTHON_BIN}" -c 'import json, sys; print(json.dumps(sys.argv[1]))' "$1"
}

wait_for_command_pod() {
  local pod_name="$1"
  local timeout_s="$2"

  if ! wait_for_pod_marker "${NS}" "${pod_name}" "${POD_DONE_FILE}" "${timeout_s}"; then
    echo "ERROR: pod ${pod_name} did not signal completion." >&2
    print_pod_debug_info "${NS}" "${pod_name}"
    return 1
  fi

  local exit_code
  exit_code="$(read_pod_exit_code "${NS}" "${pod_name}" "${POD_EXIT_CODE_FILE}")"
  if [[ -z "${exit_code}" || "${exit_code}" != "0" ]]; then
    echo "ERROR: pod ${pod_name} exited with code ${exit_code:-unknown}." >&2
    print_pod_debug_info "${NS}" "${pod_name}"
    return 1
  fi
}

cluster_nodes() {
  kubectl exec "${CLI_POD}" -n "${NS}" -- \
    "${CLI_BIN}" cluster nodes 2>/dev/null
}

wait_for_cluster_health() {
  local timeout_s="${1:-120}"
  local start elapsed info state slots masters
  start="$(date +%s)"

  while true; do
    info="$(kubectl exec "${CLI_POD}" -n "${NS}" -- \
      "${CLI_BIN}" cluster info 2>/dev/null || true)"
    state="$(awk -F: '$1=="cluster_state" {gsub(/\r/, "", $2); print $2}' <<<"${info}")"
    slots="$(awk -F: '$1=="cluster_slots_ok" {gsub(/\r/, "", $2); print $2}' <<<"${info}")"
    masters="$(cluster_nodes | awk '$3 ~ /master/ && $3 !~ /fail/ {count++} END {print count + 0}' || true)"

    if [[ "${state}" == "ok" && "${slots}" == "16384" && "${masters:-0}" -ge 3 ]]; then
      return 0
    fi

    elapsed="$(( $(date +%s) - start ))"
    if (( elapsed >= timeout_s )); then
      echo "ERROR: ${SYSTEM_NAME} cluster did not become healthy after ${timeout_s}s." >&2
      echo "cluster_state=${state:-unknown} cluster_slots_ok=${slots:-unknown} masters=${masters:-unknown}" >&2
      return 1
    fi
    sleep 5
  done
}

tool_shell_prefix() {
  cat <<EOF
mkdir -p '${REMOTE_OUT}'
EOF
}

run_tool_pod() {
  local pod_name="$1"
  local timeout_s="$2"
  local shell_body="$3"

  kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl run "${pod_name}" -n "${NS}" \
    --image="${BACKUP_IMAGE}" \
    --image-pull-policy=Always \
    --restart=Never \
    --command -- \
    /bin/sh -c "${shell_body}"

  wait_for_command_pod "${pod_name}" "${timeout_s}"
}

configure_policy() {
  local pod_name="$1"
  local maxmemory_arg=""

  if [[ -n "${CONFIGURE_MAXMEMORY}" ]]; then
    maxmemory_arg="--maxmemory '${CONFIGURE_MAXMEMORY}'"
  fi

  run_tool_pod "${pod_name}" 300 "$(tool_shell_prefix)
python /work/backup_restore_seed.py \
  --mode configure \
  --host '${HOST}' --port '${PORT}' \
  --maxmemory-policy '${MAXMEMORY_POLICY}' \
  ${maxmemory_arg}
status=\$?
echo \"\$status\" > '${POD_EXIT_CODE_FILE}'
touch '${POD_DONE_FILE}'
sleep '${POD_HOLD_SECONDS}'
"
  kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found >/dev/null
}

flush_cluster() {
  local pod_name="$1"

  run_tool_pod "${pod_name}" 600 "$(tool_shell_prefix)
python /work/backup_restore_seed.py \
  --mode flush \
  --host '${HOST}' --port '${PORT}' \
  --reset-stats
status=\$?
echo \"\$status\" > '${POD_EXIT_CODE_FILE}'
touch '${POD_DONE_FILE}'
sleep '${POD_HOLD_SECONDS}'
"
  kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found >/dev/null
}

snapshot_cluster() {
  local pod_name="$1"
  local phase="$2"
  local local_file="$3"
  local remote_file="${REMOTE_OUT}/$(basename "${local_file}")"

  run_tool_pod "${pod_name}" 300 "$(tool_shell_prefix)
python /work/backup_restore_seed.py \
  --mode snapshot \
  --host '${HOST}' --port '${PORT}' \
  --phase '${phase}' \
  --output '${remote_file}'
status=\$?
echo \"\$status\" > '${POD_EXIT_CODE_FILE}'
touch '${POD_DONE_FILE}'
sleep '${POD_HOLD_SECONDS}'
"
  kubectl cp "${NS}/${pod_name}:${remote_file}" "${local_file}"
  kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found >/dev/null
}

resolve_maxmemory_mb() {
  local snapshot_file="$1"

  "${PYTHON_BIN}" - "${snapshot_file}" <<'PY'
import json
import math
import sys

with open(sys.argv[1]) as fh:
    doc = json.load(fh)
maxmemory_total = sum(int(master.get("maxmemory", 0)) for master in doc.get("masters", []))
if maxmemory_total <= 0:
    raise SystemExit(
        "ERROR: non-zero maxmemory is required to compute the before-100% checkpoint."
    )
print(max(1, math.ceil(maxmemory_total / 1024 / 1024)))
PY
}

seed_fill() {
  local pod_name="$1"
  local report_file="$2"
  local target_mb="$3"
  local run_id="$4"
  local remote_file="${REMOTE_OUT}/${report_file}"
  local allow_partial_arg=""
  local random_data_arg=""

  if [[ "${SEED_ALLOW_PARTIAL}" == "true" ]]; then
    allow_partial_arg="--allow-partial"
  fi
  if [[ "${RANDOM_DATA}" == "true" ]]; then
    random_data_arg="--random-data"
  fi

  run_tool_pod "${pod_name}" 7200 "$(tool_shell_prefix)
python /work/backup_restore_seed.py \
  --mode seed \
  --host '${HOST}' --port '${PORT}' \
  --target-mb '${target_mb}' \
  --run-id '${run_id}' \
  --ttl-seconds '${TTL_SECONDS}' \
  --stop-after-errors '${STOP_AFTER_ERRORS}' \
  ${allow_partial_arg} \
  ${random_data_arg} \
  --output '${remote_file}'
status=\$?
echo \"\$status\" > '${POD_EXIT_CODE_FILE}'
touch '${POD_DONE_FILE}'
sleep '${POD_HOLD_SECONDS}'
"
  kubectl cp "${NS}/${pod_name}:${remote_file}" "${LOCAL_OUT}/${report_file}"
  kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found >/dev/null
}

run_memtier() {
  local pod_name="$1"
  local out_file="$2"
  local log_file="$3"

  kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl run "${pod_name}" -n "${NS}" \
    --image="${MEMTIER_IMAGE}" \
    --image-pull-policy=IfNotPresent \
    --restart=Never \
    --command -- \
    /bin/sh -c "
      mkdir -p '${REMOTE_OUT}'
      LOG_PIPE='${REMOTE_OUT}/${log_file}.pipe'
      rm -f '${REMOTE_OUT}/${log_file}' \"\${LOG_PIPE}\" '/tmp/memtier.started'
      mkfifo \"\${LOG_PIPE}\"
      (
        tr '\r' '\n' < \"\${LOG_PIPE}\" | while IFS= read -r line; do
          [ -n \"\${line}\" ] || continue
          line_ts=\"\$(date +%s)\"
          printf '%s\t%s\n' \"\${line_ts}\" \"\${line}\"
        done
      ) > '${REMOTE_OUT}/${log_file}' &
      logger_pid=\$!
      date +%s > /tmp/memtier.started
      memtier_benchmark \
        --server='${HOST}' --port='${PORT}' \
        --protocol=redis \
        --cluster-mode \
        --threads='${THREADS}' --clients='${CLIENTS}' \
        --test-time='${TEST_TIME}' \
        --key-maximum='${KEYS}' \
        --data-size='${DATA_SIZE}' \
        --ratio='${RATIO}' \
        --json-out-file '${REMOTE_OUT}/${out_file}' \
        --run-count 1 \
        --print-percentiles='50,95,99,99.9' \
        > \"\${LOG_PIPE}\" 2>&1
      status=\$?
      wait \"\${logger_pid}\" 2>/dev/null || true
      rm -f \"\${LOG_PIPE}\"
      echo \"\$status\" > '${POD_EXIT_CODE_FILE}'
      touch '${POD_DONE_FILE}'
      sleep '${POD_HOLD_SECONDS}'
    "

  wait_for_command_pod "${pod_name}" "$((TEST_TIME + 300))"
  kubectl cp "${NS}/${pod_name}:${REMOTE_OUT}/${out_file}" "${LOCAL_OUT}/${out_file}"
  kubectl cp "${NS}/${pod_name}:${REMOTE_OUT}/${log_file}" "${LOCAL_OUT}/${log_file}" || \
    echo "WARN: could not copy memtier log ${log_file}"
  MEMTIER_STARTED_EPOCH_S="$(kubectl exec "${pod_name}" -n "${NS}" -- cat /tmp/memtier.started 2>/dev/null | tr -d '[:space:]')"
  kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found >/dev/null
}

write_summary() {
  local summary_file="$1"
  local run="$2"
  local run_id="$3"
  local target_mb="$4"
  local prefill_start="$5"
  local prefill_end="$6"
  local observe_start="$7"
  local observe_end="$8"
  local before_file="$9"
  local after_fill_file="${10}"
  local after_observe_file="${11}"
  local seed_report_file="${12}"
  local memtier_file="${13}"
  local memtier_log="${14}"
  local timing_file="${15}"
  local maxmemory_mb="${16}"
  local monitor_file="${17}"

  "${PYTHON_BIN}" - \
    "${summary_file}" \
    "${before_file}" \
    "${after_fill_file}" \
    "${after_observe_file}" \
    "${seed_report_file}" \
    "${memtier_file}" \
    "${monitor_file}" \
    "${run}" \
    "${run_id}" \
    "${target_mb}" \
    "${maxmemory_mb}" \
    "${TARGET_MB}" \
    "${prefill_start}" \
    "${prefill_end}" \
    "${observe_start}" \
    "${observe_end}" \
    "${memtier_log}" \
    "${timing_file}" \
    "${PROVIDER}" \
    "${SYSTEM_NAME}" \
    "${VARIANT}" \
    "${MAXMEMORY_POLICY}" \
    "${CONFIGURE_MAXMEMORY}" \
    "${MEMTIER_IMAGE}" \
    "${BACKUP_IMAGE}" \
    "${THREADS}" \
    "${CLIENTS}" \
    "${TEST_TIME}" \
    "${KEYS}" \
    "${DATA_SIZE}" \
    "${RATIO}" <<'PY'
import json
import sys
from pathlib import Path

(
    summary_path,
    before_path,
    after_fill_path,
    after_observe_path,
    seed_report_path,
    memtier_path,
    monitor_path,
    run,
    run_id,
    target_mb,
    maxmemory_mb,
    target_mode,
    prefill_start,
    prefill_end,
    observe_start,
    observe_end,
    memtier_log,
    timing_file,
    provider,
    system_name,
    variant,
    policy,
    configured_maxmemory,
    memtier_image,
    backup_image,
    threads,
    clients,
    test_time,
    keys,
    data_size,
    ratio,
) = sys.argv[1:]

def load(path):
    with open(path) as fh:
        return json.load(fh)

def sum_masters(doc, field):
    return sum(int(master.get(field, 0) or 0) for master in doc.get("masters", []))

def first_master(doc, field, default=""):
    masters = doc.get("masters", [])
    if not masters:
        return default
    return masters[0].get(field, default)

before = load(before_path)
after_fill = load(after_fill_path)
after_observe = load(after_observe_path)
seed_report = load(seed_report_path)
memtier = load(memtier_path)
monitor = load(monitor_path) if monitor_path else None
totals = memtier.get("ALL STATS", {}).get("Totals", {})
percentiles = totals.get("Percentile Latencies", {})

evicted_before = sum_masters(before, "evicted_keys")
evicted_after_fill = sum_masters(after_fill, "evicted_keys")
evicted_after_observe = sum_masters(after_observe, "evicted_keys")
errors_before = sum_masters(before, "total_error_replies")
errors_after_fill = sum_masters(after_fill, "total_error_replies")
errors_after_observe = sum_masters(after_observe, "total_error_replies")
maxmemory_total = sum_masters(after_fill, "maxmemory")
first_reached = (monitor or {}).get("first_reached")
last_monitor_sample = (monitor or {}).get("last_sample")
prefill_samples = (monitor or {}).get("samples", [])
used_after_fill = sum_masters(after_fill, "used_memory")
used_after_observe = sum_masters(after_observe, "used_memory")
evicted_during_fill = evicted_after_fill - evicted_before
seed_oom_errors = int(seed_report.get("oom_errors", 0) or 0)
maxmemory_pressure_observed = (
    (maxmemory_total > 0 and used_after_fill >= maxmemory_total)
    or evicted_during_fill > 0
    or seed_oom_errors > 0
)
after_fill_used_pct = (
    round(used_after_fill / maxmemory_total * 100.0, 3)
    if maxmemory_total > 0 else None
)

def prefill_samples_until_reach():
    if not prefill_samples:
        return []
    if not first_reached:
        return prefill_samples
    reached_epoch = first_reached.get("epoch_s")
    if reached_epoch is None:
        return prefill_samples
    return [
        sample for sample in prefill_samples
        if sample.get("epoch_s") is not None and sample.get("epoch_s") <= reached_epoch
    ]

def sample_rate(samples, field):
    if len(samples) < 2:
        return None
    first = samples[0]
    last = samples[-1]
    duration = float(last.get("elapsed_s", 0) or 0) - float(first.get("elapsed_s", 0) or 0)
    if duration <= 0:
        return None
    return (float(last.get(field, 0) or 0) - float(first.get(field, 0) or 0)) / duration

def last_interval_rate(samples, field):
    if len(samples) < 2:
        return None
    previous = samples[-2]
    last = samples[-1]
    duration = float(last.get("elapsed_s", 0) or 0) - float(previous.get("elapsed_s", 0) or 0)
    if duration <= 0:
        return None
    return (float(last.get(field, 0) or 0) - float(previous.get(field, 0) or 0)) / duration

prefill_reach_samples = prefill_samples_until_reach()
prefill_dbsize_ops_sec = sample_rate(prefill_reach_samples, "dbsize")
prefill_last_dbsize_ops_sec = last_interval_rate(prefill_reach_samples, "dbsize")
prefill_evictions_sec = sample_rate(prefill_reach_samples, "evicted_keys")
prefill_errors_sec = sample_rate(prefill_reach_samples, "total_error_replies")

summary = {
    "benchmark": "maxmemory_allkeys_lru",
    "variant": variant,
    "run": int(run),
    "provider": provider,
    "system": system_name,
    "run_id": run_id,
    "target_mb": int(target_mb),
    "maxmemory_reference_mb": int(maxmemory_mb),
    "target_mode": target_mode,
    "policy": policy,
    "observed_policy": first_master(after_fill, "maxmemory_policy"),
    "prefill_method": "seed",
    "ttl_seconds": seed_report.get("ttl_seconds") or 0,
    "maxmemory_policy": policy,
    "observed_policy_after_fill": first_master(after_fill, "maxmemory_policy"),
    "configured_maxmemory": configured_maxmemory or None,
    "threads": int(threads),
    "clients": int(clients),
    "test_time_s": int(test_time),
    "keys": int(keys),
    "data_size": int(data_size),
    "ratio": ratio,
    "prefill_start_epoch_s": int(prefill_start),
    "prefill_end_epoch_s": int(prefill_end),
    "prefill_wall_duration_s": int(prefill_end) - int(prefill_start),
    "observe_start_epoch_s": int(observe_start),
    "observe_end_epoch_s": int(observe_end),
    "observe_duration_s": int(observe_end) - int(observe_start),
    "fill_to_observe_gap_s": int(observe_start) - int(prefill_end),
    "seed_target_keys": seed_report.get("target_keys"),
    "seed_written_keys": seed_report.get("written_keys"),
    "seed_duration_s": seed_report.get("seed_duration_s"),
    "seed_pipeline_errors": seed_report.get("errors"),
    "seed_write_errors": seed_report.get("write_errors"),
    "seed_oom_errors": seed_report.get("oom_errors"),
    "seed_first_error": seed_report.get("first_error"),
    "seed_last_error": seed_report.get("last_error"),
    "target_keys": seed_report.get("target_keys"),
    "written_keys": seed_report.get("written_keys"),
    "seed_completed": seed_report.get("completed"),
    "pipeline_errors": seed_report.get("errors"),
    "write_errors": seed_report.get("write_errors"),
    "oom_errors": seed_report.get("oom_errors"),
    "maxmemory_total_bytes": maxmemory_total,
    "used_memory_before": sum_masters(before, "used_memory"),
    "used_memory_after_fill": used_after_fill,
    "used_memory_after_observe": used_after_observe,
    "used_memory_after": used_after_observe,
    "dbsize_before": sum_masters(before, "dbsize"),
    "dbsize_after_fill": sum_masters(after_fill, "dbsize"),
    "dbsize_after_observe": sum_masters(after_observe, "dbsize"),
    "dbsize_after": sum_masters(after_observe, "dbsize"),
    "evicted_keys_before": evicted_before,
    "evicted_keys_after_fill": evicted_after_fill,
    "evicted_keys_after_observe": evicted_after_observe,
    "evicted_keys_delta_prefill": evicted_after_fill - evicted_before,
    "evicted_keys_delta_observe": evicted_after_observe - evicted_after_fill,
    "evicted_keys_delta_total": evicted_after_observe - evicted_before,
    "evicted_keys_delta": evicted_after_observe - evicted_before,
    "error_replies_before": errors_before,
    "error_replies_after_fill": errors_after_fill,
    "error_replies_after_observe": errors_after_observe,
    "error_replies_delta_prefill": errors_after_fill - errors_before,
    "error_replies_delta_observe": errors_after_observe - errors_after_fill,
    "error_replies_delta": errors_after_observe - errors_before,
    "sample_size": None,
    "sample_missing": None,
    "sample_missing_rate": None,
    "verify_errors": None,
    "prefill_memtier_ops_sec": None,
    "prefill_memtier_count": None,
    "prefill_memtier_hits_sec": None,
    "prefill_memtier_misses_sec": None,
    "prefill_memtier_connection_errors": None,
    "prefill_memtier_avg_latency_ms": None,
    "prefill_memtier_max_latency_ms": None,
    "prefill_memtier_p50_ms": None,
    "prefill_memtier_p95_ms": None,
    "prefill_memtier_p99_ms": None,
    "prefill_memtier_p999_ms": None,
    "memtier_ops_sec": totals.get("Ops/sec"),
    "memtier_count": totals.get("Count"),
    "memtier_hits_sec": totals.get("Hits/sec"),
    "memtier_misses_sec": totals.get("Misses/sec"),
    "memtier_connection_errors": totals.get("Connection Errors"),
    "memtier_avg_latency_ms": totals.get("Average Latency", totals.get("Latency")),
    "memtier_max_latency_ms": totals.get("Max Latency"),
    "memtier_p50_ms": percentiles.get("p50.00"),
    "memtier_p95_ms": percentiles.get("p95.00"),
    "memtier_p99_ms": percentiles.get("p99.00"),
    "memtier_p999_ms": percentiles.get("p99.90"),
    "prefill_report_status": "seed_completed" if seed_report.get("completed") else "seed_partial",
    "prefill_sample_count": (monitor or {}).get("sample_count"),
    "prefill_dbsize_growth_ops_sec_to_100": round(prefill_dbsize_ops_sec, 3) if prefill_dbsize_ops_sec is not None else None,
    "prefill_last_dbsize_growth_ops_sec_to_100": round(prefill_last_dbsize_ops_sec, 3) if prefill_last_dbsize_ops_sec is not None else None,
    "prefill_evictions_sec_to_100": round(prefill_evictions_sec, 3) if prefill_evictions_sec is not None else None,
    "prefill_error_replies_sec_to_100": round(prefill_errors_sec, 3) if prefill_errors_sec is not None else None,
    "maxmemory_reached": maxmemory_pressure_observed,
    "maxmemory_reached_epoch_s": None,
    "maxmemory_reached_elapsed_s": None,
    "maxmemory_reached_used_memory_pct": after_fill_used_pct,
    "prefill_last_used_memory_pct": after_fill_used_pct,
    "prefill_reached_dbsize": sum_masters(after_fill, "dbsize") if maxmemory_pressure_observed else None,
    "prefill_reached_evicted_keys": evicted_after_fill if maxmemory_pressure_observed else None,
    "prefill_reached_error_replies": errors_after_fill if maxmemory_pressure_observed else None,
    "prefill_last_dbsize": sum_masters(after_fill, "dbsize"),
    "prefill_last_evicted_keys": evicted_after_fill,
    "prefill_last_error_replies": errors_after_fill,
    "memtier_image": memtier_image,
    "backup_image": backup_image,
    "artifacts": {
        "before_snapshot": Path(before_path).name,
        "after_fill_snapshot": Path(after_fill_path).name,
        "after_observe_snapshot": Path(after_observe_path).name,
        "seed_report": Path(seed_report_path).name,
        "memtier_file": Path(memtier_path).name,
        "memtier_log": memtier_log,
        "timing_file": timing_file,
        "prefill_report": Path(seed_report_path).name,
    },
}

with open(summary_path, "w") as fh:
    json.dump(summary, fh, indent=2)
PY
}

assert_new_run_files() {
  local run="$1"
  local existing

  if [[ "${OVERWRITE_RESULTS}" == "true" ]]; then
    return 0
  fi

  existing="$(
    find "${LOCAL_OUT}" -maxdepth 1 -type f \
      \( -name "*_${run}.json" -o -name "*_${run}.log" -o -name "*_${run}_*.json" -o -name "*_${run}_*.log" \) \
      -print -quit
  )"
  if [[ -n "${existing}" ]]; then
    echo "ERROR: refusing to overwrite existing result file: ${existing}" >&2
    echo "Set OVERWRITE_RESULTS=true or use a fresh output directory." >&2
    exit 1
  fi
}

print_config() {
  cat <<EOF
==> Maxmemory allkeys-lru benchmark configuration
N=${N}
PROVIDER=${PROVIDER}
SYSTEM_NAME=${SYSTEM_NAME}
VARIANT=${VARIANT}
NS=${NS}
RELEASE=${RELEASE}
STS=${STS}
CLI_POD=${CLI_POD}
CLI_BIN=${CLI_BIN}
SERVICE_NAME=${SERVICE_NAME}
HOST=${HOST}
PORT=${PORT}
TARGET_MB=${TARGET_MB}
SEED_ALLOW_PARTIAL=${SEED_ALLOW_PARTIAL}
STOP_AFTER_ERRORS=${STOP_AFTER_ERRORS}
RANDOM_DATA=${RANDOM_DATA}
TTL_SECONDS=${TTL_SECONDS}
MAXMEMORY_POLICY=${MAXMEMORY_POLICY}
CONFIGURE_POLICY=${CONFIGURE_POLICY}
CONFIGURE_MAXMEMORY=${CONFIGURE_MAXMEMORY}
FLUSH_BETWEEN_RUNS=${FLUSH_BETWEEN_RUNS}
FLUSH_AFTER_RUNS=${FLUSH_AFTER_RUNS}
OVERWRITE_RESULTS=${OVERWRITE_RESULTS}
THREADS=${THREADS}
CLIENTS=${CLIENTS}
TEST_TIME=${TEST_TIME}
KEYS=${KEYS}
DATA_SIZE=${DATA_SIZE}
RATIO=${RATIO}
MEMTIER_IMAGE=${MEMTIER_IMAGE}
BACKUP_IMAGE=${BACKUP_IMAGE}
LOCAL_OUT=${LOCAL_OUT}
REMOTE_OUT=${REMOTE_OUT}
EOF
}

print_config

if [[ "${MAXMEMORY_POLICY}" != "allkeys-lru" ]]; then
  echo "WARN: this benchmark is intended for allkeys-lru, but MAXMEMORY_POLICY=${MAXMEMORY_POLICY}."
fi

for i in $(seq 1 "${N}"); do
  assert_new_run_files "${i}"

  SAFE_POLICY="${MAXMEMORY_POLICY//[^a-zA-Z0-9]/-}"
  RUN_ID="maxmem_lru_${PROVIDER}_${i}_$(date +%s)"
  FLUSH_POD="maxmemory-lru-flush-${i}"
  CONFIG_POD="maxmemory-lru-config-${i}"
  SNAP_BEFORE_POD="maxmemory-lru-before-${i}"
  SNAP_FILL_POD="maxmemory-lru-fill-${i}"
  SNAP_AFTER_POD="maxmemory-lru-after-${i}"
  SEED_POD="maxmemory-lru-seed-${i}"
  MEMTIER_POD="memtier-maxmemory-lru-${i}"

  BEFORE_FILE="maxmemory_before_${PROVIDER}_${SAFE_POLICY}_${i}.json"
  AFTER_FILL_FILE="maxmemory_after_fill_${PROVIDER}_${SAFE_POLICY}_${i}.json"
  AFTER_OBSERVE_FILE="maxmemory_after_observe_${PROVIDER}_${SAFE_POLICY}_${i}.json"
  SEED_REPORT="maxmemory_seed_${PROVIDER}_${SAFE_POLICY}_${i}.json"
  MEMTIER_FILE="resilience_maxmemory_run_${i}.json"
  MEMTIER_LOG="resilience_maxmemory_run_${i}.log"
  TIMING_FILE="maxmemory_resilience_timing_${i}.json"
  SUMMARY_FILE="maxmemory_summary_${PROVIDER}_${SAFE_POLICY}_${i}.json"

  echo ""
  echo "=========================================="
  echo "  Maxmemory allkeys-lru run ${i}/${N}"
  echo "=========================================="

  echo "[${i}] Waiting for ${SYSTEM_NAME} cluster health..."
  wait_for_cluster_health 120

  kubectl delete pod "${FLUSH_POD}" "${CONFIG_POD}" "${SNAP_BEFORE_POD}" \
    "${SNAP_FILL_POD}" "${SNAP_AFTER_POD}" "${SEED_POD}" "${MEMTIER_POD}" \
    -n "${NS}" --ignore-not-found >/dev/null 2>&1 || true

  if [[ "${FLUSH_BETWEEN_RUNS}" == "true" ]]; then
    echo "[${i}] Flushing existing keys and resetting stats..."
    flush_cluster "${FLUSH_POD}"
  fi

  if [[ "${CONFIGURE_POLICY}" == "true" ]]; then
    echo "[${i}] Configuring maxmemory-policy=${MAXMEMORY_POLICY}..."
    configure_policy "${CONFIG_POD}"
  fi

  echo "[${i}] Capturing before-fill snapshot..."
  snapshot_cluster "${SNAP_BEFORE_POD}" "before_fill" "${LOCAL_OUT}/${BEFORE_FILE}"

  MAXMEMORY_MB="$(resolve_maxmemory_mb "${LOCAL_OUT}/${BEFORE_FILE}")"
  if [[ "${TARGET_MB}" == "auto" ]]; then
    RUN_TARGET_MB="${MAXMEMORY_MB}"
  elif [[ "${TARGET_MB}" =~ ^[0-9]+$ ]]; then
    RUN_TARGET_MB="${TARGET_MB}"
  else
    echo "ERROR: target-mb must be an integer MB value or auto; got ${TARGET_MB}." >&2
    exit 1
  fi

  echo "[${i}] Maxmemory reference: ${MAXMEMORY_MB} MB; seed target=${RUN_TARGET_MB} MB"
  PREFILL_START_EPOCH="$(date +%s)"

  echo "[${i}] Seeding cluster to maxmemory pressure (${RUN_TARGET_MB} MB target)..."
  seed_fill "${SEED_POD}" "${SEED_REPORT}" "${RUN_TARGET_MB}" "${RUN_ID}"
  PREFILL_END_EPOCH="$(date +%s)"
  PREFILL_REACHED_EPOCH=""
  prefill_duration="$("${PYTHON_BIN}" -c "import json; print(json.load(open('${LOCAL_OUT}/${SEED_REPORT}')).get('seed_duration_s'))")"
  prefill_keys="$("${PYTHON_BIN}" -c "import json; print(json.load(open('${LOCAL_OUT}/${SEED_REPORT}')).get('written_keys'))")"
  echo "[${i}] Seed done: ${prefill_keys} keys in ${prefill_duration}s"

  echo "[${i}] Capturing after-fill snapshot..."
  snapshot_cluster "${SNAP_FILL_POD}" "after_fill" "${LOCAL_OUT}/${AFTER_FILL_FILE}"

  echo "[${i}] Running memtier for ${TEST_TIME}s on full cluster..."
  OBSERVE_START_EPOCH="$(date +%s)"
  MEMTIER_STARTED_EPOCH_S=""
  run_memtier "${MEMTIER_POD}" "${MEMTIER_FILE}" "${MEMTIER_LOG}"
  OBSERVE_END_EPOCH="$(date +%s)"
  if [[ -n "${MEMTIER_STARTED_EPOCH_S}" ]]; then
    OBSERVE_START_EPOCH="${MEMTIER_STARTED_EPOCH_S}"
    OBSERVE_END_EPOCH="$((MEMTIER_STARTED_EPOCH_S + TEST_TIME))"
  fi

  echo "[${i}] Capturing after-observe snapshot..."
  snapshot_cluster "${SNAP_AFTER_POD}" "after_observe" "${LOCAL_OUT}/${AFTER_OBSERVE_FILE}"

  cat > "${LOCAL_OUT}/${TIMING_FILE}" <<EOF
{
  "run": ${i},
  "phase": "full",
  "benchmark": "maxmemory_allkeys_lru",
  "variant": $(json_string "${VARIANT}"),
  "provider": $(json_string "${PROVIDER}"),
  "system": $(json_string "${SYSTEM_NAME}"),
  "run_id": $(json_string "${RUN_ID}"),
  "target_mb": ${RUN_TARGET_MB},
  "target_mode": $(json_string "${TARGET_MB}"),
  "maxmemory_policy": $(json_string "${MAXMEMORY_POLICY}"),
  "maxmemory_reference_mb": ${MAXMEMORY_MB},
  "prefill_start_epoch_s": ${PREFILL_START_EPOCH},
  "prefill_end_epoch_s": ${PREFILL_END_EPOCH},
  "prefill_reached_epoch_s": $(json_string "${PREFILL_REACHED_EPOCH}"),
  "observe_start_epoch_s": ${OBSERVE_START_EPOCH},
  "observe_end_epoch_s": ${OBSERVE_END_EPOCH},
  "test_time_s": ${TEST_TIME},
  "memtier_file": $(json_string "${MEMTIER_FILE}"),
  "memtier_log": $(json_string "${MEMTIER_LOG}"),
  "seed_report": $(json_string "${SEED_REPORT}"),
  "prefill_method": "seed",
  "prefill_keys": ${prefill_keys},
  "prefill_duration_s": ${prefill_duration}
}
EOF

  write_summary \
    "${LOCAL_OUT}/${SUMMARY_FILE}" \
    "${i}" \
    "${RUN_ID}" \
    "${RUN_TARGET_MB}" \
    "${PREFILL_START_EPOCH}" \
    "${PREFILL_END_EPOCH}" \
    "${OBSERVE_START_EPOCH}" \
    "${OBSERVE_END_EPOCH}" \
    "${LOCAL_OUT}/${BEFORE_FILE}" \
    "${LOCAL_OUT}/${AFTER_FILL_FILE}" \
    "${LOCAL_OUT}/${AFTER_OBSERVE_FILE}" \
    "${LOCAL_OUT}/${SEED_REPORT}" \
    "${LOCAL_OUT}/${MEMTIER_FILE}" \
    "${MEMTIER_LOG}" \
    "${TIMING_FILE}" \
    "${MAXMEMORY_MB}" \
    ""

  echo "[${i}] Summary saved: ${LOCAL_OUT}/${SUMMARY_FILE}"
  echo "[${i}] Waiting for ${SYSTEM_NAME} cluster health after run..."
  wait_for_cluster_health 120

  if [[ "${FLUSH_AFTER_RUNS}" == "true" ]]; then
    echo "[${i}] Flushing keys after run..."
    flush_cluster "${FLUSH_POD}"
  fi

  echo "[${i}] Done. Result: ${LOCAL_OUT}/${SUMMARY_FILE}"
done

echo ""
echo "=========================================="
echo "  All ${N} maxmemory allkeys-lru runs complete."
echo "  Results in: ${LOCAL_OUT}/"
echo "  Analyse eviction summary with:"
echo "    python cli.py maxmemory --input ${LOCAL_OUT} --output-dir ./plots/maxmemory_allkeys_lru"
echo "  Analyse 30s client behavior with:"
echo "    python cli.py resilience --input ${LOCAL_OUT} --scenario maxmemory --output-dir ./plots/maxmemory_allkeys_lru_resilience --no-prometheus"
echo "=========================================="

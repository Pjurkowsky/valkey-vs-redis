#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<EOF
Usage: $0 <memorystore-instance-id> [output-dir]

Runs a managed Memorystore for Redis Cluster shard-scaling benchmark:
  1. Start memtier load from a Kubernetes pod.
  2. Scale Memorystore from ORIGINAL_SHARDS to TARGET_SHARDS.
  3. Start a second memtier load.
  4. Scale Memorystore back to ORIGINAL_SHARDS.

Defaults:
  MEMORYSTORE_PRODUCT=redis
  LOCATION=europe-central2
  ORIGINAL_SHARDS=3
  TARGET_SHARDS=4
  N=5
  TEST_TIME=900
  VERIFY_RESHARD_DATA=false
  INTEGRITY_DATASET_MB=100
  INTEGRITY_VERIFY_MODE=sample
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" || -z "${1:-}" ]]; then
  usage
  exit 0
fi

INSTANCE_ID="$1"
LOCAL_OUT="${2:-./results/memorystore_reshard}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

source "${SCRIPT_DIR}/pod_results.sh"

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null)}"
MEMORYSTORE_PRODUCT="${MEMORYSTORE_PRODUCT:-redis}"
LOCATION="${LOCATION:-europe-central2}"
NS="${NS:-vk}"
ARTIFACT_REPO="${ARTIFACT_REPO:-valkey-bench}"
IMAGE="${MEMTIER_IMAGE:-${LOCATION}-docker.pkg.dev/${PROJECT_ID}/${ARTIFACT_REPO}/memtier_k8s:1}"
BACKUP_IMAGE="${BACKUP_IMAGE:-${LOCATION}-docker.pkg.dev/${PROJECT_ID}/${ARTIFACT_REPO}/backup_restore:1}"
REMOTE_OUT="/work/results/memorystore_reshard"

N="${N:-5}"
ORIGINAL_SHARDS="${ORIGINAL_SHARDS:-3}"
TARGET_SHARDS="${TARGET_SHARDS:-4}"
PORT="${MEMORYSTORE_PORT:-6379}"

THREADS="${MEMTIER_THREADS:-4}"
CLIENTS="${MEMTIER_CLIENTS:-16}"
TEST_TIME="${TEST_TIME:-900}"
KEYS="${MEMTIER_KEYS:-100000}"
DATA_SIZE="${MEMTIER_DATA_SIZE:-1024}"
RATIO="${MEMTIER_RATIO:-1:1}"
MEMTIER_RANDOM_DATA="${MEMTIER_RANDOM_DATA:-false}"
STEADY_STATE_WAIT="${STEADY_STATE_WAIT:-30}"
OPERATION_SETTLE_WAIT="${OPERATION_SETTLE_WAIT:-30}"
VERIFY_RESHARD_DATA="${VERIFY_RESHARD_DATA:-false}"
INTEGRITY_DATASET_MB="${INTEGRITY_DATASET_MB:-100}"
INTEGRITY_VERIFY_MODE="${INTEGRITY_VERIFY_MODE:-sample}"
INTEGRITY_RANDOM_DATA="${INTEGRITY_RANDOM_DATA:-true}"
INTEGRITY_CLEANUP="${INTEGRITY_CLEANUP:-true}"
INTEGRITY_SEED_TIMEOUT_SECONDS="${INTEGRITY_SEED_TIMEOUT_SECONDS:-14400}"
INTEGRITY_VERIFY_TIMEOUT_SECONDS="${INTEGRITY_VERIFY_TIMEOUT_SECONDS:-3600}"
INTEGRITY_RANDOM_DATA_ARG=""
if [[ "${INTEGRITY_RANDOM_DATA}" == "true" ]]; then
  INTEGRITY_RANDOM_DATA_ARG="--random-data"
fi
MEMTIER_RANDOM_DATA_ARG=""
case "${MEMTIER_RANDOM_DATA}" in
  1|true|TRUE|yes|YES|on|ON)
    MEMTIER_RANDOM_DATA_ARG="--random-data"
    ;;
esac

mkdir -p "${LOCAL_OUT}"

if [[ -z "${PROJECT_ID}" || "${PROJECT_ID}" == "(unset)" ]]; then
  echo "ERROR: Could not determine GCP project. Run 'gcloud config set project <project-id>' or set PROJECT_ID." >&2
  exit 1
fi

if [[ "${MEMORYSTORE_PRODUCT}" != "redis" && "${MEMORYSTORE_PRODUCT}" != "valkey" ]]; then
  echo "ERROR: MEMORYSTORE_PRODUCT must be 'redis' or 'valkey'." >&2
  exit 1
fi

PYTHON_BIN="$(command -v python3 || command -v python || true)"
if [[ -z "${PYTHON_BIN}" ]]; then
  echo "ERROR: python3 or python is required to parse gcloud JSON output." >&2
  exit 1
fi

json_string() {
  "${PYTHON_BIN}" -c 'import json, sys; print(json.dumps(sys.argv[1]))' "$1"
}

describe_instance_json() {
  if [[ "${MEMORYSTORE_PRODUCT}" == "redis" ]]; then
    gcloud redis clusters describe "${INSTANCE_ID}" \
      --project="${PROJECT_ID}" \
      --region="${LOCATION}" \
      --format=json
  else
    gcloud memorystore instances describe "${INSTANCE_ID}" \
      --project="${PROJECT_ID}" \
      --location="${LOCATION}" \
      --format=json
  fi
}

current_shard_count() {
  describe_instance_json | "${PYTHON_BIN}" -c '
import json, sys
doc = json.load(sys.stdin)
print(doc.get("shardCount", ""))
'
}

current_instance_state() {
  describe_instance_json | "${PYTHON_BIN}" -c '
import json, sys
doc = json.load(sys.stdin)
print(doc.get("state", ""))
'
}

state_is_ready() {
  local state="$1"

  case "${state}" in
    ""|ACTIVE|READY)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

discover_endpoint() {
  describe_instance_json | "${PYTHON_BIN}" -c '
import json, sys

doc = json.load(sys.stdin)

def emit(address, port):
    if address:
        print(f"{address} {port or 6379}")
        return True
    return False

for endpoint in doc.get("discoveryEndpoints") or []:
    if emit(endpoint.get("address"), endpoint.get("port")):
        raise SystemExit(0)

for endpoint in doc.get("endpoints") or []:
    port = endpoint.get("port") or 6379
    for connection in endpoint.get("connections") or []:
        candidates = [
            connection.get("pscAutoConnection") or {},
            connection.get("pscConnection") or {},
            connection,
        ]
        for candidate in candidates:
            connection_type = candidate.get("connectionType") or connection.get("connectionType")
            address = candidate.get("address") or candidate.get("ipAddress")
            if connection_type in (None, "", "CONNECTION_TYPE_DISCOVERY") and emit(address, port):
                raise SystemExit(0)

raise SystemExit("Could not find Memorystore discovery endpoint address in instance description")
'
}

wait_for_instance_shards() {
  local expected="$1"
  local timeout="${2:-900}"
  local deadline=$((SECONDS + timeout))
  local current state

  echo "  Waiting for Memorystore shardCount=${expected} and ready state (max ${timeout}s)..."
  while (( SECONDS < deadline )); do
    current="$(current_shard_count || true)"
    state="$(current_instance_state || true)"
    if [[ "${current}" == "${expected}" ]] && state_is_ready "${state}"; then
      return 0
    fi
    echo "  Current shardCount=${current:-unknown}, state=${state:-unknown}; waiting..."
    sleep 10
  done

  echo "ERROR: Memorystore did not report shardCount=${expected} with ready state within ${timeout}s" >&2
  return 1
}

update_shard_count() {
  local target="$1"
  local phase="$2"
  local run_idx="$3"
  local log_file="${LOCAL_OUT}/memorystore_${phase}_update_${run_idx}.log"
  local -a update_cmd

  echo "  Updating Memorystore ${INSTANCE_ID} to ${target} shard(s)..."
  if [[ "${MEMORYSTORE_PRODUCT}" == "redis" ]]; then
    update_cmd=(
      gcloud redis clusters update "${INSTANCE_ID}"
      --project="${PROJECT_ID}"
      --region="${LOCATION}"
      --shard-count="${target}"
      --quiet
    )
  else
    update_cmd=(
      gcloud memorystore instances update "${INSTANCE_ID}"
      --project="${PROJECT_ID}"
      --location="${LOCATION}"
      --shard-count="${target}"
      --quiet
    )
  fi

  if "${update_cmd[@]}" >"${log_file}" 2>&1; then
    echo "  Memorystore update completed. Log: ${log_file}"
    wait_for_instance_shards "${target}" 900
    return 0
  fi

  echo "ERROR: Memorystore update failed. Log: ${log_file}" >&2
  sed 's/^/  /' "${log_file}" >&2 || true
  return 1
}

ensure_shard_count() {
  local expected="$1"
  local current

  current="$(current_shard_count || true)"
  if [[ "${current}" == "${expected}" ]]; then
    return 0
  fi

  echo "Memorystore ${INSTANCE_ID} is at shardCount=${current:-unknown}; restoring to ${expected} before benchmark..."
  update_shard_count "${expected}" "restore" "0"
}

start_memtier_pod() {
  local pod_name="$1"
  local out_file="$2"
  local host="$3"
  local port="$4"

  kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found >/dev/null 2>&1 || true

  kubectl run "${pod_name}" -n "${NS}" \
    --image="${IMAGE}" \
    --restart=Never \
    --command -- \
    /bin/sh -c "
      mkdir -p '${REMOTE_OUT}'
      memtier_benchmark \
        --server='${host}' --port='${port}' \
        --protocol=redis \
        --cluster-mode \
        --threads='${THREADS}' --clients='${CLIENTS}' \
        --test-time='${TEST_TIME}' \
        --key-maximum='${KEYS}' \
        --data-size='${DATA_SIZE}' \
        ${MEMTIER_RANDOM_DATA_ARG:+${MEMTIER_RANDOM_DATA_ARG} }--ratio='${RATIO}' \
        --json-out-file '${REMOTE_OUT}/${out_file}' \
        --run-count 1 \
        --print-percentiles='50,95,99,99.9'
      status=\$?
      echo \"\$status\" > '${POD_EXIT_CODE_FILE}'
      touch '${POD_DONE_FILE}'
      sleep '${POD_HOLD_SECONDS}'
    "

  kubectl wait pod/"${pod_name}" -n "${NS}" \
    --for=condition=Ready --timeout=90s 2>/dev/null || true
}

finish_memtier_pod() {
  local pod_name="$1"
  local out_file="$2"
  local timeout=$((TEST_TIME + 900))

  if ! wait_for_pod_marker "${NS}" "${pod_name}" "${POD_DONE_FILE}" "${timeout}"; then
    echo "ERROR: memtier pod ${pod_name} did not signal completion." >&2
    print_pod_debug_info "${NS}" "${pod_name}"
    return 1
  fi

  local exit_code
  exit_code="$(read_pod_exit_code "${NS}" "${pod_name}" "${POD_EXIT_CODE_FILE}")"
  if [[ -z "${exit_code}" || "${exit_code}" != "0" ]]; then
    echo "ERROR: memtier pod ${pod_name} exited with code ${exit_code:-unknown}." >&2
    print_pod_debug_info "${NS}" "${pod_name}"
    return 1
  fi

  kubectl cp "${NS}/${pod_name}:${REMOTE_OUT}/${out_file}" "${LOCAL_OUT}/${out_file}"
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

wait_for_pod_ready() {
  local pod_name="$1"
  local timeout_s="${2:-120}"

  kubectl wait pod/"${pod_name}" -n "${NS}" \
    --for=condition=Ready --timeout="${timeout_s}s"
}

run_integrity_seed_pod() {
  local run_idx="$1"
  local host="$2"
  local port="$3"
  local run_id="$4"
  local report_file="$5"
  local pod_name="ms-reshard-seed-${run_idx}"

  kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl run "${pod_name}" -n "${NS}" \
    --image="${BACKUP_IMAGE}" \
    --restart=Never \
    --command -- \
    /bin/sh -c "
      mkdir -p '${REMOTE_OUT}'
      python /work/backup_restore_seed.py \
        --mode seed \
        --host '${host}' --port '${port}' \
        --target-mb '${INTEGRITY_DATASET_MB}' \
        --run-id '${run_id}' \
        ${INTEGRITY_RANDOM_DATA_ARG} \
        --output '${REMOTE_OUT}/${report_file}'
      status=\$?
      echo \"\$status\" > '${POD_EXIT_CODE_FILE}'
      touch '${POD_DONE_FILE}'
      sleep '${POD_HOLD_SECONDS}'
    "

  wait_for_command_pod "${pod_name}" "${INTEGRITY_SEED_TIMEOUT_SECONDS}"
  kubectl cp "${NS}/${pod_name}:${REMOTE_OUT}/${report_file}" "${LOCAL_OUT}/${report_file}"
  kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found >/dev/null
}

run_integrity_verify_pod() {
  local run_idx="$1"
  local phase="$2"
  local host="$3"
  local port="$4"
  local seed_report="$5"
  local verify_report="$6"
  local pod_name="ms-reshard-verify-${phase}-${run_idx}"

  kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl run "${pod_name}" -n "${NS}" \
    --image="${BACKUP_IMAGE}" \
    --restart=Never \
    --command -- \
    /bin/sh -c "
      mkdir -p '${REMOTE_OUT}'
      while [ ! -f '${REMOTE_OUT}/${seed_report}' ]; do sleep 1; done
      python /work/backup_restore_seed.py \
        --mode verify \
        --host '${host}' --port '${port}' \
        --seed-report '${REMOTE_OUT}/${seed_report}' \
        --verify-mode '${INTEGRITY_VERIFY_MODE}' \
        --output '${REMOTE_OUT}/${verify_report}'
      status=\$?
      echo \"\$status\" > '${POD_EXIT_CODE_FILE}'
      touch '${POD_DONE_FILE}'
      sleep '${POD_HOLD_SECONDS}'
    "

  wait_for_pod_ready "${pod_name}" 120
  kubectl cp "${LOCAL_OUT}/${seed_report}" "${NS}/${pod_name}:${REMOTE_OUT}/${seed_report}"
  wait_for_command_pod "${pod_name}" "${INTEGRITY_VERIFY_TIMEOUT_SECONDS}"
  kubectl cp "${NS}/${pod_name}:${REMOTE_OUT}/${verify_report}" "${LOCAL_OUT}/${verify_report}"
  kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found >/dev/null

  local integrity_ok
  integrity_ok="$("${PYTHON_BIN}" -c "import json; print(str(bool(json.load(open('${LOCAL_OUT}/${verify_report}')).get('integrity_ok', False))).lower())")"
  if [[ "${integrity_ok}" != "true" ]]; then
    echo "ERROR: integrity verification failed for ${verify_report}" >&2
    return 1
  fi
}

run_integrity_cleanup_pod() {
  local run_idx="$1"
  local host="$2"
  local port="$3"
  local seed_report="$4"
  local pod_name="ms-reshard-cleanup-${run_idx}"

  kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl run "${pod_name}" -n "${NS}" \
    --image="${BACKUP_IMAGE}" \
    --restart=Never \
    --command -- \
    /bin/sh -c "
      mkdir -p '${REMOTE_OUT}'
      while [ ! -f '${REMOTE_OUT}/${seed_report}' ]; do sleep 1; done
      python /work/backup_restore_seed.py \
        --mode cleanup \
        --host '${host}' --port '${port}' \
        --seed-report '${REMOTE_OUT}/${seed_report}'
      status=\$?
      echo \"\$status\" > '${POD_EXIT_CODE_FILE}'
      touch '${POD_DONE_FILE}'
      sleep '${POD_HOLD_SECONDS}'
    "

  wait_for_pod_ready "${pod_name}" 120
  kubectl cp "${LOCAL_OUT}/${seed_report}" "${NS}/${pod_name}:${REMOTE_OUT}/${seed_report}"
  wait_for_command_pod "${pod_name}" "${INTEGRITY_VERIFY_TIMEOUT_SECONDS}"
  kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found >/dev/null
}

write_integrity_summary() {
  local run_idx="$1"
  local run_id="$2"
  local seed_report="$3"
  local verify_up_report="$4"
  local verify_down_report="$5"
  local summary_file="${LOCAL_OUT}/reshard_integrity_${run_idx}.json"

  "${PYTHON_BIN}" - "${summary_file}" "${run_idx}" "${run_id}" "${seed_report}" "${verify_up_report}" "${verify_down_report}" "${LOCAL_OUT}" "${INSTANCE_ID}" "${LOCATION}" "${MEMORYSTORE_PRODUCT}" <<'PY'
import json
import sys
from pathlib import Path

summary_path = Path(sys.argv[1])
run_idx = int(sys.argv[2])
run_id = sys.argv[3]
seed_report = sys.argv[4]
verify_up_report = sys.argv[5]
verify_down_report = sys.argv[6]
local_out = Path(sys.argv[7])
instance_id = sys.argv[8]
location = sys.argv[9]
product = sys.argv[10]

def load(name):
    with (local_out / name).open() as fh:
        return json.load(fh)

seed = load(seed_report)
up = load(verify_up_report)
down = load(verify_down_report)

doc = {
    "run": run_idx,
    "run_id": run_id,
    "provider": f"memorystore_{product}",
    "instance": instance_id,
    "location": location,
    "dataset_mb": seed.get("target_mb"),
    "seed_report": seed_report,
    "verify_up_report": verify_up_report,
    "verify_down_report": verify_down_report,
    "seed_completed": bool(seed.get("completed", False)),
    "seed_written_keys": seed.get("written_keys", 0),
    "seed_random_data": bool(seed.get("random_data", False)),
    "verify_up_integrity_ok": bool(up.get("integrity_ok", False)),
    "verify_down_integrity_ok": bool(down.get("integrity_ok", False)),
    "integrity_ok": bool(up.get("integrity_ok", False)) and bool(down.get("integrity_ok", False)),
    "verify_mode": down.get("verify_mode", up.get("verify_mode", "unknown")),
    "verify_up": {
        "sample_size": up.get("sample_size"),
        "keys_found": up.get("keys_found"),
        "keys_missing": up.get("keys_missing"),
        "verify_errors": up.get("verify_errors"),
        "verify_duration_s": up.get("verify_duration_s"),
    },
    "verify_down": {
        "sample_size": down.get("sample_size"),
        "keys_found": down.get("keys_found"),
        "keys_missing": down.get("keys_missing"),
        "verify_errors": down.get("verify_errors"),
        "verify_duration_s": down.get("verify_duration_s"),
    },
}

with summary_path.open("w") as fh:
    json.dump(doc, fh, indent=2)
    fh.write("\n")
PY
}

write_up_timing() {
  local run_idx="$1"
  local memtier_start="$2"
  local operation_start="$3"
  local operation_end="$4"
  local status="$5"
  local timing_file="${LOCAL_OUT}/reshard_timing_${run_idx}.json"

  cat >"${timing_file}" <<EOF
{
  "run": ${run_idx},
  "phase": "up",
  "provider": "memorystore_${MEMORYSTORE_PRODUCT}",
  "instance": $(json_string "${INSTANCE_ID}"),
  "location": $(json_string "${LOCATION}"),
  "operation_status": $(json_string "${status}"),
  "original_shards": ${ORIGINAL_SHARDS},
  "target_shards": ${TARGET_SHARDS},
  "scale_start": ${operation_start},
  "scale_end": ${operation_end},
  "scale_duration_s": $((operation_end - operation_start)),
  "scale_start_s": $((operation_start - memtier_start)),
  "scale_end_s": $((operation_end - memtier_start)),
  "operation_start_s": $((operation_start - memtier_start)),
  "operation_end_s": $((operation_end - memtier_start)),
  "operation_duration_s": $((operation_end - operation_start))
}
EOF
}

write_down_timing() {
  local run_idx="$1"
  local memtier_start="$2"
  local operation_start="$3"
  local operation_end="$4"
  local status="$5"
  local timing_file="${LOCAL_OUT}/reshard_down_timing_${run_idx}.json"

  cat >"${timing_file}" <<EOF
{
  "run": ${run_idx},
  "phase": "down",
  "provider": "memorystore_${MEMORYSTORE_PRODUCT}",
  "instance": $(json_string "${INSTANCE_ID}"),
  "location": $(json_string "${LOCATION}"),
  "operation_status": $(json_string "${status}"),
  "original_shards": ${TARGET_SHARDS},
  "target_shards": ${ORIGINAL_SHARDS},
  "scale_down_start": ${operation_start},
  "scale_down_end": ${operation_end},
  "scale_down_duration_s": $((operation_end - operation_start)),
  "scale_down_start_s": $((operation_start - memtier_start)),
  "scale_down_end_s": $((operation_end - memtier_start)),
  "operation_start_s": $((operation_start - memtier_start)),
  "operation_end_s": $((operation_end - memtier_start)),
  "operation_duration_s": $((operation_end - operation_start))
}
EOF
}

read -r DISCOVERY_HOST DISCOVERY_PORT < <(discover_endpoint)
if [[ -z "${DISCOVERY_HOST:-}" ]]; then
  echo "ERROR: Could not discover Memorystore endpoint for ${INSTANCE_ID}." >&2
  exit 1
fi
if [[ -n "${DISCOVERY_PORT:-}" ]]; then
  PORT="${DISCOVERY_PORT}"
fi

echo "==> Memorystore reshard benchmark"
echo "PROJECT_ID=${PROJECT_ID}"
echo "MEMORYSTORE_PRODUCT=${MEMORYSTORE_PRODUCT}"
echo "LOCATION=${LOCATION}"
echo "INSTANCE_ID=${INSTANCE_ID}"
echo "DISCOVERY_ENDPOINT=${DISCOVERY_HOST}:${PORT}"
echo "MEMTIER_IMAGE=${IMAGE}"
echo "MEMTIER_THREADS=${THREADS}"
echo "MEMTIER_CLIENTS=${CLIENTS}"
echo "MEMTIER_KEYS=${KEYS}"
echo "MEMTIER_DATA_SIZE=${DATA_SIZE}"
echo "MEMTIER_RATIO=${RATIO}"
echo "MEMTIER_RANDOM_DATA=${MEMTIER_RANDOM_DATA}"
echo "BACKUP_IMAGE=${BACKUP_IMAGE}"
echo "N=${N}"
echo "TEST_TIME=${TEST_TIME}"
echo "ORIGINAL_SHARDS=${ORIGINAL_SHARDS}"
echo "TARGET_SHARDS=${TARGET_SHARDS}"
echo "VERIFY_RESHARD_DATA=${VERIFY_RESHARD_DATA}"
echo "INTEGRITY_DATASET_MB=${INTEGRITY_DATASET_MB}"
echo "INTEGRITY_VERIFY_MODE=${INTEGRITY_VERIFY_MODE}"
echo "INTEGRITY_RANDOM_DATA=${INTEGRITY_RANDOM_DATA}"
echo "INTEGRITY_CLEANUP=${INTEGRITY_CLEANUP}"

ensure_shard_count "${ORIGINAL_SHARDS}"

for i in $(seq 1 "${N}"); do
  echo ""
  echo "=========================================="
  echo "  Memorystore reshard run ${i}/${N}"
  echo "=========================================="

  UP_POD="memtier-ms-reshard-up-${i}"
  UP_OUT="reshard_run_${i}.json"
  UP_STATUS="success"
  INTEGRITY_RUN_ID="ms_reshard_${i}_$(date +%s)"
  SEED_REPORT="seed_report_reshard_${i}.json"
  VERIFY_UP_REPORT="verify_report_reshard_up_${i}.json"
  VERIFY_DOWN_REPORT="verify_report_reshard_down_${i}.json"

  if [[ "${VERIFY_RESHARD_DATA}" == "true" ]]; then
    echo "[${i}] Seeding ${INTEGRITY_DATASET_MB} MB integrity dataset..."
    run_integrity_seed_pod "${i}" "${DISCOVERY_HOST}" "${PORT}" "${INTEGRITY_RUN_ID}" "${SEED_REPORT}"
    echo "[${i}] Integrity seed report saved: ${LOCAL_OUT}/${SEED_REPORT}"
  fi

  echo "[${i}] Starting memtier for managed reshard-up (test-time=${TEST_TIME}s)..."
  start_memtier_pod "${UP_POD}" "${UP_OUT}" "${DISCOVERY_HOST}" "${PORT}"
  UP_MEMTIER_START="$(date +%s)"

  echo "[${i}] Waiting ${STEADY_STATE_WAIT}s for steady state..."
  sleep "${STEADY_STATE_WAIT}"

  UP_START="$(date +%s)"
  if ! update_shard_count "${TARGET_SHARDS}" "up" "${i}"; then
    UP_STATUS="failed"
  fi
  UP_END="$(date +%s)"
  write_up_timing "${i}" "${UP_MEMTIER_START}" "${UP_START}" "${UP_END}" "${UP_STATUS}"

  echo "[${i}] Waiting for reshard-up memtier to finish..."
  finish_memtier_pod "${UP_POD}" "${UP_OUT}"
  kubectl delete pod "${UP_POD}" -n "${NS}" --ignore-not-found >/dev/null
  echo "[${i}] Reshard-up result saved: ${LOCAL_OUT}/${UP_OUT}"

  if [[ "${UP_STATUS}" != "success" ]]; then
    echo "[${i}] Reshard-up failed; stopping before downscale." >&2
    exit 1
  fi

  if [[ "${VERIFY_RESHARD_DATA}" == "true" ]]; then
    echo "[${i}] Verifying integrity after reshard-up..."
    if ! run_integrity_verify_pod "${i}" "up" "${DISCOVERY_HOST}" "${PORT}" "${SEED_REPORT}" "${VERIFY_UP_REPORT}"; then
      if [[ "${INTEGRITY_CLEANUP}" == "true" ]]; then
        run_integrity_cleanup_pod "${i}" "${DISCOVERY_HOST}" "${PORT}" "${SEED_REPORT}" || true
      fi
      exit 1
    fi
    echo "[${i}] Reshard-up integrity OK: ${LOCAL_OUT}/${VERIFY_UP_REPORT}"
  fi

  echo "[${i}] Waiting ${OPERATION_SETTLE_WAIT}s before downscale phase..."
  sleep "${OPERATION_SETTLE_WAIT}"

  DOWN_POD="memtier-ms-reshard-down-${i}"
  DOWN_OUT="reshard_down_run_${i}.json"
  DOWN_STATUS="success"

  echo "[${i}] Starting memtier for managed reshard-down (test-time=${TEST_TIME}s)..."
  start_memtier_pod "${DOWN_POD}" "${DOWN_OUT}" "${DISCOVERY_HOST}" "${PORT}"
  DOWN_MEMTIER_START="$(date +%s)"

  echo "[${i}] Waiting ${STEADY_STATE_WAIT}s for downscale steady state..."
  sleep "${STEADY_STATE_WAIT}"

  DOWN_START="$(date +%s)"
  if ! update_shard_count "${ORIGINAL_SHARDS}" "down" "${i}"; then
    DOWN_STATUS="failed"
  fi
  DOWN_END="$(date +%s)"
  write_down_timing "${i}" "${DOWN_MEMTIER_START}" "${DOWN_START}" "${DOWN_END}" "${DOWN_STATUS}"

  echo "[${i}] Waiting for reshard-down memtier to finish..."
  finish_memtier_pod "${DOWN_POD}" "${DOWN_OUT}"
  kubectl delete pod "${DOWN_POD}" -n "${NS}" --ignore-not-found >/dev/null
  echo "[${i}] Reshard-down result saved: ${LOCAL_OUT}/${DOWN_OUT}"

  if [[ "${DOWN_STATUS}" != "success" ]]; then
    echo "[${i}] Reshard-down failed; attempting restore to ${ORIGINAL_SHARDS} shards." >&2
    update_shard_count "${ORIGINAL_SHARDS}" "restore" "${i}" || true
    exit 1
  fi

  if [[ "${VERIFY_RESHARD_DATA}" == "true" ]]; then
    echo "[${i}] Verifying integrity after reshard-down..."
    if ! run_integrity_verify_pod "${i}" "down" "${DISCOVERY_HOST}" "${PORT}" "${SEED_REPORT}" "${VERIFY_DOWN_REPORT}"; then
      if [[ "${INTEGRITY_CLEANUP}" == "true" ]]; then
        run_integrity_cleanup_pod "${i}" "${DISCOVERY_HOST}" "${PORT}" "${SEED_REPORT}" || true
      fi
      exit 1
    fi
    write_integrity_summary "${i}" "${INTEGRITY_RUN_ID}" "${SEED_REPORT}" "${VERIFY_UP_REPORT}" "${VERIFY_DOWN_REPORT}"
    echo "[${i}] Reshard integrity summary saved: ${LOCAL_OUT}/reshard_integrity_${i}.json"

    if [[ "${INTEGRITY_CLEANUP}" == "true" ]]; then
      echo "[${i}] Cleaning up integrity dataset..."
      run_integrity_cleanup_pod "${i}" "${DISCOVERY_HOST}" "${PORT}" "${SEED_REPORT}"
    fi
  fi

  echo "[${i}] Done."
done

echo ""
echo "=========================================="
echo "  All ${N} Memorystore reshard runs complete."
echo "  Results in: ${LOCAL_OUT}/"
echo "  Analyse with:"
echo "    python cli.py reshard --input ${LOCAL_OUT} --output-dir ./plots/memorystore_reshard"
echo "=========================================="

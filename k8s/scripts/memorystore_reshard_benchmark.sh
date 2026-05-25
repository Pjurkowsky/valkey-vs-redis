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
STEADY_STATE_WAIT="${STEADY_STATE_WAIT:-30}"
OPERATION_SETTLE_WAIT="${OPERATION_SETTLE_WAIT:-30}"

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
        --ratio='${RATIO}' \
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
echo "N=${N}"
echo "TEST_TIME=${TEST_TIME}"
echo "ORIGINAL_SHARDS=${ORIGINAL_SHARDS}"
echo "TARGET_SHARDS=${TARGET_SHARDS}"

ensure_shard_count "${ORIGINAL_SHARDS}"

for i in $(seq 1 "${N}"); do
  echo ""
  echo "=========================================="
  echo "  Memorystore reshard run ${i}/${N}"
  echo "=========================================="

  UP_POD="memtier-ms-reshard-up-${i}"
  UP_OUT="reshard_run_${i}.json"
  UP_STATUS="success"

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

  echo "[${i}] Done."
done

echo ""
echo "=========================================="
echo "  All ${N} Memorystore reshard runs complete."
echo "  Results in: ${LOCAL_OUT}/"
echo "  Analyse with:"
echo "    python cli.py reshard --input ${LOCAL_OUT} --output-dir ./plots/memorystore_reshard"
echo "=========================================="

#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<EOF
Usage: $0 <memorystore-instance-id> [output-dir]

Black-box managed-failover benchmark for Google Cloud Memorystore for Redis
Cluster. Memorystore does not expose pod-level access, so an unplanned
PodChaos master kill (as used for the self-hosted variants) cannot be injected.
Instead this benchmark observes a managed, coordinated failover triggered by a
simulated maintenance event:

  1. Start the failover_client.py workload from a Kubernetes pod.
  2. Wait for a steady state window.
  3. Trigger a managed coordinated failover with
       gcloud redis clusters update <id> --region=<region> --simulate-maintenance-event
  4. Record the visible operation duration and the ready-state wait duration.
  5. Keep the workload running so client-visible impact is captured.

This is explicitly a PLANNED, coordinated failover (zero-downtime,
create-before-destroy across all nodes), not an unexpected primary crash. It is
therefore a managed black-box reference point and is not equivalent to the
self-hosted pod-failure test. The client report is written in the same format
as the self-hosted failover runs so it can be analysed with:
  python cli.py failover --input <output-dir> --output-dir ./plots/memorystore_failover

Defaults:
  MEMORYSTORE_PRODUCT=redis
  LOCATION=europe-central2
  N=5
  FAILOVER_TEST_TIME=300
  FAILOVER_STEADY_STATE_WAIT=30
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" || -z "${1:-}" ]]; then
  usage
  exit 0
fi

INSTANCE_ID="$1"
LOCAL_OUT="${2:-./results/memorystore_failover}"
REMOTE_OUT="/work/results/memorystore_failover"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

source "${SCRIPT_DIR}/pod_results.sh"

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null)}"
MEMORYSTORE_PRODUCT="${MEMORYSTORE_PRODUCT:-redis}"
LOCATION="${LOCATION:-europe-central2}"
NS="${NS:-vk}"
ARTIFACT_REPO="${ARTIFACT_REPO:-valkey-bench}"
IMAGE="${FAILOVER_IMAGE:-${CONSISTENCY_IMAGE:-${LOCATION}-docker.pkg.dev/${PROJECT_ID}/${ARTIFACT_REPO}/consistency_checker:2}}"
PROVIDER="memorystore_${MEMORYSTORE_PRODUCT}"
SYSTEM_NAME="${SYSTEM_NAME:-Memorystore}"

N="${N:-5}"
PORT="${MEMORYSTORE_PORT:-6379}"
THREADS="${FAILOVER_THREADS:-4}"
CLIENTS="${FAILOVER_CLIENTS:-16}"
TEST_TIME="${FAILOVER_TEST_TIME:-300}"
KEYS="${FAILOVER_KEYS:-100000}"
DATA_SIZE="${FAILOVER_DATA_SIZE:-1024}"
RATIO="${FAILOVER_RATIO:-1:1}"
STEADY_STATE_WAIT="${FAILOVER_STEADY_STATE_WAIT:-30}"
FAILOVER_SOCKET_TIMEOUT="${FAILOVER_SOCKET_TIMEOUT:-1.0}"
FAILOVER_CONNECT_TIMEOUT="${FAILOVER_CONNECT_TIMEOUT:-1.0}"
FAILOVER_RETRY_ON_TIMEOUT="${FAILOVER_RETRY_ON_TIMEOUT:-false}"
FAILOVER_RECONNECT_BACKOFF_S="${FAILOVER_RECONNECT_BACKOFF_S:-0.01}"
FAILOVER_LATENCY_SAMPLE_LIMIT_PER_SECOND="${FAILOVER_LATENCY_SAMPLE_LIMIT_PER_SECOND:-5000}"
FAILOVER_LATENCY_SAMPLE_LIMIT_TOTAL="${FAILOVER_LATENCY_SAMPLE_LIMIT_TOTAL:-200000}"
FAILOVER_ERROR_SAMPLE_LIMIT="${FAILOVER_ERROR_SAMPLE_LIMIT:-100}"
FAILOVER_RANDOM_SEED="${FAILOVER_RANDOM_SEED:-1}"
WORKLOAD_STARTED_FILE="${FAILOVER_STARTED_FILE:-/tmp/failover.started}"
INSTANCE_READY_TIMEOUT="${INSTANCE_READY_TIMEOUT:-1800}"

MEMTIER_TLS="${MEMTIER_TLS:-false}"
MEMTIER_TLS_SKIP_VERIFY="${MEMTIER_TLS_SKIP_VERIFY:-true}"
MEMTIER_TLS_CACERT="${MEMTIER_TLS_CACERT:-}"

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

PYTHON_CLIENT_RETRY_ARGS=(--no-retry-on-timeout)
case "${FAILOVER_RETRY_ON_TIMEOUT}" in
  1|true|TRUE|yes|YES|on|ON)
    PYTHON_CLIENT_RETRY_ARGS=(--retry-on-timeout)
    ;;
esac

PYTHON_CLIENT_TLS_ARGS=()
case "${MEMTIER_TLS}" in
  1|true|TRUE|yes|YES|on|ON)
    PYTHON_CLIENT_TLS_ARGS+=(--tls)
    case "${MEMTIER_TLS_SKIP_VERIFY}" in
      1|true|TRUE|yes|YES|on|ON)
        PYTHON_CLIENT_TLS_ARGS+=(--tls-skip-verify)
        ;;
    esac
    if [[ -n "${MEMTIER_TLS_CACERT}" ]]; then
      PYTHON_CLIENT_TLS_ARGS+=(--tls-ca-cert "${MEMTIER_TLS_CACERT}")
    fi
    ;;
esac

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

current_instance_state() {
  describe_instance_json | "${PYTHON_BIN}" -c '
import json, sys
doc = json.load(sys.stdin)
print(doc.get("state", ""))
'
}

state_is_ready() {
  case "$1" in
    ""|ACTIVE|READY) return 0 ;;
    *) return 1 ;;
  esac
}

instance_replica_count() {
  describe_instance_json | "${PYTHON_BIN}" -c '
import json, sys
doc = json.load(sys.stdin)
print(doc.get("replicaCount", doc.get("replicasPerNode", "")))
'
}

wait_for_instance_ready() {
  local timeout="${1:-${INSTANCE_READY_TIMEOUT}}"
  local deadline=$((SECONDS + timeout))
  local state

  echo "  Waiting for Memorystore ready state (max ${timeout}s)..."
  while (( SECONDS < deadline )); do
    state="$(current_instance_state || true)"
    if state_is_ready "${state}"; then
      return 0
    fi
    echo "  Current state=${state:-unknown}; waiting..."
    sleep 10
  done

  echo "ERROR: Memorystore did not return to ready state within ${timeout}s" >&2
  return 1
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

trigger_simulated_maintenance() {
  local run_idx="$1"
  local log_file="${LOCAL_OUT}/memorystore_simulate_maintenance_${run_idx}.log"
  local -a cmd

  if [[ "${MEMORYSTORE_PRODUCT}" == "redis" ]]; then
    cmd=(
      gcloud redis clusters update "${INSTANCE_ID}"
      --project="${PROJECT_ID}"
      --region="${LOCATION}"
      --simulate-maintenance-event
      --quiet
    )
  else
    cmd=(
      gcloud memorystore instances update "${INSTANCE_ID}"
      --project="${PROJECT_ID}"
      --location="${LOCATION}"
      --simulate-maintenance-event
      --quiet
    )
  fi

  echo "  Triggering simulated maintenance event on ${INSTANCE_ID}..."
  if "${cmd[@]}" >"${log_file}" 2>&1; then
    echo "  Simulated maintenance command returned. Log: ${log_file}"
    return 0
  fi

  echo "ERROR: simulated maintenance command failed. Log: ${log_file}" >&2
  sed 's/^/  /' "${log_file}" >&2 || true
  return 1
}

write_timing() {
  local run_idx="$1"
  local workload_started_epoch_s="$2"
  local chaos_epoch_s="$3"
  local op_command_done_epoch_s="$4"
  local op_ready_epoch_s="$5"
  local status="$6"
  local replicas="$7"
  local timing_file="${LOCAL_OUT}/failover_timing_${run_idx}.json"
  local op_duration_s ready_wait_s total_op_s

  op_duration_s=$((op_command_done_epoch_s - chaos_epoch_s))
  ready_wait_s=$((op_ready_epoch_s - op_command_done_epoch_s))
  total_op_s=$((op_ready_epoch_s - chaos_epoch_s))

  cat >"${timing_file}" <<EOF
{
  "variant": "memorystore_simulated_maintenance_failover",
  "client_engine": "python",
  "provider": "${PROVIDER}",
  "system": "${SYSTEM_NAME}",
  "instance": "${INSTANCE_ID}",
  "location": "${LOCATION}",
  "failover_kind": "planned_coordinated_maintenance",
  "operation_status": "${status}",
  "replica_count": ${replicas:-null},
  "workload_started_epoch_s": ${workload_started_epoch_s:-null},
  "memtier_started_epoch_s": ${workload_started_epoch_s:-null},
  "chaos_epoch_s": ${chaos_epoch_s},
  "steady_state_wait_s": ${STEADY_STATE_WAIT},
  "action": "simulate-maintenance-event",
  "target": "managed coordinated failover (simulate-maintenance-event) on ${INSTANCE_ID}",
  "duration": null,
  "managed_operation_duration_s": ${op_duration_s},
  "ready_state_wait_duration_s": ${ready_wait_s},
  "managed_failover_total_duration_s": ${total_op_s},
  "threads": ${THREADS},
  "clients": ${CLIENTS}
}
EOF
  echo "  Timing written: ${timing_file} (managed op ${op_duration_s}s, ready wait ${ready_wait_s}s)"
}

read -r DISCOVERY_HOST DISCOVERY_PORT < <(discover_endpoint)
if [[ -z "${DISCOVERY_HOST:-}" ]]; then
  echo "ERROR: Could not discover Memorystore endpoint for ${INSTANCE_ID}." >&2
  exit 1
fi
if [[ -n "${DISCOVERY_PORT:-}" ]]; then
  PORT="${DISCOVERY_PORT}"
fi

REPLICA_COUNT="$(instance_replica_count || true)"

echo "==> Memorystore managed-failover benchmark"
echo "PROJECT_ID=${PROJECT_ID}"
echo "MEMORYSTORE_PRODUCT=${MEMORYSTORE_PRODUCT}"
echo "PROVIDER=${PROVIDER}"
echo "SYSTEM_NAME=${SYSTEM_NAME}"
echo "LOCATION=${LOCATION}"
echo "INSTANCE_ID=${INSTANCE_ID}"
echo "DISCOVERY_ENDPOINT=${DISCOVERY_HOST}:${PORT}"
echo "REPLICA_COUNT=${REPLICA_COUNT:-unknown}"
echo "NS=${NS}"
echo "IMAGE=${IMAGE}"
echo "LOCAL_OUT=${LOCAL_OUT}"
echo "REMOTE_OUT=${REMOTE_OUT}"
echo "N=${N}"
echo "FAILOVER_TEST_TIME=${TEST_TIME}"
echo "FAILOVER_STEADY_STATE_WAIT=${STEADY_STATE_WAIT}"
echo "FAILOVER_THREADS=${THREADS}"
echo "FAILOVER_CLIENTS=${CLIENTS}"
echo "FAILOVER_KEYS=${KEYS}"
echo "FAILOVER_DATA_SIZE=${DATA_SIZE}"
echo "FAILOVER_RATIO=${RATIO}"
echo "MEMTIER_TLS=${MEMTIER_TLS}"
echo "INSTANCE_READY_TIMEOUT=${INSTANCE_READY_TIMEOUT}"

if [[ "${REPLICA_COUNT}" == "0" ]]; then
  echo "WARN: Memorystore instance reports 0 replicas; a coordinated failover requires at least 1 replica per shard." >&2
fi

for i in $(seq 1 "${N}"); do
  POD_NAME="ms-failover-client-${i}"
  OUT_FILE="failover_run_${i}.json"
  LOG_FILE="failover_run_${i}.log"

  echo ""
  echo "=========================================="
  echo "  Memorystore managed-failover run ${i}/${N}"
  echo "=========================================="

  echo "[${i}] Waiting for Memorystore ready state before run..."
  wait_for_instance_ready "${INSTANCE_READY_TIMEOUT}"

  kubectl delete pod "${POD_NAME}" -n "${NS}" --ignore-not-found >/dev/null 2>&1 || true

  POD_COMMAND="$(cat <<EOF
mkdir -p '${REMOTE_OUT}'
rm -f '${WORKLOAD_STARTED_FILE}'
python -u /work/failover_client.py \
  --host '${DISCOVERY_HOST}' --port '${PORT}' \
  --duration '${TEST_TIME}' \
  --output '${REMOTE_OUT}/${OUT_FILE}' \
  --started-file '${WORKLOAD_STARTED_FILE}' \
  --provider '${PROVIDER}' \
  --system '${SYSTEM_NAME}' \
  --threads '${THREADS}' \
  --clients '${CLIENTS}' \
  --ratio '${RATIO}' \
  --keys '${KEYS}' \
  --data-size '${DATA_SIZE}' \
  --key-prefix 'failover.${PROVIDER}.${i}.' \
  --socket-timeout '${FAILOVER_SOCKET_TIMEOUT}' \
  --connect-timeout '${FAILOVER_CONNECT_TIMEOUT}' \
  --reconnect-backoff-s '${FAILOVER_RECONNECT_BACKOFF_S}' \
  --latency-sample-limit-per-second '${FAILOVER_LATENCY_SAMPLE_LIMIT_PER_SECOND}' \
  --latency-sample-limit-total '${FAILOVER_LATENCY_SAMPLE_LIMIT_TOTAL}' \
  --error-sample-limit '${FAILOVER_ERROR_SAMPLE_LIMIT}' \
  --seed '$((FAILOVER_RANDOM_SEED + i))' \
  ${PYTHON_CLIENT_RETRY_ARGS[*]} \
  ${PYTHON_CLIENT_TLS_ARGS[*]} \
  > '${REMOTE_OUT}/${LOG_FILE}' 2>&1
status=\$?
echo "\$status" > '${POD_EXIT_CODE_FILE}'
touch '${POD_DONE_FILE}'
sleep '${POD_HOLD_SECONDS}'
EOF
)"

  echo "[${i}] Starting failover client pod (duration=${TEST_TIME}s)..."
  kubectl run "${POD_NAME}" -n "${NS}" \
    --image="${IMAGE}" \
    --restart=Never \
    --command -- \
    /bin/sh -c "${POD_COMMAND}"

  echo "[${i}] Waiting for pod to start..."
  kubectl wait pod/"${POD_NAME}" -n "${NS}" \
    --for=condition=Ready --timeout=90s 2>/dev/null || true

  echo "[${i}] Waiting for workload to start..."
  if ! wait_for_pod_marker "${NS}" "${POD_NAME}" "${WORKLOAD_STARTED_FILE}" 120; then
    echo "[${i}] ERROR: failover client did not start its timed run."
    print_pod_debug_info "${NS}" "${POD_NAME}"
    exit 1
  fi
  WORKLOAD_STARTED_EPOCH_S="$(kubectl exec "${POD_NAME}" -n "${NS}" -- cat "${WORKLOAD_STARTED_FILE}" 2>/dev/null | tr -d '[:space:]')"

  echo "[${i}] Waiting ${STEADY_STATE_WAIT}s for steady state..."
  sleep "${STEADY_STATE_WAIT}"

  STATUS="success"
  CHAOS_EPOCH_S="$(date +%s)"
  if ! trigger_simulated_maintenance "${i}"; then
    STATUS="failed"
  fi
  OP_COMMAND_DONE_EPOCH_S="$(date +%s)"

  if ! wait_for_instance_ready "${INSTANCE_READY_TIMEOUT}"; then
    STATUS="failed"
  fi
  OP_READY_EPOCH_S="$(date +%s)"

  write_timing "${i}" "${WORKLOAD_STARTED_EPOCH_S}" "${CHAOS_EPOCH_S}" \
    "${OP_COMMAND_DONE_EPOCH_S}" "${OP_READY_EPOCH_S}" "${STATUS}" "${REPLICA_COUNT}"

  if (( OP_READY_EPOCH_S - WORKLOAD_STARTED_EPOCH_S >= TEST_TIME )); then
    echo "[${i}] WARN: managed failover outlasted the client window (${TEST_TIME}s)." >&2
    echo "[${i}] WARN: increase FAILOVER_TEST_TIME so the workload covers the whole operation." >&2
  fi

  echo "[${i}] Waiting for failover client to finish..."
  if ! wait_for_pod_marker "${NS}" "${POD_NAME}" "${POD_DONE_FILE}" $((TEST_TIME + 600)); then
    echo "[${i}] ERROR: failover client pod did not signal completion."
    print_pod_debug_info "${NS}" "${POD_NAME}"
    exit 1
  fi

  exit_code="$(read_pod_exit_code "${NS}" "${POD_NAME}" "${POD_EXIT_CODE_FILE}")"
  if [[ -z "${exit_code}" || "${exit_code}" != "0" ]]; then
    echo "[${i}] ERROR: failover client exited with code ${exit_code:-unknown}."
    print_pod_debug_info "${NS}" "${POD_NAME}"
    exit 1
  fi

  echo "[${i}] Copying results..."
  kubectl cp "${NS}/${POD_NAME}:${REMOTE_OUT}/${OUT_FILE}" "${LOCAL_OUT}/${OUT_FILE}"
  kubectl cp "${NS}/${POD_NAME}:${REMOTE_OUT}/${LOG_FILE}" "${LOCAL_OUT}/${LOG_FILE}" || \
    echo "[${i}] WARN: could not copy client log ${LOG_FILE}"

  kubectl delete pod "${POD_NAME}" -n "${NS}" --ignore-not-found >/dev/null

  if [[ "${STATUS}" != "success" ]]; then
    echo "[${i}] ERROR: managed failover operation did not complete successfully." >&2
    exit 1
  fi

  echo "[${i}] Done. Result: ${LOCAL_OUT}/${OUT_FILE}"
done

echo ""
echo "=========================================="
echo "  All ${N} Memorystore managed-failover runs complete."
echo "  Results in: ${LOCAL_OUT}/"
echo "  Analyse with:"
echo "    python cli.py failover --input ${LOCAL_OUT} --output-dir ./plots/memorystore_failover"
echo "=========================================="

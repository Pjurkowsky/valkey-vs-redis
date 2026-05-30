#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<EOF
Usage: $0 [target-mb] [output-dir]

Pre-fills cluster to maxmemory, then measures performance with memtier_benchmark.

Phase 1: Fill the cluster to TARGET_MB (as fast as possible, --allow-partial).
Phase 2: Run memtier_benchmark on the full cluster (TEST_TIME seconds).

Environment:
  PROVIDER=valkey|memorystore        default: valkey
  MEMORYSTORE_CLUSTER_ID=redis-ms-2  required when PROVIDER=memorystore
  PROJECT_ID=<gcloud project>
  LOCATION=europe-central2
  MAXMEMORY_POLICY=allkeys-lru       configured when CONFIGURE_POLICY=true
  MEMORYSTORE_MAXMEMORY=<bytes>      optional managed config update
  CONFIGURE_POLICY=true
  FLUSH_BETWEEN_RUNS=true
  N=1
  TEST_TIME=120
  MEMTIER_IMAGE=.../memtier_k8s:1
  BACKUP_IMAGE=.../backup_restore:1
  REDIS_CLI_IMAGE=redis:7.2
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

TARGET_MB="${1:-4096}"
LOCAL_OUT="${2:-./results/maxmemory_resilience}"
N="${N:-1}"
NS="${NS:-vk}"
PROVIDER="${PROVIDER:-valkey}"
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
REDIS_CLI_IMAGE="${REDIS_CLI_IMAGE:-redis:7.2}"
REMOTE_OUT="/work/results/maxmemory_resilience"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

source "${SCRIPT_DIR}/pod_results.sh"

HOST="${HOST:-valkey.vk.svc.cluster.local}"
PORT="${PORT:-6379}"
MEMORYSTORE_CLUSTER_ID="${MEMORYSTORE_CLUSTER_ID:-${MEMORYSTORE_CLUSTER:-}}"
MAXMEMORY_POLICY="${MAXMEMORY_POLICY:-allkeys-lru}"
CONFIGURE_POLICY="${CONFIGURE_POLICY:-true}"
FLUSH_BETWEEN_RUNS="${FLUSH_BETWEEN_RUNS:-true}"
MEMORYSTORE_MAXMEMORY="${MEMORYSTORE_MAXMEMORY:-}"
THREADS="${THREADS:-4}"
CLIENTS="${CLIENTS:-16}"
TEST_TIME="${TEST_TIME:-120}"
KEYS="${KEYS:-100000}"
DATA_SIZE="${DATA_SIZE:-1024}"
RATIO="${RATIO:-1:1}"

mkdir -p "${LOCAL_OUT}"

PYTHON_BIN="$(command -v python3 || command -v python || true)"
if [[ -z "${PYTHON_BIN}" ]]; then
  echo "ERROR: python3 or python is required." >&2
  exit 1
fi

if [[ "${PROVIDER}" != "valkey" && "${PROVIDER}" != "memorystore" ]]; then
  echo "ERROR: PROVIDER must be valkey or memorystore." >&2
  exit 1
fi

if [[ "${PROVIDER}" == "memorystore" ]]; then
  if [[ -z "${MEMORYSTORE_CLUSTER_ID}" ]]; then
    echo "ERROR: Set MEMORYSTORE_CLUSTER_ID when PROVIDER=memorystore." >&2
    exit 1
  fi
  if [[ -z "${PROJECT_ID}" || "${PROJECT_ID}" == "(unset)" ]]; then
    echo "ERROR: Could not determine GCP project. Set PROJECT_ID." >&2
    exit 1
  fi
fi

json_string() {
  "${PYTHON_BIN}" -c 'import json, sys; print(json.dumps(sys.argv[1]))' "$1"
}

describe_ms_cluster_json() {
  gcloud redis clusters describe "${MEMORYSTORE_CLUSTER_ID}" \
    --project="${PROJECT_ID}" \
    --region="${LOCATION}" \
    --format=json
}

ms_cluster_state() {
  describe_ms_cluster_json | "${PYTHON_BIN}" -c "import json,sys; print(json.load(sys.stdin).get('state', ''))"
}

ms_state_is_ready() {
  case "$1" in
    ""|ACTIVE|READY)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

wait_for_ms_cluster_ready() {
  local timeout="${1:-1800}"
  local deadline=$((SECONDS + timeout))
  local state

  echo "  Waiting for ${MEMORYSTORE_CLUSTER_ID} ready state (max ${timeout}s)..."
  while (( SECONDS < deadline )); do
    state="$(ms_cluster_state || true)"
    if ms_state_is_ready "${state}"; then
      echo "  ${MEMORYSTORE_CLUSTER_ID} ready; state=${state:-unknown}"
      return 0
    fi
    echo "  ${MEMORYSTORE_CLUSTER_ID} state=${state:-unknown}; waiting..."
    sleep 15
  done

  echo "ERROR: ${MEMORYSTORE_CLUSTER_ID} did not become ready within ${timeout}s" >&2
  return 1
}

discover_ms_endpoint() {
  describe_ms_cluster_json | "${PYTHON_BIN}" -c '
import json, sys
doc = json.load(sys.stdin)
for endpoint in doc.get("discoveryEndpoints") or []:
    address = endpoint.get("address")
    port = endpoint.get("port") or 6379
    if address:
        print(f"{address} {port}")
        raise SystemExit(0)
raise SystemExit("Could not find Memorystore discovery endpoint")
'
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
  local timeout_s="${2:-90}"

  kubectl wait pod/"${pod_name}" -n "${NS}" \
    --for=condition=Ready --timeout="${timeout_s}s"
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
    --restart=Never \
    --command -- \
    /bin/sh -c "${shell_body}"

  wait_for_command_pod "${pod_name}" "${timeout_s}"
}

flush_cluster() {
  local pod_name="$1"

  kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl run "${pod_name}" -n "${NS}" \
    --image="${REDIS_CLI_IMAGE}" \
    --restart=Never \
    --command -- \
    /bin/sh -c "
      status=0
      nodes=\$(redis-cli -h '${HOST}' -p '${PORT}' cluster nodes 2>&1) || status=\$?
      if [ \"\${status}\" -eq 0 ]; then
        masters=\$(printf '%s\n' \"\${nodes}\" | awk '\$3 ~ /master/ && \$3 !~ /fail/ {print \$2}')
        if [ -z \"\${masters}\" ]; then
          echo 'ERROR: no master nodes discovered from CLUSTER NODES' >&2
          status=1
        else
          for endpoint in \${masters}; do
            addr=\${endpoint%%@*}
            node_host=\${addr%:*}
            node_port=\${addr##*:}
            echo \"FLUSHALL \${node_host}:\${node_port}\"
            redis-cli -h \"\${node_host}\" -p \"\${node_port}\" flushall sync || redis-cli -h \"\${node_host}\" -p \"\${node_port}\" flushall || status=1
            redis-cli -h \"\${node_host}\" -p \"\${node_port}\" config resetstat >/dev/null 2>&1 || true
            dbsize=unknown
            for attempt in \$(seq 1 60); do
              dbsize=\$(redis-cli -h \"\${node_host}\" -p \"\${node_port}\" dbsize 2>/dev/null || echo unknown)
              if [ \"\${dbsize}\" = '0' ]; then
                break
              fi
              sleep 1
            done
            if [ \"\${dbsize}\" != '0' ]; then
              echo \"ERROR: \${node_host}:\${node_port} still has \${dbsize} keys after FLUSHALL\" >&2
              status=1
            fi
          done
        fi
      else
        printf '%s\n' \"\${nodes}\" >&2
      fi
      echo \"\${status}\" > '${POD_EXIT_CODE_FILE}'
      touch '${POD_DONE_FILE}'
      sleep '${POD_HOLD_SECONDS}'
      exit \"\${status}\"
    "

  wait_for_command_pod "${pod_name}" 1800
  kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found >/dev/null
}

configure_valkey_policy() {
  local pod_name="$1"

  run_tool_pod "${pod_name}" 300 "$(tool_shell_prefix)
python /work/backup_restore_seed.py \
  --mode configure \
  --host '${HOST}' --port '${PORT}' \
  --maxmemory-policy '${MAXMEMORY_POLICY}'
status=\$?
echo \"\$status\" > '${POD_EXIT_CODE_FILE}'
touch '${POD_DONE_FILE}'
sleep '${POD_HOLD_SECONDS}'
"
  kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found >/dev/null
}

configure_memorystore_policy() {
  local update_config="maxmemory-policy=${MAXMEMORY_POLICY}"

  if [[ -n "${MEMORYSTORE_MAXMEMORY}" ]]; then
    update_config="${update_config},maxmemory=${MEMORYSTORE_MAXMEMORY}"
  fi

  gcloud redis clusters update "${MEMORYSTORE_CLUSTER_ID}" \
    --project="${PROJECT_ID}" \
    --region="${LOCATION}" \
    --update-redis-config="${update_config}" \
    --quiet
  wait_for_ms_cluster_ready 1800
}

if [[ "${PROVIDER}" == "memorystore" ]]; then
  wait_for_ms_cluster_ready 900
  read -r HOST PORT < <(discover_ms_endpoint)
fi

echo "==> Maxmemory resilience benchmark (pre-fill then measure)"
echo "PROVIDER=${PROVIDER}"
echo "HOST=${HOST}"
echo "PORT=${PORT}"
echo "TARGET_MB=${TARGET_MB}"
echo "MAXMEMORY_POLICY=${MAXMEMORY_POLICY}"
echo "CONFIGURE_POLICY=${CONFIGURE_POLICY}"
echo "FLUSH_BETWEEN_RUNS=${FLUSH_BETWEEN_RUNS}"
echo "TEST_TIME=${TEST_TIME}"
echo "N=${N}"

if [[ "${CONFIGURE_POLICY}" == "true" ]]; then
  echo "Configuring ${PROVIDER} maxmemory-policy=${MAXMEMORY_POLICY}..."
  if [[ "${PROVIDER}" == "valkey" ]]; then
    configure_valkey_policy "maxmemory-resilience-config"
  else
    configure_memorystore_policy
  fi
fi

for i in $(seq 1 "${N}"); do
  MEMTIER_POD="memtier-maxmemory-${i}"
  PREFILL_POD="maxmemory-prefill-${i}"
  CLEANUP_POD="maxmemory-pressure-cleanup-${i}"
  FLUSH_POD="maxmemory-pressure-flush-${i}"
  RUN_ID="maxmem_res_${PROVIDER}_${TARGET_MB}mb_${i}_$(date +%s)"
  MEMTIER_FILE="resilience_maxmemory_run_${i}.json"
  SEED_REPORT="maxmemory_pressure_seed_${i}.json"
  TIMING_FILE="maxmemory_resilience_timing_${i}.json"

  echo ""
  echo "=========================================="
  echo "  Maxmemory resilience run ${i}/${N} (${TARGET_MB} MB pressure)"
  echo "=========================================="

  kubectl delete pod "${MEMTIER_POD}" "${PREFILL_POD}" "${CLEANUP_POD}" "${FLUSH_POD}" \
    -n "${NS}" --ignore-not-found 2>/dev/null || true

  if [[ "${FLUSH_BETWEEN_RUNS}" == "true" ]]; then
    echo "[${i}] Flushing existing keys and resetting stats..."
    flush_cluster "${FLUSH_POD}"
  fi

  echo "[${i}] Pre-filling cluster to maxmemory (${TARGET_MB} MB)..."
  PREFILL_START_EPOCH="$(date +%s)"
  kubectl run "${PREFILL_POD}" -n "${NS}" \
    --image="${BACKUP_IMAGE}" \
    --restart=Never \
    --command -- \
    /bin/sh -c "
      mkdir -p '${REMOTE_OUT}'
      python /work/backup_restore_seed.py \
        --mode seed \
        --host '${HOST}' --port '${PORT}' \
        --target-mb '${TARGET_MB}' \
        --run-id '${RUN_ID}' \
        --allow-partial \
        --stop-after-errors 50 \
        --output '${REMOTE_OUT}/${SEED_REPORT}'
      status=\$?
      echo \"\$status\" > '${POD_EXIT_CODE_FILE}'
      touch '${POD_DONE_FILE}'
      sleep '${POD_HOLD_SECONDS}'
    "

  wait_for_command_pod "${PREFILL_POD}" 7200
  PREFILL_END_EPOCH="$(date +%s)"
  kubectl cp "${NS}/${PREFILL_POD}:${REMOTE_OUT}/${SEED_REPORT}" "${LOCAL_OUT}/${SEED_REPORT}"
  kubectl delete pod "${PREFILL_POD}" -n "${NS}" --ignore-not-found

  prefill_duration="$("${PYTHON_BIN}" -c "import json; print(json.load(open('${LOCAL_OUT}/${SEED_REPORT}'))['seed_duration_s'])")"
  prefill_keys="$("${PYTHON_BIN}" -c "import json; print(json.load(open('${LOCAL_OUT}/${SEED_REPORT}'))['written_keys'])")"
  echo "[${i}] Pre-fill done: ${prefill_keys} keys in ${prefill_duration}s"

  echo "[${i}] Starting memtier benchmark (test-time=${TEST_TIME}s) on full cluster..."
  kubectl run "${MEMTIER_POD}" -n "${NS}" \
    --image="${MEMTIER_IMAGE}" \
    --restart=Never \
    --command -- \
    /bin/sh -c "
      mkdir -p '${REMOTE_OUT}'
      memtier_benchmark \
        --server='${HOST}' --port='${PORT}' \
        --protocol=redis \
        --cluster-mode \
        --threads='${THREADS}' --clients='${CLIENTS}' \
        --test-time='${TEST_TIME}' \
        --key-maximum='${KEYS}' \
        --data-size='${DATA_SIZE}' \
        --ratio='${RATIO}' \
        --json-out-file '${REMOTE_OUT}/${MEMTIER_FILE}' \
        --run-count 1 \
        --print-percentiles='50,95,99,99.9'
      status=\$?
      echo \"\$status\" > '${POD_EXIT_CODE_FILE}'
      touch '${POD_DONE_FILE}'
      sleep '${POD_HOLD_SECONDS}'
    "

  echo "[${i}] Waiting for memtier to finish..."
  wait_for_command_pod "${MEMTIER_POD}" $((TEST_TIME + 300))
  kubectl cp "${NS}/${MEMTIER_POD}:${REMOTE_OUT}/${MEMTIER_FILE}" "${LOCAL_OUT}/${MEMTIER_FILE}"
  kubectl delete pod "${MEMTIER_POD}" -n "${NS}" --ignore-not-found

  cat > "${LOCAL_OUT}/${TIMING_FILE}" <<EOF
{
  "run": ${i},
  "provider": $(json_string "${PROVIDER}"),
  "run_id": "${RUN_ID}",
  "target_mb": ${TARGET_MB},
  "maxmemory_policy": $(json_string "${MAXMEMORY_POLICY}"),
  "prefill_start_epoch_s": ${PREFILL_START_EPOCH},
  "prefill_end_epoch_s": ${PREFILL_END_EPOCH},
  "prefill_duration_s": ${prefill_duration},
  "prefill_keys": ${prefill_keys},
  "test_time_s": ${TEST_TIME},
  "memtier_file": "${MEMTIER_FILE}",
  "seed_report": "${SEED_REPORT}"
}
EOF

  echo "[${i}] Cleaning up surviving pressure keys..."
  kubectl run "${CLEANUP_POD}" -n "${NS}" \
    --image="${BACKUP_IMAGE}" \
    --restart=Never \
    --command -- \
    /bin/sh -c "
      mkdir -p '${REMOTE_OUT}'
      while [ ! -f '${REMOTE_OUT}/${SEED_REPORT}' ]; do sleep 1; done
      python /work/backup_restore_seed.py \
        --mode cleanup \
        --host '${HOST}' --port '${PORT}' \
        --seed-report '${REMOTE_OUT}/${SEED_REPORT}'
      status=\$?
      echo \"\$status\" > '${POD_EXIT_CODE_FILE}'
      touch '${POD_DONE_FILE}'
      sleep '${POD_HOLD_SECONDS}'
    "

  kubectl wait pod/"${CLEANUP_POD}" -n "${NS}" \
    --for=condition=Ready --timeout=60s
  kubectl cp "${LOCAL_OUT}/${SEED_REPORT}" "${NS}/${CLEANUP_POD}:${REMOTE_OUT}/${SEED_REPORT}"
  wait_for_command_pod "${CLEANUP_POD}" 1800 || true
  kubectl delete pod "${CLEANUP_POD}" -n "${NS}" --ignore-not-found

  echo "[${i}] Done. Result: ${LOCAL_OUT}/${MEMTIER_FILE}"
done

echo ""
echo "=========================================="
echo "  All ${N} maxmemory resilience runs complete."
echo "  Results in: ${LOCAL_OUT}/"
echo "  Analyse with:"
echo "    python cli.py resilience --input ${LOCAL_OUT} --scenario maxmemory --output-dir ./plots/maxmemory_resilience"
echo "=========================================="

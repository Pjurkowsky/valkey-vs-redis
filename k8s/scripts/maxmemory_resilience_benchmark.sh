#!/usr/bin/env bash
set -euo pipefail

TARGET_MB="${1:-4096}"
LOCAL_OUT="${2:-./results/maxmemory_resilience}"
N="${N:-1}"
NS="vk"
MEMTIER_IMAGE="${MEMTIER_IMAGE:-memtier_k8s:1}"
BACKUP_IMAGE="${BACKUP_IMAGE:-backup_restore:1}"
REMOTE_OUT="/work/results/maxmemory_resilience"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

source "${SCRIPT_DIR}/target_config.sh"
source "${SCRIPT_DIR}/pod_results.sh"

HOST="${TC_HOST}"
PORT="${TC_PORT}"
THREADS="${THREADS:-4}"
CLIENTS="${CLIENTS:-16}"
TEST_TIME="${TEST_TIME:-120}"
KEYS="${KEYS:-100000}"
DATA_SIZE="${DATA_SIZE:-1024}"
RATIO="${RATIO:-1:1}"
STEADY_STATE_WAIT="${STEADY_STATE_WAIT:-30}"

mkdir -p "${LOCAL_OUT}"

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

for i in $(seq 1 "${N}"); do
  MEMTIER_POD="memtier-maxmemory-${i}"
  SEED_POD="maxmemory-pressure-${i}"
  CLEANUP_POD="maxmemory-pressure-cleanup-${i}"
  RUN_ID="maxmem_res_${TARGET_MB}mb_${i}_$(date +%s)"
  MEMTIER_FILE="resilience_maxmemory_run_${i}.json"
  SEED_REPORT="maxmemory_pressure_seed_${i}.json"
  TIMING_FILE="maxmemory_resilience_timing_${i}.json"

  echo ""
  echo "=========================================="
  echo "  Maxmemory resilience run ${i}/${N} (${TARGET_MB} MB pressure, target=${TARGET})"
  echo "=========================================="

  kubectl delete pod "${MEMTIER_POD}" "${SEED_POD}" "${CLEANUP_POD}" \
    -n "${NS}" --ignore-not-found 2>/dev/null || true

  echo "[${i}] Starting memtier pod (test-time=${TEST_TIME}s)..."
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

  echo "[${i}] Waiting for memtier to start..."
  kubectl wait pod/"${MEMTIER_POD}" -n "${NS}" \
    --for=condition=Ready --timeout=60s

  echo "[${i}] Waiting ${STEADY_STATE_WAIT}s for steady state..."
  sleep "${STEADY_STATE_WAIT}"

  echo "[${i}] Starting maxmemory pressure writer (${TARGET_MB} MB)..."
  PRESSURE_START_EPOCH="$(date +%s)"
  kubectl run "${SEED_POD}" -n "${NS}" \
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
        --output '${REMOTE_OUT}/${SEED_REPORT}'
      status=\$?
      echo \"\$status\" > '${POD_EXIT_CODE_FILE}'
      touch '${POD_DONE_FILE}'
      sleep '${POD_HOLD_SECONDS}'
    "

  wait_for_command_pod "${SEED_POD}" 3600
  PRESSURE_END_EPOCH="$(date +%s)"
  kubectl cp "${NS}/${SEED_POD}:${REMOTE_OUT}/${SEED_REPORT}" "${LOCAL_OUT}/${SEED_REPORT}"
  kubectl delete pod "${SEED_POD}" -n "${NS}" --ignore-not-found

  echo "[${i}] Waiting for memtier to finish..."
  wait_for_command_pod "${MEMTIER_POD}" 600
  kubectl cp "${NS}/${MEMTIER_POD}:${REMOTE_OUT}/${MEMTIER_FILE}" "${LOCAL_OUT}/${MEMTIER_FILE}"
  kubectl delete pod "${MEMTIER_POD}" -n "${NS}" --ignore-not-found

  seed_duration="$(python3 -c "import json; print(json.load(open('${LOCAL_OUT}/${SEED_REPORT}'))['seed_duration_s'])")"
  seed_keys="$(python3 -c "import json; print(json.load(open('${LOCAL_OUT}/${SEED_REPORT}'))['written_keys'])")"

  cat > "${LOCAL_OUT}/${TIMING_FILE}" <<EOF
{
  "run": ${i},
  "run_id": "${RUN_ID}",
  "target_mb": ${TARGET_MB},
  "steady_state_wait_s": ${STEADY_STATE_WAIT},
  "pressure_start_epoch_s": ${PRESSURE_START_EPOCH},
  "pressure_end_epoch_s": ${PRESSURE_END_EPOCH},
  "pressure_duration_s": ${seed_duration},
  "written_keys": ${seed_keys},
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

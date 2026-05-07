#!/usr/bin/env bash
set -euo pipefail

SCENARIO="${1:?Usage: $0 <cpu|memory> [output_dir]}"
LOCAL_OUT="${2:-./results/resilience}"

N="${N:-5}"
NS="vk"
IMAGE="${MEMTIER_IMAGE:-memtier_k8s:1}"
REMOTE_OUT="/work/results/resilience"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

source "${SCRIPT_DIR}/pod_results.sh"

HOST="valkey.vk.svc.cluster.local"
PORT=6379
THREADS=4
CLIENTS=16
TEST_TIME=120
KEYS=100000
DATA_SIZE=1024
RATIO="1:1"
STEADY_STATE_WAIT=30

case "${SCENARIO}" in
  cpu)
    CHAOS_YAML="${SCRIPT_DIR}/../chaos/stress-cpu.yaml"
    CHAOS_NAME="valkey-cpu-stress"
    CHAOS_KIND="stresschaos"
    FILE_PREFIX="resilience_cpu"
    ;;
  memory)
    CHAOS_YAML="${SCRIPT_DIR}/../chaos/stress-memory.yaml"
    CHAOS_NAME="valkey-memory-stress"
    CHAOS_KIND="stresschaos"
    FILE_PREFIX="resilience_mem"
    ;;
  *)
    echo "ERROR: Unknown scenario '${SCENARIO}'. Use 'cpu' or 'memory'."
    exit 1
    ;;
esac

mkdir -p "${LOCAL_OUT}"

for i in $(seq 1 "${N}"); do
  POD_NAME="memtier-resilience-${i}"
  OUT_FILE="${FILE_PREFIX}_run_${i}.json"
  echo ""
  echo "=========================================="
  echo "  Resilience [${SCENARIO}] run ${i}/${N}"
  echo "=========================================="

  kubectl delete "${CHAOS_KIND}" "${CHAOS_NAME}" -n "${NS}" --ignore-not-found 2>/dev/null || true
  kubectl delete pod "${POD_NAME}" -n "${NS}" --ignore-not-found 2>/dev/null || true

  echo "[${i}] Starting memtier pod (test-time=${TEST_TIME}s)..."
  kubectl run "${POD_NAME}" -n "${NS}" \
    --image="${IMAGE}" \
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
        --json-out-file '${REMOTE_OUT}/${OUT_FILE}' \
        --run-count 1 \
        --print-percentiles='50,95,99,99.9'
      status=\$?
      echo \"\$status\" > '${POD_EXIT_CODE_FILE}'
      touch '${POD_DONE_FILE}'
      sleep '${POD_HOLD_SECONDS}'
    "

  echo "[${i}] Waiting for pod to start..."
  kubectl wait pod/"${POD_NAME}" -n "${NS}" \
    --for=condition=Ready --timeout=60s 2>/dev/null || true

  echo "[${i}] Waiting ${STEADY_STATE_WAIT}s for steady state..."
  sleep "${STEADY_STATE_WAIT}"

  echo "[${i}] Injecting stress: ${SCENARIO}..."
  kubectl apply -f "${CHAOS_YAML}"

  echo "[${i}] Waiting for memtier to finish..."
  if ! wait_for_pod_marker "${NS}" "${POD_NAME}" "${POD_DONE_FILE}" 300; then
    echo "[${i}] ERROR: memtier pod did not signal completion."
    print_pod_debug_info "${NS}" "${POD_NAME}"
    exit 1
  fi

  exit_code="$(read_pod_exit_code "${NS}" "${POD_NAME}" "${POD_EXIT_CODE_FILE}")"
  if [[ -z "${exit_code}" || "${exit_code}" != "0" ]]; then
    echo "[${i}] ERROR: memtier exited with code ${exit_code:-unknown}."
    print_pod_debug_info "${NS}" "${POD_NAME}"
    exit 1
  fi

  echo "[${i}] Copying results..."
  kubectl cp "${NS}/${POD_NAME}:${REMOTE_OUT}/${OUT_FILE}" "${LOCAL_OUT}/${OUT_FILE}"

  echo "[${i}] Cleaning up..."
  kubectl delete "${CHAOS_KIND}" "${CHAOS_NAME}" -n "${NS}" --ignore-not-found 2>/dev/null || true
  kubectl delete pod "${POD_NAME}" -n "${NS}" --ignore-not-found

  echo "[${i}] Waiting for Valkey cluster to stabilize..."
  kubectl rollout status sts/valkey -n "${NS}" --timeout=120s
  sleep 10

  echo "[${i}] Done. Result: ${LOCAL_OUT}/${OUT_FILE}"
done

echo ""
echo "=========================================="
echo "  All ${N} resilience [${SCENARIO}] runs complete."
echo "  Results in: ${LOCAL_OUT}/"
echo "  Analyse with:"
echo "    python cli.py resilience --input ${LOCAL_OUT} --scenario ${SCENARIO} --output-dir ./plots/resilience"
echo "=========================================="

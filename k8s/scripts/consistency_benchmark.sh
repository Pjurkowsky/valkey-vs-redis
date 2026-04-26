#!/usr/bin/env bash
set -euo pipefail

N="${N:-5}"
NS="vk"
IMAGE="${CONSISTENCY_IMAGE:-consistency_checker:1}"
LOCAL_OUT="${1:-./results_consistency}"
REMOTE_OUT="/work/results"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CHAOS_YAML="${SCRIPT_DIR}/../chaos/network-partition.yaml"

HOST="valkey.vk.svc.cluster.local"
PORT=6379
DURATION=120
STEADY_STATE_WAIT=30

mkdir -p "${LOCAL_OUT}"

for i in $(seq 1 "${N}"); do
  POD_NAME="consistency-checker-${i}"
  RUN_ID="run${i}_$(date +%s)"
  OUT_FILE="consistency_run_${i}.json"
  REMOTE_FILE="${REMOTE_OUT}/${OUT_FILE}"
  echo ""
  echo "=========================================="
  echo "  Consistency run ${i}/${N}"
  echo "=========================================="

  kubectl delete networkchaos valkey-network-partition -n "${NS}" --ignore-not-found 2>/dev/null || true
  kubectl delete pod "${POD_NAME}" -n "${NS}" --ignore-not-found 2>/dev/null || true

  echo "[${i}] Starting consistency checker pod (duration=${DURATION}s)..."
  kubectl run "${POD_NAME}" -n "${NS}" \
    --image="${IMAGE}" \
    --restart=Never \
    --command -- \
    python /work/consistency_check.py \
      --host "${HOST}" \
      --port "${PORT}" \
      --duration "${DURATION}" \
      --run-id "${RUN_ID}" \
      --output "${REMOTE_FILE}"

  echo "[${i}] Waiting for pod to start..."
  kubectl wait pod/"${POD_NAME}" -n "${NS}" \
    --for=condition=Ready --timeout=60s 2>/dev/null || true

  echo "[${i}] Waiting ${STEADY_STATE_WAIT}s for steady state writes..."
  sleep "${STEADY_STATE_WAIT}"

  echo "[${i}] Injecting network partition..."
  kubectl apply -f "${CHAOS_YAML}"

  echo "[${i}] Waiting for checker to finish..."
  kubectl wait pod/"${POD_NAME}" -n "${NS}" \
    --for=jsonpath='{.status.phase}'=Succeeded --timeout=600s

  echo "[${i}] Copying results..."
  kubectl cp "${NS}/${POD_NAME}:${REMOTE_FILE}" "${LOCAL_OUT}/${OUT_FILE}"

  echo "[${i}] Cleaning up..."
  kubectl delete networkchaos valkey-network-partition -n "${NS}" --ignore-not-found 2>/dev/null || true
  kubectl delete pod "${POD_NAME}" -n "${NS}" --ignore-not-found

  echo "[${i}] Waiting for Valkey cluster to stabilize..."
  kubectl rollout status sts/valkey -n "${NS}" --timeout=120s
  sleep 15

  echo "[${i}] Done. Result: ${LOCAL_OUT}/${OUT_FILE}"
done

echo ""
echo "=========================================="
echo "  All ${N} consistency runs complete."
echo "  Results in: ${LOCAL_OUT}/"
echo "  Analyse with:"
echo "    python cli.py consistency --input ${LOCAL_OUT} --output-dir ./consistency_plots"
echo "=========================================="

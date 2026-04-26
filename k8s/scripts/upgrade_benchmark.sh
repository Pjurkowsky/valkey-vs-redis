#!/usr/bin/env bash
set -euo pipefail

N="${N:-5}"
NS="vk"
IMAGE="${MEMTIER_IMAGE:-memtier_k8s:1}"
LOCAL_OUT="${1:-./results_upgrade}"
REMOTE_OUT="/work/results_upgrade"

HELM_CHART_PATH="${HELM_CHART_PATH:-../valkey-helm/valkey}"
VALUES_FILE="${VALUES_FILE:-./k8s/manifests/values.yaml}"

HOST="valkey.vk.svc.cluster.local"
PORT=6379
THREADS=4
CLIENTS=16
TEST_TIME=300
KEYS=100000
DATA_SIZE=1024
RATIO="1:1"
STEADY_STATE_WAIT=30

mkdir -p "${LOCAL_OUT}"

for i in $(seq 1 "${N}"); do
  POD_NAME="memtier-upgrade-${i}"
  OUT_FILE="upgrade_run_${i}.json"
  echo ""
  echo "=========================================="
  echo "  Upgrade run ${i}/${N}"
  echo "=========================================="

  kubectl delete pod "${POD_NAME}" -n "${NS}" --ignore-not-found 2>/dev/null || true

  echo "[${i}] Starting memtier pod (test-time=${TEST_TIME}s)..."
  kubectl run "${POD_NAME}" -n "${NS}" \
    --image="${IMAGE}" \
    --restart=Never \
    --command -- \
    memtier_benchmark \
      --server="${HOST}" --port="${PORT}" \
      --protocol=redis \
      --cluster-mode \
      --threads="${THREADS}" --clients="${CLIENTS}" \
      --test-time="${TEST_TIME}" \
      --key-maximum="${KEYS}" \
      --data-size="${DATA_SIZE}" \
      --ratio="${RATIO}" \
      --json-out-file "${REMOTE_OUT}/${OUT_FILE}" \
      --run-count 1 \
      --print-percentiles="50,95,99,99.9"

  echo "[${i}] Waiting for pod to start..."
  kubectl wait pod/"${POD_NAME}" -n "${NS}" \
    --for=condition=Ready --timeout=60s 2>/dev/null || true

  echo "[${i}] Waiting ${STEADY_STATE_WAIT}s for steady state..."
  sleep "${STEADY_STATE_WAIT}"

  TRIGGER="$(date +%s)"
  echo "[${i}] Triggering rolling upgrade (restart-trigger=${TRIGGER})..."
  helm upgrade valkey "${HELM_CHART_PATH}" \
    -n "${NS}" \
    -f "${VALUES_FILE}" \
    --set "podAnnotations.restart-trigger=${TRIGGER}" \
    --wait=false

  echo "[${i}] Waiting for memtier to finish..."
  kubectl wait pod/"${POD_NAME}" -n "${NS}" \
    --for=jsonpath='{.status.phase}'=Succeeded --timeout=600s

  echo "[${i}] Copying results..."
  kubectl cp "${NS}/${POD_NAME}:${REMOTE_OUT}/${OUT_FILE}" "${LOCAL_OUT}/${OUT_FILE}"

  echo "[${i}] Cleaning up memtier pod..."
  kubectl delete pod "${POD_NAME}" -n "${NS}" --ignore-not-found

  echo "[${i}] Waiting for Valkey rollout to fully complete..."
  kubectl rollout status sts/valkey -n "${NS}" --timeout=300s
  sleep 15

  echo "[${i}] Done. Result: ${LOCAL_OUT}/${OUT_FILE}"
done

echo ""
echo "=========================================="
echo "  All ${N} upgrade runs complete."
echo "  Results in: ${LOCAL_OUT}/"
echo "  Analyse with:"
echo "    python cli.py upgrade --input ${LOCAL_OUT} --output-dir ./upgrade_plots"
echo "=========================================="

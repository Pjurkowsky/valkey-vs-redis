#!/usr/bin/env bash
set -euo pipefail

N="${N:-5}"
NS="vk"
IMAGE="${MEMTIER_IMAGE:-memtier_k8s:1}"
LOCAL_OUT="${1:-./results_reshard}"
REMOTE_OUT="/work/results_reshard"

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

ORIGINAL_SHARDS=3
TARGET_SHARDS=4

mkdir -p "${LOCAL_OUT}"

get_master_node_id() {
  local pod="$1"
  kubectl exec "${pod}" -n "${NS}" -- valkey-cli cluster myid 2>/dev/null | tr -d '[:space:]'
}

restore_cluster() {
  echo "  [restore] Attempting graceful restore to ${ORIGINAL_SHARDS} shards..."

  local new_master_pod="valkey-${ORIGINAL_SHARDS}"
  local new_master_id
  new_master_id="$(get_master_node_id "${new_master_pod}" 2>/dev/null || echo "")"

  if [[ -z "${new_master_id}" ]]; then
    echo "  [restore] New shard pod not found or not responding, skipping graceful reshard."
    echo "  [restore] Falling back to helm reinstall..."
    helm uninstall valkey -n "${NS}" --wait 2>/dev/null || true
    sleep 10
    helm install valkey "${HELM_CHART_PATH}" -n "${NS}" -f "${VALUES_FILE}"
    kubectl rollout status sts/valkey -n "${NS}" --timeout=300s
    sleep 20
    return
  fi

  local target_id
  target_id="$(get_master_node_id valkey-0)"

  local slots_on_new
  slots_on_new="$(kubectl exec valkey-0 -n "${NS}" -- \
    valkey-cli cluster nodes 2>/dev/null \
    | grep "${new_master_id}" \
    | grep -oP '\d+-\d+' \
    | awk -F- '{s+=$2-$1+1} END {print s+0}')"

  if [[ "${slots_on_new}" -gt 0 ]]; then
    echo "  [restore] Moving ${slots_on_new} slots from shard-${ORIGINAL_SHARDS} back..."
    kubectl exec valkey-0 -n "${NS}" -- \
      valkey-cli --cluster reshard "${HOST}:${PORT}" \
        --cluster-from "${new_master_id}" \
        --cluster-to "${target_id}" \
        --cluster-slots "${slots_on_new}" \
        --cluster-yes 2>&1 || true
    sleep 5
  fi

  local replica_id
  replica_id="$(kubectl exec valkey-0 -n "${NS}" -- \
    valkey-cli cluster nodes 2>/dev/null \
    | grep "slave ${new_master_id}" \
    | awk '{print $1}' || echo "")"

  if [[ -n "${replica_id}" ]]; then
    echo "  [restore] Removing replica node ${replica_id}..."
    kubectl exec valkey-0 -n "${NS}" -- \
      valkey-cli --cluster del-node "${HOST}:${PORT}" "${replica_id}" 2>&1 || true
    sleep 3
  fi

  if [[ -n "${new_master_id}" ]]; then
    echo "  [restore] Removing master node ${new_master_id}..."
    kubectl exec valkey-0 -n "${NS}" -- \
      valkey-cli --cluster del-node "${HOST}:${PORT}" "${new_master_id}" 2>&1 || true
    sleep 3
  fi

  echo "  [restore] Scaling back to ${ORIGINAL_SHARDS} shards via helm..."
  helm upgrade valkey "${HELM_CHART_PATH}" \
    -n "${NS}" \
    -f "${VALUES_FILE}" \
    --set "cluster.shards=${ORIGINAL_SHARDS}" \
    --wait=false 2>&1 || true

  kubectl rollout status sts/valkey -n "${NS}" --timeout=300s
  sleep 20
  echo "  [restore] Cluster restored."
}

for i in $(seq 1 "${N}"); do
  POD_NAME="memtier-reshard-${i}"
  OUT_FILE="reshard_run_${i}.json"
  TIMING_FILE="reshard_timing_${i}.json"
  echo ""
  echo "=========================================="
  echo "  Reshard run ${i}/${N}"
  echo "=========================================="

  kubectl delete pod "${POD_NAME}" -n "${NS}" --ignore-not-found 2>/dev/null || true

  echo "[${i}] Verifying cluster is at ${ORIGINAL_SHARDS} shards..."
  CURRENT_MASTERS="$(kubectl exec valkey-0 -n "${NS}" -- \
    valkey-cli cluster nodes 2>/dev/null | grep master | wc -l)"
  echo "[${i}] Current masters: ${CURRENT_MASTERS}"

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

  MEMTIER_START="$(date +%s)"

  echo "[${i}] Scaling up to ${TARGET_SHARDS} shards..."
  SCALE_START="$(date +%s)"
  helm upgrade valkey "${HELM_CHART_PATH}" \
    -n "${NS}" \
    -f "${VALUES_FILE}" \
    --set "cluster.shards=${TARGET_SHARDS}" \
    --wait=false

  echo "[${i}] Waiting for new pods to be ready..."
  kubectl rollout status sts/valkey -n "${NS}" --timeout=300s
  SCALE_END="$(date +%s)"
  echo "[${i}] Scale-up took $((SCALE_END - SCALE_START))s"

  sleep 10

  echo "[${i}] Rebalancing slots to include new shard..."
  REBALANCE_START="$(date +%s)"
  kubectl exec valkey-0 -n "${NS}" -- \
    valkey-cli --cluster rebalance "${HOST}:${PORT}" \
      --cluster-use-empty-masters \
      --cluster-yes 2>&1 | tail -20
  REBALANCE_END="$(date +%s)"
  REBALANCE_DURATION=$((REBALANCE_END - REBALANCE_START))
  echo "[${i}] Rebalance completed in ${REBALANCE_DURATION}s"

  NEW_MASTERS="$(kubectl exec valkey-0 -n "${NS}" -- \
    valkey-cli cluster nodes 2>/dev/null | grep master | wc -l)"
  echo "[${i}] Masters after rebalance: ${NEW_MASTERS}"

  echo "[${i}] Waiting for memtier to finish..."
  kubectl wait pod/"${POD_NAME}" -n "${NS}" \
    --for=jsonpath='{.status.phase}'=Succeeded --timeout=600s

  echo "[${i}] Copying results..."
  kubectl cp "${NS}/${POD_NAME}:${REMOTE_OUT}/${OUT_FILE}" "${LOCAL_OUT}/${OUT_FILE}"

  cat > "${LOCAL_OUT}/${TIMING_FILE}" <<EOF
{
  "run": ${i},
  "scale_start": ${SCALE_START},
  "scale_end": ${SCALE_END},
  "scale_duration_s": $((SCALE_END - SCALE_START)),
  "rebalance_start": ${REBALANCE_START},
  "rebalance_end": ${REBALANCE_END},
  "rebalance_duration_s": ${REBALANCE_DURATION},
  "memtier_start": ${MEMTIER_START},
  "original_shards": ${ORIGINAL_SHARDS},
  "target_shards": ${TARGET_SHARDS},
  "masters_after": ${NEW_MASTERS}
}
EOF
  echo "[${i}] Timing data saved to ${LOCAL_OUT}/${TIMING_FILE}"

  echo "[${i}] Cleaning up memtier pod..."
  kubectl delete pod "${POD_NAME}" -n "${NS}" --ignore-not-found

  echo "[${i}] Restoring cluster to ${ORIGINAL_SHARDS} shards..."
  restore_cluster

  echo "[${i}] Done. Results: ${LOCAL_OUT}/${OUT_FILE}, ${LOCAL_OUT}/${TIMING_FILE}"
done

echo ""
echo "=========================================="
echo "  All ${N} reshard runs complete."
echo "  Results in: ${LOCAL_OUT}/"
echo "  Analyse with:"
echo "    python cli.py reshard --input ${LOCAL_OUT} --output-dir ./reshard_plots"
echo "=========================================="

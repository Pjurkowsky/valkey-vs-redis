#!/usr/bin/env bash
set -euo pipefail

N="${N:-5}"
NS="vk"
IMAGE="${MEMTIER_IMAGE:-memtier_k8s:1}"
LOCAL_OUT="${1:-./results/upgrade}"
REMOTE_OUT="/work/results/upgrade"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

source "${SCRIPT_DIR}/target_config.sh"
source "${SCRIPT_DIR}/pod_results.sh"

HELM_CHART_PATH="${TC_HELM_CHART}"
VALUES_FILE="${TC_VALUES_FILE}"
HELM_RELEASE="${TC_HELM_RELEASE}"
STS="${TC_STS}"
CLI="${TC_CLI}"
ADMIN_POD="$(tc_admin_pod)"
PROBE_IMAGE="${TC_PROBE_IMAGE}"

HOST="${TC_HOST}"
PORT="${TC_PORT}"
THREADS=4
CLIENTS=16
TEST_TIME=300
KEYS=100000
DATA_SIZE=1024
RATIO="1:1"
STEADY_STATE_WAIT=30

mkdir -p "${LOCAL_OUT}"

wait_cluster_client_ready() {
  local max_wait="${1:-300}"
  local elapsed=0

  echo "  Waiting for cluster client readiness (max ${max_wait}s)..."
  while [[ "${elapsed}" -lt "${max_wait}" ]]; do
    local info state slots_ok slots_assigned
    info="$(kubectl exec "${ADMIN_POD}" -n "${NS}" -- \
      ${CLI} cluster info 2>/dev/null || true)"
    state="$(awk -F: '$1=="cluster_state" {gsub(/\r/,"",$2); print $2}' <<<"${info}")"
    slots_ok="$(awk -F: '$1=="cluster_slots_ok" {gsub(/\r/,"",$2); print $2}' <<<"${info}")"
    slots_assigned="$(awk -F: '$1=="cluster_slots_assigned" {gsub(/\r/,"",$2); print $2}' <<<"${info}")"

    if [[ "${state}" == "ok" && "${slots_ok}" == "16384" && "${slots_assigned}" == "16384" ]]; then
      local probe_key="upgrade:probe:$(date +%s%N)"
      if kubectl run "upgrade-probe-${elapsed}" -n "${NS}" \
        --image="${PROBE_IMAGE}" \
        --restart=Never \
        --quiet \
        --rm \
        --attach \
        --command -- \
        /bin/sh -c "${CLI} -c -h '${HOST}' -p '${PORT}' set '${probe_key}' ok >/dev/null && test \"\$(${CLI} -c -h '${HOST}' -p '${PORT}' get '${probe_key}')\" = ok && ${CLI} -c -h '${HOST}' -p '${PORT}' del '${probe_key}' >/dev/null" \
        >/dev/null 2>&1; then
        echo "  Cluster client-ready after ${elapsed}s"
        return 0
      fi
    fi

    sleep 5
    elapsed=$((elapsed + 5))
  done

  echo "ERROR: Cluster was not client-ready after ${max_wait}s" >&2
  kubectl exec "${ADMIN_POD}" -n "${NS}" -- ${CLI} cluster info || true
  kubectl exec "${ADMIN_POD}" -n "${NS}" -- ${CLI} cluster slots || true
  return 1
}

upgrade_annotation_key() {
  if [[ "${TARGET}" == "redis" ]]; then
    echo "redis.podAnnotations.restart-trigger"
  else
    echo "podAnnotations.restart-trigger"
  fi
}

for i in $(seq 1 "${N}"); do
  POD_NAME="memtier-upgrade-${i}"
  OUT_FILE="upgrade_run_${i}.json"
  echo ""
  echo "=========================================="
  echo "  Upgrade run ${i}/${N} (target=${TARGET})"
  echo "=========================================="

  kubectl delete pod "${POD_NAME}" -n "${NS}" --ignore-not-found 2>/dev/null || true

  wait_cluster_client_ready 300

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

  TRIGGER="$(date +%s)"
  ANNOTATION_KEY="$(upgrade_annotation_key)"
  echo "[${i}] Triggering rolling upgrade (${ANNOTATION_KEY}=${TRIGGER})..."
  helm upgrade "${HELM_RELEASE}" "${HELM_CHART_PATH}" \
    -n "${NS}" \
    -f "${VALUES_FILE}" \
    --set-string "${ANNOTATION_KEY}=${TRIGGER}" \
    --wait=false

  echo "[${i}] Waiting for memtier to finish..."
  if ! wait_for_pod_marker "${NS}" "${POD_NAME}" "${POD_DONE_FILE}" 600; then
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

  echo "[${i}] Cleaning up memtier pod..."
  kubectl delete pod "${POD_NAME}" -n "${NS}" --ignore-not-found

  echo "[${i}] Waiting for rollout to fully complete..."
  kubectl rollout status "sts/${STS}" -n "${NS}" --timeout=300s
  wait_cluster_client_ready 300
  sleep 15

  echo "[${i}] Done. Result: ${LOCAL_OUT}/${OUT_FILE}"
done

echo ""
echo "=========================================="
echo "  All ${N} upgrade runs complete."
echo "  Results in: ${LOCAL_OUT}/"
echo "  Analyse with:"
echo "    python cli.py upgrade --input ${LOCAL_OUT} --output-dir ./plots/upgrade"
echo "=========================================="

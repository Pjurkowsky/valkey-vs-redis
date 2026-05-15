#!/usr/bin/env bash
set -euo pipefail

N="${N:-5}"
NS="vk"
IMAGE="${CONSISTENCY_IMAGE:-consistency_checker:1}"
LOCAL_OUT="${1:-./results/consistency}"
REMOTE_OUT="/work/results/consistency"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

source "${SCRIPT_DIR}/target_config.sh"

if [[ "${TARGET}" == "redis" ]]; then
  DEFAULT_CHAOS_YAML="${SCRIPT_DIR}/../chaos/redis-client-network-partition.yaml"
else
  DEFAULT_CHAOS_YAML="${SCRIPT_DIR}/../chaos/client-network-partition.yaml"
fi
CHAOS_YAML="${CHAOS_YAML:-${DEFAULT_CHAOS_YAML}}"

source "${SCRIPT_DIR}/pod_results.sh"

HOST="${CONSISTENCY_HOST:-${TC_HOST}}"
PORT="${CONSISTENCY_PORT:-${TC_PORT}}"
STS="${TC_STS}"
DURATION="${CONSISTENCY_DURATION:-120}"
STEADY_STATE_WAIT="${CONSISTENCY_STEADY_STATE_WAIT:-30}"
CONSISTENCY_CLIENTS="${CONSISTENCY_CLIENTS:-50}"
CONSISTENCY_SOCKET_TIMEOUT="${CONSISTENCY_SOCKET_TIMEOUT:-1.0}"
CONSISTENCY_CONNECT_TIMEOUT="${CONSISTENCY_CONNECT_TIMEOUT:-1.0}"
CONSISTENCY_RETRY_ON_TIMEOUT="${CONSISTENCY_RETRY_ON_TIMEOUT:-false}"
CONSISTENCY_SLOW_THRESHOLD_MS="${CONSISTENCY_SLOW_THRESHOLD_MS:-1000}"
CONSISTENCY_TLS="${CONSISTENCY_TLS:-false}"
CONSISTENCY_TLS_SKIP_VERIFY="${CONSISTENCY_TLS_SKIP_VERIFY:-true}"
CONSISTENCY_TLS_CA_CERT="${CONSISTENCY_TLS_CA_CERT:-}"
CONSISTENCY_TLS_CERT="${CONSISTENCY_TLS_CERT:-}"
CONSISTENCY_TLS_KEY="${CONSISTENCY_TLS_KEY:-}"

mkdir -p "${LOCAL_OUT}"

save_run_logs() {
  local run_idx="$1"
  local pod_name="$2"
  local prefix="${LOCAL_OUT}/consistency_run_${run_idx}"

  kubectl logs "${pod_name}" -n "${NS}" > "${prefix}.log" 2>&1 || true
  kubectl get networkchaos valkey-network-partition -n "${NS}" -o yaml \
    > "${prefix}_networkchaos.yaml" 2>&1 || true
  kubectl describe networkchaos valkey-network-partition -n "${NS}" \
    > "${prefix}_networkchaos_describe.txt" 2>&1 || true
  kubectl logs -n chaos-mesh ds/chaos-daemon --tail=500 \
    > "${prefix}_chaos_daemon.log" 2>&1 || true
}

wait_for_networkchaos_injected() {
  local timeout_s="${1:-60}"
  local elapsed=0
  local selected injected

  while [[ "${elapsed}" -lt "${timeout_s}" ]]; do
    selected="$(kubectl get networkchaos valkey-network-partition -n "${NS}" \
      -o jsonpath='{.status.conditions[?(@.type=="Selected")].status}' 2>/dev/null || true)"
    injected="$(kubectl get networkchaos valkey-network-partition -n "${NS}" \
      -o jsonpath='{.status.conditions[?(@.type=="AllInjected")].status}' 2>/dev/null || true)"

    if [[ "${selected}" == "True" && "${injected}" == "True" ]]; then
      return 0
    fi

    sleep 2
    elapsed=$((elapsed + 2))
  done

  return 1
}

CONSISTENCY_TLS_ARGS=()
case "${CONSISTENCY_TLS}" in
  1|true|TRUE|yes|YES|on|ON)
    CONSISTENCY_TLS_ARGS+=(--tls)
    case "${CONSISTENCY_TLS_SKIP_VERIFY}" in
      1|true|TRUE|yes|YES|on|ON)
        CONSISTENCY_TLS_ARGS+=(--tls-skip-verify)
        ;;
    esac
    if [[ -n "${CONSISTENCY_TLS_CA_CERT}" ]]; then
      CONSISTENCY_TLS_ARGS+=(--tls-ca-cert "${CONSISTENCY_TLS_CA_CERT}")
    fi
    if [[ -n "${CONSISTENCY_TLS_CERT}" ]]; then
      CONSISTENCY_TLS_ARGS+=(--tls-cert "${CONSISTENCY_TLS_CERT}")
    fi
    if [[ -n "${CONSISTENCY_TLS_KEY}" ]]; then
      CONSISTENCY_TLS_ARGS+=(--tls-key "${CONSISTENCY_TLS_KEY}")
    fi
    ;;
esac

CONSISTENCY_RETRY_ARGS=(--no-retry-on-timeout)
case "${CONSISTENCY_RETRY_ON_TIMEOUT}" in
  1|true|TRUE|yes|YES|on|ON)
    CONSISTENCY_RETRY_ARGS=(--retry-on-timeout)
    ;;
esac

for i in $(seq 1 "${N}"); do
  POD_NAME="consistency-checker-${i}"
  RUN_ID="run${i}_$(date +%s)"
  OUT_FILE="consistency_run_${i}.json"
  REMOTE_FILE="${REMOTE_OUT}/${OUT_FILE}"
  echo ""
  echo "=========================================="
  echo "  Consistency run ${i}/${N} (target=${TARGET})"
  echo "=========================================="

  kubectl delete networkchaos valkey-network-partition -n "${NS}" --ignore-not-found 2>/dev/null || true
  kubectl delete pod "${POD_NAME}" -n "${NS}" --ignore-not-found 2>/dev/null || true

  echo "[${i}] Starting consistency checker pod (duration=${DURATION}s)..."
  kubectl run "${POD_NAME}" -n "${NS}" \
    --image="${IMAGE}" \
    --restart=Never \
    --command -- \
    /bin/sh -c "
      mkdir -p '${REMOTE_OUT}'
      python -u /work/consistency_check.py \
        --host '${HOST}' \
        --port '${PORT}' \
        --duration '${DURATION}' \
        --run-id '${RUN_ID}' \
        --output '${REMOTE_FILE}' \
        --clients '${CONSISTENCY_CLIENTS}' \
        --socket-timeout '${CONSISTENCY_SOCKET_TIMEOUT}' \
        --connect-timeout '${CONSISTENCY_CONNECT_TIMEOUT}' \
        --slow-threshold-ms '${CONSISTENCY_SLOW_THRESHOLD_MS}' \
        ${CONSISTENCY_RETRY_ARGS[*]} \
        ${CONSISTENCY_TLS_ARGS[*]}
      status=\$?
      echo \"\$status\" > '${POD_EXIT_CODE_FILE}'
      touch '${POD_DONE_FILE}'
      sleep '${POD_HOLD_SECONDS}'
    "

  echo "[${i}] Waiting for pod to start..."
  kubectl wait pod/"${POD_NAME}" -n "${NS}" \
    --for=condition=Ready --timeout=60s 2>/dev/null || true

  echo "[${i}] Waiting ${STEADY_STATE_WAIT}s for steady state writes..."
  sleep "${STEADY_STATE_WAIT}"

  echo "[${i}] Injecting network partition..."
  kubectl apply -f "${CHAOS_YAML}"
  if wait_for_networkchaos_injected 60; then
    echo "[${i}] Network partition injected."
  else
    echo "[${i}] ERROR: NetworkChaos was not fully injected."
    save_run_logs "${i}" "${POD_NAME}"
    print_pod_debug_info "${NS}" "${POD_NAME}"
    kubectl delete networkchaos valkey-network-partition -n "${NS}" --ignore-not-found 2>/dev/null || true
    exit 1
  fi

  echo "[${i}] Waiting for checker to finish..."
  if ! wait_for_pod_marker "${NS}" "${POD_NAME}" "${POD_DONE_FILE}" 600; then
    echo "[${i}] ERROR: consistency checker pod did not signal completion."
    save_run_logs "${i}" "${POD_NAME}"
    print_pod_debug_info "${NS}" "${POD_NAME}"
    exit 1
  fi

  exit_code="$(read_pod_exit_code "${NS}" "${POD_NAME}" "${POD_EXIT_CODE_FILE}")"
  if [[ -z "${exit_code}" || "${exit_code}" != "0" ]]; then
    echo "[${i}] ERROR: consistency checker exited with code ${exit_code:-unknown}."
    save_run_logs "${i}" "${POD_NAME}"
    print_pod_debug_info "${NS}" "${POD_NAME}"
    exit 1
  fi

  echo "[${i}] Saving logs..."
  save_run_logs "${i}" "${POD_NAME}"

  echo "[${i}] Copying results..."
  kubectl cp "${NS}/${POD_NAME}:${REMOTE_FILE}" "${LOCAL_OUT}/${OUT_FILE}"

  echo "[${i}] Cleaning up..."
  kubectl delete networkchaos valkey-network-partition -n "${NS}" --ignore-not-found 2>/dev/null || true
  kubectl delete pod "${POD_NAME}" -n "${NS}" --ignore-not-found

  echo "[${i}] Waiting for cluster to stabilize..."
  kubectl rollout status "sts/${STS}" -n "${NS}" --timeout=120s
  sleep 15

  echo "[${i}] Done. Result: ${LOCAL_OUT}/${OUT_FILE}"
done

echo ""
echo "=========================================="
echo "  All ${N} consistency runs complete."
echo "  Results in: ${LOCAL_OUT}/"
echo "  Analyse with:"
echo "    python cli.py consistency --input ${LOCAL_OUT} --output-dir ./plots/consistency"
echo "=========================================="

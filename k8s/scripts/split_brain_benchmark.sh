#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# Split-brain consistency benchmark
#
# Partitions Valkey nodes into minority (1 shard) and majority (2 shards),
# writes keys spread across all hash slots, then verifies which ACK'd keys
# on minority-side slots were lost after the partition heals.
# ---------------------------------------------------------------------------

N="${N:-5}"
NS="vk"
IMAGE="${CONSISTENCY_IMAGE:-consistency_checker:2}"
LOCAL_OUT="${1:-./results/split_brain}"
REMOTE_OUT="/work/results/split_brain"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CHAOS_YAML="${CHAOS_YAML:-${SCRIPT_DIR}/../chaos/split-brain-partition.yaml}"
CHAOS_NAME="valkey-split-brain"
VALKEY_POD_SELECTOR="${VALKEY_POD_SELECTOR:-app.kubernetes.io/name=valkey}"

source "${SCRIPT_DIR}/pod_results.sh"

HOST="${SPLIT_BRAIN_HOST:-valkey.vk.svc.cluster.local}"
PORT="${SPLIT_BRAIN_PORT:-6379}"
DURATION="${SPLIT_BRAIN_DURATION:-120}"
STEADY_STATE_WAIT="${SPLIT_BRAIN_STEADY_STATE_WAIT:-30}"
SPLIT_BRAIN_CLIENTS="${SPLIT_BRAIN_CLIENTS:-50}"
SPLIT_BRAIN_SOCKET_TIMEOUT="${SPLIT_BRAIN_SOCKET_TIMEOUT:-1.0}"
SPLIT_BRAIN_CONNECT_TIMEOUT="${SPLIT_BRAIN_CONNECT_TIMEOUT:-1.0}"
SPLIT_BRAIN_RETRY_ON_TIMEOUT="${SPLIT_BRAIN_RETRY_ON_TIMEOUT:-false}"
SPLIT_BRAIN_SLOW_THRESHOLD_MS="${SPLIT_BRAIN_SLOW_THRESHOLD_MS:-1000}"
SPLIT_BRAIN_TLS="${SPLIT_BRAIN_TLS:-false}"
SPLIT_BRAIN_TLS_SKIP_VERIFY="${SPLIT_BRAIN_TLS_SKIP_VERIFY:-true}"

VALKEY_CLI_TLS="${VALKEY_CLI_TLS:-${SPLIT_BRAIN_TLS}}"
VALKEY_CLI_TLS_SKIP_VERIFY="${VALKEY_CLI_TLS_SKIP_VERIFY:-${SPLIT_BRAIN_TLS_SKIP_VERIFY}}"
VALKEY_CLI_CACERT="${VALKEY_CLI_CACERT:-/tls/ca.crt}"

mkdir -p "${LOCAL_OUT}"

# -- TLS args for checker and valkey-cli ---------------------------------

CHECKER_TLS_ARGS=()
case "${SPLIT_BRAIN_TLS}" in
  1|true|TRUE|yes|YES|on|ON)
    CHECKER_TLS_ARGS+=(--tls --tls-skip-verify)
    ;;
esac

CHECKER_RETRY_ARGS=(--no-retry-on-timeout)
case "${SPLIT_BRAIN_RETRY_ON_TIMEOUT}" in
  1|true|TRUE|yes|YES|on|ON)
    CHECKER_RETRY_ARGS=(--retry-on-timeout)
    ;;
esac

VALKEY_CLI_ARGS=()
case "${VALKEY_CLI_TLS}" in
  1|true|TRUE|yes|YES|on|ON)
    VALKEY_CLI_ARGS+=(--tls)
    case "${VALKEY_CLI_TLS_SKIP_VERIFY}" in
      1|true|TRUE|yes|YES|on|ON)
        VALKEY_CLI_ARGS+=(--insecure)
        ;;
      *)
        VALKEY_CLI_ARGS+=(--cacert "${VALKEY_CLI_CACERT}")
        ;;
    esac
    ;;
esac

# -- Topology discovery --------------------------------------------------

discover_minority() {
  # Isolate one master as the minority partition while leaving its replica
  # in the majority partition so the majority can promote it during the split.
  # Returns one pod name.
  local nodes_output
  nodes_output="$(kubectl exec valkey-0 -n "${NS}" -- \
    valkey-cli "${VALKEY_CLI_ARGS[@]}" cluster nodes 2>/dev/null)"

  local target_master_id target_master_pod
  target_master_id="$(echo "${nodes_output}" | awk '$3 ~ /master/ && $3 !~ /fail/ {print $1; exit}')"
  target_master_pod="$(echo "${nodes_output}" | awk -v id="${target_master_id}" \
    '$1 == id {addr=$2; sub(/:.*/, "", addr); sub(/,.*/, "", addr); print addr}')"

  local replica_pod
  replica_pod="$(echo "${nodes_output}" | awk -v mid="${target_master_id}" \
    '$4 == mid && $3 ~ /slave/ {addr=$2; sub(/:.*/, "", addr); sub(/,.*/, "", addr); print addr}')"

  if [[ -z "${target_master_pod}" ]]; then
    echo "ERROR: could not find a master node" >&2
    return 1
  fi

  if [[ -z "${replica_pod}" ]]; then
    echo "ERROR: could not find a replica for master ${target_master_id}" >&2
    return 1
  fi

  local master_pod_name
  master_pod_name="$(resolve_address_to_pod "${target_master_pod}")"

  echo "${master_pod_name}"
}

resolve_address_to_pod() {
  local addr="$1"
  if [[ "${addr}" == valkey-* ]]; then
    echo "${addr%%.*}"
    return 0
  fi
  local pod_table
  pod_table="$(kubectl get pods -n "${NS}" -o wide --no-headers 2>/dev/null)"
  local pod_name
  pod_name="$(awk -v target="${addr}" '$6 == target {print $1; exit}' <<<"${pod_table}")"
  if [[ -z "${pod_name}" ]]; then
    echo "${addr}"
    return 0
  fi
  echo "${pod_name}"
}

label_pods() {
  local -a minority_pods=("$@")
  local all_pods
  all_pods="$(kubectl get pods -n "${NS}" -l "${VALKEY_POD_SELECTOR}" \
    -o jsonpath='{.items[*].metadata.name}')"

  if [[ -z "${all_pods}" ]]; then
    echo "ERROR: no Valkey pods matched selector '${VALKEY_POD_SELECTOR}'" >&2
    return 1
  fi

  for pod in ${all_pods}; do
    local is_minority=false
    for mp in "${minority_pods[@]}"; do
      if [[ "${pod}" == "${mp}" ]]; then
        is_minority=true
        break
      fi
    done

    if [[ "${is_minority}" == "true" ]]; then
      kubectl label pod "${pod}" -n "${NS}" chaos-side=minority --overwrite
    else
      kubectl label pod "${pod}" -n "${NS}" chaos-side=majority --overwrite
    fi
  done
}

remove_labels() {
  kubectl label pods -n "${NS}" -l chaos-side chaos-side- 2>/dev/null || true
}

wait_for_networkchaos_injected() {
  local name="$1"
  local timeout_s="${2:-60}"
  local elapsed=0

  while [[ "${elapsed}" -lt "${timeout_s}" ]]; do
    local selected injected
    selected="$(kubectl get networkchaos "${name}" -n "${NS}" \
      -o jsonpath='{.status.conditions[?(@.type=="Selected")].status}' 2>/dev/null || true)"
    injected="$(kubectl get networkchaos "${name}" -n "${NS}" \
      -o jsonpath='{.status.conditions[?(@.type=="AllInjected")].status}' 2>/dev/null || true)"

    if [[ "${selected}" == "True" && "${injected}" == "True" ]]; then
      return 0
    fi

    sleep 2
    elapsed=$((elapsed + 2))
  done

  return 1
}

save_run_logs() {
  local run_idx="$1"
  local pod_name="$2"
  local prefix="${LOCAL_OUT}/split_brain_run_${run_idx}"

  kubectl logs "${pod_name}" -n "${NS}" > "${prefix}.log" 2>&1 || true
  kubectl get networkchaos "${CHAOS_NAME}" -n "${NS}" -o yaml \
    > "${prefix}_networkchaos.yaml" 2>&1 || true
  kubectl describe networkchaos "${CHAOS_NAME}" -n "${NS}" \
    > "${prefix}_networkchaos_describe.txt" 2>&1 || true
  kubectl logs -n chaos-mesh ds/chaos-daemon --tail=500 \
    > "${prefix}_chaos_daemon.log" 2>&1 || true
}

# -- Main loop -----------------------------------------------------------

for i in $(seq 1 "${N}"); do
  POD_NAME="split-brain-checker-${i}"
  RUN_ID="sb_run${i}_$(date +%s)"
  OUT_FILE="split_brain_run_${i}.json"
  REMOTE_FILE="${REMOTE_OUT}/${OUT_FILE}"

  echo ""
  echo "=========================================="
  echo "  Split-brain run ${i}/${N}"
  echo "=========================================="

  # Cleanup from previous run
  kubectl delete networkchaos "${CHAOS_NAME}" -n "${NS}" --ignore-not-found 2>/dev/null || true
  kubectl delete pod "${POD_NAME}" -n "${NS}" --ignore-not-found 2>/dev/null || true
  remove_labels

  # Discover topology and label pods
  echo "[${i}] Discovering cluster topology..."
  mapfile -t MINORITY_PODS < <(discover_minority)
  MINORITY_CSV="$(IFS=,; echo "${MINORITY_PODS[*]}")"
  echo "[${i}] Minority pods: ${MINORITY_CSV}"

  echo "[${i}] Labeling pods (minority/majority)..."
  label_pods "${MINORITY_PODS[@]}"

  kubectl get pods -n "${NS}" -l "${VALKEY_POD_SELECTOR}" \
    --show-labels --no-headers | grep -E 'chaos-side' || true

  # Start checker pod
  echo "[${i}] Starting split-brain checker pod (duration=${DURATION}s)..."
  kubectl run "${POD_NAME}" -n "${NS}" \
    --image="${IMAGE}" \
    --restart=Never \
    --command -- \
    /bin/sh -c "
      mkdir -p '${REMOTE_OUT}'
      python -u /work/split_brain_check.py \
        --host '${HOST}' \
        --port '${PORT}' \
        --duration '${DURATION}' \
        --run-id '${RUN_ID}' \
        --output '${REMOTE_FILE}' \
        --clients '${SPLIT_BRAIN_CLIENTS}' \
        --socket-timeout '${SPLIT_BRAIN_SOCKET_TIMEOUT}' \
        --connect-timeout '${SPLIT_BRAIN_CONNECT_TIMEOUT}' \
        --slow-threshold-ms '${SPLIT_BRAIN_SLOW_THRESHOLD_MS}' \
        --minority-pods '${MINORITY_CSV}' \
        ${CHECKER_RETRY_ARGS[*]} \
        ${CHECKER_TLS_ARGS[*]}
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

  # Inject split-brain partition
  echo "[${i}] Injecting split-brain partition..."
  CHAOS_EPOCH_S="$(kubectl exec "${POD_NAME}" -n "${NS}" -- date +%s 2>/dev/null || date +%s)"
  kubectl apply -f "${CHAOS_YAML}"

  if wait_for_networkchaos_injected "${CHAOS_NAME}" 60; then
    echo "[${i}] Split-brain partition injected."
  else
    echo "[${i}] ERROR: NetworkChaos was not fully injected."
    save_run_logs "${i}" "${POD_NAME}"
    kubectl delete networkchaos "${CHAOS_NAME}" -n "${NS}" --ignore-not-found 2>/dev/null || true
    remove_labels
    exit 1
  fi

  # Save timing metadata
  cat > "${LOCAL_OUT}/split_brain_timing_${i}.json" <<EOF
{
  "chaos_epoch_s": ${CHAOS_EPOCH_S},
  "steady_state_wait_s": ${STEADY_STATE_WAIT},
  "minority_pods": $(printf '%s' "${MINORITY_CSV}" | python3 -c "import sys,json; print(json.dumps(sys.stdin.read().split(',')))"),
  "chaos_yaml": "$(basename "${CHAOS_YAML}")"
}
EOF

  echo "[${i}] Waiting for checker to finish..."
  if ! wait_for_pod_marker "${NS}" "${POD_NAME}" "${POD_DONE_FILE}" 600; then
    echo "[${i}] ERROR: checker pod did not signal completion."
    save_run_logs "${i}" "${POD_NAME}"
    remove_labels
    exit 1
  fi

  exit_code="$(read_pod_exit_code "${NS}" "${POD_NAME}" "${POD_EXIT_CODE_FILE}")"
  if [[ -z "${exit_code}" || "${exit_code}" != "0" ]]; then
    echo "[${i}] ERROR: checker exited with code ${exit_code:-unknown}."
    save_run_logs "${i}" "${POD_NAME}"
    remove_labels
    exit 1
  fi

  echo "[${i}] Saving logs..."
  save_run_logs "${i}" "${POD_NAME}"

  echo "[${i}] Copying results..."
  kubectl cp "${NS}/${POD_NAME}:${REMOTE_FILE}" "${LOCAL_OUT}/${OUT_FILE}"

  echo "[${i}] Cleaning up..."
  kubectl delete networkchaos "${CHAOS_NAME}" -n "${NS}" --ignore-not-found 2>/dev/null || true
  kubectl delete pod "${POD_NAME}" -n "${NS}" --ignore-not-found
  remove_labels

  echo "[${i}] Waiting for Valkey cluster to stabilize..."
  kubectl rollout status sts/valkey -n "${NS}" --timeout=120s
  sleep 15

  echo "[${i}] Done. Result: ${LOCAL_OUT}/${OUT_FILE}"
done

echo ""
echo "=========================================="
echo "  All ${N} split-brain runs complete."
echo "  Results in: ${LOCAL_OUT}/"
echo "  Analyse with:"
echo "    python cli.py split-brain --input ${LOCAL_OUT} --output-dir ./plots/split_brain"
echo "=========================================="

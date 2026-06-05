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
NS="${NS:-vk}"
RELEASE="${RELEASE:-valkey}"
STS="${STS:-${RELEASE}}"
CLI_POD="${VALKEY_CLI_POD:-${STS}-0}"
SERVICE_NAME="${SERVICE_NAME:-${RELEASE}}"
IMAGE="${CONSISTENCY_IMAGE:-consistency_checker:2}"
LOCAL_OUT="${1:-./results/split_brain}"
REMOTE_OUT="/work/results/split_brain"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CHAOS_YAML="${CHAOS_YAML:-}"
CHAOS_NAME="${CHAOS_NAME:-valkey-split-brain}"
CHAOS_NAMESPACE="${CHAOS_NAMESPACE:-${NS}}"
CHAOS_MESH_NAMESPACE="${CHAOS_MESH_NAMESPACE:-chaos-mesh}"
CHAOS_DAEMON_NAME="${CHAOS_DAEMON_NAME:-chaos-daemon}"
VALKEY_POD_SELECTOR="${VALKEY_POD_SELECTOR:-app.kubernetes.io/name=valkey,app.kubernetes.io/instance=${RELEASE}}"

source "${SCRIPT_DIR}/pod_results.sh"

HOST="${SPLIT_BRAIN_HOST:-${SERVICE_NAME}.${NS}.svc.cluster.local}"
PORT="${SPLIT_BRAIN_PORT:-6379}"
DURATION="${SPLIT_BRAIN_DURATION:-120}"
STEADY_STATE_WAIT="${SPLIT_BRAIN_STEADY_STATE_WAIT:-30}"
if [[ -n "${SPLIT_BRAIN_CHAOS_DURATION:-}" ]]; then
  CHAOS_DURATION="${SPLIT_BRAIN_CHAOS_DURATION}"
else
  if (( DURATION <= STEADY_STATE_WAIT )); then
    echo "ERROR: SPLIT_BRAIN_DURATION must be greater than SPLIT_BRAIN_STEADY_STATE_WAIT" >&2
    exit 1
  fi
  CHAOS_DURATION="$((DURATION - STEADY_STATE_WAIT))s"
fi
CHAOS_INJECT_TIMEOUT="${SPLIT_BRAIN_CHAOS_INJECT_TIMEOUT:-60}"
POD_COMPLETION_TIMEOUT="${SPLIT_BRAIN_COMPLETION_TIMEOUT:-$((DURATION + 300))}"
ROLLOUT_TIMEOUT="${ROLLOUT_TIMEOUT:-120s}"
CLUSTER_HEALTH_TIMEOUT="${SPLIT_BRAIN_CLUSTER_HEALTH_TIMEOUT:-120}"
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

print_config() {
  cat <<EOF
==> Split-brain benchmark configuration
N=${N}
NS=${NS}
RELEASE=${RELEASE}
STS=${STS}
CLI_POD=${CLI_POD}
SERVICE_NAME=${SERVICE_NAME}
HOST=${HOST}
PORT=${PORT}
IMAGE=${IMAGE}
LOCAL_OUT=${LOCAL_OUT}
REMOTE_OUT=${REMOTE_OUT}
VALKEY_POD_SELECTOR=${VALKEY_POD_SELECTOR}
CHAOS_NAME=${CHAOS_NAME}
CHAOS_NAMESPACE=${CHAOS_NAMESPACE}
CHAOS_MESH_NAMESPACE=${CHAOS_MESH_NAMESPACE}
CHAOS_DAEMON_NAME=${CHAOS_DAEMON_NAME}
CHAOS_YAML=${CHAOS_YAML:-generated}
DURATION=${DURATION}
STEADY_STATE_WAIT=${STEADY_STATE_WAIT}
CHAOS_DURATION=${CHAOS_DURATION}
CHAOS_INJECT_TIMEOUT=${CHAOS_INJECT_TIMEOUT}
POD_COMPLETION_TIMEOUT=${POD_COMPLETION_TIMEOUT}
CLUSTER_HEALTH_TIMEOUT=${CLUSTER_HEALTH_TIMEOUT}
SPLIT_BRAIN_CLIENTS=${SPLIT_BRAIN_CLIENTS}
SPLIT_BRAIN_SOCKET_TIMEOUT=${SPLIT_BRAIN_SOCKET_TIMEOUT}
SPLIT_BRAIN_CONNECT_TIMEOUT=${SPLIT_BRAIN_CONNECT_TIMEOUT}
SPLIT_BRAIN_RETRY_ON_TIMEOUT=${SPLIT_BRAIN_RETRY_ON_TIMEOUT}
SPLIT_BRAIN_SLOW_THRESHOLD_MS=${SPLIT_BRAIN_SLOW_THRESHOLD_MS}
SPLIT_BRAIN_TLS=${SPLIT_BRAIN_TLS}
SPLIT_BRAIN_TLS_SKIP_VERIFY=${SPLIT_BRAIN_TLS_SKIP_VERIFY}
EOF
}

write_yaml_label_selectors() {
  local selector_string="$1"
  local indent="$2"
  local selector key value

  IFS=',' read -r -a selectors <<< "${selector_string}"
  for selector in "${selectors[@]}"; do
    selector="${selector//[[:space:]]/}"
    if [[ -z "${selector}" ]]; then
      continue
    fi
    if [[ "${selector}" != *=* || "${selector}" == *"!="* ]]; then
      echo "ERROR: VALKEY_POD_SELECTOR only supports comma-separated key=value selectors for NetworkChaos generation: ${selector}" >&2
      return 1
    fi
    key="${selector%%=*}"
    value="${selector#*=}"
    printf "%*s%s: \"%s\"\n" "${indent}" "" "${key}" "${value}"
  done
}

write_networkchaos_manifest() {
  local output_file="$1"

  {
    cat <<EOF
apiVersion: chaos-mesh.org/v1alpha1
kind: NetworkChaos
metadata:
  name: ${CHAOS_NAME}
  namespace: ${CHAOS_NAMESPACE}
spec:
  action: partition
  mode: all
  selector:
    namespaces:
      - ${NS}
    labelSelectors:
EOF
    write_yaml_label_selectors "${VALKEY_POD_SELECTOR}" 6
    cat <<EOF
      chaos-side: "minority"
  direction: both
  target:
    mode: all
    selector:
      namespaces:
        - ${NS}
      labelSelectors:
EOF
    write_yaml_label_selectors "${VALKEY_POD_SELECTOR}" 8
    cat <<EOF
        chaos-side: "majority"
  duration: "${CHAOS_DURATION}"
EOF
  } > "${output_file}"
}

prepare_chaos_manifest() {
  if [[ -n "${CHAOS_YAML}" ]]; then
    CHAOS_FILE="${CHAOS_YAML}"
    return 0
  fi

  GENERATED_CHAOS_FILE="$(mktemp /tmp/split-brain-networkchaos.XXXXXX.yaml)"
  write_networkchaos_manifest "${GENERATED_CHAOS_FILE}"
  CHAOS_FILE="${GENERATED_CHAOS_FILE}"
  cp "${CHAOS_FILE}" "${LOCAL_OUT}/split_brain_networkchaos_manifest.yaml"
}

cleanup_generated_manifest() {
  if [[ -n "${GENERATED_CHAOS_FILE:-}" ]]; then
    rm -f "${GENERATED_CHAOS_FILE}"
  fi
}

preflight() {
  if ! kubectl api-resources --api-group=chaos-mesh.org 2>/dev/null \
      | awk '$1 == "networkchaos" {found=1} END {exit !found}'; then
    echo "ERROR: Chaos Mesh NetworkChaos CRD is not available. Install Chaos Mesh first." >&2
    return 1
  fi

  kubectl get sts "${STS}" -n "${NS}" >/dev/null
  kubectl rollout status "sts/${STS}" -n "${NS}" --timeout="${ROLLOUT_TIMEOUT}"
  kubectl get pod "${CLI_POD}" -n "${NS}" >/dev/null
  wait_for_cluster_ok "${CLUSTER_HEALTH_TIMEOUT}"
}

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
  # Returns "<pod-name>|<slot-ranges>".
  local nodes_output
  nodes_output="$(kubectl exec "${CLI_POD}" -n "${NS}" -- \
    valkey-cli "${VALKEY_CLI_ARGS[@]}" cluster nodes 2>/dev/null)"

  local target_master_id target_master_pod target_master_slots
  target_master_id="$(echo "${nodes_output}" | awk '$3 ~ /master/ && $3 !~ /fail/ {print $1; exit}')"
  target_master_pod="$(echo "${nodes_output}" | awk -v id="${target_master_id}" \
    '$1 == id {addr=$2; sub(/:.*/, "", addr); sub(/,.*/, "", addr); print addr}')"
  target_master_slots="$(echo "${nodes_output}" | awk -v id="${target_master_id}" '
    $1 == id {
      sep = ""
      for (i = 9; i <= NF; i++) {
        if ($i ~ /^\[/) {
          continue
        }
        printf "%s%s", sep, $i
        sep = ","
      }
    }')"

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

  if [[ -z "${target_master_slots}" ]]; then
    echo "ERROR: could not find slot ownership for master ${target_master_id}" >&2
    return 1
  fi

  local master_pod_name
  master_pod_name="$(resolve_address_to_pod "${target_master_pod}")"

  echo "${master_pod_name}|${target_master_slots}"
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

wait_for_cluster_ok() {
  local timeout_s="${1:-120}"
  local elapsed=0
  local info state slots_assigned slots_ok

  while [[ "${elapsed}" -lt "${timeout_s}" ]]; do
    info="$(kubectl exec "${CLI_POD}" -n "${NS}" -- \
      valkey-cli "${VALKEY_CLI_ARGS[@]}" cluster info 2>/dev/null || true)"
    state="$(awk -F: '$1 == "cluster_state" {gsub(/\r/, "", $2); print $2}' <<<"${info}")"
    slots_assigned="$(awk -F: '$1 == "cluster_slots_assigned" {gsub(/\r/, "", $2); print $2}' <<<"${info}")"
    slots_ok="$(awk -F: '$1 == "cluster_slots_ok" {gsub(/\r/, "", $2); print $2}' <<<"${info}")"

    if [[ "${state}" == "ok" && "${slots_assigned}" == "16384" && "${slots_ok}" == "16384" ]]; then
      return 0
    fi

    sleep 2
    elapsed=$((elapsed + 2))
  done

  echo "ERROR: Valkey cluster did not become healthy within ${timeout_s}s" >&2
  kubectl exec "${CLI_POD}" -n "${NS}" -- \
    valkey-cli "${VALKEY_CLI_ARGS[@]}" cluster info >&2 || true
  return 1
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

cleanup_run_resources() {
  local pod_name="$1"

  kubectl delete networkchaos "${CHAOS_NAME}" -n "${CHAOS_NAMESPACE}" --ignore-not-found 2>/dev/null || true
  kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found 2>/dev/null || true
  remove_labels
}

wait_for_networkchaos_injected() {
  local name="$1"
  local timeout_s="${2:-60}"
  local elapsed=0

  while [[ "${elapsed}" -lt "${timeout_s}" ]]; do
    local selected injected
    selected="$(kubectl get networkchaos "${name}" -n "${CHAOS_NAMESPACE}" \
      -o jsonpath='{.status.conditions[?(@.type=="Selected")].status}' 2>/dev/null || true)"
    injected="$(kubectl get networkchaos "${name}" -n "${CHAOS_NAMESPACE}" \
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
  kubectl get networkchaos "${CHAOS_NAME}" -n "${CHAOS_NAMESPACE}" -o yaml \
    > "${prefix}_networkchaos.yaml" 2>&1 || true
  kubectl describe networkchaos "${CHAOS_NAME}" -n "${CHAOS_NAMESPACE}" \
    > "${prefix}_networkchaos_describe.txt" 2>&1 || true
  kubectl logs -n "${CHAOS_MESH_NAMESPACE}" "ds/${CHAOS_DAEMON_NAME}" --tail=500 \
    > "${prefix}_chaos_daemon.log" 2>&1 || true
}

# -- Main loop -----------------------------------------------------------

GENERATED_CHAOS_FILE=""
CHAOS_FILE=""

print_config
prepare_chaos_manifest
trap cleanup_generated_manifest EXIT
preflight

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
  kubectl delete networkchaos "${CHAOS_NAME}" -n "${CHAOS_NAMESPACE}" --ignore-not-found 2>/dev/null || true
  kubectl delete pod "${POD_NAME}" -n "${NS}" --ignore-not-found 2>/dev/null || true
  remove_labels
  wait_for_cluster_ok "${CLUSTER_HEALTH_TIMEOUT}"

  # Discover topology and label pods
  echo "[${i}] Discovering cluster topology..."
  MINORITY_INFO="$(discover_minority)"
  MINORITY_CSV="${MINORITY_INFO%%|*}"
  MINORITY_SLOTS_CSV="${MINORITY_INFO#*|}"
  IFS=',' read -r -a MINORITY_PODS <<< "${MINORITY_CSV}"
  echo "[${i}] Minority pods: ${MINORITY_CSV}"
  echo "[${i}] Minority slots: ${MINORITY_SLOTS_CSV}"

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
        --minority-slots '${MINORITY_SLOTS_CSV}' \
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
  kubectl apply -f "${CHAOS_FILE}"

  if wait_for_networkchaos_injected "${CHAOS_NAME}" "${CHAOS_INJECT_TIMEOUT}"; then
    echo "[${i}] Split-brain partition injected."
  else
    echo "[${i}] ERROR: NetworkChaos was not fully injected."
    save_run_logs "${i}" "${POD_NAME}"
    cleanup_run_resources "${POD_NAME}"
    exit 1
  fi

  # Save timing metadata
  cat > "${LOCAL_OUT}/split_brain_timing_${i}.json" <<EOF
{
  "chaos_epoch_s": ${CHAOS_EPOCH_S},
  "steady_state_wait_s": ${STEADY_STATE_WAIT},
  "chaos_duration": "${CHAOS_DURATION}",
  "chaos_namespace": "${CHAOS_NAMESPACE}",
  "minority_pods": $(printf '%s' "${MINORITY_CSV}" | python3 -c "import sys,json; print(json.dumps(sys.stdin.read().split(',')))"),
  "minority_slots": $(printf '%s' "${MINORITY_SLOTS_CSV}" | python3 -c "import sys,json; print(json.dumps(sys.stdin.read().split(',')))"),
  "chaos_yaml": "$(basename "${CHAOS_FILE}")"
}
EOF

  echo "[${i}] Waiting for checker to finish..."
  if ! wait_for_pod_marker "${NS}" "${POD_NAME}" "${POD_DONE_FILE}" "${POD_COMPLETION_TIMEOUT}"; then
    echo "[${i}] ERROR: checker pod did not signal completion."
    save_run_logs "${i}" "${POD_NAME}"
    cleanup_run_resources "${POD_NAME}"
    exit 1
  fi

  exit_code="$(read_pod_exit_code "${NS}" "${POD_NAME}" "${POD_EXIT_CODE_FILE}")"
  if [[ -z "${exit_code}" || "${exit_code}" != "0" ]]; then
    echo "[${i}] ERROR: checker exited with code ${exit_code:-unknown}."
    save_run_logs "${i}" "${POD_NAME}"
    cleanup_run_resources "${POD_NAME}"
    exit 1
  fi

  echo "[${i}] Saving logs..."
  save_run_logs "${i}" "${POD_NAME}"

  echo "[${i}] Copying results..."
  kubectl cp "${NS}/${POD_NAME}:${REMOTE_FILE}" "${LOCAL_OUT}/${OUT_FILE}"

  echo "[${i}] Cleaning up..."
  cleanup_run_resources "${POD_NAME}"

  echo "[${i}] Waiting for Valkey cluster to stabilize..."
  kubectl rollout status "sts/${STS}" -n "${NS}" --timeout="${ROLLOUT_TIMEOUT}"
  wait_for_cluster_ok "${CLUSTER_HEALTH_TIMEOUT}"
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

#!/usr/bin/env bash
set -euo pipefail

N="${N:-1}"
PROVIDER="${PROVIDER:-valkey}"
case "${PROVIDER}" in
  valkey)
    DEFAULT_NS="vk"
    DEFAULT_RELEASE="valkey"
    DEFAULT_CLI_BIN="valkey-cli"
    DEFAULT_SYSTEM_NAME="Valkey"
    ;;
  redis|redis72)
    PROVIDER="redis72"
    DEFAULT_NS="redis"
    DEFAULT_RELEASE="redis72"
    DEFAULT_CLI_BIN="redis-cli"
    DEFAULT_SYSTEM_NAME="Redis 7.2"
    ;;
  *)
    echo "ERROR: PROVIDER must be valkey or redis72." >&2
    exit 1
    ;;
esac

NS="${NS:-${DEFAULT_NS}}"
RELEASE="${RELEASE:-${DEFAULT_RELEASE}}"

case "${PROVIDER}" in
  valkey)
    DEFAULT_STS="${RELEASE}"
    DEFAULT_SERVICE_NAME="${RELEASE}"
    DEFAULT_POD_SELECTOR="app.kubernetes.io/name=valkey,app.kubernetes.io/instance=${RELEASE}"
    ;;
  redis72)
    DEFAULT_STS="${RELEASE}-redis-cluster"
    DEFAULT_SERVICE_NAME="${DEFAULT_STS}"
    DEFAULT_POD_SELECTOR="app.kubernetes.io/name=redis-cluster,app.kubernetes.io/instance=${RELEASE}"
    ;;
esac

STS="${STS:-${DEFAULT_STS}}"
CLI_POD="${CLI_POD:-${STS}-0}"
CLI_BIN="${CLI_BIN:-${DEFAULT_CLI_BIN}}"
SYSTEM_NAME="${SYSTEM_NAME:-${DEFAULT_SYSTEM_NAME}}"
SERVICE_NAME="${SERVICE_NAME:-${DEFAULT_SERVICE_NAME}}"
POD_SELECTOR="${POD_SELECTOR:-${DEFAULT_POD_SELECTOR}}"
CHAOS_PREFIX="${CHAOS_PREFIX:-${RELEASE}}"
CLIENT_ENGINE="${FAILOVER_CLIENT_ENGINE:-memtier}"
case "${CLIENT_ENGINE}" in
  memtier)
    DEFAULT_IMAGE="${MEMTIER_IMAGE:-memtier_k8s:1}"
    DEFAULT_CLIENTS="25"
    DEFAULT_PIPELINE="10"
    ;;
  python|client|failover-client)
    CLIENT_ENGINE="python"
    DEFAULT_IMAGE="${CONSISTENCY_IMAGE:-consistency_checker:2}"
    DEFAULT_CLIENTS="16"
    DEFAULT_PIPELINE="1"
    ;;
  *)
    echo "ERROR: FAILOVER_CLIENT_ENGINE must be memtier or python." >&2
    exit 1
    ;;
esac
IMAGE="${FAILOVER_IMAGE:-${DEFAULT_IMAGE}}"
LOCAL_OUT="${1:-./results/failover}"
REMOTE_OUT="/work/results/failover"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

source "${SCRIPT_DIR}/pod_results.sh"

HOST="${FAILOVER_HOST:-${SERVICE_NAME}.${NS}.svc.cluster.local}"
PORT="${FAILOVER_PORT:-6379}"
THREADS="${FAILOVER_THREADS:-4}"
CLIENTS="${FAILOVER_CLIENTS:-${DEFAULT_CLIENTS}}"
PIPELINE="${FAILOVER_PIPELINE:-${DEFAULT_PIPELINE}}"
TEST_TIME="${FAILOVER_TEST_TIME:-120}"
KEYS="${FAILOVER_KEYS:-100000}"
DATA_SIZE="${FAILOVER_DATA_SIZE:-1024}"
RATIO="${FAILOVER_RATIO:-1:1}"
STEADY_STATE_WAIT="${FAILOVER_STEADY_STATE_WAIT:-30}"
FAILOVER_MODE="${FAILOVER_MODE:-masters}"
FAILOVER_MASTERS="${FAILOVER_MASTERS:-1}"
FAILOVER_ACTION="${FAILOVER_ACTION:-pod-failure}"
FAILOVER_DURATION="${FAILOVER_DURATION:-60s}"
FAILOVER_GRACE_PERIOD="${FAILOVER_GRACE_PERIOD:-1}"
WORKLOAD_STARTED_FILE="${FAILOVER_STARTED_FILE:-/tmp/failover.started}"
MEMTIER_STARTED_FILE="${WORKLOAD_STARTED_FILE}"
MEMTIER_TLS="${MEMTIER_TLS:-false}"
MEMTIER_TLS_SKIP_VERIFY="${MEMTIER_TLS_SKIP_VERIFY:-true}"
MEMTIER_TLS_CACERT="${MEMTIER_TLS_CACERT:-}"
MEMTIER_TLS_CERT="${MEMTIER_TLS_CERT:-}"
MEMTIER_TLS_KEY="${MEMTIER_TLS_KEY:-}"
MEMTIER_TLS_SNI="${MEMTIER_TLS_SNI:-}"
MEMTIER_RECONNECT_ON_ERROR="${MEMTIER_RECONNECT_ON_ERROR:-false}"
MEMTIER_MAX_RECONNECT_ATTEMPTS="${MEMTIER_MAX_RECONNECT_ATTEMPTS:-1000}"
MEMTIER_RECONNECT_BACKOFF_FACTOR="${MEMTIER_RECONNECT_BACKOFF_FACTOR:-2}"
FAILOVER_SOCKET_TIMEOUT="${FAILOVER_SOCKET_TIMEOUT:-1.0}"
FAILOVER_CONNECT_TIMEOUT="${FAILOVER_CONNECT_TIMEOUT:-1.0}"
FAILOVER_RETRY_ON_TIMEOUT="${FAILOVER_RETRY_ON_TIMEOUT:-false}"
FAILOVER_RECONNECT_BACKOFF_S="${FAILOVER_RECONNECT_BACKOFF_S:-0.01}"
FAILOVER_LATENCY_SAMPLE_LIMIT_PER_SECOND="${FAILOVER_LATENCY_SAMPLE_LIMIT_PER_SECOND:-5000}"
FAILOVER_LATENCY_SAMPLE_LIMIT_TOTAL="${FAILOVER_LATENCY_SAMPLE_LIMIT_TOTAL:-200000}"
FAILOVER_ERROR_SAMPLE_LIMIT="${FAILOVER_ERROR_SAMPLE_LIMIT:-100}"
FAILOVER_RANDOM_SEED="${FAILOVER_RANDOM_SEED:-1}"
CLI_TLS="${FAILOVER_CLI_TLS:-${VALKEY_CLI_TLS:-${MEMTIER_TLS}}}"
CLI_TLS_SKIP_VERIFY="${FAILOVER_CLI_TLS_SKIP_VERIFY:-${VALKEY_CLI_TLS_SKIP_VERIFY:-${MEMTIER_TLS_SKIP_VERIFY}}}"
CLI_CACERT="${FAILOVER_CLI_CACERT:-${VALKEY_CLI_CACERT:-/tls/ca.crt}}"

mkdir -p "${LOCAL_OUT}"

MEMTIER_TLS_ARGS=()
case "${MEMTIER_TLS}" in
  1|true|TRUE|yes|YES|on|ON)
    MEMTIER_TLS_ARGS+=(--tls)
    case "${MEMTIER_TLS_SKIP_VERIFY}" in
      1|true|TRUE|yes|YES|on|ON)
        MEMTIER_TLS_ARGS+=(--tls-skip-verify)
        ;;
    esac
    if [[ -n "${MEMTIER_TLS_CACERT}" ]]; then
      MEMTIER_TLS_ARGS+=(--cacert="${MEMTIER_TLS_CACERT}")
    fi
    if [[ -n "${MEMTIER_TLS_CERT}" ]]; then
      MEMTIER_TLS_ARGS+=(--cert="${MEMTIER_TLS_CERT}")
    fi
    if [[ -n "${MEMTIER_TLS_KEY}" ]]; then
      MEMTIER_TLS_ARGS+=(--key="${MEMTIER_TLS_KEY}")
    fi
    if [[ -n "${MEMTIER_TLS_SNI}" ]]; then
      MEMTIER_TLS_ARGS+=(--sni="${MEMTIER_TLS_SNI}")
    fi
    ;;
esac

MEMTIER_RECONNECT_ARGS=()
case "${MEMTIER_RECONNECT_ON_ERROR}" in
  1|true|TRUE|yes|YES|on|ON)
    MEMTIER_RECONNECT_ARGS+=(--reconnect-on-error)
    MEMTIER_RECONNECT_ARGS+=(--max-reconnect-attempts="${MEMTIER_MAX_RECONNECT_ATTEMPTS}")
    MEMTIER_RECONNECT_ARGS+=(--reconnect-backoff-factor="${MEMTIER_RECONNECT_BACKOFF_FACTOR}")
    ;;
esac

PYTHON_CLIENT_TLS_ARGS=()
case "${MEMTIER_TLS}" in
  1|true|TRUE|yes|YES|on|ON)
    PYTHON_CLIENT_TLS_ARGS+=(--tls)
    case "${MEMTIER_TLS_SKIP_VERIFY}" in
      1|true|TRUE|yes|YES|on|ON)
        PYTHON_CLIENT_TLS_ARGS+=(--tls-skip-verify)
        ;;
    esac
    if [[ -n "${MEMTIER_TLS_CACERT}" ]]; then
      PYTHON_CLIENT_TLS_ARGS+=(--tls-ca-cert="${MEMTIER_TLS_CACERT}")
    fi
    if [[ -n "${MEMTIER_TLS_CERT}" ]]; then
      PYTHON_CLIENT_TLS_ARGS+=(--tls-cert="${MEMTIER_TLS_CERT}")
    fi
    if [[ -n "${MEMTIER_TLS_KEY}" ]]; then
      PYTHON_CLIENT_TLS_ARGS+=(--tls-key="${MEMTIER_TLS_KEY}")
    fi
    ;;
esac

PYTHON_CLIENT_RETRY_ARGS=(--no-retry-on-timeout)
case "${FAILOVER_RETRY_ON_TIMEOUT}" in
  1|true|TRUE|yes|YES|on|ON)
    PYTHON_CLIENT_RETRY_ARGS=(--retry-on-timeout)
    ;;
esac

CLI_ARGS=()
case "${CLI_TLS}" in
  1|true|TRUE|yes|YES|on|ON)
    CLI_ARGS+=(--tls)
    case "${CLI_TLS_SKIP_VERIFY}" in
      1|true|TRUE|yes|YES|on|ON)
        CLI_ARGS+=(--insecure)
        ;;
      *)
        CLI_ARGS+=(--cacert "${CLI_CACERT}")
        ;;
    esac
    ;;
esac

case "${FAILOVER_ACTION}" in
  pod-kill|pod-failure)
    ;;
  *)
    echo "ERROR: FAILOVER_ACTION must be pod-kill or pod-failure." >&2
    exit 1
    ;;
esac

selector_label_yaml() {
  local selector="$1"
  local pair key value

  IFS=',' read -ra pairs <<<"${selector}"
  for pair in "${pairs[@]}"; do
    key="${pair%%=*}"
    value="${pair#*=}"
    [[ -n "${key}" && -n "${value}" && "${key}" != "${value}" ]] || continue
    printf '      %s: "%s"\n' "${key}" "${value}"
  done
}

print_config() {
  cat <<EOF
==> Failover benchmark configuration
N=${N}
PROVIDER=${PROVIDER}
SYSTEM_NAME=${SYSTEM_NAME}
CLIENT_ENGINE=${CLIENT_ENGINE}
NS=${NS}
RELEASE=${RELEASE}
STS=${STS}
CLI_POD=${CLI_POD}
CLI_BIN=${CLI_BIN}
SERVICE_NAME=${SERVICE_NAME}
HOST=${HOST}
PORT=${PORT}
IMAGE=${IMAGE}
LOCAL_OUT=${LOCAL_OUT}
REMOTE_OUT=${REMOTE_OUT}
POD_SELECTOR=${POD_SELECTOR}
THREADS=${THREADS}
CLIENTS=${CLIENTS}
PIPELINE=${PIPELINE}
TEST_TIME=${TEST_TIME}
KEYS=${KEYS}
DATA_SIZE=${DATA_SIZE}
RATIO=${RATIO}
STEADY_STATE_WAIT=${STEADY_STATE_WAIT}
MEMTIER_RECONNECT_ON_ERROR=${MEMTIER_RECONNECT_ON_ERROR}
MEMTIER_MAX_RECONNECT_ATTEMPTS=${MEMTIER_MAX_RECONNECT_ATTEMPTS}
MEMTIER_RECONNECT_BACKOFF_FACTOR=${MEMTIER_RECONNECT_BACKOFF_FACTOR}
FAILOVER_SOCKET_TIMEOUT=${FAILOVER_SOCKET_TIMEOUT}
FAILOVER_CONNECT_TIMEOUT=${FAILOVER_CONNECT_TIMEOUT}
FAILOVER_RETRY_ON_TIMEOUT=${FAILOVER_RETRY_ON_TIMEOUT}
FAILOVER_RECONNECT_BACKOFF_S=${FAILOVER_RECONNECT_BACKOFF_S}
FAILOVER_MODE=${FAILOVER_MODE}
FAILOVER_MASTERS=${FAILOVER_MASTERS}
FAILOVER_ACTION=${FAILOVER_ACTION}
FAILOVER_DURATION=${FAILOVER_DURATION}
FAILOVER_GRACE_PERIOD=${FAILOVER_GRACE_PERIOD}
EOF
}

podchaos_action_fields() {
  case "${FAILOVER_ACTION}" in
    pod-kill)
      printf '  gracePeriod: %s\n' "${FAILOVER_GRACE_PERIOD}"
      ;;
    pod-failure)
      printf '  duration: %s\n' "${FAILOVER_DURATION}"
      ;;
  esac
}

cluster_nodes() {
  kubectl exec "${CLI_POD}" -n "${NS}" -- \
    "${CLI_BIN}" "${CLI_ARGS[@]}" cluster nodes 2>/dev/null
}

wait_for_cluster_health() {
  local timeout_s="${1:-120}"
  local start elapsed info state slots masters
  start="$(date +%s)"

  while true; do
    info="$(kubectl exec "${CLI_POD}" -n "${NS}" -- \
      "${CLI_BIN}" "${CLI_ARGS[@]}" cluster info 2>/dev/null || true)"
    state="$(awk -F: '$1=="cluster_state" {gsub(/\r/, "", $2); print $2}' <<<"${info}")"
    slots="$(awk -F: '$1=="cluster_slots_ok" {gsub(/\r/, "", $2); print $2}' <<<"${info}")"
    masters="$(cluster_nodes | awk '$3 ~ /master/ && $3 !~ /fail/ {count++} END {print count + 0}' || true)"

    if [[ "${state}" == "ok" && "${slots}" == "16384" && "${masters:-0}" -ge 3 ]]; then
      return 0
    fi

    elapsed="$(( $(date +%s) - start ))"
    if (( elapsed >= timeout_s )); then
      echo "ERROR: ${SYSTEM_NAME} cluster did not become healthy after ${timeout_s}s." >&2
      echo "cluster_state=${state:-unknown} cluster_slots_ok=${slots:-unknown} masters=${masters:-unknown}" >&2
      return 1
    fi
    sleep 5
  done
}

resolve_pod_name() {
  local host="$1"
  local pod_table pod_name candidate

  candidate="${host%%.*}"
  if kubectl get pod "${candidate}" -n "${NS}" >/dev/null 2>&1; then
    echo "${candidate}"
    return 0
  fi

  pod_table="$(
    kubectl get pods -n "${NS}" \
      -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.status.podIP}{"\n"}{end}' \
      2>/dev/null
  )"
  pod_name="$(
    awk -v target="${host}" '$1 == target || $2 == target {print $1; exit}' <<<"${pod_table}"
  )"

  if [[ -z "${pod_name}" ]]; then
    return 1
  fi

  echo "${pod_name}"
}

get_target_master_pods() {
  local count="$1"
  local addr host dns_name pod_name
  local -a targets=()

  while IFS= read -r addr; do
    [[ -z "${addr}" ]] && continue
    host="${addr%%:*}"
    dns_name=""
    if [[ "${addr}" == *,* ]]; then
      dns_name="${addr#*,}"
      dns_name="${dns_name%%.*}"
    fi

    pod_name=""
    if pod_name="$(resolve_pod_name "${host}")"; then
      :
    elif [[ -n "${dns_name}" ]]; then
      pod_name="${dns_name}"
    fi

    if [[ -z "${pod_name}" ]]; then
      echo "WARN: could not resolve master address ${addr} to a pod name" >&2
      return 1
    fi

    targets+=("${pod_name}")
    if [[ "${#targets[@]}" -ge "${count}" ]]; then
      break
    fi
  done < <(
    cluster_nodes \
      | awk '$3 ~ /master/ && $3 !~ /fail/ {print $2}'
  )

  if [[ "${#targets[@]}" -lt "${count}" ]]; then
    return 1
  fi

  printf '%s\n' "${targets[@]}"
}

write_podchaos_manifest() {
  local manifest_path="$1"
  shift
  : > "${manifest_path}"

  local target_pod chaos_name first_doc=1
  for target_pod in "$@"; do
    chaos_name="${CHAOS_PREFIX}-master-kill-${target_pod}"
    if [[ "${first_doc}" -eq 0 ]]; then
      printf -- '---\n' >> "${manifest_path}"
    fi
    cat >> "${manifest_path}" <<EOF
apiVersion: chaos-mesh.org/v1alpha1
kind: PodChaos
metadata:
  name: ${chaos_name}
  namespace: ${NS}
spec:
  selector:
    pods:
      ${NS}:
        - ${target_pod}
  mode: one
  action: ${FAILOVER_ACTION}
$(podchaos_action_fields)
EOF
    first_doc=0
  done
}

write_podchaos_all_manifest() {
  local manifest_path="$1"
  cat > "${manifest_path}" <<EOF
apiVersion: chaos-mesh.org/v1alpha1
kind: PodChaos
metadata:
  name: ${CHAOS_PREFIX}-kill-all
  namespace: ${NS}
spec:
  selector:
    namespaces:
      - ${NS}
    labelSelectors:
$(selector_label_yaml "${POD_SELECTOR}")
  mode: all
  action: ${FAILOVER_ACTION}
$(podchaos_action_fields)
EOF
}

expected_master_chaos_names() {
  local target_pod
  for target_pod in "$@"; do
    echo "${CHAOS_PREFIX}-master-kill-${target_pod}"
  done
}

print_config

for i in $(seq 1 "${N}"); do
  if [[ "${CLIENT_ENGINE}" == "memtier" ]]; then
    POD_NAME="memtier-failover-${i}"
  else
    POD_NAME="failover-client-${i}"
  fi
  OUT_FILE="failover_run_${i}.json"
  LOG_FILE="failover_run_${i}.log"
  TIMING_FILE="failover_timing_${i}.json"
  if [[ "${FAILOVER_MODE}" != "masters" && "${FAILOVER_MODE}" != "all" ]]; then
    echo "[${i}] ERROR: FAILOVER_MODE must be 'masters' or 'all'."
    exit 1
  fi

  TARGET_DESC=""
  if [[ "${FAILOVER_MODE}" == "masters" ]]; then
    if ! [[ "${FAILOVER_MASTERS}" =~ ^[1-9][0-9]*$ ]]; then
      echo "[${i}] ERROR: FAILOVER_MASTERS must be a positive integer."
      exit 1
    fi

    mapfile -t TARGET_MASTER_PODS < <(get_target_master_pods "${FAILOVER_MASTERS}")
    if [[ "${#TARGET_MASTER_PODS[@]}" -lt "${FAILOVER_MASTERS}" ]]; then
      echo "[${i}] ERROR: could not determine ${FAILOVER_MASTERS} target master pod(s)."
      echo "[${i}] INFO: visible master nodes from ${CLI_POD}:"
      cluster_nodes | grep master || true
      exit 1
    fi
    TARGET_DESC="${FAILOVER_ACTION} for ${FAILOVER_MASTERS} master pod(s): ${TARGET_MASTER_PODS[*]}"
  fi

  CHAOS_FILE="$(mktemp /tmp/failover-chaos-XXXX.yaml)"
  if [[ "${FAILOVER_MODE}" == "masters" ]]; then
    write_podchaos_manifest "${CHAOS_FILE}" "${TARGET_MASTER_PODS[@]}"
  else
    write_podchaos_all_manifest "${CHAOS_FILE}"
    TARGET_DESC="${FAILOVER_ACTION} for all ${SYSTEM_NAME} pods"
  fi
  echo ""
  echo "=========================================="
  echo "  Failover run ${i}/${N}"
  echo "=========================================="

  echo "[${i}] Waiting for ${SYSTEM_NAME} cluster health before run..."
  wait_for_cluster_health 120

  kubectl delete -f "${CHAOS_FILE}" -n "${NS}" --ignore-not-found 2>/dev/null || true
  kubectl delete pod "${POD_NAME}" -n "${NS}" --ignore-not-found 2>/dev/null || true

  if [[ "${CLIENT_ENGINE}" == "memtier" ]]; then
    POD_COMMAND="$(cat <<EOF
mkdir -p '${REMOTE_OUT}'
LOG_PIPE='${REMOTE_OUT}/${LOG_FILE}.pipe'
rm -f '${WORKLOAD_STARTED_FILE}'
rm -f "\${LOG_PIPE}"
mkfifo "\${LOG_PIPE}"
(
  tr '\r' '\n' < "\${LOG_PIPE}" | while IFS= read -r line; do
    [ -n "\${line}" ] || continue
    line_ts="\$(date +%s)"
    printf '%s\t%s\n' "\${line_ts}" "\${line}"
    case "\${line}" in
      *"[RUN #1"*ops/sec*)
        if [ ! -f '${WORKLOAD_STARTED_FILE}' ]; then
          echo "\${line_ts}" > '${WORKLOAD_STARTED_FILE}'
        fi
        ;;
    esac
  done
) > '${REMOTE_OUT}/${LOG_FILE}' &
logger_pid=\$!
memtier_benchmark \
  --server='${HOST}' --port='${PORT}' \
  --protocol=redis \
  --cluster-mode \
  ${MEMTIER_TLS_ARGS[*]} \
  ${MEMTIER_RECONNECT_ARGS[*]} \
  --threads='${THREADS}' --clients='${CLIENTS}' \
  --pipeline='${PIPELINE}' \
  --test-time='${TEST_TIME}' \
  --key-maximum='${KEYS}' \
  --data-size='${DATA_SIZE}' \
  --ratio='${RATIO}' \
  --json-out-file '${REMOTE_OUT}/${OUT_FILE}' \
  --run-count 1 \
  --print-percentiles='50,95,99,99.9' \
  > "\${LOG_PIPE}" 2>&1
status=\$?
wait "\${logger_pid}" 2>/dev/null || true
rm -f "\${LOG_PIPE}"
echo "\$status" > '${POD_EXIT_CODE_FILE}'
touch '${POD_DONE_FILE}'
sleep '${POD_HOLD_SECONDS}'
EOF
)"
  else
    POD_COMMAND="$(cat <<EOF
mkdir -p '${REMOTE_OUT}'
rm -f '${WORKLOAD_STARTED_FILE}'
python -u /work/failover_client.py \
  --host '${HOST}' --port '${PORT}' \
  --duration '${TEST_TIME}' \
  --output '${REMOTE_OUT}/${OUT_FILE}' \
  --started-file '${WORKLOAD_STARTED_FILE}' \
  --provider '${PROVIDER}' \
  --system '${SYSTEM_NAME}' \
  --threads '${THREADS}' \
  --clients '${CLIENTS}' \
  --ratio '${RATIO}' \
  --keys '${KEYS}' \
  --data-size '${DATA_SIZE}' \
  --key-prefix 'failover.${PROVIDER}.${i}.' \
  --socket-timeout '${FAILOVER_SOCKET_TIMEOUT}' \
  --connect-timeout '${FAILOVER_CONNECT_TIMEOUT}' \
  --reconnect-backoff-s '${FAILOVER_RECONNECT_BACKOFF_S}' \
  --latency-sample-limit-per-second '${FAILOVER_LATENCY_SAMPLE_LIMIT_PER_SECOND}' \
  --latency-sample-limit-total '${FAILOVER_LATENCY_SAMPLE_LIMIT_TOTAL}' \
  --error-sample-limit '${FAILOVER_ERROR_SAMPLE_LIMIT}' \
  --seed '$((FAILOVER_RANDOM_SEED + i))' \
  ${PYTHON_CLIENT_RETRY_ARGS[*]} \
  ${PYTHON_CLIENT_TLS_ARGS[*]} \
  > '${REMOTE_OUT}/${LOG_FILE}' 2>&1
status=\$?
echo "\$status" > '${POD_EXIT_CODE_FILE}'
touch '${POD_DONE_FILE}'
sleep '${POD_HOLD_SECONDS}'
EOF
)"
  fi

  echo "[${i}] Starting ${CLIENT_ENGINE} pod (test-time=${TEST_TIME}s)..."
  kubectl run "${POD_NAME}" -n "${NS}" \
    --image="${IMAGE}" \
    --restart=Never \
    --command -- \
    /bin/sh -c "${POD_COMMAND}"

  echo "[${i}] Waiting for pod to start..."
  kubectl wait pod/"${POD_NAME}" -n "${NS}" \
    --for=condition=Ready --timeout=60s 2>/dev/null || true

  echo "[${i}] Waiting for ${CLIENT_ENGINE} timed run to start..."
  if ! wait_for_pod_marker "${NS}" "${POD_NAME}" "${WORKLOAD_STARTED_FILE}" 120; then
    echo "[${i}] ERROR: ${CLIENT_ENGINE} did not start its timed run."
    print_pod_debug_info "${NS}" "${POD_NAME}"
    exit 1
  fi

  echo "[${i}] Waiting ${STEADY_STATE_WAIT}s for steady state..."
  sleep "${STEADY_STATE_WAIT}"

  echo "[${i}] Injecting chaos: ${TARGET_DESC} (duration=${FAILOVER_DURATION}, gracePeriod=${FAILOVER_GRACE_PERIOD}s)..."
  echo "[${i}] Generated PodChaos manifest:"
  sed 's/^/  /' "${CHAOS_FILE}"
  CHAOS_EPOCH_S="$(kubectl exec "${POD_NAME}" -n "${NS}" -- date +%s 2>/dev/null || date +%s)"
  WORKLOAD_STARTED_EPOCH_S="$(kubectl exec "${POD_NAME}" -n "${NS}" -- cat "${WORKLOAD_STARTED_FILE}" 2>/dev/null | tr -d '[:space:]')"
  cat > "${LOCAL_OUT}/${TIMING_FILE}" <<EOF
{
  "client_engine": "${CLIENT_ENGINE}",
  "workload_started_epoch_s": ${WORKLOAD_STARTED_EPOCH_S:-null},
  "memtier_started_epoch_s": ${WORKLOAD_STARTED_EPOCH_S:-null},
  "chaos_epoch_s": ${CHAOS_EPOCH_S},
  "steady_state_wait_s": ${STEADY_STATE_WAIT},
  "provider": "${PROVIDER}",
  "system": "${SYSTEM_NAME}",
  "action": "${FAILOVER_ACTION}",
  "duration": "${FAILOVER_DURATION}",
  "target": "${TARGET_DESC}",
  "grace_period_s": ${FAILOVER_GRACE_PERIOD},
  "threads": ${THREADS},
  "clients": ${CLIENTS},
  "pipeline": ${PIPELINE}
}
EOF
  kubectl apply -f "${CHAOS_FILE}"

  echo "[${i}] Verifying PodChaos resources were created..."
  if [[ "${FAILOVER_MODE}" == "masters" ]]; then
    mapfile -t CHAOS_NAMES < <(expected_master_chaos_names "${TARGET_MASTER_PODS[@]}")
  else
    CHAOS_NAMES=("${CHAOS_PREFIX}-kill-all")
  fi
  for chaos_name in "${CHAOS_NAMES[@]}"; do
    if ! kubectl get podchaos "${chaos_name}" -n "${NS}" >/dev/null 2>&1; then
      echo "[${i}] ERROR: expected PodChaos ${chaos_name} was not created."
      echo "[${i}] INFO: current PodChaos resources in ${NS}:"
      kubectl get podchaos -n "${NS}" || true
      exit 1
    fi
  done
  kubectl get podchaos -n "${NS}" "${CHAOS_NAMES[@]}"

  echo "[${i}] Waiting for ${CLIENT_ENGINE} to finish..."
  if ! wait_for_pod_marker "${NS}" "${POD_NAME}" "${POD_DONE_FILE}" 300; then
    echo "[${i}] ERROR: ${CLIENT_ENGINE} pod did not signal completion."
    print_pod_debug_info "${NS}" "${POD_NAME}"
    exit 1
  fi

  exit_code="$(read_pod_exit_code "${NS}" "${POD_NAME}" "${POD_EXIT_CODE_FILE}")"
  if [[ -z "${exit_code}" || "${exit_code}" != "0" ]]; then
    echo "[${i}] ERROR: ${CLIENT_ENGINE} exited with code ${exit_code:-unknown}."
    print_pod_debug_info "${NS}" "${POD_NAME}"
    exit 1
  fi

  echo "[${i}] Copying results..."
  kubectl cp "${NS}/${POD_NAME}:${REMOTE_OUT}/${OUT_FILE}" "${LOCAL_OUT}/${OUT_FILE}"
  kubectl cp "${NS}/${POD_NAME}:${REMOTE_OUT}/${LOG_FILE}" "${LOCAL_OUT}/${LOG_FILE}" || \
    echo "[${i}] WARN: could not copy ${CLIENT_ENGINE} log ${LOG_FILE}"

  echo "[${i}] Cleaning up..."
  kubectl delete -f "${CHAOS_FILE}" -n "${NS}" --ignore-not-found 2>/dev/null || true
  rm -f "${CHAOS_FILE}"
  kubectl delete pod "${POD_NAME}" -n "${NS}" --ignore-not-found

  echo "[${i}] Waiting for ${SYSTEM_NAME} cluster to stabilize..."
  kubectl rollout status "sts/${STS}" -n "${NS}" --timeout=120s
  kubectl wait pod -n "${NS}" -l "${POD_SELECTOR}" --for=condition=Ready --timeout=120s
  wait_for_cluster_health 120

  echo "[${i}] Done. Result: ${LOCAL_OUT}/${OUT_FILE}"
done

echo ""
echo "=========================================="
echo "  All ${N} failover runs complete."
echo "  Results in: ${LOCAL_OUT}/"
echo "  Analyse with:"
echo "    python cli.py failover --input ${LOCAL_OUT} --output-dir ./plots/failover"
echo "=========================================="

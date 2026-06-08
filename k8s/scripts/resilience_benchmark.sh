#!/usr/bin/env bash
set -euo pipefail

SCENARIO="${1:?Usage: $0 <cpu|memory|memory-extreme> [output_dir]}"
LOCAL_OUT="${2:-./results/resilience}"

N="${N:-5}"
NS="${NS:-vk}"
RELEASE="${RELEASE:-valkey}"
STS="${STS:-${RELEASE}}"
CLI_POD="${CLI_POD:-${STS}-0}"
CLI_BIN="${CLI_BIN:-valkey-cli}"
SERVICE_NAME="${SERVICE_NAME:-${RELEASE}}"
POD_SELECTOR="${POD_SELECTOR:-app.kubernetes.io/name=valkey,app.kubernetes.io/instance=${RELEASE}}"
IMAGE="${MEMTIER_IMAGE:-memtier_k8s:1}"
REMOTE_OUT="/work/results/resilience"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

source "${SCRIPT_DIR}/pod_results.sh"

HOST="${HOST:-${SERVICE_NAME}.${NS}.svc.cluster.local}"
PORT="${PORT:-6379}"
THREADS="${RESILIENCE_THREADS:-4}"
CLIENTS="${RESILIENCE_CLIENTS:-16}"
TEST_TIME="${RESILIENCE_TEST_TIME:-120}"
KEYS="${RESILIENCE_KEYS:-100000}"
DATA_SIZE="${RESILIENCE_DATA_SIZE:-1024}"
RATIO="${RESILIENCE_RATIO:-1:1}"
STEADY_STATE_WAIT="${RESILIENCE_STEADY_STATE_WAIT:-30}"
MEMTIER_STARTED_FILE="/tmp/memtier.started"

CPU_TARGET_MASTERS="${CPU_TARGET_MASTERS:-1}"
CPU_STRESS_WORKERS="${CPU_STRESS_WORKERS:-4}"
CPU_STRESS_LOAD="${CPU_STRESS_LOAD:-100}"
CPU_STRESS_DURATION="${CPU_STRESS_DURATION:-30s}"
MEMORY_STRESS_WORKERS="${MEMORY_STRESS_WORKERS:-1}"
MEMORY_STRESS_SIZE="${MEMORY_STRESS_SIZE:-900MB}"
MEMORY_STRESS_DURATION="${MEMORY_STRESS_DURATION:-60s}"
MEMORY_EXTREME_STRESS_WORKERS="${MEMORY_EXTREME_STRESS_WORKERS:-2}"
MEMORY_EXTREME_STRESS_SIZE="${MEMORY_EXTREME_STRESS_SIZE:-1800MB}"
MEMORY_EXTREME_STRESS_DURATION="${MEMORY_EXTREME_STRESS_DURATION:-60s}"

case "${SCENARIO}" in
  cpu)
    CHAOS_NAME="${RELEASE}-cpu-master-stress"
    FILE_PREFIX="resilience_cpu"
    STRESS_DURATION="${CPU_STRESS_DURATION}"
    ;;
  memory)
    CHAOS_NAME="${RELEASE}-memory-stress"
    FILE_PREFIX="resilience_mem"
    STRESS_DURATION="${MEMORY_STRESS_DURATION}"
    ;;
  memory-extreme)
    CHAOS_NAME="${RELEASE}-memory-extreme-stress"
    FILE_PREFIX="resilience_mem_extreme"
    STRESS_DURATION="${MEMORY_EXTREME_STRESS_DURATION}"
    ;;
  *)
    echo "ERROR: Unknown scenario '${SCENARIO}'. Use 'cpu', 'memory', or 'memory-extreme'."
    exit 1
    ;;
esac

mkdir -p "${LOCAL_OUT}"

json_string() {
  local value="$1"
  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  printf '"%s"' "${value}"
}

json_array() {
  local first=1 item
  printf '['
  for item in "$@"; do
    if [[ "${first}" -eq 0 ]]; then
      printf ', '
    fi
    json_string "${item}"
    first=0
  done
  printf ']'
}

duration_seconds() {
  local duration="$1"
  case "${duration}" in
    *s) echo "${duration%s}" ;;
    *m) echo "$(( ${duration%m} * 60 ))" ;;
    *h) echo "$(( ${duration%h} * 3600 ))" ;;
    *) echo "${duration}" ;;
  esac
}

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

cluster_nodes() {
  kubectl exec "${CLI_POD}" -n "${NS}" -- \
    "${CLI_BIN}" cluster nodes 2>/dev/null
}

wait_for_cluster_health() {
  local timeout_s="${1:-120}"
  local start elapsed info state slots masters
  start="$(date +%s)"

  while true; do
    info="$(kubectl exec "${CLI_POD}" -n "${NS}" -- \
      "${CLI_BIN}" cluster info 2>/dev/null || true)"
    state="$(awk -F: '$1=="cluster_state" {gsub(/\r/, "", $2); print $2}' <<<"${info}")"
    slots="$(awk -F: '$1=="cluster_slots_ok" {gsub(/\r/, "", $2); print $2}' <<<"${info}")"
    masters="$(cluster_nodes | awk '$3 ~ /master/ && $3 !~ /fail/ {count++} END {print count + 0}' || true)"

    if [[ "${state}" == "ok" && "${slots}" == "16384" && "${masters:-0}" -ge 3 ]]; then
      return 0
    fi

    elapsed="$(( $(date +%s) - start ))"
    if (( elapsed >= timeout_s )); then
      echo "ERROR: Valkey cluster did not become healthy after ${timeout_s}s." >&2
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

write_cpu_stress_manifest() {
  local manifest_path="$1"
  shift
  local target_pod

  cat > "${manifest_path}" <<EOF
apiVersion: chaos-mesh.org/v1alpha1
kind: StressChaos
metadata:
  name: ${CHAOS_NAME}
  namespace: ${NS}
spec:
  selector:
    pods:
      ${NS}:
EOF
  for target_pod in "$@"; do
    printf '        - %s\n' "${target_pod}" >> "${manifest_path}"
  done
  cat >> "${manifest_path}" <<EOF
  mode: all
  stressors:
    cpu:
      workers: ${CPU_STRESS_WORKERS}
      load: ${CPU_STRESS_LOAD}
  duration: "${CPU_STRESS_DURATION}"
EOF
}

write_memory_stress_manifest() {
  local manifest_path="$1"
  local workers="$2"
  local size="$3"
  local duration="$4"

  cat > "${manifest_path}" <<EOF
apiVersion: chaos-mesh.org/v1alpha1
kind: StressChaos
metadata:
  name: ${CHAOS_NAME}
  namespace: ${NS}
spec:
  selector:
    namespaces:
      - ${NS}
    labelSelectors:
$(selector_label_yaml "${POD_SELECTOR}")
  mode: one
  stressors:
    memory:
      workers: ${workers}
      size: "${size}"
  duration: "${duration}"
EOF
}

prepare_chaos_manifest() {
  local manifest_path="$1"
  TARGET_DESC=""
  TARGET_MASTER_COUNT="null"
  TARGET_PODS=()

  case "${SCENARIO}" in
    cpu)
      if ! [[ "${CPU_TARGET_MASTERS}" =~ ^[1-9][0-9]*$ ]]; then
        echo "ERROR: CPU_TARGET_MASTERS must be a positive integer." >&2
        exit 1
      fi

      mapfile -t TARGET_PODS < <(get_target_master_pods "${CPU_TARGET_MASTERS}")
      if [[ "${#TARGET_PODS[@]}" -lt "${CPU_TARGET_MASTERS}" ]]; then
        echo "ERROR: could not determine ${CPU_TARGET_MASTERS} target master pod(s)." >&2
        echo "INFO: visible master nodes from ${CLI_POD}:" >&2
        cluster_nodes | grep master >&2 || true
        exit 1
      fi

      TARGET_MASTER_COUNT="${CPU_TARGET_MASTERS}"
      TARGET_DESC="CPU stress for ${CPU_TARGET_MASTERS} master pod(s): ${TARGET_PODS[*]}"
      write_cpu_stress_manifest "${manifest_path}" "${TARGET_PODS[@]}"
      ;;
    memory)
      TARGET_DESC="Memory stress for one selected Valkey pod (${MEMORY_STRESS_SIZE})"
      write_memory_stress_manifest \
        "${manifest_path}" \
        "${MEMORY_STRESS_WORKERS}" \
        "${MEMORY_STRESS_SIZE}" \
        "${MEMORY_STRESS_DURATION}"
      ;;
    memory-extreme)
      TARGET_DESC="Extreme memory stress for one selected Valkey pod (${MEMORY_EXTREME_STRESS_SIZE})"
      write_memory_stress_manifest \
        "${manifest_path}" \
        "${MEMORY_EXTREME_STRESS_WORKERS}" \
        "${MEMORY_EXTREME_STRESS_SIZE}" \
        "${MEMORY_EXTREME_STRESS_DURATION}"
      ;;
  esac
}

print_config() {
  cat <<EOF
==> Resilience benchmark configuration
SCENARIO=${SCENARIO}
N=${N}
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
TEST_TIME=${TEST_TIME}
KEYS=${KEYS}
DATA_SIZE=${DATA_SIZE}
RATIO=${RATIO}
STEADY_STATE_WAIT=${STEADY_STATE_WAIT}
CPU_TARGET_MASTERS=${CPU_TARGET_MASTERS}
CPU_STRESS_WORKERS=${CPU_STRESS_WORKERS}
CPU_STRESS_LOAD=${CPU_STRESS_LOAD}
CPU_STRESS_DURATION=${CPU_STRESS_DURATION}
MEMORY_STRESS_WORKERS=${MEMORY_STRESS_WORKERS}
MEMORY_STRESS_SIZE=${MEMORY_STRESS_SIZE}
MEMORY_STRESS_DURATION=${MEMORY_STRESS_DURATION}
MEMORY_EXTREME_STRESS_WORKERS=${MEMORY_EXTREME_STRESS_WORKERS}
MEMORY_EXTREME_STRESS_SIZE=${MEMORY_EXTREME_STRESS_SIZE}
MEMORY_EXTREME_STRESS_DURATION=${MEMORY_EXTREME_STRESS_DURATION}
EOF
}

print_config

for i in $(seq 1 "${N}"); do
  POD_NAME="memtier-resilience-${SCENARIO}-${i}"
  OUT_FILE="${FILE_PREFIX}_run_${i}.json"
  LOG_FILE="${FILE_PREFIX}_run_${i}.log"
  TIMING_FILE="${FILE_PREFIX}_timing_${i}.json"
  CHAOS_FILE="$(mktemp /tmp/resilience-chaos-XXXX.yaml)"

  prepare_chaos_manifest "${CHAOS_FILE}"

  echo ""
  echo "=========================================="
  echo "  Resilience [${SCENARIO}] run ${i}/${N}"
  echo "=========================================="

  echo "[${i}] Waiting for Valkey cluster health before run..."
  wait_for_cluster_health 120

  kubectl delete stresschaos "${CHAOS_NAME}" -n "${NS}" --ignore-not-found 2>/dev/null || true
  kubectl delete pod "${POD_NAME}" -n "${NS}" --ignore-not-found 2>/dev/null || true

  POD_COMMAND="$(cat <<EOF
mkdir -p '${REMOTE_OUT}'
LOG_PIPE='${REMOTE_OUT}/${LOG_FILE}.pipe'
rm -f '${MEMTIER_STARTED_FILE}'
rm -f "\${LOG_PIPE}"
mkfifo "\${LOG_PIPE}"
(
  tr '\r' '\n' < "\${LOG_PIPE}" | while IFS= read -r line; do
    [ -n "\${line}" ] || continue
    line_ts="\$(date +%s)"
    printf '%s\t%s\n' "\${line_ts}" "\${line}"
  done
) > '${REMOTE_OUT}/${LOG_FILE}' &
logger_pid=\$!
date +%s > '${MEMTIER_STARTED_FILE}'
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

  echo "[${i}] Starting memtier pod (test-time=${TEST_TIME}s)..."
  kubectl run "${POD_NAME}" -n "${NS}" \
    --image="${IMAGE}" \
    --restart=Never \
    --command -- \
    /bin/sh -c "${POD_COMMAND}"

  echo "[${i}] Waiting for pod to start..."
  kubectl wait pod/"${POD_NAME}" -n "${NS}" \
    --for=condition=Ready --timeout=60s 2>/dev/null || true

  echo "[${i}] Waiting for memtier timed run to start..."
  if ! wait_for_pod_marker "${NS}" "${POD_NAME}" "${MEMTIER_STARTED_FILE}" 120; then
    echo "[${i}] ERROR: memtier did not start its timed run."
    print_pod_debug_info "${NS}" "${POD_NAME}"
    exit 1
  fi

  echo "[${i}] Waiting ${STEADY_STATE_WAIT}s for steady state..."
  sleep "${STEADY_STATE_WAIT}"

  echo "[${i}] Injecting stress: ${TARGET_DESC} (duration=${STRESS_DURATION})..."
  echo "[${i}] Generated StressChaos manifest:"
  sed 's/^/  /' "${CHAOS_FILE}"
  CHAOS_EPOCH_S="$(kubectl exec "${POD_NAME}" -n "${NS}" -- date +%s 2>/dev/null || date +%s)"
  MEMTIER_STARTED_EPOCH_S="$(kubectl exec "${POD_NAME}" -n "${NS}" -- cat "${MEMTIER_STARTED_FILE}" 2>/dev/null | tr -d '[:space:]')"
  STRESS_DURATION_S="$(duration_seconds "${STRESS_DURATION}")"
  cat > "${LOCAL_OUT}/${TIMING_FILE}" <<EOF
{
  "memtier_started_epoch_s": ${MEMTIER_STARTED_EPOCH_S:-null},
  "chaos_epoch_s": ${CHAOS_EPOCH_S},
  "steady_state_wait_s": ${STEADY_STATE_WAIT},
  "scenario": $(json_string "${SCENARIO}"),
  "duration": $(json_string "${STRESS_DURATION}"),
  "stress_duration_s": ${STRESS_DURATION_S},
  "target": $(json_string "${TARGET_DESC}"),
  "target_master_count": ${TARGET_MASTER_COUNT},
  "target_pods": $(json_array "${TARGET_PODS[@]}"),
  "cpu_workers": ${CPU_STRESS_WORKERS},
  "cpu_load": ${CPU_STRESS_LOAD}
}
EOF
  kubectl apply -f "${CHAOS_FILE}"

  echo "[${i}] Verifying StressChaos resource was created..."
  if ! kubectl get stresschaos "${CHAOS_NAME}" -n "${NS}" >/dev/null 2>&1; then
    echo "[${i}] ERROR: expected StressChaos ${CHAOS_NAME} was not created."
    echo "[${i}] INFO: current StressChaos resources in ${NS}:"
    kubectl get stresschaos -n "${NS}" || true
    exit 1
  fi
  kubectl get stresschaos "${CHAOS_NAME}" -n "${NS}"

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
  kubectl cp "${NS}/${POD_NAME}:${REMOTE_OUT}/${LOG_FILE}" "${LOCAL_OUT}/${LOG_FILE}" || \
    echo "[${i}] WARN: could not copy memtier log ${LOG_FILE}"

  echo "[${i}] Cleaning up..."
  kubectl delete -f "${CHAOS_FILE}" -n "${NS}" --ignore-not-found 2>/dev/null || true
  rm -f "${CHAOS_FILE}"
  kubectl delete pod "${POD_NAME}" -n "${NS}" --ignore-not-found

  echo "[${i}] Waiting for Valkey cluster to stabilize..."
  kubectl rollout status "sts/${STS}" -n "${NS}" --timeout=120s
  kubectl wait pod -n "${NS}" -l "${POD_SELECTOR}" --for=condition=Ready --timeout=120s
  wait_for_cluster_health 120
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

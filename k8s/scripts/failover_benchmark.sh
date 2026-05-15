#!/usr/bin/env bash
set -euo pipefail

N="${N:-1}"
NS="vk"
IMAGE="${MEMTIER_IMAGE:-memtier_k8s:1}"
LOCAL_OUT="${1:-./results/failover}"
REMOTE_OUT="/work/results/failover"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

source "${SCRIPT_DIR}/target_config.sh"
source "${SCRIPT_DIR}/pod_results.sh"

HOST="${TC_HOST}"
PORT="${TC_PORT}"
STS="${TC_STS}"
CLI="${TC_CLI}"
ADMIN_POD="$(tc_admin_pod)"
APP_INSTANCE_LABEL="${TC_APP_INSTANCE_LABEL}"

THREADS=4
CLIENTS=1
TEST_TIME=120
KEYS=100000
DATA_SIZE=1024
RATIO="1:1"
STEADY_STATE_WAIT=30
FAILOVER_MODE="${FAILOVER_MODE:-masters}"
FAILOVER_MASTERS="${FAILOVER_MASTERS:-1}"
FAILOVER_GRACE_PERIOD="${FAILOVER_GRACE_PERIOD:-1}"
MEMTIER_STARTED_FILE="/tmp/memtier.started"
MEMTIER_TLS="${MEMTIER_TLS:-false}"
MEMTIER_TLS_SKIP_VERIFY="${MEMTIER_TLS_SKIP_VERIFY:-true}"
MEMTIER_TLS_CACERT="${MEMTIER_TLS_CACERT:-}"
MEMTIER_TLS_CERT="${MEMTIER_TLS_CERT:-}"
MEMTIER_TLS_KEY="${MEMTIER_TLS_KEY:-}"
MEMTIER_TLS_SNI="${MEMTIER_TLS_SNI:-}"
CLI_TLS="${CLI_TLS:-${MEMTIER_TLS}}"
CLI_TLS_SKIP_VERIFY="${CLI_TLS_SKIP_VERIFY:-${MEMTIER_TLS_SKIP_VERIFY}}"
CLI_CACERT="${CLI_CACERT:-/tls/ca.crt}"

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

CLI_TLS_ARGS=()
case "${CLI_TLS}" in
  1|true|TRUE|yes|YES|on|ON)
    CLI_TLS_ARGS+=(--tls)
    case "${CLI_TLS_SKIP_VERIFY}" in
      1|true|TRUE|yes|YES|on|ON)
        CLI_TLS_ARGS+=(--insecure)
        ;;
      *)
        CLI_TLS_ARGS+=(--cacert "${CLI_CACERT}")
        ;;
    esac
    ;;
esac

resolve_pod_name() {
  local host="$1"
  local pod_table pod_name

  if [[ "${host}" == ${TC_POD_PREFIX}* ]]; then
    echo "${host%%.*}"
    return 0
  fi

  pod_table="$(kubectl get pods -n "${NS}" -o wide --no-headers 2>/dev/null)"
  pod_name="$(
    awk -v target="${host}" '$1 == target || $6 == target {print $1; exit}' <<<"${pod_table}"
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
    kubectl exec "${ADMIN_POD}" -n "${NS}" -- \
      ${CLI} "${CLI_TLS_ARGS[@]}" cluster nodes 2>/dev/null \
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
    chaos_name="${TARGET}-master-kill-${target_pod}"
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
  action: pod-kill
  gracePeriod: ${FAILOVER_GRACE_PERIOD}
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
  name: ${TARGET}-kill-all
  namespace: ${NS}
spec:
  selector:
    namespaces:
      - ${NS}
    labelSelectors:
      app.kubernetes.io/instance: ${APP_INSTANCE_LABEL}
  mode: all
  action: pod-kill
  gracePeriod: ${FAILOVER_GRACE_PERIOD}
EOF
}

expected_master_chaos_names() {
  local target_pod
  for target_pod in "$@"; do
    echo "${TARGET}-master-kill-${target_pod}"
  done
}

for i in $(seq 1 "${N}"); do
  POD_NAME="memtier-failover-${i}"
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
      echo "[${i}] INFO: visible master nodes from ${ADMIN_POD}:"
      kubectl exec "${ADMIN_POD}" -n "${NS}" -- ${CLI} "${CLI_TLS_ARGS[@]}" cluster nodes 2>/dev/null | grep master || true
      exit 1
    fi
    TARGET_DESC="killing ${FAILOVER_MASTERS} master pod(s): ${TARGET_MASTER_PODS[*]}"
  fi

  CHAOS_FILE="$(mktemp /tmp/failover-chaos-XXXX.yaml)"
  if [[ "${FAILOVER_MODE}" == "masters" ]]; then
    write_podchaos_manifest "${CHAOS_FILE}" "${TARGET_MASTER_PODS[@]}"
  else
    write_podchaos_all_manifest "${CHAOS_FILE}"
    TARGET_DESC="killing all ${TARGET} pods"
  fi
  echo ""
  echo "=========================================="
  echo "  Failover run ${i}/${N} (target=${TARGET})"
  echo "=========================================="

  kubectl delete -f "${CHAOS_FILE}" -n "${NS}" --ignore-not-found 2>/dev/null || true
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
    case "\${line}" in
      *"[RUN #1"*ops/sec*)
        if [ ! -f '${MEMTIER_STARTED_FILE}' ]; then
          echo "\${line_ts}" > '${MEMTIER_STARTED_FILE}'
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

  echo "[${i}] Injecting chaos: ${TARGET_DESC} (gracePeriod=${FAILOVER_GRACE_PERIOD}s)..."
  echo "[${i}] Generated PodChaos manifest:"
  sed 's/^/  /' "${CHAOS_FILE}"
  CHAOS_EPOCH_S="$(kubectl exec "${POD_NAME}" -n "${NS}" -- date +%s 2>/dev/null || date +%s)"
  MEMTIER_STARTED_EPOCH_S="$(kubectl exec "${POD_NAME}" -n "${NS}" -- cat "${MEMTIER_STARTED_FILE}" 2>/dev/null | tr -d '[:space:]')"
  cat > "${LOCAL_OUT}/${TIMING_FILE}" <<EOF
{
  "memtier_started_epoch_s": ${MEMTIER_STARTED_EPOCH_S:-null},
  "chaos_epoch_s": ${CHAOS_EPOCH_S},
  "steady_state_wait_s": ${STEADY_STATE_WAIT},
  "target": "${TARGET_DESC}",
  "grace_period_s": ${FAILOVER_GRACE_PERIOD}
}
EOF
  kubectl apply -f "${CHAOS_FILE}"

  echo "[${i}] Verifying PodChaos resources were created..."
  if [[ "${FAILOVER_MODE}" == "masters" ]]; then
    mapfile -t CHAOS_NAMES < <(expected_master_chaos_names "${TARGET_MASTER_PODS[@]}")
  else
    CHAOS_NAMES=("${TARGET}-kill-all")
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

  echo "[${i}] Waiting for cluster to stabilize..."
  kubectl rollout status "sts/${STS}" -n "${NS}" --timeout=120s
  sleep 10

  echo "[${i}] Done. Result: ${LOCAL_OUT}/${OUT_FILE}"
done

echo ""
echo "=========================================="
echo "  All ${N} failover runs complete."
echo "  Results in: ${LOCAL_OUT}/"
echo "  Analyse with:"
echo "    python cli.py failover --input ${LOCAL_OUT} --output-dir ./plots/failover"
echo "=========================================="

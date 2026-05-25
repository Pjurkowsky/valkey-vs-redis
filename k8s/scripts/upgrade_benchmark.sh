#!/usr/bin/env bash
set -euo pipefail

N="${N:-5}"
NS="vk"
IMAGE="${MEMTIER_IMAGE:-memtier_k8s:1}"
FROM_VALKEY_VERSION="${FROM_VALKEY_VERSION:-9.0.1}"
TO_VALKEY_VERSION="${TO_VALKEY_VERSION:-9.1.0}"
PROBE_IMAGE="${PROBE_IMAGE:-docker.io/valkey/valkey:${TO_VALKEY_VERSION}}"
LOCAL_OUT="${1:-./results/upgrade}"
REMOTE_OUT="/work/results/upgrade"
REMOTE_LOG_OUT="/work/results/upgrade_logs"
REMOTE_PROBE_OUT="/work/results/upgrade_probe"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

source "${SCRIPT_DIR}/pod_results.sh"

HELM_CHART_PATH="${HELM_CHART_PATH:-../valkey-helm/valkey}"
VALUES_FILE="${VALUES_FILE:-./k8s/manifests/values.yaml}"

HOST="valkey.vk.svc.cluster.local"
PORT=6379
THREADS=4
CLIENTS=16
TEST_TIME="${TEST_TIME:-300}"
KEYS=100000
DATA_SIZE=1024
RATIO="1:1"
STEADY_STATE_WAIT=30
ROLLOUT_TIMEOUT="${ROLLOUT_TIMEOUT:-300s}"
POD_MEMTIER_START_FILE="${POD_MEMTIER_START_FILE:-/tmp/memtier.start}"
POD_MEMTIER_END_FILE="${POD_MEMTIER_END_FILE:-/tmp/memtier.end}"
POD_PROBE_DONE_FILE="${POD_PROBE_DONE_FILE:-/tmp/app_probe.done}"
PROBE_INTERVAL_SECONDS="${PROBE_INTERVAL_SECONDS:-0.2}"
PROBE_TIMEOUT_SECONDS="${PROBE_TIMEOUT_SECONDS:-2}"
PROBE_EXTRA_SECONDS="${PROBE_EXTRA_SECONDS:-60}"

mkdir -p "${LOCAL_OUT}"

helm_apply_version() {
  local version="$1"
  local wait_mode="${2:-wait}"
  local trigger
  trigger="$(date +%s)"

  local args=(
    upgrade valkey "${HELM_CHART_PATH}"
    -n "${NS}"
    -f "${VALUES_FILE}"
    --set-string "image.tag=${version}"
    --set-string "podAnnotations.restart-trigger=${trigger}"
  )

  if [[ "${wait_mode}" == "wait" ]]; then
    args+=(--wait "--timeout=${ROLLOUT_TIMEOUT}")
  else
    args+=(--wait=false)
  fi

  helm "${args[@]}"
}

wait_valkey_rollout_ready() {
  kubectl rollout status sts/valkey -n "${NS}" "--timeout=${ROLLOUT_TIMEOUT}"
  wait_cluster_client_ready 300
}

wait_cluster_client_ready() {
  local max_wait="${1:-300}"
  local elapsed=0

  echo "  Waiting for Valkey cluster client readiness (max ${max_wait}s)..."
  while [[ "${elapsed}" -lt "${max_wait}" ]]; do
    local info state slots_ok slots_assigned
    info="$(kubectl exec valkey-0 -n "${NS}" -- \
      valkey-cli cluster info 2>/dev/null || true)"
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
        /bin/sh -c "valkey-cli -c -h '${HOST}' -p '${PORT}' set '${probe_key}' ok >/dev/null && test \"\$(valkey-cli -c -h '${HOST}' -p '${PORT}' get '${probe_key}')\" = ok && valkey-cli -c -h '${HOST}' -p '${PORT}' del '${probe_key}' >/dev/null" \
        >/dev/null 2>&1; then
        echo "  Cluster client-ready after ${elapsed}s"
        return 0
      fi
    fi

    sleep 5
    elapsed=$((elapsed + 5))
  done

  echo "ERROR: Valkey cluster was not client-ready after ${max_wait}s" >&2
  kubectl exec valkey-0 -n "${NS}" -- valkey-cli cluster info || true
  kubectl exec valkey-0 -n "${NS}" -- valkey-cli cluster slots || true
  return 1
}

for i in $(seq 1 "${N}"); do
  POD_NAME="memtier-upgrade-${i}"
  PROBE_POD_NAME="app-probe-upgrade-${i}"
  OUT_FILE="upgrade_run_${i}.json"
  LOG_FILE="upgrade_run_${i}.log"
  PROBE_FILE="upgrade_probe_${i}.csv"
  TIMING_FILE="upgrade_timing_${i}.json"
  echo ""
  echo "=========================================="
  echo "  Upgrade run ${i}/${N}: ${FROM_VALKEY_VERSION} -> ${TO_VALKEY_VERSION}"
  echo "=========================================="

  kubectl delete pod "${POD_NAME}" -n "${NS}" --ignore-not-found 2>/dev/null || true
  kubectl delete pod "${PROBE_POD_NAME}" -n "${NS}" --ignore-not-found 2>/dev/null || true

  echo "[${i}] Preparing source version ${FROM_VALKEY_VERSION}..."
  helm_apply_version "${FROM_VALKEY_VERSION}" wait
  wait_valkey_rollout_ready

  wait_cluster_client_ready 300

  echo "[${i}] Starting memtier pod (test-time=${TEST_TIME}s)..."
  kubectl run "${POD_NAME}" -n "${NS}" \
    --image="${IMAGE}" \
    --restart=Never \
    --command -- \
    /bin/sh -c "
      mkdir -p '${REMOTE_OUT}' '${REMOTE_LOG_OUT}'
      {
        date +%s > '${POD_MEMTIER_START_FILE}'
        echo \"MEMTIER_START_EPOCH=\$(cat '${POD_MEMTIER_START_FILE}')\"
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
        date +%s > '${POD_MEMTIER_END_FILE}'
        echo \"MEMTIER_END_EPOCH=\$(cat '${POD_MEMTIER_END_FILE}')\"
        echo \"\$status\" > '${POD_EXIT_CODE_FILE}'
        touch '${POD_DONE_FILE}'
      } 2>&1 | awk '{ print strftime(\"%s\"), \$0; fflush(); }' | tee '${REMOTE_LOG_OUT}/${LOG_FILE}'
      sleep '${POD_HOLD_SECONDS}'
    "

  echo "[${i}] Waiting for pod to start..."
  kubectl wait pod/"${POD_NAME}" -n "${NS}" \
    --for=condition=Ready --timeout=60s 2>/dev/null || true

  PROBE_DURATION=$((TEST_TIME + PROBE_EXTRA_SECONDS))
  echo "[${i}] Starting app probe pod (duration=${PROBE_DURATION}s, interval=${PROBE_INTERVAL_SECONDS}s)..."
  kubectl run "${PROBE_POD_NAME}" -n "${NS}" \
    --image="${PROBE_IMAGE}" \
    --restart=Never \
    --command -- \
    /bin/sh -c "
      mkdir -p '${REMOTE_PROBE_OUT}'
      out='${REMOTE_PROBE_OUT}/${PROBE_FILE}'
      echo 'epoch_ms,latency_ms,status,error' > \"\${out}\"
      end=\$((\$(date +%s) + ${PROBE_DURATION}))
      n=0
      while [ \$(date +%s) -lt \"\${end}\" ]; do
        start_ms=\$(date +%s%3N)
        key='upgrade:app-probe:'\"\${n}\"
        value=\"\${start_ms}\"
        status='ok'
        error=''

        set_out=\$(timeout '${PROBE_TIMEOUT_SECONDS}s' valkey-cli -c -h '${HOST}' -p '${PORT}' --raw SET \"\${key}\" \"\${value}\" 2>&1)
        set_rc=\$?
        if [ \"\${set_rc}\" -ne 0 ]; then
          status='error'
          error=\"SET rc=\${set_rc}: \${set_out}\"
        else
          get_out=\$(timeout '${PROBE_TIMEOUT_SECONDS}s' valkey-cli -c -h '${HOST}' -p '${PORT}' --raw GET \"\${key}\" 2>&1)
          get_rc=\$?
          if [ \"\${get_rc}\" -ne 0 ]; then
            status='error'
            error=\"GET rc=\${get_rc}: \${get_out}\"
          elif [ \"\${get_out}\" != \"\${value}\" ]; then
            status='mismatch'
            error=\"expected \${value}, got \${get_out}\"
          fi
        fi

        end_ms=\$(date +%s%3N)
        latency_ms=\$((end_ms - start_ms))
        safe_error=\$(printf '%s' \"\${error}\" | tr '\n,' '  ')
        printf '%s,%s,%s,%s\n' \"\${start_ms}\" \"\${latency_ms}\" \"\${status}\" \"\${safe_error}\" >> \"\${out}\"
        n=\$((n + 1))
        sleep '${PROBE_INTERVAL_SECONDS}'
      done
      touch '${POD_PROBE_DONE_FILE}'
      sleep '${POD_HOLD_SECONDS}'
    "

  echo "[${i}] Waiting for app probe pod to start..."
  kubectl wait pod/"${PROBE_POD_NAME}" -n "${NS}" \
    --for=condition=Ready --timeout=60s 2>/dev/null || true

  echo "[${i}] Waiting ${STEADY_STATE_WAIT}s for steady state..."
  sleep "${STEADY_STATE_WAIT}"

  UPGRADE_START="$(date +%s)"
  echo "[${i}] Triggering real rolling upgrade ${FROM_VALKEY_VERSION} -> ${TO_VALKEY_VERSION}..."
  helm_apply_version "${TO_VALKEY_VERSION}" no-wait

  echo "[${i}] Waiting for target version rollout to fully complete..."
  wait_valkey_rollout_ready
  UPGRADE_END="$(date +%s)"

  echo "[${i}] Waiting for memtier to finish..."
  if ! wait_for_pod_marker "${NS}" "${POD_NAME}" "${POD_DONE_FILE}" 600; then
    echo "[${i}] ERROR: memtier pod did not signal completion."
    kubectl cp "${NS}/${PROBE_POD_NAME}:${REMOTE_PROBE_OUT}/${PROBE_FILE}" "${LOCAL_OUT}/${PROBE_FILE}" 2>/dev/null || true
    print_pod_debug_info "${NS}" "${POD_NAME}"
    exit 1
  fi
  MEMTIER_START="$(kubectl exec "${POD_NAME}" -n "${NS}" -- cat "${POD_MEMTIER_START_FILE}" 2>/dev/null | tr -d '[:space:]')"
  MEMTIER_END="$(kubectl exec "${POD_NAME}" -n "${NS}" -- cat "${POD_MEMTIER_END_FILE}" 2>/dev/null | tr -d '[:space:]')"
  if [[ -z "${MEMTIER_START}" ]]; then
    MEMTIER_START=0
  fi
  if [[ -z "${MEMTIER_END}" ]]; then
    MEMTIER_END="$(date +%s)"
  fi

  exit_code="$(read_pod_exit_code "${NS}" "${POD_NAME}" "${POD_EXIT_CODE_FILE}")"
  if [[ -z "${exit_code}" || "${exit_code}" != "0" ]]; then
    echo "[${i}] ERROR: memtier exited with code ${exit_code:-unknown}."
    kubectl cp "${NS}/${POD_NAME}:${REMOTE_OUT}/${OUT_FILE}" "${LOCAL_OUT}/${OUT_FILE}" 2>/dev/null || true
    kubectl cp "${NS}/${POD_NAME}:${REMOTE_LOG_OUT}/${LOG_FILE}" "${LOCAL_OUT}/${LOG_FILE}" 2>/dev/null || true
    kubectl cp "${NS}/${PROBE_POD_NAME}:${REMOTE_PROBE_OUT}/${PROBE_FILE}" "${LOCAL_OUT}/${PROBE_FILE}" 2>/dev/null || true
    print_pod_debug_info "${NS}" "${POD_NAME}"
    exit 1
  fi

  echo "[${i}] Waiting for app probe to finish..."
  if ! wait_for_pod_marker "${NS}" "${PROBE_POD_NAME}" "${POD_PROBE_DONE_FILE}" 180; then
    echo "[${i}] WARN: app probe did not signal completion; copying partial CSV."
  fi

  echo "[${i}] Copying results..."
  kubectl cp "${NS}/${POD_NAME}:${REMOTE_OUT}/${OUT_FILE}" "${LOCAL_OUT}/${OUT_FILE}"
  kubectl cp "${NS}/${POD_NAME}:${REMOTE_LOG_OUT}/${LOG_FILE}" "${LOCAL_OUT}/${LOG_FILE}"
  kubectl cp "${NS}/${PROBE_POD_NAME}:${REMOTE_PROBE_OUT}/${PROBE_FILE}" "${LOCAL_OUT}/${PROBE_FILE}" 2>/dev/null || true

  echo "[${i}] Cleaning up memtier pod..."
  kubectl delete pod "${POD_NAME}" -n "${NS}" --ignore-not-found
  kubectl delete pod "${PROBE_POD_NAME}" -n "${NS}" --ignore-not-found

  cat > "${LOCAL_OUT}/${TIMING_FILE}" <<EOF
{
  "run": ${i},
  "from_version": "${FROM_VALKEY_VERSION}",
  "to_version": "${TO_VALKEY_VERSION}",
  "upgrade_start": ${UPGRADE_START},
  "upgrade_end": ${UPGRADE_END},
  "upgrade_duration_s": $((UPGRADE_END - UPGRADE_START)),
  "memtier_start": ${MEMTIER_START},
  "memtier_end": ${MEMTIER_END},
  "memtier_after_upgrade_s": $((MEMTIER_END - UPGRADE_END)),
  "memtier_log": "${LOG_FILE}",
  "app_probe_file": "${PROBE_FILE}",
  "app_probe_interval_s": ${PROBE_INTERVAL_SECONDS},
  "app_probe_timeout_s": ${PROBE_TIMEOUT_SECONDS},
  "test_time_s": ${TEST_TIME},
  "steady_state_wait_s": ${STEADY_STATE_WAIT}
}
EOF

  echo "[${i}] Restoring source version ${FROM_VALKEY_VERSION} for next run..."
  helm_apply_version "${FROM_VALKEY_VERSION}" wait
  wait_valkey_rollout_ready
  sleep 15

  echo "[${i}] Done. Result: ${LOCAL_OUT}/${OUT_FILE}, log: ${LOCAL_OUT}/${LOG_FILE}, timing: ${LOCAL_OUT}/${TIMING_FILE}"
done

echo ""
echo "=========================================="
echo "  All ${N} upgrade runs complete."
echo "  Results in: ${LOCAL_OUT}/"
echo "  Upgrade path: ${FROM_VALKEY_VERSION} -> ${TO_VALKEY_VERSION}"
echo "  Analyse with:"
echo "    python cli.py upgrade --input ${LOCAL_OUT} --output-dir ./plots/upgrade"
echo "=========================================="

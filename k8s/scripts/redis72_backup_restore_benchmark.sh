#!/usr/bin/env bash
set -euo pipefail

SIZE_MB="${1:?Usage: $0 <size_mb> [output_dir]}"
LOCAL_OUT="${2:-./results/redis72_backup}"
N="${N:-3}"
NS="${NS:-redis}"
RELEASE="${RELEASE:-redis72}"
STS="${STS:-${RELEASE}-redis-cluster}"
IMAGE="${BACKUP_IMAGE:-python:3.12-slim}"
REMOTE_OUT="/work/results/backup"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
KUBECTL_EXEC_TIMEOUT_SECONDS="${KUBECTL_EXEC_TIMEOUT_SECONDS:-30}"
CONFIGMAP_NAME="${CONFIGMAP_NAME:-redis72-backup-restore-seed}"
HOST="${REDIS_HOST:-${STS}.${NS}.svc.cluster.local}"
PORT="${REDIS_PORT:-6379}"
ADMIN_POD="${ADMIN_POD:-${STS}-0}"
HEADLESS="${HEADLESS:-${STS}-headless}"
CLUSTER_ENDPOINT="${ADMIN_HOST:-${ADMIN_POD}.${HEADLESS}.${NS}.svc.cluster.local}:${PORT}"
INSTALL_REDIS_DEPS="${INSTALL_REDIS_DEPS:-auto}"
RANDOM_DATA="${RANDOM_DATA:-true}"
VERIFY_MODE="${VERIFY_MODE:-size}"
RANDOM_DATA_ARG=""
if [[ "${RANDOM_DATA}" == "true" ]]; then
  RANDOM_DATA_ARG="--random-data"
fi

source "${SCRIPT_DIR}/pod_results.sh"

mkdir -p "${LOCAL_OUT}"

ensure_backup_script_configmap() {
  kubectl create configmap "${CONFIGMAP_NAME}" -n "${NS}" \
    --from-file=backup_restore_seed.py="${SCRIPT_DIR}/../images/backup/backup_restore_seed.py" \
    --dry-run=client -o yaml | kubectl apply -f -
}

wait_for_command_pod() {
  local pod_name="$1"
  local timeout_s="$2"

  if ! wait_for_pod_marker "${NS}" "${pod_name}" "${POD_DONE_FILE}" "${timeout_s}"; then
    echo "ERROR: pod ${pod_name} did not signal completion." >&2
    print_pod_debug_info "${NS}" "${pod_name}"
    return 1
  fi

  local exit_code
  exit_code="$(read_pod_exit_code "${NS}" "${pod_name}" "${POD_EXIT_CODE_FILE}")"
  if [[ -z "${exit_code}" || "${exit_code}" != "0" ]]; then
    echo "ERROR: pod ${pod_name} exited with code ${exit_code:-unknown}." >&2
    print_pod_debug_info "${NS}" "${pod_name}"
    return 1
  fi
}

wait_for_pod_ready() {
  local pod_name="$1"
  local timeout_s="${2:-120}"

  kubectl wait pod/"${pod_name}" -n "${NS}" \
    --for=condition=Ready --timeout="${timeout_s}s"
}

start_backup_tool_pod() {
  local pod_name="$1"
  local command_body="$2"

  kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found >/dev/null 2>&1 || true

  kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${pod_name}
  namespace: ${NS}
spec:
  restartPolicy: Never
  containers:
    - name: backup-tool
      image: ${IMAGE}
      imagePullPolicy: IfNotPresent
      command:
        - /bin/sh
        - -c
      args:
        - |
          set -e
          mkdir -p '${REMOTE_OUT}'
          if [ "${INSTALL_REDIS_DEPS}" = "true" ] || { [ "${INSTALL_REDIS_DEPS}" = "auto" ] && ! python -c 'import redis' >/dev/null 2>&1; }; then
            python -m pip install --no-cache-dir 'redis[hiredis]'
          fi
          set +e
${command_body}
          status=\$?
          set -e
          echo "\${status}" > '${POD_EXIT_CODE_FILE}'
          touch '${POD_DONE_FILE}'
          sleep '${POD_HOLD_SECONDS}'
      volumeMounts:
        - name: seed-script
          mountPath: /scripts/backup_restore_seed.py
          subPath: backup_restore_seed.py
  volumes:
    - name: seed-script
      configMap:
        name: ${CONFIGMAP_NAME}
EOF
}

wait_cluster_healthy() {
  local max_wait="${1:-300}"
  local elapsed=0
  echo "  Waiting for Redis Cluster to become healthy (max ${max_wait}s)..."
  while [[ ${elapsed} -lt ${max_wait} ]]; do
    local state slots masters
    state="$(kubectl exec "${ADMIN_POD}" -n "${NS}" -- \
      redis-cli cluster info 2>/dev/null | grep cluster_state | tr -d '[:space:]')" || true
    slots="$(kubectl exec "${ADMIN_POD}" -n "${NS}" -- \
      redis-cli cluster info 2>/dev/null | grep cluster_slots_ok | cut -d: -f2 | tr -d '[:space:]\r')" || true
    masters="$(kubectl exec "${ADMIN_POD}" -n "${NS}" -- \
      redis-cli cluster nodes 2>/dev/null | grep master | grep -v fail | wc -l | tr -d '[:space:]')" || true
    if [[ "${state}" == "cluster_state:ok" && "${slots}" == "16384" && "${masters:-0}" -ge 3 ]]; then
      echo "  Redis Cluster healthy (${masters} masters) after ${elapsed}s"
      return 0
    fi
    sleep 5
    elapsed=$((elapsed + 5))
  done

  echo "ERROR: Redis Cluster not healthy after ${max_wait}s" >&2
  kubectl get pods -n "${NS}" || true
  kubectl exec "${ADMIN_POD}" -n "${NS}" -- redis-cli cluster info || true
  kubectl exec "${ADMIN_POD}" -n "${NS}" -- redis-cli cluster nodes || true
  return 1
}

trigger_bgsave() {
  echo "  Running CLUSTER SAVECONFIG on all nodes..." >&2
  kubectl exec "${ADMIN_POD}" -n "${NS}" -- \
    redis-cli --cluster call "${CLUSTER_ENDPOINT}" cluster saveconfig >/dev/null || true

  echo "  Triggering BGSAVE on all Redis nodes..." >&2
  local save_start
  save_start="$(date +%s)"

  timeout "${KUBECTL_EXEC_TIMEOUT_SECONDS}s" \
    kubectl exec "${ADMIN_POD}" -n "${NS}" -- \
      redis-cli --cluster call "${CLUSTER_ENDPOINT}" bgsave >&2 || true

  echo "  Waiting for BGSAVE to complete on all nodes..." >&2
  sleep 5
  local waited=0
  local max_wait=900
  while [[ ${waited} -lt ${max_wait} ]]; do
    local in_progress
    in_progress="$(kubectl exec "${ADMIN_POD}" -n "${NS}" -- \
      redis-cli --cluster call "${CLUSTER_ENDPOINT}" info persistence 2>/dev/null \
      | grep rdb_bgsave_in_progress:1 || true)"
    if [[ -z "${in_progress}" ]]; then
      local save_end
      save_end="$(date +%s)"
      local save_dur=$((save_end - save_start))
      echo "  BGSAVE completed in ${save_dur}s" >&2
      echo "${save_start} ${save_end} ${save_dur}"
      return 0
    fi

    echo "    BGSAVE still running after ${waited}s..." >&2
    sleep 5
    waited=$((waited + 5))
  done

  echo "ERROR: BGSAVE did not finish after ${max_wait}s" >&2
  return 1
}

seed_data() {
  local pod_name="$1"
  local run_id="$2"
  local report_file="$3"

  start_backup_tool_pod "${pod_name}" "          python /scripts/backup_restore_seed.py \\
            --mode seed \\
            --host '${HOST}' --port '${PORT}' \\
            --target-mb '${SIZE_MB}' \\
            --run-id '${run_id}' \\
            ${RANDOM_DATA_ARG} \\
            --output '${REMOTE_OUT}/${report_file}'"

  wait_for_pod_ready "${pod_name}" 180
  wait_for_command_pod "${pod_name}" 14400
  kubectl cp "${NS}/${pod_name}:${REMOTE_OUT}/${report_file}" "${LOCAL_OUT}/${report_file}"
  kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found >/dev/null
}

verify_data() {
  local pod_name="$1"
  local seed_report="$2"
  local verify_report="$3"

  start_backup_tool_pod "${pod_name}" "          while [ ! -f '${REMOTE_OUT}/${seed_report}' ]; do sleep 1; done
          python /scripts/backup_restore_seed.py \\
            --mode verify \\
            --host '${HOST}' --port '${PORT}' \\
            --seed-report '${REMOTE_OUT}/${seed_report}' \\
            --verify-mode '${VERIFY_MODE}' \\
            --output '${REMOTE_OUT}/${verify_report}'"

  wait_for_pod_ready "${pod_name}" 180
  kubectl cp "${LOCAL_OUT}/${seed_report}" "${NS}/${pod_name}:${REMOTE_OUT}/${seed_report}"
  wait_for_command_pod "${pod_name}" 7200
  kubectl cp "${NS}/${pod_name}:${REMOTE_OUT}/${verify_report}" "${LOCAL_OUT}/${verify_report}"
  kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found >/dev/null
}

cleanup_data() {
  local pod_name="$1"
  local seed_report="$2"

  start_backup_tool_pod "${pod_name}" "          while [ ! -f '${REMOTE_OUT}/${seed_report}' ]; do sleep 1; done
          python /scripts/backup_restore_seed.py \\
            --mode cleanup \\
            --host '${HOST}' --port '${PORT}' \\
            --seed-report '${REMOTE_OUT}/${seed_report}'"

  wait_for_pod_ready "${pod_name}" 180
  kubectl cp "${LOCAL_OUT}/${seed_report}" "${NS}/${pod_name}:${REMOTE_OUT}/${seed_report}"
  wait_for_command_pod "${pod_name}" 7200 || true
  kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found >/dev/null
}

echo "=========================================="
echo "  Redis 7.2 backup/restore benchmark"
echo "=========================================="
echo "Namespace: ${NS}"
echo "Release: ${RELEASE}"
echo "StatefulSet: ${STS}"
echo "Host: ${HOST}:${PORT}"
echo "Admin endpoint: ${CLUSTER_ENDPOINT}"
echo "Backup image: ${IMAGE}"
echo "Dataset: ${SIZE_MB} MB total"
echo "Runs: ${N}"
echo "RANDOM_DATA=${RANDOM_DATA}"
echo "VERIFY_MODE=${VERIFY_MODE}"

ensure_backup_script_configmap
wait_cluster_healthy 300

for i in $(seq 1 "${N}"); do
  SEED_POD="redis72-backup-seed-${i}"
  VERIFY_POD="redis72-backup-verify-${i}"
  CLEANUP_POD="redis72-backup-cleanup-${i}"
  RUN_ID="redis72-br-${SIZE_MB}mb-${i}-$(date +%s)"
  SEED_REPORT="seed_report_redis72_${SIZE_MB}_${i}.json"
  VERIFY_REPORT="verify_report_redis72_${SIZE_MB}_${i}.json"
  TIMING_FILE="backup_timing_${SIZE_MB}_${i}.json"

  echo ""
  echo "=========================================="
  echo "  Redis backup/restore run ${i}/${N} (${SIZE_MB} MB total)"
  echo "=========================================="

  kubectl delete pod "${SEED_POD}" "${VERIFY_POD}" "${CLEANUP_POD}" -n "${NS}" --ignore-not-found 2>/dev/null || true

  echo "[${i}] Seeding ${SIZE_MB} MB..."
  seed_data "${SEED_POD}" "${RUN_ID}" "${SEED_REPORT}"
  echo "[${i}] Seed report copied."

  SAVE_OUTPUT="$(trigger_bgsave)"
  SAVE_START="$(echo "${SAVE_OUTPUT}" | tail -1 | awk '{print $1}')"
  SAVE_END="$(echo "${SAVE_OUTPUT}" | tail -1 | awk '{print $2}')"
  SAVE_DURATION="$(echo "${SAVE_OUTPUT}" | tail -1 | awk '{print $3}')"

  echo "[${i}] Killing all Redis pods (PVCs preserved)..."
  DELETE_TS="$(date +%s)"
  kubectl delete pods -n "${NS}" \
    -l app.kubernetes.io/instance="${RELEASE}",app.kubernetes.io/name=redis-cluster \
    --wait=false

  echo "[${i}] Waiting for Redis pods to restart..."
  kubectl rollout status sts/"${STS}" -n "${NS}" --timeout=600s
  PODS_READY_TS="$(date +%s)"
  POD_RECREATE_DURATION=$((PODS_READY_TS - DELETE_TS))
  echo "[${i}] Pod recreate time: ${POD_RECREATE_DURATION}s"

  echo "[${i}] Waiting for Redis Cluster to become healthy..."
  wait_cluster_healthy 600
  READY_TS="$(date +%s)"
  RESTORE_DURATION=$((READY_TS - DELETE_TS))
  CLUSTER_RECOVERY_AFTER_PODS=$((READY_TS - PODS_READY_TS))
  echo "[${i}] Restore time: ${RESTORE_DURATION}s"
  echo "[${i}] Cluster recovery after pods ready: ${CLUSTER_RECOVERY_AFTER_PODS}s"

  echo "[${i}] Verifying data integrity..."
  verify_data "${VERIFY_POD}" "${SEED_REPORT}" "${VERIFY_REPORT}"

  SEED_KEYS="$(python3 -c "import json; print(json.load(open('${LOCAL_OUT}/${SEED_REPORT}'))['written_keys'])" 2>/dev/null || echo 0)"
  SEED_DUR="$(python3 -c "import json; print(json.load(open('${LOCAL_OUT}/${SEED_REPORT}'))['seed_duration_s'])" 2>/dev/null || echo 0)"
  INTEGRITY="$(python3 -c "import json; print(json.dumps(bool(json.load(open('${LOCAL_OUT}/${VERIFY_REPORT}'))['integrity_ok'])))" 2>/dev/null || echo false)"
  VERIFY_MODE_REPORTED="$(python3 -c "import json; print(json.load(open('${LOCAL_OUT}/${VERIFY_REPORT}')).get('verify_mode','unknown'))" 2>/dev/null || echo unknown)"
  RESTORED_KEYS="$(python3 -c "import json; print(json.load(open('${LOCAL_OUT}/${VERIFY_REPORT}')).get('restored_keys', json.load(open('${LOCAL_OUT}/${VERIFY_REPORT}')).get('keys_found', 0)))" 2>/dev/null || echo 0)"
  KEY_COUNT_OK="$(python3 -c "import json; print(json.dumps(bool(json.load(open('${LOCAL_OUT}/${VERIFY_REPORT}')).get('key_count_ok', False))))" 2>/dev/null || echo false)"
  USED_MEMORY_DATASET="$(python3 -c "import json; print(json.load(open('${LOCAL_OUT}/${VERIFY_REPORT}')).get('used_memory_dataset', 0))" 2>/dev/null || echo 0)"

  cat > "${LOCAL_OUT}/${TIMING_FILE}" <<EOF
{
  "run": ${i},
  "provider": "selfhosted_redis",
  "redis_version": "7.2",
  "size_mb": ${SIZE_MB},
  "run_id": "${RUN_ID}",
  "seed_keys": ${SEED_KEYS},
  "seed_duration_s": ${SEED_DUR},
  "save_start": ${SAVE_START},
  "save_end": ${SAVE_END},
  "save_duration_s": ${SAVE_DURATION},
  "delete_ts": ${DELETE_TS},
  "pods_ready_ts": ${PODS_READY_TS},
  "ready_ts": ${READY_TS},
  "pod_recreate_duration_s": ${POD_RECREATE_DURATION},
  "cluster_recovery_after_pods_s": ${CLUSTER_RECOVERY_AFTER_PODS},
  "restore_duration_s": ${RESTORE_DURATION},
  "verify_mode": "${VERIFY_MODE_REPORTED}",
  "restored_keys": ${RESTORED_KEYS},
  "key_count_ok": ${KEY_COUNT_OK},
  "used_memory_dataset": ${USED_MEMORY_DATASET},
  "integrity_ok": ${INTEGRITY}
}
EOF

  echo "[${i}] Timing saved: ${LOCAL_OUT}/${TIMING_FILE}"

  echo "[${i}] Cleaning up test keys..."
  cleanup_data "${CLEANUP_POD}" "${SEED_REPORT}"
  echo "[${i}] Done."
done

echo ""
echo "=========================================="
echo "  All ${N} Redis backup/restore runs complete (${SIZE_MB} MB total)."
echo "  Results in: ${LOCAL_OUT}/"
echo "  Analyse with:"
echo "    python cli.py backup --input ${LOCAL_OUT} --output-dir ./plots/redis72_backup"
echo "=========================================="

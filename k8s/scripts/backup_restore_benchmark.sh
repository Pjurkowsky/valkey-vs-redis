#!/usr/bin/env bash
set -euo pipefail

SIZE_MB="${1:?Usage: $0 <size_mb> [output_dir]}"
LOCAL_OUT="${2:-./results/backup}"
N="${N:-3}"
NS="vk"
IMAGE="${BACKUP_IMAGE:-backup_restore:1}"
REMOTE_OUT="/work/results/backup"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
KUBECTL_EXEC_TIMEOUT_SECONDS="${KUBECTL_EXEC_TIMEOUT_SECONDS:-30}"

HOST="valkey.vk.svc.cluster.local"
PORT=6379
RANDOM_DATA="${RANDOM_DATA:-false}"
RANDOM_DATA_ARG=""
if [[ "${RANDOM_DATA}" == "true" ]]; then
  RANDOM_DATA_ARG="--random-data"
fi

source "${SCRIPT_DIR}/pod_results.sh"

mkdir -p "${LOCAL_OUT}"

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
  local timeout_s="${2:-60}"

  kubectl wait pod/"${pod_name}" -n "${NS}" \
    --for=condition=Ready --timeout="${timeout_s}s"
}

wait_cluster_healthy() {
  local max_wait="${1:-300}"
  local elapsed=0
  echo "  Waiting for cluster to become healthy (max ${max_wait}s)..."
  while [[ ${elapsed} -lt ${max_wait} ]]; do
    local state
    state="$(kubectl exec valkey-0 -n "${NS}" -- \
      valkey-cli cluster info 2>/dev/null | grep cluster_state | tr -d '[:space:]')" || true
    if [[ "${state}" == "cluster_state:ok" ]]; then
      local masters
      masters="$(kubectl exec valkey-0 -n "${NS}" -- \
        valkey-cli cluster nodes 2>/dev/null | grep master | wc -l)" || true
      if [[ "${masters}" -ge 3 ]]; then
        echo "  Cluster healthy (${masters} masters) after ${elapsed}s"
        return 0
      fi
    fi
    sleep 5
    elapsed=$((elapsed + 5))
  done
  echo "  WARNING: Cluster not healthy after ${max_wait}s"
  return 1
}

trigger_bgsave() {
  echo "  Triggering BGSAVE on all master pods..." >&2
  local master_pods
  master_pods="$(kubectl exec valkey-0 -n "${NS}" -- \
    valkey-cli cluster nodes 2>/dev/null \
    | grep master | awk '{print $2}' | cut -d@ -f1)"

  local save_start
  save_start="$(date +%s)"

  for addr in ${master_pods}; do
    local h="${addr%%:*}"
    local p="${addr##*:}"
    echo "    BGSAVE ${h}:${p}" >&2
    timeout "${KUBECTL_EXEC_TIMEOUT_SECONDS}s" \
      kubectl exec valkey-0 -c valkey -n "${NS}" -- \
        valkey-cli -h "${h}" -p "${p}" bgsave >&2 || true
  done

  echo "  Waiting for BGSAVE to complete on all masters..." >&2
  sleep 5
  local all_done=false
  local max_wait=120
  local waited=0
  while [[ "${all_done}" != "true" && ${waited} -lt ${max_wait} ]]; do
    all_done=true
    for addr in ${master_pods}; do
      local h="${addr%%:*}"
      local p="${addr##*:}"
      local saving
      saving="$(timeout "${KUBECTL_EXEC_TIMEOUT_SECONDS}s" \
        kubectl exec valkey-0 -c valkey -n "${NS}" -- \
        valkey-cli -h "${h}" -p "${p}" info persistence 2>/dev/null \
        | grep rdb_bgsave_in_progress | tr -d '[:space:]')" || true
      if [[ "${saving}" == *"1"* ]]; then
        all_done=false
        break
      fi
    done
    if [[ "${all_done}" != "true" ]]; then
      echo "    BGSAVE still running after ${waited}s..." >&2
      sleep 3
      waited=$((waited + 3))
    fi
  done

  if [[ "${all_done}" != "true" ]]; then
    echo "  WARNING: BGSAVE did not finish on all masters after ${max_wait}s" >&2
  fi

  local save_end
  save_end="$(date +%s)"
  local save_dur=$((save_end - save_start))
  echo "  BGSAVE completed in ${save_dur}s" >&2
  echo "${save_start} ${save_end} ${save_dur}"
}

for i in $(seq 1 "${N}"); do
  SEED_POD="backup-seed-${i}"
  VERIFY_POD="backup-verify-${i}"
  RUN_ID="br_${SIZE_MB}mb_${i}_$(date +%s)"
  SEED_REPORT="seed_report_${SIZE_MB}_${i}.json"
  VERIFY_REPORT="verify_report_${SIZE_MB}_${i}.json"
  TIMING_FILE="backup_timing_${SIZE_MB}_${i}.json"

  echo ""
  echo "=========================================="
  echo "  Backup/Restore run ${i}/${N} (${SIZE_MB} MB/shard)"
  echo "=========================================="

  kubectl delete pod "${SEED_POD}" -n "${NS}" --ignore-not-found 2>/dev/null || true
  kubectl delete pod "${VERIFY_POD}" -n "${NS}" --ignore-not-found 2>/dev/null || true

  # -- Seed phase --
  echo "[${i}] Seeding ${SIZE_MB} MB per shard..."
  kubectl run "${SEED_POD}" -n "${NS}" \
    --image="${IMAGE}" \
    --restart=Never \
    --command -- \
    /bin/sh -c "
      mkdir -p '${REMOTE_OUT}'
      python /work/backup_restore_seed.py \
        --mode seed \
        --host '${HOST}' --port '${PORT}' \
        --target-mb '${SIZE_MB}' \
        --run-id '${RUN_ID}' \
        ${RANDOM_DATA_ARG} \
        --output '${REMOTE_OUT}/${SEED_REPORT}'
      status=\$?
      echo \"\$status\" > '${POD_EXIT_CODE_FILE}'
      touch '${POD_DONE_FILE}'
      sleep '${POD_HOLD_SECONDS}'
    "

  echo "[${i}] Waiting for seed to complete..."
  wait_for_command_pod "${SEED_POD}" 1800

  kubectl cp "${NS}/${SEED_POD}:${REMOTE_OUT}/${SEED_REPORT}" "${LOCAL_OUT}/${SEED_REPORT}"
  kubectl delete pod "${SEED_POD}" -n "${NS}" --ignore-not-found

  echo "[${i}] Seed report copied."

  # -- BGSAVE phase --
  SAVE_OUTPUT="$(trigger_bgsave)"
  SAVE_START="$(echo "${SAVE_OUTPUT}" | tail -1 | awk '{print $1}')"
  SAVE_END="$(echo "${SAVE_OUTPUT}" | tail -1 | awk '{print $2}')"
  SAVE_DURATION="$(echo "${SAVE_OUTPUT}" | tail -1 | awk '{print $3}')"

  # -- Kill phase --
  echo "[${i}] Killing all Valkey pods (PVCs preserved)..."
  DELETE_TS="$(date +%s)"
  kubectl delete pods -n "${NS}" -l app.kubernetes.io/name=valkey --wait=false

  echo "[${i}] Waiting for pods to terminate..."
  sleep 10

  echo "[${i}] Waiting for pods to restart..."
  kubectl rollout status sts/valkey -n "${NS}" --timeout=300s
  PODS_READY_TS="$(date +%s)"
  POD_RECREATE_DURATION=$((PODS_READY_TS - DELETE_TS))
  echo "[${i}] Pod recreate time: ${POD_RECREATE_DURATION}s"

  echo "[${i}] Waiting for cluster to become healthy..."
  wait_cluster_healthy 300
  READY_TS="$(date +%s)"
  RESTORE_DURATION=$((READY_TS - DELETE_TS))
  CLUSTER_RECOVERY_AFTER_PODS=$((READY_TS - PODS_READY_TS))
  echo "[${i}] Restore time: ${RESTORE_DURATION}s"
  echo "[${i}] Cluster recovery after pods ready: ${CLUSTER_RECOVERY_AFTER_PODS}s"

  # -- Verify phase --
  echo "[${i}] Verifying data integrity..."
  kubectl run "${VERIFY_POD}" -n "${NS}" \
    --image="${IMAGE}" \
    --restart=Never \
    --command -- \
    /bin/sh -c "
      mkdir -p '${REMOTE_OUT}'
      while [ ! -f '${REMOTE_OUT}/${SEED_REPORT}' ]; do sleep 1; done
      python /work/backup_restore_seed.py \
        --mode verify \
        --host '${HOST}' --port '${PORT}' \
        --seed-report '${REMOTE_OUT}/${SEED_REPORT}' \
        --output '${REMOTE_OUT}/${VERIFY_REPORT}'
      status=\$?
      echo \"\$status\" > '${POD_EXIT_CODE_FILE}'
      touch '${POD_DONE_FILE}'
      sleep '${POD_HOLD_SECONDS}'
    "

  wait_for_pod_ready "${VERIFY_POD}" 60
  kubectl cp "${LOCAL_OUT}/${SEED_REPORT}" "${NS}/${VERIFY_POD}:${REMOTE_OUT}/${SEED_REPORT}"

  echo "[${i}] Waiting for verify to complete..."
  wait_for_command_pod "${VERIFY_POD}" 600

  kubectl cp "${NS}/${VERIFY_POD}:${REMOTE_OUT}/${VERIFY_REPORT}" "${LOCAL_OUT}/${VERIFY_REPORT}"
  kubectl delete pod "${VERIFY_POD}" -n "${NS}" --ignore-not-found

  # -- Timing report --
  SEED_KEYS="$(python3 -c "import json; print(json.load(open('${LOCAL_OUT}/${SEED_REPORT}'))['written_keys'])" 2>/dev/null || echo 0)"
  SEED_DUR="$(python3 -c "import json; print(json.load(open('${LOCAL_OUT}/${SEED_REPORT}'))['seed_duration_s'])" 2>/dev/null || echo 0)"
  INTEGRITY="$(python3 -c "import json; print(json.dumps(bool(json.load(open('${LOCAL_OUT}/${VERIFY_REPORT}'))['integrity_ok'])))" 2>/dev/null || echo false)"

  cat > "${LOCAL_OUT}/${TIMING_FILE}" <<EOF
{
  "run": ${i},
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
  "integrity_ok": ${INTEGRITY}
}
EOF

  echo "[${i}] Timing saved: ${LOCAL_OUT}/${TIMING_FILE}"

  # -- Cleanup test keys --
  echo "[${i}] Cleaning up test keys..."
  CLEANUP_POD="backup-cleanup-${i}"
  kubectl delete pod "${CLEANUP_POD}" -n "${NS}" --ignore-not-found 2>/dev/null || true
  kubectl run "${CLEANUP_POD}" -n "${NS}" \
    --image="${IMAGE}" \
    --restart=Never \
    --command -- \
    /bin/sh -c "
      mkdir -p '${REMOTE_OUT}'
      while [ ! -f '${REMOTE_OUT}/${SEED_REPORT}' ]; do sleep 1; done
      python /work/backup_restore_seed.py \
        --mode cleanup \
        --host '${HOST}' --port '${PORT}' \
        --seed-report '${REMOTE_OUT}/${SEED_REPORT}'
      status=\$?
      echo \"\$status\" > '${POD_EXIT_CODE_FILE}'
      touch '${POD_DONE_FILE}'
      sleep '${POD_HOLD_SECONDS}'
    "

  wait_for_pod_ready "${CLEANUP_POD}" 60
  kubectl cp "${LOCAL_OUT}/${SEED_REPORT}" "${NS}/${CLEANUP_POD}:${REMOTE_OUT}/${SEED_REPORT}"

  wait_for_command_pod "${CLEANUP_POD}" 600 || true
  kubectl delete pod "${CLEANUP_POD}" -n "${NS}" --ignore-not-found

  echo "[${i}] Done."
done

echo ""
echo "=========================================="
echo "  All ${N} backup/restore runs complete (${SIZE_MB} MB/shard)."
echo "  Results in: ${LOCAL_OUT}/"
echo "  Analyse with:"
echo "    python cli.py backup --input ${LOCAL_OUT} --output-dir ./plots/backup"
echo "=========================================="

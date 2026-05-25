#!/usr/bin/env bash
set -euo pipefail

SIZE_MB="${1:?Usage: $0 <size_mb> [output_dir]}"
LOCAL_OUT="${2:-./results/backup_restore_external}"
N="${N:-1}"
NS="${NS:-vk}"
RELEASE="${RELEASE:-valkey}"
CHART_PATH="${CHART_PATH:-../valkey-helm/valkey}"
VALUES_FILE="${VALUES_FILE:-k8s/manifests/values-backup-16gb.yaml}"
IMAGE="${BACKUP_IMAGE:-backup_restore:1}"
REMOTE_OUT="/work/results/backup"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
KUBECTL_EXEC_TIMEOUT_SECONDS="${KUBECTL_EXEC_TIMEOUT_SECONDS:-30}"

HOST="${RELEASE}.${NS}.svc.cluster.local"
PORT=6379

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

wait_for_no_valkey_pods() {
  local timeout_s="${1:-300}"
  local deadline=$((SECONDS + timeout_s))

  while (( SECONDS < deadline )); do
    local count
    count="$(kubectl get pods -n "${NS}" -l app.kubernetes.io/instance="${RELEASE}",app.kubernetes.io/name=valkey \
      --no-headers 2>/dev/null | wc -l | tr -d '[:space:]')" || true
    if [[ "${count}" == "0" ]]; then
      return 0
    fi
    sleep 2
  done

  echo "ERROR: Valkey pods did not terminate within ${timeout_s}s." >&2
  kubectl get pods -n "${NS}" -l app.kubernetes.io/instance="${RELEASE}" -o wide || true
  return 1
}

wait_for_pvcs() {
  local expected="$1"
  local timeout_s="${2:-300}"
  local deadline=$((SECONDS + timeout_s))

  while (( SECONDS < deadline )); do
    local ready
    ready="$(kubectl get pvc -n "${NS}" -l app.kubernetes.io/instance="${RELEASE}",app.kubernetes.io/name=valkey \
      --no-headers 2>/dev/null | awk '$2 == "Bound" { count++ } END { print count + 0 }')" || true
    if [[ "${ready}" == "${expected}" ]]; then
      return 0
    fi
    sleep 3
  done

  echo "ERROR: Expected ${expected} bound PVCs." >&2
  kubectl get pvc -n "${NS}" || true
  return 1
}

wait_cluster_healthy() {
  local max_wait="${1:-300}"
  local elapsed=0
  echo "  Waiting for cluster to become healthy (max ${max_wait}s)..."
  while [[ ${elapsed} -lt ${max_wait} ]]; do
    local state
    state="$(kubectl exec "${RELEASE}-0" -c valkey -n "${NS}" -- \
      valkey-cli cluster info 2>/dev/null | grep cluster_state | tr -d '[:space:]')" || true
    if [[ "${state}" == "cluster_state:ok" ]]; then
      local masters
      masters="$(kubectl exec "${RELEASE}-0" -c valkey -n "${NS}" -- \
        valkey-cli cluster nodes 2>/dev/null | grep master | grep -v fail | wc -l | tr -d '[:space:]')" || true
      if [[ "${masters}" -ge 3 ]]; then
        echo "  Cluster healthy (${masters} masters) after ${elapsed}s"
        return 0
      fi
    fi
    sleep 5
    elapsed=$((elapsed + 5))
  done
  echo "ERROR: Cluster not healthy after ${max_wait}s" >&2
  kubectl exec "${RELEASE}-0" -c valkey -n "${NS}" -- valkey-cli cluster info || true
  kubectl exec "${RELEASE}-0" -c valkey -n "${NS}" -- valkey-cli cluster nodes || true
  return 1
}

get_master_pods() {
  kubectl exec "${RELEASE}-0" -c valkey -n "${NS}" -- \
    valkey-cli cluster nodes 2>/dev/null \
    | awk '$3 ~ /master/ && $3 !~ /fail/ {print $2}' \
    | sed -E 's/^[^,]*,//; s/\..*//'
}

get_cluster_pods() {
  local count=6
  for ordinal in $(seq 0 $((count - 1))); do
    echo "${RELEASE}-${ordinal}"
  done
}

require_restore_compatible_master_ordinals() {
  local masters
  masters="$(get_master_pods | sort | tr '\n' ' ' | sed 's/[[:space:]]*$//')"
  local expected="${RELEASE}-0 ${RELEASE}-1 ${RELEASE}-2"

  if [[ "${masters}" != "${expected}" ]]; then
    echo "ERROR: External restore currently expects masters ${expected}, got: ${masters}" >&2
    echo "       Recreate the cluster cleanly before this benchmark, or extend the script slot mapping." >&2
    return 1
  fi
}

trigger_bgsave() {
  echo "  Triggering BGSAVE on all Valkey pods..." >&2
  local valkey_pods
  valkey_pods="$(get_cluster_pods)"

  local save_start
  save_start="$(date +%s)"

  for pod in ${valkey_pods}; do
    echo "    BGSAVE ${pod}" >&2
    timeout "${KUBECTL_EXEC_TIMEOUT_SECONDS}s" \
      kubectl exec "${pod}" -c valkey -n "${NS}" -- valkey-cli bgsave >&2 || true
  done

  echo "  Waiting for BGSAVE to complete on all masters..." >&2
  sleep 5
  local all_done=false
  local max_wait=600
  local waited=0
  while [[ "${all_done}" != "true" && ${waited} -lt ${max_wait} ]]; do
    all_done=true
    for pod in ${valkey_pods}; do
      local saving
      saving="$(timeout "${KUBECTL_EXEC_TIMEOUT_SECONDS}s" \
        kubectl exec "${pod}" -c valkey -n "${NS}" -- \
        valkey-cli info persistence 2>/dev/null \
        | grep rdb_bgsave_in_progress | tr -d '[:space:]')" || true
      if [[ "${saving}" == *"1"* ]]; then
        all_done=false
        break
      fi
    done
    if [[ "${all_done}" != "true" ]]; then
      echo "    BGSAVE still running after ${waited}s..." >&2
      sleep 5
      waited=$((waited + 5))
    fi
  done

  if [[ "${all_done}" != "true" ]]; then
    echo "ERROR: BGSAVE did not finish on all masters after ${max_wait}s" >&2
    return 1
  fi

  local save_end
  save_end="$(date +%s)"
  local save_dur=$((save_end - save_start))
  echo "  BGSAVE completed in ${save_dur}s" >&2
  echo "${save_start} ${save_end} ${save_dur}"
}

create_pvc_copy_pod() {
  local pod_name="$1"
  local claim_name="$2"

  kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl apply -n "${NS}" -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${pod_name}
spec:
  restartPolicy: Never
  containers:
    - name: copy
      image: busybox:1.36
      command: ["sh", "-c", "rm -rf /data/* && sleep 3600"]
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: ${claim_name}
EOF

  wait_for_pod_ready "${pod_name}" 180
}

copy_master_rdbs_to_local_backup() {
  local backup_dir="$1"
  mkdir -p "${backup_dir}"

  local copy_start
  copy_start="$(date +%s)"

  for pod in $(get_cluster_pods); do
    echo "  Copying /data/dump.rdb from ${pod}..." >&2
    kubectl cp -c valkey "${NS}/${pod}:/data/dump.rdb" "${backup_dir}/${pod}.dump.rdb"
    echo "  Copying /data/nodes.conf from ${pod}..." >&2
    kubectl cp -c valkey "${NS}/${pod}:/data/nodes.conf" "${backup_dir}/${pod}.nodes.conf"
  done

  local copy_end
  copy_end="$(date +%s)"
  echo "${copy_start} ${copy_end} $((copy_end - copy_start))"
}

restore_rdbs_into_fresh_pvcs() {
  local backup_dir="$1"
  local restore_copy_start
  restore_copy_start="$(date +%s)"

  for ordinal in 0 1 2 3 4 5; do
    local pod_name="restore-pvc-${ordinal}"
    local claim_name="valkey-data-${RELEASE}-${ordinal}"
    create_pvc_copy_pod "${pod_name}" "${claim_name}"

    local rdb_src="${backup_dir}/${RELEASE}-${ordinal}.dump.rdb"
    local nodes_src="${backup_dir}/${RELEASE}-${ordinal}.nodes.conf"
    if [[ ! -f "${rdb_src}" ]]; then
      echo "ERROR: Missing backup file ${rdb_src}" >&2
      return 1
    fi
    if [[ ! -f "${nodes_src}" ]]; then
      echo "ERROR: Missing backup file ${nodes_src}" >&2
      return 1
    fi

    echo "  Restoring ${rdb_src} into ${claim_name}/dump.rdb..." >&2
    kubectl cp "${rdb_src}" "${NS}/${pod_name}:/data/dump.rdb"
    echo "  Restoring ${nodes_src} into ${claim_name}/nodes.conf..." >&2
    kubectl cp "${nodes_src}" "${NS}/${pod_name}:/data/nodes.conf"
    kubectl exec "${pod_name}" -n "${NS}" -- sh -c "chmod 644 /data/dump.rdb /data/nodes.conf"

    if [[ ${ordinal} -lt 3 ]]; then
      local slots
      slots="$(grep -E 'myself,master|master,myself' "${nodes_src}" | awk '{for (i=9; i<=NF; i++) printf "%s ", $i; print ""}' | sed 's/[[:space:]]*$//')" || true
      if [[ -z "${slots}" ]]; then
        echo "ERROR: ${nodes_src} does not contain a master slot assignment for ${RELEASE}-${ordinal}" >&2
        return 1
      fi
      echo "  Restored master ${RELEASE}-${ordinal} slots: ${slots}" >&2
    else
      echo "  Restored replica metadata for ${RELEASE}-${ordinal}." >&2
    fi

    kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found >/dev/null
  done

  local restore_copy_end
  restore_copy_end="$(date +%s)"
  echo "${restore_copy_start} ${restore_copy_end} $((restore_copy_end - restore_copy_start))"
}

for i in $(seq 1 "${N}"); do
  SEED_POD="backup-seed-${i}"
  VERIFY_POD="backup-verify-${i}"
  RUN_ID="br_external_${SIZE_MB}mb_${i}_$(date +%s)"
  RUN_BACKUP_DIR="${LOCAL_OUT}/rdb_${SIZE_MB}_${i}"
  SEED_REPORT="seed_report_${SIZE_MB}_${i}.json"
  VERIFY_REPORT="verify_report_${SIZE_MB}_${i}.json"
  TIMING_FILE="backup_timing_${SIZE_MB}_${i}.json"

  echo ""
  echo "=========================================="
  echo "  External Backup/Restore run ${i}/${N} (${SIZE_MB} MB total)"
  echo "=========================================="

  kubectl delete pod "${SEED_POD}" -n "${NS}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl delete pod "${VERIFY_POD}" -n "${NS}" --ignore-not-found >/dev/null 2>&1 || true

  echo "[${i}] Waiting for source cluster health..."
  wait_cluster_healthy 300
  require_restore_compatible_master_ordinals

  echo "[${i}] Seeding ${SIZE_MB} MB total..."
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
        --output '${REMOTE_OUT}/${SEED_REPORT}'
      status=\$?
      echo \"\$status\" > '${POD_EXIT_CODE_FILE}'
      touch '${POD_DONE_FILE}'
      sleep '${POD_HOLD_SECONDS}'
    "

  echo "[${i}] Waiting for seed to complete..."
  wait_for_command_pod "${SEED_POD}" 7200
  kubectl cp "${NS}/${SEED_POD}:${REMOTE_OUT}/${SEED_REPORT}" "${LOCAL_OUT}/${SEED_REPORT}"
  kubectl delete pod "${SEED_POD}" -n "${NS}" --ignore-not-found >/dev/null

  SAVE_OUTPUT="$(trigger_bgsave)"
  SAVE_START="$(echo "${SAVE_OUTPUT}" | awk '{print $1}')"
  SAVE_END="$(echo "${SAVE_OUTPUT}" | awk '{print $2}')"
  SAVE_DURATION="$(echo "${SAVE_OUTPUT}" | awk '{print $3}')"

  echo "[${i}] Copying RDB files to local backup directory..."
  BACKUP_COPY_OUTPUT="$(copy_master_rdbs_to_local_backup "${RUN_BACKUP_DIR}")"
  BACKUP_COPY_START="$(echo "${BACKUP_COPY_OUTPUT}" | tail -1 | awk '{print $1}')"
  BACKUP_COPY_END="$(echo "${BACKUP_COPY_OUTPUT}" | tail -1 | awk '{print $2}')"
  BACKUP_COPY_DURATION="$(echo "${BACKUP_COPY_OUTPUT}" | tail -1 | awk '{print $3}')"

  echo "[${i}] Destroying cluster and PVCs..."
  RESTORE_START="$(date +%s)"
  helm uninstall "${RELEASE}" -n "${NS}" --ignore-not-found
  kubectl delete pvc -n "${NS}" -l app.kubernetes.io/instance="${RELEASE}",app.kubernetes.io/name=valkey --ignore-not-found --wait=true

  echo "[${i}] Installing fresh cluster resources..."
  helm install "${RELEASE}" "${CHART_PATH}" -n "${NS}" -f "${VALUES_FILE}" --wait=false
  wait_for_pvcs 6 300

  echo "[${i}] Scaling StatefulSet down before injecting RDB files..."
  kubectl scale sts/"${RELEASE}" -n "${NS}" --replicas=0
  wait_for_no_valkey_pods 300

  echo "[${i}] Restoring RDB files into fresh PVCs..."
  RESTORE_COPY_OUTPUT="$(restore_rdbs_into_fresh_pvcs "${RUN_BACKUP_DIR}")"
  RESTORE_COPY_START="$(echo "${RESTORE_COPY_OUTPUT}" | tail -1 | awk '{print $1}')"
  RESTORE_COPY_END="$(echo "${RESTORE_COPY_OUTPUT}" | tail -1 | awk '{print $2}')"
  RESTORE_COPY_DURATION="$(echo "${RESTORE_COPY_OUTPUT}" | tail -1 | awk '{print $3}')"

  echo "[${i}] Starting restored cluster..."
  kubectl scale sts/"${RELEASE}" -n "${NS}" --replicas=6
  kubectl rollout status sts/"${RELEASE}" -n "${NS}" --timeout=900s
  PODS_READY_TS="$(date +%s)"
  POD_RECREATE_DURATION=$((PODS_READY_TS - RESTORE_COPY_END))

  wait_cluster_healthy 600
  READY_TS="$(date +%s)"
  CLUSTER_RECOVERY_AFTER_PODS=$((READY_TS - PODS_READY_TS))
  RESTORE_DURATION=$((READY_TS - RESTORE_START))

  echo "[${i}] Restore duration: ${RESTORE_DURATION}s"
  echo "[${i}] RDB injection duration: ${RESTORE_COPY_DURATION}s"
  echo "[${i}] Pod recreate duration after injection: ${POD_RECREATE_DURATION}s"
  echo "[${i}] Cluster recovery after pods ready: ${CLUSTER_RECOVERY_AFTER_PODS}s"

  echo "[${i}] Verifying restored data..."
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

  wait_for_pod_ready "${VERIFY_POD}" 120
  kubectl cp "${LOCAL_OUT}/${SEED_REPORT}" "${NS}/${VERIFY_POD}:${REMOTE_OUT}/${SEED_REPORT}"
  wait_for_command_pod "${VERIFY_POD}" 1800
  kubectl cp "${NS}/${VERIFY_POD}:${REMOTE_OUT}/${VERIFY_REPORT}" "${LOCAL_OUT}/${VERIFY_REPORT}"
  kubectl delete pod "${VERIFY_POD}" -n "${NS}" --ignore-not-found >/dev/null

  SEED_KEYS="$(python3 -c "import json; print(json.load(open('${LOCAL_OUT}/${SEED_REPORT}'))['written_keys'])" 2>/dev/null || echo 0)"
  SEED_DUR="$(python3 -c "import json; print(json.load(open('${LOCAL_OUT}/${SEED_REPORT}'))['seed_duration_s'])" 2>/dev/null || echo 0)"
  INTEGRITY="$(python3 -c "import json; print(json.dumps(bool(json.load(open('${LOCAL_OUT}/${VERIFY_REPORT}'))['integrity_ok'])))" 2>/dev/null || echo false)"

  cat > "${LOCAL_OUT}/${TIMING_FILE}" <<EOF
{
  "run": ${i},
  "size_mb": ${SIZE_MB},
  "run_id": "${RUN_ID}",
  "variant": "external_rdb_restore",
  "seed_keys": ${SEED_KEYS},
  "seed_duration_s": ${SEED_DUR},
  "save_start": ${SAVE_START},
  "save_end": ${SAVE_END},
  "save_duration_s": ${SAVE_DURATION},
  "backup_copy_start": ${BACKUP_COPY_START},
  "backup_copy_end": ${BACKUP_COPY_END},
  "backup_copy_duration_s": ${BACKUP_COPY_DURATION},
  "restore_start": ${RESTORE_START},
  "restore_copy_start": ${RESTORE_COPY_START},
  "restore_copy_end": ${RESTORE_COPY_END},
  "pvc_restore_copy_duration_s": ${RESTORE_COPY_DURATION},
  "pods_ready_ts": ${PODS_READY_TS},
  "ready_ts": ${READY_TS},
  "pod_recreate_duration_s": ${POD_RECREATE_DURATION},
  "cluster_recovery_after_pods_s": ${CLUSTER_RECOVERY_AFTER_PODS},
  "restore_duration_s": ${RESTORE_DURATION},
  "integrity_ok": ${INTEGRITY}
}
EOF

  echo "[${i}] Timing saved: ${LOCAL_OUT}/${TIMING_FILE}"
done

echo ""
echo "=========================================="
echo "  All ${N} external backup/restore runs complete (${SIZE_MB} MB total)."
echo "  Results in: ${LOCAL_OUT}/"
echo "  Analyse with:"
echo "    python cli.py backup --input ${LOCAL_OUT} --output-dir ./plots/backup_external"
echo "=========================================="

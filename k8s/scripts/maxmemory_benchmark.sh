#!/usr/bin/env bash
set -euo pipefail

TARGET_MB="${1:-4096}"
LOCAL_OUT="${2:-./results/maxmemory}"
N="${N:-1}"
NS="vk"
IMAGE="${BACKUP_IMAGE:-backup_restore:1}"
REMOTE_OUT="/work/results/maxmemory"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

source "${SCRIPT_DIR}/target_config.sh"
source "${SCRIPT_DIR}/pod_results.sh"

HOST="${TC_HOST}"
PORT="${TC_PORT}"
CLI="${TC_CLI}"
ADMIN_POD="$(tc_admin_pod)"

mkdir -p "${LOCAL_OUT}"

master_addrs() {
  kubectl exec "${ADMIN_POD}" -n "${NS}" -- \
    ${CLI} cluster nodes 2>/dev/null \
    | awk '$3 ~ /master/ && $3 !~ /fail/ {print $2}' \
    | cut -d@ -f1
}

snapshot_cluster() {
  local output="$1"
  local phase="$2"

  {
    echo "{"
    echo "  \"phase\": \"${phase}\","
    echo "  \"timestamp_s\": $(date +%s),"
    echo "  \"masters\": ["

    local first=true
    local addr
    for addr in $(master_addrs); do
      local host="${addr%%:*}"
      local port="${addr##*:}"
      local info
      info="$(kubectl exec "${ADMIN_POD}" -n "${NS}" -- \
        ${CLI} -h "${host}" -p "${port}" info all 2>/dev/null || true)"

      local used_memory maxmemory evicted_keys keyspace_hits keyspace_misses total_keys
      used_memory="$(awk -F: '$1=="used_memory" {gsub(/\r/,"",$2); print $2}' <<<"${info}")"
      maxmemory="$(awk -F: '$1=="maxmemory" {gsub(/\r/,"",$2); print $2}' <<<"${info}")"
      evicted_keys="$(awk -F: '$1=="evicted_keys" {gsub(/\r/,"",$2); print $2}' <<<"${info}")"
      keyspace_hits="$(awk -F: '$1=="keyspace_hits" {gsub(/\r/,"",$2); print $2}' <<<"${info}")"
      keyspace_misses="$(awk -F: '$1=="keyspace_misses" {gsub(/\r/,"",$2); print $2}' <<<"${info}")"
      total_keys="$(kubectl exec "${ADMIN_POD}" -n "${NS}" -- \
        ${CLI} -h "${host}" -p "${port}" dbsize 2>/dev/null | tr -d '\r' || echo 0)"

      if [[ "${first}" == "true" ]]; then
        first=false
      else
        echo ","
      fi

      cat <<EOF
    {
      "addr": "${addr}",
      "used_memory": ${used_memory:-0},
      "maxmemory": ${maxmemory:-0},
      "evicted_keys": ${evicted_keys:-0},
      "keyspace_hits": ${keyspace_hits:-0},
      "keyspace_misses": ${keyspace_misses:-0},
      "dbsize": ${total_keys:-0}
    }
EOF
    done

    echo ""
    echo "  ]"
    echo "}"
  } > "${output}"
}

sum_json_field() {
  local file="$1"
  local field="$2"
  python3 -c "import json,sys; print(sum(int(m.get('${field}', 0)) for m in json.load(open('${file}')).get('masters', [])))"
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
  local timeout_s="${2:-60}"

  kubectl wait pod/"${pod_name}" -n "${NS}" \
    --for=condition=Ready --timeout="${timeout_s}s"
}

for i in $(seq 1 "${N}"); do
  SEED_POD="maxmemory-seed-${i}"
  VERIFY_POD="maxmemory-verify-${i}"
  CLEANUP_POD="maxmemory-cleanup-${i}"
  RUN_ID="maxmem_${TARGET_MB}mb_${i}_$(date +%s)"
  BEFORE_FILE="maxmemory_before_${i}.json"
  AFTER_FILE="maxmemory_after_${i}.json"
  SEED_REPORT="maxmemory_seed_${i}.json"
  VERIFY_REPORT="maxmemory_verify_${i}.json"
  SUMMARY_FILE="maxmemory_summary_${i}.json"

  echo ""
  echo "=========================================="
  echo "  Maxmemory run ${i}/${N} (${TARGET_MB} MB writes, target=${TARGET})"
  echo "=========================================="

  kubectl delete pod "${SEED_POD}" "${VERIFY_POD}" "${CLEANUP_POD}" \
    -n "${NS}" --ignore-not-found 2>/dev/null || true

  echo "[${i}] Capturing before snapshot..."
  snapshot_cluster "${LOCAL_OUT}/${BEFORE_FILE}" "before"

  echo "[${i}] Writing ${TARGET_MB} MB of 1KB values..."
  SEED_START="$(date +%s)"
  kubectl run "${SEED_POD}" -n "${NS}" \
    --image="${IMAGE}" \
    --restart=Never \
    --command -- \
    /bin/sh -c "
      mkdir -p '${REMOTE_OUT}'
      python /work/backup_restore_seed.py \
        --mode seed \
        --host '${HOST}' --port '${PORT}' \
        --target-mb '${TARGET_MB}' \
        --run-id '${RUN_ID}' \
        --output '${REMOTE_OUT}/${SEED_REPORT}'
      status=\$?
      echo \"\$status\" > '${POD_EXIT_CODE_FILE}'
      touch '${POD_DONE_FILE}'
      sleep '${POD_HOLD_SECONDS}'
    "

  wait_for_command_pod "${SEED_POD}" 3600
  SEED_END="$(date +%s)"

  kubectl cp "${NS}/${SEED_POD}:${REMOTE_OUT}/${SEED_REPORT}" "${LOCAL_OUT}/${SEED_REPORT}"
  kubectl delete pod "${SEED_POD}" -n "${NS}" --ignore-not-found

  echo "[${i}] Capturing after snapshot..."
  snapshot_cluster "${LOCAL_OUT}/${AFTER_FILE}" "after"

  echo "[${i}] Verifying sample of written keys..."
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
  wait_for_command_pod "${VERIFY_POD}" 1800
  kubectl cp "${NS}/${VERIFY_POD}:${REMOTE_OUT}/${VERIFY_REPORT}" "${LOCAL_OUT}/${VERIFY_REPORT}"
  kubectl delete pod "${VERIFY_POD}" -n "${NS}" --ignore-not-found

  before_evicted="$(sum_json_field "${LOCAL_OUT}/${BEFORE_FILE}" evicted_keys)"
  after_evicted="$(sum_json_field "${LOCAL_OUT}/${AFTER_FILE}" evicted_keys)"
  before_used="$(sum_json_field "${LOCAL_OUT}/${BEFORE_FILE}" used_memory)"
  after_used="$(sum_json_field "${LOCAL_OUT}/${AFTER_FILE}" used_memory)"
  before_keys="$(sum_json_field "${LOCAL_OUT}/${BEFORE_FILE}" dbsize)"
  after_keys="$(sum_json_field "${LOCAL_OUT}/${AFTER_FILE}" dbsize)"
  seed_keys="$(python3 -c "import json; print(json.load(open('${LOCAL_OUT}/${SEED_REPORT}'))['written_keys'])")"
  seed_duration="$(python3 -c "import json; print(json.load(open('${LOCAL_OUT}/${SEED_REPORT}'))['seed_duration_s'])")"
  sample_size="$(python3 -c "import json; print(json.load(open('${LOCAL_OUT}/${VERIFY_REPORT}'))['sample_size'])")"
  keys_missing="$(python3 -c "import json; print(json.load(open('${LOCAL_OUT}/${VERIFY_REPORT}'))['keys_missing'])")"
  verify_errors="$(python3 -c "import json; print(json.load(open('${LOCAL_OUT}/${VERIFY_REPORT}'))['verify_errors'])")"

  cat > "${LOCAL_OUT}/${SUMMARY_FILE}" <<EOF
{
  "run": ${i},
  "run_id": "${RUN_ID}",
  "target_mb": ${TARGET_MB},
  "seed_start_s": ${SEED_START},
  "seed_end_s": ${SEED_END},
  "seed_duration_s": ${seed_duration},
  "written_keys": ${seed_keys},
  "used_memory_before": ${before_used},
  "used_memory_after": ${after_used},
  "dbsize_before": ${before_keys},
  "dbsize_after": ${after_keys},
  "evicted_keys_before": ${before_evicted},
  "evicted_keys_after": ${after_evicted},
  "evicted_keys_delta": $((after_evicted - before_evicted)),
  "sample_size": ${sample_size},
  "sample_missing": ${keys_missing},
  "sample_missing_rate": $(python3 -c "print(${keys_missing} / ${sample_size} if ${sample_size} else 0.0)"),
  "verify_errors": ${verify_errors}
}
EOF

  echo "[${i}] Summary saved: ${LOCAL_OUT}/${SUMMARY_FILE}"
  echo "[${i}] Evicted keys delta: $((after_evicted - before_evicted))"
  echo "[${i}] Sample missing: ${keys_missing}/${sample_size}"

  echo "[${i}] Cleaning up surviving test keys..."
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
  wait_for_command_pod "${CLEANUP_POD}" 1800 || true
  kubectl delete pod "${CLEANUP_POD}" -n "${NS}" --ignore-not-found
done

echo ""
echo "=========================================="
echo "  All ${N} maxmemory runs complete."
echo "  Results in: ${LOCAL_OUT}/"
echo "  Analyse with:"
echo "    python cli.py maxmemory --input ${LOCAL_OUT} --output-dir ./plots/maxmemory"
echo "=========================================="

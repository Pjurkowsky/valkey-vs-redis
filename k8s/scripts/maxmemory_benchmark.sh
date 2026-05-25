#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<EOF
Usage: $0 [target-mb] [output-dir]

Runs maxmemory-policy pressure tests against Valkey in Kubernetes or Memorystore
for Redis Cluster.

Environment:
  PROVIDER=valkey|memorystore        default: valkey
  MAXMEMORY_POLICIES="allkeys-lru volatile-lru"
  KEY_TTL_SECONDS=0                  0 means writes have no TTL
  N=1
  BACKUP_IMAGE=.../backup_restore:1

Valkey:
  HOST=valkey.vk.svc.cluster.local
  PORT=6379
  VALKEY_MAXMEMORY=1gb               optional CONFIG SET maxmemory value

Memorystore:
  MEMORYSTORE_CLUSTER_ID=redis-ms-2  required when PROVIDER=memorystore
  PROJECT_ID=<gcloud project>
  LOCATION=europe-central2
  MEMORYSTORE_MAXMEMORY=<bytes>      optional managed config update
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

TARGET_MB="${1:-4096}"
LOCAL_OUT="${2:-./results/maxmemory}"
N="${N:-1}"
NS="${NS:-vk}"
PROVIDER="${PROVIDER:-valkey}"
LOCATION="${LOCATION:-europe-central2}"
PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null || true)}"
ARTIFACT_REPO="${ARTIFACT_REPO:-valkey-bench}"
if [[ -n "${BACKUP_IMAGE:-}" ]]; then
  IMAGE="${BACKUP_IMAGE}"
elif [[ -n "${PROJECT_ID}" && "${PROJECT_ID}" != "(unset)" ]]; then
  IMAGE="${LOCATION}-docker.pkg.dev/${PROJECT_ID}/${ARTIFACT_REPO}/backup_restore:1"
else
  IMAGE="backup_restore:1"
fi
REMOTE_OUT="/work/results/maxmemory"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

source "${SCRIPT_DIR}/pod_results.sh"

HOST="${HOST:-valkey.vk.svc.cluster.local}"
PORT="${PORT:-6379}"
MEMORYSTORE_CLUSTER_ID="${MEMORYSTORE_CLUSTER_ID:-${MEMORYSTORE_CLUSTER:-}}"
MAXMEMORY_POLICIES="${MAXMEMORY_POLICIES:-allkeys-lru volatile-lru}"
KEY_TTL_SECONDS="${KEY_TTL_SECONDS:-0}"
STOP_AFTER_ERRORS="${STOP_AFTER_ERRORS:-1000}"
FLUSH_BETWEEN_RUNS="${FLUSH_BETWEEN_RUNS:-true}"
CONFIGURE_POLICY="${CONFIGURE_POLICY:-true}"
VALKEY_MAXMEMORY="${VALKEY_MAXMEMORY:-}"
MEMORYSTORE_MAXMEMORY="${MEMORYSTORE_MAXMEMORY:-}"

mkdir -p "${LOCAL_OUT}"

PYTHON_BIN="$(command -v python3 || command -v python || true)"
if [[ -z "${PYTHON_BIN}" ]]; then
  echo "ERROR: python3 or python is required." >&2
  exit 1
fi

if [[ "${PROVIDER}" != "valkey" && "${PROVIDER}" != "memorystore" ]]; then
  echo "ERROR: PROVIDER must be valkey or memorystore." >&2
  exit 1
fi

if [[ "${PROVIDER}" == "memorystore" ]]; then
  if [[ -z "${MEMORYSTORE_CLUSTER_ID}" ]]; then
    echo "ERROR: Set MEMORYSTORE_CLUSTER_ID when PROVIDER=memorystore." >&2
    exit 1
  fi
  if [[ -z "${PROJECT_ID}" || "${PROJECT_ID}" == "(unset)" ]]; then
    echo "ERROR: Could not determine GCP project. Set PROJECT_ID." >&2
    exit 1
  fi
fi

json_string() {
  "${PYTHON_BIN}" -c 'import json, sys; print(json.dumps(sys.argv[1]))' "$1"
}

describe_ms_cluster_json() {
  gcloud redis clusters describe "${MEMORYSTORE_CLUSTER_ID}" \
    --project="${PROJECT_ID}" \
    --region="${LOCATION}" \
    --format=json
}

ms_cluster_state() {
  describe_ms_cluster_json | "${PYTHON_BIN}" -c "import json,sys; print(json.load(sys.stdin).get('state', ''))"
}

ms_state_is_ready() {
  case "$1" in
    ""|ACTIVE|READY)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

wait_for_ms_cluster_ready() {
  local timeout="${1:-1800}"
  local deadline=$((SECONDS + timeout))
  local state

  echo "  Waiting for ${MEMORYSTORE_CLUSTER_ID} ready state (max ${timeout}s)..."
  while (( SECONDS < deadline )); do
    state="$(ms_cluster_state || true)"
    if ms_state_is_ready "${state}"; then
      echo "  ${MEMORYSTORE_CLUSTER_ID} ready; state=${state:-unknown}"
      return 0
    fi
    echo "  ${MEMORYSTORE_CLUSTER_ID} state=${state:-unknown}; waiting..."
    sleep 15
  done

  echo "ERROR: ${MEMORYSTORE_CLUSTER_ID} did not become ready within ${timeout}s" >&2
  return 1
}

discover_ms_endpoint() {
  describe_ms_cluster_json | "${PYTHON_BIN}" -c '
import json, sys
doc = json.load(sys.stdin)
for endpoint in doc.get("discoveryEndpoints") or []:
    address = endpoint.get("address")
    port = endpoint.get("port") or 6379
    if address:
        print(f"{address} {port}")
        raise SystemExit(0)
raise SystemExit("Could not find Memorystore discovery endpoint")
'
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
  local timeout_s="${2:-90}"

  kubectl wait pod/"${pod_name}" -n "${NS}" \
    --for=condition=Ready --timeout="${timeout_s}s"
}

run_seed_tool_pod() {
  local pod_name="$1"
  local timeout_s="$2"
  local shell_body="$3"

  kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl run "${pod_name}" -n "${NS}" \
    --image="${IMAGE}" \
    --restart=Never \
    --command -- \
    /bin/sh -c "${shell_body}"

  wait_for_command_pod "${pod_name}" "${timeout_s}"
}

tool_shell_prefix() {
  cat <<EOF
mkdir -p '${REMOTE_OUT}'
EOF
}

snapshot_cluster() {
  local pod_name="$1"
  local phase="$2"
  local local_file="$3"
  local remote_file="${REMOTE_OUT}/$(basename "${local_file}")"

  run_seed_tool_pod "${pod_name}" 300 "$(tool_shell_prefix)
python /work/backup_restore_seed.py \
  --mode snapshot \
  --host '${HOST}' --port '${PORT}' \
  --phase '${phase}' \
  --output '${remote_file}'
status=\$?
echo \"\$status\" > '${POD_EXIT_CODE_FILE}'
touch '${POD_DONE_FILE}'
sleep '${POD_HOLD_SECONDS}'
"
  kubectl cp "${NS}/${pod_name}:${remote_file}" "${local_file}"
  kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found >/dev/null
}

flush_cluster() {
  local pod_name="$1"

  run_seed_tool_pod "${pod_name}" 600 "$(tool_shell_prefix)
python /work/backup_restore_seed.py \
  --mode flush \
  --host '${HOST}' --port '${PORT}' \
  --reset-stats
status=\$?
echo \"\$status\" > '${POD_EXIT_CODE_FILE}'
touch '${POD_DONE_FILE}'
sleep '${POD_HOLD_SECONDS}'
"
  kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found >/dev/null
}

configure_valkey_policy() {
  local policy="$1"
  local pod_name="$2"
  local maxmemory_arg=""

  if [[ -n "${VALKEY_MAXMEMORY}" ]]; then
    maxmemory_arg="--maxmemory '${VALKEY_MAXMEMORY}'"
  fi

  run_seed_tool_pod "${pod_name}" 300 "$(tool_shell_prefix)
python /work/backup_restore_seed.py \
  --mode configure \
  --host '${HOST}' --port '${PORT}' \
  --maxmemory-policy '${policy}' \
  ${maxmemory_arg}
status=\$?
echo \"\$status\" > '${POD_EXIT_CODE_FILE}'
touch '${POD_DONE_FILE}'
sleep '${POD_HOLD_SECONDS}'
"
  kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found >/dev/null
}

configure_memorystore_policy() {
  local policy="$1"
  local update_config="maxmemory-policy=${policy}"

  if [[ -n "${MEMORYSTORE_MAXMEMORY}" ]]; then
    update_config="${update_config},maxmemory=${MEMORYSTORE_MAXMEMORY}"
  fi

  gcloud redis clusters update "${MEMORYSTORE_CLUSTER_ID}" \
    --project="${PROJECT_ID}" \
    --region="${LOCATION}" \
    --update-redis-config="${update_config}" \
    --quiet
  wait_for_ms_cluster_ready 1800
}

run_seed() {
  local pod_name="$1"
  local run_id="$2"
  local report_file="$3"
  local ttl_arg=""

  if [[ "${KEY_TTL_SECONDS}" != "0" ]]; then
    ttl_arg="--ttl-seconds '${KEY_TTL_SECONDS}'"
  fi

  run_seed_tool_pod "${pod_name}" 14400 "$(tool_shell_prefix)
python /work/backup_restore_seed.py \
  --mode seed \
  --host '${HOST}' --port '${PORT}' \
  --target-mb '${TARGET_MB}' \
  --run-id '${run_id}' \
  --allow-partial \
  --stop-after-errors '${STOP_AFTER_ERRORS}' \
  ${ttl_arg} \
  --output '${REMOTE_OUT}/${report_file}'
status=\$?
echo \"\$status\" > '${POD_EXIT_CODE_FILE}'
touch '${POD_DONE_FILE}'
sleep '${POD_HOLD_SECONDS}'
"
  kubectl cp "${NS}/${pod_name}:${REMOTE_OUT}/${report_file}" "${LOCAL_OUT}/${report_file}"
  kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found >/dev/null
}

run_verify() {
  local pod_name="$1"
  local seed_report="$2"
  local verify_report="$3"

  run_seed_tool_pod "${pod_name}" 3600 "$(tool_shell_prefix)
while [ ! -f '${REMOTE_OUT}/${seed_report}' ]; do sleep 1; done
python /work/backup_restore_seed.py \
  --mode verify \
  --host '${HOST}' --port '${PORT}' \
  --seed-report '${REMOTE_OUT}/${seed_report}' \
  --output '${REMOTE_OUT}/${verify_report}'
status=\$?
echo \"\$status\" > '${POD_EXIT_CODE_FILE}'
touch '${POD_DONE_FILE}'
sleep '${POD_HOLD_SECONDS}'
" &
  local run_pid=$!
  wait_for_pod_ready "${pod_name}" 120
  kubectl cp "${LOCAL_OUT}/${seed_report}" "${NS}/${pod_name}:${REMOTE_OUT}/${seed_report}"
  wait "${run_pid}"
  kubectl cp "${NS}/${pod_name}:${REMOTE_OUT}/${verify_report}" "${LOCAL_OUT}/${verify_report}"
  kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found >/dev/null
}

run_cleanup() {
  local pod_name="$1"
  local seed_report="$2"

  run_seed_tool_pod "${pod_name}" 3600 "$(tool_shell_prefix)
while [ ! -f '${REMOTE_OUT}/${seed_report}' ]; do sleep 1; done
python /work/backup_restore_seed.py \
  --mode cleanup \
  --host '${HOST}' --port '${PORT}' \
  --seed-report '${REMOTE_OUT}/${seed_report}'
status=\$?
echo \"\$status\" > '${POD_EXIT_CODE_FILE}'
touch '${POD_DONE_FILE}'
sleep '${POD_HOLD_SECONDS}'
" &
  local run_pid=$!
  wait_for_pod_ready "${pod_name}" 120
  kubectl cp "${LOCAL_OUT}/${seed_report}" "${NS}/${pod_name}:${REMOTE_OUT}/${seed_report}"
  wait "${run_pid}" || true
  kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found >/dev/null
}

sum_json_field() {
  local file="$1"
  local field="$2"
  "${PYTHON_BIN}" -c "import json,sys; print(sum(int(m.get('${field}', 0)) for m in json.load(open('${file}')).get('masters', [])))"
}

first_json_field() {
  local file="$1"
  local field="$2"
  "${PYTHON_BIN}" -c "import json,sys; masters=json.load(open('${file}')).get('masters', []); print(masters[0].get('${field}', '') if masters else '')"
}

policies="$(printf '%s' "${MAXMEMORY_POLICIES}" | tr ',' ' ')"

if [[ "${PROVIDER}" == "memorystore" ]]; then
  wait_for_ms_cluster_ready 900
  read -r HOST PORT < <(discover_ms_endpoint)
fi

echo "==> Maxmemory benchmark"
echo "PROVIDER=${PROVIDER}"
echo "HOST=${HOST}"
echo "PORT=${PORT}"
echo "TARGET_MB=${TARGET_MB}"
echo "POLICIES=${policies}"
echo "KEY_TTL_SECONDS=${KEY_TTL_SECONDS}"
echo "N=${N}"
echo "BACKUP_IMAGE=${IMAGE}"

for policy in ${policies}; do
  safe_policy="${policy//[^a-zA-Z0-9]/-}"

  echo ""
  echo "=========================================="
  echo "  Policy: ${policy}"
  echo "=========================================="

  if [[ "${CONFIGURE_POLICY}" == "true" ]]; then
    echo "Configuring ${PROVIDER} maxmemory-policy=${policy}..."
    if [[ "${PROVIDER}" == "valkey" ]]; then
      configure_valkey_policy "${policy}" "maxmemory-config-${safe_policy}"
    else
      configure_memorystore_policy "${policy}"
    fi
  fi

  for i in $(seq 1 "${N}"); do
    SEED_POD="maxmemory-seed-${safe_policy}-${i}"
    VERIFY_POD="maxmemory-verify-${safe_policy}-${i}"
    CLEANUP_POD="maxmemory-cleanup-${safe_policy}-${i}"
    SNAP_BEFORE_POD="maxmemory-before-${safe_policy}-${i}"
    SNAP_AFTER_POD="maxmemory-after-${safe_policy}-${i}"
    FLUSH_POD="maxmemory-flush-${safe_policy}-${i}"
    RUN_ID="maxmem_${PROVIDER}_${safe_policy}_${TARGET_MB}mb_${i}_$(date +%s)"
    BEFORE_FILE="maxmemory_before_${PROVIDER}_${safe_policy}_${i}.json"
    AFTER_FILE="maxmemory_after_${PROVIDER}_${safe_policy}_${i}.json"
    SEED_REPORT="maxmemory_seed_${PROVIDER}_${safe_policy}_${i}.json"
    VERIFY_REPORT="maxmemory_verify_${PROVIDER}_${safe_policy}_${i}.json"
    SUMMARY_FILE="maxmemory_summary_${PROVIDER}_${safe_policy}_${i}.json"

    echo ""
    echo "------------------------------------------"
    echo "  Maxmemory run ${i}/${N} (${policy}, ${TARGET_MB} MB writes)"
    echo "------------------------------------------"

    kubectl delete pod "${SEED_POD}" "${VERIFY_POD}" "${CLEANUP_POD}" \
      "${SNAP_BEFORE_POD}" "${SNAP_AFTER_POD}" "${FLUSH_POD}" \
      -n "${NS}" --ignore-not-found >/dev/null 2>&1 || true

    if [[ "${FLUSH_BETWEEN_RUNS}" == "true" ]]; then
      echo "[${policy}/${i}] Flushing existing keys and resetting stats..."
      flush_cluster "${FLUSH_POD}"
    fi

    echo "[${policy}/${i}] Capturing before snapshot..."
    snapshot_cluster "${SNAP_BEFORE_POD}" "before" "${LOCAL_OUT}/${BEFORE_FILE}"

    echo "[${policy}/${i}] Writing ${TARGET_MB} MB of 1KB values..."
    SEED_START="$(date +%s)"
    run_seed "${SEED_POD}" "${RUN_ID}" "${SEED_REPORT}"
    SEED_END="$(date +%s)"

    echo "[${policy}/${i}] Capturing after snapshot..."
    snapshot_cluster "${SNAP_AFTER_POD}" "after" "${LOCAL_OUT}/${AFTER_FILE}"

    echo "[${policy}/${i}] Verifying sample of accepted writes..."
    run_verify "${VERIFY_POD}" "${SEED_REPORT}" "${VERIFY_REPORT}"

    before_evicted="$(sum_json_field "${LOCAL_OUT}/${BEFORE_FILE}" evicted_keys)"
    after_evicted="$(sum_json_field "${LOCAL_OUT}/${AFTER_FILE}" evicted_keys)"
    before_errors="$(sum_json_field "${LOCAL_OUT}/${BEFORE_FILE}" total_error_replies)"
    after_errors="$(sum_json_field "${LOCAL_OUT}/${AFTER_FILE}" total_error_replies)"
    before_used="$(sum_json_field "${LOCAL_OUT}/${BEFORE_FILE}" used_memory)"
    after_used="$(sum_json_field "${LOCAL_OUT}/${AFTER_FILE}" used_memory)"
    before_keys="$(sum_json_field "${LOCAL_OUT}/${BEFORE_FILE}" dbsize)"
    after_keys="$(sum_json_field "${LOCAL_OUT}/${AFTER_FILE}" dbsize)"
    observed_policy="$(first_json_field "${LOCAL_OUT}/${AFTER_FILE}" maxmemory_policy)"
    maxmemory_bytes="$(sum_json_field "${LOCAL_OUT}/${AFTER_FILE}" maxmemory)"
    seed_keys="$("${PYTHON_BIN}" -c "import json; print(json.load(open('${LOCAL_OUT}/${SEED_REPORT}')).get('written_keys', 0))")"
    target_keys="$("${PYTHON_BIN}" -c "import json; print(json.load(open('${LOCAL_OUT}/${SEED_REPORT}')).get('target_keys', 0))")"
    seed_duration="$("${PYTHON_BIN}" -c "import json; print(json.load(open('${LOCAL_OUT}/${SEED_REPORT}')).get('seed_duration_s', 0))")"
    seed_completed="$("${PYTHON_BIN}" -c "import json; print(str(json.load(open('${LOCAL_OUT}/${SEED_REPORT}')).get('completed', False)).lower())")"
    pipeline_errors="$("${PYTHON_BIN}" -c "import json; print(json.load(open('${LOCAL_OUT}/${SEED_REPORT}')).get('errors', 0))")"
    write_errors="$("${PYTHON_BIN}" -c "import json; print(json.load(open('${LOCAL_OUT}/${SEED_REPORT}')).get('write_errors', 0))")"
    oom_errors="$("${PYTHON_BIN}" -c "import json; print(json.load(open('${LOCAL_OUT}/${SEED_REPORT}')).get('oom_errors', 0))")"
    sample_size="$("${PYTHON_BIN}" -c "import json; print(json.load(open('${LOCAL_OUT}/${VERIFY_REPORT}')).get('sample_size', 0))")"
    keys_missing="$("${PYTHON_BIN}" -c "import json; print(json.load(open('${LOCAL_OUT}/${VERIFY_REPORT}')).get('keys_missing', 0))")"
    verify_errors="$("${PYTHON_BIN}" -c "import json; print(json.load(open('${LOCAL_OUT}/${VERIFY_REPORT}')).get('verify_errors', 0))")"

    cat > "${LOCAL_OUT}/${SUMMARY_FILE}" <<EOF
{
  "run": ${i},
  "provider": $(json_string "${PROVIDER}"),
  "policy": $(json_string "${policy}"),
  "observed_policy": $(json_string "${observed_policy}"),
  "run_id": $(json_string "${RUN_ID}"),
  "target_mb": ${TARGET_MB},
  "ttl_seconds": ${KEY_TTL_SECONDS},
  "seed_start_s": ${SEED_START},
  "seed_end_s": ${SEED_END},
  "seed_wall_duration_s": $((SEED_END - SEED_START)),
  "seed_duration_s": ${seed_duration},
  "target_keys": ${target_keys},
  "written_keys": ${seed_keys},
  "seed_completed": ${seed_completed},
  "pipeline_errors": ${pipeline_errors},
  "write_errors": ${write_errors},
  "oom_errors": ${oom_errors},
  "maxmemory_total_bytes": ${maxmemory_bytes},
  "used_memory_before": ${before_used},
  "used_memory_after": ${after_used},
  "dbsize_before": ${before_keys},
  "dbsize_after": ${after_keys},
  "evicted_keys_before": ${before_evicted},
  "evicted_keys_after": ${after_evicted},
  "evicted_keys_delta": $((after_evicted - before_evicted)),
  "error_replies_before": ${before_errors},
  "error_replies_after": ${after_errors},
  "error_replies_delta": $((after_errors - before_errors)),
  "sample_size": ${sample_size},
  "sample_missing": ${keys_missing},
  "sample_missing_rate": $("${PYTHON_BIN}" -c "print(${keys_missing} / ${sample_size} if ${sample_size} else 0.0)"),
  "verify_errors": ${verify_errors}
}
EOF

    echo "[${policy}/${i}] Summary saved: ${LOCAL_OUT}/${SUMMARY_FILE}"
    echo "[${policy}/${i}] Written keys: ${seed_keys}/${target_keys}"
    echo "[${policy}/${i}] Evicted keys delta: $((after_evicted - before_evicted))"
    echo "[${policy}/${i}] OOM/write errors: ${oom_errors}/${write_errors}"
    echo "[${policy}/${i}] Sample missing: ${keys_missing}/${sample_size}"

    echo "[${policy}/${i}] Cleaning up surviving test keys..."
    run_cleanup "${CLEANUP_POD}" "${SEED_REPORT}"
  done
done

echo ""
echo "=========================================="
echo "  All maxmemory runs complete."
echo "  Results in: ${LOCAL_OUT}/"
echo "  Analyse with:"
echo "    python cli.py maxmemory --input ${LOCAL_OUT} --output-dir ./plots/maxmemory"
echo "=========================================="

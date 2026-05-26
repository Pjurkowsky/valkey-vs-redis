#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<EOF
Usage: $0 <memorystore-cluster-id> [output-dir]

Seeds a Memorystore for Redis Cluster instance, creates a managed backup, restores
that backup into a new cluster, verifies sampled data, and records timing JSON.

Defaults:
  LOCATION=europe-central2
  N=1
  DATASET_MB=12288
  NODE_TYPE=redis-standard-small
  SHARD_COUNT=<source shardCount>
  REPLICA_COUNT=<source replicaCount>
  CLEANUP_RESTORE_CLUSTER=true
  FLUSH_SOURCE_BEFORE_SEED=true
  REDIS_CLI_IMAGE=docker.io/redis:7.2
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" || -z "${1:-}" ]]; then
  usage
  exit 0
fi

SOURCE_CLUSTER_ID="$1"
LOCAL_OUT="${2:-./results/memorystore_backup_restore}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

source "${SCRIPT_DIR}/pod_results.sh"

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null)}"
LOCATION="${LOCATION:-europe-central2}"
NS="${NS:-vk}"
ARTIFACT_REPO="${ARTIFACT_REPO:-valkey-bench}"
IMAGE="${BACKUP_IMAGE:-${LOCATION}-docker.pkg.dev/${PROJECT_ID}/${ARTIFACT_REPO}/backup_restore:1}"
REDIS_CLI_IMAGE="${REDIS_CLI_IMAGE:-docker.io/redis:7.2}"
REMOTE_OUT="/work/results/memorystore_backup_restore"

N="${N:-1}"
DATASET_MB="${DATASET_MB:-12288}"
PORT="${MEMORYSTORE_PORT:-6379}"
RESTORE_CLUSTER_PREFIX="${RESTORE_CLUSTER_PREFIX:-${SOURCE_CLUSTER_ID}-restore}"
CLEANUP_RESTORE_CLUSTER="${CLEANUP_RESTORE_CLUSTER:-true}"
FLUSH_SOURCE_BEFORE_SEED="${FLUSH_SOURCE_BEFORE_SEED:-true}"
RESTORE_TIMEOUT_SECONDS="${RESTORE_TIMEOUT_SECONDS:-3600}"
BACKUP_TIMEOUT_SECONDS="${BACKUP_TIMEOUT_SECONDS:-3600}"

mkdir -p "${LOCAL_OUT}"

if [[ -z "${PROJECT_ID}" || "${PROJECT_ID}" == "(unset)" ]]; then
  echo "ERROR: Could not determine GCP project. Run 'gcloud config set project <project-id>' or set PROJECT_ID." >&2
  exit 1
fi

PYTHON_BIN="$(command -v python3 || command -v python || true)"
if [[ -z "${PYTHON_BIN}" ]]; then
  echo "ERROR: python3 or python is required to parse gcloud JSON output." >&2
  exit 1
fi

json_string() {
  "${PYTHON_BIN}" -c 'import json, sys; print(json.dumps(sys.argv[1]))' "$1"
}

describe_cluster_json() {
  local cluster_id="$1"
  gcloud redis clusters describe "${cluster_id}" \
    --project="${PROJECT_ID}" \
    --region="${LOCATION}" \
    --format=json
}

cluster_field() {
  local cluster_id="$1"
  local field="$2"
  describe_cluster_json "${cluster_id}" | "${PYTHON_BIN}" -c "
import json, sys
doc = json.load(sys.stdin)
value = doc.get('${field}', '')
print('' if value is None else value)
"
}

current_cluster_state() {
  cluster_field "$1" "state"
}

state_is_ready() {
  local state="$1"
  case "${state}" in
    ""|ACTIVE|READY)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

wait_for_cluster_ready() {
  local cluster_id="$1"
  local timeout="${2:-1800}"
  local deadline=$((SECONDS + timeout))
  local state

  echo "  Waiting for ${cluster_id} ready state (max ${timeout}s)..."
  while (( SECONDS < deadline )); do
    state="$(current_cluster_state "${cluster_id}" || true)"
    if state_is_ready "${state}"; then
      echo "  ${cluster_id} ready; state=${state:-unknown}"
      return 0
    fi
    echo "  ${cluster_id} state=${state:-unknown}; waiting..."
    sleep 15
  done

  echo "ERROR: ${cluster_id} did not become ready within ${timeout}s" >&2
  return 1
}

discover_endpoint() {
  local cluster_id="$1"
  describe_cluster_json "${cluster_id}" | "${PYTHON_BIN}" -c '
import json, sys

doc = json.load(sys.stdin)

def emit(address, port):
    if address:
        print(f"{address} {port or 6379}")
        return True
    return False

for endpoint in doc.get("discoveryEndpoints") or []:
    if emit(endpoint.get("address"), endpoint.get("port")):
        raise SystemExit(0)

raise SystemExit("Could not find Memorystore discovery endpoint address in cluster description")
'
}

source_network() {
  describe_cluster_json "${SOURCE_CLUSTER_ID}" | "${PYTHON_BIN}" -c '
import json, sys
doc = json.load(sys.stdin)
network = doc.get("network", "")
if isinstance(network, str) and network:
    print(network)
    raise SystemExit(0)
for endpoint in doc.get("discoveryEndpoints") or []:
    psc_config = endpoint.get("pscConfig") or {}
    network = psc_config.get("network", "")
    if network:
        print(network)
        raise SystemExit(0)
for connection in doc.get("pscConnections") or []:
    network = connection.get("network", "")
    if network:
        print(network)
        raise SystemExit(0)
'
}

source_node_type() {
  cluster_field "${SOURCE_CLUSTER_ID}" "nodeType" | "${PYTHON_BIN}" -c '
import sys
value = sys.stdin.read().strip()
if value.startswith("REDIS_"):
    value = value.lower().replace("_", "-")
print(value)
'
}

source_shard_count() {
  cluster_field "${SOURCE_CLUSTER_ID}" "shardCount"
}

source_replica_count() {
  cluster_field "${SOURCE_CLUSTER_ID}" "replicaCount"
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

run_seed_pod() {
  local run_idx="$1"
  local host="$2"
  local port="$3"
  local run_id="$4"
  local report_file="$5"
  local pod_name="ms-backup-seed-${run_idx}"

  kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl run "${pod_name}" -n "${NS}" \
    --image="${IMAGE}" \
    --restart=Never \
    --command -- \
    /bin/sh -c "
      mkdir -p '${REMOTE_OUT}'
      python /work/backup_restore_seed.py \
        --mode seed \
        --host '${host}' --port '${port}' \
        --target-mb '${DATASET_MB}' \
        --run-id '${run_id}' \
        --output '${REMOTE_OUT}/${report_file}'
      status=\$?
      echo \"\$status\" > '${POD_EXIT_CODE_FILE}'
      touch '${POD_DONE_FILE}'
      sleep '${POD_HOLD_SECONDS}'
    "

  wait_for_command_pod "${pod_name}" 14400
  kubectl cp "${NS}/${pod_name}:${REMOTE_OUT}/${report_file}" "${LOCAL_OUT}/${report_file}"
  kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found >/dev/null
}

run_flush_source_pod() {
  local run_idx="$1"
  local host="$2"
  local port="$3"
  local pod_name="ms-backup-flush-${run_idx}"

  kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl run "${pod_name}" -n "${NS}" \
    --image="${REDIS_CLI_IMAGE}" \
    --restart=Never \
    --command -- \
    /bin/sh -c "
      status=0
      nodes=\$(redis-cli -h '${host}' -p '${port}' cluster nodes 2>&1) || status=\$?
      if [ \"\${status}\" -eq 0 ]; then
        masters=\$(printf '%s\n' \"\${nodes}\" | awk '\$3 ~ /master/ && \$3 !~ /fail/ {print \$2}')
        if [ -z \"\${masters}\" ]; then
          echo 'ERROR: no master nodes discovered from CLUSTER NODES' >&2
          status=1
        else
          for endpoint in \${masters}; do
            addr=\${endpoint%%@*}
            node_host=\${addr%:*}
            node_port=\${addr##*:}
            echo \"FLUSHALL \${node_host}:\${node_port}\"
            redis-cli -h \"\${node_host}\" -p \"\${node_port}\" flushall sync || redis-cli -h \"\${node_host}\" -p \"\${node_port}\" flushall || status=1
            redis-cli -h \"\${node_host}\" -p \"\${node_port}\" config resetstat >/dev/null 2>&1 || true
            dbsize=unknown
            for attempt in \$(seq 1 60); do
              dbsize=\$(redis-cli -h \"\${node_host}\" -p \"\${node_port}\" dbsize 2>/dev/null || echo unknown)
              if [ \"\${dbsize}\" = '0' ]; then
                break
              fi
              sleep 1
            done
            if [ \"\${dbsize}\" != '0' ]; then
              echo \"ERROR: \${node_host}:\${node_port} still has \${dbsize} keys after FLUSHALL\" >&2
              status=1
            fi
          done
        fi
      else
        printf '%s\n' \"\${nodes}\" >&2
      fi
      echo \"\${status}\" > '${POD_EXIT_CODE_FILE}'
      touch '${POD_DONE_FILE}'
      sleep '${POD_HOLD_SECONDS}'
      exit \"\${status}\"
    "

  wait_for_command_pod "${pod_name}" 1800
  kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found >/dev/null
}

run_verify_pod() {
  local run_idx="$1"
  local host="$2"
  local port="$3"
  local seed_report="$4"
  local verify_report="$5"
  local pod_name="ms-backup-verify-${run_idx}"

  kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl run "${pod_name}" -n "${NS}" \
    --image="${IMAGE}" \
    --restart=Never \
    --command -- \
    /bin/sh -c "
      mkdir -p '${REMOTE_OUT}'
      while [ ! -f '${REMOTE_OUT}/${seed_report}' ]; do sleep 1; done
      python /work/backup_restore_seed.py \
        --mode verify \
        --host '${host}' --port '${port}' \
        --seed-report '${REMOTE_OUT}/${seed_report}' \
        --output '${REMOTE_OUT}/${verify_report}'
      status=\$?
      echo \"\$status\" > '${POD_EXIT_CODE_FILE}'
      touch '${POD_DONE_FILE}'
      sleep '${POD_HOLD_SECONDS}'
    "

  wait_for_pod_ready "${pod_name}" 120
  kubectl cp "${LOCAL_OUT}/${seed_report}" "${NS}/${pod_name}:${REMOTE_OUT}/${seed_report}"
  wait_for_command_pod "${pod_name}" 3600
  kubectl cp "${NS}/${pod_name}:${REMOTE_OUT}/${verify_report}" "${LOCAL_OUT}/${verify_report}"
  kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found >/dev/null
}

create_backup() {
  local backup_id="$1"
  local log_file="$2"

  gcloud redis clusters create-backup "${SOURCE_CLUSTER_ID}" \
    --project="${PROJECT_ID}" \
    --region="${LOCATION}" \
    --backup-id="${backup_id}" \
    >"${log_file}" 2>&1
}

find_backup_resource() {
  local backup_id="$1"

  gcloud redis clusters backup-collections list \
    --project="${PROJECT_ID}" \
    --region="${LOCATION}" \
    --format=json | "${PYTHON_BIN}" -c "
import json, subprocess, sys
collections = json.load(sys.stdin)
backup_id = '${backup_id}'
project = '${PROJECT_ID}'
region = '${LOCATION}'
for coll in collections:
    name = coll.get('name', '')
    if not name:
        continue
    coll_id = name.rsplit('/', 1)[-1]
    out = subprocess.check_output([
        'gcloud', 'redis', 'clusters', 'backups', 'list',
        '--project', project,
        '--region', region,
        '--backup-collection', coll_id,
        '--format=json',
    ], text=True)
    for backup in json.loads(out):
        bname = backup.get('name', '')
        if bname.endswith('/backups/' + backup_id):
            print(bname)
            raise SystemExit(0)
raise SystemExit(1)
"
}

create_restore_cluster() {
  local restore_cluster_id="$1"
  local backup_resource="$2"
  local log_file="$3"
  local network="$4"
  local node_type="$5"
  local shard_count="$6"
  local replica_count="$7"

  gcloud redis clusters create "${restore_cluster_id}" \
    --project="${PROJECT_ID}" \
    --region="${LOCATION}" \
    --network="${network}" \
    --replica-count="${replica_count}" \
    --node-type="${node_type}" \
    --shard-count="${shard_count}" \
    --import-managed-backup="${backup_resource}" \
    >"${log_file}" 2>&1
}

delete_restore_cluster() {
  local restore_cluster_id="$1"
  gcloud redis clusters delete "${restore_cluster_id}" \
    --project="${PROJECT_ID}" \
    --region="${LOCATION}" \
    --quiet >/dev/null 2>&1 || true
}

echo "==> Memorystore backup/restore benchmark"
echo "PROJECT_ID=${PROJECT_ID}"
echo "LOCATION=${LOCATION}"
echo "SOURCE_CLUSTER_ID=${SOURCE_CLUSTER_ID}"
echo "BACKUP_IMAGE=${IMAGE}"
echo "REDIS_CLI_IMAGE=${REDIS_CLI_IMAGE}"
echo "DATASET_MB=${DATASET_MB}"
echo "N=${N}"
echo "FLUSH_SOURCE_BEFORE_SEED=${FLUSH_SOURCE_BEFORE_SEED}"

wait_for_cluster_ready "${SOURCE_CLUSTER_ID}" 900
read -r SOURCE_HOST SOURCE_PORT < <(discover_endpoint "${SOURCE_CLUSTER_ID}")
if [[ -n "${SOURCE_PORT:-}" ]]; then
  PORT="${SOURCE_PORT}"
fi

NETWORK="${NETWORK:-$(source_network)}"
NODE_TYPE="${NODE_TYPE:-$(source_node_type)}"
SHARD_COUNT="${SHARD_COUNT:-$(source_shard_count)}"
REPLICA_COUNT="${REPLICA_COUNT:-$(source_replica_count)}"

if [[ -z "${NETWORK}" || -z "${NODE_TYPE}" || -z "${SHARD_COUNT}" || -z "${REPLICA_COUNT}" ]]; then
  echo "ERROR: Could not infer network/nodeType/shardCount/replicaCount from ${SOURCE_CLUSTER_ID}." >&2
  exit 1
fi

echo "SOURCE_ENDPOINT=${SOURCE_HOST}:${PORT}"
echo "NETWORK=${NETWORK}"
echo "NODE_TYPE=${NODE_TYPE}"
echo "SHARD_COUNT=${SHARD_COUNT}"
echo "REPLICA_COUNT=${REPLICA_COUNT}"

for i in $(seq 1 "${N}"); do
  echo ""
  echo "=========================================="
  echo "  Memorystore backup/restore run ${i}/${N}"
  echo "=========================================="

  RUN_ID="ms_backup_${DATASET_MB}mb_${i}_$(date +%s)"
  BACKUP_ID="${SOURCE_CLUSTER_ID}-backup-${i}-$(date +%Y%m%d-%H%M%S)"
  RESTORE_CLUSTER_ID="${RESTORE_CLUSTER_PREFIX}-${i}-$(date +%H%M%S)"
  SEED_REPORT="seed_report_${DATASET_MB}_${i}.json"
  VERIFY_REPORT="verify_report_${DATASET_MB}_${i}.json"
  TIMING_FILE="memorystore_backup_timing_${DATASET_MB}_${i}.json"
  BACKUP_LOG="${LOCAL_OUT}/memorystore_backup_${i}.log"
  RESTORE_LOG="${LOCAL_OUT}/memorystore_restore_${i}.log"

  FLUSH_START=0
  FLUSH_END=0
  if [[ "${FLUSH_SOURCE_BEFORE_SEED}" == "true" ]]; then
    echo "[${i}] Flushing source cluster ${SOURCE_CLUSTER_ID} before seeding..."
    FLUSH_START="$(date +%s)"
    run_flush_source_pod "${i}" "${SOURCE_HOST}" "${PORT}"
    FLUSH_END="$(date +%s)"
  fi

  echo "[${i}] Seeding ${DATASET_MB} MB into ${SOURCE_CLUSTER_ID}..."
  SEED_START="$(date +%s)"
  run_seed_pod "${i}" "${SOURCE_HOST}" "${PORT}" "${RUN_ID}" "${SEED_REPORT}"
  SEED_END="$(date +%s)"

  echo "[${i}] Creating managed backup ${BACKUP_ID}..."
  BACKUP_START="$(date +%s)"
  create_backup "${BACKUP_ID}" "${BACKUP_LOG}"
  BACKUP_END="$(date +%s)"
  BACKUP_RESOURCE="$(find_backup_resource "${BACKUP_ID}")"
  echo "[${i}] Backup resource: ${BACKUP_RESOURCE}"

  echo "[${i}] Restoring backup into ${RESTORE_CLUSTER_ID}..."
  RESTORE_START="$(date +%s)"
  create_restore_cluster "${RESTORE_CLUSTER_ID}" "${BACKUP_RESOURCE}" "${RESTORE_LOG}" \
    "${NETWORK}" "${NODE_TYPE}" "${SHARD_COUNT}" "${REPLICA_COUNT}"
  wait_for_cluster_ready "${RESTORE_CLUSTER_ID}" "${RESTORE_TIMEOUT_SECONDS}"
  RESTORE_END="$(date +%s)"

  read -r RESTORE_HOST RESTORE_PORT < <(discover_endpoint "${RESTORE_CLUSTER_ID}")
  if [[ -z "${RESTORE_PORT:-}" ]]; then
    RESTORE_PORT="${PORT}"
  fi

  echo "[${i}] Verifying restored data on ${RESTORE_CLUSTER_ID}..."
  VERIFY_START="$(date +%s)"
  run_verify_pod "${i}" "${RESTORE_HOST}" "${RESTORE_PORT}" "${SEED_REPORT}" "${VERIFY_REPORT}"
  VERIFY_END="$(date +%s)"

  SEED_KEYS="$(python3 -c "import json; print(json.load(open('${LOCAL_OUT}/${SEED_REPORT}'))['written_keys'])" 2>/dev/null || echo 0)"
  SEED_DUR="$(python3 -c "import json; print(json.load(open('${LOCAL_OUT}/${SEED_REPORT}'))['seed_duration_s'])" 2>/dev/null || echo 0)"
  INTEGRITY="$(python3 -c "import json; print(json.dumps(bool(json.load(open('${LOCAL_OUT}/${VERIFY_REPORT}'))['integrity_ok'])))" 2>/dev/null || echo false)"

  cat > "${LOCAL_OUT}/${TIMING_FILE}" <<EOF
{
  "run": ${i},
  "provider": "memorystore_redis",
  "source_cluster": $(json_string "${SOURCE_CLUSTER_ID}"),
  "restore_cluster": $(json_string "${RESTORE_CLUSTER_ID}"),
  "location": $(json_string "${LOCATION}"),
  "dataset_mb": ${DATASET_MB},
  "source_flush_start": ${FLUSH_START},
  "source_flush_end": ${FLUSH_END},
  "source_flush_duration_s": $((FLUSH_END - FLUSH_START)),
  "seed_keys": ${SEED_KEYS},
  "seed_duration_s": ${SEED_DUR},
  "seed_wall_duration_s": $((SEED_END - SEED_START)),
  "backup_id": $(json_string "${BACKUP_ID}"),
  "backup_resource": $(json_string "${BACKUP_RESOURCE}"),
  "backup_start": ${BACKUP_START},
  "backup_end": ${BACKUP_END},
  "backup_duration_s": $((BACKUP_END - BACKUP_START)),
  "restore_start": ${RESTORE_START},
  "restore_end": ${RESTORE_END},
  "restore_duration_s": $((RESTORE_END - RESTORE_START)),
  "verify_start": ${VERIFY_START},
  "verify_end": ${VERIFY_END},
  "verify_duration_s": $((VERIFY_END - VERIFY_START)),
  "integrity_ok": ${INTEGRITY}
}
EOF

  echo "[${i}] Timing saved: ${LOCAL_OUT}/${TIMING_FILE}"

  if [[ "${CLEANUP_RESTORE_CLUSTER}" == "true" ]]; then
    echo "[${i}] Deleting restore cluster ${RESTORE_CLUSTER_ID}..."
    delete_restore_cluster "${RESTORE_CLUSTER_ID}"
  else
    echo "[${i}] Leaving restore cluster ${RESTORE_CLUSTER_ID} in place."
  fi
done

echo ""
echo "=========================================="
echo "  All ${N} Memorystore backup/restore runs complete."
echo "  Results in: ${LOCAL_OUT}/"
echo "=========================================="

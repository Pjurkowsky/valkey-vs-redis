#!/usr/bin/env bash
set -euo pipefail

N="${N:-5}"
NS="${NS:-redis}"
RELEASE="${RELEASE:-redis72}"
STS="${STS:-${RELEASE}-redis-cluster}"
IMAGE="${MEMTIER_IMAGE:-redislabs/memtier_benchmark:latest}"
LOCAL_OUT="${1:-./results/redis72_reshard}"
REMOTE_OUT="/work/results/redis72_reshard"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

source "${SCRIPT_DIR}/pod_results.sh"

HELM_CHART_PATH="${HELM_CHART_PATH:-oci://registry-1.docker.io/bitnamicharts/redis-cluster}"
VALUES_FILE="${VALUES_FILE:-./k8s/manifests/values-redis72.yaml}"

HOST="${REDIS_HOST:-${STS}.${NS}.svc.cluster.local}"
PORT="${REDIS_PORT:-6379}"
ADMIN_POD="${ADMIN_POD:-${STS}-0}"
HEADLESS="${HEADLESS:-${STS}-headless}"
ADMIN_HOST="${ADMIN_HOST:-${ADMIN_POD}.${HEADLESS}.${NS}.svc.cluster.local}"
CLUSTER_ENDPOINT="${ADMIN_HOST}:${PORT}"

THREADS="${MEMTIER_THREADS:-4}"
CLIENTS="${MEMTIER_CLIENTS:-16}"
TEST_TIME="${TEST_TIME:-120}"
KEYS="${MEMTIER_KEYS:-100000}"
DATA_SIZE="${MEMTIER_DATA_SIZE:-1024}"
RATIO="${MEMTIER_RATIO:-1:1}"
STEADY_STATE_WAIT="${STEADY_STATE_WAIT:-30}"
REBALANCE_TIMEOUT="${REBALANCE_TIMEOUT:-240}"

ORIGINAL_SHARDS="${ORIGINAL_SHARDS:-3}"
TARGET_SHARDS="${TARGET_SHARDS:-4}"
REPLICAS_PER_SHARD="${REPLICAS_PER_SHARD:-1}"
ORIGINAL_NODE_COUNT=$((ORIGINAL_SHARDS * (1 + REPLICAS_PER_SHARD)))
TARGET_NODE_COUNT=$((TARGET_SHARDS * (1 + REPLICAS_PER_SHARD)))
NEW_NODE_START="${ORIGINAL_NODE_COUNT}"
NEW_NODE_END=$((TARGET_NODE_COUNT - 1))

CURRENT_MASTER_IDS_SNAPSHOT=""

mkdir -p "${LOCAL_OUT}"

redis_cli() {
  kubectl exec "${ADMIN_POD}" -n "${NS}" -- redis-cli "$@"
}

cluster_info() {
  redis_cli cluster info 2>/dev/null
}

cluster_nodes() {
  redis_cli cluster nodes 2>/dev/null
}

cluster_check() {
  kubectl exec "${ADMIN_POD}" -n "${NS}" -- \
    redis-cli --cluster check "${CLUSTER_ENDPOINT}" 2>&1
}

cluster_info_field() {
  local field="$1"
  cluster_info | awk -F: -v field="${field}" '$1 == field {gsub(/\r/, "", $2); print $2}'
}

cluster_is_healthy_now() {
  local state slots_ok slots_assigned
  state="$(cluster_info_field cluster_state || true)"
  slots_ok="$(cluster_info_field cluster_slots_ok || true)"
  slots_assigned="$(cluster_info_field cluster_slots_assigned || true)"

  [[ "${state}" == "ok" && "${slots_ok}" == "16384" && "${slots_assigned}" == "16384" ]] \
    && cluster_check >/dev/null 2>&1
}

fix_cluster_slots() {
  echo "  Running redis-cli --cluster fix to clear open/importing/migrating slots..."
  kubectl exec "${ADMIN_POD}" -n "${NS}" -- \
    redis-cli --cluster fix "${CLUSTER_ENDPOINT}" \
      --cluster-yes 2>&1
}

wait_cluster_healthy() {
  local max_wait="${1:-300}"
  local waited=0 state slots_ok slots_assigned

  echo "  Waiting for Redis Cluster health (max ${max_wait}s)..."
  while [[ "${waited}" -lt "${max_wait}" ]]; do
    state="$(cluster_info_field cluster_state || true)"
    slots_ok="$(cluster_info_field cluster_slots_ok || true)"
    slots_assigned="$(cluster_info_field cluster_slots_assigned || true)"

    if [[ "${state}" == "ok" && "${slots_ok}" == "16384" && "${slots_assigned}" == "16384" ]]; then
      if cluster_check >/dev/null 2>&1; then
        return 0
      fi
    fi

    sleep 5
    waited=$((waited + 5))
  done

  echo "ERROR: Redis Cluster did not become healthy after ${max_wait}s" >&2
  cluster_info || true
  cluster_check || true
  cluster_nodes || true
  return 1
}

ensure_cluster_clean() {
  local max_wait="${1:-300}"

  if wait_cluster_healthy "${max_wait}"; then
    return 0
  fi

  fix_cluster_slots || true
  sleep 5
  wait_cluster_healthy "${max_wait}"
}

assert_no_chaos_experiments() {
  local active
  active="$(kubectl get podchaos,networkchaos,stresschaos -A --no-headers 2>/dev/null || true)"

  if [[ -n "${active}" ]]; then
    echo "ERROR: Chaos Mesh experiments are present. Remove them before resharding:" >&2
    echo "${active}" >&2
    return 1
  fi
}

helm_upgrade_nodes() {
  local nodes="$1"
  local partition="${2:-0}"

  echo "  Helm upgrade ${RELEASE}: cluster.nodes=${nodes}, rollingUpdate.partition=${partition}"
  if ! helm upgrade "${RELEASE}" "${HELM_CHART_PATH}" \
    -n "${NS}" \
    -f "${VALUES_FILE}" \
    --set "cluster.nodes=${nodes}" \
    --set "cluster.update.addNodes=false" \
    --set "redis.updateStrategy.rollingUpdate.partition=${partition}" \
    --server-side=false \
    --wait=hookOnly; then
    return 1
  fi
}

wait_for_pods_ready() {
  local start="$1"
  local end="$2"
  local ordinal

  for ordinal in $(seq "${start}" "${end}"); do
    kubectl wait "pod/${STS}-${ordinal}" -n "${NS}" \
      --for=condition=Ready \
      --timeout=300s
  done
}

wait_for_pod_ping() {
  local pod="$1"
  local max_wait="${2:-180}"
  local waited=0 phase reply

  echo "  Waiting for ${pod} to answer redis-cli ping (max ${max_wait}s)..."
  while [[ "${waited}" -lt "${max_wait}" ]]; do
    phase="$(kubectl get pod "${pod}" -n "${NS}" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
    if [[ "${phase}" == "Running" ]]; then
      reply="$(kubectl exec "${pod}" -n "${NS}" -- redis-cli ping 2>/dev/null | tr -d '[:space:]' || true)"
      if [[ "${reply}" == "PONG" ]]; then
        return 0
      fi
    fi

    sleep 3
    waited=$((waited + 3))
  done

  echo "ERROR: ${pod} did not answer redis-cli ping after ${max_wait}s" >&2
  kubectl describe pod "${pod}" -n "${NS}" || true
  return 1
}

wait_for_new_pods_ping() {
  local ordinal

  for ordinal in $(seq "${NEW_NODE_START}" "${NEW_NODE_END}"); do
    wait_for_pod_ping "${STS}-${ordinal}" 240
  done
}

wait_for_pods_absent() {
  local start="$1"
  local end="$2"
  local timeout="${3:-300}"
  local ordinal deadline

  for ordinal in $(seq "${start}" "${end}"); do
    deadline=$((SECONDS + timeout))
    while kubectl get "pod/${STS}-${ordinal}" -n "${NS}" >/dev/null 2>&1; do
      if (( SECONDS >= deadline )); then
        echo "ERROR: pod/${STS}-${ordinal} still exists after ${timeout}s" >&2
        return 1
      fi
      sleep 2
    done
  done
}

extra_pods_present() {
  local ordinal

  for ordinal in $(seq "${NEW_NODE_START}" "${NEW_NODE_END}"); do
    if kubectl get "pod/${STS}-${ordinal}" -n "${NS}" >/dev/null 2>&1; then
      return 0
    fi
  done

  return 1
}

pod_ip() {
  local pod="$1"
  kubectl get pod "${pod}" -n "${NS}" -o jsonpath='{.status.podIP}' 2>/dev/null
}

node_id_for_address() {
  local address="$1"
  cluster_nodes | awk -v address="${address}" '
    $2 ~ "^" address ":6379@" {
      print $1
      exit
    }
  '
}

node_id_for_pod() {
  local pod="$1"
  local ip fqdn node_id

  ip="$(pod_ip "${pod}")"
  if [[ -n "${ip}" ]]; then
    node_id="$(node_id_for_address "${ip}")"
    if [[ -n "${node_id}" ]]; then
      echo "${node_id}"
      return 0
    fi
  fi

  fqdn="${pod}.${HEADLESS}.${NS}.svc.cluster.local"
  node_id="$(node_id_for_address "${fqdn}")"
  if [[ -n "${node_id}" ]]; then
    echo "${node_id}"
  fi
}

node_flags_for_pod() {
  local pod="$1"
  local node_id
  node_id="$(node_id_for_pod "${pod}" || true)"
  if [[ -z "${node_id}" ]]; then
    return 0
  fi

  cluster_nodes | awk -v node_id="${node_id}" '$1 == node_id {print $3; exit}'
}

slot_count_for_node_id() {
  local node_id="$1"
  cluster_nodes | awk -v node_id="${node_id}" '
    $1 == node_id {
      for (i = 9; i <= NF; i++) {
        if ($i ~ /^[0-9]+-[0-9]+$/) {
          split($i, range, "-")
          total += range[2] - range[1] + 1
        } else if ($i ~ /^[0-9]+$/) {
          total += 1
        }
      }
      print total + 0
      found = 1
    }
    END {
      if (!found) {
        print 0
      }
    }
  '
}

original_master_ids() {
  local ordinal pod flags node_id

  if [[ -n "${CURRENT_MASTER_IDS_SNAPSHOT}" ]]; then
    printf "%s\n" ${CURRENT_MASTER_IDS_SNAPSHOT}
    return 0
  fi

  for ordinal in $(seq 0 $((ORIGINAL_NODE_COUNT - 1))); do
    pod="${STS}-${ordinal}"
    flags="$(node_flags_for_pod "${pod}" || true)"
    if [[ "${flags}" == *master* ]]; then
      node_id="$(node_id_for_pod "${pod}" || true)"
      if [[ -n "${node_id}" ]]; then
        echo "${node_id}"
      fi
    fi
  done
}

json_number_or_null() {
  local value="${1:-0}"
  if [[ "${value}" =~ ^[0-9]+$ && "${value}" -gt 0 ]]; then
    echo "${value}"
  else
    echo "null"
  fi
}

relative_or_null() {
  local value="${1:-0}"
  local base="$2"
  if [[ "${value}" =~ ^[0-9]+$ && "${value}" -gt 0 ]]; then
    echo "$((value - base))"
  else
    echo "null"
  fi
}

rebalance_weight_args() {
  local extra_master_id="$1"
  local extra_weight="$2"
  local master_id

  for master_id in $(original_master_ids); do
    printf "%s=1\n" "${master_id}"
  done
  printf "%s=%s\n" "${extra_master_id}" "${extra_weight}"
}

weighted_rebalance_to_extra_master() {
  local new_master_id="$1"
  local weight_args=()
  local weight

  mapfile -t weight_args < <(rebalance_weight_args "${new_master_id}" 1)
  if [[ "${#weight_args[@]}" -lt 2 ]]; then
    echo "ERROR: Not enough masters available for weighted rebalance." >&2
    return 1
  fi

  echo "  Redis 7.2 rebalance: equal weights across original masters and extra master ${new_master_id}..."
  for weight in "${weight_args[@]}"; do
    echo "    --cluster-weight ${weight}"
  done

  kubectl exec "${ADMIN_POD}" -n "${NS}" -- \
    redis-cli --cluster rebalance "${CLUSTER_ENDPOINT}" \
      --cluster-use-empty-masters \
      --cluster-weight "${weight_args[@]}" \
      --cluster-yes 2>&1
}

weighted_rebalance_off_extra_master() {
  local new_master_id="$1"
  local weight_args=()
  local weight

  mapfile -t weight_args < <(rebalance_weight_args "${new_master_id}" 0)
  if [[ "${#weight_args[@]}" -lt 2 ]]; then
    echo "ERROR: Not enough masters available for weighted scale-in rebalance." >&2
    return 1
  fi

  echo "  Redis 7.2 rebalance: draining extra master ${new_master_id} with weight 0..."
  for weight in "${weight_args[@]}"; do
    echo "    --cluster-weight ${weight}"
  done

  kubectl exec "${ADMIN_POD}" -n "${NS}" -- \
    redis-cli --cluster rebalance "${CLUSTER_ENDPOINT}" \
      --cluster-weight "${weight_args[@]}" \
      --cluster-yes 2>&1
}

extra_master_id() {
  local ordinal pod flags

  for ordinal in $(seq "${NEW_NODE_START}" "${NEW_NODE_END}"); do
    pod="${STS}-${ordinal}"
    flags="$(node_flags_for_pod "${pod}" || true)"
    if [[ "${flags}" == *master* ]]; then
      node_id_for_pod "${pod}"
      return 0
    fi
  done
}

add_new_nodes_to_cluster() {
  local new_master_pod="${STS}-${NEW_NODE_START}"
  local new_replica_pod="${STS}-${NEW_NODE_END}"
  local new_master_host="${new_master_pod}.${HEADLESS}.${NS}.svc.cluster.local"
  local new_replica_host="${new_replica_pod}.${HEADLESS}.${NS}.svc.cluster.local"
  local new_master_id replica_id

  new_master_id="$(node_id_for_pod "${new_master_pod}" || true)"
  if [[ -z "${new_master_id}" ]]; then
    echo "  Adding ${new_master_pod} as an empty Redis Cluster master..."
    kubectl exec "${ADMIN_POD}" -n "${NS}" -- \
      redis-cli --cluster add-node "${new_master_host}:${PORT}" "${CLUSTER_ENDPOINT}" \
        --cluster-yes 2>&1
    sleep 5
    new_master_id="$(node_id_for_pod "${new_master_pod}" || true)"
  fi

  if [[ -z "${new_master_id}" ]]; then
    echo "ERROR: Could not find node id for ${new_master_pod} after add-node." >&2
    cluster_nodes || true
    return 1
  fi

  replica_id="$(node_id_for_pod "${new_replica_pod}" || true)"
  if [[ -z "${replica_id}" ]]; then
    echo "  Adding ${new_replica_pod} as a replica of ${new_master_id}..."
    kubectl exec "${ADMIN_POD}" -n "${NS}" -- \
      redis-cli --cluster add-node "${new_replica_host}:${PORT}" "${CLUSTER_ENDPOINT}" \
        --cluster-slave \
        --cluster-master-id "${new_master_id}" \
        --cluster-yes 2>&1
  fi

  wait_for_known_nodes "${TARGET_NODE_COUNT}" 180
}

wait_for_known_nodes() {
  local expected="$1"
  local max_wait="${2:-180}"
  local waited=0 known

  echo "  Waiting for cluster_known_nodes=${expected} (max ${max_wait}s)..."
  while [[ "${waited}" -lt "${max_wait}" ]]; do
    known="$(cluster_info_field cluster_known_nodes || echo 0)"
    if [[ "${known}" == "${expected}" ]]; then
      return 0
    fi
    sleep 3
    waited=$((waited + 3))
  done

  echo "ERROR: cluster_known_nodes did not reach ${expected}" >&2
  cluster_nodes || true
  return 1
}

move_slots_off_extra_master() {
  local new_master_id="$1"
  local slots_on_new

  slots_on_new="$(slot_count_for_node_id "${new_master_id}")"
  if [[ "${slots_on_new}" -le 0 ]]; then
    echo "  No slots found on extra master ${new_master_id}; skipping slot migration."
    return 0
  fi

  weighted_rebalance_off_extra_master "${new_master_id}"
}

delete_extra_cluster_nodes() {
  local ordinal pod node_id flags

  for ordinal in $(seq "${NEW_NODE_START}" "${NEW_NODE_END}" | sort -rn); do
    pod="${STS}-${ordinal}"
    flags="$(node_flags_for_pod "${pod}" || true)"
    if [[ "${flags}" == *slave* ]]; then
      node_id="$(node_id_for_pod "${pod}" || true)"
      if [[ -n "${node_id}" ]]; then
        echo "  Removing extra replica ${pod} (${node_id})..."
        kubectl exec "${ADMIN_POD}" -n "${NS}" -- \
          redis-cli --cluster del-node "${CLUSTER_ENDPOINT}" "${node_id}" 2>&1 || true
        sleep 3
      fi
    fi
  done

  for ordinal in $(seq "${NEW_NODE_START}" "${NEW_NODE_END}" | sort -rn); do
    pod="${STS}-${ordinal}"
    flags="$(node_flags_for_pod "${pod}" || true)"
    if [[ "${flags}" == *master* ]]; then
      node_id="$(node_id_for_pod "${pod}" || true)"
      if [[ -n "${node_id}" ]]; then
        echo "  Removing extra master ${pod} (${node_id})..."
        kubectl exec "${ADMIN_POD}" -n "${NS}" -- \
          redis-cli --cluster del-node "${CLUSTER_ENDPOINT}" "${node_id}" 2>&1 || true
        sleep 3
      fi
    fi
  done
}

scale_up_to_target_nodes() {
  helm_upgrade_nodes "${TARGET_NODE_COUNT}" "${ORIGINAL_NODE_COUNT}"
  wait_for_new_pods_ping
  add_new_nodes_to_cluster
}

scale_down_to_original_nodes() {
  helm_upgrade_nodes "${ORIGINAL_NODE_COUNT}" "${ORIGINAL_NODE_COUNT}"
  wait_for_pods_absent "${NEW_NODE_START}" "${NEW_NODE_END}" 300
  wait_for_pods_ready 0 $((ORIGINAL_NODE_COUNT - 1))
  ensure_cluster_clean 300
}

reset_rollout_partition() {
  helm_upgrade_nodes "${ORIGINAL_NODE_COUNT}" 0
}

restore_cluster() {
  local current_masters known_nodes new_master_id

  echo "  [restore] Attempting graceful restore to ${ORIGINAL_SHARDS} shards..."

  if wait_cluster_healthy 30; then
    current_masters="$(cluster_nodes | awk '/master/ && !/fail/ {count++} END {print count + 0}')"
    known_nodes="$(cluster_info_field cluster_known_nodes || echo 0)"
    if [[ "${current_masters}" -eq "${ORIGINAL_SHARDS}" && "${known_nodes:-0}" -le "${ORIGINAL_NODE_COUNT}" ]] && ! extra_pods_present; then
      reset_rollout_partition
      echo "  [restore] Redis Cluster is already at ${ORIGINAL_SHARDS} healthy shards."
      return 0
    fi
  fi

  if extra_pods_present; then
    ensure_cluster_clean 120 || true
    new_master_id="$(extra_master_id || true)"
    if [[ -n "${new_master_id}" ]]; then
      move_slots_off_extra_master "${new_master_id}" || true
      ensure_cluster_clean 180 || true
    fi
    delete_extra_cluster_nodes
  fi

  echo "  [restore] Scaling Helm release back to ${ORIGINAL_NODE_COUNT} Redis nodes..."
  scale_down_to_original_nodes
  reset_rollout_partition
  echo "  [restore] Redis Cluster restored."
}

start_memtier_pod() {
  local pod_name="$1"
  local out_file="$2"

  kubectl delete pod "${pod_name}" -n "${NS}" --ignore-not-found 2>/dev/null || true

  kubectl run "${pod_name}" -n "${NS}" \
    --image="${IMAGE}" \
    --restart=Never \
    --command -- \
    /bin/sh -c "
      mkdir -p '${REMOTE_OUT}'
      memtier_benchmark \
        --server='${HOST}' --port='${PORT}' \
        --protocol=redis \
        --cluster-mode \
        --threads='${THREADS}' --clients='${CLIENTS}' \
        --test-time='${TEST_TIME}' \
        --key-maximum='${KEYS}' \
        --data-size='${DATA_SIZE}' \
        --ratio='${RATIO}' \
        --json-out-file '${REMOTE_OUT}/${out_file}' \
        --run-count 1 \
        --print-percentiles='50,95,99,99.9'
      status=\$?
      echo \"\$status\" > '${POD_EXIT_CODE_FILE}'
      touch '${POD_DONE_FILE}'
      sleep '${POD_HOLD_SECONDS}'
    "

  kubectl wait pod/"${pod_name}" -n "${NS}" \
    --for=condition=Ready --timeout=60s 2>/dev/null || true
}

finish_memtier_pod() {
  local pod_name="$1"
  local out_file="$2"

  if ! wait_for_pod_marker "${NS}" "${pod_name}" "${POD_DONE_FILE}" 600; then
    echo "ERROR: memtier pod ${pod_name} did not signal completion." >&2
    print_pod_debug_info "${NS}" "${pod_name}"
    return 1
  fi

  local exit_code
  exit_code="$(read_pod_exit_code "${NS}" "${pod_name}" "${POD_EXIT_CODE_FILE}")"
  if [[ -z "${exit_code}" || "${exit_code}" != "0" ]]; then
    echo "ERROR: memtier pod ${pod_name} exited with code ${exit_code:-unknown}." >&2
    print_pod_debug_info "${NS}" "${pod_name}"
    return 1
  fi

  kubectl cp "${NS}/${pod_name}:${REMOTE_OUT}/${out_file}" "${LOCAL_OUT}/${out_file}"
}

echo "=========================================="
echo "  Redis 7.2 reshard benchmark"
echo "=========================================="
echo "Namespace: ${NS}"
echo "Release: ${RELEASE}"
echo "StatefulSet: ${STS}"
echo "Host: ${HOST}:${PORT}"
echo "Admin endpoint: ${CLUSTER_ENDPOINT}"
echo "Memtier image: ${IMAGE}"
echo "Runs: ${N}"
echo "Nodes: ${ORIGINAL_NODE_COUNT} -> ${TARGET_NODE_COUNT} -> ${ORIGINAL_NODE_COUNT}"

for i in $(seq 1 "${N}"); do
  POD_NAME="memtier-reshard-redis72-${i}"
  OUT_FILE="reshard_redis72_run_${i}.json"
  TIMING_FILE="reshard_redis72_timing_${i}.json"

  echo ""
  echo "=========================================="
  echo "  Redis reshard run ${i}/${N}"
  echo "=========================================="

  kubectl delete pod "${POD_NAME}" -n "${NS}" --ignore-not-found 2>/dev/null || true

  echo "[${i}] Checking for active Chaos Mesh experiments..."
  assert_no_chaos_experiments

  echo "[${i}] Verifying cluster is at ${ORIGINAL_SHARDS} shards..."
  if ! ensure_cluster_clean 60; then
    restore_cluster
    ensure_cluster_clean 300
  fi

  CURRENT_MASTERS="$(cluster_nodes | awk '/master/ && !/fail/ {count++} END {print count + 0}')"
  echo "[${i}] Current masters: ${CURRENT_MASTERS}"
  if [[ "${CURRENT_MASTERS}" -ne "${ORIGINAL_SHARDS}" ]]; then
    echo "[${i}] Expected ${ORIGINAL_SHARDS} healthy masters before reshard, found ${CURRENT_MASTERS}; attempting restore..."
    restore_cluster
    CURRENT_MASTERS="$(cluster_nodes | awk '/master/ && !/fail/ {count++} END {print count + 0}')"
    if [[ "${CURRENT_MASTERS}" -ne "${ORIGINAL_SHARDS}" ]]; then
      echo "[${i}] ERROR: Could not restore to ${ORIGINAL_SHARDS} healthy masters." >&2
      cluster_nodes || true
      exit 1
    fi
  fi
  CURRENT_MASTER_IDS_SNAPSHOT="$(original_master_ids | tr '\n' ' ')"

  echo "[${i}] Starting memtier pod for reshard-up (test-time=${TEST_TIME}s)..."
  start_memtier_pod "${POD_NAME}" "${OUT_FILE}"
  MEMTIER_START="$(date +%s)"
  echo "[${i}] Waiting ${STEADY_STATE_WAIT}s for steady state..."
  sleep "${STEADY_STATE_WAIT}"

  echo "[${i}] Scaling from ${ORIGINAL_NODE_COUNT} to ${TARGET_NODE_COUNT} Redis nodes..."
  SCALE_START="$(date +%s)"
  if ! scale_up_to_target_nodes; then
    echo "[${i}] ERROR: Scale-up/add-node failed; attempting restore." >&2
    kubectl delete pod "${POD_NAME}" -n "${NS}" --ignore-not-found
    restore_cluster
    exit 1
  fi
  SCALE_END="$(date +%s)"
  echo "[${i}] Scale-up and add-node took $((SCALE_END - SCALE_START))s"

  EXTRA_MASTER_ID="$(extra_master_id || true)"
  if [[ -z "${EXTRA_MASTER_ID}" ]]; then
    echo "[${i}] ERROR: Could not find the extra master after scale-up; attempting restore." >&2
    kubectl delete pod "${POD_NAME}" -n "${NS}" --ignore-not-found
    restore_cluster
    exit 1
  fi

  EXPECTED_SLOTS_ON_NEW=$((16384 / TARGET_SHARDS))
  REBALANCE_START="$(date +%s)"
  SLOTS_BEFORE="$(slot_count_for_node_id "${EXTRA_MASTER_ID}")"
  echo "[${i}] Moving slots onto extra master ${EXTRA_MASTER_ID}..."
  if ! weighted_rebalance_to_extra_master "${EXTRA_MASTER_ID}"; then
    echo "[${i}] ERROR: Redis rebalance did not complete; attempting restore." >&2
    kubectl delete pod "${POD_NAME}" -n "${NS}" --ignore-not-found
    restore_cluster
    exit 1
  fi
  if ! ensure_cluster_clean "${REBALANCE_TIMEOUT}"; then
    echo "[${i}] ERROR: Cluster unhealthy after rebalance; attempting restore." >&2
    kubectl delete pod "${POD_NAME}" -n "${NS}" --ignore-not-found
    restore_cluster
    exit 1
  fi
  REBALANCE_END="$(date +%s)"
  SLOTS_AFTER="$(slot_count_for_node_id "${EXTRA_MASTER_ID}")"
  NEW_MASTERS="$(cluster_nodes | awk '/master/ && !/fail/ {count++} END {print count + 0}')"
  echo "[${i}] Redis rebalance took $((REBALANCE_END - REBALANCE_START))s; extra master has ${SLOTS_AFTER}/${EXPECTED_SLOTS_ON_NEW} slots."

  echo "[${i}] Waiting for reshard-up memtier to finish..."
  if ! finish_memtier_pod "${POD_NAME}" "${OUT_FILE}"; then
    kubectl delete pod "${POD_NAME}" -n "${NS}" --ignore-not-found
    restore_cluster
    exit 1
  fi

  cat > "${LOCAL_OUT}/${TIMING_FILE}" <<EOF
{
  "run": ${i},
  "phase": "up",
  "provider": "selfhosted_redis",
  "redis_version": "7.2",
  "slot_migration_mode": "redis72",
  "atomic_slot_migration": false,
  "slot_migration_command": "redis_cluster_rebalance",
  "scale_start": ${SCALE_START},
  "scale_end": ${SCALE_END},
  "scale_duration_s": $((SCALE_END - SCALE_START)),
  "scale_start_s": $((SCALE_START - MEMTIER_START)),
  "scale_end_s": $((SCALE_END - MEMTIER_START)),
  "auto_rebalance_detected": false,
  "auto_rebalance_status": "not_used",
  "auto_rebalance_duration_s": 0,
  "explicit_rebalance_used": true,
  "explicit_rebalance_status": "complete",
  "explicit_rebalance_start": ${REBALANCE_START},
  "explicit_rebalance_end": ${REBALANCE_END},
  "explicit_rebalance_duration_s": $((REBALANCE_END - REBALANCE_START)),
  "explicit_rebalance_start_s": $((REBALANCE_START - MEMTIER_START)),
  "explicit_rebalance_end_s": $((REBALANCE_END - MEMTIER_START)),
  "explicit_rebalance_slots_before": ${SLOTS_BEFORE},
  "explicit_rebalance_slots_after": ${SLOTS_AFTER},
  "rebalance_start": ${REBALANCE_START},
  "rebalance_end": ${REBALANCE_END},
  "rebalance_duration_s": $((REBALANCE_END - REBALANCE_START)),
  "rebalance_start_s": $((REBALANCE_START - MEMTIER_START)),
  "rebalance_end_s": $((REBALANCE_END - MEMTIER_START)),
  "operation_start_s": $((SCALE_START - MEMTIER_START)),
  "operation_end_s": $((REBALANCE_END - MEMTIER_START)),
  "operation_duration_s": $((REBALANCE_END - SCALE_START)),
  "memtier_start": ${MEMTIER_START},
  "original_shards": ${ORIGINAL_SHARDS},
  "target_shards": ${TARGET_SHARDS},
  "expected_slots_on_new": ${EXPECTED_SLOTS_ON_NEW},
  "slots_on_new_after": ${SLOTS_AFTER},
  "masters_after": ${NEW_MASTERS}
}
EOF
  echo "[${i}] Timing data saved to ${LOCAL_OUT}/${TIMING_FILE}"

  echo "[${i}] Cleaning up reshard-up memtier pod..."
  kubectl delete pod "${POD_NAME}" -n "${NS}" --ignore-not-found

  DOWN_POD_NAME="memtier-reshard-redis72-down-${i}"
  DOWN_OUT_FILE="reshard_redis72_down_run_${i}.json"
  DOWN_TIMING_FILE="reshard_redis72_down_timing_${i}.json"

  echo "[${i}] Starting memtier pod for reshard-down (test-time=${TEST_TIME}s)..."
  start_memtier_pod "${DOWN_POD_NAME}" "${DOWN_OUT_FILE}"
  DOWN_MEMTIER_START="$(date +%s)"
  echo "[${i}] Waiting ${STEADY_STATE_WAIT}s for downscale steady state..."
  sleep "${STEADY_STATE_WAIT}"

  echo "[${i}] Moving slots off the extra shard under load..."
  DOWNSCALE_START="$(date +%s)"
  DOWNSCALE_RESHARD_START="${DOWNSCALE_START}"
  EXTRA_MASTER_ID="$(extra_master_id || true)"
  if [[ -z "${EXTRA_MASTER_ID}" ]]; then
    echo "[${i}] ERROR: Could not find the extra master before reshard-down." >&2
    kubectl delete pod "${DOWN_POD_NAME}" -n "${NS}" --ignore-not-found
    restore_cluster
    exit 1
  fi
  SLOTS_ON_EXTRA="$(slot_count_for_node_id "${EXTRA_MASTER_ID}")"
  if ! move_slots_off_extra_master "${EXTRA_MASTER_ID}"; then
    echo "[${i}] ERROR: Reshard-down slot migration failed; attempting restore." >&2
    kubectl delete pod "${DOWN_POD_NAME}" -n "${NS}" --ignore-not-found
    restore_cluster
    exit 1
  fi
  DOWNSCALE_RESHARD_END="$(date +%s)"
  if ! ensure_cluster_clean "${REBALANCE_TIMEOUT}"; then
    echo "[${i}] ERROR: Cluster unhealthy after reshard-down slot migration; attempting restore." >&2
    kubectl delete pod "${DOWN_POD_NAME}" -n "${NS}" --ignore-not-found
    restore_cluster
    exit 1
  fi

  echo "[${i}] Removing extra cluster nodes under load..."
  DEL_NODE_START="$(date +%s)"
  delete_extra_cluster_nodes
  DEL_NODE_END="$(date +%s)"

  echo "[${i}] Scaling Helm release back to ${ORIGINAL_NODE_COUNT} Redis nodes under load..."
  SCALE_DOWN_START="$(date +%s)"
  if ! scale_down_to_original_nodes; then
    echo "[${i}] ERROR: Helm scale-down failed; attempting restore." >&2
    kubectl delete pod "${DOWN_POD_NAME}" -n "${NS}" --ignore-not-found
    restore_cluster
    exit 1
  fi
  SCALE_DOWN_END="$(date +%s)"
  DOWNSCALE_END="${SCALE_DOWN_END}"

  DOWN_MASTERS="$(cluster_nodes | awk '/master/ && !/fail/ {count++} END {print count + 0}')"
  echo "[${i}] Masters after reshard-down: ${DOWN_MASTERS}"

  echo "[${i}] Waiting for reshard-down memtier to finish..."
  if ! finish_memtier_pod "${DOWN_POD_NAME}" "${DOWN_OUT_FILE}"; then
    kubectl delete pod "${DOWN_POD_NAME}" -n "${NS}" --ignore-not-found
    restore_cluster
    exit 1
  fi

  cat > "${LOCAL_OUT}/${DOWN_TIMING_FILE}" <<EOF
{
  "run": ${i},
  "phase": "down",
  "provider": "selfhosted_redis",
  "redis_version": "7.2",
  "slot_migration_mode": "redis72",
  "atomic_slot_migration": false,
  "slot_migration_command": "redis_cluster_rebalance",
  "reshard_down_start": ${DOWNSCALE_RESHARD_START},
  "reshard_down_end": ${DOWNSCALE_RESHARD_END},
  "reshard_down_duration_s": $((DOWNSCALE_RESHARD_END - DOWNSCALE_RESHARD_START)),
  "reshard_down_start_s": $((DOWNSCALE_RESHARD_START - DOWN_MEMTIER_START)),
  "reshard_down_end_s": $((DOWNSCALE_RESHARD_END - DOWN_MEMTIER_START)),
  "del_node_start": ${DEL_NODE_START},
  "del_node_end": ${DEL_NODE_END},
  "del_node_duration_s": $((DEL_NODE_END - DEL_NODE_START)),
  "del_node_start_s": $((DEL_NODE_START - DOWN_MEMTIER_START)),
  "del_node_end_s": $((DEL_NODE_END - DOWN_MEMTIER_START)),
  "scale_down_start": ${SCALE_DOWN_START},
  "scale_down_end": ${SCALE_DOWN_END},
  "scale_down_duration_s": $((SCALE_DOWN_END - SCALE_DOWN_START)),
  "scale_down_start_s": $((SCALE_DOWN_START - DOWN_MEMTIER_START)),
  "scale_down_end_s": $((SCALE_DOWN_END - DOWN_MEMTIER_START)),
  "operation_start_s": $((DOWNSCALE_START - DOWN_MEMTIER_START)),
  "operation_end_s": $((DOWNSCALE_END - DOWN_MEMTIER_START)),
  "operation_duration_s": $((DOWNSCALE_END - DOWNSCALE_START)),
  "memtier_start": ${DOWN_MEMTIER_START},
  "original_shards": ${TARGET_SHARDS},
  "target_shards": ${ORIGINAL_SHARDS},
  "slots_moved": ${SLOTS_ON_EXTRA},
  "masters_after": ${DOWN_MASTERS}
}
EOF
  echo "[${i}] Timing data saved to ${LOCAL_OUT}/${DOWN_TIMING_FILE}"

  echo "[${i}] Cleaning up reshard-down memtier pod..."
  kubectl delete pod "${DOWN_POD_NAME}" -n "${NS}" --ignore-not-found
  reset_rollout_partition

  echo "[${i}] Done. Results: ${LOCAL_OUT}/${OUT_FILE}, ${LOCAL_OUT}/${TIMING_FILE}, ${LOCAL_OUT}/${DOWN_OUT_FILE}, ${LOCAL_OUT}/${DOWN_TIMING_FILE}"
done

echo ""
echo "=========================================="
echo "  All ${N} Redis 7.2 reshard runs complete."
echo "  Results in: ${LOCAL_OUT}/"
echo "  Analyse with:"
echo "    python cli.py reshard --input ${LOCAL_OUT} --output-dir ./plots/redis72_reshard"
echo "=========================================="
